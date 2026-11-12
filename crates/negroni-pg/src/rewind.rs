use negroni_config::{DaemonConfig, PgRoleCredentials};
use negroni_types::{HistoryEntry, WalLsn};
use tokio::process::Command;
use tokio::sync::watch;

use crate::PgError;
use crate::clone::{
    EphemeralPgpassGuard, prepare_empty_pgdata_for_clone, sanitize_conninfo_for_cli,
};
use crate::pidfd_monitor::PostmasterPidFile;
use crate::process::{run_cancellable_command, write_standby_signal};
use crate::version::PgBinaryPaths;

/// Parsed line from a PostgreSQL `pg_wal/000000XX.history` timeline history file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimelineHistoryLine {
    pub parent_timeline: u64,
    pub switchpoint: WalLsn,
    pub reason: String,
}

/// Parses a PostgreSQL `.history` file (`<parent_tli>\t<switchpoint_lsn>\t<reason>`).
///
/// # Examples
///
/// ```
/// use negroni_pg::rewind::parse_timeline_history;
/// use negroni_types::WalLsn;
///
/// let text = "# timeline 2 forked from 1\n1\t0/3000148\tno recovery target specified\n";
/// let entries = parse_timeline_history(text)?;
/// assert_eq!(entries.len(), 1);
/// assert_eq!(entries[0].parent_timeline, 1);
/// assert_eq!(entries[0].switchpoint, "0/3000148".parse::<WalLsn>()?);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn parse_timeline_history(contents: &str) -> Result<Vec<TimelineHistoryLine>, PgError> {
    let mut entries = Vec::new();
    for (idx, raw_line) in contents.lines().enumerate() {
        let trimmed = raw_line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let (tli_str, after_tli) = trimmed
            .split_once(|c: char| c.is_ascii_whitespace())
            .ok_or_else(|| {
                PgError::InvalidMetadata(format!(
                    "timeline history line {}: missing switchpoint LSN",
                    idx + 1
                ))
            })?;
        let rest_after_tli = after_tli.trim_start();
        if rest_after_tli.is_empty() {
            return Err(PgError::InvalidMetadata(format!(
                "timeline history line {}: missing switchpoint LSN",
                idx + 1
            )));
        }
        let (lsn_tok, reason_str) = rest_after_tli
            .split_once(|c: char| c.is_ascii_whitespace())
            .map_or((rest_after_tli, ""), |(l, r)| (l, r.trim()));

        let parent_timeline: u64 = tli_str.parse().map_err(|_| {
            PgError::InvalidMetadata(format!(
                "timeline history line {}: invalid timeline '{tli_str}'",
                idx + 1
            ))
        })?;
        let switchpoint: WalLsn = lsn_tok.parse().map_err(|e| {
            PgError::InvalidMetadata(format!(
                "timeline history line {}: invalid LSN '{lsn_tok}': {e}",
                idx + 1
            ))
        })?;

        entries.push(TimelineHistoryLine {
            parent_timeline,
            switchpoint,
            reason: reason_str.to_owned(),
        });
    }
    Ok(entries)
}

/// Deterministic decision returned when comparing local WAL state against the active leader's
/// timeline history (`patroni.postgresql.rewind`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RewindDecision {
    /// Local WAL lies on an ancestor of the leader's timeline at or before the fork switchpoint.
    NotNeeded,
    /// Local WAL diverged past the fork switchpoint and `pg_rewind` preconditions hold.
    RunPgRewind,
    /// Local WAL diverged, `pg_rewind` cannot be used, and `remove_data_directory_on_diverged_timelines` is enabled.
    WipeAndReclone,
    /// Local WAL diverged, `pg_rewind` preconditions are unmet (`!use_pg_rewind` or neither
    /// `data_checksums` nor `wal_log_hints`), and automatic wipe is disabled.
    ManualInterventionRequired,
}

/// Pure evaluation of whether a standby/former-primary requires `pg_rewind` before following
/// `leader_timeline`.
#[allow(clippy::too_many_arguments)]
pub fn evaluate_rewind_decision(
    local_timeline: u64,
    local_lsn: WalLsn,
    leader_timeline: u64,
    dcs_history: &[HistoryEntry],
    use_pg_rewind: bool,
    data_checksums: bool,
    wal_log_hints: bool,
    remove_on_diverged: bool,
) -> RewindDecision {
    if local_timeline == 0 || leader_timeline == 0 || local_timeline == leader_timeline {
        return RewindDecision::NotNeeded;
    }

    let diverged = if local_timeline > leader_timeline {
        true
    } else if let Some(fork_entry) = dcs_history.iter().find(|h| h.timeline == local_timeline) {
        local_lsn > fork_entry.lsn
    } else {
        // Switchpoint missing from history: conservatively treat older timeline as diverged.
        true
    };

    if !diverged {
        return RewindDecision::NotNeeded;
    }

    if use_pg_rewind && (data_checksums || wal_log_hints) {
        RewindDecision::RunPgRewind
    } else if remove_on_diverged {
        RewindDecision::WipeAndReclone
    } else {
        RewindDecision::ManualInterventionRequired
    }
}

fn effective_rewind_credentials(cfg: &DaemonConfig) -> &PgRoleCredentials {
    let rw = &cfg.postgresql.authentication.rewind;
    if rw.username.is_some() || rw.password.is_some() {
        rw
    } else {
        &cfg.postgresql.authentication.superuser
    }
}

/// Executes `pg_rewind --target-pgdata=<dir> --source-server=<dsn> --restore-target-wal`
/// (native across PostgreSQL 15–18), wiping `$PGDATA` on failure when
/// `remove_data_directory_on_rewind_failure` is configured.
pub async fn run_pg_rewind(
    cfg: &DaemonConfig,
    bins: &PgBinaryPaths,
    leader_conn: &str,
    cancel_rx: watch::Receiver<bool>,
) -> Result<(), PgError> {
    let pgdata = &cfg.postgresql.data_dir;
    if PostmasterPidFile::read_from_pgdata(pgdata)?.is_some() {
        return Err(PgError::Supervision(format!(
            "cannot run pg_rewind while postmaster.pid exists in '{}'",
            pgdata.display()
        )));
    }

    let creds = effective_rewind_credentials(cfg);
    let target = sanitize_conninfo_for_cli(leader_conn, creds, &format!("{}-rewind", cfg.name));
    let pgpass_guard = EphemeralPgpassGuard::create("rewind", &target, creds)?;

    let mut cmd = Command::new(bins.pg_rewind());
    cmd.arg(format!("--target-pgdata={}", pgdata.display()))
        .arg(format!("--source-server={}", target.password_free_dsn))
        .arg("--restore-target-wal");

    if let Some(ref guard) = pgpass_guard {
        cmd.env("PGPASSFILE", guard.path());
    } else if let Some(ref explicit_pgpass) = cfg.postgresql.pgpass {
        cmd.env("PGPASSFILE", explicit_pgpass);
    }

    match run_cancellable_command(cmd, "pg_rewind", cancel_rx).await {
        Ok(()) => {
            write_standby_signal(pgdata)?;
            Ok(())
        }
        Err(PgError::Cancelled(reason)) => Err(PgError::Cancelled(reason)),
        Err(err) => {
            if cfg.postgresql.remove_data_directory_on_rewind_failure {
                tracing::warn!(
                    pgdata = %pgdata.display(),
                    error = %err,
                    "pg_rewind failed and remove_data_directory_on_rewind_failure=true; wiping PGDATA for clean re-clone"
                );
                prepare_empty_pgdata_for_clone(pgdata)?;
            }
            Err(err)
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn sample_history() -> Vec<HistoryEntry> {
        vec![HistoryEntry {
            timeline: 1,
            lsn: WalLsn(5_000),
            reason: "no recovery target specified".to_owned(),
            timestamp: Some("2026-11-12T18:00:00Z".to_owned()),
            leader: Some(compact_str::CompactString::const_new("pg-1")),
        }]
    }

    #[test]
    fn replica_before_switchpoint_skips_rewind_while_diverged_primary_runs_rewind() {
        let history = sample_history();

        // Replica at TL 1, LSN 4_800 (<= switchpoint 5_000) -> zero divergence, stream directly!
        assert_eq!(
            evaluate_rewind_decision(1, WalLsn(4_800), 2, &history, true, true, false, false),
            RewindDecision::NotNeeded
        );

        // Former primary at TL 1, LSN 5_200 (> switchpoint 5_000) with PG 18 default checksums -> RunPgRewind!
        assert_eq!(
            evaluate_rewind_decision(1, WalLsn(5_200), 2, &history, true, true, false, false),
            RewindDecision::RunPgRewind
        );

        // Diverged without checksums or wal_log_hints:
        assert_eq!(
            evaluate_rewind_decision(1, WalLsn(5_200), 2, &history, true, false, false, false),
            RewindDecision::ManualInterventionRequired
        );
        assert_eq!(
            evaluate_rewind_decision(1, WalLsn(5_200), 2, &history, true, false, false, true),
            RewindDecision::WipeAndReclone
        );
    }
}
