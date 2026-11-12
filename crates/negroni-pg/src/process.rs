use std::fs;
use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use negroni_config::DaemonConfig;
use rustix::process::Signal;
use serde_json::Value as JsonValue;
use tokio::process::Command;
use tokio::sync::watch;

use crate::PgError;
use crate::config_writer::{atomic_write_file, sync_directory};
use crate::pidfd_monitor::{PidfdProcessMonitor, PostmasterPidFile};
use crate::version::{
    PgBinaryPaths, PgMajor, parse_postgres_version_banner, read_pgdata_major_version,
};

/// Checks whether `pgdata` contains an initialized PostgreSQL 15–18 data directory.
pub fn has_valid_pgdata(pgdata: &Path) -> bool {
    pgdata.join("PG_VERSION").is_file()
        && pgdata.join("global").join("pg_control").is_file()
        && read_pgdata_major_version(pgdata).is_ok()
}

/// Atomically writes `$PGDATA/standby.signal` and fsyncs `$PGDATA` so PostgreSQL enters hot standby.
pub fn write_standby_signal(pgdata: &Path) -> Result<(), PgError> {
    atomic_write_file(&pgdata.join("standby.signal"), b"", Some(0o600))
}

/// Atomically writes `$PGDATA/recovery.signal` and fsyncs `$PGDATA` for PITR recovery.
pub fn write_recovery_signal(pgdata: &Path) -> Result<(), PgError> {
    atomic_write_file(&pgdata.join("recovery.signal"), b"", Some(0o600))
}

/// Removes `$PGDATA/standby.signal` (if present) and fsyncs `$PGDATA`.
pub fn remove_standby_signal(pgdata: &Path) -> Result<(), PgError> {
    let signal_path = pgdata.join("standby.signal");
    match fs::remove_file(&signal_path) {
        Ok(()) => sync_directory(pgdata).map_err(|source| PgError::Io {
            context: format!(
                "syncing '{}' after unlinking standby.signal",
                pgdata.display()
            ),
            source,
        }),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(PgError::Io {
            context: format!("removing '{}'", signal_path.display()),
            source,
        }),
    }
}

/// Builds `initdb` CLI arguments from Patroni's `bootstrap.initdb` list without leaking passwords.
pub fn build_initdb_args(
    pgdata: &Path,
    initdb_options: &[JsonValue],
    major: PgMajor,
    superuser_username: Option<&str>,
    pwfile_path: Option<&Path>,
) -> Result<Vec<String>, PgError> {
    let mut args = vec![format!("--pgdata={}", pgdata.display())];
    let mut explicit_checksum_flag = false;

    for item in initdb_options {
        match item {
            JsonValue::String(flag) => {
                let cleaned = flag.trim().trim_start_matches('-');
                if cleaned.is_empty() {
                    continue;
                }
                if cleaned == "data-checksums" || cleaned == "k" || cleaned == "no-data-checksums" {
                    explicit_checksum_flag = true;
                }
                if cleaned.len() == 1 {
                    args.push(format!("-{cleaned}"));
                } else {
                    args.push(format!("--{cleaned}"));
                }
            }
            JsonValue::Object(map) => {
                for (k, v) in map {
                    let cleaned_k = k.trim().trim_start_matches('-');
                    if cleaned_k == "data-checksums" || cleaned_k == "no-data-checksums" {
                        explicit_checksum_flag = true;
                    }
                    match v {
                        JsonValue::Bool(true) => args.push(format!("--{cleaned_k}")),
                        JsonValue::Bool(false) => {}
                        JsonValue::String(s) => args.push(format!("--{cleaned_k}={s}")),
                        JsonValue::Number(n) => args.push(format!("--{cleaned_k}={n}")),
                        other => {
                            return Err(PgError::ConfigValidation(format!(
                                "unsupported bootstrap.initdb value for '{cleaned_k}': {other}"
                            )));
                        }
                    }
                }
            }
            other => {
                return Err(PgError::ConfigValidation(format!(
                    "unsupported bootstrap.initdb entry: {other}"
                )));
            }
        }
    }

    // PG 18 enables checksums by default (`default_initdb_checksums = true`); on PG 15-17 we
    // leave checksums to `bootstrap.initdb` while avoiding duplicate flags on PG 18.
    let _ = (
        major.capabilities().default_initdb_checksums,
        explicit_checksum_flag,
    );

    if let Some(user) = superuser_username.filter(|u| !u.is_empty())
        && !args
            .iter()
            .any(|a| a.starts_with("--username=") || a == "-U")
    {
        args.push(format!("--username={user}"));
    }

    if let Some(pwfile) = pwfile_path {
        args.push(format!("--pwfile={}", pwfile.display()));
    }

    Ok(args)
}

/// Probes `initdb --version` (or `postgres --version`) to determine the installed binary's `PgMajor`.
pub async fn probe_installed_pg_major(bins: &PgBinaryPaths) -> Result<PgMajor, PgError> {
    let output = Command::new(bins.postgres())
        .arg("--version")
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(|source| PgError::Io {
            context: format!("executing '{} --version'", bins.postgres().display()),
            source,
        })?;

    if !output.status.success() {
        return Err(PgError::SubprocessFailed {
            command: format!("{} --version", bins.postgres().display()),
            status: output.status.code(),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_postgres_version_banner(&stdout)
}

/// Awaits a child subprocess while honoring cooperative `cancel_rx` cancellation.
pub async fn run_cancellable_command(
    mut cmd: Command,
    cmd_label: &str,
    mut cancel_rx: watch::Receiver<bool>,
) -> Result<(), PgError> {
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    let mut child = cmd.spawn().map_err(|source| PgError::Io {
        context: format!("spawning '{cmd_label}'"),
        source,
    })?;

    let stderr_handle = child.stderr.take().map(|mut pipe| {
        tokio::spawn(async move {
            let mut buf = Vec::new();
            let _ = tokio::io::AsyncReadExt::read_to_end(&mut pipe, &mut buf).await;
            buf
        })
    });

    if *cancel_rx.borrow() {
        let _ = child.kill().await;
        return Err(PgError::Cancelled(cmd_label.to_owned()));
    }

    let status = loop {
        tokio::select! {
            biased;
            changed = cancel_rx.changed() => {
                if changed.is_ok() && *cancel_rx.borrow() {
                    let _ = child.kill().await;
                    return Err(PgError::Cancelled(cmd_label.to_owned()));
                }
                if changed.is_err() {
                    break child.wait().await.map_err(|source| PgError::Io {
                        context: format!("waiting on '{cmd_label}'"),
                        source,
                    })?;
                }
            }
            res = child.wait() => {
                break res.map_err(|source| PgError::Io {
                    context: format!("waiting on '{cmd_label}'"),
                    source,
                })?;
            }
        }
    };

    let stderr_bytes = match stderr_handle {
        Some(handle) => handle.await.unwrap_or_default(),
        None => Vec::new(),
    };

    if status.success() {
        Ok(())
    } else {
        Err(PgError::SubprocessFailed {
            command: cmd_label.to_owned(),
            status: status.code(),
            stderr: String::from_utf8_lossy(&stderr_bytes).trim().to_owned(),
        })
    }
}

/// Runs `initdb` against `cfg.postgresql.data_dir` using a temporary `0600` password file
/// that is unlinked unconditionally before returning.
pub async fn run_initdb(
    cfg: &DaemonConfig,
    bins: &PgBinaryPaths,
    cancel_rx: watch::Receiver<bool>,
) -> Result<PgMajor, PgError> {
    let major = probe_installed_pg_major(bins).await?;
    let pgdata = &cfg.postgresql.data_dir;
    if let Some(parent) = pgdata.parent() {
        fs::create_dir_all(parent).map_err(|source| PgError::Io {
            context: format!("creating parent of PGDATA '{}'", parent.display()),
            source,
        })?;
    }

    let superuser = &cfg.postgresql.authentication.superuser;
    let pwfile_guard = if let Some(ref secret) = superuser.password {
        let path = std::env::temp_dir().join(format!(
            ".negroni-initdb-pw-{}-{}.tmp",
            cfg.name,
            std::process::id()
        ));
        let mut pw_bytes = secret.expose().as_bytes().to_vec();
        pw_bytes.push(b'\n');
        atomic_write_file(&path, &pw_bytes, Some(0o600))?;
        Some(path)
    } else {
        None
    };

    let args_res = build_initdb_args(
        pgdata,
        &cfg.bootstrap_initdb,
        major,
        superuser.username.as_deref(),
        pwfile_guard.as_deref(),
    );

    let run_res = match args_res {
        Ok(args) => {
            let mut cmd = Command::new(bins.initdb());
            cmd.args(&args);
            run_cancellable_command(cmd, "initdb", cancel_rx).await
        }
        Err(err) => Err(err),
    };

    if let Some(pw_path) = pwfile_guard {
        let _ = fs::remove_file(pw_path);
    }

    run_res?;
    Ok(major)
}

/// Runs a custom bootstrap script (`bootstrap.method` != `"initdb"`).
pub async fn run_custom_bootstrap_script(
    script_cmd: &str,
    scope: &str,
    pgdata: &Path,
    cancel_rx: watch::Receiver<bool>,
) -> Result<(), PgError> {
    let words = crate::clone::split_command_words(script_cmd)?;
    let mut cmd = Command::new(&words[0]);
    cmd.args(&words[1..])
        .arg(format!("--scope={scope}"))
        .arg("--role=bootstrap")
        .arg(format!("--datadir={}", pgdata.display()));
    run_cancellable_command(cmd, "custom_bootstrap", cancel_rx).await
}

/// Adopts an already-running PostgreSQL `postmaster` via `$PGDATA/postmaster.pid` + `pidfd_open`
/// so daemon upgrades and restarts incur zero database downtime.
pub fn adopt_running_postmaster(
    pgdata: &Path,
) -> Result<Option<(PostmasterPidFile, PidfdProcessMonitor)>, PgError> {
    let Some(pid_file) = PostmasterPidFile::read_from_pgdata(pgdata)? else {
        return Ok(None);
    };
    let Some(monitor) = PidfdProcessMonitor::adopt(pid_file.pid, Some(pgdata))? else {
        return Ok(None);
    };
    Ok(Some((pid_file, monitor)))
}

/// Starts PostgreSQL via `pg_ctl start -w` and adopts the resulting postmaster via `pidfd_open`.
pub async fn start_postmaster(
    cfg: &DaemonConfig,
    bins: &PgBinaryPaths,
    as_replica: bool,
    start_timeout: Duration,
) -> Result<(PostmasterPidFile, PidfdProcessMonitor), PgError> {
    let pgdata = &cfg.postgresql.data_dir;
    if as_replica {
        write_standby_signal(pgdata)?;
    }

    if let Some(existing) = adopt_running_postmaster(pgdata)? {
        return Ok(existing);
    }

    let timeout_secs = start_timeout.as_secs().max(5);
    let mut cmd = Command::new(bins.pg_ctl());
    cmd.arg("-D")
        .arg(pgdata)
        .arg("-w")
        .arg("-t")
        .arg(timeout_secs.to_string());

    if let Some(ref cfg_dir) = cfg.postgresql.config_dir {
        let conf_file = cfg_dir.join("postgresql.conf");
        cmd.arg("-o")
            .arg(format!("--config-file={}", conf_file.display()));
    }
    cmd.arg("start")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let output = cmd.output().await.map_err(|source| PgError::Io {
        context: format!("starting PostgreSQL via '{}'", bins.pg_ctl().display()),
        source,
    })?;

    if !output.status.success() {
        return Err(PgError::SubprocessFailed {
            command: format!("{} start", bins.pg_ctl().display()),
            status: output.status.code(),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }

    adopt_running_postmaster(pgdata)?.ok_or_else(|| {
        PgError::Supervision(
            "pg_ctl start succeeded, but postmaster.pid could not be adopted".to_owned(),
        )
    })
}

/// Executes the two-stage emergency demotion ladder (`SIGINT` fast stop -> `SIGQUIT` immediate
/// abort at `immediate_at` = `T_wd - 2s` -> durable `standby.signal` before returning):
///
/// 1. Sends `SIGINT` (`-m fast`) immediately so PostgreSQL aborts read-write transactions.
/// 2. If postmaster is still alive at `immediate_at`, escalates to `SIGQUIT` (`-m immediate`)
///    so PostgreSQL terminates deterministically *before* `/dev/watchdog` hardware reset fires.
/// 3. Atomically writes and fsyncs `$PGDATA/standby.signal` before returning `Ok(())`.
pub async fn emergency_demote_ladder(
    monitor: Option<&PidfdProcessMonitor>,
    pgdata: &Path,
    immediate_at: Instant,
) -> Result<(), PgError> {
    if let Some(mon) = monitor
        && mon.is_alive()
    {
        let now = Instant::now();
        if now < immediate_at {
            tracing::warn!(
                pid = mon.raw_pid(),
                graceful_budget_ms = immediate_at.duration_since(now).as_millis(),
                "emergency demotion stage 1: sending SIGINT (fast stop) to postmaster"
            );
            let _ = mon.send_signal(Signal::INT)?;
            let budget = immediate_at.saturating_duration_since(Instant::now());
            if tokio::time::timeout(budget, mon.wait_for_exit())
                .await
                .is_err()
                && mon.is_alive()
            {
                tracing::error!(
                    pid = mon.raw_pid(),
                    "emergency demotion stage 2: graceful budget exhausted; escalating to SIGQUIT (immediate stop)"
                );
                let _ = mon.send_signal(Signal::QUIT)?;
                let _ =
                    tokio::time::timeout(Duration::from_millis(1800), mon.wait_for_exit()).await;
            }
        } else {
            tracing::error!(
                pid = mon.raw_pid(),
                "emergency demotion deadline already reached; sending SIGQUIT (immediate stop) directly"
            );
            let _ = mon.send_signal(Signal::QUIT)?;
            let _ = tokio::time::timeout(Duration::from_millis(1800), mon.wait_for_exit()).await;
        }

        if mon.is_alive() {
            return Err(PgError::Supervision(format!(
                "postmaster PID {} remained alive after SIGQUIT; keeping hardware watchdog armed!",
                mon.raw_pid()
            )));
        }
    }

    write_standby_signal(pgdata)?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn builds_initdb_arguments_from_patroni_scalars_and_maps() {
        let opts = vec![
            json!("data-checksums"),
            json!({"encoding": "UTF8"}),
            json!({"locale": "C.UTF-8"}),
        ];
        let args = build_initdb_args(
            Path::new("/pgdata/17"),
            &opts,
            PgMajor::V17,
            Some("postgres"),
            Some(Path::new("/tmp/pw")),
        )
        .unwrap();

        assert_eq!(
            args,
            vec![
                "--pgdata=/pgdata/17",
                "--data-checksums",
                "--encoding=UTF8",
                "--locale=C.UTF-8",
                "--username=postgres",
                "--pwfile=/tmp/pw",
            ]
        );
    }

    #[tokio::test]
    async fn demotion_ladder_escalates_from_sigint_to_sigquit_and_writes_standby_signal() {
        let tmp_pgdata =
            std::env::temp_dir().join(format!("negroni-demote-ladder-{}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp_pgdata);
        fs::create_dir_all(&tmp_pgdata).unwrap();

        // Spawn a mock postmaster that ignores SIGINT (simulating a stuck fast-shutdown
        // backend) and installs an explicit SIGQUIT handler (matching PostgreSQL's
        // `pqsignal(SIGQUIT, pmdie)`—note non-interactive `/bin/sh` defaults SIGQUIT to SIG_IGN)
        // so Stage 2 SIGQUIT escalation at `immediate_at` is deterministically verified.
        let ready_marker = tmp_pgdata.join("mock_ready");
        let quit_marker = tmp_pgdata.join("sigquit_received");
        let mut stubborn_child = Command::new("sh")
            .arg("-c")
            .arg(
                "trap '' INT; \
                 trap ': > \"$2\"; kill $child 2>/dev/null; exit 2' QUIT; \
                 sleep 30 & child=$!; \
                 : > \"$1\"; \
                 while kill -0 $child 2>/dev/null; do wait $child; done",
            )
            .arg("sh")
            .arg(&ready_marker)
            .arg(&quit_marker)
            .kill_on_drop(true)
            .spawn()
            .unwrap();

        let wait_deadline = Instant::now() + Duration::from_secs(2);
        while !ready_marker.exists() {
            assert!(
                Instant::now() < wait_deadline,
                "timed out waiting for mock postmaster trap installation"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let pid = stubborn_child.id().unwrap();
        let monitor = PidfdProcessMonitor::adopt(pid, None).unwrap().unwrap();
        let immediate_at = Instant::now() + Duration::from_millis(80);

        let reap_handle = tokio::spawn(async move { stubborn_child.wait().await });

        emergency_demote_ladder(Some(&monitor), &tmp_pgdata, immediate_at)
            .await
            .unwrap();

        let status = reap_handle.await.unwrap().unwrap();
        assert_eq!(status.code(), Some(2));
        assert!(quit_marker.exists());
        assert!(!monitor.is_alive());
        assert!(tmp_pgdata.join("standby.signal").exists());

        let _ = fs::remove_dir_all(&tmp_pgdata);
    }
}
