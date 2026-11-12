#![forbid(unsafe_code)]

pub mod clone;
pub mod config_writer;
pub mod guc_units;
pub mod hba_validator;
pub mod pidfd_monitor;
pub mod process;
pub mod rewind;
pub mod slots;
pub mod sql_actor;
pub mod version;

use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use compact_str::CompactString;
use negroni_config::DaemonConfig;
use negroni_types::{ClusterStatus, UnsupportedPgVersion};
use rustix::process::Signal;
use thiserror::Error;
use tokio::sync::watch;

pub use crate::clone::{SanitizedConnTarget, clone_replica_with_fallback, run_pg_basebackup};
pub use crate::config_writer::{
    RuntimeGucOverlay, atomic_write_file, ensure_postgresql_base_conf, member_name_to_slot_name,
    render_postgresql_conf, render_primary_conninfo, write_managed_pg_configs, write_pgpass_file,
};
pub use crate::guc_units::{
    CanonicalGucValue, canonicalize_guc, guc_values_equivalent, parse_pg_bool,
    parse_pg_duration_micros, parse_pg_memory_bytes,
};
pub use crate::hba_validator::{
    validate_pg_hba, validate_pg_ident, validate_version_specific_gucs,
};
pub use crate::pidfd_monitor::{PidfdProcessMonitor, PostmasterPidFile, parse_proc_stat};
pub use crate::process::{
    adopt_running_postmaster, build_initdb_args, emergency_demote_ladder, has_valid_pgdata,
    remove_standby_signal, run_custom_bootstrap_script, run_initdb, start_postmaster,
    write_recovery_signal, write_standby_signal,
};
pub use crate::rewind::{
    RewindDecision, TimelineHistoryLine, evaluate_rewind_decision, parse_timeline_history,
    run_pg_rewind,
};
pub use crate::slots::{
    DesiredSlotsSpec, LogicalSlotAdvanceOp, LogicalSlotCreateOp, ObservedSlot,
    PermanentLogicalSlotTarget, SlotReconcilePlan, format_synchronized_standby_slots,
    plan_slot_reconciliation,
};
pub use crate::sql_actor::{
    CheckpointStats, DEFAULT_SQL_CHANNEL_CAPACITY, LocalSqlEndpointConfig, PgTelemetrySnapshot,
    SqlControlHandle, TELEMETRY_1RTT_SQL, checkpoint_stats_sql,
};
pub use crate::version::{
    PgBinaryPaths, PgCapabilities, PgMajor, PgProcessState, parse_postgres_version_banner,
    read_pgdata_major_version,
};

#[derive(Debug, Error)]
pub enum PgError {
    #[error(transparent)]
    UnsupportedVersion(#[from] UnsupportedPgVersion),
    #[error("I/O error while {context}: {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },
    #[error("PostgreSQL SQL control error: {0}")]
    Sql(#[from] tokio_postgres::Error),
    #[error("PostgreSQL TLS configuration error: {0}")]
    TlsConfig(String),
    #[error("PostgreSQL configuration validation error: {0}")]
    ConfigValidation(String),
    #[error("invalid PostgreSQL on-disk metadata: {0}")]
    InvalidMetadata(String),
    #[error("subprocess '{command}' exited with status {status:?}: {stderr}")]
    SubprocessFailed {
        command: String,
        status: Option<i32>,
        stderr: String,
    },
    #[error("PostgreSQL lifecycle job '{0}' was cancelled")]
    Cancelled(String),
    #[error("PostgreSQL supervision error: {0}")]
    Supervision(String),
}

pub const fn citus_enabled() -> bool {
    cfg!(feature = "citus")
}

#[derive(Debug)]
struct SupervisorInner {
    process_state: PgProcessState,
    detected_major: Option<PgMajor>,
    overlay: RuntimeGucOverlay,
    postmaster_start_epoch: Option<i64>,
}

/// Snapshot returned by [`PgSupervisor::inspect`] for feeding `HaEvent::PgTelemetry` and
/// Prometheus `patroni_postgres_state` / `patroni_postmaster_start_time`.
#[derive(Debug, Clone)]
pub struct PgInspectionReport {
    pub has_valid_pgdata: bool,
    pub process_state: PgProcessState,
    pub postmaster_start_epoch: Option<i64>,
    pub telemetry: Option<PgTelemetrySnapshot>,
}

/// High-level PostgreSQL 15–18 local instance supervisor combining `pidfd` process management,
/// the single-connection `SqlControlActor`, crash-safe config rendering, and dual-mode slot sync.
#[derive(Clone, Debug)]
pub struct PgSupervisor {
    cfg: Arc<RwLock<DaemonConfig>>,
    bins: PgBinaryPaths,
    sql: SqlControlHandle,
    inner: Arc<Mutex<SupervisorInner>>,
}

impl PgSupervisor {
    pub fn spawn(initial_cfg: DaemonConfig) -> Self {
        let bins = PgBinaryPaths::new(initial_cfg.postgresql.bin_dir.clone());
        let endpoint = LocalSqlEndpointConfig::from_postgres_config(&initial_cfg.postgresql);
        let sql = SqlControlHandle::spawn(endpoint);

        let initial_major = read_pgdata_major_version(&initial_cfg.postgresql.data_dir).ok();
        let (proc_state, start_epoch) =
            match adopt_running_postmaster(&initial_cfg.postgresql.data_dir) {
                Ok(Some((pid_info, _))) => {
                    (PgProcessState::Running, Some(pid_info.start_epoch_secs))
                }
                _ => (PgProcessState::Stopped, None),
            };

        Self {
            cfg: Arc::new(RwLock::new(initial_cfg)),
            bins,
            sql,
            inner: Arc::new(Mutex::new(SupervisorInner {
                process_state: proc_state,
                detected_major: initial_major,
                overlay: RuntimeGucOverlay::default(),
                postmaster_start_epoch: start_epoch,
            })),
        }
    }

    pub fn sql_handle(&self) -> &SqlControlHandle {
        &self.sql
    }

    pub fn update_config(&self, next_cfg: DaemonConfig) {
        if let Ok(mut guard) = self.cfg.write() {
            *guard = next_cfg;
        }
    }

    fn snapshot_config(&self) -> Result<DaemonConfig, PgError> {
        self.cfg
            .read()
            .map(|g| g.clone())
            .map_err(|_| PgError::Supervision("DaemonConfig RwLock poisoned".to_owned()))
    }

    fn set_process_state(&self, state: PgProcessState) {
        if let Ok(mut guard) = self.inner.lock() {
            guard.process_state = state;
        }
    }

    fn resolve_major(&self, pgdata: &std::path::Path) -> Result<PgMajor, PgError> {
        if let Ok(guard) = self.inner.lock()
            && let Some(m) = guard.detected_major
        {
            return Ok(m);
        }
        let m = read_pgdata_major_version(pgdata)?;
        if let Ok(mut guard) = self.inner.lock() {
            guard.detected_major = Some(m);
        }
        Ok(m)
    }

    pub async fn inspect(&self) -> PgInspectionReport {
        let Ok(cfg) = self.snapshot_config() else {
            return PgInspectionReport {
                has_valid_pgdata: false,
                process_state: PgProcessState::Stopped,
                postmaster_start_epoch: None,
                telemetry: None,
            };
        };

        let pgdata = &cfg.postgresql.data_dir;
        let valid_pgdata = has_valid_pgdata(pgdata);
        let adopted = adopt_running_postmaster(pgdata).ok().flatten();

        let telemetry = if adopted.is_some() {
            match self.sql.fetch_telemetry().await {
                Ok(snap) => {
                    if let Ok(mut guard) = self.inner.lock() {
                        guard.detected_major = Some(snap.pg_major);
                        guard.process_state = PgProcessState::Running;
                    }
                    Some(snap)
                }
                Err(_) => None,
            }
        } else {
            if let Ok(mut guard) = self.inner.lock()
                && guard.process_state == PgProcessState::Running
            {
                guard.process_state = PgProcessState::Stopped;
            }
            None
        };

        let (process_state, postmaster_start_epoch) = match self.inner.lock() {
            Ok(guard) => (
                guard.process_state,
                adopted
                    .as_ref()
                    .map(|(p, _)| p.start_epoch_secs)
                    .or(guard.postmaster_start_epoch),
            ),
            Err(_) => (PgProcessState::Stopped, None),
        };

        PgInspectionReport {
            has_valid_pgdata: valid_pgdata,
            process_state,
            postmaster_start_epoch,
            telemetry,
        }
    }

    pub async fn start_as_replica(&self) -> Result<(), PgError> {
        let cfg = self.snapshot_config()?;
        let major = self.resolve_major(&cfg.postgresql.data_dir)?;
        let overlay = self
            .inner
            .lock()
            .map(|g| g.overlay.clone())
            .unwrap_or_default();

        self.set_process_state(PgProcessState::Starting);
        if let Err(err) = write_managed_pg_configs(&cfg, major, &overlay) {
            self.set_process_state(PgProcessState::StartFailed);
            return Err(err);
        }

        let start_timeout = Duration::from_secs(cfg.dynamic.primary_start_timeout.max(10));
        match start_postmaster(&cfg, &self.bins, true, start_timeout).await {
            Ok((pid_info, _monitor)) => {
                if let Ok(mut guard) = self.inner.lock() {
                    guard.process_state = PgProcessState::Running;
                    guard.postmaster_start_epoch = Some(pid_info.start_epoch_secs);
                }
                Ok(())
            }
            Err(err) => {
                self.set_process_state(PgProcessState::StartFailed);
                Err(err)
            }
        }
    }

    pub async fn promote(&self) -> Result<(), PgError> {
        let promoted = self.sql.promote().await?;
        if !promoted {
            return Err(PgError::Supervision(
                "pg_promote(true, 60) returned false before promotion completed".to_owned(),
            ));
        }
        let cfg = self.snapshot_config()?;
        let _ = remove_standby_signal(&cfg.postgresql.data_dir);
        Ok(())
    }

    pub async fn demote(&self, immediate_at: Instant) -> Result<(), PgError> {
        let cfg = self.snapshot_config()?;
        self.set_process_state(PgProcessState::Stopping);
        self.sql.disconnect().await;

        let adopted = adopt_running_postmaster(&cfg.postgresql.data_dir)?;
        let monitor_ref = adopted.as_ref().map(|(_, m)| m);
        match emergency_demote_ladder(monitor_ref, &cfg.postgresql.data_dir, immediate_at).await {
            Ok(()) => {
                self.set_process_state(PgProcessState::Stopped);
                Ok(())
            }
            Err(err) => {
                self.set_process_state(PgProcessState::StopFailed);
                Err(err)
            }
        }
    }

    pub async fn reconfigure_standby(
        &self,
        primary_conninfo: &str,
        slot_name: Option<&str>,
    ) -> Result<(), PgError> {
        let cfg = self.snapshot_config()?;
        let major = self.resolve_major(&cfg.postgresql.data_dir)?;
        let overlay = {
            let mut guard = self
                .inner
                .lock()
                .map_err(|_| PgError::Supervision("SupervisorInner lock poisoned".to_owned()))?;
            guard.overlay.primary_conninfo = Some(primary_conninfo.to_owned());
            guard.overlay.primary_slot_name = slot_name.map(CompactString::from);
            guard.overlay.clone()
        };

        write_managed_pg_configs(&cfg, major, &overlay)?;
        self.reload_running_instance(&cfg.postgresql.data_dir).await
    }

    pub async fn set_synchronous_standby_names(&self, guc_value: &str) -> Result<(), PgError> {
        let cfg = self.snapshot_config()?;
        let major = self.resolve_major(&cfg.postgresql.data_dir)?;
        let overlay = {
            let mut guard = self
                .inner
                .lock()
                .map_err(|_| PgError::Supervision("SupervisorInner lock poisoned".to_owned()))?;
            guard.overlay.synchronous_standby_names = Some(guc_value.to_owned());
            guard.overlay.clone()
        };

        write_managed_pg_configs(&cfg, major, &overlay)?;
        self.reload_running_instance(&cfg.postgresql.data_dir).await
    }

    async fn reload_running_instance(&self, pgdata: &std::path::Path) -> Result<(), PgError> {
        if self.sql.reload_conf().await.is_ok() {
            return Ok(());
        }
        if let Some((_, monitor)) = adopt_running_postmaster(pgdata)? {
            let _ = monitor.send_signal(Signal::HUP)?;
        }
        Ok(())
    }

    pub async fn sync_slots(
        &self,
        status: &ClusterStatus,
        is_primary: bool,
        is_demoted_rejoin: bool,
        peer_names: &[CompactString],
    ) -> Result<SlotReconcilePlan, PgError> {
        let cfg = self.snapshot_config()?;
        let major = self.resolve_major(&cfg.postgresql.data_dir)?;
        let desired = DesiredSlotsSpec::from_cluster_and_config(&cfg, status, peer_names);
        self.sql
            .reconcile_slots(major, is_primary, is_demoted_rejoin, desired)
            .await
    }

    pub async fn run_initdb_job(&self, cancel_rx: watch::Receiver<bool>) -> Result<(), PgError> {
        let cfg = self.snapshot_config()?;
        self.set_process_state(PgProcessState::InitDb);
        match run_initdb(&cfg, &self.bins, cancel_rx).await {
            Ok(major) => {
                let overlay = self
                    .inner
                    .lock()
                    .map(|mut g| {
                        g.detected_major = Some(major);
                        g.process_state = PgProcessState::BootstrapStarting;
                        g.overlay.clone()
                    })
                    .unwrap_or_default();
                write_managed_pg_configs(&cfg, major, &overlay)?;
                let start_timeout = Duration::from_secs(cfg.dynamic.primary_start_timeout.max(10));
                let (pid_info, _) =
                    start_postmaster(&cfg, &self.bins, false, start_timeout).await?;
                if let Ok(mut g) = self.inner.lock() {
                    g.process_state = PgProcessState::Running;
                    g.postmaster_start_epoch = Some(pid_info.start_epoch_secs);
                }
                Ok(())
            }
            Err(err) => {
                self.set_process_state(PgProcessState::InitDbFailed);
                Err(err)
            }
        }
    }

    pub async fn run_custom_bootstrap_job(
        &self,
        cancel_rx: watch::Receiver<bool>,
    ) -> Result<(), PgError> {
        let cfg = self.snapshot_config()?;
        let script = cfg.bootstrap_method.as_deref().ok_or_else(|| {
            PgError::ConfigValidation("custom bootstrap requested without bootstrap.method".into())
        })?;
        self.set_process_state(PgProcessState::CustomBootstrap);
        match run_custom_bootstrap_script(script, &cfg.scope, &cfg.postgresql.data_dir, cancel_rx)
            .await
        {
            Ok(()) => {
                self.set_process_state(PgProcessState::Stopped);
                Ok(())
            }
            Err(err) => {
                self.set_process_state(PgProcessState::CustomBootstrapFailed);
                Err(err)
            }
        }
    }

    pub async fn run_clone_job(
        &self,
        upstream_conn: &str,
        cancel_rx: watch::Receiver<bool>,
    ) -> Result<(), PgError> {
        let cfg = self.snapshot_config()?;
        self.set_process_state(PgProcessState::CreatingReplica);
        clone_replica_with_fallback(&cfg, &self.bins, upstream_conn, &[], cancel_rx).await?;
        self.start_as_replica().await
    }

    pub async fn run_rewind_job(
        &self,
        leader_conn: &str,
        cancel_rx: watch::Receiver<bool>,
    ) -> Result<(), PgError> {
        let cfg = self.snapshot_config()?;
        self.sql.disconnect().await;
        if let Some((_, monitor)) = adopt_running_postmaster(&cfg.postgresql.data_dir)? {
            emergency_demote_ladder(
                Some(&monitor),
                &cfg.postgresql.data_dir,
                Instant::now() + Duration::from_secs(5),
            )
            .await?;
        }
        run_pg_rewind(&cfg, &self.bins, leader_conn, cancel_rx).await?;
        self.start_as_replica().await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn prometheus_state_discriminants_match_patroni() {
        assert_eq!(PgProcessState::InitDb as u8, 0);
        assert_eq!(PgProcessState::Running as u8, 5);
        assert_eq!(PgProcessState::Crashed as u8, 14);
    }
}
