#![forbid(unsafe_code)]

pub mod actor;
pub mod dispatcher;
pub mod election;
pub mod jobs;
pub mod peer_rpc;
pub mod reducer;
pub mod sync_standby;

use std::collections::BTreeMap;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use compact_str::CompactString;
use negroni_config::DaemonConfig;
use negroni_dcs::{AnyDcs, DcsBackend};
use negroni_pg::{PgInspectionReport, PgProcessState, PgSupervisor};
use negroni_types::{ClusterSnapshot, WalLsn};
use negroni_watchdog::{WatchdogHandle, notify_systemd};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

pub use crate::actor::{
    ControlBoxFuture, ControlError, ControlOpError, ControlPlaneOps, CoordinatorHandle,
    DEFAULT_EVENT_CHANNEL_CAPACITY, DEFAULT_FAILSAFE_CHANNEL_CAPACITY, FailsafeHeartbeatRequest,
    LiveControlPlane, NoopControlPlane, ScheduledRestartInfo, SharedClusterCache, SharedNodeState,
    coordinator_loop,
};
pub use crate::dispatcher::{
    EffectDispatcher, NoopPeerRpcPort, NoopPgControlPort, PeerRpcPort, PgControlPort, WatchdogPort,
};
pub use crate::election::{
    LocalCandidateProfile, PeerElectionSample, PeerProbeContext, is_eligible_for_election,
    leader_conn_url, select_clone_source, select_upstream_source, should_acquire_after_peer_probe,
    surviving_election_peers,
};
pub use crate::jobs::{JobRunner, JobSupervisor, NoopJobRunner};
pub use crate::peer_rpc::{HttpPeerRpcClient, resolve_member_endpoint};
pub use crate::reducer::{
    ControlCommand, DemoteReason, Effect, EffectBatch, HaEvent, HaRoleState, JobKind,
    LocalPgSummary, NodeCtx, PausedRole, PublishedNodeState,
};
pub use crate::sync_standby::{SyncStandbyPlan, compute_sync_standby_plan, quote_standby_name};

pub const fn aws_callbacks_enabled() -> bool {
    cfg!(feature = "aws-callbacks")
}

fn pg_state_label(state: PgProcessState) -> CompactString {
    match state {
        PgProcessState::InitDb => CompactString::const_new("initializing new cluster"),
        PgProcessState::InitDbFailed => CompactString::const_new("initdb failed"),
        PgProcessState::CustomBootstrap => CompactString::const_new("running custom bootstrap"),
        PgProcessState::CustomBootstrapFailed => {
            CompactString::const_new("custom bootstrap failed")
        }
        PgProcessState::CreatingReplica => CompactString::const_new("creating replica"),
        PgProcessState::Running => CompactString::const_new("running"),
        PgProcessState::Starting | PgProcessState::BootstrapStarting => {
            CompactString::const_new("starting")
        }
        PgProcessState::StartFailed => CompactString::const_new("start failed"),
        PgProcessState::Restarting => CompactString::const_new("restarting"),
        PgProcessState::RestartFailed => CompactString::const_new("restart failed"),
        PgProcessState::Stopping => CompactString::const_new("stopping"),
        PgProcessState::Stopped => CompactString::const_new("stopped"),
        PgProcessState::StopFailed => CompactString::const_new("stop failed"),
        PgProcessState::Crashed => CompactString::const_new("crashed"),
    }
}

fn map_inspection_to_summary(report: PgInspectionReport) -> LocalPgSummary {
    let start_epoch = report
        .postmaster_start_epoch
        .and_then(|s| u64::try_from(s).ok())
        .unwrap_or(0);

    if let Some(tel) = report.telemetry {
        let server_version_num = (tel.pg_major as u32) * 10_000;
        let lag_bytes = if tel.in_recovery {
            tel.primary_or_recv_lsn
                .0
                .saturating_sub(tel.replay_lsn.unwrap_or(tel.primary_or_recv_lsn).0)
        } else {
            0
        };
        LocalPgSummary {
            has_valid_pgdata: report.has_valid_pgdata,
            state_label: CompactString::const_new("running"),
            postgres_state_code: PgProcessState::Running as u8,
            in_recovery: tel.in_recovery,
            lsn: tel.primary_or_recv_lsn,
            xlog_paused: tel.wal_paused,
            timeline: tel.timeline_id,
            lag_bytes,
            is_sync_standby: false,
            is_quorum_standby: false,
            pending_restart: tel.pending_restart,
            needs_rewind: false,
            postmaster_start_time_epoch: start_epoch,
            xlog_replayed_timestamp_epoch: 0.0,
            server_version_num,
            system_identifier: None,
            slots: BTreeMap::new(),
        }
    } else {
        LocalPgSummary {
            has_valid_pgdata: report.has_valid_pgdata,
            state_label: pg_state_label(report.process_state),
            postgres_state_code: report.process_state as u8,
            in_recovery: true,
            lsn: WalLsn::ZERO,
            xlog_paused: false,
            timeline: 0,
            lag_bytes: 0,
            is_sync_standby: false,
            is_quorum_standby: false,
            pending_restart: false,
            needs_rewind: false,
            postmaster_start_time_epoch: start_epoch,
            xlog_replayed_timestamp_epoch: 0.0,
            server_version_num: 170_000,
            system_identifier: None,
            slots: BTreeMap::new(),
        }
    }
}

/// Active background tasks comprising a live HA coordinator runtime.
pub struct CoordinatorRuntime {
    shutdown_tx: watch::Sender<bool>,
    coordinator_join: JoinHandle<()>,
    telemetry_join: JoinHandle<()>,
    watchdog: Arc<WatchdogHandle>,
}

impl CoordinatorRuntime {
    pub fn shutdown_receiver(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        self.telemetry_join.abort();
        let _ = self.coordinator_join.await;
        match Arc::try_unwrap(self.watchdog) {
            Ok(wd) => wd.shutdown(),
            Err(shared_wd) => {
                let _ = shared_wd.send(negroni_watchdog::WatchdogCmd::Shutdown);
            }
        }
    }
}

/// Launches the live `AnyDcs` driver, `PgSupervisor`, `HttpPeerRpcClient`, local PostgreSQL
/// telemetry poller, and cancel-safe `coordinator_loop`, returning a [`CoordinatorHandle`]
/// ready to be served by `negroni-api`.
pub async fn start_coordinator_runtime(
    cfg: DaemonConfig,
    watchdog: WatchdogHandle,
) -> Result<(CoordinatorHandle, CoordinatorRuntime), String> {
    let dcs = match AnyDcs::from_daemon_config(&cfg, None).await {
        Ok(d) => Arc::new(d),
        Err(err) => {
            watchdog.shutdown();
            return Err(format!("failed to initialize DCS backend: {err}"));
        }
    };
    let pg = Arc::new(PgSupervisor::spawn(cfg.clone()));

    let boot_now = Instant::now();
    let ctx = NodeCtx::from_config(&cfg, boot_now);
    let initial_published = ctx.publish_snapshot(boot_now);
    let published: SharedNodeState = Arc::new(ArcSwap::from_pointee(initial_published));
    let cluster_cache: SharedClusterCache =
        Arc::new(ArcSwap::from_pointee(ClusterSnapshot::default()));

    let peers = match HttpPeerRpcClient::from_daemon_config(&cfg, Arc::clone(&published)) {
        Ok(p) => Arc::new(p),
        Err(err) => {
            watchdog.shutdown();
            return Err(err);
        }
    };

    let (event_tx, event_rx) = mpsc::channel(DEFAULT_EVENT_CHANNEL_CAPACITY);
    let (failsafe_tx, failsafe_rx) = mpsc::channel(DEFAULT_FAILSAFE_CHANNEL_CAPACITY);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let watch_rx = dcs.subscribe_watches();

    let wd_arc = Arc::new(watchdog);
    let dispatcher = EffectDispatcher::new(
        cfg.name.clone(),
        Arc::clone(&dcs),
        Arc::clone(&pg),
        peers,
        Arc::clone(&wd_arc),
        Arc::clone(&pg),
        event_tx.clone(),
    )
    .with_cluster_cache(Arc::clone(&cluster_cache));

    let control_ops = Arc::new(LiveControlPlane::new(
        Arc::clone(&dcs),
        Arc::clone(&pg),
        Arc::clone(&cluster_cache),
    ));
    let handle = CoordinatorHandle::new(Arc::clone(&published), event_tx.clone(), failsafe_tx)
        .with_control_plane(cluster_cache, control_ops);

    let pg_poller = Arc::clone(&pg);
    let mut poller_shutdown = shutdown_rx.clone();
    let poll_period =
        (cfg.timings.loop_wait / 2).clamp(Duration::from_millis(500), Duration::from_secs(2));
    let telemetry_tx = event_tx;
    let telemetry_join = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(poll_period);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                changed = poller_shutdown.changed() => {
                    if changed.is_err() || *poller_shutdown.borrow() {
                        break;
                    }
                }
                _ = ticker.tick() => {}
            }
            let report = pg_poller.inspect().await;
            let summary = map_inspection_to_summary(report);
            if telemetry_tx
                .send(HaEvent::PgObservation {
                    now: Instant::now(),
                    telemetry: summary,
                })
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let coordinator_join = tokio::spawn(coordinator_loop(
        ctx,
        event_rx,
        failsafe_rx,
        watch_rx,
        shutdown_rx,
        published,
        dispatcher,
    ));

    Ok((
        handle,
        CoordinatorRuntime {
            shutdown_tx,
            coordinator_join,
            telemetry_join,
            watchdog: wd_arc,
        },
    ))
}

pub async fn run_daemon(cfg: DaemonConfig, watchdog: WatchdogHandle) -> ExitCode {
    let backends = negroni_dcs::compiled_backends();
    tracing::info!(
        scope = %cfg.scope,
        member = %cfg.name,
        namespace = %cfg.namespace,
        ?backends,
        citus = negroni_pg::citus_enabled(),
        aws_callbacks = aws_callbacks_enabled(),
        "starting negroni HA coordinator"
    );

    let (_handle, runtime) = match start_coordinator_runtime(cfg, watchdog).await {
        Ok(pair) => pair,
        Err(err) => {
            tracing::error!(error = %err, "failed to start HA coordinator runtime");
            return ExitCode::FAILURE;
        }
    };

    notify_systemd("READY=1");

    if let Err(err) = tokio::signal::ctrl_c().await {
        tracing::error!(error = %err, "failed to listen for shutdown signal");
        runtime.shutdown().await;
        return ExitCode::FAILURE;
    }

    notify_systemd("STOPPING=1");
    runtime.shutdown().await;
    ExitCode::SUCCESS
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn initial_node_snapshot_denies_read_write_traffic() {
        let state = PublishedNodeState::initial(Instant::now());
        assert!(!state.is_read_write_valid);
        assert_eq!(state.role, "uninitialized");
    }
}
