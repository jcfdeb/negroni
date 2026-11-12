#![forbid(unsafe_code)]

pub mod actor;
pub mod dispatcher;
pub mod election;
pub mod jobs;
pub mod reducer;
pub mod sync_standby;

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use negroni_config::DaemonConfig;
use negroni_watchdog::{WatchdogHandle, notify_systemd};

pub use crate::actor::{
    ControlError, CoordinatorHandle, DEFAULT_EVENT_CHANNEL_CAPACITY,
    DEFAULT_FAILSAFE_CHANNEL_CAPACITY, FailsafeHeartbeatRequest, SharedNodeState, coordinator_loop,
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
pub use crate::reducer::{
    ControlCommand, DemoteReason, Effect, EffectBatch, HaEvent, HaRoleState, JobKind,
    LocalPgSummary, NodeCtx, PausedRole, PublishedNodeState,
};
pub use crate::sync_standby::{SyncStandbyPlan, compute_sync_standby_plan, quote_standby_name};

pub const fn aws_callbacks_enabled() -> bool {
    cfg!(feature = "aws-callbacks")
}

pub async fn run_daemon(cfg: DaemonConfig, watchdog: WatchdogHandle) -> ExitCode {
    let boot_now = Instant::now();
    let shared: SharedNodeState =
        Arc::new(ArcSwap::from_pointee(PublishedNodeState::initial(boot_now)));
    let backends = negroni_dcs::compiled_backends();

    tracing::info!(
        scope = %cfg.scope,
        member = %cfg.name,
        namespace = %cfg.namespace,
        ?backends,
        citus = negroni_pg::citus_enabled(),
        aws_callbacks = aws_callbacks_enabled(),
        role = %shared.load().role,
        "negroni HA coordinator initialized"
    );

    notify_systemd("READY=1");

    if let Err(err) = tokio::signal::ctrl_c().await {
        tracing::error!(error = %err, "failed to listen for shutdown signal");
        watchdog.shutdown();
        return ExitCode::FAILURE;
    }

    notify_systemd("STOPPING=1");
    watchdog.shutdown();
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
