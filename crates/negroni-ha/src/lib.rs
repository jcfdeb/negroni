use std::collections::BTreeMap;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Instant;

use arc_swap::ArcSwap;
use compact_str::CompactString;
use negroni_config::DaemonConfig;
use negroni_watchdog::{WatchdogHandle, notify_systemd};

#[derive(Debug, Clone)]
pub struct PublishedNodeState {
    pub state: CompactString,
    pub role: CompactString,
    pub is_read_write_valid: bool,
    pub is_sync_standby: bool,
    pub is_quorum_standby: bool,
    pub noloadbalance: bool,
    pub replication_lag_bytes: u64,
    pub timeline: u64,
    pub pending_restart: bool,
    pub pause: bool,
    pub last_ha_tick: Instant,
    pub user_tags: BTreeMap<CompactString, CompactString>,
}

impl PublishedNodeState {
    pub fn initial(now: Instant) -> Self {
        Self {
            state: CompactString::const_new("starting"),
            role: CompactString::const_new("uninitialized"),
            is_read_write_valid: false,
            is_sync_standby: false,
            is_quorum_standby: false,
            noloadbalance: false,
            replication_lag_bytes: 0,
            timeline: 0,
            pending_restart: false,
            pause: false,
            last_ha_tick: now,
            user_tags: BTreeMap::new(),
        }
    }
}

pub type SharedNodeState = Arc<ArcSwap<PublishedNodeState>>;

pub const fn aws_callbacks_enabled() -> bool {
    cfg!(feature = "aws-callbacks")
}

pub async fn run_daemon(cfg: DaemonConfig, watchdog: WatchdogHandle) -> ExitCode {
    let shared: SharedNodeState = Arc::new(ArcSwap::from_pointee(PublishedNodeState::initial(
        Instant::now(),
    )));
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
