use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use compact_str::CompactString;
use negroni_dcs::{DcsBackend, DcsError, DcsWatchEvent};
use negroni_types::{CasToken, ClusterSnapshot, FailoverIntent, WalLsn};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::AbortHandle;
use tokio::time::MissedTickBehavior;

use crate::dispatcher::{EffectDispatcher, PeerRpcPort, PgControlPort, WatchdogPort};
use crate::jobs::JobRunner;
use crate::reducer::{ControlCommand, HaEvent, NodeCtx, PublishedNodeState};

pub const DEFAULT_EVENT_CHANNEL_CAPACITY: usize = 256;
pub const DEFAULT_FAILSAFE_CHANNEL_CAPACITY: usize = 32;

pub type SharedNodeState = Arc<ArcSwap<PublishedNodeState>>;
pub type SharedClusterCache = Arc<ArcSwap<ClusterSnapshot>>;
pub type ControlBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone)]
pub struct FailsafeHeartbeatRequest {
    pub leader_name: CompactString,
    pub conn_url: String,
    pub lsn: WalLsn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduledRestartInfo {
    pub schedule: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<CompactString>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub postgres_version: Option<CompactString>,
    #[serde(default)]
    pub restart_pending: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u64>,
}

#[derive(Debug, Error)]
pub enum ControlError {
    #[error("HA coordinator channel is closed")]
    CoordinatorStopped,
    #[error("HA coordinator queue is full")]
    QueueFull,
    #[error("timed out waiting for HA coordinator response")]
    Timeout,
}

#[derive(Debug, Error)]
pub enum ControlOpError {
    #[error("CAS conflict on DCS key")]
    CasConflict,
    #[error("conflicting operation already scheduled: {0}")]
    Conflict(String),
    #[error("invalid control request: {0}")]
    InvalidInput(String),
    #[error("precondition failed: {0}")]
    PreconditionFailed(String),
    #[error("service unavailable: {0}")]
    Unavailable(String),
}

fn map_dcs_error(err: DcsError) -> ControlOpError {
    match err {
        DcsError::CasConflict { .. } => ControlOpError::CasConflict,
        DcsError::LeaseExpired | DcsError::Unreachable(_) => {
            ControlOpError::Unavailable(err.to_string())
        }
    }
}

/// Object-safe control-plane port bridging `negroni-api` mutating endpoints to DCS and PostgreSQL
/// without leaking `negroni-dcs` or `tokio-postgres` upward into `negroni-api`.
pub trait ControlPlaneOps: Send + Sync + 'static {
    fn fetch_fresh_cluster(
        &self,
        timeout: Duration,
    ) -> ControlBoxFuture<'_, Result<ClusterSnapshot, ControlOpError>>;

    fn cas_config(
        &self,
        config_json: serde_json::Value,
        expected: Option<CasToken>,
    ) -> ControlBoxFuture<'_, Result<CasToken, ControlOpError>>;

    fn cas_failover(
        &self,
        intent: FailoverIntent,
        expected: Option<CasToken>,
    ) -> ControlBoxFuture<'_, Result<CasToken, ControlOpError>>;

    fn clear_failover(
        &self,
        expected: CasToken,
    ) -> ControlBoxFuture<'_, Result<(), ControlOpError>>;

    fn restart_local_postgres(
        &self,
        in_recovery: bool,
        timeout: Duration,
    ) -> ControlBoxFuture<'_, Result<(), ControlOpError>>;

    fn reload_local_node(&self) -> ControlBoxFuture<'_, Result<(), ControlOpError>>;
}

/// Default in-memory control plane used when `CoordinatorHandle::new` is constructed in standalone unit tests.
#[derive(Debug, Default)]
pub struct NoopControlPlane {
    cluster: SharedClusterCache,
}

impl NoopControlPlane {
    pub fn with_cache(cluster: SharedClusterCache) -> Self {
        Self { cluster }
    }
}

impl ControlPlaneOps for NoopControlPlane {
    fn fetch_fresh_cluster(
        &self,
        _timeout: Duration,
    ) -> ControlBoxFuture<'_, Result<ClusterSnapshot, ControlOpError>> {
        Box::pin(async move { Ok((**self.cluster.load()).clone()) })
    }

    fn cas_config(
        &self,
        config_json: serde_json::Value,
        expected: Option<CasToken>,
    ) -> ControlBoxFuture<'_, Result<CasToken, ControlOpError>> {
        Box::pin(async move {
            let mut snap = (**self.cluster.load()).clone();
            let current_tok = snap.config.as_ref().map(|v| &v.token);
            if expected.is_some() && current_tok != expected.as_ref() {
                return Err(ControlOpError::CasConflict);
            }
            let next_rev = match current_tok {
                Some(CasToken::Revision(r)) => r + 1,
                _ => 1,
            };
            let tok = CasToken::Revision(next_rev);
            snap.config = Some(negroni_types::Versioned {
                value: config_json,
                token: tok.clone(),
            });
            self.cluster.store(Arc::new(snap));
            Ok(tok)
        })
    }

    fn cas_failover(
        &self,
        intent: FailoverIntent,
        expected: Option<CasToken>,
    ) -> ControlBoxFuture<'_, Result<CasToken, ControlOpError>> {
        Box::pin(async move {
            let mut snap = (**self.cluster.load()).clone();
            let current_tok = snap.failover.as_ref().map(|v| &v.token);
            if expected.is_some() && current_tok != expected.as_ref() {
                return Err(ControlOpError::CasConflict);
            }
            let next_rev = match current_tok {
                Some(CasToken::Revision(r)) => r + 1,
                _ => 1,
            };
            let tok = CasToken::Revision(next_rev);
            snap.failover = Some(negroni_types::Versioned {
                value: intent,
                token: tok.clone(),
            });
            self.cluster.store(Arc::new(snap));
            Ok(tok)
        })
    }

    fn clear_failover(
        &self,
        _expected: CasToken,
    ) -> ControlBoxFuture<'_, Result<(), ControlOpError>> {
        Box::pin(async move {
            let mut snap = (**self.cluster.load()).clone();
            snap.failover = None;
            self.cluster.store(Arc::new(snap));
            Ok(())
        })
    }

    fn restart_local_postgres(
        &self,
        _in_recovery: bool,
        _timeout: Duration,
    ) -> ControlBoxFuture<'_, Result<(), ControlOpError>> {
        Box::pin(async { Ok(()) })
    }

    fn reload_local_node(&self) -> ControlBoxFuture<'_, Result<(), ControlOpError>> {
        Box::pin(async { Ok(()) })
    }
}

/// Live control plane backed by `D: DcsBackend` and `P: PgControlPort`.
pub struct LiveControlPlane<D, P> {
    dcs: Arc<D>,
    pg: Arc<P>,
    cluster_cache: SharedClusterCache,
}

impl<D, P> LiveControlPlane<D, P>
where
    D: DcsBackend,
    P: PgControlPort,
{
    pub fn new(dcs: Arc<D>, pg: Arc<P>, cluster_cache: SharedClusterCache) -> Self {
        Self {
            dcs,
            pg,
            cluster_cache,
        }
    }
}

impl<D, P> ControlPlaneOps for LiveControlPlane<D, P>
where
    D: DcsBackend,
    P: PgControlPort,
{
    fn fetch_fresh_cluster(
        &self,
        timeout: Duration,
    ) -> ControlBoxFuture<'_, Result<ClusterSnapshot, ControlOpError>> {
        Box::pin(async move {
            let snap = self
                .dcs
                .fetch_cluster(timeout)
                .await
                .map_err(map_dcs_error)?;
            self.cluster_cache.store(Arc::new(snap.clone()));
            Ok(snap)
        })
    }

    fn cas_config(
        &self,
        config_json: serde_json::Value,
        expected: Option<CasToken>,
    ) -> ControlBoxFuture<'_, Result<CasToken, ControlOpError>> {
        Box::pin(async move {
            self.dcs
                .cas_config(&config_json, expected.as_ref())
                .await
                .map_err(map_dcs_error)
        })
    }

    fn cas_failover(
        &self,
        intent: FailoverIntent,
        expected: Option<CasToken>,
    ) -> ControlBoxFuture<'_, Result<CasToken, ControlOpError>> {
        Box::pin(async move {
            self.dcs
                .cas_failover(&intent, expected.as_ref())
                .await
                .map_err(map_dcs_error)
        })
    }

    fn clear_failover(
        &self,
        expected: CasToken,
    ) -> ControlBoxFuture<'_, Result<(), ControlOpError>> {
        Box::pin(async move {
            self.dcs
                .clear_failover(&expected)
                .await
                .map_err(map_dcs_error)
        })
    }

    fn restart_local_postgres(
        &self,
        in_recovery: bool,
        timeout: Duration,
    ) -> ControlBoxFuture<'_, Result<(), ControlOpError>> {
        Box::pin(async move {
            self.pg
                .restart_node(in_recovery, timeout)
                .await
                .map_err(ControlOpError::Unavailable)
        })
    }

    fn reload_local_node(&self) -> ControlBoxFuture<'_, Result<(), ControlOpError>> {
        Box::pin(async move {
            self.pg
                .reload_node()
                .await
                .map_err(ControlOpError::Unavailable)
        })
    }
}

struct ScheduledRestartSlot {
    info: Option<ScheduledRestartInfo>,
    abort_handle: Option<AbortHandle>,
}

/// Lock-free read + bounded control handle shared with `negroni-api`.
#[derive(Clone)]
pub struct CoordinatorHandle {
    published: SharedNodeState,
    cluster_cache: SharedClusterCache,
    scheduled_restart: Arc<ArcSwap<Option<ScheduledRestartInfo>>>,
    scheduled_restart_slot: Arc<Mutex<ScheduledRestartSlot>>,
    control_ops: Arc<dyn ControlPlaneOps>,
    event_tx: mpsc::Sender<HaEvent>,
    failsafe_tx: mpsc::Sender<(FailsafeHeartbeatRequest, oneshot::Sender<bool>)>,
}

impl CoordinatorHandle {
    pub fn new(
        published: SharedNodeState,
        event_tx: mpsc::Sender<HaEvent>,
        failsafe_tx: mpsc::Sender<(FailsafeHeartbeatRequest, oneshot::Sender<bool>)>,
    ) -> Self {
        let cluster_cache = Arc::new(ArcSwap::from_pointee(ClusterSnapshot::default()));
        let control_ops = Arc::new(NoopControlPlane::with_cache(Arc::clone(&cluster_cache)));
        Self {
            published,
            cluster_cache,
            scheduled_restart: Arc::new(ArcSwap::from_pointee(None)),
            scheduled_restart_slot: Arc::new(Mutex::new(ScheduledRestartSlot {
                info: None,
                abort_handle: None,
            })),
            control_ops,
            event_tx,
            failsafe_tx,
        }
    }

    #[must_use]
    pub fn with_control_plane(
        mut self,
        cluster_cache: SharedClusterCache,
        control_ops: Arc<dyn ControlPlaneOps>,
    ) -> Self {
        self.cluster_cache = cluster_cache;
        self.control_ops = control_ops;
        self
    }

    /// ~15ns zero-lock snapshot load for `/primary`, `/replica`, `/read-write`, `/synchronous`, etc.
    pub fn load_published_state(&self) -> arc_swap::Guard<Arc<PublishedNodeState>> {
        self.published.load()
    }

    /// ~15ns zero-lock load of the most recently fetched [`ClusterSnapshot`].
    pub fn load_cached_cluster(&self) -> arc_swap::Guard<Arc<ClusterSnapshot>> {
        self.cluster_cache.load()
    }

    /// ~15ns zero-lock load of any pending scheduled restart metadata.
    pub fn load_scheduled_restart(&self) -> arc_swap::Guard<Arc<Option<ScheduledRestartInfo>>> {
        self.scheduled_restart.load()
    }

    pub fn shared_state(&self) -> &SharedNodeState {
        &self.published
    }

    pub fn shared_cluster_cache(&self) -> &SharedClusterCache {
        &self.cluster_cache
    }

    pub async fn submit_operator_command(&self, cmd: ControlCommand) -> Result<(), ControlError> {
        self.event_tx
            .send(HaEvent::OperatorCommand {
                now: Instant::now(),
                cmd,
            })
            .await
            .map_err(|_| ControlError::CoordinatorStopped)
    }

    pub async fn submit_failsafe_heartbeat(
        &self,
        req: FailsafeHeartbeatRequest,
        timeout: Duration,
    ) -> Result<bool, ControlError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.failsafe_tx
            .try_send((req, reply_tx))
            .map_err(|err| match err {
                mpsc::error::TrySendError::Full(_) => ControlError::QueueFull,
                mpsc::error::TrySendError::Closed(_) => ControlError::CoordinatorStopped,
            })?;

        match tokio::time::timeout(timeout, reply_rx).await {
            Ok(Ok(accepted)) => Ok(accepted),
            Ok(Err(_)) => Err(ControlError::CoordinatorStopped),
            Err(_) => Err(ControlError::Timeout),
        }
    }

    pub async fn fetch_fresh_cluster(
        &self,
        timeout: Duration,
    ) -> Result<ClusterSnapshot, ControlOpError> {
        self.control_ops.fetch_fresh_cluster(timeout).await
    }

    pub async fn cas_config(
        &self,
        config_json: serde_json::Value,
        expected: Option<CasToken>,
    ) -> Result<CasToken, ControlOpError> {
        let tok = self.control_ops.cas_config(config_json, expected).await?;
        let _ = self.event_tx.try_send(HaEvent::WatchWakeup {
            now: Instant::now(),
        });
        Ok(tok)
    }

    pub async fn cas_failover(
        &self,
        intent: FailoverIntent,
        expected: Option<CasToken>,
    ) -> Result<CasToken, ControlOpError> {
        let tok = self.control_ops.cas_failover(intent, expected).await?;
        let _ = self.event_tx.try_send(HaEvent::WatchWakeup {
            now: Instant::now(),
        });
        Ok(tok)
    }

    pub async fn clear_failover(&self, expected: CasToken) -> Result<(), ControlOpError> {
        self.control_ops.clear_failover(expected).await?;
        let _ = self.event_tx.try_send(HaEvent::WatchWakeup {
            now: Instant::now(),
        });
        Ok(())
    }

    pub async fn restart_local_postgres(
        &self,
        in_recovery: bool,
        timeout: Duration,
    ) -> Result<(), ControlOpError> {
        self.control_ops
            .restart_local_postgres(in_recovery, timeout)
            .await
    }

    pub async fn reload_local_node(&self) -> Result<(), ControlOpError> {
        self.control_ops.reload_local_node().await?;
        let _ = self.submit_operator_command(ControlCommand::Reload).await;
        Ok(())
    }

    /// Registers a future scheduled restart (`POST /restart` with `"schedule"`), returning
    /// [`ControlOpError::Conflict`] (`409`) if another restart is already scheduled unless `flush` is `true`.
    pub fn schedule_restart(
        &self,
        info: ScheduledRestartInfo,
        delay: Duration,
        flush_existing: bool,
    ) -> Result<(), ControlOpError> {
        let mut guard = self
            .scheduled_restart_slot
            .lock()
            .map_err(|_| ControlOpError::Unavailable("scheduled restart lock poisoned".into()))?;
        if guard.info.is_some() && !flush_existing {
            return Err(ControlOpError::Conflict(
                "Restart is already scheduled".to_owned(),
            ));
        }
        if let Some(prev_abort) = guard.abort_handle.take() {
            prev_abort.abort();
        }

        let handle_clone = self.clone();
        let info_for_task = info.clone();
        let join = tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let snap = handle_clone.load_published_state();
            let role_matches = info_for_task
                .role
                .as_ref()
                .is_none_or(|r| r.as_str() == snap.role.as_str());
            let pending_ok = !info_for_task.restart_pending || snap.pending_restart;
            let in_recovery = snap.role != "primary";
            drop(snap);

            if role_matches && pending_ok {
                let stop_timeout = Duration::from_secs(info_for_task.timeout.unwrap_or(60));
                let _ = handle_clone
                    .restart_local_postgres(in_recovery, stop_timeout)
                    .await;
            }

            if let Ok(mut slot) = handle_clone.scheduled_restart_slot.lock() {
                slot.info = None;
                slot.abort_handle = None;
            }
            handle_clone.scheduled_restart.store(Arc::new(None));
        });

        guard.info = Some(info.clone());
        guard.abort_handle = Some(join.abort_handle());
        self.scheduled_restart.store(Arc::new(Some(info)));
        Ok(())
    }

    /// Cancels any scheduled restart (`DELETE /restart`), returning `true` if a schedule existed.
    pub fn cancel_scheduled_restart(&self) -> bool {
        let Ok(mut guard) = self.scheduled_restart_slot.lock() else {
            return false;
        };
        let existed = guard.info.take().is_some();
        if let Some(abort) = guard.abort_handle.take() {
            abort.abort();
        }
        self.scheduled_restart.store(Arc::new(None));
        existed
    }
}

/// Strictly cancel-safe HA coordinator event loop (`AGENTS.md` §2.2).
///
/// Only cancel-safe channel receives (`mpsc::Receiver::recv`, `watch::Receiver::changed`)
/// and `tokio::time::Interval::tick` are polled inside `tokio::select!`.
pub async fn coordinator_loop<D, P, R, J, W>(
    mut ctx: NodeCtx,
    mut event_rx: mpsc::Receiver<HaEvent>,
    mut failsafe_rx: mpsc::Receiver<(FailsafeHeartbeatRequest, oneshot::Sender<bool>)>,
    mut watch_rx: mpsc::Receiver<DcsWatchEvent>,
    mut shutdown_rx: watch::Receiver<bool>,
    published_state: SharedNodeState,
    mut dispatcher: EffectDispatcher<D, P, R, J, W>,
) where
    D: DcsBackend,
    P: PgControlPort,
    R: PeerRpcPort,
    J: JobRunner,
    W: WatchdogPort,
{
    let mut current_period = ctx.loop_wait.max(Duration::from_millis(50));
    let mut ticker = tokio::time::interval(current_period);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        let ev = tokio::select! {
            biased;
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    tracing::info!(member = %ctx.self_name, "coordinator loop received shutdown signal");
                    break;
                }
                continue;
            }
            Some((fs_req, reply_tx)) = failsafe_rx.recv() => {
                dispatcher.register_failsafe_reply(reply_tx);
                HaEvent::InboundFailsafeHeartbeat {
                    now: Instant::now(),
                    leader_name: fs_req.leader_name,
                    conn_url: fs_req.conn_url,
                    lsn: fs_req.lsn,
                }
            }
            Some(ha_ev) = event_rx.recv() => ha_ev,
            Some(_watch_ev) = watch_rx.recv() => HaEvent::WatchWakeup {
                now: Instant::now(),
            },
            _ = ticker.tick() => HaEvent::Tick {
                now: Instant::now(),
            },
        };

        let (effects, snapshot) = ctx.step(ev);
        published_state.store(Arc::new(snapshot));
        dispatcher.dispatch_batch(effects);

        // Adapt interval dynamically if DCS `/config` updated `loop_wait`.
        let desired_period = ctx.loop_wait.max(Duration::from_millis(50));
        if desired_period != current_period {
            current_period = desired_period;
            ticker = tokio::time::interval_at(
                tokio::time::Instant::now() + current_period,
                current_period,
            );
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        }
    }

    dispatcher.shutdown_jobs();
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use negroni_config::parse_and_validate_with_env;
    use negroni_dcs::DcsError;
    use negroni_types::{
        CasToken, ClusterSnapshot, ClusterStatus, FailoverIntent, HistoryEntry, LeaseHandle,
        LeaseProof, MemberData, SyncState,
    };
    use negroni_watchdog::WatchdogArmError;

    use super::*;
    use crate::dispatcher::{NoopPeerRpcPort, NoopPgControlPort};
    use crate::jobs::NoopJobRunner;
    use crate::reducer::LocalPgSummary;

    #[derive(Default)]
    struct InMemoryTestDcs {
        snapshot: Mutex<ClusterSnapshot>,
    }

    impl DcsBackend for InMemoryTestDcs {
        async fn fetch_cluster(&self, _timeout: Duration) -> Result<ClusterSnapshot, DcsError> {
            Ok(self.snapshot.lock().unwrap().clone())
        }

        async fn ensure_lease(
            &self,
            _ttl: Duration,
            current: Option<&LeaseHandle>,
        ) -> Result<LeaseHandle, DcsError> {
            Ok(current
                .cloned()
                .unwrap_or_else(|| LeaseHandle(CompactString::const_new("lease-1"))))
        }

        async fn cas_initialize(&self, sys_id: &str) -> Result<CasToken, DcsError> {
            let mut g = self.snapshot.lock().unwrap();
            let tok = CasToken::Revision(1);
            g.initialize = Some(negroni_types::Versioned {
                value: CompactString::from(sys_id),
                token: tok.clone(),
            });
            Ok(tok)
        }

        async fn acquire_or_renew_leader(
            &self,
            self_name: &str,
            _lease: &LeaseHandle,
            _expected: Option<&CasToken>,
        ) -> Result<CasToken, DcsError> {
            let mut g = self.snapshot.lock().unwrap();
            let tok = CasToken::Revision(2);
            g.leader = Some(negroni_types::Versioned {
                value: CompactString::from(self_name),
                token: tok.clone(),
            });
            Ok(tok)
        }

        async fn release_leader(
            &self,
            _self_name: &str,
            _expected: &CasToken,
        ) -> Result<(), DcsError> {
            self.snapshot.lock().unwrap().leader = None;
            Ok(())
        }

        async fn upsert_member(
            &self,
            self_name: &str,
            data: &MemberData,
            _lease: &LeaseHandle,
        ) -> Result<(), DcsError> {
            self.snapshot.lock().unwrap().members.insert(
                CompactString::from(self_name),
                negroni_types::Versioned {
                    value: data.clone(),
                    token: CasToken::Revision(1),
                },
            );
            Ok(())
        }

        async fn publish_leader_state(
            &self,
            status: &ClusterStatus,
            failsafe_roster: Option<&BTreeMap<CompactString, String>>,
        ) -> Result<(), DcsError> {
            let mut g = self.snapshot.lock().unwrap();
            g.status = Some(negroni_types::Versioned {
                value: status.clone(),
                token: CasToken::Revision(1),
            });
            g.failsafe = failsafe_roster.cloned();
            Ok(())
        }

        async fn append_history_entry(&self, entry: &HistoryEntry) -> Result<(), DcsError> {
            self.snapshot.lock().unwrap().history.push(entry.clone());
            Ok(())
        }

        async fn cas_sync_state(
            &self,
            next: &SyncState,
            _expected: Option<&CasToken>,
        ) -> Result<CasToken, DcsError> {
            let tok = CasToken::Revision(1);
            self.snapshot.lock().unwrap().sync = Some(negroni_types::Versioned {
                value: next.clone(),
                token: tok.clone(),
            });
            Ok(tok)
        }

        async fn cas_config(
            &self,
            config_json: &serde_json::Value,
            _expected: Option<&CasToken>,
        ) -> Result<CasToken, DcsError> {
            let tok = CasToken::Revision(1);
            self.snapshot.lock().unwrap().config = Some(negroni_types::Versioned {
                value: config_json.clone(),
                token: tok.clone(),
            });
            Ok(tok)
        }

        async fn cas_failover(
            &self,
            intent: &FailoverIntent,
            _expected: Option<&CasToken>,
        ) -> Result<CasToken, DcsError> {
            let tok = CasToken::Revision(1);
            self.snapshot.lock().unwrap().failover = Some(negroni_types::Versioned {
                value: intent.clone(),
                token: tok.clone(),
            });
            Ok(tok)
        }

        async fn clear_failover(&self, _expected: &CasToken) -> Result<(), DcsError> {
            self.snapshot.lock().unwrap().failover = None;
            Ok(())
        }

        fn subscribe_watches(&self) -> mpsc::Receiver<DcsWatchEvent> {
            let (_tx, rx) = mpsc::channel(4);
            rx
        }
    }

    #[derive(Default)]
    struct MockWd;

    impl WatchdogPort for MockWd {
        fn arm_and_feed(&self, _proof: LeaseProof) -> Result<Option<Duration>, WatchdogArmError> {
            Ok(Some(Duration::from_secs(25)))
        }
        fn keepalive(&self, _proof: LeaseProof) -> bool {
            true
        }
        fn disarm(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn coordinator_loop_promotes_healthy_node_and_publishes_arcswap_snapshot() {
        let yaml = "scope: demo\nname: pg-0\nbootstrap:\n  dcs:\n    ttl: 30\n    loop_wait: 10\n    retry_timeout: 10\n    failsafe_mode: true\n";
        let empty: [(&str, &str); 0] = [];
        let cfg = parse_and_validate_with_env(yaml, "patroni.yml", empty).unwrap();

        let boot = Instant::now();
        let mut ctx = NodeCtx::from_config(&cfg, boot);
        ctx.last_pg = LocalPgSummary {
            has_valid_pgdata: true,
            state_label: CompactString::const_new("running"),
            in_recovery: true,
            lsn: WalLsn(16_384),
            timeline: 1,
            ..Default::default()
        };

        let shared: SharedNodeState =
            Arc::new(ArcSwap::from_pointee(PublishedNodeState::initial(boot)));
        let (event_tx, event_rx) = mpsc::channel(DEFAULT_EVENT_CHANNEL_CAPACITY);
        let (fs_tx, fs_rx) = mpsc::channel(DEFAULT_FAILSAFE_CHANNEL_CAPACITY);
        let dcs = Arc::new(InMemoryTestDcs::default());
        let watch_rx = dcs.subscribe_watches();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let dispatcher = EffectDispatcher::new(
            ctx.self_name.clone(),
            Arc::clone(&dcs),
            Arc::new(NoopPgControlPort),
            Arc::new(NoopPeerRpcPort),
            Arc::new(MockWd),
            Arc::new(NoopJobRunner),
            event_tx.clone(),
        );

        let handle = CoordinatorHandle::new(Arc::clone(&shared), event_tx, fs_tx);
        let task = tokio::spawn(coordinator_loop(
            ctx,
            event_rx,
            fs_rx,
            watch_rx,
            shutdown_rx,
            Arc::clone(&shared),
            dispatcher,
        ));

        // Wait up to 500ms for initial tick -> DCS fetch -> leader acquire -> watchdog arm -> pg_promote -> RW valid!
        let deadline = tokio::time::Instant::now() + Duration::from_millis(500);
        loop {
            let snap = handle.load_published_state();
            if snap.role == "primary" && snap.is_read_write_valid {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for primary promotion; current={:?}",
                *snap
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Sending a conflicting failsafe heartbeat from another node to an active primary must be rejected!
        let accepted = handle
            .submit_failsafe_heartbeat(
                FailsafeHeartbeatRequest {
                    leader_name: CompactString::const_new("pg-rogue"),
                    conn_url: "postgres://rogue:5432/postgres".to_owned(),
                    lsn: WalLsn(99_999),
                },
                Duration::from_millis(200),
            )
            .await
            .unwrap();
        assert!(!accepted);

        let _ = shutdown_tx.send(true);
        task.await.unwrap();
    }
}
