use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use compact_str::CompactString;
use negroni_dcs::{DcsBackend, DcsWatchEvent};
use negroni_types::WalLsn;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::MissedTickBehavior;

use crate::dispatcher::{EffectDispatcher, PeerRpcPort, PgControlPort, WatchdogPort};
use crate::jobs::JobRunner;
use crate::reducer::{ControlCommand, HaEvent, NodeCtx, PublishedNodeState};

pub const DEFAULT_EVENT_CHANNEL_CAPACITY: usize = 256;
pub const DEFAULT_FAILSAFE_CHANNEL_CAPACITY: usize = 32;

pub type SharedNodeState = Arc<ArcSwap<PublishedNodeState>>;

#[derive(Debug, Clone)]
pub struct FailsafeHeartbeatRequest {
    pub leader_name: CompactString,
    pub conn_url: String,
    pub lsn: WalLsn,
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

/// Lock-free read + bounded control handle shared with `negroni-api`.
#[derive(Clone)]
pub struct CoordinatorHandle {
    published: SharedNodeState,
    event_tx: mpsc::Sender<HaEvent>,
    failsafe_tx: mpsc::Sender<(FailsafeHeartbeatRequest, oneshot::Sender<bool>)>,
}

impl CoordinatorHandle {
    pub fn new(
        published: SharedNodeState,
        event_tx: mpsc::Sender<HaEvent>,
        failsafe_tx: mpsc::Sender<(FailsafeHeartbeatRequest, oneshot::Sender<bool>)>,
    ) -> Self {
        Self {
            published,
            event_tx,
            failsafe_tx,
        }
    }

    /// ~15ns zero-lock snapshot load for `/primary`, `/replica`, `/read-write`, `/synchronous`, etc.
    pub fn load_published_state(&self) -> arc_swap::Guard<Arc<PublishedNodeState>> {
        self.published.load()
    }

    pub fn shared_state(&self) -> &SharedNodeState {
        &self.published
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
