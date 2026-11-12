use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use compact_str::CompactString;
use negroni_dcs::DcsBackend;
use negroni_types::{ClusterStatus, LeaseHandle, LeaseProof};
use negroni_watchdog::{WatchdogArmError, WatchdogHandle, notify_systemd};
use tokio::sync::{mpsc, oneshot};

use crate::election::PeerElectionSample;
use crate::jobs::{JobRunner, JobSupervisor};
use crate::reducer::{Effect, EffectBatch, HaEvent};

/// Port implemented by `negroni-pg` (Phase 2) to execute local PostgreSQL process, GUC,
/// promotion, emergency demotion, and slot synchronization effects.
pub trait PgControlPort: Send + Sync + 'static {
    fn start_as_replica(&self) -> impl Future<Output = Result<(), String>> + Send;
    fn promote(&self) -> impl Future<Output = Result<(), String>> + Send;
    fn demote(&self, immediate_at: Instant) -> impl Future<Output = Result<(), String>> + Send;
    fn reconfigure_standby(
        &self,
        primary_conninfo: &str,
        slot_name: Option<&str>,
    ) -> impl Future<Output = Result<(), String>> + Send;
    fn set_synchronous_standby_names(
        &self,
        guc_value: &str,
    ) -> impl Future<Output = Result<(), String>> + Send;
    fn sync_slots(
        &self,
        status: &ClusterStatus,
        is_primary: bool,
        is_demoted_rejoin: bool,
    ) -> impl Future<Output = Result<(), String>> + Send;
}

/// Port for peer-to-peer REST calls (`GET /patroni` LSN election probes and `POST /failsafe`).
pub trait PeerRpcPort: Send + Sync + 'static {
    fn probe_election_peers(
        &self,
        peers: &[(CompactString, String)],
        timeout: Duration,
    ) -> impl Future<Output = Vec<PeerElectionSample>> + Send;

    fn probe_failsafe_peers(
        &self,
        peers: &[(CompactString, String)],
        timeout: Duration,
    ) -> impl Future<Output = bool> + Send;
}

/// Port abstracting hardware/software watchdog operations so tests and actor shells decouple cleanly.
pub trait WatchdogPort: Send + Sync + 'static {
    fn arm_and_feed(&self, proof: LeaseProof) -> Result<Option<Duration>, WatchdogArmError>;
    fn keepalive(&self, proof: LeaseProof) -> bool;
    fn disarm(&self) -> bool;
}

impl WatchdogPort for WatchdogHandle {
    fn arm_and_feed(&self, proof: LeaseProof) -> Result<Option<Duration>, WatchdogArmError> {
        Self::arm_and_feed(self, proof)
    }

    fn keepalive(&self, proof: LeaseProof) -> bool {
        Self::keepalive(self, proof)
    }

    fn disarm(&self) -> bool {
        Self::disarm(self)
    }
}

/// Default stub ports used when running standalone startup smoke checks prior to Phase 2/3 wiring.
#[derive(Debug, Default)]
pub struct NoopPgControlPort;

impl PgControlPort for NoopPgControlPort {
    async fn start_as_replica(&self) -> Result<(), String> {
        Ok(())
    }
    async fn promote(&self) -> Result<(), String> {
        Ok(())
    }
    async fn demote(&self, _immediate_at: Instant) -> Result<(), String> {
        Ok(())
    }
    async fn reconfigure_standby(
        &self,
        _primary_conninfo: &str,
        _slot_name: Option<&str>,
    ) -> Result<(), String> {
        Ok(())
    }
    async fn set_synchronous_standby_names(&self, _guc_value: &str) -> Result<(), String> {
        Ok(())
    }
    async fn sync_slots(
        &self,
        _status: &ClusterStatus,
        _is_primary: bool,
        _is_demoted_rejoin: bool,
    ) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct NoopPeerRpcPort;

impl PeerRpcPort for NoopPeerRpcPort {
    async fn probe_election_peers(
        &self,
        _peers: &[(CompactString, String)],
        _timeout: Duration,
    ) -> Vec<PeerElectionSample> {
        Vec::new()
    }

    async fn probe_failsafe_peers(
        &self,
        _peers: &[(CompactString, String)],
        _timeout: Duration,
    ) -> bool {
        false
    }
}

/// Executes side-effects emitted by `NodeCtx::step` asynchronously outside `tokio::select!`,
/// anchoring all lease renewal timestamps strictly BEFORE network I/O begins.
pub struct EffectDispatcher<D, P, R, J, W>
where
    D: DcsBackend,
    P: PgControlPort,
    R: PeerRpcPort,
    J: JobRunner,
    W: WatchdogPort,
{
    self_name: CompactString,
    dcs: Arc<D>,
    pg: Arc<P>,
    peers: Arc<R>,
    watchdog: Arc<W>,
    jobs: JobSupervisor<J>,
    event_tx: mpsc::Sender<HaEvent>,
    pending_failsafe_reply: Mutex<Option<oneshot::Sender<bool>>>,
    last_member_lease: Option<LeaseHandle>,
}

impl<D, P, R, J, W> EffectDispatcher<D, P, R, J, W>
where
    D: DcsBackend,
    P: PgControlPort,
    R: PeerRpcPort,
    J: JobRunner,
    W: WatchdogPort,
{
    pub fn new(
        self_name: CompactString,
        dcs: Arc<D>,
        pg: Arc<P>,
        peers: Arc<R>,
        watchdog: Arc<W>,
        job_runner: Arc<J>,
        event_tx: mpsc::Sender<HaEvent>,
    ) -> Self {
        let jobs = JobSupervisor::new(job_runner, event_tx.clone());
        Self {
            self_name,
            dcs,
            pg,
            peers,
            watchdog,
            jobs,
            event_tx,
            pending_failsafe_reply: Mutex::new(None),
            last_member_lease: None,
        }
    }

    pub fn register_failsafe_reply(&self, reply: oneshot::Sender<bool>) {
        if let Ok(mut slot) = self.pending_failsafe_reply.lock() {
            *slot = Some(reply);
        }
    }

    pub fn shutdown_jobs(&mut self) {
        self.jobs.cancel_all();
    }

    pub fn dispatch_batch(&mut self, effects: EffectBatch) {
        for effect in effects {
            self.dispatch_one(effect);
        }
    }

    fn dispatch_one(&mut self, effect: Effect) {
        match effect {
            Effect::ArmAndFeedWatchdog { state_epoch, proof } => {
                let wd = Arc::clone(&self.watchdog);
                let tx = self.event_tx.clone();
                tokio::task::spawn_blocking(move || {
                    let now = Instant::now();
                    let event = match wd.arm_and_feed(proof) {
                        Ok(_) => HaEvent::WatchdogArmed { state_epoch, now },
                        Err(err) => {
                            tracing::error!(
                                state_epoch,
                                error = %err,
                                "failed to arm hardware watchdog before primary promotion"
                            );
                            HaEvent::WatchdogArmFailed { state_epoch, now }
                        }
                    };
                    let _ = tx.blocking_send(event);
                });
            }

            Effect::FeedWatchdog(proof) => {
                let _ = self.watchdog.keepalive(proof);
            }

            Effect::DisarmWatchdog => {
                let _ = self.watchdog.disarm();
            }

            Effect::FetchDcs { rpc_epoch, timeout } => {
                let dcs = Arc::clone(&self.dcs);
                let tx = self.event_tx.clone();
                tokio::spawn(async move {
                    let attempt_started_at = Instant::now();
                    let res = match tokio::time::timeout(timeout, dcs.fetch_cluster(timeout)).await
                    {
                        Ok(Ok(snap)) => Ok(Box::new(snap)),
                        Ok(Err(err)) => Err(err.to_string()),
                        Err(_) => Err(format!("DCS fetch timed out after {timeout:?}")),
                    };
                    let _ = tx
                        .send(HaEvent::DcsFetched {
                            rpc_epoch,
                            attempt_started_at,
                            now: Instant::now(),
                            result: res,
                        })
                        .await;
                });
            }

            Effect::CasInitialize { rpc_epoch, sys_id } => {
                let dcs = Arc::clone(&self.dcs);
                let tx = self.event_tx.clone();
                tokio::spawn(async move {
                    let acquired = match dcs.cas_initialize(&sys_id).await {
                        Ok(_) => true,
                        Err(err) => {
                            tracing::debug!(error = %err, "cas_initialize lost or already initialized");
                            false
                        }
                    };
                    let _ = tx
                        .send(HaEvent::InitializeResult {
                            rpc_epoch,
                            now: Instant::now(),
                            acquired,
                        })
                        .await;
                });
            }

            Effect::ProbeElectionPeers {
                rpc_epoch,
                peers,
                timeout,
            } => {
                let rpc = Arc::clone(&self.peers);
                let tx = self.event_tx.clone();
                tokio::spawn(async move {
                    let samples = rpc.probe_election_peers(&peers, timeout).await;
                    let _ = tx
                        .send(HaEvent::ElectionPeersProbed {
                            rpc_epoch,
                            now: Instant::now(),
                            samples,
                        })
                        .await;
                });
            }

            Effect::RenewOrAcquireLeader {
                rpc_epoch,
                lease,
                expected,
                ttl,
            } => {
                let dcs = Arc::clone(&self.dcs);
                let self_name = self.self_name.clone();
                let tx = self.event_tx.clone();
                tokio::spawn(async move {
                    // Pre-Flight Anchor Rule: measure `attempt_started_at` BEFORE network RTT!
                    let attempt_started_at = Instant::now();
                    let outcome = async {
                        let ensured_lease = dcs
                            .ensure_lease(ttl, lease.as_ref())
                            .await
                            .map_err(|e| e.to_string())?;
                        let token = dcs
                            .acquire_or_renew_leader(&self_name, &ensured_lease, expected.as_ref())
                            .await
                            .map_err(|e| e.to_string())?;
                        Ok((ensured_lease, token))
                    }
                    .await;

                    let _ = tx
                        .send(HaEvent::LeaderLeaseResult {
                            rpc_epoch,
                            attempt_started_at,
                            now: Instant::now(),
                            result: outcome,
                        })
                        .await;
                });
            }

            Effect::ReleaseLeader { expected } => {
                let dcs = Arc::clone(&self.dcs);
                let self_name = self.self_name.clone();
                tokio::spawn(async move {
                    if let Err(err) = dcs.release_leader(&self_name, &expected).await {
                        tracing::warn!(error = %err, "failed to release /leader key cleanly");
                    }
                });
            }

            Effect::ClearFailover { expected } => {
                let dcs = Arc::clone(&self.dcs);
                tokio::spawn(async move {
                    if let Err(err) = dcs.clear_failover(&expected).await {
                        tracing::debug!(error = %err, "failed to clear /failover intent");
                    }
                });
            }

            Effect::UpsertMember { data, lease } => {
                if let Some(ref l) = lease {
                    self.last_member_lease = Some(l.clone());
                }
                let effective_lease = lease.or_else(|| self.last_member_lease.clone());
                let dcs = Arc::clone(&self.dcs);
                let self_name = self.self_name.clone();
                let ttl = Duration::from_secs(30);
                tokio::spawn(async move {
                    let lse = match effective_lease {
                        Some(existing) => existing,
                        None => match dcs.ensure_lease(ttl, None).await {
                            Ok(fresh) => fresh,
                            Err(err) => {
                                tracing::debug!(error = %err, "skipping member upsert: lease unavailable");
                                return;
                            }
                        },
                    };
                    if let Err(err) = dcs.upsert_member(&self_name, &data, &lse).await {
                        tracing::debug!(error = %err, "failed to upsert /members/{self_name}");
                    }
                });
            }

            Effect::PublishLeaderStatus {
                status,
                failsafe_roster,
            } => {
                let dcs = Arc::clone(&self.dcs);
                let tx = self.event_tx.clone();
                tokio::spawn(async move {
                    match dcs
                        .publish_leader_state(&status, failsafe_roster.as_ref())
                        .await
                    {
                        Ok(()) => {
                            if let Some(roster) = failsafe_roster {
                                let _ = tx
                                    .send(HaEvent::FailsafeRosterPublished {
                                        now: Instant::now(),
                                        roster,
                                    })
                                    .await;
                            }
                        }
                        Err(err) => {
                            tracing::warn!(error = %err, "failed to publish /status or /failsafe");
                        }
                    }
                });
            }

            Effect::AppendHistory { entry } => {
                let dcs = Arc::clone(&self.dcs);
                tokio::spawn(async move {
                    if let Err(err) = dcs.append_history_entry(&entry).await {
                        tracing::warn!(error = %err, "failed to append /history entry");
                    }
                });
            }

            Effect::UpdateSyncState { next, expected } => {
                let dcs = Arc::clone(&self.dcs);
                tokio::spawn(async move {
                    if let Err(err) = dcs.cas_sync_state(&next, expected.as_ref()).await {
                        tracing::warn!(error = %err, "failed to CAS /sync state");
                    }
                });
            }

            Effect::ReconfigureSynchronousStandbys { guc_value } => {
                let pg = Arc::clone(&self.pg);
                tokio::spawn(async move {
                    if let Err(err) = pg.set_synchronous_standby_names(&guc_value).await {
                        tracing::warn!(error = %err, "failed to apply synchronous_standby_names");
                    }
                });
            }

            Effect::ProbeFailsafePeers {
                rpc_epoch,
                peers,
                timeout,
            } => {
                let rpc = Arc::clone(&self.peers);
                let tx = self.event_tx.clone();
                tokio::spawn(async move {
                    let attempt_started_at = Instant::now();
                    let accepted = rpc.probe_failsafe_peers(&peers, timeout).await;
                    let _ = tx
                        .send(HaEvent::FailsafeQuorumResult {
                            rpc_epoch,
                            attempt_started_at,
                            now: Instant::now(),
                            all_peers_accepted: accepted,
                        })
                        .await;
                });
            }

            Effect::RespondFailsafeHeartbeat { accepted } => {
                if let Ok(mut slot) = self.pending_failsafe_reply.lock()
                    && let Some(reply_tx) = slot.take()
                {
                    let _ = reply_tx.send(accepted);
                }
            }

            Effect::StartPostgresAsReplica => {
                let pg = Arc::clone(&self.pg);
                tokio::spawn(async move {
                    if let Err(err) = pg.start_as_replica().await {
                        tracing::warn!(error = %err, "failed to start PostgreSQL as replica");
                    }
                });
            }

            Effect::PromotePostgres { state_epoch } => {
                let pg = Arc::clone(&self.pg);
                let tx = self.event_tx.clone();
                tokio::spawn(async move {
                    let success = match pg.promote().await {
                        Ok(()) => true,
                        Err(err) => {
                            tracing::error!(state_epoch, error = %err, "pg_promote() failed");
                            false
                        }
                    };
                    let _ = tx
                        .send(HaEvent::PgPromotionCompleted {
                            state_epoch,
                            now: Instant::now(),
                            success,
                        })
                        .await;
                });
            }

            Effect::DemotePostgres {
                state_epoch,
                immediate_at,
            } => {
                let pg = Arc::clone(&self.pg);
                let tx = self.event_tx.clone();
                tokio::spawn(async move {
                    if let Err(err) = pg.demote(immediate_at).await {
                        tracing::error!(state_epoch, error = %err, "emergency PostgreSQL demotion reported error");
                    }
                    let _ = tx
                        .send(HaEvent::PgDemotionCompleted {
                            state_epoch,
                            now: Instant::now(),
                        })
                        .await;
                });
            }

            Effect::ReconfigureStandby {
                primary_conninfo,
                slot_name,
            } => {
                let pg = Arc::clone(&self.pg);
                tokio::spawn(async move {
                    if let Err(err) = pg
                        .reconfigure_standby(&primary_conninfo, slot_name.as_deref())
                        .await
                    {
                        tracing::warn!(error = %err, "failed to reconfigure standby primary_conninfo");
                    }
                });
            }

            Effect::SyncSlots {
                status,
                is_primary,
                is_demoted_rejoin,
            } => {
                let pg = Arc::clone(&self.pg);
                tokio::spawn(async move {
                    if let Err(err) = pg.sync_slots(&status, is_primary, is_demoted_rejoin).await {
                        tracing::warn!(error = %err, "replication slot reconciliation failed");
                    }
                });
            }

            Effect::SpawnJob {
                state_epoch,
                job,
                target_conn,
            } => {
                self.jobs.spawn(state_epoch, job, target_conn);
            }

            Effect::CancelActiveJobs => {
                self.jobs.cancel_all();
            }

            Effect::NotifySystemd(status) => {
                notify_systemd(status);
            }
        }
    }
}
