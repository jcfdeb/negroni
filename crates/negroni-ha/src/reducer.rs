use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use compact_str::CompactString;
use negroni_config::{DaemonConfig, DynamicClusterConfig, SyncMode, validate_timings};
use negroni_types::{
    CasToken, ClusterSnapshot, ClusterStatus, HistoryEntry, LeaseHandle, LeaseProof, MemberData,
    SyncState, WalLsn,
};
use smallvec::SmallVec;

use crate::election::{
    LocalCandidateProfile, PeerElectionSample, PeerProbeContext, is_eligible_for_election,
    leader_conn_url, select_clone_source, select_upstream_source, should_acquire_after_peer_probe,
    surviving_election_peers,
};
use crate::sync_standby::compute_sync_standby_plan;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HaRoleState {
    Unbootstrapped,
    Bootstrapping {
        state_epoch: u64,
    },
    Cloning {
        leader_conn: String,
        state_epoch: u64,
    },
    ElectionCandidate {
        state_epoch: u64,
        peers_verified: bool,
        targeted_failover: bool,
    },
    ArmingPrimary {
        state_epoch: u64,
        leader_token: CasToken,
        lease: LeaseHandle,
        lease_valid_until: Instant,
    },
    Primary {
        leader_token: CasToken,
        lease: LeaseHandle,
        lease_valid_until: Instant,
    },
    FailsafePrimary {
        lease_valid_until: Instant,
    },
    Replica {
        following: Option<CompactString>,
    },
    StandbyLeader {
        leader_token: CasToken,
        lease: LeaseHandle,
        lease_valid_until: Instant,
    },
    Rewinding {
        target_leader_conn: String,
        state_epoch: u64,
    },
    Demoting {
        reason: DemoteReason,
        deadline: Instant,
        state_epoch: u64,
        release_leader_on_complete: Option<CasToken>,
    },
    Paused {
        underlying_role: PausedRole,
        held_leader: Option<(LeaseHandle, CasToken)>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DemoteReason {
    LeaseLost,
    FailsafeRejected,
    PlannedSwitchover,
    WatchdogArmFailed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PausedRole {
    Primary,
    Replica,
    Offline,
}

#[derive(Debug, Clone)]
pub struct PublishedNodeState {
    pub scope: CompactString,
    pub member_name: CompactString,
    pub state: CompactString,
    pub role: CompactString,
    pub is_read_write_valid: bool,
    pub holds_leader_lease: bool,
    pub is_sync_standby: bool,
    pub is_quorum_standby: bool,
    pub noloadbalance: bool,
    pub replication_lag_bytes: u64,
    pub timeline: u64,
    pub pending_restart: bool,
    pub pause: bool,
    pub ttl: Duration,
    pub last_ha_tick: Instant,
    pub dcs_last_seen: Option<Instant>,
    pub postgres_state_code: u8,
    pub xlog_location: WalLsn,
    pub xlog_paused: bool,
    pub postmaster_start_time_epoch: u64,
    pub xlog_replayed_timestamp_epoch: f64,
    pub server_version_num: u32,
    pub database_system_identifier: Option<CompactString>,
    pub cluster_unlocked: bool,
    pub failsafe_enabled: bool,
    pub failsafe_mode_is_active: bool,
    pub user_tags: BTreeMap<CompactString, CompactString>,
}

impl PublishedNodeState {
    pub fn initial(now: Instant) -> Self {
        Self {
            scope: CompactString::const_new("default"),
            member_name: CompactString::const_new("node"),
            state: CompactString::const_new("starting"),
            role: CompactString::const_new("uninitialized"),
            is_read_write_valid: false,
            holds_leader_lease: false,
            is_sync_standby: false,
            is_quorum_standby: false,
            noloadbalance: false,
            replication_lag_bytes: 0,
            timeline: 0,
            pending_restart: false,
            pause: false,
            ttl: Duration::from_secs(30),
            last_ha_tick: now,
            dcs_last_seen: None,
            postgres_state_code: 6, // Starting
            xlog_location: WalLsn::ZERO,
            xlog_paused: false,
            postmaster_start_time_epoch: 0,
            xlog_replayed_timestamp_epoch: 0.0,
            server_version_num: 170_000,
            database_system_identifier: None,
            cluster_unlocked: true,
            failsafe_enabled: false,
            failsafe_mode_is_active: false,
            user_tags: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LocalPgSummary {
    pub has_valid_pgdata: bool,
    pub state_label: CompactString,
    pub postgres_state_code: u8,
    pub in_recovery: bool,
    pub lsn: WalLsn,
    pub xlog_paused: bool,
    pub timeline: u64,
    pub lag_bytes: u64,
    pub is_sync_standby: bool,
    pub is_quorum_standby: bool,
    pub pending_restart: bool,
    pub needs_rewind: bool,
    pub postmaster_start_time_epoch: u64,
    pub xlog_replayed_timestamp_epoch: f64,
    pub server_version_num: u32,
    pub system_identifier: Option<CompactString>,
    pub slots: BTreeMap<CompactString, Option<WalLsn>>,
}

impl Eq for LocalPgSummary {}

impl Default for LocalPgSummary {
    fn default() -> Self {
        Self {
            has_valid_pgdata: false,
            state_label: CompactString::const_new("stopped"),
            postgres_state_code: 12, // Stopped
            in_recovery: true,
            lsn: WalLsn::ZERO,
            xlog_paused: false,
            timeline: 0,
            lag_bytes: 0,
            is_sync_standby: false,
            is_quorum_standby: false,
            pending_restart: false,
            needs_rewind: false,
            postmaster_start_time_epoch: 0,
            xlog_replayed_timestamp_epoch: 0.0,
            server_version_num: 170_000,
            system_identifier: None,
            slots: BTreeMap::new(),
        }
    }
}

impl LocalPgSummary {
    pub fn is_running(&self) -> bool {
        matches!(self.state_label.as_str(), "running" | "streaming")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobKind {
    InitDb,
    CustomBootstrap,
    BaseBackup,
    PgRewind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlCommand {
    Reload,
    Switchover { candidate: Option<CompactString> },
    Failover { candidate: Option<CompactString> },
    Pause,
    Resume,
    Reinitialize { force: bool },
}

#[derive(Debug, Clone)]
pub enum HaEvent {
    Tick {
        now: Instant,
    },
    WatchWakeup {
        now: Instant,
    },
    DcsFetched {
        rpc_epoch: u64,
        attempt_started_at: Instant,
        now: Instant,
        result: Result<Box<ClusterSnapshot>, String>,
    },
    InitializeResult {
        rpc_epoch: u64,
        now: Instant,
        acquired: bool,
    },
    ElectionPeersProbed {
        rpc_epoch: u64,
        now: Instant,
        samples: Vec<PeerElectionSample>,
    },
    LeaderLeaseResult {
        rpc_epoch: u64,
        attempt_started_at: Instant,
        now: Instant,
        result: Result<(LeaseHandle, CasToken), String>,
    },
    WatchdogArmed {
        state_epoch: u64,
        now: Instant,
    },
    WatchdogArmFailed {
        state_epoch: u64,
        now: Instant,
    },
    FailsafeQuorumResult {
        rpc_epoch: u64,
        attempt_started_at: Instant,
        now: Instant,
        all_peers_accepted: bool,
    },
    FailsafeRosterPublished {
        now: Instant,
        roster: BTreeMap<CompactString, String>,
    },
    InboundFailsafeHeartbeat {
        now: Instant,
        leader_name: CompactString,
        conn_url: String,
        lsn: WalLsn,
    },
    PgObservation {
        now: Instant,
        telemetry: LocalPgSummary,
    },
    PgPromotionCompleted {
        state_epoch: u64,
        now: Instant,
        success: bool,
    },
    PgDemotionCompleted {
        state_epoch: u64,
        now: Instant,
    },
    BackgroundJobFinished {
        state_epoch: u64,
        now: Instant,
        job: JobKind,
        success: bool,
    },
    OperatorCommand {
        now: Instant,
        cmd: ControlCommand,
    },
}

impl HaEvent {
    pub const fn now(&self) -> Instant {
        match self {
            Self::Tick { now }
            | Self::WatchWakeup { now }
            | Self::DcsFetched { now, .. }
            | Self::InitializeResult { now, .. }
            | Self::ElectionPeersProbed { now, .. }
            | Self::LeaderLeaseResult { now, .. }
            | Self::WatchdogArmed { now, .. }
            | Self::WatchdogArmFailed { now, .. }
            | Self::FailsafeQuorumResult { now, .. }
            | Self::FailsafeRosterPublished { now, .. }
            | Self::InboundFailsafeHeartbeat { now, .. }
            | Self::PgObservation { now, .. }
            | Self::PgPromotionCompleted { now, .. }
            | Self::PgDemotionCompleted { now, .. }
            | Self::BackgroundJobFinished { now, .. }
            | Self::OperatorCommand { now, .. } => *now,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    ArmAndFeedWatchdog {
        state_epoch: u64,
        proof: LeaseProof,
    },
    FeedWatchdog(LeaseProof),
    DisarmWatchdog,
    FetchDcs {
        rpc_epoch: u64,
        timeout: Duration,
    },
    CasInitialize {
        rpc_epoch: u64,
        sys_id: CompactString,
    },
    ProbeElectionPeers {
        rpc_epoch: u64,
        peers: Vec<(CompactString, String)>,
        timeout: Duration,
    },
    RenewOrAcquireLeader {
        rpc_epoch: u64,
        lease: Option<LeaseHandle>,
        expected: Option<CasToken>,
        ttl: Duration,
    },
    ReleaseLeader {
        expected: CasToken,
    },
    ClearFailover {
        expected: CasToken,
    },
    UpsertMember {
        data: Box<MemberData>,
        lease: Option<LeaseHandle>,
    },
    PublishLeaderStatus {
        status: ClusterStatus,
        failsafe_roster: Option<BTreeMap<CompactString, String>>,
    },
    AppendHistory {
        entry: HistoryEntry,
    },
    UpdateSyncState {
        next: SyncState,
        expected: Option<CasToken>,
    },
    ReconfigureSynchronousStandbys {
        guc_value: String,
    },
    ProbeFailsafePeers {
        rpc_epoch: u64,
        peers: Vec<(CompactString, String)>,
        timeout: Duration,
    },
    RespondFailsafeHeartbeat {
        accepted: bool,
    },
    StartPostgresAsReplica,
    PromotePostgres {
        state_epoch: u64,
    },
    DemotePostgres {
        state_epoch: u64,
        immediate_at: Instant,
    },
    ReconfigureStandby {
        primary_conninfo: String,
        slot_name: Option<CompactString>,
    },
    SyncSlots {
        status: ClusterStatus,
        is_primary: bool,
        is_demoted_rejoin: bool,
    },
    SpawnJob {
        state_epoch: u64,
        job: JobKind,
        target_conn: Option<String>,
    },
    CancelActiveJobs,
    NotifySystemd(&'static str),
}

pub type EffectBatch = SmallVec<[Effect; 8]>;

const MIN_SOFTWARE_SAFETY_MARGIN: Duration = Duration::from_secs(5);
const DEMOTE_IMMEDIATE_LEAD: Duration = Duration::from_secs(2);

pub struct NodeCtx {
    pub scope: CompactString,
    pub self_name: CompactString,
    pub self_conn_url: String,
    pub self_api_url: String,
    pub state: HaRoleState,
    pub rpc_epoch: u64,
    pub state_epoch: u64,
    pub ttl: Duration,
    pub loop_wait: Duration,
    pub retry_timeout: Duration,
    pub watchdog_mode: negroni_config::WatchdogMode,
    pub watchdog_safety_margin_cfg: i64,
    pub watchdog_safety_margin: Duration,
    pub maximum_lag_on_failover: u64,
    pub maximum_lag_on_syncnode: i64,
    pub synchronous_mode: SyncMode,
    pub synchronous_mode_strict: bool,
    pub synchronous_node_count: usize,
    pub failsafe_enabled: bool,
    pub custom_bootstrap_enabled: bool,
    pub nofailover_tag: bool,
    pub failover_priority_tag: i64,
    pub noloadbalance_tag: bool,
    pub replicatefrom_tag: Option<CompactString>,
    pub member_tags_json: BTreeMap<CompactString, serde_json::Value>,
    pub user_tags: BTreeMap<CompactString, CompactString>,
    pub last_pg: LocalPgSummary,
    pub last_tick_at: Instant,
    pub last_dcs_seen_at: Option<Instant>,
    pub last_failsafe_roster: Vec<(CompactString, String)>,
    pub candidate_failsafe_roster: Vec<(CompactString, String)>,
    pub last_known_failsafe_map: Option<BTreeMap<CompactString, String>>,
    pub last_known_sync: Option<SyncState>,
    pub last_known_sync_token: Option<CasToken>,
    pub last_known_failover_token: Option<CasToken>,
    pub last_applied_sync_guc: String,
    pub last_observed_dcs_leader: Option<CompactString>,
    pub accepted_failsafe_leader: Option<(CompactString, Instant)>,
}

impl NodeCtx {
    pub fn from_config(cfg: &DaemonConfig, boot_now: Instant) -> Self {
        let conn_addr = cfg
            .postgresql
            .connect_address
            .as_deref()
            .unwrap_or(&cfg.postgresql.listen);
        let api_addr = cfg
            .restapi
            .connect_address
            .as_deref()
            .unwrap_or(&cfg.restapi.listen);
        let api_scheme = if cfg.restapi.certfile.is_some() {
            "https"
        } else {
            "http"
        };
        let effective_margin = Self::compute_fencing_margin(
            cfg.timings.ttl,
            cfg.timings.loop_wait,
            cfg.timings.retry_timeout,
            cfg.timings.safety_margin,
        );

        let custom_bootstrap = cfg
            .bootstrap_method
            .as_deref()
            .is_some_and(|m| !m.is_empty() && m != "initdb");

        Self {
            scope: cfg.scope.clone(),
            self_name: cfg.name.clone(),
            self_conn_url: format!("postgres://{conn_addr}/postgres"),
            self_api_url: format!("{api_scheme}://{api_addr}/patroni"),
            state: HaRoleState::Unbootstrapped,
            rpc_epoch: 0,
            state_epoch: 0,
            ttl: cfg.timings.ttl,
            loop_wait: cfg.timings.loop_wait,
            retry_timeout: cfg.timings.retry_timeout,
            watchdog_mode: cfg.watchdog.mode,
            watchdog_safety_margin_cfg: cfg.watchdog.safety_margin,
            watchdog_safety_margin: effective_margin,
            maximum_lag_on_failover: cfg.dynamic.maximum_lag_on_failover,
            maximum_lag_on_syncnode: cfg.dynamic.maximum_lag_on_syncnode,
            synchronous_mode: cfg.dynamic.synchronous_mode,
            synchronous_mode_strict: cfg.dynamic.synchronous_mode_strict,
            synchronous_node_count: cfg.dynamic.synchronous_node_count,
            failsafe_enabled: cfg.dynamic.failsafe_mode,
            custom_bootstrap_enabled: custom_bootstrap,
            nofailover_tag: cfg.tags.effective_nofailover(),
            failover_priority_tag: cfg.tags.effective_failover_priority(),
            noloadbalance_tag: cfg.tags.noloadbalance,
            replicatefrom_tag: cfg.tags.replicatefrom.clone(),
            member_tags_json: cfg.tags.to_member_tags_json(),
            user_tags: cfg.tags.custom_string_tags(),
            last_pg: LocalPgSummary::default(),
            last_tick_at: boot_now,
            last_dcs_seen_at: None,
            last_failsafe_roster: Vec::new(),
            candidate_failsafe_roster: Vec::new(),
            last_known_failsafe_map: None,
            last_known_sync: None,
            last_known_sync_token: None,
            last_known_failover_token: None,
            last_applied_sync_guc: String::new(),
            last_observed_dcs_leader: None,
            accepted_failsafe_leader: None,
        }
    }

    /// Computes the local fencing margin subtracted from `attempt_started_at + ttl`.
    /// Even when hardware `/dev/watchdog` is disabled (`safety_margin == 0`), a software
    /// margin (`>= 2s`, up to `5s`) guarantees local PostgreSQL stops accepting writes
    /// before any peer's server-side DCS lease can expire.
    fn compute_fencing_margin(
        ttl: Duration,
        loop_wait: Duration,
        retry_timeout: Duration,
        configured_wd_margin: Duration,
    ) -> Duration {
        if configured_wd_margin >= DEMOTE_IMMEDIATE_LEAD {
            return configured_wd_margin;
        }
        let slack = ttl.saturating_sub(loop_wait + retry_timeout);
        slack
            .min(MIN_SOFTWARE_SAFETY_MARGIN)
            .max(DEMOTE_IMMEDIATE_LEAD)
    }

    /// Pure, synchronous, zero-I/O state reducer.
    pub fn step(&mut self, event: HaEvent) -> (EffectBatch, PublishedNodeState) {
        let mut effects = EffectBatch::new();
        let event_now = event.now();

        // Expire stale inbound failsafe heartbeats first.
        if let Some((_, until)) = self.accepted_failsafe_leader
            && event_now >= until
        {
            self.accepted_failsafe_leader = None;
        }

        // Enforce local primary write deadline proactively before processing any event.
        self.enforce_primary_deadline(event_now, &mut effects);

        match event {
            HaEvent::Tick { now } | HaEvent::WatchWakeup { now } => {
                self.last_tick_at = now;
                // Only `rpc_epoch` advances on ticks; long-running `state_epoch` jobs continue uninterrupted.
                self.rpc_epoch += 1;
                let timeout = self.bounded_rpc_timeout(now);
                effects.push(Effect::FetchDcs {
                    rpc_epoch: self.rpc_epoch,
                    timeout,
                });
            }

            HaEvent::PgObservation { now, telemetry } => {
                self.last_pg = telemetry;
                // Safety net: if we are Replica and Postgres somehow exited recovery without holding `/leader`, fence immediately!
                if matches!(self.state, HaRoleState::Replica { .. })
                    && self.last_pg.is_running()
                    && !self.last_pg.in_recovery
                {
                    self.enter_demoting(DemoteReason::LeaseLost, now, &mut effects);
                }
            }

            HaEvent::PgPromotionCompleted {
                state_epoch,
                now,
                success,
            } => {
                if !success
                    && matches!(self.state, HaRoleState::Primary { .. })
                    && state_epoch == self.state_epoch
                {
                    self.enter_demoting(DemoteReason::LeaseLost, now, &mut effects);
                } else if success && matches!(self.state, HaRoleState::Primary { .. }) {
                    self.last_pg.in_recovery = false;
                    effects.push(Effect::AppendHistory {
                        entry: HistoryEntry {
                            timeline: self.last_pg.timeline.max(1),
                            lsn: self.last_pg.lsn,
                            reason: "promoted to primary".to_owned(),
                            timestamp: None,
                            leader: Some(self.self_name.clone()),
                        },
                    });
                }
            }

            HaEvent::InboundFailsafeHeartbeat {
                now,
                leader_name,
                conn_url,
                ..
            } => {
                let conflicting_self_primary = matches!(
                    self.state,
                    HaRoleState::Primary { .. }
                        | HaRoleState::ArmingPrimary { .. }
                        | HaRoleState::FailsafePrimary { .. }
                );
                let conflicting_dcs_leader = self
                    .last_observed_dcs_leader
                    .as_ref()
                    .is_some_and(|ldr| *ldr != leader_name);
                let conflicting_failsafe_leader = self
                    .accepted_failsafe_leader
                    .as_ref()
                    .is_some_and(|(ldr, until)| *ldr != leader_name && now < *until);

                let accepted = self.failsafe_enabled
                    && !conflicting_self_primary
                    && !conflicting_dcs_leader
                    && !conflicting_failsafe_leader;

                if accepted {
                    self.accepted_failsafe_leader = Some((leader_name.clone(), now + self.ttl));
                    // If this replica was campaigning in an election, yield immediately to the active failsafe primary.
                    if matches!(self.state, HaRoleState::ElectionCandidate { .. }) {
                        self.state_epoch += 1;
                        self.rpc_epoch += 1;
                        self.state = HaRoleState::Replica {
                            following: Some(leader_name),
                        };
                        effects.push(Effect::ReconfigureStandby {
                            primary_conninfo: conn_url,
                            slot_name: Some(self.self_name.clone()),
                        });
                    }
                }
                effects.push(Effect::RespondFailsafeHeartbeat { accepted });
            }

            HaEvent::DcsFetched {
                rpc_epoch,
                now,
                result,
                ..
            } => {
                if rpc_epoch < self.rpc_epoch {
                    return (effects, self.publish_snapshot(now));
                }
                self.handle_dcs_fetched(now, result, &mut effects);
            }

            HaEvent::InitializeResult {
                rpc_epoch,
                acquired,
                ..
            } => {
                if rpc_epoch == self.rpc_epoch
                    && acquired
                    && matches!(self.state, HaRoleState::Unbootstrapped)
                    && !self.last_pg.has_valid_pgdata
                {
                    self.state_epoch += 1;
                    let s_epoch = self.state_epoch;
                    let job = if self.custom_bootstrap_enabled {
                        JobKind::CustomBootstrap
                    } else {
                        JobKind::InitDb
                    };
                    self.state = HaRoleState::Bootstrapping {
                        state_epoch: s_epoch,
                    };
                    effects.push(Effect::SpawnJob {
                        state_epoch: s_epoch,
                        job,
                        target_conn: None,
                    });
                }
            }

            HaEvent::ElectionPeersProbed {
                rpc_epoch, samples, ..
            } => {
                if rpc_epoch == self.rpc_epoch
                    && let HaRoleState::ElectionCandidate {
                        state_epoch,
                        peers_verified: false,
                        targeted_failover,
                    } = self.state
                {
                    let probe_ctx = PeerProbeContext {
                        self_name: &self.self_name,
                        self_lsn: self.last_pg.lsn,
                        self_failover_priority: self.failover_priority_tag,
                        targeted_failover,
                        failsafe_enabled: self.failsafe_enabled,
                        dcs_failsafe: self.last_known_failsafe_map.as_ref(),
                        sync_mode: self.synchronous_mode,
                        dcs_sync: self.last_known_sync.as_ref(),
                    };
                    if self.accepted_failsafe_leader.is_some() {
                        self.state = HaRoleState::Replica { following: None };
                    } else if should_acquire_after_peer_probe(&probe_ctx, &samples) {
                        self.state = HaRoleState::ElectionCandidate {
                            state_epoch,
                            peers_verified: true,
                            targeted_failover,
                        };
                        self.rpc_epoch += 1;
                        effects.push(Effect::RenewOrAcquireLeader {
                            rpc_epoch: self.rpc_epoch,
                            lease: None,
                            expected: None,
                            ttl: self.ttl,
                        });
                    } else {
                        // Yield election to surviving peer with higher WAL LSN / priority, active leader, or insufficient quorum.
                        self.state = HaRoleState::Replica { following: None };
                    }
                }
            }

            HaEvent::LeaderLeaseResult {
                rpc_epoch,
                attempt_started_at,
                now,
                result,
            } => {
                if rpc_epoch == self.rpc_epoch {
                    let valid_until = (attempt_started_at + self.ttl)
                        .checked_sub(self.watchdog_safety_margin)
                        .unwrap_or(attempt_started_at);
                    match (&self.state, result) {
                        // Paused Primary refreshed DCS `/leader` lock (watchdog stays disarmed)
                        (
                            HaRoleState::Paused {
                                underlying_role: PausedRole::Primary,
                                ..
                            },
                            Ok((lease, token)),
                        ) => {
                            self.state = HaRoleState::Paused {
                                underlying_role: PausedRole::Primary,
                                held_leader: Some((lease.clone(), token)),
                            };
                            effects.push(Effect::UpsertMember {
                                data: Box::new(self.build_member_data("primary")),
                                lease: Some(lease),
                            });
                        }
                        (HaRoleState::Paused { .. }, Err(_)) => {}

                        // Candidate won `/leader`: arm `/dev/watchdog` BEFORE promoting PostgreSQL!
                        (HaRoleState::ElectionCandidate { .. }, Ok((lease, token))) => {
                            if valid_until <= now {
                                effects.push(Effect::ReleaseLeader { expected: token });
                                self.enter_demoting(DemoteReason::LeaseLost, now, &mut effects);
                            } else {
                                self.state_epoch += 1;
                                let s_epoch = self.state_epoch;
                                self.state = HaRoleState::ArmingPrimary {
                                    state_epoch: s_epoch,
                                    leader_token: token,
                                    lease,
                                    lease_valid_until: valid_until,
                                };
                                effects.push(Effect::ArmAndFeedWatchdog {
                                    state_epoch: s_epoch,
                                    proof: LeaseProof {
                                        epoch: s_epoch,
                                        valid_until,
                                    },
                                });
                            }
                        }

                        // Existing Primary or recovering FailsafePrimary renewed `/leader`
                        (
                            HaRoleState::Primary { .. } | HaRoleState::FailsafePrimary { .. },
                            Ok((lease, token)),
                        ) => {
                            if valid_until <= now {
                                self.enter_demoting(DemoteReason::LeaseLost, now, &mut effects);
                            } else {
                                self.state_epoch += 1;
                                self.state = HaRoleState::Primary {
                                    leader_token: token,
                                    lease: lease.clone(),
                                    lease_valid_until: valid_until,
                                };
                                effects.push(Effect::FeedWatchdog(LeaseProof {
                                    epoch: self.state_epoch,
                                    valid_until,
                                }));
                                if !self.last_pg.is_running() || self.last_pg.in_recovery {
                                    effects.push(Effect::PromotePostgres {
                                        state_epoch: self.state_epoch,
                                    });
                                }
                                self.emit_primary_publication_effects(&lease, &mut effects);
                            }
                        }

                        (
                            HaRoleState::Primary { .. } | HaRoleState::FailsafePrimary { .. },
                            Err(_),
                        ) => {
                            self.enter_demoting(DemoteReason::LeaseLost, now, &mut effects);
                        }
                        (_, Err(_)) => {
                            self.state = HaRoleState::Replica { following: None };
                        }
                        _ => {}
                    }
                }
            }

            HaEvent::WatchdogArmed { state_epoch, .. } => {
                if let HaRoleState::ArmingPrimary {
                    state_epoch: expected,
                    ref leader_token,
                    ref lease,
                    lease_valid_until,
                } = self.state
                    && state_epoch == expected
                {
                    let tok = leader_token.clone();
                    let lse = lease.clone();
                    self.state = HaRoleState::Primary {
                        leader_token: tok,
                        lease: lse.clone(),
                        lease_valid_until,
                    };
                    if self.last_pg.in_recovery || !self.last_pg.is_running() {
                        effects.push(Effect::PromotePostgres { state_epoch });
                    }
                    // Atomically clear any `/failover` intent fulfilled by this election.
                    if let Some(fo_tok) = self.last_known_failover_token.take() {
                        effects.push(Effect::ClearFailover { expected: fo_tok });
                    }
                    // Claim `/sync` immediately on promotion so diverged old-timeline standbys cannot block future failovers.
                    if self.synchronous_mode.is_enabled()
                        && self
                            .last_known_sync
                            .as_ref()
                            .and_then(|s| s.leader.as_ref())
                            != Some(&self.self_name)
                    {
                        let claimed_sync = SyncState {
                            leader: Some(self.self_name.clone()),
                            sync_standby: None,
                            quorum: 0,
                        };
                        effects.push(Effect::UpdateSyncState {
                            next: claimed_sync.clone(),
                            expected: self.last_known_sync_token.take(),
                        });
                        self.last_known_sync = Some(claimed_sync);
                        self.last_applied_sync_guc.clear();
                    }
                    self.emit_primary_publication_effects(&lse, &mut effects);
                    effects.push(Effect::NotifySystemd("STATUS=Running as primary"));
                }
            }

            HaEvent::WatchdogArmFailed { state_epoch, now } => {
                if let HaRoleState::ArmingPrimary {
                    state_epoch: expected,
                    ref leader_token,
                    ..
                } = self.state
                    && state_epoch == expected
                {
                    let tok = leader_token.clone();
                    effects.push(Effect::ReleaseLeader { expected: tok });
                    self.enter_demoting(DemoteReason::WatchdogArmFailed, now, &mut effects);
                }
            }

            HaEvent::FailsafeQuorumResult {
                rpc_epoch,
                attempt_started_at,
                now,
                all_peers_accepted,
            } => {
                if rpc_epoch == self.rpc_epoch
                    && let HaRoleState::FailsafePrimary { lease_valid_until } = self.state
                {
                    let extended = (attempt_started_at + self.ttl)
                        .checked_sub(self.watchdog_safety_margin)
                        .unwrap_or(attempt_started_at);
                    if all_peers_accepted && extended > now {
                        self.state_epoch += 1;
                        self.state = HaRoleState::FailsafePrimary {
                            lease_valid_until: extended,
                        };
                        effects.push(Effect::FeedWatchdog(LeaseProof {
                            epoch: self.state_epoch,
                            valid_until: extended,
                        }));
                    } else {
                        self.enter_demoting(
                            DemoteReason::FailsafeRejected,
                            lease_valid_until,
                            &mut effects,
                        );
                    }
                }
            }

            HaEvent::FailsafeRosterPublished { roster, .. } => {
                if roster.contains_key(&self.self_name) {
                    self.last_failsafe_roster = roster
                        .iter()
                        .filter(|(m, _)| **m != self.self_name)
                        .map(|(m, url)| (m.clone(), url.clone()))
                        .collect();
                    self.last_known_failsafe_map = Some(roster);
                }
            }

            HaEvent::PgDemotionCompleted { state_epoch, .. } => {
                if let HaRoleState::Demoting {
                    state_epoch: expected,
                    ref release_leader_on_complete,
                    ..
                } = self.state
                    && expected == state_epoch
                {
                    let release_tok = release_leader_on_complete.clone();
                    // Hardware watchdog disarmed and `/leader` released ONLY after Postgres is shut down / read-only!
                    self.last_pg.in_recovery = true;
                    if let Some(tok) = release_tok {
                        effects.push(Effect::ReleaseLeader { expected: tok });
                    }
                    effects.push(Effect::DisarmWatchdog);
                    effects.push(Effect::SyncSlots {
                        status: ClusterStatus::default(),
                        is_primary: false,
                        is_demoted_rejoin: true,
                    });
                    effects.push(Effect::StartPostgresAsReplica);
                    effects.push(Effect::NotifySystemd("STATUS=Running as replica"));
                    self.state = HaRoleState::Replica { following: None };
                }
            }

            HaEvent::BackgroundJobFinished {
                state_epoch,
                job,
                success,
                ..
            } => {
                let matches_active_job = matches!(
                    self.state,
                    HaRoleState::Bootstrapping { state_epoch: e }
                    | HaRoleState::Cloning { state_epoch: e, .. }
                    | HaRoleState::Rewinding { state_epoch: e, .. }
                    if e == state_epoch
                );
                if matches_active_job {
                    match (job, success) {
                        (JobKind::InitDb | JobKind::CustomBootstrap, true) => {
                            self.last_pg.has_valid_pgdata = true;
                            self.last_pg.in_recovery = true;
                            self.state_epoch += 1;
                            self.rpc_epoch += 1;
                            self.state = HaRoleState::ElectionCandidate {
                                state_epoch: self.state_epoch,
                                peers_verified: true,
                                targeted_failover: false,
                            };
                            effects.push(Effect::RenewOrAcquireLeader {
                                rpc_epoch: self.rpc_epoch,
                                lease: None,
                                expected: None,
                                ttl: self.ttl,
                            });
                        }
                        (JobKind::BaseBackup | JobKind::PgRewind, true) => {
                            self.last_pg.has_valid_pgdata = true;
                            self.last_pg.in_recovery = true;
                            self.last_pg.needs_rewind = false;
                            effects.push(Effect::StartPostgresAsReplica);
                            self.state = HaRoleState::Replica { following: None };
                        }
                        _ => {
                            self.state = HaRoleState::Unbootstrapped;
                        }
                    }
                }
            }

            HaEvent::OperatorCommand { now, cmd } => {
                self.handle_operator_command(now, cmd, &mut effects);
            }
        }

        let snapshot = self.publish_snapshot(event_now);
        (effects, snapshot)
    }

    fn enforce_primary_deadline(&mut self, now: Instant, effects: &mut EffectBatch) {
        match self.state {
            HaRoleState::Primary {
                lease_valid_until, ..
            }
            | HaRoleState::FailsafePrimary { lease_valid_until }
            | HaRoleState::StandbyLeader {
                lease_valid_until, ..
            } if now >= lease_valid_until => {
                self.enter_demoting(DemoteReason::LeaseLost, lease_valid_until, effects);
            }
            HaRoleState::ArmingPrimary {
                ref leader_token,
                lease_valid_until,
                ..
            } if now >= lease_valid_until => {
                let tok = leader_token.clone();
                effects.push(Effect::ReleaseLeader { expected: tok });
                self.enter_demoting(DemoteReason::LeaseLost, lease_valid_until, effects);
            }
            _ => {}
        }
    }

    fn bounded_rpc_timeout(&self, now: Instant) -> Duration {
        match self.state {
            HaRoleState::Primary {
                lease_valid_until, ..
            }
            | HaRoleState::FailsafePrimary { lease_valid_until } => {
                let safe_cutoff = lease_valid_until
                    .checked_sub(DEMOTE_IMMEDIATE_LEAD)
                    .unwrap_or(lease_valid_until);
                safe_cutoff
                    .saturating_duration_since(now)
                    .min(self.retry_timeout)
                    .max(Duration::from_millis(50))
            }
            _ => self.retry_timeout,
        }
    }

    fn apply_dynamic_dcs_config(&mut self, snap: &ClusterSnapshot, effects: &mut EffectBatch) {
        let Some(ref cfg_ver) = snap.config else {
            return;
        };
        let Ok(dyn_cfg) = serde_json::from_value::<DynamicClusterConfig>(cfg_ver.value.clone())
        else {
            return;
        };

        if let Ok(timings) = validate_timings(
            dyn_cfg.ttl,
            dyn_cfg.loop_wait,
            dyn_cfg.retry_timeout,
            self.watchdog_mode,
            self.watchdog_safety_margin_cfg,
        ) {
            self.ttl = timings.ttl;
            self.loop_wait = timings.loop_wait;
            self.retry_timeout = timings.retry_timeout;
            self.watchdog_safety_margin = Self::compute_fencing_margin(
                timings.ttl,
                timings.loop_wait,
                timings.retry_timeout,
                timings.safety_margin,
            );
        }

        self.maximum_lag_on_failover = dyn_cfg.maximum_lag_on_failover;
        self.maximum_lag_on_syncnode = dyn_cfg.maximum_lag_on_syncnode;
        self.synchronous_mode = dyn_cfg.synchronous_mode;
        self.synchronous_mode_strict = dyn_cfg.synchronous_mode_strict;
        self.synchronous_node_count = dyn_cfg.synchronous_node_count;
        self.failsafe_enabled = dyn_cfg.failsafe_mode;

        let currently_paused = matches!(self.state, HaRoleState::Paused { .. });
        if dyn_cfg.pause && !currently_paused {
            self.enter_paused(effects);
        } else if !dyn_cfg.pause && currently_paused {
            self.resume_from_paused(effects);
        }
    }

    fn handle_dcs_fetched(
        &mut self,
        now: Instant,
        result: Result<Box<ClusterSnapshot>, String>,
        effects: &mut EffectBatch,
    ) {
        if let Ok(ref snap) = result {
            self.last_dcs_seen_at = Some(now);
            self.last_observed_dcs_leader = snap.leader.as_ref().map(|l| l.value.clone());
            self.last_known_sync = snap.sync.as_ref().map(|s| s.value.clone());
            self.last_known_sync_token = snap.sync.as_ref().map(|s| s.token.clone());
            self.last_known_failover_token = snap
                .failover
                .as_ref()
                .filter(|f| !f.value.is_empty())
                .map(|f| f.token.clone());
            self.last_known_failsafe_map.clone_from(&snap.failsafe);

            // Clear stale failsafe suppression if DCS shows an active leader different from or equal to failsafe leader.
            if let Some(ref dcs_ldr) = snap.leader
                && let Some((ref fs_ldr, _)) = self.accepted_failsafe_leader
                && dcs_ldr.value != *fs_ldr
            {
                self.accepted_failsafe_leader = None;
            }

            if self.failsafe_enabled || snap.failsafe.is_some() {
                // `last_failsafe_roster` ONLY contains peers ALREADY persisted in DCS `/failsafe` alongside `self_name`.
                match &snap.failsafe {
                    Some(fs_map) if fs_map.contains_key(&self.self_name) => {
                        self.last_failsafe_roster = fs_map
                            .iter()
                            .filter(|(m, _)| **m != self.self_name)
                            .map(|(m, url)| (m.clone(), url.clone()))
                            .collect();
                    }
                    _ => {
                        self.last_failsafe_roster.clear();
                    }
                }

                // `candidate_failsafe_roster` unions existing `/failsafe` with live `/members` for the next publication.
                let mut merged = BTreeMap::new();
                if let Some(ref fs_map) = snap.failsafe {
                    for (m, url) in fs_map {
                        if *m != self.self_name {
                            merged.insert(m.clone(), url.clone());
                        }
                    }
                }
                for (m, data) in &snap.members {
                    if *m != self.self_name {
                        merged.insert(m.clone(), data.value.api_url.clone());
                    }
                }
                self.candidate_failsafe_roster = merged.into_iter().collect();
            }

            self.apply_dynamic_dcs_config(snap, effects);
        }

        match (&self.state, result) {
            // 1a. Unbootstrapped with NO existing `$PGDATA`: arbitrate `/initialize` first, or clone replica.
            (HaRoleState::Unbootstrapped, Ok(snap)) if !self.last_pg.has_valid_pgdata => {
                if snap.initialize.is_none() && snap.leader.is_none() && !self.nofailover_tag {
                    self.rpc_epoch += 1;
                    effects.push(Effect::CasInitialize {
                        rpc_epoch: self.rpc_epoch,
                        sys_id: self.self_name.clone(),
                    });
                } else if let Some(source_conn) =
                    select_clone_source(&self.self_name, self.replicatefrom_tag.as_ref(), &snap)
                {
                    self.state_epoch += 1;
                    let s_epoch = self.state_epoch;
                    self.state = HaRoleState::Cloning {
                        leader_conn: source_conn.clone(),
                        state_epoch: s_epoch,
                    };
                    effects.push(Effect::SpawnJob {
                        state_epoch: s_epoch,
                        job: JobKind::BaseBackup,
                        target_conn: Some(source_conn),
                    });
                }
            }

            // 1b. Zero-downtime startup adoption when `$PGDATA` already exists on disk!
            (HaRoleState::Unbootstrapped, Ok(snap)) if self.last_pg.has_valid_pgdata => {
                if self.last_pg.is_running() && !self.last_pg.in_recovery {
                    // Running read-write Postgres on boot: adopt seamlessly if we own `/leader` (or `/leader` is vacant), else fence!
                    let can_adopt_primary = match &snap.leader {
                        Some(ldr) => ldr.value == self.self_name,
                        None => !self.nofailover_tag && self.accepted_failsafe_leader.is_none(),
                    };
                    if can_adopt_primary {
                        let expected = snap.leader.as_ref().map(|l| l.token.clone());
                        self.state_epoch += 1;
                        self.rpc_epoch += 1;
                        self.state = HaRoleState::ElectionCandidate {
                            state_epoch: self.state_epoch,
                            peers_verified: true,
                            targeted_failover: false,
                        };
                        effects.push(Effect::RenewOrAcquireLeader {
                            rpc_epoch: self.rpc_epoch,
                            lease: None,
                            expected,
                            ttl: self.ttl,
                        });
                    } else {
                        self.enter_demoting(DemoteReason::LeaseLost, now, effects);
                    }
                } else {
                    if !self.last_pg.is_running() {
                        effects.push(Effect::StartPostgresAsReplica);
                    }
                    self.state = HaRoleState::Replica { following: None };
                    self.reconcile_replica_with_snapshot(&snap, effects);
                }
            }

            // 2. Primary lost DCS connectivity -> Failsafe quorum probe or immediate demote.
            (
                HaRoleState::Primary {
                    lease_valid_until, ..
                },
                Err(_),
            ) => {
                let valid_until = *lease_valid_until;
                let probe_cutoff = valid_until
                    .checked_sub(DEMOTE_IMMEDIATE_LEAD)
                    .unwrap_or(valid_until);
                if self.failsafe_enabled
                    && !self.last_failsafe_roster.is_empty()
                    && now < probe_cutoff
                {
                    let timeout = probe_cutoff
                        .saturating_duration_since(now)
                        .min(self.retry_timeout);
                    self.state_epoch += 1;
                    self.rpc_epoch += 1;
                    self.state = HaRoleState::FailsafePrimary {
                        lease_valid_until: valid_until,
                    };
                    effects.push(Effect::ProbeFailsafePeers {
                        rpc_epoch: self.rpc_epoch,
                        peers: self.last_failsafe_roster.clone(),
                        timeout,
                    });
                } else {
                    self.enter_demoting(DemoteReason::LeaseLost, valid_until, effects);
                }
            }

            // 3. FailsafePrimary while DCS remains unreachable -> renew peer quorum every tick.
            (HaRoleState::FailsafePrimary { lease_valid_until }, Err(_)) => {
                let valid_until = *lease_valid_until;
                let probe_cutoff = valid_until
                    .checked_sub(DEMOTE_IMMEDIATE_LEAD)
                    .unwrap_or(valid_until);
                if now >= probe_cutoff || self.last_failsafe_roster.is_empty() {
                    self.enter_demoting(DemoteReason::LeaseLost, valid_until, effects);
                } else {
                    let timeout = probe_cutoff
                        .saturating_duration_since(now)
                        .min(self.retry_timeout);
                    self.rpc_epoch += 1;
                    effects.push(Effect::ProbeFailsafePeers {
                        rpc_epoch: self.rpc_epoch,
                        peers: self.last_failsafe_roster.clone(),
                        timeout,
                    });
                }
            }

            // 4. FailsafePrimary sees DCS recover -> reclaim `/leader` or demote on conflict.
            (HaRoleState::FailsafePrimary { lease_valid_until }, Ok(snap)) => {
                let valid_until = *lease_valid_until;
                match &snap.leader {
                    None => {
                        self.rpc_epoch += 1;
                        effects.push(Effect::RenewOrAcquireLeader {
                            rpc_epoch: self.rpc_epoch,
                            lease: None,
                            expected: None,
                            ttl: self.ttl,
                        });
                    }
                    Some(ldr) if ldr.value == self.self_name => {
                        self.rpc_epoch += 1;
                        effects.push(Effect::RenewOrAcquireLeader {
                            rpc_epoch: self.rpc_epoch,
                            lease: None,
                            expected: Some(ldr.token.clone()),
                            ttl: self.ttl,
                        });
                    }
                    Some(_) => {
                        self.enter_demoting(DemoteReason::LeaseLost, valid_until, effects);
                    }
                }
            }

            // 5. Primary sees DCS -> handle planned switchover, sync standbys, and lease renewal.
            (
                HaRoleState::Primary {
                    leader_token,
                    lease,
                    lease_valid_until,
                },
                Ok(snap),
            ) => {
                let leader_tok = leader_token.clone();
                let lease_clone = lease.clone();
                let valid_until = *lease_valid_until;
                if snap.leader.as_ref().map(|l| &l.value) != Some(&self.self_name) {
                    self.enter_demoting(DemoteReason::LeaseLost, now, effects);
                } else if self.should_step_down_for_failover_intent(&snap, effects) {
                    let deadline = (now + self.retry_timeout).min(valid_until);
                    self.enter_demoting_with_release(
                        DemoteReason::PlannedSwitchover,
                        deadline,
                        Some(leader_tok),
                        effects,
                    );
                } else {
                    // Clear completed `/failover` key if it targeted us as the winning `candidate`.
                    if let Some(ref fo) = snap.failover
                        && !fo.value.is_empty()
                        && fo.value.candidate.as_ref() == Some(&self.self_name)
                    {
                        effects.push(Effect::ClearFailover {
                            expected: fo.token.clone(),
                        });
                    }

                    self.reconcile_primary_sync_state(&snap, effects);

                    self.rpc_epoch += 1;
                    effects.push(Effect::RenewOrAcquireLeader {
                        rpc_epoch: self.rpc_epoch,
                        lease: Some(lease_clone),
                        expected: Some(leader_tok),
                        ttl: self.ttl,
                    });
                }
            }

            // 6. Paused Primary keeps `/leader` lock alive when DCS is reachable without auto-demoting.
            (
                HaRoleState::Paused {
                    underlying_role: PausedRole::Primary,
                    held_leader,
                },
                Ok(snap),
            ) => {
                let uncontested = snap
                    .leader
                    .as_ref()
                    .is_none_or(|l| l.value == self.self_name);
                if uncontested {
                    self.rpc_epoch += 1;
                    effects.push(Effect::RenewOrAcquireLeader {
                        rpc_epoch: self.rpc_epoch,
                        lease: held_leader.as_ref().map(|(l, _)| l.clone()),
                        expected: snap
                            .leader
                            .as_ref()
                            .map(|l| l.token.clone())
                            .or_else(|| held_leader.as_ref().map(|(_, t)| t.clone())),
                        ttl: self.ttl,
                    });
                }
            }

            // 7. Paused Replica still publishes `/members/<self>` telemetry when DCS is up.
            (
                HaRoleState::Paused {
                    underlying_role: PausedRole::Replica,
                    ..
                },
                Ok(_),
            ) => {
                effects.push(Effect::UpsertMember {
                    data: Box::new(self.build_member_data("replica")),
                    lease: None,
                });
            }

            // 8. ElectionCandidate sees another node already acquired `/leader` -> yield back to Replica.
            (HaRoleState::ElectionCandidate { .. }, Ok(snap)) => {
                if let Some(ref ldr) = snap.leader
                    && ldr.value != self.self_name
                {
                    self.state = HaRoleState::Replica { following: None };
                    self.reconcile_replica_with_snapshot(&snap, effects);
                }
            }

            // 9. Replica sees DCS -> rewind if diverged, campaign if `/leader` vacant, or follow upstream.
            (HaRoleState::Replica { .. }, Ok(snap)) => {
                self.reconcile_replica_with_snapshot(&snap, effects);
            }

            _ => {}
        }
    }

    fn should_step_down_for_failover_intent(
        &self,
        snap: &ClusterSnapshot,
        effects: &mut EffectBatch,
    ) -> bool {
        let Some(ref fo_ver) = snap.failover else {
            return false;
        };
        let fo = &fo_ver.value;
        if fo.is_empty() || fo.scheduled_at.is_some() {
            return false;
        }
        // Ignore if `candidate` already points to ourselves (post-promotion cleanup).
        if fo.candidate.as_ref() == Some(&self.self_name) {
            return false;
        }
        // Must either omit `leader` or explicitly target `self_name`.
        if let Some(ref specified_leader) = fo.leader
            && !specified_leader.is_empty()
            && *specified_leader != self.self_name
        {
            return false;
        }

        // Verify at least one healthy replica (or the requested `candidate`) is ready to take over.
        let candidate_ready = match &fo.candidate {
            Some(cand) if !cand.is_empty() => snap
                .members
                .get(cand)
                .is_some_and(|m| matches!(m.value.state.as_str(), "running" | "streaming")),
            _ => snap.members.iter().any(|(name, m)| {
                *name != self.self_name
                    && !m.value.nofailover()
                    && matches!(m.value.state.as_str(), "running" | "streaming")
            }),
        };

        if !candidate_ready {
            return false;
        }

        // If no specific candidate was targeted, clear `/failover` now; if a candidate was targeted,
        // leave `/failover` in DCS so only that candidate passes `is_eligible_for_election`.
        if fo.candidate.as_deref().is_none_or(str::is_empty) {
            effects.push(Effect::ClearFailover {
                expected: fo_ver.token.clone(),
            });
        }
        true
    }

    fn reconcile_primary_sync_state(&mut self, snap: &ClusterSnapshot, effects: &mut EffectBatch) {
        let plan = compute_sync_standby_plan(
            &self.self_name,
            self.last_pg.lsn,
            self.synchronous_mode,
            self.synchronous_mode_strict,
            self.synchronous_node_count,
            self.maximum_lag_on_syncnode,
            &snap.members,
            snap.sync.as_ref().map(|s| &s.value),
        );

        let current_dcs_sync = snap.sync.as_ref().map(|s| &s.value);
        let sync_token = snap.sync.as_ref().map(|s| s.token.clone());
        let dcs_needs_update = match (self.synchronous_mode, current_dcs_sync) {
            (SyncMode::Off, Some(existing)) => {
                existing.leader.is_some() || existing.sync_standby.is_some()
            }
            (SyncMode::Off, None) => false,
            (_, Some(existing)) => existing != &plan.dcs_sync_state,
            (_, None) => true,
        };

        let guc_needs_update = self.last_applied_sync_guc != plan.guc_synchronous_standby_names;

        // Quorum ordering rule (Patroni `QuorumStateResolver`):
        // - Decreasing `quorum` (stricter PG sync) -> reconfigure PG `synchronous_standby_names` FIRST, then `/sync`.
        // - Increasing `quorum` -> update DCS `/sync` FIRST, then PG `synchronous_standby_names`.
        let prev_quorum = current_dcs_sync.map_or(0, |s| s.quorum);
        let increasing_quorum = plan.dcs_sync_state.quorum > prev_quorum;

        if increasing_quorum {
            if dcs_needs_update {
                effects.push(Effect::UpdateSyncState {
                    next: plan.dcs_sync_state.clone(),
                    expected: sync_token,
                });
            }
            if guc_needs_update {
                self.last_applied_sync_guc
                    .clone_from(&plan.guc_synchronous_standby_names);
                effects.push(Effect::ReconfigureSynchronousStandbys {
                    guc_value: plan.guc_synchronous_standby_names,
                });
            }
        } else {
            if guc_needs_update {
                self.last_applied_sync_guc
                    .clone_from(&plan.guc_synchronous_standby_names);
                effects.push(Effect::ReconfigureSynchronousStandbys {
                    guc_value: plan.guc_synchronous_standby_names,
                });
            }
            if dcs_needs_update {
                effects.push(Effect::UpdateSyncState {
                    next: plan.dcs_sync_state.clone(),
                    expected: sync_token,
                });
            }
        }

        self.last_known_sync = Some(plan.dcs_sync_state);
    }

    fn emit_primary_publication_effects(&self, lease: &LeaseHandle, effects: &mut EffectBatch) {
        effects.push(Effect::UpsertMember {
            data: Box::new(self.build_member_data("primary")),
            lease: Some(lease.clone()),
        });

        let status = ClusterStatus {
            optime: self.last_pg.lsn,
            slots: self.last_pg.slots.clone(),
            retain_slots: Vec::new(),
        };
        let failsafe_roster = if self.failsafe_enabled {
            let mut map = BTreeMap::new();
            map.insert(self.self_name.clone(), self.self_api_url.clone());
            for (peer_name, peer_url) in &self.candidate_failsafe_roster {
                map.insert(peer_name.clone(), peer_url.clone());
            }
            Some(map)
        } else {
            None
        };
        effects.push(Effect::PublishLeaderStatus {
            status: status.clone(),
            failsafe_roster,
        });
        effects.push(Effect::SyncSlots {
            status,
            is_primary: true,
            is_demoted_rejoin: false,
        });
    }

    fn reconcile_replica_with_snapshot(
        &mut self,
        snap: &ClusterSnapshot,
        effects: &mut EffectBatch,
    ) {
        let following = match &self.state {
            HaRoleState::Replica { following } => following.clone(),
            _ => None,
        };

        effects.push(Effect::UpsertMember {
            data: Box::new(self.build_member_data("replica")),
            lease: None,
        });

        // 1. Diverged timeline/WAL requires `pg_rewind` against the active leader before streaming.
        if self.last_pg.needs_rewind {
            if let Some(leader_conn) = leader_conn_url(snap) {
                self.state_epoch += 1;
                let s_epoch = self.state_epoch;
                self.state = HaRoleState::Rewinding {
                    target_leader_conn: leader_conn.clone(),
                    state_epoch: s_epoch,
                };
                effects.push(Effect::SpawnJob {
                    state_epoch: s_epoch,
                    job: JobKind::PgRewind,
                    target_conn: Some(leader_conn),
                });
            }
            return;
        }

        if self.last_pg.has_valid_pgdata && !self.last_pg.is_running() {
            effects.push(Effect::StartPostgresAsReplica);
        }

        // 2. Vacant `/leader` -> check failsafe suppression & election eligibility!
        if snap.leader.is_none() {
            if self.accepted_failsafe_leader.is_some() {
                return;
            }
            let profile = LocalCandidateProfile {
                self_name: &self.self_name,
                has_valid_pgdata: self.last_pg.has_valid_pgdata,
                pg_running: self.last_pg.is_running(),
                local_lsn: self.last_pg.lsn,
                local_lag_bytes: self.last_pg.lag_bytes,
                max_lag_on_failover: self.maximum_lag_on_failover,
                nofailover: self.nofailover_tag,
                failover_priority: self.failover_priority_tag,
                sync_mode: self.synchronous_mode,
                sync_mode_strict: self.synchronous_mode_strict,
            };

            if is_eligible_for_election(&profile, snap) {
                let is_targeted_to_self = snap
                    .failover
                    .as_ref()
                    .is_some_and(|fo| fo.value.candidate.as_ref() == Some(&self.self_name));
                let include_failsafe = self.failsafe_enabled || snap.failsafe.is_some();
                let peers = surviving_election_peers(&self.self_name, snap, include_failsafe);
                let needs_fs_intersection = self.failsafe_enabled
                    && snap
                        .failsafe
                        .as_ref()
                        .is_some_and(|fs| fs.len() >= 2 && !fs.contains_key(&self.self_name));
                self.state_epoch += 1;
                self.rpc_epoch += 1;
                if peers.is_empty()
                    && !needs_fs_intersection
                    && (is_targeted_to_self || self.synchronous_mode != SyncMode::Quorum)
                {
                    self.state = HaRoleState::ElectionCandidate {
                        state_epoch: self.state_epoch,
                        peers_verified: true,
                        targeted_failover: is_targeted_to_self,
                    };
                    effects.push(Effect::RenewOrAcquireLeader {
                        rpc_epoch: self.rpc_epoch,
                        lease: None,
                        expected: None,
                        ttl: self.ttl,
                    });
                } else {
                    self.state = HaRoleState::ElectionCandidate {
                        state_epoch: self.state_epoch,
                        peers_verified: false,
                        targeted_failover: is_targeted_to_self,
                    };
                    effects.push(Effect::ProbeElectionPeers {
                        rpc_epoch: self.rpc_epoch,
                        peers,
                        timeout: self.retry_timeout,
                    });
                }
            }
            return;
        }

        // 3. Active `/leader` present -> follow upstream (cascading `replicatefrom` or `/leader`) and sync slots.
        if let Some((upstream_name, upstream_conn)) =
            select_upstream_source(&self.self_name, self.replicatefrom_tag.as_ref(), snap)
            && following.as_ref() != Some(&upstream_name)
        {
            self.state = HaRoleState::Replica {
                following: Some(upstream_name),
            };
            effects.push(Effect::ReconfigureStandby {
                primary_conninfo: upstream_conn,
                slot_name: Some(self.self_name.clone()),
            });
        }
        if let Some(ref status) = snap.status {
            effects.push(Effect::SyncSlots {
                status: status.value.clone(),
                is_primary: false,
                is_demoted_rejoin: false,
            });
        }
    }

    fn handle_operator_command(
        &mut self,
        now: Instant,
        cmd: ControlCommand,
        effects: &mut EffectBatch,
    ) {
        match cmd {
            ControlCommand::Pause => {
                if !matches!(self.state, HaRoleState::Paused { .. }) {
                    self.enter_paused(effects);
                }
            }
            ControlCommand::Resume => {
                if matches!(self.state, HaRoleState::Paused { .. }) {
                    self.resume_from_paused(effects);
                }
            }
            ControlCommand::Switchover { .. } | ControlCommand::Failover { .. } => {
                if let HaRoleState::Primary {
                    ref leader_token,
                    lease_valid_until,
                    ..
                } = self.state
                {
                    let tok = leader_token.clone();
                    let deadline = (now + self.retry_timeout).min(lease_valid_until);
                    self.enter_demoting_with_release(
                        DemoteReason::PlannedSwitchover,
                        deadline,
                        Some(tok),
                        effects,
                    );
                }
            }
            ControlCommand::Reinitialize { .. } => {
                if matches!(self.state, HaRoleState::Replica { .. }) {
                    self.state_epoch += 1;
                    self.last_pg.has_valid_pgdata = false;
                    effects.push(Effect::CancelActiveJobs);
                    self.state = HaRoleState::Unbootstrapped;
                }
            }
            ControlCommand::Reload => {
                self.rpc_epoch += 1;
                effects.push(Effect::FetchDcs {
                    rpc_epoch: self.rpc_epoch,
                    timeout: self.bounded_rpc_timeout(now),
                });
            }
        }
    }

    fn enter_paused(&mut self, effects: &mut EffectBatch) {
        let (underlying, held_leader) = match &self.state {
            HaRoleState::Primary {
                leader_token,
                lease,
                ..
            }
            | HaRoleState::ArmingPrimary {
                leader_token,
                lease,
                ..
            } => (
                PausedRole::Primary,
                Some((lease.clone(), leader_token.clone())),
            ),
            HaRoleState::FailsafePrimary { .. } => (PausedRole::Primary, None),
            HaRoleState::Replica { .. } | HaRoleState::ElectionCandidate { .. } => {
                (PausedRole::Replica, None)
            }
            _ => (PausedRole::Offline, None),
        };
        effects.push(Effect::DisarmWatchdog);
        self.state = HaRoleState::Paused {
            underlying_role: underlying,
            held_leader,
        };
    }

    fn resume_from_paused(&mut self, effects: &mut EffectBatch) {
        if let HaRoleState::Paused {
            underlying_role,
            ref held_leader,
        } = self.state
        {
            match underlying_role {
                PausedRole::Primary if !self.last_pg.in_recovery => {
                    let saved = held_leader.clone();
                    self.state_epoch += 1;
                    self.rpc_epoch += 1;
                    self.state = HaRoleState::ElectionCandidate {
                        state_epoch: self.state_epoch,
                        peers_verified: true,
                        targeted_failover: false,
                    };
                    effects.push(Effect::RenewOrAcquireLeader {
                        rpc_epoch: self.rpc_epoch,
                        lease: saved.as_ref().map(|(l, _)| l.clone()),
                        expected: saved.map(|(_, t)| t),
                        ttl: self.ttl,
                    });
                }
                PausedRole::Primary | PausedRole::Replica => {
                    self.state = HaRoleState::Replica { following: None };
                }
                PausedRole::Offline => {
                    self.state = HaRoleState::Unbootstrapped;
                }
            }
        }
    }

    fn enter_demoting(
        &mut self,
        reason: DemoteReason,
        deadline: Instant,
        effects: &mut EffectBatch,
    ) {
        self.enter_demoting_with_release(reason, deadline, None, effects);
    }

    fn enter_demoting_with_release(
        &mut self,
        reason: DemoteReason,
        deadline: Instant,
        release_leader_on_complete: Option<CasToken>,
        effects: &mut EffectBatch,
    ) {
        if matches!(self.state, HaRoleState::Demoting { .. }) {
            return;
        }
        self.state_epoch += 1;
        let immediate_at = deadline
            .checked_sub(DEMOTE_IMMEDIATE_LEAD)
            .unwrap_or(deadline);
        self.state = HaRoleState::Demoting {
            reason,
            deadline,
            state_epoch: self.state_epoch,
            release_leader_on_complete,
        };
        effects.push(Effect::CancelActiveJobs);
        effects.push(Effect::DemotePostgres {
            state_epoch: self.state_epoch,
            immediate_at,
        });
    }

    /// Returns `true` if this node is an active primary (`now < lease_valid_until`) or holds a
    /// non-expired inbound failsafe lease from an isolated primary.
    pub fn has_active_leader(&self, now: Instant) -> bool {
        match &self.state {
            HaRoleState::Primary {
                lease_valid_until, ..
            }
            | HaRoleState::ArmingPrimary {
                lease_valid_until, ..
            }
            | HaRoleState::FailsafePrimary { lease_valid_until } => now < *lease_valid_until,
            HaRoleState::Paused {
                underlying_role: PausedRole::Primary,
                ..
            } => true,
            _ => self
                .accepted_failsafe_leader
                .as_ref()
                .is_some_and(|(_, until)| now < *until),
        }
    }

    fn build_member_data(&self, role: &str) -> MemberData {
        MemberData {
            conn_url: self.self_conn_url.clone(),
            api_url: self.self_api_url.clone(),
            state: self.last_pg.state_label.clone(),
            role: CompactString::from(role),
            version: CompactString::const_new("4.1.0"),
            xlog_location: Some(self.last_pg.lsn),
            timeline: (self.last_pg.timeline > 0).then_some(self.last_pg.timeline),
            replication_state: (role == "replica" && self.last_pg.is_running())
                .then(|| CompactString::const_new("streaming")),
            pending_restart: self.last_pg.pending_restart,
            pending_restart_reason: BTreeMap::new(),
            pause: matches!(self.state, HaRoleState::Paused { .. }).then_some(true),
            watchdog_failed: None,
            tags: self.member_tags_json.clone(),
            extra: BTreeMap::new(),
        }
    }

    pub fn publish_snapshot(&self, now: Instant) -> PublishedNodeState {
        let pg_running_rw = self.last_pg.is_running() && !self.last_pg.in_recovery;
        let is_sync = self
            .last_known_sync
            .as_ref()
            .is_some_and(|s| s.contains_standby(&self.self_name));
        let is_quorum = is_sync && self.synchronous_mode == SyncMode::Quorum;

        let (role, is_rw, holds_leader_lease) = match &self.state {
            HaRoleState::Primary {
                lease_valid_until, ..
            }
            | HaRoleState::FailsafePrimary { lease_valid_until } => {
                let valid = now < *lease_valid_until;
                (
                    CompactString::const_new("primary"),
                    pg_running_rw && valid,
                    valid,
                )
            }
            HaRoleState::Paused {
                underlying_role: PausedRole::Primary,
                held_leader,
            } => (
                CompactString::const_new("primary"),
                pg_running_rw,
                held_leader.is_some() || pg_running_rw,
            ),
            HaRoleState::StandbyLeader {
                lease_valid_until, ..
            } => {
                let valid = now < *lease_valid_until;
                (CompactString::const_new("standby_leader"), false, valid)
            }
            HaRoleState::Replica { .. }
            | HaRoleState::ElectionCandidate { .. }
            | HaRoleState::Rewinding { .. }
            | HaRoleState::Paused {
                underlying_role: PausedRole::Replica,
                ..
            } => (CompactString::const_new("replica"), false, false),
            _ => (CompactString::const_new("uninitialized"), false, false),
        };

        let postgres_state_code =
            if self.last_pg.is_running() && self.last_pg.postgres_state_code == 12 {
                5
            } else {
                self.last_pg.postgres_state_code
            };

        let failsafe_mode_is_active = matches!(self.state, HaRoleState::FailsafePrimary { .. })
            || self
                .accepted_failsafe_leader
                .as_ref()
                .is_some_and(|(_, until)| now < *until);

        let cluster_unlocked =
            self.last_observed_dcs_leader.is_none() && !self.has_active_leader(now);

        PublishedNodeState {
            scope: self.scope.clone(),
            member_name: self.self_name.clone(),
            state: self.last_pg.state_label.clone(),
            role,
            is_read_write_valid: is_rw,
            holds_leader_lease,
            is_sync_standby: self.last_pg.is_sync_standby || is_sync,
            is_quorum_standby: self.last_pg.is_quorum_standby || is_quorum,
            noloadbalance: self.noloadbalance_tag,
            replication_lag_bytes: self.last_pg.lag_bytes,
            timeline: self.last_pg.timeline,
            pending_restart: self.last_pg.pending_restart,
            pause: matches!(self.state, HaRoleState::Paused { .. }),
            ttl: self.ttl,
            last_ha_tick: self.last_tick_at,
            dcs_last_seen: self.last_dcs_seen_at,
            postgres_state_code,
            xlog_location: self.last_pg.lsn,
            xlog_paused: self.last_pg.xlog_paused,
            postmaster_start_time_epoch: self.last_pg.postmaster_start_time_epoch,
            xlog_replayed_timestamp_epoch: self.last_pg.xlog_replayed_timestamp_epoch,
            server_version_num: self.last_pg.server_version_num,
            database_system_identifier: self.last_pg.system_identifier.clone(),
            cluster_unlocked,
            failsafe_enabled: self.failsafe_enabled,
            failsafe_mode_is_active,
            user_tags: self.user_tags.clone(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use negroni_config::parse_and_validate_with_env;
    use negroni_types::Versioned;

    use super::*;

    fn test_node(name: &str, boot: Instant) -> NodeCtx {
        let yaml = format!(
            "scope: test-ha\nname: {name}\nbootstrap:\n  dcs:\n    ttl: 30\n    loop_wait: 10\n    retry_timeout: 10\n    failsafe_mode: true\n"
        );
        let empty: [(&str, &str); 0] = [];
        let cfg = parse_and_validate_with_env(&yaml, "patroni.yml", empty).unwrap();
        NodeCtx::from_config(&cfg, boot)
    }

    #[test]
    fn arming_primary_gates_postgres_promotion_and_fences_on_watchdog_failure() {
        let t0 = Instant::now();
        let mut ctx = test_node("pg-0", t0);
        ctx.last_pg = LocalPgSummary {
            has_valid_pgdata: true,
            state_label: CompactString::const_new("running"),
            in_recovery: true,
            lsn: WalLsn(4_096),
            timeline: 1,
            ..Default::default()
        };
        ctx.state = HaRoleState::Replica { following: None };

        let (_, _) = ctx.step(HaEvent::Tick { now: t0 });
        let rpc = ctx.rpc_epoch;
        let (effects, snap) = ctx.step(HaEvent::DcsFetched {
            rpc_epoch: rpc,
            attempt_started_at: t0,
            now: t0 + Duration::from_millis(20),
            result: Ok(Box::new(ClusterSnapshot::default())),
        });
        assert!(!snap.is_read_write_valid);
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::RenewOrAcquireLeader { .. }))
        );

        // Win `/leader`: must enter `ArmingPrimary` and NOT emit `PromotePostgres` yet!
        let lease_rpc = ctx.rpc_epoch;
        let (effects, snap) = ctx.step(HaEvent::LeaderLeaseResult {
            rpc_epoch: lease_rpc,
            attempt_started_at: t0 + Duration::from_millis(20),
            now: t0 + Duration::from_millis(50),
            result: Ok((
                LeaseHandle(CompactString::const_new("L1")),
                CasToken::Revision(1),
            )),
        });
        assert!(matches!(ctx.state, HaRoleState::ArmingPrimary { .. }));
        assert!(!snap.is_read_write_valid);
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::ArmAndFeedWatchdog { .. }))
        );
        assert!(
            !effects
                .iter()
                .any(|e| matches!(e, Effect::PromotePostgres { .. }))
        );

        // Simulate `WatchdogArmFailed`: must release `/leader` and immediately demote!
        let arm_epoch = ctx.state_epoch;
        let (effects, _) = ctx.step(HaEvent::WatchdogArmFailed {
            state_epoch: arm_epoch,
            now: t0 + Duration::from_millis(60),
        });
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::ReleaseLeader { .. }))
        );
        assert!(matches!(
            ctx.state,
            HaRoleState::Demoting {
                reason: DemoteReason::WatchdogArmFailed,
                ..
            }
        ));
    }

    #[test]
    fn zero_downtime_startup_adopts_running_primary_or_fences_zombie_primary() {
        let t0 = Instant::now();
        let mut ctx = test_node("pg-0", t0);
        ctx.last_pg = LocalPgSummary {
            has_valid_pgdata: true,
            state_label: CompactString::const_new("running"),
            in_recovery: false,
            lsn: WalLsn(8_192),
            timeline: 2,
            ..Default::default()
        };

        // Case 1: `/leader` belongs to `pg-0` -> zero-downtime adoption!
        let snap = Box::new(ClusterSnapshot {
            leader: Some(Versioned {
                value: CompactString::const_new("pg-0"),
                token: CasToken::Revision(7),
            }),
            ..Default::default()
        });
        ctx.step(HaEvent::Tick { now: t0 });
        let rpc = ctx.rpc_epoch;
        let (effects, _) = ctx.step(HaEvent::DcsFetched {
            rpc_epoch: rpc,
            attempt_started_at: t0,
            now: t0,
            result: Ok(snap),
        });
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::RenewOrAcquireLeader {
                expected: Some(CasToken::Revision(7)),
                ..
            }
        )));

        // Case 2: Another node (`pg-1`) holds `/leader` while `pg-0` booted `!in_recovery` -> fence!
        let mut zombie = test_node("pg-0", t0);
        zombie.last_pg = ctx.last_pg.clone();
        let conflicting_snap = Box::new(ClusterSnapshot {
            leader: Some(Versioned {
                value: CompactString::const_new("pg-1"),
                token: CasToken::Revision(9),
            }),
            ..Default::default()
        });
        zombie.step(HaEvent::Tick { now: t0 });
        let z_rpc = zombie.rpc_epoch;
        let (effects, pub_snap) = zombie.step(HaEvent::DcsFetched {
            rpc_epoch: z_rpc,
            attempt_started_at: t0,
            now: t0,
            result: Ok(conflicting_snap),
        });
        assert!(!pub_snap.is_read_write_valid);
        assert!(
            effects
                .iter()
                .any(|e| matches!(e, Effect::DemotePostgres { .. }))
        );
    }

    #[test]
    fn pause_and_resume_preserve_primary_role_without_demoting_postgres() {
        let t0 = Instant::now();
        let mut ctx = test_node("pg-0", t0);
        ctx.last_pg = LocalPgSummary {
            has_valid_pgdata: true,
            state_label: CompactString::const_new("running"),
            in_recovery: false,
            lsn: WalLsn(16_384),
            timeline: 1,
            ..Default::default()
        };
        ctx.state = HaRoleState::Primary {
            leader_token: CasToken::Revision(5),
            lease: LeaseHandle(CompactString::const_new("L5")),
            lease_valid_until: t0 + Duration::from_secs(25),
        };

        let (effects, snap) = ctx.step(HaEvent::OperatorCommand {
            now: t0 + Duration::from_secs(1),
            cmd: ControlCommand::Pause,
        });
        assert!(snap.pause);
        assert_eq!(snap.role, "primary");
        assert!(snap.is_read_write_valid);
        assert!(effects.iter().any(|e| matches!(e, Effect::DisarmWatchdog)));

        // Resume must re-acquire/renew `/leader` with held token and re-arm watchdog without demoting!
        let (effects, _) = ctx.step(HaEvent::OperatorCommand {
            now: t0 + Duration::from_secs(10),
            cmd: ControlCommand::Resume,
        });
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::RenewOrAcquireLeader {
                expected: Some(CasToken::Revision(5)),
                ..
            }
        )));
    }
}
