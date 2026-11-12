#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};
use std::time::{Duration, Instant};

use compact_str::CompactString;
use negroni_config::{SyncMode, parse_and_validate_with_env};
use negroni_ha::{
    Effect, HaEvent, HaRoleState, JobKind, LocalPgSummary, NodeCtx, PeerElectionSample,
    PublishedNodeState,
};
use negroni_types::{
    CasToken, ClusterSnapshot, ClusterStatus, FailoverIntent, HistoryEntry, LeaseHandle,
    MemberData, SyncState, Versioned, WalLsn,
};
use proptest::prelude::*;

// ============================================================================
// Authoritative In-Memory DCS (`SimDcs`) with Exact Server-Side Lease Expiry
// ============================================================================

#[derive(Debug, Clone)]
struct SimLease {
    expires_at: Instant,
}

#[derive(Debug, Default, Clone)]
struct SimDcs {
    next_rev: u64,
    next_lease_id: u64,
    leases: BTreeMap<CompactString, SimLease>,
    initialize: Option<Versioned<CompactString>>,
    leader: Option<(Versioned<CompactString>, LeaseHandle)>,
    members: BTreeMap<CompactString, (Versioned<MemberData>, Option<LeaseHandle>)>,
    config: Option<Versioned<serde_json::Value>>,
    status: Option<Versioned<ClusterStatus>>,
    sync: Option<Versioned<SyncState>>,
    failover: Option<Versioned<FailoverIntent>>,
    failsafe: Option<BTreeMap<CompactString, String>>,
    history: Vec<HistoryEntry>,
}

impl SimDcs {
    fn alloc_token(&mut self) -> CasToken {
        self.next_rev += 1;
        CasToken::Revision(self.next_rev)
    }

    fn expire_leases(&mut self, now: Instant) {
        self.leases.retain(|_, l| now < l.expires_at);
        if let Some((_, ref leader_lease)) = self.leader
            && !self.leases.contains_key(&leader_lease.0)
        {
            self.leader = None;
        }
        self.members.retain(|_, (_, lease_opt)| match lease_opt {
            Some(l) => self.leases.contains_key(&l.0),
            None => true,
        });
    }

    fn snapshot(&mut self, now: Instant) -> ClusterSnapshot {
        self.expire_leases(now);
        ClusterSnapshot {
            initialize: self.initialize.clone(),
            leader: self.leader.as_ref().map(|(v, _)| v.clone()),
            members: self
                .members
                .iter()
                .map(|(k, (v, _))| (k.clone(), v.clone()))
                .collect(),
            config: self.config.clone(),
            status: self.status.clone(),
            sync: self.sync.clone(),
            failover: self.failover.clone(),
            history: self.history.clone(),
            failsafe: self.failsafe.clone(),
        }
    }

    fn ensure_lease(
        &mut self,
        now: Instant,
        ttl: Duration,
        existing: Option<&LeaseHandle>,
    ) -> LeaseHandle {
        self.expire_leases(now);
        if let Some(h) = existing
            && let Some(entry) = self.leases.get_mut(&h.0)
        {
            entry.expires_at = now + ttl;
            return h.clone();
        }
        self.next_lease_id += 1;
        let id = CompactString::from(format!("sim-lease-{}", self.next_lease_id));
        self.leases.insert(
            id.clone(),
            SimLease {
                expires_at: now + ttl,
            },
        );
        LeaseHandle(id)
    }

    fn acquire_or_renew_leader(
        &mut self,
        now: Instant,
        node_name: &CompactString,
        lease: &LeaseHandle,
        expected: Option<&CasToken>,
    ) -> Result<CasToken, String> {
        self.expire_leases(now);
        if !self.leases.contains_key(&lease.0) {
            return Err("lease expired on server".to_owned());
        }
        match (&self.leader, expected) {
            (None, None) => {
                let tok = self.alloc_token();
                self.leader = Some((
                    Versioned {
                        value: node_name.clone(),
                        token: tok.clone(),
                    },
                    lease.clone(),
                ));
                Ok(tok)
            }
            (Some((curr, _)), Some(exp)) if curr.value == *node_name && &curr.token == exp => {
                let tok = self.alloc_token();
                self.leader = Some((
                    Versioned {
                        value: node_name.clone(),
                        token: tok.clone(),
                    },
                    lease.clone(),
                ));
                Ok(tok)
            }
            (None, Some(_)) => Err("CAS failed: /leader already expired".to_owned()),
            (Some((curr, _)), _) => Err(format!("CAS failed: /leader held by {}", curr.value)),
        }
    }

    fn release_leader(&mut self, now: Instant, node_name: &CompactString, expected: &CasToken) {
        self.expire_leases(now);
        if let Some((ref curr, _)) = self.leader
            && curr.value == *node_name
            && &curr.token == expected
        {
            self.leader = None;
        }
    }

    fn cas_initialize(&mut self, sys_id: &CompactString) -> bool {
        if self.initialize.is_none() {
            let tok = self.alloc_token();
            self.initialize = Some(Versioned {
                value: sys_id.clone(),
                token: tok,
            });
            true
        } else {
            false
        }
    }

    fn upsert_member(
        &mut self,
        now: Instant,
        node_name: &CompactString,
        data: MemberData,
        lease: Option<LeaseHandle>,
        ttl: Duration,
    ) {
        self.expire_leases(now);
        let bound_lease = Some(self.ensure_lease(now, ttl, lease.as_ref()));
        let tok = self.alloc_token();
        self.members.insert(
            node_name.clone(),
            (
                Versioned {
                    value: data,
                    token: tok,
                },
                bound_lease,
            ),
        );
    }

    fn update_sync(&mut self, next: SyncState, expected: Option<&CasToken>) {
        let matches = match (&self.sync, expected) {
            (None, None) => true,
            (Some(curr), Some(exp)) => &curr.token == exp,
            _ => false,
        };
        if matches {
            let tok = self.alloc_token();
            self.sync = Some(Versioned {
                value: next,
                token: tok,
            });
        }
    }

    fn set_failover(&mut self, intent: FailoverIntent) {
        let tok = self.alloc_token();
        self.failover = Some(Versioned {
            value: intent,
            token: tok,
        });
    }

    fn clear_failover(&mut self, expected: &CasToken) {
        if self.failover.as_ref().is_some_and(|f| &f.token == expected) {
            self.failover = None;
        }
    }
}

// ============================================================================
// Simulated Node Hardware: PostgreSQL Process + Kernel `/dev/watchdog`
// ============================================================================

#[derive(Debug, Clone)]
struct SimNodeHw {
    has_valid_pgdata: bool,
    pg_running: bool,
    in_recovery: bool,
    lsn: WalLsn,
    timeline: u64,
    diverged_from_cluster: bool,
    following_conn: Option<String>,
    watchdog_armed: bool,
    watchdog_epoch: u64,
    watchdog_valid_until: Option<Instant>,
    frozen_until: Option<Instant>,
    demote_delay_ms: u64,
    active_job_epoch: Option<u64>,
    watchdog_resets: u64,
}

impl Default for SimNodeHw {
    fn default() -> Self {
        Self {
            has_valid_pgdata: false,
            pg_running: false,
            in_recovery: true,
            lsn: WalLsn::ZERO,
            timeline: 0,
            diverged_from_cluster: false,
            following_conn: None,
            watchdog_armed: false,
            watchdog_epoch: 0,
            watchdog_valid_until: None,
            frozen_until: None,
            demote_delay_ms: 150,
            active_job_epoch: None,
            watchdog_resets: 0,
        }
    }
}

// ============================================================================
// Discrete-Event Scheduler (`SimAction` + `ScheduledItem`)
// ============================================================================

#[derive(Debug, Clone)]
enum SimAction {
    PeriodicTick {
        node: usize,
    },
    DeliverHaEvent {
        node: usize,
        event: HaEvent,
    },
    ExecuteDcsFetch {
        node: usize,
        rpc_epoch: u64,
        attempt_started_at: Instant,
        timeout: Duration,
    },
    ExecuteLeaderLease {
        node: usize,
        rpc_epoch: u64,
        attempt_started_at: Instant,
        lease: Option<LeaseHandle>,
        expected: Option<CasToken>,
        ttl: Duration,
    },
    ExecuteElectionProbe {
        node: usize,
        rpc_epoch: u64,
        peers: Vec<CompactString>,
        timeout: Duration,
    },
    ExecuteFailsafeProbe {
        node: usize,
        rpc_epoch: u64,
        attempt_started_at: Instant,
        peers: Vec<CompactString>,
        timeout: Duration,
    },
    CompletePostgresDemotion {
        node: usize,
        state_epoch: u64,
    },
    CompletePostgresPromotion {
        node: usize,
        state_epoch: u64,
    },
    CompleteBackgroundJob {
        node: usize,
        state_epoch: u64,
        job: JobKind,
        target_conn: Option<String>,
    },
    InjectFault(FaultOp),
}

#[derive(Debug, Clone)]
pub enum FaultOp {
    SetDcsLink {
        node: usize,
        up: bool,
    },
    SetPeerLink {
        a: usize,
        b: usize,
        up: bool,
    },
    SetBaseRttMs {
        rtt_ms: u64,
    },
    SetDemoteDelayMs {
        node: usize,
        delay_ms: u64,
    },
    FreezeNode {
        node: usize,
        duration_ms: u64,
    },
    TriggerSwitchover {
        leader: Option<usize>,
        candidate: Option<usize>,
    },
    HealAll,
}

#[derive(Debug, Clone)]
struct ScheduledItem {
    at: Instant,
    seq: u64,
    action: SimAction,
}

impl PartialEq for ScheduledItem {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at && self.seq == other.seq
    }
}
impl Eq for ScheduledItem {}

impl PartialOrd for ScheduledItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduledItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap by `(at, seq)` for strict determinism.
        other
            .at
            .cmp(&self.at)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

// ============================================================================
// Deterministic Cluster Simulator (`SimCluster`)
// ============================================================================

struct SimCluster {
    t0: Instant,
    now: Instant,
    next_seq: u64,
    queue: BinaryHeap<ScheduledItem>,
    dcs: SimDcs,
    nodes: Vec<NodeCtx>,
    hw: Vec<SimNodeHw>,
    published: Vec<PublishedNodeState>,
    dcs_up: Vec<bool>,
    lan_up: Vec<Vec<bool>>,
    base_rtt_ms: u64,
    cluster_highest_timeline: u64,
    committed_sync_lsn: WalLsn,
}

impl SimCluster {
    fn new(node_count: usize, failsafe_mode: bool, sync_mode: SyncMode, sync_count: usize) -> Self {
        let t0 = Instant::now();
        let mut nodes = Vec::with_capacity(node_count);
        let mut hw = Vec::with_capacity(node_count);
        let mut published = Vec::with_capacity(node_count);

        let sync_yaml = match sync_mode {
            SyncMode::Off => "false",
            SyncMode::Priority => "true",
            SyncMode::Quorum => "\"quorum\"",
        };

        for i in 0..node_count {
            let yaml = format!(
                r#"
scope: sim-cluster
name: pg-{i}
namespace: /service
etcd3:
  hosts: ["127.0.0.1:2379"]
restapi:
  listen: 127.0.0.1:800{i}
  connect_address: 127.0.0.1:800{i}
postgresql:
  listen: 127.0.0.1:543{i}
  connect_address: 127.0.0.1:543{i}
  data_dir: /var/lib/postgresql/data-{i}
watchdog:
  mode: required
  safety_margin: 5
bootstrap:
  dcs:
    ttl: 30
    loop_wait: 10
    retry_timeout: 10
    failsafe_mode: {failsafe_mode}
    synchronous_mode: {sync_yaml}
    synchronous_node_count: {sync_count}
"#
            );
            let empty_env: [(&str, &str); 0] = [];
            let cfg = parse_and_validate_with_env(&yaml, "sim.yml", empty_env).unwrap();
            let ctx = NodeCtx::from_config(&cfg, t0);
            published.push(ctx.publish_snapshot(t0));
            nodes.push(ctx);
            hw.push(SimNodeHw::default());
        }

        let mut sim = Self {
            t0,
            now: t0,
            next_seq: 0,
            queue: BinaryHeap::new(),
            dcs: SimDcs::default(),
            nodes,
            hw,
            published,
            dcs_up: vec![true; node_count],
            lan_up: vec![vec![true; node_count]; node_count],
            base_rtt_ms: 20,
            cluster_highest_timeline: 0,
            committed_sync_lsn: WalLsn::ZERO,
        };

        // Stagger initial ticks slightly (0ms, 35ms, 70ms, ...) just like independent hosts booting.
        for i in 0..node_count {
            sim.schedule(
                t0 + Duration::from_millis((i as u64) * 35),
                SimAction::PeriodicTick { node: i },
            );
        }

        sim
    }

    fn schedule(&mut self, at: Instant, action: SimAction) {
        self.next_seq += 1;
        self.queue.push(ScheduledItem {
            at: at.max(self.now),
            seq: self.next_seq,
            action,
        });
    }

    fn node_index_by_name(&self, name: &str) -> Option<usize> {
        self.nodes.iter().position(|n| n.self_name == name)
    }

    fn node_index_by_conn(&self, conn_url: &str) -> Option<usize> {
        self.nodes.iter().position(|n| n.self_conn_url == conn_url)
    }

    fn is_frozen(&self, node: usize) -> bool {
        self.hw[node]
            .frozen_until
            .is_some_and(|until| self.now < until)
    }

    /// Enforces hardware `/dev/watchdog` resets and simulates WAL generation + streaming replication.
    fn advance_hardware_and_wal(&mut self) {
        self.dcs.expire_leases(self.now);

        // 1. Hardware `/dev/watchdog`: fires unconditionally in kernel space even if Tokio is frozen!
        for i in 0..self.nodes.len() {
            if self.hw[i].watchdog_armed
                && let Some(valid_until) = self.hw[i].watchdog_valid_until
                && self.now >= valid_until
            {
                // Host reset! Postgres dies immediately; host reboots cleanly with existing `$PGDATA` on disk.
                let was_rw = self.hw[i].pg_running && !self.hw[i].in_recovery;
                self.hw[i].pg_running = false;
                self.hw[i].in_recovery = true;
                self.hw[i].watchdog_armed = false;
                self.hw[i].watchdog_valid_until = None;
                self.hw[i].frozen_until = None;
                self.hw[i].active_job_epoch = None;
                self.hw[i].watchdog_resets += 1;
                if was_rw {
                    self.hw[i].diverged_from_cluster = true;
                }

                // Re-create fresh `NodeCtx` representing post-reboot daemon startup with existing `$PGDATA`.
                let mut rebooted = self.rebuild_clean_node_ctx(i);
                let summary = self.build_pg_summary(i);
                let (effects, snap) = rebooted.step(HaEvent::PgObservation {
                    now: self.now,
                    telemetry: summary,
                });
                self.nodes[i] = rebooted;
                self.published[i] = snap;
                self.apply_effects(i, effects);
            }
        }

        // 2. Refresh published probe state for wall-clock expiry (`now >= lease_valid_until`).
        for i in 0..self.nodes.len() {
            if !self.is_frozen(i) {
                self.published[i] = self.nodes[i].publish_snapshot(self.now);
            } else if let Some(valid_until) = self.hw[i].watchdog_valid_until
                && self.now >= valid_until
            {
                self.published[i].is_read_write_valid = false;
            }
        }

        // 3. Simulate WAL writes on active RW primary and replication to connected standbys.
        let mut active_primary_idx = None;
        for i in 0..self.nodes.len() {
            if self.hw[i].pg_running
                && !self.hw[i].in_recovery
                && self.published[i].is_read_write_valid
            {
                active_primary_idx = Some(i);
                break;
            }
        }

        if let Some(p_idx) = active_primary_idx {
            // Advance primary WAL by 64 bytes on active steps.
            self.hw[p_idx].lsn = WalLsn(self.hw[p_idx].lsn.0 + 64);
            let p_lsn = self.hw[p_idx].lsn;
            let p_tl = self.hw[p_idx].timeline;

            // Stream WAL to reachable standbys following this primary.
            let mut synced_voters = 0usize;
            for r in 0..self.nodes.len() {
                if r == p_idx || !self.hw[r].pg_running || !self.hw[r].in_recovery {
                    continue;
                }
                if self.hw[r].diverged_from_cluster && p_tl > self.hw[r].timeline {
                    continue;
                }
                let follows_p = self.hw[r]
                    .following_conn
                    .as_deref()
                    .and_then(|c| self.node_index_by_conn(c))
                    == Some(p_idx);
                if follows_p && self.lan_up[p_idx][r] && self.lan_up[r][p_idx] {
                    self.hw[r].lsn = p_lsn;
                    self.hw[r].timeline = p_tl;
                    if self
                        .dcs
                        .sync
                        .as_ref()
                        .is_some_and(|s| s.value.contains_standby(&self.nodes[r].self_name))
                    {
                        synced_voters += 1;
                    }
                }
            }

            if self.nodes[p_idx].synchronous_mode.is_enabled() {
                let needed = self.nodes[p_idx].synchronous_node_count.max(1);
                if synced_voters >= needed {
                    self.committed_sync_lsn = self.committed_sync_lsn.max(p_lsn);
                }
            }
        }
    }

    fn rebuild_clean_node_ctx(&self, i: usize) -> NodeCtx {
        let failsafe = self.nodes[i].failsafe_enabled;
        let sync_yaml = match self.nodes[i].synchronous_mode {
            SyncMode::Off => "false",
            SyncMode::Priority => "true",
            SyncMode::Quorum => "\"quorum\"",
        };
        let sync_count = self.nodes[i].synchronous_node_count;
        let yaml = format!(
            r#"
scope: sim-cluster
name: pg-{i}
namespace: /service
etcd3:
  hosts: ["127.0.0.1:2379"]
restapi:
  listen: 127.0.0.1:800{i}
  connect_address: 127.0.0.1:800{i}
postgresql:
  listen: 127.0.0.1:543{i}
  connect_address: 127.0.0.1:543{i}
  data_dir: /var/lib/postgresql/data-{i}
watchdog:
  mode: required
  safety_margin: 5
bootstrap:
  dcs:
    ttl: 30
    loop_wait: 10
    retry_timeout: 10
    failsafe_mode: {failsafe}
    synchronous_mode: {sync_yaml}
    synchronous_node_count: {sync_count}
"#
        );
        let empty_env: [(&str, &str); 0] = [];
        let cfg = parse_and_validate_with_env(&yaml, "sim.yml", empty_env).unwrap();
        NodeCtx::from_config(&cfg, self.now)
    }

    fn build_pg_summary(&self, i: usize) -> LocalPgSummary {
        let h = &self.hw[i];
        let leader_optime = self.dcs.status.as_ref().map_or(h.lsn, |s| s.value.optime);
        let needs_rewind =
            h.diverged_from_cluster && self.cluster_highest_timeline > h.timeline && h.timeline > 0;

        LocalPgSummary {
            has_valid_pgdata: h.has_valid_pgdata,
            state_label: if h.pg_running {
                CompactString::const_new("running")
            } else {
                CompactString::const_new("stopped")
            },
            in_recovery: h.in_recovery,
            lsn: h.lsn,
            timeline: h.timeline,
            lag_bytes: if h.in_recovery {
                h.lsn.saturating_lag_from(leader_optime)
            } else {
                0
            },
            is_sync_standby: false,
            is_quorum_standby: false,
            pending_restart: false,
            needs_rewind,
            system_identifier: h
                .has_valid_pgdata
                .then(|| CompactString::const_new("7400000000000000001")),
            slots: BTreeMap::new(),
            ..LocalPgSummary::default()
        }
    }

    /// Asserts Single-Writer Safety Invariants at the current virtual instant `self.now`.
    fn assert_safety_invariants(&self) {
        // Invariant 1a: At most ONE node publishes `is_read_write_valid == true` at any instant.
        let valid_rw_probes: Vec<&str> = self
            .published
            .iter()
            .enumerate()
            .filter_map(|(i, snap)| {
                let effective_rw = snap.is_read_write_valid
                    && self.hw[i]
                        .watchdog_valid_until
                        .is_none_or(|until| self.now < until);
                effective_rw.then_some(self.nodes[i].self_name.as_str())
            })
            .collect();
        assert!(
            valid_rw_probes.len() <= 1,
            "SPLIT-BRAIN PROBE VIOLATION at +{:?}: multiple nodes advertise is_read_write_valid: {:?}",
            self.now.saturating_duration_since(self.t0),
            valid_rw_probes
        );

        // Invariant 1b: At most ONE running read-write PostgreSQL process (`pg_running && !in_recovery`)
        // exists across the entire cluster at ANY virtual millisecond!
        let rw_postmasters: Vec<(&str, &HaRoleState)> = self
            .hw
            .iter()
            .enumerate()
            .filter_map(|(i, h)| {
                (h.pg_running && !h.in_recovery)
                    .then_some((self.nodes[i].self_name.as_str(), &self.nodes[i].state))
            })
            .collect();
        assert!(
            rw_postmasters.len() <= 1,
            "SPLIT-BRAIN POSTGRES VIOLATION at +{:?}: multiple running RW postmasters: {:?}",
            self.now.saturating_duration_since(self.t0),
            rw_postmasters
        );
    }

    fn step_until(&mut self, end_offset: Duration) {
        let horizon = self.t0 + end_offset;
        while let Some(item) = self.queue.peek() {
            if item.at > horizon {
                break;
            }
            let item = self.queue.pop().unwrap();
            self.now = item.at;
            self.advance_hardware_and_wal();
            self.assert_safety_invariants();
            self.process_action(item.action);
            self.advance_hardware_and_wal();
            self.assert_safety_invariants();
        }
        self.now = horizon;
        self.advance_hardware_and_wal();
        self.assert_safety_invariants();
    }

    fn deliver_event(&mut self, node: usize, event: HaEvent) {
        if self.is_frozen(node) {
            return;
        }
        // Feed fresh local PG telemetry prior to HA ticks just like `coordinator_loop`.
        if matches!(event, HaEvent::Tick { .. }) {
            let telemetry = self.build_pg_summary(node);
            let (obs_fx, obs_snap) = self.nodes[node].step(HaEvent::PgObservation {
                now: self.now,
                telemetry,
            });
            self.published[node] = obs_snap;
            self.apply_effects(node, obs_fx);
        }

        let (effects, snap) = self.nodes[node].step(event);
        self.published[node] = snap;
        self.apply_effects(node, effects);
    }

    fn process_action(&mut self, action: SimAction) {
        match action {
            SimAction::PeriodicTick { node } => {
                let next_tick = self.now + self.nodes[node].loop_wait;
                self.schedule(next_tick, SimAction::PeriodicTick { node });
                self.deliver_event(node, HaEvent::Tick { now: self.now });
            }

            SimAction::DeliverHaEvent { node, event } => {
                self.deliver_event(node, event);
            }

            SimAction::ExecuteDcsFetch {
                node,
                rpc_epoch,
                attempt_started_at,
                timeout,
            } => {
                let half_rtt = Duration::from_millis(self.base_rtt_ms / 2);
                if self.dcs_up[node] && half_rtt * 2 <= timeout {
                    let snap = self.dcs.snapshot(self.now);
                    let reply_at = self.now + half_rtt;
                    self.schedule(
                        reply_at,
                        SimAction::DeliverHaEvent {
                            node,
                            event: HaEvent::DcsFetched {
                                rpc_epoch,
                                attempt_started_at,
                                now: reply_at,
                                result: Ok(Box::new(snap)),
                            },
                        },
                    );
                } else {
                    let fail_delay = timeout.min(Duration::from_millis(400));
                    let reply_at = attempt_started_at + fail_delay;
                    self.schedule(
                        reply_at,
                        SimAction::DeliverHaEvent {
                            node,
                            event: HaEvent::DcsFetched {
                                rpc_epoch,
                                attempt_started_at,
                                now: reply_at,
                                result: Err("simulated DCS partition".to_owned()),
                            },
                        },
                    );
                }
            }

            SimAction::ExecuteLeaderLease {
                node,
                rpc_epoch,
                attempt_started_at,
                lease,
                expected,
                ttl,
            } => {
                let half_rtt = Duration::from_millis(self.base_rtt_ms / 2);
                let reply_at = self.now + half_rtt;
                let result = if self.dcs_up[node] {
                    let lse = self.dcs.ensure_lease(self.now, ttl, lease.as_ref());
                    let self_name = self.nodes[node].self_name.clone();
                    self.dcs
                        .acquire_or_renew_leader(self.now, &self_name, &lse, expected.as_ref())
                        .map(|tok| (lse, tok))
                } else {
                    Err("DCS unreachable during leader lease RPC".to_owned())
                };
                self.schedule(
                    reply_at,
                    SimAction::DeliverHaEvent {
                        node,
                        event: HaEvent::LeaderLeaseResult {
                            rpc_epoch,
                            attempt_started_at,
                            now: reply_at,
                            result,
                        },
                    },
                );
            }

            SimAction::ExecuteElectionProbe {
                node,
                rpc_epoch,
                peers,
                timeout,
            } => {
                let mut samples = Vec::new();
                let rtt = Duration::from_millis(self.base_rtt_ms);
                if rtt <= timeout {
                    for peer_name in &peers {
                        if let Some(p_idx) = self.node_index_by_name(peer_name)
                            && self.lan_up[node][p_idx]
                            && self.lan_up[p_idx][node]
                            && !self.is_frozen(p_idx)
                        {
                            let peer_pg = self.build_pg_summary(p_idx);
                            let can_promote = peer_pg.has_valid_pgdata
                                && peer_pg.is_running()
                                && !peer_pg.needs_rewind
                                && !matches!(self.nodes[p_idx].state, HaRoleState::Paused { .. });
                            samples.push(PeerElectionSample {
                                member_name: peer_name.clone(),
                                lsn: self.hw[p_idx].lsn,
                                failover_priority: self.nodes[p_idx].failover_priority_tag,
                                nofailover: self.nodes[p_idx].nofailover_tag,
                                can_promote,
                                has_active_leader: self.nodes[p_idx].has_active_leader(self.now),
                            });
                        }
                    }
                }
                let reply_at = self.now + rtt.min(timeout);
                self.schedule(
                    reply_at,
                    SimAction::DeliverHaEvent {
                        node,
                        event: HaEvent::ElectionPeersProbed {
                            rpc_epoch,
                            now: reply_at,
                            samples,
                        },
                    },
                );
            }

            SimAction::ExecuteFailsafeProbe {
                node,
                rpc_epoch,
                attempt_started_at,
                peers,
                timeout,
            } => {
                let rtt = Duration::from_millis(self.base_rtt_ms);
                let mut all_ok = !peers.is_empty() && rtt <= timeout;
                let leader_name = self.nodes[node].self_name.clone();
                let conn_url = self.nodes[node].self_conn_url.clone();
                let lsn = self.hw[node].lsn;

                if all_ok {
                    for peer_name in &peers {
                        let Some(p_idx) = self.node_index_by_name(peer_name) else {
                            all_ok = false;
                            continue;
                        };
                        if !self.lan_up[node][p_idx]
                            || !self.lan_up[p_idx][node]
                            || self.is_frozen(p_idx)
                        {
                            all_ok = false;
                            continue;
                        }
                        let (peer_fx, peer_snap) =
                            self.nodes[p_idx].step(HaEvent::InboundFailsafeHeartbeat {
                                now: self.now,
                                leader_name: leader_name.clone(),
                                conn_url: conn_url.clone(),
                                lsn,
                            });
                        self.published[p_idx] = peer_snap;
                        let accepted = peer_fx.iter().any(|fx| {
                            matches!(fx, Effect::RespondFailsafeHeartbeat { accepted: true })
                        });
                        self.apply_effects(p_idx, peer_fx);
                        if !accepted {
                            all_ok = false;
                        }
                    }
                }

                let reply_at = (attempt_started_at + rtt).min(attempt_started_at + timeout);
                self.schedule(
                    reply_at,
                    SimAction::DeliverHaEvent {
                        node,
                        event: HaEvent::FailsafeQuorumResult {
                            rpc_epoch,
                            attempt_started_at,
                            now: reply_at,
                            all_peers_accepted: all_ok,
                        },
                    },
                );
            }

            SimAction::CompletePostgresDemotion { node, state_epoch } => {
                if let Some(until) = self.hw[node].frozen_until
                    && self.now < until
                {
                    // `SIGSTOP` pauses the demoting child process; if hardware watchdog does not reset
                    // the host before `until`, `SIGCONT` resumes demotion at `until`.
                    self.schedule(
                        until + Duration::from_millis(5),
                        SimAction::CompletePostgresDemotion { node, state_epoch },
                    );
                    return;
                }
                if !matches!(
                    self.nodes[node].state,
                    HaRoleState::Demoting { state_epoch: e, .. } if e == state_epoch
                ) {
                    return;
                }
                self.hw[node].pg_running = false;
                self.hw[node].in_recovery = true;
                let telemetry = self.build_pg_summary(node);
                self.deliver_event(
                    node,
                    HaEvent::PgObservation {
                        now: self.now,
                        telemetry,
                    },
                );
                self.deliver_event(
                    node,
                    HaEvent::PgDemotionCompleted {
                        state_epoch,
                        now: self.now,
                    },
                );
            }

            SimAction::CompletePostgresPromotion { node, state_epoch } => {
                if self.is_frozen(node) {
                    return;
                }
                if matches!(self.nodes[node].state, HaRoleState::Primary { .. })
                    && self.nodes[node].state_epoch == state_epoch
                {
                    self.cluster_highest_timeline += 1;
                    self.hw[node].in_recovery = false;
                    self.hw[node].pg_running = true;
                    self.hw[node].timeline = self.cluster_highest_timeline;
                    self.hw[node].diverged_from_cluster = false;
                    self.hw[node].following_conn = None;
                    self.hw[node].lsn = WalLsn(self.hw[node].lsn.0.max(1_024));
                    let telemetry = self.build_pg_summary(node);
                    self.deliver_event(
                        node,
                        HaEvent::PgObservation {
                            now: self.now,
                            telemetry,
                        },
                    );
                    self.deliver_event(
                        node,
                        HaEvent::PgPromotionCompleted {
                            state_epoch,
                            now: self.now,
                            success: true,
                        },
                    );
                }
            }

            SimAction::CompleteBackgroundJob {
                node,
                state_epoch,
                job,
                target_conn,
            } => {
                if self.hw[node].active_job_epoch != Some(state_epoch) {
                    return;
                }
                if let Some(until) = self.hw[node].frozen_until
                    && self.now < until
                {
                    self.schedule(
                        until + Duration::from_millis(5),
                        SimAction::CompleteBackgroundJob {
                            node,
                            state_epoch,
                            job,
                            target_conn,
                        },
                    );
                    return;
                }
                self.hw[node].active_job_epoch = None;
                let mut success = true;
                match job {
                    JobKind::InitDb | JobKind::CustomBootstrap => {
                        self.hw[node].has_valid_pgdata = true;
                        self.hw[node].pg_running = false;
                        self.hw[node].in_recovery = true;
                        self.hw[node].timeline = 1;
                        self.hw[node].lsn = WalLsn(1_024);
                        self.hw[node].diverged_from_cluster = false;
                    }
                    JobKind::BaseBackup | JobKind::PgRewind => {
                        if let Some(ref conn) = target_conn
                            && let Some(src) = self.node_index_by_conn(conn)
                            && self.lan_up[node][src]
                            && self.lan_up[src][node]
                            && self.hw[src].pg_running
                        {
                            self.hw[node].has_valid_pgdata = true;
                            self.hw[node].in_recovery = true;
                            self.hw[node].lsn = self.hw[src].lsn;
                            self.hw[node].timeline = self.hw[src].timeline.max(1);
                            self.hw[node].diverged_from_cluster = false;
                            self.hw[node].following_conn = Some(conn.clone());
                        } else {
                            success = false;
                        }
                    }
                }
                let telemetry = self.build_pg_summary(node);
                self.deliver_event(
                    node,
                    HaEvent::PgObservation {
                        now: self.now,
                        telemetry,
                    },
                );
                self.deliver_event(
                    node,
                    HaEvent::BackgroundJobFinished {
                        state_epoch,
                        now: self.now,
                        job,
                        success,
                    },
                );
            }

            SimAction::InjectFault(op) => match op {
                FaultOp::SetDcsLink { node, up } => {
                    if node < self.dcs_up.len() {
                        self.dcs_up[node] = up;
                    }
                }
                FaultOp::SetPeerLink { a, b, up } => {
                    if a < self.lan_up.len() && b < self.lan_up.len() && a != b {
                        self.lan_up[a][b] = up;
                    }
                }
                FaultOp::SetBaseRttMs { rtt_ms } => {
                    self.base_rtt_ms = rtt_ms.clamp(2, 1_200);
                }
                FaultOp::SetDemoteDelayMs { node, delay_ms } => {
                    if node < self.hw.len() {
                        self.hw[node].demote_delay_ms = delay_ms;
                    }
                }
                FaultOp::FreezeNode { node, duration_ms } => {
                    if node < self.hw.len() {
                        self.hw[node].frozen_until =
                            Some(self.now + Duration::from_millis(duration_ms));
                    }
                }
                FaultOp::TriggerSwitchover { leader, candidate } => {
                    let leader_name = leader
                        .filter(|&i| i < self.nodes.len())
                        .map(|i| self.nodes[i].self_name.clone());
                    let cand_name = candidate
                        .filter(|&i| i < self.nodes.len())
                        .map(|i| self.nodes[i].self_name.clone());
                    self.dcs.set_failover(FailoverIntent {
                        leader: leader_name,
                        candidate: cand_name,
                        scheduled_at: None,
                    });
                }
                FaultOp::HealAll => {
                    self.dcs_up.fill(true);
                    for row in &mut self.lan_up {
                        row.fill(true);
                    }
                    for h in &mut self.hw {
                        h.frozen_until = None;
                        h.demote_delay_ms = 100;
                    }
                    self.base_rtt_ms = 20;
                    self.dcs.failover = None;
                }
            },
        }
    }

    fn apply_effects(&mut self, node: usize, effects: negroni_ha::EffectBatch) {
        let half_rtt = Duration::from_millis((self.base_rtt_ms / 2).max(1));

        for effect in effects {
            match effect {
                Effect::ArmAndFeedWatchdog { state_epoch, proof } => {
                    self.hw[node].watchdog_armed = true;
                    self.hw[node].watchdog_epoch = proof.epoch;
                    self.hw[node].watchdog_valid_until = Some(proof.valid_until);
                    let armed_at = self.now + Duration::from_millis(1);
                    self.schedule(
                        armed_at,
                        SimAction::DeliverHaEvent {
                            node,
                            event: HaEvent::WatchdogArmed {
                                state_epoch,
                                now: armed_at,
                            },
                        },
                    );
                }

                Effect::FeedWatchdog(proof) => {
                    if self.hw[node].watchdog_armed && proof.epoch >= self.hw[node].watchdog_epoch {
                        self.hw[node].watchdog_epoch = proof.epoch;
                        self.hw[node].watchdog_valid_until = Some(proof.valid_until);
                    }
                }

                Effect::DisarmWatchdog => {
                    self.hw[node].watchdog_armed = false;
                    self.hw[node].watchdog_valid_until = None;
                }

                Effect::FetchDcs { rpc_epoch, timeout } => {
                    let started_at = self.now;
                    self.schedule(
                        self.now + half_rtt,
                        SimAction::ExecuteDcsFetch {
                            node,
                            rpc_epoch,
                            attempt_started_at: started_at,
                            timeout,
                        },
                    );
                }

                Effect::CasInitialize { rpc_epoch, sys_id } => {
                    let acquired = if self.dcs_up[node] {
                        self.dcs.cas_initialize(&sys_id)
                    } else {
                        false
                    };
                    let reply_at = self.now + half_rtt;
                    self.schedule(
                        reply_at,
                        SimAction::DeliverHaEvent {
                            node,
                            event: HaEvent::InitializeResult {
                                rpc_epoch,
                                now: reply_at,
                                acquired,
                            },
                        },
                    );
                }

                Effect::ProbeElectionPeers {
                    rpc_epoch,
                    peers,
                    timeout,
                } => {
                    let names = peers.into_iter().map(|(n, _)| n).collect();
                    self.schedule(
                        self.now + half_rtt,
                        SimAction::ExecuteElectionProbe {
                            node,
                            rpc_epoch,
                            peers: names,
                            timeout,
                        },
                    );
                }

                Effect::RenewOrAcquireLeader {
                    rpc_epoch,
                    lease,
                    expected,
                    ttl,
                } => {
                    let started_at = self.now;
                    self.schedule(
                        self.now + half_rtt,
                        SimAction::ExecuteLeaderLease {
                            node,
                            rpc_epoch,
                            attempt_started_at: started_at,
                            lease,
                            expected,
                            ttl,
                        },
                    );
                }

                Effect::ReleaseLeader { expected } => {
                    if self.dcs_up[node] {
                        let name = self.nodes[node].self_name.clone();
                        self.dcs.release_leader(self.now, &name, &expected);
                    }
                }

                Effect::ClearFailover { expected } => {
                    if self.dcs_up[node] {
                        self.dcs.clear_failover(&expected);
                    }
                }

                Effect::UpsertMember { data, lease } => {
                    if self.dcs_up[node] {
                        let name = self.nodes[node].self_name.clone();
                        let ttl = self.nodes[node].ttl;
                        self.dcs.upsert_member(self.now, &name, *data, lease, ttl);
                    }
                }

                Effect::PublishLeaderStatus {
                    status,
                    failsafe_roster,
                } => {
                    if self.dcs_up[node] {
                        let tok = self.dcs.alloc_token();
                        self.dcs.status = Some(Versioned {
                            value: status,
                            token: tok,
                        });
                        if let Some(fs) = failsafe_roster {
                            self.dcs.failsafe = Some(fs.clone());
                            let ack_at = self.now + half_rtt;
                            self.schedule(
                                ack_at,
                                SimAction::DeliverHaEvent {
                                    node,
                                    event: HaEvent::FailsafeRosterPublished {
                                        now: ack_at,
                                        roster: fs,
                                    },
                                },
                            );
                        }
                    }
                }

                Effect::AppendHistory { entry } => {
                    if self.dcs_up[node] {
                        self.dcs.history.push(entry);
                    }
                }

                Effect::UpdateSyncState { next, expected } => {
                    if self.dcs_up[node] {
                        self.dcs.update_sync(next, expected.as_ref());
                    }
                }

                Effect::ProbeFailsafePeers {
                    rpc_epoch,
                    peers,
                    timeout,
                } => {
                    let started_at = self.now;
                    let names = peers.into_iter().map(|(n, _)| n).collect();
                    self.schedule(
                        self.now + half_rtt,
                        SimAction::ExecuteFailsafeProbe {
                            node,
                            rpc_epoch,
                            attempt_started_at: started_at,
                            peers: names,
                            timeout,
                        },
                    );
                }

                Effect::StartPostgresAsReplica => {
                    self.hw[node].pg_running = true;
                    self.hw[node].in_recovery = true;
                }

                Effect::PromotePostgres { state_epoch } => {
                    self.schedule(
                        self.now + Duration::from_millis(20),
                        SimAction::CompletePostgresPromotion { node, state_epoch },
                    );
                }

                Effect::DemotePostgres {
                    state_epoch,
                    immediate_at,
                } => {
                    // Graceful `SIGINT` takes `demote_delay_ms`, capped by `SIGQUIT` at `immediate_at + 20ms`!
                    let graceful_done =
                        self.now + Duration::from_millis(self.hw[node].demote_delay_ms);
                    let sigquit_done = (immediate_at + Duration::from_millis(20)).max(self.now);
                    let finish_at = graceful_done.min(sigquit_done);
                    self.schedule(
                        finish_at,
                        SimAction::CompletePostgresDemotion { node, state_epoch },
                    );
                }

                Effect::ReconfigureStandby {
                    primary_conninfo, ..
                } => {
                    self.hw[node].following_conn = Some(primary_conninfo);
                }

                Effect::SpawnJob {
                    state_epoch,
                    job,
                    target_conn,
                } => {
                    self.hw[node].active_job_epoch = Some(state_epoch);
                    self.schedule(
                        self.now + Duration::from_millis(120),
                        SimAction::CompleteBackgroundJob {
                            node,
                            state_epoch,
                            job,
                            target_conn,
                        },
                    );
                }

                Effect::CancelActiveJobs => {
                    self.hw[node].active_job_epoch = None;
                }

                Effect::ReconfigureSynchronousStandbys { .. }
                | Effect::RespondFailsafeHeartbeat { .. }
                | Effect::SyncSlots { .. }
                | Effect::NotifySystemd(_) => {}
            }
        }
    }

    fn current_primary_index(&self) -> Option<usize> {
        self.published.iter().position(|p| p.is_read_write_valid)
    }

    fn count_healthy_replicas(&self) -> usize {
        self.published
            .iter()
            .filter(|p| p.role == "replica" && p.state == "running")
            .count()
    }
}

// ============================================================================
// 1. Deterministic Targeted Failure Scenarios
// ============================================================================

#[test]
fn dst_3node_bootstrap_watchdog_freeze_failover_and_pg_rewind_rejoin() {
    let mut sim = SimCluster::new(3, true, SyncMode::Off, 1);

    // Boot cluster and converge to 1 Primary + 2 streaming Replicas.
    sim.step_until(Duration::from_secs(25));
    let initial_primary = sim
        .current_primary_index()
        .expect("cluster must elect primary");
    assert_eq!(sim.count_healthy_replicas(), 2);

    // Freeze the Primary for 40s (`SIGSTOP` / hung kernel I/O).
    // Hardware `/dev/watchdog` must hard-reset the frozen Primary BEFORE its 30s DCS lease expires,
    // a surviving Replica must win election cleanly, and the rebooted former Primary must run `pg_rewind`!
    sim.schedule(
        sim.now,
        SimAction::InjectFault(FaultOp::FreezeNode {
            node: initial_primary,
            duration_ms: 40_000,
        }),
    );

    sim.step_until(Duration::from_secs(95));

    assert!(
        sim.hw[initial_primary].watchdog_resets >= 1,
        "frozen primary must be fenced by hardware watchdog before TTL expiry"
    );
    let new_primary = sim
        .current_primary_index()
        .expect("surviving partition must elect new primary");
    assert_ne!(new_primary, initial_primary);
    assert_eq!(sim.count_healthy_replicas(), 2);
    assert!(
        !sim.hw[initial_primary].diverged_from_cluster,
        "former primary must clear divergence via PgRewind and rejoin as streaming replica"
    );
}

#[test]
fn dst_3node_asymmetric_dcs_outage_failsafe_quorum_prevents_split_brain_and_recovers() {
    let mut sim = SimCluster::new(3, true, SyncMode::Off, 1);
    sim.step_until(Duration::from_secs(35));
    let p = sim.current_primary_index().unwrap();
    assert_eq!(sim.count_healthy_replicas(), 2);

    // Sever ONLY `Primary <-> DCS` for 45 seconds (exceeding the 30s DCS TTL!), while
    // `Primary <-> Replicas` LAN and `Replicas <-> DCS` remain 100% healthy!
    sim.schedule(
        sim.now,
        SimAction::InjectFault(FaultOp::SetDcsLink { node: p, up: false }),
    );
    sim.step_until(Duration::from_secs(80));

    // Primary must stay writable in `FailsafePrimary` across multiple ticks, and Replicas MUST suppress election!
    assert_eq!(sim.current_primary_index(), Some(p));
    assert!(matches!(
        sim.nodes[p].state,
        HaRoleState::FailsafePrimary { .. }
    ));

    // Heal `Primary <-> DCS`: Primary must seamlessly reclaim `/leader` and return to `HaRoleState::Primary`!
    sim.schedule(
        sim.now,
        SimAction::InjectFault(FaultOp::SetDcsLink { node: p, up: true }),
    );
    sim.step_until(Duration::from_secs(105));
    assert_eq!(sim.current_primary_index(), Some(p));
    assert!(matches!(sim.nodes[p].state, HaRoleState::Primary { .. }));
    assert_eq!(sim.count_healthy_replicas(), 2);
}

#[test]
fn dst_5node_partial_failsafe_rejection_forces_demotion_and_clean_takeover() {
    let mut sim = SimCluster::new(5, true, SyncMode::Off, 1);
    sim.step_until(Duration::from_secs(30));
    let p = sim.current_primary_index().unwrap();
    assert_eq!(sim.count_healthy_replicas(), 4);

    // Cut `Primary <-> DCS` AND cut LAN to ONE replica -> failsafe unanimous quorum MUST fail!
    let cut_peer = (p + 1) % 5;
    sim.schedule(
        sim.now,
        SimAction::InjectFault(FaultOp::SetDcsLink { node: p, up: false }),
    );
    sim.schedule(
        sim.now,
        SimAction::InjectFault(FaultOp::SetPeerLink {
            a: p,
            b: cut_peer,
            up: false,
        }),
    );

    sim.step_until(Duration::from_secs(85));

    // Old primary must have demoted cleanly, and one of the 4 DCS-connected replicas must now be Primary!
    let new_p = sim
        .current_primary_index()
        .expect("4-node majority must elect new primary");
    assert_ne!(new_p, p);

    // Heal network and verify all 5 nodes converge (1 Primary + 4 Replicas).
    sim.schedule(sim.now, SimAction::InjectFault(FaultOp::HealAll));
    sim.step_until(Duration::from_secs(120));
    assert!(sim.current_primary_index().is_some());
    assert_eq!(sim.count_healthy_replicas(), 4);
}

#[test]
fn dst_3node_planned_switchover_with_slow_demotion_holds_lock_until_pg_stops() {
    let mut sim = SimCluster::new(3, true, SyncMode::Off, 1);
    sim.step_until(Duration::from_secs(25));
    let p = sim.current_primary_index().unwrap();
    let target = (p + 2) % 3;

    // Inject a slow 2,500ms PostgreSQL shutdown on outgoing Primary and trigger targeted switchover!
    sim.schedule(
        sim.now,
        SimAction::InjectFault(FaultOp::SetDemoteDelayMs {
            node: p,
            delay_ms: 2_500,
        }),
    );
    sim.schedule(
        sim.now,
        SimAction::InjectFault(FaultOp::TriggerSwitchover {
            leader: Some(p),
            candidate: Some(target),
        }),
    );

    sim.step_until(Duration::from_secs(60));

    // Target candidate must win with zero overlapping RW windows, and `/failover` must be cleared!
    assert_eq!(sim.current_primary_index(), Some(target));
    assert_eq!(sim.count_healthy_replicas(), 2);
    assert!(sim.dcs.failover.is_none());
}

#[test]
fn dst_5node_quorum_sync_election_preserves_committed_wal() {
    let mut sim = SimCluster::new(5, true, SyncMode::Quorum, 2);
    sim.step_until(Duration::from_secs(35));
    let p = sim.current_primary_index().unwrap();
    assert!(sim.committed_sync_lsn.0 > 0);

    // Isolate the Primary completely (`DCS` + all peer links) while commits were replicated to quorum!
    sim.schedule(
        sim.now,
        SimAction::InjectFault(FaultOp::SetDcsLink { node: p, up: false }),
    );
    for peer in 0..5 {
        if peer != p {
            sim.schedule(
                sim.now,
                SimAction::InjectFault(FaultOp::SetPeerLink {
                    a: p,
                    b: peer,
                    up: false,
                }),
            );
            sim.schedule(
                sim.now,
                SimAction::InjectFault(FaultOp::SetPeerLink {
                    a: peer,
                    b: p,
                    up: false,
                }),
            );
        }
    }

    let committed_before_crash = sim.committed_sync_lsn;
    sim.step_until(Duration::from_secs(90));

    let new_p = sim
        .current_primary_index()
        .expect("surviving 4-node quorum must elect new primary");
    assert_ne!(new_p, p);
    assert!(
        sim.hw[new_p].lsn >= committed_before_crash,
        "elected quorum primary LSN ({}) must never be behind committed sync LSN ({})",
        sim.hw[new_p].lsn,
        committed_before_crash
    );
}

#[test]
fn dst_3node_late_joining_node_outside_failsafe_roster_cannot_split_brain() {
    // Reproduces shrunk proptest edge case: `pg-2` is frozen when `pg-0` publishes `/failsafe = {pg-0, pg-1}`,
    // then `pg-0` loses DCS and stays `FailsafePrimary` via `pg-1` while `/failover` targets `candidate: pg-2`.
    // `pg-2` MUST probe `{pg-0, pg-1}`, detect the active failsafe primary, and refuse promotion until healed!
    run_generated_schedule(
        3,
        true,
        SyncMode::Off,
        vec![
            (
                15_000,
                FaultOp::TriggerSwitchover {
                    leader: None,
                    candidate: Some(2),
                },
            ),
            (
                15_000,
                FaultOp::FreezeNode {
                    node: 2,
                    duration_ms: 5_071,
                },
            ),
            (30_031, FaultOp::SetDcsLink { node: 0, up: false }),
        ],
    );
}

// ============================================================================
// 2. Property-Based Fault Injection (`proptest`: 10,000+ Generated Schedules)
// ============================================================================

fn arb_fault_op(node_count: usize) -> impl Strategy<Value = (u64, FaultOp)> {
    let time_offset_ms = 15_000u64..85_000u64;
    let op =
        prop_oneof![
            (0..node_count, any::<bool>()).prop_map(|(node, up)| FaultOp::SetDcsLink { node, up }),
            (0..node_count, 0..node_count, any::<bool>())
                .prop_map(|(a, b, up)| FaultOp::SetPeerLink { a, b, up }),
            (5u64..900u64).prop_map(|rtt_ms| FaultOp::SetBaseRttMs { rtt_ms }),
            (0..node_count, 20u64..9_500u64)
                .prop_map(|(node, delay_ms)| FaultOp::SetDemoteDelayMs { node, delay_ms }),
            (0..node_count, 1_000u64..38_000u64)
                .prop_map(|(node, duration_ms)| FaultOp::FreezeNode { node, duration_ms }),
            (
                prop::option::of(0..node_count),
                prop::option::of(0..node_count)
            )
                .prop_map(|(leader, candidate)| FaultOp::TriggerSwitchover { leader, candidate }),
        ];
    (time_offset_ms, op)
}

fn run_generated_schedule(
    node_count: usize,
    failsafe_mode: bool,
    sync_mode: SyncMode,
    faults: Vec<(u64, FaultOp)>,
) {
    let mut sim = SimCluster::new(node_count, failsafe_mode, sync_mode, 1);

    for (offset_ms, op) in faults {
        sim.schedule(
            sim.t0 + Duration::from_millis(offset_ms),
            SimAction::InjectFault(op),
        );
    }

    // Heal all network partitions and node freezes at `t = 90s` and verify liveness convergence by `t = 150s`!
    sim.schedule(
        sim.t0 + Duration::from_secs(90),
        SimAction::InjectFault(FaultOp::HealAll),
    );
    sim.step_until(Duration::from_secs(150));

    assert!(
        sim.current_primary_index().is_some(),
        "{node_count}-node cluster failed to converge to an active writable Primary after HealAll; states: {:?}",
        sim.nodes
            .iter()
            .map(|n| (&n.self_name, &n.state))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        sim.count_healthy_replicas(),
        node_count - 1,
        "{node_count}-node cluster failed to converge all replicas to running state after HealAll"
    );
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 5_000,
        max_shrink_iters: 256,
        .. ProptestConfig::default()
    })]

    #[test]
    fn prop_dst_3node_zero_split_brain_and_post_heal_liveness(
        failsafe in any::<bool>(),
        sync_idx in 0u8..3u8,
        faults in prop::collection::vec(arb_fault_op(3), 1..12),
    ) {
        let sync_mode = match sync_idx {
            0 => SyncMode::Off,
            1 => SyncMode::Priority,
            _ => SyncMode::Quorum,
        };
        run_generated_schedule(3, failsafe, sync_mode, faults);
    }

    #[test]
    fn prop_dst_5node_zero_split_brain_and_post_heal_liveness(
        failsafe in any::<bool>(),
        sync_idx in 0u8..3u8,
        faults in prop::collection::vec(arb_fault_op(5), 1..16),
    ) {
        let sync_mode = match sync_idx {
            0 => SyncMode::Off,
            1 => SyncMode::Priority,
            _ => SyncMode::Quorum,
        };
        run_generated_schedule(5, failsafe, sync_mode, faults);
    }
}
