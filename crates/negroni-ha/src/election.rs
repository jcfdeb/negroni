use std::collections::BTreeMap;

use compact_str::CompactString;
use negroni_config::SyncMode;
use negroni_types::{ClusterSnapshot, SyncState, WalLsn};

/// Observed WAL position and failover priority returned by a peer during an election probe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerElectionSample {
    pub member_name: CompactString,
    pub lsn: WalLsn,
    pub failover_priority: i64,
    pub nofailover: bool,
    /// `true` if the peer's PostgreSQL is running cleanly in recovery and able to campaign itself.
    pub can_promote: bool,
    /// `true` if the peer itself is an active primary or holds a non-expired failsafe leader lease.
    pub has_active_leader: bool,
}

fn is_in_sync_set(name: &CompactString, sync: &SyncState) -> bool {
    sync.contains_standby(name) || sync.leader.as_ref() == Some(name)
}

/// Inputs describing the local node's eligibility for leader election.
#[derive(Debug, Clone)]
pub struct LocalCandidateProfile<'a> {
    pub self_name: &'a CompactString,
    pub has_valid_pgdata: bool,
    pub pg_running: bool,
    pub local_lsn: WalLsn,
    pub local_lag_bytes: u64,
    pub max_lag_on_failover: u64,
    pub nofailover: bool,
    pub failover_priority: i64,
    pub sync_mode: SyncMode,
    pub sync_mode_strict: bool,
}

/// Decision context for `should_acquire_after_peer_probe`.
#[derive(Debug, Clone)]
pub struct PeerProbeContext<'a> {
    pub self_name: &'a CompactString,
    pub self_lsn: WalLsn,
    pub self_failover_priority: i64,
    pub targeted_failover: bool,
    pub failsafe_enabled: bool,
    pub dcs_failsafe: Option<&'a BTreeMap<CompactString, String>>,
    pub sync_mode: SyncMode,
    pub dcs_sync: Option<&'a SyncState>,
}

/// Evaluates whether the local replica is eligible to campaign when `/leader` is vacant.
pub fn is_eligible_for_election(
    profile: &LocalCandidateProfile<'_>,
    snap: &ClusterSnapshot,
) -> bool {
    if !profile.has_valid_pgdata || !profile.pg_running {
        return false;
    }

    // 1. Explicit `/failover` intent overrides automatic rules:
    //    - If `candidate` is specified, ONLY that candidate may acquire `/leader`.
    //    - Manual failover without a specific candidate still honors `nofailover` & sync safety.
    if let Some(ref fo) = snap.failover
        && !fo.value.is_empty()
        && let Some(ref target_candidate) = fo.value.candidate
        && !target_candidate.is_empty()
    {
        return target_candidate == profile.self_name;
    }

    let is_manual_failover = snap
        .failover
        .as_ref()
        .is_some_and(|fo| !fo.value.is_empty());

    if profile.nofailover || profile.failover_priority <= 0 {
        return false;
    }

    // 2. Maximum lag gate (skipped only during an explicit operator-triggered `/failover`).
    if !is_manual_failover {
        if profile.local_lag_bytes > profile.max_lag_on_failover {
            return false;
        }
        if let Some(ref status) = snap.status {
            let dcs_lag = profile.local_lsn.saturating_lag_from(status.value.optime);
            if dcs_lag > profile.max_lag_on_failover {
                return false;
            }
        }
    }

    // 3. Synchronous replication gate (`/sync`).
    if profile.sync_mode.is_enabled() && !is_manual_failover {
        match &snap.sync {
            Some(sync) => {
                let has_sync_members = sync
                    .value
                    .sync_standby
                    .as_deref()
                    .is_some_and(|s| !s.trim().is_empty());
                if has_sync_members {
                    if !is_in_sync_set(profile.self_name, &sync.value) {
                        return false;
                    }
                } else if profile.sync_mode_strict {
                    // Strict synchronous mode with zero registered sync standbys blocks auto-promotion.
                    return false;
                }
            }
            None if profile.sync_mode_strict => return false,
            None => {}
        }
    }

    true
}

/// Returns `(member_name, api_url)` for all peers other than `self_name` currently registered in
/// `/members` (plus `/failsafe` when failsafe mode is active, since an isolated primary's ephemeral
/// `/members/<primary>` key expires alongside `/leader`).
pub fn surviving_election_peers(
    self_name: &CompactString,
    snap: &ClusterSnapshot,
    include_failsafe: bool,
) -> Vec<(CompactString, String)> {
    let mut merged = BTreeMap::new();
    if include_failsafe && let Some(ref fs) = snap.failsafe {
        for (name, url) in fs {
            if name != self_name {
                merged.insert(name.clone(), url.clone());
            }
        }
    }
    for (name, m) in &snap.members {
        if name != self_name {
            merged.insert(name.clone(), m.value.api_url.clone());
        }
    }
    merged.into_iter().collect()
}

/// Evaluates whether the local candidate won the pre-CAS peer probe (`is_healthiest_node`):
/// - Unconditionally aborts if any responding peer reports `has_active_leader` (fencing split-brain).
/// - When `failsafe_enabled` and `/failsafe` has `>= 2` members and does not include `self_name`,
///   requires at least one member of `/failsafe` to respond so an isolated failsafe quorum cannot
///   overlap with a newly joined node across a LAN partition.
/// - In `SyncMode::Quorum`, verifies at least `sync.quorum` other voters responded.
/// - Yields if any promotable, `/sync`-eligible peer has a strictly higher WAL `lsn`, or equal `lsn`
///   with a strictly higher `failover_priority`.
pub fn should_acquire_after_peer_probe(
    ctx: &PeerProbeContext<'_>,
    samples: &[PeerElectionSample],
) -> bool {
    if samples.iter().any(|peer| peer.has_active_leader) {
        return false;
    }

    if ctx.failsafe_enabled
        && let Some(fs) = ctx.dcs_failsafe
        && fs.len() >= 2
        && !fs.contains_key(ctx.self_name)
        && !samples
            .iter()
            .any(|peer| fs.contains_key(&peer.member_name))
    {
        return false;
    }

    if ctx.targeted_failover {
        return true;
    }

    if ctx.sync_mode == SyncMode::Quorum
        && let Some(sync) = ctx.dcs_sync
        && sync.quorum > 0
    {
        let observed_other_voters = samples
            .iter()
            .filter(|sample| {
                sample.member_name != *ctx.self_name && is_in_sync_set(&sample.member_name, sync)
            })
            .count();
        if observed_other_voters < sync.quorum as usize {
            return false;
        }
    }

    let enforce_sync_filter = ctx.sync_mode.is_enabled()
        && ctx.dcs_sync.is_some_and(|s| {
            s.sync_standby
                .as_deref()
                .is_some_and(|v| !v.trim().is_empty())
        });

    for peer in samples {
        if peer.member_name == *ctx.self_name
            || !peer.can_promote
            || peer.nofailover
            || peer.failover_priority <= 0
        {
            continue;
        }
        if enforce_sync_filter
            && let Some(sync) = ctx.dcs_sync
            && !is_in_sync_set(&peer.member_name, sync)
        {
            continue;
        }
        if peer.lsn > ctx.self_lsn {
            return false;
        }
        if peer.lsn == ctx.self_lsn && peer.failover_priority > ctx.self_failover_priority {
            return false;
        }
    }

    true
}

/// Resolves the `conn_url` of the active leader in `snap`, if present in `/members`.
pub fn leader_conn_url(snap: &ClusterSnapshot) -> Option<String> {
    let leader_name = &snap.leader.as_ref()?.value;
    Some(snap.members.get(leader_name)?.value.conn_url.clone())
}

/// Selects the upstream `(member_name, conn_url)` a replica should stream WAL from,
/// honoring `tags.replicatefrom` cascading replication when the target peer is running.
pub fn select_upstream_source(
    self_name: &CompactString,
    replicatefrom: Option<&CompactString>,
    snap: &ClusterSnapshot,
) -> Option<(CompactString, String)> {
    if let Some(cascade_peer) = replicatefrom
        && cascade_peer != self_name
        && let Some(peer_member) = snap.members.get(cascade_peer)
        && matches!(peer_member.value.state.as_str(), "running" | "streaming")
    {
        return Some((cascade_peer.clone(), peer_member.value.conn_url.clone()));
    }

    let leader_name = snap.leader.as_ref()?.value.clone();
    if leader_name == *self_name {
        return None;
    }
    let conn = snap.members.get(&leader_name)?.value.conn_url.clone();
    Some((leader_name, conn))
}

/// Selects the initial `pg_basebackup` / replica clone source:
/// 1. `tags.replicatefrom` peer if running,
/// 2. Any running `tags.clonefrom: true` member,
/// 3. Active `/leader`.
pub fn select_clone_source(
    self_name: &CompactString,
    replicatefrom: Option<&CompactString>,
    snap: &ClusterSnapshot,
) -> Option<String> {
    if let Some(cascade_peer) = replicatefrom
        && cascade_peer != self_name
        && let Some(peer_member) = snap.members.get(cascade_peer)
        && matches!(peer_member.value.state.as_str(), "running" | "streaming")
    {
        return Some(peer_member.value.conn_url.clone());
    }

    for (name, member) in &snap.members {
        if name != self_name
            && member.value.clonefrom()
            && matches!(member.value.state.as_str(), "running" | "streaming")
        {
            return Some(member.value.conn_url.clone());
        }
    }

    leader_conn_url(snap)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::collections::BTreeMap;

    use negroni_types::{CasToken, ClusterStatus, FailoverIntent, MemberData, Versioned};

    use super::*;

    fn base_profile(name: &CompactString) -> LocalCandidateProfile<'_> {
        LocalCandidateProfile {
            self_name: name,
            has_valid_pgdata: true,
            pg_running: true,
            local_lsn: WalLsn(10_000),
            local_lag_bytes: 0,
            max_lag_on_failover: 1_048_576,
            nofailover: false,
            failover_priority: 1,
            sync_mode: SyncMode::Off,
            sync_mode_strict: false,
        }
    }

    #[test]
    fn targeted_failover_allows_only_designated_candidate() {
        let n1 = CompactString::const_new("pg-1");
        let n2 = CompactString::const_new("pg-2");
        let snap = ClusterSnapshot {
            failover: Some(Versioned {
                value: FailoverIntent {
                    leader: Some(CompactString::const_new("pg-0")),
                    candidate: Some(n2.clone()),
                    scheduled_at: None,
                },
                token: CasToken::Revision(4),
            }),
            ..Default::default()
        };

        assert!(!is_eligible_for_election(&base_profile(&n1), &snap));

        // Even if n2 had `nofailover: true`, explicit operator candidate targeting allows n2.
        let mut n2_prof = base_profile(&n2);
        n2_prof.nofailover = true;
        assert!(is_eligible_for_election(&n2_prof, &snap));
    }

    #[test]
    fn synchronous_mode_blocks_out_of_sync_standby_from_auto_election() {
        let n1 = CompactString::const_new("pg-1");
        let n2 = CompactString::const_new("pg-2");
        let snap = ClusterSnapshot {
            status: Some(Versioned {
                value: ClusterStatus {
                    optime: WalLsn(10_000),
                    slots: BTreeMap::new(),
                    retain_slots: Vec::new(),
                },
                token: CasToken::Revision(2),
            }),
            sync: Some(Versioned {
                value: SyncState {
                    leader: Some(CompactString::const_new("pg-0")),
                    sync_standby: Some("pg-1".to_owned()),
                    quorum: 0,
                },
                token: CasToken::Revision(3),
            }),
            ..Default::default()
        };

        let mut p1 = base_profile(&n1);
        p1.sync_mode = SyncMode::Priority;
        assert!(is_eligible_for_election(&p1, &snap));

        let mut p2 = base_profile(&n2);
        p2.sync_mode = SyncMode::Priority;
        assert!(!is_eligible_for_election(&p2, &snap));
    }

    #[test]
    fn quorum_peer_probe_requires_quorum_voters_and_highest_lsn() {
        let self_name = CompactString::const_new("node1");
        let sync = SyncState {
            leader: Some(CompactString::const_new("node0")),
            sync_standby: Some("node1,node2,node3".to_owned()),
            quorum: 1,
        };
        let probe_ctx = PeerProbeContext {
            self_name: &self_name,
            self_lsn: WalLsn(5_000),
            self_failover_priority: 1,
            targeted_failover: false,
            failsafe_enabled: false,
            dcs_failsafe: None,
            sync_mode: SyncMode::Quorum,
            dcs_sync: Some(&sync),
        };

        // Zero other voters responded -> must refuse to acquire `/leader`!
        assert!(!should_acquire_after_peer_probe(&probe_ctx, &[]));

        // Voter `node2` responded with higher LSN -> `node1` yields!
        let higher_peer = vec![PeerElectionSample {
            member_name: CompactString::const_new("node2"),
            lsn: WalLsn(5_100),
            failover_priority: 1,
            nofailover: false,
            can_promote: true,
            has_active_leader: false,
        }];
        assert!(!should_acquire_after_peer_probe(&probe_ctx, &higher_peer));

        // Voter `node2` responded with lower LSN, plus async `node4` (outside `/sync`) with higher LSN -> `node1` MUST NOT livelock!
        let lower_voter_and_higher_async = vec![
            PeerElectionSample {
                member_name: CompactString::const_new("node2"),
                lsn: WalLsn(4_900),
                failover_priority: 10,
                nofailover: false,
                can_promote: true,
                has_active_leader: false,
            },
            PeerElectionSample {
                member_name: CompactString::const_new("node4"),
                lsn: WalLsn(5_500),
                failover_priority: 10,
                nofailover: false,
                can_promote: true,
                has_active_leader: false,
            },
        ];
        assert!(should_acquire_after_peer_probe(
            &probe_ctx,
            &lower_voter_and_higher_async
        ));

        // If `node2` reports an active failsafe leader, `node1` must abort election even with higher LSN!
        let failsafe_veto_peer = vec![PeerElectionSample {
            member_name: CompactString::const_new("node2"),
            lsn: WalLsn(4_000),
            failover_priority: 1,
            nofailover: false,
            can_promote: true,
            has_active_leader: true,
        }];
        assert!(!should_acquire_after_peer_probe(
            &probe_ctx,
            &failsafe_veto_peer
        ));

        // Non-roster candidate (`node5` outside `/failsafe = {node0, node1}`) cannot promote if partitioned from `{node0, node1}`!
        let non_roster_self = CompactString::const_new("node5");
        let fs_map = BTreeMap::from([
            (
                CompactString::const_new("node0"),
                "http://n0:8008/patroni".to_owned(),
            ),
            (
                CompactString::const_new("node1"),
                "http://n1:8008/patroni".to_owned(),
            ),
        ]);
        let non_roster_ctx = PeerProbeContext {
            self_name: &non_roster_self,
            self_lsn: WalLsn(6_000),
            self_failover_priority: 1,
            targeted_failover: true,
            failsafe_enabled: true,
            dcs_failsafe: Some(&fs_map),
            sync_mode: SyncMode::Off,
            dcs_sync: None,
        };
        assert!(!should_acquire_after_peer_probe(&non_roster_ctx, &[]));
        let fs_member_replied = vec![PeerElectionSample {
            member_name: CompactString::const_new("node1"),
            lsn: WalLsn(6_500),
            failover_priority: 10,
            nofailover: false,
            can_promote: true,
            has_active_leader: false,
        }];
        assert!(should_acquire_after_peer_probe(
            &non_roster_ctx,
            &fs_member_replied
        ));
    }

    #[test]
    fn clone_source_prefers_replicatefrom_then_clonefrom_then_leader() {
        let self_name = CompactString::const_new("pg-3");
        let mut snap = ClusterSnapshot {
            leader: Some(Versioned {
                value: CompactString::const_new("pg-0"),
                token: CasToken::Revision(1),
            }),
            ..Default::default()
        };

        let mk = |url: &str, clonefrom: bool| Versioned {
            value: MemberData {
                conn_url: url.to_owned(),
                api_url: "http://127.0.0.1:8008/patroni".to_owned(),
                state: CompactString::const_new("running"),
                role: CompactString::const_new("replica"),
                version: CompactString::const_new("4.1.0"),
                xlog_location: Some(WalLsn(100)),
                timeline: Some(1),
                replication_state: None,
                pending_restart: false,
                pending_restart_reason: BTreeMap::new(),
                pause: None,
                watchdog_failed: None,
                tags: if clonefrom {
                    BTreeMap::from([(
                        CompactString::const_new("clonefrom"),
                        serde_json::Value::Bool(true),
                    )])
                } else {
                    BTreeMap::new()
                },
                extra: BTreeMap::new(),
            },
            token: CasToken::Revision(1),
        };

        snap.members.insert(
            CompactString::const_new("pg-0"),
            mk("postgres://pg0:5432/postgres", false),
        );
        snap.members.insert(
            CompactString::const_new("pg-1"),
            mk("postgres://pg1:5432/postgres", true),
        );
        snap.members.insert(
            CompactString::const_new("pg-2"),
            mk("postgres://pg2:5432/postgres", false),
        );

        let cascade = CompactString::const_new("pg-2");
        assert_eq!(
            select_clone_source(&self_name, Some(&cascade), &snap).as_deref(),
            Some("postgres://pg2:5432/postgres")
        );
        assert_eq!(
            select_clone_source(&self_name, None, &snap).as_deref(),
            Some("postgres://pg1:5432/postgres")
        );
    }
}
