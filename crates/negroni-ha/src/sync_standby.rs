use std::collections::BTreeMap;

use compact_str::CompactString;
use negroni_config::SyncMode;
use negroni_types::{MemberData, SyncState, Versioned, WalLsn};

/// Computed target for PostgreSQL `synchronous_standby_names` and DCS `/sync`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncStandbyPlan {
    pub guc_synchronous_standby_names: String,
    pub dcs_sync_state: SyncState,
    pub selected_members: Vec<CompactString>,
}

/// Quotes a standby `application_name` for PostgreSQL's `synchronous_standby_names` GUC
/// (`patroni.postgresql.sync.quote_standby_name`).
///
/// Unquoted names containing hyphens (e.g. Kubernetes StatefulSet pods `negroni-0`) or
/// uppercase letters cause PostgreSQL to reject or misparse `synchronous_standby_names`.
///
/// # Examples
///
/// ```
/// use negroni_ha::quote_standby_name;
///
/// assert_eq!(quote_standby_name("pg_node1"), "pg_node1");
/// assert_eq!(quote_standby_name("negroni-0"), "\"negroni-0\"");
/// assert_eq!(quote_standby_name("first"), "\"first\"");
/// ```
pub fn quote_standby_name(name: &str) -> String {
    if name == "*" {
        return "*".to_owned();
    }
    let mut bytes = name.bytes();
    let is_simple_ident = match bytes.next() {
        Some(first) if first.is_ascii_lowercase() || first == b'_' => {
            bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'$')
        }
        _ => false,
    };
    let is_keyword = matches!(name, "any" | "first");
    if is_simple_ident && !is_keyword {
        name.to_owned()
    } else {
        let escaped = name.replace('"', "\"\"");
        format!("\"{escaped}\"")
    }
}

#[derive(Debug, Clone)]
struct EligibleStandby<'a> {
    name: &'a CompactString,
    priority: i64,
    was_in_previous_sync: bool,
    lsn: WalLsn,
}

/// Pure, deterministic computation of PostgreSQL `synchronous_standby_names` (`FIRST N` / `ANY N`)
/// and DCS `/sync` (`SyncState`), matching Patroni 4.1+ (`patroni/postgresql/sync.py` & `patroni/quorum.py`).
#[allow(clippy::too_many_arguments)]
pub fn compute_sync_standby_plan(
    leader_name: &CompactString,
    leader_lsn: WalLsn,
    mode: SyncMode,
    strict: bool,
    requested_count: usize,
    max_lag_syncnode: i64,
    members: &BTreeMap<CompactString, Versioned<MemberData>>,
    previous_sync: Option<&SyncState>,
) -> SyncStandbyPlan {
    if mode == SyncMode::Off {
        return SyncStandbyPlan {
            guc_synchronous_standby_names: String::new(),
            dcs_sync_state: SyncState {
                leader: None,
                sync_standby: None,
                quorum: 0,
            },
            selected_members: Vec::new(),
        };
    }

    let max_lag_limit: Option<u64> = u64::try_from(max_lag_syncnode).ok();

    let mut eligible: Vec<EligibleStandby<'_>> = members
        .iter()
        .filter_map(|(name, versioned)| {
            if name == leader_name {
                return None;
            }
            let m = &versioned.value;
            if m.is_primary_role() {
                return None;
            }
            if !matches!(m.state.as_str(), "running" | "streaming") {
                return None;
            }
            if m.nosync() || m.sync_priority() <= 0 {
                return None;
            }
            let lsn = m.xlog_location.unwrap_or_default();
            if let Some(limit) = max_lag_limit
                && lsn.saturating_lag_from(leader_lsn) > limit
            {
                return None;
            }
            let was_in_previous_sync = previous_sync.is_some_and(|s| s.contains_standby(name));
            Some(EligibleStandby {
                name,
                priority: m.sync_priority(),
                was_in_previous_sync,
                lsn,
            })
        })
        .collect();

    if eligible.is_empty() {
        return SyncStandbyPlan {
            guc_synchronous_standby_names: if strict {
                "*".to_owned()
            } else {
                String::new()
            },
            dcs_sync_state: SyncState {
                leader: Some(leader_name.clone()),
                sync_standby: None,
                quorum: 0,
            },
            selected_members: Vec::new(),
        };
    }

    let sync_wanted = requested_count.max(1);

    match mode {
        SyncMode::Off => unreachable!("handled above"),
        SyncMode::Priority => {
            // Higher `sync_priority` wins, then existing sync membership (anti-flap), then higher WAL LSN, then lexical name.
            eligible.sort_by(|a, b| {
                b.priority
                    .cmp(&a.priority)
                    .then_with(|| b.was_in_previous_sync.cmp(&a.was_in_previous_sync))
                    .then_with(|| b.lsn.cmp(&a.lsn))
                    .then_with(|| a.name.cmp(b.name))
            });

            let num = sync_wanted.min(eligible.len());
            let selected: Vec<CompactString> = eligible
                .into_iter()
                .take(num)
                .map(|c| c.name.clone())
                .collect();
            let quoted = selected
                .iter()
                .map(|s| quote_standby_name(s))
                .collect::<Vec<_>>()
                .join(",");
            let csv = selected
                .iter()
                .map(CompactString::as_str)
                .collect::<Vec<_>>()
                .join(",");

            SyncStandbyPlan {
                guc_synchronous_standby_names: format!("FIRST {num} ({quoted})"),
                dcs_sync_state: SyncState {
                    leader: Some(leader_name.clone()),
                    sync_standby: Some(csv),
                    quorum: 0,
                },
                selected_members: selected,
            }
        }
        SyncMode::Quorum => {
            // Quorum mode includes all eligible caught-up voters, with `quorum = voters.len() - numsync`.
            eligible.sort_by(|a, b| a.name.cmp(b.name));
            let numsync = sync_wanted.min(eligible.len());
            let quorum = u32::try_from(eligible.len().saturating_sub(numsync)).unwrap_or(u32::MAX);
            let selected: Vec<CompactString> =
                eligible.into_iter().map(|c| c.name.clone()).collect();
            let quoted = selected
                .iter()
                .map(|s| quote_standby_name(s))
                .collect::<Vec<_>>()
                .join(",");
            let csv = selected
                .iter()
                .map(CompactString::as_str)
                .collect::<Vec<_>>()
                .join(",");

            SyncStandbyPlan {
                guc_synchronous_standby_names: format!("ANY {numsync} ({quoted})"),
                dcs_sync_state: SyncState {
                    leader: Some(leader_name.clone()),
                    sync_standby: Some(csv),
                    quorum,
                },
                selected_members: selected,
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use negroni_types::CasToken;
    use serde_json::json;

    use super::*;

    fn mk_member(state: &str, lsn: u64, tags: serde_json::Value) -> Versioned<MemberData> {
        let tag_map: BTreeMap<CompactString, serde_json::Value> =
            serde_json::from_value(tags).unwrap();
        Versioned {
            value: MemberData {
                conn_url: "postgres://127.0.0.1:5432/postgres".to_owned(),
                api_url: "http://127.0.0.1:8008/patroni".to_owned(),
                state: CompactString::from(state),
                role: CompactString::const_new("replica"),
                version: CompactString::const_new("4.1.0"),
                xlog_location: Some(WalLsn(lsn)),
                timeline: Some(1),
                replication_state: Some(CompactString::const_new("streaming")),
                pending_restart: false,
                pending_restart_reason: BTreeMap::new(),
                pause: None,
                watchdog_failed: None,
                tags: tag_map,
                extra: BTreeMap::new(),
            },
            token: CasToken::Revision(1),
        }
    }

    #[test]
    fn quotes_hyphenated_kubernetes_pod_names_and_keywords() {
        assert_eq!(quote_standby_name("node1"), "node1");
        assert_eq!(quote_standby_name("negroni-0"), "\"negroni-0\"");
        assert_eq!(quote_standby_name("NodeA"), "\"NodeA\"");
        assert_eq!(quote_standby_name("any"), "\"any\"");
        assert_eq!(quote_standby_name("*"), "*");
    }

    #[test]
    fn priority_mode_honors_sync_priority_nosync_and_lag_threshold() {
        let mut members = BTreeMap::new();
        members.insert(
            CompactString::const_new("pg-0"),
            mk_member("running", 1_000, json!({})),
        );
        members.insert(
            CompactString::const_new("pg-1"),
            mk_member("running", 950, json!({"sync_priority": 10})),
        );
        members.insert(
            CompactString::const_new("pg-2"),
            mk_member("running", 1_000, json!({"sync_priority": 2})),
        );
        members.insert(
            CompactString::const_new("pg-3"),
            mk_member("running", 1_000, json!({"sync_priority": 0})), // 0 == excluded
        );
        members.insert(
            CompactString::const_new("pg-laggy"),
            mk_member("running", 200, json!({"sync_priority": 99})), // exceeds max_lag_syncnode=500
        );

        let plan = compute_sync_standby_plan(
            &CompactString::const_new("pg-0"),
            WalLsn(1_000),
            SyncMode::Priority,
            false,
            2,
            500,
            &members,
            None,
        );

        assert_eq!(
            plan.guc_synchronous_standby_names,
            "FIRST 2 (\"pg-1\",\"pg-2\")"
        );
        assert_eq!(
            plan.dcs_sync_state.sync_standby.as_deref(),
            Some("pg-1,pg-2")
        );
        assert_eq!(plan.dcs_sync_state.quorum, 0);
    }

    #[test]
    fn quorum_mode_computes_any_n_and_pigeonhole_quorum_deficit() {
        let mut members = BTreeMap::new();
        members.insert(
            CompactString::const_new("node0"),
            mk_member("running", 5_000, json!({})),
        );
        members.insert(
            CompactString::const_new("node1"),
            mk_member("running", 5_000, json!({})),
        );
        members.insert(
            CompactString::const_new("node2"),
            mk_member("running", 4_900, json!({})),
        );
        members.insert(
            CompactString::const_new("node3"),
            mk_member("running", 5_000, json!({})),
        );

        // 3 active standbys, synchronous_node_count = 2 -> ANY 2 (node1,node2,node3), quorum = 3 - 2 = 1
        let plan = compute_sync_standby_plan(
            &CompactString::const_new("node0"),
            WalLsn(5_000),
            SyncMode::Quorum,
            true,
            2,
            1_000,
            &members,
            None,
        );

        assert_eq!(
            plan.guc_synchronous_standby_names,
            "ANY 2 (node1,node2,node3)"
        );
        assert_eq!(
            plan.dcs_sync_state.sync_standby.as_deref(),
            Some("node1,node2,node3")
        );
        assert_eq!(plan.dcs_sync_state.quorum, 1);
    }

    #[test]
    fn strict_mode_blocks_writes_with_star_when_no_standbys_qualify() {
        let members = BTreeMap::new();
        let strict_plan = compute_sync_standby_plan(
            &CompactString::const_new("pg-0"),
            WalLsn(1_000),
            SyncMode::Priority,
            true,
            1,
            100,
            &members,
            None,
        );
        assert_eq!(strict_plan.guc_synchronous_standby_names, "*");
        assert_eq!(strict_plan.dcs_sync_state.sync_standby, None);

        let non_strict_plan = compute_sync_standby_plan(
            &CompactString::const_new("pg-0"),
            WalLsn(1_000),
            SyncMode::Priority,
            false,
            1,
            100,
            &members,
            None,
        );
        assert_eq!(non_strict_plan.guc_synchronous_standby_names, "");
    }
}
