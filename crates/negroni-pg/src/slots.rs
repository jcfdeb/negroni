use std::collections::{BTreeMap, BTreeSet};

use compact_str::CompactString;
use negroni_config::DaemonConfig;
use negroni_types::{ClusterStatus, WalLsn};

use crate::config_writer::member_name_to_slot_name;
use crate::version::{PgCapabilities, PgMajor};

/// Live row read from `pg_catalog.pg_replication_slots` across PostgreSQL 15–18.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedSlot {
    pub slot_name: CompactString,
    pub is_logical: bool,
    pub database: Option<CompactString>,
    pub plugin: Option<CompactString>,
    pub restart_lsn: Option<WalLsn>,
    pub confirmed_flush_lsn: Option<WalLsn>,
    pub active: bool,
    /// Added in PostgreSQL 16 (`false` on PG 15).
    pub conflicting: bool,
    /// Added in PostgreSQL 17 (`false` on PG 15–16).
    pub failover: bool,
    /// Added in PostgreSQL 17 (`true` on standby when managed by `slotsync`).
    pub synced: bool,
    /// Added in PostgreSQL 18 (`None` on PG 15–17).
    pub slotsync_skip_reason: Option<CompactString>,
}

/// Desired state for a Patroni permanent logical replication slot (`bootstrap.dcs.slots`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermanentLogicalSlotTarget {
    pub database: CompactString,
    pub plugin: CompactString,
    pub confirmed_lsn: WalLsn,
}

/// Desired physical and permanent logical slots on the local PostgreSQL instance.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DesiredSlotsSpec {
    pub physical_members: BTreeSet<CompactString>,
    pub permanent_logical: BTreeMap<CompactString, PermanentLogicalSlotTarget>,
}

impl DesiredSlotsSpec {
    /// Builds the desired slot specification from `DaemonConfig`, DCS `/status`, and active peer names.
    pub fn from_cluster_and_config(
        cfg: &DaemonConfig,
        status: &ClusterStatus,
        peer_member_names: &[CompactString],
    ) -> Self {
        let mut spec = Self::default();
        let use_slots = cfg.dynamic.postgresql.use_slots.unwrap_or(true);

        if use_slots {
            for peer in peer_member_names {
                if *peer != cfg.name {
                    spec.physical_members.insert(member_name_to_slot_name(peer));
                }
            }
        }

        for (slot_name, slot_cfg) in &cfg.dynamic.slots {
            if slot_cfg.slot_type == "physical" {
                if use_slots {
                    spec.physical_members
                        .insert(member_name_to_slot_name(slot_name));
                }
            } else if slot_cfg.slot_type == "logical" {
                let db = slot_cfg
                    .database
                    .clone()
                    .unwrap_or_else(|| CompactString::const_new("postgres"));
                let plugin = slot_cfg
                    .plugin
                    .clone()
                    .unwrap_or_else(|| CompactString::const_new("pgoutput"));
                let confirmed_lsn = status
                    .slots
                    .get(slot_name)
                    .and_then(|v| *v)
                    .unwrap_or(status.optime);
                spec.permanent_logical.insert(
                    slot_name.clone(),
                    PermanentLogicalSlotTarget {
                        database: db,
                        plugin,
                        confirmed_lsn,
                    },
                );
            }
        }

        spec
    }
}

/// Creation parameters for a logical replication slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalSlotCreateOp {
    pub database: CompactString,
    pub slot_name: CompactString,
    pub plugin: CompactString,
    /// When `true` (PG 17–18 primaries), creates the slot with `failover => true` so PostgreSQL's
    /// native `slotsync` worker synchronizes it to standbys automatically.
    pub enable_native_failover: bool,
}

/// Advancement parameters for a Patroni-managed standby logical slot (PG 15–16).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalSlotAdvanceOp {
    pub database: CompactString,
    pub slot_name: CompactString,
    pub target_lsn: WalLsn,
}

/// Pure, deterministic reconciliation plan produced by [`plan_slot_reconciliation`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SlotReconcilePlan {
    pub drop_slots: Vec<CompactString>,
    pub create_physical: Vec<CompactString>,
    pub create_logical: Vec<LogicalSlotCreateOp>,
    pub advance_logical: Vec<LogicalSlotAdvanceOp>,
}

impl SlotReconcilePlan {
    pub fn is_empty(&self) -> bool {
        self.drop_slots.is_empty()
            && self.create_physical.is_empty()
            && self.create_logical.is_empty()
            && self.advance_logical.is_empty()
    }
}

/// Computes the exact SQL slot mutations needed across **PostgreSQL 15–16** (Patroni-driven
/// `pg_replication_slot_advance()`) and **PostgreSQL 17–18** (native `failover = true` `slotsync`
/// worker coordination + stale unsynced slot cleanup on demoted primaries).
///
/// # Examples
///
/// ```
/// use std::collections::BTreeMap;
/// use compact_str::CompactString;
/// use negroni_pg::slots::{DesiredSlotsSpec, ObservedSlot, plan_slot_reconciliation};
/// use negroni_pg::version::PgMajor;
///
/// // On PG 17, a demoted former primary rejoining as a standby drops stale `failover = true`
/// // slots where `synced == false` so PostgreSQL's `slotsync` worker can recreate them cleanly.
/// let mut observed = BTreeMap::new();
/// observed.insert(
///     CompactString::const_new("cdc_orders"),
///     ObservedSlot {
///         slot_name: CompactString::const_new("cdc_orders"),
///         is_logical: true,
///         database: Some(CompactString::const_new("orders")),
///         plugin: Some(CompactString::const_new("pgoutput")),
///         restart_lsn: None,
///         confirmed_flush_lsn: None,
///         active: false,
///         conflicting: false,
///         failover: true,
///         synced: false,
///         slotsync_skip_reason: None,
///     },
/// );
/// let plan = plan_slot_reconciliation(
///     PgMajor::V17.capabilities(),
///     false,
///     true,
///     &observed,
///     &DesiredSlotsSpec::default(),
/// );
/// assert_eq!(plan.drop_slots, vec![CompactString::const_new("cdc_orders")]);
/// ```
pub fn plan_slot_reconciliation(
    caps: PgCapabilities,
    is_primary: bool,
    is_demoted_rejoin: bool,
    observed: &BTreeMap<CompactString, ObservedSlot>,
    desired: &DesiredSlotsSpec,
) -> SlotReconcilePlan {
    let mut plan = SlotReconcilePlan::default();

    for (name, slot) in observed {
        // 1. PG 17–18 Rejoin Guard: When a former primary is demoted/rewound and rejoins as a
        //    standby, any pre-existing `failover = true` logical slots on disk have `synced = false`,
        //    which causes PostgreSQL's `slotsync` worker to abort with duplicate-slot errors unless
        //    dropped before streaming resumes.
        if caps.native_failover_slots
            && !is_primary
            && is_demoted_rejoin
            && slot.is_logical
            && slot.failover
            && !slot.synced
        {
            plan.drop_slots.push(name.clone());
            continue;
        }

        // 2. Never manually advance or garbage-collect PG 17–18 `failover = true` logical slots
        //    (whether created by Patroni or `CREATE SUBSCRIPTION ... WITH (failover = true)`);
        //    PostgreSQL's `slotsync` subsystem owns them.
        if caps.native_failover_slots && slot.is_logical && slot.failover {
            continue;
        }

        // 3. Drop invalidated/conflicting standby logical slots (PG 16+) so they can be recreated.
        if !is_primary && slot.conflicting && !slot.active {
            plan.drop_slots.push(name.clone());
            continue;
        }

        // 4. Reclaim stale physical member slots or deleted permanent logical slots once inactive.
        let keep = if slot.is_logical {
            desired.permanent_logical.contains_key(name)
        } else {
            desired.physical_members.contains(name)
        };
        if !keep && !slot.active {
            plan.drop_slots.push(name.clone());
        }
    }

    // Physical member slots are maintained on the primary (and cascading standbys when configured).
    if is_primary {
        for phys in &desired.physical_members {
            if !observed.contains_key(phys) {
                plan.create_physical.push(phys.clone());
            }
        }
    }

    for (name, target) in &desired.permanent_logical {
        match observed.get(name) {
            None => {
                // On PG 17–18, create permanent logical slots on the primary with `failover = true`
                // and let `slotsync` materialize them on standbys. On PG 16, standbys create non-failover
                // slots directly via `standby_logical_slot_create`. On PG 15, standbys cannot create
                // logical slots via SQL while in recovery (`!standby_logical_slot_create`).
                if is_primary {
                    plan.create_logical.push(LogicalSlotCreateOp {
                        database: target.database.clone(),
                        slot_name: name.clone(),
                        plugin: target.plugin.clone(),
                        enable_native_failover: caps.native_failover_slots,
                    });
                } else if caps.standby_logical_slot_create && !caps.native_failover_slots {
                    plan.create_logical.push(LogicalSlotCreateOp {
                        database: target.database.clone(),
                        slot_name: name.clone(),
                        plugin: target.plugin.clone(),
                        enable_native_failover: false,
                    });
                }
            }
            Some(existing)
                if !is_primary
                    && !existing.failover
                    && !existing.conflicting
                    && existing.confirmed_flush_lsn.unwrap_or_default() < target.confirmed_lsn =>
            {
                plan.advance_logical.push(LogicalSlotAdvanceOp {
                    database: target.database.clone(),
                    slot_name: name.clone(),
                    target_lsn: target.confirmed_lsn,
                });
            }
            _ => {}
        }
    }

    plan
}

/// Returns the version-routed `SELECT` query against `pg_catalog.pg_replication_slots` so all 10
/// columns normalize identically across PG 15, 16, 17, and 18.
pub const fn list_replication_slots_sql(major: PgMajor) -> &'static str {
    match major {
        PgMajor::V15 => {
            "SELECT
                slot_name::text,
                slot_type = 'logical' AS is_logical,
                database::text,
                plugin::text,
                (restart_lsn - '0/0'::pg_lsn)::int8 AS restart_lsn,
                (confirmed_flush_lsn - '0/0'::pg_lsn)::int8 AS confirmed_lsn,
                active,
                false AS conflicting,
                false AS failover,
                false AS synced,
                NULL::text AS slotsync_skip_reason
             FROM pg_catalog.pg_replication_slots"
        }
        PgMajor::V16 => {
            "SELECT
                slot_name::text,
                slot_type = 'logical' AS is_logical,
                database::text,
                plugin::text,
                (restart_lsn - '0/0'::pg_lsn)::int8 AS restart_lsn,
                (confirmed_flush_lsn - '0/0'::pg_lsn)::int8 AS confirmed_lsn,
                active,
                COALESCE(conflicting, false) AS conflicting,
                false AS failover,
                false AS synced,
                NULL::text AS slotsync_skip_reason
             FROM pg_catalog.pg_replication_slots"
        }
        PgMajor::V17 => {
            "SELECT
                slot_name::text,
                slot_type = 'logical' AS is_logical,
                database::text,
                plugin::text,
                (restart_lsn - '0/0'::pg_lsn)::int8 AS restart_lsn,
                (confirmed_flush_lsn - '0/0'::pg_lsn)::int8 AS confirmed_lsn,
                active,
                COALESCE(conflicting, false) AS conflicting,
                COALESCE(failover, false) AS failover,
                COALESCE(synced, false) AS synced,
                NULL::text AS slotsync_skip_reason
             FROM pg_catalog.pg_replication_slots"
        }
        PgMajor::V18 => {
            "SELECT
                slot_name::text,
                slot_type = 'logical' AS is_logical,
                database::text,
                plugin::text,
                (restart_lsn - '0/0'::pg_lsn)::int8 AS restart_lsn,
                (confirmed_flush_lsn - '0/0'::pg_lsn)::int8 AS confirmed_lsn,
                active,
                COALESCE(conflicting, false) AS conflicting,
                COALESCE(failover, false) AS failover,
                COALESCE(synced, false) AS synced,
                slotsync_skip_reason::text AS slotsync_skip_reason
             FROM pg_catalog.pg_replication_slots"
        }
    }
}

/// Renders comma-separated slot names for PostgreSQL 17–18 `synchronized_standby_slots`.
pub fn format_synchronized_standby_slots(standby_members: &[CompactString]) -> String {
    standby_members
        .iter()
        .map(|m| member_name_to_slot_name(m).to_string())
        .collect::<Vec<_>>()
        .join(",")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn sample_desired() -> DesiredSlotsSpec {
        let mut desired = DesiredSlotsSpec::default();
        desired
            .physical_members
            .insert(CompactString::const_new("pg_node_2"));
        desired.permanent_logical.insert(
            CompactString::const_new("debezium_orders"),
            PermanentLogicalSlotTarget {
                database: CompactString::const_new("orders"),
                plugin: CompactString::const_new("pgoutput"),
                confirmed_lsn: WalLsn(5_000),
            },
        );
        desired
    }

    #[test]
    fn pg16_standby_creates_and_advances_permanent_logical_slots_and_drops_conflicting() {
        let desired = sample_desired();

        // 1. Missing on PG 16 standby -> created via `standby_logical_slot_create` without failover flag
        let create_plan = plan_slot_reconciliation(
            PgMajor::V16.capabilities(),
            false,
            false,
            &BTreeMap::new(),
            &desired,
        );
        assert_eq!(create_plan.create_logical.len(), 1);
        assert!(!create_plan.create_logical[0].enable_native_failover);

        // 2. Existing behind `confirmed_lsn` -> scheduled for `pg_replication_slot_advance()`
        let mut observed = BTreeMap::new();
        observed.insert(
            CompactString::const_new("debezium_orders"),
            ObservedSlot {
                slot_name: CompactString::const_new("debezium_orders"),
                is_logical: true,
                database: Some(CompactString::const_new("orders")),
                plugin: Some(CompactString::const_new("pgoutput")),
                restart_lsn: Some(WalLsn(1_000)),
                confirmed_flush_lsn: Some(WalLsn(2_500)),
                active: false,
                conflicting: false,
                failover: false,
                synced: false,
                slotsync_skip_reason: None,
            },
        );
        let adv_plan = plan_slot_reconciliation(
            PgMajor::V16.capabilities(),
            false,
            false,
            &observed,
            &desired,
        );
        assert_eq!(
            adv_plan.advance_logical,
            vec![LogicalSlotAdvanceOp {
                database: CompactString::const_new("orders"),
                slot_name: CompactString::const_new("debezium_orders"),
                target_lsn: WalLsn(5_000),
            }]
        );

        // 3. Conflicting inactive slot on standby -> dropped so next tick recreates cleanly
        observed.get_mut("debezium_orders").unwrap().conflicting = true;
        let conflict_plan = plan_slot_reconciliation(
            PgMajor::V16.capabilities(),
            false,
            false,
            &observed,
            &desired,
        );
        assert_eq!(
            conflict_plan.drop_slots,
            vec![CompactString::const_new("debezium_orders")]
        );
    }

    #[test]
    fn pg17_and_pg18_delegate_failover_slots_to_slotsync_and_clean_stale_demoted_slots() {
        let desired = sample_desired();

        // On PG 18 Primary: creates physical member slot + logical slot with `enable_native_failover = true`
        let primary_plan = plan_slot_reconciliation(
            PgMajor::V18.capabilities(),
            true,
            false,
            &BTreeMap::new(),
            &desired,
        );
        assert_eq!(
            primary_plan.create_physical,
            vec![CompactString::const_new("pg_node_2")]
        );
        assert_eq!(primary_plan.create_logical.len(), 1);
        assert!(primary_plan.create_logical[0].enable_native_failover);

        // On PG 18 Standby: `slotsync` materializes `failover = true, synced = true` slots;
        // Negroni must NEVER manually advance or drop them!
        let mut standby_obs = BTreeMap::new();
        standby_obs.insert(
            CompactString::const_new("debezium_orders"),
            ObservedSlot {
                slot_name: CompactString::const_new("debezium_orders"),
                is_logical: true,
                database: Some(CompactString::const_new("orders")),
                plugin: Some(CompactString::const_new("pgoutput")),
                restart_lsn: Some(WalLsn(1_000)),
                confirmed_flush_lsn: Some(WalLsn(2_000)),
                active: false,
                conflicting: false,
                failover: true,
                synced: true,
                slotsync_skip_reason: Some(CompactString::const_new("remote_behind")),
            },
        );
        let steady_standby = plan_slot_reconciliation(
            PgMajor::V18.capabilities(),
            false,
            false,
            &standby_obs,
            &desired,
        );
        assert!(steady_standby.is_empty());

        // On Demoted Rejoin (`is_demoted_rejoin = true`): `failover = true` with `synced = false` IS dropped!
        standby_obs.get_mut("debezium_orders").unwrap().synced = false;
        let rejoin_plan = plan_slot_reconciliation(
            PgMajor::V18.capabilities(),
            false,
            true,
            &standby_obs,
            &desired,
        );
        assert_eq!(
            rejoin_plan.drop_slots,
            vec![CompactString::const_new("debezium_orders")]
        );
    }
}
