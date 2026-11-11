#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use compact_str::CompactString;
use negroni_types::{
    CasToken, ClusterSnapshot, ClusterStatus, FailoverIntent, HistoryEntry, MemberData, SyncState,
    Versioned, WalLsn,
};
use serde_json::json;

#[test]
fn member_data_roundtrips_real_patroni_4_1_primary_and_replica_payloads() {
    let primary_json = r#"{
        "conn_url": "postgres://10.24.0.10:5432/postgres",
        "api_url": "https://10.24.0.10:8008/patroni",
        "state": "running",
        "role": "primary",
        "version": "4.1.0",
        "xlog_location": 97500059720,
        "timeline": 4,
        "pending_restart": true,
        "pending_restart_reason": {
            "max_connections": {
                "old_value": "100",
                "new_value": "250"
            }
        },
        "group": 0,
        "tags": {
            "nofailover": false,
            "clonefrom": true,
            "sync_priority": 2,
            "datacenter": "eu-west-1a"
        }
    }"#;

    let primary: MemberData = serde_json::from_str(primary_json).unwrap();
    assert!(primary.is_primary_role());
    assert!(!primary.nofailover());
    assert!(primary.clonefrom());
    assert_eq!(primary.sync_priority(), 2);
    assert!(primary.pending_restart);
    assert_eq!(primary.timeline, Some(4));
    assert_eq!(
        primary.xlog_location,
        Some("16/B374D848".parse::<WalLsn>().unwrap())
    );
    // Citus `group` preserved via flattened `extra`
    assert_eq!(primary.extra.get("group"), Some(&json!(0)));

    let reserialized = serde_json::to_value(&primary).unwrap();
    let expected: serde_json::Value = serde_json::from_str(primary_json).unwrap();
    assert_eq!(reserialized, expected);

    // Legacy Patroni 3.x `"role": "master"` rolling-upgrade compatibility
    let legacy_master_json = r#"{
        "conn_url": "postgres://10.24.0.11:5432/postgres",
        "api_url": "http://10.24.0.11:8008/patroni",
        "state": "streaming",
        "role": "master",
        "version": "3.3.2",
        "replication_state": "streaming"
    }"#;
    let legacy: MemberData = serde_json::from_str(legacy_master_json).unwrap();
    assert!(legacy.is_primary_role());
    assert_eq!(legacy.replication_state.as_deref(), Some("streaming"));
}

#[test]
fn cluster_status_handles_patroni_4_1_slots_null_lsns_and_legacy_scalars() {
    let status_json = r#"{
        "optime": 97500059720,
        "slots": {
            "cdc_orders": 97500040192,
            "cdc_initializing": null,
            "pg_node_2": 97500059720
        },
        "retain_slots": ["pg_node_1", "pg_node_2", "cdc_orders", "cdc_initializing"]
    }"#;

    let status: ClusterStatus = serde_json::from_str(status_json).unwrap();
    assert_eq!(status.optime, WalLsn(97_500_059_720));
    assert_eq!(
        status.slots.get("cdc_orders").copied().flatten(),
        Some(WalLsn(97_500_040_192))
    );
    assert_eq!(status.slots.get("cdc_initializing"), Some(&None));
    assert_eq!(status.retain_slots.len(), 4);

    let roundtrip = serde_json::to_value(&status).unwrap();
    let expected: serde_json::Value = serde_json::from_str(status_json).unwrap();
    assert_eq!(roundtrip, expected);

    // Uninitialized `"optime": null` tolerance
    let null_optime: ClusterStatus = serde_json::from_str(r#"{"optime": null}"#).unwrap();
    assert_eq!(null_optime.optime, WalLsn::ZERO);

    // Legacy Patroni bare-integer `/status` compatibility
    let legacy_int: ClusterStatus = serde_json::from_str("67108864").unwrap();
    assert_eq!(legacy_int.optime, WalLsn(67_108_864));
}

#[test]
fn sync_state_omits_zero_quorum_in_priority_mode_and_preserves_quorum_mode() {
    let priority_json = r#"{"leader":"pg-0","sync_standby":"pg-1, pg-2"}"#;
    let priority: SyncState = serde_json::from_str(priority_json).unwrap();
    assert_eq!(priority.quorum, 0);
    assert!(priority.contains_standby("pg-1"));
    assert!(priority.contains_standby("pg-2"));
    assert!(!priority.contains_standby("pg-3"));

    // Must NOT emit `"quorum": 0` when re-serialized in priority mode
    let priority_val = serde_json::to_value(&priority).unwrap();
    assert_eq!(
        priority_val,
        json!({"leader": "pg-0", "sync_standby": "pg-1, pg-2"})
    );

    let quorum_json = r#"{"leader":"pg-0","sync_standby":"pg-1,pg-2,pg-3","quorum":2}"#;
    let quorum: SyncState = serde_json::from_str(quorum_json).unwrap();
    assert_eq!(quorum.quorum, 2);
    assert_eq!(
        serde_json::to_value(&quorum).unwrap(),
        json!({"leader": "pg-0", "sync_standby": "pg-1,pg-2,pg-3", "quorum": 2})
    );
}

#[test]
fn history_entries_roundtrip_3_4_and_5_element_patroni_tuples() {
    let history_json = r#"[
        [1, 67108864, "no recovery target specified"],
        [2, 100663296, "no recovery target specified", "2026-11-10T14:22:05+00:00"],
        [3, 134217728, "no recovery target specified", "2026-11-11T03:11:49+00:00", "pg-2"]
    ]"#;

    let entries: Vec<HistoryEntry> = serde_json::from_str(history_json).unwrap();
    assert_eq!(entries.len(), 3);

    assert_eq!(entries[0].timeline, 1);
    assert_eq!(entries[0].lsn, "0/4000000".parse().unwrap());
    assert_eq!(entries[0].timestamp, None);
    assert_eq!(entries[0].leader, None);

    assert_eq!(entries[1].timeline, 2);
    assert_eq!(
        entries[1].timestamp.as_deref(),
        Some("2026-11-10T14:22:05+00:00")
    );
    assert_eq!(entries[1].leader, None);

    assert_eq!(entries[2].timeline, 3);
    assert_eq!(entries[2].leader.as_deref(), Some("pg-2"));

    let reserialized = serde_json::to_value(&entries).unwrap();
    let expected: serde_json::Value = serde_json::from_str(history_json).unwrap();
    assert_eq!(reserialized, expected);
}

#[test]
fn failover_intent_and_full_cluster_snapshot_assemble_cleanly() {
    let intent_json =
        r#"{"leader":"pg-0","candidate":"pg-1","scheduled_at":"2026-11-12T01:00:00+00:00"}"#;
    let intent: FailoverIntent = serde_json::from_str(intent_json).unwrap();
    assert!(!intent.is_empty());
    assert_eq!(intent.candidate.as_deref(), Some("pg-1"));

    let empty_intent: FailoverIntent = serde_json::from_str(r#"{}"#).unwrap();
    assert!(empty_intent.is_empty());

    let snap = ClusterSnapshot {
        initialize: Some(Versioned {
            value: CompactString::const_new("7382910492817364512"),
            token: CasToken::Revision(10),
        }),
        leader: Some(Versioned {
            value: CompactString::const_new("pg-0"),
            token: CasToken::ResourceVersion(CompactString::const_new("48291")),
        }),
        failover: Some(Versioned {
            value: intent,
            token: CasToken::Revision(19),
        }),
        ..ClusterSnapshot::default()
    };

    assert_eq!(snap.leader.as_ref().map(|v| v.value.as_str()), Some("pg-0"));
}
