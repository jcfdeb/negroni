#![no_main]

use libfuzzer_sys::fuzz_target;
use negroni_types::{
    ClusterStatus, FailoverIntent, HistoryEntry, MemberData, SyncState, WalLsn,
};

fuzz_target!(|data: &[u8]| {
    let _ = serde_json::from_slice::<MemberData>(data);
    let _ = serde_json::from_slice::<ClusterStatus>(data);
    let _ = serde_json::from_slice::<SyncState>(data);
    let _ = serde_json::from_slice::<FailoverIntent>(data);
    let _ = serde_json::from_slice::<Vec<HistoryEntry>>(data);
    if let Ok(text) = std::str::from_utf8(data) {
        let _ = text.parse::<WalLsn>();
    }
});
