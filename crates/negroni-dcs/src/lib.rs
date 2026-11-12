#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::time::Duration;

use compact_str::CompactString;
use negroni_types::{
    CasToken, ClusterSnapshot, ClusterStatus, FailoverIntent, LeaseHandle, MemberData, SyncState,
};
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DcsWatchEvent {
    LeaderChanged,
    ConfigChanged,
    FailoverRequested,
    TopologyChanged,
    WatchDisconnected,
}

#[derive(Debug, Error)]
pub enum DcsError {
    #[error("CAS precondition failed (expected {expected:?})")]
    CasConflict { expected: Option<CasToken> },
    #[error("lease expired or invalidated on server")]
    LeaseExpired,
    #[error("DCS unreachable or timed out: {0}")]
    Unreachable(String),
}

pub trait DcsBackend: Send + Sync + 'static {
    fn fetch_cluster(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<ClusterSnapshot, DcsError>> + Send;
    fn ensure_lease(
        &self,
        ttl: Duration,
        current: Option<&LeaseHandle>,
    ) -> impl Future<Output = Result<LeaseHandle, DcsError>> + Send;
    fn cas_initialize(
        &self,
        sys_id: &str,
    ) -> impl Future<Output = Result<CasToken, DcsError>> + Send;
    fn acquire_or_renew_leader(
        &self,
        self_name: &str,
        lease: &LeaseHandle,
        expected: Option<&CasToken>,
    ) -> impl Future<Output = Result<CasToken, DcsError>> + Send;
    fn release_leader(
        &self,
        self_name: &str,
        expected: &CasToken,
    ) -> impl Future<Output = Result<(), DcsError>> + Send;
    fn upsert_member(
        &self,
        self_name: &str,
        data: &MemberData,
        lease: &LeaseHandle,
    ) -> impl Future<Output = Result<(), DcsError>> + Send;
    fn publish_leader_state(
        &self,
        status: &ClusterStatus,
        failsafe_roster: Option<&BTreeMap<CompactString, String>>,
    ) -> impl Future<Output = Result<(), DcsError>> + Send;
    fn append_history_entry(
        &self,
        entry: &negroni_types::HistoryEntry,
    ) -> impl Future<Output = Result<(), DcsError>> + Send;
    fn cas_sync_state(
        &self,
        next: &SyncState,
        expected: Option<&CasToken>,
    ) -> impl Future<Output = Result<CasToken, DcsError>> + Send;
    fn cas_config(
        &self,
        config_json: &serde_json::Value,
        expected: Option<&CasToken>,
    ) -> impl Future<Output = Result<CasToken, DcsError>> + Send;
    fn cas_failover(
        &self,
        intent: &FailoverIntent,
        expected: Option<&CasToken>,
    ) -> impl Future<Output = Result<CasToken, DcsError>> + Send;
    fn clear_failover(
        &self,
        expected: &CasToken,
    ) -> impl Future<Output = Result<(), DcsError>> + Send;
    fn subscribe_watches(&self) -> mpsc::Receiver<DcsWatchEvent>;
}

pub fn compiled_backends() -> Vec<&'static str> {
    let mut backends = Vec::new();
    if cfg!(feature = "etcd3") {
        backends.push("etcd3");
    }
    if cfg!(feature = "kube") {
        backends.push("kubernetes");
    }
    if cfg!(feature = "consul") {
        backends.push("consul");
    }
    if cfg!(feature = "zookeeper") {
        backends.push("zookeeper");
    }
    if cfg!(feature = "raft") {
        backends.push("raft");
    }
    if cfg!(feature = "etcd2") {
        backends.push("etcd2");
    }
    if cfg!(feature = "exhibitor") {
        backends.push("exhibitor");
    }
    backends
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn compiled_backends_matches_active_feature_flags() {
        let list = compiled_backends();
        assert_eq!(list.contains(&"etcd3"), cfg!(feature = "etcd3"));
        assert_eq!(list.contains(&"kubernetes"), cfg!(feature = "kube"));
    }
}
