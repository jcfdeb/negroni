#![forbid(unsafe_code)]

#[cfg(feature = "etcd3")]
pub mod etcd3;
pub mod in_memory;
#[cfg(feature = "kube")]
pub mod kube;

use std::collections::BTreeMap;
use std::time::Duration;

use compact_str::CompactString;
use negroni_config::DaemonConfig;
use negroni_types::{
    CasToken, ClusterSnapshot, ClusterStatus, FailoverIntent, HistoryEntry, LeaseHandle,
    MemberData, SyncState,
};
use thiserror::Error;
use tokio::sync::mpsc;

pub use crate::in_memory::InMemoryDcs;

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
        entry: &HistoryEntry,
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
    fn delete_cluster(&self) -> impl Future<Output = Result<(), DcsError>> + Send {
        async { Ok(()) }
    }
    fn subscribe_watches(&self) -> mpsc::Receiver<DcsWatchEvent>;
}

/// Static dispatch enum across compiled DCS drivers (`etcd3`, `kubernetes`, and `in_memory`),
/// avoiding heap `dyn` vtable boxing on every per-tick consensus operation.
#[derive(Debug)]
pub enum AnyDcs {
    #[cfg(feature = "etcd3")]
    Etcd3(Box<etcd3::Etcd3Dcs>),
    #[cfg(feature = "kube")]
    Kube(Box<kube::KubeDcs>),
    InMemory(InMemoryDcs),
}

impl AnyDcs {
    /// Connects to the DCS backend configured in [`DaemonConfig`] (with optional override scope
    /// for multi-cluster `patronictl` / `negronictl` commands).
    pub async fn from_daemon_config(
        cfg: &DaemonConfig,
        override_scope: Option<&str>,
    ) -> Result<Self, DcsError> {
        let effective_scope = override_scope.unwrap_or(cfg.scope.as_str());
        let ns = cfg.namespace.trim_matches('/');
        let prefix = if ns.is_empty() {
            format!("/{effective_scope}")
        } else {
            format!("/{ns}/{effective_scope}")
        };

        tracing::debug!(
            scope = %effective_scope,
            namespace = %cfg.namespace,
            "initializing DCS backend from daemon configuration"
        );

        if let Some(etcd_cfg) = cfg.dcs.etcd3.as_ref().or(cfg.dcs.etcd.as_ref()) {
            #[cfg(feature = "etcd3")]
            {
                let dcs = etcd3::Etcd3Dcs::connect(etcd_cfg, prefix).await?;
                return Ok(Self::Etcd3(Box::new(dcs)));
            }
            #[cfg(not(feature = "etcd3"))]
            {
                let _ = (etcd_cfg, prefix);
                return Err(DcsError::Unreachable(
                    "etcd3 DCS configured, but binary was compiled without feature 'etcd3'"
                        .to_owned(),
                ));
            }
        }

        if let Some(ref kube_cfg) = cfg.dcs.kubernetes {
            #[cfg(feature = "kube")]
            {
                let dcs = kube::KubeDcs::from_config(effective_scope, kube_cfg.clone(), None)?;
                return Ok(Self::Kube(Box::new(dcs)));
            }
            #[cfg(not(feature = "kube"))]
            {
                let _ = kube_cfg;
                return Err(DcsError::Unreachable(
                    "kubernetes DCS configured, but binary was compiled without feature 'kube'"
                        .to_owned(),
                ));
            }
        }

        Err(DcsError::Unreachable(
            "no supported Tier-1 DCS backend ('etcd3' or 'kubernetes') configured in patroni.yml"
                .to_owned(),
        ))
    }

    /// Connects directly via a `-d/--dcs-url` string (`etcd3://host:2379`, `k8s://https://...`, or `memory://`).
    pub async fn from_dcs_url(
        dcs_url: &str,
        namespace: &str,
        scope: &str,
    ) -> Result<Self, DcsError> {
        let trimmed = dcs_url.trim();
        let ns = namespace.trim_matches('/');
        let prefix = if ns.is_empty() {
            format!("/{scope}")
        } else {
            format!("/{ns}/{scope}")
        };

        if trimmed.starts_with("memory://") {
            return Ok(Self::InMemory(InMemoryDcs::new()));
        }

        if trimmed.starts_with("etcd3://")
            || trimmed.starts_with("http+etcd3://")
            || trimmed.starts_with("https+etcd3://")
            || trimmed.starts_with("etcd://")
        {
            #[cfg(feature = "etcd3")]
            {
                let normalized = trimmed
                    .strip_prefix("etcd://")
                    .map_or_else(|| trimmed.to_owned(), |r| format!("etcd3://{r}"));
                let hosts: Vec<String> = normalized
                    .split(',')
                    .map(|s| s.trim().to_owned())
                    .filter(|s| !s.is_empty())
                    .collect();
                let etcd_cfg = negroni_config::Etcd3Config {
                    hosts,
                    ..negroni_config::Etcd3Config::default()
                };
                let dcs = etcd3::Etcd3Dcs::connect(&etcd_cfg, prefix).await?;
                return Ok(Self::Etcd3(Box::new(dcs)));
            }
            #[cfg(not(feature = "etcd3"))]
            {
                let _ = prefix;
                return Err(DcsError::Unreachable(
                    "etcd3 URL requested, but feature 'etcd3' is disabled".to_owned(),
                ));
            }
        }

        if trimmed.starts_with("k8s://") || trimmed.starts_with("kubernetes://") {
            #[cfg(feature = "kube")]
            {
                let kube_cfg = negroni_config::KubeConfig::default();
                let explicit = if trimmed == "k8s://" || trimmed == "kubernetes://" {
                    None
                } else {
                    Some(trimmed)
                };
                let dcs = kube::KubeDcs::from_config(scope, kube_cfg, explicit)?;
                return Ok(Self::Kube(Box::new(dcs)));
            }
            #[cfg(not(feature = "kube"))]
            {
                return Err(DcsError::Unreachable(
                    "kubernetes URL requested, but feature 'kube' is disabled".to_owned(),
                ));
            }
        }

        Err(DcsError::Unreachable(format!(
            "unsupported DCS URL scheme in '{trimmed}' (expected etcd3://, k8s://, or memory://)"
        )))
    }
}

impl DcsBackend for AnyDcs {
    async fn fetch_cluster(&self, timeout: Duration) -> Result<ClusterSnapshot, DcsError> {
        match self {
            #[cfg(feature = "etcd3")]
            Self::Etcd3(d) => d.fetch_cluster(timeout).await,
            #[cfg(feature = "kube")]
            Self::Kube(d) => d.fetch_cluster(timeout).await,
            Self::InMemory(d) => d.fetch_cluster(timeout).await,
        }
    }

    async fn ensure_lease(
        &self,
        ttl: Duration,
        current: Option<&LeaseHandle>,
    ) -> Result<LeaseHandle, DcsError> {
        match self {
            #[cfg(feature = "etcd3")]
            Self::Etcd3(d) => d.ensure_lease(ttl, current).await,
            #[cfg(feature = "kube")]
            Self::Kube(d) => d.ensure_lease(ttl, current).await,
            Self::InMemory(d) => d.ensure_lease(ttl, current).await,
        }
    }

    async fn cas_initialize(&self, sys_id: &str) -> Result<CasToken, DcsError> {
        match self {
            #[cfg(feature = "etcd3")]
            Self::Etcd3(d) => d.cas_initialize(sys_id).await,
            #[cfg(feature = "kube")]
            Self::Kube(d) => d.cas_initialize(sys_id).await,
            Self::InMemory(d) => d.cas_initialize(sys_id).await,
        }
    }

    async fn acquire_or_renew_leader(
        &self,
        self_name: &str,
        lease: &LeaseHandle,
        expected: Option<&CasToken>,
    ) -> Result<CasToken, DcsError> {
        match self {
            #[cfg(feature = "etcd3")]
            Self::Etcd3(d) => d.acquire_or_renew_leader(self_name, lease, expected).await,
            #[cfg(feature = "kube")]
            Self::Kube(d) => d.acquire_or_renew_leader(self_name, lease, expected).await,
            Self::InMemory(d) => d.acquire_or_renew_leader(self_name, lease, expected).await,
        }
    }

    async fn release_leader(&self, self_name: &str, expected: &CasToken) -> Result<(), DcsError> {
        match self {
            #[cfg(feature = "etcd3")]
            Self::Etcd3(d) => d.release_leader(self_name, expected).await,
            #[cfg(feature = "kube")]
            Self::Kube(d) => d.release_leader(self_name, expected).await,
            Self::InMemory(d) => d.release_leader(self_name, expected).await,
        }
    }

    async fn upsert_member(
        &self,
        self_name: &str,
        data: &MemberData,
        lease: &LeaseHandle,
    ) -> Result<(), DcsError> {
        match self {
            #[cfg(feature = "etcd3")]
            Self::Etcd3(d) => d.upsert_member(self_name, data, lease).await,
            #[cfg(feature = "kube")]
            Self::Kube(d) => d.upsert_member(self_name, data, lease).await,
            Self::InMemory(d) => d.upsert_member(self_name, data, lease).await,
        }
    }

    async fn publish_leader_state(
        &self,
        status: &ClusterStatus,
        failsafe_roster: Option<&BTreeMap<CompactString, String>>,
    ) -> Result<(), DcsError> {
        match self {
            #[cfg(feature = "etcd3")]
            Self::Etcd3(d) => d.publish_leader_state(status, failsafe_roster).await,
            #[cfg(feature = "kube")]
            Self::Kube(d) => d.publish_leader_state(status, failsafe_roster).await,
            Self::InMemory(d) => d.publish_leader_state(status, failsafe_roster).await,
        }
    }

    async fn append_history_entry(&self, entry: &HistoryEntry) -> Result<(), DcsError> {
        match self {
            #[cfg(feature = "etcd3")]
            Self::Etcd3(d) => d.append_history_entry(entry).await,
            #[cfg(feature = "kube")]
            Self::Kube(d) => d.append_history_entry(entry).await,
            Self::InMemory(d) => d.append_history_entry(entry).await,
        }
    }

    async fn cas_sync_state(
        &self,
        next: &SyncState,
        expected: Option<&CasToken>,
    ) -> Result<CasToken, DcsError> {
        match self {
            #[cfg(feature = "etcd3")]
            Self::Etcd3(d) => d.cas_sync_state(next, expected).await,
            #[cfg(feature = "kube")]
            Self::Kube(d) => d.cas_sync_state(next, expected).await,
            Self::InMemory(d) => d.cas_sync_state(next, expected).await,
        }
    }

    async fn cas_config(
        &self,
        config_json: &serde_json::Value,
        expected: Option<&CasToken>,
    ) -> Result<CasToken, DcsError> {
        match self {
            #[cfg(feature = "etcd3")]
            Self::Etcd3(d) => d.cas_config(config_json, expected).await,
            #[cfg(feature = "kube")]
            Self::Kube(d) => d.cas_config(config_json, expected).await,
            Self::InMemory(d) => d.cas_config(config_json, expected).await,
        }
    }

    async fn cas_failover(
        &self,
        intent: &FailoverIntent,
        expected: Option<&CasToken>,
    ) -> Result<CasToken, DcsError> {
        match self {
            #[cfg(feature = "etcd3")]
            Self::Etcd3(d) => d.cas_failover(intent, expected).await,
            #[cfg(feature = "kube")]
            Self::Kube(d) => d.cas_failover(intent, expected).await,
            Self::InMemory(d) => d.cas_failover(intent, expected).await,
        }
    }

    async fn clear_failover(&self, expected: &CasToken) -> Result<(), DcsError> {
        match self {
            #[cfg(feature = "etcd3")]
            Self::Etcd3(d) => d.clear_failover(expected).await,
            #[cfg(feature = "kube")]
            Self::Kube(d) => d.clear_failover(expected).await,
            Self::InMemory(d) => d.clear_failover(expected).await,
        }
    }

    async fn delete_cluster(&self) -> Result<(), DcsError> {
        match self {
            #[cfg(feature = "etcd3")]
            Self::Etcd3(d) => d.delete_cluster().await,
            #[cfg(feature = "kube")]
            Self::Kube(d) => d.delete_cluster().await,
            Self::InMemory(d) => d.delete_cluster().await,
        }
    }

    fn subscribe_watches(&self) -> mpsc::Receiver<DcsWatchEvent> {
        match self {
            #[cfg(feature = "etcd3")]
            Self::Etcd3(d) => d.subscribe_watches(),
            #[cfg(feature = "kube")]
            Self::Kube(d) => d.subscribe_watches(),
            Self::InMemory(d) => d.subscribe_watches(),
        }
    }
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
