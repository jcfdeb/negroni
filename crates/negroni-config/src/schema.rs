use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use compact_str::CompactString;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value as JsonValue;

use crate::timing::{ValidatedTimings, WatchdogMode};

/// Credential wrapper that redacts secret values in `Debug` output so passwords and
/// tokens are never leaked into `tracing` logs or error diagnostics.
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("\"[REDACTED]\"")
    }
}

/// Synchronous replication mode (`synchronous_mode` in `patroni.yml` and DCS `/config`).
/// Supports `false`, `true` (priority `FIRST N`), and `"quorum"` (`ANY N`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncMode {
    #[default]
    Off,
    Priority,
    Quorum,
}

impl SyncMode {
    pub const fn is_enabled(self) -> bool {
        !matches!(self, Self::Off)
    }

    pub const fn is_quorum(self) -> bool {
        matches!(self, Self::Quorum)
    }
}

impl<'de> Deserialize<'de> for SyncMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum RawSyncMode {
            Bool(bool),
            Text(String),
        }

        match RawSyncMode::deserialize(deserializer)? {
            RawSyncMode::Bool(false) => Ok(Self::Off),
            RawSyncMode::Bool(true) => Ok(Self::Priority),
            RawSyncMode::Text(s) => match s.trim().to_ascii_lowercase().as_str() {
                "off" | "false" => Ok(Self::Off),
                "on" | "true" | "priority" => Ok(Self::Priority),
                "quorum" => Ok(Self::Quorum),
                other => Err(serde::de::Error::unknown_variant(
                    other,
                    &["false", "true", "quorum"],
                )),
            },
        }
    }
}

fn deserialize_host_list<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum RawHosts {
        List(Vec<String>),
        Csv(String),
    }

    match Option::<RawHosts>::deserialize(deserializer)? {
        None => Ok(Vec::new()),
        Some(RawHosts::List(list)) => Ok(list
            .into_iter()
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .collect()),
        Some(RawHosts::Csv(csv)) => Ok(csv
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .collect()),
    }
}

const fn default_ttl() -> u64 {
    30
}
const fn default_loop_wait() -> u64 {
    10
}
const fn default_retry_timeout() -> u64 {
    10
}
const fn default_max_lag_failover() -> u64 {
    1_048_576
}
const fn default_max_lag_syncnode() -> i64 {
    -1
}
const fn default_primary_start_timeout() -> u64 {
    300
}
const fn default_sync_node_count() -> usize {
    1
}
const fn default_true() -> bool {
    true
}
fn default_restapi_listen() -> String {
    "127.0.0.1:8008".to_owned()
}
fn default_pg_listen() -> String {
    "127.0.0.1:5432".to_owned()
}
fn default_pg_data_dir() -> PathBuf {
    PathBuf::from("/var/lib/postgresql/data")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PermanentSlotConfig {
    #[serde(rename = "type", default = "default_slot_type")]
    pub slot_type: CompactString,
    #[serde(default)]
    pub database: Option<CompactString>,
    #[serde(default)]
    pub plugin: Option<CompactString>,
}

fn default_slot_type() -> CompactString {
    CompactString::const_new("logical")
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct StandbyClusterConfig {
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub port: Option<u16>,
    #[serde(default)]
    pub primary_slot_name: Option<CompactString>,
    #[serde(default)]
    pub create_replica_methods: Vec<CompactString>,
    #[serde(default)]
    pub restore_command: Option<String>,
    #[serde(default)]
    pub archive_cleanup_command: Option<String>,
    #[serde(default)]
    pub recovery_min_apply_delay: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DynamicPgSection {
    #[serde(default)]
    pub use_pg_rewind: Option<bool>,
    #[serde(default)]
    pub use_slots: Option<bool>,
    #[serde(default)]
    pub parameters: BTreeMap<CompactString, JsonValue>,
    #[serde(default)]
    pub pg_hba: Vec<String>,
    #[serde(default)]
    pub pg_ident: Vec<String>,
}

/// Dynamic cluster-wide configuration stored in DCS `/config` (seeded from `bootstrap.dcs`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DynamicClusterConfig {
    #[serde(default = "default_ttl")]
    pub ttl: u64,
    #[serde(default = "default_loop_wait")]
    pub loop_wait: u64,
    #[serde(default = "default_retry_timeout")]
    pub retry_timeout: u64,
    #[serde(default = "default_max_lag_failover")]
    pub maximum_lag_on_failover: u64,
    #[serde(default = "default_max_lag_syncnode")]
    pub maximum_lag_on_syncnode: i64,
    #[serde(default)]
    pub max_timelines_history: u64,
    #[serde(
        default = "default_primary_start_timeout",
        alias = "master_start_timeout"
    )]
    pub primary_start_timeout: u64,
    #[serde(default, alias = "master_stop_timeout")]
    pub primary_stop_timeout: Option<u64>,
    #[serde(default)]
    pub synchronous_mode: SyncMode,
    #[serde(default)]
    pub synchronous_mode_strict: bool,
    #[serde(default = "default_sync_node_count")]
    pub synchronous_node_count: usize,
    #[serde(default)]
    pub failsafe_mode: bool,
    #[serde(default)]
    pub pause: bool,
    #[serde(default)]
    pub check_timeline: bool,
    #[serde(default)]
    pub slots: BTreeMap<CompactString, PermanentSlotConfig>,
    #[serde(default)]
    pub standby_cluster: Option<StandbyClusterConfig>,
    #[serde(default)]
    pub postgresql: DynamicPgSection,
}

impl Default for DynamicClusterConfig {
    fn default() -> Self {
        Self {
            ttl: default_ttl(),
            loop_wait: default_loop_wait(),
            retry_timeout: default_retry_timeout(),
            maximum_lag_on_failover: default_max_lag_failover(),
            maximum_lag_on_syncnode: default_max_lag_syncnode(),
            max_timelines_history: 0,
            primary_start_timeout: default_primary_start_timeout(),
            primary_stop_timeout: None,
            synchronous_mode: SyncMode::Off,
            synchronous_mode_strict: false,
            synchronous_node_count: default_sync_node_count(),
            failsafe_mode: false,
            pause: false,
            check_timeline: false,
            slots: BTreeMap::new(),
            standby_cluster: None,
            postgresql: DynamicPgSection::default(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct BootstrapSection {
    #[serde(default)]
    pub dcs: DynamicClusterConfig,
    #[serde(default)]
    pub initdb: Vec<JsonValue>,
    #[serde(default)]
    pub pg_hba: Vec<String>,
    #[serde(default)]
    pub method: Option<CompactString>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct BasicAuthCredentials {
    #[serde(default)]
    pub username: CompactString,
    #[serde(default)]
    pub password: SecretString,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RestApiConfig {
    #[serde(default = "default_restapi_listen")]
    pub listen: String,
    #[serde(default)]
    pub connect_address: Option<String>,
    #[serde(default)]
    pub certfile: Option<PathBuf>,
    #[serde(default)]
    pub keyfile: Option<PathBuf>,
    #[serde(default)]
    pub cacert: Option<PathBuf>,
    #[serde(default)]
    pub verify_client: Option<CompactString>,
    #[serde(default)]
    pub authentication: Option<BasicAuthCredentials>,
    #[serde(default)]
    pub allowlist: Vec<String>,
    #[serde(default)]
    pub allowlist_include_members: bool,
}

impl Default for RestApiConfig {
    fn default() -> Self {
        Self {
            listen: default_restapi_listen(),
            connect_address: None,
            certfile: None,
            keyfile: None,
            cacert: None,
            verify_client: None,
            authentication: None,
            allowlist: Vec::new(),
            allowlist_include_members: false,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct PgRoleCredentials {
    #[serde(default)]
    pub username: Option<CompactString>,
    #[serde(default)]
    pub password: Option<SecretString>,
    #[serde(default)]
    pub sslmode: Option<CompactString>,
    #[serde(default)]
    pub sslcert: Option<PathBuf>,
    #[serde(default)]
    pub sslkey: Option<PathBuf>,
    #[serde(default)]
    pub sslrootcert: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct PgAuthSection {
    #[serde(default)]
    pub superuser: PgRoleCredentials,
    #[serde(default)]
    pub replication: PgRoleCredentials,
    #[serde(default)]
    pub rewind: PgRoleCredentials,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct PostgresConfig {
    #[serde(default = "default_pg_listen")]
    pub listen: String,
    #[serde(default)]
    pub connect_address: Option<String>,
    #[serde(default)]
    pub proxy_address: Option<String>,
    #[serde(default = "default_pg_data_dir")]
    pub data_dir: PathBuf,
    #[serde(default)]
    pub config_dir: Option<PathBuf>,
    #[serde(default)]
    pub bin_dir: Option<PathBuf>,
    #[serde(default)]
    pub pgpass: Option<PathBuf>,
    #[serde(default = "default_true")]
    pub use_unix_socket: bool,
    #[serde(default)]
    pub use_pg_rewind: bool,
    #[serde(default)]
    pub remove_data_directory_on_rewind_failure: bool,
    #[serde(default)]
    pub remove_data_directory_on_diverged_timelines: bool,
    #[serde(default)]
    pub create_replica_methods: Vec<CompactString>,
    #[serde(default)]
    pub authentication: PgAuthSection,
    #[serde(default)]
    pub parameters: BTreeMap<CompactString, JsonValue>,
    #[serde(default)]
    pub pg_hba: Vec<String>,
    #[serde(default)]
    pub pg_ident: Vec<String>,
    #[serde(default)]
    pub callbacks: BTreeMap<CompactString, String>,
}

impl Default for PostgresConfig {
    fn default() -> Self {
        Self {
            listen: default_pg_listen(),
            connect_address: None,
            proxy_address: None,
            data_dir: default_pg_data_dir(),
            config_dir: None,
            bin_dir: None,
            pgpass: None,
            use_unix_socket: true,
            use_pg_rewind: false,
            remove_data_directory_on_rewind_failure: false,
            remove_data_directory_on_diverged_timelines: false,
            create_replica_methods: Vec::new(),
            authentication: PgAuthSection::default(),
            parameters: BTreeMap::new(),
            pg_hba: Vec::new(),
            pg_ident: Vec::new(),
            callbacks: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct Etcd3Config {
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default, deserialize_with = "deserialize_host_list")]
    pub hosts: Vec<String>,
    #[serde(default)]
    pub protocol: Option<CompactString>,
    #[serde(default)]
    pub username: Option<CompactString>,
    #[serde(default)]
    pub password: Option<SecretString>,
    #[serde(default)]
    pub cacert: Option<PathBuf>,
    #[serde(default)]
    pub cert: Option<PathBuf>,
    #[serde(default)]
    pub key: Option<PathBuf>,
}

impl Etcd3Config {
    pub fn endpoints(&self) -> Vec<String> {
        let mut out = self.hosts.clone();
        if let Some(ref h) = self.host
            && !h.trim().is_empty()
            && !out.iter().any(|existing| existing == h.trim())
        {
            out.insert(0, h.trim().to_owned());
        }
        out
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct KubeConfig {
    #[serde(default)]
    pub namespace: Option<CompactString>,
    #[serde(default)]
    pub labels: BTreeMap<CompactString, CompactString>,
    #[serde(default)]
    pub scope_label: Option<CompactString>,
    #[serde(default)]
    pub role_label: Option<CompactString>,
    #[serde(default = "default_true")]
    pub use_endpoints: bool,
    #[serde(default)]
    pub pod_ip: Option<String>,
    #[serde(default)]
    pub bypass_api_service: bool,
}

impl Default for KubeConfig {
    fn default() -> Self {
        Self {
            namespace: None,
            labels: BTreeMap::new(),
            scope_label: None,
            role_label: None,
            use_endpoints: true,
            pod_ip: None,
            bypass_api_service: false,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct ConsulConfig {
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub token: Option<SecretString>,
    #[serde(default)]
    pub scheme: Option<CompactString>,
    #[serde(default)]
    pub dc: Option<CompactString>,
    #[serde(default)]
    pub cacert: Option<PathBuf>,
    #[serde(default)]
    pub cert: Option<PathBuf>,
    #[serde(default)]
    pub key: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct ZookeeperConfig {
    #[serde(default, deserialize_with = "deserialize_host_list")]
    pub hosts: Vec<String>,
    #[serde(default)]
    pub use_ssl: bool,
    #[serde(default)]
    pub cacert: Option<PathBuf>,
    #[serde(default)]
    pub cert: Option<PathBuf>,
    #[serde(default)]
    pub key: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct RaftConfig {
    #[serde(default)]
    pub data_dir: Option<PathBuf>,
    #[serde(default)]
    pub self_addr: Option<String>,
    #[serde(default)]
    pub bind_addr: Option<String>,
    #[serde(default, deserialize_with = "deserialize_host_list")]
    pub partner_addrs: Vec<String>,
    #[serde(default)]
    pub password: Option<SecretString>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct DcsBackendConfigs {
    #[serde(default)]
    pub etcd3: Option<Etcd3Config>,
    #[serde(default)]
    pub kubernetes: Option<KubeConfig>,
    #[serde(default)]
    pub consul: Option<ConsulConfig>,
    #[serde(default)]
    pub zookeeper: Option<ZookeeperConfig>,
    #[serde(default)]
    pub raft: Option<RaftConfig>,
    #[serde(default)]
    pub etcd: Option<Etcd3Config>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct NodeTags {
    #[serde(default)]
    pub nofailover: bool,
    #[serde(default)]
    pub noloadbalance: bool,
    #[serde(default)]
    pub clonefrom: bool,
    #[serde(default)]
    pub nosync: bool,
    #[serde(default)]
    pub nostream: bool,
    #[serde(default)]
    pub sync_priority: i64,
    #[serde(default)]
    pub replicatefrom: Option<CompactString>,
    #[serde(flatten)]
    pub custom: BTreeMap<CompactString, JsonValue>,
}

impl NodeTags {
    /// Flattens custom user tags as strings for lock-free `?tag_<k>=<v>` REST probe matching.
    pub fn custom_string_tags(&self) -> BTreeMap<CompactString, CompactString> {
        let mut out = BTreeMap::new();
        for (k, v) in &self.custom {
            let rendered = match v {
                JsonValue::String(s) => CompactString::from(s.as_str()),
                JsonValue::Bool(b) => CompactString::from(if *b { "true" } else { "false" }),
                JsonValue::Number(n) => CompactString::from(n.to_string()),
                _ => continue,
            };
            out.insert(k.clone(), rendered);
        }
        out
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CitusConfig {
    pub group: u32,
    #[serde(default = "default_citus_db")]
    pub database: CompactString,
}

fn default_citus_db() -> CompactString {
    CompactString::const_new("postgres")
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct CtlConfig {
    #[serde(default)]
    pub insecure: bool,
    #[serde(default)]
    pub cacert: Option<PathBuf>,
    #[serde(default)]
    pub certfile: Option<PathBuf>,
    #[serde(default)]
    pub keyfile: Option<PathBuf>,
    #[serde(default)]
    pub authentication: Option<BasicAuthCredentials>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchdogConfig {
    pub mode: WatchdogMode,
    pub device: String,
    pub safety_margin: i64,
    pub timeout: Option<Duration>,
    pub min_timeout: Duration,
    pub ttl: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub worker_threads: NonZeroUsize,
    pub max_blocking_threads: NonZeroUsize,
    pub thread_stack_bytes: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            worker_threads: NonZeroUsize::MIN.saturating_add(3),
            max_blocking_threads: NonZeroUsize::MIN.saturating_add(7),
            thread_stack_bytes: 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct RawWatchdogSection {
    pub mode: Option<WatchdogMode>,
    pub device: Option<String>,
    pub safety_margin: Option<i64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct RawRuntimeSection {
    pub worker_threads: Option<NonZeroUsize>,
    pub max_blocking_threads: Option<NonZeroUsize>,
    pub thread_stack_bytes: Option<usize>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct RawPatroniDocument {
    pub scope: Option<CompactString>,
    pub name: Option<CompactString>,
    pub namespace: Option<CompactString>,
    #[serde(default)]
    pub bootstrap: BootstrapSection,
    #[serde(default)]
    pub restapi: RestApiConfig,
    #[serde(default)]
    pub postgresql: PostgresConfig,
    #[serde(default)]
    pub watchdog: RawWatchdogSection,
    #[serde(default)]
    pub tags: NodeTags,
    #[serde(default)]
    pub citus: Option<CitusConfig>,
    #[serde(default)]
    pub ctl: CtlConfig,
    #[serde(default)]
    pub runtime: RawRuntimeSection,
    #[serde(flatten)]
    pub dcs_backends: DcsBackendConfigs,
}

#[derive(Debug, Clone)]
pub struct DaemonConfig {
    pub scope: CompactString,
    pub name: CompactString,
    pub namespace: CompactString,
    pub config_path: String,
    pub dynamic: DynamicClusterConfig,
    pub bootstrap_initdb: Vec<JsonValue>,
    pub bootstrap_pg_hba: Vec<String>,
    pub restapi: RestApiConfig,
    pub postgresql: PostgresConfig,
    pub dcs: DcsBackendConfigs,
    pub tags: NodeTags,
    pub citus: Option<CitusConfig>,
    pub ctl: CtlConfig,
    pub timings: ValidatedTimings,
    pub watchdog: WatchdogConfig,
    pub runtime: RuntimeConfig,
}

impl DaemonConfig {
    /// Canonical logical DCS prefix (`/<namespace>/<scope>`), free of duplicate or trailing slashes.
    pub fn dcs_prefix(&self) -> String {
        let ns = self.namespace.trim_matches('/');
        let sc = self.scope.trim_matches('/');
        if ns.is_empty() {
            format!("/{sc}")
        } else {
            format!("/{ns}/{sc}")
        }
    }

    /// Effective PostgreSQL GUCs combining shared DCS `/config` `postgresql.parameters`
    /// with node-local `postgresql.parameters` overrides.
    pub fn effective_pg_parameters(&self) -> BTreeMap<CompactString, JsonValue> {
        let mut merged = self.dynamic.postgresql.parameters.clone();
        for (k, v) in &self.postgresql.parameters {
            merged.insert(k.clone(), v.clone());
        }
        merged
    }

    /// Effective `pg_hba.conf` lines: DCS `/config` `postgresql.pg_hba` takes precedence when
    /// non-empty, falling back to local `postgresql.pg_hba`, then `bootstrap.pg_hba`.
    pub fn effective_pg_hba(&self) -> &[String] {
        if !self.dynamic.postgresql.pg_hba.is_empty() {
            &self.dynamic.postgresql.pg_hba
        } else if !self.postgresql.pg_hba.is_empty() {
            &self.postgresql.pg_hba
        } else {
            &self.bootstrap_pg_hba
        }
    }
}
