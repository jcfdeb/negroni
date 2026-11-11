# Negroni — Low-Level Design (LLD)

---

## 1. Cargo Workspace Manifests & Multicall Binary

### 1.1 Repository Layout

```text
negroni/
├── AGENTS.md
├── Cargo.toml
├── docs/
│   ├── HLD.md
│   ├── LLD.md
│   └── IMPLEMENTATION_PLAN.md
├── crates/
│   ├── negroni-types/          # Wire serde structs, WalLsn, CasToken, PgMajor (15..=18)
│   ├── negroni-config/         # patroni.yml, PATRONI_*/NEGRONI_* overlay, timing validator
│   ├── negroni-watchdog/       # Dedicated OS thread (/dev/watchdog ioctl) + systemd sd_notify
│   ├── negroni-dcs/            # DcsBackend trait, AnyDcs enum, etcd3/kube/consul/zk/openraft
│   ├── negroni-pg/             # pidfd supervisor, 1-conn SqlControlActor, conf/HBA, slots, Citus
│   ├── negroni-ha/             # Pure NodeCtx::step reducer, CoordinatorActor, JobSupervisor, DST
│   ├── negroni-api/            # axum + rustls ArcSwap probes & guarded control-plane endpoints
│   └── negroni-cli/            # clap + comfy-table + similar 19-command patronictl/negronictl
└── bin/
    └── negroni/                # Multicall binary entrypoint (daemon + CLI router)
```

### 1.2 Root Workspace `Cargo.toml`

```toml
[workspace]
resolver = "3"
members = [
    "crates/negroni-types",
    "crates/negroni-config",
    "crates/negroni-watchdog",
    "crates/negroni-dcs",
    "crates/negroni-pg",
    "crates/negroni-ha",
    "crates/negroni-api",
    "crates/negroni-cli",
    "bin/negroni",
]

[workspace.package]
version = "0.1.0"
edition = "2024"
rust-version = "1.85"
license = "MIT"

[workspace.lints.rust]
unsafe_code = "deny" # Overridden exclusively in crates/negroni-watchdog/src/ioctl_linux.rs

[workspace.lints.clippy]
unwrap_used = "deny"
expect_used = "deny"
panic = "deny"

[workspace.dependencies]
anyhow = "1.0"
arc-swap = "1.9"
axum = { version = "0.8", default-features = false, features = ["http1", "json", "query", "tokio"] }
axum-server = { version = "0.7", features = ["tls-rustls"] }
clap = { version = "4.5", features = ["derive", "env"] }
comfy-table = "7.1"
compact_str = { version = "0.10", features = ["serde"] }
etcd-client = { version = "0.14", features = ["tls"] }
ipnet = "2.10"
jiff = { version = "0.2", features = ["serde"] }
k8s-openapi = { version = "0.24", features = ["v1_32"] }
kube = { version = "0.99", default-features = false, features = ["client", "runtime", "rustls-tls"] }
openraft = { version = "0.9", features = ["serde"] }
proptest = "1.6"
reqwest = { version = "0.12", default-features = false, features = ["json", "rustls-tls"] }
rustix = { version = "1.1", features = ["fs", "process"] }
rustls = { version = "0.23", default-features = false, features = ["ring", "std", "tls12"] }
saphyr = "0.3"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
similar = "2.7"
smallvec = "1.14"
subtle = "2.6"
thiserror = "2.0"
tokio = { version = "1.43", features = ["rt-multi-thread", "macros", "net", "process", "signal", "sync", "time", "fs"] }
tokio-postgres = { version = "0.7", features = ["runtime"] }
tokio-postgres-rustls = "0.13"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
zookeeper-client = "0.8"
```

### 1.3 Binary Feature Matrix (`bin/negroni/Cargo.toml`)

```toml
[package]
name = "negroni"
version.workspace = true
edition.workspace = true
rust-version.workspace = true

[features]
default = [
    "dcs-etcd3",
    "dcs-kube",
    "dcs-consul",
    "dcs-zookeeper",
    "dcs-raft",
    "citus",
    "systemd",
]

# Tier-1 Modern DCS Backends
dcs-etcd3     = ["negroni-dcs/etcd3"]
dcs-kube      = ["negroni-dcs/kube"]
dcs-consul    = ["negroni-dcs/consul"]
dcs-zookeeper = ["negroni-dcs/zookeeper"]
dcs-raft      = ["negroni-dcs/raft"]

# Legacy Opt-In DCS Hooks
legacy-dcs    = ["dcs-etcd2", "dcs-exhibitor"]
dcs-etcd2     = ["negroni-dcs/etcd2"]
dcs-exhibitor = ["negroni-dcs/exhibitor"]

# Platform Extensions
citus         = ["negroni-pg/citus"]
systemd       = ["negroni-watchdog/systemd"]
aws-callbacks = ["negroni-ha/aws-callbacks"]
```

### 1.4 Multicall Entrypoint (`bin/negroni/src/main.rs`)

```rust
use std::{env, path::Path, process::ExitCode};

enum ExecutionMode {
    Daemon { config_path: String },
    Cli { args: Vec<String> },
}

fn detect_mode() -> ExecutionMode {
    let mut args: Vec<String> = env::args().collect();
    let bin_stem = args
        .first()
        .and_then(|a| Path::new(a).file_stem())
        .and_then(|s| s.to_str())
        .unwrap_or("negroni");

    // 1. Invoked via symlink `patronictl` or `negronictl`
    if matches!(bin_stem, "negronictl" | "patronictl") {
        return ExecutionMode::Cli { args };
    }

    // 2. Invoked as `negroni ctl <subcommand>`
    if args.get(1).map(String::as_str) == Some("ctl") {
        args.remove(1);
        return ExecutionMode::Cli { args };
    }

    // 3. Default daemon mode (`negroni postgres0.yml` or env config path)
    let config_path = args
        .get(1)
        .cloned()
        .or_else(|| env::var("NEGRONI_CONFIGURATION").ok())
        .or_else(|| env::var("PATRONI_CONFIGURATION").ok())
        .unwrap_or_else(|| "/etc/patroni/patroni.yml".to_owned());

    ExecutionMode::Daemon { config_path }
}

fn main() -> ExitCode {
    let _ = rustls::crypto::ring::default_provider().install_default();

    match detect_mode() {
        ExecutionMode::Cli { args } => negroni_cli::run_cli(args),
        ExecutionMode::Daemon { config_path } => {
            let cfg = match negroni_config::load_and_validate(&config_path) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("fatal configuration error: {e:#}");
                    return ExitCode::FAILURE;
                }
            };

            // Spawn hardware watchdog OS thread BEFORE Tokio reserves worker stacks
            let watchdog_handle = match negroni_watchdog::spawn_watchdog_thread(&cfg.watchdog) {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("fatal watchdog initialization error: {e:#}");
                    return ExitCode::FAILURE;
                }
            };

            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .worker_threads(cfg.runtime.worker_threads.get())
                .max_blocking_threads(cfg.runtime.max_blocking_threads.get())
                .thread_stack_size(cfg.runtime.thread_stack_bytes)
                .thread_name("negroni-wrk")
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("failed to pre-allocate Tokio runtime under vm.overcommit_memory limits: {e}");
                    return ExitCode::FAILURE;
                }
            };

            runtime.block_on(negroni_ha::run_daemon(cfg, watchdog_handle))
        }
    }
}
```

---

## 2. `negroni-config` — Dual-Prefix Env Overlay & Timing Validator

### 2.1 `saphyr` -> `serde_json::Value` Pipeline & Dual-Prefix Env Overlay
`negroni-config` parses `patroni.yml` via pure-Rust `saphyr` into a `serde_json::Value` document tree (`yaml_to_json_value`), eliminating any `unsafe` C `libyaml` dependencies while providing a single canonical JSON tree for layering `PATRONI_*` / `NEGRONI_*` environment overrides and dynamic DCS `/config` updates prior to `serde_json::from_value` schema decoding:

1. Parse `patroni.yml` via `saphyr::Yaml::load_from_str` and convert scalar/sequence/mapping nodes to `serde_json::Value` (accepting both YAML 1.2 strings such as `mode: off` and PyYAML/JSON booleans such as `mode: false`).
2. Scan `std::env::vars()` once at startup / reload and normalize keys starting with `PATRONI_` or `NEGRONI_` into canonical config paths.
3. If both `PATRONI_<SUFFIX>` and `NEGRONI_<SUFFIX>` exist with unequal values, apply `NEGRONI_<SUFFIX>` and emit a `tracing::warn!` diagnostic.

### 2.2 Watchdog & Lease Timing Validator (`crates/negroni-config/src/lib.rs`)

```rust
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WatchdogMode {
    Off,
    #[default]
    Automatic,
    Required,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidatedTimings {
    pub ttl: Duration,
    pub loop_wait: Duration,
    pub retry_timeout: Duration,
    pub watchdog_timeout: Option<Duration>,
    pub safety_margin: Duration,
}

pub fn validate_timings(
    ttl_secs: u64,
    loop_wait_secs: u64,
    retry_timeout_secs: u64,
    wd_mode: WatchdogMode,
    safety_margin_cfg: i64, // Default 5; -1 means floor(ttl / 2); must be -1 or >= 1
) -> Result<ValidatedTimings, ConfigError> {
    let effective_ttl = ttl_secs.max(20);
    let ttl = Duration::from_secs(effective_ttl);
    let loop_wait = Duration::from_secs(loop_wait_secs.max(1));
    let retry_timeout = Duration::from_secs(retry_timeout_secs.max(3));

    if loop_wait + (retry_timeout * 2) > ttl {
        return Err(ConfigError::LeaseRuleViolated { ttl, loop_wait, retry_timeout });
    }

    if wd_mode == WatchdogMode::Off {
        return Ok(ValidatedTimings {
            ttl,
            loop_wait,
            retry_timeout,
            watchdog_timeout: None,
            safety_margin: Duration::ZERO,
        });
    }

    let wd_secs = if safety_margin_cfg == -1 {
        effective_ttl / 2
    } else if let Ok(margin) = u64::try_from(safety_margin_cfg)
        && margin >= 1
    {
        effective_ttl.saturating_sub(margin)
    } else {
        return Err(ConfigError::InvalidSafetyMargin(safety_margin_cfg));
    };

    let wd_timeout = Duration::from_secs(wd_secs);
    let min_required = loop_wait + retry_timeout + Duration::from_secs(2);

    if wd_timeout < min_required {
        return Err(ConfigError::InsufficientDemotionBudget {
            wd_timeout,
            loop_wait,
            retry_timeout,
        });
    }

    Ok(ValidatedTimings {
        ttl,
        loop_wait,
        retry_timeout,
        watchdog_timeout: Some(wd_timeout),
        safety_margin: ttl - wd_timeout,
    })
}
```

---

## 3. `negroni-types` & `negroni-dcs` — Wire Models & `DcsBackend` Trait

### 3.1 `WalLsn` & Wire-Exact `serde` Structs (`crates/negroni-types/src/lib.rs`)

```rust
use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use compact_str::CompactString;
use serde::{Deserialize, Serialize};

/// Serializes as integer u64 in Patroni DCS/REST JSON, while formatting as `X/YYYYYYYY` for SQL/CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WalLsn(pub u64);

impl fmt::Display for WalLsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let hi = (self.0 >> 32) as u32;
        let lo = (self.0 & 0xFFFF_FFFF) as u32;
        write!(f, "{hi:X}/{lo:08X}")
    }
}

impl FromStr for WalLsn {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (hi_str, lo_str) = s.split_once('/').ok_or("invalid pg_lsn format")?;
        let hi = u64::from_str_radix(hi_str, 16).map_err(|_| "invalid high LSN hex")?;
        let lo = u64::from_str_radix(lo_str, 16).map_err(|_| "invalid low LSN hex")?;
        Ok(Self((hi << 32) | lo))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasToken {
    Revision(u64),
    ResourceVersion(CompactString),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseHandle(pub CompactString);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Versioned<T> {
    pub value: T,
    pub token: CasToken,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberData {
    pub conn_url: String,
    pub api_url: String,
    pub state: CompactString,
    pub role: CompactString,
    pub version: CompactString,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xlog_location: Option<WalLsn>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeline: Option<u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<CompactString, serde_json::Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterStatus {
    pub optime: WalLsn,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub slots: BTreeMap<CompactString, Option<WalLsn>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retain_slots: Vec<CompactString>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncState {
    pub leader: Option<CompactString>,
    pub sync_standby: Option<String>, // Comma-separated member names (Patroni 1:1 wire format)
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub quorum: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailoverIntent {
    pub leader: Option<CompactString>,
    pub candidate: Option<CompactString>,
    pub scheduled_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryEntry {
    pub timeline: u64,
    pub lsn: WalLsn,
    pub reason: String,
    pub timestamp: Option<String>,
    pub leader: Option<CompactString>,
}

#[derive(Debug, Clone, Default)]
pub struct ClusterSnapshot {
    pub initialize: Option<Versioned<CompactString>>,
    pub config: Option<Versioned<serde_json::Value>>,
    pub leader: Option<Versioned<CompactString>>,
    pub members: BTreeMap<CompactString, Versioned<MemberData>>,
    pub status: Option<Versioned<ClusterStatus>>,
    pub sync: Option<Versioned<SyncState>>,
    pub failover: Option<Versioned<FailoverIntent>>,
    pub failsafe: Option<BTreeMap<CompactString, String>>,
    pub history: Vec<HistoryEntry>,
}
```

### 3.2 Domain `DcsBackend` Trait & Static `AnyDcs` Dispatch (`crates/negroni-dcs/src/lib.rs`)

```rust
use std::collections::BTreeMap;
use std::time::Duration;
use compact_str::CompactString;
use negroni_types::{
    CasToken, ClusterSnapshot, ClusterStatus, FailoverIntent, HistoryEntry, LeaseHandle,
    MemberData, SyncState,
};
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub enum DcsWatchEvent {
    LeaderChanged,
    ConfigChanged,
    FailoverRequested,
    TopologyChanged,
    WatchDisconnected,
}

#[derive(Debug, thiserror::Error)]
pub enum DcsError {
    #[error("CAS precondition failed (expected {expected:?})")]
    CasConflict { expected: Option<CasToken> },
    #[error("lease expired or invalidated on server")]
    LeaseExpired,
    #[error("DCS unreachable or timed out: {0}")]
    Unreachable(String),
}

#[allow(async_fn_in_trait)]
pub trait DcsBackend: Send + Sync + 'static {
    async fn fetch_cluster(&self, timeout: Duration) -> Result<ClusterSnapshot, DcsError>;
    async fn ensure_lease(&self, ttl: Duration, current: Option<&LeaseHandle>) -> Result<LeaseHandle, DcsError>;
    async fn cas_initialize(&self, sys_id: &str) -> Result<CasToken, DcsError>;
    async fn acquire_or_renew_leader(
        &self,
        self_name: &str,
        lease: &LeaseHandle,
        expected: Option<&CasToken>,
    ) -> Result<CasToken, DcsError>;
    async fn release_leader(&self, self_name: &str, expected: &CasToken) -> Result<(), DcsError>;
    async fn upsert_member(&self, self_name: &str, data: &MemberData, lease: &LeaseHandle) -> Result<(), DcsError>;
    async fn publish_leader_state(
        &self,
        status: &ClusterStatus,
        failsafe_roster: Option<&BTreeMap<CompactString, String>>,
    ) -> Result<(), DcsError>;
    async fn append_history_entry(&self, entry: &HistoryEntry) -> Result<(), DcsError>;
    async fn cas_sync_state(&self, next: &SyncState, expected: Option<&CasToken>) -> Result<CasToken, DcsError>;
    async fn cas_config(&self, config_json: &serde_json::Value, expected: Option<&CasToken>) -> Result<CasToken, DcsError>;
    async fn cas_failover(&self, intent: &FailoverIntent, expected: Option<&CasToken>) -> Result<CasToken, DcsError>;
    async fn clear_failover(&self, expected: &CasToken) -> Result<(), DcsError>;
    fn subscribe_watches(&self) -> mpsc::Receiver<DcsWatchEvent>;
}
```

**Backend Implementation Rules:**
- `etcd3`: Uses single-RTT `etcd_client::Txn` (`Compare::version(key) == 0` for create-if-absent; `Compare::mod_revision(key) == rev` for CAS updates) and background gRPC lease keepalive stream.
- `kube`: Watches `Endpoints` (or `coordination.k8s.io/v1::Lease` when `use_endpoints: false`) + `<scope>-config` `ConfigMap` filtered by configured `labels`; uses optimistic `metadata.resourceVersion` on `Replace`/`Patch` and synchronizes Pod target subsets on `Endpoints` when primary changes.
- `consul`: Creates session with `Behavior = delete`, `TTL = ttl`, `LockDelay = 1ms`; uses `?acquire=<session>` on `/leader` and `/members/<name>`, `?cas=<ModifyIndex>` on `/config` and `/sync`, and `?index=<X>&wait=10s` blocking queries for watches.
- `zookeeper`: Maps `/leader` and `/members/<name>` to `CreateMode::Ephemeral` znodes and persistent keys to `set_data` with `expected_version: Some(i32)`.
- `raft`: Embeds `openraft` consensus over configured peer TCP/TLS endpoints with local log/state-machine persistence inside `raft.data_dir`.

---

## 4. `negroni-ha` & `negroni-watchdog` — Cancel-Safe State Machine

### 4.1 Pure Deterministic Reducer (`crates/negroni-ha/src/reducer.rs`)

**Epoch Fencing Invariant:** `NodeCtx` separates two monotonic counters so routine ticks never invalidate multi-minute background jobs or in-flight demotions:
- `rpc_epoch: u64` — incremented on every per-tick DCS/peer RPC (`FetchDcs`, `RenewOrAcquireLeader`, `ProbeElectionPeers`, `ProbeFailsafePeers`) to discard late network responses.
- `state_epoch: u64` — incremented *only* when entering/leaving a lifecycle state (`Bootstrapping`, `Cloning`, `ElectionCandidate`, `ArmingPrimary`, `Rewinding`, `Demoting`) or cancelling jobs.

```rust
use std::collections::BTreeMap;
use std::time::{Duration, Instant};
use compact_str::CompactString;
use negroni_types::{
    CasToken, ClusterSnapshot, ClusterStatus, LeaseHandle, LeaseProof, SyncState, WalLsn,
};
use smallvec::SmallVec;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HaRoleState {
    Unbootstrapped,
    Bootstrapping { state_epoch: u64 },
    Cloning { leader_conn: String, state_epoch: u64 },
    ElectionCandidate { state_epoch: u64, peers_verified: bool },
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
    Replica { following: Option<CompactString> },
    StandbyLeader {
        leader_token: CasToken,
        lease: LeaseHandle,
        lease_valid_until: Instant,
    },
    Rewinding { target_leader_conn: String, state_epoch: u64 },
    Demoting { reason: DemoteReason, deadline: Instant, state_epoch: u64 },
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
    pub state: CompactString,
    pub role: CompactString,
    pub is_read_write_valid: bool,
    pub is_sync_standby: bool,
    pub is_quorum_standby: bool,
    pub noloadbalance: bool,
    pub replication_lag_bytes: u64,
    pub timeline: u64,
    pub pending_restart: bool,
    pub pause: bool,
    pub last_ha_tick: Instant,
    pub user_tags: BTreeMap<CompactString, CompactString>,
}

#[derive(Debug, Clone, Default)]
pub struct LocalPgSummary {
    pub has_valid_pgdata: bool,
    pub state_label: CompactString,
    pub in_recovery: bool,
    pub lsn: WalLsn,
    pub timeline: u64,
    pub lag_bytes: u64,
    pub is_sync_standby: bool,
    pub is_quorum_standby: bool,
    pub pending_restart: bool,
    pub needs_rewind: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobKind {
    InitDb,
    CustomBootstrap,
    BaseBackup,
    PgRewind,
}

#[derive(Debug)]
pub enum ControlCommand {
    Reload,
    Switchover { candidate: Option<CompactString> },
    Failover { candidate: Option<CompactString> },
    Pause,
    Resume,
    Reinitialize { force: bool },
}

#[derive(Debug)]
pub enum HaEvent {
    Tick { now: Instant },
    WatchWakeup { now: Instant },
    DcsFetched {
        rpc_epoch: u64,
        attempt_started_at: Instant,
        now: Instant,
        result: Result<ClusterSnapshot, String>,
    },
    ElectionPeersProbed {
        rpc_epoch: u64,
        now: Instant,
        max_peer_lsn: WalLsn,
    },
    LeaderLeaseResult {
        rpc_epoch: u64,
        attempt_started_at: Instant,
        now: Instant,
        result: Result<(LeaseHandle, CasToken), String>,
    },
    WatchdogArmed { state_epoch: u64, now: Instant },
    WatchdogArmFailed { state_epoch: u64, now: Instant },
    FailsafeQuorumResult {
        rpc_epoch: u64,
        attempt_started_at: Instant,
        now: Instant,
        all_peers_accepted: bool,
    },
    PgObservation { now: Instant, telemetry: LocalPgSummary },
    PgPromotionCompleted { state_epoch: u64, now: Instant, success: bool },
    PgDemotionCompleted { state_epoch: u64, now: Instant },
    BackgroundJobFinished { state_epoch: u64, now: Instant, job: JobKind, success: bool },
    OperatorCommand { now: Instant, cmd: ControlCommand },
}

#[derive(Debug)]
pub enum Effect {
    ArmAndFeedWatchdog { state_epoch: u64, proof: LeaseProof },
    FeedWatchdog(LeaseProof),
    DisarmWatchdog,
    FetchDcs { rpc_epoch: u64, timeout: Duration },
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
    ReleaseLeader { expected: CasToken },
    PublishLeaderStatus { status: ClusterStatus },
    UpdateSyncState { next: SyncState, expected: Option<CasToken> },
    ProbeFailsafePeers {
        rpc_epoch: u64,
        peers: Vec<(CompactString, String)>,
        timeout: Duration,
    },
    PromotePostgres { state_epoch: u64 },
    DemotePostgres { state_epoch: u64, immediate_at: Instant },
    ReconfigureStandby { primary_conninfo: String, slot_name: Option<CompactString> },
    SyncSlots { status: ClusterStatus, is_primary: bool, is_demoted_rejoin: bool },
    SpawnJob { state_epoch: u64, job: JobKind, target_conn: Option<String> },
    CancelActiveJobs,
    NotifySystemd(&'static str),
}

pub struct NodeCtx {
    pub self_name: CompactString,
    pub state: HaRoleState,
    pub rpc_epoch: u64,
    pub state_epoch: u64,
    pub ttl: Duration,
    pub loop_wait: Duration,
    pub retry_timeout: Duration,
    pub watchdog_safety_margin: Duration,
    pub maximum_lag_on_failover: u64,
    pub synchronous_mode: bool,
    pub failsafe_enabled: bool,
    pub nofailover_tag: bool,
    pub noloadbalance_tag: bool,
    pub user_tags: BTreeMap<CompactString, CompactString>,
    pub last_pg: LocalPgSummary,
    pub last_tick_at: Instant,
    pub last_failsafe_roster: Vec<(CompactString, String)>,
}

impl NodeCtx {
    /// Pure, synchronous, zero-I/O state reducer.
    pub fn step(&mut self, event: HaEvent) -> (SmallVec<[Effect; 6]>, PublishedNodeState) {
        let mut effects = SmallVec::new();
        let event_now = match &event {
            HaEvent::Tick { now }
            | HaEvent::WatchWakeup { now }
            | HaEvent::DcsFetched { now, .. }
            | HaEvent::ElectionPeersProbed { now, .. }
            | HaEvent::LeaderLeaseResult { now, .. }
            | HaEvent::WatchdogArmed { now, .. }
            | HaEvent::WatchdogArmFailed { now, .. }
            | HaEvent::FailsafeQuorumResult { now, .. }
            | HaEvent::PgObservation { now, .. }
            | HaEvent::PgPromotionCompleted { now, .. }
            | HaEvent::PgDemotionCompleted { now, .. }
            | HaEvent::BackgroundJobFinished { now, .. }
            | HaEvent::OperatorCommand { now, .. } => *now,
        };

        match event {
            HaEvent::Tick { now } | HaEvent::WatchWakeup { now } => {
                self.last_tick_at = now;
                // Only `rpc_epoch` advances on ticks; long-running `state_epoch` jobs continue uninterrupted.
                self.rpc_epoch += 1;
                effects.push(Effect::FetchDcs {
                    rpc_epoch: self.rpc_epoch,
                    timeout: self.retry_timeout,
                });
            }

            HaEvent::PgObservation { telemetry, .. } => {
                self.last_pg = telemetry;
            }

            HaEvent::DcsFetched { rpc_epoch, now, result, .. } => {
                if rpc_epoch < self.rpc_epoch {
                    return (effects, self.publish_snapshot(now));
                }
                if let Ok(ref snap) = result
                    && let Some(ref fs_map) = snap.failsafe
                {
                    self.last_failsafe_roster = fs_map
                        .iter()
                        .filter(|(m, _)| *m != &self.self_name)
                        .map(|(m, url)| (m.clone(), url.clone()))
                        .collect();
                }

                match (&self.state, result) {
                    // 1. Bootstrap or Clone when node is uninitialized
                    (HaRoleState::Unbootstrapped, Ok(snap)) if !self.last_pg.has_valid_pgdata => {
                        self.state_epoch += 1;
                        let s_epoch = self.state_epoch;
                        if snap.initialize.is_none() && snap.leader.is_none() && !self.nofailover_tag {
                            self.state = HaRoleState::Bootstrapping { state_epoch: s_epoch };
                            effects.push(Effect::SpawnJob {
                                state_epoch: s_epoch,
                                job: JobKind::InitDb,
                                target_conn: None,
                            });
                        } else if let Some(source_conn) = Self::select_clone_source(&snap) {
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

                    // 2. Primary lost DCS connectivity -> Failsafe quorum probe or immediate demote
                    (HaRoleState::Primary { lease_valid_until, .. }, Err(_)) => {
                        let valid_until = *lease_valid_until;
                        if self.failsafe_enabled && !self.last_failsafe_roster.is_empty() && now < valid_until {
                            self.state_epoch += 1;
                            self.rpc_epoch += 1;
                            self.state = HaRoleState::FailsafePrimary {
                                lease_valid_until: valid_until,
                            };
                            effects.push(Effect::ProbeFailsafePeers {
                                rpc_epoch: self.rpc_epoch,
                                peers: self.last_failsafe_roster.clone(),
                                timeout: self.retry_timeout,
                            });
                        } else {
                            self.enter_demoting(DemoteReason::LeaseLost, valid_until, &mut effects);
                        }
                    }

                    // 3. FailsafePrimary while DCS remains unreachable -> renew peer quorum every tick
                    (HaRoleState::FailsafePrimary { lease_valid_until }, Err(_)) => {
                        let valid_until = *lease_valid_until;
                        if now >= valid_until || self.last_failsafe_roster.is_empty() {
                            self.enter_demoting(DemoteReason::LeaseLost, valid_until, &mut effects);
                        } else {
                            self.rpc_epoch += 1;
                            effects.push(Effect::ProbeFailsafePeers {
                                rpc_epoch: self.rpc_epoch,
                                peers: self.last_failsafe_roster.clone(),
                                timeout: self.retry_timeout,
                            });
                        }
                    }

                    // 4. FailsafePrimary sees DCS recover -> reclaim `/leader` or demote on split-brain conflict
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
                                self.enter_demoting(DemoteReason::LeaseLost, valid_until, &mut effects);
                            }
                        }
                    }

                    // 5. Primary sees DCS -> verify ownership & renew lease
                    (HaRoleState::Primary { leader_token, lease, .. }, Ok(snap)) => {
                        if snap.leader.as_ref().map(|l| &l.value) != Some(&self.self_name) {
                            self.enter_demoting(DemoteReason::LeaseLost, now, &mut effects);
                        } else {
                            self.rpc_epoch += 1;
                            effects.push(Effect::RenewOrAcquireLeader {
                                rpc_epoch: self.rpc_epoch,
                                lease: Some(lease.clone()),
                                expected: Some(leader_token.clone()),
                                ttl: self.ttl,
                            });
                        }
                    }

                    // 6. Paused Primary keeps `/leader` lock alive when DCS is reachable without auto-demoting
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

                    // 7. Replica sees timeline divergence -> run pg_rewind before streaming
                    (HaRoleState::Replica { .. }, Ok(snap)) if self.last_pg.needs_rewind => {
                        if let Some(leader_conn) = Self::leader_conn_url(&snap) {
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
                    }

                    // 8. Healthy Replica sees missing `/leader` -> poll peer LSNs (`is_healthiest_node`) before CAS
                    (HaRoleState::Replica { .. }, Ok(snap)) if snap.leader.is_none() => {
                        if self.is_eligible_for_election(&snap) {
                            let peers = self.surviving_election_peers(&snap);
                            self.state_epoch += 1;
                            self.rpc_epoch += 1;
                            if peers.is_empty() {
                                self.state = HaRoleState::ElectionCandidate {
                                    state_epoch: self.state_epoch,
                                    peers_verified: true,
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
                                };
                                effects.push(Effect::ProbeElectionPeers {
                                    rpc_epoch: self.rpc_epoch,
                                    peers,
                                    timeout: self.retry_timeout,
                                });
                            }
                        }
                    }

                    // 9. Replica follows active `/leader` & syncs slots
                    (HaRoleState::Replica { following }, Ok(snap)) => {
                        if let Some(leader) = &snap.leader {
                            if following.as_ref() != Some(&leader.value)
                                && let Some(conn) = Self::leader_conn_url(&snap)
                            {
                                self.state = HaRoleState::Replica {
                                    following: Some(leader.value.clone()),
                                };
                                effects.push(Effect::ReconfigureStandby {
                                    primary_conninfo: conn,
                                    slot_name: Some(self.self_name.clone()),
                                });
                            }
                            if let Some(status) = &snap.status {
                                effects.push(Effect::SyncSlots {
                                    status: status.value.clone(),
                                    is_primary: false,
                                    is_demoted_rejoin: false,
                                });
                            }
                        }
                    }
                    _ => {}
                }
            }

            HaEvent::ElectionPeersProbed { rpc_epoch, max_peer_lsn, .. } => {
                if rpc_epoch == self.rpc_epoch
                    && let HaRoleState::ElectionCandidate {
                        state_epoch,
                        peers_verified: false,
                    } = self.state
                {
                    if self.last_pg.lsn >= max_peer_lsn {
                        self.state = HaRoleState::ElectionCandidate {
                            state_epoch,
                            peers_verified: true,
                        };
                        self.rpc_epoch += 1;
                        effects.push(Effect::RenewOrAcquireLeader {
                            rpc_epoch: self.rpc_epoch,
                            lease: None,
                            expected: None,
                            ttl: self.ttl,
                        });
                    } else {
                        // Yield election to the surviving peer with strictly higher WAL LSN.
                        self.state = HaRoleState::Replica { following: None };
                    }
                }
            }

            HaEvent::LeaderLeaseResult { rpc_epoch, attempt_started_at, now, result } => {
                if rpc_epoch == self.rpc_epoch {
                    let valid_until = attempt_started_at + self.ttl - self.watchdog_safety_margin;
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
                                held_leader: Some((lease, token)),
                            };
                        }
                        (HaRoleState::Paused { .. }, Err(_)) => {}

                        // Candidate won `/leader`: arm `/dev/watchdog` BEFORE promoting PostgreSQL!
                        (HaRoleState::ElectionCandidate { .. }, Ok((lease, token))) => {
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

                        // Existing Primary or recovering FailsafePrimary renewed `/leader`
                        (
                            HaRoleState::Primary { .. } | HaRoleState::FailsafePrimary { .. },
                            Ok((lease, token)),
                        ) => {
                            self.state_epoch += 1;
                            self.state = HaRoleState::Primary {
                                leader_token: token,
                                lease,
                                lease_valid_until: valid_until,
                            };
                            effects.push(Effect::FeedWatchdog(LeaseProof {
                                epoch: self.state_epoch,
                                valid_until,
                            }));
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
                        lease: lse,
                        lease_valid_until,
                    };
                    if self.last_pg.in_recovery {
                        effects.push(Effect::PromotePostgres { state_epoch });
                    }
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

            HaEvent::FailsafeQuorumResult { rpc_epoch, attempt_started_at, all_peers_accepted, .. } => {
                if rpc_epoch == self.rpc_epoch
                    && let HaRoleState::FailsafePrimary { lease_valid_until } = self.state
                {
                    if all_peers_accepted {
                        self.state_epoch += 1;
                        let extended = attempt_started_at + self.ttl - self.watchdog_safety_margin;
                        self.state = HaRoleState::FailsafePrimary {
                            lease_valid_until: extended,
                        };
                        effects.push(Effect::FeedWatchdog(LeaseProof {
                            epoch: self.state_epoch,
                            valid_until: extended,
                        }));
                    } else {
                        self.enter_demoting(DemoteReason::FailsafeRejected, lease_valid_until, &mut effects);
                    }
                }
            }

            HaEvent::PgDemotionCompleted { state_epoch, .. } => {
                if matches!(self.state, HaRoleState::Demoting { state_epoch: e, .. } if e == state_epoch) {
                    // Hardware watchdog disarmed ONLY after Postgres is shut down / read-only
                    effects.push(Effect::DisarmWatchdog);
                    effects.push(Effect::SyncSlots {
                        status: ClusterStatus::default(),
                        is_primary: false,
                        is_demoted_rejoin: true,
                    });
                    self.state = HaRoleState::Replica { following: None };
                }
            }

            HaEvent::BackgroundJobFinished { state_epoch, job, success, .. } => {
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
                            self.state_epoch += 1;
                            self.rpc_epoch += 1;
                            self.state = HaRoleState::ElectionCandidate {
                                state_epoch: self.state_epoch,
                                peers_verified: true,
                            };
                            effects.push(Effect::RenewOrAcquireLeader {
                                rpc_epoch: self.rpc_epoch,
                                lease: None,
                                expected: None,
                                ttl: self.ttl,
                            });
                        }
                        (JobKind::BaseBackup | JobKind::PgRewind, true) => {
                            self.state = HaRoleState::Replica { following: None };
                        }
                        _ => {
                            self.state = HaRoleState::Unbootstrapped;
                        }
                    }
                }
            }

            HaEvent::OperatorCommand { cmd: ControlCommand::Pause, .. } => {
                let (underlying, held_leader) = match &self.state {
                    HaRoleState::Primary { leader_token, lease, .. }
                    | HaRoleState::ArmingPrimary { leader_token, lease, .. } => {
                        (PausedRole::Primary, Some((lease.clone(), leader_token.clone())))
                    }
                    HaRoleState::FailsafePrimary { .. } => (PausedRole::Primary, None),
                    HaRoleState::Replica { .. } => (PausedRole::Replica, None),
                    _ => (PausedRole::Offline, None),
                };
                effects.push(Effect::DisarmWatchdog);
                self.state = HaRoleState::Paused {
                    underlying_role: underlying,
                    held_leader,
                };
            }

            HaEvent::OperatorCommand { cmd: ControlCommand::Resume, .. } => {
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
            _ => {}
        }

        let snapshot = self.publish_snapshot(event_now);
        (effects, snapshot)
    }

    fn enter_demoting(&mut self, reason: DemoteReason, deadline: Instant, effects: &mut SmallVec<[Effect; 6]>) {
        self.state_epoch += 1;
        let immediate_at = deadline
            .checked_sub(Duration::from_secs(2))
            .unwrap_or(deadline);
        self.state = HaRoleState::Demoting {
            reason,
            deadline,
            state_epoch: self.state_epoch,
        };
        effects.push(Effect::CancelActiveJobs);
        effects.push(Effect::DemotePostgres {
            state_epoch: self.state_epoch,
            immediate_at,
        });
    }

    fn is_eligible_for_election(&self, snap: &ClusterSnapshot) -> bool {
        if self.nofailover_tag || self.last_pg.lag_bytes > self.maximum_lag_on_failover {
            return false;
        }
        if let Some(status) = &snap.status {
            let dcs_lag = status.value.optime.0.saturating_sub(self.last_pg.lsn.0);
            if dcs_lag > self.maximum_lag_on_failover {
                return false;
            }
        }
        if self.synchronous_mode
            && let Some(sync) = &snap.sync
        {
            let allowed = sync
                .value
                .sync_standby
                .as_deref()
                .is_some_and(|csv| csv.split(',').any(|m| m.trim() == self.self_name.as_str()));
            if !allowed {
                return false;
            }
        }
        true
    }

    fn surviving_election_peers(&self, snap: &ClusterSnapshot) -> Vec<(CompactString, String)> {
        snap.members
            .iter()
            .filter(|(name, _)| *name != &self.self_name)
            .map(|(name, m)| (name.clone(), m.value.api_url.clone()))
            .collect()
    }

    fn leader_conn_url(snap: &ClusterSnapshot) -> Option<String> {
        let leader_name = &snap.leader.as_ref()?.value;
        Some(snap.members.get(leader_name)?.value.conn_url.clone())
    }

    fn select_clone_source(snap: &ClusterSnapshot) -> Option<String> {
        for member in snap.members.values() {
            let clonefrom = member
                .value
                .tags
                .get("clonefrom")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            if clonefrom && member.value.state == "running" {
                return Some(member.value.conn_url.clone());
            }
        }
        Self::leader_conn_url(snap)
    }

    fn publish_snapshot(&self, now: Instant) -> PublishedNodeState {
        let (role, is_rw) = match &self.state {
            HaRoleState::Primary { lease_valid_until, .. }
            | HaRoleState::FailsafePrimary { lease_valid_until } => {
                ("primary".into(), now < *lease_valid_until)
            }
            HaRoleState::Paused {
                underlying_role: PausedRole::Primary,
                ..
            } => ("primary".into(), true),
            HaRoleState::StandbyLeader { .. } => ("standby_leader".into(), false),
            HaRoleState::Replica { .. }
            | HaRoleState::Paused {
                underlying_role: PausedRole::Replica,
                ..
            } => ("replica".into(), false),
            _ => ("uninitialized".into(), false),
        };

        PublishedNodeState {
            state: self.last_pg.state_label.clone(),
            role,
            is_read_write_valid: is_rw,
            is_sync_standby: self.last_pg.is_sync_standby,
            is_quorum_standby: self.last_pg.is_quorum_standby,
            noloadbalance: self.noloadbalance_tag,
            replication_lag_bytes: self.last_pg.lag_bytes,
            timeline: self.last_pg.timeline,
            pending_restart: self.last_pg.pending_restart,
            pause: matches!(self.state, HaRoleState::Paused { .. }),
            last_ha_tick: self.last_tick_at,
            user_tags: self.user_tags.clone(),
        }
    }
}
```

### 4.2 Cancel-Safe `CoordinatorActor` & OS Watchdog Thread

```rust
// crates/negroni-ha/src/actor.rs
pub async fn coordinator_loop(
    mut ctx: crate::reducer::NodeCtx,
    mut event_rx: tokio::sync::mpsc::Receiver<crate::reducer::HaEvent>,
    mut watch_rx: tokio::sync::mpsc::Receiver<negroni_dcs::DcsWatchEvent>,
    published_state: std::sync::Arc<arc_swap::ArcSwap<crate::reducer::PublishedNodeState>>,
    dispatcher: EffectDispatcher,
) {
    let mut ticker = tokio::time::interval(ctx.loop_wait);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        // Strictly cancel-safe: zero network/process futures inside `select!`
        let ev = tokio::select! {
            biased;
            Some(ha_ev) = event_rx.recv() => ha_ev,
            Some(_watch_ev) = watch_rx.recv() => crate::reducer::HaEvent::WatchWakeup {
                now: std::time::Instant::now(),
            },
            _ = ticker.tick() => crate::reducer::HaEvent::Tick {
                now: std::time::Instant::now(),
            },
            else => break,
        };

        let (effects, snapshot) = ctx.step(ev);
        published_state.store(std::sync::Arc::new(snapshot));

        for effect in effects {
            dispatcher.dispatch(effect);
        }
    }
}
```

**Hardware Watchdog Isolation & Kernel Granularity Readback (`crates/negroni-watchdog`):**
- `ioctl_linux.rs` carries the workspace's sole `#![allow(unsafe_code)]` attribute, wrapping `WDIOC_SETTIMEOUT` (`rustix::ioctl::Updater`) and `WDIOC_KEEPALIVE` (`rustix::ioctl::Getter`) via architecture-portable `rustix::ioctl::opcode` helpers.
- Because hardware drivers may round `WDIOC_SETTIMEOUT` to coarse timer steps (e.g., snapping `25s` -> `32s` or `16s`), `validate_and_arm` inspects the kernel-returned `actual_timeout` and verifies `loop_wait + retry_timeout + 2s <= actual_timeout < ttl`. If the driver rounds outside that window, `validate_and_arm` immediately writes magic `'V'` (`b"V"`) before closing `/dev/watchdog` (preventing an unintended reboot) and returns `Err(WatchdogArmError::InvalidKernelGranularity)`—aborting promotion when `WatchdogMode::Required` is active while degrading safely to software-only when `WatchdogMode::Automatic` is configured.
- `thread.rs` executes on the pre-Tokio `negroni-wd` `std::thread` with **zero `tracing` / logging sink dependencies** so stalled `stderr`/journald pipes can never block hardware keepalives.

---

## 5. `negroni-pg` — PostgreSQL 15–18 Supervisor & Dual-Mode Slot Manager

### 5.1 Static Version Capabilities (`negroni-types`) & Prometheus Process State (`crates/negroni-pg`)

`PgMajor` (`V15..=V18`), `PgCapabilities`, and `UnsupportedPgVersion` are defined in `negroni-types` so both `negroni-config` and `negroni-pg` share the capability matrix without upward dependencies, while `PgProcessState` lives in `negroni-pg`:

```rust
use negroni_types::{PgCapabilities, PgMajor, UnsupportedPgVersion};

/// Exact integer mapping for Prometheus `patroni_postgres_state` (0..=14)
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgProcessState {
    InitDb = 0,
    InitDbFailed = 1,
    CustomBootstrap = 2,
    CustomBootstrapFailed = 3,
    CreatingReplica = 4,
    Running = 5,
    Starting = 6,
    BootstrapStarting = 7,
    StartFailed = 8,
    Restarting = 9,
    RestartFailed = 10,
    Stopping = 11,
    Stopped = 12,
    StopFailed = 13,
    Crashed = 14,
}
```

### 5.2 `pidfd` Process Adoption & 1-RTT Telemetry SQL
- **Zero-Outage Daemon Upgrades:** When `negroni` restarts while PostgreSQL is running, `PgSupervisorActor` parses `$PGDATA/postmaster.pid`, verifies `/proc/<pid>/stat` start time, and opens a kernel `PidFd` via `rustix::process::pidfd_open` wrapped in `tokio::io::unix::AsyncFd` (falling back to `kill(pid, 0)` every `500ms` on non-Linux POSIX targets).
- **Single-Connection Telemetry Query (`SqlControlActor`):**

```sql
SELECT
    pg_catalog.current_setting('server_version_num')::int4                AS version_num,
    pg_catalog.pg_is_in_recovery()                                        AS in_recovery,
    CASE WHEN pg_catalog.pg_is_in_recovery()
         THEN COALESCE(pg_catalog.pg_last_wal_receive_lsn(), '0/0'::pg_lsn) - '0/0'::pg_lsn
         ELSE pg_catalog.pg_current_wal_lsn() - '0/0'::pg_lsn
    END::int8                                                             AS primary_or_recv_lsn,
    CASE WHEN pg_catalog.pg_is_in_recovery()
         THEN (COALESCE(pg_catalog.pg_last_wal_replay_lsn(), '0/0'::pg_lsn) - '0/0'::pg_lsn)::int8
         ELSE NULL
    END                                                                   AS replay_lsn,
    CASE WHEN pg_catalog.pg_is_in_recovery()
         THEN pg_catalog.pg_is_wal_replay_paused()
         ELSE false
    END                                                                   AS wal_paused,
    (SELECT timeline_id FROM pg_catalog.pg_control_checkpoint())::int8    AS timeline_id,
    EXISTS (
        SELECT 1 FROM pg_catalog.pg_stat_wal_receiver WHERE status = 'streaming'
    )                                                                     AS is_streaming,
    EXISTS (
        SELECT 1 FROM pg_catalog.pg_settings WHERE pending_restart
    )                                                                     AS pending_restart,
    pg_catalog.current_setting('data_checksums') = 'on'                   AS data_checksums,
    pg_catalog.current_setting('wal_log_hints') = 'on'                    AS wal_log_hints;
```

### 5.3 Config Preservation, Unit Canonicalizer & HBA Validator
- **Baseline Preservation:** If `$PGDATA/postgresql.base.conf` is absent when adopting an existing instance, rename `postgresql.conf` -> `postgresql.base.conf` and emit `include 'postgresql.base.conf'` at line 1 of Negroni-managed `postgresql.conf`.
- **Unit Canonicalizer:** Converts PostgreSQL memory strings (`B`, `8kB`, `kB`, `MB`, `GB`, `TB`) to `u64` bytes and duration strings (`us`, `ms`, `s`, `min`, `h`, `d`) to `u64` microseconds before comparing desired GUCs against `pg_settings` so equivalent forms (`1024MB` vs `1GB`) never trigger redundant `SIGHUP` reloads or false `pending_restart` flags.
- **`pg_hba.conf` & `pg_ident.conf` Validation:**
  - `PgMajor::V15`: Rejects `include`, `include_if_exists`, `include_dir`, `/regex/`, and `oauth`.
  - `PgMajor::V16 | PgMajor::V17`: Allows `include*` and `/regex/`; rejects `oauth`.
  - `PgMajor::V18`: Allows `include*`, `/regex/`, and `oauth` (verifying required `issuer` and `scope` key-value options).

### 5.4 Dual-Mode Replication Slot Reconciliation (`crates/negroni-pg/src/slots.rs`)

```rust
use std::collections::{BTreeMap, BTreeSet};
use compact_str::CompactString;
use negroni_types::WalLsn;
use crate::version::PgCapabilities;

#[derive(Debug, Clone)]
pub struct ObservedSlot {
    pub slot_name: CompactString,
    pub is_logical: bool,
    pub database: Option<CompactString>,
    pub plugin: Option<CompactString>,
    pub restart_lsn: Option<WalLsn>,
    pub confirmed_flush_lsn: Option<WalLsn>,
    pub active: bool,
    pub conflicting: bool,                     // PG 16+
    pub failover: bool,                        // PG 17+ (false on PG 15-16)
    pub synced: bool,                          // PG 17+ (true on standby when managed by slotsync)
    pub slotsync_skip_reason: Option<CompactString>, // PG 18+
}

#[derive(Debug, Clone)]
pub struct PermanentLogicalSlotTarget {
    pub database: CompactString,
    pub plugin: CompactString,
    pub confirmed_lsn: WalLsn,
}

#[derive(Debug, Clone, Default)]
pub struct DesiredSlotsSpec {
    pub physical_members: BTreeSet<CompactString>,
    pub permanent_logical: BTreeMap<CompactString, PermanentLogicalSlotTarget>,
}

#[derive(Debug, Default)]
pub struct SlotReconcilePlan {
    pub drop_slots: Vec<CompactString>,
    pub create_physical: Vec<CompactString>,
    pub create_logical: Vec<(CompactString, CompactString, CompactString)>,
    pub advance_logical: Vec<(CompactString, CompactString, WalLsn)>,
}

pub fn plan_slot_reconciliation(
    caps: PgCapabilities,
    is_primary: bool,
    is_demoted_rejoin: bool,
    observed: &BTreeMap<CompactString, ObservedSlot>,
    desired: &DesiredSlotsSpec,
) -> SlotReconcilePlan {
    let mut plan = SlotReconcilePlan::default();

    for (name, slot) in observed {
        // 1. PG 17-18 Rejoin Guard: When a former primary is demoted/rewound and rejoins as a standby,
        //    any pre-existing `failover = true` logical slots have `synced = false` on disk, causing
        //    PostgreSQL's native `slotsync` worker to abort with duplicate-slot errors unless dropped first.
        if caps.native_failover_slots && !is_primary && is_demoted_rejoin && slot.is_logical && slot.failover && !slot.synced {
            plan.drop_slots.push(name.clone());
            continue;
        }

        // 2. Never manually advance or drop active PG 17-18 `failover = true` slots; `slotsync` owns them.
        if caps.native_failover_slots && slot.is_logical && slot.failover {
            continue;
        }

        // 3. Drop invalidated/conflicting standby logical slots so they can be cleanly recreated.
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

    for phys in &desired.physical_members {
        if !observed.contains_key(phys) {
            plan.create_physical.push(phys.clone());
        }
    }

    for (name, target) in &desired.permanent_logical {
        match observed.get(name) {
            None if is_primary || caps.standby_logical_slot_create => {
                plan.create_logical.push((
                    target.database.clone(),
                    name.clone(),
                    target.plugin.clone(),
                ));
            }
            Some(existing) if !is_primary && !existing.failover => {
                if existing.confirmed_flush_lsn.unwrap_or_default() < target.confirmed_lsn {
                    plan.advance_logical.push((
                        target.database.clone(),
                        name.clone(),
                        target.confirmed_lsn,
                    ));
                }
            }
            _ => {}
        }
    }

    plan
}
```

### 5.5 Citus Coordinator/Worker Integration (`#[cfg(feature = "citus")]`)
When `citus: { group: <u32>, database: "<db>" }` is configured:
- Worker clusters (`group > 0`) report their `group` tag in `MemberData` and notify the Coordinator REST/DCS view on primary transitions.
- Coordinator primary (`group == 0`) reconciles `pg_dist_node` inside serialized transactions (`SELECT pg_catalog.pg_advisory_xact_lock(29051983)` -> `citus_update_node` / `citus_add_node`), automatically maintaining secondary read-scaling entries unless `tags.noloadbalance == true`.

---

## 6. `negroni-api` — Lock-Free Probes, Control RPCs & Metrics

### 6.1 Health & Monitoring Endpoints (`GET`, `HEAD`, `OPTIONS`)
Served directly from `ArcSwap<PublishedNodeState>`:

| Endpoint(s) | `200 OK` Condition (`503 Service Unavailable` Otherwise) | Query Parameters |
| :--- | :--- | :--- |
| `/`, `/primary`, `/read-write` | `role == "primary"` AND `state == "running"` AND `is_read_write_valid` | None (tags ignored) |
| `/leader` | Valid lease/failsafe as `"primary"` OR `"standby_leader"` | None |
| `/standby-leader`, `/standby_leader` | Valid lease as `"standby_leader"` AND `state == "running"` | None |
| `/replica` | `role == "replica"` AND `state == "running"` AND `!noloadbalance` | `?lag=<bytes\|16MB\|1GB>` & `?tag_<k>=<v>` |
| `/read-only` | Passes `/replica`, OR healthy leader with `!noloadbalance` | `?lag=...` & `?tag_<k>=<v>` |
| `/synchronous`, `/sync` | Passes `/replica` AND `is_sync_standby` | `?lag=...` & `?tag_<k>=<v>` |
| `/read-only-sync` | Healthy `primary` OR passes `/synchronous` | `?tag_<k>=<v>` |
| `/quorum` | Passes `/replica` AND `is_quorum_standby` | `?lag=...` & `?tag_<k>=<v>` |
| `/read-only-quorum` | Healthy `primary` OR passes `/quorum` | `?tag_<k>=<v>` |
| `/asynchronous`, `/async` | Passes `/replica` AND `!is_sync_standby` AND `!is_quorum_standby` | `?lag=...` & `?tag_<k>=<v>` |
| `/health` | `state == "running"` regardless of role/lease | None |
| `/liveness` | `pause == true` OR `now - last_ha_tick < ttl` (`2 * ttl` on replica) | None |
| `/readiness` | Holds `/leader` OR (`state == "running"` AND lag within `?lag`) | `?lag=...&mode=apply\|write` |
| `/patroni`, `/cluster`, `/config`, `/history`, `/metrics` | Always `200 OK` (JSON schemas or Prometheus `0.0.4` text format) | Optional `?scope=...` |

### 6.2 Guarded Control & Failsafe Routes
Protected by `AuthAndAllowlistLayer` (`401` on Basic Auth mismatch via `subtle::ConstantTimeEq`; `403` on CIDR + dynamic peer IP allowlist or mTLS client-cert check):

| Method & Route | Request JSON | Status Codes & Exact Semantics |
| :--- | :--- | :--- |
| `PATCH /config` / `PUT /config` | Merge patch (`PATCH`) or full JSON (`PUT`) | `200` updated JSON; `409` / `503` on DCS CAS conflict. |
| `POST /switchover` | `{"leader": "...", "candidate": "...", "scheduled_at": "..."}` | `200` completed/scheduled; `400` missing TZ; `412` unhealthy candidate; `422` invalid schedule; `503` failed. |
| `POST /failover` | `{"leader": "...", "candidate": "..."}` | Emergency promotion (`200` / `503`). |
| `POST /restart` / `DELETE /restart` | `{"schedule": "...", "role": "...", "postgres_version": "...", "restart_pending": true, "timeout": 60}` | `200` restarted; `202` scheduled; `204` cleared; `409` conflicting schedule; `503` failed. |
| `POST /reload` | Empty | Reloads `patroni.yml`, TLS certs, and Postgres config (`200` / `202`). |
| `POST /reinitialize` | `{"force": true, "from-leader": false}` | Wipes replica `$PGDATA` and re-clones (`200`; `503` if called on leader). |
| `POST /failsafe` | `{"name": "<leader>", "conn_url": "...", "api_url": "...", "slots": {...}, "xlog_location": 12345}` | `502` if failsafe disabled; `200` (`"Accepted"`) if sender is sole primary (updates local slot advance targets); `503` if conflicting leader seen. |

### 6.3 Required Prometheus Metrics (`GET /metrics`)
Must expose exact upstream metric names with `scope="<scope>"` label:
- `patroni_version` (`1`, plus `version` label)
- `patroni_postgres_running` (`0` or `1`)
- `patroni_postgres_state` (`0..=14` from `PgProcessState`)
- `patroni_postmaster_start_time` (Unix epoch seconds)
- `patroni_master`, `patroni_primary`, `patroni_standby_leader`, `patroni_replica`, `patroni_sync_standby`, `patroni_quorum_standby`, `patroni_async_standby` (`0` or `1`)
- `patroni_xlog_location`, `patroni_xlog_received_location`, `patroni_xlog_replayed_location`, `patroni_xlog_replayed_timestamp`, `patroni_xlog_paused`
- `patroni_postgres_timeline`, `patroni_dcs_last_seen`, `patroni_cluster_unlocked`, `patroni_failsafe_mode_is_active`, `patroni_is_paused`, `patroni_pending_restart`

---

## 7. `negroni-cli` — 19-Command `patronictl` / `negronictl` Parity

Config file resolution order: `-c/--config-file` -> `-d/--dcs-url` -> `$NEGRONICTL_CONFIG_FILE` -> `$PATRONICTL_CONFIG_FILE` -> `$XDG_CONFIG_HOME/patroni/patronictl.yaml` -> `/etc/patroni/patroni.yml`.

| Subcommand | Key Flags | Behavior & Parity Contract |
| :--- | :--- | :--- |
| `list` | `-e/--extended`, `-t/--timestamp`, `-W/--watch [SEC]`, `-f plain\|tsv\|json\|yaml\|pretty` | Renders `+ Cluster: <scope> (<sysid>) +` table with `Member`, `Host`, `Role`, `State`, `TL`, `Lag in MB`, `Pending restart`, `Tags`. |
| `topology` | `-W`, `-f` | Builds indented replication cascade tree from member `conn_url` / `primary_conninfo`. |
| `dsn` | `-r primary\|replica\|any`, `-m <member>` | Prints libpq `host=... port=... dbname=...` connection string. |
| `query` | `-r`, `-m`, `-f <sql_file>`, `-c <sql>`, `-U`, `-d`, `--watch` | Executes SQL directly against matching cluster member(s) via `tokio-postgres`. |
| `show-config` | None | Prints current DCS `/config` YAML. |
| `edit-config` | `-s k=v`, `-p guc=v`, `--replace`, `--apply`, `--force`, `-q` | Fetches `/config` + `CasToken`, opens `$EDITOR`, renders `similar` unified diff, prompts `[y/N]`, commits via `cas_config`. |
| `switchover` | `--leader`, `--candidate`, `--scheduled`, `--force` | Interactive TTY prompts or non-interactive `POST /switchover`. |
| `failover` | `--leader`, `--candidate`, `--force` | Interactive TTY prompts or non-interactive `POST /failover`. |
| `pause` / `resume` | `--wait` | Toggles `pause` in `/config` and optionally polls until all members observe state. |
| `restart` | `--role`, `--pg-version`, `--pending`, `--scheduled`, `--timeout`, `--force` | Dispatches rolling or targeted `POST /restart`. |
| `reload` | `--role`, `--force` | Dispatches `POST /reload` to target member(s). |
| `reinit` | `--force`, `--wait` | Dispatches `POST /reinitialize` and optionally polls until replica returns to `running`. |
| `flush` | `restart` \| `switchover` | Cancels scheduled restart (`DELETE /restart`) or scheduled switchover (`clear_failover`). |
| `remove` | `<cluster_name>` | Prompts exact confirmation string (`Please confirm the cluster name to remove: ...` + `You are about to remove all information in DCS...`) before wiping `/<namespace>/<scope>`. |
| `history` | `-f` | Renders timeline switch history table (`TL`, `LSN`, `Reason`, `Timestamp`). |
| `version` | None | Prints client version and optionally cluster member versions. |
| `demote-cluster` / `promote-cluster` | `--dcs-model`, `--force` | Orchestrates clean cross-region Standby Cluster demotion/promotion. |
