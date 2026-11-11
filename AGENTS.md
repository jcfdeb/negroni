# AGENTS.md — Negroni Agentic Engineering Manual

## 1. Project Mission & Identity

**Negroni** is a 1:1 wire-compatible, memory-safe Rust re-implementation of **Patroni 4.1+** scoped strictly to **PostgreSQL 15 through 18** (plus Citus 12+). It compiles into a single static multicall Linux/POSIX binary (`negroni`) that also acts as `negronictl` and `patronictl` via `argv[0]` symlink detection or `negroni ctl <subcommand>`.

Before writing or modifying any code, read these specifications in order:
1. `docs/HLD.md` — System invariants, concurrency architecture, timing equations, DCS wire contracts, and HA state flows.
2. `docs/LLD.md` — Workspace manifests, Rust traits, cancel-safe state reducer, PG 15–18 capabilities, dual-mode slot sync, REST API, and CLI specs.
3. `docs/IMPLEMENTATION_PLAN.md` — Phased milestone checklist, target file paths, and verification gates.

---

## 2. Non-Negotiable Architectural Red Lines

Every agent and contributor MUST uphold these eight invariants without exception:

1. **Pure Deterministic State Machine (`negroni-ha::reducer`):**
   - `crates/negroni-ha/src/reducer.rs` MUST remain 100% synchronous, pure, and deterministic.
   - NEVER import `tokio`, `std::net`, `std::process`, `std::fs`, or call `Instant::now()` / `SystemTime::now()` inside the reducer. Monotonic timestamps (`Instant`) MUST enter exclusively via `HaEvent` fields so Deterministic Simulation Testing (DST) controls virtual time.
2. **Strict Async Cancellation Safety:**
   - Inside `tokio::select!`, ONLY poll cancel-safe channel receives (`mpsc::Receiver::recv`, `watch::Receiver::changed`) and timers (`tokio::time::Interval::tick`).
   - NEVER place multi-step DCS transactions, SQL queries, HTTP calls, or subprocesses directly inside a `tokio::select!` branch. Tag short-lived per-tick RPC effects with `rpc_epoch: u64` and long-running lifecycle transitions/jobs with `state_epoch: u64`, discarding stale completions on epoch mismatch.
3. **Hardware Watchdog Isolation (`negroni-watchdog`):**
   - `/dev/watchdog` MUST be owned by a dedicated OS `std::thread` spawned *before* the Tokio runtime initializes (`ioctl_linux.rs` is the workspace's sole `#![allow(unsafe_code)]` module).
   - The watchdog thread MUST NEVER run async tasks, call `tracing` macros, allocate unbounded heap memory, or perform network/disk I/O. Read back the kernel-rounded timeout from `WDIOC_SETTIMEOUT` and verify `loop_wait + retry_timeout + 2s <= actual_timeout < ttl` (writing `'V'` immediately on mismatch), and issue `WDIOC_KEEPALIVE` *only* while `Instant::now() + keepalive_cadence < proof.valid_until` anchored to the **pre-flight** RPC timestamp.
   - NEVER disarm `/dev/watchdog` (writing magic byte `'V'`) during demotion until `negroni-pg` confirms PostgreSQL has exited or stopped accepting writes.
4. **Single-Connection Local SQL Serialization (`negroni-pg`):**
   - NEVER use a connection pool (`deadpool`, `bb8`, `sqlx::Pool`) for local PostgreSQL control queries. External REST API probe storms must never exhaust `superuser_reserved_connections`.
   - Local control SQL executes over a single persistent `tokio_postgres::Client` owned privately by `SqlControlActor` behind a bounded `mpsc` channel. Short-lived per-database connections are allowed *only* when creating or advancing logical replication slots in non-default databases.
5. **Lock-Free Health Probes (`negroni-api`):**
   - Read-only health routes (`/`, `/primary`, `/replica`, `/read-write`, `/read-only`, `/synchronous`, `/quorum`, `/asynchronous`, `/health`, `/liveness`, `/readiness`) MUST read exclusively from `ArcSwap<PublishedNodeState>`. They MUST NEVER await SQL queries, DCS RPCs, or async locks.
6. **`vm.overcommit_memory=2` Immunity:**
   - Production PostgreSQL Linux hosts run with `vm.overcommit_memory=2`. Pre-allocate Tokio worker stacks and bounded channels at startup. NEVER spawn unbounded OS threads at runtime.
7. **Strict PostgreSQL 15–18 Scope:**
   - Reject `server_version_num < 150000` (and unsupported future versions) explicitly via `PgMajor::from_version_num`.
   - NEVER introduce legacy PG 9.3–14 code (`recovery.conf` files, `pg_xlog_*` SQL functions, `promote_trigger_file` promotion workflows, `wal_keep_segments`).
   - Route all PG 15, 16, 17, and 18 differences strictly through `PgMajor::capabilities()` (`PgCapabilities`).
8. **100% Patroni 4.1+ Wire & Operational Drop-In Parity:**
   - Preserve byte-exact JSON keys and types in DCS payloads (`/initialize`, `/leader`, `/members/<name>`, `/config`, `/status`, `/sync`, `/failover`, `/history`, `/failsafe`), Kubernetes `Endpoints`/`Lease`/`ConfigMap` annotations, HTTP `200`/`503` probe semantics, `patroni.yml` schemas, and `PATRONI_*` env vars (with `NEGRONI_*` supported as transparent higher-priority aliases).
   - *Explicit Wire Exception:* Embedded Raft (`dcs-raft`) uses `openraft` and is NOT wire-compatible with upstream Patroni's `pysyncobj` (which uses Python `pickle` over TCP). Never implement a `pickle` parser in Rust; document standby-cluster switchover for Raft migrations.

---

## 3. Workspace Dependency DAG & Crate Ownership

Enforce a strict directed acyclic graph across workspace crates. Circular dependencies or upward imports are forbidden:

| Crate | Single Responsibility | Allowed Workspace Deps | Forbidden External Crates |
| :--- | :--- | :--- | :--- |
| `negroni-types` | Wire `serde` models, `WalLsn`, `CasToken`, `PgMajor` | None | `tokio`, `reqwest`, `axum`, `rustix` |
| `negroni-config` | `patroni.yml`, `PATRONI_*`/`NEGRONI_*` merge, timing math | `negroni-types` | `tokio`, `axum`, `tokio-postgres` |
| `negroni-watchdog` | Dedicated `/dev/watchdog` OS thread & `sd_notify` | `negroni-types`, `negroni-config` | `tokio` (must remain pure OS thread) |
| `negroni-dcs` | `DcsBackend` trait, `AnyDcs` enum, etcd3/K8s/Consul/ZK/Raft | `negroni-types`, `negroni-config` | `tokio-postgres`, `axum` |
| `negroni-pg` | `pidfd` process supervisor, `SqlControlActor`, conf/HBA, slots, Citus | `negroni-types`, `negroni-config` | `axum`, `etcd-client`, `kube` |
| `negroni-ha` | Pure `NodeCtx::step` reducer, `CoordinatorActor`, `JobSupervisor`, DST | `negroni-types`, `negroni-config`, `negroni-dcs`, `negroni-pg`, `negroni-watchdog` | Direct DB or raw DCS drivers |
| `negroni-api` | `axum` + `rustls` `ArcSwap` probes & guarded control RPCs | `negroni-types`, `negroni-config`, `negroni-ha` | `tokio-postgres` |
| `negroni-cli` | 19-command `patronictl` / `negronictl` CLI | `negroni-types`, `negroni-config`, `negroni-dcs` | `negroni-ha`, `negroni-watchdog` |
| `bin/negroni` | Multicall `argv[0]` router & pre-allocated runtime entrypoint | All workspace crates | Business logic |

---

## 4. Toolchain, Crate Selection & Coding Standards

- **Rust Toolchain:** `edition = "2024"`, `rust-version = "1.85"`.
- **Zero OpenSSL / C Runtime Policy:** Must cross-compile cleanly to `x86_64-unknown-linux-musl` and `aarch64-unknown-linux-musl`.
  - TLS: `rustls` (`ring` or `aws-lc-rs` crypto provider behind clean feature flags) + `tokio-rustls` + `rustls-pemfile`.
  - Syscalls: `rustix` (`process::pidfd_open`, `process::pidfd_send_signal`, `ioctl`, `fs::fsync`).
  - YAML: `saphyr` parsed into `serde_json::Value` for unified YAML + `PATRONI_*`/`NEGRONI_*` env + DCS `/config` merging (NEVER use `serde_norway`/`unsafe-libyaml`, deprecated `serde_yaml`, or unsound `serde_yml`).
  - Time & Timezones: `jiff` (for ISO-8601 scheduled switchovers/restarts and timezone validation; NEVER mutate environment variables at runtime, as `std::env::set_var` is `unsafe` in Rust 2024).
- **Zero-Panic Daemon Hygiene:**
  - Enforce `#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]` in every library crate outside `#[cfg(test)]`.
  - Library errors: `thiserror::Error`. Binary/CLI top-level reporting: `anyhow::Result`.
- **Crash-Safe Atomic File Writes:**
  - Every write to `postgresql.conf`, `pg_hba.conf`, `pg_ident.conf`, `standby.signal`, `recovery.signal`, or `patroni.dynamic.json` MUST write to a sibling `.tmp` file in the same directory, call `File::sync_all()`, atomically `std::fs::rename()` over the target path, and call `sync_all()` on the parent directory file descriptor.
- **Structured Logging:**
  - Use `tracing` macros with structured fields (`member = %self.self_name`, `op_epoch = self.op_epoch`, `lsn = %lsn`). Never use `println!` or `eprintln!` inside daemon crates.

---

## 5. Agent Execution Loop & Verification Gates

When executing tasks from `docs/IMPLEMENTATION_PLAN.md`:

1. **Read Specs First:** Consult the matching section in `docs/LLD.md` before creating files or changing public types.
2. **Test-First Discipline:**
   - Write round-trip `serde_json` wire tests for every Patroni JSON payload struct.
   - Write pure unit tests and `proptest` Deterministic Simulation (`SimCluster`) tests for every `NodeCtx::step` transition before connecting real sockets or subprocesses.
3. **Mandatory Verification Commands (must pass with zero warnings before marking any task complete):**
   ```bash
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets --all-features -- -D warnings
   cargo clippy --workspace --no-default-features -- -D warnings
   cargo test --workspace --all-features
   ```
4. **Track State:** Update checkboxes (`- [x]`) in `docs/IMPLEMENTATION_PLAN.md` immediately upon passing all gates.
