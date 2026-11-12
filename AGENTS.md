# AGENTS.md — Negroni Agentic Engineering Manual

## 1. Project Mission & Identity

**Negroni** is a 1:1 wire-compatible, memory-safe Rust re-implementation of **Patroni 4.1+** scoped strictly to **PostgreSQL 15 through 18** (plus Citus 12+). It compiles into a single static multicall Linux/POSIX binary (`negroni`) that also acts as `negronictl` and `patronictl` via `argv[0]` symlink detection or `negroni ctl <subcommand>`.

> **Engineering Mindset:** We are building a production-grade database reliability tool to help people solve real operational problems — **never a vaporware toy**. Every rule in this manual must be backed by real compiler lints, workspace configs, deterministic tests, and CI gates.

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
   - `/dev/watchdog` MUST be owned by a dedicated OS `std::thread` spawned *before* the Tokio runtime initializes (`crates/negroni-watchdog/src/ioctl_linux.rs` is the workspace's sole `#![allow(unsafe_code)]` module).
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

## 3. Workspace Dependency DAG & Clean Crate Boundaries

Enforce a strict directed acyclic graph across workspace crates with a single shared `Cargo.lock` and `[workspace.dependencies]`. Circular dependencies, upward imports, or fat `main.rs` files are forbidden:

| Crate | Single Responsibility | `unsafe` Policy | Allowed Workspace Deps | Forbidden External Crates |
| :--- | :--- | :--- | :--- | :--- |
| `negroni-types` | Wire `serde` models, `WalLsn`, `CasToken`, `PgMajor` | `#![forbid(unsafe_code)]` | None | `tokio`, `reqwest`, `axum`, `rustix` |
| `negroni-config` | `patroni.yml`, `PATRONI_*`/`NEGRONI_*` merge, timing math | `#![forbid(unsafe_code)]` | None (`negroni-types` only if needed) | `tokio`, `axum`, `tokio-postgres` |
| `negroni-watchdog` | Dedicated `/dev/watchdog` OS thread & `sd_notify` | `ioctl_linux.rs` only | `negroni-types`, `negroni-config` | `tokio` (must remain pure OS thread) |
| `negroni-dcs` | `DcsBackend` trait, `AnyDcs` enum, etcd3/K8s/Consul/ZK/Raft | `#![forbid(unsafe_code)]` | `negroni-types`, `negroni-config` | `tokio-postgres`, `axum` |
| `negroni-pg` | `pidfd` process supervisor, `SqlControlActor`, conf/HBA, slots, Citus | `#![forbid(unsafe_code)]` | `negroni-types`, `negroni-config` | `axum`, `etcd-client`, `kube` |
| `negroni-ha` | Pure `NodeCtx::step` reducer, `CoordinatorActor`, `JobSupervisor`, DST | `#![forbid(unsafe_code)]` | `negroni-types`, `negroni-config`, `negroni-dcs`, `negroni-pg`, `negroni-watchdog` | Direct DB or raw DCS drivers |
| `negroni-api` | `axum` + `rustls` `ArcSwap` probes & guarded control RPCs | `#![forbid(unsafe_code)]` | `negroni-types`, `negroni-config`, `negroni-ha` | `tokio-postgres` |
| `negroni-cli` | 19-command `patronictl` / `negronictl` CLI | `#![forbid(unsafe_code)]` | `negroni-types`, `negroni-config`, `negroni-dcs` | `negroni-ha`, `negroni-watchdog` |
| `bin/negroni` | Thin `main.rs` delegating to `bin/negroni/src/lib.rs` | `#![forbid(unsafe_code)]` | All workspace crates | Domain business logic |

---

## 4. Production-Grade Rust Engineering Pillars

Negroni enforces five production Rust pillars in repository configuration and CI:

### 4.1 Strict Static Analysis & Quality Gates
- **Toolchain & MSRV:** `edition = "2024"`, `rust-version = "1.88"` (required for stabilized Rust 2024 `let_chains`; verified continuously by a dedicated MSRV `1.88` CI job).
- **Formatting (`rustfmt.toml`):** Enforced via `cargo fmt --all -- --check` (`edition = "2024"`, `style_edition = "2024"`, `max_width = 100`, `newline_style = "Unix"`).
- **Workspace Lints (`[workspace.lints]` in `Cargo.toml`):**
  - Denies `unwrap_used`, `expect_used`, `panic`, `todo`, `unimplemented`, `dbg_macro`, `undocumented_unsafe_blocks`, `print_stdout`, `print_stderr`, plus curated high-signal `pedantic` and `nursery` lints (`cloned_instead_of_copied`, `debug_assert_with_mut_call`, `explicit_iter_loop`, `implicit_clone`, `inefficient_to_string`, `manual_let_else`, `semicolon_if_nothing_returned`, `unnested_or_patterns`).
  - CI runs `cargo clippy --workspace --all-targets --all-features -- -D warnings` and `--no-default-features -- -D warnings`.
- **`unsafe` Discipline:**
  - Every crate without raw kernel FFI MUST start with `#![forbid(unsafe_code)]` at line 1 of `src/lib.rs` (and `src/main.rs`).
  - Any unavoidable `unsafe` (currently restricted solely to `WDIOC_SETTIMEOUT` / `WDIOC_KEEPALIVE` in `crates/negroni-watchdog/src/ioctl_linux.rs`) MUST be isolated in a dedicated module with an explicit `// SAFETY:` block comment immediately above every `unsafe` block (`clippy::undocumented_unsafe_blocks = "deny"`).

### 4.2 Supply-Chain Security & Dependency Hygiene
- **Zero OpenSSL / C Runtime Policy:** Must cross-compile cleanly to `x86_64-unknown-linux-musl` and `aarch64-unknown-linux-musl`:
  - TLS: `rustls` (`ring` or `aws-lc-rs`) + `tokio-rustls` + `rustls-pemfile`.
  - Syscalls: `rustix` (`process::pidfd_open`, `process::pidfd_send_signal`, `ioctl`, `fs::fsync`).
  - YAML: `saphyr` parsed into `serde_json::Value` (NEVER use `serde_yaml`, `serde_yml`, or `unsafe-libyaml`).
  - Time & Timezones: `jiff` (NEVER call `std::env::set_var` at runtime).
- **Vulnerability Scanning (`cargo-audit`):** Audits `Cargo.lock` against the RustSec Advisory Database on every PR.
- **License, Source & Ban Enforcement (`deny.toml` / `cargo-deny`):** Restricts dependencies to crates.io and OSI-approved permissive licenses while explicitly banning `openssl`, `openssl-sys`, unmaintained YAML crates, and SQL connection pools (`deadpool-postgres`, `bb8-postgres`).
- **Unused Dependency Pruning (`cargo-machete` & `-D unused-crate-dependencies`):** Do not add speculative dependencies ahead of the milestone phase that actually imports them. Every declared dependency in every `Cargo.toml` must be actively used.

### 4.3 Deliberate Error Handling, Crash Safety & Observability
- **Library vs. Binary Split:**
  - All library crates (`crates/negroni-*`) MUST expose structured, matchable domain error enums via `thiserror::Error`.
  - `anyhow` (`anyhow::Result` / `.context(...)`) is reserved strictly for top-level binary startup and CLI context reporting (`bin/negroni`, `negroni-cli`).
- **Zero Panics in Production Paths:** Propagate all fallible operations with `Result`, `Option`, `let ... else`, and `?`. `.unwrap()`, `.expect()`, and `panic!()` are permitted ONLY inside `#[cfg(test)]` modules.
- **Structured Diagnostics (`tracing`):** Emit structured `tracing` events and spans (`member = %self.self_name`, `rpc_epoch = self.rpc_epoch`, `state_epoch = self.state_epoch`, `lsn = %lsn`). `println!` and `eprintln!` are denied across all daemon crates (`clippy::print_stdout = "deny"`, `clippy::print_stderr = "deny"`) and allowed solely in `negroni-cli` and `bin/negroni` pre-logger CLI/fatal exit output.
- **Secret Redaction:** Wrap all passwords and bearer tokens in `SecretString` so `Debug` and structured logs emit `[REDACTED]`.
- **Crash-Safe Atomic File Writes:** Every write to `postgresql.conf`, `pg_hba.conf`, `pg_ident.conf`, `standby.signal`, `recovery.signal`, or `patroni.dynamic.json` MUST write to a sibling `.tmp` file in the same directory, call `File::sync_all()`, atomically `std::fs::rename()` over the target path, and `sync_all()` on the parent directory fd.

### 4.4 Multi-Layered Testing & Verification
- **Fast Isolated Test Runner (`cargo-nextest`):** Configured in `.config/nextest.toml` (`default` and `ci` profiles) for parallel process-per-test execution.
- **Property-Based & Deterministic Simulation Testing (`proptest`):** Every HA state-machine invariant (`NodeCtx::step`, election safety, `/failsafe`, watchdog fencing) must be verified under virtual time in `SimCluster` (`crates/negroni-ha/tests/dst_cluster_sim.rs`) across thousands of generated schedules.
- **Parser Fuzzing (`cargo-fuzz`):** Untrusted or operator-supplied text parsers (`patroni.yml` YAML/env overlay, GUC unit normalizer, `pg_hba.conf` / `pg_ident.conf` validator, and Patroni DCS/REST JSON payloads) must include `cargo-fuzz` targets ensuring zero panics or OOMs on arbitrary byte slices.
- **Runnable Doc-Tests & MSRV Matrix:** Public types and pure domain functions should carry runnable `///` examples verified by `cargo test --workspace --doc --all-features`, alongside the CI MSRV (`1.88`) check.

### 4.5 Type-Driven Design & Thin `main.rs`
- **Thin `main.rs`:** `bin/negroni/src/main.rs` MUST remain a minimal wrapper (< 15 lines) delegating immediately to `bin/negroni/src/lib.rs` so all entrypoint routing and configuration wiring can be imported by integration tests.
- **Newtypes over Primitives:** Never pass raw `u64` or `String` across module boundaries when domain invariants apply — use strongly typed wrappers (`WalLsn`, `CasToken`, `LeaseHandle`, `LeaseProof`, `SecretString`, `PgMajor`).
- **Typestate & Algebraic State Machines:** Encode preconditions in types (`ValidatedTimings` produced only by `validate_timings()`, `Box<dyn ArmedWatchdog>` produced only after `arm()`, and `HaRoleState` carrying `LeaseProof` only in `ArmingPrimary` / `Leader` / `Paused(PausedRole::Leader)`), making invalid transitions unrepresentable at compile time.

---

## 5. Agent Execution Loop & Verification Gates

When executing tasks from `docs/IMPLEMENTATION_PLAN.md`:

1. **Read Specs First:** Consult the matching section in `docs/LLD.md` before creating files or changing public types.
2. **Test-First Discipline:**
   - Write round-trip `serde_json` wire tests and runnable `///` doc-tests for new public types and parsers.
   - Write pure unit tests and `proptest` Deterministic Simulation (`SimCluster`) tests for every `NodeCtx::step` transition before connecting real sockets or subprocesses.
3. **Mandatory Verification Commands (must pass with zero warnings before marking any task complete):**
   ```bash
   # Formatting & strict Clippy (full + minimal feature matrix)
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets --all-features -- -D warnings
   cargo clippy --workspace --no-default-features -- -D warnings

   # Unused dependency check (enforced via rustc locally and cargo-machete in CI)
   RUSTFLAGS="-D unused-crate-dependencies" cargo check --workspace --lib --all-features

   # Unit, DST simulation, and runnable /// doc-tests
   # (Use `cargo nextest run --workspace --all-features` when installed, plus `--doc`)
   cargo test --workspace --all-features
   cargo test --workspace --doc --all-features
   ```
   When optional cargo subcommands are installed locally (`cargo install --locked cargo-nextest cargo-deny cargo-audit cargo-machete`), also run:
   ```bash
   cargo nextest run --workspace --all-features
   cargo deny check
   cargo audit
   cargo machete
   ```
4. **Containerized Verification (`podman`):** Run all local containerized integration suites and DCS/PostgreSQL test matrices via rootless `podman` (see §6) — never hardcode `docker`.
5. **Track State:** Update checkboxes (`- [x]`) in `docs/IMPLEMENTATION_PLAN.md` immediately upon passing all gates.

---

## 6. Cloud-Native Deployment, Local Orchestration & Packaging

Negroni follows a **Cloud-Native-aware** operational, development, and distribution model:

- **Cloud-Native Runtime Posture:**
  - Design images, configs, and entrypoints for unprivileged / rootless containers (arbitrary UIDs / OpenShift & Kubernetes `restricted` Pod Security Standards), clean `SIGTERM` shutdown, `PATRONI_*` / `NEGRONI_*` environment-driven configuration, zero-lock HTTP readiness/liveness/routing probes (`/primary`, `/replica`, `/read-write`, `/synchronous`, `/quorum`), and native Prometheus `/metrics`.
  - Publish minimal, reproducible multi-arch OCI container images (`amd64` / `arm64`) bundling the static `negroni` binary (`negronictl` / `patronictl` symlinks) with PostgreSQL 15–18 client/server tooling.
- **Local Development & Multi-Node Testing (`Podman` / Compose Spec):**
  - Primary developer host environment is **Rocky Linux 10.2 (`x86_64`, Linux 6.12, SELinux)** running **rootless `podman`**.
  - Define local multi-node HA clusters, DCS services (`etcd`, `Consul`, `ZooKeeper`), and integration/Behave test environments using standard **`compose.yaml`** manifests compatible with both **`podman compose`** and **Docker Compose** (using `:Z` SELinux volume relabeling where needed, unprivileged ports, and `$XDG_RUNTIME_DIR/podman/podman.sock` rather than hardcoded `/var/run/docker.sock` or `docker` CLI assumptions).
- **Kubernetes Distribution (`Helm`):**
  - Treat **Helm** (OCI-publishable Helm chart) as a first-class distribution and deployment mechanism alongside static binaries and OS packages:
    - Manage `StatefulSet` topologies, primary/replica `Service` routing, `PodDisruptionBudget`, least-privilege `Role`/`RoleBinding` (`Lease` or `Endpoints` + `ConfigMap` DCS RBAC), `Secret`/TLS volume mounts, and optional Prometheus Operator `ServiceMonitor` / `PodMonitor` resources.
    - Support zero-downtime rolling adoption and migration from existing Patroni / Spilo Kubernetes deployments.
