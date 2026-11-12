# Negroni — Phased Implementation Plan

Every task below maps directly to `docs/HLD.md` and `docs/LLD.md`. Complete phases in order and run the mandatory verification gates from `AGENTS.md` before checking off (`- [x]`) any item.

---

## Phase 0: Workspace Foundation, Wire Types & Config Engine

- [x] **0.1 Workspace & Multicall Binary Bootstrap**
  - **Files:** `Cargo.toml`, `bin/negroni/Cargo.toml`, `bin/negroni/src/main.rs`, stub `lib.rs` in all 8 crates.
  - **Deliverable:** Configure workspace dependencies, `#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]`, `argv[0]` symlink detection (`negroni` / `negronictl` / `patronictl`), and `negroni ctl` subcommand routing.
- [x] **0.2 Core Types & Patroni Wire Models (`negroni-types`)**
  - **Files:** `crates/negroni-types/src/lib.rs`, `crates/negroni-types/tests/wire_snapshots.rs`.
  - **Deliverable:** Implement `WalLsn` (`u64` JSON <-> `X/YYYYYYYY` `Display`/`FromStr`), `CasToken`, `LeaseHandle`, `Versioned<T>`, `MemberData`, `ClusterStatus`, `SyncState`, `FailoverIntent`, `HistoryEntry`, and `ClusterSnapshot` with round-trip snapshot tests against real Patroni 4.1+ JSON payloads.
- [x] **0.3 Configuration Loader, Dual-Prefix Env Overlay & Timing Validator (`negroni-config`)**
  - **Files:** `crates/negroni-config/src/{lib.rs, env_overlay.rs, timing.rs, schema.rs}`.
  - **Deliverable:** Parse `patroni.yml` (`saphyr`), merge `PATRONI_*` and `NEGRONI_*` overrides with redacted conflict warnings, merge dynamic DCS `/config` (RFC 7396), and enforce `validate_timings` (`loop_wait + 2 * retry_timeout <= ttl` and `B_demote >= 2s`).

---

## Phase 1: Pure HA Reducer, Hardware Watchdog & Deterministic Simulation

- [x] **1.1 Hardware Watchdog OS Thread & Systemd Notifier (`negroni-watchdog`)**
  - **Files:** `crates/negroni-watchdog/src/{lib.rs, thread.rs, ioctl_linux.rs, systemd.rs}`.
  - **Deliverable:** Dedicated pre-Tokio `std::thread` managing `/dev/watchdog` (`WDIOC_SETTIMEOUT` with kernel granularity readback validation, `WDIOC_KEEPALIVE`, magic `'V'` disarm), software mock watchdog for unit tests, and native `NOTIFY_SOCKET` (`sd_notify`) client.
- [x] **1.2 Pure Synchronous State Reducer (`negroni-ha::reducer`)**
  - **Files:** `crates/negroni-ha/src/{reducer.rs, election.rs, sync_standby.rs}`.
  - **Deliverable:** Implement all HA states (`ArmingPrimary` watchdog gate, peer-LSN `ElectionCandidate` discovery, multi-tick `FailsafePrimary` renewal & DCS recovery, and `Paused` primary preservation), `rpc_epoch` / `state_epoch` split fencing, pre-flight `LeaseProof` math, and priority (`FIRST N`) / quorum (`ANY N`) synchronous standby computation (`sync_priority`, `nosync`).
- [x] **1.3 Cancel-Safe Actor Shell & Background Job Supervisor (`negroni-ha::actor`)**
  - **Files:** `crates/negroni-ha/src/{lib.rs, actor.rs, dispatcher.rs, jobs.rs}`.
  - **Deliverable:** Implement `coordinator_loop` (strictly cancel-safe `tokio::select!`), `EffectDispatcher`, cancellable `JobSupervisor`, and `ArcSwap<PublishedNodeState>` atomic publisher.
- [x] **1.4 In-Memory Deterministic Simulation Testing (`SimCluster`)**
  - **Files:** `crates/negroni-ha/tests/dst_cluster_sim.rs`.
  - **Deliverable:** Virtual-clock 3-node and 5-node `proptest` simulator injecting asymmetric network partitions, delayed demotions, and `FailsafePrimary` rejections; prove zero overlapping writable primary windows across 10,000+ generated schedules.

---

## Phase 2: PostgreSQL 15–18 Supervisor & Dual-Mode Slot Manager (`negroni-pg`)

- [x] **2.1 Version Capability Table & `pidfd` Process Supervisor**
  - **Files:** `crates/negroni-pg/src/{version.rs, process.rs, pidfd_monitor.rs}`.
  - **Deliverable:** Implement `PgMajor` (`15..=18`), `PgCapabilities`, `PgProcessState` (`0..=14`), zero-downtime `postmaster.pid` adoption via Linux `pidfd_open` + `AsyncFd` (with POSIX fallback), `initdb` runner, and the two-stage emergency demotion ladder (`SIGINT` -> `SIGQUIT` at `T_wd - 2s` -> atomic `standby.signal`).
- [x] **2.2 Single-Connection `SqlControlActor`**
  - **Files:** `crates/negroni-pg/src/sql_actor.rs`.
  - **Deliverable:** Bounded-channel single persistent `tokio_postgres::Client` (Unix socket + `rustls` fallback), 1-RTT telemetry SQL query, `pg_promote()`, and version-routed checkpoint stats (`pg_stat_bgwriter` on 15–16 vs `pg_stat_checkpointer` on 17–18).
- [x] **2.3 Atomic Config Writer, Unit Canonicalizer, HBA/Ident Validator & Parser Fuzzers**
  - **Files:** `crates/negroni-pg/src/{config_writer.rs, guc_units.rs, hba_validator.rs}`, `fuzz/fuzz_targets/{patroni_yaml.rs, wire_json.rs, guc_units.rs, hba_validator.rs}`.
  - **Deliverable:** `postgresql.base.conf` preservation, crash-safe `.tmp` + `fsync` + `rename` + dir `fsync` writer, byte/microsecond GUC unit normalizer, PG 16+ `include*`/`/regex/` validator, PG 18 `oauth` + Async I/O validator, and `cargo-fuzz` targets covering YAML/JSON/GUC/HBA untrusted inputs.
- [x] **2.4 Replica Cloning, `pg_rewind` & Custom Bootstrap Hooks**
  - **Files:** `crates/negroni-pg/src/{clone.rs, rewind.rs}`.
  - **Deliverable:** `pg_basebackup` streaming clone (`clonefrom` aware), custom replica commands (`wal-g`, `pgbackrest`, `barman`), timeline history divergence check, checksum/`wal_log_hints` gate, and `pg_rewind --restore-target-wal` runner.
- [x] **2.5 Dual-Mode Replication Slot Manager (PG 15–16 vs. 17–18)**
  - **Files:** `crates/negroni-pg/src/slots.rs`.
  - **Deliverable:** Physical member slot TTL cleanup, PG 15–16 `pg_replication_slot_advance()` sync, and PG 17–18 native `failover = true` `slotsync` integration (`dbname` injection in `primary_conninfo`, `synchronized_standby_slots` rendering, and stale `failover = true AND synced = false` slot drop on demoted/rewound primaries).

---

## Phase 3: Tier-1 DCS Drivers, REST API & CLI (Production MVP)

- [x] **3.1 `etcd` v3 Backend (`negroni-dcs::etcd3`)**
  - **Files:** `crates/negroni-dcs/src/etcd3.rs`.
  - **Deliverable:** Pure-Rust `tonic` + `prost` HTTP/2 gRPC transactional CAS (`Txn`), background lease keepalive task, atomic prefix snapshot reader, `DeleteRange`, TLS/mTLS + `Authenticate` token support, and resilient watch stream (zero `protoc` build dependency).
- [x] **3.2 Kubernetes Backend (`negroni-dcs::kube`)**
  - **Files:** `crates/negroni-dcs/src/kube.rs`.
  - **Deliverable:** `kube-core` + `k8s-openapi` + `saphyr` streaming NDJSON watcher and `resourceVersion` CAS supporting both `Endpoints` (with active primary Pod subset sync) and `coordination.k8s.io/v1::Lease` modes plus `<scope>-config` `ConfigMap`.
- [x] **3.3 Lock-Free REST API, Guarded Control Plane & Prometheus Exporter (`negroni-api`)**
  - **Files:** `crates/negroni-api/src/{lib.rs, probes.rs, control.rs, auth.rs, metrics.rs}`.
  - **Deliverable:** `axum` + `rustls` server, zero-lock `ArcSwap` `GET`/`HEAD`/`OPTIONS` probes, `AuthAndAllowlistLayer` (constant-time Basic Auth + CIDR/peer IP allowlist + mTLS), all mutating endpoints, `POST /failsafe`, and Prometheus `/metrics`.
- [x] **3.4 Full 19-Command `negronictl` / `patronictl` CLI (`negroni-cli`)**
  - **Files:** `crates/negroni-cli/src/{lib.rs, list.rs, edit_config.rs, switchover.rs, query.rs}`.
  - **Deliverable:** Complete `clap` CLI, `PrettyTable`-identical ASCII/UTF-8/JSON/YAML/TSV tables, `topology` cascade tree, interactive switchover/failover prompts, `query` (`tokio-postgres` + `rustls`), and `edit-config` (`$EDITOR` + `similar` diff + CAS commit).

---

## Phase 4: Full DCS Matrix, Standby Clusters & Citus

- [ ] **4.1 Consul & ZooKeeper Backends (`negroni-dcs::consul`, `negroni-dcs::zookeeper`)**
  - **Files:** `crates/negroni-dcs/src/{consul.rs, zookeeper.rs}`.
  - **Deliverable:** Consul session TTL + `?cas=` + `?index=` blocking queries, and ZooKeeper ephemeral znodes + versioned `set_data` + persistent watches.
- [ ] **4.2 Standalone Embedded Raft (`negroni-dcs::raft`)**
  - **Files:** `crates/negroni-dcs/src/raft.rs`.
  - **Deliverable:** `openraft` state machine and persistent log store configured via Patroni's `raft:` YAML stanza.
- [ ] **4.3 Opt-In Legacy DCS Hooks (`legacy-dcs`)**
  - **Files:** `crates/negroni-dcs/src/{etcd2.rs, exhibitor.rs}`.
  - **Deliverable:** Feature-gated `etcd2` HTTP client and `exhibitor` dynamic ZooKeeper ensemble poller.
- [ ] **4.4 Standby Clusters, Lifecycle Callbacks & Citus Coordinator Sync**
  - **Files:** `crates/negroni-ha/src/{standby_cluster.rs, callbacks.rs}`, `crates/negroni-pg/src/citus.rs`.
  - **Deliverable:** Cross-region `standby_cluster` (`standby_leader`, `demote-cluster`, `promote-cluster`), `on_start`/`on_stop`/`on_role_change` callback runner (plus `aws-callbacks`), and Citus `pg_dist_node` transactional synchronization.

---

## Phase 5: Upstream Black-Box Compliance & Static Release Pipeline

- [ ] **5.1 Upstream Patroni Behave (Gherkin) Test Matrix**
  - **Deliverable:** Containerized CI harness running Patroni's unmodified `features/*.feature` black-box suite against `negroni` across PostgreSQL 15, 16, 17, and 18.
- [ ] **5.2 Zero-Downtime Mixed-Node Rolling Migration Suite**
  - **Deliverable:** Automated integration test migrating a live 3-node Patroni 4.1 cluster under `pgbench` read/write load node-by-node to 3 Negroni nodes on both etcd v3 and Kubernetes with zero split-brain or lost committed transactions.
- [ ] **5.3 Jepsen Nemesis Validation & Cloud-Native Packaging**
  - **Deliverable:** Pass Jepsen network-partition / `SIGSTOP` / clock-skew workloads and publish static `x86_64-unknown-linux-musl` and `aarch64-unknown-linux-musl` releases, rootless multi-arch OCI container images, `podman compose` / Docker Compose dev manifests (`compose.yaml`), an OCI **Helm chart** (`StatefulSet`, least-privilege DCS RBAC, `Service`s, `PodDisruptionBudget`, `ServiceMonitor`), and RPM/DEB packages containing `/usr/bin/negroni`, `/usr/bin/negronictl`, and `/usr/bin/patronictl` symlinks.
