# Negroni — High-Level Design (HLD)

- **Document Version:** 1.0
- **Target Upstream Parity:** Patroni 4.1+ (1:1 Wire, Config, REST API & CLI)
- **PostgreSQL Support Matrix:** PostgreSQL 15, 16, 17, and 18 (plus Citus 12+)
- **Packaging Artifact:** Single static multicall binary (`negroni` / `negronictl` / `patronictl`)

---

## 1. Executive Summary & Operational Motivation

**Negroni** is a deterministic, memory-safe Rust implementation of Patroni engineered for high-availability PostgreSQL 15–18 clusters. Rather than inventing a new clustering protocol, Negroni implements Patroni's battle-tested consensus, fencing, and configuration contracts byte-for-byte while eliminating failure modes inherent to running a multi-threaded Python supervisor on memory-constrained database hosts.

### 1.1 Production Pain Points Solved by Negroni
1. **Immunity to `vm.overcommit_memory=2` Deadlocks:** PostgreSQL production hardening mandates Linux strict memory overcommit (`vm.overcommit_memory=2`). Upstream Patroni documents that on Python 3.11+, thread creation under memory pressure can raise `MemoryError` or hang indefinitely—leaving the Patroni REST API unresponsive while PostgreSQL keeps running, forcing operators to tune `MALLOC_ARENA_MAX=1` and shrink Python thread stacks. Negroni pre-allocates its fixed OS watchdog thread, Tokio worker pool, and bounded actor queues at process startup (~10–15 MB RSS), eliminating runtime thread-allocation deadlocks permanently.
2. **Deterministic Hardware Watchdog Fencing:** When a primary loses its DCS lease, `/dev/watchdog` must reset the machine before any replica can promote. Running the watchdog keepalive loop on an isolated OS thread outside the async runtime removes GC pauses, GIL contention, and I/O scheduler jitter from the split-brain fencing path.
3. **Zero-Dependency Static Delivery:** Replaces Python virtualenvs, C `libpq` runtime libraries, `psycopg` binary wheels, and `PyInstaller` workarounds with a single `musl`-linked static binary serving both daemon (`negroni`) and CLI (`patronictl` / `negronictl`) roles.

### 1.2 Rationale for Scoping PostgreSQL Support to 15–18
PostgreSQL 14 reaches community End-of-Life in November 2026. Targeting **PostgreSQL 15 through 18** covers 100% of supported upstream releases while cutting ~55–60% of legacy supervisor complexity:
- **Zero `recovery.conf` Dual-Stack Logic (Pre-PG 12):** Every supported version uses `standby.signal` / `recovery.signal` files and native `postgresql.conf` GUCs (`primary_conninfo`, `primary_slot_name`, `restore_command`, `recovery_target_timeline`).
- **Unified `pg_wal_*` Catalogs (Pre-PG 10 / Pre-PG 13):** Eliminates runtime SQL branching for `xlog` vs `wal` function names and `wal_keep_segments` vs `wal_keep_size`.
- **Single Modern `pg_rewind` Path:** All supported versions natively provide `pg_rewind --restore-target-wal` and non-superuser rewind execution via `pg_read_all_data` + rewind function grants.
- **Focused 4-Version Delta:** Version differences narrow cleanly to four well-defined boundaries:

| Version | Configuration & HBA/Ident Changes | Replication & Slot Mechanics | Bootstrap, Rewind & Telemetry |
| :--- | :--- | :--- | :--- |
| **PG 15** *(Baseline)* | Classic `pg_hba.conf`; `promote_trigger_file` GUC still recognized by parser. | Patroni-managed permanent logical slots advanced via `pg_replication_slot_advance()`. | Checkpoints in `pg_stat_bgwriter`; `initdb` checksums off unless `--data-checksums` passed. |
| **PG 16** | Removes `promote_trigger_file`; adds `include`, `include_if_exists`, `include_dir` & `/regex/` in `pg_hba.conf`/`pg_ident.conf`. | Allows logical slot creation & decoding on physical standbys; adds `conflicting` column. | Adds `pg_stat_io`; promotion strictly via `pg_ctl promote` / `pg_promote()`. |
| **PG 17** | Adds `sync_replication_slots` and `synchronized_standby_slots` GUCs. | Adds native **`failover = true` logical slots** managed by Postgres `slotsync` worker. | Splits checkpoint stats out of `pg_stat_bgwriter` into **`pg_stat_checkpointer`**. |
| **PG 18** | Adds **`oauth`** auth method in `pg_hba.conf` & Async I/O GUCs (`io_method`, `io_combine_limit`). | Adds `slotsync_skip_reason` diagnostic column on `pg_replication_slots`. | **`initdb` enables data checksums by default** (`pg_rewind` works out-of-the-box). |

---

## 2. Core System Invariants

1. **Zero Split-Brain Writes:** Local PostgreSQL MUST NEVER accept read-write transactions after its DCS `/leader` lease can expire, unless **DCS Failsafe Mode** (`failsafe_mode: true`) is active and **100%** of cluster members in `/failsafe` explicitly accept the primary's `POST /failsafe` heartbeat within the current retry window.
2. **Hardware Watchdog Fires Before Lease Expiry:** Whenever `/dev/watchdog` is active (`automatic` or `required`), hardware reset timeout `T_wd` MUST satisfy `loop_wait + retry_timeout < T_wd < ttl`, while cluster HA timings MUST satisfy `loop_wait + 2 * retry_timeout <= ttl`.
3. **Pre-Allocated Execution Resources:** All OS threads, Tokio worker stacks, bounded `mpsc` channels, and single-connection SQL actors initialize at boot.
4. **Single-Connection SQL Control Plane:** Local PostgreSQL inspection, GUC reload checks, and promotion execute over a **single serialized persistent connection** (via Unix socket by default) so external HTTP load-balancer checks never consume `superuser_reserved_connections`.
5. **100% Rolling-Upgrade Compatibility:** Mixed Patroni 4.1+ and Negroni nodes MUST interoperate seamlessly in the same HA cluster across etcd v3, Kubernetes, Consul, and ZooKeeper by sharing identical DCS JSON schemas, HTTP status codes, `patroni.yml` files, and `PATRONI_*` environment variables *(standalone embedded Raft uses `openraft` and requires a standby-cluster switchover because upstream `pysyncobj` frames messages with Python `pickle`)*.

---

## 3. Runtime Process & Concurrency Architecture

```text
+-----------------------------------------------------------------------------------+
|                        negroni Multicall Daemon Process                           |
|                                                                                   |
|  +-----------------------------+         Monotonic LeaseProof (epoch, valid_until)|
|  | Dedicated OS Thread         | <---------------------------------------------+  |
|  | Watchdog Keeper (rustix)    |  /dev/watchdog ioctl (NO Tokio, NO network)   |  |
|  +-----------------------------+                                               |  |
|                                                                                |  |
|  +-- Tokio Multi-Thread Runtime (Fixed Workers & Pre-Allocated Stacks) --------|--+
|  |                                                                             |  |
|  |  +----------------------+   Watch Events   +-----------------------------+  |  |
|  |  | DCS Adapter Actor    | ---------------> |   HA Coordinator Actor      |--+  |
|  |  | (etcd3/K8s/ZK/Consul | <--------------- | (Pure Deterministic Reducer |     |
|  |  |  /OpenRaft)          |   CAS / Lease    |  + Epoch-Fenced Dispatcher) |     |
|  |  +----------------------+                  +--------------+--------------+     |
|  |                                                           |                    |
|  |            +----------------------------------------------+---------------+    |
|  |            | Bounded Commands (mpsc)                      | Spawn/Cancel  |    |
|  |            v                                              v               |    |
|  |  +-------------------------------+         +---------------------------+  |    |
|  |  | Postgres Supervisor Actor     |         | Background Job Supervisor |  |    |
|  |  | - Linux pidfd / POSIX monitor |         | - initdb / custom boot    |  |    |
|  |  | - 1 Serialized SQL Connection |         | - pg_basebackup / wal-g   |  |    |
|  |  | - Atomic conf/HBA & Slot Sync |         | - pg_rewind               |  |    |
|  |  |   (PG 15-18 Capability Table) |         | - Callbacks & /failsafe   |  |    |
|  |  +-------------------------------+         +---------------------------+  |    |
|  |                                                                           |    |
|  |  +-------------------------------+         +---------------------------+  |    |
|  |  | Lock-Free Node Snapshot       | <------ | Atomic Pointer Swap       |--+    |
|  |  | (ArcSwap<PublishedNodeState>) |         +---------------------------+       |
|  |  +---------------+---------------+                                             |
|  |                  | ~15ns Zero-Lock Reads               Guarded Control RPCs    |
|  |                  v                                     (BasicAuth + Allowlist) |
|  |  +-----------------------------------------------------------------------+     |
|  |  | REST API & Metrics Server (axum + rustls)                             |---->|
|  |  | - Probes: /primary, /replica, /read-write, /synchronous, /metrics     |     |
|  |  | - Control: /switchover, /failover, /restart, /reload, /failsafe       |     |
|  |  +-----------------------------------------------------------------------+     |
|  +--------------------------------------------------------------------------------+
+-----------------------------------------------------------------------------------+
           | Unix Socket / pidfd / Atomic Files             | HTTPS / mTLS / gRPC
           v                                                v
  [ PostgreSQL 15–18 Postmaster ]                  [ DCS Cluster + Peer Nodes ]
```

### Architectural Isolation Guarantees
- **OS Watchdog Thread:** Spawned before Tokio starts. Receives `LeaseProof { epoch, valid_until }` over a `std::sync::mpsc` channel. If Tokio deadlocks or hangs in kernel I/O, `Instant::now()` passes `valid_until`, keepalives stop immediately, and Linux resets the host prior to `ttl` expiry.
- **Pure Synchronous State Reducer:** `NodeCtx::step` computes `(CurrentState, HaEvent) -> (SmallVec<Effect>, PublishedNodeState)` with zero I/O or `.await` points, eliminating async cancellation bugs and enabling fast in-memory Deterministic Simulation Testing.
- **Zero-Lock Probe Plane:** HAProxy, Envoy, and Kubernetes probes read `ArcSwap<PublishedNodeState>` directly without touching PostgreSQL or waiting behind DCS timeouts.

---

## 4. Configuration Precedence & DCS Wire Contracts

### 4.1 Five-Tier Configuration Resolution
Resolved on boot, `SIGHUP`, `POST /reload`, and DCS `/config` watch notifications:
1. **DCS `/config` Dynamic JSON:** Cluster-wide HA timings (`ttl`, `loop_wait`, `retry_timeout`, `maximum_lag_on_failover`, `maximum_lag_on_syncnode`), `synchronous_mode`, `synchronous_mode_strict`, `synchronous_node_count`, `failsafe_mode`, `pause`, shared `postgresql.parameters`, `pg_hba`, `pg_ident`, and permanent `slots`.
2. **`NEGRONI_*` Environment Variables:** Canonical Negroni environment overrides.
3. **`PATRONI_*` Environment Variables:** Full 1:1 Patroni environment overrides (logs a structured warning if both `NEGRONI_<KEY>` and `PATRONI_<KEY>` are set to different values).
4. **Local Node YAML (`patroni.yml`):** Node `name`, `scope`, `namespace`, local `restapi` and `postgresql` bind addresses, credentials, `bootstrap`, custom replica creation methods, `watchdog` device settings, and `tags` (`nofailover`, `noloadbalance`, `clonefrom`, `nosync`, `sync_priority`).
5. **Base PostgreSQL Config (`postgresql.base.conf`):** Existing DBA baseline included at line 1 of Negroni-rendered `postgresql.conf`.

### 4.2 Logical DCS Keyspace (`/<namespace>/<scope>/...`)

| Logical Path | Lifecycle | Atomic Primitive | Payload Schema & Protocol Role |
| :--- | :--- | :--- | :--- |
| `/initialize` | Persistent | Create-if-absent CAS | String containing PostgreSQL `system_identifier`; arbitrates initial cluster `initdb`. |
| `/leader` | Ephemeral (`ttl`) | Create / Renew-if-match CAS | String containing current leader `name`; authoritative single-writer lease. |
| `/members/<name>` | Ephemeral (`ttl`) | Lease-bound Upsert | JSON `MemberData`: `conn_url`, `api_url`, `state`, `role`, `version`, `timeline`, `xlog_location`, `tags`. |
| `/config` | Persistent | Versioned CAS | JSON dynamic cluster configuration edited via `PATCH /config` or `edit-config`. |
| `/status` | Persistent | Leader-written | JSON `ClusterStatus`: `{"optime": <u64_lsn>, "slots": {"<slot>": <u64_lsn>}}`. |
| `/sync` | Persistent | Versioned CAS | JSON `SyncState`: `{"leader": "<name>", "sync_standby": "n1,n2", "quorum": 1}`. |
| `/failover` | Ephemeral intent | Versioned CAS | JSON `FailoverIntent`: `{"leader": "...", "candidate": "...", "scheduled_at": "..."}`. |
| `/history` | Persistent | Leader-written | JSON array of `[timeline, lsn, reason, timestamp]` tuples driving `pg_rewind`. |
| `/failsafe` | Persistent | Leader-written | JSON map `{"<member_name>": "<api_url>"}` for DCS-outage peer quorum checks. |

**Kubernetes Mapping:** When `kubernetes:` DCS is active, `/leader` and member metadata live on `Endpoints` (or `Lease` when `use_endpoints: false`) annotations while `/config`, `/status`, `/sync`, `/history`, `/failover`, and `/failsafe` live on `<scope>-config` `ConfigMap` annotations, using `metadata.resourceVersion` for optimistic CAS.

---

## 5. HA State Machine, Replication Modes & Watchdog Math

### 5.1 Seven Deterministic Lifecycle States
1. **`Unbootstrapped` (`Bootstrapping` / `Cloning`):** When `$PGDATA` is empty, eligible nodes race via `cas_initialize` to run `initdb` (or custom bootstrap script) and claim `/leader`. Non-winning nodes transition to **`Cloning`**, streaming from the leader or a `clonefrom: true` peer via `pg_basebackup` or custom methods (`wal-g`, `pgbackrest`, `barman`).
2. **`Primary`:** Renews `/leader`, `/status`, and `/failsafe` every `loop_wait`; sends monotonic `LeaseProof` to the Watchdog Thread; maintains `synchronous_standby_names` and `/sync` for priority (`FIRST N`) or quorum (`ANY N`) synchronous modes; coordinates Citus `pg_dist_node` updates when `citus:` is enabled.
3. **`FailsafePrimary`:** Entered if DCS fetch/renewal fails while `failsafe_mode: true`. Sends parallel `POST /failsafe` requests to every member in the last known `/failsafe` map within `retry_timeout`. Only if **all** peers return `200 OK` does the primary extend `lease_valid_until` and keep PostgreSQL read-write; any rejected or timed-out peer forces immediate **`Demoting`**.
4. **`Demoting`:** Triggered on lost lease, failed failsafe quorum, or planned switchover. Issues `pg_ctl stop -m fast` (`SIGINT`) + client backend termination, escalates to `-m immediate` (`SIGQUIT`) 2 seconds before `T_wd` if still running, writes `standby.signal`, and disarms `/dev/watchdog` *only after* PostgreSQL stops.
5. **`ElectionCandidate`:** Entered by healthy replicas when `/leader` expires and no Failsafe primary holds quorum. Verifies `!tags.nofailover`, lag within `maximum_lag_on_failover`, and `/sync` membership (when `synchronous_mode: true`); polls surviving peers via REST to ensure highest WAL LSN wins; acquires `/leader` via CAS; arms `/dev/watchdog` (aborting if `watchdog.mode: required` fails); executes `pg_promote()`.
6. **`Replica` / `StandbyLeader`:** Streams WAL from `/leader` (or from an external upstream cluster when `standby_cluster:` is configured, where one node holds `/leader` as `standby_leader` in recovery while cascading to local replicas) and reconciles physical/logical slots.
7. **`Rewinding` & `Paused`:**
   - **`Rewinding`:** Detects timeline/LSN divergence after a former primary reboots, verifies `data_checksums` or `wal_log_hints`, runs `pg_rewind --restore-target-wal`, and transitions to **`Replica`**.
   - **`Paused`:** Maintenance mode (`pause: true`). Disarms `/dev/watchdog` and suspends automatic demotions, elections, and Postgres restarts while keeping `/members/<name>` and REST health telemetry updated.

### 5.2 Watchdog & Lease Timing Equations
For configured `ttl`, `loop_wait`, `retry_timeout`, and `watchdog.safety_margin` (default `5s`; `-1` selects `floor(ttl / 2)`):
- **Configuration Validity Constraint:** `loop_wait + 2 * retry_timeout <= ttl`
- **Hardware Watchdog Timeout (`T_wd`):**
  - Standard mode (`safety_margin >= 0`): `T_wd = ttl - safety_margin`
  - Strict half-TTL mode (`safety_margin == -1`): `T_wd = floor(ttl / 2)`
- **Fencing & Demotion Budget (`B_demote`):**
  - `B_demote = T_wd - loop_wait - retry_timeout`
  - Validator enforces `B_demote >= 2s` whenever watchdog is enabled so Negroni always has a deterministic window to escalate from `SIGINT` to `SIGQUIT` before hardware reset.
- **Pre-Flight Anchor Rule:** `lease_valid_until = renewal_rpc_started_at + T_wd`. Measuring from RPC start rather than RPC completion guarantees network round-trip latency never extends local write validity past server-side lease expiration.

---

## 6. Security, Observability & Verification Strategy

- **mTLS Everywhere (`rustls`):** Unified pure-Rust TLS configuration supporting server verification, custom CA bundles, and mutual TLS client certificates across DCS, PostgreSQL control/replication sockets, REST API server, peer `/failsafe` calls, and `negronictl`.
- **Control-Plane Hardening:** Mutating REST routes require HTTP Basic Auth (`subtle::ConstantTimeEq`), IP allowlist matching (`restapi.allowlist` CIDRs + dynamic `allowlist_include_members` peer IPs), and optional `verify_client: required` mTLS certificates.
- **Observability & Systemd:** Exposes Patroni-compatible Prometheus metrics on `GET /metrics`, structured JSON `tracing` logs, and native `NOTIFY_SOCKET` (`READY=1`, `STATUS=...`, `STOPPING=1`).
- **Two-Tier Verification:**
  1. **Deterministic Simulation Testing (DST):** `proptest` fuzzing of 3-to-5 node `NodeCtx` clusters over simulated asymmetric network partitions, packet drops, slow `pg_rewind` jobs, and clock jumps, asserting Invariant 1 on every virtual millisecond.
  2. **Upstream Black-Box Oracle:** Unmodified Patroni **Behave (Gherkin)** (`features/`) and **Jepsen** test suites run against containerized Negroni + PostgreSQL 15, 16, 17, and 18 clusters, plus live 3-node Patroni-to-Negroni rolling upgrade tests.
