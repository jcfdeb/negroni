# Security Policy

## Supported Versions

Negroni is currently in pre-v0.1 active development. Security fixes are applied directly to the `main` branch.

| Version | Supported |
| :--- | :--- |
| `main` (pre-0.1) | Yes |

## Automated Memory-Safety & Supply-Chain Controls

Negroni enforces automated defense-in-depth gates across the repository and CI pipeline:

- **Compile-Time `unsafe` Isolation:** All 8 pure-Rust workspace crates and `bin/negroni` enforce `#![forbid(unsafe_code)]`. Kernel `ioctl` calls (`WDIOC_SETTIMEOUT`, `WDIOC_KEEPALIVE`) are isolated exclusively inside `crates/negroni-watchdog/src/ioctl_linux.rs` with mandatory `// SAFETY:` invariants (`clippy::undocumented_unsafe_blocks = "deny"`).
- **Continuous Supply-Chain Auditing:** Every pull request runs **`cargo-audit`** against the RustSec Advisory Database and **`cargo-deny`** (`deny.toml`) to enforce OSI-permissive licenses, crates.io source provenance, and explicit bans on C/OpenSSL (`openssl-sys`) and unsound/deprecated YAML crates (`serde_yaml`, `serde_yml`, `unsafe-libyaml`).
- **Secret Redaction by Construction:** Database, DCS, and REST credentials are stored in `SecretString`, redacting sensitive values (`[REDACTED]`) across `Debug` formatting and structured `tracing` diagnostics.
- **Deterministic Simulation & Fuzzing:** Fencing invariants are verified across 10,000+ `proptest` schedules in `SimCluster`, and untrusted configuration/wire parsers are paired with `cargo-fuzz` targets.

## Security-Critical Scope

We treat the following classes of issues as high-severity security or data-safety vulnerabilities:

1. **Split-Brain / Fencing Bypasses:** Any state-machine flaw allowing two nodes to expose read-write PostgreSQL primaries simultaneously, or disarming `/dev/watchdog` prior to PostgreSQL write termination.
2. **Control-Plane Authentication / Authorization Bypasses:** Flaws in REST API HTTP Basic Auth constant-time verification, CIDR/member IP allowlists, or `rustls` mutual TLS (mTLS) client certificate validation.
3. **Credential Exposure:** Leakage of PostgreSQL (`superuser`, `replication`, `rewind`), DCS, or REST credentials in logs, Prometheus `/metrics`, CLI arguments (`ps`), or world-readable temporary files.
4. **Command / SQL Injection & Parser DoS:** Unsanitized interpolation in custom bootstrap/replica callbacks, `postgresql.conf` / `pg_hba.conf` rendering, `SqlControlActor` queries, or panics/unbounded allocations when parsing untrusted inputs.

## Reporting a Vulnerability

**Please do NOT report security vulnerabilities through public GitHub issues.**

Use **[GitHub Private Vulnerability Reporting](../../security/advisories/new)** on this repository to submit a confidential report including:
- Affected crate(s) and configuration (`patroni.yml` topology, DCS backend, PostgreSQL major version).
- Minimal reproduction steps or a Deterministic Simulation (`SimCluster`) test case demonstrating the issue.
- Impact assessment (e.g., unauthorized failover, credential disclosure, split-brain write window).

You can expect an initial acknowledgment within **72 hours** and coordinated disclosure once a patch and regression test are ready.
