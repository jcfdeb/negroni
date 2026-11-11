# Contributing to Negroni

Thank you for contributing to **Negroni**! Because database high-availability orchestrators sit directly in the split-brain fencing path, Negroni enforces strict architectural and testing discipline.

## 1. Read the Architecture First

Before opening a pull request, review:
1. [`AGENTS.md`](AGENTS.md) — Non-negotiable architectural red lines and crate dependency boundaries.
2. [`docs/HLD.md`](docs/HLD.md) — Core fencing invariants, concurrency topology, and timing equations.
3. [`docs/LLD.md`](docs/LLD.md) — Trait signatures, state reducer, PG 15–18 capabilities, REST API, and CLI contracts.

## 2. Core Engineering Rules

- **Keep `negroni-ha::reducer` Pure:** Never introduce async I/O, timers, syscalls, or `Instant::now()` inside `crates/negroni-ha/src/reducer.rs`. All state transitions must be deterministic and testable under virtual time (`SimCluster`).
- **Cancel-Safe Async Only:** Never place network RPCs, SQL queries, or subprocesses directly inside `tokio::select!` branches. Poll only channels and timers in `select!`, and fence background effects with `op_epoch`.
- **Zero-Panic Daemon Crates:** `#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]` is enforced across all non-test library code. Return typed `thiserror` errors and handle degraded states gracefully.
- **Zero C / OpenSSL Runtime Dependencies:** All TLS must use `rustls`, and Linux syscalls must use `rustix` so `x86_64-unknown-linux-musl` and `aarch64-unknown-linux-musl` static builds always succeed.
- **PostgreSQL 15–18 Only:** Route version-specific behavior strictly through `PgMajor::capabilities()`. Do not add legacy pre-PG 15 workarounds.

## 3. Development Setup & Local Pre-PR Checks

Install Rust **1.85+** (`rustup update stable`). Every pull request must pass all four checks locally with zero warnings:

```bash
# 1. Check formatting
cargo fmt --all -- --check

# 2. Lint full feature matrix
cargo clippy --workspace --all-targets --all-features -- -D warnings

# 3. Lint minimal no-default-features build
cargo clippy --workspace --no-default-features -- -D warnings

# 4. Run unit and Deterministic Simulation (DST) suites
cargo test --workspace --all-features
```

## 4. Pull Request Guidelines

- Keep pull requests focused on a single subsystem or milestone task from [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md).
- Include `serde_json` wire compatibility tests when touching DCS payloads or REST responses.
- Include `SimCluster` property or unit tests when modifying leader election, quorum standby calculation, `FailsafePrimary`, or watchdog lease proofs.
- By submitting a contribution, you agree that your work is licensed under the repository's [MIT License](LICENSE).
