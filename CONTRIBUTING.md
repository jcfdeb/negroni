# Contributing to Negroni

Thank you for contributing to **Negroni**! Because database high-availability orchestrators sit directly in the split-brain fencing path, Negroni enforces strict production-grade Rust architecture, static analysis, supply-chain hygiene, and verification gates.

## 1. Read the Architecture First

Before opening a pull request, review:
1. [`AGENTS.md`](AGENTS.md) — Non-negotiable architectural red lines, crate DAG boundaries, and the 5 Production-Grade Rust Pillars.
2. [`docs/HLD.md`](docs/HLD.md) — Core fencing invariants, concurrency topology, and timing equations.
3. [`docs/LLD.md`](docs/LLD.md) — Trait signatures, state reducer, PG 15–18 capabilities, REST API, and CLI contracts.

## 2. Core Engineering & Rust Quality Rules

- **Keep `negroni-ha::reducer` Pure:** Never introduce async I/O, timers, syscalls, or `Instant::now()` inside `crates/negroni-ha/src/reducer.rs`. All state transitions must be deterministic and testable under virtual time (`SimCluster`).
- **Cancel-Safe Async Only:** Never place network RPCs, SQL queries, or subprocesses directly inside `tokio::select!` branches. Poll only channels and timers in `select!`, and fence background effects with `rpc_epoch` and `state_epoch`.
- **`unsafe` Discipline & Zero Panics:**
  - Every crate except `negroni-watchdog::ioctl_linux` enforces `#![forbid(unsafe_code)]`. Any `unsafe` in `ioctl_linux.rs` requires an explicit `// SAFETY:` comment (`clippy::undocumented_unsafe_blocks = "deny"`).
  - `unwrap_used`, `expect_used`, `panic`, `todo`, `unimplemented`, `dbg_macro`, `print_stdout`, and `print_stderr` are denied across non-test library code via `[workspace.lints]`.
  - Expose structured `thiserror` enums in library crates and reserve `anyhow` (`Context`) for `bin/negroni` / CLI reporting. Emit structured diagnostics via `tracing`.
- **Thin `main.rs` & Type-Driven Design:** Keep `bin/negroni/src/main.rs` minimal (< 15 lines delegating to `lib.rs`). Prefer domain newtypes (`WalLsn`, `CasToken`, `SecretString`) and typestates (`ValidatedTimings`, `ArmedWatchdog`, `HaRoleState`) over bare primitives.
- **Supply-Chain & Zero C/OpenSSL Hygiene:** All TLS must use `rustls`, YAML must use `saphyr`, and Linux syscalls must use `rustix` (enforced by `deny.toml` and the `x86_64-unknown-linux-musl` CI build). Never commit unused dependencies (`cargo-machete` / `-D unused-crate-dependencies`).
- **PostgreSQL 15–18 Only:** Route version-specific behavior strictly through `PgMajor::capabilities()`. Do not add legacy pre-PG 15 workarounds.

## 3. Development Setup & Local Pre-PR Checks

Install Rust **1.88+** (`rustup update stable`). Optionally install the supply-chain and fast test runner subcommands enforced in CI:

```bash
cargo install --locked cargo-nextest cargo-deny cargo-audit cargo-machete
```

Every pull request must pass all checks locally with zero warnings:

```bash
# 1. Check formatting against rustfmt.toml
cargo fmt --all -- --check

# 2. Lint full and minimal feature matrices
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo clippy --workspace --no-default-features -- -D warnings

# 3. Verify zero unused crate dependencies
RUSTFLAGS="-D unused-crate-dependencies" cargo check --workspace --lib --all-features

# 4. Run unit, SimCluster DST, and runnable /// doc-tests
cargo test --workspace --all-features
cargo test --workspace --doc --all-features
# (or: cargo nextest run --workspace --all-features && cargo deny check && cargo audit && cargo machete)
```

For containerized multi-node integration testing on Linux, use rootless **`podman`** (`podman compose`) rather than hardcoding `docker`.

## 4. Pull Request Guidelines

- Keep pull requests focused on a single subsystem or milestone task from [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md).
- Include `serde_json` wire compatibility tests and runnable `///` doc-tests when touching public types, DCS payloads, or REST responses.
- Include `SimCluster` `proptest` or unit tests when modifying leader election, quorum standby calculation, `FailsafePrimary`, or watchdog lease proofs.
- Include `cargo-fuzz` targets when introducing new parsers for untrusted or operator-supplied text (`patroni.yml`, GUC units, `pg_hba.conf`).
- By submitting a contribution, you agree that your work is licensed under the repository's [MIT License](LICENSE).
