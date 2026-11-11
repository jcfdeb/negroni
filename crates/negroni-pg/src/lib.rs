use negroni_types::UnsupportedPgVersion;
use thiserror::Error;

/// Exact integer mapping for Prometheus `patroni_postgres_state` (`0..=14`).
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

#[derive(Debug, Error)]
pub enum PgError {
    #[error(transparent)]
    UnsupportedVersion(#[from] UnsupportedPgVersion),
    #[error("PostgreSQL supervision error: {0}")]
    Supervision(String),
}

pub const fn citus_enabled() -> bool {
    cfg!(feature = "citus")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn prometheus_state_discriminants_match_patroni() {
        assert_eq!(PgProcessState::InitDb as u8, 0);
        assert_eq!(PgProcessState::Running as u8, 5);
        assert_eq!(PgProcessState::Crashed as u8, 14);
    }
}
