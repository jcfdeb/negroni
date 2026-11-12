use std::fs;
use std::path::{Path, PathBuf};

pub use negroni_types::{PgCapabilities, PgMajor, UnsupportedPgVersion};

use crate::PgError;

/// Exact integer mapping for Prometheus `patroni_postgres_state` (`0..=14`).
///
/// # Examples
///
/// ```
/// use negroni_pg::PgProcessState;
///
/// assert_eq!(PgProcessState::Running.as_u8(), 5);
/// assert_eq!(PgProcessState::Running.patroni_state_label(), "running");
/// assert_eq!(PgProcessState::CreatingReplica.patroni_state_label(), "creating replica");
/// ```
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

impl PgProcessState {
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    pub const fn is_running(self) -> bool {
        matches!(self, Self::Running)
    }

    /// Canonical Patroni REST & DCS `/members/<name>` `state` string corresponding to this state.
    pub const fn patroni_state_label(self) -> &'static str {
        match self {
            Self::InitDb => "initializing new cluster",
            Self::InitDbFailed => "initdb failed",
            Self::CustomBootstrap => "running custom bootstrap script",
            Self::CustomBootstrapFailed => "custom bootstrap failed",
            Self::CreatingReplica => "creating replica",
            Self::Running => "running",
            Self::Starting | Self::BootstrapStarting => "starting",
            Self::StartFailed => "start failed",
            Self::Restarting => "restarting",
            Self::RestartFailed => "restart failed",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::StopFailed => "stop failed",
            Self::Crashed => "crashed",
        }
    }
}

/// Parses `$PGDATA/PG_VERSION` (e.g. `"17\n"`) and enforces Negroni's PostgreSQL 15–18 support window.
pub fn read_pgdata_major_version(pgdata: &Path) -> Result<PgMajor, PgError> {
    let version_file = pgdata.join("PG_VERSION");
    let raw = fs::read_to_string(&version_file).map_err(|source| PgError::Io {
        context: format!("reading '{}'", version_file.display()),
        source,
    })?;
    parse_major_str(raw.trim())
}

/// Parses `postgres --version` / `pg_ctl --version` output (e.g. `"postgres (PostgreSQL) 18.1"`)
/// into a validated `PgMajor` (`15..=18`).
///
/// # Examples
///
/// ```
/// use negroni_pg::version::{PgMajor, parse_postgres_version_banner};
///
/// assert_eq!(
///     parse_postgres_version_banner("postgres (PostgreSQL) 17.4 (Debian 17.4-1)")?,
///     PgMajor::V17
/// );
/// assert!(parse_postgres_version_banner("postgres (PostgreSQL) 14.15").is_err());
/// # Ok::<(), negroni_pg::PgError>(())
/// ```
pub fn parse_postgres_version_banner(banner: &str) -> Result<PgMajor, PgError> {
    for token in banner.split_whitespace() {
        let digits_prefix: String = token.chars().take_while(char::is_ascii_digit).collect();
        if !digits_prefix.is_empty() {
            return parse_major_str(&digits_prefix);
        }
    }
    Err(PgError::InvalidMetadata(format!(
        "could not extract PostgreSQL major version from banner '{banner}'"
    )))
}

fn parse_major_str(major_token: &str) -> Result<PgMajor, PgError> {
    let major: i32 = major_token.parse().map_err(|_| {
        PgError::InvalidMetadata(format!("invalid PostgreSQL major version '{major_token}'"))
    })?;
    let version_num = major.saturating_mul(10_000);
    PgMajor::from_version_num(version_num).map_err(PgError::from)
}

/// Resolves PostgreSQL server and client utility binaries from `postgresql.bin_dir` (or `$PATH`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgBinaryPaths {
    bin_dir: Option<PathBuf>,
}

impl PgBinaryPaths {
    pub fn new(bin_dir: Option<PathBuf>) -> Self {
        Self { bin_dir }
    }

    pub fn resolve(&self, tool_name: &str) -> PathBuf {
        match &self.bin_dir {
            Some(dir) => dir.join(tool_name),
            None => PathBuf::from(tool_name),
        }
    }

    pub fn initdb(&self) -> PathBuf {
        self.resolve("initdb")
    }

    pub fn pg_ctl(&self) -> PathBuf {
        self.resolve("pg_ctl")
    }

    pub fn postgres(&self) -> PathBuf {
        self.resolve("postgres")
    }

    pub fn pg_basebackup(&self) -> PathBuf {
        self.resolve("pg_basebackup")
    }

    pub fn pg_rewind(&self) -> PathBuf {
        self.resolve("pg_rewind")
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn parses_banners_across_pg15_through_pg18_and_rejects_eol() {
        assert_eq!(
            parse_postgres_version_banner("postgres (PostgreSQL) 15.10").unwrap(),
            PgMajor::V15
        );
        assert_eq!(
            parse_postgres_version_banner("postgres (PostgreSQL) 16.6 (Rocky Linux)").unwrap(),
            PgMajor::V16
        );
        assert_eq!(
            parse_postgres_version_banner("pg_ctl (PostgreSQL) 17.2").unwrap(),
            PgMajor::V17
        );
        assert_eq!(
            parse_postgres_version_banner("postgres (PostgreSQL) 18beta2").unwrap(),
            PgMajor::V18
        );
        assert!(matches!(
            parse_postgres_version_banner("postgres (PostgreSQL) 14.12"),
            Err(PgError::UnsupportedVersion(UnsupportedPgVersion(14)))
        ));
    }

    #[test]
    fn resolves_binaries_with_and_without_bin_dir() {
        let with_dir = PgBinaryPaths::new(Some(PathBuf::from("/usr/pgsql-17/bin")));
        assert_eq!(
            with_dir.pg_rewind(),
            PathBuf::from("/usr/pgsql-17/bin/pg_rewind")
        );

        let on_path = PgBinaryPaths::new(None);
        assert_eq!(on_path.initdb(), PathBuf::from("initdb"));
    }
}
