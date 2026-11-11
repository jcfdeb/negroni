use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::time::Instant;

use compact_str::CompactString;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Byte offset in PostgreSQL WAL; serializes as `u64` in Patroni JSON payloads
/// while formatting as `X/YYYYYYYY` for SQL queries and CLI tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WalLsn(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WalLsnParseError {
    #[error("missing '/' separator in LSN string")]
    MissingSeparator,
    #[error("invalid hexadecimal component in LSN string")]
    InvalidHex,
}

impl Display for WalLsn {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let hi = (self.0 >> 32) as u32;
        let lo = (self.0 & 0xFFFF_FFFF) as u32;
        write!(f, "{hi:X}/{lo:08X}")
    }
}

impl FromStr for WalLsn {
    type Err = WalLsnParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (hi_str, lo_str) = s.split_once('/').ok_or(WalLsnParseError::MissingSeparator)?;
        let hi = u64::from_str_radix(hi_str, 16).map_err(|_| WalLsnParseError::InvalidHex)?;
        let lo = u64::from_str_radix(lo_str, 16).map_err(|_| WalLsnParseError::InvalidHex)?;
        Ok(Self((hi << 32) | lo))
    }
}

/// Supported PostgreSQL major versions (15 through 18).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PgMajor {
    V15 = 15,
    V16 = 16,
    V17 = 17,
    V18 = 18,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PgCapabilities {
    pub hba_include_and_regex: bool,
    pub standby_logical_slot_create: bool,
    pub native_failover_slots: bool,
    pub split_stat_checkpointer: bool,
    pub default_initdb_checksums: bool,
    pub hba_oauth_and_aio: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("unsupported PostgreSQL major version {0}; Negroni supports PostgreSQL 15 through 18")]
pub struct UnsupportedPgVersion(pub i32);

impl PgMajor {
    pub fn from_version_num(version_num: i32) -> Result<Self, UnsupportedPgVersion> {
        match version_num / 10_000 {
            15 => Ok(Self::V15),
            16 => Ok(Self::V16),
            17 => Ok(Self::V17),
            18 => Ok(Self::V18),
            other => Err(UnsupportedPgVersion(other)),
        }
    }

    pub const fn capabilities(self) -> PgCapabilities {
        match self {
            Self::V15 => PgCapabilities {
                hba_include_and_regex: false,
                standby_logical_slot_create: false,
                native_failover_slots: false,
                split_stat_checkpointer: false,
                default_initdb_checksums: false,
                hba_oauth_and_aio: false,
            },
            Self::V16 => PgCapabilities {
                hba_include_and_regex: true,
                standby_logical_slot_create: true,
                native_failover_slots: false,
                split_stat_checkpointer: false,
                default_initdb_checksums: false,
                hba_oauth_and_aio: false,
            },
            Self::V17 => PgCapabilities {
                hba_include_and_regex: true,
                standby_logical_slot_create: true,
                native_failover_slots: true,
                split_stat_checkpointer: true,
                default_initdb_checksums: false,
                hba_oauth_and_aio: false,
            },
            Self::V18 => PgCapabilities {
                hba_include_and_regex: true,
                standby_logical_slot_create: true,
                native_failover_slots: true,
                split_stat_checkpointer: true,
                default_initdb_checksums: true,
                hba_oauth_and_aio: true,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasToken {
    Revision(u64),
    ResourceVersion(CompactString),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseHandle(pub CompactString);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseProof {
    pub epoch: u64,
    pub valid_until: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Versioned<T> {
    pub value: T,
    pub token: CasToken,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterStatus {
    pub optime: WalLsn,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub slots: BTreeMap<CompactString, WalLsn>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncState {
    pub leader: Option<CompactString>,
    pub sync_standby: Option<String>,
    #[serde(default)]
    pub quorum: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailoverIntent {
    pub leader: Option<CompactString>,
    pub candidate: Option<CompactString>,
    pub scheduled_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ClusterSnapshot {
    pub initialize: Option<Versioned<CompactString>>,
    pub config: Option<Versioned<serde_json::Value>>,
    pub leader: Option<Versioned<CompactString>>,
    pub members: BTreeMap<CompactString, Versioned<MemberData>>,
    pub status: Option<Versioned<ClusterStatus>>,
    pub sync: Option<Versioned<SyncState>>,
    pub failover: Option<Versioned<FailoverIntent>>,
    pub failsafe: Option<BTreeMap<CompactString, String>>,
    pub history: Vec<(u64, WalLsn, String, Option<String>)>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn wal_lsn_formats_and_serializes_compatibly() {
        let lsn: WalLsn = "16/B374D848".parse().unwrap();
        assert_eq!(lsn.to_string(), "16/B374D848");

        let json = serde_json::to_string(&lsn).unwrap();
        assert_eq!(json, lsn.0.to_string());
    }

    #[test]
    fn pg_major_enforces_15_to_18_window() {
        assert_eq!(PgMajor::from_version_num(140_012), Err(UnsupportedPgVersion(14)));
        assert!(!PgMajor::from_version_num(160_004).unwrap().capabilities().native_failover_slots);
        assert!(PgMajor::from_version_num(170_001).unwrap().capabilities().native_failover_slots);
        assert!(PgMajor::from_version_num(180_000).unwrap().capabilities().default_initdb_checksums);
    }
}
