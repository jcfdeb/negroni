#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;
use std::time::Instant;

use compact_str::CompactString;
use serde::de::{self, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

/// Byte offset in PostgreSQL WAL (`XLogRecPtr`).
///
/// Serializes as a bare `u64` integer in Patroni DCS and REST JSON payloads while
/// formatting and parsing as PostgreSQL `pg_lsn` (`X/YYYYYYYY`) for SQL and CLI output.
///
/// # Examples
///
/// ```
/// use negroni_types::WalLsn;
///
/// let lsn: WalLsn = "1/00000148".parse()?;
/// assert_eq!(lsn.as_u64(), 0x1_0000_0148);
/// assert_eq!(lsn.to_string(), "1/00000148");
/// assert_eq!(serde_json::to_string(&lsn)?, "4294967624");
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash, Serialize, Deserialize,
)]
#[serde(transparent)]
pub struct WalLsn(pub u64);

impl WalLsn {
    pub const ZERO: Self = Self(0);

    pub const fn as_u64(self) -> u64 {
        self.0
    }

    pub const fn saturating_lag_from(self, upstream: Self) -> u64 {
        upstream.0.saturating_sub(self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WalLsnParseError {
    #[error("missing '/' separator in pg_lsn string")]
    MissingSeparator,
    #[error("pg_lsn segment must contain 1 to 8 ASCII hexadecimal digits")]
    InvalidSegmentLength,
    #[error("invalid hexadecimal digit in pg_lsn string")]
    InvalidHex,
}

fn parse_lsn_half(segment: &str) -> Result<u32, WalLsnParseError> {
    if segment.is_empty() || segment.len() > 8 {
        return Err(WalLsnParseError::InvalidSegmentLength);
    }
    if !segment.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(WalLsnParseError::InvalidHex);
    }
    u32::from_str_radix(segment, 16).map_err(|_| WalLsnParseError::InvalidHex)
}

impl Display for WalLsn {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let hi = (self.0 >> 32) as u32;
        let lo = (self.0 & 0xFFFF_FFFF) as u32;
        write!(f, "{hi:X}/{lo:08X}")
    }
}

impl FromStr for WalLsn {
    type Err = WalLsnParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (hi_str, lo_str) = s
            .split_once('/')
            .ok_or(WalLsnParseError::MissingSeparator)?;
        let hi = u64::from(parse_lsn_half(hi_str)?);
        let lo = u64::from(parse_lsn_half(lo_str)?);
        Ok(Self((hi << 32) | lo))
    }
}

/// Supported PostgreSQL major versions (15 through 18).
///
/// # Examples
///
/// ```
/// use negroni_types::PgMajor;
///
/// let pg17 = PgMajor::from_version_num(170_004)?;
/// assert_eq!(pg17, PgMajor::V17);
/// assert!(pg17.capabilities().native_failover_slots);
/// assert!(PgMajor::from_version_num(140_012).is_err());
/// # Ok::<(), negroni_types::UnsupportedPgVersion>(())
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

    pub const fn as_u8(self) -> u8 {
        self as u8
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

/// Optimistic concurrency token across DCS backends:
/// - `Revision`: numeric index (`etcd3` `mod_revision`, Consul `ModifyIndex`, ZooKeeper `version`, Raft log index).
/// - `ResourceVersion`: opaque string (`Kubernetes` `metadata.resourceVersion`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CasToken {
    Revision(u64),
    ResourceVersion(CompactString),
}

/// Backend-specific ephemeral session or lease identifier (`etcd3` lease ID, Consul session ID, etc.).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LeaseHandle(pub CompactString);

/// Monotonic proof of DCS lease validity anchored to the pre-flight RPC timestamp.
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

const fn is_false(v: &bool) -> bool {
    !*v
}

const fn is_zero_u32(v: &u32) -> bool {
    *v == 0
}

fn deserialize_null_default_lsn<'de, D>(deserializer: D) -> Result<WalLsn, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<WalLsn>::deserialize(deserializer)?.unwrap_or_default())
}

/// Patroni `/members/<name>` payload (`Member.to_json()` in upstream Patroni 4.1+).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberData {
    pub conn_url: String,
    pub api_url: String,
    pub state: CompactString,
    pub role: CompactString,
    #[serde(default, skip_serializing_if = "CompactString::is_empty")]
    pub version: CompactString,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub xlog_location: Option<WalLsn>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeline: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication_state: Option<CompactString>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub pending_restart: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub pending_restart_reason: BTreeMap<CompactString, serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pause: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watchdog_failed: Option<bool>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<CompactString, serde_json::Value>,
    /// Preserves extension fields (e.g. Citus `group`, custom metadata) across mixed-node rolling upgrades.
    #[serde(flatten)]
    pub extra: BTreeMap<CompactString, serde_json::Value>,
}

impl MemberData {
    /// Recognizes both Patroni 4.x `"primary"` and legacy Patroni 3.x `"master"` role strings.
    pub fn is_primary_role(&self) -> bool {
        matches!(self.role.as_str(), "primary" | "master")
    }

    pub fn is_standby_leader_role(&self) -> bool {
        self.role.as_str() == "standby_leader"
    }

    fn raw_bool_tag(&self, key: &str) -> Option<bool> {
        self.tags
            .get(key)
            .or_else(|| self.extra.get(key))
            .and_then(serde_json::Value::as_bool)
    }

    fn raw_int_tag(&self, key: &str) -> Option<i64> {
        self.tags
            .get(key)
            .or_else(|| self.extra.get(key))
            .and_then(serde_json::Value::as_i64)
    }

    pub fn bool_tag(&self, key: &str) -> bool {
        self.raw_bool_tag(key).unwrap_or(false)
    }

    /// Mirrors `patroni.tags.Tags.nofailover`: explicit `nofailover` bool wins; otherwise
    /// `failover_priority <= 0` disables automatic failover; defaults to `false`.
    pub fn nofailover(&self) -> bool {
        if let Some(explicit) = self.raw_bool_tag("nofailover") {
            explicit
        } else if let Some(prio) = self.raw_int_tag("failover_priority") {
            prio <= 0
        } else {
            false
        }
    }

    /// Mirrors `patroni.tags.Tags.failover_priority`: returns `0` when `nofailover: true`,
    /// otherwise `tags.failover_priority` defaulting to `1`.
    pub fn failover_priority(&self) -> i64 {
        if self.raw_bool_tag("nofailover") == Some(true) {
            0
        } else {
            self.raw_int_tag("failover_priority").unwrap_or(1)
        }
    }

    pub fn noloadbalance(&self) -> bool {
        self.bool_tag("noloadbalance")
    }

    pub fn clonefrom(&self) -> bool {
        self.bool_tag("clonefrom")
    }

    /// Mirrors `patroni.tags.Tags.nosync`: explicit `nosync` bool wins; otherwise
    /// `sync_priority <= 0` excludes the node from synchronous standby selection.
    pub fn nosync(&self) -> bool {
        if let Some(explicit) = self.raw_bool_tag("nosync") {
            explicit
        } else if let Some(prio) = self.raw_int_tag("sync_priority") {
            prio <= 0
        } else {
            false
        }
    }

    /// Mirrors `patroni.tags.Tags.sync_priority`: returns `0` when `nosync: true`,
    /// otherwise `tags.sync_priority` defaulting to `1`.
    pub fn sync_priority(&self) -> i64 {
        if self.raw_bool_tag("nosync") == Some(true) {
            0
        } else {
            self.raw_int_tag("sync_priority").unwrap_or(1)
        }
    }

    pub fn replicatefrom(&self) -> Option<&str> {
        self.tags
            .get("replicatefrom")
            .and_then(serde_json::Value::as_str)
    }
}

/// Patroni `/status` payload (`{"optime": <u64>, "slots": {"<slot>": <u64|null>}, "retain_slots": [...]}`).
///
/// Deserializes both the modern Patroni JSON object format and legacy bare-integer `/status` values,
/// and tolerates `"optime": null` or `"slots": {"slot": null}` when a newly created slot has no LSN yet.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ClusterStatus {
    #[serde(deserialize_with = "deserialize_null_default_lsn")]
    pub optime: WalLsn,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub slots: BTreeMap<CompactString, Option<WalLsn>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub retain_slots: Vec<CompactString>,
}

impl<'de> Deserialize<'de> for ClusterStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct ObjectStatus {
            #[serde(default, deserialize_with = "deserialize_null_default_lsn")]
            optime: WalLsn,
            #[serde(default)]
            slots: BTreeMap<CompactString, Option<WalLsn>>,
            #[serde(default)]
            retain_slots: Vec<CompactString>,
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum RawClusterStatus {
            Object(ObjectStatus),
            LegacyOptimeInt(u64),
            LegacyOptimeStr(String),
        }

        match RawClusterStatus::deserialize(deserializer)? {
            RawClusterStatus::Object(obj) => Ok(Self {
                optime: obj.optime,
                slots: obj.slots,
                retain_slots: obj.retain_slots,
            }),
            RawClusterStatus::LegacyOptimeInt(n) => Ok(Self {
                optime: WalLsn(n),
                slots: BTreeMap::new(),
                retain_slots: Vec::new(),
            }),
            RawClusterStatus::LegacyOptimeStr(s) => {
                let n = s.trim().parse::<u64>().map_err(de::Error::custom)?;
                Ok(Self {
                    optime: WalLsn(n),
                    slots: BTreeMap::new(),
                    retain_slots: Vec::new(),
                })
            }
        }
    }
}

/// Patroni `/sync` payload (`{"leader": "<name>", "sync_standby": "n1,n2", "quorum": 1}`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncState {
    pub leader: Option<CompactString>,
    pub sync_standby: Option<String>,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub quorum: u32,
}

impl SyncState {
    pub fn standby_names(&self) -> impl Iterator<Item = &str> {
        self.sync_standby
            .as_deref()
            .into_iter()
            .flat_map(|csv| csv.split(','))
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }

    pub fn contains_standby(&self, member_name: &str) -> bool {
        self.standby_names().any(|m| m == member_name)
    }
}

/// Patroni `/failover` payload (`{"leader": "...", "candidate": "...", "scheduled_at": "..."}`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailoverIntent {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader: Option<CompactString>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate: Option<CompactString>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheduled_at: Option<String>,
}

impl FailoverIntent {
    pub fn is_empty(&self) -> bool {
        self.leader.as_deref().is_none_or(str::is_empty)
            && self.candidate.as_deref().is_none_or(str::is_empty)
            && self.scheduled_at.as_deref().is_none_or(str::is_empty)
    }
}

/// Single timeline transition tuple in Patroni's `/history` JSON array.
///
/// Upstream Patroni writes 3-, 4-, or 5-element arrays:
/// - `[timeline, lsn, reason]`
/// - `[timeline, lsn, reason, timestamp]`
/// - `[timeline, lsn, reason, timestamp, leader_name]` (Patroni 3.2+ / 4.1+)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryEntry {
    pub timeline: u64,
    pub lsn: WalLsn,
    pub reason: String,
    pub timestamp: Option<String>,
    pub leader: Option<CompactString>,
}

impl Serialize for HistoryEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let len = match (&self.timestamp, &self.leader) {
            (_, Some(_)) => 5,
            (Some(_), None) => 4,
            (None, None) => 3,
        };
        let mut seq = serializer.serialize_seq(Some(len))?;
        seq.serialize_element(&self.timeline)?;
        seq.serialize_element(&self.lsn)?;
        seq.serialize_element(&self.reason)?;
        if len >= 4 {
            seq.serialize_element(&self.timestamp)?;
        }
        if let Some(ref leader) = self.leader {
            seq.serialize_element(leader)?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for HistoryEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum LsnWire {
            Int(u64),
            Text(String),
        }

        struct HistoryTupleVisitor;

        impl<'de> Visitor<'de> for HistoryTupleVisitor {
            type Value = HistoryEntry;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
                formatter.write_str(
                    "a 3-, 4-, or 5-element Patroni history tuple [timeline, lsn, reason, timestamp?, leader?]",
                )
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let timeline: u64 = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let raw_lsn: LsnWire = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let lsn = match raw_lsn {
                    LsnWire::Int(n) => WalLsn(n),
                    LsnWire::Text(s) => s.parse::<WalLsn>().map_err(de::Error::custom)?,
                };
                let reason: String = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                let timestamp: Option<String> = seq.next_element()?.flatten();
                let leader: Option<CompactString> = seq.next_element()?.flatten();

                // Drain any future trailing fields gracefully for forward compatibility.
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}

                Ok(HistoryEntry {
                    timeline,
                    lsn,
                    reason,
                    timestamp,
                    leader,
                })
            }
        }

        deserializer.deserialize_seq(HistoryTupleVisitor)
    }
}

/// Point-in-time consistent view of `/<namespace>/<scope>/...` in DCS.
#[derive(Debug, Clone, Default)]
pub struct ClusterSnapshot {
    pub initialize: Option<Versioned<CompactString>>,
    pub config: Option<Versioned<serde_json::Value>>,
    pub leader: Option<Versioned<CompactString>>,
    pub members: BTreeMap<CompactString, Versioned<MemberData>>,
    pub status: Option<Versioned<ClusterStatus>>,
    pub sync: Option<Versioned<SyncState>>,
    pub failover: Option<Versioned<FailoverIntent>>,
    pub failsafe: Option<BTreeMap<CompactString, String>>,
    pub history: Vec<HistoryEntry>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn wal_lsn_formats_parses_and_rejects_overflowing_segments() {
        let lsn: WalLsn = "16/B374D848".parse().unwrap();
        assert_eq!(lsn.to_string(), "16/B374D848");
        assert_eq!(serde_json::to_string(&lsn).unwrap(), lsn.0.to_string());

        // Lowercase hex accepted by PostgreSQL pg_lsn_in
        let lower: WalLsn = "16/b374d848".parse().unwrap();
        assert_eq!(lower, lsn);

        // Reject > 8 hex digits in low half (prevents silent carry into high 32 bits)
        assert_eq!(
            "0/100000000".parse::<WalLsn>(),
            Err(WalLsnParseError::InvalidSegmentLength)
        );
        assert_eq!(
            "16B374D848".parse::<WalLsn>(),
            Err(WalLsnParseError::MissingSeparator)
        );
    }

    #[test]
    fn pg_major_enforces_15_to_18_window() {
        assert_eq!(
            PgMajor::from_version_num(140_012),
            Err(UnsupportedPgVersion(14))
        );
        assert_eq!(
            PgMajor::from_version_num(190_000),
            Err(UnsupportedPgVersion(19))
        );
        assert!(
            !PgMajor::from_version_num(160_004)
                .unwrap()
                .capabilities()
                .native_failover_slots
        );
        assert!(
            PgMajor::from_version_num(170_001)
                .unwrap()
                .capabilities()
                .native_failover_slots
        );
        assert!(
            PgMajor::from_version_num(180_000)
                .unwrap()
                .capabilities()
                .default_initdb_checksums
        );
    }
}
