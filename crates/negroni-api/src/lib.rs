#![forbid(unsafe_code)]

use negroni_ha::PublishedNodeState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeStatus {
    Ok200,
    Unavailable503,
}

pub fn evaluate_primary_probe(snapshot: &PublishedNodeState) -> ProbeStatus {
    if snapshot.role == "primary" && snapshot.state == "running" && snapshot.is_read_write_valid {
        ProbeStatus::Ok200
    } else {
        ProbeStatus::Unavailable503
    }
}

pub fn evaluate_replica_probe(snapshot: &PublishedNodeState, max_lag: Option<u64>) -> ProbeStatus {
    if snapshot.role != "replica" || snapshot.state != "running" || snapshot.noloadbalance {
        return ProbeStatus::Unavailable503;
    }
    if let Some(limit) = max_lag
        && snapshot.replication_lag_bytes > limit
    {
        return ProbeStatus::Unavailable503;
    }
    ProbeStatus::Ok200
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::time::Instant;

    use compact_str::CompactString;

    use super::*;

    #[test]
    fn primary_probe_requires_valid_lease_and_running_postgres() {
        let mut snap = PublishedNodeState::initial(Instant::now());
        assert_eq!(evaluate_primary_probe(&snap), ProbeStatus::Unavailable503);

        snap.role = CompactString::const_new("primary");
        snap.state = CompactString::const_new("running");
        snap.is_read_write_valid = true;
        assert_eq!(evaluate_primary_probe(&snap), ProbeStatus::Ok200);
    }
}
