use std::fmt::Write as _;
use std::time::Instant;

use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use negroni_ha::PublishedNodeState;

use crate::ApiState;
use crate::probes::instant_to_unix_epoch_secs;

pub const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

fn escape_label_value(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

fn push_gauge_u64(buf: &mut String, name: &str, help: &str, scope_escaped: &str, val: u64) {
    let _ = writeln!(buf, "# HELP {name} {help}");
    let _ = writeln!(buf, "# TYPE {name} gauge");
    let _ = writeln!(buf, "{name}{{scope=\"{scope_escaped}\"}} {val}");
}

fn push_gauge_f64(buf: &mut String, name: &str, help: &str, scope_escaped: &str, val: f64) {
    let _ = writeln!(buf, "# HELP {name} {help}");
    let _ = writeln!(buf, "# TYPE {name} gauge");
    let _ = writeln!(buf, "{name}{{scope=\"{scope_escaped}\"}} {val}");
}

/// Renders all 22 Patroni 4.1 Prometheus `0.0.4` metrics from a lock-free [`PublishedNodeState`] snapshot.
///
/// # Examples
///
/// ```
/// use std::time::Instant;
/// use negroni_api::metrics::render_prometheus_metrics;
/// use negroni_ha::PublishedNodeState;
///
/// let now = Instant::now();
/// let mut snap = PublishedNodeState::initial(now);
/// snap.scope = "batman".into();
/// snap.role = "primary".into();
/// snap.state = "running".into();
/// snap.is_read_write_valid = true;
///
/// let text = render_prometheus_metrics(&snap, now);
/// assert!(text.contains("patroni_primary{scope=\"batman\"} 1"));
/// assert!(text.contains("patroni_postgres_running{scope=\"batman\"} 1"));
/// ```
pub fn render_prometheus_metrics(snap: &PublishedNodeState, now: Instant) -> String {
    let mut out = String::with_capacity(2_560);
    let scope = escape_label_value(&snap.scope);

    let pg_running = u64::from(snap.state == "running" || snap.state == "streaming");
    let is_primary = u64::from(snap.role == "primary" && snap.is_read_write_valid);
    let is_standby_leader = u64::from(snap.role == "standby_leader" && snap.holds_leader_lease);
    let is_replica = u64::from(snap.role == "replica");
    let is_sync = u64::from(snap.role == "replica" && snap.is_sync_standby);
    let is_quorum = u64::from(snap.role == "replica" && snap.is_quorum_standby);
    let is_async =
        u64::from(snap.role == "replica" && !snap.is_sync_standby && !snap.is_quorum_standby);

    let xlog_loc = if snap.role == "primary" {
        snap.xlog_location.0
    } else {
        0
    };
    let xlog_recv = if snap.role == "primary" {
        0
    } else {
        snap.xlog_location.0
    };
    let dcs_seen_epoch = snap
        .dcs_last_seen
        .map_or(0, |at| instant_to_unix_epoch_secs(at, now));

    let _ = writeln!(out, "# HELP patroni_version Patroni daemon version info.");
    let _ = writeln!(out, "# TYPE patroni_version gauge");
    let _ = writeln!(
        out,
        "patroni_version{{scope=\"{scope}\",version=\"4.1.0\"}} 1"
    );

    push_gauge_u64(
        &mut out,
        "patroni_postgres_running",
        "1 if PostgreSQL is running, 0 otherwise.",
        &scope,
        pg_running,
    );
    push_gauge_u64(
        &mut out,
        "patroni_postgres_state",
        "Current PostgreSQL supervisor state code (0..14).",
        &scope,
        u64::from(snap.postgres_state_code),
    );
    push_gauge_u64(
        &mut out,
        "patroni_postmaster_start_time",
        "Epoch seconds since PostgreSQL postmaster started.",
        &scope,
        snap.postmaster_start_time_epoch,
    );
    push_gauge_u64(
        &mut out,
        "patroni_master",
        "1 if this node is the active primary (legacy alias), 0 otherwise.",
        &scope,
        is_primary,
    );
    push_gauge_u64(
        &mut out,
        "patroni_primary",
        "1 if this node is the active primary, 0 otherwise.",
        &scope,
        is_primary,
    );
    push_gauge_u64(
        &mut out,
        "patroni_standby_leader",
        "1 if this node holds the standby_leader lock, 0 otherwise.",
        &scope,
        is_standby_leader,
    );
    push_gauge_u64(
        &mut out,
        "patroni_replica",
        "1 if this node has the replica role, 0 otherwise.",
        &scope,
        is_replica,
    );
    push_gauge_u64(
        &mut out,
        "patroni_sync_standby",
        "1 if this node is an active synchronous standby, 0 otherwise.",
        &scope,
        is_sync,
    );
    push_gauge_u64(
        &mut out,
        "patroni_quorum_standby",
        "1 if this node is an active quorum synchronous standby, 0 otherwise.",
        &scope,
        is_quorum,
    );
    push_gauge_u64(
        &mut out,
        "patroni_async_standby",
        "1 if this node is an asynchronous replica, 0 otherwise.",
        &scope,
        is_async,
    );
    push_gauge_u64(
        &mut out,
        "patroni_xlog_location",
        "Current WAL flush/write location on primary in bytes.",
        &scope,
        xlog_loc,
    );
    push_gauge_u64(
        &mut out,
        "patroni_xlog_received_location",
        "Last WAL location received on standby in bytes.",
        &scope,
        xlog_recv,
    );
    push_gauge_u64(
        &mut out,
        "patroni_xlog_replayed_location",
        "Last WAL location replayed on standby in bytes.",
        &scope,
        xlog_recv,
    );
    push_gauge_f64(
        &mut out,
        "patroni_xlog_replayed_timestamp",
        "Unix timestamp of the last replayed transaction on standby.",
        &scope,
        snap.xlog_replayed_timestamp_epoch,
    );
    push_gauge_u64(
        &mut out,
        "patroni_xlog_paused",
        "1 if WAL replay is paused on standby, 0 otherwise.",
        &scope,
        u64::from(snap.xlog_paused),
    );
    push_gauge_u64(
        &mut out,
        "patroni_postgres_timeline",
        "Current PostgreSQL timeline ID.",
        &scope,
        snap.timeline,
    );
    push_gauge_u64(
        &mut out,
        "patroni_dcs_last_seen",
        "Unix epoch timestamp when DCS was last contacted by the HA loop.",
        &scope,
        dcs_seen_epoch,
    );
    push_gauge_u64(
        &mut out,
        "patroni_cluster_unlocked",
        "1 when the cluster has no active leader lock, 0 otherwise.",
        &scope,
        u64::from(snap.cluster_unlocked),
    );
    push_gauge_u64(
        &mut out,
        "patroni_failsafe_mode_is_active",
        "1 when DCS failsafe mode is currently active, 0 otherwise.",
        &scope,
        u64::from(snap.failsafe_mode_is_active),
    );
    push_gauge_u64(
        &mut out,
        "patroni_is_paused",
        "1 when cluster maintenance mode (pause) is active, 0 otherwise.",
        &scope,
        u64::from(snap.pause),
    );
    push_gauge_u64(
        &mut out,
        "patroni_pending_restart",
        "1 when PostgreSQL has pending_restart GUC changes, 0 otherwise.",
        &scope,
        u64::from(snap.pending_restart),
    );

    out
}

pub async fn handle_metrics_get(State(state): State<ApiState>) -> Response {
    let snap = state.coordinator.load_published_state();
    let body = render_prometheus_metrics(&snap, Instant::now());
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)],
        body,
    )
        .into_response()
}
