use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::Json;
use axum::extract::{Query, State};
use axum::http::{Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use compact_str::CompactString;
use jiff::Timestamp;
use negroni_ha::PublishedNodeState;
use negroni_types::ClusterSnapshot;
use serde::Serialize;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::ApiState;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeStatus {
    Ok200,
    Unavailable503,
}

impl ProbeStatus {
    pub const fn http_status(self) -> StatusCode {
        match self {
            Self::Ok200 => StatusCode::OK,
            Self::Unavailable503 => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

/// Parses a Patroni `?lag=` query value supporting raw byte integers (`1048576`) and
/// IEC/SI byte suffixes (`B`, `kB`/`KB`, `MB`, `GB`, `TB`, case-insensitive).
///
/// # Examples
///
/// ```
/// use negroni_api::probes::parse_lag_param;
///
/// assert_eq!(parse_lag_param("1048576")?, 1_048_576);
/// assert_eq!(parse_lag_param("16MB")?, 16 * 1024 * 1024);
/// assert_eq!(parse_lag_param("1GB")?, 1024 * 1024 * 1024);
/// assert!(parse_lag_param("invalid").is_err());
/// # Ok::<(), String>(())
/// ```
pub fn parse_lag_param(raw: &str) -> Result<u64, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("empty lag parameter".to_owned());
    }

    let split_idx = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (num_part, unit_part) = trimmed.split_at(split_idx);
    if num_part.is_empty() {
        return Err(format!("invalid lag value '{trimmed}'"));
    }

    let base: u64 = num_part
        .parse()
        .map_err(|_| format!("invalid integer in lag value '{trimmed}'"))?;
    let mult: u64 = match unit_part.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1_024,
        "m" | "mb" | "mib" => 1_024 * 1_024,
        "g" | "gb" | "gib" => 1_024 * 1_024 * 1_024,
        "t" | "tb" | "tib" => 1_024_u64 * 1_024 * 1_024 * 1_024,
        other => return Err(format!("unsupported lag unit '{other}'")),
    };

    base.checked_mul(mult)
        .ok_or_else(|| format!("lag parameter '{trimmed}' overflows u64"))
}

pub fn matches_query_tags(snapshot: &PublishedNodeState, query: &BTreeMap<String, String>) -> bool {
    for (key, expected_val) in query {
        if let Some(tag_key) = key.strip_prefix("tag_") {
            match snapshot.user_tags.get(tag_key) {
                Some(actual) if actual.as_str() == expected_val.as_str() => {}
                _ => return false,
            }
        }
    }
    true
}

fn parse_optional_query_lag(query: &BTreeMap<String, String>) -> Result<Option<u64>, StatusCode> {
    match query.get("lag").or_else(|| query.get("max-lag")) {
        None => Ok(None),
        Some(raw) => parse_lag_param(raw)
            .map(Some)
            .map_err(|_| StatusCode::BAD_REQUEST),
    }
}

pub fn evaluate_primary_probe(snapshot: &PublishedNodeState) -> ProbeStatus {
    if snapshot.role == "primary" && snapshot.state == "running" && snapshot.is_read_write_valid {
        ProbeStatus::Ok200
    } else {
        ProbeStatus::Unavailable503
    }
}

pub fn evaluate_leader_probe(snapshot: &PublishedNodeState) -> ProbeStatus {
    if snapshot.holds_leader_lease
        && (snapshot.role == "primary" || snapshot.role == "standby_leader")
    {
        ProbeStatus::Ok200
    } else {
        ProbeStatus::Unavailable503
    }
}

pub fn evaluate_standby_leader_probe(snapshot: &PublishedNodeState) -> ProbeStatus {
    if snapshot.holds_leader_lease
        && snapshot.role == "standby_leader"
        && snapshot.state == "running"
    {
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

pub fn evaluate_replica_probe_with_tags(
    snapshot: &PublishedNodeState,
    max_lag: Option<u64>,
    query: &BTreeMap<String, String>,
) -> ProbeStatus {
    if !matches_query_tags(snapshot, query) {
        return ProbeStatus::Unavailable503;
    }
    evaluate_replica_probe(snapshot, max_lag)
}

pub fn evaluate_read_only_probe(
    snapshot: &PublishedNodeState,
    max_lag: Option<u64>,
    query: &BTreeMap<String, String>,
) -> ProbeStatus {
    if evaluate_replica_probe_with_tags(snapshot, max_lag, query) == ProbeStatus::Ok200 {
        return ProbeStatus::Ok200;
    }
    if evaluate_leader_probe(snapshot) == ProbeStatus::Ok200
        && snapshot.state == "running"
        && !snapshot.noloadbalance
        && matches_query_tags(snapshot, query)
    {
        ProbeStatus::Ok200
    } else {
        ProbeStatus::Unavailable503
    }
}

pub fn evaluate_synchronous_probe(
    snapshot: &PublishedNodeState,
    max_lag: Option<u64>,
    query: &BTreeMap<String, String>,
) -> ProbeStatus {
    if snapshot.is_sync_standby
        && evaluate_replica_probe_with_tags(snapshot, max_lag, query) == ProbeStatus::Ok200
    {
        ProbeStatus::Ok200
    } else {
        ProbeStatus::Unavailable503
    }
}

pub fn evaluate_read_only_sync_probe(
    snapshot: &PublishedNodeState,
    max_lag: Option<u64>,
    query: &BTreeMap<String, String>,
) -> ProbeStatus {
    if (evaluate_primary_probe(snapshot) == ProbeStatus::Ok200
        && !snapshot.noloadbalance
        && matches_query_tags(snapshot, query))
        || evaluate_synchronous_probe(snapshot, max_lag, query) == ProbeStatus::Ok200
    {
        ProbeStatus::Ok200
    } else {
        ProbeStatus::Unavailable503
    }
}

pub fn evaluate_quorum_probe(
    snapshot: &PublishedNodeState,
    max_lag: Option<u64>,
    query: &BTreeMap<String, String>,
) -> ProbeStatus {
    if snapshot.is_quorum_standby
        && evaluate_replica_probe_with_tags(snapshot, max_lag, query) == ProbeStatus::Ok200
    {
        ProbeStatus::Ok200
    } else {
        ProbeStatus::Unavailable503
    }
}

pub fn evaluate_read_only_quorum_probe(
    snapshot: &PublishedNodeState,
    max_lag: Option<u64>,
    query: &BTreeMap<String, String>,
) -> ProbeStatus {
    if (evaluate_primary_probe(snapshot) == ProbeStatus::Ok200
        && !snapshot.noloadbalance
        && matches_query_tags(snapshot, query))
        || evaluate_quorum_probe(snapshot, max_lag, query) == ProbeStatus::Ok200
    {
        ProbeStatus::Ok200
    } else {
        ProbeStatus::Unavailable503
    }
}

pub fn evaluate_asynchronous_probe(
    snapshot: &PublishedNodeState,
    max_lag: Option<u64>,
    query: &BTreeMap<String, String>,
) -> ProbeStatus {
    if !snapshot.is_sync_standby
        && !snapshot.is_quorum_standby
        && evaluate_replica_probe_with_tags(snapshot, max_lag, query) == ProbeStatus::Ok200
    {
        ProbeStatus::Ok200
    } else {
        ProbeStatus::Unavailable503
    }
}

pub fn evaluate_health_probe(snapshot: &PublishedNodeState) -> ProbeStatus {
    if snapshot.state == "running" {
        ProbeStatus::Ok200
    } else {
        ProbeStatus::Unavailable503
    }
}

pub fn evaluate_liveness_probe(snapshot: &PublishedNodeState, now: Instant) -> ProbeStatus {
    if snapshot.pause {
        return ProbeStatus::Ok200;
    }
    let window = if snapshot.role == "replica" {
        snapshot.ttl.saturating_mul(2)
    } else {
        snapshot.ttl
    };
    if now.saturating_duration_since(snapshot.last_ha_tick) < window {
        ProbeStatus::Ok200
    } else {
        ProbeStatus::Unavailable503
    }
}

pub fn evaluate_readiness_probe(
    snapshot: &PublishedNodeState,
    max_lag: Option<u64>,
) -> ProbeStatus {
    if snapshot.holds_leader_lease {
        return ProbeStatus::Ok200;
    }
    if snapshot.state != "running" {
        return ProbeStatus::Unavailable503;
    }
    if let Some(limit) = max_lag
        && snapshot.replication_lag_bytes > limit
    {
        return ProbeStatus::Unavailable503;
    }
    ProbeStatus::Ok200
}

pub fn instant_to_unix_epoch_secs(target: Instant, now: Instant) -> u64 {
    let sys_now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_secs();
    let age = now.saturating_duration_since(target).as_secs();
    sys_now.saturating_sub(age)
}

/// Builds the wire-compatible Patroni `/patroni` JSON document from lock-free state.
pub fn build_patroni_payload(state: &ApiState, snap: &PublishedNodeState) -> JsonValue {
    let mut root = JsonMap::new();
    root.insert(
        "state".to_owned(),
        JsonValue::String(snap.state.to_string()),
    );
    if snap.postmaster_start_time_epoch > 0
        && let Ok(ts) =
            Timestamp::from_second(i64::try_from(snap.postmaster_start_time_epoch).unwrap_or(0))
    {
        root.insert(
            "postmaster_start_time".to_owned(),
            JsonValue::String(ts.to_string()),
        );
    }
    root.insert("role".to_owned(), JsonValue::String(snap.role.to_string()));
    root.insert(
        "server_version".to_owned(),
        JsonValue::Number(snap.server_version_num.into()),
    );

    let mut xlog = JsonMap::new();
    if snap.role == "primary" {
        xlog.insert(
            "location".to_owned(),
            JsonValue::Number(snap.xlog_location.0.into()),
        );
    } else {
        xlog.insert(
            "received_location".to_owned(),
            JsonValue::Number(snap.xlog_location.0.into()),
        );
        xlog.insert(
            "replayed_location".to_owned(),
            JsonValue::Number(snap.xlog_location.0.into()),
        );
        xlog.insert("paused".to_owned(), JsonValue::Bool(snap.xlog_paused));
        if snap.xlog_replayed_timestamp_epoch > 0.0
            && let Some(n) = serde_json::Number::from_f64(snap.xlog_replayed_timestamp_epoch)
        {
            xlog.insert("replayed_timestamp".to_owned(), JsonValue::Number(n));
        }
    }
    root.insert("xlog".to_owned(), JsonValue::Object(xlog));

    if snap.is_sync_standby {
        root.insert("sync_standby".to_owned(), JsonValue::Bool(true));
    }
    if snap.is_quorum_standby {
        root.insert("quorum_standby".to_owned(), JsonValue::Bool(true));
    }
    if snap.role == "replica" && snap.state == "running" {
        root.insert(
            "replication_state".to_owned(),
            JsonValue::String("streaming".to_owned()),
        );
    }
    if snap.timeline > 0 {
        root.insert(
            "timeline".to_owned(),
            JsonValue::Number(snap.timeline.into()),
        );
    }
    if let Some(ref sysid) = snap.database_system_identifier {
        root.insert(
            "database_system_identifier".to_owned(),
            JsonValue::String(sysid.to_string()),
        );
    }
    if snap.pending_restart {
        root.insert("pending_restart".to_owned(), JsonValue::Bool(true));
    }
    if snap.pause {
        root.insert("pause".to_owned(), JsonValue::Bool(true));
    }
    if snap.failsafe_mode_is_active {
        root.insert("failsafe_mode_is_active".to_owned(), JsonValue::Bool(true));
    }
    if let Some(dcs_at) = snap.dcs_last_seen {
        let epoch = instant_to_unix_epoch_secs(dcs_at, Instant::now());
        root.insert("dcs_last_seen".to_owned(), JsonValue::Number(epoch.into()));
    }
    if !snap.user_tags.is_empty() || snap.noloadbalance {
        let mut tags = JsonMap::new();
        if snap.noloadbalance {
            tags.insert("noloadbalance".to_owned(), JsonValue::Bool(true));
        }
        for (k, v) in &snap.user_tags {
            tags.insert(k.to_string(), JsonValue::String(v.to_string()));
        }
        root.insert("tags".to_owned(), JsonValue::Object(tags));
    }
    if let Some(ref sched) = **state.coordinator.load_scheduled_restart()
        && let Ok(val) = serde_json::to_value(sched)
    {
        root.insert("scheduled_restart".to_owned(), val);
    }

    let mut patroni_meta = JsonMap::new();
    patroni_meta.insert("version".to_owned(), JsonValue::String("4.1.0".to_owned()));
    patroni_meta.insert(
        "scope".to_owned(),
        JsonValue::String(snap.scope.to_string()),
    );
    patroni_meta.insert(
        "name".to_owned(),
        JsonValue::String(snap.member_name.to_string()),
    );
    root.insert("patroni".to_owned(), JsonValue::Object(patroni_meta));

    JsonValue::Object(root)
}

fn render_probe_response(
    method: Method,
    probe: ProbeStatus,
    state: &ApiState,
    snap: &PublishedNodeState,
) -> Response {
    let status = probe.http_status();
    match method {
        Method::HEAD => status.into_response(),
        Method::OPTIONS => (status, [(header::ALLOW, "GET, HEAD, OPTIONS")], "").into_response(),
        _ => (status, Json(build_patroni_payload(state, snap))).into_response(),
    }
}

pub async fn handle_primary_probe(method: Method, State(state): State<ApiState>) -> Response {
    let snap = state.coordinator.load_published_state();
    render_probe_response(method, evaluate_primary_probe(&snap), &state, &snap)
}

pub async fn handle_leader_probe(method: Method, State(state): State<ApiState>) -> Response {
    let snap = state.coordinator.load_published_state();
    render_probe_response(method, evaluate_leader_probe(&snap), &state, &snap)
}

pub async fn handle_standby_leader_probe(
    method: Method,
    State(state): State<ApiState>,
) -> Response {
    let snap = state.coordinator.load_published_state();
    render_probe_response(method, evaluate_standby_leader_probe(&snap), &state, &snap)
}

pub async fn handle_replica_probe(
    method: Method,
    State(state): State<ApiState>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let max_lag = match parse_optional_query_lag(&query) {
        Ok(v) => v,
        Err(code) => return code.into_response(),
    };
    let snap = state.coordinator.load_published_state();
    render_probe_response(
        method,
        evaluate_replica_probe_with_tags(&snap, max_lag, &query),
        &state,
        &snap,
    )
}

pub async fn handle_read_only_probe(
    method: Method,
    State(state): State<ApiState>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let max_lag = match parse_optional_query_lag(&query) {
        Ok(v) => v,
        Err(code) => return code.into_response(),
    };
    let snap = state.coordinator.load_published_state();
    render_probe_response(
        method,
        evaluate_read_only_probe(&snap, max_lag, &query),
        &state,
        &snap,
    )
}

pub async fn handle_synchronous_probe(
    method: Method,
    State(state): State<ApiState>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let max_lag = match parse_optional_query_lag(&query) {
        Ok(v) => v,
        Err(code) => return code.into_response(),
    };
    let snap = state.coordinator.load_published_state();
    render_probe_response(
        method,
        evaluate_synchronous_probe(&snap, max_lag, &query),
        &state,
        &snap,
    )
}

pub async fn handle_read_only_sync_probe(
    method: Method,
    State(state): State<ApiState>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let max_lag = match parse_optional_query_lag(&query) {
        Ok(v) => v,
        Err(code) => return code.into_response(),
    };
    let snap = state.coordinator.load_published_state();
    render_probe_response(
        method,
        evaluate_read_only_sync_probe(&snap, max_lag, &query),
        &state,
        &snap,
    )
}

pub async fn handle_quorum_probe(
    method: Method,
    State(state): State<ApiState>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let max_lag = match parse_optional_query_lag(&query) {
        Ok(v) => v,
        Err(code) => return code.into_response(),
    };
    let snap = state.coordinator.load_published_state();
    render_probe_response(
        method,
        evaluate_quorum_probe(&snap, max_lag, &query),
        &state,
        &snap,
    )
}

pub async fn handle_read_only_quorum_probe(
    method: Method,
    State(state): State<ApiState>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let max_lag = match parse_optional_query_lag(&query) {
        Ok(v) => v,
        Err(code) => return code.into_response(),
    };
    let snap = state.coordinator.load_published_state();
    render_probe_response(
        method,
        evaluate_read_only_quorum_probe(&snap, max_lag, &query),
        &state,
        &snap,
    )
}

pub async fn handle_asynchronous_probe(
    method: Method,
    State(state): State<ApiState>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let max_lag = match parse_optional_query_lag(&query) {
        Ok(v) => v,
        Err(code) => return code.into_response(),
    };
    let snap = state.coordinator.load_published_state();
    render_probe_response(
        method,
        evaluate_asynchronous_probe(&snap, max_lag, &query),
        &state,
        &snap,
    )
}

pub async fn handle_health_probe(method: Method, State(state): State<ApiState>) -> Response {
    let snap = state.coordinator.load_published_state();
    render_probe_response(method, evaluate_health_probe(&snap), &state, &snap)
}

pub async fn handle_liveness_probe(method: Method, State(state): State<ApiState>) -> Response {
    let snap = state.coordinator.load_published_state();
    render_probe_response(
        method,
        evaluate_liveness_probe(&snap, Instant::now()),
        &state,
        &snap,
    )
}

pub async fn handle_readiness_probe(
    method: Method,
    State(state): State<ApiState>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let max_lag = match parse_optional_query_lag(&query) {
        Ok(v) => v,
        Err(code) => return code.into_response(),
    };
    let snap = state.coordinator.load_published_state();
    render_probe_response(
        method,
        evaluate_readiness_probe(&snap, max_lag),
        &state,
        &snap,
    )
}

pub async fn handle_patroni_get(method: Method, State(state): State<ApiState>) -> Response {
    let snap = state.coordinator.load_published_state();
    render_probe_response(method, ProbeStatus::Ok200, &state, &snap)
}

#[derive(Debug, Serialize)]
struct ClusterMemberEntry {
    name: CompactString,
    role: CompactString,
    state: CompactString,
    api_url: String,
    host: String,
    port: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeline: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lag: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pending_restart: Option<bool>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    tags: BTreeMap<CompactString, JsonValue>,
}

#[derive(Debug, Serialize)]
struct ClusterResponse {
    members: Vec<ClusterMemberEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pause: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    scheduled_switchover: Option<JsonValue>,
}

pub fn parse_conn_url_host_port(conn_url: &str) -> (String, u16) {
    let without_scheme = conn_url
        .split_once("://")
        .map_or(conn_url, |(_, rest)| rest);
    let authority = without_scheme
        .split_once('/')
        .map_or(without_scheme, |(auth, _)| auth);
    let host_port = authority.rsplit_once('@').map_or(authority, |(_, hp)| hp);

    if let Some(bracket_rest) = host_port.strip_prefix('[')
        && let Some((ipv6, after)) = bracket_rest.split_once(']')
    {
        let port = after
            .strip_prefix(':')
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(5432);
        return (ipv6.to_owned(), port);
    }

    if let Some((h, p_str)) = host_port.rsplit_once(':')
        && let Ok(port) = p_str.parse::<u16>()
    {
        return (h.to_owned(), port);
    }
    (host_port.to_owned(), 5432)
}

pub fn build_cluster_json(cluster: &ClusterSnapshot, local: &PublishedNodeState) -> JsonValue {
    let leader_name = cluster.leader.as_ref().map(|l| &l.value);
    let leader_lsn = cluster
        .status
        .as_ref()
        .map(|s| s.value.optime.0)
        .or_else(|| {
            leader_name
                .and_then(|ln| cluster.members.get(ln))
                .and_then(|m| m.value.xlog_location.map(|x| x.0))
        })
        .unwrap_or(local.xlog_location.0);

    let mut members = Vec::with_capacity(cluster.members.len());
    for (name, ver_member) in &cluster.members {
        let m = &ver_member.value;
        let (host, port) = parse_conn_url_host_port(&m.conn_url);
        let is_leader = leader_name == Some(name);
        let role = if is_leader {
            if m.role == "standby_leader" {
                CompactString::const_new("standby_leader")
            } else {
                CompactString::const_new("leader")
            }
        } else if cluster
            .sync
            .as_ref()
            .is_some_and(|s| s.value.contains_standby(name))
        {
            CompactString::const_new("sync_standby")
        } else {
            CompactString::const_new("replica")
        };

        let lag = if is_leader {
            None
        } else {
            m.xlog_location.map(|lsn| leader_lsn.saturating_sub(lsn.0))
        };

        members.push(ClusterMemberEntry {
            name: name.clone(),
            role,
            state: m.state.clone(),
            api_url: m.api_url.clone(),
            host,
            port,
            timeline: m.timeline,
            lag,
            pending_restart: m.pending_restart.then_some(true),
            tags: m.tags.clone(),
        });
    }

    let pause_cfg = cluster
        .config
        .as_ref()
        .and_then(|c| c.value.get("pause"))
        .and_then(JsonValue::as_bool)
        .unwrap_or(local.pause);

    let scheduled_switchover = cluster.failover.as_ref().and_then(|fo| {
        fo.value.scheduled_at.as_ref().map(|at| {
            let mut m = JsonMap::new();
            m.insert("at".to_owned(), JsonValue::String(at.clone()));
            if let Some(ref ldr) = fo.value.leader {
                m.insert("from".to_owned(), JsonValue::String(ldr.to_string()));
            }
            if let Some(ref cand) = fo.value.candidate {
                m.insert("to".to_owned(), JsonValue::String(cand.to_string()));
            }
            JsonValue::Object(m)
        })
    });

    serde_json::to_value(ClusterResponse {
        members,
        pause: pause_cfg.then_some(true),
        scheduled_switchover,
    })
    .unwrap_or_else(|_| JsonValue::Object(JsonMap::new()))
}

pub async fn handle_cluster_get(State(state): State<ApiState>) -> Response {
    let snap = match state
        .coordinator
        .fetch_fresh_cluster(Duration::from_secs(2))
        .await
    {
        Ok(fresh) => fresh,
        Err(_) => (**state.coordinator.load_cached_cluster()).clone(),
    };
    let local = state.coordinator.load_published_state();
    (StatusCode::OK, Json(build_cluster_json(&snap, &local))).into_response()
}

pub async fn handle_config_get(State(state): State<ApiState>) -> Response {
    let snap = match state
        .coordinator
        .fetch_fresh_cluster(Duration::from_secs(2))
        .await
    {
        Ok(fresh) => fresh,
        Err(_) => (**state.coordinator.load_cached_cluster()).clone(),
    };
    let val = snap
        .config
        .map(|v| v.value)
        .unwrap_or_else(|| state.default_dynamic_config.clone());
    (StatusCode::OK, Json(val)).into_response()
}

pub async fn handle_history_get(State(state): State<ApiState>) -> Response {
    let snap = match state
        .coordinator
        .fetch_fresh_cluster(Duration::from_secs(2))
        .await
    {
        Ok(fresh) => fresh,
        Err(_) => (**state.coordinator.load_cached_cluster()).clone(),
    };
    (StatusCode::OK, Json(snap.history)).into_response()
}
