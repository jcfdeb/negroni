use std::collections::BTreeMap;
use std::time::Duration;

use axum::Json;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use compact_str::CompactString;
use jiff::Timestamp;
use negroni_config::deep_merge_json;
use negroni_ha::{ControlCommand, ControlOpError, FailsafeHeartbeatRequest, ScheduledRestartInfo};
use negroni_types::{FailoverIntent, WalLsn};
use serde::Deserialize;
use serde_json::Value as JsonValue;

use crate::ApiState;

fn map_control_op_error(err: ControlOpError) -> Response {
    match err {
        ControlOpError::CasConflict | ControlOpError::Conflict(_) => {
            (StatusCode::CONFLICT, err.to_string()).into_response()
        }
        ControlOpError::InvalidInput(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
        ControlOpError::PreconditionFailed(msg) => {
            (StatusCode::PRECONDITION_FAILED, msg).into_response()
        }
        ControlOpError::Unavailable(msg) => (StatusCode::SERVICE_UNAVAILABLE, msg).into_response(),
    }
}

/// Checks whether an ISO-8601 timestamp carries an explicit UTC/offset suffix (`Z`, `+HH:MM`, `-HH:MM`).
pub fn has_explicit_timezone(ts: &str) -> bool {
    let trimmed = ts.trim();
    if trimmed.ends_with('Z') || trimmed.ends_with('z') {
        return true;
    }
    if let Some((_, time_part)) = trimmed.split_once('T').or_else(|| trimmed.split_once(' ')) {
        return time_part.contains('+') || time_part.contains('-');
    }
    false
}

/// Validates and parses a future ISO-8601 schedule timestamp, returning:
/// - `Err(StatusCode::BAD_REQUEST)` (`400`) when timezone info is missing,
/// - `Err(StatusCode::UNPROCESSABLE_ENTITY)` (`422`) when malformed or in the past.
pub fn parse_future_schedule(raw: &str) -> Result<Duration, (StatusCode, String)> {
    if !has_explicit_timezone(raw) {
        return Err((
            StatusCode::BAD_REQUEST,
            "Timezone information is mandatory for the scheduled operation".to_owned(),
        ));
    }
    let parsed: Timestamp = raw.trim().parse().map_err(|e| {
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("Unable to parse scheduled timestamp '{raw}': {e}"),
        )
    })?;
    let now = Timestamp::now();
    if parsed <= now {
        return Err((
            StatusCode::UNPROCESSABLE_ENTITY,
            "Cannot schedule operation in the past".to_owned(),
        ));
    }
    let delta = parsed.duration_since(now);
    let secs = u64::try_from(delta.as_secs()).unwrap_or(0);
    let nanos = delta.subsec_nanos().unsigned_abs();
    Ok(Duration::new(secs, nanos))
}

fn parse_pg_version_filter(raw: &str) -> Result<u32, String> {
    let trimmed = raw.trim();
    if let Ok(direct) = trimmed.parse::<u32>()
        && direct >= 100_000
    {
        return Ok(direct);
    }
    let mut parts = trimmed.split('.');
    let major: u32 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| format!("invalid postgres_version '{trimmed}'"))?;
    let minor: u32 = parts
        .next()
        .map_or(Ok(0), |s| s.parse())
        .map_err(|_| format!("invalid minor version in '{trimmed}'"))?;
    if parts.next().is_some() {
        return Err(format!("invalid postgres_version format '{trimmed}'"));
    }
    Ok(major * 10_000 + minor)
}

fn member_is_nofailover(tags: &BTreeMap<CompactString, JsonValue>) -> bool {
    if let Some(explicit) = tags.get("nofailover").and_then(JsonValue::as_bool) {
        return explicit;
    }
    tags.get("failover_priority")
        .and_then(JsonValue::as_i64)
        .is_some_and(|p| p <= 0)
}

pub async fn handle_config_patch(
    State(state): State<ApiState>,
    Json(patch): Json<JsonValue>,
) -> Response {
    if !patch.is_object() {
        return (
            StatusCode::BAD_REQUEST,
            "PATCH /config payload must be a JSON object",
        )
            .into_response();
    }

    let snap = match state
        .coordinator
        .fetch_fresh_cluster(Duration::from_secs(3))
        .await
    {
        Ok(s) => s,
        Err(err) => return map_control_op_error(err),
    };

    let expected_tok = snap.config.as_ref().map(|v| v.token.clone());
    let mut merged = snap
        .config
        .map(|v| v.value)
        .unwrap_or_else(|| state.default_dynamic_config.clone());
    deep_merge_json(&mut merged, &patch);

    if let Err(err) = state.daemon_cfg.with_dcs_dynamic_config(&merged) {
        return (StatusCode::BAD_REQUEST, err.to_string()).into_response();
    }

    let pause_toggle = merged.get("pause").and_then(JsonValue::as_bool);

    match state
        .coordinator
        .cas_config(merged.clone(), expected_tok)
        .await
    {
        Ok(_) => {
            if let Some(paused) = pause_toggle {
                let cmd = if paused {
                    ControlCommand::Pause
                } else {
                    ControlCommand::Resume
                };
                let _ = state.coordinator.submit_operator_command(cmd).await;
            }
            (StatusCode::OK, Json(merged)).into_response()
        }
        Err(err) => map_control_op_error(err),
    }
}

pub async fn handle_config_put(
    State(state): State<ApiState>,
    Json(replacement): Json<JsonValue>,
) -> Response {
    if !replacement.is_object() {
        return (
            StatusCode::BAD_REQUEST,
            "PUT /config payload must be a JSON object",
        )
            .into_response();
    }
    if let Err(err) = state.daemon_cfg.with_dcs_dynamic_config(&replacement) {
        return (StatusCode::BAD_REQUEST, err.to_string()).into_response();
    }

    let snap = match state
        .coordinator
        .fetch_fresh_cluster(Duration::from_secs(3))
        .await
    {
        Ok(s) => s,
        Err(err) => return map_control_op_error(err),
    };
    let expected_tok = snap.config.as_ref().map(|v| v.token.clone());

    match state
        .coordinator
        .cas_config(replacement.clone(), expected_tok)
        .await
    {
        Ok(_) => (StatusCode::OK, Json(replacement)).into_response(),
        Err(err) => map_control_op_error(err),
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct SwitchoverRequest {
    #[serde(default)]
    pub leader: Option<CompactString>,
    #[serde(default)]
    pub candidate: Option<CompactString>,
    #[serde(default)]
    pub scheduled_at: Option<String>,
}

pub async fn handle_switchover_post(
    State(state): State<ApiState>,
    Json(req): Json<SwitchoverRequest>,
) -> Response {
    let clean_leader = req.leader.filter(|s| !s.trim().is_empty());
    let clean_candidate = req.candidate.filter(|s| !s.trim().is_empty());
    let clean_schedule = req.scheduled_at.filter(|s| !s.trim().is_empty());

    if let Some(ref sched_str) = clean_schedule
        && let Err((code, msg)) = parse_future_schedule(sched_str)
    {
        return (code, msg).into_response();
    }

    let snap = match state
        .coordinator
        .fetch_fresh_cluster(Duration::from_secs(3))
        .await
    {
        Ok(s) => s,
        Err(err) => return map_control_op_error(err),
    };

    let Some(ref current_leader_ver) = snap.leader else {
        return (
            StatusCode::PRECONDITION_FAILED,
            "This cluster has no leader",
        )
            .into_response();
    };
    let current_leader = &current_leader_ver.value;

    if let Some(ref specified_leader) = clean_leader
        && specified_leader != current_leader
    {
        return (
            StatusCode::PRECONDITION_FAILED,
            "Cluster leader name does not match",
        )
            .into_response();
    }

    if let Some(ref cand) = clean_candidate {
        if cand == current_leader {
            return (
                StatusCode::BAD_REQUEST,
                "Switchover target and source are the same",
            )
                .into_response();
        }
        let Some(cand_member) = snap.members.get(cand) else {
            return (
                StatusCode::PRECONDITION_FAILED,
                format!("Candidate '{cand}' does not exist in cluster"),
            )
                .into_response();
        };
        if clean_schedule.is_none() {
            let healthy = matches!(cand_member.value.state.as_str(), "running" | "streaming")
                && !member_is_nofailover(&cand_member.value.tags);
            if !healthy {
                return (
                    StatusCode::PRECONDITION_FAILED,
                    format!("Switchover is not possible: candidate '{cand}' is not healthy"),
                )
                    .into_response();
            }
        }
    } else if clean_schedule.is_none() {
        // Immediate switchover with no explicit candidate requires at least one healthy replica.
        let any_healthy = snap.members.iter().any(|(name, m)| {
            name != current_leader
                && matches!(m.value.state.as_str(), "running" | "streaming")
                && !member_is_nofailover(&m.value.tags)
        });
        if !any_healthy {
            return (
                StatusCode::PRECONDITION_FAILED,
                "Switchover is not possible: cluster does not have healthy replicas",
            )
                .into_response();
        }
    }

    let intent = FailoverIntent {
        leader: Some(current_leader.clone()),
        candidate: clean_candidate.clone(),
        scheduled_at: clean_schedule.clone(),
    };
    let expected_fo = snap.failover.as_ref().map(|v| v.token.clone());
    if let Err(err) = state.coordinator.cas_failover(intent, expected_fo).await {
        return (StatusCode::SERVICE_UNAVAILABLE, err.to_string()).into_response();
    }

    if clean_schedule.is_some() {
        return (StatusCode::OK, "Switchover scheduled").into_response();
    }

    if state.coordinator.load_published_state().member_name == *current_leader {
        let _ = state
            .coordinator
            .submit_operator_command(ControlCommand::Switchover {
                candidate: clean_candidate,
            })
            .await;
    }

    (StatusCode::OK, "Successfully switched over").into_response()
}

#[derive(Debug, Default, Deserialize)]
pub struct FailoverRequest {
    #[serde(default)]
    pub leader: Option<CompactString>,
    #[serde(default)]
    pub candidate: Option<CompactString>,
}

pub async fn handle_failover_post(
    State(state): State<ApiState>,
    Json(req): Json<FailoverRequest>,
) -> Response {
    let clean_leader = req.leader.filter(|s| !s.trim().is_empty());
    let clean_candidate = req.candidate.filter(|s| !s.trim().is_empty());

    if clean_leader.is_none() && clean_candidate.is_none() {
        return (
            StatusCode::BAD_REQUEST,
            "Failover could be performed only to a specific candidate",
        )
            .into_response();
    }

    let snap = match state
        .coordinator
        .fetch_fresh_cluster(Duration::from_secs(3))
        .await
    {
        Ok(s) => s,
        Err(err) => return map_control_op_error(err),
    };

    if let Some(ref cand) = clean_candidate {
        let Some(cand_member) = snap.members.get(cand) else {
            return (
                StatusCode::PRECONDITION_FAILED,
                format!("Failover failed: candidate '{cand}' does not exist"),
            )
                .into_response();
        };
        if !matches!(cand_member.value.state.as_str(), "running" | "streaming") {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                format!("Failover failed: candidate '{cand}' is not running"),
            )
                .into_response();
        }
    }

    let intent = FailoverIntent {
        leader: clean_leader,
        candidate: clean_candidate.clone(),
        scheduled_at: None,
    };
    let expected_fo = snap.failover.as_ref().map(|v| v.token.clone());
    if let Err(err) = state.coordinator.cas_failover(intent, expected_fo).await {
        return (StatusCode::SERVICE_UNAVAILABLE, err.to_string()).into_response();
    }

    let _ = state
        .coordinator
        .submit_operator_command(ControlCommand::Failover {
            candidate: clean_candidate,
        })
        .await;

    (StatusCode::OK, "Successfully failed over").into_response()
}

#[derive(Debug, Default, Deserialize)]
pub struct RestartRequest {
    #[serde(default)]
    pub schedule: Option<String>,
    #[serde(default)]
    pub role: Option<CompactString>,
    #[serde(default)]
    pub postgres_version: Option<CompactString>,
    #[serde(default)]
    pub restart_pending: bool,
    #[serde(default)]
    pub timeout: Option<u64>,
    #[serde(default)]
    pub flush: bool,
}

pub async fn handle_restart_post(State(state): State<ApiState>, body: Bytes) -> Response {
    let req: RestartRequest = if body.is_empty() {
        RestartRequest::default()
    } else {
        match serde_json::from_slice(&body) {
            Ok(parsed) => parsed,
            Err(err) => {
                return (StatusCode::BAD_REQUEST, format!("Invalid JSON body: {err}"))
                    .into_response();
            }
        }
    };

    let local = state.coordinator.load_published_state();
    if let Some(ref expected_role) = req.role {
        let normalized = if expected_role == "master" || expected_role == "leader" {
            "primary"
        } else {
            expected_role.as_str()
        };
        if local.role.as_str() != normalized && local.role.as_str() != expected_role.as_str() {
            return (
                StatusCode::PRECONDITION_FAILED,
                format!("Node role is '{}', not '{expected_role}'", local.role),
            )
                .into_response();
        }
    }

    if let Some(ref pg_ver_str) = req.postgres_version {
        let threshold = match parse_pg_version_filter(pg_ver_str) {
            Ok(v) => v,
            Err(msg) => return (StatusCode::BAD_REQUEST, msg).into_response(),
        };
        if local.server_version_num >= threshold {
            return (
                StatusCode::PRECONDITION_FAILED,
                "PostgreSQL version is already up-to-date",
            )
                .into_response();
        }
    }

    if let Some(sched_str) = req.schedule.filter(|s| !s.trim().is_empty()) {
        let delay = match parse_future_schedule(&sched_str) {
            Ok(d) => d,
            Err((code, msg)) => return (code, msg).into_response(),
        };
        let info = ScheduledRestartInfo {
            schedule: sched_str,
            role: req.role,
            postgres_version: req.postgres_version,
            restart_pending: req.restart_pending,
            timeout: req.timeout,
        };
        drop(local);
        return match state.coordinator.schedule_restart(info, delay, req.flush) {
            Ok(()) => (StatusCode::ACCEPTED, "Restart scheduled").into_response(),
            Err(err) => map_control_op_error(err),
        };
    }

    if req.restart_pending && !local.pending_restart {
        return (StatusCode::OK, "postgres does not have pending restart").into_response();
    }

    let in_recovery = local.role != "primary";
    drop(local);
    let stop_timeout = Duration::from_secs(req.timeout.unwrap_or(60));
    match state
        .coordinator
        .restart_local_postgres(in_recovery, stop_timeout)
        .await
    {
        Ok(()) => (StatusCode::OK, "restarted successfully").into_response(),
        Err(err) => map_control_op_error(err),
    }
}

pub async fn handle_restart_delete(State(state): State<ApiState>) -> Response {
    if state.coordinator.cancel_scheduled_restart() {
        StatusCode::NO_CONTENT.into_response()
    } else {
        (
            StatusCode::NOT_FOUND,
            "Neither restart nor reinitialize is scheduled",
        )
            .into_response()
    }
}

pub async fn handle_reload_post(State(state): State<ApiState>) -> Response {
    match state.coordinator.reload_local_node().await {
        Ok(()) => (StatusCode::OK, "reload scheduled").into_response(),
        Err(err) => map_control_op_error(err),
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct ReinitializeRequest {
    #[serde(default)]
    pub force: bool,
}

pub async fn handle_reinitialize_post(State(state): State<ApiState>, body: Bytes) -> Response {
    let req: ReinitializeRequest = if body.is_empty() {
        ReinitializeRequest::default()
    } else {
        match serde_json::from_slice(&body) {
            Ok(parsed) => parsed,
            Err(err) => {
                return (StatusCode::BAD_REQUEST, format!("Invalid JSON body: {err}"))
                    .into_response();
            }
        }
    };

    let local = state.coordinator.load_published_state();
    if local.role == "primary" || local.holds_leader_lease {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "I am the leader, can not reinitialize",
        )
            .into_response();
    }
    drop(local);

    match state
        .coordinator
        .submit_operator_command(ControlCommand::Reinitialize { force: req.force })
        .await
    {
        Ok(()) => (StatusCode::OK, "reinitialize started").into_response(),
        Err(err) => (StatusCode::SERVICE_UNAVAILABLE, err.to_string()).into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub struct InboundFailsafeBody {
    pub name: CompactString,
    pub conn_url: String,
    #[serde(default)]
    pub api_url: Option<String>,
    #[serde(default)]
    pub slots: BTreeMap<CompactString, u64>,
    #[serde(default)]
    pub xlog_location: u64,
}

pub async fn handle_failsafe_post(
    State(state): State<ApiState>,
    Json(payload): Json<InboundFailsafeBody>,
) -> Response {
    let _ = (&payload.api_url, &payload.slots);
    if !state.coordinator.load_published_state().failsafe_enabled {
        return (StatusCode::BAD_GATEWAY, "Failsafe mode is not enabled").into_response();
    }

    let hb = FailsafeHeartbeatRequest {
        leader_name: payload.name,
        conn_url: payload.conn_url,
        lsn: WalLsn(payload.xlog_location),
    };
    match state
        .coordinator
        .submit_failsafe_heartbeat(hb, Duration::from_secs(2))
        .await
    {
        Ok(true) => (StatusCode::OK, "Accepted").into_response(),
        Ok(false) | Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "Rejected").into_response(),
    }
}
