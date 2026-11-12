use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use comfy_table::presets::{ASCII_FULL, NOTHING, UTF8_FULL};
use comfy_table::{Cell, ContentArrangement, Table};
use compact_str::CompactString;
use jiff::Timestamp;
use negroni_types::{ClusterSnapshot, HistoryEntry, MemberData};
use serde::Serialize;
use serde_json::Value as JsonValue;

use crate::OutputFormat;
use crate::edit_config::json_to_yaml_string;

#[derive(Debug, Clone, Serialize)]
pub struct MemberRowView {
    #[serde(rename = "Cluster")]
    pub cluster: String,
    #[serde(rename = "Member")]
    pub member: String,
    #[serde(rename = "Host")]
    pub host: String,
    #[serde(rename = "Role")]
    pub role: String,
    #[serde(rename = "State")]
    pub state: String,
    #[serde(rename = "TL", skip_serializing_if = "Option::is_none")]
    pub timeline: Option<u64>,
    #[serde(rename = "Lag in MB", skip_serializing_if = "Option::is_none")]
    pub lag_mb: Option<u64>,
    #[serde(rename = "Pending restart", skip_serializing_if = "std::ops::Not::not")]
    pub pending_restart: bool,
    #[serde(rename = "Tags", skip_serializing_if = "BTreeMap::is_empty")]
    pub tags: BTreeMap<CompactString, JsonValue>,
}

pub fn extract_host_display(conn_url: &str) -> String {
    let without_scheme = conn_url
        .split_once("://")
        .map_or(conn_url, |(_, rest)| rest);
    let authority = without_scheme
        .split_once('/')
        .map_or(without_scheme, |(auth, _)| auth);
    let host_port = authority.rsplit_once('@').map_or(authority, |(_, hp)| hp);
    if let Some((host, port)) = host_port.rsplit_once(':')
        && port == "5432"
    {
        return host.to_owned();
    }
    host_port.to_owned()
}

pub fn effective_member_role(
    name: &CompactString,
    member: &MemberData,
    snap: &ClusterSnapshot,
) -> &'static str {
    let is_leader = snap.leader.as_ref().is_some_and(|l| l.value == *name);
    if is_leader {
        if member.role == "standby_leader" {
            "Standby Leader"
        } else {
            "Leader"
        }
    } else if snap
        .sync
        .as_ref()
        .is_some_and(|s| s.value.contains_standby(name))
    {
        "Sync Standby"
    } else {
        "Replica"
    }
}

fn compute_leader_lsn(snap: &ClusterSnapshot) -> u64 {
    if let Some(ref st) = snap.status {
        return st.value.optime.0;
    }
    if let Some(ref ldr) = snap.leader
        && let Some(m) = snap.members.get(&ldr.value)
        && let Some(lsn) = m.value.xlog_location
    {
        return lsn.0;
    }
    0
}

fn build_row_view(
    scope: &str,
    display_member: String,
    raw_name: &CompactString,
    member: &MemberData,
    snap: &ClusterSnapshot,
    leader_lsn: u64,
) -> MemberRowView {
    let role = effective_member_role(raw_name, member, snap).to_owned();
    let is_leader = snap.leader.as_ref().is_some_and(|l| l.value == *raw_name);
    let lag_mb = if is_leader {
        None
    } else {
        member
            .xlog_location
            .map(|lsn| leader_lsn.saturating_sub(lsn.0) / (1024 * 1024))
    };

    MemberRowView {
        cluster: scope.to_owned(),
        member: display_member,
        host: extract_host_display(&member.conn_url),
        role,
        state: member.state.to_string(),
        timeline: member.timeline,
        lag_mb,
        pending_restart: member.pending_restart,
        tags: member.tags.clone(),
    }
}

fn format_member_rows(
    scope: &str,
    snap: &ClusterSnapshot,
    rows: &[MemberRowView],
    format: OutputFormat,
    extended: bool,
    show_timestamp: bool,
) -> String {
    match format {
        OutputFormat::Json => {
            serde_json::to_string_pretty(rows).unwrap_or_else(|_| "[]".to_owned())
        }
        OutputFormat::Yaml => {
            let val = serde_json::to_value(rows).unwrap_or(JsonValue::Array(Vec::new()));
            json_to_yaml_string(&val)
        }
        OutputFormat::Tsv => {
            let mut out = String::new();
            let _ = writeln!(
                out,
                "Cluster\tMember\tHost\tRole\tState\tTL\tLag in MB\tPending restart\tTags"
            );
            for r in rows {
                let tl = r.timeline.map_or_else(String::new, |t| t.to_string());
                let lag = r.lag_mb.map_or_else(String::new, |l| l.to_string());
                let pend = if r.pending_restart { "*" } else { "" };
                let tags = if r.tags.is_empty() {
                    String::new()
                } else {
                    serde_json::to_string(&r.tags).unwrap_or_default()
                };
                let _ = writeln!(
                    out,
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                    r.cluster, r.member, r.host, r.role, r.state, tl, lag, pend, tags
                );
            }
            out
        }
        OutputFormat::Plain | OutputFormat::Pretty => {
            let sysid = snap
                .initialize
                .as_ref()
                .map_or("uninitialized", |i| i.value.as_str());
            let show_pending = extended || rows.iter().any(|r| r.pending_restart);
            let show_tags = extended || rows.iter().any(|r| !r.tags.is_empty());

            let mut table = Table::new();
            match format {
                OutputFormat::Plain => {
                    table.load_preset(ASCII_FULL);
                }
                OutputFormat::Pretty => {
                    table.load_preset(UTF8_FULL);
                }
                _ => {
                    table.load_preset(NOTHING);
                }
            }
            table.set_content_arrangement(ContentArrangement::Dynamic);

            let mut headers = vec![
                Cell::new("Member"),
                Cell::new("Host"),
                Cell::new("Role"),
                Cell::new("State"),
                Cell::new("TL"),
                Cell::new("Lag in MB"),
            ];
            if show_pending {
                headers.push(Cell::new("Pending restart"));
            }
            if show_tags {
                headers.push(Cell::new("Tags"));
            }
            table.set_header(headers);

            for r in rows {
                let mut cells = vec![
                    Cell::new(&r.member),
                    Cell::new(&r.host),
                    Cell::new(&r.role),
                    Cell::new(&r.state),
                    Cell::new(r.timeline.map_or_else(String::new, |t| t.to_string())),
                    Cell::new(r.lag_mb.map_or_else(String::new, |l| l.to_string())),
                ];
                if show_pending {
                    cells.push(Cell::new(if r.pending_restart { "*" } else { "" }));
                }
                if show_tags {
                    let rendered_tags = if r.tags.is_empty() {
                        String::new()
                    } else {
                        serde_json::to_string(&r.tags).unwrap_or_default()
                    };
                    cells.push(Cell::new(rendered_tags));
                }
                table.add_row(cells);
            }

            let mut out = String::new();
            if show_timestamp {
                let _ = writeln!(out, "Timestamp: {}", Timestamp::now());
            }
            let _ = writeln!(out, "+ Cluster: {scope} ({sysid}) +");
            let _ = writeln!(out, "{table}");

            let paused = snap
                .config
                .as_ref()
                .and_then(|c| c.value.get("pause"))
                .and_then(JsonValue::as_bool)
                .unwrap_or(false);
            if paused {
                let _ = writeln!(out, " Maintenance mode: on");
            }
            if let Some(ref fo) = snap.failover
                && let Some(ref at) = fo.value.scheduled_at
            {
                let cand = fo.value.candidate.as_deref().unwrap_or("<auto>");
                let _ = writeln!(out, " Switchover scheduled at: {at} (to: {cand})");
            }
            out
        }
    }
}

/// Renders `negronictl list` / `patronictl list` output in `plain`, `pretty`, `tsv`, `json`, or `yaml`.
///
/// # Examples
///
/// ```
/// use negroni_cli::OutputFormat;
/// use negroni_cli::list::render_cluster_list;
/// use negroni_types::ClusterSnapshot;
///
/// let out = render_cluster_list("batman", &ClusterSnapshot::default(), OutputFormat::Plain, false, false);
/// assert!(out.contains("+ Cluster: batman (uninitialized) +"));
/// ```
pub fn render_cluster_list(
    scope: &str,
    snap: &ClusterSnapshot,
    format: OutputFormat,
    extended: bool,
    show_timestamp: bool,
) -> String {
    let leader_lsn = compute_leader_lsn(snap);
    let mut rows = Vec::with_capacity(snap.members.len());
    for (name, ver) in &snap.members {
        rows.push(build_row_view(
            scope,
            name.to_string(),
            name,
            &ver.value,
            snap,
            leader_lsn,
        ));
    }
    format_member_rows(scope, snap, &rows, format, extended, show_timestamp)
}

/// Renders `negronictl topology` cascading replication tree using `tags.replicatefrom` + leader roots.
pub fn render_topology(scope: &str, snap: &ClusterSnapshot, format: OutputFormat) -> String {
    let leader_lsn = compute_leader_lsn(snap);
    let leader_name = snap.leader.as_ref().map(|l| l.value.clone());

    let mut children_map: BTreeMap<CompactString, Vec<CompactString>> = BTreeMap::new();
    let mut roots: Vec<CompactString> = Vec::new();

    if let Some(ref ldr) = leader_name
        && snap.members.contains_key(ldr)
    {
        roots.push(ldr.clone());
    }

    for (name, ver) in &snap.members {
        if Some(name) == leader_name.as_ref() {
            continue;
        }
        let parent = ver
            .value
            .tags
            .get("replicatefrom")
            .and_then(JsonValue::as_str)
            .map(CompactString::from)
            .filter(|p| snap.members.contains_key(p))
            .or_else(|| leader_name.clone());

        match parent {
            Some(p) if p != *name => {
                children_map.entry(p).or_default().push(name.clone());
            }
            _ => {
                roots.push(name.clone());
            }
        }
    }

    struct TreeWalkCtx<'a> {
        scope: &'a str,
        snap: &'a ClusterSnapshot,
        children_map: &'a BTreeMap<CompactString, Vec<CompactString>>,
        visited: BTreeSet<CompactString>,
        rows: Vec<MemberRowView>,
        leader_lsn: u64,
    }

    fn walk_tree(node: &CompactString, depth: usize, ctx: &mut TreeWalkCtx<'_>) {
        if !ctx.visited.insert(node.clone()) {
            return;
        }
        if let Some(ver) = ctx.snap.members.get(node) {
            let label = if depth == 0 {
                node.to_string()
            } else {
                format!("{}+ {}", "  ".repeat(depth.saturating_sub(1)), node)
            };
            ctx.rows.push(build_row_view(
                ctx.scope,
                label,
                node,
                &ver.value,
                ctx.snap,
                ctx.leader_lsn,
            ));
        }
        if let Some(kids) = ctx.children_map.get(node) {
            for kid in kids {
                walk_tree(kid, depth + 1, ctx);
            }
        }
    }

    let mut walk_ctx = TreeWalkCtx {
        scope,
        snap,
        children_map: &children_map,
        visited: BTreeSet::new(),
        rows: Vec::with_capacity(snap.members.len()),
        leader_lsn,
    };

    for root in &roots {
        walk_tree(root, 0, &mut walk_ctx);
    }
    for leftover in snap.members.keys() {
        if !walk_ctx.visited.contains(leftover) {
            walk_tree(leftover, 0, &mut walk_ctx);
        }
    }

    format_member_rows(scope, snap, &walk_ctx.rows, format, false, false)
}

#[derive(Debug, Serialize)]
struct HistoryRowView {
    #[serde(rename = "TL")]
    tl: u64,
    #[serde(rename = "LSN")]
    lsn: String,
    #[serde(rename = "Reason")]
    reason: String,
    #[serde(rename = "Timestamp", skip_serializing_if = "Option::is_none")]
    timestamp: Option<String>,
}

/// Renders `negronictl history` timeline switch history in `plain`, `pretty`, `tsv`, `json`, or `yaml`.
pub fn render_history(entries: &[HistoryEntry], format: OutputFormat) -> String {
    let views: Vec<HistoryRowView> = entries
        .iter()
        .map(|e| HistoryRowView {
            tl: e.timeline,
            lsn: e.lsn.to_string(),
            reason: e.reason.clone(),
            timestamp: e.timestamp.clone(),
        })
        .collect();

    match format {
        OutputFormat::Json => {
            serde_json::to_string_pretty(&views).unwrap_or_else(|_| "[]".to_owned())
        }
        OutputFormat::Yaml => {
            let val = serde_json::to_value(&views).unwrap_or(JsonValue::Array(Vec::new()));
            json_to_yaml_string(&val)
        }
        OutputFormat::Tsv => {
            let mut out = String::from("TL\tLSN\tReason\tTimestamp\n");
            for v in &views {
                let _ = writeln!(
                    out,
                    "{}\t{}\t{}\t{}",
                    v.tl,
                    v.lsn,
                    v.reason,
                    v.timestamp.as_deref().unwrap_or("")
                );
            }
            out
        }
        OutputFormat::Plain | OutputFormat::Pretty => {
            let mut table = Table::new();
            if format == OutputFormat::Pretty {
                table.load_preset(UTF8_FULL);
            } else {
                table.load_preset(ASCII_FULL);
            }
            table.set_header(vec!["TL", "LSN", "Reason", "Timestamp"]);
            for v in &views {
                table.add_row(vec![
                    v.tl.to_string(),
                    v.lsn.clone(),
                    v.reason.clone(),
                    v.timestamp.clone().unwrap_or_default(),
                ]);
            }
            format!("{table}\n")
        }
    }
}
