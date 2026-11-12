use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;
use std::time::Duration;

use compact_str::CompactString;
use negroni_config::{BasicAuthCredentials, DaemonConfig};
use negroni_types::WalLsn;
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;

use crate::actor::SharedNodeState;
use crate::dispatcher::PeerRpcPort;
use crate::election::PeerElectionSample;

#[derive(Debug, Deserialize)]
struct PeerPatroniXlog {
    #[serde(default)]
    location: Option<u64>,
    #[serde(default)]
    received_location: Option<u64>,
    #[serde(default)]
    replayed_location: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct PeerPatroniPayload {
    #[serde(default)]
    state: CompactString,
    #[serde(default)]
    role: CompactString,
    #[serde(default)]
    pause: bool,
    #[serde(default)]
    failsafe_mode_is_active: bool,
    #[serde(default)]
    xlog: Option<PeerPatroniXlog>,
    #[serde(default)]
    tags: BTreeMap<CompactString, serde_json::Value>,
}

#[derive(Debug, Serialize)]
struct OutboundFailsafePayload<'a> {
    name: &'a str,
    conn_url: &'a str,
    api_url: &'a str,
    slots: BTreeMap<CompactString, u64>,
    xlog_location: u64,
}

/// Normalizes a member `api_url` (e.g. `http://10.0.0.2:8008/patroni`) and joins `path` (`/patroni` or `/failsafe`).
pub fn resolve_member_endpoint(api_url: &str, path: &str) -> String {
    let trimmed = api_url.trim().trim_end_matches('/');
    let base = trimmed.strip_suffix("/patroni").unwrap_or(trimmed);
    let clean_path = path.trim_start_matches('/');
    format!("{base}/{clean_path}")
}

/// Production HTTP/HTTPS peer RPC client implementing [`PeerRpcPort`] for pre-election LSN probes
/// (`GET /patroni`) and unanimous DCS-outage failsafe heartbeats (`POST /failsafe`).
#[derive(Clone, Debug)]
pub struct HttpPeerRpcClient {
    client: reqwest::Client,
    auth: Option<BasicAuthCredentials>,
    self_name: CompactString,
    self_conn_url: String,
    self_api_url: String,
    published_state: SharedNodeState,
}

impl HttpPeerRpcClient {
    pub fn from_daemon_config(
        cfg: &DaemonConfig,
        published_state: SharedNodeState,
    ) -> Result<Self, String> {
        let mut builder = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(2))
            .pool_idle_timeout(Duration::from_secs(30));

        if cfg.ctl.insecure {
            builder = builder.danger_accept_invalid_certs(true);
        }

        if let Some(ca_path) = cfg.ctl.cacert.as_ref().or(cfg.restapi.cacert.as_ref()) {
            let pem = fs::read(ca_path)
                .map_err(|e| format!("failed to read peer RPC CA '{}': {e}", ca_path.display()))?;
            let cert = reqwest::Certificate::from_pem(&pem)
                .map_err(|e| format!("invalid peer RPC CA PEM '{}': {e}", ca_path.display()))?;
            builder = builder.add_root_certificate(cert);
        }

        let cert_pair = cfg
            .ctl
            .certfile
            .as_ref()
            .zip(cfg.ctl.keyfile.as_ref())
            .or_else(|| {
                cfg.restapi
                    .certfile
                    .as_ref()
                    .zip(cfg.restapi.keyfile.as_ref())
            });
        if let Some((cert_path, key_path)) = cert_pair {
            let mut combined = fs::read(cert_path).map_err(|e| {
                format!(
                    "failed to read peer RPC client cert '{}': {e}",
                    cert_path.display()
                )
            })?;
            combined.push(b'\n');
            let key_bytes = fs::read(key_path).map_err(|e| {
                format!(
                    "failed to read peer RPC client key '{}': {e}",
                    key_path.display()
                )
            })?;
            combined.extend_from_slice(&key_bytes);
            let identity = reqwest::Identity::from_pem(&combined)
                .map_err(|e| format!("invalid peer RPC client identity PEM: {e}"))?;
            builder = builder.identity(identity);
        }

        let client = builder
            .build()
            .map_err(|e| format!("failed to build peer RPC HTTP client: {e}"))?;

        let conn_addr = cfg
            .postgresql
            .connect_address
            .as_deref()
            .unwrap_or(&cfg.postgresql.listen);
        let api_addr = cfg
            .restapi
            .connect_address
            .as_deref()
            .unwrap_or(&cfg.restapi.listen);
        let scheme = if cfg.restapi.certfile.is_some() {
            "https"
        } else {
            "http"
        };

        let auth = cfg
            .ctl
            .authentication
            .clone()
            .or_else(|| cfg.restapi.authentication.clone());

        Ok(Self {
            client,
            auth,
            self_name: cfg.name.clone(),
            self_conn_url: format!("postgres://{conn_addr}/postgres"),
            self_api_url: format!("{scheme}://{api_addr}/patroni"),
            published_state,
        })
    }

    async fn probe_single_peer(
        client: reqwest::Client,
        auth: Option<BasicAuthCredentials>,
        member_name: CompactString,
        api_url: String,
        timeout: Duration,
    ) -> Option<PeerElectionSample> {
        let url = resolve_member_endpoint(&api_url, "/patroni");
        let mut req = client.get(url).timeout(timeout);
        if let Some(ref creds) = auth {
            req = req.basic_auth(creds.username.as_str(), Some(creds.password.expose()));
        }
        let resp = req.send().await.ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let payload: PeerPatroniPayload = resp.json().await.ok()?;
        let raw_lsn = payload
            .xlog
            .as_ref()
            .and_then(|x| x.received_location.or(x.location).or(x.replayed_location))
            .unwrap_or(0);
        let explicit_nf = payload
            .tags
            .get("nofailover")
            .and_then(serde_json::Value::as_bool);
        let prio = payload
            .tags
            .get("failover_priority")
            .and_then(serde_json::Value::as_i64);
        let nofailover = explicit_nf.unwrap_or_else(|| prio.is_some_and(|p| p <= 0));
        let failover_priority = if explicit_nf == Some(true) {
            0
        } else {
            prio.unwrap_or(1)
        };
        let pg_ok = matches!(payload.state.as_str(), "running" | "streaming");
        let has_active_leader = matches!(payload.role.as_str(), "primary" | "master")
            || payload.failsafe_mode_is_active;

        Some(PeerElectionSample {
            member_name,
            lsn: WalLsn(raw_lsn),
            failover_priority,
            nofailover,
            can_promote: pg_ok && !nofailover && !payload.pause,
            has_active_leader,
        })
    }
}

impl PeerRpcPort for HttpPeerRpcClient {
    async fn probe_election_peers(
        &self,
        peers: &[(CompactString, String)],
        timeout: Duration,
    ) -> Vec<PeerElectionSample> {
        let mut set = JoinSet::new();
        for (name, api_url) in peers {
            let client = self.client.clone();
            let auth = self.auth.clone();
            let member = name.clone();
            let url = api_url.clone();
            set.spawn(
                async move { Self::probe_single_peer(client, auth, member, url, timeout).await },
            );
        }

        let mut samples = Vec::with_capacity(peers.len());
        while let Some(joined) = set.join_next().await {
            if let Ok(Some(sample)) = joined {
                samples.push(sample);
            }
        }
        samples
    }

    async fn probe_failsafe_peers(
        &self,
        peers: &[(CompactString, String)],
        timeout: Duration,
    ) -> bool {
        if peers.is_empty() {
            return false;
        }
        let current_lsn = self.published_state.load().xlog_location.0;
        let body = Arc::new(
            serde_json::to_vec(&OutboundFailsafePayload {
                name: self.self_name.as_str(),
                conn_url: &self.self_conn_url,
                api_url: &self.self_api_url,
                slots: BTreeMap::new(),
                xlog_location: current_lsn,
            })
            .unwrap_or_default(),
        );

        let mut set = JoinSet::new();
        for (_name, api_url) in peers {
            let client = self.client.clone();
            let auth = self.auth.clone();
            let payload = Arc::clone(&body);
            let url = resolve_member_endpoint(api_url, "/failsafe");
            set.spawn(async move {
                let mut req = client
                    .post(url)
                    .header("content-type", "application/json")
                    .body((*payload).clone())
                    .timeout(timeout);
                if let Some(ref creds) = auth {
                    req = req.basic_auth(creds.username.as_str(), Some(creds.password.expose()));
                }
                match req.send().await {
                    Ok(resp) => resp.status() == reqwest::StatusCode::OK,
                    Err(_) => false,
                }
            });
        }

        let mut accepted_count = 0usize;
        while let Some(joined) = set.join_next().await {
            match joined {
                Ok(true) => accepted_count += 1,
                _ => {
                    set.abort_all();
                    return false;
                }
            }
        }
        accepted_count == peers.len()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn resolve_member_endpoint_strips_patroni_suffix_cleanly() {
        assert_eq!(
            resolve_member_endpoint("http://10.0.0.2:8008/patroni", "/failsafe"),
            "http://10.0.0.2:8008/failsafe"
        );
        assert_eq!(
            resolve_member_endpoint("https://10.0.0.3:8008/", "/patroni"),
            "https://10.0.0.3:8008/patroni"
        );
    }
}
