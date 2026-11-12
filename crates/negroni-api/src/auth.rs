use std::net::{IpAddr, SocketAddr};

use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use base64::Engine as _;
use ipnet::IpNet;
use negroni_config::{BasicAuthCredentials, RestApiConfig};
use negroni_types::ClusterSnapshot;
use subtle::ConstantTimeEq as _;

use crate::ApiState;
use crate::probes::parse_conn_url_host_port;

/// Per-connection transport metadata injected by `serve_rest_api` into Axum request extensions.
#[derive(Debug, Clone, Copy)]
pub struct ConnectionPeerMeta {
    pub peer_addr: Option<SocketAddr>,
    pub client_cert_verified: bool,
}

#[derive(Debug, Clone)]
pub struct AuthPolicy {
    basic_auth: Option<BasicAuthCredentials>,
    static_allowlist: Vec<IpNet>,
    allowlist_include_members: bool,
    require_client_cert: bool,
}

impl AuthPolicy {
    pub fn from_restapi_config(cfg: &RestApiConfig) -> Result<Self, String> {
        let mut static_allowlist = Vec::with_capacity(cfg.allowlist.len());
        for entry in &cfg.allowlist {
            let trimmed = entry.trim();
            if trimmed.is_empty() {
                continue;
            }
            if let Ok(net) = trimmed.parse::<IpNet>() {
                static_allowlist.push(net);
            } else if let Ok(ip) = trimmed.parse::<IpAddr>() {
                static_allowlist.push(IpNet::from(ip));
            } else {
                return Err(format!(
                    "invalid IP or CIDR entry '{trimmed}' in restapi.allowlist"
                ));
            }
        }

        let require_client_cert = cfg
            .verify_client
            .as_deref()
            .is_some_and(|mode| mode.eq_ignore_ascii_case("required"));

        Ok(Self {
            basic_auth: cfg.authentication.clone(),
            static_allowlist,
            allowlist_include_members: cfg.allowlist_include_members,
            require_client_cert,
        })
    }

    pub fn verify_basic_header(&self, header_val: Option<&str>) -> bool {
        let Some(ref creds) = self.basic_auth else {
            return true;
        };
        let Some(raw_header) = header_val else {
            return false;
        };
        let Some(b64_part) = raw_header
            .trim()
            .strip_prefix("Basic ")
            .or_else(|| raw_header.trim().strip_prefix("basic "))
        else {
            return false;
        };
        let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(b64_part.trim()) else {
            return false;
        };

        let expected = format!("{}:{}", creds.username, creds.password.expose());
        constant_time_bytes_eq(expected.as_bytes(), &decoded)
    }

    pub fn is_peer_allowed(
        &self,
        peer_meta: Option<ConnectionPeerMeta>,
        cached_cluster: &ClusterSnapshot,
    ) -> bool {
        if self.require_client_cert && !peer_meta.is_some_and(|m| m.client_cert_verified) {
            return false;
        }

        if self.static_allowlist.is_empty() && !self.allowlist_include_members {
            return true;
        }

        let Some(sock) = peer_meta.and_then(|m| m.peer_addr) else {
            return false;
        };
        let peer_ip = canonical_ip(sock.ip());

        if self
            .static_allowlist
            .iter()
            .any(|net| net.contains(&peer_ip))
        {
            return true;
        }

        if self.allowlist_include_members {
            for member in cached_cluster.members.values() {
                let (api_host, _) = parse_conn_url_host_port(&member.value.api_url);
                if let Ok(ip) = api_host.parse::<IpAddr>()
                    && canonical_ip(ip) == peer_ip
                {
                    return true;
                }
                let (pg_host, _) = parse_conn_url_host_port(&member.value.conn_url);
                if let Ok(ip) = pg_host.parse::<IpAddr>()
                    && canonical_ip(ip) == peer_ip
                {
                    return true;
                }
            }
        }

        false
    }
}

fn canonical_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(IpAddr::V6(v6), IpAddr::V4),
        IpAddr::V4(v4) => IpAddr::V4(v4),
    }
}

/// Constant-time comparison over fixed 512-byte length-prefixed buffers so neither credential
/// content nor credential length leaks through timing side-channels.
///
/// # Examples
///
/// ```
/// use negroni_api::auth::constant_time_bytes_eq;
///
/// assert!(constant_time_bytes_eq(b"admin:secret", b"admin:secret"));
/// assert!(!constant_time_bytes_eq(b"admin:secret", b"admin:wrong"));
/// assert!(!constant_time_bytes_eq(b"admin:secret", b"admin"));
/// ```
pub fn constant_time_bytes_eq(expected: &[u8], provided: &[u8]) -> bool {
    const BUF_CAP: usize = 512;
    if expected.len() > BUF_CAP - 2 || provided.len() > BUF_CAP - 2 {
        return false;
    }

    let mut lhs = [0u8; BUF_CAP];
    let mut rhs = [0u8; BUF_CAP];

    let lhs_len = u16::try_from(expected.len()).unwrap_or(u16::MAX);
    let rhs_len = u16::try_from(provided.len()).unwrap_or(u16::MAX);
    lhs[..2].copy_from_slice(&lhs_len.to_be_bytes());
    rhs[..2].copy_from_slice(&rhs_len.to_be_bytes());
    lhs[2..2 + expected.len()].copy_from_slice(expected);
    rhs[2..2 + provided.len()].copy_from_slice(provided);

    bool::from(lhs.ct_eq(&rhs))
}

/// Axum middleware enforcing `AuthPolicy` (`401` on Basic Auth failure, `403` on IP/CIDR
/// allowlist or client-certificate mTLS rejection) across all mutating control & failsafe routes.
pub async fn auth_and_allowlist_middleware(
    State(state): State<ApiState>,
    req: Request,
    next: Next,
) -> Response {
    let peer_meta = req.extensions().get::<ConnectionPeerMeta>().copied();
    let cached_cluster = state.coordinator.load_cached_cluster();
    if !state
        .auth_policy
        .is_peer_allowed(peer_meta, &cached_cluster)
    {
        return (StatusCode::FORBIDDEN, "Forbidden").into_response();
    }
    drop(cached_cluster);

    let auth_hdr = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());
    if !state.auth_policy.verify_basic_header(auth_hdr) {
        return (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Basic realm=\"Patroni\"")],
            "Unauthorized",
        )
            .into_response();
    }

    next.run(req).await
}
