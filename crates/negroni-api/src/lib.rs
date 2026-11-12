#![forbid(unsafe_code)]

pub mod auth;
pub mod control;
pub mod metrics;
pub mod probes;

use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use axum::Router;
use axum::extract::Request;
use axum::middleware;
use axum::routing::{delete, get, patch, post, put};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use negroni_config::{DaemonConfig, RestApiConfig};
use negroni_ha::CoordinatorHandle;
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use serde_json::{Map as JsonMap, Value as JsonValue};
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;

pub use crate::auth::{AuthPolicy, ConnectionPeerMeta, constant_time_bytes_eq};
pub use crate::metrics::{PROMETHEUS_CONTENT_TYPE, render_prometheus_metrics};
pub use crate::probes::{
    ProbeStatus, evaluate_asynchronous_probe, evaluate_health_probe, evaluate_leader_probe,
    evaluate_liveness_probe, evaluate_primary_probe, evaluate_quorum_probe,
    evaluate_read_only_probe, evaluate_read_only_quorum_probe, evaluate_read_only_sync_probe,
    evaluate_readiness_probe, evaluate_replica_probe, evaluate_standby_leader_probe,
    evaluate_synchronous_probe, parse_lag_param,
};

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("REST API configuration error: {0}")]
    Config(String),
    #[error("failed to bind REST API listener on '{addr}': {source}")]
    Bind {
        addr: String,
        #[source]
        source: std::io::Error,
    },
    #[error("TLS certificate/key error: {0}")]
    Tls(String),
}

#[derive(Clone)]
pub struct ApiState {
    pub coordinator: CoordinatorHandle,
    pub daemon_cfg: Arc<DaemonConfig>,
    pub default_dynamic_config: JsonValue,
    pub auth_policy: Arc<AuthPolicy>,
}

impl ApiState {
    pub fn from_config(
        daemon_cfg: DaemonConfig,
        coordinator: CoordinatorHandle,
    ) -> Result<Self, ApiError> {
        let auth_policy =
            AuthPolicy::from_restapi_config(&daemon_cfg.restapi).map_err(ApiError::Config)?;
        let default_dynamic_config = serde_json::to_value(&daemon_cfg.dynamic)
            .unwrap_or_else(|_| JsonValue::Object(JsonMap::new()));
        Ok(Self {
            coordinator,
            daemon_cfg: Arc::new(daemon_cfg),
            default_dynamic_config,
            auth_policy: Arc::new(auth_policy),
        })
    }
}

/// Builds the complete Patroni 4.1 wire-compatible Axum [`Router`] with lock-free `GET`/`HEAD`/`OPTIONS`
/// probe endpoints and `AuthAndAllowlistLayer`-guarded control & failsafe routes.
pub fn build_router(state: ApiState) -> Router {
    let probe_routes = Router::new()
        .route(
            "/",
            get(probes::handle_primary_probe).options(probes::handle_primary_probe),
        )
        .route(
            "/primary",
            get(probes::handle_primary_probe).options(probes::handle_primary_probe),
        )
        .route(
            "/master",
            get(probes::handle_primary_probe).options(probes::handle_primary_probe),
        )
        .route(
            "/read-write",
            get(probes::handle_primary_probe).options(probes::handle_primary_probe),
        )
        .route(
            "/leader",
            get(probes::handle_leader_probe).options(probes::handle_leader_probe),
        )
        .route(
            "/standby-leader",
            get(probes::handle_standby_leader_probe).options(probes::handle_standby_leader_probe),
        )
        .route(
            "/standby_leader",
            get(probes::handle_standby_leader_probe).options(probes::handle_standby_leader_probe),
        )
        .route(
            "/replica",
            get(probes::handle_replica_probe).options(probes::handle_replica_probe),
        )
        .route(
            "/read-only",
            get(probes::handle_read_only_probe).options(probes::handle_read_only_probe),
        )
        .route(
            "/synchronous",
            get(probes::handle_synchronous_probe).options(probes::handle_synchronous_probe),
        )
        .route(
            "/sync",
            get(probes::handle_synchronous_probe).options(probes::handle_synchronous_probe),
        )
        .route(
            "/read-only-sync",
            get(probes::handle_read_only_sync_probe).options(probes::handle_read_only_sync_probe),
        )
        .route(
            "/quorum",
            get(probes::handle_quorum_probe).options(probes::handle_quorum_probe),
        )
        .route(
            "/read-only-quorum",
            get(probes::handle_read_only_quorum_probe)
                .options(probes::handle_read_only_quorum_probe),
        )
        .route(
            "/asynchronous",
            get(probes::handle_asynchronous_probe).options(probes::handle_asynchronous_probe),
        )
        .route(
            "/async",
            get(probes::handle_asynchronous_probe).options(probes::handle_asynchronous_probe),
        )
        .route(
            "/health",
            get(probes::handle_health_probe).options(probes::handle_health_probe),
        )
        .route(
            "/liveness",
            get(probes::handle_liveness_probe).options(probes::handle_liveness_probe),
        )
        .route(
            "/readiness",
            get(probes::handle_readiness_probe).options(probes::handle_readiness_probe),
        )
        .route(
            "/patroni",
            get(probes::handle_patroni_get).options(probes::handle_patroni_get),
        )
        .route("/cluster", get(probes::handle_cluster_get))
        .route("/config", get(probes::handle_config_get))
        .route("/history", get(probes::handle_history_get))
        .route("/metrics", get(metrics::handle_metrics_get));

    let guarded_routes = Router::new()
        .route("/config", patch(control::handle_config_patch))
        .route("/config", put(control::handle_config_put))
        .route("/switchover", post(control::handle_switchover_post))
        .route("/failover", post(control::handle_failover_post))
        .route("/restart", post(control::handle_restart_post))
        .route("/restart", delete(control::handle_restart_delete))
        .route("/reload", post(control::handle_reload_post))
        .route("/reinitialize", post(control::handle_reinitialize_post))
        .route("/failsafe", post(control::handle_failsafe_post))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth::auth_and_allowlist_middleware,
        ));

    probe_routes.merge(guarded_routes).with_state(state)
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, ApiError> {
    let file = File::open(path)
        .map_err(|e| ApiError::Tls(format!("failed to open '{}': {e}", path.display())))?;
    let mut reader = BufReader::new(file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| ApiError::Tls(format!("invalid PEM cert in '{}': {e}", path.display())))?;
    if certs.is_empty() {
        return Err(ApiError::Tls(format!(
            "no PEM certificates found in '{}'",
            path.display()
        )));
    }
    Ok(certs)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, ApiError> {
    let file = File::open(path)
        .map_err(|e| ApiError::Tls(format!("failed to open '{}': {e}", path.display())))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| ApiError::Tls(format!("invalid PEM key in '{}': {e}", path.display())))?
        .ok_or_else(|| ApiError::Tls(format!("no PEM private key found in '{}'", path.display())))
}

pub fn build_rustls_server_config(
    cfg: &RestApiConfig,
) -> Result<Option<Arc<ServerConfig>>, ApiError> {
    let Some(ref cert_path) = cfg.certfile else {
        return Ok(None);
    };
    let key_path = cfg.keyfile.as_ref().ok_or_else(|| {
        ApiError::Tls("restapi.certfile is set, but restapi.keyfile is missing".to_owned())
    })?;

    let certs = load_certs(cert_path)?;
    let key = load_private_key(key_path)?;
    let verify_mode = cfg
        .verify_client
        .as_deref()
        .unwrap_or("none")
        .to_ascii_lowercase();

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| ApiError::Tls(format!("rustls protocol setup failed: {e}")))?;

    let server_cfg = match verify_mode.as_str() {
        "none" | "" => builder
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| ApiError::Tls(format!("invalid server cert/key pair: {e}")))?,
        "optional" | "required" => {
            let ca_path = cfg.cacert.as_ref().ok_or_else(|| {
                ApiError::Tls(format!(
                    "restapi.verify_client='{verify_mode}' requires restapi.cacert"
                ))
            })?;
            let ca_certs = load_certs(ca_path)?;
            let mut roots = RootCertStore::empty();
            for ca in ca_certs {
                roots
                    .add(ca)
                    .map_err(|e| ApiError::Tls(format!("invalid CA cert: {e}")))?;
            }
            let verifier_builder =
                WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider);
            let verifier = if verify_mode == "optional" {
                verifier_builder.allow_unauthenticated().build()
            } else {
                verifier_builder.build()
            }
            .map_err(|e| ApiError::Tls(format!("failed to build client cert verifier: {e}")))?;

            builder
                .with_client_cert_verifier(verifier)
                .with_single_cert(certs, key)
                .map_err(|e| ApiError::Tls(format!("invalid server cert/key pair: {e}")))?
        }
        other => {
            return Err(ApiError::Config(format!(
                "unsupported restapi.verify_client mode '{other}' (expected none, optional, or required)"
            )));
        }
    };

    Ok(Some(Arc::new(server_cfg)))
}

async fn serve_connection<I>(io: I, app: Router, meta: ConnectionPeerMeta)
where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let hyper_svc = service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
        let router = app.clone();
        async move {
            let (mut parts, body) = req.into_parts();
            parts.extensions.insert(meta);
            let axum_req = Request::from_parts(parts, axum::body::Body::new(body));
            let resp = match tower::ServiceExt::oneshot(router, axum_req).await {
                Ok(r) => r,
                Err(never) => match never {},
            };
            Ok::<_, std::convert::Infallible>(resp)
        }
    });

    let _ = hyper::server::conn::http1::Builder::new()
        .keep_alive(true)
        .serve_connection(TokioIo::new(io), hyper_svc)
        .await;
}

/// Binds `cfg.restapi.listen` and serves the Negroni HTTP / HTTPS / mTLS REST API until
/// `shutdown_rx` signals `true`.
pub async fn serve_rest_api(
    cfg: DaemonConfig,
    coordinator: CoordinatorHandle,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<(), ApiError> {
    let listen_addr = cfg.restapi.listen.clone();
    let listener = TcpListener::bind(&listen_addr)
        .await
        .map_err(|source| ApiError::Bind {
            addr: listen_addr,
            source,
        })?;
    serve_rest_api_with_listener(listener, cfg, coordinator, shutdown_rx).await
}

pub async fn serve_rest_api_with_listener(
    listener: TcpListener,
    cfg: DaemonConfig,
    coordinator: CoordinatorHandle,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<(), ApiError> {
    let tls_acceptor = build_rustls_server_config(&cfg.restapi)?.map(TlsAcceptor::from);
    let state = ApiState::from_config(cfg, coordinator)?;
    let router = build_router(state);

    if let Ok(local_addr) = listener.local_addr() {
        tracing::info!(
            listen = %local_addr,
            tls = tls_acceptor.is_some(),
            "negroni REST API listening"
        );
    }

    loop {
        let accepted = tokio::select! {
            biased;
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
                continue;
            }
            res = listener.accept() => res,
        };

        let (tcp_stream, peer_addr): (_, SocketAddr) = match accepted {
            Ok(pair) => pair,
            Err(err) => {
                tracing::warn!(error = %err, "TCP accept error on REST API listener");
                continue;
            }
        };
        let _ = tcp_stream.set_nodelay(true);

        let app = router.clone();
        if let Some(ref acceptor) = tls_acceptor {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                if let Ok(tls_stream) = acceptor.accept(tcp_stream).await {
                    let (_, server_conn) = tls_stream.get_ref();
                    let cert_verified = server_conn
                        .peer_certificates()
                        .is_some_and(|certs| !certs.is_empty());
                    let meta = ConnectionPeerMeta {
                        peer_addr: Some(peer_addr),
                        client_cert_verified: cert_verified,
                    };
                    serve_connection(tls_stream, app, meta).await;
                }
            });
        } else {
            tokio::spawn(async move {
                let meta = ConnectionPeerMeta {
                    peer_addr: Some(peer_addr),
                    client_cert_verified: false,
                };
                serve_connection(tcp_stream, app, meta).await;
            });
        }
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode, header};
    use base64::Engine as _;
    use compact_str::CompactString;
    use http_body_util::BodyExt as _;
    use negroni_config::parse_and_validate_with_env;
    use negroni_ha::{
        CoordinatorHandle, FailsafeHeartbeatRequest, HaEvent, PublishedNodeState, SharedNodeState,
    };
    use negroni_types::{CasToken, ClusterSnapshot, MemberData, Versioned, WalLsn};
    use tokio::sync::{mpsc, oneshot};
    use tower::ServiceExt as _;

    use super::*;

    struct TestHarness {
        router: Router,
        published: SharedNodeState,
        handle: CoordinatorHandle,
        _event_rx: mpsc::Receiver<HaEvent>,
        failsafe_rx: mpsc::Receiver<(FailsafeHeartbeatRequest, oneshot::Sender<bool>)>,
    }

    fn make_harness(yaml_extra: &str) -> TestHarness {
        let yaml = format!(
            "scope: batman\nname: pg-0\nbootstrap:\n  dcs:\n    ttl: 30\n    loop_wait: 10\n    retry_timeout: 10\n    failsafe_mode: true\n{yaml_extra}"
        );
        let empty: [(&str, &str); 0] = [];
        let cfg = parse_and_validate_with_env(&yaml, "patroni.yml", empty).unwrap();
        let now = Instant::now();
        let mut init = PublishedNodeState::initial(now);
        init.scope = CompactString::const_new("batman");
        init.member_name = CompactString::const_new("pg-0");
        init.failsafe_enabled = true;

        let published = Arc::new(arc_swap::ArcSwap::from_pointee(init));
        let (event_tx, event_rx) = mpsc::channel(32);
        let (failsafe_tx, failsafe_rx) = mpsc::channel(8);
        let handle = CoordinatorHandle::new(Arc::clone(&published), event_tx, failsafe_tx);
        let api_state = ApiState::from_config(cfg, handle.clone()).unwrap();
        let router = build_router(api_state);

        TestHarness {
            router,
            published,
            handle,
            _event_rx: event_rx,
            failsafe_rx,
        }
    }

    #[tokio::test]
    async fn probes_honor_roles_lag_units_tags_head_options_and_prometheus_metrics() {
        let h = make_harness("");
        let now = Instant::now();

        // Initially uninitialized -> /primary and /replica are 503.
        let resp = h
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/primary")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

        // Publish active sync replica with lag = 8MB and tag rack=eu1.
        let mut replica = PublishedNodeState::initial(now);
        replica.scope = CompactString::const_new("batman");
        replica.member_name = CompactString::const_new("pg-1");
        replica.role = CompactString::const_new("replica");
        replica.state = CompactString::const_new("running");
        replica.postgres_state_code = 5;
        replica.is_sync_standby = true;
        replica.replication_lag_bytes = 8 * 1024 * 1024;
        replica.timeline = 3;
        replica.xlog_location = WalLsn(64 * 1024 * 1024);
        replica.user_tags.insert(
            CompactString::const_new("rack"),
            CompactString::const_new("eu1"),
        );
        h.published.store(Arc::new(replica));

        // /replica?lag=16MB&tag_rack=eu1 -> 200 OK
        let ok_rep = h
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/replica?lag=16MB&tag_rack=eu1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ok_rep.status(), StatusCode::OK);

        // /replica?lag=4MB -> 503 (lag 8MB > 4MB)
        let lag_fail = h
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/replica?lag=4MB")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(lag_fail.status(), StatusCode::SERVICE_UNAVAILABLE);

        // /synchronous -> 200 OK, /asynchronous -> 503 (because is_sync_standby == true)
        let sync_ok = h
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::HEAD)
                    .uri("/synchronous")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(sync_ok.status(), StatusCode::OK);

        let async_fail = h
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/asynchronous")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(async_fail.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            async_fail.headers().get(header::ALLOW).unwrap(),
            "GET, HEAD, OPTIONS"
        );

        // GET /metrics -> verify all required Patroni Prometheus gauges
        let metrics_resp = h
            .router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(metrics_resp.status(), StatusCode::OK);
        let body_bytes = metrics_resp.into_body().collect().await.unwrap().to_bytes();
        let text = std::str::from_utf8(&body_bytes).unwrap();
        for metric_name in [
            "patroni_version",
            "patroni_postgres_running",
            "patroni_postgres_state",
            "patroni_postmaster_start_time",
            "patroni_master",
            "patroni_primary",
            "patroni_standby_leader",
            "patroni_replica",
            "patroni_sync_standby",
            "patroni_quorum_standby",
            "patroni_async_standby",
            "patroni_xlog_location",
            "patroni_xlog_received_location",
            "patroni_xlog_replayed_location",
            "patroni_xlog_replayed_timestamp",
            "patroni_xlog_paused",
            "patroni_postgres_timeline",
            "patroni_dcs_last_seen",
            "patroni_cluster_unlocked",
            "patroni_failsafe_mode_is_active",
            "patroni_is_paused",
            "patroni_pending_restart",
        ] {
            assert!(text.contains(metric_name), "missing metric {metric_name}");
        }
    }

    #[tokio::test]
    async fn guarded_endpoints_enforce_basic_auth_allowlist_and_control_semantics() {
        let mut h = make_harness(
            "restapi:\n  authentication:\n    username: admin\n    password: s3cr3t\n  allowlist:\n    - 10.10.0.0/16\n  allowlist_include_members: true\n",
        );

        // Populate cluster snapshot with leader pg-0 and healthy replica pg-1 at 192.168.50.12
        let mut snap = ClusterSnapshot {
            leader: Some(Versioned {
                value: CompactString::const_new("pg-0"),
                token: CasToken::Revision(1),
            }),
            ..Default::default()
        };
        snap.members.insert(
            CompactString::const_new("pg-0"),
            Versioned {
                value: MemberData {
                    conn_url: "postgres://10.10.1.1:5432/postgres".to_owned(),
                    api_url: "http://10.10.1.1:8008/patroni".to_owned(),
                    state: CompactString::const_new("running"),
                    role: CompactString::const_new("master"),
                    version: CompactString::const_new("4.1.0"),
                    xlog_location: Some(WalLsn(1000)),
                    timeline: Some(1),
                    replication_state: None,
                    pending_restart: false,
                    pending_restart_reason: BTreeMap::new(),
                    pause: None,
                    watchdog_failed: None,
                    tags: BTreeMap::new(),
                    extra: BTreeMap::new(),
                },
                token: CasToken::Revision(1),
            },
        );
        snap.members.insert(
            CompactString::const_new("pg-1"),
            Versioned {
                value: MemberData {
                    conn_url: "postgres://192.168.50.12:5432/postgres".to_owned(),
                    api_url: "http://192.168.50.12:8008/patroni".to_owned(),
                    state: CompactString::const_new("running"),
                    role: CompactString::const_new("replica"),
                    version: CompactString::const_new("4.1.0"),
                    xlog_location: Some(WalLsn(1000)),
                    timeline: Some(1),
                    replication_state: Some(CompactString::const_new("streaming")),
                    pending_restart: false,
                    pending_restart_reason: BTreeMap::new(),
                    pause: None,
                    watchdog_failed: None,
                    tags: BTreeMap::new(),
                    extra: BTreeMap::new(),
                },
                token: CasToken::Revision(2),
            },
        );
        h.handle.shared_cluster_cache().store(Arc::new(snap));

        let valid_auth = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode("admin:s3cr3t")
        );
        let allowed_peer = ConnectionPeerMeta {
            peer_addr: Some(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(192, 168, 50, 12)),
                44100,
            )),
            client_cert_verified: false,
        };
        let blocked_peer = ConnectionPeerMeta {
            peer_addr: Some(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9)),
                44100,
            )),
            client_cert_verified: false,
        };

        // 1. Disallowed IP -> 403 Forbidden
        let mut req_403 = Request::builder()
            .method(Method::POST)
            .uri("/reload")
            .header(header::AUTHORIZATION, &valid_auth)
            .body(Body::empty())
            .unwrap();
        req_403.extensions_mut().insert(blocked_peer);
        assert_eq!(
            h.router.clone().oneshot(req_403).await.unwrap().status(),
            StatusCode::FORBIDDEN
        );

        // 2. Allowed dynamic member IP, wrong password -> 401 Unauthorized
        let mut req_401 = Request::builder()
            .method(Method::POST)
            .uri("/reload")
            .header(header::AUTHORIZATION, "Basic YWRtaW46d3Jvbmc=")
            .body(Body::empty())
            .unwrap();
        req_401.extensions_mut().insert(allowed_peer);
        assert_eq!(
            h.router.clone().oneshot(req_401).await.unwrap().status(),
            StatusCode::UNAUTHORIZED
        );

        // 3. PATCH /config -> 200 OK
        let mut req_patch = Request::builder()
            .method(Method::PATCH)
            .uri("/config")
            .header(header::AUTHORIZATION, &valid_auth)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"maximum_lag_on_failover":2097152}"#))
            .unwrap();
        req_patch.extensions_mut().insert(allowed_peer);
        let patch_resp = h.router.clone().oneshot(req_patch).await.unwrap();
        assert_eq!(patch_resp.status(), StatusCode::OK);

        // 4. POST /switchover without timezone -> 400 Bad Request
        let mut req_sw_400 = Request::builder()
            .method(Method::POST)
            .uri("/switchover")
            .header(header::AUTHORIZATION, &valid_auth)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"leader":"pg-0","candidate":"pg-1","scheduled_at":"2099-01-01T00:00:00"}"#,
            ))
            .unwrap();
        req_sw_400.extensions_mut().insert(allowed_peer);
        assert_eq!(
            h.router.clone().oneshot(req_sw_400).await.unwrap().status(),
            StatusCode::BAD_REQUEST
        );

        // 5. POST /restart schedule -> 202, duplicate -> 409, DELETE /restart -> 204
        let mut req_rs = Request::builder()
            .method(Method::POST)
            .uri("/restart")
            .header(header::AUTHORIZATION, &valid_auth)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"schedule":"2099-01-01T00:00:00Z"}"#))
            .unwrap();
        req_rs.extensions_mut().insert(allowed_peer);
        assert_eq!(
            h.router.clone().oneshot(req_rs).await.unwrap().status(),
            StatusCode::ACCEPTED
        );

        let mut req_rs_dup = Request::builder()
            .method(Method::POST)
            .uri("/restart")
            .header(header::AUTHORIZATION, &valid_auth)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"schedule":"2099-01-01T01:00:00Z"}"#))
            .unwrap();
        req_rs_dup.extensions_mut().insert(allowed_peer);
        assert_eq!(
            h.router.clone().oneshot(req_rs_dup).await.unwrap().status(),
            StatusCode::CONFLICT
        );

        let mut req_rs_del = Request::builder()
            .method(Method::DELETE)
            .uri("/restart")
            .header(header::AUTHORIZATION, &valid_auth)
            .body(Body::empty())
            .unwrap();
        req_rs_del.extensions_mut().insert(allowed_peer);
        assert_eq!(
            h.router.clone().oneshot(req_rs_del).await.unwrap().status(),
            StatusCode::NO_CONTENT
        );

        // 6. POST /failsafe -> 200 "Accepted" when coordinator accepts, 502 when failsafe disabled
        tokio::spawn(async move {
            if let Some((_hb, reply)) = h.failsafe_rx.recv().await {
                let _ = reply.send(true);
            }
        });
        let mut req_fs = Request::builder()
            .method(Method::POST)
            .uri("/failsafe")
            .header(header::AUTHORIZATION, &valid_auth)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"name":"pg-0","conn_url":"postgres://10.10.1.1:5432/postgres","xlog_location":2048}"#,
            ))
            .unwrap();
        req_fs.extensions_mut().insert(allowed_peer);
        let fs_resp = h.router.clone().oneshot(req_fs).await.unwrap();
        assert_eq!(fs_resp.status(), StatusCode::OK);

        // Disable failsafe -> 502 Bad Gateway
        let mut disabled_state = PublishedNodeState::initial(Instant::now());
        disabled_state.failsafe_enabled = false;
        h.published.store(Arc::new(disabled_state));
        let mut req_fs_502 = Request::builder()
            .method(Method::POST)
            .uri("/failsafe")
            .header(header::AUTHORIZATION, &valid_auth)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"name":"pg-0","conn_url":"postgres://10.10.1.1:5432/postgres","xlog_location":2048}"#,
            ))
            .unwrap();
        req_fs_502.extensions_mut().insert(allowed_peer);
        assert_eq!(
            h.router.clone().oneshot(req_fs_502).await.unwrap().status(),
            StatusCode::BAD_GATEWAY
        );

        let _ = Duration::from_millis(1);
    }
}
