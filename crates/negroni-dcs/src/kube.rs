use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STD;
use compact_str::CompactString;
use futures_util::StreamExt;
use jiff::Timestamp;
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::api::core::v1::{
    ConfigMap, EndpointAddress, EndpointPort, EndpointSubset, Endpoints, Pod,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use k8s_openapi::chrono::{DateTime, Utc};
use kube_core::{ObjectList, WatchEvent};
use negroni_config::{KubeConfig, SecretString};
use negroni_types::{
    CasToken, ClusterSnapshot, ClusterStatus, FailoverIntent, HistoryEntry, LeaseHandle,
    MemberData, SyncState, Versioned, WalLsn,
};
use reqwest::{
    Certificate as ReqwestCert, Client as HttpClient, Identity as ReqwestIdentity, StatusCode,
};
use saphyr::{LoadableYamlNode, Yaml};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc;

use crate::{DcsBackend, DcsError, DcsWatchEvent};

const IN_CLUSTER_SA_DIR: &str = "/var/run/secrets/kubernetes.io/serviceaccount";
const DEFAULT_SCOPE_LABEL: &str = "cluster-name";
const DEFAULT_ROLE_LABEL: &str = "role";

/// Resolved Kubernetes API transport settings (in-cluster ServiceAccount, `$KUBECONFIG`, or explicit URL).
#[derive(Debug, Clone)]
pub struct KubeConnectionConfig {
    pub api_server_url: String,
    pub namespace: String,
    pub bearer_token: Option<SecretString>,
    pub basic_auth: Option<(String, SecretString)>,
    pub ca_pem: Option<Vec<u8>>,
    pub client_cert_pem: Option<Vec<u8>>,
    pub client_key_pem: Option<Vec<u8>>,
    pub insecure_skip_tls_verify: bool,
}

impl KubeConnectionConfig {
    /// Resolves Kubernetes connection parameters from `KubeConfig`, optional explicit URL,
    /// in-cluster ServiceAccount mounts, or `$KUBECONFIG` / `~/.kube/config` (parsed via pure-Rust `saphyr`).
    pub fn discover(
        kube_cfg: &KubeConfig,
        explicit_api_url: Option<&str>,
    ) -> Result<Self, DcsError> {
        if let Some(url) = explicit_api_url {
            let clean = url
                .trim()
                .strip_prefix("k8s://")
                .or_else(|| url.trim().strip_prefix("kubernetes://"))
                .map_or_else(
                    || url.trim().to_owned(),
                    |rest| {
                        if rest.starts_with("http://") || rest.starts_with("https://") {
                            rest.to_owned()
                        } else {
                            format!("https://{rest}")
                        }
                    },
                );
            let ns = kube_cfg
                .namespace
                .as_ref()
                .map_or_else(|| "default".to_owned(), ToString::to_string);
            return Ok(Self {
                api_server_url: clean.trim_end_matches('/').to_owned(),
                namespace: ns,
                bearer_token: None,
                basic_auth: None,
                ca_pem: None,
                client_cert_pem: None,
                client_key_pem: None,
                insecure_skip_tls_verify: false,
            });
        }

        let sa_token_path = Path::new(IN_CLUSTER_SA_DIR).join("token");
        if sa_token_path.is_file()
            && let Ok(host) = std::env::var("KUBERNETES_SERVICE_HOST")
        {
            let port =
                std::env::var("KUBERNETES_SERVICE_PORT").unwrap_or_else(|_| "443".to_owned());
            let host_fmt = if host.contains(':') && !host.starts_with('[') {
                format!("[{host}]")
            } else {
                host
            };
            let token_raw = fs::read_to_string(&sa_token_path)
                .map_err(|e| DcsError::Unreachable(format!("reading SA token: {e}")))?;
            let ca_pem = fs::read(Path::new(IN_CLUSTER_SA_DIR).join("ca.crt")).ok();
            let ns = kube_cfg
                .namespace
                .as_ref()
                .map(ToString::to_string)
                .or_else(|| {
                    fs::read_to_string(Path::new(IN_CLUSTER_SA_DIR).join("namespace"))
                        .ok()
                        .map(|s| s.trim().to_owned())
                })
                .unwrap_or_else(|| "default".to_owned());

            return Ok(Self {
                api_server_url: format!("https://{host_fmt}:{port}"),
                namespace: ns,
                bearer_token: Some(SecretString::new(token_raw.trim())),
                basic_auth: None,
                ca_pem,
                client_cert_pem: None,
                client_key_pem: None,
                insecure_skip_tls_verify: false,
            });
        }

        let kubeconfig_path = std::env::var("KUBECONFIG")
            .ok()
            .and_then(|v| {
                v.split(':')
                    .next()
                    .filter(|s| !s.is_empty())
                    .map(PathBuf::from)
            })
            .or_else(|| {
                std::env::var("HOME")
                    .ok()
                    .map(|h| PathBuf::from(h).join(".kube").join("config"))
            });

        if let Some(kc_path) = kubeconfig_path
            && kc_path.is_file()
        {
            return Self::from_kubeconfig_file(&kc_path, kube_cfg.namespace.as_deref());
        }

        Err(DcsError::Unreachable(
            "Kubernetes API credentials not found in ServiceAccount or KUBECONFIG".to_owned(),
        ))
    }

    /// Parses a standard Kubernetes `kubeconfig` YAML file via pure-Rust `saphyr` (zero `unsafe-libyaml`).
    pub fn from_kubeconfig_str(
        yaml_text: &str,
        base_dir: Option<&Path>,
        override_namespace: Option<&str>,
    ) -> Result<Self, DcsError> {
        let docs = Yaml::load_from_str(yaml_text)
            .map_err(|e| DcsError::Unreachable(format!("parsing kubeconfig YAML: {e}")))?;
        let Some(root) = docs.first() else {
            return Err(DcsError::Unreachable("kubeconfig YAML is empty".to_owned()));
        };

        let current_ctx = yaml_map_str(root, "current-context").unwrap_or_default();
        let mut cluster_name = String::new();
        let mut user_name = String::new();
        let mut ctx_namespace: Option<String> = None;

        if let Some(contexts) = yaml_map_seq(root, "contexts") {
            for item in contexts {
                let name = yaml_map_str(item, "name").unwrap_or_default();
                if (current_ctx.is_empty() || name == current_ctx)
                    && let Some(ctx_obj) = yaml_map_get(item, "context")
                {
                    cluster_name = yaml_map_str(ctx_obj, "cluster").unwrap_or_default();
                    user_name = yaml_map_str(ctx_obj, "user").unwrap_or_default();
                    ctx_namespace = yaml_map_str(ctx_obj, "namespace");
                    break;
                }
            }
        }

        let mut server_url: Option<String> = None;
        let mut ca_pem: Option<Vec<u8>> = None;
        let mut insecure = false;

        if let Some(clusters) = yaml_map_seq(root, "clusters") {
            for item in clusters {
                let name = yaml_map_str(item, "name").unwrap_or_default();
                if (cluster_name.is_empty() || name == cluster_name)
                    && let Some(cl_obj) = yaml_map_get(item, "cluster")
                {
                    server_url = yaml_map_str(cl_obj, "server");
                    insecure = yaml_map_bool(cl_obj, "insecure-skip-tls-verify").unwrap_or(false);
                    if let Some(b64) = yaml_map_str(cl_obj, "certificate-authority-data") {
                        ca_pem = Some(decode_b64(&b64)?);
                    } else if let Some(rel) = yaml_map_str(cl_obj, "certificate-authority") {
                        ca_pem = Some(read_kc_file(base_dir, &rel)?);
                    }
                    break;
                }
            }
        }

        let api_server_url = server_url
            .ok_or_else(|| DcsError::Unreachable("kubeconfig missing cluster.server".to_owned()))?
            .trim_end_matches('/')
            .to_owned();

        let mut bearer_token: Option<SecretString> = None;
        let mut basic_auth: Option<(String, SecretString)> = None;
        let mut client_cert_pem: Option<Vec<u8>> = None;
        let mut client_key_pem: Option<Vec<u8>> = None;

        if let Some(users) = yaml_map_seq(root, "users") {
            for item in users {
                let name = yaml_map_str(item, "name").unwrap_or_default();
                if (user_name.is_empty() || name == user_name)
                    && let Some(u_obj) = yaml_map_get(item, "user")
                {
                    if let Some(tok) = yaml_map_str(u_obj, "token") {
                        bearer_token = Some(SecretString::new(tok));
                    } else if let Some(tok_file) = yaml_map_str(u_obj, "tokenFile") {
                        let bytes = read_kc_file(base_dir, &tok_file)?;
                        let tok = String::from_utf8_lossy(&bytes).trim().to_owned();
                        bearer_token = Some(SecretString::new(tok));
                    }
                    if let (Some(u), Some(p)) = (
                        yaml_map_str(u_obj, "username"),
                        yaml_map_str(u_obj, "password"),
                    ) {
                        basic_auth = Some((u, SecretString::new(p)));
                    }
                    if let Some(b64) = yaml_map_str(u_obj, "client-certificate-data") {
                        client_cert_pem = Some(decode_b64(&b64)?);
                    } else if let Some(rel) = yaml_map_str(u_obj, "client-certificate") {
                        client_cert_pem = Some(read_kc_file(base_dir, &rel)?);
                    }
                    if let Some(b64) = yaml_map_str(u_obj, "client-key-data") {
                        client_key_pem = Some(decode_b64(&b64)?);
                    } else if let Some(rel) = yaml_map_str(u_obj, "client-key") {
                        client_key_pem = Some(read_kc_file(base_dir, &rel)?);
                    }
                    break;
                }
            }
        }

        let namespace = override_namespace
            .map(ToOwned::to_owned)
            .or(ctx_namespace)
            .unwrap_or_else(|| "default".to_owned());

        Ok(Self {
            api_server_url,
            namespace,
            bearer_token,
            basic_auth,
            ca_pem,
            client_cert_pem,
            client_key_pem,
            insecure_skip_tls_verify: insecure,
        })
    }

    pub fn from_kubeconfig_file(
        path: &Path,
        override_namespace: Option<&str>,
    ) -> Result<Self, DcsError> {
        let raw = fs::read_to_string(path).map_err(|e| {
            DcsError::Unreachable(format!("reading kubeconfig '{}': {e}", path.display()))
        })?;
        Self::from_kubeconfig_str(&raw, path.parent(), override_namespace)
    }
}

fn decode_b64(raw: &str) -> Result<Vec<u8>, DcsError> {
    let stripped: String = raw.chars().filter(|c| !c.is_ascii_whitespace()).collect();
    BASE64_STD
        .decode(stripped)
        .map_err(|e| DcsError::Unreachable(format!("invalid base64 in kubeconfig: {e}")))
}

fn read_kc_file(base_dir: Option<&Path>, rel_or_abs: &str) -> Result<Vec<u8>, DcsError> {
    let p = Path::new(rel_or_abs);
    let resolved = if p.is_absolute() {
        p.to_path_buf()
    } else if let Some(base) = base_dir {
        base.join(p)
    } else {
        p.to_path_buf()
    };
    fs::read(&resolved)
        .map_err(|e| DcsError::Unreachable(format!("reading '{}': {e}", resolved.display())))
}

fn yaml_map_get<'a>(node: &'a Yaml<'a>, key: &str) -> Option<&'a Yaml<'a>> {
    let map = node.as_mapping_get(key);
    if map.is_some() {
        return map;
    }
    None
}

fn yaml_map_str(node: &Yaml<'_>, key: &str) -> Option<String> {
    yaml_map_get(node, key)
        .and_then(Yaml::as_str)
        .map(ToOwned::to_owned)
}

fn yaml_map_bool(node: &Yaml<'_>, key: &str) -> Option<bool> {
    yaml_map_get(node, key).and_then(Yaml::as_bool)
}

fn yaml_map_seq<'a>(node: &'a Yaml<'a>, key: &str) -> Option<&'a [Yaml<'a>]> {
    yaml_map_get(node, key)
        .and_then(Yaml::as_vec)
        .map(Vec::as_slice)
}

/// Evaluates whether a Kubernetes `Endpoints` or `Lease` leader record is still unexpired at `now`.
///
/// # Examples
///
/// ```
/// use jiff::Timestamp;
/// use negroni_dcs::kube::is_k8s_lease_fresh;
///
/// let now: Timestamp = "2026-11-12T18:00:20Z".parse()?;
/// assert!(is_k8s_lease_fresh("2026-11-12T18:00:00Z", 30, now));
/// assert!(!is_k8s_lease_fresh("2026-11-12T18:00:00Z", 15, now));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn is_k8s_lease_fresh(renew_time_rfc3339: &str, ttl_secs: u64, now: Timestamp) -> bool {
    let Ok(renewed) = renew_time_rfc3339.trim().parse::<Timestamp>() else {
        return false;
    };
    let ttl_signed = i64::try_from(ttl_secs).unwrap_or(30);
    let Ok(expires_at) = renewed.checked_add(jiff::SignedDuration::from_secs(ttl_signed)) else {
        return false;
    };
    now < expires_at
}

/// Extracts `(ip, port)` from a member `conn_url` (`postgres://10.244.0.9:5432/postgres`)
/// so `Endpoints` mode can populate active primary `EndpointSubset` targets automatically.
pub fn parse_pod_ip_and_port_from_conn_url(
    conn_url: &str,
    configured_pod_ip: Option<&str>,
) -> (Option<String>, i32) {
    let mut port: i32 = 5432;
    let mut parsed_ip: Option<String> = None;

    if let Some(rest) = conn_url
        .strip_prefix("postgres://")
        .or_else(|| conn_url.strip_prefix("postgresql://"))
    {
        let authority = rest.split('/').next().unwrap_or(rest);
        let hostport = authority.rsplit_once('@').map_or(authority, |(_, hp)| hp);
        if let Some(bracketed) = hostport.strip_prefix('[') {
            if let Some((ip6, after)) = bracketed.split_once(']') {
                if ip6.parse::<std::net::IpAddr>().is_ok() {
                    parsed_ip = Some(ip6.to_owned());
                }
                if let Some(p_str) = after.strip_prefix(':')
                    && let Ok(p) = p_str.parse::<i32>()
                {
                    port = p;
                }
            }
        } else if let Some((h, p_str)) = hostport.rsplit_once(':') {
            if h.parse::<std::net::IpAddr>().is_ok() {
                parsed_ip = Some(h.to_owned());
            }
            if let Ok(p) = p_str.parse::<i32>() {
                port = p;
            }
        } else if hostport.parse::<std::net::IpAddr>().is_ok() {
            parsed_ip = Some(hostport.to_owned());
        }
    }

    let final_ip = configured_pod_ip
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.trim().to_owned())
        .or(parsed_ip);
    (final_ip, port)
}

#[derive(Debug, Default)]
struct LocalKubeHint {
    ttl_secs: u64,
    last_conn_url: Option<String>,
}

/// Production Kubernetes DCS backend supporting both `Endpoints` (with active primary Pod subset
/// synchronization) and `coordination.k8s.io/v1::Lease` modes plus `<scope>-config` `ConfigMap`
/// optimistic `metadata.resourceVersion` CAS.
pub struct KubeDcs {
    http: HttpClient,
    conn: KubeConnectionConfig,
    scope: CompactString,
    kube_cfg: KubeConfig,
    hint: Arc<Mutex<LocalKubeHint>>,
}

impl std::fmt::Debug for KubeDcs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubeDcs")
            .field("api_server_url", &self.conn.api_server_url)
            .field("namespace", &self.conn.namespace)
            .field("scope", &self.scope)
            .field("use_endpoints", &self.kube_cfg.use_endpoints)
            .finish_non_exhaustive()
    }
}

impl KubeDcs {
    pub fn new(
        scope: impl Into<CompactString>,
        kube_cfg: KubeConfig,
        conn: KubeConnectionConfig,
    ) -> Result<Self, DcsError> {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let mut builder = HttpClient::builder()
            .connect_timeout(Duration::from_secs(5))
            .danger_accept_invalid_certs(conn.insecure_skip_tls_verify);

        if let Some(ref ca_bytes) = conn.ca_pem {
            let cert = ReqwestCert::from_pem(ca_bytes)
                .map_err(|e| DcsError::Unreachable(format!("invalid Kubernetes CA PEM: {e}")))?;
            builder = builder.add_root_certificate(cert);
        }

        if let (Some(cert_pem), Some(key_pem)) = (&conn.client_cert_pem, &conn.client_key_pem) {
            let mut combined = Vec::with_capacity(cert_pem.len() + key_pem.len() + 2);
            combined.extend_from_slice(cert_pem);
            if !combined.ends_with(b"\n") {
                combined.push(b'\n');
            }
            combined.extend_from_slice(key_pem);
            let identity = ReqwestIdentity::from_pem(&combined).map_err(|e| {
                DcsError::Unreachable(format!("invalid Kubernetes client cert/key PEM: {e}"))
            })?;
            builder = builder.identity(identity);
        }

        let http = builder
            .build()
            .map_err(|e| DcsError::Unreachable(format!("building Kubernetes HTTP client: {e}")))?;

        Ok(Self {
            http,
            conn,
            scope: scope.into(),
            kube_cfg,
            hint: Arc::new(Mutex::new(LocalKubeHint {
                ttl_secs: 30,
                last_conn_url: None,
            })),
        })
    }

    pub fn from_config(
        scope: impl Into<CompactString>,
        kube_cfg: KubeConfig,
        explicit_api_url: Option<&str>,
    ) -> Result<Self, DcsError> {
        let conn = KubeConnectionConfig::discover(&kube_cfg, explicit_api_url)?;
        Self::new(scope, kube_cfg, conn)
    }

    fn scope_label_key(&self) -> &str {
        self.kube_cfg
            .scope_label
            .as_deref()
            .unwrap_or(DEFAULT_SCOPE_LABEL)
    }

    fn role_label_key(&self) -> &str {
        self.kube_cfg
            .role_label
            .as_deref()
            .unwrap_or(DEFAULT_ROLE_LABEL)
    }

    fn config_map_name(&self) -> String {
        format!("{}-config", self.scope)
    }

    fn base_labels(&self) -> BTreeMap<String, String> {
        let mut out: BTreeMap<String, String> = self
            .kube_cfg
            .labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        out.insert(self.scope_label_key().to_owned(), self.scope.to_string());
        out
    }

    fn label_selector_query(&self) -> String {
        self.base_labels()
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    fn apply_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(ref bearer) = self.conn.bearer_token {
            req.bearer_auth(bearer.expose())
        } else if let Some((ref u, ref p)) = self.conn.basic_auth {
            req.basic_auth(u, Some(p.expose()))
        } else {
            req
        }
    }

    fn endpoints_collection_url(&self) -> String {
        format!(
            "{}/api/v1/namespaces/{}/endpoints",
            self.conn.api_server_url, self.conn.namespace
        )
    }

    fn endpoints_item_url(&self) -> String {
        format!("{}/{}", self.endpoints_collection_url(), self.scope)
    }

    fn lease_collection_url(&self) -> String {
        format!(
            "{}/apis/coordination.k8s.io/v1/namespaces/{}/leases",
            self.conn.api_server_url, self.conn.namespace
        )
    }

    fn lease_item_url(&self) -> String {
        format!("{}/{}", self.lease_collection_url(), self.scope)
    }

    fn configmap_collection_url(&self) -> String {
        format!(
            "{}/api/v1/namespaces/{}/configmaps",
            self.conn.api_server_url, self.conn.namespace
        )
    }

    fn configmap_item_url(&self) -> String {
        format!(
            "{}/{}",
            self.configmap_collection_url(),
            self.config_map_name()
        )
    }

    fn pods_collection_url(&self) -> String {
        format!(
            "{}/api/v1/namespaces/{}/pods",
            self.conn.api_server_url, self.conn.namespace
        )
    }

    fn pod_item_url(&self, pod_name: &str) -> String {
        format!("{}/{pod_name}", self.pods_collection_url())
    }

    async fn get_optional_json<T: DeserializeOwned>(
        &self,
        url: &str,
        timeout: Duration,
    ) -> Result<Option<T>, DcsError> {
        let req = self.apply_auth(self.http.get(url)).timeout(timeout);
        let resp = req
            .send()
            .await
            .map_err(|e| DcsError::Unreachable(format!("GET '{url}' failed: {e}")))?;
        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(DcsError::Unreachable(format!(
                "GET '{url}' returned HTTP {status}: {body}"
            )));
        }
        resp.json::<T>()
            .await
            .map(Some)
            .map_err(|e| DcsError::Unreachable(format!("decoding '{url}' JSON: {e}")))
    }

    async fn create_or_replace_cas<T: Serialize + DeserializeOwned>(
        &self,
        collection_url: &str,
        item_url: &str,
        is_create: bool,
        body: &T,
        expected: Option<&CasToken>,
    ) -> Result<T, DcsError> {
        let req = if is_create {
            self.apply_auth(self.http.post(collection_url)).json(body)
        } else {
            self.apply_auth(self.http.put(item_url)).json(body)
        };
        let resp = req
            .send()
            .await
            .map_err(|e| DcsError::Unreachable(format!("Kubernetes write failed: {e}")))?;

        if resp.status() == StatusCode::CONFLICT
            || resp.status() == StatusCode::UNPROCESSABLE_ENTITY
        {
            return Err(DcsError::CasConflict {
                expected: expected.cloned(),
            });
        }
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(DcsError::Unreachable(format!(
                "Kubernetes write returned HTTP {status}: {text}"
            )));
        }
        resp.json::<T>()
            .await
            .map_err(|e| DcsError::Unreachable(format!("decoding Kubernetes write response: {e}")))
    }

    fn rv_token(meta: &ObjectMeta) -> CasToken {
        CasToken::ResourceVersion(CompactString::from(
            meta.resource_version.as_deref().unwrap_or("1"),
        ))
    }

    fn extract_rv_str(tok: &CasToken) -> Result<&str, DcsError> {
        match tok {
            CasToken::ResourceVersion(rv) => Ok(rv.as_str()),
            CasToken::Revision(_) => Err(DcsError::CasConflict {
                expected: Some(tok.clone()),
            }),
        }
    }

    fn build_primary_subsets(&self) -> Option<Vec<EndpointSubset>> {
        let last_conn = self
            .hint
            .lock()
            .ok()
            .and_then(|g| g.last_conn_url.clone())
            .unwrap_or_default();
        let (ip_opt, port) =
            parse_pod_ip_and_port_from_conn_url(&last_conn, self.kube_cfg.pod_ip.as_deref());
        let ip = ip_opt?;
        Some(vec![EndpointSubset {
            addresses: Some(vec![EndpointAddress {
                ip,
                hostname: None,
                node_name: None,
                target_ref: None,
            }]),
            not_ready_addresses: None,
            ports: Some(vec![EndpointPort {
                app_protocol: None,
                name: Some("postgresql".to_owned()),
                port,
                protocol: Some("TCP".to_owned()),
            }]),
        }])
    }

    async fn mutate_config_annotations<F>(
        &self,
        expected: Option<&CasToken>,
        check_key_cas: Option<&str>,
        mutate_fn: F,
    ) -> Result<CasToken, DcsError>
    where
        F: FnOnce(&mut BTreeMap<String, String>) -> Result<(), DcsError>,
    {
        let existing_cm: Option<ConfigMap> = self
            .get_optional_json(&self.configmap_item_url(), Duration::from_secs(10))
            .await?;

        if let Some(key_name) = check_key_cas {
            match (&existing_cm, expected) {
                (None, Some(exp)) => {
                    return Err(DcsError::CasConflict {
                        expected: Some(exp.clone()),
                    });
                }
                (Some(cm), None) => {
                    let has_key = cm
                        .metadata
                        .annotations
                        .as_ref()
                        .is_some_and(|a| a.contains_key(key_name));
                    if has_key {
                        return Err(DcsError::CasConflict { expected: None });
                    }
                }
                (Some(cm), Some(exp)) => {
                    let exp_rv = Self::extract_rv_str(exp)?;
                    if cm.metadata.resource_version.as_deref() != Some(exp_rv) {
                        return Err(DcsError::CasConflict {
                            expected: Some(exp.clone()),
                        });
                    }
                }
                (None, None) => {}
            }
        }

        let is_create = existing_cm.is_none();
        let mut cm = existing_cm.unwrap_or_else(|| ConfigMap {
            metadata: ObjectMeta {
                name: Some(self.config_map_name()),
                namespace: Some(self.conn.namespace.clone()),
                labels: Some(self.base_labels()),
                annotations: Some(BTreeMap::new()),
                ..ObjectMeta::default()
            },
            ..ConfigMap::default()
        });

        let ann = cm.metadata.annotations.get_or_insert_with(BTreeMap::new);
        mutate_fn(ann)?;

        let saved: ConfigMap = self
            .create_or_replace_cas(
                &self.configmap_collection_url(),
                &self.configmap_item_url(),
                is_create,
                &cm,
                expected,
            )
            .await?;
        Ok(Self::rv_token(&saved.metadata))
    }
}

impl DcsBackend for KubeDcs {
    async fn fetch_cluster(&self, timeout: Duration) -> Result<ClusterSnapshot, DcsError> {
        let now_ts = Timestamp::now();
        let mut snap = ClusterSnapshot::default();

        // 1. Read `<scope>-config` ConfigMap annotations
        if let Some(cm) = self
            .get_optional_json::<ConfigMap>(&self.configmap_item_url(), timeout)
            .await?
        {
            let cm_tok = Self::rv_token(&cm.metadata);
            if let Some(ann) = &cm.metadata.annotations {
                if let Some(init_id) = ann.get("initialize").filter(|s| !s.trim().is_empty()) {
                    snap.initialize = Some(Versioned {
                        value: CompactString::from(init_id.trim()),
                        token: cm_tok.clone(),
                    });
                }
                if let Some(cfg_str) = ann.get("config")
                    && let Ok(val) = serde_json::from_str::<serde_json::Value>(cfg_str)
                {
                    snap.config = Some(Versioned {
                        value: val,
                        token: cm_tok.clone(),
                    });
                }
                if let Some(st_str) = ann.get("status") {
                    if let Ok(st) = serde_json::from_str::<ClusterStatus>(st_str) {
                        snap.status = Some(Versioned {
                            value: st,
                            token: cm_tok.clone(),
                        });
                    } else if let Ok(lsn) = st_str.trim().parse::<u64>() {
                        snap.status = Some(Versioned {
                            value: ClusterStatus {
                                optime: WalLsn(lsn),
                                slots: BTreeMap::new(),
                                retain_slots: Vec::new(),
                            },
                            token: cm_tok.clone(),
                        });
                    }
                }
                if let Some(sync_str) = ann.get("sync")
                    && let Ok(sync) = serde_json::from_str::<SyncState>(sync_str)
                {
                    snap.sync = Some(Versioned {
                        value: sync,
                        token: cm_tok.clone(),
                    });
                }
                if let Some(fo_str) = ann.get("failover")
                    && let Ok(fo) = serde_json::from_str::<FailoverIntent>(fo_str)
                    && (fo.leader.is_some() || fo.candidate.is_some() || fo.scheduled_at.is_some())
                {
                    snap.failover = Some(Versioned {
                        value: fo,
                        token: cm_tok.clone(),
                    });
                }
                if let Some(fs_str) = ann.get("failsafe")
                    && let Ok(fs_map) =
                        serde_json::from_str::<BTreeMap<CompactString, String>>(fs_str)
                {
                    snap.failsafe = Some(fs_map);
                }
                if let Some(hist_str) = ann.get("history")
                    && let Ok(hist) = serde_json::from_str::<Vec<HistoryEntry>>(hist_str)
                {
                    snap.history = hist;
                }
                // Fallback member annotations when running outside StatefulSet Pods
                for (k, v) in ann {
                    if let Some(m_name) = k.strip_prefix("member:")
                        && let Ok(m_data) = serde_json::from_str::<MemberData>(v)
                    {
                        snap.members.insert(
                            CompactString::from(m_name),
                            Versioned {
                                value: m_data,
                                token: cm_tok.clone(),
                            },
                        );
                    }
                }
            }
        }

        // 2. Read `<scope>` Leader (`Endpoints` or `Lease`)
        let default_ttl = self.hint.lock().map_or(30, |g| g.ttl_secs.max(5));
        if self.kube_cfg.use_endpoints {
            if let Some(ep) = self
                .get_optional_json::<Endpoints>(&self.endpoints_item_url(), timeout)
                .await?
                && let Some(ann) = &ep.metadata.annotations
                && let Some(holder) = ann
                    .get("leader")
                    .map(|s| s.trim())
                    .filter(|s| !s.is_empty())
            {
                let ttl_secs = ann
                    .get("ttl")
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(default_ttl);
                let fresh = ann
                    .get("renewTime")
                    .is_none_or(|rt| is_k8s_lease_fresh(rt, ttl_secs, now_ts));
                if fresh {
                    snap.leader = Some(Versioned {
                        value: CompactString::from(holder),
                        token: Self::rv_token(&ep.metadata),
                    });
                }
            }
        } else if let Some(lease_obj) = self
            .get_optional_json::<Lease>(&self.lease_item_url(), timeout)
            .await?
            && let Some(spec) = &lease_obj.spec
            && let Some(holder) = spec
                .holder_identity
                .as_deref()
                .map(str::trim)
                .filter(|s| !s.is_empty())
        {
            let ttl_secs = spec
                .lease_duration_seconds
                .and_then(|s| u64::try_from(s).ok())
                .unwrap_or(default_ttl);
            let fresh = spec
                .renew_time
                .as_ref()
                .is_none_or(|MicroTime(dt)| is_k8s_lease_fresh(&dt.to_rfc3339(), ttl_secs, now_ts));
            if fresh {
                snap.leader = Some(Versioned {
                    value: CompactString::from(holder),
                    token: Self::rv_token(&lease_obj.metadata),
                });
            }
        }

        // 3. Read cluster `Pod`s (`metadata.annotations["status"]` -> `MemberData`)
        let pods_url = format!(
            "{}?labelSelector={}",
            self.pods_collection_url(),
            self.label_selector_query()
        );
        if let Ok(Some(pod_list)) = self
            .get_optional_json::<ObjectList<Pod>>(&pods_url, timeout)
            .await
        {
            for pod in pod_list.items {
                let Some(pod_name) = pod.metadata.name.as_deref() else {
                    continue;
                };
                let Some(status_json) = pod
                    .metadata
                    .annotations
                    .as_ref()
                    .and_then(|a| a.get("status"))
                else {
                    continue;
                };
                if let Ok(member) = serde_json::from_str::<MemberData>(status_json) {
                    snap.members.insert(
                        CompactString::from(pod_name),
                        Versioned {
                            value: member,
                            token: Self::rv_token(&pod.metadata),
                        },
                    );
                }
            }
        }

        Ok(snap)
    }

    async fn ensure_lease(
        &self,
        ttl: Duration,
        current: Option<&LeaseHandle>,
    ) -> Result<LeaseHandle, DcsError> {
        let secs = ttl.as_secs().max(5);
        if let Ok(mut g) = self.hint.lock() {
            g.ttl_secs = secs;
        }
        if let Some(existing) = current {
            return Ok(existing.clone());
        }
        Ok(LeaseHandle(CompactString::from(format!(
            "k8s:{}:{secs}",
            self.scope
        ))))
    }

    async fn cas_initialize(&self, sys_id: &str) -> Result<CasToken, DcsError> {
        self.mutate_config_annotations(None, Some("initialize"), |ann| {
            ann.insert("initialize".to_owned(), sys_id.to_owned());
            Ok(())
        })
        .await
    }

    async fn acquire_or_renew_leader(
        &self,
        self_name: &str,
        _lease: &LeaseHandle,
        expected: Option<&CasToken>,
    ) -> Result<CasToken, DcsError> {
        let now_ts = Timestamp::now();
        let now_rfc3339 = now_ts.to_string();
        let ttl_secs = self.hint.lock().map_or(30, |g| g.ttl_secs.max(5));

        if self.kube_cfg.use_endpoints {
            let existing: Option<Endpoints> = self
                .get_optional_json(&self.endpoints_item_url(), Duration::from_secs(10))
                .await?;
            let is_create = existing.is_none();

            match (&existing, expected) {
                (None, Some(exp)) => {
                    return Err(DcsError::CasConflict {
                        expected: Some(exp.clone()),
                    });
                }
                (Some(ep), Some(exp)) => {
                    let exp_rv = Self::extract_rv_str(exp)?;
                    let cur_holder = ep
                        .metadata
                        .annotations
                        .as_ref()
                        .and_then(|a| a.get("leader"))
                        .map_or("", String::as_str);
                    if ep.metadata.resource_version.as_deref() != Some(exp_rv)
                        || cur_holder != self_name
                    {
                        return Err(DcsError::CasConflict {
                            expected: Some(exp.clone()),
                        });
                    }
                }
                (Some(ep), None) => {
                    // Only allow takeover when leader annotation is empty OR its TTL has expired!
                    if let Some(ann) = &ep.metadata.annotations
                        && let Some(cur) = ann
                            .get("leader")
                            .map(|s| s.trim())
                            .filter(|s| !s.is_empty())
                    {
                        let prev_ttl = ann
                            .get("ttl")
                            .and_then(|s| s.parse::<u64>().ok())
                            .unwrap_or(ttl_secs);
                        let still_fresh = ann
                            .get("renewTime")
                            .is_none_or(|rt| is_k8s_lease_fresh(rt, prev_ttl, now_ts));
                        if still_fresh && cur != self_name {
                            return Err(DcsError::CasConflict { expected: None });
                        }
                    }
                }
                (None, None) => {}
            }

            let mut ep = existing.unwrap_or_else(|| Endpoints {
                metadata: ObjectMeta {
                    name: Some(self.scope.to_string()),
                    namespace: Some(self.conn.namespace.clone()),
                    labels: Some(self.base_labels()),
                    annotations: Some(BTreeMap::new()),
                    ..ObjectMeta::default()
                },
                subsets: None,
            });

            let ann = ep.metadata.annotations.get_or_insert_with(BTreeMap::new);
            let prev_holder = ann.get("leader").cloned().unwrap_or_default();
            if prev_holder != self_name {
                let prev_trans = ann
                    .get("transitions")
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);
                ann.insert("transitions".to_owned(), (prev_trans + 1).to_string());
                ann.insert("acquireTime".to_owned(), now_rfc3339.clone());
            }
            ann.insert("leader".to_owned(), self_name.to_owned());
            ann.insert("ttl".to_owned(), ttl_secs.to_string());
            ann.insert("renewTime".to_owned(), now_rfc3339);
            ep.subsets = self.build_primary_subsets();

            let saved: Endpoints = self
                .create_or_replace_cas(
                    &self.endpoints_collection_url(),
                    &self.endpoints_item_url(),
                    is_create,
                    &ep,
                    expected,
                )
                .await?;
            Ok(Self::rv_token(&saved.metadata))
        } else {
            let existing: Option<Lease> = self
                .get_optional_json(&self.lease_item_url(), Duration::from_secs(10))
                .await?;
            let is_create = existing.is_none();

            match (&existing, expected) {
                (None, Some(exp)) => {
                    return Err(DcsError::CasConflict {
                        expected: Some(exp.clone()),
                    });
                }
                (Some(l), Some(exp)) => {
                    let exp_rv = Self::extract_rv_str(exp)?;
                    let cur_holder = l
                        .spec
                        .as_ref()
                        .and_then(|s| s.holder_identity.as_deref())
                        .unwrap_or("");
                    if l.metadata.resource_version.as_deref() != Some(exp_rv)
                        || cur_holder != self_name
                    {
                        return Err(DcsError::CasConflict {
                            expected: Some(exp.clone()),
                        });
                    }
                }
                (Some(l), None) => {
                    if let Some(spec) = &l.spec
                        && let Some(cur) = spec
                            .holder_identity
                            .as_deref()
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                    {
                        let prev_ttl = spec
                            .lease_duration_seconds
                            .and_then(|d| u64::try_from(d).ok())
                            .unwrap_or(ttl_secs);
                        let still_fresh = spec.renew_time.as_ref().is_none_or(|MicroTime(dt)| {
                            is_k8s_lease_fresh(&dt.to_rfc3339(), prev_ttl, now_ts)
                        });
                        if still_fresh && cur != self_name {
                            return Err(DcsError::CasConflict { expected: None });
                        }
                    }
                }
                (None, None) => {}
            }

            let now_chrono: DateTime<Utc> = Utc::now();
            let mut lease_obj = existing.unwrap_or_else(|| Lease {
                metadata: ObjectMeta {
                    name: Some(self.scope.to_string()),
                    namespace: Some(self.conn.namespace.clone()),
                    labels: Some(self.base_labels()),
                    ..ObjectMeta::default()
                },
                spec: Some(LeaseSpec::default()),
            });

            let spec = lease_obj.spec.get_or_insert_with(LeaseSpec::default);
            let prev_holder = spec.holder_identity.as_deref().unwrap_or("");
            if prev_holder != self_name {
                spec.lease_transitions =
                    Some(spec.lease_transitions.unwrap_or(0).saturating_add(1));
                spec.acquire_time = Some(MicroTime(now_chrono));
            }
            spec.holder_identity = Some(self_name.to_owned());
            spec.lease_duration_seconds = Some(i32::try_from(ttl_secs).unwrap_or(30));
            spec.renew_time = Some(MicroTime(now_chrono));

            let saved: Lease = self
                .create_or_replace_cas(
                    &self.lease_collection_url(),
                    &self.lease_item_url(),
                    is_create,
                    &lease_obj,
                    expected,
                )
                .await?;
            Ok(Self::rv_token(&saved.metadata))
        }
    }

    async fn release_leader(&self, self_name: &str, expected: &CasToken) -> Result<(), DcsError> {
        let exp_rv = Self::extract_rv_str(expected)?;
        if self.kube_cfg.use_endpoints {
            let Some(mut ep) = self
                .get_optional_json::<Endpoints>(&self.endpoints_item_url(), Duration::from_secs(10))
                .await?
            else {
                return Ok(());
            };
            let cur_holder = ep
                .metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get("leader"))
                .map_or("", String::as_str);
            if ep.metadata.resource_version.as_deref() != Some(exp_rv) || cur_holder != self_name {
                return Err(DcsError::CasConflict {
                    expected: Some(expected.clone()),
                });
            }
            if let Some(ann) = ep.metadata.annotations.as_mut() {
                ann.remove("leader");
            }
            ep.subsets = None;
            let _: Endpoints = self
                .create_or_replace_cas(
                    &self.endpoints_collection_url(),
                    &self.endpoints_item_url(),
                    false,
                    &ep,
                    Some(expected),
                )
                .await?;
        } else {
            let Some(mut lease_obj) = self
                .get_optional_json::<Lease>(&self.lease_item_url(), Duration::from_secs(10))
                .await?
            else {
                return Ok(());
            };
            let cur_holder = lease_obj
                .spec
                .as_ref()
                .and_then(|s| s.holder_identity.as_deref())
                .unwrap_or("");
            if lease_obj.metadata.resource_version.as_deref() != Some(exp_rv)
                || cur_holder != self_name
            {
                return Err(DcsError::CasConflict {
                    expected: Some(expected.clone()),
                });
            }
            if let Some(spec) = lease_obj.spec.as_mut() {
                spec.holder_identity = None;
            }
            let _: Lease = self
                .create_or_replace_cas(
                    &self.lease_collection_url(),
                    &self.lease_item_url(),
                    false,
                    &lease_obj,
                    Some(expected),
                )
                .await?;
        }
        Ok(())
    }

    async fn upsert_member(
        &self,
        self_name: &str,
        data: &MemberData,
        _lease: &LeaseHandle,
    ) -> Result<(), DcsError> {
        if let Ok(mut g) = self.hint.lock() {
            g.last_conn_url = Some(data.conn_url.clone());
        }
        let status_json = serde_json::to_string(data)
            .map_err(|e| DcsError::Unreachable(format!("serializing MemberData: {e}")))?;

        let pod_url = self.pod_item_url(self_name);
        if let Some(mut pod) = self
            .get_optional_json::<Pod>(&pod_url, Duration::from_secs(5))
            .await?
        {
            pod.metadata
                .labels
                .get_or_insert_with(BTreeMap::new)
                .insert(self.role_label_key().to_owned(), data.role.to_string());
            pod.metadata
                .annotations
                .get_or_insert_with(BTreeMap::new)
                .insert("status".to_owned(), status_json);
            let _: Pod = self
                .create_or_replace_cas(&self.pods_collection_url(), &pod_url, false, &pod, None)
                .await?;
            return Ok(());
        }

        // Fallback when running outside a StatefulSet Pod
        let key = format!("member:{self_name}");
        let _ = self
            .mutate_config_annotations(None, None, |ann| {
                ann.insert(key, status_json);
                Ok(())
            })
            .await?;
        Ok(())
    }

    async fn publish_leader_state(
        &self,
        status: &ClusterStatus,
        failsafe_roster: Option<&BTreeMap<CompactString, String>>,
    ) -> Result<(), DcsError> {
        let status_str = serde_json::to_string(status)
            .map_err(|e| DcsError::Unreachable(format!("serializing ClusterStatus: {e}")))?;
        let fs_str = match failsafe_roster {
            Some(roster) => Some(
                serde_json::to_string(roster)
                    .map_err(|e| DcsError::Unreachable(format!("serializing failsafe: {e}")))?,
            ),
            None => None,
        };
        let _ = self
            .mutate_config_annotations(None, None, |ann| {
                ann.insert("status".to_owned(), status_str);
                if let Some(fs_json) = fs_str {
                    ann.insert("failsafe".to_owned(), fs_json);
                }
                Ok(())
            })
            .await?;
        Ok(())
    }

    async fn append_history_entry(&self, entry: &HistoryEntry) -> Result<(), DcsError> {
        let _ = self
            .mutate_config_annotations(None, None, |ann| {
                let mut entries: Vec<HistoryEntry> = ann
                    .get("history")
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or_default();
                if !entries
                    .iter()
                    .any(|h| h.timeline == entry.timeline && h.lsn == entry.lsn)
                {
                    entries.push(entry.clone());
                }
                let serialized = serde_json::to_string(&entries)
                    .map_err(|e| DcsError::Unreachable(format!("serializing history: {e}")))?;
                ann.insert("history".to_owned(), serialized);
                Ok(())
            })
            .await?;
        Ok(())
    }

    async fn cas_sync_state(
        &self,
        next: &SyncState,
        expected: Option<&CasToken>,
    ) -> Result<CasToken, DcsError> {
        let serialized = serde_json::to_string(next)
            .map_err(|e| DcsError::Unreachable(format!("serializing SyncState: {e}")))?;
        self.mutate_config_annotations(expected, Some("sync"), |ann| {
            ann.insert("sync".to_owned(), serialized);
            Ok(())
        })
        .await
    }

    async fn cas_config(
        &self,
        config_json: &serde_json::Value,
        expected: Option<&CasToken>,
    ) -> Result<CasToken, DcsError> {
        let serialized = serde_json::to_string(config_json)
            .map_err(|e| DcsError::Unreachable(format!("serializing config: {e}")))?;
        self.mutate_config_annotations(expected, Some("config"), |ann| {
            ann.insert("config".to_owned(), serialized);
            Ok(())
        })
        .await
    }

    async fn cas_failover(
        &self,
        intent: &FailoverIntent,
        expected: Option<&CasToken>,
    ) -> Result<CasToken, DcsError> {
        let serialized = serde_json::to_string(intent)
            .map_err(|e| DcsError::Unreachable(format!("serializing FailoverIntent: {e}")))?;
        self.mutate_config_annotations(expected, Some("failover"), |ann| {
            ann.insert("failover".to_owned(), serialized);
            Ok(())
        })
        .await
    }

    async fn clear_failover(&self, expected: &CasToken) -> Result<(), DcsError> {
        let _ = self
            .mutate_config_annotations(Some(expected), Some("failover"), |ann| {
                ann.remove("failover");
                Ok(())
            })
            .await?;
        Ok(())
    }

    async fn delete_cluster(&self) -> Result<(), DcsError> {
        let cm_url = self.configmap_item_url();
        let _ = self.apply_auth(self.http.delete(&cm_url)).send().await;
        let ldr_url = if self.kube_cfg.use_endpoints {
            self.endpoints_item_url()
        } else {
            self.lease_item_url()
        };
        let _ = self.apply_auth(self.http.delete(&ldr_url)).send().await;
        Ok(())
    }

    fn subscribe_watches(&self) -> mpsc::Receiver<DcsWatchEvent> {
        let (tx, rx) = mpsc::channel(64);

        // Spawn two resilient NDJSON watch loops:
        // 1) Leader resource (`Endpoints` or `Lease`) -> `LeaderChanged`
        // 2) `<scope>-config` `ConfigMap` -> `ConfigChanged` / `FailoverRequested`
        let leader_watch_url = if self.kube_cfg.use_endpoints {
            format!(
                "{}?watch=true&fieldSelector=metadata.name%3D{}",
                self.endpoints_collection_url(),
                self.scope
            )
        } else {
            format!(
                "{}?watch=true&fieldSelector=metadata.name%3D{}",
                self.lease_collection_url(),
                self.scope
            )
        };
        let cm_watch_url = format!(
            "{}?watch=true&fieldSelector=metadata.name%3D{}",
            self.configmap_collection_url(),
            self.config_map_name()
        );

        spawn_k8s_ndjson_watch_task(
            self.http.clone(),
            self.conn.clone(),
            leader_watch_url,
            self.kube_cfg.use_endpoints,
            tx.clone(),
        );
        spawn_k8s_cm_watch_task(self.http.clone(), self.conn.clone(), cm_watch_url, tx);

        rx
    }
}

fn apply_conn_auth(
    conn: &KubeConnectionConfig,
    req: reqwest::RequestBuilder,
) -> reqwest::RequestBuilder {
    if let Some(ref bearer) = conn.bearer_token {
        req.bearer_auth(bearer.expose())
    } else if let Some((ref u, ref p)) = conn.basic_auth {
        req.basic_auth(u, Some(p.expose()))
    } else {
        req
    }
}

fn spawn_k8s_ndjson_watch_task(
    http: HttpClient,
    conn: KubeConnectionConfig,
    watch_url: String,
    use_endpoints: bool,
    tx: mpsc::Sender<DcsWatchEvent>,
) {
    tokio::spawn(async move {
        loop {
            let Ok(resp) = apply_conn_auth(&conn, http.get(&watch_url)).send().await else {
                if tx.send(DcsWatchEvent::WatchDisconnected).await.is_err() {
                    return;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            };
            if !resp.status().is_success() {
                if tx.send(DcsWatchEvent::WatchDisconnected).await.is_err() {
                    return;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }

            let mut stream = resp.bytes_stream();
            let mut buf = Vec::new();
            while let Some(chunk_res) = stream.next().await {
                let Ok(chunk) = chunk_res else {
                    break;
                };
                buf.extend_from_slice(&chunk);
                while let Some(nl_idx) = buf.iter().position(|&b| b == b'\n') {
                    let line: Vec<u8> = buf.drain(..=nl_idx).collect();
                    let valid_event = if use_endpoints {
                        matches!(
                            serde_json::from_slice::<WatchEvent<Endpoints>>(&line),
                            Ok(WatchEvent::Added(_)
                                | WatchEvent::Modified(_)
                                | WatchEvent::Deleted(_))
                        )
                    } else {
                        matches!(
                            serde_json::from_slice::<WatchEvent<Lease>>(&line),
                            Ok(WatchEvent::Added(_)
                                | WatchEvent::Modified(_)
                                | WatchEvent::Deleted(_))
                        )
                    };
                    if valid_event && tx.send(DcsWatchEvent::LeaderChanged).await.is_err() {
                        return;
                    }
                }
            }
            if tx.send(DcsWatchEvent::WatchDisconnected).await.is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });
}

fn spawn_k8s_cm_watch_task(
    http: HttpClient,
    conn: KubeConnectionConfig,
    watch_url: String,
    tx: mpsc::Sender<DcsWatchEvent>,
) {
    tokio::spawn(async move {
        loop {
            let Ok(resp) = apply_conn_auth(&conn, http.get(&watch_url)).send().await else {
                if tx.send(DcsWatchEvent::WatchDisconnected).await.is_err() {
                    return;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            };
            if !resp.status().is_success() {
                if tx.send(DcsWatchEvent::WatchDisconnected).await.is_err() {
                    return;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }

            let mut stream = resp.bytes_stream();
            let mut buf = Vec::new();
            while let Some(chunk_res) = stream.next().await {
                let Ok(chunk) = chunk_res else {
                    break;
                };
                buf.extend_from_slice(&chunk);
                while let Some(nl_idx) = buf.iter().position(|&b| b == b'\n') {
                    let line: Vec<u8> = buf.drain(..=nl_idx).collect();
                    if let Ok(
                        WatchEvent::Added(cm) | WatchEvent::Modified(cm) | WatchEvent::Deleted(cm),
                    ) = serde_json::from_slice::<WatchEvent<ConfigMap>>(&line)
                    {
                        let ev = if cm
                            .metadata
                            .annotations
                            .as_ref()
                            .is_some_and(|a| a.contains_key("failover"))
                        {
                            DcsWatchEvent::FailoverRequested
                        } else {
                            DcsWatchEvent::ConfigChanged
                        };
                        if tx.send(ev).await.is_err() {
                            return;
                        }
                    }
                }
            }
            if tx.send(DcsWatchEvent::WatchDisconnected).await.is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    });
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::collections::HashMap;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    #[derive(Default)]
    struct MockK8sStore {
        next_rv: u64,
        objects: HashMap<String, serde_json::Value>,
    }

    impl MockK8sStore {
        fn alloc_rv(&mut self) -> String {
            self.next_rv += 1;
            self.next_rv.to_string()
        }
    }

    async fn spawn_mock_k8s_apiserver() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let store = Arc::new(Mutex::new(MockK8sStore::default()));

        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let st = Arc::clone(&store);
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 65536];
                    if let Ok(n) = sock.read(&mut buf).await
                        && n > 0
                        && let Some((head, body_str)) =
                            String::from_utf8_lossy(&buf[..n]).split_once("\r\n\r\n")
                    {
                        let head = head.to_owned();
                        let mut full_body = body_str.as_bytes().to_vec();
                        let content_len: usize = head
                            .lines()
                            .find_map(|l| {
                                let (k, v) = l.split_once(':')?;
                                if k.eq_ignore_ascii_case("content-length") {
                                    v.trim().parse().ok()
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(0);
                        while full_body.len() < content_len {
                            let mut more = vec![0u8; content_len - full_body.len()];
                            if sock.read_exact(&mut more).await.is_err() {
                                break;
                            }
                            full_body.extend_from_slice(&more);
                        }

                        let first_line = head.lines().next().unwrap_or("");
                        let mut parts = first_line.split_whitespace();
                        let method = parts.next().unwrap_or("GET");
                        let raw_target = parts.next().unwrap_or("/");
                        let (path, query) = raw_target
                            .split_once('?')
                            .map_or((raw_target, ""), |(p, q)| (p, q));

                        let (status_line, resp_json) =
                            handle_mock_k8s_req(&st, method, path, query, &full_body);
                        let payload = serde_json::to_vec(&resp_json).unwrap();
                        let http_resp = format!(
                            "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            payload.len()
                        );
                        let _ = sock.write_all(http_resp.as_bytes()).await;
                        let _ = sock.write_all(&payload).await;
                    }
                });
            }
        });

        format!("http://{addr}")
    }

    fn handle_mock_k8s_req(
        store: &Arc<Mutex<MockK8sStore>>,
        method: &str,
        path: &str,
        query: &str,
        body: &[u8],
    ) -> (&'static str, serde_json::Value) {
        let mut g = store.lock().unwrap();
        if method == "GET" && path.ends_with("/pods") {
            return (
                "200 OK",
                serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "PodList",
                    "metadata": {"resourceVersion": "1"},
                    "items": []
                }),
            );
        }
        if method == "GET" && query.contains("watch=true") {
            return ("200 OK", serde_json::json!({}));
        }
        match method {
            "GET" => match g.objects.get(path) {
                Some(obj) => ("200 OK", obj.clone()),
                None => ("404 Not Found", serde_json::json!({"reason": "NotFound"})),
            },
            "POST" => {
                let mut val: serde_json::Value = serde_json::from_slice(body).unwrap();
                let name = val["metadata"]["name"]
                    .as_str()
                    .unwrap_or("item")
                    .to_owned();
                let item_path = format!("{path}/{name}");
                if g.objects.contains_key(&item_path) {
                    return (
                        "409 Conflict",
                        serde_json::json!({"reason": "AlreadyExists"}),
                    );
                }
                let rv = g.alloc_rv();
                val["metadata"]["resourceVersion"] = serde_json::Value::String(rv);
                g.objects.insert(item_path, val.clone());
                ("201 Created", val)
            }
            "PUT" => {
                let mut val: serde_json::Value = serde_json::from_slice(body).unwrap();
                let Some(existing) = g.objects.get(path).cloned() else {
                    return ("404 Not Found", serde_json::json!({"reason": "NotFound"}));
                };
                let cur_rv = existing["metadata"]["resourceVersion"].as_str();
                let req_rv = val["metadata"]["resourceVersion"].as_str();
                if req_rv.is_some() && req_rv != cur_rv {
                    return ("409 Conflict", serde_json::json!({"reason": "Conflict"}));
                }
                let next_rv = g.alloc_rv();
                val["metadata"]["resourceVersion"] = serde_json::Value::String(next_rv);
                g.objects.insert(path.to_owned(), val.clone());
                ("200 OK", val)
            }
            "DELETE" => {
                g.objects.remove(path);
                ("200 OK", serde_json::json!({"status": "Success"}))
            }
            _ => ("405 Method Not Allowed", serde_json::json!({})),
        }
    }

    #[test]
    fn parses_kubeconfig_yaml_via_saphyr() {
        let kc = r#"
apiVersion: v1
kind: Config
current-context: prod-ctx
clusters:
  - name: prod-cluster
    cluster:
      server: https://10.96.0.1:6443/
      insecure-skip-tls-verify: true
contexts:
  - name: prod-ctx
    context:
      cluster: prod-cluster
      user: patroni-sa
      namespace: db-ha
users:
  - name: patroni-sa
    user:
      token: top-secret-jwt
"#;
        let conn = KubeConnectionConfig::from_kubeconfig_str(kc, None, None).unwrap();
        assert_eq!(conn.api_server_url, "https://10.96.0.1:6443");
        assert_eq!(conn.namespace, "db-ha");
        assert!(conn.insecure_skip_tls_verify);
        assert_eq!(
            conn.bearer_token.as_ref().map(SecretString::expose),
            Some("top-secret-jwt")
        );
    }

    #[tokio::test]
    async fn endpoints_and_lease_modes_enforce_resource_version_cas_and_sync_primary_subsets() {
        let server_url = spawn_mock_k8s_apiserver().await;

        // 1. Endpoints mode (`use_endpoints: true`) + active primary Pod subset IP sync
        let ep_cfg = KubeConfig {
            namespace: Some(CompactString::const_new("postgres-ha")),
            use_endpoints: true,
            pod_ip: Some("10.244.1.15".to_owned()),
            ..KubeConfig::default()
        };
        let dcs_ep = KubeDcs::from_config("batman", ep_cfg, Some(&server_url)).unwrap();
        let lease = dcs_ep
            .ensure_lease(Duration::from_secs(30), None)
            .await
            .unwrap();

        let _init_tok = dcs_ep.cas_initialize("73000011112222").await.unwrap();
        assert!(matches!(
            dcs_ep.cas_initialize("other").await,
            Err(DcsError::CasConflict { .. })
        ));

        let ldr_tok = dcs_ep
            .acquire_or_renew_leader("pg-0", &lease, None)
            .await
            .unwrap();

        // Competing node cannot steal unexpired Endpoints leader
        assert!(matches!(
            dcs_ep.acquire_or_renew_leader("pg-1", &lease, None).await,
            Err(DcsError::CasConflict { .. })
        ));

        // Verify Endpoints subsets carries primary Pod IP 10.244.1.15:5432!
        let raw_ep: Endpoints = dcs_ep
            .get_optional_json(&dcs_ep.endpoints_item_url(), Duration::from_secs(5))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            raw_ep.subsets.as_ref().unwrap()[0]
                .addresses
                .as_ref()
                .unwrap()[0]
                .ip,
            "10.244.1.15"
        );

        let snap = dcs_ep.fetch_cluster(Duration::from_secs(5)).await.unwrap();
        assert_eq!(snap.leader.as_ref().map(|l| l.value.as_str()), Some("pg-0"));

        // Stale resourceVersion CAS fails with CasConflict
        let stale_tok = CasToken::ResourceVersion(CompactString::const_new("999999"));
        assert!(matches!(
            dcs_ep
                .acquire_or_renew_leader("pg-0", &lease, Some(&stale_tok))
                .await,
            Err(DcsError::CasConflict { .. })
        ));

        let renewed_tok = dcs_ep
            .acquire_or_renew_leader("pg-0", &lease, Some(&ldr_tok))
            .await
            .unwrap();
        dcs_ep.release_leader("pg-0", &renewed_tok).await.unwrap();

        // 2. Lease mode (`use_endpoints: false`)
        let lease_cfg = KubeConfig {
            namespace: Some(CompactString::const_new("postgres-ha")),
            use_endpoints: false,
            ..KubeConfig::default()
        };
        let dcs_lease = KubeDcs::from_config("robin", lease_cfg, Some(&server_url)).unwrap();
        let l_handle = dcs_lease
            .ensure_lease(Duration::from_secs(30), None)
            .await
            .unwrap();
        let k8s_lease_tok = dcs_lease
            .acquire_or_renew_leader("robin-0", &l_handle, None)
            .await
            .unwrap();
        let snap2 = dcs_lease
            .fetch_cluster(Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(
            snap2.leader.as_ref().map(|l| l.value.as_str()),
            Some("robin-0")
        );
        dcs_lease
            .release_leader("robin-0", &k8s_lease_tok)
            .await
            .unwrap();
    }
}
