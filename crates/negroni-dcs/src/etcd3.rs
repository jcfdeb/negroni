use std::collections::BTreeMap;
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use compact_str::CompactString;
use http::uri::PathAndQuery;
use negroni_config::Etcd3Config;
use negroni_types::{
    CasToken, ClusterSnapshot, ClusterStatus, FailoverIntent, HistoryEntry, LeaseHandle,
    MemberData, SyncState, Versioned, WalLsn,
};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_stream::wrappers::ReceiverStream;
use tonic::client::Grpc;
use tonic::codec::{ProstCodec, Streaming};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::{Request, Status};

use crate::{DcsBackend, DcsError, DcsWatchEvent};

/// Wire-exact `etcd` v3 (`mvccpb` + `etcdserverpb`) Protobuf messages derived directly with
/// `prost` so `negroni` compiles hermetically without an external C++ `protoc` binary.
pub mod pb {
    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct ResponseHeader {
        #[prost(uint64, tag = "1")]
        pub cluster_id: u64,
        #[prost(uint64, tag = "2")]
        pub member_id: u64,
        #[prost(int64, tag = "3")]
        pub revision: i64,
        #[prost(uint64, tag = "4")]
        pub raft_term: u64,
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct KeyValue {
        #[prost(bytes = "vec", tag = "1")]
        pub key: Vec<u8>,
        #[prost(int64, tag = "2")]
        pub create_revision: i64,
        #[prost(int64, tag = "3")]
        pub mod_revision: i64,
        #[prost(int64, tag = "4")]
        pub version: i64,
        #[prost(bytes = "vec", tag = "5")]
        pub value: Vec<u8>,
        #[prost(int64, tag = "6")]
        pub lease: i64,
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct Event {
        #[prost(int32, tag = "1")]
        pub r#type: i32,
        #[prost(message, optional, tag = "2")]
        pub kv: Option<KeyValue>,
        #[prost(message, optional, tag = "3")]
        pub prev_kv: Option<KeyValue>,
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct RangeRequest {
        #[prost(bytes = "vec", tag = "1")]
        pub key: Vec<u8>,
        #[prost(bytes = "vec", tag = "2")]
        pub range_end: Vec<u8>,
        #[prost(int64, tag = "3")]
        pub limit: i64,
        #[prost(int64, tag = "4")]
        pub revision: i64,
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct RangeResponse {
        #[prost(message, optional, tag = "1")]
        pub header: Option<ResponseHeader>,
        #[prost(message, repeated, tag = "2")]
        pub kvs: Vec<KeyValue>,
        #[prost(bool, tag = "3")]
        pub more: bool,
        #[prost(int64, tag = "4")]
        pub count: i64,
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct PutRequest {
        #[prost(bytes = "vec", tag = "1")]
        pub key: Vec<u8>,
        #[prost(bytes = "vec", tag = "2")]
        pub value: Vec<u8>,
        #[prost(int64, tag = "3")]
        pub lease: i64,
        #[prost(bool, tag = "4")]
        pub prev_kv: bool,
        #[prost(bool, tag = "5")]
        pub ignore_value: bool,
        #[prost(bool, tag = "6")]
        pub ignore_lease: bool,
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct PutResponse {
        #[prost(message, optional, tag = "1")]
        pub header: Option<ResponseHeader>,
        #[prost(message, optional, tag = "2")]
        pub prev_kv: Option<KeyValue>,
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct DeleteRangeRequest {
        #[prost(bytes = "vec", tag = "1")]
        pub key: Vec<u8>,
        #[prost(bytes = "vec", tag = "2")]
        pub range_end: Vec<u8>,
        #[prost(bool, tag = "3")]
        pub prev_kv: bool,
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct DeleteRangeResponse {
        #[prost(message, optional, tag = "1")]
        pub header: Option<ResponseHeader>,
        #[prost(int64, tag = "2")]
        pub deleted: i64,
        #[prost(message, repeated, tag = "3")]
        pub prev_kvs: Vec<KeyValue>,
    }

    pub const COMPARE_EQUAL: i32 = 0;

    pub const TARGET_VERSION: i32 = 0;
    pub const TARGET_MOD: i32 = 2;
    pub const TARGET_VALUE: i32 = 3;

    #[derive(Clone, PartialEq, Eq, ::prost::Oneof)]
    pub enum TargetUnion {
        #[prost(int64, tag = "4")]
        Version(i64),
        #[prost(int64, tag = "5")]
        CreateRevision(i64),
        #[prost(int64, tag = "6")]
        ModRevision(i64),
        #[prost(bytes, tag = "7")]
        Value(Vec<u8>),
        #[prost(int64, tag = "8")]
        Lease(i64),
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct Compare {
        #[prost(int32, tag = "1")]
        pub result: i32,
        #[prost(int32, tag = "2")]
        pub target: i32,
        #[prost(bytes = "vec", tag = "3")]
        pub key: Vec<u8>,
        #[prost(oneof = "TargetUnion", tags = "4, 5, 6, 7, 8")]
        pub target_union: Option<TargetUnion>,
    }

    impl Compare {
        pub fn version_eq(key: impl Into<Vec<u8>>, ver: i64) -> Self {
            Self {
                result: COMPARE_EQUAL,
                target: TARGET_VERSION,
                key: key.into(),
                target_union: Some(TargetUnion::Version(ver)),
            }
        }

        pub fn mod_revision_eq(key: impl Into<Vec<u8>>, rev: i64) -> Self {
            Self {
                result: COMPARE_EQUAL,
                target: TARGET_MOD,
                key: key.into(),
                target_union: Some(TargetUnion::ModRevision(rev)),
            }
        }

        pub fn value_eq(key: impl Into<Vec<u8>>, val: impl Into<Vec<u8>>) -> Self {
            Self {
                result: COMPARE_EQUAL,
                target: TARGET_VALUE,
                key: key.into(),
                target_union: Some(TargetUnion::Value(val.into())),
            }
        }
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Oneof)]
    pub enum RequestUnion {
        #[prost(message, tag = "1")]
        RequestRange(RangeRequest),
        #[prost(message, tag = "2")]
        RequestPut(PutRequest),
        #[prost(message, tag = "3")]
        RequestDeleteRange(DeleteRangeRequest),
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct RequestOp {
        #[prost(oneof = "RequestUnion", tags = "1, 2, 3")]
        pub request: Option<RequestUnion>,
    }

    impl RequestOp {
        pub fn put(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>, lease: i64) -> Self {
            Self {
                request: Some(RequestUnion::RequestPut(PutRequest {
                    key: key.into(),
                    value: value.into(),
                    lease,
                    prev_kv: false,
                    ignore_value: false,
                    ignore_lease: false,
                })),
            }
        }

        pub fn delete_key(key: impl Into<Vec<u8>>) -> Self {
            Self {
                request: Some(RequestUnion::RequestDeleteRange(DeleteRangeRequest {
                    key: key.into(),
                    range_end: Vec::new(),
                    prev_kv: false,
                })),
            }
        }
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Oneof)]
    pub enum ResponseUnion {
        #[prost(message, tag = "1")]
        ResponseRange(RangeResponse),
        #[prost(message, tag = "2")]
        ResponsePut(PutResponse),
        #[prost(message, tag = "3")]
        ResponseDeleteRange(DeleteRangeResponse),
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct ResponseOp {
        #[prost(oneof = "ResponseUnion", tags = "1, 2, 3")]
        pub response: Option<ResponseUnion>,
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct TxnRequest {
        #[prost(message, repeated, tag = "1")]
        pub compare: Vec<Compare>,
        #[prost(message, repeated, tag = "2")]
        pub success: Vec<RequestOp>,
        #[prost(message, repeated, tag = "3")]
        pub failure: Vec<RequestOp>,
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct TxnResponse {
        #[prost(message, optional, tag = "1")]
        pub header: Option<ResponseHeader>,
        #[prost(bool, tag = "2")]
        pub succeeded: bool,
        #[prost(message, repeated, tag = "3")]
        pub responses: Vec<ResponseOp>,
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct LeaseGrantRequest {
        #[prost(int64, tag = "1")]
        pub ttl: i64,
        #[prost(int64, tag = "2")]
        pub id: i64,
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct LeaseGrantResponse {
        #[prost(message, optional, tag = "1")]
        pub header: Option<ResponseHeader>,
        #[prost(int64, tag = "2")]
        pub id: i64,
        #[prost(int64, tag = "3")]
        pub ttl: i64,
        #[prost(string, tag = "4")]
        pub error: String,
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct LeaseKeepAliveRequest {
        #[prost(int64, tag = "1")]
        pub id: i64,
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct LeaseKeepAliveResponse {
        #[prost(message, optional, tag = "1")]
        pub header: Option<ResponseHeader>,
        #[prost(int64, tag = "2")]
        pub id: i64,
        #[prost(int64, tag = "3")]
        pub ttl: i64,
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct WatchCreateRequest {
        #[prost(bytes = "vec", tag = "1")]
        pub key: Vec<u8>,
        #[prost(bytes = "vec", tag = "2")]
        pub range_end: Vec<u8>,
        #[prost(int64, tag = "3")]
        pub start_revision: i64,
        #[prost(bool, tag = "4")]
        pub progress_notify: bool,
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Oneof)]
    pub enum WatchRequestUnion {
        #[prost(message, tag = "1")]
        CreateRequest(WatchCreateRequest),
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct WatchRequest {
        #[prost(oneof = "WatchRequestUnion", tags = "1")]
        pub request_union: Option<WatchRequestUnion>,
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct WatchResponse {
        #[prost(message, optional, tag = "1")]
        pub header: Option<ResponseHeader>,
        #[prost(int64, tag = "2")]
        pub watch_id: i64,
        #[prost(bool, tag = "3")]
        pub created: bool,
        #[prost(bool, tag = "4")]
        pub canceled: bool,
        #[prost(int64, tag = "5")]
        pub compact_revision: i64,
        #[prost(string, tag = "6")]
        pub cancel_reason: String,
        #[prost(bool, tag = "7")]
        pub fragment: bool,
        #[prost(message, repeated, tag = "11")]
        pub events: Vec<Event>,
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct AuthenticateRequest {
        #[prost(string, tag = "1")]
        pub name: String,
        #[prost(string, tag = "2")]
        pub password: String,
    }

    #[derive(Clone, PartialEq, Eq, ::prost::Message)]
    pub struct AuthenticateResponse {
        #[prost(message, optional, tag = "1")]
        pub header: Option<ResponseHeader>,
        #[prost(string, tag = "2")]
        pub token: String,
    }
}

/// Computes the `etcd` v3 `range_end` byte slice matching every key prefixed by `prefix`.
///
/// # Examples
///
/// ```
/// use negroni_dcs::etcd3::prefix_range_end;
///
/// assert_eq!(prefix_range_end(b"/service/batman/"), b"/service/batman0");
/// assert_eq!(prefix_range_end(&[0x61, 0xff]), vec![0x62]);
/// assert_eq!(prefix_range_end(&[0xff, 0xff]), vec![0x00]);
/// ```
pub fn prefix_range_end(prefix: &[u8]) -> Vec<u8> {
    for (idx, &byte) in prefix.iter().enumerate().rev() {
        if byte < 0xff {
            let mut out = prefix[..=idx].to_vec();
            out[idx] = byte + 1;
            return out;
        }
    }
    vec![0x00]
}

/// Raw key/value/revision tuple decoded from an `etcd` v3 prefix range response.
#[derive(Debug, Clone)]
pub struct RawEtcdKv<'a> {
    pub key: &'a str,
    pub value: &'a [u8],
    pub mod_revision: u64,
}

/// Normalizes an `etcd3` endpoint string into a scheme-prefixed URI (`http://` or `https://`).
///
/// # Examples
///
/// ```
/// use negroni_dcs::etcd3::normalize_etcd_endpoint;
///
/// assert_eq!(normalize_etcd_endpoint("127.0.0.1:2379", false), "http://127.0.0.1:2379");
/// assert_eq!(normalize_etcd_endpoint("etcd-1.internal:2379", true), "https://etcd-1.internal:2379");
/// assert_eq!(normalize_etcd_endpoint("https://10.0.0.5:2379", false), "https://10.0.0.5:2379");
/// ```
pub fn normalize_etcd_endpoint(raw: &str, default_https: bool) -> String {
    let trimmed = raw.trim();
    if let Some(rest) = trimmed.strip_prefix("etcd3://") {
        let scheme = if default_https { "https" } else { "http" };
        return format!("{scheme}://{rest}");
    }
    if let Some(rest) = trimmed.strip_prefix("http+etcd3://") {
        return format!("http://{rest}");
    }
    if let Some(rest) = trimmed.strip_prefix("https+etcd3://") {
        return format!("https://{rest}");
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        return trimmed.to_owned();
    }
    if default_https {
        format!("https://{trimmed}")
    } else {
        format!("http://{trimmed}")
    }
}

/// Parses an atomic `etcd` v3 prefix snapshot (`/<namespace>/<scope>/...`) into a [`ClusterSnapshot`].
///
/// Accepts both Patroni 4.x JSON `/status` objects and legacy Patroni bare integer LSN `/status`
/// strings for mixed-node rolling migration compatibility.
///
/// # Examples
///
/// ```
/// use negroni_dcs::etcd3::{RawEtcdKv, parse_etcd_prefix_kvs};
/// use negroni_types::CasToken;
///
/// let kvs = [
///     RawEtcdKv { key: "/service/batman/initialize", value: b"7345891203948", mod_revision: 3 },
///     RawEtcdKv { key: "/service/batman/leader", value: b"pg-1", mod_revision: 11 },
///     RawEtcdKv { key: "/service/batman/status", value: br#"{"optime":4096}"#, mod_revision: 12 },
/// ];
/// let snap = parse_etcd_prefix_kvs("/service/batman", &kvs);
/// assert_eq!(snap.leader.as_ref().map(|l| l.value.as_str()), Some("pg-1"));
/// assert_eq!(snap.leader.as_ref().map(|l| &l.token), Some(&CasToken::Revision(11)));
/// assert_eq!(snap.status.as_ref().map(|s| s.value.optime.0), Some(4096));
/// ```
pub fn parse_etcd_prefix_kvs(prefix: &str, kvs: &[RawEtcdKv<'_>]) -> ClusterSnapshot {
    let clean_prefix = prefix.trim_end_matches('/');
    let mut snap = ClusterSnapshot::default();

    for kv in kvs {
        let Some(rel) = kv
            .key
            .strip_prefix(clean_prefix)
            .and_then(|s| s.strip_prefix('/'))
        else {
            continue;
        };
        let token = CasToken::Revision(kv.mod_revision);
        let Ok(val_str) = std::str::from_utf8(kv.value) else {
            continue;
        };

        if let Some(member_name) = rel.strip_prefix("members/") {
            if !member_name.is_empty()
                && !member_name.contains('/')
                && let Ok(member) = serde_json::from_slice::<MemberData>(kv.value)
            {
                snap.members.insert(
                    CompactString::from(member_name),
                    Versioned {
                        value: member,
                        token,
                    },
                );
            }
            continue;
        }

        match rel {
            "initialize" => {
                snap.initialize = Some(Versioned {
                    value: CompactString::from(val_str.trim()),
                    token,
                });
            }
            "leader" => {
                let trimmed = val_str.trim();
                if !trimmed.is_empty() {
                    snap.leader = Some(Versioned {
                        value: CompactString::from(trimmed),
                        token,
                    });
                }
            }
            "config" => {
                if let Ok(json_val) = serde_json::from_slice::<serde_json::Value>(kv.value) {
                    snap.config = Some(Versioned {
                        value: json_val,
                        token,
                    });
                }
            }
            "status" => {
                if let Ok(status) = serde_json::from_slice::<ClusterStatus>(kv.value) {
                    snap.status = Some(Versioned {
                        value: status,
                        token,
                    });
                } else if let Ok(raw_lsn) = val_str.trim().parse::<u64>() {
                    snap.status = Some(Versioned {
                        value: ClusterStatus {
                            optime: WalLsn(raw_lsn),
                            slots: BTreeMap::new(),
                            retain_slots: Vec::new(),
                        },
                        token,
                    });
                }
            }
            "sync" => {
                if let Ok(sync) = serde_json::from_slice::<SyncState>(kv.value) {
                    snap.sync = Some(Versioned { value: sync, token });
                }
            }
            "failover" => {
                if let Ok(fo) = serde_json::from_slice::<FailoverIntent>(kv.value) {
                    // Patroni writes an empty JSON `{}` or deletes `/failover` when cleared;
                    // ignore empty intents where all three fields are `None`.
                    if fo.leader.is_some() || fo.candidate.is_some() || fo.scheduled_at.is_some() {
                        snap.failover = Some(Versioned { value: fo, token });
                    }
                }
            }
            "failsafe" => {
                if let Ok(fs_map) =
                    serde_json::from_slice::<BTreeMap<CompactString, String>>(kv.value)
                {
                    snap.failsafe = Some(fs_map);
                }
            }
            "history" => {
                if let Ok(hist) = serde_json::from_slice::<Vec<HistoryEntry>>(kv.value) {
                    snap.history = hist;
                }
            }
            _ => {}
        }
    }

    snap
}

/// Maps a relative key suffix under `/<namespace>/<scope>/` to a [`DcsWatchEvent`].
pub fn classify_etcd_watch_key(prefix: &str, full_key: &str) -> Option<DcsWatchEvent> {
    let clean_prefix = prefix.trim_end_matches('/');
    let rel = full_key.strip_prefix(clean_prefix)?.strip_prefix('/')?;
    if rel == "leader" {
        Some(DcsWatchEvent::LeaderChanged)
    } else if rel == "config" {
        Some(DcsWatchEvent::ConfigChanged)
    } else if rel == "failover" {
        Some(DcsWatchEvent::FailoverRequested)
    } else if rel.starts_with("members/") || rel == "initialize" || rel == "sync" {
        Some(DcsWatchEvent::TopologyChanged)
    } else {
        None
    }
}

#[derive(Clone)]
struct EtcdGrpcClient {
    channel: Channel,
    auth_token: Option<MetadataValue<Ascii>>,
}

impl EtcdGrpcClient {
    fn attach_auth<T>(&self, msg: T) -> Request<T> {
        let mut req = Request::new(msg);
        if let Some(ref tok) = self.auth_token {
            req.metadata_mut().insert("token", tok.clone());
        }
        req
    }

    async fn unary<Req, Resp>(&self, path: &'static str, msg: Req) -> Result<Resp, DcsError>
    where
        Req: ::prost::Message + 'static,
        Resp: ::prost::Message + Default + 'static,
    {
        let mut grpc = Grpc::new(self.channel.clone());
        grpc.ready()
            .await
            .map_err(|e| DcsError::Unreachable(format!("etcd3 channel not ready: {e}")))?;
        let resp = grpc
            .unary(
                self.attach_auth(msg),
                PathAndQuery::from_static(path),
                ProstCodec::default(),
            )
            .await
            .map_err(map_grpc_status)?;
        Ok(resp.into_inner())
    }

    async fn range(&self, req: pb::RangeRequest) -> Result<pb::RangeResponse, DcsError> {
        self.unary("/etcdserverpb.KV/Range", req).await
    }

    async fn put(&self, req: pb::PutRequest) -> Result<pb::PutResponse, DcsError> {
        self.unary("/etcdserverpb.KV/Put", req).await
    }

    async fn delete_range(
        &self,
        req: pb::DeleteRangeRequest,
    ) -> Result<pb::DeleteRangeResponse, DcsError> {
        self.unary("/etcdserverpb.KV/DeleteRange", req).await
    }

    async fn txn(&self, req: pb::TxnRequest) -> Result<pb::TxnResponse, DcsError> {
        self.unary("/etcdserverpb.KV/Txn", req).await
    }

    async fn lease_grant(
        &self,
        req: pb::LeaseGrantRequest,
    ) -> Result<pb::LeaseGrantResponse, DcsError> {
        self.unary("/etcdserverpb.Lease/LeaseGrant", req).await
    }

    async fn lease_keep_alive(
        &self,
    ) -> Result<
        (
            mpsc::Sender<pb::LeaseKeepAliveRequest>,
            Streaming<pb::LeaseKeepAliveResponse>,
        ),
        DcsError,
    > {
        let (tx, rx) = mpsc::channel(8);
        let mut grpc = Grpc::new(self.channel.clone());
        grpc.ready().await.map_err(|e| {
            DcsError::Unreachable(format!("etcd3 keepalive channel not ready: {e}"))
        })?;
        let resp = grpc
            .streaming(
                self.attach_auth(ReceiverStream::new(rx)),
                PathAndQuery::from_static("/etcdserverpb.Lease/LeaseKeepAlive"),
                ProstCodec::default(),
            )
            .await
            .map_err(map_grpc_status)?;
        Ok((tx, resp.into_inner()))
    }

    async fn open_watch(
        &self,
        prefix_bytes: Vec<u8>,
    ) -> Result<(mpsc::Sender<pb::WatchRequest>, Streaming<pb::WatchResponse>), DcsError> {
        let (tx, rx) = mpsc::channel(4);
        let create = pb::WatchRequest {
            request_union: Some(pb::WatchRequestUnion::CreateRequest(
                pb::WatchCreateRequest {
                    range_end: prefix_range_end(&prefix_bytes),
                    key: prefix_bytes,
                    start_revision: 0,
                    progress_notify: true,
                },
            )),
        };
        let _ = tx.send(create).await;

        let mut grpc = Grpc::new(self.channel.clone());
        grpc.ready()
            .await
            .map_err(|e| DcsError::Unreachable(format!("etcd3 watch channel not ready: {e}")))?;
        let resp = grpc
            .streaming(
                self.attach_auth(ReceiverStream::new(rx)),
                PathAndQuery::from_static("/etcdserverpb.Watch/Watch"),
                ProstCodec::default(),
            )
            .await
            .map_err(map_grpc_status)?;
        Ok((tx, resp.into_inner()))
    }
}

fn map_grpc_status(status: Status) -> DcsError {
    let msg = status.message();
    if msg.contains("requested lease not found") || msg.contains("lease not found") {
        DcsError::LeaseExpired
    } else {
        DcsError::Unreachable(format!("etcd3 gRPC {}: {msg}", status.code()))
    }
}

struct ActiveLeaseTask {
    lease_id: i64,
    handle: LeaseHandle,
    ttl: Duration,
    alive: Arc<AtomicBool>,
    stop_tx: watch::Sender<bool>,
    join: JoinHandle<()>,
}

impl Drop for ActiveLeaseTask {
    fn drop(&mut self) {
        let _ = self.stop_tx.send(true);
        self.join.abort();
    }
}

/// Production `etcd` v3 DCS driver using single-RTT `Txn` CAS operations, a background gRPC
/// `LeaseKeepAlive` stream, atomic prefix snapshot reads, and resilient prefix watch reconnects.
pub struct Etcd3Dcs {
    client: EtcdGrpcClient,
    prefix: String,
    active_lease: Arc<Mutex<Option<ActiveLeaseTask>>>,
}

impl std::fmt::Debug for Etcd3Dcs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Etcd3Dcs")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl Etcd3Dcs {
    pub async fn connect(cfg: &Etcd3Config, prefix: impl Into<String>) -> Result<Self, DcsError> {
        let raw_endpoints = cfg.endpoints();
        if raw_endpoints.is_empty() {
            return Err(DcsError::Unreachable(
                "etcd3 configuration has no host or hosts configured".to_owned(),
            ));
        }

        let default_https = cfg
            .protocol
            .as_deref()
            .is_some_and(|p| p.eq_ignore_ascii_case("https"))
            || cfg.cacert.is_some()
            || cfg.cert.is_some();

        let normalized_uris: Vec<String> = raw_endpoints
            .iter()
            .map(|e| normalize_etcd_endpoint(e, default_https))
            .collect();

        let needs_tls = normalized_uris.iter().any(|u| u.starts_with("https://"))
            || cfg.cacert.is_some()
            || cfg.cert.is_some();

        let tls_cfg = if needs_tls {
            let mut tls = ClientTlsConfig::new().with_native_roots();
            if let Some(ref ca_path) = cfg.cacert {
                let ca_pem = fs::read(ca_path).map_err(|e| {
                    DcsError::Unreachable(format!(
                        "reading etcd3 cacert '{}': {e}",
                        ca_path.display()
                    ))
                })?;
                tls = tls.ca_certificate(Certificate::from_pem(ca_pem));
            }
            if let (Some(cert_path), Some(key_path)) = (&cfg.cert, &cfg.key) {
                let cert_pem = fs::read(cert_path).map_err(|e| {
                    DcsError::Unreachable(format!(
                        "reading etcd3 client cert '{}': {e}",
                        cert_path.display()
                    ))
                })?;
                let key_pem = fs::read(key_path).map_err(|e| {
                    DcsError::Unreachable(format!(
                        "reading etcd3 client key '{}': {e}",
                        key_path.display()
                    ))
                })?;
                tls = tls.identity(Identity::from_pem(cert_pem, key_pem));
            }
            Some(tls)
        } else {
            None
        };

        let mut endpoints = Vec::with_capacity(normalized_uris.len());
        for uri in normalized_uris {
            let mut ep = Endpoint::from_shared(uri.clone())
                .map_err(|e| DcsError::Unreachable(format!("invalid etcd3 URI '{uri}': {e}")))?
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(10))
                .tcp_keepalive(Some(Duration::from_secs(15)))
                .http2_keep_alive_interval(Duration::from_secs(10))
                .keep_alive_timeout(Duration::from_secs(5))
                .keep_alive_while_idle(true);
            if let Some(ref tls) = tls_cfg {
                ep = ep
                    .tls_config(tls.clone())
                    .map_err(|e| DcsError::Unreachable(format!("etcd3 TLS setup failed: {e}")))?;
            }
            endpoints.push(ep);
        }

        let channel = Channel::balance_list(endpoints.into_iter());
        let mut client = EtcdGrpcClient {
            channel,
            auth_token: None,
        };

        if let Some(ref user) = cfg.username {
            let pw = cfg
                .password
                .as_ref()
                .map_or("", negroni_config::SecretString::expose);
            let auth_resp: pb::AuthenticateResponse = client
                .unary(
                    "/etcdserverpb.Auth/Authenticate",
                    pb::AuthenticateRequest {
                        name: user.as_str().to_owned(),
                        password: pw.to_owned(),
                    },
                )
                .await?;
            let meta = MetadataValue::try_from(auth_resp.token)
                .map_err(|e| DcsError::Unreachable(format!("invalid etcd3 auth token: {e}")))?;
            client.auth_token = Some(meta);
        }

        let raw_prefix = prefix.into();
        let normalized_prefix = format!("/{}", raw_prefix.trim_matches('/'));
        Ok(Self {
            client,
            prefix: normalized_prefix,
            active_lease: Arc::new(Mutex::new(None)),
        })
    }

    fn key(&self, suffix: &str) -> String {
        format!("{}/{}", self.prefix, suffix.trim_start_matches('/'))
    }

    fn parse_lease_id(lease: &LeaseHandle) -> Result<i64, DcsError> {
        lease.0.parse::<i64>().map_err(|_| DcsError::LeaseExpired)
    }

    fn extract_revision_token(expected: &CasToken) -> Result<i64, DcsError> {
        match expected {
            CasToken::Revision(rev) => i64::try_from(*rev).map_err(|_| DcsError::CasConflict {
                expected: Some(expected.clone()),
            }),
            CasToken::ResourceVersion(_) => Err(DcsError::CasConflict {
                expected: Some(expected.clone()),
            }),
        }
    }

    async fn cas_json_key(
        &self,
        suffix: &str,
        payload: Vec<u8>,
        expected: Option<&CasToken>,
    ) -> Result<CasToken, DcsError> {
        let full_key = self.key(suffix).into_bytes();
        let cmp = match expected {
            None => pb::Compare::version_eq(full_key.clone(), 0),
            Some(tok) => {
                let rev = Self::extract_revision_token(tok)?;
                pb::Compare::mod_revision_eq(full_key.clone(), rev)
            }
        };

        let resp = self
            .client
            .txn(pb::TxnRequest {
                compare: vec![cmp],
                success: vec![pb::RequestOp::put(full_key, payload, 0)],
                failure: Vec::new(),
            })
            .await?;

        if !resp.succeeded {
            return Err(DcsError::CasConflict {
                expected: expected.cloned(),
            });
        }
        let new_rev = resp
            .header
            .map_or(1_u64, |h| u64::try_from(h.revision).unwrap_or(1));
        Ok(CasToken::Revision(new_rev))
    }
}

impl DcsBackend for Etcd3Dcs {
    async fn fetch_cluster(&self, timeout: Duration) -> Result<ClusterSnapshot, DcsError> {
        let range_key = format!("{}/", self.prefix).into_bytes();
        let range_end = prefix_range_end(&range_key);
        let get_fut = self.client.range(pb::RangeRequest {
            key: range_key,
            range_end,
            limit: 0,
            revision: 0,
        });
        let resp = tokio::time::timeout(timeout, get_fut).await.map_err(|_| {
            DcsError::Unreachable(format!("etcd3 fetch_cluster timed out after {timeout:?}"))
        })??;

        let mut raw_kvs = Vec::with_capacity(resp.kvs.len());
        for entry in &resp.kvs {
            if let Ok(key_str) = std::str::from_utf8(&entry.key) {
                let mod_rev = u64::try_from(entry.mod_revision).unwrap_or(0);
                raw_kvs.push(RawEtcdKv {
                    key: key_str,
                    value: &entry.value,
                    mod_revision: mod_rev,
                });
            }
        }

        Ok(parse_etcd_prefix_kvs(&self.prefix, &raw_kvs))
    }

    async fn ensure_lease(
        &self,
        ttl: Duration,
        current: Option<&LeaseHandle>,
    ) -> Result<LeaseHandle, DcsError> {
        if let Some(cur_handle) = current {
            let matches_active = {
                let guard = self
                    .active_lease
                    .lock()
                    .map_err(|_| DcsError::Unreachable("etcd3 lease lock poisoned".to_owned()))?;
                guard.as_ref().and_then(|task| {
                    if &task.handle == cur_handle
                        && task.ttl == ttl
                        && task.alive.load(Ordering::Acquire)
                    {
                        Some(task.lease_id)
                    } else {
                        None
                    }
                })
            };

            if let Some(lease_id) = matches_active {
                let (ka_tx, mut stream) = self.client.lease_keep_alive().await?;
                ka_tx
                    .send(pb::LeaseKeepAliveRequest { id: lease_id })
                    .await
                    .map_err(|_| DcsError::LeaseExpired)?;
                match stream.message().await.map_err(map_grpc_status)? {
                    Some(ka_resp) if ka_resp.ttl > 0 => return Ok(cur_handle.clone()),
                    _ => return Err(DcsError::LeaseExpired),
                }
            }
        }

        let ttl_secs = i64::try_from(ttl.as_secs().max(5)).unwrap_or(30);
        let granted = self
            .client
            .lease_grant(pb::LeaseGrantRequest {
                ttl: ttl_secs,
                id: 0,
            })
            .await?;
        if !granted.error.is_empty() {
            return Err(DcsError::Unreachable(format!(
                "etcd3 LeaseGrant error: {}",
                granted.error
            )));
        }
        let lease_id = granted.id;
        let handle = LeaseHandle(CompactString::from(lease_id.to_string()));

        let alive = Arc::new(AtomicBool::new(true));
        let alive_bg = Arc::clone(&alive);
        let (stop_tx, mut stop_rx) = watch::channel(false);
        let bg_cli = self.client.clone();
        let cadence = (ttl / 3).max(Duration::from_secs(1));

        let join = tokio::spawn(async move {
            let Ok((ka_tx, mut stream)) = bg_cli.lease_keep_alive().await else {
                alive_bg.store(false, Ordering::Release);
                return;
            };
            let mut ticker = tokio::time::interval(cadence);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    biased;
                    changed = stop_rx.changed() => {
                        if changed.is_err() || *stop_rx.borrow() {
                            break;
                        }
                    }
                    _ = ticker.tick() => {
                        if ka_tx.send(pb::LeaseKeepAliveRequest { id: lease_id }).await.is_err() {
                            alive_bg.store(false, Ordering::Release);
                            break;
                        }
                        match stream.message().await {
                            Ok(Some(resp)) if resp.ttl > 0 => {}
                            _ => {
                                alive_bg.store(false, Ordering::Release);
                                break;
                            }
                        }
                    }
                }
            }
        });

        let mut guard = self
            .active_lease
            .lock()
            .map_err(|_| DcsError::Unreachable("etcd3 lease lock poisoned".to_owned()))?;
        *guard = Some(ActiveLeaseTask {
            lease_id,
            handle: handle.clone(),
            ttl,
            alive,
            stop_tx,
            join,
        });

        Ok(handle)
    }

    async fn cas_initialize(&self, sys_id: &str) -> Result<CasToken, DcsError> {
        let init_key = self.key("initialize").into_bytes();
        let resp = self
            .client
            .txn(pb::TxnRequest {
                compare: vec![pb::Compare::version_eq(init_key.clone(), 0)],
                success: vec![pb::RequestOp::put(init_key, sys_id.as_bytes().to_vec(), 0)],
                failure: Vec::new(),
            })
            .await?;
        if !resp.succeeded {
            return Err(DcsError::CasConflict { expected: None });
        }
        let rev = resp
            .header
            .map_or(1_u64, |h| u64::try_from(h.revision).unwrap_or(1));
        Ok(CasToken::Revision(rev))
    }

    async fn acquire_or_renew_leader(
        &self,
        self_name: &str,
        lease: &LeaseHandle,
        expected: Option<&CasToken>,
    ) -> Result<CasToken, DcsError> {
        let lease_id = Self::parse_lease_id(lease)?;
        let leader_key = self.key("leader").into_bytes();

        let compares = match expected {
            None => vec![pb::Compare::version_eq(leader_key.clone(), 0)],
            Some(tok) => {
                let rev = Self::extract_revision_token(tok)?;
                vec![
                    pb::Compare::mod_revision_eq(leader_key.clone(), rev),
                    pb::Compare::value_eq(leader_key.clone(), self_name.as_bytes().to_vec()),
                ]
            }
        };

        let resp = self
            .client
            .txn(pb::TxnRequest {
                compare: compares,
                success: vec![pb::RequestOp::put(
                    leader_key,
                    self_name.as_bytes().to_vec(),
                    lease_id,
                )],
                failure: Vec::new(),
            })
            .await?;
        if !resp.succeeded {
            return Err(DcsError::CasConflict {
                expected: expected.cloned(),
            });
        }
        let new_rev = resp
            .header
            .map_or(1_u64, |h| u64::try_from(h.revision).unwrap_or(1));
        Ok(CasToken::Revision(new_rev))
    }

    async fn release_leader(&self, self_name: &str, expected: &CasToken) -> Result<(), DcsError> {
        let rev = Self::extract_revision_token(expected)?;
        let leader_key = self.key("leader").into_bytes();
        let resp = self
            .client
            .txn(pb::TxnRequest {
                compare: vec![
                    pb::Compare::mod_revision_eq(leader_key.clone(), rev),
                    pb::Compare::value_eq(leader_key.clone(), self_name.as_bytes().to_vec()),
                ],
                success: vec![pb::RequestOp::delete_key(leader_key)],
                failure: Vec::new(),
            })
            .await?;
        if !resp.succeeded {
            return Err(DcsError::CasConflict {
                expected: Some(expected.clone()),
            });
        }
        Ok(())
    }

    async fn upsert_member(
        &self,
        self_name: &str,
        data: &MemberData,
        lease: &LeaseHandle,
    ) -> Result<(), DcsError> {
        let lease_id = Self::parse_lease_id(lease)?;
        let member_key = self.key(&format!("members/{self_name}")).into_bytes();
        let payload = serde_json::to_vec(data)
            .map_err(|e| DcsError::Unreachable(format!("serializing MemberData: {e}")))?;
        self.client
            .put(pb::PutRequest {
                key: member_key,
                value: payload,
                lease: lease_id,
                prev_kv: false,
                ignore_value: false,
                ignore_lease: false,
            })
            .await?;
        Ok(())
    }

    async fn publish_leader_state(
        &self,
        status: &ClusterStatus,
        failsafe_roster: Option<&BTreeMap<CompactString, String>>,
    ) -> Result<(), DcsError> {
        let mut ops = Vec::with_capacity(2);
        let status_bytes = serde_json::to_vec(status)
            .map_err(|e| DcsError::Unreachable(format!("serializing ClusterStatus: {e}")))?;
        ops.push(pb::RequestOp::put(
            self.key("status").into_bytes(),
            status_bytes,
            0,
        ));

        if let Some(roster) = failsafe_roster {
            let fs_bytes = serde_json::to_vec(roster)
                .map_err(|e| DcsError::Unreachable(format!("serializing failsafe roster: {e}")))?;
            ops.push(pb::RequestOp::put(
                self.key("failsafe").into_bytes(),
                fs_bytes,
                0,
            ));
        }

        self.client
            .txn(pb::TxnRequest {
                compare: Vec::new(),
                success: ops,
                failure: Vec::new(),
            })
            .await?;
        Ok(())
    }

    async fn append_history_entry(&self, entry: &HistoryEntry) -> Result<(), DcsError> {
        let history_key = self.key("history").into_bytes();
        let current = self
            .client
            .range(pb::RangeRequest {
                key: history_key.clone(),
                range_end: Vec::new(),
                limit: 1,
                revision: 0,
            })
            .await?;

        let (mut entries, cmp) = if let Some(existing_kv) = current.kvs.first() {
            let parsed =
                serde_json::from_slice::<Vec<HistoryEntry>>(&existing_kv.value).unwrap_or_default();
            (
                parsed,
                pb::Compare::mod_revision_eq(history_key.clone(), existing_kv.mod_revision),
            )
        } else {
            (Vec::new(), pb::Compare::version_eq(history_key.clone(), 0))
        };

        if !entries
            .iter()
            .any(|h| h.timeline == entry.timeline && h.lsn == entry.lsn)
        {
            entries.push(entry.clone());
        }
        let payload = serde_json::to_vec(&entries)
            .map_err(|e| DcsError::Unreachable(format!("serializing history: {e}")))?;
        let resp = self
            .client
            .txn(pb::TxnRequest {
                compare: vec![cmp],
                success: vec![pb::RequestOp::put(history_key, payload, 0)],
                failure: Vec::new(),
            })
            .await?;
        if !resp.succeeded {
            return Err(DcsError::CasConflict { expected: None });
        }
        Ok(())
    }

    async fn cas_sync_state(
        &self,
        next: &SyncState,
        expected: Option<&CasToken>,
    ) -> Result<CasToken, DcsError> {
        let payload = serde_json::to_vec(next)
            .map_err(|e| DcsError::Unreachable(format!("serializing SyncState: {e}")))?;
        self.cas_json_key("sync", payload, expected).await
    }

    async fn cas_config(
        &self,
        config_json: &serde_json::Value,
        expected: Option<&CasToken>,
    ) -> Result<CasToken, DcsError> {
        let payload = serde_json::to_vec(config_json)
            .map_err(|e| DcsError::Unreachable(format!("serializing dynamic config: {e}")))?;
        self.cas_json_key("config", payload, expected).await
    }

    async fn cas_failover(
        &self,
        intent: &FailoverIntent,
        expected: Option<&CasToken>,
    ) -> Result<CasToken, DcsError> {
        let payload = serde_json::to_vec(intent)
            .map_err(|e| DcsError::Unreachable(format!("serializing FailoverIntent: {e}")))?;
        self.cas_json_key("failover", payload, expected).await
    }

    async fn clear_failover(&self, expected: &CasToken) -> Result<(), DcsError> {
        let rev = Self::extract_revision_token(expected)?;
        let fo_key = self.key("failover").into_bytes();
        let resp = self
            .client
            .txn(pb::TxnRequest {
                compare: vec![pb::Compare::mod_revision_eq(fo_key.clone(), rev)],
                success: vec![pb::RequestOp::delete_key(fo_key)],
                failure: Vec::new(),
            })
            .await?;
        if !resp.succeeded {
            return Err(DcsError::CasConflict {
                expected: Some(expected.clone()),
            });
        }
        Ok(())
    }

    async fn delete_cluster(&self) -> Result<(), DcsError> {
        let range_key = format!("{}/", self.prefix).into_bytes();
        let range_end = prefix_range_end(&range_key);
        self.client
            .delete_range(pb::DeleteRangeRequest {
                key: range_key,
                range_end,
                prev_kv: false,
            })
            .await?;
        Ok(())
    }

    fn subscribe_watches(&self) -> mpsc::Receiver<DcsWatchEvent> {
        let (tx, rx) = mpsc::channel(64);
        let watch_cli = self.client.clone();
        let prefix = self.prefix.clone();
        let range_prefix = format!("{prefix}/").into_bytes();

        tokio::spawn(async move {
            loop {
                let Ok((_req_tx, mut stream)) = watch_cli.open_watch(range_prefix.clone()).await
                else {
                    if tx.send(DcsWatchEvent::WatchDisconnected).await.is_err() {
                        return;
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                };

                loop {
                    match stream.message().await {
                        Ok(Some(resp)) => {
                            if resp.canceled {
                                let _ = tx.send(DcsWatchEvent::WatchDisconnected).await;
                                break;
                            }
                            for ev in &resp.events {
                                if let Some(ref kv) = ev.kv
                                    && let Ok(key_str) = std::str::from_utf8(&kv.key)
                                    && let Some(mapped) = classify_etcd_watch_key(&prefix, key_str)
                                    && tx.send(mapped).await.is_err()
                                {
                                    return;
                                }
                            }
                        }
                        Ok(None) | Err(_) => {
                            if tx.send(DcsWatchEvent::WatchDisconnected).await.is_err() {
                                return;
                            }
                            break;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        });

        rx
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::convert::Infallible;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::net::TcpListener;
    use tokio::sync::broadcast;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::body::Body;
    use tonic::server::{Grpc as ServerGrpc, StreamingService, UnaryService};
    use tower::Service;

    use super::*;

    #[test]
    fn parses_complete_patroni_etcd3_prefix_tree_and_classifies_watches() {
        let member_json = br#"{"conn_url":"postgres://10.0.0.1:5432/postgres","api_url":"http://10.0.0.1:8008/patroni","state":"running","role":"primary","version":"4.1.0","xlog_location":67108864,"timeline":3}"#;
        let sync_json = br#"{"leader":"pg-1","sync_standby":"pg-2","quorum":0}"#;
        let hist_json =
            br#"[[1,33554432,"no recovery target specified","2026-11-12T10:00:00Z","pg-1"]]"#;
        let failsafe_json =
            br#"{"pg-1":"http://10.0.0.1:8008/patroni","pg-2":"http://10.0.0.2:8008/patroni"}"#;

        let kvs = [
            RawEtcdKv {
                key: "/service/prod/initialize",
                value: b"7399112233445566778",
                mod_revision: 2,
            },
            RawEtcdKv {
                key: "/service/prod/leader",
                value: b"pg-1",
                mod_revision: 44,
            },
            RawEtcdKv {
                key: "/service/prod/members/pg-1",
                value: member_json,
                mod_revision: 45,
            },
            RawEtcdKv {
                key: "/service/prod/sync",
                value: sync_json,
                mod_revision: 46,
            },
            RawEtcdKv {
                key: "/service/prod/history",
                value: hist_json,
                mod_revision: 47,
            },
            RawEtcdKv {
                key: "/service/prod/failsafe",
                value: failsafe_json,
                mod_revision: 48,
            },
            RawEtcdKv {
                key: "/service/prod/failover",
                value: b"{}",
                mod_revision: 49,
            },
        ];

        let snap = parse_etcd_prefix_kvs("/service/prod", &kvs);
        assert_eq!(
            snap.initialize.as_ref().map(|i| i.value.as_str()),
            Some("7399112233445566778")
        );
        assert_eq!(snap.leader.as_ref().map(|l| l.value.as_str()), Some("pg-1"));
        assert_eq!(snap.members.len(), 1);
        assert_eq!(snap.members["pg-1"].value.timeline, Some(3));
        assert_eq!(
            snap.sync
                .as_ref()
                .and_then(|s| s.value.sync_standby.as_deref()),
            Some("pg-2")
        );
        assert_eq!(snap.history.len(), 1);
        assert_eq!(snap.history[0].timeline, 1);
        assert_eq!(snap.failsafe.as_ref().map(BTreeMap::len), Some(2));
        assert!(snap.failover.is_none());

        assert_eq!(
            classify_etcd_watch_key("/service/prod", "/service/prod/leader"),
            Some(DcsWatchEvent::LeaderChanged)
        );
        assert_eq!(
            classify_etcd_watch_key("/service/prod", "/service/prod/config"),
            Some(DcsWatchEvent::ConfigChanged)
        );
        assert_eq!(
            classify_etcd_watch_key("/service/prod", "/service/prod/failover"),
            Some(DcsWatchEvent::FailoverRequested)
        );
        assert_eq!(
            classify_etcd_watch_key("/service/prod", "/service/prod/members/pg-2"),
            Some(DcsWatchEvent::TopologyChanged)
        );
    }

    // -----------------------------------------------------------------------
    // In-process HTTP/2 gRPC `etcdserverpb` mock server for end-to-end tests
    // -----------------------------------------------------------------------

    #[derive(Default)]
    struct MockMvccState {
        revision: i64,
        next_lease_id: i64,
        leases: BTreeMap<i64, i64>,
        kvs: BTreeMap<Vec<u8>, pb::KeyValue>,
    }

    impl MockMvccState {
        fn header(&self) -> Option<pb::ResponseHeader> {
            Some(pb::ResponseHeader {
                cluster_id: 1,
                member_id: 1,
                revision: self.revision.max(1),
                raft_term: 1,
            })
        }

        fn apply_put(
            &mut self,
            req: &pb::PutRequest,
            events: &mut Vec<pb::Event>,
        ) -> pb::PutResponse {
            let prev = self.kvs.get(&req.key).cloned();
            let (create_rev, version) = match prev {
                Some(ref old) => (old.create_revision, old.version + 1),
                None => (self.revision, 1),
            };
            let kv = pb::KeyValue {
                key: req.key.clone(),
                create_revision: create_rev,
                mod_revision: self.revision,
                version,
                value: req.value.clone(),
                lease: req.lease,
            };
            self.kvs.insert(req.key.clone(), kv.clone());
            events.push(pb::Event {
                r#type: 0,
                kv: Some(kv),
                prev_kv: prev.clone(),
            });
            pb::PutResponse {
                header: self.header(),
                prev_kv: if req.prev_kv { prev } else { None },
            }
        }

        fn apply_delete(
            &mut self,
            req: &pb::DeleteRangeRequest,
            events: &mut Vec<pb::Event>,
        ) -> pb::DeleteRangeResponse {
            let keys_to_remove: Vec<Vec<u8>> = if req.range_end.is_empty() {
                if self.kvs.contains_key(&req.key) {
                    vec![req.key.clone()]
                } else {
                    Vec::new()
                }
            } else {
                self.kvs
                    .keys()
                    .filter(|k| {
                        k.as_slice() >= req.key.as_slice()
                            && (req.range_end == [0u8] || k.as_slice() < req.range_end.as_slice())
                    })
                    .cloned()
                    .collect()
            };
            let deleted = i64::try_from(keys_to_remove.len()).unwrap_or(0);
            for k in keys_to_remove {
                if let Some(old) = self.kvs.remove(&k) {
                    events.push(pb::Event {
                        r#type: 1,
                        kv: Some(pb::KeyValue {
                            key: k,
                            create_revision: 0,
                            mod_revision: self.revision,
                            version: 0,
                            value: Vec::new(),
                            lease: 0,
                        }),
                        prev_kv: Some(old),
                    });
                }
            }
            pb::DeleteRangeResponse {
                header: self.header(),
                deleted,
                prev_kvs: Vec::new(),
            }
        }

        fn eval_compare(&self, cmp: &pb::Compare) -> bool {
            let cur = self.kvs.get(&cmp.key);
            match (&cmp.target_union, cmp.target) {
                (Some(pb::TargetUnion::Version(v)), pb::TARGET_VERSION) => {
                    cur.map_or(0, |kv| kv.version) == *v
                }
                (Some(pb::TargetUnion::ModRevision(r)), pb::TARGET_MOD) => {
                    cur.map_or(0, |kv| kv.mod_revision) == *r
                }
                (Some(pb::TargetUnion::Value(val)), pb::TARGET_VALUE) => {
                    cur.is_some_and(|kv| &kv.value == val)
                }
                _ => false,
            }
        }
    }

    #[derive(Clone)]
    struct MockEtcdService {
        state: Arc<Mutex<MockMvccState>>,
        watch_bcast: broadcast::Sender<Vec<pb::Event>>,
    }

    impl Service<http::Request<Body>> for MockEtcdService {
        type Response = http::Response<Body>;
        type Error = Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: http::Request<Body>) -> Self::Future {
            let st = Arc::clone(&self.state);
            let bcast = self.watch_bcast.clone();
            Box::pin(async move {
                let path = req.uri().path().to_owned();
                let resp = match path.as_str() {
                    "/etcdserverpb.KV/Range" => {
                        struct Svc(Arc<Mutex<MockMvccState>>);
                        impl UnaryService<pb::RangeRequest> for Svc {
                            type Response = pb::RangeResponse;
                            type Future = Pin<
                                Box<
                                    dyn Future<
                                            Output = Result<
                                                tonic::Response<Self::Response>,
                                                Status,
                                            >,
                                        > + Send,
                                >,
                            >;
                            fn call(&mut self, request: Request<pb::RangeRequest>) -> Self::Future {
                                let r = request.into_inner();
                                let guard = self.0.lock().unwrap();
                                let kvs: Vec<pb::KeyValue> = if r.range_end.is_empty() {
                                    guard.kvs.get(&r.key).cloned().into_iter().collect()
                                } else {
                                    guard
                                        .kvs
                                        .iter()
                                        .filter(|(k, _)| {
                                            k.as_slice() >= r.key.as_slice()
                                                && (r.range_end == [0u8]
                                                    || k.as_slice() < r.range_end.as_slice())
                                        })
                                        .map(|(_, v)| v.clone())
                                        .collect()
                                };
                                let count = i64::try_from(kvs.len()).unwrap_or(0);
                                let out = pb::RangeResponse {
                                    header: guard.header(),
                                    kvs,
                                    more: false,
                                    count,
                                };
                                Box::pin(async move { Ok(tonic::Response::new(out)) })
                            }
                        }
                        let mut grpc = ServerGrpc::new(ProstCodec::default());
                        grpc.unary(Svc(st), req).await
                    }
                    "/etcdserverpb.KV/Put" => {
                        struct Svc(Arc<Mutex<MockMvccState>>, broadcast::Sender<Vec<pb::Event>>);
                        impl UnaryService<pb::PutRequest> for Svc {
                            type Response = pb::PutResponse;
                            type Future = Pin<
                                Box<
                                    dyn Future<
                                            Output = Result<
                                                tonic::Response<Self::Response>,
                                                Status,
                                            >,
                                        > + Send,
                                >,
                            >;
                            fn call(&mut self, request: Request<pb::PutRequest>) -> Self::Future {
                                let r = request.into_inner();
                                let mut guard = self.0.lock().unwrap();
                                if r.lease != 0 && !guard.leases.contains_key(&r.lease) {
                                    return Box::pin(async move {
                                        Err(Status::not_found(
                                            "etcdserver: requested lease not found",
                                        ))
                                    });
                                }
                                guard.revision += 1;
                                let mut evs = Vec::new();
                                let out = guard.apply_put(&r, &mut evs);
                                drop(guard);
                                let _ = self.1.send(evs);
                                Box::pin(async move { Ok(tonic::Response::new(out)) })
                            }
                        }
                        let mut grpc = ServerGrpc::new(ProstCodec::default());
                        grpc.unary(Svc(st, bcast), req).await
                    }
                    "/etcdserverpb.KV/DeleteRange" => {
                        struct Svc(Arc<Mutex<MockMvccState>>, broadcast::Sender<Vec<pb::Event>>);
                        impl UnaryService<pb::DeleteRangeRequest> for Svc {
                            type Response = pb::DeleteRangeResponse;
                            type Future = Pin<
                                Box<
                                    dyn Future<
                                            Output = Result<
                                                tonic::Response<Self::Response>,
                                                Status,
                                            >,
                                        > + Send,
                                >,
                            >;
                            fn call(
                                &mut self,
                                request: Request<pb::DeleteRangeRequest>,
                            ) -> Self::Future {
                                let r = request.into_inner();
                                let mut guard = self.0.lock().unwrap();
                                guard.revision += 1;
                                let mut evs = Vec::new();
                                let out = guard.apply_delete(&r, &mut evs);
                                drop(guard);
                                if !evs.is_empty() {
                                    let _ = self.1.send(evs);
                                }
                                Box::pin(async move { Ok(tonic::Response::new(out)) })
                            }
                        }
                        let mut grpc = ServerGrpc::new(ProstCodec::default());
                        grpc.unary(Svc(st, bcast), req).await
                    }
                    "/etcdserverpb.KV/Txn" => {
                        struct Svc(Arc<Mutex<MockMvccState>>, broadcast::Sender<Vec<pb::Event>>);
                        impl UnaryService<pb::TxnRequest> for Svc {
                            type Response = pb::TxnResponse;
                            type Future = Pin<
                                Box<
                                    dyn Future<
                                            Output = Result<
                                                tonic::Response<Self::Response>,
                                                Status,
                                            >,
                                        > + Send,
                                >,
                            >;
                            fn call(&mut self, request: Request<pb::TxnRequest>) -> Self::Future {
                                let r = request.into_inner();
                                let mut guard = self.0.lock().unwrap();
                                let succeeded = r.compare.iter().all(|c| guard.eval_compare(c));
                                let branch = if succeeded { &r.success } else { &r.failure };
                                let mut evs = Vec::new();
                                let mut responses = Vec::new();
                                if !branch.is_empty() {
                                    guard.revision += 1;
                                    for op in branch {
                                        match &op.request {
                                            Some(pb::RequestUnion::RequestPut(put_req)) => {
                                                if put_req.lease != 0
                                                    && !guard.leases.contains_key(&put_req.lease)
                                                {
                                                    return Box::pin(async move {
                                                        Err(Status::not_found(
                                                            "etcdserver: requested lease not found",
                                                        ))
                                                    });
                                                }
                                                let resp = guard.apply_put(put_req, &mut evs);
                                                responses.push(pb::ResponseOp {
                                                    response: Some(pb::ResponseUnion::ResponsePut(
                                                        resp,
                                                    )),
                                                });
                                            }
                                            Some(pb::RequestUnion::RequestDeleteRange(del_req)) => {
                                                let resp = guard.apply_delete(del_req, &mut evs);
                                                responses.push(pb::ResponseOp {
                                                    response: Some(
                                                        pb::ResponseUnion::ResponseDeleteRange(
                                                            resp,
                                                        ),
                                                    ),
                                                });
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                let out = pb::TxnResponse {
                                    header: guard.header(),
                                    succeeded,
                                    responses,
                                };
                                drop(guard);
                                if !evs.is_empty() {
                                    let _ = self.1.send(evs);
                                }
                                Box::pin(async move { Ok(tonic::Response::new(out)) })
                            }
                        }
                        let mut grpc = ServerGrpc::new(ProstCodec::default());
                        grpc.unary(Svc(st, bcast), req).await
                    }
                    "/etcdserverpb.Lease/LeaseGrant" => {
                        struct Svc(Arc<Mutex<MockMvccState>>);
                        impl UnaryService<pb::LeaseGrantRequest> for Svc {
                            type Response = pb::LeaseGrantResponse;
                            type Future = Pin<
                                Box<
                                    dyn Future<
                                            Output = Result<
                                                tonic::Response<Self::Response>,
                                                Status,
                                            >,
                                        > + Send,
                                >,
                            >;
                            fn call(
                                &mut self,
                                request: Request<pb::LeaseGrantRequest>,
                            ) -> Self::Future {
                                let r = request.into_inner();
                                let mut guard = self.0.lock().unwrap();
                                guard.next_lease_id += 1;
                                let id = 1000 + guard.next_lease_id;
                                guard.leases.insert(id, r.ttl);
                                let out = pb::LeaseGrantResponse {
                                    header: guard.header(),
                                    id,
                                    ttl: r.ttl,
                                    error: String::new(),
                                };
                                Box::pin(async move { Ok(tonic::Response::new(out)) })
                            }
                        }
                        let mut grpc = ServerGrpc::new(ProstCodec::default());
                        grpc.unary(Svc(st), req).await
                    }
                    "/etcdserverpb.Lease/LeaseKeepAlive" => {
                        struct Svc(Arc<Mutex<MockMvccState>>);
                        impl StreamingService<pb::LeaseKeepAliveRequest> for Svc {
                            type Response = pb::LeaseKeepAliveResponse;
                            type ResponseStream =
                                ReceiverStream<Result<pb::LeaseKeepAliveResponse, Status>>;
                            type Future = Pin<
                                Box<
                                    dyn Future<
                                            Output = Result<
                                                tonic::Response<Self::ResponseStream>,
                                                Status,
                                            >,
                                        > + Send,
                                >,
                            >;
                            fn call(
                                &mut self,
                                request: Request<Streaming<pb::LeaseKeepAliveRequest>>,
                            ) -> Self::Future {
                                let st = Arc::clone(&self.0);
                                let mut inbound = request.into_inner();
                                let (tx, rx) = mpsc::channel(8);
                                tokio::spawn(async move {
                                    while let Ok(Some(ka)) = inbound.message().await {
                                        let (hdr, ttl) = {
                                            let g = st.lock().unwrap();
                                            let ttl = g.leases.get(&ka.id).copied().unwrap_or(0);
                                            (g.header(), ttl)
                                        };
                                        if tx
                                            .send(Ok(pb::LeaseKeepAliveResponse {
                                                header: hdr,
                                                id: ka.id,
                                                ttl,
                                            }))
                                            .await
                                            .is_err()
                                        {
                                            break;
                                        }
                                    }
                                });
                                Box::pin(async move {
                                    Ok(tonic::Response::new(ReceiverStream::new(rx)))
                                })
                            }
                        }
                        let mut grpc = ServerGrpc::new(ProstCodec::default());
                        grpc.streaming(Svc(st), req).await
                    }
                    "/etcdserverpb.Watch/Watch" => {
                        struct Svc(broadcast::Sender<Vec<pb::Event>>);
                        impl StreamingService<pb::WatchRequest> for Svc {
                            type Response = pb::WatchResponse;
                            type ResponseStream = ReceiverStream<Result<pb::WatchResponse, Status>>;
                            type Future = Pin<
                                Box<
                                    dyn Future<
                                            Output = Result<
                                                tonic::Response<Self::ResponseStream>,
                                                Status,
                                            >,
                                        > + Send,
                                >,
                            >;
                            fn call(
                                &mut self,
                                request: Request<Streaming<pb::WatchRequest>>,
                            ) -> Self::Future {
                                let mut sub = self.0.subscribe();
                                let mut inbound = request.into_inner();
                                let (tx, rx) = mpsc::channel(16);
                                tokio::spawn(async move {
                                    let _ = inbound.message().await;
                                    let _ = tx
                                        .send(Ok(pb::WatchResponse {
                                            header: None,
                                            watch_id: 1,
                                            created: true,
                                            canceled: false,
                                            compact_revision: 0,
                                            cancel_reason: String::new(),
                                            fragment: false,
                                            events: Vec::new(),
                                        }))
                                        .await;
                                    while let Ok(evs) = sub.recv().await {
                                        if tx
                                            .send(Ok(pb::WatchResponse {
                                                header: None,
                                                watch_id: 1,
                                                created: false,
                                                canceled: false,
                                                compact_revision: 0,
                                                cancel_reason: String::new(),
                                                fragment: false,
                                                events: evs,
                                            }))
                                            .await
                                            .is_err()
                                        {
                                            break;
                                        }
                                    }
                                });
                                Box::pin(async move {
                                    Ok(tonic::Response::new(ReceiverStream::new(rx)))
                                })
                            }
                        }
                        let mut grpc = ServerGrpc::new(ProstCodec::default());
                        grpc.streaming(Svc(bcast), req).await
                    }
                    _ => http::Response::builder()
                        .status(404)
                        .body(Body::empty())
                        .unwrap(),
                };
                Ok(resp)
            })
        }
    }

    async fn spawn_mock_etcd_grpc_server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (watch_bcast, _) = broadcast::channel(64);
        let svc = MockEtcdService {
            state: Arc::new(Mutex::new(MockMvccState::default())),
            watch_bcast,
        };
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .accept_http1(false)
                .serve_with_incoming(svc, TcpListenerStream::new(listener))
                .await;
        });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn etcd3_grpc_round_trip_cas_leases_watches_and_delete_cluster() {
        let endpoint = spawn_mock_etcd_grpc_server().await;
        let cfg = Etcd3Config {
            host: Some(endpoint),
            ..Etcd3Config::default()
        };
        let dcs = Etcd3Dcs::connect(&cfg, "/service/etcd-test").await.unwrap();
        let mut watch_rx = dcs.subscribe_watches();
        // Give watch stream a brief moment to register
        tokio::time::sleep(Duration::from_millis(50)).await;

        let lease = dcs
            .ensure_lease(Duration::from_secs(30), None)
            .await
            .unwrap();
        let renewed = dcs
            .ensure_lease(Duration::from_secs(30), Some(&lease))
            .await
            .unwrap();
        assert_eq!(lease, renewed);

        let init_tok = dcs.cas_initialize("740011223344").await.unwrap();
        assert!(matches!(init_tok, CasToken::Revision(_)));
        assert!(matches!(
            dcs.cas_initialize("999").await,
            Err(DcsError::CasConflict { expected: None })
        ));

        let ldr_tok = dcs
            .acquire_or_renew_leader("pg-1", &lease, None)
            .await
            .unwrap();
        // Competing candidate must lose CAS
        assert!(matches!(
            dcs.acquire_or_renew_leader("pg-2", &lease, None).await,
            Err(DcsError::CasConflict { expected: None })
        ));
        // Owner renews cleanly
        let ldr_tok2 = dcs
            .acquire_or_renew_leader("pg-1", &lease, Some(&ldr_tok))
            .await
            .unwrap();

        let member = MemberData {
            conn_url: "postgres://10.0.0.1:5432/postgres".to_owned(),
            api_url: "http://10.0.0.1:8008/patroni".to_owned(),
            state: CompactString::const_new("running"),
            role: CompactString::const_new("primary"),
            version: CompactString::const_new("0.1.0"),
            xlog_location: Some(WalLsn(8192)),
            replication_state: None,
            timeline: Some(1),
            pending_restart: false,
            pending_restart_reason: BTreeMap::new(),
            tags: BTreeMap::new(),
            watchdog_failed: Some(false),
            pause: None,
            extra: BTreeMap::new(),
        };
        dcs.upsert_member("pg-1", &member, &lease).await.unwrap();

        let mut roster = BTreeMap::new();
        roster.insert(
            CompactString::const_new("pg-1"),
            "http://10.0.0.1:8008/patroni".to_owned(),
        );
        dcs.publish_leader_state(
            &ClusterStatus {
                optime: WalLsn(8192),
                slots: BTreeMap::new(),
                retain_slots: Vec::new(),
            },
            Some(&roster),
        )
        .await
        .unwrap();

        let cfg_tok = dcs
            .cas_config(&serde_json::json!({"ttl": 30}), None)
            .await
            .unwrap();
        assert!(matches!(
            dcs.cas_config(&serde_json::json!({"ttl": 45}), None).await,
            Err(DcsError::CasConflict { expected: None })
        ));
        let _cfg_tok2 = dcs
            .cas_config(&serde_json::json!({"ttl": 45}), Some(&cfg_tok))
            .await
            .unwrap();

        let snap = dcs.fetch_cluster(Duration::from_secs(5)).await.unwrap();
        assert_eq!(snap.leader.as_ref().map(|l| l.value.as_str()), Some("pg-1"));
        assert_eq!(snap.members.len(), 1);
        assert_eq!(snap.status.as_ref().map(|s| s.value.optime.0), Some(8192));
        assert_eq!(
            snap.config
                .as_ref()
                .and_then(|c| c.value.get("ttl"))
                .and_then(serde_json::Value::as_u64),
            Some(45)
        );

        // Verify gRPC Watch stream delivered events!
        let first_watch = tokio::time::timeout(Duration::from_secs(2), watch_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first_watch, DcsWatchEvent::TopologyChanged);

        dcs.release_leader("pg-1", &ldr_tok2).await.unwrap();
        dcs.delete_cluster().await.unwrap();
        let wiped = dcs.fetch_cluster(Duration::from_secs(5)).await.unwrap();
        assert!(wiped.leader.is_none());
        assert!(wiped.members.is_empty());
    }
}
