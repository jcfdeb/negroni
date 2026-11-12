use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use compact_str::CompactString;
use negroni_config::{PgRoleCredentials, PostgresConfig};
use negroni_types::WalLsn;
use rustls::RootCertStore;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::sync::{mpsc, oneshot};
use tokio_postgres::{Client, Config as PgClientConfig, NoTls};
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::PgError;
use crate::config_writer::parse_listen_host_port;
use crate::pidfd_monitor::PostmasterPidFile;
use crate::slots::{
    DesiredSlotsSpec, LogicalSlotAdvanceOp, LogicalSlotCreateOp, ObservedSlot, SlotReconcilePlan,
    list_replication_slots_sql, plan_slot_reconciliation,
};
use crate::version::PgMajor;

pub const DEFAULT_SQL_CHANNEL_CAPACITY: usize = 32;

/// Exact 1-RTT local PostgreSQL inspection query from `docs/LLD.md` §5.2.
pub const TELEMETRY_1RTT_SQL: &str = "
SELECT
    pg_catalog.current_setting('server_version_num')::int4                AS version_num,
    pg_catalog.pg_is_in_recovery()                                        AS in_recovery,
    CASE WHEN pg_catalog.pg_is_in_recovery()
         THEN COALESCE(pg_catalog.pg_last_wal_receive_lsn(), '0/0'::pg_lsn) - '0/0'::pg_lsn
         ELSE pg_catalog.pg_current_wal_lsn() - '0/0'::pg_lsn
    END::int8                                                             AS primary_or_recv_lsn,
    CASE WHEN pg_catalog.pg_is_in_recovery()
         THEN (COALESCE(pg_catalog.pg_last_wal_replay_lsn(), '0/0'::pg_lsn) - '0/0'::pg_lsn)::int8
         ELSE NULL
    END                                                                   AS replay_lsn,
    CASE WHEN pg_catalog.pg_is_in_recovery()
         THEN pg_catalog.pg_is_wal_replay_paused()
         ELSE false
    END                                                                   AS wal_paused,
    (SELECT timeline_id FROM pg_catalog.pg_control_checkpoint())::int8    AS timeline_id,
    EXISTS (
        SELECT 1 FROM pg_catalog.pg_stat_wal_receiver WHERE status = 'streaming'
    )                                                                     AS is_streaming,
    EXISTS (
        SELECT 1 FROM pg_catalog.pg_settings WHERE pending_restart
    )                                                                     AS pending_restart,
    pg_catalog.current_setting('data_checksums') = 'on'                   AS data_checksums,
    pg_catalog.current_setting('wal_log_hints') = 'on'                    AS wal_log_hints
";

/// Version-routed checkpoint statistics SQL (`pg_stat_bgwriter` on PG 15–16 vs
/// `pg_stat_checkpointer` on PG 17–18).
///
/// # Examples
///
/// ```
/// use negroni_pg::sql_actor::checkpoint_stats_sql;
/// use negroni_pg::version::PgMajor;
///
/// assert!(checkpoint_stats_sql(PgMajor::V16).contains("pg_stat_bgwriter"));
/// assert!(checkpoint_stats_sql(PgMajor::V17).contains("pg_stat_checkpointer"));
/// ```
pub const fn checkpoint_stats_sql(major: PgMajor) -> &'static str {
    if major.capabilities().split_stat_checkpointer {
        "SELECT num_timed::int8, num_requested::int8 FROM pg_catalog.pg_stat_checkpointer"
    } else {
        "SELECT checkpoints_timed::int8, checkpoints_req::int8 FROM pg_catalog.pg_stat_bgwriter"
    }
}

/// Decoded snapshot from [`TELEMETRY_1RTT_SQL`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgTelemetrySnapshot {
    pub pg_major: PgMajor,
    pub in_recovery: bool,
    pub primary_or_recv_lsn: WalLsn,
    pub replay_lsn: Option<WalLsn>,
    pub wal_paused: bool,
    pub timeline_id: u64,
    pub is_streaming: bool,
    pub pending_restart: bool,
    pub data_checksums: bool,
    pub wal_log_hints: bool,
}

/// Checkpoint counters normalized across PG 15–16 (`pg_stat_bgwriter`) and PG 17–18 (`pg_stat_checkpointer`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointStats {
    pub timed: u64,
    pub requested: u64,
}

#[derive(Debug, Clone)]
pub struct LocalSqlEndpointConfig {
    pub pgdata: PathBuf,
    pub listen_host: String,
    pub listen_port: u16,
    pub use_unix_socket: bool,
    pub explicit_socket_dir: Option<PathBuf>,
    pub maintenance_db: CompactString,
    pub superuser: PgRoleCredentials,
    pub connect_timeout: Duration,
}

impl LocalSqlEndpointConfig {
    pub fn from_postgres_config(pg: &PostgresConfig) -> Self {
        let (host, port) = parse_listen_host_port(&pg.listen);
        let tcp_host = if host == "*" || host == "0.0.0.0" || host == "::" {
            "127.0.0.1".to_owned()
        } else {
            host
        };
        let explicit_socket_dir = pg
            .parameters
            .get("unix_socket_directories")
            .and_then(|v| v.as_str())
            .and_then(|csv| csv.split(',').next())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(PathBuf::from);

        Self {
            pgdata: pg.data_dir.clone(),
            listen_host: tcp_host,
            listen_port: port,
            use_unix_socket: pg.use_unix_socket,
            explicit_socket_dir,
            maintenance_db: CompactString::const_new("postgres"),
            superuser: pg.authentication.superuser.clone(),
            connect_timeout: Duration::from_secs(5),
        }
    }

    fn candidate_unix_socket_dirs(&self) -> Vec<PathBuf> {
        let mut dirs = Vec::with_capacity(4);
        if let Ok(Some(pid_file)) = PostmasterPidFile::read_from_pgdata(&self.pgdata)
            && let Some(dir) = pid_file.socket_dir
        {
            dirs.push(dir);
        }
        if let Some(ref explicit) = self.explicit_socket_dir
            && !dirs.contains(explicit)
        {
            dirs.push(explicit.clone());
        }
        for fallback in ["/var/run/postgresql", "/run/postgresql", "/tmp"] {
            let p = PathBuf::from(fallback);
            if !dirs.contains(&p) {
                dirs.push(p);
            }
        }
        dirs
    }
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, PgError> {
    let file = File::open(path).map_err(|source| PgError::Io {
        context: format!("opening TLS certificate '{}'", path.display()),
        source,
    })?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| PgError::Io {
            context: format!("parsing PEM certificates from '{}'", path.display()),
            source,
        })
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, PgError> {
    let file = File::open(path).map_err(|source| PgError::Io {
        context: format!("opening TLS private key '{}'", path.display()),
        source,
    })?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|source| PgError::Io {
            context: format!("parsing PEM private key from '{}'", path.display()),
            source,
        })?
        .ok_or_else(|| {
            PgError::TlsConfig(format!("no PEM private key found in '{}'", path.display()))
        })
}

/// Builds a pure-Rust `MakeRustlsConnect` (`ring` provider, TLS 1.2/1.3) honoring optional
/// `sslrootcert` CA bundle and `sslcert`/`sslkey` mutual-TLS client credentials.
pub fn build_rustls_connector(creds: &PgRoleCredentials) -> Result<MakeRustlsConnect, PgError> {
    let mut roots = RootCertStore::empty();
    if let Some(ref ca_path) = creds.sslrootcert {
        for cert in load_certs(ca_path)? {
            roots
                .add(cert)
                .map_err(|e| PgError::TlsConfig(format!("invalid CA certificate: {e}")))?;
        }
    }

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| PgError::TlsConfig(format!("TLS protocol setup failed: {e}")))?
        .with_root_certificates(roots);

    let client_cfg = match (&creds.sslcert, &creds.sslkey) {
        (Some(cert_path), Some(key_path)) => {
            let chain = load_certs(cert_path)?;
            let key = load_private_key(key_path)?;
            builder
                .with_client_auth_cert(chain, key)
                .map_err(|e| PgError::TlsConfig(format!("invalid mTLS client cert/key: {e}")))?
        }
        _ => builder.with_no_client_auth(),
    };

    Ok(MakeRustlsConnect::new(client_cfg))
}

async fn connect_single_client(
    endpoint: &LocalSqlEndpointConfig,
    dbname: &str,
) -> Result<Client, PgError> {
    let mut base_cfg = PgClientConfig::new();
    let user = endpoint.superuser.username.as_deref().unwrap_or("postgres");
    base_cfg.user(user);
    if let Some(ref pw) = endpoint.superuser.password {
        base_cfg.password(pw.expose());
    }
    base_cfg.dbname(dbname);
    base_cfg.application_name("negroni-control");
    base_cfg.connect_timeout(endpoint.connect_timeout);

    // 1. Prefer local Unix domain socket when `use_unix_socket == true` (zero TCP/TLS overhead).
    if endpoint.use_unix_socket {
        for sock_dir in endpoint.candidate_unix_socket_dirs() {
            let sock_file = sock_dir.join(format!(".s.PGSQL.{}", endpoint.listen_port));
            if !sock_file.exists() {
                continue;
            }
            let mut unix_cfg = base_cfg.clone();
            unix_cfg.host_path(&sock_dir);
            unix_cfg.port(endpoint.listen_port);
            match unix_cfg.connect(NoTls).await {
                Ok((client, connection)) => {
                    tokio::spawn(async move {
                        if let Err(err) = connection.await {
                            tracing::debug!(error = %err, "local PostgreSQL Unix socket closed");
                        }
                    });
                    return Ok(client);
                }
                Err(err) => {
                    tracing::debug!(
                        socket_dir = %sock_dir.display(),
                        error = %err,
                        "Unix socket connect attempt failed; trying next fallback"
                    );
                }
            }
        }
    }

    // 2. Fallback to TCP (with `MakeRustlsConnect` when SSL is enabled/available, or `NoTls` when disabled).
    let mut tcp_cfg = base_cfg;
    tcp_cfg.host(&endpoint.listen_host);
    tcp_cfg.port(endpoint.listen_port);

    let sslmode = endpoint
        .superuser
        .sslmode
        .as_deref()
        .unwrap_or("prefer")
        .to_ascii_lowercase();

    if sslmode != "disable"
        && (endpoint.superuser.sslrootcert.is_some()
            || matches!(sslmode.as_str(), "require" | "verify-ca" | "verify-full"))
    {
        let tls = build_rustls_connector(&endpoint.superuser)?;
        let (client, connection) = tcp_cfg.connect(tls).await.map_err(PgError::Sql)?;
        tokio::spawn(async move {
            if let Err(err) = connection.await {
                tracing::debug!(error = %err, "local PostgreSQL TLS TCP connection closed");
            }
        });
        return Ok(client);
    }

    let (client, connection) = tcp_cfg.connect(NoTls).await.map_err(PgError::Sql)?;
    tokio::spawn(async move {
        if let Err(err) = connection.await {
            tracing::debug!(error = %err, "local PostgreSQL TCP connection closed");
        }
    });
    Ok(client)
}

enum SqlCommand {
    Telemetry {
        reply: oneshot::Sender<Result<PgTelemetrySnapshot, PgError>>,
    },
    CheckpointStats {
        major: PgMajor,
        reply: oneshot::Sender<Result<CheckpointStats, PgError>>,
    },
    Promote {
        reply: oneshot::Sender<Result<bool, PgError>>,
    },
    ReloadConf {
        reply: oneshot::Sender<Result<bool, PgError>>,
    },
    SystemIdentifier {
        reply: oneshot::Sender<Result<String, PgError>>,
    },
    PingSimple {
        reply: oneshot::Sender<Result<(), PgError>>,
    },
    ReconcileSlots {
        major: PgMajor,
        is_primary: bool,
        is_demoted_rejoin: bool,
        desired: Box<DesiredSlotsSpec>,
        reply: oneshot::Sender<Result<SlotReconcilePlan, PgError>>,
    },
    Disconnect,
}

/// Cloneable bounded-channel handle to the single-connection [`SqlControlActor`].
///
/// Enforces `AGENTS.md` Red Line #4: all local PostgreSQL inspection, promotion, reload, and
/// default-DB slot operations serialize strictly over **one** persistent connection so REST API
/// storms can never exhaust `superuser_reserved_connections`.
#[derive(Clone, Debug)]
pub struct SqlControlHandle {
    tx: mpsc::Sender<SqlCommand>,
}

impl SqlControlHandle {
    pub fn spawn(endpoint: LocalSqlEndpointConfig) -> Self {
        let (tx, rx) = mpsc::channel(DEFAULT_SQL_CHANNEL_CAPACITY);
        let mut actor = SqlControlActor {
            endpoint,
            client: None,
            rx,
        };
        tokio::spawn(async move {
            actor.run().await;
        });
        Self { tx }
    }

    async fn call<T>(
        &self,
        make_cmd: impl FnOnce(oneshot::Sender<Result<T, PgError>>) -> SqlCommand,
    ) -> Result<T, PgError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(make_cmd(reply_tx))
            .await
            .map_err(|_| PgError::Supervision("SqlControlActor channel closed".to_owned()))?;
        reply_rx
            .await
            .map_err(|_| PgError::Supervision("SqlControlActor dropped reply channel".to_owned()))?
    }

    pub async fn fetch_telemetry(&self) -> Result<PgTelemetrySnapshot, PgError> {
        self.call(|reply| SqlCommand::Telemetry { reply }).await
    }

    pub async fn fetch_checkpoint_stats(&self, major: PgMajor) -> Result<CheckpointStats, PgError> {
        self.call(|reply| SqlCommand::CheckpointStats { major, reply })
            .await
    }

    pub async fn promote(&self) -> Result<bool, PgError> {
        self.call(|reply| SqlCommand::Promote { reply }).await
    }

    pub async fn reload_conf(&self) -> Result<bool, PgError> {
        self.call(|reply| SqlCommand::ReloadConf { reply }).await
    }

    pub async fn query_system_identifier(&self) -> Result<String, PgError> {
        self.call(|reply| SqlCommand::SystemIdentifier { reply })
            .await
    }

    pub async fn ping_simple(&self) -> Result<(), PgError> {
        self.call(|reply| SqlCommand::PingSimple { reply }).await
    }

    pub async fn reconcile_slots(
        &self,
        major: PgMajor,
        is_primary: bool,
        is_demoted_rejoin: bool,
        desired: DesiredSlotsSpec,
    ) -> Result<SlotReconcilePlan, PgError> {
        self.call(|reply| SqlCommand::ReconcileSlots {
            major,
            is_primary,
            is_demoted_rejoin,
            desired: Box::new(desired),
            reply,
        })
        .await
    }

    pub async fn disconnect(&self) {
        let _ = self.tx.send(SqlCommand::Disconnect).await;
    }
}

struct SqlControlActor {
    endpoint: LocalSqlEndpointConfig,
    client: Option<Client>,
    rx: mpsc::Receiver<SqlCommand>,
}

impl SqlControlActor {
    async fn ensure_client(&mut self) -> Result<&Client, PgError> {
        let needs_reconnect = self.client.as_ref().is_none_or(Client::is_closed);
        if needs_reconnect {
            let db = self.endpoint.maintenance_db.clone();
            let connected = connect_single_client(&self.endpoint, &db).await?;
            self.client = Some(connected);
        }
        self.client
            .as_ref()
            .ok_or_else(|| PgError::Supervision("missing SQL client after connect".to_owned()))
    }

    async fn run(&mut self) {
        while let Some(cmd) = self.rx.recv().await {
            match cmd {
                SqlCommand::Disconnect => {
                    self.client = None;
                }
                SqlCommand::PingSimple { reply } => {
                    let res = self.exec_ping_simple().await;
                    if res.is_err() {
                        self.client = None;
                    }
                    let _ = reply.send(res);
                }
                SqlCommand::Telemetry { reply } => {
                    let res = self.exec_telemetry().await;
                    if res.is_err() {
                        self.client = None;
                    }
                    let _ = reply.send(res);
                }
                SqlCommand::CheckpointStats { major, reply } => {
                    let res = self.exec_checkpoint_stats(major).await;
                    if res.is_err() {
                        self.client = None;
                    }
                    let _ = reply.send(res);
                }
                SqlCommand::Promote { reply } => {
                    let res = self.exec_promote().await;
                    if res.is_err() {
                        self.client = None;
                    }
                    let _ = reply.send(res);
                }
                SqlCommand::ReloadConf { reply } => {
                    let res = self.exec_reload_conf().await;
                    if res.is_err() {
                        self.client = None;
                    }
                    let _ = reply.send(res);
                }
                SqlCommand::SystemIdentifier { reply } => {
                    let res = self.exec_system_identifier().await;
                    if res.is_err() {
                        self.client = None;
                    }
                    let _ = reply.send(res);
                }
                SqlCommand::ReconcileSlots {
                    major,
                    is_primary,
                    is_demoted_rejoin,
                    desired,
                    reply,
                } => {
                    let res = self
                        .exec_reconcile_slots(major, is_primary, is_demoted_rejoin, &desired)
                        .await;
                    let _ = reply.send(res);
                }
            }
        }
    }

    async fn exec_ping_simple(&mut self) -> Result<(), PgError> {
        let client = self.ensure_client().await?;
        client
            .simple_query("SELECT 1")
            .await
            .map_err(PgError::Sql)?;
        Ok(())
    }

    async fn exec_telemetry(&mut self) -> Result<PgTelemetrySnapshot, PgError> {
        let client = self.ensure_client().await?;
        let row = client
            .query_one(TELEMETRY_1RTT_SQL, &[])
            .await
            .map_err(PgError::Sql)?;

        let version_num: i32 = row.try_get(0).map_err(PgError::Sql)?;
        let pg_major = PgMajor::from_version_num(version_num)?;
        let in_recovery: bool = row.try_get(1).map_err(PgError::Sql)?;
        let lsn_i64: i64 = row.try_get(2).map_err(PgError::Sql)?;
        let replay_i64: Option<i64> = row.try_get(3).map_err(PgError::Sql)?;
        let wal_paused: bool = row.try_get(4).map_err(PgError::Sql)?;
        let timeline_i64: i64 = row.try_get(5).map_err(PgError::Sql)?;
        let is_streaming: bool = row.try_get(6).map_err(PgError::Sql)?;
        let pending_restart: bool = row.try_get(7).map_err(PgError::Sql)?;
        let data_checksums: bool = row.try_get(8).map_err(PgError::Sql)?;
        let wal_log_hints: bool = row.try_get(9).map_err(PgError::Sql)?;

        Ok(PgTelemetrySnapshot {
            pg_major,
            in_recovery,
            primary_or_recv_lsn: WalLsn(u64::try_from(lsn_i64.max(0)).unwrap_or(0)),
            replay_lsn: replay_i64.map(|v| WalLsn(u64::try_from(v.max(0)).unwrap_or(0))),
            wal_paused,
            timeline_id: u64::try_from(timeline_i64.max(0)).unwrap_or(0),
            is_streaming,
            pending_restart,
            data_checksums,
            wal_log_hints,
        })
    }

    async fn exec_checkpoint_stats(&mut self, major: PgMajor) -> Result<CheckpointStats, PgError> {
        let client = self.ensure_client().await?;
        let row = client
            .query_one(checkpoint_stats_sql(major), &[])
            .await
            .map_err(PgError::Sql)?;
        let timed: i64 = row.try_get(0).map_err(PgError::Sql)?;
        let requested: i64 = row.try_get(1).map_err(PgError::Sql)?;
        Ok(CheckpointStats {
            timed: u64::try_from(timed.max(0)).unwrap_or(0),
            requested: u64::try_from(requested.max(0)).unwrap_or(0),
        })
    }

    async fn exec_promote(&mut self) -> Result<bool, PgError> {
        let client = self.ensure_client().await?;
        let row = client
            .query_one("SELECT pg_catalog.pg_promote(true, 60)", &[])
            .await
            .map_err(PgError::Sql)?;
        row.try_get(0).map_err(PgError::Sql)
    }

    async fn exec_reload_conf(&mut self) -> Result<bool, PgError> {
        let client = self.ensure_client().await?;
        let row = client
            .query_one("SELECT pg_catalog.pg_reload_conf()", &[])
            .await
            .map_err(PgError::Sql)?;
        row.try_get(0).map_err(PgError::Sql)
    }

    async fn exec_system_identifier(&mut self) -> Result<String, PgError> {
        let client = self.ensure_client().await?;
        let row = client
            .query_one(
                "SELECT system_identifier::text FROM pg_catalog.pg_control_system()",
                &[],
            )
            .await
            .map_err(PgError::Sql)?;
        row.try_get(0).map_err(PgError::Sql)
    }

    async fn fetch_observed_slots(
        &mut self,
        major: PgMajor,
    ) -> Result<BTreeMap<CompactString, ObservedSlot>, PgError> {
        let client = self.ensure_client().await?;
        let rows = client
            .query(list_replication_slots_sql(major), &[])
            .await
            .map_err(PgError::Sql)?;

        let mut out = BTreeMap::new();
        for row in rows {
            let name: String = row.try_get(0).map_err(PgError::Sql)?;
            let is_logical: bool = row.try_get(1).map_err(PgError::Sql)?;
            let database: Option<String> = row.try_get(2).map_err(PgError::Sql)?;
            let plugin: Option<String> = row.try_get(3).map_err(PgError::Sql)?;
            let restart_i64: Option<i64> = row.try_get(4).map_err(PgError::Sql)?;
            let confirmed_i64: Option<i64> = row.try_get(5).map_err(PgError::Sql)?;
            let active: bool = row.try_get(6).map_err(PgError::Sql)?;
            let conflicting: bool = row.try_get(7).map_err(PgError::Sql)?;
            let failover: bool = row.try_get(8).map_err(PgError::Sql)?;
            let synced: bool = row.try_get(9).map_err(PgError::Sql)?;
            let skip_reason: Option<String> = row.try_get(10).map_err(PgError::Sql)?;

            let slot_name = CompactString::from(name);
            out.insert(
                slot_name.clone(),
                ObservedSlot {
                    slot_name,
                    is_logical,
                    database: database.map(CompactString::from),
                    plugin: plugin.map(CompactString::from),
                    restart_lsn: restart_i64.map(|v| WalLsn(u64::try_from(v.max(0)).unwrap_or(0))),
                    confirmed_flush_lsn: confirmed_i64
                        .map(|v| WalLsn(u64::try_from(v.max(0)).unwrap_or(0))),
                    active,
                    conflicting,
                    failover,
                    synced,
                    slotsync_skip_reason: skip_reason.map(CompactString::from),
                },
            );
        }
        Ok(out)
    }

    async fn exec_reconcile_slots(
        &mut self,
        major: PgMajor,
        is_primary: bool,
        is_demoted_rejoin: bool,
        desired: &DesiredSlotsSpec,
    ) -> Result<SlotReconcilePlan, PgError> {
        let observed = self.fetch_observed_slots(major).await?;
        let plan = plan_slot_reconciliation(
            major.capabilities(),
            is_primary,
            is_demoted_rejoin,
            &observed,
            desired,
        );
        if plan.is_empty() {
            return Ok(plan);
        }

        let endpoint = self.endpoint.clone();
        let default_db = endpoint.maintenance_db.clone();
        let client = self.ensure_client().await?;
        for drop_name in &plan.drop_slots {
            let name_str = drop_name.as_str();
            client
                .execute(
                    "SELECT pg_catalog.pg_drop_replication_slot($1)",
                    &[&name_str],
                )
                .await
                .map_err(PgError::Sql)?;
        }

        for phys_name in &plan.create_physical {
            let name_str = phys_name.as_str();
            client
                .execute(
                    "SELECT pg_catalog.pg_create_physical_replication_slot($1, true, false)",
                    &[&name_str],
                )
                .await
                .map_err(PgError::Sql)?;
        }

        for op in &plan.create_logical {
            if op.database == default_db {
                execute_create_logical_slot(client, op).await?;
            } else {
                // Allowed exception in AGENTS.md Red Line #4: short-lived per-DB connection
                // strictly for logical replication slots in non-default databases.
                let transient = connect_single_client(&endpoint, &op.database).await?;
                execute_create_logical_slot(&transient, op).await?;
            }
        }

        for adv in &plan.advance_logical {
            if adv.database == default_db {
                execute_advance_logical_slot(client, adv).await?;
            } else {
                let transient = connect_single_client(&endpoint, &adv.database).await?;
                execute_advance_logical_slot(&transient, adv).await?;
            }
        }

        Ok(plan)
    }
}

async fn execute_create_logical_slot(
    client: &Client,
    op: &LogicalSlotCreateOp,
) -> Result<(), PgError> {
    let slot_str = op.slot_name.as_str();
    let plugin_str = op.plugin.as_str();
    if op.enable_native_failover {
        client
            .execute(
                "SELECT pg_catalog.pg_create_logical_replication_slot($1, $2, false, false, true)",
                &[&slot_str, &plugin_str],
            )
            .await
            .map_err(PgError::Sql)?;
    } else {
        client
            .execute(
                "SELECT pg_catalog.pg_create_logical_replication_slot($1, $2, false)",
                &[&slot_str, &plugin_str],
            )
            .await
            .map_err(PgError::Sql)?;
    }
    Ok(())
}

async fn execute_advance_logical_slot(
    client: &Client,
    op: &LogicalSlotAdvanceOp,
) -> Result<(), PgError> {
    let slot_str = op.slot_name.as_str();
    let lsn_str = op.target_lsn.to_string();
    client
        .execute(
            "SELECT pg_catalog.pg_replication_slot_advance($1, $2::pg_lsn)",
            &[&slot_str, &lsn_str],
        )
        .await
        .map_err(PgError::Sql)?;
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::UnixListener;

    use super::*;

    /// Speaks minimal PostgreSQL v3 Frontend/Backend wire protocol over a Unix domain socket
    /// to verify `SqlControlActor` serializes concurrent callers onto strictly ONE connection!
    async fn run_mock_pg_unix_server(listener: UnixListener, accepted_count: Arc<AtomicUsize>) {
        while let Ok((mut stream, _)) = listener.accept().await {
            accepted_count.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async move {
                // Read StartupMessage (`len: i32`, `protocol: 196608`, key/value pairs)
                let Ok(startup_len) = stream.read_i32().await else {
                    return;
                };
                let mut startup_body = vec![0u8; (startup_len as usize).saturating_sub(4)];
                if stream.read_exact(&mut startup_body).await.is_err() {
                    return;
                }

                // Send AuthenticationOk ('R', len=8, status=0) + ReadyForQuery ('Z', len=5, 'I')
                let mut hello = Vec::new();
                hello.push(b'R');
                hello.extend_from_slice(&8_i32.to_be_bytes());
                hello.extend_from_slice(&0_i32.to_be_bytes());
                hello.push(b'Z');
                hello.extend_from_slice(&5_i32.to_be_bytes());
                hello.push(b'I');
                if stream.write_all(&hello).await.is_err() {
                    return;
                }

                // Serve SimpleQuery ('Q') requests over the persistent connection
                while let Ok(tag) = stream.read_u8().await {
                    let Ok(msg_len) = stream.read_i32().await else {
                        break;
                    };
                    let mut payload = vec![0u8; (msg_len as usize).saturating_sub(4)];
                    if stream.read_exact(&mut payload).await.is_err() {
                        break;
                    }
                    if tag == b'X' {
                        break;
                    }
                    if tag == b'Q' {
                        // CommandComplete ('C', "SELECT 1\0") + ReadyForQuery ('Z', 'I')
                        let cmd_tag = b"SELECT 1\0";
                        let mut reply = Vec::new();
                        reply.push(b'C');
                        reply.extend_from_slice(&((4 + cmd_tag.len()) as i32).to_be_bytes());
                        reply.extend_from_slice(cmd_tag);
                        reply.push(b'Z');
                        reply.extend_from_slice(&5_i32.to_be_bytes());
                        reply.push(b'I');
                        if stream.write_all(&reply).await.is_err() {
                            break;
                        }
                    }
                }
            });
        }
    }

    #[tokio::test]
    async fn single_connection_actor_serializes_concurrent_storms_onto_one_unix_socket() {
        let sock_dir =
            std::env::temp_dir().join(format!("negroni-sql-sock-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&sock_dir);
        std::fs::create_dir_all(&sock_dir).unwrap();

        let port = 15432_u16;
        let sock_path = sock_dir.join(format!(".s.PGSQL.{port}"));
        let listener = UnixListener::bind(&sock_path).unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let server_accepted = Arc::clone(&accepted);
        let server_task = tokio::spawn(async move {
            run_mock_pg_unix_server(listener, server_accepted).await;
        });

        let endpoint = LocalSqlEndpointConfig {
            pgdata: sock_dir.clone(),
            listen_host: "127.0.0.1".to_owned(),
            listen_port: port,
            use_unix_socket: true,
            explicit_socket_dir: Some(sock_dir.clone()),
            maintenance_db: CompactString::const_new("postgres"),
            superuser: PgRoleCredentials::default(),
            connect_timeout: Duration::from_secs(2),
        };

        let sql = SqlControlHandle::spawn(endpoint);

        // Fire 16 concurrent control requests simultaneously!
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let h = sql.clone();
            tasks.push(tokio::spawn(async move { h.ping_simple().await }));
        }
        for t in tasks {
            t.await.unwrap().unwrap();
        }

        // Invariant check: ALL 16 concurrent callers shared strictly ONE Unix socket connection!
        assert_eq!(accepted.load(Ordering::SeqCst), 1);

        sql.disconnect().await;
        server_task.abort();
        let _ = std::fs::remove_dir_all(&sock_dir);
    }
}
