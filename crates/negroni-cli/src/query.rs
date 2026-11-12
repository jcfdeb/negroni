use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use comfy_table::Table;
use comfy_table::presets::ASCII_FULL;
use compact_str::CompactString;
use negroni_config::DaemonConfig;
use negroni_dcs::DcsBackend;
use negroni_types::{ClusterSnapshot, MemberData};
use rustls::RootCertStore;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio_postgres::{Config as PgClientConfig, NoTls, SimpleQueryMessage};
use tokio_postgres_rustls::MakeRustlsConnect;

#[derive(Debug, Clone)]
pub struct QueryArgs {
    pub role: Option<String>,
    pub members: Vec<String>,
    pub sql_file: Option<PathBuf>,
    pub command: Option<String>,
    pub username: Option<String>,
    pub dbname: Option<String>,
    pub watch_secs: Option<Option<u64>>,
}

/// Resolves matching `(member_name, conn_url)` pairs from [`ClusterSnapshot`] for `dsn` and `query`.
pub fn select_target_members(
    snap: &ClusterSnapshot,
    role_filter: Option<&str>,
    member_filters: &[String],
) -> Vec<(CompactString, MemberData)> {
    let explicit_names: Vec<&str> = member_filters
        .iter()
        .flat_map(|s| s.split(','))
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();

    let leader_name = snap.leader.as_ref().map(|l| &l.value);
    let mut out = Vec::new();

    for (name, ver) in &snap.members {
        if !explicit_names.is_empty() && !explicit_names.contains(&name.as_str()) {
            continue;
        }
        let is_leader = leader_name == Some(name);
        if let Some(rf) = role_filter {
            match rf.trim().to_ascii_lowercase().as_str() {
                "primary" | "master" | "leader" | "read-write" => {
                    if !is_leader {
                        continue;
                    }
                }
                "replica" | "standby" | "read-only" => {
                    if is_leader {
                        continue;
                    }
                }
                "any" | "" => {}
                other => {
                    if ver.value.role.as_str() != other {
                        continue;
                    }
                }
            }
        }
        out.push((name.clone(), ver.value.clone()));
    }
    out
}

/// Formats a PostgreSQL connection URL (`postgres://host:port/db`) into a `libpq` key-value DSN
/// (`host=... port=... dbname=...`) for `negronictl dsn`.
///
/// # Examples
///
/// ```
/// use negroni_cli::query::conn_url_to_libpq_dsn;
///
/// let dsn = conn_url_to_libpq_dsn("postgres://10.0.0.5:5432/appdb", Some("alice"), None);
/// assert_eq!(dsn, "host=10.0.0.5 port=5432 dbname=appdb user=alice");
/// ```
pub fn conn_url_to_libpq_dsn(
    conn_url: &str,
    override_user: Option<&str>,
    override_db: Option<&str>,
) -> String {
    let without_scheme = conn_url
        .split_once("://")
        .map_or(conn_url, |(_, rest)| rest);
    let (authority, path_and_q) = without_scheme
        .split_once('/')
        .unwrap_or((without_scheme, "postgres"));
    let db_raw = path_and_q.split_once('?').map_or(path_and_q, |(d, _)| d);
    let dbname = override_db
        .filter(|s| !s.is_empty())
        .unwrap_or(if db_raw.is_empty() {
            "postgres"
        } else {
            db_raw
        });

    let (userinfo, host_port) = match authority.rsplit_once('@') {
        Some((u, hp)) => (Some(u), hp),
        None => (None, authority),
    };
    let parsed_user = userinfo.map(|u| u.split_once(':').map_or(u, |(usr, _)| usr));
    let effective_user = override_user.or(parsed_user);

    let (host, port) = if let Some(bracket_rest) = host_port.strip_prefix('[')
        && let Some((v6, after)) = bracket_rest.split_once(']')
    {
        let p = after
            .strip_prefix(':')
            .filter(|s| !s.is_empty())
            .unwrap_or("5432");
        (v6, p)
    } else if let Some((h, p)) = host_port.rsplit_once(':') {
        (h, p)
    } else {
        (host_port, "5432")
    };

    let mut dsn = format!("host={host} port={port} dbname={dbname}");
    if let Some(u) = effective_user
        && !u.is_empty()
    {
        let _ = write!(dsn, " user={u}");
    }
    dsn
}

fn build_pg_tls_connector(daemon_cfg: Option<&DaemonConfig>) -> Result<Option<MakeRustlsConnect>> {
    let Some(cfg) = daemon_cfg else {
        return Ok(None);
    };
    let su = &cfg.postgresql.authentication.superuser;
    let Some(ref ca_path) = su.sslrootcert else {
        return Ok(None);
    };

    let ca_file = File::open(ca_path).with_context(|| {
        format!(
            "failed to open PostgreSQL sslrootcert '{}'",
            ca_path.display()
        )
    })?;
    let mut ca_reader = BufReader::new(ca_file);
    let ca_certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut ca_reader)
        .collect::<Result<Vec<_>, _>>()
        .context("invalid PEM in sslrootcert")?;
    let mut roots = RootCertStore::empty();
    for cert in ca_certs {
        roots
            .add(cert)
            .map_err(|e| anyhow::anyhow!("invalid root cert in sslrootcert: {e}"))?;
    }

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| anyhow::anyhow!("rustls version config failed: {e}"))?
        .with_root_certificates(roots);

    let tls_cfg = if let (Some(cert_p), Some(key_p)) = (&su.sslcert, &su.sslkey) {
        let mut cert_reader = BufReader::new(File::open(cert_p)?);
        let certs: Vec<CertificateDer<'static>> =
            rustls_pemfile::certs(&mut cert_reader).collect::<Result<Vec<_>, _>>()?;
        let mut key_reader = BufReader::new(File::open(key_p)?);
        let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_reader)?
            .ok_or_else(|| anyhow::anyhow!("no private key found in '{}'", key_p.display()))?;
        builder
            .with_client_auth_cert(certs, key)
            .map_err(|e| anyhow::anyhow!("invalid PostgreSQL client cert/key: {e}"))?
    } else {
        builder.with_no_client_auth()
    };

    Ok(Some(MakeRustlsConnect::new(tls_cfg)))
}

async fn execute_sql_against_member(
    conn_url: &str,
    sql: &str,
    daemon_cfg: Option<&DaemonConfig>,
    override_user: Option<&str>,
    override_db: Option<&str>,
) -> Result<String> {
    let mut pg_cfg: PgClientConfig = conn_url
        .parse()
        .with_context(|| format!("invalid member PostgreSQL URL '{conn_url}'"))?;
    pg_cfg.connect_timeout(Duration::from_secs(5));

    if let Some(u) = override_user {
        pg_cfg.user(u);
    } else if let Some(cfg) = daemon_cfg
        && let Some(ref su_user) = cfg.postgresql.authentication.superuser.username
    {
        pg_cfg.user(su_user.as_str());
    }

    if let Some(cfg) = daemon_cfg
        && let Some(ref su_pass) = cfg.postgresql.authentication.superuser.password
    {
        pg_cfg.password(su_pass.expose());
    }

    if let Some(db) = override_db {
        pg_cfg.dbname(db);
    }

    let client = if let Some(tls) = build_pg_tls_connector(daemon_cfg)? {
        let (client, connection) = pg_cfg.connect(tls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    } else {
        let (client, connection) = pg_cfg.connect(NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    };

    let messages = client.simple_query(sql).await?;
    let mut table = Table::new();
    table.load_preset(ASCII_FULL);
    let mut header_set = false;
    let mut row_count = 0usize;
    let mut out = String::new();

    for msg in messages {
        match msg {
            SimpleQueryMessage::Row(row) => {
                let cols = row.columns();
                if !header_set {
                    let names: Vec<String> = cols.iter().map(|c| c.name().to_owned()).collect();
                    table.set_header(names);
                    header_set = true;
                }
                let mut cells = Vec::with_capacity(cols.len());
                for idx in 0..cols.len() {
                    cells.push(row.get(idx).unwrap_or("NULL").to_owned());
                }
                table.add_row(cells);
                row_count += 1;
            }
            SimpleQueryMessage::CommandComplete(affected) if row_count == 0 && !header_set => {
                let _ = writeln!(out, "COMMAND COMPLETE ({affected})");
            }
            _ => {}
        }
    }

    if header_set {
        let _ = writeln!(out, "{table}");
    }
    Ok(out)
}

pub async fn run_query<D: DcsBackend>(
    dcs: &D,
    daemon_cfg: Option<&DaemonConfig>,
    args: &QueryArgs,
) -> Result<()> {
    let sql = if let Some(ref cmd) = args.command {
        cmd.clone()
    } else if let Some(ref path) = args.sql_file {
        fs::read_to_string(path)
            .with_context(|| format!("failed to read SQL file '{}'", path.display()))?
    } else {
        bail!("Either -c/--command or -f/--file must be provided to 'query'");
    };

    let effective_role = args.role.as_deref().or(if args.members.is_empty() {
        Some("primary")
    } else {
        None
    });

    loop {
        let snap = dcs
            .fetch_cluster(Duration::from_secs(5))
            .await
            .context("failed to fetch cluster topology from DCS")?;
        let targets = select_target_members(&snap, effective_role, &args.members);
        if targets.is_empty() {
            bail!("No cluster members matched role/member selector");
        }

        for (idx, (name, member)) in targets.iter().enumerate() {
            if idx > 0 {
                println!();
            }
            if targets.len() > 1 {
                println!("=== Member: {name} ===");
            }
            let rendered = execute_sql_against_member(
                &member.conn_url,
                &sql,
                daemon_cfg,
                args.username.as_deref(),
                args.dbname.as_deref(),
            )
            .await?;
            print!("{rendered}");
        }

        match args.watch_secs {
            Some(opt_sec) => {
                tokio::time::sleep(Duration::from_secs(opt_sec.unwrap_or(2).max(1))).await;
            }
            None => break,
        }
    }

    Ok(())
}
