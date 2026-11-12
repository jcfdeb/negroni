use std::fs;
use std::path::{Path, PathBuf};

use compact_str::CompactString;
use negroni_config::{DaemonConfig, PgRoleCredentials, SecretString};
use tokio::process::Command;
use tokio::sync::watch;

use crate::PgError;
use crate::config_writer::write_pgpass_file;
use crate::pidfd_monitor::PostmasterPidFile;
use crate::process::{run_cancellable_command, write_standby_signal};
use crate::version::PgBinaryPaths;

/// Parsed connection components with credentials separated so CLI arguments (`ps`) never expose
/// replication or rewind passwords (`SECURITY.md` item #3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SanitizedConnTarget {
    pub host: String,
    pub port: String,
    pub dbname: String,
    pub user: Option<String>,
    pub password: Option<SecretString>,
    pub password_free_dsn: String,
}

/// Parses a `postgres://` URI (or keyword=value string), stripping embedded passwords while
/// injecting configured `role_creds` username/TLS paths cleanly.
///
/// # Examples
///
/// ```
/// use negroni_config::PgRoleCredentials;
/// use negroni_pg::clone::sanitize_conninfo_for_cli;
///
/// let target = sanitize_conninfo_for_cli(
///     "postgres://replicator:topsecret@10.24.0.10:5432/postgres",
///     &PgRoleCredentials::default(),
///     "negroni-2",
/// );
/// assert!(!target.password_free_dsn.contains("topsecret"));
/// assert!(target.password_free_dsn.contains("host=10.24.0.10"));
/// assert!(target.password_free_dsn.contains("user=replicator"));
/// assert_eq!(target.password.as_ref().map(|s| s.expose()), Some("topsecret"));
/// ```
pub fn sanitize_conninfo_for_cli(
    raw_conn: &str,
    role_creds: &PgRoleCredentials,
    app_name: &str,
) -> SanitizedConnTarget {
    let mut host = "127.0.0.1".to_owned();
    let mut port = "5432".to_owned();
    let mut dbname = "postgres".to_owned();
    let mut user: Option<String> = role_creds.username.as_ref().map(ToString::to_string);
    let mut password: Option<SecretString> = role_creds.password.clone();

    if let Some(rest) = raw_conn
        .strip_prefix("postgres://")
        .or_else(|| raw_conn.strip_prefix("postgresql://"))
    {
        let (authority_and_path, _query) = rest
            .split_once('?')
            .map_or((rest, ""), |(lhs, rhs)| (lhs, rhs));
        let (authority, path_db) = authority_and_path
            .split_once('/')
            .map_or((authority_and_path, ""), |(a, p)| (a, p));
        let (userinfo, hostport) = authority
            .rsplit_once('@')
            .map_or((None, authority), |(u, h)| (Some(u), h));

        if let Some(bracketed) = hostport.strip_prefix('[') {
            if let Some((ipv6, after)) = bracketed.split_once(']') {
                host = ipv6.to_owned();
                if let Some(p) = after.strip_prefix(':').filter(|s| !s.is_empty()) {
                    port = p.to_owned();
                }
            }
        } else if let Some((h, p)) = hostport.rsplit_once(':') {
            if !h.is_empty() {
                host = h.to_owned();
            }
            if !p.is_empty() {
                port = p.to_owned();
            }
        } else if !hostport.is_empty() {
            host = hostport.to_owned();
        }

        if let Some(ui) = userinfo {
            let (uri_user, uri_pw) = ui.split_once(':').map_or((ui, None), |(n, p)| (n, Some(p)));
            if user.is_none() && !uri_user.is_empty() {
                user = Some(uri_user.to_owned());
            }
            if password.is_none()
                && let Some(pw) = uri_pw.filter(|s| !s.is_empty())
            {
                password = Some(SecretString::new(pw));
            }
        }
        if !path_db.is_empty() {
            dbname = path_db.to_owned();
        }
    }

    let mut kv = vec![
        format!("host={host}"),
        format!("port={port}"),
        format!("dbname={dbname}"),
        format!("application_name={app_name}"),
    ];
    if let Some(ref u) = user {
        kv.push(format!("user={u}"));
    }
    if let Some(ref mode) = role_creds.sslmode {
        kv.push(format!("sslmode={mode}"));
    }
    if let Some(ref root_ca) = role_creds.sslrootcert {
        kv.push(format!("sslrootcert={}", root_ca.display()));
    }
    if let Some(ref cert) = role_creds.sslcert {
        kv.push(format!("sslcert={}", cert.display()));
    }
    if let Some(ref key) = role_creds.sslkey {
        kv.push(format!("sslkey={}", key.display()));
    }

    SanitizedConnTarget {
        host,
        port,
        dbname,
        user,
        password,
        password_free_dsn: kv.join(" "),
    }
}

/// Safely clears `$PGDATA` prior to fresh replica cloning, refusing root paths (`/`) or
/// directories with an active `postmaster.pid`.
pub fn prepare_empty_pgdata_for_clone(pgdata: &Path) -> Result<(), PgError> {
    if pgdata.parent().is_none() || pgdata.as_os_str().is_empty() {
        return Err(PgError::ConfigValidation(format!(
            "refusing to wipe unsafe PGDATA path '{}'",
            pgdata.display()
        )));
    }
    if PostmasterPidFile::read_from_pgdata(pgdata)?.is_some() {
        return Err(PgError::Supervision(format!(
            "refusing to wipe '{}' while postmaster.pid exists",
            pgdata.display()
        )));
    }

    if pgdata.exists() {
        for entry_res in fs::read_dir(pgdata).map_err(|source| PgError::Io {
            context: format!("listing '{}'", pgdata.display()),
            source,
        })? {
            let entry = entry_res.map_err(|source| PgError::Io {
                context: format!("reading entry in '{}'", pgdata.display()),
                source,
            })?;
            let path = entry.path();
            let ft = entry.file_type().map_err(|source| PgError::Io {
                context: format!("inspecting '{}'", path.display()),
                source,
            })?;
            if ft.is_dir() {
                fs::remove_dir_all(&path).map_err(|source| PgError::Io {
                    context: format!("removing directory '{}'", path.display()),
                    source,
                })?;
            } else {
                fs::remove_file(&path).map_err(|source| PgError::Io {
                    context: format!("removing file '{}'", path.display()),
                    source,
                })?;
            }
        }
    } else {
        fs::create_dir_all(pgdata).map_err(|source| PgError::Io {
            context: format!("creating PGDATA '{}'", pgdata.display()),
            source,
        })?;
    }
    Ok(())
}

pub(crate) struct EphemeralPgpassGuard {
    path: PathBuf,
}

impl EphemeralPgpassGuard {
    pub(crate) fn create(
        purpose: &str,
        target: &SanitizedConnTarget,
        creds: &PgRoleCredentials,
    ) -> Result<Option<Self>, PgError> {
        let Some(secret) = creds.password.as_ref().or(target.password.as_ref()) else {
            return Ok(None);
        };
        let path = std::env::temp_dir().join(format!(
            ".negroni-pgpass-{purpose}-{}.tmp",
            std::process::id()
        ));
        let user = target.user.as_deref().unwrap_or("*");
        write_pgpass_file(
            &path,
            &target.host,
            &target.port,
            "*",
            user,
            secret.expose(),
        )?;
        Ok(Some(Self { path }))
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for EphemeralPgpassGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

/// Runs streaming `pg_basebackup` against `upstream_conn`, passing credentials strictly via an
/// ephemeral `0600` `PGPASSFILE` and writing `standby.signal` on completion.
pub async fn run_pg_basebackup(
    cfg: &DaemonConfig,
    bins: &PgBinaryPaths,
    upstream_conn: &str,
    cancel_rx: watch::Receiver<bool>,
) -> Result<(), PgError> {
    let pgdata = &cfg.postgresql.data_dir;
    prepare_empty_pgdata_for_clone(pgdata)?;

    let repl_creds = &cfg.postgresql.authentication.replication;
    let target = sanitize_conninfo_for_cli(upstream_conn, repl_creds, &cfg.name);
    let pgpass_guard = EphemeralPgpassGuard::create("basebackup", &target, repl_creds)?;

    let mut cmd = Command::new(bins.pg_basebackup());
    cmd.arg("--pgdata")
        .arg(pgdata)
        .arg("-Fp")
        .arg("-Xs")
        .arg("-R")
        .arg("-c")
        .arg("fast")
        .arg("--dbname")
        .arg(&target.password_free_dsn);

    if let Some(ref guard) = pgpass_guard {
        cmd.env("PGPASSFILE", guard.path());
    } else if let Some(ref explicit_pgpass) = cfg.postgresql.pgpass {
        cmd.env("PGPASSFILE", explicit_pgpass);
    }

    run_cancellable_command(cmd, "pg_basebackup", cancel_rx).await?;
    write_standby_signal(pgdata)?;
    Ok(())
}

/// Splits a command string into argv words using POSIX `shlex` quoting rules
/// (single quotes, double quotes, and backslash escapes) without invoking `/bin/sh -c`.
pub fn split_command_words(command_line: &str) -> Result<Vec<String>, PgError> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut in_token = false;
    let mut single_quoted = false;
    let mut double_quoted = false;
    let mut chars = command_line.chars().peekable();

    while let Some(ch) = chars.next() {
        if single_quoted {
            if ch == '\'' {
                single_quoted = false;
            } else {
                current.push(ch);
            }
            continue;
        }
        if double_quoted {
            match ch {
                '"' => double_quoted = false,
                '\\' => {
                    if let Some(&next) = chars.peek() {
                        if matches!(next, '"' | '\\' | '$' | '`') {
                            current.push(next);
                            chars.next();
                        } else {
                            current.push('\\');
                        }
                    } else {
                        return Err(PgError::ConfigValidation(format!(
                            "trailing backslash in command '{command_line}'"
                        )));
                    }
                }
                other => current.push(other),
            }
            continue;
        }

        match ch {
            c if c.is_ascii_whitespace() => {
                if in_token {
                    words.push(std::mem::take(&mut current));
                    in_token = false;
                }
            }
            '\'' => {
                single_quoted = true;
                in_token = true;
            }
            '"' => {
                double_quoted = true;
                in_token = true;
            }
            '\\' => {
                in_token = true;
                let escaped = chars.next().ok_or_else(|| {
                    PgError::ConfigValidation(format!(
                        "trailing backslash in command '{command_line}'"
                    ))
                })?;
                current.push(escaped);
            }
            other => {
                in_token = true;
                current.push(other);
            }
        }
    }

    if single_quoted || double_quoted {
        return Err(PgError::ConfigValidation(format!(
            "unterminated quote in command '{command_line}'"
        )));
    }
    if in_token {
        words.push(current);
    }
    if words.is_empty() {
        return Err(PgError::ConfigValidation(
            "custom command string cannot be empty".to_owned(),
        ));
    }
    Ok(words)
}

/// Executes a custom replica bootstrap tool (e.g. `wal-g`, `pgbackrest`, `barman`) with
/// Patroni-compatible `--scope`, `--role=replica`, `--datadir`, and `--connstring` arguments.
pub async fn run_custom_replica_command(
    method_name: &str,
    command_line: &str,
    cfg: &DaemonConfig,
    upstream_conn: &str,
    cancel_rx: watch::Receiver<bool>,
) -> Result<(), PgError> {
    let words = split_command_words(command_line)?;
    let pgdata = &cfg.postgresql.data_dir;
    prepare_empty_pgdata_for_clone(pgdata)?;

    let repl_creds = &cfg.postgresql.authentication.replication;
    let target = sanitize_conninfo_for_cli(upstream_conn, repl_creds, &cfg.name);
    let pgpass_guard = EphemeralPgpassGuard::create(method_name, &target, repl_creds)?;

    let mut cmd = Command::new(&words[0]);
    cmd.args(&words[1..])
        .arg(format!("--scope={}", cfg.scope))
        .arg("--role=replica")
        .arg(format!("--datadir={}", pgdata.display()))
        .arg(format!("--connstring={}", target.password_free_dsn));

    if let Some(ref guard) = pgpass_guard {
        cmd.env("PGPASSFILE", guard.path());
    }

    run_cancellable_command(cmd, method_name, cancel_rx).await?;
    write_standby_signal(pgdata)?;
    Ok(())
}

/// Iterates `postgresql.create_replica_methods` in priority order (defaulting to `["basebackup"]`),
/// stopping immediately on success or cooperative cancellation while falling through to the next
/// method if an archive restore method (`wal-g`, `pgbackrest`) fails.
pub async fn clone_replica_with_fallback(
    cfg: &DaemonConfig,
    bins: &PgBinaryPaths,
    upstream_conn: &str,
    custom_method_commands: &[(CompactString, String)],
    cancel_rx: watch::Receiver<bool>,
) -> Result<CompactString, PgError> {
    let methods: Vec<CompactString> = if cfg.postgresql.create_replica_methods.is_empty() {
        vec![CompactString::const_new("basebackup")]
    } else {
        cfg.postgresql.create_replica_methods.clone()
    };

    let mut last_err: Option<PgError> = None;

    for method in methods {
        if *cancel_rx.borrow() {
            return Err(PgError::Cancelled("clone_replica".to_owned()));
        }

        let res = if method == "basebackup" {
            run_pg_basebackup(cfg, bins, upstream_conn, cancel_rx.clone()).await
        } else if let Some((_, cmd_str)) = custom_method_commands.iter().find(|(m, _)| *m == method)
        {
            run_custom_replica_command(&method, cmd_str, cfg, upstream_conn, cancel_rx.clone())
                .await
        } else {
            Err(PgError::ConfigValidation(format!(
                "create_replica_methods lists '{method}', but no command is configured for it"
            )))
        };

        match res {
            Ok(()) => return Ok(method),
            Err(PgError::Cancelled(reason)) => return Err(PgError::Cancelled(reason)),
            Err(err) => {
                tracing::warn!(
                    method = %method,
                    error = %err,
                    "replica creation method failed; trying next configured fallback method"
                );
                last_err = Some(err);
            }
        }
    }

    Err(last_err.unwrap_or_else(|| {
        PgError::Supervision("all configured create_replica_methods failed".to_owned())
    }))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use negroni_config::parse_and_validate_with_env;

    use super::*;
    use crate::config_writer::atomic_write_file;

    #[tokio::test]
    async fn falls_back_from_failing_custom_method_to_working_method_and_writes_standby_signal() {
        let tmp_root =
            std::env::temp_dir().join(format!("negroni-clone-fb-{}", std::process::id()));
        let _ = fs::remove_dir_all(&tmp_root);
        fs::create_dir_all(&tmp_root).unwrap();
        let tmp_pgdata = tmp_root.join("pgdata");
        fs::create_dir_all(&tmp_pgdata).unwrap();

        let fail_script = tmp_root.join("wal_g_fail.sh");
        atomic_write_file(&fail_script, b"#!/bin/sh\nexit 1\n", Some(0o700)).unwrap();

        let ok_script = tmp_root.join("pgbackrest_ok.sh");
        let script_body = br#"#!/bin/sh
set -eu
[ "$1" = "--stanza=main" ]
[ "$2" = "--scope=prod" ]
[ "$3" = "--role=replica" ]
datadir="${4#--datadir=}"
conn="${5#--connstring=}"
# Verify password was stripped from CLI argv and placed in ephemeral PGPASSFILE!
case "$conn" in
  *secret*) exit 99 ;;
esac
[ -f "$PGPASSFILE" ]
mkdir -p "$datadir/global"
echo 17 > "$datadir/PG_VERSION"
"#;
        atomic_write_file(&ok_script, script_body, Some(0o700)).unwrap();

        let yaml = format!(
            r#"
scope: prod
name: pg-2
watchdog:
  mode: off
postgresql:
  data_dir: {}
  create_replica_methods:
    - wal_g
    - pgbackrest
"#,
            tmp_pgdata.display()
        );
        let empty_env: [(&str, &str); 0] = [];
        let cfg = parse_and_validate_with_env(&yaml, "clone.yml", empty_env).unwrap();

        let custom_cmds = vec![
            (
                CompactString::const_new("wal_g"),
                fail_script.display().to_string(),
            ),
            (
                CompactString::const_new("pgbackrest"),
                format!("{} --stanza=main", ok_script.display()),
            ),
        ];

        let (_tx, rx) = watch::channel(false);
        let chosen = clone_replica_with_fallback(
            &cfg,
            &PgBinaryPaths::new(None),
            "postgres://replicator:secret@10.0.0.1:5432/postgres",
            &custom_cmds,
            rx,
        )
        .await
        .unwrap();

        assert_eq!(chosen, "pgbackrest");
        assert!(tmp_pgdata.join("standby.signal").exists());
        assert_eq!(
            fs::read_to_string(tmp_pgdata.join("PG_VERSION"))
                .unwrap()
                .trim(),
            "17"
        );
        let _ = fs::remove_dir_all(&tmp_root);
    }
}
