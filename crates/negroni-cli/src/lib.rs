#![forbid(unsafe_code)]
#![allow(clippy::print_stdout, clippy::print_stderr)]

pub mod edit_config;
pub mod list;
pub mod query;
pub mod switchover;

use std::env;
use std::fs;
use std::io::{self, Write as _};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use negroni_config::{BasicAuthCredentials, DaemonConfig, deep_merge_json};
use negroni_dcs::{AnyDcs, DcsBackend};
use negroni_types::ClusterSnapshot;
use serde::Serialize;
use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::edit_config::{EditConfigArgs, json_to_yaml_string, run_edit_config};
use crate::list::{render_cluster_list, render_history, render_topology};
use crate::query::{QueryArgs, conn_url_to_libpq_dsn, run_query, select_target_members};
use crate::switchover::{FailoverArgs, SwitchoverArgs, run_failover, run_switchover};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum OutputFormat {
    #[default]
    Plain,
    Tsv,
    Json,
    #[value(alias = "yml")]
    Yaml,
    Pretty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum FlushTarget {
    Restart,
    Switchover,
}

#[derive(Debug, Parser)]
#[command(
    name = "negronictl",
    version,
    about = "Negroni / Patroni HA cluster control CLI (19-command patronictl parity)"
)]
pub struct CliApp {
    /// Path to patroni.yml / patronictl.yaml configuration file
    #[arg(short = 'c', long = "config-file")]
    pub config_file: Option<String>,

    /// Direct DCS endpoint URL (e.g., etcd3://127.0.0.1:2379, k8s://, memory://)
    #[arg(short = 'd', long = "dcs-url")]
    pub dcs_url: Option<String>,

    /// Allow connections to SSL sites without certs
    #[arg(short = 'k', long = "insecure")]
    pub insecure: bool,

    #[command(subcommand)]
    pub command: CliCommand,
}

#[derive(Debug, PartialEq, Eq, Subcommand)]
pub enum CliCommand {
    /// List the Patroni members for a given cluster
    List {
        /// Cluster scope name(s)
        #[arg()]
        cluster_names: Vec<String>,
        #[arg(short = 'e', long = "extended")]
        extended: bool,
        #[arg(short = 't', long = "timestamp")]
        timestamp: bool,
        #[arg(short = 'W', long = "watch", num_args = 0..=1, default_missing_value = "2")]
        watch: Option<u64>,
        #[arg(short = 'f', long = "format", value_enum, default_value_t = OutputFormat::Plain)]
        format: OutputFormat,
    },
    /// Prints ASCII topology for given cluster
    Topology {
        #[arg()]
        cluster_names: Vec<String>,
        #[arg(short = 'W', long = "watch", num_args = 0..=1, default_missing_value = "2")]
        watch: Option<u64>,
        #[arg(short = 'f', long = "format", value_enum, default_value_t = OutputFormat::Plain)]
        format: OutputFormat,
    },
    /// Generate a libpq dsn for the provided member, defaults to primary
    Dsn {
        #[arg()]
        cluster_name: Option<String>,
        #[arg(short = 'r', long = "role")]
        role: Option<String>,
        #[arg(short = 'm', long = "member")]
        member: Option<String>,
    },
    /// Query a Patroni PostgreSQL member
    Query {
        #[arg()]
        cluster_name: Option<String>,
        #[arg(short = 'r', long = "role")]
        role: Option<String>,
        #[arg(short = 'm', long = "member")]
        member: Vec<String>,
        #[arg(short = 'f', long = "file")]
        file: Option<PathBuf>,
        #[arg(short = 'c', long = "command")]
        command: Option<String>,
        #[arg(short = 'U', long = "username")]
        username: Option<String>,
        #[arg(long = "dbname")]
        dbname: Option<String>,
        #[arg(short = 'W', long = "watch", num_args = 0..=1, default_missing_value = "2")]
        watch: Option<u64>,
    },
    /// Show cluster configuration stored in DCS
    ShowConfig {
        #[arg()]
        cluster_name: Option<String>,
    },
    /// Edit cluster configuration stored in DCS
    EditConfig {
        #[arg()]
        cluster_name: Option<String>,
        #[arg(short = 's', long = "set")]
        set: Vec<String>,
        #[arg(short = 'p', long = "pg")]
        pg: Vec<String>,
        #[arg(long = "replace")]
        replace: Option<String>,
        #[arg(long = "apply")]
        apply: Option<String>,
        #[arg(long = "force")]
        force: bool,
        #[arg(short = 'q', long = "quiet")]
        quiet: bool,
    },
    /// Switchover to a replica
    Switchover {
        #[arg()]
        cluster_name: Option<String>,
        #[arg(long = "leader", alias = "master")]
        leader: Option<String>,
        #[arg(long = "candidate")]
        candidate: Option<String>,
        #[arg(long = "scheduled")]
        scheduled: Option<String>,
        #[arg(long = "force")]
        force: bool,
    },
    /// Failover to a replica
    Failover {
        #[arg()]
        cluster_name: Option<String>,
        #[arg(long = "leader", alias = "master")]
        leader: Option<String>,
        #[arg(long = "candidate")]
        candidate: Option<String>,
        #[arg(long = "force")]
        force: bool,
    },
    /// Disable auto failover (maintenance mode)
    Pause {
        #[arg()]
        cluster_name: Option<String>,
        #[arg(long = "wait")]
        wait: bool,
    },
    /// Resume auto failover
    Resume {
        #[arg()]
        cluster_name: Option<String>,
        #[arg(long = "wait")]
        wait: bool,
    },
    /// Restart cluster member(s)
    Restart {
        #[arg()]
        cluster_name: String,
        #[arg()]
        member_names: Vec<String>,
        #[arg(short = 'r', long = "role")]
        role: Option<String>,
        #[arg(long = "pg-version")]
        pg_version: Option<String>,
        #[arg(long = "pending")]
        pending: bool,
        #[arg(long = "scheduled")]
        scheduled: Option<String>,
        #[arg(long = "timeout")]
        timeout: Option<u64>,
        #[arg(long = "force")]
        force: bool,
    },
    /// Reload configuration on cluster member(s)
    Reload {
        #[arg()]
        cluster_name: String,
        #[arg()]
        member_names: Vec<String>,
        #[arg(short = 'r', long = "role")]
        role: Option<String>,
        #[arg(long = "force")]
        force: bool,
    },
    /// Reinitialize cluster member(s)
    Reinit {
        #[arg()]
        cluster_name: String,
        #[arg(required = true)]
        member_names: Vec<String>,
        #[arg(long = "force")]
        force: bool,
        #[arg(long = "wait")]
        wait: bool,
    },
    /// Discard scheduled events (`restart` or `switchover`)
    Flush {
        #[arg()]
        cluster_name: String,
        #[arg(value_enum)]
        target: FlushTarget,
        #[arg()]
        member_names: Vec<String>,
        #[arg(short = 'r', long = "role")]
        role: Option<String>,
        #[arg(long = "force")]
        force: bool,
    },
    /// Remove cluster from DCS
    Remove {
        #[arg()]
        cluster_name: String,
    },
    /// Show cluster failover/switchover timeline history
    History {
        #[arg()]
        cluster_name: Option<String>,
        #[arg(short = 'f', long = "format", value_enum, default_value_t = OutputFormat::Plain)]
        format: OutputFormat,
    },
    /// Output version of negronictl / patronictl and optionally cluster members
    Version {
        #[arg()]
        cluster_name: Option<String>,
    },
    /// Convert a primary cluster into a standby cluster
    DemoteCluster {
        #[arg()]
        cluster_name: Option<String>,
        #[arg(long = "dcs-model")]
        dcs_model: Option<String>,
        #[arg(long = "host")]
        host: Option<String>,
        #[arg(long = "port")]
        port: Option<u16>,
        #[arg(long = "force")]
        force: bool,
    },
    /// Promote a standby cluster into an independent primary cluster
    PromoteCluster {
        #[arg()]
        cluster_name: Option<String>,
        #[arg(long = "dcs-model")]
        dcs_model: Option<String>,
        #[arg(long = "force")]
        force: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedConfigSource {
    ConfigFile(String),
    DcsUrl(String),
}

/// Resolves the CLI configuration source following `docs/LLD.md` §7:
/// `-c/--config-file` -> `-d/--dcs-url` -> `$NEGRONICTL_CONFIG_FILE` ->
/// `$PATRONICTL_CONFIG_FILE` -> `$XDG_CONFIG_HOME/patroni/patronictl.yaml` -> `/etc/patroni/patroni.yml`.
///
/// # Examples
///
/// ```
/// use negroni_cli::{ResolvedConfigSource, resolve_config_source_with_lookup};
///
/// let src = resolve_config_source_with_lookup(
///     None,
///     None,
///     |k| (k == "PATRONICTL_CONFIG_FILE").then(|| "/custom/ctl.yaml".to_owned()),
///     |_| false,
/// );
/// assert_eq!(src, ResolvedConfigSource::ConfigFile("/custom/ctl.yaml".to_owned()));
/// ```
pub fn resolve_config_source_with_lookup<E, F>(
    cli_config: Option<&str>,
    cli_dcs_url: Option<&str>,
    env_lookup: E,
    file_exists: F,
) -> ResolvedConfigSource
where
    E: Fn(&str) -> Option<String>,
    F: Fn(&str) -> bool,
{
    if let Some(c) = cli_config.filter(|s| !s.trim().is_empty()) {
        return ResolvedConfigSource::ConfigFile(c.to_owned());
    }
    if let Some(d) = cli_dcs_url.filter(|s| !s.trim().is_empty()) {
        return ResolvedConfigSource::DcsUrl(d.to_owned());
    }
    if let Some(env_neg) = env_lookup("NEGRONICTL_CONFIG_FILE").filter(|s| !s.trim().is_empty()) {
        return ResolvedConfigSource::ConfigFile(env_neg);
    }
    if let Some(env_pat) = env_lookup("PATRONICTL_CONFIG_FILE").filter(|s| !s.trim().is_empty()) {
        return ResolvedConfigSource::ConfigFile(env_pat);
    }
    if let Some(dcs_env) = env_lookup("PATRONI_DCS_URL")
        .or_else(|| env_lookup("NEGRONI_DCS_URL"))
        .filter(|s| !s.trim().is_empty())
    {
        return ResolvedConfigSource::DcsUrl(dcs_env);
    }

    let xdg_candidate = if let Some(xdg) = env_lookup("XDG_CONFIG_HOME").filter(|s| !s.is_empty()) {
        Some(format!(
            "{}/patroni/patronictl.yaml",
            xdg.trim_end_matches('/')
        ))
    } else {
        env_lookup("HOME").filter(|s| !s.is_empty()).map(|h| {
            format!(
                "{}/.config/patroni/patronictl.yaml",
                h.trim_end_matches('/')
            )
        })
    };

    if let Some(candidate) = xdg_candidate
        && file_exists(&candidate)
    {
        return ResolvedConfigSource::ConfigFile(candidate);
    }

    ResolvedConfigSource::ConfigFile("/etc/patroni/patroni.yml".to_owned())
}

fn normalize_member_api_endpoint(api_url: &str, path: &str) -> String {
    let trimmed = api_url.trim().trim_end_matches('/');
    let base = trimmed.strip_suffix("/patroni").unwrap_or(trimmed);
    let clean_path = path.trim_start_matches('/');
    format!("{base}/{clean_path}")
}

#[derive(Clone, Debug)]
pub struct CliRestClient {
    http: reqwest::Client,
    auth: Option<BasicAuthCredentials>,
}

impl CliRestClient {
    pub fn build(daemon_cfg: Option<&DaemonConfig>, cli_insecure: bool) -> Result<Self> {
        let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(15));
        let insecure = cli_insecure || daemon_cfg.is_some_and(|c| c.ctl.insecure);
        if insecure {
            builder = builder.danger_accept_invalid_certs(true);
        }

        if let Some(cfg) = daemon_cfg {
            if let Some(ca_path) = cfg.ctl.cacert.as_ref().or(cfg.restapi.cacert.as_ref()) {
                let pem = fs::read(ca_path).with_context(|| {
                    format!("failed to read CTL CA certificate '{}'", ca_path.display())
                })?;
                let cert = reqwest::Certificate::from_pem(&pem)?;
                builder = builder.add_root_certificate(cert);
            }
            if let (Some(cert_p), Some(key_p)) = (&cfg.ctl.certfile, &cfg.ctl.keyfile) {
                let mut combined = fs::read(cert_p)?;
                combined.push(b'\n');
                combined.extend_from_slice(&fs::read(key_p)?);
                let id = reqwest::Identity::from_pem(&combined)?;
                builder = builder.identity(id);
            }
        }

        let auth = daemon_cfg.and_then(|c| {
            c.ctl
                .authentication
                .clone()
                .or_else(|| c.restapi.authentication.clone())
        });

        Ok(Self {
            http: builder.build()?,
            auth,
        })
    }

    fn apply_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(ref creds) = self.auth {
            req.basic_auth(creds.username.as_str(), Some(creds.password.expose()))
        } else {
            req
        }
    }

    pub async fn post_json<T: Serialize>(
        &self,
        api_url: &str,
        path: &str,
        body: &T,
    ) -> Result<String> {
        let url = normalize_member_api_endpoint(api_url, path);
        let resp = self
            .apply_auth(self.http.post(&url).json(body))
            .send()
            .await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("HTTP {status} from {url}: {text}");
        }
        Ok(text)
    }

    pub async fn post_empty(&self, api_url: &str, path: &str) -> Result<String> {
        let url = normalize_member_api_endpoint(api_url, path);
        let resp = self.apply_auth(self.http.post(&url)).send().await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!("HTTP {status} from {url}: {text}");
        }
        Ok(text)
    }

    pub async fn delete_path(&self, api_url: &str, path: &str) -> Result<(u16, String)> {
        let url = normalize_member_api_endpoint(api_url, path);
        let resp = self.apply_auth(self.http.delete(&url)).send().await?;
        let code = resp.status().as_u16();
        let text = resp.text().await.unwrap_or_default();
        Ok((code, text))
    }

    pub async fn get_json(&self, api_url: &str, path: &str) -> Result<JsonValue> {
        let url = normalize_member_api_endpoint(api_url, path);
        let resp = self.apply_auth(self.http.get(&url)).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            bail!("HTTP {status} from {url}: {text}");
        }
        Ok(resp.json().await?)
    }
}

fn prompt_yes_no(msg: &str) -> Result<bool> {
    print!("{msg}");
    io::stdout().flush()?;
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    Ok(matches!(
        line.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

fn prompt_exact(msg: &str) -> Result<String> {
    print!("{msg}");
    io::stdout().flush()?;
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    Ok(line.trim().to_owned())
}

async fn set_cluster_pause<D: DcsBackend>(dcs: &D, pause_state: bool, wait: bool) -> Result<()> {
    let snap = dcs.fetch_cluster(Duration::from_secs(5)).await?;
    let expected = snap.config.as_ref().map(|v| v.token.clone());
    let mut cfg_val = snap
        .config
        .map(|v| v.value)
        .unwrap_or_else(|| JsonValue::Object(JsonMap::new()));
    let mut patch = JsonMap::new();
    patch.insert("pause".to_owned(), JsonValue::Bool(pause_state));
    deep_merge_json(&mut cfg_val, &JsonValue::Object(patch));

    dcs.cas_config(&cfg_val, expected.as_ref())
        .await
        .context("failed to update 'pause' flag in DCS /config")?;

    if wait {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            let poll = dcs.fetch_cluster(Duration::from_secs(3)).await?;
            let all_observed = poll
                .members
                .values()
                .all(|m| m.value.pause.unwrap_or(false) == pause_state);
            if all_observed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    if pause_state {
        println!("Success: cluster management is paused");
    } else {
        println!("Success: cluster management is resumed");
    }
    Ok(())
}

#[derive(Debug, Serialize)]
struct RestartRpcBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    schedule: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    postgres_version: Option<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    restart_pending: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    timeout: Option<u64>,
}

#[derive(Debug, Serialize)]
struct ReinitRpcBody {
    force: bool,
}

/// Executes a parsed [`CliCommand`] against an initialized [`DcsBackend`] and [`CliRestClient`].
pub async fn execute_command_with_dcs<D: DcsBackend>(
    cmd: CliCommand,
    default_scope: &str,
    dcs: &D,
    rest: &CliRestClient,
    daemon_cfg: Option<&DaemonConfig>,
) -> Result<()> {
    match cmd {
        CliCommand::Version { cluster_name } => {
            let backends = negroni_dcs::compiled_backends().join(", ");
            println!(
                "negronictl version {} (Patroni 4.1.0 wire parity; backends: [{}])",
                env!("CARGO_PKG_VERSION"),
                backends
            );
            if cluster_name.is_some()
                && let Ok(snap) = dcs.fetch_cluster(Duration::from_secs(3)).await
            {
                for (name, m) in &snap.members {
                    println!("  {name}: Patroni {}", m.value.version);
                }
            }
            Ok(())
        }

        CliCommand::List {
            cluster_names,
            extended,
            timestamp,
            watch,
            format,
        } => {
            let scope = cluster_names.first().map_or(default_scope, String::as_str);
            loop {
                let snap = dcs.fetch_cluster(Duration::from_secs(5)).await?;
                print!(
                    "{}",
                    render_cluster_list(scope, &snap, format, extended, timestamp)
                );
                match watch {
                    Some(secs) => tokio::time::sleep(Duration::from_secs(secs.max(1))).await,
                    None => break,
                }
            }
            Ok(())
        }

        CliCommand::Topology {
            cluster_names,
            watch,
            format,
        } => {
            let scope = cluster_names.first().map_or(default_scope, String::as_str);
            loop {
                let snap = dcs.fetch_cluster(Duration::from_secs(5)).await?;
                print!("{}", render_topology(scope, &snap, format));
                match watch {
                    Some(secs) => tokio::time::sleep(Duration::from_secs(secs.max(1))).await,
                    None => break,
                }
            }
            Ok(())
        }

        CliCommand::Dsn {
            cluster_name: _,
            role,
            member,
        } => {
            let snap = dcs.fetch_cluster(Duration::from_secs(5)).await?;
            let member_vec: Vec<String> = member.into_iter().collect();
            let effective_role = role.as_deref().or(if member_vec.is_empty() {
                Some("primary")
            } else {
                None
            });
            let matches = select_target_members(&snap, effective_role, &member_vec);
            let Some((_, chosen)) = matches.first() else {
                bail!("Can not find a suitable member for requested role/member");
            };
            println!("{}", conn_url_to_libpq_dsn(&chosen.conn_url, None, None));
            Ok(())
        }

        CliCommand::Query {
            cluster_name: _,
            role,
            member,
            file,
            command,
            username,
            dbname,
            watch,
        } => {
            let q_args = QueryArgs {
                role,
                members: member,
                sql_file: file,
                command,
                username,
                dbname,
                watch_secs: watch.map(Some),
            };
            run_query(dcs, daemon_cfg, &q_args).await
        }

        CliCommand::ShowConfig { cluster_name: _ } => {
            let snap = dcs.fetch_cluster(Duration::from_secs(5)).await?;
            let cfg_json = snap
                .config
                .map(|v| v.value)
                .or_else(|| daemon_cfg.and_then(|c| serde_json::to_value(&c.dynamic).ok()))
                .unwrap_or_else(|| JsonValue::Object(JsonMap::new()));
            print!("{}", json_to_yaml_string(&cfg_json));
            Ok(())
        }

        CliCommand::EditConfig {
            cluster_name: _,
            set,
            pg,
            replace,
            apply,
            force,
            quiet,
        } => {
            let opts = EditConfigArgs {
                set_overrides: set,
                pg_overrides: pg,
                replace_file: replace,
                apply_file: apply,
                force,
                quiet,
            };
            run_edit_config(dcs, &opts).await
        }

        CliCommand::Switchover {
            cluster_name,
            leader,
            candidate,
            scheduled,
            force,
        } => {
            let scope = cluster_name.unwrap_or_else(|| default_scope.to_owned());
            run_switchover(
                dcs,
                rest,
                SwitchoverArgs {
                    scope,
                    leader,
                    candidate,
                    scheduled,
                    force,
                },
            )
            .await
        }

        CliCommand::Failover {
            cluster_name,
            leader,
            candidate,
            force,
        } => {
            let scope = cluster_name.unwrap_or_else(|| default_scope.to_owned());
            run_failover(
                dcs,
                rest,
                FailoverArgs {
                    scope,
                    leader,
                    candidate,
                    force,
                },
            )
            .await
        }

        CliCommand::Pause {
            cluster_name: _,
            wait,
        } => set_cluster_pause(dcs, true, wait).await,

        CliCommand::Resume {
            cluster_name: _,
            wait,
        } => set_cluster_pause(dcs, false, wait).await,

        CliCommand::Restart {
            cluster_name,
            member_names,
            role,
            pg_version,
            pending,
            scheduled,
            timeout,
            force,
        } => {
            let snap = dcs.fetch_cluster(Duration::from_secs(5)).await?;
            let targets = select_target_members(&snap, role.as_deref(), &member_names);
            if targets.is_empty() {
                bail!("No matching members found in cluster '{cluster_name}'");
            }
            if !force
                && !prompt_yes_no(&format!(
                    "Are you sure you want to restart members {:?} in cluster {cluster_name}? [y/N]: ",
                    targets.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>()
                ))?
            {
                println!("Aborted.");
                return Ok(());
            }

            let body = RestartRpcBody {
                schedule: scheduled,
                role,
                postgres_version: pg_version,
                restart_pending: pending,
                timeout,
            };
            for (name, member) in &targets {
                let msg = rest.post_json(&member.api_url, "/restart", &body).await?;
                println!("Success: restart on member {name}: {msg}");
            }
            Ok(())
        }

        CliCommand::Reload {
            cluster_name,
            member_names,
            role,
            force,
        } => {
            let snap = dcs.fetch_cluster(Duration::from_secs(5)).await?;
            let targets = select_target_members(&snap, role.as_deref(), &member_names);
            if targets.is_empty() {
                bail!("No matching members found in cluster '{cluster_name}'");
            }
            if !force
                && !prompt_yes_no(&format!(
                    "Are you sure you want to reload members {:?} in cluster {cluster_name}? [y/N]: ",
                    targets.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>()
                ))?
            {
                println!("Aborted.");
                return Ok(());
            }

            for (name, member) in &targets {
                let msg = rest.post_empty(&member.api_url, "/reload").await?;
                println!("Reload request received for member {name}: {msg}");
            }
            Ok(())
        }

        CliCommand::Reinit {
            cluster_name,
            member_names,
            force,
            wait,
        } => {
            let snap = dcs.fetch_cluster(Duration::from_secs(5)).await?;
            let targets = select_target_members(&snap, None, &member_names);
            if targets.is_empty() {
                bail!("No matching members found in cluster '{cluster_name}'");
            }
            if !force
                && !prompt_yes_no(&format!(
                    "Are you sure you want to reinitialize members {:?} in cluster {cluster_name}? [y/N]: ",
                    targets.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>()
                ))?
            {
                println!("Aborted.");
                return Ok(());
            }

            for (name, member) in &targets {
                let msg = rest
                    .post_json(&member.api_url, "/reinitialize", &ReinitRpcBody { force })
                    .await?;
                println!("Success: reinitialize for member {name}: {msg}");
            }

            if wait {
                let deadline = Instant::now() + Duration::from_secs(120);
                while Instant::now() < deadline {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    let poll: ClusterSnapshot = dcs.fetch_cluster(Duration::from_secs(3)).await?;
                    let all_running = targets.iter().all(|(name, _)| {
                        poll.members
                            .get(name)
                            .is_some_and(|m| m.value.state == "running")
                    });
                    if all_running {
                        break;
                    }
                }
            }
            Ok(())
        }

        CliCommand::Flush {
            cluster_name,
            target,
            member_names,
            role,
            force,
        } => match target {
            FlushTarget::Switchover => {
                let snap = dcs.fetch_cluster(Duration::from_secs(5)).await?;
                let Some(fo) = snap.failover else {
                    println!("No switchover is scheduled for cluster '{cluster_name}'");
                    return Ok(());
                };
                if fo.value.scheduled_at.is_none() && !force {
                    bail!(
                        "No scheduled switchover found (use --force to clear active failover key)"
                    );
                }
                dcs.clear_failover(&fo.token).await?;
                println!("Success: flushed scheduled switchover");
                Ok(())
            }
            FlushTarget::Restart => {
                let snap = dcs.fetch_cluster(Duration::from_secs(5)).await?;
                let targets = select_target_members(&snap, role.as_deref(), &member_names);
                let mut flushed_any = false;
                for (name, member) in &targets {
                    let (code, _body) = rest.delete_path(&member.api_url, "/restart").await?;
                    if (200..300).contains(&code) {
                        println!("Success: flush scheduled restart on member {name}");
                        flushed_any = true;
                    } else if code != 404 {
                        bail!("Unexpected HTTP {code} flushing restart on member {name}");
                    }
                }
                if !flushed_any {
                    println!("No scheduled restart found");
                }
                Ok(())
            }
        },

        CliCommand::Remove { cluster_name } => {
            let typed_cluster = prompt_exact("Please confirm the cluster name to remove: ")?;
            if typed_cluster != cluster_name {
                bail!(
                    "Cluster names specified do not match ('{typed_cluster}' != '{cluster_name}')"
                );
            }
            let ack = prompt_exact(&format!(
                "You are about to remove all information in DCS for {cluster_name}, please type: \"Yes I am aware\": "
            ))?;
            if ack != "Yes I am aware" {
                bail!("You did not exactly type \"Yes I am aware\"; aborting remove");
            }
            remove_cluster_from_dcs(dcs).await?;
            println!("Removed cluster '{cluster_name}' from DCS");
            Ok(())
        }

        CliCommand::History {
            cluster_name: _,
            format,
        } => {
            let snap = dcs.fetch_cluster(Duration::from_secs(5)).await?;
            print!("{}", render_history(&snap.history, format));
            Ok(())
        }

        CliCommand::DemoteCluster {
            cluster_name,
            dcs_model: _,
            host,
            port,
            force,
        } => {
            let scope = cluster_name.as_deref().unwrap_or(default_scope);
            if !force
                && !prompt_yes_no(&format!(
                    "Are you sure you want to demote cluster '{scope}' into a standby cluster? [y/N]: "
                ))?
            {
                println!("Aborted.");
                return Ok(());
            }
            let snap = dcs.fetch_cluster(Duration::from_secs(5)).await?;
            let expected = snap.config.as_ref().map(|v| v.token.clone());
            let mut cfg_val = snap
                .config
                .map(|v| v.value)
                .unwrap_or_else(|| JsonValue::Object(JsonMap::new()));

            let mut sc = JsonMap::new();
            sc.insert(
                "host".to_owned(),
                JsonValue::String(host.unwrap_or_else(|| "127.0.0.1".to_owned())),
            );
            sc.insert(
                "port".to_owned(),
                JsonValue::Number((port.unwrap_or(5432)).into()),
            );
            sc.insert(
                "create_replica_methods".to_owned(),
                JsonValue::Array(vec![JsonValue::String("basebackup".to_owned())]),
            );
            let mut patch = JsonMap::new();
            patch.insert("standby_cluster".to_owned(), JsonValue::Object(sc));
            deep_merge_json(&mut cfg_val, &JsonValue::Object(patch));

            dcs.cas_config(&cfg_val, expected.as_ref()).await?;
            println!("Cluster '{scope}' configured as standby_cluster in DCS");
            Ok(())
        }

        CliCommand::PromoteCluster {
            cluster_name,
            dcs_model: _,
            force,
        } => {
            let scope = cluster_name.as_deref().unwrap_or(default_scope);
            if !force
                && !prompt_yes_no(&format!(
                    "Are you sure you want to promote standby cluster '{scope}'? [y/N]: "
                ))?
            {
                println!("Aborted.");
                return Ok(());
            }
            let snap = dcs.fetch_cluster(Duration::from_secs(5)).await?;
            let expected = snap.config.as_ref().map(|v| v.token.clone());
            let mut cfg_val = snap
                .config
                .map(|v| v.value)
                .unwrap_or_else(|| JsonValue::Object(JsonMap::new()));

            let mut patch = JsonMap::new();
            patch.insert("standby_cluster".to_owned(), JsonValue::Null);
            deep_merge_json(&mut cfg_val, &JsonValue::Object(patch));

            dcs.cas_config(&cfg_val, expected.as_ref()).await?;
            println!("Standby cluster '{scope}' promoted to primary cluster in DCS");
            Ok(())
        }
    }
}

pub async fn remove_cluster_from_dcs<D: DcsBackend>(dcs: &D) -> Result<()> {
    dcs.delete_cluster()
        .await
        .context("failed to delete cluster prefix from DCS")
}

fn extract_explicit_scope(cmd: &CliCommand) -> Option<&str> {
    match cmd {
        CliCommand::List { cluster_names, .. } | CliCommand::Topology { cluster_names, .. } => {
            cluster_names.first().map(String::as_str)
        }
        CliCommand::Dsn { cluster_name, .. }
        | CliCommand::Query { cluster_name, .. }
        | CliCommand::ShowConfig { cluster_name }
        | CliCommand::EditConfig { cluster_name, .. }
        | CliCommand::Switchover { cluster_name, .. }
        | CliCommand::Failover { cluster_name, .. }
        | CliCommand::Pause { cluster_name, .. }
        | CliCommand::Resume { cluster_name, .. }
        | CliCommand::History { cluster_name, .. }
        | CliCommand::Version { cluster_name }
        | CliCommand::DemoteCluster { cluster_name, .. }
        | CliCommand::PromoteCluster { cluster_name, .. } => cluster_name.as_deref(),
        CliCommand::Restart { cluster_name, .. }
        | CliCommand::Reload { cluster_name, .. }
        | CliCommand::Reinit { cluster_name, .. }
        | CliCommand::Flush { cluster_name, .. }
        | CliCommand::Remove { cluster_name } => Some(cluster_name.as_str()),
    }
}

async fn run_cli_async(parsed: CliApp) -> Result<()> {
    if let CliCommand::Version { cluster_name: None } = parsed.command {
        let backends = negroni_dcs::compiled_backends().join(", ");
        println!(
            "negronictl version {} (Patroni 4.1.0 wire parity; backends: [{}])",
            env!("CARGO_PKG_VERSION"),
            backends
        );
        return Ok(());
    }

    let source = resolve_config_source_with_lookup(
        parsed.config_file.as_deref(),
        parsed.dcs_url.as_deref(),
        |k| env::var(k).ok(),
        |p| std::path::Path::new(p).exists(),
    );

    let explicit_scope = extract_explicit_scope(&parsed.command).map(str::to_owned);

    let (dcs, default_scope, daemon_cfg) = match source {
        ResolvedConfigSource::DcsUrl(url) => {
            let scope = explicit_scope
                .clone()
                .unwrap_or_else(|| "postgres".to_owned());
            let ns = env::var("NEGRONI_NAMESPACE")
                .or_else(|_| env::var("PATRONI_NAMESPACE"))
                .unwrap_or_else(|_| "/service".to_owned());
            let dcs = AnyDcs::from_dcs_url(&url, &ns, &scope)
                .await
                .with_context(|| format!("failed to connect to DCS URL '{url}'"))?;
            (dcs, scope, None)
        }
        ResolvedConfigSource::ConfigFile(path) => {
            let cfg = negroni_config::load_and_validate(&path)
                .with_context(|| format!("failed to load configuration '{path}'"))?;
            let scope = explicit_scope
                .clone()
                .unwrap_or_else(|| cfg.scope.to_string());
            let dcs = AnyDcs::from_daemon_config(&cfg, Some(&scope))
                .await
                .context("failed to connect to configured DCS backend")?;
            (dcs, scope, Some(cfg))
        }
    };

    let rest = CliRestClient::build(daemon_cfg.as_ref(), parsed.insecure)?;
    execute_command_with_dcs(
        parsed.command,
        &default_scope,
        &dcs,
        &rest,
        daemon_cfg.as_ref(),
    )
    .await
}

pub fn run_cli(args: Vec<String>) -> ExitCode {
    let parsed = match CliApp::try_parse_from(args) {
        Ok(app) => app,
        Err(err) => {
            let code = if err.use_stderr() {
                ExitCode::FAILURE
            } else {
                ExitCode::SUCCESS
            };
            let _ = err.print();
            return code;
        }
    };

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("Failed to initialize Tokio runtime: {err}");
            return ExitCode::FAILURE;
        }
    };

    match rt.block_on(run_cli_async(parsed)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err:#}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::collections::BTreeMap;

    use compact_str::CompactString;
    use negroni_dcs::InMemoryDcs;
    use negroni_types::{HistoryEntry, MemberData, WalLsn};

    use super::*;

    #[test]
    fn parses_all_19_patronictl_subcommands() {
        let cases: &[&[&str]] = &[
            &[
                "patronictl",
                "-c",
                "/etc/patroni.yml",
                "list",
                "batman",
                "-e",
                "-f",
                "json",
            ],
            &["patronictl", "topology", "batman"],
            &["patronictl", "dsn", "batman", "-r", "replica"],
            &[
                "patronictl",
                "-c",
                "/etc/patroni.yml",
                "query",
                "batman",
                "-c",
                "SELECT 1",
            ],
            &["patronictl", "show-config", "batman"],
            &[
                "patronictl",
                "edit-config",
                "batman",
                "-s",
                "ttl=45",
                "--force",
            ],
            &[
                "patronictl",
                "switchover",
                "batman",
                "--candidate",
                "pg-1",
                "--force",
            ],
            &[
                "patronictl",
                "failover",
                "batman",
                "--candidate",
                "pg-1",
                "--force",
            ],
            &["patronictl", "pause", "batman", "--wait"],
            &["patronictl", "resume", "batman"],
            &[
                "patronictl",
                "restart",
                "batman",
                "pg-0",
                "--pending",
                "--force",
            ],
            &[
                "patronictl",
                "reload",
                "batman",
                "--role",
                "replica",
                "--force",
            ],
            &[
                "patronictl",
                "reinit",
                "batman",
                "pg-1",
                "--force",
                "--wait",
            ],
            &["patronictl", "flush", "batman", "switchover", "--force"],
            &["patronictl", "remove", "batman"],
            &["patronictl", "history", "batman", "-f", "tsv"],
            &["patronictl", "version"],
            &[
                "patronictl",
                "demote-cluster",
                "batman",
                "--host",
                "10.0.0.9",
                "--force",
            ],
            &["patronictl", "promote-cluster", "batman", "--force"],
        ];

        assert_eq!(cases.len(), 19);
        for argv in cases {
            CliApp::try_parse_from(*argv)
                .unwrap_or_else(|e| panic!("failed to parse {argv:?}: {e}"));
        }
    }

    #[test]
    fn config_resolution_order_follows_lld_precedence() {
        // 1. -c wins over -d and env
        assert_eq!(
            resolve_config_source_with_lookup(
                Some("/cli/a.yml"),
                Some("etcd3://127.0.0.1:2379"),
                |_| Some("/env/b.yml".to_owned()),
                |_| true,
            ),
            ResolvedConfigSource::ConfigFile("/cli/a.yml".to_owned())
        );

        // 2. -d wins over env
        assert_eq!(
            resolve_config_source_with_lookup(
                None,
                Some("etcd3://127.0.0.1:2379"),
                |_| Some("/env/b.yml".to_owned()),
                |_| true,
            ),
            ResolvedConfigSource::DcsUrl("etcd3://127.0.0.1:2379".to_owned())
        );

        // 3. $NEGRONICTL_CONFIG_FILE wins over $PATRONICTL_CONFIG_FILE
        assert_eq!(
            resolve_config_source_with_lookup(
                None,
                None,
                |k| match k {
                    "NEGRONICTL_CONFIG_FILE" => Some("/env/negroni.yml".to_owned()),
                    "PATRONICTL_CONFIG_FILE" => Some("/env/patroni.yml".to_owned()),
                    _ => None,
                },
                |_| true,
            ),
            ResolvedConfigSource::ConfigFile("/env/negroni.yml".to_owned())
        );

        // 4. $XDG_CONFIG_HOME/patroni/patronictl.yaml when file exists, else /etc/patroni/patroni.yml
        assert_eq!(
            resolve_config_source_with_lookup(
                None,
                None,
                |k| (k == "XDG_CONFIG_HOME").then(|| "/home/jcf/.config".to_owned()),
                |p| p == "/home/jcf/.config/patroni/patronictl.yaml",
            ),
            ResolvedConfigSource::ConfigFile(
                "/home/jcf/.config/patroni/patronictl.yaml".to_owned()
            )
        );
        assert_eq!(
            resolve_config_source_with_lookup(None, None, |_| None, |_| false),
            ResolvedConfigSource::ConfigFile("/etc/patroni/patroni.yml".to_owned())
        );
    }

    #[tokio::test]
    async fn cli_commands_execute_end_to_end_against_dcs() {
        let dcs = InMemoryDcs::new();
        let lease = dcs
            .ensure_lease(Duration::from_secs(30), None)
            .await
            .unwrap();
        let _ = dcs.cas_initialize("7400000000000000042").await.unwrap();
        let _ = dcs
            .acquire_or_renew_leader("pg-0", &lease, None)
            .await
            .unwrap();
        dcs.upsert_member(
            "pg-0",
            &MemberData {
                conn_url: "postgres://10.0.0.1:5432/postgres".to_owned(),
                api_url: "http://10.0.0.1:8008/patroni".to_owned(),
                state: CompactString::const_new("running"),
                role: CompactString::const_new("master"),
                version: CompactString::const_new("4.1.0"),
                xlog_location: Some(WalLsn(64 * 1024 * 1024)),
                timeline: Some(2),
                replication_state: None,
                pending_restart: false,
                pending_restart_reason: BTreeMap::new(),
                pause: None,
                watchdog_failed: None,
                tags: BTreeMap::new(),
                extra: BTreeMap::new(),
            },
            &lease,
        )
        .await
        .unwrap();

        let mut rep_tags = BTreeMap::new();
        rep_tags.insert(
            CompactString::const_new("replicatefrom"),
            JsonValue::String("pg-0".to_owned()),
        );
        dcs.upsert_member(
            "pg-1",
            &MemberData {
                conn_url: "postgres://10.0.0.2:5432/postgres".to_owned(),
                api_url: "http://10.0.0.2:8008/patroni".to_owned(),
                state: CompactString::const_new("running"),
                role: CompactString::const_new("replica"),
                version: CompactString::const_new("4.1.0"),
                xlog_location: Some(WalLsn(60 * 1024 * 1024)),
                timeline: Some(2),
                replication_state: Some(CompactString::const_new("streaming")),
                pending_restart: true,
                pending_restart_reason: BTreeMap::new(),
                pause: None,
                watchdog_failed: None,
                tags: rep_tags,
                extra: BTreeMap::new(),
            },
            &lease,
        )
        .await
        .unwrap();
        dcs.append_history_entry(&HistoryEntry {
            timeline: 2,
            lsn: WalLsn(32 * 1024 * 1024),
            reason: "no recovery target specified".to_owned(),
            timestamp: Some("2026-11-12T20:00:00Z".to_owned()),
            leader: Some(compact_str::CompactString::const_new("pg-0")),
        })
        .await
        .unwrap();

        let rest = CliRestClient::build(None, false).unwrap();

        // edit-config -s ttl=45 -p max_connections=250 --force -q
        execute_command_with_dcs(
            CliCommand::EditConfig {
                cluster_name: Some("batman".to_owned()),
                set: vec!["ttl=45".to_owned()],
                pg: vec!["max_connections=250".to_owned()],
                replace: None,
                apply: None,
                force: true,
                quiet: true,
            },
            "batman",
            &dcs,
            &rest,
            None,
        )
        .await
        .unwrap();

        let snap = dcs.fetch_cluster(Duration::from_secs(1)).await.unwrap();
        assert_eq!(snap.config.as_ref().unwrap().value["ttl"], 45);
        assert_eq!(
            snap.config.as_ref().unwrap().value["postgresql"]["parameters"]["max_connections"],
            250
        );

        // Verify topology & history rendering
        let topo = render_topology("batman", &snap, OutputFormat::Plain);
        assert!(topo.contains("pg-0"));
        assert!(topo.contains("+ pg-1"));

        let hist = render_history(&snap.history, OutputFormat::Plain);
        assert!(hist.contains("no recovery target specified"));

        // pause -> demote-cluster -> promote-cluster -> resume -> remove_cluster_from_dcs
        execute_command_with_dcs(
            CliCommand::Pause {
                cluster_name: Some("batman".to_owned()),
                wait: false,
            },
            "batman",
            &dcs,
            &rest,
            None,
        )
        .await
        .unwrap();
        execute_command_with_dcs(
            CliCommand::DemoteCluster {
                cluster_name: Some("batman".to_owned()),
                dcs_model: None,
                host: Some("10.9.0.1".to_owned()),
                port: Some(5432),
                force: true,
            },
            "batman",
            &dcs,
            &rest,
            None,
        )
        .await
        .unwrap();
        let demoted = dcs.fetch_cluster(Duration::from_secs(1)).await.unwrap();
        assert_eq!(
            demoted.config.as_ref().unwrap().value["standby_cluster"]["host"],
            "10.9.0.1"
        );

        execute_command_with_dcs(
            CliCommand::PromoteCluster {
                cluster_name: Some("batman".to_owned()),
                dcs_model: None,
                force: true,
            },
            "batman",
            &dcs,
            &rest,
            None,
        )
        .await
        .unwrap();
        let promoted = dcs.fetch_cluster(Duration::from_secs(1)).await.unwrap();
        assert!(
            promoted
                .config
                .as_ref()
                .unwrap()
                .value
                .get("standby_cluster")
                .is_none()
        );

        remove_cluster_from_dcs(&dcs).await.unwrap();
        let wiped = dcs.fetch_cluster(Duration::from_secs(1)).await.unwrap();
        assert!(wiped.members.is_empty());
        assert!(wiped.leader.is_none());
    }
}
