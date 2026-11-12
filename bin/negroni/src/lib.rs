#![forbid(unsafe_code)]
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::env;
use std::path::Path;
use std::process::ExitCode;

use anyhow::Context;

/// Top-level execution mode resolved from `argv[0]` symlink stem or subcommand.
///
/// # Examples
///
/// ```
/// use negroni::{ExecutionMode, detect_mode_from_args};
///
/// let mode = detect_mode_from_args(
///     vec!["/usr/bin/patronictl".to_owned(), "list".to_owned()],
///     None,
/// );
/// assert_eq!(
///     mode,
///     ExecutionMode::Cli {
///         args: vec!["/usr/bin/patronictl".to_owned(), "list".to_owned()],
///     }
/// );
/// ```
#[derive(Debug, PartialEq, Eq)]
pub enum ExecutionMode {
    Daemon { config_path: String },
    Cli { args: Vec<String> },
    Version,
    Help,
}

pub fn detect_mode_from_args(
    mut args: Vec<String>,
    default_cfg_from_env: Option<String>,
) -> ExecutionMode {
    let bin_stem = args
        .first()
        .and_then(|a| Path::new(a).file_stem())
        .and_then(|s| s.to_str())
        .unwrap_or("negroni");

    if matches!(bin_stem, "negronictl" | "patronictl") {
        return ExecutionMode::Cli { args };
    }

    match args.get(1).map(String::as_str) {
        Some("ctl") => {
            args.remove(1);
            ExecutionMode::Cli { args }
        }
        Some("-V" | "--version") => ExecutionMode::Version,
        Some("-h" | "--help") => ExecutionMode::Help,
        Some(path) => ExecutionMode::Daemon {
            config_path: path.to_owned(),
        },
        None => ExecutionMode::Daemon {
            config_path: default_cfg_from_env
                .unwrap_or_else(|| "/etc/patroni/patroni.yml".to_owned()),
        },
    }
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

fn boot_daemon(config_path: &str) -> anyhow::Result<ExitCode> {
    let cfg = negroni_config::load_and_validate(config_path)
        .with_context(|| format!("fatal configuration error loading '{config_path}'"))?;

    let watchdog = negroni_watchdog::spawn_watchdog_thread(&cfg.watchdog)
        .context("fatal hardware watchdog thread initialization error")?;

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .worker_threads(cfg.runtime.worker_threads.get())
        .max_blocking_threads(cfg.runtime.max_blocking_threads.get())
        .thread_stack_size(cfg.runtime.thread_stack_bytes)
        .thread_name("negroni-wrk")
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            watchdog.shutdown();
            return Err(anyhow::Error::new(err).context("failed to pre-allocate Tokio runtime"));
        }
    };

    Ok(runtime.block_on(negroni_ha::run_daemon(cfg, watchdog)))
}

pub fn run_entrypoint(args: Vec<String>) -> ExitCode {
    let env_cfg = env::var("NEGRONI_CONFIGURATION")
        .ok()
        .or_else(|| env::var("PATRONI_CONFIGURATION").ok());

    match detect_mode_from_args(args, env_cfg) {
        ExecutionMode::Version => {
            let backends = negroni_dcs::compiled_backends().join(", ");
            println!(
                "negroni {} (backends: [{}], citus: {})",
                env!("CARGO_PKG_VERSION"),
                backends,
                negroni_pg::citus_enabled(),
            );
            ExitCode::SUCCESS
        }
        ExecutionMode::Help => {
            println!("Usage:\n  negroni <patroni.yml>\n  negroni ctl <subcommand> [args...]");
            ExitCode::SUCCESS
        }
        ExecutionMode::Cli { args } => negroni_cli::run_cli(args),
        ExecutionMode::Daemon { config_path } => {
            init_tracing();
            match boot_daemon(&config_path) {
                Ok(code) => code,
                Err(err) => {
                    eprintln!("{err:#}");
                    ExitCode::FAILURE
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn routes_patronictl_symlink_to_cli_mode() {
        let args = vec!["/usr/bin/patronictl".to_owned(), "list".to_owned()];
        assert_eq!(
            detect_mode_from_args(args.clone(), None),
            ExecutionMode::Cli { args }
        );
    }

    #[test]
    fn routes_ctl_subcommand_to_cli_mode() {
        let args = vec!["negroni".to_owned(), "ctl".to_owned(), "version".to_owned()];
        assert_eq!(
            detect_mode_from_args(args, None),
            ExecutionMode::Cli {
                args: vec!["negroni".to_owned(), "version".to_owned()],
            }
        );
    }

    #[test]
    fn routes_yaml_argument_to_daemon_mode() {
        let args = vec!["negroni".to_owned(), "postgres0.yml".to_owned()];
        assert_eq!(
            detect_mode_from_args(args, None),
            ExecutionMode::Daemon {
                config_path: "postgres0.yml".to_owned(),
            }
        );
    }
}
