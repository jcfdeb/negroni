use std::env;
use std::path::Path;
use std::process::ExitCode;

#[derive(Debug, PartialEq, Eq)]
enum ExecutionMode {
    Daemon { config_path: String },
    Cli { args: Vec<String> },
    Version,
    Help,
}

fn detect_mode_from_args(mut args: Vec<String>, default_cfg_from_env: Option<String>) -> ExecutionMode {
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

fn main() -> ExitCode {
    let env_cfg = env::var("NEGRONI_CONFIGURATION")
        .ok()
        .or_else(|| env::var("PATRONI_CONFIGURATION").ok());

    match detect_mode_from_args(env::args().collect(), env_cfg) {
        ExecutionMode::Version => {
            let backends = negroni_dcs::compiled_backends().join(", ");
            println!("negroni {} (backends: [{}])", env!("CARGO_PKG_VERSION"), backends);
            ExitCode::SUCCESS
        }
        ExecutionMode::Help => {
            println!("Usage:\n  negroni <patroni.yml>\n  negroni ctl <subcommand> [args...]");
            ExitCode::SUCCESS
        }
        ExecutionMode::Cli { args } => negroni_cli::run_cli(args),
        ExecutionMode::Daemon { config_path } => {
            init_tracing();

            let cfg = match negroni_config::load_and_validate(&config_path) {
                Ok(c) => c,
                Err(err) => {
                    eprintln!("fatal configuration error: {err}");
                    return ExitCode::FAILURE;
                }
            };

            let watchdog = match negroni_watchdog::spawn_watchdog_thread(&cfg.watchdog) {
                Ok(w) => w,
                Err(err) => {
                    eprintln!("fatal watchdog initialization error: {err}");
                    return ExitCode::FAILURE;
                }
            };

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
                    eprintln!("failed to pre-allocate Tokio runtime: {err}");
                    watchdog.shutdown();
                    return ExitCode::FAILURE;
                }
            };

            runtime.block_on(negroni_ha::run_daemon(cfg, watchdog))
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
        let args = vec![
            "negroni".to_owned(),
            "ctl".to_owned(),
            "version".to_owned(),
        ];
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
