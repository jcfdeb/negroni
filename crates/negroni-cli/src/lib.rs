#![forbid(unsafe_code)]
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::process::ExitCode;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "negronictl",
    version,
    about = "Negroni / Patroni HA cluster control CLI"
)]
pub struct CliApp {
    /// Path to patroni.yml configuration file
    #[arg(short = 'c', long = "config-file", global = true)]
    pub config_file: Option<String>,

    /// Direct DCS endpoint URL (e.g., etcd3://127.0.0.1:2379)
    #[arg(short = 'd', long = "dcs-url", global = true)]
    pub dcs_url: Option<String>,

    #[command(subcommand)]
    pub command: CliCommand,
}

#[derive(Debug, PartialEq, Eq, Subcommand)]
pub enum CliCommand {
    /// List cluster members and replication lag
    List {
        #[arg(default_value = "postgres")]
        cluster_name: String,
    },
    /// Render ASCII cascading replication topology
    Topology {
        #[arg(default_value = "postgres")]
        cluster_name: String,
    },
    /// Print dynamic cluster configuration from DCS
    ShowConfig {
        #[arg(default_value = "postgres")]
        cluster_name: String,
    },
    /// Print client version and compiled DCS backends
    Version,
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

    match parsed.command {
        CliCommand::Version => {
            let backends = negroni_dcs::compiled_backends().join(", ");
            println!(
                "negronictl {} (backends: [{}])",
                env!("CARGO_PKG_VERSION"),
                backends
            );
            ExitCode::SUCCESS
        }
        CliCommand::List { cluster_name }
        | CliCommand::Topology { cluster_name }
        | CliCommand::ShowConfig { cluster_name } => {
            eprintln!(
                "cluster '{cluster_name}': DCS client transport scheduled for Phase 3 implementation"
            );
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn parses_patronictl_symlink_invocation() {
        let app = CliApp::try_parse_from([
            "patronictl",
            "-c",
            "/etc/patroni/patroni.yml",
            "list",
            "prod-cluster",
        ])
        .unwrap();
        assert_eq!(app.config_file.as_deref(), Some("/etc/patroni/patroni.yml"));
        assert_eq!(
            app.command,
            CliCommand::List {
                cluster_name: "prod-cluster".to_owned(),
            }
        );
    }
}
