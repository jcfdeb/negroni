#![forbid(unsafe_code)]

pub mod env_overlay;
pub mod schema;
pub mod timing;

use std::env;
use std::fs;
use std::time::Duration;

use compact_str::CompactString;
use saphyr::{LoadableYamlNode, Scalar, Yaml};
use serde_json::{Map as JsonMap, Value as JsonValue};
use thiserror::Error;

pub use crate::env_overlay::apply_dual_prefix_env_overlay;
pub use crate::schema::{
    BasicAuthCredentials, BootstrapSection, CitusConfig, ConsulConfig, CtlConfig, DaemonConfig,
    DcsBackendConfigs, DynamicClusterConfig, DynamicPgSection, Etcd3Config, KubeConfig, NodeTags,
    PermanentSlotConfig, PgAuthSection, PgRoleCredentials, PostgresConfig, RaftConfig,
    RestApiConfig, RuntimeConfig, SecretString, StandbyClusterConfig, SyncMode, WatchdogConfig,
    ZookeeperConfig,
};
pub use crate::timing::{ValidatedTimings, WatchdogMode, validate_timings};

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read configuration file '{path}': {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse YAML syntax: {0}")]
    YamlScan(#[from] saphyr::ScanError),
    #[error("YAML configuration contains a non-finite floating-point value")]
    NonFiniteFloat,
    #[error("YAML mapping keys must be scalar strings, booleans, or integers")]
    NonScalarKey,
    #[error("unsupported or malformed YAML node in configuration")]
    UnsupportedYamlNode,
    #[error("failed to decode merged configuration schema: {0}")]
    Schema(#[from] serde_json::Error),
    #[error(
        "invalid watchdog safety_margin ({0}); must be -1 (half-TTL) or >= 1 second so watchdog fires strictly before ttl"
    )]
    InvalidSafetyMargin(i64),
    #[error(
        "invalid HA timings: loop_wait ({loop_wait:?}) + 2 * retry_timeout ({retry_timeout:?}) > ttl ({ttl:?})"
    )]
    LeaseRuleViolated {
        ttl: Duration,
        loop_wait: Duration,
        retry_timeout: Duration,
    },
    #[error(
        "watchdog timeout ({wd_timeout:?}) leaves < 2s demotion budget after loop_wait ({loop_wait:?}) + retry_timeout ({retry_timeout:?})"
    )]
    InsufficientDemotionBudget {
        wd_timeout: Duration,
        loop_wait: Duration,
        retry_timeout: Duration,
    },
}

fn scalar_key_to_string(node: &Yaml<'_>) -> Result<String, ConfigError> {
    match node {
        Yaml::Value(Scalar::String(s)) | Yaml::Representation(s, _, _) => Ok(s.to_string()),
        Yaml::Value(Scalar::Integer(i)) => Ok(i.to_string()),
        Yaml::Value(Scalar::Boolean(b)) => Ok(b.to_string()),
        Yaml::Tagged(_, inner) => scalar_key_to_string(inner),
        _ => Err(ConfigError::NonScalarKey),
    }
}

/// Parses a YAML 1.2 document via pure-Rust `saphyr` into a `serde_json::Value` tree.
pub fn yaml_to_json_value(yaml_str: &str) -> Result<JsonValue, ConfigError> {
    let docs = Yaml::load_from_str(yaml_str)?;
    match docs.first() {
        None | Some(Yaml::Value(Scalar::Null)) => Ok(JsonValue::Object(JsonMap::new())),
        Some(root) => convert_yaml_node(root),
    }
}

fn convert_yaml_node(node: &Yaml<'_>) -> Result<JsonValue, ConfigError> {
    match node {
        Yaml::Value(Scalar::Null) => Ok(JsonValue::Null),
        Yaml::Value(Scalar::Boolean(b)) => Ok(JsonValue::Bool(*b)),
        Yaml::Value(Scalar::Integer(i)) => Ok(JsonValue::Number((*i).into())),
        Yaml::Value(Scalar::FloatingPoint(f)) => serde_json::Number::from_f64(f.0)
            .map(JsonValue::Number)
            .ok_or(ConfigError::NonFiniteFloat),
        Yaml::Value(Scalar::String(s)) | Yaml::Representation(s, _, _) => {
            Ok(JsonValue::String(s.to_string()))
        }
        Yaml::Sequence(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(convert_yaml_node(item)?);
            }
            Ok(JsonValue::Array(out))
        }
        Yaml::Mapping(entries) => {
            let mut map = JsonMap::with_capacity(entries.len());
            for (k, v) in entries {
                map.insert(scalar_key_to_string(k)?, convert_yaml_node(v)?);
            }
            Ok(JsonValue::Object(map))
        }
        Yaml::Tagged(_, inner) => convert_yaml_node(inner),
        Yaml::Alias(_) | Yaml::BadValue => Err(ConfigError::UnsupportedYamlNode),
    }
}

/// Recursively applies RFC 7396 JSON Merge Patch semantics (`patch` over `target`),
/// where `null` object values delete keys in `target` (matching Patroni `PATCH /config`).
pub fn deep_merge_json(target: &mut JsonValue, patch: &JsonValue) {
    match (target, patch) {
        (JsonValue::Object(dst), JsonValue::Object(src)) => {
            for (k, v) in src {
                if v.is_null() {
                    dst.remove(k);
                } else {
                    let entry = dst.entry(k.clone()).or_insert(JsonValue::Null);
                    deep_merge_json(entry, v);
                }
            }
        }
        (dst, src) => {
            *dst = src.clone();
        }
    }
}

pub fn load_and_validate(config_path: &str) -> Result<DaemonConfig, ConfigError> {
    let content = fs::read_to_string(config_path).map_err(|source| ConfigError::Io {
        path: config_path.to_owned(),
        source,
    })?;
    parse_and_validate_with_env(&content, config_path, env::vars())
}

pub fn parse_and_validate_str(
    yaml_str: &str,
    config_path: &str,
) -> Result<DaemonConfig, ConfigError> {
    parse_and_validate_with_env(yaml_str, config_path, env::vars())
}

pub fn parse_and_validate_with_env<I, K, V>(
    yaml_str: &str,
    config_path: &str,
    vars: I,
) -> Result<DaemonConfig, ConfigError>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    let mut json_tree = yaml_to_json_value(yaml_str)?;
    apply_dual_prefix_env_overlay(&mut json_tree, vars)?;

    let raw: schema::RawPatroniDocument = serde_json::from_value(json_tree)?;

    let scope = raw
        .scope
        .unwrap_or_else(|| CompactString::const_new("postgres"));
    let name = raw
        .name
        .unwrap_or_else(|| CompactString::const_new("node0"));
    let namespace = raw
        .namespace
        .unwrap_or_else(|| CompactString::const_new("/service"));

    let wd_mode = raw.watchdog.mode.unwrap_or_default();
    let safety_margin = raw.watchdog.safety_margin.unwrap_or(5);
    let dynamic = raw.bootstrap.dcs;

    let timings = validate_timings(
        dynamic.ttl,
        dynamic.loop_wait,
        dynamic.retry_timeout,
        wd_mode,
        safety_margin,
    )?;

    let defaults = RuntimeConfig::default();
    let runtime = RuntimeConfig {
        worker_threads: raw
            .runtime
            .worker_threads
            .unwrap_or(defaults.worker_threads),
        max_blocking_threads: raw
            .runtime
            .max_blocking_threads
            .unwrap_or(defaults.max_blocking_threads),
        thread_stack_bytes: raw
            .runtime
            .thread_stack_bytes
            .unwrap_or(defaults.thread_stack_bytes),
    };

    Ok(DaemonConfig {
        scope,
        name,
        namespace,
        config_path: config_path.to_owned(),
        dynamic,
        bootstrap_method: raw.bootstrap.method,
        bootstrap_initdb: raw.bootstrap.initdb,
        bootstrap_pg_hba: raw.bootstrap.pg_hba,
        restapi: raw.restapi,
        postgresql: raw.postgresql,
        dcs: raw.dcs_backends,
        tags: raw.tags,
        citus: raw.citus,
        ctl: raw.ctl,
        timings,
        watchdog: WatchdogConfig {
            mode: wd_mode,
            device: raw
                .watchdog
                .device
                .unwrap_or_else(|| "/dev/watchdog".to_owned()),
            safety_margin,
            timeout: timings.watchdog_timeout,
            min_timeout: timings.loop_wait + timings.retry_timeout + Duration::from_secs(2),
            ttl: timings.ttl,
        },
        runtime,
    })
}

impl DaemonConfig {
    /// Applies Tier-1 dynamic DCS `/config` JSON over `self.dynamic` (RFC 7396 merge patch)
    /// and re-validates HA and watchdog timings before committing the updated view.
    pub fn with_dcs_dynamic_config(
        &self,
        dcs_config_json: &JsonValue,
    ) -> Result<Self, ConfigError> {
        let mut merged_dyn = serde_json::to_value(&self.dynamic)?;
        deep_merge_json(&mut merged_dyn, dcs_config_json);
        let next_dynamic: DynamicClusterConfig = serde_json::from_value(merged_dyn)?;

        let next_timings = validate_timings(
            next_dynamic.ttl,
            next_dynamic.loop_wait,
            next_dynamic.retry_timeout,
            self.watchdog.mode,
            self.watchdog.safety_margin,
        )?;

        let mut updated = self.clone();
        updated.dynamic = next_dynamic;
        updated.timings = next_timings;
        updated.watchdog.timeout = next_timings.watchdog_timeout;
        updated.watchdog.min_timeout =
            next_timings.loop_wait + next_timings.retry_timeout + Duration::from_secs(2);
        updated.watchdog.ttl = next_timings.ttl;
        Ok(updated)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn default_patroni_timings_pass_with_5s_margin() {
        let t = validate_timings(30, 10, 10, WatchdogMode::Automatic, 5).unwrap();
        assert_eq!(t.watchdog_timeout, Some(Duration::from_secs(25)));
        assert_eq!(t.demotion_budget(), Some(Duration::from_secs(5)));
    }

    #[test]
    fn strict_half_ttl_rejects_insufficient_demotion_budget_and_passes_when_valid() {
        let res = validate_timings(30, 10, 10, WatchdogMode::Required, -1);
        assert!(matches!(
            res,
            Err(ConfigError::InsufficientDemotionBudget { .. })
        ));

        // With ttl=40, loop_wait=5, retry_timeout=5 -> T_wd = 20s >= 5+5+2 (12s)
        let ok = validate_timings(40, 5, 5, WatchdogMode::Required, -1).unwrap();
        assert_eq!(ok.watchdog_timeout, Some(Duration::from_secs(20)));
    }

    #[test]
    fn rejects_zero_or_invalid_negative_safety_margin() {
        assert!(matches!(
            validate_timings(30, 10, 10, WatchdogMode::Automatic, 0),
            Err(ConfigError::InvalidSafetyMargin(0))
        ));
        assert!(matches!(
            validate_timings(30, 10, 10, WatchdogMode::Automatic, -5),
            Err(ConfigError::InvalidSafetyMargin(-5))
        ));
    }

    #[test]
    fn parses_real_world_patroni_yaml_and_redacts_secrets_in_debug() {
        let yaml = r#"
scope: orders-ha
namespace: /service/
name: pg-eu-1

restapi:
  listen: 0.0.0.0:8008
  connect_address: 10.24.1.10:8008
  authentication:
    username: admin
    password: super-secret-api-pw
  allowlist:
    - 10.24.0.0/16
  allowlist_include_members: true

etcd3:
  hosts: 10.24.0.5:2379, 10.24.0.6:2379

bootstrap:
  dcs:
    ttl: 30
    loop_wait: 10
    retry_timeout: 10
    maximum_lag_on_failover: 2097152
    synchronous_mode: quorum
    synchronous_node_count: 2
    failsafe_mode: true
    postgresql:
      use_pg_rewind: true
      parameters:
        max_connections: 300
        shared_buffers: 8GB
    slots:
      debezium_orders:
        type: logical
        database: orders
        plugin: pgoutput

postgresql:
  listen: 0.0.0.0:5432
  connect_address: 10.24.1.10:5432
  data_dir: /var/lib/postgresql/17/main
  parameters:
    shared_buffers: 16GB
  authentication:
    superuser:
      username: postgres
      password: root-db-password
    replication:
      username: replicator
      password: repl-secret-password

watchdog:
  mode: off

tags:
  nofailover: false
  clonefrom: true
  sync_priority: 10
  az: eu-central-1a
"#;

        let empty_env: [(&str, &str); 0] = [];
        let cfg = parse_and_validate_with_env(yaml, "/etc/patroni/patroni.yml", empty_env).unwrap();

        assert_eq!(cfg.dcs_prefix(), "/service/orders-ha");
        assert_eq!(cfg.dynamic.synchronous_mode, SyncMode::Quorum);
        assert_eq!(cfg.dynamic.synchronous_node_count, 2);
        assert!(cfg.dynamic.failsafe_mode);
        assert_eq!(
            cfg.dcs.etcd3.as_ref().unwrap().endpoints(),
            vec!["10.24.0.5:2379".to_owned(), "10.24.0.6:2379".to_owned()]
        );
        assert!(cfg.tags.clonefrom);
        assert_eq!(cfg.tags.sync_priority, Some(10));
        assert_eq!(cfg.tags.effective_sync_priority(), 10);
        assert!(!cfg.tags.effective_nosync());
        assert_eq!(
            cfg.tags.custom_string_tags().get("az").map(|s| s.as_str()),
            Some("eu-central-1a")
        );

        // Local `postgresql.parameters` overrides shared `bootstrap.dcs.postgresql.parameters`
        let eff_params = cfg.effective_pg_parameters();
        assert_eq!(eff_params.get("max_connections"), Some(&json!(300)));
        assert_eq!(eff_params.get("shared_buffers"), Some(&json!("16GB")));

        // Verify secrets NEVER appear in Debug formatting
        let debug_dump = format!("{cfg:?}");
        assert!(!debug_dump.contains("super-secret-api-pw"));
        assert!(!debug_dump.contains("root-db-password"));
        assert!(!debug_dump.contains("repl-secret-password"));
        assert!(debug_dump.contains("[REDACTED]"));
    }

    #[test]
    fn dual_prefix_env_overlay_prefers_negroni_and_preserves_numeric_passwords() {
        let yaml = "scope: yaml-scope\nname: yaml-node\nwatchdog:\n  mode: off\n";
        let env_vars = [
            ("PATRONI_SCOPE", "patroni-cluster"),
            ("NEGRONI_SCOPE", "negroni-cluster"),
            ("PATRONI_NAME", "pg-from-patroni-env"),
            ("PATRONI_SUPERUSER_USERNAME", "postgres"),
            // Leading-zero numeric password MUST remain exact string `"001234"`, not integer `1234`!
            ("PATRONI_SUPERUSER_PASSWORD", "001234"),
            ("PATRONI_POSTGRESQL_DATA_DIR", "/pgdata/18/main"),
            (
                "PATRONI_KUBERNETES_LABELS",
                "{application: spilo, cluster-name: orders}",
            ),
            ("PATRONI_KUBERNETES_USE_ENDPOINTS", "false"),
            (
                "NEGRONI_POSTGRESQL_PARAMETERS",
                r#"{"work_mem": "64MB", "io_method": "io_uring"}"#,
            ),
        ];

        let cfg = parse_and_validate_with_env(yaml, "env.yml", env_vars).unwrap();
        assert_eq!(cfg.scope, "negroni-cluster");
        assert_eq!(cfg.name, "pg-from-patroni-env");
        assert_eq!(
            cfg.postgresql
                .authentication
                .superuser
                .password
                .as_ref()
                .unwrap()
                .expose(),
            "001234"
        );
        assert_eq!(cfg.postgresql.data_dir.to_str(), Some("/pgdata/18/main"));

        let kube = cfg.dcs.kubernetes.as_ref().unwrap();
        assert!(!kube.use_endpoints);
        assert_eq!(
            kube.labels.get("application").map(|s| s.as_str()),
            Some("spilo")
        );
        assert_eq!(
            cfg.postgresql.parameters.get("io_method"),
            Some(&json!("io_uring"))
        );
    }

    #[test]
    fn dynamic_dcs_config_merge_updates_timings_and_rejects_unsafe_patches() {
        let yaml = "scope: prod\nname: pg-0\nwatchdog:\n  mode: automatic\n  safety_margin: 5\n";
        let empty_env: [(&str, &str); 0] = [];
        let base = parse_and_validate_with_env(yaml, "base.yml", empty_env).unwrap();
        assert_eq!(base.timings.ttl, Duration::from_secs(30));

        // Valid DCS `/config` update: bump ttl to 45s, loop_wait to 5s, enable pause
        let valid_patch = json!({
            "ttl": 45,
            "loop_wait": 5,
            "pause": true,
            "postgresql": {
                "parameters": {
                    "wal_keep_size": "2GB"
                }
            }
        });
        let updated = base.with_dcs_dynamic_config(&valid_patch).unwrap();
        assert_eq!(updated.timings.ttl, Duration::from_secs(45));
        assert_eq!(
            updated.timings.watchdog_timeout,
            Some(Duration::from_secs(40))
        );
        assert!(updated.dynamic.pause);
        assert_eq!(
            updated.effective_pg_parameters().get("wal_keep_size"),
            Some(&json!("2GB"))
        );

        // Unsafe DCS `/config` patch violating `loop_wait + 2 * retry_timeout <= ttl` is rejected!
        let invalid_patch = json!({
            "ttl": 20,
            "loop_wait": 10,
            "retry_timeout": 10
        });
        assert!(matches!(
            base.with_dcs_dynamic_config(&invalid_patch),
            Err(ConfigError::LeaseRuleViolated { .. })
        ));
    }
}
