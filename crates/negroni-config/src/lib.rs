use std::env;
use std::fs;
use std::num::NonZeroUsize;
use std::time::Duration;

use compact_str::CompactString;
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WatchdogMode {
    Off,
    #[default]
    Automatic,
    Required,
}

#[derive(Debug, Clone)]
pub struct WatchdogConfig {
    pub mode: WatchdogMode,
    pub device: String,
    pub safety_margin: i64,
    pub timeout: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub worker_threads: NonZeroUsize,
    pub max_blocking_threads: NonZeroUsize,
    pub thread_stack_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidatedTimings {
    pub ttl: Duration,
    pub loop_wait: Duration,
    pub retry_timeout: Duration,
    pub watchdog_timeout: Option<Duration>,
    pub safety_margin: Duration,
}

#[derive(Debug, Clone)]
pub struct DaemonConfig {
    pub scope: CompactString,
    pub name: CompactString,
    pub namespace: CompactString,
    pub config_path: String,
    pub timings: ValidatedTimings,
    pub watchdog: WatchdogConfig,
    pub runtime: RuntimeConfig,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read configuration file '{path}': {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse YAML configuration: {0}")]
    Yaml(#[from] serde_norway::Error),
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

#[derive(Debug, Default, Deserialize)]
struct RawPatroniYaml {
    scope: Option<CompactString>,
    name: Option<CompactString>,
    namespace: Option<CompactString>,
    #[serde(default)]
    bootstrap: RawBootstrapSection,
    #[serde(default)]
    watchdog: RawWatchdogSection,
}

#[derive(Debug, Default, Deserialize)]
struct RawBootstrapSection {
    #[serde(default)]
    dcs: RawDcsTimings,
}

#[derive(Debug, Default, Deserialize)]
struct RawDcsTimings {
    ttl: Option<u64>,
    loop_wait: Option<u64>,
    retry_timeout: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
struct RawWatchdogSection {
    mode: Option<WatchdogMode>,
    device: Option<String>,
    safety_margin: Option<i64>,
}

pub fn validate_timings(
    ttl_secs: u64,
    loop_wait_secs: u64,
    retry_timeout_secs: u64,
    wd_mode: WatchdogMode,
    safety_margin_cfg: i64,
) -> Result<ValidatedTimings, ConfigError> {
    let effective_ttl = ttl_secs.max(20);
    let ttl = Duration::from_secs(effective_ttl);
    let loop_wait = Duration::from_secs(loop_wait_secs.max(1));
    let retry_timeout = Duration::from_secs(retry_timeout_secs.max(3));

    if loop_wait + (retry_timeout * 2) > ttl {
        return Err(ConfigError::LeaseRuleViolated {
            ttl,
            loop_wait,
            retry_timeout,
        });
    }

    if wd_mode == WatchdogMode::Off {
        return Ok(ValidatedTimings {
            ttl,
            loop_wait,
            retry_timeout,
            watchdog_timeout: None,
            safety_margin: Duration::ZERO,
        });
    }

    let wd_secs = if safety_margin_cfg == -1 {
        effective_ttl / 2
    } else {
        let margin = u64::try_from(safety_margin_cfg.max(1)).unwrap_or(5);
        effective_ttl.saturating_sub(margin)
    };
    let wd_timeout = Duration::from_secs(wd_secs);
    let min_required = loop_wait + retry_timeout + Duration::from_secs(2);

    if wd_timeout < min_required {
        return Err(ConfigError::InsufficientDemotionBudget {
            wd_timeout,
            loop_wait,
            retry_timeout,
        });
    }

    Ok(ValidatedTimings {
        ttl,
        loop_wait,
        retry_timeout,
        watchdog_timeout: Some(wd_timeout),
        safety_margin: ttl - wd_timeout,
    })
}

fn resolve_dual_env(negroni_key: &str, patroni_key: &str) -> Option<CompactString> {
    let neg = env::var(negroni_key).ok();
    let pat = env::var(patroni_key).ok();
    if let (Some(n), Some(p)) = (&neg, &pat)
        && n != p
    {
        tracing::warn!(
            negroni_key,
            patroni_key,
            "conflicting NEGRONI_* and PATRONI_* environment variables; preferring NEGRONI_*"
        );
    }
    neg.or(pat).map(CompactString::from)
}

pub fn load_and_validate(config_path: &str) -> Result<DaemonConfig, ConfigError> {
    let content = fs::read_to_string(config_path).map_err(|source| ConfigError::Io {
        path: config_path.to_owned(),
        source,
    })?;
    parse_and_validate_str(&content, config_path)
}

pub fn parse_and_validate_str(yaml_str: &str, config_path: &str) -> Result<DaemonConfig, ConfigError> {
    let raw: RawPatroniYaml = serde_norway::from_str(yaml_str)?;

    let scope = resolve_dual_env("NEGRONI_SCOPE", "PATRONI_SCOPE")
        .or(raw.scope)
        .unwrap_or_else(|| CompactString::const_new("postgres"));
    let name = resolve_dual_env("NEGRONI_NAME", "PATRONI_NAME")
        .or(raw.name)
        .unwrap_or_else(|| CompactString::const_new("node0"));
    let namespace = resolve_dual_env("NEGRONI_NAMESPACE", "PATRONI_NAMESPACE")
        .or(raw.namespace)
        .unwrap_or_else(|| CompactString::const_new("/service"));

    let wd_mode = raw.watchdog.mode.unwrap_or_default();
    let safety_margin = raw.watchdog.safety_margin.unwrap_or(5);
    let timings = validate_timings(
        raw.bootstrap.dcs.ttl.unwrap_or(30),
        raw.bootstrap.dcs.loop_wait.unwrap_or(10),
        raw.bootstrap.dcs.retry_timeout.unwrap_or(10),
        wd_mode,
        safety_margin,
    )?;

    Ok(DaemonConfig {
        scope,
        name,
        namespace,
        config_path: config_path.to_owned(),
        timings,
        watchdog: WatchdogConfig {
            mode: wd_mode,
            device: raw
                .watchdog
                .device
                .unwrap_or_else(|| "/dev/watchdog".to_owned()),
            safety_margin,
            timeout: timings.watchdog_timeout,
        },
        runtime: RuntimeConfig {
            worker_threads: NonZeroUsize::MIN.saturating_add(3),
            max_blocking_threads: NonZeroUsize::MIN.saturating_add(7),
            thread_stack_bytes: 1024 * 1024,
        },
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn default_patroni_timings_pass_with_5s_margin() {
        let t = validate_timings(30, 10, 10, WatchdogMode::Automatic, 5).unwrap();
        assert_eq!(t.watchdog_timeout, Some(Duration::from_secs(25)));
    }

    #[test]
    fn strict_half_ttl_rejects_insufficient_demotion_budget() {
        let res = validate_timings(30, 10, 10, WatchdogMode::Required, -1);
        assert!(matches!(res, Err(ConfigError::InsufficientDemotionBudget { .. })));
    }

    #[test]
    fn parses_minimal_patroni_yaml() {
        let yaml = "scope: prod-pg\nname: pg-1\nwatchdog:\n  mode: off\n";
        let cfg = parse_and_validate_str(yaml, "memory.yml").unwrap();
        assert_eq!(cfg.scope, "prod-pg");
        assert_eq!(cfg.name, "pg-1");
        assert_eq!(cfg.watchdog.mode, WatchdogMode::Off);
    }
}
