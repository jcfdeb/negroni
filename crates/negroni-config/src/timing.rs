use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize};

use crate::ConfigError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WatchdogMode {
    Off,
    #[default]
    Automatic,
    Required,
}

impl<'de> Deserialize<'de> for WatchdogMode {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum RawMode {
            Bool(bool),
            Text(String),
        }

        match RawMode::deserialize(deserializer)? {
            RawMode::Bool(false) => Ok(Self::Off),
            RawMode::Bool(true) => Ok(Self::Automatic),
            RawMode::Text(s) => match s.trim().to_ascii_lowercase().as_str() {
                "off" | "false" | "none" | "disabled" => Ok(Self::Off),
                "automatic" | "auto" | "true" => Ok(Self::Automatic),
                "required" => Ok(Self::Required),
                other => Err(serde::de::Error::unknown_variant(
                    other,
                    &["off", "automatic", "required"],
                )),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValidatedTimings {
    pub ttl: Duration,
    pub loop_wait: Duration,
    pub retry_timeout: Duration,
    pub watchdog_timeout: Option<Duration>,
    pub safety_margin: Duration,
}

impl ValidatedTimings {
    /// Available time budget to gracefully stop PostgreSQL (`SIGINT` -> `SIGQUIT`) before hardware reset.
    pub fn demotion_budget(&self) -> Option<Duration> {
        self.watchdog_timeout
            .map(|wd| wd.saturating_sub(self.loop_wait + self.retry_timeout))
    }
}

/// Validates Patroni HA lease and hardware watchdog timing equations:
/// - `loop_wait + 2 * retry_timeout <= ttl`
/// - `watchdog_timeout >= loop_wait + retry_timeout + 2s` (leaving >= 2s demotion budget)
///
/// # Examples
///
/// ```
/// use std::time::Duration;
/// use negroni_config::{WatchdogMode, validate_timings};
///
/// let timings = validate_timings(30, 10, 10, WatchdogMode::Automatic, 5)?;
/// assert_eq!(timings.watchdog_timeout, Some(Duration::from_secs(25)));
/// assert_eq!(timings.demotion_budget(), Some(Duration::from_secs(5)));
/// # Ok::<(), negroni_config::ConfigError>(())
/// ```
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
    } else if let Ok(margin) = u64::try_from(safety_margin_cfg)
        && margin >= 1
    {
        effective_ttl.saturating_sub(margin)
    } else {
        return Err(ConfigError::InvalidSafetyMargin(safety_margin_cfg));
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
