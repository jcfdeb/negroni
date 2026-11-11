use std::collections::BTreeMap;

use serde_json::{Map as JsonMap, Value as JsonValue};

use crate::{ConfigError, yaml_to_json_value};

#[derive(Debug, Default)]
struct DualVarPair {
    negroni: Option<(String, String)>,
    patroni: Option<(String, String)>,
}

/// Maps canonical `PATRONI_<SUFFIX>` / `NEGRONI_<SUFFIX>` suffixes to JSON config paths
/// matching upstream Patroni 4.1+ `Config._process_environment_variables()`.
fn resolve_env_path(suffix: &str) -> Option<(&'static [&'static str], Option<String>, bool)> {
    // Returns `(fixed_prefix_segments, optional_leaf_segment, force_plain_string)`.
    match suffix {
        // Ignored file-pointer variables (handled by binary CLI/daemon router)
        "CONFIGURATION" => None,

        // Node identity (strictly plain strings)
        "SCOPE" => Some((&["scope"], None, true)),
        "NAME" => Some((&["name"], None, true)),
        "NAMESPACE" => Some((&["namespace"], None, true)),

        // Upstream Patroni PostgreSQL authentication shortcuts (`PATRONI_SUPERUSER_PASSWORD`, etc.)
        s if s.starts_with("SUPERUSER_") => {
            let field = s.strip_prefix("SUPERUSER_")?.to_ascii_lowercase();
            Some((
                &["postgresql", "authentication", "superuser"],
                Some(field),
                true,
            ))
        }
        s if s.starts_with("REPLICATION_") => {
            let field = s.strip_prefix("REPLICATION_")?.to_ascii_lowercase();
            Some((
                &["postgresql", "authentication", "replication"],
                Some(field),
                true,
            ))
        }
        s if s.starts_with("REWIND_") => {
            let field = s.strip_prefix("REWIND_")?.to_ascii_lowercase();
            Some((
                &["postgresql", "authentication", "rewind"],
                Some(field),
                true,
            ))
        }

        // Upstream Patroni REST API authentication shortcuts
        "RESTAPI_USERNAME" => Some((&["restapi", "authentication", "username"], None, true)),
        "RESTAPI_PASSWORD" => Some((&["restapi", "authentication", "password"], None, true)),

        // Structured top-level sections
        "TAGS" => Some((&["tags"], None, false)),
        s if s.starts_with("TAGS_") => {
            let tag_name = s.strip_prefix("TAGS_")?.to_ascii_lowercase();
            Some((&["tags"], Some(tag_name), false))
        }

        // Known subsystem prefixes where the remainder can contain underscores (`DATA_DIR`, `USE_ENDPOINTS`, etc.)
        _ => {
            const SECTIONS: &[(&str, &[&str])] = &[
                ("POSTGRESQL_", &["postgresql"]),
                ("RESTAPI_", &["restapi"]),
                ("ETCD3_", &["etcd3"]),
                ("ETCD_", &["etcd"]),
                ("KUBERNETES_", &["kubernetes"]),
                ("CONSUL_", &["consul"]),
                ("ZOOKEEPER_", &["zookeeper"]),
                ("RAFT_", &["raft"]),
                ("WATCHDOG_", &["watchdog"]),
                ("CITUS_", &["citus"]),
                ("CTL_", &["ctl"]),
                ("RUNTIME_", &["runtime"]),
                ("STANDBY_CLUSTER_", &["bootstrap", "dcs", "standby_cluster"]),
                ("LOG_", &["log"]),
            ];

            for &(prefix, path_prefix) in SECTIONS {
                if let Some(rest) = suffix.strip_prefix(prefix)
                    && !rest.is_empty()
                {
                    let leaf = rest.to_ascii_lowercase();
                    let force_string = is_string_only_leaf(&leaf);
                    return Some((path_prefix, Some(leaf), force_string));
                }
            }
            None
        }
    }
}

fn is_string_only_leaf(leaf: &str) -> bool {
    matches!(
        leaf,
        "password"
            | "username"
            | "token"
            | "key_password"
            | "keyfile_password"
            | "listen"
            | "connect_address"
            | "proxy_address"
            | "data_dir"
            | "config_dir"
            | "bin_dir"
            | "pgpass"
            | "device"
            | "cert"
            | "key"
            | "cacert"
            | "certfile"
            | "keyfile"
            | "host"
            | "url"
            | "self_addr"
            | "bind_addr"
            | "pod_ip"
            | "namespace"
            | "database"
            | "protocol"
            | "scheme"
            | "dc"
            | "verify_client"
    )
}

fn parse_env_value(raw: &str, force_plain_string: bool) -> Result<JsonValue, ConfigError> {
    if force_plain_string {
        return Ok(JsonValue::String(raw.to_owned()));
    }

    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(JsonValue::String(String::new()));
    }

    // Parse structured YAML/JSON objects, lists, booleans, or numbers (`{app: spilo}`, `["a","b"]`, `true`, `25`),
    // falling back to a plain string when a host:port token like `10.0.0.1:2379` is not a YAML mapping.
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        return yaml_to_json_value(trimmed);
    }

    match trimmed.to_ascii_lowercase().as_str() {
        "true" => return Ok(JsonValue::Bool(true)),
        "false" => return Ok(JsonValue::Bool(false)),
        _ => {}
    }

    if let Ok(int_val) = trimmed.parse::<i64>() {
        return Ok(JsonValue::Number(int_val.into()));
    }

    Ok(JsonValue::String(raw.to_owned()))
}

fn insert_at_path(root: &mut JsonValue, prefix: &[&str], leaf: Option<&str>, value: JsonValue) {
    let mut cursor = root;
    let total_fixed = prefix.len();
    for (idx, seg) in prefix.iter().enumerate() {
        let is_final = idx + 1 == total_fixed && leaf.is_none();
        if is_final {
            if let JsonValue::Object(map) = cursor {
                merge_or_set(map, seg, value);
            }
            return;
        }
        if !cursor.is_object() {
            *cursor = JsonValue::Object(JsonMap::new());
        }
        if let JsonValue::Object(map) = cursor {
            cursor = map
                .entry((*seg).to_owned())
                .or_insert_with(|| JsonValue::Object(JsonMap::new()));
        }
    }

    if let Some(leaf_key) = leaf {
        if !cursor.is_object() {
            *cursor = JsonValue::Object(JsonMap::new());
        }
        if let JsonValue::Object(map) = cursor {
            merge_or_set(map, leaf_key, value);
        }
    }
}

fn merge_or_set(map: &mut JsonMap<String, JsonValue>, key: &str, value: JsonValue) {
    match (map.get_mut(key), value) {
        (Some(JsonValue::Object(existing)), JsonValue::Object(incoming)) => {
            for (k, v) in incoming {
                merge_or_set(existing, &k, v);
            }
        }
        (_, v) => {
            map.insert(key.to_owned(), v);
        }
    }
}

/// Overlays `PATRONI_*` and `NEGRONI_*` environment variables onto `tree` in deterministic key order.
///
/// If both `NEGRONI_<SUFFIX>` and `PATRONI_<SUFFIX>` are present with unequal values, emits a
/// `tracing::warn!` (without logging secret values) and applies `NEGRONI_<SUFFIX>`.
pub fn apply_dual_prefix_env_overlay<I, K, V>(
    tree: &mut JsonValue,
    vars: I,
) -> Result<(), ConfigError>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: AsRef<str>,
{
    let mut grouped: BTreeMap<String, DualVarPair> = BTreeMap::new();

    for (raw_key, raw_val) in vars {
        let key = raw_key.as_ref();
        if let Some(suffix) = key.strip_prefix("NEGRONI_")
            && !suffix.is_empty()
        {
            grouped.entry(suffix.to_owned()).or_default().negroni =
                Some((key.to_owned(), raw_val.as_ref().to_owned()));
        } else if let Some(suffix) = key.strip_prefix("PATRONI_")
            && !suffix.is_empty()
        {
            grouped.entry(suffix.to_owned()).or_default().patroni =
                Some((key.to_owned(), raw_val.as_ref().to_owned()));
        }
    }

    if !tree.is_object() {
        *tree = JsonValue::Object(JsonMap::new());
    }

    for (suffix, pair) in grouped {
        let Some((prefix_path, leaf, force_string)) = resolve_env_path(&suffix) else {
            continue;
        };

        let chosen_val = match (pair.negroni, pair.patroni) {
            (Some((neg_key, neg_val)), Some((pat_key, pat_val))) => {
                if neg_val != pat_val {
                    tracing::warn!(
                        negroni_var = %neg_key,
                        patroni_var = %pat_key,
                        "conflicting NEGRONI_* and PATRONI_* environment variables; preferring NEGRONI_* (values redacted)"
                    );
                }
                neg_val
            }
            (Some((_, neg_val)), None) => neg_val,
            (None, Some((_, pat_val))) => pat_val,
            (None, None) => continue,
        };

        let parsed = parse_env_value(&chosen_val, force_string)?;
        insert_at_path(tree, prefix_path, leaf.as_deref(), parsed);
    }

    Ok(())
}
