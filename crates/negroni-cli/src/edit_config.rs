use std::env;
use std::fmt::Write as _;
use std::fs;
use std::io::{self, Write as _};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use negroni_config::{deep_merge_json, yaml_to_json_value};
use negroni_dcs::{DcsBackend, DcsError};
use serde_json::{Map as JsonMap, Value as JsonValue};
use similar::TextDiff;

fn yaml_needs_quotes(s: &str) -> bool {
    s.is_empty()
        || s.starts_with(' ')
        || s.ends_with(' ')
        || s.contains([':', '#', '\n', '\r', '\t', '"', '\'', '{', '}', '[', ']'])
        || matches!(
            s.to_ascii_lowercase().as_str(),
            "true" | "false" | "null" | "~" | "yes" | "no" | "on" | "off"
        )
        || s.parse::<i64>().is_ok()
        || s.parse::<f64>().is_ok()
}

fn format_yaml_scalar(val: &JsonValue) -> String {
    match val {
        JsonValue::Null => "null".to_owned(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::String(s) => {
            if yaml_needs_quotes(s) {
                serde_json::to_string(s).unwrap_or_else(|_| format!("\"{s}\""))
            } else {
                s.clone()
            }
        }
        _ => String::new(),
    }
}

fn write_yaml_node(buf: &mut String, val: &JsonValue, indent: usize) {
    let pad = " ".repeat(indent);
    match val {
        JsonValue::Object(map) => {
            if map.is_empty() {
                let _ = writeln!(buf, "{pad}{{}}");
                return;
            }
            for (k, v) in map {
                let key_str = if yaml_needs_quotes(k) {
                    serde_json::to_string(k).unwrap_or_else(|_| k.clone())
                } else {
                    k.clone()
                };
                match v {
                    JsonValue::Object(sub) if !sub.is_empty() => {
                        let _ = writeln!(buf, "{pad}{key_str}:");
                        write_yaml_node(buf, v, indent + 2);
                    }
                    JsonValue::Array(arr) if !arr.is_empty() => {
                        let _ = writeln!(buf, "{pad}{key_str}:");
                        write_yaml_node(buf, v, indent + 2);
                    }
                    JsonValue::Object(_) => {
                        let _ = writeln!(buf, "{pad}{key_str}: {{}}");
                    }
                    JsonValue::Array(_) => {
                        let _ = writeln!(buf, "{pad}{key_str}: []");
                    }
                    _ => {
                        let _ = writeln!(buf, "{pad}{key_str}: {}", format_yaml_scalar(v));
                    }
                }
            }
        }
        JsonValue::Array(items) => {
            if items.is_empty() {
                let _ = writeln!(buf, "{pad}[]");
                return;
            }
            for item in items {
                match item {
                    JsonValue::Object(map) if !map.is_empty() => {
                        let mut iter = map.iter();
                        if let Some((first_k, first_v)) = iter.next() {
                            if first_v.is_object() || first_v.is_array() {
                                let _ = writeln!(buf, "{pad}- {first_k}:");
                                write_yaml_node(buf, first_v, indent + 4);
                            } else {
                                let _ = writeln!(
                                    buf,
                                    "{pad}- {first_k}: {}",
                                    format_yaml_scalar(first_v)
                                );
                            }
                            let inner_pad = " ".repeat(indent + 2);
                            for (k, v) in iter {
                                if v.is_object() || v.is_array() {
                                    let _ = writeln!(buf, "{inner_pad}{k}:");
                                    write_yaml_node(buf, v, indent + 4);
                                } else {
                                    let _ =
                                        writeln!(buf, "{inner_pad}{k}: {}", format_yaml_scalar(v));
                                }
                            }
                        }
                    }
                    _ => {
                        let _ = writeln!(buf, "{pad}- {}", format_yaml_scalar(item));
                    }
                }
            }
        }
        _ => {
            let _ = writeln!(buf, "{pad}{}", format_yaml_scalar(val));
        }
    }
}

/// Formats a `serde_json::Value` tree into deterministic, round-trippable YAML 1.2 text.
///
/// # Examples
///
/// ```
/// use negroni_cli::edit_config::json_to_yaml_string;
/// use negroni_config::yaml_to_json_value;
///
/// let input = serde_json::json!({"ttl": 30, "synchronous_mode": true});
/// let yaml = json_to_yaml_string(&input);
/// let parsed = yaml_to_json_value(&yaml)?;
/// assert_eq!(parsed, input);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn json_to_yaml_string(val: &JsonValue) -> String {
    let mut out = String::new();
    write_yaml_node(&mut out, val, 0);
    out
}

/// Parses a CLI scalar override (`ttl=45`, `pause=true`, `standby_cluster=null`, `shared_buffers=2GB`).
pub fn parse_cli_value(raw: &str) -> JsonValue {
    let trimmed = raw.trim();
    if let Ok(parsed) = yaml_to_json_value(trimmed)
        && !parsed.is_object()
    {
        return parsed;
    }
    JsonValue::String(trimmed.to_owned())
}

/// Applies a dotted path override (e.g. `postgresql.use_pg_rewind=true`) into a JSON object,
/// deleting keys when `value` is `null`.
pub fn apply_dotted_kv_override(
    root: &mut JsonValue,
    dotted_key: &str,
    raw_value: &str,
) -> Result<()> {
    let segments: Vec<&str> = dotted_key
        .split('.')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if segments.is_empty() {
        bail!("empty key path in override '{dotted_key}={raw_value}'");
    }

    let val = parse_cli_value(raw_value);
    set_nested_path(root, &segments, val);
    Ok(())
}

fn set_nested_path(current: &mut JsonValue, segments: &[&str], val: JsonValue) {
    if !current.is_object() {
        *current = JsonValue::Object(JsonMap::new());
    }
    let Some((first, rest)) = segments.split_first() else {
        return;
    };
    let Some(map) = current.as_object_mut() else {
        return;
    };

    if rest.is_empty() {
        if val.is_null() {
            map.remove(*first);
        } else {
            map.insert((*first).to_owned(), val);
        }
    } else {
        let entry = map
            .entry((*first).to_owned())
            .or_insert_with(|| JsonValue::Object(JsonMap::new()));
        set_nested_path(entry, rest, val);
    }
}

/// Renders a unified line diff (`--- before` / `+++ after`) via `similar::TextDiff`.
pub fn render_unified_config_diff(before_yaml: &str, after_yaml: &str) -> String {
    TextDiff::from_lines(before_yaml, after_yaml)
        .unified_diff()
        .context_radius(3)
        .header("before", "after")
        .to_string()
}

pub struct EditConfigArgs {
    pub set_overrides: Vec<String>,
    pub pg_overrides: Vec<String>,
    pub replace_file: Option<String>,
    pub apply_file: Option<String>,
    pub force: bool,
    pub quiet: bool,
}

fn open_in_editor(initial_yaml: &str) -> Result<String> {
    let editor = env::var("EDITOR")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .or_else(|| env::var("VISUAL").ok().filter(|s| !s.trim().is_empty()))
        .unwrap_or_else(|| "vi".to_owned());

    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos();
    let path = env::temp_dir().join(format!("negronictl-config-{stamp}.yaml"));
    fs::write(&path, initial_yaml)
        .with_context(|| format!("failed to write temporary editor file '{}'", path.display()))?;

    let status = Command::new(&editor)
        .arg(&path)
        .status()
        .with_context(|| format!("failed to invoke editor '{editor}'"))?;
    let edited = fs::read_to_string(&path)
        .with_context(|| format!("failed to read edited temporary file '{}'", path.display()));
    let _ = fs::remove_file(&path);

    if !status.success() {
        bail!("editor '{editor}' exited with non-zero status {status}");
    }
    edited
}

fn confirm_apply() -> Result<bool> {
    print!("Apply these changes? [y/N]: ");
    io::stdout().flush()?;
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    Ok(matches!(
        line.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

pub async fn run_edit_config<D: DcsBackend>(dcs: &D, opts: &EditConfigArgs) -> Result<()> {
    let non_interactive = !opts.set_overrides.is_empty()
        || !opts.pg_overrides.is_empty()
        || opts.replace_file.is_some()
        || opts.apply_file.is_some();

    loop {
        let snap = dcs
            .fetch_cluster(Duration::from_secs(5))
            .await
            .context("failed to fetch cluster state from DCS")?;
        let expected_tok = snap.config.as_ref().map(|v| v.token.clone());
        let original_json = snap
            .config
            .map(|v| v.value)
            .unwrap_or_else(|| JsonValue::Object(JsonMap::new()));
        let before_yaml = json_to_yaml_string(&original_json);

        let mut candidate_json = if let Some(ref replace_path) = opts.replace_file {
            let content = if replace_path == "-" {
                io::read_to_string(io::stdin())?
            } else {
                fs::read_to_string(replace_path)
                    .with_context(|| format!("failed to read --replace file '{replace_path}'"))?
            };
            yaml_to_json_value(&content)
                .with_context(|| format!("invalid YAML/JSON in '{replace_path}'"))?
        } else {
            original_json.clone()
        };

        if let Some(ref apply_path) = opts.apply_file {
            let content = if apply_path == "-" {
                io::read_to_string(io::stdin())?
            } else {
                fs::read_to_string(apply_path)
                    .with_context(|| format!("failed to read --apply file '{apply_path}'"))?
            };
            let patch = yaml_to_json_value(&content)
                .with_context(|| format!("invalid YAML/JSON in '{apply_path}'"))?;
            deep_merge_json(&mut candidate_json, &patch);
        }

        for kv in &opts.set_overrides {
            let Some((k, v)) = kv.split_once('=') else {
                bail!("invalid -s/--set argument '{kv}' (expected key=value)");
            };
            apply_dotted_kv_override(&mut candidate_json, k, v)?;
        }

        for kv in &opts.pg_overrides {
            let Some((guc, v)) = kv.split_once('=') else {
                bail!("invalid -p/--pg argument '{kv}' (expected parameter=value)");
            };
            let full_key = format!("postgresql.parameters.{}", guc.trim());
            apply_dotted_kv_override(&mut candidate_json, &full_key, v)?;
        }

        if !non_interactive {
            let edited_yaml = open_in_editor(&before_yaml)?;
            candidate_json =
                yaml_to_json_value(&edited_yaml).context("invalid YAML edited in $EDITOR")?;
        }

        let after_yaml = json_to_yaml_string(&candidate_json);
        if candidate_json == original_json {
            if !opts.quiet {
                println!("No changes made.");
            }
            return Ok(());
        }

        if !opts.quiet {
            let diff = render_unified_config_diff(&before_yaml, &after_yaml);
            print!("{diff}");
        }

        if !opts.force && !confirm_apply()? {
            println!("Aborted.");
            return Ok(());
        }

        match dcs.cas_config(&candidate_json, expected_tok.as_ref()).await {
            Ok(_) => {
                if !opts.quiet {
                    println!("Configuration changed");
                }
                return Ok(());
            }
            Err(DcsError::CasConflict { .. }) => {
                if non_interactive && opts.force {
                    // Retry CAS cleanly with fresh snapshot when non-interactive `--force` is set.
                    continue;
                }
                bail!(
                    "Error: configuration in DCS was modified concurrently by another process (CAS conflict)"
                );
            }
            Err(err) => return Err(err).context("failed to write updated /config to DCS"),
        }
    }
}
