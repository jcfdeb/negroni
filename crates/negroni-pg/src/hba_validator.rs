use std::collections::BTreeMap;

use compact_str::CompactString;
use serde_json::Value as JsonValue;

use crate::PgError;
use crate::guc_units::parse_pg_memory_bytes;
use crate::version::{PgCapabilities, PgMajor};

const STANDARD_AUTH_METHODS: &[&str] = &[
    "trust",
    "reject",
    "scram-sha-256",
    "md5",
    "password",
    "gss",
    "sspi",
    "ident",
    "peer",
    "ldap",
    "radius",
    "cert",
    "pam",
    "bsd",
    "oauth",
];

fn is_include_directive(tok: &str) -> bool {
    matches!(tok, "include" | "include_if_exists" | "include_dir")
}

fn is_known_auth_method(tok: &str) -> bool {
    STANDARD_AUTH_METHODS.contains(&tok)
}

/// Tokenizes a single logical line from `pg_hba.conf` or `pg_ident.conf`, stripping trailing `#`
/// comments while preserving `#` inside double quotes (`"..."`) or `/regex/` literals.
pub fn tokenize_conf_line(line: &str, line_no: usize) -> Result<Vec<String>, PgError> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut in_regex = false;
    let mut token_started = false;

    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if in_quotes {
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    let _ = chars.next();
                    current.push('"');
                } else {
                    in_quotes = false;
                }
            } else {
                current.push(ch);
            }
            continue;
        }

        if in_regex {
            current.push(ch);
            if ch == '\\' {
                if let Some(escaped) = chars.next() {
                    current.push(escaped);
                }
            } else if ch == '/' {
                in_regex = false;
            }
            continue;
        }

        match ch {
            '#' => break,
            c if c.is_ascii_whitespace() => {
                if token_started {
                    tokens.push(std::mem::take(&mut current));
                    token_started = false;
                }
            }
            '"' => {
                in_quotes = true;
                token_started = true;
            }
            '/' if !token_started || current.ends_with(',') => {
                in_regex = true;
                token_started = true;
                current.push('/');
            }
            other => {
                token_started = true;
                current.push(other);
            }
        }
    }

    if in_quotes {
        return Err(PgError::ConfigValidation(format!(
            "line {line_no}: unterminated double-quoted string"
        )));
    }
    if in_regex {
        return Err(PgError::ConfigValidation(format!(
            "line {line_no}: unterminated '/regex/' token"
        )));
    }
    if token_started {
        tokens.push(current);
    }

    Ok(tokens)
}

fn validate_comma_field_regexes(
    field: &str,
    field_label: &str,
    caps: PgCapabilities,
    line_no: usize,
) -> Result<(), PgError> {
    for part in field.split(',') {
        if part.starts_with('/') {
            if !caps.hba_include_and_regex {
                return Err(PgError::ConfigValidation(format!(
                    "line {line_no}: regular expression token '{part}' in {field_label} requires PostgreSQL 16+"
                )));
            }
            if part.len() < 2 || !part.ends_with('/') {
                return Err(PgError::ConfigValidation(format!(
                    "line {line_no}: malformed regular expression '{part}' in {field_label}"
                )));
            }
        }
    }
    Ok(())
}

fn validate_oauth_options(options: &[String], line_no: usize) -> Result<(), PgError> {
    let mut has_issuer = false;
    let mut has_scope = false;

    for opt in options {
        let Some((k, v)) = opt.split_once('=') else {
            return Err(PgError::ConfigValidation(format!(
                "line {line_no}: invalid oauth option '{opt}'; expected key=value"
            )));
        };
        if v.trim().is_empty() {
            return Err(PgError::ConfigValidation(format!(
                "line {line_no}: oauth option '{k}' must have a non-empty value"
            )));
        }
        match k {
            "issuer" => has_issuer = true,
            "scope" => has_scope = true,
            _ => {}
        }
    }

    if !has_issuer || !has_scope {
        return Err(PgError::ConfigValidation(format!(
            "line {line_no}: PostgreSQL 18 'oauth' authentication requires both 'issuer=...' and 'scope=...' options"
        )));
    }
    Ok(())
}

/// Validates `pg_hba.conf` lines against `major.capabilities()` (**PostgreSQL 15 through 18**).
///
/// # Examples
///
/// ```
/// use negroni_pg::hba_validator::validate_pg_hba;
/// use negroni_pg::version::PgMajor;
///
/// let pg18_hba = [
///     "local all postgres peer".to_owned(),
///     "include_if_exists pg_hba_extra.conf".to_owned(),
///     "hostssl orders /^svc_.+$/ 10.24.0.0/16 oauth issuer=\"https://idp.example\" scope=\"openid pg\"".to_owned(),
/// ];
/// assert!(validate_pg_hba(PgMajor::V18, &pg18_hba).is_ok());
/// assert!(validate_pg_hba(PgMajor::V17, &pg18_hba).is_err()); // `oauth` requires PG 18
/// assert!(validate_pg_hba(PgMajor::V15, &pg18_hba[1..2]).is_err()); // `include*` requires PG 16+
/// ```
pub fn validate_pg_hba(major: PgMajor, lines: &[String]) -> Result<(), PgError> {
    let caps = major.capabilities();

    for (idx, raw_line) in lines.iter().enumerate() {
        let line_no = idx + 1;
        let tokens = tokenize_conf_line(raw_line, line_no)?;
        let Some(first) = tokens.first().map(String::as_str) else {
            continue;
        };

        if is_include_directive(first) {
            if !caps.hba_include_and_regex {
                return Err(PgError::ConfigValidation(format!(
                    "pg_hba.conf line {line_no}: directive '{first}' requires PostgreSQL 16+ (current: PG {})",
                    major.as_u8()
                )));
            }
            if tokens.len() != 2 || tokens[1].trim().is_empty() {
                return Err(PgError::ConfigValidation(format!(
                    "pg_hba.conf line {line_no}: directive '{first}' expects exactly one target path"
                )));
            }
            continue;
        }

        let (auth_idx, addr_token) = match first {
            "local" => {
                if tokens.len() < 4 {
                    return Err(PgError::ConfigValidation(format!(
                        "pg_hba.conf line {line_no}: 'local' record requires at least 4 fields"
                    )));
                }
                (3usize, None)
            }
            "host" | "hostssl" | "hostnossl" | "hostgssenc" | "hostnogssenc" => {
                if tokens.len() < 5 {
                    return Err(PgError::ConfigValidation(format!(
                        "pg_hba.conf line {line_no}: '{first}' record requires at least 5 fields"
                    )));
                }
                if is_known_auth_method(&tokens[4]) {
                    (4usize, Some(tokens[3].as_str()))
                } else if tokens.len() >= 6 && is_known_auth_method(&tokens[5]) {
                    (5usize, Some(tokens[3].as_str()))
                } else {
                    return Err(PgError::ConfigValidation(format!(
                        "pg_hba.conf line {line_no}: unrecognized authentication method '{}'",
                        tokens[4]
                    )));
                }
            }
            other => {
                return Err(PgError::ConfigValidation(format!(
                    "pg_hba.conf line {line_no}: unknown connection type '{other}'"
                )));
            }
        };

        validate_comma_field_regexes(&tokens[1], "database", caps, line_no)?;
        validate_comma_field_regexes(&tokens[2], "user", caps, line_no)?;
        if let Some(addr) = addr_token
            && addr.starts_with('/')
        {
            validate_comma_field_regexes(addr, "address", caps, line_no)?;
        }

        let auth_method = tokens[auth_idx].as_str();
        if !is_known_auth_method(auth_method) {
            return Err(PgError::ConfigValidation(format!(
                "pg_hba.conf line {line_no}: unknown authentication method '{auth_method}'"
            )));
        }

        let auth_options = &tokens[auth_idx + 1..];
        if auth_method == "oauth" {
            if !caps.hba_oauth_and_aio {
                return Err(PgError::ConfigValidation(format!(
                    "pg_hba.conf line {line_no}: 'oauth' authentication requires PostgreSQL 18+ (current: PG {})",
                    major.as_u8()
                )));
            }
            validate_oauth_options(auth_options, line_no)?;
        }
    }

    Ok(())
}

/// Validates `pg_ident.conf` lines against `major.capabilities()` (**PostgreSQL 15 through 18**).
pub fn validate_pg_ident(major: PgMajor, lines: &[String]) -> Result<(), PgError> {
    let caps = major.capabilities();

    for (idx, raw_line) in lines.iter().enumerate() {
        let line_no = idx + 1;
        let tokens = tokenize_conf_line(raw_line, line_no)?;
        let Some(first) = tokens.first().map(String::as_str) else {
            continue;
        };

        if is_include_directive(first) {
            if !caps.hba_include_and_regex {
                return Err(PgError::ConfigValidation(format!(
                    "pg_ident.conf line {line_no}: directive '{first}' requires PostgreSQL 16+ (current: PG {})",
                    major.as_u8()
                )));
            }
            if tokens.len() != 2 || tokens[1].trim().is_empty() {
                return Err(PgError::ConfigValidation(format!(
                    "pg_ident.conf line {line_no}: directive '{first}' expects exactly one target path"
                )));
            }
            continue;
        }

        if tokens.len() != 3 {
            return Err(PgError::ConfigValidation(format!(
                "pg_ident.conf line {line_no}: expected 3 fields (<map-name> <system-username> <pg-username>), got {}",
                tokens.len()
            )));
        }

        let pg_user = tokens[2].as_str();
        if pg_user.starts_with('/') && !caps.hba_include_and_regex {
            return Err(PgError::ConfigValidation(format!(
                "pg_ident.conf line {line_no}: regular expression in pg-username '{pg_user}' requires PostgreSQL 16+"
            )));
        }
    }

    Ok(())
}

fn json_scalar_as_str(v: &JsonValue) -> String {
    match v {
        JsonValue::String(s) => s.clone(),
        JsonValue::Bool(b) => if *b { "on" } else { "off" }.to_owned(),
        JsonValue::Number(n) => n.to_string(),
        _ => v.to_string(),
    }
}

/// Validates version-gated PostgreSQL GUCs across **PG 15–18** (`promote_trigger_file`,
/// PG 17+ `sync_replication_slots` / `synchronized_standby_slots`, and PG 18+ Async I/O GUCs).
pub fn validate_version_specific_gucs(
    major: PgMajor,
    params: &BTreeMap<CompactString, JsonValue>,
) -> Result<(), PgError> {
    let caps = major.capabilities();

    if major != PgMajor::V15 && params.contains_key("promote_trigger_file") {
        return Err(PgError::ConfigValidation(format!(
            "GUC 'promote_trigger_file' was removed in PostgreSQL 16 (current: PG {})",
            major.as_u8()
        )));
    }

    if !caps.native_failover_slots {
        for guc in ["sync_replication_slots", "synchronized_standby_slots"] {
            if params.contains_key(guc) {
                return Err(PgError::ConfigValidation(format!(
                    "GUC '{guc}' requires PostgreSQL 17+ (current: PG {})",
                    major.as_u8()
                )));
            }
        }
    }

    for aio_guc in ["io_method", "io_combine_limit", "io_workers"] {
        if let Some(val) = params.get(aio_guc) {
            if !caps.hba_oauth_and_aio {
                return Err(PgError::ConfigValidation(format!(
                    "Async I/O GUC '{aio_guc}' requires PostgreSQL 18+ (current: PG {})",
                    major.as_u8()
                )));
            }
            let val_str = json_scalar_as_str(val);
            if aio_guc == "io_method" && !matches!(val_str.trim(), "sync" | "worker" | "io_uring") {
                return Err(PgError::ConfigValidation(format!(
                    "invalid PostgreSQL 18 io_method '{val_str}'; expected 'sync', 'worker', or 'io_uring'"
                )));
            }
            if aio_guc == "io_combine_limit"
                && val_str.trim().parse::<u64>().is_err()
                && parse_pg_memory_bytes(&val_str).is_err()
            {
                return Err(PgError::ConfigValidation(format!(
                    "invalid PostgreSQL 18 io_combine_limit '{val_str}'"
                )));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use proptest::prelude::*;
    use serde_json::json;

    use super::*;

    #[test]
    fn enforces_pg15_pg16_pg17_and_pg18_hba_matrix() {
        let classic = vec![
            "# Comment line".to_owned(),
            "local all all scram-sha-256".to_owned(),
            "host replication replicator 10.0.0.0 255.255.0.0 scram-sha-256".to_owned(),
        ];
        for v in [PgMajor::V15, PgMajor::V16, PgMajor::V17, PgMajor::V18] {
            assert!(validate_pg_hba(v, &classic).is_ok());
        }

        let include_and_regex = vec![
            "include_dir conf.d".to_owned(),
            "hostssl /^app_.+$/ /^role_.+$/ 10.24.0.0/16 scram-sha-256".to_owned(),
        ];
        assert!(validate_pg_hba(PgMajor::V15, &include_and_regex).is_err());
        assert!(validate_pg_hba(PgMajor::V16, &include_and_regex).is_ok());
        assert!(validate_pg_hba(PgMajor::V17, &include_and_regex).is_ok());
        assert!(validate_pg_hba(PgMajor::V18, &include_and_regex).is_ok());

        let oauth_missing_scope =
            vec!["hostssl all all 0.0.0.0/0 oauth issuer=https://auth.internal".to_owned()];
        assert!(validate_pg_hba(PgMajor::V18, &oauth_missing_scope).is_err());

        let oauth_valid = vec![
            "hostssl all all 0.0.0.0/0 oauth issuer=\"https://auth.internal\" scope=\"openid postgres\""
                .to_owned(),
        ];
        assert!(validate_pg_hba(PgMajor::V17, &oauth_valid).is_err());
        assert!(validate_pg_hba(PgMajor::V18, &oauth_valid).is_ok());
    }

    #[test]
    fn enforces_version_specific_aio_and_slot_gucs() {
        let mut aio = BTreeMap::new();
        aio.insert(CompactString::const_new("io_method"), json!("io_uring"));
        aio.insert(CompactString::const_new("io_combine_limit"), json!("256kB"));

        assert!(validate_version_specific_gucs(PgMajor::V17, &aio).is_err());
        assert!(validate_version_specific_gucs(PgMajor::V18, &aio).is_ok());

        aio.insert(CompactString::const_new("io_method"), json!("bogus"));
        assert!(validate_version_specific_gucs(PgMajor::V18, &aio).is_err());
    }

    proptest! {
        #[test]
        fn prop_hba_and_ident_validators_never_panic_on_arbitrary_lines(line in ".*") {
            let lines = [line];
            for major in [PgMajor::V15, PgMajor::V16, PgMajor::V17, PgMajor::V18] {
                let _ = validate_pg_hba(major, &lines);
                let _ = validate_pg_ident(major, &lines);
            }
        }
    }
}
