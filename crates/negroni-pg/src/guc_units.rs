use crate::PgError;

/// Canonicalized PostgreSQL GUC scalar value used to diff desired `patroni.yml` / DCS `/config`
/// parameters against `pg_catalog.pg_settings` without false-positive reloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonicalGucValue {
    Bytes(u64),
    Microseconds(u64),
    Bool(bool),
    Integer(i64),
    Text(String),
}

fn unquote_guc(raw: &str) -> &str {
    let trimmed = raw.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('\'') && trimmed.ends_with('\'') {
        &trimmed[1..trimmed.len() - 1]
    } else {
        trimmed
    }
}

fn memory_unit_multiplier_bytes(unit: &str) -> Option<u64> {
    match unit {
        "B" => Some(1),
        "kB" => Some(1024),
        "8kB" => Some(8 * 1024),
        "MB" => Some(1024 * 1024),
        "16MB" => Some(16 * 1024 * 1024),
        "GB" => Some(1024 * 1024 * 1024),
        "TB" => Some(1024_u64.pow(4)),
        _ => None,
    }
}

fn duration_unit_multiplier_micros(unit: &str) -> Option<u64> {
    match unit {
        "us" => Some(1),
        "ms" => Some(1_000),
        "s" => Some(1_000_000),
        "min" => Some(60 * 1_000_000),
        "h" => Some(3_600 * 1_000_000),
        "d" => Some(86_400 * 1_000_000),
        _ => None,
    }
}

/// Splits a GUC literal such as `"128MB"`, `"16384 8kB"`, or `"1.5GB"` into `(number_str, unit_str)`.
fn split_number_and_unit(input: &str) -> Option<(&str, &str)> {
    let s = unquote_guc(input).trim();
    if s.is_empty() {
        return None;
    }

    // Find the end of the leading numeric portion (optional `+`, digits, at most one `.`).
    let bytes = s.as_bytes();
    let mut idx = 0;
    if bytes.first() == Some(&b'+') {
        idx += 1;
    }
    let num_start = idx;
    let mut saw_digit = false;
    let mut saw_dot = false;
    while idx < bytes.len() {
        let b = bytes[idx];
        if b.is_ascii_digit() {
            saw_digit = true;
            idx += 1;
        } else if b == b'.' && !saw_dot {
            saw_dot = true;
            idx += 1;
        } else {
            break;
        }
    }
    if !saw_digit || idx == num_start {
        return None;
    }

    // Special case: `"16384 8kB"` or `"8 16MB"` has whitespace before a digit-prefixed unit;
    // whereas `"1288kB"` without space is number `1288` + unit `kB`.
    let num_part = &s[..idx];
    let unit_part = s[idx..].trim();
    if unit_part.is_empty() {
        return None;
    }
    Some((num_part, unit_part))
}

fn scale_decimal_to_u64(num_str: &str, multiplier: u64) -> Option<u64> {
    if let Some((whole_s, frac_s)) = num_str.split_once('.') {
        let whole: u128 = if whole_s.is_empty() || whole_s == "+" {
            0
        } else {
            whole_s.parse().ok()?
        };
        if frac_s.len() > 18 {
            return None;
        }
        let frac: u128 = if frac_s.is_empty() {
            0
        } else {
            frac_s.parse().ok()?
        };
        let denom = 10_u128.checked_pow(u32::try_from(frac_s.len()).ok()?)?;
        let mult = u128::from(multiplier);
        let whole_scaled = whole.checked_mul(mult)?;
        // PostgreSQL rounds half-away-from-zero when converting fractional unit GUCs.
        let frac_numer = frac.checked_mul(mult)?.checked_add(denom / 2)?;
        let total = whole_scaled.checked_add(frac_numer / denom)?;
        u64::try_from(total).ok()
    } else {
        let whole: u64 = num_str.parse().ok()?;
        whole.checked_mul(multiplier)
    }
}

/// Parses a PostgreSQL memory size GUC (e.g. `"16GB"`, `"1024MB"`, `"16384 8kB"`) into exact bytes.
///
/// # Examples
///
/// ```
/// use negroni_pg::guc_units::parse_pg_memory_bytes;
///
/// assert_eq!(parse_pg_memory_bytes("1GB")?, 1_073_741_824);
/// assert_eq!(parse_pg_memory_bytes("1024MB")?, 1_073_741_824);
/// assert_eq!(parse_pg_memory_bytes("131072 8kB")?, 1_073_741_824);
/// # Ok::<(), negroni_pg::PgError>(())
/// ```
pub fn parse_pg_memory_bytes(raw: &str) -> Result<u64, PgError> {
    let (num_part, unit_part) = split_number_and_unit(raw).ok_or_else(|| {
        PgError::ConfigValidation(format!("invalid PostgreSQL memory GUC literal '{raw}'"))
    })?;
    let mult = memory_unit_multiplier_bytes(unit_part).ok_or_else(|| {
        PgError::ConfigValidation(format!(
            "unsupported PostgreSQL memory unit '{unit_part}' in '{raw}'"
        ))
    })?;
    scale_decimal_to_u64(num_part, mult).ok_or_else(|| {
        PgError::ConfigValidation(format!("overflow parsing PostgreSQL memory GUC '{raw}'"))
    })
}

/// Parses a PostgreSQL duration GUC (e.g. `"30s"`, `"500ms"`, `"5min"`, `"1h"`) into microseconds.
///
/// # Examples
///
/// ```
/// use negroni_pg::guc_units::parse_pg_duration_micros;
///
/// assert_eq!(parse_pg_duration_micros("2min")?, 120_000_000);
/// assert_eq!(parse_pg_duration_micros("120s")?, 120_000_000);
/// assert_eq!(parse_pg_duration_micros("250ms")?, 250_000);
/// # Ok::<(), negroni_pg::PgError>(())
/// ```
pub fn parse_pg_duration_micros(raw: &str) -> Result<u64, PgError> {
    let (num_part, unit_part) = split_number_and_unit(raw).ok_or_else(|| {
        PgError::ConfigValidation(format!("invalid PostgreSQL duration GUC literal '{raw}'"))
    })?;
    let mult = duration_unit_multiplier_micros(unit_part).ok_or_else(|| {
        PgError::ConfigValidation(format!(
            "unsupported PostgreSQL duration unit '{unit_part}' in '{raw}'"
        ))
    })?;
    scale_decimal_to_u64(num_part, mult).ok_or_else(|| {
        PgError::ConfigValidation(format!("overflow parsing PostgreSQL duration GUC '{raw}'"))
    })
}

/// Normalizes PostgreSQL boolean GUC strings (`on`/`off`, `true`/`false`, `yes`/`no`, `1`/`0`).
pub fn parse_pg_bool(raw: &str) -> Option<bool> {
    match unquote_guc(raw).to_ascii_lowercase().as_str() {
        "on" | "true" | "tru" | "tr" | "t" | "yes" | "ye" | "y" | "1" => Some(true),
        "off" | "of" | "false" | "fals" | "fal" | "fa" | "f" | "no" | "n" | "0" => Some(false),
        _ => None,
    }
}

/// Canonicalizes a GUC string paired with an optional `pg_settings.unit` column.
pub fn canonicalize_guc(raw_val: &str, implicit_unit: Option<&str>) -> CanonicalGucValue {
    let cleaned = unquote_guc(raw_val).trim();
    if let Some(unit) = implicit_unit.map(str::trim).filter(|u| !u.is_empty()) {
        // If `raw_val` does not already carry an explicit unit suffix, combine with `pg_settings.unit`.
        if split_number_and_unit(cleaned).is_none() {
            // Negative settings such as `-1` disable limits regardless of unit (`wal_keep_size = -1`).
            if let Ok(neg) = cleaned.parse::<i64>()
                && neg < 0
            {
                return CanonicalGucValue::Integer(neg);
            }
            let combined = format!("{cleaned} {unit}");
            if let Ok(bytes) = parse_pg_memory_bytes(&combined) {
                return CanonicalGucValue::Bytes(bytes);
            }
            if let Ok(micros) = parse_pg_duration_micros(&combined) {
                return CanonicalGucValue::Microseconds(micros);
            }
        }
    }

    if let Ok(bytes) = parse_pg_memory_bytes(cleaned) {
        return CanonicalGucValue::Bytes(bytes);
    }
    if let Ok(micros) = parse_pg_duration_micros(cleaned) {
        return CanonicalGucValue::Microseconds(micros);
    }
    if let Some(b) = parse_pg_bool(cleaned) {
        return CanonicalGucValue::Bool(b);
    }
    if let Ok(int_val) = cleaned.parse::<i64>() {
        return CanonicalGucValue::Integer(int_val);
    }
    CanonicalGucValue::Text(cleaned.to_owned())
}

/// Compares a desired GUC value from `patroni.yml` / DCS `/config` against a live row from
/// `pg_catalog.pg_settings` (`setting`, `unit`), returning `true` when both denote the exact
/// same effective setting without requiring a reload or restart.
pub fn guc_values_equivalent(
    desired_raw: &str,
    pg_setting_val: &str,
    pg_setting_unit: Option<&str>,
) -> bool {
    let lhs = canonicalize_guc(desired_raw, pg_setting_unit);
    let rhs = canonicalize_guc(pg_setting_val, pg_setting_unit);
    lhs == rhs
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn memory_and_duration_equivalences_match_pg_settings_units() {
        // `shared_buffers = 1GB` vs `pg_settings` (`setting = "131072"`, `unit = "8kB"`)
        assert!(guc_values_equivalent("1GB", "131072", Some("8kB")));
        assert!(guc_values_equivalent("1024MB", "1GB", None));
        // `max_wal_size = 2GB` vs `pg_settings` (`setting = "128"`, `unit = "16MB"`)
        assert!(guc_values_equivalent("2GB", "128", Some("16MB")));
        // `checkpoint_timeout = 5min` vs `pg_settings` (`setting = "300"`, `unit = "s"`)
        assert!(guc_values_equivalent("5min", "300", Some("s")));
        assert!(guc_values_equivalent("500ms", "0.5s", None));
        // Booleans and `-1` sentinel values
        assert!(guc_values_equivalent("true", "on", None));
        assert!(guc_values_equivalent("-1", "-1", Some("MB")));
        assert!(!guc_values_equivalent("2GB", "1GB", None));
    }

    proptest! {
        #[test]
        fn prop_memory_unit_algebra_is_exact(gb in 0_u64..1024) {
            let from_gb = parse_pg_memory_bytes(&format!("{gb}GB")).unwrap();
            let from_mb = parse_pg_memory_bytes(&format!("{}MB", gb * 1024)).unwrap();
            let from_8kb = parse_pg_memory_bytes(&format!("{} 8kB", gb * 131_072)).unwrap();
            prop_assert_eq!(from_gb, from_mb);
            prop_assert_eq!(from_gb, from_8kb);
        }

        #[test]
        fn prop_arbitrary_guc_strings_never_panic(
            raw in ".*",
            setting in ".*",
            unit in proptest::option::of(".*"),
        ) {
            let _ = guc_values_equivalent(&raw, &setting, unit.as_deref());
        }
    }
}
