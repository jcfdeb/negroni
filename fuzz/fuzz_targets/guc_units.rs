#![no_main]

use libfuzzer_sys::fuzz_target;
use negroni_pg::guc_units::{
    canonicalize_guc, guc_values_equivalent, parse_pg_duration_micros, parse_pg_memory_bytes,
};

fuzz_target!(|data: &[u8]| {
    if let Ok(text) = std::str::from_utf8(data) {
        let _ = parse_pg_memory_bytes(text);
        let _ = parse_pg_duration_micros(text);
        let _ = canonicalize_guc(text, Some("8kB"));
        let _ = guc_values_equivalent(text, "128MB", Some("8kB"));
    }
});
