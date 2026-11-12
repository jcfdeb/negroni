#![no_main]

use libfuzzer_sys::fuzz_target;
use negroni_pg::hba_validator::{validate_pg_hba, validate_pg_ident};
use negroni_pg::rewind::parse_timeline_history;
use negroni_pg::version::PgMajor;

fuzz_target!(|data: &[u8]| {
    if let Ok(text) = std::str::from_utf8(data) {
        let lines: Vec<String> = text.lines().map(ToOwned::to_owned).collect();
        for major in [PgMajor::V15, PgMajor::V16, PgMajor::V17, PgMajor::V18] {
            let _ = validate_pg_hba(major, &lines);
            let _ = validate_pg_ident(major, &lines);
        }
        let _ = parse_timeline_history(text);
    }
});
