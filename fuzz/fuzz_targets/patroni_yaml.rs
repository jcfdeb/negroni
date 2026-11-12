#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(yaml_str) = std::str::from_utf8(data) {
        let empty_env: [(&str, &str); 0] = [];
        let _ = negroni_config::parse_and_validate_with_env(yaml_str, "fuzz.yml", empty_env);
    }
});
