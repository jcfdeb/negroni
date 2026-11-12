#![forbid(unsafe_code)]

use std::env;
use std::process::ExitCode;

fn main() -> ExitCode {
    negroni::run_entrypoint(env::args().collect())
}
