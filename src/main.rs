use std::io;
use std::process::ExitCode;

fn sub() -> Result<(), io::Error> {
    rs_avro_enum_str2num::str2num::converted2stdout()
}

fn main() -> ExitCode {
    sub().map(|_| ExitCode::SUCCESS).unwrap_or_else(|e| {
        eprintln!("{e}");
        ExitCode::FAILURE
    })
}
