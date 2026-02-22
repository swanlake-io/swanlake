use std::env;
use std::io::{self, Write};

fn emit(line: impl AsRef<str>) -> io::Result<()> {
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "{}", line.as_ref())
}

fn main() -> io::Result<()> {
    if let Some(lib_dir) = env::var_os("DEP_SWANLAKE_DUCKDB_LIB_DIR") {
        let lib_dir = lib_dir.to_string_lossy();
        emit(format!("cargo:rustc-link-arg=-Wl,-rpath,{lib_dir}"))?;
        emit("cargo:rerun-if-env-changed=DEP_SWANLAKE_DUCKDB_LIB_DIR")?;
    } else {
        emit("cargo:warning=DEP_SWANLAKE_DUCKDB_LIB_DIR missing; DuckDB may not load at runtime")?;
    }

    Ok(())
}
