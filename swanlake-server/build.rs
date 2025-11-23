use std::env;

fn main() {
    if let Some(lib_dir) = env::var_os("DEP_SWANLAKE_DUCKDB_LIB_DIR") {
        let lib_dir = lib_dir.to_string_lossy();
        println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir);
        println!("cargo:rerun-if-env-changed=DEP_SWANLAKE_DUCKDB_LIB_DIR");
    } else {
        println!(
            "cargo:warning=DEP_SWANLAKE_DUCKDB_LIB_DIR missing; DuckDB may not load at runtime"
        );
    }
}
