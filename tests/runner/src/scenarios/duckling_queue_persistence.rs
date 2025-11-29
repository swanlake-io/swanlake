use std::path::PathBuf;

use anyhow::{Context, Result};
use swanlake_client::FlightSQLClient;
use tracing::info;

use crate::scenarios::client_ext::FlightSqlClientExt;
use crate::scenarios::duckling_queue_utils::{buffer_path, wait_for_staging_rows};
use crate::CliArgs;

pub async fn run_duckling_queue_persistence(args: &CliArgs) -> Result<()> {
    info!("running duckling queue persistence scenario");
    let test_dir = args
        .test_dir
        .as_ref()
        .context("--test-dir is required for duckling queue persistence scenario")?;
    let attach_sql = format!(
        "ATTACH IF NOT EXISTS 'ducklake:postgres:dbname=swanlake_test' AS swanlake \
         (DATA_PATH '{test_dir}/swanlake_files', OVERRIDE_DATA_PATH true);"
    );

    let root = PathBuf::from(format!("{test_dir}/duckling_queue"));
    let buffer = buffer_path(&root);
    // Root is managed by the server; ensure it exists but don't clear it mid-run.
    std::fs::create_dir_all(&root)
        .with_context(|| format!("failed to ensure dq root {}", root.display()))?;

    let mut conn = FlightSQLClient::connect(&args.endpoint)?;
    conn.update(&attach_sql)?;
    conn.update("DROP TABLE IF EXISTS swanlake.dq_persist_target;")?;

    conn.update("CREATE TABLE swanlake.dq_persist_target (i INTEGER);")?;
    conn.update("INSERT INTO duckling_queue.dq_persist_target SELECT 1 AS id UNION ALL SELECT 2;")?;

    wait_for_staging_rows(&mut conn, &buffer, "dq_persist_target", |count| count >= 1).await?;

    conn.update("PRAGMA duckling_queue.flush;")?;

    let total = conn.query_scalar_i64("SELECT COUNT(*) FROM swanlake.dq_persist_target")?;
    assert_eq!(
        total, 2,
        "flushed data should land in swanlake.dq_persist_target"
    );

    wait_for_staging_rows(&mut conn, &buffer, "dq_persist_target", |count| count == 0).await?;

    conn.update("DROP TABLE IF EXISTS swanlake.dq_persist_target;")?;
    info!("duckling queue persistence scenario passed");
    Ok(())
}
