use std::path::PathBuf;

use anyhow::{Context, Result};
use flight_sql_client::FlightSQLClient;
use tracing::info;

use crate::scenarios::duckling_queue_utils::{reset_dir, wait_for_parquet_chunks};
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
    reset_dir(&root)?;

    let mut conn = FlightSQLClient::connect(&args.endpoint)?;
    conn.execute_update(&attach_sql)?;
    conn.execute_update("DROP TABLE IF EXISTS swanlake.dq_persist_target;")?;

    conn.execute_update("CREATE TABLE swanlake.dq_persist_target (i INTEGER);")?;
    conn.execute_update(
        "INSERT INTO duckling_queue.dq_persist_target SELECT 1 AS id UNION ALL SELECT 2;",
    )?;

    wait_for_parquet_chunks(&root, |count| count >= 1).await?;

    conn.execute_update("PRAGMA duckling_queue.flush;")?;

    let total = conn.query_scalar_i64("SELECT COUNT(*) FROM swanlake.dq_persist_target")?;
    assert_eq!(
        total, 2,
        "flushed data should land in swanlake.dq_persist_target"
    );

    wait_for_parquet_chunks(&root, |count| count == 0).await?;

    conn.execute_update("DROP TABLE IF EXISTS swanlake.dq_persist_target;")?;
    info!("duckling queue persistence scenario passed");
    Ok(())
}
