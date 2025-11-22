use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use flight_sql_client::FlightSQLClient;
use tracing::info;

use crate::scenarios::duckling_queue_utils::{reset_dir, wait_for_parquet_chunks};
use crate::CliArgs;

pub async fn run_duckling_queue_dlq(args: &CliArgs) -> Result<()> {
    info!("running duckling queue DLQ offload scenario");
    let test_dir = args
        .test_dir
        .as_ref()
        .context("--test-dir is required for duckling queue DLQ scenario")?;

    let dq_root = PathBuf::from(
        env::var("SWANLAKE_DUCKLING_QUEUE_ROOT")
            .unwrap_or_else(|_| format!("{test_dir}/duckling_queue")),
    );
    let dlq_dir = PathBuf::from(
        env::var("SWANLAKE_DUCKLING_QUEUE_DLQ_TARGET")
            .unwrap_or_else(|_| format!("{test_dir}/duckling_dlq")),
    );

    reset_dir(&dq_root)?;
    reset_dir(&dlq_dir)?;

    let attach_sql = format!(
        "ATTACH IF NOT EXISTS 'ducklake:postgres:dbname=swanlake_test' AS swanlake \
         (DATA_PATH '{test_dir}/swanlake_files', OVERRIDE_DATA_PATH true);"
    );

    let mut conn = FlightSQLClient::connect(&args.endpoint)?;
    conn.execute_update(&attach_sql)?;
    conn.execute_update("DROP TABLE IF EXISTS swanlake.dq_dlq_target;")?;

    // Insert into duckling_queue; the target table doesn't exist so the flush will fail.
    conn.execute_update("INSERT INTO duckling_queue.dq_dlq_target SELECT 1 AS id;")?;

    wait_for_parquet_chunks(&dq_root, |count| count >= 1).await?;

    // Force a flush; this will fail because the target table is missing, triggering DLQ copy.
    conn.execute_update("PRAGMA duckling_queue.flush;")?;

    // After offload, local buffer should be cleaned up.
    wait_for_parquet_chunks(&dq_root, |count| count == 0).await?;

    let copied = count_dlq_chunks(&dlq_dir.join("dq_dlq_target"))?;
    assert!(
        copied > 0,
        "expected DLQ to contain copied chunks for dq_dlq_target"
    );

    info!("duckling queue DLQ offload scenario passed");
    Ok(())
}

fn count_dlq_chunks(root: &Path) -> Result<usize> {
    if !root.exists() {
        return Ok(0);
    }
    let mut count = 0usize;
    for entry in
        fs::read_dir(root).with_context(|| format!("failed to read DLQ dir {}", root.display()))?
    {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            count += count_dlq_chunks(&entry.path())?;
            continue;
        }
        let path = entry.path();
        let ext = path.extension().and_then(|ext| ext.to_str());
        if matches!(ext, Some("arrow") | Some("parquet")) {
            count += 1;
        }
    }
    Ok(count)
}
