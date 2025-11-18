use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use flight_sql_client::FlightSQLClient;
use tokio::time::sleep;
use tracing::info;

use crate::CliArgs;

pub async fn run_duckling_queue_persistence(args: &CliArgs) -> Result<()> {
    info!("running duckling queue persistence scenario");
    let test_dir = args
        .test_dir()
        .context("--test-dir is required for duckling queue persistence scenario")?;
    let attach_sql = format!(
        "ATTACH IF NOT EXISTS 'ducklake:postgres:dbname=swanlake_test' AS swanlake \
         (DATA_PATH '{test_dir}/swanlake_files', OVERRIDE_DATA_PATH true);"
    );

    let root = PathBuf::from(format!("{test_dir}/duckling_queue"));
    reset_duckling_root(&root)?;

    let mut conn = FlightSQLClient::connect(args.endpoint())?;
    conn.execute_update(&attach_sql)?;
    conn.execute_update("DROP TABLE IF EXISTS swanlake.dq_persist_target;")?;

    conn.execute_update(
        "CREATE TABLE duckling_queue.dq_persist_target AS SELECT 1 AS id UNION ALL SELECT 2;",
    )?;

    wait_for_arrow_chunks(&root, |count| count >= 1).await?;

    conn.execute_update("PRAGMA duckling_queue.flush;")?;

    let total = conn.query_scalar_i64("SELECT COUNT(*) FROM swanlake.dq_persist_target")?;
    assert_eq!(
        total, 2,
        "flushed data should land in swanlake.dq_persist_target"
    );

    wait_for_arrow_chunks(&root, |count| count == 0).await?;

    conn.execute_update("DROP TABLE IF EXISTS swanlake.dq_persist_target;")?;
    info!("duckling queue persistence scenario passed");
    Ok(())
}

fn reset_duckling_root(root: &Path) -> Result<()> {
    if root.exists() {
        fs::remove_dir_all(root).with_context(|| {
            format!(
                "failed to clear duckling queue root before persistence test: {}",
                root.display()
            )
        })?;
    }
    fs::create_dir_all(root).with_context(|| {
        format!(
            "failed to initialize duckling queue root before persistence test: {}",
            root.display()
        )
    })
}

async fn wait_for_arrow_chunks<F>(root: &Path, condition: F) -> Result<()>
where
    F: Fn(usize) -> bool,
{
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let count = count_arrow_chunks(root)?;
        if condition(count) {
            return Ok(());
        }
        if std::time::Instant::now() > deadline {
            return Err(anyhow!(
                "timed out waiting for duckling queue chunks (last count: {count})"
            ));
        }
        sleep(Duration::from_millis(50)).await;
    }
}

fn count_arrow_chunks(root: &Path) -> Result<usize> {
    if !root.exists() {
        return Ok(0);
    }
    let mut count = 0usize;
    for entry in fs::read_dir(root)
        .with_context(|| format!("failed to read duckling queue root {}", root.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        for chunk in fs::read_dir(entry.path())? {
            let chunk = chunk?;
            if chunk.path().extension().and_then(|ext| ext.to_str()) == Some("arrow") {
                count += 1;
            }
        }
    }
    Ok(count)
}
