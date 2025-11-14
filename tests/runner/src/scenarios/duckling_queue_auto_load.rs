use anyhow::{Context, Result};
use tracing::info;

use super::SqlClient;
use crate::CliArgs;

pub async fn run_duckling_queue_auto_load(args: &CliArgs) -> Result<()> {
    info!("running duckling queue auto-load scenario");
    let test_dir = args
        .test_dir()
        .context("--test-dir is required for duckling queue scenario")?;
    let attach_sql = format!(
        "ATTACH IF NOT EXISTS 'ducklake:postgres:dbname=swanlake_test' AS swanlake \
         (DATA_PATH '{test_dir}/swanlake_files', OVERRIDE_DATA_PATH true);"
    );

    // First connection: Create table in target schema
    let mut conn1 = SqlClient::connect(args.endpoint())?;
    conn1.exec(&attach_sql)?;

    // Clean up any existing table
    conn1
        .exec("DROP TABLE IF EXISTS swanlake.auto_load_test;")
        .ok();

    // Create table in target schema
    conn1.exec("CREATE TABLE swanlake.auto_load_test (id INTEGER, value VARCHAR);")?;
    conn1.exec("INSERT INTO swanlake.auto_load_test VALUES (1, 'initial');")?;

    // Drop the first connection
    drop(conn1);

    // Second connection: Should have the table schema auto-loaded in duckling_queue
    // Verify the data is in duckling_queue
    let mut conn2 = SqlClient::connect(args.endpoint())?;
    conn2.exec(&attach_sql)?;

    let count = conn2.query_single_i64("SELECT COUNT(*) FROM swanlake.auto_load_test")?;
    assert_eq!(count, 1, "Should have 1 row in swanlake.auto_load_test");

    // This should work without creating the table in duckling_queue first
    conn2.exec("INSERT INTO duckling_queue.auto_load_test VALUES (2, 'from_queue');")?;

    // Verify the data is in duckling_queue
    let count = conn2.query_single_i64("SELECT COUNT(*) FROM duckling_queue.auto_load_test")?;
    assert_eq!(
        count, 1,
        "Should have 1 row in duckling_queue.auto_load_test"
    );

    // Flush to target
    conn2.exec("PRAGMA duckling_queue.flush;")?;

    // Verify both records are in the target schema
    let total = conn2.query_single_i64("SELECT COUNT(*) FROM swanlake.auto_load_test")?;
    assert_eq!(
        total, 2,
        "Should have 2 rows in swanlake.auto_load_test after flush"
    );

    // Clean up
    conn2.exec("DROP TABLE IF EXISTS swanlake.auto_load_test;")?;

    info!("duckling queue auto-load scenario passed");
    Ok(())
}
