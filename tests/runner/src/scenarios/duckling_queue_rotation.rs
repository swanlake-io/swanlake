use anyhow::{Context, Result};
use flight_sql_client::FlightSQLClient;
use tracing::info;

use crate::CliArgs;

pub async fn run_duckling_queue_rotation(args: &CliArgs) -> Result<()> {
    info!("running duckling queue rotation scenario");
    let test_dir = args
        .test_dir()
        .context("--test-dir is required for duckling queue scenario")?;
    let attach_sql = format!(
        "ATTACH IF NOT EXISTS 'ducklake:postgres:dbname=swanlake_test' AS swanlake \
         (DATA_PATH '{test_dir}/swanlake_files', OVERRIDE_DATA_PATH true);"
    );

    let mut writer = FlightSQLClient::connect(args.endpoint())?;
    writer.execute_update(&attach_sql)?;
    let mut peer = FlightSQLClient::connect(args.endpoint())?;
    peer.execute_update(&attach_sql)?;

    writer
        .execute_update("DROP TABLE IF EXISTS duckling_queue.concurrent_case_a;")
        .ok();
    writer
        .execute_update("DROP TABLE IF EXISTS duckling_queue.concurrent_case_b;")
        .ok();
    writer
        .execute_update("DROP TABLE IF EXISTS duckling_queue.concurrent_case_post;")
        .ok();
    writer.execute_update("DROP TABLE IF EXISTS swanlake.concurrent_case_a;")?;
    writer.execute_update("DROP TABLE IF EXISTS swanlake.concurrent_case_b;")?;
    writer.execute_update("DROP TABLE IF EXISTS swanlake.concurrent_case_post;")?;

    writer.execute_update("CREATE TABLE duckling_queue.concurrent_case_a AS SELECT 1 AS i;")?;
    peer.execute_update("CREATE TABLE duckling_queue.concurrent_case_b AS SELECT 2 AS i;")?;

    assert_eq!(
        writer.query_scalar_i64("SELECT COUNT(*) FROM duckling_queue.concurrent_case_a")?,
        1,
        "writer sees its own table"
    );
    assert_eq!(
        peer.query_scalar_i64("SELECT COUNT(*) FROM duckling_queue.concurrent_case_b")?,
        1,
        "peer sees its own table"
    );

    writer.execute_update("PRAGMA duckling_queue.flush;")?;

    assert_eq!(
        peer.query_scalar_i64("SELECT COUNT(*) FROM swanlake.concurrent_case_a")?,
        1,
        "flushed data should be available in swanlake"
    );

    peer.execute_update("CREATE TABLE duckling_queue.concurrent_case_post AS SELECT 3 AS i")?;

    peer.execute_update("PRAGMA duckling_queue.flush;")?;

    assert_eq!(
        writer.query_scalar_i64("SELECT COUNT(*) FROM swanlake.concurrent_case_b")?,
        1,
        "flushed data should be available in swanlake"
    );
    assert_eq!(
        writer.query_scalar_i64("SELECT COUNT(*) FROM swanlake.concurrent_case_post")?,
        1,
        "flushed data should be available in swanlake"
    );

    writer.execute_update("DROP TABLE IF EXISTS swanlake.concurrent_case_a;")?;
    writer.execute_update("DROP TABLE IF EXISTS swanlake.concurrent_case_b;")?;
    writer.execute_update("DROP TABLE IF EXISTS swanlake.concurrent_case_post;")?;

    Ok(())
}
