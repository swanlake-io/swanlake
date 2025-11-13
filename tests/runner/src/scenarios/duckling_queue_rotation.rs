use anyhow::{Context, Result};
use tracing::info;

use super::SqlClient;
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

    let mut writer = SqlClient::connect(args.endpoint()).await?;
    writer.exec(&attach_sql).await?;
    let mut peer = SqlClient::connect(args.endpoint()).await?;
    peer.exec(&attach_sql).await?;

    writer
        .exec("DROP TABLE IF EXISTS duckling_queue.concurrent_case_a;")
        .await
        .ok();
    writer
        .exec("DROP TABLE IF EXISTS duckling_queue.concurrent_case_b;")
        .await
        .ok();
    writer
        .exec("DROP TABLE IF EXISTS duckling_queue.concurrent_case_post;")
        .await
        .ok();
    writer
        .exec("DROP TABLE IF EXISTS swanlake.concurrent_case_a;")
        .await?;
    writer
        .exec("DROP TABLE IF EXISTS swanlake.concurrent_case_b;")
        .await?;
    writer
        .exec("DROP TABLE IF EXISTS swanlake.concurrent_case_post;")
        .await?;

    writer
        .exec("CREATE TABLE duckling_queue.concurrent_case_a AS SELECT 1 AS i;")
        .await?;
    peer.exec("CREATE TABLE duckling_queue.concurrent_case_b AS SELECT 2 AS i;")
        .await?;

    assert_eq!(
        writer
            .query_single_i64("SELECT COUNT(*) FROM duckling_queue.concurrent_case_a")
            .await?,
        1,
        "writer sees its own table"
    );
    assert_eq!(
        peer.query_single_i64("SELECT COUNT(*) FROM duckling_queue.concurrent_case_b")
            .await?,
        1,
        "peer sees its own table"
    );

    writer.exec("PRAGMA duckling_queue.flush;").await?;

    assert_eq!(
        peer.query_single_i64("SELECT COUNT(*) FROM swanlake.concurrent_case_a")
            .await?,
        1,
        "flushed data should be available in swanlake"
    );

    peer.exec("CREATE TABLE duckling_queue.concurrent_case_post AS SELECT 3 AS i")
        .await?;

    peer.exec("PRAGMA duckling_queue.flush;").await?;

    assert_eq!(
        writer
            .query_single_i64("SELECT COUNT(*) FROM swanlake.concurrent_case_b")
            .await?,
        1,
        "flushed data should be available in swanlake"
    );
    assert_eq!(
        writer
            .query_single_i64("SELECT COUNT(*) FROM swanlake.concurrent_case_post")
            .await?,
        1,
        "flushed data should be available in swanlake"
    );

    writer
        .exec("DROP TABLE IF EXISTS swanlake.concurrent_case_a;")
        .await?;
    writer
        .exec("DROP TABLE IF EXISTS swanlake.concurrent_case_b;")
        .await?;
    writer
        .exec("DROP TABLE IF EXISTS swanlake.concurrent_case_post;")
        .await?;

    Ok(())
}
