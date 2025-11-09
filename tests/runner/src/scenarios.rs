use std::path::PathBuf;

use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_core::Connection;
use adbc_core::Database;
use adbc_core::Driver;
use adbc_core::Statement;
use adbc_driver_flightsql::DRIVER_PATH;
use adbc_driver_manager::{ManagedConnection, ManagedDriver};
use anyhow::{anyhow, bail, Context, Result};
use arrow_array::{
    Array, Int16Array, Int32Array, Int64Array, UInt16Array, UInt32Array, UInt64Array,
};
use tracing::info;

use crate::CliArgs;

pub fn requires_test_dir(args: &CliArgs) -> bool {
    !args.test_files().is_empty()
}

pub async fn run_all(args: &CliArgs) -> Result<()> {
    run_duckling_queue_rotation(args).await
}

async fn run_duckling_queue_rotation(args: &CliArgs) -> Result<()> {
    info!("running duckling queue rotation scenario");
    let test_dir = args
        .test_dir()
        .context("--test-dir is required for duckling queue scenario")?;
    let attach_sql = format!(
        "ATTACH 'ducklake:postgres:dbname=swanlake_test' AS swanlake \
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

struct SqlClient {
    connection: ManagedConnection,
}

impl SqlClient {
    async fn connect(endpoint: &str) -> Result<Self> {
        let driver_path = PathBuf::from(DRIVER_PATH);
        let mut driver =
            ManagedDriver::load_dynamic_from_filename(&driver_path, None, AdbcVersion::default())
                .with_context(|| {
                format!(
                    "failed to load Flight SQL driver from {}",
                    driver_path.display()
                )
            })?;

        let database = driver
            .new_database_with_opts([(OptionDatabase::Uri, OptionValue::from(endpoint))])
            .map_err(|err| anyhow!("failed to create database handle: {err}"))?;
        let connection = database
            .new_connection()
            .map_err(|err| anyhow!("failed to create connection: {err}"))?;

        Ok(Self { connection })
    }

    async fn exec(&mut self, sql: &str) -> Result<()> {
        info!("scenario exec: {sql}");
        let mut statement = self
            .connection
            .new_statement()
            .map_err(|err| anyhow!("failed to create statement: {err}"))?;
        statement
            .set_sql_query(sql)
            .map_err(|err| anyhow!("failed to set SQL query: {err}"))?;
        statement
            .execute_update()
            .map_err(|err| anyhow!("failed to execute statement: {err}"))?;
        Ok(())
    }

    async fn query_single_i64(&mut self, sql: &str) -> Result<i64> {
        info!("scenario query: {sql}");
        let mut statement = self
            .connection
            .new_statement()
            .map_err(|err| anyhow!("failed to create statement: {err}"))?;
        statement
            .set_sql_query(sql)
            .map_err(|err| anyhow!("failed to set SQL query: {err}"))?;
        let mut reader = statement
            .execute()
            .map_err(|err| anyhow!("failed to execute query: {err}"))?;
        let Some(batch) = reader.next().transpose()? else {
            bail!("query returned no rows");
        };
        if batch.num_rows() == 0 || batch.num_columns() == 0 {
            bail!("query returned empty result");
        }
        let column = batch.column(0);
        Ok(value_as_i64(column.as_ref(), 0)?)
    }
}

fn value_as_i64(column: &dyn Array, idx: usize) -> Result<i64> {
    if column.is_null(idx) {
        bail!("value is NULL");
    }
    if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
        return Ok(array.value(idx));
    }
    if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
        return Ok(array.value(idx) as i64);
    }
    if let Some(array) = column.as_any().downcast_ref::<Int16Array>() {
        return Ok(array.value(idx) as i64);
    }
    if let Some(array) = column.as_any().downcast_ref::<UInt64Array>() {
        return Ok(array.value(idx) as i64);
    }
    if let Some(array) = column.as_any().downcast_ref::<UInt32Array>() {
        return Ok(array.value(idx) as i64);
    }
    if let Some(array) = column.as_any().downcast_ref::<UInt16Array>() {
        return Ok(array.value(idx) as i64);
    }

    Err(anyhow!(
        "unsupported column type {} for integer projection",
        column.data_type()
    ))
}
