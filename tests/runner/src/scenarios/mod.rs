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
    Array, Int16Array, Int32Array, Int64Array, RecordBatch, UInt16Array, UInt32Array, UInt64Array,
};
use tracing::info;

use crate::CliArgs;

pub mod duckling_queue_rotation;
pub mod parameter_types;

pub fn requires_test_dir(args: &CliArgs) -> bool {
    !args.test_files().is_empty()
}

pub async fn run_all(args: &CliArgs) -> Result<()> {
    duckling_queue_rotation::run_duckling_queue_rotation(args).await?;
    parameter_types::run_parameter_types(args).await
}

pub struct SqlClient {
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

    #[allow(dead_code)]
    pub async fn exec_prepared(&mut self, sql: &str, params: RecordBatch) -> Result<()> {
        info!("scenario exec prepared: {sql}");
        let mut statement = self
            .connection
            .new_statement()
            .map_err(|err| anyhow!("failed to create statement: {err}"))?;
        statement
            .set_sql_query(sql)
            .map_err(|err| anyhow!("failed to set SQL query: {err}"))?;
        statement
            .prepare()
            .map_err(|err| anyhow!("failed to prepare statement: {err}"))?;
        statement
            .bind(params)
            .map_err(|err| anyhow!("failed to bind params: {err}"))?;
        statement
            .execute_update()
            .map_err(|err| anyhow!("failed to execute prepared statement: {err}"))?;
        Ok(())
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

    pub async fn query_single_i64(&mut self, sql: &str) -> Result<i64> {
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

    #[allow(dead_code)]
    pub async fn query_single_string(&mut self, sql: &str) -> Result<String> {
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
        Ok(value_as_string(column.as_ref(), 0)?)
    }
}

pub fn value_as_i64(column: &dyn Array, idx: usize) -> Result<i64> {
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

pub fn value_as_string(column: &dyn Array, idx: usize) -> Result<String> {
    use arrow_array::StringArray;
    if column.is_null(idx) {
        bail!("value is NULL");
    }
    if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
        return Ok(array.value(idx).to_string());
    }
    Err(anyhow!(
        "unsupported column type {} for string projection",
        column.data_type()
    ))
}
