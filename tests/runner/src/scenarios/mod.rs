use adbc_core::{Connection, Statement};
use adbc_driver_manager::ManagedConnection;
use anyhow::{anyhow, bail, Context, Result};
use arrow_array::RecordBatch;
use flight_sql_client::FlightSqlConnectionBuilder;
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
        let connection = FlightSqlConnectionBuilder::new(endpoint)
            .connect()
            .with_context(|| format!("failed to connect to {endpoint}"))?;

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

pub use flight_sql_client::arrow::{value_as_i64, value_as_string};
