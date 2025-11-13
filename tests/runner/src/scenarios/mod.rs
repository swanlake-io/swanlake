use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use flight_sql_client::FlightSQLClient;

use crate::CliArgs;

pub mod duckling_queue_rotation;
pub mod parameter_types;

pub async fn run_all(args: &CliArgs) -> Result<()> {
    duckling_queue_rotation::run_duckling_queue_rotation(args).await?;
    parameter_types::run_parameter_types(args).await?;
    Ok(())
}

pub struct SqlClient {
    client: FlightSQLClient,
}

impl SqlClient {
    pub fn connect(endpoint: &str) -> Result<Self> {
        let client = FlightSQLClient::connect(endpoint)
            .with_context(|| format!("failed to connect to {endpoint}"))?;
        Ok(Self { client })
    }

    pub fn exec_prepared(&mut self, sql: &str, params: RecordBatch) -> Result<()> {
        self.client.execute_batch_update(sql, params)?;
        Ok(())
    }

    pub fn exec(&mut self, sql: &str) -> Result<()> {
        self.client.exec(sql)?;
        Ok(())
    }

    pub fn query_single_i64(&mut self, sql: &str) -> Result<i64> {
        self.client.query_scalar_i64(sql)
    }
}
