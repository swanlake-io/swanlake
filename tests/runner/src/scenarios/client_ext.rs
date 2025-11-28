use anyhow::{bail, Context, Result};
use arrow_array::Int64Array;
use swanlake_client::FlightSQLClient;

/// Convenience helpers for working with FlightSQLClient in tests.
pub trait FlightSqlClientExt {
    fn query_scalar_i64(&mut self, sql: &str) -> Result<i64>;
}

impl FlightSqlClientExt for FlightSQLClient {
    fn query_scalar_i64(&mut self, sql: &str) -> Result<i64> {
        let result = self.query(sql)?;
        let batch = result
            .batches
            .into_iter()
            .next()
            .context("expected at least one batch for scalar query")?;
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("expected Int64 scalar result")?;
        if col.is_empty() {
            bail!("scalar query returned no rows");
        }
        Ok(col.value(0))
    }
}
