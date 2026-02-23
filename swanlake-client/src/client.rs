use std::sync::Arc;

use adbc_driver_manager::ManagedConnection;
use anyhow::Result;
use arrow_array::RecordBatch;
use arrow_schema::Schema;

use crate::driver::get_cached_driver;
use crate::internal::{
    execute_query, execute_query_with_params, execute_update, execute_update_with_batch,
    run_healthcheck,
};

/// A Flight SQL client for connecting to SwanLake servers.
///
/// This client wraps an ADBC-managed connection to a Flight SQL server,
/// providing high-level methods for executing queries and updates.
///
/// # Example
///
/// ```rust,ignore
/// use swanlake_client::FlightSQLClient;
///
/// let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;
/// let result = client.execute("SELECT 1 as col")?;
/// println!("Rows: {}", result.total_rows);
/// # Ok::<(), anyhow::Error>(())
/// ```
pub struct FlightSQLClient {
    conn: ManagedConnection,
}

impl Clone for FlightSQLClient {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
        }
    }
}

/// Result of executing a query.
///
/// Contains the Arrow record batches and metadata about the result set.
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// The record batches returned by the query.
    pub batches: Vec<RecordBatch>,
    /// Total number of rows across all batches.
    pub total_rows: usize,
    /// Rows affected reported by the server (if available).
    pub rows_affected: Option<i64>,
}

impl QueryResult {
    /// Create a new QueryResult from batches.
    ///
    /// # Example
    ///
    /// ```rust
    /// use swanlake_client::QueryResult;
    /// use arrow_array::RecordBatch;
    ///
    /// let batches = vec![]; // Assume some batches
    /// let result = QueryResult::new(batches);
    /// ```
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        let total_rows = batches.iter().map(|b| b.num_rows()).sum();
        Self {
            batches,
            total_rows,
            rows_affected: None,
        }
    }

    /// Create a QueryResult with an explicit rows-affected value.
    pub fn with_rows_affected(batches: Vec<RecordBatch>, rows_affected: Option<i64>) -> Self {
        let total_rows = batches.iter().map(|b| b.num_rows()).sum();
        Self {
            batches,
            total_rows,
            rows_affected,
        }
    }

    /// Get the schema from the first batch (if available).
    ///
    /// Returns `None` if there are no batches.
    pub fn schema(&self) -> Option<Arc<Schema>> {
        self.batches.first().map(|b| b.schema())
    }

    /// Check if the result is empty.
    ///
    /// Returns `true` if no rows were returned.
    pub fn is_empty(&self) -> bool {
        self.total_rows == 0
    }
}

/// Result of executing an update/DDL statement.
///
/// Contains the number of rows affected by the operation, if reported by the server.
#[derive(Debug, Clone)]
pub struct UpdateResult {
    /// Number of rows affected (if available).
    pub rows_affected: Option<i64>,
}

impl FlightSQLClient {
    /// Connect to a SwanLake Flight SQL server.
    ///
    /// Establishes a connection and tests it with a simple query.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use swanlake_client::FlightSQLClient;
    ///
    /// let client = FlightSQLClient::connect("grpc://localhost:4214")?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn connect(endpoint: &str) -> Result<Self> {
        let driver = get_cached_driver()?;
        let mut conn = driver.new_connection(endpoint)?;
        // Test the connection by executing a simple query.
        run_healthcheck(&mut conn, "SELECT 1")?;
        Ok(Self { conn })
    }

    /// Execute a SQL query and return the full result set.
    /// Execute a query and return results. Use when you expect rows.
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        execute_query(&mut self.conn, sql)
    }

    /// Execute a SQL statement, returning results if any.
    /// Convenience wrapper that always calls `query`. The server accepts
    /// commands via ExecuteQuery, so callers that are unsure can use this
    /// method.
    pub fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        self.query(sql)
    }

    /// Execute an update/DDL statement and return rows affected if available.
    /// Execute an update/DDL statement (INSERT, UPDATE, DELETE, CREATE, etc.).
    pub fn update(&mut self, sql: &str) -> Result<UpdateResult> {
        execute_update(&mut self.conn, sql)
    }

    /// Execute a query with parameters using a prepared statement.
    ///
    /// The parameters are provided as a `RecordBatch`, allowing callers to bind
    /// typed values directly.
    pub fn query_with_param(&mut self, sql: &str, params: RecordBatch) -> Result<QueryResult> {
        execute_query_with_params(&mut self.conn, sql, params)
    }

    /// Execute an update with a record batch for batch inserts or parameterized DML.
    pub fn update_with_record_batch(
        &mut self,
        sql: &str,
        batch: RecordBatch,
    ) -> Result<UpdateResult> {
        execute_update_with_batch(&mut self.conn, sql, batch)
    }

    /// Get a reference to the underlying ADBC connection.
    ///
    /// Use this for advanced operations not covered by the high-level API.
    pub fn connection(&mut self) -> &mut ManagedConnection {
        &mut self.conn
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn test_query_result_empty() {
        let result = QueryResult::new(vec![]);
        assert!(result.is_empty());
        assert_eq!(result.total_rows, 0);
    }

    #[test]
    fn test_query_result_schema_and_rows_affected_helpers() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2]))])?;
        let result = QueryResult::with_rows_affected(vec![batch], Some(2));

        assert!(!result.is_empty());
        assert_eq!(result.total_rows, 2);
        assert_eq!(result.rows_affected, Some(2));
        assert!(result.schema().is_some());
        Ok(())
    }
}
