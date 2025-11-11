//! SwanLake Flight SQL Client Library
//!
//! This library provides a convenient client for connecting to and querying
//! SwanLake Flight SQL servers using ADBC.

use std::sync::Arc;

use adbc_core::{
    options::{AdbcVersion, OptionDatabase, OptionValue},
    Connection, Database, Driver, Statement,
};
use adbc_driver_flightsql::DRIVER_PATH;
use adbc_driver_manager::{ManagedConnection, ManagedDriver};
use anyhow::Result;
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

/// A Flight SQL client for connecting to SwanLake servers
pub struct FlightSQLClient {
    conn: ManagedConnection,
}

/// Result of executing a query
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// The record batches returned by the query
    pub batches: Vec<RecordBatch>,
    /// Total number of rows across all batches
    pub total_rows: usize,
}

impl QueryResult {
    /// Create a new QueryResult from batches
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        let total_rows = batches.iter().map(|b| b.num_rows()).sum();
        Self {
            batches,
            total_rows,
        }
    }

    /// Get the schema from the first batch (if available)
    pub fn schema(&self) -> Option<Arc<Schema>> {
        self.batches.first().map(|b| b.schema())
    }

    /// Check if the result is empty
    pub fn is_empty(&self) -> bool {
        self.total_rows == 0
    }
}

/// Result of executing an update/DDL statement
#[derive(Debug, Clone)]
pub struct UpdateResult {
    /// Number of rows affected (if available)
    pub rows_affected: Option<i64>,
}

impl FlightSQLClient {
    /// Connect to a SwanLake Flight SQL server
    ///
    /// # Arguments
    /// * `endpoint` - The server endpoint (e.g., "grpc://localhost:4214")
    ///
    /// # Example
    /// ```no_run
    /// use swanlake_client::FlightSQLClient;
    ///
    /// let client = FlightSQLClient::connect("grpc://localhost:4214")?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn connect(endpoint: &str) -> Result<Self> {
        let driver_path = std::path::PathBuf::from(DRIVER_PATH);
        let mut driver =
            ManagedDriver::load_dynamic_from_filename(&driver_path, None, AdbcVersion::default())?;
        let database =
            driver.new_database_with_opts([(OptionDatabase::Uri, OptionValue::from(endpoint))])?;
        let mut conn = database.new_connection()?;
        // Test the connection by executing a simple query
        let mut stmt = conn.new_statement()?;
        stmt.set_sql_query("SELECT 1")?;
        let _reader = stmt.execute()?;
        // Discard the result, just to verify connection
        Ok(Self { conn })
    }

    /// Execute a query and return results
    ///
    /// # Arguments
    /// * `sql` - The SQL query to execute
    ///
    /// # Example
    /// ```no_run
    /// # use swanlake_client::FlightSQLClient;
    /// # let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;
    /// let result = client.execute("SELECT * FROM my_table")?;
    /// println!("Returned {} rows", result.total_rows);
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn execute(&mut self, sql: &str) -> Result<QueryResult> {
        let mut stmt = self.conn.new_statement()?;
        stmt.set_sql_query(sql)?;
        let reader = stmt.execute()?;

        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch?);
        }

        Ok(QueryResult::new(batches))
    }

    /// Execute an update/DDL statement (INSERT, UPDATE, DELETE, CREATE, etc.)
    ///
    /// # Arguments
    /// * `sql` - The SQL statement to execute
    ///
    /// # Example
    /// ```no_run
    /// # use swanlake_client::FlightSQLClient;
    /// # let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;
    /// client.execute_update("CREATE TABLE test (id INT, name VARCHAR)")?;
    /// client.execute_update("INSERT INTO test VALUES (1, 'Alice')")?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn execute_update(&mut self, sql: &str) -> Result<UpdateResult> {
        let mut stmt = self.conn.new_statement()?;
        stmt.set_sql_query(sql)?;
        let rows_affected = stmt.execute_update()?;
        Ok(UpdateResult {
            rows_affected: rows_affected,
        })
    }

    /// Execute a query with string parameters using prepared statements
    ///
    /// # Arguments
    /// * `sql` - The SQL query with ? placeholders
    /// * `params` - The parameters to bind
    ///
    /// # Example
    /// ```no_run
    /// # use swanlake_client::FlightSQLClient;
    /// # let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;
    /// let result = client.execute_with_params(
    ///     "SELECT * FROM users WHERE name = ?",
    ///     vec!["Alice".to_string()]
    /// )?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn execute_with_params(&mut self, sql: &str, params: Vec<String>) -> Result<QueryResult> {
        let mut stmt = self.conn.new_statement()?;
        stmt.set_sql_query(sql)?;
        stmt.prepare()?;

        if !params.is_empty() {
            let schema = Arc::new(Schema::new(
                params
                    .iter()
                    .enumerate()
                    .map(|(i, _)| Field::new(format!("param{}", i), DataType::Utf8, false))
                    .collect::<Vec<Field>>(),
            ));
            let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(params))])?;
            stmt.bind(batch)?;
        }

        let reader = stmt.execute()?;
        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch?);
        }

        Ok(QueryResult::new(batches))
    }

    /// Execute an update with a record batch for batch inserts
    ///
    /// # Arguments
    /// * `sql` - The SQL statement with ? placeholders
    /// * `batch` - The record batch to bind
    ///
    /// # Example
    /// ```no_run
    /// # use swanlake_client::FlightSQLClient;
    /// # use arrow_array::{RecordBatch, StringArray};
    /// # use arrow_schema::{Schema, Field, DataType};
    /// # use std::sync::Arc;
    /// # let mut client = FlightSQLClient::connect("grpc://localhost:4214")?;
    /// let schema = Arc::new(Schema::new(vec![
    ///     Field::new("name", DataType::Utf8, false),
    /// ]));
    /// let batch = RecordBatch::try_new(
    ///     schema,
    ///     vec![Arc::new(StringArray::from(vec!["Alice", "Bob"]))],
    /// )?;
    /// client.execute_batch_update("INSERT INTO users (name) VALUES (?)", batch)?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn execute_batch_update(&mut self, sql: &str, batch: RecordBatch) -> Result<UpdateResult> {
        let mut stmt = self.conn.new_statement()?;
        stmt.set_sql_query(sql)?;
        stmt.prepare()?;
        stmt.bind(batch)?;
        let rows_affected = stmt.execute_update()?;
        Ok(UpdateResult {
            rows_affected: rows_affected,
        })
    }

    /// Get a reference to the underlying ADBC connection
    pub fn connection(&mut self) -> &mut ManagedConnection {
        &mut self.conn
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_result_empty() {
        let result = QueryResult::new(vec![]);
        assert!(result.is_empty());
        assert_eq!(result.total_rows, 0);
    }
}
