use std::sync::Arc;

use adbc_core::{Connection, Statement};
use adbc_driver_manager::ManagedConnection;
use anyhow::Result;
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

use crate::connection;

/// A Flight SQL client for connecting to SwanLake servers.
pub struct FlightSQLClient {
    conn: ManagedConnection,
}

/// Result of executing a query.
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// The record batches returned by the query.
    pub batches: Vec<RecordBatch>,
    /// Total number of rows across all batches.
    pub total_rows: usize,
}

impl QueryResult {
    /// Create a new QueryResult from batches.
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        let total_rows = batches.iter().map(|b| b.num_rows()).sum();
        Self {
            batches,
            total_rows,
        }
    }

    /// Get the schema from the first batch (if available).
    pub fn schema(&self) -> Option<Arc<Schema>> {
        self.batches.first().map(|b| b.schema())
    }

    /// Check if the result is empty.
    pub fn is_empty(&self) -> bool {
        self.total_rows == 0
    }
}

/// Result of executing an update/DDL statement.
#[derive(Debug, Clone)]
pub struct UpdateResult {
    /// Number of rows affected (if available).
    pub rows_affected: Option<i64>,
}

impl FlightSQLClient {
    /// Connect to a SwanLake Flight SQL server.
    pub fn connect(endpoint: &str) -> Result<Self> {
        let mut conn = connection::connect(endpoint)?;
        // Test the connection by executing a simple query.
        let mut stmt = conn.new_statement()?;
        stmt.set_sql_query("SELECT 1")?;
        let _reader = stmt.execute()?;
        Ok(Self { conn })
    }

    /// Execute a query and return results.
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

    /// Execute an update/DDL statement (INSERT, UPDATE, DELETE, CREATE, etc.).
    pub fn execute_update(&mut self, sql: &str) -> Result<UpdateResult> {
        let mut stmt = self.conn.new_statement()?;
        stmt.set_sql_query(sql)?;
        let rows_affected = stmt.execute_update()?;
        Ok(UpdateResult { rows_affected })
    }

    /// Execute a query with string parameters using prepared statements.
    pub fn execute_with_params(&mut self, sql: &str, params: Vec<String>) -> Result<QueryResult> {
        let mut stmt = self.conn.new_statement()?;
        stmt.set_sql_query(sql)?;
        stmt.prepare()?;

        if !params.is_empty() {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "param",
                DataType::Utf8,
                false,
            )]));
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

    /// Execute an update with a record batch for batch inserts.
    pub fn execute_batch_update(&mut self, sql: &str, batch: RecordBatch) -> Result<UpdateResult> {
        let mut stmt = self.conn.new_statement()?;
        stmt.set_sql_query(sql)?;
        stmt.prepare()?;
        stmt.bind(batch)?;
        let rows_affected = stmt.execute_update()?;
        Ok(UpdateResult { rows_affected })
    }

    /// Get a reference to the underlying ADBC connection.
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
