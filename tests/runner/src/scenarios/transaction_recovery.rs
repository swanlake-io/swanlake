use anyhow::{ensure, Context, Result};
use arrow_array::Int32Array;
use flight_sql_client::{FlightSQLClient, QueryResult};
use tracing::info;

use crate::CliArgs;

/// Transaction recovery tests
///
/// These tests verify auto-recovery when DuckDB enters an aborted transaction state.
/// Current coverage:
/// - Auto-rollback and retry when executing statements after transaction abort
/// - Data consistency after recovery
/// - Fresh sessions work after abort
///
/// NOT YET COVERED (requires Flight SQL action API support):
/// - Explicit COMMIT via ActionEndTransaction on an aborted transaction
///   (should return TransactionAborted error, not Ok)
/// - Explicit ROLLBACK via ActionEndTransaction on an aborted transaction
///   (should return TransactionAborted error, not TransactionNotFound)
pub async fn run_transaction_recovery(args: &CliArgs) -> Result<()> {
    info!("Running transaction recovery tests");

    let mut client = FlightSQLClient::connect(&args.endpoint)
        .context("failed to connect to FlightSQL server")?;
    client.execute_update("use swanlake")?;

    test_auto_rollback_after_abort(&mut client, "tx_recovery")?;
    test_auto_rollback_after_abort(&mut client, "swanlake.tx_recovery_catalog")?;
    test_auto_retry_after_schema_error(&mut client)?;
    test_new_session_after_abort(args)?;

    info!("Transaction recovery tests completed successfully");
    Ok(())
}

fn test_auto_rollback_after_abort(client: &mut FlightSQLClient, table: &str) -> Result<()> {
    client.execute_update(&format!("DROP TABLE IF EXISTS {table}"))?;
    let result = (|| -> Result<()> {
        client.execute_update(&format!("CREATE TABLE {table} (id INT)"))?;

        client.execute_update("BEGIN")?;
        client.execute_update(&format!("INSERT INTO {table} VALUES (1)"))?;

        // Cause an error inside the transaction so the context is marked aborted.
        let type_err = client
            .execute_update(&format!("INSERT INTO {table} VALUES ('oops')"))
            .expect_err("expected type error inside transaction");
        let err_text = type_err.to_string().to_lowercase();
        ensure!(
            err_text.contains("type")
                || err_text.contains("conversion")
                || err_text.contains("cast"),
            "expected a type-related error, got {err_text}"
        );

        // Next statement will detect the aborted transaction, auto-rollback, and retry.
        // The retry succeeds in autocommit mode (outside the transaction).
        client.execute_update(&format!("INSERT INTO {table} VALUES (2)"))?;

        // After the auto-rollback and retry, fresh statements should continue to work.
        client.execute_update(&format!("INSERT INTO {table} VALUES (3)"))?;

        let ids = fetch_ids(client, &format!("SELECT id FROM {table} ORDER BY id"))?;
        ensure!(
            ids == vec![2, 3],
            "expected rolled-back transaction to remove INSERT(1), leaving only auto-retried INSERT(2) and INSERT(3) for {table}, got {ids:?}"
        );
        Ok(())
    })();

    client.execute_update(&format!("DROP TABLE IF EXISTS {table}"))?;
    result
}

fn test_auto_retry_after_schema_error(client: &mut FlightSQLClient) -> Result<()> {
    // This test simulates the scenario from the bug report:
    // 1. A query with an error (e.g., missing file) puts DuckDB in aborted transaction state
    // 2. The next query should auto-retry after rollback and succeed

    client.execute_update("DROP TABLE IF EXISTS auto_retry_test")?;
    client.execute_update("CREATE TABLE auto_retry_test (id INT, name VARCHAR)")?;
    client.execute_update("INSERT INTO auto_retry_test VALUES (1, 'test')")?;

    // First, cause an error that puts the transaction in aborted state.
    // We'll try to query a non-existent table.
    let err = client
        .execute("SELECT * FROM non_existent_table_xyz LIMIT 10")
        .expect_err("expected error for non-existent table");
    let err_text = err.to_string().to_lowercase();
    ensure!(
        err_text.contains("catalog")
            || err_text.contains("not found")
            || err_text.contains("does not exist"),
        "expected catalog/not found error, got {err_text}"
    );

    // Now execute a valid query. This should trigger auto-retry after detecting
    // the aborted transaction state, roll back, and succeed on the retry.
    let result = client.execute("SHOW TABLES")?;
    ensure!(
        result.total_rows > 0,
        "expected SHOW TABLES to succeed after auto-retry"
    );

    // Verify we can still query the table that exists
    let result = client.execute("SELECT COUNT(*) FROM auto_retry_test")?;
    ensure!(
        result.total_rows == 1,
        "expected to query existing table after recovery"
    );

    client.execute_update("DROP TABLE IF EXISTS auto_retry_test")?;
    info!("Auto-retry after schema error test passed");
    Ok(())
}

fn test_new_session_after_abort(args: &CliArgs) -> Result<()> {
    // Ensure a fresh connection works even after a previous session experienced an abort.
    let mut client = FlightSQLClient::connect(&args.endpoint)
        .context("failed to connect to FlightSQL server for new session test")?;
    client.execute_update("use swanlake")?;
    let QueryResult { total_rows, .. } = client.execute("SELECT 1")?;
    ensure!(total_rows == 1, "expected SELECT 1 to return a single row");
    Ok(())
}

fn fetch_ids(client: &mut FlightSQLClient, sql: &str) -> Result<Vec<i32>> {
    let QueryResult { batches, .. } = client.execute(sql)?;
    let batch = batches
        .first()
        .context("expected at least one batch for id fetch")?;
    let array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .context("expected Int32 array for id column")?;
    Ok((0..array.len()).map(|i| array.value(i)).collect())
}
