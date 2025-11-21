use anyhow::{ensure, Context, Result};
use arrow_array::Int32Array;
use flight_sql_client::{FlightSQLClient, QueryResult};
use tracing::info;

use crate::CliArgs;

pub async fn run_transaction_recovery(args: &CliArgs) -> Result<()> {
    info!("Running transaction recovery tests");

    let mut client = FlightSQLClient::connect(args.endpoint())
        .context("failed to connect to FlightSQL server")?;
    client.exec("use swanlake")?;

    test_auto_rollback_after_abort(&mut client, "tx_recovery")?;
    test_auto_rollback_after_abort(&mut client, "swanlake.tx_recovery_catalog")?;
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

        // Next statement should fail with the transaction aborted error; the server should
        // auto-rollback in response so the session becomes usable again.
        let tx_err = client
            .execute_update(&format!("INSERT INTO {table} VALUES (2)"))
            .expect_err("expected transaction abort after failure");
        let tx_err_text = tx_err.to_string().to_lowercase();
        ensure!(
            tx_err_text.contains("transaction") && tx_err_text.contains("abort"),
            "expected transaction abort error, got {tx_err_text}"
        );

        // After the abort was detected, the server should have rolled back the transaction,
        // so fresh statements should succeed in autocommit mode.
        client.execute_update(&format!("INSERT INTO {table} VALUES (3)"))?;

        let ids = fetch_ids(client, &format!("SELECT id FROM {table} ORDER BY id"))?;
        ensure!(
            ids == vec![3],
            "expected rolled-back transaction to remove earlier writes for {table}, got {ids:?}"
        );
        Ok(())
    })();

    client.execute_update(&format!("DROP TABLE IF EXISTS {table}"))?;
    result
}

fn test_new_session_after_abort(args: &CliArgs) -> Result<()> {
    // Ensure a fresh connection works even after a previous session experienced an abort.
    let mut client = FlightSQLClient::connect(args.endpoint())
        .context("failed to connect to FlightSQL server for new session test")?;
    client.exec("use swanlake")?;
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
