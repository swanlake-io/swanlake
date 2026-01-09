use std::sync::Arc;

use anyhow::Result;
use arrow_array::{Int32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use swanlake_client::{FlightSQLClient, FlightSQLPool};

fn endpoint() -> String {
    std::env::var("SWANLAKE_ENDPOINT").unwrap_or_else(|_| "grpc://127.0.0.1:4214".to_string())
}

fn table_schema() -> String {
    std::env::var("SWANLAKE_TEST_SCHEMA").unwrap_or_else(|_| "swanlake".to_string())
}

fn sample_int_batch(values: Vec<i32>) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))])?;
    Ok(batch)
}

fn extract_count(result: &swanlake_client::QueryResult) -> Result<i64> {
    let batch = result
        .batches
        .first()
        .ok_or_else(|| anyhow::anyhow!("missing result batch"))?;
    let array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("COUNT(*) did not return Int64"))?;
    Ok(array.value(0))
}

#[test]
fn integration_client_and_pool_usage() -> Result<()> {
    let endpoint = endpoint();

    // Client usage: query, update, and parameter binding.
    let mut client = FlightSQLClient::connect(&endpoint)?;
    let result = client.query("SELECT 1 AS one")?;
    assert_eq!(result.total_rows, 1);

    client.update("CREATE TEMP TABLE client_tmp (id INTEGER)")?;
    client.update("INSERT INTO client_tmp VALUES (1), (2)")?;
    let count = client.query("SELECT COUNT(*) AS cnt FROM client_tmp")?;
    assert_eq!(count.total_rows, 1);

    let params = sample_int_batch(vec![10])?;
    let result = client.query_with_param("SELECT ? AS val", params)?;
    assert_eq!(result.total_rows, 1);

    // Pool usage: stateless query and session-scoped prepared statement.
    let pool = FlightSQLPool::with_default(&endpoint)?;
    let pooled = pool.query("SELECT 1")?;
    assert_eq!(pooled.total_rows, 1);

    let mut session = pool.acquire_session()?;
    session.begin_transaction()?;
    session.update("CREATE TEMP TABLE pool_tmp (id INTEGER)")?;
    session.commit()?;

    let mut prepared = session.prepare_query("SELECT ? + 1 AS val")?;
    let params = sample_int_batch(vec![1])?;
    prepared.bind(params)?;
    let result = prepared.query()?;
    assert_eq!(result.total_rows, 1);

    Ok(())
}

#[test]
fn integration_client_multi_row_params() -> Result<()> {
    let endpoint = endpoint();
    let mut client = FlightSQLClient::connect(&endpoint)?;
    let schema = table_schema();

    client.update(&format!(
        "CREATE TABLE {}.multi_row_tmp (val INTEGER)",
        schema
    ))?;
    let params = sample_int_batch(vec![1, 2, 3])?;
    client.update_with_record_batch(
        &format!("INSERT INTO {}.multi_row_tmp VALUES (?)", schema),
        params,
    )?;

    let result = client.query(&format!("SELECT COUNT(*) FROM {}.multi_row_tmp", schema))?;
    assert_eq!(extract_count(&result)?, 3);

    client.update(&format!("DROP TABLE {}.multi_row_tmp", schema))?;

    Ok(())
}
