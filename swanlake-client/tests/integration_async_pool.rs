#![cfg(feature = "tokio")]

use std::sync::Arc;

use anyhow::Result;
use arrow_array::{Int32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use swanlake_client::{AsyncFlightSQLPool, QueryResult};

fn endpoint() -> String {
    std::env::var("SWANLAKE_ENDPOINT")
        .unwrap_or_else(|_| "grpc://127.0.0.1:4214".to_string())
}

fn table_schema() -> String {
    std::env::var("SWANLAKE_TEST_SCHEMA").unwrap_or_else(|_| "swanlake".to_string())
}

fn sample_int_batch(values: Vec<i32>) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))])?;
    Ok(batch)
}

fn unique_table_name() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("async_multi_row_{}", nanos)
}

fn extract_count(result: &QueryResult) -> Result<i64> {
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

#[tokio::test]
async fn integration_async_pool_usage() -> Result<()> {
    let endpoint = endpoint();
    let pool = AsyncFlightSQLPool::with_default(&endpoint).await?;

    let (q1, q2) = tokio::try_join!(pool.query("SELECT 1"), pool.query("SELECT 2"))?;
    assert_eq!(q1.total_rows, 1);
    assert_eq!(q2.total_rows, 1);

    let mut session = pool.acquire_session().await?;
    session.begin_transaction().await?;
    session.update("CREATE TEMP TABLE async_tmp (id INTEGER)")
        .await?;
    session.commit().await?;

    let params = sample_int_batch(vec![5])?;
    let result = pool.query_with_param("SELECT ? AS val", params).await?;
    assert_eq!(result.total_rows, 1);

    Ok(())
}

#[tokio::test]
async fn integration_async_pool_multi_row_params() -> Result<()> {
    let endpoint = endpoint();
    let schema = table_schema();
    let pool = AsyncFlightSQLPool::with_default(&endpoint).await?;

    let table = unique_table_name();
    pool.update(&format!("CREATE TABLE {}.{} (val INTEGER)", schema, table))
        .await?;

    let params = sample_int_batch(vec![4, 5, 6])?;
    pool.update_with_record_batch(&format!("INSERT INTO {}.{} VALUES (?)", schema, table), params)
        .await?;

    let result = pool
        .query(&format!("SELECT COUNT(*) FROM {}.{}", schema, table))
        .await?;
    assert_eq!(extract_count(&result)?, 3);

    pool.update(&format!("DROP TABLE {}.{}", schema, table)).await?;
    Ok(())
}
