#![cfg(feature = "tokio")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use arrow_array::{Int32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use swanlake_client::{AsyncFlightSQLPool, PoolConfig, QueryOptions, QueryResult};

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

fn unique_table_name() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("async_multi_row_{nanos}")
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
    session
        .update("CREATE TEMP TABLE async_tmp (id INTEGER)")
        .await?;
    let probe = session.query("SELECT 1 AS one").await?;
    assert_eq!(probe.total_rows, 1);
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
    pool.update(&format!("CREATE TABLE {schema}.{table} (val INTEGER)"))
        .await?;

    let params = sample_int_batch(vec![4, 5, 6])?;
    pool.update_with_record_batch(&format!("INSERT INTO {schema}.{table} VALUES (?)"), params)
        .await?;

    let result = pool
        .query(&format!("SELECT COUNT(*) FROM {schema}.{table}"))
        .await?;
    assert_eq!(extract_count(&result)?, 3);

    pool.update(&format!("DROP TABLE {schema}.{table}")).await?;
    Ok(())
}

#[tokio::test]
async fn integration_async_pool_usage_examples_cover_async_apis() -> Result<()> {
    let endpoint = endpoint();
    let schema = table_schema();
    let table = unique_table_name();
    let qualified = format!("{schema}.{table}");
    let opts = QueryOptions::new()
        .with_timeout_ms(50)
        .with_retry_on_failure(false);

    let pool = AsyncFlightSQLPool::new(
        &endpoint,
        PoolConfig {
            min_idle: 0,
            max_size: 4,
            acquire_timeout_ms: 5_000,
            idle_ttl_ms: 1,
            healthcheck_sql: "SELECT 1".to_string(),
            default_query_timeout_ms: Some(200),
            retry_on_failure: false,
        },
    )
    .await?;

    let execute = pool.execute("SELECT 10").await?;
    assert_eq!(execute.total_rows, 1);
    let query = pool.query_with_options("SELECT 11", opts.clone()).await?;
    assert_eq!(query.total_rows, 1);
    let param_query = pool
        .query_with_param("SELECT ? + 1 AS val", sample_int_batch(vec![10])?)
        .await?;
    assert_eq!(param_query.total_rows, 1);

    pool.update_with_options(
        &format!("CREATE TABLE {qualified} (id INTEGER)"),
        opts.clone(),
    )
    .await?;

    let test_result = async {
        let mut session = pool.acquire_session().await?;
        session.begin_transaction().await?;
        session
            .update(&format!("INSERT INTO {qualified} VALUES (1)"))
            .await?;
        session.rollback().await?;

        let after_rollback = pool
            .query(&format!("SELECT COUNT(*) FROM {qualified}"))
            .await?;
        assert_eq!(extract_count(&after_rollback)?, 0);

        let inserted_batch = sample_int_batch(vec![3, 4])?;
        pool.update_with_record_batch(
            &format!("INSERT INTO {qualified} VALUES (?)"),
            inserted_batch,
        )
        .await?;

        let inserted_count = pool
            .query(&format!("SELECT COUNT(*) FROM {qualified}"))
            .await?;
        assert_eq!(extract_count(&inserted_count)?, 2);

        let query_error = pool
            .query_with_options(
                "SELECT * FROM __missing_async_usage_table__",
                QueryOptions::new().with_retry_on_failure(false),
            )
            .await;
        assert!(query_error.is_err());

        let update_error = pool
            .update_with_options(
                "INSERT INTO __missing_async_usage_table__ VALUES (1)",
                QueryOptions::new().with_retry_on_failure(false),
            )
            .await;
        assert!(update_error.is_err());

        Ok::<(), anyhow::Error>(())
    }
    .await;

    let _ = pool
        .update(&format!("DROP TABLE IF EXISTS {qualified}"))
        .await;
    test_result?;
    Ok(())
}

#[tokio::test]
async fn integration_async_pool_acquire_timeout_when_exhausted() -> Result<()> {
    let endpoint = endpoint();
    let pool = AsyncFlightSQLPool::new(
        &endpoint,
        PoolConfig {
            min_idle: 0,
            max_size: 1,
            acquire_timeout_ms: 20,
            idle_ttl_ms: 1_000,
            healthcheck_sql: "SELECT 1".to_string(),
            default_query_timeout_ms: None,
            retry_on_failure: false,
        },
    )
    .await?;

    let _held = pool.acquire_session().await?;
    let start = Instant::now();
    let err = match pool.acquire_session().await {
        Ok(_) => return Err(anyhow::anyhow!("expected async acquire timeout")),
        Err(err) => err,
    };
    assert!(err
        .to_string()
        .contains("timed out waiting for pool connection"));
    assert!(start.elapsed() >= Duration::from_millis(20));
    Ok(())
}

#[tokio::test]
async fn integration_async_pool_retry_path_drops_unhealthy_connections() -> Result<()> {
    let endpoint = endpoint();
    let pool = AsyncFlightSQLPool::new(
        &endpoint,
        PoolConfig {
            min_idle: 0,
            max_size: 1,
            acquire_timeout_ms: 500,
            idle_ttl_ms: 1_000,
            healthcheck_sql: "SELECT * FROM __missing_async_healthcheck_table__".to_string(),
            default_query_timeout_ms: None,
            retry_on_failure: true,
        },
    )
    .await?;

    let err = pool
        .query("SELECT * FROM __missing_async_retry_query_table__")
        .await
        .expect_err("expected query to fail");
    let message = err.to_string();
    assert!(message.contains("__missing_async_retry_query_table__") || message.contains("Catalog"));

    let healthy = pool.query("SELECT 1 AS ok").await?;
    assert_eq!(healthy.total_rows, 1);
    Ok(())
}
