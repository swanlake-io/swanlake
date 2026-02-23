use std::sync::Arc;
use std::time::{Duration, Instant};

use adbc_core::{Connection, Statement};
use anyhow::Result;
use arrow_array::{Int32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use swanlake_client::{FlightSQLClient, FlightSQLPool, PoolConfig, QueryOptions, QueryResult};

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

fn unique_table_name(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{prefix}_{nanos}")
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
        "CREATE TABLE {schema}.multi_row_tmp (val INTEGER)"
    ))?;
    let params = sample_int_batch(vec![1, 2, 3])?;
    client.update_with_record_batch(
        &format!("INSERT INTO {schema}.multi_row_tmp VALUES (?)"),
        params,
    )?;

    let result = client.query(&format!("SELECT COUNT(*) FROM {schema}.multi_row_tmp"))?;
    assert_eq!(extract_count(&result)?, 3);

    client.update(&format!("DROP TABLE {schema}.multi_row_tmp"))?;

    Ok(())
}

#[test]
fn integration_client_usage_examples_cover_sync_pool_apis() -> Result<()> {
    let endpoint = endpoint();
    let schema = table_schema();
    let table = unique_table_name("client_usage");
    let qualified = format!("{schema}.{table}");

    let mut client = FlightSQLClient::connect(&endpoint)?;
    let boot = client.execute("SELECT 42 AS answer")?;
    assert_eq!(boot.total_rows, 1);
    assert!(!boot.is_empty());
    assert!(boot.schema().is_some());

    let mut stmt = client.connection().new_statement()?;
    stmt.set_sql_query("SELECT 1")?;
    let mut low_level_rows = 0usize;
    for batch in stmt.execute()? {
        low_level_rows += batch?.num_rows();
    }
    assert_eq!(low_level_rows, 1);

    let pool = FlightSQLPool::new(
        &endpoint,
        PoolConfig {
            min_idle: 0,
            max_size: 2,
            acquire_timeout_ms: 5_000,
            idle_ttl_ms: 1,
            healthcheck_sql: "SELECT 1".to_string(),
            default_query_timeout_ms: Some(200),
            retry_on_failure: false,
        },
    )?;

    let opts = QueryOptions::new()
        .with_timeout_ms(50)
        .with_retry_on_failure(false);
    let execute = pool.execute("SELECT 9")?;
    assert_eq!(execute.total_rows, 1);
    let queried = pool.query_with_options("SELECT 8", opts.clone())?;
    assert_eq!(queried.total_rows, 1);
    let pooled_param_query =
        pool.query_with_param("SELECT ? + 1 AS val", sample_int_batch(vec![8])?)?;
    assert_eq!(pooled_param_query.total_rows, 1);

    let test_result = (|| -> Result<()> {
        let create = pool.update_with_options(
            &format!("CREATE TABLE {qualified} (id INTEGER)"),
            opts.clone(),
        )?;
        if let Some(value) = create.rows_affected {
            assert!(value >= 0);
        }

        let mut session = pool.acquire_session()?;
        session.begin_transaction()?;
        session.update(&format!("INSERT INTO {qualified} VALUES (1)"))?;
        session.rollback()?;

        let session_query = session.query("SELECT 1 AS one")?;
        assert_eq!(session_query.total_rows, 1);
        let mut session_stmt = session.connection()?.new_statement()?;
        session_stmt.set_sql_query("SELECT 2")?;
        let mut session_stmt_rows = 0usize;
        for batch in session_stmt.execute()? {
            session_stmt_rows += batch?.num_rows();
        }
        assert_eq!(session_stmt_rows, 1);

        let after_rollback = pool.query(&format!("SELECT COUNT(*) FROM {qualified}"))?;
        assert_eq!(extract_count(&after_rollback)?, 0);

        let inserted_direct = pool.update(&format!("INSERT INTO {qualified} VALUES (7)"))?;
        if let Some(value) = inserted_direct.rows_affected {
            assert!(value >= 0);
        }
        let inserted_batch = pool.update_with_record_batch(
            &format!("INSERT INTO {qualified} VALUES (?)"),
            sample_int_batch(vec![8, 9])?,
        )?;
        assert_eq!(inserted_batch.rows_affected, Some(2));

        let mut prepared_insert =
            session.prepare_query(&format!("INSERT INTO {qualified} VALUES (?)"))?;
        prepared_insert.bind(sample_int_batch(vec![11, 12])?)?;
        let inserted = prepared_insert.update()?;
        assert_eq!(inserted.rows_affected, Some(2));

        let mut prepared_query = session.prepare_query("SELECT ? + 10 AS val")?;
        prepared_query.bind(sample_int_batch(vec![2])?)?;
        let prepared_rows = prepared_query.execute()?;
        assert_eq!(prepared_rows.total_rows, 1);

        let insert_error = pool.query_with_options(
            "SELECT * FROM __missing_sync_usage_table__",
            QueryOptions::new().with_retry_on_failure(false),
        );
        assert!(insert_error.is_err());
        let update_error = pool.update_with_options(
            "INSERT INTO __missing_sync_usage_table__ VALUES (1)",
            QueryOptions::new().with_retry_on_failure(false),
        );
        assert!(update_error.is_err());

        let persisted = pool.query(&format!("SELECT COUNT(*) FROM {qualified}"))?;
        assert_eq!(extract_count(&persisted)?, 5);
        Ok(())
    })();

    let _ = client.update(&format!("DROP TABLE IF EXISTS {qualified}"));
    test_result
}

#[test]
fn query_result_helper_methods_are_consistent() -> Result<()> {
    let empty = QueryResult::with_rows_affected(Vec::new(), Some(0));
    assert!(empty.is_empty());
    assert!(empty.schema().is_none());
    assert_eq!(empty.rows_affected, Some(0));

    let non_empty = QueryResult::new(vec![sample_int_batch(vec![1, 2, 3])?]);
    assert!(!non_empty.is_empty());
    assert_eq!(non_empty.total_rows, 3);
    assert!(non_empty.schema().is_some());
    Ok(())
}

#[test]
fn integration_sync_pool_acquire_timeout_when_exhausted() -> Result<()> {
    let endpoint = endpoint();
    let pool = FlightSQLPool::new(
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
    )?;

    let _held = pool.acquire()?;
    let start = Instant::now();
    let err = match pool.acquire() {
        Ok(_) => return Err(anyhow::anyhow!("expected acquire timeout")),
        Err(err) => err,
    };
    assert!(err
        .to_string()
        .contains("timed out waiting for pool connection"));
    assert!(start.elapsed() >= Duration::from_millis(20));
    Ok(())
}

#[test]
fn integration_sync_pool_retry_path_drops_unhealthy_connections() -> Result<()> {
    let endpoint = endpoint();
    let pool = FlightSQLPool::new(
        &endpoint,
        PoolConfig {
            min_idle: 0,
            max_size: 1,
            acquire_timeout_ms: 500,
            idle_ttl_ms: 1_000,
            healthcheck_sql: "SELECT * FROM __missing_sync_healthcheck_table__".to_string(),
            default_query_timeout_ms: None,
            retry_on_failure: true,
        },
    )?;

    let err = pool
        .query("SELECT * FROM __missing_sync_retry_query_table__")
        .expect_err("expected query to fail");
    let message = err.to_string();
    assert!(message.contains("__missing_sync_retry_query_table__") || message.contains("Catalog"));

    let healthy = pool.query("SELECT 1 AS ok")?;
    assert_eq!(healthy.total_rows, 1);
    Ok(())
}
