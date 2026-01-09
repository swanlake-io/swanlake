#![cfg(feature = "tokio")]

use std::sync::Arc;

use anyhow::Result;
use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use swanlake_client::AsyncFlightSQLPool;
use tokio::time::Instant;

fn endpoint() -> String {
    std::env::var("SWANLAKE_ENDPOINT")
        .unwrap_or_else(|_| "grpc://127.0.0.1:4214".to_string())
}

fn table_schema() -> String {
    std::env::var("SWANLAKE_TEST_SCHEMA").unwrap_or_else(|_| "swanlake".to_string())
}

fn unique_table_name() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("perf_async_{}", nanos)
}

fn build_insert_batch(batch_size: usize) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let ids: Vec<i32> = (0..batch_size as i32).collect();
    let payloads = vec!["payload"; batch_size];
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(ids)),
            Arc::new(StringArray::from(payloads)),
        ],
    )?;
    Ok(batch)
}

fn read_env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn perf_async_pool_mixed_load() -> Result<()> {
    let endpoint = endpoint();
    let schema = table_schema();
    let pool = AsyncFlightSQLPool::with_default(&endpoint).await?;

    let table = unique_table_name();
    pool.update(&format!(
        "CREATE TABLE {}.{} (id INTEGER, payload VARCHAR)",
        schema, table
    ))
    .await?;

    let readers = read_env_usize("SWANLAKE_PERF_READERS", 8);
    let writers = read_env_usize("SWANLAKE_PERF_WRITERS", 4);
    let write_iters = read_env_usize("SWANLAKE_PERF_WRITE_ITERS", 50);
    let read_iters = read_env_usize("SWANLAKE_PERF_READ_ITERS", 200);
    let batch_size = read_env_usize("SWANLAKE_PERF_BATCH_SIZE", 128);

    let insert_batch = build_insert_batch(batch_size)?;
    let insert_sql = format!("INSERT INTO {}.{} VALUES (?, ?)", schema, table);
    let read_sql = format!("SELECT COUNT(*) FROM {}.{}", schema, table);

    let start = Instant::now();

    let mut tasks = Vec::new();
    for _ in 0..writers {
        let pool = pool.clone();
        let sql = insert_sql.clone();
        let batch = insert_batch.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..write_iters {
                pool.update_with_record_batch(&sql, batch.clone()).await?;
            }
            Ok::<(), anyhow::Error>(())
        }));
    }

    for _ in 0..readers {
        let pool = pool.clone();
        let sql = read_sql.clone();
        tasks.push(tokio::spawn(async move {
            for _ in 0..read_iters {
                let result = pool.query(&sql).await?;
                if result.total_rows != 1 {
                    anyhow::bail!("unexpected row count from COUNT(*)");
                }
            }
            Ok::<(), anyhow::Error>(())
        }));
    }

    for task in tasks {
        task.await??;
    }

    let elapsed = start.elapsed();
    let total_ops = readers * read_iters + writers * write_iters;
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64().max(0.001);

    println!(
        "perf_async_pool_mixed_load: ops={} elapsed={:.2?} ops/s={:.1}",
        total_ops, elapsed, ops_per_sec
    );

    pool.update(&format!("DROP TABLE {}.{}", schema, table)).await?;
    Ok(())
}
