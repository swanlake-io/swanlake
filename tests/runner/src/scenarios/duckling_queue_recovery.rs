use std::env;
use std::path::PathBuf;

use crate::scenarios::duckling_queue_utils::{
    first_parquet_chunk, reset_dir, wait_for_parquet_chunks,
};
use crate::CliArgs;
use anyhow::{anyhow, Context, Result};
use flight_sql_client::FlightSQLClient;
use tracing::info;

/// Ensure duckling_queue persistent files are readable and preserve data faithfully.
pub async fn run_duckling_queue_recovery(args: &CliArgs) -> Result<()> {
    info!("running duckling queue recovery scenario");
    let test_dir = args
        .test_dir
        .as_ref()
        .context("--test-dir is required for duckling queue recovery scenario")?;

    let dq_root = PathBuf::from(
        env::var("SWANLAKE_DUCKLING_QUEUE_ROOT")
            .unwrap_or_else(|_| format!("{test_dir}/duckling_queue")),
    );
    reset_dir(&dq_root)?;

    let attach_sql = format!(
        "ATTACH IF NOT EXISTS 'ducklake:postgres:dbname=swanlake_test' AS swanlake \
         (DATA_PATH '{test_dir}/swanlake_files', OVERRIDE_DATA_PATH true);"
    );

    let mut conn = FlightSQLClient::connect(&args.endpoint)?;
    conn.execute_update(&attach_sql)?;
    conn.execute_update("DROP TABLE IF EXISTS swanlake.dq_recovery_target;")?;

    // Write buffered data but do not flush; we want to read the persisted chunk itself.
    conn.execute_update(
        "INSERT INTO duckling_queue.dq_recovery_target \
         SELECT CAST(1 AS BIGINT) AS id, CAST('alpha' AS VARCHAR) AS label \
         UNION ALL SELECT CAST(2 AS BIGINT), CAST('beta' AS VARCHAR);",
    )?;

    wait_for_parquet_chunks(&dq_root, |count| count >= 1).await?;
    let chunk = first_parquet_chunk(&dq_root)?;

    // Read back from the persisted Parquet file via DuckDB to ensure the data is recoverable.
    let query = format!(
        "SELECT id, label FROM '{}' ORDER BY id",
        chunk.to_string_lossy()
    );
    let result = conn.execute_query(&query)?;
    let mut values = Vec::new();
    for batch in result.batches {
        let id_col = batch
            .column_by_name("id")
            .ok_or_else(|| anyhow!("id column missing in recovered batch"))?;
        let label_col = batch
            .column_by_name("label")
            .ok_or_else(|| anyhow!("label column missing in recovered batch"))?;

        let ids = id_col
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .ok_or_else(|| anyhow!("id column is not Int64"))?;
        let labels = label_col
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .ok_or_else(|| anyhow!("label column is not Utf8"))?;
        for row_idx in 0..batch.num_rows() {
            values.push((ids.value(row_idx), labels.value(row_idx).to_string()));
        }
    }
    values.sort_by_key(|(id, _)| *id);
    assert_eq!(
        values,
        vec![(1, "alpha".to_string()), (2, "beta".to_string())],
        "persisted chunk should preserve all rows and types"
    );

    info!("duckling queue recovery scenario passed");
    Ok(())
}
