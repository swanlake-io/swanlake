use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::scenarios::client_ext::FlightSqlClientExt;
use anyhow::{anyhow, Context, Result};
use arrow_array::{Array, Int64Array, StringArray};
use swanlake_client::FlightSQLClient;
use tokio::time::sleep;

pub fn reset_dir(dir: &Path) -> Result<()> {
    if dir.exists() {
        fs::remove_dir_all(dir).with_context(|| format!("failed to clear {}", dir.display()))?;
    }
    fs::create_dir_all(dir).with_context(|| format!("failed to create {}", dir.display()))
}

pub fn buffer_path(root: &Path) -> PathBuf {
    root.join("buffer.duckdb")
}

pub async fn wait_for_staging_rows<F>(
    conn: &mut FlightSQLClient,
    buffer_path: &Path,
    table: &str,
    condition: F,
) -> Result<()>
where
    F: Fn(usize) -> bool,
{
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let count = count_staging_rows(conn, buffer_path, table)?;
        if condition(count) {
            return Ok(());
        }
        if std::time::Instant::now() > deadline {
            return Err(anyhow!(
                "timed out waiting for duckling queue staging rows (last count: {count})"
            ));
        }
        sleep(Duration::from_millis(50)).await;
    }
}

pub fn count_staging_rows(
    conn: &mut FlightSQLClient,
    buffer_path: &Path,
    table: &str,
) -> Result<usize> {
    attach_buffer(conn, buffer_path)?;
    let staging = staging_table_name(table);
    let sql = format!("SELECT COUNT(*) FROM dqbuf.\"{}\"", staging);
    let count = conn.query_scalar_i64(&sql).map_err(|err| {
        anyhow!(
            "failed to count staging rows in {}: {}",
            staging,
            err
        )
    })?;
    Ok(count as usize)
}

pub fn read_staging_rows(
    conn: &mut FlightSQLClient,
    buffer_path: &Path,
    table: &str,
    columns: &[&str],
) -> Result<Vec<Vec<String>>> {
    attach_buffer(conn, buffer_path)?;
    let cols = columns.join(", ");
    let staging = staging_table_name(table);
    let sql = format!("SELECT {cols} FROM dqbuf.\"{}\" ORDER BY rowid", staging);
    let result = conn.query(&sql)?;
    let mut out = Vec::new();
    for batch in result.batches {
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(batch.num_columns());
            for col_idx in 0..batch.num_columns() {
                row.push(array_value_to_string(batch.column(col_idx), row_idx));
            }
            out.push(row);
        }
    }
    Ok(out)
}

fn array_value_to_string(array: &dyn Array, row_idx: usize) -> String {
    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        if arr.is_null(row_idx) {
            String::new()
        } else {
            arr.value(row_idx).to_string()
        }
    } else if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        if arr.is_null(row_idx) {
            String::new()
        } else {
            arr.value(row_idx).to_string()
        }
    } else {
        "<unsupported>".to_string()
    }
}

fn staging_table_name(table: &str) -> String {
    format!("dq_{}", table)
}

fn attach_buffer(conn: &mut FlightSQLClient, buffer_path: &Path) -> Result<()> {
    let attach_sql = format!(
        "ATTACH IF NOT EXISTS '{}' AS dqbuf (READ_ONLY);",
        buffer_path.display()
    );
    conn.update(&attach_sql)?;
    Ok(())
}
