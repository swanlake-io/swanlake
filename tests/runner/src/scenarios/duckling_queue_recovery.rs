use std::env;
use std::path::PathBuf;

use crate::scenarios::duckling_queue_utils::{
    buffer_path, read_staging_rows, wait_for_staging_rows,
};
use crate::CliArgs;
use anyhow::{anyhow, Context, Result};
use swanlake_client::FlightSQLClient;
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
    // Buffer root is managed by the server; ensure it exists without clearing it.
    std::fs::create_dir_all(&dq_root)
        .with_context(|| format!("failed to ensure dq root {}", dq_root.display()))?;
    let buffer = buffer_path(&dq_root);

    let attach_sql = format!(
        "ATTACH IF NOT EXISTS 'ducklake:postgres:dbname=swanlake_test' AS swanlake \
         (DATA_PATH '{test_dir}/swanlake_files', OVERRIDE_DATA_PATH true);"
    );

    let mut conn = FlightSQLClient::connect(&args.endpoint)?;
    conn.update(&attach_sql)?;
    conn.update("DROP TABLE IF EXISTS swanlake.dq_recovery_target;")?;

    // Write buffered data but do not flush; we want to read the persisted chunk itself.
    conn.update(
        "INSERT INTO duckling_queue.dq_recovery_target \
         SELECT CAST(1 AS BIGINT) AS id, CAST('alpha' AS VARCHAR) AS label \
         UNION ALL SELECT CAST(2 AS BIGINT), CAST('beta' AS VARCHAR);",
    )?;

    wait_for_staging_rows(&mut conn, &buffer, "dq_recovery_target", |count| count >= 1).await?;

    let mut values = read_staging_rows(&mut conn, &buffer, "dq_recovery_target", &["id", "label"])?
        .into_iter()
        .map(|row| {
            let id = row.first()
                .ok_or_else(|| anyhow!("missing id column"))?
                .parse::<i64>()
                .map_err(|err| anyhow!("failed to parse id: {err}"))?;
            let label = row
                .get(1)
                .cloned()
                .ok_or_else(|| anyhow!("missing label"))?;
            Ok((id, label))
        })
        .collect::<Result<Vec<_>>>()?;
    values.sort_by_key(|(id, _)| *id);
    assert_eq!(
        values,
        vec![(1, "alpha".to_string()), (2, "beta".to_string())],
        "persisted chunk should preserve all rows and types"
    );

    info!("duckling queue recovery scenario passed");
    Ok(())
}
