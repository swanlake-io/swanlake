use anyhow::{anyhow, Result};

use crate::scenarios::SqlClient;
use crate::CliArgs;

pub async fn run_parameter_types(args: &CliArgs) -> Result<()> {
    let endpoint = args.endpoint();
    let mut client = SqlClient::connect(endpoint).await?;

    // Create test table with various supported types
    client
        .exec(
            r#"
            CREATE TABLE IF NOT EXISTS parameter_types_test (
                id INTEGER,
                date32_col DATE,
                date64_col DATE,
                time32_sec_col TIME,
                time32_ms_col TIME,
                time64_us_col TIME,
                time64_ns_col TIME,
                interval_ym_col INTERVAL,
                interval_dt_col INTERVAL,
                interval_mdn_col INTERVAL
            )
            "#,
        )
        .await?;

    // Clear table
    client.exec("DELETE FROM parameter_types_test").await?;

    // Insert with literals to test type support
    client
        .exec(
            r#"
            INSERT INTO parameter_types_test VALUES (
                1,
                '2023-12-25'::DATE,
                '2023-12-25'::DATE,
                '10:20:30'::TIME,
                '10:20:30'::TIME,
                '10:20:30'::TIME,
                '10:20:30'::TIME,
                '1 year 2 months'::INTERVAL,
                '3 days 4 hours'::INTERVAL,
                '5 months 6 days 7 seconds'::INTERVAL
            )
            "#,
        )
        .await?;

    // Verify insertion
    let count = client
        .query_single_i64("SELECT COUNT(*) FROM parameter_types_test")
        .await?;
    if count != 1 {
        return Err(anyhow!("Expected 1 row, got {}", count));
    }

    // Query back and verify values (basic check for non-null)
    let id = client
        .query_single_i64("SELECT id FROM parameter_types_test")
        .await?;
    if id != 1 {
        return Err(anyhow!("Expected id 1, got {}", id));
    }

    // Check each column is not null
    let columns = vec![
        "date32_col",
        "date64_col",
        "time32_sec_col",
        "time32_ms_col",
        "time64_us_col",
        "time64_ns_col",
        "interval_ym_col",
        "interval_dt_col",
        "interval_mdn_col",
    ];

    for col in columns {
        let col_count = client
            .query_single_i64(&format!(
                "SELECT COUNT({}) FROM parameter_types_test WHERE {} IS NOT NULL",
                col, col
            ))
            .await?;
        if col_count != 1 {
            return Err(anyhow!("{} column not inserted correctly", col));
        }
    }

    // Clean up
    client.exec("DROP TABLE parameter_types_test").await?;

    Ok(())
}
