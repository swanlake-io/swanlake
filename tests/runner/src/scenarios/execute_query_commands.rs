use crate::CliArgs;
use anyhow::{bail, Context, Result};
use arrow_array::{Array, Int64Array};
use swanlake_client::FlightSQLClient;
use tracing::info;

const TABLE_NAME: &str = "execute_query_accepts_commands";

pub async fn run_execute_query_commands(args: &CliArgs) -> Result<()> {
    info!("Running ExecuteQuery compatibility tests for command statements");

    exercise_commands_via_execute(args)?;

    info!("ExecuteQuery compatibility tests completed");
    Ok(())
}

fn exercise_commands_via_execute(args: &CliArgs) -> Result<()> {
    let mut client = FlightSQLClient::connect(&args.endpoint)
        .context("failed to connect via FlightSQLClient")?;
    client.execute("USE swanlake")?;

    client.execute(&format!("DROP TABLE IF EXISTS {TABLE_NAME}"))?;
    client.execute(&format!(
        "CREATE TABLE {TABLE_NAME} (id INTEGER, name VARCHAR)"
    ))?;
    client.execute(&format!(
        "INSERT INTO {TABLE_NAME} VALUES (1, 'alice'), (2, 'bob')"
    ))?;

    let result = client.execute(&format!("SELECT COUNT(*) FROM {TABLE_NAME}"))?;
    let first_batch = result
        .batches
        .first()
        .context("expected count batch from SELECT")?;
    let counts = first_batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .context("expected Int64 count column")?;
    if counts.value(0) != 2 {
        bail!("expected 2 rows after insert via ExecuteQuery commands");
    }

    client.execute(&format!("DROP TABLE {TABLE_NAME}"))?;
    Ok(())
}
