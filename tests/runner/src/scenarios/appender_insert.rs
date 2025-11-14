use crate::CliArgs;
use anyhow::{Context, Result};
use arrow_array::{Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use flight_sql_client::FlightSQLClient;
use std::sync::Arc;
use tracing::info;

pub async fn run(args: &CliArgs) -> Result<()> {
    info!("Running appender insert test");

    let mut client = FlightSQLClient::connect(args.endpoint())
        .context("failed to connect to FlightSQL server")?;

    // Create a test table
    client.execute_update("CREATE TABLE IF NOT EXISTS appender_test (id INT, name VARCHAR)")?;

    info!("Created test table appender_test");

    // Create a RecordBatch with test data
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);

    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(id_array), Arc::new(name_array)],
    )?;

    info!("Created RecordBatch with {} rows", batch.num_rows());

    // Insert using prepared statement (which will use appender internally)
    let result = client.execute_batch_update(
        "INSERT INTO appender_test (id, name) VALUES (?, ?)",
        batch,
    )?;

    info!("Inserted rows using appender: {:?}", result);

    // Verify the data was inserted
    let result = client.run_statement("SELECT COUNT(*) as count FROM appender_test")?;
    
    match result {
        flight_sql_client::StatementResult::Query { batches, .. } => {
            let count: i64 = if let Some(batch) = batches.into_iter().next() {
                let count_array = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .context("expected Int64Array")?;
                count_array.value(0)
            } else {
                0
            };

            info!("Row count after insert: {}", count);
            
            if count != 5 {
                anyhow::bail!("Expected 5 rows, but got {}", count);
            }
        }
        _ => anyhow::bail!("Expected query result"),
    }

    // Clean up
    client.execute_update("DROP TABLE appender_test")?;
    info!("Cleaned up test table");

    info!("Appender insert test completed successfully");
    Ok(())
}
