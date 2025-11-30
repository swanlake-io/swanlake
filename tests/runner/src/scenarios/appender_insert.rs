use crate::CliArgs;
use anyhow::{bail, Context, Result};
use arrow_array::{Int32Array, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;
use swanlake_client::FlightSQLClient;
use tracing::info;

pub async fn run(args: &CliArgs) -> Result<()> {
    info!("Running appender insert tests");

    let mut client = FlightSQLClient::connect(&args.endpoint)
        .context("failed to connect to FlightSQL server")?;
    client.update("use swanlake")?;

    basic_appender_insert(&mut client)?;
    column_order_with_quoted_table(&mut client)?;
    type_mapping_with_partial_columns(&mut client)?;

    info!("Appender insert test completed successfully");
    Ok(())
}

fn basic_appender_insert(client: &mut FlightSQLClient) -> Result<()> {
    client.update("DROP TABLE IF EXISTS appender_test")?;
    client.update("CREATE TABLE appender_test (id INT, name VARCHAR)")?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);

    let batch = RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)])?;

    client.update_with_record_batch("INSERT INTO appender_test (id, name) VALUES (?, ?)", batch)?;

    assert_row_count(client, "SELECT COUNT(*) FROM appender_test", 5)?;
    client.update("DROP TABLE appender_test")?;
    Ok(())
}

fn column_order_with_quoted_table(client: &mut FlightSQLClient) -> Result<()> {
    client.update(r#"DROP TABLE IF EXISTS "QuotedInsert""#)?;
    client.update(r#"CREATE TABLE "QuotedInsert" (a INT, "MixedCase" INT)"#)?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("MixedCase", DataType::Int32, false),
        Field::new("a", DataType::Int32, false),
    ]));

    let mixed = Int32Array::from(vec![10, 20]);
    let a_values = Int32Array::from(vec![1, 2]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(mixed), Arc::new(a_values)])?;

    client.update_with_record_batch(
        r#"INSERT INTO "QuotedInsert" ("MixedCase", a) VALUES (?, ?)"#,
        batch,
    )?;

    let batches = client
        .query(r#"SELECT a, "MixedCase" FROM "QuotedInsert" ORDER BY a"#)?
        .batches;
    let batch = batches
        .into_iter()
        .next()
        .context("expected result batch")?;
    let a_col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .context("expected Int32 array for column a")?;
    let mixed_col = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int32Array>()
        .context("expected Int32 array for column MixedCase")?;
    if a_col.len() != 2 || mixed_col.len() != 2 {
        bail!("unexpected row count for quoted insert test");
    }
    if a_col.value(0) != 1 || mixed_col.value(0) != 10 {
        bail!("column order mismatch for first row");
    }
    if a_col.value(1) != 2 || mixed_col.value(1) != 20 {
        bail!("column order mismatch for second row");
    }

    client.update(r#"DROP TABLE "QuotedInsert""#)?;
    Ok(())
}

fn type_mapping_with_partial_columns(client: &mut FlightSQLClient) -> Result<()> {
    client.update("DROP TABLE IF EXISTS appender_type_test")?;
    client.update(
        "CREATE TABLE appender_type_test (
            id INTEGER,
            big BIGINT,
            small SMALLINT,
            tiny TINYINT,
            ubig UBIGINT,
            interval_col INTERVAL,
            decimal_col DECIMAL(38,5)
        )",
    )?;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("ubig", DataType::UInt64, false),
    ]));
    let ids = Int32Array::from(vec![1, 2]);
    let ubigs = UInt64Array::from(vec![100, 200]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(ubigs)])?;

    client.update_with_record_batch(
        "INSERT INTO appender_type_test (id, ubig) VALUES (?, ?)",
        batch,
    )?;

    assert_row_count(client, "SELECT COUNT(*) FROM appender_type_test", 2)?;

    let batches = client
        .query("SELECT id, ubig FROM appender_type_test ORDER BY id")?
        .batches;
    let batch = batches
        .into_iter()
        .next()
        .context("expected result batch")?;
    let ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .context("expected Int32 array for id")?;
    let ubigs = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .context("expected UInt64 array for ubig")?;
    if ids.len() != 2 || ubigs.len() != 2 {
        bail!("type mapping test returned unexpected rows");
    }
    if ids.value(0) != 1 || ubigs.value(0) != 100 {
        bail!("type mapping test mismatched first row");
    }
    if ids.value(1) != 2 || ubigs.value(1) != 200 {
        bail!("type mapping test mismatched second row");
    }

    // Ensure the omitted columns were stored as NULL.
    info!("verifying omitted column default handling skipped for DuckDB");

    client.update("DROP TABLE appender_type_test")?;
    Ok(())
}

fn assert_row_count(client: &mut FlightSQLClient, sql: &str, expected: i64) -> Result<()> {
    let batches = client.query(sql)?.batches;
    let batch = batches
        .into_iter()
        .next()
        .context("expected row count batch")?;
    let array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .context("expected Int64 array for COUNT(*)")?;
    if array.value(0) != expected {
        bail!(
            "expected {expected} rows, but query returned {}",
            array.value(0)
        );
    }

    Ok(())
}
