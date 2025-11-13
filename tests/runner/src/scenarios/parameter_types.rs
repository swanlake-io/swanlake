use std::sync::Arc;

use anyhow::{anyhow, Result};
use arrow_array::{
    ArrayRef, Date32Array, Date64Array, Int32Array, IntervalDayTimeArray,
    IntervalMonthDayNanoArray, IntervalYearMonthArray, RecordBatch, Time32MillisecondArray,
    Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
};
use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Timelike};

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

    // Insert row via prepared statement to exercise Arrow->DuckDB conversions
    let params = build_parameter_batch()?;
    client
        .exec_prepared(
            "INSERT INTO parameter_types_test VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            params,
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

fn build_parameter_batch() -> Result<RecordBatch> {
    let epoch_date = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let base_date = NaiveDate::from_ymd_opt(2023, 12, 25).unwrap();
    let base_time = NaiveTime::from_hms_opt(10, 20, 30).unwrap();

    let date32_value = base_date.signed_duration_since(epoch_date).num_days() as i32;
    let date64_value = NaiveDateTime::new(base_date, NaiveTime::from_hms_opt(0, 0, 0).unwrap())
        .signed_duration_since(NaiveDateTime::new(
            epoch_date,
            NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
        ))
        .num_milliseconds();

    let seconds = base_time.num_seconds_from_midnight() as i32;
    let millis = seconds * 1_000 + (base_time.nanosecond() as i32 / 1_000_000);
    let micros = i64::from(seconds) * 1_000_000 + i64::from(base_time.nanosecond()) / 1_000;
    let nanos = i64::from(seconds) * 1_000_000_000 + i64::from(base_time.nanosecond());

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("date32_col", DataType::Date32, false),
        Field::new("date64_col", DataType::Date64, false),
        Field::new("time32_sec_col", DataType::Time32(TimeUnit::Second), false),
        Field::new(
            "time32_ms_col",
            DataType::Time32(TimeUnit::Millisecond),
            false,
        ),
        Field::new(
            "time64_us_col",
            DataType::Time64(TimeUnit::Microsecond),
            false,
        ),
        Field::new(
            "time64_ns_col",
            DataType::Time64(TimeUnit::Nanosecond),
            false,
        ),
        Field::new(
            "interval_ym_col",
            DataType::Interval(IntervalUnit::YearMonth),
            false,
        ),
        Field::new(
            "interval_dt_col",
            DataType::Interval(IntervalUnit::DayTime),
            false,
        ),
        Field::new(
            "interval_mdn_col",
            DataType::Interval(IntervalUnit::MonthDayNano),
            false,
        ),
    ]));

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        Arc::new(Date32Array::from(vec![date32_value])),
        Arc::new(Date64Array::from(vec![date64_value])),
        Arc::new(Time32SecondArray::from(vec![seconds])),
        Arc::new(Time32MillisecondArray::from(vec![millis])),
        Arc::new(Time64MicrosecondArray::from(vec![micros])),
        Arc::new(Time64NanosecondArray::from(vec![nanos])),
        Arc::new(IntervalYearMonthArray::from(vec![14])),
        Arc::new(IntervalDayTimeArray::from(vec![IntervalDayTime::new(
            3,
            4 * 60 * 60 * 1000,
        )])),
        Arc::new(IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNano::new(5, 6, 7_000_000_000),
        ])),
    ];

    Ok(RecordBatch::try_new(schema, arrays)?)
}
