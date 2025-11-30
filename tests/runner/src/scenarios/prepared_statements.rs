use std::sync::Arc;

use anyhow::{anyhow, ensure, Context, Result};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, IntervalDayTimeArray,
    IntervalMonthDayNanoArray, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray,
    Time32MillisecondArray, Time64MicrosecondArray, Time64NanosecondArray,
    TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use swanlake_client::{FlightSQLClient, QueryResult};

use crate::CliArgs;

const PREPARED_UPDATE_TABLE: &str = "prepared_update_test";
const PREPARED_DELETE_TABLE: &str = "prepared_delete_test";
const PREPARED_SELECT_TABLE: &str = "prepared_select_test";

pub async fn run_prepared_statements(args: &CliArgs) -> Result<()> {
    let endpoint = &args.endpoint;
    let mut client = FlightSQLClient::connect(endpoint)?;

    let mut tester = PreparedStatementTester::new(&mut client);
    tester.test_update_with_parameters()?;
    tester.test_delete_with_parameters()?;
    tester.test_select_with_parameters()?;
    tester.test_insert_with_column_alignment()?;
    tester.test_current_catalog_unqualified_insert()?;
    tester.test_default_catalog_unqualified_insert()?;
    Ok(())
}

struct PreparedStatementTester<'a> {
    client: &'a mut FlightSQLClient,
}

impl<'a> PreparedStatementTester<'a> {
    fn new(client: &'a mut FlightSQLClient) -> Self {
        Self { client }
    }

    fn test_update_with_parameters(&mut self) -> Result<()> {
        self.drop_table_if_exists(PREPARED_UPDATE_TABLE)?;
        let test_result = (|| -> Result<()> {
            self.update(
                r#"
                CREATE TABLE prepared_update_test (
                    id INTEGER PRIMARY KEY,
                    int8_col TINYINT,
                    int16_col SMALLINT,
                    int32_col INTEGER,
                    int64_col BIGINT,
                    uint8_col UTINYINT,
                    uint16_col USMALLINT,
                    uint32_col UINTEGER,
                    uint64_col UBIGINT,
                    float32_col FLOAT,
                    float64_col DOUBLE,
                    bool_col BOOLEAN,
                    string_col VARCHAR,
                    binary_col BLOB,
                    date_col DATE,
                    time_col TIME,
                    timestamp_col TIMESTAMP,
                    interval_dt_col INTERVAL,
                    interval_mdn_col INTERVAL,
                    date64_col DATE,
                    time32_col TIME,
                    time64_ns_col TIME,
                    interval_ym_col INTERVAL
                )
                "#,
            )?;
            self.update(
                r#"
                INSERT INTO prepared_update_test VALUES (
                    1,
                    0, 0, 0, 0,
                    0, 0, 0, 0,
                    0.0, 0.0,
                    false,
                    'initial',
                    'initial'::BLOB,
                    DATE '2000-01-01',
                    TIME '00:00:00',
                    TIMESTAMP '2000-01-01 00:00:00',
                    INTERVAL '0 days',
                    INTERVAL '0 days',
                    DATE '2000-01-01',
                    TIME '00:00:00',
                    TIME '00:00:00',
                    INTERVAL '0 months'
                )
                "#,
            )?;

            let update_params = build_update_parameter_batch()?;
            self.client.update_with_record_batch(
                r#"
                UPDATE prepared_update_test SET
                    int8_col = ?,
                    int16_col = ?,
                    int32_col = ?,
                    int64_col = ?,
                    uint8_col = ?,
                    uint16_col = ?,
                    uint32_col = ?,
                    uint64_col = ?,
                    float32_col = ?,
                    float64_col = ?,
                    bool_col = ?,
                    string_col = ?,
                    binary_col = ?,
                    date_col = ?,
                    time_col = ?,
                    timestamp_col = ?,
                    interval_dt_col = ?,
                    interval_mdn_col = ?,
                    date64_col = ?,
                    time32_col = ?,
                    time64_ns_col = ?,
                    interval_ym_col = ?
                WHERE id = ?
                "#,
                update_params,
            )?;

            self.verify_update_results()?;
            Ok(())
        })();

        self.finalize_table(PREPARED_UPDATE_TABLE, test_result)
    }

    fn verify_update_results(&mut self) -> Result<()> {
        let result = self.client.query(
            r#"
            SELECT CAST(int8_col AS INTEGER) AS int8_col,
                   int16_col,
                   int32_col,
                   int64_col,
                   CAST(uint8_col AS INTEGER) AS uint8_col,
                   CAST(uint16_col AS INTEGER) AS uint16_col,
                   CAST(uint32_col AS BIGINT) AS uint32_col,
                   float32_col,
                   float64_col,
                   bool_col,
                   string_col,
                   CAST((date64_col - DATE '1970-01-01') * 86400000 AS BIGINT) AS date64_col,
                   CAST(EXTRACT(epoch FROM time32_col) * 1000 AS BIGINT) AS time32_col,
                   CAST(EXTRACT(epoch FROM time64_ns_col) * 1000000000 AS BIGINT) AS time64_ns_col
            FROM prepared_update_test
            WHERE id = 1
            "#,
        )?;
        let batch = result
            .batches
            .first()
            .context("expected row in prepared_update_test after update")?;

        self.expect_i64(batch, 0, 42, "int8_col")?;
        self.expect_i64(batch, 1, 1000, "int16_col")?;
        self.expect_i64(batch, 2, 100000, "int32_col")?;
        self.expect_i64(batch, 3, 1000000000, "int64_col")?;
        self.expect_i64(batch, 4, 255, "uint8_col")?;
        self.expect_i64(batch, 5, 65000, "uint16_col")?;
        self.expect_i64(batch, 6, 4000000000, "uint32_col")?;
        self.expect_f64(batch, 7, std::f32::consts::PI as f64, "float32_col")?;
        self.expect_f64(batch, 8, std::f64::consts::E, "float64_col")?;
        self.expect_bool(batch, 9, true, "bool_col")?;
        self.expect_string(batch, 10, "updated value", "string_col")?;
        self.expect_i64(batch, 11, 20081i64 * 24 * 3600 * 1000, "date64_col")?;
        self.expect_i64(
            batch,
            12,
            ((14 * 3600 + 30 * 60 + 45) * 1000) as i64,
            "time32_col",
        )?;
        self.expect_i64(
            batch,
            13,
            (14 * 3600 + 30 * 60 + 45) * 1_000_000 * 1000,
            "time64_ns_col",
        )?;
        Ok(())
    }

    fn test_delete_with_parameters(&mut self) -> Result<()> {
        self.drop_table_if_exists(PREPARED_DELETE_TABLE)?;
        let test_result = (|| -> Result<()> {
            self.update(
                r#"
                CREATE TABLE prepared_delete_test (
                    id INTEGER,
                    category VARCHAR,
                    value INTEGER,
                    created_date DATE,
                    is_active BOOLEAN
                )
                "#,
            )?;
            self.update(
                r#"
                INSERT INTO prepared_delete_test VALUES
                    (1, 'A', 100, DATE '2024-01-01', true),
                    (2, 'B', 200, DATE '2024-01-02', true),
                    (3, 'A', 300, DATE '2024-01-03', false),
                    (4, 'C', 400, DATE '2024-01-04', true)
                "#,
            )?;

            self.client.update_with_record_batch(
                "DELETE FROM prepared_delete_test WHERE category = ?",
                build_delete_string_params()?,
            )?;
            self.assert_remaining_ids(&[2, 4])?;

            self.client.update_with_record_batch(
                "DELETE FROM prepared_delete_test WHERE value > ?",
                build_delete_int_params()?,
            )?;
            self.assert_remaining_ids(&[2])?;

            self.client.update_with_record_batch(
                "DELETE FROM prepared_delete_test WHERE is_active = ?",
                build_delete_bool_params()?,
            )?;
            self.assert_remaining_ids(&[])?;
            Ok(())
        })();

        self.finalize_table(PREPARED_DELETE_TABLE, test_result)
    }

    fn test_select_with_parameters(&mut self) -> Result<()> {
        self.drop_table_if_exists(PREPARED_SELECT_TABLE)?;
        let test_result = (|| -> Result<()> {
            self.update(
                r#"
                CREATE TABLE prepared_select_test (
                    id INTEGER,
                    name VARCHAR,
                    score DOUBLE,
                    created_at TIMESTAMP,
                    metadata BLOB
                )
                "#,
            )?;
            self.update(
                r#"
                INSERT INTO prepared_select_test VALUES
                    (1, 'Alice', 95.5, TIMESTAMP '2024-01-01 10:00:00', 'meta1'::BLOB),
                    (2, 'Bob', 87.3, TIMESTAMP '2024-01-02 11:00:00', 'meta2'::BLOB),
                    (3, 'Charlie', 92.1, TIMESTAMP '2024-01-03 12:00:00', 'meta3'::BLOB),
                    (4, 'Diana', 88.9, TIMESTAMP '2024-01-04 13:00:00', 'meta4'::BLOB)
                "#,
            )?;

            let param_schema =
                Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
            let params = RecordBatch::try_new(
                param_schema,
                vec![Arc::new(StringArray::from(vec!["Charlie"])) as ArrayRef],
            )?;
            let result = self.client.query_with_param(
                "SELECT score, metadata FROM prepared_select_test WHERE name = ?",
                params,
            )?;
            self.verify_select_result(result)?;
            Ok(())
        })();

        self.finalize_table(PREPARED_SELECT_TABLE, test_result)
    }

    fn test_insert_with_column_alignment(&mut self) -> Result<()> {
        const TABLE: &str = "swanlake.prepared_alignment_test";
        self.drop_table_if_exists(TABLE)?;
        let test_result = (|| -> Result<()> {
            self.update(
                "CREATE TABLE IF NOT EXISTS swanlake.prepared_alignment_test (id INTEGER, name VARCHAR, active BOOLEAN)",
            )?;

            let schema = Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("id", DataType::Int32, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["alpha", "beta"])) as ArrayRef,
                    Arc::new(Int32Array::from(vec![11, 12])),
                ],
            )?;

            self.client.update_with_record_batch(
                "INSERT INTO swanlake.prepared_alignment_test (name, id) VALUES (?, ?)",
                batch,
            )?;

            let result = self.client.query(
                "SELECT id, name, active FROM swanlake.prepared_alignment_test ORDER BY id",
            )?;
            let batch = result
                .batches
                .first()
                .context("expected rows after prepared insert")?;

            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .context("expected Int32 array for id column")?;
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("expected Utf8 array for name column")?;
            let flags = batch
                .column(2)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .context("expected Boolean array for active column")?;

            ensure!(
                ids.value(0) == 11 && ids.value(1) == 12,
                "ids not aligned correctly"
            );
            ensure!(
                names.value(0) == "alpha" && names.value(1) == "beta",
                "names not aligned correctly"
            );
            ensure!(
                flags.is_null(0) && flags.is_null(1),
                "missing active column should be NULL"
            );
            Ok(())
        })();

        self.finalize_table(TABLE, test_result)
    }

    /// Verify unqualified INSERTs honor the session's current catalog (after USE).
    fn test_current_catalog_unqualified_insert(&mut self) -> Result<()> {
        const TABLE: &str = "swanlake.current_catalog_test";
        self.drop_table_if_exists(TABLE)?;
        let test_result = (|| -> Result<()> {
            // Establish schema in swanlake and switch session to that catalog.
            self.update("CREATE TABLE swanlake.current_catalog_test (id INTEGER, name VARCHAR)")?;
            self.update("USE swanlake")?;

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int32Array::from(vec![7, 8])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["cat-a", "cat-b"])),
                ],
            )?;

            // Unqualified table name should resolve to the current catalog, not default fallback.
            self.client.update_with_record_batch(
                "INSERT INTO current_catalog_test (id, name) VALUES (?, ?)",
                batch,
            )?;

            let result = self
                .client
                .execute("SELECT id, name FROM swanlake.current_catalog_test ORDER BY id")?;
            let batch = result
                .batches
                .first()
                .context("expected rows after current catalog insert")?;

            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .context("expected ids in current catalog test")?;
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("expected names in current catalog test")?;

            ensure!(
                ids.value(0) == 7 && ids.value(1) == 8,
                "unexpected ids in current catalog test"
            );
            ensure!(
                names.value(0) == "cat-a" && names.value(1) == "cat-b",
                "unexpected names in current catalog test"
            );
            Ok(())
        })();

        self.finalize_table(TABLE, test_result)
    }

    /// Verify unqualified INSERTs fall back to the target catalog when no USE has been issued.
    fn test_default_catalog_unqualified_insert(&mut self) -> Result<()> {
        const TABLE: &str = "swanlake.default_catalog_test";
        self.drop_table_if_exists(TABLE)?;
        let test_result = (|| -> Result<()> {
            self.update("CREATE TABLE swanlake.default_catalog_test (id INTEGER, name VARCHAR)")?;

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int32Array::from(vec![1])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["default-cat"])),
                ],
            )?;

            // No USE issued; unqualified insert should pick configured target catalog.
            self.client.update_with_record_batch(
                "INSERT INTO default_catalog_test (id, name) VALUES (?, ?)",
                batch,
            )?;

            let result = self
                .client
                .execute("SELECT id, name FROM swanlake.default_catalog_test ORDER BY id")?;
            let batch = result
                .batches
                .first()
                .context("expected rows after default catalog insert")?;

            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .context("expected ids in default catalog test")?;
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("expected names in default catalog test")?;

            ensure!(ids.value(0) == 1, "unexpected id in default catalog test");
            ensure!(
                names.value(0) == "default-cat",
                "unexpected name in default catalog test"
            );
            Ok(())
        })();

        self.finalize_table(TABLE, test_result)
    }

    fn verify_select_result(&self, result: QueryResult) -> Result<()> {
        ensure!(
            result.total_rows == 1,
            "expected one row for prepared_select_test lookup, got {}",
            result.total_rows
        );
        let batch = result
            .batches
            .first()
            .context("expected result batch for prepared_select_test lookup")?;
        let score = read_f64(batch.column(0).as_ref(), 0)?;
        ensure!(
            (score - 92.1).abs() < 1e-6,
            "expected score 92.1, got {}",
            score
        );
        let metadata = read_string(batch.column(1).as_ref(), 0)?;
        ensure!(
            metadata == "meta3",
            "expected metadata 'meta3', got '{}'",
            metadata
        );
        Ok(())
    }

    fn assert_remaining_ids(&mut self, expected_ids: &[i64]) -> Result<()> {
        let ids = self.collect_i64_column("SELECT id FROM prepared_delete_test ORDER BY id")?;
        ensure!(
            ids == expected_ids,
            "expected ids {:?}, got {:?}",
            expected_ids,
            ids
        );
        Ok(())
    }

    fn collect_i64_column(&mut self, sql: &str) -> Result<Vec<i64>> {
        let result = self.client.query(sql)?;
        let mut values = Vec::new();
        for batch in result.batches {
            let column = batch.column(0);
            for row_idx in 0..batch.num_rows() {
                values.push(read_i64(column.as_ref(), row_idx)?);
            }
        }
        Ok(values)
    }

    fn expect_i64(
        &self,
        batch: &RecordBatch,
        column_idx: usize,
        expected: i64,
        label: &str,
    ) -> Result<()> {
        let actual = read_i64(batch.column(column_idx).as_ref(), 0)?;
        ensure!(
            actual == expected,
            "expected {label} = {expected}, got {actual}"
        );
        Ok(())
    }

    fn expect_f64(
        &self,
        batch: &RecordBatch,
        column_idx: usize,
        expected: f64,
        label: &str,
    ) -> Result<()> {
        let actual = read_f64(batch.column(column_idx).as_ref(), 0)?;
        ensure!(
            (actual - expected).abs() < 1e-6,
            "expected {label} ≈ {expected}, got {actual}"
        );
        Ok(())
    }

    fn expect_bool(
        &self,
        batch: &RecordBatch,
        column_idx: usize,
        expected: bool,
        label: &str,
    ) -> Result<()> {
        let actual = read_bool(batch.column(column_idx).as_ref(), 0)?;
        ensure!(
            actual == expected,
            "expected {label} = {expected}, got {actual}"
        );
        Ok(())
    }

    fn expect_string(
        &self,
        batch: &RecordBatch,
        column_idx: usize,
        expected: &str,
        label: &str,
    ) -> Result<()> {
        let actual = read_string(batch.column(column_idx).as_ref(), 0)?;
        ensure!(
            actual == expected,
            "expected {label} = '{expected}', got '{actual}'"
        );
        Ok(())
    }

    fn update(&mut self, sql: &str) -> Result<()> {
        let _ = self.client.update(sql)?;
        Ok(())
    }

    fn drop_table_if_exists(&mut self, table: &str) -> Result<()> {
        let drop_sql = format!("DROP TABLE IF EXISTS {table}");
        let _ = self.client.update(&drop_sql)?;
        Ok(())
    }

    fn finalize_table(&mut self, table: &str, test_result: Result<()>) -> Result<()> {
        let drop_result = self.drop_table_if_exists(table);
        match (test_result, drop_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(err), _) => Err(err),
            (Ok(()), Err(drop_err)) => Err(drop_err),
        }
    }
}

fn read_i64(column: &dyn Array, idx: usize) -> Result<i64> {
    if column.is_null(idx) {
        return Err(anyhow!("value is NULL"));
    }
    if let Some(arr) = column.as_any().downcast_ref::<Int64Array>() {
        return Ok(arr.value(idx));
    }
    if let Some(arr) = column.as_any().downcast_ref::<Int32Array>() {
        return Ok(arr.value(idx) as i64);
    }
    if let Some(arr) = column.as_any().downcast_ref::<Int16Array>() {
        return Ok(arr.value(idx) as i64);
    }
    if let Some(arr) = column.as_any().downcast_ref::<Int8Array>() {
        return Ok(arr.value(idx) as i64);
    }
    if let Some(arr) = column.as_any().downcast_ref::<UInt64Array>() {
        return Ok(arr.value(idx) as i64);
    }
    if let Some(arr) = column.as_any().downcast_ref::<UInt32Array>() {
        return Ok(arr.value(idx) as i64);
    }
    if let Some(arr) = column.as_any().downcast_ref::<UInt16Array>() {
        return Ok(arr.value(idx) as i64);
    }
    if let Some(arr) = column.as_any().downcast_ref::<UInt8Array>() {
        return Ok(arr.value(idx) as i64);
    }
    Err(anyhow!(
        "unsupported column type {} for integer projection",
        column.data_type()
    ))
}

fn read_f64(column: &dyn Array, idx: usize) -> Result<f64> {
    if column.is_null(idx) {
        return Err(anyhow!("value is NULL"));
    }
    if let Some(arr) = column.as_any().downcast_ref::<Float64Array>() {
        return Ok(arr.value(idx));
    }
    if let Some(arr) = column.as_any().downcast_ref::<Float32Array>() {
        return Ok(arr.value(idx) as f64);
    }
    Err(anyhow!(
        "unsupported column type {} for float projection",
        column.data_type()
    ))
}

fn read_bool(column: &dyn Array, idx: usize) -> Result<bool> {
    if column.is_null(idx) {
        return Err(anyhow!("value is NULL"));
    }
    if let Some(arr) = column.as_any().downcast_ref::<BooleanArray>() {
        return Ok(arr.value(idx));
    }
    Err(anyhow!(
        "unsupported column type {} for boolean projection",
        column.data_type()
    ))
}

fn read_string(column: &dyn Array, idx: usize) -> Result<String> {
    if column.is_null(idx) {
        return Err(anyhow!("value is NULL"));
    }
    if let Some(arr) = column.as_any().downcast_ref::<StringArray>() {
        return Ok(arr.value(idx).to_string());
    }
    if let Some(arr) = column.as_any().downcast_ref::<LargeStringArray>() {
        return Ok(arr.value(idx).to_string());
    }
    if let Some(arr) = column.as_any().downcast_ref::<BinaryArray>() {
        return Ok(binary_bytes_to_string(arr.value(idx)));
    }
    if let Some(arr) = column.as_any().downcast_ref::<LargeBinaryArray>() {
        return Ok(binary_bytes_to_string(arr.value(idx)));
    }
    Err(anyhow!(
        "unsupported column type {} for string projection",
        column.data_type()
    ))
}

fn binary_bytes_to_string(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) => text.to_string(),
        Err(_) => format!("{:?}", bytes),
    }
}

fn build_update_parameter_batch() -> Result<RecordBatch> {
    // Build a batch with one row containing all the parameter values
    let schema = Arc::new(Schema::new(vec![
        Field::new("int8_col", DataType::Int8, false),
        Field::new("int16_col", DataType::Int16, false),
        Field::new("int32_col", DataType::Int32, false),
        Field::new("int64_col", DataType::Int64, false),
        Field::new("uint8_col", DataType::UInt8, false),
        Field::new("uint16_col", DataType::UInt16, false),
        Field::new("uint32_col", DataType::UInt32, false),
        Field::new("uint64_col", DataType::UInt64, false),
        Field::new("float32_col", DataType::Float32, false),
        Field::new("float64_col", DataType::Float64, false),
        Field::new("bool_col", DataType::Boolean, false),
        Field::new("string_col", DataType::Utf8, false),
        Field::new("binary_col", DataType::Binary, false),
        Field::new("date_col", DataType::Date32, false),
        Field::new("time_col", DataType::Time64(TimeUnit::Microsecond), false),
        Field::new(
            "timestamp_col",
            DataType::Timestamp(TimeUnit::Microsecond, None),
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
        Field::new("date64_col", DataType::Date64, false),
        Field::new("time32_col", DataType::Time32(TimeUnit::Millisecond), false),
        Field::new(
            "time64_ns_col",
            DataType::Time64(TimeUnit::Nanosecond),
            false,
        ),
        Field::new(
            "interval_ym_col",
            DataType::Interval(IntervalUnit::MonthDayNano),
            false,
        ),
        Field::new("id", DataType::Int32, false),
    ]));

    // Date: 2024-12-25 (days since 1970-01-01)
    let date32_value = 20081; // 2024-12-25
    let date64_value = date32_value as i64 * 24 * 3600 * 1000; // milliseconds

    // Time: 14:30:45 in microseconds
    let time_us = (14 * 3600 + 30 * 60 + 45) * 1_000_000i64;
    let time_ms = (14 * 3600 + 30 * 60 + 45) * 1000;
    let time_ns = time_us * 1000;

    // Timestamp: 2024-12-25 14:30:45 in microseconds since epoch
    let timestamp_us = 1735138245000000i64;

    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from(vec![42])) as ArrayRef,
        Arc::new(Int16Array::from(vec![1000])),
        Arc::new(Int32Array::from(vec![100000])),
        Arc::new(Int64Array::from(vec![1000000000])),
        Arc::new(UInt8Array::from(vec![255])),
        Arc::new(UInt16Array::from(vec![65000])),
        Arc::new(UInt32Array::from(vec![4000000000])),
        Arc::new(UInt64Array::from(vec![18000000000000000000])),
        Arc::new(Float32Array::from(vec![std::f32::consts::PI])),
        Arc::new(Float64Array::from(vec![std::f64::consts::E])),
        Arc::new(BooleanArray::from(vec![true])),
        Arc::new(StringArray::from(vec!["updated value"])),
        Arc::new(BinaryArray::from(vec![b"binary data" as &[u8]])),
        Arc::new(Date32Array::from(vec![date32_value])),
        Arc::new(Time64MicrosecondArray::from(vec![time_us])),
        Arc::new(TimestampMicrosecondArray::from(vec![timestamp_us])),
        Arc::new(IntervalDayTimeArray::from(vec![IntervalDayTime::new(
            5,
            3 * 60 * 60 * 1000,
        )])),
        Arc::new(IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNano::new(2, 15, 500_000_000),
        ])),
        Arc::new(Date64Array::from(vec![date64_value])),
        Arc::new(Time32MillisecondArray::from(vec![time_ms])),
        Arc::new(Time64NanosecondArray::from(vec![time_ns])),
        Arc::new(IntervalMonthDayNanoArray::from(vec![
            IntervalMonthDayNano::new(18, 0, 0),
        ])),
        Arc::new(Int32Array::from(vec![1])), // WHERE id = 1
    ];

    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn build_delete_string_params() -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "category",
        DataType::Utf8,
        false,
    )]));

    let arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec!["A"])) as ArrayRef];

    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn build_delete_int_params() -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));

    let arrays: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![250])) as ArrayRef];

    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn build_delete_bool_params() -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "is_active",
        DataType::Boolean,
        false,
    )]));

    let arrays: Vec<ArrayRef> = vec![Arc::new(BooleanArray::from(vec![true])) as ArrayRef];

    Ok(RecordBatch::try_new(schema, arrays)?)
}
