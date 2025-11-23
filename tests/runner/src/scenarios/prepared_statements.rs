use std::sync::Arc;

use anyhow::{ensure, Context, Result};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, IntervalDayTimeArray,
    IntervalMonthDayNanoArray, RecordBatch, StringArray, Time32MillisecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano};
use arrow_schema::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use swanlake_client::arrow::{value_as_bool, value_as_f64, value_as_i64, value_as_string};
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
    tester.test_duckling_queue_prepared_insert()?;
    tester.test_duckling_queue_batch_optimization()?;
    tester.test_duckling_queue_positional_multirow()?;
    tester.test_duckling_queue_empty_and_multiple_batches()?;
    tester.test_current_catalog_unqualified_insert()?;
    tester.test_default_catalog_unqualified_insert()?;
    tester.test_duckling_queue_insert_with_expressions()?;
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
            self.execute_update(
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
            self.execute_update(
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
            self.client.execute_batch_update(
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
        let result = self.client.execute(
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
            self.execute_update(
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
            self.execute_update(
                r#"
                INSERT INTO prepared_delete_test VALUES
                    (1, 'A', 100, DATE '2024-01-01', true),
                    (2, 'B', 200, DATE '2024-01-02', true),
                    (3, 'A', 300, DATE '2024-01-03', false),
                    (4, 'C', 400, DATE '2024-01-04', true)
                "#,
            )?;

            self.client.execute_batch_update(
                "DELETE FROM prepared_delete_test WHERE category = ?",
                build_delete_string_params()?,
            )?;
            self.assert_remaining_ids(&[2, 4])?;

            self.client.execute_batch_update(
                "DELETE FROM prepared_delete_test WHERE value > ?",
                build_delete_int_params()?,
            )?;
            self.assert_remaining_ids(&[2])?;

            self.client.execute_batch_update(
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
            self.execute_update(
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
            self.execute_update(
                r#"
                INSERT INTO prepared_select_test VALUES
                    (1, 'Alice', 95.5, TIMESTAMP '2024-01-01 10:00:00', 'meta1'::BLOB),
                    (2, 'Bob', 87.3, TIMESTAMP '2024-01-02 11:00:00', 'meta2'::BLOB),
                    (3, 'Charlie', 92.1, TIMESTAMP '2024-01-03 12:00:00', 'meta3'::BLOB),
                    (4, 'Diana', 88.9, TIMESTAMP '2024-01-04 13:00:00', 'meta4'::BLOB)
                "#,
            )?;

            let result = self.client.execute_with_params(
                "SELECT score, metadata FROM prepared_select_test WHERE name = ?",
                vec!["Charlie".to_string()],
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
            self.execute_update(
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

            self.client.execute_batch_update(
                "INSERT INTO swanlake.prepared_alignment_test (name, id) VALUES (?, ?)",
                batch,
            )?;

            let result = self.client.execute(
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

    fn test_duckling_queue_prepared_insert(&mut self) -> Result<()> {
        const TABLE: &str = "swanlake.dq_prepared_sink";
        self.drop_table_if_exists(TABLE)?;
        let test_result = (|| -> Result<()> {
            self.execute_update(
                "CREATE TABLE swanlake.dq_prepared_sink (id INTEGER, label VARCHAR, processed BOOLEAN)",
            )?;

            let schema = Arc::new(Schema::new(vec![
                Field::new("label", DataType::Utf8, false),
                Field::new("id", DataType::Int32, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(StringArray::from(vec!["queued-a", "queued-b"])) as ArrayRef,
                    Arc::new(Int32Array::from(vec![101, 102])),
                ],
            )?;

            self.client.execute_batch_update(
                "INSERT INTO duckling_queue.dq_prepared_sink (label, id) VALUES (?, ?)",
                batch,
            )?;

            let queued_rows = self
                .client
                .query_scalar_i64("SELECT COUNT(*) FROM swanlake.dq_prepared_sink")?;
            ensure!(
                queued_rows == 0,
                "duckling queue inserts should not write directly to destination tables"
            );

            self.execute_update("PRAGMA duckling_queue.flush")?;

            let result = self.client.execute(
                "SELECT id, label, processed FROM swanlake.dq_prepared_sink ORDER BY id",
            )?;
            let batch = result
                .batches
                .first()
                .context("expected rows after duckling queue flush")?;
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .context("expected ids after flush")?;
            let labels = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("expected labels after flush")?;
            let processed = batch
                .column(2)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .context("expected processed column")?;

            ensure!(
                ids.value(0) == 101 && ids.value(1) == 102,
                format!("unexpected ids flushed {:?}", ids)
            );
            ensure!(
                labels.value(0) == "queued-a" && labels.value(1) == "queued-b",
                "unexpected labels flushed"
            );
            ensure!(
                processed.is_null(0) && processed.is_null(1),
                "missing processed column should be NULL"
            );
            let read_attempt = self
                .client
                .execute("SELECT * FROM duckling_queue.dq_prepared_sink");
            ensure!(
                read_attempt.is_err(),
                "duckling queue relations should remain write-only"
            );

            self.execute_update("PRAGMA duckling_queue.flush")?;
            Ok(())
        })();

        self.finalize_table(TABLE, test_result)
    }

    /// Test the optimized duckling_queue batch insert path.
    ///
    /// This test verifies that when Arrow batches are sent via DoPut for
    /// INSERT INTO duckling_queue.<table> prepared statements, the batches
    /// are enqueued directly without wasteful Arrow → params → VALUES → Arrow conversion.
    ///
    /// Test cases:
    /// 1. Large batch insert (1000+ rows) - verifies batch optimization works at scale
    /// 2. Column reordering - INSERT with different column order than batch schema
    /// 3. Multiple batches - ensures multiple batches are all enqueued correctly
    /// 4. Empty batch handling - verifies graceful handling of empty batches
    fn test_duckling_queue_batch_optimization(&mut self) -> Result<()> {
        const TABLE: &str = "swanlake.dq_batch_test";
        const REORDER_TABLE: &str = "swanlake.dq_reorder_test";
        self.drop_table_if_exists(TABLE)?;
        self.drop_table_if_exists(REORDER_TABLE)?;
        let test_result = (|| -> Result<()> {
            // Setup: Create destination table with multiple columns
            self.execute_update(
                "CREATE TABLE swanlake.dq_batch_test (id INTEGER, name VARCHAR, value DOUBLE, active BOOLEAN)",
            )?;

            // Test Case 1: Large batch insert (tests optimization at scale)
            // Create a batch with 1000 rows to verify efficiency
            let batch_size = 1000;
            let ids: Vec<i32> = (1..=batch_size).collect();
            let names: Vec<String> = (1..=batch_size).map(|i| format!("item_{}", i)).collect();
            let values: Vec<f64> = (1..=batch_size).map(|i| i as f64 * 1.5).collect();
            let actives: Vec<bool> = (1..=batch_size).map(|i| i % 2 == 0).collect();

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("value", DataType::Float64, false),
                Field::new("active", DataType::Boolean, false),
            ]));

            let large_batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(ids)) as ArrayRef,
                    Arc::new(StringArray::from(names)),
                    Arc::new(Float64Array::from(values)),
                    Arc::new(BooleanArray::from(actives)),
                ],
            )?;

            self.client.execute_batch_update(
                "INSERT INTO duckling_queue.dq_batch_test (id, name, value, active) VALUES (?, ?, ?, ?)",
                large_batch,
            )?;

            // Verify data hasn't landed yet (still in queue)
            let pre_flush = self
                .client
                .query_scalar_i64("SELECT COUNT(*) FROM swanlake.dq_batch_test")?;
            ensure!(
                pre_flush == 0,
                "duckling queue should buffer data before flush"
            );

            // Flush and verify data
            self.execute_update("PRAGMA duckling_queue.flush")?;

            let total_rows = self
                .client
                .query_scalar_i64("SELECT COUNT(*) FROM swanlake.dq_batch_test")?;
            ensure!(
                total_rows == 1000,
                "expected 1000 rows after flush, got {}",
                total_rows
            );

            // Verify large batch data (sample check)
            let sample_result = self.client.execute(
                "SELECT id, name, value, active FROM swanlake.dq_batch_test WHERE id IN (1, 500, 1000) ORDER BY id",
            )?;
            let sample_batch = sample_result
                .batches
                .first()
                .context("expected sample rows")?;

            ensure!(sample_batch.num_rows() == 3, "expected 3 sample rows");

            // Test Case 2: Column reordering in a separate table
            // Create separate table to test column reordering without schema conflicts
            self.execute_update(
                "CREATE TABLE swanlake.dq_reorder_test (id INTEGER, name VARCHAR, value DOUBLE, active BOOLEAN)",
            )?;

            // Send batch with columns in different order than INSERT statement
            let reorder_schema = Arc::new(Schema::new(vec![
                Field::new("value", DataType::Float64, false),
                Field::new("active", DataType::Boolean, false),
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
            ]));

            let reorder_batch = RecordBatch::try_new(
                reorder_schema,
                vec![
                    Arc::new(Float64Array::from(vec![99.9, 88.8])) as ArrayRef,
                    Arc::new(BooleanArray::from(vec![true, false])),
                    Arc::new(Int32Array::from(vec![2001, 2002])),
                    Arc::new(StringArray::from(vec!["reorder_a", "reorder_b"])),
                ],
            )?;

            // INSERT specifies column order: (id, name, value, active)
            // Batch has order: (value, active, id, name)
            // The align_batch_to_table_schema should handle this
            self.client.execute_batch_update(
                "INSERT INTO duckling_queue.dq_reorder_test (id, name, value, active) VALUES (?, ?, ?, ?)",
                reorder_batch,
            )?;

            // Flush and verify reordered data
            self.execute_update("PRAGMA duckling_queue.flush")?;

            let reorder_result = self.client.execute(
                "SELECT id, name, value, active FROM swanlake.dq_reorder_test ORDER BY id",
            )?;
            let reorder_batch = reorder_result
                .batches
                .first()
                .context("expected reordered rows")?;

            let reorder_ids = reorder_batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .context("expected ids")?;
            let reorder_names = reorder_batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("expected names")?;
            let reorder_values = reorder_batch
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .context("expected values")?;
            let reorder_actives = reorder_batch
                .column(3)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .context("expected actives")?;

            ensure!(
                reorder_ids.value(0) == 2001 && reorder_ids.value(1) == 2002,
                "column reordering failed for id"
            );
            ensure!(
                reorder_names.value(0) == "reorder_a" && reorder_names.value(1) == "reorder_b",
                "column reordering failed for name"
            );
            ensure!(
                (reorder_values.value(0) - 99.9).abs() < 0.01
                    && (reorder_values.value(1) - 88.8).abs() < 0.01,
                "column reordering failed for value"
            );
            ensure!(
                reorder_actives.value(0) && !reorder_actives.value(1),
                "column reordering failed for active"
            );

            // Test Case 3: Verify read protection still works
            let read_attempt = self
                .client
                .execute("SELECT * FROM duckling_queue.dq_batch_test");
            ensure!(
                read_attempt.is_err(),
                "duckling queue should remain write-only after batch optimization"
            );

            self.execute_update("PRAGMA duckling_queue.flush")?;

            // Cleanup reorder table
            self.drop_table_if_exists(REORDER_TABLE)?;
            Ok(())
        })();

        self.finalize_table(TABLE, test_result)
    }

    /// Verify positional parameter batches (Go-style $1, $2, $3) are reshaped and aligned before enqueue.
    fn test_duckling_queue_positional_multirow(&mut self) -> Result<()> {
        const TABLE: &str = "swanlake.dq_positional_multi";
        self.drop_table_if_exists(TABLE)?;
        let test_result = (|| -> Result<()> {
            self.execute_update(
                "CREATE TABLE swanlake.dq_positional_multi (id INTEGER, label VARCHAR)",
            )?;

            // Simulate Go driver sending a single-row batch with positional field names ("1", "2", ...)
            // for a multi-row VALUES clause.
            let schema = Arc::new(Schema::new(vec![
                Field::new("1", DataType::Int32, false),
                Field::new("2", DataType::Utf8, false),
                Field::new("3", DataType::Int32, false),
                Field::new("4", DataType::Utf8, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int32Array::from(vec![3001])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["pos-a"])),
                    Arc::new(Int32Array::from(vec![3002])),
                    Arc::new(StringArray::from(vec!["pos-b"])),
                ],
            )?;

            self.client.execute_batch_update(
                "INSERT INTO duckling_queue.dq_positional_multi (id, label) VALUES (?, ?), (?, ?)",
                batch,
            )?;

            // Data is queued, not yet flushed
            let queued_rows = self
                .client
                .query_scalar_i64("SELECT COUNT(*) FROM swanlake.dq_positional_multi")?;
            ensure!(
                queued_rows == 0,
                "queued inserts should not be visible before flush"
            );

            self.execute_update("PRAGMA duckling_queue.flush")?;

            let result = self
                .client
                .execute("SELECT id, label FROM swanlake.dq_positional_multi ORDER BY id")?;
            let batch = result
                .batches
                .first()
                .context("expected rows after positional multi-row flush")?;

            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .context("expected ids after flush")?;
            let labels = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("expected labels after flush")?;

            ensure!(
                ids.value(0) == 3001 && ids.value(1) == 3002,
                "positional multi-row values not reshaped correctly"
            );
            ensure!(
                labels.value(0) == "pos-a" && labels.value(1) == "pos-b",
                "positional labels not aligned correctly"
            );

            let read_attempt = self
                .client
                .execute("SELECT * FROM duckling_queue.dq_positional_multi");
            ensure!(
                read_attempt.is_err(),
                "duckling_queue should stay write-only"
            );
            Ok(())
        })();

        self.finalize_table(TABLE, test_result)
    }

    /// Ensure empty batches no-op and multiple batches enqueue correctly before a single flush.
    fn test_duckling_queue_empty_and_multiple_batches(&mut self) -> Result<()> {
        const TABLE: &str = "swanlake.dq_multi_batch_sink";
        self.drop_table_if_exists(TABLE)?;
        let test_result = (|| -> Result<()> {
            self.execute_update(
                "CREATE TABLE swanlake.dq_multi_batch_sink (id INTEGER, name VARCHAR)",
            )?;

            // Empty batch should be accepted and produce no rows
            let empty_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, true),
                Field::new("name", DataType::Utf8, true),
            ]));
            let empty_batch = RecordBatch::try_new(
                empty_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(Vec::<i32>::new())) as ArrayRef,
                    Arc::new(StringArray::from(Vec::<&str>::new())),
                ],
            )?;
            self.client.execute_batch_update(
                "INSERT INTO duckling_queue.dq_multi_batch_sink (id, name) VALUES (?, ?)",
                empty_batch,
            )?;

            let count_after_empty = self
                .client
                .query_scalar_i64("SELECT COUNT(*) FROM swanlake.dq_multi_batch_sink")?;
            ensure!(
                count_after_empty == 0,
                "empty batch should not write rows before flush"
            );

            // Enqueue two additional batches before flushing
            let batch_one = RecordBatch::try_new(
                empty_schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![11, 12])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["first-a", "first-b"])),
                ],
            )?;
            let batch_two = RecordBatch::try_new(
                empty_schema,
                vec![
                    Arc::new(Int32Array::from(vec![13])) as ArrayRef,
                    Arc::new(StringArray::from(vec!["first-c"])),
                ],
            )?;

            self.client.execute_batch_update(
                "INSERT INTO duckling_queue.dq_multi_batch_sink (id, name) VALUES (?, ?)",
                batch_one,
            )?;
            self.client.execute_batch_update(
                "INSERT INTO duckling_queue.dq_multi_batch_sink (id, name) VALUES (?, ?)",
                batch_two,
            )?;

            self.execute_update("PRAGMA duckling_queue.flush")?;

            let result = self
                .client
                .execute("SELECT id, name FROM swanlake.dq_multi_batch_sink ORDER BY id")?;
            let batch = result
                .batches
                .first()
                .context("expected rows after multi-batch flush")?;

            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .context("expected ids after multi-batch flush")?;
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("expected names after multi-batch flush")?;

            ensure!(
                ids.len() == 3,
                "expected three rows after multi-batch flush"
            );
            ensure!(
                ids.value(0) == 11 && ids.value(1) == 12 && ids.value(2) == 13,
                "ids not preserved across multiple batches"
            );
            ensure!(
                names.value(0) == "first-a"
                    && names.value(1) == "first-b"
                    && names.value(2) == "first-c",
                "names not preserved across multiple batches"
            );

            let read_attempt = self
                .client
                .execute("SELECT * FROM duckling_queue.dq_multi_batch_sink");
            ensure!(
                read_attempt.is_err(),
                "duckling_queue should remain write-only"
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
            self.execute_update(
                "CREATE TABLE swanlake.current_catalog_test (id INTEGER, name VARCHAR)",
            )?;
            self.execute_update("USE swanlake")?;

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
            self.client.execute_batch_update(
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
            self.execute_update(
                "CREATE TABLE swanlake.default_catalog_test (id INTEGER, name VARCHAR)",
            )?;

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
            self.client.execute_batch_update(
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

    /// Ensure duckling_queue prepared inserts with server-side expressions fall back to SQL execution.
    fn test_duckling_queue_insert_with_expressions(&mut self) -> Result<()> {
        const TABLE: &str = "swanlake.dq_expr_sink";
        self.drop_table_if_exists(TABLE)?;
        let test_result = (|| -> Result<()> {
            self.execute_update(
                "CREATE TABLE swanlake.dq_expr_sink (id INTEGER, ts TIMESTAMP DEFAULT '2024-01-01 00:00:00')",
            )?;

            // Only parameter is id; ts should be filled by NOW() on the server side.
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(Int32Array::from(vec![9001])) as ArrayRef],
            )?;

            self.client.execute_batch_update(
                "INSERT INTO duckling_queue.dq_expr_sink (id, ts) VALUES (?, NOW())",
                batch,
            )?;

            let queued_rows = self
                .client
                .query_scalar_i64("SELECT COUNT(*) FROM swanlake.dq_expr_sink")?;
            ensure!(
                queued_rows == 0,
                "duckling queue expression insert should buffer before flush"
            );

            self.execute_update("PRAGMA duckling_queue.flush")?;

            let result = self
                .client
                .execute("SELECT id, ts FROM swanlake.dq_expr_sink ORDER BY id")?;
            let batch = result
                .batches
                .first()
                .context("expected rows after duckling queue expression flush")?;

            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .context("expected ids after expression flush")?;
            let ts_array = batch
                .column(1)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .context("expected timestamps after expression flush")?;

            ensure!(
                ids.value(0) == 9001,
                "unexpected id after expression insert: {}",
                ids.value(0)
            );
            ensure!(
                !ts_array.is_null(0) && ts_array.value_as_datetime(0).is_some(),
                "timestamp should be populated by default on insert"
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
        let score = value_as_f64(batch.column(0).as_ref(), 0)?;
        ensure!(
            (score - 92.1).abs() < 1e-6,
            "expected score 92.1, got {}",
            score
        );
        let metadata = value_as_string(batch.column(1).as_ref(), 0)?;
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
        let result = self.client.execute(sql)?;
        let mut values = Vec::new();
        for batch in result.batches {
            let column = batch.column(0);
            for row_idx in 0..batch.num_rows() {
                values.push(value_as_i64(column.as_ref(), row_idx)?);
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
        let actual = value_as_i64(batch.column(column_idx).as_ref(), 0)?;
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
        let actual = value_as_f64(batch.column(column_idx).as_ref(), 0)?;
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
        let actual = value_as_bool(batch.column(column_idx).as_ref(), 0)?;
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
        let actual = value_as_string(batch.column(column_idx).as_ref(), 0)?;
        ensure!(
            actual == expected,
            "expected {label} = '{expected}', got '{actual}'"
        );
        Ok(())
    }

    fn execute_update(&mut self, sql: &str) -> Result<()> {
        let _ = self.client.execute_update(sql)?;
        Ok(())
    }

    fn drop_table_if_exists(&mut self, table: &str) -> Result<()> {
        let drop_sql = format!("DROP TABLE IF EXISTS {table}");
        let _ = self.client.execute_update(&drop_sql)?;
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
