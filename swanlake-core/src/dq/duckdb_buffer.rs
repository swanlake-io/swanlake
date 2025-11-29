use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Schema, SchemaRef};
use duckdb::{params_from_iter, Connection};
use tracing::{debug, error};

use crate::error::ServerError;

/// Handle that identifies which rows were flushed for a table.
#[derive(Debug, Clone)]
pub struct FlushHandle {
    pub table: String,
    pub max_seq: i64,
}

/// Slice of buffered rows selected for flushing.
#[derive(Debug, Clone)]
pub struct SelectedPayload {
    pub table: String,
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub handle: FlushHandle,
    pub rows: usize,
    pub bytes: u64,
}

/// Basic table metadata captured from the buffer.
#[derive(Debug, Clone)]
pub struct TableState {
    pub table: String,
    pub schema: SchemaRef,
    pub row_count: usize,
}

/// DuckDB-backed buffer that stores staging tables inside a single database file.
pub struct DuckDbBuffer {
    conn: Mutex<Connection>,
    path: PathBuf,
    flush_chunk_rows: usize,
}

impl DuckDbBuffer {
    fn with_conn<F, R>(&self, f: F) -> Result<R, ServerError>
    where
        F: FnOnce(&mut Connection) -> Result<R, ServerError>,
    {
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| ServerError::Internal("buffer connection mutex poisoned".into()))?;
        f(&mut conn)
    }

    pub fn new(root: PathBuf, flush_chunk_rows: usize) -> Result<Self, ServerError> {
        fs::create_dir_all(&root).map_err(|err| {
            ServerError::Internal(format!(
                "failed to create duckling queue root {}: {err}",
                root.display()
            ))
        })?;
        let path = root.join("buffer.duckdb");
        let conn = Connection::open(&path)?;
        let buffer = Self {
            conn: Mutex::new(conn),
            path,
            flush_chunk_rows: flush_chunk_rows.max(1),
        };
        buffer.init_meta_table()?;
        Ok(buffer)
    }

    fn init_meta_table(&self) -> Result<(), ServerError> {
        self.with_conn(|conn| {
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS dq_meta (
                    table_name TEXT PRIMARY KEY,
                    schema_json TEXT NOT NULL,
                    last_flushed_seq BIGINT DEFAULT 0
                );",
            )?;
            Ok(())
        })
    }

    /// Load known tables and their schemas/row counts from the buffer.
    pub fn load_table_states(&self) -> Result<Vec<TableState>, ServerError> {
        self.with_conn(|conn| {
            let mut states = Vec::new();
            let mut stmt =
                conn.prepare("SELECT table_name, schema_json FROM dq_meta ORDER BY table_name")?;
            let rows = stmt.query_map([], |row| {
                let table: String = row.get(0)?;
                let schema_json: String = row.get(1)?;
                Ok((table, schema_json))
            })?;
            let entries = rows
                .collect::<Result<Vec<_>, _>>()
                .map_err(ServerError::DuckDb)?;

            for (table, schema_json) in entries {
                let schema = decode_schema(&schema_json)?;
                let staging = staging_table_name(&table);
                let count: i64 = if staging_exists(conn, &table)? {
                    conn.query_row(
                        &format!("SELECT COUNT(*) FROM {}", quote_ident(&staging)),
                        [],
                        |row| row.get(0),
                    )
                    .map_err(ServerError::DuckDb)?
                } else {
                    error!(
                        table = %table,
                        staging = %staging,
                        "staging table missing during load; recreating from metadata"
                    );
                    self.recreate_staging_table(conn, &table, &schema, &schema_json)?;
                    0
                };
                states.push(TableState {
                    table,
                    schema: Arc::new(schema),
                    row_count: count as usize,
                });
            }

            Ok(states)
        })
    }

    /// Retrieve current schema and row count for a table if it exists.
    pub fn table_info(&self, table: &str) -> Result<Option<(SchemaRef, usize)>, ServerError> {
        self.with_conn(|conn| {
            let mut stmt = conn.prepare(
                "SELECT schema_json, last_flushed_seq FROM dq_meta WHERE table_name = ?",
            )?;
            let stored: Option<String> = stmt
                .query_row(params_from_iter([table]), |row| row.get(0))
                .optional()
                .map_err(ServerError::DuckDb)?;
            let Some(schema_json) = stored else {
                debug!(table = %table, "table_info: no metadata found");
                return Ok(None);
            };
            let schema = decode_schema(&schema_json)?;
            let staging = staging_table_name(table);
            if !staging_exists(conn, table)? {
                // Heal metadata if the staging table was dropped manually.
                error!(
                    table = %table,
                    staging = %staging,
                    "staging table missing in table_info; recreating"
                );
                self.recreate_staging_table(conn, table, &schema, &schema_json)?;
                return Ok(Some((Arc::new(schema), 0)));
            }
            let row_count: i64 = conn
                .query_row(
                    &format!("SELECT COUNT(*) FROM {}", quote_ident(&staging)),
                    [],
                    |row| row.get(0),
                )
                .map_err(ServerError::DuckDb)?;
            Ok(Some((Arc::new(schema), row_count as usize)))
        })
    }

    /// Ensure a staging table exists and matches the incoming schema. If the table exists with
    /// a different schema and still holds buffered rows, an error is returned to avoid dropping
    /// data implicitly. Callers should flush pending rows first.
    pub fn ensure_table(&self, table: &str, schema: &Schema) -> Result<(), ServerError> {
        let schema_json = encode_schema(schema)?;
        self.with_conn(|conn| self.ensure_table_with_conn(conn, table, schema, &schema_json))
    }

    fn create_staging_table(
        &self,
        conn: &mut Connection,
        table: &str,
        schema: &Schema,
        schema_json: &str,
    ) -> Result<(), ServerError> {
        let create_sql = build_create_table_sql(table, schema)?;
        let tx = conn.transaction()?;
        tx.execute_batch(&create_sql)?;
        tx.execute(
            "INSERT OR REPLACE INTO dq_meta(table_name, schema_json, last_flushed_seq) VALUES(?, ?, 0)",
            params_from_iter([table, schema_json]),
        )?;
        tx.commit()?;
        Ok(())
    }

    fn recreate_staging_table(
        &self,
        conn: &mut Connection,
        table: &str,
        schema: &Schema,
        schema_json: &str,
    ) -> Result<(), ServerError> {
        let staging = staging_table_name(table);
        let create_sql = build_create_table_sql(table, schema)?;
        let tx = conn.transaction()?;
        tx.execute_batch(&format!("DROP TABLE IF EXISTS {};", quote_ident(&staging)))?;
        tx.execute_batch(&create_sql)?;
        tx.execute(
            "UPDATE dq_meta SET schema_json = ?, last_flushed_seq = 0 WHERE table_name = ?",
            params_from_iter([schema_json, table]),
        )?;
        tx.commit()?;
        Ok(())
    }

    fn ensure_table_with_conn(
        &self,
        conn: &mut Connection,
        table: &str,
        schema: &Schema,
        schema_json: &str,
    ) -> Result<(), ServerError> {
        let staging = staging_table_name(table);
        let mut stmt = conn.prepare("SELECT schema_json FROM dq_meta WHERE table_name = ?")?;
        let stored: Option<String> = stmt
            .query_row(params_from_iter([table]), |row| row.get(0))
            .optional()
            .map_err(ServerError::DuckDb)?;

        match stored {
            Some(stored_schema) => {
                // If metadata exists but the staging table was dropped externally, recreate it.
                if !staging_exists(conn, table)? {
                    error!(
                        table = %table,
                        staging = %staging,
                        "staging table missing in ensure_table; recreating"
                    );
                    self.recreate_staging_table(conn, table, schema, schema_json)?;
                    return Ok(());
                }

                if stored_schema != schema_json {
                    let row_count: i64 = conn
                        .query_row(
                            &format!("SELECT COUNT(*) FROM {}", quote_ident(&staging)),
                            [],
                            |row| row.get(0),
                        )
                        .map_err(ServerError::DuckDb)?;
                    if row_count > 0 {
                        error!(
                            table = %table,
                            staging = %staging,
                            row_count,
                            "schema mismatch with buffered rows; refusing to recreate staging table"
                        );
                        return Err(ServerError::Internal(format!(
                            "duckling queue schema mismatch for {table}; flush existing buffered rows before schema change"
                        )));
                    }
                    // Safe to recreate the empty staging table with the new schema.
                    self.recreate_staging_table(conn, table, schema, schema_json)?;
                }
            }
            None => {
                self.create_staging_table(conn, table, schema, schema_json)?;
            }
        }
        Ok(())
    }

    /// Replace the staging table with the given schema, dropping any existing table.
    pub fn replace_table_schema(&self, table: &str, schema: &Schema) -> Result<(), ServerError> {
        let schema_json = encode_schema(schema)?;
        self.with_conn(|conn| self.recreate_staging_table(conn, table, schema, &schema_json))
    }

    /// Append RecordBatches into the staging table. Returns (rows, bytes).
    pub fn enqueue(
        &self,
        table: &str,
        schema: &Schema,
        batches: &[RecordBatch],
    ) -> Result<(usize, u64), ServerError> {
        let schema_json = encode_schema(schema)?;
        self.with_conn(|conn| {
            self.ensure_table_with_conn(conn, table, schema, &schema_json)?;
            let staging = staging_table_name(table);
            let ident = staging.clone();
            let exists = staging_exists(conn, table)?;
            if !exists {
                error!(table = %table, staging = %staging, "staging table missing before enqueue; recreating");
                self.recreate_staging_table(conn, table, schema, &schema_json)?;
            }
            let mut appender = match conn.appender(&ident) {
                Ok(appender) => appender,
                Err(err) => {
                    error!(
                        table = %table,
                        staging = %staging,
                        ident = %ident,
                        %err,
                        exists,
                        "failed to open appender for duckling_queue staging table"
                    );
                    return Err(ServerError::DuckDb(err));
                }
            };
            let mut rows = 0usize;
            let mut bytes = 0u64;
            for batch in batches {
                rows += batch.num_rows();
                bytes += batch.get_array_memory_size() as u64;
                appender.append_record_batch(batch.clone())?;
            }
            appender.flush()?;
            Ok((rows, bytes))
        })
    }

    /// Read up to `flush_chunk_rows` ordered by dq_seq for the given table.
    pub fn select_for_flush(&self, table: &str) -> Result<Option<SelectedPayload>, ServerError> {
        let staging = staging_table_name(table);
        self.with_conn(|conn| {
            debug!(
                table = %table,
                staging = %staging,
                limit = self.flush_chunk_rows,
                "select_for_flush: starting scan"
            );

            debug!(table = %table, staging = %staging, "select_for_flush: preparing statement");
            let mut stmt = conn.prepare(&format!(
                "SELECT rowid, * FROM {} ORDER BY rowid LIMIT {}",
                quote_ident(&staging),
                self.flush_chunk_rows
            ))?;
            debug!(table = %table, staging = %staging, "select_for_flush: prepared; executing");
            let arrow = stmt.query_arrow([])?;
            debug!(table = %table, staging = %staging, "select_for_flush: query_arrow returned");
            let schema = arrow.get_schema();
            let mut payload_batches = Vec::new();
            let mut total_rows = 0usize;
            let mut total_bytes = 0u64;
            let mut max_seq: Option<i64> = None;

            debug!(
                table = %table,
                staging = %staging,
                "select_for_flush: iterating arrow result"
            );
            for batch in arrow {
                total_rows += batch.num_rows();
                total_bytes += batch.get_array_memory_size() as u64;
                if batch.num_rows() == 0 {
                    continue;
                }
                debug!(
                    table = %table,
                    staging = %staging,
                    rows = batch.num_rows(),
                    cols = batch.num_columns(),
                    "select_for_flush: processing batch"
                );

                let seq_column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        ServerError::Internal(format!(
                            "duckling queue rowid column missing or wrong type for table {table}"
                        ))
                    })?;
                let last_seq = seq_column.value(seq_column.len() - 1);
                max_seq = Some(max_seq.map_or(last_seq, |prev| prev.max(last_seq)));

                // Drop rowid column before handing batches to flush workers.
                let mut cols = Vec::with_capacity(batch.num_columns() - 1);
                let mut fields = Vec::with_capacity(batch.num_columns() - 1);
                for (idx, array) in batch.columns().iter().enumerate() {
                    if idx == 0 {
                        continue;
                    }
                    cols.push(array.clone());
                    fields.push(schema.field(idx).clone());
                }
                let payload_schema = SchemaRef::new(Schema::new(fields));
                payload_batches.push(RecordBatch::try_new(payload_schema, cols)?);
            }

            if total_rows == 0 {
                debug!(
                    table = %table,
                    staging = %staging,
                    "select_for_flush: no rows selected"
                );
                return Ok(None);
            }
            debug!(
                table = %table,
                staging = %staging,
                total_rows,
                total_bytes,
                batches = payload_batches.len(),
                "select_for_flush: built payload"
            );

            let stored_schema = fetch_schema_with_conn(conn, table)?;
            let handle = FlushHandle {
                table: table.to_string(),
                max_seq: max_seq.expect("rowid present when rows > 0"),
            };

            Ok(Some(SelectedPayload {
                table: table.to_string(),
                schema: stored_schema,
                batches: payload_batches,
                handle,
                rows: total_rows,
                bytes: total_bytes,
            }))
        })
    }

    /// Delete rows up to the flushed max_seq for the table.
    pub fn ack(&self, handle: &FlushHandle) -> Result<usize, ServerError> {
        let staging = staging_table_name(&handle.table);
        self.with_conn(|conn| {
            let deleted = conn.execute(
                &format!("DELETE FROM {} WHERE rowid <= ?", quote_ident(&staging)),
                params_from_iter([handle.max_seq]),
            )?;
            debug!(
                table = %handle.table,
                staging = %staging,
                max_seq = handle.max_seq,
                deleted,
                "ack: deleted flushed rows from staging"
            );
            Ok(deleted)
        })
    }

    /// Current size of buffer.duckdb on disk.
    pub fn database_size_bytes(&self) -> Result<u64, ServerError> {
        let metadata = fs::metadata(&self.path).map_err(|err| {
            ServerError::Internal(format!(
                "failed to read metadata for {}: {err}",
                self.path.display()
            ))
        })?;
        Ok(metadata.len())
    }
}

fn staging_table_name(table: &str) -> String {
    format!("dq_{}", table)
}

fn fetch_schema_with_conn(conn: &Connection, table: &str) -> Result<SchemaRef, ServerError> {
    let schema_json: String = conn
        .query_row(
            "SELECT schema_json FROM dq_meta WHERE table_name = ?",
            params_from_iter([table]),
            |row| row.get(0),
        )
        .optional()
        .map_err(ServerError::DuckDb)?
        .ok_or_else(|| {
            ServerError::Internal(format!("duckling queue metadata missing for table {table}"))
        })?;
    let schema = decode_schema(&schema_json)?;
    Ok(Arc::new(schema))
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn build_create_table_sql(table: &str, schema: &Schema) -> Result<String, ServerError> {
    let staging = staging_table_name(table);
    let mut cols = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let col_type = arrow_type_to_duckdb(field.data_type())?;
        let nullable = if field.is_nullable() { "" } else { " NOT NULL" };
        cols.push(format!(
            "{} {}{}",
            quote_ident(field.name()),
            col_type,
            nullable
        ));
    }
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {} ({});",
        quote_ident(&staging),
        cols.join(", ")
    ))
}

fn encode_schema(schema: &Schema) -> Result<String, ServerError> {
    serde_json::to_string(schema).map_err(|err| {
        ServerError::Internal(format!(
            "failed to serialize schema for duckling queue: {err}"
        ))
    })
}

fn decode_schema(encoded: &str) -> Result<Schema, ServerError> {
    serde_json::from_str(encoded).map_err(|err| {
        ServerError::Internal(format!("failed to decode schema for duckling queue: {err}"))
    })
}

fn arrow_type_to_duckdb(dt: &DataType) -> Result<String, ServerError> {
    let t = match dt {
        DataType::Int8 => "TINYINT",
        DataType::Int16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::Int64 => "BIGINT",
        DataType::UInt8 => "UTINYINT",
        DataType::UInt16 => "USMALLINT",
        DataType::UInt32 => "UINTEGER",
        DataType::UInt64 => "UBIGINT",
        DataType::Float16 | DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR",
        DataType::Binary | DataType::FixedSizeBinary(_) | DataType::LargeBinary => "BLOB",
        DataType::Boolean => "BOOLEAN",
        DataType::Date32 => "DATE",
        DataType::Date64 => "TIMESTAMP",
        DataType::Timestamp(_, _) => "TIMESTAMP",
        DataType::Time32(_) | DataType::Time64(_) => "TIME",
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
            let scale = (*s).max(0);
            return Ok(format!("DECIMAL({}, {})", p, scale));
        }
        DataType::List(field) => {
            let inner = arrow_type_to_duckdb(field.data_type())?;
            return Ok(format!("{}[]", inner));
        }
        _ => {
            return Err(ServerError::Internal(format!(
                "unsupported arrow type in duckling queue buffer: {dt:?}"
            )))
        }
    };
    Ok(t.to_string())
}

fn staging_exists(conn: &Connection, table: &str) -> Result<bool, ServerError> {
    // DuckDB stores staging tables in the main schema; use catalog tables instead of string
    // matching on error messages.
    let staging = staging_table_name(table);
    let exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'main' AND table_name = ?",
            params_from_iter([staging]),
            |row| row.get(0),
        )
        .map_err(ServerError::DuckDb)?;
    Ok(exists > 0)
}

trait OptionalRow<T> {
    fn optional(self) -> Result<Option<T>, duckdb::Error>;
}

impl<T> OptionalRow<T> for Result<T, duckdb::Error> {
    fn optional(self) -> Result<Option<T>, duckdb::Error> {
        match self {
            Ok(v) => Ok(Some(v)),
            Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

// Allow referencing Arc without importing everywhere above.
use std::sync::Arc;
