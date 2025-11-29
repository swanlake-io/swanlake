use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;

use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{Schema, SchemaRef};
use duckdb::{params_from_iter, Connection};
use tracing::{debug, error};

use crate::engine::EngineFactory;
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
    target_catalog: String,
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

    pub fn new(
        root: PathBuf,
        flush_chunk_rows: usize,
        target_catalog: String,
        factory: &EngineFactory,
    ) -> Result<Self, ServerError> {
        fs::create_dir_all(&root).map_err(|err| {
            ServerError::Internal(format!(
                "failed to create duckling queue root {}: {err}",
                root.display()
            ))
        })?;
        let path = root.join("buffer.duckdb");

        // Use factory to create connection with all extensions and init SQL
        let conn = factory.create_connection_with_path(&path)?;

        let buffer = Self {
            conn: Mutex::new(conn),
            path,
            flush_chunk_rows: flush_chunk_rows.max(1),
            target_catalog,
        };
        Ok(buffer)
    }

    /// Load known tables and their schemas/row counts from the buffer.
    pub fn load_table_states(&self) -> Result<Vec<TableState>, ServerError> {
        self.with_conn(|conn| {
            let mut states = Vec::new();
            let mut stmt = conn.prepare(
                "SELECT table_name FROM information_schema.tables
                 WHERE table_catalog = 'buffer' AND table_schema = 'main' AND table_name LIKE 'dq_%'
                 ORDER BY table_name",
            )?;
            let rows = stmt.query_map([], |row| {
                let staging_name: String = row.get(0)?;
                Ok(staging_name)
            })?;
            let staging_names = rows
                .collect::<Result<Vec<_>, _>>()
                .map_err(ServerError::DuckDb)?;

            for staging_name in staging_names {
                // Extract table name from staging name (remove "dq_" prefix)
                let table = staging_name
                    .strip_prefix("dq_")
                    .unwrap_or(&staging_name)
                    .to_string();

                // Get schema directly from the staging table - skip if it fails
                // (table might have been dropped externally or corrupted)
                let schema = match fetch_schema_from_table(conn, &staging_name) {
                    Ok(s) => s,
                    Err(e) => {
                        error!(
                            table = %table,
                            staging = %staging_name,
                            error = %e,
                            "failed to fetch schema for staging table; skipping"
                        );
                        continue;
                    }
                };

                let count: i64 = match conn.query_row(
                    &format!("SELECT COUNT(*) FROM main.{}", quote_ident(&staging_name)),
                    [],
                    |row| row.get(0),
                ) {
                    Ok(c) => c,
                    Err(e) => {
                        error!(
                            table = %table,
                            staging = %staging_name,
                            error = %e,
                            "failed to count rows in staging table; skipping"
                        );
                        continue;
                    }
                };

                states.push(TableState {
                    table,
                    schema,
                    row_count: count as usize,
                });
            }

            Ok(states)
        })
    }

    /// Retrieve current schema and row count for a table if it exists.
    pub fn table_info(&self, table: &str) -> Result<Option<(SchemaRef, usize)>, ServerError> {
        self.with_conn(|conn| {
            let staging = staging_table_name(table);
            if !staging_exists(conn, table)? {
                debug!(table = %table, "table_info: staging table does not exist");
                return Ok(None);
            }

            // Get schema directly from the staging table
            let schema = fetch_schema_from_table(conn, &staging)?;

            let row_count: i64 = conn
                .query_row(
                    &format!("SELECT COUNT(*) FROM main.{}", quote_ident(&staging)),
                    [],
                    |row| row.get(0),
                )
                .map_err(ServerError::DuckDb)?;
            Ok(Some((schema, row_count as usize)))
        })
    }

    /// Ensure a staging table exists and matches the incoming schema. If the table exists with
    /// a different schema and still holds buffered rows, an error is returned to avoid dropping
    /// data implicitly. Callers should flush pending rows first.
    pub fn ensure_table(&self, table: &str, schema: &Schema) -> Result<(), ServerError> {
        self.with_conn(|conn| self.ensure_table_with_conn(conn, table, schema))
    }

    fn create_staging_table(
        &self,
        conn: &mut Connection,
        table: &str,
        schema: &Schema,
    ) -> Result<(), ServerError> {
        let create_sql = build_create_table_sql(table, schema, &self.target_catalog)?;
        conn.execute_batch(&create_sql)?;
        Ok(())
    }

    fn recreate_staging_table(
        &self,
        conn: &mut Connection,
        table: &str,
        schema: &Schema,
    ) -> Result<(), ServerError> {
        let staging = staging_table_name(table);
        let create_sql = build_create_table_sql(table, schema, &self.target_catalog)?;
        conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS main.{};",
            quote_ident(&staging)
        ))?;
        conn.execute_batch(&create_sql)?;
        Ok(())
    }

    fn ensure_table_with_conn(
        &self,
        conn: &mut Connection,
        table: &str,
        schema: &Schema,
    ) -> Result<(), ServerError> {
        let staging = staging_table_name(table);

        if !staging_exists(conn, table)? {
            // Table doesn't exist, create it
            self.create_staging_table(conn, table, schema)?;
            return Ok(());
        }

        // Table exists, check if schema matches
        let existing_schema = fetch_schema_from_table(conn, &staging)?;
        let schema_json = encode_schema(schema)?;
        let existing_schema_json = encode_schema(&existing_schema)?;

        if existing_schema_json != schema_json {
            let row_count: i64 = conn
                .query_row(
                    &format!("SELECT COUNT(*) FROM main.{}", quote_ident(&staging)),
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
            self.recreate_staging_table(conn, table, schema)?;
        }

        Ok(())
    }

    /// Replace the staging table with the given schema, dropping any existing table.
    pub fn replace_table_schema(&self, table: &str, schema: &Schema) -> Result<(), ServerError> {
        self.with_conn(|conn| self.recreate_staging_table(conn, table, schema))
    }

    /// Append RecordBatches into the staging table. Returns (rows, bytes).
    pub fn enqueue(
        &self,
        table: &str,
        schema: &Schema,
        batches: &[RecordBatch],
    ) -> Result<(usize, u64), ServerError> {
        self.with_conn(|conn| {
            let staging = staging_table_name(table);

            // Try to open appender first (fast path)
            // Check if table exists first to avoid borrow checker issues
            let needs_creation = conn.appender(&staging).is_err();
            if needs_creation {
                debug!(table = %table, staging = %staging, "appender failed; ensuring table exists");
                self.ensure_table_with_conn(conn, table, schema)?;
            }

            let mut appender = conn.appender(&staging).map_err(|err| {
                error!(
                    table = %table,
                    staging = %staging,
                    %err,
                    "failed to open appender for duckling_queue staging table"
                );
                ServerError::DuckDb(err)
            })?;

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
                "SELECT rowid, * FROM main.{} ORDER BY rowid LIMIT {}",
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

            let stored_schema = fetch_schema_from_table(conn, &staging)?;
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
                &format!(
                    "DELETE FROM main.{} WHERE rowid <= ?",
                    quote_ident(&staging)
                ),
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

fn fetch_schema_from_table(
    conn: &Connection,
    staging_name: &str,
) -> Result<SchemaRef, ServerError> {
    // Use DuckDB's query_arrow to get the schema directly from the table
    // Explicitly use main schema to avoid catalog confusion when catalogs are attached
    let mut stmt = conn.prepare(&format!(
        "SELECT * FROM main.{} LIMIT 0",
        quote_ident(staging_name)
    ))?;
    let arrow = stmt.query_arrow([])?;
    Ok(arrow.get_schema())
}

fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn quote_qualified_name(catalog: &str, table: &str) -> String {
    format!("{}.{}", quote_ident(catalog), quote_ident(table))
}

fn build_create_table_sql(
    table: &str,
    _schema: &Schema,
    target_catalog: &str,
) -> Result<String, ServerError> {
    let staging = staging_table_name(table);
    // Use CREATE TABLE AS SELECT to copy schema from target table
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {} AS SELECT * FROM {} WHERE 1=0;",
        quote_ident(&staging),
        quote_qualified_name(target_catalog, table)
    ))
}

fn encode_schema(schema: &Schema) -> Result<String, ServerError> {
    serde_json::to_string(schema).map_err(|err| {
        ServerError::Internal(format!(
            "failed to serialize schema for duckling queue: {err}"
        ))
    })
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
