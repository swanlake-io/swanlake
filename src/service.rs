use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_flight::sql::metadata::SqlInfoDataBuilder;
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::DoPutPreparedStatementResult;
use arrow_flight::sql::{
    ActionBeginTransactionRequest, ActionBeginTransactionResult,
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, ActionEndTransactionRequest, CommandGetSqlInfo,
    CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementQuery,
    CommandStatementUpdate, ProstMessageExt, SqlInfo, SqlSupportedTransaction,
    SqlTransactionIsolationLevel, TicketStatementQuery,
};
use arrow_flight::{decode::FlightRecordBatchStream, error::FlightError};
use arrow_flight::{
    flight_service_server::FlightService, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    Ticket,
};
use arrow_schema::{DataType, Schema};
use duckdb::{types::Value, Connection, DuckdbConnectionManager};
use futures::{stream, Stream, StreamExt, TryStreamExt};
use prost::Message;
use r2d2::PooledConnection;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, instrument};

use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};

use crate::duckdb::{DuckDbEngine, QueryResult};
use crate::error::ServerError;
use tokio::sync::Mutex;

#[derive(Clone)]
struct PreparedStatementMeta {
    sql: String,
    is_query: bool,
    transaction_id: Option<String>,
}

struct PreparedStatementState {
    meta: PreparedStatementMeta,
    pending_parameters: Option<Vec<Vec<Value>>>,
}

impl PreparedStatementState {
    fn new(meta: PreparedStatementMeta) -> Self {
        Self {
            meta,
            pending_parameters: None,
        }
    }
}

#[derive(Default)]
struct PreparedStatementStore {
    inner: Mutex<HashMap<String, PreparedStatementState>>,
}

impl PreparedStatementStore {
    async fn insert(&self, handle: String, meta: PreparedStatementMeta) {
        let mut guard = self.inner.lock().await;
        guard.insert(handle, PreparedStatementState::new(meta));
    }

    async fn meta(&self, handle: &str) -> Result<PreparedStatementMeta, ServerError> {
        let guard = self.inner.lock().await;
        guard
            .get(handle)
            .map(|state| state.meta.clone())
            .ok_or_else(|| ServerError::PreparedStatementNotFound(handle.to_string()))
    }

    async fn set_parameters(
        &self,
        handle: &str,
        params: Option<Vec<Vec<Value>>>,
    ) -> Result<(), ServerError> {
        let mut guard = self.inner.lock().await;
        let state = guard
            .get_mut(handle)
            .ok_or_else(|| ServerError::PreparedStatementNotFound(handle.to_string()))?;
        state.pending_parameters = params;
        Ok(())
    }

    async fn take_parameters(&self, handle: &str) -> Result<Option<Vec<Vec<Value>>>, ServerError> {
        let mut guard = self.inner.lock().await;
        let state = guard
            .get_mut(handle)
            .ok_or_else(|| ServerError::PreparedStatementNotFound(handle.to_string()))?;
        Ok(state.pending_parameters.take())
    }

    async fn remove(&self, handle: &str) -> Result<(), ServerError> {
        let mut guard = self.inner.lock().await;
        guard
            .remove(handle)
            .ok_or_else(|| ServerError::PreparedStatementNotFound(handle.to_string()))?;
        Ok(())
    }
}

#[derive(Default)]
struct TransactionManager {
    inner: Mutex<HashMap<String, Arc<StdMutex<PooledConnection<DuckdbConnectionManager>>>>>,
}

impl TransactionManager {
    async fn insert(&self, id: String, conn: PooledConnection<DuckdbConnectionManager>) {
        let mut guard = self.inner.lock().await;
        guard.insert(id, Arc::new(StdMutex::new(conn)));
    }

    async fn get(
        &self,
        id: &str,
    ) -> Result<Arc<StdMutex<PooledConnection<DuckdbConnectionManager>>>, ServerError> {
        let guard = self.inner.lock().await;
        guard
            .get(id)
            .cloned()
            .ok_or_else(|| ServerError::TransactionNotFound(id.to_string()))
    }

    async fn remove(
        &self,
        id: &str,
    ) -> Option<Arc<StdMutex<PooledConnection<DuckdbConnectionManager>>>> {
        let mut guard = self.inner.lock().await;
        guard.remove(id)
    }
}

#[derive(Clone)]
pub struct SwanFlightSqlService {
    engine: Arc<DuckDbEngine>,
    transactions: Arc<TransactionManager>,
    prepared: Arc<PreparedStatementStore>,
    next_statement_id: Arc<AtomicU64>,
}

impl SwanFlightSqlService {
    pub fn new(engine: Arc<DuckDbEngine>) -> Self {
        Self {
            engine,
            transactions: Arc::new(TransactionManager::default()),
            prepared: Arc::new(PreparedStatementStore::default()),
            next_statement_id: Arc::new(AtomicU64::new(1)),
        }
    }

    fn status_from_error(err: ServerError) -> Status {
        match err {
            ServerError::DuckDb(e) => {
                error!(error = %e, "duckdb engine error");
                Status::internal(format!("duckdb error: {e}"))
            }
            ServerError::Arrow(e) => {
                error!(error = %e, "arrow conversion error");
                Status::internal(format!("arrow error: {e}"))
            }
            ServerError::Pool(e) => {
                error!(error = %e, "connection pool error");
                Status::internal(format!("connection pool error: {e}"))
            }
            ServerError::WritesDisabled => {
                Status::permission_denied("write operations are disabled by configuration")
            }
            ServerError::TransactionNotFound(id) => {
                Status::invalid_argument(format!("unknown transaction '{id}'"))
            }
            ServerError::PreparedStatementNotFound(handle) => {
                Status::invalid_argument(format!("unknown prepared statement '{handle}'"))
            }
            ServerError::UnsupportedParameter(param) => {
                Status::invalid_argument(format!("unsupported parameter type: {param}"))
            }
        }
    }

    fn status_from_join(err: tokio::task::JoinError) -> Status {
        if err.is_panic() {
            error!(%err, "blocking task panicked");
            Status::internal("blocking task panicked")
        } else {
            error!(%err, "blocking task cancelled");
            Status::internal(format!("blocking task cancelled: {err}"))
        }
    }

    fn status_from_flight_error(err: FlightError) -> Status {
        match err {
            FlightError::Tonic(status) => *status,
            other => Status::internal(format!("flight decode error: {other}")),
        }
    }

    fn into_stream(
        batches: Vec<FlightData>,
    ) -> Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>> {
        Box::pin(stream::iter(batches.into_iter().map(Ok)))
    }

    /// Detect if SQL is a query (returns results) or statement (doesn't return results)
    /// using keyword-based analysis
    fn is_query_statement(sql: &str) -> bool {
        let trimmed = sql.trim_start();

        // Remove leading SQL comments
        let mut cleaned = trimmed;
        loop {
            if let Some(rest) = cleaned.strip_prefix("--") {
                // Single-line comment
                if let Some(newline_pos) = rest.find('\n') {
                    cleaned = rest[newline_pos + 1..].trim_start();
                } else {
                    // Comment to end of string
                    return false;
                }
            } else if let Some(rest) = cleaned.strip_prefix("/*") {
                // Multi-line comment
                if let Some(end_pos) = rest.find("*/") {
                    cleaned = rest[end_pos + 2..].trim_start();
                } else {
                    // Unclosed comment
                    return false;
                }
            } else {
                break;
            }
        }

        // Get first keyword (case-insensitive)
        let first_word = cleaned
            .split(|c: char| c.is_whitespace() || c == '(' || c == ';')
            .find(|w| !w.is_empty())
            .unwrap_or("")
            .to_uppercase();

        // Statements that return results (queries)
        matches!(
            first_word.as_str(),
            "SELECT"
                | "WITH"
                | "SHOW"
                | "DESCRIBE"
                | "DESC"
                | "EXPLAIN"
                | "VALUES"
                | "TABLE"
                | "PRAGMA"
        )
    }

    fn parse_transaction_id(id: &[u8]) -> Result<String, Status> {
        String::from_utf8(id.to_vec())
            .map_err(|_| Status::invalid_argument("transaction id must be utf-8"))
    }

    fn parse_statement_handle(handle: &[u8]) -> Result<String, Status> {
        String::from_utf8(handle.to_vec())
            .map_err(|_| Status::invalid_argument("prepared statement handle must be utf-8"))
    }

    fn allocate_statement_handle(&self) -> Vec<u8> {
        let id = self.next_statement_id.fetch_add(1, Ordering::Relaxed);
        format!("stmt-{id}").into_bytes()
    }

    async fn collect_parameter_sets(
        request: Request<PeekableFlightDataStream>,
    ) -> Result<Vec<Vec<Value>>, Status> {
        let stream = request.into_inner();
        let mapped = stream.map_err(|status| FlightError::Tonic(Box::new(status)));
        let mut record_stream = FlightRecordBatchStream::new_from_flight_data(mapped);

        let mut params = Vec::new();
        while let Some(batch) = record_stream.next().await {
            let batch = batch.map_err(Self::status_from_flight_error)?;
            let mut rows = Self::record_batch_to_params(&batch).map_err(Self::status_from_error)?;
            params.append(&mut rows);
        }

        if params.is_empty() {
            params.push(Vec::new());
        }

        Ok(params)
    }

    fn record_batch_to_params(batch: &RecordBatch) -> Result<Vec<Vec<Value>>, ServerError> {
        let row_count = batch.num_rows();
        let column_count = batch.num_columns();
        let mut rows = vec![Vec::with_capacity(column_count); row_count];

        for col_idx in 0..column_count {
            let column = batch.column(col_idx);
            for row_idx in 0..row_count {
                let value = Self::value_from_array(column, row_idx)?;
                rows[row_idx].push(value);
            }
        }

        Ok(rows)
    }

    fn value_from_array(array: &ArrayRef, row: usize) -> Result<Value, ServerError> {
        if array.is_null(row) {
            return Ok(Value::Null);
        }

        match array.data_type() {
            DataType::Null => Ok(Value::Null),
            DataType::Boolean => {
                let values = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("boolean array downcast");
                Ok(Value::Boolean(values.value(row)))
            }
            DataType::Int8 => {
                let values = array
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .expect("int8 array downcast");
                Ok(Value::TinyInt(values.value(row)))
            }
            DataType::Int16 => {
                let values = array
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .expect("int16 array downcast");
                Ok(Value::SmallInt(values.value(row)))
            }
            DataType::Int32 => {
                let values = array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("int32 array downcast");
                Ok(Value::Int(values.value(row)))
            }
            DataType::Int64 => {
                let values = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int64 array downcast");
                Ok(Value::BigInt(values.value(row)))
            }
            DataType::UInt8 => {
                let values = array
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .expect("uint8 array downcast");
                Ok(Value::UTinyInt(values.value(row)))
            }
            DataType::UInt16 => {
                let values = array
                    .as_any()
                    .downcast_ref::<UInt16Array>()
                    .expect("uint16 array downcast");
                Ok(Value::USmallInt(values.value(row)))
            }
            DataType::UInt32 => {
                let values = array
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .expect("uint32 array downcast");
                Ok(Value::UInt(values.value(row)))
            }
            DataType::UInt64 => {
                let values = array
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("uint64 array downcast");
                Ok(Value::UBigInt(values.value(row)))
            }
            DataType::Float32 => {
                let values = array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .expect("float32 array downcast");
                Ok(Value::Float(values.value(row)))
            }
            DataType::Float64 => {
                let values = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("float64 array downcast");
                Ok(Value::Double(values.value(row)))
            }
            DataType::Utf8 => {
                let values = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("string array downcast");
                Ok(Value::Text(values.value(row).to_string()))
            }
            DataType::LargeUtf8 => {
                let values = array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .expect("large string array downcast");
                Ok(Value::Text(values.value(row).to_string()))
            }
            DataType::Binary => {
                let values = array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .expect("binary array downcast");
                Ok(Value::Blob(values.value(row).to_vec()))
            }
            DataType::LargeBinary => {
                let values = array
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .expect("large binary array downcast");
                Ok(Value::Blob(values.value(row).to_vec()))
            }
            other => Err(ServerError::UnsupportedParameter(other.to_string())),
        }
    }

    fn schema_to_ipc_bytes(schema: &Schema) -> Result<Vec<u8>, ServerError> {
        let data_gen = IpcDataGenerator::default();
        let mut dict_tracker = DictionaryTracker::new(false);
        let write_options = IpcWriteOptions::default();
        let encoded = data_gen.schema_to_bytes_with_dictionary_tracker(
            schema,
            &mut dict_tracker,
            &write_options,
        );
        let mut buffer = vec![];
        arrow_ipc::writer::write_message(&mut buffer, encoded, &write_options)
            .map_err(ServerError::Arrow)?;
        Ok(buffer)
    }

    fn execute_statement_batches(
        conn: &Connection,
        sql: &str,
        param_sets: &[Vec<Value>],
    ) -> Result<i64, ServerError> {
        if param_sets.is_empty() {
            let affected = DuckDbEngine::execute_statement_on_conn_with_params(conn, sql, &[])?;
            return Ok(affected as i64);
        }

        let mut total = 0i64;
        for params in param_sets {
            let affected = DuckDbEngine::execute_statement_on_conn_with_params(conn, sql, params)?;
            total += affected as i64;
        }
        Ok(total)
    }

    async fn execute_prepared_query_handle(
        &self,
        handle: &str,
        meta: PreparedStatementMeta,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let params = self
            .prepared
            .take_parameters(handle)
            .await
            .map_err(Self::status_from_error)?;
        let mut param_sets = params.unwrap_or_else(|| vec![Vec::new()]);
        if param_sets.is_empty() {
            param_sets.push(Vec::new());
        }
        if param_sets.len() > 1 {
            return Err(Status::invalid_argument(
                "multiple parameter batches are not supported for queries",
            ));
        }
        let parameters = param_sets.into_iter().next().unwrap();

        info!(
            handle = handle,
            sql = %meta.sql,
            transaction = meta.transaction_id.as_deref().unwrap_or("auto"),
            "executing prepared statement via handle"
        );

        let QueryResult {
            schema,
            batches,
            total_rows,
            total_bytes,
        } = if let Some(txn_id) = meta.transaction_id.clone() {
            let conn_arc = self
                .transactions
                .get(&txn_id)
                .await
                .map_err(Self::status_from_error)?;
            let params_for_exec = parameters.clone();
            let sql_for_exec = meta.sql.clone();
            let engine = self.engine.clone();
            tokio::task::spawn_blocking(move || {
                let guard = conn_arc.lock().expect("transaction connection poisoned");
                engine.ensure_connection_state(&*guard)?;
                if params_for_exec.is_empty() {
                    DuckDbEngine::execute_query_on_conn(&*guard, &sql_for_exec)
                } else {
                    DuckDbEngine::execute_query_on_conn_with_params(
                        &*guard,
                        &sql_for_exec,
                        &params_for_exec,
                    )
                }
            })
            .await
            .map_err(Self::status_from_join)?
            .map_err(Self::status_from_error)?
        } else {
            let engine = self.engine.clone();
            let params_for_exec = parameters.clone();
            let sql_for_exec = meta.sql.clone();
            tokio::task::spawn_blocking(move || {
                let conn = engine.get_read_connection()?;
                if params_for_exec.is_empty() {
                    DuckDbEngine::execute_query_on_conn(&conn, &sql_for_exec)
                } else {
                    DuckDbEngine::execute_query_on_conn_with_params(
                        &conn,
                        &sql_for_exec,
                        &params_for_exec,
                    )
                }
            })
            .await
            .map_err(Self::status_from_join)?
            .map_err(Self::status_from_error)?
        };

        let flight_data =
            arrow_flight::utils::batches_to_flight_data(&schema, batches).map_err(|err| {
                error!(%err, "failed to convert record batches to flight data");
                Status::internal(format!(
                    "failed to convert record batches to flight data: {err}"
                ))
            })?;

        debug!(
            handle,
            batch_count = flight_data.len(),
            "converted batches to flight data"
        );

        let stream = Self::into_stream(flight_data);
        let mut response = Response::new(stream);
        if let Ok(value) = MetadataValue::try_from(total_rows.to_string()) {
            response.metadata_mut().insert("x-swandb-total-rows", value);
        }
        if let Ok(value) = MetadataValue::try_from(total_bytes.to_string()) {
            response
                .metadata_mut()
                .insert("x-swandb-total-bytes", value);
        }
        info!(
            handle = handle,
            total_rows, total_bytes, "prepared statement completed"
        );
        Ok(response)
    }
}

#[tonic::async_trait]
impl FlightSqlService for SwanFlightSqlService {
    type FlightService = SwanFlightSqlService;

    #[instrument(skip(self, request), fields(sql = %query.query))]
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = query.query.clone();
        let transaction_id = match query.transaction_id.as_ref() {
            Some(id) => Some(Self::parse_transaction_id(id.as_ref())?),
            None => None,
        };

        info!(%sql, "planning query via get_flight_info_statement");

        let schema = if let Some(txn_id) = transaction_id.clone() {
            let conn = self
                .transactions
                .get(&txn_id)
                .await
                .map_err(Self::status_from_error)?;
            let sql_for_schema = sql.clone();
            tokio::task::spawn_blocking(move || {
                let guard = conn.lock().expect("transaction connection poisoned");
                DuckDbEngine::schema_for_query_on_conn(&*guard, &sql_for_schema)
            })
            .await
            .map_err(Self::status_from_join)?
            .map_err(Self::status_from_error)?
        } else {
            let engine = self.engine.clone();
            let sql_for_schema = sql.clone();
            tokio::task::spawn_blocking(move || engine.schema_for_query(&sql_for_schema))
                .await
                .map_err(Self::status_from_join)?
                .map_err(Self::status_from_error)?
        };

        debug!(field_count = schema.fields().len(), "planned schema");

        let handle_bytes = query.encode_to_vec();

        let descriptor = request.into_inner();
        let ticket = TicketStatementQuery {
            statement_handle: handle_bytes.into(),
        };
        let ticket_bytes = ticket.as_any().encode_to_vec();
        let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(ticket_bytes));

        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|err| Status::internal(format!("failed to encode schema: {err}")))?
            .with_descriptor(descriptor)
            .with_endpoint(endpoint);

        Ok(Response::new(info))
    }

    #[instrument(skip(self, _request), fields(handle_len = ticket.statement_handle.len()))]
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let command = CommandStatementQuery::decode(ticket.statement_handle.as_ref())
            .map_err(|err| Status::invalid_argument(format!("invalid statement handle: {err}")))?;
        let sql = command.query.clone();
        if let Ok(meta) = self.prepared.meta(&sql).await {
            return self.execute_prepared_query_handle(&sql, meta).await;
        }
        let sql_for_exec = sql.clone();
        let transaction_id = match command.transaction_id.as_ref() {
            Some(id) => Some(Self::parse_transaction_id(id.as_ref())?),
            None => None,
        };

        info!(%sql, "executing query via do_get_statement");

        let QueryResult {
            schema,
            batches,
            total_rows,
            total_bytes,
        } = if let Some(txn_id) = transaction_id {
            let conn = self
                .transactions
                .get(&txn_id)
                .await
                .map_err(Self::status_from_error)?;
            let engine = self.engine.clone();
            tokio::task::spawn_blocking(move || {
                let guard = conn.lock().expect("transaction connection poisoned");
                engine.ensure_connection_state(&*guard)?;
                DuckDbEngine::execute_query_on_conn(&*guard, &sql_for_exec)
            })
            .await
            .map_err(Self::status_from_join)?
            .map_err(Self::status_from_error)?
        } else {
            let engine = self.engine.clone();
            tokio::task::spawn_blocking(move || engine.execute_query(&sql_for_exec))
                .await
                .map_err(Self::status_from_join)?
                .map_err(Self::status_from_error)?
        };

        let flight_data =
            arrow_flight::utils::batches_to_flight_data(&schema, batches).map_err(|err| {
                error!(%err, "failed to convert record batches to flight data");
                Status::internal(format!(
                    "failed to convert record batches to flight data: {err}"
                ))
            })?;

        debug!(
            batch_count = flight_data.len(),
            "converted batches to flight data"
        );

        let stream = Self::into_stream(flight_data);
        let mut response = Response::new(stream);
        if let Ok(value) = MetadataValue::try_from(total_rows.to_string()) {
            response.metadata_mut().insert("x-swandb-total-rows", value);
        }
        if let Ok(value) = MetadataValue::try_from(total_bytes.to_string()) {
            response
                .metadata_mut()
                .insert("x-swandb-total-bytes", value);
        }
        info!(%sql, total_rows, total_bytes, "query completed");
        Ok(response)
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {
        // No-op: we don't need to register info dynamically
    }

    #[instrument(skip(self, request))]
    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        debug!("get_flight_info_sql_info called");

        // Build SQL info metadata to get schema
        let mut builder = SqlInfoDataBuilder::new();

        // Report that Flight SQL transaction API is supported
        builder.append(
            SqlInfo::FlightSqlServerTransaction,
            SqlSupportedTransaction::Transaction as i32,
        );

        // Report that SQL-level transactions are supported
        builder.append(SqlInfo::SqlTransactionsSupported, true);

        // Report transaction isolation level support
        // DuckDB defaults to serializable isolation
        builder.append(
            SqlInfo::SqlDefaultTransactionIsolation,
            SqlTransactionIsolationLevel::SqlTransactionSerializable as i32,
        );

        // Report supported isolation levels (all standard levels)
        // Bitmask: bit 0 = none, bit 1 = read uncommitted, bit 2 = read committed,
        //          bit 3 = repeatable read, bit 4 = serializable
        builder.append(SqlInfo::SqlSupportedTransactionsIsolationLevels, 0b11110);

        let info_data = builder
            .build()
            .map_err(|e| Status::internal(format!("Failed to build SqlInfo data: {}", e)))?;

        let schema = info_data.schema();

        // Create ticket for do_get_sql_info
        let ticket_bytes = query.as_any().encode_to_vec();
        let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(ticket_bytes));

        let descriptor = request.into_inner();
        let info = FlightInfo::new()
            .try_with_schema(schema.as_ref())
            .map_err(|err| Status::internal(format!("failed to encode schema: {err}")))?
            .with_descriptor(descriptor)
            .with_endpoint(endpoint)
            .with_total_records(-1); // Unknown number of records

        debug!("Returning FlightInfo for SqlInfo");
        Ok(Response::new(info))
    }

    #[instrument(skip(self, _request))]
    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        debug!("do_get_sql_info called");

        // Build SQL info metadata
        let mut builder = SqlInfoDataBuilder::new();

        // Report that Flight SQL transaction API is supported
        builder.append(
            SqlInfo::FlightSqlServerTransaction,
            SqlSupportedTransaction::Transaction as i32,
        );

        // Report that SQL-level transactions are supported
        builder.append(SqlInfo::SqlTransactionsSupported, true);

        // Report transaction isolation level support
        // DuckDB defaults to serializable isolation
        builder.append(
            SqlInfo::SqlDefaultTransactionIsolation,
            SqlTransactionIsolationLevel::SqlTransactionSerializable as i32,
        );

        // Report supported isolation levels (all standard levels)
        builder.append(SqlInfo::SqlSupportedTransactionsIsolationLevels, 0b11110);

        let info_data = builder
            .build()
            .map_err(|e| Status::internal(format!("Failed to build SqlInfo data: {}", e)))?;

        // Build the response batch
        let batch = query
            .into_builder(&info_data)
            .build()
            .map_err(|e| Status::internal(format!("Failed to build SqlInfo response: {}", e)))?;

        let schema = batch.schema();
        debug!("Returning SqlInfo with {} rows", batch.num_rows());

        // Convert to Flight data stream
        let flight_data = arrow_flight::utils::batches_to_flight_data(&schema, vec![batch])
            .map_err(|err| {
                error!(%err, "failed to convert SqlInfo batch to flight data");
                Status::internal(format!(
                    "failed to convert SqlInfo batch to flight data: {err}"
                ))
            })?;

        let stream = Self::into_stream(flight_data);
        Ok(Response::new(stream))
    }

    #[instrument(skip(self, _request), fields(sql = %command.query))]
    async fn do_put_statement_update(
        &self,
        command: CommandStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let sql = command.query.clone();
        let transaction_id = match command.transaction_id.as_ref() {
            Some(id) => Some(Self::parse_transaction_id(id.as_ref())?),
            None => None,
        };

        info!(%sql, "executing statement via do_put_statement_update");

        let sql_for_exec = sql.clone();
        self.engine.register_session_init(&sql);
        let affected_rows = if let Some(txn_id) = transaction_id {
            let conn = self
                .transactions
                .get(&txn_id)
                .await
                .map_err(Self::status_from_error)?;
            let engine = self.engine.clone();
            tokio::task::spawn_blocking(move || {
                let guard = conn.lock().expect("transaction connection poisoned");
                engine.ensure_connection_state(&*guard)?;
                DuckDbEngine::execute_statement_on_conn(&*guard, &sql_for_exec)
            })
            .await
            .map_err(Self::status_from_join)?
            .map_err(Self::status_from_error)?
        } else {
            let engine = self.engine.clone();
            tokio::task::spawn_blocking(move || engine.execute_statement(&sql_for_exec))
                .await
                .map_err(Self::status_from_join)?
                .map_err(Self::status_from_error)?
        };

        info!(%sql, affected_rows, "statement completed");

        Ok(affected_rows)
    }

    #[instrument(skip(self, _request), fields(sql = %query.query))]
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        _request: Request<arrow_flight::Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let sql = query.query.clone();
        let is_query = Self::is_query_statement(&sql);
        let transaction_id = match query.transaction_id.as_ref() {
            Some(id) => Some(Self::parse_transaction_id(id.as_ref())?),
            None => None,
        };

        if !is_query && !self.engine.writes_enabled() {
            return Err(Self::status_from_error(ServerError::WritesDisabled));
        }

        if let Some(ref txn) = transaction_id {
            // Verify transaction exists
            let _ = self
                .transactions
                .get(txn)
                .await
                .map_err(Self::status_from_error)?;
        }

        info!(
            %sql,
            transaction = transaction_id.as_deref().unwrap_or("auto"),
            "creating prepared statement"
        );

        let dataset_schema = if is_query {
            if let Some(txn_id) = transaction_id.clone() {
                let conn = self
                    .transactions
                    .get(&txn_id)
                    .await
                    .map_err(Self::status_from_error)?;
                let sql_for_schema = sql.clone();
                let engine = self.engine.clone();
                tokio::task::spawn_blocking(move || {
                    let guard = conn.lock().expect("transaction connection poisoned");
                    engine.ensure_connection_state(&*guard)?;
                    let schema = DuckDbEngine::schema_for_query_on_conn(&*guard, &sql_for_schema)?;
                    SwanFlightSqlService::schema_to_ipc_bytes(&schema)
                })
                .await
                .map_err(Self::status_from_join)?
                .map_err(Self::status_from_error)?
            } else {
                let engine = self.engine.clone();
                let sql_for_schema = sql.clone();
                tokio::task::spawn_blocking(move || {
                    let schema = engine.schema_for_query(&sql_for_schema)?;
                    SwanFlightSqlService::schema_to_ipc_bytes(&schema)
                })
                .await
                .map_err(Self::status_from_join)?
                .map_err(Self::status_from_error)?
            }
        } else {
            Vec::new()
        };

        let handle_bytes = self.allocate_statement_handle();
        let handle_string =
            String::from_utf8(handle_bytes.clone()).expect("statement handle must be utf-8");

        self.prepared
            .insert(
                handle_string.clone(),
                PreparedStatementMeta {
                    sql: sql.clone(),
                    is_query,
                    transaction_id: transaction_id.clone(),
                },
            )
            .await;

        info!(
            handle = handle_string,
            schema_len = dataset_schema.len(),
            is_query,
            "prepared statement created"
        );

        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: handle_bytes.into(),
            dataset_schema: dataset_schema.into(),
            parameter_schema: Vec::<u8>::new().into(),
        })
    }

    #[instrument(skip(self, request), fields(handle_len = query.prepared_statement_handle.len()))]
    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let handle = Self::parse_statement_handle(&query.prepared_statement_handle)?;
        let meta = self
            .prepared
            .meta(&handle)
            .await
            .map_err(Self::status_from_error)?;

        if !meta.is_query {
            return Err(Status::invalid_argument(
                "prepared statement does not return a result set",
            ));
        }

        info!(
            handle = handle.as_str(),
            sql = %meta.sql,
            transaction = meta.transaction_id.as_deref().unwrap_or("auto"),
            "getting flight info for prepared statement"
        );

        let schema = if let Some(txn_id) = meta.transaction_id.clone() {
            let conn = self
                .transactions
                .get(&txn_id)
                .await
                .map_err(Self::status_from_error)?;
            let sql_for_schema = meta.sql.clone();
            let engine = self.engine.clone();
            tokio::task::spawn_blocking(move || {
                let guard = conn.lock().expect("transaction connection poisoned");
                engine.ensure_connection_state(&*guard)?;
                DuckDbEngine::schema_for_query_on_conn(&*guard, &sql_for_schema)
            })
            .await
            .map_err(Self::status_from_join)?
            .map_err(Self::status_from_error)?
        } else {
            let engine = self.engine.clone();
            let sql_for_schema = meta.sql.clone();
            tokio::task::spawn_blocking(move || engine.schema_for_query(&sql_for_schema))
                .await
                .map_err(Self::status_from_join)?
                .map_err(Self::status_from_error)?
        };

        debug!(
            handle = handle.as_str(),
            field_count = schema.fields().len(),
            "prepared statement schema retrieved"
        );

        // Create ticket for execution
        let handle_bytes = query.encode_to_vec();
        let descriptor = request.into_inner();
        let ticket = TicketStatementQuery {
            statement_handle: handle_bytes.into(),
        };
        let ticket_bytes = ticket.as_any().encode_to_vec();
        let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(ticket_bytes));

        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|err| Status::internal(format!("failed to encode schema: {err}")))?
            .with_descriptor(descriptor)
            .with_endpoint(endpoint)
            .with_total_records(-1); // -1 = unknown row count (per Flight SQL protocol)

        Ok(Response::new(info))
    }

    #[instrument(skip(self, _request), fields(handle_len = query.prepared_statement_handle.len()))]
    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let handle = Self::parse_statement_handle(&query.prepared_statement_handle)?;
        let meta = self
            .prepared
            .meta(&handle)
            .await
            .map_err(Self::status_from_error)?;

        if !meta.is_query {
            return Err(Status::invalid_argument(
                "prepared statement does not return a result set",
            ));
        }

        self.execute_prepared_query_handle(&handle, meta.clone())
            .await
    }

    #[instrument(skip(self, request), fields(handle_len = query.prepared_statement_handle.len()))]
    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        let handle = Self::parse_statement_handle(&query.prepared_statement_handle)?;
        let meta = self
            .prepared
            .meta(&handle)
            .await
            .map_err(Self::status_from_error)?;

        if !meta.is_query {
            return Err(Status::invalid_argument(
                "prepared statement does not support query binding",
            ));
        }

        let parameter_sets = Self::collect_parameter_sets(request).await?;
        self.prepared
            .set_parameters(&handle, Some(parameter_sets))
            .await
            .map_err(Self::status_from_error)?;

        info!(
            handle = handle.as_str(),
            "parameters bound to prepared statement"
        );

        Ok(DoPutPreparedStatementResult {
            prepared_statement_handle: None,
        })
    }

    #[instrument(skip(self, _request), fields(handle_len = query.prepared_statement_handle.len()))]
    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        _request: Request<arrow_flight::Action>,
    ) -> Result<(), Status> {
        let handle = Self::parse_statement_handle(&query.prepared_statement_handle)?;
        self.prepared
            .remove(&handle)
            .await
            .map_err(Self::status_from_error)?;
        info!(handle = handle.as_str(), "prepared statement closed");
        Ok(())
    }

    #[instrument(skip(self, request), fields(handle_len = query.prepared_statement_handle.len()))]
    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let handle = Self::parse_statement_handle(&query.prepared_statement_handle)?;
        let meta = self
            .prepared
            .meta(&handle)
            .await
            .map_err(Self::status_from_error)?;

        if meta.is_query {
            return Err(Status::invalid_argument(
                "prepared statement returns rows; use ExecuteQuery",
            ));
        }

        let mut parameter_sets = Self::collect_parameter_sets(request).await?;
        if parameter_sets.is_empty() {
            parameter_sets.push(Vec::new());
        }

        info!(
            handle = handle.as_str(),
            sql = %meta.sql,
            transaction = meta.transaction_id.as_deref().unwrap_or("auto"),
            "executing prepared statement update via do_put_prepared_statement_update"
        );

        self.engine.register_session_init(&meta.sql);
        let affected_rows = if let Some(txn_id) = meta.transaction_id.clone() {
            let conn_arc = self
                .transactions
                .get(&txn_id)
                .await
                .map_err(Self::status_from_error)?;
            let sql_for_exec = meta.sql.clone();
            let params = parameter_sets.clone();
            tokio::task::spawn_blocking(move || {
                let guard = conn_arc.lock().expect("transaction connection poisoned");
                SwanFlightSqlService::execute_statement_batches(&*guard, &sql_for_exec, &params)
            })
            .await
            .map_err(Self::status_from_join)?
            .map_err(Self::status_from_error)?
        } else {
            let engine = self.engine.clone();
            let sql_for_exec = meta.sql.clone();
            let params = parameter_sets.clone();
            tokio::task::spawn_blocking(move || {
                let conn = engine.get_write_connection()?;
                SwanFlightSqlService::execute_statement_batches(&conn, &sql_for_exec, &params)
            })
            .await
            .map_err(Self::status_from_join)?
            .map_err(Self::status_from_error)?
        };

        info!(
            handle = handle.as_str(),
            affected_rows, "prepared statement update completed"
        );

        Ok(affected_rows)
    }

    #[instrument(skip(self, _request))]
    async fn do_action_begin_transaction(
        &self,
        _query: ActionBeginTransactionRequest,
        _request: Request<arrow_flight::Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        if !self.engine.writes_enabled() {
            return Err(Self::status_from_error(ServerError::WritesDisabled));
        }

        debug!("do_action_begin_transaction called");

        let engine = self.engine.clone();
        let transaction_id = format!(
            "txn_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );

        let txn_id_clone = transaction_id.clone();
        let conn = tokio::task::spawn_blocking(move || {
            let conn = engine.get_write_connection()?;
            conn.execute_batch("BEGIN TRANSACTION")?;
            Ok::<_, ServerError>(conn)
        })
        .await
        .map_err(Self::status_from_join)?
        .map_err(Self::status_from_error)?;

        self.transactions.insert(txn_id_clone.clone(), conn).await;

        info!(transaction_id = txn_id_clone, "transaction started");

        Ok(ActionBeginTransactionResult {
            transaction_id: transaction_id.into_bytes().into(),
        })
    }

    #[instrument(skip(self, _request), fields(transaction_id))]
    async fn do_action_end_transaction(
        &self,
        query: ActionEndTransactionRequest,
        _request: Request<arrow_flight::Action>,
    ) -> Result<(), Status> {
        let transaction_id = String::from_utf8(query.transaction_id.to_vec())
            .map_err(|_| Status::invalid_argument("invalid transaction id encoding"))?;

        tracing::Span::current().record("transaction_id", &transaction_id);

        let action = query.action;
        debug!(transaction_id, action, "do_action_end_transaction called");

        let conn_arc = match self.transactions.remove(&transaction_id).await {
            Some(conn) => conn,
            None => {
                return Err(Self::status_from_error(ServerError::TransactionNotFound(
                    transaction_id,
                )))
            }
        };

        let commit_result = tokio::task::spawn_blocking(move || {
            let guard = conn_arc.lock().expect("transaction connection poisoned");
            let sql = if action == 0 { "COMMIT" } else { "ROLLBACK" };
            guard.execute_batch(sql)?;
            Ok::<_, ServerError>(())
        })
        .await
        .map_err(Self::status_from_join)?
        .map_err(Self::status_from_error);

        match commit_result {
            Ok(()) => {
                // all good
            }
            Err(status) => {
                if action == 0
                    && status
                        .message()
                        .contains("Cannot commit when autocommit is enabled")
                {
                    info!(
                        transaction_id,
                        "commit requested while autocommit enabled; treated as no-op"
                    );
                } else if action != 0
                    && status
                        .message()
                        .contains("cannot rollback when autocommit is enabled")
                {
                    info!(
                        transaction_id,
                        "rollback requested while autocommit enabled; treated as no-op"
                    );
                } else {
                    return Err(status);
                }
            }
        }

        if action == 0 {
            info!(transaction_id, "transaction committed");
        } else {
            info!(transaction_id, "transaction rolled back");
        }

        Ok(())
    }
}
