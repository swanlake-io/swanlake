use std::pin::Pin;
use std::sync::Arc;

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
use duckdb::types::Value;
use futures::{stream, Stream, StreamExt, TryStreamExt};
use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, instrument};

use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};

use crate::engine::connection::QueryResult;
use crate::error::ServerError;
use crate::session::{
    registry::SessionRegistry,
    session::{PreparedStatementMeta, Session},
    SessionId,
};
use uuid::Uuid;

// Phase 2 Complete: All state (prepared statements, transactions) is session-scoped
// - Each gRPC connection gets a dedicated session (based on remote_addr)
// - Sessions persist across requests from the same connection
// - Prepared statements and transactions are isolated per session
// - Automatic cleanup via idle timeout (30min default)

#[derive(Clone)]
pub struct SwanFlightSqlService {
    registry: Arc<SessionRegistry>,
}

impl SwanFlightSqlService {
    pub fn new(registry: Arc<SessionRegistry>) -> Self {
        Self { registry }
    }

    /// Extract session ID from tonic Request for session tracking (Phase 2)
    ///
    /// This uses the remote peer address as the session ID.
    /// Sessions persist across requests from the same gRPC connection.
    fn extract_session_id<T>(request: &Request<T>) -> SessionId {
        // Use remote address as session ID for connection-based persistence
        if let Some(addr) = request.remote_addr() {
            SessionId::from_string(addr.to_string())
        } else {
            // Fallback: generate a unique ID per request (Phase 1 behavior)
            SessionId::from_string(Uuid::new_v4().to_string())
        }
    }

    /// Get or create a session based on connection (Phase 2: connection-based persistence)
    ///
    /// Extracts session ID from the gRPC connection and reuses sessions across requests.
    fn get_session<T>(&self, request: &Request<T>) -> Result<Arc<Session>, Status> {
        let session_id = Self::extract_session_id(request);
        self.registry
            .get_or_create_by_id(&session_id)
            .map_err(|e| Self::status_from_error(e))
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
            ServerError::TransactionNotFound => Status::invalid_argument("unknown transaction"),
            ServerError::PreparedStatementNotFound => {
                Status::invalid_argument("unknown prepared statement")
            }

            ServerError::MaxSessionsReached => {
                Status::resource_exhausted("maximum number of sessions reached")
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
        sql: &str,
        param_batches: &[Vec<Value>],
        session: &Session,
    ) -> Result<i64, ServerError> {
        if param_batches.is_empty() {
            let affected = session.execute_statement_with_params(sql, &[])?;
            return Ok(affected as i64);
        }

        let mut total = 0i64;
        for params in param_batches {
            let affected = session.execute_statement_with_params(sql, params)?;
            total += affected as i64;
        }
        Ok(total)
    }

    async fn execute_prepared_query_handle(
        &self,
        session: &Arc<Session>,
        handle: &[u8],
        meta: PreparedStatementMeta,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let parameters = session
            .take_prepared_statement_parameters(handle)
            .map_err(Self::status_from_error)?
            .unwrap_or_else(Vec::new);

        info!(
            handle = ?handle,
            sql = %meta.sql,
            param_count = parameters.len(),
            "executing prepared statement via handle"
        );

        // Execute query on session's connection
        let sql_for_exec = meta.sql.clone();
        let params_for_exec = parameters.clone();
        let session_clone = session.clone();

        let QueryResult {
            schema,
            batches,
            total_rows,
            total_bytes,
        } = tokio::task::spawn_blocking(move || {
            if params_for_exec.is_empty() {
                session_clone.execute_query(&sql_for_exec)
            } else {
                session_clone.execute_query_with_params(&sql_for_exec, &params_for_exec)
            }
        })
        .await
        .map_err(Self::status_from_join)?
        .map_err(Self::status_from_error)?;

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
            handle = ?handle,
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

        info!(%sql, "planning query via get_flight_info_statement");

        // Get session (Phase 2: reuses existing session for same connection)
        let session = self.get_session(&request)?;

        // Execute schema extraction on session's dedicated connection
        let schema = tokio::task::spawn_blocking(move || session.schema_for_query(&sql))
            .await
            .map_err(Self::status_from_join)?
            .map_err(Self::status_from_error)?;

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

    #[instrument(skip(self, request), fields(handle_len = ticket.statement_handle.len()))]
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        // Try to decode as prepared statement first
        if let Ok(prepared_query) =
            CommandPreparedStatementQuery::decode(ticket.statement_handle.as_ref())
        {
            let handle = &prepared_query.prepared_statement_handle;

            // Get session (Phase 2: reuses existing session for same connection)
            let session = self.get_session(&request)?;

            // Try to get prepared statement metadata, but fall back to direct query if not found
            if let Ok(meta) = session.get_prepared_statement_meta(handle) {
                if !meta.is_query {
                    return Err(Status::invalid_argument(
                        "prepared statement does not return a result set",
                    ));
                }

                info!(
                    handle = ?handle,
                    sql = %meta.sql,
                    "executing prepared statement via do_get_statement"
                );

                return self
                    .execute_prepared_query_handle(&session, handle, meta)
                    .await;
            }
            // If prepared statement not found, fall through to direct query path
        }

        // Fall back to direct query
        let command = CommandStatementQuery::decode(ticket.statement_handle.as_ref())
            .map_err(|err| Status::invalid_argument(format!("invalid statement handle: {err}")))?;
        let sql = command.query.clone();

        info!(%sql, "executing query via do_get_statement");

        // Get session (Phase 2: reuses existing session for same connection)
        let session = self.get_session(&request)?;

        // Execute query on session's dedicated connection
        let sql_clone = sql.clone();
        let QueryResult {
            schema,
            batches,
            total_rows,
            total_bytes,
        } = tokio::task::spawn_blocking(move || session.execute_query(&sql_clone))
            .await
            .map_err(Self::status_from_join)?
            .map_err(Self::status_from_error)?;

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

    #[instrument(skip(self, request), fields(sql = %command.query))]
    async fn do_put_statement_update(
        &self,
        command: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let sql = command.query.clone();

        info!(%sql, "executing statement via do_put_statement_update");

        // Get session (Phase 2: reuses existing session for same connection)
        let session = self.get_session(&request)?;

        // Execute statement on session's dedicated connection
        let sql_clone = sql.clone();
        let affected_rows =
            tokio::task::spawn_blocking(move || session.execute_statement(&sql_clone))
                .await
                .map_err(Self::status_from_join)?
                .map_err(Self::status_from_error)?;

        info!(%sql, affected_rows, "statement completed");

        Ok(affected_rows)
    }

    #[instrument(skip(self, request), fields(sql = %query.query, is_query))]
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<arrow_flight::Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let sql = query.query.clone();
        let is_query = Self::is_query_statement(&sql);

        info!(%sql, is_query, "creating prepared statement");

        // Get session (Phase 2: reuses existing session for same connection)
        let session = self.get_session(&request)?;

        // Get schema for queries
        let dataset_schema = if is_query {
            let sql_for_schema = sql.clone();
            let session_clone = session.clone();
            tokio::task::spawn_blocking(move || {
                let schema = session_clone.schema_for_query(&sql_for_schema)?;
                SwanFlightSqlService::schema_to_ipc_bytes(&schema)
            })
            .await
            .map_err(Self::status_from_join)?
            .map_err(Self::status_from_error)?
        } else {
            Vec::new()
        };

        // Store in session-scoped prepared statement storage (Phase 2)
        let handle_bytes = session
            .create_prepared_statement(sql.clone(), is_query)
            .map_err(Self::status_from_error)?;

        info!(
            handle = ?handle_bytes,
            schema_len = dataset_schema.len(),
            is_query,
            "prepared statement created in session"
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
        let handle = &query.prepared_statement_handle;

        // Get session (Phase 2: reuses existing session for same connection)
        let session = self.get_session(&request)?;

        let meta = session
            .get_prepared_statement_meta(handle)
            .map_err(Self::status_from_error)?;

        if !meta.is_query {
            return Err(Status::invalid_argument(
                "prepared statement does not return a result set",
            ));
        }

        info!(
            handle = ?handle,
            sql = %meta.sql,
            "getting flight info for prepared statement"
        );

        // Get schema for the prepared statement
        let sql_for_schema = meta.sql.clone();
        let session_clone = session.clone();
        let schema =
            tokio::task::spawn_blocking(move || session_clone.schema_for_query(&sql_for_schema))
                .await
                .map_err(Self::status_from_join)?
                .map_err(Self::status_from_error)?;

        debug!(
            handle = ?handle,
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

    #[instrument(skip(self, request), fields(handle_len = query.prepared_statement_handle.len()))]
    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let handle = &query.prepared_statement_handle;

        // Get session (Phase 2: reuses existing session for same connection)
        let session = self.get_session(&request)?;

        let meta = session
            .get_prepared_statement_meta(handle)
            .map_err(Self::status_from_error)?;

        if !meta.is_query {
            return Err(Status::invalid_argument(
                "prepared statement does not return a result set",
            ));
        }

        self.execute_prepared_query_handle(&session, handle, meta)
            .await
    }

    #[instrument(skip(self, request))]
    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        let handle = &query.prepared_statement_handle;

        // Get session (Phase 2: reuses existing session for same connection)
        let session = self.get_session(&request)?;

        let meta = session
            .get_prepared_statement_meta(handle)
            .map_err(Self::status_from_error)?;

        if !meta.is_query {
            return Err(Status::invalid_argument(
                "prepared statement does not support query binding",
            ));
        }

        let parameter_sets = Self::collect_parameter_sets(request).await?;

        // Use first parameter set (Flight SQL doesn't support batching for queries)
        if let Some(first_params) = parameter_sets.into_iter().next() {
            session
                .set_prepared_statement_parameters(handle, first_params)
                .map_err(Self::status_from_error)?;
        }

        info!(
            handle = ?handle,
            "parameters bound to prepared statement"
        );

        Ok(DoPutPreparedStatementResult::default())
    }

    #[instrument(skip(self, request), fields(handle_len = query.prepared_statement_handle.len()))]
    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        request: Request<arrow_flight::Action>,
    ) -> Result<(), Status> {
        let handle = &query.prepared_statement_handle;

        // Get session (Phase 2: reuses existing session for same connection)
        let session = self.get_session(&request)?;

        session
            .close_prepared_statement(handle)
            .map_err(Self::status_from_error)?;
        info!(handle = ?handle, "prepared statement closed");
        Ok(())
    }

    #[instrument(skip(self, request))]
    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let handle = &query.prepared_statement_handle;

        // Get session (Phase 2: reuses existing session for same connection)
        let session = self.get_session(&request)?;

        let meta = session
            .get_prepared_statement_meta(handle)
            .map_err(Self::status_from_error)?;

        if meta.is_query {
            return Err(Status::invalid_argument(
                "prepared statement returns rows; use ExecuteQuery",
            ));
        }

        let parameter_sets = Self::collect_parameter_sets(request).await?;

        info!(
            handle = ?handle,
            sql = %meta.sql,
            parameter_sets = parameter_sets.len(),
            "executing prepared statement update"
        );

        // Execute statement batches with session connection
        let sql_for_exec = meta.sql.clone();
        let params = parameter_sets.clone();
        let session_clone = session.clone();

        let affected_rows = tokio::task::spawn_blocking(move || {
            SwanFlightSqlService::execute_statement_batches(&sql_for_exec, &params, &session_clone)
        })
        .await
        .map_err(Self::status_from_join)?
        .map_err(Self::status_from_error)?;

        info!(
            handle = ?handle,
            affected_rows,
            "prepared statement update complete"
        );
        Ok(affected_rows)
    }

    #[instrument(skip(self, request))]
    async fn do_action_begin_transaction(
        &self,
        _query: ActionBeginTransactionRequest,
        request: Request<arrow_flight::Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        debug!("do_action_begin_transaction called");

        // Get session (Phase 2: reuses existing session for same connection)
        let session = self.get_session(&request)?;

        // Begin transaction on session's connection
        let session_clone = session.clone();
        let transaction_id = tokio::task::spawn_blocking(move || session_clone.begin_transaction())
            .await
            .map_err(Self::status_from_join)?
            .map_err(Self::status_from_error)?;

        info!(transaction_id = ?transaction_id, "transaction started in session");

        Ok(ActionBeginTransactionResult {
            transaction_id: transaction_id.into(),
        })
    }

    #[instrument(skip(self, request), fields(transaction_id))]
    async fn do_action_end_transaction(
        &self,
        query: ActionEndTransactionRequest,
        request: Request<arrow_flight::Action>,
    ) -> Result<(), Status> {
        let transaction_id = query.transaction_id.to_vec();

        tracing::Span::current().record("transaction_id", format!("{:?}", transaction_id).as_str());

        let action = query.action;
        debug!(transaction_id = ?transaction_id, action, "do_action_end_transaction called");

        // Get session (Phase 2: reuses existing session for same connection)
        let session = self.get_session(&request)?;

        // Commit or rollback transaction on session's connection
        let session_clone = session.clone();
        let txn_id_clone = transaction_id.clone();

        let commit_result = tokio::task::spawn_blocking(move || {
            if action == 0 {
                session_clone.commit_transaction(&txn_id_clone)
            } else {
                session_clone.rollback_transaction(&txn_id_clone)
            }
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
                        transaction_id = ?transaction_id,
                        "commit requested while autocommit enabled; treated as no-op"
                    );
                } else if action != 0
                    && status
                        .message()
                        .contains("cannot rollback when autocommit is enabled")
                {
                    info!(
                        transaction_id = ?transaction_id,
                        "rollback requested while autocommit enabled; treated as no-op"
                    );
                } else {
                    return Err(status);
                }
            }
        }

        let op = if action == 0 {
            "committed"
        } else {
            "rolled back"
        };
        info!(transaction_id = ?transaction_id, op, "transaction completed in session");

        Ok(())
    }
}
