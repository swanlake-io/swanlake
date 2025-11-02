use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::server::PeekableFlightDataStream;
use arrow_flight::sql::{
    ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult,
    CommandPreparedStatementQuery, CommandStatementQuery, CommandStatementUpdate, ProstMessageExt,
    SqlInfo, TicketStatementQuery,
};
use arrow_flight::{
    flight_service_server::FlightService, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    Ticket,
};
use futures::{stream, Stream};
use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, instrument};

use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};

use crate::duckdb::{DuckDbEngine, QueryResult};
use crate::error::ServerError;

#[derive(Clone)]
pub struct SwanFlightSqlService {
    engine: Arc<DuckDbEngine>,
}

impl SwanFlightSqlService {
    pub fn new(engine: Arc<DuckDbEngine>) -> Self {
        Self { engine }
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
        let engine = self.engine.clone();

        info!(%sql, "planning query via get_flight_info_statement");

        let schema = tokio::task::spawn_blocking(move || engine.schema_for_query(&sql))
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

    #[instrument(skip(self, _request), fields(handle_len = ticket.statement_handle.len()))]
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let command = CommandStatementQuery::decode(ticket.statement_handle.as_ref())
            .map_err(|err| Status::invalid_argument(format!("invalid statement handle: {err}")))?;
        let sql = command.query.clone();
        let sql_for_exec = sql.clone();
        let engine = self.engine.clone();

        info!(%sql, "executing query via do_get_statement");

        let QueryResult {
            schema,
            batches,
            total_rows,
            total_bytes,
        } = tokio::task::spawn_blocking(move || engine.execute_query(&sql_for_exec))
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

    async fn register_sql_info(&self, id: i32, info: &SqlInfo) {
        tracing::debug!(id, ?info, "register_sql_info invoked");
    }

    #[instrument(skip(self, _request), fields(sql = %command.query))]
    async fn do_put_statement_update(
        &self,
        command: CommandStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let sql = command.query.clone();
        let engine = self.engine.clone();

        info!(%sql, "executing statement via do_put_statement_update");

        let sql_for_exec = sql.clone();
        let affected_rows =
            tokio::task::spawn_blocking(move || engine.execute_statement(&sql_for_exec))
                .await
                .map_err(Self::status_from_join)?
                .map_err(Self::status_from_error)?;

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
        let sql_for_exec = sql.clone();
        let engine = self.engine.clone();

        info!(%sql, "creating prepared statement");

        let (dataset_schema, parameter_schema) = tokio::task::spawn_blocking(move || {
            // Check if it's a query or statement by keyword detection
            // We can't use column_count() because it requires execution in DuckDB
            let is_query = Self::is_query_statement(&sql_for_exec);

            let schema_bytes = if !is_query {
                // Statement (DDL/DML) - empty schema
                debug!("detected statement via keyword analysis");
                vec![]
            } else {
                // Query - prepare and get schema
                debug!("detected query via keyword analysis");
                let conn = engine.get_connection()?;
                let mut stmt = conn.prepare(&sql_for_exec)?;

                // Execute to get schema (queries are safe to execute for schema detection)
                let arrow = stmt.query_arrow([])?;
                let schema = arrow.get_schema();

                // Convert Arrow schema to IPC bytes
                let data_gen = IpcDataGenerator::default();
                let mut dict_tracker = DictionaryTracker::new(false);
                let write_options = IpcWriteOptions::default();
                let encoded_data = data_gen.schema_to_bytes_with_dictionary_tracker(
                    schema.as_ref(),
                    &mut dict_tracker,
                    &write_options,
                );

                let mut writer = vec![];
                arrow_ipc::writer::write_message(&mut writer, encoded_data, &write_options)
                    .map_err(|e| ServerError::Arrow(e))?;
                writer
            };

            // Parameter schema (not supported yet)
            let param_schema = vec![];

            Ok::<_, ServerError>((schema_bytes, param_schema))
        })
        .await
        .map_err(Self::status_from_join)?
        .map_err(Self::status_from_error)?;

        // Use SQL as the prepared statement handle
        let handle = sql.as_bytes().to_vec();

        info!(
            handle_len = handle.len(),
            schema_len = dataset_schema.len(),
            "prepared statement created"
        );

        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: handle.into(),
            dataset_schema: dataset_schema.into(),
            parameter_schema: parameter_schema.into(),
        })
    }

    #[instrument(skip(self, request), fields(handle_len = query.prepared_statement_handle.len()))]
    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // Decode the prepared statement handle (which is just the SQL text)
        let sql = String::from_utf8(query.prepared_statement_handle.to_vec())
            .map_err(|err| Status::invalid_argument(format!("invalid handle encoding: {err}")))?;

        let engine = self.engine.clone();
        let sql_for_schema = sql.clone();

        info!(%sql, "getting flight info for prepared statement");

        // Get schema using schema_for_query (optimized with LIMIT 0)
        //
        // Implementation Note:
        // We use SQL text as the handle and re-prepare on execution because:
        // 1. DuckDB prepared statements are tied to specific connections
        // 2. Connection pooling means we can't guarantee same connection
        // 3. Multiple server instances can't share statement handles
        // 4. Re-preparing is fast (~1-2ms) and avoids caching complexity
        //
        // The schema_for_query() uses LIMIT 0 optimization to extract schema
        // without executing the full query. See PREPARED_STATEMENT_OPTIONS.md
        let schema = tokio::task::spawn_blocking(move || engine.schema_for_query(&sql_for_schema))
            .await
            .map_err(Self::status_from_join)?
            .map_err(Self::status_from_error)?;

        debug!(
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
        // Decode the prepared statement handle (which is just the SQL text)
        let sql = String::from_utf8(query.prepared_statement_handle.to_vec())
            .map_err(|err| Status::invalid_argument(format!("invalid handle encoding: {err}")))?;

        let engine = self.engine.clone();
        let sql_for_exec = sql.clone();

        info!(%sql, "executing prepared statement via do_get_prepared_statement");

        let QueryResult {
            schema,
            batches,
            total_rows,
            total_bytes,
        } = tokio::task::spawn_blocking(move || engine.execute_query(&sql_for_exec))
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
        info!(%sql, total_rows, total_bytes, "prepared statement completed");
        Ok(response)
    }
}
