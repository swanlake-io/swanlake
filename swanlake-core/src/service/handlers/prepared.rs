use std::sync::Arc;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::PeekableFlightDataStream;
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, CommandPreparedStatementQuery,
    CommandPreparedStatementUpdate, DoPutPreparedStatementResult, ProstMessageExt,
    TicketStatementQuery,
};
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use arrow_schema::Schema;
use prost::Message;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info};

use super::ticket::{StatementTicketKind, TicketStatementPayload};
use crate::engine::batch::align_batch_to_table_schema;
use crate::service::SwanFlightSqlService;
use crate::session::id::StatementHandle;
use crate::session::{PreparedStatementMeta, PreparedStatementOptions, Session};
use crate::sql_parser::ParsedStatement;

const DEFAULT_CATALOG: &str = "swanlake";

fn parse_statement_handle(bytes: &[u8], context: &str) -> Result<StatementHandle, Status> {
    StatementHandle::from_bytes(bytes).ok_or_else(|| {
        error!(
            context = %context,
            handle_len = bytes.len(),
            "invalid prepared statement handle"
        );
        Status::invalid_argument("invalid prepared statement handle")
    })
}

async fn resolve_session_and_meta<T>(
    service: &SwanFlightSqlService,
    handle_bytes: &[u8],
    context: &'static str,
    request: &Request<T>,
) -> Result<(Arc<Session>, StatementHandle, PreparedStatementMeta), Status> {
    let handle = parse_statement_handle(handle_bytes, context)?;
    let session = service.prepare_request(request).await?;
    let meta = session
        .get_prepared_statement_meta(handle)
        .map_err(SwanFlightSqlService::status_from_error)?;
    tracing::Span::current().record("sql", meta.sql.as_str());
    Ok((session, handle, meta))
}

/// Creates a prepared statement for the provided SQL, inferring whether it is
/// a query and caching schema when possible. Invoked by the FlightSQL
/// `CreatePreparedStatement` action.
pub(crate) async fn do_action_create_prepared_statement(
    service: &SwanFlightSqlService,
    query: ActionCreatePreparedStatementRequest,
    request: Request<arrow_flight::Action>,
) -> Result<ActionCreatePreparedStatementResult, Status> {
    let sql = query.query;

    // Try to parse SQL to determine if this is a query, but don't fail if it can't be parsed
    // (e.g., multi-statement SQL or vendor-specific syntax)
    let is_query = if let Some(parsed) = crate::sql_parser::ParsedStatement::parse(&sql) {
        parsed.is_query()
    } else {
        // Fallback: treat multi-statement or unparseable SQL as non-query (safer default)
        // User will get appropriate error when trying to execute if this is wrong
        false
    };

    tracing::Span::current().record("is_query", is_query);
    let session = service.prepare_request(&request).await?;

    let (cached_schema, dataset_schema) = if is_query {
        let sql_for_schema = sql.clone();
        let session_clone = Arc::clone(&session);
        tokio::task::spawn_blocking(move || {
            let schema = session_clone.schema_for_query(&sql_for_schema)?;
            let bytes = SwanFlightSqlService::schema_to_ipc_bytes(&schema)?;
            Ok::<_, crate::error::ServerError>((Some(schema), bytes))
        })
        .await
        .map_err(SwanFlightSqlService::status_from_join)?
        .map_err(SwanFlightSqlService::status_from_error)?
    } else {
        (None, Vec::new())
    };

    let handle = session
        .create_prepared_statement(
            sql.clone(),
            is_query,
            PreparedStatementOptions::new().with_cached_schema(cached_schema),
        )
        .map_err(SwanFlightSqlService::status_from_error)?;

    info!(
        handle = %handle,
        schema_len = dataset_schema.len(),
        is_query,
        "prepared statement created in session"
    );

    Ok(ActionCreatePreparedStatementResult {
        prepared_statement_handle: handle.into(),
        dataset_schema: dataset_schema.into(),
        parameter_schema: Vec::<u8>::new().into(),
    })
}

/// Returns FlightInfo (including ticket and schema) for a prepared statement so
/// clients can call DoGet. Streams schemas for query statements and returns an
/// empty schema for commands so ExecuteQuery callers can run DDL/DML.
pub(crate) async fn get_flight_info_prepared_statement(
    service: &SwanFlightSqlService,
    query: CommandPreparedStatementQuery,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    let (session, handle, meta) = resolve_session_and_meta(
        service,
        query.prepared_statement_handle.as_ref(),
        "get_flight_info_prepared_statement",
        &request,
    )
    .await?;

    let returns_rows = meta.is_query;
    info!(
        handle = %handle,
        sql = %meta.sql,
        returns_rows,
        "getting flight info for prepared statement"
    );

    let schema = if returns_rows {
        if let Some(schema) = &meta.schema {
            schema.clone()
        } else {
            let sql_for_schema = meta.sql.clone();
            let session_clone = Arc::clone(&session);
            let schema = tokio::task::spawn_blocking(move || {
                session_clone.schema_for_query(&sql_for_schema)
            })
            .await
            .map_err(SwanFlightSqlService::status_from_join)?
            .map_err(SwanFlightSqlService::status_from_error)?;
            session
                .cache_prepared_statement_schema(handle, schema.clone())
                .map_err(SwanFlightSqlService::status_from_error)?;
            schema
        }
    } else {
        Schema::empty()
    };

    debug!(
        handle = %handle,
        field_count = schema.fields().len(),
        "prepared statement schema retrieved"
    );

    let descriptor = request.into_inner();
    let ticket = TicketStatementQuery {
        statement_handle: TicketStatementPayload::new(StatementTicketKind::Prepared)
            .with_handle(handle)
            .with_returns_rows(returns_rows)
            .encode_to_vec()
            .into(),
    };
    let ticket_bytes = ticket.as_any().encode_to_vec();
    let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(ticket_bytes));

    let info = FlightInfo::new()
        .try_with_schema(&schema)
        .map_err(|err| Status::internal(format!("failed to encode schema: {err}")))?
        .with_descriptor(descriptor)
        .with_endpoint(endpoint)
        .with_total_records(-1);

    Ok(Response::new(info))
}

/// Streams results or executes a command for a prepared statement referenced by
/// handle. This is the DoGet path after `get_flight_info_prepared_statement`.
pub(crate) async fn do_get_prepared_statement(
    service: &SwanFlightSqlService,
    query: CommandPreparedStatementQuery,
    request: Request<Ticket>,
) -> Result<Response<<SwanFlightSqlService as FlightService>::DoGetStream>, Status> {
    let (session, handle, meta) = resolve_session_and_meta(
        service,
        query.prepared_statement_handle.as_ref(),
        "do_get_prepared_statement",
        &request,
    )
    .await?;

    if meta.is_query {
        service
            .execute_prepared_query_handle(&session, handle, meta)
            .await
    } else {
        service
            .execute_prepared_update_handle(&session, handle, meta)
            .await
    }
}

/// Binds parameters for a prepared statement via DoPut (query or command). No
/// rows are returned; it only updates the prepared statement's bound parameters.
pub(crate) async fn do_put_prepared_statement_query(
    service: &SwanFlightSqlService,
    query: CommandPreparedStatementQuery,
    request: Request<PeekableFlightDataStream>,
) -> Result<DoPutPreparedStatementResult, Status> {
    let (session, handle, meta) = resolve_session_and_meta(
        service,
        query.prepared_statement_handle.as_ref(),
        "do_put_prepared_statement_query",
        &request,
    )
    .await?;

    let parameter_sets = SwanFlightSqlService::collect_parameter_sets(request).await?;

    if let Some(first_params) = parameter_sets.into_iter().next() {
        session
            .set_prepared_statement_parameters(handle, first_params)
            .map_err(SwanFlightSqlService::status_from_error)?;
    }

    info!(
        handle = %handle,
        returns_rows = meta.is_query,
        "parameters bound to prepared statement"
    );

    Ok(DoPutPreparedStatementResult::default())
}

/// Closes a prepared statement handle within the current session.
pub(crate) async fn do_action_close_prepared_statement(
    service: &SwanFlightSqlService,
    query: ActionClosePreparedStatementRequest,
    request: Request<arrow_flight::Action>,
) -> Result<(), Status> {
    let handle = parse_statement_handle(
        query.prepared_statement_handle.as_ref(),
        "do_action_close_prepared_statement",
    )?;
    let session = service.prepare_request(&request).await?;

    session
        .close_prepared_statement(handle)
        .map_err(SwanFlightSqlService::status_from_error)?;
    info!(handle = %handle, "prepared statement closed");
    Ok(())
}

/// Handle DoPut for prepared statement updates (INSERT, UPDATE, DELETE, DDL).
pub(crate) async fn do_put_prepared_statement_update(
    service: &SwanFlightSqlService,
    query: CommandPreparedStatementUpdate,
    request: Request<PeekableFlightDataStream>,
) -> Result<i64, Status> {
    let (session, handle, PreparedStatementMeta { sql, is_query, .. }) = resolve_session_and_meta(
        service,
        query.prepared_statement_handle.as_ref(),
        "do_put_prepared_statement_update",
        &request,
    )
    .await?;

    if is_query {
        error!(
            handle = %handle,
            sql = %sql,
            "prepared statement returns rows; use ExecuteQuery"
        );
        return Err(Status::invalid_argument(
            "prepared statement returns rows; use ExecuteQuery",
        ));
    }

    let parsed_opt = ParsedStatement::parse(&sql);

    let affected_rows = if let Some((parsed, table_ref)) = parsed_opt
        .as_ref()
        .filter(|p| p.is_insert())
        .and_then(|p| p.get_insert_table().map(|t| (p, t)))
    {
        // Uses DuckDB appender API for efficient bulk inserts.
        let batches = SwanFlightSqlService::collect_record_batches(request).await?;
        if batches.is_empty() {
            // No batches sent, execute the statement with empty params.
            let session_clone = Arc::clone(&session);
            tokio::task::spawn_blocking(move || {
                SwanFlightSqlService::execute_statement_batches(&sql, &[], &session_clone)
            })
            .await
            .map_err(SwanFlightSqlService::status_from_join)?
            .map_err(SwanFlightSqlService::status_from_error)?
        } else {
            let parts = table_ref.parts();

            // Extract catalog and table name from the parsed reference
            // Format: "catalog.table" or just "table" (uses default catalog)
            let (catalog_name, table_name) =
                session.resolve_catalog_and_table(parts, DEFAULT_CATALOG);
            let insert_columns = parsed.get_insert_columns();

            info!(
                handle = %handle,
                table_name = %table_name,
                batch_count = batches.len(),
                total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                "using appender optimization for INSERT"
            );

            let session_clone = Arc::clone(&session);
            tokio::task::spawn_blocking(move || {
                // Get the target table schema for validation and alignment
                let qualified_table = format!("{}.{}", catalog_name, table_name);
                let table_schema = Arc::new(session_clone.table_schema(&qualified_table)?);

                // Align batches if INSERT specifies column list
                // Example: "INSERT INTO t (col2, col1)" needs column reordering
                let batches_to_use = if let Some(cols) = insert_columns.as_ref() {
                    batches
                        .iter()
                        .map(|batch| {
                            align_batch_to_table_schema(batch, &table_schema, Some(cols.as_slice()))
                        })
                        .collect::<Result<Vec<_>, crate::error::ServerError>>()?
                } else {
                    batches
                };

                // Use DuckDB appender API for efficient bulk insert
                let total_rows = session_clone
                    .insert_with_appender(&catalog_name, &table_name, batches_to_use)
                    .map_err(|e| {
                        error!(
                            %e,
                            "appender insert failed sql {} for table {}.{}",
                            sql,
                            catalog_name,
                            table_name
                        );
                        e
                    })?;
                Ok::<_, crate::error::ServerError>(total_rows as i64)
            })
            .await
            .map_err(SwanFlightSqlService::status_from_join)?
            .map_err(SwanFlightSqlService::status_from_error)?
        }
    } else {
        // Non-INSERT statements (UPDATE, DELETE, DDL, etc.)
        // Collect parameter sets and execute via standard path
        let params = SwanFlightSqlService::collect_parameter_sets(request).await?;
        info!(
            handle = %handle,
            sql = %sql,
            "executing prepared statement update with parameters"
        );

        let session_clone = Arc::clone(&session);
        tokio::task::spawn_blocking(move || {
            SwanFlightSqlService::execute_statement_batches(&sql, &params, &session_clone)
        })
        .await
        .map_err(SwanFlightSqlService::status_from_join)?
        .map_err(SwanFlightSqlService::status_from_error)?
    };

    info!(
        handle = %handle,
        affected_rows,
        "prepared statement update complete"
    );
    Ok(affected_rows)
}
