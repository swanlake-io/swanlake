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
use crate::session::{PreparedStatementMeta, PreparedStatementOptions};
use crate::sql_parser::{ParsedStatement, TableReference};

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
    let handle = parse_statement_handle(
        query.prepared_statement_handle.as_ref(),
        "get_flight_info_prepared_statement",
    )?;
    let session = service.prepare_request(&request).await?;

    let meta = session
        .get_prepared_statement_meta(handle)
        .map_err(SwanFlightSqlService::status_from_error)?;

    tracing::Span::current().record("sql", meta.sql.as_str());

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
    let handle = parse_statement_handle(
        query.prepared_statement_handle.as_ref(),
        "do_get_prepared_statement",
    )?;
    let session = service.prepare_request(&request).await?;

    let meta = session
        .get_prepared_statement_meta(handle)
        .map_err(SwanFlightSqlService::status_from_error)?;

    tracing::Span::current().record("sql", meta.sql.as_str());

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
    let handle = parse_statement_handle(
        query.prepared_statement_handle.as_ref(),
        "do_put_prepared_statement_query",
    )?;
    let session = service.prepare_request(&request).await?;

    let meta = session
        .get_prepared_statement_meta(handle)
        .map_err(SwanFlightSqlService::status_from_error)?;

    tracing::Span::current().record("sql", meta.sql.as_str());

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
///
/// This is the entry point when clients send Arrow batches via DoPut to execute
/// a prepared statement that modifies data or schema.
///
/// # Execution Paths
///
/// ## 1. Duckling Queue INSERT (Optimized)
/// For `INSERT INTO duckling_queue.<table>`:
/// - Directly enqueues Arrow batches without conversion
/// - Avoids wasteful Arrow → params → VALUES → Arrow round-trip
/// - Aligns batch schema if INSERT specifies column list
///
/// ## 2. Regular Table INSERT (Appender)
/// For `INSERT INTO <regular_table>`:
/// - Uses DuckDB appender API for efficient bulk insert
/// - Aligns batch schema to match table schema
/// - Handles column reordering if needed
///
/// ## 3. Other Statements (Parameter Batching)
/// For UPDATE, DELETE, DDL, etc.:
/// - Converts Arrow batches to parameter sets
/// - Executes statement once per parameter set
/// - Standard SQL execution path
///
/// # Flow
/// 1. Parse the prepared statement SQL
/// 2. Detect statement type (INSERT vs others)
/// 3. Route to appropriate execution path
/// 4. Return total affected rows
pub(crate) async fn do_put_prepared_statement_update(
    service: &SwanFlightSqlService,
    query: CommandPreparedStatementUpdate,
    request: Request<PeekableFlightDataStream>,
) -> Result<i64, Status> {
    let handle = parse_statement_handle(
        query.prepared_statement_handle.as_ref(),
        "do_put_prepared_statement_update",
    )?;
    let session = service.prepare_request(&request).await?;

    let PreparedStatementMeta { sql, is_query, .. } =
        session
            .get_prepared_statement_meta(handle)
            .map_err(SwanFlightSqlService::status_from_error)?;

    tracing::Span::current().record("sql", sql.as_str());

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
        if is_duckling_queue_table(&table_ref) {
            // If the INSERT contains server-side expressions/defaults in the VALUES list,
            // fall back to executing the statement so those expressions are evaluated.
            let values_all_placeholders = parsed.insert_values_all_placeholders();
            if !values_all_placeholders {
                let params = SwanFlightSqlService::collect_parameter_sets(request).await?;
                info!(
                    handle = %handle,
                    sql = %sql,
                    "falling back to parameter execution for duckling_queue (expressions/defaults present)"
                );
                let session_clone = Arc::clone(&session);
                return tokio::task::spawn_blocking(move || {
                    SwanFlightSqlService::execute_statement_batches(&sql, &params, &session_clone)
                })
                .await
                .map_err(SwanFlightSqlService::status_from_join)?
                .map_err(SwanFlightSqlService::status_from_error);
            }

            // Optimized path for duckling_queue: directly enqueue Arrow batches
            // instead of converting Arrow → params → VALUES → Arrow (wasteful)
            //
            // Flow:
            // 1. Collect Arrow batches from DoPut stream
            // 2. Align batches to match INSERT column order if needed
            // 3. Enqueue directly to duckling_queue coordinator
            let batches = SwanFlightSqlService::collect_record_batches(request).await?;

            if batches.is_empty() {
                // No data to enqueue
                0
            } else {
                let parts = table_ref.parts();
                let target_table = parts[1].to_string(); // Extract "events" from "duckling_queue.events"
                let target_catalog = service.registry.target_catalog().to_string();
                let insert_columns = parsed.get_insert_columns();

                info!(
                    handle = %handle,
                    sql = %sql,
                    target_table = %target_table,
                    batch_count = batches.len(),
                    total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                    "enqueuing duckling_queue batches directly (optimized path)"
                );

                let session_clone = Arc::clone(&session);
                tokio::task::spawn_blocking(move || {
                    // If INSERT specifies columns like "INSERT INTO t (id, name) VALUES (?)",
                    // we need to align the Arrow batch schema to match the column order
                    // Get the destination table schema from the target catalog (duckling_queue is write-only envelope)
                    let qualified_table = format!("{}.{}", target_catalog, target_table);
                    let table_schema = Arc::new(session_clone.table_schema(&qualified_table)?);

                    // Align each batch to match the destination schema; fail fast on mismatches so we
                    // don't enqueue data that would later fail during async flush.
                    let batches_to_enqueue = batches
                        .iter()
                        .map(|batch| {
                            align_batch_to_table_schema(
                                batch,
                                &table_schema,
                                insert_columns.as_deref(),
                            )
                        })
                        .collect::<Result<Vec<_>, crate::error::ServerError>>()?;

                    session_clone.enqueue_duckling_batches(&target_table, batches_to_enqueue)
                })
                .await
                .map_err(SwanFlightSqlService::status_from_join)?
                .map_err(SwanFlightSqlService::status_from_error)?
            }
        } else {
            // Regular table INSERT path (not duckling_queue)
            // Uses DuckDB appender API for efficient bulk inserts
            //
            // Flow:
            // 1. Collect Arrow batches from DoPut stream
            // 2. Align batches to match table schema if needed
            // 3. Use DuckDB appender to insert efficiently
            let batches = SwanFlightSqlService::collect_record_batches(request).await?;
            if batches.is_empty() {
                // No batches sent, execute the statement with empty params
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
                    session.resolve_catalog_and_table(parts, service.registry.target_catalog());
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
                                align_batch_to_table_schema(
                                    batch,
                                    &table_schema,
                                    Some(cols.as_slice()),
                                )
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

fn is_duckling_queue_table(table_ref: &TableReference) -> bool {
    let parts = table_ref.parts();
    parts.len() == 2 && parts[0].eq_ignore_ascii_case("duckling_queue")
}
