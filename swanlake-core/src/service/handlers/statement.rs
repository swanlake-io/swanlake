use std::sync::Arc;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::PeekableFlightDataStream;
use arrow_flight::sql::{
    CommandStatementQuery, CommandStatementUpdate, ProstMessageExt, TicketStatementQuery,
};
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use arrow_schema::Schema;
use prost::Message;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info};

use super::ticket::{StatementTicketKind, TicketStatementPayload};
use crate::error::ServerError;
use crate::service::SwanFlightSqlService;
use crate::session::PreparedStatementOptions;
use crate::sql_parser::ParsedStatement;

/// Plans an ad-hoc SQL statement and returns a FlightInfo+ticket that can be
/// consumed via DoGet. Supports both queries (with planned schema) and commands
/// (empty schema so ExecuteQuery callers can still issue DDL/DML).
pub(crate) async fn get_flight_info_statement(
    service: &SwanFlightSqlService,
    query: CommandStatementQuery,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    let sql = query.query;
    let session = service.prepare_request(&request).await?;

    let parsed = ParsedStatement::parse(&sql);
    let mut returns_rows = parsed.as_ref().is_some_and(|p| p.is_query());

    let schema = if returns_rows || parsed.is_none() {
        let sql_for_schema = sql.clone();
        let session_for_schema = Arc::clone(&session);
        match tokio::task::spawn_blocking(move || {
            session_for_schema.schema_for_query(&sql_for_schema)
        })
        .await
        {
            Ok(Ok(schema)) => {
                debug!(field_count = schema.fields().len(), "planned schema");
                returns_rows = true;
                schema
            }
            Ok(Err(err)) if returns_rows => {
                return Err(SwanFlightSqlService::status_from_error(err))
            }
            Ok(Err(err)) => {
                debug!(%err, "treating statement as command after schema planning failed");
                Schema::empty()
            }
            Err(join_err) => return Err(SwanFlightSqlService::status_from_join(join_err)),
        }
    } else {
        Schema::empty()
    };

    let descriptor = request.into_inner();
    let ticket = TicketStatementQuery {
        statement_handle: TicketStatementPayload::new(StatementTicketKind::Ephemeral)
            .with_fallback_sql(sql.clone())
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
        .with_endpoint(endpoint);

    Ok(Response::new(info))
}

/// Executes a ticket created by `get_flight_info_statement` or a prepared
/// statement ticket, streaming rows for query statements and executing commands
/// for non-query tickets so ExecuteQuery callers do not fail.
pub(crate) async fn do_get_statement(
    service: &SwanFlightSqlService,
    ticket: TicketStatementQuery,
    request: Request<Ticket>,
) -> Result<Response<<SwanFlightSqlService as FlightService>::DoGetStream>, Status> {
    let payload =
        TicketStatementPayload::decode(ticket.statement_handle.as_ref()).map_err(|err| {
            error!(%err, "failed to decode ticket payload");
            Status::invalid_argument("invalid ticket payload")
        })?;

    if let Some(handle) = payload.handle() {
        let session = service.prepare_request(&request).await?;
        match session.get_prepared_statement_meta(handle) {
            Ok(meta) => {
                let ticket_kind = payload
                    .ticket_kind()
                    .unwrap_or(StatementTicketKind::Prepared);
                info!(
                    handle = %handle,
                    kind = ?ticket_kind,
                    sql = %meta.sql,
                    returns_rows = meta.is_query,
                    "executing statement via indexed ticket"
                );
                return if meta.is_query {
                    service
                        .execute_prepared_query_handle(&session, handle, meta)
                        .await
                } else {
                    service
                        .execute_prepared_update_handle(&session, handle, meta)
                        .await
                };
            }
            Err(ServerError::PreparedStatementNotFound) => {
                if let Some(sql) = payload.fallback_sql_str() {
                    info!(
                        handle = %handle,
                        "prepared handle missing; executing fallback SQL"
                    );
                    return execute_ephemeral_ticket_statement(
                        service,
                        sql.to_owned(),
                        payload.returns_rows_flag(),
                        &request,
                    )
                    .await;
                }
                error!(handle = %handle, "unknown prepared statement handle");
                return Err(Status::invalid_argument("unknown prepared statement"));
            }
            Err(err) => {
                return Err(SwanFlightSqlService::status_from_error(err));
            }
        }
    }

    if let Some(sql) = payload.fallback_sql_str() {
        return execute_ephemeral_ticket_statement(
            service,
            sql.to_owned(),
            payload.returns_rows_flag(),
            &request,
        )
        .await;
    }

    error!("ticket payload missing handle and fallback SQL");
    Err(Status::invalid_argument("invalid ticket payload"))
}

/// Handles DoPut for ad-hoc update statements (INSERT/UPDATE/DDL) and returns
/// the affected row count. This is the ExecuteUpdate entrypoint for
/// non-prepared statements.
pub(crate) async fn do_put_statement_update(
    service: &SwanFlightSqlService,
    command: CommandStatementUpdate,
    request: Request<PeekableFlightDataStream>,
) -> Result<i64, Status> {
    let sql = command.query;
    let session = service.prepare_request(&request).await?;

    let affected_rows = tokio::task::spawn_blocking(move || session.execute_statement(&sql))
        .await
        .map_err(SwanFlightSqlService::status_from_join)?
        .map_err(SwanFlightSqlService::status_from_error)?;

    Ok(affected_rows)
}

async fn execute_ephemeral_ticket_statement(
    service: &SwanFlightSqlService,
    sql: String,
    returns_rows: bool,
    request: &Request<Ticket>,
) -> Result<Response<<SwanFlightSqlService as FlightService>::DoGetStream>, Status> {
    info!(sql = %sql, "executing ticket via fallback SQL");
    let session = service.prepare_request(request).await?;
    if returns_rows {
        let handle = session
            .create_prepared_statement(
                sql.clone(),
                true,
                PreparedStatementOptions::new().ephemeral(),
            )
            .map_err(SwanFlightSqlService::status_from_error)?;
        let meta = session
            .get_prepared_statement_meta(handle)
            .map_err(SwanFlightSqlService::status_from_error)?;
        service
            .execute_prepared_query_handle(&session, handle, meta)
            .await
    } else {
        let sql_for_exec = sql.clone();
        let session_clone = Arc::clone(&session);
        let affected_rows =
            tokio::task::spawn_blocking(move || session_clone.execute_statement(&sql_for_exec))
                .await
                .map_err(SwanFlightSqlService::status_from_join)?
                .map_err(SwanFlightSqlService::status_from_error)?;
        info!(sql = %sql, affected_rows, "ephemeral command executed via DoGet");
        Ok(SwanFlightSqlService::empty_affected_rows_response(
            affected_rows,
        ))
    }
}
