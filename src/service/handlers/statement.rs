use std::sync::Arc;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::PeekableFlightDataStream;
use arrow_flight::sql::{
    CommandPreparedStatementQuery, CommandStatementQuery, CommandStatementUpdate, ProstMessageExt,
    TicketStatementQuery,
};
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info};

use super::ticket::{StatementTicketKind, TicketStatementPayload};
use crate::engine::connection::QueryResult;
use crate::service::SwanFlightSqlService;
use crate::session::id::StatementHandle;
use crate::session::PreparedStatementOptions;

pub(crate) async fn get_flight_info_statement(
    service: &SwanFlightSqlService,
    query: CommandStatementQuery,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    let sql = query.query;
    let session = service.prepare_request(&request)?;

    let sql_for_schema = sql.clone();
    let session_for_schema = Arc::clone(&session);
    let schema =
        tokio::task::spawn_blocking(move || session_for_schema.schema_for_query(&sql_for_schema))
            .await
            .map_err(SwanFlightSqlService::status_from_join)?
            .map_err(SwanFlightSqlService::status_from_error)?;

    debug!(field_count = schema.fields().len(), "planned schema");

    let handle = session
        .create_prepared_statement(
            sql.clone(),
            true,
            PreparedStatementOptions::new()
                .with_cached_schema(Some(schema.clone()))
                .ephemeral(),
        )
        .map_err(SwanFlightSqlService::status_from_error)?;

    let descriptor = request.into_inner();
    let ticket = TicketStatementQuery {
        statement_handle: TicketStatementPayload::new(handle, StatementTicketKind::Ephemeral)
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

pub(crate) async fn do_get_statement(
    service: &SwanFlightSqlService,
    ticket: TicketStatementQuery,
    request: Request<Ticket>,
) -> Result<Response<<SwanFlightSqlService as FlightService>::DoGetStream>, Status> {
    if let Ok(payload) = TicketStatementPayload::decode(ticket.statement_handle.as_ref()) {
        if payload.version == TicketStatementPayload::CURRENT_VERSION {
            if let Some(handle) = payload.handle() {
                let session = service.get_session(&request)?;
                let meta = session
                    .get_prepared_statement_meta(handle)
                    .map_err(SwanFlightSqlService::status_from_error)?;
                let ticket_kind = payload
                    .ticket_kind()
                    .unwrap_or(StatementTicketKind::Prepared);
                info!(
                    handle = %handle,
                    kind = ?ticket_kind,
                    sql = %meta.sql,
                    "executing statement via indexed ticket"
                );
                return service
                    .execute_prepared_query_handle(&session, handle, meta)
                    .await;
            } else {
                error!("statement handle payload missing handle bytes");
                return Err(Status::invalid_argument("invalid ticket payload"));
            }
        }
    }

    if let Ok(prepared_query) =
        CommandPreparedStatementQuery::decode(ticket.statement_handle.as_ref())
    {
        let handle_bytes = prepared_query.prepared_statement_handle.as_ref();
        if let Some(handle) = StatementHandle::from_bytes(handle_bytes) {
            let session = service.get_session(&request)?;

            if let Ok(meta) = session.get_prepared_statement_meta(handle) {
                if !meta.is_query {
                    error!(
                        handle = %handle,
                        "prepared statement does not return a result set"
                    );
                    return Err(Status::invalid_argument(
                        "prepared statement does not return a result set",
                    ));
                }

                info!(
                    handle = %handle,
                    sql = %meta.sql,
                    "executing prepared statement via do_get_statement"
                );

                return service
                    .execute_prepared_query_handle(&session, handle, meta)
                    .await;
            }
        } else {
            debug!(
                handle_len = handle_bytes.len(),
                "statement handle payload did not decode to prepared handle; falling back to direct execution"
            );
        }
    }

    let CommandStatementQuery { query: sql, .. } =
        CommandStatementQuery::decode(ticket.statement_handle.as_ref()).map_err(|err| {
            error!(%err, "failed to decode statement handle payload");
            Status::invalid_argument(format!("invalid statement handle: {err}"))
        })?;

    info!(sql = %sql, "executing query via do_get_statement");

    let session = service.get_session(&request)?;

    let QueryResult {
        schema,
        batches,
        total_rows,
        total_bytes,
    } = tokio::task::spawn_blocking(move || session.execute_query(&sql))
        .await
        .map_err(SwanFlightSqlService::status_from_join)?
        .map_err(SwanFlightSqlService::status_from_error)?;

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

    let stream = SwanFlightSqlService::into_stream(flight_data);
    let mut response = Response::new(stream);
    if let Ok(value) = MetadataValue::try_from(total_rows.to_string()) {
        response
            .metadata_mut()
            .insert("x-swanlake-total-rows", value);
    }
    if let Ok(value) = MetadataValue::try_from(total_bytes.to_string()) {
        response
            .metadata_mut()
            .insert("x-swanlake-total-bytes", value);
    }
    Ok(response)
}

pub(crate) async fn do_put_statement_update(
    service: &SwanFlightSqlService,
    command: CommandStatementUpdate,
    request: Request<PeekableFlightDataStream>,
) -> Result<i64, Status> {
    let sql = command.query;
    let session = service.prepare_request(&request)?;

    let affected_rows = tokio::task::spawn_blocking(move || session.execute_statement(&sql))
        .await
        .map_err(SwanFlightSqlService::status_from_join)?
        .map_err(SwanFlightSqlService::status_from_error)?;

    Ok(affected_rows)
}
