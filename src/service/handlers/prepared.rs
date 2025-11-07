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
use prost::Message;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info};

use crate::service::SwanFlightSqlService;
use crate::session::id::StatementHandle;
use crate::session::PreparedStatementMeta;

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

pub(crate) async fn do_action_create_prepared_statement(
    service: &SwanFlightSqlService,
    query: ActionCreatePreparedStatementRequest,
    request: Request<arrow_flight::Action>,
) -> Result<ActionCreatePreparedStatementResult, Status> {
    let sql = query.query;
    let is_query = SwanFlightSqlService::is_query_statement(&sql);
    tracing::Span::current().record("is_query", is_query);
    let session = service.prepare_request(&request)?;

    let dataset_schema = if is_query {
        let sql_for_schema = sql.clone();
        let session_clone = Arc::clone(&session);
        tokio::task::spawn_blocking(move || {
            let schema = session_clone.schema_for_query(&sql_for_schema)?;
            SwanFlightSqlService::schema_to_ipc_bytes(&schema)
        })
        .await
        .map_err(SwanFlightSqlService::status_from_join)?
        .map_err(SwanFlightSqlService::status_from_error)?
    } else {
        Vec::new()
    };

    let handle = session
        .create_prepared_statement(sql, is_query)
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

pub(crate) async fn get_flight_info_prepared_statement(
    service: &SwanFlightSqlService,
    query: CommandPreparedStatementQuery,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    let handle = parse_statement_handle(
        query.prepared_statement_handle.as_ref(),
        "get_flight_info_prepared_statement",
    )?;
    let session = service.prepare_request(&request)?;

    let PreparedStatementMeta { sql, is_query } = session
        .get_prepared_statement_meta(handle)
        .map_err(SwanFlightSqlService::status_from_error)?;

    tracing::Span::current().record("sql", sql.as_str());

    if !is_query {
        error!(
            handle = %handle,
            sql = %sql,
            "prepared statement does not return a result set"
        );
        return Err(Status::invalid_argument(
            "prepared statement does not return a result set",
        ));
    }

    info!(
        handle = %handle,
        sql = %sql,
        "getting flight info for prepared statement"
    );

    let session_clone = Arc::clone(&session);
    let schema = tokio::task::spawn_blocking(move || session_clone.schema_for_query(&sql))
        .await
        .map_err(SwanFlightSqlService::status_from_join)?
        .map_err(SwanFlightSqlService::status_from_error)?;

    debug!(
        handle = %handle,
        field_count = schema.fields().len(),
        "prepared statement schema retrieved"
    );

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
        .with_total_records(-1);

    Ok(Response::new(info))
}

pub(crate) async fn do_get_prepared_statement(
    service: &SwanFlightSqlService,
    query: CommandPreparedStatementQuery,
    request: Request<Ticket>,
) -> Result<Response<<SwanFlightSqlService as FlightService>::DoGetStream>, Status> {
    let handle = parse_statement_handle(
        query.prepared_statement_handle.as_ref(),
        "do_get_prepared_statement",
    )?;
    let session = service.prepare_request(&request)?;

    let meta = session
        .get_prepared_statement_meta(handle)
        .map_err(SwanFlightSqlService::status_from_error)?;

    tracing::Span::current().record("sql", meta.sql.as_str());

    if !meta.is_query {
        error!(
            handle = %handle,
            sql = %meta.sql,
            "prepared statement does not return a result set"
        );
        return Err(Status::invalid_argument(
            "prepared statement does not return a result set",
        ));
    }

    service
        .execute_prepared_query_handle(&session, handle, meta)
        .await
}

pub(crate) async fn do_put_prepared_statement_query(
    service: &SwanFlightSqlService,
    query: CommandPreparedStatementQuery,
    request: Request<PeekableFlightDataStream>,
) -> Result<DoPutPreparedStatementResult, Status> {
    let handle = parse_statement_handle(
        query.prepared_statement_handle.as_ref(),
        "do_put_prepared_statement_query",
    )?;
    let session = service.prepare_request(&request)?;

    let meta = session
        .get_prepared_statement_meta(handle)
        .map_err(SwanFlightSqlService::status_from_error)?;

    tracing::Span::current().record("sql", meta.sql.as_str());

    if !meta.is_query {
        error!(
            handle = %handle,
            sql = %meta.sql,
            "prepared statement does not support query binding"
        );
        return Err(Status::invalid_argument(
            "prepared statement does not support query binding",
        ));
    }

    let parameter_sets = SwanFlightSqlService::collect_parameter_sets(request).await?;

    if let Some(first_params) = parameter_sets.into_iter().next() {
        session
            .set_prepared_statement_parameters(handle, first_params)
            .map_err(SwanFlightSqlService::status_from_error)?;
    }

    info!(
        handle = %handle,
        "parameters bound to prepared statement"
    );

    Ok(DoPutPreparedStatementResult::default())
}

pub(crate) async fn do_action_close_prepared_statement(
    service: &SwanFlightSqlService,
    query: ActionClosePreparedStatementRequest,
    request: Request<arrow_flight::Action>,
) -> Result<(), Status> {
    let handle = parse_statement_handle(
        query.prepared_statement_handle.as_ref(),
        "do_action_close_prepared_statement",
    )?;
    let session = service.prepare_request(&request)?;

    session
        .close_prepared_statement(handle)
        .map_err(SwanFlightSqlService::status_from_error)?;
    info!(handle = %handle, "prepared statement closed");
    Ok(())
}

pub(crate) async fn do_put_prepared_statement_update(
    service: &SwanFlightSqlService,
    query: CommandPreparedStatementUpdate,
    request: Request<PeekableFlightDataStream>,
) -> Result<i64, Status> {
    let handle = parse_statement_handle(
        query.prepared_statement_handle.as_ref(),
        "do_put_prepared_statement_update",
    )?;
    let session = service.prepare_request(&request)?;

    let PreparedStatementMeta { sql, is_query } = session
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

    let parameter_sets = SwanFlightSqlService::collect_parameter_sets(request).await?;

    let parameter_set_count = parameter_sets.len();

    info!(
        handle = %handle,
        sql = %sql,
        parameter_sets = parameter_set_count,
        "executing prepared statement update"
    );

    let session_clone = Arc::clone(&session);

    let affected_rows = tokio::task::spawn_blocking(move || {
        SwanFlightSqlService::execute_statement_batches(&sql, &parameter_sets, &session_clone)
    })
    .await
    .map_err(SwanFlightSqlService::status_from_join)?
    .map_err(SwanFlightSqlService::status_from_error)?;

    info!(
        handle = %handle,
        affected_rows,
        "prepared statement update complete"
    );
    Ok(affected_rows)
}
