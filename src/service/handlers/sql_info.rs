use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::metadata::SqlInfoDataBuilder;
use arrow_flight::sql::{
    CommandGetSqlInfo, ProstMessageExt, SqlInfo, SqlSupportedTransaction,
    SqlTransactionIsolationLevel,
};
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use prost::Message;
use tonic::{Request, Response, Status};
use tracing::error;

use crate::service::SwanFlightSqlService;

/// Static registration hook for SqlInfo; currently unused because values are
/// compiled in.
pub(crate) async fn register_sql_info(_id: i32, _result: &SqlInfo) {
    // No-op: we don't need to register info dynamically
}

/// Builds `FlightInfo` for the SqlInfo metadata endpoint so clients can fetch
/// supported capabilities via DoGet.
pub(crate) async fn get_flight_info_sql_info(
    service: &SwanFlightSqlService,
    query: CommandGetSqlInfo,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    service.prepare_request(&request).await?;

    let mut builder = SqlInfoDataBuilder::new();
    builder.append(
        SqlInfo::FlightSqlServerTransaction,
        SqlSupportedTransaction::Transaction as i32,
    );
    builder.append(SqlInfo::SqlTransactionsSupported, true);
    builder.append(
        SqlInfo::SqlDefaultTransactionIsolation,
        SqlTransactionIsolationLevel::SqlTransactionSerializable as i32,
    );
    builder.append(SqlInfo::SqlSupportedTransactionsIsolationLevels, 0b11110);

    let info_data = builder
        .build()
        .map_err(|e| Status::internal(format!("Failed to build SqlInfo data: {}", e)))?;

    let schema = info_data.schema();

    let ticket_bytes = query.as_any().encode_to_vec();
    let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(ticket_bytes));

    let descriptor = request.into_inner();
    let info = FlightInfo::new()
        .try_with_schema(schema.as_ref())
        .map_err(|err| Status::internal(format!("failed to encode schema: {err}")))?
        .with_descriptor(descriptor)
        .with_endpoint(endpoint)
        .with_total_records(-1);

    Ok(Response::new(info))
}

/// Streams SqlInfo metadata as record batches in response to a DoGet request.
pub(crate) async fn do_get_sql_info(
    service: &SwanFlightSqlService,
    query: CommandGetSqlInfo,
    request: Request<Ticket>,
) -> Result<Response<<SwanFlightSqlService as FlightService>::DoGetStream>, Status> {
    service.prepare_request(&request).await?;

    let mut builder = SqlInfoDataBuilder::new();
    builder.append(
        SqlInfo::FlightSqlServerTransaction,
        SqlSupportedTransaction::Transaction as i32,
    );
    builder.append(SqlInfo::SqlTransactionsSupported, true);
    builder.append(
        SqlInfo::SqlDefaultTransactionIsolation,
        SqlTransactionIsolationLevel::SqlTransactionSerializable as i32,
    );
    builder.append(SqlInfo::SqlSupportedTransactionsIsolationLevels, 0b11110);

    let info_data = builder
        .build()
        .map_err(|e| Status::internal(format!("Failed to build SqlInfo data: {}", e)))?;

    let batch = query
        .into_builder(&info_data)
        .build()
        .map_err(|e| Status::internal(format!("Failed to build SqlInfo response: {}", e)))?;

    let schema = batch.schema();

    let flight_data =
        arrow_flight::utils::batches_to_flight_data(&schema, vec![batch]).map_err(|err| {
            error!(%err, "failed to convert SqlInfo batch to flight data");
            Status::internal(format!(
                "failed to convert SqlInfo batch to flight data: {err}"
            ))
        })?;

    let stream = SwanFlightSqlService::into_stream(flight_data);
    Ok(Response::new(stream))
}
