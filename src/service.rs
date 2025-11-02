use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery};
use arrow_flight::{
    FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
    flight_service_server::FlightService,
};
use futures::Stream;
use futures::stream;
use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status};
use tracing::instrument;

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
            ServerError::DuckDb(e) => Status::internal(format!("duckdb error: {e}")),
            ServerError::Arrow(e) => Status::internal(format!("arrow error: {e}")),
            ServerError::Pool(e) => Status::internal(format!("connection pool error: {e}")),
        }
    }

    fn status_from_join(err: tokio::task::JoinError) -> Status {
        if err.is_panic() {
            Status::internal("blocking task panicked")
        } else {
            Status::internal(format!("blocking task cancelled: {err}"))
        }
    }

    fn into_stream(
        batches: Vec<FlightData>,
    ) -> Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>> {
        Box::pin(stream::iter(batches.into_iter().map(Ok)))
    }
}

#[tonic::async_trait]
impl FlightSqlService for SwanFlightSqlService {
    type FlightService = SwanFlightSqlService;

    #[instrument(skip_all, fields(sql = %query.query))]
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = query.query.clone();
        let engine = self.engine.clone();

        let schema = tokio::task::spawn_blocking(move || engine.schema_for_query(&sql))
            .await
            .map_err(Self::status_from_join)?
            .map_err(Self::status_from_error)?;

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

    #[instrument(skip_all, fields(handle_len = ticket.statement_handle.len()))]
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let command = CommandStatementQuery::decode(ticket.statement_handle.as_ref())
            .map_err(|err| Status::invalid_argument(format!("invalid statement handle: {err}")))?;
        let sql = command.query.clone();
        let engine = self.engine.clone();

        let QueryResult {
            schema,
            batches,
            total_rows,
            total_bytes,
        } = tokio::task::spawn_blocking(move || engine.execute_query(&sql))
            .await
            .map_err(Self::status_from_join)?
            .map_err(Self::status_from_error)?;

        let flight_data =
            arrow_flight::utils::batches_to_flight_data(&schema, batches).map_err(|err| {
                Status::internal(format!(
                    "failed to convert record batches to flight data: {err}"
                ))
            })?;

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
        Ok(response)
    }

    async fn register_sql_info(&self, id: i32, info: &SqlInfo) {
        tracing::debug!(id, ?info, "register_sql_info invoked");
    }
}
