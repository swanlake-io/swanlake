use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::DoPutPreparedStatementResult;
use arrow_flight::sql::{
    ActionBeginTransactionRequest, ActionBeginTransactionResult,
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, ActionEndTransactionRequest, CommandGetSqlInfo,
    CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementQuery,
    CommandStatementUpdate, SqlInfo, TicketStatementQuery,
};
use arrow_flight::{FlightDescriptor, FlightInfo, Ticket};
use tonic::{Request, Response, Status};
use tracing::instrument;

use super::SwanFlightSqlService;

mod prepared;
mod sql_info;
mod statement;
mod ticket;
mod transaction;

#[tonic::async_trait]
impl FlightSqlService for SwanFlightSqlService {
    type FlightService = SwanFlightSqlService;

    #[instrument(skip(self, request, query), fields(session_id, sql = %query.query))]
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        statement::get_flight_info_statement(self, query, request).await
    }

    #[instrument(skip(self, request, ticket), fields(session_id))]
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        statement::do_get_statement(self, ticket, request).await
    }

    async fn register_sql_info(&self, id: i32, result: &SqlInfo) {
        sql_info::register_sql_info(id, result).await
    }

    #[instrument(skip(self, request), fields(session_id))]
    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        sql_info::get_flight_info_sql_info(self, query, request).await
    }

    #[instrument(skip(self, request), fields(session_id))]
    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        sql_info::do_get_sql_info(self, query, request).await
    }

    #[instrument(skip(self, request, command), fields(session_id, sql = %command.query))]
    async fn do_put_statement_update(
        &self,
        command: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        statement::do_put_statement_update(self, command, request).await
    }

    #[instrument(skip(self, request, query), fields(session_id, sql = %query.query, is_query))]
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<arrow_flight::Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        prepared::do_action_create_prepared_statement(self, query, request).await
    }

    #[instrument(skip(self, request, query), fields(session_id, sql))]
    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        prepared::get_flight_info_prepared_statement(self, query, request).await
    }

    #[instrument(skip(self, request, query), fields(session_id, sql))]
    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        prepared::do_get_prepared_statement(self, query, request).await
    }

    #[instrument(skip(self, request, query), fields(session_id, sql))]
    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        prepared::do_put_prepared_statement_query(self, query, request).await
    }

    #[instrument(skip(self, request, query), fields(session_id))]
    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        request: Request<arrow_flight::Action>,
    ) -> Result<(), Status> {
        prepared::do_action_close_prepared_statement(self, query, request).await
    }

    #[instrument(skip(self, request, query), fields(session_id, sql))]
    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        prepared::do_put_prepared_statement_update(self, query, request).await
    }

    #[instrument(skip(self, request, query), fields(session_id))]
    async fn do_action_begin_transaction(
        &self,
        query: ActionBeginTransactionRequest,
        request: Request<arrow_flight::Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        transaction::do_action_begin_transaction(self, query, request).await
    }

    #[instrument(skip(self, request, query), fields(session_id))]
    async fn do_action_end_transaction(
        &self,
        query: ActionEndTransactionRequest,
        request: Request<arrow_flight::Action>,
    ) -> Result<(), Status> {
        transaction::do_action_end_transaction(self, query, request).await
    }
}
