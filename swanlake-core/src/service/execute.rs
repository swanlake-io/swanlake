use std::sync::Arc;
use std::time::Instant;

use arrow_flight::flight_service_server::FlightService;
use duckdb::types::Value;
use tonic::{metadata::MetadataValue, Response, Status};
use tracing::{debug, error, info, warn};

use crate::engine::connection::QueryResult;
use crate::session::id::StatementHandle;
use crate::session::{PreparedStatementMeta, Session};

use super::SwanFlightSqlService;

impl SwanFlightSqlService {
    pub(crate) fn execute_statement_batches(
        sql: &str,
        param_batches: &[Vec<Value>],
        session: &Session,
    ) -> Result<i64, crate::error::ServerError> {
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

    pub(crate) async fn execute_prepared_query_handle(
        &self,
        session: &Arc<Session>,
        handle: StatementHandle,
        meta: PreparedStatementMeta,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let parameters = session
            .take_prepared_statement_parameters(handle)
            .map_err(Self::status_from_error)?
            .unwrap_or_else(Vec::new);

        self.execute_prepared_query_with_params(session, handle, meta, parameters)
            .await
    }

    pub(crate) async fn execute_prepared_update_handle(
        &self,
        session: &Arc<Session>,
        handle: StatementHandle,
        meta: PreparedStatementMeta,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let _in_flight = self.metrics.start_update();
        let start = Instant::now();
        let PreparedStatementMeta { sql, ephemeral, .. } = meta;

        let parameters = session
            .take_prepared_statement_parameters(handle)
            .map_err(Self::status_from_error)?
            .unwrap_or_else(Vec::new);

        if ephemeral {
            if let Err(err) = session.close_prepared_statement(handle) {
                warn!(
                    handle = %handle,
                    %err,
                    "failed to close ephemeral prepared statement"
                );
            }
        }

        let param_count = parameters.len();
        info!(
            handle = %handle,
            sql = %sql,
            param_count,
            "executing prepared statement update via handle"
        );

        let session_clone = session.clone();
        let params_for_exec = parameters;
        let sql_for_exec = sql.clone();

        let result = tokio::task::spawn_blocking(move || {
            if params_for_exec.is_empty() {
                Self::execute_statement_batches(&sql_for_exec, &[], &session_clone)
            } else {
                Self::execute_statement_batches(
                    &sql_for_exec,
                    std::slice::from_ref(&params_for_exec),
                    &session_clone,
                )
            }
        })
        .await;

        let affected_rows = match result {
            Ok(Ok(rows)) => rows,
            Ok(Err(err)) => {
                self.metrics
                    .record_update_error(&sql, start.elapsed(), err.to_string());
                return Err(Self::status_from_error(err));
            }
            Err(err) => {
                self.metrics
                    .record_update_error(&sql, start.elapsed(), err.to_string());
                return Err(Self::status_from_join(err));
            }
        };

        self.metrics
            .record_update_success(&sql, start.elapsed(), Some(affected_rows));

        info!(
            handle = %handle,
            affected_rows,
            "prepared statement update completed"
        );

        Ok(Self::empty_affected_rows_response(affected_rows))
    }

    pub(crate) fn empty_affected_rows_response(
        affected_rows: i64,
    ) -> Response<<Self as FlightService>::DoGetStream> {
        // Emit an empty schema message so Flight clients don't hit EOF/Unknown.
        let empty_schema = arrow_schema::Schema::empty();
        let flight_data =
            arrow_flight::utils::batches_to_flight_data(&empty_schema, vec![]).unwrap_or_default();
        let stream = Self::into_stream(flight_data);
        let mut response = Response::new(stream);
        if let Ok(value) = MetadataValue::try_from(affected_rows.to_string()) {
            response
                .metadata_mut()
                .insert("x-swanlake-affected-rows", value);
        }
        response
    }

    async fn execute_prepared_query_with_params(
        &self,
        session: &Arc<Session>,
        handle: StatementHandle,
        meta: PreparedStatementMeta,
        parameters: Vec<Value>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let _in_flight = self.metrics.start_query();
        let start = Instant::now();
        let PreparedStatementMeta { sql, ephemeral, .. } = meta;

        if ephemeral {
            if let Err(err) = session.close_prepared_statement(handle) {
                warn!(
                    handle = %handle,
                    %err,
                    "failed to close ephemeral prepared statement"
                );
            }
        }

        let param_count = parameters.len();
        info!(
            handle = %handle,
            sql = %sql,
            param_count,
            "executing prepared statement via handle"
        );

        let session_clone = session.clone();
        let params_for_exec = parameters;
        let sql_for_exec = sql.clone();

        let result = tokio::task::spawn_blocking(move || {
            if params_for_exec.is_empty() {
                session_clone.execute_query(&sql_for_exec)
            } else {
                session_clone.execute_query_with_params(&sql_for_exec, &params_for_exec)
            }
        })
        .await;

        let QueryResult {
            schema,
            batches,
            total_rows,
            total_bytes,
        } = match result {
            Ok(Ok(query_result)) => query_result,
            Ok(Err(err)) => {
                self.metrics
                    .record_query_error(&sql, start.elapsed(), err.to_string());
                return Err(Self::status_from_error(err));
            }
            Err(err) => {
                self.metrics
                    .record_query_error(&sql, start.elapsed(), err.to_string());
                return Err(Self::status_from_join(err));
            }
        };

        let flight_data =
            arrow_flight::utils::batches_to_flight_data(&schema, batches).map_err(|err| {
                error!(%err, "failed to convert record batches to flight data");
                self.metrics
                    .record_query_error(&sql, start.elapsed(), err.to_string());
                Status::internal(format!(
                    "failed to convert record batches to flight data: {err}"
                ))
            })?;

        self.metrics
            .record_query_success(&sql, start.elapsed(), total_rows, total_bytes);

        debug!(
            handle = %handle,
            batch_count = flight_data.len(),
            "converted batches to flight data"
        );

        let stream = Self::into_stream(flight_data);
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
        info!(
            handle = %handle,
            total_rows, total_bytes, "prepared statement completed"
        );
        Ok(response)
    }
}
