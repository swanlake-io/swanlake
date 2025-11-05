use std::sync::Arc;

use arrow_flight::flight_service_server::FlightService;
use duckdb::types::Value;
use tonic::{metadata::MetadataValue, Response, Status};
use tracing::{debug, error, info};

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

        info!(
            handle = %handle,
            sql = %meta.sql,
            param_count = parameters.len(),
            "executing prepared statement via handle"
        );

        let sql_for_exec = meta.sql.clone();
        let params_for_exec = parameters.clone();
        let session_clone = session.clone();

        let QueryResult {
            schema,
            batches,
            total_rows,
            total_bytes,
        } = tokio::task::spawn_blocking(move || {
            if params_for_exec.is_empty() {
                session_clone.execute_query(&sql_for_exec)
            } else {
                session_clone.execute_query_with_params(&sql_for_exec, &params_for_exec)
            }
        })
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
