use arrow_array::{ArrayRef, RecordBatch};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::sql::server::PeekableFlightDataStream;
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};

use duckdb::types::Value;
use futures::{StreamExt, TryStreamExt};
use tonic::{Request, Status};

use crate::error::ServerError;
use crate::types::arrow_array_to_duckdb_values;

use super::SwanFlightSqlService;

impl SwanFlightSqlService {
    /// Collect RecordBatches directly from the stream without conversion.
    ///
    /// This is used for optimized INSERT operations where we can pass
    /// RecordBatch directly to the DuckDB appender API.
    pub(crate) async fn collect_record_batches(
        request: Request<PeekableFlightDataStream>,
    ) -> Result<Vec<RecordBatch>, Status> {
        let stream = request.into_inner();
        let mapped =
            stream.map_err(|status| arrow_flight::error::FlightError::Tonic(Box::new(status)));
        let mut record_stream = FlightRecordBatchStream::new_from_flight_data(mapped);

        let mut batches = Vec::new();
        while let Some(batch) = record_stream.next().await {
            let batch = batch.map_err(Self::status_from_flight_error)?;
            batches.push(batch);
        }

        Ok(batches)
    }

    pub(crate) async fn collect_parameter_sets(
        request: Request<PeekableFlightDataStream>,
    ) -> Result<Vec<Vec<Value>>, Status> {
        let stream = request.into_inner();
        let mapped =
            stream.map_err(|status| arrow_flight::error::FlightError::Tonic(Box::new(status)));
        let mut record_stream = FlightRecordBatchStream::new_from_flight_data(mapped);

        let mut params = Vec::new();
        while let Some(batch) = record_stream.next().await {
            let batch = batch.map_err(Self::status_from_flight_error)?;
            let mut rows = Self::record_batch_to_params(&batch).map_err(Self::status_from_error)?;

            if params.is_empty() {
                params = rows;
            } else {
                params.reserve(rows.len());
                params.append(&mut rows);
            }
        }

        if params.is_empty() {
            params.push(Vec::new());
        }

        Ok(params)
    }

    pub(crate) fn record_batch_to_params(
        batch: &RecordBatch,
    ) -> Result<Vec<Vec<Value>>, ServerError> {
        let row_count = batch.num_rows();
        let column_count = batch.num_columns();
        let mut rows = vec![Vec::with_capacity(column_count); row_count];

        for col_idx in 0..column_count {
            let column = batch.column(col_idx);
            Self::push_column_values(column, &mut rows)?;
        }

        Ok(rows)
    }

    fn push_column_values(array: &ArrayRef, rows: &mut [Vec<Value>]) -> Result<(), ServerError> {
        let values = arrow_array_to_duckdb_values(array)?;
        for (row_idx, row) in rows.iter_mut().enumerate() {
            row.push(values[row_idx].clone());
        }
        Ok(())
    }

    pub(crate) fn schema_to_ipc_bytes(
        schema: &arrow_schema::Schema,
    ) -> Result<Vec<u8>, ServerError> {
        let data_gen = IpcDataGenerator::default();
        let mut dict_tracker = DictionaryTracker::new(false);
        let write_options = IpcWriteOptions::default();
        let encoded = data_gen.schema_to_bytes_with_dictionary_tracker(
            schema,
            &mut dict_tracker,
            &write_options,
        );
        let mut buffer = vec![];
        arrow_ipc::writer::write_message(&mut buffer, encoded, &write_options)
            .map_err(ServerError::Arrow)?;
        Ok(buffer)
    }
}
