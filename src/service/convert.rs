use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, RecordBatch, StringArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::sql::server::PeekableFlightDataStream;
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow_schema::DataType;
use duckdb::types::Value;
use futures::{StreamExt, TryStreamExt};
use tonic::{Request, Status};

use crate::error::ServerError;

use super::SwanFlightSqlService;

impl SwanFlightSqlService {
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

    fn record_batch_to_params(batch: &RecordBatch) -> Result<Vec<Vec<Value>>, ServerError> {
        let row_count = batch.num_rows();
        let column_count = batch.num_columns();
        let mut rows = vec![Vec::with_capacity(column_count); row_count];

        for col_idx in 0..column_count {
            let column = batch.column(col_idx);
            Self::push_column_values(column, &mut rows)?;
        }

        Ok(rows)
    }

    fn push_column_values(
        array: &ArrayRef,
        rows: &mut [Vec<Value>],
    ) -> Result<(), ServerError> {
        match array.data_type() {
            DataType::Null => {
                for row in rows.iter_mut() {
                    row.push(Value::Null);
                }
            }
            DataType::Boolean => {
                let values = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("boolean array downcast");
                Self::push_array_values(values, rows, |arr, idx| Value::Boolean(arr.value(idx)));
            }
            DataType::Int8 => {
                let values = array
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .expect("int8 array downcast");
                Self::push_array_values(values, rows, |arr, idx| Value::TinyInt(arr.value(idx)));
            }
            DataType::Int16 => {
                let values = array
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .expect("int16 array downcast");
                Self::push_array_values(values, rows, |arr, idx| Value::SmallInt(arr.value(idx)));
            }
            DataType::Int32 => {
                let values = array
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("int32 array downcast");
                Self::push_array_values(values, rows, |arr, idx| Value::Int(arr.value(idx)));
            }
            DataType::Int64 => {
                let values = array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("int64 array downcast");
                Self::push_array_values(values, rows, |arr, idx| Value::BigInt(arr.value(idx)));
            }
            DataType::UInt8 => {
                let values = array
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .expect("uint8 array downcast");
                Self::push_array_values(values, rows, |arr, idx| Value::UTinyInt(arr.value(idx)));
            }
            DataType::UInt16 => {
                let values = array
                    .as_any()
                    .downcast_ref::<UInt16Array>()
                    .expect("uint16 array downcast");
                Self::push_array_values(values, rows, |arr, idx| Value::USmallInt(arr.value(idx)));
            }
            DataType::UInt32 => {
                let values = array
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .expect("uint32 array downcast");
                Self::push_array_values(values, rows, |arr, idx| Value::UInt(arr.value(idx)));
            }
            DataType::UInt64 => {
                let values = array
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("uint64 array downcast");
                Self::push_array_values(values, rows, |arr, idx| Value::UBigInt(arr.value(idx)));
            }
            DataType::Float32 => {
                let values = array
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .expect("float32 array downcast");
                Self::push_array_values(values, rows, |arr, idx| Value::Float(arr.value(idx)));
            }
            DataType::Float64 => {
                let values = array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .expect("float64 array downcast");
                Self::push_array_values(values, rows, |arr, idx| Value::Double(arr.value(idx)));
            }
            DataType::Utf8 => {
                let values = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("string array downcast");
                Self::push_array_values(values, rows, |arr, idx| {
                    Value::Text(arr.value(idx).to_string())
                });
            }
            DataType::LargeUtf8 => {
                let values = array
                    .as_any()
                    .downcast_ref::<LargeStringArray>()
                    .expect("large string array downcast");
                Self::push_array_values(values, rows, |arr, idx| {
                    Value::Text(arr.value(idx).to_string())
                });
            }
            DataType::Binary => {
                let values = array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .expect("binary array downcast");
                Self::push_array_values(values, rows, |arr, idx| {
                    Value::Blob(arr.value(idx).to_vec())
                });
            }
            DataType::LargeBinary => {
                let values = array
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .expect("large binary array downcast");
                Self::push_array_values(values, rows, |arr, idx| {
                    Value::Blob(arr.value(idx).to_vec())
                });
            }
            other => return Err(ServerError::UnsupportedParameter(other.to_string())),
        }

        Ok(())
    }

    fn push_array_values<T, F>(array: &T, rows: &mut [Vec<Value>], mut value_fn: F)
    where
        T: Array,
        F: FnMut(&T, usize) -> Value,
    {
        for (row_idx, row) in rows.iter_mut().enumerate() {
            if array.is_null(row_idx) {
                row.push(Value::Null);
            } else {
                row.push(value_fn(array, row_idx));
            }
        }
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
