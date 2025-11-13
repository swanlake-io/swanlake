use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, IntervalDayTimeArray,
    IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeBinaryArray, LargeStringArray,
    RecordBatch, StringArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::sql::server::PeekableFlightDataStream;
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use duckdb::types::{TimeUnit as DuckTimeUnit, Value};
use futures::{StreamExt, TryStreamExt};
use std::any::type_name;
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

    fn push_column_values(array: &ArrayRef, rows: &mut [Vec<Value>]) -> Result<(), ServerError> {
        macro_rules! push_values {
            ($array:expr, $rows:expr, $arr_type:ty, $variant:path) => {{
                let values = Self::downcast_array::<$arr_type>($array)?;
                Self::push_array_values(values, $rows, |arr, idx| $variant(arr.value(idx)));
            }};
            ($array:expr, $rows:expr, $arr_type:ty, $variant:path, $conv:ident) => {{
                let values = Self::downcast_array::<$arr_type>($array)?;
                Self::push_array_values(values, $rows, |arr, idx| $variant(arr.value(idx).$conv()));
            }};
        }
        macro_rules! push_timestamp_values {
            ($array:expr, $rows:expr, $arr_type:ty, $duck_unit:expr) => {{
                let values = Self::downcast_array::<$arr_type>($array)?;
                Self::push_array_values(values, $rows, |arr, idx| {
                    Value::Timestamp($duck_unit, arr.value(idx))
                });
            }};
        }
        macro_rules! push_interval_values {
            ($array:expr, $rows:expr, $arr_type:ty, $binding:ident, $make_value:expr) => {{
                let values = Self::downcast_array::<$arr_type>($array)?;
                Self::push_array_values(values, $rows, |arr, idx| {
                    let $binding = arr.value(idx);
                    $make_value
                });
            }};
        }

        match array.data_type() {
            DataType::Null => {
                for row in rows.iter_mut() {
                    row.push(Value::Null);
                }
            }
            DataType::Boolean => push_values!(array, rows, BooleanArray, Value::Boolean),
            DataType::Int8 => push_values!(array, rows, Int8Array, Value::TinyInt),
            DataType::Int16 => push_values!(array, rows, Int16Array, Value::SmallInt),
            DataType::Int32 => push_values!(array, rows, Int32Array, Value::Int),
            DataType::Int64 => push_values!(array, rows, Int64Array, Value::BigInt),
            DataType::UInt8 => push_values!(array, rows, UInt8Array, Value::UTinyInt),
            DataType::UInt16 => push_values!(array, rows, UInt16Array, Value::USmallInt),
            DataType::UInt32 => push_values!(array, rows, UInt32Array, Value::UInt),
            DataType::UInt64 => push_values!(array, rows, UInt64Array, Value::UBigInt),
            DataType::Float32 => push_values!(array, rows, Float32Array, Value::Float),
            DataType::Float64 => push_values!(array, rows, Float64Array, Value::Double),
            DataType::Utf8 => push_values!(array, rows, StringArray, Value::Text, to_string),
            DataType::LargeUtf8 => {
                push_values!(array, rows, LargeStringArray, Value::Text, to_string)
            }
            DataType::Binary => push_values!(array, rows, BinaryArray, Value::Blob, to_vec),
            DataType::LargeBinary => {
                push_values!(array, rows, LargeBinaryArray, Value::Blob, to_vec)
            }
            DataType::Date32 => {
                let values = Self::downcast_array::<Date32Array>(array)?;
                Self::push_array_values(values, rows, |arr, idx| {
                    Value::Timestamp(DuckTimeUnit::Second, (arr.value(idx) as i64) * 86400)
                });
            }
            DataType::Date64 => {
                let values = Self::downcast_array::<Date64Array>(array)?;
                Self::push_array_values(values, rows, |arr, idx| {
                    Value::Timestamp(DuckTimeUnit::Millisecond, arr.value(idx))
                });
            }
            DataType::Time32(unit) => match unit {
                TimeUnit::Second => {
                    let values = Self::downcast_array::<Time32SecondArray>(array)?;
                    Self::push_array_values(values, rows, |arr, idx| {
                        Value::Timestamp(DuckTimeUnit::Second, arr.value(idx) as i64)
                    });
                }
                TimeUnit::Millisecond => {
                    let values = Self::downcast_array::<Time32MillisecondArray>(array)?;
                    Self::push_array_values(values, rows, |arr, idx| {
                        Value::Timestamp(DuckTimeUnit::Millisecond, arr.value(idx) as i64)
                    });
                }
                _ => {
                    return Err(ServerError::UnsupportedParameter(format!(
                        "Time32 with unit {:?}",
                        unit
                    )))
                }
            },
            DataType::Time64(unit) => match unit {
                TimeUnit::Microsecond => {
                    let values = Self::downcast_array::<Time64MicrosecondArray>(array)?;
                    Self::push_array_values(values, rows, |arr, idx| {
                        Value::Timestamp(DuckTimeUnit::Microsecond, arr.value(idx))
                    });
                }
                TimeUnit::Nanosecond => {
                    let values = Self::downcast_array::<Time64NanosecondArray>(array)?;
                    Self::push_array_values(values, rows, |arr, idx| {
                        Value::Timestamp(DuckTimeUnit::Nanosecond, arr.value(idx))
                    });
                }
                _ => {
                    return Err(ServerError::UnsupportedParameter(format!(
                        "Time64 with unit {:?}",
                        unit
                    )))
                }
            },
            DataType::Interval(unit) => match unit {
                IntervalUnit::YearMonth => push_interval_values!(
                    array,
                    rows,
                    IntervalYearMonthArray,
                    months,
                    Value::Interval {
                        months,
                        days: 0,
                        nanos: 0,
                    }
                ),
                IntervalUnit::DayTime => push_interval_values!(
                    array,
                    rows,
                    IntervalDayTimeArray,
                    dt,
                    Value::Interval {
                        months: 0,
                        days: dt.days,
                        nanos: i64::from(dt.milliseconds) * 1_000_000,
                    }
                ),
                IntervalUnit::MonthDayNano => push_interval_values!(
                    array,
                    rows,
                    IntervalMonthDayNanoArray,
                    mdn,
                    Value::Interval {
                        months: mdn.months,
                        days: mdn.days,
                        nanos: mdn.nanoseconds,
                    }
                ),
            },
            DataType::Timestamp(unit, _tz) => {
                let duck_unit = match unit {
                    TimeUnit::Second => DuckTimeUnit::Second,
                    TimeUnit::Millisecond => DuckTimeUnit::Millisecond,
                    TimeUnit::Microsecond => DuckTimeUnit::Microsecond,
                    TimeUnit::Nanosecond => DuckTimeUnit::Nanosecond,
                };
                match unit {
                    TimeUnit::Second => {
                        push_timestamp_values!(array, rows, TimestampSecondArray, duck_unit)
                    }
                    TimeUnit::Millisecond => {
                        push_timestamp_values!(array, rows, TimestampMillisecondArray, duck_unit)
                    }
                    TimeUnit::Microsecond => {
                        push_timestamp_values!(array, rows, TimestampMicrosecondArray, duck_unit)
                    }
                    TimeUnit::Nanosecond => {
                        push_timestamp_values!(array, rows, TimestampNanosecondArray, duck_unit)
                    }
                }
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

    fn downcast_array<T: 'static>(array: &ArrayRef) -> Result<&T, ServerError> {
        array.as_any().downcast_ref::<T>().ok_or_else(|| {
            ServerError::Internal(format!(
                "expected {} but found {}",
                type_name::<T>(),
                array.data_type()
            ))
        })
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
