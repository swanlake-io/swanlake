//! Type conversion utilities between DuckDB and Arrow types.
//!
//! This module centralizes logic for converting between DuckDB types and Arrow types,
//! ensuring consistency and reusability across the codebase.

use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, IntervalDayTimeArray,
    IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeBinaryArray, LargeStringArray,
    StringArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use duckdb::types::{TimeUnit as DuckTimeUnit, Value};
use std::any::type_name;

use crate::error::ServerError;

/// Convert a DuckDB type string to an Arrow DataType.
pub fn duckdb_type_to_arrow(duckdb_type: &str) -> Result<DataType, ServerError> {
    let upper = duckdb_type.trim().to_uppercase();
    match upper.as_str() {
        // Signed integers
        "BIGINT" | "INT8" | "LONG" => Ok(DataType::Int64),
        "INTEGER" | "INT" | "INT4" | "SIGNED" => Ok(DataType::Int32),
        "SMALLINT" | "INT2" | "SHORT" => Ok(DataType::Int16),
        "TINYINT" | "INT1" => Ok(DataType::Int8),
        // Unsigned integers
        "UBIGINT" => Ok(DataType::UInt64),
        "UINTEGER" => Ok(DataType::UInt32),
        "USMALLINT" => Ok(DataType::UInt16),
        "UTINYINT" => Ok(DataType::UInt8),
        // Strings
        "VARCHAR" | "CHAR" | "BPCHAR" | "TEXT" | "STRING" => Ok(DataType::Utf8),
        // Booleans
        "BOOLEAN" | "BOOL" | "LOGICAL" => Ok(DataType::Boolean),
        // Floats
        "DOUBLE" | "DOUBLE PRECISION" | "FLOAT8" => Ok(DataType::Float64),
        "FLOAT" | "FLOAT4" | "REAL" => Ok(DataType::Float32),
        // Dates and times
        "DATE" => Ok(DataType::Date32),
        "TIME" => Ok(DataType::Time64(arrow_schema::TimeUnit::Microsecond)),
        "TIMESTAMP" | "DATETIME" => Ok(DataType::Timestamp(
            arrow_schema::TimeUnit::Microsecond,
            None,
        )),
        "TIMESTAMP WITH TIME ZONE" | "TIMESTAMPTZ" => Ok(DataType::Timestamp(
            arrow_schema::TimeUnit::Microsecond,
            Some("UTC".into()),
        )),
        // Binary
        "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" => Ok(DataType::Binary),
        // UUID
        "UUID" => Ok(DataType::FixedSizeBinary(16)),
        // JSON (treat as string)
        "JSON" => Ok(DataType::Utf8),
        // Bit string types
        "BIT" | "BITSTRING" => Ok(DataType::Binary),
        // Interval
        s if s.starts_with("INTERVAL") => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),
        // Decimal / Numeric
        s if s.starts_with("DECIMAL") || s.starts_with("NUMERIC") => {
            let (precision_raw, scale_raw) = parse_decimal_precision_scale(duckdb_type);
            let precision = precision_raw.min(76);
            let scale = scale_raw.min(precision);
            let arrow_precision = precision as u8;
            let arrow_scale = scale as i8;
            if precision <= 38 {
                Ok(DataType::Decimal128(arrow_precision, arrow_scale))
            } else {
                Ok(DataType::Decimal256(arrow_precision, arrow_scale))
            }
        }
        // Bignum/Hugeint (approximate with Decimal)
        "BIGNUM" | "HUGEINT" => Ok(DataType::Decimal128(38, 0)),
        "UHUGEINT" => Ok(DataType::Decimal128(38, 0)),
        _ => Err(ServerError::Internal(format!(
            "unsupported DuckDB type: {}",
            duckdb_type
        ))),
    }
}

/// Parse precision and scale from a DECIMAL or NUMERIC type string.
fn parse_decimal_precision_scale(spec: &str) -> (usize, usize) {
    if let Some(start) = spec.find('(') {
        let end = spec[start + 1..]
            .find(')')
            .map(|idx| start + 1 + idx)
            .unwrap_or(spec.len());
        let inner = spec[start + 1..end].trim();
        if inner.is_empty() {
            return (18, 3);
        }
        let mut parts = inner
            .split(',')
            .map(|part| part.trim())
            .filter(|s| !s.is_empty());
        let precision = parts
            .next()
            .and_then(|p| p.parse::<usize>().ok())
            .unwrap_or(18);
        let scale = parts
            .next()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(0);
        (precision, scale)
    } else {
        (18, 3)
    }
}

/// Convert an Arrow ArrayRef to a vector of DuckDB Values.
pub fn arrow_array_to_duckdb_values(array: &ArrayRef) -> Result<Vec<Value>, ServerError> {
    let mut values = Vec::with_capacity(array.len());

    macro_rules! push_values {
        ($array:expr, $values:expr, $arr_type:ty, $variant:path) => {{
            let arr = downcast_array::<$arr_type>($array)?;
            for idx in 0..arr.len() {
                if arr.is_null(idx) {
                    $values.push(Value::Null);
                } else {
                    $values.push($variant(arr.value(idx)));
                }
            }
        }};
        ($array:expr, $values:expr, $arr_type:ty, $variant:path, $conv:ident) => {{
            let arr = downcast_array::<$arr_type>($array)?;
            for idx in 0..arr.len() {
                if arr.is_null(idx) {
                    $values.push(Value::Null);
                } else {
                    $values.push($variant(arr.value(idx).$conv()));
                }
            }
        }};
    }
    macro_rules! push_timestamp_values {
        ($array:expr, $values:expr, $arr_type:ty, $duck_unit:expr) => {{
            let arr = downcast_array::<$arr_type>($array)?;
            for idx in 0..arr.len() {
                if arr.is_null(idx) {
                    $values.push(Value::Null);
                } else {
                    $values.push(Value::Timestamp($duck_unit, arr.value(idx)));
                }
            }
        }};
    }
    macro_rules! push_interval_values {
        ($array:expr, $values:expr, $arr_type:ty, $binding:ident, $make_value:expr) => {{
            let arr = downcast_array::<$arr_type>($array)?;
            for idx in 0..arr.len() {
                if arr.is_null(idx) {
                    $values.push(Value::Null);
                } else {
                    let $binding = arr.value(idx);
                    $values.push($make_value);
                }
            }
        }};
    }

    match array.data_type() {
        DataType::Null => {
            for _ in 0..array.len() {
                values.push(Value::Null);
            }
        }
        DataType::Boolean => push_values!(array, values, BooleanArray, Value::Boolean),
        DataType::Int8 => push_values!(array, values, Int8Array, Value::TinyInt),
        DataType::Int16 => push_values!(array, values, Int16Array, Value::SmallInt),
        DataType::Int32 => push_values!(array, values, Int32Array, Value::Int),
        DataType::Int64 => push_values!(array, values, Int64Array, Value::BigInt),
        DataType::UInt8 => push_values!(array, values, UInt8Array, Value::UTinyInt),
        DataType::UInt16 => push_values!(array, values, UInt16Array, Value::USmallInt),
        DataType::UInt32 => push_values!(array, values, UInt32Array, Value::UInt),
        DataType::UInt64 => push_values!(array, values, UInt64Array, Value::UBigInt),
        DataType::Float32 => push_values!(array, values, Float32Array, Value::Float),
        DataType::Float64 => push_values!(array, values, Float64Array, Value::Double),
        DataType::Utf8 => push_values!(array, values, StringArray, Value::Text, to_string),
        DataType::LargeUtf8 => {
            push_values!(array, values, LargeStringArray, Value::Text, to_string)
        }
        DataType::Binary => push_values!(array, values, BinaryArray, Value::Blob, to_vec),
        DataType::LargeBinary => {
            push_values!(array, values, LargeBinaryArray, Value::Blob, to_vec)
        }
        DataType::Date32 => {
            let arr = downcast_array::<Date32Array>(array)?;
            for idx in 0..arr.len() {
                if arr.is_null(idx) {
                    values.push(Value::Null);
                } else {
                    values.push(Value::Timestamp(
                        DuckTimeUnit::Second,
                        (arr.value(idx) as i64) * 86400,
                    ));
                }
            }
        }
        DataType::Date64 => {
            let arr = downcast_array::<Date64Array>(array)?;
            for idx in 0..arr.len() {
                if arr.is_null(idx) {
                    values.push(Value::Null);
                } else {
                    values.push(Value::Timestamp(DuckTimeUnit::Millisecond, arr.value(idx)));
                }
            }
        }
        DataType::Time32(unit) => match unit {
            TimeUnit::Second => {
                let arr = downcast_array::<Time32SecondArray>(array)?;
                for idx in 0..arr.len() {
                    if arr.is_null(idx) {
                        values.push(Value::Null);
                    } else {
                        values.push(Value::Timestamp(
                            DuckTimeUnit::Second,
                            arr.value(idx) as i64,
                        ));
                    }
                }
            }
            TimeUnit::Millisecond => {
                let arr = downcast_array::<Time32MillisecondArray>(array)?;
                for idx in 0..arr.len() {
                    if arr.is_null(idx) {
                        values.push(Value::Null);
                    } else {
                        values.push(Value::Timestamp(
                            DuckTimeUnit::Millisecond,
                            arr.value(idx) as i64,
                        ));
                    }
                }
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
                let arr = downcast_array::<Time64MicrosecondArray>(array)?;
                for idx in 0..arr.len() {
                    if arr.is_null(idx) {
                        values.push(Value::Null);
                    } else {
                        values.push(Value::Timestamp(DuckTimeUnit::Microsecond, arr.value(idx)));
                    }
                }
            }
            TimeUnit::Nanosecond => {
                let arr = downcast_array::<Time64NanosecondArray>(array)?;
                for idx in 0..arr.len() {
                    if arr.is_null(idx) {
                        values.push(Value::Null);
                    } else {
                        values.push(Value::Timestamp(DuckTimeUnit::Nanosecond, arr.value(idx)));
                    }
                }
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
                values,
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
                values,
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
                values,
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
                    push_timestamp_values!(array, values, TimestampSecondArray, duck_unit)
                }
                TimeUnit::Millisecond => {
                    push_timestamp_values!(array, values, TimestampMillisecondArray, duck_unit)
                }
                TimeUnit::Microsecond => {
                    push_timestamp_values!(array, values, TimestampMicrosecondArray, duck_unit)
                }
                TimeUnit::Nanosecond => {
                    push_timestamp_values!(array, values, TimestampNanosecondArray, duck_unit)
                }
            }
        }
        other => return Err(ServerError::UnsupportedParameter(other.to_string())),
    }

    Ok(values)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duckdb_integer_mappings() {
        assert_eq!(duckdb_type_to_arrow("INTEGER").unwrap(), DataType::Int32);
        assert_eq!(duckdb_type_to_arrow("BIGINT").unwrap(), DataType::Int64);
        assert_eq!(duckdb_type_to_arrow("SMALLINT").unwrap(), DataType::Int16);
        assert_eq!(duckdb_type_to_arrow("UTINYINT").unwrap(), DataType::UInt8);
    }

    #[test]
    fn duckdb_interval_and_timestamp_mappings() {
        assert_eq!(
            duckdb_type_to_arrow("INTERVAL").unwrap(),
            DataType::Interval(IntervalUnit::MonthDayNano)
        );
        assert_eq!(
            duckdb_type_to_arrow("TIMESTAMPTZ").unwrap(),
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()))
        );
    }

    #[test]
    fn duckdb_decimal_mapping_promotes_precision() {
        assert_eq!(
            duckdb_type_to_arrow("DECIMAL(20,2)").unwrap(),
            DataType::Decimal128(20, 2)
        );
        assert_eq!(
            duckdb_type_to_arrow("NUMERIC(60,5)").unwrap(),
            DataType::Decimal256(60, 5)
        );
    }
}
