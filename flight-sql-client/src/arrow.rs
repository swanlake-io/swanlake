use anyhow::{anyhow, bail, Result};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, StringArray,
    TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::DataType;
use chrono::{DateTime, NaiveDate, TimeDelta, Utc};

/// Convert an Arrow value to a printable string.
///
/// Supports various Arrow data types, formatting them appropriately.
/// Returns "NULL" for null values.
///
/// # Example
///
/// ```rust
/// use arrow_array::{Array, Int64Array};
/// use flight_sql_client::arrow::array_value_to_string;
/// use std::sync::Arc;
///
/// let arr = Arc::new(Int64Array::from(vec![1, 2])) as Arc<dyn Array>;
/// let str_val = array_value_to_string(&*arr, 0)?;
/// assert_eq!(str_val, "1");
/// # Ok::<(), anyhow::Error>(())
/// ```
pub fn array_value_to_string(column: &dyn Array, row_idx: usize) -> Result<String> {
    if column.is_null(row_idx) {
        return Ok("NULL".to_string());
    }

    match column.data_type() {
        DataType::Boolean => {
            let arr = column.as_any().downcast_ref::<BooleanArray>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Int8 => {
            let arr = column.as_any().downcast_ref::<Int8Array>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Int16 => {
            let arr = column.as_any().downcast_ref::<Int16Array>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Int32 => {
            let arr = column.as_any().downcast_ref::<Int32Array>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Int64 => {
            let arr = column.as_any().downcast_ref::<Int64Array>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::UInt8 => {
            let arr = column.as_any().downcast_ref::<UInt8Array>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::UInt16 => {
            let arr = column.as_any().downcast_ref::<UInt16Array>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::UInt32 => {
            let arr = column.as_any().downcast_ref::<UInt32Array>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::UInt64 => {
            let arr = column.as_any().downcast_ref::<UInt64Array>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Float32 => {
            let arr = column.as_any().downcast_ref::<Float32Array>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Float64 => {
            let arr = column.as_any().downcast_ref::<Float64Array>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Utf8 => {
            let arr = column.as_any().downcast_ref::<StringArray>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::LargeUtf8 => {
            let arr = column.as_any().downcast_ref::<LargeStringArray>().unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        DataType::Binary => {
            let arr = column.as_any().downcast_ref::<BinaryArray>().unwrap();
            Ok(format!("{:?}", arr.value(row_idx)))
        }
        DataType::LargeBinary => {
            let arr = column.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            Ok(format!("{:?}", arr.value(row_idx)))
        }
        DataType::Date32 => {
            let arr = column.as_any().downcast_ref::<Date32Array>().unwrap();
            let days = arr.value(row_idx) as i64;
            let date = NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .checked_add_signed(TimeDelta::days(days))
                .unwrap();
            Ok(date.format("%Y-%m-%d").to_string())
        }
        DataType::Date64 => {
            let arr = column.as_any().downcast_ref::<Date64Array>().unwrap();
            let millis = arr.value(row_idx);
            let secs = millis / 1000;
            let date = DateTime::<Utc>::from_timestamp(secs, 0)
                .unwrap()
                .date_naive();
            Ok(date.format("%Y-%m-%d").to_string())
        }
        DataType::Timestamp(_, _) => {
            let arr = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| anyhow!("unsupported timestamp precision"))?;
            Ok(arr.value(row_idx).to_string())
        }
        _ => Ok(format!("{:?}", column)),
    }
}

/// Interpret a scalar Arrow value as i64.
///
/// Supports integer types (signed and unsigned, various widths).
/// Fails if the value is null or of an unsupported type.
///
/// # Example
///
/// ```rust
/// use arrow_array::{Array, Int64Array};
/// use flight_sql_client::arrow::value_as_i64;
/// use std::sync::Arc;
///
/// let arr = Arc::new(Int64Array::from(vec![42])) as Arc<dyn Array>;
/// let val = value_as_i64(&*arr, 0)?;
/// assert_eq!(val, 42);
/// # Ok::<(), anyhow::Error>(())
/// ```
pub fn value_as_i64(column: &dyn Array, idx: usize) -> Result<i64> {
    if column.is_null(idx) {
        bail!("value is NULL");
    }
    if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
        return Ok(array.value(idx));
    }
    if let Some(array) = column.as_any().downcast_ref::<Int32Array>() {
        return Ok(array.value(idx) as i64);
    }
    if let Some(array) = column.as_any().downcast_ref::<Int16Array>() {
        return Ok(array.value(idx) as i64);
    }
    if let Some(array) = column.as_any().downcast_ref::<UInt64Array>() {
        return Ok(array.value(idx) as i64);
    }
    if let Some(array) = column.as_any().downcast_ref::<UInt32Array>() {
        return Ok(array.value(idx) as i64);
    }
    if let Some(array) = column.as_any().downcast_ref::<UInt16Array>() {
        return Ok(array.value(idx) as i64);
    }

    Err(anyhow!(
        "unsupported column type {} for integer projection",
        column.data_type()
    ))
}

/// Interpret a scalar Arrow value as string.
///
/// Supports UTF-8 string types.
/// Fails if the value is null or of an unsupported type.
///
/// # Example
///
/// ```rust
/// use arrow_array::{Array, StringArray};
/// use flight_sql_client::arrow::value_as_string;
/// use std::sync::Arc;
///
/// let arr = Arc::new(StringArray::from(vec!["hello"])) as Arc<dyn Array>;
/// let val = value_as_string(&*arr, 0)?;
/// assert_eq!(val, "hello");
/// # Ok::<(), anyhow::Error>(())
/// ```
pub fn value_as_string(column: &dyn Array, idx: usize) -> Result<String> {
    if column.is_null(idx) {
        bail!("value is NULL");
    }
    if let Some(array) = column.as_any().downcast_ref::<StringArray>() {
        return Ok(array.value(idx).to_string());
    }
    Err(anyhow!(
        "unsupported column type {} for string projection",
        column.data_type()
    ))
}
