use arrow_schema::{DataType, SchemaRef};

use crate::error::ServerError;

/// Generate DuckDB column definitions for the provided Arrow schema.
pub fn duckdb_columns(schema: &SchemaRef) -> Result<Vec<String>, ServerError> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let ty = duckdb_type(field.data_type())?;
            let mut col = format!("{} {}", quote_ident(field.name()), ty);
            if !field.is_nullable() {
                col.push_str(" NOT NULL");
            }
            Ok(col)
        })
        .collect()
}

fn duckdb_type(dt: &DataType) -> Result<String, ServerError> {
    Ok(match dt {
        DataType::Null => "VARCHAR".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INTEGER".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::UInt8 => "UTINYINT".to_string(),
        DataType::UInt16 => "USMALLINT".to_string(),
        DataType::UInt32 => "UINTEGER".to_string(),
        DataType::UInt64 => "UBIGINT".to_string(),
        DataType::Float16 | DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            format!("DECIMAL({}, {})", precision, scale)
        }
        DataType::Utf8 | DataType::LargeUtf8 => "VARCHAR".to_string(),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            "BLOB".to_string()
        }
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Time32(_) | DataType::Time64(_) => "TIME".to_string(),
        DataType::Timestamp(_, _) => "TIMESTAMP".to_string(),
        DataType::Duration(_) => "BIGINT".to_string(),
        DataType::Interval(_) => "INTERVAL".to_string(),
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            "JSON".to_string()
        }
        DataType::Struct(_) | DataType::Map(_, _) | DataType::Union(_, _) => "JSON".to_string(),
        other => {
            return Err(ServerError::Internal(format!(
                "unsupported Arrow data type in duckling queue buffer: {other:?}"
            )));
        }
    })
}

pub fn quote_ident(ident: &str) -> String {
    let mut quoted = String::with_capacity(ident.len() + 2);
    quoted.push('"');
    for ch in ident.chars() {
        if ch == '"' {
            quoted.push('"');
        }
        quoted.push(ch);
    }
    quoted.push('"');
    quoted
}
