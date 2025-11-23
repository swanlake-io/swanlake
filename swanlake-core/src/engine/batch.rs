use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{new_null_array, ArrayRef, RecordBatch};
use arrow_cast::cast;
use arrow_schema::{Field, Schema, SchemaRef};

use crate::error::ServerError;

/// Check if batch schema uses positional parameter names like "1", "2", "3"
/// This happens when Go drivers send parameters with $1, $2, $3 placeholders
fn has_positional_field_names(batch: &RecordBatch) -> bool {
    batch
        .schema()
        .fields()
        .iter()
        .enumerate()
        .all(|(idx, field)| field.name() == &(idx + 1).to_string())
}

/// Reshape a batch from Go driver's multi-row INSERT format.
///
/// Go drivers send multi-row INSERTs like `VALUES (?, ?, ?),(?, ?, ?),(?, ?, ?)` as:
/// - N×M columns (e.g., "1" through "9") × 1 row
///
/// We need to reshape it to:
/// - M columns × N rows (e.g., 3 columns × 3 rows)
fn reshape_batch_for_multi_row_insert(
    batch: &RecordBatch,
    num_columns: usize,
) -> Result<RecordBatch, ServerError> {
    let total_params = batch.num_columns();

    if !total_params.is_multiple_of(num_columns) {
        return Err(ServerError::Internal(format!(
            "Cannot reshape batch: {} total parameters is not divisible by {} columns",
            total_params, num_columns
        )));
    }

    let num_rows = total_params / num_columns;

    if batch.num_rows() != 1 {
        return Err(ServerError::Internal(format!(
            "Expected batch with 1 row for reshaping, got {} rows",
            batch.num_rows()
        )));
    }

    tracing::debug!(
        "Reshaping batch from {} columns × 1 row to {} columns × {} rows",
        total_params,
        num_columns,
        num_rows
    );

    // Build new schema with the target column count
    let batch_schema = batch.schema();
    let mut fields = Vec::with_capacity(num_columns);
    for i in 0..num_columns {
        let field = batch_schema.field(i);
        fields.push(Field::new(
            (i + 1).to_string(),
            field.data_type().clone(),
            field.is_nullable(),
        ));
    }
    let new_schema = Arc::new(Schema::new(fields));

    // Build new columns by slicing and concatenating the original columns
    let mut new_columns: Vec<ArrayRef> = Vec::with_capacity(num_columns);

    for col_idx in 0..num_columns {
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(num_rows);

        for row_idx in 0..num_rows {
            let source_col_idx = row_idx * num_columns + col_idx;
            let source_array = batch.column(source_col_idx);

            // Each source column has 1 row, extract that single value
            arrays.push(source_array.slice(0, 1));
        }

        // Concatenate all the single-value arrays into one column
        let concatenated =
            arrow_select::concat::concat(&arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>())
                .map_err(ServerError::Arrow)?;
        new_columns.push(concatenated);
    }

    RecordBatch::try_new(new_schema, new_columns).map_err(ServerError::Arrow)
}

fn build_lookup(
    batch: &RecordBatch,
    table_schema: &SchemaRef,
    column_override: Option<&[String]>,
) -> Result<HashMap<String, usize>, ServerError> {
    let mut map = HashMap::new();

    if let Some(names) = column_override {
        if batch.num_columns() != names.len() {
            return Err(ServerError::Internal(format!(
                "column count mismatch for INSERT: batch has {} columns but SQL specified {} columns",
                batch.num_columns(),
                names.len(),
            )));
        }

        // If batch uses positional parameter names ("1", "2", "3"), map by position
        // This handles Go drivers that send $1, $2, $3 which become field names "1", "2", "3"
        if has_positional_field_names(batch) {
            for (idx, name) in names.iter().enumerate() {
                map.insert(name.clone(), idx);
            }
        } else {
            // Map each column name to its actual position in the batch schema
            for name in names.iter() {
                let batch_idx = batch.schema().index_of(name).map_err(|_| {
                    ServerError::Internal(format!(
                        "Column '{}' not found in batch schema {}",
                        name,
                        batch.schema()
                    ))
                })?;
                map.insert(name.clone(), batch_idx);
            }
        }
        return Ok(map);
    }

    if has_positional_field_names(batch) {
        if batch.num_columns() != table_schema.fields().len() {
            return Err(ServerError::Internal(format!(
                "column count mismatch for INSERT: batch has {} columns but table has {}",
                batch.num_columns(),
                table_schema.fields().len()
            )));
        }
        for (idx, field) in table_schema.fields().iter().enumerate() {
            map.insert(field.name().clone(), idx);
        }
        return Ok(map);
    }

    for (idx, field) in batch.schema().fields().iter().enumerate() {
        map.insert(field.name().clone(), idx);
    }
    Ok(map)
}

/// Align a RecordBatch to the physical table schema.
///
/// Columns that exist in the table but are missing from the batch are populated
/// with NULL arrays. Columns present in the batch but absent from the table are
/// ignored.
pub fn align_batch_to_table_schema(
    batch: &RecordBatch,
    table_schema: &SchemaRef,
    column_override: Option<&[String]>,
) -> Result<RecordBatch, ServerError> {
    // Fast path for empty batches: preserve row count (0) with table-shaped empty columns.
    if batch.num_rows() == 0 && batch.num_columns() == 0 {
        let columns = table_schema
            .fields()
            .iter()
            .map(|field| new_null_array(field.data_type(), 0))
            .collect::<Vec<_>>();
        return RecordBatch::try_new(table_schema.clone(), columns).map_err(ServerError::Arrow);
    }

    // Handle Go driver's multi-row INSERT format:
    // Go drivers send VALUES (?, ?, ?),(?, ?, ?),(?, ?, ?) as 9 columns × 1 row
    // but we expect 3 columns × 3 rows. Detect and reshape if needed.
    let batch_to_use = if let Some(names) = column_override {
        if has_positional_field_names(batch)
            && batch.num_columns() > names.len()
            && batch.num_columns().is_multiple_of(names.len())
            && batch.num_rows() == 1
        {
            // This looks like Go driver's multi-row INSERT format
            tracing::debug!(
                "Detected Go driver multi-row INSERT format: {} columns, reshaping to {} columns",
                batch.num_columns(),
                names.len()
            );
            reshape_batch_for_multi_row_insert(batch, names.len())?
        } else {
            batch.clone()
        }
    } else if has_positional_field_names(batch)
        && batch.num_columns() > table_schema.fields().len()
        && batch
            .num_columns()
            .is_multiple_of(table_schema.fields().len())
        && batch.num_rows() == 1
    {
        tracing::debug!(
            "Detected positional multi-row INSERT format without column list: {} columns, reshaping to {} columns",
            batch.num_columns(),
            table_schema.fields().len()
        );
        reshape_batch_for_multi_row_insert(batch, table_schema.fields().len())?
    } else {
        batch.clone()
    };

    // Build lookup for column mapping
    let lookup = build_lookup(&batch_to_use, table_schema, column_override)?;
    let batch_schema = batch_to_use.schema();
    tracing::debug!(
        "Aligning batch schema {:?} to table schema {:?}, lookup: {:?}",
        batch_schema,
        table_schema,
        lookup
    );

    let mut columns = Vec::with_capacity(table_schema.fields().len());
    for field in table_schema.fields() {
        if let Some(idx) = lookup.get(field.name()) {
            let column = batch_to_use.column(*idx);
            let source_field = batch_schema.field(*idx);
            if source_field.data_type() == field.data_type() {
                columns.push(column.clone());
            } else {
                let casted =
                    cast(column.as_ref(), field.data_type()).map_err(ServerError::Arrow)?;
                columns.push(casted);
            }
        } else {
            columns.push(new_null_array(field.data_type(), batch_to_use.num_rows()));
        }
    }

    RecordBatch::try_new(table_schema.clone(), columns).map_err(ServerError::Arrow)
}
