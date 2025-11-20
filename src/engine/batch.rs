use std::collections::HashMap;

use arrow_array::{new_null_array, RecordBatch};
use arrow_cast::cast;
use arrow_schema::SchemaRef;

use crate::error::ServerError;

fn build_lookup(
    batch: &RecordBatch,
    column_override: Option<&[String]>,
) -> Result<HashMap<String, usize>, ServerError> {
    match column_override {
        Some(names) => {
            if batch.num_columns() != names.len() {
                return Err(ServerError::Internal(format!(
                    "column count mismatch for INSERT: batch has {} columns but SQL specified {}",
                    batch.num_columns(),
                    names.len()
                )));
            }
            let mut map = HashMap::new();
            for (idx, name) in names.iter().enumerate() {
                map.insert(name.clone(), idx);
            }
            Ok(map)
        }
        None => {
            let mut map = HashMap::new();
            for (idx, field) in batch.schema().fields().iter().enumerate() {
                map.insert(field.name().clone(), idx);
            }
            Ok(map)
        }
    }
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
    let lookup = build_lookup(batch, column_override)?;
    let batch_schema = batch.schema();

    let mut columns = Vec::with_capacity(table_schema.fields().len());
    for field in table_schema.fields() {
        if let Some(idx) = lookup.get(field.name()) {
            let column = batch.column(*idx);
            let source_field = batch_schema.field(*idx);
            if source_field.data_type() == field.data_type() {
                columns.push(column.clone());
            } else {
                let casted =
                    cast(column.as_ref(), field.data_type()).map_err(ServerError::Arrow)?;
                columns.push(casted);
            }
        } else {
            columns.push(new_null_array(field.data_type(), batch.num_rows()));
        }
    }

    RecordBatch::try_new(table_schema.clone(), columns).map_err(ServerError::Arrow)
}
