use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, LargeStringArray, RecordBatch, StringArray, UInt8Array};
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::{
    CommandGetCatalogs, CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys,
    CommandGetImportedKeys, CommandGetPrimaryKeys, CommandGetTableTypes, CommandGetTables,
    ProstMessageExt,
};
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use prost::Message;
use tonic::{Request, Response, Status};
use tracing::{error, warn};

use crate::service::SwanFlightSqlService;

const SCHEMAS_SQL: &str = "SELECT \
    catalog_name AS catalog_name, \
    schema_name AS schema_name \
    FROM information_schema.schemata \
    WHERE schema_name NOT IN ('information_schema', 'pg_catalog') \
      AND schema_name NOT LIKE '\\_\\_ducklake_metadata%' ESCAPE '\\'";

const TABLES_SQL: &str = "SELECT \
    table_catalog AS table_catalog, \
    table_schema AS table_schema, \
    table_name AS table_name, \
    table_type AS table_type \
    FROM information_schema.tables \
    WHERE table_schema NOT IN ('information_schema', 'pg_catalog') \
      AND table_schema NOT LIKE '\\_\\_ducklake_metadata%' ESCAPE '\\' \
      AND table_name NOT LIKE 'ducklake\\_%' ESCAPE '\\'";

const CATALOGS_SQL: &str = "PRAGMA database_list";

fn primary_keys_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, true),
        Field::new("db_schema_name", DataType::Utf8, true),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, false),
        Field::new("key_name", DataType::Utf8, true),
        Field::new("key_sequence", DataType::Int32, false),
    ]))
}

fn foreign_keys_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("pk_catalog_name", DataType::Utf8, true),
        Field::new("pk_db_schema_name", DataType::Utf8, true),
        Field::new("pk_table_name", DataType::Utf8, false),
        Field::new("pk_column_name", DataType::Utf8, false),
        Field::new("fk_catalog_name", DataType::Utf8, true),
        Field::new("fk_db_schema_name", DataType::Utf8, true),
        Field::new("fk_table_name", DataType::Utf8, false),
        Field::new("fk_column_name", DataType::Utf8, false),
        Field::new("key_sequence", DataType::Int32, false),
        Field::new("fk_key_name", DataType::Utf8, true),
        Field::new("pk_key_name", DataType::Utf8, true),
        Field::new("update_rule", DataType::UInt8, false),
        Field::new("delete_rule", DataType::UInt8, false),
    ]))
}

struct TableRow {
    catalog: String,
    schema: String,
    name: String,
    table_type: String,
    schema_def: Schema,
}

pub(crate) async fn get_flight_info_catalogs(
    service: &SwanFlightSqlService,
    query: CommandGetCatalogs,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    service.prepare_request(&request).await?;

    let ticket_bytes = query.as_any().encode_to_vec();
    let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(ticket_bytes));

    let schema = query.into_builder().schema();
    let descriptor = request.into_inner();
    let info = FlightInfo::new()
        .try_with_schema(schema.as_ref())
        .map_err(|err| Status::internal(format!("failed to encode schema: {err}")))?
        .with_descriptor(descriptor)
        .with_endpoint(endpoint)
        .with_total_records(-1);

    Ok(Response::new(info))
}

pub(crate) async fn do_get_catalogs(
    service: &SwanFlightSqlService,
    query: CommandGetCatalogs,
    request: Request<Ticket>,
) -> Result<Response<<SwanFlightSqlService as FlightService>::DoGetStream>, Status> {
    let session = service.prepare_request(&request).await?;
    let session_clone = Arc::clone(&session);
    let result = tokio::task::spawn_blocking(move || {
        let query_result = session_clone.execute_query(CATALOGS_SQL)?;
        let mut rows = Vec::new();
        for batch in query_result.batches {
            rows.extend(extract_catalog_rows(&batch));
        }
        Ok::<_, crate::error::ServerError>(rows)
    })
    .await
    .map_err(SwanFlightSqlService::status_from_join)?;

    let mut catalogs = match result {
        Ok(rows) => rows,
        Err(err) => {
            warn!(%err, "failed to load catalogs; falling back to current catalog");
            Vec::new()
        }
    };
    if catalogs.is_empty() {
        catalogs.push(
            session
                .current_catalog()
                .unwrap_or_else(|_| "memory".to_string()),
        );
    }

    let mut builder = query.into_builder();
    let mut seen = HashSet::new();
    for catalog in catalogs {
        if seen.insert(catalog.clone()) {
            builder.append(catalog);
        }
    }
    let batch = builder
        .build()
        .map_err(|err| Status::internal(format!("failed to build catalogs batch: {err}")))?;

    stream_single_batch(batch)
}

pub(crate) async fn get_flight_info_schemas(
    service: &SwanFlightSqlService,
    query: CommandGetDbSchemas,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    service.prepare_request(&request).await?;

    let ticket_bytes = query.as_any().encode_to_vec();
    let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(ticket_bytes));

    let schema = query.into_builder().schema();
    let descriptor = request.into_inner();
    let info = FlightInfo::new()
        .try_with_schema(schema.as_ref())
        .map_err(|err| Status::internal(format!("failed to encode schema: {err}")))?
        .with_descriptor(descriptor)
        .with_endpoint(endpoint)
        .with_total_records(-1);

    Ok(Response::new(info))
}

pub(crate) async fn do_get_schemas(
    service: &SwanFlightSqlService,
    query: CommandGetDbSchemas,
    request: Request<Ticket>,
) -> Result<Response<<SwanFlightSqlService as FlightService>::DoGetStream>, Status> {
    let session = service.prepare_request(&request).await?;
    let session_clone = Arc::clone(&session);

    let result = tokio::task::spawn_blocking(move || {
        let current_catalog = session_clone
            .current_catalog()
            .unwrap_or_else(|_| "memory".to_string());
        let query_result = session_clone.execute_query(SCHEMAS_SQL)?;

        let mut rows = Vec::new();
        for batch in query_result.batches {
            rows.extend(extract_schema_rows(&batch, &current_catalog));
        }
        Ok::<_, crate::error::ServerError>(rows)
    })
    .await
    .map_err(SwanFlightSqlService::status_from_join)?;

    let rows = result.map_err(SwanFlightSqlService::status_from_error)?;
    let mut builder = query.into_builder();
    for (catalog, schema_name) in rows {
        builder.append(catalog, schema_name);
    }
    let batch = builder
        .build()
        .map_err(|err| Status::internal(format!("failed to build schemas batch: {err}")))?;

    stream_single_batch(batch)
}

pub(crate) async fn get_flight_info_tables(
    service: &SwanFlightSqlService,
    query: CommandGetTables,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    service.prepare_request(&request).await?;

    let ticket_bytes = query.as_any().encode_to_vec();
    let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(ticket_bytes));

    let schema = query.into_builder().schema();
    let descriptor = request.into_inner();
    let info = FlightInfo::new()
        .try_with_schema(schema.as_ref())
        .map_err(|err| Status::internal(format!("failed to encode schema: {err}")))?
        .with_descriptor(descriptor)
        .with_endpoint(endpoint)
        .with_total_records(-1);

    Ok(Response::new(info))
}

pub(crate) async fn do_get_tables(
    service: &SwanFlightSqlService,
    query: CommandGetTables,
    request: Request<Ticket>,
) -> Result<Response<<SwanFlightSqlService as FlightService>::DoGetStream>, Status> {
    let include_schema = query.include_schema;
    let session = service.prepare_request(&request).await?;
    let session_clone = Arc::clone(&session);

    let result = tokio::task::spawn_blocking(move || {
        let current_catalog = session_clone
            .current_catalog()
            .unwrap_or_else(|_| "memory".to_string());
        let query_result = session_clone.execute_query(TABLES_SQL)?;
        let mut rows = Vec::new();
        for batch in query_result.batches {
            rows.extend(extract_table_rows(
                &batch,
                &session_clone,
                include_schema,
                &current_catalog,
            ));
        }
        Ok::<_, crate::error::ServerError>(rows)
    })
    .await
    .map_err(SwanFlightSqlService::status_from_join)?;

    let rows = result.map_err(SwanFlightSqlService::status_from_error)?;
    let mut builder = query.into_builder();
    for row in rows {
        builder
            .append(
                &row.catalog,
                &row.schema,
                &row.name,
                &row.table_type,
                &row.schema_def,
            )
            .map_err(|err| Status::internal(format!("failed to append table row: {err}")))?;
    }

    let batch = builder
        .build()
        .map_err(|err| Status::internal(format!("failed to build tables batch: {err}")))?;

    stream_single_batch(batch)
}

pub(crate) async fn get_flight_info_table_types(
    service: &SwanFlightSqlService,
    query: CommandGetTableTypes,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    service.prepare_request(&request).await?;

    let ticket_bytes = query.as_any().encode_to_vec();
    let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(ticket_bytes));

    let schema = query.into_builder().schema();
    let descriptor = request.into_inner();
    let info = FlightInfo::new()
        .try_with_schema(schema.as_ref())
        .map_err(|err| Status::internal(format!("failed to encode schema: {err}")))?
        .with_descriptor(descriptor)
        .with_endpoint(endpoint)
        .with_total_records(-1);

    Ok(Response::new(info))
}

pub(crate) async fn do_get_table_types(
    service: &SwanFlightSqlService,
    query: CommandGetTableTypes,
    request: Request<Ticket>,
) -> Result<Response<<SwanFlightSqlService as FlightService>::DoGetStream>, Status> {
    service.prepare_request(&request).await?;

    let mut builder = query.into_builder();
    builder.append("TABLE");
    builder.append("VIEW");
    let batch = builder
        .build()
        .map_err(|err| Status::internal(format!("failed to build table types batch: {err}")))?;

    stream_single_batch(batch)
}

pub(crate) async fn get_flight_info_primary_keys(
    service: &SwanFlightSqlService,
    query: CommandGetPrimaryKeys,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    service.prepare_request(&request).await?;
    flight_info_with_schema(
        query.as_any().encode_to_vec(),
        primary_keys_schema(),
        request,
    )
}

pub(crate) async fn do_get_primary_keys(
    service: &SwanFlightSqlService,
    _query: CommandGetPrimaryKeys,
    request: Request<Ticket>,
) -> Result<Response<<SwanFlightSqlService as FlightService>::DoGetStream>, Status> {
    service.prepare_request(&request).await?;
    stream_single_batch(empty_batch(primary_keys_schema())?)
}

pub(crate) async fn get_flight_info_exported_keys(
    service: &SwanFlightSqlService,
    query: CommandGetExportedKeys,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    service.prepare_request(&request).await?;
    flight_info_with_schema(
        query.as_any().encode_to_vec(),
        foreign_keys_schema(),
        request,
    )
}

pub(crate) async fn do_get_exported_keys(
    service: &SwanFlightSqlService,
    _query: CommandGetExportedKeys,
    request: Request<Ticket>,
) -> Result<Response<<SwanFlightSqlService as FlightService>::DoGetStream>, Status> {
    service.prepare_request(&request).await?;
    stream_single_batch(empty_batch(foreign_keys_schema())?)
}

pub(crate) async fn get_flight_info_imported_keys(
    service: &SwanFlightSqlService,
    query: CommandGetImportedKeys,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    service.prepare_request(&request).await?;
    flight_info_with_schema(
        query.as_any().encode_to_vec(),
        foreign_keys_schema(),
        request,
    )
}

pub(crate) async fn do_get_imported_keys(
    service: &SwanFlightSqlService,
    _query: CommandGetImportedKeys,
    request: Request<Ticket>,
) -> Result<Response<<SwanFlightSqlService as FlightService>::DoGetStream>, Status> {
    service.prepare_request(&request).await?;
    stream_single_batch(empty_batch(foreign_keys_schema())?)
}

pub(crate) async fn get_flight_info_cross_reference(
    service: &SwanFlightSqlService,
    query: CommandGetCrossReference,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    service.prepare_request(&request).await?;
    flight_info_with_schema(
        query.as_any().encode_to_vec(),
        foreign_keys_schema(),
        request,
    )
}

pub(crate) async fn do_get_cross_reference(
    service: &SwanFlightSqlService,
    _query: CommandGetCrossReference,
    request: Request<Ticket>,
) -> Result<Response<<SwanFlightSqlService as FlightService>::DoGetStream>, Status> {
    service.prepare_request(&request).await?;
    stream_single_batch(empty_batch(foreign_keys_schema())?)
}

fn extract_schema_rows(batch: &RecordBatch, default_catalog: &str) -> Vec<(String, String)> {
    let mut rows = Vec::new();
    if batch.num_rows() == 0 || batch.num_columns() == 0 {
        return rows;
    }

    let schema = batch.schema();
    let catalog_idx = column_index(&schema, "catalog_name").unwrap_or(0);
    let schema_idx = column_index(&schema, "schema_name").unwrap_or(1);
    let catalog_col = batch.column(catalog_idx);
    let schema_col = batch.column(schema_idx);
    for row in 0..batch.num_rows() {
        let catalog = string_value(catalog_col, row).unwrap_or_else(|| default_catalog.to_string());
        if let Some(schema) = string_value(schema_col, row) {
            rows.push((catalog, schema));
        }
    }
    rows
}

fn extract_table_rows(
    batch: &RecordBatch,
    session: &Arc<crate::session::Session>,
    include_schema: bool,
    default_catalog: &str,
) -> Vec<TableRow> {
    let mut rows = Vec::new();
    if batch.num_rows() == 0 || batch.num_columns() < 3 {
        return rows;
    }

    let schema = batch.schema();
    let catalog_idx = column_index(&schema, "table_catalog").unwrap_or(0);
    let schema_idx = column_index(&schema, "table_schema").unwrap_or(1);
    let name_idx = column_index(&schema, "table_name").unwrap_or(2);
    let type_idx = column_index(&schema, "table_type").unwrap_or(3);

    let catalog_col = batch.column(catalog_idx);
    let schema_col = batch.column(schema_idx);
    let name_col = batch.column(name_idx);
    let type_col = batch.column(type_idx);

    for row in 0..batch.num_rows() {
        let catalog = string_value(catalog_col, row).unwrap_or_else(|| default_catalog.to_string());
        let schema = string_value(schema_col, row).unwrap_or_else(|| "main".to_string());
        let name = match string_value(name_col, row) {
            Some(value) => value,
            None => continue,
        };
        let table_type = normalize_table_type(string_value(type_col, row));

        let schema_def = if include_schema {
            let qualified_name = format!("{catalog}.{schema}.{name}");
            match session.table_schema(&qualified_name) {
                Ok(schema) => schema,
                Err(err) => {
                    warn!(%err, table = %qualified_name, "failed to load table schema");
                    Schema::empty()
                }
            }
        } else {
            Schema::empty()
        };

        rows.push(TableRow {
            catalog,
            schema,
            name,
            table_type,
            schema_def,
        });
    }

    rows
}

fn normalize_table_type(value: Option<String>) -> String {
    match value.as_deref() {
        Some("BASE TABLE") => "TABLE".to_string(),
        Some("VIEW") => "VIEW".to_string(),
        Some(other) => other.to_string(),
        None => "TABLE".to_string(),
    }
}

fn extract_catalog_rows(batch: &RecordBatch) -> Vec<String> {
    let mut rows = Vec::new();
    if batch.num_rows() == 0 || batch.num_columns() == 0 {
        return rows;
    }

    let schema = batch.schema();
    let name_idx = column_index(&schema, "name")
        .or_else(|| column_index(&schema, "database_name"))
        .or_else(|| {
            schema
                .fields()
                .iter()
                .position(|field| matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8))
        })
        .unwrap_or(0);
    let name_col = batch.column(name_idx);
    for row in 0..batch.num_rows() {
        if let Some(name) = string_value(name_col, row) {
            rows.push(name);
        }
    }

    rows
}

fn string_value(array: &ArrayRef, row: usize) -> Option<String> {
    if array.is_null(row) {
        return None;
    }
    if let Some(values) = array.as_any().downcast_ref::<StringArray>() {
        return Some(values.value(row).to_string());
    }
    if let Some(values) = array.as_any().downcast_ref::<LargeStringArray>() {
        return Some(values.value(row).to_string());
    }
    None
}

fn column_index(schema: &SchemaRef, name: &str) -> Option<usize> {
    schema
        .fields()
        .iter()
        .position(|field| field.name() == name)
}

fn empty_batch(schema: SchemaRef) -> Result<RecordBatch, Status> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let array: ArrayRef = match field.data_type() {
            DataType::Utf8 => Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            DataType::Int32 => Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
            DataType::UInt8 => Arc::new(UInt8Array::from(Vec::<Option<u8>>::new())),
            other => {
                return Err(Status::internal(format!(
                    "unsupported metadata type: {other:?}"
                )))
            }
        };
        columns.push(array);
    }

    RecordBatch::try_new(schema, columns)
        .map_err(|err| Status::internal(format!("failed to build empty batch: {err}")))
}

fn flight_info_with_schema(
    ticket_bytes: Vec<u8>,
    schema: SchemaRef,
    request: Request<FlightDescriptor>,
) -> Result<Response<FlightInfo>, Status> {
    let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(ticket_bytes));
    let descriptor = request.into_inner();
    let info = FlightInfo::new()
        .try_with_schema(schema.as_ref())
        .map_err(|err| Status::internal(format!("failed to encode schema: {err}")))?
        .with_descriptor(descriptor)
        .with_endpoint(endpoint)
        .with_total_records(-1);
    Ok(Response::new(info))
}

fn stream_single_batch(
    batch: RecordBatch,
) -> Result<Response<<SwanFlightSqlService as FlightService>::DoGetStream>, Status> {
    let schema = batch.schema();
    let flight_data =
        arrow_flight::utils::batches_to_flight_data(&schema, vec![batch]).map_err(|err| {
            error!(%err, "failed to convert metadata batch to flight data");
            Status::internal(format!(
                "failed to convert metadata batch to flight data: {err}"
            ))
        })?;
    let stream = SwanFlightSqlService::into_stream(flight_data);
    Ok(Response::new(stream))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::{ensure, Context, Result};
    use arrow_array::{ArrayRef, Int32Array, LargeStringArray, StringArray, UInt8Array};
    use arrow_schema::{DataType, Field, Schema};
    use duckdb::Connection;
    use futures::StreamExt;
    use tonic::Request;

    use super::*;
    use crate::engine::DuckDbConnection;
    use crate::session::{Session, SessionId};

    fn test_session() -> Result<Arc<Session>> {
        let conn = Connection::open_in_memory().context("failed to open in-memory duckdb")?;
        let session = Session::new_with_id(
            SessionId::from_string("metadata-test-session".to_string()),
            DuckDbConnection::new(conn),
        );
        Ok(Arc::new(session))
    }

    #[test]
    fn primary_and_foreign_key_schema_shapes_are_stable() {
        let pk = primary_keys_schema();
        let fk = foreign_keys_schema();

        assert_eq!(pk.fields().len(), 6);
        assert_eq!(pk.field(2).name(), "table_name");
        assert_eq!(pk.field(5).data_type(), &DataType::Int32);

        assert_eq!(fk.fields().len(), 13);
        assert_eq!(fk.field(0).name(), "pk_catalog_name");
        assert_eq!(fk.field(12).data_type(), &DataType::UInt8);
    }

    #[test]
    fn extract_schema_rows_prefers_named_columns_and_defaults_catalog() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("schema_name", DataType::Utf8, true),
            Field::new("catalog_name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("main"), Some("analytics")])),
                Arc::new(StringArray::from(vec![Some("swanlake"), None])),
            ],
        )
        .context("failed to build schema extraction test batch")?;

        let rows = extract_schema_rows(&batch, "fallback");
        assert_eq!(
            rows,
            vec![
                ("swanlake".to_string(), "main".to_string()),
                ("fallback".to_string(), "analytics".to_string())
            ]
        );
        Ok(())
    }

    #[test]
    fn extract_catalog_rows_supports_named_and_large_utf8_columns() -> Result<()> {
        let named_schema = Arc::new(Schema::new(vec![Field::new(
            "database_name",
            DataType::Utf8,
            true,
        )]));
        let named_batch = RecordBatch::try_new(
            named_schema,
            vec![Arc::new(StringArray::from(vec![
                Some("swanlake"),
                Some("memory"),
            ]))],
        )
        .context("failed to build named catalog batch")?;
        assert_eq!(
            extract_catalog_rows(&named_batch),
            vec!["swanlake".to_string(), "memory".to_string()]
        );

        let fallback_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("catalog_value", DataType::LargeUtf8, true),
        ]));
        let fallback_batch = RecordBatch::try_new(
            fallback_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                Arc::new(LargeStringArray::from(vec![
                    Some("catalog_a"),
                    None,
                    Some("catalog_b"),
                ])),
            ],
        )
        .context("failed to build fallback catalog batch")?;
        assert_eq!(
            extract_catalog_rows(&fallback_batch),
            vec!["catalog_a".to_string(), "catalog_b".to_string()]
        );
        Ok(())
    }

    #[test]
    fn extract_table_rows_handles_missing_values_and_schema_lookup_failures() -> Result<()> {
        let session = test_session()?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_catalog", DataType::Utf8, true),
            Field::new("table_schema", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, true),
            Field::new("table_type", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("swanlake"), None, None])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("main"), Some("main"), None])),
                Arc::new(StringArray::from(vec![
                    Some("missing_table"),
                    Some("view_like"),
                    None,
                ])),
                Arc::new(StringArray::from(vec![
                    Some("BASE TABLE"),
                    Some("VIEW"),
                    None,
                ])),
            ],
        )
        .context("failed to build table metadata batch")?;

        let no_schema_rows = extract_table_rows(&batch, &session, false, "fallback");
        assert_eq!(no_schema_rows.len(), 2);
        assert_eq!(no_schema_rows[0].table_type, "TABLE");
        assert_eq!(no_schema_rows[1].table_type, "VIEW");
        assert_eq!(no_schema_rows[1].catalog, "fallback");

        let with_schema_rows = extract_table_rows(&batch, &session, true, "fallback");
        assert_eq!(with_schema_rows.len(), 2);
        assert!(with_schema_rows[0].schema_def.fields().is_empty());
        assert!(with_schema_rows[1].schema_def.fields().is_empty());

        Ok(())
    }

    #[test]
    fn normalize_and_string_helpers_cover_utf8_large_utf8_and_nulls() {
        assert_eq!(
            normalize_table_type(Some("BASE TABLE".to_string())),
            "TABLE"
        );
        assert_eq!(normalize_table_type(Some("VIEW".to_string())), "VIEW");
        assert_eq!(normalize_table_type(Some("TEMP".to_string())), "TEMP");
        assert_eq!(normalize_table_type(None), "TABLE");

        let utf8: ArrayRef = Arc::new(StringArray::from(vec![Some("alpha"), None]));
        let large_utf8: ArrayRef = Arc::new(LargeStringArray::from(vec![Some("beta"), None]));
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));

        assert_eq!(string_value(&utf8, 0).as_deref(), Some("alpha"));
        assert_eq!(string_value(&utf8, 1), None);
        assert_eq!(string_value(&large_utf8, 0).as_deref(), Some("beta"));
        assert_eq!(string_value(&large_utf8, 1), None);
        assert_eq!(string_value(&ints, 0), None);

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]));
        assert_eq!(column_index(&schema, "a"), Some(0));
        assert_eq!(column_index(&schema, "missing"), None);
    }

    #[test]
    fn empty_batch_supports_metadata_types_and_rejects_unsupported() -> Result<()> {
        let supported = Arc::new(Schema::new(vec![
            Field::new("txt", DataType::Utf8, true),
            Field::new("n", DataType::Int32, true),
            Field::new("rule", DataType::UInt8, true),
        ]));
        let batch = empty_batch(supported)?;
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.num_rows(), 0);
        let col = batch.column(2);
        let values = col.as_any().downcast_ref::<UInt8Array>();
        assert!(values.is_some());

        let unsupported = Arc::new(Schema::new(vec![Field::new("bin", DataType::Binary, true)]));
        let result = empty_batch(unsupported);
        assert!(result.is_err());
        if let Err(status) = result {
            assert!(status.message().contains("unsupported metadata type"));
        }
        Ok(())
    }

    #[test]
    fn flight_info_with_schema_sets_descriptor_ticket_and_schema() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "catalog_name",
            DataType::Utf8,
            true,
        )]));
        let descriptor = FlightDescriptor::new_cmd(vec![9, 8, 7]);
        let response = flight_info_with_schema(vec![1, 2, 3], schema, Request::new(descriptor))?;
        let info = response.into_inner();

        ensure!(
            !info.schema.is_empty(),
            "flight info schema should be encoded"
        );
        ensure!(
            info.flight_descriptor.is_some(),
            "flight descriptor should be set"
        );
        ensure!(
            info.total_records == -1,
            "metadata flight info should use unknown row count"
        );
        ensure!(
            info.endpoint.len() == 1,
            "metadata flight info should have one endpoint"
        );
        ensure!(
            info.endpoint
                .first()
                .and_then(|endpoint| endpoint.ticket.as_ref())
                .is_some_and(|ticket| ticket.ticket.as_ref() == [1, 2, 3]),
            "metadata endpoint ticket should preserve payload"
        );

        Ok(())
    }

    #[tokio::test]
    async fn stream_single_batch_emits_flight_data() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![Some("x"), Some("y")])) as ArrayRef],
        )
        .context("failed to build stream test batch")?;

        let response = stream_single_batch(batch)?;
        let mut stream = response.into_inner();
        let mut frames = 0usize;
        while let Some(item) = stream.next().await {
            item.context("stream frame returned error")?;
            frames = frames.saturating_add(1);
        }

        ensure!(frames > 0, "expected at least one flight frame");
        Ok(())
    }
}
