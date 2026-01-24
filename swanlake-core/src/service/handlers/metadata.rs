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
    let catalog = session
        .current_catalog()
        .unwrap_or_else(|_| "memory".to_string());

    let mut builder = query.into_builder();
    builder.append(catalog);
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
            let qualified_name = format!("{schema}.{name}");
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
