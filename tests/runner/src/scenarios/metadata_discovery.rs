use std::collections::HashSet;

use adbc_core::options::ObjectDepth;
use adbc_core::Connection;
use anyhow::{anyhow, ensure, Context, Result};
use arrow_array::{Array, LargeStringArray, RecordBatchReader, StringArray};
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::{
    CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys,
    CommandGetPrimaryKeys, CommandGetTables,
};
use arrow_flight::FlightInfo;
use futures::StreamExt;
use swanlake_client::FlightSQLClient;
use tonic::transport::{Channel, Endpoint};
use tracing::info;

use crate::CliArgs;

const TABLE_NAME: &str = "metadata_discovery_table";
const VIEW_NAME: &str = "metadata_discovery_view";

pub fn run_metadata_discovery(args: &CliArgs) -> Result<()> {
    info!("Running metadata discovery tests");

    let mut client = FlightSQLClient::connect(&args.endpoint)
        .context("failed to connect to FlightSQL server for metadata tests")?;
    client.update("use swanlake")?;

    setup_metadata_fixture(&mut client)?;

    let test_result = (|| -> Result<()> {
        run_adbc_metadata_checks(&mut client)?;
        run_flight_sql_metadata_checks(&args.endpoint)?;
        Ok(())
    })();

    cleanup_metadata_fixture(&mut client)?;
    test_result?;

    info!("Metadata discovery tests completed successfully");
    Ok(())
}

fn run_adbc_metadata_checks(client: &mut FlightSQLClient) -> Result<()> {
    let mut table_types = HashSet::new();
    let type_reader = client.connection().get_table_types()?;
    for batch in type_reader {
        let batch = batch?;
        let column = batch.column(0);
        if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
            for idx in 0..values.len() {
                if !values.is_null(idx) {
                    table_types.insert(values.value(idx).to_uppercase());
                }
            }
        } else if let Some(values) = column.as_any().downcast_ref::<LargeStringArray>() {
            for idx in 0..values.len() {
                if !values.is_null(idx) {
                    table_types.insert(values.value(idx).to_uppercase());
                }
            }
        } else {
            return Err(anyhow!(
                "get_table_types returned non-string table_type column"
            ));
        }
    }
    ensure!(
        table_types.contains("TABLE"),
        "metadata table types did not include TABLE"
    );
    ensure!(
        table_types.contains("VIEW"),
        "metadata table types did not include VIEW"
    );

    let catalog_rows = count_reader_rows(client.connection().get_objects(
        ObjectDepth::Catalogs,
        None,
        None,
        None,
        None,
        None,
    )?)?;
    ensure!(catalog_rows > 0, "expected at least one catalog row");

    let schema_rows = count_reader_rows(client.connection().get_objects(
        ObjectDepth::Schemas,
        None,
        None,
        None,
        None,
        None,
    )?)?;
    ensure!(schema_rows > 0, "expected at least one schema row");

    let table_rows = count_reader_rows(client.connection().get_objects(
        ObjectDepth::Tables,
        Some("swanlake"),
        Some("main"),
        Some(TABLE_NAME),
        None,
        None,
    )?)?;
    ensure!(
        table_rows > 0,
        "expected table metadata rows for {TABLE_NAME}"
    );

    let table_schema = client
        .connection()
        .get_table_schema(None, None, TABLE_NAME)
        .context("failed to load table schema via ADBC metadata API")?;
    let field_names = table_schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    ensure!(
        field_names == vec!["id", "name"],
        "unexpected table schema field names: {field_names:?}"
    );

    Ok(())
}

fn run_flight_sql_metadata_checks(endpoint: &str) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime for Flight SQL metadata checks")?;

    runtime.block_on(async move {
        let endpoint = normalize_endpoint_for_tonic(endpoint)?;
        let channel = Endpoint::from_shared(endpoint.clone())
            .with_context(|| format!("invalid endpoint URI for metadata checks: {endpoint}"))?
            .connect()
            .await
            .with_context(|| format!("failed to connect metadata Flight SQL client: {endpoint}"))?;

        let mut client = FlightSqlServiceClient::new(channel);

        let handshake_payload = client
            .handshake("swanlake", "swanlake")
            .await
            .context("handshake request failed")?;
        ensure!(
            handshake_payload.is_empty(),
            "expected empty handshake payload from server"
        );

        let catalogs_info = client.get_catalogs().await.context("get_catalogs failed")?;
        let catalogs_rows = consume_flight_info_rows(&mut client, catalogs_info).await?;
        ensure!(
            catalogs_rows > 0,
            "expected at least one catalog from Flight SQL"
        );

        let schemas_info = client
            .get_db_schemas(CommandGetDbSchemas {
                catalog: Some("swanlake".to_string()),
                db_schema_filter_pattern: None,
            })
            .await
            .context("get_db_schemas failed")?;
        let schemas_rows = consume_flight_info_rows(&mut client, schemas_info).await?;
        ensure!(
            schemas_rows > 0,
            "expected at least one schema from Flight SQL"
        );

        let tables_info = client
            .get_tables(CommandGetTables {
                catalog: Some("swanlake".to_string()),
                db_schema_filter_pattern: Some("main".to_string()),
                table_name_filter_pattern: Some(TABLE_NAME.to_string()),
                table_types: Vec::new(),
                include_schema: true,
            })
            .await
            .context("get_tables failed")?;
        let tables_rows = consume_flight_info_rows(&mut client, tables_info).await?;
        ensure!(
            tables_rows > 0,
            "expected table rows from Flight SQL metadata"
        );

        let table_types_info = client
            .get_table_types()
            .await
            .context("get_table_types failed")?;
        let table_types_rows = consume_flight_info_rows(&mut client, table_types_info).await?;
        ensure!(
            table_types_rows >= 2,
            "expected at least two table type rows from Flight SQL"
        );

        let primary_info = client
            .get_primary_keys(CommandGetPrimaryKeys {
                catalog: Some("swanlake".to_string()),
                db_schema: Some("main".to_string()),
                table: TABLE_NAME.to_string(),
            })
            .await
            .context("get_primary_keys failed")?;
        let primary_rows = consume_flight_info_rows(&mut client, primary_info).await?;
        ensure!(
            primary_rows == 0,
            "table has no primary key; expected empty metadata"
        );

        let exported_info = client
            .get_exported_keys(CommandGetExportedKeys {
                catalog: Some("swanlake".to_string()),
                db_schema: Some("main".to_string()),
                table: TABLE_NAME.to_string(),
            })
            .await
            .context("get_exported_keys failed")?;
        let exported_rows = consume_flight_info_rows(&mut client, exported_info).await?;
        ensure!(
            exported_rows == 0,
            "table has no referencing foreign keys; expected empty metadata"
        );

        let imported_info = client
            .get_imported_keys(CommandGetImportedKeys {
                catalog: Some("swanlake".to_string()),
                db_schema: Some("main".to_string()),
                table: TABLE_NAME.to_string(),
            })
            .await
            .context("get_imported_keys failed")?;
        let imported_rows = consume_flight_info_rows(&mut client, imported_info).await?;
        ensure!(
            imported_rows == 0,
            "table has no imported foreign keys; expected empty metadata"
        );

        let cross_ref_info = client
            .get_cross_reference(CommandGetCrossReference {
                pk_catalog: Some("swanlake".to_string()),
                pk_db_schema: Some("main".to_string()),
                pk_table: TABLE_NAME.to_string(),
                fk_catalog: Some("swanlake".to_string()),
                fk_db_schema: Some("main".to_string()),
                fk_table: TABLE_NAME.to_string(),
            })
            .await
            .context("get_cross_reference failed")?;
        let cross_ref_rows = consume_flight_info_rows(&mut client, cross_ref_info).await?;
        ensure!(
            cross_ref_rows == 0,
            "expected no cross-reference rows for unrelated table pair"
        );

        Ok(())
    })
}

fn setup_metadata_fixture(client: &mut FlightSQLClient) -> Result<()> {
    cleanup_metadata_fixture(client)?;
    client.update(&format!(
        "CREATE TABLE swanlake.{TABLE_NAME} (id INTEGER, name VARCHAR)"
    ))?;
    client.update(&format!(
        "INSERT INTO swanlake.{TABLE_NAME} VALUES (1, 'alpha'), (2, 'beta')"
    ))?;
    client.update(&format!(
        "CREATE VIEW swanlake.{VIEW_NAME} AS SELECT id, name FROM swanlake.{TABLE_NAME}"
    ))?;
    Ok(())
}

fn cleanup_metadata_fixture(client: &mut FlightSQLClient) -> Result<()> {
    client.update(&format!("DROP VIEW IF EXISTS swanlake.{VIEW_NAME}"))?;
    client.update(&format!("DROP TABLE IF EXISTS swanlake.{TABLE_NAME}"))?;
    Ok(())
}

async fn consume_flight_info_rows(
    client: &mut FlightSqlServiceClient<Channel>,
    info: FlightInfo,
) -> Result<usize> {
    let endpoint = info
        .endpoint
        .first()
        .context("metadata FlightInfo did not contain any endpoints")?;
    let ticket = endpoint
        .ticket
        .clone()
        .context("metadata FlightInfo endpoint missing ticket")?;

    let mut stream = client
        .do_get(ticket)
        .await
        .context("metadata DoGet request failed")?;
    let mut total_rows = 0usize;

    while let Some(batch) = stream.next().await {
        let batch = batch.context("metadata DoGet stream returned an error")?;
        total_rows = total_rows.saturating_add(batch.num_rows());
    }

    Ok(total_rows)
}

fn count_reader_rows<R>(reader: R) -> Result<usize>
where
    R: RecordBatchReader,
{
    let mut total_rows = 0usize;
    for batch in reader {
        total_rows = total_rows.saturating_add(batch?.num_rows());
    }
    Ok(total_rows)
}

fn normalize_endpoint_for_tonic(endpoint: &str) -> Result<String> {
    if let Some(rest) = endpoint.strip_prefix("grpc://") {
        return Ok(format!("http://{rest}"));
    }
    if let Some(rest) = endpoint.strip_prefix("grpc+tls://") {
        return Ok(format!("https://{rest}"));
    }
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        return Ok(endpoint.to_string());
    }

    Err(anyhow!(
        "unsupported endpoint scheme for metadata checks: {endpoint}"
    ))
}
