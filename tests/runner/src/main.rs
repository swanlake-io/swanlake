use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::{Arc, Once};

use adbc_core::{
    error::Status as AdbcStatus,
    options::{AdbcVersion, OptionDatabase, OptionValue},
    Connection,
    Database,
    Driver,
    Statement, // It's required for trait bounds
};
use adbc_driver_flightsql::DRIVER_PATH;
use adbc_driver_manager::{ManagedConnection, ManagedDriver};
use anyhow::{anyhow, bail, Context, Result};
#[allow(unused_imports)]
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray,
    RecordBatchReader, StringArray, TimestampMicrosecondArray, UInt16Array, UInt32Array,
    UInt64Array, UInt8Array,
};

use arrow_schema::DataType;
use async_trait::async_trait;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType, Runner};
use thiserror::Error;
use tracing::{info, warn};

mod scenarios;

#[derive(Debug, Error)]
enum RunnerError {
    #[error(transparent)]
    Source(#[from] anyhow::Error),
}

#[derive(Clone)]
struct FlightSqlDb {
    connection: Option<ManagedConnection>,
    substitutions: Arc<HashMap<String, String>>,
}

impl FlightSqlDb {
    async fn connect(endpoint: &str, substitutions: Arc<HashMap<String, String>>) -> Result<Self> {
        let uri = endpoint.trim().to_string();
        info!("Loading Flight SQL driver and connecting to {uri}");
        let driver_path = PathBuf::from(DRIVER_PATH);
        let mut driver =
            ManagedDriver::load_dynamic_from_filename(&driver_path, None, AdbcVersion::default())
                .map_err(|err| {
                anyhow!(
                    "failed to load Flight SQL driver from {}: {err}",
                    driver_path.display()
                )
            })?;

        let database = driver
            .new_database_with_opts([(OptionDatabase::Uri, OptionValue::from(uri.as_str()))])
            .map_err(|err| anyhow!("failed to create database handle: {err}"))?;
        let connection = database
            .new_connection()
            .map_err(|err| anyhow!("failed to create connection: {err}"))?;
        info!("Flight SQL connection established to {uri}");

        Ok(Self {
            connection: Some(connection),
            substitutions,
        })
    }

    fn apply_substitutions(&self, sql: &str) -> String {
        let mut substituted = sql.to_owned();
        for (key, value) in self.substitutions.iter() {
            substituted = substituted.replace(key, value);
        }
        substituted
    }

    async fn execute_statement(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>> {
        let substituted = self.apply_substitutions(sql);
        info!("Executing SQL: {substituted}");
        let conn = self
            .connection
            .as_mut()
            .context("database connection has been shut down")?;

        let mut statement = conn
            .new_statement()
            .map_err(|err| anyhow!("failed to create statement: {err}"))?;
        statement
            .set_sql_query(&substituted)
            .map_err(|err| anyhow!("failed to set SQL query: {err}"))?;

        let is_query = match statement.execute_schema() {
            Ok(schema) => !schema.fields().is_empty(),
            Err(err) => match err.status {
                AdbcStatus::NotImplemented | AdbcStatus::Unknown => {
                    infer_query_from_sql(&substituted)
                }
                _ => return Err(anyhow!("failed to inspect query schema: {err}")),
            },
        };
        info!(
            "Treating statement as {}",
            if is_query { "query" } else { "command" }
        );

        if is_query {
            let reader = statement
                .execute()
                .map_err(|err| anyhow!("failed to execute query: {err}"))?;
            let schema = reader.schema();

            let mut rows = Vec::new();
            let mut row_count = 0usize;
            for batch in reader {
                let batch = batch.map_err(|err| anyhow!("failed to read result batch: {err}"))?;
                let column_count = batch.num_columns();
                for row_idx in 0..batch.num_rows() {
                    let mut row = Vec::with_capacity(column_count);
                    for col_idx in 0..column_count {
                        let column = batch.column(col_idx);
                        if column.is_null(row_idx) {
                            row.push("NULL".to_string());
                        } else {
                            row.push(array_value_to_string(column.as_ref(), row_idx)?);
                        }
                    }
                    rows.push(row);
                    row_count += 1;
                }
            }
            info!("Query completed with {row_count} row(s) returned");

            let types = schema
                .fields()
                .iter()
                .map(|field| column_type_from_arrow(field.data_type()))
                .collect();
            Ok(DBOutput::Rows { types, rows })
        } else {
            let affected = statement
                .execute_update()
                .map_err(|err| anyhow!("failed to execute statement: {err}"))?;
            let affected = affected
                .and_then(|value| value.try_into().ok())
                .unwrap_or(0);
            info!("Command completed with {affected} row(s) affected");
            Ok(DBOutput::StatementComplete(affected))
        }
    }
}

fn infer_query_from_sql(sql: &str) -> bool {
    let trimmed = sql.trim_start();
    if trimmed.is_empty() {
        return false;
    }
    let first_token = trimmed
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    matches!(
        first_token.as_str(),
        "select" | "with" | "show" | "describe" | "explain" | "values"
    )
}

#[async_trait]
impl AsyncDB for FlightSqlDb {
    type Error = RunnerError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        self.execute_statement(sql).await.map_err(RunnerError::from)
    }

    async fn shutdown(&mut self) {
        if self.connection.take().is_some() {
            info!("Flight SQL connection closed");
        } else {
            warn!("Flight SQL connection already closed");
        }
    }

    fn engine_name(&self) -> &str {
        "swanlake"
    }
}

pub struct CliArgs {
    endpoint: String,
    test_files: Vec<PathBuf>,
    labels: Vec<String>,
    vars: HashMap<String, String>,
    test_dir: Option<String>,
}

impl CliArgs {
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn test_dir(&self) -> Option<&str> {
        self.test_dir.as_deref()
    }

    pub fn test_files(&self) -> &[PathBuf] {
        &self.test_files
    }
}

fn parse_args<I: IntoIterator<Item = String>>(args_iter: I) -> Result<CliArgs> {
    let mut args = args_iter.into_iter();
    let mut endpoint = String::from("grpc://127.0.0.1:4214");
    let mut labels = Vec::new();
    let mut vars = HashMap::new();
    let mut test_files = Vec::new();
    let mut test_dir: Option<String> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--endpoint" | "-e" => {
                endpoint = args.next().context("expected value after --endpoint")?;
            }
            "--label" | "-l" => {
                let label = args.next().context("expected value after --label")?;
                labels.push(label);
            }
            "--var" => {
                let raw = args.next().context("expected KEY=VALUE after --var")?;
                let (key, value) = raw
                    .split_once('=')
                    .ok_or_else(|| anyhow!("--var expects KEY=VALUE, got {raw}"))?;
                vars.insert(format!("__{key}__"), value.to_string());
            }
            "--test-dir" => {
                let dir = args.next().context("expected value after --test-dir")?;
                test_dir = Some(dir);
            }
            "--help" | "-h" => {
                bail!("help requested");
            }
            other if other.starts_with('-') => {
                bail!("unknown argument: {other}");
            }
            path => test_files.push(PathBuf::from(path)),
        }
    }

    if test_files.is_empty() {
        bail!("no test files provided");
    }

    Ok(CliArgs {
        endpoint,
        test_files,
        labels,
        vars,
        test_dir,
    })
}

fn load_script_without_requires(path: &Path) -> Result<String> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let lines: Vec<&str> = raw.lines().collect();
    let mut out = String::with_capacity(raw.len());
    let had_trailing_newline = raw.ends_with('\n');

    for (idx, line) in lines.iter().enumerate() {
        let trimmed = line.trim_start();
        if trimmed.starts_with("require ") {
            let indent_len = line.len() - trimmed.len();
            if indent_len > 0 {
                out.push_str(&line[..indent_len]);
            }
            out.push_str("# ");
            out.push_str(trimmed);
        } else {
            out.push_str(line);
        }

        if idx + 1 < lines.len() || had_trailing_newline {
            out.push('\n');
        }
    }

    if lines.is_empty() && had_trailing_newline {
        out.push('\n');
    }

    Ok(out)
}

fn column_type_from_arrow(data_type: &DataType) -> DefaultColumnType {
    match data_type {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Date32
        | DataType::Date64
        | DataType::Interval(_) => DefaultColumnType::Integer,
        DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => DefaultColumnType::FloatingPoint,
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)
        | DataType::Boolean => DefaultColumnType::Text,
        _ => DefaultColumnType::Any,
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    init_logging();

    if let Err(err) = run_entrypoint().await {
        eprintln!("runner failed: {err:?}");
        process::exit(1);
    }

    info!("Runner completed successfully, exiting process");
    process::exit(0);
}

async fn run_entrypoint() -> Result<()> {
    let raw_args: Vec<String> = env::args().skip(1).collect();
    let args = parse_args(raw_args)?;

    if args.test_dir.is_none() && scenarios::requires_test_dir(&args) {
        bail!("--test-dir is required");
    }

    info!(
        "Running SQLLogicTest with {} script(s)",
        args.test_files.len()
    );
    run_sqllogictest(&args).await?;
    scenarios::run_all(&args).await
}

async fn run_sqllogictest(args: &CliArgs) -> Result<()> {
    for test_file in &args.test_files {
        info!("Preparing script {}", test_file.display());
        let test_dir = args.test_dir.as_ref().unwrap().clone();
        let mut substitutions = args.vars.clone();
        substitutions.insert("__TEST_DIR__".to_string(), test_dir.clone());

        let substitutions = Arc::new(substitutions);
        let endpoint = args.endpoint.clone();

        let mut runner = Runner::new({
            let substitutions = substitutions.clone();
            let endpoint = endpoint.clone();
            move || {
                let substitutions = substitutions.clone();
                let endpoint = endpoint.clone();
                async move {
                    FlightSqlDb::connect(&endpoint, substitutions)
                        .await
                        .map_err(RunnerError::from)
                }
            }
        });

        for label in &args.labels {
            runner.add_label(label);
        }
        runner.set_var("__TEST_DIR__".to_string(), test_dir.clone());
        for (key, value) in substitutions.iter() {
            runner.set_var(key.clone(), value.clone());
        }

        let script = load_script_without_requires(test_file)?;
        info!("Executing SQLLogicTest script {}", test_file.display());

        runner
            .run_script_with_name_async(&script, test_file.display().to_string())
            .await
            .with_context(|| format!("failed while running {}", test_file.display()))?;
        info!("Script {} completed successfully", test_file.display());
        runner.shutdown_async().await;
        info!("Runner shutdown complete for {}", test_file.display());
    }

    info!("All SQLLogicTest scripts completed");

    Ok(())
}

fn array_value_to_string(column: &dyn Array, row_idx: usize) -> Result<String> {
    use arrow_array::*;
    use arrow_schema::DataType;

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
            let days = arr.value(row_idx);
            // Date32 is days since Unix epoch (1970-01-01)
            let date = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .checked_add_signed(chrono::TimeDelta::days(days as i64))
                .unwrap();
            Ok(date.format("%Y-%m-%d").to_string())
        }
        DataType::Date64 => {
            let arr = column.as_any().downcast_ref::<Date64Array>().unwrap();
            let millis = arr.value(row_idx);
            // Date64 is milliseconds since Unix epoch
            let secs = millis / 1000;
            let date = chrono::DateTime::from_timestamp(secs, 0)
                .unwrap()
                .date_naive();
            Ok(date.format("%Y-%m-%d").to_string())
        }
        DataType::Timestamp(_, _) => {
            let arr = column
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap();
            Ok(arr.value(row_idx).to_string())
        }
        _ => Ok(format!("{:?}", column)),
    }
}

fn init_logging() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_target(false)
            .compact()
            .try_init();
    });
}
