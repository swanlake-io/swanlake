use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::{Arc, Once};

use anyhow::{anyhow, bail, Context, Result};
#[allow(unused_imports)]
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, StringArray,
    TimestampMicrosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};

use arrow_schema::DataType;
use async_trait::async_trait;
use flight_sql_client::{arrow::array_value_to_string, FlightSQLClient, StatementResult};
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType, MakeConnection, Runner};
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
    client: Option<FlightSQLClient>,
    substitutions: Arc<HashMap<String, String>>,
}

impl FlightSqlDb {
    async fn connect(endpoint: &str, substitutions: Arc<HashMap<String, String>>) -> Result<Self> {
        let uri = endpoint.trim().to_string();
        info!("Connecting to Flight SQL endpoint {uri}");
        let client = FlightSQLClient::connect(&uri)
            .with_context(|| format!("failed to establish connection to {uri}"))?;
        info!("Flight SQL connection established to {uri}");

        Ok(Self {
            client: Some(client),
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
            .client
            .as_mut()
            .context("database connection has been shut down")?;

        match conn.run_statement(&substituted)? {
            StatementResult::Query { schema, batches } => {
                let mut rows = Vec::new();
                let mut row_count = 0usize;
                for batch in batches {
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
            }
            StatementResult::Command { rows_affected } => {
                let affected = rows_affected.unwrap_or(0).max(0) as u64;
                info!("Command completed with {affected} row(s) affected");
                Ok(DBOutput::StatementComplete(affected))
            }
        }
    }
}

#[async_trait]
impl AsyncDB for FlightSqlDb {
    type Error = RunnerError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        self.execute_statement(sql).await.map_err(RunnerError::from)
    }

    async fn shutdown(&mut self) {
        if self.client.take().is_some() {
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
    let mut test_dir: Option<String> = Some("target/ducklake-tests".to_string());

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

    Ok(CliArgs {
        endpoint,
        test_files,
        labels,
        vars,
        test_dir,
    })
}

fn load_script_without_requires(path: &Path) -> Result<String> {
    let file = File::open(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut buffer = String::new();
    let mut output = String::new();

    while reader.read_line(&mut buffer)? != 0 {
        let trimmed = buffer.trim_start();
        if trimmed.starts_with("require ") {
            let indent_len = buffer.len() - trimmed.len();
            if indent_len > 0 {
                output.push_str(&buffer[..indent_len]);
            }
            output.push_str("# ");
            output.push_str(trimmed);
            if !trimmed.ends_with('\n') {
                output.push('\n');
            }
        } else {
            output.push_str(&buffer);
        }
        buffer.clear();
    }

    Ok(output)
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

    info!(
        "Running SQLLogicTest with {} script(s)",
        args.test_files.len()
    );
    run_sqllogictest(&args).await?;
    scenarios::run_all(&args).await?;
    Ok(())
}

fn build_runner(
    endpoint: &str,
    substitutions: Arc<HashMap<String, String>>,
    labels: &[String],
) -> Runner<FlightSqlDb, impl MakeConnection<Conn = FlightSqlDb>> {
    let endpoint = endpoint.to_string();
    let mut runner = Runner::new({
        let endpoint = endpoint.clone();
        let substitutions = substitutions.clone();
        move || {
            let endpoint = endpoint.clone();
            let substitutions = substitutions.clone();
            async move {
                FlightSqlDb::connect(&endpoint, substitutions)
                    .await
                    .map_err(RunnerError::from)
            }
        }
    });

    for label in labels {
        runner.add_label(label);
    }

    runner
}

async fn run_sqllogictest(args: &CliArgs) -> Result<()> {
    let test_dir = args
        .test_dir()
        .context("--test-dir is required for SQLLogicTest")?
        .to_string();

    for test_file in &args.test_files {
        info!("Preparing script {}", test_file.display());
        let mut substitutions = args.vars.clone();
        substitutions.insert("__TEST_DIR__".to_string(), test_dir.clone());
        let substitutions = Arc::new(substitutions);

        let mut runner = build_runner(args.endpoint(), substitutions.clone(), &args.labels);
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

fn init_logging() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_target(false)
            .compact()
            .try_init();
    });
}
