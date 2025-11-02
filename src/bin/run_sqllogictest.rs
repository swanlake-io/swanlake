use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use arrow_array::Array;
use arrow_cast::display::array_value_to_string;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_schema::DataType;
use async_trait::async_trait;
use futures::StreamExt;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType, Runner};
use thiserror::Error;
use tonic::transport::{Channel, Endpoint};

#[derive(Debug, Error)]
enum RunnerError {
    #[error(transparent)]
    Source(#[from] anyhow::Error),
}

#[derive(Clone)]
struct FlightSqlDb {
    client: FlightSqlServiceClient<Channel>,
    substitutions: Arc<HashMap<String, String>>,
}

impl FlightSqlDb {
    async fn connect(endpoint: &str, substitutions: Arc<HashMap<String, String>>) -> Result<Self> {
        let channel = Endpoint::from_shared(endpoint.to_string())?
            .connect()
            .await
            .context("failed to connect to Flight SQL endpoint")?;
        let client = FlightSqlServiceClient::new(channel);
        Ok(Self {
            client,
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

        // Step 1: Create prepared statement to detect query vs statement
        let prepared = self
            .client
            .prepare(substituted.clone(), None)
            .await
            .map_err(|err| anyhow!("Flight SQL prepare failed: {}", err))?;

        // Step 2: Check if it's a query or statement based on schema
        let schema = prepared
            .dataset_schema()
            .context("failed to get dataset schema")?;
        let is_query = !schema.fields().is_empty();

        if is_query {
            // Query path: use GetFlightInfo + DoGet
            let mut info = self
                .client
                .execute(substituted.clone(), None)
                .await
                .map_err(|err| anyhow!("Flight SQL execute failed: {}", err))?;
            let schema = info
                .clone()
                .try_decode_schema()
                .context("failed to decode result schema")?;

            let mut rows = Vec::new();
            let mut _total_rows: u64 = 0;
            for endpoint in info.endpoint.drain(..) {
                if let Some(ticket) = endpoint.ticket {
                    let mut stream = self
                        .client
                        .do_get(ticket)
                        .await
                        .map_err(|err| anyhow!("Flight SQL do_get failed: {}", err))?;
                    while let Some(batch) = stream
                        .next()
                        .await
                        .transpose()
                        .map_err(|err| anyhow!("error reading result batch: {}", err))?
                    {
                        let column_count = batch.num_columns();
                        for row_idx in 0..batch.num_rows() {
                            let mut row = Vec::with_capacity(column_count);
                            for col_idx in 0..column_count {
                                let column = batch.column(col_idx);
                                if column.is_null(row_idx) {
                                    row.push("NULL".to_string());
                                } else {
                                    row.push(
                                        array_value_to_string(column.as_ref(), row_idx)
                                            .context("failed to render value")?,
                                    );
                                }
                            }
                            _total_rows += 1;
                            rows.push(row);
                        }
                    }
                }
            }

            let types = schema
                .fields()
                .iter()
                .map(|field| column_type_from_arrow(field.data_type()))
                .collect();
            Ok(DBOutput::Rows { types, rows })
        } else {
            // Statement path: use DoPut for updates/DDL
            let affected_rows = self
                .client
                .execute_update(substituted.clone(), None)
                .await
                .map_err(|err| anyhow!("Flight SQL execute_update failed: {}", err))?;

            Ok(DBOutput::StatementComplete(affected_rows as u64))
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
        let _ = self.client.close().await;
    }

    fn engine_name(&self) -> &str {
        "swandb"
    }
}

struct CliArgs {
    endpoint: String,
    test_files: Vec<PathBuf>,
    labels: Vec<String>,
    vars: HashMap<String, String>,
}

fn parse_args() -> Result<CliArgs> {
    let mut args = std::env::args().skip(1);
    let mut endpoint = String::from("http://127.0.0.1:4214");
    let mut labels = Vec::new();
    let mut vars = HashMap::new();
    let mut test_files = Vec::new();

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
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
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
    })
}

fn print_usage() {
    eprintln!(
        "Usage: run_sqllogictest [--endpoint <url>] [--label <name>] [--var KEY=VALUE] <test-file>..."
    );
}

fn resolve_test_dir(path: &Path) -> Result<String> {
    let dir = path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let canonical = dir
        .canonicalize()
        .with_context(|| format!("failed to canonicalize {}", dir.display()))?;
    canonical
        .to_str()
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow!("test directory contains invalid UTF-8"))
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

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args()?;

    for test_file in &args.test_files {
        let test_dir = resolve_test_dir(test_file)?;
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

        runner
            .run_script_with_name_async(&script, test_file.display().to_string())
            .await
            .with_context(|| format!("failed while running {}", test_file.display()))?;
        runner.shutdown_async().await;
    }

    Ok(())
}
