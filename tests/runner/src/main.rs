use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Once;

use anyhow::{anyhow, bail, Context, Result};
use arrow_array::Array;
use flight_sql_client::{arrow::array_value_to_string, FlightSQLClient};
use tracing::info;

mod scenarios;

#[derive(Clone, Debug)]
struct CliArgs {
    endpoint: String,
    test_files: Vec<PathBuf>,
    vars: HashMap<String, String>,
    test_dir: Option<String>,
}

#[derive(Debug)]
enum RecordKind {
    Statement { expect_error: bool },
    Query {
        expect_error: bool,
        expected_rows: Vec<String>,
    },
}

#[derive(Debug)]
struct TestRecord {
    kind: RecordKind,
    sql: String,
}

fn parse_args<I: IntoIterator<Item = String>>(args_iter: I) -> Result<CliArgs> {
    let mut args = args_iter.into_iter();
    let mut endpoint = String::from("grpc://127.0.0.1:4214");
    let mut vars = HashMap::new();
    let mut test_files = Vec::new();
    let mut test_dir: Option<String> = Some("target/ducklake-tests".to_string());

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--endpoint" | "-e" => {
                endpoint = args.next().context("expected value after --endpoint")?;
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
        let root = PathBuf::from("tests/sql");
        for entry in root
            .read_dir()
            .with_context(|| format!("failed to read {}", root.display()))?
        {
            let entry = entry?;
            if entry.file_type()?.is_file() && entry.path().extension().and_then(|e| e.to_str()) == Some("test") {
                test_files.push(entry.path());
            }
        }
    }

    Ok(CliArgs {
        endpoint,
        test_files,
        vars,
        test_dir,
    })
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
        "Running SQL tests with {} script(s)",
        args.test_files.len()
    );
    run_sql_tests(&args).await?;
    scenarios::run_all(&args).await?;
    Ok(())
}

async fn run_sql_tests(args: &CliArgs) -> Result<()> {
    let test_dir = args
        .test_dir
        .as_ref()
        .context("--test-dir is required for SQL tests")?;

    let mut substitutions = args.vars.clone();
    substitutions.insert("__TEST_DIR__".to_string(), test_dir.clone());

    for path in &args.test_files {
        info!("Executing script {}", path.display());
        let records = parse_test_file(path)?;
        execute_records(&args.endpoint, &substitutions, &records)
            .await
            .with_context(|| format!("failed while running {}", path.display()))?;
        info!("Script {} completed successfully", path.display());
    }
    Ok(())
}

fn parse_test_file(path: &Path) -> Result<Vec<TestRecord>> {
    let file = File::open(path).with_context(|| format!("failed to read {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let mut buf = String::new();
    let mut records = Vec::new();

    while reader.read_line(&mut buf)? != 0 {
        let line = buf.trim_end().to_string();
        buf.clear();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with("require ") {
            continue;
        }

        if line.starts_with("statement") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            let expect_error = parts.get(1).is_some_and(|p| *p == "error");
            let mut sql_lines = Vec::new();
            while reader.read_line(&mut buf)? != 0 {
                let line_owned = buf.trim_end().to_string();
                buf.clear();
                if line_owned.is_empty() {
                    break;
                }
                sql_lines.push(line_owned);
            }
            records.push(TestRecord {
                kind: RecordKind::Statement { expect_error },
                sql: sql_lines.join("\n"),
            });
            continue;
        }

        if line.starts_with("query") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 2 {
                bail!("invalid query header: {line}");
            }
            let expect_error = parts[1] == "error";

            let mut sql_lines = Vec::new();
            while reader.read_line(&mut buf)? != 0 {
                let line_owned = buf.trim_end().to_string();
                buf.clear();
                if line_owned == "----" {
                    break;
                }
                sql_lines.push(line_owned);
            }

            let mut expected_rows = Vec::new();
            while reader.read_line(&mut buf)? != 0 {
                let line_owned = buf.trim_end().to_string();
                buf.clear();
                if line_owned.is_empty() {
                    break;
                }
                // Use tabs as column separators when present; otherwise split on whitespace.
                if line_owned.contains('\t') {
                    expected_rows.push(line_owned);
                } else {
                    expected_rows.push(
                        line_owned
                            .split_whitespace()
                            .collect::<Vec<_>>()
                            .join("\t"),
                    );
                }
            }

            records.push(TestRecord {
                kind: RecordKind::Query {
                    expect_error,
                    expected_rows,
                },
                sql: sql_lines.join("\n"),
            });
            continue;
        }

        bail!("unknown directive: {line}");
    }

    Ok(records)
}

async fn execute_records(
    endpoint: &str,
    substitutions: &HashMap<String, String>,
    records: &[TestRecord],
) -> Result<()> {
    let mut client = FlightSQLClient::connect(endpoint)
        .with_context(|| format!("failed to establish connection to {endpoint}"))?;

    for rec in records {
        let sql = apply_substitutions(&rec.sql, substitutions);
        match &rec.kind {
            RecordKind::Statement { expect_error } => {
                let result = client.execute_update(&sql);
                if *expect_error {
                    if result.is_ok() {
                        bail!("expected error but statement succeeded: {sql}");
                    }
                } else {
                    result.with_context(|| format!("statement failed: {sql}"))?;
                }
            }
            RecordKind::Query {
                expect_error,
                expected_rows,
            } => {
                let result = client.execute(&sql);
                match (result, expect_error) {
                    (Err(_), true) => continue,
                    (Err(err), false) => {
                        return Err(anyhow!("query failed: {err} for sql: {sql}"));
                    }
                    (Ok(_res), true) => bail!("expected query to error but it succeeded: {sql}"),
                    (Ok(res), false) => {
                        let rows = collect_rows(&res)?;
                        let normalized: Vec<String> = rows
                            .into_iter()
                            .map(|r| r.join("\t"))
                            .collect();
                        if normalized != *expected_rows {
                            bail!(
                                "unexpected rows for sql {}: got {:?}, expected {:?}",
                                sql,
                                normalized,
                                expected_rows
                            );
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn apply_substitutions(sql: &str, subs: &HashMap<String, String>) -> String {
    let mut substituted = sql.to_owned();
    for (k, v) in subs {
        substituted = substituted.replace(k, v);
    }
    substituted
}

fn collect_rows(result: &flight_sql_client::QueryResult) -> Result<Vec<Vec<String>>> {
    let mut rows = Vec::new();
    for batch in &result.batches {
        let cols = batch.num_columns();
        for row_idx in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(cols);
            for col_idx in 0..cols {
                let col = batch.column(col_idx);
                if col.is_null(row_idx) {
                    row.push("NULL".to_string());
                } else {
                    row.push(format_value(col.as_ref(), row_idx)?);
                }
            }
            rows.push(row);
        }
    }
    Ok(rows)
}

fn format_value(array: &dyn Array, idx: usize) -> Result<String> {
    array_value_to_string(array, idx).map_err(|e| anyhow!(e))
}

fn init_logging() {
    static START: Once = Once::new();
    START.call_once(|| {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .init();
    });
}
