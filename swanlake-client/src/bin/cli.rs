//! SwanLake CLI - Interactive SQL client for SwanLake Flight SQL servers

use anyhow::{Context, Result};
use arrow_array::{Array, RecordBatch};
use clap::Parser;
use comfy_table::{
    modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL, Cell, CellAlignment, Color,
    ContentArrangement, Table,
};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::time::Instant;
use swanlake_client::arrow::array_value_to_string;
use swanlake_client::FlightSQLClient;

/// SwanLake CLI - Interactive SQL client
#[derive(Parser, Debug)]
#[command(name = "swanlake-cli")]
#[command(about = "Interactive SQL client for SwanLake Flight SQL servers", long_about = None)]
struct Args {
    /// SwanLake server endpoint
    #[arg(short, long, default_value = "grpc://localhost:4214")]
    endpoint: String,

    /// SQL query to execute (if provided, runs in non-interactive mode)
    #[arg(short, long)]
    query: Option<String>,

    /// Enable debug logging
    #[arg(short, long)]
    debug: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    let log_level = if args.debug { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level)),
        )
        .compact()
        .init();

    // Connect to server
    println!("Connecting to SwanLake at {}...", args.endpoint);
    let mut client =
        FlightSQLClient::connect(&args.endpoint).context("Failed to connect to SwanLake server")?;
    println!("Connected successfully!\n");

    // Execute query or start interactive mode
    if let Some(query) = args.query {
        execute_and_display(&mut client, &query)?;
    } else {
        interactive_mode(&mut client, args.debug)?;
    }

    Ok(())
}

fn interactive_mode(client: &mut FlightSQLClient, debug: bool) -> Result<()> {
    println!("SwanLake Interactive SQL Shell");
    println!("Type your SQL queries and press Enter. Type 'exit' or 'quit' to exit.");
    println!("Press Ctrl-C twice to exit.\n");

    let mut rl = DefaultEditor::new()?;
    let history_file = dirs::home_dir()
        .map(|mut p| {
            p.push(".swanlake_history");
            p
        })
        .unwrap_or_default();

    // Load history
    if history_file.exists() {
        let _ = rl.load_history(&history_file);
    }

    let mut interrupt_count = 0;

    loop {
        let readline = rl.readline("swanlake> ");
        match readline {
            Ok(line) => {
                // Reset interrupt count on successful input
                interrupt_count = 0;

                let query = line.trim();

                if query.is_empty() {
                    continue;
                }

                // Add to history
                let _ = rl.add_history_entry(query);

                // Check for exit commands
                if query.eq_ignore_ascii_case("exit") || query.eq_ignore_ascii_case("quit") {
                    println!("Goodbye!");
                    break;
                }

                // Execute query
                if let Err(e) = execute_and_display(client, query) {
                    eprintln!("Error: {}", e);
                    if debug {
                        eprintln!("Details: {:?}", e);
                    }
                }
                println!();
            }
            Err(ReadlineError::Interrupted) => {
                interrupt_count += 1;
                if interrupt_count >= 2 {
                    println!("\nGoodbye!");
                    break;
                } else {
                    println!("^C (press Ctrl-C again to exit)");
                    continue;
                }
            }
            Err(ReadlineError::Eof) => {
                println!("Goodbye!");
                break;
            }
            Err(err) => {
                eprintln!("Error reading input: {}", err);
                break;
            }
        }
    }

    // Save history
    if !history_file.as_os_str().is_empty() {
        let _ = rl.save_history(&history_file);
    }

    Ok(())
}

fn execute_and_display(client: &mut FlightSQLClient, query: &str) -> Result<()> {
    let start = Instant::now();

    // Detect if it's a query or update statement
    let is_query = is_query_statement(query);

    if is_query {
        let result = client.execute(query)?;
        let elapsed = start.elapsed();

        if result.is_empty() {
            println!("(No rows returned)");
        } else {
            display_results(&result.batches)?;
        }

        println!(
            "{} row{} in {:.3}s",
            result.total_rows,
            if result.total_rows == 1 { "" } else { "s" },
            elapsed.as_secs_f64()
        );
    } else {
        let result = client.execute_update(query)?;
        let elapsed = start.elapsed();

        if let Some(rows) = result.rows_affected {
            println!("{} row{} affected", rows, if rows == 1 { "" } else { "s" });
        } else {
            println!("Query executed successfully");
        }

        println!("{:.3}s", elapsed.as_secs_f64());
    }

    Ok(())
}

fn is_query_statement(sql: &str) -> bool {
    // Strip comments and whitespace
    let normalized = sql
        .lines()
        .map(|line| {
            if let Some(pos) = line.find("--") {
                &line[..pos]
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_uppercase();

    // Check first keyword
    normalized.starts_with("SELECT")
        || normalized.starts_with("WITH")
        || normalized.starts_with("SHOW")
        || normalized.starts_with("DESCRIBE")
        || normalized.starts_with("DESC")
        || normalized.starts_with("EXPLAIN")
}

fn display_results(batches: &[RecordBatch]) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }

    let schema = batches[0].schema();
    let mut table = Table::new();

    // Configure table appearance
    table
        .load_preset(UTF8_FULL)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_content_arrangement(ContentArrangement::Dynamic);

    // Add header row
    let mut header_cells = Vec::new();
    for field in schema.fields() {
        header_cells.push(
            Cell::new(field.name())
                .fg(Color::Cyan)
                .set_alignment(CellAlignment::Center),
        );
    }
    table.set_header(header_cells);

    // Add data rows
    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let mut row_cells = Vec::new();
            for col_idx in 0..batch.num_columns() {
                let column = batch.column(col_idx);
                let value = format_cell_value(column.as_ref(), row_idx)?;
                row_cells.push(Cell::new(value));
            }
            table.add_row(row_cells);
        }
    }

    println!("{}", table);
    Ok(())
}

fn format_cell_value(column: &dyn Array, row_idx: usize) -> Result<String> {
    array_value_to_string(column, row_idx)
}
