use anyhow::{Context, Result};
use swanlake_client::FlightSQLClient;
use tracing::info;

use crate::CliArgs;

/// Concurrent session creation test (TOCTOU fix, issue #129)
///
/// Spawns multiple threads that each open a connection and execute `SELECT 1`
/// concurrently, verifying that the session registry handles parallel creation
/// without race conditions.
pub fn run_concurrent_sessions(args: &CliArgs) -> Result<()> {
    info!("Running concurrent session creation tests");

    let num_threads = 10;
    let endpoint = args.endpoint.clone();

    let handles: Vec<_> = (0..num_threads)
        .map(|i| {
            let ep = endpoint.clone();
            std::thread::spawn(move || -> Result<()> {
                let mut client = FlightSQLClient::connect(&ep)
                    .with_context(|| format!("thread {i}: failed to connect"))?;
                let result = client.execute("SELECT 1")?;
                anyhow::ensure!(
                    result.total_rows == 1,
                    "thread {i}: expected 1 row from SELECT 1, got {}",
                    result.total_rows
                );
                info!("thread {i}: connected and queried successfully");
                Ok(())
            })
        })
        .collect();

    for (i, handle) in handles.into_iter().enumerate() {
        handle
            .join()
            .map_err(|_| anyhow::anyhow!("thread {i} panicked"))?
            .with_context(|| format!("thread {i} returned an error"))?;
    }

    info!("Concurrent session creation tests completed successfully ({num_threads} threads)");
    Ok(())
}
