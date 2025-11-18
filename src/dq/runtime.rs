use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::dq::config::Settings;
use crate::dq::coordinator::{DqCoordinator, FlushPayload};
use crate::dq::schema::{duckdb_columns, quote_ident};
use crate::engine::EngineFactory;
use crate::error::ServerError;

/// Background runtime that receives flush payloads and writes them into DuckLake.
pub struct QueueRuntime;

impl QueueRuntime {
    /// Bootstrap the runtime and return the coordinator alongside it.
    pub fn bootstrap(
        factory: Arc<Mutex<EngineFactory>>,
        settings: Settings,
    ) -> (Arc<DqCoordinator>, Arc<Self>) {
        let (tx, rx) = mpsc::unbounded_channel::<FlushPayload>();
        let coordinator = Arc::new(DqCoordinator::new(settings.clone(), tx));
        let runtime = Arc::new(Self);
        spawn_flush_workers(factory, settings.clone(), coordinator.clone(), rx);
        spawn_age_sweeper(coordinator.clone(), settings.flush_interval);
        (coordinator, runtime)
    }
}

fn spawn_age_sweeper(coordinator: Arc<DqCoordinator>, interval: Duration) {
    let mut ticker = tokio::time::interval(interval.max(Duration::from_secs(1)));
    tokio::spawn(async move {
        loop {
            ticker.tick().await;
            coordinator.flush_stale_buffers();
        }
    });
}

fn spawn_flush_workers(
    factory: Arc<Mutex<EngineFactory>>,
    settings: Settings,
    coordinator: Arc<DqCoordinator>,
    mut rx: UnboundedReceiver<FlushPayload>,
) {
    let semaphore = Arc::new(Semaphore::new(settings.max_parallel_flushes));
    tokio::spawn(async move {
        while let Some(payload) = rx.recv().await {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let factory_cloned = factory.clone();
            let coordinator_cloned = coordinator.clone();
            let settings_clone = settings.clone();
            let retry_payload = payload.clone();
            tokio::spawn(async move {
                let res = tokio::task::spawn_blocking(move || {
                    flush_payload(factory_cloned, &settings_clone, payload)
                })
                .await;

                match res {
                    Ok(Ok(())) => {
                        debug!("duckling queue flush task completed");
                    }
                    Ok(Err(err)) => {
                        warn!(error = %err, "duckling queue flush failed; re-queuing payload");
                        coordinator_cloned.requeue(retry_payload);
                    }
                    Err(join_err) => {
                        warn!(error = %join_err, "duckling queue flush panicked; re-queuing payload");
                        coordinator_cloned.requeue(retry_payload);
                    }
                }
                drop(permit);
            });
        }
    });
}

fn flush_payload(
    factory: Arc<Mutex<EngineFactory>>,
    settings: &Settings,
    payload: FlushPayload,
) -> Result<(), ServerError> {
    if payload.batches.is_empty() {
        return Ok(());
    }

    info!(
        table = %payload.table,
        rows = payload.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        "flushing duckling queue payload"
    );

    let conn = factory.lock().unwrap().create_connection()?;
    create_schema_if_needed(&conn, &settings.target_schema)?;
    if settings.auto_create_tables {
        create_table_if_needed(&conn, &settings.target_schema, &payload)?;
    }

    let target = format!(
        "{}.{}",
        quote_ident(&settings.target_schema),
        quote_ident(&payload.table)
    );
    for batch in payload.batches {
        conn.insert_with_appender(&target, batch)?;
    }
    Ok(())
}

fn create_schema_if_needed(
    conn: &crate::engine::DuckDbConnection,
    schema: &str,
) -> Result<(), ServerError> {
    let sql = format!("CREATE SCHEMA IF NOT EXISTS {};", quote_ident(schema));
    conn.execute_batch(&sql)
}

fn create_table_if_needed(
    conn: &crate::engine::DuckDbConnection,
    schema: &str,
    payload: &FlushPayload,
) -> Result<(), ServerError> {
    let cols = duckdb_columns(&payload.schema)?;
    let statement = format!(
        "CREATE TABLE IF NOT EXISTS {}.{} ({});",
        quote_ident(schema),
        quote_ident(&payload.table),
        cols.join(", ")
    );
    conn.execute_batch(&statement)
}
