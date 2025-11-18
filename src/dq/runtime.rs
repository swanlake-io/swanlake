use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow_array::Array;
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::dq::config::Settings;
use crate::dq::coordinator::{DqCoordinator, FlushPayload};
use crate::dq::schema::quote_ident;
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

    // Log all tables in the target schema
    let query = format!(
        "SELECT table_name FROM information_schema.tables WHERE table_catalog = '{}'",
        settings.target_schema
    );
    match conn.execute_query(&query) {
        Ok(result) => {
            let tables: Vec<String> = result
                .batches
                .iter()
                .flat_map(|batch| {
                    if let Some(array) = batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                    {
                        (0..array.len())
                            .filter_map(|i| array.value(i).to_string().into())
                            .collect()
                    } else {
                        vec![]
                    }
                })
                .collect();
            info!(schema = %settings.target_schema, tables = ?tables, "tables in target schema");
        }
        Err(e) => {
            debug!(error = %e, "failed to query tables in target schema");
        }
    }

    let target = fully_qualified_table(&settings.target_schema, &payload.table);
    for batch in payload.batches {
        conn.insert_with_appender(&target, batch)?;
    }
    Ok(())
}

fn fully_qualified_table(schema: &str, table: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(table))
}
