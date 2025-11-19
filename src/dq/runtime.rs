use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::dq::config::Settings;
use crate::dq::coordinator::{DqCoordinator, FlushPayload, FlushTracker};
use crate::dq::storage::DurableStorage;
use crate::engine::EngineFactory;
use crate::error::ServerError;

/// Background runtime that receives flush payloads and writes them into DuckLake.
pub struct QueueRuntime;

impl QueueRuntime {
    /// Bootstrap the runtime and return the coordinator alongside it.
    pub fn bootstrap(
        factory: Arc<Mutex<EngineFactory>>,
        settings: Settings,
    ) -> Result<(Arc<DqCoordinator>, Arc<Self>), ServerError> {
        let (tx, rx) = mpsc::unbounded_channel::<FlushPayload>();
        let storage = Arc::new(DurableStorage::new(settings.root_dir.clone())?);
        let tracker = Arc::new(FlushTracker::new());
        let coordinator = Arc::new(DqCoordinator::new(
            settings.clone(),
            storage.clone(),
            tracker.clone(),
            tx,
        )?);
        let runtime = Arc::new(Self);
        spawn_flush_workers(
            factory,
            settings.clone(),
            storage.clone(),
            tracker.clone(),
            coordinator.clone(),
            rx,
        );
        spawn_age_sweeper(coordinator.clone(), settings.flush_interval);
        Ok((coordinator, runtime))
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
    storage: Arc<DurableStorage>,
    tracker: Arc<FlushTracker>,
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
            let storage_clone = storage.clone();
            let tracker_clone = tracker.clone();
            let retry_payload = payload.clone();
            tokio::spawn(async move {
                let res = tokio::task::spawn_blocking(move || {
                    flush_payload(factory_cloned, &settings_clone, storage_clone, payload)
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
                tracker_clone.complete();
                drop(permit);
            });
        }
    });
}

fn flush_payload(
    factory: Arc<Mutex<EngineFactory>>,
    settings: &Settings,
    storage: Arc<DurableStorage>,
    payload: FlushPayload,
) -> Result<(), ServerError> {
    if payload.is_empty() {
        return Ok(());
    }

    let rows = payload.total_rows();
    let table = payload.table.clone();
    let (batches, chunk_handles) = payload.into_batches_and_chunks();

    info!(
        table = %table,
        rows = rows,
        "flushing duckling queue payload"
    );

    let conn = factory.lock().unwrap().create_connection()?;
    conn.insert_with_appender(&settings.target_catalog, &table, batches)?;
    storage.remove_chunks(chunk_handles)?;
    Ok(())
}
