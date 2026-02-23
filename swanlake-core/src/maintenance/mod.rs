//! Background DuckLake maintenance tasks (checkpointing).
//!
//! Coordinates cross-instance checkpoint execution using PostgreSQL for both
//! metadata (`ducklake_checkpoints` table) and advisory locks.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use tokio_postgres::Client;
use tracing::{debug, info, warn};

use crate::config::ServerConfig;
use crate::engine::{DuckDbConnection, EngineFactory};

mod lock;
mod postgres;
use lock::PostgresLock;
use postgres::connect_client;

const CHECK_TICK_SECS: u64 = 300;

/// Parsed, normalized checkpoint settings.
#[derive(Clone)]
pub struct CheckpointConfig {
    databases: Vec<String>,
    interval: Duration,
    tick: Duration,
}

impl CheckpointConfig {
    pub fn from_server_config(config: &ServerConfig) -> Option<Self> {
        let databases = parse_checkpoint_databases(config.checkpoint_databases.as_deref());
        if databases.is_empty() {
            return None;
        }

        let hours = config.checkpoint_interval_hours.unwrap_or(24);
        let tick_secs = config.checkpoint_poll_seconds.unwrap_or(CHECK_TICK_SECS);
        let interval = Duration::from_secs(hours.saturating_mul(3600));
        Some(Self {
            databases,
            interval,
            tick: Duration::from_secs(tick_secs),
        })
    }

    pub fn databases(&self) -> &[String] {
        &self.databases
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }

    pub fn tick(&self) -> Duration {
        self.tick
    }
}

/// Handles background checkpointing lifecycle.
pub struct CheckpointService {
    cfg: CheckpointConfig,
    factory: Arc<EngineFactory>,
    checkpoint_conn: Arc<Mutex<Option<DuckDbConnection>>>,
}

impl CheckpointService {
    pub fn new(cfg: CheckpointConfig, factory: Arc<EngineFactory>) -> Self {
        Self {
            cfg,
            factory,
            checkpoint_conn: Arc::new(Mutex::new(None)),
        }
    }

    /// Spawn the checkpoint loop if there is work configured.
    pub async fn spawn_from_config(
        config: &ServerConfig,
        factory: Arc<EngineFactory>,
    ) -> Result<()> {
        let Some(cfg) = CheckpointConfig::from_server_config(config) else {
            debug!("no checkpoint databases configured; skipping checkpoint task");
            return Ok(());
        };

        ensure_checkpoint_table().await?;

        let svc = Self::new(cfg, factory);
        tokio::spawn(async move {
            svc.run_loop().await;
        });
        Ok(())
    }

    async fn run_loop(self) {
        // The first tick fires immediately; consume it so we don't race
        // startup session creation with an immediate checkpoint connection bootstrap.
        let mut ticker = tokio::time::interval(self.cfg.tick());
        ticker.tick().await;
        loop {
            ticker.tick().await;
            for db in self.cfg.databases() {
                if let Err(err) = self.process_database(db).await {
                    warn!(db_name = %db, error = %err, "checkpoint attempt failed");
                }
            }
        }
    }

    async fn process_database(&self, db_name: &str) -> Result<()> {
        let client = connect_client().await?;
        let lock_target = PathBuf::from("/ducklake/checkpoint").join(db_name);
        let Some(lock) =
            PostgresLock::try_acquire(client, &lock_target, Some("checkpoint")).await?
        else {
            info!(
                db_name = %db_name,
                "checkpoint skipped this round: lock held by another instance"
            );
            return Ok(());
        };

        let Some(last) = self.last_checkpoint_at(db_name, lock.client()).await? else {
            self.record_checkpoint(db_name, lock.client()).await?;
            info!(
                db_name = %db_name,
                "checkpoint schedule initialized; skipping immediate checkpoint"
            );
            return Ok(());
        };

        let elapsed = Utc::now()
            .signed_duration_since(last)
            .to_std()
            .unwrap_or_default();
        if elapsed < self.cfg.interval() {
            info!(
                db_name = %db_name,
                last_checkpoint_at = %last.to_rfc3339(),
                "checkpoint skipped this round: interval not reached"
            );
            return Ok(());
        }

        let started = Instant::now();
        self.run_checkpoint(db_name).await?;
        let duration_ms = started.elapsed().as_millis() as u64;
        self.record_checkpoint(db_name, lock.client()).await?;
        info!(
            db_name = %db_name,
            duration_ms,
            "ducklake checkpoint completed successfully"
        );
        Ok(())
    }

    async fn last_checkpoint_at(
        &self,
        db_name: &str,
        client: &Client,
    ) -> Result<Option<DateTime<Utc>>> {
        let row = client
            .query_opt(
                "SELECT last_checkpoint_at FROM ducklake_checkpoints WHERE db_name = $1",
                &[&db_name],
            )
            .await
            .context("querying last checkpoint")?;
        row.map(|r| r.try_get(0))
            .transpose()
            .context("parsing last_checkpoint_at")
    }

    async fn record_checkpoint(&self, db_name: &str, client: &Client) -> Result<()> {
        client
            .execute(
                "INSERT INTO ducklake_checkpoints (db_name, last_checkpoint_at) \
                 VALUES ($1, NOW()) \
                 ON CONFLICT (db_name) DO UPDATE \
                 SET last_checkpoint_at = EXCLUDED.last_checkpoint_at",
                &[&db_name],
            )
            .await
            .context("upserting last checkpoint")?;
        Ok(())
    }

    async fn run_checkpoint(&self, db_name: &str) -> Result<()> {
        let db = db_name.to_string();
        let factory = self.factory.clone();
        let checkpoint_conn = self.checkpoint_conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = checkpoint_conn
                .lock()
                .map_err(|_| anyhow!("checkpoint connection lock poisoned"))?;
            if guard.is_none() {
                let conn = factory
                    .create_connection()
                    .map_err(|e| anyhow!(e.to_string()))?;
                *guard = Some(conn);
            }

            let sql = format!("USE {db}; CHECKPOINT;");
            let exec = if let Some(conn) = guard.as_ref() {
                conn.execute_batch(&sql).map_err(|e| anyhow!(e.to_string()))
            } else {
                Err(anyhow!("checkpoint connection initialization failed"))
            };
            if let Err(err) = exec {
                // Recreate connection on next attempt.
                *guard = None;
                return Err(err).context("running checkpoint");
            }
            Ok(())
        })
        .await
        .map_err(|e| anyhow!(e.to_string()))?
    }
}

fn parse_checkpoint_databases(raw: Option<&str>) -> Vec<String> {
    raw.map(|value| {
        value
            .split(',')
            .filter_map(|s| {
                let trimmed = s.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(trimmed.to_string())
                }
            })
            .collect()
    })
    .unwrap_or_default()
}

async fn ensure_checkpoint_table() -> Result<()> {
    let client = connect_client().await?;
    client
        .batch_execute(
            "CREATE TABLE IF NOT EXISTS ducklake_checkpoints (\
                 db_name TEXT PRIMARY KEY,\
                 last_checkpoint_at TIMESTAMPTZ\
             )",
        )
        .await
        .context("creating ducklake_checkpoints table")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::{anyhow, Result};

    use super::*;

    #[test]
    fn parse_checkpoint_databases_filters_empty_entries() {
        let parsed = parse_checkpoint_databases(Some(" swanlake, ,analytics,, logs "));
        assert_eq!(
            parsed,
            vec![
                "swanlake".to_string(),
                "analytics".to_string(),
                "logs".to_string()
            ]
        );
        assert!(parse_checkpoint_databases(None).is_empty());
        assert!(parse_checkpoint_databases(Some(" , , ")).is_empty());
    }

    #[test]
    fn checkpoint_config_from_server_config_uses_defaults() -> Result<()> {
        let config = ServerConfig {
            checkpoint_databases: Some("swanlake,analytics".to_string()),
            checkpoint_interval_hours: None,
            checkpoint_poll_seconds: None,
            ..ServerConfig::default()
        };

        let checkpoint = CheckpointConfig::from_server_config(&config)
            .ok_or_else(|| anyhow!("expected checkpoint config for non-empty database list"))?;
        assert_eq!(
            checkpoint.databases(),
            &vec!["swanlake".to_string(), "analytics".to_string()]
        );
        assert_eq!(checkpoint.interval(), Duration::from_secs(24 * 3600));
        assert_eq!(checkpoint.tick(), Duration::from_secs(CHECK_TICK_SECS));
        Ok(())
    }

    #[test]
    fn checkpoint_config_returns_none_without_databases() {
        let config = ServerConfig {
            checkpoint_databases: None,
            ..ServerConfig::default()
        };
        assert!(CheckpointConfig::from_server_config(&config).is_none());
    }

    #[tokio::test]
    async fn run_checkpoint_clears_cached_connection_after_failure() -> Result<()> {
        let config = ServerConfig::default();
        let factory = Arc::new(EngineFactory::new_for_tests(&config));
        let checkpoint = CheckpointConfig {
            databases: vec!["missing_catalog".to_string()],
            interval: Duration::from_secs(1),
            tick: Duration::from_secs(1),
        };
        let service = CheckpointService::new(checkpoint, factory);

        let result = service.run_checkpoint("missing_catalog").await;
        assert!(result.is_err());

        let guard = service
            .checkpoint_conn
            .lock()
            .map_err(|_| anyhow!("checkpoint connection mutex poisoned"))?;
        assert!(guard.is_none());
        Ok(())
    }
}
