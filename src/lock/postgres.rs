use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};
use tracing::{debug, warn};

use super::DistributedLock;

/// PostgreSQL-based distributed lock using advisory locks.
///
/// This lock uses PostgreSQL advisory locks to coordinate access across multiple hosts.
/// The lock is identified by a 64-bit integer derived from hashing the target path.
pub struct PostgresLock {
    client: Arc<Mutex<Client>>,
    lock_key: i64,
    target_path: PathBuf,
    owner: Option<String>,
    acquired_at: SystemTime,
}

impl PostgresLock {
    /// Create a new PostgreSQL lock client.
    ///
    /// # Arguments
    /// * `connection_string` - PostgreSQL connection string (e.g., "host=localhost user=postgres")
    pub async fn connect(connection_string: &str) -> Result<Arc<Mutex<Client>>> {
        let (client, connection) = tokio_postgres::connect(connection_string, NoTls)
            .await
            .context("failed to connect to PostgreSQL")?;

        // Spawn connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                warn!(error = %e, "PostgreSQL connection error");
            }
        });

        Ok(Arc::new(Mutex::new(client)))
    }

    /// Convert a path to a lock key by hashing it to a 64-bit integer.
    fn path_to_lock_key(path: &Path) -> i64 {
        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        hasher.finish() as i64
    }
}

impl DistributedLock for PostgresLock {
    async fn try_acquire(
        target: &Path,
        _ttl: Duration,
        owner: Option<&str>,
    ) -> Result<Option<Self>> {
        // Get PostgreSQL connection string from environment
        let connection_string = std::env::var("SWANLAKE_LOCK_POSTGRES_CONNECTION")
            .or_else(|_| std::env::var("PGCONNECTION"))
            .unwrap_or_else(|_| {
                // Build connection string from individual env vars
                let host = std::env::var("PGHOST").unwrap_or_else(|_| "localhost".to_string());
                let port = std::env::var("PGPORT").unwrap_or_else(|_| "5432".to_string());
                let user = std::env::var("PGUSER").unwrap_or_else(|_| "postgres".to_string());
                let dbname =
                    std::env::var("PGDATABASE").unwrap_or_else(|_| "postgres".to_string());

                let mut conn_str = format!("host={} port={} user={} dbname={}", host, port, user, dbname);
                if let Ok(password) = std::env::var("PGPASSWORD") {
                    conn_str.push_str(&format!(" password={}", password));
                }
                conn_str
            });

        let client = Self::connect(&connection_string).await?;
        let lock_key = Self::path_to_lock_key(target);

        debug!(
            lock_key = lock_key,
            target = %target.display(),
            "attempting to acquire PostgreSQL advisory lock"
        );

        // Try to acquire the advisory lock (non-blocking)
        let locked = {
            let client = client.lock().await;
            let row = client
                .query_one("SELECT pg_try_advisory_lock($1)", &[&lock_key])
                .await
                .context("failed to execute pg_try_advisory_lock")?;
            row.get::<_, bool>(0)
        };

        if locked {
            debug!(
                lock_key = lock_key,
                target = %target.display(),
                "acquired PostgreSQL advisory lock"
            );
            Ok(Some(Self {
                client,
                lock_key,
                target_path: target.to_path_buf(),
                owner: owner.map(|s| s.to_string()),
                acquired_at: SystemTime::now(),
            }))
        } else {
            debug!(
                lock_key = lock_key,
                target = %target.display(),
                "PostgreSQL advisory lock already held by another process"
            );
            Ok(None)
        }
    }

    async fn refresh(&self) -> Result<()> {
        // PostgreSQL advisory locks are session-based and don't expire,
        // so this is a no-op. We just verify the connection is still alive.
        let client = self.client.lock().await;
        client
            .query_one("SELECT 1", &[])
            .await
            .context("failed to refresh PostgreSQL lock (connection lost)")?;
        Ok(())
    }
}

impl Drop for PostgresLock {
    fn drop(&mut self) {
        let client = self.client.clone();
        let lock_key = self.lock_key;
        let target_path = self.target_path.clone();

        // Spawn a task to release the lock asynchronously
        tokio::spawn(async move {
            let client = client.lock().await;
            if let Err(err) = client
                .execute("SELECT pg_advisory_unlock($1)", &[&lock_key])
                .await
            {
                warn!(
                    error = %err,
                    lock_key = lock_key,
                    target = %target_path.display(),
                    "failed to release PostgreSQL advisory lock"
                );
            } else {
                debug!(
                    lock_key = lock_key,
                    target = %target_path.display(),
                    "released PostgreSQL advisory lock"
                );
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_path_to_lock_key() {
        let path1 = PathBuf::from("/tmp/test.db");
        let path2 = PathBuf::from("/tmp/test.db");
        let path3 = PathBuf::from("/tmp/other.db");

        let key1 = PostgresLock::path_to_lock_key(&path1);
        let key2 = PostgresLock::path_to_lock_key(&path2);
        let key3 = PostgresLock::path_to_lock_key(&path3);

        // Same paths should produce same keys
        assert_eq!(key1, key2);
        // Different paths should produce different keys
        assert_ne!(key1, key3);
    }
}
