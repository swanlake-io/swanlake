use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use duckdb::Connection;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use super::DistributedLock;

/// PostgreSQL-based distributed lock using advisory locks via DuckDB's postgres extension.
///
/// This lock uses PostgreSQL advisory locks to coordinate access across multiple hosts.
/// The lock is identified by a 64-bit integer derived from hashing the target path.
/// Uses DuckDB's postgres extension to connect, avoiding additional dependencies.
pub struct PostgresLock {
    conn: Arc<Mutex<Connection>>,
    lock_key: i64,
    target_path: PathBuf,
    owner: Option<String>,
}

impl PostgresLock {
    /// Create a new PostgreSQL lock connection using DuckDB's postgres extension.
    fn connect() -> Result<Arc<Mutex<Connection>>> {
        let conn = Connection::open_in_memory()?;
        
        // Install and load postgres extension
        conn.execute_batch("INSTALL postgres; LOAD postgres;")?;
        
        // Attach to PostgreSQL using environment variables
        // DuckDB's postgres extension reads from standard PG* environment variables
        conn.execute_batch("ATTACH '' AS lock_db (TYPE postgres);")?;
        
        Ok(Arc::new(Mutex::new(conn)))
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
        let conn = Self::connect()?;
        let lock_key = Self::path_to_lock_key(target);

        debug!(
            lock_key = lock_key,
            target = %target.display(),
            "attempting to acquire PostgreSQL advisory lock"
        );

        // Try to acquire the advisory lock (non-blocking)
        let locked = {
            let conn = conn.lock().await;
            let mut stmt = conn.prepare("SELECT pg_try_advisory_lock(?)")?;
            let locked: bool = stmt.query_row([lock_key], |row| row.get(0))?;
            locked
        };

        if locked {
            debug!(
                lock_key = lock_key,
                target = %target.display(),
                "acquired PostgreSQL advisory lock"
            );
            Ok(Some(Self {
                conn,
                lock_key,
                target_path: target.to_path_buf(),
                owner: owner.map(|s| s.to_string()),
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
        let conn = self.conn.lock().await;
        conn.execute("SELECT 1", [])?;
        Ok(())
    }
}

impl Drop for PostgresLock {
    fn drop(&mut self) {
        let conn = self.conn.clone();
        let lock_key = self.lock_key;
        let target_path = self.target_path.clone();

        // Spawn a task to release the lock asynchronously
        tokio::spawn(async move {
            let conn = conn.lock().await;
            if let Err(err) = conn.execute("SELECT pg_advisory_unlock(?)", [lock_key]) {
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
