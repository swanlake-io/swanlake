use siphasher::sip::SipHasher13;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use postgres_native_tls::MakeTlsConnector;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};
use tracing::{debug, warn};

use super::DistributedLock;

/// PostgreSQL connection configuration, built once from environment variables.
struct PgConfig {
    connection_string: String,
    use_tls: bool,
}

impl PgConfig {
    fn from_env() -> Self {
        let host = std::env::var("PGHOST").unwrap_or_else(|_| "localhost".to_string());
        let port = std::env::var("PGPORT").unwrap_or_else(|_| "5432".to_string());
        let user = std::env::var("PGUSER").unwrap_or_else(|_| "postgres".to_string());
        let dbname = std::env::var("PGDATABASE").unwrap_or_else(|_| "postgres".to_string());
        let password = std::env::var("PGPASSWORD").ok();

        let mut config = format!(
            "host={} port={} user={} dbname={}",
            host, port, user, dbname
        );
        if let Some(pwd) = password {
            config.push_str(&format!(" password={}", pwd));
        }

        let use_tls = std::env::var("PGSSLMODE")
            .map(|s| s.to_lowercase() == "require")
            .unwrap_or(false);

        Self {
            connection_string: config,
            use_tls,
        }
    }

    fn get() -> &'static Self {
        use std::sync::OnceLock;
        static CONFIG: OnceLock<PgConfig> = OnceLock::new();
        CONFIG.get_or_init(Self::from_env)
    }
}

/// PostgreSQL-based distributed lock using advisory locks.
///
/// This lock uses PostgreSQL advisory locks to coordinate access across multiple hosts.
/// The lock is identified by a 64-bit integer derived from hashing the target path.
/// Uses tokio-postgres for lightweight connection management.
pub struct PostgresLock {
    client: Arc<Mutex<Client>>,
    lock_key: i64,
    target_path: PathBuf,
}

impl PostgresLock {
    /// Create a new PostgreSQL client connection.
    /// Uses connection configuration built from environment variables.
    async fn connect() -> Result<Arc<Mutex<Client>>> {
        let config = PgConfig::get();

        macro_rules! spawn_and_return {
            ($client:expr, $connection:expr) => {{
                tokio::spawn(async move {
                    if let Err(e) = $connection.await {
                        warn!(error = %e, "PostgreSQL connection error");
                    }
                });
                $client
            }};
        }

        let client = if config.use_tls {
            debug!("connecting to PostgreSQL with TLS");
            let connector = native_tls::TlsConnector::new()?;
            let tls = MakeTlsConnector::new(connector);
            let (client, connection) =
                tokio_postgres::connect(&config.connection_string, tls).await?;
            spawn_and_return!(client, connection)
        } else {
            debug!("connecting to PostgreSQL without TLS");
            let (client, connection) =
                tokio_postgres::connect(&config.connection_string, NoTls).await?;
            spawn_and_return!(client, connection)
        };

        Ok(Arc::new(Mutex::new(client)))
    }

    /// Convert a path to a lock key by hashing it to a 64-bit integer.
    fn path_to_lock_key(path: &Path) -> i64 {
        let mut hasher = SipHasher13::new_with_key(&[0u8; 16]);
        path.hash(&mut hasher);
        hasher.finish() as i64
    }
}

impl DistributedLock for PostgresLock {
    async fn try_acquire(
        target: &Path,
        _ttl: Duration,
        _owner: Option<&str>,
    ) -> Result<Option<Self>> {
        let client = Self::connect().await?;
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
                .await?;
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
