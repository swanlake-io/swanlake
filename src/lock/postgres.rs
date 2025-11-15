use siphasher::sip::SipHasher13;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use tokio::sync::OnceCell;
use tokio_postgres::{Client, NoTls};
use tracing::{debug, warn};

use super::DistributedLock;

/// PostgreSQL connection configuration, built once from environment variables.
#[derive(Clone)]
struct PgConfig {
    connection_string: String,
    ssl_mode: PgSslMode,
}

#[derive(Clone, Copy, Debug)]
enum PgSslMode {
    Disable,
    Require,
    VerifyCa,
    VerifyFull,
}

impl PgSslMode {
    fn from_env() -> Self {
        let value = std::env::var("PGSSLMODE").unwrap_or_else(|_| "disable".to_string());
        Self::from_str(value.as_str())
    }

    fn from_str(value: &str) -> Self {
        match value.to_lowercase().as_str() {
            "require" => Self::Require,
            "verify-ca" => Self::VerifyCa,
            "verify-full" => Self::VerifyFull,
            _ => Self::Disable,
        }
    }
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

        Self {
            connection_string: config,
            ssl_mode: PgSslMode::from_env(),
        }
    }

    fn get() -> &'static Self {
        use std::sync::OnceLock;
        static CONFIG: OnceLock<PgConfig> = OnceLock::new();
        CONFIG.get_or_init(Self::from_env)
    }
}

struct PgClient {
    client: Client,
}

impl PgClient {
    async fn connect(config: &PgConfig) -> Result<Self> {
        let maybe_tls = Self::build_tls(config.ssl_mode)?;
        let client = if let Some(tls) = maybe_tls {
            debug!(
                "connecting to PostgreSQL with TLS mode {:?}",
                config.ssl_mode
            );
            let (client, connection) =
                tokio_postgres::connect(&config.connection_string, tls).await?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    warn!(error = %e, "PostgreSQL connection error");
                }
            });
            client
        } else {
            debug!("connecting to PostgreSQL without TLS");
            let (client, connection) =
                tokio_postgres::connect(&config.connection_string, NoTls).await?;
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    warn!(error = %e, "PostgreSQL connection error");
                }
            });
            client
        };

        Ok(Self { client })
    }

    fn build_tls(mode: PgSslMode) -> Result<Option<MakeTlsConnector>> {
        match mode {
            PgSslMode::Disable => Ok(None),
            PgSslMode::Require => {
                let connector = TlsConnector::builder()
                    .danger_accept_invalid_certs(true)
                    .danger_accept_invalid_hostnames(true)
                    .build()
                    .context("failed to build TLS connector for PGSSLMODE=require")?;
                Ok(Some(MakeTlsConnector::new(connector)))
            }
            PgSslMode::VerifyCa => {
                let connector = TlsConnector::builder()
                    .danger_accept_invalid_hostnames(true)
                    .build()
                    .context("failed to build TLS connector for PGSSLMODE=verify-ca")?;
                Ok(Some(MakeTlsConnector::new(connector)))
            }
            PgSslMode::VerifyFull => {
                let connector = TlsConnector::builder()
                    .build()
                    .context("failed to build TLS connector for PGSSLMODE=verify-full")?;
                Ok(Some(MakeTlsConnector::new(connector)))
            }
        }
    }

    async fn try_lock(&self, lock_key: i64) -> Result<bool> {
        let row = self
            .client
            .query_one("SELECT pg_try_advisory_lock($1)", &[&lock_key])
            .await?;
        Ok(row.get::<_, bool>(0))
    }

    async fn unlock(&self, lock_key: i64) -> Result<()> {
        self.client
            .execute("SELECT pg_advisory_unlock($1)", &[&lock_key])
            .await?;
        Ok(())
    }
}

async fn shared_client() -> Result<Arc<PgClient>> {
    static CLIENT: OnceCell<Arc<PgClient>> = OnceCell::const_new();
    CLIENT
        .get_or_try_init(|| async {
            let config = PgConfig::get().clone();
            PgClient::connect(&config).await.map(Arc::new)
        })
        .await
        .map(Arc::clone)
}

/// PostgreSQL-based distributed lock using advisory locks.
///
/// This lock uses PostgreSQL advisory locks to coordinate access across multiple hosts.
/// The lock is identified by a 64-bit integer derived from hashing the target path.
/// Uses tokio-postgres for lightweight connection management.
pub struct PostgresLock {
    client: Arc<PgClient>,
    lock_key: i64,
    target_path: PathBuf,
}

impl PostgresLock {
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
        // PostgreSQL advisory locks are tied to the connection lifetime, so TTL is not required.
        let client = shared_client().await?;
        let lock_key = Self::path_to_lock_key(target);

        debug!(
            lock_key = lock_key,
            target = %target.display(),
            "attempting to acquire PostgreSQL advisory lock"
        );

        let locked = client.try_lock(lock_key).await?;

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
            if let Err(err) = client.unlock(lock_key).await {
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

    #[test]
    fn test_ssl_mode_parsing() {
        assert!(matches!(PgSslMode::from_str("disable"), PgSslMode::Disable));
        assert!(matches!(PgSslMode::from_str("require"), PgSslMode::Require));
        assert!(matches!(
            PgSslMode::from_str("verify-ca"),
            PgSslMode::VerifyCa
        ));
        assert!(matches!(
            PgSslMode::from_str("verify-full"),
            PgSslMode::VerifyFull
        ));
        assert!(matches!(PgSslMode::from_str("DiSaBlE"), PgSslMode::Disable));
    }

    #[test]
    fn test_tls_connector_building_per_mode() {
        assert!(PgClient::build_tls(PgSslMode::Disable)
            .expect("disable mode should not error")
            .is_none());
        for mode in [
            PgSslMode::Require,
            PgSslMode::VerifyCa,
            PgSslMode::VerifyFull,
        ] {
            let connector = PgClient::build_tls(mode)
                .unwrap_or_else(|e| panic!("failed to build TLS connector for {:?}: {}", mode, e));
            assert!(
                connector.is_some(),
                "TLS mode {:?} should produce a connector",
                mode
            );
        }
    }
}
