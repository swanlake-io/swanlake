use siphasher::sip::SipHasher13;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};

use anyhow::Result;
use tokio_postgres::Client;
use tracing::debug;

/// PostgreSQL-based distributed lock using advisory locks.
///
/// A fresh PostgreSQL connection is opened for every attempt; the connection
/// lifetime owns the advisory lock and dropping the lock closes the connection,
/// releasing the advisory lock without extra cleanup.
pub(super) struct PostgresLock {
    client: Client,
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

    pub async fn try_acquire(
        client: Client,
        target: &Path,
        _owner: Option<&str>,
    ) -> Result<Option<Self>> {
        let lock_key = Self::path_to_lock_key(target);

        debug!(
            lock_key = lock_key,
            target = %target.display(),
            "attempting to acquire PostgreSQL advisory lock"
        );

        let row = client
            .query_one("SELECT pg_try_advisory_lock($1)", &[&lock_key])
            .await?;
        let locked = row.get::<_, bool>(0);

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

    pub fn client(&self) -> &Client {
        &self.client
    }
}

impl Drop for PostgresLock {
    fn drop(&mut self) {
        debug!(
            lock_key = self.lock_key,
            target = %self.target_path.display(),
            "releasing PostgreSQL advisory lock (connection closing)"
        );
        // Dropping the client closes the connection, which releases the advisory lock.
    }
}
