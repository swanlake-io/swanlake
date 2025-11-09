use std::borrow::Cow;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};

/// File-based lock used to coordinate flush workers across hosts.
pub struct FileLock {
    lock_path: PathBuf,
    owner: Option<String>,
}

fn generate_lock_payload(owner: Option<&str>) -> String {
    let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".to_string());
    let pid = std::process::id();
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let mut payload = format!("host={hostname}\npid={pid}\nacquired_at={now}\n");
    if let Some(owner) = owner {
        payload.push_str(&format!("session_id={owner}\n"));
    }
    payload
}

impl FileLock {
    pub fn try_acquire(target: &Path, ttl: Duration, owner: Option<&str>) -> Result<Option<Self>> {
        let lock_path = lock_path_for(target);

        // best effort loop: try to grab lock; if stale, remove and retry once
        for attempt in 0..2 {
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock_path)
            {
                Ok(mut file) => {
                    let payload = generate_lock_payload(owner);
                    file.write_all(payload.as_bytes()).ok();
                    return Ok(Some(Self {
                        lock_path,
                        owner: owner.map(|s| s.to_string()),
                    }));
                }
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    if attempt == 0 && is_lock_stale(&lock_path, ttl)? {
                        fs::remove_file(&lock_path).ok();
                        continue;
                    } else {
                        return Ok(None);
                    }
                }
                Err(err) => {
                    return Err(anyhow::anyhow!(
                        "failed to create lock file {:?}: {err}",
                        lock_path
                    ));
                }
            }
        }

        Ok(None)
    }

    /// Refresh the lock by updating its timestamp to prevent expiration.
    pub fn refresh(&self) -> Result<()> {
        let payload = generate_lock_payload(self.owner.as_deref());
        fs::write(&self.lock_path, payload)
            .with_context(|| format!("failed to refresh lock file {:?}", self.lock_path))?;
        Ok(())
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        if let Err(err) = fs::remove_file(&self.lock_path) {
            tracing::warn!(
                error = %err,
                lock = %self.lock_path.display(),
                "failed to remove duckling queue lock file"
            );
        }
    }
}

pub fn lock_path_for(db_path: &Path) -> PathBuf {
    let file_name = db_path
        .file_name()
        .map(|name| name.to_string_lossy())
        .unwrap_or_else(|| Cow::Borrowed("duckling_queue"));
    db_path
        .with_file_name(format!("{file_name}.lock"))
        .to_path_buf()
}

pub fn read_lock_session_id(db_path: &Path) -> Result<Option<String>> {
    let lock_path = lock_path_for(db_path);
    let contents = match fs::read_to_string(&lock_path) {
        Ok(data) => data,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(anyhow::anyhow!(
                "failed to read lock file {:?}: {err}",
                lock_path
            ))
        }
    };

    for line in contents.lines() {
        if let Some(rest) = line.strip_prefix("session_id=") {
            if rest.is_empty() {
                return Ok(None);
            }
            return Ok(Some(rest.to_string()));
        }
    }

    Ok(None)
}

fn is_lock_stale(lock_path: &Path, ttl: Duration) -> Result<bool> {
    let meta = match fs::metadata(lock_path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(err) => {
            return Err(anyhow::anyhow!(
                "failed to read metadata for lock {:?}: {err}",
                lock_path
            ))
        }
    };
    let modified = meta
        .modified()
        .context("lock file missing modified timestamp")?;
    let age = modified
        .elapsed()
        .unwrap_or_else(|_| Duration::from_secs(0));
    Ok(age >= ttl)
}
