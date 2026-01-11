//! Session registry - manages all active client sessions.
//!
//! The registry:
//! - Creates new sessions on client connect
//! - Tracks active sessions by ID
//! - Provides session lookup
//! - Cleans up idle sessions
//! - Enforces max session limit

use std::collections::HashMap;

use std::sync::{Arc, RwLock};
use std::time::Duration;

use serde::Serialize;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, info, instrument, warn};

use crate::config::ServerConfig;
use crate::engine::EngineFactory;
use crate::error::ServerError;
use crate::session::id::SessionId;
use crate::session::Session;

/// Registry for managing all active sessions
#[derive(Clone)]
pub struct SessionRegistry {
    inner: Arc<RwLock<RegistryInner>>,
    factory: Arc<EngineFactory>,
    max_sessions: usize,
    session_timeout: Duration,
    session_permits: Arc<Semaphore>,
}

#[derive(Clone, Serialize)]
pub struct SessionRegistrySnapshot {
    pub total_sessions: usize,
    pub max_sessions: usize,
    pub session_timeout_seconds: u64,
    pub oldest_idle_ms: u64,
    pub average_idle_ms: u64,
}

struct RegistryInner {
    sessions: HashMap<SessionId, SessionEntry>,
}

struct SessionEntry {
    session: Arc<Session>,
    _permit: OwnedSemaphorePermit,
}

impl SessionRegistry {
    /// Create a new session registry
    #[instrument(skip(config, factory))]
    pub fn new(config: &ServerConfig, factory: Arc<EngineFactory>) -> Result<Self, ServerError> {
        let max_sessions = config.max_sessions.unwrap_or(100);
        let session_timeout = Duration::from_secs(config.session_timeout_seconds.unwrap_or(1800)); // 30min default

        info!(
            max_sessions,
            session_timeout_seconds = session_timeout.as_secs(),
            "session registry initialized"
        );

        Ok(Self {
            inner: Arc::new(RwLock::new(RegistryInner {
                sessions: HashMap::new(),
            })),
            factory,
            max_sessions,
            session_timeout,
            session_permits: Arc::new(Semaphore::new(max_sessions)),
        })
    }

    pub fn engine_factory(&self) -> Arc<EngineFactory> {
        self.factory.clone()
    }

    pub fn snapshot(&self) -> SessionRegistrySnapshot {
        let inner = self.inner.read().expect("registry lock poisoned");
        let total_sessions = inner.sessions.len();
        let session_timeout_seconds = self.session_timeout.as_secs();
        let mut total_idle_ms = 0u64;
        let mut oldest_idle_ms = 0u64;

        for entry in inner.sessions.values() {
            let idle_ms = entry.session.idle_duration().as_millis() as u64;
            total_idle_ms = total_idle_ms.saturating_add(idle_ms);
            if idle_ms > oldest_idle_ms {
                oldest_idle_ms = idle_ms;
            }
        }

        let average_idle_ms = if total_sessions == 0 {
            0
        } else {
            total_idle_ms / total_sessions as u64
        };

        SessionRegistrySnapshot {
            total_sessions,
            max_sessions: self.max_sessions,
            session_timeout_seconds,
            oldest_idle_ms,
            average_idle_ms,
        }
    }

    /// Clean up idle sessions that have exceeded the timeout
    #[instrument(skip(self))]
    pub fn cleanup_idle_sessions(&self) -> usize {
        let mut inner = self.inner.write().expect("registry lock poisoned");
        let before = inner.sessions.len();

        inner.sessions.retain(|id, entry| {
            if entry.session.idle_duration() > self.session_timeout {
                info!(
                    session_id = %id,
                    idle_duration = ?entry.session.idle_duration(),
                    "removing idle session"
                );
                false
            } else {
                true
            }
        });

        let removed = before - inner.sessions.len();
        if removed > 0 {
            info!(
                removed,
                total_sessions = inner.sessions.len(),
                "cleaned up idle sessions"
            );
        }
        removed
    }

    /// Get or create session by session ID (Phase 2)
    ///
    /// This enables session persistence across requests from the same gRPC connection.
    /// The session_id is derived from the connection info (e.g., remote address).
    /// If a session already exists with this ID, it is reused.
    /// Otherwise, a new session is created with the given ID.
    pub async fn get_or_create_by_id(
        &self,
        session_id: &SessionId,
    ) -> Result<Arc<Session>, ServerError> {
        // First, try to get existing session (read lock)
        {
            let inner = self.inner.read().expect("registry lock poisoned");
            if let Some(entry) = inner.sessions.get(session_id) {
                debug!(
                    session_id = %session_id,
                    "reusing existing session"
                );
                return Ok(entry.session.clone());
            }
        }

        // Acquire a session permit to enforce max sessions.
        let permit = self
            .session_permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                let current = self
                    .inner
                    .read()
                    .expect("registry lock poisoned")
                    .sessions
                    .len();
                warn!(
                    current,
                    max = self.max_sessions,
                    "max sessions limit reached"
                );
                ServerError::MaxSessionsReached
            })?;

        // Create new connection in spawn_blocking to avoid blocking async runtime
        // (DuckDB connection creation involves I/O: loading extensions, init SQL, etc.)
        let factory = self.factory.clone();
        let connection = tokio::task::spawn_blocking(move || factory.create_connection())
            .await
            .map_err(|e| ServerError::Internal(format!("connection task failed: {}", e)))??;

        // Create session with the specified ID
        let session = Arc::new(Session::new_with_id(session_id.clone(), connection));

        // Register session
        {
            let mut inner = self.inner.write().expect("registry lock poisoned");
            if let Some(entry) = inner.sessions.get(session_id) {
                return Ok(entry.session.clone());
            }
            inner.sessions.insert(
                session_id.clone(),
                SessionEntry {
                    session: session.clone(),
                    _permit: permit,
                },
            );
            info!(
                session_id = %session_id,
                total_sessions = inner.sessions.len(),
                "session created with specific ID"
            );
        }

        Ok(session)
    }
}
