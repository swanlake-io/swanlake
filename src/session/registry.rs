//! Session registry - manages all active client sessions.
//!
//! The registry:
//! - Creates new sessions on client connect
//! - Tracks active sessions by ID
//! - Provides session lookup
//! - Cleans up idle sessions
//! - Enforces max session limit

use std::collections::HashMap;

use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use tracing::{debug, info, instrument, warn};

use crate::config::ServerConfig;
use crate::dq::DqCoordinator;
use crate::engine::EngineFactory;
use crate::error::ServerError;
use crate::session::id::SessionId;
use crate::session::Session;

/// Registry for managing all active sessions
#[derive(Clone)]
pub struct SessionRegistry {
    inner: Arc<RwLock<RegistryInner>>,
    factory: Arc<Mutex<EngineFactory>>,
    max_sessions: usize,
    session_timeout: Duration,
    target_catalog: String,
}

struct RegistryInner {
    sessions: HashMap<SessionId, Arc<Session>>,
    dq_coordinator: Option<Arc<DqCoordinator>>,
}

impl SessionRegistry {
    /// Create a new session registry
    #[instrument(skip(config, factory, dq_coordinator))]
    pub fn new(
        config: &ServerConfig,
        factory: Arc<Mutex<EngineFactory>>,
        dq_coordinator: Option<Arc<DqCoordinator>>,
    ) -> Result<Self, ServerError> {
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
                dq_coordinator,
            })),
            factory,
            max_sessions,
            session_timeout,
            target_catalog: config.duckling_queue_target_catalog.clone(),
        })
    }

    pub fn engine_factory(&self) -> Arc<Mutex<EngineFactory>> {
        self.factory.clone()
    }

    pub fn target_catalog(&self) -> &str {
        &self.target_catalog
    }

    /// Clean up idle sessions that have exceeded the timeout
    #[instrument(skip(self))]
    pub fn cleanup_idle_sessions(&self) -> usize {
        let mut inner = self.inner.write().expect("registry lock poisoned");
        let before = inner.sessions.len();

        inner.sessions.retain(|id, session| {
            if session.idle_duration() > self.session_timeout {
                info!(
                    session_id = %id,
                    idle_duration = ?session.idle_duration(),
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
            if let Some(session) = inner.sessions.get(session_id) {
                debug!(
                    session_id = %session_id,
                    "reusing existing session"
                );
                return Ok(session.clone());
            }
        }

        // No existing session, create new one with specific ID (write lock)
        // Check session limit
        {
            let inner = self.inner.read().expect("registry lock poisoned");
            if inner.sessions.len() >= self.max_sessions {
                warn!(
                    current = inner.sessions.len(),
                    max = self.max_sessions,
                    "max sessions limit reached"
                );
                return Err(ServerError::MaxSessionsReached);
            }
        }

        // Create new connection
        let connection = self.factory.lock().unwrap().create_connection()?;

        // Create session with the specified ID
        let dq_coordinator = {
            let inner = self.inner.read().expect("registry lock poisoned");
            inner.dq_coordinator.clone()
        };

        let session = if let Some(dq_coord) = dq_coordinator {
            Arc::new(Session::new_with_id_and_dq(
                session_id.clone(),
                connection,
                dq_coord,
            )?)
        } else {
            Arc::new(Session::new_with_id(session_id.clone(), connection))
        };

        // Register session
        {
            let mut inner = self.inner.write().expect("registry lock poisoned");
            inner.sessions.insert(session_id.clone(), session.clone());
            info!(
                session_id = %session_id,
                total_sessions = inner.sessions.len(),
                "session created with specific ID"
            );
        }

        Ok(session)
    }
}
