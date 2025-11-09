//! Session registry - manages all active client sessions.
//!
//! The registry:
//! - Creates new sessions on client connect
//! - Tracks active sessions by ID
//! - Provides session lookup
//! - Cleans up idle sessions
//! - Enforces max session limit

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::Context;
use tracing::{debug, info, instrument, warn};

use crate::config::ServerConfig;
use crate::dq::DucklingQueueManager;
use crate::engine::EngineFactory;
use crate::error::ServerError;
use crate::session::id::SessionId;
use crate::session::Session;

/// Registry for managing all active sessions
#[derive(Clone)]
pub struct SessionRegistry {
    inner: Arc<RwLock<RegistryInner>>,
    factory: EngineFactory,
    max_sessions: usize,
    session_timeout: Duration,
    writes_enabled: bool,
    #[allow(dead_code)]
    dq_manager: Option<Arc<DucklingQueueManager>>,
}

struct RegistryInner {
    sessions: HashMap<SessionId, Arc<Session>>,
}

impl SessionRegistry {
    /// Create a new session registry
    #[instrument(skip(config, dq_manager))]
    pub fn new(
        config: &ServerConfig,
        dq_manager: Option<Arc<DucklingQueueManager>>,
    ) -> Result<Self, ServerError> {
        let factory = EngineFactory::new(config, dq_manager.clone())?;
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
            writes_enabled: config.enable_writes,
            dq_manager,
        })
    }

    pub fn engine_factory(&self) -> EngineFactory {
        self.factory.clone()
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
    pub fn get_or_create_by_id(&self, session_id: &SessionId) -> Result<Arc<Session>, ServerError> {
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
        let connection = self.factory.create_connection()?;

        // Create session with the specified ID
        let session = Arc::new(Session::new_with_id(
            session_id.clone(),
            connection,
            self.writes_enabled,
        ));

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

    pub fn switch_duckling_queue_except(&self, new_path: &Path) -> anyhow::Result<()> {
        info!(
            new_path = %new_path.display(),
            "switching duckling queue to new path"
        );

        // Step 1: Recreate the base connection with new duckling queue path
        self.factory
            .recreate_base_connection(new_path)
            .context("failed to recreate base connection")?;

        // Step 2: Collect all sessions
        let sessions = {
            let inner = self.inner.read().expect("registry lock poisoned");
            inner
                .sessions
                .iter()
                .map(|(id, session)| (id.clone(), session.clone()))
                .collect::<Vec<_>>()
        };

        // Step 3: Handle each session selectively
        for (session_id, session) in sessions {
            // Check if session has active transactions
            if session.has_active_transactions() {
                info!(
                    session_id = %session_id,
                    "removing session with active transactions during duckling queue rotation"
                );
                self.remove_session(&session_id);
                continue;
            }

            // Check if session has prepared statements
            if session.has_prepared_statements() {
                info!(
                    session_id = %session_id,
                    "removing session with prepared statements during duckling queue rotation"
                );
                self.remove_session(&session_id);
                continue;
            }

            // Session is clean - try to replace connection
            match self.factory.create_connection() {
                Ok(new_conn) => {
                    if let Err(err) = session.replace_connection(new_conn) {
                        warn!(
                            session_id = %session_id,
                            error = %err,
                            "failed to replace connection; removing session"
                        );
                        self.remove_session(&session_id);
                    } else {
                        debug!(
                            session_id = %session_id,
                            "successfully switched session to new duckling queue file"
                        );
                    }
                }
                Err(err) => {
                    warn!(
                        session_id = %session_id,
                        error = %err,
                        "failed to create new connection; removing session"
                    );
                    self.remove_session(&session_id);
                }
            }
        }

        Ok(())
    }

    pub fn remove_session(&self, session_id: &SessionId) {
        let mut inner = self.inner.write().expect("registry lock poisoned");
        if inner.sessions.remove(session_id).is_some() {
            info!(session_id = %session_id, "session removed");
        }
    }
}
