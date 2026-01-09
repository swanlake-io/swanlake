use std::time::{Duration, Instant};

use adbc_driver_manager::ManagedConnection;

pub(crate) struct IdleConnection {
    pub(crate) conn: ManagedConnection,
    pub(crate) last_used: Instant,
}

pub(crate) struct PoolState {
    pub(crate) idle: Vec<IdleConnection>,
}

pub(crate) fn evict_idle(state: &mut PoolState, idle_ttl_ms: u64) -> usize {
    if idle_ttl_ms == 0 {
        return 0;
    }
    let ttl = Duration::from_millis(idle_ttl_ms);
    let cutoff = Instant::now()
        .checked_sub(ttl)
        .unwrap_or_else(Instant::now);
    let mut removed = 0usize;
    state.idle.retain(|entry| {
        let keep = entry.last_used >= cutoff;
        if !keep {
            removed += 1;
        }
        keep
    });
    removed
}
