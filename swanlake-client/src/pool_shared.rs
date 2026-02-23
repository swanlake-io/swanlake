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
    let cutoff = Instant::now().checked_sub(ttl).unwrap_or_else(Instant::now);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evict_idle_keeps_connections_when_ttl_is_zero() {
        let mut state = PoolState { idle: Vec::new() };
        let removed = evict_idle(&mut state, 0);
        assert_eq!(removed, 0);
        assert!(state.idle.is_empty());
    }

    #[test]
    fn evict_idle_is_stable_for_empty_state_with_non_zero_ttl() {
        let mut state = PoolState { idle: Vec::new() };
        let removed = evict_idle(&mut state, 5);
        assert_eq!(removed, 0);
        assert!(state.idle.is_empty());
    }
}
