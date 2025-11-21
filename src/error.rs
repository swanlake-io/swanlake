use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("duckdb error: {0}")]
    DuckDb(#[from] duckdb::Error),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("transaction was aborted and rolled back")]
    TransactionAborted,
    #[error("transaction not found")]
    TransactionNotFound,
    #[error("prepared statement not found")]
    PreparedStatementNotFound,
    #[error("maximum number of sessions reached")]
    MaxSessionsReached,
    #[error("unsupported parameter type: {0}")]
    UnsupportedParameter(String),
    #[error(
        "duckling queue runtime is disabled; enable it via SWANLAKE_DUCKLING_QUEUE_ENABLED=true"
    )]
    DucklingQueueDisabled,
    #[error("duckling queue tables are write-only; only INSERT INTO duckling_queue.* is allowed")]
    DucklingQueueWriteOnly,
    #[error("internal error: {0}")]
    Internal(String),
}
