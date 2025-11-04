use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("duckdb error: {0}")]
    DuckDb(#[from] duckdb::Error),
    #[error("arrow error: {0}")]
    Arrow(#[from] arrow_schema::ArrowError),
    #[error("connection pool error: {0}")]
    Pool(#[from] r2d2::Error),
    #[error("write operations are disabled by configuration")]
    WritesDisabled,
    #[error("transaction not found")]
    TransactionNotFound,
    #[error("prepared statement not found")]
    PreparedStatementNotFound,
    #[error("maximum number of sessions reached")]
    MaxSessionsReached,
    #[error("unsupported parameter type: {0}")]
    UnsupportedParameter(String),
}
