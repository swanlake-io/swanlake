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
    #[error("transaction not found: {0}")]
    TransactionNotFound(String),
    #[error("prepared statement not found: {0}")]
    PreparedStatementNotFound(String),
    #[error("unsupported parameter type: {0}")]
    UnsupportedParameter(String),
}
