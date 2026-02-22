use anyhow::{bail, Context, Result};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use tokio_postgres::{Client, NoTls};
use tracing::{debug, warn};

/// PostgreSQL connection configuration, built once from environment variables.
#[derive(Clone)]
struct PgConfig {
    connection_string: String,
    ssl_mode: PgSslMode,
}

#[derive(Clone, Copy, Debug)]
enum PgSslMode {
    Disable,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

impl PgSslMode {
    fn from_env() -> Self {
        let value = std::env::var("PGSSLMODE").unwrap_or_else(|_| "disable".to_string());
        Self::from_str(value.as_str())
    }

    fn from_str(value: &str) -> Self {
        match value.to_lowercase().as_str() {
            "prefer" => Self::Prefer,
            "require" => Self::Require,
            "verify-ca" => Self::VerifyCa,
            "verify-full" => Self::VerifyFull,
            _ => Self::Disable,
        }
    }
}

impl PgConfig {
    fn from_env() -> Self {
        let host = std::env::var("PGHOST").unwrap_or_else(|_| "localhost".to_string());
        let port = std::env::var("PGPORT").unwrap_or_else(|_| "5432".to_string());
        let user = std::env::var("PGUSER").unwrap_or_else(|_| "postgres".to_string());
        let dbname = std::env::var("PGDATABASE").unwrap_or_else(|_| "postgres".to_string());
        let password = std::env::var("PGPASSWORD").ok();

        let mut config = format!("host={host} port={port} user={user} dbname={dbname}");
        if let Some(pwd) = password {
            config.push_str(&format!(" password={pwd}"));
        }

        Self {
            connection_string: config,
            ssl_mode: PgSslMode::from_env(),
        }
    }

    fn get() -> &'static Self {
        use std::sync::OnceLock;
        static CONFIG: OnceLock<PgConfig> = OnceLock::new();
        CONFIG.get_or_init(Self::from_env)
    }
}

enum TlsConfig {
    None,
    Prefer(MakeTlsConnector),
    Enforced(MakeTlsConnector),
}

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsConfig::None => write!(f, "TlsConfig::None"),
            TlsConfig::Prefer(_) => write!(f, "TlsConfig::Prefer(..)"),
            TlsConfig::Enforced(_) => write!(f, "TlsConfig::Enforced(..)"),
        }
    }
}

async fn connect_with_tls(connection_string: &str, connector: MakeTlsConnector) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(connection_string, connector).await?;
    spawn_connection(connection);
    Ok(client)
}

async fn connect_without_tls(connection_string: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(connection_string, NoTls).await?;
    spawn_connection(connection);
    Ok(client)
}

fn spawn_connection<T>(connection: T)
where
    T: std::future::Future<Output = std::result::Result<(), tokio_postgres::Error>>
        + Send
        + 'static,
{
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            warn!(error = %e, "PostgreSQL connection error");
        }
    });
}

fn build_tls_connector(mode: PgSslMode) -> Result<MakeTlsConnector> {
    match mode {
        PgSslMode::Prefer | PgSslMode::Require => {
            let connector = TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true)
                .build()
                .context("failed to build TLS connector for PGSSLMODE=require")?;
            Ok(MakeTlsConnector::new(connector))
        }
        PgSslMode::VerifyCa => {
            let connector = TlsConnector::builder()
                .danger_accept_invalid_hostnames(true)
                .build()
                .context("failed to build TLS connector for PGSSLMODE=verify-ca")?;
            Ok(MakeTlsConnector::new(connector))
        }
        PgSslMode::VerifyFull => {
            let connector = TlsConnector::builder()
                .build()
                .context("failed to build TLS connector for PGSSLMODE=verify-full")?;
            Ok(MakeTlsConnector::new(connector))
        }
        PgSslMode::Disable => {
            bail!("PGSSLMODE=disable should not attempt to build a TLS connector")
        }
    }
}

fn build_tls_config(mode: PgSslMode) -> Result<TlsConfig> {
    match mode {
        PgSslMode::Disable => Ok(TlsConfig::None),
        PgSslMode::Prefer => Ok(TlsConfig::Prefer(build_tls_connector(mode)?)),
        PgSslMode::Require | PgSslMode::VerifyCa | PgSslMode::VerifyFull => {
            Ok(TlsConfig::Enforced(build_tls_connector(mode)?))
        }
    }
}

pub(super) async fn connect_client() -> Result<Client> {
    let config = PgConfig::get().clone();
    let tls_config = build_tls_config(config.ssl_mode)?;

    match tls_config {
        TlsConfig::None => {
            debug!("connecting to PostgreSQL without TLS");
            connect_without_tls(&config.connection_string).await
        }
        TlsConfig::Prefer(connector) => {
            debug!(
                "connecting to PostgreSQL with TLS mode {:?}",
                config.ssl_mode
            );
            match connect_with_tls(&config.connection_string, connector).await {
                Ok(client) => Ok(client),
                Err(err) => {
                    warn!(
                        error = %err,
                        "TLS connection failed in PGSSLMODE=prefer, retrying without TLS"
                    );
                    debug!("connecting to PostgreSQL without TLS (prefer fallback)");
                    connect_without_tls(&config.connection_string).await
                }
            }
        }
        TlsConfig::Enforced(connector) => {
            debug!(
                "connecting to PostgreSQL with TLS mode {:?}",
                config.ssl_mode
            );
            connect_with_tls(&config.connection_string, connector).await
        }
    }
}
