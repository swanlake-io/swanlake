use std::path::PathBuf;

use adbc_core::{
    options::{AdbcVersion, OptionDatabase, OptionValue},
    Database, Driver,
};
use adbc_driver_flightsql::DRIVER_PATH;
use adbc_driver_manager::{ManagedConnection, ManagedDriver};
use anyhow::{Context, Result};

/// Configurable builder for establishing Flight SQL connections.
pub struct FlightSqlConnectionBuilder {
    endpoint: String,
    driver_path: PathBuf,
}

impl FlightSqlConnectionBuilder {
    /// Create a new builder for the given endpoint.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            driver_path: PathBuf::from(DRIVER_PATH),
        }
    }

    /// Override the driver lookup path if needed.
    pub fn with_driver_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.driver_path = path.into();
        self
    }

    /// Establish the connection synchronously.
    pub fn connect(&self) -> Result<ManagedConnection> {
        let mut driver = ManagedDriver::load_dynamic_from_filename(
            &self.driver_path,
            None,
            AdbcVersion::default(),
        )
        .with_context(|| {
            format!(
                "failed to load Flight SQL driver from {}",
                self.driver_path.display()
            )
        })?;

        let database = driver
            .new_database_with_opts([(OptionDatabase::Uri, OptionValue::from(
                self.endpoint.as_str(),
            ))])
            .with_context(|| "failed to create database handle")?;
        let connection = database
            .new_connection()
            .with_context(|| "failed to create Flight SQL connection")?;
        Ok(connection)
    }
}

/// Convenience helper to open a Flight SQL connection with default settings.
pub fn connect(endpoint: &str) -> Result<ManagedConnection> {
    FlightSqlConnectionBuilder::new(endpoint).connect()
}
