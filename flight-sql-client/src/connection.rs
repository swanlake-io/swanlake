use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

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

struct CachedDriver {
    driver: Mutex<ManagedDriver>,
}

impl CachedDriver {
    fn new(driver: ManagedDriver) -> Self {
        Self {
            driver: Mutex::new(driver),
        }
    }

    fn new_connection(&self, endpoint: &str) -> Result<ManagedConnection> {
        let mut driver = self
            .driver
            .lock()
            .expect("Flight SQL driver mutex poisoned");
        let database = driver
            .new_database_with_opts([(OptionDatabase::Uri, OptionValue::from(endpoint))])
            .with_context(|| "failed to create database handle")?;
        drop(driver);
        let connection = database
            .new_connection()
            .with_context(|| "failed to create Flight SQL connection")?;
        Ok(connection)
    }
}

static DRIVER_CACHE: OnceLock<Mutex<HashMap<PathBuf, Arc<CachedDriver>>>> = OnceLock::new();

fn get_cached_driver(path: &Path) -> Result<Arc<CachedDriver>> {
    let cache = DRIVER_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = cache
        .lock()
        .expect("Flight SQL driver cache mutex poisoned");
    let cache_key = path.to_path_buf();

    if let Some(entry) = guard.get(&cache_key) {
        return Ok(entry.clone());
    }

    let driver = ManagedDriver::load_dynamic_from_filename(path, None, AdbcVersion::default())
        .with_context(|| format!("failed to load Flight SQL driver from {}", path.display()))?;
    let entry = Arc::new(CachedDriver::new(driver));
    guard.insert(cache_key, entry.clone());
    Ok(entry)
}

impl FlightSqlConnectionBuilder {
    /// Create a new builder for the given endpoint.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use flight_sql_client::FlightSqlConnectionBuilder;
    ///
    /// let builder = FlightSqlConnectionBuilder::new("grpc://localhost:4214");
    /// ```
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            driver_path: PathBuf::from(DRIVER_PATH),
        }
    }

    /// Override the driver lookup path if needed.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use flight_sql_client::FlightSqlConnectionBuilder;
    /// use std::path::Path;
    ///
    /// let builder = FlightSqlConnectionBuilder::new("grpc://localhost:4214")
    ///     .with_driver_path("/custom/path/to/driver");
    /// ```
    pub fn with_driver_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.driver_path = path.into();
        self
    }

    /// Establish the connection synchronously.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use flight_sql_client::FlightSqlConnectionBuilder;
    ///
    /// let conn = FlightSqlConnectionBuilder::new("grpc://localhost:4214").connect()?;
    /// # Ok::<(), anyhow::Error>(())
    /// ```
    pub fn connect(&self) -> Result<ManagedConnection> {
        let driver = get_cached_driver(&self.driver_path)?;
        driver.new_connection(self.endpoint.as_str())
    }
}

/// Convenience helper to open a Flight SQL connection with default settings.
///
/// # Example
///
/// ```rust,ignore
/// use flight_sql_client::connect;
///
/// let conn = connect("grpc://localhost:4214")?;
/// # Ok::<(), anyhow::Error>(())
/// ```
pub fn connect(endpoint: &str) -> Result<ManagedConnection> {
    FlightSqlConnectionBuilder::new(endpoint).connect()
}
