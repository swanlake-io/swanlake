use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use adbc_core::{
    options::AdbcVersion,
    options::OptionDatabase,
    options::OptionValue,
    Database,
    Driver,
};
use adbc_driver_manager::{ManagedConnection, ManagedDriver};
use adbc_driver_flightsql::DRIVER_PATH;
use anyhow::{anyhow, Context, Result};

pub(crate) struct CachedDriver {
    driver: Mutex<ManagedDriver>,
}

impl CachedDriver {
    fn new(driver: ManagedDriver) -> Self {
        Self {
            driver: Mutex::new(driver),
        }
    }

    pub(crate) fn new_connection(&self, endpoint: &str) -> Result<ManagedConnection> {
        let mut driver = self
            .driver
            .lock()
            .map_err(|e| anyhow!("Flight SQL driver mutex poisoned: {}", e))?;
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

static DRIVER_CACHE: OnceLock<Result<Arc<CachedDriver>>> = OnceLock::new();

pub(crate) fn get_cached_driver() -> Result<Arc<CachedDriver>> {
    match DRIVER_CACHE.get_or_init(|| {
        ManagedDriver::load_dynamic_from_filename(
            PathBuf::from(DRIVER_PATH),
            None,
            AdbcVersion::default(),
        )
        .with_context(|| "failed to load Flight SQL driver")
        .map(|driver| Arc::new(CachedDriver::new(driver)))
    }) {
        Ok(driver) => Ok(driver.clone()),
        Err(e) => Err(anyhow!("failed to load Flight SQL driver: {}", e)),
    }
}
