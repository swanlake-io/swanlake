use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use tokio::time::sleep;

pub fn reset_dir(dir: &Path) -> Result<()> {
    if dir.exists() {
        fs::remove_dir_all(dir).with_context(|| format!("failed to clear {}", dir.display()))?;
    }
    fs::create_dir_all(dir).with_context(|| format!("failed to create {}", dir.display()))
}

pub async fn wait_for_parquet_chunks<F>(root: &Path, condition: F) -> Result<()>
where
    F: Fn(usize) -> bool,
{
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let count = count_parquet_chunks(root)?;
        if condition(count) {
            return Ok(());
        }
        if std::time::Instant::now() > deadline {
            return Err(anyhow!(
                "timed out waiting for duckling queue chunks (last count: {count})"
            ));
        }
        sleep(Duration::from_millis(50)).await;
    }
}

pub fn count_parquet_chunks(root: &Path) -> Result<usize> {
    if !root.exists() {
        return Ok(0);
    }
    let mut count = 0usize;
    for entry in fs::read_dir(root)
        .with_context(|| format!("failed to read duckling queue dir {}", root.display()))?
    {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            count += count_parquet_chunks(&entry.path())?;
            continue;
        }
        if entry.path().extension().and_then(|ext| ext.to_str()) == Some("parquet") {
            count += 1;
        }
    }
    Ok(count)
}

pub fn first_parquet_chunk(root: &Path) -> Result<PathBuf> {
    if !root.exists() {
        return Err(anyhow!(
            "duckling queue root does not exist: {}",
            root.display()
        ));
    }
    for entry in fs::read_dir(root)
        .with_context(|| format!("failed to read duckling queue dir {}", root.display()))?
    {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            if let Ok(path) = first_parquet_chunk(&entry.path()) {
                return Ok(path);
            }
        } else if entry.path().extension().and_then(|ext| ext.to_str()) == Some("parquet") {
            return Ok(entry.path());
        }
    }
    Err(anyhow!("no parquet chunks found under {}", root.display()))
}
