use std::fs;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::str;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use chrono::Utc;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tracing::warn;

use crate::error::ServerError;

/// On-disk chunk that survives process crashes.
#[derive(Debug, Clone)]
pub struct DurableChunk {
    path: PathBuf,
}

impl DurableChunk {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Chunk restored from disk at startup.
#[derive(Debug)]
pub struct PersistedChunk {
    pub table: String,
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
    pub handle: DurableChunk,
}

/// Simple durable storage that writes buffered batches to Arrow IPC files.
pub struct DurableStorage {
    root: PathBuf,
}

impl DurableStorage {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, ServerError> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(|err| {
            ServerError::Internal(format!(
                "failed to create duckling queue root {}: {err}",
                root.display()
            ))
        })?;
        Ok(Self { root })
    }

    pub fn persist_chunk(
        &self,
        table: &str,
        schema: &SchemaRef,
        batches: &[RecordBatch],
    ) -> Result<DurableChunk, ServerError> {
        let table_dir = self.table_dir(table)?;
        fs::create_dir_all(&table_dir).map_err(|err| {
            ServerError::Internal(format!(
                "failed to create duckling queue table dir {}: {err}",
                table_dir.display()
            ))
        })?;

        let file_name = format!(
            "{}-{}.parquet",
            Utc::now().timestamp_millis(),
            uuid::Uuid::new_v4()
        );
        let path = table_dir.join(file_name);
        let file = File::create(&path).map_err(|err| {
            ServerError::Internal(format!(
                "failed to create duckling queue chunk {}: {err}",
                path.display()
            ))
        })?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut writer =
            ArrowWriter::try_new(file, schema.clone(), Some(props)).map_err(|err| {
                ServerError::Internal(format!("failed to create parquet writer: {err}"))
            })?;
        for batch in batches {
            writer.write(batch).map_err(|err| {
                ServerError::Internal(format!("failed to write parquet batch: {err}"))
            })?;
        }
        writer.close().map_err(|err| {
            ServerError::Internal(format!("failed to finish parquet file: {err}"))
        })?;
        Ok(DurableChunk::new(path))
    }

    pub fn load_pending(&self) -> Result<Vec<PersistedChunk>, ServerError> {
        let mut restored = Vec::new();
        if !self.root.exists() {
            return Ok(restored);
        }

        let mut table_dirs = self
            .scandir(&self.root)?
            .into_iter()
            .filter(|entry| entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false))
            .collect::<Vec<_>>();
        table_dirs.sort_by_key(|e| e.file_name());

        for entry in table_dirs {
            let dir_name = entry.file_name();
            let dir_name = dir_name.to_string_lossy();
            let Some(table) = decode_table_dir_name(&dir_name) else {
                continue;
            };
            let mut chunk_paths = self
                .scandir(&entry.path())?
                .into_iter()
                .filter(|chunk| chunk.file_type().map(|ft| ft.is_file()).unwrap_or(false))
                .map(|chunk| chunk.path())
                .filter(|path| {
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .map(|name| name.ends_with(".parquet"))
                        .unwrap_or(false)
                })
                .collect::<Vec<_>>();
            chunk_paths.sort();

            for chunk_path in chunk_paths {
                match self.read_chunk(&table, &chunk_path) {
                    Ok(chunk) => restored.push(chunk),
                    Err(err) => {
                        warn!(
                            path = %chunk_path.display(),
                            error = %err,
                            "failed to load duckling queue chunk from disk; skipping chunk"
                        );
                    }
                }
            }
        }
        Ok(restored)
    }

    pub fn remove_chunks(&self, chunks: Vec<DurableChunk>) -> Result<(), ServerError> {
        for chunk in chunks {
            let processed_path = processed_chunk_path(&chunk.path);
            match fs::rename(&chunk.path, &processed_path) {
                Ok(_) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    self.cleanup_table_dir(chunk.path.parent());
                    continue;
                }
                Err(err) => {
                    return Err(ServerError::Internal(format!(
                        "failed to mark duckling queue chunk {} for cleanup: {err}",
                        chunk.path.display()
                    )));
                }
            }

            match fs::remove_file(&processed_path) {
                Ok(_) => self.cleanup_table_dir(processed_path.parent()),
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    self.cleanup_table_dir(processed_path.parent())
                }
                Err(err) => {
                    warn!(
                        path = %processed_path.display(),
                        error = %err,
                        "failed to remove processed duckling queue chunk; will retry later"
                    );
                }
            }
        }
        Ok(())
    }

    fn scandir(&self, dir: &Path) -> Result<Vec<fs::DirEntry>, ServerError> {
        let iter = match fs::read_dir(dir) {
            Ok(iter) => iter,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(err) => {
                return Err(ServerError::Internal(format!(
                    "failed to list directory {}: {err}",
                    dir.display()
                )))
            }
        };
        let mut entries = Vec::new();
        for entry in iter {
            let entry = entry.map_err(|err| {
                ServerError::Internal(format!("failed to read directory entry: {err}"))
            })?;
            entries.push(entry);
        }
        Ok(entries)
    }

    fn table_dir(&self, table: &str) -> Result<PathBuf, ServerError> {
        let encoded = encode_table_dir_name(table)?;
        Ok(self.root.join(encoded))
    }

    fn cleanup_table_dir(&self, dir: Option<&Path>) {
        if let Some(path) = dir {
            if path == self.root {
                return;
            }
            if let Ok(mut iter) = fs::read_dir(path) {
                if iter.next().is_none() {
                    let _ = fs::remove_dir(path);
                }
            }
        }
    }
    fn read_chunk(&self, table: &str, chunk_path: &Path) -> Result<PersistedChunk, ServerError> {
        let file = File::open(chunk_path).map_err(|err| {
            ServerError::Internal(format!(
                "failed to open duckling queue chunk {}: {err}",
                chunk_path.display()
            ))
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|err| {
            ServerError::Internal(format!(
                "failed to build parquet reader for {}: {err}",
                chunk_path.display()
            ))
        })?;
        let schema = builder.schema().clone();
        let mut reader = builder
            .build()
            .map_err(|err| ServerError::Internal(format!("failed to read parquet file: {err}")))?;
        let mut batches = Vec::new();
        for item in reader.by_ref() {
            let batch = item.map_err(|err| {
                ServerError::Internal(format!("failed to read parquet batch: {err}"))
            })?;
            batches.push(batch);
        }
        Ok(PersistedChunk {
            table: table.to_string(),
            schema: schema.clone(),
            batches,
            handle: DurableChunk::new(chunk_path.to_path_buf()),
        })
    }
}

fn encode_table_dir_name(table: &str) -> Result<String, ServerError> {
    if table.is_empty() {
        return Err(ServerError::Internal(
            "duckling queue table name cannot be empty".into(),
        ));
    }
    let mut encoded = String::from("table-");
    encoded.reserve(table.len() * 2);
    for byte in table.as_bytes() {
        encoded.push_str(&format!("{:02x}", byte));
    }
    Ok(encoded)
}

fn decode_table_dir_name(dir_name: &str) -> Option<String> {
    let encoded = dir_name.strip_prefix("table-")?;
    if encoded.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(encoded.len() / 2);
    let chars = encoded.as_bytes();
    let mut i = 0;
    while i < chars.len() {
        let hi = chars.get(i)?;
        let lo = chars.get(i + 1)?;
        let pair = [*hi, *lo];
        let byte = std::str::from_utf8(&pair)
            .ok()
            .and_then(|hex| u8::from_str_radix(hex, 16).ok())?;
        bytes.push(byte);
        i += 2;
    }
    String::from_utf8(bytes).ok()
}

fn processed_chunk_path(original: &Path) -> PathBuf {
    let mut processed = original.to_path_buf();
    processed.set_extension("done");
    processed
}
