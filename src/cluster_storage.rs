#![allow(clippy::result_large_err)]

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::{Cursor, Error as IoError, ErrorKind};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine};
use openraft::{
    Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, LogState, RaftLogReader,
    RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError, StoredMembership, Vote,
};
use redb::{Database, Durability, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
const LOG_ENTRIES: TableDefinition<u64, &str> = TableDefinition::new("entries");
const LOG_META: TableDefinition<&str, &str> = TableDefinition::new("metadata");
use tokio::sync::Mutex;

use crate::model::{Command, Error};
use crate::store::Engine;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = RaftCommand,
        R = RaftResponse,
        Node = openraft::BasicNode,
);

pub type Node = openraft::BasicNode;
pub type RaftEntry = Entry<TypeConfig>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RaftCommand {
    pub command: Command,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RaftResponse {
    pub result: std::result::Result<serde_json::Value, Error>,
}

#[derive(Clone)]
pub struct PersistentLog {
    database: Arc<Database>,
}

#[derive(Clone)]
pub struct PersistentStateMachine {
    engine: Arc<Engine>,
    inner: Arc<Mutex<StateDisk>>,
    metadata_path: PathBuf,
    snapshot_path: PathBuf,
    pending_snapshot_path: PathBuf,
    pending_metadata_path: PathBuf,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct LogDisk {
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
    last_purged: Option<LogId<u64>>,
    entries: BTreeMap<u64, RaftEntry>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct StateDisk {
    last_applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, Node>,
    current_snapshot: Option<SnapshotMeta<u64, Node>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SnapshotHeader {
    last_applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, Node>,
}

impl PersistentLog {
    pub fn open(directory: &Path) -> crate::model::Result<Self> {
        std::fs::create_dir_all(directory).map_err(model_storage_error)?;
        let database =
            Database::create(directory.join("raft-log.redb")).map_err(model_storage_error)?;
        let mut tx = database.begin_write().map_err(model_storage_error)?;
        tx.set_durability(Durability::Immediate);
        {
            let mut meta = tx.open_table(LOG_META).map_err(model_storage_error)?;
            let mut entries = tx.open_table(LOG_ENTRIES).map_err(model_storage_error)?;
            let initialized = meta
                .get("initialized")
                .map_err(model_storage_error)?
                .is_some();
            if !initialized {
                let old: LogDisk = read_json_or_default(&directory.join("raft-log.json"))
                    .map_err(model_storage_error)?;
                for (index, entry) in old.entries {
                    if index != entry.log_id.index {
                        return Err(Error::new("storage_error", "legacy log index mismatch"));
                    }
                    let value = serde_json::to_string(&entry).map_err(model_storage_error)?;
                    entries
                        .insert(index, value.as_str())
                        .map_err(model_storage_error)?;
                }
                for (k, v) in [
                    ("vote", serde_json::to_string(&old.vote)),
                    ("committed", serde_json::to_string(&old.committed)),
                    ("purged", serde_json::to_string(&old.last_purged)),
                ] {
                    let v = v.map_err(model_storage_error)?;
                    meta.insert(k, v.as_str()).map_err(model_storage_error)?;
                }
                meta.insert("initialized", "1")
                    .map_err(model_storage_error)?;
            }
        }
        tx.commit().map_err(model_storage_error)?;
        Ok(Self {
            database: Arc::new(database),
        })
    }
    fn read_meta<T: serde::de::DeserializeOwned>(
        &self,
        k: &str,
    ) -> Result<Option<T>, StorageError<u64>> {
        let tx = self.database.begin_read().map_err(log_error)?;
        let table = tx.open_table(LOG_META).map_err(log_error)?;
        let value = table.get(k).map_err(log_error)?;
        match value {
            Some(v) => serde_json::from_str(v.value()).map_err(log_error),
            None => Ok(None),
        }
    }
    fn write_meta<T: Serialize>(&self, k: &str, value: &T) -> Result<(), StorageError<u64>> {
        let value = serde_json::to_string(value).map_err(log_error)?;
        let mut tx = self.database.begin_write().map_err(log_error)?;
        tx.set_durability(Durability::Immediate);
        {
            let mut table = tx.open_table(LOG_META).map_err(log_error)?;
            table.insert(k, value.as_str()).map_err(log_error)?;
        }
        tx.commit().map_err(log_error)
    }
    fn append_durable(
        &self,
        entries: impl IntoIterator<Item = RaftEntry>,
    ) -> Result<(), StorageError<u64>> {
        let mut tx = self.database.begin_write().map_err(log_error)?;
        tx.set_durability(Durability::Immediate);
        {
            let mut table = tx.open_table(LOG_ENTRIES).map_err(log_error)?;
            for entry in entries {
                let value = serde_json::to_string(&entry).map_err(log_error)?;
                table
                    .insert(entry.log_id.index, value.as_str())
                    .map_err(log_error)?;
            }
        }
        tx.commit().map_err(log_error)
    }
}
fn log_error(error: impl std::fmt::Display) -> StorageError<u64> {
    storage_error(ErrorSubject::Logs, ErrorVerb::Write, error)
}

impl PersistentStateMachine {
    pub fn open(directory: &Path, engine: Arc<Engine>) -> crate::model::Result<Self> {
        std::fs::create_dir_all(directory).map_err(model_storage_error)?;
        let metadata_path = directory.join("state-meta.json");
        let snapshot_path = directory.join("snapshot.bin");
        let pending_snapshot_path = directory.join("snapshot-install.bin");
        let pending_metadata_path = directory.join("snapshot-install.json");
        let mut disk = read_json_or_default(&metadata_path).map_err(model_storage_error)?;
        recover_pending_snapshot(
            &engine,
            &mut disk,
            &metadata_path,
            &snapshot_path,
            &pending_snapshot_path,
            &pending_metadata_path,
        )?;
        Ok(Self {
            engine,
            inner: Arc::new(Mutex::new(disk)),
            metadata_path,
            snapshot_path,
            pending_snapshot_path,
            pending_metadata_path,
        })
    }

    pub fn engine(&self) -> Arc<Engine> {
        self.engine.clone()
    }
}

impl RaftLogReader<TypeConfig> for PersistentLog {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<RaftEntry>, StorageError<u64>> {
        let tx = self.database.begin_read().map_err(log_error)?;
        let table = tx.open_table(LOG_ENTRIES).map_err(log_error)?;
        let mut result = Vec::new();
        for row in table.range(range).map_err(log_error)? {
            let (_, value) = row.map_err(log_error)?;
            result.push(serde_json::from_str(value.value()).map_err(log_error)?);
        }
        Ok(result)
    }
}
impl RaftLogStorage<TypeConfig> for PersistentLog {
    type LogReader = Self;
    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let last_purged_log_id = self.read_meta("purged")?;
        let tx = self.database.begin_read().map_err(log_error)?;
        let table = tx.open_table(LOG_ENTRIES).map_err(log_error)?;
        let last = table.last().map_err(log_error)?;
        let last_log_id = match last {
            Some((_, value)) => Some(
                serde_json::from_str::<RaftEntry>(value.value())
                    .map_err(log_error)?
                    .log_id,
            ),
            None => last_purged_log_id,
        };
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }
    async fn get_log_reader(&mut self) -> Self {
        self.clone()
    }
    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.write_meta("vote", vote)
    }
    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        self.read_meta("vote")
    }
    async fn save_committed(&mut self, value: Option<LogId<u64>>) -> Result<(), StorageError<u64>> {
        self.write_meta("committed", &value)
    }
    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        self.read_meta("committed")
    }
    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = RaftEntry> + Send,
        I::IntoIter: Send,
    {
        match self.append_durable(entries) {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(error) => {
                callback.log_io_completed(Err(IoError::other(error.to_string())));
                Err(error)
            }
        }
    }
    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut tx = self.database.begin_write().map_err(log_error)?;
        tx.set_durability(Durability::Immediate);
        {
            let mut table = tx.open_table(LOG_ENTRIES).map_err(log_error)?;
            let keys = table
                .range(log_id.index..)
                .map_err(log_error)?
                .map(|row| row.map(|(k, _)| k.value()))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(log_error)?;
            for k in keys {
                table.remove(k).map_err(log_error)?;
            }
        }
        tx.commit().map_err(log_error)
    }
    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut tx = self.database.begin_write().map_err(log_error)?;
        tx.set_durability(Durability::Immediate);
        {
            let mut table = tx.open_table(LOG_ENTRIES).map_err(log_error)?;
            let keys = table
                .range(..=log_id.index)
                .map_err(log_error)?
                .map(|row| row.map(|(k, _)| k.value()))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(log_error)?;
            for k in keys {
                table.remove(k).map_err(log_error)?;
            }
            let mut meta = tx.open_table(LOG_META).map_err(log_error)?;
            let value = serde_json::to_string(&log_id).map_err(log_error)?;
            meta.insert("purged", value.as_str()).map_err(log_error)?;
        }
        tx.commit().map_err(log_error)
    }
}

impl RaftStateMachine<TypeConfig> for PersistentStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, Node>), StorageError<u64>> {
        let disk = self.inner.lock().await;
        Ok((disk.last_applied, disk.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<RaftResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = RaftEntry> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();
        let mut disk = self.inner.lock().await;
        for entry in entries {
            let response = match &entry.payload {
                EntryPayload::Blank => RaftResponse {
                    result: Ok(serde_json::Value::Null),
                },
                EntryPayload::Membership(membership) => {
                    disk.membership = StoredMembership::new(Some(entry.log_id), membership.clone());
                    RaftResponse {
                        result: Ok(serde_json::Value::Null),
                    }
                }
                EntryPayload::Normal(request) => {
                    // The engine transaction is durable before the applied boundary below. If the
                    // process stops between them, OpenRaft reapplies this identical command and
                    // Engine request-id deduplication returns the original result.
                    let result = {
                        let engine = self.engine.clone();
                        let command = request.command.clone();
                        tokio::task::spawn_blocking(move || engine.execute(&command))
                            .await
                            .map_err(|error| {
                                storage_error(ErrorSubject::StateMachine, ErrorVerb::Write, error)
                            })?
                    };
                    RaftResponse {
                        result: match result {
                            Err(error)
                                if matches!(
                                    error.code.as_str(),
                                    "storage_error" | "internal" | "unsupported_format"
                                ) =>
                            {
                                return Err(storage_error(
                                    ErrorSubject::StateMachine,
                                    ErrorVerb::Write,
                                    error,
                                ))
                            }
                            other => other,
                        },
                    }
                }
            };
            disk.last_applied = Some(entry.log_id);
            persist_json(
                &self.metadata_path,
                &*disk,
                ErrorSubject::StateMachine,
                ErrorVerb::Write,
            )?;
            responses.push(response);
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, Node>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let bytes = snapshot.into_inner();
        let (header, engine_bytes) = decode_snapshot(&bytes)?;
        if header.last_applied != meta.last_log_id || header.membership != meta.last_membership {
            return Err(storage_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Read,
                "snapshot metadata does not match its payload",
            ));
        }

        let mut disk = self.inner.lock().await;
        persist_bytes(
            &self.pending_snapshot_path,
            &bytes,
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
        )?;
        persist_json(
            &self.pending_metadata_path,
            meta,
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
        )?;
        self.engine
            .import_state(engine_bytes)
            .map_err(|error| storage_error(ErrorSubject::StateMachine, ErrorVerb::Write, error))?;
        disk.last_applied = header.last_applied;
        disk.membership = header.membership;
        disk.current_snapshot = Some(meta.clone());
        persist_json(
            &self.metadata_path,
            &*disk,
            ErrorSubject::StateMachine,
            ErrorVerb::Write,
        )?;
        promote_snapshot(&self.pending_snapshot_path, &self.snapshot_path)?;
        remove_durable(&self.pending_metadata_path)?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let disk = self.inner.lock().await;
        let Some(meta) = disk.current_snapshot.clone() else {
            return Ok(None);
        };
        let bytes = std::fs::read(&self.snapshot_path).map_err(|error| {
            StorageError::from_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, error)
        })?;
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        }))
    }
}

impl RaftSnapshotBuilder<TypeConfig> for PersistentStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        static SNAPSHOT_SEQUENCE: AtomicU64 = AtomicU64::new(1);

        let mut disk = self.inner.lock().await;
        let engine_bytes = self
            .engine
            .export_state()
            .map_err(|error| storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, error))?;
        let header = SnapshotHeader {
            last_applied: disk.last_applied,
            membership: disk.membership.clone(),
        };
        let bytes = encode_snapshot(&header, &engine_bytes)?;
        let sequence = SNAPSHOT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let snapshot_id = match disk.last_applied {
            Some(log_id) => format!("{}-{sequence}", log_id),
            None => format!("empty-{sequence}"),
        };
        let meta = SnapshotMeta {
            last_log_id: disk.last_applied,
            last_membership: disk.membership.clone(),
            snapshot_id,
        };
        persist_bytes(
            &self.snapshot_path,
            &bytes,
            ErrorSubject::Snapshot(None),
            ErrorVerb::Write,
        )?;
        disk.current_snapshot = Some(meta.clone());
        persist_json(
            &self.metadata_path,
            &*disk,
            ErrorSubject::StateMachine,
            ErrorVerb::Write,
        )?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        })
    }
}

fn read_json_or_default<T>(path: &Path) -> std::io::Result<T>
where
    T: for<'de> Deserialize<'de> + Default,
{
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(|error| IoError::new(ErrorKind::InvalidData, error)),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(T::default()),
        Err(error) => Err(error),
    }
}

fn persist_json<T: Serialize>(
    path: &Path,
    value: &T,
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
) -> Result<(), StorageError<u64>> {
    let bytes =
        serde_json::to_vec(value).map_err(|error| storage_error(subject.clone(), verb, error))?;
    persist_bytes(path, &bytes, subject, verb)
}

fn persist_bytes(
    path: &Path,
    bytes: &[u8],
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
) -> Result<(), StorageError<u64>> {
    let temporary = path.with_extension("tmp");
    let result = (|| -> std::io::Result<()> {
        use std::io::Write;
        let mut file = std::fs::File::create(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        if let Some(parent) = path.parent() {
            std::fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    })();
    result.map_err(|error| StorageError::from_io_error(subject, verb, error))
}

fn encode_snapshot(
    header: &SnapshotHeader,
    engine_bytes: &[u8],
) -> Result<Vec<u8>, StorageError<u64>> {
    let header = serde_json::to_vec(header)
        .map_err(|error| storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, error))?;
    let length = u64::try_from(header.len())
        .map_err(|error| storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, error))?;
    let mut bytes = Vec::with_capacity(8 + header.len() + engine_bytes.len());
    bytes.extend_from_slice(&length.to_be_bytes());
    bytes.extend_from_slice(&header);
    bytes.extend_from_slice(engine_bytes);
    Ok(bytes)
}

fn decode_snapshot(bytes: &[u8]) -> Result<(SnapshotHeader, &[u8]), StorageError<u64>> {
    let length_bytes: [u8; 8] = bytes
        .get(..8)
        .and_then(|value| value.try_into().ok())
        .ok_or_else(|| {
            storage_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Read,
                "snapshot header is truncated",
            )
        })?;
    let length = usize::try_from(u64::from_be_bytes(length_bytes))
        .map_err(|error| storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, error))?;
    let header_end = 8usize.checked_add(length).ok_or_else(|| {
        storage_error(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Read,
            "snapshot header length overflow",
        )
    })?;
    let header_bytes = bytes.get(8..header_end).ok_or_else(|| {
        storage_error(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Read,
            "snapshot header is truncated",
        )
    })?;
    let header = serde_json::from_slice(header_bytes)
        .map_err(|error| storage_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, error))?;
    Ok((header, &bytes[header_end..]))
}

fn recover_pending_snapshot(
    engine: &Engine,
    disk: &mut StateDisk,
    metadata_path: &Path,
    snapshot_path: &Path,
    pending_snapshot_path: &Path,
    pending_metadata_path: &Path,
) -> crate::model::Result<()> {
    let meta: SnapshotMeta<u64, Node> = match std::fs::read(pending_metadata_path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map_err(|error| Error::new("storage_error", error.to_string()))?,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(model_storage_error(error)),
    };
    let source = if pending_snapshot_path.exists() {
        pending_snapshot_path
    } else {
        snapshot_path
    };
    let bytes = std::fs::read(source).map_err(model_storage_error)?;
    let (header, engine_bytes) =
        decode_snapshot(&bytes).map_err(|error| Error::new("storage_error", error.to_string()))?;
    if header.last_applied != meta.last_log_id || header.membership != meta.last_membership {
        return Err(Error::new(
            "storage_error",
            "pending snapshot metadata does not match its payload",
        ));
    }
    engine.import_state(engine_bytes)?;
    disk.last_applied = header.last_applied;
    disk.membership = header.membership;
    disk.current_snapshot = Some(meta);
    persist_json(
        metadata_path,
        disk,
        ErrorSubject::StateMachine,
        ErrorVerb::Write,
    )
    .map_err(|error| Error::new("storage_error", error.to_string()))?;
    if pending_snapshot_path.exists() {
        promote_snapshot(pending_snapshot_path, snapshot_path)
            .map_err(|error| Error::new("storage_error", error.to_string()))?;
    }
    remove_durable(pending_metadata_path)
        .map_err(|error| Error::new("storage_error", error.to_string()))
}

fn promote_snapshot(source: &Path, destination: &Path) -> Result<(), StorageError<u64>> {
    std::fs::rename(source, destination).map_err(|error| {
        StorageError::from_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, error)
    })?;
    sync_parent(destination)
}

fn remove_durable(path: &Path) -> Result<(), StorageError<u64>> {
    match std::fs::remove_file(path) {
        Ok(()) => sync_parent(path),
        Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
        Err(error) => Err(StorageError::from_io_error(
            ErrorSubject::Snapshot(None),
            ErrorVerb::Delete,
            error,
        )),
    }
}

fn sync_parent(path: &Path) -> Result<(), StorageError<u64>> {
    if let Some(parent) = path.parent() {
        std::fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| {
                StorageError::from_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Write, error)
            })?;
    }
    Ok(())
}

fn storage_error(
    subject: ErrorSubject<u64>,
    verb: ErrorVerb,
    error: impl std::fmt::Display,
) -> StorageError<u64> {
    StorageError::from_io_error(subject, verb, IoError::other(error.to_string()))
}

fn model_storage_error(error: impl std::fmt::Display) -> Error {
    Error::new("storage_error", error.to_string())
}

#[cfg(test)]
mod log_tests {
    use super::*;
    fn entry(index: u64) -> RaftEntry {
        Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Blank,
        }
    }
    #[tokio::test]
    async fn incremental_log_reopens_truncates_purges_and_imports_once() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = PersistentLog::open(dir.path()).unwrap();
        log.append_durable((1..=5).map(entry)).unwrap();
        let vote = Vote::new_committed(2, 1);
        log.save_vote(&vote).await.unwrap();
        log.save_committed(Some(entry(3).log_id)).await.unwrap();
        log.truncate(entry(5).log_id).await.unwrap();
        log.purge(entry(2).log_id).await.unwrap();
        drop(log);
        let mut log = PersistentLog::open(dir.path()).unwrap();
        assert_eq!(log.read_vote().await.unwrap(), Some(vote));
        assert_eq!(log.read_committed().await.unwrap(), Some(entry(3).log_id));
        let entries = log.try_get_log_entries(0..10).await.unwrap();
        assert_eq!(
            entries.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            vec![3, 4]
        );
        let state = log.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(entry(2).log_id));
        assert_eq!(state.last_log_id, Some(entry(4).log_id));
        drop(log);
        let old = tempfile::tempdir().unwrap();
        let legacy = LogDisk {
            entries: BTreeMap::from([(1, entry(1))]),
            ..Default::default()
        };
        std::fs::write(
            old.path().join("raft-log.json"),
            serde_json::to_vec(&legacy).unwrap(),
        )
        .unwrap();
        let mut log = PersistentLog::open(old.path()).unwrap();
        assert_eq!(log.try_get_log_entries(..).await.unwrap().len(), 1);
        log.append_durable([entry(2)]).unwrap();
        drop(log);
        std::fs::write(old.path().join("raft-log.json"), "invalid old file").unwrap();
        let mut log = PersistentLog::open(old.path()).unwrap();
        assert_eq!(log.try_get_log_entries(..).await.unwrap().len(), 2);
    }
}
