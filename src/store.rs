use std::collections::BTreeMap;
use std::path::Path;
use std::sync::RwLock;

use redb::{
    Database, Durability, ReadTransaction, ReadableTable, TableDefinition, WriteTransaction,
};
use serde_json::{json, Value};

use crate::documents;
use crate::model::{Command, Error, Result};

const DATA: TableDefinition<&str, &str> = TableDefinition::new("substrate_data");
const FORMAT_KEY: &str = "format_version";
const FORMAT_VERSION: u64 = 1;
const MAX_IDENTIFIER_BYTES: usize = 256;

pub trait Kv {
    fn get(&self, bucket: &str, key: &str) -> Result<Option<Value>>;
    fn scan(&self, bucket: &str, prefix: &str) -> Result<Vec<(String, Value)>>;
    fn scan_page(
        &self,
        bucket: &str,
        prefix: &str,
        after: &str,
        limit: usize,
    ) -> Result<Vec<(String, Value)>> {
        Ok(self
            .scan(bucket, prefix)?
            .into_iter()
            .filter(|(k, _)| k.as_str() > after)
            .take(limit)
            .collect())
    }
    fn put(&mut self, bucket: &str, key: &str, value: &Value) -> Result<()>;
    fn delete(&mut self, bucket: &str, key: &str) -> Result<()>;
}

pub fn key(parts: &[&str]) -> String {
    let mut output = String::new();
    for part in parts {
        for byte in part.as_bytes() {
            use std::fmt::Write;
            let _ = write!(output, "{byte:02x}");
        }
        output.push('/');
    }
    output
}

pub fn emit(
    kv: &mut dyn Kv,
    cmd: &Command,
    kind: &str,
    resource: &str,
    data: Value,
) -> Result<u64> {
    let current = match kv.get("meta", "event_sequence")? {
        Some(value) => value
            .as_u64()
            .ok_or_else(|| Error::new("storage_error", "invalid event sequence metadata"))?,
        None => 0,
    };
    let sequence = current
        .checked_add(1)
        .ok_or_else(|| Error::new("storage_error", "event sequence exhausted"))?;
    let event = json!({
        "sequence": sequence,
        "timestamp_ms": cmd.now_ms,
        "kind": kind,
        "resource": resource,
        "data": data,
    });
    kv.put("events", &format!("{sequence:020}"), &event)?;
    kv.put("meta", "event_sequence", &json!(sequence))?;
    Ok(sequence)
}

#[derive(Clone, Default)]
pub struct MemoryKv {
    entries: BTreeMap<(String, String), Value>,
}

impl Kv for MemoryKv {
    fn get(&self, bucket: &str, key: &str) -> Result<Option<Value>> {
        Ok(self
            .entries
            .get(&(bucket.to_owned(), key.to_owned()))
            .cloned())
    }

    fn scan(&self, bucket: &str, prefix: &str) -> Result<Vec<(String, Value)>> {
        let mut values = Vec::new();
        for ((entry_bucket, entry_key), value) in &self.entries {
            if entry_bucket == bucket && entry_key.starts_with(prefix) {
                values.push((entry_key.clone(), value.clone()));
            }
        }
        Ok(values)
    }

    fn put(&mut self, bucket: &str, key: &str, value: &Value) -> Result<()> {
        self.entries
            .insert((bucket.to_owned(), key.to_owned()), value.clone());
        Ok(())
    }

    fn delete(&mut self, bucket: &str, key: &str) -> Result<()> {
        self.entries.remove(&(bucket.to_owned(), key.to_owned()));
        Ok(())
    }
}

pub struct Engine {
    database: Database,
    execution_lock: RwLock<()>,
}

impl Engine {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let database = Database::create(path).map_err(database_error)?;
        {
            let mut transaction = database.begin_write().map_err(database_error)?;
            transaction.set_durability(Durability::Immediate);
            let mut table = transaction.open_table(DATA).map_err(database_error)?;
            let stored = table
                .get(FORMAT_KEY)
                .map_err(database_error)?
                .map(|value| value.value().to_owned());
            if let Some(stored) = stored {
                let version: u64 = stored
                    .parse()
                    .map_err(|_| Error::new("storage_error", "invalid database format metadata"))?;
                if version > FORMAT_VERSION {
                    return Err(Error::new(
                        "unsupported_format",
                        format!("database format {version} is newer than supported format {FORMAT_VERSION}"),
                    ));
                }
            } else {
                table
                    .insert(FORMAT_KEY, FORMAT_VERSION.to_string().as_str())
                    .map_err(database_error)?;
            }
            drop(table);
            transaction.commit().map_err(database_error)?;
        }
        Ok(Self {
            database,
            execution_lock: RwLock::new(()),
        })
    }

    pub fn execute(&self, cmd: &Command) -> Result<Value> {
        validate_command(cmd)?;
        // redb serializes writers and provides MVCC read snapshots. The lifecycle
        // guard only excludes an operator import; readers need not block writers.
        let _guard = self
            .execution_lock
            .read()
            .map_err(|_| Error::new("storage_error", "execution lock poisoned"))?;
        self.execute_transaction(cmd, is_mutation(&cmd.action))
    }

    fn execute_transaction(&self, cmd: &Command, mutation: bool) -> Result<Value> {
        if !mutation {
            let transaction = self.database.begin_read().map_err(database_error)?;
            let mut kv = ReadKv {
                transaction: &transaction,
                scope: key(&[&cmd.namespace, &cmd.partition]),
            };
            let result = dispatch(&mut kv, cmd)?;
            if serde_json::to_vec(&result)
                .map_err(json_storage_error)?
                .len()
                > 4 * 1024 * 1024
            {
                return Err(Error::new(
                    "response_too_large",
                    "result exceeds 4 MiB; narrow the query or page through smaller results",
                ));
            }
            return Ok(result);
        }
        let mut transaction = self.database.begin_write().map_err(database_error)?;
        transaction.set_durability(Durability::Immediate);
        let mut kv = RedbKv::new(&transaction, &cmd.namespace, &cmd.partition);

        if mutation {
            if let Some(saved) = kv.get("requests", &key(&[&cmd.request_id]))? {
                let saved_object = saved.as_object().ok_or_else(|| {
                    Error::new("storage_error", "invalid request deduplication record")
                })?;
                let content = saved_object.get("content").ok_or_else(|| {
                    Error::new("storage_error", "invalid request deduplication content")
                })?;
                if content != &canonical_content(cmd) {
                    return Err(Error::new(
                        "conflict",
                        "request_id was already used with different command content",
                    ));
                }
                return saved_object.get("result").cloned().ok_or_else(|| {
                    Error::new("storage_error", "invalid request deduplication result")
                });
            }
        }

        let result = dispatch(&mut kv, cmd)?;
        if !kv.changed
            && matches!(
                cmd.action.as_str(),
                "job.claim" | "vector.flush" | "schedule.tick" | "scope.maintain"
            )
        {
            return Ok(result);
        }
        if mutation {
            kv.put(
                "requests",
                &key(&[&cmd.request_id]),
                &json!({"content": canonical_content(cmd), "result": result, "created_at_ms":cmd.now_ms}),
            )?;
            let mut table = transaction.open_table(DATA).map_err(database_error)?;
            table
                .insert(
                    format!("scope/{}", key(&[&cmd.namespace, &cmd.partition])).as_str(),
                    serde_json::to_string(&(&cmd.namespace, &cmd.partition))
                        .map_err(json_storage_error)?
                        .as_str(),
                )
                .map_err(database_error)?;
        }
        let limits = kv.get("scope_settings", "limits")?.unwrap_or(json!({}));
        let bytes = kv
            .get("accounting", "bytes")?
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let max = limits["max_bytes"].as_u64().unwrap_or(1024 * 1024 * 1024);
        if bytes > max
            && kv.byte_delta > 0
            && !matches!(cmd.action.as_str(), "scope.configure" | "scope.maintain")
        {
            return Err(Error::new(
                "quota_exceeded",
                "partition storage limit reached",
            ));
        }
        drop(kv);
        transaction.commit().map_err(database_error)?;
        Ok(result)
    }

    pub fn scopes(&self) -> Result<Vec<(String, String)>> {
        let tx = self.database.begin_read().map_err(database_error)?;
        let table = tx.open_table(DATA).map_err(database_error)?;
        let mut scopes = Vec::new();
        for item in table.range("scope/"..).map_err(database_error)? {
            let (k, v) = item.map_err(database_error)?;
            if !k.value().starts_with("scope/") {
                break;
            }
            scopes.push(serde_json::from_str(v.value()).map_err(json_storage_error)?);
        }
        Ok(scopes)
    }
    pub fn export_state(&self) -> Result<Vec<u8>> {
        let _lock = self
            .execution_lock
            .read()
            .map_err(|_| Error::new("storage_error", "lock poisoned"))?;
        let tx = self.database.begin_read().map_err(database_error)?;
        let table = tx.open_table(DATA).map_err(database_error)?;
        let mut state = BTreeMap::new();
        for row in table.iter().map_err(database_error)? {
            let (k, v) = row.map_err(database_error)?;
            state.insert(k.value().to_string(), v.value().to_string());
        }
        serde_json::to_vec(&state).map_err(json_storage_error)
    }
    pub fn import_state(&self, bytes: &[u8]) -> Result<()> {
        self.import(bytes, false)
    }
    fn import(&self, bytes: &[u8], pause: bool) -> Result<()> {
        let state: BTreeMap<String, String> =
            serde_json::from_slice(bytes).map_err(json_storage_error)?;
        if state.get(FORMAT_KEY).map(String::as_str) != Some("1") {
            return Err(Error::new(
                "unsupported_format",
                "snapshot format is not supported",
            ));
        }
        let _lock = self
            .execution_lock
            .write()
            .map_err(|_| Error::new("storage_error", "lock poisoned"))?;
        let mut tx = self.database.begin_write().map_err(database_error)?;
        tx.set_durability(Durability::Immediate);
        {
            let mut table = tx.open_table(DATA).map_err(database_error)?;
            let keys = table
                .iter()
                .map_err(database_error)?
                .map(|row| row.map(|(k, _)| k.value().to_string()))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(database_error)?;
            for k in keys {
                table.remove(k.as_str()).map_err(database_error)?;
            }
            for (k, v) in &state {
                table
                    .insert(k.as_str(), v.as_str())
                    .map_err(database_error)?;
            }
        }
        if pause {
            for (k, v) in state.iter().filter(|(k, _)| k.starts_with("scope/")) {
                let _ = k;
                let (ns, part): (String, String) =
                    serde_json::from_str(v).map_err(json_storage_error)?;
                let mut kv = RedbKv::new(&tx, &ns, &part);
                for (_, j) in kv.scan("jobs", "")? {
                    if let Some(queue) = j["queue"].as_str() {
                        kv.put("queue_settings", &key(&[queue]), &json!({"paused":true}))?;
                    }
                }
                for (k, mut s) in kv.scan("schedules", "")? {
                    s["paused"] = json!(true);
                    kv.put("schedules", &k, &s)?;
                }
            }
        }
        tx.commit().map_err(database_error)
    }
    pub fn backup(&self, path: impl AsRef<Path>) -> Result<()> {
        use sha2::{Digest, Sha256};
        use std::io::Write;
        let state = self.export_state()?;
        let digest = format!("{:x}", Sha256::digest(&state));
        let image = json!({"format":"substratedb-backup-v1","version":env!("CARGO_PKG_VERSION"),"boundary":"single-engine-atomic","sha256":digest,"state":serde_json::from_slice::<Value>(&state).map_err(json_storage_error)?});
        let state = serde_json::to_vec(&image).map_err(json_storage_error)?;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(database_error)?;
        file.write_all(&state).map_err(database_error)?;
        file.sync_all().map_err(database_error)
    }
    pub fn restore(&self, path: impl AsRef<Path>) -> Result<()> {
        use sha2::{Digest, Sha256};
        let bytes = std::fs::read(path).map_err(database_error)?;
        let image: Value = serde_json::from_slice(&bytes).map_err(json_storage_error)?;
        if image["format"] != "substratedb-backup-v1" {
            return Err(Error::new(
                "unsupported_format",
                "unsupported backup manifest",
            ));
        }
        let bytes = serde_json::to_vec(&image["state"]).map_err(json_storage_error)?;
        let digest = format!("{:x}", Sha256::digest(&bytes));
        if image["sha256"].as_str() != Some(digest.as_str()) {
            return Err(Error::new(
                "storage_error",
                "backup checksum does not match",
            ));
        }
        self.import(&bytes, true)
    }
}

pub fn is_mutation(action: &str) -> bool {
    if action == "transaction" {
        return true;
    }
    if action.starts_with("scope.") {
        return !matches!(action, "scope.stats" | "scope.work");
    }
    if action.starts_with("route.") || action.starts_with("message.") {
        return crate::messaging::is_mutation(action);
    }
    if action.starts_with("job.") {
        return crate::jobs::is_mutation(action);
    }
    if action.starts_with("cache.") {
        return crate::cache::is_mutation(action);
    }
    if action.starts_with("vector.") || action.starts_with("search.") {
        return crate::search::is_mutation(action);
    }
    if action.starts_with("schedule.") {
        return crate::scheduler::is_mutation(action);
    }
    if action.starts_with("auth.") {
        return action == "auth.revoke";
    }
    if action.starts_with("consumer.") {
        return action == "consumer.ack";
    }
    documents::is_mutation(action)
}

fn canonical_content(cmd: &Command) -> Value {
    json!({"action": cmd.action, "payload": cmd.payload})
}

fn validate_command(cmd: &Command) -> Result<()> {
    validate_identifier("namespace", &cmd.namespace)?;
    validate_identifier("partition", &cmd.partition)?;
    validate_identifier("action", &cmd.action)?;
    if !cmd.payload.is_object() {
        return Err(Error::new("invalid_argument", "payload must be an object"));
    }
    if is_mutation(&cmd.action) {
        validate_identifier("request_id", &cmd.request_id)?;
    } else if cmd.request_id.len() > MAX_IDENTIFIER_BYTES {
        return Err(Error::new("invalid_argument", "request_id is too long"));
    }
    Ok(())
}

fn validate_identifier(name: &str, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(Error::new(
            "invalid_argument",
            format!("{name} must not be empty"),
        ));
    }
    if value.len() > MAX_IDENTIFIER_BYTES {
        return Err(Error::new(
            "invalid_argument",
            format!("{name} is too long"),
        ));
    }
    if value.chars().any(char::is_control) {
        return Err(Error::new(
            "invalid_argument",
            format!("{name} must not contain control characters"),
        ));
    }
    Ok(())
}

fn dispatch(kv: &mut dyn Kv, cmd: &Command) -> Result<Value> {
    if cmd.action == "transaction" {
        let commands = cmd.payload["commands"]
            .as_array()
            .filter(|v| !v.is_empty() && v.len() <= 1000)
            .ok_or_else(|| {
                Error::new(
                    "invalid_argument",
                    "commands must contain 1..1000 operations",
                )
            })?;
        let mut results = Vec::new();
        for (index, operation) in commands.iter().enumerate() {
            let action = operation["action"]
                .as_str()
                .ok_or_else(|| Error::new("invalid_argument", "action required"))?;
            if action == "transaction" || action.starts_with("scope.") {
                return Err(Error::new(
                    "invalid_argument",
                    "nested transaction and scope operations are not supported in a batch",
                ));
            }
            use sha2::{Digest, Sha256};
            let request_id = format!(
                "{:x}",
                Sha256::digest(format!("{}:{index}", cmd.request_id).as_bytes())
            );
            let child = Command {
                action: action.into(),
                payload: operation["payload"].clone(),
                request_id,
                ..cmd.clone()
            };
            validate_command(&child)?;
            results.push(dispatch(kv, &child)?);
        }
        return Ok(json!({"results":results}));
    }

    if cmd.action.starts_with("scope.") {
        return crate::operations::execute(kv, cmd);
    }
    if cmd.action.starts_with("route.") || cmd.action.starts_with("message.") {
        return crate::messaging::execute(kv, cmd);
    }
    if cmd.action == "document.commit" {
        let d = cmd
            .payload
            .get("document")
            .filter(|v| v.is_object())
            .ok_or_else(|| Error::new("invalid_argument", "document required"))?;
        let j = cmd
            .payload
            .get("job")
            .filter(|v| v.is_object())
            .ok_or_else(|| Error::new("invalid_argument", "job required"))?;
        let doc = documents::execute(
            kv,
            &Command {
                action: "document.put".into(),
                payload: d.clone(),
                ..cmd.clone()
            },
        )?;
        crate::search::index_document(
            kv,
            d["collection"].as_str().unwrap_or(""),
            d["id"].as_str().unwrap_or(""),
        )?;
        let job = crate::jobs::execute(
            kv,
            &Command {
                action: "job.enqueue".into(),
                payload: j.clone(),
                ..cmd.clone()
            },
        )?;
        return Ok(json!({"document":doc,"job":job}));
    }
    if cmd.action == "document.snapshot" {
        let collection = cmd.payload["collection"]
            .as_str()
            .ok_or_else(|| Error::new("invalid_argument", "collection required"))?;
        if let Some(id) = cmd.payload.get("id") {
            let document = documents::execute(
                kv,
                &Command {
                    action: "document.get".into(),
                    payload: json!({"collection":collection,"id":id}),
                    ..cmd.clone()
                },
            );
            let rows = match document {
                Ok(doc) => {
                    let matches = cmd.payload["filter"].as_object().is_none_or(|f| {
                        f.iter().all(|(path, expected)| {
                            let mut value = &doc["value"];
                            for part in path.split('.') {
                                value = &value[part];
                            }
                            value == expected
                        })
                    });
                    if matches {
                        vec![doc]
                    } else {
                        vec![]
                    }
                }
                Err(e) if e.code == "not_found" => vec![],
                Err(e) => return Err(e),
            };
            return Ok(
                json!({"documents":rows,"total":rows.len(),"sequence":kv.get("meta","event_sequence")?.unwrap_or(json!(0))}),
            );
        }
        let fields = cmd.payload["filter"]
            .as_object()
            .map(|f| f.keys().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        if fields.is_empty() || !documents::has_index(kv, collection, &fields)? {
            return Err(Error::new(
                "index_required",
                "live subscriptions require an equality filter covered by an index",
            ));
        }
        let mut result = documents::execute(
            kv,
            &Command {
                action: "document.query".into(),
                ..cmd.clone()
            },
        )?;
        result["sequence"] = kv.get("meta", "event_sequence")?.unwrap_or(json!(0));
        return Ok(result);
    }
    if cmd.action == "auth.revoke" || cmd.action == "auth.is_revoked" {
        let id = cmd.payload["id"]
            .as_str()
            .filter(|s| !s.is_empty() && s.len() <= 256)
            .ok_or_else(|| Error::new("invalid_argument", "id required"))?;
        if cmd.action == "auth.revoke" {
            kv.put("revocations", &key(&[id]), &json!(true))?;
            return Ok(json!({"revoked":true}));
        }
        return Ok(json!(kv.get("revocations", &key(&[id]))?.is_some()));
    }
    if cmd.action == "consumer.read" || cmd.action == "consumer.ack" {
        let name = cmd.payload["name"]
            .as_str()
            .filter(|s| !s.is_empty() && s.len() <= 256)
            .ok_or_else(|| Error::new("invalid_argument", "consumer name required"))?;
        let cursor = kv
            .get("consumers", &key(&[name]))?
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        if cmd.action == "consumer.ack" {
            let seq = cmd.payload["sequence"]
                .as_u64()
                .ok_or_else(|| Error::new("invalid_argument", "sequence required"))?;
            let latest = kv
                .get("meta", "event_sequence")?
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            if seq < cursor || seq > latest {
                return Err(Error::new("conflict", "invalid cursor advancement"));
            }
            kv.put("consumers", &key(&[name]), &json!(seq))?;
            return Ok(json!({"sequence":seq}));
        }
        return documents::execute(
            kv,
            &Command {
                action: "event.read".into(),
                payload: json!({"after":cursor,"limit":cmd.payload.get("limit").cloned().unwrap_or(json!(100))}),
                ..cmd.clone()
            },
        );
    }
    if cmd.action.starts_with("job.") {
        return crate::jobs::execute(kv, cmd);
    }
    if cmd.action.starts_with("cache.") {
        return crate::cache::execute(kv, cmd);
    }
    if cmd.action.starts_with("vector.") || cmd.action.starts_with("search.") {
        return crate::search::execute(kv, cmd);
    }
    if cmd.action.starts_with("schedule.") {
        return crate::scheduler::execute(kv, cmd);
    }
    if cmd.action.starts_with("document.")
        || cmd.action.starts_with("index.")
        || cmd.action.starts_with("event.")
    {
        let result = documents::execute(kv, cmd)?;
        if cmd.action == "document.put" || cmd.action == "document.delete" {
            crate::search::index_document(
                kv,
                cmd.payload["collection"].as_str().unwrap_or(""),
                cmd.payload["id"].as_str().unwrap_or(""),
            )?;
        }
        Ok(result)
    } else {
        Err(Error::new(
            "unsupported",
            format!("unsupported action: {}", cmd.action),
        ))
    }
}

struct RedbKv<'transaction> {
    transaction: &'transaction WriteTransaction,
    scope: String,
    byte_delta: i128,
    changed: bool,
}

struct ReadKv<'a> {
    transaction: &'a ReadTransaction,
    scope: String,
}
impl Kv for ReadKv<'_> {
    fn get(&self, bucket: &str, k: &str) -> Result<Option<Value>> {
        let table = self.transaction.open_table(DATA).map_err(database_error)?;
        let name = format!("{}{}{}", self.scope, key(&[bucket]), k);
        let value = table.get(name.as_str()).map_err(database_error)?;
        value
            .map(|v| serde_json::from_str(v.value()).map_err(json_storage_error))
            .transpose()
    }
    fn scan(&self, bucket: &str, prefix: &str) -> Result<Vec<(String, Value)>> {
        let table = self.transaction.open_table(DATA).map_err(database_error)?;
        let base = format!("{}{}", self.scope, key(&[bucket]));
        let start = format!("{base}{prefix}");
        let mut values = Vec::new();
        for row in table.range(start.as_str()..).map_err(database_error)? {
            let (k, v) = row.map_err(database_error)?;
            if !k.value().starts_with(&start) {
                break;
            }
            values.push((
                k.value()[base.len()..].to_string(),
                serde_json::from_str(v.value()).map_err(json_storage_error)?,
            ));
        }
        Ok(values)
    }
    fn scan_page(
        &self,
        bucket: &str,
        prefix: &str,
        after: &str,
        limit: usize,
    ) -> Result<Vec<(String, Value)>> {
        let table = self.transaction.open_table(DATA).map_err(database_error)?;
        scan_page_table(&table, &self.scope, bucket, prefix, after, limit)
    }
    fn put(&mut self, _: &str, _: &str, _: &Value) -> Result<()> {
        Err(Error::new(
            "internal",
            "mutation attempted in read transaction",
        ))
    }
    fn delete(&mut self, _: &str, _: &str) -> Result<()> {
        Err(Error::new(
            "internal",
            "mutation attempted in read transaction",
        ))
    }
}

impl<'transaction> RedbKv<'transaction> {
    fn new(transaction: &'transaction WriteTransaction, namespace: &str, partition: &str) -> Self {
        Self {
            transaction,
            scope: key(&[namespace, partition]),
            byte_delta: 0,
            changed: false,
        }
    }

    fn account(&mut self, delta: i128) -> Result<()> {
        self.byte_delta += delta;
        let current = self
            .get("accounting", "bytes")?
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let next = (i128::from(current) + delta)
            .max(0)
            .min(i128::from(u64::MAX)) as u64;
        self.put("accounting", "bytes", &json!(next))
    }
    fn storage_key(&self, bucket: &str, entry_key: &str) -> String {
        format!("{}{}{}", self.scope, key(&[bucket]), entry_key)
    }
}

impl Kv for RedbKv<'_> {
    fn get(&self, bucket: &str, entry_key: &str) -> Result<Option<Value>> {
        let table = self.transaction.open_table(DATA).map_err(database_error)?;
        let storage_key = self.storage_key(bucket, entry_key);
        let value = table.get(storage_key.as_str()).map_err(database_error)?;
        value
            .map(|value| serde_json::from_str(value.value()).map_err(json_storage_error))
            .transpose()
    }

    fn scan(&self, bucket: &str, prefix: &str) -> Result<Vec<(String, Value)>> {
        let table = self.transaction.open_table(DATA).map_err(database_error)?;
        let bucket_prefix = self.storage_key(bucket, "");
        let storage_prefix = format!("{bucket_prefix}{prefix}");
        let mut values = Vec::new();
        let range = table
            .range(storage_prefix.as_str()..)
            .map_err(database_error)?;
        for entry in range {
            let (entry_key, value) = entry.map_err(database_error)?;
            let entry_key = entry_key.value();
            if !entry_key.starts_with(&storage_prefix) {
                break;
            }
            let visible_key = entry_key
                .strip_prefix(&bucket_prefix)
                .ok_or_else(|| Error::new("storage_error", "invalid scoped storage key"))?;
            values.push((
                visible_key.to_owned(),
                serde_json::from_str(value.value()).map_err(json_storage_error)?,
            ));
        }
        Ok(values)
    }

    fn scan_page(
        &self,
        bucket: &str,
        prefix: &str,
        after: &str,
        limit: usize,
    ) -> Result<Vec<(String, Value)>> {
        let table = self.transaction.open_table(DATA).map_err(database_error)?;
        scan_page_table(&table, &self.scope, bucket, prefix, after, limit)
    }
    fn put(&mut self, bucket: &str, entry_key: &str, value: &Value) -> Result<()> {
        let storage_key = self.storage_key(bucket, entry_key);
        let encoded = serde_json::to_string(value).map_err(json_storage_error)?;
        let delta = {
            let mut table = self.transaction.open_table(DATA).map_err(database_error)?;
            let old = table
                .insert(storage_key.as_str(), encoded.as_str())
                .map_err(database_error)?;
            self.changed |= old.as_ref().is_none_or(|v| v.value() != encoded.as_str());
            encoded.len() as i128 + storage_key.len() as i128
                - old
                    .map(|v| v.value().len() + storage_key.len())
                    .unwrap_or(0) as i128
        };
        if bucket != "accounting" {
            self.account(delta)?;
        }
        Ok(())
    }
    fn delete(&mut self, bucket: &str, entry_key: &str) -> Result<()> {
        let storage_key = self.storage_key(bucket, entry_key);
        let delta = {
            let mut table = self.transaction.open_table(DATA).map_err(database_error)?;
            let old = table.remove(storage_key.as_str()).map_err(database_error)?;
            self.changed |= old.is_some();
            -(old
                .map(|v| v.value().len() + storage_key.len())
                .unwrap_or(0) as i128)
        };
        if bucket != "accounting" {
            self.account(delta)?;
        }
        Ok(())
    }
}

fn database_error(error: impl std::fmt::Display) -> Error {
    Error::new("storage_error", error.to_string())
}

fn json_storage_error(error: serde_json::Error) -> Error {
    Error::new("storage_error", error.to_string())
}

fn scan_page_table(
    table: &impl ReadableTable<&'static str, &'static str>,
    scope: &str,
    bucket: &str,
    prefix: &str,
    after: &str,
    limit: usize,
) -> Result<Vec<(String, Value)>> {
    let base = format!("{scope}{}", key(&[bucket]));
    let wanted = format!("{base}{prefix}");
    let start = format!("{base}{}", prefix.max(after));
    let mut output = Vec::new();
    for row in table.range(start.as_str()..).map_err(database_error)? {
        let (k, v) = row.map_err(database_error)?;
        let name = k.value();
        if !name.starts_with(&wanted) {
            break;
        }
        let visible = &name[base.len()..];
        if visible <= after {
            continue;
        }
        if output.len() >= limit {
            break;
        }
        output.push((
            visible.to_string(),
            serde_json::from_str(v.value()).map_err(json_storage_error)?,
        ));
    }
    Ok(output)
}
