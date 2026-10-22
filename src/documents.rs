use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::model::{Command, Error, Result};
use crate::store::{emit, key, Kv};

const MAX_NAME_BYTES: usize = 256;
const MAX_INDEX_FIELDS: usize = 16;
const MAX_FILTER_FIELDS: usize = 64;
const MAX_QUERY_LIMIT: u64 = 1_000;
const MAX_QUERY_OFFSET: u64 = 100_000;
const MAX_QUERY_SCAN: usize = 100_000;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredDocument {
    id: String,
    revision: u64,
    value: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct IndexDefinition {
    collection: String,
    name: String,
    fields: Vec<String>,
    unique: bool,
}

pub fn is_mutation(action: &str) -> bool {
    matches!(
        action,
        "document.put" | "document.delete" | "document.commit" | "index.create" | "event.trim"
    )
}

pub fn execute(kv: &mut dyn Kv, cmd: &Command) -> Result<Value> {
    match cmd.action.as_str() {
        "document.put" => put_document(kv, cmd),
        "document.get" => get_document(kv, cmd),
        "document.delete" => delete_document(kv, cmd),
        "document.query" => query_documents(kv, cmd),
        "document.commit" => Err(Error::new(
            "unsupported",
            "document.commit is not available until job support is wired",
        )),
        "index.create" => create_index(kv, cmd),
        "index.list" => list_indexes(kv, cmd),
        "event.read" => read_events(kv, cmd),
        "event.trim" => trim_events(kv, cmd),
        _ => Err(Error::new(
            "unsupported",
            format!("unsupported action: {}", cmd.action),
        )),
    }
}

pub fn has_index(kv: &dyn Kv, collection: &str, fields: &[String]) -> Result<bool> {
    validate_name("collection", collection)?;
    if fields.is_empty() || fields.len() > MAX_INDEX_FIELDS {
        return Err(invalid("fields must contain between 1 and 16 paths"));
    }
    for field in fields {
        validate_field_path(field)?;
    }
    Ok(load_indexes(kv, collection)?.iter().any(|definition| {
        definition.fields.len() == fields.len()
            && definition.fields.iter().all(|f| fields.contains(f))
    }))
}

fn put_document(kv: &mut dyn Kv, cmd: &Command) -> Result<Value> {
    let payload = object_payload(cmd, &["collection", "id", "value", "expected_revision"])?;
    let collection = required_string(payload, "collection")?;
    let id = required_string(payload, "id")?;
    validate_name("collection", collection)?;
    validate_name("id", id)?;
    let value = payload
        .get("value")
        .ok_or_else(|| invalid("value is required"))?;
    if !value.is_object() {
        return Err(invalid("value must be an object"));
    }
    let expected = optional_u64(payload, "expected_revision")?;
    let document_key = key(&[collection, id]);
    let old = kv
        .get("documents", &document_key)?
        .map(parse_document)
        .transpose()?;
    validate_put_revision(old.as_ref(), expected)?;
    let previous_revision = match kv.get("revisions", &document_key)? {
        Some(value) => value
            .as_u64()
            .ok_or_else(|| storage("invalid document revision metadata"))?,
        None => old.as_ref().map_or(0, |document| document.revision),
    };
    let revision = previous_revision
        .checked_add(1)
        .ok_or_else(|| Error::new("conflict", "document revision exhausted"))?;
    let document = StoredDocument {
        id: id.to_owned(),
        revision,
        value: value.clone(),
    };
    let indexes = load_indexes(kv, collection)?;
    validate_unique_postings(kv, &indexes, &document, id)?;
    if let Some(old) = &old {
        remove_postings(kv, &indexes, old)?;
    }
    add_postings(kv, &indexes, &document)?;
    let result = document_value(&document)?;
    kv.put("documents", &document_key, &result)?;
    kv.put("revisions", &document_key, &json!(revision))?;
    emit(kv, cmd, "document.put", &document_key, result.clone())?;
    Ok(result)
}

fn get_document(kv: &mut dyn Kv, cmd: &Command) -> Result<Value> {
    let payload = object_payload(cmd, &["collection", "id"])?;
    let collection = required_string(payload, "collection")?;
    let id = required_string(payload, "id")?;
    validate_name("collection", collection)?;
    validate_name("id", id)?;
    let document = kv
        .get("documents", &key(&[collection, id]))?
        .ok_or_else(|| Error::new("not_found", "document not found"))?;
    let parsed = parse_document(document)?;
    document_value(&parsed)
}

fn delete_document(kv: &mut dyn Kv, cmd: &Command) -> Result<Value> {
    let payload = object_payload(cmd, &["collection", "id", "expected_revision"])?;
    let collection = required_string(payload, "collection")?;
    let id = required_string(payload, "id")?;
    validate_name("collection", collection)?;
    validate_name("id", id)?;
    let expected = optional_u64(payload, "expected_revision")?;
    let document_key = key(&[collection, id]);
    let document = kv
        .get("documents", &document_key)?
        .ok_or_else(|| Error::new("not_found", "document not found"))
        .and_then(parse_document)?;
    if let Some(expected) = expected {
        if expected != document.revision {
            return Err(Error::new(
                "conflict",
                format!(
                    "expected revision {expected}, found revision {}",
                    document.revision
                ),
            ));
        }
    }
    let revision = document
        .revision
        .checked_add(1)
        .ok_or_else(|| Error::new("conflict", "document revision exhausted"))?;
    let indexes = load_indexes(kv, collection)?;
    remove_postings(kv, &indexes, &document)?;
    kv.delete("documents", &document_key)?;
    kv.put("revisions", &document_key, &json!(revision))?;
    let result = json!({"deleted": true, "revision": revision});
    emit(
        kv,
        cmd,
        "document.delete",
        &document_key,
        json!({"id": id, "revision": revision}),
    )?;
    Ok(result)
}

fn query_documents(kv: &mut dyn Kv, cmd: &Command) -> Result<Value> {
    let payload = object_payload(
        cmd,
        &[
            "collection",
            "filter",
            "sort",
            "limit",
            "offset",
            "projection",
        ],
    )?;
    let collection = required_string(payload, "collection")?;
    validate_name("collection", collection)?;
    let filter = parse_filter(payload.get("filter"))?;
    let sort = parse_sort(payload.get("sort"))?;
    let limit = optional_u64(payload, "limit")?.unwrap_or(100);
    if limit > MAX_QUERY_LIMIT {
        return Err(invalid("limit must not exceed 1000"));
    }
    let offset = optional_u64(payload, "offset")?.unwrap_or(0);
    if offset > MAX_QUERY_OFFSET {
        return Err(invalid("offset must not exceed 100000"));
    }
    let projection = parse_projection(payload.get("projection"))?;

    let indexes = load_indexes(kv, collection)?;
    let selected_index = indexes
        .iter()
        .filter(|definition| {
            definition
                .fields
                .iter()
                .all(|field| filter.contains_key(field))
        })
        .max_by_key(|definition| definition.fields.len());
    let mut documents = if let Some(index) = selected_index {
        query_index(kv, collection, index, &filter)?
    } else {
        scan_collection(kv, collection)?
    };
    documents.retain(|document| matches_filter(&document.value, &filter));
    if let Some((field, descending)) = &sort {
        documents.sort_by(|left, right| {
            let order = compare_optional_values(
                nested_value(&left.value, field),
                nested_value(&right.value, field),
            )
            .then_with(|| left.id.cmp(&right.id));
            if *descending {
                order.reverse()
            } else {
                order
            }
        });
    } else {
        documents.sort_by(|left, right| left.id.cmp(&right.id));
    }
    let total =
        u64::try_from(documents.len()).map_err(|_| storage("query result count overflow"))?;
    let selected = documents
        .into_iter()
        .skip(offset as usize)
        .take(limit as usize)
        .map(|document| projected_document(&document, projection.as_deref()))
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({"documents": selected, "total": total}))
}

fn create_index(kv: &mut dyn Kv, cmd: &Command) -> Result<Value> {
    let payload = object_payload(cmd, &["collection", "name", "fields", "unique"])?;
    let collection = required_string(payload, "collection")?;
    let name = required_string(payload, "name")?;
    validate_name("collection", collection)?;
    validate_name("name", name)?;
    let fields_value = payload
        .get("fields")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("fields must be an array"))?;
    if fields_value.is_empty() || fields_value.len() > MAX_INDEX_FIELDS {
        return Err(invalid("fields must contain between 1 and 16 paths"));
    }
    let mut fields = Vec::with_capacity(fields_value.len());
    let mut seen = BTreeSet::new();
    for field in fields_value {
        let field = field
            .as_str()
            .ok_or_else(|| invalid("index fields must be strings"))?;
        validate_field_path(field)?;
        if !seen.insert(field.to_owned()) {
            return Err(invalid("index fields must not contain duplicates"));
        }
        fields.push(field.to_owned());
    }
    let unique = optional_bool(payload, "unique")?.unwrap_or(false);
    let definition_key = key(&[collection, name]);
    if kv.get("indexes", &definition_key)?.is_some() {
        return Err(Error::new("conflict", "index already exists"));
    }
    let definition = IndexDefinition {
        collection: collection.to_owned(),
        name: name.to_owned(),
        fields,
        unique,
    };
    let documents = scan_collection(kv, collection)?;
    if unique {
        let mut values = BTreeMap::new();
        for document in &documents {
            let composite = composite_value(&document.value, &definition.fields)?;
            if let Some(existing) = values.insert(composite, document.id.clone()) {
                return Err(Error::new(
                    "conflict",
                    format!(
                        "unique index {} has duplicate values for documents {existing} and {}",
                        definition.name, document.id
                    ),
                ));
            }
        }
    }
    let encoded = serde_json::to_value(&definition).map_err(json_storage_error)?;
    kv.put("indexes", &definition_key, &encoded)?;
    for document in &documents {
        add_posting(kv, &definition, document)?;
    }
    Ok(encoded)
}

fn list_indexes(kv: &mut dyn Kv, cmd: &Command) -> Result<Value> {
    let payload = object_payload(cmd, &["collection"])?;
    let collection = required_string(payload, "collection")?;
    validate_name("collection", collection)?;
    let indexes = load_indexes(kv, collection)?
        .into_iter()
        .map(|definition| serde_json::to_value(definition).map_err(json_storage_error))
        .collect::<Result<Vec<_>>>()?;
    Ok(json!({"indexes": indexes}))
}

fn read_events(kv: &mut dyn Kv, cmd: &Command) -> Result<Value> {
    let payload = object_payload(cmd, &["after", "limit"])?;
    let after = optional_u64(payload, "after")?.unwrap_or(0);
    let limit = optional_u64(payload, "limit")?.unwrap_or(100);
    if limit == 0 || limit > MAX_QUERY_LIMIT {
        return Err(invalid("limit must be between 1 and 1000"));
    }
    let latest = metadata_u64(kv, "event_sequence")?.unwrap_or(0);
    let floor = metadata_u64(kv, "event_floor")?.unwrap_or(0);
    if after < floor {
        return Err(Error::new(
            "history_unavailable",
            format!("events through sequence {floor} are no longer retained"),
        ));
    }
    let mut events = Vec::new();
    for (_, event) in kv.scan_page("events", "", &format!("{after:020}"), limit as usize)? {
        validate_event(&event)?;
        let sequence = event["sequence"]
            .as_u64()
            .ok_or_else(|| storage("invalid retained event"))?;
        if sequence > after {
            events.push(event);
            if events.len() == limit as usize {
                break;
            }
        }
    }
    let oldest = floor
        .checked_add(1)
        .ok_or_else(|| storage("event retention floor exhausted"))?;
    Ok(json!({"events": events, "latest": latest, "oldest": oldest}))
}

fn trim_events(kv: &mut dyn Kv, cmd: &Command) -> Result<Value> {
    let payload = object_payload(cmd, &["through"])?;
    let through = required_u64(payload, "through")?;
    let latest = metadata_u64(kv, "event_sequence")?.unwrap_or(0);
    let old_floor = metadata_u64(kv, "event_floor")?.unwrap_or(0);
    let new_floor = old_floor.max(through.min(latest));
    let mut trimmed = 0_u64;
    for (event_key, event) in kv.scan("events", "")? {
        let sequence = event
            .get("sequence")
            .and_then(Value::as_u64)
            .ok_or_else(|| storage("invalid retained event"))?;
        if sequence <= new_floor {
            kv.delete("events", &event_key)?;
            trimmed = trimmed
                .checked_add(1)
                .ok_or_else(|| storage("trim count overflow"))?;
        }
    }
    kv.put("meta", "event_floor", &json!(new_floor))?;
    Ok(json!({"trimmed": trimmed, "through": new_floor}))
}

fn load_indexes(kv: &dyn Kv, collection: &str) -> Result<Vec<IndexDefinition>> {
    let mut indexes = Vec::new();
    for (_, value) in kv.scan("indexes", &key(&[collection]))? {
        let definition: IndexDefinition =
            serde_json::from_value(value).map_err(json_storage_error)?;
        if definition.collection != collection {
            return Err(storage("index stored under incorrect collection"));
        }
        indexes.push(definition);
    }
    indexes.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(indexes)
}

fn validate_unique_postings(
    kv: &dyn Kv,
    indexes: &[IndexDefinition],
    document: &StoredDocument,
    id: &str,
) -> Result<()> {
    for definition in indexes.iter().filter(|definition| definition.unique) {
        let prefix = posting_value_prefix(definition, &document.value)?;
        for (_, posting) in kv.scan("postings", &prefix)? {
            let existing_id = posting
                .as_str()
                .ok_or_else(|| storage("invalid index posting"))?;
            if existing_id != id {
                return Err(Error::new(
                    "conflict",
                    format!("unique index {} would be violated", definition.name),
                ));
            }
        }
    }
    Ok(())
}

fn add_postings(
    kv: &mut dyn Kv,
    indexes: &[IndexDefinition],
    document: &StoredDocument,
) -> Result<()> {
    for definition in indexes {
        add_posting(kv, definition, document)?;
    }
    Ok(())
}

fn add_posting(
    kv: &mut dyn Kv,
    definition: &IndexDefinition,
    document: &StoredDocument,
) -> Result<()> {
    let posting_key = posting_key(definition, &document.value, &document.id)?;
    kv.put("postings", &posting_key, &json!(document.id))
}

fn remove_postings(
    kv: &mut dyn Kv,
    indexes: &[IndexDefinition],
    document: &StoredDocument,
) -> Result<()> {
    for definition in indexes {
        let posting_key = posting_key(definition, &document.value, &document.id)?;
        kv.delete("postings", &posting_key)?;
    }
    Ok(())
}

fn posting_key(definition: &IndexDefinition, value: &Value, id: &str) -> Result<String> {
    let composite = composite_value(value, &definition.fields)?;
    Ok(key(&[
        &definition.collection,
        &definition.name,
        &composite,
        id,
    ]))
}

fn posting_value_prefix(definition: &IndexDefinition, value: &Value) -> Result<String> {
    let composite = composite_value(value, &definition.fields)?;
    Ok(key(&[&definition.collection, &definition.name, &composite]))
}

fn composite_value(value: &Value, fields: &[String]) -> Result<String> {
    let values: Vec<Value> = fields
        .iter()
        .map(|field| match nested_value(value, field) {
            Some(value) => json!({"present": true, "value": value}),
            None => json!({"present": false}),
        })
        .collect();
    serde_json::to_string(&values).map_err(json_storage_error)
}

fn query_index(
    kv: &dyn Kv,
    collection: &str,
    definition: &IndexDefinition,
    filter: &BTreeMap<String, Value>,
) -> Result<Vec<StoredDocument>> {
    let values: Vec<Value> = definition
        .fields
        .iter()
        .map(|field| json!({"present": true, "value": filter[field]}))
        .collect();
    let composite = serde_json::to_string(&values).map_err(json_storage_error)?;
    let prefix = key(&[collection, &definition.name, &composite]);
    let postings = kv.scan("postings", &prefix)?;
    if postings.len() > MAX_QUERY_SCAN {
        return Err(Error::new(
            "query_too_broad",
            "indexed query exceeds the scan bound",
        ));
    }
    let mut documents = Vec::with_capacity(postings.len());
    for (_, posting) in postings {
        let id = posting
            .as_str()
            .ok_or_else(|| storage("invalid index posting"))?;
        if let Some(value) = kv.get("documents", &key(&[collection, id]))? {
            documents.push(parse_document(value)?);
        }
    }
    Ok(documents)
}

fn scan_collection(kv: &dyn Kv, collection: &str) -> Result<Vec<StoredDocument>> {
    let entries = kv.scan("documents", &key(&[collection]))?;
    if entries.len() > MAX_QUERY_SCAN {
        return Err(Error::new(
            "query_too_broad",
            "document query exceeds the scan bound",
        ));
    }
    entries
        .into_iter()
        .map(|(_, value)| parse_document(value))
        .collect()
}

fn matches_filter(value: &Value, filter: &BTreeMap<String, Value>) -> bool {
    filter
        .iter()
        .all(|(field, expected)| nested_value(value, field) == Some(expected))
}

fn nested_value<'a>(value: &'a Value, field: &str) -> Option<&'a Value> {
    let mut current = value;
    for part in field.split('.') {
        current = current.as_object()?.get(part)?;
    }
    Some(current)
}

fn projected_document(document: &StoredDocument, projection: Option<&[String]>) -> Result<Value> {
    let value = if let Some(projection) = projection {
        let mut projected = Map::new();
        for field in projection {
            if let Some(value) = nested_value(&document.value, field) {
                insert_projected(&mut projected, field, value.clone())?;
            }
        }
        Value::Object(projected)
    } else {
        document.value.clone()
    };
    Ok(json!({"id": document.id, "revision": document.revision, "value": value}))
}

fn insert_projected(target: &mut Map<String, Value>, field: &str, value: Value) -> Result<()> {
    let parts: Vec<&str> = field.split('.').collect();
    let mut current = target;
    for part in &parts[..parts.len() - 1] {
        let entry = current
            .entry((*part).to_owned())
            .or_insert_with(|| Value::Object(Map::new()));
        current = entry
            .as_object_mut()
            .ok_or_else(|| invalid("projection contains overlapping paths"))?;
    }
    current.insert(parts[parts.len() - 1].to_owned(), value);
    Ok(())
}

fn parse_filter(value: Option<&Value>) -> Result<BTreeMap<String, Value>> {
    let Some(value) = value else {
        return Ok(BTreeMap::new());
    };
    let object = value
        .as_object()
        .ok_or_else(|| invalid("filter must be an object"))?;
    if object.len() > MAX_FILTER_FIELDS {
        return Err(invalid("filter has too many fields"));
    }
    let mut filter = BTreeMap::new();
    for (field, value) in object {
        validate_field_path(field)?;
        filter.insert(field.clone(), value.clone());
    }
    Ok(filter)
}

fn parse_sort(value: Option<&Value>) -> Result<Option<(String, bool)>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let object = value
        .as_object()
        .ok_or_else(|| invalid("sort must be an object"))?;
    ensure_known_keys(object, &["field", "direction"])?;
    let field = required_string(object, "field")?;
    validate_field_path(field)?;
    let descending = match required_string(object, "direction")? {
        "asc" => false,
        "desc" => true,
        _ => return Err(invalid("sort direction must be asc or desc")),
    };
    Ok(Some((field.to_owned(), descending)))
}

fn parse_projection(value: Option<&Value>) -> Result<Option<Vec<String>>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let fields = value
        .as_array()
        .ok_or_else(|| invalid("projection must be an array"))?;
    if fields.len() > MAX_FILTER_FIELDS {
        return Err(invalid("projection has too many fields"));
    }
    let mut projection = Vec::with_capacity(fields.len());
    let mut seen = BTreeSet::new();
    for field in fields {
        let field = field
            .as_str()
            .ok_or_else(|| invalid("projection fields must be strings"))?;
        validate_field_path(field)?;
        if !seen.insert(field.to_owned()) {
            return Err(invalid("projection fields must not contain duplicates"));
        }
        projection.push(field.to_owned());
    }
    for left in &projection {
        for right in &projection {
            if left != right && right.starts_with(&format!("{left}.")) {
                return Err(invalid("projection contains overlapping paths"));
            }
        }
    }
    Ok(Some(projection))
}

fn compare_optional_values(left: Option<&Value>, right: Option<&Value>) -> Ordering {
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left), Some(right)) => compare_values(left, right),
    }
}

fn compare_values(left: &Value, right: &Value) -> Ordering {
    let left_rank = value_rank(left);
    let right_rank = value_rank(right);
    left_rank
        .cmp(&right_rank)
        .then_with(|| match (left, right) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
            (Value::Number(left), Value::Number(right)) => left
                .as_f64()
                .partial_cmp(&right.as_f64())
                .unwrap_or(Ordering::Equal),
            (Value::String(left), Value::String(right)) => left.cmp(right),
            _ => left.to_string().cmp(&right.to_string()),
        })
}

fn value_rank(value: &Value) -> u8 {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}

fn validate_put_revision(document: Option<&StoredDocument>, expected: Option<u64>) -> Result<()> {
    let Some(expected) = expected else {
        return Ok(());
    };
    if expected == 0 {
        if document.is_some() {
            return Err(Error::new("conflict", "document already exists"));
        }
        return Ok(());
    }
    match document {
        Some(document) if document.revision == expected => Ok(()),
        Some(document) => Err(Error::new(
            "conflict",
            format!(
                "expected revision {expected}, found revision {}",
                document.revision
            ),
        )),
        None => Err(Error::new("conflict", "document does not exist")),
    }
}

fn document_value(document: &StoredDocument) -> Result<Value> {
    serde_json::to_value(document).map_err(json_storage_error)
}

fn parse_document(value: Value) -> Result<StoredDocument> {
    let document: StoredDocument = serde_json::from_value(value).map_err(json_storage_error)?;
    if !document.value.is_object() {
        return Err(storage("stored document value is not an object"));
    }
    Ok(document)
}

fn validate_event(value: &Value) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| storage("invalid retained event"))?;
    if object.get("sequence").and_then(Value::as_u64).is_none()
        || object.get("timestamp_ms").and_then(Value::as_u64).is_none()
        || object.get("kind").and_then(Value::as_str).is_none()
        || object.get("resource").and_then(Value::as_str).is_none()
        || !object.contains_key("data")
    {
        return Err(storage("invalid retained event"));
    }
    Ok(())
}

fn metadata_u64(kv: &dyn Kv, name: &str) -> Result<Option<u64>> {
    kv.get("meta", name)?
        .map(|value| {
            value
                .as_u64()
                .ok_or_else(|| storage(format!("invalid {name} metadata")))
        })
        .transpose()
}

fn object_payload<'a>(cmd: &'a Command, keys: &[&str]) -> Result<&'a Map<String, Value>> {
    let object = cmd
        .payload
        .as_object()
        .ok_or_else(|| invalid("payload must be an object"))?;
    ensure_known_keys(object, keys)?;
    Ok(object)
}

fn ensure_known_keys(object: &Map<String, Value>, keys: &[&str]) -> Result<()> {
    if let Some(key) = object.keys().find(|key| !keys.contains(&key.as_str())) {
        return Err(invalid(format!("unknown field: {key}")));
    }
    Ok(())
}

fn required_string<'a>(object: &'a Map<String, Value>, name: &str) -> Result<&'a str> {
    object
        .get(name)
        .and_then(Value::as_str)
        .ok_or_else(|| invalid(format!("{name} must be a string")))
}

fn required_u64(object: &Map<String, Value>, name: &str) -> Result<u64> {
    object
        .get(name)
        .and_then(Value::as_u64)
        .ok_or_else(|| invalid(format!("{name} must be an unsigned integer")))
}

fn optional_u64(object: &Map<String, Value>, name: &str) -> Result<Option<u64>> {
    object
        .get(name)
        .map(|value| {
            value
                .as_u64()
                .ok_or_else(|| invalid(format!("{name} must be an unsigned integer")))
        })
        .transpose()
}

fn optional_bool(object: &Map<String, Value>, name: &str) -> Result<Option<bool>> {
    object
        .get(name)
        .map(|value| {
            value
                .as_bool()
                .ok_or_else(|| invalid(format!("{name} must be a boolean")))
        })
        .transpose()
}

fn validate_name(name: &str, value: &str) -> Result<()> {
    if value.is_empty() || value.len() > MAX_NAME_BYTES || value.chars().any(char::is_control) {
        return Err(invalid(format!(
            "{name} must be a nonempty bounded string without control characters"
        )));
    }
    Ok(())
}

fn validate_field_path(field: &str) -> Result<()> {
    validate_name("field", field)?;
    if field.split('.').any(str::is_empty) {
        return Err(invalid("field paths must not contain empty components"));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new("invalid_argument", message)
}

fn storage(message: impl Into<String>) -> Error {
    Error::new("storage_error", message)
}

fn json_storage_error(error: serde_json::Error) -> Error {
    storage(error.to_string())
}
