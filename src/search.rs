use crate::model::{Command, Error, Result};
use crate::store::{key, Kv};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};

fn bad(s: &str) -> Error {
    Error::new("invalid_argument", s)
}
fn text<'a>(p: &'a Value, k: &str) -> Result<&'a str> {
    p[k].as_str()
        .filter(|s| !s.is_empty() && s.len() <= 512)
        .ok_or_else(|| bad(k))
}
fn field<'a>(v: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.').try_fold(v, |v, k| v.get(k))
}
fn matches(doc: &Value, filter: Option<&Value>) -> bool {
    filter
        .and_then(Value::as_object)
        .is_none_or(|f| f.iter().all(|(k, v)| field(&doc["value"], k) == Some(v)))
}
fn numeric(v: &Value) -> Result<Vec<f64>> {
    v.as_array()
        .ok_or_else(|| bad("vector must be an array"))?
        .iter()
        .map(|n| {
            n.as_f64()
                .filter(|x| {
                    x.is_finite() && (*x as f32).is_finite() && (*x == 0.0 || (*x as f32) != 0.0)
                })
                .ok_or_else(|| {
                    bad("vector values must fit finite f32 range without nonzero underflow")
                })
        })
        .collect()
}
fn vector(v: &Value, c: &Value) -> Result<Value> {
    let dim = c["dimensions"].as_u64().unwrap_or(0) as usize;
    if c["kind"] == "sparse" {
        let ix = v["indices"]
            .as_array()
            .ok_or_else(|| bad("sparse indices required"))?;
        let vals = numeric(&v["values"])?;
        if ix.len() != vals.len() || ix.len() > dim {
            return Err(bad("sparse vector length mismatch"));
        }
        let mut seen = BTreeSet::new();
        for i in ix {
            let n = i
                .as_u64()
                .filter(|n| (*n as usize) < dim)
                .ok_or_else(|| bad("sparse index out of range"))?;
            if !seen.insert(n) {
                return Err(bad("duplicate sparse index"));
            }
        }
        Ok(v.clone())
    } else {
        let nums = numeric(v)?;
        if nums.len() != dim {
            return Err(bad("vector dimension mismatch"));
        }
        if c["metric"] == "cosine" && nums.iter().all(|x| *x == 0.0) {
            return Err(bad("cosine vector must have nonzero magnitude"));
        }
        Ok(v.clone())
    }
}
fn score(a: &Value, b: &Value, c: &Value) -> f64 {
    if c["kind"] == "sparse" {
        let mut m = BTreeMap::new();
        if let (Some(ix), Some(vals)) = (a["indices"].as_array(), a["values"].as_array()) {
            for (i, v) in ix.iter().zip(vals) {
                m.insert(i.as_u64().unwrap_or(0), v.as_f64().unwrap_or(0.0));
            }
        }
        return b["indices"]
            .as_array()
            .zip(b["values"].as_array())
            .map(|(ix, vals)| {
                ix.iter()
                    .zip(vals)
                    .map(|(i, v)| {
                        m.get(&i.as_u64().unwrap_or(0)).copied().unwrap_or(0.0)
                            * v.as_f64().unwrap_or(0.0)
                    })
                    .sum()
            })
            .unwrap_or(0.0);
    }
    let aa = a.as_array().map(Vec::as_slice).unwrap_or(&[]);
    let bb = b.as_array().map(Vec::as_slice).unwrap_or(&[]);
    let (mut dot, mut na, mut nb, mut dist) = (0.0, 0.0, 0.0, 0.0);
    for (a, b) in aa.iter().zip(bb) {
        let x = a.as_f64().unwrap_or(0.0);
        let y = b.as_f64().unwrap_or(0.0);
        dot += x * y;
        na += x * x;
        nb += y * y;
        dist += (x - y) * (x - y);
    }
    let result = match c["metric"].as_str() {
        Some("cosine") => {
            if na > 0.0 && nb > 0.0 {
                dot / (na.sqrt() * nb.sqrt())
            } else {
                0.0
            }
        }
        Some("euclidean") => -dist.sqrt(),
        _ => dot,
    };
    if result.is_finite() {
        result
    } else {
        f64::MIN
    }
}
fn tokens(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::String(s) => out.extend(
            s.split(|c: char| !c.is_alphanumeric())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_lowercase()),
        ),
        Value::Array(a) => {
            for x in a {
                tokens(x, out)
            }
        }
        Value::Object(m) => {
            for x in m.values() {
                tokens(x, out)
            }
        }
        _ => {}
    }
}
fn config(kv: &dyn Kv, collection: &str, name: &str) -> Result<Value> {
    kv.get("vector_config", &key(&[collection, name]))?
        .ok_or_else(|| Error::new("not_found", "vector field is not configured"))
}
fn validated_upsert(
    kv: &dyn Kv,
    collection: &str,
    name: &str,
    item: &Value,
    c: &Value,
) -> Result<Value> {
    let id = text(item, "id")?;
    let v = vector(&item["vector"], c)?;
    let doc = kv
        .get("documents", &key(&[collection, id]))?
        .ok_or_else(|| Error::new("not_found", "source document does not exist"))?;
    let revision = item["source_revision"]
        .as_u64()
        .filter(|r| *r > 0)
        .ok_or_else(|| bad("source_revision required"))?;
    if doc["revision"] != revision {
        return Err(Error::new("conflict", "stale source revision"));
    }
    Ok(json!({"collection":collection,"id":id,"name":name,"vector":v,"source_revision":revision}))
}
fn submit_upserts(kv: &mut dyn Kv, collection: &str, payloads: Vec<Value>) -> Result<Vec<Value>> {
    let mut meta = kv
        .get("search_meta", &key(&[collection]))?
        .unwrap_or_else(|| json!({"sequence":0,"submitted":0}));
    let submitted = meta["submitted"].as_u64().unwrap_or(0);
    let end = submitted
        .checked_add(payloads.len() as u64)
        .ok_or_else(|| bad("sequence overflow"))?;
    meta["submitted"] = json!(end);
    if !meta["sequence"].is_u64() {
        meta["sequence"] = json!(0);
    }
    kv.put("search_meta", &key(&[collection]), &meta)?;
    let mut results = Vec::with_capacity(payloads.len());
    for (payload, seq) in payloads.into_iter().zip((submitted + 1)..=end) {
        kv.put(
            "pending_vectors",
            &format!("{}{:020}", key(&[collection]), seq),
            &json!({"payload":payload,"sequence":seq}),
        )?;
        results.push(json!({"id":payload["id"],"name":payload["name"],"source_revision":payload["source_revision"],"sequence":seq,"searchable":false}));
    }
    Ok(results)
}
fn advance_visibility(kv: &mut dyn Kv, collection: &str) -> Result<u64> {
    let mut meta = kv
        .get("search_meta", &key(&[collection]))?
        .unwrap_or_else(|| json!({"sequence":0,"submitted":0}));
    let submitted = meta["submitted"].as_u64().unwrap_or(0);
    let mut sequence = meta["sequence"].as_u64().unwrap_or(0);
    while sequence < submitted
        && kv
            .get(
                "index_status",
                &key(&[collection, &(sequence + 1).to_string()]),
            )?
            .is_some()
    {
        sequence += 1;
    }
    meta["sequence"] = json!(sequence);
    kv.put("search_meta", &key(&[collection]), &meta)?;
    Ok(sequence)
}
fn rejection(error: &Error) -> Value {
    json!({"status":"rejected","error":error})
}
fn remove_backlinks(
    kv: &mut dyn Kv,
    collection: &str,
    name: &str,
    id: &str,
) -> Result<Vec<String>> {
    let prefix = key(&[collection, name]);
    let mut adjacent = BTreeSet::new();
    if let Some(old) = kv.get("vectors", &key(&[collection, name, id]))? {
        if let Some(neighbors) = old["neighbors"].as_array() {
            adjacent.extend(
                neighbors
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned),
            );
        }
    }
    for (k, mut record) in kv.scan("vectors", &prefix)? {
        let Some(other) = record["id"].as_str().map(str::to_owned) else {
            continue;
        };
        if other == id {
            continue;
        }
        let links = record["neighbors"].as_array().cloned().unwrap_or_default();
        let filtered = links
            .iter()
            .filter(|v| v.as_str() != Some(id))
            .cloned()
            .collect::<Vec<_>>();
        if filtered.len() != links.len() {
            adjacent.insert(other);
            record["neighbors"] = json!(filtered);
            kv.put("vectors", &k, &record)?;
        }
    }
    adjacent.remove(id);
    let mut live = Vec::new();
    for other in adjacent {
        if kv
            .get("vectors", &key(&[collection, name, &other]))?
            .is_some()
        {
            live.push(other);
        }
    }
    Ok(live)
}
fn add_link(kv: &mut dyn Kv, collection: &str, name: &str, from: &str, to: &str) -> Result<()> {
    if from == to {
        return Ok(());
    }
    let k = key(&[collection, name, from]);
    let Some(mut record) = kv.get("vectors", &k)? else {
        return Ok(());
    };
    let mut links = record["neighbors"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|v| v.as_str().map(str::to_owned))
        .filter(|v| v != from && v != to)
        .collect::<Vec<_>>();
    links.push(to.to_owned());
    links.sort();
    links.dedup();
    while links.len() > 32 {
        let remove = links.iter().position(|v| v != to).unwrap_or(0);
        links.remove(remove);
    }
    record["neighbors"] = json!(links);
    kv.put("vectors", &k, &record)
}
fn reconnect(kv: &mut dyn Kv, collection: &str, name: &str, adjacent: &[String]) -> Result<()> {
    for pair in adjacent.windows(2) {
        add_link(kv, collection, name, &pair[0], &pair[1])?;
        add_link(kv, collection, name, &pair[1], &pair[0])?;
    }
    Ok(())
}
fn graph_candidates(
    kv: &dyn Kv,
    collection: &str,
    name: &str,
    q: &Value,
    c: &Value,
    ef: usize,
) -> Result<Vec<(String, f64)>> {
    let configured_entry = kv
        .get("vector_meta", &key(&[collection, name]))?
        .and_then(|m| m["entry"].as_str().map(str::to_owned));
    let entry = match configured_entry {
        Some(id) if kv.get("vectors", &key(&[collection, name, &id]))?.is_some() => Some(id),
        _ => kv
            .scan("vectors", &key(&[collection, name]))?
            .into_iter()
            .find_map(|(_, v)| v["id"].as_str().map(str::to_owned)),
    };
    let Some(entry) = entry else {
        return Ok(vec![]);
    };
    let Some(record) = kv.get("vectors", &key(&[collection, name, &entry]))? else {
        return Ok(vec![]);
    };
    let mut frontier = BTreeMap::from([(entry, score(q, &record["vector"], c))]);
    let mut visited = BTreeSet::new();
    let mut found = Vec::new();
    while !frontier.is_empty() && visited.len() < ef {
        let (id, s) = frontier
            .iter()
            .max_by(|a, b| a.1.total_cmp(b.1).then_with(|| b.0.cmp(a.0)))
            .map(|(id, s)| (id.clone(), *s))
            .unwrap();
        frontier.remove(&id);
        if !visited.insert(id.clone()) {
            continue;
        }
        found.push((id.clone(), s));
        if let Some(v) = kv.get("vectors", &key(&[collection, name, &id]))? {
            if let Some(n) = v["neighbors"].as_array() {
                for neighbor in n.iter().filter_map(Value::as_str) {
                    if !visited.contains(neighbor) && !frontier.contains_key(neighbor) {
                        if let Some(next) =
                            kv.get("vectors", &key(&[collection, name, neighbor]))?
                        {
                            frontier.insert(neighbor.into(), score(q, &next["vector"], c));
                        }
                    }
                }
            }
        }
    }
    found.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    Ok(found)
}
pub fn is_mutation(a: &str) -> bool {
    !matches!(
        a,
        "search.query" | "vector.status" | "search.wait" | "vector.describe"
    )
}
pub fn execute(kv: &mut dyn Kv, cmd: &Command) -> Result<Value> {
    let p = &cmd.payload;
    if cmd.action == "vector.flush" {
        let mut applied = 0;
        for (k, task) in kv.scan_page("pending_vectors", "", "", 100)? {
            let collection = task["payload"]["collection"]
                .as_str()
                .ok_or_else(|| bad("invalid pending collection"))?;
            let seq = task["sequence"]
                .as_u64()
                .ok_or_else(|| bad("invalid pending sequence"))?;
            let inner = Command {
                action: "vector.apply".into(),
                payload: task["payload"].clone(),
                ..cmd.clone()
            };
            let result = execute(kv, &inner);
            match result {
                Ok(_) => {
                    kv.put(
                        "index_status",
                        &key(&[collection, &seq.to_string()]),
                        &json!({"status":"indexed"}),
                    )?;
                    applied += 1;
                }
                Err(e) if e.code == "conflict" || e.code == "not_found" => {
                    kv.put(
                        "index_status",
                        &key(&[collection, &seq.to_string()]),
                        &rejection(&e),
                    )?;
                }
                Err(e) => return Err(e),
            }
            kv.delete("pending_vectors", &k)?;
            advance_visibility(kv, collection)?;
        }
        return Ok(json!({"applied":applied}));
    }
    let collection = text(p, "collection")?;
    match cmd.action.as_str() {
        "vector.describe" => config(kv, collection, text(p, "name")?),
        "vector.configure" => {
            let name = text(p, "name")?;
            let dimensions = p["dimensions"]
                .as_u64()
                .filter(|n| *n > 0 && *n <= 65536)
                .ok_or_else(|| bad("dimensions must be 1..65536"))?;
            let metric = p.get("metric").and_then(Value::as_str).unwrap_or("cosine");
            let kind = p.get("kind").and_then(Value::as_str).unwrap_or("dense");
            if !matches!(metric, "cosine" | "dot" | "euclidean")
                || !matches!(kind, "dense" | "sparse")
                || kind == "sparse" && metric != "dot"
            {
                return Err(bad("unsupported metric/kind (sparse requires dot)"));
            }
            let c = json!({"name":name,"dimensions":dimensions,"metric":metric,"kind":kind});
            if let Some(existing) = kv.get("vector_config", &key(&[collection, name]))? {
                if existing != c {
                    return Err(Error::new(
                        "conflict",
                        "vector configuration is immutable; use a new named field",
                    ));
                }
                return Ok(existing);
            }
            kv.put("vector_config", &key(&[collection, name]), &c)?;
            Ok(c)
        }
        "vector.upsert" => {
            let name = text(p, "name")?;
            let c = config(kv, collection, name)?;
            let payload = validated_upsert(kv, collection, name, p, &c)?;
            Ok(submit_upserts(kv, collection, vec![payload])?.remove(0))
        }
        "vector.upsert_batch" => {
            let name = text(p, "name")?;
            let c = config(kv, collection, name)?;
            let items = p["items"]
                .as_array()
                .filter(|items| !items.is_empty() && items.len() <= 1000)
                .ok_or_else(|| bad("items must contain 1..1000 vectors"))?;
            let mut payloads = Vec::with_capacity(items.len());
            for item in items {
                if !item.is_object() {
                    return Err(bad("batch items must be objects"));
                }
                payloads.push(validated_upsert(kv, collection, name, item, &c)?);
            }
            Ok(json!({"items":submit_upserts(kv,collection,payloads)?}))
        }
        "vector.apply" => {
            let id = text(p, "id")?;
            let name = text(p, "name")?;
            let c = config(kv, collection, name)?;
            let v = vector(&p["vector"], &c)?;
            let doc = kv
                .get("documents", &key(&[collection, id]))?
                .ok_or_else(|| Error::new("not_found", "source document does not exist"))?;
            let revision = p["source_revision"]
                .as_u64()
                .filter(|r| *r > 0)
                .ok_or_else(|| bad("source_revision required"))?;
            if doc["revision"] != revision {
                return Err(Error::new("conflict", "stale source revision"));
            }
            let neighbors = graph_candidates(kv, collection, name, &v, &c, 128)?
                .into_iter()
                .filter(|(n, _)| n != id)
                .take(16)
                .map(|x| x.0)
                .collect::<Vec<_>>();
            if kv.get("vectors", &key(&[collection, name, id]))?.is_some() {
                remove_backlinks(kv, collection, name, id)?;
            }
            kv.put(
                "vectors",
                &key(&[collection, name, id]),
                &json!({"id":id,"vector":v,"revision":revision,"neighbors":neighbors}),
            )?;
            for neighbor in &neighbors {
                add_link(kv, collection, name, neighbor, id)?;
            }
            let mut meta = kv
                .get("vector_meta", &key(&[collection, name]))?
                .unwrap_or_else(|| json!({"entry":id}));
            meta["entry"] = json!(id);
            kv.put("vector_meta", &key(&[collection, name]), &meta)?;
            Ok(json!({"id":id,"name":name,"source_revision":revision,"searchable":true}))
        }
        "vector.delete" => {
            let id = text(p, "id")?;
            let name = text(p, "name")?;
            let adjacent = remove_backlinks(kv, collection, name, id)?;
            kv.delete("vectors", &key(&[collection, name, id]))?;
            reconnect(kv, collection, name, &adjacent)?;
            let canceled = Error::new(
                "conflict",
                "vector upsert was canceled by delete before indexing",
            );
            let mut rejected = 0;
            for (k, task) in kv.scan("pending_vectors", &key(&[collection]))? {
                if task["payload"]["name"] == name && task["payload"]["id"] == id {
                    let seq = task["sequence"]
                        .as_u64()
                        .ok_or_else(|| bad("invalid pending sequence"))?;
                    kv.put(
                        "index_status",
                        &key(&[collection, &seq.to_string()]),
                        &rejection(&canceled),
                    )?;
                    kv.delete("pending_vectors", &k)?;
                    rejected += 1;
                }
            }
            advance_visibility(kv, collection)?;
            if kv
                .get("vector_meta", &key(&[collection, name]))?
                .is_some_and(|m| m["entry"] == id)
            {
                let entry = match adjacent.first() {
                    Some(id) => Some(id.clone()),
                    None => kv
                        .scan("vectors", &key(&[collection, name]))?
                        .into_iter()
                        .find_map(|(_, v)| v["id"].as_str().map(str::to_owned)),
                };
                kv.put(
                    "vector_meta",
                    &key(&[collection, name]),
                    &json!({"entry":entry}),
                )?;
            }
            Ok(json!({"deleted":true,"rejected_pending":rejected}))
        }
        "vector.status" | "search.wait" => {
            let meta = kv
                .get("search_meta", &key(&[collection]))?
                .unwrap_or_else(|| json!({"sequence":0,"submitted":0}));
            let seq = meta["sequence"].as_u64().unwrap_or(0);
            let wanted = p
                .get("sequence")
                .map(|v| v.as_u64().ok_or_else(|| bad("sequence must be integer")))
                .transpose()?
                .unwrap_or(0);
            if wanted > seq {
                return Err(Error::new(
                    "not_ready",
                    "index has not reached requested sequence",
                ));
            }
            if wanted > 0 {
                if let Some(status) =
                    kv.get("index_status", &key(&[collection, &wanted.to_string()]))?
                {
                    if status["status"] == "rejected" {
                        let error = serde_json::from_value(status["error"].clone())
                            .unwrap_or_else(|_| Error::new("conflict", "vector indexing rejected"));
                        return Err(error);
                    }
                }
            }
            Ok(json!({"sequence":seq,"submitted":meta["submitted"],"searchable":true}))
        }
        "search.query" => {
            let limit = p
                .get("limit")
                .map(|v| v.as_u64().ok_or_else(|| bad("limit")))
                .transpose()?
                .unwrap_or(10);
            if limit == 0 || limit > 1000 {
                return Err(bad("limit must be 1..1000"));
            }
            if p.get("filter").is_some_and(|f| !f.is_object()) {
                return Err(bad("filter must be object"));
            }
            let text_query = p
                .get("text")
                .map(|v| v.as_str().ok_or_else(|| bad("text must be string")))
                .transpose()?;
            if text_query.is_none() && p.get("vector").is_none() {
                return Err(bad("text or vector required"));
            }
            let exact = p
                .get("exact")
                .map(|v| v.as_bool().ok_or_else(|| bad("exact must be boolean")))
                .transpose()?
                .unwrap_or(true);
            let ef = p
                .get("ef")
                .map(|v| v.as_u64().ok_or_else(|| bad("ef")))
                .transpose()?
                .unwrap_or(128.max(limit));
            if ef < limit || ef > 100000 {
                return Err(bad("ef must be at least limit and at most 100000"));
            }
            let mut dense = Vec::new();
            let mut docs = BTreeMap::new();
            if let Some(q) = p.get("vector") {
                let name = text(p, "name")?;
                let c = config(kv, collection, name)?;
                let q = vector(q, &c)?;
                let candidates = if exact || p.get("filter").is_some() {
                    kv.scan("vectors", &key(&[collection, name]))?
                        .into_iter()
                        .map(|(_, v)| {
                            (
                                v["id"].as_str().unwrap_or("").to_string(),
                                score(&q, &v["vector"], &c),
                            )
                        })
                        .collect()
                } else {
                    graph_candidates(kv, collection, name, &q, &c, ef as usize)?
                };
                for (id, s) in candidates {
                    if let Some(doc) = kv.get("documents", &key(&[collection, &id]))? {
                        if let Some(v) = kv.get("vectors", &key(&[collection, name, &id]))? {
                            if doc["revision"] == v["revision"] && matches(&doc, p.get("filter")) {
                                docs.insert(id.clone(), doc);
                                dense.push((id, s));
                            }
                        }
                    }
                }
                dense.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
            }
            let mut lexical = Vec::new();
            if let Some(query) = text_query {
                let mut terms = Vec::new();
                tokens(&json!(query), &mut terms);
                terms.sort();
                terms.dedup();
                let stats = kv
                    .get("lexical_stats", &key(&[collection]))?
                    .unwrap_or_else(|| json!({"count":0,"length":0}));
                let n = stats["count"].as_u64().unwrap_or(0) as f64;
                let avg = stats["length"].as_u64().unwrap_or(0) as f64 / n.max(1.0);
                let mut scores: BTreeMap<String, f64> = BTreeMap::new();
                for term in terms {
                    let postings = kv.scan("lexical_postings", &key(&[collection, &term]))?;
                    let df = postings.len() as f64;
                    let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).max(1.0).ln();
                    for (_, posting) in postings {
                        let Some(id) = posting["id"].as_str() else {
                            continue;
                        };
                        let f = posting["frequency"].as_u64().unwrap_or(0) as f64;
                        let length = posting["length"].as_u64().unwrap_or(0) as f64;
                        *scores.entry(id.to_string()).or_default() +=
                            idf * f * 2.2 / (f + 1.2 * (0.25 + 0.75 * length / avg.max(1.0)));
                    }
                }
                for (id, s) in scores {
                    if let Some(doc) = kv.get("documents", &key(&[collection, &id]))? {
                        if matches(&doc, p.get("filter")) {
                            docs.insert(id.clone(), doc);
                            lexical.push((id, s));
                        }
                    }
                }
                lexical.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
            }
            let hybrid = text_query.is_some() && p.get("vector").is_some();
            let mut ranked = if hybrid {
                let mut fused = BTreeMap::new();
                for list in [&dense, &lexical] {
                    for (rank, (id, _)) in list.iter().enumerate() {
                        *fused.entry(id.clone()).or_insert(0.0) += 1.0 / (60.0 + rank as f64 + 1.0);
                    }
                }
                fused.into_iter().collect::<Vec<_>>()
            } else if text_query.is_some() {
                lexical
            } else {
                dense
            };
            ranked.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
            let hits = ranked
                .into_iter()
                .take(limit as usize)
                .filter_map(|(id, s)| {
                    docs.get(&id).map(
                        |d| json!({"id":id,"score":s,"revision":d["revision"],"value":d["value"]}),
                    )
                })
                .collect::<Vec<_>>();
            Ok(
                json!({"hits":hits,"mode":if hybrid{"hybrid_rrf"}else if text_query.is_some(){"bm25"}else if exact||p.get("filter").is_some(){"exact"}else{"graph_approximate"}}),
            )
        }
        _ => Err(Error::new("unsupported", "unknown search operation")),
    }
}

/// Maintain lexical postings in the same transaction as source-document changes.
pub fn index_document(kv: &mut dyn Kv, collection: &str, id: &str) -> Result<()> {
    let k = key(&[collection, id]);
    let mut stats = kv
        .get("lexical_stats", &key(&[collection]))?
        .unwrap_or_else(|| json!({"count":0,"length":0}));
    let mut count = stats["count"].as_u64().unwrap_or(0);
    let mut length = stats["length"].as_u64().unwrap_or(0);
    if let Some(old) = kv.get("lexical_docs", &k)? {
        if let Some(terms) = old["terms"].as_object() {
            for term in terms.keys() {
                kv.delete("lexical_postings", &key(&[collection, term, id]))?;
            }
        }
        count = count.saturating_sub(1);
        length = length.saturating_sub(old["length"].as_u64().unwrap_or(0));
        kv.delete("lexical_docs", &k)?;
    }
    if let Some(doc) = kv.get("documents", &k)? {
        let mut tok = Vec::new();
        tokens(&doc["value"], &mut tok);
        let len = tok.len() as u64;
        let mut tf: BTreeMap<String, u64> = BTreeMap::new();
        for t in tok {
            *tf.entry(t).or_default() += 1;
        }
        for (term, f) in &tf {
            kv.put(
                "lexical_postings",
                &key(&[collection, term, id]),
                &json!({"id":id,"frequency":f,"length":len}),
            )?;
        }
        kv.put(
            "lexical_docs",
            &k,
            &json!({"length":len,"terms":tf,"revision":doc["revision"]}),
        )?;
        count = count
            .checked_add(1)
            .ok_or_else(|| bad("lexical count overflow"))?;
        length = length
            .checked_add(len)
            .ok_or_else(|| bad("lexical length overflow"))?;
    }
    stats["count"] = json!(count);
    stats["length"] = json!(length);
    kv.put("lexical_stats", &key(&[collection]), &stats)
}
