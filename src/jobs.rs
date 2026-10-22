use crate::model::{Command, Error, Result};
use crate::store::{emit, key, Kv};
use serde_json::{json, Value};

fn err(message: &str) -> Error {
    Error::new("invalid_argument", message)
}
fn text<'a>(v: &'a Value, name: &str) -> Result<&'a str> {
    v.get(name)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty() && s.len() <= 512)
        .ok_or_else(|| err(&format!("{name} must be a nonempty string up to 512 bytes")))
}
fn number(v: &Value, name: &str, default: u64, min: u64, max: u64) -> Result<u64> {
    let n = match v.get(name) {
        None => default,
        Some(x) => x.as_u64().ok_or_else(|| err(name))?,
    };
    if n < min || n > max {
        return Err(err(&format!("{name} outside supported range")));
    }
    Ok(n)
}
fn add(a: u64, b: u64) -> Result<u64> {
    a.checked_add(b).ok_or_else(|| err("timestamp overflow"))
}
fn get(kv: &dyn Kv, id: &str) -> Result<Value> {
    kv.get("jobs", &key(&[id]))?
        .ok_or_else(|| Error::new("not_found", "job does not exist"))
}
fn save(kv: &mut dyn Kv, cmd: &Command, j: &Value, kind: &str) -> Result<Value> {
    let mut j = j.clone();
    j["updated_at_ms"] = json!(cmd.now_ms);
    let id = text(&j, "id")?;
    if let Some(old) = kv.get("jobs", &key(&[id]))? {
        pending_index(kv, &old, false)?;
    }
    kv.put("jobs", &key(&[id]), &j)?;
    pending_index(kv, &j, true)?;
    kv.put("queues", &key(&[text(&j, "queue")?]), &j["queue"])?;
    emit(
        kv,
        cmd,
        kind,
        id,
        json!({"id":id,"queue":j["queue"],"status":j["status"]}),
    )?;
    Ok(j.clone())
}
fn terminal(j: &Value) -> bool {
    matches!(
        j["status"].as_str(),
        Some("completed" | "cancelled" | "expired" | "dead_letter")
    )
}
fn validate_receipt(j: &Value, p: &Value, now: u64) -> Result<()> {
    if j["status"] != "leased"
        || j["receipt"].as_str() != Some(text(p, "receipt")?)
        || j["lease_until_ms"].as_u64().unwrap_or(0) <= now
    {
        return Err(Error::new(
            "conflict",
            "receipt is stale, expired, or job is not leased",
        ));
    }
    Ok(())
}
pub fn is_mutation(action: &str) -> bool {
    !matches!(action, "job.get" | "job.list" | "job.queues")
}
pub fn execute(kv: &mut dyn Kv, cmd: &Command) -> Result<Value> {
    let p = &cmd.payload;
    let now = cmd.now_ms;
    match cmd.action.as_str() {
        "job.queues" => {
            let queues = kv
                .scan("queues", "")?
                .into_iter()
                .map(|(_, q)| q)
                .collect::<Vec<_>>();
            Ok(json!({"queues":queues}))
        }
        "job.enqueue" => {
            let id = text(p, "id")?;
            let queue = text(p, "queue")?;
            if let Some(old) = kv.get("jobs", &key(&[id]))? {
                if old["input"] == *p {
                    return Ok(old);
                }
                return Err(Error::new("conflict", "job id already used"));
            }
            let max_attempts = number(p, "max_attempts", 5, 1, 1000)?;
            let lease = number(p, "lease_ms", 30000, 100, 3600000)?;
            let available = number(p, "available_at_ms", now, 0, u64::MAX)?;
            if let Some(o) = p.get("ordering_key") {
                if !o.is_string() || o.as_str().unwrap_or("").len() > 512 {
                    return Err(err("invalid ordering_key"));
                }
            }
            if let Some(d) = p.get("delivery") {
                let url = reqwest::Url::parse(text(d, "url")?)
                    .map_err(|_| err("invalid delivery URL"))?;
                if !matches!(url.scheme(), "http" | "https")
                    || url.host_str().is_none()
                    || !url.username().is_empty()
                    || url.password().is_some()
                {
                    return Err(err("delivery must be an http(s) URL without credentials"));
                }
            }
            let expires = match p.get("expires_at_ms") {
                Some(v) => Some(
                    v.as_u64()
                        .ok_or_else(|| err("expires_at_ms must be an integer"))?,
                ),
                None => None,
            };
            if expires.is_some_and(|e| e <= now || e <= available) {
                return Err(err("expiry must follow availability"));
            }
            let seq = kv
                .get("meta", "job_sequence")?
                .and_then(|v| v.as_u64())
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| err("sequence overflow"))?;
            kv.put("meta", "job_sequence", &json!(seq))?;
            let j = json!({"id":id,"queue":queue,"payload":p.get("payload").cloned().unwrap_or(Value::Null),"status":"ready","attempt":0,"generation":0,"max_attempts":max_attempts,"lease_ms":lease,"available_at_ms":available,"expires_at_ms":expires,"ordering_key":p.get("ordering_key"),"delivery":p.get("delivery"),"created_at_ms":now,"sequence":seq,"input":p});
            save(kv, cmd, &j, "job.enqueued")
        }
        "job.get" => get(kv, text(p, "id")?),
        "job.list" => {
            let limit = number(p, "limit", 100, 1, 1000)? as usize;
            let mut jobs = kv
                .scan("jobs", "")?
                .into_iter()
                .map(|(_, v)| v)
                .filter(|j| {
                    p.get("queue").is_none_or(|q| q == &j["queue"])
                        && p.get("status").is_none_or(|s| s == &j["status"])
                })
                .collect::<Vec<_>>();
            jobs.sort_by_key(|j| j["sequence"].as_u64().unwrap_or(0));
            jobs.truncate(limit);
            Ok(json!({"jobs":jobs}))
        }
        "job.pause" | "job.resume" => {
            let queue = text(p, "queue")?;
            let paused = cmd.action == "job.pause";
            kv.put("queue_settings", &key(&[queue]), &json!({"paused":paused}))?;
            Ok(json!({"queue":queue,"paused":paused}))
        }
        "job.claim" => {
            let queue = text(p, "queue")?;
            let worker = text(p, "worker")?;
            let mode = p
                .get("delivery_mode")
                .and_then(Value::as_str)
                .unwrap_or("pull");
            if !matches!(mode, "pull" | "http") {
                return Err(err("invalid delivery_mode"));
            }
            if kv
                .get("queue_settings", &key(&[queue]))?
                .is_some_and(|v| v["paused"] == true)
            {
                return Ok(json!({"job":null}));
            }
            let prefix = key(&[queue, mode]);
            let scan_key = format!("claim:{prefix}");
            let after = kv
                .get("job_scan", &scan_key)?
                .and_then(|v| v.as_str().map(str::to_owned))
                .unwrap_or_default();
            let mut all = Vec::new();
            let page = kv.scan_page("job_due", &prefix, &after, 1024)?;
            let next = if page.len() == 1024
                && page
                    .last()
                    .is_some_and(|(_, v)| v["at"].as_u64().unwrap_or(u64::MAX) <= now)
            {
                page.last().map(|(k, _)| k.clone()).unwrap_or_default()
            } else {
                String::new()
            };
            // The due-time index lets a deep delayed backlog remain untouched by claims.
            for (_, pointer) in page {
                let at = pointer["at"].as_u64().unwrap_or(u64::MAX);
                if at > now {
                    break;
                }
                if let Some(id) = pointer["id"].as_str() {
                    if let Some(j) = kv.get("jobs", &key(&[id]))? {
                        all.push(j);
                    }
                }
            }
            all.sort_by_key(|j| j["sequence"].as_u64().unwrap_or(0));
            for j in &mut all {
                if terminal(j) {
                    continue;
                }
                if j["expires_at_ms"].as_u64().is_some_and(|e| e <= now) {
                    j["status"] = json!("expired");
                    save(kv, cmd, j, "job.expired")?;
                } else if j["status"] == "leased"
                    && j["lease_until_ms"].as_u64().unwrap_or(0) <= now
                    && j["attempt"].as_u64().unwrap_or(0) >= j["max_attempts"].as_u64().unwrap_or(1)
                {
                    j["status"] = json!("dead_letter");
                    save(kv, cmd, j, "job.exhausted")?;
                }
            }
            for j in &all {
                if j["queue"] != queue
                    || terminal(j)
                    || (mode == "http") != j["delivery"].is_object()
                {
                    continue;
                }
                let due = j["status"] == "ready"
                    && j["available_at_ms"].as_u64().unwrap_or(u64::MAX) <= now;
                let reclaim = j["status"] == "leased"
                    && j["lease_until_ms"].as_u64().unwrap_or(u64::MAX) <= now;
                if !due && !reclaim {
                    continue;
                }
                if let Some(order) = j["ordering_key"].as_str() {
                    let first = kv.scan_page("job_order", &key(&[queue, order]), "", 1)?;
                    if first
                        .first()
                        .is_some_and(|(_, v)| v.as_str() != j["id"].as_str())
                    {
                        continue;
                    }
                }
                let mut claimed = j.clone();
                let attempt = j["attempt"].as_u64().unwrap_or(0) + 1;
                let generation = j["generation"]
                    .as_u64()
                    .unwrap_or(0)
                    .checked_add(1)
                    .ok_or_else(|| err("generation overflow"))?;
                let lease = number(
                    p,
                    "lease_ms",
                    j["lease_ms"].as_u64().unwrap_or(30000),
                    100,
                    3600000,
                )?;
                let receipt = format!("{}:{generation}", cmd.request_id);
                claimed["status"] = json!("leased");
                claimed["attempt"] = json!(attempt);
                claimed["generation"] = json!(generation);
                claimed["receipt"] = json!(receipt);
                claimed["worker"] = json!(worker);
                claimed["lease_until_ms"] = json!(add(now, lease)?);
                save(kv, cmd, &claimed, "job.claimed")?;
                return Ok(json!({"job":claimed,"receipt":receipt}));
            }
            kv.put("job_scan", &scan_key, &json!(next))?;
            Ok(json!({"job":null}))
        }
        "job.ack" | "job.nack" | "job.heartbeat" => {
            let mut j = get(kv, text(p, "id")?)?;
            if cmd.action == "job.ack"
                && j["status"] == "completed"
                && j["receipt"].as_str() == Some(text(p, "receipt")?)
            {
                return Ok(j);
            }
            validate_receipt(&j, p, now)?;
            match cmd.action.as_str() {
                "job.ack" => {
                    j["status"] = json!("completed");
                    j["completed_at_ms"] = json!(now);
                }
                "job.heartbeat" => {
                    let lease = number(
                        p,
                        "lease_ms",
                        j["lease_ms"].as_u64().unwrap_or(30000),
                        100,
                        3600000,
                    )?;
                    j["lease_until_ms"] = json!(add(now, lease)?);
                }
                _ => {
                    let retryable = match p.get("retryable") {
                        Some(v) => v
                            .as_bool()
                            .ok_or_else(|| err("retryable must be boolean"))?,
                        None => true,
                    };
                    if retryable
                        && j["attempt"].as_u64().unwrap_or(0)
                            < j["max_attempts"].as_u64().unwrap_or(1)
                    {
                        let base = 1000u64
                            .saturating_mul(1u64 << j["attempt"].as_u64().unwrap_or(1).min(16))
                            .min(3600000);
                        let jitter = cmd
                            .request_id
                            .bytes()
                            .fold(0u64, |a, b| a.wrapping_mul(31).wrapping_add(b as u64))
                            % (base / 4 + 1);
                        let delay = number(p, "delay_ms", base + jitter, 0, 86400000)?;
                        j["status"] = json!("ready");
                        j["available_at_ms"] = json!(add(now, delay)?);
                    } else {
                        j["status"] = json!("dead_letter");
                    }
                }
            }
            save(kv, cmd, &j, &cmd.action)
        }
        "job.cancel" | "job.retry" => {
            let mut j = get(kv, text(p, "id")?)?;
            if cmd.action == "job.retry" {
                if j["status"] != "dead_letter" {
                    return Err(Error::new(
                        "conflict",
                        "only dead-letter jobs can be retried",
                    ));
                }
                j["status"] = json!("ready");
                j["attempt"] = json!(0);
                j["available_at_ms"] = json!(now);
                j["receipt"] = Value::Null;
            } else {
                if j["status"] == "completed" {
                    return Err(Error::new("conflict", "completed job cannot be cancelled"));
                }
                j["status"] = json!("cancelled");
            }
            save(kv, cmd, &j, &cmd.action)
        }
        _ => Err(Error::new("unsupported", "unknown job operation")),
    }
}

fn pending_index(kv: &mut dyn Kv, j: &Value, insert: bool) -> Result<()> {
    if terminal(j) {
        return Ok(());
    }
    let queue = text(j, "queue")?;
    let id = text(j, "id")?;
    let sequence = j["sequence"]
        .as_u64()
        .ok_or_else(|| err("invalid job sequence"))?;
    let at = if j["status"] == "leased" {
        j["lease_until_ms"].as_u64().unwrap_or(0)
    } else {
        j["available_at_ms"].as_u64().unwrap_or(0)
    };
    let expiry = j["expires_at_ms"].as_u64().map(|e| e.min(at)).unwrap_or(at);
    let due = format!(
        "{}{:020}/{sequence:020}",
        key(&[
            queue,
            if j["delivery"].is_object() {
                "http"
            } else {
                "pull"
            }
        ]),
        expiry
    );
    if insert {
        kv.put("job_due", &due, &json!({"id":id,"at":expiry}))?;
    } else {
        kv.delete("job_due", &due)?;
    }
    if let Some(order) = j["ordering_key"].as_str() {
        let k = format!("{}{sequence:020}", key(&[queue, order]));
        if insert {
            kv.put("job_order", &k, &json!(id))?;
        } else {
            kv.delete("job_order", &k)?;
        }
    }
    Ok(())
}
