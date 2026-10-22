use crate::model::{Command, Error, Result};
use crate::store::{key, Kv};
use serde_json::{json, Value};

fn invalid(s: &str) -> Error {
    Error::new("invalid_argument", s)
}
fn string<'a>(p: &'a Value, k: &str) -> Result<&'a str> {
    p[k].as_str()
        .filter(|s| !s.is_empty() && s.len() <= 1024)
        .ok_or_else(|| invalid(k))
}
fn read(kv: &dyn Kv, k: &str, now: u64) -> Result<Option<Value>> {
    Ok(kv
        .get("cache", k)?
        .filter(|v| v["expires_at_ms"].as_u64().is_none_or(|t| t > now)))
}
fn store(kv: &mut dyn Kv, k: &str, v: &Value) -> Result<Value> {
    kv.put("cache", k, v)?;
    Ok(v["value"].clone())
}
pub fn is_mutation(a: &str) -> bool {
    !matches!(
        a,
        "cache.get" | "cache.ttl" | "cache.hget" | "cache.members" | "cache.range" | "cache.zrange"
    )
}
pub fn execute(kv: &mut dyn Kv, cmd: &Command) -> Result<Value> {
    let p = &cmd.payload;
    let k = key(&[string(p, "key")?]);
    if cmd.action.starts_with("cache.lease_") {
        return lease(kv, cmd, &k);
    }
    if cmd.action == "cache.rate_limit" {
        return rate_limit(kv, cmd, &k);
    }
    let old = read(kv, &k, cmd.now_ms)?;
    let expected = match cmd.action.as_str() {
        "cache.hset" | "cache.hget" | "cache.hdel" => "hash",
        "cache.sadd" | "cache.srem" | "cache.members" => "set",
        "cache.lpush" | "cache.rpush" | "cache.lpop" | "cache.rpop" | "cache.range" => "list",
        "cache.zadd" | "cache.zrem" | "cache.zrange" => "sorted_set",
        _ => "value",
    };
    if !matches!(
        cmd.action.as_str(),
        "cache.set" | "cache.get" | "cache.delete" | "cache.expire" | "cache.ttl"
    ) && old.as_ref().is_some_and(|v| v["kind"] != expected)
    {
        return Err(Error::new("wrong_type", "cache key has another type"));
    }
    let initial = match expected {
        "hash" | "sorted_set" => json!({}),
        "set" | "list" => json!([]),
        _ => Value::Null,
    };
    let mut record = old
        .clone()
        .unwrap_or_else(|| json!({"kind":expected,"value":initial,"expires_at_ms":null}));
    match cmd.action.as_str() {
        "cache.get" => Ok(old.map(|v| v["value"].clone()).unwrap_or(Value::Null)),
        "cache.set" => {
            if p.get("value").is_none() {
                return Err(invalid("value is required"));
            }
            if p.get("nx").is_some_and(|v| !v.is_boolean())
                || p.get("xx").is_some_and(|v| !v.is_boolean())
            {
                return Err(invalid("nx/xx must be booleans"));
            }
            if p["nx"] == true && p["xx"] == true {
                return Err(invalid("nx and xx are mutually exclusive"));
            }
            if p["nx"] == true && old.is_some() || p["xx"] == true && old.is_none() {
                return Ok(json!({"stored":false}));
            }
            let expiry = match p.get("ttl_ms") {
                None => None,
                Some(v) => Some(
                    cmd.now_ms
                        .checked_add(
                            v.as_u64()
                                .filter(|n| *n > 0)
                                .ok_or_else(|| invalid("ttl_ms"))?,
                        )
                        .ok_or_else(|| invalid("ttl overflow"))?,
                ),
            };
            kv.put(
                "cache",
                &k,
                &json!({"kind":"value","value":p["value"],"expires_at_ms":expiry}),
            )?;
            Ok(json!({"stored":true}))
        }
        "cache.delete" => {
            kv.delete("cache", &k)?;
            Ok(json!({"deleted":old.is_some()}))
        }
        "cache.expire" => {
            if old.is_none() {
                return Ok(json!(false));
            }
            let ttl = p["ttl_ms"].as_u64().ok_or_else(|| invalid("ttl_ms"))?;
            record["expires_at_ms"] = json!(cmd
                .now_ms
                .checked_add(ttl)
                .ok_or_else(|| invalid("ttl overflow"))?);
            kv.put("cache", &k, &record)?;
            Ok(json!(true))
        }
        "cache.ttl" => Ok(match old {
            None => json!(-2),
            Some(v) => v["expires_at_ms"]
                .as_u64()
                .map(|t| json!(t.saturating_sub(cmd.now_ms)))
                .unwrap_or(json!(-1)),
        }),
        "cache.increment" => {
            let n = if old.is_none() {
                0
            } else {
                record["value"]
                    .as_i64()
                    .ok_or_else(|| invalid("value is not integer"))?
            };
            let by = match p.get("by") {
                None => 1,
                Some(v) => v.as_i64().ok_or_else(|| invalid("by must be integer"))?,
            };
            record["value"] = json!(n
                .checked_add(by)
                .ok_or_else(|| invalid("counter overflow"))?);
            store(kv, &k, &record)
        }
        "cache.hset" | "cache.hdel" => {
            let f = string(p, "field")?;
            let map = record["value"]
                .as_object_mut()
                .ok_or_else(|| invalid("invalid hash"))?;
            if cmd.action == "cache.hdel" {
                map.remove(f);
            } else {
                let value = p.get("value").ok_or_else(|| invalid("value required"))?;
                map.insert(f.into(), value.clone());
            }
            store(kv, &k, &record)
        }
        "cache.hget" => Ok(record["value"][string(p, "field")?].clone()),
        "cache.sadd" | "cache.srem" => {
            let value = p.get("value").ok_or_else(|| invalid("value required"))?;
            let a = record["value"]
                .as_array_mut()
                .ok_or_else(|| invalid("invalid set"))?;
            if cmd.action == "cache.sadd" {
                if !a.contains(value) {
                    a.push(value.clone());
                }
            } else {
                a.retain(|v| v != value);
            }
            store(kv, &k, &record)
        }
        "cache.members" => Ok(record["value"].clone()),
        "cache.lpush" | "cache.rpush" => {
            let value = p.get("value").ok_or_else(|| invalid("value required"))?;
            let a = record["value"]
                .as_array_mut()
                .ok_or_else(|| invalid("invalid list"))?;
            if cmd.action == "cache.lpush" {
                a.insert(0, value.clone());
            } else {
                a.push(value.clone());
            }
            let n = a.len();
            store(kv, &k, &record)?;
            Ok(json!(n))
        }
        "cache.lpop" | "cache.rpop" => {
            let a = record["value"]
                .as_array_mut()
                .ok_or_else(|| invalid("invalid list"))?;
            let v = if a.is_empty() {
                Value::Null
            } else if cmd.action == "cache.lpop" {
                a.remove(0)
            } else {
                a.pop().unwrap_or(Value::Null)
            };
            store(kv, &k, &record)?;
            Ok(v)
        }
        "cache.range" => {
            let start = p
                .get("start")
                .map(|v| v.as_u64().ok_or_else(|| invalid("start")))
                .transpose()?
                .unwrap_or(0);
            let limit = p
                .get("limit")
                .map(|v| v.as_u64().ok_or_else(|| invalid("limit")))
                .transpose()?
                .unwrap_or(100);
            if limit > 1000 {
                return Err(invalid("limit max 1000"));
            }
            Ok(Value::Array(
                record["value"]
                    .as_array()
                    .ok_or_else(|| invalid("invalid list"))?
                    .iter()
                    .skip(start as usize)
                    .take(limit as usize)
                    .cloned()
                    .collect(),
            ))
        }
        "cache.zadd" | "cache.zrem" => {
            let member = string(p, "member")?;
            if cmd.action == "cache.zadd" {
                let score = p["score"]
                    .as_f64()
                    .filter(|n| n.is_finite())
                    .ok_or_else(|| invalid("score"))?;
                record["value"][member] = json!(score);
            } else {
                record["value"]
                    .as_object_mut()
                    .ok_or_else(|| invalid("invalid sorted set"))?
                    .remove(member);
            }
            store(kv, &k, &record)
        }
        "cache.zrange" => {
            let limit = p
                .get("limit")
                .map(|v| v.as_u64().ok_or_else(|| invalid("limit")))
                .transpose()?
                .unwrap_or(100);
            if limit > 1000 {
                return Err(invalid("limit max 1000"));
            }
            let mut entries = record["value"]
                .as_object()
                .ok_or_else(|| invalid("invalid sorted set"))?
                .iter()
                .map(|(m, s)| (m.clone(), s.as_f64().unwrap_or(0.0)))
                .collect::<Vec<_>>();
            entries.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
            if p["descending"] == true {
                entries.reverse();
            }
            Ok(json!(entries
                .into_iter()
                .take(limit as usize)
                .map(|(m, s)| json!({"member":m,"score":s}))
                .collect::<Vec<_>>()))
        }
        _ => Err(Error::new("unsupported", "unknown cache operation")),
    }
}

fn lease(kv: &mut dyn Kv, cmd: &Command, k: &str) -> Result<Value> {
    let p = &cmd.payload;
    let owner = string(p, "owner")?;
    let old = kv.get("leases", k)?;
    let active = old
        .as_ref()
        .filter(|v| v["expires_at_ms"].as_u64().is_some_and(|t| t > cmd.now_ms));
    let ttl = if cmd.action == "cache.lease_release" {
        0
    } else {
        p["ttl_ms"]
            .as_u64()
            .filter(|t| *t >= 100 && *t <= 86400000)
            .ok_or_else(|| invalid("lease ttl_ms must be 100..86400000"))?
    };
    match cmd.action.as_str() {
        "cache.lease_acquire" => {
            if active.is_some() {
                return Ok(json!({"acquired":false}));
            }
            let generation = kv
                .get("lease_fences", k)?
                .and_then(|v| v.as_u64())
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| invalid("fencing token exhausted"))?;
            let expires = cmd
                .now_ms
                .checked_add(ttl)
                .ok_or_else(|| invalid("expiry overflow"))?;
            let record = json!({"owner":owner,"token":generation,"expires_at_ms":expires});
            kv.put("lease_fences", k, &json!(generation))?;
            kv.put("leases", k, &record)?;
            Ok(json!({"acquired":true,"token":generation,"expires_at_ms":expires}))
        }
        "cache.lease_renew" | "cache.lease_release" => {
            let mut record = active
                .filter(|v| v["owner"] == owner && v["token"] == p["token"] && p["token"].is_u64())
                .cloned()
                .ok_or_else(|| {
                    Error::new("conflict", "lease expired or fencing token does not match")
                })?;
            if cmd.action == "cache.lease_release" {
                kv.delete("leases", k)?;
                return Ok(json!({"released":true}));
            }
            record["expires_at_ms"] = json!(cmd
                .now_ms
                .checked_add(ttl)
                .ok_or_else(|| invalid("expiry overflow"))?);
            kv.put("leases", k, &record)?;
            Ok(record)
        }
        _ => Err(Error::new("unsupported", "unknown lease operation")),
    }
}
fn rate_limit(kv: &mut dyn Kv, cmd: &Command, k: &str) -> Result<Value> {
    let p = &cmd.payload;
    let window = p["window_ms"]
        .as_u64()
        .filter(|n| *n > 0 && *n <= 86400000)
        .ok_or_else(|| invalid("window_ms must be 1..86400000"))?;
    let limit = p["limit"]
        .as_u64()
        .filter(|n| *n > 0)
        .ok_or_else(|| invalid("limit must be positive"))?;
    let bucket = cmd.now_ms / window;
    let old = kv.get("rate_limits", k)?;
    let count = old
        .as_ref()
        .filter(|v| v["bucket"] == bucket && v["window_ms"] == window)
        .and_then(|v| v["count"].as_u64())
        .unwrap_or(0);
    let allowed = count < limit;
    let next = if allowed { count + 1 } else { count };
    let reset = cmd.now_ms.saturating_add(window - cmd.now_ms % window);
    kv.put(
        "rate_limits",
        k,
        &json!({"bucket":bucket,"window_ms":window,"count":next,"expires_at_ms":reset}),
    )?;
    Ok(json!({"allowed":allowed,"remaining":limit.saturating_sub(next),"reset_at_ms":reset}))
}
