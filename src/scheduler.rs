use crate::model::{Command, Error, Result};
use crate::store::{key, Kv};
use chrono::{DateTime, LocalResult, TimeZone, Utc};
use chrono_tz::Tz;
use croner::Cron;
use serde_json::{json, Value};

fn bad(s: &str) -> Error {
    Error::new("invalid_argument", s)
}
fn strval<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    v[k].as_str()
        .filter(|s| !s.is_empty() && s.len() <= 200)
        .ok_or_else(|| bad(k))
}
pub fn next_occurrence(pattern: &str, zone: &str, after: u64, repeat: &str) -> Result<u64> {
    let tz: Tz = zone.parse().map_err(|_| bad("unknown IANA timezone"))?;
    if pattern.split_whitespace().count() != 5 {
        return Err(bad(
            "cron requires five fields (minute hour day month weekday)",
        ));
    }
    let cron = Cron::new(pattern)
        .parse()
        .map_err(|e| bad(&e.to_string()))?;
    let start = DateTime::<Utc>::from_timestamp_millis(
        i64::try_from(after).map_err(|_| bad("timestamp out of range"))?,
    )
    .ok_or_else(|| bad("timestamp out of range"))?
    .with_timezone(&tz);
    let mut local = start.naive_local().and_utc();
    for _ in 0..10000 {
        let candidate = cron
            .find_next_occurrence(&local, false)
            .map_err(|e| bad(&e.to_string()))?;
        let times = match tz.from_local_datetime(&candidate.naive_utc()) {
            LocalResult::None => {
                local = candidate;
                continue;
            }
            LocalResult::Single(t) => vec![t],
            LocalResult::Ambiguous(a, b) => {
                if repeat == "second" {
                    vec![a.max(b)]
                } else {
                    vec![a.min(b)]
                }
            }
        };
        for t in times {
            let ms = t.timestamp_millis();
            if ms >= 0 && (ms as u64) > after {
                return Ok(ms as u64);
            }
        }
        local = candidate;
    }
    Err(bad("cron has no occurrence within search bound"))
}
pub fn is_mutation(a: &str) -> bool {
    !matches!(a, "schedule.get" | "schedule.list")
}
pub fn execute(kv: &mut dyn Kv, cmd: &Command) -> Result<Value> {
    let p = &cmd.payload;
    match cmd.action.as_str() {
        "schedule.put" => {
            let id = strval(p, "id")?;
            let cron = strval(p, "cron")?;
            let tz = p.get("timezone").and_then(Value::as_str).unwrap_or("UTC");
            let repeat = p
                .get("dst_repeat")
                .and_then(Value::as_str)
                .unwrap_or("first");
            if !matches!(repeat, "first" | "second") {
                return Err(bad("dst_repeat must be first or second"));
            }
            let missed = p.get("missed").and_then(Value::as_str).unwrap_or("once");
            if !matches!(missed, "once" | "skip" | "replay") {
                return Err(bad("missed must be once, skip, or replay"));
            }
            let template = p
                .get("job")
                .filter(|v| v.is_object())
                .ok_or_else(|| bad("job template required"))?;
            strval(template, "queue")?;
            let mut validation_payload = template.clone();
            validation_payload["id"] = json!("schedule-template-validation");
            if template.get("id").is_some()
                || template.get("available_at_ms").is_some()
                || template.get("expires_at_ms").is_some()
            {
                return Err(bad(
                    "schedule template may not contain id or absolute availability/expiry",
                ));
            }
            crate::jobs::execute(
                &mut crate::store::MemoryKv::default(),
                &Command {
                    action: "job.enqueue".into(),
                    payload: validation_payload,
                    ..cmd.clone()
                },
            )?;
            let max = p
                .get("max_catch_up")
                .map(|v| v.as_u64().ok_or_else(|| bad("max_catch_up")))
                .transpose()?
                .unwrap_or(100);
            if max == 0 || max > 1000 {
                return Err(bad("max_catch_up must be 1..1000"));
            }
            let overlap = p
                .get("allow_overlap")
                .map(|v| {
                    v.as_bool()
                        .ok_or_else(|| bad("allow_overlap must be boolean"))
                })
                .transpose()?
                .unwrap_or(false);
            let next = next_occurrence(cron, tz, cmd.now_ms, repeat)?;
            let old = kv.get("schedules", &key(&[id]))?;
            let record = json!({"id":id,"cron":cron,"timezone":tz,"dst_repeat":repeat,"missed":missed,"max_catch_up":max,"allow_overlap":overlap,"job":template,"next_at_ms":next,"paused":false,"last_job_id":old.and_then(|v|v["last_job_id"].as_str().map(str::to_owned))});
            kv.put("schedules", &key(&[id]), &record)?;
            Ok(record)
        }
        "schedule.get" => kv
            .get("schedules", &key(&[strval(p, "id")?]))?
            .ok_or_else(|| Error::new("not_found", "schedule not found")),
        "schedule.list" => Ok(
            json!({"schedules":kv.scan("schedules","")?.into_iter().map(|(_,v)|v).collect::<Vec<_>>()}),
        ),
        "schedule.pause" | "schedule.resume" | "schedule.delete" => {
            let k = key(&[strval(p, "id")?]);
            let mut s = kv
                .get("schedules", &k)?
                .ok_or_else(|| Error::new("not_found", "schedule not found"))?;
            if cmd.action == "schedule.delete" {
                kv.delete("schedules", &k)?;
                Ok(json!({"deleted":true}))
            } else {
                s["paused"] = json!(cmd.action == "schedule.pause");
                kv.put("schedules", &k, &s)?;
                Ok(s)
            }
        }
        "schedule.tick" => {
            let mut created = Vec::new();
            for (k, mut s) in kv.scan("schedules", "")? {
                if s["paused"] == true {
                    continue;
                }
                let mut due = s["next_at_ms"]
                    .as_u64()
                    .ok_or_else(|| bad("invalid schedule state"))?;
                if due > cmd.now_ms {
                    continue;
                }
                let busy = if s["allow_overlap"] == true {
                    false
                } else if let Some(id) = s["last_job_id"].as_str() {
                    kv.get("jobs", &key(&[id]))?.is_some_and(|j| {
                        !matches!(
                            j["status"].as_str(),
                            Some("completed" | "cancelled" | "dead_letter" | "expired")
                        )
                    })
                } else {
                    false
                };
                let pattern = s["cron"].as_str().unwrap_or("").to_string();
                let tz = s["timezone"].as_str().unwrap_or("UTC").to_string();
                let repeat = s["dst_repeat"].as_str().unwrap_or("first").to_string();
                let missed = s["missed"].as_str().unwrap_or("once");
                let count = if missed == "replay" {
                    s["max_catch_up"].as_u64().unwrap_or(100)
                } else {
                    1
                };
                let skip = busy || missed == "skip" && cmd.now_ms.saturating_sub(due) > 60000;
                if !skip {
                    for _ in 0..count {
                        if due > cmd.now_ms {
                            break;
                        }
                        let id = format!("schedule:{}:{due}", s["id"].as_str().unwrap_or(""));
                        let mut payload = s["job"].clone();
                        payload["id"] = json!(id);
                        payload["available_at_ms"] = json!(cmd.now_ms);
                        if s["allow_overlap"] != true {
                            payload["ordering_key"] =
                                json!(format!("schedule:{}", s["id"].as_str().unwrap_or("")));
                        }
                        let enq = Command {
                            action: "job.enqueue".into(),
                            payload,
                            ..cmd.clone()
                        };
                        crate::jobs::execute(kv, &enq)?;
                        created.push(id.clone());
                        s["last_job_id"] = json!(id);
                        due = next_occurrence(&pattern, &tz, due, &repeat)?;
                    }
                }
                if s["missed"] != "replay" || skip {
                    due = next_occurrence(&pattern, &tz, cmd.now_ms, &repeat)?;
                }
                s["next_at_ms"] = json!(due);
                kv.put("schedules", &k, &s)?;
            }
            Ok(json!({"created":created}))
        }
        _ => Err(Error::new("unsupported", "unknown schedule operation")),
    }
}
