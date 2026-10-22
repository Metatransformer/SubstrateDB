use crate::{
    model::{Command, Error, Result},
    store::{key, Kv},
};
use serde_json::{json, Value};
const DAY: u64 = 86_400_000;
fn invalid(s: &str) -> Error {
    Error::new("invalid_argument", s)
}
pub fn execute(kv: &mut dyn Kv, cmd: &Command) -> Result<Value> {
    let mut settings=kv.get("scope_settings","limits")?.unwrap_or(json!({"max_bytes":1073741824u64,"request_retention_ms":DAY,"event_retention_ms":7*DAY,"terminal_job_retention_ms":7*DAY}));
    match cmd.action.as_str() {
        "scope.configure" => {
            for (k, v) in cmd
                .payload
                .as_object()
                .ok_or_else(|| invalid("object required"))?
            {
                let n = v
                    .as_u64()
                    .ok_or_else(|| invalid("limits must be unsigned integers"))?;
                match k.as_str() {
                    "max_bytes" if n >= 1024 * 1024 => {}
                    "request_retention_ms" if n >= DAY => {}
                    "event_retention_ms" | "terminal_job_retention_ms" if n >= 60000 => {}
                    _ => return Err(invalid("unknown limit or value below minimum")),
                };
                settings[k] = json!(n);
            }
            kv.put("scope_settings", "limits", &settings)?;
            Ok(settings)
        }
        "scope.work" => {
            let index_pending = !kv.scan_page("pending_vectors", "", "", 1)?.is_empty();
            let schedules_due = kv.scan("schedules", "")?.iter().any(|(_, s)| {
                s["paused"] != true && s["next_at_ms"].as_u64().is_some_and(|t| t <= cmd.now_ms)
            });
            let mut http_queues = Vec::new();
            for (_, queue) in kv.scan("queues", "")? {
                if let Some(q) = queue.as_str() {
                    if kv
                        .get("queue_settings", &key(&[q]))?
                        .is_some_and(|v| v["paused"] == true)
                    {
                        continue;
                    }
                    if kv
                        .scan_page("job_due", &key(&[q, "http"]), "", 1)?
                        .first()
                        .is_some_and(|(_, v)| v["at"].as_u64().is_some_and(|t| t <= cmd.now_ms))
                    {
                        http_queues.push(queue);
                    }
                }
            }
            Ok(
                json!({"index_pending":index_pending,"schedules_due":schedules_due,"http_queues":http_queues}),
            )
        }
        "scope.stats" => Ok(
            json!({"limits":settings,"logical_bytes":kv.get("accounting","bytes")?.unwrap_or(json!(0)),"event_sequence":kv.get("meta","event_sequence")?.unwrap_or(json!(0))}),
        ),
        "scope.maintain" => {
            let mut removed = 0u64;
            // Rotating cursors bound each sweep; expiration never depends on cleanup for read correctness.
            for bucket in ["cache", "requests", "jobs", "leases", "rate_limits"] {
                let cursor = kv
                    .get("maintenance", bucket)?
                    .and_then(|v| v.as_str().map(str::to_owned))
                    .unwrap_or_default();
                let page = kv.scan_page(bucket, "", &cursor, 256)?;
                let next = if page.len() == 256 {
                    page.last().map(|(k, _)| k.as_str()).unwrap_or("")
                } else {
                    ""
                };
                for (k, v) in &page {
                    let expired = match bucket {
                        "cache" | "leases" | "rate_limits" => {
                            v["expires_at_ms"].as_u64().is_some_and(|t| t <= cmd.now_ms)
                        }
                        "requests" => v["created_at_ms"].as_u64().is_some_and(|t| {
                            t.saturating_add(
                                settings["request_retention_ms"].as_u64().unwrap_or(DAY),
                            ) < cmd.now_ms
                        }),
                        _ => {
                            matches!(
                                v["status"].as_str(),
                                Some("completed" | "cancelled" | "expired")
                            ) && v["updated_at_ms"].as_u64().is_some_and(|t| {
                                t.saturating_add(
                                    settings["terminal_job_retention_ms"]
                                        .as_u64()
                                        .unwrap_or(7 * DAY),
                                ) < cmd.now_ms
                            })
                        }
                    };
                    if expired {
                        kv.delete(bucket, k)?;
                        removed += 1;
                    }
                }
                kv.put("maintenance", bucket, &json!(next))?;
            }
            let floor = kv
                .get("meta", "event_floor")?
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let mut through = floor;
            for (k, v) in kv.scan_page("events", "", &format!("{floor:020}"), 256)? {
                if v["timestamp_ms"]
                    .as_u64()
                    .unwrap_or(cmd.now_ms)
                    .saturating_add(settings["event_retention_ms"].as_u64().unwrap_or(7 * DAY))
                    >= cmd.now_ms
                {
                    break;
                }
                through = v["sequence"]
                    .as_u64()
                    .ok_or_else(|| Error::new("storage_error", "invalid event"))?;
                kv.delete("events", &k)?;
                removed += 1;
            }
            kv.put("meta", "event_floor", &json!(through))?;
            // Dead letters are retained until explicitly retried or cancelled.
            Ok(json!({"removed":removed,"event_floor":through}))
        }
        "scope.fence" => {
            let resource = cmd.payload["resource"]
                .as_str()
                .filter(|s| !s.is_empty() && s.len() <= 256)
                .ok_or_else(|| invalid("resource required"))?;
            let k = key(&[resource]);
            let next = kv
                .get("fences", &k)?
                .and_then(|v| v.as_u64())
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| invalid("fence exhausted"))?;
            kv.put("fences", &k, &json!(next))?;
            Ok(json!({"token":next}))
        }
        _ => Err(Error::new("unsupported", "unknown scope operation")),
    }
}
