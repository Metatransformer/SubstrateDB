use crate::{
    model::{Command, Error, Result},
    store::{emit, key, Kv},
};
use serde_json::{json, Value};
fn string<'a>(p: &'a Value, k: &str) -> Result<&'a str> {
    p[k].as_str()
        .filter(|s| !s.is_empty() && s.len() <= 100)
        .ok_or_else(|| Error::new("invalid_argument", format!("{k} required")))
}
pub fn is_mutation(a: &str) -> bool {
    a != "route.list"
}
pub fn execute(kv: &mut dyn Kv, cmd: &Command) -> Result<Value> {
    let p = &cmd.payload;
    match cmd.action.as_str() {
        "route.bind" => {
            let id = string(p, "id")?;
            let topic = string(p, "topic")?;
            let queue = string(p, "queue")?;
            if topic
                .split('.')
                .enumerate()
                .any(|(i, s)| s == "#" && i + 1 != topic.split('.').count())
            {
                return Err(Error::new("invalid_argument", "# wildcard must be final"));
            }
            let binding = json!({"id":id,"topic":topic,"queue":queue,"delivery":p.get("delivery")});
            kv.put("routes", &key(&[id]), &binding)?;
            Ok(binding)
        }
        "route.unbind" => {
            kv.delete("routes", &key(&[string(p, "id")?]))?;
            Ok(json!({"deleted":true}))
        }
        "route.list" => Ok(
            json!({"bindings":kv.scan("routes","")?.into_iter().map(|(_,v)|v).collect::<Vec<_>>()}),
        ),
        "message.publish" => {
            let topic = string(p, "topic")?;
            let id = string(p, "id")?;
            let mut deliveries = std::collections::BTreeMap::new();
            for (_, binding) in kv.scan("routes", "")? {
                if crate::server::topic_matches(binding["topic"].as_str().unwrap_or(""), topic) {
                    deliveries
                        .entry(binding["queue"].as_str().unwrap_or("").to_string())
                        .or_insert(binding);
                }
            }
            if deliveries.is_empty() {
                return Err(Error::new("unroutable", "no binding matches topic"));
            }
            let mut jobs = Vec::new();
            for (queue, binding) in deliveries {
                let mut payload = json!({"id":format!("message:{id}:{}",binding["id"].as_str().unwrap_or("")),"queue":queue,"payload":p["payload"]});
                if let Some(d) = binding.get("delivery").filter(|v| !v.is_null()) {
                    payload["delivery"] = d.clone();
                }
                for f in [
                    "available_at_ms",
                    "expires_at_ms",
                    "max_attempts",
                    "ordering_key",
                ] {
                    if let Some(v) = p.get(f) {
                        payload[f] = v.clone();
                    }
                }
                let j = crate::jobs::execute(
                    kv,
                    &Command {
                        action: "job.enqueue".into(),
                        payload,
                        ..cmd.clone()
                    },
                )?;
                jobs.push(j["id"].clone());
            }
            emit(
                kv,
                cmd,
                "message.published",
                topic,
                json!({"id":id,"jobs":jobs}),
            )?;
            Ok(json!({"id":id,"jobs":jobs}))
        }
        _ => Err(Error::new("unsupported", "unknown messaging operation")),
    }
}
