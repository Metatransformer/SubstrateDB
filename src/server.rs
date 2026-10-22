use crate::{
    auth::{self, Capability},
    model::{Command, Error},
    runtime::{command, now_ms, Runtime},
};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        DefaultBodyLimit, State,
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::StreamExt;
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::atomic::Ordering, time::Duration};

pub fn router(runtime: Runtime) -> Router {
    Router::new()
        .route("/internal/realtime", post(relay))
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/v1/command", post(execute))
        .route("/v1/capabilities", post(capability))
        .route("/v1/service-tokens", post(service_token))
        .route("/v1/ws", get(upgrade))
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .with_state(runtime)
}
async fn relay(State(r): State<Runtime>, headers: HeaderMap, Json(value): Json<Value>) -> Response {
    if !admin(&r, &headers) {
        return failure(Error::new("unauthorized", "cluster credential required"));
    }
    if !matches!(
        value["type"].as_str(),
        Some("publish" | "request" | "reply" | "presence_update" | "presence_leave")
    ) || value["namespace"].as_str().is_none()
        || value["partition"].as_str().is_none()
        || value["topic"].as_str().is_none()
    {
        return failure(Error::new("invalid_argument", "invalid relay envelope"));
    }
    r.receive_ephemeral(value).await;
    StatusCode::NO_CONTENT.into_response()
}
async fn health(State(r): State<Runtime>) -> Json<Value> {
    Json(json!({"status":"ok","mode":r.mode(),"version":env!("CARGO_PKG_VERSION")}))
}
fn token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("authorization")?
        .to_str()
        .ok()?
        .strip_prefix("Bearer ")
}
fn admin(runtime: &Runtime, headers: &HeaderMap) -> bool {
    // HMAC verification makes token comparison independent of a shared-prefix match.
    token(headers).is_some_and(|s| {
        auth::signature(runtime.admin.as_bytes(), 0, "admin", s.as_bytes())
            == auth::signature(
                runtime.admin.as_bytes(),
                0,
                "admin",
                runtime.admin.as_bytes(),
            )
    })
}
fn failure(e: Error) -> Response {
    let status = match e.code.as_str() {
        "not_found" => StatusCode::NOT_FOUND,
        "response_too_large" => StatusCode::PAYLOAD_TOO_LARGE,
        "unauthorized" => StatusCode::UNAUTHORIZED,
        "forbidden" => StatusCode::FORBIDDEN,
        "conflict" | "history_unavailable" => StatusCode::CONFLICT,
        "overloaded" => StatusCode::TOO_MANY_REQUESTS,
        "not_ready" => StatusCode::TOO_EARLY,
        "quorum_unavailable" | "cluster_error" | "not_leader" => StatusCode::SERVICE_UNAVAILABLE,
        "storage_error" | "internal" => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::BAD_REQUEST,
    };
    let message = if status.is_server_error() {
        "internal operation failed".to_string()
    } else {
        e.message
    };
    (
        status,
        Json(json!({"error":{"code":e.code,"message":message}})),
    )
        .into_response()
}
async fn metrics(State(r): State<Runtime>, headers: HeaderMap) -> Response {
    if !admin(&r, &headers) {
        return failure(Error::new("unauthorized", "admin credential required"));
    }
    let mut output=format!("substrate_operations_total {}\nsubstrate_failures_total {}\nsubstrate_runtime_info{{mode=\"{}\"}} 1\n",r.operations.load(Ordering::Relaxed),r.failures.load(Ordering::Relaxed),r.mode());
    if let Some(cluster) = r.cluster_metrics().await {
        if let Some(shards) = cluster["shards"].as_array() {
            for shard in shards {
                let shard_id = shard["shard"].as_u64().unwrap_or(0);
                let node_id = shard["node_id"].as_u64().unwrap_or(0);
                let leader_id = shard["leader_id"].as_u64().unwrap_or(0);
                let state = shard["state"].as_str().unwrap_or("Unknown");
                output.push_str(&format!("substrate_cluster_shard_info{{shard=\"{shard_id}\",node_id=\"{node_id}\",leader_id=\"{leader_id}\",state=\"{state}\"}} 1\n"));
                output.push_str(&format!(
                    "substrate_cluster_shard_running{{shard=\"{shard_id}\"}} {}\n",
                    u8::from(shard["running"] == true)
                ));
                if let Some(index) = shard["last_applied"]["index"].as_u64() {
                    output.push_str(&format!(
                        "substrate_cluster_shard_applied_index{{shard=\"{shard_id}\"}} {index}\n"
                    ));
                }
                if let Some(index) = shard["last_log_index"].as_u64() {
                    output.push_str(&format!(
                        "substrate_cluster_shard_last_log_index{{shard=\"{shard_id}\"}} {index}\n"
                    ));
                }
            }
        }
    }
    output.into_response()
}
async fn execute(
    State(r): State<Runtime>,
    headers: HeaderMap,
    Json(mut body): Json<Value>,
) -> Response {
    let root = admin(&r, &headers);
    if !root && token(&headers).is_none_or(|v| !v.starts_with("svc.")) {
        return failure(Error::new(
            "unauthorized",
            "trusted application credential required",
        ));
    }
    if body.get("request_id").is_none() {
        body["request_id"] = json!(uuid::Uuid::new_v4().to_string());
    }
    body["now_ms"] = json!(now_ms());
    let cmd: Command = match serde_json::from_value(body) {
        Ok(c) => c,
        Err(e) => return failure(Error::new("invalid_argument", e.to_string())),
    };
    if cmd.action == "search.scatter" {
        if !root {
            return failure(Error::new(
                "forbidden",
                "cross-partition search requires administrator credential",
            ));
        }
        return match scatter(&r, &cmd).await {
            Ok(value) => Json(json!({"result":value})).into_response(),
            Err(e) => failure(e),
        };
    }
    if !root {
        let Some(raw) = token(&headers).and_then(|v| v.strip_prefix("svc.")) else {
            return failure(Error::new(
                "unauthorized",
                "trusted application credential required",
            ));
        };
        let c = match r.verify(raw).await {
            Ok(c) => c,
            Err(e) => return failure(e),
        };
        if c.namespace != cmd.namespace || c.partition != cmd.partition {
            return failure(Error::new(
                "forbidden",
                "credential scope does not match command",
            ));
        }
        if let Err(e) = authorize_service(&r, &c, &cmd).await {
            return failure(e);
        }
    }
    match r.run(cmd).await {
        Ok(value) => Json(json!({"result":value})).into_response(),
        Err(e) => failure(e),
    }
}
async fn capability(
    State(r): State<Runtime>,
    headers: HeaderMap,
    Json(c): Json<Capability>,
) -> Response {
    if !admin(&r, &headers) {
        return failure(Error::new("unauthorized", "admin credential required"));
    }
    if c.expires_at_ms <= now_ms()
        || c.expires_at_ms.saturating_sub(now_ms()) > 3600000
        || c.id.is_empty()
        || c.id.len() > 256
        || c.namespace.is_empty()
        || c.partition.is_empty()
        || c.resources.is_empty()
        || c.resources.len() > 100
        || c.resources.iter().any(|s| s.is_empty() || s.len() > 256)
        || c.permissions
            .iter()
            .any(|s| !matches!(s.as_str(), "subscribe" | "publish" | "presence"))
        || !c.filter.is_null() && !c.filter.is_object()
    {
        return failure(Error::new(
            "invalid_argument",
            "invalid capability; maximum lifetime is one hour",
        ));
    }
    match auth::sign(&r.signing, &c) {
        Ok(token) => Json(json!({"token":token,"expires_at_ms":c.expires_at_ms})).into_response(),
        Err(e) => failure(e),
    }
}
async fn upgrade(ws: WebSocketUpgrade, State(r): State<Runtime>) -> Response {
    ws.max_message_size(1024 * 1024)
        .max_frame_size(1024 * 1024)
        .on_upgrade(move |s| session(s, r))
}
async fn send(socket: &mut WebSocket, value: Value) -> bool {
    let body = value.to_string();
    if body.len() > 4 * 1024 * 1024 {
        return false;
    }
    tokio::time::timeout(
        Duration::from_secs(5),
        socket.send(Message::Text(body.into())),
    )
    .await
    .is_ok_and(|v| v.is_ok())
}
pub fn topic_matches(pattern: &str, topic: &str) -> bool {
    let p = pattern.split('.').collect::<Vec<_>>();
    let t = topic.split('.').collect::<Vec<_>>();
    let mut i = 0;
    for (seg, pos) in p.iter().zip(0..) {
        if *seg == "#" {
            return pos == p.len() - 1;
        }
        if i >= t.len() || *seg != "*" && *seg != t[i] {
            return false;
        }
        i += 1;
    }
    i == t.len()
}
async fn session(mut socket: WebSocket, r: Runtime) {
    let Ok(_session_permit) = r.sessions.clone().try_acquire_owned() else {
        return;
    };
    let first = tokio::time::timeout(Duration::from_secs(10), socket.next()).await;
    let Ok(Some(Ok(Message::Text(first)))) = first else {
        return;
    };
    let Ok(first) = serde_json::from_str::<Value>(&first) else {
        return;
    };
    let Some(raw) = first["token"].as_str() else {
        return;
    };
    let mut raw = raw.to_string();
    let mut claims = match r.verify(&raw).await {
        Ok(c) => c,
        Err(e) => {
            send(&mut socket, json!({"type":"error","error":e})).await;
            return;
        }
    };
    let session_id = uuid::Uuid::new_v4().to_string();
    let mut messages = r.channels.subscribe();
    let mut channel: Option<String> = None;
    let mut live: Option<Value> = None;
    let mut rows: BTreeMap<String, Value> = BTreeMap::new();
    let mut cursor = 0u64;
    let mut interval = tokio::time::interval(Duration::from_millis(250));
    let mut presence_value: Option<Value> = None;
    let mut presence_refresh = 0u64;
    if !send(
        &mut socket,
        json!({"type":"authenticated","session_id":session_id}),
    )
    .await
    {
        return;
    }
    loop {
        tokio::select! {
                   incoming=socket.next()=>{
                     let Some(Ok(message))=incoming else{break};
                     let Message::Text(body)=message else{if matches!(message,Message::Close(_)){break;}continue};
                     let Ok(p)=serde_json::from_str::<Value>(&body)else{if !send(&mut socket,json!({"type":"error","error":{"code":"invalid_argument","message":"invalid JSON"}})).await{break;}continue};
                     let kind=p["type"].as_str().unwrap_or("");
                     if kind=="refresh"{
                       if let Some(t)=p["token"].as_str(){match r.verify(t).await{Ok(c) if c.namespace==claims.namespace&&c.partition==claims.partition=>{claims=c;raw=t.into();live=None;channel=None;rows.clear();presence_value=None;
                if let Some(value)={let mut presence=r.presence.write().await;presence.remove(&session_id)}{r.broadcast(json!({"type":"presence_leave","namespace":value["namespace"],"partition":value["partition"],"topic":value["topic"],"session_id":session_id})).await;}
        if !send(&mut socket,json!({"type":"authenticated","resubscribe":true})).await{break;}},_=>break}}
                       continue;
                     }
                     if kind=="subscribe"{
                       let Some(collection)=p["collection"].as_str()else{continue};
                       if !auth::permits(&claims,"subscribe",collection){if !send(&mut socket,json!({"type":"error","error":{"code":"forbidden"}})).await{break;}continue;}
                       let mut filter=p.get("filter").cloned().unwrap_or_else(||json!({}));if !filter.is_object(){continue;}
                       if let Some(scope)=claims.filter.as_object(){for(k,v)in scope{filter[k]=v.clone();}}
                       let mut query=json!({"collection":collection,"filter":filter,"limit":1000});if let Some(id)=p.get("id"){query["id"]=id.clone();}
                       match r.run(command(&claims.namespace,&claims.partition,"document.snapshot",query.clone())).await{
                         Ok(snapshot)=>{let values=snapshot["documents"].as_array().cloned().unwrap_or_default();if snapshot["total"].as_u64().unwrap_or(0)>1000{if !send(&mut socket,json!({"type":"error","error":{"code":"overloaded","message":"subscription exceeds 1000-document bound; narrow its indexed filter"}})).await{break;}continue;}
                           rows=values.iter().filter_map(|d|d["id"].as_str().map(|id|(id.to_string(),d.clone()))).collect();cursor=snapshot["sequence"].as_u64().unwrap_or(0);live=Some(query);if !send(&mut socket,json!({"type":"snapshot","documents":values,"sequence":cursor})).await{break;}
                         },Err(e)=>{if !send(&mut socket,json!({"type":"error","error":e})).await{break;}}
                       }continue;
                     }
                     if kind=="channel"{
                       if let Some(topic)=p["topic"].as_str(){if auth::permits(&claims,"subscribe",topic){channel=Some(topic.into());if !send(&mut socket,json!({"type":"joined","topic":topic})).await{break;}}}continue;
                     }
                     if kind=="publish"||kind=="request"||kind=="reply"{
                       if let Some(topic)=p["topic"].as_str(){if auth::permits(&claims,"publish",topic){r.broadcast(json!({"type":kind,"namespace":claims.namespace,"partition":claims.partition,"topic":topic,"payload":p["payload"],"correlation_id":p["correlation_id"],"sender":claims.id})).await;}}
                       continue;
                     }
                     if kind=="presence"{
                       if let Some(topic)=p["topic"].as_str(){if auth::permits(&claims,"presence",topic){let value=json!({"type":"presence_update","namespace":claims.namespace,"partition":claims.partition,"topic":topic,"session_id":session_id,"subject":claims.id,"state":p["state"],"expires_at_ms":now_ms()+15000});presence_value=Some(value.clone());presence_refresh=now_ms();r.broadcast(value).await;}}continue;
                     }
                     if kind=="ping"&&!send(&mut socket,json!({"type":"pong"})).await{break;}
                   },
                   event=messages.recv()=>match event{
                     Ok(v)=>{if v["namespace"]==claims.namespace&&v["partition"]==claims.partition&&channel.as_ref().is_some_and(|p|topic_matches(p,v["topic"].as_str().unwrap_or("")))&&!send(&mut socket,v).await{break;}},
                     Err(tokio::sync::broadcast::error::RecvError::Lagged(n))=>{if !send(&mut socket,json!({"type":"gap","ephemeral":true,"dropped":n})).await{break;}},Err(_)=>break
                   },
                   _=interval.tick()=>{
                     if now_ms().saturating_sub(presence_refresh)>=5000{if let Some(value)=presence_value.as_mut(){value["expires_at_ms"]=json!(now_ms()+15000);r.broadcast(value.clone()).await;presence_refresh=now_ms();}}

                     if r.verify(&raw).await.is_err(){send(&mut socket,json!({"type":"error","error":{"code":"unauthorized","message":"credential expired or revoked"}})).await;break;}
                     if let Some(query)=&live{
                       let changes=r.run(command(&claims.namespace,&claims.partition,"event.read",json!({"after":cursor,"limit":1000}))).await;
                       match changes{
                         Ok(v) if v["latest"].as_u64().unwrap_or(0)>cursor=>{
                           match r.run(command(&claims.namespace,&claims.partition,"document.snapshot",query.clone())).await{
                             Ok(snapshot)=>{
                               if snapshot["total"].as_u64().unwrap_or(0)>1000{send(&mut socket,json!({"type":"error","error":{"code":"overloaded","message":"subscription grew beyond limit"}})).await;break;}
                               let current=snapshot["documents"].as_array().cloned().unwrap_or_default();let next:BTreeMap<String,Value>=current.into_iter().filter_map(|d|d["id"].as_str().map(|id|(id.to_string(),d.clone()))).collect();
                               let added=next.iter().filter(|(id,_)|!rows.contains_key(*id)).map(|(_,v)|v.clone()).collect::<Vec<_>>();let updated=next.iter().filter(|(id,v)|rows.get(*id).is_some_and(|old|old!=*v)).map(|(_,v)|v.clone()).collect::<Vec<_>>();let removed=rows.keys().filter(|id|!next.contains_key(*id)).cloned().collect::<Vec<_>>();cursor=snapshot["sequence"].as_u64().unwrap_or(cursor);rows=next;
                               if (!added.is_empty()||!updated.is_empty()||!removed.is_empty()) && !send(&mut socket,json!({"type":"changes","added":added,"updated":updated,"removed":removed,"sequence":cursor})).await{break;}
                             },Err(e)=>{send(&mut socket,json!({"type":"error","error":e})).await;break;}
                           }
                         },Err(e)=>{send(&mut socket,json!({"type":"error","error":e,"resnapshot_required":true})).await;break;},_=>{}
                       }
                     }
                   }
                 }
    }
    if let Some(value) = {
        let mut presence = r.presence.write().await;
        presence.remove(&session_id)
    } {
        r.broadcast(json!({"type":"presence_leave","namespace":value["namespace"],"partition":value["partition"],"topic":value["topic"],"session_id":session_id})).await;
    }
}

async fn service_token(
    State(r): State<Runtime>,
    headers: HeaderMap,
    Json(c): Json<Capability>,
) -> Response {
    if !admin(&r, &headers) {
        return failure(Error::new("unauthorized", "admin credential required"));
    }
    if c.id.is_empty()
        || c.id.len() > 256
        || c.namespace.is_empty()
        || c.namespace.len() > 256
        || c.partition.is_empty()
        || c.partition.len() > 256
        || c.expires_at_ms <= now_ms()
        || c.expires_at_ms.saturating_sub(now_ms()) > 86400000
        || c.resources.is_empty()
        || c.resources.len() > 100
        || c.resources.iter().any(|s| s.is_empty() || s.len() > 512)
        || c.permissions.is_empty()
        || c.permissions
            .iter()
            .any(|s| !matches!(s.as_str(), "read" | "write"))
        || !c.filter.is_null()
    {
        return failure(Error::new("invalid_argument","service credentials need read/write permissions, exact resources, no filter and a lifetime of at most 24 hours"));
    }
    match auth::sign(&r.signing, &c) {
        Ok(token) => Json(json!({"token":format!("svc.{token}"),"expires_at_ms":c.expires_at_ms}))
            .into_response(),
        Err(e) => failure(e),
    }
}
async fn authorize_service(r: &Runtime, c: &Capability, cmd: &Command) -> crate::model::Result<()> {
    let operations = if cmd.action == "transaction" {
        cmd.payload["commands"]
            .as_array()
            .cloned()
            .ok_or_else(|| Error::new("invalid_argument", "commands required"))?
    } else {
        vec![json!({"action":cmd.action,"payload":cmd.payload})]
    };
    for operation in operations {
        let action = operation["action"].as_str().unwrap_or("");
        let p = &operation["payload"];
        let permission = if crate::store::is_mutation(action) {
            "write"
        } else {
            "read"
        };
        if !c.permissions.iter().any(|v| v == permission) {
            return Err(Error::new("forbidden", "operation permission denied"));
        }
        let mut resources = Vec::new();
        if action == "document.commit" {
            resources.push(format!(
                "collection:{}",
                p["document"]["collection"].as_str().unwrap_or("")
            ));
            resources.push(format!(
                "queue:{}",
                p["job"]["queue"].as_str().unwrap_or("")
            ));
        } else if action.starts_with("document.")
            || action.starts_with("index.")
            || action.starts_with("vector.")
            || action.starts_with("search.")
        {
            if matches!(action, "vector.flush" | "vector.apply") {
                return Err(Error::new(
                    "forbidden",
                    "maintenance requires administrator",
                ));
            }
            resources.push(format!(
                "collection:{}",
                p["collection"].as_str().unwrap_or("")
            ));
        } else if action.starts_with("cache.") {
            resources.push(format!("cache:{}", p["key"].as_str().unwrap_or("")));
        } else if action.starts_with("job.") {
            let queue = if let Some(q) = p["queue"].as_str() {
                q.to_string()
            } else if let Some(id) = p["id"].as_str() {
                r.run(command(
                    &cmd.namespace,
                    &cmd.partition,
                    "job.get",
                    json!({"id":id}),
                ))
                .await?["queue"]
                    .as_str()
                    .unwrap_or("")
                    .to_string()
            } else {
                return Err(Error::new(
                    "forbidden",
                    "queue required for scoped credential",
                ));
            };
            resources.push(format!("queue:{queue}"));
        } else if action == "event.read" || action == "consumer.read" || action == "consumer.ack" {
            resources.push("events".into());
        } else {
            return Err(Error::new("forbidden", "operation requires administrator"));
        }
        if resources.iter().any(|resource| {
            !c.resources.contains(resource) && !c.resources.iter().any(|v| v == "*")
        }) {
            return Err(Error::new("forbidden", "resource permission denied"));
        }
    }
    Ok(())
}

async fn scatter(r: &Runtime, cmd: &Command) -> crate::model::Result<Value> {
    let p = &cmd.payload;
    let partitions = p["partitions"]
        .as_array()
        .filter(|v| !v.is_empty() && v.len() <= 64)
        .ok_or_else(|| Error::new("invalid_argument", "partitions must contain 1..64 names"))?;
    let mut names = std::collections::BTreeSet::new();
    for part in partitions {
        let name = part
            .as_str()
            .filter(|s| !s.is_empty() && s.len() <= 256)
            .ok_or_else(|| Error::new("invalid_argument", "partition name invalid"))?;
        names.insert(name.to_string());
    }
    let query = p["query"]
        .as_object()
        .ok_or_else(|| Error::new("invalid_argument", "query required"))?;
    if !query.contains_key("vector") || query.contains_key("text") {
        return Err(Error::new(
            "unsupported",
            "scatter currently merges vector scores; lexical and hybrid scoring is partition-local",
        ));
    }
    let limit = query.get("limit").and_then(Value::as_u64).unwrap_or(10);
    if limit == 0 || limit > 1000 {
        return Err(Error::new("invalid_argument", "limit must be 1..1000"));
    }
    let query = Value::Object(query.clone());
    let namespace = cmd.namespace.clone();
    let mut tasks = futures_util::stream::iter(names.into_iter().map(|partition| {
        let r = r.clone();
        let query = query.clone();
        let namespace = namespace.clone();
        async move {
            let schema = r
                .run(command(
                    &namespace,
                    &partition,
                    "vector.describe",
                    json!({"collection":query["collection"],"name":query["name"]}),
                ))
                .await?;
            let result = r
                .run(command(&namespace, &partition, "search.query", query))
                .await?;
            Ok::<_, Error>((partition, schema, result))
        }
    }))
    .buffer_unordered(8);
    let mut schema = None;
    let mut hits = Vec::new();
    let mut modes = std::collections::BTreeMap::new();
    while let Some(result) = tasks.next().await {
        let (partition, current, result) = result?;
        if schema.as_ref().is_some_and(|s| s != &current) {
            return Err(Error::new(
                "conflict",
                "vector schemas differ across partitions",
            ));
        }
        schema = Some(current);
        modes.insert(partition.clone(), result["mode"].clone());
        for mut hit in result["hits"].as_array().cloned().unwrap_or_default() {
            hit["partition"] = json!(partition);
            hits.push(hit);
        }
    }
    hits.sort_by(|a, b| {
        b["score"]
            .as_f64()
            .unwrap_or(f64::NEG_INFINITY)
            .total_cmp(&a["score"].as_f64().unwrap_or(f64::NEG_INFINITY))
            .then_with(|| a["partition"].as_str().cmp(&b["partition"].as_str()))
            .then_with(|| a["id"].as_str().cmp(&b["id"].as_str()))
    });
    hits.truncate(limit as usize);
    Ok(json!({"hits":hits,"partition_modes":modes,"consistent_snapshot":false}))
}
