use crate::{
    auth,
    cluster::Cluster,
    model::{Command, Error, Result},
    store::Engine,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{broadcast, RwLock, Semaphore};

#[derive(Clone)]
pub enum Backend {
    Standalone(Arc<Engine>),
    Cluster(Cluster),
}

#[derive(Clone)]
pub struct Runtime {
    pub backend: Backend,
    pub admin: Arc<String>,
    pub signing: Arc<Vec<u8>>,
    pub channels: broadcast::Sender<Value>,
    pub revoked: Arc<RwLock<BTreeSet<String>>>,
    pub presence: Arc<RwLock<BTreeMap<String, Value>>>,
    pub operations: Arc<AtomicU64>,
    pub failures: Arc<AtomicU64>,
    pub permits: Arc<Semaphore>,
    pub sessions: Arc<Semaphore>,
    relay_permits: Arc<Semaphore>,
    relay_client: reqwest::Client,
}
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
pub fn command(ns: &str, partition: &str, action: &str, payload: Value) -> Command {
    Command {
        namespace: ns.into(),
        partition: partition.into(),
        action: action.into(),
        payload,
        request_id: uuid::Uuid::new_v4().to_string(),
        now_ms: now_ms(),
    }
}
impl Runtime {
    pub fn new(engine: Engine, admin: String, signing: Vec<u8>) -> Self {
        Self::with_backend(Backend::Standalone(Arc::new(engine)), admin, signing)
    }
    pub fn new_cluster(cluster: Cluster, admin: String, signing: Vec<u8>) -> Self {
        Self::with_backend(Backend::Cluster(cluster), admin, signing)
    }
    fn with_backend(backend: Backend, admin: String, signing: Vec<u8>) -> Self {
        let (channels, _) = broadcast::channel(1024);
        Self {
            backend,
            admin: Arc::new(admin),
            signing: Arc::new(signing),
            channels,
            revoked: Default::default(),
            presence: Default::default(),
            operations: Default::default(),
            failures: Default::default(),
            permits: Arc::new(Semaphore::new(128)),
            sessions: Arc::new(Semaphore::new(1024)),
            relay_permits: Arc::new(Semaphore::new(32)),
            relay_client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("valid relay client"),
        }
    }
    pub fn mode(&self) -> &'static str {
        match self.backend {
            Backend::Standalone(_) => "standalone",
            Backend::Cluster(_) => "cluster",
        }
    }
    pub async fn cluster_metrics(&self) -> Option<Value> {
        match &self.backend {
            Backend::Standalone(_) => None,
            Backend::Cluster(cluster) => Some(cluster.metrics().await),
        }
    }
    async fn scopes(&self) -> Result<Vec<(String, String)>> {
        match &self.backend {
            Backend::Standalone(engine) => {
                let engine = engine.clone();
                tokio::task::spawn_blocking(move || engine.scopes())
                    .await
                    .map_err(|e| Error::new("internal", e.to_string()))?
            }
            Backend::Cluster(cluster) => cluster.leader_scopes().await,
        }
    }
    pub async fn run(&self, mut cmd: Command) -> Result<Value> {
        cmd.now_ms = now_ms();
        if cmd.request_id.is_empty() {
            cmd.request_id = uuid::Uuid::new_v4().to_string();
        }
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::new("overloaded", "runtime concurrency limit reached"))?;
        self.operations.fetch_add(1, Ordering::Relaxed);
        let result = match &self.backend {
            Backend::Standalone(engine) => {
                let engine = engine.clone();
                tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    engine.execute(&cmd)
                })
                .await
                .map_err(|e| Error::new("internal", e.to_string()))?
            }
            Backend::Cluster(cluster) => {
                let _permit = permit;
                cluster.execute(cmd).await
            }
        };
        if result.is_err() {
            self.failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
    pub async fn verify(&self, token: &str) -> Result<auth::Capability> {
        let c = auth::verify(&self.signing, token, now_ms())?;
        if self.revoked.read().await.contains(&c.id) {
            return Err(Error::new("unauthorized", "capability revoked"));
        }
        let rev = self
            .run(command(
                &c.namespace,
                &c.partition,
                "auth.is_revoked",
                json!({"id":c.id}),
            ))
            .await?;
        if rev == true {
            return Err(Error::new("unauthorized", "capability revoked"));
        }
        Ok(c)
    }
    pub async fn broadcast(&self, value: Value) {
        self.receive_ephemeral(value.clone()).await;
        if let Backend::Cluster(cluster) = &self.backend {
            for origin in cluster.peer_addresses() {
                let Ok(permit) = self.relay_permits.clone().try_acquire_owned() else {
                    tracing::warn!("ephemeral relay overloaded; message dropped");
                    continue;
                };
                let client = self.relay_client.clone();
                let token = self.admin.clone();
                let value = value.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(error) = client
                        .post(format!("{origin}/internal/realtime"))
                        .bearer_auth(&*token)
                        .json(&value)
                        .send()
                        .await
                        .and_then(reqwest::Response::error_for_status)
                    {
                        tracing::debug!(%error,"ephemeral relay dropped");
                    }
                });
            }
        }
    }
    pub async fn receive_ephemeral(&self, value: Value) {
        if value["type"] == "presence_update" {
            let Some(id) = value["session_id"].as_str() else {
                return;
            };
            let mut presence = self.presence.write().await;
            presence.insert(id.to_string(), value.clone());
            let members = presence
                .values()
                .filter(|v| {
                    v["namespace"] == value["namespace"]
                        && v["partition"] == value["partition"]
                        && v["topic"] == value["topic"]
                })
                .cloned()
                .collect::<Vec<_>>();
            drop(presence);
            let _=self.channels.send(json!({"type":"presence","namespace":value["namespace"],"partition":value["partition"],"topic":value["topic"],"members":members}));
        } else {
            if value["type"] == "presence_leave" {
                if let Some(id) = value["session_id"].as_str() {
                    self.presence.write().await.remove(id);
                }
            }
            let _ = self.channels.send(value);
        }
    }
    async fn expire_presence(&self) {
        let expired = {
            let mut presence = self.presence.write().await;
            let expired = presence
                .values()
                .filter(|v| v["expires_at_ms"].as_u64().unwrap_or(0) <= now_ms())
                .cloned()
                .collect::<Vec<_>>();
            for value in &expired {
                if let Some(id) = value["session_id"].as_str() {
                    presence.remove(id);
                }
            }
            expired
        };
        for v in expired {
            let _=self.channels.send(json!({"type":"presence_leave","namespace":v["namespace"],"partition":v["partition"],"topic":v["topic"],"session_id":v["session_id"]}));
        }
    }
    pub fn background(&self) -> tokio::task::JoinHandle<()> {
        let runtime = self.clone();
        tokio::spawn(async move {
            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(20))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("valid static HTTP client configuration");
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut dispatches = tokio::task::JoinSet::new();
            let mut tick = 0u64;
            loop {
                interval.tick().await;
                tick = tick.wrapping_add(1);
                runtime.expire_presence().await;
                while let Some(result) = dispatches.try_join_next() {
                    if let Err(error) = result {
                        if !error.is_cancelled() {
                            tracing::warn!(%error,"HTTP dispatch task failed");
                        }
                    }
                }
                let scopes = match runtime.scopes().await {
                    Ok(scopes) => scopes,
                    Err(error) => {
                        tracing::warn!(code=%error.code,"scope discovery failed");
                        continue;
                    }
                };
                for (ns, partition) in scopes {
                    if tick % 20 == 0 {
                        if let Err(e) = runtime
                            .run(command(&ns, &partition, "scope.maintain", json!({})))
                            .await
                        {
                            tracing::warn!(code=%e.code,"retention maintenance failed");
                        }
                    }

                    let plan = match runtime
                        .run(command(&ns, &partition, "scope.work", json!({})))
                        .await
                    {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    for (flag, action) in [
                        ("index_pending", "vector.flush"),
                        ("schedules_due", "schedule.tick"),
                    ] {
                        if plan[flag] == true {
                            if let Err(e) = runtime
                                .run(command(&ns, &partition, action, json!({})))
                                .await
                            {
                                tracing::warn!(code=%e.code,action,"background operation failed");
                            }
                        }
                    }
                    let queues = plan["http_queues"].as_array().cloned().unwrap_or_default();
                    for queue in queues.iter().filter_map(Value::as_str) {
                        while let Some(result) = dispatches.try_join_next() {
                            if let Err(error) = result {
                                if !error.is_cancelled() {
                                    tracing::warn!(%error,"HTTP dispatch task failed");
                                }
                            }
                        }
                        if dispatches.len() >= 32 {
                            continue;
                        }
                        let claimed=runtime.run(command(&ns,&partition,"job.claim",json!({"queue":queue,"worker":"http-dispatcher","delivery_mode":"http","lease_ms":30000}))).await;
                        let Ok(claimed) = claimed else {
                            continue;
                        };
                        let j = &claimed["job"];
                        if j.is_null() {
                            continue;
                        }
                        let (Some(url), Some(id), Some(receipt)) = (
                            j["delivery"]["url"].as_str(),
                            j["id"].as_str(),
                            claimed["receipt"].as_str(),
                        ) else {
                            continue;
                        };
                        let body = match serde_json::to_vec(&j["payload"]) {
                            Ok(b) => b,
                            Err(_) => continue,
                        };
                        let timestamp = now_ms();
                        let signature = auth::signature(&runtime.signing, timestamp, id, &body);
                        let runtime = runtime.clone();
                        let client = client.clone();
                        let ns = ns.clone();
                        let partition = partition.clone();
                        let url = url.to_string();
                        let id = id.to_string();
                        let receipt = receipt.to_string();
                        dispatches.spawn(async move{
             let response=client.post(url).header("Content-Type","application/json").header("Substrate-Job-Id",&id).header("Substrate-Timestamp",timestamp).header("Substrate-Signature",signature).body(body).send().await;
             let success=response.as_ref().is_ok_and(|r|r.status().is_success()&&r.status().as_u16()!=202);
             let action=if success{"job.ack"}else{"job.nack"};
             if let Err(e)=runtime.run(command(&ns,&partition,action,json!({"id":id,"receipt":receipt}))).await{tracing::warn!(code=%e.code,job_id=%id,"HTTP job acknowledgement failed");}
           });
                    }
                }
            }
        })
    }
}
