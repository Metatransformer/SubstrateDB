use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Request, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Config, Raft, SnapshotPolicy};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::cluster_storage::{PersistentLog, PersistentStateMachine, RaftCommand, TypeConfig};
use crate::model::{Command, Error, Result};
use crate::store::{is_mutation, Engine};

const SECRET_HEADER: &str = "x-substrate-raft-secret";
const MAX_RPC_BYTES: usize = 8 * 1024 * 1024;
const MAX_COMMAND_BYTES: usize = 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);
const FORWARD_ATTEMPT_TIMEOUT: Duration = Duration::from_millis(500);
const MEMBERSHIP_TIMEOUT: Duration = Duration::from_secs(30);
const EXECUTE_ATTEMPTS: usize = 4;
const BOOTSTRAP_ATTEMPTS: usize = 100;

#[derive(Clone)]
pub struct Cluster {
    inner: Arc<ClusterInner>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub node_id: u64,
    pub advertise: String,
    pub peers: BTreeMap<u64, String>,
    pub data_dir: PathBuf,
    pub shards: u64,
    pub secret: String,
    pub bootstrap: bool,
}

struct ClusterInner {
    node_id: u64,
    advertise: String,
    members: BTreeMap<u64, BasicNode>,
    secret: String,
    client: reqwest::Client,
    shards: Vec<Shard>,
}

struct Shard {
    id: u64,
    raft: Raft<TypeConfig>,
    engine: Arc<Engine>,
    initial_voters: BTreeSet<u64>,
    reconfiguration: tokio::sync::Mutex<()>,
}

#[derive(Clone)]
struct NetworkFactory {
    client: reqwest::Client,
    secret: String,
    shard: u64,
}

struct HttpNetwork {
    client: reqwest::Client,
    secret: String,
    shard: u64,
    node: BasicNode,
}

#[derive(Debug, Serialize, Deserialize)]
struct RpcEnvelope<T> {
    response: Option<T>,
    error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CommandRequest {
    command: Command,
}

#[derive(Debug, Serialize, Deserialize)]
struct CommandReply {
    result: std::result::Result<Value, Error>,
    leader_id: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct InitializeRequest {
    voters: BTreeSet<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct MembershipRequest {
    voters: BTreeSet<u64>,
}

impl Cluster {
    pub async fn new(config: ClusterConfig) -> Result<Self> {
        let voter_ids = config
            .peers
            .keys()
            .copied()
            .chain(std::iter::once(config.node_id))
            .collect::<BTreeSet<_>>();
        let placements = (0..config.shards)
            .map(|shard| (shard, voter_ids.clone()))
            .collect();
        Self::build(config, placements, false).await
    }

    pub async fn new_with_placement(
        config: ClusterConfig,
        placements: BTreeMap<u64, BTreeSet<u64>>,
    ) -> Result<Self> {
        Self::build(config, placements, true).await
    }

    async fn build(
        config: ClusterConfig,
        placements: BTreeMap<u64, BTreeSet<u64>>,
        explicit_placement: bool,
    ) -> Result<Self> {
        validate_config(&config)?;
        if explicit_placement {
            validate_placements(&config, &placements)?;
        }
        std::fs::create_dir_all(&config.data_dir)
            .map_err(|error| Error::new("storage_error", error.to_string()))?;

        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| Error::new("internal", error.to_string()))?;
        let mut members = config
            .peers
            .iter()
            .map(|(id, address)| (*id, BasicNode::new(normalize_address(address))))
            .collect::<BTreeMap<_, _>>();
        members.insert(
            config.node_id,
            BasicNode::new(normalize_address(&config.advertise)),
        );

        let mut shards = Vec::with_capacity(config.shards as usize);
        for shard_id in 0..config.shards {
            let initial_voters = placements.get(&shard_id).cloned().ok_or_else(|| {
                Error::new(
                    "invalid_argument",
                    format!("placement missing shard {shard_id}"),
                )
            })?;
            let directory = config.data_dir.join(format!("shard-{shard_id}"));
            std::fs::create_dir_all(&directory)
                .map_err(|error| Error::new("storage_error", error.to_string()))?;
            let engine = Arc::new(Engine::open(directory.join("engine.redb"))?);
            let log = PersistentLog::open(&directory)?;
            let state_machine = PersistentStateMachine::open(&directory, engine.clone())?;
            let network = NetworkFactory {
                client: client.clone(),
                secret: config.secret.clone(),
                shard: shard_id,
            };
            let raft_config = Config {
                cluster_name: format!("substratedb-shard-{shard_id}"),
                // The library's 50 ms default also bounds append RPCs. Allow
                // maximum-sized durable batches to finish rather than retry forever.
                heartbeat_interval: 500,
                election_timeout_min: 1500,
                election_timeout_max: 3000,
                // Four maximum-size commands stay below the 8 MiB RPC body bound.
                max_payload_entries: 4,
                snapshot_policy: SnapshotPolicy::LogsSinceLast(1_000),
                snapshot_max_chunk_size: 1024 * 1024,
                replication_lag_threshold: 2_000,
                ..Config::default()
            };
            let raft_config = Arc::new(
                raft_config
                    .validate()
                    .map_err(|error| Error::new("invalid_argument", error.to_string()))?,
            );
            let raft = Raft::new(config.node_id, raft_config, network, log, state_machine)
                .await
                .map_err(|error| Error::new("storage_error", error.to_string()))?;
            shards.push(Shard {
                id: shard_id,
                raft,
                engine,
                initial_voters,
                reconfiguration: tokio::sync::Mutex::new(()),
            });
        }

        let cluster = Self {
            inner: Arc::new(ClusterInner {
                node_id: config.node_id,
                advertise: normalize_address(&config.advertise),
                members: members.clone(),
                secret: config.secret,
                client,
                shards,
            }),
        };

        if config.bootstrap {
            for shard in &cluster.inner.shards {
                let cluster = cluster.clone();
                let shard_id = shard.id;
                tokio::spawn(async move {
                    cluster.bootstrap_shard(shard_id).await;
                });
            }
        }

        Ok(cluster)
    }

    pub async fn execute(&self, mut command: Command) -> Result<Value> {
        let encoded = serde_json::to_vec(&command)
            .map_err(|error| Error::new("invalid_argument", error.to_string()))?;
        if encoded.len() > MAX_COMMAND_BYTES {
            return Err(Error::new(
                "invalid_argument",
                "command exceeds the 1 MiB limit",
            ));
        }
        let shard_id = shard_for_scope(
            &command.namespace,
            &command.partition,
            self.inner.shards.len() as u64,
        );
        if is_mutation(&command.action) {
            if command.request_id.is_empty() {
                command.request_id = uuid::Uuid::new_v4().to_string();
            }
            if command.now_ms == 0 {
                command.now_ms = system_now_ms()?;
            }
        }
        tokio::time::timeout(REQUEST_TIMEOUT, self.execute_with_retry(shard_id, command))
            .await
            .map_err(|_| Error::new("not_ready", "request deadline expired"))?
    }

    async fn execute_with_retry(&self, shard_id: u64, command: Command) -> Result<Value> {
        let shard = &self.inner.shards[shard_id as usize];
        let mut preferred = shard.raft.current_leader().await;
        let mut candidates = shard.initial_voters.iter().copied().collect::<Vec<_>>();
        for node_id in self.inner.members.keys().copied() {
            if !candidates.contains(&node_id) {
                candidates.push(node_id);
            }
        }
        let max_attempts = EXECUTE_ATTEMPTS.max(candidates.len().saturating_mul(2));
        let mut candidate_index = 0;
        let mut last_error = Error::new("not_ready", "shard has no elected leader");

        for _ in 0..max_attempts {
            let target = preferred.take().unwrap_or_else(|| {
                let target = candidates[candidate_index % candidates.len()];
                candidate_index += 1;
                target
            });
            if target == self.inner.node_id {
                match self.execute_local(shard_id, command.clone()).await {
                    Ok(value) => return Ok(value),
                    Err(error) => {
                        if !retryable_cluster_error(&error) {
                            return Err(error);
                        }
                        last_error = error;
                        preferred = shard.raft.current_leader().await;
                    }
                }
            } else {
                match tokio::time::timeout(
                    FORWARD_ATTEMPT_TIMEOUT,
                    self.forward_command(target, shard_id, &command),
                )
                .await
                {
                    Ok(Ok(reply)) => match reply.result {
                        Ok(value) => return Ok(value),
                        Err(error) => {
                            if !retryable_cluster_error(&error) {
                                return Err(error);
                            }
                            last_error = error;
                            if reply.leader_id != Some(target) {
                                preferred = reply.leader_id;
                            }
                        }
                    },
                    Ok(Err(error)) => {
                        last_error = error;
                        preferred = shard.raft.current_leader().await;
                    }
                    Err(_) => {
                        last_error = Error::new("not_ready", "forwarding attempt timed out");
                        preferred = shard.raft.current_leader().await;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(75)).await;
        }
        Err(last_error)
    }

    pub fn peer_addresses(&self) -> Vec<String> {
        self.inner
            .members
            .iter()
            .filter(|(node_id, _)| **node_id != self.inner.node_id)
            .map(|(_, node)| node.addr.clone())
            .collect()
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/internal/raft/{shard}/append", post(append_entries))
            .route("/internal/raft/{shard}/vote", post(vote))
            .route("/internal/raft/{shard}/snapshot", post(install_snapshot))
            .route("/internal/raft/{shard}/command", post(forwarded_command))
            .route("/internal/raft/{shard}/initialize", post(initialize_shard))
            .route(
                "/internal/raft/{shard}/membership",
                post(change_shard_membership),
            )
            .route(
                "/internal/raft/{shard}/snapshot/trigger",
                post(trigger_snapshot),
            )
            .route_layer(middleware::from_fn_with_state(self.clone(), authenticate))
            .layer(DefaultBodyLimit::max(MAX_RPC_BYTES))
            .with_state(self.clone())
    }

    pub async fn metrics(&self) -> Value {
        let mut shards = Vec::with_capacity(self.inner.shards.len());
        for shard in &self.inner.shards {
            let metrics = shard.raft.metrics().borrow().clone();
            let voters = metrics.membership_config.voter_ids().collect::<Vec<_>>();
            let learners = metrics
                .membership_config
                .membership()
                .learner_ids()
                .collect::<Vec<_>>();
            shards.push(json!({
                "shard": shard.id,
                "node_id": metrics.id,
                "state": format!("{:?}", metrics.state),
                "term": metrics.current_term,
                "leader_id": metrics.current_leader,
                "last_log_index": metrics.last_log_index,
                "last_applied": metrics.last_applied,
                "snapshot": metrics.snapshot,
                "members": voters,
                "voters": voters,
                "learners": learners,
                "running": metrics.running_state.is_ok(),
            }));
        }
        json!({
            "node_id": self.inner.node_id,
            "advertise": self.inner.advertise,
            "shard_count": self.inner.shards.len(),
            "shards": shards,
        })
    }

    pub async fn leader_scopes(&self) -> Result<Vec<(String, String)>> {
        let mut scopes = Vec::new();
        for shard in &self.inner.shards {
            let metrics = shard.raft.metrics().borrow().clone();
            if metrics.current_leader != Some(self.inner.node_id) {
                continue;
            }
            let engine = shard.engine.clone();
            let mut shard_scopes = tokio::task::spawn_blocking(move || engine.scopes())
                .await
                .map_err(|error| Error::new("internal", error.to_string()))??;
            scopes.append(&mut shard_scopes);
        }
        Ok(scopes)
    }

    pub async fn shutdown(&self) {
        for shard in &self.inner.shards {
            let _ = tokio::time::timeout(REQUEST_TIMEOUT, shard.raft.shutdown()).await;
        }
    }

    async fn execute_local(&self, shard_id: u64, mut command: Command) -> Result<Value> {
        let shard = self
            .inner
            .shards
            .get(shard_id as usize)
            .ok_or_else(|| Error::new("invalid_argument", "unknown shard"))?;
        if shard.raft.current_leader().await != Some(self.inner.node_id) {
            return Err(Error::new(
                "not_leader",
                "request must be sent to the shard leader",
            ));
        }

        if is_mutation(&command.action) {
            if command.request_id.is_empty() {
                command.request_id = uuid::Uuid::new_v4().to_string();
            }
            if command.now_ms == 0 {
                command.now_ms = system_now_ms()?;
            }
            let write = tokio::time::timeout(
                REQUEST_TIMEOUT,
                shard.raft.client_write(RaftCommand { command }),
            )
            .await
            .map_err(|_| {
                Error::new(
                    "quorum_unavailable",
                    "write quorum was not reached before the deadline",
                )
            })?
            .map_err(|error| {
                if error.forward_to_leader::<BasicNode>().is_some() {
                    Error::new(
                        "not_leader",
                        "leadership changed while executing the command",
                    )
                } else {
                    Error::new("cluster_error", error.to_string())
                }
            })?;
            write.data.result
        } else {
            tokio::time::timeout(REQUEST_TIMEOUT, shard.raft.ensure_linearizable())
                .await
                .map_err(|_| {
                    Error::new(
                        "quorum_unavailable",
                        "read quorum was not reached before the deadline",
                    )
                })?
                .map_err(|error| {
                    if error.forward_to_leader::<BasicNode>().is_some() {
                        Error::new("not_leader", "leadership changed while executing the read")
                    } else {
                        Error::new("cluster_error", error.to_string())
                    }
                })?;
            let engine = shard.engine.clone();
            tokio::task::spawn_blocking(move || engine.execute(&command))
                .await
                .map_err(|error| Error::new("internal", error.to_string()))?
        }
    }

    async fn forward_command(
        &self,
        target: u64,
        shard: u64,
        command: &Command,
    ) -> Result<CommandReply> {
        let node = self
            .inner
            .members
            .get(&target)
            .ok_or_else(|| Error::new("not_ready", "leader address is not configured"))?;
        let url = format!(
            "{}/internal/raft/{shard}/command",
            node.addr.trim_end_matches('/')
        );
        let body = serde_json::to_vec(&CommandRequest {
            command: command.clone(),
        })
        .map_err(|error| Error::new("invalid_argument", error.to_string()))?;
        let response = self
            .inner
            .client
            .post(url)
            .header(SECRET_HEADER, &self.inner.secret)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|error| Error::new("not_ready", error.to_string()))?;
        if response.status() != StatusCode::OK {
            return Err(Error::new(
                "not_ready",
                format!("leader returned HTTP {}", response.status()),
            ));
        }
        let bytes = bounded_response(response).await?;
        serde_json::from_slice(&bytes)
            .map_err(|error| Error::new("cluster_error", error.to_string()))
    }

    async fn bootstrap_shard(&self, shard_id: u64) {
        let Some(shard) = self.inner.shards.get(shard_id as usize) else {
            return;
        };
        let Some(target) = shard.initial_voters.iter().next().copied() else {
            return;
        };
        let Some(node) = self.inner.members.get(&target) else {
            return;
        };
        let url = format!(
            "{}/internal/raft/{shard_id}/initialize",
            node.addr.trim_end_matches('/')
        );
        let request = InitializeRequest {
            voters: shard.initial_voters.clone(),
        };
        for _ in 0..BOOTSTRAP_ATTEMPTS {
            let result = self
                .inner
                .client
                .post(&url)
                .header(SECRET_HEADER, &self.inner.secret)
                .json(&request)
                .send()
                .await;
            if result.is_ok_and(|response| response.status() == StatusCode::OK) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        tracing::warn!(
            shard = shard_id,
            target,
            "cluster bootstrap attempts exhausted"
        );
    }
}

pub fn shard_for_scope(namespace: &str, partition: &str, shards: u64) -> u64 {
    assert!(shards > 0, "shard count must be non-zero");
    let mut digest = Sha256::new();
    digest.update((namespace.len() as u64).to_be_bytes());
    digest.update(namespace.as_bytes());
    digest.update((partition.len() as u64).to_be_bytes());
    digest.update(partition.as_bytes());
    let hash = digest.finalize();
    let prefix = u64::from_be_bytes(hash[..8].try_into().expect("SHA-256 prefix is eight bytes"));
    prefix % shards
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = HttpNetwork;

    async fn new_client(&mut self, _target: u64, node: &BasicNode) -> Self::Network {
        HttpNetwork {
            client: self.client.clone(),
            secret: self.secret.clone(),
            shard: self.shard,
            node: node.clone(),
        }
    }
}

impl RaftNetwork<TypeConfig> for HttpNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>>
    {
        self.post("append", &rpc).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        self.post("snapshot", &rpc).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> std::result::Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.post("vote", &rpc).await
    }
}

impl HttpNetwork {
    async fn post<Req, Resp, E>(
        &self,
        operation: &str,
        request: &Req,
    ) -> std::result::Result<Resp, RPCError<u64, BasicNode, E>>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
        E: std::error::Error,
    {
        let body = serde_json::to_vec(request)
            .map_err(|error| RPCError::Unreachable(Unreachable::new(&error)))?;
        if body.len() > MAX_RPC_BYTES {
            return Err(openraft::error::PayloadTooLarge::new_entries_hint(1).into());
        }
        let url = format!(
            "{}/internal/raft/{}/{}",
            self.node.addr.trim_end_matches('/'),
            self.shard,
            operation
        );
        let response = self
            .client
            .post(url)
            .header(SECRET_HEADER, &self.secret)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|error| RPCError::Unreachable(Unreachable::new(&error)))?;
        if response.status() != StatusCode::OK {
            let error = IoRpcError(format!("raft peer returned HTTP {}", response.status()));
            return Err(RPCError::Unreachable(Unreachable::new(&error)));
        }
        let bytes = bounded_rpc_response(response)
            .await
            .map_err(|error| RPCError::Unreachable(Unreachable::new(&error)))?;
        let envelope: RpcEnvelope<Resp> = serde_json::from_slice(&bytes)
            .map_err(|error| RPCError::Unreachable(Unreachable::new(&error)))?;
        match (envelope.response, envelope.error) {
            (Some(response), None) => Ok(response),
            (_, Some(message)) => {
                let error = IoRpcError(message);
                Err(RPCError::Unreachable(Unreachable::new(&error)))
            }
            _ => {
                let error = IoRpcError("invalid raft RPC envelope".to_string());
                Err(RPCError::Unreachable(Unreachable::new(&error)))
            }
        }
    }
}

#[derive(Debug)]
struct IoRpcError(String);

impl std::fmt::Display for IoRpcError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for IoRpcError {}

async fn authenticate(State(cluster): State<Cluster>, request: Request, next: Next) -> Response {
    let authorized = request
        .headers()
        .get(SECRET_HEADER)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|candidate| secret_matches(candidate, &cluster.inner.secret));
    if !authorized {
        return error_response(
            StatusCode::UNAUTHORIZED,
            Error::new("unauthorized", "valid internal cluster credential required"),
        );
    }
    next.run(request).await
}

async fn append_entries(
    Path(shard): Path<u64>,
    State(cluster): State<Cluster>,
    Json(request): Json<AppendEntriesRequest<TypeConfig>>,
) -> Response {
    let Some(shard) = cluster.inner.shards.get(shard as usize) else {
        return error_response(
            StatusCode::NOT_FOUND,
            Error::new("not_found", "unknown shard"),
        );
    };
    rpc_response(shard.raft.append_entries(request).await)
}

async fn vote(
    Path(shard): Path<u64>,
    State(cluster): State<Cluster>,
    Json(request): Json<VoteRequest<u64>>,
) -> Response {
    let Some(shard) = cluster.inner.shards.get(shard as usize) else {
        return error_response(
            StatusCode::NOT_FOUND,
            Error::new("not_found", "unknown shard"),
        );
    };
    rpc_response(shard.raft.vote(request).await)
}

async fn install_snapshot(
    Path(shard): Path<u64>,
    State(cluster): State<Cluster>,
    Json(request): Json<InstallSnapshotRequest<TypeConfig>>,
) -> Response {
    let Some(shard) = cluster.inner.shards.get(shard as usize) else {
        return error_response(
            StatusCode::NOT_FOUND,
            Error::new("not_found", "unknown shard"),
        );
    };
    rpc_response(shard.raft.install_snapshot(request).await)
}

async fn forwarded_command(
    Path(shard): Path<u64>,
    State(cluster): State<Cluster>,
    Json(request): Json<CommandRequest>,
) -> Response {
    if serde_json::to_vec(&request).map_or(true, |bytes| bytes.len() > MAX_COMMAND_BYTES) {
        return error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            Error::new("invalid_argument", "command exceeds the 1 MiB limit"),
        );
    }
    let result = cluster.execute_local(shard, request.command).await;
    let leader_id = cluster
        .inner
        .shards
        .get(shard as usize)
        .and_then(|value| value.raft.metrics().borrow().current_leader);
    json_response(&CommandReply { result, leader_id })
}

async fn initialize_shard(
    Path(shard): Path<u64>,
    State(cluster): State<Cluster>,
    Json(request): Json<InitializeRequest>,
) -> Response {
    let Some(shard) = cluster.inner.shards.get(shard as usize) else {
        return error_response(
            StatusCode::NOT_FOUND,
            Error::new("not_found", "unknown shard"),
        );
    };
    if request.voters != shard.initial_voters {
        return error_response(
            StatusCode::CONFLICT,
            Error::new(
                "conflict",
                "initial voter set differs from this node's placement configuration",
            ),
        );
    }
    if !shard.initial_voters.contains(&cluster.inner.node_id) {
        return error_response(
            StatusCode::CONFLICT,
            Error::new(
                "conflict",
                "shard must be initialized on one of its configured voters",
            ),
        );
    }
    match shard.raft.is_initialized().await {
        Ok(true) => {
            let voters = shard
                .raft
                .metrics()
                .borrow()
                .membership_config
                .voter_ids()
                .collect::<Vec<_>>();
            return json_response(
                &json!({"initialized": false, "existing": true, "voters": voters}),
            );
        }
        Ok(false) => {}
        Err(error) => {
            return error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                Error::new("cluster_error", error.to_string()),
            );
        }
    }
    let members = shard
        .initial_voters
        .iter()
        .filter_map(|node_id| {
            cluster
                .inner
                .members
                .get(node_id)
                .cloned()
                .map(|node| (*node_id, node))
        })
        .collect::<BTreeMap<_, _>>();
    match shard.raft.initialize(members).await {
        Ok(()) => json_response(&json!({"initialized": true})),
        Err(error) => match shard.raft.is_initialized().await {
            Ok(true) => {
                let voters = shard
                    .raft
                    .metrics()
                    .borrow()
                    .membership_config
                    .voter_ids()
                    .collect::<Vec<_>>();
                json_response(&json!({"initialized": false, "existing": true, "voters": voters}))
            }
            _ => error_response(
                StatusCode::CONFLICT,
                Error::new("cluster_error", error.to_string()),
            ),
        },
    }
}

async fn change_shard_membership(
    Path(shard_id): Path<u64>,
    State(cluster): State<Cluster>,
    Json(request): Json<MembershipRequest>,
) -> Response {
    let Some(shard) = cluster.inner.shards.get(shard_id as usize) else {
        return error_response(
            StatusCode::NOT_FOUND,
            Error::new("not_found", "unknown shard"),
        );
    };
    if let Err(error) = validate_voter_set(&request.voters, &cluster.inner.members) {
        return error_response(StatusCode::BAD_REQUEST, error);
    }
    if shard.raft.current_leader().await != Some(cluster.inner.node_id) {
        return error_response(
            StatusCode::CONFLICT,
            Error::new(
                "not_leader",
                "membership changes must be sent to the shard leader",
            ),
        );
    }
    let Ok(_guard) = shard.reconfiguration.try_lock() else {
        return error_response(
            StatusCode::CONFLICT,
            Error::new(
                "conflict",
                "a shard membership change is already in progress",
            ),
        );
    };
    if shard.raft.current_leader().await != Some(cluster.inner.node_id) {
        return error_response(
            StatusCode::CONFLICT,
            Error::new(
                "not_leader",
                "leadership changed before membership reconfiguration",
            ),
        );
    }

    let operation = async {
        let current_voters = shard
            .raft
            .metrics()
            .borrow()
            .membership_config
            .voter_ids()
            .collect::<BTreeSet<_>>();
        for node_id in request.voters.difference(&current_voters) {
            let node = cluster
                .inner
                .members
                .get(node_id)
                .cloned()
                .ok_or_else(|| Error::new("invalid_argument", "unknown configured peer"))?;
            shard
                .raft
                .add_learner(*node_id, node, true)
                .await
                .map_err(membership_error)?;
        }
        shard
            .raft
            .change_membership(request.voters.clone(), false)
            .await
            .map_err(membership_error)?;
        let metrics = shard.raft.metrics().borrow().clone();
        let voters = metrics.membership_config.voter_ids().collect::<Vec<_>>();
        let learners = metrics
            .membership_config
            .membership()
            .learner_ids()
            .collect::<Vec<_>>();
        Ok::<_, Error>(json!({
            "shard": shard_id,
            "leader_id": metrics.current_leader,
            "voters": voters,
            "learners": learners,
        }))
    };

    match tokio::time::timeout(MEMBERSHIP_TIMEOUT, operation).await {
        Ok(Ok(report)) => json_response(&report),
        Ok(Err(error)) => error_response(StatusCode::SERVICE_UNAVAILABLE, error),
        Err(_) => error_response(
            StatusCode::GATEWAY_TIMEOUT,
            Error::new(
                "quorum_unavailable",
                "membership operation timed out; outcome may be committed, inspect current membership",
            ),
        ),
    }
}

async fn trigger_snapshot(Path(shard): Path<u64>, State(cluster): State<Cluster>) -> Response {
    let Some(shard) = cluster.inner.shards.get(shard as usize) else {
        return error_response(
            StatusCode::NOT_FOUND,
            Error::new("not_found", "unknown shard"),
        );
    };
    match shard.raft.trigger().snapshot().await {
        Ok(()) => json_response(&json!({"accepted": true})),
        Err(error) => error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            Error::new("cluster_error", error.to_string()),
        ),
    }
}

fn rpc_response<T: Serialize, E: std::fmt::Display>(result: std::result::Result<T, E>) -> Response {
    match result {
        Ok(response) => json_response(&RpcEnvelope {
            response: Some(response),
            error: None,
        }),
        Err(error) => json_response(&RpcEnvelope::<T> {
            response: None,
            error: Some(error.to_string()),
        }),
    }
}

fn json_response(value: &impl Serialize) -> Response {
    match serde_json::to_vec(value) {
        Ok(bytes) if bytes.len() <= MAX_RPC_BYTES => (
            StatusCode::OK,
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            bytes,
        )
            .into_response(),
        Ok(_) => error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            Error::new("overloaded", "internal response exceeds the 8 MiB limit"),
        ),
        Err(error) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            Error::new("internal", error.to_string()),
        ),
    }
}

fn error_response(status: StatusCode, error: Error) -> Response {
    (status, Json(json!({"error": error}))).into_response()
}

async fn bounded_response(response: reqwest::Response) -> Result<Bytes> {
    bounded_body(response)
        .await
        .map_err(|error| Error::new("cluster_error", error.to_string()))
}

async fn bounded_rpc_response(
    response: reqwest::Response,
) -> std::result::Result<Bytes, IoRpcError> {
    bounded_body(response).await.map_err(IoRpcError)
}

async fn bounded_body(mut response: reqwest::Response) -> std::result::Result<Bytes, String> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RPC_BYTES as u64)
    {
        return Err("raft response exceeds the 8 MiB limit".to_string());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|error| error.to_string())? {
        if bytes.len().saturating_add(chunk.len()) > MAX_RPC_BYTES {
            return Err("raft response exceeds the 8 MiB limit".to_string());
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(Bytes::from(bytes))
}

fn validate_config(config: &ClusterConfig) -> Result<()> {
    if config.shards == 0 || config.shards > 1_024 {
        return Err(Error::new(
            "invalid_argument",
            "shards must be between 1 and 1024",
        ));
    }
    if config.secret.is_empty() || config.secret.len() > 4_096 {
        return Err(Error::new(
            "invalid_argument",
            "cluster secret must be non-empty and at most 4096 bytes",
        ));
    }
    validate_address("advertise", &config.advertise)?;
    for (node_id, address) in &config.peers {
        if *node_id == config.node_id
            && normalize_address(address) != normalize_address(&config.advertise)
        {
            return Err(Error::new(
                "invalid_argument",
                "local peer address differs from advertise",
            ));
        }
        validate_address("peer", address)?;
    }
    Ok(())
}

fn validate_placements(
    config: &ClusterConfig,
    placements: &BTreeMap<u64, BTreeSet<u64>>,
) -> Result<()> {
    if placements.len() != config.shards as usize {
        return Err(Error::new(
            "invalid_argument",
            "placements must contain exactly one voter set for every shard",
        ));
    }
    let mut members = config
        .peers
        .keys()
        .copied()
        .map(|node_id| (node_id, BasicNode::default()))
        .collect::<BTreeMap<_, _>>();
    members.insert(config.node_id, BasicNode::default());
    for shard_id in 0..config.shards {
        let voters = placements.get(&shard_id).ok_or_else(|| {
            Error::new(
                "invalid_argument",
                format!("placement missing shard {shard_id}"),
            )
        })?;
        validate_voter_set(voters, &members)?;
    }
    if let Some(shard_id) = placements
        .keys()
        .find(|shard_id| **shard_id >= config.shards)
    {
        return Err(Error::new(
            "invalid_argument",
            format!("placement references unknown shard {shard_id}"),
        ));
    }
    Ok(())
}

fn validate_voter_set(voters: &BTreeSet<u64>, members: &BTreeMap<u64, BasicNode>) -> Result<()> {
    if voters.is_empty() {
        return Err(Error::new(
            "invalid_argument",
            "voter set must not be empty",
        ));
    }
    if voters.len() == 1 && members.len() != 1 {
        return Err(Error::new(
            "invalid_argument",
            "a single-voter set is only allowed when exactly one cluster node is configured",
        ));
    }
    if voters.len() != 1 && (voters.len() < 3 || voters.len() % 2 == 0) {
        return Err(Error::new(
            "invalid_argument",
            "voter set must contain one node for development or an odd number of at least three nodes",
        ));
    }
    if let Some(node_id) = voters.iter().find(|node_id| !members.contains_key(node_id)) {
        return Err(Error::new(
            "invalid_argument",
            format!("voter {node_id} is not a configured peer"),
        ));
    }
    Ok(())
}

fn retryable_cluster_error(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "not_ready" | "not_leader" | "quorum_unavailable" | "cluster_error"
    )
}

fn membership_error(error: impl std::fmt::Display) -> Error {
    Error::new("cluster_error", error.to_string())
}

fn validate_address(name: &str, address: &str) -> Result<()> {
    let url = reqwest::Url::parse(address).map_err(|_| {
        Error::new(
            "invalid_argument",
            format!("{name} must be an absolute HTTP URL"),
        )
    })?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
        || !matches!(url.path(), "" | "/")
    {
        return Err(Error::new(
            "invalid_argument",
            format!("{name} must be an HTTP(S) origin"),
        ));
    }
    Ok(())
}

fn normalize_address(address: &str) -> String {
    address.trim_end_matches('/').to_string()
}

fn secret_matches(candidate: &str, expected: &str) -> bool {
    let left = Sha256::digest(candidate.as_bytes());
    let right = Sha256::digest(expected.as_bytes());
    left.iter()
        .zip(right.iter())
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

fn system_now_ms() -> Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| Error::new("internal", error.to_string()))?
        .as_millis();
    u64::try_from(millis)
        .map_err(|_| Error::new("internal", "system clock exceeds supported range"))
}
