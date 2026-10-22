use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::time::Duration;

use serde_json::{json, Value};
use substratedb::cluster::{Cluster, ClusterConfig};
use substratedb::model::Command;
use substratedb::runtime::Runtime;
use substratedb::store::Engine;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const ADMIN: &str = "cluster-integration-admin-token";
const DEADLINE: Duration = Duration::from_secs(20);

struct RunningNode {
    address: String,
    cluster: Cluster,
    runtime: Runtime,
    background: JoinHandle<()>,
    server: JoinHandle<std::io::Result<()>>,
    _directory: TempDir,
}

fn raw_command(action: &str, payload: Value, now_ms: u64) -> Command {
    Command {
        namespace: "tenant".to_string(),
        partition: "main".to_string(),
        action: action.to_string(),
        payload,
        request_id: "offline-identical-schedule-seed".to_string(),
        now_ms,
    }
}

fn seed_due_schedule(directory: &TempDir) {
    let shard = directory.path().join("shard-0");
    std::fs::create_dir_all(&shard).unwrap();
    let engine = Engine::open(shard.join("engine.redb")).unwrap();
    engine
        .execute(&raw_command(
            "schedule.put",
            json!({
                "id":"seed",
                "cron":"* * * * *",
                "timezone":"UTC",
                "job":{"queue":"scheduled","payload":{"source":"background"}}
            }),
            0,
        ))
        .unwrap();
}

async fn public_command(node: &RunningNode, action: &str, payload: Value) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{}/v1/command", node.address))
        .bearer_auth(ADMIN)
        .json(&json!({
            "namespace":"tenant",
            "partition":"main",
            "action":action,
            "payload":payload
        }))
        .send()
        .await
        .unwrap()
}

async fn public_result(node: &RunningNode, action: &str, payload: Value) -> Value {
    let response = public_command(node, action, payload).await;
    let status = response.status();
    let body: Value = response.json().await.unwrap();
    assert!(status.is_success(), "HTTP {status}: {body}");
    body["result"].clone()
}

async fn wait_for_leader(nodes: &[Option<RunningNode>]) -> u64 {
    tokio::time::timeout(DEADLINE, async {
        loop {
            for node in nodes.iter().flatten() {
                if let Some(leader) =
                    node.cluster.metrics().await["shards"][0]["leader_id"].as_u64()
                {
                    if nodes
                        .get((leader - 1) as usize)
                        .is_some_and(Option::is_some)
                    {
                        return leader;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("leader election exceeded deadline")
}

async fn wait_for_result(
    node: &RunningNode,
    action: &str,
    payload: Value,
    predicate: impl Fn(&Value) -> bool,
) -> Value {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let response = public_command(node, action, payload.clone()).await;
            if response.status().is_success() {
                let body: Value = response.json().await.unwrap();
                if predicate(&body["result"]) {
                    return body["result"].clone();
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("background operation exceeded deadline")
}

async fn stop_node(nodes: &mut [Option<RunningNode>], node_id: u64) {
    let node = nodes[(node_id - 1) as usize].take().unwrap();
    node.background.abort();
    let _ = node.background.await;
    node.server.abort();
    let _ = node.server.await;
    node.cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn public_cluster_runtime_survives_failover_and_rejects_minority_writes() {
    tokio::time::timeout(DEADLINE * 3, async {
        let mut listeners = Vec::new();
        let mut addresses = BTreeMap::new();
        for node_id in 1..=3 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            addresses.insert(node_id, format!("http://{}", listener.local_addr().unwrap()));
            listeners.push(listener);
        }

        let mut nodes = Vec::new();
        for (offset, listener) in listeners.into_iter().enumerate() {
            let node_id = offset as u64 + 1;
            let directory = tempfile::tempdir().unwrap();
            seed_due_schedule(&directory);
            let cluster = Cluster::new(ClusterConfig {
                node_id,
                advertise: addresses[&node_id].clone(),
                peers: addresses.clone(),
                data_dir: directory.path().to_path_buf(),
                shards: 1,
                secret: "server-cluster-test-secret".to_string(),
                bootstrap: node_id == 1,
            })
            .await
            .unwrap();
            let runtime = Runtime::new_cluster(
                cluster.clone(),
                ADMIN.to_string(),
                b"cluster-test-signing-key".to_vec(),
            );
            let router = substratedb::server::router(runtime.clone()).merge(cluster.router());
            let server = tokio::spawn(async move { axum::serve(listener, router).await });
            let background = runtime.background();
            nodes.push(Some(RunningNode {
                address: addresses[&node_id].clone(),
                cluster,
                runtime,
                background,
                server,
                _directory: directory,
            }));
        }

        let original_leader = wait_for_leader(&nodes).await;
        let follower_id = (1..=3).find(|id| *id != original_leader).unwrap();
        let follower = nodes[(follower_id - 1) as usize].as_ref().unwrap();

        let health: Value = reqwest::get(format!("{}/health", follower.address))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(health["mode"], "cluster");
        let metrics = reqwest::Client::new()
            .get(format!("{}/metrics", follower.address))
            .bearer_auth(ADMIN)
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        assert!(metrics.contains("substrate_cluster_shard_info"));

        let write = public_result(
            follower,
            "document.put",
            json!({"collection":"orders","id":"one","value":{"status":"committed"}}),
        )
        .await;
        assert_eq!(write["revision"], 1);

        public_result(
            follower,
            "vector.configure",
            json!({"collection":"orders","name":"semantic","dimensions":2,"metric":"cosine"}),
        )
        .await;
        let vector = public_result(
            follower,
            "vector.upsert",
            json!({"collection":"orders","id":"one","name":"semantic","source_revision":1,"vector":[1.0,0.0]}),
        )
        .await;
        let sequence = vector["sequence"].as_u64().unwrap();
        wait_for_result(
            follower,
            "search.wait",
            json!({"collection":"orders","sequence":sequence}),
            |result| result["sequence"].as_u64().is_some_and(|value| value >= sequence),
        )
        .await;
        let scheduled = wait_for_result(
            follower,
            "job.get",
            json!({"id":"schedule:seed:60000"}),
            |result| result["payload"]["source"] == "background",
        )
        .await;
        assert_eq!(scheduled["queue"], "scheduled");

        stop_node(&mut nodes, original_leader).await;
        let replacement_leader = wait_for_leader(&nodes).await;
        assert_ne!(replacement_leader, original_leader);
        let survivor = nodes.iter().flatten().next().unwrap();
        let committed = public_result(
            survivor,
            "document.get",
            json!({"collection":"orders","id":"one"}),
        )
        .await;
        assert_eq!(committed["value"]["status"], "committed");

        let other_survivor = (1..=3)
            .find(|id| *id != original_leader && *id != replacement_leader)
            .unwrap();
        stop_node(&mut nodes, other_survivor).await;
        let minority = nodes[(replacement_leader - 1) as usize].as_ref().unwrap();
        let before = minority.runtime.operations.load(Ordering::Relaxed);
        let response = public_command(
            minority,
            "document.put",
            json!({"collection":"orders","id":"minority","value":{"accepted":false}}),
        )
        .await;
        assert!(!response.status().is_success());
        assert!(minority.runtime.operations.load(Ordering::Relaxed) > before);

        stop_node(&mut nodes, replacement_leader).await;
    })
    .await
    .expect("server cluster integration test exceeded its deadline");
}
