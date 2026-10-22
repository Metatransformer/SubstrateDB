use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{json, Value};
use substratedb::cluster::{shard_for_scope, Cluster, ClusterConfig};
use substratedb::cluster_storage::{PersistentStateMachine, TypeConfig};
use substratedb::model::Command;
use substratedb::store::Engine;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tower::ServiceExt;

use openraft::storage::RaftStateMachine;
use openraft::RaftSnapshotBuilder;

const DEADLINE: Duration = Duration::from_secs(15);

struct RunningNode {
    cluster: Cluster,
    server: JoinHandle<std::io::Result<()>>,
    _directory: TempDir,
}

fn command(
    namespace: &str,
    partition: &str,
    action: &str,
    payload: Value,
    request_id: &str,
) -> Command {
    Command {
        namespace: namespace.to_string(),
        partition: partition.to_string(),
        action: action.to_string(),
        payload,
        request_id: request_id.to_string(),
        now_ms: 1,
    }
}

async fn wait_for_leader(nodes: &[Option<RunningNode>], shard: u64) -> u64 {
    tokio::time::timeout(DEADLINE, async {
        loop {
            for node in nodes.iter().flatten() {
                let metrics = node.cluster.metrics().await;
                if let Some(leader) = metrics["shards"][shard as usize]["leader_id"].as_u64() {
                    if leader > 0
                        && nodes
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

async fn stop_node(nodes: &mut [Option<RunningNode>], node_id: u64) {
    let index = usize::try_from(node_id - 1).unwrap();
    let node = nodes[index].take().expect("node is running");
    node.server.abort();
    node.cluster.shutdown().await;
}

#[test]
fn scope_hashing_is_stable_and_unambiguous() {
    for shards in [1, 2, 7, 64] {
        let first = shard_for_scope("tenant", "logical-partition", shards);
        assert_eq!(
            first,
            shard_for_scope("tenant", "logical-partition", shards)
        );
        assert!(first < shards);
    }
    assert_ne!(
        shard_for_scope("ab", "c", u64::MAX),
        shard_for_scope("a", "bc", u64::MAX),
        "length framing must keep scope pairs distinct"
    );
}

#[tokio::test]
async fn internal_control_routes_require_the_shared_secret() {
    let directory = tempfile::tempdir().unwrap();
    let cluster = Cluster::new(ClusterConfig {
        node_id: 1,
        advertise: "http://127.0.0.1:41001".to_string(),
        peers: BTreeMap::from([(1, "http://127.0.0.1:41001".to_string())]),
        data_dir: directory.path().to_path_buf(),
        shards: 1,
        secret: "internal-secret".to_string(),
        bootstrap: false,
    })
    .await
    .unwrap();
    let response = cluster
        .router()
        .oneshot(
            Request::post("/internal/raft/0/initialize")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    cluster.shutdown().await;
}

#[tokio::test]
async fn consensus_snapshot_round_trips_engine_state() {
    let source_directory = tempfile::tempdir().unwrap();
    let source_engine =
        Arc::new(Engine::open(source_directory.path().join("engine.redb")).unwrap());
    source_engine
        .execute(&command(
            "snapshot",
            "main",
            "document.put",
            json!({"collection":"docs","id":"one","value":{"saved":true}}),
            "snapshot-write",
        ))
        .unwrap();
    let mut source = PersistentStateMachine::open(source_directory.path(), source_engine).unwrap();
    let snapshot = RaftSnapshotBuilder::<TypeConfig>::build_snapshot(&mut source)
        .await
        .unwrap();
    let snapshot_meta = snapshot.meta.clone();
    let snapshot_bytes = snapshot.snapshot.get_ref().clone();

    let target_directory = tempfile::tempdir().unwrap();
    let target_engine =
        Arc::new(Engine::open(target_directory.path().join("engine.redb")).unwrap());
    let mut target =
        PersistentStateMachine::open(target_directory.path(), target_engine.clone()).unwrap();
    RaftStateMachine::<TypeConfig>::install_snapshot(
        &mut target,
        &snapshot.meta,
        snapshot.snapshot,
    )
    .await
    .unwrap();

    let restored = target_engine
        .execute(&command(
            "snapshot",
            "main",
            "document.get",
            json!({"collection":"docs","id":"one"}),
            "",
        ))
        .unwrap();
    assert_eq!(restored["value"]["saved"], true);

    // Simulate a process stopping after the incoming snapshot and recovery marker
    // are durable but before the engine import and applied boundary complete.
    let recovery_directory = tempfile::tempdir().unwrap();
    std::fs::write(
        recovery_directory.path().join("snapshot-install.bin"),
        snapshot_bytes,
    )
    .unwrap();
    std::fs::write(
        recovery_directory.path().join("snapshot-install.json"),
        serde_json::to_vec(&snapshot_meta).unwrap(),
    )
    .unwrap();
    let recovery_engine =
        Arc::new(Engine::open(recovery_directory.path().join("engine.redb")).unwrap());
    let _recovered =
        PersistentStateMachine::open(recovery_directory.path(), recovery_engine.clone()).unwrap();
    let recovered = recovery_engine
        .execute(&command(
            "snapshot",
            "main",
            "document.get",
            json!({"collection":"docs","id":"one"}),
            "",
        ))
        .unwrap();
    assert_eq!(recovered["value"]["saved"], true);
    assert!(!recovery_directory
        .path()
        .join("snapshot-install.json")
        .exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn three_nodes_replicate_fail_over_and_reject_minority_writes() {
    tokio::time::timeout(DEADLINE * 3, async {
        let mut listeners = Vec::new();
        let mut addresses = BTreeMap::new();
        for node_id in 1..=3 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            addresses.insert(
                node_id,
                format!("http://{}", listener.local_addr().unwrap()),
            );
            listeners.push(listener);
        }

        let secret = "test-only-shared-cluster-secret";
        let mut nodes = Vec::new();
        for (offset, listener) in listeners.into_iter().enumerate() {
            let node_id = offset as u64 + 1;
            let directory = tempfile::tempdir().unwrap();
            let cluster = Cluster::new(ClusterConfig {
                node_id,
                advertise: addresses[&node_id].clone(),
                peers: addresses.clone(),
                data_dir: directory.path().to_path_buf(),
                shards: 2,
                secret: secret.to_string(),
                bootstrap: node_id == 1,
            })
            .await
            .unwrap();
            let router = cluster.router();
            let server = tokio::spawn(async move { axum::serve(listener, router).await });
            nodes.push(Some(RunningNode {
                cluster,
                server,
                _directory: directory,
            }));
        }

        let namespace = "acme";
        let partition = "orders";
        let shard = shard_for_scope(namespace, partition, 2);
        let original_leader = wait_for_leader(&nodes, shard).await;
        let follower = (1..=3).find(|id| *id != original_leader).unwrap();

        let write = nodes[(follower - 1) as usize]
            .as_ref()
            .unwrap()
            .cluster
            .execute(command(
                namespace,
                partition,
                "document.put",
                json!({"collection":"orders","id":"one","value":{"status":"committed"}}),
                "cluster-write-one",
            ))
            .await
            .unwrap();
        assert_eq!(write["revision"], 1);

        let read = nodes[(follower - 1) as usize]
            .as_ref()
            .unwrap()
            .cluster
            .execute(command(
                namespace,
                partition,
                "document.get",
                json!({"collection":"orders","id":"one"}),
                "",
            ))
            .await
            .unwrap();
        assert_eq!(read["value"]["status"], "committed");

        stop_node(&mut nodes, original_leader).await;
        let replacement_leader = wait_for_leader(&nodes, shard).await;
        assert_ne!(replacement_leader, original_leader);
        let survivor = nodes.iter().flatten().next().unwrap();
        let after_failover = survivor
            .cluster
            .execute(command(
                namespace,
                partition,
                "document.get",
                json!({"collection":"orders","id":"one"}),
                "",
            ))
            .await
            .unwrap();
        assert_eq!(after_failover["value"]["status"], "committed");

        let other_survivor = (1..=3)
            .find(|id| *id != original_leader && *id != replacement_leader)
            .unwrap();
        stop_node(&mut nodes, other_survivor).await;
        let minority = nodes[(replacement_leader - 1) as usize].as_ref().unwrap();
        let error = minority
            .cluster
            .execute(command(
                namespace,
                partition,
                "document.put",
                json!({"collection":"orders","id":"minority","value":{"bad":true}}),
                "minority-write",
            ))
            .await
            .expect_err("a lone node must not acknowledge a write");
        assert!(
            matches!(
                error.code.as_str(),
                "quorum_unavailable" | "not_ready" | "not_leader"
            ),
            "unexpected minority error: {error:?}"
        );

        stop_node(&mut nodes, replacement_leader).await;
    })
    .await
    .expect("cluster integration test exceeded its deadline");
}
