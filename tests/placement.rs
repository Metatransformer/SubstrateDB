use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use serde_json::{json, Value};
use substratedb::cluster::{shard_for_scope, Cluster, ClusterConfig};
use substratedb::model::Command;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const DEADLINE: Duration = Duration::from_secs(20);
const SECRET: &str = "placement-test-shared-secret";

struct RunningNode {
    address: String,
    cluster: Cluster,
    server: JoinHandle<std::io::Result<()>>,
    _directory: TempDir,
}

fn command(partition: &str, action: &str, payload: Value, request_id: &str) -> Command {
    Command {
        namespace: "placement-test".to_string(),
        partition: partition.to_string(),
        action: action.to_string(),
        payload,
        request_id: request_id.to_string(),
        now_ms: 1,
    }
}

fn partition_for(shard: u64) -> String {
    (0..10_000)
        .map(|index| format!("partition-{index}"))
        .find(|partition| shard_for_scope("placement-test", partition, 2) == shard)
        .expect("a partition maps to each shard")
}

async fn wait_for_leader(nodes: &[Option<RunningNode>], shard: u64, voters: &BTreeSet<u64>) -> u64 {
    tokio::time::timeout(DEADLINE, async {
        loop {
            for node in nodes.iter().flatten() {
                let metrics = node.cluster.metrics().await;
                if let Some(leader) = metrics["shards"][shard as usize]["leader_id"].as_u64() {
                    if voters.contains(&leader)
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

async fn wait_for_voters(node: &RunningNode, shard: u64, expected: &[u64]) {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let metrics = node.cluster.metrics().await;
            if metrics["shards"][shard as usize]["voters"] == json!(expected) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("membership metrics did not converge")
}

async fn stop_node(nodes: &mut [Option<RunningNode>], node_id: u64) {
    let node = nodes[(node_id - 1) as usize]
        .take()
        .expect("node is running");
    node.server.abort();
    let _ = node.server.await;
    node.cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn placement_forwards_from_excluded_nodes_and_moves_membership_safely() {
    tokio::time::timeout(DEADLINE * 4, async {
        let mut listeners = Vec::new();
        let mut addresses = BTreeMap::new();
        for node_id in 1..=4 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            addresses.insert(
                node_id,
                format!("http://{}", listener.local_addr().unwrap()),
            );
            listeners.push(listener);
        }

        let shard_zero = BTreeSet::from([1, 2, 3]);
        let shard_one = BTreeSet::from([2, 3, 4]);
        let placements = BTreeMap::from([(0, shard_zero.clone()), (1, shard_one.clone())]);
        let mut nodes = Vec::new();
        for (offset, listener) in listeners.into_iter().enumerate() {
            let node_id = offset as u64 + 1;
            let directory = tempfile::tempdir().unwrap();
            let cluster = Cluster::new_with_placement(
                ClusterConfig {
                    node_id,
                    advertise: addresses[&node_id].clone(),
                    peers: addresses.clone(),
                    data_dir: directory.path().to_path_buf(),
                    shards: 2,
                    secret: SECRET.to_string(),
                    bootstrap: node_id == 1,
                },
                placements.clone(),
            )
            .await
            .unwrap();
            let server_cluster = cluster.clone();
            let server =
                tokio::spawn(async move { axum::serve(listener, server_cluster.router()).await });
            nodes.push(Some(RunningNode {
                address: addresses[&node_id].clone(),
                cluster,
                server,
                _directory: directory,
            }));
        }

        let original_leader = wait_for_leader(&nodes, 0, &shard_zero).await;
        let _other_leader = wait_for_leader(&nodes, 1, &shard_one).await;
        wait_for_voters(nodes[0].as_ref().unwrap(), 0, &[1, 2, 3]).await;
        wait_for_voters(nodes[3].as_ref().unwrap(), 1, &[2, 3, 4]).await;
        assert_eq!(
            nodes[3].as_ref().unwrap().cluster.metrics().await["shards"][0]["voters"],
            json!([]),
            "node 4 starts outside shard 0"
        );
        assert_eq!(
            nodes[0].as_ref().unwrap().cluster.metrics().await["shards"][1]["voters"],
            json!([]),
            "node 1 starts outside shard 1"
        );

        let partition = partition_for(0);
        let excluded = nodes[3].as_ref().unwrap();
        let write = excluded
            .cluster
            .execute(command(
                &partition,
                "document.put",
                json!({"collection":"docs","id":"placed","value":{"durable":true}}),
                "placement-write",
            ))
            .await
            .unwrap();
        assert_eq!(write["revision"], 1);
        let read = excluded
            .cluster
            .execute(command(
                &partition,
                "document.get",
                json!({"collection":"docs","id":"placed"}),
                "",
            ))
            .await
            .unwrap();
        assert_eq!(read["value"]["durable"], true);

        let desired = [1, 2, 3, 4]
            .into_iter()
            .filter(|node_id| *node_id != original_leader)
            .collect::<BTreeSet<_>>();
        let response = reqwest::Client::new()
            .post(format!(
                "{}/internal/raft/0/membership",
                nodes[(original_leader - 1) as usize]
                    .as_ref()
                    .unwrap()
                    .address
            ))
            .header("x-substrate-raft-secret", SECRET)
            .json(&json!({"voters": desired}))
            .send()
            .await
            .unwrap();
        let status = response.status();
        let report: Value = response.json().await.unwrap();
        assert!(status.is_success(), "membership HTTP {status}: {report}");
        let desired_vec = desired.iter().copied().collect::<Vec<_>>();
        assert_eq!(report["voters"], json!(desired_vec));

        for node_id in &desired {
            wait_for_voters(
                nodes[(*node_id - 1) as usize].as_ref().unwrap(),
                0,
                &desired_vec,
            )
            .await;
        }

        stop_node(&mut nodes, original_leader).await;
        let replacement = wait_for_leader(&nodes, 0, &desired).await;
        let survivor = nodes[(replacement - 1) as usize].as_ref().unwrap();
        let after_move = survivor
            .cluster
            .execute(command(
                &partition,
                "document.get",
                json!({"collection":"docs","id":"placed"}),
                "",
            ))
            .await
            .unwrap();
        assert_eq!(after_move["value"]["durable"], true);
        assert_eq!(
            survivor.cluster.metrics().await["shards"][0]["voters"],
            json!(desired_vec)
        );

        for node_id in desired {
            if nodes[(node_id - 1) as usize].is_some() {
                stop_node(&mut nodes, node_id).await;
            }
        }
        for node_id in shard_one {
            if nodes[(node_id - 1) as usize].is_some() {
                stop_node(&mut nodes, node_id).await;
            }
        }
    })
    .await
    .expect("placement integration test exceeded its deadline");
}
