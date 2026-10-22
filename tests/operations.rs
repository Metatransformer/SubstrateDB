use serde_json::{json, Value};
use substratedb::{model::Command, store::Engine};
fn run(
    e: &Engine,
    action: &str,
    payload: Value,
    now: u64,
    id: &str,
) -> substratedb::model::Result<Value> {
    e.execute(&Command {
        namespace: "n".into(),
        partition: "p".into(),
        action: action.into(),
        payload,
        now_ms: now,
        request_id: id.into(),
    })
}
#[test]
fn quotas_rollback_and_partition_isolation() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open(d.path().join("db")).unwrap();
    run(
        &e,
        "scope.configure",
        json!({"max_bytes":1048576}),
        0,
        "limits",
    )
    .unwrap();
    let before = run(&e, "scope.stats", json!({}), 0, "").unwrap()["logical_bytes"].clone();
    assert_eq!(
        run(
            &e,
            "cache.set",
            json!({"key":"large","value":"x".repeat(600000)}),
            0,
            "large"
        )
        .unwrap_err()
        .code,
        "quota_exceeded"
    );
    assert_eq!(
        run(&e, "scope.stats", json!({}), 0, "").unwrap()["logical_bytes"],
        before
    );
    assert!(run(&e, "cache.get", json!({"key":"large"}), 0, "")
        .unwrap()
        .is_null());
    run(
        &e,
        "cache.set",
        json!({"key":"small","value":"ok"}),
        0,
        "small",
    )
    .unwrap();
    let other = e
        .execute(&Command {
            namespace: "n".into(),
            partition: "other".into(),
            action: "cache.get".into(),
            payload: json!({"key":"small"}),
            now_ms: 0,
            request_id: "".into(),
        })
        .unwrap();
    assert!(other.is_null());
}
#[test]
fn retention_preserves_pending_jobs_and_reports_event_gap() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open(d.path().join("db")).unwrap();
    run(
        &e,
        "scope.configure",
        json!({"event_retention_ms":60000,"terminal_job_retention_ms":60000}),
        1,
        "limits",
    )
    .unwrap();
    run(
        &e,
        "cache.set",
        json!({"key":"tmp","value":1,"ttl_ms":100}),
        1,
        "cache",
    )
    .unwrap();
    run(
        &e,
        "job.enqueue",
        json!({"id":"pending","queue":"q","payload":1}),
        1,
        "pending",
    )
    .unwrap();
    run(
        &e,
        "job.enqueue",
        json!({"id":"done","queue":"q","payload":1}),
        1,
        "done",
    )
    .unwrap();
    run(&e, "job.cancel", json!({"id":"done"}), 2, "cancel").unwrap();
    let result = run(&e, "scope.maintain", json!({}), 86400002, "maint").unwrap();
    assert!(result["removed"].as_u64().unwrap() >= 4);
    assert_eq!(
        run(&e, "job.get", json!({"id":"pending"}), 86400002, "").unwrap()["status"],
        "ready"
    );
    assert_eq!(
        run(&e, "job.get", json!({"id":"done"}), 86400002, "")
            .unwrap_err()
            .code,
        "not_found"
    );
    assert_eq!(
        run(&e, "event.read", json!({"after":0}), 86400002, "")
            .unwrap_err()
            .code,
        "history_unavailable"
    );
    assert!(run(&e, "cache.get", json!({"key":"tmp"}), 86400002, "")
        .unwrap()
        .is_null());
}
#[test]
fn routing_fanout_deduplicates_queue_and_rolls_back_invalid_delivery() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open(d.path().join("db")).unwrap();
    for (id, topic, queue) in [
        ("a", "orders.*", "q1"),
        ("b", "orders.#", "q1"),
        ("c", "orders.created", "q2"),
    ] {
        run(
            &e,
            "route.bind",
            json!({"id":id,"topic":topic,"queue":queue}),
            1,
            id,
        )
        .unwrap();
    }
    let publish = run(
        &e,
        "message.publish",
        json!({"id":"m","topic":"orders.created","payload":{"x":1}}),
        2,
        "m",
    )
    .unwrap();
    assert_eq!(publish["jobs"].as_array().unwrap().len(), 2);
    assert_eq!(
        run(
            &e,
            "message.publish",
            json!({"id":"x","topic":"unknown","payload":null}),
            2,
            "x"
        )
        .unwrap_err()
        .code,
        "unroutable"
    );
    assert_eq!(
        run(&e, "job.list", json!({}), 2, "").unwrap()["jobs"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}
#[test]
fn schedule_templates_fail_before_installation() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open(d.path().join("db")).unwrap();
    assert_eq!(
        run(
            &e,
            "schedule.put",
            json!({"id":"bad","cron":"* * * * *","job":{"queue":"q","lease_ms":1}}),
            1,
            "bad"
        )
        .unwrap_err()
        .code,
        "invalid_argument"
    );
    assert!(
        run(&e, "schedule.list", json!({}), 2, "").unwrap()["schedules"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}
#[test]
fn corrupt_backup_is_rejected_without_changing_live_state() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open(d.path().join("db")).unwrap();
    run(&e, "cache.set", json!({"key":"safe","value":1}), 1, "safe").unwrap();
    let p = d.path().join("backup.json");
    e.backup(&p).unwrap();
    let mut image: Value = serde_json::from_slice(&std::fs::read(&p).unwrap()).unwrap();
    image["sha256"] = json!("wrong");
    std::fs::write(&p, image.to_string()).unwrap();
    assert_eq!(e.restore(&p).unwrap_err().code, "storage_error");
    assert_eq!(
        run(&e, "cache.get", json!({"key":"safe"}), 2, "").unwrap(),
        1
    );
}
#[test]
fn lease_fences_survive_release_and_expiry_rate_windows_are_atomic() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open(d.path().join("db")).unwrap();
    let first = run(
        &e,
        "cache.lease_acquire",
        json!({"key":"lock","owner":"a","ttl_ms":100}),
        1,
        "a",
    )
    .unwrap();
    assert_eq!(first["token"], 1);
    assert_eq!(
        run(
            &e,
            "cache.lease_acquire",
            json!({"key":"lock","owner":"b","ttl_ms":100}),
            2,
            "b"
        )
        .unwrap()["acquired"],
        false
    );
    let second = run(
        &e,
        "cache.lease_acquire",
        json!({"key":"lock","owner":"b","ttl_ms":100}),
        102,
        "b2",
    )
    .unwrap();
    assert_eq!(second["token"], 2);
    assert_eq!(
        run(
            &e,
            "cache.lease_release",
            json!({"key":"lock","owner":"a","token":1}),
            103,
            "stale"
        )
        .unwrap_err()
        .code,
        "conflict"
    );
    run(
        &e,
        "cache.lease_release",
        json!({"key":"lock","owner":"b","token":2}),
        103,
        "release",
    )
    .unwrap();
    assert_eq!(
        run(
            &e,
            "cache.lease_acquire",
            json!({"key":"lock","owner":"a","ttl_ms":100}),
            104,
            "a2"
        )
        .unwrap()["token"],
        3
    );
    for (i, expected) in [(1, true), (2, true), (3, false)] {
        assert_eq!(
            run(
                &e,
                "cache.rate_limit",
                json!({"key":"rate","window_ms":100,"limit":2}),
                1,
                &format!("rate-{i}")
            )
            .unwrap()["allowed"],
            expected
        );
    }
    assert_eq!(
        run(
            &e,
            "cache.rate_limit",
            json!({"key":"rate","window_ms":100,"limit":2}),
            100,
            "next-window"
        )
        .unwrap()["allowed"],
        true
    );
}
#[test]
fn batches_are_atomic_and_idle_maintenance_does_not_grow_storage() {
    let d = tempfile::tempdir().unwrap();
    let e = Engine::open(d.path().join("db")).unwrap();
    assert!(run(&e,"transaction",json!({"commands":[{"action":"cache.set","payload":{"key":"x","value":1}},{"action":"cache.increment","payload":{"key":"x","by":"bad"}}]}),1,"batch").is_err());
    assert!(run(&e, "cache.get", json!({"key":"x"}), 1, "")
        .unwrap()
        .is_null());
    run(
        &e,
        "job.claim",
        json!({"queue":"q","worker":"w"}),
        1,
        "empty",
    )
    .unwrap();
    run(&e, "scope.maintain", json!({}), 1, "init").unwrap();
    let before = run(&e, "scope.stats", json!({}), 1, "").unwrap()["logical_bytes"].clone();
    for i in 0..10 {
        run(
            &e,
            "job.claim",
            json!({"queue":"q","worker":"w"}),
            2,
            &format!("empty-{i}"),
        )
        .unwrap();
        run(&e, "scope.maintain", json!({}), 2, &format!("maint-{i}")).unwrap();
    }
    assert_eq!(
        run(&e, "scope.stats", json!({}), 2, "").unwrap()["logical_bytes"],
        before
    );
}

#[test]
fn concurrent_reads_observe_complete_batch_snapshots() {
    let d = tempfile::tempdir().unwrap();
    let e = std::sync::Arc::new(Engine::open(d.path().join("db")).unwrap());
    let writer = e.clone();
    let thread = std::thread::spawn(move || {
        for value in 0..30 {
            run(&writer,"transaction",json!({"commands":[{"action":"document.put","payload":{"collection":"pairs","id":"a","value":{"value":value}}},{"action":"document.put","payload":{"collection":"pairs","id":"b","value":{"value":value}}}]}),value,&format!("batch-{value}")).unwrap();
        }
    });
    for _ in 0..60 {
        let result = run(&e, "document.query", json!({"collection":"pairs"}), 1, "").unwrap();
        let documents = result["documents"].as_array().unwrap();
        assert!(documents.is_empty() || documents.len() == 2);
        if documents.len() == 2 {
            assert_eq!(documents[0]["value"], documents[1]["value"]);
        }
    }
    thread.join().unwrap();
}
