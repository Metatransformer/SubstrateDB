use serde_json::{json, Value};
use substratedb::{model::Command, store::Engine};
fn cmd(action: &str, payload: Value, now: u64) -> Command {
    Command {
        namespace: "test".into(),
        partition: "main".into(),
        action: action.into(),
        payload,
        request_id: uuid::Uuid::new_v4().to_string(),
        now_ms: now,
    }
}
fn run(db: &Engine, action: &str, payload: Value, now: u64) -> Value {
    db.execute(&cmd(action, payload, now)).unwrap()
}

#[test]
fn atomic_document_and_job_rollback_and_replay() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(dir.path().join("db")).unwrap();
    let mut c = cmd(
        "document.commit",
        json!({"document":{"collection":"orders","id":"a","value":{"amount":42}},"job":{"id":"j","queue":"pay","payload":{"order":"a"}}}),
        100,
    );
    let result = db.execute(&c).unwrap();
    assert_eq!(result["document"]["revision"], 1);
    c.now_ms = 200;
    assert_eq!(db.execute(&c).unwrap(), result);
    let bad = cmd(
        "document.commit",
        json!({"document":{"collection":"orders","id":"b","value":{}},"job":{"id":"j","queue":"different"}}),
        200,
    );
    assert!(db.execute(&bad).is_err());
    assert!(db
        .execute(&cmd(
            "document.get",
            json!({"collection":"orders","id":"b"}),
            200
        ))
        .is_err());
}
#[test]
fn job_leases_ordering_retries_and_retention() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(dir.path().join("db")).unwrap();
    for id in ["a", "b"] {
        run(
            &db,
            "job.enqueue",
            json!({"id":id,"queue":"q","payload":{},"ordering_key":"customer","max_attempts":2}),
            100,
        );
    }
    let first = run(
        &db,
        "job.claim",
        json!({"queue":"q","worker":"one","lease_ms":100}),
        100,
    );
    assert_eq!(first["job"]["id"], "a");
    assert!(run(&db, "job.claim", json!({"queue":"q","worker":"two"}), 101)["job"].is_null());
    let second = run(
        &db,
        "job.claim",
        json!({"queue":"q","worker":"two","lease_ms":100}),
        201,
    );
    assert_eq!(second["job"]["attempt"], 2);
    assert!(db
        .execute(&cmd(
            "job.ack",
            json!({"id":"a","receipt":first["receipt"]}),
            202
        ))
        .is_err());
    run(
        &db,
        "job.nack",
        json!({"id":"a","receipt":second["receipt"]}),
        202,
    );
    assert_eq!(
        run(&db, "job.get", json!({"id":"a"}), 202)["status"],
        "dead_letter"
    );
    run(&db, "event.trim", json!({"through":u64::MAX}), 203);
    assert_eq!(
        run(&db, "job.claim", json!({"queue":"q","worker":"two"}), 204)["job"]["id"],
        "b"
    );
}
#[test]
fn cache_ttl_types_atomic_counter_and_collections() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(dir.path().join("db")).unwrap();
    run(
        &db,
        "cache.set",
        json!({"key":"a","value":1,"ttl_ms":10}),
        100,
    );
    assert_eq!(
        run(&db, "cache.increment", json!({"key":"a","by":2}), 101),
        3
    );
    assert!(run(&db, "cache.get", json!({"key":"a"}), 110).is_null());
    run(
        &db,
        "cache.hset",
        json!({"key":"h","field":"name","value":"hello"}),
        100,
    );
    assert!(db
        .execute(&cmd("cache.increment", json!({"key":"h"}), 100))
        .is_err());
    assert_eq!(
        run(&db, "cache.hget", json!({"key":"h","field":"name"}), 100),
        "hello"
    );
    run(&db, "cache.sadd", json!({"key":"s","value":"a"}), 100);
    run(&db, "cache.sadd", json!({"key":"s","value":"a"}), 100);
    assert_eq!(
        run(&db, "cache.members", json!({"key":"s"}), 100),
        json!(["a"])
    );
    run(&db, "cache.rpush", json!({"key":"l","value":"first"}), 100);
    assert_eq!(run(&db, "cache.lpop", json!({"key":"l"}), 100), "first");
    run(
        &db,
        "cache.zadd",
        json!({"key":"z","member":"a","score":2}),
        100,
    );
    run(
        &db,
        "cache.zadd",
        json!({"key":"z","member":"b","score":1}),
        100,
    );
    assert_eq!(
        run(&db, "cache.zrange", json!({"key":"z"}), 100)[0]["member"],
        "b"
    );
}
#[test]
fn vectors_filter_revision_fusion_and_sparse() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(dir.path().join("db")).unwrap();
    run(
        &db,
        "vector.configure",
        json!({"collection":"docs","name":"dense","dimensions":2,"metric":"cosine"}),
        1,
    );
    run(
        &db,
        "vector.configure",
        json!({"collection":"docs","name":"sparse","dimensions":100,"kind":"sparse","metric":"dot"}),
        1,
    );
    for (id, tenant, body, v) in [
        ("a", "x", "rust database", json!([1.0, 0.0])),
        ("b", "y", "cooking recipes", json!([0.0, 1.0])),
    ] {
        run(
            &db,
            "document.put",
            json!({"collection":"docs","id":id,"value":{"tenant":tenant,"body":body}}),
            1,
        );
        run(
            &db,
            "vector.upsert",
            json!({"collection":"docs","id":id,"name":"dense","source_revision":1,"vector":v}),
            1,
        );
    }
    assert_eq!(
        db.execute(&cmd(
            "search.wait",
            json!({"collection":"docs","sequence":2}),
            1
        ))
        .unwrap_err()
        .code,
        "not_ready"
    );
    run(&db, "vector.flush", json!({}), 1);
    let q = json!({"collection":"docs","name":"dense","vector":[1.0,0.0],"filter":{"tenant":"x"},"text":"rust"});
    assert_eq!(run(&db, "search.query", q.clone(), 1)["hits"][0]["id"], "a");
    run(
        &db,
        "vector.upsert",
        json!({"collection":"docs","id":"a","name":"sparse","source_revision":1,"vector":{"indices":[3],"values":[2.0]}}),
        1,
    );
    run(&db, "vector.flush", json!({}), 1);
    assert_eq!(
        run(
            &db,
            "search.query",
            json!({"collection":"docs","name":"sparse","vector":{"indices":[3],"values":[1.0]}}),
            1
        )["hits"][0]["score"],
        2.0
    );
    run(
        &db,
        "document.put",
        json!({"collection":"docs","id":"a","value":{"tenant":"x","body":"changed"}}),
        2,
    );
    assert!(run(
        &db,
        "search.query",
        json!({"collection":"docs","name":"dense","vector":[1.0,0.0],"filter":{"tenant":"x"}}),
        2
    )["hits"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(db.execute(&cmd("vector.upsert",json!({"collection":"docs","id":"a","name":"dense","source_revision":1,"vector":[1.0,0.0]}),2)).is_err());
    run(
        &db,
        "document.delete",
        json!({"collection":"docs","id":"b"}),
        2,
    );
    assert!(run(
        &db,
        "search.query",
        json!({"collection":"docs","name":"dense","vector":[0.0,1.0]}),
        2
    )["hits"]
        .as_array()
        .unwrap()
        .is_empty());
}
#[test]
fn schedules_catch_up_and_restore_paused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let db = Engine::open(&path).unwrap();
    run(
        &db,
        "schedule.put",
        json!({"id":"minutely","cron":"* * * * *","job":{"queue":"q","payload":{}}}),
        0,
    );
    assert_eq!(
        run(&db, "schedule.tick", json!({}), 3600000)["created"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        run(&db, "schedule.tick", json!({}), 3660000)["created"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    let backup = dir.path().join("backup.json");
    db.backup(&backup).unwrap();
    let restored = Engine::open(dir.path().join("restore")).unwrap();
    restored.restore(&backup).unwrap();
    assert_eq!(
        run(&restored, "schedule.get", json!({"id":"minutely"}), 3660000)["paused"],
        true
    );
    assert!(run(
        &restored,
        "job.claim",
        json!({"queue":"q","worker":"w"}),
        3660000
    )["job"]
        .is_null());
    run(&restored, "job.resume", json!({"queue":"q"}), 3660000);
    assert!(!run(
        &restored,
        "job.claim",
        json!({"queue":"q","worker":"w"}),
        3660000
    )["job"]
        .is_null());
}
#[test]
fn dst_gap_is_skipped_and_repeat_runs_once() {
    use chrono::{TimeZone, Utc};
    use substratedb::scheduler::next_occurrence;
    let before = Utc
        .with_ymd_and_hms(2026, 3, 8, 7, 0, 0)
        .unwrap()
        .timestamp_millis() as u64;
    let next = next_occurrence("30 2 * * *", "America/Denver", before, "first").unwrap();
    assert_eq!(
        Utc.timestamp_millis_opt(next as i64).unwrap(),
        Utc.with_ymd_and_hms(2026, 3, 9, 8, 30, 0).unwrap()
    );
    let before = Utc
        .with_ymd_and_hms(2026, 11, 1, 6, 0, 0)
        .unwrap()
        .timestamp_millis() as u64;
    let first = next_occurrence("30 1 * * *", "America/Denver", before, "first").unwrap();
    let after = next_occurrence("30 1 * * *", "America/Denver", first, "first").unwrap();
    assert!(after - first > 24 * 3600000);
}
#[test]
fn indexed_snapshot_and_durable_consumer_gap() {
    let dir = tempfile::tempdir().unwrap();
    let db = Engine::open(dir.path().join("db")).unwrap();
    assert_eq!(
        db.execute(&cmd(
            "document.snapshot",
            json!({"collection":"c","filter":{"tenant":"a"}}),
            1
        ))
        .unwrap_err()
        .code,
        "index_required"
    );
    run(
        &db,
        "index.create",
        json!({"collection":"c","name":"tenant","fields":["tenant"]}),
        1,
    );
    run(
        &db,
        "document.put",
        json!({"collection":"c","id":"d","value":{"tenant":"a"}}),
        1,
    );
    let snap = run(
        &db,
        "document.snapshot",
        json!({"collection":"c","filter":{"tenant":"a"}}),
        1,
    );
    assert_eq!(snap["total"], 1);
    assert_eq!(snap["sequence"], 1);
    let events = run(&db, "consumer.read", json!({"name":"worker"}), 1);
    assert_eq!(events["events"].as_array().unwrap().len(), 1);
    run(&db, "event.trim", json!({"through":1}), 1);
    assert_eq!(
        db.execute(&cmd("consumer.read", json!({"name":"worker"}), 1))
            .unwrap_err()
            .code,
        "history_unavailable"
    );
}
