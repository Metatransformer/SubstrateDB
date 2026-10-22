use redb::{Database, TableDefinition};
use serde_json::{json, Value};
use substratedb::model::Command;
use substratedb::store::Engine;
use tempfile::tempdir;

fn command(
    action: &str,
    payload: Value,
    request_id: &str,
    namespace: &str,
    partition: &str,
) -> Command {
    Command {
        namespace: namespace.to_owned(),
        partition: partition.to_owned(),
        action: action.to_owned(),
        payload,
        request_id: request_id.to_owned(),
        now_ms: 1_700_000_000_000,
    }
}

fn execute(engine: &Engine, action: &str, payload: Value, request_id: &str) -> Value {
    engine
        .execute(&command(action, payload, request_id, "test", "main"))
        .unwrap()
}

#[test]
fn documents_and_dedup_survive_reopen() {
    let directory = tempdir().unwrap();
    let path = directory.path().join("data.redb");
    let put = command(
        "document.put",
        json!({"collection": "users", "id": "one", "value": {"name": "Ada"}}),
        "put-one",
        "test",
        "main",
    );
    let original = {
        let engine = Engine::open(&path).unwrap();
        engine.execute(&put).unwrap()
    };
    let engine = Engine::open(&path).unwrap();
    assert_eq!(engine.execute(&put).unwrap(), original);
    assert_eq!(
        execute(
            &engine,
            "document.get",
            json!({"collection": "users", "id": "one"}),
            ""
        ),
        json!({"id": "one", "revision": 1, "value": {"name": "Ada"}})
    );
}

#[test]
fn future_database_format_is_rejected() {
    const DATA: TableDefinition<&str, &str> = TableDefinition::new("substrate_data");

    let directory = tempdir().unwrap();
    let path = directory.path().join("data.redb");
    let database = Database::create(&path).unwrap();
    let transaction = database.begin_write().unwrap();
    {
        let mut table = transaction.open_table(DATA).unwrap();
        table.insert("format_version", "2").unwrap();
    }
    transaction.commit().unwrap();
    drop(database);

    assert_eq!(
        Engine::open(&path).err().unwrap().code,
        "unsupported_format"
    );
}

#[test]
fn cas_conflicts_are_atomic_and_recreate_keeps_revision_history() {
    let directory = tempdir().unwrap();
    let engine = Engine::open(directory.path().join("data.redb")).unwrap();
    execute(
        &engine,
        "document.put",
        json!({"collection": "items", "id": "a", "value": {"n": 1}, "expected_revision": 0}),
        "create-a",
    );
    let error = engine
        .execute(&command(
            "document.put",
            json!({"collection": "items", "id": "a", "value": {"n": 2}, "expected_revision": 9}),
            "bad-cas",
            "test",
            "main",
        ))
        .unwrap_err();
    assert_eq!(error.code, "conflict");
    assert_eq!(
        execute(
            &engine,
            "document.get",
            json!({"collection": "items", "id": "a"}),
            ""
        )["value"],
        json!({"n": 1})
    );
    assert_eq!(execute(&engine, "event.read", json!({}), "")["latest"], 1);
    assert_eq!(
        execute(
            &engine,
            "document.delete",
            json!({"collection": "items", "id": "a", "expected_revision": 1}),
            "delete-a",
        )["revision"],
        2
    );
    assert_eq!(
        execute(
            &engine,
            "document.put",
            json!({"collection": "items", "id": "a", "value": {"n": 3}, "expected_revision": 0}),
            "recreate-a",
        )["revision"],
        3
    );
}

#[test]
fn index_postings_follow_updates_and_deletes() {
    let directory = tempdir().unwrap();
    let engine = Engine::open(directory.path().join("data.redb")).unwrap();
    execute(
        &engine,
        "index.create",
        json!({"collection": "people", "name": "by_team_role", "fields": ["team", "profile.role"]}),
        "create-index",
    );
    execute(
        &engine,
        "document.put",
        json!({"collection": "people", "id": "a", "value": {"team": "red", "profile": {"role": "dev"}}}),
        "put-a",
    );
    execute(
        &engine,
        "document.put",
        json!({"collection": "people", "id": "b", "value": {"team": "red", "profile": {"role": "dev"}}}),
        "put-b",
    );
    assert_eq!(
        execute(
            &engine,
            "document.query",
            json!({"collection": "people", "filter": {"team": "red", "profile.role": "dev"}}),
            ""
        )["total"],
        2
    );
    execute(
        &engine,
        "document.put",
        json!({"collection": "people", "id": "a", "value": {"team": "blue", "profile": {"role": "dev"}}, "expected_revision": 1}),
        "move-a",
    );
    assert_eq!(
        execute(
            &engine,
            "document.query",
            json!({"collection": "people", "filter": {"team": "red", "profile.role": "dev"}}),
            ""
        )["total"],
        1
    );
    execute(
        &engine,
        "document.delete",
        json!({"collection": "people", "id": "b"}),
        "delete-b",
    );
    assert_eq!(
        execute(
            &engine,
            "document.query",
            json!({"collection": "people", "filter": {"team": "red", "profile.role": "dev"}}),
            ""
        )["total"],
        0
    );
}

#[test]
fn unique_violations_roll_back_document_and_event_changes() {
    let directory = tempdir().unwrap();
    let engine = Engine::open(directory.path().join("data.redb")).unwrap();
    execute(
        &engine,
        "index.create",
        json!({"collection": "users", "name": "email", "fields": ["email"], "unique": true}),
        "email-index",
    );
    execute(
        &engine,
        "document.put",
        json!({"collection": "users", "id": "one", "value": {"email": "same@example.com"}}),
        "put-one",
    );
    let error = engine
        .execute(&command(
            "document.put",
            json!({"collection": "users", "id": "two", "value": {"email": "same@example.com"}}),
            "put-two",
            "test",
            "main",
        ))
        .unwrap_err();
    assert_eq!(error.code, "conflict");
    assert_eq!(
        engine
            .execute(&command(
                "document.get",
                json!({"collection": "users", "id": "two"}),
                "",
                "test",
                "main",
            ))
            .unwrap_err()
            .code,
        "not_found"
    );
    assert_eq!(execute(&engine, "event.read", json!({}), "")["latest"], 1);
}

#[test]
fn failed_unique_index_build_leaves_no_definition_or_postings() {
    let directory = tempdir().unwrap();
    let engine = Engine::open(directory.path().join("data.redb")).unwrap();
    for (id, request) in [("one", "one"), ("two", "two")] {
        execute(
            &engine,
            "document.put",
            json!({"collection": "users", "id": id, "value": {"email": "duplicate@example.com"}}),
            request,
        );
    }
    let error = engine
        .execute(&command(
            "index.create",
            json!({"collection": "users", "name": "email", "fields": ["email"], "unique": true}),
            "unique-index",
            "test",
            "main",
        ))
        .unwrap_err();
    assert_eq!(error.code, "conflict");
    assert_eq!(
        execute(&engine, "index.list", json!({"collection": "users"}), ""),
        json!({"indexes": []})
    );
    assert_eq!(
        execute(
            &engine,
            "document.query",
            json!({"collection": "users", "filter": {"email": "duplicate@example.com"}}),
            ""
        )["total"],
        2
    );
}

#[test]
fn namespace_partition_and_request_ids_are_isolated() {
    let directory = tempdir().unwrap();
    let engine = Engine::open(directory.path().join("data.redb")).unwrap();
    for (namespace, partition, value) in [("a", "one", 1), ("a", "two", 2), ("b", "one", 3)] {
        engine
            .execute(&command(
                "document.put",
                json!({"collection": "items", "id": "same", "value": {"n": value}}),
                "same-request",
                namespace,
                partition,
            ))
            .unwrap();
    }
    for (namespace, partition, value) in [("a", "one", 1), ("a", "two", 2), ("b", "one", 3)] {
        let result = engine
            .execute(&command(
                "document.get",
                json!({"collection": "items", "id": "same"}),
                "",
                namespace,
                partition,
            ))
            .unwrap();
        assert_eq!(result["value"]["n"], value);
    }
}

#[test]
fn request_id_reuse_with_changed_content_is_rejected() {
    let directory = tempdir().unwrap();
    let engine = Engine::open(directory.path().join("data.redb")).unwrap();
    execute(
        &engine,
        "document.put",
        json!({"collection": "items", "id": "one", "value": {"n": 1}}),
        "request",
    );
    let error = engine
        .execute(&command(
            "document.put",
            json!({"collection": "items", "id": "one", "value": {"n": 2}}),
            "request",
            "test",
            "main",
        ))
        .unwrap_err();
    assert_eq!(error.code, "conflict");
}

#[test]
fn trim_reports_gaps_and_event_sequences_remain_monotonic() {
    let directory = tempdir().unwrap();
    let engine = Engine::open(directory.path().join("data.redb")).unwrap();
    for (id, request) in [("one", "one"), ("two", "two"), ("three", "three")] {
        execute(
            &engine,
            "document.put",
            json!({"collection": "items", "id": id, "value": {"id": id}}),
            request,
        );
    }
    assert_eq!(
        execute(&engine, "event.trim", json!({"through": 2}), "trim",),
        json!({"trimmed": 2, "through": 2})
    );
    assert_eq!(
        engine
            .execute(&command(
                "event.read",
                json!({"after": 0}),
                "",
                "test",
                "main",
            ))
            .unwrap_err()
            .code,
        "history_unavailable"
    );
    let retained = execute(&engine, "event.read", json!({"after": 2}), "");
    assert_eq!(retained["oldest"], 3);
    assert_eq!(retained["events"][0]["sequence"], 3);
    execute(
        &engine,
        "document.put",
        json!({"collection": "items", "id": "four", "value": {"id": "four"}}),
        "four",
    );
    let events = execute(&engine, "event.read", json!({"after": 2}), "");
    assert_eq!(events["latest"], 4);
    assert_eq!(events["events"][1]["sequence"], 4);
}

#[test]
fn query_supports_nested_filter_sort_projection_and_bounds() {
    let directory = tempdir().unwrap();
    let engine = Engine::open(directory.path().join("data.redb")).unwrap();
    for (id, score) in [("a", 1), ("b", 3), ("c", 2)] {
        execute(
            &engine,
            "document.put",
            json!({"collection": "scores", "id": id, "value": {"group": {"name": "x"}, "score": score, "private": true}}),
            id,
        );
    }
    let result = execute(
        &engine,
        "document.query",
        json!({
            "collection": "scores",
            "filter": {"group.name": "x"},
            "sort": {"field": "score", "direction": "desc"},
            "offset": 1,
            "limit": 1,
            "projection": ["group.name", "score"]
        }),
        "",
    );
    assert_eq!(result["total"], 3);
    assert_eq!(result["documents"][0]["id"], "c");
    assert_eq!(
        result["documents"][0]["value"],
        json!({"group": {"name": "x"}, "score": 2})
    );
    assert_eq!(
        engine
            .execute(&command(
                "document.query",
                json!({"collection": "scores", "limit": 1001}),
                "",
                "test",
                "main",
            ))
            .unwrap_err()
            .code,
        "invalid_argument"
    );
}
