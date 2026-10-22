use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use substratedb::{model::Command, store::Engine};

static REQUEST: AtomicU64 = AtomicU64::new(1);

fn command(action: &str, payload: Value) -> Command {
    Command {
        namespace: "search-tests".into(),
        partition: "main".into(),
        action: action.into(),
        payload,
        request_id: format!("search-{}", REQUEST.fetch_add(1, Ordering::Relaxed)),
        now_ms: 1,
    }
}

fn run(engine: &Engine, action: &str, payload: Value) -> Value {
    engine.execute(&command(action, payload)).unwrap()
}

fn engine() -> (tempfile::TempDir, Engine) {
    let directory = tempfile::tempdir().unwrap();
    let engine = Engine::open(directory.path().join("search.redb")).unwrap();
    (directory, engine)
}

fn put(engine: &Engine, id: &str, value: Value) {
    run(
        engine,
        "document.put",
        json!({"collection":"docs","id":id,"value":value}),
    );
}

fn configure_dense(engine: &Engine) {
    run(
        engine,
        "vector.configure",
        json!({"collection":"docs","name":"embedding","dimensions":2,"metric":"cosine"}),
    );
}

#[test]
fn pending_upsert_is_rejected_by_delete_and_cannot_resurrect() {
    let (_directory, engine) = engine();
    configure_dense(&engine);
    put(&engine, "a", json!({"tenant":"red"}));
    let submitted = run(
        &engine,
        "vector.upsert",
        json!({"collection":"docs","name":"embedding","id":"a","vector":[1.0,0.0],"source_revision":1}),
    );
    assert_eq!(submitted["sequence"], 1);

    let deleted = run(
        &engine,
        "vector.delete",
        json!({"collection":"docs","name":"embedding","id":"a"}),
    );
    assert_eq!(deleted["rejected_pending"], 1);
    assert_eq!(
        run(&engine, "vector.status", json!({"collection":"docs"}))["sequence"],
        1
    );
    let rejection = engine
        .execute(&command(
            "search.wait",
            json!({"collection":"docs","sequence":1}),
        ))
        .unwrap_err();
    assert_eq!(rejection.code, "conflict");
    assert_eq!(
        rejection.message,
        "vector upsert was canceled by delete before indexing"
    );
    assert_eq!(run(&engine, "vector.flush", json!({}))["applied"], 0);
    assert!(run(
        &engine,
        "search.query",
        json!({"collection":"docs","name":"embedding","vector":[1.0,0.0]})
    )["hits"]
        .as_array()
        .unwrap()
        .is_empty());

    let replacement = run(
        &engine,
        "vector.upsert",
        json!({"collection":"docs","name":"embedding","id":"a","vector":[1.0,0.0],"source_revision":1}),
    );
    assert_eq!(replacement["sequence"], 2);
    assert_eq!(run(&engine, "vector.flush", json!({}))["applied"], 1);
    assert_eq!(
        run(
            &engine,
            "search.query",
            json!({"collection":"docs","name":"embedding","vector":[1.0,0.0]})
        )["hits"][0]["id"],
        "a"
    );
}

#[test]
fn vector_configuration_is_idempotent_but_immutable_with_pending_data() {
    let (_directory, engine) = engine();
    let configured = run(
        &engine,
        "vector.configure",
        json!({"collection":"docs","name":"embedding","dimensions":2,"metric":"cosine"}),
    );
    put(&engine, "a", json!({}));
    run(
        &engine,
        "vector.upsert",
        json!({"collection":"docs","name":"embedding","id":"a","vector":[1.0,0.0],"source_revision":1}),
    );
    assert_eq!(
        run(
            &engine,
            "vector.configure",
            json!({"collection":"docs","name":"embedding","dimensions":2,"metric":"cosine","kind":"dense"})
        ),
        configured
    );
    let error = engine
        .execute(&command(
            "vector.configure",
            json!({"collection":"docs","name":"embedding","dimensions":3,"metric":"cosine"}),
        ))
        .unwrap_err();
    assert_eq!(error.code, "conflict");
    assert!(error.message.contains("immutable"));
    assert_eq!(
        run(&engine, "vector.status", json!({"collection":"docs"}))["submitted"],
        1
    );
    run(&engine, "vector.flush", json!({}));
    let indexed_change = engine
        .execute(&command(
            "vector.configure",
            json!({"collection":"docs","name":"embedding","dimensions":2,"metric":"dot"}),
        ))
        .unwrap_err();
    assert_eq!(indexed_change.code, "conflict");
}

#[test]
fn batch_submission_is_atomic_and_returns_per_item_sequences() {
    let (_directory, engine) = engine();
    configure_dense(&engine);
    put(&engine, "a", json!({}));
    put(&engine, "b", json!({}));
    let too_many = (0..1001)
        .map(|_| json!({"id":"a","vector":[1.0,0.0],"source_revision":1}))
        .collect::<Vec<_>>();
    let over_limit = engine
        .execute(&command(
            "vector.upsert_batch",
            json!({"collection":"docs","name":"embedding","items":too_many}),
        ))
        .unwrap_err();
    assert_eq!(over_limit.code, "invalid_argument");
    let invalid = engine.execute(&command(
        "vector.upsert_batch",
        json!({"collection":"docs","name":"embedding","items":[
            {"id":"a","vector":[1.0,0.0],"source_revision":1},
            {"id":"b","vector":[0.0],"source_revision":1}
        ]}),
    ));
    assert_eq!(invalid.unwrap_err().code, "invalid_argument");
    let status = run(&engine, "vector.status", json!({"collection":"docs"}));
    assert_eq!(status["submitted"], 0);
    assert_eq!(status["sequence"], 0);
    assert_eq!(run(&engine, "vector.flush", json!({}))["applied"], 0);

    let batch = run(
        &engine,
        "vector.upsert_batch",
        json!({"collection":"docs","name":"embedding","items":[
            {"id":"a","vector":[1.0,0.0],"source_revision":1},
            {"id":"b","vector":[0.0,1.0],"source_revision":1}
        ]}),
    );
    assert_eq!(batch["items"][0]["sequence"], 1);
    assert_eq!(batch["items"][1]["sequence"], 2);
    assert_eq!(run(&engine, "vector.flush", json!({}))["applied"], 2);
}

#[test]
fn stale_revision_rejection_advances_visibility_and_preserves_reason() {
    let (_directory, engine) = engine();
    configure_dense(&engine);
    put(&engine, "a", json!({"version":1}));
    run(
        &engine,
        "vector.upsert",
        json!({"collection":"docs","name":"embedding","id":"a","vector":[1.0,0.0],"source_revision":1}),
    );
    put(&engine, "a", json!({"version":2}));
    assert_eq!(run(&engine, "vector.flush", json!({}))["applied"], 0);
    let rejected = engine
        .execute(&command(
            "search.wait",
            json!({"collection":"docs","sequence":1}),
        ))
        .unwrap_err();
    assert_eq!(rejected.code, "conflict");
    assert_eq!(rejected.message, "stale source revision");
    let status = run(&engine, "vector.status", json!({"collection":"docs"}));
    assert_eq!(status["sequence"], 1);
    assert_eq!(status["submitted"], 1);
}

#[test]
fn visibility_only_advances_across_contiguous_completed_sequences() {
    let (_directory, engine) = engine();
    configure_dense(&engine);
    put(&engine, "a", json!({}));
    put(&engine, "b", json!({}));
    for (id, vector) in [("a", json!([1.0, 0.0])), ("b", json!([0.0, 1.0]))] {
        run(
            &engine,
            "vector.upsert",
            json!({"collection":"docs","name":"embedding","id":id,"vector":vector,"source_revision":1}),
        );
    }
    run(
        &engine,
        "vector.delete",
        json!({"collection":"docs","name":"embedding","id":"b"}),
    );
    let blocked = run(&engine, "vector.status", json!({"collection":"docs"}));
    assert_eq!(blocked["sequence"], 0);
    assert_eq!(blocked["submitted"], 2);

    assert_eq!(run(&engine, "vector.flush", json!({}))["applied"], 1);
    let visible = run(&engine, "vector.status", json!({"collection":"docs"}));
    assert_eq!(visible["sequence"], 2);
    assert_eq!(visible["submitted"], 2);
    let rejection = engine
        .execute(&command(
            "search.wait",
            json!({"collection":"docs","sequence":2}),
        ))
        .unwrap_err();
    assert_eq!(
        rejection.message,
        "vector upsert was canceled by delete before indexing"
    );
}

#[test]
fn dense_and_sparse_search_apply_document_filters() {
    let (_directory, engine) = engine();
    configure_dense(&engine);
    run(
        &engine,
        "vector.configure",
        json!({"collection":"docs","name":"terms","dimensions":16,"metric":"dot","kind":"sparse"}),
    );
    put(&engine, "red", json!({"tenant":"red","body":"rust"}));
    put(&engine, "blue", json!({"tenant":"blue","body":"rust"}));
    for (id, vector, index) in [
        ("red", json!([1.0, 0.0]), 3),
        ("blue", json!([0.9, 0.1]), 3),
    ] {
        run(
            &engine,
            "vector.upsert",
            json!({"collection":"docs","name":"embedding","id":id,"vector":vector,"source_revision":1}),
        );
        run(
            &engine,
            "vector.upsert",
            json!({"collection":"docs","name":"terms","id":id,"vector":{"indices":[index],"values":[2.0]},"source_revision":1}),
        );
    }
    run(&engine, "vector.flush", json!({}));
    let dense = run(
        &engine,
        "search.query",
        json!({"collection":"docs","name":"embedding","vector":[1.0,0.0],"filter":{"tenant":"blue"}}),
    );
    assert_eq!(dense["hits"].as_array().unwrap().len(), 1);
    assert_eq!(dense["hits"][0]["id"], "blue");
    let sparse = run(
        &engine,
        "search.query",
        json!({"collection":"docs","name":"terms","vector":{"indices":[3],"values":[1.0]},"filter":{"tenant":"red"}}),
    );
    assert_eq!(sparse["hits"].as_array().unwrap().len(), 1);
    assert_eq!(sparse["hits"][0]["id"], "red");
    assert_eq!(sparse["hits"][0]["score"], 2.0);
}

#[test]
fn bm25_and_hybrid_rrf_have_deterministic_rankings() {
    let (_directory, engine) = engine();
    configure_dense(&engine);
    for (id, body, vector) in [
        ("a", "rust rust rust database", json!([-1.0, 0.0])),
        ("b", "rust cooking recipes", json!([1.0, 0.0])),
        ("c", "gardening notes", json!([0.8, 0.2])),
    ] {
        put(&engine, id, json!({"body":body}));
        run(
            &engine,
            "vector.upsert",
            json!({"collection":"docs","name":"embedding","id":id,"vector":vector,"source_revision":1}),
        );
    }
    run(&engine, "vector.flush", json!({}));
    let lexical = run(
        &engine,
        "search.query",
        json!({"collection":"docs","text":"rust","limit":3}),
    );
    assert_eq!(lexical["mode"], "bm25");
    assert_eq!(lexical["hits"][0]["id"], "a");
    let hybrid = run(
        &engine,
        "search.query",
        json!({"collection":"docs","name":"embedding","vector":[1.0,0.0],"text":"rust","limit":3}),
    );
    assert_eq!(hybrid["mode"], "hybrid_rrf");
    assert_eq!(hybrid["hits"][0]["id"], "b");
}

#[test]
fn approximate_graph_search_remains_deterministic_after_entry_delete() {
    let (_directory, engine) = engine();
    configure_dense(&engine);
    for index in 0..8 {
        let id = format!("v{index}");
        put(&engine, &id, json!({"index":index}));
        run(
            &engine,
            "vector.upsert",
            json!({"collection":"docs","name":"embedding","id":id,"vector":[1.0,index as f64 + 1.0],"source_revision":1}),
        );
    }
    run(&engine, "vector.flush", json!({}));
    run(
        &engine,
        "vector.delete",
        json!({"collection":"docs","name":"embedding","id":"v7"}),
    );
    let query = json!({"collection":"docs","name":"embedding","vector":[1.0,8.0],"exact":false,"ef":7,"limit":7});
    let first = run(&engine, "search.query", query.clone());
    let second = run(&engine, "search.query", query);
    assert_eq!(first, second);
    assert_eq!(first["mode"], "graph_approximate");
    assert_eq!(first["hits"].as_array().unwrap().len(), 7);
    assert!(first["hits"]
        .as_array()
        .unwrap()
        .iter()
        .all(|hit| hit["id"] != "v7"));
}

#[test]
fn vector_numeric_range_prevents_nonfinite_scores() {
    let (_dir, engine) = engine();
    configure_dense(&engine);
    put(&engine, "large", json!({}));
    for value in [1e308, 1e-300] {
        let error=engine.execute(&command("vector.upsert",json!({"collection":"docs","name":"embedding","id":"large","vector":[value,0.0],"source_revision":1}))).unwrap_err();
        assert_eq!(error.code, "invalid_argument");
    }
    run(
        &engine,
        "vector.upsert",
        json!({"collection":"docs","name":"embedding","id":"large","vector":[3e38,0.0],"source_revision":1}),
    );
    run(&engine, "vector.flush", json!({}));
    let results = run(
        &engine,
        "search.query",
        json!({"collection":"docs","name":"embedding","vector":[3e38,0.0],"limit":1000}),
    );
    assert_eq!(results["hits"][0]["score"], 1.0);
}
