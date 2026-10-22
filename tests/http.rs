use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    Router,
};
use serde_json::{json, Value};
use substratedb::{
    auth::{self, Capability},
    runtime::Runtime,
    store::Engine,
};
use tower::ServiceExt;
const ADMIN: &str = "a-long-test-administrator-token";
async fn call(
    router: &Router,
    token: &str,
    namespace: &str,
    partition: &str,
    action: &str,
    payload: Value,
) -> (StatusCode, Value) {
    let response=router.clone().oneshot(Request::post("/v1/command").header("Authorization",format!("Bearer {token}")).header("Content-Type","application/json").body(Body::from(json!({"namespace":namespace,"partition":partition,"action":action,"payload":payload}).to_string())).unwrap()).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 4 * 1024 * 1024)
        .await
        .unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}
#[tokio::test]
async fn service_permissions_bind_namespace_partition_resource_and_operation() {
    let d = tempfile::tempdir().unwrap();
    let r = substratedb::server::router(Runtime::new(
        Engine::open(d.path().join("db")).unwrap(),
        ADMIN.into(),
        b"secret".to_vec(),
    ));
    let c = Capability {
        id: "service".into(),
        namespace: "app".into(),
        partition: "p".into(),
        resources: vec!["collection:docs".into()],
        permissions: vec!["read".into()],
        expires_at_ms: substratedb::runtime::now_ms() + 60000,
        filter: Value::Null,
    };
    let token = format!("svc.{}", auth::sign(b"secret", &c).unwrap());
    assert_eq!(
        call(
            &r,
            ADMIN,
            "app",
            "p",
            "document.put",
            json!({"collection":"docs","id":"one","value":{"ok":true}})
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        call(
            &r,
            &token,
            "app",
            "p",
            "document.get",
            json!({"collection":"docs","id":"one"})
        )
        .await
        .0,
        StatusCode::OK
    );
    for (ns, part, action, payload) in [
        (
            "other",
            "p",
            "document.get",
            json!({"collection":"docs","id":"one"}),
        ),
        (
            "app",
            "other",
            "document.get",
            json!({"collection":"docs","id":"one"}),
        ),
        (
            "app",
            "p",
            "document.get",
            json!({"collection":"private","id":"one"}),
        ),
        (
            "app",
            "p",
            "document.delete",
            json!({"collection":"docs","id":"one"}),
        ),
        (
            "app",
            "p",
            "transaction",
            json!({"commands":[{"action":"document.delete","payload":{"collection":"docs","id":"one"}}]}),
        ),
    ] {
        assert_eq!(
            call(&r, &token, ns, part, action, payload).await.0,
            StatusCode::FORBIDDEN
        );
    }
    call(
        &r,
        ADMIN,
        "app",
        "p",
        "auth.revoke",
        json!({"id":"service"}),
    )
    .await;
    assert_eq!(
        call(
            &r,
            &token,
            "app",
            "p",
            "document.get",
            json!({"collection":"docs","id":"one"})
        )
        .await
        .0,
        StatusCode::UNAUTHORIZED
    );
}
#[tokio::test]
async fn scatter_merges_vector_scores_and_rejects_schema_mismatch() {
    let d = tempfile::tempdir().unwrap();
    let r = substratedb::server::router(Runtime::new(
        Engine::open(d.path().join("db")).unwrap(),
        ADMIN.into(),
        b"secret".to_vec(),
    ));
    for (part, metric, v) in [
        ("a", "cosine", json!([0, 1])),
        ("b", "cosine", json!([1, 0])),
        ("wrong", "dot", json!([1, 0])),
    ] {
        for (action, payload) in [
            (
                "document.put",
                json!({"collection":"docs","id":"same-id","value":{}}),
            ),
            (
                "vector.configure",
                json!({"collection":"docs","name":"v","dimensions":2,"metric":metric}),
            ),
            (
                "vector.upsert",
                json!({"collection":"docs","name":"v","id":"same-id","vector":v,"source_revision":1}),
            ),
            ("vector.flush", json!({})),
        ] {
            assert_eq!(
                call(&r, ADMIN, "app", part, action, payload).await.0,
                StatusCode::OK
            );
        }
    }
    let query = json!({"collection":"docs","name":"v","vector":[1,0],"limit":2});
    let (status, result) = call(
        &r,
        ADMIN,
        "app",
        "unused",
        "search.scatter",
        json!({"partitions":["a","b"],"query":query}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["result"]["hits"][0]["partition"], "b");
    assert_eq!(result["result"]["hits"].as_array().unwrap().len(), 2);
    assert_eq!(
        call(
            &r,
            ADMIN,
            "app",
            "unused",
            "search.scatter",
            json!({"partitions":["a","wrong"],"query":query})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
}
