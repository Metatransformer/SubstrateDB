use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use substratedb::{runtime::Runtime, store::Engine};
use tower::ServiceExt;

#[tokio::test]
async fn dashboard_assets_and_offline_docs_share_the_authenticated_runtime() {
    let temp = tempfile::tempdir().unwrap();
    let app = substratedb::server::router(Runtime::new(
        Engine::open(temp.path().join("db")).unwrap(),
        "private-administrator-test-token".into(),
        b"test-signing-key".to_vec(),
    ));
    for path in [
        "/dashboard",
        "/dashboard/",
        "/dashboard/docs/QUICKSTART.md",
        "/dashboard/docs/PROTOCOL.md",
    ] {
        let response = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");
        let bytes = to_bytes(response.into_body(), 4 * 1024 * 1024)
            .await
            .unwrap();
        assert!(!bytes.is_empty());
        assert!(!String::from_utf8_lossy(&bytes).contains("private-administrator-test-token"));
    }
    let index = app
        .clone()
        .oneshot(Request::get("/dashboard").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let html = String::from_utf8(
        to_bytes(index.into_body(), 4 * 1024 * 1024)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    let mut scripts = 0;
    for part in html.split("src=\"").skip(1) {
        let path = part.split('"').next().unwrap();
        if path.starts_with("/dashboard/") {
            let asset = app
                .clone()
                .oneshot(Request::get(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(asset.status(), StatusCode::OK, "{path}");
            assert!(asset.headers()["content-type"]
                .to_str()
                .unwrap()
                .starts_with("text/javascript"));
            scripts += 1;
        }
    }
    assert!(
        scripts > 0,
        "dashboard must include working bundled scripts"
    );
    for path in [
        "/dashboard/missing.js",
        "/dashboard/docs/missing.md",
        "/dashboard/docs/../../Cargo.toml",
    ] {
        assert_eq!(
            app.clone()
                .oneshot(Request::get(path).body(Body::empty()).unwrap())
                .await
                .unwrap()
                .status(),
            StatusCode::NOT_FOUND
        );
    }
    let unauthorized = app.oneshot(Request::post("/v1/command")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"namespace":"app","partition":"default","action":"scope.stats","payload":{}}"#)).unwrap()).await.unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
}
