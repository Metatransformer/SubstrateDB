//! Static administration UI. Credentials are supplied by the operator to the existing API.
use axum::{
    extract::Path,
    http::{header, StatusCode},
    response::{IntoResponse, Redirect, Response},
    routing::get,
    Router,
};

include!(concat!(env!("OUT_DIR"), "/dashboard_assets.rs"));

pub fn router<S: Clone + Send + Sync + 'static>() -> Router<S> {
    Router::new()
        .route("/", get(|| async { Redirect::temporary("/dashboard") }))
        .route("/dashboard", get(index))
        .route("/dashboard/", get(index))
        .route("/dashboard/{*path}", get(asset))
}

async fn index() -> Response {
    serve("index.html")
}

async fn asset(Path(path): Path<String>) -> Response {
    if let Some(name) = path.strip_prefix("docs/") {
        return document(name);
    }
    serve(&path)
}

fn response(body: &'static [u8], content_type: &'static str) -> Response {
    (
        [
            (header::CONTENT_TYPE, content_type),
            (header::CACHE_CONTROL, "no-cache"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            (header::X_FRAME_OPTIONS, "DENY"),
            (header::REFERRER_POLICY, "no-referrer"),
        ],
        body,
    )
        .into_response()
}

fn serve(path: &str) -> Response {
    let Some((_, bytes)) = ASSETS.iter().find(|(name, _)| *name == path) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let content_type = match path.rsplit('.').next() {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("json") => "application/json",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        Some("woff2") => "font/woff2",
        _ => "text/plain; charset=utf-8",
    };
    response(bytes, content_type)
}

fn document(name: &str) -> Response {
    let content: &'static str = match name {
        "" | "README.md" => include_str!("../README.md"),
        "QUICKSTART.md" => include_str!("../docs/QUICKSTART.md"),
        "DASHBOARD.md" => include_str!("../docs/DASHBOARD.md"),
        "PROTOCOL.md" => include_str!("../docs/PROTOCOL.md"),
        "OPERATIONS.md" => include_str!("../docs/OPERATIONS.md"),
        "SEARCH.md" => include_str!("../docs/SEARCH.md"),
        "CLUSTER.md" => include_str!("../docs/CLUSTER.md"),
        "IMPLEMENTATION.md" => include_str!("../docs/IMPLEMENTATION.md"),
        "SDK.md" => include_str!("../sdk/README.md"),
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    response(content.as_bytes(), "text/plain; charset=utf-8")
}
