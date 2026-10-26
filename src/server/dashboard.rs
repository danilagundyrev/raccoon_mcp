use super::*;
use axum::extract::{Path as UrlPath, Query};
use serde::Deserialize;

#[derive(Deserialize)]
pub(super) struct SnapshotQuery {
    project_id: Option<String>,
}

pub(super) async fn index() -> Response {
    asset(
        "text/html; charset=utf-8",
        include_str!("../../raccoon_mcp_web/index.html"),
    )
}

pub(super) async fn assets(UrlPath(file): UrlPath<String>) -> Response {
    match file.as_str() {
        "app.js" => asset(
            "text/javascript; charset=utf-8",
            include_str!("../../raccoon_mcp_web/app.js"),
        ),
        "model.js" => asset(
            "text/javascript; charset=utf-8",
            include_str!("../../raccoon_mcp_web/model.js"),
        ),
        "demo.js" => asset(
            "text/javascript; charset=utf-8",
            include_str!("../../raccoon_mcp_web/demo.js"),
        ),
        "style.css" => asset(
            "text/css; charset=utf-8",
            include_str!("../../raccoon_mcp_web/style.css"),
        ),
        "favicon.svg" => asset(
            "image/svg+xml",
            include_str!("../../raccoon_mcp_web/favicon.svg"),
        ),
        _ => not_found().await,
    }
}

fn asset(content_type: &'static str, body: &'static str) -> Response {
    Response::builder().status(StatusCode::OK)
        .header(CONTENT_TYPE, content_type).header(CACHE_CONTROL, "no-cache")
        .header("X-Content-Type-Options", "nosniff")
        .header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
        .body(Body::from(body)).unwrap()
}

fn observer_allowed(headers: &HeaderMap, state: &AppState) -> bool {
    headers.get(ORIGIN).map_or(true, |origin| {
        origin
            .to_str()
            .is_ok_and(|origin| origin_allowed(origin, &state.allowed_hosts))
    }) && !headers
        .get("sec-fetch-site")
        .is_some_and(|value| value == "cross-site")
}

pub(super) async fn snapshot(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<SnapshotQuery>,
) -> Response {
    if !observer_allowed(&headers, &state) {
        return json_response(
            StatusCode::FORBIDDEN,
            json!({"error": "Origin not allowed"}),
        );
    }
    if query
        .project_id
        .as_deref()
        .is_some_and(|id| uuid::Uuid::parse_str(id).is_err())
    {
        return json_response(
            StatusCode::BAD_REQUEST,
            json!({"error": "Invalid project_id"}),
        );
    }
    match state
        .store
        .dashboard_snapshot(query.project_id.as_deref())
        .await
    {
        Ok(mut snapshot) => {
            snapshot["version"] = json!(SERVER_VERSION);
            json_response(StatusCode::OK, snapshot)
        }
        Err(error) => {
            tracing::warn!(%error, "dashboard snapshot failed");
            json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"error": "Cannot read coordination state. Retrying shortly."}),
            )
        }
    }
}

pub(super) async fn artifact(
    State(state): State<AppState>,
    headers: HeaderMap,
    UrlPath(id): UrlPath<String>,
) -> Response {
    if !observer_allowed(&headers, &state) {
        return json_response(
            StatusCode::FORBIDDEN,
            json!({"error": "Origin not allowed"}),
        );
    }
    if uuid::Uuid::parse_str(&id).is_err() {
        return json_response(
            StatusCode::BAD_REQUEST,
            json!({"error": "Invalid artifact ID"}),
        );
    }
    match state.store.dashboard_artifact(&id).await {
        Ok(Some(artifact)) => json_response(StatusCode::OK, artifact),
        Ok(None) => not_found().await,
        Err(error) => {
            tracing::warn!(%error, "dashboard artifact read failed");
            json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                json!({"error": "Cannot load artifact"}),
            )
        }
    }
}
