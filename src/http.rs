//! HTTP server implementation.
//!
//! Provides core HTTP endpoints and a simple state pattern that can be extended.
//! Sinks can add their own routes via the `Sink::build_routes()` method.

use axum::{extract::State, response::Json, routing::get, Router};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// HTTP server state.
///
/// This is a simple example showing how to use Axum state in HTTP handlers.
/// We can extend this as Torii grows.
#[derive(Clone)]
pub struct HttpState {
    pub version: String,
    pub startup_time: i64,
}

impl HttpState {
    pub fn new() -> Self {
        Self {
            version: env!("CARGO_PKG_VERSION").to_string(),
            startup_time: chrono::Utc::now().timestamp(),
        }
    }
}

impl Default for HttpState {
    fn default() -> Self {
        Self::new()
    }
}

/// Health check response.
#[derive(Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub uptime_seconds: i64,
}

/// Health check endpoint.
async fn health_handler(State(state): State<Arc<HttpState>>) -> Json<HealthResponse> {
    let now = chrono::Utc::now().timestamp();
    let uptime = now - state.startup_time;

    Json(HealthResponse {
        status: "healthy".to_string(),
        version: state.version.clone(),
        uptime_seconds: uptime,
    })
}

/// Create the core HTTP router with basic endpoints.
pub fn create_http_router() -> Router {
    let state = Arc::new(HttpState::new());

    Router::new()
        .route("/health", get(health_handler))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_health_endpoint() {
        let app = create_http_router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let health_response: HealthResponse = serde_json::from_slice(&body).unwrap();

        assert_eq!(health_response.status, "healthy");
        assert!(health_response.uptime_seconds >= 0);
    }
}
