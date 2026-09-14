mod ws;
mod state;

use std::sync::Arc;
use axum::Router;
use axum::routing::get;
use tokio::net::TcpListener;
use crate::state::AppState;

async fn health() -> &'static str {
    "OK"
}

#[tokio::main]
async fn main() {

    let state = Arc::new(AppState::new());

    let app = Router::new()
        .route("/health", get(health))
        .route("/ws", get(ws::ws_handler))
        .with_state(state);

    let listener = TcpListener::bind("127.0.0.1:8080")
        .await
        .unwrap();

    println!("Server running on http://127.0.0.1:8080");

    axum::serve(listener, app)
        .await
        .unwrap();
}