use axum::Router;
use axum::routing::get;
use tokio::net::TcpListener;

async fn health() -> &'static str {
    "OK"
}

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/health", get(health));

    let listener = TcpListener::bind("127.0.0.1:8080")
        .await
        .unwrap();

    println!("Server running on http://127.0.0.1:8080");

    axum::serve(listener, app)
        .await
        .unwrap();
}