mod network;

#[tokio::main]
async fn main() {
    if let Err(error) = network::run().await {
        eprintln!("Client error {}", error);
    }
}