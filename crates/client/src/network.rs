// Responsibilities of this module:
//1. connect to server
//2. send messages
//3. receive messages

use std::error::Error;
use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use common::protocols::{ClientMessage, ServerMessage};

pub async fn run() -> Result<(), Box<dyn Error>>{
    let url = "ws://127.0.0.1:8080/ws";

    println!("Connecting to {}", url);

    let (mut socket, response) = connect_async(url).await?;

    println!("Connected to server! HTTP status: {}", response.status());

    let message = ClientMessage::TextMessage {
        content: "Hello from Rust client!".to_string(),
    };

    println!("Rust message: {:?}", message);
    let json = serde_json::to_string(&message)?;
    println!("Serialized JSON: {}", json);
    socket.send(Message::Text(json.into())).await?;
    println!("Message sent!");

    if let Some(result) = socket.next().await {
        match result {
            Ok(Message::Text(text)) => {
                println!("Raw server response: {}", text);

                let server_message : ServerMessage = serde_json::from_str(&text)?;

                match server_message {
                    ServerMessage::TextMessage { content } => {
                        println!("Server says: {}", content);
                    }

                    ServerMessage::RegistrationSuccessful => {
                        println!("Registration successful.");
                    }

                    ServerMessage::LoginSuccessful => {
                        println!("Login successful.");
                    }

                    ServerMessage::Error { message } => {
                        eprintln!("Server error: {}", message);
                    }
                }
            }
            Ok(other) => {
                println!("Received non-text Websocket message {}", other);
            }
            Err(error) => {
                return Err(error.into());
            }
        }
    }
    else {
        println!("Server closed the connection");
    }
    Ok(())
}