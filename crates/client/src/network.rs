// Responsibilities of this module:
//1. connect to server
//2. send messages
//3. receive messages


use std::error::Error;
use std::time::Duration;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{stdin, AsyncBufReadExt, BufReader};
use tokio::sync::mpsc::channel;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use common::models::Position;
use common::protocols::{ClientMessage, ServerMessage};

pub async fn run() -> Result<(), Box<dyn Error>>{
    let url = "ws://127.0.0.1:8080/ws";

    println!("Connecting to {}", url);

    let (socket, response) = connect_async(url).await?;
    let (mut writer, mut reader) = socket.split();
    let (tx, mut rx) = channel::<ClientMessage>(32);

    let tx_keyboard = tx.clone();
    let tx_position = tx.clone();

    println!("Connected to server! HTTP status: {}", response.status());

    tx.send(ClientMessage::TextMessage {
        content: "Hello from Rust client".to_string()
    }).await?;

    println!("Message sent!");

    let receiver_task = tokio::spawn(async move {
        while let Some(result) = reader.next().await {
            match result {
                Ok(Message::Text(text)) => {
                    match serde_json::from_str::<ServerMessage>(text.as_str()) {
                        Ok(server_message) => {
                            match server_message {
                                ServerMessage::TextMessage { content } => {
                                    println!("Server: {}", content);
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
                        Err(error) => {
                            eprintln!("Invalid message from server: {}", error);
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    println!("Server closed the connection.");
                    break;
                }
                Err(error) => {
                    eprintln!("Receive error {}", error);
                    break;
                }
                Ok(_) => {},
            }
        }
    });


    let writer_task = tokio::spawn(async move {
        while let Some(client_message) = rx.recv().await {
            let json = match serde_json::to_string(&client_message) {
                Ok(json) => json,
                Err(error) => {
                    eprintln!("Failed to deserialize client message: {}", error);
                    continue;
                }
            };

            if let Err(error) =writer.send(Message::Text(json.into())).await {
                eprintln!("Failed to send WebSocket message: {}", error);
                break;
            }
        }
    });


    let keyboard_task = tokio::spawn(async move {
        let stdin = stdin();
        let reader = BufReader::new(stdin);
        let mut lines = reader.lines();

        println!("Type messages and press Enter:");

        while let Ok(Some(line)) = lines.next_line().await {
            if line.trim().is_empty() {
                continue;
            }

            let message = ClientMessage::TextMessage {
                content: line,
            };

            if tx_keyboard.send(message).await.is_err() {
                break;
            }
        }
    });

    let position_task = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        let mut latitude = 45.0703;
        let mut longitude = 7.6869;

        loop {
            interval.tick().await;

            let message = ClientMessage::PositionUpdate {
                position: Position {
                    latitude,
                    longitude
                }
            };

            if tx_position.send(message).await.is_err() {
                break;
            }

            longitude += 0.001;
            latitude += 0.001;
        }
    });

    receiver_task.await?;
    keyboard_task.abort();
    position_task.abort();
    writer_task.abort();
    Ok(())
}