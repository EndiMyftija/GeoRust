use axum::Error;
use axum::extract::WebSocketUpgrade;
use axum::extract::ws::{Message, WebSocket};
use axum::response::Response;
use common::protocols::{ClientMessage, ServerMessage};

pub async fn ws_handler(ws: WebSocketUpgrade) -> Response {
    ws.on_upgrade(handle_socket)
}

async fn send_message(socket: &mut WebSocket, message: &ServerMessage) -> Result<(), axum::Error> {
    let json = serde_json::to_string(message).expect("Server message should be serializable");
    socket.send(Message::Text(json.into())).await
}

async fn handle_socket(mut socket: WebSocket) {
    println!("A websocket client connected");

    while let Some(result) = socket.recv().await {
        match result {
            Ok(message) => {
                match message {
                    Message::Text(text) => {
                        println!("Raw message from client: {}", text);

                        match serde_json::from_str::<ClientMessage>(text.as_str()) {
                            Ok(client_message) => {
                                match client_message {
                                    ClientMessage::Register { username, password } => {
                                        println!("Registration requested for {}", username);
                                    }
                                    ClientMessage::Login { username, password} => {
                                        println!("Login requested for {}", username);
                                    }
                                    ClientMessage::PositionUpdate { position } => {
                                        println!("Position received: {} {}", position.latitude ,position.longitude)
                                    }
                                    ClientMessage::TextMessage { content } => {
                                        let response = ServerMessage::TextMessage {
                                            content: format!("Echo {}", content),
                                        };
                                        if let Err(_) = send_message(&mut socket, &response).await {
                                            break;
                                        }
                                    }
                                }
                            }
                            Err(err) => {
                                let response = ServerMessage::Error {message: format!("Failed to deserialize client message")};
                                if send_message(&mut socket, &response).await.is_err() {
                                    break;
                                }
                            }
                        }
                    },
                    Message::Close(_) => {
                        println!("Client requested to close the connection");
                        break;
                    },
                    _ => {
                        println!("Received a non-text websocket message");
                    },
                }
            }
            Err(err) => {
                println!("Websocket error: {:?}", err);
                break;
            }
        }
    }

    println!("Websocket connection closed");
}