use std::fmt::format;
use std::sync::Arc;
use axum::extract::{State, WebSocketUpgrade};
use axum::extract::ws::{Message, WebSocket};
use axum::response::Response;
use common::protocols::{ClientMessage, ServerMessage};
use crate::state::AppState;

pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> Response {
    ws.on_upgrade(move |socket| {
        handle_socket(socket, state)
    })
}

async fn send_message(socket: &mut WebSocket, message: &ServerMessage) -> Result<(), axum::Error> {
    let json = serde_json::to_string(message).expect("Server message should be serializable");
    socket.send(Message::Text(json.into())).await
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    let mut authenticated_user: Option<String> = None;
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
                                        let mut users = state.users.write().await;
                                        if users.contains_key(&username) {
                                            let response = ServerMessage::Error {
                                                message: format!("Username '{}' already registered", username),
                                            };

                                            if send_message(&mut socket, &response).await.is_err() {
                                                break;
                                            }
                                        }
                                        else {
                                            users.insert(
                                                username.clone(),
                                                password,
                                            );
                                            drop(users);
                                            let response = ServerMessage::RegistrationSuccessful;
                                            if send_message(&mut socket, &response).await.is_err() {
                                                break;
                                            }
                                        }
                                    }
                                    ClientMessage::Login { username, password} => {
                                        let users = state.users.read().await;
                                        let valid = match users.get(&username) {
                                            Some(stored_password) => stored_password == &password,
                                            None => false,
                                        };

                                        if valid {
                                            authenticated_user = Some(username.clone());
                                            println!("{} authenticated", username);
                                            let response = ServerMessage::LoginSuccessful;

                                            if send_message(&mut socket, &response).await.is_err() {
                                                break;
                                            }
                                        }
                                        else {
                                            let response = ServerMessage::Error {
                                                message: "Invalid username or password".to_string(),
                                            };
                                            if send_message(&mut socket, &response).await.is_err() {
                                                break;
                                            }
                                        }
                                    }
                                    ClientMessage::PositionUpdate { position } => {
                                        match authenticated_user.as_ref() {
                                            None => {
                                                let response = ServerMessage::Error {
                                                    message: "You must login first".to_string(),
                                                };

                                                if send_message(&mut socket, &response).await.is_err() {
                                                    break;
                                                }
                                            }
                                            Some(username) => {
                                                println!("Position from {}: {}, {}",
                                                username,
                                                position.latitude,
                                                position.longitude);
                                            }
                                        }
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