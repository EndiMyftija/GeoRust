// Responsibilities of this module:
//1. connect to server
//2. send messages
//3. receive messages

type ClientSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

use std::error::Error;
use std::io::ErrorKind;
use std::time::Duration;
use futures_util::{SinkExt, StreamExt};
use tokio::io;
use tokio::io::{AsyncBufReadExt, BufReader, Lines, Stdin};
use tokio::net::TcpStream;
use tokio::sync::mpsc::channel;
use tokio_tungstenite::{connect_async, WebSocketStream, MaybeTlsStream};
use tokio_tungstenite::tungstenite::Message;
use common::models::Position;
use common::protocols::{ClientMessage, ServerMessage};

pub async fn run() -> Result<(), Box<dyn Error>>{
    let url = "ws://127.0.0.1:8080/ws";

    println!("Connecting to {}", url);

    let (mut socket, response) = connect_async(url).await?;

    println!("Connected to server");

    let stdin = io::stdin();
    let reader = BufReader::new(stdin);
    let mut lines = reader.lines();

    authenticate(&mut socket, &mut lines).await?;

    println!("Authenticated. Starting client...");

    let (mut write, mut read) = socket.split();
    let (tx, mut rx) = channel::<ClientMessage>(32);

    let tx_keyboard = tx.clone();
    let tx_position = tx.clone();

    println!("Connected to server! HTTP status: {}", response.status());

    let receiver_task = tokio::spawn(async move {
        while let Some(result) = read.next().await {
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
                    eprintln!("Failed to serialize client message: {}", error);
                    continue;
                }
            };

            if let Err(error) =write.send(Message::Text(json.into())).await {
                eprintln!("Failed to send WebSocket message: {}", error);
                break;
            }
        }
    });


    let keyboard_task = tokio::spawn(async move {
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

async fn read_input(lines: &mut Lines<BufReader<Stdin>>, prompt: &str) -> Result<String, Box<dyn Error>> {
    println!("{}", prompt);
    let line = lines.next_line().await?;

    match line {
        None => {Err(Box::new(std::io::Error::new(ErrorKind::UnexpectedEof, "Unexpected EOF")))}
        Some(line) => {Ok(line.trim().to_string())}
    }
}

async fn send_client_message(
    socket: &mut ClientSocket,
    message: &ClientMessage,
) -> Result<(), Box<dyn Error>> {
    let json = serde_json::to_string(message)?;

    socket
        .send(Message::Text(json.into()))
        .await?;

    Ok(())
}

async fn receive_server_message(socket: &mut ClientSocket) -> Result<ServerMessage, Box<dyn Error>> {
    loop {
        match socket.next().await {
            None => {
                return Err(Box::new(std::io::Error::new(ErrorKind::ConnectionAborted, "Connection Aborted")))
            }
            Some(result) => {
                match result {
                    Ok(msg) => {
                        match msg {
                            Message::Text(text) => {
                                let message: ServerMessage = serde_json::from_str(text.as_str())?;
                                return Ok(message);
                            }
                            Message::Close(_) => {
                                return Err(Box::new(std::io::Error::new(ErrorKind::ConnectionAborted, "Server closed the connection")));
                            }
                            _ => continue,
                        }
                    }
                    Err(err) => {
                        return Err(Box::new(err));
                    }
                }
            }
        }
    }
}

async fn authenticate(
    socket: &mut ClientSocket,
    lines: &mut Lines<BufReader<Stdin>>,
) -> Result<(), Box<dyn Error>> {

    loop {
        println!();
        println!("1 - Register");
        println!("2 - Login");

        let choice = read_input(lines, "Please enter your option").await?;

        if choice != "1" && choice != "2" {
            println!("Invalid option.");
            continue;
        }

        let username = read_input(lines, "Please enter your username").await?;
        let password = read_input(lines, "Please enter your password").await?;

        match choice.as_str() {
            "1" => {
                // Register
                // Register here means register + automatic login so we don't have to undergo login again.
                let register_message = ClientMessage::Register {
                    username: username.clone(),
                    password: password.clone(),
                };

                send_client_message(socket, &register_message).await?;
                let message = receive_server_message(socket).await?;
                match message {
                    ServerMessage::RegistrationSuccessful => {
                        // Login immediately
                        let login_message = ClientMessage::Login {
                            username: username,
                            password: password,
                        };
                        send_client_message(socket, &login_message).await?;
                        match receive_server_message(socket).await? {
                            ServerMessage::LoginSuccessful => {
                                println!("Login successful.");
                                return Ok(());
                            }
                            ServerMessage::Error { message } => {
                                eprintln!("Login failed after registration: {}", message);
                            }
                            other => {
                                println!("Unexpected response from server {:?}", other);
                            }
                        }
                    }
                    ServerMessage::Error { message } => {
                        eprintln!("Registration failed: {}", message);
                    }
                    other => {
                        println!("Unexpected registration response {:?}", other);
                    }
                }
            }
            "2" => {
                // Login
                let message = ClientMessage::Login {
                    username: username,
                    password: password,
                };

                send_client_message(socket, &message).await?;

                let response = receive_server_message(socket).await?;

                match response {
                    ServerMessage::LoginSuccessful => {
                        println!("Login successful.");
                        return Ok(());
                    }
                    ServerMessage::Error { message } => {
                        eprintln!("Server error: {}", message);
                    }
                    other => {
                        println!("Unexpected server response: {:?}", other);
                    }
                }
            },
            _ => unreachable!("This should not happen since we check that choice can only be 1 or 2")
        }
    }
}