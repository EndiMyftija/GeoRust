use serde::{Deserialize, Serialize};
use crate::models::Position;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ClientMessage {
    Register {
        username: String,
        password: String
    },

    Login {
        username: String,
        password: String
    },

    PositionUpdate {
        position: Position
    },

    TextMessage {
        content: String,
    },
}


#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum ServerMessage {
    RegistrationSuccessful,

    LoginSuccessful,

    Error {
        message: String,
    },

    TextMessage {
        content: String,
    },
}