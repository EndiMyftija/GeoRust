use std::collections::HashMap;
use tokio::sync::RwLock;

// Instead of passing around 10 independent global things later such as users, connected_clients, database, configuration, etc
// We create one application-state object called AppState
// The users field will store all registered users, specifically their username and associated password
pub struct AppState {
    pub users: RwLock<HashMap<String, String>>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            users: RwLock::new(HashMap::new()),
        }
    }
}