//! The password login of a [`Client`], for the tests whose app has one.

use serde_json::json;

use crate::support::{Client, Reply};

impl Client {
    pub async fn log_in_at(&mut self, uri: &str, email: &str, password: &str) -> Reply {
        self.post(uri, Some(json!({ "email": email, "password": password })))
            .await
    }

    pub async fn log_in(&mut self, email: &str, password: &str) -> Reply {
        self.log_in_at("/login", email, password).await
    }
}
