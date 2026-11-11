//! Publishes alerts to an ntfy server.

use std::time::Duration;

use serde_json::json;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::{config::NtfyConfig, rules::Alert};

const ATTEMPTS: u32 = 3;

#[derive(Clone)]
pub struct Ntfy {
    client: reqwest::Client,
    url: String,
    topic: String,
    token: Option<String>,
}

impl Ntfy {
    /// Reads the publish token from the configured environment variable.
    pub fn new(config: &NtfyConfig) -> Self {
        let token = std::env::var(&config.token_env).ok();
        if token.is_none() {
            warn!(
                "{} is unset, so ntfy publishes have no token",
                config.token_env
            );
        }
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("the TLS backend loads"),
            url: config.url.trim_end_matches('/').to_owned(),
            topic: config.topic.clone(),
            token,
        }
    }

    /// Publishes one alert, with retries.
    pub async fn publish(&self, alert: &Alert) -> Result<(), String> {
        let body = json!({
            "topic": self.topic,
            "title": alert.title,
            "message": alert.message,
            "priority": alert.priority.ntfy(),
            "tags": [alert.rule],
        });

        let mut last_error = String::new();
        for attempt in 0..ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_secs(5 << attempt)).await;
            }
            let mut request = self.client.post(&self.url).json(&body);
            if let Some(token) = &self.token {
                request = request.bearer_auth(token);
            }
            match request.send().await {
                Ok(response) if response.status().is_success() => return Ok(()),
                Ok(response) => last_error = format!("HTTP {}", response.status()),
                Err(e) => last_error = e.to_string(),
            }
        }
        Err(last_error)
    }

    /// Starts a task that publishes queued alerts in order.
    pub fn spawn(self) -> mpsc::UnboundedSender<Alert> {
        let (tx, mut rx) = mpsc::unbounded_channel::<Alert>();
        tokio::spawn(async move {
            while let Some(alert) = rx.recv().await {
                match self.publish(&alert).await {
                    Ok(()) => {
                        info!("published {} alert", alert.rule);
                        metrics::increment_counter!("mempool_alerts_published_total");
                    }
                    Err(e) => {
                        warn!("ntfy publish of {} alert failed: {e}", alert.rule);
                        metrics::increment_counter!("mempool_ntfy_errors_total");
                    }
                }
            }
        });
        tx
    }
}
