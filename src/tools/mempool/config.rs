//! The listener's TOML config. Every field has a default, so an empty file works.

use std::{net::SocketAddr, path::PathBuf};

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub listener: ListenerConfig,
    /// Alerts go to ntfy only when this section is present.
    pub ntfy: Option<NtfyConfig>,
    pub rules: RulesConfig,
}

impl Config {
    pub fn load(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        let text = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&text)?)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ListenerConfig {
    /// The newest `census-*.ndjson.gz` in this directory supplies the peers.
    pub census_dir: PathBuf,
    /// Peers to use instead of the census, as `ip:port`.
    pub peers: Vec<SocketAddr>,
    /// How many peers to keep connected.
    pub target_peers: usize,
    pub data_dir: PathBuf,
    pub user_agent: String,
    pub metrics_addr: Option<SocketAddr>,
    pub mempool_poll_secs: u64,
    pub getdata_timeout_secs: u64,
    /// A tx missing from every snapshot and inv for this long gets a `gone` event.
    pub gone_after_secs: u64,
    /// Zebra drops a new connection from an IP it saw recently, so wait this long.
    pub reconnect_secs: u64,
    /// Replace a peer after this many failed connects in a row.
    pub max_failures: u32,
}

impl Default for ListenerConfig {
    fn default() -> Self {
        Self {
            census_dir: "data/census".into(),
            peers: Vec::new(),
            target_peers: 50,
            data_dir: "data/mempool".into(),
            user_agent: "/zero-mempool:0.1.0/".into(),
            metrics_addr: None,
            mempool_poll_secs: 60,
            getdata_timeout_secs: 30,
            gone_after_secs: 600,
            reconnect_secs: 120,
            max_failures: 1,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NtfyConfig {
    /// The server base URL, such as `https://alerts.shieldedinfra.net`.
    pub url: String,
    pub topic: String,
    /// The environment variable that holds the publish token.
    #[serde(default = "default_token_env")]
    pub token_env: String,
}

fn default_token_env() -> String {
    "NTFY_TOKEN".into()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RulesConfig {
    /// Each alert shape fires at most once per cooldown.
    pub cooldown_secs: u64,
    pub burst: BurstConfig,
    pub under_floor: UnderFloorConfig,
    pub depth: DepthConfig,
    pub large_tx: LargeTxConfig,
    pub blind: BlindConfig,
    pub heartbeat: HeartbeatConfig,
}

impl Default for RulesConfig {
    fn default() -> Self {
        Self {
            cooldown_secs: 3600,
            burst: Default::default(),
            under_floor: Default::default(),
            depth: Default::default(),
            large_tx: Default::default(),
            blind: Default::default(),
            heartbeat: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BurstConfig {
    /// The fewest txs of one shape inside the window that can fire.
    pub count: usize,
    pub window_secs: u64,
    /// Fire only above this many times the shape's usual count per window.
    pub factor: f64,
    /// The usual count comes from this span before the window. No burst fires until the
    /// listener has run this long.
    pub baseline_secs: u64,
    /// Sizes inside the same bucket count as the same shape.
    pub size_bucket_bytes: usize,
}

impl Default for BurstConfig {
    fn default() -> Self {
        Self {
            count: 5,
            window_secs: 600,
            factor: 3.0,
            baseline_secs: 3600,
            size_bucket_bytes: 1000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct UnderFloorConfig {
    pub count: usize,
    pub window_secs: u64,
    /// The fee floor per ZIP 317 logical action.
    pub zats_per_action: u64,
}

impl Default for UnderFloorConfig {
    fn default() -> Self {
        Self {
            count: 3,
            window_secs: 600,
            zats_per_action: 1000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DepthConfig {
    /// Fire when the mempool is this many times its trailing median.
    pub factor: f64,
    pub baseline_secs: u64,
    /// The baseline needs this many one-minute samples before the rule fires.
    pub min_samples: usize,
    /// Ignore mempools smaller than these.
    pub min_count: u64,
    pub min_bytes: u64,
}

impl Default for DepthConfig {
    fn default() -> Self {
        Self {
            factor: 3.0,
            baseline_secs: 86_400,
            min_samples: 60,
            min_count: 100,
            min_bytes: 1_000_000,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LargeTxConfig {
    pub bytes: usize,
}

impl Default for LargeTxConfig {
    fn default() -> Self {
        Self { bytes: 100_000 }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BlindConfig {
    /// Fire when no peer announced a tx for this long.
    pub quiet_secs: u64,
}

impl Default for BlindConfig {
    fn default() -> Self {
        Self { quiet_secs: 1800 }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HeartbeatConfig {
    pub hour: u32,
    pub timezone: String,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            hour: 8,
            timezone: "America/New_York".into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_config_uses_defaults() {
        let config: Config = toml::from_str("").unwrap();
        assert_eq!(config.listener.target_peers, 50);
        assert_eq!(config.rules.burst.count, 5);
        assert!(config.ntfy.is_none());
    }

    #[test]
    fn example_config_parses() {
        let text = include_str!("../../../deploy/mempool/mempool.toml");
        let config: Config = toml::from_str(text).unwrap();
        assert!(config.ntfy.is_some());
    }
}
