//! The listener's TOML config. Every field has a default, so an empty file works.

use std::{net::SocketAddr, path::PathBuf};

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub listener: ListenerConfig,
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
#[serde(default, deny_unknown_fields)]
pub struct RulesConfig {
    /// Each alert shape fires at most once per cooldown.
    pub cooldown_secs: u64,
    pub burst: BurstConfig,
    pub under_floor: UnderFloorConfig,
    pub large_tx: LargeTxConfig,
}

impl Default for RulesConfig {
    fn default() -> Self {
        Self {
            cooldown_secs: 3600,
            burst: Default::default(),
            under_floor: Default::default(),
            large_tx: Default::default(),
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
pub struct LargeTxConfig {
    pub bytes: usize,
}

impl Default for LargeTxConfig {
    fn default() -> Self {
        Self { bytes: 100_000 }
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
    }

    #[test]
    fn example_config_parses() {
        let text = include_str!("../../../deploy/mempool/mempool.toml");
        let config: Config = toml::from_str(text).unwrap();
        assert!(config.listener.metrics_addr.is_some());
    }
}
