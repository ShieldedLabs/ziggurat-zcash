//! Alert rules. Pure state machines: the core and the replay mode feed them events.

use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, NaiveDate, Timelike, Utc};
use chrono_tz::Tz;
use serde::Serialize;

use crate::{config::RulesConfig, events::TxSummary};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Priority {
    Low,
    Default,
    High,
    Urgent,
}

impl Priority {
    /// The ntfy priority number.
    pub fn ntfy(self) -> u8 {
        match self {
            Self::Low => 2,
            Self::Default => 3,
            Self::High => 4,
            Self::Urgent => 5,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Alert {
    pub rule: &'static str,
    /// The alert shape. The cooldown applies per key.
    pub key: String,
    pub priority: Priority,
    pub title: String,
    pub message: String,
}

/// What the core knows on each tick.
pub struct Status {
    pub peers: usize,
    /// Seconds since any peer announced a tx.
    pub quiet_secs: u64,
}

pub struct Rules {
    config: RulesConfig,
    tz: Tz,
    cooldown: HashMap<String, DateTime<Utc>>,
    burst: HashMap<String, VecDeque<DateTime<Utc>>>,
    under_floor: VecDeque<DateTime<Utc>>,
    depth: VecDeque<(DateTime<Utc>, u64, u64)>,
    heartbeat_day: Option<NaiveDate>,
    txs_since_heartbeat: u64,
    alerts_since_heartbeat: u64,
}

impl Rules {
    pub fn new(config: RulesConfig, now: DateTime<Utc>) -> Result<Self, String> {
        let tz: Tz = config
            .heartbeat
            .timezone
            .parse()
            .map_err(|e| format!("heartbeat timezone: {e}"))?;
        // A start after the heartbeat hour waits for the next day.
        let local = now.with_timezone(&tz);
        let heartbeat_day = (local.hour() >= config.heartbeat.hour).then(|| local.date_naive());

        Ok(Self {
            config,
            tz,
            cooldown: HashMap::new(),
            burst: HashMap::new(),
            under_floor: VecDeque::new(),
            depth: VecDeque::new(),
            heartbeat_day,
            txs_since_heartbeat: 0,
            alerts_since_heartbeat: 0,
        })
    }

    pub fn on_tx(&mut self, t: DateTime<Utc>, tx: &TxSummary) -> Vec<Alert> {
        self.txs_since_heartbeat += 1;
        let mut alerts = Vec::new();

        let shape = shape_label(tx, self.config.burst.size_bucket_bytes);
        let window = secs(self.config.burst.window_secs);
        let times = self.burst.entry(shape.clone()).or_default();
        times.push_back(t);
        while times.front().is_some_and(|&first| t - first > window) {
            times.pop_front();
        }
        let count = times.len();
        // Drop idle shapes so the map stays small.
        self.burst
            .retain(|_, times| times.back().is_some_and(|&last| t - last <= window));
        if count >= self.config.burst.count {
            alerts.extend(self.fire(
                t,
                Alert {
                    rule: "burst",
                    key: format!("burst:{shape}"),
                    priority: Priority::High,
                    title: format!("Mempool burst: {count} txs of one shape"),
                    message: format!(
                        "{count} txs in {} min: {shape}. Latest {}",
                        self.config.burst.window_secs / 60,
                        tx.txid
                    ),
                },
            ));
        }

        let floor = self.config.under_floor.zats_per_action * u64::from(tx.logical_actions);
        if let Some(fee) = tx.fee.filter(|&fee| (fee as u64) < floor) {
            let window = secs(self.config.under_floor.window_secs);
            self.under_floor.push_back(t);
            while self
                .under_floor
                .front()
                .is_some_and(|&first| t - first > window)
            {
                self.under_floor.pop_front();
            }
            let count = self.under_floor.len();
            if count >= self.config.under_floor.count {
                alerts.extend(self.fire(
                    t,
                    Alert {
                        rule: "under_floor",
                        key: "under_floor".into(),
                        priority: Priority::Default,
                        title: format!("{count} txs under the fee floor"),
                        message: format!(
                            "{count} txs in {} min pay under {} zats per action. Latest {}: fee {fee} for {} actions",
                            self.config.under_floor.window_secs / 60,
                            self.config.under_floor.zats_per_action,
                            tx.txid,
                            tx.logical_actions,
                        ),
                    },
                ));
            }
        }

        if tx.size > self.config.large_tx.bytes {
            alerts.extend(self.fire(
                t,
                Alert {
                    rule: "large_tx",
                    key: format!("large_tx:{}", tx.size / 10_000),
                    priority: Priority::Default,
                    title: format!("Large tx: {} KB", tx.size / 1000),
                    message: format!("{}: {}", tx.txid, shape_label(tx, 1000)),
                },
            ));
        }

        alerts
    }

    /// Takes one mempool sample: the median snapshot count and the known bytes.
    pub fn on_depth(&mut self, t: DateTime<Utc>, count: u64, bytes: u64) -> Vec<Alert> {
        let config = self.config.depth.clone();
        let baseline_window = secs(config.baseline_secs);
        while self
            .depth
            .front()
            .is_some_and(|&(first, ..)| t - first > baseline_window)
        {
            self.depth.pop_front();
        }

        let mut alerts = Vec::new();
        if self.depth.len() >= config.min_samples {
            let base_count = median(self.depth.iter().map(|s| s.1));
            let base_bytes = median(self.depth.iter().map(|s| s.2));
            if count >= config.min_count && count as f64 > config.factor * base_count as f64 {
                alerts.extend(self.fire(
                    t,
                    Alert {
                        rule: "depth",
                        key: "depth:count".into(),
                        priority: Priority::High,
                        title: format!("Mempool depth: {count} txs"),
                        message: format!("{count} txs, trailing median {base_count}"),
                    },
                ));
            }
            if bytes >= config.min_bytes && bytes as f64 > config.factor * base_bytes as f64 {
                alerts.extend(self.fire(
                    t,
                    Alert {
                        rule: "depth",
                        key: "depth:bytes".into(),
                        priority: Priority::High,
                        title: format!("Mempool depth: {} KB", bytes / 1000),
                        message: format!(
                            "{} KB of txs, trailing median {} KB",
                            bytes / 1000,
                            base_bytes / 1000
                        ),
                    },
                ));
            }
        }
        self.depth.push_back((t, count, bytes));
        alerts
    }

    pub fn on_tick(&mut self, t: DateTime<Utc>, status: &Status) -> Vec<Alert> {
        let mut alerts = Vec::new();

        let blind = if status.peers == 0 {
            Some("0 peers connected".to_string())
        } else if status.quiet_secs >= self.config.blind.quiet_secs {
            Some(format!(
                "No tx announcements for {} min from {} peers",
                status.quiet_secs / 60,
                status.peers
            ))
        } else {
            None
        };
        if let Some(message) = blind {
            alerts.extend(self.fire(
                t,
                Alert {
                    rule: "blind",
                    key: "blind".into(),
                    priority: Priority::Urgent,
                    title: "Mempool listener is blind".into(),
                    message,
                },
            ));
        }

        let local = t.with_timezone(&self.tz);
        if local.hour() >= self.config.heartbeat.hour
            && self.heartbeat_day != Some(local.date_naive())
        {
            self.heartbeat_day = Some(local.date_naive());
            alerts.push(Alert {
                rule: "heartbeat",
                key: "heartbeat".into(),
                priority: Priority::Low,
                title: "Mempool listener heartbeat".into(),
                message: format!(
                    "{} peers, {} txs fetched, {} alerts since the last heartbeat",
                    status.peers, self.txs_since_heartbeat, self.alerts_since_heartbeat
                ),
            });
            self.txs_since_heartbeat = 0;
            self.alerts_since_heartbeat = 0;
        }

        alerts
    }

    fn fire(&mut self, t: DateTime<Utc>, alert: Alert) -> Option<Alert> {
        let cooldown = secs(self.config.cooldown_secs);
        if let Some(&last) = self.cooldown.get(&alert.key) {
            if t - last < cooldown {
                return None;
            }
        }
        self.cooldown.insert(alert.key.clone(), t);
        self.alerts_since_heartbeat += 1;
        Some(alert)
    }
}

/// A readable shape: version, size bucket, component counts and fee.
pub fn shape_label(tx: &TxSummary, bucket_bytes: usize) -> String {
    let bucket = tx.size / bucket_bytes.max(1) * bucket_bytes.max(1);
    let mut parts = vec![
        format!("v{}", tx.version),
        format!("{}-{} B", bucket, bucket + bucket_bytes),
    ];
    for (n, name) in [
        (tx.transparent_inputs, "t-in"),
        (tx.transparent_outputs, "t-out"),
        (tx.sprout_joinsplits, "joinsplit"),
        (tx.sapling_spends, "sapling spend"),
        (tx.sapling_outputs, "sapling out"),
        (tx.orchard_actions, "orchard"),
        (tx.ironwood_actions, "ironwood"),
    ] {
        if n > 0 {
            parts.push(format!("{n} {name}"));
        }
    }
    parts.push(match tx.fee {
        Some(fee) => format!("fee {fee}"),
        None => "fee ?".into(),
    });
    parts.join(", ")
}

fn secs(n: u64) -> chrono::Duration {
    chrono::Duration::seconds(n as i64)
}

fn median(values: impl Iterator<Item = u64>) -> u64 {
    let mut values: Vec<u64> = values.collect();
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    values[values.len() / 2]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(secs: i64) -> DateTime<Utc> {
        // 2026-11-11 12:00 UTC, 07:00 in New York.
        DateTime::from_timestamp(1_794_052_800 + secs, 0).unwrap()
    }

    fn tx(n: usize, size: usize, orchard: usize, fee: Option<i64>) -> TxSummary {
        TxSummary {
            txid: format!("{n:064x}"),
            wtxid: format!("{n:0128x}"),
            version: 5,
            size,
            transparent_inputs: 0,
            transparent_outputs: 0,
            sprout_joinsplits: 0,
            sapling_spends: 0,
            sapling_outputs: 0,
            orchard_actions: orchard,
            ironwood_actions: 0,
            logical_actions: orchard.max(2) as u32,
            fee,
            sapling_value_balance: 0,
            orchard_value_balance: fee.unwrap_or(0),
            ironwood_value_balance: 0,
            transparent_output_value: 0,
            expiry_height: None,
        }
    }

    fn rules() -> Rules {
        Rules::new(RulesConfig::default(), at(0)).unwrap()
    }

    #[test]
    fn burst_and_under_floor_fire_once() {
        let mut rules = rules();
        let mut fired = Vec::new();
        for i in 0..8 {
            fired.extend(rules.on_tx(
                at(i * 30),
                &tx(i as usize, 9_150 + i as usize, 2, Some(800)),
            ));
        }
        let names: Vec<_> = fired.iter().map(|a| a.rule).collect();
        assert_eq!(names, ["under_floor", "burst"]);
        assert!(fired[1].message.contains("2 orchard, fee 800"));
    }

    #[test]
    fn spread_out_txs_do_not_burst() {
        let mut rules = rules();
        for i in 0..8 {
            let alerts = rules.on_tx(at(i * 200), &tx(i as usize, 9_150, 2, Some(10_000)));
            assert!(alerts.is_empty());
        }
    }

    #[test]
    fn cooldown_is_per_shape_and_expires() {
        let mut rules = rules();
        let big = |i| tx(i, 239_000, 100, Some(500_000));
        assert_eq!(rules.on_tx(at(0), &big(1)).len(), 1);
        assert!(rules.on_tx(at(60), &big(2)).is_empty());
        assert_eq!(rules.on_tx(at(60), &tx(3, 150_000, 2, None)).len(), 1);
        assert_eq!(rules.on_tx(at(3_700), &big(4)).len(), 1);
    }

    #[test]
    fn depth_needs_a_baseline() {
        let mut rules = rules();
        assert!(rules.on_depth(at(0), 10_000, 0).is_empty());
        for i in 1..=60 {
            assert!(rules.on_depth(at(i * 60), 200, 400_000).is_empty());
        }
        let alerts = rules.on_depth(at(61 * 60), 700, 400_000);
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].key, "depth:count");
    }

    #[test]
    fn blind_and_heartbeat() {
        let mut rules = rules();
        let ok = Status {
            peers: 40,
            quiet_secs: 10,
        };
        assert!(rules.on_tick(at(0), &ok).is_empty());
        let alerts = rules.on_tick(
            at(60),
            &Status {
                peers: 0,
                quiet_secs: 0,
            },
        );
        assert_eq!(alerts[0].priority, Priority::Urgent);
        // 08:00 in New York.
        let alerts = rules.on_tick(at(3_600), &ok);
        assert_eq!(alerts.len(), 1);
        assert_eq!(alerts[0].rule, "heartbeat");
        assert!(rules.on_tick(at(7_200), &ok).is_empty());
    }
}
