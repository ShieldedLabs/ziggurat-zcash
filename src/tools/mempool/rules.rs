//! Per-tx alert rules. Pure state machines: the core and the replay mode feed them txs.
//! Grafana alerts on the `mempool_alerts_total` counter, and on depth, peers and liveness.

use std::collections::{HashMap, HashSet, VecDeque};

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::{config::RulesConfig, events::TxSummary};

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Alert {
    pub rule: &'static str,
    /// The alert shape. The cooldown applies per key.
    pub key: String,
    pub title: String,
    pub message: String,
}

/// The rule names, so the metrics can start each counter at zero.
pub const RULES: [&str; 3] = ["burst", "under_floor", "large_tx"];

/// How close each per-tx rule is to firing, for the dashboard.
#[derive(Debug, Default, PartialEq)]
pub struct Pressure {
    /// The busiest shape (by count over what fires) in the burst window, and what fires it.
    /// `None` during the first baseline after a start, when Burst can't fire.
    pub burst_count: usize,
    pub burst_needed: Option<usize>,
    pub under_floor_count: usize,
    /// The largest tx in the burst window.
    pub largest_tx: usize,
}

pub struct Rules {
    config: RulesConfig,
    started: DateTime<Utc>,
    counted: VecDeque<(DateTime<Utc>, String)>,
    counted_ids: HashSet<String>,
    cooldown: HashMap<String, DateTime<Utc>>,
    burst: HashMap<String, VecDeque<DateTime<Utc>>>,
    under_floor: VecDeque<DateTime<Utc>>,
    sizes: VecDeque<(DateTime<Utc>, usize)>,
}

impl Rules {
    pub fn new(config: RulesConfig, now: DateTime<Utc>) -> Self {
        Self {
            config,
            started: now,
            counted: VecDeque::new(),
            counted_ids: HashSet::new(),
            cooldown: HashMap::new(),
            burst: HashMap::new(),
            under_floor: VecDeque::new(),
            sizes: VecDeque::new(),
        }
    }

    fn windows(&self) -> (chrono::Duration, chrono::Duration) {
        let window = secs(self.config.burst.window_secs);
        (
            window,
            secs(self.config.burst.baseline_secs).max(window * 2),
        )
    }

    /// A shape's count inside the window, and its usual count per window from the
    /// baseline before the window.
    fn burst_stats(&self, times: &VecDeque<DateTime<Utc>>, t: DateTime<Utc>) -> (usize, f64) {
        let (window, baseline) = self.windows();
        let live = times.iter().filter(|&&at| t - at <= baseline);
        let count = live.clone().filter(|&&at| t - at <= window).count();
        let prior = live.count() - count;
        let usual =
            prior as f64 * window.num_seconds() as f64 / (baseline - window).num_seconds() as f64;
        (count, usual)
    }

    /// The fewest txs in the window that fire Burst for a shape with this usual count.
    fn burst_needed(&self, usual: f64) -> usize {
        let above_usual = (self.config.burst.factor * usual).floor() as usize + 1;
        self.config.burst.count.max(above_usual)
    }

    pub fn pressure(&self, t: DateTime<Utc>) -> Pressure {
        let (window, baseline) = self.windows();
        let (burst_count, burst_needed) = self
            .burst
            .values()
            .map(|times| {
                let (count, usual) = self.burst_stats(times, t);
                (count, self.burst_needed(usual))
            })
            .max_by(|a, b| (a.0 * b.1).cmp(&(b.0 * a.1)))
            .unwrap_or((0, self.config.burst.count));
        let floor_window = secs(self.config.under_floor.window_secs);
        Pressure {
            burst_count,
            burst_needed: (t - self.started >= baseline).then_some(burst_needed),
            under_floor_count: self
                .under_floor
                .iter()
                .filter(|&&at| t - at <= floor_window)
                .count(),
            largest_tx: self
                .sizes
                .iter()
                .filter(|(at, _)| t - *at <= window)
                .map(|(_, size)| *size)
                .max()
                .unwrap_or(0),
        }
    }

    pub fn on_tx(&mut self, t: DateTime<Utc>, tx: &TxSummary) -> Vec<Alert> {
        // A tx can come back after a `gone`. Count it once.
        let keep = secs(
            self.config
                .burst
                .baseline_secs
                .max(self.config.under_floor.window_secs),
        );
        while self.counted.front().is_some_and(|(at, _)| t - *at > keep) {
            let (_, wtxid) = self.counted.pop_front().expect("checked above");
            self.counted_ids.remove(&wtxid);
        }
        if !self.counted_ids.insert(tx.wtxid.clone()) {
            return Vec::new();
        }
        self.counted.push_back((t, tx.wtxid.clone()));

        let mut alerts = Vec::new();

        let (window, baseline) = self.windows();
        self.sizes.push_back((t, tx.size));
        while self.sizes.front().is_some_and(|(at, _)| t - *at > window) {
            self.sizes.pop_front();
        }

        let shape = shape_label(tx, self.config.burst.size_bucket_bytes);
        let times = self.burst.entry(shape.clone()).or_default();
        times.push_back(t);
        while times.front().is_some_and(|&first| t - first > baseline) {
            times.pop_front();
        }
        // Drop idle shapes so the map stays small.
        self.burst
            .retain(|_, times| times.back().is_some_and(|&last| t - last <= baseline));
        let (count, usual) = self.burst_stats(&self.burst[&shape], t);
        let warm = t - self.started >= baseline;
        if warm && count >= self.burst_needed(usual) {
            alerts.extend(self.fire(
                t,
                Alert {
                    rule: "burst",
                    key: format!("burst:{shape}"),
                    title: format!("Mempool burst: {count} txs of one shape"),
                    message: format!(
                        "{count} txs in {} min, usually {usual:.1}: {shape}. Latest {}",
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
                    title: format!("Large tx: {} KB", tx.size / 1000),
                    message: format!("{}: {}", tx.txid, shape_label(tx, 1000)),
                },
            ));
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

#[cfg(test)]
mod tests {
    use super::*;

    fn at(secs: i64) -> DateTime<Utc> {
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
        Rules::new(RulesConfig::default(), at(0))
    }

    fn rules_at(t: DateTime<Utc>) -> Rules {
        Rules::new(RulesConfig::default(), t)
    }

    #[test]
    fn busy_shape_stays_quiet_and_rare_shape_bursts() {
        let mut rules = rules();
        let mut fired = Vec::new();
        // Two hours of a common shape, 20 per 10 min.
        for i in 0..240 {
            fired.extend(rules.on_tx(at(i * 30), &tx(1000 + i as usize, 4_200, 0, None)));
        }
        assert!(fired.is_empty());

        // Then 8 cheap 2-action txs in 4 min.
        for i in 0..8 {
            fired.extend(rules.on_tx(
                at(7_200 + i * 30),
                &tx(i as usize, 9_150 + i as usize, 2, Some(800)),
            ));
        }
        let names: Vec<_> = fired.iter().map(|a| a.rule).collect();
        assert_eq!(names, ["under_floor", "burst"]);
        assert!(fired[1].message.contains("usually 0.0"));
        assert!(fired[1].message.contains("2 orchard, fee 800"));
    }

    #[test]
    fn pressure_tracks_each_rule() {
        let mut rules = rules();
        assert_eq!(rules.pressure(at(0)), Pressure::default());
        // An hour of 6 txs per 10 min sets the shape's usual count.
        for i in 0..36 {
            rules.on_tx(at(i * 100), &tx(100 + i as usize, 4_200, 0, None));
        }
        rules.on_tx(at(3_600), &tx(1, 9_150, 2, Some(800)));
        rules.on_tx(at(3_610), &tx(2, 120_000, 2, Some(800)));
        let p = rules.pressure(at(3_620));
        assert_eq!((p.burst_count, p.burst_needed), (5, Some(19)));
        assert_eq!(p.under_floor_count, 2);
        assert_eq!(p.largest_tx, 120_000);
        assert_eq!(rules.pressure(at(5_000)).under_floor_count, 0);
    }

    #[test]
    fn no_burst_while_warming_up() {
        let mut rules = rules();
        for i in 0..8 {
            let alerts = rules.on_tx(at(i * 30), &tx(i as usize, 9_150, 2, Some(10_000)));
            assert!(alerts.is_empty());
        }
    }

    #[test]
    fn a_repeated_tx_counts_once() {
        let mut rules = rules_at(at(-7_200));
        for i in 0..3 {
            assert!(rules
                .on_tx(at(i * 60), &tx(7, 9_150, 2, Some(800)))
                .is_empty());
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
}
