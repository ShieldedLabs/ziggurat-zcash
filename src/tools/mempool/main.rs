//! Listens to Zcash mempool gossip from many peers, logs it, and exports metrics.
//! Grafana alerts on the metrics.

use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{self, BufRead, BufReader},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::ExitCode,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use clap::Parser;
use metrics_exporter_prometheus::Matcher;
use metrics_util::MetricKindMask;
use serde_json::{json, Value};
use tokio::{
    signal::unix::{signal, SignalKind},
    sync::mpsc,
};
use tracing::{debug, error, info};
use tracing_subscriber::filter::{EnvFilter, LevelFilter};
use ziggurat_zcash::protocol::{
    message::Message,
    payload::{
        inv::{Inv, InvHash},
        Tx,
    },
};

use crate::{
    config::{Config, ListenerConfig},
    events::{event_line, EventLog, SeenToday, TxIds, TxSummary},
    node::{Listener, NodeEvent},
    peers::PeerSet,
    rules::{Alert, Rules, RULES},
};

mod config;
mod events;
mod node;
mod peers;
mod rules;

const TICK: Duration = Duration::from_secs(5);
/// Zebra answers `mempool` at once, so a later inv is gossip.
const SNAPSHOT_REPLY: Duration = Duration::from_secs(10);
const DEPTH_INTERVAL: Duration = Duration::from_secs(60);
const ALIVE_INTERVAL: Duration = Duration::from_secs(3600);
/// A tx that returns this soon after `gone` is the same tx, not a new one.
const GONE_MEMORY: Duration = Duration::from_secs(3600);
const GAUGE_IDLE: Duration = Duration::from_secs(300);
/// 999 is the last bucket under the 1,000 zats per action floor.
const FEE_PER_ACTION_BUCKETS: [f64; 11] = [
    100.0, 250.0, 500.0, 800.0, 999.0, 1500.0, 2500.0, 5000.0, 10_000.0, 25_000.0, 100_000.0,
];

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// The TOML config file
    #[clap(long, value_parser, default_value = "mempool.toml")]
    config: PathBuf,

    /// Feed the tx events of an NDJSON file (or .ndjson.gz) through the rules, print the alerts, and exit
    #[clap(long, value_parser)]
    replay: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> ExitCode {
    start_logger();
    let args = Args::parse();
    let config = match Config::load(&args.config) {
        Ok(config) => config,
        Err(e) => {
            error!("can't load {}: {e}", args.config.display());
            return ExitCode::FAILURE;
        }
    };
    if let Some(path) = args.replay {
        return match replay(&config, &path) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                error!("replay failed: {e}");
                ExitCode::FAILURE
            }
        };
    }

    match run(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!("{e}");
            ExitCode::FAILURE
        }
    }
}

fn start_logger() {
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy()
        .add_directive("tokio_util=off".parse().unwrap())
        .add_directive("mio=off".parse().unwrap())
        .add_directive("pea2pea=warn".parse().unwrap());
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

fn replay(config: &Config, path: &Path) -> io::Result<()> {
    let file = File::open(path)?;
    let reader: Box<dyn BufRead> = if path.extension().is_some_and(|e| e == "gz") {
        Box::new(BufReader::new(flate2::read::MultiGzDecoder::new(file)))
    } else {
        Box::new(BufReader::new(file))
    };
    let lines = reader.lines();
    for (t, alert) in replay_alerts(config, lines)? {
        print!("{}", event_line(t, "alert", serde_json::to_value(&alert)?));
    }
    Ok(())
}

/// Feeds the `tx` events of NDJSON lines through fresh rules.
fn replay_alerts(
    config: &Config,
    lines: impl Iterator<Item = io::Result<String>>,
) -> io::Result<Vec<(DateTime<Utc>, Alert)>> {
    let mut rules: Option<Rules> = None;
    let mut alerts = Vec::new();
    for line in lines {
        let value: Value = serde_json::from_str(&line?)?;
        if value["event"] != "tx" {
            continue;
        }
        let t = value["t"]
            .as_i64()
            .and_then(|t| DateTime::from_timestamp(t, 0))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad t"))?;
        let summary: TxSummary = serde_json::from_value(value)?;
        let rules = match &mut rules {
            Some(rules) => rules,
            None => rules.insert(Rules::new(config.rules.clone(), t)),
        };
        alerts.extend(rules.on_tx(t, &summary).into_iter().map(|a| (t, a)));
    }
    Ok(alerts)
}

async fn run(config: Config) -> Result<(), String> {
    let listener_config = config.listener.clone();
    let candidates = if listener_config.peers.is_empty() {
        let census =
            peers::newest_census(&listener_config.census_dir).map_err(|e| e.to_string())?;
        let candidates = peers::census_candidates(&census).map_err(|e| e.to_string())?;
        info!(
            "{} Zebra and Zakura NU7 peers in {}",
            candidates.len(),
            census.display()
        );
        candidates
    } else {
        listener_config.peers.clone()
    };
    if candidates.is_empty() {
        return Err("no peers to connect to".into());
    }

    if let Some(addr) = listener_config.metrics_addr {
        metrics_exporter_prometheus::PrometheusBuilder::new()
            .with_http_listener(addr)
            .set_buckets_for_metric(
                Matcher::Full("mempool_fee_per_action_zats".into()),
                &FEE_PER_ACTION_BUCKETS,
            )
            .map_err(|e| format!("metrics buckets: {e}"))?
            // A dropped peer's gauge expires. Every other gauge is set on each tick.
            .idle_timeout(MetricKindMask::GAUGE, Some(GAUGE_IDLE))
            .install()
            .map_err(|e| format!("metrics on {addr}: {e}"))?;
        // Start the counters at zero, so Prometheus sees the first event as an increase.
        for rule in RULES {
            metrics::counter!("mempool_alerts_total", 0, "rule" => rule);
        }
        metrics::counter!("mempool_txs_seen_total", 0);
        metrics::counter!("mempool_txs_undecoded_total", 0);
        info!("metrics on http://{addr}/metrics");
    }

    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let max_connections = (listener_config.target_peers + 10).min(u16::MAX as usize) as u16;
    let listener = Listener::new(
        listener_config.user_agent.clone(),
        max_connections,
        events_tx,
    );
    listener.enable().await;

    let log = EventLog::open(&listener_config.data_dir).map_err(|e| e.to_string())?;
    let rules = Rules::new(config.rules.clone(), Utc::now());
    let seen_today =
        SeenToday::load(&listener_config.data_dir, Utc::now()).map_err(|e| e.to_string())?;
    info!("{} txs seen earlier today", seen_today.len());
    let mut core = Core::new(
        listener_config,
        listener,
        candidates,
        log,
        seen_today,
        rules,
    );

    let mut sigterm = signal(SignalKind::terminate()).map_err(|e| e.to_string())?;
    let mut tick = tokio::time::interval(TICK);
    loop {
        tokio::select! {
            Some(event) = events_rx.recv() => core.on_node_event(event),
            _ = tick.tick() => core.on_tick(),
            _ = tokio::signal::ctrl_c() => break,
            _ = sigterm.recv() => break,
        }
    }
    core.alive("stop");
    info!("stopped");
    Ok(())
}

struct PeerInfo {
    user_agent: String,
    ready_at: Option<Instant>,
    last_poll: Option<Instant>,
    awaiting_snapshot: Option<Instant>,
    snapshot: Option<(Instant, u64)>,
    /// The txs this peer announced, by wtxid.
    announced: HashSet<String>,
}

struct TxEntry {
    ids: TxIds,
    inv: InvHash,
    first_seen: Instant,
    last_seen: Instant,
    announcers: Vec<SocketAddr>,
    size: Option<usize>,
    request: Option<(SocketAddr, Instant)>,
    tried: HashSet<SocketAddr>,
}

#[derive(Default)]
struct Counts {
    seen: u64,
    fetched: u64,
    undecoded: u64,
    alerts: u64,
}

struct Core {
    config: ListenerConfig,
    listener: Listener,
    peers: PeerSet,
    connected: HashMap<SocketAddr, PeerInfo>,
    txs: HashMap<String, TxEntry>,
    /// Txs marked gone in the last hour, with when and their size if fetched.
    recently_gone: HashMap<String, (Instant, Option<usize>)>,
    log: EventLog,
    seen_today: SeenToday,
    rules: Rules,
    last_depth: Instant,
    last_alive: Option<Instant>,
    counts: Counts,
}

impl Core {
    fn new(
        config: ListenerConfig,
        listener: Listener,
        candidates: Vec<SocketAddr>,
        log: EventLog,
        seen_today: SeenToday,
        rules: Rules,
    ) -> Self {
        let now = Instant::now();
        let peers = PeerSet::new(
            candidates,
            config.target_peers,
            Duration::from_secs(config.reconnect_secs),
            config.max_failures,
        );
        Self {
            config,
            listener,
            peers,
            connected: HashMap::new(),
            txs: HashMap::new(),
            recently_gone: HashMap::new(),
            log,
            seen_today,
            rules,
            last_depth: now,
            last_alive: None,
            counts: Counts::default(),
        }
    }

    fn emit(&mut self, event: &str, fields: Value) {
        if let Err(e) = self.log.write(Utc::now(), event, fields) {
            error!("can't write {event} event: {e}");
        }
    }

    fn ready_peers(&self) -> usize {
        self.connected
            .values()
            .filter(|p| p.ready_at.is_some())
            .count()
    }

    fn on_node_event(&mut self, event: NodeEvent) {
        let now = Instant::now();
        match event {
            NodeEvent::Version(addr, user_agent) => {
                self.connected.insert(
                    addr,
                    PeerInfo {
                        user_agent,
                        ready_at: None,
                        last_poll: None,
                        awaiting_snapshot: None,
                        snapshot: None,
                        announced: HashSet::new(),
                    },
                );
            }
            NodeEvent::Ready(addr) => {
                let Some(peer) = self.connected.get_mut(&addr) else {
                    return;
                };
                peer.ready_at = Some(now);
                let user_agent = peer.user_agent.clone();
                self.peers.connected(addr);
                self.emit("peer_up", json!({ "peer": addr, "user_agent": user_agent }));
                self.poll(addr, now);
            }
            NodeEvent::ConnectFailed(addr, e) => {
                debug!("connect to {addr} failed: {e}");
                self.peers.failed(addr);
            }
            NodeEvent::Disconnected(addr) => {
                match self.connected.remove(&addr) {
                    Some(PeerInfo {
                        ready_at: Some(ready_at),
                        ..
                    }) => {
                        let secs = now.duration_since(ready_at).as_secs();
                        self.emit("peer_down", json!({ "peer": addr, "secs": secs }));
                    }
                    _ => self.peers.failed(addr),
                }
                self.peers.dropped(addr, now);
            }
            NodeEvent::Message(addr, Message::Inv(inv)) => self.on_inv(addr, inv, now),
            NodeEvent::Message(addr, Message::Tx(tx)) => self.on_tx(addr, tx, now),
            NodeEvent::Message(addr, Message::NotFound(inv)) => {
                for hash in &inv.inventory {
                    let Some(ids) = TxIds::from_inv(hash) else {
                        continue;
                    };
                    let asked = self
                        .txs
                        .get(&ids.wtxid)
                        .and_then(|entry| entry.request)
                        .is_some_and(|(peer, _)| peer == addr);
                    if asked {
                        self.retry(&ids.wtxid, now);
                    }
                }
            }
            NodeEvent::Message(..) => {}
        }
    }

    fn on_inv(&mut self, addr: SocketAddr, inv: Inv, now: Instant) {
        let Some(peer) = self.connected.get_mut(&addr) else {
            return;
        };
        let snapshot = peer
            .awaiting_snapshot
            .take()
            .is_some_and(|sent| now.duration_since(sent) < SNAPSHOT_REPLY);
        let hashes: Vec<(TxIds, InvHash)> = inv
            .inventory
            .iter()
            .filter_map(|hash| TxIds::from_inv(hash).map(|ids| (ids, *hash)))
            .collect();
        if hashes.is_empty() && !snapshot {
            return;
        }

        let mut lines = Vec::new();
        if snapshot {
            peer.snapshot = Some((now, hashes.len() as u64));
            lines.push(("snapshot", json!({ "peer": addr, "count": hashes.len() })));
        }
        let src = if snapshot { "mempool" } else { "inv" };

        let mut new = 0;
        let today = Utc::now();
        let mut fetch = Vec::new();
        for (ids, hash) in hashes {
            let new_for_peer = peer.announced.insert(ids.wtxid.clone());
            let first = !self.txs.contains_key(&ids.wtxid);
            let back = if first {
                self.recently_gone.remove(&ids.wtxid)
            } else {
                None
            };
            let entry = self
                .txs
                .entry(ids.wtxid.clone())
                .or_insert_with(|| TxEntry {
                    ids: ids.clone(),
                    inv: hash,
                    first_seen: now,
                    last_seen: now,
                    announcers: Vec::new(),
                    size: back.and_then(|(_, size)| size),
                    request: None,
                    tried: HashSet::new(),
                });
            // After a restart, the first snapshots list txs the day file already has.
            let first_today = first && back.is_none() && self.seen_today.first(&ids.wtxid, today);
            entry.last_seen = now;
            if !entry.announcers.contains(&addr) {
                entry.announcers.push(addr);
            }
            if let Some((gone_at, _)) = back {
                let after = now.duration_since(gone_at).as_secs();
                lines.push((
                    "back",
                    json!({ "txid": ids.txid, "wtxid": ids.wtxid, "peer": addr, "after_secs": after }),
                ));
            }
            // A snapshot repeats what the peer has, so only gossip and new txs log.
            if new_for_peer && (!snapshot || first_today) {
                lines.push((
                    "seen",
                    json!({ "txid": ids.txid, "wtxid": ids.wtxid, "peer": addr, "src": src }),
                ));
            }
            if first_today {
                new += 1;
            }
            if first && entry.size.is_none() {
                fetch.push(ids.wtxid);
            }
        }

        for (event, fields) in lines {
            self.emit(event, fields);
        }
        self.counts.seen += new;
        metrics::counter!("mempool_txs_seen_total", new);
        for wtxid in fetch {
            self.request(&wtxid, addr, now);
        }
    }

    fn request(&mut self, wtxid: &str, addr: SocketAddr, now: Instant) {
        let Some(entry) = self.txs.get_mut(wtxid) else {
            return;
        };
        entry.request = Some((addr, now));
        entry.tried.insert(addr);
        self.listener
            .send(addr, Message::GetData(Inv::new(vec![entry.inv])));
    }

    /// Asks another peer that announced the tx, or gives up.
    fn retry(&mut self, wtxid: &str, now: Instant) {
        let Some(entry) = self.txs.get_mut(wtxid) else {
            return;
        };
        entry.request = None;
        let next = entry.announcers.iter().copied().find(|addr| {
            !entry.tried.contains(addr)
                && self
                    .connected
                    .get(addr)
                    .is_some_and(|p| p.ready_at.is_some())
        });
        if let Some(addr) = next {
            self.request(wtxid, addr, now);
        }
    }

    fn on_tx(&mut self, addr: SocketAddr, tx: Tx, now: Instant) {
        let hash = tx.inv_hash();
        let ids = TxIds::from_inv(&hash).expect("a tx inv hash names a tx");
        let t = Utc::now();

        let Some(parsed) = tx.transaction() else {
            self.counts.undecoded += 1;
            metrics::increment_counter!("mempool_txs_undecoded_total");
            let error = tx.decode_error().unwrap_or_default().to_owned();
            self.emit(
                "tx_undecoded",
                json!({ "txid": ids.txid, "peer": addr, "size": tx.raw().len(), "error": error }),
            );
            if let Err(e) = self.log.write_raw(t, &ids.wtxid, tx.raw()) {
                error!("can't write raw tx: {e}");
            }
            return;
        };

        let entry = self
            .txs
            .entry(ids.wtxid.clone())
            .or_insert_with(|| TxEntry {
                ids: ids.clone(),
                inv: hash,
                first_seen: now,
                last_seen: now,
                announcers: vec![addr],
                size: None,
                request: None,
                tried: HashSet::new(),
            });
        if entry.size.is_some() {
            return;
        }
        entry.size = Some(tx.raw().len());
        entry.request = None;

        let summary = TxSummary::new(ids.clone(), parsed, tx.raw().len());
        let mut fields = serde_json::to_value(&summary).expect("a summary serializes");
        fields["peer"] = json!(addr);
        self.emit("tx", fields);
        if let Err(e) = self.log.write_raw(t, &ids.wtxid, tx.raw()) {
            error!("can't write raw tx: {e}");
        }
        self.counts.fetched += 1;
        metrics::increment_counter!("mempool_txs_fetched_total", "pools" => summary.pools());
        if let Some(fee) = summary.fee {
            metrics::histogram!(
                "mempool_fee_per_action_zats",
                fee as f64 / f64::from(summary.logical_actions)
            );
        }

        let alerts = self.rules.on_tx(t, &summary);
        self.raise(alerts);
    }

    fn raise(&mut self, alerts: Vec<Alert>) {
        for alert in alerts {
            info!("alert: {}", alert.title);
            self.emit(
                "alert",
                serde_json::to_value(&alert).expect("an alert serializes"),
            );
            self.counts.alerts += 1;
            metrics::increment_counter!("mempool_alerts_total", "rule" => alert.rule);
        }
    }

    fn poll(&mut self, addr: SocketAddr, now: Instant) {
        if let Some(peer) = self.connected.get_mut(&addr) {
            peer.last_poll = Some(now);
            peer.awaiting_snapshot = Some(now);
            self.listener.send(addr, Message::MemPool);
        }
    }

    fn on_tick(&mut self) {
        let now = Instant::now();

        let listener = self.listener.clone();
        for addr in self.peers.due(now, |addr| listener.is_up(addr)) {
            self.listener.dial(addr);
        }

        let poll_every = Duration::from_secs(self.config.mempool_poll_secs);
        let due: Vec<SocketAddr> = self
            .connected
            .iter()
            .filter(|(_, p)| p.ready_at.is_some())
            .filter(|(_, p)| {
                p.last_poll
                    .is_none_or(|last| now.duration_since(last) >= poll_every)
            })
            .map(|(addr, _)| *addr)
            .collect();
        for addr in due {
            self.poll(addr, now);
        }

        let timeout = Duration::from_secs(self.config.getdata_timeout_secs);
        let stale: Vec<String> = self
            .txs
            .iter()
            .filter(|(_, e)| {
                e.request
                    .is_some_and(|(_, at)| now.duration_since(at) >= timeout)
            })
            .map(|(wtxid, _)| wtxid.clone())
            .collect();
        for wtxid in stale {
            self.retry(&wtxid, now);
        }

        let peers = self.ready_peers();
        if peers > 0 {
            self.sweep_gone(now);
        }

        if now.duration_since(self.last_depth) >= DEPTH_INTERVAL {
            self.last_depth = now;
            self.sample_depth(now);
        }

        metrics::gauge!("mempool_peers_connected", peers as f64);
        self.export_peer_snapshots(now);
        // The thresholds, so the dashboard draws them from the config.
        let rules = self.rules.config();
        metrics::gauge!("mempool_under_floor_needed", rules.under_floor.count as f64);
        metrics::gauge!(
            "mempool_fee_floor_zats_per_action",
            rules.under_floor.zats_per_action as f64
        );
        metrics::gauge!(
            "mempool_large_tx_threshold_bytes",
            rules.large_tx.bytes as f64
        );
        let pressure = self.rules.pressure(Utc::now());
        metrics::gauge!("mempool_burst_top_count", pressure.burst_count as f64);
        // NaN leaves a gap in the dashboard while Burst can't fire yet.
        let needed = pressure.burst_needed.map_or(f64::NAN, |n| n as f64);
        metrics::gauge!("mempool_burst_top_needed", needed);
        metrics::gauge!(
            "mempool_under_floor_count",
            pressure.under_floor_count as f64
        );
        metrics::gauge!("mempool_largest_tx_bytes", pressure.largest_tx as f64);
        metrics::gauge!("mempool_txs_live", self.txs.len() as f64);
        if self
            .last_alive
            .is_none_or(|last| now.duration_since(last) >= ALIVE_INTERVAL)
        {
            self.last_alive = Some(now);
            self.alive("alive");
        }
    }

    fn sweep_gone(&mut self, now: Instant) {
        let gone_after = Duration::from_secs(self.config.gone_after_secs);
        let gone: Vec<String> = self
            .txs
            .iter()
            .filter(|(_, e)| now.duration_since(e.last_seen) >= gone_after)
            .map(|(wtxid, _)| wtxid.clone())
            .collect();
        self.recently_gone
            .retain(|_, (at, _)| now.duration_since(*at) < GONE_MEMORY);
        for wtxid in gone {
            let entry = self.txs.remove(&wtxid).expect("listed above");
            self.recently_gone.insert(wtxid.clone(), (now, entry.size));
            for peer in self.connected.values_mut() {
                peer.announced.remove(&wtxid);
            }
            self.emit(
                "gone",
                json!({
                    "txid": entry.ids.txid,
                    "wtxid": entry.ids.wtxid,
                    "secs": entry.last_seen.duration_since(entry.first_seen).as_secs(),
                    "peers": entry.announcers.len(),
                    "fetched": entry.size.is_some(),
                }),
            );
        }
    }

    /// Each ready peer's latest mempool count. A stale snapshot counts as empty, because
    /// Zebra doesn't answer `mempool` when its mempool is empty.
    fn export_peer_snapshots(&self, now: Instant) {
        let fresh = Duration::from_secs(self.config.mempool_poll_secs * 2);
        for (addr, peer) in self.connected.iter().filter(|(_, p)| p.ready_at.is_some()) {
            let count = peer
                .snapshot
                .filter(|(at, _)| now.duration_since(*at) < fresh)
                .map_or(0, |(_, count)| count);
            metrics::gauge!(
                "mempool_peer_snapshot",
                count as f64,
                "peer" => addr.to_string(),
                "user_agent" => peer.user_agent.clone()
            );
        }
    }

    /// Samples the median fresh snapshot count and the bytes of fetched live txs.
    fn sample_depth(&self, now: Instant) {
        let fresh = Duration::from_secs(self.config.mempool_poll_secs * 2);
        let mut counts: Vec<u64> = self
            .connected
            .values()
            .filter_map(|p| p.snapshot)
            .filter(|(at, _)| now.duration_since(*at) < fresh)
            .map(|(_, count)| count)
            .collect();
        counts.sort_unstable();
        let count = counts.get(counts.len() / 2).copied().unwrap_or(0);
        let bytes: u64 = self
            .txs
            .values()
            .filter_map(|e| e.size)
            .map(|s| s as u64)
            .sum();
        metrics::gauge!("mempool_snapshot_median", count as f64);
        metrics::gauge!("mempool_live_bytes", bytes as f64);
    }

    fn alive(&mut self, event: &str) {
        let fields = json!({
            "peers": self.ready_peers(),
            "targets": self.peers.targets().len(),
            "txs_live": self.txs.len(),
            "seen": self.counts.seen,
            "fetched": self.counts.fetched,
            "undecoded": self.counts.undecoded,
            "alerts": self.counts.alerts,
        });
        info!("{event}: {fields}");
        self.emit(event, fields);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_800_zat_burst() {
        let fixture = include_str!("testdata/burst-800zat.ndjson");
        let lines = fixture.lines().map(|l| Ok(l.to_owned()));
        let alerts = replay_alerts(&Config::default(), lines).unwrap();
        let rules: Vec<_> = alerts.iter().map(|(_, a)| a.rule).collect();
        assert_eq!(rules, ["under_floor", "burst"]);
    }
}
