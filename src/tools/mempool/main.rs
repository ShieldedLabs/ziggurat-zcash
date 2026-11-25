//! Listens to Zcash mempool gossip from many peers, logs it, and exports metrics.
//! Grafana alerts on the metrics.

use std::{
    collections::{HashMap, HashSet, VecDeque},
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
use pea2pea::Pea2Pea;
use serde_json::{json, Value};
use tokio::{
    signal::unix::{signal, SignalKind},
    sync::mpsc,
    time::MissedTickBehavior,
};
use tracing::{debug, error, info};
use tracing_subscriber::filter::{EnvFilter, LevelFilter};
use ziggurat_zcash::protocol::{
    message::Message,
    payload::{
        block::{Headers, LocatorHashes},
        inv::{Inv, InvHash},
        Hash, Tx,
    },
};

use crate::{
    chain::{Chain, Height, Locator, Tips},
    config::{Config, ListenerConfig},
    events::{display_hex, event_line, EventLog, SeenToday, TxIds, TxSummary},
    fetch::Fetches,
    lookup::{FeeSource, Lookup},
    node::{Listener, NodeEvent},
    peers::PeerSet,
    rules::{Alert, Rules, RULES},
};

mod chain;
mod config;
mod events;
mod fetch;
mod lookup;
mod node;
mod peers;
mod rules;
mod wire;

const TICK: Duration = Duration::from_secs(5);
/// Zebra answers `mempool` at once, so a later inv is gossip.
const SNAPSHOT_REPLY: Duration = Duration::from_secs(10);
const DEPTH_INTERVAL: Duration = Duration::from_secs(60);
const ALIVE_INTERVAL: Duration = Duration::from_secs(3600);
/// A tx that returns this soon after `gone` is the same tx, not a new one.
const GONE_MEMORY: Duration = Duration::from_secs(3600);
const GAUGE_IDLE: Duration = Duration::from_secs(300);
/// Ask for headers with blocks this fresh. Past 160 blocks, an hour, every reply is full.
const CHAIN_FRESH: Duration = Duration::from_secs(1800);
/// The slowest real headers reply took 7.2 s.
const HEADERS_REPLY: Duration = Duration::from_secs(10);
/// The events that say what became of a connection to a peer.
const PEER_EVENTS: [&str; 4] = ["peer_up", "peer_down", "connect_failed", "turned_away"];
/// A peer announces one block at a time, or a few after a gap.
const BLOCKS_PER_INV: usize = 50;
/// Blocks remembered per peer, so a flood costs only the peer that sent it.
const BLOCKS_KEPT: usize = 200;
/// A peer logs at most `BLOCKS_KEPT` blocks in this time.
const BLOCKS_WINDOW: Duration = Duration::from_secs(60);
const TIP_INTERVAL: Duration = Duration::from_secs(30);
/// The lookup's cache of parent output values, in the data dir.
const PREVOUTS: &str = "prevouts.ndjson";
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

    /// With --replay: the day's raw file, so the lookup can fill in unknown fees
    #[clap(long, value_parser, requires = "replay")]
    raw: Option<PathBuf>,
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
        return match replay(&config, &path, args.raw.as_deref()).await {
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
    // Replay prints its alerts on stdout, so logs go to stderr.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(filter)
        .with_target(false)
        .init();
}

fn read_lines(path: &Path) -> io::Result<io::Lines<Box<dyn BufRead>>> {
    let file = File::open(path)?;
    let reader: Box<dyn BufRead> = if path.extension().is_some_and(|e| e == "gz") {
        Box::new(BufReader::new(flate2::read::MultiGzDecoder::new(file)))
    } else {
        Box::new(BufReader::new(file))
    };
    Ok(reader.lines())
}

async fn replay(config: &Config, path: &Path, raw: Option<&Path>) -> io::Result<()> {
    let mut txs = HashMap::new();
    if let Some(raw) = raw {
        for line in read_lines(raw)? {
            let value: Value = serde_json::from_str(&line?)?;
            if let (Some(wtxid), Some(hex)) = (value["wtxid"].as_str(), value["hex"].as_str()) {
                let bytes =
                    hex::decode(hex).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                txs.insert(wtxid.to_owned(), bytes);
            }
        }
    }
    let lookup = match raw {
        Some(_) => Lookup::new(&config.lookup, &config.listener.data_dir.join(PREVOUTS))
            .map_err(io::Error::other)?,
        None => None,
    };
    let replay = Replay { txs, lookup };
    for (t, alert) in replay_alerts(config, read_lines(path)?, &replay).await? {
        print!("{}", event_line(t, "alert", serde_json::to_value(&alert)?));
    }
    Ok(())
}

/// What a replay needs to fill in unknown fees.
#[derive(Default)]
struct Replay {
    /// Raw tx bytes by wtxid.
    txs: HashMap<String, Vec<u8>>,
    lookup: Option<Lookup>,
}

/// Feeds the `tx` events of NDJSON lines through fresh rules.
async fn replay_alerts(
    config: &Config,
    lines: impl Iterator<Item = io::Result<String>>,
    replay: &Replay,
) -> io::Result<Vec<(DateTime<Utc>, Alert)>> {
    let mut rules: Option<Rules> = None;
    let mut alerts = Vec::new();
    let mut sources: HashMap<FeeSource, u64> = HashMap::new();
    for line in lines {
        let value: Value = serde_json::from_str(&line?)?;
        if value["event"] != "tx" {
            continue;
        }
        let t = value["t"]
            .as_i64()
            .and_then(|t| DateTime::from_timestamp(t, 0))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad t"))?;
        let mut summary: TxSummary = serde_json::from_value(value)?;
        if summary.fee.is_none() {
            let tx = replay
                .txs
                .get(&summary.wtxid)
                .map(|raw| Tx::from_raw(raw.clone()));
            if let (Some(lookup), Some(parsed)) =
                (&replay.lookup, tx.as_ref().and_then(Tx::transaction))
            {
                if let Err(e) = set_fee(&mut summary, lookup.fee(parsed).await) {
                    debug!("{}: {e}", summary.txid);
                }
            }
        } else if summary.fee_source == FeeSource::Unknown {
            // Logs from before fee_source only had shielded fees.
            summary.fee_source = FeeSource::Shielded;
        }
        *sources.entry(summary.fee_source).or_default() += 1;
        let rules = match &mut rules {
            Some(rules) => rules,
            None => rules.insert(Rules::new(config.rules.clone(), t)),
        };
        alerts.extend(rules.on_tx(t, &summary).into_iter().map(|a| (t, a)));
    }
    info!("fee sources: {sources:?}");
    Ok(alerts)
}

/// Records a lookup's fee. Returns the lookup's error.
fn set_fee(summary: &mut TxSummary, fee: Result<i64, String>) -> Result<(), String> {
    let fee = fee?;
    summary.fee = Some(fee);
    summary.fee_source = FeeSource::Lookup;
    Ok(())
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
        metrics::counter!("chain_block_announcements_total", 0);
        for result in ["at", "at_most", "unplaced", "no_reply"] {
            metrics::counter!("chain_headers_total", 0, "result" => result);
        }
        for event in PEER_EVENTS {
            metrics::counter!("chain_peer_events_total", 0, "event" => event);
        }
        for result in ["ok", "error"] {
            metrics::counter!("mempool_fee_lookups_total", 0, "result" => result);
        }
        info!("metrics on http://{addr}/metrics");
    }

    let wire = match &config.wire.dir {
        Some(dir) => wire::WireLog::open(dir, config.wire.zstd_level)
            .map_err(|e| format!("wire log in {}: {e}", dir.display()))?,
        None => wire::WireLog::off(),
    };
    let (events_tx, mut events_rx) = mpsc::unbounded_channel();
    let max_connections = (listener_config.target_peers + 10).min(u16::MAX as usize) as u16;
    let listener = Listener::new(
        listener_config.user_agent.clone(),
        max_connections,
        events_tx,
        wire,
    );
    listener.enable().await;

    let log = EventLog::open(&listener_config.data_dir).map_err(|e| e.to_string())?;
    let rules = Rules::new(config.rules.clone(), Utc::now());
    let seen_today =
        SeenToday::load(&listener_config.data_dir, Utc::now()).map_err(|e| e.to_string())?;
    info!("{} txs seen earlier today", seen_today.len());
    let lookup = Lookup::new(&config.lookup, &listener_config.data_dir.join(PREVOUTS))?;
    let (fees_tx, mut fees_rx) = mpsc::unbounded_channel();
    let (tips_tx, mut tips_rx) = mpsc::unbounded_channel();
    if let Some(lookup) = &lookup {
        tokio::spawn(poll_tip(lookup.clone(), tips_tx));
    }
    let mut core = Core::new(
        listener_config,
        listener,
        candidates,
        log,
        seen_today,
        rules,
    );
    core.lookup = lookup.map(|lookup| (lookup, fees_tx));

    let mut sigterm = signal(SignalKind::terminate()).map_err(|e| e.to_string())?;
    let mut tick = tokio::time::interval(TICK);
    loop {
        tokio::select! {
            Some(event) = events_rx.recv() => core.on_node_event(event),
            Some((wtxid, fee)) = fees_rx.recv() => core.on_fee(wtxid, fee),
            Some(news) = tips_rx.recv() => core.on_chain(news),
            _ = tick.tick() => core.on_tick(),
            _ = tokio::signal::ctrl_c() => break,
            _ = sigterm.recv() => break,
        }
    }
    core.finish_pending();
    core.alive("stop");
    // Disconnect first, so each peer's `disconnect` lands before the file closes.
    core.listener.node().shut_down().await;
    core.listener.wire().close();
    info!("stopped");
    Ok(())
}

/// The blocks at these heights, or `None` if one of them can't be had.
async fn fetch_blocks<F>(heights: &[u32], block: impl Fn(u32) -> F) -> Option<Locator>
where
    F: std::future::Future<Output = Result<Hash, String>>,
{
    let mut found = Vec::new();
    for height in heights {
        match block(*height).await {
            Ok(hash) => found.push((*height, hash)),
            Err(e) => {
                debug!("block {height}: {e}");
                return None;
            }
        }
    }
    Some(found)
}

/// What the lookup servers say of the chain.
enum ChainNews {
    Tip(u32),
    Blocks { blocks: Locator, reset: bool },
}

/// Sends the chain tip every `TIP_INTERVAL`, and the blocks a locator needs. A failed poll
/// keeps the last tip.
async fn poll_tip(lookup: Lookup, news: mpsc::UnboundedSender<ChainNews>) {
    let mut tips = Tips::default();
    let mut interval = tokio::time::interval(TIP_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        let answers = lookup.tips().await;
        on_tips(&mut tips, &answers, lookup.servers(), &news, |h| {
            lookup.block(h)
        })
        .await;
        if news.is_closed() {
            return;
        }
    }
}

/// Sends the servers' median tip, then the blocks a locator needs if most of them agree on it.
async fn on_tips<F>(
    tips: &mut Tips,
    answers: &[u32],
    servers: usize,
    news: &mpsc::UnboundedSender<ChainNews>,
    block: impl Fn(u32) -> F,
) where
    F: std::future::Future<Output = Result<Hash, String>>,
{
    let Some(tip) = chain::median(answers) else {
        debug!("tip: no server answered");
        return;
    };
    let _ = news.send(ChainNews::Tip(tip));
    let Some(tip) = chain::agreed_tip(answers, servers) else {
        debug!("tip: the servers disagree: {answers:?}");
        return;
    };
    let wanted = tips.see(tip);
    if let Some(blocks) = fetch_blocks(&wanted.heights, block).await {
        tips.done(tip);
        let reset = wanted.reset;
        let _ = news.send(ChainNews::Blocks { blocks, reset });
    }
}

/// A tx's wtxid and its lookup's fee.
type FeeResult = (String, Result<i64, String>);

struct PeerInfo {
    user_agent: String,
    protocol_version: u32,
    /// The peer's chain height at the handshake, by its own word.
    start_height: i32,
    /// The blocks this peer announced, oldest first.
    blocks: VecDeque<(Hash, Instant)>,
    last_headers: Option<Instant>,
    /// The headers request that waits for its reply, and the locator it carried.
    asked: Option<(Instant, Locator)>,
    /// The height the latest headers reply gave.
    height: Option<(Instant, u32)>,
    ready_at: Option<Instant>,
    last_poll: Option<Instant>,
    awaiting_snapshot: Option<Instant>,
    snapshot: Option<Snapshot>,
    /// The txs this peer announced, by wtxid.
    announced: HashSet<String>,
    fetches: Fetches,
}

/// A peer's latest `mempool` reply.
#[derive(Clone, Copy)]
struct Snapshot {
    at: Instant,
    count: u64,
    /// Bytes of the listed txs the listener has fetched. A tx fetched after the reply
    /// counts from the next reply.
    bytes: u64,
}

/// The median peer's count and bytes over fresh snapshots. Each is a median of its own,
/// so one peer with a long backlog moves neither.
fn median_depth(
    snapshots: impl Iterator<Item = Snapshot>,
    now: Instant,
    fresh: Duration,
) -> (u64, u64) {
    let (mut counts, mut bytes): (Vec<u64>, Vec<u64>) = snapshots
        .filter(|s| now.duration_since(s.at) < fresh)
        .map(|s| (s.count, s.bytes))
        .unzip();
    counts.sort_unstable();
    bytes.sort_unstable();
    let median = |v: &[u64]| v.get(v.len() / 2).copied().unwrap_or(0);
    (median(&counts), median(&bytes))
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
    /// The lookup, and where its tasks send each tx's fee.
    lookup: Option<(Lookup, mpsc::UnboundedSender<FeeResult>)>,
    /// Fetched txs that wait for a lookup, by wtxid.
    pending: HashMap<String, (SocketAddr, TxSummary)>,
    /// The chain tip from the lookup servers. `None` without servers.
    tip: Option<u32>,
    chain: Chain,
    /// When a block at a new height last came.
    chain_moved: Option<Instant>,
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
            lookup: None,
            pending: HashMap::new(),
            tip: None,
            chain: Chain::default(),
            chain_moved: None,
            last_depth: now,
            last_alive: None,
            counts: Counts::default(),
        }
    }

    fn on_chain(&mut self, news: ChainNews) {
        match news {
            ChainNews::Tip(height) => self.tip = Some(height),
            ChainNews::Blocks { blocks, reset } => {
                if reset {
                    self.chain = Chain::default();
                }
                for (height, hash) in blocks {
                    if !self.chain.has(height) {
                        self.chain_moved = Some(Instant::now());
                    }
                    self.chain.insert(height, hash);
                }
            }
        }
    }

    fn emit(&mut self, event: &'static str, fields: Value) {
        if PEER_EVENTS.contains(&event) {
            metrics::increment_counter!("chain_peer_events_total", "event" => event);
        }
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
            NodeEvent::Version(addr, version) => {
                self.connected.insert(
                    addr,
                    PeerInfo {
                        user_agent: version.user_agent.0,
                        protocol_version: version.version.0,
                        start_height: version.start_height,
                        blocks: VecDeque::new(),
                        last_headers: None,
                        asked: None,
                        height: None,
                        ready_at: None,
                        last_poll: None,
                        awaiting_snapshot: None,
                        snapshot: None,
                        announced: HashSet::new(),
                        fetches: Fetches::default(),
                    },
                );
            }
            NodeEvent::Ready(addr) => {
                let Some(peer) = self.connected.get_mut(&addr) else {
                    return;
                };
                peer.ready_at = Some(now);
                let fields = json!({
                    "peer": addr,
                    "user_agent": peer.user_agent,
                    "protocol_version": peer.protocol_version,
                    "start_height": peer.start_height,
                });
                self.peers.connected(addr);
                self.emit("peer_up", fields);
                self.poll(addr, now);
            }
            NodeEvent::ConnectFailed(addr, e) => {
                debug!("connect to {addr} failed: {e}");
                self.peers.failed(addr);
                self.peers.ended(addr, now);
                self.emit("connect_failed", json!({ "peer": addr, "reason": e }));
            }
            NodeEvent::Disconnected(addr) => {
                let peer = self.connected.remove(&addr);
                let open = self.peers.ended(addr, now);
                // pea2pea's reader and writer can each report the same drop.
                if peer.is_none() && !open {
                    return;
                }
                match peer.as_ref().and_then(|p| p.ready_at) {
                    Some(ready_at) => {
                        let secs = now.duration_since(ready_at).as_secs();
                        self.emit("peer_down", json!({ "peer": addr, "secs": secs }));
                    }
                    None => {
                        self.peers.failed(addr);
                        let start_height = peer.as_ref().map(|p| p.start_height);
                        self.emit(
                            "turned_away",
                            json!({ "peer": addr, "start_height": start_height }),
                        );
                    }
                }
                for wtxid in peer.into_iter().flat_map(|p| p.fetches.into_wtxids()) {
                    self.retry(&wtxid, now);
                }
            }
            NodeEvent::Message(addr, Message::Inv(inv)) => {
                self.on_blocks(addr, &inv, now);
                self.on_inv(addr, inv, now);
            }
            NodeEvent::Message(addr, Message::Headers(headers)) => {
                self.on_headers(addr, headers, now)
            }
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

    /// Logs each block the first time this peer announces it, with the time since any
    /// connected peer did.
    fn on_blocks(&mut self, addr: SocketAddr, inv: &Inv, now: Instant) {
        let blocks = inv.inventory.iter().filter_map(|hash| match hash {
            InvHash::Block(hash) => Some(*hash),
            _ => None,
        });
        for hash in blocks.take(BLOCKS_PER_INV) {
            let Some(peer) = self.connected.get(&addr).filter(|p| p.ready_at.is_some()) else {
                return;
            };
            // Checked before the scan over every peer, so a flood stays cheap.
            let full = peer.blocks.len() == BLOCKS_KEPT;
            if full && now.duration_since(peer.blocks[0].1) < BLOCKS_WINDOW {
                return;
            }
            if peer.blocks.iter().any(|(h, _)| *h == hash) {
                continue;
            }
            let first = self
                .connected
                .values()
                .flat_map(|p| &p.blocks)
                .filter(|(h, _)| *h == hash)
                .map(|(_, at)| *at)
                .min();
            let peer = self.connected.get_mut(&addr).expect("looked up above");
            if full {
                peer.blocks.pop_front();
            }
            peer.blocks.push_back((hash, now));
            let after_ms = now.duration_since(first.unwrap_or(now)).as_millis() as u64;
            let hash = display_hex(&hash);
            metrics::increment_counter!("chain_block_announcements_total");
            self.emit(
                "block",
                json!({ "hash": hash, "peer": addr, "after_ms": after_ms }),
            );
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
        let count = hashes.len();
        let mut bytes = 0;
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
            bytes += entry.size.unwrap_or(0) as u64;
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

        if snapshot {
            peer.snapshot = Some(Snapshot {
                at: now,
                count: count as u64,
                bytes,
            });
            lines.insert(
                0,
                (
                    "snapshot",
                    json!({ "peer": addr, "count": count, "bytes": bytes }),
                ),
            );
        }

        for (event, fields) in lines {
            self.emit(event, fields);
        }
        self.counts.seen += new;
        metrics::counter!("mempool_txs_seen_total", new);
        for wtxid in fetch {
            self.request(wtxid, addr);
        }
        self.pump(addr, now);
    }

    /// Queues the tx for a getdata to the peer. `pump` sends it.
    fn request(&mut self, wtxid: String, addr: SocketAddr) {
        let (Some(entry), Some(peer)) = (self.txs.get_mut(&wtxid), self.connected.get_mut(&addr))
        else {
            return;
        };
        entry.tried.insert(addr);
        peer.fetches.push(wtxid);
    }

    /// Sends the peer's queued txs in batches, while its window has room.
    fn pump(&mut self, addr: SocketAddr, now: Instant) {
        let Some(peer) = self.connected.get_mut(&addr) else {
            return;
        };
        let txs = &mut self.txs;
        while let Some(batch) = peer.fetches.next_batch(|wtxid| {
            txs.get(wtxid)
                .is_some_and(|e| e.size.is_none() && e.request.is_none())
        }) {
            let mut invs = Vec::with_capacity(batch.len());
            for wtxid in &batch {
                let entry = txs.get_mut(wtxid).expect("next_batch keeps live txs");
                entry.request = Some((addr, now));
                invs.push(entry.inv);
            }
            self.listener.send(addr, Message::GetData(Inv::new(invs)));
        }
    }

    /// Clears the tx's request, and sends the asked peer its next batch.
    fn settle(&mut self, wtxid: &str, now: Instant) {
        let Some((addr, _)) = self.txs.get_mut(wtxid).and_then(|e| e.request.take()) else {
            return;
        };
        if let Some(peer) = self.connected.get_mut(&addr) {
            peer.fetches.settle(wtxid);
            self.pump(addr, now);
        }
    }

    /// Asks another peer that announced the tx, or gives up.
    fn retry(&mut self, wtxid: &str, now: Instant) {
        self.settle(wtxid, now);
        let Some(entry) = self.txs.get(wtxid).filter(|e| e.size.is_none()) else {
            return;
        };
        let next = entry.announcers.iter().copied().find(|addr| {
            !entry.tried.contains(addr)
                && self
                    .connected
                    .get(addr)
                    .is_some_and(|p| p.ready_at.is_some())
        });
        if let Some(addr) = next {
            self.request(wtxid.to_owned(), addr);
            self.pump(addr, now);
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
        self.settle(&ids.wtxid, now);

        if let Err(e) = self.log.write_raw(t, &ids.wtxid, tx.raw()) {
            error!("can't write raw tx: {e}");
        }
        let mut summary = TxSummary::new(ids.clone(), parsed, tx.raw().len());
        let mut fee_error = None;
        if let (FeeSource::Unknown, Some((lookup, fees))) = (summary.fee_source, &self.lookup) {
            if let Some(fee) = lookup.cached_fee(parsed) {
                fee_error = set_fee(&mut summary, fee).err();
            } else if let Some(slot) = lookup.slot() {
                let (lookup, fees, parsed) = (lookup.clone(), fees.clone(), parsed.clone());
                let wtxid = ids.wtxid.clone();
                tokio::spawn(async move {
                    let fee = lookup.fee(&parsed).await;
                    drop(slot);
                    let _ = fees.send((wtxid, fee));
                });
                self.pending.insert(ids.wtxid, (addr, summary));
                return;
            } else {
                fee_error = Some("too many lookups pending".into());
            }
        }
        self.finish_tx(addr, summary, fee_error);
    }

    fn on_fee(&mut self, wtxid: String, fee: Result<i64, String>) {
        if let Some((addr, mut summary)) = self.pending.remove(&wtxid) {
            let fee_error = set_fee(&mut summary, fee).err();
            self.finish_tx(addr, summary, fee_error);
        }
    }

    /// Logs the txs still waiting for a lookup, with an unknown fee.
    fn finish_pending(&mut self) {
        for (_, (addr, summary)) in std::mem::take(&mut self.pending) {
            self.finish_tx(addr, summary, Some("the listener stopped".into()));
        }
    }

    /// Logs a fetched tx with its fee, and runs the rules on it.
    fn finish_tx(&mut self, addr: SocketAddr, summary: TxSummary, fee_error: Option<String>) {
        let t = Utc::now();
        let mut fields = serde_json::to_value(&summary).expect("a summary serializes");
        fields["peer"] = json!(addr);
        if let Some(e) = fee_error {
            fields["fee_error"] = json!(e);
        }
        self.emit("tx", fields);
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

    /// Asks the peer where its chain is, over the connection it already holds with us.
    fn ask_headers(&mut self, addr: SocketAddr, now: Instant) {
        let locator = self.chain.locator();
        let fresh = self
            .chain_moved
            .is_some_and(|moved| now.duration_since(moved) < CHAIN_FRESH);
        if locator.is_empty() || !fresh {
            return;
        }
        if let Some(peer) = self.connected.get_mut(&addr) {
            let hashes = locator.iter().map(|(_, hash)| *hash).collect();
            peer.last_headers = Some(now);
            peer.asked = Some((now, locator));
            let request = LocatorHashes::new(hashes, Hash::zeroed());
            self.listener.send(addr, Message::GetHeaders(request));
        }
    }

    /// Logs the height a headers reply gives. A reply to no request is ignored.
    fn on_headers(&mut self, addr: SocketAddr, headers: Headers, now: Instant) {
        let Some(peer) = self.connected.get_mut(&addr) else {
            return;
        };
        let Some((sent, locator)) = peer.asked.take() else {
            return;
        };
        let found = chain::reply_height(&headers.headers, &locator);
        let (result, height) = match found {
            Height::At(height) => ("at", Some(height)),
            Height::AtMost(height) => ("at_most", Some(height)),
            Height::Unplaced => ("unplaced", None),
        };
        peer.height = match found {
            Height::At(height) => Some((now, height)),
            _ => None,
        };
        metrics::increment_counter!("chain_headers_total", "result" => result);
        self.emit(
            "height",
            json!({
                "peer": addr,
                "height": height,
                "at_most": result == "at_most",
                "headers": headers.headers.len(),
                "ms": now.duration_since(sent).as_millis() as u64,
            }),
        );
    }

    fn check_headers(&mut self, now: Instant) {
        let every = Duration::from_secs(self.config.headers_poll_secs);
        let mut silent = Vec::new();
        let mut due = Vec::new();
        for (addr, peer) in self
            .connected
            .iter_mut()
            .filter(|(_, p)| p.ready_at.is_some())
        {
            if let Some((sent, _)) = &peer.asked {
                if now.duration_since(*sent) < HEADERS_REPLY {
                    continue;
                }
                peer.asked = None;
                peer.height = None;
                silent.push(*addr);
            }
            if peer
                .last_headers
                .is_none_or(|last| now.duration_since(last) >= every)
            {
                due.push(*addr);
            }
        }
        for addr in silent {
            metrics::increment_counter!("chain_headers_total", "result" => "no_reply");
            self.emit("no_reply", json!({ "peer": addr }));
        }
        for addr in due {
            self.ask_headers(addr, now);
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

        self.check_headers(now);

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
                peer.fetches.settle(&wtxid);
            }
            self.emit(
                "gone",
                json!({
                    "txid": entry.ids.txid,
                    "wtxid": entry.ids.wtxid,
                    "secs": entry.last_seen.duration_since(entry.first_seen).as_secs(),
                    "peers": entry.announcers.len(),
                    "fetched": entry.size.is_some(),
                    "tip_height": self.tip,
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
                .filter(|s| now.duration_since(s.at) < fresh)
                .map_or(0, |s| s.count);
            metrics::gauge!(
                "mempool_peer_snapshot",
                count as f64,
                "peer" => addr.to_string(),
                "user_agent" => peer.user_agent.clone()
            );
            let fresh = Duration::from_secs(self.config.headers_poll_secs * 2);
            // NaN leaves a gap when the peer's height isn't known now.
            let height = peer
                .height
                .filter(|(at, _)| now.duration_since(*at) < fresh)
                .map_or(f64::NAN, |(_, height)| f64::from(height));
            metrics::gauge!(
                "chain_peer_height",
                height,
                "peer" => addr.to_string(),
                "user_agent" => peer.user_agent.clone()
            );
            metrics::gauge!(
                "chain_peer_start_height",
                f64::from(peer.start_height),
                "peer" => addr.to_string(),
                "user_agent" => peer.user_agent.clone()
            );
        }
    }

    /// Samples the median peer's mempool, and the bytes of all fetched live txs.
    fn sample_depth(&self, now: Instant) {
        let fresh = Duration::from_secs(self.config.mempool_poll_secs * 2);
        let (count, peer_bytes) = median_depth(
            self.connected.values().filter_map(|p| p.snapshot),
            now,
            fresh,
        );
        let bytes: u64 = self
            .txs
            .values()
            .filter_map(|e| e.size)
            .map(|s| s as u64)
            .sum();
        metrics::gauge!("mempool_snapshot_median", count as f64);
        metrics::gauge!("mempool_snapshot_median_bytes", peer_bytes as f64);
        metrics::gauge!("mempool_live_bytes", bytes as f64);
    }

    fn alive(&mut self, event: &'static str) {
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
    use pea2pea::protocols::Reading;
    use ziggurat_zcash::protocol::payload::{
        block::{Block, Header},
        VarStr, Version,
    };

    use super::*;

    #[test]
    fn one_backlog_moves_no_median() {
        let now = Instant::now();
        let at = |count, bytes| Snapshot {
            at: now,
            count,
            bytes,
        };
        let mut snapshots = vec![at(40, 1_000_000); 4];
        snapshots.push(at(1_300, 9_000_000));
        // A stale snapshot is left out.
        snapshots.push(Snapshot {
            at: now - Duration::from_secs(600),
            count: 5_000,
            bytes: 50_000_000,
        });
        let fresh = Duration::from_secs(120);
        assert_eq!(
            median_depth(snapshots.into_iter(), now, fresh),
            (40, 1_000_000)
        );
        assert_eq!(median_depth(std::iter::empty(), now, fresh), (0, 0));
    }

    #[tokio::test]
    async fn replay_800_zat_burst() {
        let fixture = include_str!("testdata/burst-800zat.ndjson");
        let lines = fixture.lines().map(|l| Ok(l.to_owned()));
        let alerts = replay_alerts(&Config::default(), lines, &Replay::default())
            .await
            .unwrap();
        let rules: Vec<_> = alerts.iter().map(|(_, a)| a.rule).collect();
        assert_eq!(rules, ["under_floor", "burst"]);
    }

    const A: &str = "192.0.2.1:8233";
    const B: &str = "192.0.2.2:8233";

    fn addr(peer: &str) -> SocketAddr {
        peer.parse().unwrap()
    }

    /// A core that dials nobody, and the events it logs.
    fn core(name: &str) -> (Core, PathBuf) {
        let dir = std::env::temp_dir().join(format!("mempool-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let (events, _) = mpsc::unbounded_channel();
        let config = Config::default();
        let listener = Listener::new("/test/".into(), 1, events, wire::WireLog::off());
        let log = EventLog::open(&dir).unwrap();
        let seen = SeenToday::load(&dir, Utc::now()).unwrap();
        let rules = Rules::new(config.rules, Utc::now());
        let core = Core::new(config.listener, listener, Vec::new(), log, seen, rules);
        (core, dir)
    }

    fn logged(dir: &Path, event: &str) -> Vec<Value> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|e| e == "ndjson") {
                out.extend(
                    read_lines(&path)
                        .unwrap()
                        .map(|line| serde_json::from_str::<Value>(&line.unwrap()).unwrap()),
                );
            }
        }
        out.retain(|line| line["event"] == event);
        out
    }

    fn version(start_height: i32) -> Version {
        let mut version = Version::new(addr(A), addr(B)).with_version(170_150);
        version.user_agent = VarStr("/Zebra:6.4.1/".into());
        version.start_height = start_height;
        version
    }

    fn up(core: &mut Core, peer: &str) {
        core.on_node_event(NodeEvent::Version(addr(peer), version(3_600_000)));
        core.on_node_event(NodeEvent::Ready(addr(peer)));
    }

    fn announce(core: &mut Core, peer: &str, blocks: &[u16]) {
        let hashes = blocks.iter().map(|n| {
            let mut hash = [0; 32];
            hash[..2].copy_from_slice(&n.to_be_bytes());
            InvHash::Block(Hash::new(hash))
        });
        let inv = Inv::new(hashes.collect());
        core.on_node_event(NodeEvent::Message(addr(peer), Message::Inv(inv)));
    }

    #[tokio::test]
    async fn peer_up_says_what_the_handshake_said() {
        let (mut core, dir) = core("handshake");
        up(&mut core, A);
        let events = logged(&dir, "peer_up");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["user_agent"], "/Zebra:6.4.1/");
        assert_eq!(events[0]["start_height"], 3_600_000);
        assert_eq!(events[0]["protocol_version"], 170_150);
    }

    #[tokio::test]
    async fn a_failed_connect_is_an_event() {
        let (mut core, dir) = core("failed");
        core.on_node_event(NodeEvent::ConnectFailed(addr(A), "refused".into()));
        let events = logged(&dir, "connect_failed");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["peer"], A);
        assert_eq!(events[0]["reason"], "refused");
    }

    #[tokio::test]
    async fn a_failed_dial_and_a_drop_both_free_the_peer() {
        let (mut core, _dir) = core("freed");
        let wait = Duration::from_secs(300);
        core.peers = PeerSet::new(vec![addr(A), addr(B)], 2, wait, 9);
        let t0 = Instant::now();
        assert_eq!(core.peers.due(t0, |_| false), [addr(A), addr(B)]);
        let later = t0 + wait * 2;
        assert!(core.peers.due(later, |_| false).is_empty());
        core.on_node_event(NodeEvent::ConnectFailed(addr(A), "refused".into()));
        core.on_node_event(NodeEvent::Disconnected(addr(B)));
        assert!(core.peers.due(t0 + wait, |_| false).is_empty());
        assert_eq!(core.peers.due(later, |_| false), [addr(A), addr(B)]);
    }

    #[tokio::test]
    async fn a_drop_reported_twice_is_one_drop() {
        let (mut core, dir) = core("twice");
        let wait = Duration::from_secs(300);
        core.peers = PeerSet::new(vec![addr(A), addr(B)], 1, wait, 1);
        core.peers.due(Instant::now(), |_| false);
        up(&mut core, A);
        core.on_node_event(NodeEvent::Disconnected(addr(A)));
        core.on_node_event(NodeEvent::Disconnected(addr(A)));
        assert_eq!(logged(&dir, "peer_down").len(), 1);
        assert!(logged(&dir, "turned_away").is_empty());
        core.peers.due(Instant::now(), |_| false);
        assert_eq!(core.peers.targets(), [addr(A)]);
    }

    #[tokio::test]
    async fn a_drop_before_verack_is_an_event() {
        let (mut core, dir) = core("early");
        core.on_node_event(NodeEvent::Version(addr(A), version(1)));
        core.on_node_event(NodeEvent::Disconnected(addr(A)));
        // This one took the connection and closed it before its version.
        core.peers = PeerSet::new(vec![addr(B)], 1, Duration::from_secs(300), 9);
        core.peers.due(Instant::now(), |_| false);
        core.on_node_event(NodeEvent::Disconnected(addr(B)));
        let events = logged(&dir, "turned_away");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["start_height"], 1);
        assert_eq!(events[1]["start_height"], Value::Null);
        assert!(logged(&dir, "peer_down").is_empty());
    }

    #[tokio::test]
    async fn a_block_logs_once_per_peer_in_display_order() {
        let (mut core, dir) = core("blocks");
        up(&mut core, A);
        up(&mut core, B);
        announce(&mut core, A, &[0xab]);
        tokio::time::sleep(Duration::from_millis(30)).await;
        announce(&mut core, B, &[0xab]);
        announce(&mut core, B, &[0xab]);
        let events = logged(&dir, "block");
        assert_eq!(events.len(), 2);
        assert!(events[0]["hash"].as_str().unwrap().ends_with("ab00"));
        assert_eq!(events[0]["peer"], A);
        assert_eq!(events[0]["after_ms"], 0);
        assert_eq!(events[1]["peer"], B);
        assert!(events[1]["after_ms"].as_u64().unwrap() >= 30);
        assert!(logged(&dir, "seen").is_empty());
    }

    #[tokio::test]
    async fn after_ms_counts_from_the_first_peer_of_that_block() {
        const C: &str = "192.0.2.3:8233";
        let (mut core, dir) = core("first");
        for peer in [A, B, C] {
            up(&mut core, peer);
        }
        announce(&mut core, A, &[1]);
        tokio::time::sleep(Duration::from_millis(30)).await;
        announce(&mut core, B, &[1]);
        tokio::time::sleep(Duration::from_millis(30)).await;
        announce(&mut core, C, &[1, 2]);
        let after: Vec<u64> = logged(&dir, "block")
            .iter()
            .map(|e| e["after_ms"].as_u64().unwrap())
            .collect();
        assert!(after[2] >= 60, "{after:?}");
        assert_eq!(after[3], 0);
    }

    /// The first testnet headers, with the first one sampled at height 100.
    fn sampled(core: &mut Core) -> Vec<Header> {
        let headers: Vec<Header> = Block::initial_testnet_blocks()
            .into_iter()
            .map(|b| b.header)
            .collect();
        let blocks = vec![(100, headers[0].double_sha256().unwrap())];
        core.on_chain(ChainNews::Blocks {
            blocks,
            reset: false,
        });
        headers
    }

    fn reply(core: &mut Core, peer: &str, headers: &[Header]) {
        let message = Message::Headers(Headers::new(headers.to_vec()));
        core.on_node_event(NodeEvent::Message(addr(peer), message));
    }

    #[tokio::test]
    async fn a_headers_reply_is_a_height_event_once() {
        let (mut core, dir) = core("headers");
        let headers = sampled(&mut core);
        up(&mut core, A);
        core.ask_headers(addr(A), Instant::now());
        reply(&mut core, A, &headers[1..4]);
        reply(&mut core, A, &headers[1..3]);
        let events = logged(&dir, "height");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["peer"], A);
        assert_eq!(events[0]["height"], 103);
        assert_eq!(events[0]["at_most"], false);
        assert_eq!(core.connected[&addr(A)].height.unwrap().1, 103);
    }

    #[tokio::test]
    async fn a_reply_we_cannot_place_gives_no_height() {
        let (mut core, dir) = core("unplaced");
        let headers = sampled(&mut core);
        up(&mut core, A);
        core.ask_headers(addr(A), Instant::now());
        reply(&mut core, A, &headers[1..4]);
        core.ask_headers(addr(A), Instant::now());
        reply(&mut core, A, &headers[3..5]);
        let events = logged(&dir, "height");
        assert_eq!(events[1]["height"], Value::Null);
        assert_eq!(core.connected[&addr(A)].height, None);
    }

    #[tokio::test]
    async fn a_ready_peer_is_asked_every_interval() {
        let (mut core, _dir) = core("asked");
        up(&mut core, A);
        core.on_tick();
        // No sample yet, so there is nothing to ask with.
        assert!(core.connected[&addr(A)].asked.is_none());
        let headers = sampled(&mut core);
        core.on_node_event(NodeEvent::Version(addr(B), version(1)));
        core.on_tick();
        assert!(core.connected[&addr(A)].asked.is_some());
        assert!(core.connected[&addr(B)].asked.is_none());
        reply(&mut core, A, &headers[1..2]);
        core.on_tick();
        assert!(core.connected[&addr(A)].asked.is_none());
        let interval = Duration::from_secs(core.config.headers_poll_secs);
        core.connected.get_mut(&addr(A)).unwrap().last_headers = Some(Instant::now() - interval);
        core.on_tick();
        assert!(core.connected[&addr(A)].asked.is_some());
    }

    #[tokio::test]
    async fn an_unanswered_request_is_one_event() {
        let (mut core, dir) = core("silent");
        let headers = sampled(&mut core);
        up(&mut core, A);
        core.ask_headers(addr(A), Instant::now());
        reply(&mut core, A, &headers[1..2]);
        core.ask_headers(addr(A), Instant::now() - HEADERS_REPLY + TICK);
        core.on_tick();
        assert!(logged(&dir, "no_reply").is_empty());
        let peer = core.connected.get_mut(&addr(A)).unwrap();
        peer.asked.as_mut().unwrap().0 = Instant::now() - HEADERS_REPLY;
        core.on_tick();
        core.on_tick();
        assert_eq!(logged(&dir, "no_reply").len(), 1);
        assert_eq!(core.connected[&addr(A)].height, None);
        // A reply that comes after that answers nothing.
        reply(&mut core, A, &headers[1..2]);
        assert_eq!(logged(&dir, "height").len(), 1);
    }

    #[tokio::test]
    async fn blocks_come_all_or_none() {
        let block = |h: u32| async move {
            if h == 93 {
                Err("down".to_owned())
            } else {
                Ok(Hash::zeroed())
            }
        };
        assert_eq!(fetch_blocks(&[100, 93], block).await, None);
        let found = fetch_blocks(&[100, 98], block).await.unwrap();
        assert_eq!(found, [(100, Hash::zeroed()), (98, Hash::zeroed())]);
    }

    #[tokio::test]
    async fn a_poll_follows_only_an_agreed_tip() {
        let (news, mut received) = mpsc::unbounded_channel();
        let mut tips = Tips::default();
        let up = |_: u32| async { Ok(Hash::zeroed()) };
        let mut polled = |answers: &'static [u32], block_up: bool| {
            // Each poll ends at once, so there is nothing to wait for.
            let poll = async {
                if block_up {
                    on_tips(&mut tips, answers, 3, &news, up).await;
                } else {
                    on_tips(&mut tips, answers, 3, &news, |_| async {
                        Err("down".to_owned())
                    })
                    .await;
                }
            };
            futures_util::FutureExt::now_or_never(poll).expect("no waits");
            let mut got = Vec::new();
            while let Ok(item) = received.try_recv() {
                got.push(match item {
                    ChainNews::Tip(tip) => (tip, None),
                    ChainNews::Blocks { blocks, reset } => (blocks[0].0, Some(reset)),
                });
            }
            got
        };
        assert_eq!(polled(&[], true), []);
        // They disagree, so `gone` gets a tip and the locator gets nothing.
        assert_eq!(polled(&[3_000_000, 3_600_000], true), [(3_000_000, None)]);
        let agree: &[u32] = &[3_600_000, 3_600_000, 3_000_000];
        assert_eq!(polled(agree, false), [(3_600_000, None)]);
        let first = [(3_600_000, None), (3_599_997, Some(true))];
        assert_eq!(polled(agree, true), first);
        let next = [(3_600_000, None), (3_599_997, Some(false))];
        assert_eq!(polled(agree, true), next);
    }

    fn news(core: &mut Core, heights: &[u32], reset: bool) {
        let blocks = heights
            .iter()
            .map(|h| (*h, Hash::new([*h as u8; 32])))
            .collect();
        core.on_chain(ChainNews::Blocks { blocks, reset });
    }

    #[tokio::test]
    async fn a_reset_drops_the_earlier_samples() {
        let (mut core, _dir) = core("reset");
        news(&mut core, &[100, 90], false);
        news(&mut core, &[50], true);
        let heights: Vec<u32> = core.chain.locator().iter().map(|b| b.0).collect();
        assert_eq!(heights, [50]);
    }

    #[tokio::test]
    async fn no_peer_is_asked_with_old_blocks() {
        let (mut core, _dir) = core("stale");
        up(&mut core, A);
        news(&mut core, &[100], false);
        let stale = Instant::now() + CHAIN_FRESH;
        core.ask_headers(addr(A), stale);
        assert!(core.connected[&addr(A)].asked.is_none());
        // A server stuck on one block isn't news either.
        let before = core.chain_moved;
        news(&mut core, &[100], false);
        assert_eq!(core.chain_moved, before);
        news(&mut core, &[101], false);
        assert!(core.chain_moved > before);
        // A block under one that is too high is news.
        news(&mut core, &[200], false);
        let before = core.chain_moved;
        news(&mut core, &[102], false);
        assert!(core.chain_moved > before);
        // So is a jump to a lower tip.
        let before = core.chain_moved;
        news(&mut core, &[50], true);
        assert!(core.chain_moved > before);
        core.ask_headers(addr(A), Instant::now());
        assert!(core.connected[&addr(A)].asked.is_some());
    }

    #[tokio::test]
    async fn a_ceiling_is_marked_and_is_no_height() {
        let (mut core, dir) = core("ceiling");
        let headers = chain::full_reply();
        up(&mut core, A);
        core.on_chain(ChainNews::Blocks {
            blocks: vec![(900, Hash::zeroed()), (500, headers[0].prev_block)],
            reset: false,
        });
        core.ask_headers(addr(A), Instant::now());
        reply(&mut core, A, &headers);
        let events = logged(&dir, "height");
        assert_eq!(events[0]["height"], 899);
        assert_eq!(events[0]["at_most"], true);
        assert_eq!(events[0]["headers"], 160);
        assert_eq!(core.connected[&addr(A)].height, None);
    }

    #[tokio::test]
    async fn a_headers_message_reaches_the_core() {
        let (events, mut received) = mpsc::unbounded_channel();
        let listener = Listener::new("/test/".into(), 1, events, wire::WireLog::off());
        let message = Message::Headers(Headers::empty());
        Reading::process_message(&listener, addr(A), message)
            .await
            .unwrap();
        let event = received.try_recv().unwrap();
        assert!(matches!(event, NodeEvent::Message(_, Message::Headers(_))));
    }

    #[tokio::test]
    async fn a_peer_logs_its_fill_of_blocks_and_waits_a_minute() {
        let (mut core, dir) = core("fill");
        up(&mut core, A);
        let blocks: Vec<u16> = (0..BLOCKS_KEPT as u16 + 5).collect();
        for chunk in blocks.chunks(BLOCKS_PER_INV) {
            announce(&mut core, A, chunk);
        }
        assert_eq!(logged(&dir, "block").len(), BLOCKS_KEPT);
        let peer = core.connected.get_mut(&addr(A)).unwrap();
        peer.blocks[0].1 = Instant::now() - BLOCKS_WINDOW;
        announce(&mut core, A, &[9_000, 9_001]);
        assert_eq!(logged(&dir, "block").len(), BLOCKS_KEPT + 1);
        assert_eq!(core.connected[&addr(A)].blocks.len(), BLOCKS_KEPT);
    }

    #[tokio::test]
    async fn a_flood_of_blocks_costs_only_the_peer_that_sent_it() {
        let (mut core, dir) = core("flood");
        up(&mut core, A);
        up(&mut core, B);
        announce(&mut core, A, &[7]);
        let flood: Vec<u16> = (1_000..2_000).collect();
        for chunk in flood.chunks(BLOCKS_PER_INV + 10) {
            announce(&mut core, B, chunk);
        }
        announce(&mut core, B, &flood[..BLOCKS_PER_INV + 10]);
        assert_eq!(core.connected[&addr(B)].blocks.len(), BLOCKS_KEPT);
        // A's block is still known as A's.
        announce(&mut core, A, &[7]);
        let from_a = logged(&dir, "block")
            .iter()
            .filter(|e| e["peer"] == A)
            .count();
        assert_eq!(from_a, 1);
    }

    #[tokio::test]
    async fn an_inv_keeps_its_first_blocks_only() {
        let (mut core, dir) = core("cap");
        up(&mut core, A);
        announce(&mut core, A, &(0..500).collect::<Vec<u16>>());
        assert_eq!(logged(&dir, "block").len(), BLOCKS_PER_INV);
        // A peer before its verack has told us nothing yet.
        core.on_node_event(NodeEvent::Version(addr(B), version(1)));
        announce(&mut core, B, &[9_999]);
        assert!(logged(&dir, "block").iter().all(|e| e["peer"] == A));
    }
}
