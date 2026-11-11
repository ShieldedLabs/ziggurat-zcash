//! Peer choice from the newest census, and per-peer reconnect state.

use std::{
    collections::HashMap,
    fs::{self, File},
    io::{self, BufRead, BufReader},
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use flate2::read::GzDecoder;
use rand::seq::SliceRandom;
use serde::Deserialize;
use ziggurat_zcash::protocol::message::constants::NU7_PROTOCOL_VERSION;

/// The census fields the listener needs.
#[derive(Debug, Deserialize)]
struct CensusRecord {
    ip: std::net::IpAddr,
    port: u16,
    protocol_version: u32,
    user_agent: String,
    chain: String,
}

/// Returns the newest `census-*.ndjson.gz` in `dir`. The names sort by time.
pub fn newest_census(dir: &Path) -> io::Result<PathBuf> {
    let mut names: Vec<PathBuf> = fs::read_dir(dir)?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| {
            path.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("census-") && n.ends_with(".ndjson.gz"))
        })
        .collect();
    names.sort();
    names.pop().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("no census-*.ndjson.gz in {}", dir.display()),
        )
    })
}

/// Reads Zcash Zebra and Zakura nodes on the NU7 protocol from a census file.
///
/// Zebra and Zakura nodes alternate, each list shuffled, so any prefix spreads across both.
pub fn census_candidates(path: &Path) -> io::Result<Vec<SocketAddr>> {
    let reader = BufReader::new(GzDecoder::new(File::open(path)?));
    let mut zebra = Vec::new();
    let mut zakura = Vec::new();
    for line in reader.lines() {
        let Ok(record) = serde_json::from_str::<CensusRecord>(&line?) else {
            continue;
        };
        if record.chain != "zcash" || record.protocol_version < NU7_PROTOCOL_VERSION {
            continue;
        }
        let addr = SocketAddr::new(record.ip, record.port);
        if record.user_agent.starts_with("/Zebra:") {
            zebra.push(addr);
        } else if record.user_agent.starts_with("/Zakura:") {
            zakura.push(addr);
        }
    }

    let mut rng = rand::thread_rng();
    zebra.shuffle(&mut rng);
    zakura.shuffle(&mut rng);
    Ok(interleave(zebra, zakura))
}

fn interleave(a: Vec<SocketAddr>, b: Vec<SocketAddr>) -> Vec<SocketAddr> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut a, mut b) = (a.into_iter(), b.into_iter());
    loop {
        match (a.next(), b.next()) {
            (None, None) => return out,
            (x, y) => out.extend(x.into_iter().chain(y)),
        }
    }
}

#[derive(Debug, Default)]
struct PeerState {
    failures: u32,
    last_attempt: Option<Instant>,
    last_drop: Option<Instant>,
}

/// The target peer slots plus spares to replace failing peers.
pub struct PeerSet {
    targets: Vec<SocketAddr>,
    spares: Vec<SocketAddr>,
    state: HashMap<SocketAddr, PeerState>,
    reconnect: Duration,
    max_failures: u32,
}

impl PeerSet {
    pub fn new(
        mut candidates: Vec<SocketAddr>,
        target: usize,
        reconnect: Duration,
        max_failures: u32,
    ) -> Self {
        let spares = candidates.split_off(target.min(candidates.len()));
        Self {
            targets: candidates,
            spares: spares.into_iter().rev().collect(),
            state: HashMap::new(),
            reconnect,
            max_failures,
        }
    }

    pub fn targets(&self) -> &[SocketAddr] {
        &self.targets
    }

    /// Returns the peers to dial now. `is_up` says if a peer is connected or connecting.
    pub fn due(&mut self, now: Instant, is_up: impl Fn(SocketAddr) -> bool) -> Vec<SocketAddr> {
        self.replace_failing();
        let mut due = Vec::new();
        for addr in &self.targets {
            if is_up(*addr) {
                continue;
            }
            let state = self.state.entry(*addr).or_default();
            let last = state.last_attempt.max(state.last_drop);
            if last.is_none_or(|last| now.duration_since(last) >= self.reconnect) {
                state.last_attempt = Some(now);
                due.push(*addr);
            }
        }
        due
    }

    pub fn connected(&mut self, addr: SocketAddr) {
        self.state.entry(addr).or_default().failures = 0;
    }

    pub fn failed(&mut self, addr: SocketAddr) {
        self.state.entry(addr).or_default().failures += 1;
    }

    pub fn dropped(&mut self, addr: SocketAddr, now: Instant) {
        self.state.entry(addr).or_default().last_drop = Some(now);
    }

    fn replace_failing(&mut self) {
        for slot in 0..self.targets.len() {
            let addr = self.targets[slot];
            let failing = self
                .state
                .get(&addr)
                .is_some_and(|s| s.failures >= self.max_failures);
            if failing {
                if let Some(spare) = self.spares.pop() {
                    self.targets[slot] = spare;
                    self.state.remove(&addr);
                    // Retry the failed peer once the spares run out.
                    self.spares.insert(0, addr);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flate2::{write::GzEncoder, Compression};

    use super::*;

    fn addr(i: u8) -> SocketAddr {
        ([10, 0, 0, i], 8233).into()
    }

    #[test]
    fn candidates_filter_and_interleave() {
        let dir = std::env::temp_dir().join(format!("mempool-peers-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let lines = [
            r#"{"ip":"10.0.0.1","port":8233,"protocol_version":170190,"user_agent":"/Zebra:7.0.0/","chain":"zcash"}"#,
            r#"{"ip":"10.0.0.2","port":8233,"protocol_version":170190,"user_agent":"/Zebra:7.0.0/","chain":"zcash"}"#,
            r#"{"ip":"10.0.0.3","port":8233,"protocol_version":170190,"user_agent":"/Zakura:1.8.0/","chain":"zcash"}"#,
            r#"{"ip":"10.0.0.4","port":8233,"protocol_version":170140,"user_agent":"/Zebra:6.0.0/","chain":"zcash"}"#,
            r#"{"ip":"10.0.0.5","port":8233,"protocol_version":170190,"user_agent":"/MagicBean:6.3.0/","chain":"zcash"}"#,
            r#"{"ip":"10.0.0.6","port":16125,"protocol_version":170190,"user_agent":"/Zebra:7.0.0/","chain":"flux"}"#,
        ];
        for name in [
            "census-20261101T000000Z.ndjson.gz",
            "census-20261108T000000Z.ndjson.gz",
        ] {
            let mut gz = GzEncoder::new(File::create(dir.join(name)).unwrap(), Compression::fast());
            writeln!(gz, "{}", lines.join("\n")).unwrap();
            gz.finish().unwrap();
        }

        let newest = newest_census(&dir).unwrap();
        assert!(newest.ends_with("census-20261108T000000Z.ndjson.gz"));
        let candidates = census_candidates(&newest).unwrap();
        assert_eq!(candidates.len(), 3);
        // Zebra, Zakura, Zebra.
        assert_eq!(candidates[1], addr(3));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn reconnect_waits_and_spares_replace_failures() {
        let reconnect = Duration::from_secs(120);
        let mut set = PeerSet::new(vec![addr(1), addr(2), addr(3)], 2, reconnect, 2);
        let t0 = Instant::now();
        assert_eq!(set.due(t0, |_| false), vec![addr(1), addr(2)]);
        // Too soon for a second attempt.
        assert!(set.due(t0 + Duration::from_secs(60), |_| false).is_empty());

        set.failed(addr(1));
        set.failed(addr(1));
        let due = set.due(t0 + reconnect, |a| a == addr(2));
        assert_eq!(due, vec![addr(3)]);
        assert_eq!(set.targets(), &[addr(3), addr(2)]);

        set.dropped(addr(2), t0 + reconnect);
        assert!(set.due(t0 + reconnect, |a| a == addr(3)).is_empty());
        assert_eq!(set.due(t0 + 2 * reconnect, |a| a == addr(3)), vec![addr(2)]);
    }
}
