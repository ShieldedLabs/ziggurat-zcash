//! Per-node census export (NDJSON.gz) and the NU7 readiness summary.

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    fs::{self, File},
    io::{self, BufWriter, Write},
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
};

use chrono::{DateTime, SecondsFormat, Utc};
use flate2::{write::GzEncoder, Compression};
use serde::Serialize;

use crate::{network::KnownNode, protocol::NU7_PROTOCOL_VERSION};

/// Flux's P2P port. Flux forked zcashd and answers on the Zcash mainnet magic.
pub const FLUX_P2P_PORT: u16 = 16125;
/// A start height further than this from the Zcash tip marks a Flux node.
/// About 87 days of Zcash blocks, so a lagging Zcash node still counts as Zcash.
pub const TIP_TOLERANCE_BLOCKS: u32 = 100_000;
/// The number of failure reasons the summary lists.
const MAX_FAILURE_REASONS: usize = 8;

/// The chain a node belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Chain {
    Zcash,
    Flux,
}

/// Classifies a node as Flux or Zcash.
///
/// A node is Flux if it listens on Flux's port, names Flux in its user agent, or reports a
/// start height far from the Zcash tip. The height rule is off when the tip is unknown.
pub fn classify_chain(
    port: u16,
    user_agent: &str,
    start_height: i32,
    zcash_tip: Option<i32>,
) -> Chain {
    let far_from_tip =
        zcash_tip.is_some_and(|tip| start_height.abs_diff(tip) > TIP_TOLERANCE_BLOCKS);

    if port == FLUX_P2P_PORT || user_agent.to_lowercase().contains("flux") || far_from_tip {
        Chain::Flux
    } else {
        Chain::Zcash
    }
}

/// A node implementation, in summary order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Implementation {
    Zebra,
    Zakura,
    Zcashd,
    Other,
}

impl fmt::Display for Implementation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Zebra => "Zebra",
            Self::Zakura => "Zakura",
            Self::Zcashd => "zcashd (MagicBean)",
            Self::Other => "other",
        };
        f.write_str(name)
    }
}

/// Splits a user agent such as "/Zebra:4.1.0/" into its implementation and version.
/// An unknown implementation keeps the full user agent as its version.
fn implementation(user_agent: &str) -> (Implementation, &str) {
    let first = user_agent
        .trim_start_matches('/')
        .split('/')
        .next()
        .unwrap_or("");
    let (name, version) = first.split_once(':').unwrap_or((first, ""));

    match name {
        "Zebra" => (Implementation::Zebra, version),
        "Zakura" => (Implementation::Zakura, version),
        "MagicBean" => (Implementation::Zcashd, version),
        _ => (Implementation::Other, user_agent),
    }
}

/// Median start height of the Zebra and Zakura nodes.
fn median_zebra_zakura_height(nodes: &HashMap<SocketAddr, KnownNode>) -> Option<i32> {
    let mut heights: Vec<i32> = nodes
        .values()
        .filter_map(|node| {
            let (implementation, _) = implementation(&node.user_agent.as_ref()?.0);
            matches!(
                implementation,
                Implementation::Zebra | Implementation::Zakura
            )
            .then_some(node.start_height?)
        })
        .collect();
    heights.sort_unstable();

    heights.get(heights.len() / 2).copied()
}

fn rfc3339(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// One NDJSON line of the census export: a node that sent its Version message.
#[derive(Debug, Serialize)]
pub struct CensusRecord<'a> {
    ip: IpAddr,
    port: u16,
    protocol_version: u32,
    user_agent: &'a str,
    services: u64,
    start_height: i32,
    relay: bool,
    first_seen: String,
    last_seen: String,
    handshake_ms: Option<u64>,
    chain: Chain,
}

impl<'a> CensusRecord<'a> {
    /// Returns `None` for a node that never sent its Version message.
    fn new(addr: SocketAddr, node: &'a KnownNode, zcash_tip: Option<i32>) -> Option<Self> {
        let user_agent = node.user_agent.as_ref()?.0.as_str();
        let start_height = node.start_height?;

        Some(Self {
            ip: addr.ip(),
            port: addr.port(),
            protocol_version: node.protocol_version?.0,
            user_agent,
            services: node.services?,
            start_height,
            relay: node.relay?,
            first_seen: rfc3339(node.first_seen?),
            last_seen: rfc3339(node.last_seen?),
            handshake_ms: node.handshake_time.map(|time| time.as_millis() as u64),
            chain: classify_chain(addr.port(), user_agent, start_height, zcash_tip),
        })
    }
}

/// How far the crawl got with the known addresses.
#[derive(Debug, Default, PartialEq)]
struct Funnel<'a> {
    known: usize,
    attempted: usize,
    /// TCP connected, and our Version message sent.
    connected: usize,
    /// Why attempted nodes sent no Version message, most common first.
    failures: Vec<(&'a str, usize)>,
}

impl<'a> Funnel<'a> {
    fn new(nodes: &'a HashMap<SocketAddr, KnownNode>) -> Self {
        let mut funnel = Funnel {
            known: nodes.len(),
            ..Default::default()
        };
        let mut failures: HashMap<&str, usize> = HashMap::new();

        for node in nodes.values().filter(|node| node.connection_attempts > 0) {
            funnel.attempted += 1;
            funnel.connected += node.last_connected.is_some() as usize;

            if node.protocol_version.is_none() {
                let reason = match (&node.last_failure, node.last_connected) {
                    (Some(failure), _) => failure.as_str(),
                    (None, Some(_)) => "connected, no Version yet",
                    (None, None) => "connect in progress",
                };
                *failures.entry(reason).or_default() += 1;
            }
        }

        funnel.failures = failures.into_iter().collect();
        funnel
            .failures
            .sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
        funnel
    }
}

/// The census of all nodes that sent their Version message.
pub struct Census<'a> {
    /// The tip used for the height rule: the given tip, else the Zebra and Zakura median.
    zcash_tip: Option<i32>,
    records: Vec<CensusRecord<'a>>,
    funnel: Funnel<'a>,
}

impl<'a> Census<'a> {
    pub fn new(nodes: &'a HashMap<SocketAddr, KnownNode>, zcash_tip: Option<i32>) -> Self {
        let zcash_tip = zcash_tip.or_else(|| median_zebra_zakura_height(nodes));

        let mut records: Vec<_> = nodes
            .iter()
            .filter_map(|(addr, node)| CensusRecord::new(*addr, node, zcash_tip))
            .collect();
        records.sort_by_key(|record| (record.ip, record.port));

        Self {
            zcash_tip,
            records,
            funnel: Funnel::new(nodes),
        }
    }

    /// Writes `census-<run start>.ndjson.gz` into `dir` and returns its path.
    ///
    /// Each call replaces the file of the same run. The write goes to a temporary file
    /// first, so a reader never sees a partial file.
    pub fn write_ndjson_gz(&self, dir: &Path, run_start: DateTime<Utc>) -> io::Result<PathBuf> {
        fs::create_dir_all(dir)?;
        let path = dir.join(format!(
            "census-{}.ndjson.gz",
            run_start.format("%Y%m%dT%H%M%SZ")
        ));
        let tmp_path = path.with_extension("gz.tmp");

        let file = BufWriter::new(File::create(&tmp_path)?);
        let mut gz = GzEncoder::new(file, Compression::default());
        for record in &self.records {
            serde_json::to_writer(&mut gz, record)?;
            gz.write_all(b"\n")?;
        }
        gz.finish()?.flush()?;
        fs::rename(&tmp_path, &path)?;

        Ok(path)
    }
}

/// The census summary. It excludes Flux nodes.
impl fmt::Display for Census<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn percent(part: usize, total: usize) -> f64 {
            if total == 0 {
                0.0
            } else {
                100.0 * part as f64 / total as f64
            }
        }

        // Implementation -> (version -> node count, NU7 node count).
        let mut implementations: BTreeMap<Implementation, (HashMap<&str, usize>, usize)> =
            BTreeMap::new();
        let mut num_zcash = 0;
        let mut num_nu7 = 0;
        for record in self.records.iter().filter(|r| r.chain == Chain::Zcash) {
            let (implementation, version) = implementation(record.user_agent);
            let nu7 = record.protocol_version >= NU7_PROTOCOL_VERSION;
            let (versions, impl_nu7) = implementations.entry(implementation).or_default();
            *versions.entry(version).or_default() += 1;
            *impl_nu7 += nu7 as usize;
            num_zcash += 1;
            num_nu7 += nu7 as usize;
        }

        writeln!(
            f,
            "Known addresses: {}, attempted: {}, TCP connected: {}, sent Version: {}",
            self.funnel.known,
            self.funnel.attempted,
            self.funnel.connected,
            self.records.len()
        )?;
        writeln!(f, "Top reasons for no Version:")?;
        for (reason, count) in self.funnel.failures.iter().take(MAX_FAILURE_REASONS) {
            writeln!(f, "  {reason}: {count}")?;
        }

        writeln!(f, "Zcash census (Flux excluded)")?;
        match self.zcash_tip {
            Some(tip) => writeln!(f, "Zcash tip for the height rule: {tip}")?,
            None => writeln!(f, "Zcash tip for the height rule: unknown, rule off")?,
        }
        writeln!(
            f,
            "Nodes with a Version message: {} ({num_zcash} Zcash, {} Flux)",
            self.records.len(),
            self.records.len() - num_zcash
        )?;
        writeln!(
            f,
            "Advertising protocol {NU7_PROTOCOL_VERSION} or higher: {num_nu7} of {num_zcash} ({:.1}%)",
            percent(num_nu7, num_zcash)
        )?;

        for (implementation, (versions, impl_nu7)) in &implementations {
            let total: usize = versions.values().sum();
            writeln!(
                f,
                "{implementation}: {total} ({impl_nu7} at {NU7_PROTOCOL_VERSION}+)"
            )?;

            let mut versions: Vec<_> = versions.iter().collect();
            versions.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
            for (version, count) in versions {
                writeln!(f, "  {version}: {count}")?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use chrono::TimeZone;
    use ziggurat_zcash::protocol::payload::{ProtocolVersion, VarStr};

    use super::*;

    const TIP: Option<i32> = Some(3_500_000);

    fn node(user_agent: &str, start_height: i32) -> KnownNode {
        let seen = Utc.with_ymd_and_hms(2026, 10, 12, 14, 0, 0).unwrap();
        KnownNode {
            connection_attempts: 1,
            last_connected: Some(Instant::now()),
            handshake_time: Some(Duration::from_millis(42)),
            protocol_version: Some(ProtocolVersion(NU7_PROTOCOL_VERSION)),
            user_agent: Some(VarStr(user_agent.into())),
            start_height: Some(start_height),
            services: Some(1),
            relay: Some(true),
            first_seen: Some(seen),
            last_seen: Some(seen + chrono::Duration::minutes(5)),
            ..Default::default()
        }
    }

    #[test]
    fn classify_chain_test() {
        // Flux by port, by user agent (any case), and by height.
        assert_eq!(
            classify_chain(FLUX_P2P_PORT, "/Zebra:4.1.0/", 3_500_000, TIP),
            Chain::Flux
        );
        assert_eq!(
            classify_chain(8233, "/MagicBean:9.0.6(flux)/", 3_500_000, TIP),
            Chain::Flux
        );
        assert_eq!(
            classify_chain(8233, "/MagicBean:9.0.6(FLUX)/", 3_500_000, TIP),
            Chain::Flux
        );
        assert_eq!(
            classify_chain(8233, "/MagicBean:9.0.6/", 1_900_000, TIP),
            Chain::Flux
        );
        assert_eq!(
            classify_chain(8233, "/Zebra:4.1.0/", i32::MIN, TIP),
            Chain::Flux
        );

        // Zcash: near the tip, a lagging node, and no tip at all.
        assert_eq!(
            classify_chain(8233, "/Zebra:4.1.0/", 3_500_010, TIP),
            Chain::Zcash
        );
        assert_eq!(
            classify_chain(8233, "/MagicBean:6.2.0/", 3_450_000, TIP),
            Chain::Zcash
        );
        assert_eq!(
            classify_chain(8233, "/MagicBean:9.0.6/", 1_900_000, None),
            Chain::Zcash
        );
    }

    #[test]
    fn ndjson_line_test() {
        let node = node("/Zebra:4.1.0/", 3_500_000);
        let record = CensusRecord::new("192.0.2.1:8233".parse().unwrap(), &node, TIP).unwrap();

        assert_eq!(
            serde_json::to_string(&record).unwrap(),
            concat!(
                r#"{"ip":"192.0.2.1","port":8233,"protocol_version":170190,"#,
                r#""user_agent":"/Zebra:4.1.0/","services":1,"start_height":3500000,"#,
                r#""relay":true,"first_seen":"2026-10-12T14:00:00Z","#,
                r#""last_seen":"2026-10-12T14:05:00Z","handshake_ms":42,"chain":"zcash"}"#
            )
        );

        // A node without a Version message has no line.
        assert!(CensusRecord::new(
            "192.0.2.2:8233".parse().unwrap(),
            &KnownNode::default(),
            TIP
        )
        .is_none());
    }

    #[test]
    fn census_summary_test() {
        let nodes = HashMap::from([
            (
                "192.0.2.1:8233".parse().unwrap(),
                node("/Zebra:4.1.0/", 3_500_000),
            ),
            (
                "192.0.2.2:8233".parse().unwrap(),
                node("/Zakura:1.3.1/", 3_500_002),
            ),
            (
                "192.0.2.3:8233".parse().unwrap(),
                node("/Zebra:4.0.0/", 3_499_000),
            ),
            (
                "192.0.2.4:16125".parse().unwrap(),
                node("/MagicBean:9.0.6/", 1_900_000),
            ),
            (
                "192.0.2.5:8233".parse().unwrap(),
                KnownNode {
                    connection_attempts: 3,
                    last_connected: Some(Instant::now()),
                    last_failure: Some("closed before Version".into()),
                    ..Default::default()
                },
            ),
            ("192.0.2.6:8233".parse().unwrap(), KnownNode::default()),
        ]);

        let census = Census::new(&nodes, None);
        assert_eq!(census.zcash_tip, Some(3_500_000));
        assert_eq!(census.records.len(), 4);
        assert_eq!(census.records[3].chain, Chain::Flux);

        // The summary explains the funnel and excludes the Flux node.
        assert_eq!(
            census.to_string(),
            "Known addresses: 6, attempted: 5, TCP connected: 5, sent Version: 4\n\
             Top reasons for no Version:\n  \
             closed before Version: 1\n\
             Zcash census (Flux excluded)\n\
             Zcash tip for the height rule: 3500000\n\
             Nodes with a Version message: 4 (3 Zcash, 1 Flux)\n\
             Advertising protocol 170190 or higher: 3 of 3 (100.0%)\n\
             Zebra: 2 (2 at 170190+)\n  \
             4.0.0: 1\n  \
             4.1.0: 1\n\
             Zakura: 1 (1 at 170190+)\n  \
             1.3.1: 1\n"
        );

        // The flag overrides the estimate.
        assert_eq!(Census::new(&nodes, Some(42)).zcash_tip, Some(42));
    }

    #[test]
    fn write_ndjson_gz_test() {
        use std::io::Read;

        use flate2::read::GzDecoder;

        let nodes = HashMap::from([
            (
                "192.0.2.1:8233".parse().unwrap(),
                node("/Zebra:4.1.0/", 3_500_000),
            ),
            ("192.0.2.2:8233".parse().unwrap(), KnownNode::default()),
        ]);
        let dir = std::env::temp_dir().join(format!("census-test-{}", std::process::id()));
        let run_start = Utc.with_ymd_and_hms(2026, 10, 12, 14, 0, 0).unwrap();

        let path = Census::new(&nodes, TIP)
            .write_ndjson_gz(&dir, run_start)
            .unwrap();
        assert_eq!(path, dir.join("census-20261012T140000Z.ndjson.gz"));

        let mut ndjson = String::new();
        GzDecoder::new(File::open(&path).unwrap())
            .read_to_string(&mut ndjson)
            .unwrap();
        assert_eq!(ndjson.lines().count(), 1);
        assert!(ndjson.ends_with("\"chain\":\"zcash\"}\n"));

        fs::remove_dir_all(&dir).unwrap();
    }
}
