use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use time::OffsetDateTime;
use ziggurat_core_crawler::connection::KnownConnection;
use ziggurat_zcash::protocol::payload::{addr::NetworkAddr, ProtocolVersion, VarStr};

/// The elapsed time before a connection should be regarded as inactive.
pub const LAST_SEEN_CUTOFF: u64 = 10 * 60;
/// The wait before the crawler reconnects to a node that sent its Version.
/// Zebra and Zakura drop an inbound connection from an IP that connected in the last
/// 119 seconds (`MIN_PEER_RECONNECTION_DELAY`), so this must be longer.
pub const RECONNECT_INTERVAL: Duration = Duration::from_secs(5 * 60);
/// The first wait before the crawler retries an address that failed at the TCP level.
/// The wait doubles with each failure, up to 32 minutes.
const RETRY_BACKOFF_BASE: Duration = Duration::from_secs(60);
const RETRY_BACKOFF_MAX_DOUBLINGS: u8 = 5;

/// The addresses kept from one node, so a peer that invents them can't fill memory.
const MAX_GOSSIP_PER_SOURCE: usize = 5_000;
/// Gossip entries kept in all, about 160 MB. The per-source cap alone lets one host
/// with many ports fill memory, since every ip:port is its own source.
const MAX_GOSSIP_TOTAL: usize = 2_000_000;

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub enum ConnectionState {
    /// The node is not connected.
    #[default]
    Disconnected,
    /// The node is connected.
    Connected,
}

/// A node encountered in the network or obtained from one of the peers.
#[derive(Debug, Default, Clone)]
pub struct KnownNode {
    // The address is omitted, as it's a key in the owning HashMap.
    /// The last time the node was successfully connected to.
    pub last_connected: Option<Instant>,
    /// The time it took to complete a connection.
    pub handshake_time: Option<Duration>,
    /// The node's protocol version.
    pub protocol_version: Option<ProtocolVersion>,
    /// The node's user agent.
    pub user_agent: Option<VarStr>,
    /// The node's block height.
    pub start_height: Option<i32>,
    /// The number of services supported by the node.
    pub services: Option<u64>,
    /// The node's transaction relay flag.
    pub relay: Option<bool>,
    /// The first time the node's Version message arrived.
    pub first_seen: Option<DateTime<Utc>>,
    /// The latest time the node's Version message arrived.
    pub last_seen: Option<DateTime<Utc>>,
    /// The number of subsequent connection errors.
    pub connection_failures: u8,
    /// The number of connection attempts.
    pub connection_attempts: u32,
    /// How many Version messages the node sent us.
    pub versions: u32,
    /// The node's clock minus ours, in seconds, at its latest Version.
    pub clock_offset_s: Option<i64>,
    /// The time of the latest connection attempt.
    pub last_attempt: Option<Instant>,
    /// Why the node sent no Version message on the latest attempt.
    pub last_failure: Option<String>,
    /// The node's state.
    pub state: ConnectionState,
}

/// Why a node is due for a connection attempt. The crawler tries the classes in this order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Retry {
    /// The node accepted TCP but closed before its Version message, because its inbound
    /// slots were full. A slot can free up at any time, so the crawler retries on every
    /// crawl loop. Zebra does not count these rejected attempts against our IP.
    SlotsFull,
    /// The crawler never tried the address.
    New,
    /// The node sent its Version before, or the address failed at the TCP level.
    Later,
}

impl KnownNode {
    /// Records a Version message that says `theirs` and arrived at `ours`.
    pub fn saw_version(&mut self, theirs: OffsetDateTime, ours: OffsetDateTime) {
        self.versions += 1;
        self.clock_offset_s = Some((theirs - ours).whole_seconds());
    }

    /// Returns why the node is due for a connection attempt at `now`, or `None` if it is not due.
    pub fn retry(&self, now: Instant) -> Option<Retry> {
        let since = |time: Option<Instant>| {
            time.map_or(Duration::MAX, |time| now.saturating_duration_since(time))
        };

        if self.protocol_version.is_some() {
            (since(self.last_connected) >= RECONNECT_INTERVAL).then_some(Retry::Later)
        } else if self.last_connected.is_some() && self.connection_failures == 0 {
            Some(Retry::SlotsFull)
        } else if self.connection_attempts == 0 {
            Some(Retry::New)
        } else {
            let doublings = self
                .connection_failures
                .saturating_sub(1)
                .min(RETRY_BACKOFF_MAX_DOUBLINGS);
            let backoff = RETRY_BACKOFF_BASE * (1 << doublings);
            (since(self.last_attempt) >= backoff).then_some(Retry::Later)
        }
    }
}

/// An address as one node described it.
#[derive(Debug, Clone, Copy)]
pub struct GossipedAddr {
    pub services: u64,
    /// When that node says it last saw the address, in Unix seconds.
    pub last_seen: i64,
}

/// What each node told us about other addresses. It proves the node heard of an address.
pub type Gossip = HashMap<SocketAddr, HashMap<SocketAddr, GossipedAddr>>;

/// The list of nodes and connections the crawler is aware of.
#[derive(Default)]
pub struct KnownNetwork {
    pub nodes: RwLock<HashMap<SocketAddr, KnownNode>>,
    pub connections: RwLock<HashSet<KnownConnection>>,
    pub gossip: RwLock<Gossip>,
    /// Entries in `gossip`, counted as they are added, so the total cap needs no walk.
    gossip_held: std::sync::atomic::AtomicUsize,
}

impl KnownNetwork {
    /// Extends the list of known nodes and connections.
    pub fn add_addrs(&self, source: SocketAddr, listening_addrs: &[SocketAddr]) {
        {
            let connections = &mut self.connections.write();
            for addr in listening_addrs {
                connections.insert(KnownConnection::new(source, *addr));
            }
        }
        let mut nodes = self.nodes.write();
        nodes.entry(source).or_default();
        listening_addrs.iter().for_each(|addr| {
            nodes.entry(*addr).or_default();
        });
    }

    /// Keeps what `source` said about other addresses, the newest sighting of each.
    pub fn add_gossip(&self, source: SocketAddr, addrs: &[NetworkAddr]) {
        self.add_gossip_capped(source, addrs, MAX_GOSSIP_TOTAL);
    }

    fn add_gossip_capped(&self, source: SocketAddr, addrs: &[NetworkAddr], total_cap: usize) {
        use std::sync::atomic::Ordering::Relaxed;
        let mut gossip = self.gossip.write(); // the count only changes under this lock
        let known = gossip.entry(source).or_default();
        for addr in addrs {
            let new = GossipedAddr {
                services: addr.services,
                last_seen: addr.last_seen.map_or(0, |time| time.unix_timestamp()),
            };
            let room =
                known.len() < MAX_GOSSIP_PER_SOURCE && self.gossip_held.load(Relaxed) < total_cap;
            let keep = known
                .get(&addr.addr)
                .map_or(room, |old| new.last_seen > old.last_seen);
            if keep && known.insert(addr.addr, new).is_none() {
                self.gossip_held.fetch_add(1, Relaxed);
            }
        }
    }

    /// Sets the node's connection state.
    pub fn set_node_state(&self, addr: SocketAddr, state: ConnectionState) {
        if let Some(node) = self.nodes.write().get_mut(&addr) {
            node.state = state;
        }
    }

    /// Returns a snapshot of the known connections.
    pub fn connections(&self) -> HashSet<KnownConnection> {
        self.connections.read().clone()
    }

    /// Returns a snapshot of the known nodes.
    pub fn nodes(&self) -> HashMap<SocketAddr, KnownNode> {
        self.nodes.read().clone()
    }

    /// Returns the number of known connections.
    pub fn num_connections(&self) -> usize {
        self.connections.read().len()
    }

    /// Returns the number of known nodes.
    pub fn num_nodes(&self) -> usize {
        self.nodes.read().len()
    }

    /// Prunes the list of known connections by removing connections last seen long ago.
    pub fn remove_old_connections(&self) {
        let mut old_conns: HashSet<KnownConnection> = HashSet::new();
        for conn in self.connections() {
            if conn.last_seen.elapsed().as_secs() > LAST_SEEN_CUTOFF {
                old_conns.insert(conn);
            }
        }

        if !old_conns.is_empty() {
            let mut conns = self.connections.write();
            for conn in old_conns {
                conns.remove(&conn);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_test() {
        let now = Instant::now();
        let ago = |secs| Some(now - Duration::from_secs(secs));

        // A new address is due.
        assert_eq!(KnownNode::default().retry(now), Some(Retry::New));

        // A node that sent its Version waits out Zebra's per-IP window.
        let handshaken = KnownNode {
            connection_attempts: 1,
            last_attempt: ago(130),
            last_connected: ago(130),
            protocol_version: Some(ProtocolVersion(170_190)),
            ..Default::default()
        };
        assert_eq!(handshaken.retry(now), None);
        let handshaken = KnownNode {
            last_connected: ago(RECONNECT_INTERVAL.as_secs()),
            ..handshaken
        };
        assert_eq!(handshaken.retry(now), Some(Retry::Later));

        // A node with full inbound slots is due on every crawl loop.
        let slots_full = KnownNode {
            connection_attempts: 1,
            last_attempt: ago(1),
            last_connected: ago(1),
            ..Default::default()
        };
        assert_eq!(slots_full.retry(now), Some(Retry::SlotsFull));

        // A TCP failure backs off: 1 minute, then doubles up to 32 minutes.
        let failed = |failures, secs| KnownNode {
            connection_attempts: 1,
            connection_failures: failures,
            last_attempt: ago(secs),
            ..Default::default()
        };
        assert_eq!(failed(1, 59).retry(now), None);
        assert_eq!(failed(1, 60).retry(now), Some(Retry::Later));
        assert_eq!(failed(3, 239).retry(now), None);
        assert_eq!(failed(3, 240).retry(now), Some(Retry::Later));
        assert_eq!(failed(u8::MAX, 32 * 60 - 1).retry(now), None);
        assert_eq!(failed(u8::MAX, 32 * 60).retry(now), Some(Retry::Later));
    }

    #[test]
    fn gossip_keeps_the_newest_sighting_of_each_address() {
        let addr = |n: u8| SocketAddr::from(([192, 0, 2, n], 8233));
        let seen = |n, secs| NetworkAddr {
            last_seen: Some(OffsetDateTime::from_unix_timestamp(secs).unwrap()),
            services: 1,
            addr: addr(n),
        };
        let network = KnownNetwork::default();
        network.add_gossip(addr(1), &[seen(7, 100), seen(8, 100)]);
        network.add_gossip(addr(1), &[seen(7, 200), seen(8, 50)]);
        network.add_gossip(addr(2), &[seen(7, 10)]);
        let gossip = network.gossip.read();
        assert_eq!(gossip[&addr(1)][&addr(7)].last_seen, 200);
        assert_eq!(gossip[&addr(1)][&addr(8)].last_seen, 100);
        assert_eq!(gossip[&addr(2)][&addr(7)].last_seen, 10);
    }

    #[test]
    fn gossip_from_one_source_is_capped() {
        let source = SocketAddr::from(([192, 0, 2, 1], 8233));
        let network = KnownNetwork::default();
        let addrs: Vec<NetworkAddr> = (1..=MAX_GOSSIP_PER_SOURCE as u32 + 5)
            .map(|n| NetworkAddr::new(SocketAddr::from((std::net::Ipv4Addr::from(n), 1))))
            .collect();
        network.add_gossip(source, &addrs);
        assert_eq!(network.gossip.read()[&source].len(), MAX_GOSSIP_PER_SOURCE);
    }

    #[test]
    fn gossip_in_total_is_capped_so_many_ports_of_one_host_cannot_fill_memory() {
        let network = KnownNetwork::default();
        let addrs = |from: u32| -> Vec<NetworkAddr> {
            (from..from + 4)
                .map(|n| NetworkAddr::new(SocketAddr::from((std::net::Ipv4Addr::from(n), 1))))
                .collect()
        };
        for port in 1..=5 {
            network.add_gossip_capped(
                SocketAddr::from(([192, 0, 2, 1], port)),
                &addrs(port as u32 * 100),
                10,
            );
        }
        let held: usize = network
            .gossip
            .read()
            .values()
            .map(|known| known.len())
            .sum();
        assert_eq!(held, 10);
        // A newer sighting of an address already held still replaces it at the cap.
        let mut newer = addrs(100);
        newer[0].last_seen = Some(OffsetDateTime::from_unix_timestamp(4_000_000_000).unwrap()); // NetworkAddr::new stamps now
        network.add_gossip_capped(SocketAddr::from(([192, 0, 2, 1], 1)), &newer[..1], 10);
        let source = SocketAddr::from(([192, 0, 2, 1], 1));
        assert_eq!(
            network.gossip.read()[&source][&newer[0].addr].last_seen,
            4_000_000_000
        );
    }

    #[test]
    fn a_replaced_gossip_entry_is_not_counted_as_a_new_one() {
        let network = KnownNetwork::default();
        let source = SocketAddr::from(([192, 0, 2, 1], 8233));
        let at = |n: u32, secs| NetworkAddr {
            last_seen: Some(OffsetDateTime::from_unix_timestamp(secs).unwrap()),
            services: 1,
            addr: SocketAddr::from((std::net::Ipv4Addr::from(n), 1)),
        };
        // Under the cap, replace one address twice with newer sightings.
        for secs in [100, 200, 300] {
            network.add_gossip_capped(source, &[at(1, secs)], 4);
        }
        // Then fill: the cap is 4, so 3 more fit only if the replacements weren't counted.
        network.add_gossip_capped(source, &[at(2, 100), at(3, 100), at(4, 100)], 4);
        assert_eq!(network.gossip.read()[&source].len(), 4);
    }

    #[test]
    fn a_version_records_its_count_and_clock_offset() {
        let at = |secs| OffsetDateTime::from_unix_timestamp(secs).unwrap();
        let mut node = KnownNode::default();
        node.saw_version(at(1_000), at(1_300));
        node.saw_version(at(2_000), at(1_990));
        assert_eq!((node.versions, node.clock_offset_s), (2, Some(10)));
    }
}
