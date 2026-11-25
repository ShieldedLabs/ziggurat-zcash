//! The chain as the lookup servers show it, and what a peer's headers reply says of its
//! height. The servers are public, so a height is only as good as their block hashes.

use std::collections::BTreeMap;

use ziggurat_zcash::protocol::payload::{block::Header, Hash};

/// The newest sample sits this far under the tip, because older Zebra sends nothing when
/// it has no header to add.
pub const TIP_MARGIN: u32 = 3;
/// How far under the newest sample a locator reaches: dense near the tip, and back past NU7.
pub const OFFSETS: [u32; 13] = [
    0, 2, 7, 17, 47, 97, 400, 1000, 2000, 5000, 25_000, 30_000, 100_000,
];
/// The most headers Zebra sends in one reply.
const MAX_HEADERS: usize = 160;
/// Below this far under the newest sample, one sample per `SPARSE` blocks stays.
const DENSE: u32 = 400;
const SPARSE: u32 = 1000;

/// The least change of the tip that counts as a jump.
const JUMP: u32 = 100;

/// Blocks by height. A `Locator` is a pick of them, newest first.
pub type Locator = Vec<(u32, Hash)>;

#[derive(Default)]
pub struct Chain {
    samples: BTreeMap<u32, Hash>,
}

impl Chain {
    pub fn insert(&mut self, height: u32, hash: Hash) {
        self.samples.insert(height, hash);
        let newest = self.newest().expect("just inserted");
        let mut last = None;
        self.samples.retain(|&height, _| {
            let bucket = height / SPARSE;
            newest - height <= DENSE || last.replace(bucket) != Some(bucket)
        });
    }

    pub fn has(&self, height: u32) -> bool {
        self.samples.contains_key(&height)
    }

    fn newest(&self) -> Option<u32> {
        self.samples.last_key_value().map(|(height, _)| *height)
    }

    pub fn locator(&self) -> Locator {
        let Some(newest) = self.newest() else {
            return Vec::new();
        };
        let mut picks: Locator = OFFSETS
            .iter()
            .filter_map(|offset| newest.checked_sub(*offset))
            .filter_map(|at| self.samples.range(..=at).next_back())
            .map(|(height, hash)| (*height, *hash))
            .collect();
        picks.dedup();
        picks
    }
}

/// The middle answer, or the lower of the two middle ones.
pub fn median(answers: &[u32]) -> Option<u32> {
    let mut answers = answers.to_vec();
    answers.sort_unstable();
    answers.get(answers.len().saturating_sub(1) / 2).copied()
}

/// The median tip, if most of the configured servers answered near it. One stuck or false
/// server of three can't move it.
pub fn agreed_tip(answers: &[u32], servers: usize) -> Option<u32> {
    median(answers).filter(|tip| {
        let near = answers.iter().filter(|a| a.abs_diff(*tip) <= JUMP);
        near.count() > servers / 2
    })
}

/// The tip the samples follow.
#[derive(Default)]
pub struct Tips {
    last: Option<u32>,
}

/// The heights to sample at a tip.
#[derive(Debug, PartialEq, Eq)]
pub struct Wanted {
    pub heights: Vec<u32>,
    /// Drop every earlier sample, since they are from another picture of the chain.
    pub reset: bool,
}

impl Tips {
    pub fn see(&mut self, tip: u32) -> Wanted {
        let newest = tip.saturating_sub(TIP_MARGIN);
        if self.last.is_some_and(|last| last.abs_diff(tip) <= JUMP) {
            self.last = Some(tip);
            return Wanted {
                heights: vec![newest],
                reset: false,
            };
        }
        Wanted {
            heights: OFFSETS
                .iter()
                .filter_map(|offset| newest.checked_sub(*offset))
                .collect(),
            reset: true,
        }
    }

    /// The wanted samples arrived, so this tip is the one to follow.
    pub fn done(&mut self, tip: u32) {
        self.last = Some(tip);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Height {
    /// The peer's chain ends here.
    At(u32),
    /// A full reply hides the end, and the peer lacks the next newer block of the locator.
    AtMost(u32),
    /// The reply joins no block we asked with, or its headers don't link.
    Unplaced,
}

/// Reads a reply against the locator it answers.
pub fn reply_height(headers: &[Header], asked: &[(u32, Hash)]) -> Height {
    let Some(first) = headers.first() else {
        // The peer has the newest block we asked with, or none of them. It doesn't say.
        return Height::Unplaced;
    };
    let linked = headers.windows(2).all(|pair| {
        pair[0]
            .double_sha256()
            .is_ok_and(|hash| hash == pair[1].prev_block)
    });
    let base = asked.iter().position(|(_, hash)| *hash == first.prev_block);
    match base {
        Some(i) if linked && headers.len() < MAX_HEADERS => {
            Height::At(asked[i].0.saturating_add(headers.len() as u32))
        }
        Some(i) if linked && i > 0 => Height::AtMost(asked[i - 1].0 - 1),
        _ => Height::Unplaced,
    }
}

/// Real testnet headers are too few for a full reply, so these link by hash alone.
#[cfg(test)]
pub fn full_reply() -> Vec<Header> {
    use ziggurat_zcash::protocol::payload::block::Block;

    let mut headers = vec![Block::testnet_1().header];
    while headers.len() < MAX_HEADERS {
        let mut next = headers[0].clone();
        next.prev_block = headers.last().unwrap().double_sha256().unwrap();
        headers.push(next);
    }
    headers
}

#[cfg(test)]
mod tests {
    use ziggurat_zcash::protocol::payload::block::Block;

    use super::*;

    fn hash(n: u32) -> Hash {
        let mut bytes = [0; 32];
        bytes[..4].copy_from_slice(&n.to_le_bytes());
        Hash::new(bytes)
    }

    /// The first testnet blocks, whose headers link.
    fn headers() -> Vec<Header> {
        Block::initial_testnet_blocks()
            .into_iter()
            .map(|b| b.header)
            .collect()
    }

    #[test]
    fn most_servers_must_agree_on_a_tip() {
        // One stuck, one far too high, or one a little ahead moves nothing.
        assert_eq!(
            agreed_tip(&[3_599_000, 3_600_000, 3_600_001], 3),
            Some(3_600_000)
        );
        assert_eq!(
            agreed_tip(&[3_600_001, 4_000_000, 3_600_000], 3),
            Some(3_600_001)
        );
        assert_eq!(
            agreed_tip(&[3_600_090, 3_600_000, 3_600_001], 3),
            Some(3_600_001)
        );
        // Two answers of three that agree, and two that don't.
        assert_eq!(agreed_tip(&[3_600_001, 3_600_000], 3), Some(3_600_000));
        assert_eq!(agreed_tip(&[3_599_000, 3_600_000], 3), None);
        assert_eq!(agreed_tip(&[1, 3_599_000, 3_600_000], 3), None);
        assert_eq!(agreed_tip(&[3_600_000], 3), None);
        assert_eq!(
            agreed_tip(&[3_600_000 - JUMP, 3_600_000], 2),
            Some(3_599_900)
        );
        assert_eq!(agreed_tip(&[3_600_000], 1), Some(3_600_000));
        assert_eq!(agreed_tip(&[], 1), None);
    }

    #[test]
    fn one_stuck_server_of_three_never_wins() {
        let mut tips = Tips::default();
        let mut resets = 0;
        for tip in 3_600_000..3_603_000 {
            let answers = [tip, 3_599_500, tip + tip % 2];
            let wanted = tips.see(agreed_tip(&answers, 3).unwrap());
            resets += u32::from(wanted.reset);
            assert_eq!(wanted.heights[0], tip - TIP_MARGIN);
            tips.done(tip);
        }
        assert_eq!(resets, 1);
    }

    #[test]
    fn a_first_or_far_tip_samples_back() {
        let mut tips = Tips::default();
        let wanted = tips.see(200_004);
        assert!(wanted.reset);
        assert_eq!(wanted.heights.len(), 13);
        assert_eq!(wanted.heights[..3], [200_001, 199_999, 199_994]);
        assert_eq!(wanted.heights[12], 100_001);
        // The samples failed, so the next poll wants them again.
        assert!(tips.see(200_005).reset);
        tips.done(200_005);
        let next = Wanted {
            heights: vec![200_003],
            reset: false,
        };
        assert_eq!(tips.see(200_006), next);
        assert!(!tips.see(200_006 + JUMP).reset);
        assert!(!tips.see(200_006).reset);
        for far in [200_007 + JUMP, 300_000, 10] {
            assert!(tips.see(far).reset);
            tips.done(far);
        }
    }

    #[test]
    fn the_dense_edge_keeps_its_sample() {
        let mut chain = Chain::default();
        for height in [2_099, 2_100, 2_500] {
            chain.insert(height, hash(height));
        }
        assert!(chain.samples.contains_key(&2_100));
        chain.insert(2_501, hash(2_501));
        assert!(!chain.samples.contains_key(&2_100));
    }

    #[test]
    fn an_empty_chain_has_no_locator() {
        assert!(Chain::default().locator().is_empty());
    }

    #[test]
    fn a_locator_is_newest_first_without_repeats() {
        let mut chain = Chain::default();
        for height in [1_000, 100_000, 100_398, 100_400] {
            chain.insert(height, hash(height));
        }
        let heights: Vec<u32> = chain.locator().iter().map(|(h, _)| *h).collect();
        assert_eq!(heights, [100_400, 100_398, 100_000, 1_000]);
        assert_eq!(chain.locator()[0].1, hash(100_400));
    }

    #[test]
    fn samples_thin_out_with_age_and_still_reach_back() {
        let mut chain = Chain::default();
        for height in (0..299_000).step_by(10).chain(299_000..300_000) {
            chain.insert(height, hash(height));
        }
        assert!(chain.samples.len() < 800, "{}", chain.samples.len());
        for (pick, offset) in chain.locator().iter().zip(OFFSETS) {
            let wanted = 299_999 - offset;
            assert!(pick.0 <= wanted && wanted - pick.0 < SPARSE, "{pick:?}");
        }
        assert_eq!(chain.locator().len(), OFFSETS.len());
    }

    #[test]
    fn an_old_sample_that_arrives_late_is_kept() {
        let mut chain = Chain::default();
        chain.insert(200_000, hash(200_000));
        chain.insert(100_000, hash(100_000));
        assert_eq!(chain.locator().last().unwrap().0, 100_000);
    }

    #[test]
    fn a_reply_puts_the_peer_where_its_headers_end() {
        let headers = headers();
        let asked = vec![(99, hash(9)), (50, headers[0].double_sha256().unwrap())];
        assert_eq!(reply_height(&headers[1..4], &asked), Height::At(53));
    }

    #[test]
    fn a_reply_to_another_chain_or_with_a_gap_is_unplaced() {
        let headers = headers();
        let asked = vec![(50, headers[0].double_sha256().unwrap())];
        assert_eq!(reply_height(&headers[2..4], &asked), Height::Unplaced);
        assert_eq!(reply_height(&[], &asked), Height::Unplaced);
        let gap = [headers[1].clone(), headers[3].clone()];
        assert_eq!(reply_height(&gap, &asked), Height::Unplaced);
    }

    #[test]
    fn a_full_reply_only_caps_the_height() {
        let headers = full_reply();
        assert_eq!(headers.len(), 160);
        let base = headers[0].prev_block;
        let asked = vec![(900, hash(9)), (500, base), (100, hash(1))];
        assert_eq!(reply_height(&headers, &asked), Height::AtMost(899));
        assert_eq!(reply_height(&headers[..159], &asked), Height::At(659));
        let mut gap = headers.clone();
        gap[80].prev_block = hash(7);
        assert_eq!(reply_height(&gap, &asked), Height::Unplaced);
        // Past the newest block there's no cap to give.
        assert_eq!(reply_height(&headers, &asked[1..]), Height::Unplaced);
    }
}
