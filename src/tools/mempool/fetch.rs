//! Per-peer getdata batches. A long `mempool` reply becomes a few getdata messages in
//! flight at a time, so pea2pea's send queue never fills.

use std::collections::{HashSet, VecDeque};

/// Zebra stops a getdata reply after 1 MB of txs, and skips the rest without a
/// `notfound`. 16 txs of up to 62 KB each fit.
pub const BATCH: usize = 16;
/// The txs asked of one peer and not answered yet. 4 batches stay far below pea2pea's
/// queue of 64 messages.
pub const WINDOW: usize = 4 * BATCH;

/// The txs one peer will be asked for, by wtxid.
#[derive(Default)]
pub struct Fetches {
    queue: VecDeque<String>,
    in_flight: HashSet<String>,
}

impl Fetches {
    pub fn push(&mut self, wtxid: String) {
        self.queue.push_back(wtxid);
    }

    /// Takes the next batch, if the window has room for a full one or for the rest of
    /// the queue. `wanted` drops the txs that need no fetch now.
    pub fn next_batch(&mut self, mut wanted: impl FnMut(&str) -> bool) -> Option<Vec<String>> {
        let room = WINDOW - self.in_flight.len();
        if room < BATCH.min(self.queue.len()) {
            return None;
        }
        let mut batch = Vec::new();
        while batch.len() < BATCH.min(room) {
            let Some(wtxid) = self.queue.pop_front() else {
                break;
            };
            if wanted(&wtxid) {
                self.in_flight.insert(wtxid.clone());
                batch.push(wtxid);
            }
        }
        (!batch.is_empty()).then_some(batch)
    }

    /// Frees the tx's place in the window.
    pub fn settle(&mut self, wtxid: &str) {
        self.in_flight.remove(wtxid);
    }

    /// The queued and unanswered txs, for another peer to fetch.
    pub fn into_wtxids(self) -> impl Iterator<Item = String> {
        self.in_flight.into_iter().chain(self.queue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_backlog_goes_out_in_batches_within_the_window() {
        let mut fetches = Fetches::default();
        for i in 0..2_000 {
            fetches.push(i.to_string());
        }
        let all = |_: &str| true;
        let mut sent = Vec::new();
        while let Some(batch) = fetches.next_batch(all) {
            assert_eq!(batch.len(), BATCH);
            sent.extend(batch);
        }
        assert_eq!(sent.len(), WINDOW);

        // One answer leaves too little room for a full batch.
        fetches.settle(&sent[0]);
        assert!(fetches.next_batch(all).is_none());
        for wtxid in &sent[1..BATCH] {
            fetches.settle(wtxid);
        }
        let batch = fetches.next_batch(all).unwrap();
        assert_eq!(batch[0], WINDOW.to_string());
    }

    #[test]
    fn unwanted_txs_skip_and_the_tail_goes_short() {
        let mut fetches = Fetches::default();
        for i in 0..20 {
            fetches.push(i.to_string());
        }
        // Odd txs arrived from another peer.
        let even = |w: &str| w.parse::<u32>().unwrap() % 2 == 0;
        assert_eq!(fetches.next_batch(even).unwrap().len(), 10);
        assert!(fetches.next_batch(even).is_none());

        let mut left: Vec<String> = fetches.into_wtxids().collect();
        left.sort_unstable();
        assert_eq!(
            left,
            ["0", "10", "12", "14", "16", "18", "2", "4", "6", "8"]
        );
    }
}
