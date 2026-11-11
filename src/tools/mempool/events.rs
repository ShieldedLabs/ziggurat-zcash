//! Event records and the daily NDJSON files they go to.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

use chrono::{DateTime, NaiveDate, Utc};
use flate2::{write::GzEncoder, Compression};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use zebra_chain::transaction::{zip317, Transaction};
use ziggurat_zcash::protocol::payload::{inv::InvHash, Hash};

/// The ids of a tx, in the byte order explorers show.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TxIds {
    pub txid: String,
    /// The txid plus the auth digest for V5 and later. The txid alone before V5.
    pub wtxid: String,
}

impl TxIds {
    /// Returns `None` for an inv hash that names no tx.
    pub fn from_inv(inv: &InvHash) -> Option<Self> {
        match inv {
            InvHash::Tx(id) => {
                let txid = display_hex(id);
                Some(Self {
                    wtxid: txid.clone(),
                    txid,
                })
            }
            InvHash::MsgWtx(w) => {
                let txid = display_hex(&w.id);
                Some(Self {
                    wtxid: format!("{txid}{}", display_hex(&w.auth_digest)),
                    txid,
                })
            }
            _ => None,
        }
    }
}

fn display_hex(hash: &Hash) -> String {
    let mut bytes = *hash.as_bytes();
    bytes.reverse();
    hex::encode(bytes)
}

/// The fields of a `tx` event. The rules read the same fields back in a replay.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TxSummary {
    pub txid: String,
    pub wtxid: String,
    pub version: u32,
    pub size: usize,
    pub transparent_inputs: usize,
    pub transparent_outputs: usize,
    pub sprout_joinsplits: usize,
    pub sapling_spends: usize,
    pub sapling_outputs: usize,
    pub orchard_actions: usize,
    pub ironwood_actions: usize,
    /// ZIP 317 logical actions, at least 2.
    pub logical_actions: u32,
    /// Exact when the tx has no transparent inputs, else `None` (needs a UTXO lookup).
    pub fee: Option<i64>,
    pub sapling_value_balance: i64,
    pub orchard_value_balance: i64,
    pub ironwood_value_balance: i64,
    pub transparent_output_value: i64,
    pub expiry_height: Option<u32>,
}

impl TxSummary {
    pub fn new(ids: TxIds, tx: &Transaction, size: usize) -> Self {
        let fee = if tx.has_transparent_inputs() {
            None
        } else {
            tx.value_balance(&Default::default())
                .ok()
                .and_then(|balance| balance.remaining_transaction_value().ok())
                .map(i64::from)
        };

        Self {
            txid: ids.txid,
            wtxid: ids.wtxid,
            version: tx.version(),
            size,
            transparent_inputs: tx.inputs().len(),
            transparent_outputs: tx.outputs().len(),
            sprout_joinsplits: tx.joinsplit_count(),
            sapling_spends: tx.sapling_spends_count(),
            sapling_outputs: tx.sapling_outputs().count(),
            orchard_actions: tx.orchard_actions().count(),
            ironwood_actions: tx.ironwood_actions().count(),
            logical_actions: zip317::conventional_actions(tx),
            fee,
            sapling_value_balance: tx.sapling_value_balance().sapling_amount().into(),
            orchard_value_balance: tx.orchard_value_balance().orchard_amount().into(),
            ironwood_value_balance: tx.ironwood_value_balance().ironwood_amount().into(),
            transparent_output_value: tx
                .outputs()
                .iter()
                .map(|output| i64::from(output.value()))
                .sum(),
            expiry_height: tx.expiry_height().map(|height| height.0),
        }
    }
}

/// Appends events to `mempool-YYYYMMDD.ndjson` and raw txs to `raw-YYYYMMDD.ndjson.gz`.
///
/// Each raw line is its own gzip member, so a crash never leaves a broken file.
pub struct EventLog {
    dir: PathBuf,
    day: Option<NaiveDate>,
    events: Option<File>,
    raw: Option<File>,
}

impl EventLog {
    pub fn open(dir: &Path) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        Ok(Self {
            dir: dir.to_owned(),
            day: None,
            events: None,
            raw: None,
        })
    }

    /// Writes one event line. `fields` must be a JSON object.
    pub fn write(&mut self, t: DateTime<Utc>, event: &str, fields: Value) -> io::Result<()> {
        let line = event_line(t, event, fields);
        self.rotate(t)?;
        let file = self.events.as_mut().expect("rotate opens the file");
        file.write_all(line.as_bytes())
    }

    /// Writes one raw tx line.
    pub fn write_raw(&mut self, t: DateTime<Utc>, wtxid: &str, raw: &[u8]) -> io::Result<()> {
        let line = event_line(t, "raw", json!({ "wtxid": wtxid, "hex": hex::encode(raw) }));
        let mut gz = GzEncoder::new(Vec::new(), Compression::default());
        gz.write_all(line.as_bytes())?;
        let member = gz.finish()?;
        self.rotate(t)?;
        self.raw
            .as_mut()
            .expect("rotate opens the file")
            .write_all(&member)
    }

    fn rotate(&mut self, t: DateTime<Utc>) -> io::Result<()> {
        let day = t.date_naive();
        if self.day == Some(day) {
            return Ok(());
        }
        let stamp = day.format("%Y%m%d");
        let open = |name: String| {
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.dir.join(name))
        };
        self.events = Some(open(format!("mempool-{stamp}.ndjson"))?);
        self.raw = Some(open(format!("raw-{stamp}.ndjson.gz"))?);
        self.day = Some(day);
        Ok(())
    }
}

/// `{"t":<unix secs>,"event":<name>,...fields}` plus a newline.
pub fn event_line(t: DateTime<Utc>, event: &str, fields: Value) -> String {
    let mut map = Map::new();
    map.insert("t".into(), t.timestamp().into());
    map.insert("event".into(), event.into());
    if let Value::Object(fields) = fields {
        map.extend(fields);
    }
    let mut line = Value::Object(map).to_string();
    line.push('\n');
    line
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader};

    use flate2::read::MultiGzDecoder;
    use ziggurat_zcash::protocol::payload::inv::WtxId;

    use super::*;

    #[test]
    fn ids_from_inv() {
        let mut id = [0u8; 32];
        id[0] = 0xab;
        let mut auth = [0u8; 32];
        auth[31] = 0xcd;
        let ids = TxIds::from_inv(&InvHash::MsgWtx(WtxId {
            id: Hash::new(id),
            auth_digest: Hash::new(auth),
        }))
        .unwrap();
        assert!(ids.txid.ends_with("ab"));
        assert_eq!(ids.wtxid.len(), 128);
        assert!(ids.wtxid.starts_with(&ids.txid));
        assert!(ids.wtxid[64..].starts_with("cd"));
        assert!(TxIds::from_inv(&InvHash::Block(Hash::zeroed())).is_none());
    }

    #[test]
    fn line_format() {
        let t = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let line = event_line(t, "seen", json!({ "peer": "1.2.3.4:8233" }));
        assert_eq!(
            line,
            "{\"t\":1700000000,\"event\":\"seen\",\"peer\":\"1.2.3.4:8233\"}\n"
        );
    }

    #[test]
    fn raw_file_survives_reopen() {
        let dir = std::env::temp_dir().join(format!("mempool-events-{}", std::process::id()));
        let t = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        for i in 0..2u8 {
            let mut log = EventLog::open(&dir).unwrap();
            log.write_raw(t, "aa", &[i]).unwrap();
            log.write(t, "alive", json!({})).unwrap();
        }
        let raw = File::open(dir.join("raw-20231114.ndjson.gz")).unwrap();
        let lines: Vec<String> = BufReader::new(MultiGzDecoder::new(raw))
            .lines()
            .map(Result::unwrap)
            .collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[1].contains("\"hex\":\"01\""));
        let events = fs::read_to_string(dir.join("mempool-20231114.ndjson")).unwrap();
        assert_eq!(events.lines().count(), 2);
        fs::remove_dir_all(dir).unwrap();
    }
}
