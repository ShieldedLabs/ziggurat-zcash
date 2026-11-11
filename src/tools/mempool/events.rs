//! Event records and the daily NDJSON files they go to.

use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use chrono::{DateTime, NaiveDate, Utc};
use flate2::{read::MultiGzDecoder, write::GzEncoder, Compression};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tracing::warn;
use zebra_chain::transaction::{zip317, Transaction};
use ziggurat_zcash::protocol::payload::{inv::InvHash, Hash};

use crate::lookup::FeeSource;

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
    /// `None` when `fee_source` is unknown.
    pub fee: Option<i64>,
    #[serde(default)]
    pub fee_source: FeeSource,
    pub sapling_value_balance: i64,
    pub orchard_value_balance: i64,
    pub ironwood_value_balance: i64,
    pub transparent_output_value: i64,
    pub expiry_height: Option<u32>,
}

impl TxSummary {
    /// The pools a tx touches, such as `transparent+ironwood`. A small, fixed label set.
    pub fn pools(&self) -> String {
        let pools: Vec<&str> = [
            (
                self.transparent_inputs + self.transparent_outputs,
                "transparent",
            ),
            (self.sprout_joinsplits, "sprout"),
            (self.sapling_spends + self.sapling_outputs, "sapling"),
            (self.orchard_actions, "orchard"),
            (self.ironwood_actions, "ironwood"),
        ]
        .into_iter()
        .filter(|(n, _)| *n > 0)
        .map(|(_, name)| name)
        .collect();
        if pools.is_empty() {
            "none".into()
        } else {
            pools.join("+")
        }
    }

    /// The fee is known here only without transparent inputs. The lookup fills in the rest.
    pub fn new(ids: TxIds, tx: &Transaction, size: usize) -> Self {
        let (fee, fee_source) = if tx.has_transparent_inputs() {
            (None, FeeSource::Unknown)
        } else {
            let fee = tx
                .value_balance(&Default::default())
                .ok()
                .and_then(|balance| balance.remaining_transaction_value().ok())
                .map(i64::from);
            (fee, FeeSource::Shielded)
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
            fee_source,
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

/// The wtxids seen so far today (UTC). The listener loads them from the day's event file at
/// start, so a restart counts each tx once a day.
pub struct SeenToday {
    day: NaiveDate,
    wtxids: HashSet<String>,
}

impl SeenToday {
    /// Reads the `seen` events of today's `mempool-YYYYMMDD.ndjson`, and of its `.gz` if
    /// the file was compressed. Lines that don't parse, such as a cut last line, are skipped.
    pub fn load(dir: &Path, now: DateTime<Utc>) -> io::Result<Self> {
        let day = now.date_naive();
        let name = format!("mempool-{}.ndjson", day.format("%Y%m%d"));
        let mut wtxids = HashSet::new();
        for path in [dir.join(&name), dir.join(format!("{name}.gz"))] {
            let file = match File::open(&path) {
                Ok(file) => file,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            let reader: Box<dyn BufRead> = if path.extension().is_some_and(|e| e == "gz") {
                Box::new(BufReader::new(MultiGzDecoder::new(file)))
            } else {
                Box::new(BufReader::new(file))
            };
            for line in reader.lines() {
                let Ok(Value::Object(event)) = serde_json::from_str(&line?) else {
                    continue;
                };
                if event.get("event").and_then(Value::as_str) == Some("seen") {
                    if let Some(wtxid) = event.get("wtxid").and_then(Value::as_str) {
                        wtxids.insert(wtxid.to_owned());
                    }
                }
            }
        }
        Ok(Self { day, wtxids })
    }

    /// True the first time a wtxid shows up today.
    pub fn first(&mut self, wtxid: &str, now: DateTime<Utc>) -> bool {
        let day = now.date_naive();
        if day != self.day {
            self.day = day;
            self.wtxids.clear();
        }
        self.wtxids.insert(wtxid.to_owned())
    }

    pub fn len(&self) -> usize {
        self.wtxids.len()
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
                .read(true)
                .append(true)
                .open(self.dir.join(name))
        };
        let mut events = open(format!("mempool-{stamp}.ndjson"))?;
        end_line(&mut events)?;
        self.events = Some(events);
        self.raw = Some(open(format!("raw-{stamp}.ndjson.gz"))?);
        self.day = Some(day);

        let dir = self.dir.clone();
        std::thread::spawn(move || {
            if let Err(e) = compress_past_events(&dir, day) {
                warn!("can't compress past event files: {e}");
            }
        });
        Ok(())
    }
}

/// Ends a line that a crash cut, so the next event starts on its own line.
fn end_line(file: &mut File) -> io::Result<()> {
    if file.metadata()?.len() == 0 {
        return Ok(());
    }
    let mut last = [0u8];
    file.seek(SeekFrom::End(-1))?;
    file.read_exact(&mut last)?;
    if last[0] != b'\n' {
        file.write_all(b"\n")?;
    }
    Ok(())
}

/// Gzips every `mempool-YYYYMMDD.ndjson` before `today` into `.ndjson.gz` and removes it.
pub fn compress_past_events(dir: &Path, today: NaiveDate) -> io::Result<()> {
    let current = format!("mempool-{}.ndjson", today.format("%Y%m%d"));
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !name.starts_with("mempool-") || !name.ends_with(".ndjson") || name == current {
            continue;
        }
        let gz_path = dir.join(format!("{name}.gz"));
        let gz_file = OpenOptions::new().create(true).append(true).open(gz_path)?;
        let mut gz = GzEncoder::new(gz_file, Compression::default());
        io::copy(&mut File::open(&path)?, &mut gz)?;
        gz.finish()?;
        fs::remove_file(&path)?;
    }
    Ok(())
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
    fn past_event_files_get_gzipped() {
        let dir = std::env::temp_dir().join(format!("mempool-gzip-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("mempool-20261110.ndjson"),
            "{\"event\":\"alive\"}\n",
        )
        .unwrap();
        fs::write(dir.join("mempool-20261111.ndjson"), "today\n").unwrap();
        let today = NaiveDate::from_ymd_opt(2026, 11, 11).unwrap();
        compress_past_events(&dir, today).unwrap();

        assert!(!dir.join("mempool-20261110.ndjson").exists());
        assert!(dir.join("mempool-20261111.ndjson").exists());
        let gz = File::open(dir.join("mempool-20261110.ndjson.gz")).unwrap();
        let lines: Vec<String> = BufReader::new(MultiGzDecoder::new(gz))
            .lines()
            .map(Result::unwrap)
            .collect();
        assert_eq!(lines, ["{\"event\":\"alive\"}"]);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_restart_counts_no_tx_twice_a_day() {
        let dir = std::env::temp_dir().join(format!("mempool-seen-{}", std::process::id()));
        let t = DateTime::from_timestamp(1_794_418_000, 0).unwrap();
        {
            let mut log = EventLog::open(&dir).unwrap();
            for wtxid in ["aa", "bb"] {
                log.write(t, "seen", json!({ "wtxid": wtxid, "src": "inv" }))
                    .unwrap();
            }
            log.write(t, "tx", json!({ "wtxid": "cc" })).unwrap();
        }
        let path = dir.join("mempool-20261111.ndjson");
        // A crash cut the last line.
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"{\"t\":1794418000,\"event\":\"seen\",\"wtx")
            .unwrap();

        let mut seen = SeenToday::load(&dir, t).unwrap();
        assert_eq!(seen.len(), 2);
        assert!(!seen.first("aa", t));
        assert!(seen.first("cc", t));
        assert!(!seen.first("cc", t));
        // A new day starts empty.
        assert!(seen.first("aa", t + chrono::Duration::days(1)));

        // The next event after the cut line starts on its own line.
        EventLog::open(&dir)
            .unwrap()
            .write(t, "seen", json!({ "wtxid": "dd" }))
            .unwrap();
        assert!(!SeenToday::load(&dir, t).unwrap().first("dd", t));

        // After the day rolls over, the .gz still counts.
        compress_past_events(&dir, t.date_naive().succ_opt().unwrap()).unwrap();
        assert_eq!(SeenToday::load(&dir, t).unwrap().len(), 3);
        fs::remove_dir_all(dir).unwrap();
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
