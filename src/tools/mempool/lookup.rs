//! Fees for txs with transparent inputs. The listener asks lightwalletd for each spent
//! output's parent tx, parses it with zebra-chain, and caches its output values on disk.

use std::{
    collections::{HashMap, HashSet},
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, Write},
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore},
    time::{Interval, MissedTickBehavior},
};
use tonic::{
    codegen::http::uri::PathAndQuery,
    transport::{Channel, ClientTlsConfig, Endpoint},
};
use zebra_chain::{
    amount::{Amount, NonNegative},
    serialization::ZcashDeserialize,
    transaction::{self, Transaction},
    transparent,
};

use crate::config::LookupConfig;

const GET_TRANSACTION: &str = "/cash.z.wallet.sdk.rpc.CompactTxStreamer/GetTransaction";
const CALL_TIMEOUT: Duration = Duration::from_secs(10);

// The lightwalletd messages this module uses, from `service.proto`.
#[derive(Clone, PartialEq, prost::Message)]
struct TxFilter {
    /// The txid in internal byte order.
    #[prost(bytes = "vec", tag = "3")]
    hash: Vec<u8>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct RawTransaction {
    #[prost(bytes = "vec", tag = "1")]
    data: Vec<u8>,
    #[prost(uint64, tag = "2")]
    height: u64,
}

/// Where a `tx` event's fee comes from.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FeeSource {
    /// No transparent inputs, so the tx itself gives the fee.
    Shielded,
    /// The spent outputs' values came from lightwalletd or the cache.
    Lookup,
    /// Transparent inputs whose values are unknown. Old logs have no `fee_source`.
    #[default]
    Unknown,
}

#[derive(Clone)]
pub struct Lookup {
    servers: Arc<[Channel]>,
    next: Arc<AtomicUsize>,
    limiter: Arc<tokio::sync::Mutex<Interval>>,
    cache: Arc<Mutex<Cache>>,
    slots: Arc<Semaphore>,
    timeout: Duration,
}

impl Lookup {
    /// Returns `None` when the config lists no servers.
    pub fn new(config: &LookupConfig, cache_path: &Path) -> Result<Option<Self>, String> {
        if config.servers.is_empty() {
            return Ok(None);
        }
        let tls = ClientTlsConfig::new().with_webpki_roots();
        let servers = config
            .servers
            .iter()
            .map(|url| {
                let endpoint = Endpoint::from_shared(url.clone())
                    .map_err(|e| format!("{url}: {e}"))?
                    .connect_timeout(CALL_TIMEOUT)
                    .timeout(CALL_TIMEOUT);
                let endpoint = if url.starts_with("https") {
                    endpoint
                        .tls_config(tls.clone())
                        .map_err(|e| format!("{url}: {e}"))?
                } else {
                    endpoint
                };
                Ok(endpoint.connect_lazy())
            })
            .collect::<Result<Vec<_>, String>>()?;
        let mut interval = tokio::time::interval(Duration::from_secs_f64(1.0 / config.per_sec));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let cache =
            Cache::open(cache_path).map_err(|e| format!("{}: {e}", cache_path.display()))?;

        Ok(Some(Self {
            servers: servers.into(),
            next: Arc::new(AtomicUsize::new(0)),
            limiter: Arc::new(tokio::sync::Mutex::new(interval)),
            cache: Arc::new(Mutex::new(cache)),
            slots: Arc::new(Semaphore::new(config.max_pending)),
            timeout: Duration::from_secs(config.timeout_secs),
        }))
    }

    /// The fee from cached parents alone, if all of them are cached.
    pub fn cached_fee(&self, tx: &Transaction) -> Option<Result<i64, String>> {
        let cache = self.cache.lock();
        tx.spent_outpoints()
            .all(|o| cache.values.contains_key(&o.hash))
            .then(|| fee(tx, &cache.values))
    }

    /// A slot for one tx's lookups, or `None` when `max_pending` txs already wait.
    pub fn slot(&self) -> Option<OwnedSemaphorePermit> {
        self.slots.clone().try_acquire_owned().ok()
    }

    /// Fetches the parents not in the cache, then computes the fee. Gives up after the
    /// configured timeout.
    pub async fn fee(&self, tx: &Transaction) -> Result<i64, String> {
        tokio::time::timeout(self.timeout, self.fetch_parents(tx))
            .await
            .map_err(|_| "lookup timed out".to_owned())??;
        self.cached_fee(tx).expect("all parents are cached")
    }

    async fn fetch_parents(&self, tx: &Transaction) -> Result<(), String> {
        let parents: HashSet<transaction::Hash> = tx.spent_outpoints().map(|o| o.hash).collect();
        for parent in parents {
            if self.cache.lock().values.contains_key(&parent) {
                continue;
            }
            let values = self.fetch(parent).await?;
            self.cache
                .lock()
                .insert(parent, values)
                .map_err(|e| format!("can't write the cache: {e}"))?;
        }
        Ok(())
    }

    /// Asks each server in turn until one returns the tx.
    async fn fetch(&self, txid: transaction::Hash) -> Result<Vec<i64>, String> {
        let mut errors = Vec::new();
        for _ in 0..self.servers.len() {
            self.limiter.lock().await.tick().await;
            let i = self.next.fetch_add(1, Ordering::Relaxed) % self.servers.len();
            let result = get_transaction(self.servers[i].clone(), txid).await;
            metrics::increment_counter!(
                "mempool_fee_lookups_total",
                "result" => if result.is_ok() { "ok" } else { "error" }
            );
            match result {
                Ok(values) => return Ok(values),
                Err(e) => errors.push(e),
            }
        }
        Err(format!("{txid}: {}", errors.join("; ")))
    }
}

async fn get_transaction(channel: Channel, txid: transaction::Hash) -> Result<Vec<i64>, String> {
    let mut grpc = tonic::client::Grpc::new(channel);
    grpc.ready().await.map_err(|e| e.to_string())?;
    let request = TxFilter {
        hash: txid.0.to_vec(),
    };
    let response: tonic::Response<RawTransaction> = grpc
        .unary(
            tonic::Request::new(request),
            PathAndQuery::from_static(GET_TRANSACTION),
            tonic_prost::ProstCodec::default(),
        )
        .await
        .map_err(|e| e.message().to_owned())?;
    let data = response.into_inner().data;
    let tx = Transaction::zcash_deserialize(&data[..]).map_err(|e| e.to_string())?;
    // The servers are public, so check that the bytes are the tx we asked for.
    if tx.hash() != txid {
        return Err(format!("server sent {} instead", tx.hash()));
    }
    Ok(tx.outputs().iter().map(|o| i64::from(o.value())).collect())
}

/// The fee from the spent outputs' values. Every parent must be in `values`.
pub fn fee(tx: &Transaction, values: &HashMap<transaction::Hash, Vec<i64>>) -> Result<i64, String> {
    let mut outputs = HashMap::new();
    for outpoint in tx.spent_outpoints() {
        let value = values
            .get(&outpoint.hash)
            .and_then(|v| v.get(outpoint.index as usize))
            .ok_or_else(|| format!("{} has no output {}", outpoint.hash, outpoint.index))?;
        let amount = Amount::<NonNegative>::try_from(*value).map_err(|e| e.to_string())?;
        outputs.insert(
            outpoint,
            transparent::Output::new(amount, transparent::Script::new(&[])),
        );
    }
    let balance = (tx
        .transparent_value_balance_from_outputs(&outputs)
        .map_err(|e| e.to_string())?
        + tx.sprout_value_balance().map_err(|e| e.to_string())?
        + tx.sapling_value_balance()
        + tx.orchard_value_balance()
        + tx.ironwood_value_balance())
    .map_err(|e| e.to_string())?;
    balance
        .remaining_transaction_value()
        .map(i64::from)
        .map_err(|e| e.to_string())
}

/// Parent txid to output values, appended to an NDJSON file as each parent arrives.
struct Cache {
    values: HashMap<transaction::Hash, Vec<i64>>,
    file: File,
}

#[derive(Serialize, Deserialize)]
struct CacheLine {
    txid: String,
    values: Vec<i64>,
}

impl Cache {
    fn open(path: &Path) -> io::Result<Self> {
        let mut values = HashMap::new();
        if let Ok(file) = File::open(path) {
            for line in BufReader::new(file).lines() {
                // A crash can cut the last line. Skip what doesn't parse.
                let Ok(line) = serde_json::from_str::<CacheLine>(&line?) else {
                    continue;
                };
                if let Ok(txid) = line.txid.parse() {
                    values.insert(txid, line.values);
                }
            }
        }
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self { values, file })
    }

    fn insert(&mut self, txid: transaction::Hash, values: Vec<i64>) -> io::Result<()> {
        let line = CacheLine {
            txid: txid.to_string(),
            values,
        };
        let mut text = serde_json::to_string(&line).expect("a cache line serializes");
        text.push('\n');
        self.file.write_all(text.as_bytes())?;
        self.values.insert(txid, line.values);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx(hex_tx: &str) -> Transaction {
        Transaction::zcash_deserialize(&hex::decode(hex_tx).unwrap()[..]).unwrap()
    }

    // A mainnet V5 tx from the 2026-11-11 dust sweep: 50 transparent inputs, 1 output.
    const SWEEP_TX: &str = include_str!("testdata/sweep-tx.hex");

    #[test]
    fn fee_from_parent_values() {
        let tx = tx(SWEEP_TX.trim());
        let mut values = HashMap::new();
        for outpoint in tx.spent_outpoints() {
            let v: &mut Vec<i64> = values.entry(outpoint.hash).or_default();
            if v.len() <= outpoint.index as usize {
                v.resize(outpoint.index as usize + 1, 0);
            }
            v[outpoint.index as usize] = 200;
        }
        let out: i64 = tx.outputs().iter().map(|o| i64::from(o.value())).sum();
        let inputs = tx.inputs().len() as i64;
        assert_eq!(fee(&tx, &values), Ok(200 * inputs - out));

        let first = tx.spent_outpoints().next().unwrap();
        values.remove(&first.hash);
        assert!(fee(&tx, &values).is_err());
    }

    #[test]
    fn cache_survives_reopen_and_a_cut_line() {
        let dir = std::env::temp_dir().join(format!("mempool-lookup-{}", std::process::id()));
        let path = dir.join("prevouts.ndjson");
        let txid = transaction::Hash([7; 32]);
        Cache::open(&path)
            .unwrap()
            .insert(txid, vec![1, 2])
            .unwrap();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"{\"txid\":\"ab")
            .unwrap();
        let cache = Cache::open(&path).unwrap();
        assert_eq!(cache.values[&txid], [1, 2]);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
