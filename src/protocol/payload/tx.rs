//! Transaction-related types.

use std::{
    fmt,
    io::{self, Cursor, Read},
    sync::Arc,
};

use bytes::{Buf, BufMut};
use sha2::Digest;
use zebra_chain::{
    serialization::ZcashDeserialize,
    transaction::{Transaction, UnminedTxId},
};

use crate::protocol::payload::{
    codec::Codec,
    inv::{InvHash, WtxId},
    Hash,
};

/// A Zcash transaction, kept as raw bytes and parsed by `zebra-chain`.
///
/// A `tx` message that `zebra-chain` can't parse still decodes: [`Tx::transaction`] is `None`
/// and [`Tx::decode_error`] says why. The raw bytes are always kept.
#[derive(Clone)]
pub struct Tx {
    raw: Vec<u8>,
    parsed: Result<Arc<Transaction>, String>,
}

impl Tx {
    /// Parses raw transaction bytes. All bytes must belong to one transaction.
    pub fn from_raw(raw: Vec<u8>) -> Self {
        let mut cursor = Cursor::new(&raw[..]);
        let parsed = match Transaction::zcash_deserialize(&mut cursor) {
            Ok(_) if cursor.position() as usize != raw.len() => Err(format!(
                "{} trailing bytes",
                raw.len() - cursor.position() as usize
            )),
            Ok(tx) => Ok(Arc::new(tx)),
            Err(e) => Err(e.to_string()),
        };

        Self { raw, parsed }
    }

    /// Decodes a `tx` message payload. Takes all remaining bytes and never fails.
    pub fn decode_payload<B: Buf>(bytes: &mut B) -> Self {
        Self::from_raw(bytes.copy_to_bytes(bytes.remaining()).to_vec())
    }

    /// The serialized transaction.
    pub fn raw(&self) -> &[u8] {
        &self.raw
    }

    /// The parsed transaction, if `zebra-chain` could parse it.
    pub fn transaction(&self) -> Option<&Arc<Transaction>> {
        self.parsed.as_ref().ok()
    }

    /// Why `zebra-chain` could not parse the transaction.
    pub fn decode_error(&self) -> Option<&str> {
        self.parsed.as_ref().err().map(String::as_str)
    }

    /// The [`InvHash`] for this `Tx`: `MsgWtx` for V5 and later, `Tx` for older versions.
    ///
    /// An unparsed transaction gets the double SHA-256 of its bytes (the V1 to V4 txid).
    pub fn inv_hash(&self) -> InvHash {
        match self.transaction().map(|tx| tx.unmined_id()) {
            Some(UnminedTxId::Witnessed(wtx_id)) => InvHash::MsgWtx(WtxId {
                id: Hash::new(wtx_id.id.0),
                auth_digest: Hash::new(wtx_id.auth_digest.0),
            }),
            Some(UnminedTxId::Legacy(id)) => InvHash::Tx(Hash::new(id.0)),
            None => {
                let digest = sha2::Sha256::digest(sha2::Sha256::digest(&self.raw));
                InvHash::Tx(Hash::new(digest.into()))
            }
        }
    }
}

impl PartialEq for Tx {
    fn eq(&self, other: &Self) -> bool {
        self.raw == other.raw
    }
}

impl fmt::Debug for Tx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut s = f.debug_struct("Tx");
        s.field("len", &self.raw.len());
        match &self.parsed {
            Ok(tx) => s.field("version", &tx.version()),
            Err(e) => s.field("decode_error", e),
        };
        s.finish()
    }
}

impl Codec for Tx {
    fn encode<B: BufMut>(&self, buffer: &mut B) -> io::Result<()> {
        buffer.put_slice(&self.raw);
        Ok(())
    }

    /// Decodes one transaction from a stream, such as a block's transaction list.
    ///
    /// The transaction's length is only known after parsing, so a parse failure is an error.
    /// Use [`Tx::decode_payload`] for a `tx` message.
    fn decode<B: Buf>(bytes: &mut B) -> io::Result<Self> {
        let mut reader = RecordingReader {
            buf: bytes,
            raw: Vec::new(),
        };
        let tx = Transaction::zcash_deserialize(&mut reader)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        Ok(Self {
            raw: reader.raw,
            parsed: Ok(Arc::new(tx)),
        })
    }
}

// Reads from a `Buf` and keeps a copy of every byte read.
struct RecordingReader<'a, B> {
    buf: &'a mut B,
    raw: Vec<u8>,
}

impl<B: Buf> Read for RecordingReader<'_, B> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let n = out.len().min(self.buf.remaining());
        self.buf.copy_to_slice(&mut out[..n]);
        self.raw.extend_from_slice(&out[..n]);
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{protocol::payload::block::Block, vectors::BLOCK_MAINNET_3_562_040_BYTES};

    const IRONWOOD_TXID: &str = "97afb2f7c1b37afe4a530824b29c748d09605b7185b1158cdb895bcd18295ce0";

    #[test]
    fn mainnet_v6_block_round_trip() {
        let bytes = &BLOCK_MAINNET_3_562_040_BYTES[..];
        let block = Block::decode(&mut Cursor::new(bytes)).unwrap();
        assert_eq!(block.txs.len(), 5);

        let mut encoded = Vec::new();
        block.encode(&mut encoded).unwrap();
        assert_eq!(encoded, bytes);
    }

    #[test]
    fn mainnet_v6_ironwood_tx() {
        let block = Block::decode(&mut Cursor::new(&BLOCK_MAINNET_3_562_040_BYTES[..])).unwrap();
        let tx = block
            .txs
            .iter()
            .find(|tx| tx.transaction().unwrap().hash().to_string() == IRONWOOD_TXID)
            .unwrap();

        // A tx message with the same bytes parses the same way.
        let payload = Tx::decode_payload(&mut Cursor::new(tx.raw()));
        assert_eq!(&payload, tx);

        let parsed = payload.transaction().unwrap();
        assert_eq!(parsed.version(), 6);
        assert_eq!(payload.raw().len(), 18_668);
        assert_eq!(parsed.ironwood_actions().count(), 5);
        assert!(!parsed.has_transparent_inputs());
        let fee = parsed
            .value_balance(&Default::default())
            .unwrap()
            .remaining_transaction_value()
            .unwrap();
        assert_eq!(i64::from(fee), 30_000);
        assert!(
            matches!(payload.inv_hash(), InvHash::MsgWtx(w) if w.id == Hash::new(parsed.hash().0))
        );
    }

    #[test]
    fn undecodable_tx_payload_is_kept() {
        let raw = vec![6, 0, 0, 0x80, 1, 2, 3];
        let tx = Tx::decode_payload(&mut Cursor::new(&raw[..]));
        assert!(tx.transaction().is_none());
        assert!(tx.decode_error().is_some());
        assert_eq!(tx.raw(), &raw[..]);
        assert!(matches!(tx.inv_hash(), InvHash::Tx(_)));
    }

    #[test]
    fn testnet_legacy_txs_parse() {
        for block in Block::initial_testnet_blocks() {
            for tx in &block.txs {
                assert!(matches!(tx.inv_hash(), InvHash::Tx(_)));
            }
        }
    }
}
