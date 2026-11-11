//! The wire log: every frame in and out, taken in the codec before decode, plus connect,
//! handshake and disconnect events. A writer thread appends NDJSON to hourly zstd files.
//!
//! The current hour goes to `wire-YYYYMMDD-HH.ndjson.zst.part`. At the hour's end the
//! writer closes the zstd frame and moves it to `wire-YYYYMMDD-HH.ndjson.zst`. At start,
//! the writer recovers the complete lines of any `.part` that a crash left.

use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

use bytes::BytesMut;
use chrono::{DateTime, Timelike, Utc};
use parking_lot::Mutex;
use serde::Serialize;
use tokio_util::codec::{Decoder, Encoder};
use tracing::{error, warn};
use ziggurat_zcash::{
    protocol::message::{
        constants::{HEADER_LEN, MAX_MESSAGE_LEN},
        Message,
    },
    tools::synthetic_node::MessageCodec,
};

/// The writer flushes a zstd block this often, so a crash loses at most this much.
const FLUSH: Duration = Duration::from_secs(5);
/// 16 MiB, so zstd finds the snapshots that repeat each minute. DuckDB decodes up to 128 MiB.
const WINDOW_LOG: u32 = 24;

/// One line of the wire log.
#[derive(Debug, Serialize)]
pub struct Record {
    /// UTC with microseconds.
    #[serde(serialize_with = "micros")]
    pub t: DateTime<Utc>,
    pub peer: SocketAddr,
    /// `frame`, `connect`, `connect_failed`, `handshake` or `disconnect`.
    pub kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dir: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
    /// The payload length from the header.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub len: Option<u32>,
    /// `ok`, or why the frame or its tx did not decode.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decode: Option<String>,
    /// The frame's bytes as hex, header included.
    #[serde(skip_serializing_if = "Option::is_none", serialize_with = "hex_bytes")]
    pub raw: Option<Vec<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_agent: Option<String>,
}

impl Record {
    pub fn event(peer: SocketAddr, kind: &'static str) -> Self {
        Self {
            t: Utc::now(),
            peer,
            kind,
            dir: None,
            command: None,
            len: None,
            decode: None,
            raw: None,
            reason: None,
            user_agent: None,
        }
    }

    fn frame(peer: SocketAddr, dir: &'static str, raw: &[u8], decode: String) -> Self {
        let (command, len) = header(raw);
        Self {
            dir: Some(dir),
            command,
            len,
            decode: Some(decode),
            raw: Some(raw.to_vec()),
            ..Self::event(peer, "frame")
        }
    }
}

fn micros<S: serde::Serializer>(t: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error> {
    s.collect_str(&t.format("%Y-%m-%dT%H:%M:%S%.6fZ"))
}

fn hex_bytes<S: serde::Serializer>(raw: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
    s.collect_str(&hex::encode(raw.as_deref().unwrap_or_default()))
}

/// The command and payload length from a frame's header, if the header is all there.
fn header(raw: &[u8]) -> (Option<String>, Option<u32>) {
    if raw.len() < HEADER_LEN {
        return (None, None);
    }
    let command = String::from_utf8_lossy(&raw[4..16])
        .trim_end_matches('\0')
        .to_owned();
    let len = u32::from_le_bytes(raw[16..20].try_into().expect("4 bytes"));
    (Some(command), Some(len))
}

enum Msg {
    Record(Record),
    Close(mpsc::Sender<()>),
}

/// A handle to the writer thread. Cheap to clone. `off()` drops every record.
#[derive(Clone)]
pub struct WireLog {
    sender: Option<mpsc::Sender<Msg>>,
    /// Why each peer's reading stopped, for its `disconnect` event.
    reasons: Arc<Mutex<HashMap<SocketAddr, String>>>,
}

impl WireLog {
    pub fn off() -> Self {
        Self {
            sender: None,
            reasons: Default::default(),
        }
    }

    /// Recovers old `.part` files, then starts the writer thread.
    pub fn open(dir: &Path, level: i32) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        recover(dir)?;
        let (sender, receiver) = mpsc::channel();
        let mut writer = Writer {
            dir: dir.to_owned(),
            level,
            hour: None,
        };
        thread::Builder::new()
            .name("wire-log".into())
            .spawn(move || writer.run(receiver))?;
        Ok(Self {
            sender: Some(sender),
            reasons: Default::default(),
        })
    }

    pub fn write(&self, record: Record) {
        if let Some(sender) = &self.sender {
            let _ = sender.send(Msg::Record(record));
        }
    }

    /// Notes why reading from a peer stopped. The first reason wins.
    fn stopped(&self, peer: SocketAddr, reason: String) {
        if self.sender.is_some() {
            self.reasons.lock().entry(peer).or_insert(reason);
        }
    }

    /// Writes the `disconnect` event with the reason the codec saw, if any.
    pub fn disconnected(&self, peer: SocketAddr) {
        let reason = self
            .reasons
            .lock()
            .remove(&peer)
            .unwrap_or_else(|| "read error, or the listener closed it".into());
        self.write(Record {
            reason: Some(reason),
            ..Record::event(peer, "disconnect")
        });
    }

    /// Closes the current file, so it moves to its final name. Waits for the writer.
    pub fn close(&self) {
        if let Some(sender) = &self.sender {
            let (done, wait) = mpsc::channel();
            if sender.send(Msg::Close(done)).is_ok() {
                let _ = wait.recv();
            }
        }
    }
}

/// The listener's codec. It logs each frame's bytes, then decodes them.
pub struct WireCodec {
    inner: MessageCodec,
    peer: SocketAddr,
    log: WireLog,
}

impl WireCodec {
    pub fn new(peer: SocketAddr, log: WireLog) -> Self {
        Self {
            inner: MessageCodec::default(),
            peer,
            log,
        }
    }
}

impl Decoder for WireCodec {
    type Item = Message;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> io::Result<Option<Message>> {
        // The header stays in `src` until the whole frame is there.
        let (_, len) = header(src);
        let frame = len
            .map(|len| HEADER_LEN + len as usize)
            .filter(|&n| n <= HEADER_LEN + MAX_MESSAGE_LEN && src.len() >= n)
            .map(|n| src[..n].to_vec());
        let result = self.inner.decode(src);
        match &result {
            Ok(None) => {}
            Ok(Some(message)) => {
                let decode = match message {
                    Message::Tx(tx) => tx
                        .decode_error()
                        .map_or("ok".into(), |e| format!("tx: {e}")),
                    _ => "ok".into(),
                };
                let raw = frame.unwrap_or_default();
                self.log.write(Record::frame(self.peer, "in", &raw, decode));
            }
            Err(e) => {
                // A frame over the size limit never arrives whole. Keep its header.
                let raw = frame.unwrap_or_else(|| src[..src.len().min(HEADER_LEN)].to_vec());
                self.log
                    .write(Record::frame(self.peer, "in", &raw, e.to_string()));
                self.log.stopped(self.peer, format!("decode: {e}"));
            }
        }
        result
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> io::Result<Option<Message>> {
        if let Some(message) = self.decode(src)? {
            return Ok(Some(message));
        }
        if src.is_empty() {
            self.log
                .stopped(self.peer, "the peer closed the connection".into());
            return Ok(None);
        }
        let reason = "the peer closed the connection inside a frame";
        self.log
            .write(Record::frame(self.peer, "in", src, reason.into()));
        self.log.stopped(self.peer, reason.into());
        Err(io::Error::new(io::ErrorKind::UnexpectedEof, reason))
    }
}

impl Encoder<Message> for WireCodec {
    type Error = io::Error;

    fn encode(&mut self, message: Message, dst: &mut BytesMut) -> io::Result<()> {
        let start = dst.len();
        let result = self.inner.encode(message, dst);
        let decode = match &result {
            Ok(()) => "ok".into(),
            Err(e) => e.to_string(),
        };
        self.log
            .write(Record::frame(self.peer, "out", &dst[start..], decode));
        result
    }
}

struct Writer {
    dir: PathBuf,
    level: i32,
    /// The open hour, its `.part` path, and its encoder.
    hour: Option<(String, PathBuf, zstd::Encoder<'static, File>)>,
}

impl Writer {
    fn run(&mut self, receiver: mpsc::Receiver<Msg>) {
        loop {
            match receiver.recv_timeout(FLUSH) {
                Ok(Msg::Record(record)) => {
                    if let Err(e) = self.write(&record) {
                        error!("can't write the wire log: {e}");
                    }
                }
                Ok(Msg::Close(done)) => {
                    if let Err(e) = self.close() {
                        error!("can't close the wire log: {e}");
                    }
                    let _ = done.send(());
                    return;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if let Err(e) = self.flush(Utc::now()) {
                        error!("can't flush the wire log: {e}");
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    let _ = self.close();
                    return;
                }
            }
        }
    }

    fn write(&mut self, record: &Record) -> io::Result<()> {
        let key = hour_key(record.t);
        if self.hour.as_ref().is_none_or(|(open, ..)| *open != key) {
            self.close()?;
            let part = self.dir.join(format!("wire-{key}.ndjson.zst.part"));
            let file = OpenOptions::new().create(true).append(true).open(&part)?;
            let mut encoder = zstd::Encoder::new(file, self.level)?;
            encoder.window_log(WINDOW_LOG)?;
            encoder.long_distance_matching(true)?;
            encoder.include_checksum(true)?;
            self.hour = Some((key, part, encoder));
        }
        let (.., encoder) = self.hour.as_mut().expect("opened above");
        let mut line = serde_json::to_vec(record)?;
        line.push(b'\n');
        encoder.write_all(&line)
    }

    /// Flushes a block, and closes the hour once it has passed, even with no new records.
    fn flush(&mut self, now: DateTime<Utc>) -> io::Result<()> {
        match &mut self.hour {
            Some((key, ..)) if *key != hour_key(now) => self.close(),
            Some((.., encoder)) => encoder.flush(),
            None => Ok(()),
        }
    }

    fn close(&mut self) -> io::Result<()> {
        let Some((_, part, encoder)) = self.hour.take() else {
            return Ok(());
        };
        encoder.finish()?.sync_all()?;
        publish(&part)
    }
}

/// `YYYYMMDD-HH` in UTC.
fn hour_key(t: DateTime<Utc>) -> String {
    format!("{}-{:02}", t.format("%Y%m%d"), t.hour())
}

/// Moves a closed `.part` to its final name. A restart inside the hour already left a
/// file there, so append: DuckDB reads concatenated zstd frames.
fn publish(part: &Path) -> io::Result<()> {
    let done = part.with_extension("");
    if done.exists() {
        let mut out = OpenOptions::new().append(true).open(&done)?;
        io::copy(&mut File::open(part)?, &mut out)?;
        out.sync_all()?;
        fs::remove_file(part)
    } else {
        fs::rename(part, done)
    }
}

/// Re-encodes the complete lines of each `.part` that a crash left, then publishes it.
fn recover(dir: &Path) -> io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let part = entry?.path();
        if part.extension().is_none_or(|e| e != "part") {
            continue;
        }
        let fixed = part.with_extension("fixed");
        let mut lines = 0;
        {
            let mut decoder = BufReader::new(zstd::Decoder::new(File::open(&part)?)?);
            let mut encoder = zstd::Encoder::new(File::create(&fixed)?, 3)?;
            let mut line = Vec::new();
            loop {
                line.clear();
                // A crash cuts the stream. Keep what came before the cut.
                match decoder.read_until(b'\n', &mut line) {
                    Ok(_) if line.ends_with(b"\n") => {
                        encoder.write_all(&line)?;
                        lines += 1;
                    }
                    _ => break,
                }
            }
            encoder.finish()?.sync_all()?;
        }
        warn!("recovered {lines} wire log lines from {}", part.display());
        fs::rename(&fixed, &part)?;
        publish(&part)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use ziggurat_zcash::protocol::payload::{inv::Inv, Nonce};

    use super::*;

    fn read_lines(path: &Path) -> Vec<serde_json::Value> {
        let mut text = String::new();
        zstd::Decoder::new(File::open(path).unwrap())
            .unwrap()
            .read_to_string(&mut text)
            .unwrap();
        text.lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("wire-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn frames_in_and_out_keep_their_bytes() {
        let dir = temp_dir("frames");
        let log = WireLog::open(&dir, 3).unwrap();
        let peer: SocketAddr = "192.0.2.1:8233".parse().unwrap();
        let mut codec = WireCodec::new(peer, log.clone());

        let mut wire = BytesMut::new();
        codec
            .encode(Message::Ping(Nonce::default()), &mut wire)
            .unwrap();
        codec.encode(Message::MemPool, &mut wire).unwrap();
        let sent = wire.to_vec();
        // Read back what was sent, plus a frame with an unknown command.
        let mut bad = sent[..HEADER_LEN].to_vec();
        bad[4..16].copy_from_slice(b"nonsense\0\0\0\0");
        bad[16..20].copy_from_slice(&0u32.to_le_bytes());
        wire.extend_from_slice(&bad);

        assert!(matches!(
            codec.decode(&mut wire),
            Ok(Some(Message::Ping(_)))
        ));
        assert!(matches!(
            codec.decode(&mut wire),
            Ok(Some(Message::MemPool))
        ));
        assert!(codec.decode(&mut wire).is_err());
        log.disconnected(peer);
        log.close();

        let files: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert_eq!(files.len(), 1);
        assert!(files[0].to_str().unwrap().ends_with(".ndjson.zst"));
        let lines = read_lines(&files[0]);
        let kinds: Vec<_> = lines
            .iter()
            .map(|l| format!("{} {} {}", l["kind"], l["dir"], l["command"]))
            .collect();
        assert_eq!(
            kinds,
            [
                "\"frame\" \"out\" \"ping\"",
                "\"frame\" \"out\" \"mempool\"",
                "\"frame\" \"in\" \"ping\"",
                "\"frame\" \"in\" \"mempool\"",
                "\"frame\" \"in\" \"nonsense\"",
                "\"disconnect\" null null",
            ]
        );
        let ping_len = sent.len() - HEADER_LEN;
        assert_eq!(lines[0]["raw"], hex::encode(&sent[..ping_len]));
        assert_eq!(lines[0]["len"], 8);
        assert_eq!(lines[2]["decode"], "ok");
        assert!(lines[4]["decode"]
            .as_str()
            .unwrap()
            .contains("Unknown command"));
        assert_eq!(lines[4]["raw"], hex::encode(&bad));
        assert!(lines[5]["reason"].as_str().unwrap().starts_with("decode:"));
        // Microseconds: 2026-11-11T16:00:00.123456Z
        assert_eq!(lines[0]["t"].as_str().unwrap().len(), 27);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_cut_frame_at_eof_ends_the_stream() {
        let log = WireLog::off();
        let peer: SocketAddr = "192.0.2.1:8233".parse().unwrap();
        let mut codec = WireCodec::new(peer, log);
        let mut wire = BytesMut::new();
        codec
            .encode(Message::GetData(Inv::new(Vec::new())), &mut wire)
            .unwrap();
        wire.truncate(wire.len() - 1);
        assert!(codec.decode_eof(&mut wire).is_err());
    }

    #[test]
    fn a_crashed_part_keeps_its_complete_lines() {
        let dir = temp_dir("recover");
        fs::create_dir_all(&dir).unwrap();
        let part = dir.join("wire-20261111-16.ndjson.zst.part");
        let mut encoder = zstd::Encoder::new(File::create(&part).unwrap(), 3).unwrap();
        encoder.write_all(b"{\"a\":1}\n{\"a\":2}\n{\"a\":").unwrap();
        // A flush with no finish: the crash cut the frame.
        encoder.flush().unwrap();
        drop(encoder);
        // An earlier run in the same hour already published a frame.
        let done = dir.join("wire-20261111-16.ndjson.zst");
        fs::write(&done, zstd::encode_all(&b"{\"a\":0}\n"[..], 3).unwrap()).unwrap();

        recover(&dir).unwrap();
        assert!(!part.exists());
        let values: Vec<_> = read_lines(&done).iter().map(|l| l["a"].clone()).collect();
        assert_eq!(values, [0, 1, 2]);
        fs::remove_dir_all(dir).unwrap();
    }
}
