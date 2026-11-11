# Mempool listener deployment

The listener runs on the metrics VPS (`metrics.shieldedinfra.net`). It connects to about 50 Zebra and Zakura peers, logs mempool gossip, and exports Prometheus metrics. Grafana on the same host alerts on them.

```mermaid
flowchart LR
  P["Zebra/Zakura peers"] -->|inv, tx| L[listener]
  L -->|getdata, mempool| P
  L --> D["data/mempool/*.ndjson(.gz)"]
  M["Prometheus + Grafana<br/>(same host)"] -->|"mempool-listener:9101 on metrics-net"| L
  M -->|alerts| Phone
```

## Files

| File | Use |
|---|---|
| `compose.yml` | the listener service, on `metrics-net`, capped at 512 MB |
| `Dockerfile` | fetches the release binary |
| `mempool.toml` | listener config and per-tx rule thresholds |

The Grafana alert rules live in `ShieldedLabs/shielded-infra` (`workspaces/metrics/grafana/alerting/mempool.yaml`). Its `workspaces/metrics/README.md` lists the host steps.

## Set up

1. Copy this directory to `/root/mempool` on the metrics VPS, and `.env.example` to `.env`.
2. Copy a recent `census-*.ndjson.gz` from the crawler into `data/census/`.
3. Start the listener: `docker compose up -d --build`.
4. Check the metrics: `docker run --rm --network metrics-net curlimages/curl -s http://mempool-listener:9101/metrics`.

## Alerts

| Alert | Where it runs | Fires when |
|---|---|---|
| Burst | listener | 5+ txs of one shape in 10 min, over 3x the shape's usual rate, and under a 1 in 10,000 chance at that rate |
| Under floor | listener | 3+ txs in 10 min pay under 1,000 zats per ZIP 317 action |
| Large tx | listener | a tx is over 100 KB |
| Depth, bytes | Grafana | the mempool is over 3x its 24 h median |
| Few peers, quiet, down | Grafana | under 10 peers, no new tx for 30 min, or no scrape |
| Peer backlog | Grafana | one peer lists 100+ txs and over 10x the median peer for 10 min |

The listener counts its alerts in `mempool_alerts_total{rule}`, and Grafana alerts on that counter. The txids and shapes are in the `alert` events.

## Metrics

| Metric | Meaning |
|---|---|
| `mempool_peers_connected` | peers past the handshake |
| `mempool_txs_seen_total` | txs seen for the first time today (UTC). At start the listener reads today's `seen` events, so a restart counts no tx twice |
| `mempool_txs_fetched_total`, `mempool_txs_undecoded_total` | txs fetched, and those zebra-chain could not parse |
| `mempool_txs_live` | txs seen in the last 10 min |
| `mempool_snapshot_median`, `mempool_live_bytes` | median peer mempool count, bytes of fetched live txs |
| `mempool_peer_snapshot{peer,user_agent}` | each peer's latest mempool count (0 when stale); a dropped peer's series expires after 5 min |
| `mempool_alerts_total{rule}` | per-tx rule alerts |

## Events

One line per event in `mempool-YYYYMMDD.ndjson`, with `t` (unix seconds) and `event`. At each UTC day change the listener gzips the past days' files to `.ndjson.gz`. Expect about 50 MB a day in all.

| Event | Meaning |
|---|---|
| `peer_up`, `peer_down` | a peer finished its handshake, or dropped |
| `seen` | a peer announced a tx by gossip, or a snapshot showed a tx new today |
| `snapshot` | a peer answered `mempool` with `count` ids |
| `tx` | a fetched tx: counts, ZIP 317 actions, fee (null with transparent inputs) |
| `tx_undecoded` | zebra-chain could not parse a tx; the raw file keeps its bytes |
| `gone` | no peer listed the tx for 10 min (mined or evicted) |
| `back` | a tx marked gone returned within an hour (rebroadcast, or a node re-added it); it isn't counted or fetched again |
| `alert` | a per-tx rule fired |
| `alive`, `stop` | hourly counts, and the counts at shutdown |

`raw-YYYYMMDD.ndjson.gz` holds `{"wtxid","hex"}` per fetched tx. Each line is its own gzip member, so `gzcat` reads it after a crash.

Replay a day through the per-tx rules: `mempool-listener --config mempool.toml --replay mempool-YYYYMMDD.ndjson.gz`.
