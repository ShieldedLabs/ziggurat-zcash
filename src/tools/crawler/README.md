# Network crawler

## Running

The network crawler uses optional features and dependencies, which **must** be enabled in order for the binary to compile. These can be enabled by supplying `--features crawler` when running the command. 

The following command will print the command line options for the crawler:

```fish
$ cargo run --release --features crawler --bin crawler -- --help

OPTIONS:
    -c, --crawl-interval <CRAWL_INTERVAL>
            The main crawling loop interval in seconds [default: 5]

    -h, --help
            Print help information

    -r, --rpc-addr <RPC_ADDR>
            If present, start an RPC server at the specified address

    -s, --seed-addrs <SEED_ADDRS>...
            A list of initial standalone IP addresses and/or DNS servers to connect to

    -n, --node-listening-port <NODE_LISTENING_PORT>
            Default port used for connecting to the nodes [default: 8233]

    -V, --version
            Print version information
```

`--seed-addrs` \ `--dns-seed` is the only required argument and needs at least one specified address for it to run.

## Census

The crawler advertises protocol version 170190 (NU7) and the user agent `/zero-census:0.1.0/` (change it with `--user-agent`).

- `--export-dir <dir>` writes `census-<UTC start>.ndjson.gz` after each crawl interval and on exit. Each write replaces the file of the same run. The file has one JSON line per node that sent its Version message: `ip`, `port`, `protocol_version`, `user_agent`, `services`, `start_height`, `relay`, `first_seen`, `last_seen`, `handshake_ms`, `chain`.
- `chain` is `flux` when the port is 16125, the user agent contains "flux", or `start_height` is more than 100,000 blocks from the Zcash tip. Otherwise it is `zcash`.
- `--zcash-tip <height>` sets the tip. Without it, the tip is the median start height of Zebra and Zakura peers.
- `--duration-secs <n>` crawls for n seconds from process start, writes the census, and exits 0.
- On exit, the crawler prints a census summary without Flux nodes (counts by implementation and version, and the share at protocol 170190 or higher) and appends it to `crawler-log.txt`. The summary starts with the crawl funnel (known, attempted, TCP connected, sent Version) and the top reasons nodes sent no Version.
- Many listening nodes have full inbound slots. They accept TCP and close the connection before they send a Version. The crawler retries them on every crawl loop, because Zebra does not count a rejected attempt against our IP. It reconnects to a node that sent its Version after 5 minutes, because Zebra and Zakura drop a reconnect from the same IP within 119 seconds. Addresses that fail at the TCP level back off from 1 to 32 minutes.
- A crawler only sees listening nodes with a free inbound slot. Coverage grows with run time, so prefer long runs. On a Mac, run under `caffeinate -i`: the run clock stops while the Mac sleeps.

## Metrics

The crawler collects some data for each node it visits, then aggregates it and compiles related metrics. By default, it will only print and log these on exit (`Ctrl-C`) to a file called `crawler-log.txt`, unless the `--rpc-addr` argument is supplied, in which case these metrics will also be made available to RPC requests.

Fetching metrics from the RPC via `cURL` (piping through [`jq`](https://github.com/stedolan/jq) for prettier output):

```fish
$ curl --data-binary '{"jsonrpc": "2.0", "id":0, "method": "ge
tmetrics", "params": [] }' -H 'content-type: application/json'
 http://127.0.0.1:54321/ | jq .result
```

A sample of the data we collect and metrics we compute (obtained via RPC):

```json
{
  "num_known_nodes": 13654,
  "num_good_nodes": 2066,
  "num_known_connections": 1888893,
  "num_versions": 2019,
  "protocol_versions": {
    "170017": 10,
    "170018": 1958,
    "170016": 1,
    "170100": 50
  },
  "user_agents": {
    "/MagicBean:4.0.1/": 1,
    "/MagicBean:5.1.0-rc1/": 2,
    "/MagicBean:5.1.0/": 28,
    "/MagicBean:5.0.2/": 7,
    "/MagicBean:5.2.0/": 2,
    "/MagicBean:6.0.0/": 1957,
    "/MagicBean:5.1.1/": 1,
    "/Zebra:1.0.0-beta.10/": 1,
    "/MagicBean:6.0.0(bitcore)/": 1,
    "/MagicBean:5.0.0/": 19
  },
  "crawler_runtime": {
    "secs": 145,
    "nanos": 298240944
  },
  "density": 0.020101430617415265,
  "degree_centrality_delta": 1178,
  "avg_degree_centrality": 274
}
```
