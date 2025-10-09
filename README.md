
<p align="center">
  <img src="lokirich.png" alt="loki richlist dashboard" width="540">
</p>

`loki-richlist` is a standalone CLI for maintaining and reporting the richest addresses on a Lokichain. It reuses the node’s `ffldb` database, tracks per-address balances with resume support, and prints a human-readable rich list summary after each run.

### Requirements

- Go 1.24.4 or newer in your `PATH`.
- Local access to a fully synced `flokicoind` data directory (stop the daemon before running the CLI).

### Usage

```bash
go run .
```

First sync your Lokichain using the [run-a-node guide](https://docs.flokicoin.org/lokichain#run-a-node), then shut the node down so the CLI can read a clean snapshot from `ffldb`. Rich list state lives under `richlist`, namespaced per network.

- `--datadir` (optional): Custom `flokicoind` data path; defaults to the node’s standard location.
- `--dbtype` (default `ffldb`): Backend for block and rich list storage.
- `--limit` (default `100`): Count of addresses printed after indexing.
- `--progress` (default `10`): Seconds between progress updates; `0` silences them.
- `--dune-output` (optional): CSV file for `rank,address,balance_flc,last_seen_utc`.
- `--reindex` (optional): Drop and rebuild the rich list.

### Exporting to Dune

Provide `--dune-output=/path/to/top100.csv` to generate a CSV file with the top `N` entries (default 100) containing `rank`, `address`, `balance_flc`, and `last_seen_utc`. Upload this file to Dune to back a dashboard that visualises the current top addresses alongside their last activity timestamp.

### Development

```bash
go build ./...
```

Set `GOCACHE=$PWD/.gocache` (or another writable directory) if needed when building in restricted environments.
