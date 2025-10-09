
<p align="center">
  <img src="lokirich.png" alt="loki richlist dashboard" width="540">
</p>

`loki-richlist` is a standalone CLI for maintaining and reporting the richest addresses on a Flokicoin chain. It reuses the node’s `ffldb` database, tracks per-address balances with resume support, and prints a human-readable rich list summary after each run.

### Usage

```bash
go run . \
  --datadir=/path/to/flokicoind/data \
  --dbtype=ffldb \
  --limit=1000 \
  --reindex \
  --dune-output=/path/to/top1000.csv
```

- `--datadir` should point at the same data directory used by `flokicoind`.
- The tool stores its own state under `richlist` inside that directory (namespaced per network).
- Pass `--reindex` to rebuild the rich list database from scratch.

### Exporting to Dune

Provide `--dune-output=/path/to/top100.csv` to generate a CSV file with the top `N` entries (default 100) containing `rank`, `address`, `balance_flc`, and `last_seen_utc`. Upload this file to Dune to back a dashboard that visualises the current top addresses alongside their last activity timestamp.

### Development

```bash
go build ./...
```

Set `GOCACHE=$PWD/.gocache` (or another writable directory) if needed when building in restricted environments.
