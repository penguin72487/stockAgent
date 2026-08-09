# Shioaji HFT dataset

This module converts the raw Shioaji top-200 Tick and five-level BidAsk capture
into one causal row per `(trade_date, snapshot_ts_ns, code)`.

Each two-worker capture has one shared `capture_id`. Parquet part filenames and
worker manifests carry that identifier, so a retry on the same trade date does
not get silently combined with an earlier attempt. Schema-2 captures are read
by their manifest start/finish interval for backward compatibility.

## Automatic startup

Install the systemd service once. It starts the foreground runner at boot and
lets the runner wait for the next Taipei capture window:

```bash
sudo bash scripts/install_shioaji_top200_service.sh
systemctl status stockagent-shioaji-top200.service
journalctl -u stockagent-shioaji-top200.service -f
```

Operational controls:

```bash
systemctl stop stockagent-shioaji-top200.service
systemctl start stockagent-shioaji-top200.service
systemctl restart stockagent-shioaji-top200.service
systemctl disable --now stockagent-shioaji-top200.service
```

## Build and audit

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/build_shioaji_hft_dataset.py --trade-date 2026-07-20
run_fintech_python scripts/audit_shioaji_hft_dataset.py --trade-date 2026-07-20
```

Omit `--trade-date` from the builder to rebuild every captured date. The normal
`scripts/run_shioaji_top200_stream.sh` workflow builds and audits the partition
automatically after both capture workers and the raw-capture audit succeed.

Outputs:

- `data_tw_microstructure/hft_dataset/trade_date=YYYY-MM-DD/data.parquet`
- `data_tw_microstructure/hft_dataset/trade_date=YYYY-MM-DD/summary.json`
- `data_tw_microstructure/hft_dataset/manifest.json`
- `data_tw_microstructure/audits/hft_YYYY-MM-DD.json`

## Compact raw captures for transfer

The live recorder intentionally writes immutable parts so an interrupted market
session cannot corrupt a large Parquet file.  After the corresponding HFT dates
have passed both audits, compact the selected `capture_id` parts into an
immutable transfer snapshot with exactly one Parquet file per symbol:

```bash
source scripts/runtime_env.sh
POLARS_MAX_THREADS=32 run_fintech_python \
  scripts/compact_shioaji_microstructure_by_symbol.py \
  --through-date 2026-08-06 \
  --output-root data_tw_microstructure/symbol_dataset_20260806
```

The compactor checks the HFT summary, audit receipt, worker manifests, stream
row totals, symbol membership, and output hashes before atomically publishing
the output.  It excludes dates after `--through-date`, so the current live
capture is not read while it is being written.  Output files are
`symbols/symbol=CODE/data.parquet`; Tick, BidAsk events, and one-second books
share a union schema and are identified by `source_stream`.  The bounded-memory
writer reads one completed date and stream at a time and appends large row
groups directly to each symbol file; it does not create a second partitioned
dataset as an intermediate result.

This symbol layout is for raw-event archival and machine transfer.  Keep the
causal HFT training dataset partitioned by trading date, because training reads
the market cross-section for each time interval.  Parquet is not appendable, so
publish each compacted history as a new immutable snapshot instead of mutating
an older symbol file in place.

## Feature and label contract

- Session rows are restricted to `[09:00:00, 13:30:00)` Asia/Taipei.
- Tick and BidAsk events are bucketed by their local receive time, rounded up to
  the next one-second snapshot boundary. Feature event timestamps therefore
  never exceed `snapshot_ts_ns`.
- `feature_valid` requires a non-suspended, two-sided, non-crossed book no more
  than 5 seconds old. Invalid rows remain present so models can use an explicit
  mask; they are not silently imputed as valid observations.
- Features include top-five prices and volumes, spread, microprice, L1/L5 depth
  imbalance, Tick flow, BidAsk update flow, and causal rolling returns/flow.
- Labels are provided at 1, 5, 30, and 60 seconds. A label is valid only when
  the exact future second exists and both current and future books are valid.
- `future_mid_log_return_*` is a continuous mid-price target.
- `long_cross_spread_markout_bps_*` buys at the current ask and marks at the
  future bid. `short_cross_spread_markout_bps_*` sells at the current bid and
  marks at the future ask. Both are gross of broker fees, tax, latency, queue
  position, partial fills, and market impact.

Do not randomly split rows. Split by complete trading dates with a purge gap at
least as long as the largest label horizon. A single partial day is suitable for
pipeline verification only, not for train/validation/test evaluation.
