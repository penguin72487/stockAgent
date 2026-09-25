# Shared Windows/WSL host resource audit (2026-09-20)

This is a measured snapshot on penguin, not a portable throughput benchmark.
The Taiwan market was closed on Sunday. No trading ledger or Shioaji execution
service was modified.

## Causal diagnosis

- The weekly `registered-data-backfill` and daily registered refresh overlapped.
  The full-history OKX/Bybit/Binance processes used 16/24/32 candle workers,
  with the Binance feature stage configured for 32 more. Their three resident
  sets were about 13.6/18.7/36.7 GB. The backfill cgroup reached about 71 GB,
  host swap reached 25 GB, and the system spent substantial time reclaiming
  memory. Provider independence alone does not make unbounded *decoded history*
  concurrency safe on a shared host.
- The free-public collector had 22,524,308 rows in one Parquet. After its 16
  requests finished in about 12 minutes, its old full-table unique/sort/rewrite
  path continued for roughly three hours. This was local CPU/memory work, not
  provider throttling. One failed Coin Metrics catalog request was separately
  retried after the optimization.
- The Discord process had completed a five-model CUDA startup warmup at 15:17
  Friday, after the market close, and retained its model cache throughout the
  weekend. GPU memory occupancy is not GPU compute utilization; WSL cannot
  attribute the remaining Windows-side GPU allocation to this process.

## Changes and evidence

- Full-history refresh now defaults to 8 OKX, 8 Bybit, 12 Binance candle and 8
  Binance feature workers. The three providers still run independently in
  parallel and their endpoint rate limiters are unchanged. Overrides are
  `BACKFILL_OKX_WORKERS`, `BACKFILL_BYBIT_WORKERS`,
  `BACKFILL_BINANCE_WORKERS`, and `BACKFILL_BINANCE_FEATURE_WORKERS`.
  Daily hot-tail concurrency is unchanged. The ongoing weekly job was stopped
  once and resumed with those settings; completed per-symbol files remained.
  In-flight, uncommitted symbol pages may have needed refetching.
- Free-public observations keep the same atomic single-Parquet ABI and full
  vintages. A strictly later observed vintage now streams old row groups into
  a sibling temporary file and appends the new rows. A repeated or out-of-order
  vintage uses the original deduplicating path. The targeted Coin Metrics retry
  added 131,382 rows and completed fetch plus merge in 42 seconds; the manifest
  then showed all 26 registered sources as `updated`. This does not prove the
  entire 16-source daily cycle now takes 42 seconds.
- Startup CUDA warmup is permitted only in the 75 minutes before a verified
  trading session's 08:15 preopen preparation. The bot checks every five
  minutes outside protected trading hours and clears retained model/panel
  caches plus PyTorch's unused CUDA allocation only when the inference lock is
  free. The Discord service was restarted on the closed Sunday after verifying
  zero active/queued interactive jobs. Gateway reconnected and the day-trade
  and overnight execution services remained active. Immediately after restart,
  `nvidia-smi` showed no WSL compute process; the shared GPU still reported
  roughly 5.5 GB allocated by other/Windows-side activity.

## Acceptance boundary

Focused tests: 180 passed. The new backfill processes were observed with
`--workers 8/8/12 --feature-workers 8`; their cgroup had 0 swap immediately
after restart. This establishes a safer resource envelope, not proof that all
three official request ceilings are saturated, that the multi-year backfill is
complete, or that a Windows game has a particular frame rate. Compare request
pages per minute, completed symbols, cgroup RSS/swap, and game responsiveness
over a full run before raising the backfill overrides. Do not lower the daily
09:00 signal priority or restart stateful Shioaji execution to reclaim GPU.

## Second pass: bounded memory and idle CPU

The three 1m collectors previously accumulated every decoded page for one
symbol in Python lists before normalizing. `CandleFrameBuffer` now converts at
most 50,000 raw rows at a time into the existing provider-specific Polars
schema. Cross-batch minute duplicates keep the later value. Endpoint limiters,
request parameters, output columns, atomic Parquet publication, and completed
symbol resume semantics are unchanged. A 150,000-row synthetic Binance
comparison returned the same count/checksum: old/new Python traced peak
139.2/46.5 MiB, process peak RSS 733.7/356.0 MiB, elapsed 4.193/4.023 s.
This is an isolated normalization benchmark, not a full-download speed claim.

The active weekly backfill was restarted once on the closed Sunday to load the
new code. Completed symbol files were preserved; uncommitted in-flight pages
needed refetching. After roughly five minutes, the cgroup was about 35.6 GB
with zero swap, compared with 42.3 GB immediately before restart. These are
different symbol mixes and are not a causal long-run memory comparison. The
remaining major memory cost is concurrent full-base Parquet reconciliation;
reducing it requires preserving the logical base+hot-tail reader and the
weekly compaction contract, not merely reducing API workers. In the same run,
progress receipts showed about 18.8 Binance kline pages/s (37.6 weight/s
against 40 weight/s configured), 8.3 OKX pages/s against its 10/s configured
limit, and 40 Bybit pages/s against 120/s configured. The Bybit gap should be
revisited only with sustained latency and memory evidence.

The public status snapshot was a separate idle CPU consumer: one 30-second
cycle used about 6-8 s CPU. Profiling found a repeated parse of about 157,000
JSONL rows from an already verified immutable TW public cold inventory, about
1.9 s per cycle. The short-lived snapshot process now stores only the small
membership/count result in its writable live-status directory. Every cycle
still validates the head and manifest and checks the inventory path/identity;
a changed release, file identity, or verification hour forces full SHA-256 and
gzip/JSONL validation. The first run after the change took 5.6 s, the next
unchanged run 3.18 s and 161 MB peak, versus 6.0-8.0 s and roughly 260-302 MB
before the cache. Feature-inventory regeneration remains about 2 s when its
physical footer cache changes, because exact non-null counts must be updated.

The day-trade runner now waits five seconds only when idle outside 08:10-14:00
or on a closed session outside the protected 08:10-09:10 interval. Pending
signals and open positions keep the fast path, and inotify still wakes on a
new atomic signal. The Discord scheduler reuses a one-second TW session proof
outside 08:10-09:10; during the protected opening it re-verifies every call.
These two long-lived processes were intentionally not restarted, so their
live CPU improvement is not yet measured. Neither Shioaji execution service
nor its ledger was restarted. The Shioaji history service's reported 19 GB
cgroup occupancy was mostly reclaimable file cache/slab, not 19 GB Python RSS.

## WSL host RAM: measured cause and bounded live reclaim

On the same Sunday, Windows reported 127.9 GiB physical RAM, only 16.6 GiB
free, and a `vmmemWSL` working set of 85.0 GiB. Linux still reported about
86 GiB `MemAvailable` out of 94 GiB, with about 50 GiB cached files and 8 GiB
reclaimable slab. The three running full-history crypto processes had about
23 GiB anonymous RSS, but `smaps_rollup` identified about 16 GiB of that as
`LazyFree`: already discarded by the allocator, yet resident until the kernel
reclaims it. The WSL config already had `autoMemoryReclaim=gradual`; this was
not an absent-reclaim-setting problem. Historic 11 GiB swap use was present,
but `vmstat` showed no current swap-in or swap-out in the sampled interval.

Five bounded `memory.reclaim` requests against **only** the registered backfill
cgroup, totaling 22 GiB requested, reduced its charged memory and later
raised Windows free RAM to about 30.0 GiB while `vmmemWSL` fell to about
68.8 GiB. The difference between requested reclaim and Windows working-set
change is expected: the cgroup and the VM account different layers, and the
host release is asynchronous. No downloader or trading service was restarted.

`scripts/reclaim_wsl_backfill_memory.py` makes this pressure-dependent. Its
default dry run reports Windows free RAM, the backfill processes' `LazyFree`,
and a bounded request. `--apply` acts only if Windows has under 32 GiB free,
retains at least 2 GiB of reported `LazyFree`, requests at most 4 GiB per
invocation, and skips 08:20–09:10 Taipei time. The systemd timer runs every
15 minutes and only touches the registered backfill cgroup. It cannot reclaim
the unrelated Shioaji or OpenBB file caches, and the kernel can choose which
pages to reclaim inside the target cgroup; it is not a throughput-neutral
guarantee. Watch page throughput and major faults before changing the budget.

Useful read-only check:

```bash
/usr/bin/python3 scripts/reclaim_wsl_backfill_memory.py
systemctl status stockagent-wsl-backfill-memory-reclaim.timer
free -h
```

The current `.wslconfig` stays at `memory=96GB` and
`autoMemoryReclaim=gradual`. Lowering the VM cap or switching to `dropCache`
would require a WSL shutdown to take effect and could sacrifice repeated-read
throughput; neither was silently changed while collectors and trading services
were running. A subsequent Windows/game workload and full backfill measurement
is needed before changing that global policy.
