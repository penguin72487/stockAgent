# stockAgent

Multi-asset Taiwan stock trading research workspace.

## Current status

- Raw research data lives under `data_parquet/`.
- Each parquet file represents one stock symbol, for example `2330_features.parquet`.
- The current training and validation specification is documented in `docs/training_spec.md`.
- Market-specific experiment templates live under `configs/markets/`, for example `configs/markets/tw.yaml`. The legacy `configs/experiment_baseline.yaml` is kept for compatibility.

## Planned workflow

1. Normalize all symbol parquet files into a shared date x symbol panel.
2. Build benchmark returns from each market config's `data.benchmark_name`; use `universe_average_return` only when an explicit universe-average benchmark is desired.
3. Run yearly expanding-window walk-forward validation.
4. Train GPU-enabled reference models first, then portfolio and RL policies.

## Training

- Install dependencies from `requirements.txt` inside the `fintech` environment.
- Source `scripts/runtime_env.sh` once per shell, then run Python entrypoints with `run_fintech_python`; it discovers the local `fintech` environment without assuming an absolute installation path.
- Run Taiwan training with `run_fintech_python train.py --config configs/markets/tw.yaml`; outputs go to that market config's `runner.output_dir`.
- Run the independent Taiwan public-data experiment with `run_fintech_python train.py --config configs/markets/tw_public.yaml`; outputs go to `artifacts/markets/tw_public_official_2005_v1`.
- `data.use_tw_public_rules` applies official TW execution masks independently from
  model inputs. TW configs enable it by default and fail fast if the configured
  public parquet is missing. `configs/markets/tw_public.yaml` additionally enables
  `data.use_tw_public_features`, appending its `twpub_*` columns to model inputs.
- Use `data.feature_include` and `data.feature_exclude` to manually switch panel features by exact name or glob pattern, for example `twpub_*` or `*_logret_1d`; leave both empty to keep all features.
- Or use the project runner: `./coda_runner.sh`.
- Runner defaults live in each experiment YAML's `runner` section; runtime discovery is centralized in `scripts/runtime_env.sh`.

### Multi-GPU market job manager

Assign physical GPUs to existing market configs in `configs/gpu_jobs.yaml`, then
manage all jobs or selected jobs without changing their market/training settings:

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/manage_gpu_jobs.py validate
run_fintech_python scripts/manage_gpu_jobs.py start
run_fintech_python scripts/manage_gpu_jobs.py status
run_fintech_python scripts/manage_gpu_jobs.py restart crypto
run_fintech_python scripts/manage_gpu_jobs.py stop us crypto
```

Each job is launched in its own process session with `CUDA_VISIBLE_DEVICES` set
from its `gpus` list. A multi-GPU job can use `gpus: [2, 3]`; its referenced
market config must also select the appropriate multi-GPU strategy. Runtime PID
state and launcher logs are stored under `artifacts/gpu_jobs` by default.

Market configs default to `training.multi_gpu_strategy: auto`: one visible GPU
uses the canonical single-device executor, while two or more visible GPUs
automatically relaunch one DDP rank per GPU. The manager controls visibility, so
`gpus: [0, 1, 2, 3]` makes the same market config use four-way DDP.
`configs/markets/tw_parallel.yaml` inherits `tw.yaml`, and
`configs/gpu_jobs_tw_parallel.yaml` exposes GPUs 0 and 1 to that one DDP job:

```bash
run_fintech_python scripts/manage_gpu_jobs.py validate \
  --config configs/gpu_jobs_tw_parallel.yaml
run_fintech_python scripts/manage_gpu_jobs.py start \
  --config configs/gpu_jobs_tw_parallel.yaml
```

Two-GPU US DDP is preconfigured in `configs/gpu_jobs_us_ddp.yaml`. Stop any
existing processes using GPUs 0 and 1, then validate and start the single DDP
job:

```bash
run_fintech_python scripts/manage_gpu_jobs.py validate \
  --config configs/gpu_jobs_us_ddp.yaml
run_fintech_python scripts/manage_gpu_jobs.py start \
  --config configs/gpu_jobs_us_ddp.yaml
run_fintech_python scripts/manage_gpu_jobs.py status \
  --config configs/gpu_jobs_us_ddp.yaml
```
- Outputs include one folder per walk-forward fold and a top-level `summary.json`.
- Neural models use one lazy-window executor per process: either one device or one
  process per GPU through torchrun DDP. Contiguous fixed-shape batches use the
  panel-slab forward when supported; guarded in-executor window materialization
  handles unsupported/non-contiguous/auxiliary cases. LightGBM/XGBoost keep their
  separate CPU materialized route because they are a different algorithm family.
- `trading.reporting_leverage` does not alter canonical train/validation/test
  exposure or integer-share execution. It only creates separate `leverage_*`
  reporting plots, recomputing turnover and fees after scaling weights.
- The latest experimental fold intentionally has validation/test overlap when
  `walk_forward.require_future_test_year: false`. With lookback 32, each split also
  intentionally discards its first 31 trading rows so its first sample owns a full
  in-split window. These are deliberate experiment semantics; do not normalize
  either behavior away.

## Market Data Downloads

- Source `scripts/runtime_env.sh` first and use `run_fintech_python` for every Python entrypoint; bare `python` is not a supported runtime selector for this repository.
- Run `run_fintech_python downloader/download_yahoo_ohlcv.py` to download four separate folders under `data_yahoo/`: `tw_stocks/`, `us_stocks/`, `crypto/`, and `forex/`.
- The downloader defaults to `2000-01-01` through today.
- Symbol downloads are parallelized with `--workers` (within each asset); when using `--asset all`, you can also parallelize assets via `--asset-workers`.
- Taiwan symbols are loaded from `data_parquet/symbols.csv` when available; otherwise they are fetched from TWSE ISIN listed (`strMode=2`) and OTC (`strMode=4`) lists.
- Taiwan delisted candidates are also included by default (`--include-tw-delisted`) and attempted with `.TW` / `.TWO` style Yahoo tickers.
- U.S. symbols are loaded from Nasdaq Trader symbol directories (`nasdaqlisted.txt` and `otherlisted.txt`) with static fallback.
- U.S. delisted symbols can be included from Alpha Vantage `LISTING_STATUS` when `ALPHAVANTAGE_API_KEY` (or `--alpha-vantage-api-key`) is provided.
- Crypto symbols are loaded from CoinGecko `/coins/list` and mapped to Yahoo format `${SYMBOL}-USD`.
- Forex symbols are loaded from Yahoo Finance currencies page tickers (with static fallback when rate-limited).
- Pepperstone-style FX universe is available via `configs/forex_pepperstone_pairs.txt` and can be downloaded to `data_yahoo/forex_pepperstone/`.
- Use `run_fintech_python downloader/download_yahoo_ohlcv.py --asset tw_stocks` to download only Taiwan stocks.
- Use `run_fintech_python downloader/download_alpaca_us_ohlcv.py --mode daily-update` to refresh the expanded U.S. stock universe from Alpaca Market Data into `data_yahoo/us_stocks`. Set `APCA_API_KEY_ID` and `APCA_API_SECRET_KEY` first.
- The Alpaca downloader batches many symbols into each paginated request. Basic accounts use the exact `200 requests/minute` limit by default; set `ALPACA_REQUESTS_PER_MINUTE=10000` for Algo Trader Plus.
- Use `run_fintech_python downloader/download_cboe_us_ohlcv.py --mode daily-update` when Cboe delayed historical OHLCV is preferred as an alternative U.S. source.
- The old Yahoo U.S. path remains available with `run_fintech_python downloader/download_yahoo_ohlcv.py --asset us_stocks`, but daily automation defaults to Alpaca because Yahoo often rate-limits large U.S. refreshes.
- Use `run_fintech_python downloader/download_yahoo_ohlcv.py --asset crypto` to download only the expanded crypto universe.
- Use `run_fintech_python downloader/download_yahoo_ohlcv.py --asset crypto --mode incremental` to refresh only missing/stale Yahoo crypto 15-minute bars.
- Yahoo crypto uses 15-minute bars; existing crypto parquet files that look like old daily data are rebuilt from the 15-minute source instead of being merged.
- Use `run_fintech_python downloader/download_yahoo_ohlcv.py --asset forex` to download only the expanded FX universe.
- Use `run_fintech_python downloader/download_forex_pepperstone.py` to download the Pepperstone-style FX universe.
- Use `run_fintech_python downloader/download_forex_pepperstone.py --mode repair` to repair stale/missing Pepperstone forex files.
- Use `run_fintech_python downloader/download_forex_pepperstone.py --mode daily-update` for daily incremental updates.
- Use `run_fintech_python downloader/download_pepperstone.py` to download grouped Pepperstone-style data to `data_peperstone/24hTrading`, `data_peperstone/commodites`, `data_peperstone/crypto`, and `data_peperstone/fores`.
- Use `run_fintech_python downloader/download_pepperstone.py --groups crypto fores` to download only selected groups.
- Use `run_fintech_python downloader/download_pepperstone.py --mode daily-update --groups all` for daily incremental updates across groups.
- Use `run_fintech_python downloader/download_okx_perp_15m.py --output-dir data_okx` to download all OKX perpetual swap 15-minute bars.
- Use `run_fintech_python downloader/download_okx_perp_15m.py --start-date 2020-01-01 --workers 6` to control download range and parallelism.
- Use `run_fintech_python downloader/download_okx_perp_15m.py --mode incremental` for incremental updates (only missing 15-minute candles).
- Use `run_fintech_python downloader/download_okx_perp_15m.py --mode full --refresh` when you need a full re-download.
- Use `run_fintech_python downloader/download_bybit_perp_15m.py --output-dir data_bybit` to download Bybit perpetual swap 15-minute bars.
- Use `run_fintech_python downloader/download_bybit_perp_15m.py --categories linear inverse --start-date 2020-01-01 --workers 6` to control Bybit categories, range, and parallelism.
- Use `run_fintech_python downloader/download_bybit_perp_15m.py --mode incremental` for incremental updates (only missing 15-minute candles).
- Use `run_fintech_python downloader/download_forex_frankfurter.py --mode daily-update --output-dir data_yahoo/forex` for daily incremental FX updates from Frankfurter.
- Each asset folder includes `symbols.csv`, `download_report.csv`, and `download_summary.json` alongside `*_features.parquet` files.
- Parquet output includes at least `date`, `open`, `max`, `min`, `close`, `adjclose`, `Trading_Volume`, and also preserves extra Yahoo columns when available (for example `Dividends`, `Stock Splits`).
- Override the default universe with `--symbols` or `--symbols-file`, for example `run_fintech_python downloader/download_yahoo_ohlcv.py --asset forex --symbols EURUSD GBPUSD USDJPY`.
- Use `run_fintech_python downloader/download_yahoo_ohlcv.py --mode incremental --asset all` for incremental updates across Yahoo assets; crypto remains 15-minute.

## Taiwan Public Data Download

- Use `run_fintech_python downloader/download_tw_official_data.py --mode rebuild|repair|daily` as the canonical TWSE/TPEx-first data-layer entry point. `rebuild` stages an atomic replacement, `repair` checks and fills historical gaps, and `daily` requires a verified baseline. From 2000 onward, the default audited Yahoo archive fills only missing stock/ETF OHLCV keys; official rows always win and row-level lineage is retained. Use `--ohlcv-fallback none` to disable it.
- The source archive retains receipt-backed rows from 2000, but the certified model panel starts at `2005-01-01` (`2005-01-03` is the first session). Official archives and terminal Yahoo receipts prove that 80 companies delisted by `2004-04-28` cannot be reconstructed without fabrication; starting with the first complete calendar year keeps that unavailable cohort outside the model horizon. `configs/markets/tw*.yaml` enforce this with `data.panel_start_date` and a 2005 walk-forward identity.
- A promoted rebuild is training-eligible only when both the staged and post-promote strict audits report `model_safe: true`. The durable production check is `artifacts/data_rebuild/<run>/audit_post_promote/summary.json`.
- On a new machine, run `run_fintech_python downloader/download_tw_official_data.py --mode rebuild --stage-root artifacts/data_rebuild/tw_2000_bootstrap --promote`. Reuse the same stage path after a rate-limited retry so completed symbols are skipped. To reuse existing Yahoo TW files, add `--yahoo-fallback-dir data_yahoo/tw_stocks --skip-yahoo-download`.
- With `--ohlcv-fallback yahoo`, the canonical runner builds
  `data_tw_public/tw_transfer_adjustment_reference.parquet` before projecting the
  Yahoo archive. This receipt-verified artifact supplies an adjustment factor
  only for an exact `date + symbol` match on a canonical Yahoo source's first
  retained row when its original factor is null. Coverage, input/output
  receipts, raw official request receipts, candidate keys, and
  reference-to-applied row counts are fail-closed. Its official requests share
  the `tw_public` global limiter; when `--request-interval` is omitted the core
  default is 10 requests/second. The artifact is not built or consulted with
  `--ohlcv-fallback none`.
- The high-level daily update also maintains official delisted-company histories in `twse_delisted_company.parquet` and `tpex_delisted_company.parquet`. For source diagnostics only, the low-level `download_tw_public_data.py --mode repair --datasets delisted` command can restrict the request to those tables.
- Backfill point-in-time delisting and short-sale/cover announcements separately:
  `run_fintech_python downloader/download_tw_short_sale_restrictions.py --output-dir data_tw_public --start-year 1995 --end-year "$(date +%Y)"`.
  The downloader writes `tw_short_sale_download_report.json` and refuses to replace
  the parquet outputs after an incomplete request unless `--allow-partial` is
  explicitly supplied.
- The low-level source downloader exposes `--datasets model_useful` for diagnostics. This group archives 117 point-in-time snapshots covering financial statements and monthly revenue, ownership and institutional flows, company/lifecycle events, shorting and securities-lending inputs, corporate actions, market-state rules, calendars, and index/constituent data. It intentionally excludes intraday leaderboards, broker rankings, auction-frequency-only feeds, warrants, bonds, gold, and funds.
- Run `repair` or `rebuild` before the first `daily` update. Daily mode never treats a few recent rows as a complete historical database.
- Snapshot-style OpenAPI feeds that only publish the latest table are stored with a `date` batch column, so daily runs accumulate same-day-replaced snapshots instead of discarding prior days.
- Government Data Platform datasets are resolved through `data.gov.tw` metadata at runtime, then written to parquet with raw metadata under `data_tw_public/metadata/`.
- Use tags to limit scope, for example `--datasets price`, `--datasets twse tpex`, `--datasets macro`, `--datasets taifex tdcc`, or a concrete dataset such as `twse_daily_ohlcv`.
- Use `--mode list --datasets all` to print the bundled dataset manifest.
- Outputs include one parquet per dataset, raw responses under `raw/` unless `--skip-raw` is set, plus `download_report.csv`, `download_summary.json`, and `dataset_manifest.json`.
- Historical backfills use both dataset-level concurrency (`--workers`) and date-level concurrency (`--date-workers`), and periodically flush partial parquet output with `--flush-every-dates` so long first runs can resume.
- For a smoke run, use `--start-date 2024-06-03 --end-date 2024-06-03 --datasets twse_daily_ohlcv tpex_daily_ohlcv --skip-raw`.
- Build the training feature parquet with `run_fintech_python scripts/build_tw_public_training_features.py --input-dir data_tw_public --output-path data_tw_public/features/tw_public_stock_daily.parquet --symbols-root data_tw_public/stocks`.
- Run that rebuild after updating the dedicated restriction archive. The generated
  rule columns keep `can_sell_mask` (may reduce an owned long) separate from
  `can_short_open_mask` (may open/increase a borrowed short). Ordinary halts,
  missing/zero-volume rows, and price-limit blocks freeze positions; only an
  explicit official permanent-exit event sets `force_exit_mask` and settles the
  position with the applicable buy/sell fee.
- Venue migration is not terminal: official `櫃轉市` rows and immediate
  same-symbol continuation do not trigger a synthetic liquidation. Sparse
  official daily snapshots are not interpreted as halts across long coverage
  gaps; explicit halt/resume notices remain authoritative.
- The feature parquet is a sparse `date` x `symbol` long table. Stock-specific rows align by ticker/date; macro/TAIFEX market rows use symbol `__MARKET__` and are broadcast to all stocks during panel build.
- Its `date` is the conservative availability date: daily market tables use trading date, TDCC uses data date plus a safety lag, monthly/quarterly macro uses period end plus lag when no explicit release date exists, and event tables use announcement/report date or downloader as-of date.
- `downloader/run_daily_all_markets.sh` and `downloader/daily_downloader_daemon.sh` update the restriction archive and then rebuild `data_tw_public/features/tw_public_stock_daily.parquet` by default. The TW stage uses `TW_PUBLIC_OHLCV_FALLBACK=yahoo` and `TW_PUBLIC_FALLBACK_START_DATE=2000-01-01` by default. Set `RUN_TW_SHORT_RESTRICTIONS=0` to skip the dedicated rules update, `RUN_TW_PUBLIC_DATA=0` to skip raw public data, `RUN_TW_PUBLIC_FEATURES=0` to skip feature rebuild, or `TW_PUBLIC_SKIP_RAW=1` when raw response archives are not needed.

### Repair Mode

- Use `run_fintech_python downloader/download_yahoo_ohlcv.py --mode repair --asset all` to check all assets and repair missing/stale parquet files toward today.
- Repair mode checks each symbol file for existence, latest date, and required schema columns (`date/open/max/min/close/adjclose`); missing/broken/stale/schema-mismatch symbols are repaired automatically.
- Repair outputs include top-level `repair_summary.json` and per-asset `repair_report.csv`.
- Adjust overlap with `--repair-overlap-days` (default `7`) to re-fetch a small trailing window before the local last date.
- If Yahoo returns `possibly delisted; no timezone found`, that ticker is automatically appended to per-asset `yahoo_blacklist.txt` and skipped in later runs.
- Successfully downloaded Yahoo tickers are persisted into per-asset `yahoo_whitelist.txt`.

### Daily All-Market Update

- Use `bash downloader/run_daily_all_markets.sh` to run daily updates across all configured markets.
- The script runs only the source-of-truth feed for each configured market by default: Alpaca `us_stocks`, Taiwan TWSE/TPEx public data plus feature rebuild and official OHLCV sync, Frankfurter forex incremental update to `data_yahoo/forex`, and OKX/Bybit perpetual 15-minute crypto updates. Yahoo and Pepperstone grouped downloads are opt-in fallback or research paths, not the fast daily default.
- Independent provider groups run concurrently by default; set `DAILY_PARALLEL_GROUPS=0` to force the old serial order.
- Set `RUN_TW_PUBLIC_DATA=0` to skip the Taiwan public data downloader. The first enabled run may backfill many historical official-data dates.
- Set `RUN_TW_PUBLIC_FEATURES=0` to skip rebuilding `data_tw_public/features/tw_public_stock_daily.parquet`.
- Set `RUN_PEPPERSTONE_GROUPS=1` to also run Pepperstone grouped fallback/research downloads.
- Set `RUN_FRANKFURTER=0` to skip Frankfurter cross-rate updates.
- Set `RUN_CEX_PERP=0` to skip OKX/Bybit updates.
- Full data-quality audit is opt-in because it scans parquet roots; set `RUN_DATA_QUALITY_AUDIT=1` when you want that check after downloads.
- Alpaca U.S. updates are capped by `ALPACA_US_STEP_TIMEOUT_SECONDS` (default `1800`). Tune throughput with `ALPACA_US_BATCH_SIZE`, `ALPACA_US_WORKERS`, `ALPACA_US_METADATA_WORKERS`, and the exact plan limit in `ALPACA_REQUESTS_PER_MINUTE`.
- Set `WORKERS`, `ALPACA_US_WORKERS`, `ASSET_WORKERS`, `PEPPERSTONE_WORKERS`, `OKX_WORKERS`, `BYBIT_WORKERS`, and `REPAIR_OVERLAP_DAYS` via environment variables to tune speed.

## Live Signal And Discord Bot

- Each market has one YAML file under `services/discord_bot/markets/`, for example `services/discord_bot/markets/tw.yaml`.
- Run a local live signal from a market config:
  `run_fintech_python scripts/live_signal.py --market-config services/discord_bot/markets/tw.yaml --price-source panel`
- Leave `fold_id` empty/null in the market YAML to discover the latest `fold_*/checkpoint_best.pt` under that market's `output_dir`.
- Use `--price-source csv --prices-csv path/to/prices.csv` for current-price mark-to-market. The CSV must include `symbol`/`code`/`ticker` and `price`/`close`/`last` columns.
- Per-market output is written under the market YAML's `live_output_dir`, for example `artifacts/live_signals/tw/YYYY-MM-DD/`:
  `summary.json`, `discord_message.md`, `target_weights.parquet`,
  `target_positions.md`, `rebalance.parquet`, `rebalance.md`,
  `decision_explanations.parquet`, `decision_explanations.md`,
  `decision_report.md`, and `model_explanation.json`.
- The Discord bot entrypoint is `services/discord_bot/bot.py`; configure it with `DISCORD_BOT_TOKEN`, `DISCORD_CHANNEL_ID`, `STOCKAGENT_MARKETS_DIR`, and `STOCKAGENT_DEFAULT_MARKET`.
- `services/discord_bot/bot.py` includes a reload supervisor by default: watched
  file changes restart the child bot process 10 seconds after the last update.
  Set `STOCKAGENT_BOT_RELOAD=0` to run without the supervisor.
- The bot exposes `/signal_now`, `/positions`, `/rebalance`,
  `/portfolio_history`, `/stock_history`, `/explain_signal`, `/markets`, and
  `/health`; market-aware commands accept a `market` option. `/positions`,
  `/rebalance`, `/portfolio_history`, `/stock_history`, and `/explain_signal`
  use paged Discord responses so long lists are not truncated.
  `/portfolio_history market:tw days:32 current_capital:1000000` shows recent
  PnL, current exposure, and holding changes from fold artifacts scaled to the
  supplied capital. Daily markets use days; crypto can set
  `history_frequency: bar` to show 15-minute bars. `/stock_history market:tw
  symbol:2330 limit:32` shows recent per-symbol trade/adjustment records.
  `/positions` and `/rebalance` accept `current_capital` to estimate
  current/target/trade amounts.
  `/set_capital` stores per-market default capital. `/explain_signal` can
  filter by symbol/action, sort by delta/score/target/return/rank, and
  optionally attach the full markdown decision report.
- Crypto Discord scheduling can use `schedule_interval_minutes: 15` plus a
  `pre_signal_command` data updater so each completed 15-minute bar is fetched
  before the bot sends the next signal. For manual testing, run
  `/signal_now market:crypto refresh_data:true`.
- `/signal_now` with `price_source:auto` now treats open markets as realtime:
  it runs the configured updater when available and uses current prices; closed
  markets use the latest panel close.
- Set `STOCKAGENT_SCHEDULED_MARKETS=all` to schedule every configured Discord
  market YAML.

## Environment

- Conda or mamba environment: `fintech`
- Training target: CUDA with Tensor Core acceleration
- Recommended repository command: `source scripts/runtime_env.sh`; then use `run_fintech_python <script> ...`.
- All repository shell entrypoints use the same runtime resolver. It selects the
  `fintech` environment and normalizes `CONDA_PREFIX`, `PATH`, and CUDA roots so
  an IDE/CI parent environment cannot leak into the run. Use an explicit
  `FINTECH_ENV_PATH=/path/to/fintech` or `PYTHON_BIN=/path/to/python` on machines
  with a nonstandard layout.

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda
run_fintech_python train.py --config configs/experiment_baseline.yaml
```

The environment checker prints the selected Python/CUDA roots, inherited values
before normalization, tool paths, package versions, and GPU inventory. Add
`--strict` when warnings (including a non-`fintech` prefix) should fail CI.

To recreate or update the environment:

```bash
mamba env export -n fintech --no-builds > fintech_environment.yml
mamba create -n fintech python=3.12
mamba env update -n fintech -f fintech_environment.yml

mkdir -p "$CONDA_PREFIX/conda-meta"
nano "$CONDA_PREFIX/conda-meta/pinned"
```

Example `conda-meta/pinned` contents:

```text
rapids>0.0.1
cuda-version >=13,<14
python=3.12
pydantic >=2.13.4
transformers >= 5.12.1
```

7z x data_tw_public.7z

sudo apt update && sudo apt full-upgrade -y && sudo apt autoremove -y && sudo snap refresh
cd /root/stockAgent
mamba activate fintech
mamba update --all
# train

cd /root/stockAgent
mamba activate fintech
source scripts/runtime_env.sh
CUDA_VISIBLE_DEVICES=0,1 run_fintech_python train.py   --config configs/markets/tw_public_lanten_market_candles_select.yaml --multi-gpu-strategy distributed_data_parallel

# explain 只有feature
cd /root/stockAgent
source scripts/runtime_env.sh

run_fintech_python scripts/check_environment.py --require-cuda --strict

export CUDA_VISIBLE_DEVICES=0,1
export OMP_NUM_THREADS=16
export MKL_NUM_THREADS=16
export POLARS_MAX_THREADS=16

run_fintech_python -m torch.distributed.run \
  --standalone \
  --nnodes=1 \
  --nproc-per-node=2 \
  scripts/screen_explainability_features.py \
  --config configs/markets/tw_public_lanten_market_candles.yaml \
  --device cuda \
  --cpu-threads 16 \
  --row-chunk-size 16 \
  --amp-dtype bf16 \
  --ig-steps 8 \
  --ig-batch-size 2 \
  --no-reuse-complete-explainability \
  --progress \
  --negligible-uniform-fraction 0.1

  # 全出
  cd /root/stockAgent
source scripts/runtime_env.sh

run_fintech_python scripts/check_environment.py --require-cuda --strict

export CUDA_VISIBLE_DEVICES=0,1
export OMP_NUM_THREADS=16
export MKL_NUM_THREADS=16
export POLARS_MAX_THREADS=16

run_fintech_python -m torch.distributed.run \
  --standalone \
  --nnodes=1 \
  --nproc-per-node=2 \
  explain_model.py \
  --config configs/markets/tw_public_lanten_market_candles_day_trade_2006plus.yaml/
  --device cuda \
  --cpu-threads 16 \
  --max-rows 0 \
  --row-chunk-size 16 \
  --amp-dtype bf16 \
  --no-compile-model \
  --ig-steps 8 \
  --ig-batch-size 2 \
  --sample-method even \
  --perturb \
  --perturb-batch-size 2 \
  --perturb-max-auto-batch-size 32 \
  --perturb-max-input-elements 536870912 \
  --counterfactual-compile \
  --j-lens \
  --j-lens-vjp-batch-size 16 \
  --shap \
  --regime-analysis \
  --fold-stability \
  --umap \
  --umap-max-points 0 \
  --umap-max-projections 0 \
  --umap-n-neighbors 16 \
  --plot-backend rapids_datashader \
  --plots \
  --standard-plots \
  --plot-theme paper \
  --report-style paper \
  --no-interactive-plots \
  --strict-no-fallback \
  --progress



# shioaji

# 查看狀態
systemctl status stockagent-shioaji-top200.service

# 即時查看 log
journalctl -u stockagent-shioaji-top200.service -f

# 重新啟動
systemctl restart stockagent-shioaji-top200.service

# 停止
systemctl stop stockagent-shioaji-top200.service

# 停止並取消開機啟動
systemctl disable --now stockagent-shioaji-top200.service