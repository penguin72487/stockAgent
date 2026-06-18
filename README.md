# stockAgent

Multi-asset Taiwan stock trading research workspace.

## Current status

- Raw research data lives under `data_parquet/`.
- Each parquet file represents one stock symbol, for example `2330_features.parquet`.
- The current training and validation specification is documented in `docs/training_spec.md`.
- A concrete experiment template is provided in `configs/experiment_baseline.yaml`.

## Planned workflow

1. Normalize all symbol parquet files into a shared date x symbol panel.
2. Derive the baseline from the same-day average return of all tradable stocks.
3. Run yearly expanding-window walk-forward validation.
4. Train GPU-enabled baseline models first, then portfolio and RL policies.

## Training

- Install dependencies from `requirements.txt` inside the `fintech` environment.
- Run training with `python train.py --config configs/experiment_baseline.yaml --output-dir artifacts`.
- Or use the project runner: `./coda_runner.sh`.
- Runner defaults are centralized in `configs/runner.env`.
- Outputs include one folder per walk-forward fold and a top-level `summary.json`.

## Yahoo Finance Multi-Asset Download

- Run `python download_yahoo_ohlcv.py` to download four separate folders under `data_yahoo/`: `tw_stocks/`, `us_stocks/`, `crypto/`, and `forex/`.
- The downloader defaults to `2000-01-01` through today.
- Symbol downloads are parallelized with `--workers` (within each asset); when using `--asset all`, you can also parallelize assets via `--asset-workers`.
- Taiwan symbols are loaded from `data_parquet/symbols.csv` when available; otherwise they are fetched from TWSE ISIN listed (`strMode=2`) and OTC (`strMode=4`) lists.
- Taiwan delisted candidates are also included by default (`--include-tw-delisted`) and attempted with `.TW` / `.TWO` style Yahoo tickers.
- U.S. symbols are loaded from Nasdaq Trader symbol directories (`nasdaqlisted.txt` and `otherlisted.txt`) with static fallback.
- U.S. delisted symbols can be included from Alpha Vantage `LISTING_STATUS` when `ALPHAVANTAGE_API_KEY` (or `--alpha-vantage-api-key`) is provided.
- Crypto symbols are loaded from CoinGecko `/coins/list` and mapped to Yahoo format `${SYMBOL}-USD`.
- Forex symbols are loaded from Yahoo Finance currencies page tickers (with static fallback when rate-limited).
- Pepperstone-style FX universe is available via `configs/forex_pepperstone_pairs.txt` and can be downloaded to `data_yahoo/forex_pepperstone/`.
- Use `python downloader/download_yahoo_ohlcv.py --asset tw_stocks` to download only Taiwan stocks.
- Use `python downloader/download_yahoo_ohlcv.py --asset us_stocks` to download only the expanded U.S. stock universe.
- Use `python downloader/download_yahoo_ohlcv.py --asset crypto` to download only the expanded crypto universe.
- Yahoo crypto uses 15-minute bars; existing crypto parquet files that look like old daily data are rebuilt from the 15-minute source instead of being merged.
- Use `python downloader/download_yahoo_ohlcv.py --asset forex` to download only the expanded FX universe.
- Use `python downloader/download_forex_pepperstone.py` to download the Pepperstone-style FX universe.
- Use `python downloader/download_forex_pepperstone.py --mode repair` to repair stale/missing Pepperstone forex files.
- Use `python downloader/download_forex_pepperstone.py --mode daily-update` for daily incremental updates.
- Use `python downloader/download_pepperstone.py` to download grouped Pepperstone-style data to `data_peperstone/24hTrading`, `data_peperstone/commodites`, `data_peperstone/crypto`, and `data_peperstone/fores`.
- Use `python downloader/download_pepperstone.py --groups crypto fores` to download only selected groups.
- Use `python downloader/download_pepperstone.py --mode daily-update --groups all` for daily incremental updates across groups.
- Use `python downloader/download_okx_perp_daily.py --output-dir data_okx` to download all OKX perpetual swap 15-minute bars.
- Use `python downloader/download_okx_perp_daily.py --start-date 2020-01-01 --workers 6` to control download range and parallelism.
- Use `python downloader/download_okx_perp_daily.py --mode daily-update` for incremental updates (only missing candles).
- Use `python downloader/download_okx_perp_daily.py --mode full --refresh` when you need a full re-download.
- Use `python downloader/download_bybit_perp_daily.py --output-dir data_bybit` to download Bybit perpetual 15-minute bars.
- Use `python downloader/download_bybit_perp_daily.py --categories linear inverse --start-date 2020-01-01 --workers 6` to control Bybit categories, range, and parallelism.
- Use `python downloader/download_bybit_perp_daily.py --mode daily-update` for incremental updates (only missing candles).
- Use `python downloader/download_forex_frankfurter.py --mode daily-update --output-dir data_yahoo/forex` for daily incremental FX updates from Frankfurter.
- Each asset folder includes `symbols.csv`, `download_report.csv`, and `download_summary.json` alongside `*_features.parquet` files.
- Parquet output includes at least `date`, `open`, `max`, `min`, `close`, `adjclose`, `Trading_Volume`, and also preserves extra Yahoo columns when available (for example `Dividends`, `Stock Splits`).
- Override the default universe with `--symbols` or `--symbols-file`, for example `python download_yahoo_ohlcv.py --asset forex --symbols EURUSD GBPUSD USDJPY`.
- Use `python download_yahoo_ohlcv.py --mode daily-update --asset all` for daily incremental updates across Yahoo assets.

### Repair Mode

- Use `python download_yahoo_ohlcv.py --mode repair --asset all` to check all assets and repair missing/stale parquet files toward today.
- Repair mode checks each symbol file for existence, latest date, and required schema columns (`date/open/max/min/close/adjclose`); missing/broken/stale/schema-mismatch symbols are repaired automatically.
- Repair outputs include top-level `repair_summary.json` and per-asset `repair_report.csv`.
- Adjust overlap with `--repair-overlap-days` (default `7`) to re-fetch a small trailing window before the local last date.
- If Yahoo returns `possibly delisted; no timezone found`, that ticker is automatically appended to per-asset `yahoo_blacklist.txt` and skipped in later runs.
- Successfully downloaded Yahoo tickers are persisted into per-asset `yahoo_whitelist.txt`.

### Daily All-Market Update

- Use `bash downloader/run_daily_all_markets.sh` to run daily updates across all configured markets.
- The script runs Yahoo all-asset daily-update (`tw_stocks`, `us_stocks`, `crypto`, `forex`), Frankfurter forex incremental update to `data_yahoo/forex`, Pepperstone grouped daily-update, and OKX/Bybit perpetual 15-minute incremental updates.
- Set `RUN_PEPPERSTONE_GROUPS=0` to skip Pepperstone groups when you only want Yahoo+Frankfurter.
- Set `RUN_CEX_PERP=0` to skip OKX/Bybit updates.
- Set `WORKERS`, `ASSET_WORKERS`, `PEPPERSTONE_WORKERS`, `OKX_WORKERS`, `BYBIT_WORKERS`, and `REPAIR_OVERLAP_DAYS` via environment variables to tune speed.

## Environment

- Conda or mamba environment: `fintech`
- Training target: CUDA with Tensor Core acceleration
- Recommended activation command: `mamba activate fintech`
- Shell runners auto-detect the active `fintech` conda/mamba environment across common WSL install paths; override with `PYTHON_BIN=/path/to/python` if needed.
mamba env export -n fintech --no-builds > fintech_environment.yml
mamba env update -n fintech -f fintech_environment.yml

mkdir -p "$CONDA_PREFIX/conda-meta"
nano "$CONDA_PREFIX/conda-meta/pinned"
