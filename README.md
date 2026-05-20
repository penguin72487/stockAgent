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

## Yahoo Finance 台股股票與 ETF OHLCV 下載

- 安裝依賴後可用 `python download_yahoo_tw_ohlcv.py --output-dir data_yahoo_tw_ohlcv` 另存一份 Yahoo Finance 台股資料。
- 腳本會從 TWSE/OTC 清單整理四位數代碼（股票 + ETF），轉成 Yahoo 的 `.TW` / `.TWO` 代碼後平行下載 2000-01-01 到今天的 OHLCV。
- 每檔股票輸出成一個 parquet 檔，例如 `2330_features.parquet`，欄位為 `date/open/max/min/close/Trading_Volume`。
- 可用 `--workers 8` 控制平行度，或用 `--symbols 2330 6488` 先做小範圍驗證。

## Environment

- Conda or mamba environment: `fintech`
- Training target: CUDA with Tensor Core acceleration
- Recommended activation command: `mamba activate fintech`
mamba env export -n fintech --no-builds > fintech_environment.yml
