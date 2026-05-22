# stockAgent

台股量化研究與訓練工作區，包含資料下載、特徵 parquet 管理、walk-forward 訓練與回測輸出。

## 專案重點

- 主要訓練設定在 [configs/experiment_baseline.yaml](configs/experiment_baseline.yaml)
- 訓練入口在 [train.py](train.py)
- Yahoo Finance 資料下載工具在 [download_yahoo_tw_ohlcv.py](download_yahoo_tw_ohlcv.py)
- 套件清單在 [requirements.txt](requirements.txt)
- 訓練規格文件在 [docs/training_spec.md](docs/training_spec.md)

## 環境準備

1. 建議使用 conda/mamba 環境 `fintech`
2. 安裝依賴：

```bash
pip install -r requirements.txt
```

3. 如需 GPU 訓練，請確認 `torch.cuda.is_available()` 為 `True`

## 1) 抓取 Yahoo Finance 台股資料

最基本下載（2000-01-01 到今天，寫入 `data_parquet`）：

```bash
python download_yahoo_tw_ohlcv.py --output-dir data_parquet
```

常用參數範例：

```bash
# 調整平行下載數
python download_yahoo_tw_ohlcv.py --output-dir data_parquet --workers 16

# 只抓少量股票驗證流程
python download_yahoo_tw_ohlcv.py --output-dir data_parquet --symbols 2330 2317 0050

# 強制重新下載
python download_yahoo_tw_ohlcv.py --output-dir data_parquet --refresh

# 只做缺漏日期補齊
python download_yahoo_tw_ohlcv.py --output-dir data_parquet --fill-only
```

資料輸出格式：

- 每檔一個 parquet，例如 `2330_features.parquet`
- 欄位：`date`, `open`, `max`, `min`, `close`, `adjclose`, `Trading_Volume`
- 預設輸出報表：`download_report.csv`, `download_summary.json`, `fill_report.csv`

## 2) 啟動訓練 (train)

最基本訓練：

```bash
python train.py --config configs/experiment_baseline.yaml --output-dir artifacts
```

不從既有 checkpoint 續訓：

```bash
python train.py --config configs/experiment_baseline.yaml --output-dir artifacts --no-resume
```

也可以用專案腳本啟動：

```bash
./coda_runner.sh
```

注意事項：

- `configs/experiment_baseline.yaml` 目前預設 `environment.device: cuda`
- 若目前資料都在 `data_parquet/`，請確認 `configs/experiment_baseline.yaml` 的 `data.parquet_root` 設為 `data_parquet`
- 若機器沒有可用 CUDA，請先改設定或換到有 GPU 的環境
- 訓練完成後會在 `artifacts/` 產生各 fold 結果與 `summary.json`

## 3) 建議執行流程

```bash
# 1. 抓資料
python download_yahoo_tw_ohlcv.py --output-dir data_parquet --workers 16

# 2. 開始訓練
python train.py --config configs/experiment_baseline.yaml --output-dir artifacts --no-resume
```

## 4) 常見輸出位置

- 資料 parquet: `data_parquet/`
- 訓練輸出: `artifacts/fold_XX/`
- 訓練彙總: `artifacts/summary.json`
