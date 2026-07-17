# stockAgent 執行指南

本指南只描述目前程式實際支援的入口與合約。模型超參數與效能數字應以
各市場 YAML、當前資料與硬體重新量測，不把歷史 benchmark 當成保證。

## 1. 載入可攜式環境

所有機器都從專案根目錄解析 runtime，不使用固定的使用者家目錄：

```bash
cd /path/to/stockAgent
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
```

`run_fintech_python` 會尋找 `fintech` Conda/Mamba 環境並把該環境的 `bin`
放到 `PATH` 前端，讓 PyTorch/Triton 使用同一套 Python、CUDA 與 `ptxas`。
非標準安裝位置可用下列任一方式覆寫：

```bash
export FINTECH_ENV_PATH=/path/to/fintech
source scripts/runtime_env.sh
# 或改用：export PYTHON_BIN=/path/to/python
```

訓練設定要求 CUDA 時，檢查失敗就應修復環境，不應靜默改用 CPU。

## 2. 管理台股官方資料層

唯一的高階入口有三種模式。三者都以 TWSE／TPEx 為第一順位，並串接
OHLCV、除權息參考價、股票／ETF 個股 parquet、交易規則、公開特徵與嚴格
稽核。自 2000-01-01 起，官方缺少的 `date + symbol` OHLCV 才允許由 Yahoo
替補；同鍵官方列永遠覆蓋 Yahoo，輸出逐列保留 `data_source`。若官方 OHLCV
存在但漲跌／除權息參考因子缺失，只借用 Yahoo 調整比率並另標
`adjustment_source`，不覆蓋官方 OHLCV。

從零重建會先寫入 immutable stage，稽核通過後才原子替換 production：

```bash
run_fintech_python downloader/download_tw_official_data.py \
  --mode rebuild \
  --stage-root artifacts/data_rebuild/tw_2000_bootstrap \
  --promote
```

`configs/markets/tw_public.yaml` 保留從 2000 年開始的原始 fold 編號，但免費
TWSE 每日行情只從 2004-02-11 起提供。預設 `--ohlcv-fallback yahoo` 會把
Yahoo 每檔資料下載到 `data_tw_public/fallback/yahoo_tw_stocks`，轉成有收據的
低優先封存，再只填官方空缺。Yahoo 不會補融資融券、法人、估值、公司事件、
交易規則或其他 `twpub_*` 公開特徵；這些仍按官方可得年代與 point-in-time
規則處理。

新機器從零開始使用上面的 `rebuild --promote` 即可。若機器已經有完整的
`data_yahoo/tw_stocks`，可避免重抓並重用現有檔案：

```bash
run_fintech_python downloader/download_tw_official_data.py \
  --mode rebuild \
  --stage-root artifacts/data_rebuild/tw_2000_bootstrap \
  --yahoo-fallback-dir data_yahoo/tw_stocks \
  --skip-yahoo-download \
  --promote
```

若要完全禁用 Yahoo，使用 `--ohlcv-fallback none`；此時 2000--2004 缺口必須
另以 `--legacy-official-ohlcv` 與 `--legacy-source-name` 提供可追溯官方舊檔。

檢查並補漏會掃描官方歷史來源的日期、schema、重複代碼及異常低筆數，並
修復 Yahoo 替補檔後重新做官方優先合併；成功確認的休市日會寫入 coverage
state：

```bash
run_fintech_python downloader/download_tw_official_data.py --mode repair
```

首次 Yahoo 回補採單工與 1.5 秒請求間隔，避免追求速度反而觸發 429。若仍被
限流，保留相同 `--stage-root` 重跑同一條命令；已成功且原子寫入的個股檔會
直接略過，只續抓未完成項目。不要刪除 stage，也不必從第一檔重來。

每日增量要求先有通過 `rebuild` 或 `repair` 的完整基線，並重抓最近七個
日曆日以接收官方更正；Yahoo 替補也會增量更新後再重新合併：

```bash
run_fintech_python downloader/download_tw_official_data.py --mode daily
```

任何日期請求失敗或仍有未解析缺口時，`download_summary.json` 的
`coverage_complete` 會是 `false`，命令以非零狀態結束，且 `rebuild` 不會
覆蓋既有 production。`downloader/run_daily_all_markets.sh` 的台股階段使用
同一個 `daily` 入口；新機器必須先執行一次 `rebuild` 或 `repair`。

下市、停止融券與強制回補公告仍採 fail-closed：請求失敗時只更新完整性
報告，不以部分結果覆蓋既有 parquet。官方公告 archive 的起始年代限制仍
會如實保留，不可用下市日期反推公告日，避免 look-ahead bias。

`data.use_tw_public_rules` 與 `data.use_tw_public_features` 是兩個不同開關：

- `use_tw_public_rules: true` 只套用執行遮罩，缺檔會直接失敗。
- `use_tw_public_features: true` 才會把 `twpub_*` 欄位加入模型輸入。
- 官方 `櫃轉市` 與下一交易日同代號續掛不屬於 terminal exit；官方每日
  成交資料的多年空窗也不等於停牌。這兩者都已在 panel 規則層明確區分。
- `downloader/run_daily_all_markets.sh` 預設會依序更新規則並重建特徵；可用
  `RUN_TW_SHORT_RESTRICTIONS=0` 或 `RUN_TW_PUBLIC_FEATURES=0` 個別停用。

## 3. 啟動訓練

單 GPU／單程序：

```bash
source scripts/runtime_env.sh
run_fintech_python train.py --config configs/markets/tw.yaml
```

臨時覆寫輸出位置、fold 與 epoch：

```bash
run_fintech_python train.py \
  --config configs/markets/tw.yaml \
  --output-dir artifacts/tw_probe \
  --start-fold 1 \
  --max-folds 1 \
  --epochs 2 \
  --profile-timing
```

多 GPU 使用 torchrun DDP，一張 GPU 對應一個程序：

```bash
run_fintech_python -m torch.distributed.run \
  --standalone \
  --nproc_per_node=2 \
  train.py \
  --config configs/markets/tw.yaml \
  --multi-gpu-strategy distributed_data_parallel
```

DDP 目前支援不需要模型 auxiliary tensors 的 canonical return-series
objectives；若 objective 需要 aux，請使用單程序路徑。手動指定的全域
`batch_size_train` 必須可被 world size 整除。

## 4. 刻意保留的 walk-forward 語義

以下不是待修 bug，請勿為了讓日期看起來整齊而改掉：

- `walk_forward.require_future_test_year: false` 會增加最後一個實驗 fold，
  並刻意讓該 fold 的 validation 與 test 使用同一期間。它適合最新年度
  實驗，不是無偏的模型選擇結果。
- 每個 split 的 lookback 必須完整落在該 split 裡。`lookback: 32` 因此
  刻意丟掉每個 split 的前 31 個交易日，第一筆樣本是第 32 個交易日。
- stitched deployment test 會把下一個模型尚未累積完 lookback 的 warmup
  日期留給上一個模型，避免在跨 fold 曲線產生日期缺口。

## 5. 目前 executor 邊界

訓練路徑只有下列演算法邊界：

- 神經網路單程序：一個 lazy `WindowedSplitTensors` executor。
- 神經網路 DDP：每個 rank 使用同一份 lazy-window 語義並同步 canonical
  return-series loss/backtest state。
- LightGBM/XGBoost：獨立的 CPU materialized fit/evaluation route。

神經網路 executor 內，連續且固定形狀的 batch 優先使用
`forward_from_panel_slab`；不連續、需要 auxiliary outputs，或模型不支援
slab 時才在同一 executor 內 materialize window。兩者共用同一個
`risk_aware_loss`、`run_backtest_torch`、費率、交易遮罩與跨 chunk state，
不是兩套損益公式。固定 batch padding 由 `sample_mask` 排除，不應改變 loss。

`torch.compile` 可花超過十分鐘建立目前形狀的圖；是否值得應比較第二個
epoch 之後的穩態時間。改 batch、symbol 數或 chunk shape 可能觸發新圖。

## 6. 交易規則與槓桿語義

- `can_sell_mask`：是否能減少既有多頭。
- `can_short_open_mask`：是否能新增或增加借券空頭。
- 兩者不能合併；禁止放空不等於禁止賣出自己持有的股票。
- 一般停牌、缺值、零成交量與漲跌停單側限制會凍結受影響部位。
- 只有明確的官方永久退出事件才設 `force_exit_mask`，並依平多或回補空頭
  計入相應的賣出或買進手續費。
- 官方回補期限由 `force_short_cover_mask` 表示，和永久退出是不同事件。

Canonical train/validation/test tensor backtest 與整股 audit 的 gross exposure
固定為 `1.0`。`trading.reporting_leverage` 只是報表後處理倍率，只影響
`leverage_*` 圖；程式會在縮放後重新計算 turnover 與買賣費用。它不會
回頭改變 checkpoint 選擇、canonical metrics 或模型梯度，但仍屬於完整
設定快照／稽核指紋。它不屬於語義 resume gate，因此可用同一模型重產不同
倍率報表；報表必須保留所用倍率，不能把它誤寫成訓練曝險。

## 7. Checkpoint、恢復與輸出

`--resume`（或 YAML 的 `runner.resume`）會讀取 checkpoint；`--no-resume`
可明確關閉。恢復前會驗證資料內容、執行遮罩、前處理、設定與 fold 語義
指紋，避免拿不相容的 checkpoint 繼續訓練。

Schema 4 checkpoint 同時保存完整設定快照／指紋供稽核，以及分層的
data、model、training、evaluation、trading、walk-forward 語義指紋供恢復
判定。輸出路徑、cache 位置、compile/chunk/VRAM 等機器本地執行選項不會
阻止跨電腦恢復；實際模型、資料、費率、fold 或有效 global batch 改變仍會
拒絕。DDP checkpoint 會保存每個 rank 的 RNG；增加 rank 時產生不同且穩定
的衍生 stream，不複製既有 rank 的隨機序列。

Canonical backtest contract 目前是 version 2：成交後持倉會依資產報酬與淨
費用做 mark-to-market，並攜帶 absorbing `alive` 狀態跨 batch/chunk。舊的
schema 4（version 1／未記錄版本）checkpoint 仍可載入模型做 inference，
但不能 resume optimizer，避免在不同報酬與 turnover 數學下靜默續訓。

主要輸出如下；實際表格格式由 YAML 決定：

```text
<output_dir>/
├── summary.json
├── fold_01/
│   ├── checkpoint_best.pt
│   ├── checkpoint_last.pt
│   ├── metrics.json
│   ├── fold_complete.json
│   ├── test_backtest.npz
│   ├── annual_report.txt
│   ├── equity_curve.png
│   └── leverage_equity_curve.png
└── train_<years>/
    └── epoch_curve.jsonl
```

Tree models 另有 `fold_XX/model.pt`。大型 daily weights／holdings 表是否寫出
以及 CSV/Parquet 格式，由 `training.save_*_table` 與
`training.table_output_format` 控制。

## 8. 監控與除錯

先找出實際 group curve，再追蹤它；不要假設存在 `training.log`：

```bash
find artifacts -path '*/train_*/epoch_curve.jsonl' -print
tail -f artifacts/<experiment>/train_<years>/epoch_curve.jsonl
```

效能調整以完整 epoch wall time 為準，並同時檢查 train、validation、test
curve、plot、checkpoint 與 reporting。常見處理：

- OOM：先降低 `batch_size_train`、`batch_size_eval` 或 eval chunk cap；只有
  設定 `training.auto_batch_size: true` 時才會自動搜尋批次。
- compile 失敗：先看環境檢查中的 `ptxas`、編譯器與 CUDA roots；
  `strict_no_fallback: true` 會刻意拒絕靜默切回 eager。
- 找不到資料：核對 YAML 的 `data.parquet_root`；TW rules 開啟時也要核對
  `data.tw_public_feature_path` 並先完成第 2 節的 rebuild。
- checkpoint 拒絕恢復：閱讀 mismatch 訊息；不要繞過資料／設定指紋，應
  使用相容資料設定或另開輸出目錄重新訓練。

## 9. 驗證

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python -m py_compile \
  stockagent/config.py \
  stockagent/training/trainer.py \
  stockagent/training/loss.py \
  stockagent/backtest/simulator.py
run_fintech_python -m pytest -q -s test
```

正式測試應限定在維護中的 `test/` 目錄。涉及 compile 或 DDP 的變更，還
需要以實際 GPU、固定 shape 與至少第二個 epoch 的數據驗證。
