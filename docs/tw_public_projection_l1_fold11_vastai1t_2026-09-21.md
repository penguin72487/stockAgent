# 台股 428 欄窗口 RMS / `projection_l1`：vastai1T fold11

## 目標與資料邊界

此輪遵照指定的 `projection_l1` 輸出，在兩張 RTX 5090 上測量完整 fold epoch，然後啟動 fold11。資料是「有觀測值就納入」的 2014 v3 **研究資料**：9,617,311 筆外部特徵紀錄；204 個公開數值、20 個股票數值與 204 個公開欄位可用旗標，共 428 個模型通道。面板 3,103 個交易日、2,755 檔股票。現行來源允許現在版本的歷史值、推測公告日期及起始較晚的欄位；`historical_vintage_verified=false`。模型內 32 日窗口尺度的時間因果性，不會補齊來源當年原始版本或確切發布時間，因此此訓練結果不能當作已證實的 2014 年 PIT 策略績效。

使用經過完整物件驗證後 materialize 的確切冷釋出，不在訓練中重新解析 `latest`：

| 資料集 | 固定 snapshot | 用途 |
|---|---|---|
| `tw-public` | `tw-public-20260921T040006535230961Z-l0-penguin-4ab6cb1612e4862c` | 股票 Parquet、正式除權息來源 |
| `tw-public-research-all-observed-2014-v3` | `tw-public-research-all-observed-2014-v3-20260921T041303541818804Z-l0-penguin-af32bae76010f47e` | 428 通道外部研究欄位與上游收據 |

兩個 cold release 在 vastai1T 分別通過 2,659 / 7 個物件完整性驗證；節點使用 index-only cold store，實際訓練讀取固定 materialized release。v3 Parquet SHA-256 為 `5427373e369db04524fca6b2da31d630c256191ac33cfb6af5c38c4824a60884`。

## 輸入與投影

每筆決策只看當時可見的 32 個交易日。對每檔、每欄以該窗口的觀測值計算無中心 RMS，缺值維持零，`__available` 旗標維持二元，然後將縮放後窗口送進普通輸入及 Haar / DCT 多基底特徵。來源的發布時鐘另依資料收據審核。詳細公式與 ABI 見 [32 日窗口說明](tw_window_rms_causal_speed_2026-09-20.md)。

大截面下，未按候選數縮放的原始 score 在投影到單位 L1 球時會集中在少數股票：先前 8 epoch 探針在 epoch5 發生零梯度。因此此輪仍用 **`portfolio_output_mode: projection_l1`**，啟用 `projection_l1_scale_by_active_count: true`；先讓每個有效 score 乘當筆有效候選數，再做同一個投影。這改變了 score 的尺度及 checkpoint ABI，不能與舊的未縮放 projection checkpoint 混用。持續檢查每個 epoch 的零梯度計數、回退、非有限值與投影後權重；縮放本身並不保證策略收益。

## 正式路徑的吞吐測量

對同一 fold11 使用 BF16、兩張 GPU、DDP、同一模型與資料，在每個獨立測試目錄跑 8 個完整 epoch，排除編譯暖機的 epoch1、2；包含正式 train、val、抽樣 test 曲線及寫檔。資料預檢冷建表約 314 秒（symbols 32.7、external 162.8、assemble 79.3、cache save 39.0 秒）；之後使用面板快取的載入約 2.3 秒。以下以 2,656 個 train rows ÷ epoch3+ 完整耗時中位數計算真實 rows/s；顯存採兩卡最差峰值，放行門檻為每卡至少 3 GiB 可用且最高使用量不超過 90%。

| 全域 batch / 每卡 | epoch 中位秒 | rows/s | 最低顯存餘裕 | 結論 |
|---|---:|---:|---:|---|
| 32 / 16 | 11.934 | 222.5 | 17.79 GiB | 通過 |
| 64 / 32 | 11.302 | 235.0 | 18.57 GiB | 通過 |
| 124 / 62 | 7.977 | 332.9 | 3.44 GiB | 在測過的安全配置中最快 |
| 128 / 64 | 約 6.4–8.0 | 不放行 | 2.25 GiB | 最高使用 92.94%，未通過顯存門檻 |

已放行配置的 epoch3–8 梯度均未歸零，沒有新增編譯圖或後備執行；訓練、驗證與測試值均為有限值。batch 改動同時影響 optimizer 更新路徑，因此上表僅比較吞吐和機器資源，並非同一訓練軌跡的指標 parity，也不證明模型品質變好。詳細逐次測量已核對複製到遠端主專案的 `artifacts/benchmarks/tw_public_window_rms_projection_l1_scaled_*20260921/batch_*/result.json`。

## 啟動、監督及驗收

- [固定部署配置](../configs/deployments/tw_public_all_observed_window_rms_projection_l1_fold11_v1.yaml) 繼承完整 1,000 epoch、lookback32 設定，指定 global batch124、eval16、`projection_l1`、雙 GPU DDP 與獨立產物根目錄。
- [啟動腳本](../scripts/run_tw_public_window_rms_fold11_vastai1t.sh) 先通過 CUDA 嚴格檢查、資料存在與設定斷言、fold11 `--check-data-only`，再以 `--resume --start-fold 11 --max-folds 1` 啟動正式訓練；`flock` 防止重複啟動。
- [Supervisor 配置](../deploy/supervisor/stockagent-tw-public-window-rms-fold11.conf) 位於 vastai1T 的 `/etc/supervisor/conf.d/`，程序名稱 `stockagent_tw_public_window_rms_fold11`；異常結束時保留失敗狀態與日誌，不自動重跑研究任務。

vastai1T 後續操作固定在 `/root/stockAgent`。batch124 已完成產物已逐檔雜湊核對後複製至主專案的 `artifacts/markets/tw_public_all_observed_window_rms_projection_l1_fold11_v1/`，原暫用 worktree 的副本仍保留。面板快取移入主專案的 `artifacts/cache/tw_public_all_observed_window_rms_projection_l1_v1/`。曲線在該 run 的 `train_2014-2015-2016-2017-2018-2019-2020-2021-2022-2023-2024/epoch_curve.jsonl`，持續檢查 `train_zero_grad_batches`、`dynamo_unique_graphs_epoch_delta`、`epoch_total_s` 與驗證指標。

batch124 的 fold11 已於 epoch174 早停並完成正式評估，`fold_11/fold_complete.json` 顯示 `status=complete`、2026 年完整測試 172 列。測試累積報酬 -21.33%，相對同期間基準的 excess -77.91%；這是研究資料下的觀察，不代表可交易績效或 batch128 的結果。

## batch128：使用者自行啟動

2026-09-21 依使用者指定建立 [batch128 獨立配置](../configs/deployments/tw_public_all_observed_window_rms_projection_l1_fold11_batch128_v2.yaml)。它繼承同一資料、模型、`projection_l1` 與 32 日窗口，但在獨立產物目錄從頭訓練，不從 batch124 的 optimizer checkpoint 續跑。batch128 的 8 epoch 探針曾達兩卡最差 92.94% 顯存使用率、最低餘裕 2.25 GiB；若正式跑出 CUDA OOM，保留失敗日誌及產物，不把資料或模型版本混在 batch124 run 內。

在遠端主專案以前景模式自己啟動；標準輸出與錯誤即時顯示於目前終端。這是既有 `run_fintech_python train.py` 形式，無須透過 Supervisor：

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
export CUDA_VISIBLE_DEVICES=0,1 PYTHONUNBUFFERED=1 STOCKAGENT_STRICT_NO_FALLBACK=1
export PYTORCH_ALLOC_CONF=expandable_segments:True STOCKAGENT_POLARS_THREADS=1 POLARS_MAX_THREADS=1 RAYON_NUM_THREADS=1
run_fintech_python train.py \
  --config configs/deployments/tw_public_all_observed_window_rms_projection_l1_fold11_batch128_v2.yaml \
  --output-dir artifacts/markets/tw_public_all_observed_window_rms_projection_l1_fold11_batch128_v2 \
  --start-fold 11 --max-folds 1 \
  --multi-gpu-strategy distributed_data_parallel \
  --batch-size-train 128 --batch-size-eval 16 \
  --cpu-threads 112 --torch-compile-threads 16 \
  --no-isolate-train-folds --resume
```

batch128 的 Supervisor 工作雖已登錄，目前仍是 `STOPPED / Not started`；上述指令不透過它。啟動後請保持終端連線，避免前景工作因連線中斷而結束。`--resume` 只會續接 batch128 自己的產物目錄，不會讀取 batch124 checkpoint。

這個 Vast instance 的 `workspace_is_volume=false`，目前 container 上的產物在 recycle / destroy 時會消失。fold 完成後必須依專案的 completed-artifact 冷儲存合約、hash、收據、對端收斂與驗證搬離此機，才能宣告耐久保存。這次未將活躍中的 checkpoint 誤當作已完成且可發布的 artifact。
