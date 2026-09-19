# vastai1T v8 當沖訓練就緒與測速紀錄（2026-09-17）

## 已驗證

- 遠端工作樹：`/root/stockAgent`；正式設定：
  `configs/deployments/tw_day_trade_last_last_only_training_vastai1t_v8_score_entmax_cash.yaml`。
- `tw-public` 固定在
  `tw-public-20260917T022610857716059Z-l0-penguin-7e9a08bb7548fd88`。
  vastai1T 已驗證 2,265 個 packed payload（17,026,004,457 bytes）、
  materialized `READY` 與 exact-release current link；Syncthing 完成後
  `needBytes=0`、`needItems=0`、`pullErrors=0`。此 release 由 Syncthing 傳送，
  SSH 僅用於遠端管理及測試命令。
- `scripts/check_environment.py --require-cuda --strict` 與 v8 的
  `train.py --start-fold 11 --max-folds 1 --check-data-only` 均退出 0。
  環境為兩張 RTX 5090、PyTorch 2.11.0+cu128；資料為 3,099 個交易日、
  2,755 檔股票、99 個模型特徵。資料檢查記錄
  `source_gap_symbol_days=0`；406 個未解事件仍按既有合約阻擋其受影響的前綴，
  不是宣稱所有歷史事件都有完整證據。
- 兩卡 DDP、BF16、正式 global batch 32 的 fold 11 四輪短程實測已完成
  train、validation、final test、checkpoint、圖表與 cumulative report。
  模型 panel-slab forward 的 compile probe 通過；physical FIFO loss 的
  exact eager probe 通過，但 loss 本身顯示 `loss_compile=off`，不能稱其已編譯。

## 效能與等價性

| 測試 | epoch 3 雙卡較慢 rank | epoch 4 雙卡較慢 rank | 2026 測試累計報酬 |
| --- | ---: | ---: | ---: |
| 首次 CPU 112 | 76.110 s | 75.369 s | 33.54% |
| CPU 48 | 71.671 s | 72.725 s | 36.77% |
| CPU 112 重跑 | 75.769 s | 76.065 s | 36.77% |
| CPU 112、隔離冷編譯 | 尚未用於穩態比較 | 尚未用於穩態比較 | 34.40% |

CPU 48 與 CPU 112 重跑的四輪 train/validation loss、最佳 checkpoint
的每個模型 tensor、`test_backtest.npz` 每個陣列及 `test_metrics` 完全相同；
第 3–4 輪較慢 rank 的時間中位數降低約 4.9%。三次的設定與資料指紋相同。
但首次 CPU 112 結果不同，故**尚未證明跨次啟動完全決定性**，也不能把
33.54% 與 36.77% 差異歸因於 CPU 執行緒數。正式設定目前繼承
`deterministic_algorithms: false`，且 `cudnn_benchmark: true`；首次差異的
具體根因尚未確認。首次測試的 `dataset_and_batch_size` 階段為 59.708 秒，
且編譯探測有 Inductor cache miss；後兩次相同階段約 0.31 秒、編譯探測
皆為 cache hit。另以新的 `STOCKAGENT_COMPILE_CACHE_NAMESPACE` 保留資料
變換熱快取、單獨重做冷編譯，完整四輪也退出 0，但 epoch 0 來源驗證 loss
已從原本的 0.0161925 變成 0.0161115，epoch 4 驗證 loss 為 -0.3138061
（熱編譯對照 -0.3053766）。這證明冷編譯隔離條件下不能重現同一數值軌跡，
但尚未證明是編譯器 codegen、autotune、非決定性 GPU 算子，還是其他與
冷編譯同時變動的狀態；需要固定輸入的 compiled/eager 前向與梯度對照。
四輪是整個 fold 生命週期的短程測試，不是正式訓練績效。

## 遠端正式啟動命令

目前沒有正式 fold checkpoint，也沒有執行中的 `train.py`。已把雙卡
完整 fold 驗證過的 `environment.cpu_threads: 48` 寫入遠端 v8 主設定檔；
產物仍寫到設定檔指定的遠端 `artifacts/markets` 目錄：

```bash
cd /root/stockAgent
source scripts/runtime_env.sh
run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_last_last_only_training_vastai1t_v8_score_entmax_cash.yaml \
  --start-fold 11 --max-folds 1 --profile-timing
```

啟動前仍應確認 exact release 的 `READY`、兩張 GPU 與沒有其他訓練佔用；
啟動後應核對兩個 DDP rank、各自 CUDA 裝置與前幾輪 loss。這個命令
可執行，不代表不同編譯快取環境會產生完全相同的 checkpoint；若要求
位元級重現，應先完成上述數值差異修復與冷／熱雙卡回歸。

## 主訓練第二輪效能審核

2026-09-17 又完成下列同一個 pinned release、fold 11、四輪、BF16、
global batch 32、兩卡 DDP 的端到端測試。表內是第 3/4 輪雙卡較慢 rank；
第 1 輪有編譯／資料冷啟，不代表穩態。四輪短測不代表正式收斂或可用測試績效。

| 測試條件（僅同組 A/B 可作單因素歸因） | 第 3 輪 | 第 4 輪 | 逐項產物對照 |
| --- | ---: | ---: | --- |
| 主設定 CPU 48、原始 dense FIFO、直接 DDP | 85.299 s | 76.957 s | checkpoint、test、deployment、metrics 與隔離式 CPU 48 完全一致 |
| CPU 24、原始 dense FIFO | 69.268 s | 74.378 s | checkpoint/test/metrics 相同，但 deployment 171 格差 2.22e-16；不升級 |
| CPU 48、關閉 cuDNN benchmark A/B | 83.707/88.166 s | 80.621/83.067 s | A/B 的 checkpoint/test/deployment/metrics 完全一致，且等同原設定的熱快取 CPU 48；速度較慢 |
| CPU 48、強制決定性 A/B | 93.041/112.045 s | 93.267/92.853 s | A/B 完全一致，但與主設定不同，且反向傳播更慢 |
| CPU 48、真正 sparse、關閉 cuDNN benchmark | 74.729 s | 75.825 s | checkpoint/test/metrics 均不同；不得作無損替換 |
| CPU 48、真正 sparse、強制決定性 | 89.297 s | 89.639 s | 與決定性 dense checkpoint/test/metrics 仍不同；證實不只是一般 GPU 非決定性 |

`STOCKAGENT_DAY_TRADE_SPARSE_EVENTS=1` 單獨放在 shell **不能**啟用
sparse：訓練設定載入會覆寫此環境變數。真正的 sparse 必須寫在
`training.day_trade_sparse_events: true`；兩個 sparse 候選設定已留在
`configs/deployments/*v8*sparse_profile_candidate.yaml`，只供使用者後續
獨立消融。舊的 env-only 測試不計入 sparse 速度或績效證據。

主訓練沒有切換 sparse、關閉嚴格 FIFO、改 batch/optimizer cadence、降低
50% 分鐘容量或跳過最後的 test/report。遠端主 v8 設定只新增 CPU 48。
`--no-isolate-train-folds` 確實省略父行程重複的約 20 秒 panel 載入，
但完整 fold 一次 A/B 並未快過隔離式訓練，故也不改預設。原始
physical FIFO loss 的外層依舊是 exact eager；模型固定形狀 hot path
已通過 compile probe，不能把整輪 I/O、回測、繪圖稱為 compiled。

主設定修改後重新通過 `scripts/check_environment.py --require-cuda --strict`
和 `train.py --start-fold 11 --max-folds 1 --check-data-only`（兩者退出 0）；
輸出確認 `torch_threads=48`、兩張 RTX 5090、同一個 physical source release、
`source_gap_symbol_days=0`，而 `--check-data-only` 不會寫出 checkpoint。
沒有啟動正式訓練，也沒有刪除任何候選或舊產物。遠端當下磁碟剩餘約
138 GiB；正式長訓前應再核對容量與既有產物保留政策。

### 效能下界與後續消融界線

穩態一輪約 83 個 batch；可歸因的主要耗時是 FIFO loss 約 32–38 秒、
backward 約 27–31 秒、資料取得約 5–8 秒、validation/backtest 約 5–7 秒，
model forward 僅約 1–3 秒。這是已量到的當前路徑成本，不是演算法
的數學極限。增加 prefetch/cache 只能處理資料取得部分，不能把 60 秒
以上的 loss/backward 消掉；改稀疏算法又已證實會改變訓練結果。

使用者決定自行跑消融；本輪停止新增候選 fold。後續要宣稱候選優於
基線，至少應固定同一 release、fold 與 seed、完整正式訓練與
walk-forward 的非重疊測試所有權，分開比較訓練耗時、validation 選模、
交易成本後報酬／風險，不能拿本表四輪 fold 11 的 2026 報酬挑模型。
