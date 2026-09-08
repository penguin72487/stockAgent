# vastai1T 個股期貨歷史訓練準備與效能驗收

本次修正使用者在容器 `45963859` 執行一般 `train.py` 時找不到 historical YAML 的問題。
程式以遠端較新的 Git 版本為基礎合併，保留遠端既有 crypto 訓練改動；沒有覆蓋整個工作樹。
本文件記錄 2026-09-08 的實測，硬體與資料狀態日後須重新核對。

使用者後續已將準備範圍改為永豐歷史可用期間。一般 historical YAML 現從
2020-03-23 起建立面板，年度 fold 起年為 2020，採用新的分鐘資料與訓練產物目錄。
最新交付狀態見 [2020/03/23 起資料準備](futures_from_20200323_preparation.md)；
本頁的 2014–2019 效能數字及較早交付 receipt 仍保留為當次測量證據。

## 資料交付

歷史設定為 `configs/markets/tw_stock_futures_day_trade_0845_historical.yaml`。
股票特徵固定使用設定繼承的 `tw-public-20260906T151104843336596Z-l0-penguin-246aab6e5c72e427`，
避免遠端較舊的 `data_tw_public` 捷徑混入資料。

| 資料 | 遠端位置 | 驗證 |
|---|---|---|
| 配對日線、模型特徵、商品主檔、逐商品檔 | `artifacts/data_preparation/futures_daily_70a57dd76de74fd0a365/` | 1,940 個檔案，350,580,327 bytes，逐檔 SHA 一致 |
| 全日分鐘、執行事件分鐘、coverage、gaps、manifest | `data_tw_futures/taifex_stock_futures_minute_history_v2/` | 5 個檔案 SHA 一致，日線來源 SHA 相符 |
| 逐商品、逐年及未解合約日清單 | `artifacts/operations/vastai1t_futures_historical_20260908/` | 隨準備資料一併交付 |

分鐘 manifest SHA 為 `146e419ed5eb9e5aaaa5bd64a9add700b55a267d873fd63211c461b9e0c80e5c`；
其日線來源 SHA 為 `70a57dd76de74fd0a3652b1ecf932ccaf32320d01c252bf7b608a30b3f598df5`。
舊的已發布期貨快照雖有同名日線，其 SHA 不同，因此保留新日線的獨立版本並核對配對關係。

這是使用者明確要求送到指定機器的**私人準備快照**，保留 `partial` 與全部缺口；
沒有繞過 catalog 把它發布為完整 cold release，也沒有建立正式訓練 `READY` 宣告。
遠端 `--check-data-only` 已實際讀取資料，仍因 **37,723 個未解合約日、1,622 個未完整日期**退出 1。
2020-01-01 以後的缺口不改成日線成交，不刪除事後缺資料的商品。
完整來源盤點見 [分鐘資料驗收](FUTURES_MINUTE_TRAINING_2026-09-08.md)。

交付證據集中在 `artifacts/operations/vastai1t_futures_historical_20260908/`：
`minute_delivery_receipt.json`、`daily_source_inventory.json`、`full_preflight.log`。
指定股票 release 的 1,037 個 packed objects、9,879,136,867 bytes，以及既有期貨 release
的 88 個 packed objects、17,462,853,573 bytes 均重新核對；materialization 也經完整驗證。
兩個 `use` receipt 各記錄固定 release、完整驗證與七日 lease。
本次觀測 penguin ↔ vastai1T 為 QUIC、100%、待傳量與錯誤全零。
lab203 未連線，沒有宣稱整個機群同步完成。vastai1T 維持既有 index-only edge 模式。
此 Vast 工作目錄實測 `workspace_is_volume=false`；容器回收或刪除後不得把它當作永久保存。
本機來源及既有 durable cold store 保留。

## 硬體與測量方法

| 項目 | 容器實測 |
|---|---|
| GPU | 2 × RTX 5090，各 32,607 MiB |
| GPU 互連 | `SYS`，跨 NUMA，沒有 NVLink |
| GPU 0 鄰近 CPU | `42-55,154-167` |
| GPU 1 鄰近 CPU | `28-41,140-153` |
| CPU 配額 | 約 53.76 核；主機可見 224 邏輯核不能直接當作容器額度 |
| RAM 上限 | 約 241.77 GiB；主機 503 GiB 不是容器上限 |
| PyTorch | 2.11.0+cu128；嚴格 CUDA 環境檢查通過 |

測量沿用 canonical `train.py`、FinancialTransformer、98 個前日特徵、多基底輸入、BF16、
整數口帳務及完整 train/validation/test/checkpoint/報告流程。
獨立工程資料包含 2014–2019 的全部 2,230 檔有歷史觀測的股票，訓練 2014–2017、
驗證 2018、測試 2019。所有 307,737 個早期日線近似合約日均有 coverage。
此量測使用明確固定的舊已發布日線 SHA `bf9d0f0e6ac18965ff2a193637fd8b1a713a9dee36ac5df0efdf6586834066b4`，
與正式準備資料隔離，不能代表 2020 年以後分鐘資料完整。

每個候選設定串行執行 3 個完整 epoch；保留第一次編譯成本並比較後續 epoch。
GPU 記憶體以每 0.5 秒的 `nvidia-smi` 整卡使用量取樣，不能當作配置器精確峰值。
測量設定先取全機 CPU 48 threads、compile 16 threads，由 DDP 分配至兩個 rank，
並用既有 `STOCKAGENT_DDP_CPU_AFFINITY` 綁定上述 GPU 鄰近 CPU。

整數口執行器新增可選的單日編譯：重用原本 `_scheduled_futures_day`，僅融合一天內的張量運算。
跨日資金與存活狀態仍依時間順序傳遞，不把整個年度展開成一張巨圖。
編譯錯誤直接回報；CUDA graphs 關閉，避免多日 backward 所需的結果遭覆寫。
63/67 通道、不同商品寬度、日線／分鐘混合、padding、沖銷失敗及梯度的 CUDA 對照測試皆通過。

已完成的暖機後測量如下，取 epoch 2、3 的中位數；完整原始紀錄保存在
`benchmark_results.json` 與各組 `epoch_curve.jsonl`。

| 設定 | 每 epoch 秒數 | Train 秒數 | Validation 秒數 | Test 秒數 | GPU 最高取樣 MiB |
|---|---:|---:|---:|---:|---:|
| 原執行器、CPU 資料 | 12.224 | 10.404 | 1.710 | 1.717 | 10,598 |
| 單日編譯、CPU 資料 | 2.997 | 2.471 | 0.433 | 0.431 | 10,432 |
| 單日編譯、GPU 共用快取 | 2.654 | 2.159 | 0.402 | 0.402 | 13,332 |
| 上述設定、global batch 256 | 2.630 | 2.108 | 0.406 | 0.405 | 18,764 |
| 單日編譯、GPU 快取、代數計算 | 2.324 | 2.012 | 0.204 | 0.206 | 10,274 |
| GPU 快取方案重測，6 epochs | 2.664 | 2.166 | 0.404 | 0.404 | 13,332 |
| 代數計算方案重測，6 epochs | **2.165** | **1.866** | **0.200** | **0.201** | **10,272** |

兩個 rank 分別計算 validation / test，兩欄時間有重疊，不能直接加總。
各組 checkpoint 每 epoch 約 0.09–0.11 秒。epoch 記錄以前的編譯 probe、面板建置與
非同步繪圖另有 setup / process wall 紀錄，不能把 2.654 秒當作新程序的全部耗時。
所有組別均完成必要 artifact contract；epoch 2、3 的新增 Dynamo graph 數為零。

batch 256 僅快約 0.9%，但多用 5,432 MiB，且每 epoch optimizer 更新由 8 次變成 4 次；
這個差距不足以證明值得改動預設 batch，因此保留 128。
最終兩組各重測 6 個完整 epoch，取 epoch 2–6 的中位數。代數計算減少基底中間張量，
比共用快取方案再省約 18.7% 時間與 3 GiB 顯存；相對原執行器約快 **5.6 倍**。
這是上述日期／股票範圍內的實測結果，沒有聲稱已證明所有未來 fold 的全域最佳值。

historical YAML 現已採用單日編譯、GPU 共用快取及既有代數計算選項；
保留 batch 128、98 個特徵、18 類基底、模型寬度、BF16、1000 epochs 與年度 walk-forward。
顯存設定為 32 GiB 級、保留 3 GiB margin，沿用現有實際可用顯存檢查。
正式產物隔離在 `artifacts/markets/tw_stock_futures_day_trade_0845_historical_v2_vast5090/`。
七組實驗共完成 27 個 epoch；各自的 optimizer/checkpoint/報告均由原 `train.py` 產生。
遠端最終回歸 **237 項通過**，涵蓋期貨資料／帳務、checkpoint/resume、DDP runtime 與
FinancialTransformer；七組實驗的 63 張必要累積報告圖皆存在且非空。
`final_acceptance.json` 記錄最終設定 SHA、與實測模型的一致性、資料檢查結果及產物驗收。

同輸入帳務對照另取 2019 年 64 個真實日期、完整 2,230 股票軸，以及 TWD 10M／11.37M
兩種資金，測試合計 14,000 口。編譯前後口數、殘倉、失敗旗標與逐日資金完全一致；
報酬最大絕對差約 1.4e-9、梯度差小於 1e-7。
憑證為 `real_executor_parity.json`。模型代數計算的既有前向／梯度對照也通過。
BF16 與編譯的運算順序會使訓練軌跡略有不同，不把工程加速解讀成策略報酬改善。

`frozen_configs/` 保存五組完整解析的對照設定，避免修改正式預設後反向改變實驗定義。
初次建置面板／編譯與共用快取後的啟動耗時不同；process wall 只作各次成本紀錄，
速度比較使用完整暖機後 epoch。兩個六 epoch 重測的完整程序分別耗時約 74.5／67.6 秒，
包含啟動、檢查、訓練、回測與最終圖表產生。

## 一般入口

```bash
cd ~/stockAgent
source scripts/runtime_env.sh
run_fintech_python train.py --config configs/markets/tw_stock_futures_day_trade_0845_historical.yaml --check-data-only
```

目前這一步會如實列出資料缺口。來源補齊、重新建置且驗收通過後，同一入口移除
`--check-data-only` 即開始正常訓練；不需要專用 bash 啟動器。
要重現本機 NUMA 配置的測量，訓練指令為：

```bash
STOCKAGENT_DDP_CPU_AFFINITY='42-55,154-167;28-41,140-153' \
run_fintech_python train.py --config configs/markets/tw_stock_futures_day_trade_0845_historical.yaml
```

這組 CPU 編號只適用本次實測容器；其他機器須重新核對拓撲。
