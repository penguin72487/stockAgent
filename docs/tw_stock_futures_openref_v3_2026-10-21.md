# 股票期貨跨日、雙決策時鐘 v3/v4

截至 **2026-10-21 16:44:53（台北）**：v4 正式 08:45 已完成 3 個 epoch，
正在接續訓練；09:00 排在同一序列後方。兩組四 epoch 工程驗證均已完成。
正式 last checkpoint 為 15,854,214 bytes，已獨立 SHA 回讀；證據見
`artifacts/operations/futures-opening-reference-20261021-v4/formal-training-readback.json`。

## 問題與責任邊界

v2 準備程序先讀舊近月歷史，將剩餘 371,277 個契約日交給 exact
KBar reader；實際上既有 FinMind 全市場逐筆已涵蓋這些月份。KBar
receipt 的查詢終日又超過已核驗規則日期，造成準備中止。這不是 GPU
訓練或付費帳號權限問題。原始盤點保留在
`artifacts/analysis/tw_stock_futures_source_audit_20261021/audit_conclusion.json`。

新流程延伸共用 `tw_futures_finmind_ticks.py` 與
`prepare_tw_futures_decision_clocks.py`，沒有新增 trainer 或金融帳本。

1. 固定既有 financial daily/rules、原始來源 SHA 與 active global tick
   receipts；不 glob 已淘汰版本，不向供應商重下載已有資料。
2. 以日期批次讀取全部交割月，透過明確 date/product/contract 對到唯一
   physical lifetime。每個完整原件只讀一次，避免逐契約重讀同一物件。
3. 每筆 FinMind 雙邊量必須為正偶數，再精確除二；保留重複成交與來源順序。
4. 原始 Float32 報價保留不動。衍生報價只接受唯一的當日合法 tick，且必須
   重新編碼回完全相同的 Float32；無法唯一恢復就拒收，不任意四捨五入 VWAP。
5. 共用官方鉅額 reader 支援有 SHA 的區間查詢；一般市場成交量等於
   official volume 扣除價差腿及鉅額量。完整日量與 HLC 對帳成功後，才可
   證明指定分鐘無成交。該證明不改寫可持倉資格或結算價。
6. 對帳不足的近月可沿既有 Shioaji R1 reader，以 dated tenor、實際 HLC
   和完整日量核驗互補來源。2021-12-30 NMF/202201 的一口差異由既有原件補證。
7. 開盤參考價取真實第一筆 session print，保守地等該分鐘完成才可見；此前
   使用同一 physical contract 已知的最後真實成交。官方結算、價差與鉅額
   推導的 daily OPEN/CLOSE 不會變成一般市場成交、參考價或可成交容量。

兩組成交視窗分別為 `[08:46,08:47)` 和 `[09:01,09:02)`；右端標記
527/542。參考價有原成交時間、來源 SHA 和獨立的 availability bound。
這是分鐘成交 VWAP 研究代理，沒有歷史接收時間、委託簿深度或實際撮合保證。
來源歷史回應也不等同完整的歷史版本 point-in-time 證明。

## 實驗設定與產物

沿 v2 設定：FinancialTransformer、d32、lookback32、global batch128、BF16、
雙卡 DDP、整口數與跨日保證金帳務、單一 log utility、從零訓練。
epoch cap1000、連續100回合無改善、min_delta0.0001；不匯入舊模型或 optimizer。
最後 fold5 的年份為 train2020–2024 / validation2025 / test2026，邊界維持
原本 `lookback_shifted` 與 `panel_history`。

v3 作為修正與速度對照；目前正式排程使用後文 v4，以下 v3 正式路徑已停用。

實際時間邊界為 train 2020-05-11 至 2025-02-25、validation 2025-02-26
至 2026-02-25、test 2026-02-26 至 2026-09-24；年份標籤不能當作自然年切割。

| 用途 | 路徑 |
| --- | --- |
| v3 08:45 原預定正式，已停用 | `artifacts/markets/tw_stock_futures_decision_clocks_20261021_v3/clock__0845` |
| v3 09:00 原預定正式，已停用 | `artifacts/markets/tw_stock_futures_decision_clocks_20261021_v3/clock__0900` |
| 完整來源核驗與雙時鐘資料 | `artifacts/data_preparation/tw_stock_futures_decision_clocks_20261021_v3` |
| 雙卡四 epoch 工程流程 | `artifacts/benchmarks/tw_stock_futures_decision_clocks_20261021_v3/smoke-context` |
| 排程／交付／GPU 核驗收據 | `artifacts/operations/futures-opening-reference-20261021-v3` |

v3 程式 release：`20261021T074738463074Z-a596db97b339`；source SHA
`a596db97b339ad593410933f84cbdb35c0ca2de7d5869fca8deced6df8834a5e`。
queue request SHA：
`0df800a94e303023ca6ad8dfa898ac6179db6f0a632e8ea791580afc26d6951f`。
固定 financial daily/rules 與 v2 相同；衍生 source pair、config/output root
另立 v3，不覆寫 v1/v2 或從不相容 checkpoint resume。

## 驗證與效能範圍

76 項來源／時鐘測試、254 項帳務／checkpoint／lifecycle 回歸已通過。
五個真實日期的 1,947 個契約日完整量與 HLC 對帳成功，包含兩個僅鉅額案例
與 NMF 原件互補。完整範圍、遠端獨立回讀、四 epoch smoke 和正式進度分別
由下述收據負責，不能從單元測試或 Supervisor RUNNING 推論完成：

- 準備：`progress.json`、`source_coverage.parquet`、兩組 manifest。
- 遠端來源：operations 下 `data-admission.json`、`remote-progress.json`。
- GPU 機制：`ledger-gpu-checks.json`、`ledger-gpu-checks.log`。
- 完整訓練：各 case 的 `run_manifest.json`、`progress.json`、
  `train_*/epoch_curve.jsonl`、checkpoint 與 `fold_05/fold_complete.json`。

遠端核驗也發現舊 absorption fixture 已落後於 account contract7：普通風險
減倉因無成交而未完成時，正資產帳戶保留責任並於後續繼續減倉，並非自動破產。
修正測試使用真正無法完成的到期平倉義務，並另測普通風險減倉維持存活。
未改變正式帳務；修正後三項 target-GPU forward/gradient/CUDA Graph 核驗通過。

加速保留 train/validation/test、每 epoch 圖、checkpoint 和最終報告。
批次來源處理減少重複 I/O；正式帳本沿用已實作的 metadata hoisting、
編譯 funding/position projection、CUDA Graph 與 canonical trajectory 更新。
完整來源 SHA 核驗仍需要讀取必要原件；初次建置／編譯與 epoch3+ MAX-rank
穩態時間須分開。尚未以此紀錄宣稱純 GPU 算力極限或策略超越舊股票模型。

## 完整來源核驗實測

完整 1,587 日全市場 tick 正規化／對帳耗時 935.380 秒（不含此前規則原件
SHA 核驗及此後交接）。913,707 個契約日中，721,548 個可交易契約日全部
通過，`coverage_gaps=0`；其餘 192,159 列維持原先不可交易狀態。
488 個契約日有獨立官方鉅額交易扣量，其中兩列一般委託簿為零；另有一個
NMF 契約日透過已驗證且綁定當日實體近月的 Shioaji 原件互補。原始收據
不變。`source_coverage.parquet.outright_volume` 是扣除鉅額後的一般委託簿量，
全部等於 `tick_volume`，不可再次扣除 `block_volume`。

| 決策 | 有真實成交的執行分鐘 | 已核驗但該分鐘無成交 | 開盤參考已知 | 本時段首筆成交已可見 |
| --- | ---: | ---: | ---: | ---: |
| 08:45 | 102,617 | 618,931 | 890,790 | 0 |
| 09:00 | 226,054 | 495,494 | 893,125 | 257,365 |

08:45 首分鐘尚未完成，所以不能提前看到該分鐘首筆；09:00 已可見的
本時段開盤成交依原始時間／順序選取。沒有已知前筆成交的參考保留 unavailable，
不能補成虛構交易。兩組共用配對來源識別：
`52cb8734ea31f6c8fb11edbb079d0039a49af4ae9219b97d2d4f3580301ffb5b`。
交接清單含 56,866 檔、6,060,650,633 bytes；傳輸之後仍須遠端獨立回讀，
本節的來源成功不代表訓練已成功。

## 凍結程式包啟動修正

第一輪 smoke 在建立 panel cache 前發現共用 runtime 政策未被 source
inventory 收錄：`configs/data_sync/cache_capacity.json` 不存在於隔離程式包。
修正 `runtime_identity.source_identity` 的明確公開依賴清單，連同
`prepared_capacity.json` 一起納入 SHA 與 source bundle；私人節點 enrollment
仍不收錄。50 項 runtime/release/cache-capacity 測試通過，再建立新的不可變
程式包；原金融帳本、loss、梯度與實驗參數未改。原失敗 smoke 與舊 request
保留，修正版本使用獨立 `smoke-runtime-policy` 工程產物路徑。
原雙時鐘資料與配對 SHA 完全沿用，但交付 request 重新綁定且遠端重新逐檔
核驗，不把舊傳輸成功視為新程式已可訓練的證明。

後續 smoke 找出 verified source end 與 decision-clock start 裁切對
factorized storage 使用一般 NumPy slicing，分別企圖展開 504,246,808,440
及 257,726,146,536 bytes。修正共用 start/end 裁切，透過
`FactorizedPanelFeatures.slice_dates` 保留原本實體 block 座標、來源 row offset、
共享已核驗 cache、零複製 base/common views，並刷新裁切後 content fingerprint。
訓練的 bounded slab / Torch / compact statistics 仍沿原本共用路徑；保留全部
14,726 個特徵，沒有放大容量上限、改小網路或改用不完整來源。
112 項 factorized/panel/financial source boundary/decision-clock 測試通過，
最終另驗三個跨 block 日期區間案例：dense/Torch slabs、重排重複日期的 compact
statistics 與 transport rectangles 逐位元相同，RMS 統計一致，且裁切時
不讀取任何個股特徵 block。起始／截止裁切組合與直接區間裁切的指紋相同。

雙卡完整路徑另找出 `WindowedSplitTensors` 仍只接受舊 context 寬度，
遺漏模型／資料 reader 已支援的「22 基礎 + 3 保證金 + 2 風險觀察 + 4 開盤參考」
31 欄組合。明確加入這一組合法 schema，保留其餘 shape/whole-contract tape
驗證；1936/2560/2816 三個版本的 action 軸皆受測。
87 項 opening-reference/account/corporate/window admission 測試、29 項共用
window/cache 回歸通過。與前個 release 比較，金融帳本／模型程式 SHA 不變；
只有 window admission 與本研究不執行的日內曲線重建工具變動。原 target-GPU
帳務收據以獨立 code-equivalence 收據保留，新的完整 GPU 訓練仍須自行通過。

## v4 無損傳輸與單次 slab 原始封包快取

v3 的 08:45 雙卡四 epoch 已完成 canonical artifact gate，含每 epoch
train/validation/test、圖、checkpoint 與最終報告。未完成的 09:00 v3
工程排程於此時停止，未將 v3 工程 checkpoint 用於正式初始化。

| v3 08:45 epoch | MAX-rank 全 epoch 秒 | MAX-rank train 秒 | 新編譯圖 |
| --- | ---: | ---: | ---: |
| 1 | 391.110 | 289.756 | 10 |
| 2 | 229.379 | 174.262 | 0 |
| 3 | 200.983 | 149.307 | 0 |
| 4 | 205.412 | 152.256 | 0 |

四回合梯度範數依序 11.9067、10.0243、8.9015、7.7779，沒有零梯度 batch；
帳戶存活，return-adjoint 重播最大差均為 0。這是工程驗證，不是收斂結果。
穩態完整 epoch 平均 203.198 秒；模型前向／反向包含內部來源展開與傳輸，
不能將外層 metadata transfer 的毫秒數當成全部特徵 I/O。逐日帳務不再是
本輪主要耗時，不能據此繼續盲目優化帳本。

v4 只把 `data.factorized_transfer_mode` 從 `dense_cpu` 改為既有
`compact_cuda_cached`：用原始無損 rectangles 傳輸，在 GPU 還原相同的完整
FP32、14,726 欄連續 tensor，再執行原模型；於同一次 slab/checkpoint
生命週期內快取原始封包。保留 signed zero、NaN bits、特徵順序和全部維度。
沒有 sparse 模型、feature selection、永久 BF16 source 或 learned embedding
跨 optimizer 重用。所有模型、training、backtest、data 與主入口的 204 個程式
SHA 與 v3 相同；另有正常變動的新聞／股票部署工具不在期貨執行路徑。

新設定為 `configs/markets/tw_stock_futures_carry_{0845,0900}_openref_20261021_v4.yaml`；
source release `20261021T081118180991Z-ea5d9ab68f72`，request
`178eb8ee7bc0e2ce703ee54d6f9ad573512a232dc2c79aeb08ba475e9b26cb41`。
固定 financial daily/rules、v3 配對分鐘與 opening-reference 來源不變。
7 項既有 lossless/transport/cache 測試及 resolved-config 單軸比對通過。

v4 的獨立遠端回讀後，先於兩張實際 5090 核對日期裁切、跨 block、來源尾端、
重排重複日期等 24 個真實全欄 chunk 及快取重播，然後兩組各跑完整四 epoch。
artifact、有限非零梯度、adjoint 重播、穩態零重編譯與完整 MAX-rank 耗時改善
全部通過後，同一 canonical queue 才啟動從零的正式訓練。各 gate 以目前收據
為準，不能把本段排程設計視為已通過。

2026-10-21 16:17（台北）遠端已重讀全部交付來源，完成兩卡 raw gate：
24 個 chunk 共 3,291,084,288 logical bytes，首輪封包只傳 247,686,864 bytes
（7.53%），同一 slab 再讀的 host payload 為 0，逐位元核對全部通過；
此項獨立核驗耗時 10.038 秒。這個壓縮比只屬所測切片，不能直接當全 fold
加速比。完整四 epoch smoke 已開始，正式訓練仍等待完整 gate。

08:45 v4 四個完整 epoch 的 MAX-rank 耗時為 110.093、34.807、33.763、
25.465 秒；epoch3/4 平均 29.614 秒，相對 v3 同兩回合的 203.198 秒為
6.862 倍吞吐。四回合均有有限非零梯度、無新增零梯度 batch，return-adjoint
重播差為 0，epoch2 起沒有新編譯圖。每回合的 validation/test/圖/checkpoint
仍在；冷初始化與最終報告另計，不把這個穩態數字稱為完整冷 fold 加速比。

raw 位元等價不代表 optimizer trajectory 必定逐位元相同。兩版首回合 train loss
相同，梯度 norm 已相差約 9.54e-7，validation/test loss 隨後也不同；本輪保留
用戶允許的浮點／梯度軌跡差異，金融機制仍由 fixed-action 帳務測試核驗。
未透過完整 native A/A 證明差異的單一來源，亦未將四 epoch 表現視為正式收斂。
完整 source SHA 讀取、冷啟動、正規化及仍存在的模型／傳輸開銷另有成本，
本輪不宣稱已達到只剩 GPU 算力的全域極限。

08:45 兩版 canonical ablation `summary.json.elapsed_s` 的完整四 epoch case
總耗時為 v3 1,472.272 秒、v4 568.849 秒，約 2.588 倍吞吐；包含程序啟動、
來源／panel、normalizer、訓練、每 epoch 輸出與最終報告。兩者為同節點 fresh
process，既有 panel/編譯／檔案系統 cache 狀態仍可能影響冷成本，並非新節點
首次取得資料的對照，也沒有完整 ABBA 重複。09:00 v3 未完成，不能虛構其速度。

## 雙時鐘完整 gate 與正式排程

2026-10-21 16:36:49（台北），兩組四 epoch 的 canonical artifact gate、
非零有限梯度／adjoint replay／穩態零編譯與加速 gate 全數通過，既有排程
已呼叫正式 08:45 case；09:00 在同一序列後接續。工程 optimizer 不捐贈給
正式模型。此時正式程序在冷初始化，真正進入 epoch／寫 checkpoint 的證據
由正式 `clock__0845/progress.json`、`train_*/epoch_curve.jsonl` 負責。

09:00 工程 epoch1–4 MAX-rank 分別為 109.025、27.030、24.676、27.192 秒；
epoch3/4 平均 25.934 秒。每回合非零有限梯度、alive、adjoint replay差0，
epoch2 起沒有新編譯圖。股票歷史 RMS 命中同一訓練範圍的共用 cache，進入
fold 後的準備共 3.530 秒；這不包含先前來源核驗與父／DDP 程序初始化。

遠端獨立回讀最終 test：兩組均覆蓋 145 日，持倉皆為 Int64 整口數，實際
持倉分別有 5,194／7,722 個非零契約日格，跨日 residual 4,988／7,455 格；
策略／benchmark 報酬有限、terminal alive、settlement-default 日數0。
這驗證了實際 carry 路徑，不是完整收斂績效。

原始收據為 v4 operations 下 `raw-transport-gpu-checks.json`、
`transport-timing-acceptance.json`、`financial-smoke-readback.json`、
`remote-progress.json`；小型收據與工程 curves 已回讀本機，模型權重仍由遠端
正式產物路徑持有。

正式 08:45 第 1 epoch MAX-rank 為 107.414 秒，第 3 epoch 為 24.498 秒；
第 3 回合梯度 norm 8.9240、alive、adjoint replay差0，train/val/test loss 有限。
這是正式 1000-cap／100 回合無改善契約下的真實 checkpoint，並未將工程
optimizer 轉成正式續跑。正式產物在遠端：

- 根：`/root/stockAgent/artifacts/markets/tw_stock_futures_decision_clocks_20261021_v4`。
- 08:45：`clock__0845/train_2020-2021-2022-2023-2024/checkpoint_last.pt`。
- 08:45 進度：`clock__0845/progress.json`、`train_*/epoch_curve.jsonl`。
- 09:00：`clock__0900`，接續 08:45 完成或失敗後由原排程啟動。

本機目前只回讀小型核驗／進度／curve 檔案；正式權重仍在上述遠端位置。

| v4 用途 | 遠端 `/root/stockAgent/` 下路徑 |
| --- | --- |
| 08:45 正式 | `artifacts/markets/tw_stock_futures_decision_clocks_20261021_v4/clock__0845` |
| 09:00 正式 | `artifacts/markets/tw_stock_futures_decision_clocks_20261021_v4/clock__0900` |
| 四 epoch 工程驗證 | `artifacts/benchmarks/tw_stock_futures_decision_clocks_20261021_v4/smoke-transport` |
| 原始 GPU 位元、效能與排程收據 | `artifacts/operations/futures-opening-reference-20261021-v4` |
