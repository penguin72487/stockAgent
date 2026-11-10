# 台股參數空間 Unit MGDA：本金與漲停殘留空單成本實驗

本次依使用者選擇，回到 `geometry__unit_mgda_sgd_step` 的當沖研究設定，
從零訓練十個策略，初始本金依序為新臺幣 100、200、…、1,000 萬元。
既有研究模擬服務與舊 checkpoint 另存，不將舊策略權重當作新訓練的起點。

## 帳務規則

使用者已確認：收盤漲停且依來源成交容量回補後，仍未回補的空單才罰。
每股費用為官方收盤價乘以 7%，乘上該時點剩餘空單股數。
例如收盤價 110 元，剩餘空單 2,000 股，額外成本為 15,400 元。
已回補的數量、多單、僅盤中觸及漲停但收盤未漲停者不收這筆費用。
原價差損益、佣金、交易稅及其他原有成本仍照原帳務計算。

原設定啟用了研究用「收盤價、無限容量的最後平倉」。因此認定時間必須是
**來源容量回補完成後、研究用最後平倉之前**；若放在最後平倉之後，剩餘部位
永遠為零，罰則就會失效。本次保留原最後平倉設定，在其前一次計入收盤成本。
這仍是原研究模擬的成交假設，沒有把最後合成平倉描述成實際市場成交。

成本計入 `realized_net_pnl`，不挪入隔夜持倉成本，以保留原平倉帳務的仿射結構。
來源層在準備階段計算每股成本向量，密集、緊湊傳輸、融合帳務及 CUDA Graph
共用同一帳務函式。日內訓練熱路徑不新增 Python 判斷或 CPU/GPU 同步。
費率啟用時寫入 config／checkpoint／run manifest；費率為零時省略新合約欄位，
維持原有停用分支的版本相容性。

## 原設定與研究範圍

| 項目 | 設定 |
| --- | --- |
| 策略 | 參數空間單位梯度 MGDA / Parameter-space Unit MGDA |
| 神經網路 | Financial Transformer，`d_model=32`、`lookback=32` |
| 輸出 | `score_entmax_scale_separated_cash_v2` |
| 梯度目標 | −Sharpe、−Log Return、−Sortino、−Calmar；參數梯度先作全域 L2 正規化，再求 MGDA 組合 |
| 最佳化器 | SGD，learning rate 0.1，無 momentum，原 warmup/cosine 與 clip 設定 |
| 訓練 | seed 42，最高 1,000 epochs，連續 100 次驗證無改善停止 |
| 批次與精度 | 原 global batch 128、eval batch 16，雙卡 DDP、BF16 AMP；敏感帳務保留 FP64 |
| 資料 | 固定原 v4 source snapshot，2,757 股票、14,726 輸入通道、3,109 個原日曆點 |
| 原最後 fold | fold 10；2015–2024 訓練、2025 驗證、2026 測試，2014 為歷史特徵背景 |
| 基準 | 原 2330 買進持有 / Buy and Hold |
| 成交 | 原整張與成交量參與限制、分鐘回補排程及原研究用最後平倉 |
| 本次差異 | 本金 100–1,000 萬，以及上述殘留空單 7% 成本 |

這是明確選取的舊策略實驗，並未套用其他新模型預設的 d128、lookback256 或
learned-cash 輸出。新版本會使用目前可驗證的程式與遠端套件環境，並非宣稱與
舊 Torch 版本逐位元重現。沿用的歷史資料仍屬原回溯研究 ABI；測試年已被歷史
研究多次查看，因此新結果不能當作未曾使用的獨立測試集。

本金改變會改變整張門檻、可成交數量、費用占比與參與容量，進而改變整條梯度
路徑。因此十組均獨立訓練，不能把同一策略的資產曲線等比例縮放充當實驗。

## 排程、驗證與效率

1. 原 canonical cold transport 恢復三個固定快照，沿用完整 SHA 驗證與七日 lease。
2. 優先從遠端既有較長期間的同源 panel，按原 calendar 無變更取回 value-only
   特徵；透過原 codec 重建原區塊，逐一比對舊版 SHA。2026-11-03 已驗收
   common 整段與全部 4,224 個區塊相同，恢復耗時 216.57 秒；未補值、重設或
   移動觀測時點。證據為 `_operations/exact-feature-recovery.json`。若無法逐塊
   通過，才交回原 builder，禁止放寬一致性要求以啟動訓練。
3. 嚴格雙卡 CUDA 預檢，實際執行串行／平行及融合／一般 CUDA Graph 前向、反向對照。
4. 使用原完整流程 benchmark 跑 4 epochs，保留 validation、test、每 epoch 曲線、
   圖與 checkpoint，量測完整 fold 與第 3、4 epoch；工程測速不取代正式收斂。
5. canonical ablation runner 依本金順序跑十組，每次共用兩張卡，失敗依既有重試／
   跳過規則繼續。只有實際容量不足才調整批次，保留獨立 attempt 與原失敗紀錄。

三個固定來源快照已全部通過完整物件驗證並取得七日 materialized lease，
收據為 `_operations/source-leases.json`。首次執行仍需建立原始 panel、分鐘事件與
physical source 快取；這段冷準備成本與穩態 epoch 吞吐分開記錄，不能以 NCCL
等待期間的 GPU busy 百分比當作有效計算。
首次 physical source 建立耗時 516.5 秒、source gap 為零；訓練窗口 RMS 首次
計算耗時 237.807 秒，十組共用其內容鍵快取。

完整測速的前兩個 attempt 在 epoch 1 前，由嚴格 loss probe 擋下不存在的
`/venv/main/bin/x86_64-conda-linux-gnu-gcc`。模型編譯檢查曾將任意絕對路徑
直接視為可用，而且停用模型編譯時，帳務的獨立入口沒有選取系統編譯器。
最終把解析統一放到共用 `runtime_configuration`，經 `shutil.which` 驗證存在、
一般檔案與執行權限，並由共用帳務入口與排程父程序匯出 CC／CXX。
相關編譯器與 factorized panel 範圍 69 項測試通過；以原 frozen release 的兩檔
overlay 發佈修正，未改金融帳務、模型與特徵。失敗 attempt 與測量證據保留。
獨立編譯 smoke test 不能代替完整 loss probe；後續測量兩者均須通過。
重啟續接改先核對原 hot lease、READY、固定 snapshot、登記 alias，再在原 dataset
鎖內使用原 inventory 與 materialized verifier 重驗全部來源；三份檢查分別耗時
5.05、1.71、5.81 秒。透過原 edge operation lock／receipt／allowlist 撤回本研究
重複提出的三個 hydration，沒有刪檔、改 lease 或取消其他 retained payload。
這避免已展開的來源為了重啟，又重新下載約 19 GB 的 packed payload；收據為
`_operations/materialized-source-readback.json`。

重用的工程方法包括 GPU sparse projection、訓練正規化 CSR disk cache、
bounded transport、編譯融合逐日帳務、saved adjoint、CUDA Graph、平行帳務反向與
共用不可變來源快取。新增成本保留這些路徑的前向與 VJP 等價性。
新遠端的自動來源 RAM cache 僅 5.31 GiB/rank；原日曆最少五年就有 1,211 個
session，僅 packed marks 的儲存下界已為 6.72 GiB/rank，因此排程明定
`STOCKAGENT_DAY_TRADE_SOURCE_CACHE_GIB=16`。當時 cgroup 上限 241.75 GiB，
扣除 inactive file 後的占用約 62.61 GiB，匿名記憶體僅約 1.44 GiB；沒有調高
主機配額或解除資源限制。這是快取容量設定，不是模型／損失變更，吞吐收益
仍由完整測速與快取計數驗收。證據為 `_operations/source-cache-admission-probe.json`。
完整四 epoch 測速已通過。第 3、4 epoch 的 post-plot MAX-rank 完整時間分別為
16.7245、13.5595 秒，中位數 15.1420 秒；完整訓練階段中位數 14.0144 秒。
每 epoch 包含全部 2,439 個訓練日、五次模型前向、四次參數 VJP、一次更新，
以及原驗證、測試、曲線、圖與 checkpoint。穩態沒有新增 Dynamo graph 或
帳務 fallback，實際帳務前向、反向與平行 adjoint 均有重播。
雙卡峰值分別為 27,486、26,240 MiB，最小顯存餘裕 5.001 GiB。
rank 0 來源快取峰值為 11,977,369,800 bytes（11.15 GiB），compact hits 24,018、
packed hits 243，兩種表示都沒有淘汰或載入錯誤；16 GiB 設定已涵蓋實際工作集。

這個四 epoch fold 的完整 wall time 為 1,259.899 秒，首個 epoch 為 879.158 秒，
包含首次 normalized CSR 填充、目前環境的編譯，以及最後最佳模型重播／報表。
較早的來源搬回、physical source 初建及 RMS 首次計算另有紀錄，不在這個數字內。
因此 15.1420 秒只代表已暖機的完整 epoch，不能當作新機全冷啟動成本，也沒有
以四 epoch 策略表現作選模或收斂結論。精簡原始證據共 24 份、2,006,653 bytes，
已在遠端與本機逐檔 SHA 核驗，位於 `_operations/engineering-accepted/`。
遠端回收控制還修正了 private module 的 canonical `__file__`，並與 archive／reader
一起攜帶 `storage_layout`、`service_layout`，使舊 compute checkout 也能解析目前
的相對 policy 與來源路徑。共用回收範圍 228 項測試通過，另有一項模擬缺少
layout 模組的獨立載入檢查通過；沒有改遠端 checkout 或放寬 process/reference gate。
回收程序亦可用 `--remote-env-dir` 指定已發現的環境；本次由 `vast-capabilities`
取得 main 環境，解決舊 control 環境缺少 pyarrow、因而無法審查現行訓練設定的問題。
canonical dry-run-05 的 blockers 為空，apply-01 已刪除唯一指定的測速目錄，回收
104,280,064 allocated bytes。獨立讀回確認測速目錄不存在、準備好的資料仍在，
十組實際設定的本金／7% 費用／模型與 scratch 初始化亦已逐組核對。
「僅剩硬體計算限制」尚須完整實測與 profiler 證明；GPU 利用率或單元測試通過
不能代替這個結論，也不能為測速省略曲線、驗證或 checkpoint。

## 設定與產物

- 基底：[市場設定](../configs/markets/tw_day_trade_unit_mgda_capital_20261103_v1.yaml)。
- 十組實驗：[消融矩陣](../configs/ablations/tw_day_trade_unit_mgda_capital_20261103_v1.yaml)。
- 正式產物：`/root/stockAgent/artifacts/markets/tw_day_trade_unit_mgda_capital_limitup_20261103_v1/`。
- 遠端工作資料：`/root/stockAgent/data_tw_day_trade_unit_mgda_20261103_v1/`。
- 工程測速暫存：`/root/stockAgent/artifacts/benchmarks/tw_day_trade_unit_mgda_capital_20261103_v1/full-workflow/`；
  正式區保留精簡測量證據，完成測速後依原 testing-discard 的逐路徑程序回收。
- 排程程式：正式產物下 `_operations/run_study.py`，Supervisor 程序
  `stockagent-daytrade-unit-mgda-capital-20261103-v1`。
- 階段進度：`_operations/status.json`；來源 lease、版本驗收、工程結果與指令紀錄亦在該處。
- 個別策略：`capital__twd_1000000` 至 `capital__twd_10000000`；實際 attempt
  由原 `_capacity/` owner 保存，正式完成狀態以各 fold 的 lifecycle receipt 為準。
- epoch 曲線：各 attempt 的 `training/train_*/epoch_curve.jsonl` 與同目錄產生的圖；
  fold 10 的完整資產曲線、績效與 checkpoint 由原 artifact contract 產出。

2026-11-03 14:56（UTC+8）獨立讀回：十組正式設定均已建立，100 萬組已完成
7 epochs，另九組依序等待。每個已暖機 epoch 仍為四次參數 VJP、一次更新，
沒有新增 Dynamo graph 或帳務 certificate fallback；最近四個完整 epoch 為
14.64–16.81 秒。最佳 checkpoint 當時為 epoch 6，已實際 CPU 載入，loss 與
時間圖也已產生；此時尚未收斂。證據為 `_operations/formal-first-epoch-readback.json`，
後續即時進度以 `_operations/status.json` 和各組 `epoch_curve.jsonl` 為準。
本次 frozen source 為 `20261103T061646746478Z-05bd2b506fd3`；
`_operations/formal-launch-readback.json` 保存十組本金與成本設定的核對結果。

2026-11-03 工程驗證：CPU 帳務與傳輸範圍 76 passed、25 skipped（含 CUDA）；
遠端四組實際 CUDA Graph 對照 4 passed。較廣的 checkpoint 範圍有 158 passed、
2 skipped，另有一項既有 Bybit config 的 cache path 不在 exchange scope 而失敗，
不將它報為本次台股完整回歸通過。正式遠端執行狀態以當前 `_operations/status.json`
及 canonical epoch／completion 產物讀回為準。

遠端 QuACK 0.6.5 的實際 softmax 前向與反向也已對照通過（FP32、BF16），
紀錄在 `_operations/quack-kernel-check.log`。這些驗證證明各自的工程範圍；
完整 fold 測速與本金實驗績效仍須讀取正式結果，不能由核函式對照推論。

追加兩項 CPU 財務邊界對照通過：Float32 儲存的 55.1 元收盤價經原價格格還原後
能正確認定漲停與費用；收盤額外成本造成的破產保留吸收態，後續報酬與成交皆為零，
不將金融損失誤標成來源失敗。紀錄為 `_operations/additional-fee-boundary-tests.log`。
