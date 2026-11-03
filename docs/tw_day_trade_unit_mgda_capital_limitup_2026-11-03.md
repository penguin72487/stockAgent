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

重用的工程方法包括 GPU sparse projection、訓練正規化 CSR disk cache、
bounded transport、編譯融合逐日帳務、saved adjoint、CUDA Graph、平行帳務反向與
共用不可變來源快取。新增成本保留這些路徑的前向與 VJP 等價性。
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
