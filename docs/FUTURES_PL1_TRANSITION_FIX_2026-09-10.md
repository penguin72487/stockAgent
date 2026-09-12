# vastai1T 期貨 carry 訓練修復（2026-09-10）

本次修復遠端 `tw_stock_futures_day_trade_0845_carry_v9.yaml` 在第一個 fold 最終測試發生的 `FuturesCarryDataError`。原始錯誤是 2024-09-24 合晶期貨持倉遇到未處理的實體合約轉換；NCCL 的錯誤訊息是另一個 rank 收到相同失敗後退出。

## 修正與資料證據

[期交所合晶期貨調整公告](https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/6182_20240924.pdf) 宣告舊 PLF 轉為 PL1，同日另掛新的標準 PLF。公告於 2024-09-12 發布，2024-09-24 生效。本次支援公告明定的相同乘數、相同口數轉換：

- 生效日開盤前，舊持倉的方向、口數及前日帳面成本移往 PL1；移轉本身沒有成交或手續費。
- 舊 PL1 與同日新掛 PLF 分開記帳、分開使用各自的分鐘成交量。新單候選仍由原有規則決定。
- PL1 到期採官方完整契約結算價值；2024 年 10 月資料為 59,923 元，包含權利價值，不能僅用 29.82 × 2,000。
- 其他未支援的公司行動、缺分鐘來源或缺持倉估值，繼續中止該軌跡。沒有新增隔離標的或日期，也沒有日線替代成交。

補回 PL1 三個月份共 178 筆分鐘資料，核對 42 個有分鐘來源的合約日與 68 個官方無普通成交的合約日。公告另有兩個月份未觀測到資料，未宣稱齊備；執行器只接受實際可承接舊持倉且已驗證至到期的合約。本次完整歷史可承接的調整合約是 `PL1:202410`。

原 checkpoint 跨過此事件後，另外查出 2026-09-01 `VNF:202609` 的持倉分鐘來源缺口，已透過原有永豐下載器取得 154 筆 exact-month KBar，保留原始資料與完成收據。

新來源為遠端的 `artifacts/data_preparation/futures_corporate_transitions_v2_20260910/manifest.json`，SHA-256：

```text
e10a820da78e0a49454bf777a008a09976477c3711b3634d414fe04f7beb25bc
```

來源包含 111 個合約日、332 筆完整分鐘觀測，策略的執行時點其中有 9 筆觀測。每個引用檔案須通過 SHA 驗證，執行分鐘須與原始 KBar 重建結果一致，有分鐘證據的日期也須實際出現在其來源中。沒有成交時不補造價量。

## 相容性與實際驗證

修正仍使用 `train.py`、共同訓練器、整口期貨執行器與原有報表流程。資料/執行契約從 4 版升為 5 版，增加實體合約移轉欄位；原有 4 版和 v9 檔案保留。新的 v10 設定綁定來源 manifest，使用新輸出目錄及新的 optimizer。

原有 08:45 決策、08:46 目標差額委託、13:20 限價、13:24 市價替換與 13:30 未成交餘額續留政策不變。保留遠端 v9 的 50% 分鐘容量向上取整、整口交易、費稅、1,000 萬元資本、full-trajectory optimizer、BF16/DDP、1000 epoch，以及既有的精確合約日隔離範圍。

原始失敗 checkpoint 的 SHA-256：

```text
312475ae55b852af481f8880c1b9ad180f23b4b91a7a960310c2a922f5803357
```

其第 66 epoch 模型權重已透過原有 compiled panel-slab 推論重播完整 1,133 個交易日（2022-01-03～2026-09-04），沿用原執行形狀：model chunk 64、backtest chunk 256。2024-09-23 淨值 9,484,238 元與原失敗紀錄完全一致，之後完整測試通過。持倉紀錄可見 9 月 23 日 PLF 空 1 口移至 9 月 24 日 PL1 空 1 口，9 月 30 日透過可驗證的分鐘成交出場。

回放終值 8,965,514 元是這個舊模型在修復資料下的工程重現結果，不代表完成新策略訓練或通過投資績效門檻。舊 checkpoint 只用於回放，沒有把舊 optimizer 偽裝成新契約續訓。

遠端針對性回歸套件分別通過 127 項及 217 項（兩批有重疊），涵蓋移轉記帳、長短倉、費用、梯度、CUDA 編譯、未來資料擾動、來源重建、checkpoint 與原有分鐘路徑。新增模組與測試通過 Ruff；遠端未安裝 Ruff，因此使用本機對完全相同 SHA 的檔案檢查。

雙 RTX 5090 驗收已完成。設定最多 3 個 epoch，依原有短測試早停規則（patience=1）完成 2 個 epoch 後正常結束；訓練、驗證、完整測試、checkpoint、fold 完成標記與 9 張累積 walk-forward 圖均通過共同 artifact validator。第一 epoch 含編譯共 125.18 秒，第二 epoch 2.20 秒。正式設定仍為 1,000 epoch。

驗收確認新舊模型 fingerprint 相同、舊 optimizer 因執行契約改變被正確拒絕續用，原始 checkpoint SHA 未變。詳細結果見 `artifacts/operations/futures_pl1_remote_repair_20260910/acceptance_receipt.json`。這是本次錯誤與完整訓練流程的工程驗收，不宣稱其他模型可能持有的所有未觀測合約都已補齊。

## 啟動

以下命令在 **vastai1T 的 `/root/stockAgent`** 執行：

```bash
source scripts/runtime_env.sh
run_fintech_python train.py --config configs/markets/tw_stock_futures_day_trade_0845_carry_v10.yaml
```

正式輸出為 `artifacts/markets/tw_stock_futures_0845_carry_v10_vast5090`。原 v9 的模型、optimizer checkpoint 和失敗證據保留在原輸出目錄。v10 第一次啟動建立新訓練；之後可用同一命令續訓相容的 v10 checkpoint。

修正的比較前快照、15 個檔案的 SHA、可檢閱 patch、原始下載、回放與測試日誌位於遠端 `artifacts/operations/futures_pl1_remote_repair_20260910/`。本機同名 operation 保留程式快照及 patch，沒有用本機較舊的訓練架構覆蓋遠端後續變更。
