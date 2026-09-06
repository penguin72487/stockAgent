# 台股當沖策略切換

這個接口用來更換既有穩定 paper market ID 背後的模型、設定與 checkpoint，並重建指定期間的模擬歷史。它不會送出正式委託。

## 第一性責任邊界

策略「已選用」、歷史「已重算」、每分鐘資料「完整」、服務「已載入」與公開介面「可查詢」是五個不同命題：

1. selector 必須解析到指定 config、fold 與 checkpoint fingerprint；
2. 舊 ledger 的目標 market signal pins 必須明確替換，不能只換顯示名稱；
3. 所有 active paper modes 必須在隔離 candidate 中一起重建，非目標模式仍 pin 舊 signal；
4. 每個完成交易日、每個模式必須有 09:01–13:30 共 270 個 right-labelled 點，且內部點都有歷史來源；
5. candidate 驗證成功後才可原子交換，然後驗證 engine、Discord warmup 與本機/公開 API。

歷史官方開盤訊號重建屬於反事實模擬。它不表示訊號當天 09:00 已實際存在，也不表示成交是交易所或券商回報。

事後回補的價格契約固定分成兩個時鐘：09:00 官方開盤價只供模型輸入與整張股數計算；紙上買賣記在 09:01，優先使用右標記首分鐘的 `Amount / volume_shares`，無法計算 VWAP 時改用來源實際發布的同根 K 棒 `Close`。沒有逐筆 tick 本身不構成不成交；只有整根 09:01 K 棒缺失或價格無效才 fail-closed，而且不得用 09:00 開盤、前價、Bid/Ask 或不利一 Tick 補造。Ledger 必須留下 `minute_vwap` 或 `minute_close` 方法，兩者都只是反事實紙上估值。

### 每分鐘曲線與成交價格分開核對

曲線的每個點是「初始資金＋截至該分鐘的已實現淨損益＋剩餘持倉淨清算損益」。
09:01 成交可用 VWAP，但 09:01 估值必須與後續分鐘一致：使用該根來源 K 棒 Close，
扣除尚未分攤的進場費用與預估出場費用。不可只記進場費用、把第一分鐘價格變化拖到 09:02。
停利、停損與部分出場的股數及已實現損益必須以 `fills.jsonl` 核對；
舊 replay 留下、卻不在本次進場帳本中的 `position_history` 不可重新算成有效交易。

修復既有曲線時，先複製完整 live 目錄為同檔案系統的隔離 candidate，再執行：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/rebuild_tw_day_trade_minute_curves.py \
  --state-dir <candidate-directory> \
  --start-date 2026-02-25 --end-date 2026-09-04 \
  --output-dir <audit-output-directory> \
  --simulation --revalue-opening-marks --recompute-existing-strategy-marks --publish
```

日期須依完成交易日調整；此處 `--publish` 只寫入 candidate，不可直接指定運行中的 live。
重建不改成交、模型、部位封存及 13:30 收盤損益；收據保留第一點重估前後值與不變的成交帳本 hash。
之後必須使用既有 `promote_tw_day_trade_replay.py --validate-only`、空倉檢查與原子交換流程，
保留 rollback，再驗證公開頁。缺少實際 09:01 來源時，重估必須拒絕，不可用其他價格補造。

公開曲線使用 `api/history?range=all&resolution=1m&start_date=...&end_date=...`。
`minute_columns_v1` 每列為 `[UTC epoch minute, 所選期間報酬率%, 累積報酬率%, 品質位元]`，
品質位元依序表示舊價、歷史回補、缺價。它是無損欄位精簡，不是每 2,000 點抽樣；
每條線在所選期間第一個有效點重新設為 0%，帳本絕對權益不變。舊客戶端仍可使用預設抽樣介面。

## 日常接口

先預覽，不修改資料：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/switch_tw_day_trade_strategy.py plan \
  --market-config services/discord_bot/markets/tw_day_trade_multi_basis_projection_l1_gelu.yaml \
  --start-date 2026-02-25 \
  --end-date latest \
  --expected-initial-capital 10000000
```

確認 plan 後執行：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/switch_tw_day_trade_strategy.py apply \
  --market-config services/discord_bot/markets/tw_day_trade_multi_basis_projection_l1_gelu.yaml \
  --start-date 2026-02-25 \
  --end-date latest \
  --expected-initial-capital 10000000
```

查詢最後一次執行收據：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/switch_tw_day_trade_strategy.py status \
  --market-config services/discord_bot/markets/tw_day_trade_multi_basis_projection_l1_gelu.yaml
```

重新驗證目前正在服務的版本（唯讀）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/switch_tw_day_trade_strategy.py verify \
  --market-config services/discord_bot/markets/tw_day_trade_multi_basis_projection_l1_gelu.yaml \
  --start-date 2026-02-25 \
  --end-date latest \
  --expected-initial-capital 10000000
```

`verified_with_data_gaps` 表示策略身份、ledger、分鐘曲線與服務同步均通過，但仍有缺價／未成交等明確 operational issue；應保留問題內容，不能改用無來源價格消除警示。

## `plan` 必須顯示的證據

- 穩定 market ID、使用者可見 label、experiment config、fold、checkpoint 與 fingerprint；
- 初始資金；
- receipt-verified TAIEX 起訖日與交易日數；
- active mode 集合與所有模式空倉；
- 舊 source ledger 的 pinned/replaced/discovered keys；
- 需要修復的既有缺 pin 日期；
- 預期 minute rows：`交易日數 × active modes × 270`；
- live filesystem 可用空間。

若 `plan.status` 是 `already_current`，代表目前 live promotion 的策略 fingerprint、完整日期、初始資金與分鐘收據都已相同；此時 `apply` 是無副作用的 no-op，不會重建或再次重啟服務。

## 成功、失敗與回滾

狀態收據位於：

```text
artifacts/operations/tw_day_trade_strategy_switch/<stable-market-id>/status.json
```

沒有資料缺口時狀態為 `complete`；若原子切換、分鐘完整性與服務同步都成功，但存在 fail-closed 的缺價／未成交等 operational issue，狀態為 `complete_with_data_gaps`，不得把它包裝成全綠。兩者都必須包含 promotion receipt、rollback directory、engine sync、Discord warmup 與 dashboard acceptance。任何階段失敗會記為 `failed_retryable` 並保留 candidate/rollback 與 stage logs；不要以部分輸出覆蓋 live ledger。

promotion 使用同一 filesystem 的 atomic directory exchange。成功後的 rollback directory 就是交換前的 live ledger；驗收完成前不可刪除。回滾也必須在所有 paper positions 為零時停止 simulation service、交換完整目錄、重啟三個服務並重新驗收，不能手動複製個別 `state.json` 或 JSONL。
