# 09:01 成交量單位事故與歷史修復邊界（2026-09-22）

## 根因與受影響範圍

永豐 regular-board stock historical Tick `volume` 是「張」；本地 1 分鐘
Parquet 與 KBar fallback 則存已正規化的「股」。舊版 missed-opening runner
與 replay builder 都把 `tick_volume_units_0901` 無條件除以 1,000，
因此 Tick 路徑的可用容量被縮小 1,000 倍。正確上限是
`floor(原始張數 × minute_volume_participation) × 1,000 股`；
它只是紙上分鐘容量證據，不是券商成交或排隊撮合證明。
永豐公開的 [stock Tick 欄位定義](https://sinotrade.github.io/tutor/market_data/streaming/stocks/)
也明列普通股 Tick `volume` 為張，盤中零股則為股；本程式此路徑只接普通股整張資料。

唯讀逐筆收據稽核：
`artifacts/data_quality/tw_day_trade_0901_volume_impact_2026-09-22.json`。
自第一筆 2026-02-25 至 2026-09-22 的完整來源／執行契約稽核：
`artifacts/data_quality/tw_day_trade_0901_full_history_audit_v4_2026-09-22.json`。
其中 `legacy_capacity_corrections` 逐筆記錄原容量、來源可證容量與
當日原始收據雜湊，作為後續隔離重建的逐筆定位證據；它本身不改寫成交或損益。
`missing_0901_price_symbols_by_date` 保留所有缺價的交易日／標的，供
`scripts/recheck_tw_day_trade_0901_local_price_gaps.py` 先對本地來源重新核對。
本地重查收據是
`artifacts/data_quality/tw_day_trade_0901_local_price_gap_recheck_2026-09-22.json`：
129 個缺價日期、30,263 個不重複日期／標的中，現有本地資料只新增解出
9/10 的 16 個，仍有 30,247 個日期／標的缺 09:01 價格；來源讀取錯誤為零。
新增的全市場首根時間診斷在
`artifacts/data_quality/tw_day_trade_0901_local_price_gap_recheck_v3_2026-09-22.json`：
129 日分區均存在，剩餘缺價組合中，1,131 組第一根為 09:02、10,047 組
為 09:03、19,039 組晚於 09:03、30 組該分區沒有這個標的；**09:01
存在卻未被本地解析器解出的組合為 0**。例如 2/25 的 0056，其原始
`shioaji_1m/minute_chunks` 與衍生 `research_dataset` 都從 09:03 開始，
不是衍生分區漏掉 09:01。這只證明目前這些本機來源缺該時點，不能證明
永豐歷史 Tick 或其他未查來源也沒有 09:00–09:00:59 的交易。
另核對本機 `data_tw_microstructure/hft_dataset` 15 個日期分區：它只涵蓋
當時的 top-200 股票，與這 30,247 個缺價日期／標的**交集為 0**，
故不能從其逐秒資料補上這批 09:01 缺口。此檢查只比對股票宇宙；若未來
交集非零，仍要查原始 Tick 的交易所時間及收據，不能拿逐秒特徵當成交。
這是日期／標的粒度，與前述 30,454 筆跨模式訊號列不能直接相加。
可重跑：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_tw_day_trade_0901_volume_units.py
run_fintech_python scripts/recheck_tw_day_trade_0901_local_price_gaps.py \
  --audit artifacts/data_quality/tw_day_trade_0901_full_history_audit_v4_2026-09-22.json \
  --output artifacts/data_quality/CHOOSE_NEW_LOCAL_RECHECK_RECEIPT.json
```

受影響的來源日是 2026-09-10、09-11、09-14、09-16、09-18、09-22。
共 2,340 筆 Tick 來源的有目標股數訊號，其中 2,094 筆容量被低估，
2,003 筆原記零容量但來源顯示正容量，246 筆修正後仍零容量。
全部 21,881 筆 Tick 來源訊號均與當日原始收據對上，來源錯誤計數為零。
09-02、09-03、09-08 雖有 Tick 收據，當前帳本沒有使用這條來源的訊號；
更早的本地分鐘 K 棒紀錄不得因相同文字的「容量不足」而批次改成交。

完整稽核涵蓋 144 個有訊號日期。2/25 起的本地分鐘 VWAP 路徑有
50,048 筆有目標股數訊號，與這次 Tick 張／股錯誤不同；另有 30,454 筆
有目標股數訊號標記為 `missing_observed_09_01_minute_price`，也不能由
修正成交量單位變成成交。以 2/25 為例，215 筆缺 09:01 價格的目標訊號
目前沒有本地 09:01 來源；抽查 0056 的原始分鐘 K 棒首筆是 09:03。
沒有 09:01 來源時不得以 09:03、日開盤價或後見價格冒充 09:01 成交。
另一個抽查日期 9/3 的 376 筆缺價目標訊號涉及 370 個標的；
這些標的目前在保留的進場書及本地 09:01 分鐘來源均無可用價格。

## 已完成的程式與隔離驗證（盤中服務尚未重啟）

- 新來源列明確帶 `observed_volume_unit_0901`；舊收據按可信來源標籤解碼。
- live missed-opening 與 retrospective replay 共用相同換算；未知來源零容量。
- 全期間稽核現在分開統計來源、執行政策與已修正／舊版 Tick 單位；
  新程式產生的正確張數不會被誤報為舊版錯誤。
- replay 可直接重用持久 JSON 價格收據，對收據未涵蓋的標的只用本地分鐘
  資料補足，不增加永豐歷史查詢。
- 9/10 單日隔離演練位於
  `/tmp/stockagent-20260910-0901-volume-unit-local-overlay-candidate`：
  324 個收據價格，加上 507 個本地來源價格，沒有剩餘 actionable 價格缺口；
  五個模式各 270 個右標分鐘點，無新增永豐查詢。

## 尚未完成：正式歷史帳本修復

隔離單日演練以初始資金起算，不保留 9/9 前累積 NAV、持倉及後續結算。
把整段歷史一律改用 09:01 回放，也會覆蓋 9/15、9/17、9/21 等原本的
`causal_best_quote` 執行契約；全期間來源稽核也見到 9/10、9/11 的
部分模式使用該契約。這不能宣稱是對既有帳本的等價修復。
此外 9/22 仍在盤中，正式帳本有持倉；策略切換唯讀計畫因 1 億模式的
`mode_artifact_contract.json` 缺漏而失敗。不得繞過平倉、訊號身分、
全歷史起始 NAV、每分鐘曲線及原子切換驗收，直接改寫 `signals.jsonl`、
`fills.jsonl` 或 state snapshot。

後續正式修復必須以原帳本的每日期訊號 ID、執行契約、先前持倉與資金為
輸入，只更正上述 Tick 來源的容量計算，在隔離候選中重算受影響日期及其
後續狀態；驗證每個已完成交易日／模式都有 270 點、來源零缺口、非受影響
日期的填單身分與價格不變，且原始帳本保留可回復，再於所有紙上持倉平倉
後原子切換。今天盤中的錯誤不得冒充當時已有成交。

缺 09:01 的 30,247 組，下一來源是**收盤後**以現有
`fetch_shioaji_historical_stock_0901_vwaps` 按帳號流量預算查原始 Tick，
若仍無資料再查同分鐘 KBar；僅把時間戳確為 09:00–09:00:59 的成交
聚合成右標 09:01 收據。下載、核驗與隔離候選重播是不同步驟；
09:02/09:03 或當日開盤價不能在舊 09:01 契約下直接補成交。
