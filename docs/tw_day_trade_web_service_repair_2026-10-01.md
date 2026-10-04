# 2026-10-01 當沖網站／執行引擎修復

## 1. 執行進度與驗收結果

截至台北時間 10:23:27，四個啟用模型都已寫入當日訊號、完成進場續單，並恢復每分鐘估值；各模式最新 mark 為 10:23。網站與 Discord 的帳本版本落差為 0；帳本 `divergence_count=0`、`ready=true`。這是服務恢復的驗收，不是今天開盤準時成功的證明。

- 模式：`tw_day_trade_100m`、`tw_day_trade_multi_basis`、`tw_day_trade_multi_basis_22`、`tw_day_trade_v8_annual_log_cash`。
- 最終引擎重新載入：10:14:37，PID 1551092；截至 10:18:34，`NRestarts=0`，啟動後日誌無 watchdog／fatal error。
- 公開 gateway：09:56:01 載入估值來源欄位，PID 1526156。最終引擎重新載入時沒有再次重啟 gateway。
- Discord：原 PID 898 保留，連線正常；沒有為本次修復重新登入或重啟 Discord。
- `simulation_only=true`、`production_order_possible=false`。沒有送出券商真實或模擬委託；所有成交仍是現有本地 paper 帳本。
- 483 項相關 Python 測試通過（最後一次 33.36 秒）；40 項前端行為測試通過；Python 編譯檢查、前端語法及本次修改的 diff 空白檢查通過。

網站仍顯示 `degraded`：今天確實未在 09:00:15 前完成所有模式的 causal live 訊號及執行提交。此警告沒有被刪除或改成成功。

## 2. 從資料流定位根因

訊號發布、來源查詢、紙上成交、帳本提交、網站投影與 Discord 同步是不同階段；HTTP 200 和服務 `active` 不能證明後面五個階段都完成。

1. 四個不可變訊號已在約 09:00:15–09:00:16.6 發布，但主執行迴圈自 09:01:01 同步查詢 820 檔的 09:01 歷史成交價。必須整批完成才交付，網站因此仍顯示等待訊號，帳本也沒有更新。
2. 820 檔包含官方有開盤、權重非零、實際卻不足一張的股票。來源查詢成本不等於需要執行的委託成本。
3. 多模式共用一個全域等待條件；只有少數必要股票的小模式也被大模式的缺價拖住。查詢工作持續發出 watchdog 通知，掩蓋帳本迴圈沒有進度的事實。
4. 持倉行情集合超過 200 筆訂閱上限；原來每秒只建立／替換 25 筆，完成輪替所需時間與 10 秒可用報價期限不匹配。單純加速輪替也無法讓所有股票同時具備可成交行情。

舊版整批查詢於 **09:16:11 自行完成**，820 檔價格才落盤，隨後才逐模式登記當日反事實執行。這早於新程式部署，不能將該次完成歸功於本次修復。

交通帳本記錄該 consumer 共 820 次請求，請求 duration 中位數約 46.31 ms、加總約 279.89 秒。主迴圈停滯約 909 秒；差額未逐段完整剖析，因此不把全部等待都稱作網路耗時。

## 3. 已實作的修正

### 有界、可恢復、逐模式釋放的來源查詢

- 同一個 process-local Shioaji 連線增加歷史查詢背景工作；每批最多 8 檔，先處理必要集合較小的模式。主迴圈獨立估值、提交帳本與發送 watchdog，不新增登入連線。
- 本地真實分鐘資料優先；本地已解出價格立即寫入 durable receipt。遠端每解出一檔同樣先寫入並發布進度，不再等下一檔回應或整批完成。
- 新 receipt 為 schema 3、`bounded_incremental_per_mode_source_queries_v3`；保留真實來源、價格方法與已嘗試／尚未嘗試／需重試集合。失敗、流量延期與官方確實空資料分開處理。
- 每個模式只等待自己的必要股票；另一模式的缺價或錯誤不會形成全域 barrier。舊不完整 receipt 不能因錯誤的 attempted metadata 而永久跳過缺價。
- 即時 recovery 使用 SDK `timeout=0` 回呼，等待實際結果而非 placeholder。Python Event 有界等待讓出 interpreter；逾時回呼不會污染後續請求或交易日。離線預設同步介面保留。
- 歷史查詢跨批共享既有供應商限速配置的 80% 額度；此 process 內的歷史限速不是所有行情、帳號或網站請求的全域額度證明。

### 只減少真正不需成交的查詢

回補需要的整張目標沿用 `floor(abs(weight) × frozen_session_NAV / official_open / lot) × lot`。沿用實際凍結的複利資金基準，不拿初始資金代替。持有跨日部位、有企業行動 claims、未知開盤價或 NAV 的情況保留原範圍。

以今天實際凍結 NAV 做查詢範圍 dry calculation：100m 為 817 → 352，多基底 31 → 21，多基底22 為 5 → 2，V8 為 59 → 16。這是新規則的必要集合計算，不宣稱今天已完成的 820 次查詢因此減少。

### 行情輪替與估值分離

- 尚未完成訂閱目標 reconciliation 時用 0.1 秒步進，完成後維持 1 秒；200 筆上限、25 筆批次、既有 dwell 與行情期限不放寬。
- 每分鐘、開盤保護時窗後，只對持倉中缺少平倉方向報價者讀取 Snapshot；只附加 `valuation_*` 欄位，要求當日來源時間及有效價格，明確試撮或過期觀察拒絕。
- 新估值證據標籤為 `indicative_snapshot_book_only_not_execution_evidence_v1`；Snapshot 未提供的非試撮資訊仍未知。原有 `bid/ask/last/simtrade`、分鐘成交量與執行證據不被替代，停損、停利和成交引擎不能使用這些參考欄位。
- 帳本 mark 保存估值來源／證據，API 提供 `indicative_valuation_position_count`；網頁總覽、模型卡片與警示明確顯示「參考快照估值（非成交證據）」，來源筆數改變也會觸發畫面更新。

此前 [09/24 即時報價修復](tw_day_trade_live_quote_recovery_2026-09-24.md) 禁止的高頻 Snapshot 紙上成交沒有重新啟用。本次只增加持倉估值備援，不改券商成交證據。

## 4. 實測、帳本保全與尚未解決的限制

### 本機 API 暖快取

10:18:34 後逐 endpoint 暖一次，再各量測 20 次 loopback HTTP 請求；包含讀完整個回應，不包含外網、瀏覽器繪圖或新訊號推論。

| Endpoint | p50 | p95 | 回應大小 |
| --- | ---: | ---: | ---: |
| `/tw-day-trade/api/revision` | 1.602 ms | 1.954 ms | 1,354 bytes |
| `/tw-day-trade/api/status` | 2.492 ms | 3.047 ms | 72,558 bytes |

這不是任何網路、硬體或冷啟動的延遲保證。公開路由和私有 API 都返回當日模式；靜態 `app.js` 已提供新的估值來源文案。前端驗收為元件／狀態機測試，沒有宣稱完成真瀏覽器視覺或外網測試。

### 沒有為了修服務重寫金融紀錄

- 四個當日 signal ID 與凍結 `session_sizing_nav_twd` 在重新載入前後一致；未 rearm、重算或刪除既有部位。
- 4,442,867,645 bytes 的 `signals.jsonl` 以串流 SHA-256 驗證保持不變：`321a9d9bdba6bf522e6fe2c307b8226888e12ed78f70bdc71470af2e3a0093f8`。
- `fills.jsonl` 重新載入前既有 291,190,218 bytes 前綴雜湊保持不變；之後只保留正常引擎 append。
- 當日 09:01 來源 receipt SHA-256 保持 `c94ab45db50e341e0861ae40ddfb5a506ceadf81a0e31d729a9a394503ce99d4`；來源交通紀錄仍 820 次，未因重新載入重抓。
- 沒有修改模型、權重、手續費、進場時鐘、容量與收盤規則。新增的參考估值不是 broker fill，也不等同逐檔都有新鮮可成交串流。

### 必須保留的真實缺口

- 10:15 mark：四模式持倉數為 350／18／2／15，未完成進場股數均為 0；stale 持倉為 1／1／0／1。沒有為清掉警示強制成交。
- 來源檢查顯示 2305 多單跌停缺 Bid、4174 與 6877 空單漲停缺 Ask；10:18:34 帳本仍是這三筆 stale。不存在可用平倉方向報價時，不用 Last 或反方向買賣價冒充可清算價格。
- 10:15–10:18 每分鐘串流真正 available 僅 171／145／140／135 檔，requested=361；Snapshot 估值備援不能增加這個執行 coverage。提高完整即時成交覆蓋需要供應商授權或另一個可證明非試撮、因果時鐘與深度的資料源，未在本次新增。
- 開盤 SLO 仍 `false`。來源覆蓋、盤前資料與推論發布的上游開盤延遲尚不能由本次 consumer 修復保證；下一交易日須重新驗收，不能承諾每天必定 09:00:15 前成交。
- 今天早盤中斷造成分鐘 mark 缺段；10:19 投影為 260／316 個模式分鐘紀錄。尚未補造、插值或宣稱曲線完整。完成交易日仍需由既有分鐘曲線維護，以真實價格驗證每模式 270 個 09:01–13:30 右標記分鐘後才可宣告歷史曲線完整。

## 5. 可重跑的相關驗證

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q -s \
  test/test_tw_day_trade_recovery_responsiveness.py \
  test/test_tw_day_trade_simulation.py \
  test/test_tw_day_trade_open_price_replay.py \
  test/test_quote_provider.py \
  test/test_day_trade_intraday_discipline.py \
  test/test_tw_day_trade_dashboard_session_projection.py \
  test/test_serve_tw_day_trade_dashboard.py \
  test/test_public_dashboards.py \
  test/test_shioaji_traffic_ledger.py \
  test/test_day_trade_execution_reconciliation.py

node --check services/tw_day_trade_dashboard/app.js
node --test test/test_tw_day_trade_components.mjs \
  test/test_day_trade_rollover.mjs test/test_dashboard_core.mjs
```

涵蓋網路阻塞與不重疊工作、逐檔 durable 進度、跨模式隔離、斷點重試、真實 NAV 整張篩選、跨批限速、回呼逾時、試撮／舊日期／未來報價拒絕、估值不可觸發成交、投影與前端標示。未執行全專案測試、GPU 訓練或整個交易日的未來實盤驗收。
