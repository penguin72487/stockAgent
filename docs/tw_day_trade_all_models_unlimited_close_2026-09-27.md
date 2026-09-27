# 全部當沖模型：13:30 不限容量平倉重算

## 使用者決定與範圍

2026-09-27 使用者指定：網頁收盤不管容量一定可以出，而且全部模型重算。
此決定取代同日 v7「13:30 仍受 50% 容量、未成交跨日保留」的尾盤設定。
這是使用者指定的**紙上研究假設**，不是交易所保證成交或 Shioaji 成交回報。

啟用帳戶為 `tw_day_trade_100m`、`tw_day_trade_multi_basis`、
`tw_day_trade_multi_basis_22`、`tw_day_trade_v8_annual_log_cash`。
已退役的 Attention 與 Projection-L1 v12 不重新啟用，不修改其殘倉為虛構成交。

重算期間：2026-02-25 至目前官方已完成的 2026-09-24，共 146 個交易日。
重用正式帳本每個日期／模式的 signal ID 與原始訊號檔案雜湊，不重新訓練、
不另選 checkpoint、不換模型決策。四帳戶沿原初始資本獨立從重算起日開始。

## 執行合約

- 09:00 官方開盤價決策／計算目標；09:01 起以右標分鐘 VWAP（或來源 Close）成交，沒有不利 tick。
- 09:01 至退出窗以前凍結原目標逐分鐘續單；同股票同分鐘共用 50% 容量，不能每筆委託各用一次。
- 原止盈止損、13:20 限價、13:24 市價至 13:25 的分鐘規則保留。
- **只有 13:30 終局平倉豁免容量**：以同日官方收盤或可驗證非試撮收盤成交價結清可交付殘倉。
- 缺價、停牌、非合法 tick 或尚未交付股票不製造成交。缺收盤證據保留可重試狀態，重啟也不重複平倉。
- 歷史收盤價由原始 TWSE／TPEx 報表、日期、SHA-256、有效正成交量驗證；不使用前日價或任意最後價替代。
- 本機紙上帳務重用原始費稅／FIFO；同日收盤清倉不再收取當日收盤後的融券轉換成本。
- 收盤不限量記錄 `source_close_full_deliverable_residual_no_capacity_paper_only_v1`，
  `broker_fill=false`；有效成交時點與實際補登時點分開保存。
- 每帳戶每完成交易日恰好 270 個 09:01～13:30 分鐘點；總驗收目標 157,680 點。
- 即時開盤仍保留原來的因果行情紙上路徑；本次沒有提交任何券商委託。

## 實作及驗收方式

1. `tw_day_trade_simulation.py` 新增 opt-in 終局策略，註冊訊號時固定本日合約；
   runner 只讀官方 retained report 補收盤證據。既有已提交日期不因改 YAML 偷換合約。
2. `rebuild_tw_day_trade_open_price_replay.py --terminal-close-unlimited` 在最後分鐘記帳前
   注入同日官方收盤證據，避免先轉融券再補平倉，也避免漏掉最後 13:30 費稅。
3. `audit_tw_day_trade_margin_replay.py` 獨立核對每筆不限量平倉的官方價格／SHA、
   時間、完整剩餘數量、去重與每日期平倉計數。其它委託照原 50% 共同容量檢查。
4. canonical minute revaluation 由成交帳本與分鐘來源獨立重估，逐點比對原重播。
5. 切換仍須舊活動帳戶 flat。先以 canonical official-close settlement 補登舊活動帳戶，
   保留完整回滾與原 JSONL 前綴；退役模式只能在 runtime enabled 集合和
   `configured_enabled=false` 同時證明退役時排除，未知模式仍阻擋切換。
6. 全期間來源與分鐘驗收完成後，同檔案系統原子交換；所有舊帳本保留。

隔離候選根目錄：
`artifacts/candidates/tw_day_trade_all_close_unlimited_20260927/`

## 訓練設定

新設定 `configs/deployments/tw_day_trade_v8_web_close_unlimited_v8.yaml`
繼承已固定的 v5 架構及年度切割與 v7 逐分鐘續單，只修改終局平倉及明示的不認列
假想認購權現金政策。輸出到新的 `artifacts/markets/tw_day_trade_v8_web_close_unlimited_v8`，
不續接舊 v5/v7 optimizer。舊 artifact 與設定不改寫。

本機訓練 executor 與 paper executor 的多／空、09:01 零量、後續分鐘續單及
收盤不限量案例，270 點權益逐點一致（rtol 1e-12、atol 1e-7）。
這不是遠端雙卡完整 fold 的訓練驗收；未完成遠端測試前不宣稱正式訓練就緒。

## 執行結果

原始官方收盤報表缺少 35 份（TWSE 20、TPEx 15）。已在各 dataset writer lock 下，
重用 `download_tw_public_data._download_historical_date`、原有 host-global limiter 及
官方回應驗證，只補缺少 raw，沒有重寫日價聚合表或訊號。
重補報表對現有日價表的 47,026 個非空 close 比對，差異 0。
收據：`artifacts/candidates/tw_day_trade_all_close_unlimited_20260927/raw_close_repair_receipt.json`。

**已完成重算及正式切換**：2026-09-27 07:35:09（Asia/Taipei），以原子目錄交換
部署至 `artifacts/live/tw_day_trade_simulation/`。四個啟用帳戶的 146 個交易日均於
收盤清倉，最終未平部位皆為 0。沒有重跑模型訓練或更換 checkpoint。

四個帳戶以原始初始資本重新連續記帳的結果如下；「累計報酬」分母為原始資本，
不是網頁篩選區間第一筆 09:01（已含入場費損）的估值。

| 帳戶 | 初始資本 TWD | 9/24 結束權益 TWD | 累計報酬 |
| --- | ---: | ---: | ---: |
| 一億 | 100,000,000 | 105,726,795.45 | 5.726795% |
| 多基底 | 10,000,000 | 13,330,403.53 | 33.304035% |
| 多基底 22 | 10,000,000 | 16,322,523.29 | 63.225233% |
| V8 年度 Log Cash | 10,000,000 | 12,206,197.48 | 22.061975% |

正式驗收收據位於新 live 目錄的 `promotion_receipt.json`、`rebuild_receipt.json`、
`minute_curve_receipt.json`。收據內原候選來源路徑是驗證當時的歷史 provenance；
原子交換後新資料位於 live，候選 `replay/` 現在保存的是切換前舊帳本，不能混讀。

回復來源完整保留，沒有刪除：

- 未補登任何舊部位前的原始帳本：`artifacts/live/official-close-settlement-lm29evg1/ledger`。
- 已按官方收盤補登舊活動部位的切換前帳本：
  `artifacts/candidates/tw_day_trade_all_close_unlimited_20260927/replay/`。

切換前完整來源／帳務驗收：146 日、error_count=0，最大帳務誤差
6.2528e-10 TWD；292 份官方收盤報表重新驗證。
舊活動帳戶 202 筆殘倉於 9/24 官方收盤補登後 flat（無缺價），原始完整帳本保留於
`artifacts/live/official-close-settlement-lm29evg1/ledger`。
退役兩模式的 positions 與金融 book fingerprint 保持相同；隔離目錄流程僅重綁
`executed_positions_path` 運行路徑，沒有修改其股數／費用／權益或虛構平倉。

最終合併回歸測試：559 passed（77.98 秒），涵蓋 terminal receipt、官方來源改寫拒絕、
重啟去重、零量／試撮拒絕、訓練／paper 270 點一致、續單、FIFO／費稅、來源、
重播／promotion、分鐘重估及網頁投影。完整輸出保存在候選根目錄 `pytest.log`。

獨立分鐘重估通過：83,843 個必要 symbol-date 全有來源，無補抓券商 API、無線性插值；
157,680 個策略點的差異點數 0，浮點最大絕對差 1.49e-8 TWD。
101,889 個分鐘權益點含至少一筆前次已觀測成交價的明示延續估值，**不等於每個持倉
每分鐘都有新成交**，也沒有憑延續估值製造成交。三條參考曲線一併保留／驗證。

另發現 Discord 的本機 `STOCKAGENT_SCHEDULED_MARKETS` 還固定舊五模式（含兩個
退役策略、漏 V8），造成實際只排程三個活動模式。只改該非敏感 allowlist 為目前四個
TW 當沖 ID，不啟用其他市場；服務重啟後已驗證 scheduled set 與 engine set 相同。

## 切換後實際服務驗收

2026-09-27 07:41 實測：

- 當沖引擎、Discord、read-only gateway 均 active；分鐘曲線 path／timer 已恢復。
- Discord Gateway 已連線，22 個全域命令同步成功；排程集合為四個啟用模型，
  `engine_run_id` 與引擎一致。重啟後日誌沒有 fatal／watchdog／traceback。
- 8766 原始 API 與 8770 網頁 gateway 均 `synchronized=true`、`revision_lag=0`、
  `ledger_integrity.ready=true`、`divergence_count=0`。
- 網頁 history API 的四模型各 39,420 點、146 日，每日逐分鐘時間鍵完整且唯一，
  恰為 09:01～13:30 的 270 點，`downsampled=false`；結束權益與帳本一致。
- 三條基準線合計 122,932 點；連同策略曲線 API 實際回傳 280,612 點。
- runtime 收據：候選根目錄的 `runtime_status_acceptance.json` 與
  `runtime_history_acceptance.json`。

這次是**執行假設重播**，不是策略／checkpoint 替換。額外執行的
`switch_tw_day_trade_strategy.py verify` 要求 model replacement 專用 receipt 與新
model-scoped 訊號目錄，因此不適用保留原 584 個訊號 ID 的本次工作；其
`target_summary_outside_model_scoped_output` 後提早停止，造成後續 session-count／
fingerprint 集合未建立的連帶失敗。本次不偽造 replacement receipt、移動原訊號或
以此宣稱換模驗收通過；採 canonical replay promotion 的全來源／帳務／曲線閘門
與上述實際服務驗收。該額外檢查輸出留在 `runtime_strategy_verify.log`。

## 尚存限制（沒有隱藏）

- V8 的 9/24 仍有兩筆 `price_limit_unavailable`，因缺當日合法漲跌停界限而未進場；
  原 `entry_partial` 警告保留。這是入場資料缺口，不是本次收盤清倉失敗，所以 API
  的整體 `health` 仍為 `degraded`，不能宣稱所有來源都健康。
- 週日 off-hours 的 startup GPU warmup 按原交易日／盤前時窗政策 deferred／pending；
  本次沒有強制跑盤前推論，也沒有把週末服務連線當成下一交易日 09:00 保證。
- 「收盤不限容量」只是一項紙上研究假設，不能宣稱真實市場必然成交；來源價格、
  合法 tick、非停牌與可交付庫存仍須成立。缺證據時待補，不製造收盤價。
- 遠端正式雙卡 DDP 訓練未在本次執行；本機 executor parity 不等同遠端完整 fold 驗收。
