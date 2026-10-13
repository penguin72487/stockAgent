# TW 開盤官方交易名冊修正與驗收（2026-10-13）

## 結論與範圍

目前驗收：官方掛牌 **2,311**，停止交易 **8**，可交易 **2,303**。
範圍沿用現有上市／上櫃普通股與 ETF 契約，不含興櫃、權證、特別股、ETN、債券等其他證券。
不是把總數寫死成 2,303；每個交易日依官方成分與有效停復牌狀態產生有版本的名冊。

舊 2,303 是上一模型資料列 `alive_mask` 聯集，不能代表當日市場。
新舊總數恰好相同，但必須 **移除 30 檔、加入 30 檔**。
[獨立比對與原始名單差異](../artifacts/operations/tw_opening_latency_20261013/official_universe_acceptance.json)。

名冊契約 `tw_official_current_regular_stock_etf_universe_v3`，整合觀測時間
`2026-10-13T10:10:25.773878+08:00`，fingerprint
`34e5459456d25b986bdccb0da852def52fff051f9f4b97c097feee94dbd98cfd`。
上市可交易 1,297、上櫃可交易 1,006。
這是當前名冊，不回填成 09:00 當時已知，也不改寫今天原始訊號的覆蓋率。

## 從來源到模型的契約

1. [TWSE ISIN 上市名冊](https://isin.twse.com.tw/isin/C_public.jsp?strMode=2)、
   [TPEx ISIN 上櫃名冊](https://isin.twse.com.tw/isin/C_public.jsp?strMode=4)：
   共用既有 downloader 的證券範圍與 HTML parser，排除未到上市日的標的。
2. [TWSE 暫停／恢復交易](https://openapi.twse.com.tw/v1/exchangeReport/TWTAWU)、
   [TPEx 暫停／恢復交易歷史](https://www.tpex.org.tw/openapi/v1/tpex_spendi_history)：
   依標的與有效時間還原最後事件，復牌時點為停牌區間的排除上界。
   TPEx 分列公布停牌與復牌；不能把舊停牌列的空白復牌欄當成永久停牌。
3. [TPEx 現行交易狀態](https://www.tpex.org.tw/openapi/v1/tpex_cmode)、
   [TWSE 現行停止買賣](https://www.twse.com.tw/rwd/zh/violation/stop?response=json)：
   區分財務停止買賣與暫停交易；較新的明確復牌事件可覆蓋較早的狀態日。
4. 減資／換股停牌沿用已驗收的 `tw_share_replacement_reference` collector。
   驗證當日 lifecycle coverage、614 列輸出、Parquet SHA、原始來源 manifest SHA；
   不另建立 MOPS crawler，也不把費用／現金返還尚未完成誤當沒有停牌證據。
5. 保留各來源原始 bytes、SHA 與真實取得時鐘，完整驗收後才原子發布名冊。
   `[1589, 2067, 3625, 3632, 4155, 4804, 6167, 6173]` 為本次範圍內停止交易標的。
6. 所有已配置取價 provider 以 **完整官方可交易名冊** 平行取價；
   各模型開盤覆蓋率以 **官方可交易名冊 ∩ 原模型 symbol axis** 計算。
   不改 checkpoint、模型維度、原始訊號權重、整張規則、費用或 90% 開盤輸入門檻。

| 模式 | 原模型標的 | 官方交易交集 | 官方但模型未涵蓋 |
| --- | ---: | ---: | ---: |
| 當沖 1 億 | 2,744 | 2,289 | 14 |
| V8 annual log cash | 2,754 | 2,298 | 5 |
| Multi-Basis 22 | 2,746 | 2,290 | 13 |
| Multi-Basis | 2,745 | 2,289 | 14 |

不以成交量決定名冊成員。沒有第一筆成交與沒有有效開盤價是價格狀態，
不是下市證據；bid/ask 中點不改稱開盤價，未知不填入昨日價或零價。

## 恢復與效能

- 08:15 準備與約 08:45 final-arm 沿用既有排程；正式 09:00 路徑只讀已驗收名冊，
  不下載大名冊、不等 HTTP、不在 model mutex 裡建名冊。
- 每日 ISIN bytes 與短期停牌狀態分開快取；成功來源保留原取得時間，來源失敗只重試缺口。
  壞 schema 不快取成正常，保留舊驗收 pointer 與失敗證據。
- 共用 canonical dataset lock 防止跨程序雙 writer，atomic publication 防止半份資料，
  名冊日期／SHA／count／type 不符時明確 `TWTradingUniversePending`，不退回歷史 alive-mask。
- immutable 官方索引與模型交集按 fingerprint 快取；每個 caller 拿到自己的 mask/list，
  不會互相污染。名冊變更使 price prefetch/request identity 失效。
- 開盤 receipt 只以官方 requested scope 算有效價數；範圍外舊觀測仍保留。
  scope 改變前的 receipt 歸檔，不刪價格證據，也不改歷史訊號與帳本。

500 次／模式，本機完整模型 axis：membership mask P50 從 **1.44–1.55 ms**
降為 **0.166–0.174 ms**，P95 **0.286–0.409 ms**；request identity P50
**0.367–0.389 ms**。首次完整 source SHA 驗收 **59.79 ms**。
[基準](../artifacts/operations/tw_opening_latency_20261013/official_universe_acceptance.json)、
[新版](../artifacts/operations/tw_opening_latency_20261013/official_universe_optimized.json)。

首次官方大頁網路取得曾為 51.14 秒；本次保留已成功取得的同日 bytes、
結合最新停牌與 canonical 參考檔後，解析／發布為 5.29 秒。
後者不是「全來源重新上網只要 5.29 秒」，兩者都在開盤熱路徑之外。
沒有用這個 bounded benchmark 宣稱真正開盤→訊號→券商成交已達 1 秒或 1 ms。

## 上線與驗收證據

只重啟 Discord 與唯讀 public gateway；盤中 execution engine 未重啟：
`MainPID=208392`、InvocationID `34227ee9d55a401f9471c032d43bc9e9`、
engine run `87de6ae919854689a548d1fc6168cd7a` 維持相同。
新 Discord 的四模式均讀回 `already_recorded_after_restart`，沒有再推論一次。
[前後獨立 pointer／weights SHA 讀回](../artifacts/operations/tw_opening_latency_20261013/official_universe_runtime_readback.json)。

公用 status／summary 均保留官方名冊 DTO；名冊檔案新 generation 也會使 status 快取失效，
不必等交易引擎另一次經濟提交。前端 `app.js?v=97`，不新增全域 rate/concurrency 限制。

實際 Chromium headless-shell 檢查官方 KPI、核心 API、paint 與響應版面：
1366×768 與 390×844 均為 2,303 檔、無水平溢出與 JavaScript exception。
[筆電驗收](../artifacts/operations/tw_opening_latency_20261013/official_universe_visible_browser/official-universe-1366x768.json)、
[手機驗收](../artifacts/operations/tw_opening_latency_20261013/official_universe_visible_browser/official-universe-390x844.json)。
兩張截圖均保留；官方名冊卡明示「非當日原始訊號覆蓋率」。

最終聚合回歸：**763 個 Python 測試、33 個 Node 測試通過**，並通過兩支前端腳本語法檢查。
範圍為名冊、來源合併、開盤輸入、Discord、模擬執行、dashboard 與 public gateway，
不是全 repository 測試。
[完整 run log](../artifacts/operations/agent-workflow/runs/official-universe-complete-scope-20261013T024256-98b0cc0e/run.log)。
較早聚合驗收的單一失敗是 UI 測試仍要求 `app.js?v=96`；已隨新版 cache-busting 更新至 97，
重跑全部上述範圍，不刪除先前失敗紀錄。

重測入口：`scripts/benchmark_tw_trading_universe.py`（指定四個 `--signal-pointer`），
`scripts/verify_tw_trading_universe_browser.mjs PORT BASE OUTPUT WIDTH HEIGHT`。
手動來源刷新：`scripts/refresh_tw_trading_universe.py`，`--force` 才強制重新上網；
一般失敗恢復保留已成功來源，所有接受／失敗版本與 workflow logs 留存。

## 尚未解決的邊界

- 10:43 的真實價格修復為 2,197／2,303，106 檔仍缺開盤價；不是資料已完整。
  此次來源回報 105 檔無開盤成交、1 檔來源未解析；不能因此把這 106 檔排除名冊。
  repair 從 attempt 29 的 2,185、10:37 的 2,194 持續取得更多價格，
  不推論、不下單、不偽造成交。
- 本輪公網 IPv4 HTTPS probe 逾時，外部 web probe 亦失敗；localhost 成功不等於公網驗收。
- 完整頁面 browser audit 未通過：完整歷史 signals/positions 的首次 pending 超過 30 秒；
  新官方 KPI 的核心驗收通過不能取代完整頁面驗收。
  當日 filtered positions／signals 另實測 200、約 200／46 ms，但不代表全歷史冷讀速度。
- 系統原本的 `degraded`、guardian `FAILED` 與未完整成交仍忠實保留。
  新 Discord Gateway/core 為 ready，但其 startup inference-warmup 仍 pending；
  當日舊盤前 READY receipt 不能拿來證明新程序已熱身。
- 今日四模式原始訊號在 09:34:05～09:34:11 發布，沒有改成正常 09:00 訊號。
  模擬帳本證據、行情證據、券商 order/deal callback 仍是不同契約。
