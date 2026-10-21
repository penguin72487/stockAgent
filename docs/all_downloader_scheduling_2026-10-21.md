# 全來源下載器排程續修與實測 2026 年 10 月 21 日

## 本輪結果

已部署 Binance 的依視窗扣重、OKX 分鐘追新的雙端點路由、公開目錄的有界平行，
以及台股清冊的等待狀態修正。
OpenBB 已重啟載入背景監控，沿用原計畫繼續下載。另修正五個公開／加密貨幣
adapter 在最後一次 HTTP 失敗後沒有保留共用冷卻的問題。

OKX 的 514 個現行商品原生追新，由修正前約 53.94 秒降到連續三輪
27.32、26.52、26.18 秒；包含其他來源的正式 intraday 工作由約 53–59 秒降到
30–31 秒；再減少 Binance 的無效配額扣重後，連續觀察到 27–28 秒的完整工作。
以上是每輪工作時間，不含原 timer 間隔，也是自然排程的前後觀察，
不是控制網路與其他作業負載的 ABBA。
固定 300 根的正式 API ABBA 另證明兩端點原始九欄與正規化結果完全相同。

另已修正 OpenBB 稽核誤把普通收據發布視為路徑替換而中止的問題；
實際 directory／symlink 變更仍拒絕通過。

**仍未達成所有來源都只剩 API 瓶頸。** OpenBB 全量統計與小檔稽核、跨 lane
writer 協調、部分來源／SDK 錯誤，以及 FinLab 的一般資料優先閘門仍需處理。
本輪沒有刪除、搬移或重建歷史資料；factorized 及其他視窗負責的檔案保持唯讀，
交易與 bidask 服務沒有重啟。

## 排程的基本邊界

`有效吞吐 ≤ min(API 可用配額、網路回應能力、驗證解析能力、durable 寫入能力)`。
獨立來源可重疊工作；同帳戶、IP 或官方 endpoint family 必須共用限制，
不能把增加 worker 當成增加配額。所有 API retry 本身也算請求。

減少等待要先找關鍵路徑：本輪原本最慢的是 OKX 的歷史端點節拍，
不是 GPU 或把所有 HTTP 改成非同步。改用合法、資料範圍相符的較快端點後，
完整 intraday 工作的關鍵路徑移向其他來源，而非宣稱整個工作也加速兩倍。
未發布、休市、來源空值、權限阻擋及磁碟不足分開保留，不能為提高 calls 而改標成功。

## 全來源盤點與目前邊界

本次 01:57 台北時間快照包含 255 個下載／更新／回補與共用 Python 檔案，
靜態辨識 65 個有網路存取，其中 55 個可沿已安裝服務的入口與 import 關係找到。
檔案數不是獨立 provider 數；SDK 動態呼叫及實際成功覆蓋仍需各自收據。
02:36 再盤點為 256 檔，增加的是另一視窗的 `downloader/news_fulltext_watchmedia.py`；
其餘上述網路／服務／旗標計數不變，本輪沒有修改該新增檔案。
完整入口、owner、timeout、retry、限流及收據欄位位於
[最新盤點 CSV](../artifacts/data_quality/all_downloader_scheduling_2026-10-21/inventory_final/downloader_inventory.csv)。
五個靜態旗標仍是既有 FinMind 帳戶 limiter、台股共用 parser／收據、
期貨帳戶 admission 與工具安裝入口，不直接判為失控下載器。

| 來源群組 | 本輪操作與觀察 | 尚未驗收的部分 |
| --- | --- | --- |
| Binance／OKX／Bybit | 原生 intraday 各自平行；Binance 新扣重與 OKX 新路由已採用。Binance 29.72→約 17.53 秒，OKX 53.94→約 26.18 秒；Bybit 本輪初始觀察約 18.39 秒 | 各來源全歷史完整性、所有 feature 回補與跨 lane writer 協調 |
| OpenBB 與其 EIA／OECD／FRED 等 providers | 背景完整監控與每分鐘輕量檢查分離；保持既有 provider bucket 與原計畫 | 全量 SQL 統計、數百萬小檔完整性、25 個 repair 項目及舊失敗；不是全部最新資料 |
| FinMind | 唯讀時間索引：02:17:54 前滾動 15 分鐘 4,610 calls，1 小時 16,762 calls；前者 4,607 次為美股分鐘、3 次為新聞 | 此取樣不是已達方案上限的證明；維持既有資料優先順序與帳戶 owner |
| FinLab | 一般資料正在推進；02:22:57 有 16 個當時可做項目、192 個 blocked；分鐘補充工作受一般資料優先閘門限制 | 數字隨排程變動，blocked 不全等於來源空值；分鐘狀態另有約八小時舊快照。本輪保留另一組工作的程式 |
| 永豐與台股公開資訊 | 原 owner、交易日曆、發布時間、API 登入及資料發布契約保留；修正清冊等待時的無效自動重啟 | 公司換股參考覆蓋仍止於 10 月 20 日，當日清冊等待來源；不繞過生命周期驗證 |
| TAIFEX／公司事件／公開公告 | 正式 owner、timer 與共用 HTTP 納入清冊，未另開重複管線 | 本輪沒有全歷史重跑或新增完整性結論 |
| Coin Metrics／公開宏觀與加密背景／Dune／SEC ETF | 最後一次可重試 HTTP 失敗也會延後原共用 bucket；下次正式程序載入新程式 | 不把 402、403、來源空值或權限改成成功；本輪沒有強制重抓全部歷史 |
| NOAA／BEA／Census／Finnhub／FIRMS／CWA／MOENV 目錄或快照 | 原生服務已採用有界平行，七份 retained objects 獨立 SHA／bytes 驗證通過 | 目錄不是歷史 observations；AirNow 仍需範圍，api.data.gov 是共用 credential hub |
| 新聞／TEJ／其他正在修改的來源 | 正式入口納入盤點；保留其原 owner、容量、授權及來源身份驗證 | 本輪唯讀，不以服務 active 當作已抓全或效能驗收 |

## OKX 使用正確端點提高追新吞吐

官方 `GET /api/v5/market/candles` 為 40 requests／2 秒、IP 限制，
只覆蓋最新 1,440 根；`history-candles` 為 20 requests／2 秒。
兩者不是同一配額，也不能拿前者宣稱提供全部歷史。
參考 [OKX 官方端點文件](https://app.okx.com/docs-v5/en/)。

`OkxClient` 現在依請求時間選路：只對 1m、已結束、整個 page 位於保留窗內的
請求嘗試 recent。距保留邊界預留 300 根只影響路由，不延後資料可用時間，
較舊頁面仍立即走 history。回應必須達請求頁數、嚴格倒序、
無重複／跳分鐘，且每根 `confirm=1`。否則同一 cursor 回退 history，
不提交部分 recent page。403、429、網路或其他 transport 失敗直接沿原錯誤契約回報，
不藉切換端點繞過 backpressure。

recent 的共用 IP bucket 為 0.05 秒，history 為 0.1 秒；不同商品不另分配額。
使用者明確設定更慢 interval 時仍遵守。原分頁 cursor、原始值、成交量單位、
closed-minute 時鐘、單一 writer、hot-tail 增量與歷史修補均不變。
summary 新增路由契約與命中／回退計數，原 feature schema 不變。

正式 BTC-USDT-SWAP ABBA 共四頁、每頁 300 根，全部原始九欄、
正規化欄位／數值／型別及 SHA 相同。單頁 recent 約 0.14–0.15 秒，
history 約 0.10 秒；**提升來自較高合法配額，不是單次 HTTP 較快**。
證據：[固定視窗比對](../artifacts/data_quality/all_downloader_scheduling_2026-10-21/okx_recent_native_probe.json)。

三輪全市場工作皆為 514 商品成功；第二輪 24 頁、第三輪 1 頁依法回退 history。
第二輪因回退共有 514 次 recent 加 24 次 history grants，不能宣稱每輪實際 calls
一定減少。正常一頁即可涵蓋追新窗；沒有全市場 OHLCV 批次端點時，每商品一頁
仍是這種查詢方式的下界。

獨立讀回持有原 dataset 的短暫共享讀鎖，1.71 秒內驗證全部 514 商品、
各最新 8 根共 4,112 列：來源 identity 穩定、已結束分鐘、日期唯一順序、
OHLCV 有限非負、價格高低關係，以及 `Trading_Volume == okx_volume_quote`。
不把這 4,112 列當作約 4.72 億個邏輯歷史列的完整驗收。
證據：[獨立讀回](../artifacts/data_quality/all_downloader_scheduling_2026-10-21/okx_recent_independent_readback.json)。

## Binance 短視窗不再付出長頁面配額

USD-M klines 官方以 `limit` 扣重：1–99 根為 1 weight，100–499 根為 2 weight。
長歷史仍使用最佳 rows／weight 的 499 根，而短追新與最後一頁改成
`min(剩餘分鐘視窗格數, 499)`，依實際頁數取得正確 weight。
參考 [Binance 官方 Market Data](https://developers.binance.info/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)。

原本每商品只需少量新分鐘，卻固定按 499 根扣 2 weight。原生 575 商品這輪
仍為 575 次 grants、全部 updated，但由 1,150 降成 575 weight；工作由約
29.72 秒降到連續兩輪約 17.55、17.53 秒。這是減少配額浪費，不是把兩次 HTTP
冒充一次，也不是放寬 exchangeInfo 的每分鐘上限。

起訖時間、閉合分鐘過濾、分頁進度、稀疏資料與寫入原子性不變。
新分頁上限、實際 weight 與 short-page 結束條件一致，不能改 limit 卻保留舊停止判斷。
1、99、100、499、500、1,000 格及稀疏視窗都與原固定 499 頁面 oracle 比對，
數值／型別相同，HTTP 次數不增加，總 weight 不增加。
`kline_limit`／`kline_request_weight` 保留 bulk 最大值，新增版本化的 page policy，
實際用量仍以 `limiter_activity.grant_cost_total` 為準。

獨立讀回以原 dataset 短暫共享讀鎖，驗證全部 575 現行商品、最新各 8 根共
4,600 列，2.26 秒完成；canonical OHLCV 驗證、closed／唯一／順序時鐘、
`Trading_Volume == binance_volume_base`、每檔 identity 與 summary fence 均通過。
初次非阻塞讀鎖碰到正常 writer，未讀取或修改來源；第二次有界等待後驗證成功。
這不是全部歷史列或缺口驗收，也沒有改用 OKX 的 quote-volume 單位。
證據：[Binance 獨立讀回](../artifacts/data_quality/all_downloader_scheduling_2026-10-21/binance_weight_independent_readback.json)。

## 公開目錄平行與可靠性

`download_keyed_public_catalogs` 由跨 provider 串行改為有界 thread pool。
預設 requested workers 為 4，但仍受請求票數與 32 MiB raw-response 在途預算限制；
正式服務每回應上限 16 MiB，因此實際為 2 workers。512 MiB cgroup、原 dataset lock、
原子 source／receipt 發布、憑證遮罩、目的地主機驗證及磁碟保護均保留。

請求票以鎖保護的 `claim(provider)` 原子扣除，包含 retry 與 CDN hop。
每個 provider 的計量獨立，輸出清單順序穩定，但不要求獨立 HTTP 依序發送。
到期時鐘、共用冷卻與快取 SHA 驗證保留，不使用 force 製造測試流量。

離線完整流程 ABBA 包含 HTTP、解析、正規化、原子物件／收據、SHA 讀回與下一輪快取。
原測量 serial 中位 0.853 秒、parallel 0.313 秒，約 2.72 倍，資料 bytes 與數值相同，
下一輪全部零 requests；不是網際網路或全部來源吞吐量的倍數。
證據：[完整流程測量](../artifacts/data_quality/all_downloader_scheduling_2026-10-21/catalog_schedule_abba/result.json)。

原生服務 02:17 只抓取兩個到期快照，其餘沿用快取；2.70 秒完成，CPU 1.83 秒、
記憶體峰值 42,938,368 bytes。七份物件 SHA／大小與各 provider 請求總和獨立驗證成功。
這證明本次 bounded scope 正常，不代表全球天氣、經濟與證券歷史已抓完。

五個 adapter 的 429／可重試 5xx 等 HTTP 最末次失敗，現在也會保留來源的
`Retry-After`／退避至既有共用 bucket；不增加重試上限。錯誤 body 有界讀取並關閉，
即使本機 pacing 狀態失敗也不留下連線。Dune 的 402／訂閱限制與其他永久錯誤仍保留。

## OpenBB 與台股清冊的原生採用

OpenBB 的完整 manifest SQL 掃描可能耗時數分鐘；同步等待會讓原本每分鐘的
磁碟、RSS、FD 與 stall 檢查一起停住。現在每次僅允許一個有 600 秒期限的
背景完整掃描，輕量 watchdog 繼續執行。結束驗收必須等 downloader 結束後
再開始新的完整掃描，不能採用舊背景 snapshot 宣稱完成。

02:21 只重啟此可續跑下載 owner；原計畫 `56e52165c96966261d74547b`、
封存截止 2026-07-18 與官方 bucket 均保留。原生紀錄顯示 02:28 開始背景掃描後，
02:29、02:30、02:31 仍持續 watchdog。完整統計的計算成本尚未消除。
OECD 維持官方每小時 60 次資料下載，不能提高 worker 跨過此限制。
參考 [OECD 官方最佳實務](https://www.oecd.org/en/data/insights/data-explainers/2024/11/Api-best-practices-and-recommendations.html)。

上輪容量不足的觀察已過時：本次開始約 292 GiB 可用，已高於原 100 GiB 保護線，
因此不再將 OpenBB 未完成歸因於舊容量快照；沒有調低保護線。
現有完整檔案 audit 是舊證據，不能取代本次 source／ownership 全量驗收。
另一次高 CPU 程序已核實屬另一視窗的 factorized 驗收，不計入 OpenBB 負載。

02:34 的完整狀態快照當時已無 pending／running／retryable；保留 11 failed 與
25 repair，所以 complete=false。02:35 正式 owner 進入全量 Parquet audit，
未將「無可重試工作」誤當「全部來源與檔案已完成」。

02:41 的這次全量 audit 在 ownership index 階段中止：根目錄正常更新收據使
mtime／ctime 改變，舊 guard 誤報整批路徑變更，未發布新的 audit 成功收據。
修正後仍逐批驗證 device／inode／mode；只有時間戳變動時，整批再經原逐路徑
resolver 比對，必須與快速路徑完全一致，且 oracle 期間不得再發生 namespace／
時間戳變動。真正的目錄替換、leaf symlink 替換及驗證中再次改動均 fail closed。
不忽略錯誤、不採 TTL 跨批快取，也不修改任何 source shard。

02:47 已重新啟動同一正式 owner，後續 audit 將讀取修正後程式；02:48 的原計畫快照有 527 pending、
9 failed、25 repair，仍需等待原生排程與全量驗收，不把 02:34 的零 backlog
快照當作現在沒有工作。

修補後以 12,000 份正式來源檔名做 ownership 階段 ABBA：相較逐檔 `Path.resolve`
oracle，中位 2.863→1.116 秒，計數／issue samples 完全相同，來源 identity／SHA
與臨時副本回收後讀回均不變。這是保留既有快速路徑的有界驗證，
不是新修正相較上一快速版本的加速比，也不是全量 file audit。
證據：[ownership ABBA](../artifacts/data_quality/all_downloader_scheduling_2026-10-21/openbb_path_oracle_abba_v2.json)。

台股清冊 `75` 代表持久化 waiting_source／waiting_calendar 等待，
不再視為 crash 連續重啟。只安裝 universe unit／timer；原生仍保留 waiting receipt，
systemd Result=success、ExecMainStatus=75、NRestarts=0，後續由原 preopen checkpoints 重查。
systemd 成功只代表等待處理正常，不代表當日交易清冊已 ready。

## 驗收與後續工作

跨 HTTP、quota、目錄、OpenBB 監控、OKX、Binance、Bybit 的 focused 回歸
**693 passed，42.60 秒**；不是全專案 test suite。新舊報告的測試數有重疊，不相加。
涵蓋收據造成的父目錄時間戳變動、真實 directory／leaf symlink 替換，
以及 exact oracle 期間再次改動時必須拒絕通過。
證據：[最後版本測試紀錄](../artifacts/operations/agent-workflow/runs/remaining-final-current-code-reg-20261020T184914-1111fdc4/run.log)。
回歸曾攔下舊測試假設「平行 provider 必須按固定次序發送」；已改為確認
清單輸出穩定、各 provider 精確一次與正確計費，原失敗收據保留。
所有測試禁用 CUDA；正式公開 API probe 只有四頁，沒有為了 benchmark 強制重抓歷史。

完整目標保留未完成，優先後續項目為：

1. OpenBB 剩餘來源／SDK 404、repair queue 與完整 ownership 驗收；統計改為增量時需原子維護與 oracle 比對。
2. CEX fetch 與 generation-fenced 單一提交的跨 lane 協調，不能直接移除 writer lock。
3. FinLab 的必要資料阻擋、分鐘狀態時效與可作業項目分類，需與目前修改 owner 協調。
4. 各未實測 provider 的正式端到端計量；目錄數、service active 與單頁成功不代替覆蓋證明。

實作與收據沿用任務 `all-downloader-scheduling-20261018`，仍為 active。
交易／bidask 的 PID 與 InvocationID 在前後讀回相同；沒有變更交易帳本、下單或額外登入。
