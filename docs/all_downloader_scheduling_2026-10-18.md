# 全來源下載器排程與吞吐修正（2026-10-18）

## 結論與範圍

完成全域入口／正式服務盤點，以及共用限流、HTTP、加密貨幣增量處理、
永豐既有資料重用與無人值守發布的修正。中午續修 OpenBB 有界啟動對帳、
批次路徑解析與多程序稽核，並修正 OKX 合法空值反覆重抓及 Bybit 無限等待寫入鎖。
沒有另建下載管線、沒有修改交易服務，
也沒有放寬來源驗證、發布時間、交易日曆、單位或磁碟保護。

**尚未達到「全部來源都只剩 API 瓶頸」**：OpenBB 的大型所有權稽核、
數百萬小檔發布與安全容量門檻仍是本機邊界；兩份 DGBAS 原始公告仍無法取得。
服務 active、單次 HTTP 成功、局部修復或工程測試通過，都不代表所有歷史已抓齊。

另一個視窗擁有的 factorized 產生器、helper、測試及 benchmark 保持唯讀。
先前暫停的全系統優化目標沒有恢復。本次沒有搬移、刪除資料或送出交易指令。

## 第一性原理與排程判準

有效吞吐量不是 worker 數，而是以下邊界的最小值：

`有效資料／秒 ≤ min(剩餘來源配額、來源回應速率、解析驗證速率、落盤發布速率)`。

估算所需時間須分開計入「尚未發布／休市」、「API slot 等待」、
「HTTP」、「解析與資料合併」、「durable receipt／發布／完整性驗證」。
不能把查詢候選數、API calls、bytes、有效 rows 或完成資料集混為同一進度。

- 同一帳戶／IP／endpoint family 的工作共用官方 bucket；不同限制域可平行。
- 不套用全域 10 req/s，也不把不同端點上限直接相加。
- 來源尚未到發布時間，不靠盲目輪詢提高表面 calls；原交易日曆與追新優先級保留。
- API 重試本身仍須取得新 slot；限流冷卻寫回共用 bucket，不能只讓單一 worker 休眠。
- 依來源可用視窗、原生頻率及已驗證邊界作增量；不改成每輪重抓整個保留區間。
- 併發必須有 queue／response／記憶體／磁碟背壓，不能讓更多 workers 擠掉正式交易服務。

## 全域清冊

截至本次修正後盤點：

| 指標 | 數量與意義 |
| --- | --- |
| Python 下載／更新／回補及共用模組 | 12:30 快照 246 個檔案，不等同 246 個獨立下載器；09:42 快照為 245 個 |
| 靜態辨識有網路存取 | 末次快照 65 個，其中 55 個可沿正式已安裝服務的入口與 import 關係找到；不是原生執行證明 |
| 已安裝正式 StockAgent systemd units | 154 個，包含交易、timer、發布與支援服務；不是 154 個下載服務，排除本次工程暫態 units |
| 資料登錄列 | 2,399 列，含來源端點、衍生投影及參考項目 |
| provider 標籤 | 157 個，含複合來源名稱，不等同 157 套獨立帳戶或採集器 |

清冊保留每個檔案的 transport、timeout、retry、limiter、receipt、原子發布、
正式 owner／timer 及靜態待覆核旗標。旗標不是執行分支或資料錯誤的證明。
舊 `scheduled_networked_file_count` 只計 shell 內字面引用，不能當作實際已排程總數；
新增服務 import closure 的獨立計數，包含相對 import 與小型 CLI wrapper。
盤點只輸出必要服務屬性，不輸出 ExecStart、環境變數值、金鑰或授權 URL。

工程證據位於
`artifacts/data_quality/all_downloader_scheduling_2026-10-18/`：
`schedule_inventory_remaining/downloader_inventory.{json,csv}` 為 12:30 最新盤點；
`schedule_inventory_final/` 為 09:42 快照。兩者差異只有另一項工作新增的
`downloader/news_fulltext_static_instagram.py`，本輪未修改該檔。
另保留 `source_bundle/`、
`data_acquisition_inventory.csv` 與 `provider_policy.json`。
provider policy／來源 bundle 是 08:34 左右的基線快照，不冒充後續修復的最新全量驗收。

五個靜態旗標已逐項追到共用實作，不能直接當作五條失控下載管線：

| 入口 | 覆核結果 |
| --- | --- |
| `download_finmind_sponsor.py` | 呼叫 `finmind_account.rate_limiter`／`backfill_budget` 與原 `_fetch_rows`，並非沒有帳戶限流 |
| `download_tw_share_replacement_reference.py` | 使用 `_configure_tw_public_rate_limiter`、`_cached_or_post` 及 content-addressed 收據 |
| `download_tw_mof_trade_release_values.py` | raw PDF SHA、來源 URL、發布日及 state 已保存；不是因缺少字面 `manifest` 就沒有 provenance |
| `download_tw_futures_intraday_gaps.py` | 經 `DownloadAdmission`、帳戶配額／追新預留與 canonical `_fetch_rows`，不是獨立無限流 API 呼叫 |
| `download_pinned_tool.py` | 有界、固定 SHA 的工具安裝測試，不是正式金融採集器；不能套用金融帳戶 bucket |

## 已實作的共用修正

### 連線重用與共用限流

新增有界、process-local keep-alive pool，先用於 Binance、OKX JSON 與 Bybit。
單一 process 至多 16 origins、每 origin 32 connections；不做隱藏 retry／redirect。
完整讀完 body 才釋放連線，重試與 API slot 仍由原 adapter 負責。
代理、特殊 handler、非 GET／HEAD、含 body 請求及需要 redirect 的 ZIP 保留原 transport。
fork 後不繼承父程序 socket／lock。

共用 rate limiter 閒置可退休 dispatcher／observer，避免大量閒置執行緒常駐。
enqueue／退休／啟動由同一把鎖協調，避免新請求落在已退出的 worker。
本機 pacing state I/O 失敗會釋放等待者並回報本機錯誤，不發送未獲准的 HTTP。
診斷 observer 無法開 thread 不會卡死已授予的 API ticket；只在明確 evidence barrier
才允許呼叫端排空診斷，不把落盤成本塞回一般派送路徑。

429／可重試 5xx／HTTP 200 內的來源限流碼，即使最後一次重試已耗盡，
仍會延後共用 bucket。Bybit IP 403 依官方規則關閉閒置 HTTP 連線並至少等待 10 分鐘，
不能立即重試。每次重試都有有限 budget，網路錯誤與本機 preflight 分開記錄。
參考：[Bybit 官方限制](https://bybit-exchange.github.io/docs/v5/rate-limit)。

`urllib3>=2.2,<3` 改為顯式依賴。08:40 更新 lock 時也反映了先前已在 pyproject
但未同步的 protego／PyMuPDF／Shioaji 等宣告；本次沒有升級已安裝的 SDK。
09:37 期間另有工作新增 `classical` 依賴，故最後 `uv lock --check` 回報需要重新同步。
沒有覆寫這組外部變更，也沒有宣稱目前全專案 lock 已通過檢查；下載器回歸使用既有正式 runtime。

### Binance：避免重抓 30 天與錯誤保留邊界

原 taker／basis 在每輪清掉整個 30 天再下載。現在使用有 fingerprint 的
`period-start-available-at-close-v1` 時鐘證明，保留已驗證舊資料，只補尾端重疊與新值。
舊的不明時鐘資料沿用原有不可信值遮罩，不能偷偷視為已驗證歷史。
OHLC-only hot tail 不會抹掉既有統計時鐘證明。

保留窗以來源原始 timestamp 計算，再依原生 period-start／period-end
轉回模型分鐘座標；修正先加 safety margin 又減 period，仍落出來源保留窗的錯誤。
工作排隊後也更新觀察時鐘，避免長批次使用啟動時的過期 now。
對已知對齊原生 5 分鐘相位的資料，在下一個 period 關閉前不發送統計查詢；
未知舊相位仍查詢，有更新時仍保留原重疊驗證。

INUSDT 正式 targeted CLI 四輪皆無 feature errors。首次修復 28 calls／18.14 秒，
下一輪 11 calls／12.21 秒；再驗證未到新 5 分鐘發布時為 6 calls／11.40 秒。
這是當時邊界下的實測，不是所有商品永久固定 calls。
局部報告放 `quality_repair_runs/`，沒有覆寫全市場進度、catalog、summary 或 receipt。
舊全市場報告的 307 個 partial 尚未全部由原生重跑驗收，不宣稱全部修復。

### OKX：歷史資金費率視窗與共用 IP 限制

`GET /api/v5/public/market-data-history` 官方規則為 5 requests／2 秒、按 IP 限制。
module 3 的月份查詢有效；指定商品 family 時不應改成 daily 來迴避問題。
月視窗最多 **10 個相異月份，首尾皆包含，日期按 UTC+8**。
舊程式每批 20 個月，且用 UTC 月界，造成 8 個新商品的 funding archive 400／50077。

現在以 UTC+8 月首切分、批尾為下一批月首前 1 ms，最多 10 個相異月份，
各批無缺口且不重疊。連續 N 個月只需 `ceil(N/10)` 次 archive 索引查詢。
所有 family 共用新的 IP bucket，不是每個商品各自 5 requests／2 秒。
acquisition contract 升為 v4，feature schema 仍為 v1，原始 CSV 值不變。
參考：[OKX 官方文件](https://www.okx.com/docs-v5/en/)。

同時逐一核對五個 Rubik 統計端點：open-interest 是 10 requests／2 秒；
taker、一般帳戶多空比、大戶帳戶多空比與大戶持倉多空比各為 5 requests／2 秒。
以上均為 **IP + Instrument ID**，不是整個 endpoint 一個共用 10 req/s。
改成同商品同端點共用、不同商品獨立的 bucket，既避免單商品超限，也不把不同商品
不必要地串行。archive 的 IP-only bucket 則不能按商品拆開；明確指定更慢 interval 仍保留。

原生探測已確認：跨 11 個相異月份失敗，10 個相異月份成功。
原先失敗的 ALLO、BEAT、BREV、DASH、LIGHT、LIT、MMT、NIGHT 共 8 個 USDT swap，
正式 targeted CLI 85.14 秒完成，8 個商品及 64 個 feature stages 全部無錯誤。
archive 索引共 16 grants，所有 family 的 limiter count 為 1、interval 0.4 秒。
獨立讀回 8 檔的 realized funding 非空數，全部與 report coverage 相等；
archive 本來未提供的其他欄位仍保留 null，不把 realized rate 冒充另一種來源欄位。

此原生驗證期間正式 intraday timer 等待同一 dataset lock 後也更新了全域 tail-only 報告，
因此全域報告的前後 SHA 不相同；不能把這段非靜止區間宣稱為「全域檔案 byte 不變」。
局部 CLI 不覆寫全域 evidence 的語義另以 features 開／關兩組隔離測試驗證。

Rubik 分商品 bucket 改版後再用 ALLO／BEAT 正式原生驗證：五個統計 family
各有 2 個 instrument limiter，全部回報 per-instrument scope，兩檔 feature updated、零失敗。
總耗時 78.27 秒中 **55.29 秒在等待既有 writer lock，實際工作 22.98 秒**。
這段等待不能算成 API 限流；同一檔案仍採單一 writer，沒有為縮短測試繞過鎖。

### OKX 首次資金費率的結構性空值

第一個已觀察結算事件沒有前一個事件，兩次結算間隔可能合法為 null。
舊程式把每一根沿用該事件的分鐘列都視為缺口，因此每輪從商品上市附近重抓。
現在用既有 age 與分鐘結束時間反推事件，只有同一最早事件、有限 realized rate、
有效非負 age、且該事件完全沒有已知 interval 時，才豁免這一種結構性 null。
後續間隔缺口、缺少／錯誤 age、NaN rate、同事件混合已知與未知 interval 仍回補。
原始 rate、age、interval 不變，也不因此宣告更早的來源歷史完整。

acquisition contract 升為 v5，feature schema 仍為 v1。
ALLO／BEAT 正式 targeted CLI 全程 12.30 秒，2 商品／16 stages 全部成功，
鎖等待 0.000124 秒；此為新的原生驗收，不與前述不同截止時間的網路執行作 ABBA 比較。
獨立讀回 979,165 列：八組 coverage 與收據相等，兩檔各 240 根合法未知 interval
仍為 null，下一次 funding 查詢皆只退回最新分鐘的重疊邊界，檔案 SHA／identity 讀取前後穩定。
證據：`okx_prefix_independent_readback.json` 與
`data_okx/1m/quality_repair_runs/20261018T041348942063Z/`。

### Bybit 有界等待與異常釋鎖

Bybit 原本直接 blocking flock，平台沒有 fcntl 時甚至可能繼續寫入。
改用 Binance／OKX 相同的 `exclusive_dataset_lock`：預設最多等 180 秒、
可明確覆寫，記錄 waiting／acquired／timeout／error，例外與正常退出都釋鎖。
無法鎖定時拒絕寫入；逾時不建立 client、不發請求、不覆寫資料或正式進度收據。
沒有縮小鎖定範圍，尚未解決跨 lane 的粗粒度競爭。

KAT／PONS 正式 targeted tail 更新共 399,796 邏輯列，2 商品成功，
全程 2.715 秒／工作 2.713 秒／等待 0.000178 秒；報告保留在獨立
`data_bybit/1m/quality_repair_runs/20261018T042801932921Z/`，不是全市場完成宣告。

### 加密貨幣共用欄式處理

Binance／OKX 重用窄欄位 overlay：只 join `date + 被更新欄位`，
不為每一組 feature 反覆 join／複製全部寬欄位；無關欄位重用 Arrow buffers。
重複新日期或保留欄位衝突回退原 wide-join oracle，不偷偷 deduplicate。
coverage 一次 aggregate；latest non-null 只過濾時間欄，保留原 overlap 判式。
signed raw values、NaN、null、型別、列順序與原衍生比率保持一致。

### 永豐：重用既有資料與修復正式發布

hybrid builder 在完整來源／輸出 SHA、calendar sessions、builder 與套件版本一致時，
重用未變動的商品。來源、輸出、日曆或 ABI 有變即走原重建路徑；前後做 generation fence。
完整 daily receipt／chunk checksum 稽核和逐檔值讀回沒有取消。

正式 backfill 曾因 systemd 缺少 WSL distribution／Windows PATH 而在發布階段反覆失敗。
改成在 opt-in 的正式發布與 backfill wrapper 載入受信任既有 node enrollment；
不猜 distro、不修改全域 PATH、不建立新登入、不放寬磁碟保護。
原 canonical owner 隨自然重試於 09:16:29 完成：564,735 個來源檔案中
重用 564,732 個、只變更 3 個；新同步 47,339,901 bytes／4 objects。
research 3,231 檔完全重用、無新增同步 bytes。這證明發布成功，不證明所有歷史缺口消失。

quote 稽核原本逐值 Python `float`／`math.isclose`。
第一版 eager Polars Series 算術在全市場 ABBA 反而慢約 12%，因此不採用。
原因是大量短欄位的多次 dispatch 固定成本，不是資料值比較本身。
最終對長度至少 256 的 numeric／Boolean 使用 FP64 NumPy 向量比較，短欄位與
其他型別保留 scalar oracle；連續 Float64 buffers 可直接重用，必要轉型與分段欄位仍允許有界 copy。
仍保留 `rel_tol=1e-12`／`abs_tol=1e-9`；NaN／null 不相等，
Inf 只等於同號 Inf。邊界值、極端浮點、nextafter、單個壞值及 multi-chunk 有測試。

### 公開宏觀公告

CBC HTTP 200 暫時 proxy error 現在走有界 transient retry；真正 access denial
仍 fail closed。DGBAS redirect cycle 不再把相同 homepage 重複抓五次；
永久 4xx 立即退出，429／5xx 保留原有限重試及共用冷卻。
原來源 allowlist、TLS／CA、bytes cap 和完整 projection 發布閘門都保留。
未取得的官方原文不以首頁、估值或其他日期替代。

## 實測：相同結果與完整驗證

本機工程測試限制 CPU threads、禁止 CUDA。A/B/B/A 保留 SHA／raw-value／null 驗證；
共享主機與 page cache 未控制，數字只代表所列工作量，不外推所有網路與商品。

| 工作量 | 原路徑 | 修正後 | 驗收邊界 |
| --- | ---: | ---: | --- |
| 永豐全市場 hybrid 建置 + 完整來源 checksum 稽核 + 逐檔完整值讀回 | 中位 191.39 秒 | 47.45 秒，4.03× | 2,761 檔逐檔一致，兩輪 B 全部重用；這組使用原 scalar quote 稽核 |
| BTC 3,738,266 列、63 欄：讀檔、10 組 overlay、derive、coverage、atomic Snappy、SHA、完整讀回 | 33.91 秒 | 29.86 秒，約少 12% | 四輪 Parquet SHA 全相同；沒有生產資料寫入或 provider calls |
| 同一 BTC 工作量尖峰 RSS | 9,188,579,328 bytes | 7,534,368,768 bytes，少 18.0% | 不是整個系統的記憶體下降百分比 |
| 另一次全市場完整 checksum 稽核 + 2,761 檔完整值讀回 | scalar 51.69 秒 | 短欄 scalar／長欄向量 44.37 秒，1.165× | 四輪 audit 結果與完整 raw values 相同，code SHA fence 通過；不與上列 4.03× 相乘 |
| 有界 HTTP loopback fixture，80 pages、8 workers | 1.373 秒、80 connections | 0.350 秒、4～6 connections | 80 requests 與輸出 SHA 相同；3.93× 僅為本機 fixture，不能稱 Internet 加速倍數 |
| OpenBB 12,000 個實際檔案的完整 ownership 階段，SQLite 規則相同 | 1.338 秒 | 0.547 秒，2.45× | 批次解析目錄；symlink／特殊路徑回退原 oracle，來源 bytes／identity 不變 |
| 同 12,000 檔的完整抽樣 file audit，含 index、footer、schema、完整首個 row group 的 provenance 解碼 | 13.017 秒 | 8 spawned processes：2.910 秒，4.47× | 保留真實來源檔名、所有結果／錯誤計數一致，包含程序啟動、IPC、收尾；不是 748 萬檔或整個服務加速倍數 |
| 同一 file audit 的 CPU 與 RSS 加總 | CPU 14.919 秒／RSS 102 MiB | CPU 13.822 秒／RSS 791 MiB | CPU 含已等待完成的子程序；RSS 為 20 ms 取樣的程序樹加總，會重複計共享頁，不是實體記憶體精確尖峰 |
| OpenBB 60,000 檔完整啟動對帳，661 MB 來源複本 | 0.921 秒／RSS 159 MiB | 0.837 秒／RSS 82 MiB | 真實檔名、既有主鍵有界 range lookup，包含每次 B 首建例外索引成本；所有原檔 SHA 不變，不能外推全量耗時 |

對應證據：`hybrid_abba/result.json`、`feature_overlay_abba.json`、
`http_abba/result.json`、`binance_native_repair.json`、`okx_native_repair.json`、
`quote_vector_audit_abba/result.json`。
未採用的 eager Polars 路徑測量保留在 `quote_audit_abba/result.json`，避免只保存正面結果。
OpenBB 新證據：`openbb_ownership_batch_abba.json`、
`openbb_forced_proof_file_audit_abba.json`、`openbb_forced_proof_reconciliation_abba.json`。
4 個執行緒曾讓完整抽樣 file audit 從 11.414 秒退化至 14.136 秒，已移除該路徑；
4 個獨立程序另測得 3.080 秒、RSS 加總 470 MiB，8 個程序另測得 2.392 秒、830 MiB。
採用 8 個程序是此主機／負載下的有界選擇，不把不同輪基線相乘或保證全域最優。
舊 `openbb_process_file_audit_abba.json` 只量主程序資源，不能拿其 CPU／RSS 當整體下降證據；
receipt schema v3 補等待完成的子程序 CPU 與整棵程序樹 RSS 取樣；v4 再要求
保留來源原檔名，不能用重命名後的 fixture 證明正式檔名捷徑可用。
v5 的 reconciliation 每次 B trial 都重建隔離清冊的例外索引並計入耗時，
沒有只比較省略首次遷移的暖查詢。
先前的全量 Python-update／SQL-upsert TEMP 中間版本反而較慢：
12,000 檔 0.160 → 0.170 秒、60,000 檔 1.044 → 1.543 秒。
這些負面證據與錯誤假設 full-ID 檔名的 prototype 都保留；後者未套用正式資料。
最終採用真實的 16 位 task ID prefix 與碰撞處理，不把不同輪基線相乘。

## 各來源 owner、採用與剩餘邊界

| 來源群組 | 正式分工／本次處置 | 尚未完成的邊界 |
| --- | --- | --- |
| FinMind Free／Sponsor／Complement | 同帳戶配額協調；保留現有 v12、20,000/hour、發布日曆、追新預留、16 workers 與 durability | 12:21 最近 15 分鐘 4,828 requests，折合 19,312/hour、上限 96.6%；不等同一個完整滾動小時或有效 rows 完成率。長駐 process 尚未重載本次 common 修改 |
| FinLab | 保留現有 v14、分鐘 32 workers／31 日、tick 8 workers／7 日、有界簽名／quota／GC；沿原 timer 啟動 | 12:31 原生狀態為 tw_tick，已知分鐘供應窗待補／可行／到期重查均為 0，容許 residual tick；不代表 2003 起所有理論候選日都有來源資料 |
| 永豐 | 同一 owner／session；修復既有資料重用與 WSL 發布，沒有新登入 | 完整 chunk／冷資料發布仍需時間；官方查詢限制依目前文件，不沿用過期 50 次／5 秒數字 |
| Binance／OKX／Bybit | 原 registered owner；共用 HTTP／bucket、修正增量與查詢視窗；三者均有界 dataset lock，局部正式修復沿原 owner | 舊全市場 backfill 仍保留 failed 收據，不把 targeted 報告當成全域成功；跨 lane 粗粒度等待仍在 |
| 台股公開／MOPS／TAIFEX／ToAlpha | 保留原 parser、來源適用日曆、發布可用性、共用 host 節流與來源收據 | 容量、歷史版本、驗證與來源服務限制分開；本次不是全部重新下載或重新 OCR |
| CBC／DGBAS 等公告 | 修正 transient proxy 與 redirect-cycle retry 浪費；原 canonical 服務實跑一次 | DGBAS 649／651 原文成功，GDP 236591、失業 236510 尚未取回；projection fail closed |
| OpenBB 及其 EIA／OECD／IMF 等 routes | 同一 provider router／原任務 manifest，不新建重複採集器 | 下列所有權及容量問題仍阻止安全重啟 |
| Dune／SEC／ETF、新聞、其他公開資料 | 靜態及正式 owner 納入清冊；保留既有優先、配額、權限與範圍 | 沒有以清冊列數當作已下載量；本次未宣稱每一來源原生全量重跑 |

FinLab／FinMind 先前的原生驗收與政策細節見
[同日吞吐報告](finlab_finmind_throughput_2026-10-18.md)。
FinMind 本輪流量讀取使用唯讀、按時間索引的 request log，不掃全 queue、不呼叫 provider。
其中 4,826 次為 USStockPriceMinute、2 次新聞；超過 2 秒的間隔有 5 次、合計 31.53 秒。
Sponsor Pro 的 20,000 次／小時上限另核對
[官方方案文件](https://finmind.github.io/Pricing/)。
永豐官方現行查詢限制見 [官方限制頁](https://sinotrade.github.io/zh/tutor/limit/)。

### OpenBB：尚不能忽略的本機問題

上次 audit 的 7,328,863 個 active source Parquet，335,379,117 rows，
在已執行的 footer／metadata／wrapper 檢查中沒有 missing、unreadable、row/schema mismatch。
不代表每個 payload cell 已稽核。
掃描包含歷史共 7,476,049 檔，仍有 **611 個不符合目前 success owner 的檔案**，
另有 146,575 個 inactive-success 歷史檔合法保留。
不能將 pending owner／舊 provider 的資料直接改標 success，也不能刪除這些唯一歷史來通過檢查。

舊全量 private 1.616 GB ownership SQLite index 建置約 1,142 秒、掃描約 419 秒。
現在批次解析只重用同批、經前後 identity 檢查的普通目錄，葉節點仍逐一檢查，
不以 TTL 跨批重用。不明路徑回退原 `Path.resolve`，目錄並行改變則拒絕結果。
完整抽樣 file audit 採 source-order、64 files／batch、最多 2×workers inflight 的 spawn pool，
不把來源 SQLite connection 傳給子程序，也不巢狀啟動 Arrow threads。
CLI 的 `--audit-files` 預設 8 workers，可選 1 回到 serial；一般狀態查詢不啟動 pool。
report contract 升為 v16，保留 backend／workers／batch／解碼範圍。

啟動對帳先在同一 SQLite snapshot 證明全 active plan 的實際檔名尾碼契約與既有主鍵。
新增的部分索引只存「不符合檔名／ID 契約且 active」的例外，正常列不進索引；
SQLite 在 task／path／plan／active 改變時原子維護，不靠容易過期的 TTL 或外部快照。
資格查詢明確指定該索引，防止未執行 ANALYZE 時 planner 選回全清冊跳讀；
舊的唯讀、尚未 migration 清冊仍可走原精確掃描。
正常檔名為 `_safe_scope(scope)-task_id前16碼.parquet`，不是完整 task ID。
每批至多 256 個檔案，依 16 位 prefix 以既有完整 task ID 主鍵作 range lookup，
仍需逐一比對完整 output path，不把同名副本認成擁有者。
prefix 不假設唯一：串流所有碰撞列，遇同路徑多 owner 明確拒絕，不能任意選一個覆蓋。
換行／非 ASCII 檔名、pending transaction、失敗釋放 snapshot、1,024 個 prefix
碰撞與 duplicate-owner fail-closed 均有測試。
完整 task-ID 主鍵直接重用；不另建包含數百萬健康完整路徑的常駐 duplicate index。

不符合契約或主鍵者使用 SQLite TEMP fallback：批次輸入，SQL 內完成 status upsert
與健康列過濾；不在 Python 保留全量 Path、fetchall 或 status dictionary。
本機 SQLite 3.54.1 的 `TEMP_STORE=1` 可落盤；兩條路徑均保留 pending／running
的 last-good shard，terminal／orphan 原樣走既有可恢復 quarantine。
本輪沒有對正式來源執行 quarantine。唯讀正式清冊證明目前契約與主鍵確實相容，
但整份約 19.7 GB manifest 在尚無例外索引時的資格檢查用了 169.88 秒；
另一次唯讀強制順序掃描為 99.38 秒，兩者不是受控 native ABBA。
這正是採用例外索引的原因，不能只用小樣本跳過全量複雜度問題。
正式來源尚受 100 GiB 容量門檻保護，本輪沒有繞過門檻套用全量 migration／對帳；
不能把資格證明或隔離 migration 測試當作正式新索引已採用。
證據：`openbb_real_filename_eligibility.json`、`openbb_native_proof_table_scan.json`。

上述修正已通過有界實際來源 ABBA，尚未在 748 萬檔上驗收新吞吐，
也未完成可跨次安全重用的 ownership index；不能由有界樣本外推全量時間或記憶體。
OpenBB 既有 100 GiB 安全門檻高於 12:24 觀察 WSL 約 79 GiB 可用空間（仍隨其他服務變動）；
遵守使用者「留 WSL、容量不足安全暫停」的選擇，沒有降低門檻或盲目重啟全歷史。
OECD 既有 9 個 GDP forecast 404 等來源問題也沒有被本次 common 修正掩蓋。
此外，同一 CEX dataset 的 backfill／intraday 仍可能等待單一 writer lock；
本次保留隔離與一致性，尚未建立可並行取得、generation-fenced 單一提交的跨 lane coordinator。
OKX 首個事件 interval 的結構性 null 重抓已修正並完成兩檔原生驗收；
其餘全市場 backfill 與來源原本缺漏仍需各自的正式收據。

## 驗收與可重現入口

全來源盤點（零 provider calls）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_downloader_contracts.py \
  --include-runtime --output-dir artifacts/downloader_review/latest
```

兩個有界實測入口：

```bash
# 必須使用新的輸出目錄；自動取得原 minute owner lock。
run_fintech_python scripts/benchmark_shioaji_hybrid.py --output-dir artifacts/benchmarks/hybrid-new
# 只比較 strict audit，保留完整 checksum 和逐檔讀回，不 rebuild。
run_fintech_python scripts/benchmark_shioaji_hybrid.py --audit-only --output-dir artifacts/benchmarks/audit-new
run_fintech_python scripts/benchmark_crypto_feature_overlay.py --help
```

跨 FinLab／FinMind／CEX／HTTP／limiter／WSL／發布／公告／稽核
修復後回歸 **2,806 passed**；五個 Rubik endpoint scope 的末輪調整再以相關
**300 passed** focused 回歸驗證（與前者重疊，不相加）。
`bash -n`、受影響 Python compile、scoped `git diff --check` 通過。
新檔案限於共用 source、可重用 benchmark、測試與此報告；測量 payload／收據維持 ignored。
清冊 CLI 的直接執行 import 路徑也已修正：不需依賴 workflow 額外設定的 PYTHONPATH。
以 isolated Python／非 repo cwd 覆核；清冊與服務 parser 相關 **370 passed**（與前述回歸重疊）。
兩個正式交易／bidask owner 的 PID 與 InvocationID 保持不變。

### 末輪驗收

OKX 正式回補與原生獨立欄位讀回完成；新 quote-audit ABBA 四輪全部通過。
2,806 項跨來源回歸與 300 項末輪相關回歸完成；不能解讀為整個專案全部測試都執行。
Rubik 分商品 bucket 的額外正式原生驗證完成：2 商品、5 family，
78.27 秒總耗時含 55.29 秒 lock 等待，無來源／feature 失敗。
`native_service_readback.json` 獨立確認兩個受保護交易 owner 身分不變。
工作尚未包含 OpenBB 全量 ownership 修復、跨 lane writer coordinator、
Binance 307 個舊 partial 的全市場原生驗收，故全域任務保留未完成的下一步，不標記已全面 API-bound。

### 中午續修驗收

OpenBB／OKX／Bybit／共用進度／鎖／supervisor／watchdog 相關回歸 **777 passed，29.94 秒**，
與先前測試有重疊，不相加；這不是全專案 test suite。
僅對既有 matplotlib import 觸發的第三方 `PyparsingDeprecationWarning` 使用類別限縮過濾，
其餘 warnings/error policy 保留，沒有更換正式 SDK 或訓練環境。
先前新 telemetry 的 serial／parallel 比較失敗，已改成分別驗證 execution contract，
再比較所有資料錯誤計數與來源順序；失敗收據保留，後續回歸已通過。
新增的 partial-index 查詢計畫測試也攔下 planner 選回 broad index 的候選，
目前明確綁定例外索引並保留 legacy read-only fallback；該失敗原收據未改寫。

受影響 Python compile、scoped diff check 通過。兩個受保護交易／bidask owner
PID／InvocationID 再次核對不變，factorized 與另一項工作的新聞程式沒有修改。
OpenBB 只驗收隔離 fixture，正式 100 GiB 門檻下未啟動下載；
OKX／Bybit 新原生結果為各 2 商品，不能當作全市場完整性證明。
