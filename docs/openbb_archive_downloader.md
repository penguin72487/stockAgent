# OpenBB 2000+ 歷史資料下載器

## 範圍

預設從 `2000-01-01` 下載至執行當日，股票清單取自本機 US 與 TW universe。

預設排除：

- crypto 與 derivatives。
- 即時／當下快照，例如 quote、market snapshots、gainers/losers/active、screener、price performance。
- SEC HTML、國會法案全文、逐字稿及 PDF/HTML body；清單、連結與其他 metadata 仍會保存。
- 無法由有限 catalog 完整枚舉的任意文字搜尋。原因會記在 `catalog/coverage.parquet`，不會假裝已完整下載。

相同資料採主要 provider 加失敗備援；歷史行情預設為 Yahoo → FMP → Intrinio → Tiingo，因此 Tiingo 不會成為主要來源。Yahoo 與 FMP 同時能回答的 fundamentals／metadata 也先走 Yahoo，再把每日只有 250 calls 的 FMP Basic 留給真正 FMP-exclusive backlog；SEC 等官方 primary 仍保持在兩者之前。這個 runtime scarcity order 也會套到舊 manifest，不必重規劃或重抓 success。只有 Tiingo 能提供的端點預設不執行。

## 下載

```bash
./scripts/run_openbb_archive_download.sh
```

此入口會自動固定 `data_openBB/_state/archive_end_date.txt` 並以
`--resume-existing-plan` 續傳；中斷或隔日重跑不會重新枚舉整個初始計畫。截止日是該輸出目錄的
archive identity；若要不同右界，請用 `OPENBB_ARCHIVE_END_DATE=YYYY-MM-DD` 搭配新的 `--output-dir`，
避免悄悄改寫既有 manifest 的時間身分。
resume token 也包含實際 universe 與 OpenBB command/schema coverage fingerprint；symbol 清單或
provider/plugin coverage 改變時會自動觸發重規劃。

預設不重試已判定為 provider 永久失敗／方案不支援的路由；若更換 API 方案後要重新驗證，
才使用 `--retry-permanent`（或 `--refresh`）。429、quota、timeout 等暫時錯誤仍由共享冷卻與
checkpoint 機制續傳。

Manifest checkpoint 之外，Congress 的多頁 collection 與 bill/amendment metadata 另有 request-level checkpoint。每個成功 JSON 子頁會原子保存到 `data_openBB/_state/request_checkpoints/congress_gov/<task_id>/`；URL fingerprint 會先移除 `api_key`／token，response checkpoint 也不保存 credential。若第 7 個子請求 timeout，下一次只補第 7 個，不會重新消耗前 6 個 hourly quota；cache hit 不取得 limiter ticket。只有 Parquet 已原子發布後才清掉該 task 的子頁 checkpoint，損壞 checkpoint 則改名保留為 `.corrupt.*` 並重新抓取，不會靜默採用。

需要長時間背景下載、自動續跑與定期完整性快照時，使用 supervisor：

```bash
./scripts/run_openbb_archive_until_complete.sh
```

這是給使用者的單一前景入口：固定從 `2000-01-01`、先驗證 fintech runtime 與
OpenBB/DuckDB/Polars/PyArrow、檢查重複 PID 和剩餘空間，然後執行 supervisor 直到下載、終局檔案
稽核、compaction 與 compact audit 全部通過。預設保留詳細 tqdm；要安靜背景執行可用：

```bash
OPENBB_SUPERVISOR_TQDM=0 tmux new-session -d -s openbb-archive \
  './scripts/run_openbb_archive_until_complete.sh'
tmux attach -t openbb-archive
```

supervisor 每 60 秒只讀原子 `provider_scheduler.json` 與最新 monitor snapshot，檢查進度、RSS、FD、
completion backpressure 與 cooldown，不掃描大型 manifest；完整 manifest/contract/pagination/quota
快照預設每 900 秒寫入 `data_openBB/_state/monitor_latest.json` 與 `monitor_history.jsonl`。可分別用
`OPENBB_MONITOR_INTERVAL_SECONDS` 與 `OPENBB_FULL_MONITOR_INTERVAL_SECONDS` 調整。這避免約一分鐘的
1,000 萬 task 掃描每分鐘與所有 provider 爭用 CPU/SQLite。程序異常結束時仍會自動重跑。當所有可重試任務收斂後，會逐一打開每個
成功 Parquet，核對檔案存在、可讀性、row count，以及 endpoint、provider、scope、query、
retrieved timestamp 五個 archive metadata 欄位，並拒絕未展平的單列 `result + metadata` JSON wrapper、零列 success、缺少 provider/path 或重複輸出路徑；只有沒有可處理的未解決任務（已證實訂閱不可用的
`unavailable` 另行列示）且稽核通過，才會自動壓縮。
所有 task 與 catalog 的共享 Parquet writer 先掃描各列欄位聯集；若後續列才出現 BLS code-map 欄位、validation recovery evidence 或其他合法 provider 欄位，會用零拷貝式 schema sentinel 讓 Arrow 完整推斷後再移除 sentinel。這避免 `Table.from_pylist` 只採第一列欄位而靜默遺失其他市場的異質 metadata。一次性 migration 會重建舊版 BLS search、SEC statement recovery 與修復過渡期間的 Form 4 shards；SEC 原始資料仍從 durable cache 讀取，其他 provider 的 fallback outcome 不會被清除。
完整稽核也會從成功的 CFTC、FRED、BLS、EconDB、SEC、Congress、index、currency catalog Parquet
重新建立預期 follow-up scope，並核對 FMP 新聞、filings 與 major holders 滿頁的下一頁任務；任何 parent row 缺少 active child task 都會使稽核失敗。
planner 同時把 OpenBB 的所有已選 endpoint/provider query schema 與 fetcher 原始碼轉成
`catalog/completeness_contract_audit.parquet`，並把摘要寫入
`catalog/completeness_contract_summary.json`。契約逐一驗證時間區間、分類維度、分頁終止、provider
預設值、fetcher 對時間參數的實際引用，以及未暴露在 query schema 的 hard-coded source cap；任何
primary/schema obligation 未分類都會在 API 下載前 fail closed。監控器把這個契約納入 `complete` 與
critical alert，不能再以「所有目前 manifest tasks 跑完」冒充「所有市場資料完整」。可獨立查看：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_openbb_archive_contracts.py \
  --output-dir data_openBB --fail-on-unresolved
```
背景模式預設關閉 tqdm，避免數週下載產生巨量控制字元日誌；每分鐘輕量 watchdog 會輸出 scheduler
phase、attempted、active、completion queue 與 backpressure，預設每 15 分鐘的完整監控再輸出精確
計數、速率、活躍 endpoint、預估時間與剩餘磁碟。如需在 tmux 中保留 tqdm，可設定
`OPENBB_SUPERVISOR_TQDM=1`。

完整健康快照另會保存 `pending_with_error`、`pending_rate_limited`、`pending_cooldown`、`pending_attempted`、`active_provider_cooldowns`（不綁特定市場/provider 的實際截止時間與剩餘秒數）、`provider_runtime_limits`（本次程序對每個 provider 實際採用的 RPS 與 concurrency）、`category_progress`（每個市場分類的 total／success／empty／unavailable／pending／running／failed、rows 與完成率）、`provider_progress`（全期由各實際 selected provider 完成的 success／empty task 與 rows）、`recent_progress`（最近 15 分鐘分 category 與實際 selected provider 的完成 task／row 數，避免總吞吐掩蓋單一市場或來源停滯）、`provider_progress_stalls`（所有 provider 的 unresolved backlog、cooldown 與最近 accepted 交叉判定的來源級停滯）、`snapshot_delta`（與上一份同 plan 快照相比的新增 task、accepted、rows、retryable、零資料 endpoint，以及 category/provider 細分增量）、`provider_outcomes`（含 empty／unavailable／permanent task 數、permanent endpoint 分組與最多 50 筆樣本）、
`request_checkpoints`（尚未完成的 request-level task/file/bytes、provider 分組、最舊／最新時間與 corrupt quarantine 數）、
`provider_constraints`（保留 FMP/BLS/SEC/Tiingo 的相容性診斷欄位與通用 provider capacity），而真正跨市場、跨 provider 的
來源級停滯判斷以 `provider_progress_stalls` 為準；完整未解 fallback 壓力另存於 `dynamic_provider_backlogs`。監控同時計算 `exclusive_provider_backlogs`：eligible backlog 會在主要＋備援間重疊、不可相加，exclusive backlog 則只計已無其他 provider 可走的 task，供 quota 完成下限使用；同一趟掃描另保存 `provider_category_backlogs` 與 `provider_endpoint_backlogs`，避免 provider 總數掩蓋特定市場或 endpoint。每個 endpoint backlog 會乘上 HTTP boundary 的實測 requests/task；尚無樣本才使用明確標記的 1-request 樂觀預設。`provider_eta_projections` 與 `category_eta_projections` 分開呈現最近 15 分鐘吞吐、configured RPS 的物理樂觀值與 hourly/daily reset 下限；quota windows 使用剩餘 estimated HTTP requests，而不是 task 數，且 `lower_bound_only` 會指出是否仍有未觀測的 exclusive endpoint。任何宣告配額都由通用 `provider_quota_feasibility` 產生告警，不硬編碼單一市場。這些欄位都會從每筆 task 的 fallback chain 動態枚舉所有 selected provider，
`last_accepted_update`、`minutes_since_last_accepted`、`accepted_progress_stalled`、`health` 與結構化
`alerts`。監控也會把尚未耗盡的 failed task 依 endpoint、provider 與完整錯誤訊息聚成
`retryable_failure_clusters`；相同 signature 達三筆即產生 `systematic_retryable_failures` warning，讓
schema／adapter 類的共通問題在第 20 次重試耗盡前被發現。因此只有 pending task 被重新排隊不會被誤認成實質下載進展；預設連續 120 分鐘沒有新的
success／confirmed-empty，或 provider quota 正在延後任務時會產生 warning。supervisor/downloader 停止、stale running、attempts exhausted、
FRED/FMP 續頁缺口、其他 active plan、低於 100 GiB 可用空間或全量檔案稽核失敗會產生 critical alert。
對無法分頁但有固定 provider row cap 的 catalog，monitor 另以 `source_cap_saturations` 核對實際
success rows；任何 partition 的列數達到上限都代表仍可能截斷，會產生 `source_cap_saturated`
critical alert 並阻擋完成。目前 `etf.search` 的 10,000 列上限以 installed schema 的全部 exchange
分區，且每個分區最終都必須實際少於 10,000 列。
`endpoint_progress_summary` 另按 active endpoint 統計已開始產生 accepted task、完全零 accepted、
已解析完成與仍未解析的端點數；`zero_accepted_endpoints` 保存最多 50 個最大未開始端點及其
pending/running/failed 狀態及 FMP/BLS/SEC quota、provider capacity、Tiingo entitlement blocker
計數。摘要另分列 quota、capacity、in-flight 與真正無可解釋 blocker 的零 accepted endpoint。只有仍有 actionable unresolved task 的零 accepted endpoint 會產生
warning；已權威判定 subscription unavailable 的 resolved endpoint 仍透明列示，但不誤報為排程飢餓。
同一套跨市場稽核也計算每個 endpoint 的 success／authoritative-empty 比例；至少 10 個 accepted task
且空資料率達 90% 會列入 `high_empty_endpoints`，若成功數為零則產生 `endpoints_all_empty` critical alert，
用來提早發現共用 query 維度、日期切片或 provider routing 錯誤，而不是逐一市場等到下載結束才修；
全空 endpoint 會阻止最終完成判定，直到規劃或來源有效性獲得修正。
完整快照的 `downloader_process` 同時保存 RSS、peak RSS、virtual memory、swap、thread 與 open FD
數，方便分辨正常工作集、socket 累積與長期記憶體成長；human monitor 以 `[openbb-process]` 顯示。
supervisor 預設在 downloader RSS 達 16 GiB 時安全重啟程序；manifest 會在下一輪把中斷中的 running
task 恢復為 pending，已完成 Parquet 不重抓。可用 `OPENBB_MAX_DOWNLOADER_RSS_BYTES` 調整，設為 0
才會停用此保護；FD、停滯與 log rotation 保護仍各自獨立。
rolling scheduler 的 supervisor 預設 queue 為 1792，兼顧 provider 延遲覆蓋與 thread/GIL/cache-lock 成本；可用
`OPENBB_DOWNLOAD_BATCH_SIZE` 調整。catalog records 在 follow-up discovery 完成後立即清空；排程器
平時只用該 provider 的 endpoint 索引補滿 120 秒 request-start queue；全域 endpoint 公平掃描只負責發現新
provider／route 並低頻執行。至少每個全域 worker-width 才做一次 Python cycle／Arrow unused-pool
回收，避免每個 future 都重掃 135 個 endpoint 並觸發全域 GC。已完成 future 以最多 256 筆共用
一個 SQLite `synchronous=FULL` transaction；等待持久化的結果超過 1024 筆時會暫停所有 provider
的新 admission，降回界線才恢復。這個 producer/consumer 背壓同時限制 retained records 記憶體，
避免任何單一市場的 discovery 或 manifest commit 讓所有 provider 一起失去執行容量。
啟動 wrapper 預設設定 `MALLOC_ARENA_MAX=4`，避免大量 I/O workers 各自保留約 64 MiB allocator
arena；可用既有 `MALLOC_ARENA_MAX` 或 `OPENBB_MALLOC_ARENA_MAX` 覆寫，但必須是正整數。
supervisor 的重啟輪次會使用 `--resume-existing-plan`：先驗證 active plan token、2000 年起迄日、
596-row completeness audit、coverage catalog、已設定 credential 集合與 planner state version，全部一致
才略過數百萬 initial task 的重建；任一證據缺失或不一致會自動退回完整規劃。續頁修復、provider
pruning 與舊語意修復至少完整執行一次後會留下 maintenance-version marker；相同已驗證 plan 的後續
recycle 只做 running-task recovery、failed retry 與成功檔案存在性核對，避免每次重掃 580 萬列。
planner 或 maintenance 邏輯版本提高時會再次執行完整維護，最終完整性閘門永遠不會略過。
可用 `--accepted-stall-minutes` 與 `--min-free-gib` 調整門檻。

supervisor 第一次啟動會把截止日寫入 `data_openBB/_state/archive_end_date.txt`，後續跨日重啟仍使用同一日期，避免數週任務每天產生新的 plan。已固定的輸出目錄若收到不同的 `OPENBB_ARCHIVE_END_DATE` 會 fail closed；若要刻意建立不同截止日的 archive，請使用新的輸出目錄。
若 manifest 連續一小時沒有任何進度，supervisor 會終止並重啟可能卡住的 downloader；可用
`OPENBB_STALL_TIMEOUT_SECONDS` 調整秒數。停滯以真正牆鐘時間與 scheduler attempted／manifest
timestamp 判斷，不會因完整監控本身耗時而扭曲。一般完整快照預設最多執行 600 秒，可用
`OPENBB_FULL_MONITOR_TIMEOUT_SECONDS` 調整；timeout 或 snapshot 錯誤會 fail-open 到下一個 resumable
round，不會把舊快照的 `retryable=0` 誤當完成。當剩餘工作全部在 provider cooldown，supervisor 直接
睡到最早 reset deadline，不會每分鐘重啟、重掃 startup manifest。
Yahoo/yfinance 的共享 curl session 在長時間大量請求時可能累積 `CLOSE_WAIT` socket；supervisor 每分鐘檢查 downloader 的 open file descriptors，預設達 4,096 時受控重啟並由 manifest 續傳。可用 `OPENBB_MAX_DOWNLOADER_FDS` 調整，設為 `0` 可停用此 watchdog。
supervisor 每分鐘也檢查輸出磁碟，預設保留 100 GiB 安全空間；低於門檻會停止且不自動重啟，避免
SQLite/Parquet 因磁碟耗盡而損壞。可用 `OPENBB_MIN_FREE_BYTES` 調整 bytes，設為 `0` 才停用。
背景程序的第三方 SDK 輸出會寫入 `data_openBB/logs/supervisor.log`；supervisor 預設在 256 MiB 時輪替為單一 `supervisor.log.1` 並截短目前檔案，避免數週執行耗盡磁碟。可用 `OPENBB_SUPERVISOR_LOG_MAX_BYTES` 調整 bytes，設為 `0` 可停用輪替。

隨時查看權威進度：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/monitor_openbb_archive.py
```

執行當下的完整逐檔稽核：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/monitor_openbb_archive.py --audit-files
```

下載器會先把任務分批寫入 `data_openBB/_state/openbb_archive.sqlite3`，再開始 API 請求。每個成功、已確認為空或已證實目前訂閱不可用的任務都會持久化；三種狀態會分開統計。中斷後以完全相同的參數重跑即可續傳：

```bash
./scripts/run_openbb_archive_download.sh
```

以 2026-07-18 的本機 universe 實測，規劃基礎是 17,293 個去重後股票代碼、5,639 個 ETF、870 個外匯對。任務數會隨季度 ownership checkpoints、CFTC、FRED、index、SEC、Congress catalog 與滿頁 continuation 動態增加；權威總數一律以目前 manifest／監控快照為準，不以文件內的舊靜態估計判斷完成度。

SEC `equity.compare.company_facts` 使用每個美股 symbol 一個 bulk task，直接展開官方 `companyfacts/CIK.json` 的全部 taxonomy、fact tag、unit 與歷史 observations。這取代原本 symbol × 279 個 fact 的 4,061,682 個重複 `companyconcept` tasks；目前 universe 只需 14,558 個 API tasks，並保存 `taxonomy`、`fact_tag` 與 `fact_description` 以維持可追溯性。

SEC 六個 balance／cash／income 原始與 growth statement endpoint 共用同一份 durable companyfacts
解析，direct fetcher 的 `AnnotatedResult.result` 會先解包成逐 fiscal period 資料列再寫 Parquet，禁止
保存單列 `result + metadata` JSON wrapper。啟動 migration 會檢查實體 Parquet schema，只重排確實
有 wrapper 證據的舊 shard。若 OpenBB 的嚴格欄位模型只因一個 XBRL mapping cell 型別不符而失敗，
adapter 僅移除與 validation error 的欄位及輸入值完全匹配的 cell，保留其餘 statement，並在該期
`openbb_validation_recoveries` 記錄欄位、錯誤類型與原值；結構錯誤與無法精確匹配的錯誤仍會失敗。

SEC `equity.fundamental.filings` 對美股使用每個 symbol 一個完整歷史 task。SEC provider 以 `limit=0` 讀取 filer 的 recent submissions 與所有歷史 submissions shards，再過濾 2000 年後資料；非美股不嘗試無效的 SEC CIK 查找，直接保留 FMP／Intrinio 逐年 fallback，避免 provider 的完整歷史方案限制掩蓋可取得的近期年份。這把 filings 從 468,936 個舊任務縮減為 88,403 個任務。

SEC `equity.ownership.insider_trading` 依官方原生單位處理：2006 年起每個已發布季度只下載一次 Form 3/4/5 structured-data ZIP，尚未發布的當季與 2000–2005 區間則只讀和請求日期相交的 filer submissions shards，再下載所需 ownership XML；不再為每個 symbol 掃過全部歷史 shard。季度 ZIP、submission JSON 與 XML 都原子保存於 `_state/raw_cache/sec_insider_transactions`，中斷後不重抓，快取命中也不取得 limiter ticket。SEC 對沒有電子信箱格式的 User-Agent 會回 403；可用 `SEC_USER_AGENT='組織名稱 聯絡信箱'` 設定符合官方 fair-access 說明的真實識別，未設定時使用可運作的保留範例位址。SEC `companyfacts` URL 的明確 404 XML 回應視為該 CIK 沒有 JSON 資源，不消耗永久錯誤預算。
Form 3/4/5 的空 XML element 會被 `xmltodict` 解成 `None`；archive 在 OpenBB parser 邊界只把 parser 明確要求為 mapping 的 null container 正規化為空 mapping，並把每個欄位路徑與 `null_mapping` 類型寫入該列 `openbb_validation_recoveries`。例如合法 holding 的空 `transactionCoding` 不再使整份 filing 永久失敗；任何非 null 的非預期結構仍 fail closed。

SEC `equity.shorts.fails_to_deliver` 不再為每個股票重複呼叫預設僅最近 24 份報告的 symbol route。SEC 官方資料是每半月一個包含全部股票的 bulk ZIP，因此 planner 以 `report=YYYYMMa/b` 建立自 2009 年起的穩定 checkpoint，每份只下載、解析與保存一次，並依 archive 起訖日過濾列。N‑PORT 同一報告期間若同時存在原件與 amendment，會選 filing date 最新的一份；合法的空 `<invstOrSecs/>` 與重複 mapping/list 容器會在 OpenBB transformer 邊界正規化，未知非空型別則保留為可重試的 schema 錯誤，不會誤判為資料永久不存在。SEC 限速設在真正的 HTTP 邊界：所有 `*.sec.gov` 同步／非同步 helper 與 `asyncio.gather` 子請求，以及自訂 N‑PORT、filings、FTD、companyfacts 的直接 HTTP，都逐請求共用同一個 10 req/s limiter。因此 Form 4 單一 OpenBB 呼叫即使同時展開最多 8 份 filing，也不會把 8 個子請求錯算成一個 ticket。archive 另以 manifest＋原子 Parquet 作為唯一續傳層，對所有支援 `use_cache` 的 route 停用 OpenBB 內部 HTTP SQLite cache；這避免多 event loop 共用 cache connection 的關閉競態，也使 limiter claim 與真正網路請求一一對齊。rolling scheduler 會在 SQLite claim 前依第一個可用 provider 保留有界 queue；每個 provider 有自己的 bounded executor，queue 中超過服務容量的任務只佔記憶體、不建立執行緒，進入 execution slot 後才由 HTTP limiter 逐子請求限制 10 req/s。因此 manifest／Parquet 控制面忙碌時，SEC 與其他 provider 的資料面仍可獨立續跑，也不會形成 concurrency-busy → pending 的容量空轉。

N‑PORT 的 contract normalization 不只處理最外層空容器：identifiers、counterparties、`descRefInstrmnt`、swap reset tenor、repo collateral 等 OpenBB transformer 會直接解參照的 nullable optional mappings 會一次正規化。缺少的 optional 金額以 NaN 經 pandas/model 邊界保存為 null，不偽造成零；任何未知非空型別仍 fail closed 並保留為可重試 schema drift。

FMP `equity.ownership.institutional` 與 `equity.ownership.major_holders` 不採用 provider 的「最新一季」預設，而是對美股逐一建立 2000 年後每個已完成季度的 checkpoint。major holders 每頁固定 100 筆，只有滿頁才建立下一頁，短頁才證明該季度已完整；全量稽核會由成功 parent Parquet 反推並核對 continuation。`equity.fundamental.management_compensation` 使用 FMP 的 `year=0` 全年度模式，dividends 將上限提高至 10,000，避免先截到最近資料才套日期篩選。

FMP `equity.estimates.price_target` 不再停在預設 100 筆，`equity.ownership.government_trades` 也不再停在預設 1,000 筆。兩者都逐頁讀到短頁／空頁，逐頁通過 provider-wide limiter、即時更新內層進度，再依 2000 年後日期範圍過濾；House 與 Senate 分別追蹤完成狀態，已結束的 chamber 不再送無效請求。所有自訂 FMP 分頁不再使用 99／999／10,000 這類任意最大頁數；唯一完成證據是 terminal short/empty page。若 API 忽略 page 而重複內容，page fingerprint 會使任務明確失敗，避免假完成或無限迴圈。啟動時也會跨所有 FMP paginated endpoints 從既有成功滿頁補建缺少的下一頁。

BLS `economy.survey.bls_series` 使用每批 50 個 series，並把 2000 年後資料切成最多 20 年的 API 區段。1,076,915 個 catalog series 因此形成 21,546 個可續傳 batch tasks，不再逐一建立逾一百萬個 API tasks。每個批次明確啟用 `calculations=true` 與 `aspects=true`，因此 provider 可提供的 calculations 與 observation aspects metadata 不會因 OpenBB 預設值而消失；annual average 可由月資料重建，不重複保存。HTTP 子請求改為逐一通過 provider-wide limiter；只有實際 timeout 的 50-series／20-year payload 才會遞迴二分，正常批次仍維持官方上限效率。前景執行會分別顯示 catalog 掃描、batch 建立與實際 BLS HTTP request／split 進度。BLS 將無效 registration key 明確視為 credential 錯誤，不會誤記成已確認無資料；每日 request threshold 用完時，任務會維持 pending、暫停 provider 並等待配額恢復，不消耗重試額度；HTTP 200 但 `Results` 暫時為 null 的 malformed 回應會列為 server-side transient 並保留原批次重試。

歷史 `news.company` 不使用 YFinance，因為該 adapter 會忽略 `start_date` / `end_date` 並只回傳最新新聞。所有其他 provider 的新聞列在寫入前也必須落在 task 日期範圍內；無法驗證日期或超出範圍的列不會存成歷史資料。

終端會顯示分層進度：

- 規劃：全部 endpoint 總進度，以及任務數達 1,000 的單一 endpoint 任務進度與已寫入 SQLite 數量。
- 下載：整次執行的總 task 進度，以及目前 batch 的 task 進度；同時顯示成功、空資料、失敗、新增 follow-up、目前 provider、scope 與狀態。
- Provider 內部分頁：FRED calendar、CFTC catalog、FMP filings／price targets／government trades、Congress metadata 與 BLS HTTP 子請求會顯示 page/request 進度；BLS timeout/null response 二分後，總 request 數會同步增加。
- 大型列集：SEC company facts observations、CFTC contracts、FMP/BLS 正規化、新聞日期篩選與 catalog follow-up 會顯示逐列掃描進度。
- Parquet 落盤：每個 task 會依序顯示 `enrich rows`、`build Arrow table`、`zstd parquet write`、`fsync and publish` 四階段；超過 1,000 列時另顯示逐列 enrichment。
- 大型 catalog：BLS 等大量 follow-up 會另顯示逐筆掃描、code-map 保存、日期篩選、批次建立與實際 API request 進度。
- 新發現的 follow-up tasks 會即時加入下載總數，因此總數在執行中可能增加。
- retryable pending task 會依 `updated_at` 移到 endpoint 佇列尾端；provider cooldown 不會讓同一小批 task 反覆占用 worker 或餓死後續任務。
- manifest 會在每筆 task 的 `provider_outcomes_json` 持久化各 provider 已確認的 empty／unavailable／permanent 結果；unavailable 與 permanent 都必須在 `provider_evidence_json` 保存經遮罩的原始錯誤。舊 worker 若留下沒有 evidence 的 permanent 標記，resume maintenance 只移除該 provider 標記並重新排程，既有成功 fallback shard 保留到新結果原子發布。rolling scheduler 的 SQL 候選條件是「至少還有一個尚未完成且未冷卻的 provider」，因此 `SEC → FMP` 或 `FMP → Yahoo` 不會因其中一個 provider 冷卻而整條停住，也不會在配額恢復後重做已確認 empty 的 provider；同一 endpoint 內的 FMP-only 大型 backlog 也不能遮蔽有 SEC/Yahoo fallback 的 task。只有所有尚未完成 provider 都在 cooldown 的 task 才等待最早截止時間。明確 `--retry-empty`／`--refresh` 會清除對應 outcome 以真的重新抓取；預設 failed retry 保留有 evidence 的 permanent outcome，只有明確 `--retry-permanent` 才重新驗證已證實的永久失敗路由。
- 所有市場與 provider 共用同一個外部解析契約：adapter 對上游 null/list 結構誤呼叫 `.get()`、`.items()` 或 `.values()` 是可恢復的 schema negotiation error，不是永久無資料證據。已知 endpoint 可在嚴格、可稽核的資料邊界正規化；未知結構保持 retryable，等 adapter revision 後再處理。一次性、plan-scoped migration 會跨全部 endpoint 移除舊版誤記的 parser-shape permanent outcome，只重試受影響 provider 並保留其他 fallback outcome。
- 下載器使用 rolling refill，不等待固定 batch 的最後一個 straggler。supervisor 預設最多保留 1792 個已領取工作；每個 provider 依自己的 RPS 維持約 120 秒、最多 512 筆的有界 queue，並由獨立的 bounded executor 消費。排隊任務留在 executor 的內部 queue，不會各自建立一條阻塞 provider semaphore 的作業系統執行緒；lane thread 上限只涵蓋該 provider 的自適應服務容量，所有 lane 真正執行中的工作另共用 `--workers` 全域硬上限。provider I/O 返回並釋放 slot 後，同一 provider executor 可立即接續預取任務並平行展開 catalog follow-up；主控執行緒只接收已縮小的 follow-up task。大型 SEC/FRED/Congress catalog 的 follow-up 以 8192 筆為一個 durable SQLite chunk，每個 chunk 都有獨立進度更新，並重新檢查所有 provider 的 refill threshold 與全域完成背壓；所有子任務仍會在父任務標記 success 前落盤，因此維持 crash ordering，同時不讓其他市場等待整個數十萬筆 catalog transaction。`wait(FIRST_COMPLETED)` 若一次累積大量已完成 futures，控制面每次最多取 256 筆，以一個 durable transaction 批次持久化；executor 已在發布 Future 前清除 normalized records，因此 retained completed 超過 1024 筆才暫停 producer admission，至少保留約 768 個全域工作槽供 API lane 使用。全域 queue 仍硬限 1792，supervisor 另以 16 GiB RSS 強制重啟，故不會無界保留結果。active provider 的 refill threshold 永遠介於 execution limit 與 queue limit 之間；即使全域容量一次只釋出一筆，也會在後續 control-plane turns 持續補貨，不會等最後一筆跑完。active provider 的日常補貨只讀它自己的 indexed endpoint；完整 135-route 公平掃描每五分鐘或在 provider-specific 補貨確認耗盡時執行，用來發現新 route/provider，而不是每消耗 5% queue 就掃全部市場。排程器只 claim 各 provider 尚有 reservation 的任務，因此任何市場的大型單一-provider backlog 都不能遮蔽其他市場／provider，也不再把明知沒有容量的任務反覆改寫 running→pending。若某 endpoint 的完整主要＋備援鏈目前都在 cooldown／unavailable，該 route 會在 SQLite 查詢前暫時排除；例如 FMP-only 的 150 萬筆 endpoint 不會在每日 quota 冷卻期間每次 refill 都全表掃描，provider 恢復時則在下一次公平掃描重新納入。公平掃描內的 endpoint 與 provider reservation 仍輪轉，並優先探測尚無任何 success／authoritative-empty 證據的 endpoint；provider-specific 快速補貨採 progressive rounds，先讓全部 selected routes 各取公平份額，再把剩餘 queue 容量反覆補給實際仍有 backlog 的 routes。這避免像 FRED 僅一個活躍 route 卻被 36 個已選 route 稀釋成每輪只補 7 筆，也同時適用 SEC、Yahoo、FMP 與其他市場。
- 每次 initial plan 都有一個 generation。新版 universe 或 planner 不再產生的舊 initial task 會設成 inactive，監控、下載、逐檔稽核與壓縮只讀取 `archive_meta.active_plan_token` 對應的 active tasks；既有 Parquet 不會被刪除，catalog 衍生的 follow-up tasks 也不會被誤移除。
- Benzinga、Intrinio 等完全不能匿名使用的 provider 若沒有必要金鑰，會在規劃階段從 selected providers 排除。只剩這類 provider 的 endpoint 會記為 `unavailable`，不會建立永遠失敗的下載任務。
- API 在執行時明確回覆目前訂閱不支援某一 route 時，只停用該 provider/endpoint；其他同 provider 路徑仍會繼續。停用證據會與 cooldown 一起持久化，避免 supervisor 重啟後重複測試相同 402/403。若該 endpoint 的 pending task 只有這一個 provider、沒有 fallback，manifest 會用一次 SQL 批次標成 `unavailable`，不再為百萬個 symbol/year/quarter task 逐筆重送；含 fallback 的任務保持 pending 並交給下一 provider。`--refresh` 會清除這份 runtime entitlement 狀態，供 API key 或方案升級後重新驗證。監控將 `unavailable` 與 `success`／`empty` 分列，最終稽核不會把訂閱外資料偽裝成已下載。
- EconDB `economy.export_destinations` 的網站 widget 若回 HTTP 500，會改由它標示的原始來源 UN Comtrade public preview API 重建最近可用年度的目的地資料；輸出保留 `reference_year`、`source` 與 `source_url`。UN API 使用獨立 8 req/s limiter 與 429 退避。
- EconDB `economy.indicators` 會把 provider 靜態 units map 中合法 series 的 `null` 單位正規化為空單位，再交回原 OpenBB extraction/validation 流程；避免 `CONFAU`、`CONFIL`、`CONFKR`、`CONFUS`、`RCINO`、`WAGEMANMX` 因 adapter 對 null 呼叫 `.replace()` 而遺失資料，不改動日期或數值。
- `fixedincome.government.yield_curve` 依 provider 的真實 HTTP 語意規劃，而不是把合併後的 OpenBB schema 當成 upstream 行為。EconDB、Federal Reserve 與 FRED 都先下載完整歷史 series，再由 OpenBB 在本機套用 `date`；planner 對這三者以每個國家／曲線類型一個 archive task 傳入逗號分隔的完整日期網格。FMP 會把日期轉成實際 URL 的小範圍，因此只有在沒有任何完整歷史 provider 時才保留逐日可續傳 shard。planner state 版本會讓既有 manifest 在下次續傳時停用所有舊逐日完整歷史 task；其他 endpoint 已完成狀態保持不變。
- EconDB 的通用 yield-curve transform 會為日期網格中的每一天重新掃描完整 DatetimeIndex，2000+ 日資料因此是 O(T²)。archive worker 保留其一次完整歷史 HTTP extraction，但直接以 O(T) 展開並篩選 upstream 原生日 observations；零利率也會保留。每個國家仍是一個可續傳 Parquet shard。
- EconDB 現行官方流程要求註冊帳戶 API key；舊版 OpenBB 的 anonymous `create_token` helper 可能只收到 `code=anon`，再把 series API 的未認證回應誤判成 empty。worker 優先使用 `econdb_api_key`，否則只單次讀取本機既有的有效 cached token；認證失敗會標成 auth/unavailable 而非資料空。舊的錯誤 country-archive empty 會精準重排一次。
- FMP `equity.discovery.filings` 逐頁讀取時會顯示內層進度；若原始列缺少 `acceptedDate`，以 `filingDate` 當日零時補值並標記 `accepted_date_inferred=true`。FMP `Limit Reach` 會視為 quota cooldown，不會誤記成空資料或耗盡總嘗試次數。
- EIA petroleum status report 會對每個 workbook category 明確使用 `table=all`；唯一拒絕 `all` 的 `weekly_estimates` 會逐一枚舉其全部 tables，不再只保存預設 stocks。planner 動態比較 installed provider 的 `WpsrTableMap` 與 declared choices；真實 workbook table 若因 provider schema 拼字不一致被外層拒絕，會直接經底層 fetcher 解析而不刪除資料。已驗證 Data 13 `inputs_utilization_avg` 可保存 2000 年起的 58,032 rows。FMP ETF catalog 也會按 OpenBB 支援的每個 exchange 分區，避開 adapter 內部未暴露的 10,000 列總表上限。
- NY Fed SOMA holdings 不再用「每週三」猜測日期。planner 先讀官方 `asOfDates` catalog，對要求範圍內每個真實日期建立 treasury/agency holdings 與 WAM 四種 task；provider 對沒有 agency WAM 的有效日期回傳 `{}` 時，worker 會保存為有證據的 `empty`，而不是讓缺少 `date` 的資料模型驗證反覆失敗。summary 與 monthly 全歷史模式另行保存。
- Yahoo `etf.info` 若回傳有效基金 metadata 但省略 `longName`，下載器沿用 provider extraction 並以 symbol 作 traceable name fallback，輸出 `name_inferred_from_symbol=true`；不會因 OpenBB nullable-but-required 的 `name` schema 把整筆資料丟棄。一次性 manifest migration 只重排符合這個精確 validation signature 的舊任務。Yahoo 限速設在 yfinance 實際 session request 邊界：cookie、crumb 取得／刷新、被拒後重試與最後資料請求都分別計入同一個 8 req/s limiter；不再把可能展開多個 HTTP request 的一個 OpenBB command 錯算成一個 request。
- Tiingo 仍只作最後 fallback；目前 entitlement 若拒絕 2020 年前歷史價格，下載器會把 Tiingo 子查詢裁到 `2020-01-01`，保存其可取得的尾端資料，不改動 primary provider 的 2000 年起始範圍。Tiingo news adapter 原本會忽略 `offset` 並固定只取第一個 1,000 筆；備援路徑現在直接逐 offset 下載到短頁，並以 page fingerprint 防止重複頁。Tiingo 的 `hourly request allocation` 回覆由中央錯誤分類器視為 provider-wide quota，會套用一小時冷卻且不消耗任務重試額度；daily allocation 依官方 midnight EST reset，延後五分鐘再恢復，不再誤用 UTC 日界線。Starter 公開額度 50/hour、1,000/day 會進入 quota/ETA，Tiingo 本身不設每秒或每分鐘上限，8 req/s 只作 client smoothing。一次性 migration 會跨所有 category／endpoint 清除舊版誤記的 Tiingo permanent outcome，同時保留其他 provider 已確認的 empty／unavailable 證據。
- Congress.gov bill/amendment info 使用 60 秒 timeout，所有 base metadata 與 cosponsors、actions、subjects、summaries、committees、titles、related bills、text-version metadata 都逐頁通過共享 limiter。只保存 JSON metadata 與文件 URL，不下載 PDF/HTML body，provenance URL 不含 API key。HTTP/status 5xx（包含 Congress.gov 曾回覆的 520）一律視為可重試的 server-side transient，不會誤耗盡永久失敗額度。
- 所有狀態仍以 SQLite manifest 為準；關閉終端或中斷後不依賴畫面上的進度續傳。
- 每個含 `start_date`／`end_date` 的歷史任務在 Parquet 發布前會再做一次通用日期邊界檢查，
  防止 provider 忽略查詢日期而把區間外 observation 寫入資料；沒有可辨識日期欄位的 metadata 列則保留。

小規模驗證：

```bash
./scripts/run_openbb_archive_download.sh \
  --limit-symbols 5 \
  --max-tasks 100 \
  --workers 4
```

只下載指定端點：

```bash
./scripts/run_openbb_archive_download.sh \
  --endpoint equity.price.historical
```

只建立完整任務 manifest、不呼叫 API：

```bash
./scripts/run_openbb_archive_download.sh --plan-only
```

## API 上限

每個 provider 都有自己的 limiter 與 concurrency semaphore，不共享一個全域 RPS。決策規則是：官方有公布瞬時或 sustained ceiling 就用該上限；只公布 hourly/daily allocation 或完全沒有數字時，瞬時值依操作規則使用 `8 req/s`，配額耗盡則另外以 durable cooldown 等待 reset。

| Provider | 生效 RPS | 依據 |
|---|---:|---|
| SEC | 10 | [官方 fair-access 上限](https://www.sec.gov/about/developer-resources) |
| BLS | 5 | [50 requests / 10 seconds](https://www.bls.gov/developers/api_faqs.htm)；registered v2 另有 500 queries/day |
| FRED | 2 | OpenBB FRED provider 依 120/min ceiling 設定；[FRED 官方頁](https://fred.stlouisfed.org/docs/api/fred/errors.html)目前只確認超限回 429，沒有公開數字 |
| Congress.gov | 1.388889 | [5,000 requests/hour](https://github.com/LibraryOfCongress/api.congress.gov/) |
| EIA | 2.5 | [sustained <9,000/hour，burst <5/second](https://www.eia.gov/opendata/faqs.php)，長期下載以 2.5 為 binding limit |
| OECD | 0.016667 | [60 data downloads/hour](https://www.oecd.org/en/data/insights/data-explainers/2024/11/Api-best-practices-and-recommendations.html) |
| TradingEconomics | 2 | [官方 general limit](https://docs.tradingeconomics.com/get_started/rate-limits/) |
| Intrinio ordinary free-feed | 100 | [官方 free-feed throttle](https://docs.intrinio.com/documentation/api_v2/limits) |
| Intrinio `page_size > 100`／bulk（free） | 0.016667 | 同一官方文件的 1 request/minute route bucket；paid 可覆寫為 1 req/s |
| UN Comtrade preview | 8 | [官方只公布 preview 500-row 與超限後 1 秒再試，沒有 sustained request ceiling](https://uncomtrade.org/docs/what-is-data-preview/)；依未公布規則使用 8，429 的 1 秒只作退避；只作 EconDB export-destination 備援，獨立計速 |
| Benzinga、CFTC、EconDB、Federal Reserve、Government US、IMF、FMP Basic、Tiingo、YFinance | 8 | 沒有可套用的公開瞬時數字，採操作規則預設值；FMP Basic 公布 250/day 但沒有分鐘值。Tiingo 不設瞬時上限，8/s 只作平滑；Starter 另有 50/hour、1,000/day，Power 為 10,000/hour、100,000/day |

這份來源與判定也會保存到 `catalog/provider_rate_limits.parquet`；程序實際值（包含 CLI override）會寫入 `_state/provider_cooldowns.json` 並由 monitor 的 `provider_runtime_limits` 顯示。SEC filings shards、NPORT search/XML、companyfacts symbol map、Yahoo cookie/crumb/data requests，以及其他 15 個實際 OpenBB provider 都在真實 HTTP start 計票，不只限制最外層函式。一般 provider 由 host 對應到獨立 bucket，並同時攔截 aiohttp、requests、urllib 與同步／非同步 httpx；所以任何市場或 endpoint 一個 command 展開多個 URL 時，每個 URL 都個別限速。自訂分頁先取得的 ticket 會由下一個 HTTP boundary 消耗，不會重複計兩次；Intrinio 大頁面／bulk 則需同時取得 ordinary 與低速 route-bucket ticket。

若帳號方案明確允許不同速度，可明確覆寫：

```bash
./scripts/run_openbb_archive_download.sh \
  --provider-rps fmp=10 \
  --provider-rps intrinio_large_page=1 \
  --provider-concurrency fmp=8 \
  --workers 24
```

全域 I/O worker 預設為 `112 × CPU`、最少 256、最多 1792（目前 16-thread 主機因此使用 1792），supervisor queue 預設也是 1792。RPS 是不可超越的 request-start 上限，不是 concurrency。execution slots 初值依 Little’s Law 配為 `RPS × 3.5 秒`、一般來源最多 28；另以 `RPS × 120 秒`（單一 provider 最多 512）配置已 claim 的 standby queue，吸收 indexed refill、低頻全市場公平掃描、manifest commit 與結果 discovery 延遲。每個 provider executor 只為目前 execution slots 加一次 `ceil(RPS × 0.5)` 自適應步幅建立 thread，且不超過 `RPS × 15 秒`、最多 128 的 adaptive cap；超出的 standby task 留在內部 queue 而不建立 thread。這避免把尚未由實測證明需要的 15 秒最壞延遲 ceiling 預先變成 OS thread／GIL 競爭。所有 lane 真正執行中的工作再共用全域 worker semaphore。

所有市場共用同一個自適應規則：session 至少 30 秒、provider slots 全滿、limiter waiter 為零且最近 request-start 低於目標 95% 時，才以 `ceil(RPS × 0.5)` 小步增加該 provider 的 resizable execution capacity；一旦有持續 waiter就停止，RPS 本身永遠不變。SEC 的 outer task 即使包含 fanout／解析也遵循同一證據規則，因為自適應只增加供給、每個 child HTTP start 仍受 10 req/s 邊界限制。SEC 初值 72、adaptive cap 128、executor 初始最多 77 threads；FRED 初值 8／executor 9；Congress 初值 4／executor 5；一般 8 RPS provider 初值 28／executor 32；Intrinio 初值 100／executor 128，OECD 保持 1。global worker/queue 仍把所有 provider standby 合計硬限在 1792，避免無界 task／thread。limiter 使用絕對時槽，吸收小於一個 interval 的 OS/GIL 喚醒延遲；若整個 interval 都錯過則從目前時間重設，因此不會用 catch-up burst 補發。可用 `--workers` 與 `--provider-concurrency` 明確覆寫初始容量。

每個 provider limiter 各有一條只負責 FIFO ticket 的 daemon dispatcher。dispatcher 取得 host-global 時槽後立即喚醒一個 data worker，不執行 OpenBB 網路、解析、Parquet 或 telemetry persistence；claim 記帳另送到同 provider 的有序 observer queue。即使 JSON checkpoint 正在寫入或 runtime state lock 忙碌，下一個 API 時槽也不會被診斷工作延遲。不同 provider 的 dispatcher／observer 彼此獨立，官方上限或 8 req/s 預設可同時運作。

配額封鎖除了保存到 archive 的 JSON checkpoint，也會延伸同 provider 的
`SharedRateLimiter` process-shared 時槽；因此同一台機器上不同市場或另一個下載程序不會在
其中一個程序收到 429 後立刻再次撞上相同配額。OpenBB 的 `yfinance` 與直接 Yahoo downloader
共用 `yahoo_finance` canonical bucket。

`_state/provider_cooldowns.json` 的 `rate_activity` 會每五秒保存各 provider 最近 60 秒實際取得的 limiter slots、observed claims/s 與利用率。所有 17 個 OpenBB provider、Intrinio 大頁面 bucket 與 UN Comtrade fallback 的 claim 都對應真實 HTTP start；快取命中不會製造 claim。每個 data worker 同時把 claim 歸屬到目前 endpoint，保存 `endpoint_request_costs` 的實際 requests、claiming attempts、平均值與單次最大值；dispatcher 仍只負責穩定發票，endpoint attribution 則在收到票的 worker context 完成。因此 Congress metadata 的多子資源、FRED 組合 series、EIA symbol chunks 或其他 OpenBB adapter 的隱藏 fan-out 都會進入 quota/ETA，不再假設一個 manifest task 等於一個 HTTP request。尚未執行過的 endpoint 明確以一 request/task 作樂觀預設並標成 unobserved。

UTC 小時與 provider 自己的日額度視窗也會保存並跨 supervisor 重啟累加：BLS 使用 US/Eastern 日曆日，FMP 使用官方 15:00 EST reset，Tiingo 使用 midnight EST，其餘未指定來源使用 UTC 日。已公布的 BLS 500/day、FMP Basic 250/day、Tiingo Starter 50/hour 與 1,000/day、Congress 5,000/hour、EIA 9,000/hour 與 OECD 60/hour 會同時顯示已用量和剩餘量；未公布數字的 provider 在第一次 429/quota 回覆時保存撞限視窗與 managed claim count，並明確標記為觀測證據而非推定方案上限。這是判定是否真的跑到 API 上限的依據；task/min 只代表完成資料工作，不能替代 request-start 遙測。FMP key metrics／financial ratios 另修正 OpenBB adapter 無條件同時打 historical 與 TTM URL、再丟棄其中一份的浪費：`ttm=exclude` 與 `ttm=only` 各只發必要的一次請求，只有 `ttm=include` 才發兩次，輸出模型與欄位語意不變。FRED series 也不再為每個 observation task 額外抓一次最後未保存的 `/fred/series` metadata；archive 只需要的 `/fred/series/observations` 保持 provider query/result model 與相同資料列，一 task 由兩次降為一次 request。endpoint 實作若改變 request 成本，revision 只淘汰該 endpoint 的舊樣本，不會清掉其他市場已累積的 fan-out 證據。

遇到 429/quota 時會封鎖該 provider 一段時間並繼續其他 provider；`--quota-cooldown` 是沒有明確視窗時的保守預設。下載器以所有 provider 共用的語意分類解析上游回覆：FMP Basic `Limit Reach` 等到官方每日 15:00 EST reset 後五分鐘，BLS daily threshold 等到下一個 US/Eastern 日曆日後五分鐘，Tiingo daily allocation 等到下一個 midnight EST 後五分鐘，其他沒有專屬規則的明確 daily allocation 才使用下一個 UTC 日界線；明確 hourly allocation 則等一小時加一分鐘。因此不會把每日上限誤當成每小時上限反覆試，也不會讓 Tiingo 在真正重置前五小時提早重試。載入舊 checkpoint 時也會把曾誤存成一小時或 UTC 日界線的 daily cooldown 升級到正確完整日視窗。所有 provider 的有效 cooldown 截止時間會原子保存到 `_state/provider_cooldowns.json`，supervisor、RSS watchdog 或人工重啟後仍會先恢復同一配額視窗，不會因程序記憶體清空又撞一次上限。429 與 SEC fair-access cooldown 不消耗 task 的有限錯誤次數，因此配額視窗恢復後仍可續傳，不會被誤列為永久 exhausted。每個 worker 會在正常 RPS wait 後、真正送出 API 前再次檢查 cooldown；競態中已進入 limiter 的 worker會以專用 deferred 訊號立即無損退回 pending，該訊號不會再次延長已存在的 cooldown。舊版曾因 cooldown 累積 attempts 的未完成列會依持久化的 cooldown error 證據重置為 0，讓新版取得完整且真實的重試額度。缺少或無效 credential 只會停用該 provider，不會清掉其他已完成任務。CFTC 在程序內強制使用匿名 Public Reporting API，不使用已失效 token。
FRED 的 OpenBB 內部 limiter 也會在 import 前設定成同一個間隔，避免 provider 內部分頁使用另一個速率。Intrinio 的 ordinary free-feed 與 large-page/bulk 是不同限制；目前沒有 Intrinio credential，因此沒有 active task，但下載器已依 URL 的 `page_size`／bulk path 自動分流：free 預設 1/min，paid 帳號可用 `--provider-rps intrinio_large_page=1` 提升為 1/s，ordinary bucket 仍獨立維持 100/s。UN Comtrade direct fallback 沒有公布 sustained ceiling，因此依規則使用獨立 8 req/s limiter 與遙測；官方錯誤訊息的 1 秒只用於 429 backoff，不併入 EconDB 的 8 req/s。

速率遙測的時間戳在 cadence-owning dispatcher 真正 grant slot 的瞬間記錄；較慢的 durable quota observer 另顯示 `limiter_observed_claims_total` 與 `pending_claim_observations`，其排隊延遲不會被誤報成 API 未跑滿。archive 進入 provider execution 前會把本 process 的 Python thread switch interval 設為 1 ms，避免 XML／JSON／pandas CPU 工作以預設 5 ms GIL 時片延遲所有 provider dispatcher。provider lane 降到約 190 threads 後，0.5 ms 雖在隔離壓測優於 1 ms，但完整 archive live workload 的 SEC、Yahoo、FRED、Congress grant utilization 同向下降、CPU 上升且 scheduler checkpoint 變舊，因此正式預設維持 live-validated 1 ms；0.25 ms 曾在舊 300+ thread 工作量產生更高 context-switch overhead，同樣不採用。可用 `OPENBB_THREAD_SWITCH_INTERVAL_SECONDS` 明確覆寫。這只改善排程供給，host-global limiter 的 RPS 上限仍不變。

## DuckDB 合併與 Polars/PyArrow 驗證

下載完成或累積一批資料後執行：

```bash
./scripts/run_openbb_archive_compaction.sh
```

處理流程：

1. DuckDB 以 `union_by_name` 串流讀取同 endpoint 的所有 task Parquet。
2. 輸出 Zstandard compact Parquet 至 `data_openBB/compact/`。
3. PyArrow 與 Polars 各自核對 row count，Polars 再計算可用日期範圍。
4. 建立 `data_openBB/openbb.duckdb` query views。
5. 將結果保存到 `data_openBB/catalog/compaction_summary.parquet`，未變動 endpoint 下次直接略過。

互動下載會顯示 bootstrap、manifest schema、初始規劃總進度、每個大型 endpoint 的任務展開、manifest maintenance、prepare/resume、下載總量、rolling scheduler，以及 CFTC/FRED/BLS/SEC/index/Congress 等大型 catalog 的逐筆 follow-up 展開。SQLite 無法對任意 UPDATE/JSON scan 提供可靠百分比，因此百萬列 statement 另外顯示持續累加的 `vm-batch` 工作量與 elapsed time；已成功 shard 的存在性檢查則有精確逐檔百分比與 missing 計數。當一批 API task 長時間尚未返回時，rolling scheduler 每秒顯示最早提交且尚未完成的 endpoint、scope、`oldest_age_s`、inflight 數與 worker 數；這個年齡包含 executor 排隊時間，不會誤稱為單一 API 的純執行時間。監控命令也顯示 manifest aggregate、plan boundary、FRED/FMP pagination、retryable failure clusters 與逐檔 audit 的分層進度。壓縮時會分別顯示 manifest 掃描、全部 endpoint、單一 endpoint 的五個 stage、超過 1,000 個 shard 時的逐檔 signature，以及耗時超過一秒的 DuckDB 查詢內部進度。可在任何下載、監控或壓縮命令加上 `--no-progress` 關閉所有進度顯示。

`index.constituents` 只規劃 OpenBB/FMP 支援的 `dowjones`、`sp500`、`nasdaq` 三個別名；`index.available` 的其他代碼只展開歷史價格，不會產生永遠無法通過驗證的 constituents 任務。

FRED release series 使用每頁 1,000 筆的 offset 分頁：只有回傳滿頁才建立下一頁，短頁或空頁才視為該 release 已完整。啟動時也會掃描既有的滿頁成功任務並補建 continuation，避免舊 manifest 把截斷頁誤認為完整；接著逐檔掃描全部成功的 release catalog，以獨立進度條核對現有 series scope，僅分批補建真正遺漏的 follow-up tasks。

全球經濟 calendar 採逐月 checkpoint；每月內仍會依 FRED `count` 完整分頁。這避免近年單一年度數千筆資料在任一頁 timeout 後從全年 offset 0 重來。TradingEconomics 不支援此 archive 使用的 global `country=all`，因此 calendar 僅使用 FRED 主來源與 FMP 備援。

DuckDB 查詢例：

```bash
source scripts/runtime_env.sh
run_fintech_python - <<'PY'
import duckdb

con = duckdb.connect("data_openBB/openbb.duckdb", read_only=True)
print(con.sql("SHOW ALL TABLES"))
print(con.sql("SELECT * FROM openbb_equity_price_historical LIMIT 10"))
PY
```

compact 檔是原始 task shards 的查詢最佳化副本；預設不刪除 shards，避免失去逐任務續傳與稽核依據。

## 主要狀態檔

- `data_openBB/_state/openbb_archive.sqlite3`：逐任務狀態、各 fallback provider 的 durable outcome、失敗原因、provider 事件、compact 狀態。
- `data_openBB/_state/provider_cooldowns.json`：provider RPS/concurrency、跨重啟的小時／日 request-start 計數、逐 endpoint 實測 HTTP fan-out 成本、已知配額剩餘量、撞限觀測、所有尚未到期的 cooldown，以及已證實的 provider/route entitlement 狀態。
- `data_openBB/_state/provider_scheduler.json`：每個 provider 獨立 execution pool／queue 的 executor-submitted active、尚未提交 buffered、reservation、refill threshold、是否啟用 queue preload、等待 manifest persistence 的 `completed_pending_total`、`completion_persistence_batch_size`、`completion_backpressure_limit`、`completion_backpressure_active` 與 cooldown 狀態；真正正在持有 provider slot 的數量以 `provider_cooldowns.json` 的 `active_calls` 為準。
- manifest 排程只保留 `idx_tasks_schedule_age_v2`（`active/status/plan/category/endpoint/updated_at/task_id`）與 `idx_tasks_active_plan` 兩個非唯一索引；啟動時會移除舊的 `idx_tasks_status`、`idx_tasks_schedule`、`idx_tasks_schedule_age`。在約 820 萬 tasks 的實測資料庫，三個舊索引合計約 4.63 GiB，且會讓每個 catalog follow-up 多寫三棵 B-tree；移除後空頁由後續 SQLite 寫入重用，不需在下載熱路徑執行 `VACUUM`。
- `data_openBB/_state/request_checkpoints/`：尚未完成的 Congress 多頁／多子資源 task 暫存；成功 Parquet 發布後自動清除，讓 transient retry 只補缺頁。
- `data_openBB/_state/last_run_summary.json`：最近一次執行摘要。
- `data_openBB/_state/monitor_latest.json`：最近一次監控快照。
- `data_openBB/_state/audit_latest.json`：最近一次完成的全量 Parquet／catalog follow-up 實體稽核；一般完整健康快照不會覆蓋它。
- `data_openBB/_state/monitor_history.jsonl`：完整監控歷史；supervisor 預設每 15 分鐘新增一筆，可計算實際吞吐量。
- `data_openBB/logs/supervisor.log`：背景下載、自動重啟、最終稽核與壓縮日誌。
- `data_openBB/catalog/coverage.parquet`：所有 OpenBB 端點的 included/excluded/deferred/not_enumerable 決策。
- `data_openBB/catalog/equity_universe.parquet`：實際規劃的 US/TW symbol universe。
- `data_openBB/catalog/provider_eta.parquet`：逐 provider 的 eligible（備援鏈重疊）與 exclusive（不可繞過）backlog、rolling ETA、RPS 樂觀下限及 hourly/daily quota reset 下限。
- `data_openBB/catalog/category_eta.parquet`：逐資料類別的 rolling ETA、exclusive provider blocker 與其不可繞過 backlog。
- `data_openBB/catalog/provider_endpoint_eta.parquet`：逐 provider／category／endpoint 的 eligible/exclusive task backlog、實測 requests/task、估計剩餘 HTTP requests，以及該成本是否已有觀測證據。
- `data_openBB/data/`：PyArrow 原子寫入的 task shards。
- `data_openBB/compact/`：DuckDB 合併後的 endpoint Parquet。
