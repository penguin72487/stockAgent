# FinMind 新聞啟用與全清冊估時

觀測時間：2026-09-29 23:48（Asia/Taipei）。這是有限截止日的排程與條件式估算，不是全歷史已完成聲明。

## 已實作並啟動

- 最新使用者指示取代先前「不要新聞」：`TaiwanStockNews` 交給既有 Complement worker，不另建下載服務。106 個已知邏輯資料集均有主責；132 個 owner 顯示項目不是 132 份獨立資料。清冊停用、未排程、無法盤點均為 0。
- 新聞一次查詢全市場一天，保留週末、休市日與未知／下市代號。2026-09-29 全市場回覆 1,554 筆、461 個代號；其中 2330 的 36 筆與獨立逐檔查詢完全相符。
- 新聞優先抓最近三天，再回補 2014 年起的研究區間，更早年度較後；tick 保持最低優先。最近三天每小時重查，補晚到資料，沿用帳號共用限流與固定追新預留。
- API 每次只提供一天。官方沒有公布最早新聞日；已驗證 2015-01-05 有資料。1900-01-01 是待搜尋下界，不是觀測起日或官方保證。46,293 個日曆日全部入列，不以交易日排除新聞；較早搜尋會產生空回，空回不當成有資料。
- 原始日期、標題、來源、連結以及回傳的其他欄位原樣存入內容定址 Parquet，保留版本收據。同篇文章可以關聯多個股票，不把不同股票關聯誤刪。缺少可選欄位另記，不補零、不杜撰正文、不抓第三方文章頁。
- 來源時間的時區、精度與真正發布時點未驗證，不自動變成嚴格 PIT 訓練特徵。

官方契約：[新聞接口](https://finmind.github.io/tutor/TaiwanMarket/Others/)、[完整來源目錄](https://finmind.github.io/llms-full.txt)。每次全市場範圍由本機共享限流的實測另行佐證。

## 當前下載與驗證

23:48 新聞已落盤 45 日、130,199 筆，觀測日期 2026-08-16 至 2026-09-29；尚有 46,248 日待查。筆數是文章／股票關聯紀錄，不是去重後文章篇數。

23:43 對當時已完成的 36 份新聞收據逐一驗 SHA-256、Parquet 筆數、日期、股票／連結與原始重複：109,480 筆，失敗 0，完全重複 0。之後的新下載仍經相同入口驗證，但不能把此批稽核說成已全量稽核未來檔案。

相關回歸測試 627 passed；Node 語法檢查通過。真實本機 gateway 的桌面 1440px 與手機 390px 都顯示三階段估時，JavaScript 錯誤 0、整頁水平溢出 0。另修復共享 `content-visibility:auto` 在長清冊下造成進度卡整塊空白的問題，只取消這張小摘要卡的延後渲染，保留長清冊原本的效能策略。

已重啟 Complement 與唯讀 gateway；沒有重啟交易、Shioaji 或 FinLab。其餘 FinMind workers 繼續使用共用帳號。每分鐘配額 timer 同時更新估時，不因瀏覽網頁呼叫資料 API。

## 23:48 的估時快照

| 累計範圍 | 剩餘請求／候選 | 最快情境 | 中間情境 | 保守情境 |
| --- | ---: | --- | --- | --- |
| 核心日資料、財報、總經與新聞 | 74,072 | 23.3 小時；9/30 23:05 | 26.3 小時；10/1 02:07 | 27.7 小時；10/1 03:26 |
| 全部非 tick（含分鐘、分點） | 60,291,611 | 約 2.14 年 | 約 2.43 年 | 約 2.55 年 |
| 全部含低優先 tick | 77,856,337 | 約 2.76 年 | 約 3.14 年 | 約 3.30 年 |

重要：後兩列是未完整剔除上市前、休市日等的日曆候選工作量投影，**不是已確認缺失數，也不是承諾要下載這麼多年**。例如 US 分鐘來源目前單獨約 3,669 萬個代號／日候選，占很大比例。後續有可驗證存續／交易日證據時，工作量還能下降；新增代號、失敗、停機、配額競爭則可能增加。不能為縮短數字而假定未知歷史不存在。

估時修正：

1. 當前增量欠帳只加入工作分子一次，不再把它當成永久每小時消耗反覆相減。
2. 未來增量按 worker 更新週期及新日級分區估算預留。距離下次日更只剩一分鐘，不等於每分鐘都要更新一次。
3. 最近一小時四個完整活躍 15 分鐘窗皆存在時，用該窗的 P50／P10；不足則退回 24h。它們是請求發出速度，不是成功機率，來源組成改變會使估算變動。
4. 共用官方配額 6,000 次／小時；所有階段共用同一帳號，不把三支 worker 容量相加。僅歷史回補暫停、增量仍在執行時，不錯判整個帳號停工。
5. 日曆投影加入 08:20–09:10 開盤保護；一年後未公布的日曆按平日近似。超過十年的情境僅保留工時，不捏造日期。
6. 磁碟約剩 326 GiB，全量容量尚未驗證；既有 25 GiB 防滿碟門檻保留。上述估時假設空間足夠，不是存儲容量驗收。持續增量本來就沒有永久完工日。

## 程式與可重現證據

- `downloader/finmind_news.py`：查詢範圍、日曆佇列、重查時鐘與欄位驗證。
- `downloader/download_finmind_complement.py`：沿用原本 quota、儲存與收據。
- `downloader/finmind_eta.py`、`finmind_eta_work.py`、`finmind_eta_telemetry.py`：有限工作與三階段預估；報告契約更新。
- `artifacts/data_quality/finmind_news_2026-09-29/contract_probe.json`：不含憑證或新聞正文的接口探查。
- 同目錄 `local_receipt_acceptance.json`、`current_acceptance.json`、`browser_acceptance.json` 與 `live_eta_*.png`：下載、估時、畫面證據。
- 同目錄時間戳 `finmind_query_ranges_*.csv/json`：清冊快照；以最新產出為準，舊快照保留歷史證據。

重建只讀清冊／本機估時：

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.audit_finmind_query_ranges --output-dir artifacts/data_quality/finmind_news_2026-09-29
run_fintech_python -m scripts.snapshot_finmind_quota --eta-only
run_fintech_python -m pytest -q test/test_finmind*.py test/test_probe_finmind_max_ranges.py
```

不需手動再開下載器；既有 systemd 常駐工作負責續跑，防止第二個 writer 重複取得相同資料。

## 2026-10-08：共用新聞全文補充管線

此節新增原始媒體全文下載能力，取代上文「不抓第三方文章頁」的舊實作描述；不改變 FinMind 新聞 API 的既有主責、配額、日級排程與股票關聯。工程驗證、來源授權、正文可用性、全歷史覆蓋是不同的驗收。

### 清冊與目前驗收

首次完整本機清點包含 5,664 份已接受的 FinMind／FinLab 新聞分區、4,682,289 筆新聞／股票關聯，正規化網址去重後 3,006,655 個；747 筆缺少有效連結、來源分區驗證錯誤 0。這是首次清點快照，不是全部新聞已下載；既有來源仍持續增量。本管線保留原始分區和所有股票關聯，不把一篇對應多檔股票誤刪。

其中 1,285,019 個網址是 `news.google.com` 包裝連結，不是原始媒體文章網址；已抽查的新式 token 沒有直接嵌入可還原的文章 URL。不猜測原始媒體、不把 Google 新聞頁的摘要當全文，待可驗證原始連結或授權來源解決。

2026-10-08 15:49（Asia/Taipei）離線驗收已落盤的 143 個正文版本：原始回應共 22,706,572 bytes、正文 UTF-8 共 1,167,727 bytes、完整佇列 heads 143、SHA／筆數／增量進度一致性錯誤 0。驗收涵蓋已存下的這批，不保證每個歷史網站的 DOM、所有段落或全部新聞歷史完整。當次 `historical_completeness_verified=false`、`publish=false`。

私有證據位於 `artifacts/data_quality/news_fulltext_20261008/acceptance.json`。全來源／網域／狀態清冊為 `data_news_fulltext/source_inventory.csv`；持續更新的狀態為同目錄 `status.json`，包含來源許可、已存正文、待抓、重試、待授權、錯誤與 HTTP 實際請求量。它不是公開網頁端點，不會把正文、私人路徑或授權文件送到瀏覽器。

### 第一性原理分工與效能

| 問題 | 本次實作 |
| --- | --- |
| 同一文章有多個股票關聯、多個來源 API 重複提供連結 | 一個 URL frontier，共用 SHA-256 身分；來源原始 Parquet 保留股票關聯。全文服務只讀已驗收的本機 metadata，不再呼叫 FinLab／FinMind API。 |
| 每分鐘重掃百萬筆資料耗 CPU | 收據 mtime／大小檢查後僅處理變動分區；變更才驗 SHA／schema／筆數。主頁狀態使用 SQLite 增量計數，dispatch 有索引，不在每次抓取排序全表。 |
| 不同媒體等待互相拖慢 | 4 個有界 I/O workers，獨立 publisher 共用限流；RSS、sitemap 與正文共用該來源額度。不共享額度的媒體可以並行，同來源不任意多開突破限制。 |
| 網路失敗或重啟後永久停住／重複寫入 | 單一 owner lock、lease 恢復；先 durable Parquet＋SHA 收據，再提交 SQLite。有限重試用完後一小時冷卻，再開始新的有限重試；404、付費牆、來源／授權限制不偽裝成成功。 |
| 空摘要、導覽或相關文章被當成正文 | 驗證來源特定容器／本篇 JSON-LD，排除推薦、導覽、影音與圖說；保留擷取方法、原始回應、作者、原始時間、許可與 data fingerprint。未知時區不補精確時間。 |
| 修改稿／來源收據新版讓進度重複加總 | 當前 metadata 筆數只計每份來源收據的最新已接受版本；另列歷史版本保留數。正文版本與去重後篇數分開。 |
| 把估時當成全部媒體一定能抓完 | 只對當前允許且已知待抓 URL 顯示當次成功吞吐估算；有冷卻、待授權／未知來源時不提供全來源完成日期。rolling RSS 不宣稱全部歷史。 |

不做 GPU、NER、向量化或模型訓練。平時以 systemd oneshot＋timer 休眠等待續抓；`Nice=15`、低 CPU／I/O weight，保留 25 GiB 磁碟門檻、1 GiB memory high／2 GiB hard limit。不透過縮小已知資料範圍或跳過驗證改善速度。

### 批次 API 實測與保留邊界

[WordPress 官方分頁文件](https://developer.wordpress.org/rest-api/using-the-rest-api/pagination/)規定單頁至多 100 筆；已實作公開 `content.rendered` 批次 adapter、官方總筆數／頁數檢查、原始 JSON 單份共享、逐篇錯誤與來源別 circuit breaker。測試證明 100 篇有效 fixture 只用一個正文 API 請求，不把這個上限描述成所有媒體都有或實際吞吐已達 100 倍。

真實 Global Voices 英文 API 兩頁共 200 筆、2 個 HTTP 請求、3,641,619 response bytes，來源當時回報 104,407 篇。200 筆均缺少本 adapter 所需的可驗證作者名稱，實際接受正文 0，不能把它算入已完成。隨後診斷遇到 `transport_error`／`No route to host`；保留逐篇錯誤與 workflow 記錄，不更改主機網路、繞過來源限制或造假作者。

因此 `configs/news_fulltext.json` 的 Global Voices `wordpress_apis` 目前留空，既有候選分頁留存但標示 disabled；來源特定 HTML 方式保持啟用，來源網路恢復後續抓。批次 adapter 需真實 byline／授權完整驗證後再啟用。系統性欄位失敗只冷卻該批次 adapter，不阻塞其他正文／媒體；帶明確權利例外的整份共享回應不存下，符合條件的文章另走逐頁驗證。一般擷取缺欄位可保存一份私有壓縮診斷回應，不用反覆重抓才能診斷。

### 來源條件與新增來源方式

- 自動啟用 Global Voices 的原創文字（[CC-BY／署名規則](https://globalvoices.org/about/global-voices-attribution-policy/)）：HTML 頁逐篇確認授權標記、作者與來源 URL，排除第三方影音／圖片；遵守實際 robots 的 10 秒間隔。這不是官方公布的 API 每秒配額。
- 自動啟用 Federal Reserve Board 自製新聞稿、演講與證詞文字（[官方使用聲明](https://www.federalreserve.gov/disclaimer.htm)）：不包括第三方材料、標誌，也不把新聞稿頁誤稱為其 PDF 附件全文；現有 RSS 是 rolling 範圍。
- 下文 19:22 的本機研究修正已啟用鉅亨和 CMoney 的公開文章取得入口，不再把缺少 blanket license 一律等同技術上不能下載。聯合／經濟、MoneyDJ、Yahoo、LINE、工商／中時等的明確自動化限制，以及其他未知來源／用途條件仍另列，不等同所有來源永久不能取得。FinLab VIP／FinMind Sponsor 是 metadata 供應者權限，不自動授予原始媒體全文權利。

以 `configs/news_fulltext.json` 為唯一來源註冊：設定 `domains`、feed／sitemap、正文容器、作者／許可選取器，以及取得／用途範圍。書面許可使用 `authorization.kind=written_authorization`、`full_text_download=true`、`scope` 和本機私有 `evidence_file`，程式保存文件 SHA；有期限時 `valid_until` 必須含時區。另有下文的 `personal_research` 取得依據，並不是書面許可的別名。僅切換 `enabled=true` 不會繞過來源檢查；禁止付費牆、captcha、robots 或權限繞過。

### 操作與續抓

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.download_news_fulltext status
run_fintech_python -m scripts.download_news_fulltext verify \
  --output artifacts/data_quality/news_fulltext_20261008/acceptance.json
run_fintech_python -m pytest -q test/test_news_fulltext.py test/test_finmind_news.py \
  test/test_downloader_common_progress.py test/test_downloader_rate_limits.py
```

首次或必要時重建私有全來源 CSV：`run_fintech_python -m scripts.download_news_fulltext inventory`。不要在自動 owner 正在運行時另開 writer；會拒絕而不是重複下載。正常續抓為 `scripts/run_news_fulltext.sh`，systemd `stockagent-news-fulltext.service`／`.timer` 已安裝、timer enabled。2026-10-08 22:38 起改為常駐單一 owner：開機 3 分鐘後啟動，有可抓資料時立即續批、保留暖連線；每個 cycle 最多 512 個正文嘗試、64 個 discovery 嘗試或 180 秒後排空，批次上限不是資料範圍上限。timer 的 inactive 1 分鐘是服務停止後的接續備援，不再是每 64 篇的固定空檔；故障由 systemd 每 10 秒重啟。沒有重啟 FinLab／FinMind、交易、GPU 或其他既有來源服務。

本次 102 個相關測試通過、Ruff／shell 語法通過；真實保存正文另以 SHA 收據驗收。今日回抓的歷史新聞仍是「今日觀測到的版本」，不是當年已知版本；發布日期保留來源原字串，不自動注入嚴格 PIT 訓練、不公開正文或同步到遠端訓練。

### 2026-10-08 19:22：本機個人研究模式修正

使用者明確要求本機個人、非商業研究下載。之前把「沒有公開 blanket license」當作一律不能抓的必要條件過於粗糙，本次把 **公開取得、正文品質、授權再利用** 分開：`collection_purpose=personal_research`，公開原創／轉載文字保持原來源與署名，不自動開啟再發布、商用、遠端匯出或訓練注入。這不是合理使用的法律保證；[著作權法第 51 條](https://law.moj.gov.tw/LawClass/LawSingle.aspx?pcode=J0070017&flno=51)有合理範圍等條件，[第 65 條](https://law.moj.gov.tw/LawClass/LawSingle.aspx?pcode=J0070017&flno=65)也不是只看非營利一項。尤其大量整站存檔與第三方轉載權利不可直接認定均已清除。

- 鉅亨及 CMoney 使用 `authorization.kind=personal_research`，保存已檢視的條款 URL、日期、公開存取審查與限制；這個欄位是取得目的分類，**不是 publisher license**。已識別的明確自動化禁止不能透過此模式開啟；robots 和每個跳轉的來源檢查照常適用。
- 每筆新增正文標記 `capture_policy_version=2`、`rights_status=not_cleared_for_reuse`（個人研究來源）、`fair_use_legally_determined=false`、`commercial_use_enabled=false`、`training_injection_enabled=false`，另保留作者是否存在的 `attribution_status`。來源授權文字／data fingerprint 與原始回應、正文 SHA 一起保留；舊收據與捕捉時鐘不改寫。既有獨立 metadata owners、股票關聯及配額不變。
- 實測鉅亨公開頁 `https://news.cnyes.com/news/id/6624856` 回傳 200，以本篇 JSON-LD 正文保存 405 字元／1,057 UTF-8 bytes，保留來源作者與 `2026-10-08T08:31:06.000Z`。不在文件或公開頁再刊全文；HTML fallback 限定 `#article-container`，不抓整個 article 的推薦／其他區塊。文章路徑跳轉成首頁不能接受成正文。這一篇的成功不等於所有歷史頁已完整。
- CMoney 的 robots 允許該公開文章路徑，但試查文章回傳 403；記錄來源拒絕與重試，不使用 cookies、偽裝 bot、代理、登入或被禁止的 `onlynotecontent.aspx` 端點。Global Voices 當次仍有 `transport_error`；不把這些狀態說成正文完成。
- publisher cooldown 寫入私人 checkpoint。新 timer process 在 lease 前先跳過尚在冷卻的來源，避免新 process 卡在共享 limiter 的 15 分鐘等待而拖慢其他媒體；`Retry-After` 和共用 limiter 沒有縮短。其他媒體繼續有界並行。
- 新增 `enqueue`／`fetch` 指定網址入口。UTF-8 URL 清單逐行讀取、每 5,000 行提交既有 SQLite frontier，記憶體不隨全文清單全部膨脹；全域 URL 去重、既有 retry deadline、owner lock、原子接受與 SHA 驗證共用。已存正文不重抓，`--refresh` 使用條件式 HTTP。這兩個入口不掃本機金融資料湖、不呼叫 FinLab／FinMind API。

私有驗收：`artifacts/data_quality/news_fulltext_20261008/personal_capture_acceptance.json`，2026-10-08 19:22:57（Asia/Taipei）共 145 個正文版本／145 個已驗證 queue heads，原始回應 22,979,131 bytes、正文 1,180,710 bytes，SHA、筆數、heads 與增量 metrics 錯誤 0；全歷史完整性仍為 false。121 項相關測試、Ruff、shell 語法檢查通過。工作收據為 `news-personal-final-validation-20261008T112247-f2e96547`；公開鉅亨實測收據為 `news-personal-public-article-pro-20261008T111741-01c2750c`。

```bash
source scripts/runtime_env.sh
# 指定一篇公開文章（背景 owner 正在抓時會拒絕第二個 writer）
run_fintech_python -m scripts.download_news_fulltext fetch \
  --url https://news.cnyes.com/news/id/6624856
# 匯入自己整理的網址清單，後續由同一個 timer 排程；不建立第二條下載管線
run_fintech_python -m scripts.download_news_fulltext enqueue --url-file urls.txt
run_fintech_python -m scripts.download_news_fulltext status
```

全文仍在私人 `data_news_fulltext/bodies/` 的 Zstd Parquet，未上傳、發布或注入訓練；系統不使用 GPU。

19:32 追加實跑修正與驗收：舊版共用 limiter 可能已留下長冷卻、但尚未有新聞自己的 checkpoint。`SharedRateLimiter.remaining_delay_seconds()` 新增唯讀 readiness 提示，不領取 slot、不改排程；新聞 owner 在 lease 前識別這段既有等待，仍必須經原本 `wait()` 才發出請求，並保留並行 `defer()` 的限制。來源冷卻與歷史等待都不縮短，其他 provider 的原有 limiter 行為不改。

`news-personal-cooldown-final-val-20261008T113231-f23750dd` 的 123 項測試與 Ruff／shell 檢查通過；19:32:50 的離線收據 `artifacts/data_quality/news_fulltext_20261008/personal_capture_acceptance_v2.json` 確認 208 個正文版本／208 個 queue heads、原始回應 35,554,407 bytes、正文 1,338,231 bytes，錯誤 0。其中鉅亨已保存 64 篇；其他為先前及持續增量的開放來源，不能全部算成這次新抓的鉅亨文章。原來的 145-head 收據不覆寫。只重啟新聞 unit／timer，timer 保持 enabled；更晚的即時筆數是運行快照，不自動等同這份凍結驗收已包含它們。全部已知歷史和所有媒體的完成仍未宣稱。

### 2026-10-08 22:38：新聞全文整段效率與恢復

本輪只優化新聞全文管線；FinLab／FinMind 仍是原來的 metadata owners，不增加它們的 API 呼叫。依工程偏好保留整段流程、來源限制與原始資料，不把部分成功或提高執行緒數當作全歷史完成。

| 階段 | 原瓶頸／保留邊界 | 已實作與驗收 |
| --- | --- | --- |
| 啟動與續批 | 每 64 篇重建程序與連線，另停約 1 分鐘 | 同一 SQLite owner、4 個有界 worker、暖 HTTP Session 跨 cycle 重用；就緒歷史立即續批，全來源冷卻時等到最近 deadline，每 15 秒留心跳 |
| 本機 metadata | 不能重打金融來源 API 或重讀全部資料 | 保留 accepted receipt mtime／size、完整 SHA、4096-row 串流及增量來源關聯；15 分鐘檢查變動，未變 policy fingerprint 不再掃數百萬個 URL |
| 發布與歷史分工 | 歷史 sitemap 一直優先，排擠已知缺正文 | RSS 優先；可用 bulk 正文保持優先；歷史 sitemap 與已知正文按 1:8 嘗試交錯，沒有 ready 正文時直接清點；正文批次額度用完便排空續批，不被大量 sitemap 拖住 |
| HTTP 與限流 | 同源全部串行；加併行可能造成實際開始時間擠在一起 | 鉅亨 2 個併行回應，仍至少每 2 秒才開始一個請求；Global Voices 保留 robots 至少 10 秒與 1 個併行；send reservation lock 只鎖發出時機，不鎖整個回應；其他來源互不共用此配額 |
| 網路故障 | 單次 ReadTimeout 就凍結整個 publisher 30 秒 | 失敗連線的 Session 重建、該 URL 仍退避；3 次連續 transport failure／中間無 HTTP 回應才打開來源 circuit，DNS、已確認 route／TLS 失敗立即冷卻；403／401、429、5xx、Retry-After 仍照原來源等待 |
| 正文解析 | 完整 DOM 與無 markup 的文字重複解析 | 已驗證來源改用 lxml；純文字略過不必要 DOM，實體／markup 仍解析；每筆保存 parser backend、來源 data fingerprint，既有正文／捕捉時鐘不改寫 |
| sitemap／feed 增量 | 相同 XML 反覆 parse、重新寫大量已知 URL | 成功處理才保存 response SHA＋parser fingerprint；相同 200 回應只更新 discovery 的實際檢查時間、不改文章原觀察時間；不同設定重新處理，未版本化舊 ETag 不能隱藏解析變動 |
| 壓縮、落盤與驗證 | 不能用丟 raw、少驗證或降可靠性換速度 | 保留 Zstd level 9、原始 HTML、正文與 SHA，先 fsync Parquet／receipt 再提交 SQLite；另按原始回應 16 MiB、正文 4 MiB、64 篇或 30 秒 flush 控制記憶體 |
| 停機與恢復 | shell PID 不代表 Python owner；停止時不可丟已完成結果 | canonical runtime 以 exec 換成 Python PID；SIGTERM／SIGINT 停止新工作、排空 futures／flush；故障及未知 in-flight 從既有 lease／收據恢復；設定變更或授權到期於排空邊界交接重啟 |
| 進度與估時 | 單批成功數不能代表長期速度或被擋來源 | 私有 `status.json.performance` 包含 queue 初始化、清點、dispatch、限流等待、網路、解析、註冊、壓縮、fsync、SQL、idle／CPU，並記實際 request gap、併行峰值、HTTP status；owner 時間含等待／失敗／idle，但 interpreter 啟動另在 process benchmark 中計入 |

測量與限制：

- 原流程第一次完整測試遇到 transport error，只新增 6 篇／25.10 秒；重跑 134.77 秒新增 48 篇。保留失敗／慢案例，不能只挑最快的一批。原 timer 每批約另有 60 秒 inactive 空檔。這兩次帶 cProfile，不能直接當作與無 profiling 的新實跑嚴格 A/B。
- 同樣原始資料離線、包含佇列、耐久 Parquet／receipt、獨立 verify：179 篇三輪中位數 13.69 → 12.66 秒；全保存樣本 1670 個版本／1660 個唯一 URL 的單輪 89.56 → 76.33 秒（14.8% 少 wall、CPU 77.94 → 66.61 秒）。全部正文 SHA、作者、日期等解析結果相符；這是本機離線處理量測，不是全網下載倍率或所有未來 HTML 的保證。
- 修正後無 profiling 的 240 秒目標實跑，排空後 owner wall 246.19 秒，實際整個程序 246.82 秒；新增並耐久提交 **112 篇：鉅亨 90、Global Voices 22**。平均程序約 5.4% 單核心，peak RSS 216.5 MiB；不使用 GPU。同次鉅亨至少 2.000005 秒、Global Voices 至少 10.000093 秒開始一個請求，鉅亨峰值 2 併行；一次 ReadTimeout 重建失敗 Session、其他健康請求持續。
- 官方沒有在本清點中確認鉅亨的數值總配額；2 秒是現有保守 client policy，不是「官方最高速」。網路、來源拒絕、HTML 差異與必要清點仍會改變長期吞吐量，沒有宣稱達到全網理論極限。
- 最終凍結離線收據在 22:32:16 驗證 **1839 個版本／1829 個 complete heads**、原回應 361247027 bytes、正文 5210672 bytes，SHA／rows／progress metrics 錯誤 0。163 項相關測試、Ruff／shell 通過；包含限流下的回應重疊、拒絕競態、停止排空、暖 worker、無空轉、資料界限、sitemap 不餓死正文、相同 XML no-op 及設定交接。

證據均為私人文件：`artifacts/benchmarks/news_fulltext_20261008/parser-v1.json`、`all-saved-parser-v2.json`、`repaired240.snapshot.json`／`.resource.txt`；凍結驗收 `artifacts/data_quality/news_fulltext_20261008/speed_capture_acceptance_v1.json`。工作收據為 `news-speed-repaired-live-240s-20261008T142349-e86d9f37`、`news-speed-final-integrity-20261008T143204-53302222`、`news-speed-final-regression-20261008T143606-18e481f8`。較早失敗的測試及較慢候選保留；修正後才通過，不覆蓋原證據。

部署後暖重啟驗收：原 owner PID 1842985 在正式運行中另提交 167 篇，SIGTERM 排空／flush 後正常 exit 0、無遺留 in-flight；22:45:16 的 `speed_restart_acceptance_v1.json` 獨立驗證 2006 個正文版本／1996 個 complete heads、SHA／rows／metrics 錯誤 0。22:52:34 恢復 owner PID 1868341，service active／running、timer active／enabled；22:55:56 私有進度快照已有 2100 個保存正文項目，重啟後新提交 104 個版本。即時快照後新增資料未自動算入先前凍結的 2006-version 驗收。綜合範圍與部署證據另存 `artifacts/data_quality/news_fulltext_20261008/speed_optimization_v1.json`；沒有把暖重啟當成實際主機斷電／冷開機測試。

剩餘界限：CMoney 仍為 HTTP 403，鉅亨仍會有 502／缺正文和未支援 print 等網址，部分 Global Voices 文章署名不能驗證；其他有明確限制／未註冊的來源仍獨立列出，不靠重建 Session 繞過來源拒絕。URL 去重不等於不同網址／不同媒體的內容級去重。`all_sources_eta_seconds=null` 保留，只在已量到各來源 throughput 時給條件式估時；全歷史完整性、再發布、遠端訓練及自動訓練注入仍為 false。服務續抓與整庫驗收、暖重啟與實際 Windows／WSL 冷開機證明分開。

### 2026-10-08 23:54：新聞爬蟲公開唯讀進度頁

[新聞爬蟲面板](https://penguin72487.ddnsgeek.com/news/)已接上既有下載器的本機心跳，
不是第二個 downloader。沿用 FinMind 的樣式、共用導覽與「少量重要摘要在前、
完整清冊在後」層級；首頁及各面板有入口，每 15 秒唯讀刷新。

- 顯示實存正文篇數與 UTF-8 bytes、已知唯一網址、可回補／處理中／重試／未啟用、
  來源拒絕、owner 請求／回應流量、CPU 與已量測階段。全部來源可搜尋、按狀態分類、
  排序及每 24 個分頁；不把 metadata、股票關聯列或版本數混成全文篇數。
- 個別媒體顯示 client 限流配置、實測間隔、當前 owner 速率與條件完成估時。
  不套用 FinMind API 配額，不捏造未知的官方總上限／重置時間；全部來源完成時間
  仍為未知。快照過期、程序停止、冷卻、磁碟等待或排空等情況不留有效倒數。
- 對齊真實 owner 的 `cycle_complete`／`waiting_next_due`，修正正常續批被誤判停止。
  新聞頁窄版長表格解除繼承的 720px scroll cap，避免完整卡片列在可見 overflow 下
  疊進後方區塊；表格 caption 亦按真實寬度排版。其他 provider 的樣式不改。
- `/news/api/status` 是 bounded allowlist 投影，只讀兩個小型 JSON，不開隊列 writer、
  掃描正文或呼叫 publisher／FinLab／FinMind。API／前端快取與隱藏頁輪詢有界；
  只保留最新新聞 cache 世代，完整來源清冊不在每個心跳累積。
  正文、HTML、文章 URL、金鑰、私人路徑與自由格式錯誤均不公開。

23:54:20（Asia/Taipei）的正式頁驗收快照：841 個來源、3,113,385 個已知唯一網址、
3,656 個已保存正文；這是凍結的 UI 口徑證據，不是全部新聞歷史已完成，之後仍持續抓取。
只有鉅亨、Global Voices、Federal Reserve Board 和 CMoney 的自動取得入口已啟用；
CMoney 仍遭遇 403，其他來源與未知歷史不偽裝成成功或給確定完工日。

驗收：313 項 Python 相關回歸、12 項受影響 JavaScript 測試通過；最終新聞專屬
34 項測試及 Ruff 再次通過。正式 HTTPS 頁以明確 `cpu-2d` Chromium 驗證 1440、
768、390、320 CSS-pixel：摘要和當前快照一致、搜尋空／有值、分類、排序、
全部 36 頁、原生控制、流量／階段、長表垂直界線與 caption 可讀性都通過；
零 JavaScript 錯誤、零瀏覽器 publisher 請求、零整頁水平溢位。實際截圖另經檢視；
不宣稱 GPU／WebGL 或全部媒體正文品質已通過。

只重啟 `stockagent-public-dashboards.service`。部署核對保留其餘 17 個常駐服務
PID／restart count，包含新聞、FinMind、永豐、TEJ、Discord、當沖／隔日沖均未變。
新聞 owner PID 1868341 未重啟，部署前後實存正文 3,441 → 3,479，證明觀測期間有續抓；
工程驗收的短命 systemd run unit 結束不算 production 服務變更。

較早的手機長表重疊截圖與 CSP 字串 predicate 驗收失敗保留，修正後才接受最終結果。
另外 `test/js/data_monitor_feature_refresh.test.cjs` 有一項既有 `0 !== 80` 失敗：測試與
其獨立 VM 載入的 `data_monitor_dashboard/app.js` 均與 HEAD 完全相同，未載入新聞／共用 JS。
已獨立重現並保留，沒有將它算成通過，也未在此新聞頁任務修改該全資料測試。

私人證據：`artifacts/data_quality/news_dashboard_20261008/public-v4/acceptance.json` 與桌機／
手機截圖、`service_deployment.json`、`baseline_js_disposition.json`。工作收據為
`news-dashboard-visual-regression-20261008T155017-7feb7eae`（313＋12）、
`news-dashboard-final-acceptance--20261008T155339-7a8f6b8c`（34＋正式四寬度），
部署為 `news-dashboard-deploy-gateway-v1-20261008T154604-d0950acc`。
可重跑 `scripts/verify_news_dashboard.py`，完整統計契約見
[公開面板架構](public_dashboards_architecture.md#新聞全文進度頁2026-10-08)。
