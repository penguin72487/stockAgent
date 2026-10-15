# Anue 鉅亨新聞抓取：效率、分頁與恢復驗收

日期：2026-10-09（Asia/Taipei）。任務：`news-cnyes-efficiency-20261009`。
沿用 `downloader/news_fulltext*.py`、`data_news_fulltext/queue.sqlite3` 與
`stockagent-news-fulltext.service`；沒有第二個新聞資料庫、瀏覽器登入爬蟲或訓練注入。

同日後續全來源實測與正式設定見 [全來源新聞抓取報告](news_all_providers_efficiency_2026-10-09.md)。
本文的 2 requests/s 是較早階段的量測，保留原證據，不代表目前正式設定。
最新晚間修復與運行證據見[晚間解析修復與持續吞吐量](#晚間解析修復與持續吞吐量)。
2026-10-10 凌晨的同 owner 續抓與其他來源修復見[凌晨續抓驗收](#凌晨續抓驗收)。

## 結論與範圍

- 改由公開 JSON 正文字段批次取得，已知 URL 的網頁正文作回退。
- 最快通過本次短測的客戶端設定為整個鉅亨 provider 共享 **2 requests/s、3 個並行請求**。
  不是每個主機各 2 requests/s，也不是宣稱官方上限；robots／拒絕／Retry-After 可以要求更慢。
- 一般分頁 `per_page` 最多 30、可請求最多第 30 頁。晚間另確認同 ID 重複可使原回應有 31／32 列，
  必須驗證重複列完全相同與來源列範圍，再去重；不是把可請求筆數上限改大。
  來源 `last_page` 可以超過 30，不能直接翻到底。
- 日期窗口依來源實際總頁數自動細分。舊 page-31 失敗保留為 superseded，不刪收據或假裝成功。
- 混合付費頁面只保留可公開連結、分頁資訊及錯誤；免費正文回到 HTML 驗證，後面的日期仍接續。
  後續 304 只確認 metadata 未變，不會清除付費缺口或假裝正文驗證完成。
- 此次完成的是抓取系統修復及有界量測；**沒有證明鉅亨所有年份、分類及付費內容都已抓全**。

## 第一性原理：瓶頸在哪裡

取得一篇合格正文至少需要：發現 URL → 遵守來源節奏 → 取得原回應 → 驗證正文及來源時間 →
保存正文與原回應 → 壓縮寫入 → fsync／SHA 收據 → 提交 queue head。

若每次合格回應平均帶有 `k` 篇、實際來源請求速率為 `q`，請求側容量至多約 `k*q`；
真正吞吐量還受網路、解析、寫入、重複內容與來源拒絕影響。並行只重疊回應等待，不能倍增共享配額。
因此先提高 `k`、移除錯誤造成的全來源停等，再測 `q`；不能只計 HTTP 200 或下載位元組。

原問題及修復：

| 問題 | 機制 | 修復 |
| --- | --- | --- |
| print 頁缺 title／byline | 簡化列印頁不是一般新聞頁，手機版本亦然 | 實測確認 news ID permalink；原 requested URL 與 metadata identity 保留 |
| 單一 502 拖住所有鉅亨 URL | gateway 故障被當成全來源過載 | 單筆重試；連續三次 HTTP／transport 故障才開全來源熔斷；429、503、拒絕及 Retry-After 仍立即遵守 |
| 每篇 HTML 數百 KB、通常只一篇正文 | 導航及前端 payload 大於正文 | 同一公開 API 回應至多取得 30 篇；批次只存一份原 JSON envelope |
| 宣告百頁但 page 31 必然 422 | 來源總頁數不是可存取頁上限 | 以 `ceil(pages/30)` 估計所需日期分區，每個子區重查總數；過密再細分 |
| 64 個 discovery jobs 後長時間切回慢 HTML／等待其他慢來源收尾 | cycle 配額是行政邊界，不是來源限制 | bulk 到界後立即續期；正式服務工作上限 512，仍受 180 秒周期及各來源 bucket 約束，減少不必要全體排空 |
| 一篇付費文章阻塞整個月份 | 原 shared envelope 含不得保存的正文 | mixed-page metadata-only、明示缺口；免費 URL 用 HTML，後續分頁續抓 |
| 限流後重啟又回到同一過高速率 | cooldown 只記等待期限 | 429 降速倍增，持久保存 learned interval；robots 更新及重啟不能清除它 |
| live verify 誤報剛新增 head 沒有證據 | 檔案與資料庫不同 generation | 先 pin SQLite WAL 唯讀快照，再核對 immutable receipts；runtime status 同時標識 owner generation |

### 日期分區的正確性

子窗口覆蓋原 `[startAt, endAt]` 的精確 inclusive bounds，無縫、無缺口。
來源可能把 timestamp 擴成發布日，跨區文章 ID 允許重複但正文 head 去重；不把邊界重複當新增。
優先順序是滾動最新頁、最新已關閉日期窗口、較舊窗口，分頁只在持久化後接續。
舊的失敗 job／原始錯誤碼仍可稽核，但不再佔待執行隊列。

單日仍超過 900 篇時，最多取得合法的前 30 頁並保留 `cnyes_single_day_page_cap` 缺口。
沒有證據表明來源接受時內切分，就不虛構按小時取得完整資料。

## 實測：端到端吞吐量與成功率

私有證據位於 `artifacts/data_quality/news_cnyes_efficiency_20261009/`。
測試使用同一正式 frontier；只暫停新聞 service/timer，其他下載器、TEJ、交易服務沒有被本次操作重啟。
每個階段包含取得、解析驗證、壓縮與 durable acceptance，再獨立核驗本階段新增批次。
不同階段遇到的文章與重複比例不同，不能當等工作量的因果 benchmark。

### HTML 與 API 2s／1s ABBA

來源證據：`live-benchmark.json`。

| 模式 | 請求數 | 完整階段牆鐘 | 新增保存 URL heads | 新 heads/s |
| --- | ---: | ---: | ---: | ---: |
| HTML、每 2 秒一請求 | 48 | 105.440 秒 | 40 | 0.379 |
| API、每 2 秒一請求 | 16 | 30.474 秒 | 219 | 7.187 |
| API、每 1 秒一請求 | 16 | 16.923 秒 | 152 | 8.982 |

API 合計 32 次：32 次回應成功，驗證 931 個 API article IDs，新增 371 個已保存 URL heads。
按全部 API 階段加權約 **7.83 heads/s，為 HTML 樣本的 20.6 倍**。
單看 1s API 階段約 23.7 倍，但不是全歷史或未知官方最大吞吐量。
HTML 控制組包含 timeout 與尚未修復的手機列印頁；失敗保留在原證據。

### API 1s／0.5s ABBA 與部署選擇

來源證據：`adaptive-rate-benchmark.json`。48 次全部 HTTP 200，沒有 429／403 或 transient error。

| 請求間隔 | 請求數 | 階段牆鐘 | 驗證 API IDs | 驗證 IDs/s | 新 URL heads |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1s | 24 | 23.754 秒 | 720 | 30.31 | 266 |
| 0.5s | 24 | 12.217 秒 | 698 | 57.14 | 9 |

0.5s 階段多數是舊已保存頁面的日期重分區，因此 **不能聲稱新增正文速度也翻倍**。
它證明本次 24 個較快請求可通過傳輸、分頁及儲存核驗；實際 start gap 至少約 0.50012s。
全根獨立核驗：34,941 body versions、34,931 complete heads，SHA／收據錯誤為 0。
測試最終全根核驗的 CPU／RSS 包含大量檔案讀取，不是 steady collector 消耗。

部署後的第一個連續觀察：同 owner 125.466 秒新增 2,876 保存 URL heads（22.92/s），
其中 canonical news-ID URL heads 增加 2,803；觀測到的 221 次回應都是 200，
三個 mixed-paid 頁面只走 metadata-only，page-31 錯誤沒有再進入 active jobs。
這是當下隊列／重複比例的結果，不是可保證數年的永久速率。

最新版本的最終連續進度與驗收見 `production-final-acceptance.json`；
正文完整性以該文件綁定的 `live-body-verification-final-512.json` 一致快照為準，不是未來寫入。
正式啟動腳本的 `NEWS_FULLTEXT_MAX_DISCOVERY_REQUESTS` 預設改成 512，是單周期的行政工作量，
**不是來源配額或每秒速率**。每周期最多 180 秒，worker、共享節奏、拒絕退避及 durable receipt 均未放寬。
流量位元組是 Requests 解碼後的 response body 大小，不是 gzip/TLS 網路線路精確流量或官方扣額。

512 工作上限的最終正式驗收（2026-10-09 01:31:44 Taipei）：

- 同 owner 的 DB 快照相隔 113.615 秒，新增 4,217 保存 URL heads（37.12/s），canonical news-ID heads 新增 4,067。
- HTTP status 快照是另一個 121.357 秒區間，242 次開始／回應、242 個 200，實際 1.994 requests/s；不混用兩種時鐘。
- 此時鉅亨保存 78,359 URL heads，其中 canonical news-ID URL heads 76,577；不是獨立報導總量或全歷史完成率。
- 最終一致快照核驗 75,146 body versions（含其他已啟用新聞來源），原始回應／正文／Parquet SHA 及收據錯誤為 0。
- 11 個 mixed-paid jobs 保留獨立缺口，未阻塞其他日期；單日大於 900 篇的 overflow 缺口本次尚未觀測到。
- 來源全歷史完成、官方每秒限額、最早可取得日期及全文訓練授權仍未驗收。

## 來源、robots 與未知邊界

- [鉅亨官方資料服務](https://campaign.cnyes.com/topics/anuedatabase/news.html)提供 API／RSS／XML 等洽詢管道；
  未取得可套用本次公開端點的官方 requests/s、每日資料額度或 SLA。2 requests/s 是有界實測客戶端設定。
- [官方使用條款](https://www.cnyes.com/cnyes_about/cnyes_sos01.html)及
  [鉅亨號社群規範](https://hao.cnyes.com/post/286)不等同原始全文再發布或模型訓練授權。
  本次仍為本機私人研究，`publish=false`、`remote_training_export=false`、`training_injection_enabled=false`。
- [新聞網站 robots.txt](https://news.cnyes.com/robots.txt)由正式 transport 查閱／快取。
  本次 custom user-agent 未讀到數字 Crawl-delay；這不是無限流量許可。
  `api.cnyes.com/robots.txt` 實測 404，亦不把 robots 可爬當正面內容授權。
- [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html#name-502-bad-gateway)區分 gateway error 與明確來源拒絕；
  仍尊重 Retry-After，不繞過 paywall、CAPTCHA、登入、403 或限流。
- [Requests 連線重用](https://requests.readthedocs.io/en/latest/user/advanced/#keep-alive)沿用 thread-local sessions；
  原 service 保留 bounded workers 與跨 cycle 重用，沒有輪換 IP／cookies／身份。
- `headline` 類端點已實測；`category/all` 實測 422，未假造支援或另開失敗管線。
- API `content` 經實測與三篇 HTML 正文的去空白內容相符；原 JSON 與 HTML 都保留。
  格式／換行可能不同，不能宣稱原始 response hash 相同。
- API 缺 byline 時明示 unknown，保留 `source`／`columnists`，不拿「Anue 鉅亨」冒充作者。
  已驗證且仍在 recheck 期限內的 HTML 正文／署名不被無署名 API 降級覆寫。
- `publishAt` 原 epoch 與 UTC 宣告一併保存；不把現在的修訂正文冒稱當年的 point-in-time 版本。
- 回補自 2010（本機已知歷史最早年份）開始；2010／2014 日期探針成功。
  2007 部分日期空回應不足以證明更早所有月份無資料，最早可取得日期仍未驗收。
- 一個既有 link 是股票公司資訊占位頁，不是新聞；保留 `unsupported_article_path`，不捏造正文。
- 早期短／空正文走 HTML 回退；晚間修正為依公開 `content` 欄位及來源證據接收完整短快訊，
  空值、缺欄與 `null` 仍回到 HTML。付費項目及未知歷史不計完成。
  網頁 ETA 只對已發現且可執行 frontier 條件推估，全來源／全歷史 ETA 仍未知。

## 驗證與操作

相關語意／共用 regression：

本次選定相關範圍 340 項通過；啟動腳本另做 `bash -n` 與正式 argv 驗收。未宣稱全專案 test suite 通過。
追加重跑曾出現 `test_finlab_browser_recovers_from_temporary_status_failure` 的
30 秒瀏覽器等待逾時（339 passed、1 failed）。該 FinLab 程式未在本次修改；
單項隔離重跑 9.15 秒通過，之後受監督的整組再跑為 340 passed／47.40 秒。
原失敗不隱藏，也不據此聲稱已根治 FinLab 的瀏覽器時序問題。

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q \
  test/test_cnyes_news_bulk.py test/test_news_fulltext.py \
  test/test_news_fulltext_performance.py test/test_news_dashboard.py \
  test/test_public_dashboards.py test/test_downloader_rate_limits.py \
  test/test_downloader_common_progress.py
```

唯讀進度與核驗，不會呼叫鉅亨 API：

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.download_news_fulltext status
run_fintech_python -m scripts.download_news_fulltext verify --output artifacts/news_verify.json
run_fintech_python -m scripts.audit_cnyes_runtime --output artifacts/news_runtime.json
```

速度實驗必須只有一個 owner；先維護停止正式新聞 service/timer，結束立即恢復。
測試程式也尊重既有 bucket、robots、來源 cooldown；任何拒絕或故障門檻會停止加速。

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.benchmark_cnyes_acquisition \
  --output artifacts/cnyes_bounded_benchmark.json --bulk-requests 12 --faster-api-interval 0.5
```

不要在 production owner 工作時啟動第二個下載 owner；`owner.lock` 會拒絕重疊。
運行進度沿用 [新聞面板](https://penguin72487.ddnsgeek.com/news/)，
四種 viewport 的唯讀／原生操作／禁止來源請求驗收在 `browser-final/acceptance.json`。

## 晚間解析修復與持續吞吐量

2026-10-09 23:06 Taipei 的正式同 owner 觀察：234.401 秒新增 18,798 個已保存正文 URL，
約 80.196 URL/s。修復前同 owner 的 210.908 秒觀察新增 386 個，約 1.830 URL/s。
這兩個實際回補階段相差約 43.8 倍；新階段恢復了先前無法取得的批次正文，
**不是固定同一批 URL 的純 CPU 加速倍數，也不是官方來源限速或全歷史完成率**。

根因與修復沿用同一 collector、provider bucket、queue、Parquet 與收據。

最後的 source contract 8 另加上完整 JSON 型別檢查：相同 ID 的重複物件必須連型別都相同，
不能利用 Python 的 `False == 0` 或浮點數／整數相等來放過發布時間、付費標記或其他欄位衝突。
這是防止去重放寬來源驗證的回歸保護，不表示來源已觀測到這三種衝突。

| 根因 | 真實來源證據 | 修復與驗收界線 |
| --- | --- | --- |
| 來源 join 重複使整頁被拒絕 | 台股頁 32 列只有 29 個 ID，同一 ID 有四個完全相同物件；另一頁 31 列有 30 個 ID | 核對 `from/to`、完整物件與有界筆數後去重；相同 ID 內容衝突、缺列或不明超額仍拒絕 |
| 100 字門檻丟失完整快訊 | `bc_live` 的八篇公開 `content` 只有 59–89 字 | 接受來源實際完整欄位；不以標題、摘要或空值補正文；API 無署名仍標示 unknown |
| 正常 metadata-only 頁造成整個分類熔斷 | `mag` 沒有 `content`；公告及 `tw_bull` 另回傳編碼後的 `<p>null</p>` | 保留實際標題與發布時間、接續分頁與 HTML；只證明 metadata，不計全文完成 |
| HTTP 200 軟 404 被誤判為暫時缺正文 | 兩個文章頁只有 `404 Not Found` 與零寬字元，沒有文章容器或 schema | 明列 `source_not_found`；不把含 404 字樣的真實文章排除 |
| 一次瞬斷反覆降速 | 20–99 次結果內的一個錯誤超過 1% 門檻 | 保持同一壓力補足樣本，不提前加速；第二次錯誤、600 秒期限、連續錯誤熔斷與明確拒絕仍生效 |
| 舊失敗期限拖住已修好的分類 | 舊 source contract 的特定 parser circuit | contract 改變後重排該 circuit 的原失敗工作；保留原證據，不重排 HTTP 拒絕 |

另確認一個 CMNews 失敗樣本為來源圖表搭配很短的導流文字，改標示 `source_body_image_only`。
沒有把導流文字當完整文章、下載會員內容或偷偷加入 OCR。17 個已啟用來源已重新清點；
CMoney／PeoPo 的存取拒絕、付費、第三方權利、署名與原文身分問題仍保留，沒有被計為完成。

### 不重複呼叫的本機回收

`downloader/news_fulltext_replay.py` 由既有唯一 owner 分段處理已保留的公開 API 回應。
每次至多 64 頁、解析階段約 0.75 秒、原回應上限 8 MiB，每 10 秒給一次工作片段；
檔名清冊每小時更新，第一次清冊建立另計入整條鏈的時間。

只有 SHA／收據／來源授權／原觀察時間通過才可回收，已有正文、活躍 lease、來源拒絕與未到的重試期限不覆寫。
`source_observed_at_utc` 保留原抓取時刻，`reparsed_at_utc` 另記重新解析時刻，沒有把本機重算偽裝成新 HTTP 觀察。
入庫失敗不提交回收 checkpoint，沿用 canonical receipt recovery；重啟後已提交的正文不再寫一次。
本機回收使用獨立 workload／計數，從線上 capacity 的正文分子扣除，不會用零 HTTP 的回收量宣稱網站能跑更快。

23:04 的 snapshot 有 40,787 個已知 retained response files、檢查 712 個、待檢查 40,075 個，
沒有檢查出的收據／授權驗證錯誤。這是**檔案重新解析進度，不是待抓文章數或全歷史分母**。
先前同日已有 1,457 個正文 URL 經本機回收，不消耗新的來源請求；後續回收仍按原始 response 與現有 heads 去重。

### 最新實測與證據

- 23:28 Taipei 的最新 source contract 8 同 owner 驗收：DB head 區間 **124.493 秒新增 11,651 個正文 URL，約 93.587 URL/s**。
  HTTP counter 區間另為 120.336 秒，985 次開始、約 8.185 requests/s；九項正式運行判準全部通過。
  該區間本機回收新增 head 為零，未觀測新 transport error、429 或 parser circuit。
- 23:06 的前一個正式區間為 DB head 234.401 秒；HTTP counter 區間另為 242.584 秒，1,814 次開始、約 7.478 requests/s。
  不拿不同時鐘相除，也不拿客戶端目標頻率當實際吞吐量。
- 最新 owner 已驗證最近 **128 批共 2,594 個 body versions**，沒有正文／原回應／Parquet SHA 或收據錯誤；
  前一輪亦驗證 128 批共 3,257 個版本。兩輪各為滾動樣本，不能相加當成不重複覆蓋量。
  這是近期有界驗證，不宣稱所有二百多萬筆都在這次重驗。
- 23:06 的 Cnyes 清冊為 2,346,756 個已知 URL、1,979,394 個有保存正文、367,330 個待處理 missing-body work。
  清冊與 heartbeat 有獨立 snapshot 時刻，數值仍持續變動；不可直接用約 80/s 的批次階段速率外推所有 HTML 尾端。
- 最終 25 個相關測試檔共 **1,021 項通過，32.23 秒**，包含最後增加的三種 JSON 型別衝突回歸測試。
  未宣稱全專案 test suite、官方速度上限、全來源全文或全歷史完成。
- 現有[新聞面板](https://penguin72487.ddnsgeek.com/news/)沿用正式進度；本機唯讀 API 已確認 `health=running`、`snapshot_consistent=true`，
  網頁有收到 Cnyes 更新後的 head 數。此回合沒有新的桌機／手機 browser 或 WAN 連線驗收。

直接證據：

- [修復前區間](../artifacts/data_quality/news_goal_stage01_20261009/cnyes_v17/runtime_before_window.json)
- [最新 contract 8 正式區間與九項驗收](../artifacts/data_quality/news_goal_stage01_20261009/cnyes_v17/runtime_typed_accepted.json)
- [最新 128 批正文核驗](../artifacts/data_quality/news_goal_stage01_20261009/cnyes_v17/recent_bodies_typed.json)
- [前一正式區間與九項驗收](../artifacts/data_quality/news_goal_stage01_20261009/cnyes_v17/runtime_final_window.json)
- [前一輪近期正文核驗](../artifacts/data_quality/news_goal_stage01_20261009/cnyes_v17/recent_bodies_final.json)
- [最新解析器八個真實來源樣本驗證](../artifacts/data_quality/news_goal_stage01_20261009/cnyes_v17/source_reparse_verified_v4.json)
- [兩個 null 來源回應及回收清冊](../artifacts/data_quality/news_goal_stage01_20261009/cnyes_v17/cached_null_source_proof.json)
- [全 provider 目前清冊](../artifacts/data_quality/news_goal_stage01_20261009/cnyes_v17/providers_final.json)
- [最終相關測試 log](../artifacts/operations/agent-workflow/runs/cnyes-v18-final-regression-20261009T152447-1dcdb69b/run.log)

有界、本機、唯讀的回收核驗指令：

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.audit_news_cached_replay \
  --source-parquet quarantine/ff002a388d58a8b3cbc99cb67882af7be938ab848a134e62a44e90832caeb283.parquet \
  --source-parquet quarantine/751715bd3703ce6a3d0989ad3a7e33283c34f53a50c0b3de9641b35d38e382be.parquet \
  --output artifacts/news_cached_replay_audit.json
```

## 凌晨續抓驗收

2026-10-10 00:22 Taipei 的[正式運行驗收](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v18/runtime_v10_accepted.json)
九項條件通過。相同 PID／owner 的 WAL head 窗口 **157.910 秒新增 13,683 個正文 URL，約 86.651 URL/s**；
另一個 HTTP counter 窗口為 151.620 秒、1,148 次開始，約 7.572 requests/s，沒有新增 429、transport error 或 parser circuit。
HTTP 計數期間有 1,369 個 head 來自已保留回應的本機回收；這與 WAL 窗口的起訖時鐘不同，
不拿總 head 速率宣稱源站純線上容量，也不把不同回補工作量當固定 cohort 的加速倍數。

00:21:55 Taipei 的[完整來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v18/providers_final.json)
中，鉅亨有 **2,671,259 個已知 URL、2,384,638 個已保存正文 head、286,589 個缺正文待處理工作**。
已保存但待重新確認的紀錄與缺正文分開；舊 502／transport／缺正文的錯誤證據仍保留在各筆工作上。
這些不是獨立報導總數、媒體全歷史分母，亦不代表所有 HTML 尾端都能維持批次階段的速度。

[最近 128 批核驗](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v18/recent_batches_v10.json)
涵蓋 1,853 個正文版本，原始回應／正文／Parquet SHA 與收據錯誤為零；不是重驗全部二百多萬個 head。
26 個新聞與限流相關測試檔共 **1,006 項通過，31.49 秒**，不宣稱全專案 test suite 通過。

巡檢同時修復 Fed 舊版公告、會議紀錄、預測表及逐字稿接線；
已知 HTML 解析失敗已收斂為零，四個影像 PDF 與三個影片入口頁仍分開列出。
詳見[全來源修復的最新快照](news_all_source_repair_2026-10-09.md#凌晨正式續抓與-fed-文件修復)。
[新聞面板](https://penguin72487.ddnsgeek.com/news/)的本機唯讀 API 已確認 741 個來源鍵、
`health=running`、`snapshot_consistent=true`；本回合未重新宣稱 WAN／桌機／手機版面驗收。

00:39:52 的[後續正式窗口](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v18/runtime_v10_final.json)
包含六個分類的來源 HTTP 500 與其自動退避恢復，九項運行檢查仍通過。
**1,048.199 秒新增保存 113,979 個正文 URL，約 108.738 URL/s**，包含本機回收；
另一個 1,056.374 秒 HTTP counter 窗口約 6.433 requests/s。
41 個失敗 URL 的診斷 SHA 及後續同 URL 的來源驗證時鐘／retained response SHA
已[逐筆核對恢復](../artifacts/operations/agent-workflow/runs/cnyes-v10-exact-error-url-recove-20261009T164325-9438b11a/run.log)，
恢復判定以同一 URL 的成功證據為依據；來源內部 500 的成因仍未獲得官方證明。
[後續 128 批](../artifacts/data_quality/news_goal_stage01_20261009/remaining_v18/recent_batches_v10_final.json)
共 2,674 個正文版本通過收據核驗。
該快照共有 2,765,649 個已知 URL、2,503,000 個已保存正文 head、262,617 個缺正文待辦；
metadata／付費／unsupported path 及未發現的歷史仍不能算已抓全。
