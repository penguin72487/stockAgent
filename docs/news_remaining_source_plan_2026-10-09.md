# 其他新聞來源的一次性下載與後續追新規劃

2026-10-09 21:43 台北時間資料快照：**NOWnews 已存 527／658、民視 869／2,630 個
已知網址正文**。民視這輪已沒有待派的固定網址，但仍有 1,536 個署名／來源未核實結果，
不能把佇列派完當成全部抓完。NOWnews 的 186 筆署名誤判與 50 筆發布／更新時鐘誤判已全部回補；
民視另修復舊電視稿末尾署名、同段換行署名、子站錯誤時區及三種本社 newsroom credit。
21:27 同 owner 的 521.44 秒窗口新增民視 20、鉅亨 162、Global Voices 52 個正文 head，
九項工程檢查及最近 128 個 batch／2,444 個版本收據通過；先前零新增的失敗窗口照常保留。
974 項相關回歸通過，不把來源 HTTP 200、程式存活或重排計數當成取得正文。
使用者已確認中央社授權，已準備官方 API 能力檢查、金鑰欄位與接線清單；
目前本機 `CNA_API_KEY` 尚未設定，沒有假裝已連線或取得中央社全文。
民視原固定集合 2,629 個網址不變，另有 1 個原站證據已核對的追加網址；網頁分開顯示，
不改舊進度分母。這些都是本機已知集合，**不是全媒體歷史完整率**。
全清冊 741 個來源鍵仍保留；真實 403、轉載作品、署名未確認及未核對來源尚未全部解決，
**全新聞目標仍 active**。詳見 [NOWnews 與民視原文存取及回補修復](#nownews-ftv-root-repair)。
中央社原始稿件及接線狀態詳見 [中央社授權 API 接入](#cna-authorized-api-setup)。

### 前輪：匯流與信傳媒驗收

2026-10-09 18:23 台北時間資料快照：**匯流 65／70、信傳媒 354／355 的本機固定 URL
集合已有正式正文**。修復導覽登入誤判、本文選取與署名污染，並補回舊格式與編輯帳號
造成的 31 篇匯流誤排除文章；不是將失敗狀態改成完成。
兩個集合的 419 個已存 head／不可變收據通過核對，12 個修復相關原文以現行程式重解析，
正文、作者、標題及發布時鐘與正式入庫值一致；768 項相關回歸與四種螢幕寬度面板驗收通過。
18:43 最終同 owner 執行期九項檢查通過，健康來源持續獨立抓取，真實拒絕仍保留。
另保留真實 403、第三方作品、未核對署名、原站 404 和轉載 namespace，**全新聞目標仍 active**。
詳見 [匯流與信傳媒原文及署名修復](#匯流與信傳媒原文及署名修復)。

### 前輪：官方來源驗收

2026-10-09 17:19 台北時間驗收：**台南市政府 1／1、金門日報 4／4 的本機已知
原網址均已正式入庫，正文、作者、原始發布字串與耐久收據通過核對**。
台南網址的 HTML 轉義參數造成 410；金門日報的正文容器未選中，且 `og:url` 指向舊文章。
已修復精確網址表示法與本篇文章辨識，不用頁尾、摘要或隱藏文字補正文。
819 項相關回歸與四種螢幕寬度的進度頁驗收通過；固定批次不再反覆抓取。
全清冊仍有 738 個來源鍵，真實來源拒絕、權利範圍與未核對來源尚未全部解決，目標維持 active。
詳見 [台南市政府與金門日報存取修復](#台南市政府與金門日報存取修復)。

### 前輪驗收

2026-10-09 16:29 台北時間驗收：**CMNews 已存 13,555 個正文 URL head，
原來解析失敗的公開公告已由正式服務自動補回**。它的資料表被圖片清理規則刪掉，
不是來源拒絕；修復保留本篇表格原值、作者、發布時鐘及原始回應。
同一批 11 個缺正文原網址中，另外 10 個實際顯示 VIP 登入遮罩，已正確分成需登入，
未把預覽或隱藏內容當全文。780 項相關回歸通過，近期 128 批次、7,805 個正文版本驗證無錯誤。
仍有真實 403、圖文／圖片正文、未核對來源及未完成歷史；全新聞目標維持 active。
詳見 [本篇資料表與會員遮罩修復](#本篇資料表與會員遮罩修復)。

### 更早驗收

2026-10-09 15:59 台北時間最新驗收：**CMNews 已存 4,491 個正文 URL head，
正式服務持續下載；HTTP 舊網址反覆冷卻與無日期文章被餓死的兩個根因已修復**。
原本失敗 5 次的 HTTP 舊網址已取得全文，原三網址驗收集合也全數入庫，不更換集合。
同 owner 的 130 秒窗口新增 CMNews 684、鉅亨 62、Global Voices 13 個 head；
近期 128 批次、5,375 個正文版本驗證無錯誤，770 項相關回歸與追加的 19 項收據測試通過。
CMoney 的實際 HTTP 403、PeoPo 的單篇拒絕，以及其他未核對來源尚未全數解決；
全新聞目標仍為 active，不把冷卻、設定值或規劃完成當成修復／下載完成。
詳見 [轉址與無日期派發修復](#news-origin-fairness)；[15:27 前輪驗收](#news-root-repair) 保留。

本輪先完成全來源規劃，再依來源實測實作。小來源先抓本機已知文章，不為每個站點新增
常駐服務或反覆掃首頁；新文章成長另列，確認值得追新後才擴充既有排程。
範圍是已接受 FinLab／FinMind metadata 中的全部來源鍵，不等於所有媒體的全站歷史。

## 清冊與順序

2026-10-09 10:37:18 台北時間快照有 738 個來源鍵、3,872,268 個 URL、966,417 個已存正文。
尚未啟用且各不超過 300 URL 的 678 個鍵共有 9,723 個 URL；部分是轉址、聚合器、
社群、閉站、重複子網域或非文章，不能把它們全當成 678 家媒體。

完整逐來源規劃由 [plan_news_sources.py](../scripts/plan_news_sources.py) 讀取一致快照產生，
保存來源類型、篇數、正文缺口、metadata 首末日、前置核對、優先級與未知 ETA：
[完整清單](../artifacts/data_quality/news_remaining_sources_20261009/plan.csv)。
300 篇只是選擇有限批次的工程起點，不是來源配額，也不截斷可下載歷史。

| 階段 | 分工 | 結束條件 |
| --- | --- | --- |
| 現行下載 | 鉅亨、Fed、Global Voices 等繼續用原 owner；來源拒絕保留 | 各自正文收據與就緒工作進展，不拿 service active 代替成功 |
| 少量來源 | 核對來源規則後，凍結既有 URL 集合，用同一佇列下載 | 固定集合每個 URL 有已驗證正文或明列原因；失敗不是完成 |
| 較大媒體 | 財經／產業來源優先，核對 API／feed／索引，批次比逐篇優先 | 端點與分頁、正文、時鐘、去重、限流實測通過，再加入共用排程 |
| 轉址／聚合 | 先找到可信原始媒體與文章身分，合併相同 URL | 不把 Google News、MSN、LINE 或社群摘要當原文全文 |
| 閉站／其他 | 查合法可用的來源、文字範圍、失效狀況 | 保留 unresolved／source-not-found，不造正文或反覆撞拒絕 |

排序先官方經濟／政策原始來源，再財經與產業、其他新聞；同層的小集合先做。
這是研究價值假設，不是文章價值已量化的結論。所有來源仍保留在清單，不刪掉低順位。

## 有限批次設計

來源設定選擇 `acquisition_mode: finite`，首次由唯一 queue owner 凍結本機已知 URL，
持久保存集合／SHA／時間。重啟不重建集合、不重抓已存正文、不清來源 cooldown。
不種首頁／sitemap／feed，不對已完成文章排周期重查；來源之間仍使用原本獨立限流。
HTTP、本文辨識、壓縮、不可變收據、SHA 和佇列提交全部重用現有下載器。

後來的 metadata URL 列為 `waiting_cohort`，可在清冊看到成長，不自動膨脹已交付批次。
待決定追新時改為既有 continuous 模式，釋放新增工作，不新建第二套下載器。
固定批次正文完整、所有 URL 有結果、排程閒置與媒體全歷史完整分開。

## 第一批核對

最初核對公視 8 篇、匯流 70 篇、PeoPo 160 篇、台南市政府 1 篇與金門日報 4 篇。
台南與金門的已知五篇現已入庫；其他來源依各輪結果分別處理，不把最初候選清單當成已下載。
PeoPo 的 CC 由作者選擇，不能把整站都當成 CC；政府網域也不是所有內容都公有領域。
來源未表明數值速率上限時先採有界 client pacing，不能把自己的頻率稱官方上限。
私人下載用途、轉載、訓練與分享權利分開，不把「研究」當全面授權。

## 本輪實作與實際下載

11:11:25 台北時間的後續快照仍完整列出 738 個來源鍵：
6 個沿用現行追新／回補、2 個有限批次、671 個待核對後有限下載、44 個較大來源待核對、
10 個聚合／轉址、4 個社群／影片平台及 1 個閉站。
[最新逐來源規劃 CSV](../artifacts/data_quality/news_remaining_sources_20261009/after_plan.csv)
和 [計數／輸入身分](../artifacts/data_quality/news_remaining_sources_20261009/after_plan.json)
包含每個來源的順序、已存／缺少篇數、metadata 首末日、請求前置條件及未知 ETA。
目前 4,010,907 個已知 URL、1,102,621 個已存正文；這是全佇列的移動快照，
不是本輪新下載量或效能改善倍數，也不代表全站歷史。metadata 日期不是發表時鐘。

| 來源／固定範圍 | 真實結果 | 發表日期／正文驗證 | 後續動作 |
| --- | --- | --- | --- |
| 鏈新聞 262 URL | 262 / 262，100% 通過固定批次驗收 | 262 篇皆有來源發表時鐘與作者；raw/body/file SHA 全通過 | 已停止重抓；之後新增 URL 分開列為待評估追新 |
| PeoPo 160 URL | 1 / 160；1 篇未通過本篇授權核對，157 pending、1 retry | 已存 1 篇有正文、作者、原始發表時間及本篇 CC 標示；沒有把相關文章的 CC 或日期套上來 | 11:19:18 自動再試仍為 HTTP 403，保留來源拒絕；本快照下一次可嘗試為 11:34:18 |

PeoPo 的 1 篇正文與 1 篇未驗證授權不是同一筆。未驗證授權不保留完整原始回應／正文，
HTTP 403 也不是可把進度算成完成的理由。下次時間只表示來源冷卻結束，**不是保證抓完時間**；
若來源持續拒絕，不切換 UA／代理或清除 cooldown，保持未知 ETA。
驗收依據為 [固定集合與已存正文收據](../artifacts/data_quality/news_remaining_sources_20261009/cohort_delivery_status.json)，
其中集合正確、已存正文正確、所有集合正文完整三個判準各自獨立。

鏈新聞正文自身聲明的發表時間範圍為 2025-07-28T01:30:26Z 至 2026-10-08T09:11:36Z，
不是該媒體全部歷史的最早／最新文章。
本批第一次至最後一次成功入庫相隔 211.23 秒，實際 262 次文章請求得到 262 篇正文；
原始回應合計 37,300,637 bytes，純正文 UTF-8 合計 872,485 bytes。
這是批次實測，不是官方配額、穩態最大吞吐量或 gzip／Parquet 壓縮率。
PeoPo 的原始日期沒有明確時區，報告不擅自補時區或製造 UTC 日期邊界。

## 小來源核對與仍未啟用原因

| 來源 | 來源依據／觀察 | 本輪決定 |
| --- | --- | --- |
| 鏈新聞 | [來源首頁](https://abmedia.io/) 的版權聲明；未找到數值爬蟲配額或全面重用授權 | 僅本機已知公開文章的私人固定批次；不自動注入訓練、公開正文或付費內容 |
| PeoPo | [官方規範](https://www.peopo.org/about/regulation/P1-1.htm) 的作者個別授權；實測 [CC 文章](https://www.peopo.org/news/849412) 與 [權利保留文章](https://www.peopo.org/news/844854) | 每篇核對自身授權，未通過就明列；來源 403 冷卻獨立 |
| 公視新聞 8 URL | [使用規範](https://news.pts.org.tw/service/2025-07-29) 第四點要求取得同意；隱私政策不是正文下載授權 | 維持停用待授權，不能把 PeoPo 作者授權擴張至公視新聞 |
| 匯流 70 URL | [版權聲明](https://cnews.com.tw/%E7%89%88%E6%AC%8A%E8%81%B2%E6%98%8E/) 搜尋結果可見限制，直接讀取遭拒；第三方稿件另有權利 | 暫未啟用，仍需完成來源條款核對／授權；不是已下載或無限重試 |
| 台南市政府 1 URL | metadata query 含 HTML entity；原文章身分需核對，版權政策端點讀取逾時 | 未自動改寫所有 URL，也未以其他機關條款推定本篇授權；保留待核對 |
| 金門日報 4 URL | [官方 RSS 說明](https://www.kmdn.gov.tw/1119/1120/1409/?cprint=pt) 支援訂閱，但不等於全文重用權；文章讀取逾時 | 未啟用，保留網路／條款核對事項；政府網域不是全面公有領域 |

其餘 671 個小來源與 44 個大來源已逐一列入規劃，但**未全部逐站核對或抓完**。
已登錄的通用 JSON-LD／CSS 配方是候選實作，不拿「有程式」當實際可抓、正文正確或授權通過。
原始診斷／正文只在私人目錄，不輸出到公開清單或瀏覽器。

## 排程、效能與網頁

- 只延伸原 `stockagent-news-fulltext.service` owner，不為每個小站建立服務；
  原始 HTTP、robots、本文辨識、壓縮、原子收據與 SHA 驗證共用。
- `news_fulltext_cohorts.py` 保存不可變 URL 集合；restart 核對 SHA、成員與收據，
  保留 retry／拒絕時限。核對結果每個 owner 快取一次，不在每個循環反覆全掃集合。
- 有限批次不新增索引、feed 或 sitemap 工作，既有 discovery 也不派發；
  已取得正文不排週期重查。新 metadata 身分用既有 URL 去重，成長列為 `waiting_cohort`。
- 不要求各來源的並行上限總和小於共享池；實際派發仍由 16 個共享 worker 有界管理，
  公平輪替，各來源 cooldown 相互獨立。不能用提高設定值當實測官方上限。
- [新聞進度頁](https://penguin72487.ddnsgeek.com/news/) 已沿用現有頁面，
  有限來源顯示固定分母、已存正文、批次完成與新增未排程；授權未驗證／拒絕不算完成。
  網頁讀取不呼叫媒體，不傳正文、私人收據位置或金鑰到瀏覽器。

在同 owner 的 51.07 秒資料庫觀測窗口中，鏈新聞新增 HTTP 請求 **0**、新增正文 **0**；
Global Voices 與鉅亨仍各新增 5 與 5,860 個已存 URL 正文，PeoPo 只在冷卻結束後再試一次。
HTTP 計數自身快照窗口為 45.22 秒，與資料庫窗口不同，不能混作每請求／每秒速率。
見 [完成後閒置／獨立進展證據](../artifacts/data_quality/news_remaining_sources_20261009/runtime_idle_progress.json)。
跨窗口可能有正文批次落盤，不能拿瞬間入庫突增聲稱 HTML 一請求得到多篇正文。
本輪證明避免不必要重抓與共用服務仍有實際進展，**沒有宣稱量測到全系統 CPU 降幅或速率極限**。

驗證包含重啟續抓、不清 cooldown、成長不變更分母、壞收據拒絕、權利／作者／日期的來源範圍，
以及共用限流、鉅亨批次、正文收據、公開頁面的回歸；
最後局部時鐘／頁面檢查 74 項通過，較廣回歸最終 555 項通過
（[測試收據](../artifacts/operations/agent-workflow/runs/news-finite-complete-regression-20261009T032009-095cdc2f/run.json)）。
頁面在本機正式 gateway 的 1440／768／390／320 px，原生篩選、搜尋、排序與分頁皆通過，
見 [頁面驗收](../artifacts/data_quality/news_remaining_sources_20261009/dashboard_final/acceptance.json)。
外網 HTTPS 本輪未通過連線驗證，不以本機頁面驗收宣稱外網可用。

## 重跑清點與核對

以下只讀既有清冊／佇列，不呼叫新聞來源，不改排程：

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.plan_news_sources \
  --inventory artifacts/data_quality/news_remaining_sources_20261009/after_inventory.json \
  --output artifacts/data_quality/news_remaining_sources_20261009/review_plan.json
run_fintech_python -m scripts.audit_news_finite_cohorts \
  --output artifacts/data_quality/news_remaining_sources_20261009/cohort_recheck.json
```

驗收 script 的 exit 0 表示集合與**已存**正文完整性核對通過，不表示所有缺少正文皆已補齊；
務必查看每個來源的 `accepted` 及 `all_cohort_bodies_stored`。
尚有正文缺口或來源拒絕時，原全新聞任務保持 active，不能以本次交付宣稱全來源完成。

## 全來源接續實作規劃

11:28:03 台北時間的一致快照有 4,106,481 個已知 URL、1,194,630 個已存正文，
仍為 738 個來源／主機鍵。接續實作前，已為每個鍵產生下載方式、已知端點、
正文／時鐘驗證、觀察到的錯誤及修復策略、CPU／HTTP 策略與後續增量操作：
[全來源實作清單](../artifacts/data_quality/news_site_efficiency_20261009/implementation_plan.csv)。
[完整規劃與來源身分](../artifacts/data_quality/news_site_efficiency_20261009/implementation_plan.json)
採報告 contract v2；清單涵蓋全部既有來源，不表示已逐站實測全部網站。

| 下一步 | 設計與驗證 |
| --- | --- |
| 生技與科技來源 | 先以原佇列已知 URL 有界驗證環球生技、基因線上、INSIDE、PanSci；核對 robots、轉址、正文範圍、作者與原始日期，再啟用共用有限批次 |
| 共用 CPU 優化 | 同一批原始收據離線重播；定位解析與高頻統計成本，保留逐篇 raw/body/file SHA、正文文字及失敗結果 |
| 排程與統計 | 完成批次不再派發；統計使用交易式增量計數，與一致快照的完整 COUNT 對照，避免每次更新狀態快照重新走訪百萬篇正文；網頁仍只讀快照 |
| 較大財經媒體 | 優先原始媒體與已驗證的批次正文端點，未知端點不得視為已驗證；各來源獨立節流與恢復，共用有界 worker |
| 授權／拒絕 | 科技新報、今周刊、公視的來源規則列為待授權；來源拒絕、付費與權利未核對保留，不改成成功、不清冷卻或改身份繞過 |

本階段來源核對紀錄見
[來源審查清單](../configs/news/source_reviews_20261009.json)。
其中 14 個新增審查紀錄有來源依據，尚未核對的鍵仍明列，私人研究用途不是全面授權。
環球生技的新舊主機需要確認原文身分；PanSci 的個別作者權利不能套用成全站 CC。
待測來源沒有已驗證的官方數值速率上限，初次只做少量有界測試，不宣稱理論上限。

## 接續實作與驗收：2026-10-09 12:34 台北時間

本輪先產生全來源規劃，再做有界來源驗證與共用實作；沒有建立平行的第二個新聞管線。
全部 738 個鍵的最新方案在
[階段完成後全來源清單](../artifacts/data_quality/news_site_efficiency_20261009/completed_stage_plan.csv)；
[完整方案](../artifacts/data_quality/news_site_efficiency_20261009/completed_stage_plan.json) 為報告 contract v3。
v3 修正價值排序：已知貨幣政策來源優先，再排財經、產業與科技來源，
不能只因 `.gov` 網域就推定有金融價值或開放授權。這是研究主題啟發式排序，不是預測 alpha。

[一致快照清冊](../artifacts/data_quality/news_site_efficiency_20261009/inventory_accepted_final.json)
有 57 個已設定 provider，其中 11 個允許目前私人下載範圍；另外 681 個未註冊來源鍵。
已知 URL 4,340,115 個、已存正文 URL head 1,423,033 個，待來源審查 2,136,102 個。
全清冊每列都有來源角色、價值依據、已知端點、正文與時間／作者驗證、錯誤與修復方法、
CPU／HTTP 策略、歷史回補與增量分工。未知 API 不猜路徑，未知完成時間維持空值。
清冊的最早／最新 metadata 日期不是媒體原始發布時間；也不表示全站歷史已發現。

### 已部署的共用修正

| 問題 | 根因與修正 | 保留的邊界 |
| --- | --- | --- |
| 基因線上抓不到正文、標題／日期錯位 | 使用本篇 `.post-content-details`、本篇 h2、作者與原始日期容器，避開分類 h1 與側欄 | 不補造日期內的發布時分，不把標題當正文 |
| 真實短公告被擋掉 | 一篇公開庫藏股公告正文只有 89 字，被通用 100 字門檻誤拒；對已審查且唯一的本篇正文容器允許短文字，仍驗證標題／作者 | 只對明確設定來源生效；其他來源門檻不變，沒有正文或只有圖片不算成功 |
| INSIDE 舊專題轉址被拒 | 註冊同來源 `/article/ID-*` 與 `/feature/主題/ID-*`；驗證實際文章、JSON-LD 正文及作者，針對舊解析失敗重排 | 404、403、429、付費、robots 及來源冷卻不清除 |
| PanSci 混入多篇正文與日期 | 無限捲動頁的第一篇同時核對 post ID 與 URL，只讀第一篇的正文、作者、日期 | 不借用第二篇的日期或內容；來源日期沒有時分就保留原格式 |
| 不同網址的 soft-404 收據被覆蓋 | 診斷檔名納入 provider、請求 URL、最後 URL 及回應 SHA；同一錯誤頁也保留各自請求證據 | 舊診斷與原失敗紀錄保留；新收據重新驗證，沒有把錯誤頁入庫成正文 |
| 狀態更新反覆 COUNT 百萬篇正文 | 交易式增量 stored-head 計數，同時涵蓋新 head、SHA 更新、provider 移動、刪除與回滾；只做一次 migration | 存過正文但重查失敗仍算已存；版本數與 URL head 數不混用 |
| 每次設定變更重掃所有來源 | 比對每個來源的捕捉 fingerprint、允許狀態與主機映射，跳過未受影響的來源重分類 | 授權到期／改變、來源映射改變與舊分頁 migration 仍執行 |
| CSS 重複全樹掃描 | 簡單選擇器先縮小候選，再用 SoupSieve 精確驗證；複雜選擇器走原本路徑，付費標記一次查找 | 保留大小寫、多值 rel、順序、limit、標題、正文、作者、時鐘與拒絕語意 |
| 網頁突然無法讀心跳 | 約 8 千個歷史查詢範圍及其完整子範圍陣列重複進入心跳，突破 4 MiB 讀取上限；改按 provider／真正端點彙總，投影 contract v2 | 所有查詢範圍、分割樹、原始回應及收據仍由 SQLite／檔案保存，沒有縮減下載範圍或放寬讀取上限 |
| 重啟時產生九萬多行日誌 | 常駐服務結束／交接只輸出一行摘要；完整狀態仍寫在 `status.json` | 明確執行的一次性 CLI 仍輸出詳細結果；沒有刪除歷史日誌或收據 |

主要實作在 [共用來源政策](../downloader/news_fulltext_policy.py)、
[佇列與耐久接受](../downloader/news_fulltext_queue.py)、
[來源 HTTP 到正文管線](../downloader/news_fulltext.py)、
[CLI](../scripts/download_news_fulltext.py) 與
[來源登錄](../configs/news/publisher_catalog.json)。
新增來源共用既有 `stockagent-news-fulltext.service`，仍是單一 queue writer／HTTP owner；
不同 provider 各自節流與恢復，完成有限批次不再反覆掃首頁或重抓。

### 固定已知批次的實際結果

[12:34:26 一致快照與逐 head 驗證](../artifacts/data_quality/news_site_efficiency_20261009/cohort_completed_stage.json)：

| 來源 | 已存正文 URL head／固定已知 URL | 可派發剩餘 | 尚未入庫的來源結果 |
| --- | ---: | ---: | --- |
| 基因線上 | 1,351／1,356 | 0 | 4 個來源不存在；1 個只有圖片，尚未做媒體／OCR |
| INSIDE | 43／46 | 0 | 3 個來源不存在；舊專題漏抓已修復 |
| PanSci | 1／1 | 0 | 這一個已知網址正文已存，不表示全站已抓完 |
| 鏈新聞 | 262／262 | 0 | 固定已知批次已完成，不表示全站已抓完 |
| PeoPo | 1／160 | 158 | 1 個逐篇授權未驗證；來源 403 仍遵守冷卻，不能當成解析器已修復 |

URL head 不是唯一新聞事件：INSIDE 原始文章網址與專題轉址可能指同一篇，
此處保留各個 metadata 關聯與原 URL，不能說 43 個 head 就是 43 篇不同報導。
影像正文也不能僅靠正規化／壓縮產生文字，需要另確認媒體下載與 OCR 範圍。

短公告的來源實測及離線重播見
[原始診斷](../artifacts/data_quality/news_site_efficiency_20261009/last_genet_gap_probe.json) 與
[89 字正文／作者／原始日期驗證](../artifacts/data_quality/news_site_efficiency_20261009/last_gap_parser_acceptance.json)。
來源原始標示為 `日期 : 2025/10/13`，沒有把 URL、metadata 或推測年份當成發布日期。
所有正文仍是本次捕捉快照，不宣稱歷史 point-in-time 版本。

### CPU 與整條保存鏈路：同一原始收據重播

| 測量範圍 | 參考／舊路徑 | 修正路徑 | 驗證及限制 |
| --- | ---: | ---: | --- |
| 固定 48 個原始回應的正文擷取 CPU 中位數 | 2.684 秒 | 1.712 秒 | 約少 36%；16 個鉅亨、16 個 Fed、16 個 Global Voices，正文／標題／作者／時鐘零差異 |
| 同一鉅亨原始回應，420 個 head 的解析到耐久接受 CPU 中位數 | 1.578 秒 | 0.774 秒 | 約少 51%；raw/body/file SHA、Parquet 與 SQLite 接受全部驗證，未省掉驗證或 fsync |
| 上述耐久鏈路 wall 中位數 | 2.615 秒 | 2.212 秒 | 約少 15%，不能將 CPU 減幅直接當成網路吞吐增幅 |
| 同一 WAL 的 1,311,185 個已存 head：單次統計 CPU 中位數 | 55.276 ms | 0.0146 ms | 9 次交替查詢，全部 provider 與總計一致；只代表這個查詢，不是整個服務變快數千倍 |
| 同一 WAL 的 7,994 個歷史 scope：覆蓋投影與 JSON CPU 中位數 | 137.268 ms | 33.233 ms | 約少 76%；66 個真實端點彙總，全部 scope／leaf／父區間／資料筆數／頁數／enabled／觀測時點與原始投影一致 |
| 上述 scope 投影 JSON | 3,462,453 bytes | 21,436 bytes | 縮小約 99.4%；不是刪掉 scope，完整詳細記錄仍留在 SQLite |

對照收據：
[最終固定語料 CPU 重播](../artifacts/data_quality/news_site_efficiency_20261009/cpu_accepted_final.json)、
[精確 head 統計](../artifacts/data_quality/news_site_efficiency_20261009/stored_head_query_benchmark.json)、
[全部歷史 scope 的彙總等價與成本](../artifacts/data_quality/news_site_efficiency_20261009/status_projection_benchmark.json)。
同輪多次重播的擷取 CPU 減幅約 30–39%，耐久接受 CPU 減幅約 44–52%；上表使用最後一次結果，
保留其他結果，不挑最快的一次當保證。這些是有界離線／查詢測量，沒有證明整個主機的 CPU 減幅或來源速率極限。

### 運行、資料與網頁的不同驗收

最後版本相關 14 個測試檔有
[625 項通過](../artifacts/operations/agent-workflow/runs/news-sites-final-regression-v5-20261009T043206-70ca7e3f/run.json)，
包含生命週期、HTTP／Retry-After、佇列回復、範圍登錄、分頁、耐久收據、資料語意、統計與網頁；
不是整個 repository 全套測試。
排序修正另有
[規劃測試](../artifacts/operations/agent-workflow/runs/news-sites-planning-relevance-te-20261009T043423-4e372a18/run.json)。

[目前 owner 的前後運行驗收](../artifacts/data_quality/news_site_efficiency_20261009/runtime_current_after.json)
通過目前 PID／owner／設定／程式 fingerprint 一致、心跳新鮮、來源間獨立增長與來源間隔檢查。
同一 owner 的約 133 秒觀察窗新增鉅亨 9,727 個、Global Voices 11 個、基因線上 1 個正文 head。
[近期 128 批次](../artifacts/data_quality/news_site_efficiency_20261009/recent_batch_current.json)
驗證 1,933 個正文版本且無錯誤；這不是全部 142 萬個 head 已在本輪重新驗證。

[本機部署的最終實際網頁驗收](../artifacts/data_quality/news_site_efficiency_20261009/dashboard_completed_stage/acceptance.json)
檢查 1440、768、390、320 px：完整來源清冊分頁、篩選／重設、搜尋、排序、進度與筆數、
新三來源、圖片正文獨立狀態、長表與無水平溢出均通過，沒有 JavaScript 錯誤或媒體網站請求。
使用既有 [/news/](https://penguin72487.ddnsgeek.com/news/) 頁面；
實測的是本機部署 `127.0.0.1:8770/news/`，不把這個結果當成對外 HTTPS 已驗證。
12:38 對外 HTTPS 嘗試在 TCP 連線階段逾時；本輪沒有改動外部網路、DDNS 或路由器設定。
瀏覽器仍只讀白名單快照，不讀 queue、正文或憑證，也不為顯示而呼叫新聞來源。

### 仍未完成的範圍與下一階段

全部來源鍵已經規劃，不等於全部來源已實測：**12:34 的上述快照**有 718 個鍵仍為 `not_reviewed`，
另 5 個審查未完成、3 個需先取得來源允許。原始網站條款與 robots 的限制是來源邊界，
不能用私人研究用途推定所有網站皆授權，也不能改成偽造成功或繞過 403／robots／付費驗證。

下一階段的高價值候選依清冊是聯合／經濟日報、富聯、MoneyDJ、自由財經、ETtoday 財經、
科技／生技產業及原始研究公告；先補各自來源規則、公開歷史與唯一文章身分的證據，
優先驗證能一次帶回多篇完整正文的端點，再接既有共用管線。
在上述 12:34 階段，TechNews／今周刊／公視的權利限制、環球生技的 robots 非規則頁、CMoney 的 403、
CMNews 的 robots 回應與 PeoPo 的 403／逐篇授權分別保留。CMNews 後續修復見下一節；
保留舊快照與失敗證據，不用最新修復回寫當時的結果。
詳細來源依據在 [來源審查清單](../configs/news/source_reviews_20261009.json)，
可派發追新／回補／固定批次與未知 ETA 都保留在既有進度頁。

<a id="news-root-repair"></a>

## 根因修復與目前驗收：2026-10-09 15:27 台北時間

本階段延續 `news-adaptive-capacity-20261009`，所有操作仍由原本新聞 queue／HTTP owner
負責。處理順序為辨識拒絕層級、修正已證實的客戶端錯誤、原始回應重播、實際正文入庫，
最後核對同 owner 的前後窗口；沒有建立另一套爬蟲、改身份或清除來源拒絕期限。

### 無法訪問與正文缺口：逐層修復

| 問題 | 已查明的機制與修正 | 實際證據／仍未完成 |
| --- | --- | --- |
| CMNews 文章請求前就被擋住 | `/robots.txt` 302 到已註冊同來源首頁，200 HTML 沒有 REP 群組。舊程式一律當作不可訪問；現在解析所有有效規則，僅對有本來源文章連結、身分一致、無登入／挑戰的註冊普通首頁認定未提供規則 | 正式 owner 已通過 robots 並取得文章 200；未知 HTML、錯誤／挑戰頁、Disallow 與真實 403／429 不會變成允許 |
| CMNews 文章 200 卻找不到正文 | 三篇原始 SSR 都沒有舊 `.article-content`。改用 `.articleContent__textContent`，先核對 canonical、data article path、UUID、目前作者 profile，限在本篇 scope 解析 | 三篇原始回應的 SHA／精確正文／作者／時鐘重播皆通過；不需要執行瀏覽器或解出隱藏 hydration 正文 |
| 網頁本篇分鐘日期被其他 schema／側欄干擾 | 只採與本篇可見標題、作者及分鐘時鐘一致的 NewsArticle metadata，保留來源實際的秒數、`+08:00`；沒有匹配就留下可見原值／未知 | 原始來源 08:10:50、10:50:19、14:20:23 均保留；不把抓取時間、metadata 日期或 URL 日期當發布時間 |
| 403 只有重試、沒有故障層級證據 | 在共用 HTTP 路徑新增只於失敗時寫入的私人不可變診斷，記錄來源、URL、status、安全 header 與已讀回應 SHA；正常文章不增加解析、雜湊或診斷寫檔 | CMoney 實際拒絕回傳 `Microsoft-IIS/8.5`、`via: kong/3.9.0`、403；連線已有 HTTP 回應，不是 DNS／TLS 尚未連上。仍不能僅憑 header 判定特定 WAF 規則或登入原因 |
| PeoPo 看起來不抓、驗收誤認停住 | provider 冷卻結束不代表單篇 `due` 已到。運行驗收 contract v2 用索引查實際到期的缺正文／本 owner 有效 lease，不再只看 retry 狀態數 | 該唯一剩餘 retry 的 `due` 為 15:35:43 台北；保留來源 403、原收據及未知完成時間，不把它改成完成 |

robots 規則處理依 [RFC 9309](https://www.rfc-editor.org/rfc/rfc9309.html) 的可解析行與重新導向要求；
[Google 的官方解析說明](https://developers.google.com/crawling/docs/robots-txt/robots-txt-spec#file-format)
亦明列 HTML 中有效規則的處理。這不是下載權利或空白規則的全面授權。
普通首頁特例仍綁定來源 capture fingerprint；改變來源範圍會重新驗證。

實作位置：
[robots 表示解析](../downloader/news_fulltext_robots.py)、
[共用 HTTP 與私人拒絕診斷](../downloader/news_fulltext_http.py)、
[已驗證的 CMNews 本篇 scope](../downloader/news_fulltext_publishers.py)、
[來源登錄](../configs/news/publisher_catalog.json)、
[有界存取診斷](../scripts/diagnose_news_access.py) 與
[到期工作運行驗收](../scripts/audit_news_runtime.py)。
正文 schema 僅補充匹配的來源 metadata，不代替可見正文；會員／付費正文不取用。

### 真正入庫與持續進展

[15:25 → 15:27 同 owner 驗收](../artifacts/data_quality/news_goal_stage01_20261009/runtime_clock_after.json)
核對 PID、程序起點、owner UUID、目前設定與載入程式、新鮮心跳、各來源間隔及近期正文收據，
九項工程檢查全通過。資料庫窗口為 151.344830 秒，HTTP 計數窗口為 152.889534 秒：

| 來源 | 同 owner 新增已存 URL head | 該 HTTP 計數窗口請求數 | 15:27 已存總數 |
| --- | ---: | ---: | ---: |
| CMNews 投資網誌 | 400 | 451 | 2,804 |
| 鉅亨 | 44 | 95 | 1,808,713 |
| Global Voices | 10 | 15 | 3,021 |

兩個時鐘不同，批次落盤也有窗口邊界，不能直接將每列相除當成一請求的精確正文收益。
CMNews 先前一直為 0 head；這次已有新正文耐久保存，不是只有 HTTP 200 或服務 active。
來源獨立進展，CMoney／PeoPo 的來源拒絕與等待仍明列，沒有算成全來源驗收。

[三篇私人原始回應的目前解析驗收](../artifacts/data_quality/news_goal_stage01_20261009/cmnews_parser_current_acceptance.json)
皆通過，原 raw/file SHA 與舊 `body_not_found` 證據仍保留。
[指定三網址耐久 head 核對](../artifacts/data_quality/news_goal_stage01_20261009/cmnews_exact_head_acceptance.json)
在 15:26 的結果是 **兩個已入庫、一個仍 pending**，整個指定集合當時未通過；
原始回應重播成功不等於第三個網址已從正式佇列派發。該項仍留在後續清單，不更換驗收集合。
後續 [16:00 相同三網址驗收](../artifacts/data_quality/news_goal_stage01_20261009/cmnews_exact_head_final.json)
已全數通過：第三篇在 15:52:12 正式入庫，正文 SHA、688 字與來源發布時間
`2026-10-09T14:20:23+08:00` 與原始診斷相符。15:26 的失敗證據原樣保留。
[15:27 近期 128 批次驗證](../artifacts/data_quality/news_goal_stage01_20261009/recent_clock_current.json)
核對 4,272 個正文版本、raw/body/file SHA 與 queue heads，錯誤 0；不是全湖所有正文重新驗證。

修正解析器 fingerprint 後，只重排既有解析失敗；403、429、404／410、來源拒絕、paywall
與冷卻皆保留。剛重啟的舊心跳也曾被起點檢查辨識，後續量測採用新 owner 新鮮基線。
第一次新 owner 前後核對因 PeoPo 尚未到期而判失敗的
[原報告](../artifacts/data_quality/news_goal_stage01_20261009/runtime_cmnews_after.json) 保留；
新的 v2 到期判準經測試與新窗口驗證，沒有修改舊報告或掩蓋拒絕。

### 全清冊、網頁、排程與仍未完成

[15:20:51 一致快照](../artifacts/data_quality/news_goal_stage01_20261009/inventory_cmnews.json)
有 4,758,445 個已知 URL、1,828,271 個已存正文 head；57 個設定 provider、11 個啟用、
681 個未註冊鍵。完整 738 個鍵都有
[逐來源實作／修復規劃](../artifacts/data_quality/news_goal_stage01_20261009/cmnews_plan.csv)；
其中 711 個仍未逐站核對、8 個審查未完成、7 個需來源允許。不是 738 家不同媒體已全數抓完。
較後的 15:27 head 數字是另一個移動快照，不能用兩個快照差值宣稱全部是本次修復增益。

另修正鉅亨舊單日頁數 migration 誤退役的 **18,135 個合法頁工作**，
見 [修復前](../artifacts/data_quality/news_goal_stage01_20261009/window_dispatch_before.json) 與
[修復後](../artifacts/data_quality/news_goal_stage01_20261009/window_dispatch_after.json)。
單日頁數上限本身仍存在；工作重新可排不表示那 18,135 頁已全部下載。
缺正文與已存正文追新採每來源 8:1 有界服務，避免只重查而不補缺；
`dispatch_work_kind` 的 ready-only 索引約 27.44 MiB。
[相同 WAL 的 5 次交替量測](../artifacts/data_quality/news_goal_stage01_20261009/dispatch_benchmark.json)
中，鉅亨指定 missing-first 選擇查詢中位數由 0.538158 秒到 0.000081 秒，無 TEMP sort；
這是精確查詢成本，不是整個爬蟲或主機變快數千倍，也沒有略過正文／收據驗證。

理財周刊固定已知集合 **552／2,441** 已存，作者／權利未確認、未支援格式與來源空模板
分開列出；未確認作者不是已證實第三方作者。
INSIDE 後續核對到集團明確自動抓取限制，已停止新請求，原 43 head 與收據未刪除。
這些判準與原日期在 [來源審查紀錄](../configs/news/source_reviews_20261009.json)。

[正式本機 gateway 網頁驗收](../artifacts/data_quality/news_goal_stage01_20261009/dashboard_cmnews/acceptance.json)
在 1440／768／390／320 px 通過：正文筆數／比例、738 鍵的搜尋篩選排序分頁、CMNews 與
理財周刊來源卡、唯讀／不向媒體發請求、沒有 JS 錯誤與水平溢出。畫面也已目視核對；
當時 owner 正在清點新增 metadata，頁面呈現 `inventorying`，沒有假稱全程皆是 `running`。
只驗證 `127.0.0.1:8770/news/`；不是外網 DDNS／HTTPS 已驗證。

目前最後版本有
[747 項相關回歸通過](../artifacts/operations/agent-workflow/runs/news-goal-final-clock-regression-20261009T072613-a0493d8e/run.json)，
涵蓋 19 個測試檔，含 robots／存取診斷、來源身分／正文／時鐘、dispatch／retry／lease、
耐久收據、限流與公開頁面。不是整個 repository 全測試，也不是來源最高速已量測完成。
來源未公布數值限制時，沿用有界漸進升速與實際新正文收益量測；設定 30 req/s 本身不是已達 30 req/s。

15:27 時尚未派發的 CMNews 指定新網址已在下一節完成入庫驗收。仍需核對 CMoney 的
公開支援端點／原始媒體與實際拒絕原因，以及環球生技等非規則 robots 回應和其他未核對來源。
真實來源拒絕、登入／付費、權利邊界與 source-not-found 不能靠客戶端更換身分修復；
但不能把網路拒絕、格式問題、解析器缺口與正常未到期工作混為一談。全來源完成 ETA 仍未知，
持續追新與已知範圍條件估時分開，全新聞目標保持 active。

<a id="news-origin-fairness"></a>

## HTTP 舊網址與無日期文章修復

### 轉址核對的根因與真實入庫

一筆 `http://cmnews.com.tw/article/...` 的 robots 請求會由來源重新導向至
`https://cmnews.com.tw/`。表示分類器原本拿 HTTP 原點組成首頁身份，因而錯拒已登錄的
HTTPS 首頁；該筆錯誤又打開 provider circuit，讓所有 CMNews 文章每次等待 300 秒。
這是客戶端身份核對錯誤，不是該來源拒絕訪問。原 failure-only 診斷仍保留。

REP representation v2 核對**實際重新導向**的同 host HTTP → HTTPS 升級，仍要求已登錄的
普通首頁、相符 canonical／og URL 與至少兩個本來源文章連結。來源禁止規則、不同 host、
HTTPS 降級、任意 HTML、登入和 challenge 都不因此變成允許。
robots cache 仍綁定最初原點，並另存實際取得首頁；HTTPS 文章的 robots 也獨立核對。
規則重新導向依 [RFC 9309 第 2.3.1.2 節](https://www.rfc-editor.org/rfc/rfc9309.html#section-2.3.1.2)
處理；它不是正文、重製或訓練的授權。明示 challenge header 現在也先於 plain-text 路徑檢查。

[失敗五次舊網址的耐久收據](../artifacts/data_quality/news_goal_stage01_20261009/cmnews_http_origin_head.json)
證明它在 **15:55:10** 自然到期後取得 194 字正文：保留原始 HTTP requested URL、實際 HTTPS
final URL、raw/body/file SHA 和來源原樣發布字串，沒有清冷卻、換身分或猜測別的文章網址。
該來源發布字串含 2024 年及更新註記，與既有 metadata 的 2026 年不同；原值均保留，
不拿 metadata 排序日期或抓取時間代替發布時間。這份資料不自動注入 PIT／訓練。

### 無日期文章的派發與計算成本

首頁發現的 17 篇文章沒有 metadata 日期。舊 latest-first 排序讓它們始終落在數萬篇有日期的
歷史缺正文之後，連當天已公開文章也沒機會下載。article dispatch contract v2 增加獨立的
`dated_body_undated_burst: 8`：每八次有日期缺正文派發後，服務一次到期的無日期缺正文。
既有每八次缺正文服務一次已存正文追新的時鐘獨立保留；兩個計數不一起清零。
沒有到期工作就回到一般派發，不浪費下載槽位。發布時鐘、來源冷卻、retry `due`、lease、
finite cohort、未知與已完成狀態都保留，不為優先下載造日期。

首次完整 snapshot 查詢發現，`空日期 OR NULL` 雖沒有 TEMP sort，仍會掃過有日期的缺正文。
改為相同 ready-only 索引的兩次等值 range seek，用 `UNION ALL` 合併原有空字串／NULL 順序。
不新增大索引、不做全文掃描，也不刪掉 NULL 支援。
[相同 WAL 快照三次交替量測](../artifacts/data_quality/news_goal_stage01_20261009/dispatch_undated_seeks.json)
核對 CMNews、鉅亨、Global Voices 的 missing、recheck、undated 九個案例，索引／參考查詢的
排序與資格相同且沒有 TEMP sort。無日期查詢的三狀態合計中位數分別約 35、47、36 微秒。
這是查詢成本，不是來源最大吞吐量或整條服務的倍數改善；較早
[OR 條件快照](../artifacts/data_quality/news_goal_stage01_20261009/dispatch_undated_full.json)
與現在是不同時間的移動資料，不能直接當成同工作量的整體速度比較。

### 正式服務與剩餘問題

[15:57 → 15:59 同 owner 驗收](../artifacts/data_quality/news_goal_stage01_20261009/runtime_origin_seek_after.json)
九項工程檢查全通過，核對目前 PID、程序與 owner 起點、載入程式與設定、heartbeat、
來源最小間隔，以及新鮮不可變正文收據。資料庫窗口為 130.085829 秒，HTTP 計數窗口為
135.426101 秒：

| 來源 | 新增已存正文 URL head | 窗口 HTTP 請求 | 15:59 已存總數 |
| --- | ---: | ---: | ---: |
| CMNews | 684 | 690 | 4,491 |
| 鉅亨 | 62 | 93 | 1,809,266 |
| Global Voices | 13 | 14 | 3,193 |

不把不同時鐘的請求與落盤相除成精確每 call 收益。
[近期 128 批次](../artifacts/data_quality/news_goal_stage01_20261009/recent_origin_current.json)
共 5,375 正文版本、0 錯誤；這不是全湖、未來寫入或全部來源歷史的證明。
[770 項相關回歸](../artifacts/operations/agent-workflow/runs/news-goal-final-origin-regressio-20261009T075614-82bc7f5c/run.json)
與 [19 項收據／轉址針對測試](../artifacts/operations/agent-workflow/runs/news-goal-selected-redirect-cont-20261009T080031-89831549/run.json)
皆通過，不等於整個 repository 的全測試套件。

本機 `/news/api/status` 在 15:59 直接讀到一致的新鮮 running 狀態、CMNews 已存數與條件 ETA，
來源冷卻則保留 CMoney 16:08:10、PeoPo 16:05:45 的 next-attempt 和未知 ETA。
這不是外部 DDNS／HTTPS 連線驗收。CMNews 的 30 req/s 仍是 client 設定，不是官方或已證實
可持續最大速率。

下一步處理真實 403 的來源支援方式／原始來源，並逐一核對仍未登錄的來源；CMNews 尚有
9 個 `body_not_found`、3 個 source-not-found 和 5 個非文章 namespace 排除，需逐項分清短正文、
付費預覽、沒有正文與解析缺口，不能一律稱為來源資料錯誤。全新聞目標繼續執行。

## 本篇資料表與會員遮罩修復

### 原網址缺口的逐項診斷

[同一批 11 個原網址的診斷](../artifacts/data_quality/news_goal_stage01_20261009/cmnews_missing_body_access.json)
保留每篇的 requested／final URL、回應 SHA、容器字數、文章身分及遮罩結構，未保存會員回應全文。
10 篇在本篇正文內明示需登入，與 [原站 VIP 文章提示](https://cmnews.com.tw/article/jiahongxlinying-a2092984-c2fa-11f1-b685-a144ac2ae47f)
相符。CMNews 的 JSON-LD 不一定填寫 `isAccessibleForFree`；只看該欄會把遮罩當解析缺口，
有較長預覽時還可能誤收。現在先核對 canonical、文章 path／UUID、作者對應身分，再檢查本篇
`vip-content.vipWrapper` 內的可見遮罩。導航登入按鈕、相關文章的遮罩及引用句子不影響本篇。
[原集合重新診斷](../artifacts/data_quality/news_goal_stage01_20261009/cmnews_body_structure_v3.json)
證明這 10 篇均分為 `paywall_or_login`，不是已取得正文，也不因解析版本更新重排付費內容。

剩下的 [CM法人團隊公開公告](https://cmnews.com.tw/article/cmfaren-b85c2866-c2ff-11f1-8b79-91dcf7ccf38a)
原容器有 178 字，但清理後只剩 5 字。正文的 172 字資料表被編輯器放在
`figure.table > table`，通用圖片清理把整個 figure 刪掉了。
CMNews adapter contract v4 只在已核對的本篇正文、通過存取檢查後，解除包住單一直接 table 的
`figure.table`；圖片 figure、沒有直接 table 的偽容器、相關文章及會員遮罩不變。
沒有放寬 100 字最低門檻，也沒有用標題、作者簡介、JSON-LD／hydration 隱藏文字湊正文。

[原公告耐久收據](../artifacts/data_quality/news_goal_stage01_20261009/cmnews_table_durable_head.json)
證明正式服務於 **16:26:04** 自然派發補回 178 字正文，body SHA 為
`487cbcd82596e7697f12a125010982aa407bb45665cde44f9a55be5eff2a7ed2`。
來源原始發布字串 `2026-10-08T18:04:59+08:00` 保留；requested／final URL 相同，
raw/body/file SHA、所在批次 19 個正文版本和 queue head 都通過驗證。
資料保持私人用途、不發布或自動輸出訓練；來源用途範圍不等於逐篇開放授權。

### 正式運行與覆蓋邊界

正式 owner 於 **16:25:27** 載入 v4。
[16:26 → 16:29 同 owner 驗收](../artifacts/data_quality/news_goal_stage01_20261009/runtime_cmnews_v4_after.json)
九項工程檢查通過，包含載入程式／設定、PID／owner 起點、新鮮狀態、來源最小間隔與正文收據。
資料庫窗口 199.282314 秒、HTTP 計數窗口 197.704936 秒：

| 來源 | 新增已存正文 URL head | 窗口 HTTP 請求 | 16:29 已存總數 |
| --- | ---: | ---: | ---: |
| CMNews | 682 | 633 | 13,555 |
| 鉅亨 | 33 | 82 | 1,809,927 |
| Global Voices | 22 | 20 | 3,351 |

落盤批次與 HTTP 計數的窗口邊界不同，可能包含窗口前請求的延後提交；不把上表相除當成
每 call 篇數、官方最大速率或本輪加速倍數。鉅亨窗口仍有正文缺口、502 和 timeout，
九項工程檢查通過不表示所有端點／文章都成功。
[近期 128 批次收據](../artifacts/data_quality/news_goal_stage01_20261009/recent_cmnews_v4_current.json)
驗證 7,805 個正文版本、0 錯誤；不是全湖、舊版本內容語意或全部歷史的驗收。
[780 項相關回歸](../artifacts/operations/agent-workflow/runs/news-goal-cmnews-v4-regression-20261009T082527-425fbe21/run.json)
涵蓋會員／相關文章隔離、資料表與圖片容器、派發、來源限流、不可變收據和面板；不是全 repository 測試。

16:29 快照仍有 CMNews 1 篇 `body_not_found`，原站是圖片搭配短文字的
[CMoney官方文章](https://cmnews.com.tw/article/cmoney-24d662f0-9146-11f1-b81d-ed9af7479378)，
需核對媒體／文字範圍，不能把短促銷文字當完整新聞。
CMoney 真實 403 的下次可嘗試時間為 16:38:11，PeoPo 單篇拒絕為 16:35:50；
這是 next-attempt，不是成功 ETA。支援的存取方式、真正原始來源與其他未登錄來源繼續核對，
不清拒絕證據、不換身分繞過來源限制，也不將全新聞目標標成完成。

## 台南市政府與金門日報存取修復

### 網址與文章身分

[原站逐網址診斷](../artifacts/data_quality/news_goal_stage01_20261009/government_sources_access_initial.json)
保留八個文章／政策觀察及 robots 路徑，不以瀏覽工具的 timeout 推定原站拒絕。
台南已知 frontier URL 使用 `n=13370&amp;s=7868304`，正式 HTTP 收到 410；
[同一官方文章的正常 n/s 查詢](https://www.tainan.gov.tw/News_Content.aspx?n=13370&s=7868304)
收到 200，頁面自己聲明相同文章編號及標題。原先診斷用的 `.area-editor` 選中導覽／頁尾，
不是 999 字正文；該診斷從未當成正式文章接受。

[來源 adapter](../downloader/news_fulltext_taiwan_pages.py) 只修正該官方 host、
`News_Content.aspx`、`n=13370` 與單一數字 `amp;s` 的已實證表示法。
額外參數、簽章、歧義、多個文章編號、其他主機或非 HTTPS 不改寫。
保留原 frontier URL／article ID，實際 request／final URL 與 `url_resolution_basis` 另外保存；
不做全域 HTML 解碼、不切換 origin host、不在來源拒絕後另換網址重試。
擷取前核對官方 `og:url` 的 n/s、可見標題、唯一正文容器及新聞及國際關係處發稿署名。
發稿時間原文 `111年1月20日11:50(12:24更新)` 保留，未捏造時區、秒數、歷史修訂版本或 PIT。

金門四個原網址都收到 200；正文位於本篇 `section.cp`，不是原候選容器。
三篇地方新聞的 `og:url` 錯指 262893，快訊錯指 261387；不能因此下載舊文章或誤判四篇都不可取得。
現在核對唯一可見標題、當篇 function panel 中兩個獨立分享 URL 的分類與文章編號，
再取該範圍的正文、實際記者／作者欄及日期。分享連結只在原始 HTML 中核對，**不請求社群站**。
舊 `og:url` 仍保留在原始回應，不偽造 canonical；點閱數、照片說明、相關文章及 JSON 隱藏文字不充正文。
來源只發布日期的四篇，不借用 FinMind metadata 的 07:00 當實際發布時分。

兩個來源仍按各自規則處理。[台南網站資料開放宣告](https://www.tainan.gov.tw/cp.aspx?n=13612)
不涵蓋被另行限制或第三方的作品；目前只收已核對的官方發稿文字。
[金門日報 RSS 說明](https://www.kmdn.gov.tw/1119/1120/1409/) 的閱讀範圍和報社著作權保留，
與政府開放宣告不同；`.gov.tw` 不等於公有領域。這四篇只作私人固定批次，
不主張全站重用、轉載或訓練授權，不發布正文或自動匯出訓練。

### 已入庫原文與驗收

兩個 provider 已註冊到 [共用來源清冊](../configs/news/publisher_catalog.json)，
正式服務於 17:08:25 至 17:08:32 自然派發取得五篇，17:08:34 提交到同一不可變批次。
不是第二套爬蟲或另存成獨立資料湖。
[最終離線驗收](../artifacts/data_quality/news_goal_stage01_20261009/government_articles_offline_acceptance_v2.json)
核對原始回應、私人診斷收據及最終程式雜湊；政策頁不算新聞正文。
[正式 head 與固定集合驗收](../artifacts/data_quality/news_goal_stage01_20261009/government_article_durable_heads_v2.json)
核對原網址、實際請求、source fingerprint、作者／時鐘、重新解析結果、raw/body/file SHA、
queue head 與兩個凍結集合的收據；五篇皆通過，沒有重置來源拒絕或改換驗收集合。

| 來源與原文章編號 | 本篇發布字串 | 正文字數 | 正文 UTF-8 bytes | 結果 |
| --- | --- | ---: | ---: | --- |
| 台南市政府 7868304 | 111年1月20日11:50(12:24更新) | 999 | 2,847 | complete，原轉義 URL 保留 |
| 金門日報 306415 | 2019/05/26 | 920 | 2,722 | complete |
| 金門日報 327827 | 2021/01/17 | 1,024 | 3,034 | complete |
| 金門日報 543633 | 2022/05/27 | 546 | 1,604 | complete |
| 金門日報 332881 | 2021/06/18 | 939 | 2,649 | complete |

台南固定集合 1／1、2,847 bytes，金門 4／4、10,009 bytes，皆 100%，沒有待處理工作。
此批五篇位於包含其他來源文章的 64-version 批次；批次全部 raw/body/file SHA 與這五個當前 head
皆驗證。這不是所有來源歷史的驗收，也沒有把其他來源的 59 個版本算成本輪五篇。
來源未提供可證明的數值速率上限；本批沒有作飽和壓測或宣稱理論最大速度。

[819 項相關回歸](../artifacts/operations/agent-workflow/runs/news-goal-government-final-regre-20261009T090840-9bd3e154/run.json)
包含網址轉義與簽章保護、文章／時鐘／作者隔離、來源範圍、正文缺口、拒絕不繞行、
固定集合、限流／派發、收據與面板；不是整個 repository 的全測試。
初次輸入測試檔名與舊允許來源清單造成的回歸失敗保留，修正後才採用這份最終通過結果。
[近期 128 批次驗證](../artifacts/data_quality/news_goal_stage01_20261009/recent_government_current.json)
另核對 7,745 個正文版本、0 錯誤；只證明該已提交範圍，不證明未來寫入、全湖或全部歷史完整。

### 面板與整體剩餘工作

[本機唯讀面板驗收](../artifacts/data_quality/news_goal_stage01_20261009/dashboard_government_current/acceptance.json)
在 1440、768、390、320 px 實際瀏覽並核對兩個來源卡片，顯示 1／1、4／4 和 100%，
不把私人原文傳入瀏覽器。
[桌機來源卡片](../artifacts/data_quality/news_goal_stage01_20261009/dashboard_government_current/desktop-enabled-sources.png)
可看到來源範圍與結果；固定批次完成後不抓首頁、feed、sitemap 或重查正文，
後來新增 metadata 仍另列集合外成長。**此輪本機 gateway 通過；外部 DDNS HTTPS 未重新驗證。**

[17:09 至 17:14 同 owner 驗收](../artifacts/data_quality/news_goal_stage01_20261009/runtime_government_after.json)
九項工程檢查通過，包含實際 PID／owner／載入程式與設定、狀態新鮮度、正文收據和來源最小間隔。
資料庫窗口 284.165417 秒、HTTP 計數窗口 286.342932 秒：
CMNews 新增 1,922、鉅亨 87、Global Voices 29 個正文 URL head，健康來源各自持續推進；
台南與金門沒有重抓。此時 CMNews 已存 30,148 個 head，但全歷史仍未完成。
鉅亨窗口仍有正文解析與 timeout 失敗，不把整鏈路工程通過等同每個端點成功。
批次提交與 HTTP 窗口不同，不把上述數字相除當作每 call 正文數或加速倍數。

[17:17 全來源清冊](../artifacts/data_quality/news_goal_stage01_20261009/providers_after_government.csv)
與 [全部端點](../artifacts/data_quality/news_goal_stage01_20261009/providers_after_government.endpoints.csv)
保留 738 個來源鍵、59 個設定 provider，其中啟用 13 個、未登錄 host 鍵 679 個；
移動快照共有 4,758,588 URL、1,862,256 已存正文，並非本輪新增數或 738 家不同媒體。
一次一致快照掃描約 20.15 秒、CPU 16.90 秒，只作此輪盤點，不加入每分鐘面板輪詢。
[更新後逐來源排程](../artifacts/data_quality/news_goal_stage01_20261009/remaining_plan_after_government.csv)
仍包含全部 738 列：6 個沿用現行 owner、7 個有限集合、668 個小來源待核對、42 個較大來源待核對、
10 個原站映射、4 個平台／社群、1 個閉站來源。
CMoney 真正 403、PeoPo 單篇拒絕及其餘未核對來源繼續按支持的端點、實際原站、解析及權利範圍修復，
不把冷卻或規劃完成當成存取修復；全新聞目標維持 active。

## 匯流與信傳媒原文及署名修復

### 實際存取與根因

[最初存取診斷](../artifacts/data_quality/news_goal_stage01_20261009/cnews_cmmedia_access_initial.json)
與 [本篇結構](../artifacts/data_quality/news_goal_stage01_20261009/cnews_current_header_structure.json)
使用唯一新聞 HTTP owner、原 UA／origin 及既有 robots／限流路徑。
瀏覽工具的 403 與本機正式 HTTP 的 200 分開記錄；沒有改 UA、切換代理、猜 API 或清拒絕期限。

| 根因 | 實作修復 | 保留的邊界 |
| --- | --- | --- |
| 匯流的導覽登入 form 被當作本篇付費牆 | 只辨識位於正文／標題外的既有 `#modalLogin > #login`，不將全站登入字樣當成本篇限制 | 真正 paywall、其他 login form、本篇付費 schema 仍排除 |
| 本篇標題不是 h1，原 CSS 不存在 | 核對唯一 `#articleTitle` 的可見 strong、canonical／og:url 與當篇 `post-ID` | 不取 logo、延伸閱讀、sidebar 或隱藏 schema 正文 |
| 編輯部 header 與正文記者不同 | 保留 `cnews001` 編輯帳號、實際記者／共著者及各自原文角色 | 不把編輯帳號假裝成記者，也不替未知第三方授權 |
| 舊稿有不同署名語法 | 支援斜線後的具名記者、只署本社報導、`nongdesign` header 與正文具名記者 | 本社未具名時 `body_reporter_names` 保持空；不捏造人名 |
| 信傳媒 author itemprop 包住整條資訊列 | 只讀本篇 `.author_name`，不把日期、字級按鈕與圖片來源併入姓名 | 原正文 SHA 與來源秒數／時區不改 |

以上沿用 [來源 adapter](../downloader/news_fulltext_cnews_cmmedia.py) 與
[共用解析器](../downloader/news_fulltext_policy.py)，不是新建另一套爬蟲。
只讀正式來源 HTML；匯流 `/wp-json/`、search、tag、admin，以及信傳媒 API／preview
禁止路徑不作下載端點。[匯流版權聲明](https://cnews.com.tw/%E7%89%88%E6%AC%8A%E8%81%B2%E6%98%8E/)
不推導出全站歸檔、轉載或訓練授權；[信傳媒聲明](https://www.cmmedia.com.tw/home/copyright)
的私人／教育用途與排除條件也沒有套用到第三方作品。此批仍為私人固定閱讀集合，
不發布正文、媒體、不自動匯出訓練，也不是合理使用的法律判定。

### 有證據的回補，不重設真正的拒絕

正式服務的唯一 queue owner 執行兩次可重啟的版本遷移，原結果直到取得新結果才更新：

- 舊 v1 capture fingerprint 下的 32 個署名／來源誤分類：28 篇在 v2 取得正文。
- [剩餘舊稿原站診斷](../artifacts/data_quality/news_goal_stage01_20261009/cnews_remaining_scope_review.json)
  證明其中 3 篇是舊署名格式；v3 只重排該 pinned v2 與三個精確 URL SHA，補回 3 篇。
- 不重排其餘 guest／第三方範圍結果、404、付費、403／429 或已存正文。
  `queue_settings.cnews_editorial_byline_v2_repair`／`cnews_original_credit_v3_repair`
  保存舊／新 fingerprint、原 outcome、URL 數、集合 SHA 和時間；重啟不再重排。

[最終原站私人回應驗收](../artifacts/data_quality/news_goal_stage01_20261009/cnews_cmmedia_offline_acceptance_v4.json)
包含 12 個真實公開原文，先驗證診斷 Parquet／receipt／response SHA，再重解析；政策頁不算文章。
[正式 12 個 head 驗收](../artifacts/data_quality/news_goal_stage01_20261009/cnews_cmmedia_durable12_v4_current.json)
另核對當前 queue head、raw/body/file SHA，並用現行程式重解析正文、標題、作者及發布字串，全部一致。
涉及 448 個不可變 batch versions，只有 12 個是此明列驗收集合；沒有將其他文章計入 12 篇。

最後的 v4 另防止沒有姓名的「報導／綜合報導」被解析成一個人名；缺署名分類保持未知，
不借用旁邊文章的作者。舊 v1／v2／v3 捕獲版本的 fingerprint 仍原樣保留，沒有回填成 v4。
`capture_contract_matches_current_source=false` 與 `current_source_replay_verified=true`
可同時成立：前者表示捕獲版本較舊，後者表示其實際原文通過現在的語意檢查。
來源秒數、時區、只發布日期時的原精度均保留；不是重建當年的發布版本／PIT 證據。

### 固定範圍資料與未取得原因

[所有凍結集合驗收](../artifacts/data_quality/news_goal_stage01_20261009/cohorts_cnews_cmmedia_current.json)
以同一唯讀 WAL snapshot 核對 URL membership／SHA／receipt，以及正式已存 head。
其中這兩個來源的結果為：

| 來源 | 固定網址 | 已存正文 | 正文 UTF-8 bytes | 來源聲明發布範圍 UTC | 未取得 |
| --- | ---: | ---: | ---: | --- | --- |
| 匯流 | 70 | 65，92.86% | 181,090 | 2017-03-06T07:20:08 至 2026-09-15T14:48:52 | 1 guest 署名範圍未確認、1 原站 404、3 FTV／CTWANT 轉載 namespace |
| 信傳媒 | 355 | 354，99.72% | 1,147,500 | 2018-03-08T03:55:27 至 2026-10-05T00:04:39 | 1 明列第三方授權轉載 |

419 個已存 head 皆有作者及來源聲明時鐘，raw/body/file 與固定集合收據核對無錯。
這是本機已知固定 URL 集合，**不是媒體所有年份／文章的全站完整率**。
信傳媒剩餘 [27682 原文](https://www.cmmedia.com.tw/home/articles/27682) 實際為公開 HTTP 200，
且明列授權轉載自第三方作者；不是下載器壞掉、沒有正文或來源拒絕。
匯流尚未核對的 guest 也不偽稱來源 403。三個轉載 namespace 需找 FTV／CTWANT 原始作品，
不以匯流本社記者的捕獲範圍代理第三方。原站 404 保留，尋找候選原文時不以相近新聞補成同一篇。
這六個未取得結果不算完成；來源持續阻擋／署名權利未知時，不給假倒數 ETA。

### 驗收工具、網頁與全清冊

[選定文章驗收器](../scripts/audit_news_selected_articles.py) 現在每個不同 batch 只作一次投影讀取，
再由既有不可變 verifier 查 SHA／receipt。舊鄰居版本只需保留有效 raw/body/file，
只有所選當前 head 才比對最新 pointer，避免正確歷史批次被誤報損壞。
增加 `--reparse-current-source` 可在不呼叫來源、不改 queue／capture fingerprint 的情況下，
核對所選 HTML 原文與現行解析器。測試保留三個舊 versions，驗證兩個當前 head，
同 batch 讀取次數由逐文章降低到投影一次加共同 SHA 驗證一次；沒有宣稱生產加速倍數。
[768 項最終相關回歸](../artifacts/operations/agent-workflow/runs/news-goal-cnews-v4-final-regress-20261009T103953-6a57a52c/run.json)
涵蓋 20 個測試檔與上述保護，不是全 repository 測試；早先猜錯測試檔名、fixture 誤斷言與
原型解析失敗保留，沒有把試驗 log 的模擬速率當真實來源上限。

[最新本機網頁驗收](../artifacts/data_quality/news_goal_stage01_20261009/dashboard_cnews_cmmedia_v3_current/acceptance.json)
在 1440、768、390、320 px 都顯示 65／70、354／355，比例與同一 API snapshot 一致。
空 pending 不顯示全正文完成；尚未取得原因與未知 ETA 保留，不向瀏覽器送原文、金鑰或來源帳號。
此輪核對本機 gateway，**未重新驗證外部 DDNS HTTPS**。

[18:23 全來源盤點](../artifacts/data_quality/news_goal_stage01_20261009/providers_after_cnews_cmmedia.csv)
與 [全端點](../artifacts/data_quality/news_goal_stage01_20261009/providers_after_cnews_cmmedia.endpoints.csv)
仍完整保存 738 個來源鍵、59 個設定 provider、15 個啟用來源、679 個未登錄 host 鍵，
移動快照 4,758,597 URL、1,888,894 已存正文；不是本輪新增量或不同媒體家數。
盤點一次 wall 21.68 秒／CPU 19.63 秒，不加入面板輪詢。
[完整下一階段工作清單](../artifacts/data_quality/news_goal_stage01_20261009/remaining_plan_after_cnews_cmmedia.csv)
保留 6 個現行 collector、9 個有限集合、667 個小來源待核對、41 個較大來源待核對、
10 個原站映射、4 個平台／社群及 1 個閉站來源；沒有縮小全新聞目標。

### 最終執行期與保留的失敗證據

[第一個 v3 執行期窗口](../artifacts/data_quality/news_goal_stage01_20261009/runtime_cnews_cmmedia_v3_after.json)
沒有通過整體驗收，原因有二：窗口內當時就緒的 CMoney 實際再次收到 403；
提供的舊收據快照已超過 300 秒新鮮度。此結果保留，不把新增來源拒絕從窗口刪掉，
也不放寬驗收期限。當時健康來源仍新增 CMNews 3,518、鉅亨 295、Global Voices 49 個 head；
這不證明 CMoney 已修復。

[最終 v4 同 owner 窗口](../artifacts/data_quality/news_goal_stage01_20261009/runtime_cnews_cmmedia_v4_after.json)
於 18:41:56 至 18:43:24 核對實際 PID 1435251／owner、載入版本與設定、狀態新鮮度、
來源間隔、健康就緒來源進展及新鮮正文收據，九項工程檢查通過。
資料庫窗口 87.387938 秒／HTTP 計數窗口 90.344129 秒：
CMNews 新增 580、鉅亨 44、Global Voices 9 個正文 head；匯流與信傳媒沒有重抓已完成正文。
提交與 HTTP 窗口不同，不將此相除當成每 call 正文量、官方速率上限或加速倍數。
[同命令先更新的近期收據](../artifacts/data_quality/news_goal_stage01_20261009/recent_cnews_cmmedia_v4_current.json)
驗證最近 128 個已提交批次的 8,086 個正文版本、0 錯誤；只證明該固定新寫入範圍。

此窗口開始前 CMoney／PeoPo 已有實際拒絕期限，分開保留、不算完成：
下一次可嘗試時間分別為 18:53:28、18:50:54，不是成功 ETA。
先前來源拒絕已存在時，健康來源的執行期驗收與全來源取得成功是不同判準；
既沒有清拒絕來製造快速度，也沒有把「沒有待跑工作」或 service active 當全部完成。
真正支持的來源端點、三篇轉載的原作者／原站，以及其餘 738 列中的未核對來源仍繼續處理；
全新聞目標與原 task `news-adaptive-capacity-20261009` 保持 active。

<a id="nownews-ftv-root-repair"></a>

## NOWnews 與民視原文存取及回補修復

這輪先將「無法訪問」拆成傳輸／來源拒絕、文章身分、正文選取、署名／作品範圍及時鐘，
使用同一下載器取得的實際公開回應判定。網站 HTTP 200 可能仍是轉載、空頁或會員預覽；
解析器拒絕也不代表原站封鎖。沒有以放寬所有條件或反覆清 cooldown 代替根因修復。

### 根因與實作

| 根因 | 共用下載器修復 | 保留的邊界 |
| --- | --- | --- |
| NOWnews regular 與 AMP 的正文容器不同，泛用選取混入作者介紹與推薦 | 核對精確文章 ID、可見標題、當篇 canonical／schema，分別選取真實正文 | 不取隱藏 hydration 全文，不在拒絕後切 AMP |
| 舊 regular 記者署名沒有 profile 連結 | 可見完整具名署名、當篇 schema 與 author meta 必須一致；保留來源角色 | 不捏造 profile，不借旁邊文章的作者，不代替第三方作品權利 |
| 同一 time 中同時列出版與更新時間，原本串在一起解析 | 分別讀來源明標的時間；核對秒數、時區、schema 與 meta，保存兩個 raw clock | 更新不當作首次發布，現在抓到的修訂版不回填成歷史 PIT |
| 民視舊 TV 稿署名在正文尾；另一版把署名與長正文放同一 p，以 br 分開 | 核對正文內實際本社 credit 的第一邏輯行，保留原始全文與各個來源署名 | 不拿圖片 caption、隱藏文字、第二行或第三方 credit 充當本社作者 |
| 民視財經／運動／健康子站把本地時間宣告成 +00 | 只依實際公開頁明列的同 ID www canonical，再獨立核對原文／時鐘；兩次原始回應均保存 | 不猜替代網址、不直接改時區、不在 403／429 後換站繞過 |
| 新找到的民視原文不屬於舊固定集合，原本正確留在 waiting_cohort | 增加明列、來源回應 SHA 已核對的不可變 supplement；重用原 queue 與收據 | 原 membership／SHA 不變，不自動膨脹範圍、不重置拒絕／已存 head |

實作集中於 [公開原文 adapter](../downloader/news_fulltext_public_originals.py)、
[共用 collector](../downloader/news_fulltext.py)、[queue](../downloader/news_fulltext_queue.py)
及 [有限集合](../downloader/news_fulltext_cohorts.py)，沒有另開常駐爬蟲或平行來源 owner。
所有 publisher 仍獨立排程，共用不可變原始回應／正文／receipt 與單一佇列提交。

[NOWnews 版權聲明](https://www.nownews.com/copyright) 的個人非商業用途與第三方排除條件，
以及 [使用條款](https://www.nownews.com/terms) 的不當 BOT／干擾服務限制，沒有被推導成
全站歸檔、分享或訓練授權。民視原頁版權與 robots 也不等於全面再利用授權。
本批仍為既有私人閱讀集合；不向瀏覽器提供正文、不自動匯出訓練，不作合理使用的法律判定。

### 真實回補及來源時鐘

20:35 的 [完整來源快照](../artifacts/data_quality/news_goal_stage01_20261009/providers_after_root_repair_v11.json)
記錄 NOWnews 從 291 增為 527 個正文 head：186 個署名解析誤判由 capture-contract 變更回補；
50 個時鐘誤判另由 pinned 舊 fingerprint 的單次遷移修復。
遷移只動 `invalid_bulk_publication_clock` 且缺正文的舊版 NOWnews 原網址；
保存舊／新 fingerprint、原 outcome、URL 集合 SHA 與時間。
已存 head、來源 404、真實拒絕、付費與第三方範圍不重排；重啟也不再重複遷移。

[12 個正式 head 驗收](../artifacts/data_quality/news_goal_stage01_20261009/public_originals_twelve_durable_v11.json)
核對 raw／body／file SHA 及作者／標題／發布／更新原字串；兩個子站樣本另有獨立 alias
binary／SHA／來源 canonical 證據。以下僅列有助定位錯誤的時間，不當作歷史版本證明：

| 當篇來源 | 原始發布字串 | 原始更新字串／核對方式 |
| --- | --- | --- |
| NOWnews 6879722 | 2026-10-01T15:04:26+08:00 | 2026-10-03T10:58:38+08:00；各自核對可見標籤 |
| NOWnews 6880048 | 2026-10-02T14:58:02+08:00 | 2026-10-02T14:59:04+08:00；不串接或混用 |
| 民視 2019624F02M1 | 2019-06-24T12:02:21+08:00 | 2019-06-24T23:54:29+08:00；TV reporters 與 schema author 分開保存 |
| 民視財經 2026814W0767 | www 原站 2026-08-14T20:57:48+08:00 | 子站 +00 宣告原樣保留；不直接加減八小時 |

最後一個財經換行署名樣本有 881 字正文，正式入庫並通過現行重解析。
其他民視署名解析結果仍在逐篇核對；例如只列 schema 編輯姓名而沒有可確認本社 credit 的原頁，
不能直接當成具名 TV 記者或權利已確認。來源 clock／署名未知與傳輸拒絕是不同結果。

### 固定集合、網頁及未取得原因

[集合驗收](../artifacts/data_quality/news_goal_stage01_20261009/cohorts_final_root_v11.json)
通過原 membership／SHA／receipt 完整性，但 `all_cohort_bodies_stored=false`。
民視 2,629 個原固定網址與 1 個已核對追加分開；新增原文的 receipt 及原始證據都不可變。
當原原始媒體文章找不到時，轉載來源 namespace 不會因找到相近標題或另一站正文而被改成完成。

20:35 同一快照的未取得結果如下；會隨正式回補改變，並非現在或每家媒體全站總量：

| 來源 | 正式已存 | 其餘結果 |
| --- | ---: | --- |
| NOWnews | 527／658 | 122 第三方／範圍排除、6 舊路徑待核對、1 身分未核對、2 原站找不到 |
| 民視 | 842／2,630 | 1,489 pending＋1 inflight、73 署名未核對、13 身分未核對、203 範圍排除、8 原站找不到、1 未支援路徑 |

完整 source snapshot 保存 4,892,544 已知 URL／2,009,930 已存正文、59 個設定 provider、
17 個啟用 provider、682 個未登錄來源鍵，共 741 列；來源鍵不是 741 家不同媒體。
[逐列後續工作清單](../artifacts/data_quality/news_goal_stage01_20261009/source_plan_after_root_repair_v12.csv)
保留 6 個現行 collector、11 個有限集合、670 個小來源待核對、39 個較大來源待核對、
10 個原始媒體映射、4 個平台／社群及 1 個閉站來源。
`all_source_keys_planned=true` 與 `all_sites_live_reviewed=false` 同時保留；清單存在不等於已實作。
v12 使用相同 20:35 清冊，另納入已驗證的民視換行署名來源紀錄，不發出網站請求。

[四寬度正式面板驗收](../artifacts/data_quality/news_goal_stage01_20261009/dashboard_final_root_v11/acceptance.json)
通過 1440／768／390／320 px、741 列分頁與 59 列流量表，沒有 JS 錯誤或來源網站呼叫。
民視卡片明列「固定批次」與「已核對追加 1／1」，已完成追加不再誤列為待排新增；
未核對成長、失敗、條件式 ETA 與未知 ETA 仍獨立。
桌面／手機圖另有實際視覺檢查；只驗收本機 gateway，沒有在此輪重新驗證外部 DDNS HTTPS。

### 整鏈路驗收與剩餘根因

[889 項最終回歸](../artifacts/operations/agent-workflow/runs/news-goal-full-root-final-regres-20261009T123255-703e309f/run.json)
涵蓋 22 個相關測試檔，30.60 秒通過；不是整個 repository suite。
[選定文章驗收器](../scripts/audit_news_selected_articles.py) 升為報告 contract 4，
現在連修改時間也核對原站重解析；只有合法檔案 SHA 不足以證明正文時鐘語意正確。
revision-only 篡改的回歸會拒絕，舊 capture fingerprint 不被回填成新版。

[最後同 owner 窗口](../artifacts/data_quality/news_goal_stage01_20261009/runtime_source_v11_after.json)
於 20:35:21 至 20:38:46 核對 PID 1683691／owner、載入版本與設定、狀態新鮮度、
獨立來源進展、實際來源間隔及耐久收據，九項工程檢查通過。
205.354399 秒資料庫窗口內新增 Global Voices 20、鉅亨 163、民視 2 個正文 head；
HTTP 計數窗口為 214.035520 秒，不能混用分母宣稱每 call 收益或官方最高速。
民視 619 次 HTTP 開始／618 次 200 中有 616 次署名未確認，明列保留，
**HTTP 吞吐量並不是有用正文吞吐量，民視仍需處理根因，沒有宣稱已全部修復**。

[同窗口新鮮收據驗收](../artifacts/data_quality/news_goal_stage01_20261009/recent_source_v11.json)
核對最近 128 個已提交 batch／3,245 個正文版本，0 錯誤；這不是全歷史收據掃描。
PeoPo 在原拒絕期限到期後仍有一次實際 403，真實來源拒絕保留，並非來源取得成功；
CMoney 的原 403、第三方作品與其他未核對端點也沒有被清除。
工程 owner 驗收、可取得集合完整及全來源修復是三個不同判準。

曾遇到的補充證據目錄契約錯誤、修復樣本不在原固定集合、報告版本舊斷言，以及
執行期窗口碰到刻意診斷停機，均保留原 failed run／log，另有後續實證驗收取代；
沒有把失敗刪掉、把測試 fixture 模擬速率當來源上限或以重排數量當已下載。
後續優先修復民視未確認署名的實際版型、驗證舊網址與原始媒體映射，
再依完整清冊核對尚未啟用來源。既有正式服務繼續抓取，全目標與 task 均保持 active。

### 後續根因修復：民視舊站名與南部新聞中心

21:01 再修復一個實際造成未取得的根因，而非增加冷卻或直接改成功：
原署名文法漏掉「民視新聞網／綜合報導」、「民視新聞網報導」與「南部中心」。
[六篇實際原站診斷](../artifacts/data_quality/news_goal_stage01_20261009/ftv_newsroom_syntax_root_diagnosis_v12.json)
有三篇明列這些可見本社 credit；另外三篇缺少本社 credit 或另有撰文署名，仍保留未確認。
沒有從 schema 編輯姓名推測稿件記者，也不將文章中的風險提醒當成作者。

[原始回應離線驗收](../artifacts/data_quality/news_goal_stage01_20261009/ftv_newsroom_v4_offline_acceptance_v12.json)
明列選定三篇；其餘結果完整留在六篇原始診斷，不將選定驗收稱全六篇成功。
[正式三篇 head 驗收](../artifacts/data_quality/news_goal_stage01_20261009/ftv_newsroom_v4_three_durable_v12.json)
另核對真正入庫的 body／raw／file、title、author、publication 及 revision：
2020111W0036 有 406 字、2020428W0123 有 1,370 字、2026403W0333 有 794 字，均通過。
`qaza6565` 原始編輯帳號照來源保存；「本社未具名報導」與 schema 所列姓名／角色不合併成人名。

來源 adapter contract 4 在唯一正式 owner 內單次重排前版缺正文署名結果，
其他來源、已存 head、拒絕期限及第三方範圍不重設；新 owner 自動續抓，沒有增加第二個 collector。
只允許已實證的本社名稱格式；caption／hidden／第二行／第三方 override 的回歸仍拒絕。
[最新 899 項相關回歸](../artifacts/operations/agent-workflow/runs/ftv-v4-full-root-regression-v12-20261009T130118-10adc988/run.json)
在 22 個檔案、30.71 秒通過，這是 889 項後追加 10 個相應語意保護，不是全 repository suite。

### 連續執行與未確認結果，不把 HTTP 當成正文

21:02 至 21:07 的 [v12 執行期窗口](../artifacts/data_quality/news_goal_stage01_20261009/runtime_ftv_v4_v12_after.json)
只有八項檢查通過：民視 162 次請求／162 次署名未確認、零新增正文，
`healthy_sources_make_independent_body_progress=false`。保留 failed run，沒有用單元測試替它改成成功。

21:18 至 21:27 的 [v13 同 owner 窗口](../artifacts/data_quality/news_goal_stage01_20261009/runtime_ftv_v4_v13_after.json)
在 PID 1749838／owner 不變下通過全部九項檢查。
資料庫窗口 521.440627 秒新增民視 20、Global Voices 52、鉅亨 162 個正文 head；
HTTP 計數窗口另為 532.095304 秒，民視仍有 779 次請求開始、760 次署名未確認。
這證明修復後自動抓取有進展，不證明每 call 效率最佳、全部署名已修復或來源極限。
[最近 128 個 batch／2,444 個版本](../artifacts/data_quality/news_goal_stage01_20261009/recent_ftv_v4_v13.json)
有原始檔／body／receipt 實際驗收，0 錯誤；不是全冷庫或全部未來寫入驗收。

21:43 的 [更新完整清冊](../artifacts/data_quality/news_goal_stage01_20261009/providers_after_cna_setup_v14.json)
有 4,892,638 個已知網址、2,012,148 個正文 head，741 個來源鍵／17 個啟用來源。
民視 869 個 head 中包含 868 個原固定集合 head 與 1 個獨立追加 head；
另外有 1,536 署名／來源未核實、13 身分未核實、203 原創／權利範圍排除、8 原站不存在、1 未支援路徑。
這些未取得結果仍保留，不將所有署名未確認一概歸為中央社或來源拒絕。

<a id="cna-authorized-api-setup"></a>
### 中央社原始稿件與授權 API 接入

[三篇民視 2022 原始回應診斷](../artifacts/data_quality/news_goal_stage01_20261009/ftv_2022_structure_diagnosis_v13.json)
都有明列「中央社」轉載 credit，schema 署名是民視社群編輯，不是中央社記者。
已在官方站找到三篇原文候選，保存於
[來源映射清單](../configs/news/original_source_candidates_20261009.json)：

| 民視網址 ID | 官方原文 | 原文可見發布字串 | 民視發布原字串 |
| --- | --- | --- | --- |
| 2022902W0018 | [成都封城／鴻海營運](https://www.cna.com.tw/news/afe/202209020018.aspx) | 2022/9/2 08:17 | 2022-09-02T08:58:22+08:00 |
| 2022902W0052 | [台股早盤](https://www.cna.com.tw/news/afe/202209020031.aspx) | 2022/9/2 09:24；09:52 更新 | 2022-09-02T09:58:17+08:00 |
| 2022902W0184 | [中華電光世代優惠](https://www.cna.com.tw/news/afe/202209020114.aspx) | 2022/9/2 13:32；14:21 更新 | 2022-09-02T15:05:41+08:00 |

原文只核對到可見分鐘，尚未由正式 API 回應驗證原始精度、raw receipt 及正文相等性。
**不從民視轉載時間逆推原文時間，不補造秒數，不用編輯姓名替代原記者**。

使用者已明確回答已有授權，要求接入授權範圍內的 API。此聲明已納入接線設定，
不再重問授權是否存在。尚缺的是本機憑證／實際產品與回應契約，不是再次要求同一許可。
官方 [MCP 接入教學](https://ask.cna.com.tw/ask/onboarding/mcp) 的公開前端文件明列
`https://ask.cna.com.tw/mcp/connect`、Streamable HTTP、Bearer token 與 `CNA_API_KEY`。
網站公開的 OAuth Client ID 不是使用者的 API Key，不能拿來試登入。
若實際授權是另一種中央社 API，應提供其官方文件／端點，不能把 MCP 當成所有方案的同一介面。

已加入 [CNA_API_KEY 欄位](../.env.example)、
[非秘密 API registry](../configs/data_api_credentials.json)、
[CNA 接線設定](../configs/news/cna_authorized_api.json)、
[有界能力檢查 client](../downloader/cna_authorized_api.py) 與
[檢查命令](../scripts/inspect_cna_authorized_api.py)。client 只支援 initialize、initialized 與 tools/list，
不猜 tool 名／schema、不呼叫 tools/call、不把搜尋摘要當正文、不另開常駐 downloader。
可見對話確認不被宣稱為已實測全文 entitlement、配額或歷史版本證明。

接入機制遵守官方 [MCP lifecycle](https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle)
與 [Streamable HTTP](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)：
UTF-8 JSON／SSE、協商 protocol version、私有 session header、最多八頁／512 tools／每回應 1 MiB，
45 秒邏輯期限及單次最多 10 秒網路等待。SSE 在匹配回應到達後立即關閉，
不等待長連線永久 EOF；CR／LF／CRLF、跨 chunk UTF-8 及反射秘密字串有測試。
不跟隨帶憑證轉址、不自動 replay POST、不在拒絕後換端點，不記錄原始錯誤訊息／帳號／session。

將 token **自行填在 `.env` 的 `CNA_API_KEY=`**，不在聊天、命令列、Git 或 log 貼值。
先跑本機檢查，不發送中央社 API 請求：

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.inspect_cna_authorized_api \
  --output artifacts/data_quality/news_goal_stage01_20261009/cna_local_setup.json
```

`--connect` 才做初始化及列工具，且遵守同一個 HTTP owner lock；正式服務仍持 lock 時會明確拒絕重複 owner。
後續實測需要既有有界診斷交接／自動恢復保護，不讓第二個 client 跟正式 service 重複用額度。
即使 initialize／tools/list 成功，也不會直接把整家中央社標成已完成。
[21:41 的本機檢查](../artifacts/data_quality/news_goal_stage01_20261009/cna_authorized_setup_v14.json)
實際是 `credential_missing`、0 HTTP／0 tool calls、connection false；沒有把它叫作接入成功。
實際帳戶配額、每次可取得正文量、分頁範圍、最早歷史與全量 ETA 目前皆未知。

[更新逐來源計畫](../artifacts/data_quality/news_goal_stage01_20261009/source_plan_after_cna_setup_v14.csv)
升為報告 contract 4，中央社獨立列為 `authorized_api_configuration_and_capability_check`；
978 個已知中央社網址／0 個正文是本機 metadata 範圍，不是中央社全站歷史總量。
其餘來源仍是 6 個現行 collector、11 個有限集合、670 個小來源待核對、38 個較大來源待核對、
10 個原始來源映射、4 個平台及 1 個閉站來源；全部 741 列保留，`all_sites_live_reviewed=false`。
網頁只新增配置階段白名單，明列「授權 API 待接線」與未知 ETA；不讀 `.env`、不顯示 key／工具 schema，
也不刪除或重標舊 HTML 佇列的等待結果。

### 中央社接線準備的驗收範圍

[24 檔最終相關回歸](../artifacts/operations/agent-workflow/runs/cna-complete-news-regression-v16-20261009T140243-3ea321a5/run.json)
974 項、32.40 秒全部通過；新增測試保護秘密值、協商版本、session、UTF-8／SSE 早結束、
分頁迴圈、JSON-RPC 身分、回應大小／期限、拒絕／轉址不重送、既有 HTTP owner lock，以及本機缺 key 不發請求。
這是 mock／本機契約驗收，不是官方 API 成功率／速率／配額的實測。

[四寬度面板最終驗收](../artifacts/data_quality/news_goal_stage01_20261009/dashboard_cna_v16/acceptance.json)
於 22:06 通過 1440／768／390／320 px 與完整 741 列來源分頁，0 JS 錯誤／來源網站請求／資源失敗。
中央社用獨立 `authorized_api_setup_only` role 核對，而非放寬啟用 collector 的判準：
`--require-provider` 仍必須啟用；`--require-api-setup-provider` 則必須未啟用全文、尚無配額／連線證據及 ETA。
卡片不再顯示「共用下載器已接線」造成 API 已運轉的誤解，亦不重複標成授權未確認。
桌面／手機有實際視覺檢查；驗收限於本機 gateway，沒有重驗外部 DDNS HTTPS。

面板部署只重啟唯讀 public gateway；新聞正式 service 的 PID 1749838／owner 沒有變。
最初 browser invocation 把 CNA 當成 enabled collector 而失敗的 run 照常保留，
後續改用正確、獨立的 setup role；不是將未工作的 downloader 改成通過。
缺憑證時先保留尚未啟用狀態，等真正資料回應、欄位／原文映射及耐久收據驗收後才升到下載階段。
全新聞 goal／task 仍 active；沒有宣稱原始稿件已下載、全來源修復或理論速度上限。
