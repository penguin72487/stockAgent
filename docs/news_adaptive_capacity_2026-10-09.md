# 新聞下載：逐來源自適應測速、批次端點與正式驗收

2026-10-09，時間使用 Asia/Taipei。工程任務 `news-adaptive-capacity-20261009`。
沿用原新聞佇列、systemd owner、不可變正文檔與收據；沒有另建資料管線、改金鑰或重啟交易服務。

後續 08:55 的[鉅亨固定頻率重測與 512 秒退避修復](cnyes_rate_retest_2026-10-09.md)
取代下文鉅亨早期容量快照：512 秒是無界暫時故障退避缺陷，並非官方限制。
正式恢復為 0.1 秒／8 槽；完整設定階梯與實際吞吐、三窗口核驗及面板連線邊界見新報告。

## 結果與適用範圍

新版已部署並持續抓取。已設定 11 個新聞 provider，4 個啟用；鉅亨新增到 **65 個實測合格分類**，
Fed 新增歷史 FOMC 索引及原生 PDF 正文下載。完整註冊清冊包含 **82 個端點／路徑命名空間**，
不是 82 個獨立配額，也不是全網端點都已實作。另有 830 個未註冊 hostname 來源鍵保留待核實。

04:08:10 的正式驗收中，PID `2485607`、owner `0bd739e2eaeb4c3c86644bb4970dd871`、
設定／原始碼／心跳／WAL owner 一致；同 owner 的 400.81 秒窗口新增鉅亨 **13,380** 個正文 URL、
Fed **1,221** 個。這是已驗證的新 URL head，不是新聞事件去重數、HTTP 成功數或正文修訂數。
[正式驗收](../artifacts/data_quality/news_adaptive_capacity_20261009/current-owner-acceptance.json)
保留各項判定及不同時計的請求／資料快照。

尚未證明任何未公布配額來源的真正上限，也沒有宣稱全部年份、媒體或文章已下載完成。
進度頁：[新聞下載監控](https://penguin72487.ddnsgeek.com/news/)。

## 清冊與目前分工

[全來源 CSV](../artifacts/data_quality/news_adaptive_capacity_20261009/provider-inventory-final.csv)、
[端點 CSV](../artifacts/data_quality/news_adaptive_capacity_20261009/provider-inventory-final.endpoints.csv)、
[一致資料快照](../artifacts/data_quality/news_adaptive_capacity_20261009/provider-inventory-final.json)
列出啟用狀態、已知／已存 URL、重查與缺正文工作、metadata 首末日、最新保存時間、
請求形狀、間隔／並行數、來源證據及未證明的歷史／配額邊界。
metadata 日期只供本機索引，不能用來證明來源最早年份或歷史發布時鐘。

以下是 04:08:10 驗收快照，正式排程會繼續學習與變動，不是永久官方限額。

| Provider | 已存正文 URL | 可排程但缺正文 | 當時排程與邊界 |
| --- | ---: | ---: | --- |
| 鉅亨 | 675,277 | 326,891 | 批次 JSON 優先；0.125 秒開始間隔、4 槽；當時判定本機管線穩定，非來源最大值 |
| Fed Board | 11,990 | 1,452 | 年／會議索引跟隨、HTML／PDF 分類測速；0.1 秒、6 槽，仍在測速 |
| Global Voices | 1,050 | 107,898 | 全文 RSS 批次及 HTML fallback；robots 10 秒下限、1 槽；當時連線 circuit |
| CMoney | 0 | 271,246 | 原公開正文路徑；2 秒、1 槽；403 拒絕後退避，不繞過 |
| 聯合、MoneyDJ、中時／工商、Yahoo、LINE TODAY、富聯網、科技新報 | 0 | 不計入可送出工作 | 7 個來源未啟用，保留 `awaiting_authorization`，不是抓完或來源錯誤 |

可排程缺正文不包含已保存正文的重查；Fed 的解析失敗也不冒充可正常完成的排程工作。
同 URL 的別名、股票關聯與不同正文版本保留來源身分，不直接當作獨立新聞事件。

## 第一性原理：最大化可用輸出，而非請求數

有效吞吐定義為 `新正文 URL 經驗證及耐久入庫 / 完整 owner wall time`。
原始回應、正文、去重、Zstd、fsync、SQLite 提交都保留；增加無正文的 200 或重複抓同頁不能算加速。

排程先提升每個請求的有效內容量，再獨立填滿來源回應槽，最後才提高開始頻率：

1. 每個 publisher 共用一個 bucket，所有子網域／端點合併計量；不同 publisher 可以並行。
   production `http-owner.lock` 防止診斷工具另開限流器疊加同源流量，佇列另有 exclusive writer。
2. 已知官方限制、robots、來源學到的最慢開始間隔是硬下限。沒有官方數字時，使用
   `0.1 → 0.2 → 0.5 → 1 → 2 → 4 → 5 → 8 → 10 → 20 → 30 → 50 → 75 → 100 req/s`
   客戶端階梯；既有健康設定可以作為起點，不必每次重啟從 10 秒重新測起。
   100 是有限客戶端防護上界，不是官方配額。
3. 每窗口至少 45 秒，至少 20 次請求與回應；資料不足可延長至 600 秒。保留實際 req/s、
   新正文/s、每回應新正文量、工作類型、HTTP／transport 錯誤與完整管線等待。
   已送出頻率未填滿時先加一槽，不把目標頻率當成實際送出頻率。
4. `cnyes_bulk`、RSS 全文、WordPress、HTML、PDF 各自保留最佳樣本；混合／metadata-only
   窗口不拿來證明較高正文吞吐。正文修訂不冒充首次取得。三個健康窗口用於穩定性判斷。
5. 429 持久化較慢間隔並服從 Retry-After；401／403／451、200 的驗證／拒絕頁會暫停 publisher。
   重複 transport／5xx 打開 circuit。恢復後先驗證健康窗口，不能直接跳回先前高速。
   429 與 Retry-After 的含義見 [RFC 6585](https://www.rfc-editor.org/info/rfc6585/)。
6. 若提高速率沒有可重複的正文收益，退回同工作類型最佳設定；不為等到 429 而無限增加壓力。
   排程保留當前 frontier、退避和測速 checkpoint，正常中斷／失敗後繼續，不把服務 active 當作下載完成。

實作入口：[容量控制](../downloader/news_fulltext_capacity.py)、[HTTP 共用限制](../downloader/news_fulltext_http.py)、
[常駐排程](../downloader/news_fulltext.py)、[耐久佇列](../downloader/news_fulltext_queue.py)。
worker 上限 16；各來源槽上限為鉅亨 8、Fed 6、GV 1、CMoney 1，彼此預留，不讓單一來源吃掉全部 worker。

## 鉅亨：每次多篇正文、實際分類與歷史範圍

分類來自實際官方導航 bundle，再逐一驗證 JSON pagination／正文結構：75 個候選中 65 個合格，
10 個無效或結構不符沒有啟用；不是猜測 ID 或把未知分類默認為合法。
[導航探測](../artifacts/data_quality/news_adaptive_capacity_20261009/navigation-probes-all.json)
與[已核實分類 manifest](../configs/news/cnyes_categories_20261009.json)保留來源 URL、SHA 和逐分類結果。

公開 JSON 每頁實測最多 30 筆；要求 100／300 沒有增加。可用日期窗超過兩年回
422「指定的時間區間超過 2 年」；2010–2011 的 730 天請求成功，headline 回 30 筆正文與總數 66,296。
[超長範圍的官方回應](../artifacts/data_quality/news_adaptive_capacity_20261009/category-wide-bounds.json)、
[730 天驗證](../artifacts/data_quality/news_adaptive_capacity_20261009/category-two-year-bounds.json)。

歷史從 1999-01-01 搜尋，**1999 是查找下界，不是已證明有新聞的最早年份**。
閉合區間先用最大已測可用的 730 天窗，密度超過可取得頁數時依實際 totals 細分至日；
來源的 `last_page` 可以超過 30，但 page 31 的存取拒絕仍保留。單日超過可取量時保留 gap，
不假設端點支援小時篩選。空來源 `total=0,last_page=0,data=[]` 正確記成空區間而非解析失敗。

已有 monthly／daily 任務、分割子窗及失敗證據都沿用，不建立重疊第二份歷史工作。
同 owner 的日期種子只建一次；dense window supersede 使用分類／日期索引，不反覆掃整個 discovery。
paid／login／不完整正文只保留 metadata 或既有 fallback，不將摘要或付費原始回應存成完整正文。

## Fed：歷史索引、PDF 與舊網頁

新增實際[1936–2020 年度 FOMC 官方索引](https://www.federalreserve.gov/monetarypolicy/fomc_historical_year.htm)，
搭配現行會議日曆，只跟隨來源連結，不猜檔名或發布時鐘。索引含該年材料不等於它在當年已公開。

已註冊 PDF 原先帶 HTML Accept，造成大量 406；改用 `Accept: application/pdf` 後，
三個實際檔案均回 200 且可提取原生文字，包含 1936 年紀錄及 137 頁 Tealbook。
[PDF 內容協商驗證](../artifacts/data_quality/news_adaptive_capacity_20261009/pdf-negotiation.json)。
僅已核實 Board 文件命名空間使用 32 MiB 回應預算；一般 HTML 仍為 8 MiB，沒有放寬所有端點。
原生文字保留原始 PDF／SHA，影像-only 文件明確報缺正文，不以 OCR 猜成完整歷史。

舊版 `555/600/650` release tables 支援多段正文、列表與數字附件，從來源 release clock
界定正文到實際頁尾，排除導覽／footer。無法辨識標題時只在已驗證舊版契約內保留 unknown，
不杜撰標題／日內發布時間。[舊版修正實測](../artifacts/data_quality/news_adaptive_capacity_20261009/corrected-legacy-layout.json)。
Board 原創資料的使用邊界仍遵守[官方 Disclaimer](https://www.federalreserve.gov/disclaimer.htm)中的
第三方內容、標誌等例外，不把整個網站所有媒體都當公有領域。

## 實測結果、驗證與效能邊界

900 秒容量搜尋完整耗時 **938.47 秒**，含 drain／耐久保存與獨立核驗；CPU 658.71 秒、
peak RSS 630,344 KiB。正文版本核驗 65,621 個，其中 65,119 是本窗口新取得正文 URL。
[原始全流程證據](../artifacts/data_quality/news_adaptive_capacity_20261009/live-capacity-search.json)。

最高批次窗口：目標 20 req/s、4 槽，45.26 秒實際 **9.17 req/s**，新正文 **188.60 URL/s**，
每回應約 **20.52 個**新正文。跨窗口的 inflight 可讓 response 數大於 request-start 數，
因此不直接用它推算理論單次容量。這只是該窗口觀察值，不是整段或正式服務持續速度。

目標 30 req/s 後工作曾切換到 HTML，並出現 timeout／500／502，最終退到目標 15 req/s 驗證。
沒有 429；因此「30 被來源限速」和「20 是官方最大值」都沒有證據。
當時端點診斷也有重疊流量，資料重複率／工作類型不同，不能視為隔離公平的因果速度比較。
後來補上 exclusive HTTP owner、同類型樣本比較和來源獨立 fingerprint；
900 秒紀錄保留當時原始碼與設定 SHA，**不是最新 65 分類版本的完整公平 benchmark**。

正式新版的驗收獨立於較早測速：

- 04:08:10 同 PID／owner 窗口獨立新增正文，所有工程檢查通過；仍明確 `all_history_complete=false`。
- 03:56:02 的全庫一致快照核驗 **664,797 正文版本／664,275 complete heads**，無收據／hash／進度表不一致；
  [全庫核驗](../artifacts/data_quality/news_adaptive_capacity_20261009/body-verification-final.json)不涵蓋往後寫入。
- 04:07:59 的[最新 128 批核驗](../artifacts/data_quality/news_adaptive_capacity_20261009/body-verification-recent.json)
  含 **2,585 正文版本**，raw/body/file SHA 與 pinned queue heads 一致；這是有界近期寫入驗收，不是全庫重驗。
- [最終回歸紀錄](../artifacts/operations/agent-workflow/runs/news-adaptive-final-regression-20261008T200848-f98d5251/run.log)：
  **406 passed in 47.10s**；涵蓋限流、來源獨立恢復、workload、PDF、分類、日期窗、queue、面板、readonly 核驗。
- [正式網頁驗收](../artifacts/data_quality/news_adaptive_capacity_20261009/browser-final/acceptance.json)：
  1440／768／390／320 四種寬度，進度計數、全部清冊頁、篩選／排序、零 JS 錯誤與無正文／金鑰洩露通過。

新聞 timer 與公開面板服務已啟用；下載服務 `Restart=on-failure,10s`。
既有 `Nice=15,CPUWeight=10,IOWeight=10,MemoryHigh=1 GiB,MemoryMax=2 GiB` 維持不變，沒有增加 GPU 工作。
這些是資源政策，不是零資源占用或重開機已實測的證明。

## 估時與未完成工作

網頁顯示實際 runtime 間隔／槽、窗口請求率、新正文率、批次能力、狀態與有條件 ETA。
估時分母使用完整 owner wall，分子使用新 URL 正文；無新正文、來源退避或失去 owner 一致性時不顯示成功倒數。
已知 frontier 的剩餘量仍會隨探索增加，不能以 188.60 的最佳窗口推算全網完成時間。
Global Voices／CMoney 無健康正文速率，所以全來源完成時間仍是**未知**。

後續清單保持可查且不改成完成：

- Fed 在 04:08:10 仍有 **232 個 `body_not_found`、3 個影像-only／無正文 PDF**；
  屬於待分類的 parser／來源呈現缺口，不是已證明來源數據錯誤，也不能靠重試相同解析器補齊。
- GV 網路 errno 113／connection circuit 與 CMoney 403 需來源／連線條件恢復；
  自動保存退避、到期再探，不換身分、代理或繞過來源拒絕。
- 未啟用 7 個 provider／830 個來源鍵需來源、權利、正文容器與端點核實，不能將 hostname 當完整媒體清冊。
- 鉅亨最新 page 1 每 300 秒更新；若單分類窗口內超過 30 篇，閉合日期回補可補到，
  但當日 burst 追新還沒證明即時完整。開放當日日期窗／多頁追新需另測來源日期行為。
- 730 天是已測可用範圍；閏年 731 天或不同兩年邊界未實測，不能叫理論最大日期窗。
- PDF 影像 OCR、其他未核實歷史 namespace、未知最早年份與單日頁數截斷仍是獨立缺口。
- config 的長 `note` 尚有前一輪月分區／固定槽描述；執行以正式欄位、manifest 與本次驗收為準。
  後續應先把純展示文字從 source fingerprint 分離，再更新說明，避免改一句 note 就重排全文解析。

個人研究 capture 不代表已取得再散布／商業／ML 授權；`publish=false`、
`remote_training_export=false`、`training_injection_enabled=false` 保持不變。
工程工作可持續改善剩餘缺口；現有來源與未證明上限不得合併成「全部抓完」的結論。
