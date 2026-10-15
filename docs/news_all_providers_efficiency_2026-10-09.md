# 全來源新聞抓取：清冊、根因修復與限量速率實測

2026-10-09，時間以 Asia/Taipei 解讀。工程任務：`news-all-providers-efficiency-20261009`。
Git 基線 `13e5801bc6e8122da72b9c3723f5c3b9e1af83c6`；保留既有 dirty work，沒有改動金鑰或其他下載服務。

本文件保留較早固定槽／短窗測試。後續已改為逐來源自適應排程、65 個鉅亨分類、Fed PDF 與舊版解析：
目前正式驗收與未完成清冊請看[新聞自適應容量報告](news_adaptive_capacity_2026-10-09.md)，不將下列舊快照當作當前設定。
後續新增 44 個來源設定、全來源清冊、CPU 回放、鉅亨端點隔離及 Fed v7 正文修復，請看[全來源修復報告](news_all_source_repair_2026-10-09.md)。

## 結果與仍未解的邊界

沿用同一個 `NewsQueue`／新聞 owner／共享限流器／不可變 Parquet／fsync 收據，不另開爬蟲資料庫。
正式服務已恢復。鉅亨採用每次開始間隔 0.125 秒、3 個回應槽；Fed 0.25 秒、2 個槽。
Global Voices 仍為每 10 秒一次；CMoney 的 403 維持來源退避。每個 provider 的限制獨立，
同一 provider 的子網域共用限制，不能藉多開主機／換身分／多子網域繞過。

這是**本次最高已測試的健康客戶端設定**，不是已找到官方上限，也不是持續最高吞吐的保證。
鉅亨與 Fed 未找到可引用的官方數字限額；實際速率受回應延遲、解析、去重及落盤影響。
沒有宣稱全部媒體、全部年份或全部正文已完成，也沒有自動把新聞注入訓練或輸出到遠端。

## 清冊與文章範圍

02:30:16 的一致 WAL 快照：已設定 **11** 個 provider，其中 **4** 個啟用；另有 **830** 個未註冊來源鍵。
總計 3,259,958 個已知 URL，251,786 個 URL 有已保存正文。
未註冊鍵是 hostname，不等於 830 家不同媒體：包含 Google News 轉址、聚合網站、同出版社子網域。
完整清冊逐列保留 host、來源用途、設定間隔、並行數、狀態、錯誤、正文數、metadata 首末日與抓取時間：

- [CSV 清冊](../artifacts/data_quality/news_all_providers_efficiency_20261009/final-inventory.csv)
- [清冊與原始狀態證據](../artifacts/data_quality/news_all_providers_efficiency_20261009/final-inventory.json)
- [修復前快照](../artifacts/data_quality/news_all_providers_efficiency_20261009/baseline.json)

| 來源 | 已知 URL | 已保存正文 | 狀態與文章取得方式 |
| --- | ---: | ---: | --- |
| Anue 鉅亨 | 669,465 | 248,064 | 公開 JSON `content` 批次優先；每頁實測最多 30 筆；閉合日期窗回補、最近頁追新、殘缺 URL 才走 HTML |
| Fed Board | 7,834 | 2,672 | RSS 摘要只供索引；年度新聞稿／演講／證詞清單提供正文連結；已接入包含 1996 年的舊索引 |
| Global Voices／全球之聲 | 108,948 | 1,050 | 已接入有 `dc:creator`、`pubDate`、`content:encoded` 的公開全文 RSS；本機目前出現 No route to host |
| CMoney | 271,207 | 0 | 一次公開頁測試回 403；未硬撞、未用受 robots 限制的正文捷徑 |
| 聯合／經濟日報 | 169,179 | 0 | 授權未清；不批量探測全文速率 |
| MoneyDJ | 48,031 | 0 | 官方條款明列未授權自動化下載限制；未啟用 |
| 中時／工商時報 | 107,981 | 0 | 批量自動化取得授權未清；未啟用 |
| Yahoo | 202,348 | 0 | 自動化取得及轉載媒體權利未清；未啟用 |
| LINE TODAY | 38,739 | 0 | 官方條款限制 crawler／保存；未啟用 |
| 富聯網 | 101,904 | 0 | 儲存／複製授權未清；未啟用 |
| 科技新報 | 8,133 | 0 | 條款頁目前無法核實；不能把無法讀取當成已准許或已禁止 |

「待下載」與「沒有正文」不同：未啟用的 URL 保留 `awaiting_authorization`，不是完成，也不是已排好可送出工作。
此快照鉅亨 421,400 個未保存正文的可排程工作、492 個已有正文但待重查的工作，兩者分開計數。
相同 URL 的股票關聯、body revisions、desktop/print aliases 也不直接等同不重複文章數。

清冊的 `earliest_metadata_day`／`latest_metadata_day` 是排序用 metadata，不是可證明的歷史發布上下界。
例如 Fed 的舊 metadata 出現 `1899-12-30` 占位值，**不能**宣稱有 1899 年新聞。
鉅亨本機 metadata 最早 2010-01-17、GV 2004-10-27，只代表本機已知線索，仍非來源最早完整可得年份。
Fed 官方舊版 1996-12-27 頁面實測可解析，原始 `Release Date` 只有日期，不捏造日內時間；
頁尾 1996-12-31 更新時間不是發布時間。當前取得的修訂正文仍不是歷史 PIT 版本。

## 第一性原理：什麼才是有效吞吐

目標量是「新 URL 的正文經驗證並不可變保存」，不是 HTTP 200、執行緒數或下載器 active。
有效輸出受到五個階段限制：已授權且存在的來源 → 實際送出間隔 → 網路回應 → 正文／署名／日期驗證 → 去重及耐久提交。
因此先減少每篇所需請求，再消除跨來源等待，最後才增加回應槽；任何來源拒絕都要縮回其 own bucket。

| 根因 | 修正 | 驗證與限制 |
| --- | --- | --- |
| 全域每輪額度用完就排空所有 future，快來源被慢來源卡住 | 常駐 owner 續補工作額度；維護／metadata 更新不再成為全域排空柵欄 | slow/fast fixture 證明快來源 6 篇先於慢來源完成；stop／duration 仍排空並保存結果 |
| 4 個全域 worker 少於啟用來源的槽總和 6 | 7 個 bounded workers，為 3+2+1+1 槽留容量；設定不足立即拒絕 | provider 彼此不搶掉全部 worker；不等於源站允許更多速率 |
| 每輪每來源只補一個任務便睡等 response，保留槽可能一直欠填 | 只有本輪新提交工作時零等待再補；沒有新提交就阻塞等待 | 不是忙輪詢；回應數、worker cap、robots 及單一 writer 都保留 |
| 啟用新 RSS 時，舊的停用 WordPress 任務被 generic bulk flag 一起啟用 | SQL 在 LIMIT 之前按「kind＋註冊 endpoint」過濾；worker 在網路前再次核對 | 舊 error／job 保存；24 個停用 WP fixture 均沒有網路呼叫，合法 RSS 不被擋在後面 |
| 逐篇 GV HTML 需一篇一請求 | 用發布者 RSS 明確全文欄位批次取得，不把 description 當 body | 先前 live `paged=2` 提供 15 個全文欄位、署名及含時區時鐘；端到端落盤與 hash 用 fixture 驗證，本機目前網路阻塞，未宣稱 15 倍實現 |
| RSS 部分項目缺正文／作者會掉出文章佇列 | 保留合法 URL 走既有 HTML fallback；全部不可用只打開 bulk adapter circuit | 不偽造作者、不接受摘要；有權利覆寫的共享原始回應仍不保存 |
| Fed RSS 只有最近摘要，缺長期歷史 | 實際官方年／archive 索引跟隨，只收符合設定路徑的連結 | 不猜 ID；新路徑契約會讓已成功舊索引重解析一次，403 等退避不重設 |
| Fed 1996 舊文章沒有 `#article` | 經 live 驗證的 `Release Date`＋單一 600-pixel 正文表格解析器 | 不抓整頁、不納入 navigation／Last update；其他舊版型仍列為缺口 |
| 有些頁面 h1 只有圖，標題空導致署名檢查失敗 | 同頁 Open Graph 標題 fallback，署名去空與去重 | fixture 驗證；GV 失敗頁的 live 修復驗收仍受 No route to host 阻塞 |
| 重啟瞬間 active 但仍讀到上一個 owner 的新鮮 heartbeat | 核對 WAL UUID、同 PID 進度、`/proc` start clock 與 owner metric epoch | 修正稽核腳本，不以剛 fork 或 HTTP 200 當正式抓取證明 |

## 速率梯度及多開實測

實測短暫停止**僅新聞**正式 owner／timer，以 exclusive lock 沿用原 queue；加上 systemd 自動恢復保護。
各 provider 同時跑自己的工作；403、429、Retry-After、來源 circuit 都在原共享限制中處理。
每輪包括 pacing、完整 drain、解析、去重、Zstd／fsync／SQLite commit、獨立 batch SHA 核驗。

第一組：每個步階 20 秒的送出窗口，加完整 drain／核驗。以下 request rate 以整段 wall 為分母。

| 輪次 | Cnyes 設定 req/s | 實際 request/s | 新正文 URL/s | Fed 設定 req/s | 實際 request/s | 新正文 URL/s |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 2 | 1.81 | 53.09 | 0.5 | 0.51 | 0.46 |
| 2 | 3 | 2.54 | 59.39 | 1 | 0.96 | 0.88 |
| 3 | 4 | 3.48 | 84.39 | 2 | 1.83 | 1.60 |
| 4 | 6 | 5.44 | 125.79 | 3 | 2.81 | 2.53 |
| 5 | 8 | 5.83 | 142.83 | 4 | 3.13 | 2.75 |
| 6 重複 | 8 | 6.02 | 156.40 | 4 | 3.15 | 2.82 |
| 7 回到控制 | 2 | 1.84 | 40.36 | 0.5 | 0.51 | 0.43 |

第二組在同樣 8／4 req/s 下，回應槽採 **3/2 → 5/3 → 5/3 → 3/2**，workers 容量足夠。
全流程 Cnyes request/s 為 4.76、4.60、3.87、3.28；Fed 為 2.29、2.46、2.78、3.28。
Cnyes 平均 transport 時間也從約 0.214 秒升至 0.303、0.626、0.628 秒。
測到的更多並行沒有一致勝出，**正式仍用 3／2**；不把互相重疊的 wall stage 時間相加。

兩組 Cnyes **948／948** 次、Fed **515／515** 次回 HTTP 200，無 403／429／5xx。
這是 HTTP 回應率，不是每篇正文 100% 成功率：批次會有既有正文、metadata-only、解析／權利缺口。
比較頁面與日期窗不同，duplicate 比例與源站延遲會變，不能當固定工作量的嚴格加速比。
沒有把 `8` 稱作源站飽和點；更高未試，長期持續上限也未驗收。

- [逐級速率與每輪持久化證據](../artifacts/data_quality/news_all_providers_efficiency_20261009/parallel-benchmark.json)
- [同速回應槽 ABBA，含慢／未勝出候選](../artifacts/data_quality/news_all_providers_efficiency_20261009/parallel-capacity-benchmark.json)
- [原鉅亨獨立效率實測](cnyes_news_acquisition_efficiency_2026-10-09.md)

## 來源阻塞、權利及後續工作

### 已啟用但來源未恢復

1. **Global Voices**：本機目前 `ConnectionError → OSError(errno=113, No route to host)`，在拿到 HTTP 回應前失敗。
   `/feed/` 先前 live 回 200，但新的重測沒有回應；不是用摘要補全文能解決，也不能由此推定全球網站永久失效。
   保留短期來源退避、每 URL retry 與 HTML/RSS 恢復；其失敗不凍住 Fed／Cnyes。沒有改路由／代理或換來源身分繞過。
2. **CMoney**：公開頁回 403，source-wide 900 秒冷卻；到期按原排程再試，拒絕時不反覆對 27 萬個 URL 逐一硬撞。
   個人研究用途不等於已獲出版社全文資料庫或訓練授權。

### 7 個設定來源及 830 個未註冊來源鍵

未啟用不是抓取完成。既有 metadata 與錯誤不刪；先保留按數量排序的清冊，取得可自動化抓取的授權／合法公開接口再註冊。
`news.google.com` 的 1,285,019 個轉址線索不能算 1,285,019 篇已取得的原始媒體正文。
本文沒有聲稱逐家核實 830 個鍵的授權；TechNews 的條款也沒有成功讀取。

原始官方證據（2026-10-09 核查；法律用途與 robots／技術可達性分開）：

- [Global Voices attribution policy](https://globalvoices.org/about/global-voices-attribution-policy/)：出版社原創文字 CC-BY-3.0，保留作者／連結；不同權利標示、第三方媒體另處理。
- [Fed disclaimer](https://www.federalreserve.gov/disclaimer.htm)：Board 原創公共領域資料，非 Board 內容、seal／logo 例外。
- [鉅亨條款](https://www.cnyes.com/cnyes_about/cnyes_sos01.html)、[CMoney 條款](https://www.cmoney.tw/member/tos.aspx)：未把私人蒐集目的當正面再發布／訓練許可。
- [UDN 著作權聲明](https://udn.com/page/topic/495)：AI／ML 使用需相應書面同意，第三方文章權利另屬原權利人。
- [MoneyDJ 服務條款](https://www.moneydj.com/z/ABMoneydj/aboutTermsService.djhtm)、[旺旺中時條款](https://member.want-media.com/member-term-of-service.htm)：未授權的自動化／批次取得限制。
- [Yahoo 台灣條款](https://legal.yahoo.com/tw/zh-hant/yahoo/terms/otos/index.html)、[LINE TODAY 條款](https://terms2.line.me/globalnews_TOS_Plus/sp?country=tw&lang=zh-Hant)、[富聯網條款](https://ww2.money-link.com.tw/MemberCenter/MemberTerms.aspx?term=4)：自動化或保存限制需先處理。
- [TechNews 條款頁](https://technews.tw/terms-of-use/)：目前核查未成功，保留待核實。

來源單篇文字不能直接代替第三方授權，也不把付費／登入牆當可修復的解析錯誤。

## 調度、資料品質與驗收

優先到期追新與合法全文批次 → 原已知缺正文 URL → 舊年 archive 索引，歷史索引每 8 篇文章插入一次，
避免一直探索卻沒有抓正文。closed API 日期窗與 unknown-total RSS 頁不同，保留來源原生語義。
RSS 一頁一頁觀察、跨頁重複 IDs 即開 adapter circuit；不先製造十萬頁任務或假總量。
source cutoff、已保存 URL heads、body versions、抓取時鐘／發布時鐘分開。

374 項相鄰回歸測試通過，包含未知來源零呼叫、robots、redirect、429 降速重啟持續、worker 限制、
來源 fallback、停用端點不重啟、RSS raw/body/receipt SHA、舊版 Fed parser 與公開頁唯讀邊界。
最後補上 owner／PID 啟動時鐘邊界案例後，22 項針對性測試再次通過；不把兩輪有重疊的測試數直接相加。
原錯誤指令使用不存在的 `test_public_gateway.py`，保留 failed exit 4；改用實際 `test_public_dashboards.py` 完整重跑通過。
較早 fast/slow 排程測試曾抓到額度欠填錯誤，保留 failed 紀錄，修正後完整重跑。

進度頁四種尺寸 1440／768／390／320px 驗收通過；頁面與控制均唯讀，不會呼叫出版社、輸出正文或金鑰：
[新聞進度頁](https://penguin72487.ddnsgeek.com/news/)。

最終工程驗收使用同一正式 PID／owner 的兩份 WAL 快照、獨立正文核驗及精確設定 fingerprint。
02:30:15–02:34:49 的 274.05 秒正式下載窗口：鉅亨新增 29,560 個正文 URL heads（107.86/s），
Fed 新增 833 個（3.04/s）；HTTP counter 的獨立 276.13 秒窗口為鉅亨 4.14 req/s、Fed 3.02 req/s。
不能把這兩種時鐘混用，也不把測試設定 8／4 當已實現持續 rate。
此窗口同時有一次獨立全根核驗與只讀清冊工作，不是隔離的源站飽和實驗。
來源拒絕／網路失敗沒有拖住兩個健康來源；其自身正文新增仍為 0，沒有偽裝為修復完成。
正式服務在此窗口沒有自動重啟，期末 memory current 約 457 MiB、peak 約 471 MiB；沒有 GPU 工作。
一致快照獨立核驗 251,766 個 body versions，raw／body／Parquet hash 與收據錯誤為 0。
正文核驗是有明確快照時刻的 receipt／Parquet／raw response／body SHA 檢查，**不驗證未來寫入或所有來源歷史**。
公開監控、正文保存、授權、歷史完整性、可訓練 PIT 是不同驗收。

- [正式多來源 runtime 驗收](../artifacts/data_quality/news_all_providers_efficiency_20261009/production-multi-final.json)
- [最終正文核驗](../artifacts/data_quality/news_all_providers_efficiency_20261009/production-body-verification-final.json)
- [頁面驗收與四種尺寸證據](../artifacts/data_quality/news_all_providers_efficiency_20261009/dashboard/acceptance.json)

本次以 repo CSV／JSON／文件及既有網站交付可核對來源；環境沒有 Data 專用原生 Sources receipt 的依賴工具，
未捏造該原生 widget 的渲染／發布成功。

## 重測與維運

```bash
source scripts/runtime_env.sh
run_fintech_python -m scripts.audit_news_providers --output artifacts/data_quality/news-audit/inventory.json
run_fintech_python -m scripts.download_news_fulltext verify --output artifacts/data_quality/news-audit/bodies.json
```

限量 throughput 實驗前停止**僅新聞** owner／timer、設自動恢復保護，確認 exclusive owner 已釋放；
不要與正式 owner 同時跑。`benchmark_news_providers.py` 的 `--maximum-cnyes-rps` 是測試邊界，不是官方配額。

```bash
run_fintech_python -m scripts.benchmark_news_providers --maximum-cnyes-rps 8 --phase-seconds 20 \
  --output artifacts/data_quality/news-audit/ramp.json
run_fintech_python -m scripts.benchmark_news_providers --maximum-cnyes-rps 8 --capacity-compare --phase-seconds 20 \
  --output artifacts/data_quality/news-audit/capacity.json
```

完成後恢復 `stockagent-news-fulltext.service`／`.timer`，以 `audit_news_runtime.py` 核對兩份 same-owner 進度。
403／429／Retry-After 或體系性錯誤在原 limiter／circuit 自行退避；不能清除冷卻、換 UA 或多開突破。
全歷史抓齊、830 個來源鍵的逐家審核、來源最早年份與未知總量仍為原新聞工作流待辦，不由本次工程驗收代替。
