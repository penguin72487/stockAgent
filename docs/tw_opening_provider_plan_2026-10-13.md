# 台股 09:00 開盤行情：來源、異常與主備方案

查核日：2026-10-13，時間均為 Asia/Taipei。這是行情取得與公開展示規劃，不是投資建議或券商成交驗收。

## 結論

14:33 續作已在四模式清倉後部署；Discord connected／core ready，訊號、委託與成交帳本 SHA 不變。
現有三路仍是永豐共用串流優先、MIS／FinMind 並行補缺；**並未接通新的全市場串流，亦未達成一秒全量 SLA**。
本輪 2,303 檔完整離線對照（5 組交錯順序）保留所有價格／時鐘／旗標與進度；本機呼叫至第一份來源檔保存中位數由 285.95 ms 降至 150.26 ms，同時補上檔案與目錄 fsync。
詳見[現有三路續作與測速](#現有三路續作與測速2026-10-13-1433)及[工程／部署收據](../artifacts/operations/tw_opening_quote_plan_20261013/continuous_facts/acceptance.json)。

目前三路的「全市場 HTTP 快照＋缺口重試」不足以保證一秒內取得開盤輸入。
優先評估 **元大 SPARK 全市場預先訂閱**；其次為 **確認帳號總額度後的富果 WebSocket／企業方案**。
永豐保留交易標的即時行情與既有執行介面；MIS、FinMind 保留為補洞來源，不作一秒目標的必要前置條件。
這是依文件容量與資料語義的候選排序，**不是已證明哪家 09:00 最快／最穩**。

使用者新增要求已納入：受限來源可繼續供內部模型使用，但不可直接對外顯示價格。
公開價格應有獨立來源、展示授權與行情時間；缺少證據時先空白／等待，不改內部帳本或模型輸入。
「付費 API 權限」不等於「公開網站展示權限」。

## 今日異常：已知機制，不猜測封鎖原因

開盤原始 evidence：
[來源事件](../artifacts/live/tw_opening_snapshots/2026-10-13.acquisition.jsonl)。

| 今日時間 | 原始觀察 | 可以推論／不能推論 |
| --- | --- | --- |
| 09:00:00.129 起 | 永豐、MIS、FinMind 同時開始 | 並非依序等待三家；起始官方 scope 2,293，後續加入 10 個標的至 2,303 |
| 09:00:01.133 | 永豐首批收到 500 行，只有 12 筆有效開盤價 | HTTP／SDK 回覆成功不等於正式開盤輸入更新完成；不是證明另外 488 檔都沒有成交 |
| 09:00:01.676 | FinMind 首批有效開盤數 0 | 需分辨資料更新節奏、交易日、開盤欄與正式成交狀態，不能以「有價格數字」直接放行 |
| 09:00:04.773 | 合併有 92 筆正開盤、94 筆已解來源觀察／2,303 | 仍未達既有模型輸入門檻；未知不能冒充官方無首筆成交 |
| 12:39 有界診斷 | MIS 80 檔單次 2,692.566 ms | 原 1.5 秒逾時短於實際來源成本；單檔快不能外推全市場 |
| 13:07 有界重測 | MIS 單檔仍 `RemoteDisconnected` | 對端關閉 HTTP 連線；沒有足夠證據判定 IP 封鎖、WAF 或伺服器故障 |

MIS 原逾時／批次／缺口重試已在共用來源層修正，來源供價尚未恢復；部署與剩餘邊界見
[MIS 修復證據](tw_opening_latency_resilience_2026-10-13.md#1013-午後mis-全部失敗的分層診斷與修正)。
這份報告不把外部供價失敗改寫成「已修好」。

13:07 本輪有限唯讀測試：

| 管道 | 測試邊界 | 本次結果 |
| --- | --- | --- |
| 永豐共用連線 | 一檔 2330；不新增登入 | 140.811 ms；1／1 有效開盤 |
| FinMind | 一次全市場快照；與官方 scope 對齊 | 645.752 ms；2,264／2,303 有效開盤，39 未解 |
| MIS | 一檔 probe；失敗後不送全市場突發 | 1,374.153 ms；`ConnectionError → ProtocolError → RemoteDisconnected` |

[實測收據](../artifacts/operations/tw_opening_provider_plan_20261013/access_probe.json)
保留 Taiwan 時間、請求範圍與延遲。這是單次午盤存取測試，不是 09:00 覆蓋／P95／P99、長期可用率或成交 SLA。
其他需帳號的來源僅查官方文件，未冒稱實測，未購買、註冊、寄信或券商下單。

## 可以取得行情的管道清單

以下「全市場」必須以當日官方交易清單為準，不能固定寫死 2,303，亦不能拿 API 回應行數當分母。
ETF、特別股、創新板等須按實際官方 scope 對照；只選普通股可能漏標的。
費用為本日官網資料，未列開戶、憑證、專線、非展示使用、再散布或另外的交易所費用；需依正式權限／報價確認。

| 管道 | 介面／容量或更新限制 | 費用／權限 | 本專案定位與穩定性證據 |
| --- | --- | --- | --- |
| 官方 MIS | 網站 HTTP 行情；官方歷史介紹描述五秒快照，未找到全市場逐筆開發 API 的 SLA | 網站免費；API 大量取用與對外轉送不能由免費瀏覽推定 | 補洞／交叉核對。今日先能讀回、後被對端關閉；不能當唯一主源。[官方介紹](https://www.twse.com.tw/staticFiles/product/publication/twse60/P8.pdf)、[使用問答](https://www.twse.com.tw/zh/products/information/qa.html) |
| 永豐 Shioaji | 即時訂閱上限 200；行情查詢共 50 次／10 秒；官方明確不允許以反覆 snapshots／ticks／kbars 輪詢替代即時行情 | 券商帳戶／API 權限；查詢流量依成交級距，推播不計流量 | 已有共用連線；適合實際交易標的，不足單獨推播 2,303 檔。不得靠多登入規避限制。[使用限制](https://sinotrade.github.io/zh/tutor/limit/) |
| FinMind 股票快照 | 全市場一次請求；約 10 秒更新 | Sponsor NT$999／月；Sponsor Pro NT$3,330／月；免費／Backer 不含此股票即時權限 | 已有可用權限；補洞／稍晚交叉核對。提高請求額度不會把 10 秒更新變成逐筆推播。[資料說明](https://finmind.github.io/tutor/TaiwanMarket/RealTime/)、[價格與授權](https://finmind.github.io/Pricing/) |
| 富果 REST Snapshot | 按市場整批；每五秒更新 | 開發者或進階方案 | 比逐檔請求省成本，但不足保證一秒目標；不是串流替代品。[Snapshot](https://developer.fugle.tw/docs/data/http-api/snapshot/quotes/) |
| 富果 WebSocket | 免費 5 訂閱／1 連線；開發者 300／2 連線；進階文件列 2,000 訂閱／2 連線，並說每連線最多 2,000；股票×channel 占一訂閱 | 開發者 NT$1,499／月；進階 NT$2,999／月；企業方案另詢價 | 重要候選。**須確認 2 條連線的帳號合計上限**，不能直接宣稱有 4,000 或只有 2,000；依訂閱 ACK／readback 驗證是否容納完整 scope。[定價](https://developer.fugle.tw/docs/pricing/) |
| 富邦 Neo | WebSocket 單連線 300、同時 7 條：最多 2,100 訂閱槽；REST 日內／快照 300 次／分鐘 | 需券商 API 開通；未確認獨立行情月費，不能宣稱免費 | 可作部分備援；預設額度不足 2,303 全量。Speed mode 是性能候選，需確認保留必需欄位後才測；未登入測試。[限制](https://www.fbs.com.tw/TradeAPI/docs/market-data/rate-limit/)、[模式](https://www.fbs.com.tw/TradeAPI/docs/market-data/websocket-api/upgrade-guide/) |
| 元大 SPARK | 單連線 2,000、帳號合計 3,000，最多 10 連線；指定欄位訂閱按股票×欄位計數 | 需帳戶、API 申請與正式權限；官網無資金／交易量門檻，但不能因此推定所有費用為零 | **優先測試全市場候選**：兩條授權連線分片，若 `WatchlistAll` 每檔只占一額度，容量可涵蓋目前 scope。須實測正式開盤欄、delta 重組及 ACK。[限制](https://www.yuanta.com.tw/file-repository/content/sparkapi_docs/1.%E5%89%8D%E8%A8%80/3.%E4%BD%BF%E7%94%A8%E9%99%90%E5%88%B6%E8%AA%AA%E6%98%8E/index.html)、[平台](https://www.yuanta.com.tw/file-repository/content/API/page/index.html) |
| 群益 SKCOM 等 | 官方提供行情／交易 API；SKCOM 涉及 Windows 元件 | 帳戶、API／行情權限；額度、費用待官方 SDK／帳號確認 | 候選但未取得全市場容量、來源時鐘及實測，不列作已達標來源。[官方下載](https://www.capitalfutures.com.tw/zh-tw/downloads) |
| 凱基與其他券商 API | 券商提供 API 不一定等於有全市場逐筆行情訂閱 | 按券商方案確認 | 未確認的容量、價格、來源語義列待查，不以名稱推定可用。[凱基平台](https://www.kgi.com.tw/zh-tw/support-index/trading-platforms-overview/individual-trading-platform7) |
| IBKR TWS／Gateway API | 行情線數由帳號權限決定；桌面與 API 共用，不能以多 API 連線倍增 | 行情價目列 TWSE L1 USD1／月、TPEx L1 TWD15／月；需另外確認 API/off-platform 權限及線數 | 有台灣市場候選，但低單價不等於全市場容量；未登入實測。[費用](https://www.interactivebrokers.com/en/pricing/market-data-pricing.php)、[線數](https://www.interactivebrokers.com/docs/general/market-data-subscriptions/market-data-lines/introduction)、[API 權限](https://www.interactivebrokers.com/docs/general/market-data-subscriptions/tws-data-vs-api-data) |
| 證交所／櫃買中心直接資訊連線 | 官方機器資訊 feed、主備連線；須分別涵蓋 TWSE／TPEx | TWSE 官網非券商傳輸授權費 NT$60,000／月；**不是全包個人方案**，另有資訊費、網路、用途與 TPEx 契約 | 可控制更多傳輸環節的企業級候選；仍需量測，不保證全標的有首筆成交或一秒成交。[TWSE](https://www.twse.com.tw/zh/products/information/real-time.html)、[TPEx](https://www.tpex.org.tw/zh-tw/service/data/product/regulation.html) |
| 國內資訊公司 | 三竹、嘉實／XQ、精誠、時報資訊、奇唯、凱衛、寶碩、CMoney 等；是否提供可購買機器介面須逐一確認 | 企業授權、SDK、專線或專案詢價 | 官方名冊可作完整詢價長名單，但不能把「名列資訊公司」當成「已有全市場低延遲 API」。[名冊](https://www.twse.com.tw/zh/products/information/list.html) |
| LSEG Real-Time／Bloomberg B-PIPE | 企業 push feed／SDK／雲端或專線 | 依股票市場、非展示用途、容量、對外展示權利詢價 | 有企業機器行情產品，需分別確認 TWSE＋TPEx 包含於合約與是否逐筆、是否合併節流。[LSEG](https://www.lseg.com/en/data-analytics/market-data/data-feeds/real-time-full-tick-data)、[B-PIPE](https://professional.bloomberg.com/products/data/enterprise-catalog/real-time-data-feed/) |
| ICE Consolidated Feed／其他國際供應商 | 訂閱推播，容量由帳號／契約決定；ICE 曾公告台灣在地 Shared Farm | 企業報價；在地方案的現行可售範圍與 SLA 待確認 | 企業候選，不把 2022 公告當成 2026 實測。[API](https://developer.theice.com/hc/en-us/articles/200717514-About-the-Consolidated-Feed-API)、[台灣服務公告](https://ir.theice.com/press/news-details/2022/ICE-Launches-Plug-and-Play-Connectivity-to-Managed-Data-Feed-in-Taiwan/default.aspx) |
| 桌面報價／DDE | GUI／Windows session／DDE 才能送資料；能力與掛牌來源各異 | 桌面軟體與對外使用權限須確認 | 可為備援候選，重開機／登出／GUI 故障面較大，不作首選；未量測 |
| Yahoo／Google／TradingView／新聞看盤頁 | 可顯示行情，不等於供全市場機器抓取的授權 API；可能即時或延遲，依產品而異 | 顯示、取用、再散布權利不同 | 不將未公開授權的網頁端點或爬蟲當一秒主源，亦不籠統宣稱各頁全部延遲 |
| 本機分鐘 K、TEJ／FinLab 歷史、官方日資料／OpenAPI | 保存歷史、參考檔與已完成分鐘 | 各資料授權 | 有助盤前準備／盤後驗證／明確回放，但不能用未來完成分鐘 K 冒充 09:00 當時即時觀察 |

完整官方長名單已直接讀回：[簽約資訊公司收據](../artifacts/operations/tw_opening_provider_plan_20261013/official_vendor_roster.json)，
13:15:15 有 23 家國內資訊公司、25 家國外資訊公司，另列 9 家即時指數與 20 家延遲資訊。
這些是分組列數，不是去重後 77 個逐筆 API；電視台／資訊展示商也在名冊內。
國際長名單包括 LSEG／Refinitiv、Quick、Bloomberg、ICE、SIX、FactSet、Morningstar、Pico、TNS 等。
**私有方案無公開規格的部分保留待確認，不宣稱所有廠商已測試或永不失敗。**

### 多來源不是同等獨立

富果由時報資訊與 Fugle 團隊提供；富邦證券行情也使用時報資訊／群馥，並由 Fugle 團隊開發。
因此富邦＋富果可提供不同入口，但不算完全獨立的供應商雙備援。
不同供應商最後仍可能共用同一交易所、網路節點或資料分發平台；要查中間故障面。
[富果來源](https://developer.fugle.tw/docs/data/intro/)、[富邦來源](https://www.fbs.com.tw/TradeAPI/docs/market-data/intro/)。

## 第一性原理：要縮短的是哪一段

```text
交易所正式首筆成交
  → 資料發布／供應商轉送 → 本機收到 → 驗證與合併
  → 原模型輸入條件達成 → 推論／持久發布
  → 執行介面受理 → 交易所撮合／券商 StockDeal
```

「09:00 到完成」包含首筆成交何時產生；「資料發布到本機」則是另一個可比較的傳輸延遲。
開盤延後、停牌、未成交時，正式開盤價還不存在，任何 API 都不能提供不存在的事實。
試撮價、昨收／參考價、bid／ask 中點也不是正式開盤成交價。
重連後收到的第一筆普通成交，不一定是該股票今天第一筆，需明確開盤欄／旗標或可追溯補缺。

定期快照還有更新等待：若更新週期是 Δ，理想化任意相位可能多等約 0～Δ；均勻相位下均值 Δ／2。
這是機制分析，不是這次實測的延遲分布。增加並行只能減少本地排隊，不能把來源的 10 秒更新變成逐筆。
一秒目標必須以 **盤前已建立的正式串流**為主，而不是 09:00 才登入、下載合約或掃描全部檔案。

## 分階段方案與採購／測試順序

| 階段 | 做法 | 完成證據／邊界 |
| --- | --- | --- |
| 現有權限 | 保持共用永豐連線，成功事實不丟失；MIS 限缺口重試／來源局部退避；FinMind 按來源節奏補缺 | 三路仍有價值，但不承諾全市場一秒；避免官方禁止的 snapshot 輪詢與反覆登入 |
| 優先帳戶候選 | 元大 SPARK：盤前兩條合法連線分片；優先完整 quote table，不把每欄重複訂閱放大額度 | 要先取得 API 權限、SDK／.NET8 相容性、正式訂閱 ACK 與已訂閱清單；不是尚無憑證時冒稱已接通 |
| 訂閱式候選 | 富果進階先確認跨連線帳號總額度；不足則詢價企業方案 | 一檔一個 `aggregates` channel 有開盤欄、時間、買賣盤與試撮／延開訊號，需測推送更新方式及容量；不可用多帳號繞限制 |
| 強主備 | 一個有全量額度的主推播＋不同供應商的全量備援；持續並行接收、先達原輸入條件者釋放模型 | 富邦＋富果不是完全獨立；逐筆、訂閱容量、來源時鐘、缺序補發與權限均須驗證 |
| 企業極低延遲 | 比較 TWSE＋TPEx 直接 feed、國內 licensed feed 與國際在地 feed | 依正式報價／非展示使用與公開展示授權／09:00 P99 決定，不能先宣稱最貴最快 |

候選串流接入時，沿現有共用 collector／fan-in 擴充，不另建一套模型／帳本：

- 08:30 前準備當日官方 universe、資格、除權息／參考檔版本、price grid、模型與資金，並完成登入與訂閱。
- 訂閱容量至少覆蓋整個官方 scope，保留合理成長餘量；成交與五檔是否各占額度按實際帳號驗證。
- callback 僅解析／更新本機事實／喚醒 consumer，不同步做網路、模型、日誌逐筆 fsync；模型訊號與帳本仍保留原持久性、冪等性與因果契約。
- 保存股票別正式開盤事實、source event clock、received clock、序號、試撮／延開／停牌狀態；價格與成交量單位不可混同張、股、口。
- 分片 delta 來源先重組欄位，不能把舊欄位跟新時鐘拼成假的完整快照。
- 多源衝突保留原值與證據，不平均；來源慢／失敗不取消其他來源已成功的結果。
- 斷線重連要完成 auth／訂閱 ACK／清單讀回與缺序補洞；socket connected 不等於資料完整。權限／429／對端拒絕做該來源退避，不讓全系統進入重試風暴。
- 照既有模型的價格／mask／quorum 契約釋放推論，不為達標偷減分母、拿中點替代正式開盤、改模型或補造成交。

## 09:00 驗收與持續測速

至少跨 10 個真實交易日測試，先 shadow／只讀，不新增真實委託；測試環境不能替代正式開盤節點性能。
每檔記錄交易所事件、來源發布、收到、驗證，另記 quorum、推論、原子發布、執行受理、真實成交回報。
本機耗時用 monotonic clock；跨主機／交易所時鐘誤差要單獨量測，不把兩個不同邊界相減冒充 CPU 耗時。

驗收表至少含：

- 每源與合併 09:00＋100／250／500／1,000／2,000／5,000 ms 的正開盤數、已解狀態數與未知數；同一官方 scope。
- 可取得報價的 source-to-receive／quorum／inference／publication P50、P95、P99；失敗與超時也納入，不只統計成功樣本。
- 確認已正式成交的標的覆蓋，並把延開／停牌／未成交與未知分開，不能把 provider 缺價全部叫做市場沒成交。
- 訂閱 ACK 完整率、丟序／補缺、斷線恢復、權限到期、重試量、CPU／記憶體、時鐘偏移。
- 若要宣稱一秒「送出／受理」，只對輸入已足夠且可下單的原模型目標統計；「全市場都有開盤價」和「所有券商成交」不是相同 SLA。

目前未取得新來源的實測與正式帳號／價格確認，不宣稱毫秒極限或一秒已達成。

## 公開展示邊界：模型與網站分離

FinMind 本日條款明列：即使 Sponsor Pro 也不能再散布原始資料／建立鏡像 API，亦不能把即時資料直接顯示於對外 web／App。
因此不能把購買 Pro 當公開面板行情展示的解法；其他供應商的 API／非展示／再散布權限也分別確認。
[FinMind 授權](https://finmind.github.io/Pricing/)。

本輪共用實作：

- [公開價格政策](../stockagent/live/public_quote_display.py)與[設定](../configs/public_quote_display.json)：預設無已確認的公開來源，價格先不傳送。
- [公開投影](../stockagent/live/public_dashboards.py)：當沖／隔日沖的 status、signals、positions、events，巢狀 signal position 及 TAIFEX status 一起處理，不只用 CSS 隱藏。
- 保留訊號、股數、執行狀態、既有損益與分鐘曲線；來源原值不被修改，內部模型／帳本不讀公開替代行情。
- 價格待補不能當作零名目金額，也不能讓已平倉顯示成持倉中。

將來授權來源以 canonical collector 的 **獨立公開價格觀察**補值，不能把受限價格重新貼來源標籤。
`public_price_observations[field]` 必須含 `value`、`price_role`、`symbol`、`venue`、含時區的 `observed_at`、`source_id`、`grant_id`。
每個 source grant 在設定內有 `source_id`、`grant_id`、`public_label`、`license_reference`、`public_display=true`、
ISO `valid_from`／`valid_until`、`venues`；只有本機可信設定能授權，payload 的 `public_display_authorized=true` 無效。

公開來源的角色／股票／交易日／venue／時間／價格均要匹配；來源與時間一同顯示。
記錄成交／進出場／委託／計價價格不能被另一筆不同價格替換，只有公開觀察驗證相同值才揭露；
另源的現價只是公開行情，不改內部估值與損益。
授權到期或政策變更須重新載入公開讀取服務、清除舊編碼快取；不要重啟交易引擎或登入券商。
這是技術揭露邊界，不代替供應商確認模型／商業／非展示用途的許可。

本輪部署與驗收收據：
[公開價格邊界驗收](../artifacts/operations/public_quote_display_boundary_20261013/acceptance.json)。
分析任務與公開展示修改分開記錄；完成這兩者不等於 MIS 已恢復、全部上游健康或新的 09:00 SLA 已通過。

### 本輪工程驗收

- 181 個公開投影／價格政策測試、87 個頁面／歷史／生命週期測試通過；47 個 Node 共用元件測試通過。這不是全庫測試。
- [API 回讀](../artifacts/operations/public_quote_display_boundary_20261013/api_readback.json)：12 個本機出口與 DDNS IPv4 HTTPS 訊號頁均未傳出受限價格；包含巢狀持倉、事件及歷史。DDNS 測試由本機發起，不代表獨立外網或 IPv6 驗收。
- 真實私有訊號／持倉 DTO 比較：原值未被 sanitizer 修改，股數及損益保留。每 2 筆資料、10 次局部序列化樣本的中位數分別為 0.839 ms／0.171 ms；不是全市場或開盤端到端延遲。
- [瀏覽器回讀](../artifacts/operations/public_quote_display_boundary_20261013/browser/readback.json)：當沖、隔日沖與 TAIFEX 各測 1366×768／390×844，6 組均無 JavaScript 錯誤與 body 橫向溢出；查看價格待補與原有狀態，不把 degraded／blocked 改成正常。使用明確 `cpu-2d` profile，不宣稱 GPU／WebGL 驗收。
- 手機版原先把新提示當一般副標隱藏；已改共用獨立 disclosure 樣式並加回歸測試。CSP 保持不允許 `unsafe-eval`；瀏覽器測試用實際函式與正常載入／繪製完成條件。
- [部署回讀](../artifacts/operations/public_quote_display_boundary_20261013/runtime_after.json)：只重新載入公開 reader；當沖、隔日沖、Discord invocation 不變，未新增券商登入或委託、未修復／重寫帳本。當前公開價格授權設定為空，**新公開行情來源尚未接通**。

## 現有三路續作與測速：2026-10-13 14:33

### 可以立即運作的方案

盤前沿用官方當日名冊、資格／參考檔、共享永豐登入、合約與訂閱準備；
09:00 三來源沿同一 fan-in 並行，正式來源觀察先累積，達原模型門檻後持久保存再推論，慢來源不阻擋已成功來源。
缺口、正式零成交觀察與未知分開；剩餘缺口繼續由原來源 owner 修復，不能變成假的價格、假的現金持倉或新的模型權重。

| 現有來源 | 正確用途 | 實際限制／目前狀態 |
| --- | --- | --- |
| 永豐共用連線 | 盤前訂閱、原生正式 quote 優先；不另外登入 | 推播額度 200，不足整個名冊；Snapshot 是查詢型來源，不得用高頻輪詢替代全市場推播。本輪沒有提高查詢頻率 |
| MIS | 官方公開欄與明確 no-first-print 的補缺／核對 | 14:33 單檔 probe 仍被關閉連線；只對這個來源判失敗，診斷不再送全市場突發 |
| FinMind | 現有權限一次全市場快照，按來源更新節奏補缺 | 約 10 秒更新；提高本機並行度不會改變更新頻率，且原始即時價仍不對外展示 |

200 訂閱與每 10 秒 50 行情查詢的限制／合理使用規範：
[永豐官方文件](https://sinotrade.github.io/zh/tutor/limit/)。
全市場快照與約 10 秒更新：[FinMind 官方文件](https://finmind.github.io/tutor/TaiwanMarket/RealTime/)。
若要「已發布的全部標的開盤價」穩定次秒接收，仍要另取得完整官方 scope 容量的合法串流；
延開／停牌／尚未首筆成交的股票沒有正式開盤價，不能承諾在 09:00 取得不存在的值。

### 共用實作修正

- 正式開盤事實與最新 book 分開保存；後續試撮、缺欄、輪轉、同日重連不再擦掉開盤證據。保留原接收／市場時鐘；新連線不會把舊 book 變成新鮮可成交報價。真正的開盤價變更仍被觀察，canonical cache 保留衝突，不平均價格。
- Snapshot 原生 callback 仍只交付結果，解析在 worker；每列解碼一次，累積進度與最終輸出共用解碼結果。亂序批次仍保持原批次優先次序，失敗／超時／late callback fence 不移除。
- 正式新來源欄位、接收時間或請求範圍更新仍保存；完全無變更的重試不再重寫整個來源檔。
- 修正原來源檔僅 atomic rename 的斷電缺口：沿用 `downloader.artifact_io`，成功回傳前完成 file fsync 與 parent directory fsync。同步失敗保持 dirty、拒絕成功宣告並可重試。
- `np.float64` 直接轉為同位元 Python float，避免逐格字串來回轉換；bool、float32、Decimal 與其他來源型別仍維持既有語義。以極小值、最大有限值、隨機值與 float32 反例檢驗。
- 保存解碼、來源診斷、快取合併、來源總耗時等巢狀測速；新增 100／250／500／1,000／2,000／5,000 ms 覆蓋稽核。今天早上只有舊聚合日誌、逐檔 schema 12:53 才開始，輸出 `insufficient_evidence` 與 null，**不會把缺少逐檔證據寫成零報價**。
- 正常 `--runtime` 不掃描 5.2 GB 原始帳本；深度重載比對必須明確指定 `--ledger-fingerprints`。

本輪沿共用取價／collector 做修改，不另建模型、帳本或登入流程；原模型 ABI、官方分母、quorum、資金與成交政策保持不變。
內部取價與公開展示分離的政策持續有效。

### 完整軸離線對照

[測速與逐組 SHA](../artifacts/operations/tw_opening_quote_plan_20261013/continuous_facts/comparison.json)
載入實際修改前的程式副本，不以自造慢路徑充當 baseline。
2,303 檔、5 組前後交錯順序，每組全部 batch 都完成；SDK／來源為明確 synthetic、零網路、零生產帳本寫入。

| 中位數／工作量 | 修改前 | 修改後 |
| --- | ---: | ---: |
| 本機呼叫到最終 source DTO | 175.84 ms | 71.72 ms |
| 本機呼叫到第一份來源檔保存 | 285.95 ms | 150.26 ms |
| 5 組相同進度＋既有 scope seed，共 10 次 | 960.20 ms | 161.30 ms |
| 每輪原始 row 解碼次數 | 9,606 | 2,303 |
| 無變更來源檔重寫 | 10 | 0 |
| 首份來源檔 file／directory 同步 | 0／0 | 1／1 |

所有價格、量欄、時間戳、trial flag、每批進度、來源識別與保存 row 值 SHA 完全一致。
before 原本沒有 fsync，after 的測速已包含新增持久性成本；不把減少同步假裝成加速。
這不是市場傳輸延遲、推論／委託／券商成交耗時、P99、理論極限或明日 09:00 SLA。

### 14:33 真實來源／部署驗收

[唯讀存取證據](../artifacts/operations/tw_opening_quote_plan_20261013/continuous_facts/access_readback.json)：
使用同一官方 2,303 軸與全新空暫存，正常現有共用 broker，不新增測試登入、不發布訊號、不送券商委託。

- 單檔 MIS probe 失敗隔離後，兩個健康來源的 fan-in 於 1,143.04 ms 回傳原 quorum，完成於 1,569.87 ms；此計時不含前置 MIS probe、模型或委託。最終有 2,276 個正式開盤值、27 未知、0 值衝突，不冒稱 100%。
- 永豐 2,302 來源回應，2,276 有效；其餘 26 市場時間陳舊／開盤前、1 無觀察。沒有證據把 27 檔一概稱為「當日沒成交」。Snapshot 的 2,302 筆 trial flag 仍 unknown，不升格成可成交 book。
- FinMind 2,275 有效、28 無觀察。MIS probe 仍 `ConnectionError → ProtocolError → RemoteDisconnected`；沒有足夠證據宣稱封鎖原因。
- IPC 明確讀回 `single_decode_per_callback_row_v1`、`formal_open_separate_from_latest_book_v1` 與 Taipei wall-clock normalization，證明不是僅改磁碟檔、未載入服務。
- 盤後這次 IPC queue 442.36 ms、永豐 server provider 875.07 ms；這是盤後一般 polling 邊界，不能當成 09:00 hot gate 的排隊量。其他來源與原 quorum 仍先完成。
- 285 個 Python 來源、恢復、持久性、排程、benchmark 與期貨回歸通過；不宣稱全庫測試。
- 14:31:44 通過既有四模式全清倉 gate 後，重新載入 Discord／當沖共用取價 owner；14:33 Discord connected、core ready、ledger integrity ready／divergence 0。正常休市／盤後 waiting 保留，不改成 ready。
- [重載前](../artifacts/operations/tw_opening_quote_plan_20261013/continuous_facts/runtime_before.json)與[重載後](../artifacts/operations/tw_opening_quote_plan_20261013/continuous_facts/runtime_after.json)三份 signals／orders／fills 的 SHA 與大小完全相同；未重算／替換帳本、未改當日原訊號或 broker fill 語義。
- 公開當沖 status／頁面依現有 reader 回讀 HTTP 200；只證明可連線，不能代替明日來源完整性。前一節限制價格隔離仍保留。

重測入口（不推論、不交易）：

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_tw_opening_latency.py \
  --session-date 2026-10-14 --runtime \
  --output artifacts/operations/tw_opening_20261014_audit.json
```

明日須用新日誌驗收當日官方 scope 與真實 09:00 分位數／覆蓋；目前多交易日 SLA、MIS 外部恢復與完整市場推播權限仍未完成。
