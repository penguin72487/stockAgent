# 公開價格與使用者模擬交易報告：2026-10-13

本次修正分離「來源授權的原始行情」與「使用者選擇公開的已記錄交易報告」。
先前把進出場價與 bid／ask 一律遮蔽過於粗糙，會使已有模擬成交的帳本看似缺價。
不改交易引擎、模型、帳本原值、股數、時鐘、費用或損益；不新增券商登入或下單。

## 查證範圍

- [永豐證券 API 同意書](https://www.sinotrade.com.tw/newweb/signCenter/S_openAPI/)
  說明 API 交易／資料風險，沒有建立一項可直接證實的全面公網行情轉載許可；
  也不能把期貨條款直接當成證券全部價格不可公開的證明。
- [永豐期貨 API 同意書](https://www.sinotrade.com.tw/newweb/signCenter/F_openApi/)
  的 non-display 與第十條書面授權約定是期貨行情範圍。
- [證交所資訊服務 FAQ 第二項](https://www.twse.com.tw/zh/products/information/qa.html)
  要求取得同意與書面契約後，才可將加值的市場交易資訊傳播或供他人使用。
  沒有從該頁找到所有非商用個人網站的普遍例外。
- [智慧財產局 8704420 號解釋](https://www.tipo.gov.tw/tw/copyright/692-11576.html)
  區分文字手冊與單純指數數字；不能把這份著作權解釋延伸成行情契約豁免。
- [TWSE 日資料](https://data.gov.tw/dataset/11549)、
  [TPEx 日資料](https://data.gov.tw/dataset/11370) 已確認適用
  [政府資料開放授權條款](https://data.gov.tw/license)。授權只擴及識別出的日資料，
  不擴張成永豐分鐘、tick 或即時 bid／ask 全部有公開許可。

本報告是來源查證與軟體分類，不是對使用者個案的法律保證。

## 共用發布機制

- 官方日價格：獨立讀取 canonical 本機 Parquet，依正確事件日期與角色核對。
  Buy-and-Hold 成本使用真正的建倉日；隔日沖退出使用次日官方開盤，而非建倉日收盤。
- 使用者交易報告：伺服器設定啟用 `owner_paper_transaction_report_v1`；
  可信的 positions／signals／fill endpoint 才可公開已記錄的紙上成交值。
  這不是把 Shioaji 添加到已授權行情來源中。
- 進出場保留全精度；顯示用既有格式化，不改 VWAP 為整數或硬湊合法 tick。
- 完整訊號有非零紙上成交且可對應同標的／模式／日期／成本的持倉，才顯示執行價。
  零成交訊號的計價來源不假冒自己的買賣。
- 原始市價、bid／ask、分鐘 OHLC 與其他市場價格維持獨立來源授權判斷。
  10 月 14 日使用者另明確授權公開策略 TP／SL 與已記錄委託，依下方獨立
  owner-strategy contract 發布；未成交委託不是成交報告，也不是原始行情授權。
  nested raw quote 不能繼承 owner-report context。
- 公開原因顯示「公開展示授權待確認」或「公開價格核對未通過」，不把權限不明
  說成沒有取得行情，也不暗示已確認禁止。
- 共用當沖／隔日沖 renderer 明示「模擬帳本成交・非券商成交」；
  原始反事實／回放旗標繼續顯示，來源日行情另列。

## 重測

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q \
  test/test_public_daily_quotes.py test/test_public_quote_display.py \
  test/test_public_dashboards.py test/test_public_price_acceptance.py \
  test/test_dashboard_updates.py
node --test test/test_public_daily_quotes.mjs test/test_tw_day_trade_components.mjs
run_fintech_python scripts/verify_public_price_projection.py \
  --session-date 2026-10-13 --history-date 2026-09-21 \
  --address-family ipv4 --browser-profile cpu-2d \
  --output-dir artifacts/dashboard_acceptance/owner_paper_prices_20261013_release
```

API 驗收直接讀取官方檔與未遮蔽的本機模擬帳本投影，不用新發布策略自己證明自己。
完整分頁的價格、股數、費用、P&L 與交易時間須精確相等；receipt 另存 canonical
source hash 與金融快照 semantic hash。只有持倉封存 envelope 的 `archived_at`
分鐘重寫可單獨列出，不排除任何實際交易時間或金融欄位。

瀏覽器實際測試兩頁各 1366×768、1920×1080、2560×1440、768×1024、390×844。
檢查可見的價格與性質標籤、不可變資源版本、橫向溢位與 page error。
`cpu-2d` 不是 GPU WebGL 驗收，IPv4 成功也不代表 IPv6 已恢復。

最終結果以本次 release 的 `receipt.json` 與 canonical agent run 為準；
資料健康、分鐘來源完整性、真實券商成交與全庫測試仍是獨立驗收。

## 本次部署驗收結果

- 265 個 Python 測試、31 個 Node 測試通過。首次回歸的舊 fixture 與非同步快取
  測試失敗保留在 run 紀錄；修正為 owner-report 開／關兩種情境，以及等待真正
  cache commit 的證據，不刪測試或略過價格來源邊界。
- 今日完整分頁 837 筆持倉：837 筆進場價、837 筆出場價可見；
  1,085 個欄位走 owner paper report，其餘可見價格保有官方日價證據。
  每一筆可見交易價、股數、交易時間、費用與 P&L 均與未遮蔽帳本精確相等。
- 2026-09-21 的首 100 筆歷史持倉也逐筆比對，不代表全歷史已逐頁驗收。
- 兩頁 × 五種 viewport，共 10 組 Chromium `cpu-2d` 驗收通過；
  實際查看筆電價格明細截圖，沒有 page error 或整頁橫向溢位。
- 官方 source 檔案 hash 與所有金融 snapshot semantic hash 不變。
  4 個封存 envelope 的 `archived_at` 由原本的分鐘 writer 正常更新，已單獨列出。
- 僅重啟公開 reader；部署 readback 的當沖引擎 PID 142、Discord PID 43289
  保持不變。當沖與隔日沖資料健康仍為 `degraded`，不是這次價格分類修正已
  完成所有來源與交易驗收。沒有驗證真實券商成交或 IPv6。

完整 [release receipt](../artifacts/dashboard_acceptance/owner_paper_prices_20261013_release/receipt.json)。

## 2026 年 10 月 14 日官方參考資料接線

當沖、隔日沖共用 `official_daily_quote_v2`，直接讀取本機 canonical 官方日資料。
已接出的官方欄位與使用者模擬成交分開呈現，不改模型輸入、帳本、股數、交易時間、
費用與損益。永豐既有內部行情使用不變；原始行情公網展示仍按來源授權判斷。

### 哪些內容可以展示

| 來源 | 本次展示範圍 | 展示依據與限制 |
| --- | --- | --- |
| TWSE 官方日資料 | 開高低收、成交股數、成交金額、成交筆數 | [開放資料 11549](https://data.gov.tw/dataset/11549)；此資料集沒有列全市場最後買賣價，不把 MI_INDEX 的其他欄位自動放行。 |
| TPEx 官方日資料 | 上述日資料、均價、盤後最後買賣價、次日參考價、次日申報上下界 | [開放資料 11370](https://data.gov.tw/dataset/11370) 明列這些欄位；標示原始來源日，不冒充當日市價或 tick。 |
| 使用者模擬交易報告 | 已記錄的進場、退出成交價與交易損益 | 使用者要求公開；保留模擬及回放標籤，不描述為券商成交。 |
| 使用者策略設定 | TP／SL、13:20 與尾盤限價、次日開盤沖銷委託價 | 10 月 14 日使用者明確要求公開；沿用原帳本策略值，標示非行情／非成交，不要求先成交才展示計畫。 |
| 永豐證券與期貨 | 原有模型、資料取得及模擬帳本內部使用 | 證券公網報價轉載許可仍未確認；[期貨 API 同意書](https://www.sinotrade.com.tw/newweb/signCenter/F_openApi/) 另有書面授權條款，不混用兩種商品的約定。 |
| MIS 即時市況 | 原有內部取價不變 | 免費瀏覽不等於上述日資料的開放授權；依 [TWSE FAQ](https://www.twse.com.tw/zh/products/information/qa.html) 與 [使用條款](https://www.twse.com.tw/zh/terms/use.html) 保留公網展示待確認狀態。 |
| FinMind | 原有取得統計及內部取得不變；本次官行情由官來源接出 | [資料授權聲明](https://finmind.github.io/Disclaimer/) 區分原始開放來源與服務權限；歷史或非即時本身不是全面轉載許可。 |
| FinLab、TEJ | 公開取得進度、配額、欄位目錄等既有唯讀統計 | 不公開原始 vendor 資料表。TEJ 的 [E Shop 條款](https://eshop.tej.com.tw/E-Shop/order_terms) 與使用者實際 Smart Wizard 契約須分別核對。 |
| 其他已登記來源 | 既有取得與資料監控不變 | 逐資料集授權核對；匿名 API 能下載與允許轉載是不同條件。 |

完整清冊涵蓋 39 個既有來源群組、47 個來源／查證項目、161 個官方 collector 資料項目。
官方 metadata API 已確認其中 15 個具政府開放資料授權；另 146 個仍是逐項待核對，
不是已確認禁止，也不是全部開放。價格發布目前只啟用表中兩個逐欄位核對的日資料集。
欄位與授權的原始查證見
[來源清冊](../artifacts/operations/public-reference-prices-20261014/source_inventory_final_v2.json)。

### 日期與價格界線

盤中尚無當日盤後資料時，明確顯示前一份已驗證日資料的日期，並註明非當日報價。
例如 10 月 14 日的 6016 參考 10 月 13 日官方資料：盤後最後買 23.4、最後賣 23.45、
次日上界 25.7、下界 21.1。這些欄位不覆蓋當天模擬進場價，也不替代即時 bid／ask。

原始次日參考價缺值就維持缺值，不從上下界倒算。來源的 9,999.95／0.01 等界線
保留原數值，不宣稱所有商品均有 ±10% 漲跌停；參考價、除權息、IPO 與商品規則
需要各自的證據。完整日資料可在兩頁的持倉／訊號中展開，核心成交與損益留在前面。

未記錄的單日訊號查詢回覆 `404 session_unavailable` 與 `data_complete=false`，
不再誤報暫時服務故障。起始日／終止日區間查詢仍依保留交易日回傳；隔日沖
10 月 13 日沒有保留訊號，區間回傳零筆及 `session_dates=[]`，驗收收據另標示
`selected_session_recorded=false`，
不能據此宣稱當日策略完整執行。

### 驗收與效能

- 331 個相關 Python 測試及 35 個 Node 測試通過；不是全 repository 測試宣告。
- 公網當沖與隔日沖在筆電、1080p、2K、平板及手機各五種尺寸驗收；日資料直接
  對比 canonical 原檔，成交報告直接對比未遮蔽帳本，價格、股數、費用、損益與
  金融時鐘完全相等。官方檔案與帳本金融 snapshot hash 保持不變。
- 共 10 組瀏覽器畫面通過；當沖五種尺寸另外實際點擊上櫃官方詳情並驗證展開
  後的盤後買賣價與次日界線。已人工查看展開後的筆電與手機截圖。
- 只更新公開 reader；當沖引擎、Discord、Cloudflare connector 的 PID 未變。
- 新增欄位的初次日資料投影中位數 134.64 ms，原 OHLC 投影為 61.34 ms。
  新投影保留全部原欄位且多提供 22 個無 OHLC、但有官方參考欄位的標的，不以
  刪資料換取較快數字。共享快取後的當日參考查詢中位數為 0.080 ms；首次新
  adapter 與 Arrow 載入約 824 ms。OS 快取未清除，主機非獨佔，不能當作冷儲存測速。
- 公開請求不新增 provider 呼叫，不登入永豐、不下單，也不為修正展示補造價格。

測試、部署與量測原始證據：
[彙總驗收](../artifacts/operations/public-reference-prices-20261014/acceptance.json)、
[部署讀回](../artifacts/operations/public-reference-prices-20261014/deployment.json)、
[初次投影比較](../artifacts/operations/public-reference-prices-20261014/performance.json)、
[快取量測](../artifacts/operations/public-reference-prices-20261014/reference_cache_final.json)。
完整瀏覽器與 API 的
[最終驗收收據](../artifacts/dashboard_acceptance/public_reference_prices_20261014_final_v3/receipt.json)
記錄 10 月 13 日 837 筆持倉的完整分頁逐筆核對；9 月 21 日只核對首 100 筆，
不代表所有歷史日期已驗收。

### 重測指令

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_public_source_display.py \
  --output artifacts/operations/public-source-display/recheck.json
run_fintech_python scripts/verify_public_price_projection.py \
  --url https://penguin72487.dpdns.org \
  --session-date 2026-10-13 --history-date 2026-09-21 \
  --address-family ipv4 --browser-profile cpu-2d \
  --output-dir artifacts/dashboard_acceptance/public-reference-prices-recheck
```

`cpu-2d` 驗證 HTML、SVG、Canvas 2D 及原生互動，不等同 GPU／WebGL 驗收。
網站接線、資料健康、完整分鐘行情、行情轉載授權與真正券商成交仍分別驗收。

## 2026 年 10 月 14 日策略 TP／SL 分類修正

根因是原共用 `*_price` 遮蔽器只對「已成交的使用者交易報告」開例外，沒有
區分策略輸出；TP／SL 因而被錯誤當作未核對的原始行情。改用
`public_quote_display_v2` 與獨立 `owner_paper_strategy_report_v1`，兩頁重用
同一發布層和 `detail-components.js?v=17`，不改模型、訊號、交易帳本或費用。

### 欄位契約

- TP／SL 是帳本中已存在的策略設定，不從官方日價猜算或覆蓋，保留原精度。
- `eod_limit_price`、`closing_auction_limit_price`、`opening_exit_limit_price`
  及訊號 `order_limit_price`、已記錄 LMT／LOCAL_STOP 指令可按策略報告顯示。
  數值與對應交易日／原始 basis 或提交時鐘須有效；開盤沖銷委託使用其自己的
  次日日期，不能拿建倉日配對。策略報告沒有成交量門檻，不能把「未成交」
  等同「沒有策略價格」。
- basis／提交時鐘不冒充回放的實際生成時間。前端明示「策略設定 · 非行情／非成交」，
  成交欄則維持「模擬帳本成交 · 非券商成交」，原回放旗標仍保留。
- 例外只在可信的持倉、訊號及指令集合生效；巢狀行情、原始 OHLC／bid／ask、
  sizing 輸入價與交易所價格界線不會繼承使用者策略設定權限。
- 官方價格仍依命名的資料集、欄位、角色與日期核對。櫃買 11370 明列次日
  申報上下界；不能換標籤當今天漲跌停。上市價格界線的公開規則與直接轉載
  來源產品也不能混為一談：[交易制度](https://www.twse.com.tw/zh/products/system/trading.html)
  與 [漲跌幅資料表的使用範圍](https://eshop.twse.com.tw/zh/product/detail/d701c003b95c4a7497b9ea6447f0fcdd)
  分別說明計算原則與來源產品條件，不據此聲稱所有漲跌停數字都禁止展示。

### 刷新與範圍

公開策略政策 fingerprint 納入當沖、隔日沖 status／detail 的 revision；只有
政策改變時也會更新價格投影。fingerprint 每個載入政策只計算一次，不逐列讀檔
或重新 hash，且不改 `history_revision`、不重算金融分鐘曲線。政策設定仍由
唯讀 reader 重啟載入；不重新登入永豐、不啟動 provider 下載、不重啟交易引擎。

本輪完整來源清冊為 39 群組、48 個登記／查證項目及 161 個官方 collector 項目。
15 個命名資料集的政府授權 metadata 已核對，146 個仍待逐項核對。公開價格
grant 仍只有 TWSE／TPEx 的兩個日資料集；使用者策略／模擬成交是另兩種報告
用途，不因來源可匿名下載、價格為歷史資料或使用者非商用，就擴張成其他原始
行情的公開授權。進度、配額、效能、儲存及已登記欄位清冊沿用既有公網投影。

完整 [本輪來源清冊](../artifacts/operations/public-owner-strategy-prices-20261014/source_inventory.json)
包含 owner-report 可發布欄位、所有來源分類及待核對項目。

### 驗收

361 個相關 Python 測試、38 個 Node 測試通過；不是全庫測試宣告。今日 681 筆
持倉逐頁直接對比未遮蔽的本機帳本，681 個 TP、681 個 SL 及共 1,755 個可見
策略價格欄位與原值完全相等，另驗證進場、退出、股數、費用與損益不變。
9 月 21 日只抽查首 100 筆持倉，不代表全歷史都逐頁查驗。

公網兩頁各五種 viewport（筆電、1080p、2K、平板、手機）的原生互動與價格標籤
通過，共 10 組；實際查看筆電、手機截圖，沒有整頁橫向溢位或 page error。
`cpu-2d` 不代表 GPU／WebGL 驗證；IPv4 的這次價格驗收不重複宣稱雙棧網路已
全面驗收。僅更新公開 reader，當沖、隔日沖、Discord、TAIFEX capture 與
Cloudflare connector 的 PID／InvocationID 在最終部署前後不變。

重測沿用本頁的 `verify_public_price_projection.py`，改用 2026-10-14 的
`--session-date`。它以獨立 raw-file／private-ledger 比對，不反過來呼叫新分類器
作為「證明」；原始失敗與替代驗收仍保留於 agent 工作流程收據。

本次 [公網驗收](../artifacts/dashboard_acceptance/owner_strategy_prices_20261014_final/receipt.json)
及 [部署讀回](../artifacts/operations/public-owner-strategy-prices-20261014/deployment_final.json)
是價格接線證據，不替代策略健康、開盤延遲、行情完整性或券商成交驗收。

## 2026-10-14：價格來源集中頁尾與距進場指標

當沖、隔日沖共用 `detail-components.js` 的價格格式化，不再在進場、出場、TP／SL
或事件價格旁重複來源。整頁最後的「資料來源與計算說明」保留來源角色、行情日期、
實際取得／帳本紀錄時間及去重後完整官方日資料；隔日沖的來源契約也移入頁尾。
公開權限、缺價理由、原始交易／重建時間和反事實標示仍保留，不增加行情公開授權。
重新取資料不重載整份文件，且保留已展開的官方資料明細。

新增唯讀 `public_entry_price_distance_v1`：只有進場與比較價格各自通過公開檢查後，
才產生 `(比較價 - 進場價) / 進場價 × 100`；這是價格變化，不反轉空方符號，
也不是扣費後損益。Tick 距離使用 canonical `tw_price_rules.py` 的商品／交易日級距
跨區間累計，例如股票 999 → 1,005 是 2 tick，不能以末端的 5 元直接相除。
VWAP／均價不改原值、不量化到報價格點，距離標為「約」；未知商品／日期沒有
猜測 tick，未公開的價格也不能透過百分比或 tick 洩漏。

已平倉的清算優先顯示帳本出場均價與出場時間；缺少均價時只顯示末次出場價，
不補造完整出場均價。未平倉仍使用帳本當時清算估值；
這是 renderer 的取欄位規則，沒有改成交、股數、費用、PnL、分鐘曲線或模型。
475 個相關 Python 測試及 43 個 Node 測試通過，包含歷史級距、跨區間、ETF、
正負方向、VWAP、缺價與發布權限；不是全庫測試宣告。

今日 681 筆持倉的 1,456 個距進場欄位通過獨立 Decimal 驗算，原始財務欄位及
兩份官方日資料來源 SHA 不變。公網兩頁各五種 viewport 共 10 組通過，無
page error 或整頁橫向溢位；筆電與手機另以原生更新按鈕驗證展開狀態與不重載。
隔日沖今日沒有持倉，實盤頁面的距離顯示不宣稱已驗證有成交的隔日沖紀錄；
共用 renderer 的隔日沖案例由 Node 測試驗證。僅重啟公開 reader，交易引擎不動。

本輪 [公網驗收與截圖](../artifacts/dashboard_acceptance/tw_price_footer_distance_20261014_release/receipt.json)
及 [部署讀回](../artifacts/operations/tw-price-footer-distance-20261014/deployment.json)
保留實際查驗範圍，沿用 `verify_public_price_projection.py` 可重新測試。
