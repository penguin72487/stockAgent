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
- 原始市價、bid／ask、分鐘 OHLC、未成交委託與其他市場價格維持獨立來源授權判斷。
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
