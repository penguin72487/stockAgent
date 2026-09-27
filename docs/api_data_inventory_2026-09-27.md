# API 清冊、有界下載器與全球歷史容量驗收

日期：2026-09-27。範圍是既有全來源清冊，以及使用者確認的「全球明細也要，先估容量與配額」。
不是宣稱所有網路資料已列全或所有歷史下載完成。全球影像等大體積回補尚未啟動。

## 交付與證據

- [清冊閱讀說明](../artifacts/data_quality/api_data_inventory_2026-09-27/README.md)
- [API 設定位置與存在狀態](../artifacts/data_quality/api_data_inventory_2026-09-27/api_inventory.csv)
- [全監控來源、實際首末筆、收據與排程](../artifacts/data_quality/api_data_inventory_2026-09-27/data_inventory.csv)
- [逐項公共資料集／年份目錄](../artifacts/data_quality/api_data_inventory_2026-09-27/discovered_public_datasets.csv)
- [官方 TAIFEX 端點細目](../artifacts/data_quality/api_data_inventory_2026-09-27/taifex_endpoints.csv)
- [全球容量、配額、免費 bulk 與歷史起日研究](global_public_data_capacity_2026-09-27.md)

本次安全稽核：42 個 API／憑證來源，26 configured、1 partial、15 missing。
`.env` 有 47 個設定名稱，範本有 52 個；移除範本兩個重複空 slot、補 legacy FinLab 空 slot。
`.env` 機密值未更動，API key、session、key 前後綴與 hash 都不寫入清冊。
FinLab 本機 SDK session 的三個必要欄位可讀；只驗存在，不登入、不刷新 token，也不是 entitlement 證明。

本次匯出 1,772 筆監控登錄、37 個公共來源群組、158 個 TAIFEX 清冊端點。
這些互有彙總／別名，不可相加成獨立 API、特徵或股票數。
1,829 個已校驗的公共目錄項目來自 NOAA 11、Census 1,807、FIRMS 11；皆 `history_downloaded=false`。
精確快照時間與可重跑計數在 bundle 的 `summary.json`；下載仍進行，之後重跑可能不同。

## 新下載器的工作邊界

`downloader/download_keyed_public_catalogs.py` 重用現有 `SharedRateLimiter`、HTTP retry 與 atomic artifact primitives。
它不是第二套金融回補器：既有 FinLab／FinMind／永豐／OpenBB／TAIFEX／三家加密交易所仍由原 owner 處理。
未啟動新聞、新交易所或加密 tick/L2。

| 範圍 | 目前有界取得方式 | 不能代表的範圍 |
| --- | --- | --- |
| NOAA | CDO dataset catalog、每 dataset 官方 mindate/maxdate | 1763 起全球逐站觀測已取得 |
| Census | 全 API metadata discovery | 1,807 個目錄項目內的全部統計值 |
| FIRMS | 各產品 data availability | 全部熱點或衛星影像已取得 |
| Finnhub | 美國證券 identity catalog 已取得 31,116 列；官方精確單跳 CDN 交付 | 全美股歷史行情或當年成分名單 |
| CWA | O-A0001-001 目前 876 站觀測快照（首次實測） | 所有氣象產品與歷史均已補齊 |
| MOENV | AQX_P_432 目前 84 站 AQI 快照（首次實測） | AQX_P_488 歷史月度資料或所有環境資料 |
| BEA | GetDatasetList adapter 已實作 | key 已啟用；目前應用層 code 4，需要註冊信啟用 |
| AirNow | 全域需求已登錄，尚待來源分區／歷史日期／容量計畫 | 已排全史或已有資料 |
| API.Data.gov | 共用憑證入口 | 獨立 dataset 或所有子機關均有相同權限 |

BEA code 4 經安全分類為 `credential_activation_required`；需完成官方註冊郵件啟用。
API 回應會包含 UserID，故錯誤原文不進日誌／面板，只留固定 reason code。
[BEA 官方 API 指南](https://apps.bea.gov/api/_pdf/bea_web_service_api_user_guide.pdf)

預設 `--plan` 零 API；`--execute` 每輪最多 12 requests（含 retry／CDN 交付）、每回應最多 16 MiB。
正式服務保留至少 20 GiB 磁碟、512 MiB 記憶體上限，CPU／I/O 低權重。
CDO、BEA、FIRMS 的官方 quota 計量單位不同；本有界探測預算不是全世界通用的官方上限。
429 與 Retry-After 持久化，單一來源冷卻不阻擋其他來源；沒有七天一律冷卻。
下載後保留去除憑證回顯的 JSON、SHA-256、版本化成功收據及獨立失敗紀錄。
現在 workspace `publish:false`，沒有寫到 packed／materialized 或向遠端同步。

## 面板與完整性修正

- 新 9 個來源列已接既有 `/data-monitor/`，網頁只讀本機投影，不用 API 補顯示。
- 目錄宣稱的最早／最新不填入本機歷史首末筆；CWA 觀測時間與 MOENV 發布時間分開。
- 本輪成功只證明有界目錄／快照工作；catalog 列舉不完整仍 degraded，不宣稱全部歷史完成。
- 庫存取每個 provider 的最近成功收據；更新失敗、429 或局部 `--providers` 執行不清掉既有資料數字。
- 下載器日期收據、實體檔案統計、查詢下界與本機下次排程分欄，不以「查到今日」充當「資料到今日」。
- FinMind 補充來源的兩段 ID 與 Sponsor 的三段 ID 分別對應，保留收據首末日期而非要求日期。
- unknown credential 不再顯示「缺少金鑰」；壞結構只使相關來源 degraded，不讓全站建置失敗。

## 可重現操作

```bash
source scripts/runtime_env.sh
run_fintech_python scripts/audit_data_credentials.py
run_fintech_python -m downloader.download_keyed_public_catalogs --plan
run_fintech_python -m downloader.download_keyed_public_catalogs \
  --execute --max-requests 12 --max-response-mb 16 --min-free-gb 20
run_fintech_python scripts/export_data_acquisition_inventory.py \
  --bundle-dir artifacts/data_quality/api_data_inventory_2026-09-27
sudo bash scripts/install_registered_data_refresh_services.sh keyed-public-catalogs-only
```

timer 每小時執行；metadata TTL 24 小時，CWA／MOENV 快照至下一小時重新取得；休市日照常。
授權／來源錯誤保持非零退出與可見 degraded，timer 仍可下一輪重試；不因服務狀態而清掉成功資料。

## 未完成與驗證界限

全球逐站／細地理／影像完整分區列舉、代表性 Parquet 壓縮實測、全史回補、PIT 訓練可用性及冷庫發布尚未完成。
容量研究的壓縮範圍是情境假設，不是已量得 bytes；全球影像不能裝入當次約 436 GiB 餘量。
缺 key、未啟用 key、免費方案不涵蓋的資料，不能靠重試或改顯示狀態解決。

本次資料下載器、憑證、清冊、監控與 FinLab 回歸共 **373 tests passed**；
另執行完整 `test_public_dashboards.py`，結果 **110 passed、1 項既有失敗**：
`test_day_trade_clock_ends_at_auction_without_overnight_contract_details` 仍要求「沒有成交證據的部位仍顯示未平倉」，
但當前 Git HEAD 的 13:30 文案已是「依收盤價不限容量紙上平倉」。這是既有當沖契約／測試不同步；
本次沒有改當沖頁面、交易行為或這個測試來消除紅燈。
