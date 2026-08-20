# 公開資料 API 憑證位置

所有憑證欄位的 machine-readable owner 是
configs/data_api_credentials.json。這份 registry 只保存欄位名稱、存放位置、
申請網址與用途；禁止保存 secret value。

## 所有下載器（含 OpenBB）

位置：repository 根目錄 .env

    cp .env.example .env
    chmod 600 .env

.env.example 已預留以下欄位：

- 市場：Shioaji、Alpaca、Alpha Vantage、Finnhub、Nasdaq Data Link。
- 總經／政府：BEA、API.Data.gov、Census。
- 天氣／災害／環境：NOAA CDO、CWA、MOENV、NASA FIRMS、AirNow。
- 能源／交通：ENTSO-E、TDX。
- Copernicus：CDS/ERA5、Data Space/Sentinel。
- 農業／貿易／識別：USDA NASS、UN Comtrade、OpenFIGI。
- 健康／開發活動：openFDA、GitHub。
- 加密／鏈上：Etherscan、Dune、CoinGecko、CoinGlass、CoinMarketCap。
- SEC：SEC_USER_AGENT 或 STOCKAGENT_CONTACT_EMAIL。SEC 不需要 API
  key，但 fair-access 要求可識別聯絡資訊。
- OpenBB providers：FRED_API_KEY、BLS_API_KEY、EIA_API_KEY、
  CONGRESS_GOV_API_KEY、FMP_API_KEY、TIINGO_TOKEN、BENZINGA_API_KEY、
  INTRINIO_API_KEY、ECONDB_API_KEY、TRADINGECONOMICS_API_KEY、
  CFTC_APP_TOKEN。

Binance、OKX、Bybit 公開市場資料不需要 key；目前仍只啟用這三家交易所與
一分鐘 K，不因預留其他欄位而自動啟用新來源。

## OpenBB

canonical 位置：repository 根目錄 .env。OpenBB 歸檔器啟動時會把上列大寫
環境變數映射到 OpenBB 的小寫 runtime credential field；已 export 的程序環境
變數優先於 .env。

原始位置是 ~/.openbb_platform/user_settings.json 的 credentials object。現在只
保留為 legacy fallback，不會自動刪除，以免外部 OpenBB CLI 失效。安全遷移：

    source scripts/runtime_env.sh
    run_fintech_python scripts/migrate_openbb_credentials_to_env.py --dry-run
    run_fintech_python scripts/migrate_openbb_credentials_to_env.py

映射如下：

- FRED_API_KEY -> fred_api_key
- BLS_API_KEY -> bls_api_key
- EIA_API_KEY -> eia_api_key
- CONGRESS_GOV_API_KEY -> congress_gov_api_key
- FMP_API_KEY -> fmp_api_key
- TIINGO_TOKEN -> tiingo_token
- BENZINGA_API_KEY -> benzinga_api_key
- INTRINIO_API_KEY -> intrinio_api_key
- ECONDB_API_KEY -> econdb_api_key
- TRADINGECONOMICS_API_KEY -> tradingeconomics_api_key
- CFTC_APP_TOKEN -> cftc_app_token

cftc_app_token 只保留給 OpenBB 相容；stockAgent 現行 CFTC 下載器使用匿名
官方 Public Reporting API，不要求 token。FMP、Benzinga、Intrinio、Trading
Economics 即使填入 key，仍可能受付費 entitlement 限制。

## 稽核與面板

安全檢查：

    source scripts/runtime_env.sh
    run_fintech_python scripts/audit_data_credentials.py

預設輸出 artifacts/data_credentials/status.json，只包含：

- configured / partial / missing
- 變數名稱
- canonical storage location
- requirement 與申請網址
- .env 的 canonical 狀態，以及 OpenBB legacy settings 的備援狀態與檔案權限

輸出不包含任何 key、token、secret 或 password value。registered refresh 每次
執行前都會更新這份 receipt，資料監控面板沿用它顯示憑證閘門。

## 不需要金鑰的主要來源

ECB、BIS、World Bank、OECD、Eurostat、GLEIF、USGS 地震、NWS、
NOAA bulk、NASA POWER、FEMA OpenFEMA、GDELT bulk、Federal Register、
USAspending、FAOSTAT、MOPS、TWSE/TPEx/TAIFEX 公開檔案、台灣政府開放資料
與 ETF 發行商公開檔案通常不需 API key。

「不需 key」不代表無速率、無授權或可任意重散布；下載器仍必須遵守官方
rate-limit、User-Agent、license 與 bulk-download policy。
