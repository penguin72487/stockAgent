# Dune、SEC 與 Crypto ETF 發行商歷史資料

## 啟用範圍

- 交易所行情只由 Binance、OKX、Bybit 的既有一分鐘管線維護。
- `download_free_public_context.py` 不再排程 Hyperliquid、Deribit、Coinbase、Kraken、Bitfinex；舊檔與 adapter 保留供稽核。
- Dune 只執行 `configs/dune_crypto_queries.json` 登錄且 SQL 已版本控制的查詢：DEX 資產活動、Binance／OKX／Bybit 標籤地址流量、穩定幣 mint/burn。
- SEC 清冊涵蓋 `configs/crypto_etf_sources.json` 登錄的美國數位資產 ETP；發行商的機器可讀歷史目前正規化 iShares IBIT／ETHA 與 Bitwise BITB。

## 憑證與合規識別

在 repository 根目錄的 `.env` 設定：

```dotenv
DUNE_API_KEY=...
SEC_USER_AGENT='你的名字 你的組織 your-email@example.com'
```

SEC 不需要 API key，但 SEC fair-access 政策要求可識別的 User-Agent。iShares 與 Bitwise 公開檔案不需要 key。下載器只 allowlist 讀取上述必要環境變數，不會把值寫進 log、Parquet 或面板。
若姓名或組織含中文，下載器會保留 ASCII 聯絡信箱並使用穩定的
`stockAgent research` 識別前綴，避免 HTTP header 在送出前因編碼失敗；聯絡信箱仍為必要欄位。

## 執行

```bash
source scripts/runtime_env.sh

run_fintech_python downloader/download_dune_crypto_history.py \
  --output-dir data_dune_crypto \
  --end-date today \
  --workers 3

run_fintech_python downloader/download_crypto_etf_history.py \
  --output-dir data_crypto_etf \
  --sec-workers 10 \
  --issuer-workers 4 \
  --primary-documents
```

`scripts/run_registered_data_refresh.sh daily` 已啟用兩條管線；intraday 輪次明確停用，避免 Dune credits 與 SEC 全歷史工作每分鐘重跑。不同供應商平行執行，相同供應商共用 process/host-global limiter。

## 資料與回執

### Dune

- `data_dune_crypto/raw/<contract>/<partition>/<execution>/page_*.json.gz`
- `data_dune_crypto/normalized/<fact_family>/year=YYYY/YYYY-MM-DD.parquet`
- `data_dune_crypto/receipts/<contract>/YYYY-MM-DD.json`
- `data_dune_crypto/state/executions/...`：先保存 execution ID，網路中斷後沿用同一執行，不重複花 credits。
- `data_dune_crypto/progress.json` 與 `download_summary.json`

分區鍵只使用穩定的 calendar start。當期右界每天前進時會原地更新同一分區，不會累積重疊分區。每列保存 contract ID、Dune query ID（direct SQL 為 `0`）、execution ID、SQL SHA-256、抓取視窗、available/retrieved time。

### SEC／ETF 發行商

- `data_crypto_etf/raw/sec/...`：SEC submissions、舊分片、companyfacts、primary documents。
- `data_crypto_etf/normalized/sec/<CIK>/filings.parquet`
- `data_crypto_etf/normalized/sec/<CIK>/companyfacts.parquet`
- `data_crypto_etf/normalized/issuer_daily_fund_metrics.parquet`
- `data_crypto_etf/normalized/issuer_holdings_snapshots.parquet`
- `data_crypto_etf/normalized/issuer_reserve_snapshots.parquet`
- `data_crypto_etf/progress.json` 與 `download_summary.json`

每輪都以 SEC `company_tickers.json` 解析 ticker 到 CIK，避免硬編 CIK 漂移。SEC filing 使用 acceptance time；只有 filing date 時保守使用該 UTC 日結束。發行商回傳的歷史列不回填成過去即可知：`available_at_utc` 至少是本機第一次觀測時間，原始 bytes 以 SHA-256 版本化。

若 EDGAR 目錄列出的 primary document URL 持續不可用，下載器會保存錯誤證據，並改抓同一 accession 的官方 complete-submission `.txt`。回執明確標記
`fallback_complete_submission`，不會把完整申報檔冒充為原 primary document；成功快取後也不會每日重打已知失效 URL。

Bitwise proof-of-reserves 欄位只標為「發行商發布的 reserve snapshot」，不宣稱已獨立驗證償付能力。

## 面板

既有資料監控面板已新增：

- `Dune 鏈上歷史資料`：逐 SQL 契約顯示完成分區／總分區、狀態、進度與 ETA。
- `SEC／ETF 發行商歷史資料`：SEC aggregate 與每個 issuer endpoint 分列顯示。
- 其他交易所來源顯示為 deferred，不會因舊回執而被判成仍在自動更新。

狀態排序仍為：正在抓／還沒到最新、正在串流、已完成／已到最新、無法完成。
