# OKX 永續合約歷史特徵下載契約

本文件描述 `downloader/download_okx_perp_15m.py` 在既有 15 分鐘完成柱之外，
會下載哪些 OKX 公開歷史資料、如何對齊，以及哪些 snapshot/stream 資料刻意排除。

完整機器可讀分類會由每次下載寫到：

```text
data_okx/okx_historical_feature_catalog.json
```

逐商品 coverage 與失敗原因會寫到：

```text
data_okx/historical_feature_report.csv
```

## 預設納入

| 類別 | 官方歷史來源 | 官方歷史邊界 | 寫入內容 |
|---|---|---|---|
| 成交 K 線 | `/api/v5/market/history-candles` | 近年 | 既有 OHLCV、合約/base/quote volume |
| Mark price | `/api/v5/market/history-mark-price-candles` | 近年 | mark OHLC、return、range |
| Index price | `/api/v5/market/history-index-candles` | 近年 | index OHLC、return、range |
| Funding | `market-data-history module=3` ＋ `/api/v5/public/funding-rate-history` | archive coverage 持續回補；REST 最多三個月 | realized/predicted-at-settlement、間隔、age、年化值 |
| OI | `/api/v5/rubik/stat/contracts/open-interest-history` | 最新 1,440 筆；15m 約 15 天 | contracts、coin、USD、15m 變化 |
| Taker flow | `/api/v5/rubik/stat/taker-volume-contract` | 最新 1,440 筆 | buy/sell contracts、imbalance |
| 全體帳戶 ratio | `/api/v5/rubik/stat/contracts/long-short-account-ratio-contract` | 最新 1,440 筆 | account-count long/short ratio |
| Top trader 帳戶 ratio | `/api/v5/rubik/stat/contracts/long-short-account-ratio-contract-top-trader` | 最新 1,440 筆 | top 5% account-count ratio |
| Top trader 部位 ratio | `/api/v5/rubik/stat/contracts/long-short-position-ratio-contract-top-trader` | 最新 1,440 筆 | top 5% position-notional ratio |

`account ratio` 與 `position ratio` 是不同統計量，下載器不會把它們合併。
OI 每一口同時存在多方和空方，因此 OI 上升也不會被命名為「多頭增加」。

Raw nominal mark/index OHLC 只作 provenance 與衍生計算；給模型時應優先選擇
`*_log_return_15m`、`*_range_log`、basis、OI change、taker imbalance 等正規化欄位。

## 歷史存在，但不放進預設 compact 15m 層

| 類別 | 來源 | 原因 |
|---|---|---|
| Raw premium samples | `/api/v5/public/premium-history` | 六個月高頻樣本；15m mark/index 歷史已能重建同一 fair-value 機制 |
| Tick trades | `market-data-history module=1` | 可重建 trade count、size、精確 CVD，但全市場歷史量很大，需要獨立 archive 預算 |
| 400/5000-level L2 | `market-data-history module=4/5` | 可歷史重建，但 2026-07-20 至 2026-07-25 的 BTC-USDT 400-level 實測約 1,973 MB |
| 1m candle archive | `market-data-history module=2` | 與既有官方 15m K 線重複 |
| Borrowing-rate archive | `market-data-history module=11` | 是 margin borrowing，不是目前 SWAP 商品粒度 |
| Options Trading Statistics | Rubik options endpoints | 主要是 BTC/ETH 市場級 context，不是每個 SWAP 的相同 grain |

這些資料不是 snapshot-only；它們只是不能在沒有儲存與運算預算的情況下，
悄悄併入每 15 分鐘 live updater。若要使用 tick/L2，應建立獨立 immutable
raw archive、容量上限、receipt、缺口稽核及 15 分鐘 materializer。

## 明確排除：無法 point-in-time 重建

| 類別 | 來源 | 排除理由 |
|---|---|---|
| Current OI | `/api/v5/public/open-interest` | 只有當下 snapshot |
| Current estimated funding | `/api/v5/public/funding-rate` | 無法用事後 realized rate 偽造當時 estimate |
| Ticker／rolling 24h | ticker、platform 24h volume | 只有當下 rolling snapshot |
| REST order book | books、books-full | 只有當下 snapshot |
| Liquidation | WebSocket liquidation-orders | 歷史 REST 已移除，沒有既有 archive 就無法回建 |
| Index components | index-components | 只有目前成分，不能投射到歷史 |
| Option summary | opt-summary | 只有目前 option snapshot，且不是 SWAP 商品粒度 |

## 因果對齊

- K 線與 Rubik 15m 統計只保留已完成 bar。
- Funding event 使用 `fundingTime <= bar_close_ts` 的最後一筆，並保存
  `okx_funding_age_hours`。
- Funding interval 用前一個已知 settlement 計算；下載器額外抓 24 小時 causal
  context，避免 requested range 第一筆事件失去間隔。
- 模型若在 15 分鐘 bar close 後決策，必須在 next-bar open 或更晚執行。
- 短 coverage 欄位的舊歷史保持 null，不會用今天的值回填。

## 執行與進度

```bash
source scripts/runtime_env.sh
run_fintech_python downloader/download_okx_perp_15m.py \
  --output-dir data_okx \
  --mode incremental \
  --start-date 2019-01-01 \
  --end-date today \
  --workers 16
```

下載時有兩層進度：

- `download:okx`：完成柱商品數。
- `download:okx-history`：`商品數 × 8 個歷史資料集`，顯示 ETA、dataset/s、
  成功與失敗數。

緊急只更新 K 線時可明確使用 `--skip-historical-features`。若只想跳過月度 funding
ZIP，可使用 `--skip-funding-archive`；這時 funding 只剩官方 REST 的三個月邊界。

