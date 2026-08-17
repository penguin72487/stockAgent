# Downloader API Rate Limits

The downloader defaults avoid Yahoo where project-specific alternatives exist.
Rate limits live in `downloader.common.PROVIDER_RATE_LIMITS`; command-line
`--request-interval` values that exceed the configured request rate are clamped.
There is no percentage safety margin: documented limits use their exact average
request rate, and providers without a documented special limit default to
`10 req/s`.

`SharedRateLimiter` is host-global, process-safe, FIFO, and supports weighted
claims. A provider retry defers the same named limiter, so already-queued worker
threads and sibling downloader processes observe the cooldown before sending.

## Provider Profiles

| provider | source | documented limit | default rate | notes |
|---|---|---:|---:|---|
| `okx_history_candles` | OKX `GET /api/v5/market/history-candles` | 20 requests / 2 seconds, IP | 10 req/s | Published average endpoint limit. |
| `okx_history_mark_price_candles` | OKX mark-price candle history | 20 requests / 2 seconds, IP | 10 req/s | Independent endpoint limiter. |
| `okx_history_index_candles` | OKX index candle history | 10 requests / 2 seconds, IP | 5 req/s | Independent endpoint limiter. |
| `okx_funding_rate_history` | OKX funding-rate history | 10 requests / 2 seconds, IP + instrument | 5 req/s/instrument | Limiter state is partitioned by `instId`. |
| `bybit_public_rest` | Bybit public REST IP bucket | 600 requests / 5 seconds, IP | 120 req/s | Exact published average IP limit; endpoint headers can impose an additional bucket. |
| `binance_usdm_request_weight` | Binance USD-M `exchangeInfo` | runtime `REQUEST_WEIGHT`; documented fallback 2400 weight / minute, IP | 40 weight/s | Runtime `exchangeInfo` is authoritative. Klines use limit 499 at weight 2. |
| `binance_usdm_funding_history` | Binance funding history | 500 requests / 5 minutes, shared IP bucket | 1.667 req/s | Shared by funding-rate history and funding-info. |
| `binance_usdm_statistics_history` | Binance `futures/data` statistics | 1000 requests / 5 minutes, IP | 3.333 req/s | OI, ratios, taker flow and basis retain a rolling 30-day window. |
| `shioaji_quote_query` | Shioaji quote API | conflicting docs: current Python page 50/10s; older PDF/C# 50/5s | 10 req/s | User-selected deployment ceiling matching the 50/5s documentation; all historical quote downloaders share one host-global limiter. |
| `frankfurter_public` | Frankfurter public API | no daily/monthly quota; unspecified anti-abuse throttling | 10 req/s | Uses the project default for an unspecified limit. |
| `tw_public` | TWSE/TPEx public endpoints | no stable public hard limit found | 10 req/s | Uses the project default; retries still handle WAF/403/429 responses. |
| `alpaca_market_data_basic` | Alpaca historical market data | 200 requests / minute, account | 3.333333 req/s | Exact Basic plan limit; requests batch many symbols. |
| `defillama_public` | DefiLlama free API hosts | no stable public numeric cap | 10 req/s | Compact snapshots only; no Pro endpoint. |
| `hyperliquid_info` | Hyperliquid `info` endpoint | 1200 weight / minute, IP | 1 req/s | Conservative 20-weight cadence for the selected info calls. |
| `deribit_public` | Deribit anonymous public API | credit-based limits vary by endpoint | 10 req/s | Client-side ceiling for compact book summaries. |
| `coinmetrics_community` | Coin Metrics Community API | 10 requests / 6 seconds, IP | 1.667 req/s | Anonymous community tier; preserve its license boundary. |
| `coinbase_exchange_public` | Coinbase Exchange public REST | 10 requests / second/IP, burst 15 | 10 req/s | Sustained official limit; the burst allowance is deliberately unused. |
| `kraken_public` | Kraken anonymous public REST | no numeric cap found for selected catalog call | 10 req/s | Unspecified-provider default; 429/backoff remains authoritative. |
| `bitfinex_public` | Bitfinex anonymous public REST | endpoint dependent | 10 req/s | Project-wide hard ceiling; compact runner makes one all-symbol request. |
| `alternative_me_public` | Alternative.me Fear and Greed | no numeric cap documented | 10 req/s | Complete public history is fetched in one request with attribution retained. |
| `mempool_space_public` | mempool.space public REST | numeric cap not published; 429/ban enforced | 10 req/s | Unspecified-provider default, compact state only, with 429 backoff. |
| `blockscout_ethereum_public` | Blockscout Ethereum instance REST | runtime header reports 10 requests/window | 10 req/min | Anonymous gas-price and latest-block snapshots; per-instance API carries a future-deprecation warning. |
| `binance_public_archive` | Binance Data Vision archive objects | no documented numeric cap | 10 req/s | Checksum files and ZIP objects share one process-safe archive-host limiter. |
| `binance_public_listing` | Binance Data Vision S3 listing | no documented numeric cap | 10 req/s | Independent process-safe listing-host limiter; no cross-host serialization. |
| `coingecko_demo` | CoinGecko Demo API | 100 requests/minute and 10,000 calls/month | 1.667 req/s | Keyed primary for canonical cross-market asset identity and market-cap/supply snapshots. |
| `coinmarketcap_basic` | CoinMarketCap Basic | 50 requests/minute and 15,000 call credits/month | 0.833 req/s | Identifier fallback and low-volume QA only; `/v1/key/info` supplies live plan usage. |
| `coinglass_keyed` | CoinGlass API | active plan and response-header dependent; lowest published plan is 30 requests/minute | 0.5 req/s | Entitlement must pass before data calls; currently not a free substitute for venue-native derivatives data. |
| `etherscan_free` | Etherscan V2 Free | 3 requests/second and 100,000 calls/day | 3 req/s | Selected chains only; key validity is verified independently of presence. |
| `dune_free_low` | Dune Free write/execute endpoints | 15 requests/minute | 0.25 req/s | Query execution consumes credits and requires a registered query contract. |
| `dune_free_high` | Dune Free read/result endpoints | 40 requests/minute | 0.667 req/s | Separate from the low-limit bucket; result exports also consume credits. |
| `sec_edgar` | SEC EDGAR APIs and Archives | 10 requests/second | 10 req/s | Official fair-access ceiling shared by every SEC request; `SEC_USER_AGENT` identification is mandatory. |
| `ishares_public` | iShares public tax/holdings files | no numeric limit published | 10 req/s client ceiling | Historical workbooks are immutable and content-addressed, so recurring runs do not redownload them. |
| `bitwise_public` | Bitwise BITB public page | no numeric limit published | 10 req/s client ceiling | One versioned page request per scheduled refresh. |
| unregistered provider | any new provider without a profile | no documented special limit | 10 req/s | Automatic fallback from `provider_rate_limit()`. |

## Source Notes

- OKX documents rate limits per endpoint and `GET /api/v5/market/history-candles`
  as `20 requests per 2 seconds`, rate-limit rule `IP`.
- Bybit documents an HTTP IP limit of `600 requests within a 5-second window`
  for traffic to `api.bybit.com`; stockAgent uses the exact `120 req/s` average.
  HTTP 403 `access too frequent` responses defer the provider for at least ten
  minutes, and `Retry-After`/`X-Bapi-Limit-Reset-Timestamp` can extend it.
- Binance publishes the active IP request-weight limits in
  `GET /fapi/v1/exchangeInfo`. The downloader converts every returned
  `REQUEST_WEIGHT` window to weight/minute and chooses the tightest one. Kline
  pages use 499 rows at weight 2 because this maximizes rows per documented
  weight tier. Funding and rolling-statistics calls also use their official
  endpoint buckets. HTTP 429/418 `Retry-After` applies to the shared IP limiter.
- Shioaji's current Python `Use Restrictions` page states 50 market-data quote
  calls per 10 seconds, while an older PDF/C# page states 50 per 5 seconds. The
  configured 10 req/s ceiling is an explicit user deployment decision matching
  the latter; the documentation conflict remains visible in receipts/docs.
  Daily KBars, minute KBars, and historical futures ticks share the same
  host-global limiter name. The traffic quota remains an independent hard stop.
  Separate machines using the same account still require external coordination
  because a host-local lock cannot enforce an account-wide ceiling across hosts.
- Frankfurter documents no monthly/daily quotas, but states public requests are
  rate-limited to prevent abuse.
- TWSE OpenAPI publishes Swagger endpoints, but no stable hard request rate was
  found for the historical web endpoints used here, so the default is `10 req/s`.
  Runtime `403`/`429` responses still use the downloader retry path.
- Alpaca Basic allows `200 requests/minute`; Algo Trader Plus allows
  `10,000 requests/minute`. `download_alpaca_us_ohlcv.py` accepts the exact plan
  value through `ALPACA_REQUESTS_PER_MINUTE` and batches symbols per request.

## Overrides

In `downloader/run_daily_all_markets.sh`, leave interval overrides empty to use
provider profiles. Alpaca uses an explicit plan limit:

```bash
OKX_REQUEST_INTERVAL=
BYBIT_REQUEST_INTERVAL=
BINANCE_REQUEST_WEIGHT_PER_MINUTE=
FRANKFURTER_REQUEST_INTERVAL=
TW_PUBLIC_REQUEST_INTERVAL=
ALPACA_REQUESTS_PER_MINUTE=200
```

Interval overrides only slow a provider down; values above an official limit or
the `10 req/s` unspecified-provider default are clamped. Alpaca instead accepts
the exact active account-plan limit (`200` for Basic or `10000` for Algo Trader
Plus).
