# Downloader API Rate Limits

The downloader defaults avoid Yahoo where project-specific alternatives exist.
Rate limits live in `downloader.common.PROVIDER_RATE_LIMITS`; command-line
`--request-interval` values that exceed the configured request rate are clamped.
There is no percentage safety margin: documented limits use their exact average
request rate, and providers without a documented special limit default to
`10 req/s`.

## Provider Profiles

| provider | source | documented limit | default rate | notes |
|---|---|---:|---:|---|
| `okx_history_candles` | OKX `GET /api/v5/market/history-candles` | 20 requests / 2 seconds, IP | 10 req/s | Published average endpoint limit. |
| `bybit_public_rest` | Bybit public REST IP bucket | 600 requests / 5 seconds, IP | 120 req/s | Published average IP limit. Bybit also returns per-endpoint headers. |
| `frankfurter_public` | Frankfurter public API | no daily/monthly quota; unspecified anti-abuse throttling | 10 req/s | Uses the project default for an unspecified limit. |
| `tw_public` | TWSE/TPEx public endpoints | no stable public hard limit found | 10 req/s | Uses the project default; retries still handle WAF/403/429 responses. |
| `alpaca_market_data_basic` | Alpaca historical market data | 200 requests / minute, account | 3.333333 req/s | Exact Basic plan limit; requests batch many symbols. |
| unregistered provider | any new provider without a profile | no documented special limit | 10 req/s | Automatic fallback from `provider_rate_limit()`. |

## Source Notes

- OKX documents rate limits per endpoint and `GET /api/v5/market/history-candles`
  as `20 requests per 2 seconds`, rate-limit rule `IP`.
- Bybit documents an HTTP IP limit of `600 requests within a 5-second window`
  for traffic to `api.bybit.com`; the configured average is exactly `120 req/s`.
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
FRANKFURTER_REQUEST_INTERVAL=
TW_PUBLIC_REQUEST_INTERVAL=
ALPACA_REQUESTS_PER_MINUTE=200
```

Interval overrides only slow a provider down; values above an official limit or
the `10 req/s` unspecified-provider default are clamped. Alpaca instead accepts
the exact active account-plan limit (`200` for Basic or `10000` for Algo Trader
Plus).
