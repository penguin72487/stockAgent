# TW Public Download Resume And Rate-Limit Contract

Last official-document check: 2026-07-12.

## Published upstream limits

No numeric request-per-second limit was found for the TWSE, TPEx, or
`data.gov.tw/api/v2/rest/dataset/...` endpoints used by the TW public data
layer:

- TWSE OpenAPI: <https://openapi.twse.com.tw/>
- TWSE OAS: <https://openapi.twse.com.tw/v1/swagger.json>
- TWSE website terms: <https://www.twse.com.tw/zh/terms/use.html>
- TPEx OpenAPI: <https://www.tpex.org.tw/openapi/>
- TPEx OAS: <https://www.tpex.org.tw/openapi/swagger.json>
- TPEx website terms: <https://www.tpex.org.tw/zh-tw/gtsm_disclaimer.html?l=zh-tw>
- data.gov.tw metadata API guide:
  <https://data.gov.tw/about/doc?chapter=27&doc=8>
- data.gov.tw platform policy: <https://data.gov.tw/privacy>

The `50 requests/second` example in the government common API guide applies to
authenticated TDX transport APIs. It is not a published limit for the
`data.gov.tw` metadata endpoint used here:
<https://data.gov.tw/about/doc?chapter=32&doc=9>.

The TWSE and TPEx website terms also restrict automated download methods unless
they are used in an approved manner. Operators are responsible for ensuring
their use is authorized. A client-side rate limit does not itself grant that
authorization.

Therefore, the default `8 req/s` is a stockAgent client-side safety policy, not
an official upstream allowance. A `403`, `429`, `Retry-After`, or observed WAF
response always takes precedence over the configured default. In environments
that receive such responses, set a slower explicit interval, for example
`--request-interval 1.0` for at most one request per second.

TWSE may return an HTTP `307` security page instead of JSON after a request
burst. The downloader recognizes that body, applies a 30-second provider-global
cooldown, and retries. The configured 8 req/s remains a peak client policy; it
does not override an observed upstream block.

## Global limiter semantics

`SharedRateLimiter` coordinates a provider-named schedule across:

- all threads and datasets in one downloader process; and
- all stockAgent subprocesses for the same operating-system user and provider.

The host-global state defaults to the operating system temporary directory
under `stockagent-rate-limits-<uid>`. Set `STOCKAGENT_RATE_LIMIT_DIR` to choose a
different portable state directory. Unregistered providers default to 8 req/s.
Explicit intervals faster than the configured policy are clamped; slower
intervals are accepted. Transient `403`, `429`, `5xx`, and `Retry-After`
responses defer the shared provider schedule, not only the worker that received
the response.

Yahoo Finance uses its own `yahoo_finance` provider bucket. A direct
`download_yahoo_ohlcv.py` invocation therefore defaults to 8 requests/second
when `--request-interval` is omitted and shares that cap with concurrent
stockAgent Yahoo processes. The canonical first TW fallback bootstrap remains a
deliberately slower exception: one worker with a 1.5-second interval, because a
large symbol sweep is more likely to trigger Yahoo throttling.

TW Yahoo repair resolves a stable union rather than treating the current live
listing response as a complete historical universe. The union includes live
discovery, the cached and repository manifests, locally tracked symbol parquet,
and canonical TWSE/TPEx delisted-company parquet. Each accepted source file must
carry `stockagent.source=yahoo`, `stockagent.asset_class=tw_stocks`, a
`stockagent.yahoo_requested_start` no later than the archive start, and a
`stockagent.yahoo_checked_through` no earlier than the archive end. The archive
accounts for every manifest symbol as either a verified file or a terminal
unavailable result, then writes an adjacent `.inputs.json` containing full file
size/SHA-256 and coverage receipts. Its summary receipts that manifest; symbol
build and final audit reject a missing, stale, or tampered receipt chain.

An HTTP 200 response can still be semantically unsafe: malformed JSON/HTML,
wrong date, a poisoned cache body, or decode damage. Such a parse failure closes
and discards only the current worker thread's persistent HTTP session before the
route switch or cache-busted retry. It does not add a second provider-global
defer or sleep, because `_http_get` already enforces status/backoff policy and
the next call still executes the shared limiter's `wait()`. Thus semantic
retries remain bounded by the default 8 req/s policy, while WAF, 429,
`Retry-After`, and transport cooldowns remain provider-global.

## Append-only historical resume

Each historical dataset writes an append-only control journal:

```text
<output-dir>/state/journals/<dataset>.jsonl
```

Each terminal request outcome records the dataset/date, parser fingerprint,
status (`data`, `empty`, or `failed`), URL, row count, error, timestamp, and—when
available—a relative raw path, byte size, and SHA-256 receipt. Every record is
appended with `fsync`. A torn final line caused by process termination is
ignored; corruption before the final unterminated line fails closed.

Parsed successful dates are batch-merged atomically into a deterministic
fingerprinted partial parquet:

```text
<output-dir>/state/partials/<dataset>.<fingerprint>.parquet
```

On `--resume` (the default), rebuild planning resolves dates from the readable
partial parquet plus journal-confirmed empty dates. Existing atomic raw JSON or
HTML receipts are parsed with the current parser and can bootstrap missing
partial data. Only missing, failed, corrupt, or suspicious dates are sent to the
upstream service. `--no-resume` deliberately ignores prior partial/journal/raw
progress and performs a fresh request plan.

Partial success never weakens publication rules:

- unresolved dates keep the dataset and command nonzero/incomplete;
- successful and confirmed-empty dates remain reusable on the next run;
- the prior canonical parquet and production tree remain untouched;
- complete staged coverage is validated before an atomic canonical copy; and
- the outer rebuild still audits the full staged tree before production
  promotion.

On POSIX, the complete mutation for one historical dataset also holds an
exclusive, nonblocking process lock:

```text
<output-dir>/state/locks/<dataset>.lock
```

This prevents two terminals from concurrently appending the same journal or
replacing the same partial/canonical parquet. A second writer to the same
dataset and stage fails immediately; different datasets may still run in
parallel. The JSONL lock inside one process remains separate from this
cross-process lock.

When strict actual-session calendars replace older weekday planning, legacy
state can contain `failed_dates` for holidays or malformed date keys. Final
state construction removes those non-session keys and any session failure now
resolved by verified data or an accepted empty receipt, but retains every real
unresolved session failure. The state records `last_pruned_failed_dates`, up to
ten `last_pruned_failed_date_examples`, and cumulative
`pruned_failed_dates_total`. This cleanup may restore `coverage_complete=true`
when stale non-session failures were the only residue; the counters preserve
the audit trail.

## TWSE official historical fallbacks and WAF behavior

The newer `rwd` routes can return a wrong-date cached payload, an incorrect
historical-range status, a structured empty, or a `307 FOR SECURITY REASONS`
page. Every affected history has a same-product official legacy fallback:

| Dataset | Primary path | Official fallback path |
|---|---|---|
| `twse_daily_ohlcv` | `/rwd/zh/afterTrading/MI_INDEX?type=ALLBUT0999` | `/exchangeReport/MI_INDEX?type=ALLBUT0999` |
| `twse_daily_valuation` | `/rwd/zh/afterTrading/BWIBBU_d` | `/exchangeReport/BWIBBU_d` |
| `twse_institutional_trades` | `/rwd/zh/fund/T86` | `/fund/T86` |
| `twse_margin_balance` | `/rwd/zh/marginTrading/MI_MARGN` | `/exchangeReport/MI_MARGN` |

`twse_market_index` independently uses the `rwd` and `exchangeReport`
`MI_INDEX?type=IND` pair. A semantic failure switches from the primary to the
legacy route. If the legacy response is itself stale or invalid and retries
remain, its next URL receives a unique `_=<time_ns>` cachebuster; the bad body
is retained in `raw_failures` with its hash if it remains the terminal failure.
Neither route nor cachebusting weakens schema, selected-table-date, or
session-calendar validation.

Failure receipts are content-addressed and created immutably. After a parser
contract bump, `--resume` may reparse an older failed receipt without another
request, but only after validating its append-only `failed/network/HTTP 200`
journal event across the old cache key, exact official URL and date, confined
path, byte counts, filename hash prefix, full raw/body SHA-256, and a nonempty
current-contract parse. Any damaged, empty, mismatched, or unjournaled receipt
stays unresolved and is fetched again.

A live recovery recheck on 2026-07-12 used
`--request-interval 1.0`, i.e. one provider-global request per second, for these
WAF-sensitive histories. This is a measured recovery setting, not a published
TWSE allowance. Because the limiter is provider-global, increasing date workers
does not raise that aggregate rate. A subsequent security page still triggers
the shared 30-second cooldown.

### Market-index historical anomaly

The canonical `twse_market_index` series starts on 2009-01-05, the first date
for which the official `IND` category returns index rows. The newer
`rwd/zh/afterTrading/MI_INDEX?type=IND` route has a known semantic anomaly for
2009-02-02: it can claim that the historical date is in the future even though
the market traded. If the primary response fails validation, or if it reports a
structured weekday empty, the downloader cross-checks TWSE's official
`exchangeReport/MI_INDEX?type=IND` route before accepting the outcome.

The same official `exchangeReport/MI_INDEX` fallback is used for
`twse_daily_ohlcv` with `type=ALLBUT0999`. This covers historical dates such as
2007-04-25 for which the newer route incorrectly claims the request predates
2004-02-11 even though the official fallback returns a valid daily table.

Date validation follows the selected target table. Some old MI_INDEX payloads
have a stale top-level `date` while the actual daily table title carries the
correct requested date. A wrong target-table date still fails closed, even when
the top-level date happens to match.

The table title alone is not sufficient on a verified non-session: TWSE can
retitle stale rows with the requested historical date. This was observed for
2005-02-08, 2008-10-10, 2010-06-16, and 2015-02-27; the first payload even
contained securities introduced years later. Canonical strict-calendar mode
does not request those dates and rejects an older canonical that contains them,
rather than silently treating the stale rows as trades. The affected
`twse_daily_ohlcv` and `twse_market_index` parser contracts were advanced from
v5 to v6: an existing v5 partial remains immutable evidence, while v6 reparses
the already downloaded SHA-receipted raw files into a new partial using only
verified sessions. It therefore preserves successful download work without
carrying the stale holiday rows forward.

## TPEx daily parser v11 evidence contract

The immutable receipts prove five distinct daily-quote layouts. Indices below
are zero-based and are part of the parser contract:

| Width and verified sessions | Exact field indices |
|---|---|
| 27, 2003-08-01--2004-01-30 | `code=1,name=3,close=5,change=7,open=9,high=11,low=13,average=15,volume=17,amount=19,trades=21,bid=23,ask=25`; other cells are spacers |
| 26, 2004-02-02--2004-10-27 | `code=0,name=2,close=4,change=6,open=8,high=10,low=12,average=14,volume=16,amount=18,trades=20,bid=22,ask=24`; interleaved cells are spacers |
| 18, 2004-10-28--2004-11-24 | `code=0,name=1,close=2,direction=3,change=4,open=5,high=6,low=7,average=8,volume=9,amount=10,trades=11,bid=12,ask=13,shares=14,reference=15,limit_up=16,limit_down=17` |
| 19, 2004-11-25--2006-12-29 | `code=0,name=1,status=2,close=3,direction=4,change=5,open=6,high=7,low=8,average=9,volume=10,amount=11,trades=12,bid=13,ask=14,shares=15,reference=16,limit_up=17,limit_down=18` |
| 17, legacy JSON `html`, 2007-01-02--2007-06-29 | `code=0,name=1,close=2,change=3,open=4,high=5,low=6,average=7,volume=8,amount=9,trades=10,bid=11,ask=12,shares=13,reference=14,limit_up=15,limit_down=16` |

The normalized header retains `代號`, `名稱`, `收盤`, `漲跌`, `開盤`, `最高`,
`最低`, `均價`, `成交股數`, `成交金額(元)`, `成交筆數`, `最後買價`,
`最後賣價`, `發行股數`, `次日參考價`, `次日漲停價`, and `次日跌停價`.
Widths 27 and 26 legitimately leave the last four fields empty. A synthetic
width-18 status-plus-combined-change form remains a compatibility case; it was
not among the measured immutable archive receipts.

Archive identity requires a labeled `資料日期`/`交易日期` compact ROC date.
For permanently damaged 2004 labels, the sole alternative is exactly one ROC
date inside `<tt>` in a cell whose attributes are `width=71`, `colspan=5`,
`rowspan=2`, and include `class=table-body-right`. All matching declarations
must agree with the requested date. The 2007 legacy JSON payload date is checked
the same way before its embedded `html` is accepted.

Permanent CP950 damage is narrowly provenance-backed:

- An unrecoverable security name may be retained only with
  `_name_decode_status=official_receipt_name_bytes_unrecoverable`; if a receipt
  contains replacement-byte evidence, every name row in that receipt receives
  this status because CP950 can re-pair `EF BF BD` into plausible CJK text.
  Symbol and every numeric field remain fail-closed.
- Only `嚙踝蕭嚙緞`, `嚙踝蕭嚙踝蕭`, and `嚙踝蕭嚙緞嚙踝蕭` may be recovered as
  `除權`, `除息`, and `除權息`. The row records
  `_change_decode_status=official_receipt_change_recovered_from_cp950_byte_pattern`.
  Any unknown damaged change token, or a known token outside a lossy receipt,
  fails closed.
- `均價=註` (including its known damaged rendering) is accepted only when
  close/open/high/low, change, volume, amount, and trade count are all exactly
  zero. It is normalized back to `註`; it is never treated as a numeric average.

Two zero-price cases have different downstream semantics:

- `open=high=low=0` with a positive official close remains official. Raw data is
  untouched; the canonical symbol file may form a flat bar at that close only
  with `ohlc_normalization=official_close_flat_bar`, and Yahoo cannot replace it.
- `open=high=low=close=0` with positive volume plus positive, internally
  consistent amount/average is an official **unpriceable** observation. It
  proves the session/transaction statistics but is not usable OHLC, and average
  must never be substituted for close. A valid Yahoo bar may fill that key with
  `fallback_reason=official_ohlcv_unusable`; otherwise the key remains an
  explicit unfilled limitation.

The symbol-build receipt reconciles this classification exactly:

```text
official_unusable_ohlcv_rows
  = fallback_replaced_unusable_official_rows
  + unfilled_unusable_official_rows
```

The data-layer audit recomputes row-level `fallback_reason` and
`ohlc_normalization` counts against that summary, so an unpriceable key cannot
silently disappear or be mislabeled as an official quote.

## TPEx margin v8 layouts and v7 official archive gap

Parser contract v8 recognizes the official 16-cell margin layout introduced on
2004-10-19 and its standalone compact ROC date in the exact right-aligned,
vertically centered, `colspan=14` unit/date header cell. The final note `<td>`
is structurally real even when blank, so the malformed Oracle HTML extractor
preserves all 16 cells. A 15-cell row is not accepted as an implicit blank
note, because it could instead be missing a financial field.

TPEx valuation parser contract v7 recognizes the labeled ROC header used by
the 2004--2006 archive, for example `交易日期:94年08月08日`, and binds it to
the requested Gregorian date before accepting any five-cell security rows.

Live checks against `tpex_margin_balance` found official data through
2007-05-31 and again from 2008-09-30. Every one of the 331 verified open
sessions from 2007-06-01 through 2008-09-29 returned HTTP 200 with the same
explicit structured no-data outcome. Parser contract v7 records this as
`official_endpoint_archive_gap`; it does not fabricate margin rows and does not
infer coverage merely from the range endpoints.

Each open session in this gap becomes reusable coverage only when all of the
following evidence survives:

- the response is HTTP 200 and reparses as explicit no-data with no status
  error or rows;
- its date is inside the declared gap and inside the receipt-verified official
  session calendar;
- the body is atomically retained, even under `--skip-raw`, at
  `raw_empty/tpex_margin_balance/<date>.json` and cannot later be replaced by
  different bytes;
- the latest parser-fingerprinted JSONL event is `status=empty` with
  `source_unavailable_reason=official_endpoint_archive_gap`;
- the journal URL matches the exact official date request (ignoring only the
  retry `_` cachebuster), and raw size, content length, raw SHA-256, and body
  SHA-256 all match the immutable file.

Resume revalidates that full chain before skipping a request. Missing, changed,
wrong-URL, wrong-hash, or non-explicit receipts are requested again. An empty
response outside 2007-06-01--2008-09-29 still fails as missing data on a
validated open session, while a nonempty official response inside the range is
kept as data. Coverage state reports confirmed gap dates separately through
`confirmed_source_unavailable_dates`, `confirmed_empty_date_accounting`, and
per-range expected/confirmed session counts.

## Complete TAIEX OHLC archive

The multi-index `MI_INDEX?type=IND` table does not provide the pre-2009 TAIEX
history needed by a panel that begins in 2000. TWSE separately publishes the
official `MI_5MINS_HIST` TAIEX OHLC archive from 1999-01-05:
<https://www.twse.com.tw/zh/indices/taiex/mi-5min-hist.html>.

`downloader/download_tw_taiex_ohlc.py` retrieves that product once per calendar
month and writes:

```text
data_tw_public/twse_taiex_ohlc.parquet
data_tw_public/twse_taiex_ohlc.summary.json
```

Standalone rebuilds use the same portable runtime and shared provider limiter:

```bash
source scripts/runtime_env.sh
run_fintech_python downloader/download_tw_taiex_ohlc.py \
  --mode rebuild \
  --output-dir data_tw_public \
  --start-date 1999-01-05 \
  --end-date today \
  --resume
```

It has the same `rebuild`, `repair`, and `daily` modes as the canonical data
layer. Month outcomes are appended to
`state/journals/twse_taiex_ohlc.jsonl`; successful parsed rows are retained in
the deterministic `state/partials/twse_taiex_ohlc.<fingerprint>.parquet` until
publication. Raw monthly responses live under
`raw/twse_taiex_ohlc/YYYY-MM.json`. Resume accepts a month only when its parsed
price rows and semantic checksum match the latest journal event. A stale
response for another month, malformed schema, duplicate date, impossible OHLC,
empty month, HTTP/WAF failure, or corrupt raw receipt remains unresolved and
cannot replace the canonical parquet.

Monthly responses can contain rows later than a requested mid-month cutoff.
Rows are therefore validated against the requested month and then clipped to
`--end-date` before they enter the partial or canonical output. Daily mode
requires a completed rebuild/repair baseline and refetches every month touched
by the recent seven-calendar-day correction window.

The outer `download_tw_official_data.py` workflow runs this monthly stage before
**any** historical public-source request, then invokes
`download_tw_public_data.py --require-taiex-session-calendar`. That strict flag
is intentionally opt-in for the standalone public downloader, but mandatory in
the canonical outer workflow. Before using the archive as a session calendar,
the child verifies all of the following fail-closed:

- the summary certifies a promoted, coverage-complete range spanning the whole
  requested source range;
- the summary receipt path, byte size, SHA-256, and row count match the exact
  canonical parquet;
- dates are unique and OHLC/provenance columns are valid; and
- an existing historical canonical or staged partial has no row outside its
  verified expected-session set.

Canonical TWSE histories and TPEx OHLCV therefore request actual TAIEX sessions,
not every weekday. Non-session weekdays are never journaled as
`confirmed_empty`. An empty, 404, wrong-date, or wrong-schema response on a
verified session remains a failed date. TPEx margin, institutional, and
valuation histories continue to use the audited `tpex_daily_ohlcv` session set;
strict mode also proves that this baseline is bound to the current TAIEX receipt
and has exactly the same requested sessions.

After the public histories complete, the feature builder treats the monthly
archive as the complete TAIEX base. Same-date `MI_INDEX IND` values have higher
priority only after their official closes agree; a mismatch fails feature
construction. The three existing feature names remain unchanged:

- `twpub_twse_taiex_log`
- `twpub_twse_taiex_logret_1d`
- `twpub_twse_taiex_pct`
