# Shioaji connection and post-close collection schedule

The official account boundary is five simultaneous logins, 200 quote
subscriptions per login, and 50 quote queries per rolling ten seconds. A
connection is not a task: one owner process may serve several consumers, but
independent Python processes cannot share its `api` object. Historical K-bars
are request/reply calls and must not block a live quote callback. Current
limits: <https://sinotrade.github.io/zh/tutor/limit/>.

## Priority and ownership

1. Keep live stock execution/quotes and non-recoverable TAIFEX Tick/BidAsk
   capture connected; never evict them for historical backfill.
2. Fetch the latest *completed* stock session first. Older source-gap retries
   follow its terminal source catalog, rather than delaying today's frontier.
3. Run the two futures/options historical services through their existing
   single-login lock. Their throughput and the stock-minute workers share the
   process-wide `shioaji_quote_query` pacing state; another login does not
   multiply the account's request or traffic allowance.
4. Treat the research dataset, full-history audit, and daily hybrid as
   downstream materializations. A finished K-bar query alone is not a training
   or publication receipt.

| Taiwan time, ordinary weekday | Protected connections | Stock-minute behavior |
| --- | --- | --- |
| 05:00:10–07:45 | Two stock quote reservations, one historical login | Up to two minute logins; resume pending frontier |
| 07:45–14:31 | Live trading and capture | No historical stock-minute queries |
| 14:31–14:45 | Actual FOP workers, actual history workers, two stock quote reservations | Use remaining slots, up to the configured four workers; a hard resumable deadline yields before night pre-open |
| 14:45–14:50 | Three future FOP slots plus stock quotes | No new stock-minute login |
| 14:50–05:00:05 | Three FOP capture logins plus stock quotes | No stock-minute login if all five slots are occupied |

The night FOP collector starts at 14:50 (pre-open), not 15:00. Its three
workers have recently used 200 subscriptions each, so collapsing them into
one login is not a valid no-loss optimization. A live 2026-09-22 check found
five Shioaji sockets: three FOP workers, the day-trade paper engine, and the
Discord bot. The two quote reservations were therefore real that night, not
merely conservative accounting. Re-measure this snapshot before changing
reservations.

The Top-200 stock Tick+BidAsk collector requests two further logins. Under
the current simultaneous day-session workload, `3 FOP + 2 stock quote + 2
Top-200 = 7 > 5`, so its `connection_budget` skip is a genuine unresolved
coverage gap. Recovering the whole stream requires deduplicated subscription
ownership or an explicit product-scope trade-off; scheduling alone cannot
reconstruct unrecorded live books.

The minute runner's 14:31 frontier uses receipt-backed chunks. At 14:45 the
downloader stops admitting new queries, finishes in-flight work, writes an
incomplete run receipt without replacing the last terminal catalog, logs out,
and resumes at the next safe capacity window. Traffic-exhausted runs wait for
the next post-08:00 trading-day reset and 14:31 historical window; transient
errors retry within the current post-close window. Research materialization is
reused only when source chunk hashes, gap classifications, terminal universe,
the research manifest's source fingerprint, and the existing full audit all
match; row count alone is never sufficient. Older manifests without a source
fingerprint are rebuilt once.

Acceptance must check `download_summary.json` target date and selected symbol
count, the source-history audit's pending/source-gap classifications, the
research manifest and full audit, plus live FOP/Discord/day-trade service
continuity. `systemctl active` is not evidence of a data request or completion.
