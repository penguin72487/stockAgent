# Panel, feature and crypto data contracts

Read the relevant section for panel backends, feature availability, or crypto acquisition. Preserve data semantics and point-in-time gates. Hardware timings and provider observations require current verification before reuse.

## FinMind finite product source verification

Whole-market daily acquisition does not complete a separately requested
per-product full-history source comparison. Register explicitly enumerated existing
`id_history` keys with `downloader/finmind_product_query_verification.py` and an
explicit `finmind-finite-product-query-verification-v1` plan, source evidence
and SHA. The canonical Complement dispatcher may admit only those exact keys
across dataset-level delegation; unregistered neighbors remain delegated.
Keep incremental precedence, quota/reserve, retry clocks, terminal/empty states,
worker lock, original captures and financial adoption separate. Registration
never reopens tasks or proves provider completeness. A code handoff uses the
existing drained-idle reload lifecycle and the pinned semantic JUnit acceptance
`finmind_finite_product_query_semantics_v1`; it is not a speed measurement.
See the [725-source repair evidence](../tw_futures_universe725_recovery_2026-10-25.md).

A registered, finite source check may preempt due US/UK/Japan `id_history`
backfills, including pending acquisitions and due retries. The canonical owner
subtracts only these whitelisted, nondelegated due rows from its temporary
reservation view; dated publications, upcoming reservations, provider usage,
pacing and retry cooldowns remain protected. Task states and retry clocks do
not change. This supersedes the accepted-renewals-only rule after the user's
2026-10-27 priority request; that rule starved the 39 requested new keys behind
foreign history acquisition. See the [765-code recovery evidence](../../artifacts/markets/tw_futures_universe765_recovery_20261027_v1/README.md).
The check retains its original task priority and due clock and uses the same
serial exact-key handler. This prevents a many-thousand-symbol renewal backlog
from starving a bounded user-requested source comparison.

## Finite queries through canonical provider owners

Other programs may submit `provider_priority_query_v1` through
`scripts/request_provider_data.py` or `downloader.provider_priority`. Requests
have one or more exact registered items, a reason and at most seven days of priority.
The 2026-10-28 request removes fixed item-count, active-registry and plan-byte
ceilings. Duplicate scopes coalesce; local SQL batches respect SQLite's actual
parameter limit without truncating total demand. Preserve old receipt IDs.
Use the native maximum verified range and existing shared tasks/receipts;
an item is not a vendor request. Source row/page/usage limits still apply.
Registration/status make no vendor calls and never reopen terminal/unknown
tasks. Due releases, quota, protected opening, retry isolation and dedicated
delegation remain binding. As with finite product checks, only the known
US/UK/Japan ID-history renewal reservation may yield to a ready finite read;
dated releases and upcoming reservations are retained. FinMind JSON admission
and selection use the same view. See the [guide and reusable skill](../provider_priority_scheduling_2026-10-27.md).

TEJ incremental cycles retain the initial history cutoff and unfinished lazy
plan. Native query splits retain cycle lineage. A `checked` cycle proves the
bounded source queries ended, including explicit empty responses; it does not
prove the source published all values or certify historical revision coverage.

## Data Panel Backend Contract

The full US Yahoo universe is a distinct data-processing regime. Benchmark it
with the actual full parquet directory instead of small synthetic subsets when
choosing defaults.

Rules:

- `scripts/benchmark_data_backends.py` is the reproducible scanner/benchmark for
  data-processing hotspots. Its active optimization scope is PyArrow plus Polars
  Lazy/Streaming; do not add compatibility/reference paths outside those backends.
- Do not add DuckDB or cuDF back to the panel/runtime benchmark set unless a new
  request explicitly reopens those candidates.
- For full US daily parquet (`16811` files, `S≈16811`) with
  `tradable_mode: tradable`, runtime `build_panel(... panel_backend="auto")`
  should select Polars Lazy when available, then PyArrow. Explicit
  `panel_backend="polars"` is an alias for `polars_lazy`; explicit
  `panel_backend="polars_streaming"` is available for measurement but is not the
  current auto default.
- US Yahoo parquet files under `us_stocks` must keep `Trading_Volume` for
  trainable stock-like assets. If old `_DL`/delisted/archive parquet files are
  missing that column, treat them as schema-broken data: repair should normalize
  `_DL` records back to the base Yahoo symbol and remove unusable delisted
  schema-mismatch files instead of letting panel build fail on the first file.
- For the US Yahoo universe, do not remove a symbol only because it is currently
  delisted; historical delisted common stocks/ADRs/ETFs are needed to reduce
  survivorship bias. Do exclude security types outside the normal broker-tradable
  stock/ETF/ADR universe, such as warrants, rights, units, preferred/depositary
  preferreds, and exchange-listed notes/debt instruments.
- Foreground full-US PyArrow+Polars benchmark after narrowing the scope:
  `panel_build` measured Polars Lazy `69.36s`, Polars Streaming `86.55s`, and
  PyArrow `195.85s` on recheck. PyArrow checksum was
  `8707711790994.017`; Polars Lazy differed by about `913` in feature checksum
  on very large OHLC anomaly-derived ratios, while returns and masks matched in
  spot checks. Use `panel_backend="pyarrow"` when bitwise checksum parity is more
  important than panel-build speed.
- Wide full-US daily weight parquet output (`512 x 16811`) should prefer Polars
  Streaming sink among active native backends: repeat-5 foreground benchmark
  measured Polars Streaming `1.81s`, Polars Lazy `2.13s`, and PyArrow `4.02s`.
- Feature-prep proxy benchmarks favor PyArrow because they use direct Arrow to
  NumPy arrays: PyArrow `29.58s` on recheck, Polars Lazy `206.41s`, and Polars
  Streaming `296.95s`. For the current many-small-files US layout, PyArrow's
  single-pass full-table read is faster than adding a per-file schema projection
  pass.

## Immutable Panel storage

Full/tail builders resolve model feature patterns through the same complete
schema selector before payload decoding; excluded columns must not be read
merely because an exclusion list is nonempty. Preserve model order, raw/gap
opt-in, null/clock semantics and all independently enabled rule columns. Share
contiguous normalized symbol buffers instead of copying the complete stock
subset. This is physical work avoidance, not a smaller source/model ABI.

Keep original cache keys. `load_cached_panel` accepts the same explicit
`panel_cache_root` as the writer; audit also respects the configured root.
Existing original or legacy projected hits remain usable; new audits write
only the original config key. Unknown selection/source changes still fail
closed. The native 08:30 audit dependency fingerprint includes shared Panel,
cache and chunk implementations; old audit success is not current-code proof.

`panel_cache_v2` keeps its canonical writer lock, atomic generation/variant
commit and exact source/backend/version identity. `panel_time_chunks_v1` is an
explicit storage marker, not a feature or precision change. Use
`load_panel_cache_array` (or the full/pinned cache loaders), never assume an
array manifest's `file` is a single NPY. Readers must be deployed before opting
writers in with `STOCKAGENT_PANEL_CHUNK_STORAGE=1`; existing NPYs need no forced
rebuild. A fresh source-bound commit may reuse old immutable payloads, so do not
invalidate it from the oldest shared payload mtime. Sliding live tails opt out
of byte chunks because removing their first row shifts the complete byte axis.
See [producer evidence and rollout](../penguin_service_storage_layout_2026-10-15.md#1016-續修panel-追加只寫變動塊共用歷史而不另組整份副本).

## Incremental dataset versions

All registered `data_*` roots share complete logical inventories and immutable
payload reuse. `configs/columnar_storage.json:source_versioning` records every
root and its native/target grain; the existing packed catalog owns publication
scope/rights. Shared v2 writers can chunk changed large files without altering
their exact bytes; readers support both protocols. A new root must be registered,
not silently excluded. Use `publish_data_releases.py layout` to distinguish code
support, observed head format and native migration. Existing working paths,
raw units/clocks and legacy payloads are not moved or deleted merely by adopting
this policy. Reader-first activation and remaining producer migrations are
explicit in [the common layout](../dataset_incremental_storage.md).

TW public Parquet readers distinguish one legacy byte file from an explicit
ordered partition manifest. Reuse `ParquetSource` and `ParquetLineage`; do not
put a logical or manifest digest into an existing raw-file SHA field. New
partition receipts carry an explicit source identity and every referenced
member's raw byte hash, including older partitions. Conflicting ABIs, changed
generations and missing/corrupt historical members fail closed. Reader support,
offline full-value parity and formal runtime admission remain separate gates;
the formal writer does not switch merely because a reader supports manifests.
See [implementation and measured boundaries](../dashboard_daily_incremental_2026-10-20.md#第六階段1021-研究發布身分與驗收恢復).

### News native copy-on-write

`data_news` versions are complete logical views, not new full physical copies.
The canonical `scripts/build_news_dataset.py --refresh` uses manifest ABI 2:
unchanged immutable Parquet parts are referenced, additions are written, and
only retained rows in affected parts are rewritten for corrections/deletions.
Complete typed row matching is a multiset, not semantic deduplication. Preserve
original strings, null bodies, source clocks, provenance and use flags.

The root manifest is the publication pointer; root README and verification
files are convenience copies and can describe a candidate if a publication is
interrupted before the pointer moves. Bind acceptance to the manifest's own
`versions/<generation>/verification.json` and its manifest-content hash; only
fall back to a matching root receipt when the generation receipt is absent for
a legacy version. A present but invalid generation receipt must not fall back.
The layout inventory rechecks the root manifest after the receipt read and
reports pending if it changed. New READMEs link to their fixed generation and
distinguish it from the moving root pointer; quality details are authoritative
inside the bound verification receipt. New generation schemas are included in
new fixed private-news backup plans when present; previously sealed plans stay
valid. None of this revalidates source content or proves historical coverage.
See the [interruption and readback evidence](../news_all_source_repair_2026-10-09.md).

Readers must use `news_table_paths` / corpus iterators or a pinned manifest;
support legacy ABI 1 and root-relative ABI 2 paths. Never glob all versions.
Input projection chunks reconstruct exact SQLite bytes and SHA; only the new
transaction's reconstructible work files are removed after verification and
commit. A crash after publication retries verification/cleanup, never rebuilds
an accepted generation. Retained manifests protect all referenced old parts and
chunks. No automatic legacy deletion, GC, compaction or remote publication is
enabled. Full source comparison and acceptance still run; physical delta storage
does not imply delta-only CPU work. Other providers are not implicitly migrated.
See [implementation and evidence](../news_incremental_storage_2026-10-16.md).

The headline scope `cnyes_market_and_cmnews_tag_metadata_only_v2` keeps no-body
CNYES `/twstock/` references and CMNews `/tag/` references, including strictly
decoded literal Google envelopes, as source observations rather than news
headlines. The CMNews route is an unverified page hint, not proof of its
content or absence. The scope retains existing accepted bodies and does not
equate these routes with articles.
This changes the corpus data scope: a new build must use a separate root and a
new compatible input snapshot; old ABI 1/2 manifests remain readable and must
not be silently rewritten. Corpus construction and DuckDB verification use
the news owner's sampled physical-backing check as well as filesystem free
space; stale or unavailable samples defer work. This sampled 25 GiB guard is
not an exact forecast or a reservation for a complete rebuild. See the
[bounded readback and unresolved rebuild](../news_all_source_repair_2026-10-09.md).

The body quality scope `owner_reviewed_exact_body_exclusions_v1` excludes a
stored body from a new corpus only when a successful canonical owner review
matches the article, provider, body hash, source batch and capture time and
its sealed request and accepted source artifacts verify. Retain the exact
request bytes in the input snapshot; an unavailable or inconsistent witness
defers the build. A later body or capture, even with identical text, is not
excluded by the old review. Preserve original source data and retain an
eligible title as a headline. This scope change also requires a new corpus root
and compatible snapshot; legacy manifests remain readable. The [quality audit
and private end-to-end rehearsal](../news_all_source_repair_2026-10-09.md)
do not imply that the production corpus has been rebuilt.

The former metadata selection scope `explicit_current_origin_or_legacy_inference_v1`
uses the news owner's verified current complete receipt version. Historical
versions and unfinished cursors remain evidence; a reverted receipt must not
select a later obsolete origin or count its unfinished version as active.
Legacy entries without a recorded selection use the highest complete origin id
as an inference, reported separately, rather than as observed current-source
proof. Invalid explicit pointers defer the snapshot. This changes corpus scope:
new builds require a separate root and a compatible input snapshot; old
manifests remain readable. The [offline repair and evidence](../news_all_source_repair_2026-10-09.md)
do not establish that a production receipt reverted or that a corpus was rebuilt.

The successor `explicit_current_origin_or_legacy_inference_alias_census_v2`
also records a logical receipt alias only after its target was verified. A
detected retarget is durable before importing the new target, and stays pending
through a restart, an incomplete receipt or an unavailable source catalog.
Only a completed two-pass, catalog-membership-stable reference census may
retire an old physical target from the current selection; any other active
reference keeps it selected. Historical origins, associations and source files
remain intact.
Pending retargets make current metadata totals unknown and defer a training
snapshot. The census yields between paths. New completed retarget events retain
the compressed catalog membership, resolved references, pending changes and
retirement candidates under a content hash in the canonical queue transaction;
`read_alias_census_proof` independently checks integrity and internal membership
and retirement consistency. The normal write path verifies the stored bytes;
it does not re-run that full independent readback on every new proof. Older
digest-only events remain readable but cannot reconstruct a changed catalog.
Proofs are private queue evidence, not embedded in the current training snapshot,
publisher attestations or proof that a historical catalog was globally complete.
Filesystem and producer SQLite updates do not share an atomic transaction:
later changes are detected on a subsequent inventory, not proved impossible. Unrelated task
generation and receipt-mtime changes do not change reference membership; the
normal ingestion path remains responsible for those receipt versions. Older
physical origins without recorded logical aliases cannot be retroactively
mapped by guessing. This scope needs a fresh input snapshot and separate corpus root;
old manifests remain readable. Code and offline tests alone do not deploy a
new owner or prove the production corpus changed.

The publication-basis scope `globalvoices_exact_retained_meta_declaration_v1`
allows a new corpus to annotate an old Global Voices HTML record whose entire
source batch lacks `published_time_basis`. The frozen head, immutable batch and
receipt must agree; the same requested and final publisher URL, source response
digest, HTML extraction method and exactly one matching source
`article:published_time` declaration are required. Noncandidates do not decode
raw HTML; missing or conflicting evidence remains unknown. The annotation is a
source declaration in a retained response, not evidence of historical first
publication or availability. Preserve the original clock, capture time, rights,
body and source batch. `available_at_utc` remains the actual capture time.
This output-semantic change requires a new compatible input snapshot and a
separate corpus root; old manifests remain readable. No existing corpus is
rewritten merely by changing the builder. See the [offline readback](../news_all_source_repair_2026-10-09.md).

The successor `globalvoices_and_cmnews_exact_retained_declarations_v2` also
allows a new corpus to recover a CMNews publisher declaration from an older
combined visible publication/update string. The frozen head, immutable batch,
receipt, URL, capture, body digest and original response digest must agree. The
canonical article parser must confirm the same article, title, authors, visible
publication and update minutes, absence of the known login mask and an explicitly
timezone-aware `NewsArticle` declaration. If any evidence is absent or
contradictory, preserve the old uncertainty. Decode original HTML only for
candidate records, one row at a time. The new nullable
`retained_published_declaration_raw` and `retained_modified_declaration_raw`
fields preserve the exact timezone-aware declarations separately from the old
`published_at_raw` and `modified_at_raw`; capture availability and rights are
unchanged. This is a retained source claim, not evidence that the captured body
was available historically at publication. The changed scope and schema need
a new snapshot and corpus root. Readers and verification still support the old
article schema; do not silently refresh an old root with the new scope.

## Crypto Downloader Baseline

The active crypto downloader baseline is one-minute bars.

Rules:

- Yahoo `crypto`, OKX perpetual, Bybit perpetual, and Binance USD-M perpetual
  downloaders should treat completed `1m` candles as the canonical intraday
  candle source.
- Keep canonical `daily`, `1m`, and actual `tick` data in separate directories.
  Never relabel candles or snapshots as tick events.
- Do not silently merge legacy daily or 15-minute crypto parquet rows with new
  one-minute rows. Rebuild/migrate into the dedicated `1m` path instead.
- Derive daily crypto bars from validated one-minute rows only when a stronger
  provider-native daily adjustment contract is unavailable, and persist minute
  row counts plus expected-row coverage.
- Provider statistics that are natively slower than one minute (for example
  five-minute OI/ratio series) must retain their native grain and may be
  causally carried/aligned to a one-minute decision grid; never fabricate
  one-minute observations.
- For a newly listed crypto contract in tail-only mode, anchor the lookback to
  the latest **completed** candle at or before the requested end, not the
  requested date's possibly future UTC 23:59. Preserve the official listing
  lower bound and do not mark an empty or not-yet-listed source complete. A
  Taiwan early-morning run exposed this failure simultaneously in OKX, Bybit,
  and Binance; bound forward request windows by the same completed-candle end.
- Keep stock and FX Yahoo downloads on daily bars unless the user explicitly changes those markets too.

## Feature Engineering Guardrails

Explainability indicated suspicious dependence on raw price level and raw liquidity.

Rules:

- Avoid feeding raw OHLC price levels directly when the goal is cross-stock generalization.
- Prefer log returns, relative price ratios, rolling normalization, and engineered K-line/volume features.
- If changing feature schema, update cache/versioning so stale panel caches are not reused.
- Keep `return_1d`, tradable masks, TW limit guards, and benchmark construction aligned with the canonical backtest.
- TW public snapshot-only families remain forbidden in strict/live model inputs. Never
  add them to a strict/live `data.feature_include`, model categorical-feature list,
  explainability selection, or replacement model schema: `twpub_monthly_revenue_*`,
  `twpub_cumulative_revenue_yoy`, `twpub_financial_*`, `twpub_insider_*`,
  `twpub_borrow_*`, `twpub_sbl_*`, `twpub_short_sale_available_*`,
  `twpub_tdcc_*`, and `twpub_company_*`. Their raw/source columns may remain
  available solely for provenance, auditing, and future data-quality research;
  they must not influence strict/live training, validation, test, inference, or
  feature importance. The user's 2026-09-19 all-features request explicitly
  permits these observed-date values only in the separate
  `tw_public_preopen_all_observed_research_2014_v3.yaml` research ABI. There,
  keep capture dates and missingness channels, never backdate a current snapshot
  to an earlier year, shift unproven capture-session snapshots to the next
  exchange session at a 09:00 decision, and never claim historical PIT or
  executable performance.
- The 2026-09-18 broad-history request authorizes a **separate** research-only
  table/config for obtainable current-revision macro values and historical
  MOPS XBRL facts mapped by labelled theoretical release dates. That earlier
  request did not repeal the snapshot-only denylist; the later v3 exception
  above is limited to its separate research ABI. In that v1 wide ABI, use
  `twpub_xbrl_*` columns from quarter archives, not recent
  `twpub_financial_*` OpenAPI snapshots.
  Keep the canonical strict/live feature table and checkpoint ABI unchanged.
  The wide config removes log/asinh inputs, retains ratios, carries only
  released state features, and adds per-feature availability channels before
  NaN-to-zero. Retired TIFRS concepts expire after 400 calendar days without
  a new observation; do not carry their last value across taxonomy eras.
  Event flags such as attention/disposal must never forward-fill;
  source coverage is a separate channel and pre-capture zeros are unknown.
  Current revisions and estimated dates are research approximations, not
  verified historical PIT values. See `docs/tw_public_wide_research_2014.md`.
- The 2026-09-28 request additionally permits documented historical release
  schedules in the separate `tw_preopen_release_schedule_research_v3` ABI.
  Preserve the audited strict base, original-value missingness, source receipts,
  late-upload lower bounds and explicit estimated-time/current-revision flags.
  Invalid state observations are NULL barriers, not permission to carry the
  previous value through an invalid report. This does not authorize retrocasting
  current snapshots or enabling these channels in strict/live configs. See
  `docs/tw_release_schedule_research_2026-09-28.md` and
  `scripts/build_tw_release_schedule_dataset.py` for the opt-in contract.
- The later 2026-09-28 request caps **additional safety delay** at one calendar
  day in `tw_preopen_release_schedule_research_v4_delay1d` and allows genuine
  missing-only alternate observations in a separate cross-source research ABI.
  Distinguish reporting period, estimated/known publication, safety delay and
  exchange-closed wait; a known upload replaces a guessed later deadline.
  Compare original economic keys/units before daily alignment, preserve finite
  primary observations and explicit official-conflict masks, and retain invalid
  state NULL barriers through sparse overlays. Do not equate FinMind monthly
  `create_time` with an issuer announcement (its 2026-04-21 bootstrap is not
  historical release evidence). Provider quota, unimplemented semantic mapping,
  and licensing restrictions are not proof that a value is unavailable online.
  The measured gate and unresolved scope live in
  `docs/tw_feature_gap_repair_2026-09-28.md`, not strict/live eligibility rules.
- In the separate 2026-09-28 semantic-repair research ABI, NULL is not a source
  corruption verdict. Diagnose cadence, applicability, missing report items,
  formula domains, warmup and publication history separately. TDCC was monthly
  before May 2015 and weekly thereafter; use the dated carry rule, not today's
  weekly expiry across all history. NMI's retrospective 2014 observations were
  first released on 2015-02-02 and cannot be used earlier. Preserve the one-day
  safety delay. An event absence may become zero only after reconciling the
  complete venue/day report, and financial identities require exact issuer,
  period, units, validated overlap and the latest operand's availability clock.
  Optional missing accounts are not automatically zero. Rebuild a new dataset
  and pass `scripts/verify_tw_feature_semantics.py` before refreshing the
  worklist; keep original sources, explicit conflict masks and strict/live
  configs unchanged. Evidence: `docs/tw_feature_gap_repair_2026-09-28.md`.

### TW Phase-Aware Current-Open Feature Contract

- The only model-visible session-`t` opening quote is the opt-in normalized
  feature `next_session_open_gap_logret`. Panel row `r` stores
  `log(open[r+1] / close[r])`; because phase-aware Taiwan modes keep their
  execution feature lag at one, the final row of a target-`t` window exposes
  exactly `log(open[t] / close[t-1])`.
- Do not replace this with a raw nominal opening-price feature. The normalized
  gap is invariant to stock price scale and does not let the model identify a
  symbol merely from its price level.
- This feature is opt-in and legal only with the phase-aware
  `tw_day_trade`, `tw_cash`, and `tw_overnight` execution modes.
  Empty/wildcard panel feature selection must retain the close-complete schema
  and must not silently include a next-session quote.
- No session-`t` high, low, close, or volume may enter the model window used to
  submit its opening order. Same-session full-day volume is future information.
  Execution participation limits must continue to use completed session `t-1`
  share volume as their causal proxy and keep that volume in the execution
  layer, not in the model input.
- Historical 98-feature close-complete checkpoints remain a separate schema.
  Use a new artifact root and retrain for the 99-feature open-aware model.

### Observed opening reference and last real trade

The user's 2026-10-21 definition is the observed first trade of the current
trading session; until that print is known, retain the latest already-known real
trade, including a prior session's actual close or an evidenced pre-open trade.
Retain the original trade date/time, source identity, availability bound and
carried-reference status. A known old trade can remain an input during a source
gap, but does not prove that no new trade occurred. Indicative auction quotes,
official settlement marks and missing observations are not real trades.

This is an input/reference policy, never permission to fill an order or create
volume at a stale price. The opt-in futures opening-reference ABI keeps stock
history unchanged, appends normalized reference/availability/age context, and
retains canonical post-decision execution and carrying margin accounting.
Historical minute endpoints are availability bounds, not invented tick or
receive timestamps. Use a fresh artifact/source/checkpoint identity. See
[paired futures experiments](../daytrade_research_closeout_futures_2026-10-21.md).
