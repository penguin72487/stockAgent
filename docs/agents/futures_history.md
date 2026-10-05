# Versioned stock-futures research experiments

Use when reproducing or modifying the named 08:45 stock-futures v2-v15 controls. These dated amendments preserve each experiment's evidence, approved scope and checkpoint identity; a newer paragraph does not silently migrate another config or establish current deployment readiness. New all-futures modes must use their own resolved contracts rather than inheriting an unrelated historical experiment.

The user's 2026-09-09 correction replaces whole-session source quarantine with
explicit physical-contract-day quarantine. For historical and v2-v4 controls,
`tw_stock_futures_day_trade_quarantine_contract_days` excludes
`2021-06-21 / LVF:202107`; `tw_stock_futures_day_trade_quarantine_dates` is empty.
Preserve every other candidate, original candidate slots, the complete decision
and stock-feature calendars, and raw unresolved evidence. This is retrospective
source-quality scope, not a claim about historical market eligibility. Never
infer additional exclusions from gaps or failed strategy exits. Manifest,
checkpoint, reporting, and a new artifact root must bind the exact scope.

For the 08:45 futures cash-stagnation diagnosis, a nonzero gradient norm and
Adam step count do not prove executable learning. Measure requested budgets
against whole-contract thresholds and inspect raw feature units. Row RMSNorm
does not normalize columns with different units. The bounded v4 experiment
`tw_stock_futures_day_trade_0845_gradient_v4.yaml` reuses the existing training-
only feature RMS normalizer, with saved scales and train-unseen feature masks;
it retains v3 accounting and requires a new model fingerprint/output root.
See `docs/FUTURES_CASH_POLICY_AUDIT_2026-09-09.md`: its first-fold 128-epoch
comparison restored actual train/validation trades but failed the held-out
13:30 exit contract. Do not promote this engineering result as strategy
readiness or remove the failed test contract from eligibility/quarantine.

The user's later 2026-09-09 instruction explicitly changes minute participation
capacity to `ceil(0.5 * observed_contract_volume)` in
`tw_stock_futures_day_trade_0845_capacity_ceil_v5.yaml`. Apply it to both entry
and exit minutes; zero volume stays zero. Cash affordability, contract budgets,
fees/tax and the 13:30 deadline do not round up. Keep floor as the reproducible
default for older configs and bind the new rounding to the trading fingerprint
and a new output root. Positive daily volume cannot establish zero minute
capacity under ceil: require actual minute evidence or explicitly approved
source exclusions, never silently carry floor-only proofs into the new run.
The user then explicitly approved connecting the reviewed scope proposal to v5:
add only `2023-07-13 / PZF:202308` and `2024-01-10 / LIF:202401` to the existing
`2021-06-21 / LVF:202107` exclusion. V5 uses the new
`futures_minutes_capacity_ceil_quarantine_v2_20260909` preparation and a new
`contract_quarantine_v2_capacity_ceil_v5` output root. All 1,573 decision dates,
other candidates, original observations, and unresolved source evidence remain.
Earlier controls keep their old scope. See
`docs/FUTURES_CAPACITY_CEIL_2026-09-09.md` for the measured source boundary.

The user's subsequent 2026-09-09 instruction explicitly authorizes residual
carry: attempt the existing 13:20/13:24 exits through 13:30, retain unfilled
physical contracts, and trade only the next session's target-minus-held delta.
`tw_stock_futures_day_trade_0845_carry_v8.yaml` introduced the opt-in contract;
`tw_stock_futures_day_trade_0845_carry_v9.yaml` now repairs its validity and
cash-boundary gradient. The current v8 file explicitly sets 1,000 epochs and
v9 inherits it; follow the live file or user override. Retain 50% ceil minute capacities. Do not apply the
old flat-account residual-failure gradient to it. Preserve physical identity,
daily research valuation, exact filled fees/tax, and actual official final settlement dates;
an inferred last observed trade date is not expiry. Unknown held-contract
minutes/valuations are data failures, never fabricated fills or zero returns.
The canonical daily mark can fall back to close or last-known same-contract
valuation. Disclose this approximation; do not call it a verified current-day
official settlement price. Official final settlement remains independently verified.
The optional carry evidence sidecar may add only independently audited zero
outright-volume continuation days. Validate its Parquet, receipt and raw archive
hashes; never overwrite disputed coverage or contradict observed minutes.
V9 also extracts reported daily settlements from those verified original daily
archives across the physical calendar. Zero outright trading and a valid daily
settlement can coexist (DCF:202109 on 2021-09-07: 16.75). Use same-day, same-
contract official settlement before the canonical close/last-known fallback;
never turn that mark into a minute fill. Only originally absent coverage keys
may accept extra no-trade proof; official final expiry settlement still prevails.
The v9 evidence is pinned to `futures_carry_evidence_v2_settlement_20260909`.
V9 uses receipt-backed, timely pure cash entitlements to credit/debit previous
signed positions by floor(cash per share * contract multiplier) once per event.
New event-day positions receive no old-holder credit. Other held corporate
transitions fail closed until adjusted physical identity, multiplier and minutes
are verified. Full adjusted-contract transitions and live inventory transfer
between fold models remain research limitations. Do not describe this baseline
as a complete exchange clearing model.
Retain the signed carry state through training chunks, evaluation chunks,
artifacts and prefix reports. The existing full-notional new-order budget is
a research constraint, not a historical broker-margin or SPAN implementation.
See `docs/FUTURES_RESIDUAL_CARRY_2026-09-09.md`; bounded engineering validation
does not establish profitable strategy performance or full carry-source coverage.

For v9, an unknown carry trajectory raises FuturesCarryDataError before loss
reduction/backward and coordinated DDP update. Clear accumulated gradients;
independent evaluation ranks must synchronize the data error before selection
or scalar broadcasts. Legacy data-failure return markers cannot enter financial
metrics or new artifacts. Only economic nonpositive equity retains ruin semantics.
The cash-to-first-contract surrogate uses both actual integer round-trip costs;
at zero, if both alternatives lose, its local slope is zero. This correction is
limited to an initially flat account with executable alternatives ending flat;
other states retain the declared recurrent STE, not an exact integer derivative.
Carry execution contract v4 has 72 channels; all readers use shared constants.
V8 optimizer/checkpoint state is incompatible; use a fresh v9 output root.
See `docs/FUTURES_CARRY_CURVE_AUDIT_2026-09-09.md` for current evidence.

The observed v9 full-test failure on 2024-09-24 holds PLF:202410 across
the official PLF-to-PL1 adjustment. Do not clear the corporate-transition
guard or use newly listed PLF bars for that old inventory. Minute-repair
planning must use explicit gap identities plus the official inventory, not
the policy's eligible front contracts; unknown identities must not disappear
in a join. Preserve the official final `final_settlement_value` when present:
PL1:202410 settles at value 59,923 TWD versus 29.82 * 2,000 = 59,640.
The extra 283 is embedded rights value, not a minute fill. Existing pinned
v9 sources remain unchanged. The receipt-backed PLF-to-PL1 path was repaired in
v10; v11 additionally covers reachable CLF-to-CL1 inventory from 2023-12-08.
Both use the existing signed inventory transfer, without a fill or transfer fee,
and keep adjusted inventory separate from the new standard contract. Other
unsupported transitions still fail closed. See
`docs/FUTURES_PL1_TRANSITION_FIX_2026-09-10.md` and
`docs/FUTURES_QUARANTINED_CARRY_FAILURE_2026-09-10.md`.

The user's 2026-09-10 approval explicitly permits existing signed inventory to
survive an already-declared quarantined physical-contract-day. V11 opts into
`tw_stock_futures_day_trade_quarantined_carry_policy: hold_official_settlement`:
disable every order/fill for that contract on that date, charge no trading fees,
retain its signed quantity, and mark only at the verified same-day, same-physical
official settlement. That closing mark cannot fund opening orders. The next
session resumes the original target-minus-held and scheduled exit rules.
Do not turn the unknown minute source into a verified zero-volume source, add
quarantines, or suppress other identity/valuation/corporate/expiry failures.
The existing three exact exclusions and all 1,573 decision dates are unchanged.
Carry contract v6 uses 75 channels and a new checkpoint fingerprint/output root;
preserve v10 checkpoint bytes and do not resume its optimizer under v11.
`tw_stock_futures_day_trade_0845_carry_v11.yaml` pins corporate transition bundle
`futures_corporate_transitions_v3_20260910`. CL1:202312's nine dated Tick receipts
match official outright volume and all OHLC; its final value is 77,759 TWD.
Rejected CL1 KBars remain raw evidence and never supply execution capacity.
The direct `train.py --config ...carry_v11.yaml` entry point remains canonical.

The subsequent v11 formal run completed folds 1-3 but failed at fold 4 epoch 58
on 2023-03-10 with one held RFF:202303 contract. V12 extends the same inventory
transfer with official JFF-to-JF1 and RFF-to-RF1 evidence, unchanged multipliers
and signed quantities, six exact Tick days plus two official zero-outright days,
and complete final per-contract values (JF1 1,593,063; RF1 79,653 TWD).
`tw_stock_futures_day_trade_0845_carry_v12.yaml` pins immutable
`futures_corporate_transitions_v4_20260910` and uses a new output root. Preserve
v11 checkpoints; the new source fingerprint must not inherit their optimizer.
The next actual v12 failure at epoch 120 held KGF:202311 short on 2023-11-08.
The same official transfer mechanism now covers KG1:202311 with six complete
Tick days (700 ticks) and official full final value 278,811 TWD. V13 pins
`futures_corporate_transitions_v5_20260910`, preserving all earlier evidence.
Read-only replay of epoch 119 also found 2026-09-04 / DQF:202612: the minute
archive's official-evidence manifest was stale, although the complete raw
report was present on penguin. The verified official day has zero volume and
settlement 48.6. V14 pins `futures_carry_evidence_v3_recent_20260910`, refreshing
all 8,425 missing official contract-day facts for 2026-08-12 through 2026-09-04,
while preserving all keys and no-trade-proof eligibility flags. This is actual
official zero-volume evidence, not another quarantine or a synthetic fill.
The direct `train.py --config ...carry_v14.yaml` entry point is the current
source repair, with another fresh output/optimizer fingerprint. All previous
timing, capacity, quarantine, and carry rules remain.
The complete prior-inventory audit still exposes 207 other corporate events;
retain `unsupported_pre_event_holding_contract_days` in physical metadata and
fail if actual inventory reaches an unsupported event. Passing one model path
does not prove all possible inventory transitions have sources. Never add
quarantines or silently discard those rows to make this warning disappear.
See `docs/FUTURES_FOLD4_JF_RF_REPAIR_2026-09-10.md` for measured acceptance.

The later v14 fold-5 epoch-77 failure held MKF:202409 on 2024-08-30.
The official mixed dividend/subscription transition MKF-to-MK1 retains 2,000
shares and signed quantity, adds/subtracts 8,297 TWD once for prior inventory,
and expires at full value 254,130 TWD. Bundle schema 2 binds the notice cash
amount; schema 1 retains its zero-cash-only contract. Reuse the existing cash
channel after inventory transfer, without a second credit or a transfer fill.
V15 pins `futures_corporate_transitions_v6_20260910` with a fresh output root.
All 13 reachable September-contract sessions have receipt-backed observations.
On August 30 exact KBars and ticks agree minute by minute on 314 observed lots
versus 315 official lots; the one-lot difference remains unexplained. Preserve
`mkf_observed_volume_audit.json` and its `trade_completeness: not_proven`.
The existing exact KBar observed-volume policy allows only those observations;
do not scale capacity to the daily total or weaken strict Tick equality checks.
The other 12 days exactly reconcile to official outright volume and OHLC.
No quarantine or calendar scope was added; 206 other potential corporate events
remain unsupported. See `docs/FUTURES_MKF_TRANSITION_REPAIR_2026-09-10.md`.
