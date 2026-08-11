# Unified training mode adapter architecture

`stockAgent` has one outer training lifecycle and multiple market/execution
adapters.  Product identity or sampling frequency must not create another copy
of optimizer, AMP, scheduler, checkpoint, epoch-curve, plotting, or progress
code.

## First-principles boundary

A training system has four irreducible inputs:

1. **Information clock**: which raw fields are observable when a decision is
   made, how a causal window is formed, and which samples own each split.
2. **Action contract**: the model output shape and its interpretation (stock
   weights, TX exposure, TXO Call/Put contracts, auction channels).
3. **Execution state machine**: fills, fees, liquidity, inventory, margin,
   settlement, forced exits, and absorbing failure.
4. **Optimization/reporting lifecycle**: device, AMP, model construction,
   optimizer, scheduler, epochs, resume, progress, validation/testing, curves,
   checkpoints, plots, and completion markers.

Only the first three depend on the traded product or decision frequency.  The
fourth is invariant.  Therefore a new product is an adapter plugged into the
same lifecycle, not a copied trainer with similar-looking logs.

This is also the safety boundary.  Sharing the lifecycle cannot make a
right-labelled one-minute KBar legal at its own open, cannot turn TX margin
into stock T+2 settlement, and cannot give a force-flat strategy meaningful
terminal concentration.  Those facts remain inside the native adapter.

## Stable lifecycle protocol

`stockagent/training/lifecycle.py` is the product-independent protocol.  Every
neural runner now uses:

- `TrainingArtifactLayout` for root, train-group, and fold paths;
- `TrainingRunLifecycle` for atomic `run_manifest.json` and `progress.json`;
- `normalize_epoch_curve_record` for one flat epoch schema;
- `canonical_mode_artifact_contract` for comparable report semantics;
- `validate_completed_training_artifacts` as a machine-checkable handoff gate.

The external stage machine is fixed:

```text
setup -> training -> validation -> testing -> reporting -> complete
  \-----------------------------------------------------------> failed
```

Modes may revisit training/validation for each epoch and may omit an actual
test calculation during an intermediate epoch, but they do not invent new
top-level phase names.  Mode detail belongs under metrics or native audit
tables.

The stable artifact tree is:

```text
<output>/
  run_manifest.json
  progress.json
  summary.json
  startup_timing.jsonl
  train_<ownership-key>/
    checkpoint_last.pt
    epoch_curve.jsonl
    pre_epoch_timing.jsonl
  fold_XX/
    checkpoint_best.pt
    fold_complete.json
    mode_artifact_contract.json
    metrics.json
    model.pt
    test_backtest.npz
    deployment_test_backtest.npz
    annual_report.txt
    equity_curve.png
    equity_curve_log.png
    annual_performance.png
    plot_timing.json
```

Annual modes use `train_2020-2021`; day-count folds use a date ownership key
such as `train_20260102-20260630`.  This changes the key, not the files inside
the group.

`progress.json` always retains the same envelope: run identity, phase, group,
fold, epoch, work count/unit, last sample, metrics, failure, and timestamps.
It never replaces that envelope with a mode-only object.  Intraday batch
updates are atomically persisted at most once per second (plus phase/final
updates), so monitoring does not become a training bottleneck.

`epoch_curve.jsonl` always has `epoch`, `train_loss`, `val_mean`, `test_mean`,
`lr`, `no_improve`, `best_val_loss`, `improved`, and `epoch_total_s`.  A mode
may append prefixed flat fields such as `minute_cache_gib`; it cannot replace
the core with a nested or differently named record.

Completion is a checked state transition, not a final log message.
`TrainingRunLifecycle.complete()` requires the completed fold set to equal the
manifest's selected fold set, writes the candidate complete envelope, and then
runs the shared artifact conformance gate.  The gate verifies non-empty
required files, run/progress/summary/fold identity, every epoch-curve row,
canonical backtest ZIP members, and PNG signatures.  If any check fails,
`progress.json` is atomically returned to the same envelope with
`state=failed`; a partial run is never left advertised as complete.  This is a
lightweight structural check: it does not import a model, execute a checkpoint,
or decompress large backtest arrays.

## Shared outer lifecycle

The canonical numerical implementation remains in
`stockagent/training/trainer.py`; the stable observable protocol is in
`stockagent/training/lifecycle.py`.  Specialized runners reuse both. Shared
responsibilities are:

- device and BF16/FP16/TF32 resolution;
- AdamW and LR scheduler construction;
- GradScaler policy (FP16 only);
- compile availability/options/fallback policy;
- safe atomic, weights-only checkpoints and RNG restore;
- train-group `checkpoint_last.pt` and `epoch_curve.jsonl` ownership;
- `_EpochCurveLifecycle` resume trimming, interval requests, async coalescing,
  deferred plotting, and final flush;
- `FoldResult`, standard return artifacts, annual reports, equity plots,
  walk-forward summaries, and completion markers;
- durable progress and mode-artifact contract JSON.

TX/TXO tick checkpoints use the same canonical group/fold checkpoint writers
as daily and minute modes.  Tick-only normalizer, feature, source-manifest, and
execution versions live under `tw_index_derivatives_tick_state`; they do not
reimplement model/optimizer/scaler/scheduler/RNG serialization.  The loader
retains read compatibility for the earlier schema-4 tick envelope.

`stockagent/training/mode_adapter.py` is the central registry and specialized
dispatch layer.  Every canonical execution mode must have exactly one
`TrainingModeSpec`; import-time coverage fails if it differs from
`backtest.tw_execution.EXECUTION_MODES`.

## Mode-only adapter responsibilities

An adapter owns only semantics that cannot be shared safely:

- source manifest and point-in-time data validation;
- sample/fold ownership when it is not the annual panel contract;
- feature window materialization for its event clock;
- model-output interpretation (stock weights, one TX exposure, Call/Put legs);
- native execution state and accounting;
- terminal flatten/default rules;
- conversion of exact native day results to the common reporting container;
- a namespaced dataset/execution/config fingerprint.

Do not make `tw_minute` call the daily open-to-close executor, and do not make
TX/TXO use cash-stock fee or settlement rules merely to share code.  Share the
lifecycle around the native ledger, not the ledger formula.

## Registered contracts

| Mode family | Product | Frequency | Native state | Runner |
|---|---|---|---|---|
| `naive`, `tw_cash`, `tw_day_trade`, `tw_overnight` | configured market or Taiwan stock/ETF | configured/daily auctions | canonical portfolio/T+2 state | `trainer.run_training` |
| `tw_index_futures_day` | TX/MTX/TMF | daily session | one index exposure | `trainer.run_training` with futures adapter |
| `tw_minute` | all Taiwan stocks/ETFs | completed 1m KBar | within-session cash/inventory | `training.minute` |
| `tw_index_derivatives_tick` | TX | completed public second | within-session TX exposure | `training.index_derivatives_tick` |
| `tw_index_options_tick_long/short` | TXO Call/Put | completed public second | premium/margin Call/Put account | shared derivative tick runner |

The registry records product family, frequency, decision clock, first execution
clock, recurrent-state scope, terminal policy, split ownership, benchmark,
sample-order contract, weight-snapshot semantics, and turnover unit.  Run manifests persist these
fields so reports cannot silently reinterpret one mode as another.

For `tw_minute` and TX/TXO tick modes, the session sample order is strictly
chronological. Training may batch adjacent independent sessions for compute,
but it must not shuffle the day axis. The progress axis is processed sessions,
not the calendar value of the most recently completed batch; `batch_date` is
shown only as an audit field.

## Reporting contract

Universal plots consume net daily log returns and dates.  Mode adapters must
also declare what benchmark, weight snapshot, and turnover mean:

- minute stocks use the configured benchmark from the first possible strategy
  execution through session close; weight history is the realised portfolio
  immediately before mandatory flatten; turnover is traded notional divided by
  daily initial equity;
- derivative tick modes currently use cash as the explicit return benchmark;
  terminal daily weights are zero because they force-flat, while intraday
  fills/margin/unfilled quantities remain in the native daily-curve parquet;
  TX turnover is normalized exposure change and TXO turnover is contract count.

Cross-mode dashboards must read `mode_artifact_contract.json` before comparing
turnover or concentration.  Equal field names do not imply equal units.

## Adding a product or frequency

1. Add the canonical execution name/aliases in `backtest.tw_execution`.
2. Add one `TrainingModeSpec`; registry coverage must remain exact.
3. Write the raw-event, decision, first-execution, holding, terminal,
   unavailable-data clocks, and sample-order contract before implementation.
4. Select an existing runner by sample ownership: annual panel, stock minute,
   or derivatives tick.  Add another runner only if recurrent state or fold
   ownership truly cannot fit one of them.
5. Instantiate `TrainingRunLifecycle`, use `TrainingArtifactLayout`, and reuse
   canonical AMP, optimizer, scheduler, compile, checkpoint, curve, progress,
   and fold-output helpers.
6. Persist training and reporting fingerprints separately so a report-only
   benchmark change does not silently reuse stale plots or unnecessarily resume
   an optimizer under a changed execution contract.
7. Add causality, accounting, gradient, permission, checkpoint, registry, and
   old-mode regression tests.  The runner's final lifecycle transition invokes
   `validate_completed_training_artifacts(...)`; also assert it directly in the
   end-to-end smoke test so the artifact contract is visible in test failures.

Adding `minute_progress.json`, `checkpoint_latest.pt`, a new nested curve
format, or another terminal progress renderer is a failed abstraction: the
new adapter has leaked product identity into the invariant lifecycle.
