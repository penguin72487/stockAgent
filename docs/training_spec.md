# Training And Evaluation Contract

This document describes the stable system contract. A resolved market config is
an experiment snapshot; its model, dates, batch sizes, and execution mode are not
global defaults. When sources disagree, use this order of authority:

1. point-in-time and accounting contracts in `AGENTS.md`;
2. the resolved YAML configuration and its checkpoint fingerprint;
3. the canonical implementation and contract-version constants;
4. this overview and mode-specific documentation.

## First-principles model

A trading result is meaningful only if these questions have explicit answers:

1. **Information:** what values were observable before each decision?
2. **Causality:** which target row owns a lookback window, and can that window
   read any future or unannounced value?
3. **Mechanism:** how do actions become fills, fees, taxes, borrow, settlement,
   and recurrent portfolio state?
4. **Latency and deployability:** can the same feature and model path run by the
   required deadline, using data actually available then?
5. **Reproducibility:** can a checkpoint be tied to its config, data universe,
   fold, random state, and artifact schema?

Model architecture and headline return come after these gates.

## Data and time

The canonical panel represents observations as dense tensors plus point-in-time
masks. Typical shapes are:

- base features `[date, symbol, feature]`;
- model windows `[batch, lookback, symbol, feature]`;
- ordinary actions `[batch, symbol]`;
- phase actions `[batch, phase, symbol]` for multi-auction modes.

Panel construction owns schema normalization, symbol/date alignment, labels,
feature availability, and execution masks. Missing permission evidence must not
be forward-filled into permission. Data snapshots, official-download receipts,
and coverage audits remain part of the input contract, not incidental caches.

## Walk-forward ownership

`stockagent.data.walkforward.build_expanding_year_folds` constructs expanding
training folds. Validation selects checkpoints; test data does not select them.
Every target belongs to exactly the split specified by the fold.

The canonical `lookback_context: panel_history` policy owns targets from the
first eligible date of every split and may read earlier, already-observed panel
rows for their causal feature windows. It does not import earlier returns,
portfolio state, or future information. `split_only` remains available only as
a legacy reproduction mode and drops the first `lookback - 1` targets per split.

If `require_future_test_year: false`, the final fold may intentionally reuse its
validation window as its test window. Such output must be labelled latest-period
experimentation, not unbiased model selection.

## Execution modes

Execution mode is a semantic boundary, not a reporting label. The registry in
`stockagent/training/mode_adapter.py` determines the supported modes and whether
they use the generic or specialized runner. Important families include:

- ordinary close-to-close portfolios (`naive`);
- Taiwan OPEN/CLOSE carrying modes (`tw_cash`, `tw_overnight`);
- open-to-close day trade (`tw_day_trade`);
- intraday minute decisions (`tw_minute`);
- Taiwan index futures and TAIFEX tick/options modes.

Each mode must define its decision clock, executable price clock, permissions,
fees/taxes, volume and borrow limits, settlement, and recurrent state. Never
flatten a phase action into an ordinary signed exposure merely to reuse an API.

## Model, loss, and backtest alignment

`stockagent.models.factory.build_model` is the model construction boundary.
Neural models should emit raw scores or the explicitly configured portfolio
representation. Tradability is applied before allocation.

`stockagent.training.loss.risk_aware_loss` and
`stockagent.backtest.simulator.run_backtest_torch` are the canonical return path
for training and evaluation. Model output mode, loss activation, long/short
intent, and inference post-processing must agree. A compatibility alias is not
evidence that two economic contracts are equivalent.

Fees and recurrent state are part of the objective. Cross-batch state carries
the previous executed, mark-to-market portfolio and absorbing alive/default
state; it is detached from the prior optimization graph. Do not fork separate
train, validation, test, and live return formulas.

## Precision and performance

CUDA runs use the configured autocast type. The recommended baseline is BF16
autocast with FP32 parameters and numerically sensitive reductions. `GradScaler`
is enabled only for FP16. Fixed-shape panel slabs, `torch.compile`, DDP, caching,
and chunking are performance representations; they must remain numerically and
semantically equivalent to the eager canonical path.

Measure whole steady-state epochs, including validation, sampled test, plots,
checkpointing, and artifact work. A fast kernel is not a fast or deployable
experiment by itself.

## Checkpoints and artifacts

Every neural runner uses `stockagent.training.lifecycle` for the outer artifact
contract:

- root `run_manifest.json`;
- fixed-envelope `progress.json`;
- flat `epoch_curve.jsonl`;
- fold `mode_artifact_contract.json`;
- canonical `checkpoint_last.pt`, `checkpoint_best.pt`, and completion marker.

Checkpoint manifests bind semantic configuration, data/fold identity, universe,
RNG state, and the canonical backtest contract version. Incompatible accounting
or sample-order changes may load weights for explicit inference, but must not
silently resume optimizer state.

## Verification gates

Use progressively stronger evidence:

1. syntax and correctness lint;
2. focused unit tests for the touched contract;
3. checkpoint/resume and artifact validation;
4. eager/compiled and single-device/DDP parity where applicable;
5. a complete epoch or mode smoke test using real panel shapes;
6. the full formal test suite before declaring the repository green.

Feature validation, live deployability, and investment merit are separate
conclusions. Passing one does not establish the others.
