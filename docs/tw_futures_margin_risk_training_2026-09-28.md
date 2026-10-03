# Fold 14 margin training: exact policy update acceptance

The latest completed v10 satisfies all three revised benchmark-relative targets:
2026 return 55.6553% > 54.5166%, Sharpe 2.53305 > 2.02897, and drawdown
12.0606% < 17.2172%. See the concise
[current configuration and acceptance report](tw_futures_fold14_benchmark_training_2026-09-28.md).
This document retains the full development evidence, including failed attempts
and superseded absolute targets.

## Requested outcome and evidence boundary

The user's latest request replaces the original absolute targets with strict
benchmark outperformance in the Fold 14 2026 test period: strategy Sharpe and
cumulative return must exceed the same-period TX front rolling benchmark, and
strategy drawdown magnitude must be smaller. The observed benchmark is return
54.516574%, canonical Sharpe 2.028971, drawdown 17.217248%. All three comparisons
must pass together. The original Sharpe >=4 / return >=100% / drawdown <=20%
target is archived in `research_target_v1_absolute.json`, not an active gate.
The pinned source covers 2026-01-02 through 2026-09-04 (163 decisions), not a
complete 2026 year. Training is 2011..2024; checkpoint selection and early
stopping use 2025 validation. The 2026 period has already been inspected during
research and must not be represented as an untouched holdout.

Formal v5 was launched with its configured 1,000-epoch cap and 100-validation-
check patience. The combined successor is v10. V6 is a configuration building
block, not a separately launched formal run. Results will be recorded from
completed canonical lifecycles in
`artifacts/analysis/tw_futures_margin_risk_training_20260928/formal_comparison.json`.

## Why a correct curve can jump

The executor owns physical quantities, capacity, fees, dated margin, official
settlement and mandatory closes. An unavailable mandatory close is absorbing
default under this research account, even when marked NAV remains positive.
Do not reinterpret it as an observed broker bankruptcy or assume unlimited
liquidation capacity. See the preceding v3 loss audit for its concrete source
event and the physical-contract backward repair.

For solvent rows, let E[t] be actual marked NAV and

    r[t] = log(E[t+1] / E[t])
    L(theta) = -252 / T * sum(r[t]).

This objective maximizes growth; it does not impose a 20% drawdown requirement.
Whole-contract choices and default boundaries are discontinuous. The declared
straight-through backward is a biased local search direction, not an exact
derivative of integer choices. An Adam proposal can therefore worsen the actual
account, including crossing a default boundary. Plot smoothing cannot repair it.

The distinction between growth maximization and explicit drawdown control is
also present in [Busseti, Ryu and Boyd (2016)](https://web.stanford.edu/~boyd/papers/kelly.html).
Their distributional probability bound is **not** a guarantee for our neural,
historical, integer-account acceptance rule.

## Combined implementation

1. Zero only the existing final affine futures action head (33 parameters at
   d_model=32). Zero scores produce exactly zero requested positions under
   `score_entmax_log_cash`; the head has a nonzero local learning path, and the
   backbone receives gradients after the first head update. This introduces no
   learnable parameter or permanent exposure cap.
2. Reuse the existing training-only RMS fitter for the 17 continuous futures
   features. It uses the selected 2011..2024 decision rows and causal candidate
   mask. Product identity remains categorical; validation/test cannot fit the
   scales. Saved normalizer buffers remain part of the checkpoint.
3. Preserve one Adam proposal per complete chronological training trajectory.
   Evaluate each proposed parameter state through the existing canonical
   full-training account, using the same evaluation kernel and chunking for the
   baseline and every candidate. Only the training split enters this decision.
4. Starting with alpha=1, try theta_old + alpha * (theta_Adam - theta_old),
   halving alpha for at most eight backtracks. Accept only a finite, non-defaulted
   path with drawdown >= -0.20 and loss no worse than the baseline apart from
   8*FP32-epsilon*max(1,abs(loss)) numerical tolerance. This is not Armijo and
   carries no convergence guarantee for the biased discrete gradient.
5. A partial accepted displacement keeps that proposal's Adam moments and its
   single scheduler advance. If all nine candidates fail, restore parameters,
   Adam state and scheduler state. Equal-loss sub-lot plateaus may advance so
   the policy can reach its first executable contract. A repeated rejection can
   still stall optimization; the existing validation early stop remains active.

The runtime executor, data, price/fee/mask logic and benchmark are unchanged.
The 20% criterion constrains the **observed training account**, not future
drawdown or a live position limit. The historical loss remains raw. Additional
`train_policy_*` fields describe accepted policy loss, drawdown, step fraction
and added evaluation time. `policy_step_trials.jsonl` retains every rejected
candidate's actual loss/default/drawdown. No rejected events are relabeled as
successful updates.

The new initialization and input scale have a new model fingerprint; the update
acceptance rule has a training-only contract version. Use the fresh v7 root.
Inactive defaults preserve old model/training fingerprints. Old optimizer state
cannot be resumed into the new experiment.

## Checks and remaining numerical question

The initial implementation passed 505 regression tests with four conditional
skips, including the existing ledger, causal input/RMS, loss, checkpoint and
trajectory tests. Separate live DDP readiness and completed-fold results are
recorded in the analysis artifact directory; unit tests are not a profitability
claim.

Two frozen real 32-day liquidation packets were also replayed at chunk lengths
1, 2, 4, 8, 16 and 32. All 12 checks retained identical whole quantities and
default reasons; the largest log-return difference was 1.073e-6. This only tests
those physical state transfers. It does not establish full-model invariance
when changing global batch size. Earlier batch-32 and batch-128 epoch-zero
policies had different full-path losses; the entire cause remains unresolved.
PyTorch documents that [batched and sliced floating-point computations need not
be bitwise identical](https://docs.pytorch.org/docs/main/notes/numerical_accuracy.html),
but that fact alone does not prove the cause of this observed difference.
The formal comparison holds global batch at 128.

Live readiness: the combined v7 completed the canonical three-epoch two-GPU
Fold 14 lifecycle with no zero-gradient epoch, no steady-state graph growth,
and no runtime fallback. Its first three epochs remained sub-lot cash and are
not strategy evidence. The steady third epoch took 9.610 s, including 3.590 s
of added full-training policy evaluation; this was a correctness smoke, not a
throughput promotion. Source snapshots, environment checks and full logs are
under the analysis root. The futures RMS fit selected exactly 2011-01-03 through
2024-12-31, with 12/17 observed channels: daily settlement-return RMS 0.01093
versus open-interest-log RMS 8.14982.

## Completed v5 control

The ordinary v5 run completed at epoch 227 after 100 validation checks without
improvement. Validation chose epoch 127 (loss -1.4639228582). The exact 2026
test returned +47.5712%, Sharpe 0.24319, Sortino 0.31754 and maximum drawdown
92.5330%. TX front 1x gross returned +54.5166%. None of the user's three targets
was reached. Twenty-six training epochs had an absorbing account default;
the raw curves and complete date-level account are preserved.

The selected checkpoint's 2025 validation return was +310.27%, but its drawdown
was already 48.10%. Optimizing only validation log growth rewarded a risk profile
inconsistent with the requested 20% limit. In 2026 its mean gross notional / NAV
was 9.26x (maximum 15.54x), even though mean requested initial-margin allocation
was 74.53%. Margin allocation and notional exposure are different quantities.
Its worst day lost 41.30%; this is an economic risk failure, not a plot defect.

The existing project's log-return Sharpe is retained for like-for-like reporting.
The explicitly named arithmetic-return companion is also shown by the auditor:
v5 simple-return Sharpe 1.4930 versus canonical log-return Sharpe 0.2432. Neither
meets 4. Do not switch formulas to make a candidate look successful.

For the fixed 163-day test, exactly doubling requires average log return
ln(2)/163 = 0.42524% per decision. At canonical Sharpe 4, the corresponding
daily log standard deviation is 1.68763%. Multiplying every log return by a
positive constant leaves its Sharpe unchanged; increased leverage alone cannot
meet both targets. Actual discrete financing, costs and default make leverage
scaling even less interchangeable with a better decision signal. These are
target arithmetic and retrospective diagnosis, not forecast performance.

## Entry point

```bash
source scripts/runtime_env.sh
run_fintech_python train.py \
  --config configs/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_risk_step_v7.yaml \
  --start-fold 14 --max-folds 1
```

## Completed V8 and finite-step interior repair

V8 completed the canonical Fold 14 lifecycle at epoch 38 through the explicit
optimizer-stagnation gate. Validation selected epoch 37. Its 2026 test returned
32.1893%, canonical Sharpe 2.35608, Sortino 3.61329 and drawdown 7.80952%.
There were no training-default epochs. Under the original absolute targets,
only the drawdown target was reached. Under the latest benchmark-relative
targets, Sharpe and drawdown pass while cumulative return still fails;
the lower return versus v5 is reported, not hidden. Raw unsmoothed curves and
the exact complete-account comparison are in `formal_comparison.png/json`.

The training risk normal pointed to 2020-01-15..2020-03-19. At epoch 38 the full
Adam proposal had 21.25898% drawdown; its projected tangent had 20.46158%.
Even tangent fraction 1/256 still had 20.05107%, above the strict 20% budget.
An infinitesimal tangent cannot guarantee feasibility for a finite parameter
step, particularly with BF16 model decisions, whole contracts and a truncated
surrogate derivative. A circular-constraint test reproduces this geometrical
failure without any finance code.

V9 adds one candidate only when the full tangent remains infeasible:

    d_interior = a - 2 * max(0, g^T a) / ||g||^2 * g.

This reflects the outward normal component inward while retaining the tangent
component. The factor two is the reflection formula, not a fitted risk penalty
or extra model parameter. Accept it only after the same whole-training replay
passes both the risk and actual-loss criteria. If it fails, keep the existing
tangent backtracking and explicit stopping behavior. Actual account feasibility
remains the authority; neither direction formula guarantees discrete feasibility.
V9 uses a new training-contract version and a fresh output root. It passed
516 tests with four conditional skips, including both rejection of the tangent
and acceptance of the reflected direction on the curved-boundary counterexample.

```bash
source scripts/runtime_env.sh
run_fintech_python train.py \
  --config configs/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_risk_interior_v9.yaml \
  --start-fold 14 --max-folds 1
```

## Latest benchmark-relative experiment: V10

V9's formal run was stopped and preserved when the user revised the requirements;
its three-epoch smoke was complete, but the interrupted formal run is not a
completed performance result. V10 combines the cash initialization, causal input
RMS, physical backward, tangent/interior candidate search and exact acceptance.

`futures_training_max_drawdown: benchmark` resolves its budget from the benchmark
in the **training-only account replay**: 2011-01-03..2024-12-31, 3430 decisions,
28.751902% observed maximum drawdown. It does not import the 2026 benchmark's
17.217248% into gradients or the optimizer. The two periods have different path
risks and serve different purposes. This replaces the old fixed training 20%
setting; it does not assert any test-period drawdown guarantee. Zero/invalid
benchmark drawdown fails closed. Checkpoint selection remains 2025 log loss.

The source and training contract changed in a fresh v10 root; old optimizer
state is not resumed. `train_policy_drawdown_budget` and
`train_benchmark_max_drawdown` expose the resolved budget each epoch. Regression:
524 passed, four conditional skips, including unusable-benchmark rejection and
forward-fingerprint preservation. The comparison script independently computes
test benchmark drawdown and requires bit-identical benchmark dates/returns
across completed runs before making the three strict comparisons.

```bash
source scripts/runtime_env.sh
run_fintech_python train.py \
  --config configs/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_benchmark_risk_v10.yaml \
  --start-fold 14 --max-folds 1
```

Do not start a duplicate while the agent-managed formal run is active.

## V7 observed stall and V8 direction repair

V7 was stopped at epoch 40 after identical rejected updates in epochs 31..40.
Its accepted training loss stayed -0.15124901599 and training maximum drawdown
stayed 19.98546115%. Every epoch replayed nine infeasible/worse Adam steps and
restored the same parameters, moments and scheduler. With no stochastic input,
that restoration necessarily repeats the same proposal. V7 is an interrupted
diagnostic, not a completed strategy result; its checkpoint, raw curve and stop
receipt remain preserved. The changed optimizer starts in a fresh v8 root.

For a fixed peak p and trough q of a solvent account, log drawdown magnitude is

    D(theta) = log(E[p]/E[q]) = -sum(r[p:q]).

The risk probe reuses the full chronological DDP training pass and the existing
physical-contract straight-through derivative. It gives return rows p:q weight
one and other rows zero, while retaining EVERY day's execution/state advance.
A stateless zero-step SGD object collects the VJP without touching Adam. The
common annualization/mean/gradient-clip scaling cancels out of the projection.
As in the original trainer, gradients are truncated at global batch boundaries;
this is a local surrogate normal, not an exact long-horizon integer derivative.

Let a be Adam's proposed parameter displacement and g the risk normal. The
closest direction satisfying the linearized nonincreasing-risk condition is

    minimize_d 0.5 * ||d-a||^2, subject to g^T d <= 0
    d = a - max(0, g^T a) / ||g||^2 * g.

This is a half-space projection, related to the standard constrained-gradient
construction in [Stanford's optimization lecture](https://web.stanford.edu/class/msande211x/lecture12.pdf).
The neural integer-account problem is not convex: this formula proves only the
local projection, not global convergence or future risk bounds. No additional
learnable parameter or penalty coefficient is introduced.

V8 first checks the full Adam proposal. If it violates account feasibility,
compute the normal at the old feasible policy, remove its outward component,
and backtrack the projected displacement. Every acceptance still requires a
full exact training replay, no default, drawdown <=20%, and non-worsening actual
log loss. All attempted directions, fractions, peak/trough rows and measurements
are retained in `policy_step_trials.jsonl`. A zero/inward risk normal falls back
to ordinary Adam backtracking. If the finite search is exhausted, terminate
through the canonical final reporting lifecycle with `optimizer_stop.json`;
do not waste 100 identical rejected epochs or fake validation improvement.
The configured validation early stop remains active as a separate condition.

V8 passed 514 regression tests with four conditional skips and strict dual-GPU
CUDA readiness. New cases prove boundary-direction escape, first-day drawdown,
DDP tail-weight alignment, rollback on nonfinite normals, and equality of the
weighted-loss VJP to the complete physical account (including positions created
before the selected drawdown). A full-data DDP smoke and formal results are
recorded separately below when completed.

```bash
source scripts/runtime_env.sh
run_fintech_python train.py \
  --config configs/markets/tw_futures_v8_margin_verified_2011_capital100m_tx_front_roll_risk_tangent_v8.yaml \
  --start-fold 14 --max-folds 1
```
