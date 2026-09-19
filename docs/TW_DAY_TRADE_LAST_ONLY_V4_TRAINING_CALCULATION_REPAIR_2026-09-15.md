# TW day-trade last/last_only v4 training-calculation repair

## Conclusion

The completed v4 artifact is not truncated and its exact physical account did
not default.  Its canonical stitched result is `+508.11%`, Sharpe `0.756`, and
maximum drawdown `-29.58%`, versus the report's 2330 buy-and-hold comparator at
`+2133.39%` and Sharpe `1.101`.  The comparator includes close-to-close
overnight exposure and omits the strategy's daily execution costs/capacity, so
it remains an opportunity-cost reference rather than an attainable same-day
execution baseline.

The actionable defect is a train/evaluation policy-cadence mismatch.  The
physical FIFO state continues across batch boundaries, but v4 performed AdamW
after every batch.  Therefore batch `b+1` started from an account produced by
`theta_b` and was valued by `theta_(b+1)`.  One epoch was a path through 7 to 83
different policies, while validation and inference replay one fixed checkpoint.
The result also depended on an otherwise arbitrary batch boundary.

For decomposable log utility with truncated recurrent state, the intended
gradient is

```
L(theta) = sum_b (valid_rows_b / valid_rows_total)
                 * L_b(theta; stop_gradient(state_b))
```

All chunks must use the same `theta`; only after the weighted sum is
back-propagated may AdamW and the step scheduler advance.  The repair exposes
that cadence for the physical Taiwan day-trade account and records it in the
checkpoint contract.

## Controlled v5 change

`configs/deployments/tw_day_trade_last_last_only_training_vastai1t_v5.yaml` is
the immutable fresh-root v5 calculation experiment. Relative to the immutable
reproduction config `tw_day_trade_last_last_only_training_vastai1t_v4.yaml`, it
changes only:

- `day_trade_optimizer_step_per_trajectory: true`;
- scheduler warm-up from 256 batch updates to 32 trajectory updates;
- the original `early_stopping_no_improve_ratio: 0.1`, unchanged from v4;
- an exact flat-cash checkpoint floor: a transferred policy that does not beat
  zero loss cannot seed `checkpoint_best`; its final scalar score layer is
  zeroed only while that analytical checkpoint is written, then the complete
  transferred state is restored before epoch 1;
- the artifact root, so no v4 optimizer state can be resumed.

It deliberately preserves v4's FinancialTransformer ABI and initialization:
`last`, `last_only`, LayerNorm, raw-feature temporal bases, and non-exact partial
pretrained transfer.  It also preserves all 99 features, TWD 10M capital, BF16,
global batch 32, 1000 epochs, 50% minute capacity, fees/tax, margin/borrow rates,
daily proxy policy, and exact physical-account forward loss.

The scheduler repair matters because v4's 256 optimizer-step warm-up represented
36.57 epochs in fold 1 but only 3.08 epochs in fold 11.  V5 has one update per
trajectory, so every fold has the same 32-epoch warm-up before the unchanged
10%-patience early-stop rule is applied.  The v4 receipts also show that 9 of 11 transferred epoch-zero
validation accounts lost money relative to exact flat cash despite remaining
solvent; they are no longer eligible as best checkpoints.

The completed artifact provides the following direct evidence.  `batches` is
also the number of AdamW steps per v4 epoch; `stop` is the actual last epoch,
not the configured 1000:

| Fold | batches | stop | best validation epoch | epoch-0 loss | epoch-0 equity |
|---:|---:|---:|---:|---:|---:|
| 1 | 7 | 117 | 17 | +0.302090 | 0.746396 |
| 2 | 15 | 130 | 30 | +0.062235 | 0.941520 |
| 3 | 22 | 102 | 2 | +0.082036 | 0.923040 |
| 4 | 30 | 119 | 19 | +0.399256 | 0.676154 |
| 5 | 38 | 139 | 39 | +0.061283 | 0.942847 |
| 6 | 45 | 119 | 19 | -0.037369 | 1.036999 |
| 7 | 53 | 156 | 56 | +0.050582 | 0.952204 |
| 8 | 61 | 125 | 25 | +0.291098 | 0.752641 |
| 9 | 68 | 153 | 53 | +0.382414 | 0.695805 |
| 10 | 76 | 138 | 38 | -0.198846 | 1.210406 |
| 11 | 83 | 101 | 1 | +0.045699 | 0.956890 |

The temporary reset is essential rather than cosmetic.  In the exact
whole-share account, zero requested weights are below every board-lot threshold,
so the executed position and exact loss are locally constant and the policy
gradient is exactly zero.  Permanently starting stock training from a zeroed
head would therefore create an absorbing no-trade state.  V5 keeps flat cash
only as the validation/checkpoint floor and trains from the nonzero transferred
v4 policy.  The dedicated continuous futures head retains its separate proven
trainable flat-reset path.

## Remaining bounded approximation

The forward loss and account state remain exact across every batch boundary,
but autograd intentionally stops at the carried FIFO state.  This is truncated
BPTT, not a surrogate return or proxy loss.  V4 artifacts contain positions
held across a 32-row boundary and one observed carry duration reaches 235
sessions, so a future full-state-gradient experiment could still change
optimization.  It is not folded into v5 without measurement: exact full BPTT
would retain or recompute the model and ledger graph over the complete carry
horizon and needs a separate memory/throughput/parity acceptance test.  V5
repairs the proven parameter-cadence defect while leaving this limitation
explicit instead of silently substituting a differentiable proxy.

## Active v6 learned-cash output correction

Across all 11 v4 `test_backtest.npz` files, 15,224 recorded requested rows had
median gross `0.999999866`, mean gross `0.999999753`, and 96.41% were within
`1e-6` of full gross. The corresponding executor-realized median gross was
`0.0` and mean gross was only `0.01362`. This directly confirms that almost all
reported cash was produced downstream by executable-account constraints rather
than selected by the policy.

The unversioned deployment config now selects a fresh-root v6 successor. The
v5 `projection_l1` policy received O(1) scores for roughly 2,754 stocks, so its
unconstrained gross grew as O(S). Almost every row was therefore projected to
the unit-L1 boundary: cash was usually created only later by masks, board-lot
rounding, funding and minute capacity. That is an output-contract defect, not
evidence that the policy chose full investment.

V6 uses `portfolio_output_mode: learned_cash` and disables cross-sectional
score centering. For active stock logits `z_i` and an independently learned,
zero-initialized contextual cash logit `c`, it computes

```
d_i  = z_i / sum_j |z_j|
cash = sigmoid(c)
w_i  = (1 - cash) * d_i
```

The all-zero score vector is defined as exact flat cash. Otherwise
`sum(abs(w)) + cash = 1`, while long gross, short gross, net, total gross and
cash remain model outputs. Replicating an otherwise identical candidate
universe only splits directional weights; it does not change gross. Arbitrary
stock-score scale also cannot force gross. This path is O(S), has no sort or
projection threshold, and preserves the exact execution ledger as the
authoritative forward loss. Settlement reports now record requested gross/cash
separately from realized gross, so executor-created unfilled cash is not
mislabelled as model intent.

The v6 config inherits the old 22-family artifact's exact per-family retained
component map (524 components total) and novelty threshold `1e-4`. It preserves
the current LayerNorm/raw-feature input ABI rather than changing the backbone
to the old artifact's RMSNorm/input-feature ABI. This isolates the output
correction while keeping the same redundancy control.

Historical `projection_l1` and `cash_l1` implementations remain callable only
to replay old checkpoints exactly. They are retired from new single-target
Taiwan stock day-trade training; deleting their readers would make existing
artifacts non-reproducible.

On an idle RTX 5090, a bounded BF16 forward+backward microbenchmark at the
formal output shape (`B=32`, `S=2754`, 128 steady iterations) measured the old
projection at 1.258 ms eager / 0.490 ms compiled and learned-cash at 0.777 ms
eager / 0.468 ms compiled. This is a 1.62x eager and 1.05x compiled output-layer
speedup. It is not a claim about full-epoch throughput, which remains dominated
by the FinancialTransformer and exact physical FIFO ledger.

## Main-worktree consolidation

The formal path is now `/root/stockAgent`.  The source and minute caches were
moved there, and the active config contains no reference to
`/root/stockAgent-daytrade-training-20260910`.

The main tree now contains the exact source/cache ABI
`tw_day_trade_physical_source_cache_v11_share_replacement_session` and session
ABI `tw_day_trade_physical_fifo_sessions_v5_pending_stock`, including verified
share-replacement handling, pending stock delivery, event-compressed execution,
and its eager audit fallback.  Sparse event reduction remains disabled because
it changes FP64 reduction order.

## Validation and formal command

The strict data gate accepted 3,096 sessions, 2,754 symbols, 99 features, all 11
folds, 2,798,943 minute symbol-days, 2,986,868 daily-proxy symbol-days, and zero
source-gap symbol-days.  It explicitly reported that no model, optimizer,
checkpoint, or fold-completion marker was started.

```bash
cd /root/stockAgent
source scripts/runtime_env.sh

run_fintech_python scripts/check_environment.py --require-cuda --strict
run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_last_last_only_training_vastai1t.yaml \
  --check-data-only
run_fintech_python train.py \
  --config configs/deployments/tw_day_trade_last_last_only_training_vastai1t.yaml
```

CPU/config/integration validation passed 309 relevant cases with three
environment-gated cases skipped.  Both batch-cadence and trajectory-cadence
real two-GPU NCCL/DDP cases then passed separately under `torchrun`.  Another
optional test module could not be collected because `numba` is not installed in
the fintech environment; the formal PyTorch/CUDA path does not use that optional
module.  Strategy outperformance is not established by these engineering tests
and requires completion of all 11 walk-forward folds.

For the v6 output correction, 272 focused normalization, model-gradient,
pretrained-transfer, checkpoint-contract, explainability, and deployment-config
tests passed. A separate broader exact-ledger regression run passed 401 cases
with three skips. Its repository-wide config sweep still reports four unrelated
experimental candidate YAMLs containing already-removed prefetch/cohort/sparse
event keys; the active v6 config itself loads and its complete effective
contract was checked. No formal fold training was started for v6.
