# no_qk_norm CPU staging regression, 2026-09-22

The verified final change reduces steady complete epochs from **279.401 s to
67.305 s (4.151x, 75.91% less wall time)**. The formal 1,000-epoch experiment was
not restarted. Its epoch-90 checkpoint hash is unchanged.

The inspected run is
`artifacts/ablations/tw_day_trade_last_last_only_v8_twpublic_248d0869_ofat/no_qk_norm`.
The abrupt increase around epoch 79 is in validation/test preparation. The
training phase remains around one minute. A model-forward-only optimization
would therefore miss the main regression.

## Cause and correction

The launcher's thread message was not the actual trainer setting. On this host,
Numba's first parallel panel operation initializes the shared OpenMP runtime
to 224 threads, overwriting the Torch intra-op setting. A fresh-process probe
reproduced `torch.get_num_threads(): 1 -> 224`; a second call after explicitly
restoring 56 stayed at 56. Added trainer telemetry also observed 224 despite a
requested 56 (or 1) per rank. Initialize and mask Numba's pool before restoring
the resolved Torch budget in `train.py`, respecting a smaller explicit Numba
maximum. Cold-process tests exercise both smaller and larger Numba maxima.

`PreparedDayTradeCarryBatch.from_packed_sessions` padded each day's three ragged
event fields with separate `Tensor.new_full`, `torch.cat`, and `torch.stack`
calls. With the inflated CPU pool, repeated dispatch/allocation dominated a
whole validation year while the GPU waited. The reproduced control
spent about 220 seconds on validation and 202 seconds on test in steady epochs.
The two evaluations run on different ranks; these times must not be added.

For immutable CPU FP64/int64 transport, allocate each complete padded field
once with NumPy and copy its rows. Stack dense fields with the same CPU copy
path. Preserve exact bytes, dtype, shape, padding sentinels, event ordering,
source ownership, and the dense CUDA reconstruction. Keep the original tensor
path for other devices/dtypes and tensors requiring gradients. Neither the
account recurrence nor the compiled kernel is changed.

Source binding is now counted in backtest preparation rather than the ledger
runner. Startup timing also records actual intra-op/inter-op threads and CPU
affinity. The old run did not record enough runtime information to prove which
historical thread/affinity change triggered its epoch-79 discontinuity.

The final CPU prefix/stitched replay is a separate cost. It now uses one
intra-op thread within those CPU-only functions and restores the process's
previous setting even after a source exception. Training/GPU evaluation thread
settings are unchanged. A bounded 2-day, 2,754-symbol probe preserved every
returned NumPy array bit-for-bit between 1 and 56 threads; its 0.896/3.296-second
timings are diagnostic only, not an isolated full-fold speed claim.

## Verification protocol and limits

The benchmark root is
`artifacts/benchmarks/no_qk_norm_eval_20260922`.
Both measured training runs use the same configured initialization, pinned
source, Fold 11, 2,653 training rows, 2,754 symbols, global batch 32, BF16,
two RTX 5090 GPUs, and a requested 56 intra-op threads per rank. The final runtime
fix must prove that this requested count survives panel loading. Four epochs retain all
validation, test, curves, checkpoints, and final artifacts. Compare epochs 3-4,
requiring zero new compiled graphs and matching loss/gradient/optimizer states.

The unpatched control completed four epochs, then its CPU artifact replay
exceeded 20 minutes. It was resumed with one intra-op thread, using the saved
original transport implementation, to finish every artifact without additional
optimizer steps. That reporting-only resume has pre-existing differences: it
skips the training feature-cache/compile setup and reapplies the fitted RMS
normalizer before rewriting `checkpoint_last`. Its best checkpoint remains
identical to the candidate, but its final metrics differ. Therefore exclude
those resumed final artifacts from equality claims. This observation is saved
in `report_resume_observation.json`; this performance patch does not change
normalization or reporting-only resume semantics.

A further run, `control_cpu1`, starts fresh with the original transport and
one intra-op thread per rank, retaining the full training setup and four epochs
without interruption. Use it to compare checkpoint states and final artifacts;
require all of its epoch metrics to equal both main runs as well. The earlier
`control_complete` attempt exposed the Numba thread reset and was stopped before
it could serve as a low-thread control. The epoch speedup denominator remains
the original control with the **same requested thread budget**, never this
additional correctness control. Both compared final CPU artifact sets use one
thread. Do not claim an exact full-fold speedup or full-source high-thread-versus-1-thread
CPU artifact equality from this comparison.

`compare.py` checks completed lifecycle receipts, exact per-epoch financial and
gradient values, model/optimizer/scaler/scheduler/RNG checkpoint states, and
every array in matching backtest NPZ inventories. `comparison.json` is the
machine-readable verified result.

## Measured result

Median of epochs 3-4, same requested CPU budget, global batch 32, BF16 and
two RTX 5090 GPUs; neither steady epoch created a new compiled graph:

| Phase | Original control | Final correction |
|---|---:|---:|
| Complete epoch | 279.401 s | 67.305 s |
| Training | 58.729 s | 61.826 s |
| Validation | 220.374 s | 5.206 s |
| Test curve | 201.809 s | 3.444 s |

Validation and test run on different ranks, so their times are not additive.
The gain is removal of evaluation overhead; the training phase remains about
one minute and was slightly slower in this bounded comparison.

The final candidate and uninterrupted original-transport correctness control
both pass the complete fold lifecycle (18 checked artifacts each). All four
epochs have exactly equal loss, gradient telemetry and learning rate. Model,
optimizer, scaler, scheduler and RNG checkpoint tensors match byte-for-byte.
Final metrics match, and all **105 arrays across three backtest NPZ files**
match dtype, shape and bytes. The timing control's best model also matches.

Validation also includes strict CUDA readiness, 358 passing focused/runtime/
loss/backtest tests (18 skipped), Python compilation, and `git diff --check`.
The root-cause cold-process tests confirm both a smaller explicit Numba limit
and restoration of Torch's requested count after the first real panel kernel.

The original epoch-90 checkpoint and the formal 1,000-epoch experiment settings
are preserved. A four-epoch engineering comparison is not a completed ablation
suite or evidence of improved strategy returns.
