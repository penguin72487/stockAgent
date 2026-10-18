# Full-workflow performance and measured observations

Use for compile, cache, batch, throughput or hardware investigations. Measure the complete requested workflow. Timings and tuning values below are observations of the named shape, hardware and experiment; they are starting points, not requirements for unrelated configurations. For vastai1T TW day-trade acceptance use two-GPU DDP, the formal global batch, the complete fold lifecycle and steady epoch 3+ maximum-rank wall time as specified in runtime.md. Epoch 2 observations below are historical or diagnostic. No benchmark result authorizes changing accounting or silently skipping work.

## Numerical and optimizer experiments

For semantic-preserving work, use the selected account's equality/gradient oracle.
An explicitly selected precision, batch, kernel or gradient experiment may instead
change continuous outputs and optimizer trajectory. Declare its numerical tolerance,
changed axes and reference; compare nonempty trades, finite gradients and the full
requested workflow. Preserve source values/units/clocks, fee and settlement formulas,
whole-lot legality, masks and absorbing ruin. At fixed source/actions, test the
account's discrete fields and financial recurrence; a changed model trajectory is
not expected to produce an identical historical trade list or checkpoint.

Record matching version/fingerprints and a new artifact root for incompatible
training changes; retain the old comparator/results and reject old optimizer resume.
The user's 2026-10-06 and 2026-10-12 numerical permissions apply to their named
performance experiments, not every historical replay. See
[batch/precision evidence](../tw_daytrade_no_basis_batch_powers_2026-10-06.md)
for a scoped example.
Neither a finite scalar nor a small speed difference proves acceptance or a global
optimum; repeat measurements only enough to resolve a material uncertainty.

### Configured annual account ownership: 2026-10-14 observation

For the measured Vastai1T dual-RTX-5090 annual-reset FinancialTransformer
baseline (d32, lookback32, global256/local128, fold11), whole configured accounts
can be owned independently by each rank. Within-account financial recurrence,
FP64 closing, fees, FIFO, masks and absorbing default still follow the canonical
bridge. Local actions remove per-batch action gathering and replicated accounting;
date-weighted gradients are reduced once before the epoch update. Unequal NN
call counts require avoiding DDP forward collectives and handling globally used,
locally absent gradients without changing globally unused parameters.

The accepted BPTT experiment truncates at local128 and configured account
boundaries; bind `annual_whole_account_local_actions_weighted_gradient_mean_v2`
and actual configured world size to the checkpoint contract. Reject incompatible
optimizer resume. A single continuous account, including `annual_reset__off`,
does not supply this independent axis. Validate all date coverage, unique account
ownership, padding, gradient weights and update cadence using the canonical
ownership receipt; do not waive an old batch-count failure without new evidence.

Four complete A/B/B/A trials measured steady maximum-rank epoch means
74.414 -> 51.610 seconds (30.6% less time, 1.4418x throughput) for annual ownership
plus compiled closing. Complete four-epoch fold means were 490.318 -> 480.891
seconds (1.9% less time); startup remains material. NVML average utilization did
not increase, and is not the effective-work denominator. Standalone closing
fusion failed host headroom admission; annual-only versus combined fusion has
only single-screen evidence. Do not extrapolate these results to other hardware,
shapes, strategy quality or a completed 1000-epoch run. Source, tests, exceptions
and raw results are in [the measured structural report](../gpu_structural_optimization_2026-10-14.md).

### Full-schema resident projection: 2026-10-14 observation

On the same dual-RTX-5090 d32/lookback32 fold11 workload, a repeated CPU compact
packet and wide H2D/checkpoint path dominated before Tensor Core math. Cache
immutable physical date blocks as full-schema CSR, retain zero-column effects
in stable LayerNorm/RMSNorm affine projection, and checkpoint narrow learned
embeddings. Store the CSR dense operand contiguously as `[F, projected_width]`;
the transposed view was a measured bottleneck. Fixed source statistics may be
cached; learned weights and embeddings must be recomputed each update.

Common-source A/B/B/A measured complete steady MAX-rank epochs
53.372 -> 17.351 seconds (3.076x throughput), while cold four-epoch folds only
improved 482.979 -> 456.163 seconds (5.55% less time). New cold CSR construction
remains expensive. Original global256/local128 failed the 1.5GiB headroom gate;
the admitted capacity exception is global128/local64, with every date, feature,
dimension and one epoch update retained. This changes BPTT truncation and numeric
trajectory: use the resident ABI/new root and reject v9 optimizer resume.
Do not project baseline acceptance onto unmeasured d128/lookback256 variants.
After staging is removed, financial recurrence/adjoint dominates; low NVML busy
alone is not evidence that replacing attention kernels will multiply throughput.
Mechanism/adjoint tests, retained numeric rejections, full artifact/financial-owner
checks and CPU-readable off-node results are in
[the resident rewrite report](../gpu_architecture_rewrite_2026-10-14.md).

### Buffered ledger graphs: 2026-10-15 observation

The same selected d32/lookback32 workload uses event-compressed accounting in
training. Test that actual path before timing a full fold; an enabled flag with
zero forward/backward replays is a rejected integration. The dedicated graph
owner uses fixed buffers, date tensors and bounded saved-tape leases; restore
each backward's external source, preserve pending forwards, and replay the
canonical executor if the terminal-flat financial certificate fails. Explicit
graphs require inner Inductor graph trees disabled. Inference-first construction
must leave tensors usable by later training and mutation checks.

Repeated source staging needs a separate immutable, byte-bounded prepared-batch
cache. Pin under the owning device and complete asynchronous transport before
claiming its time. Cache normalized CSR derivatives using source identity and
the exact fold-fitted normalizer, verify typed arrays/hashes on reuse, and retain
source receipts and damaged generations. Do not cache learned projections or
optimizer-dependent outputs. Separate initial cache fill, fresh-process reuse,
steady complete epochs and isolated accounting speed.

Common-source A/B/B/A on the selected dual-RTX-5090 d32/lookback32 fold11,
global128/local64 workload measured complete steady MAX-rank epochs
18.456 -> 5.570 seconds (3.313x throughput), and fresh-process complete four-epoch
folds 481.042 -> 171.358 seconds (2.807x). Both candidate trials reused a verified
normalized-CSR disk cache; this is not a new-node first-fill measurement. All
validation/test/curves/plots/checkpoints/final replay were retained. Peak GPU
memory was 28194 MiB with at least 4.3096 GiB headroom. At that acceptance, source
prototype v13 backed the independent ablation v11 root; `--ledger-graph` had
35/35 remote dry-run/calendar gates passed and the formal queue was stopped.
Reject incompatible optimizer resume rather than copying v10 checkpoints.

A separate complete-train-epoch Nsight capture still shows host gaps, source
transport and rank imbalance. GPU activity includes NCCL waiting and is not SM
or Tensor Core occupancy; neither 100% useful utilization nor a global compute
limit was established. Rejections, mechanism tests and admission evidence are in
[the ledger report](../ledger_block_optimization_2026-10-15.md).

### Whole-session fusion and bounded device prefetch: 2026-10-15 observation

Compile the canonical funding, execution, closing, financial certificate and
state update as one fixed session step inside the existing saved-adjoint graph.
Retain native sensitive FP64 scan boundaries and canonical fallback. A graph
program owns input schema rather than one source row view: a row view can pin an
entire field-major device tape. Stage at most the next 32 source rows on a copy
stream, record the allocation's consumer lifetime once per field, and expand
the suffix on its consuming stream. CPU source caches must remain CUDA-free.
The compiled step has its own checkpoint fingerprint; reject incompatible resume.

Same-source v19 A/B/B/A versus the already buffered/cached implementation measured
complete steady MAX-rank epochs 5.595056 -> 5.000720 seconds (1.118850x throughput).
Complete four-epoch folds were 170.356601 -> 173.429279 seconds, 1.80% slower;
the selection serves the formal many-epoch workload, not a cold short-run speedup.
All validation, tests, plots, checkpoints and final replay remain. Peak memory
was 30576 MiB with 1.983398 GiB minimum headroom on the named dual RTX5090 d32/L32
global128/local64 workload; unmeasured variants retain their OOM capacity policy.
391 CPU checks, 13 remote GPU checks, canonical real-action forward/VJP and all
16 resume transitions passed; four folds/311 files/eight checkpoints were
SHA-verified off-node and checkpoints CPU-loaded. The shell now selects the
independent fused v12 root, bound to source prototype v19. Current formal launch
proof and remaining recurrence/rank-tail boundaries are in the ledger report.

## Remote node selection

The user's 2026-10-03 instruction requires every remote build to use that node's
actual conditions and measured performance. Previous winners are measurement
seeds, never evidence that a different machine/workload has the same optimum.
Before selection observe CPU model/topology/NUMA, process affinity, accessible
cgroup ancestors and CPU-time quota, RAM limit/headroom, source/output storage,
current load/pressure and, for GPU work, GPU/VRAM/driver/CUDA and active owners.
Host-visible CPU count and MemAvailable alone do not establish container capacity.

Measure the complete requested build/epoch/fold, keeping source verification,
summary, evaluation, output, curves and checkpoints. Compare feasible candidates
under the same code/config/source/runtime, repeat finalists in balanced order,
and select by measured total wall time when speed is the requested priority.
Record CPU-seconds, peak memory, I/O and output parity alongside timings. CPU
quota limits CPU time; bounded pool oversubscription is a measurable candidate,
not a larger CPU entitlement. Report overlapping timing ranges honestly.

Bind the selection to observed node limits/topology/storage, exact workload and
artifact/runtime identities. Changed conditions require new measurements; for
an updated workload, previous winners may seed a smaller measured comparison
before widening it. GPU tuning still uses the canonical manager/preflight/lifecycle.

The reusable CPU observation helper is `stockagent/remote_build.py`; the first
complete source-build implementation is
`scripts/benchmark_tw_public_remote_derivation.py`. Commands and current scope
are in [Remote build workflow](../remote_build_workflow.md). Other workloads keep
their canonical builders and require their own measurements and parity proof;
the TW source result is not a universal training/installation recommendation.

## Epoch-Level Timing And Throughput

The user cares about total epoch wall time, not only train step time.

Rules:

- `scripts/run_tw_day_trade_ablation.py` runs independent experiments strictly
  one at a time.  Each experiment keeps within-fold DDP ownership of every
  visible GPU and receives the full host CPU/Inductor thread budgets; do not
  divide GPUs or threads across concurrent ablation variants.  Historical
  partial epoch curves may contribute to aggregate progress, but the live UI
  must label only the single newest/current experiment so it cannot imply that
  stale resumable variants are running concurrently.
- For the dual-RTX-5090 `tw_minute` FinancialTransformer config that inherits
  `tw_public_lanten_market_candles.yaml`, the measured throughput-only capacity
  is global `batch_size_train: 64`, `batch_size_eval: 16`, and train/eval
  decision chunks of 16. Complete fold-1 epoch-2 times for train batches
  8/16/32/64 at chunk 16 were 30.53/28.60/28.15/27.02 seconds. Batch 128 with
  chunk 8 tied at 27.02 seconds but reserved about 31.5 of 32.6 GiB per GPU;
  keep 64/16 for the same throughput with more headroom. Compare finite-loss
  runs by throughput only when that is the user's explicit priority.
- The long/short minute execution-contract v3 and training-contract v6 add a
  compact point-in-time short-rule sidecar, signed target/ledger execution,
  deterministic stacked host-batch caching, and one DDP gradient reduction per
  optimizer batch via non-static DDP `no_sync` chunks. Freeze disabled
  positional tables before optimizer creation instead of enabling per-forward
  unused-parameter discovery. On fold 1, the complete steady epoch 2 measured
  `24.061s = 20.798s train + 3.262s validation`, versus the preceding exact
  production rerun at `27.260s` (11.7% faster) despite adding short execution.
  The official eligibility/direction remains exact-session, official
  post-close capacity shifts by exactly one observed dataset session, and all
  missing evidence fails closed without forward fill.

- Use `epoch_curve.jsonl` together with `epoch_workflow_timing.jsonl` when
  optimizing epoch-level speed. The latter's
  `complete_epoch_wall_s_max_rank` is recorded after curve plotting and
  lifecycle updates on all ranks. Historical `epoch_wall_s_max_rank` can stop
  before plotting; label its scope explicitly. Final asynchronous plot flush,
  startup and final replay remain covered by complete-fold wall time.
- Break down "other" time before optimizing blindly.
- Initialize/mask Numba's parallel pool before setting the resolved per-rank
  Torch thread count. On a shared OpenMP runtime, Numba's first parallel panel
  operation can overwrite Torch's budget with the host-wide maximum. Preserve
  a smaller explicit Numba maximum, and verify actual trainer counts after
  panel loading rather than relying on the launcher's earlier thread message.
- Attribute physical-source decoding, padding, stacking, and device transfer to
  backtest preparation separately from the account runner. Large CPU thread
  budgets can make per-session tensor padding much slower than one batched
  allocation/copy. For immutable FP64/int64 CPU transport, preserve source bytes
  (including NaNs and signed zero), padding, ownership, and CUDA reconstruction;
  keep the tensor/autograd path for other inputs. Limit CPU-only final replay
  threads within its own scope and restore the previous count on exceptions.
  Validate exact metrics/checkpoint states and complete artifacts; report any
  control restart or changed reporting runtime explicitly. See
  `docs/no_qk_norm_cpu_staging_2026-09-22.md`.
- For long-year runs, re-check the latest artifact before optimizing. The run under
  `artifacts/train_2000-2001-...-2024/epoch_curve.jsonl` showed train time
  dominating epoch wall time, with CPU-to-GPU train tensor transfer larger than
  model forward. In that case, prioritize guarded GPU train tensor caching over
  test-curve work.
- Every epoch should account for train, validation, sampled test loss, curve test, curve plot, checkpoint, scheduler/progress, and any reporting work.
- Expanding `train_union` folds change the symbol dimension even when the global
  batch dimension is fixed. For compiled canonical loss, mark only the symbol
  axes dynamic, including the direct `symbol_indices [S]` companion and
  recurrent-state vectors, and reuse one compiled loss wrapper across train
  groups. Derive the upper bound from the largest symbol width any current
  walk-forward training group can actually produce after compaction; do not use
  validation/test-only symbols from the full panel. Recompute it every run so
  listings and delistings remain runtime data. An arbitrary very large upper
  bound makes Inductor constraint analysis and cold compilation much more
  expensive and can violate flattened-index guards.
- Do not hide expensive work behind `val_interval_epochs > 1` or skip curve/test/plot work unless the user explicitly asks.
- Recent preference: sampled test loss only needs one fold per epoch to reduce epoch-level overhead. For `tw_minute`, compute that audit-only loss over the first calendar year of the current fold's test interval, record its year/row scope in `epoch_curve.jsonl`, and never use it for checkpoint selection, early stopping, or the scheduler.
- Keep curve plotting async where possible.
- Compare throughput in the selected benchmark's steady-state acceptance
  window. For vastai1T TW day-trade, use epoch 3+ maximum-rank wall time under
  the [runtime contract](runtime.md); epoch 2 is diagnostic there. Keep cold
  compile/autotune/warmup measurements separate rather than choosing defaults
  from the first epoch.
- For the high-throughput TW cash candles configuration, keep
  `finite_check_interval_steps: 0` and `checkpoint_finite_check: false` when the
  user opts out of scanners. Prevent non-finite states in the settlement math
  and bound recurrent differentiation instead of adding parameter/gradient
  sweeps to the training hot path.
- GPU tensor caching is allowed when transfer dominates and VRAM checks pass:
  - prefer `cache_train_tensors_on_gpu: true` for transfer-bound long-year runs
  - keep `cache_eval_tensors_on_gpu: true` for lazy windowed tensor runs when train/val/test can reuse the same cached base panel tensors
  - do not duplicate full `[T,S,F]` panel tensors for train/val/test windowed splits on GPU; cache the base tensors once and share them, moving only split-specific `valid_indices` / `sample_mask`
  - for very large universes such as US full universe (`S≈16808`) on 16GB GPUs, do not force-cache the shared base panel when it would leave too little post-cache VRAM for compiled model/eval workspaces; the safe measured starting point is `batch_size_train: 8`, `batch_size_eval: 8`, `eval_auto_chunk_rows_cap: 16`, `vram_safety_margin_gb: 1.5`, `target_vram_fraction: 0.85`
  - if a windowed shared base remains on CPU, keep windowed metadata on CPU too; CUDA `valid_indices` must not index CPU base tensors
  - prepare lazy train/val/test windowed splits with a shared base so large `[T,S,F]` tensors are not repeatedly pinned or copied before GPU caching; only split metadata should be prepared separately
  - prefer the panel-slab forward wrapper for contiguous train/eval lazy-window batches so `torch.compile` sees fixed slab tensors instead of dynamic date-index gathers; use generic panel forward for non-contiguous rows, factor-augmented/detailed-aux paths, and padded final eval chunks
  - compile the panel-slab forward wrapper with `dynamic=False` and `options={"triton.cudagraphs": False}` rather than `mode="reduce-overhead"`; the reduce-overhead/cudagraphs variant was observed to segfault on the second compiled slab backward for the active RTX 4070 Ti SUPER CUDA environment
  - `_maybe_cache_tensors_on_device` must keep the VRAM safety check and skip caching if it does not fit
  - keep `eval_auto_chunk_rows_cap: 64` for the current speed ablation to reduce validation/test chunk overhead; re-check VRAM and epoch 2+ timing before raising it to 128
  - eval chunk code pads only the final ragged chunk to the configured chunk size and trims outputs back to valid rows; keep this to avoid extra compile shapes without changing canonical returns
  - `eval_auto_chunk_rows_cap: 32` and `batch_size_train: 64` were tested on the current single-fold lookback32 benchmark and were not adopted; cap32 had worse warmup/final eval, batch64 had worse warmup and slower steady epoch

Compile/runtime rules:

- Use CUDA 13 ptxas for the current PyTorch CUDA 13 environment. Prefer mamba/conda packages such as `cuda-nvcc` / `cuda-nvvm-tools` in the `fintech` env; do not leave a CUDA 12 pip `nvidia-cuda-nvcc-cu12` package around as a fallback ptxas source.
- RTX 5070 Ti (`sm_120a`) native NVFP4 uses source-built NVIDIA Transformer Engine, not bitsandbytes NF4 or fake quantization. The verified build is Transformer Engine `2.16.0+4220403`, CUDA 13.3 `nvcc`/`ptxas`, CUDA 13 runtime libraries, and cuDNN 9 in the `fintech` env.
- Build Transformer Engine for Blackwell consumer GPUs with `NVTE_CUDA_ARCHS=120a` / `CMAKE_CUDA_ARCHITECTURES=120a`, and force CUDA library discovery to the conda env. Do not let `libtransformer_engine.so` link to system CUDA 12 libraries; set RPATH or `LD_LIBRARY_PATH` so `libcublas.so.13`, `libcudart.so.13`, `libcublasLt.so.13`, and `libcudnn.so.9` resolve from the `fintech` env.
- On RTX 5070 Ti, NVFP4 stochastic rounding is not supported by ptxas for `sm_120a` (`cvt.rs.satfinite.e2m1x4.f32` rejects `.rs`). Use native deterministic NVFP4 recipes such as `NVFP4BlockScaling(disable_rht=True, disable_stochastic_rounding=True)` for project benchmarks and training probes.
- Transformer Engine NVFP4 `te.Linear` needs a conservative padding adapter in this project: pad input/K to 32, output/N to 32, and leading rows to 32, then slice outputs back. The lower FP4 type granularity is 16, but `K=48` FFN output projections failed on RTX 5070 Ti unless padded to `K=64`.
- Source `scripts/runtime_env.sh` before invoking Python. It prepends the selected
  environment's `bin`, so Triton resolves that environment's `ptxas` regardless
  of where the environment is installed.
- Compile cache paths should be stable and persistent across runs:
  - `TORCHINDUCTOR_CACHE_DIR=~/.cache/torchinductor`
  - `TRITON_CACHE_DIR=~/.cache/triton`
  - `CUDA_CACHE_PATH=~/.cache/nv_cuda`
  - do not delete these caches between repeated same-shape benchmarks unless explicitly testing cold compile behavior
- On the measured dual-RTX-5090 TW public run, letting each DDP rank inherit 64
  Inductor compile workers created 128 concurrent workers. After an interrupted
  compile they became orphaned and remained blocked in XFS
  `filename_create`/`xfs_buf_lock`, stalling later pre-epoch work. Keep the
  `tw_public` host-wide `environment.torch_compile_threads` budget at 16 (8 per
  rank), serialize the independent DDP model probe so the later rank reuses the
  persistent cache, and preserve graceful SIGTERM-to-atexit cleanup for
  Inductor workers. The canonical-loss probe is intentionally collective: it
  must reproduce the real autograd all-gather input on all ranks, otherwise the
  first train step compiles the same loss again.
- The TW public open-aware `tw_day_trade` executor now uses the same bounded
  compiled-settlement pattern as `tw_cash`. Keep the eager ledger as the
  semantic oracle, compile fixed 32-row chunks with CUDA graphs disabled, and
  carry `cash`, `payables`, `receivables`, `alive`, and `equity_scale`
  differentiably between chunks. A non-aligned tail stays on the exact eager
  implementation. Do not replace this state machine with independent daily
  returns: T+2 default is absorbing and volume caps depend on carried equity.
  The DDP canonical-loss probe must report `tw_day_trade_chunked=true`, at
  least one compiled chunk call, and zero eager fallbacks before epoch 1.
- Measured on dual RTX 5090 with the open-aware public config, `T=128`,
  `S=2738`, and chunk/horizon 32: settlement forward+backward improved from
  `546.7ms` eager to `114.1ms` compiled in an isolated actual-shape probe; a
  no-grad `T=512` evaluation improved from `950.0ms` to `68.5ms`. The complete
  fold-1 epoch-2/3 median improved from the prior `2.213s` baseline to
  `0.439s`, including validation, sampled test curve, plotting, and checkpoint
  work. Cold compile remains material (roughly 100 seconds per grad/no-grad
  contract), so preserve stable caches and judge throughput only after epoch 1.
- After DDP training, run the saved-model inference/plot artifact pass on rank
  0 only. Other ranks must wait through a dedicated CPU/Gloo process group with
  a long artifact-I/O timeout; do not use the default NCCL group for this wait.
  A measured XFS discard stall exceeded NCCL's 600-second watchdog even though
  no GPU work was wrong, while the Gloo wait completed normally and avoided a
  duplicate inference pass and artifact write race.
- Expanding `train_union` folds change their symbol count. When
  `training.compile_loss_dynamic_symbols: true`, keep the time/batch axis static,
  mark only the canonical loss symbol axis dynamic, and reuse one compiled loss
  wrapper across train groups. This is an executor optimization: do not pad real
  assets, fork the loss formula, or add it to the semantic checkpoint contract.
- Current benchmark result for the active `data_okx` lookback32 run: compare only epoch 2 or later. The fastest measured compile combination was model compile plus the canonical fullgraph log-utility loss:
  - `enable_torch_compile: true`
  - `backtest_compile: true`
  - `backtest_compile_stateful: true`
  - `backtest_compile_dynamic: false` for fixed train/eval shapes
  - `compile_loss: true`
  - epoch 2 wall time improved from about `67.54s` with all compile off to about `18.99s`.
- Compile mode benchmark result:
  - keep `torch_compile_mode: reduce-overhead`
  - `default` and `max-autotune` were slower on epoch 2 for the active `data_okx` lookback32 shape
- Current chunk/batch benchmark result:
  - keep `eval_model_chunk_rows: auto` with `eval_auto_chunk_rows_cap: 64`
  - keep `eval_backtest_chunk_rows: 512`; larger compiled backtest chunks such as 1024/2048 stalled compilation and did not produce epoch 2 within the manual test window
  - keep `batch_size_train: 32`; `batch_size_train: 64` was only marginally faster in one epoch-2 run and changes optimizer batch granularity
  - keep `backtest_autotune: true`; disabling it was only noise-level faster in one epoch-2 run and can hurt other shapes
  - keep backtest prep compile enabled; `STOCKAGENT_BACKTEST_COMPILE_PREP=0` was not faster on epoch 2
- Trainer compile checks should discover the selected fintech environment's
  `ptxas` and conda compilers `x86_64-conda-linux-gnu-gcc/g++` even when the
  parent shell PATH is sparse.
- Historical actual-shape compile probes on the 2000-2024 TW checkpoint showed:
  - compiled `transformer_base_portfolio` model forward is beneficial
  - compiled tensor backtest is beneficial and may use fallback on unsupported graph states
  - isolated compiled loss has small benefit, but compiled model plus compiled loss was unstable in the actual-shape probe
- Current safe baseline preference:
  - `enable_torch_compile: true`
  - `auto_torch_compile_sharpe: false`
  - `backtest_compile: true`
  - `backtest_compile_stateful: true`
  - `backtest_compile_dynamic: false`
  - `backtest_autotune: true`
  - `compile_loss: true`
  - compile canonical `risk_aware_loss` with `fullgraph=true` for log utility; do not maintain a second loss formula
- Eval model forward chunking and eval backtest chunking are intentionally decoupled:
  - keep model chunk sizing VRAM-driven, often `eval_model_chunk_rows: auto`
  - use larger `eval_backtest_chunk_rows`, currently `512`, to reduce `run_backtest_torch()` calls without skipping any val/test curve rows
  - preserve `prev_weights` continuation across backtest chunks and reset only at fold/segment boundaries
