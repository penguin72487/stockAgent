# Whole-project Engineering Review — 2026-08-10

## Verdict

The repository has substantial causal execution, checkpoint, data, and testing
infrastructure. After the contract and environment repairs in this review, the
current branch is verification-green: the formal suite passes, the active TW
candle contract is internally consistent, and the optional Transformer Engine
NVFP4 path completes a real CUDA forward/backward test. This is an engineering
baseline, not proof that every dataset is complete, every live service is ready,
or any strategy is investable.

## First-principles assessment

| Gate | Current evidence | Assessment |
| --- | --- | --- |
| Information | point-in-time masks, public-data feature timing, immutable snapshot and receipt workflows | Strong design; completeness must still be checked per run |
| Causality | explicit fold ownership, split-local/panel-history lookbacks, execution-specific clocks | Strong contract; the active candle YAML is now consistently named as day-trade |
| Mechanism | canonical tensor simulator, fees/taxes, Taiwan settlement and phase executors | Strong but high-complexity implementation |
| Latency/deployability | lazy slabs, compile/DDP paths, live signal path | Capable; pure inference runtime no longer requires eager trainer import, while manifest construction remains trainer-owned behind a narrow API |
| Reproducibility | checkpoint manifests, RNG/fold fingerprints, lifecycle artifacts | Strong run artifacts; environment runtime and repository-local metadata boundaries are now verified |

## Measured inventory

- 487 tracked-or-patch files; 126 test modules and 2,100 test functions.
- About 294,358 tracked-or-patch Python lines.
- Largest production modules: `stockagent/training/trainer.py` (22,326 lines),
  `downloader/download_openbb_archive.py` (18,732), and
  `stockagent/explainability.py` (10,110).
- The worktree was clean at the start of the review.
- `scripts/check_environment.py --require-cuda --strict` passed on Python 3.12,
  PyTorch 2.12.1, CUDA 13.0, and one RTX 5070 Ti.

## Verification results

```text
compileall: passed
Ruff correctness checks: passed
focused correction tests: 35 passed
shared runtime/live/checkpoint focused tests: 143 passed
real CUDA NVFP4 forward/backward: 1 passed
strict CUDA environment check: passed
formal suite: 2725 passed, 1 skipped, 1 xfailed, 32 warnings
formal suite elapsed: 266.27 seconds
```

The focused set covered active trading/output configuration, inherited ablation
semantics, public-source fingerprints, rebuild gating, package boundaries, and
the precision stack. The follow-up focused set covered the shared runtime, live
imports, precision, nested model wrappers, safe checkpoint I/O, checkpoint
manifests, and live helpers. The eight original failures were resolved according
to their owning contracts; production execution formulas, checkpoint schemas,
fee semantics, and point-in-time masks were not relaxed to make the suite pass.

## Findings by priority

### Resolved P0 — Restore one explicit active contract

`configs/markets/tw_public_lanten_market_candles.yaml` now consistently names the
experiment as `tw_day_trade`. Its Financial Transformer output is
`projection_l1`, matching the documented L1-ball contract where unused gross
exposure may remain cash. The OFAT ablation now explicitly pins its declared 1m
volume-participation baseline instead of inheriting the active 10m value.

Tests now distinguish active-model shared encoder/runtime fields from deliberate
experiment-level portfolio-head overrides. Base-config tests follow the current
fixed-width `tw_day_trade` ABI rather than silently restoring an older dynamic
`tw_cash` experiment.

### Resolved P0 — Repair the precision environment

The runtime now imports protobuf 7.35.1, ONNX, `einops` 0.8.2, and Transformer
Engine 2.16.0. A real NVFP4 CUDA forward/backward test passes. Conda still has a
stale duplicate protobuf metadata record from the previous pip installation;
runtime imports and package metadata observed by Python are correct, but this is
an environment-hygiene issue to clean during a controlled rebuild rather than by
manually deleting package records.

### Partially resolved P1 — Extract shared runtime contracts

Device resolution, BF16/FP16 autocast, model calling, model-output extraction,
wrapper unwrapping, safe checkpoint loading, and state-dict compatibility now
have one implementation in `stockagent/training/runtime.py`. The trainer keeps
its historical private aliases for compatibility; live inference imports only
the public runtime API. A subprocess regression test proves that importing the
live signal module no longer imports the trainer.

One measured cold import changed from 7.68s / 818,268 KiB RSS / 2,866 modules to
4.54s / 804,060 KiB RSS / 2,680 modules. This is import-boundary evidence, not a
claim that complete signal generation is 41% faster: checkpoint manifest
construction and panel-universe alignment still delegate lazily to their single
trainer implementation through `stockagent/training/inference_contract.py`.

The former `stockagent/data` dependency on `downloader/` was repaired in this
review: reusable capture-part selection now lives in
`stockagent/data/shioaji_capture_parts.py`, while the downloader path is a thin
compatibility re-export. The remaining runtime-boundary priority is to move the
manifest/alignment implementations behind the public inference API, eliminating
its lazy delegates into trainer internals without duplicating schema logic.

### Partially resolved P1 — Repair repository boundaries

- `.build/xformers-src` was introduced as a gitlink without any `.gitmodules`
  contract and is not consumed by the current source/environment. It is removed
  from version control and `.build/` is ignored. The local directory remains.
- Machine-local `.codex/config.toml` and `.codex/environments/environment.toml`
  are removed from version control and `.codex/` is ignored. Local files remain;
  VS Code tasks now resolve the local Codex helper through `${env:HOME}` rather
  than `/home/user`.
- The Git database is about 4.1 GiB and reports about 2.41 GiB of garbage/temp
  pack data. Back up and inspect before an explicit maintenance window; this
  review did not run destructive cleanup.

### P1 — Establish enforceable verification

There was no repository-level pytest/Ruff configuration or CI entry point. This
review adds a minimal `pyproject.toml`: default pytest discovery is `test/`, and
default Ruff checks only execution-affecting errors. Full style cleanup remains
separate because the legacy tree currently has about 190 broad Ruff findings.

### P2 — Decompose monoliths behind contracts

The trainer, downloader, explainability module, and several tests are too large
for safe local reasoning. Start with seams that already have tests:

1. checkpoint manifest/load/save and runtime precision helpers;
2. fold/artifact orchestration;
3. train/eval step execution and timing;
4. mode-specific adapters;
5. reporting/explainability consumers.

Do not begin with a file split alone. Characterize outputs, resume behavior,
RNG, fee/state parity, compiled/eager parity, and artifacts before extraction.

### P2 — Make dependencies reproducible

`requirements.txt` contains broad lower bounds and omits the development/test
toolchain. Conda and pip metadata currently disagree for several CUDA/RAPIDS
packages. Define a supported environment lock strategy and add capability checks
for optional stacks instead of assuming package metadata implies importability.

## Changes made by this review

- Replaced the outdated long-only/RL roadmap in `training_spec.md` with the
  current causal training and evaluation contract.
- Added this documentation index and review report.
- Added minimal pytest and correctness-lint defaults in `pyproject.toml`.
- Corrected two stale direct-execution test function names. These did not change
  pytest test semantics.
- Updated the README entry points and labelled machine-specific operator notes.
- Corrected the active day-trade config's experiment naming and L1-ball output
  mode, and pinned the ablation's declared baseline.
- Updated stale tests only where repository history and active contracts showed
  that the expected behavior had intentionally changed.
- Made public-source cache fingerprinting explicitly distinguish legacy and
  current TWSE hosts, preserving fail-closed cache provenance.
- Moved shared Shioaji capture-part selection into the core data package without
  duplicating its implementation.
- Repaired protobuf and `einops` in the active fintech environment and verified
  the optional NVFP4 CUDA path.
- Extracted the shared model/precision/checkpoint-I/O runtime from the trainer,
  preserved existing trainer aliases, and moved live inference to public APIs.
- Added regression coverage for empty attention rows, nested wrapper state,
  safe checkpoint payloads, canonical alias reuse, precision, and lazy live
  imports.
- Removed the invalid xFormers gitlink and machine-local Codex settings from Git
  tracking without deleting their local files; made VS Code task paths portable.

Production execution formulas, checkpoint schemas, datasets, and services were
not changed.

## Recommended next sequence

1. Extract checkpoint manifest construction/validation and panel-universe
   alignment from the trainer after characterizing all schema-1--4 compatibility
   paths; remove the lazy delegates only in that same patch.
2. Turn the now-green formal suite and strict environment check into CI gates.
3. Back up and schedule explicit Git object maintenance; do not combine history
   rewriting or garbage collection with feature work.
4. Rebuild the fintech environment from a lock to eliminate duplicate package
   metadata and verify optional precision capabilities from a clean prefix.
5. Continue decomposing the largest modules only behind characterized semantic
   contracts.
