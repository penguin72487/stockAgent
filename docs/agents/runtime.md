# Runtime and verification

Use for environment, Windows supervision, test selection, or GPU job setup. Commands below are examples for the affected surface, not a requirement to run the full suite for documentation edits.

## Agent workflow

For multi-step implementation/operations, start with `stockagent-agent status`
and a task record. Update its next action and evidence at milestones; resume the
same task after interruption. Use `status --architecture` only when the task needs
the full configuration inventory. Read a task-relevant summary of status rather
than loading every task's Git baseline and run history into context; the filtered
example in the workflow document preserves access to the full records.
Small text edits do not require this setup.

The launcher sources the canonical runtime. Long foreground commands may use
its tmux/systemd supervision; GPU jobs, training acceptance, source receipts and
production services keep their existing owners. Run success alone does not prove
their acceptance. Installation, commands and recovery boundaries are documented
in [Agent workflow](../agent_workflow.md).

The opt-in cross-node engineering role is documented in
[Control workflow](../control_plane_workflow.md). Its PostgreSQL and Mamba Python role
are independent of acquisition, CUDA and intraday services. Runtime discovery
recognizes a venv's `pyvenv.cfg` before resolving its Python symlink to the base
interpreter, so an explicitly selected legacy role keeps its own dependency set.
New engineering roles use Miniforge/mamba declarations plus platform-specific
explicit locks; create fresh prefixes and retain native runtime before/after
proof. Setup and measured engine trials are in
[Technology trials](../architecture_technology_trials_2026-10-03.md).

## Workspace And Environment

- Repo root contains `AGENTS.md`, `train.py` and `scripts/`; this file is under
  `docs/agents/`. Resolve the current checkout root, for example with
  `git rev-parse --show-toplevel`; its absolute path differs across machines.
- Preferred Python runtime is the `fintech` Conda/Mamba environment, whose absolute
  path differs across machines. Source `scripts/runtime_env.sh` and use
  `run_fintech_python`; `FINTECH_ENV_PATH` or `PYTHON_BIN` may override discovery.
- Do not assume `python` exists on PATH or hard-code one user's home directory.
  Run `run_fintech_python scripts/check_environment.py --require-cuda --strict` before expensive jobs.
- CUDA is expected for training. If CUDA is unavailable and `runner.require_cuda` is true, do not silently fall back to CPU.
- For vastai1T TW day-trade training performance work, the acceptance baseline is
  two-GPU DistributedDataParallel with the resolved formal global batch and a
  complete fold lifecycle. Single-GPU runs may diagnose correctness only. Treat
  epoch 1 as compiler warm-up; compare steady epoch 3+ maximum-rank wall time,
  and require exact artifact/metric parity before promoting an optimization.
- The 428-feature all-observed fold11 dataset with exact physical FIFO exceeded
  a 32 GiB card's workspace when its 14.72 GiB FP32 panel was kept on each
  GPU. The measured v4 experiment keeps train/eval panels in host memory;
  six consecutive two-GPU epochs used at most 18.8 GiB on GPU 0 and added no
  new Dynamo graphs after epoch 1, with identical v3 train/val/test scalars
  through epoch 5. This is a dataset/hardware observation, not a universal
  cache policy. See `docs/tw_public_all_observed_v8_ofat_fold11_data_contract.md`.
- Use `rg` / `rg --files` for search.
- Use `apply_patch` for manual file edits.
- Do not revert user changes or unrelated dirty files.
- Do not use destructive git commands such as `git reset --hard` or `git checkout --` unless the user explicitly asks.
- For the Windows public Caddy supervisor, verify the actual `wsl.exe` child
  exit code as well as gateway health.  On this host, a quoted distribution
  name inside `ProcessStartInfo.Arguments` returned `WSL_E_DISTRO_NOT_FOUND`
  even though the already-running gateway stayed healthy; use the validated
  unquoted safe-name path.  Measure child `StartTime` to `ExitTime` separately
  from the supervisor's polling/observation lag.  A warm WSL child success is
  not a Windows cold-boot recovery proof.

## Testing And Verification

Use focused tests after small changes, then broader tests when training/model/loss code changes.

Common commands:

```bash
source scripts/runtime_env.sh
run_fintech_python -m py_compile \
  stockagent/config.py \
  stockagent/training/trainer.py \
  stockagent/training/loss.py \
  stockagent/backtest/simulator.py

run_fintech_python -m pytest -q -s test
```

Known repo quirk:

- Prefer `run_fintech_python -m pytest -q -s test` for the formal test suite to
  keep collection scoped to the maintained test directory.

Model-specific tests:

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q -s \
  test/test_low_rank_market_transformer_portfolio.py \
  test/test_explainability_smoke.py
```

Loss/backtest consistency tests:

```bash
source scripts/runtime_env.sh
run_fintech_python -m pytest -q -s \
  test/test_backtest_tensor_consistency.py \
  test/test_pure_rank_loss.py
```
