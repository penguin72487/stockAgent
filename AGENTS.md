# AGENTS.md

StockAgent instructions. Keep this entrypoint concise; read the topic contracts
below when the task touches them. Those contracts remain binding in their scope.
Paths written as `stockagent/...`, `scripts/...`, `configs/...`, `test/...` or
`docs/...` in the topic files are relative to the repository root.

## Working agreements

- Reply in Traditional Chinese unless requested otherwise. Be direct and carry
  authorized work through implementation and appropriate verification.
- Preserve the user's intent across turns. Use the latest explicit experiment
  settings. A task/config that selects an experiment supplies its scope; do not
  ask again merely because a historical recommendation differs.
- Inspect Git state before editing. Preserve unrelated and overlapping dirty
  work. Use `apply_patch` for manual edits; use `rg` for discovery. Do not use
  destructive Git commands without explicit authorization.
- For multi-step implementation/operations, use the [agent workflow](docs/agents/runtime.md#agent-workflow)
  to inspect current state, retain the original task and record milestone evidence.
- Source `scripts/runtime_env.sh` and use `run_fintech_python`; do not hard-code
  a Python environment path. Before expensive GPU jobs run
  `run_fintech_python scripts/check_environment.py --require-cuda --strict`.
  If `runner.require_cuda` is true, never silently fall back to CPU.
- Validate in proportion to the change. Use focused semantic tests for affected
  behavior and broader regression/lifecycle checks for shared contracts. Text
  edits need link/structure checks, not unrelated training runs. The maintained
  full test suite is `run_fintech_python -m pytest -q -s test`.
- A supplied skill is a workflow aid. Select its relevant references; repository
  code/configs and evidence establish current implementation. Do not require
  hidden memory or private skills to understand a repository contract.

## Correctness boundaries

- Keep point-in-time features, decision clocks, eligibility and executable
  prices distinct. Never invent historical observations, interpolate missing
  execution prices, backdate a snapshot, or describe a research/paper proxy as
  a broker fill. Approved research exceptions stay confined to their named ABI.
- Use canonical collectors, training lifecycle/checkpoints, backtest/accounting,
  publication and service paths. Extend shared implementations instead of
  copying them. Product-specific action, settlement and fee semantics remain
  distinct; train/eval/inference must agree within the selected contract.
- Changes to data, features, execution, source scope or optimizer trajectory
  require the matching version/fingerprint and compatible artifact root. Reject
  incompatible resume. Report-only changes must refresh their artifact contract.
- Keep numerically sensitive financial reductions stable; BF16 AMP does not
  mean permanent BF16 storage. Preserve absorbing ruin, exact fees/masks,
  recurrent state and whole-lot/contract accounting.
- Preserve source data and receipts. Publication, delivery, cold reconstruction,
  local materialization and runtime readiness are separate proofs. Storage
  cleanup requires the inventory/dry run, exact recovery and process-reference
  gates in [storage.md](docs/agents/storage.md); apparent duplication is not proof.
- Service activity or HTTP 200 proves reachability only. Claim data completeness,
  successful deployment or live readiness only from the corresponding current
  receipts, acceptance checks and runtime evidence.
- Performance work measures the entire requested epoch/fold workflow. Do not
  skip validation, test, curves, plotting or checkpoints to improve timings.
  Hardware-specific tuning recommendations must be re-measured before reuse.
- Separate engineering validation, research results and investment conclusions.
  State exactly what was verified and what remains unresolved.

## Read by task

Read applicable sections before changing the corresponding behavior. Start with
the relevant row, follow cross-references only as needed, and do not load this
whole table's documents for every task. For a cross-cutting change, use every
affected contract. Documentation edits require the contract being edited, not
unrelated production preflights.

| Task | Contract |
| --- | --- |
| Environment, Windows supervisor, GPU setup, test commands | [Runtime](docs/agents/runtime.md) |
| Data publication, Syncthing, hot/cold storage, migration or deletion | [Storage](docs/agents/storage.md) |
| Discord, manual signals, TAIFEX capture/strategy service and health | [Services](docs/agents/services.md) |
| New training config, baseline, model precision or output policy | [Model defaults](docs/agents/models.md) |
| Transformer-base architecture or its historical speed experiment | [Transformer reference](docs/agents/transformer_reference.md) |
| Panel backend, feature timing, crypto downloader grain | [Data and features](docs/agents/data.md) |
| Portfolio direction, TW replay, fills, fees, settlement, backtest or loss | [Execution and loss](docs/agents/execution.md) |
| Split ownership, model horizon, benchmarks and stitched deployment | [Walk-forward](docs/agents/walk_forward.md) |
| Trainer/runner, DDP, checkpoints, resume and artifact completion | [Training lifecycle](docs/agents/training.md) |
| Compile, cache, batch size or throughput optimization | [Performance](docs/agents/performance.md) plus [Runtime](docs/agents/runtime.md) |
| TW public acquisition/parsers, price grids, lifecycle and execution masks | [TW public rules](docs/agents/tw_public.md) |
| Attribution, feature screening, cross-asset analysis and report coverage | [Explainability](docs/agents/explainability.md) |
| Named 08:45 stock-futures v2-v15 reproduction or amendments | [Futures experiment history](docs/agents/futures_history.md) |

## Maintaining these instructions

- Correctness contracts and reproducibility boundaries persist. Model choices,
  batch sizes, compile modes, timings and labels such as "active" are scoped
  recommendations or observations, not permanent restrictions on experiments.
- Record a durable, evidenced lesson in its existing topic file or dated project
  report. Revise superseded wording and link the evidence rather than appending
  another universal rule here. Add an entrypoint rule only when it applies
  across the repository. Do not retain config keys ignored by the implementation.
- Full cross-stock attention remains an explicit experiment, with shape/VRAM
  guards; a requested architecture experiment or selected config provides that
  scope. Historical speed settings do not prohibit evaluating other designs.
- Do not update personal memory unless the user explicitly requests it.
