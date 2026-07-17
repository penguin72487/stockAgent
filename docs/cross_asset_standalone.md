# Standalone Cross-Asset Explainability

Cross-asset GPU computation is intentionally separate from the primary
`explain_model.py` workflow. This keeps its quadratic source-target artifacts,
GPU wall time, and storage lifecycle independent.

The runner has one fixed data-coverage contract. It requires the canonical
`checkpoint_best.pt` for every configured fold and processes every valid date
after the in-split lookback warmup in the first test calendar year of each fold.
There are no single-fold, alternate-checkpoint, train/validation, date-sampling,
or all-test-years command-line paths.

A normal launch automatically relaunches torchrun across all visible CUDA
devices. On the current host this uses both RTX 5090 cards:

```bash
source scripts/runtime_env.sh
run_fintech_python cross_asset_model.py \
  --config configs/markets/tw_public_lanten_market.yaml
```

Each process owns one rank-local CUDA device and a disjoint set of 32 physical
CPU cores; folds are assigned with longest-processing-time scheduling weighted
by their first-test-year row counts. A fold is never divided between ranks.
The explicit equivalent is:

```bash
source scripts/runtime_env.sh
run_fintech_python -m torch.distributed.run \
  --standalone \
  --nproc-per-node=2 \
  cross_asset_model.py \
  --config configs/markets/tw_public_lanten_market.yaml
```

For a deliberate one-GPU diagnostic, restrict visibility before launch, for
example `CUDA_VISIBLE_DEVICES=0`.

For the current training root
`artifacts/markets/tw_public_lanten_market_token_all_available`, output is
written into that explicitly selected artifact's explainability tree:

```text
artifacts/markets/tw_public_lanten_market_token_all_available/explainability/
  cross_asset_run_manifest.json
  fold_01_test/
    cross_asset_runner_timing.json
    abstract_cross_asset_transmission/
  ...
```

Source and target caps remain algorithm/benchmark knobs. They do not change the
fixed all-fold, first-test-calendar-year, exhaustive-date contract:

```bash
run_fintech_python cross_asset_model.py \
  --config configs/markets/tw_public_lanten_market.yaml \
  --max-sources 32 \
  --max-targets 32
```

A source or target value of zero means every active symbol. An explicit
`--cross-asset-output-dir` remains available only when the user deliberately
requests a different artifact volume; the default always stays under the
selected training artifact.

The standalone production runner always enables compact artifacts: numeric edge
metrics are stored once in the canonical Parquet representation with symbol and
shock lookup metadata. It does not duplicate the same full-universe values into
dense CSV matrices.

All graph figures are exhaustive. `graph_topology.png` includes every
inter-symbol edge in a directed source-by-target adjacency map;
`graph_node_importance.png` and `graph_self_influence.png` include every graph
node. There is no Top-K/Top-N graph selection, no graph-plot node cap, and no
betweenness vertex cap. Sparse axis labels are layout-only: they do not remove
matrix cells, nodes, edges, or calculations.

## Current dual-RTX-5090 throughput profile

The measured production defaults are `source_chunk_size=128`, automatic
`row_chunk_size=32`, `max_repeated_rows=4096`, BF16 autocast, and compiled
post-temporal counterfactual forward. The last ragged date chunk stays eager so
it cannot create a second CUDA-graph memory pool.

On fold 1 of the TW public artifact (`L=32`, `S=2735`, `F=131`), a 4096-scenario
steady batch measured about 47.2k source-date scenarios/s per RTX 5090 after
vectorizing source-score replacement, with 96.8% average SM utilization in the
roofline run and approximately 99% of the measured required-kernel
roof. Two independent fold ranks therefore have an ideal compute-only ceiling
near 94.4k scenarios/s before panel materialization, graph algorithms, plots,
Parquet writes, ragged chunks, and fold startup.

An 8192-scenario probe did not materially improve throughput and reserved about
28.9 GiB; a 16384-scenario probe exhausted the 32 GiB device. Keep the 4096
default for this host unless the model shape or GPU changes, then re-run the
actual-shape sweep.

Under torchrun, every rank writes only its assigned fold directories. At the end
of each scheduling round, ranks exchange timing or failure status. Rank 0 writes
the global manifest atomically only after all configured folds have completed;
a Python exception on one rank is propagated to every live rank instead of
leaving peers blocked at a final gather.
