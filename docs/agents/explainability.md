# Explainability and reporting contracts

Use when changing attribution, feature screening, cross-asset analysis or walk-forward visuals. Preserve the requested coverage and fixed comparison-window contracts. Measured GPU settings apply only to their recorded shapes and must be re-profiled for a different run.

## Explainability Contract

The user wants detailed model explainability to detect strange rules and judge strategy trustworthiness.

Expected explainability workflow:

- `run_fintech_python explain_model.py` should default to drawing the full explainability set unless the user asks for a smaller run.
- Do not generate Top-K/Top-N explainability tables or charts. Default portfolio
  attribution must cover every tradable non-zero position and report both
  position-count and gross-exposure coverage. Persist the complete sampled
  date-symbol inventory, including flat and masked rows, so omissions are auditable.
  Present high-dimensional results as complete matrices, distributions, cumulative
  coverage curves, or full machine-readable tables rather than rank truncation.
- Persist an explainability completeness table that reconciles decision-inventory
  rows, attributed positions, gross exposure, and enabled lookback-by-feature
  cells. Distinguish completeness within sampled dates from date coverage; an
  explicit exhaustive-date run must remain available without changing model semantics.
- Analyze all folds when making model-level claims.
- Keep `training.explain_after_each_fold: false` by default so training VRAM/time stays focused on train/eval/test artifacts.
- Generate explainability after training with `run_fintech_python explain_model.py`, which defaults to scanning all folds that have `checkpoint_best.pt`.
- Only enable `training.explain_after_each_fold: true` for deliberate smoke/debug runs, because paper explainability can be slow and VRAM-heavy.
- All model explainability and feature-screening calculations have one fixed
  comparison window: the first calendar year of each fold's test split, using
  every valid date after the in-split lookback. This is a correctness contract,
  not a default. Explainability has no train/validation split selector and no
  alternate test-year coverage selector. Do not reintroduce config fields, CLI
  flags, aliases, or helper branches that allow train/validation rows, later
  test years, or the complete future test tail into an explainability
  calculation. Positive within-year date limits remain explicit smoke/debug
  reductions only. Analyze all folds before making model-level or feature-
  selection claims so every fold contributes one comparable test year.
- Feature-selection screening must require every configured fold checkpoint and
  process every valid date in the fixed first-test-year window. Do not add fold
  subset or date-sampling CLI options to the screening runner; only reuse saved
  attribution files after validating identical complete coverage.
- The active explainability feature-selection rule keeps a feature if either
  Gradient x Input or Integrated Gradients is non-zero in at least one fold.
  Disable/comment a feature only when both methods are exactly zero in every
  configured fold; do not apply a small-contribution threshold unless the user
  explicitly changes this rule.
- Standalone `explain_model.py` must enable gradient × input, Integrated
  Gradients, feature-time perturbation, surrogate SHAP, regime analysis, fold
  stability, aux diagnostics, and all eligible cuML UMAP projections. Cross-asset
  GPU work is an independently scheduled project: run `cross_asset_model.py`,
  which owns cross-asset shocks, attention flow, validated transmission, role
  embeddings, and graph explainability. Its default artifacts live under the
  explicitly selected training root at `explainability/fold_XX_test/`, alongside
  the other fold explainability products. Do not add a compatibility cross-asset switch
  back to `explain_model.py` or the training CLI; the standalone runner is the
  only execution entry point.
  Chunking may change execution shape but must not omit outputs within the
  selected project.
- `cross_asset_model.py` has a fixed coverage contract: all configured folds,
  canonical `checkpoint_best.pt`, the first calendar year of each test split,
  and every valid date after the in-split lookback. Do not reintroduce
  fold/checkpoint/split/date-sampling/all-test-years CLI paths. Torchrun ranks
  independently own whole folds; do not wrap the model in gradient DDP.
- On the measured dual-RTX-5090 TW public shape (`L=32`, `S=2735`, `F=131`),
  use BF16, temporal-stock embedding reuse, compiled post-temporal forward,
  `source_chunk_size: 128`, automatic `row_chunk_size: 32`, and
  `max_repeated_rows: 4096`. The 4K scenario batch measured about 45.4k
  scenarios/s/GPU before source-score scatter vectorization and about 47.2k
  afterward, with 96.8% average SM utilization and roughly 99% of the measured
  required-kernel roof. An 8K batch reserved about 28.9GiB without material
  throughput gain and 16K OOMed; re-profile before changing the 4K default.
- Accumulate cross-asset metrics on GPU and transfer once per shock. Production
  output stores the complete numeric metrics once in compact edge Parquet plus
  source/target/shock lookup tables; do not duplicate the same full-universe
  values into dozens of dense matrix files.
- Cross-asset graph figures must not use Top-K/Top-N node or edge selection.
  Render every inter-symbol edge in the directed topology adjacency map and
  every graph node in importance/self-influence figures. Sparse tick labels are
  a layout choice only and must not omit matrix cells, graph metrics, or
  betweenness computation.
- Long standalone explainability stages should expose tqdm progress with ETA and
  throughput. Persist per-stage compute/write/cross-asset timings plus CUDA peak
  memory in the fold explainability timing artifacts; `--no-progress` may hide
  terminal bars but must not disable timing collection.
- Paper-grade explainability is the default report style:
  - `explain_report_style: paper`
  - `explain_plot_theme: paper`
  - `explain_shap_enabled: true`
  - `explain_shap_mode: score_head_surrogate`
- Use local artifacts under paths such as:
  - `data_yahoo/tw_stocks/lookback16/explainability`
  - future lookback-32 explainability outputs
- Inspect:
  - feature importance: gradient, integrated gradients, perturbation weight delta
  - time importance by lookback day
  - feature-time heatmaps
  - correlations between raw features and scores/weights
  - stock contribution and concentration
  - aux summaries for enabled latent factors and/or market tokens
- Dense explainability plots should use RAPIDS/cuDF/Datashader when available.
- Dimensionality reduction for transformer aux tensors should use cuML UMAP, not PCA, for the default explainability projection path.
- Aux UMAP projection outputs live under `aux_projections/*.csv` and `plots/aux_umap/*.png`; use them to inspect stock embeddings, enabled latent factors/market tokens, and token collapse/regime clustering.
- Be cautious with perturbation `score_abs_delta` when masked scores use sentinel values such as `-1e9`; prefer weight deltas, rank changes, gradients, and integrated gradients.
- Report concentration, turnover, drawdown, and time-attribution issues plainly.
- Paper outputs should be generated under:
  - `plots_paper/*.png`
  - `paper_tables/*.csv`
  - `paper_explainability_report.md`
  - `paper_explainability_summary.json`
- If `config_lookback` and attribution lookback differ, the paper report must warn that the artifact is not a complete explanation for that lookback.

Plot/backend rules:

- PyQtGraph is for live scalar monitoring from streams such as `epoch_curve.jsonl`; do not put a GUI event loop in the trainer main path.
- Plotly is for optional interactive dashboards from saved CSV artifacts; do not make Plotly a required training dependency.
- SHAP for `transformer_base_portfolio` should use score-head/surrogate SHAP by default. Do not run full `[batch, lookback, symbols, features]` tensor SHAP except as a tiny explicit case study.
- Datashader is the preferred backend for dense scatter, UMAP projections, and GPU-resident high-cardinality plots.
- Do not use Datashader point rasterization for small discrete feature-time matrices; use true grid heatmaps with visible cells, colorbar, subtitles, and `t-0/t-1/...` labels.
- For US full-universe explainability on a 16GB GPU, do not put all sampled days on CUDA at once. Use row microbatching around 4 sampled days for `S≈16800`; measured 32-row explainability completed with ~8.9GB peak VRAM, while 8 rows without row microbatching reached the 16GB ceiling.
- Keep perturbation feature-time batches small for full-universe explainability. Larger perturb batches reduce Python loop count but were slower in practice: a 4-row smoke run with perturb batch 4 took much longer than perturb batch 1 because each forward became a worse large-batch attention workload.
- Cross-asset transmission should chunk both source symbols and sampled rows. Keep `source_chunk_size * row_chunk_size` bounded around 8 repeated rows for `S≈16800` unless a fresh VRAM profile proves more headroom.
- Project-owned cross-asset graph processing and graph explainability should default to cuGraph (`explain_cross_asset_graph_backend: cugraph`). Do not implement new project graph analytics with NetworkX. Keep `networkx` only as an environment/runtime dependency required by PyTorch `torch.compile`/functorch internals; removing it breaks compiled training.
- Static PNG chart labels should avoid CJK text unless a CJK-capable Matplotlib font is confirmed; use ASCII feature-group labels in plots and explain them in the Markdown report.

Walk-forward summary visualization rules:

- Do not recreate `walkforward_first_test_year_only.png`; delete stale copies when refreshing artifacts.
- Top-level walk-forward summary plots should include multiple first-test-year views, not only one equity curve.
- First-test-year summary visuals should use only each fold's first test year, even when the fold's test split contains all future years.
- Keep fold-level first-test-year return/risk, turnover, and concentration views visible so strategy behavior can be judged before later test years dominate the picture.
