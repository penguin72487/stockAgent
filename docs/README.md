# Documentation Index

Use this page as the starting point. The repository contains both active
contracts and historical engineering snapshots; their status is intentionally
separated below.

## Current contracts

- [`../AGENTS.md`](../AGENTS.md): correctness constraints and latest measured
  engineering recommendations.
- [`training_spec.md`](training_spec.md): first-principles training, evaluation,
  checkpoint, and verification contract.
- [`training_mode_adapter_architecture.md`](training_mode_adapter_architecture.md):
  generic and specialized training-mode boundaries.
- [`tw_execution_modes.md`](tw_execution_modes.md): Taiwan execution-mode
  semantics.
- [`windowed_tensor_pipeline.md`](windowed_tensor_pipeline.md): lazy window and
  panel-slab representations.
- [`temporal_multi_basis.md`](temporal_multi_basis.md): online-safe temporal
  multi-basis representation.

## Data and operations

- [`desync_multiwriter_sync.md`](desync_multiwriter_sync.md): immutable snapshot
  synchronization.
- [`tw_public_download_resume_and_rate_limits.md`](tw_public_download_resume_and_rate_limits.md):
  resumable Taiwan public-data downloads.
- [`openbb_archive_downloader.md`](openbb_archive_downloader.md): OpenBB archive
  ingestion.
- [`okx_historical_features.md`](okx_historical_features.md): OKX historical
  feature contract.
- [`tw_public_explainability_guide.md`](tw_public_explainability_guide.md):
  explainability workflow.
- [`RUN_GUIDE.md`](RUN_GUIDE.md): operator commands; verify configs and paths
  against the current machine before use.

## Strategy-specific references

- [`tw_index_futures_day_strategy.md`](tw_index_futures_day_strategy.md)
- [`tw_index_derivatives_tick_strategy.md`](tw_index_derivatives_tick_strategy.md)
- [`tw_minute_kbar_research.md`](tw_minute_kbar_research.md)
- [`cross_asset_standalone.md`](cross_asset_standalone.md)
- [`shioaji_hft_dataset.md`](shioaji_hft_dataset.md)

## Reviews and historical snapshots

- [`PROJECT_REVIEW_2026-08-10.md`](PROJECT_REVIEW_2026-08-10.md) is the latest
  whole-project review.
- `ARCHITECTURE_REVIEW.md`, `COMPREHENSIVE_ANALYSIS.md`, `FIXES_*`,
  `OPTIMIZATION_*`, `EXECUTIVE_SUMMARY.md`, `ANALYSIS_INDEX.md`, and
  `CODE_ORGANIZATION.md` are dated snapshots. They are useful provenance, but
  must not override current configs, code, tests, or `AGENTS.md`.

When adding a durable document, link it here and state whether it is a current
contract, an operational runbook, a research note, or a historical snapshot.
