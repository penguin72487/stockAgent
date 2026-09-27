# Shared training and checkpoint lifecycle

Use when changing neural runners, distributed execution, resume, checkpoint identity or completion artifacts. Keep specialized product semantics inside the canonical lifecycle.

## Trainer Executor Boundaries

- Never guard a distributed phase rendezvous with a mutable shared-file
  existence check. Rank 0 owns checkpoint archival; every pending-fold rank
  enters the same phase even if the file is absent or was already moved by
  rank 0. Phase status payloads must validate rank and phase identity before
  proceeding, so a skipped phase cannot silently match the next collective.
- Checkpoint schema 1--4 construction/validation and ordered universe alignment
  are owned by `stockagent.training.checkpoint_contract`. Training may retain
  private compatibility aliases, but live/explainability code must not import
  trainer orchestration for these semantics. When present, the manifest's
  ordered symbols are authoritative; `symbol_position` is only a capacity hint
  and `daily_weights` is a legacy fallback. A model/data symbol-contract
  disagreement must fail closed.
- Neural training has one lazy `WindowedSplitTensors` executor per process. The
  single-device and torchrun DDP variants share the same canonical model, loss,
  side masks, fees, and stateful backtest semantics.
- A contiguous fixed-shape batch may use `forward_from_panel_slab`; unsupported,
  non-contiguous, or auxiliary-output cases materialize the window inside that same
  guarded executor. These are input representations, not separate loss/backtest
  implementations.
- LightGBM/XGBoost intentionally retain a separate CPU materialized fit/evaluation
  route because they are a different algorithm family. Do not add another neural
  DataLoader or single-process multi-GPU executor.
- The loss path is canonical `risk_aware_loss` plus `run_backtest_torch`; compile it
  when useful, but do not add an alternate return formula.
- Keep `tw_minute` and TX/TXO tick training sessions in strict chronological
  order; do not shuffle the day axis. The canonical progress UI reports
  processed sessions and optimizer batches, while `batch_date` is only an audit
  field. A sample-order change alters the optimizer trajectory and must
  invalidate resume.
- Every neural product/frequency runner must use
  `stockagent.training.lifecycle`: one `TrainingArtifactLayout`, root
  `run_manifest.json`, fixed-envelope `progress.json`, normalized flat
  `epoch_curve.jsonl`, and canonical fold `mode_artifact_contract.json`.
  Compatibility manifest filenames may mirror the canonical manifest but must
  not evolve a second schema. A completed mode smoke test must pass
  `validate_completed_training_artifacts`; mode-specific details belong in
  namespaced checkpoint state and prefixed metrics, not new outer files.
- A lifecycle may publish `state=complete` only when its completed fold IDs
  exactly match the selected manifest IDs and the shared artifact gate passes.
  The gate must fail closed on empty required files, inconsistent
  manifest/progress/summary/fold identities, malformed or incomplete epoch
  rows, missing canonical backtest ZIP members, and invalid plot signatures.
  Keep this completion check structural and lightweight: do not import models,
  execute checkpoint payloads, or decompress full-universe backtest arrays.
  On failure, persist the same progress envelope with `state=failed`.
- Market configs default to `training.multi_gpu_strategy: auto`: use the
  canonical single-device executor with one visible GPU and automatically
  relaunch torchrun/DDP with two or more visible GPUs. GPU visibility and
  assignment belong to `scripts/manage_gpu_jobs.py`; `tw_parallel` means
  within-fold DDP and should remain semantically aligned with `tw.yaml`.

