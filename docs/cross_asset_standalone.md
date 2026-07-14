# Standalone Cross-Asset Explainability

Cross-asset GPU computation is intentionally separate from the primary
`explain_model.py` workflow. This keeps its quadratic source-target artifacts,
GPU wall time, and storage lifecycle independent.

The default command processes every valid date in the first test year of every
fold and every active source/target symbol:

```bash
source scripts/runtime_env.sh
run_fintech_python cross_asset_model.py \
  --config configs/markets/tw_public.yaml \
  --split test \
  --first-test-year-only \
  --max-rows 0
```

For the current training root
`artifacts/markets/tw_public_lantent`, output defaults to:

```text
artifacts/cross_asset/tw_public_lantent/
  cross_asset_run_manifest.json
  fold_01_test/
    cross_asset_runner_timing.json
    abstract_cross_asset_transmission/
  ...
```

A bounded operational run can retain all selected dates and all Cross-asset
modules while limiting the source-target matrix:

```bash
run_fintech_python cross_asset_model.py \
  --config configs/markets/tw_public.yaml \
  --split test \
  --first-test-year-only \
  --max-rows 0 \
  --max-sources 32 \
  --max-targets 32
```

Positive source/target/date limits are explicit reduced runs. A value of zero
means exhaustive coverage of that axis. Use `--cross-asset-output-dir` to place
the independent project on another disk or artifact volume.
