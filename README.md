# stockAgent

Multi-asset Taiwan stock trading research workspace.

## Current status

- Raw research data lives under `data_parquet/`.
- Each parquet file represents one stock symbol, for example `2330_features.parquet`.
- The current training and validation specification is documented in `docs/training_spec.md`.
- A concrete experiment template is provided in `configs/experiment_baseline.yaml`.

## Planned workflow

1. Normalize all symbol parquet files into a shared date x symbol panel.
2. Derive the baseline from the same-day average return of all tradable stocks.
3. Run yearly expanding-window walk-forward validation.
4. Train GPU-enabled baseline models first, then portfolio and RL policies.

## Training

- Install dependencies from `requirements.txt` inside the `fintech` environment.
- Run training with `python train.py --config configs/experiment_baseline.yaml --output-dir artifacts`.
- Or use the project runner: `./coda_runner.sh`.
- Runner defaults are centralized in `configs/runner.env`.
- Outputs include one folder per walk-forward fold and a top-level `summary.json`.

## Environment

- Conda or mamba environment: `fintech`
- Training target: CUDA with Tensor Core acceleration
- Recommended activation command: `mamba activate fintech`
mamba env export -n fintech --no-builds > fintech_environment.yml
