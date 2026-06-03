# Project Notes

## Validation Cadence

- `training.val_interval_epochs` is intentionally set to `1`.
- The training loop should validate every epoch for close monitoring of rank IC and checkpoint behavior.
- Do not change this to a larger interval as a speed optimization unless explicitly requested.
- If validation cost becomes too high, optimize the rank validation path itself instead of reducing validation frequency.

