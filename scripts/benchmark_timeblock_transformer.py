from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path


def _read_epoch_curve(path: Path) -> list[dict[str, float]]:
    rows: list[dict[str, float]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            rows.append(payload)
    return rows


def _summarize_curve(label: str, path: Path) -> None:
    rows = _read_epoch_curve(path)
    if not rows:
        print(f"{label}: no epoch_curve rows found at {path}")
        return
    wall = [float(row["epoch_wall_s"]) for row in rows if row.get("epoch_wall_s") is not None]
    train = [float(row["train_total_s"]) for row in rows if row.get("train_total_s") is not None]
    forward = [float(row["train_model_forward_s"]) for row in rows if row.get("train_model_forward_s") is not None]
    print(f"{label}: {len(rows)} curve rows from {path}")
    for name, values in (("epoch_wall_s", wall), ("train_total_s", train), ("train_model_forward_s", forward)):
        if values:
            print(
                f"  {name}: median={statistics.median(values):.3f}s "
                f"mean={statistics.fmean(values):.3f}s last={values[-1]:.3f}s"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize or print commands for time-block Transformer benchmark.")
    parser.add_argument("--baseline-curve", type=Path, default=None)
    parser.add_argument("--timeblock-curve", type=Path, default=None)
    args = parser.parse_args()

    print("Baseline command:")
    print("  /home/user/miniforge3/envs/fintech/bin/python train.py --config configs/experiment_baseline.yaml --profile-timing")
    print("Time-block command:")
    print("  /home/user/miniforge3/envs/fintech/bin/python train.py --config configs/experiment_timeblock_transformer_logutil.yaml --profile-timing")
    print()
    print("Compare only after both runs write epoch_curve.jsonl. This helper reports measured numbers only.")
    if args.baseline_curve is not None:
        _summarize_curve("baseline", args.baseline_curve)
    if args.timeblock_curve is not None:
        _summarize_curve("timeblock", args.timeblock_curve)


if __name__ == "__main__":
    main()
