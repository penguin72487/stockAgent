#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
PYTHON = Path(sys.executable).resolve()


def _parse_ints(value: str) -> list[int]:
    return [int(part.strip()) for part in str(value).split(",") if part.strip()]


def _load_jsonl(path: Path) -> list[dict]:
    rows: list[dict] = []
    if not path.exists():
        return rows
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def _score_curve(curve_path: Path, skip_epochs: int) -> dict:
    rows = _load_jsonl(curve_path)
    usable = [row for row in rows if int(row.get("epoch", -1)) >= int(skip_epochs)]
    if not usable:
        usable = rows[-1:]
    if not usable:
        return {"ok": False, "reason": "missing epoch_curve rows"}
    wall = [float(row.get("epoch_wall_s", 0.0)) for row in usable if float(row.get("epoch_wall_s", 0.0)) > 0]
    train_batches = [int(row.get("train_batches", 0)) for row in usable if int(row.get("train_batches", 0)) > 0]
    if not wall:
        return {"ok": False, "reason": "missing epoch_wall_s"}
    mean_wall = sum(wall) / len(wall)
    mean_batches = sum(train_batches) / len(train_batches) if train_batches else 0.0
    return {
        "ok": True,
        "epochs_scored": len(wall),
        "mean_epoch_wall_s": mean_wall,
        "mean_train_batches": mean_batches,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark fixed batch sizes with full tw_boost short training runs.")
    parser.add_argument("--config", default="configs/markets/tw_boost.yaml")
    parser.add_argument("--batch-sizes", default="16,24,32,40,48,64,80,96")
    parser.add_argument("--epochs", type=int, default=8)
    parser.add_argument("--skip-epochs", type=int, default=2)
    parser.add_argument("--start-fold", type=int, default=1)
    parser.add_argument("--max-folds", type=int, default=1)
    parser.add_argument("--output-root", default="artifacts/benchmarks/tw_boost_batch_sizes")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    config_path = (REPO_ROOT / args.config).resolve()
    output_root = (REPO_ROOT / args.output_root).resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    summary_path = output_root / "summary.jsonl"
    summary_rows: list[dict] = []

    base = yaml.safe_load(config_path.read_text())
    batch_sizes = _parse_ints(args.batch_sizes)
    for batch_size in batch_sizes:
        run_dir = output_root / f"batch_{batch_size:03d}"
        cfg_path = run_dir / "config.yaml"
        log_path = run_dir / "train.log"
        if run_dir.exists() and args.force:
            shutil.rmtree(run_dir)
        run_dir.mkdir(parents=True, exist_ok=True)

        cfg = yaml.safe_load(yaml.safe_dump(base, sort_keys=False))
        cfg["runner"]["output_dir"] = str(run_dir)
        cfg["runner"]["resume"] = False
        cfg["runner"]["start_fold"] = int(args.start_fold)
        training = cfg["training"]
        training["batch_size_train"] = int(batch_size)
        training["batch_size_eval"] = int(batch_size)
        training["auto_batch_size"] = False
        training["epochs"] = int(args.epochs)
        training["early_stopping_no_improve_ratio"] = 0.0
        training["save_daily_weights_table"] = False
        training["save_integer_share_daily_weights_table"] = False
        training["save_integer_share_holdings_table"] = False
        training["postprocess_benchmark_after_fold"] = False
        training["postprocess_benchmark_after_best_val"] = False
        cfg_path.write_text(yaml.safe_dump(cfg, sort_keys=False))

        cmd = [
            str(PYTHON),
            "train.py",
            "--config",
            str(cfg_path),
            "--no-resume",
            "--epochs",
            str(args.epochs),
            "--start-fold",
            str(args.start_fold),
            "--max-folds",
            str(args.max_folds),
        ]
        start = time.perf_counter()
        with log_path.open("w") as log_file:
            proc = subprocess.run(
                cmd,
                cwd=REPO_ROOT,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
                check=False,
            )
        elapsed = time.perf_counter() - start

        curve_path = run_dir / "train_2000" / "epoch_curve.jsonl"
        score = _score_curve(curve_path, int(args.skip_epochs))
        row = {
            "batch_size": int(batch_size),
            "returncode": int(proc.returncode),
            "elapsed_s": round(elapsed, 3),
            "output_dir": str(run_dir),
            "log_path": str(log_path),
            **score,
        }
        if proc.returncode == 0 and row.get("ok"):
            batches = float(row.get("mean_train_batches") or 0.0)
            wall = float(row.get("mean_epoch_wall_s") or 0.0)
            row["samples_per_s"] = round((batches * batch_size) / wall, 3) if wall > 0 else 0.0
        else:
            tail = log_path.read_text(errors="replace").splitlines()[-20:]
            row["ok"] = False
            row["reason"] = row.get("reason") or f"returncode={proc.returncode}"
            row["log_tail"] = tail
        summary_rows.append(row)
        summary_path.write_text("\n".join(json.dumps(item, sort_keys=True) for item in summary_rows) + "\n")
        print(json.dumps(row, ensure_ascii=False, sort_keys=True), flush=True)

    ok_rows = [row for row in summary_rows if row.get("ok") and row.get("returncode") == 0]
    if ok_rows:
        best = max(ok_rows, key=lambda row: float(row.get("samples_per_s", 0.0)))
        (output_root / "best.json").write_text(json.dumps(best, indent=2, sort_keys=True) + "\n")
        print("BEST " + json.dumps(best, ensure_ascii=False, sort_keys=True), flush=True)
    else:
        print("No successful batch-size benchmark runs.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
