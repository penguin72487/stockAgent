from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = REPO_ROOT / "configs/markets/tw_parallel_latefold_gated_net_short12.yaml"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "artifacts/markets/tw_parallel_latefold_gated_net_short12_repeats"
DEFAULT_RUNNER = REPO_ROOT / "coda_runner.sh"


def parse_seed_spec(raw: str) -> list[int]:
    seeds: list[int] = []
    for item in str(raw).replace(" ", "").split(","):
        if not item:
            continue
        if "-" in item:
            left, right = item.split("-", 1)
            start = int(left)
            end = int(right)
            step = 1 if end >= start else -1
            seeds.extend(range(start, end + step, step))
        else:
            seeds.append(int(item))
    return list(dict.fromkeys(seeds))


def _metric(metrics: dict[str, Any], split: str, key: str) -> float | None:
    section = metrics.get(f"{split}_metrics") or {}
    value = section.get(key)
    if value is None:
        return None
    return float(value)


def _load_fold_metrics(output_dir: Path, fold: int) -> dict[str, Any] | None:
    path = output_dir / f"fold_{fold:02d}" / "metrics.json"
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _last_epoch_record(output_dir: Path) -> dict[str, Any] | None:
    latest: tuple[float, dict[str, Any]] | None = None
    for path in output_dir.glob("train_*/epoch_curve.jsonl"):
        try:
            mtime = path.stat().st_mtime
            lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
            if not lines:
                continue
            record = json.loads(lines[-1])
        except (OSError, json.JSONDecodeError):
            continue
        if latest is None or mtime > latest[0]:
            latest = (mtime, record)
    return None if latest is None else latest[1]


def collect_run(output_dir: Path, seed: int, folds: list[int]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    complete_count = 0
    all_gates_count = 0
    last_epoch = _last_epoch_record(output_dir)
    train_loss = None if last_epoch is None else last_epoch.get("train_loss")
    for fold in folds:
        metrics = _load_fold_metrics(output_dir, fold)
        row: dict[str, Any] = {
            "seed": int(seed),
            "fold": int(fold),
            "output_dir": str(output_dir),
            "status": "missing",
            "train_loss": train_loss,
        }
        if metrics is None:
            rows.append(row)
            continue
        complete = (output_dir / f"fold_{fold:02d}" / "fold_complete.json").exists()
        val_return = _metric(metrics, "val", "cumulative_return")
        test_return = _metric(metrics, "test", "cumulative_return")
        test_sharpe = _metric(metrics, "test", "sharpe")
        test_sortino = _metric(metrics, "test", "sortino")
        row.update(
            {
                "status": "complete" if complete else "metrics_only",
                "train_years": "|".join(str(year) for year in metrics.get("train_years", [])),
                "val_years": "|".join(str(year) for year in metrics.get("val_years", [])),
                "test_years": "|".join(str(year) for year in metrics.get("test_years", [])),
                "best_val_loss": metrics.get("best_val_loss"),
                "val_cumulative_return": val_return,
                "test_cumulative_return": test_return,
                "test_sharpe": test_sharpe,
                "test_sortino": test_sortino,
                "test_max_drawdown": _metric(metrics, "test", "max_drawdown"),
                "test_turnover": _metric(metrics, "test", "turnover"),
            }
        )
        val_ok = val_return is not None and val_return > 0.0
        test_ok = test_return is not None and test_return > 0.0
        sharpe_ok = test_sharpe is not None and test_sharpe > 0.0
        sortino_ok = test_sortino is not None and test_sortino > 0.0
        row["val_profitable"] = val_ok
        row["test_profitable"] = test_ok
        row["test_positive_sharpe"] = sharpe_ok
        row["test_positive_sortino"] = sortino_ok
        row["all_gates"] = bool(complete and val_ok and test_ok and sharpe_ok and sortino_ok)
        complete_count += int(complete)
        all_gates_count += int(bool(row["all_gates"]))
        rows.append(row)

    run_summary = {
        "seed": int(seed),
        "output_dir": str(output_dir),
        "folds_requested": len(folds),
        "folds_complete": complete_count,
        "folds_all_gates": all_gates_count,
        "run_all_gates": bool(complete_count == len(folds) and all_gates_count == len(folds)),
        "train_loss": train_loss,
    }
    return rows, run_summary


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _write_reports(output_root: Path, fold_rows: list[dict[str, Any]], run_rows: list[dict[str, Any]]) -> None:
    _write_csv(output_root / "fold_metrics.csv", fold_rows)
    _write_csv(output_root / "run_summary.csv", run_rows)
    total = len(run_rows)
    complete = sum(1 for row in run_rows if row.get("folds_complete") == row.get("folds_requested"))
    passed = sum(1 for row in run_rows if row.get("run_all_gates"))
    missing = total - complete
    summary = {
        "runs_requested": total,
        "runs_complete": complete,
        "runs_missing_or_incomplete": missing,
        "runs_all_gates": passed,
        "run_all_gates_rate": (passed / total) if total else None,
        "run_all_gates_rate_complete": (passed / complete) if complete else None,
        "fold_rows": len(fold_rows),
    }
    (output_root / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")


def _print_summary(output_root: Path) -> None:
    summary = json.loads((output_root / "summary.json").read_text(encoding="utf-8"))
    print(
        f"[summary] runs_all_gates={summary['runs_all_gates']}/{summary['runs_requested']} "
        f"rate={summary['run_all_gates_rate']} "
        f"complete_rate={summary.get('run_all_gates_rate_complete')} "
        f"complete={summary['runs_complete']} "
        f"missing={summary.get('runs_missing_or_incomplete', 0)}",
        flush=True,
    )


def _run_output_dir(output_root: Path, seed: int) -> Path:
    return output_root / f"seed_{seed:03d}"


def _build_command(args: argparse.Namespace, seed: int, output_dir: Path) -> list[str]:
    command = [
        str(args.runner),
        "-c",
        str(args.config),
        "--",
        "--seed",
        str(seed),
        "--output-dir",
        str(output_dir),
        "--start-fold",
        str(args.start_fold),
        "--max-folds",
        str(args.max_folds),
    ]
    if args.no_resume:
        command.append("--no-resume")
    if args.retrain_completed_folds:
        command.append("--retrain-completed-folds")
    if args.epochs is not None:
        command.extend(["--epochs", str(args.epochs)])
    if args.multi_gpu_strategy:
        command.extend(["--multi-gpu-strategy", args.multi_gpu_strategy])
    if args.data_parallel_device_ids:
        command.extend(["--data-parallel-device-ids", args.data_parallel_device_ids])
    return command


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run repeated late-fold TW profitability convergence tests across seeds."
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--runner", type=Path, default=DEFAULT_RUNNER)
    parser.add_argument("--seeds", default="0-99", help="Comma list and/or ranges, e.g. 0-99 or 1,7,11.")
    parser.add_argument("--start-fold", type=int, default=24)
    parser.add_argument("--max-folds", type=int, default=3)
    parser.add_argument("--epochs", type=int, default=None, help="Optional smoke override; omit for config epochs.")
    parser.add_argument("--no-resume", action="store_true", help="Pass --no-resume to train.py.")
    parser.add_argument("--retrain-completed-folds", action="store_true")
    parser.add_argument("--skip-existing", action="store_true", default=True)
    parser.add_argument("--no-skip-existing", action="store_false", dest="skip_existing")
    parser.add_argument("--collect-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--stop-on-fail", action="store_true")
    parser.add_argument("--multi-gpu-strategy", default=None)
    parser.add_argument("--data-parallel-device-ids", default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    seeds = parse_seed_spec(args.seeds)
    if not seeds:
        raise SystemExit("No seeds requested.")
    folds = list(range(int(args.start_fold), int(args.start_fold) + int(args.max_folds)))
    output_root = args.output_root
    output_root.mkdir(parents=True, exist_ok=True)

    all_fold_rows: list[dict[str, Any]] = []
    all_run_rows: list[dict[str, Any]] = []
    for seed in seeds:
        run_dir = _run_output_dir(output_root, seed)
        command = _build_command(args, seed, run_dir)
        if args.dry_run:
            print(" ".join(command))
            continue

        existing_rows, existing_summary = collect_run(run_dir, seed, folds)
        if args.collect_only or (args.skip_existing and existing_summary["run_all_gates"]):
            all_fold_rows.extend(existing_rows)
            all_run_rows.append(existing_summary)
            print(
                f"[seed {seed}] collected existing "
                f"complete={existing_summary['folds_complete']}/{existing_summary['folds_requested']} "
                f"gates={existing_summary['folds_all_gates']}/{existing_summary['folds_requested']}"
            )
            continue

        start = time.perf_counter()
        env = os.environ.copy()
        env.setdefault("PYTHONUNBUFFERED", "1")
        print(f"[seed {seed}] running: {' '.join(command)}", flush=True)
        try:
            result = subprocess.run(command, cwd=REPO_ROOT, env=env)
        except KeyboardInterrupt:
            fold_rows, run_summary = collect_run(run_dir, seed, folds)
            run_summary["returncode"] = 130
            run_summary["interrupted"] = True
            all_fold_rows.extend(fold_rows)
            all_run_rows.append(run_summary)
            _write_reports(output_root, all_fold_rows, all_run_rows)
            _print_summary(output_root)
            raise
        elapsed = time.perf_counter() - start
        fold_rows, run_summary = collect_run(run_dir, seed, folds)
        run_summary["elapsed_s"] = elapsed
        run_summary["returncode"] = int(result.returncode)
        all_fold_rows.extend(fold_rows)
        all_run_rows.append(run_summary)
        _write_reports(output_root, all_fold_rows, all_run_rows)
        print(
            f"[seed {seed}] done returncode={result.returncode} "
            f"complete={run_summary['folds_complete']}/{run_summary['folds_requested']} "
            f"gates={run_summary['folds_all_gates']}/{run_summary['folds_requested']} "
            f"elapsed={elapsed:.1f}s",
            flush=True,
        )
        if result.returncode != 0 and args.stop_on_fail:
            raise SystemExit(result.returncode)

    if not args.dry_run:
        _write_reports(output_root, all_fold_rows, all_run_rows)
        _print_summary(output_root)


if __name__ == "__main__":
    main()
