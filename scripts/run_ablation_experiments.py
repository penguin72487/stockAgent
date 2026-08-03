from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shlex
import subprocess
import sys
import time
from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SPEC = REPO_ROOT / "configs/ablations/transformer_base_portfolio.yaml"
DEFAULT_RUNNER = REPO_ROOT / "coda_runner.sh"
_SAFE_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]*$")


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return payload


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = deepcopy(value)
    return merged


def _resolve_path(raw: str | Path, *, relative_to: Path) -> Path:
    path = Path(raw).expanduser()
    return path.resolve() if path.is_absolute() else (relative_to / path).resolve()


def _set_dotted(raw: dict[str, Any], path: str, value: Any) -> None:
    keys = [part.strip() for part in path.split(".") if part.strip()]
    if not keys:
        raise ValueError("ablation parameter path must not be empty")
    cursor = raw
    for key in keys[:-1]:
        child = cursor.setdefault(key, {})
        if not isinstance(child, dict):
            raise ValueError(f"cannot set {path!r}: {key!r} is not a mapping")
        cursor = child
    cursor[keys[-1]] = deepcopy(value)


def _experiment_rows(spec_path: Path, selected: set[str] | None = None) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    spec = _load_yaml(spec_path)
    base_config_raw = spec.get("base_config")
    if not isinstance(base_config_raw, str) or not base_config_raw.strip():
        raise ValueError("ablation spec requires a non-empty base_config")
    matrix = spec.get("matrix")
    if not isinstance(matrix, dict):
        raise ValueError("ablation spec requires a matrix mapping")
    if str(matrix.get("mode", "one_factor_at_a_time")) != "one_factor_at_a_time":
        raise ValueError("only matrix.mode=one_factor_at_a_time is supported")
    dimensions = matrix.get("dimensions")
    if not isinstance(dimensions, list) or not dimensions:
        raise ValueError("ablation spec requires a non-empty matrix.dimensions list")

    rows: list[dict[str, Any]] = []
    if bool(matrix.get("include_baseline", True)):
        rows.append(
            {
                "name": "baseline",
                "dimension": "baseline",
                "description": "Unmodified base configuration.",
                "overrides": {},
            }
        )

    for index, dimension in enumerate(dimensions):
        if not isinstance(dimension, dict):
            raise ValueError(f"matrix.dimensions[{index}] must be a mapping")
        dimension_name = str(dimension.get("name", "")).strip()
        if not _SAFE_NAME.fullmatch(dimension_name):
            raise ValueError(f"invalid dimension name: {dimension_name!r}")
        if not bool(dimension.get("enabled", False)):
            continue
        path = dimension.get("path")
        paths = dimension.get("paths")
        values = dimension.get("values")
        variants = dimension.get("variants")
        path_modes = sum(value is not None for value in (path, paths, variants))
        if path_modes != 1:
            raise ValueError(
                f"dimension {dimension_name!r} requires exactly one of path, paths, or variants"
            )
        if paths is not None and (
            not isinstance(paths, list)
            or not paths
            or not all(isinstance(item, str) and item.strip() for item in paths)
        ):
            raise ValueError(f"dimension {dimension_name!r} paths must be a non-empty string list")
        entries = variants if variants is not None else values
        if not isinstance(entries, list) or not entries:
            raise ValueError(f"dimension {dimension_name!r} has no discrete values")
        for value_index, entry in enumerate(entries):
            if not isinstance(entry, dict):
                raise ValueError(
                    f"dimension {dimension_name!r} value {value_index} must be a mapping"
                )
            label = str(entry.get("name", "")).strip()
            experiment_name = str(entry.get("experiment_name", f"{dimension_name}__{label}"))
            if not _SAFE_NAME.fullmatch(experiment_name):
                raise ValueError(f"invalid experiment name: {experiment_name!r}")
            overrides = entry.get("overrides", {})
            if not isinstance(overrides, dict):
                raise ValueError(f"experiment {experiment_name!r} overrides must be a mapping")
            if path is not None or paths is not None:
                if "value" not in entry:
                    raise ValueError(f"experiment {experiment_name!r} requires value")
                overrides = deepcopy(overrides)
                if paths is not None:
                    discrete_values = entry["value"]
                    if not isinstance(discrete_values, list) or len(discrete_values) != len(paths):
                        raise ValueError(
                            f"experiment {experiment_name!r} value must contain "
                            f"{len(paths)} entries for paths"
                        )
                    for discrete_path, discrete_value in zip(paths, discrete_values, strict=True):
                        _set_dotted(overrides, discrete_path, discrete_value)
                else:
                    _set_dotted(overrides, str(path), entry["value"])
            rows.append(
                {
                    "name": experiment_name,
                    "dimension": dimension_name,
                    "description": str(entry.get("description", dimension.get("description", ""))).strip(),
                    "overrides": overrides,
                }
            )

    seen: set[str] = set()
    for row in rows:
        if row["name"] in seen:
            raise ValueError(f"duplicate experiment name: {row['name']}")
        seen.add(row["name"])
    if selected:
        missing = selected - seen
        if missing:
            raise ValueError(f"unknown experiments: {', '.join(sorted(missing))}")
        rows = [row for row in rows if row["name"] in selected]
    return spec, rows


def _build_configs(
    spec_path: Path,
    spec: dict[str, Any],
    experiments: list[dict[str, Any]],
    output_root: Path,
) -> list[dict[str, Any]]:
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))
    from stockagent.config import _load_raw_config, load_config

    base_path = _resolve_path(spec["base_config"], relative_to=REPO_ROOT)
    base_raw = _load_raw_config(base_path)
    base_overrides = spec.get("matrix", {}).get("base_overrides", {})
    if not isinstance(base_overrides, dict):
        raise ValueError("matrix.base_overrides must be a mapping")
    base_raw = _deep_merge(base_raw, base_overrides)
    fixed_overrides = spec.get("matrix", {}).get("fixed_overrides", {})
    if not isinstance(fixed_overrides, dict):
        raise ValueError("matrix.fixed_overrides must be a mapping")
    generated_root = output_root / "generated_configs"
    generated_root.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, Any]] = []
    for experiment in experiments:
        name = experiment["name"]
        run_dir = output_root / name
        raw = _deep_merge(base_raw, experiment["overrides"])
        # Fixed controls are applied last so no discrete variant can silently
        # change a contract that the matrix declares invariant.
        raw = _deep_merge(raw, fixed_overrides)
        raw["experiment_name"] = f"{base_raw.get('experiment_name', base_path.stem)}-ablation-{name}"
        raw.setdefault("runner", {})
        raw["runner"]["output_dir"] = str(run_dir)
        config_path = generated_root / f"{name}.yaml"
        with config_path.open("w", encoding="utf-8") as handle:
            yaml.safe_dump(raw, handle, sort_keys=False, allow_unicode=True)
        # Validate every effective config before launching the first expensive run.
        load_config(config_path)
        rows.append(
            {
                **experiment,
                "source_config_path": base_path,
                "config_path": config_path,
                "output_dir": run_dir,
            }
        )
    return rows


def _fold_status(output_dir: Path, start_fold: int | None, max_folds: int | None) -> tuple[int, int]:
    if start_fold is not None and max_folds is not None:
        requested = max(0, int(max_folds))
        folds = range(int(start_fold), int(start_fold) + requested)
        complete = sum(
            (output_dir / f"fold_{fold:02d}" / "fold_complete.json").is_file()
            for fold in folds
        )
        return int(complete), requested
    markers = list(output_dir.glob("fold_*/fold_complete.json"))
    return len(markers), len(markers)


def _metric(metrics: dict[str, Any], split: str, key: str) -> float | None:
    value = (metrics.get(f"{split}_metrics") or {}).get(key)
    return None if value is None else float(value)


def _collect_metrics(output_dir: Path) -> dict[str, Any]:
    fold_rows: list[dict[str, Any]] = []
    for path in sorted(output_dir.glob("fold_*/metrics.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        fold_rows.append(
            {
                "test_cumulative_return": _metric(payload, "test", "cumulative_return"),
                "test_sharpe": _metric(payload, "test", "sharpe"),
                "test_sortino": _metric(payload, "test", "sortino"),
                "test_max_drawdown": _metric(payload, "test", "max_drawdown"),
            }
        )

    def average(key: str) -> float | None:
        values = [float(row[key]) for row in fold_rows if row[key] is not None]
        return sum(values) / len(values) if values else None

    drawdowns = [float(row["test_max_drawdown"]) for row in fold_rows if row["test_max_drawdown"] is not None]
    return {
        "folds_with_metrics": len(fold_rows),
        "mean_test_cumulative_return": average("test_cumulative_return"),
        "mean_test_sharpe": average("test_sharpe"),
        "mean_test_sortino": average("test_sortino"),
        "worst_test_max_drawdown": min(drawdowns) if drawdowns else None,
    }


def _write_summary(output_root: Path, rows: list[dict[str, Any]]) -> None:
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with (output_root / "summary.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    (output_root / "summary.json").write_text(
        json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def _print_progress(current: int, total: int, label: str, *, width: int = 28) -> None:
    total = max(1, int(total))
    current = min(max(0, int(current)), total)
    filled = round(width * current / total)
    bar = "█" * filled + "░" * (width - filled)
    percent = 100.0 * current / total
    print(f"[ablation] |{bar}| {current}/{total} ({percent:5.1f}%) {label}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a validated, resumable sequence of stockAgent ablation experiments."
    )
    parser.add_argument("--spec", type=Path, default=DEFAULT_SPEC)
    parser.add_argument("--runner", type=Path, default=DEFAULT_RUNNER)
    parser.add_argument("--output-root", type=Path, default=None)
    parser.add_argument("--only", default=None, help="Comma-separated experiment names.")
    parser.add_argument("--start-fold", type=int, default=None)
    parser.add_argument("--max-folds", type=int, default=None)
    parser.add_argument("--epochs", type=int, default=None)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--multi-gpu-strategy", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--collect-only", action="store_true")
    parser.add_argument("--force", action="store_true", help="Run even when all requested fold markers exist.")
    parser.add_argument("--stop-on-fail", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spec_path = args.spec.resolve()
    selected = (
        {item.strip() for item in args.only.split(",") if item.strip()}
        if args.only
        else None
    )
    spec, experiments = _experiment_rows(spec_path, selected)
    output_root = (
        args.output_root.resolve()
        if args.output_root
        else _resolve_path(
            spec.get("output_root", "artifacts/ablations/transformer_base_portfolio"),
            relative_to=REPO_ROOT,
        )
    )
    output_root.mkdir(parents=True, exist_ok=True)
    runs = _build_configs(spec_path, spec, experiments, output_root)

    summary_rows: list[dict[str, Any]] = []
    total_runs = len(runs)
    _print_progress(0, total_runs, "ready")
    for run_index, run in enumerate(runs, start=1):
        _print_progress(run_index - 1, total_runs, f"starting {run['name']}")
        command = [str(args.runner.resolve()), "-c", str(run["config_path"]), "--"]
        for flag, value in (
            ("--start-fold", args.start_fold),
            ("--max-folds", args.max_folds),
            ("--epochs", args.epochs),
            ("--seed", args.seed),
            ("--multi-gpu-strategy", args.multi_gpu_strategy),
        ):
            if value is not None:
                command.extend([flag, str(value)])

        complete_before, requested = _fold_status(
            run["output_dir"], args.start_fold, args.max_folds
        )
        already_complete = requested > 0 and complete_before == requested
        returncode: int | None = None
        elapsed_s = 0.0
        status = "complete" if already_complete else "pending"
        if args.dry_run:
            print(shlex.join(command))
            status = "dry_run"
        elif args.collect_only:
            status = "complete" if already_complete else "collected"
        elif already_complete and not args.force:
            print(f"[{run['name']}] skip: requested folds already complete", flush=True)
        else:
            print(f"[{run['name']}] running: {shlex.join(command)}", flush=True)
            started = time.perf_counter()
            env = os.environ.copy()
            env.setdefault("PYTHONUNBUFFERED", "1")
            try:
                result = subprocess.run(command, cwd=REPO_ROOT, env=env)
                returncode = int(result.returncode)
            except KeyboardInterrupt:
                returncode = 130
                raise
            finally:
                elapsed_s = time.perf_counter() - started
            status = "succeeded" if returncode == 0 else "failed"

        complete_after, requested_after = _fold_status(
            run["output_dir"], args.start_fold, args.max_folds
        )
        row = {
            "name": run["name"],
            "description": run["description"],
            "status": status,
            "returncode": returncode,
            "elapsed_s": elapsed_s,
            "folds_complete": complete_after,
            "folds_requested": requested_after,
            "output_dir": str(run["output_dir"]),
            "source_config_path": str(run["source_config_path"]),
            "config_path": str(run["config_path"]),
            **_collect_metrics(run["output_dir"]),
        }
        summary_rows.append(row)
        _write_summary(output_root, summary_rows)
        _print_progress(run_index, total_runs, f"{run['name']}: {status}")
        print(
            f"[{run['name']}] {status} folds={complete_after}/{requested_after} "
            f"elapsed={elapsed_s:.1f}s",
            flush=True,
        )
        if returncode not in (None, 0) and args.stop_on_fail:
            raise SystemExit(returncode)

    _write_summary(output_root, summary_rows)
    if any(row["returncode"] not in (None, 0) for row in summary_rows):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
