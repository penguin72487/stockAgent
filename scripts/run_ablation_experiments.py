from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
import json
import os
import re
import signal
import shlex
import subprocess
import sys
import time
from copy import deepcopy
from pathlib import Path
from typing import Any, TextIO

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SPEC = REPO_ROOT / "configs/ablations/transformer_base_portfolio.yaml"
DEFAULT_RUNNER = REPO_ROOT / "coda_runner.sh"
_SAFE_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_CUDA_INFRASTRUCTURE_FAILURE_PATTERNS = (
    "cuda is not available in this environment",
    "cuda driver initialization failed",
    "cuda initialization: cuda unknown error",
    "cuda_error_unknown",
    "cudaerrorinitializationerror",
    "found no nvidia driver",
    "driver/library version mismatch",
    "failed to initialize nvml",
    "/dev/nvidia-uvm",
)
_CHECKPOINT_CONTRACT_FAILURE_PATTERNS = (
    "checkpoint semantic fingerprint mismatch",
    "checkpoint settings do not match",
    "checkpoint data fingerprint does not match",
)


@dataclass
class _ActiveRun:
    order: int
    run: dict[str, Any]
    command: list[str]
    process: subprocess.Popen
    started: float
    log_handle: TextIO | None
    log_path: Path | None
    log_start_offset: int
    attempt: int
    consecutive_no_progress_failures: int
    progress_before: tuple[int, int, int]


def _per_job_thread_budget(total: object | None, parallel_jobs: int) -> int | None:
    """Split a host-wide thread budget without producing zero-thread jobs."""

    if total is None:
        return None
    resolved = int(total)
    if resolved <= 0:
        raise ValueError("ablation thread budgets must be positive")
    return max(1, resolved // max(1, int(parallel_jobs)))


def _effective_parallel_jobs(requested: int, multi_gpu_strategy: object) -> int:
    """Respect the GPU ownership contract of an independent training job.

    A DDP experiment already owns every visible GPU. Launching N such jobs does
    not create N times more GPU capacity; it places N ranks on each GPU and can
    OOM during the compile probe. Until the scheduler assigns disjoint device
    sets, all-visible-GPU DDP jobs must run one at a time.
    """

    resolved = int(requested)
    if resolved <= 0:
        raise ValueError("parallel_jobs must be positive")
    strategy = str(multi_gpu_strategy or "").strip().lower().replace("-", "_")
    if strategy in {
        "ddp",
        "distributed",
        "torch_ddp",
        "distributed_data_parallel",
    }:
        return 1
    return resolved


def _latest_jsonl_epoch(path: Path) -> int:
    if not path.is_file():
        return 0
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return 0
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
            return max(0, int(payload.get("epoch", 0)))
        except (AttributeError, TypeError, ValueError, json.JSONDecodeError):
            continue
    return 0


def _run_progress_signature(output_dir: Path) -> tuple[int, int, int]:
    """Cheap durable-work signature used to distinguish progress from loops."""

    complete_folds = sum(
        path.is_file() for path in output_dir.glob("fold_*/fold_complete.json")
    )
    epoch_sum = sum(
        _latest_jsonl_epoch(path)
        for path in output_dir.glob("train_*/epoch_curve.jsonl")
    )
    checkpoint_mtime_ns = 0
    for path in output_dir.glob("train_*/checkpoint_last.pt"):
        try:
            checkpoint_mtime_ns = max(checkpoint_mtime_ns, path.stat().st_mtime_ns)
        except OSError:
            continue
    return int(complete_folds), int(epoch_sum), int(checkpoint_mtime_ns)


def _attempt_log_text(job: _ActiveRun) -> str:
    if job.log_path is None or not job.log_path.is_file():
        return ""
    try:
        with job.log_path.open("rb") as handle:
            handle.seek(max(0, int(job.log_start_offset)))
            return handle.read().decode("utf-8", errors="replace")
    except OSError:
        return ""


def _failure_kind(returncode: int, log_text: str) -> str:
    lowered = log_text.lower()
    if any(pattern in lowered for pattern in _CHECKPOINT_CONTRACT_FAILURE_PATTERNS):
        return "checkpoint_contract_mismatch"
    if "outofmemoryerror" in lowered or "cuda out of memory" in lowered:
        return "cuda_oom"
    if any(
        pattern in lowered for pattern in _CUDA_INFRASTRUCTURE_FAILURE_PATTERNS
    ):
        return "cuda_infrastructure_unavailable"
    if "childfailederror" in lowered:
        return "distributed_worker_failure"
    if returncode < 0 or returncode in {
        128 + signal.SIGINT,
        128 + signal.SIGTERM,
        128 + signal.SIGKILL,
    }:
        return "signal_termination"
    return "worker_failure"


def _cuda_runtime_health() -> tuple[bool, str]:
    """Probe the actual CUDA compute path in an isolated interpreter.

    NVML-backed tools such as nvidia-smi can remain healthy while CUDA Driver
    API initialization or /dev/nvidia-uvm is broken. A one-element allocation
    on every visible device tests the path training actually needs without
    contaminating this long-lived scheduler process with CUDA state.
    """

    probe = """
import torch

if not torch.cuda.is_available():
    raise RuntimeError("torch.cuda.is_available() is false")
count = torch.cuda.device_count()
if count <= 0:
    raise RuntimeError("torch.cuda.device_count() is zero")
for index in range(count):
    value = torch.ones(1, device=f"cuda:{index}")
    torch.cuda.synchronize(index)
    if value.item() != 1.0:
        raise RuntimeError(f"CUDA allocation verification failed on cuda:{index}")
print(f"healthy CUDA devices={count}")
"""
    try:
        completed = subprocess.run(
            [sys.executable, "-c", probe],
            cwd=REPO_ROOT,
            env=os.environ.copy(),
            capture_output=True,
            text=True,
            timeout=30.0,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return False, f"probe failed: {type(exc).__name__}: {exc}"
    output = "\n".join(
        part.strip() for part in (completed.stdout, completed.stderr) if part.strip()
    )
    detail = output.splitlines()[-1] if output else f"returncode={completed.returncode}"
    return completed.returncode == 0, detail


def _descendant_process_ids(root_pid: int) -> list[int]:
    """Snapshot descendants without adding a psutil runtime dependency."""

    try:
        result = subprocess.run(
            ["ps", "-e", "-o", "pid=,ppid="],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.SubprocessError):
        return []
    children: dict[int, list[int]] = {}
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) != 2:
            continue
        try:
            pid, parent = (int(field) for field in fields)
        except ValueError:
            continue
        children.setdefault(parent, []).append(pid)
    descendants: list[int] = []
    frontier = list(children.get(int(root_pid), ()))
    while frontier:
        pid = frontier.pop()
        descendants.append(pid)
        frontier.extend(children.get(pid, ()))
    return descendants


def _pid_is_live(pid: int) -> bool:
    try:
        state = Path(f"/proc/{int(pid)}/stat").read_text(encoding="utf-8").split()[2]
    except (OSError, IndexError):
        return False
    return state != "Z"


def _terminate_process_tree(
    process: subprocess.Popen,
    *,
    grace_s: float = 10.0,
) -> None:
    """Stop a runner plus torchrun ranks that may create their own groups."""

    known = {int(process.pid), *_descendant_process_ids(int(process.pid))}
    for sig, timeout in (
        (signal.SIGINT, grace_s),
        (signal.SIGTERM, grace_s),
        (signal.SIGKILL, 2.0),
    ):
        known.update(_descendant_process_ids(int(process.pid)))
        for pid in sorted(known, reverse=True):
            if not _pid_is_live(pid):
                continue
            try:
                os.kill(pid, sig)
            except ProcessLookupError:
                pass
        try:
            os.killpg(int(process.pid), sig)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not any(_pid_is_live(pid) for pid in known):
                process.poll()
                return
            time.sleep(0.1)
    process.poll()


def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return payload


def _load_ablation_spec(
    path: Path,
    stack: tuple[Path, ...] = (),
) -> dict[str, Any]:
    """Load a reusable ablation matrix with fail-closed single inheritance."""

    resolved = path.expanduser().resolve()
    if resolved in stack:
        cycle = " -> ".join(str(item) for item in (*stack, resolved))
        raise ValueError(f"Ablation spec inheritance cycle detected: {cycle}")
    payload = _load_yaml(resolved)
    base_raw = payload.pop("base_spec", None)
    if base_raw is None:
        return payload
    if not isinstance(base_raw, str) or not base_raw.strip():
        raise ValueError(f"{resolved} base_spec must be a non-empty path")
    base_path = Path(base_raw).expanduser()
    if not base_path.is_absolute():
        base_path = resolved.parent / base_path
    base = _load_ablation_spec(base_path, (*stack, resolved))
    return _deep_merge(base, payload)


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    # These two fields are a coupled contract.  Once an ablation replaces the
    # enabled family set, limits belonging to disabled families must not leak
    # through recursive mapping merge.
    if "temporal_basis_families" in override:
        merged["temporal_basis_components_by_family"] = {}
        merged["temporal_basis_disabled_families"] = []
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = deepcopy(value)
    return merged


def _resolve_path(raw: str | Path, *, relative_to: Path) -> Path:
    path = Path(raw).expanduser()
    return path.resolve() if path.is_absolute() else (relative_to / path).resolve()


def _resolve_pinned_panel_cache_env(spec: dict[str, Any]) -> dict[str, str]:
    """Resolve an immutable panel receipt without embedding host paths in YAML."""

    raw = spec.get("pinned_panel_cache")
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ValueError("pinned_panel_cache must be a mapping")

    explicit_manifest = str(raw.get("manifest_path", "")).strip()
    if explicit_manifest:
        manifest_path = _resolve_path(explicit_manifest, relative_to=REPO_ROOT)
    else:
        snapshot_id = str(raw.get("snapshot_id", "")).strip()
        variant_id = str(raw.get("variant_id", "")).strip()
        if not snapshot_id or Path(snapshot_id).name != snapshot_id:
            raise ValueError(
                "pinned_panel_cache.snapshot_id must be one directory name"
            )
        if not re.fullmatch(r"[0-9a-f]{64}", variant_id):
            raise ValueError(
                "pinned_panel_cache.variant_id must be a 64-character sha256"
            )
        stores: list[Path] = []
        configured_store = os.environ.get(
            "STOCKAGENT_TW_PUBLIC_SNAPSHOT_STORE", ""
        ).strip()
        if configured_store:
            stores.append(Path(configured_store).expanduser())
        active_public = REPO_ROOT / "data_tw_public"
        if active_public.exists():
            try:
                stores.append(active_public.resolve().parent)
            except OSError:
                pass
        stores.append(Path("/srv/stockagent-snapshots/tw-public"))
        candidates: list[Path] = []
        seen: set[Path] = set()
        for store in stores:
            resolved_store = store.resolve()
            if resolved_store in seen:
                continue
            seen.add(resolved_store)
            candidates.append(
                resolved_store
                / snapshot_id
                / "stocks"
                / "panel_cache_v2"
                / "variants"
                / f"{variant_id}.json"
            )
        manifest_path = next(
            (candidate for candidate in candidates if candidate.is_file()),
            candidates[0] if candidates else Path(),
        )
        if not manifest_path.is_file():
            raise FileNotFoundError(
                "pinned panel cache manifest was not found; searched: "
                + ", ".join(str(candidate) for candidate in candidates)
            )

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"invalid pinned panel cache manifest: {manifest_path}"
        ) from exc
    if not isinstance(manifest, dict):
        raise ValueError(
            f"pinned panel cache manifest must be a mapping: {manifest_path}"
        )

    expected = {
        "version": str(raw.get("version", "")).strip(),
        "generation": str(raw.get("generation", "")).strip(),
        "source_hash": str(raw.get("source_hash", "")).strip(),
    }
    actual = {
        "version": str(manifest.get("version", "")).strip(),
        "generation": str(manifest.get("generation", "")).strip(),
        "source_hash": str(manifest.get("source_hash", "")).strip(),
    }
    missing = [name for name, value in expected.items() if not value]
    if missing:
        raise ValueError(
            "pinned_panel_cache requires explicit " + ", ".join(missing)
        )
    mismatches = [
        f"{name}: expected={expected[name]} actual={actual[name]}"
        for name in expected
        if expected[name] != actual[name]
    ]
    if mismatches:
        raise ValueError(
            "pinned panel cache identity mismatch ("
            + "; ".join(mismatches)
            + f"): {manifest_path}"
        )
    return {
        "STOCKAGENT_PINNED_PANEL_CACHE_MANIFEST": str(manifest_path.resolve()),
        "STOCKAGENT_PINNED_PANEL_CACHE_VERSION": expected["version"],
        "STOCKAGENT_PINNED_PANEL_CACHE_GENERATION": expected["generation"],
        "STOCKAGENT_PINNED_PANEL_CACHE_SOURCE_HASH": expected["source_hash"],
    }


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
    spec = _load_ablation_spec(spec_path)
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

    # A child spec often needs to repair one inherited experiment without
    # copying the entire dimensions list. Lists intentionally replace rather
    # than deep-merge, so provide a narrow, fail-closed patch surface keyed by
    # the inherited experiment name. Renaming is important when the effective
    # config changes: it prevents checkpoints and fold artifacts produced by
    # the old contract from being resumed under the repaired one.
    experiment_overrides = matrix.get("experiment_overrides", {})
    if not isinstance(experiment_overrides, dict):
        raise ValueError("matrix.experiment_overrides must be a mapping")
    rows_by_name = {str(row["name"]): row for row in rows}
    unknown_overrides = sorted(set(experiment_overrides) - set(rows_by_name))
    if unknown_overrides:
        raise ValueError(
            "matrix.experiment_overrides references unknown experiments: "
            + ", ".join(unknown_overrides)
        )
    for inherited_name, patch in experiment_overrides.items():
        if not isinstance(patch, dict):
            raise ValueError(
                "matrix.experiment_overrides entries must be mappings: "
                f"{inherited_name}"
            )
        unsupported = sorted(
            set(patch) - {"experiment_name", "description", "overrides"}
        )
        if unsupported:
            raise ValueError(
                f"matrix.experiment_overrides.{inherited_name} has unsupported "
                f"keys: {', '.join(unsupported)}"
            )
        row = rows_by_name[str(inherited_name)]
        replacement_name = str(
            patch.get("experiment_name", inherited_name)
        ).strip()
        if not _SAFE_NAME.fullmatch(replacement_name):
            raise ValueError(f"invalid experiment name: {replacement_name!r}")
        override_patch = patch.get("overrides", {})
        if not isinstance(override_patch, dict):
            raise ValueError(
                "matrix.experiment_overrides."
                f"{inherited_name}.overrides must be a mapping"
            )
        row["name"] = replacement_name
        row["overrides"] = _deep_merge(row["overrides"], override_patch)
        if "description" in patch:
            row["description"] = str(patch["description"]).strip()

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


def _fold_status(
    output_dir: Path,
    start_fold: int | None,
    max_folds: int | None,
    expected_fold_count: int | None = None,
) -> tuple[int, int | None]:
    first_fold = 1 if start_fold is None else int(start_fold)
    if max_folds is not None:
        requested = max(0, int(max_folds))
        folds = range(first_fold, first_fold + requested)
        complete = sum(
            (output_dir / f"fold_{fold:02d}" / "fold_complete.json").is_file()
            for fold in folds
        )
        return int(complete), requested
    if expected_fold_count is not None:
        last_fold = int(expected_fold_count)
        requested = max(0, last_fold - first_fold + 1)
        folds = range(first_fold, last_fold + 1)
        complete = sum(
            (output_dir / f"fold_{fold:02d}" / "fold_complete.json").is_file()
            for fold in folds
        )
        return int(complete), requested
    markers = list(output_dir.glob("fold_*/fold_complete.json"))
    # Without an explicit bounded range, marker count does not tell us how many
    # folds the effective panel will generate. Treating N existing markers as
    # N/N complete incorrectly skips interrupted experiments. Let the canonical
    # trainer inspect and resume those folds instead.
    return len(markers), None


def _format_fold_status(complete: int, requested: int | None) -> str:
    return f"{complete}/{requested if requested is not None else '?'}"


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
    parser.add_argument(
        "--cpu-threads",
        type=int,
        default=None,
        help="Host-wide runtime thread budget; defaults to spec.runtime.cpu_threads.",
    )
    parser.add_argument(
        "--torch-compile-threads",
        type=int,
        default=None,
        help="Host-wide Inductor worker budget; defaults to spec.runtime.torch_compile_threads.",
    )
    parser.add_argument(
        "--parallel-jobs",
        type=int,
        default=None,
        help=(
            "Concurrent independent experiments. Each job keeps its configured "
            "single-device/DDP semantics; host-wide CPU budgets are divided "
            "across jobs. Defaults to spec.runtime.parallel_jobs or 1."
        ),
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--collect-only", action="store_true")
    parser.add_argument("--force", action="store_true", help="Run even when all requested fold markers exist.")
    parser.add_argument("--stop-on-fail", action="store_true")
    parser.add_argument(
        "--auto-resume",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Automatically requeue failed experiments from their durable "
            "fold/epoch checkpoints (default: enabled)."
        ),
    )
    parser.add_argument(
        "--max-no-progress-retries",
        type=int,
        default=3,
        help=(
            "Maximum consecutive retries that produce no new fold, epoch, or "
            "checkpoint progress. Progress resets this counter."
        ),
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=5.0,
        help="Initial automatic-resume backoff; consecutive delays are exponential.",
    )
    parser.add_argument(
        "--cuda-health-poll-seconds",
        type=float,
        default=30.0,
        help=(
            "Seconds between real CUDA allocation probes after a driver/UVM "
            "failure. Infrastructure waiting does not consume experiment retries."
        ),
    )
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
    pinned_panel_cache_env = _resolve_pinned_panel_cache_env(spec)
    if pinned_panel_cache_env:
        print(
            "[ablation] pinned panel cache "
            f"generation={pinned_panel_cache_env['STOCKAGENT_PINNED_PANEL_CACHE_GENERATION']} "
            f"manifest={pinned_panel_cache_env['STOCKAGENT_PINNED_PANEL_CACHE_MANIFEST']}",
            flush=True,
        )
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
    runtime = spec.get("runtime", {})
    if not isinstance(runtime, dict):
        raise ValueError("ablation spec runtime must be a mapping")
    cpu_threads = args.cpu_threads if args.cpu_threads is not None else runtime.get("cpu_threads")
    compile_threads = (
        args.torch_compile_threads
        if args.torch_compile_threads is not None
        else runtime.get("torch_compile_threads")
    )
    requested_parallel_jobs = int(
        args.parallel_jobs
        if args.parallel_jobs is not None
        else runtime.get("parallel_jobs", 1)
    )
    parallel_jobs = _effective_parallel_jobs(
        requested_parallel_jobs,
        args.multi_gpu_strategy,
    )
    if args.max_no_progress_retries < 0:
        raise ValueError("max_no_progress_retries must be non-negative")
    if args.retry_backoff_seconds < 0:
        raise ValueError("retry_backoff_seconds must be non-negative")
    if args.cuda_health_poll_seconds <= 0:
        raise ValueError("cuda_health_poll_seconds must be positive")
    job_cpu_threads = _per_job_thread_budget(cpu_threads, parallel_jobs)
    job_compile_threads = _per_job_thread_budget(compile_threads, parallel_jobs)
    expected_fold_count_raw = spec.get("expected_fold_count")
    expected_fold_count = (
        None if expected_fold_count_raw is None else int(expected_fold_count_raw)
    )
    if expected_fold_count is not None and expected_fold_count <= 0:
        raise ValueError("ablation spec expected_fold_count must be positive")

    summary_by_order: dict[int, dict[str, Any]] = {}
    total_runs = len(runs)
    _print_progress(
        0,
        total_runs,
        f"ready parallel_jobs={parallel_jobs} "
        f"requested_parallel_jobs={requested_parallel_jobs} "
        f"cpu_threads/job={job_cpu_threads or 'config'} "
        f"compile_threads/job={job_compile_threads or 'config'}",
    )
    if parallel_jobs != requested_parallel_jobs:
        print(
            "[scheduler] capped independent experiment concurrency from "
            f"{requested_parallel_jobs} to {parallel_jobs}: "
            "distributed_data_parallel already owns every visible GPU",
            flush=True,
        )

    def write_current_summary() -> None:
        _write_summary(
            output_root,
            [summary_by_order[index] for index in sorted(summary_by_order)],
        )

    def record_result(
        *,
        order: int,
        run: dict[str, Any],
        status: str,
        returncode: int | None,
        elapsed_s: float,
        attempts: int = 0,
        failure_kind: str | None = None,
        consecutive_no_progress_failures: int = 0,
    ) -> None:
        complete_after, requested_after = _fold_status(
            run["output_dir"],
            args.start_fold,
            args.max_folds,
            expected_fold_count,
        )
        summary_by_order[order] = {
            "name": run["name"],
            "description": run["description"],
            "status": status,
            "returncode": returncode,
            "elapsed_s": elapsed_s,
            "attempts": int(attempts),
            "failure_kind": failure_kind,
            "consecutive_no_progress_failures": int(
                consecutive_no_progress_failures
            ),
            "folds_complete": complete_after,
            "folds_requested": requested_after,
            "output_dir": str(run["output_dir"]),
            "source_config_path": str(run["source_config_path"]),
            "config_path": str(run["config_path"]),
            **_collect_metrics(run["output_dir"]),
        }
        write_current_summary()
        _print_progress(
            len(summary_by_order), total_runs, f"{run['name']}: {status}"
        )
        print(
            f"[{run['name']}] {status} "
            f"folds={_format_fold_status(complete_after, requested_after)} "
            f"elapsed={elapsed_s:.1f}s",
            flush=True,
        )

    pending: list[dict[str, Any]] = []
    for run_index, run in enumerate(runs, start=1):
        command = [str(args.runner.resolve()), "-c", str(run["config_path"]), "--"]
        for flag, value in (
            ("--start-fold", args.start_fold),
            ("--max-folds", args.max_folds),
            ("--epochs", args.epochs),
            ("--seed", args.seed),
            ("--multi-gpu-strategy", args.multi_gpu_strategy),
            ("--cpu-threads", job_cpu_threads),
            ("--torch-compile-threads", job_compile_threads),
        ):
            if value is not None:
                command.extend([flag, str(value)])

        complete_before, requested = _fold_status(
            run["output_dir"],
            args.start_fold,
            args.max_folds,
            expected_fold_count,
        )
        already_complete = (
            requested is not None
            and requested > 0
            and complete_before == requested
        )
        if args.dry_run:
            print(shlex.join(command))
            continue
        elif args.collect_only:
            record_result(
                order=run_index,
                run=run,
                status="complete" if already_complete else "collected",
                returncode=None,
                elapsed_s=0.0,
            )
        elif already_complete and not args.force:
            print(f"[{run['name']}] skip: requested folds already complete", flush=True)
            record_result(
                order=run_index,
                run=run,
                status="complete",
                returncode=None,
                elapsed_s=0.0,
            )
        else:
            pending.append(
                {
                    "order": run_index,
                    "run": run,
                    "command": command,
                    "attempt": 1,
                    "consecutive_no_progress_failures": 0,
                    "ready_at": 0.0,
                }
            )

    if args.dry_run:
        _print_progress(total_runs, total_runs, "dry-run configs validated")
        return

    active: dict[int, _ActiveRun] = {}
    first_failure: int | None = None
    cuda_health_blocked = False
    next_cuda_health_probe_at = 0.0
    try:
        while pending or active:
            while pending and len(active) < parallel_jobs and first_failure is None:
                now = time.monotonic()
                if cuda_health_blocked:
                    if now < next_cuda_health_probe_at:
                        break
                    cuda_healthy, cuda_health_detail = _cuda_runtime_health()
                    if not cuda_healthy:
                        next_cuda_health_probe_at = (
                            now + float(args.cuda_health_poll_seconds)
                        )
                        print(
                            "[scheduler] CUDA compute path is still unhealthy; "
                            "all pending experiments remain paused and no retry "
                            "budget is consumed. "
                            f"next_probe={args.cuda_health_poll_seconds:.1f}s "
                            f"detail={cuda_health_detail}",
                            flush=True,
                        )
                        break
                    cuda_health_blocked = False
                    next_cuda_health_probe_at = 0.0
                    print(
                        "[scheduler] CUDA compute path recovered; resuming the "
                        f"same experiment ({cuda_health_detail})",
                        flush=True,
                    )
                if parallel_jobs == 1:
                    # Sequential ablations are experiment-major. If the
                    # current experiment is backing off before an automatic
                    # resume, wait for it instead of skipping ahead to a
                    # sibling experiment. This keeps folds 1..N together and
                    # gives one experiment exclusive ownership until success
                    # or terminal failure.
                    ready_index = (
                        0
                        if float(pending[0].get("ready_at", 0.0)) <= now
                        else None
                    )
                else:
                    ready_index = next(
                        (
                            index
                            for index, candidate in enumerate(pending)
                            if float(candidate.get("ready_at", 0.0)) <= now
                        ),
                        None,
                    )
                if ready_index is None:
                    break
                item = pending.pop(ready_index)
                run = item["run"]
                command = item["command"]
                attempt = int(item.get("attempt", 1))
                env = os.environ.copy()
                env.setdefault("PYTHONUNBUFFERED", "1")
                env.update(pinned_panel_cache_env)
                log_handle: TextIO | None = None
                stdout = None
                worker_log: Path | None = None
                log_start_offset = 0
                if parallel_jobs > 1 or bool(args.auto_resume):
                    worker_log = run["output_dir"] / "ablation_worker.log"
                    worker_log.parent.mkdir(parents=True, exist_ok=True)
                    try:
                        log_start_offset = worker_log.stat().st_size
                    except OSError:
                        log_start_offset = 0
                    log_handle = worker_log.open(
                        "a", encoding="utf-8", buffering=1
                    )
                    log_handle.write(
                        f"\n[ablation worker] attempt={attempt} "
                        f"command={shlex.join(command)}\n"
                    )
                    stdout = log_handle
                print(
                    f"[{run['name']}] running attempt={attempt}: "
                    f"{shlex.join(command)}"
                    + (
                        f" (log={run['output_dir'] / 'ablation_worker.log'})"
                        if worker_log is not None
                        else ""
                    ),
                    flush=True,
                )
                try:
                    process = subprocess.Popen(
                        command,
                        cwd=REPO_ROOT,
                        env=env,
                        stdout=stdout,
                        stderr=(
                            subprocess.STDOUT if log_handle is not None else None
                        ),
                        start_new_session=True,
                        text=True,
                    )
                except OSError:
                    if log_handle is not None:
                        log_handle.close()
                    record_result(
                        order=item["order"],
                        run=run,
                        status="failed_to_launch",
                        returncode=127,
                        elapsed_s=0.0,
                    )
                    if args.stop_on_fail:
                        first_failure = 127
                    continue
                active[int(process.pid)] = _ActiveRun(
                    order=item["order"],
                    run=run,
                    command=command,
                    process=process,
                    started=time.perf_counter(),
                    log_handle=log_handle,
                    log_path=worker_log,
                    log_start_offset=log_start_offset,
                    attempt=attempt,
                    consecutive_no_progress_failures=int(
                        item.get("consecutive_no_progress_failures", 0)
                    ),
                    progress_before=_run_progress_signature(run["output_dir"]),
                )

            finished = [
                pid
                for pid, job in active.items()
                if job.process.poll() is not None
            ]
            if not finished:
                if active:
                    time.sleep(0.25)
                    continue
                if pending:
                    next_ready = (
                        next_cuda_health_probe_at
                        if cuda_health_blocked
                        else min(
                            float(item.get("ready_at", 0.0)) for item in pending
                        )
                    )
                    delay = max(0.01, min(0.25, next_ready - time.monotonic()))
                    time.sleep(delay)
                    continue
                break
            for pid in finished:
                job = active.pop(pid)
                returncode = int(job.process.returncode or 0)
                elapsed_s = time.perf_counter() - job.started
                if job.log_handle is not None:
                    job.log_handle.close()
                failure_kind: str | None = None
                no_progress_failures = job.consecutive_no_progress_failures
                if returncode != 0:
                    failure_kind = _failure_kind(
                        returncode,
                        _attempt_log_text(job),
                    )
                    progress_after = _run_progress_signature(job.run["output_dir"])
                    made_progress = progress_after > job.progress_before
                    infrastructure_wait = (
                        failure_kind == "cuda_infrastructure_unavailable"
                    )
                    non_retryable_contract_failure = (
                        failure_kind == "checkpoint_contract_mismatch"
                    )
                    if infrastructure_wait:
                        # A host driver/UVM outage is not evidence that this
                        # experiment cannot progress. Preserve its retry budget
                        # and pause the entire queue so sibling experiments do
                        # not burn their own retries against the same bad host.
                        no_progress_failures = (
                            0
                            if made_progress
                            else job.consecutive_no_progress_failures
                        )
                        retry_allowed = bool(args.auto_resume)
                    elif non_retryable_contract_failure:
                        no_progress_failures = (
                            0
                            if made_progress
                            else job.consecutive_no_progress_failures + 1
                        )
                        retry_allowed = False
                    else:
                        no_progress_failures = (
                            0
                            if made_progress
                            else job.consecutive_no_progress_failures + 1
                        )
                        retry_allowed = bool(args.auto_resume) and (
                            made_progress
                            or no_progress_failures <= args.max_no_progress_retries
                        )
                    if retry_allowed:
                        if infrastructure_wait:
                            retry_delay = 0.0
                            cuda_health_blocked = True
                            next_cuda_health_probe_at = time.monotonic()
                        else:
                            exponent = max(0, no_progress_failures - 1)
                            retry_delay = min(
                                300.0,
                                float(args.retry_backoff_seconds) * (2**exponent),
                            )
                        retry_item = {
                            "order": job.order,
                            "run": job.run,
                            "command": job.command,
                            "attempt": job.attempt + 1,
                            "consecutive_no_progress_failures": (
                                no_progress_failures
                            ),
                            "ready_at": time.monotonic() + retry_delay,
                        }
                        if infrastructure_wait or parallel_jobs == 1:
                            pending.insert(0, retry_item)
                        else:
                            pending.append(retry_item)
                        complete_after, requested_after = _fold_status(
                            job.run["output_dir"],
                            args.start_fold,
                            args.max_folds,
                            expected_fold_count,
                        )
                        retry_label = (
                            "CUDA infrastructure wait queued"
                            if infrastructure_wait
                            else "auto-resume queued"
                        )
                        retry_suffix = (
                            "retry_budget_consumed=no"
                            if infrastructure_wait
                            else f"backoff={retry_delay:.1f}s"
                        )
                        print(
                            f"[{job.run['name']}] {retry_label} "
                            f"attempt={job.attempt + 1} kind={failure_kind} "
                            f"progress={'yes' if made_progress else 'no'} "
                            f"no_progress_failures={no_progress_failures}/"
                            f"{args.max_no_progress_retries} "
                            f"folds={_format_fold_status(complete_after, requested_after)} "
                            f"{retry_suffix}",
                            flush=True,
                        )
                        continue
                record_result(
                    order=job.order,
                    run=job.run,
                    status="succeeded" if returncode == 0 else "failed",
                    returncode=returncode,
                    elapsed_s=elapsed_s,
                    attempts=job.attempt,
                    failure_kind=failure_kind,
                    consecutive_no_progress_failures=no_progress_failures,
                )
                if returncode != 0 and args.stop_on_fail and first_failure is None:
                    first_failure = returncode

            if first_failure is not None:
                for job in list(active.values()):
                    print(
                        f"[{job.run['name']}] stopping after peer failure",
                        flush=True,
                    )
                    _terminate_process_tree(job.process)
                    returncode = job.process.poll()
                    if returncode is None:
                        returncode = job.process.wait()
                    if job.log_handle is not None:
                        job.log_handle.close()
                    record_result(
                        order=job.order,
                        run=job.run,
                        status="cancelled_after_peer_failure",
                        returncode=int(returncode),
                        elapsed_s=time.perf_counter() - job.started,
                    )
                active.clear()
                break
    except KeyboardInterrupt:
        print(
            "\n[ablation] interrupt received; stopping every runner and "
            "torchrun descendant...",
            flush=True,
        )
        for job in list(active.values()):
            _terminate_process_tree(job.process)
            if job.log_handle is not None:
                job.log_handle.close()
        raise SystemExit(130)

    write_current_summary()
    if first_failure is not None:
        raise SystemExit(first_failure)
    summary_rows = [summary_by_order[index] for index in sorted(summary_by_order)]
    if any(row["returncode"] not in (None, 0) for row in summary_rows):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
