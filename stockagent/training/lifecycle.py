"""Stable outer lifecycle and artifact contracts for every training mode.

Market adapters own data clocks and execution ledgers.  This module owns the
observable shell around them: run identity, persistent progress, group/fold
paths, and completed-artifact conformance.  Keeping this layer free of model
and executor imports lets daily, minute, and tick runners share it without
creating a second training implementation.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import time
from typing import Any, Iterable, Mapping, Sequence

from stockagent.training.mode_adapter import (
    TrainingModeSpec,
    training_mode_spec,
    write_training_json,
)


TRAINING_LIFECYCLE_SCHEMA_VERSION = 1
TRAINING_PROGRESS_SCHEMA_VERSION = 1
TRAINING_ARTIFACT_LAYOUT_VERSION = 1
TRAINING_EPOCH_CURVE_SCHEMA_VERSION = 1

TRAINING_PHASES = frozenset(
    {
        "setup",
        "training",
        "validation",
        "testing",
        "reporting",
        "complete",
        "failed",
    }
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_component(value: object, *, label: str) -> str:
    text = str(value).strip()
    if (
        not text
        or text in {".", ".."}
        or "/" in text
        or "\\" in text
        or Path(text).name != text
    ):
        raise ValueError(f"invalid {label}: {value!r}")
    return text


def _optional_metric(value: object) -> float | int | str | bool | None:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int):
        return int(value)
    if isinstance(value, float):
        return float(value) if math.isfinite(value) else None
    try:
        converted = float(value)
    except (TypeError, ValueError):
        return str(value)
    return converted if math.isfinite(converted) else None


@dataclass(frozen=True, slots=True)
class TrainingArtifactLayout:
    """One path vocabulary shared by every product and sampling frequency."""

    root: Path

    def __post_init__(self) -> None:
        object.__setattr__(self, "root", Path(self.root))

    @property
    def run_manifest_path(self) -> Path:
        return self.root / "run_manifest.json"

    @property
    def progress_path(self) -> Path:
        return self.root / "progress.json"

    @property
    def summary_path(self) -> Path:
        return self.root / "summary.json"

    def fold_dir(self, fold_id: int) -> Path:
        fold_id = int(fold_id)
        if fold_id < 0:
            raise ValueError(f"fold_id must be non-negative, got {fold_id}")
        return self.root / f"fold_{fold_id:02d}"

    def best_checkpoint_path(self, fold_id: int) -> Path:
        return self.fold_dir(fold_id) / "checkpoint_best.pt"

    def fold_complete_path(self, fold_id: int) -> Path:
        return self.fold_dir(fold_id) / "fold_complete.json"

    @staticmethod
    def year_group_name(train_years: Iterable[int]) -> str:
        years = [int(year) for year in train_years]
        if not years:
            raise ValueError("training-year groups require at least one year")
        if any(right <= left for left, right in zip(years, years[1:])):
            raise ValueError("training years must be strictly increasing")
        return "train_" + "-".join(str(year) for year in years)

    def group_dir(self, group_name: str) -> Path:
        name = _safe_component(group_name, label="training group name")
        if not name.startswith("train_"):
            raise ValueError(
                f"training group directories must start with 'train_': {name!r}"
            )
        return self.root / name

    def year_group_dir(self, train_years: Iterable[int]) -> Path:
        return self.group_dir(self.year_group_name(train_years))

    def group_checkpoint_path(self, group_name: str) -> Path:
        return self.group_dir(group_name) / "checkpoint_last.pt"

    def group_curve_path(self, group_name: str) -> Path:
        return self.group_dir(group_name) / "epoch_curve.jsonl"

    def group_pre_epoch_timing_path(self, group_name: str) -> Path:
        return self.group_dir(group_name) / "pre_epoch_timing.jsonl"

    def required_fold_paths(
        self,
        fold_id: int,
        *,
        require_plots: bool = True,
    ) -> tuple[Path, ...]:
        fold_dir = self.fold_dir(fold_id)
        paths = [
            fold_dir / "checkpoint_best.pt",
            fold_dir / "metrics.json",
            fold_dir / "model.pt",
            fold_dir / "test_backtest.npz",
            fold_dir / "deployment_test_backtest.npz",
            fold_dir / "mode_artifact_contract.json",
            fold_dir / "fold_complete.json",
        ]
        if require_plots:
            paths.extend(
                [
                    fold_dir / "equity_curve.png",
                    fold_dir / "equity_curve_log.png",
                    fold_dir / "annual_performance.png",
                    fold_dir / "annual_report.txt",
                    fold_dir / "plot_timing.json",
                ]
            )
        return tuple(paths)


@dataclass(frozen=True, slots=True)
class ArtifactConformance:
    """Result of checking the stable, cross-mode completed artifact surface."""

    checked: tuple[Path, ...]
    missing: tuple[Path, ...]
    invalid: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.missing and not self.invalid

    def require(self) -> None:
        if self.missing or self.invalid:
            details = [str(path) for path in self.missing]
            details.extend(self.invalid)
            raise RuntimeError(
                "training artifact contract is incomplete: " + ", ".join(details)
            )


def validate_completed_training_artifacts(
    root: str | Path,
    *,
    fold_ids: Iterable[int],
    group_names: Iterable[str],
    require_plots: bool = True,
    require_epoch_curve: bool = True,
) -> ArtifactConformance:
    """Check files whose names and locations must not vary by training mode."""

    layout = TrainingArtifactLayout(Path(root))
    fold_id_values = [int(fold_id) for fold_id in fold_ids]
    group_name_values = [str(group_name) for group_name in group_names]
    checked: list[Path] = [
        layout.run_manifest_path,
        layout.progress_path,
        layout.summary_path,
    ]
    for group_name in group_name_values:
        checked.append(layout.group_checkpoint_path(group_name))
        checked.append(layout.group_pre_epoch_timing_path(group_name))
        if require_epoch_curve:
            checked.append(layout.group_curve_path(group_name))
    for fold_id in fold_id_values:
        checked.extend(
            layout.required_fold_paths(fold_id, require_plots=require_plots)
        )
    missing = tuple(path for path in checked if not path.is_file())
    invalid: list[str] = []

    def read_json(path: Path) -> Any | None:
        if not path.is_file():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            invalid.append(f"{path}: invalid JSON ({type(exc).__name__})")
            return None

    manifest = read_json(layout.run_manifest_path)
    if isinstance(manifest, dict):
        if manifest.get("lifecycle_schema_version") != TRAINING_LIFECYCLE_SCHEMA_VERSION:
            invalid.append(f"{layout.run_manifest_path}: lifecycle schema mismatch")
        if manifest.get("artifact_layout_version") != TRAINING_ARTIFACT_LAYOUT_VERSION:
            invalid.append(f"{layout.run_manifest_path}: artifact layout mismatch")
    elif manifest is not None:
        invalid.append(f"{layout.run_manifest_path}: expected a JSON object")

    progress = read_json(layout.progress_path)
    if isinstance(progress, dict):
        if progress.get("schema_version") != TRAINING_PROGRESS_SCHEMA_VERSION:
            invalid.append(f"{layout.progress_path}: progress schema mismatch")
        if progress.get("state") != "complete" or progress.get("phase") != "complete":
            invalid.append(f"{layout.progress_path}: lifecycle is not complete")
    elif progress is not None:
        invalid.append(f"{layout.progress_path}: expected a JSON object")

    summary = read_json(layout.summary_path)
    if summary is not None and not isinstance(summary, list):
        invalid.append(f"{layout.summary_path}: expected a JSON array")

    for fold_id in fold_id_values:
        contract_path = layout.fold_dir(fold_id) / "mode_artifact_contract.json"
        contract = read_json(contract_path)
        if isinstance(contract, dict):
            if contract.get("artifact_layout_version") != TRAINING_ARTIFACT_LAYOUT_VERSION:
                invalid.append(f"{contract_path}: artifact layout mismatch")
        elif contract is not None:
            invalid.append(f"{contract_path}: expected a JSON object")
        complete_path = layout.fold_complete_path(fold_id)
        complete = read_json(complete_path)
        if isinstance(complete, dict):
            if complete.get("status") != "complete":
                invalid.append(f"{complete_path}: fold is not complete")
        elif complete is not None:
            invalid.append(f"{complete_path}: expected a JSON object")

    for group_name in group_name_values:
        curve_path = layout.group_curve_path(group_name)
        if require_epoch_curve and curve_path.is_file():
            try:
                first_line = next(
                    line.strip()
                    for line in curve_path.read_text(encoding="utf-8").splitlines()
                    if line.strip()
                )
                curve_row = json.loads(first_line)
                if curve_row.get("schema_version") != TRAINING_EPOCH_CURVE_SCHEMA_VERSION:
                    invalid.append(f"{curve_path}: epoch curve schema mismatch")
            except (OSError, json.JSONDecodeError, StopIteration, AttributeError) as exc:
                invalid.append(f"{curve_path}: invalid epoch curve ({type(exc).__name__})")

    return ArtifactConformance(
        checked=tuple(checked),
        missing=missing,
        invalid=tuple(invalid),
    )


def canonical_mode_artifact_contract(
    execution_mode: object,
    *,
    details: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the universal reporting semantics persisted under each fold."""

    spec = training_mode_spec(execution_mode)
    payload: dict[str, Any] = {
        "schema_version": TRAINING_LIFECYCLE_SCHEMA_VERSION,
        "artifact_layout_version": TRAINING_ARTIFACT_LAYOUT_VERSION,
        "execution_mode": spec.execution_mode,
        "product_family": spec.product_family,
        "frequency": spec.frequency,
        "decision_clock": spec.decision_clock,
        "execution_clock": spec.execution_clock,
        "recurrent_state_scope": spec.recurrent_state_scope,
        "terminal_policy": spec.terminal_policy,
        "split_ownership": spec.split_ownership,
        "sample_order_contract": spec.sample_order_contract,
        "benchmark_contract": spec.benchmark_contract,
        "weight_snapshot_contract": spec.weight_snapshot_contract,
        "turnover_contract": spec.turnover_contract,
    }
    if details:
        payload["mode_details"] = dict(details)
    return payload


def normalize_epoch_curve_record(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Guarantee one flat epoch-curve core while preserving mode metrics."""

    if "epoch" not in payload:
        raise ValueError("epoch curve records require an epoch")
    epoch = int(payload["epoch"])
    if epoch < 1:
        raise ValueError(f"epoch curve epoch must be positive, got {epoch}")
    row: dict[str, Any] = {
        "schema_version": TRAINING_EPOCH_CURVE_SCHEMA_VERSION,
        "epoch": epoch,
        "train_loss": None,
        "val_mean": None,
        "test_mean": None,
        "lr": None,
        "no_improve": None,
        "best_val_loss": None,
        "improved": None,
        "epoch_total_s": None,
    }
    row.update(dict(payload))
    row["schema_version"] = TRAINING_EPOCH_CURVE_SCHEMA_VERSION
    row["epoch"] = epoch
    return row


class TrainingRunLifecycle:
    """Atomic persistent run/progress coordinator shared by all runners."""

    def __init__(
        self,
        root: str | Path,
        *,
        execution_mode: object,
        run_mode: str,
        strategy: str,
        model_name: str,
        writer_enabled: bool = True,
    ) -> None:
        normalized_run_mode = str(run_mode).strip().lower()
        if normalized_run_mode not in {"train", "infer"}:
            raise ValueError(f"unsupported training run mode: {run_mode!r}")
        self.layout = TrainingArtifactLayout(Path(root))
        self.spec: TrainingModeSpec = training_mode_spec(execution_mode)
        self.run_mode = normalized_run_mode
        self.strategy = str(strategy)
        self.model_name = str(model_name)
        self.writer_enabled = bool(writer_enabled)
        self._started_at = _utc_now()
        self._last_progress_write_monotonic = float("-inf")
        self._progress: dict[str, Any] = self._initial_progress()
        self._manifest: dict[str, Any] | None = None

    def _initial_progress(self) -> dict[str, Any]:
        return {
            "schema_version": TRAINING_PROGRESS_SCHEMA_VERSION,
            "lifecycle_schema_version": TRAINING_LIFECYCLE_SCHEMA_VERSION,
            "state": "running",
            "phase": "setup",
            "run_mode": self.run_mode,
            "execution_mode": self.spec.execution_mode,
            "strategy": self.strategy,
            "model_name": self.model_name,
            "product_family": self.spec.product_family,
            "frequency": self.spec.frequency,
            "sample_order_contract": self.spec.sample_order_contract,
            "group": {"index": None, "total": None, "name": None},
            "fold": {
                "id": None,
                "selected_ids": [],
                "group_ids": [],
                "completed_ids": [],
                "total": None,
            },
            "epoch": {"current": None, "total": None},
            "work": {"completed": 0, "total": None, "unit": None},
            "last_sample": None,
            "metrics": {},
            "message": None,
            "failure": None,
            "started_at": self._started_at,
            "updated_at": self._started_at,
        }

    @property
    def progress(self) -> Mapping[str, Any]:
        return self._progress

    @property
    def manifest(self) -> Mapping[str, Any] | None:
        return self._manifest

    def _write_progress(self, *, force: bool = True) -> None:
        if not self.writer_enabled:
            return
        now = time.monotonic()
        if not force and now - self._last_progress_write_monotonic < 1.0:
            return
        self.layout.root.mkdir(parents=True, exist_ok=True)
        self._progress["updated_at"] = _utc_now()
        write_training_json(self.layout.progress_path, self._progress)
        self._last_progress_write_monotonic = now

    def start(
        self,
        *,
        fold_ids: Sequence[int],
        dataset_fingerprint: str | None = None,
        configuration_fingerprint: str | None = None,
        contract_versions: Mapping[str, Any] | None = None,
        data_summary: Mapping[str, Any] | None = None,
        configuration: Mapping[str, Any] | None = None,
        mode_details: Mapping[str, Any] | None = None,
        compatibility_manifest_paths: Iterable[str | Path] = (),
    ) -> dict[str, Any]:
        selected_fold_ids = [int(fold_id) for fold_id in fold_ids]
        manifest: dict[str, Any] = {
            "schema_version": TRAINING_LIFECYCLE_SCHEMA_VERSION,
            "lifecycle_schema_version": TRAINING_LIFECYCLE_SCHEMA_VERSION,
            "artifact_layout_version": TRAINING_ARTIFACT_LAYOUT_VERSION,
            "execution_mode": self.spec.execution_mode,
            "run_mode": self.run_mode,
            "strategy": self.strategy,
            "model_name": self.model_name,
            "product_family": self.spec.product_family,
            "frequency": self.spec.frequency,
            "decision_clock": self.spec.decision_clock,
            "execution_clock": self.spec.execution_clock,
            "recurrent_state_scope": self.spec.recurrent_state_scope,
            "terminal_policy": self.spec.terminal_policy,
            "split_ownership": self.spec.split_ownership,
            "sample_order_contract": self.spec.sample_order_contract,
            "benchmark_contract": self.spec.benchmark_contract,
            "weight_snapshot_contract": self.spec.weight_snapshot_contract,
            "turnover_contract": self.spec.turnover_contract,
            "selected_fold_ids": selected_fold_ids,
            "dataset_fingerprint": dataset_fingerprint,
            "configuration_fingerprint": configuration_fingerprint,
            "contract_versions": dict(contract_versions or {}),
            "data_summary": dict(data_summary or {}),
            "configuration": dict(configuration or {}),
            "mode_details": dict(mode_details or {}),
            "started_at": self._started_at,
        }
        self._manifest = manifest
        self._progress["fold"]["selected_ids"] = selected_fold_ids
        self._progress["fold"]["total"] = len(selected_fold_ids)
        if self.writer_enabled:
            self.layout.root.mkdir(parents=True, exist_ok=True)
            write_training_json(self.layout.run_manifest_path, manifest)
            for compatibility_path in compatibility_manifest_paths:
                write_training_json(Path(compatibility_path), manifest)
        self._write_progress()
        return manifest

    def start_group(
        self,
        *,
        group_name: str,
        group_index: int,
        group_total: int,
        fold_ids: Sequence[int],
        epoch_total: int | None,
        message: str | None = None,
    ) -> None:
        self.layout.group_dir(group_name)  # validate the component before writing it
        fold_ids = [int(fold_id) for fold_id in fold_ids]
        self._progress.update(
            {
                "state": "running",
                "phase": "setup",
                "group": {
                    "index": int(group_index),
                    "total": int(group_total),
                    "name": str(group_name),
                },
                "epoch": {"current": None, "total": epoch_total},
                "work": {"completed": 0, "total": None, "unit": None},
                "last_sample": None,
                "metrics": {},
                "message": message,
                "failure": None,
            }
        )
        self._progress["fold"]["id"] = fold_ids[0] if len(fold_ids) == 1 else None
        self._progress["fold"]["group_ids"] = fold_ids
        self._write_progress()

    def update_manifest(
        self,
        *,
        dataset_fingerprint: str | None = None,
        configuration_fingerprint: str | None = None,
        contract_versions: Mapping[str, Any] | None = None,
        data_summary: Mapping[str, Any] | None = None,
        mode_details: Mapping[str, Any] | None = None,
    ) -> None:
        """Enrich the run identity after a runner finishes expensive setup."""

        if self._manifest is None:
            raise RuntimeError("training lifecycle manifest has not been started")
        if dataset_fingerprint is not None:
            self._manifest["dataset_fingerprint"] = str(dataset_fingerprint)
        if configuration_fingerprint is not None:
            self._manifest["configuration_fingerprint"] = str(
                configuration_fingerprint
            )
        if contract_versions:
            self._manifest["contract_versions"].update(dict(contract_versions))
        if data_summary:
            self._manifest["data_summary"].update(dict(data_summary))
        if mode_details:
            self._manifest["mode_details"].update(dict(mode_details))
        if self.writer_enabled:
            write_training_json(self.layout.run_manifest_path, self._manifest)

    def set_phase(
        self,
        phase: str,
        *,
        fold_id: int | None = None,
        epoch: int | None = None,
        epoch_total: int | None = None,
        work_completed: int = 0,
        work_total: int | None = None,
        work_unit: str | None = None,
        last_sample: str | None = None,
        metrics: Mapping[str, Any] | None = None,
        message: str | None = None,
    ) -> None:
        normalized = str(phase).strip().lower()
        if normalized not in TRAINING_PHASES:
            raise ValueError(f"unsupported training phase: {phase!r}")
        self._progress["phase"] = normalized
        self._progress["state"] = (
            "complete" if normalized == "complete" else "failed" if normalized == "failed" else "running"
        )
        if fold_id is not None:
            self._progress["fold"]["id"] = int(fold_id)
        self._progress["epoch"] = {
            "current": None if epoch is None else int(epoch),
            "total": (
                self._progress["epoch"].get("total")
                if epoch_total is None
                else int(epoch_total)
            ),
        }
        self._progress["work"] = {
            "completed": int(work_completed),
            "total": None if work_total is None else int(work_total),
            "unit": None if work_unit is None else str(work_unit),
        }
        self._progress["last_sample"] = last_sample
        self._progress["metrics"] = {
            str(name): _optional_metric(value)
            for name, value in (metrics or {}).items()
        }
        self._progress["message"] = message
        self._write_progress()

    def update_work(
        self,
        *,
        completed: int,
        total: int,
        unit: str,
        last_sample: str | None = None,
        metrics: Mapping[str, Any] | None = None,
    ) -> None:
        self._progress["work"] = {
            "completed": int(completed),
            "total": int(total),
            "unit": str(unit),
        }
        self._progress["last_sample"] = last_sample
        if metrics is not None:
            self._progress["metrics"] = {
                str(name): _optional_metric(value)
                for name, value in metrics.items()
            }
        self._write_progress(force=(int(completed) >= int(total)))

    def finish_epoch(
        self,
        *,
        epoch: int,
        epoch_total: int,
        metrics: Mapping[str, Any],
    ) -> None:
        self.set_phase(
            "training",
            fold_id=self._progress["fold"].get("id"),
            epoch=epoch,
            epoch_total=epoch_total,
            work_completed=int(epoch),
            work_total=int(epoch_total),
            work_unit="epoch",
            last_sample=self._progress.get("last_sample"),
            metrics=metrics,
        )

    def finish_fold(self, fold_id: int) -> None:
        completed = self._progress["fold"]["completed_ids"]
        fold_id = int(fold_id)
        if fold_id not in completed:
            completed.append(fold_id)
            completed.sort()
        self.set_phase(
            "reporting",
            fold_id=fold_id,
            message=f"fold {fold_id} artifacts complete",
        )

    def complete(self, *, fold_ids: Iterable[int], message: str | None = None) -> None:
        completed = sorted({int(fold_id) for fold_id in fold_ids})
        self._progress["fold"]["completed_ids"] = completed
        self._progress["fold"]["id"] = completed[-1] if completed else None
        self.set_phase(
            "complete",
            work_completed=len(completed),
            work_total=self._progress["fold"].get("total"),
            work_unit="fold",
            message=message or "training lifecycle complete",
        )

    def fail(self, error: BaseException) -> None:
        self._progress["failure"] = {
            "type": type(error).__name__,
            "message": str(error),
        }
        self._progress["phase"] = "failed"
        self._progress["state"] = "failed"
        self._progress["message"] = "training lifecycle failed"
        self._write_progress()


__all__ = [
    "ArtifactConformance",
    "TRAINING_ARTIFACT_LAYOUT_VERSION",
    "TRAINING_EPOCH_CURVE_SCHEMA_VERSION",
    "TRAINING_LIFECYCLE_SCHEMA_VERSION",
    "TRAINING_PHASES",
    "TRAINING_PROGRESS_SCHEMA_VERSION",
    "TrainingArtifactLayout",
    "TrainingRunLifecycle",
    "canonical_mode_artifact_contract",
    "normalize_epoch_curve_record",
    "validate_completed_training_artifacts",
]
