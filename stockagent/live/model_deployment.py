from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Callable


SCHEMA_VERSION = 1


@dataclass(frozen=True, slots=True)
class ModelDeployment:
    config_path: str
    output_dir: str
    fold_id: int
    checkpoint_path: str
    weights_path: str
    candidate_signature: str


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _file_signature(path: Path) -> str:
    stat = path.stat()
    digest = hashlib.sha256()
    digest.update(f"{stat.st_size}:{stat.st_mtime_ns}".encode("ascii"))
    with path.open("rb") as handle:
        digest.update(handle.read(1024 * 1024))
        if stat.st_size > 1024 * 1024:
            handle.seek(max(0, stat.st_size - 1024 * 1024))
            digest.update(handle.read(1024 * 1024))
    return digest.hexdigest()[:16]


def _candidate_signature(paths: tuple[Path, ...]) -> str:
    digest = hashlib.sha256()
    for path in paths:
        digest.update(str(path.resolve()).encode("utf-8"))
        digest.update(_file_signature(path).encode("ascii"))
    return digest.hexdigest()[:16]


def discover_model_candidate(
    candidate_roots: tuple[str, ...] | list[str],
    *,
    candidate_configs: tuple[str, ...] | list[str],
    root: Path,
) -> ModelDeployment | None:
    """Select the latest complete fold from the highest-priority configured root."""

    roots = tuple(candidate_roots)
    configs = tuple(candidate_configs)
    if len(roots) != len(configs):
        raise ValueError("model candidate roots and config paths must have equal length")
    for index in reversed(range(len(roots))):
        raw_root = roots[index]
        config_path = Path(configs[index])
        if not config_path.is_absolute():
            config_path = root / config_path
        if not config_path.is_file():
            continue
        output_dir = Path(raw_root)
        if not output_dir.is_absolute():
            output_dir = root / output_dir
        candidates: list[tuple[int, Path, Path]] = []
        for fold_dir in output_dir.glob("fold_*"):
            try:
                fold_id = int(fold_dir.name.removeprefix("fold_"))
            except ValueError:
                continue
            complete_path = fold_dir / "fold_complete.json"
            checkpoint_path = fold_dir / "checkpoint_best.pt"
            weights_path = fold_dir / "daily_weights.parquet"
            metrics_path = fold_dir / "metrics.json"
            if not all(path.is_file() for path in (complete_path, checkpoint_path, weights_path, metrics_path)):
                continue
            try:
                complete = json.loads(complete_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if complete.get("status") != "complete" or int(complete.get("fold_id", -1)) != fold_id:
                continue
            candidates.append((fold_id, checkpoint_path, weights_path))
        if not candidates:
            continue
        fold_id, checkpoint_path, weights_path = max(candidates, key=lambda item: item[0])
        complete_path = checkpoint_path.parent / "fold_complete.json"
        return ModelDeployment(
            config_path=str(config_path),
            output_dir=str(output_dir),
            fold_id=fold_id,
            checkpoint_path=str(checkpoint_path),
            weights_path=str(weights_path),
            candidate_signature=_candidate_signature(
                (config_path, checkpoint_path, weights_path, complete_path)
            ),
        )
    return None


def load_deployment(path: str | Path | None, *, root: Path) -> ModelDeployment | None:
    if not path:
        return None
    manifest_path = Path(path)
    if not manifest_path.is_absolute():
        manifest_path = root / manifest_path
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        active = payload["active"]
        deployment = ModelDeployment(**active)
    except Exception:
        return None
    required = (
        Path(deployment.config_path),
        Path(deployment.checkpoint_path),
        Path(deployment.weights_path),
    )
    if not all(item.is_file() for item in required):
        return None
    return deployment


def _write_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def attempt_model_deployment(
    *,
    market: str,
    candidate_roots: tuple[str, ...] | list[str],
    candidate_configs: tuple[str, ...] | list[str],
    manifest_path: str | Path,
    root: Path,
    smoke_test: Callable[[ModelDeployment], dict[str, Any]],
    retry_failed_after_seconds: int = 300,
) -> tuple[str, ModelDeployment | None]:
    candidate = discover_model_candidate(
        candidate_roots,
        candidate_configs=candidate_configs,
        root=root,
    )
    if candidate is None:
        return "no_candidate", None

    path = Path(manifest_path)
    if not path.is_absolute():
        path = root / path
    existing: dict[str, Any] = {}
    try:
        existing = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    active = existing.get("active") if isinstance(existing, dict) else None
    if isinstance(active, dict) and active.get("candidate_signature") == candidate.candidate_signature:
        return "already_active", candidate
    last_attempt = existing.get("last_attempt") if isinstance(existing, dict) else None
    failed_attempt_is_recent = False
    if isinstance(last_attempt, dict) and last_attempt.get("attempted_at_utc"):
        try:
            attempted_at = datetime.fromisoformat(str(last_attempt["attempted_at_utc"]))
            failed_attempt_is_recent = (
                datetime.now(timezone.utc) - attempted_at.astimezone(timezone.utc)
            ).total_seconds() < max(0, int(retry_failed_after_seconds))
        except (TypeError, ValueError):
            pass
    if (
        isinstance(last_attempt, dict)
        and last_attempt.get("candidate_signature") == candidate.candidate_signature
        and last_attempt.get("status") == "failed"
        and failed_attempt_is_recent
    ):
        return "known_failed", candidate

    attempted_at = _utc_now()
    try:
        smoke_summary = smoke_test(candidate)
    except Exception as exc:
        payload = dict(existing) if isinstance(existing, dict) else {}
        payload.update({"schema_version": SCHEMA_VERSION, "market": market})
        payload["last_attempt"] = {
            **asdict(candidate),
            "status": "failed",
            "attempted_at_utc": attempted_at,
            "error": f"{type(exc).__name__}: {exc}",
        }
        _write_manifest(path, payload)
        raise

    payload = {
        "schema_version": SCHEMA_VERSION,
        "market": market,
        "active": asdict(candidate),
        "promoted_at_utc": _utc_now(),
        "last_attempt": {
            **asdict(candidate),
            "status": "passed",
            "attempted_at_utc": attempted_at,
            "panel_date": smoke_summary.get("panel_date"),
            "checkpoint_fingerprint": smoke_summary.get("checkpoint_fingerprint"),
            "config_fingerprint": smoke_summary.get("config_fingerprint"),
        },
    }
    _write_manifest(path, payload)
    return "promoted", candidate
