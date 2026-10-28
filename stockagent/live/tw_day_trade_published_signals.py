"""Read the signal producer's published pointers without reading a paper ledger.

These are model targets, not orders, positions, or evidence of a fill.  Each
mode is independently fenced by its atomic pointer and identity-checked
execution payload so a changing ledger cannot delay the first signal view.
"""

from __future__ import annotations

from datetime import date, datetime
from functools import lru_cache
import json
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from stockagent.live.market_config import load_market_configs, resolved_live_output_dir
from stockagent.live.public_dashboards import public_paper_signal_key
from stockagent.live.signal_artifacts import read_execution_weight_rows


_TAIPEI = ZoneInfo("Asia/Taipei")
_MAX_POINTER_BYTES = 64 * 1024
_MAX_SUMMARY_BYTES = 16 * 1024 * 1024
_MAX_WEIGHTS_BYTES = 16 * 1024 * 1024
_MAX_ROWS = 20_000


def _signature(path: Path) -> tuple[int, int, int, int, int]:
    stat = path.stat()
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def _object(path: Path, maximum_bytes: int) -> dict[str, Any]:
    before = _signature(path)
    if before[2] > maximum_bytes:
        raise ValueError("published artifact too large")
    value = json.loads(path.read_bytes())
    if before != _signature(path) or not isinstance(value, dict):
        raise ValueError("published artifact changed or is not an object")
    return value


def _artifact_path(repo: Path, root: Path, value: Any, filename: str) -> Path:
    if not isinstance(value, str) or not value:
        raise ValueError("missing artifact path")
    path = Path(value)
    path = (path if path.is_absolute() else repo / path).resolve(strict=True)
    if path.name != filename or not path.is_relative_to(root):
        raise ValueError("artifact is outside its configured signal root")
    return path


def _time(value: Any, session: str) -> str:
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.astimezone(_TAIPEI).date().isoformat() != session:
        raise ValueError("invalid signal time")
    return str(value)


@lru_cache(maxsize=18)
def _read_market(
    repo_string: str, root_string: str, market: str, session: str,
    pointer_signature: tuple[int, int, int, int, int],
) -> tuple[dict[str, Any], tuple[dict[str, Any], ...]]:
    repo, root = Path(repo_string), Path(root_string)
    pointer_path = root / "latest_signal.json"
    if _signature(pointer_path) != pointer_signature:
        raise ValueError("signal pointer changed")
    pointer = _object(pointer_path, _MAX_POINTER_BYTES)
    signal_id = pointer.get("signal_id")
    if (pointer.get("schema_version") != 2 or pointer.get("market") != market
            or not isinstance(signal_id, str) or not signal_id
            or Path(signal_id).name != signal_id or "\\" in signal_id):
        raise ValueError("signal pointer identity mismatch")
    ready_at = _time(pointer.get("signal_ready_at"), session)
    artifact_at = _time(pointer.get("artifact_published_at"), session)
    summary_path = _artifact_path(repo, root, pointer.get("summary_path"), "summary.json")
    weights_path = _artifact_path(repo, root, pointer.get("execution_weights_path"), "execution_weights.json")
    if summary_path.parent != weights_path.parent or summary_path.parent.name != signal_id:
        raise ValueError("signal artifact location mismatch")
    summary = _object(summary_path, _MAX_SUMMARY_BYTES)
    if (summary.get("market") != market or summary.get("signal_id") != signal_id
            or str(summary.get("asof_date") or "")[:10] != session
            or summary.get("execution_mode") != "tw_day_trade"
            or summary.get("day_trade_model_observation") != "session_open"
            or summary.get("signal_ready_at") != ready_at):
        raise ValueError("signal summary identity mismatch")
    revision = summary.get("opening_signal_revision") or {}
    if not isinstance(revision, dict):
        raise ValueError("invalid signal revision")
    revision_index = revision.get("index")
    if revision_index is not None and (type(revision_index) is not int or revision_index < 0):
        raise ValueError("invalid signal revision index")
    weights_signature = _signature(weights_path)
    if weights_signature[2] > _MAX_WEIGHTS_BYTES:
        raise ValueError("execution payload too large")
    source_rows = read_execution_weight_rows(weights_path, market=market, signal_id=signal_id)
    if len(source_rows) > _MAX_ROWS or _signature(weights_path) != weights_signature:
        raise ValueError("execution payload changed or too large")
    public_key = public_paper_signal_key({"market": market, "session_date": session, "signal_id": signal_id})
    rows: list[dict[str, Any]] = []
    for source in source_rows:
        # The shared artifact reader already validates symbol identity and
        # finite numeric weights for both the dashboard and the executor.
        symbol, weight = source["symbol"], source["target_weight"]
        if weight == 0:
            continue
        name = source.get("name")
        rows.append({"market": market, "symbol": symbol,
                     "name": name if isinstance(name, str) and len(name) <= 128 else "",
                     "target_weight": weight, "side": "long" if weight > 0 else "short",
                     "public_signal_key": public_key})
    if _signature(pointer_path) != pointer_signature:
        raise ValueError("signal pointer changed during read")
    return ({"status": "published", "signal_ready_at": ready_at,
             "artifact_published_at": artifact_at, "revision_index": revision_index,
             "target_count": len(rows), "public_signal_key": public_key}, tuple(rows))


def build_published_signal_snapshot(*, repo_root: Path, observed_at: datetime | None = None) -> dict[str, Any]:
    """Return per-mode current-day published targets with no fill assertions."""
    repo = Path(repo_root).resolve(strict=True)
    now = observed_at or datetime.now(_TAIPEI)
    if now.tzinfo is None:
        raise ValueError("observation time must be timezone-aware")
    session = now.astimezone(_TAIPEI).date().isoformat()
    date.fromisoformat(session)
    configs = load_market_configs(repo / "services/discord_bot/markets")
    modes, rows = [], []
    for market, config in sorted(configs.items()):
        if not config.enabled or not config.day_trade_simulation_enabled:
            continue
        mode: dict[str, Any] = {"market": market, "label": config.label}
        configured_root = resolved_live_output_dir(config)
        root = (configured_root if configured_root.is_absolute() else repo / configured_root).resolve()
        try:
            pointer_path = root / "latest_signal.json"
            signature = _signature(pointer_path)
            detail, target_rows = _read_market(str(repo), str(root), market, session, signature)
            # Cached values are usable only while this exact pointer is still current.
            if _signature(pointer_path) != signature:
                raise ValueError("signal pointer changed after read")
            ready = datetime.fromisoformat(detail["signal_ready_at"].replace("Z", "+00:00"))
            artifact = datetime.fromisoformat(detail["artifact_published_at"].replace("Z", "+00:00"))
            checked_at = now if observed_at is not None else datetime.now(_TAIPEI)
            if artifact < ready or artifact > checked_at:
                raise ValueError("signal artifact time is not yet observable")
            mode.update(detail)
            rows.extend(target_rows)
        except FileNotFoundError:
            mode.update(status="waiting")
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            mode.update(status="unavailable")
        modes.append(mode)
    rows.sort(key=lambda row: (-abs(row["target_weight"]), row["market"], row["symbol"]))
    return {"schema_version": 1, "simulation_only": True, "production_order_possible": False,
            "session_date": session, "modes": modes, "rows": rows}
