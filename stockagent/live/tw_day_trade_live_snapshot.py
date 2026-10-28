"""Small, durably committed paper projections for the live dashboard.

This is not a second ledger.  It reads only the engine's atomic projections
at one acknowledged commit; journal history remains with the canonical
complete-history reader.  An already opened projection is safe to read even
if a later commit appears, provided the opened file itself did not change.
"""

from __future__ import annotations

from datetime import date
import json
import os
from pathlib import Path
import stat
import time
from typing import Any

from stockagent.live.public_dashboards import sanitize_tw_positions, sanitize_tw_status
from stockagent.live.tw_day_trade_service_sync import PaperReadChanged, load_service_sync


_FILES = {"status.json": 2 * 1024 * 1024, "positions.json": 32 * 1024 * 1024}
_MODE_FIELDS = (
    "market", "label", "session_date", "engine_status", "readiness_error", "signal_at",
    "signal_counts", "entry_requested_shares", "entry_filled_shares", "entry_unfilled_shares",
    "pending_entry_shares", "open_position_count", "stale_position_count", "initial_capital_twd",
    "total_equity_twd", "cumulative_realized_net_pnl_twd", "open_net_liquidation_pnl_twd",
    "execution_evidence_complete", "unresolved_exit_count", "force_exit_failures",
)


def _signature(info: os.stat_result) -> list[int]:
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns]


def _read_once(root: Path) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    receipt = load_service_sync(root)
    if not isinstance(receipt, dict) or not isinstance(receipt.get("durability_ack"), dict):
        raise PaperReadChanged("paper commit is not durably acknowledged")
    identity = (receipt.get("engine_run_id"), receipt.get("state_revision"), receipt.get("content_revision"))
    if (not isinstance(identity[0], str) or not identity[0]
            or type(identity[1]) is not int or identity[1] <= 0
            or type(identity[2]) is not int or identity[2] < 0
            or receipt.get("simulation_only") is not True or receipt.get("production_order_possible") is not False):
        raise PaperReadChanged("invalid paper commit identity")
    generations = receipt.get("projection_generations")
    if not isinstance(generations, dict):
        raise PaperReadChanged("paper projection generations unavailable")
    opened: list[tuple[str, Any, list[int]]] = []
    try:
        # Capture both open handles at the SAME receipt before reading either.
        for name, maximum in _FILES.items():
            fd = os.open(root / name, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            stream = os.fdopen(fd, "rb")
            info = os.fstat(stream.fileno())
            signature = _signature(info)
            opened.append((name, stream, signature))
            if (not stat.S_ISREG(info.st_mode) or info.st_size > maximum
                    or signature != generations.get(name)):
                raise PaperReadChanged("projection is not at the acknowledged commit")
        result: dict[str, dict[str, Any]] = {}
        for name, stream, signature in opened:
            raw = stream.read(_FILES[name] + 1)
            if len(raw) != signature[2] or _signature(os.fstat(stream.fileno())) != signature:
                raise PaperReadChanged("projection changed during capture")
            value = json.loads(raw)
            if (not isinstance(value, dict)
                    or tuple(value.get(k) for k in ("engine_run_id", "state_revision", "content_revision")) != identity
                    or value.get("simulation_only") is not True
                    or value.get("production_order_possible") is not False):
                raise PaperReadChanged("projection identity differs from committed paper state")
            result[name] = value
        return receipt, result
    finally:
        for _, stream, _ in opened:
            stream.close()


def build_live_paper_snapshot(*, state_dir: Path, quote_source=None, private_prices: bool = False) -> dict[str, Any]:
    """Return all currently open positions and mode summaries, never history.

    Retries are short and bounded, since the writer can be between atomic
    replacements.  The exact receipt and each opened file are checked; a
    newer commit after capture does not invalidate this explicitly as-of view.
    """
    for attempt in range(16):
        try:
            receipt, files = _read_once(Path(state_dir))
            break
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            if attempt == 15:
                raise PaperReadChanged("committed live paper snapshot unavailable") from exc
            time.sleep(0.02)
    status, positions = files["status.json"], files["positions.json"]
    status_modes, position_modes = status.get("modes"), positions.get("modes")
    if (not isinstance(status_modes, dict) or not isinstance(position_modes, dict)
            or set(status_modes) != set(position_modes)):
        raise PaperReadChanged("committed mode projections disagree")
    modes, rows = [], []
    for market, mode in status_modes.items():
        position_mode = position_modes[market]
        if (not isinstance(mode, dict) or not isinstance(position_mode, dict)
                or mode.get("market") != market or position_mode.get("market") != market
                or not isinstance(position_mode.get("positions"), list)):
            raise PaperReadChanged("invalid committed mode projection")
        mode_rows = position_mode["positions"]
        count = mode.get("open_position_count")
        if (type(count) is not int or count < 0 or count != len(mode_rows)
                or position_mode.get("open_position_count") != count
                or any(not isinstance(row, dict) or row.get("market") != market for row in mode_rows)):
            raise PaperReadChanged("committed open-position count mismatch")
        modes.append({key: mode[key] for key in _MODE_FIELDS if key in mode})
        rows.extend(mode_rows)
    envelope = {"schema_version": 1, "simulation_only": True, "production_order_possible": False}
    safe_status = sanitize_tw_status({**envelope, "modes": modes}, private_prices=private_prices)
    # A live view normally touches a single session. Populate the existing
    # complete-session LRU once so subsequent revisions don't re-scan daily
    # Parquet for the same hundreds of symbols. read_view below still pins
    # and checks the official source generation. Wide historical scopes keep
    # the ordinary projected reader instead of filling the LRU indiscriminately.
    days = {str(row.get("session_date") or "") for row in rows}
    if quote_source is not None and 0 < len(days) <= 2:
        for day in days:
            try:
                if date.fromisoformat(day).isoformat() == day:
                    quote_source.session(day)
            except (ValueError, TypeError):
                pass
    safe_positions = sanitize_tw_positions({**envelope, "rows": rows}, quote_source=quote_source,
                                            private_prices=private_prices)
    return {**envelope, "as_of": positions.get("generated_at"),
            "status_updated_at": status.get("updated_at"),
            "state_revision": receipt["state_revision"], "content_revision": receipt["content_revision"],
            "health": safe_status.get("health", status.get("health")),
            "modes": safe_status["modes"], "rows": safe_positions["rows"],
            "scope": "committed_open_positions_only"}
