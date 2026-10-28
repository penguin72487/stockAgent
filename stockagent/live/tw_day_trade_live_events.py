"""Bounded recent paper events at one acknowledged journal generation."""
from __future__ import annotations

from datetime import datetime
import json
import os
from pathlib import Path
import re
import stat
import time
from typing import Any

from stockagent.live.public_dashboards import sanitize_tw_events
from stockagent.live.tw_day_trade_service_sync import PaperReadChanged, load_service_sync

_FILES = {"orders.jsonl": "order", "fills.jsonl": "fill", "events.jsonl": "notice"}
_MAX_TAIL_BYTES = 1024 * 1024
_MAX_ROWS_PER_FILE = 50
_CODE = re.compile(r"^[a-zA-Z0-9_]{1,96}$")
_FIELDS = ("recorded_at", "fill_at", "exchange_match_at", "session_date", "market", "symbol", "side",
           "purpose", "order_type", "price", "quantity", "status", "fill_contract", "pricing_rule",
           "depth_assumption", "odd_lot_execution_policy", "simulation_only", "simulation_replay")


def _signature(info: os.stat_result) -> list[int]:
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns]


def _capture(root: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    receipt = load_service_sync(root)
    if (not isinstance(receipt, dict) or not isinstance(receipt.get("durability_ack"), dict)
            or receipt.get("simulation_only") is not True or receipt.get("production_order_possible") is not False
            or not isinstance(receipt.get("engine_run_id"), str) or not receipt["engine_run_id"]
            or type(receipt.get("state_revision")) is not int or receipt["state_revision"] <= 0
            or type(receipt.get("content_revision")) is not int or receipt["content_revision"] < 0):
        raise PaperReadChanged("journal commit is not durably acknowledged")
    generations = receipt.get("journal_generations")
    markets = receipt.get("enabled_markets")
    if (not isinstance(generations, dict) or not isinstance(markets, list)
            or any(not isinstance(market, str) or not market for market in markets)):
        raise PaperReadChanged("committed journal scope unavailable")
    # The bounded raw-tail reader is not an immutable-journal reader. Do not
    # silently serve its legacy projection after journal enrollment changes.
    if any(str(k).startswith("journal_heads/") for k in generations):
        raise PaperReadChanged("immutable journal layout needs its canonical reader")
    opened = []
    try:
        for name, kind in _FILES.items():
            if name not in generations:
                raise PaperReadChanged("journal generation missing from receipt")
            expected = generations.get(name)
            if expected is None:
                continue
            stream = os.fdopen(os.open(root / name, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)), "rb")
            info = os.fstat(stream.fileno())
            sig = _signature(info)
            opened.append((name, kind, stream, sig))
            if not stat.S_ISREG(info.st_mode) or sig != expected:
                raise PaperReadChanged("journal is ahead of acknowledged commit")
        rows = []
        for _, kind, stream, sig in opened:
            start = max(0, sig[2] - _MAX_TAIL_BYTES)
            stream.seek(start)
            data = stream.read(_MAX_TAIL_BYTES + 1)
            if (len(data) != sig[2] - start or (data and not data.endswith(b"\n"))
                    or _signature(os.fstat(stream.fileno())) != sig):
                raise PaperReadChanged("journal changed during recent-event capture")
            lines = data.splitlines()
            if start:
                lines = lines[1:]  # discard a possibly partial first line
            for line in lines[-_MAX_ROWS_PER_FILE:]:
                value = json.loads(line)
                if not isinstance(value, dict) or value.get("simulation_only") is False:
                    raise PaperReadChanged("invalid paper event")
                if value.get("market") not in markets:
                    continue
                row = {key: value[key] for key in _FIELDS if key in value}
                row["event_kind"] = kind
                if kind == "fill":
                    row.update(order_type="FILL", status="filled")
                elif kind == "notice":
                    code = value.get("event")
                    row["purpose"] = code if isinstance(code, str) and _CODE.fullmatch(code) else "notice"
                    row["status"] = "notice"
                row["replay"] = value.get("simulation_replay") is True or value.get("counterfactual_open_replay") is True
                rows.append(row)
        def event_time(row):
            try:
                value = datetime.fromisoformat(str(row.get("recorded_at") or row.get("fill_at") or "").replace("Z", "+00:00"))
                return value.timestamp() if value.tzinfo is not None else float("-inf")
            except (TypeError, ValueError, OverflowError):
                return float("-inf")
        rows.sort(key=event_time, reverse=True)
        return receipt, rows
    finally:
        for _, _, stream, _ in opened:
            stream.close()


def build_live_paper_events(*, state_dir: Path, quote_source=None, private_prices: bool = False) -> dict[str, Any]:
    for attempt in range(16):
        try:
            receipt, rows = _capture(Path(state_dir))
            break
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            if attempt == 15:
                raise PaperReadChanged("committed recent events unavailable") from exc
            time.sleep(0.02)
    envelope = {"schema_version": 1, "simulation_only": True, "production_order_possible": False}
    safe = sanitize_tw_events({**envelope, "rows": rows}, quote_source=quote_source, private_prices=private_prices)
    # Replay is a derived non-price label, intentionally restored after the
    # canonical event allowlist so a replay is never described as a live fill.
    for safe_row, source_row in zip(safe["rows"], rows, strict=True):
        safe_row["replay"] = source_row["replay"]
    return {**envelope, "scope": "recent_committed_events_only", "as_of": receipt.get("published_at"),
            "state_revision": receipt["state_revision"], "limit_per_journal": _MAX_ROWS_PER_FILE,
            "rows": safe["rows"]}
