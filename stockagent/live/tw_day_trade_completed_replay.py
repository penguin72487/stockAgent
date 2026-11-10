"""Verify source-pinned same-day opening inputs for isolated paper replay.

This is not an opening quote, broker trade, or permission to publish a live
signal. A completed raw daily report is checked against every input CSV row.
"""
from __future__ import annotations

import csv
from datetime import date, datetime
import hashlib
import io
import json
import math
import os
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from stockagent.live.shioaji_schedule import HISTORICAL_QUERY_RESUME

_TAIPEI = ZoneInfo("Asia/Taipei")
CONTRACT = "same_day_official_open_isolated_replay_source_v1"


def _pinned_bytes(path: Path) -> tuple[bytes, str]:
    with path.open("rb") as stream:
        before = stream.fileno()
        first = os.fstat(before)
        raw = stream.read()
        last = os.fstat(before)
    current = path.stat()
    identity = lambda st: (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns)
    if identity(first) != identity(last) or identity(last) != identity(current):
        raise ValueError("counterfactual source changed during read")
    return raw, hashlib.sha256(raw).hexdigest()


def _number(value: Any) -> float | None:
    try:
        result = float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def verify_completed_open_sources(*, session: date, observed_at: datetime,
                                  prices_csv: Path, raw_root: Path,
                                  expected: Mapping[str, Any] | None = None,
                                  source_paths: Mapping[str, str | Path] | None = None) -> dict[str, Any]:
    """Re-read both official reports and compare all positive-volume CSV opens.

    Equality of prices does not establish intraday tradability or a fill.
    Unknown/zero-volume rows remain absent, so downstream model-only last
    trade references cannot turn into execution evidence.
    """
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("counterfactual source clock must be timezone-aware")
    local = observed_at.astimezone(_TAIPEI)
    if session != local.date() or local.time() < HISTORICAL_QUERY_RESUME:
        raise ValueError("same-day counterfactual source requires completed session maintenance window")
    root = raw_root.resolve(strict=True)
    if expected is not None and source_paths is not None:
        raise ValueError("use either explicit source paths or an existing proof")
    if expected is not None:
        entries = expected.get("official_sources")
        if not isinstance(entries, Mapping) or set(entries) != {"twse", "tpex"}:
            raise ValueError("counterfactual source proof has invalid exchange membership")
        source_paths = {exchange: str((entries.get(exchange) or {}).get("path") or "")
                        for exchange in ("twse", "tpex")}
    if source_paths is not None and set(source_paths) != {"twse", "tpex"}:
        raise ValueError("counterfactual source paths have invalid exchange membership")
    csv_path = prices_csv.resolve(strict=True)
    inputs, csv_digest = _pinned_bytes(csv_path)
    opens: dict[str, float] = {}
    official_symbols: set[str] = set()
    sources: dict[str, dict[str, str]] = {}
    for exchange in ("twse", "tpex"):
        path = (Path(source_paths[exchange]) if source_paths is not None
                else root / f"{exchange}_daily_ohlcv" / f"{session}.json").resolve(strict=True)
        if (not path.is_relative_to(root) or path.parent.name != f"{exchange}_daily_ohlcv"
                or path.name != f"{session}.json"):
            raise ValueError("official daily source escaped its raw root")
        raw, digest = _pinned_bytes(path)
        payload = json.loads(raw)
        if (not isinstance(payload, dict) or str(payload.get("date")) != session.strftime("%Y%m%d")
                or str(payload.get("stat")).lower() != "ok" or not isinstance(payload.get("tables"), list)):
            raise ValueError("invalid official daily source date or status")
        sources[exchange] = {"path": str(path), "sha256": digest}
        for table in payload["tables"]:
            if not isinstance(table, dict):
                raise ValueError("invalid official daily table")
            if "fields" not in table:
                continue
            fields = table["fields"]
            if not isinstance(fields, list) or not all(isinstance(x, str) for x in fields):
                raise ValueError("invalid official daily columns")
            fields = [x.strip() for x in fields]
            symbol_key = next((x for x in ("證券代號", "代號") if x in fields), None)
            open_key = next((x for x in ("開盤價", "開盤") if x in fields), None)
            volume_key = next((x for x in ("成交股數", "成交量") if x in fields), None)
            if not symbol_key or not open_key or not volume_key:
                continue
            rows = table.get("data")
            if not isinstance(rows, list):
                raise ValueError("invalid official daily rows")
            for values in rows:
                if not isinstance(values, list) or len(values) != len(fields):
                    raise ValueError("invalid official daily row width")
                row = dict(zip(fields, values, strict=True))
                symbol = str(row[symbol_key]).strip()
                opening, volume = _number(row[open_key]), _number(row[volume_key])
                if not symbol:
                    continue
                if symbol in official_symbols:
                    raise ValueError("duplicate official daily symbol")
                official_symbols.add(symbol)
                if opening is None or opening <= 0 or volume is None or volume <= 0 or not volume.is_integer():
                    continue
                opens[symbol] = opening
    reader = csv.DictReader(io.StringIO(inputs.decode("utf-8-sig")))
    if not {"symbol", "price", "open_price"}.issubset(reader.fieldnames or []):
        raise ValueError("counterfactual opening CSV columns missing")
    seen: set[str] = set()
    for row in reader:
        symbol = str(row.get("symbol") or "").strip()
        opening, price = _number(row.get("open_price")), _number(row.get("price"))
        if (not symbol or symbol in seen or symbol not in opens or opening is None or price is None
                or not math.isclose(opening, opens[symbol], rel_tol=0, abs_tol=1e-8)
                or not math.isclose(price, opening, rel_tol=0, abs_tol=1e-8)):
            raise ValueError("counterfactual opening CSV differs from official traded opens")
        seen.add(symbol)
    if not seen or seen != set(opens):
        raise ValueError("counterfactual opening CSV omits official traded opens")
    proof = {"contract": CONTRACT, "session_date": str(session), "prices_csv": str(csv_path),
             "prices_sha256": csv_digest, "official_sources": sources, "traded_open_count": len(seen),
             "simulation_only": True, "broker_fill": False, "live_publication": False}
    if expected is not None and dict(expected) != proof:
        raise ValueError("counterfactual source proof changed after validation")
    return proof
