#!/usr/bin/env python3
"""Reconstruct immutable signal-target transitions, never simulated fills.

The publication plan pins each summary and weight file. This tool independently
rechecks those bytes and calculates every retained target transition in order.
It does not assert publication completeness, executable prices, orders, fees,
positions, curves, or promotion readiness; those require separate evidence.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import math
import os
from pathlib import Path
import struct
from typing import Any

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
MAX_ARTIFACT_BYTES = 32 * 1024 * 1024


def _pinned_bytes(path: str, expected: str) -> bytes:
    target = Path(path)
    with target.open("rb") as stream:
        before = os.fstat(stream.fileno())
        if before.st_size > MAX_ARTIFACT_BYTES:
            raise ValueError(f"source artifact too large: {target}")
        data = stream.read(MAX_ARTIFACT_BYTES + 1)
        after = os.fstat(stream.fileno())
    signature = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
    if (len(data) > MAX_ARTIFACT_BYTES or signature(before) != signature(after)
            or signature(after) != signature(target.stat())
            or hashlib.sha256(data).hexdigest() != expected):
        raise ValueError(f"source artifact changed or hash mismatch: {target}")
    return data


def _targets(event: dict[str, Any], market: str) -> dict[str, float]:
    summary = json.loads(_pinned_bytes(event["summary_path"], event["summary_sha256"]))
    if (not isinstance(summary, dict) or summary.get("market") != market
            or summary.get("signal_id") != event["signal_id"]):
        raise ValueError("pinned summary identity mismatch")
    raw = _pinned_bytes(event["weights_path"], event["weights_sha256"])
    suffix = Path(event["weights_path"]).suffix
    if suffix == ".parquet":
        rows = pl.read_parquet(io.BytesIO(raw), columns=["symbol", "target_weight"]).to_dicts()
    elif suffix == ".json":
        payload = json.loads(raw)
        if (not isinstance(payload, dict) or payload.get("schema_version") != 1
                or payload.get("market") != market or payload.get("signal_id") != event["signal_id"]):
            raise ValueError("pinned execution payload identity mismatch")
        rows = payload.get("rows")
    else:
        raise ValueError("unsupported pinned weight format")
    if not isinstance(rows, list) or not rows or len(rows) > 20_000:
        raise ValueError("invalid pinned target rows")
    targets: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("invalid pinned target row")
        symbol, weight = row.get("symbol"), row.get("target_weight")
        if (not isinstance(symbol, str) or not symbol or len(symbol) > 32
                or symbol in targets or type(weight) not in (int, float)):
            raise ValueError("invalid or duplicate pinned target")
        try:
            weight = float(weight)
        except OverflowError as exc:
            raise ValueError("invalid pinned target weight") from exc
        if not math.isfinite(weight):
            raise ValueError("nonfinite pinned target weight")
        targets[symbol] = weight
    return targets


def _target_digest(targets: dict[str, float]) -> str:
    digest = hashlib.sha256()
    for symbol, value in sorted(targets.items()):
        name = symbol.encode("utf-8")
        digest.update(len(name).to_bytes(4, "big"))
        digest.update(name)
        digest.update(struct.pack(">d", value))
    return digest.hexdigest()


def replay_targets(plan: dict[str, Any]) -> dict[str, Any]:
    if (plan.get("contract") != "published_revision_causal_minute_replay_plan_v1"
            or plan.get("simulation_only") is not True
            or plan.get("production_order_possible") is not False
            or plan.get("ledger_modified") is not False):
        raise ValueError("expected a read-only published revision plan")
    modes = plan.get("modes")
    if not isinstance(modes, dict) or not modes:
        raise ValueError("revision plan has no modes")
    result: dict[str, Any] = {}
    total = blocked = 0
    for market, mode in sorted(modes.items()):
        events = mode.get("events")
        if not isinstance(events, list) or len(events) != mode.get("publication_count") or not events:
            raise ValueError(f"{market}: incomplete planned events")
        previous: dict[str, float] = {}
        replayed = []
        seen: set[str] = set()
        mode_blocked = 0
        for event in events:
            signal_id = event.get("signal_id")
            if not isinstance(signal_id, str) or not signal_id or signal_id in seen:
                raise ValueError(f"{market}: duplicate or missing signal")
            seen.add(signal_id)
            targets = _targets(event, market)
            symbols = previous.keys() | targets.keys()
            changes = [(previous.get(symbol, 0.0), targets.get(symbol, 0.0)) for symbol in symbols]
            issues = list(event.get("source_errors_blocking_replay") or [])
            mode_blocked += bool(issues)
            replayed.append({
                "signal_id": signal_id,
                "revision": event.get("revision"),
                "visibility_lower_bound": event.get("publication_visibility_lower_bound"),
                "visibility_upper_bound": event.get("publication_visibility_upper_bound"),
                "weights_sha256": event.get("weights_sha256"),
                "target_vector_sha256": _target_digest(targets),
                "target_symbols": len(targets),
                "nonzero_targets": sum(value != 0.0 for value in targets.values()),
                "changed_targets": sum(before != after for before, after in changes),
                "opened_targets": sum(before == 0.0 and after != 0.0 for before, after in changes),
                "removed_targets": sum(before != 0.0 and after == 0.0 for before, after in changes),
                "direction_flips": sum(before * after < 0.0 for before, after in changes),
                "gross_target_weight": math.fsum(abs(value) for value in targets.values()),
                "target_change_l1": math.fsum(abs(after - before) for before, after in changes),
                "was_registered_in_original_ledger": event.get("was_registered_in_original_ledger") is True,
                "source_errors_blocking_execution_replay": issues,
            })
            previous = targets
        total += len(replayed)
        blocked += mode_blocked
        result[market] = {
            "event_count": len(replayed),
            "registered_event_count": sum(e["was_registered_in_original_ledger"] for e in replayed),
            "source_clock_blocked_event_count": mode_blocked,
            "chronological_execution_replay_accepted": False,
            "events": replayed,
        }
    if total != plan.get("event_count"):
        raise ValueError("total publication count differs from plan")
    return {
        "schema_version": 1,
        "contract": "immutable_paper_signal_target_transition_replay_v1",
        "session_date": plan.get("session_date"),
        "status": "target_transitions_only_not_trades_or_curve",
        "simulation_only": True,
        "production_order_possible": False,
        "ledger_modified": False,
        "event_count": total,
        "source_clock_blocked_event_count": blocked,
        "publication_completeness_proven": False,
        "fills_fees_curves_and_restart_accepted": False,
        "modes": result,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = args.plan.read_bytes()
    result = replay_targets(json.loads(source))
    result["revision_plan_sha256"] = hashlib.sha256(source).hexdigest()
    output = args.output.resolve()
    if not output.is_relative_to((ROOT / "artifacts/data_repair").resolve()):
        raise ValueError("target replay output must stay in isolated data_repair")
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")
    print(json.dumps({key: result[key] for key in (
        "status", "event_count", "source_clock_blocked_event_count")}, ensure_ascii=False))


if __name__ == "__main__":
    main()
