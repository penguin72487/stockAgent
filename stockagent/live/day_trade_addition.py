"""Add one isolated replay to the canonical paper ledger without replacing modes.

The prior account states and execution/decision byte streams are retained;
minute valuation is separately rebuilt from those unchanged executions. The
promoter uses the ordinary same-filesystem exchange and writer locks.
"""
from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import shutil

from downloader.artifact_io import atomic_write_json

CONTRACT = "additive_independent_paper_mode_v1"
LEDGERS = ("signals.jsonl", "fills.jsonl", "orders.jsonl", "events.jsonl", "latency.jsonl")


def sha(path: Path, count: int | None = None, *, offset: int = 0) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        stream.seek(offset)
        remaining = count
        while remaining is None or remaining > 0:
            block = stream.read(1024**2 if remaining is None else min(1024**2, remaining))
            if not block:
                if remaining:
                    raise ValueError(f"truncated retained ledger: {path}")
                break
            digest.update(block)
            if remaining is not None:
                remaining -= len(block)
    return digest.hexdigest()


def _object(path):
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"expected object: {path}")
    return value


def evidence_path(candidate: Path, relative: str) -> Path:
    path = (candidate / relative).resolve(strict=True)
    if Path(relative).is_absolute() or not path.is_relative_to(candidate.resolve()) or path == candidate.resolve():
        raise ValueError("addition evidence escaped candidate")
    return path


def _flat(state):
    for market, mode in state["modes"].items():
        if int(mode.get("open_position_count") or 0) or any(
                int(p.get("signed_shares") or 0) for p in (mode.get("positions") or {}).values()):
            raise ValueError(f"cannot add mode while existing inventory is open: {market}")


def merge_addition(live: Path, new_replay: Path, candidate: Path, *, added_market: str) -> dict:
    """Stage an isolated union. Never write to either input directory."""
    old_state = _object(live / "state.json")
    new_state = _object(new_replay / "state.json")
    _flat(old_state)
    _flat(new_state)
    if added_market in old_state["modes"] or set(new_state["modes"]) != {added_market}:
        raise ValueError("addition must contain exactly one genuinely new mode")
    source_proofs = {name: {"bytes": (live / name).stat().st_size, "sha256": sha(live / name)}
                     for name in LEDGERS if (live / name).is_file()}
    if candidate.exists() and any(candidate.iterdir()):
        raise ValueError("additive candidate must be empty")
    shutil.copytree(live, candidate, dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns("*.lock", ".lock", "*.tmp.*", "quote_broker"))
    evidence = candidate / "addition_evidence"
    evidence.mkdir()
    atomic_write_json(evidence / "prior_state.json", old_state)
    atomic_write_json(evidence / "prior_rebuild_receipt.json", _object(live / "rebuild_receipt.json"))
    # Keep the independently accepted new replay as a component with its own
    # source/price contracts; never relabel old executions as the new policy.
    component = evidence / "new_mode"
    shutil.copytree(new_replay, component, ignore=shutil.ignore_patterns("*.lock", ".lock"))
    appended = {}
    for name in (*LEDGERS, "marks.jsonl"):
        source = new_replay / name
        target = candidate / name
        if not source.is_file():
            continue
        if name in LEDGERS:
            appended[name] = {"offset": target.stat().st_size if target.is_file() else 0,
                              "bytes": source.stat().st_size, "sha256": sha(source)}
        if target.is_file() and target.stat().st_size:
            with target.open("rb") as f:
                f.seek(-1, 2)
                if f.read(1) != b"\n":
                    raise ValueError(f"retained JSONL lacks a terminal newline: {name}")
        with target.open("ab") as out, source.open("rb") as incoming:
            shutil.copyfileobj(incoming, out, 1024**2)
    merged = deepcopy(old_state)
    merged["modes"][added_market] = new_state["modes"][added_market]
    merged["enabled_markets"] = sorted(merged["modes"])
    merged["state_revision"] = int(merged.get("state_revision") or 0)+1
    merged["dashboard_content_revision"] = int(merged.get("dashboard_content_revision") or 0)+1
    atomic_write_json(candidate / "state.json", merged)
    positions_path = candidate / "positions.json"
    if positions_path.is_file() and (new_replay / "positions.json").is_file():
        positions = _object(positions_path)
        incoming = _object(new_replay / "positions.json")
        # Canonical positions snapshots wrap accounts under modes.
        if isinstance(positions.get("modes"), dict) and isinstance(incoming.get("modes"), dict):
            positions["modes"][added_market] = incoming["modes"][added_market]
            atomic_write_json(positions_path, positions)
    for path in (new_replay / "position_history").glob("*/*.json"):
        relative = path.relative_to(new_replay)
        target = candidate / relative
        if not target.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
        else:
            prior, incoming = _object(target), _object(path)
            if isinstance(prior.get("modes"), dict) and isinstance(incoming.get("modes"), dict):
                prior["modes"][added_market] = incoming["modes"][added_market]
                atomic_write_json(target, prior)
            else:
                raise ValueError(f"unsupported overlapping position history: {relative}")
    for name, proof in source_proofs.items():
        if ((live / name).stat().st_size != proof["bytes"] or sha(live / name) != proof["sha256"]
                or sha(candidate / name, proof["bytes"]) != proof["sha256"]):
            raise ValueError(f"existing ledger changed during addition: {name}")
    if _object(live / "state.json")["modes"] != old_state["modes"]:
        raise ValueError("existing accounts advanced during addition; retry from a fresh snapshot")
    for name, proof in appended.items():
        if sha(new_replay / name) != proof["sha256"] or sha(component / name) != proof["sha256"]:
            raise ValueError(f"new component changed during addition: {name}")
    receipt = {"schema_version": 1, "contract": CONTRACT, "simulation_only": True,
               "production_order_possible": False, "added_market": added_market,
               "retained_markets": sorted(old_state["modes"]),
               "retained_execution_prefixes": source_proofs,
               "appended_execution_segments": appended,
               "new_mode_component": "addition_evidence/new_mode",
               "prior_state": "addition_evidence/prior_state.json",
               "prior_rebuild_receipt": "addition_evidence/prior_rebuild_receipt.json",
               "old_signal_pins_and_executions_unchanged": True,
               "sessions": _object(new_replay / "rebuild_receipt.json")["sessions"]}
    atomic_write_json(candidate / "rebuild_receipt.json", receipt)
    return receipt


def validate_addition(candidate: Path, *, expected_markets: set[str]) -> dict:
    """Validate both evidence chains plus the complete combined minute grid."""
    from scripts.promote_tw_day_trade_replay import _validate_rebuild, _validate_minute_curve_coverage, _promotion_input_hashes
    receipt = _object(candidate / "rebuild_receipt.json")
    input_hashes = _promotion_input_hashes(candidate)
    if receipt.get("contract") != CONTRACT or receipt.get("simulation_only") is not True or receipt.get("production_order_possible") is not False:
        raise ValueError("invalid additive paper contract")
    added = receipt["added_market"]
    retained = set(receipt["retained_markets"])
    if added in retained or expected_markets != retained | {added}:
        raise ValueError("additive mode sets do not match exactly")
    state = _object(candidate / "state.json")
    prior = _object(evidence_path(candidate, receipt["prior_state"]))
    _flat(state)
    if set(state["modes"]) != expected_markets:
        raise ValueError("candidate mode set changed")
    for market in retained:
        if state["modes"][market] != prior["modes"][market]:
            raise ValueError(f"existing account was altered: {market}")
    for name, proof in receipt["retained_execution_prefixes"].items():
        if name not in LEDGERS or sha(candidate / name, proof["bytes"]) != proof["sha256"]:
            raise ValueError(f"retained decision/execution bytes changed: {name}")
    component = evidence_path(candidate, receipt["new_mode_component"])
    if _object(component / "rebuild_receipt.json").get("contract") == CONTRACT:
        raise ValueError("an addition component must be an independent replay, not another union")
    segments = receipt.get("appended_execution_segments") or {}
    if set(segments) != {name for name in LEDGERS if (component / name).is_file()}:
        raise ValueError("addition is missing exact new execution segment proofs")
    for name, proof in segments.items():
        prefix = receipt["retained_execution_prefixes"].get(name, {}).get("bytes", 0)
        if (proof["offset"] != prefix or (candidate / name).stat().st_size != prefix + proof["bytes"]
                or (component / name).stat().st_size != proof["bytes"]
                or sha(component / name) != proof["sha256"]
                or sha(candidate / name, proof["bytes"], offset=prefix) != proof["sha256"]):
            raise ValueError(f"new execution segment differs from accepted replay: {name}")
    acceptance = _validate_rebuild(component, expected_markets={added})
    if state["modes"][added] != _object(component / "state.json")["modes"][added]:
        raise ValueError("new mode differs from independently validated component")
    dates = acceptance["session_dates"]
    failures = []
    minute = _validate_minute_curve_coverage(candidate, completed_session_dates=dates,
                                            expected_markets=expected_markets, failures=failures)
    if failures:
        raise ValueError("additive minute acceptance failed: " + "; ".join(failures[:15]))
    if input_hashes != _promotion_input_hashes(candidate):
        raise ValueError("additive ledger changed during validation")
    return {**acceptance, "contract": CONTRACT, "added_market": added, "mode_set": sorted(expected_markets),
            "retained_markets": sorted(retained), "old_signal_pins_and_executions_unchanged": True,
            "ending_equity_twd": {m: float(v["total_equity_twd"]) for m,v in state["modes"].items()},
            "final_open_positions": {m: int(v.get("open_position_count") or 0) for m,v in state["modes"].items()},
            "minute_curve_validation": minute, "state_sha256": sha(candidate / "state.json"),
            "rebuild_receipt_sha256": sha(candidate / "rebuild_receipt.json"),
            "promotion_input_hashes": input_hashes,
            "new_component_acceptance": acceptance}
