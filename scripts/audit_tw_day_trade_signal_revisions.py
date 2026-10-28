#!/usr/bin/env python3
"""Pin every registered paper signal and audit its publication clock.

This inspects immutable signal artifacts against a previously pinned opening
inventory.  It does not change the paper ledger, infer missing signals, run
orders, or assert that minute prices and fills are sufficient for a replay.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timedelta
import hashlib
import json
import math
from pathlib import Path
import sys
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audit_tw_day_trade_opening_replay_inputs import _digest
from scripts.run_tw_day_trade_simulation import _mode_specs
from stockagent.live.market_config import SIGNAL_UPDATE_ONCE, SIGNAL_UPDATE_PROGRESSIVE
from stockagent.live.opening_signal_revision import revision_admission
from stockagent.live.signal_artifacts import read_execution_weight_rows

TAIPEI = ZoneInfo("Asia/Taipei")


def _clock(value: Any, day: date) -> datetime:
    clock = datetime.fromisoformat(str(value or ""))
    if clock.utcoffset() is None or clock.astimezone(TAIPEI).date() != day:
        raise ValueError("clock is not an aware timestamp in the requested session")
    return clock


def inspect_revisions(*, day: date, inventory: dict[str, Any], specs: list) -> dict[str, Any]:
    """Compare all registered versions; retain anomalies instead of repairing clocks."""
    if (inventory.get("contract") != "first_published_signal_and_local_minute_inventory_v1"
            or inventory.get("session_date") != str(day)
            or inventory.get("simulation_only") is not True
            or inventory.get("production_order_possible") is not False):
        raise ValueError("expected a simulation-only opening inventory for the requested session")
    source_modes = inventory.get("modes") or {}
    by_market = {spec.market: spec for spec in specs}
    if not source_modes or set(by_market) != set(source_modes):
        raise ValueError("enabled modes differ from pinned inventory")
    output: dict[str, Any] = {}
    for market, spec in sorted(by_market.items()):
        source = source_modes[market]
        registrations = source.get("registrations") or []
        if source.get("registration_count") != len(registrations):
            raise ValueError(f"{market}: registration count differs from pinned inventory")
        # Index once per mode instead of doing a recursive disk search for each
        # of hundreds of revisions. Duplicate names are ambiguous, not winners.
        paths: dict[str, list[Path]] = {}
        for path in spec.live_output_dir.glob(f"{day}*/**/summary.json"):
            paths.setdefault(path.parent.name, []).append(path)
        issues: Counter[str] = Counter()
        versions: list[dict[str, Any]] = []
        seen: set[str] = set()
        last_recorded: datetime | None = None
        previous_mode: dict[str, Any] = {}
        previous_revision_id: str | None = None
        unregistered_revision_gaps = 0
        for registration in registrations:
            signal_id = str(registration.get("signal_id") or "")
            entry: dict[str, Any] = {"signal_id": signal_id, "recorded_at": registration.get("recorded_at"),
                                     "source_signal_at": registration.get("source_signal_at"),
                                     "simulation_replay": registration.get("simulation_replay"),
                                     "errors": []}
            errors = entry["errors"]
            if not signal_id or signal_id in seen:
                errors.append("missing_or_duplicate_signal_id")
            seen.add(signal_id)
            try:
                registered_at = _clock(registration.get("recorded_at"), day)
                if last_recorded is not None and registered_at < last_recorded:
                    errors.append("registration_clock_regressed")
                last_recorded = registered_at
            except (TypeError, ValueError):
                registered_at = None
                errors.append("invalid_registration_clock")
            candidates = paths.get(signal_id, [])
            if len(candidates) != 1:
                errors.append("missing_signal_artifact" if not candidates else "ambiguous_signal_artifact")
            else:
                summary_path = candidates[0].resolve()
                try:
                    before = _digest(summary_path)
                    summary = json.loads(summary_path.read_bytes())
                    if (summary.get("signal_id") != signal_id
                            or summary.get("market") != str(spec.signal_market or market)
                            or summary.get("execution_mode") != "tw_day_trade"):
                        raise ValueError("signal identity mismatch")
                    policy = getattr(spec, "signal_update_policy", SIGNAL_UPDATE_ONCE)
                    is_revision, revision_error = revision_admission(
                        policy=policy, summary=summary, mode=previous_mode, session_date=str(day))
                    if revision_error:
                        errors.append("revision_contract_" + revision_error)
                    elif policy == SIGNAL_UPDATE_PROGRESSIVE:
                        revision = summary["opening_signal_revision"]
                        old_revision = previous_mode.get("opening_signal_revision") or {}
                        if is_revision:
                            gap = revision["index"] - old_revision["index"] - 1
                            unregistered_revision_gaps += max(0, gap)
                            if (gap == 0) != (revision["parent_signal_id"] == previous_revision_id):
                                errors.append("revision_parent_index_mismatch")
                        elif revision["index"] > 0:
                            # The live consumer may start at revision N; do not
                            # pretend its N predecessors were registered.
                            unregistered_revision_gaps += revision["index"]
                        previous_mode = {"session_date": str(day), "entry_completed_at": registration.get("recorded_at"),
                                         "day_trade_signal_update_policy": policy,
                                         "opening_signal_revision": revision,
                                         "checkpoint_fingerprint": summary.get("checkpoint_fingerprint"),
                                         "config_fingerprint": summary.get("config_fingerprint")}
                        previous_revision_id = signal_id
                        entry["opening_signal_revision"] = revision
                    artifact_at = _clock(summary.get("artifact_published_at"), day)
                    # This timestamp is written *after* the atomic pointer
                    # update. It is a conservative upper bound on when the
                    # pointer became visible, not its exact publication time.
                    published_at = _clock(summary.get("execution_pointer_written_at"), day)
                    generated_at = _clock(summary.get("generated_at"), day)
                    if generated_at > artifact_at:
                        errors.append("generated_after_publication")
                    if artifact_at > published_at:
                        errors.append("pointer_before_artifact")
                    completed_at = None
                    if summary.get("artifact_completed_at"):
                        completed_at = _clock(summary["artifact_completed_at"], day)
                        # The signal producer starts rich artifact completion
                        # after the pointer write. Reversed clocks indicate
                        # an inconsistent retained summary or producer path.
                        if completed_at < published_at:
                            errors.append("artifact_completed_before_pointer_write")
                    # recorded_at is second-truncated. An artifact created after
                    # that entire second is certainly too late. A pointer upper
                    # bound after the second, with an earlier artifact, leaves
                    # the actual pointer/registration order unresolved.
                    if registered_at is not None:
                        registration_end = registered_at + timedelta(seconds=1)
                        if artifact_at >= registration_end:
                            errors.append("published_after_registration")
                        elif published_at >= registration_end:
                            errors.append("publication_order_unresolved_within_clock_intervals")
                    weights = summary_path.with_name("target_weights.parquet")
                    if weights.is_file():
                        weights_hash = _digest(weights)
                        rows = pl.read_parquet(weights, columns=["symbol", "target_weight"]).to_dicts()
                    else:
                        weights = summary_path.with_name("execution_weights.json")
                        weights_hash = _digest(weights)
                        rows = read_execution_weight_rows(weights, market=str(spec.signal_market or market),
                                                          signal_id=signal_id)
                    symbols: set[str] = set()
                    nonzero = 0
                    for row in rows:
                        symbol = str(row.get("symbol") or "")
                        value = float(row.get("target_weight") or 0.0)
                        if not symbol or symbol in symbols or not math.isfinite(value):
                            raise ValueError("invalid symbol or target weight")
                        symbols.add(symbol)
                        nonzero += value != 0.0
                    if not rows:
                        raise ValueError("empty signal weights")
                    if _digest(summary_path) != before or _digest(weights) != weights_hash:
                        raise RuntimeError("signal changed during audit")
                    entry.update({"summary_path": str(summary_path), "summary_sha256": before,
                                  "weights_path": str(weights), "weights_sha256": weights_hash,
                                  "generated_at": generated_at.isoformat(),
                                  "artifact_published_at": artifact_at.isoformat(),
                                  "execution_pointer_written_at": published_at.isoformat(),
                                  "artifact_completed_at": completed_at.isoformat() if completed_at else None,
                                  "target_rows": len(rows), "nonzero_target_rows": nonzero})
                except (OSError, ValueError, TypeError, KeyError, RuntimeError, json.JSONDecodeError) as exc:
                    errors.append("invalid_or_unstable_artifact")
                    entry["artifact_error"] = f"{type(exc).__name__}: {exc}"
            issues.update(errors)
            versions.append(entry)
        first_id = source.get("first_published_signal_id")
        output[market] = {"first_published_signal_id": first_id,
                          "first_published_at": source.get("first_published_at"),
                          "first_registered_matches_first_publication": bool(
                              registrations and registrations[0].get("signal_id") == first_id),
                          "registration_count": len(registrations),
                          "unregistered_intermediate_revision_count": unregistered_revision_gaps,
                          "issue_counts": dict(sorted(issues.items())), "versions": versions}
    return {"schema_version": 1, "contract": "paper_signal_revision_source_clock_audit_v1",
            "session_date": str(day), "status": "source_audit_only_not_a_replay_or_execution_acceptance",
            "simulation_only": True, "production_order_possible": False, "ledger_modified": False,
            "logical_event_journal_sha256": inventory.get("logical_event_journal_sha256"),
            "all_registered_artifacts_valid_and_causal": all(not m["issue_counts"] for m in output.values()),
            "pointer_written_at_is_visibility_upper_bound_not_exact_clock": True,
            "source_timestamps_within_one_second_not_order_proof": True,
            "completeness_of_unregistered_publications_not_proven": True, "modes": output}


def inspect_publications(*, day: date, inventory: dict[str, Any], specs: list) -> dict[str, Any]:
    """Inventory retained intraday publications, separately from ledger consumption.

    Revision artifacts in the first-published root's lineage may never have
    reached the executor. Their publication times are source times, never
    claims that the ledger registered them or that an order filled.
    """
    virtual_modes: dict[str, Any] = {}
    scan_counts: dict[str, Any] = {}
    actual: dict[str, dict[str, list[str]]] = {}
    for spec in specs:
        market = spec.market
        source = inventory["modes"][market]
        root_id = source["first_published_signal_id"]
        first_path = Path(source["summary_path"])
        if _digest(first_path) != source["summary_sha256"]:
            raise ValueError(f"{market}: first published source changed")
        paths = sorted(spec.live_output_dir.glob(f"{day}*/**/summary.json"))
        selected: list[dict[str, Any]] = []
        unclassified = 0
        actual[market] = {}
        for row in source.get("registrations") or []:
            actual[market].setdefault(str(row.get("signal_id") or ""), []).append(str(row.get("recorded_at") or ""))
        for path in paths:
            try:
                before = _digest(path)
                summary = json.loads(path.read_bytes())
                if _digest(path) != before:
                    raise RuntimeError("summary changed during publication scan")
                if (summary.get("opening_signal_revision") or {}).get("root_signal_id") != root_id:
                    continue
                published = _clock(summary.get("execution_pointer_written_at"), day)
                local = published.astimezone(TAIPEI)
                if not (local.hour == 9 or 10 <= local.hour < 13
                        or local.hour == 13 and local.minute < 20):
                    continue
                selected.append({"signal_id": str(summary.get("signal_id") or ""),
                                 "recorded_at": published.isoformat(), "simulation_replay": False})
            except (OSError, TypeError, ValueError, AttributeError, RuntimeError, json.JSONDecodeError):
                # An unreadable artifact cannot be ruled out of the first
                # lineage solely because its identity failed to load.
                unclassified += 1
        selected.sort(key=lambda item: (_clock(item["recorded_at"], day), item["signal_id"]))
        if not selected or selected[0]["signal_id"] != root_id:
            raise ValueError(f"{market}: retained publications do not start with the first root")
        after = sorted(spec.live_output_dir.glob(f"{day}*/**/summary.json"))
        scan_counts[market] = {"examined_summary_files": len(paths), "unclassified_summary_files": unclassified,
                               "summary_membership_stable_during_scan": paths == after}
        virtual_modes[market] = {**source, "registration_count": len(selected), "registrations": selected}
    virtual = {**inventory, "modes": virtual_modes}
    result = inspect_revisions(day=day, inventory=virtual, specs=specs)
    result.update({"contract": "paper_signal_published_revision_clock_audit_v1",
                   "status": "published_source_audit_only_not_ledger_registration_or_replay",
                   "completeness_of_unregistered_publications_not_proven": True})
    for market, mode in result["modes"].items():
        mode["publication_count"] = mode.pop("registration_count")
        mode.pop("first_registered_matches_first_publication", None)
        mode.update(scan_counts[market])
        mode["published_but_not_registered_count"] = 0
        for version in mode["versions"]:
            version["publication_event_at"] = version.pop("recorded_at")
            version["ledger_registration_times"] = actual[market].get(version["signal_id"], [])
            version["was_registered"] = bool(version["ledger_registration_times"])
            if not version["was_registered"]:
                mode["published_but_not_registered_count"] += 1
    result["all_retained_published_lineages_causal"] = (
        result.pop("all_registered_artifacts_valid_and_causal")
        and all(not m["unclassified_summary_files"] and m["summary_membership_stable_during_scan"]
                for m in result["modes"].values()))
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--markets-dir", type=Path, default=ROOT / "services/discord_bot/markets")
    parser.add_argument("--published", action="store_true",
                        help="Audit retained first-root publications instead of registered events")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    inventory_bytes = args.inventory.read_bytes()
    specs, _configs, errors = _mode_specs(args.markets_dir)
    if errors:
        raise ValueError(f"mode configuration errors: {errors}")
    inspect = inspect_publications if args.published else inspect_revisions
    report = inspect(day=args.date, inventory=json.loads(inventory_bytes), specs=specs)
    report["inventory_sha256"] = hashlib.sha256(inventory_bytes).hexdigest()
    output = args.output.resolve()
    if output.is_relative_to((ROOT / "artifacts/live/tw_day_trade_simulation").resolve()):
        raise ValueError("revision audit cannot write into the live ledger")
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    print(json.dumps({"status": report["status"], "output": str(output),
                      "modes": {name: {"versions": m.get("publication_count", m.get("registration_count")),
                                       "unregistered": m.get("published_but_not_registered_count"),
                                       "issues": m["issue_counts"]}
                                for name, m in report["modes"].items()}}, ensure_ascii=False))


if __name__ == "__main__":
    main()
