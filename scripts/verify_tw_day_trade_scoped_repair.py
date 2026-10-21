#!/usr/bin/env python3
"""Verify one append-only paper repair independently of its live writer."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import requests
import polars as pl

from scripts import repair_tw_day_trade_flat_opening as repair
from scripts import rebuild_tw_day_trade_open_price_replay as replay


def prefix_sha256(path: Path, length: int) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        remaining = length
        while remaining:
            block = handle.read(min(8 * 1024 * 1024, remaining))
            if not block:
                raise RuntimeError(f"journal was truncated: {path}")
            digest.update(block)
            remaining -= len(block)
    return digest.hexdigest()


def suffix_rows(path: Path, offset: int):
    with path.open("rb") as handle:
        handle.seek(offset)
        for line in handle:
            if line.endswith(b"\n"):
                yield json.loads(line)


def baseline(root: Path) -> dict:
    manifest = repair._object(root / "ledger_repair_manifest.json")
    if manifest["status"] != "candidate_validated_not_applied":
        raise RuntimeError("baseline requires an unapplied candidate")
    repair._assert_source_pins(manifest["source_pins"])
    result = {"status": "captured_before_apply", "market": manifest["market"],
              "recorded_at": datetime.now(repair.TAIPEI).isoformat(), "journals": {}}
    live = Path(manifest["live_dir"])
    for name in ("signals.jsonl", "orders.jsonl", "fills.jsonl", "marks.jsonl", "events.jsonl"):
        path = live / name
        stat = path.stat()
        digest = prefix_sha256(path, stat.st_size)
        after = path.stat()
        if (after.st_dev, after.st_ino) != (stat.st_dev, stat.st_ino) or after.st_size < stat.st_size:
            raise RuntimeError(f"journal identity changed during baseline: {path}")
        result["journals"][name] = {"prefix_bytes": stat.st_size, "sha256": digest,
                                    "device": stat.st_dev, "inode": stat.st_ino}
    repair._atomic_json(root / "journal_prefix_baseline.json", result)
    return result


def _api(base: str, path: str, params: dict) -> dict:
    response = requests.get(f"{base.rstrip('/')}/{path}", params=params, timeout=(10, 40))
    response.raise_for_status()
    value = response.json()
    if value.get("simulation_only") is not True or value.get("production_order_possible") is not False:
        raise RuntimeError(f"paper/read-only API boundary missing: {base}/{path}")
    return value


def source_no_trade_minutes(files: list[dict], day: str) -> dict[str, set[str]]:
    """Explicit zero-volume source rows are not missing price observations."""
    result: dict[str, set[str]] = {}
    for source in files:
        frame = pl.read_parquet(source["path"])
        if not {"ts", "symbol", "volume_shares", "Amount"} <= set(frame.columns):
            continue
        for row in frame.filter((pl.col("volume_shares") == 0) & (pl.col("Amount") == 0)).to_dicts():
            stamp = row["ts"]
            if stamp.date().isoformat() == day:
                result.setdefault(stamp.isoformat()[:16], set()).add(str(row["symbol"]))
    return result


def verify_carried_minute(row: dict, *, no_trade_symbols: set[str], held_symbols: set[str]) -> None:
    carried = int(row.get("last_trade_carried_position_count") or 0)
    if (row.get("historical_minute_replay") is not True
            or row.get("minute_valuation_contract") != "right_labelled_historical_last_trade_mark_v1"
            or row.get("missing_price_position_count") != 0
            or carried <= 0 or carried != int(row.get("stale_position_count") or 0)
            or carried != len(no_trade_symbols & held_symbols)):
        raise RuntimeError("carried valuation lacks explicit zero-trade source evidence")


def verify_api_positions(rows: list[dict], expected: list[dict], signal_id: str) -> dict:
    """Compare financial meaning even when the public DTO redacts private IDs."""
    fields = (
        "market", "session_date", "symbol", "side", "target_weight",
        "requested_shares", "filled_shares", "signed_shares", "entry_at",
        "entry_price", "exit_at", "exit_price", "realized_net_pnl_twd",
        "total_net_pnl_twd", "simulation_replay",
    )
    identity = lambda row: tuple(str(row.get(field) or "") for field in fields[:4])
    actual = sorted(rows, key=identity)
    canonical = sorted(expected, key=identity)
    if len(actual) != len(canonical):
        raise RuntimeError("API repaired position count differs")
    for row, source in zip(actual, canonical, strict=True):
        if any(field not in row or row[field] != source.get(field) for field in fields):
            raise RuntimeError("API repaired position economics or provenance differ")
        if row.get("signal_id") not in (None, signal_id):
            raise RuntimeError("API disclosed a different repaired signal identity")
    return {"position_rows": len(actual),
            "private_signal_ids_redacted": all(row.get("signal_id") is None for row in actual),
            "exact_position_fields_verified": list(fields)}


def verify_applied_source_provenance(manifest: dict) -> dict:
    """After apply, a collector may refresh its mutable official rule alias.

    Never relax candidate/apply source pins. Only a declared eligibility alias
    can differ here, with the original private normalized snapshot still pinned
    byte-for-byte and every dated security/rule value equal to the new response.
    """
    if manifest.get("status") != "applied":
        raise RuntimeError("post-apply provenance verification requires applied status")
    references = {}
    for session in manifest["applied_sessions"]:
        for dataset, proof in session.get("eligibility_source_proof", {}).items():
            if dataset in {"twse_day_trade_eligibility", "tpex_day_trade_eligibility"}:
                references[proof["raw_path"]] = (session["session_date"], dataset, proof)
    fields = ["date", "證券代號", "證券名稱", "暫停現股賣出後現款買進當沖註記"]
    refreshed = []
    exact = 0
    for pin in manifest["source_pins"]:
        if repair._sha256(Path(pin["path"])) == pin["sha256"]:
            exact += 1
            continue
        reference = references.get(pin["path"])
        if reference is None or reference[2]["raw_sha256"] != pin["sha256"]:
            raise RuntimeError(f"repair source changed: {pin['path']}")
        day, dataset, proof = reference
        normalized = Path(proof["normalized_path"])
        if (repair._sha256(normalized) != proof["normalized_sha256"]
                or not any(p["path"] == str(normalized) and p["sha256"] == proof["normalized_sha256"]
                           for p in manifest["source_pins"])):
            raise RuntimeError("original exact-session rule snapshot changed or was not pinned")
        raw = Path(pin["path"]).read_bytes()
        frame, _ = replay._parse_historical_response_content(
            replay.DEFAULT_DATASETS[dataset], datetime.fromisoformat(day).date(), raw,
            replay._retained_rule_response_kind(dataset, raw))
        original = pl.read_parquet(normalized)
        if (not set(fields) <= set(frame.columns) or not set(fields) <= set(original.columns)
                or frame.height != proof["rows"] or original.height != proof["rows"]
                or not original.select(fields).sort(fields).equals(frame.select(fields).sort(fields))):
            raise RuntimeError("refreshed official reference changed dated security/rule values")
        refreshed.append({"dataset": dataset, "session_date": day, "mutable_raw_path": pin["path"],
                          "original_raw_sha256_at_apply": pin["sha256"],
                          "current_raw_sha256": hashlib.sha256(raw).hexdigest(),
                          "original_normalized_snapshot": str(normalized),
                          "original_normalized_sha256": proof["normalized_sha256"],
                          "normalized_rows": frame.height, "exact_rule_fields_verified": fields,
                          "original_raw_bytes_unchanged_claim": False,
                          "dated_rule_values_unchanged": True})
    return {"source_pins_total": len(manifest["source_pins"]), "byte_identical_source_pins": exact,
            "mutable_official_reference_refreshes": refreshed}


def verify(root: Path, api_bases: list[str]) -> dict:
    manifest = repair._object(root / "ledger_repair_manifest.json")
    if manifest["status"] != "applied" or manifest.get("non_target_modes_unchanged") is not True:
        raise RuntimeError("scoped append has not been accepted")
    source_proof = verify_applied_source_provenance(manifest)
    original = repair._object(root / "journal_prefix_baseline.json")
    live, market = Path(manifest["live_dir"]), manifest["market"]
    for name, proof in original["journals"].items():
        path = live / name
        stat = path.stat()
        identity_matches = (stat.st_dev, stat.st_ino) == (proof["device"], proof["inode"])
        if not identity_matches and manifest.get("atomic_recovery"):
            recovery = manifest["atomic_recovery"]
            failed_path = Path(recovery["interrupted_manifest_path"])
            if repair._sha256(failed_path) != recovery["interrupted_manifest_sha256"]:
                raise RuntimeError("interrupted recovery manifest changed")
            rollback = Path(recovery["rollback_directory"]).resolve(strict=True)
            if rollback == live.resolve() or rollback.parent != live.resolve().parent:
                raise RuntimeError("recovery rollback is not a distinct same-parent directory")
            old_path = rollback / name
            old = old_path.stat()
            identity_matches = ((old.st_dev, old.st_ino) == (proof["device"], proof["inode"])
                and prefix_sha256(old_path, proof["prefix_bytes"]) == proof["sha256"])
        if (not identity_matches or prefix_sha256(path, proof["prefix_bytes"]) != proof["sha256"]):
            raise RuntimeError(f"original journal prefix changed: {name}")
    state = repair._object(live / "state.json")
    mode = state["modes"][market]
    if repair._outcome(mode) != manifest["candidate_final_outcome"]:
        raise RuntimeError("live economics differ from independently validated candidate")
    if mode.get("ledger_state_divergence") or mode.get("open_position_count"):
        raise RuntimeError("target is divergent or not flat")
    proofs = []
    marks = list(suffix_rows(live / "marks.jsonl", manifest["journal_prefix_bytes"]["marks.jsonl"]))
    fills = list(suffix_rows(live / "fills.jsonl", manifest["journal_prefix_bytes"]["fills.jsonl"]))
    for item in manifest["applied_sessions"]:
        day, signal_id = item["session_date"], item["signal_id"]
        selected = [row for row in fills if row.get("market") == market and row.get("session_date") == day]
        entry = sum(int(row["quantity"]) for row in selected if row.get("purpose") == "entry")
        exit_quantity = sum(int(row["quantity"]) for row in selected if row.get("purpose") != "entry")
        if entry != item["entry_requested_shares"] or exit_quantity != entry:
            raise RuntimeError("entry/exit journal quantities do not balance")
        by_minute = {str(row["minute"])[:16]: row for row in marks
                     if row.get("market") == market and row.get("session_date") == day}
        start = datetime.fromisoformat(f"{day}T09:01:00+08:00")
        expected = {(start+timedelta(minutes=i)).isoformat()[:16] for i in range(270)}
        if set(by_minute) != expected:
            raise RuntimeError("270 exact source-backed minute marks required")
        source = next(source for source in manifest["sessions"] if source["session_date"] == day)
        no_trades = source_no_trade_minutes(source["minute_source_after"]["source_files"], day)
        carried_minutes = []
        for minute, row in by_minute.items():
            if int(row.get("missing_price_position_count") or 0):
                raise RuntimeError(f"minute has an unsourced held position: {minute}")
            if row.get("valuation_stale"):
                held = {str(p["symbol"]) for p in mode["positions"].values()
                        if str(p["entry_at"])[:16] <= minute < str(p.get("exit_at") or f"{day}T13:31")[:16]}
                verify_carried_minute(row, no_trade_symbols=no_trades.get(minute, set()), held_symbols=held)
                carried_minutes.append(minute)
        endpoints = []
        expected_positions = [position for position in mode["positions"].values()
                              if position.get("session_date") == day and position.get("signal_id") == signal_id]
        for base in api_bases:
            positions = _api(base, "positions", {"mode": market, "date": day, "limit": 10})
            position_proof = verify_api_positions(positions["rows"], expected_positions, signal_id)
            history = _api(base, "history", {"range": "all", "resolution": "1m", "start_date": day, "end_date": day})
            series = next(row for row in history["minute_series"] if row["series_id"] == market)
            points = series["points"]
            if (len(points) != 270 or points[0][0] != int(start.timestamp() // 60)
                    or points[-1][0]-points[0][0] != 269 or history.get("downsampled") is True):
                raise RuntimeError(f"API does not expose the complete 270-minute curve: {base}")
            endpoints.append({"base": base, **position_proof, "minute_points": len(points),
                              "first_point": points[0], "last_point": points[-1]})
        proofs.append({"session_date": day, "entry_shares": entry, "exit_shares": exit_quantity,
                       "minute_points": len(by_minute), "explicit_zero_trade_carried_minutes": sorted(carried_minutes),
                       "unsourced_held_price_minutes": 0, "api_evidence": endpoints})
    unit = subprocess.run(["systemctl", "show", repair.SERVICE, "-p", "ActiveState", "-p", "InvocationID", "-p", "MainPID"],
                          check=True, text=True, capture_output=True).stdout
    if "ActiveState=active" not in unit:
        raise RuntimeError("native paper owner did not recover")
    result = {"status": "accepted", "market": market, "recorded_at": datetime.now(repair.TAIPEI).isoformat(),
              "original_journal_prefixes_sha256_verified": True, "source_provenance": source_proof,
              "original_weights_unchanged": manifest.get("preserve_original_signal") is True,
              "non_target_modes_unchanged_during_append": True, "candidate_live_economics_equal": True,
              "simulation_only": True, "production_order_possible": False, "sessions": proofs,
              "total_equity_twd": mode["total_equity_twd"], "native_owner": unit}
    repair._atomic_json(root / "acceptance.json", result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("baseline", "verify"))
    parser.add_argument("--operation-root", type=Path, required=True)
    parser.add_argument("--api-base", action="append", default=[])
    args = parser.parse_args()
    result = baseline(args.operation_root) if args.action == "baseline" else verify(args.operation_root, args.api_base)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
