#!/usr/bin/env python3
"""Stage source-verified yearly paper price limits without touching live files."""
from __future__ import annotations

import argparse
from datetime import date, datetime
import json
import os
from pathlib import Path
import shutil
import sys

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file  # noqa: E402
from scripts.prepare_finmind_tw_day_trade_price_limits import verify  # noqa: E402
from scripts.switch_tw_day_trade_strategy import _latest_completed_session  # noqa: E402
from stockagent.data_sync import workspace_volume  # noqa: E402
from stockagent.live.tw_day_trade_simulation import TAIPEI  # noqa: E402


def stage(*, sessions: list[date], baseline_root: Path, projection_root: Path,
          source_audit_path: Path, output_root: Path) -> dict:
    audit = json.loads(source_audit_path.read_text())
    if (audit.get("contract") != "isolated_counterfactual_signals_source_audit_v1"
            or audit.get("status") != "source_gaps_remain"
            or audit.get("ledger_modified") is not False
            or audit.get("publication_authorized") is not False
            or len(sessions) != audit.get("sessions") or not sessions
            or str(sessions[0]) != audit.get("start_date") or str(sessions[-1]) != audit.get("end_date")):
        raise ValueError("price-limit bundle requires the matching private signal audit")
    if sessions != sorted(set(sessions)):
        raise ValueError("completed sessions must be unique and ordered")
    workspace = workspace_volume.configuration()
    build_root = (Path(workspace["mount_point"]) / "build").resolve()
    target = output_root.resolve()
    if not target.is_relative_to(build_root) or target == build_root or target.exists():
        raise ValueError("a new isolated E build directory is required")
    workspace_volume.admit(required_bytes=128 * 1024**2, c=workspace)
    audit_hash = sha256_file(source_audit_path)
    required_days = {item["date"] for item in audit.get("targets_without_valid_limits", [])}
    if not required_days.issubset({str(day) for day in sessions}):
        raise ValueError("audited missing limits extend outside the selected sessions")
    projections = {}
    for day in sorted(required_days):
        proof = verify(projection_root / day)
        if proof.get("session_date") != day:
            raise ValueError(f"{day}: projection belongs to another session")
        projections[day] = proof
    target.mkdir(mode=0o700, parents=True, exist_ok=False)
    sources, unresolved = [], []
    for day_value in sessions:
        day = str(day_value)
        baseline = (baseline_root / f"{day}.parquet").resolve(strict=True)
        projection = projections.get(day)
        selected = Path(projection["output"]["path"]).resolve(strict=True) if projection else baseline
        before = sha256_file(selected)
        if projection and before != projection["output"]["sha256"]:
            raise ValueError(f"{day}: price-limit projection changed")
        frame = pl.read_parquet(selected)
        if (set(frame["trading_date"].to_list()) != {day}
                or frame.select(pl.col("symbol").is_duplicated().any()).item()):
            raise ValueError(f"{day}: price limits have duplicate symbols or wrong dates")
        if projection:
            unresolved.extend({"date": day, "symbol": symbol} for symbol in
                              projection["summary"]["unresolved_or_no_limit_symbols"])
        destination = target / f"{day}.parquet"
        shutil.copyfile(selected, destination)
        with destination.open("rb") as stream:
            os.fsync(stream.fileno())
        if sha256_file(destination) != before or sha256_file(selected) != before:
            raise ValueError(f"{day}: source changed during isolated staging")
        sources.append({"date": day, "kind": "finmind_supplement" if projection else "original_snapshot",
                        "input_path": str(selected), "sha256": before,
                        "original_baseline_sha256": sha256_file(baseline),
                        "projection_receipt": str((projection_root / day / "finmind_price_limits_source.json").resolve())
                        if projection else None})
    if sha256_file(source_audit_path) != audit_hash:
        raise ValueError("signal source audit changed during staging")
    for day in sorted(projections):
        verify(projection_root / day)
    workspace_volume.admit(required_bytes=0, c=workspace)
    result = {
        "contract": "private_counterfactual_price_limit_bundle_v1",
        "status": "staged_with_unresolved_limits" if unresolved else "staged_not_accepted",
        "created_at": datetime.now(TAIPEI).isoformat(), "sessions": len(sessions),
        "start_date": str(sessions[0]), "end_date": str(sessions[-1]),
        "market": audit["market"], "source_audit_path": str(source_audit_path.resolve()),
        "source_audit_sha256": audit_hash, "unresolved_or_no_limit_targets": unresolved,
        "simulation_only": True, "broker_orders_created": 0,
        "ledger_modified": False, "publication_authorized": False, "sources": sources,
    }
    atomic_write_json(target / "price_limit_bundle.json", result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--source-audit", type=Path, required=True)
    parser.add_argument("--projection-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--baseline-root", type=Path, default=ROOT / "artifacts/live/tw_price_limits")
    parser.add_argument("--tw-public-dir", type=Path, default=Path("/srv/stockagent-live/data_tw_public"))
    args = parser.parse_args()
    latest, sessions, calendar_sha = _latest_completed_session(
        tw_public_dir=args.tw_public_dir, start_date=args.start_date)
    sessions = [day for day in sessions if day <= args.end_date]
    if (args.end_date > latest or not sessions or sessions[0] != args.start_date
            or sessions[-1] != args.end_date):
        raise ValueError("boundaries must be official completed sessions")
    result = stage(sessions=sessions, baseline_root=args.baseline_root,
                   projection_root=args.projection_root, source_audit_path=args.source_audit,
                   output_root=args.output_root)
    print(json.dumps({"status": result["status"], "sessions": result["sessions"],
                      "unresolved_or_no_limit_targets": result["unresolved_or_no_limit_targets"],
                      "calendar_sha256": calendar_sha, "output_root": str(args.output_root)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
