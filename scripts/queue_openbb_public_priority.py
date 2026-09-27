#!/usr/bin/env python3
"""Repair missing macro plan membership and enqueue tails in the SAME manifest.

This is a planner, not a second downloader. Existing OpenBB workers, provider
quotas, retries and atomic Parquet writer execute every queued task. Superseded
plans retain their evidence; verified task IDs can be reattached without a
network call. Current-revision tails are labelled followups, never edits to the
pinned initial archive end date.
"""
from __future__ import annotations

import argparse
import fcntl
from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json
from downloader.download_openbb_archive import Manifest, PlannerContext, _load_openbb, _plan_endpoint, make_task, load_country_codes
from downloader.openbb_eia_history import ENDPOINT as ENERGY_ENDPOINT, ROUTES as ENERGY_ROUTES

PROVIDERS = frozenset({"eia", "imf", "oecd", "uncomtrade"})


def footer_evidence(path: Path, expected_rows: int) -> dict:
    try:
        file = pq.ParquetFile(path)
        if file.metadata.num_rows != expected_rows or expected_rows <= 0:
            return {"valid": False, "reason": "row_count_mismatch"}
        first, last = [], []
        for name in ["date", "period", "timestamp", "time", "time_period"]:
            index = file.schema_arrow.get_field_index(name)
            if index < 0:
                continue
            for i in range(file.metadata.num_row_groups):
                stats = file.metadata.row_group(i).column(index).statistics
                if stats and stats.has_min_max:
                    first.append(str(stats.min))
                    last.append(str(stats.max))
            if first:
                break
        return {"valid": True, "rows": expected_rows, "first_observation": min(first, default=None),
                "last_observation": max(last, default=None), "validation": "footer_and_row_count; not full content or PIT audit"}
    except Exception as exc:
        return {"valid": False, "reason": type(exc).__name__}


def tail_kwargs(kwargs: dict, end: str) -> dict | None:
    if not kwargs.get("end_date") or not kwargs.get("start_date"):
        return None  # discovery/snapshot routes retain their own ownership
    previous_end = date.fromisoformat(str(kwargs["end_date"])[:10])
    if previous_end >= date.fromisoformat(end):
        return None
    frequency = str(kwargs.get("frequency", "daily")).lower()
    overlap = (previous_end - timedelta(days=31)).isoformat()
    if frequency in {"annual", "year", "yearly"}:
        overlap = f"{previous_end.year - 2}-01-01"
    elif frequency in {"quarter", "quarterly", "month", "monthly"}:
        overlap = f"{previous_end.year - 1}-01-01"
    return {**kwargs, "start_date": max(str(kwargs["start_date"]), overlap),
            "end_date": end}


def build_work(manifest: Manifest, context: PlannerContext, *, end: str, cursors: dict | None = None) -> tuple[list, list, list]:
    cursors = cursors if cursors is not None else {}
    restore, tails, report = [], [], []
    commands = {**context.commands, ENERGY_ENDPOINT: ["eia"]}
    for raw_endpoint, providers in sorted(commands.items()):
        if not set(providers) & PROVIDERS and raw_endpoint.lstrip(".") != "economy.export_destinations":
            continue
        endpoint = raw_endpoint.lstrip(".")
        if endpoint == ENERGY_ENDPOINT:
            tasks = [make_task(context, endpoint, name, {"dataset": name, "start_date": "1900-01-01", "end_date": context.end_date}, ("eia",))
                     for name in ENERGY_ROUTES]
        else:
            tasks, decision = _plan_endpoint(context, raw_endpoint, providers)
            if decision.decision != "included":
                report.append({"endpoint": endpoint, "state": decision.decision, "reason": decision.reason})
                continue
        for task in tasks:
            if endpoint == "economy.gdp.forecast":
                # New source/precision contract gets new immutable task IDs.
                # Preserve the old SDK's edition/truncated-positive-only shards
                # as evidence instead of silently overwriting their meaning.
                task = make_task(context, endpoint, task.scope_key + "/adapter=latest_signed_raw_v1", task.kwargs, task.providers)
            row = manifest.connection.execute("SELECT active,status,rows,output_path,selected_provider FROM tasks WHERE task_id=?", (task.task_id,)).fetchone()
            evidence = footer_evidence(Path(row["output_path"]), int(row["rows"])) if row and row["status"] == "success" else {}
            entry = {"endpoint": endpoint, "scope": task.scope_key, "task_id": task.task_id,
                     "providers": task.providers, "active": bool(row["active"]) if row else False,
                     "status": row["status"] if row else "missing_from_manifest", "stored": evidence}
            if endpoint == "economy.export_destinations":
                entry["history_scope"] = "Latest available annual export destinations; UN Comtrade fallback preview is not global trade history"
            if not row or not row["active"]:
                if row and row["status"] == "success" and not evidence.get("valid"):
                    entry["repair_state"] = "invalid_source_requires_archive_repair"
                else:
                    restore.append(task)
            boundary = task.kwargs
            cursor = cursors.get(task.task_id, {})
            if cursor.get("task_id"):
                tail_row = manifest.connection.execute("SELECT active,status,rows,output_path FROM tasks WHERE task_id=?",
                                                       (cursor["task_id"],)).fetchone()
                if tail_row and tail_row["status"] == "success" and footer_evidence(Path(tail_row["output_path"]), int(tail_row["rows"]))["valid"]:
                    boundary = cursor["kwargs"]
                elif tail_row and tail_row["status"] == "empty":
                    # The canonical worker accepted an authoritative empty
                    # window. Advance the checked boundary (not data-through),
                    # retaining overlap so later publications are still fetched.
                    boundary = cursor["kwargs"]
                elif (tail_row and tail_row["status"] == "failed"
                      and tail_kwargs(task.kwargs, cursor["kwargs"]["end_date"]) != cursor["kwargs"]):
                    # One bounded repair of the old daily-window assumption.
                    # Keep the failure receipt, replace only this exact task
                    # with a frequency-aware query, never reset all failures.
                    entry["supersedes_failed_task"] = cursor["task_id"]
                elif tail_row:
                    # Pending/error followup is owned by the existing worker.
                    # A fresh timer date must not clone the unresolved work.
                    entry["incremental_state"] = "existing_followup_" + str(tail_row["status"])
                    report.append(entry)
                    continue
            updated = tail_kwargs(boundary, end)
            if updated and evidence.get("valid"):
                # Fixed daily task identity. Repeated timer invocations cannot
                # create duplicate requests for the same endpoint/scope/day.
                tail = make_task(context, endpoint, task.scope_key + f"/incremental={end}", updated, task.providers)
                tails.append(tail)
                cursors[task.task_id] = {"task_id": tail.task_id, "kwargs": updated}
            report.append(entry)
    return restore, tails, report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("data_openBB"))
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--end-date", default=date.today().isoformat())
    args = parser.parse_args(argv)
    root = args.output_dir
    lock = (root / "_state/public_priority.lock").open("a")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    pinned_end = (root / "_state/archive_end_date.txt").read_text().strip()
    _, schemas, commands = _load_openbb(args.env_file)
    context = PlannerContext(schemas, commands, root, "2000-01-01", pinned_end,
                             [], [], [], [], load_country_codes(), None, set(), (), None, show_progress=False)
    manifest = Manifest(root / "_state/openbb_archive.sqlite3")
    try:
        token = manifest.meta_value("active_plan_token")
        if not token:
            raise RuntimeError("No canonical active plan; refusing a second archive")
        cursor_path = root / "_state/public_priority_cursors.json"
        cursors = json.loads(cursor_path.read_text()) if cursor_path.is_file() else {}
        restore, tails, entries = build_work(manifest, context, end=args.end_date, cursors=cursors)
        if args.apply:
            # No reconcile/deactivate operation, no status reset and no new
            # worker: preserve the running archive and existing good shards.
            if manifest.meta_value("active_plan_token") != token:
                raise RuntimeError("Archive plan changed during repair; retry without modifying the new plan")
            manifest.upsert_tasks(restore + tails, plan_token=token, task_source="followup")
            for entry in entries:
                if entry.get("supersedes_failed_task"):
                    manifest.connection.execute("UPDATE tasks SET active=0 WHERE task_id=? AND status='failed' AND plan_token=?",
                                                (entry["supersedes_failed_task"], token))
            manifest.connection.commit()
            atomic_write_json(cursor_path, cursors)
            if restore or tails:
                # A sleeping archive must not wait for an unrelated FMP quota
                # reset before accepting new EIA/IMF/OECD work. Atomic signal
                # after the committed upsert; the worker polls only its stat.
                atomic_write_json(root / "_state/public_priority_queue_event.json",
                                  {"plan_token": token, "updated_at_utc": datetime.now(UTC).isoformat(),
                                   "tasks": len(restore) + len(tails)})
        payload = {"updated_at_utc": datetime.now(UTC).isoformat(), "mode": "applied" if args.apply else "plan",
                   "active_plan_token": token, "pinned_initial_end": pinned_end, "incremental_end": args.end_date,
                   "restored_or_missing_tasks": len(restore), "incremental_tasks": len(tails),
                   "network_requests": 0, "history_complete": False, "datasets": entries}
        atomic_write_json(root / "_state/public_priority_plan.json", payload)
        print(json.dumps({k:v for k,v in payload.items() if k != "datasets"}))
    finally:
        manifest.connection.close()
        lock.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
