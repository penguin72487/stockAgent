"""Read existing migration journals; never scan payloads or invoke a provider.

This is a local status projection, not another writer or acceptance authority.
Overlapping source branches cannot be added into a global byte percentage.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from downloader.artifact_io import atomic_write_json  # noqa: E402
from stockagent.data_sync.dataset_migration import (  # noqa: E402
    migration_owner_observation, migration_process_identity,
)


def read_document(path):
    before = path.lstat()
    if path.is_symlink() or not path.is_file() or before.st_size > 32 * 1024**2:
        raise ValueError("status journal is redirected, non-regular or oversized")
    document = json.loads(path.read_bytes())
    after = path.lstat()
    if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
        raise ValueError("status journal changed; retry this read")
    return document


def file_descriptor_status(owner, observed_owner, *, proc_root=Path("/proc")):
    """Best-effort process-wide pressure, never activity or completion proof."""
    empty = {"contract": "process_fd_budget_observation_v1", "state": "unverified_owner",
             "open_descriptors": None, "soft_limit": None, "remaining_descriptors": None,
             "fraction_used": None, "sample_is_atomic": False}
    if not owner or not observed_owner.get("process_matches"):
        return empty
    try:
        pid = owner["pid"]
        expected = {key: owner[key] for key in ("pid", "start_ticks", "boot_id")}
        if migration_process_identity(pid) != expected:
            return empty
        process = proc_root / str(pid)
        limit_line = next(line for line in (process / "limits").read_text().splitlines()
                          if line.startswith("Max open files"))
        raw_limit = limit_line.split()[3]
        limit = None if raw_limit == "unlimited" else int(raw_limit)
        if limit is not None and limit < 1:
            raise ValueError("nonpositive process descriptor limit")
        count = sum(1 for _ in (process / "fd").iterdir())
        if migration_process_identity(pid) != expected:
            return empty
        return {**empty, "state": "sampled", "open_descriptors": count, "soft_limit": limit,
                "remaining_descriptors": None if limit is None else limit - count,
                "fraction_used": None if limit is None else count / limit}
    except (OSError, ValueError, KeyError, IndexError, StopIteration) as error:
        return {**empty, "state": "unavailable", "error_type": type(error).__name__}


def activity_progress_observation(item, now):
    """Separate a transition journal's age from actual, attempt-bound work.

    Never infer a deadlock from a stale sample: a single verified cold read may
    itself take minutes. This is an observation, not a restart/completion gate.
    """
    result = {"state": "inactive", "observed_at_utc": None, "age_seconds": None,
              "stage": None, "recent_threshold_seconds": 300,
              "stale_is_failure_proof": False, "acceptance": False}
    if not item["runtime_active"]:
        return result
    kind = "recovery" if item["state"] == "recovery" else "publication"
    document = item.get(kind + "_progress")
    if not document:
        return {**result, "state": "missing_sample"}
    stamp = document.get("observed_at_utc", document.get("updated_at_utc"))
    result.update(stage=document.get("stage"), observed_at_utc=stamp)
    if not stamp:
        return {**result, "state": "missing_timestamp"}
    try:
        timestamp = datetime.fromisoformat(stamp)
        if timestamp.tzinfo is None or timestamp.utcoffset() is None:
            raise ValueError("progress timestamp has no timezone")
        age = (now - timestamp).total_seconds()
        if age < 0:
            raise ValueError("progress timestamp is in the future")
    except (TypeError, ValueError):
        return {**result, "state": "invalid_timestamp"}
    return {**result, "state": "recent" if age <= 300 else "stale_sample",
            "age_seconds": age}


def status(current_path):
    current_path = current_path.absolute()
    current = read_document(current_path)
    operation = Path(current["operation_directory"])
    if (operation.resolve() != operation or not operation.is_relative_to(current_path.parent / "operations")
            or current.get("contract") != "all_catalog_incremental_migration_v1"):
        raise ValueError("migration operation does not belong to this original owner")
    plan, progress = read_document(operation / "plan.json"), read_document(operation / "progress.json")
    if not (current["plan_sha256"] == plan["plan_sha256"] == progress["plan_sha256"]):
        raise ValueError("current/plan/progress identities disagree")
    planned = {entry["dataset"]: entry for entry in plan["catalog"]}
    if set(planned) != set(progress["datasets"]):
        raise ValueError("migration status omits an enrolled source")
    owner = progress.get('runtime_owner')
    observed_owner = migration_owner_observation(operation, owner)
    rows = []
    for dataset, row in progress["datasets"].items():
        item = {"dataset": dataset, **row}
        active = bool(row['state'] in ('processing', 'recovery') and owner
            and observed_owner['lock_held'] and observed_owner['process_matches']
            and owner.get('active_dataset') == dataset
            and owner.get('active_attempt_id') == row.get('attempt_id')
            and owner.get('activity_id') == row.get('owner_activity_id'))
        item['runtime_active'] = active
        if row['state'] in ('processing', 'recovery') and not active:
            item['journal_state'] = row['state']
            item['state'] = 'interrupted' if owner or not observed_owner['lock_held'] else 'inflight_unverified'
        for kind in ("publication", "recovery"):
            path = operation / kind / (dataset + (".json" if kind == "publication" else ".progress.json"))
            if path.is_file():
                document = read_document(path)
                if row.get("attempt_id"):
                    current_attempt = document.get("attempt_id") == row["attempt_id"]
                else:
                    # Existing in-flight workers predate attempt IDs. A
                    # progress timestamp older than the new start is stale.
                    stamp = document.get("observed_at_utc", document.get("updated_at_utc"))
                    current_attempt = not (stamp and row.get("started_at_utc")
                        and datetime.fromisoformat(stamp) < datetime.fromisoformat(row["started_at_utc"]))
                if kind == "recovery" and row.get("snapshot_id"):
                    current_attempt = current_attempt and document.get("snapshot_id") == row["snapshot_id"]
                if not current_attempt:
                    item["previous_attempt_" + kind + "_progress"] = document
                    item[kind + "_progress"] = None
                elif item['state'] in ('interrupted', 'inflight_unverified'):
                    item['unverified_' + kind + '_progress'] = document
                    item[kind + '_progress'] = None
                else:
                    item[kind + "_progress"] = document
        item["shared_export"] = bool(planned[dataset]["publish"])
        item["native_workspace_deletion_authorized"] = False
        rows.append(item)
    observed_at = datetime.now(UTC)
    for item in rows:
        item["activity_progress_observation"] = activity_progress_observation(item, observed_at)
    states = Counter(row["state"] for row in rows)
    counts = {"shared": sum(row["shared_export"] for row in rows),
              "host_private": sum(not row["shared_export"] for row in rows)}
    return {"contract": "dataset_migration_status_projection_v1",
        "observed_at_utc": observed_at.isoformat(), "plan_sha256": current["plan_sha256"],
        "transition_journal_updated_at_utc": progress.get("updated_at_utc"),
        "latest_active_work_progress_at_utc": max((
            row["activity_progress_observation"]["observed_at_utc"] for row in rows
            if row["activity_progress_observation"]["state"] in {"recent", "stale_sample"}),
            key=lambda value: datetime.fromisoformat(value), default=None),
        "operation_directory": str(operation), "state": progress["state"], "source_count": len(rows),
        "verified_sources": states.get("verified", 0), "states": dict(states), "domains": counts,
        "runtime_owner": {**observed_owner, 'identity_recorded': owner is not None,
                          "file_descriptors": file_descriptor_status(owner, observed_owner)},
        "active_sources": sum(row['runtime_active'] for row in rows),
        "datasets": rows, "source_bytes_overlap": True, "global_byte_percent": None,
        "all_current_source_history_complete_claim": False, "NAS_acceptance_claim": False,
        "source_payload_reads": 0, "provider_calls": 0,
        "notes": ["The fixed-source migration queue is distinct from download completeness and native table partitioning.",
                  "Public branches and private whole-root complements overlap; byte counts are not additive.",
                  "Publication progress is hashing/storing work, not recovery or deletion acceptance.",
                  "The transition journal changes between sources; inspect attempt-bound work samples for liveness.",
                  "A stale progress sample alone proves neither a deadlock nor a failed source."]}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--current", type=Path, default=Path("/var/lib/stockagent/lakehouse-control/dataset-versions/current.json"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    result = status(args.current)
    if args.output:
        atomic_write_json(args.output, result, durable=True)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
