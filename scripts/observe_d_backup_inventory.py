#!/usr/bin/env python3
"""Classify the canonical backup catalog without scanning or moving D data."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import fcntl
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from stockagent.data_sync.d_backup_inventory import CLASSIFIER_REVISION, CONTRACT, classify
from stockagent.data_sync.desync_snapshots import atomic_write_bytes
from stockagent.service_layout import service_path


def observe_layout(output, *, cold=Path('/srv/stockagent-packed'),
                   workspace=Path('/srv/stockagent-workspace'), force=False,
                   now=None, guard=lambda:None):
    """Cache a bounded metadata-only classification under the same observer."""
    from stockagent.data_sync import d_pack_layout_inventory as layout, workspace_volume
    now=time.time() if now is None else now
    if output.exists() and not force:
        if output.is_symlink():
            raise ValueError('redirected_layout_projection')
        try:
            prior=read_small(output)
        except (OSError,ValueError):
            prior={}  # A disposable damaged cache is rebuilt from the owners.
        clean=layout.public_status(prior,now=now)
        if clean is not None and now-datetime.fromisoformat(clean['observed_at_utc']).timestamp()<6*3600:
            return 'current'
    output.parent.mkdir(parents=True,exist_ok=True)
    fd=os.open(output.with_suffix('.lock'),os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW|os.O_CLOEXEC,0o600)
    with os.fdopen(fd,'a+') as lock:
        try:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:
            return 'already_running'
        guard();workspace_volume.check()
        observed=layout.collect(cold,workspace,workers=4,progress=lambda _row:guard())
        measured=layout.object_presence(cold,observed['objects'],workers=4,progress=lambda _row:guard())
        measured.pop('objects',None)
        measured.update(dataset_count=observed['dataset_count'],incomplete_datasets=len(observed['errors']),
                        layout_observed_at_utc=observed['observed_at_utc'])
        write_private(output,measured)
        return 'completed'


def utc():
    return datetime.now(timezone.utc).isoformat()


def read_small(path):
    if path.is_symlink():
        raise ValueError("redirected_metadata")
    with path.open("rb") as stream:
        raw = stream.read(8 * 1024 * 1024 + 1)
    if len(raw) > 8 * 1024 * 1024:
        raise ValueError("metadata_too_large")
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError("metadata_not_object")
    return value


def write_private(path, value):
    atomic_write_bytes(path, json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode(), mode=0o600)


def observe(catalog, status, output, progress_path, *, force=False):
    """One owner at a time; an unchanged identity reuses its bounded result."""
    output.parent.mkdir(parents=True, exist_ok=True)
    lock_path = output.with_suffix(".lock")
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    with os.fdopen(fd, "a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return "already_running"
        before = read_small(status)
        identity = before.get("catalog_identity_sha256")
        expected_files, expected_bytes = before.get("available_file_count"), before.get("available_bytes")
        if type(expected_files) is not int or type(expected_bytes) is not int:
            raise ValueError("catalog_owner_counts_unknown")
        try:
            cached = read_small(output)
        except (OSError, ValueError):
            cached = {}
        if (not force and cached.get("contract") == CONTRACT and cached.get("classifier_revision") == CLASSIFIER_REVISION
                and cached.get("catalog_identity_sha256") == identity
                and cached.get("total_files") == expected_files and cached.get("total_bytes") == expected_bytes):
            write_private(progress_path, {"contract": CONTRACT, "state": "current", "observed_at_utc": utc(),
                "catalog_observed_at_utc": before.get("catalog_observed_at_utc")})
            return "current"
        started = time.monotonic()
        last = 0.0
        def progress(stream, size):
            nonlocal last
            now = time.monotonic()
            if now - last < 5:
                return
            last = now
            processed = min(stream.tell(), max(0, size - 1))
            elapsed = now - started
            remaining = ((size - processed) * elapsed / processed) if processed > 0 and elapsed >= 3 else None
            eta = (datetime.now(timezone.utc) + timedelta(seconds=remaining)).isoformat() if remaining is not None else None
            write_private(progress_path, {"contract": CONTRACT, "state": "scanning", "observed_at_utc": utc(),
                "started_at_utc": (datetime.now(timezone.utc) - timedelta(seconds=elapsed)).isoformat(),
                "processed_bytes": processed, "total_bytes": size, "completion_at_utc": eta,
                "scope": "catalog_classification_only"})
        try:
            value = classify(catalog, expected_identity=identity, expected_files=expected_files,
                             expected_bytes=expected_bytes, progress=progress)
            after = read_small(status)
            if after.get("catalog_identity_sha256") != identity:
                raise ValueError("catalog_owner_changed")
            value["catalog_observed_at_utc"] = after.get("catalog_observed_at_utc")
            value["duration_seconds"] = round(time.monotonic() - started, 3)
            write_private(output, value)
            write_private(progress_path, {"contract": CONTRACT, "state": "current", "observed_at_utc": utc(),
                "catalog_observed_at_utc": after.get("catalog_observed_at_utc")})
            return "completed"
        except Exception as exc:
            write_private(progress_path, {"contract": CONTRACT, "state": "failed", "observed_at_utc": utc(),
                "error_code": type(exc).__name__, "scope": "catalog_classification_only"})
            raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    output = service_path("data_monitor/d_backup_inventory.json", root=ROOT)
    state = observe(Path("/var/lib/stockagent/backup-stream/catalog.json"),
                    Path("/var/lib/stockagent/backup-stream/status.json"), output,
                    output.with_name("d_backup_inventory_progress.json"), force=args.force)
    from scripts.run_bounded_storage_maintenance import TradingPriorityGuard, GUARD_ENV
    previous=os.environ.get(GUARD_ENV)
    os.environ[GUARD_ENV]='1'
    try:
        try:
            layout_state=observe_layout(output.with_name('d_pack_layout.json'),force=args.force,
                                        guard=TradingPriorityGuard().check)
        except (OSError,ValueError,RuntimeError) as error:
            # Keep the last measured generation; a busy or protected scan
            # cannot become an empty inventory or a failure of the old catalog.
            layout_state='deferred_'+type(error).__name__
    finally:
        if previous is None:
            os.environ.pop(GUARD_ENV,None)
        else:
            os.environ[GUARD_ENV]=previous
    print(json.dumps({"state": state, "layout_state":layout_state,"observed_at_utc": utc()}))


if __name__ == "__main__":
    main()
