#!/usr/bin/env python3
"""Schedule only the selected paper model's FinLab inputs through its owner.

The source projection remains the existing completed-session producer.  This
coordinator neither implements a second downloader nor changes its generator.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import UTC, datetime, time as datetime_time, timedelta
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json  # noqa: E402
from downloader.provider_priority import register  # noqa: E402
from stockagent.data.finlab_acquisition_contract import safe_stem, source_check_due  # noqa: E402
from stockagent.data.finlab_gap_priority import DAYTRADE_PRIORITY_REASON, pending_gap_requests  # noqa: E402
from stockagent.live.tw_public_opening_revision import active_opening_revision_freeze  # noqa: E402

PARENT = ROOT / "artifacts/data_preparation/tw_factorized_fold11_live_parent_20261013/factorized_manifest.json"
FINLAB = ROOT / "data_finlab"
PROJECTION = ROOT / "artifacts/data_preparation/tw_factorized_scale_cash_live_v1"
PUBLIC = Path("/srv/stockagent-live/data_tw_public")
STATE = Path("/var/lib/stockagent/daytrade-finlab-priority")
TAIPEI = ZoneInfo("Asia/Taipei")


def _stat_identity(path: Path) -> list[int]:
    info = path.stat()
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns]


def _json(path: Path) -> dict:
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


@contextmanager
def _lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def selected_datasets(parent_path: Path, cache_path: Path) -> tuple[str, list[str]]:
    """Cache only a verified dictionary selection; no provider calls or data scan."""
    parent_bytes = parent_path.read_bytes()
    parent_sha = hashlib.sha256(parent_bytes).hexdigest()
    parent = json.loads(parent_bytes)
    expected = parent.get("feature_dictionary_sha256")
    dictionary = parent_path.parent / "feature_dictionary.json"
    before = _stat_identity(dictionary)
    try:
        cache = _json(cache_path)
    except (OSError, ValueError):
        cache = {}
    keys = cache.get("datasets")
    if (cache.get("parent_sha256") == parent_sha and cache.get("dictionary_sha256") == expected
            and cache.get("dictionary_stat") == before and isinstance(keys, list)
            and keys == sorted(set(keys)) and all(isinstance(k, str) and k for k in keys)):
        return parent_sha, keys
    raw = dictionary.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected or _stat_identity(dictionary) != before:
        raise ValueError("selected factorized dictionary changed or failed its parent SHA")
    definitions = json.loads(raw).get("features")
    if not isinstance(definitions, list):
        raise ValueError("selected factorized dictionary has no features")
    by_name = {d["feature"]: d for d in definitions}
    if len(by_name) != len(definitions):
        raise ValueError("selected factorized dictionary repeats a feature")
    names = parent["individual_channels"] + parent["common_channels"]
    selected = [by_name[name] for name in names]
    keys = sorted({d["dataset"] for d in selected if d.get("source") == "FinLab"})
    if not keys or any(not isinstance(k, str) or not k for k in keys):
        raise ValueError("selected factorized FinLab dependencies are absent or invalid")
    cache_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(cache_path, {"parent_sha256": parent_sha, "dictionary_sha256": expected,
                                  "dictionary_stat": before, "datasets": keys})
    return parent_sha, keys


def _local_receipt(root: Path, key: str) -> dict:
    try:
        receipt = _json(root / "receipts" / (safe_stem(key) + ".json"))
        rel = Path(str(receipt.get("parquet_path") or ""))
        source = root / rel
        digest = receipt.get("sha256")
        if (receipt.get("dataset") != key or rel.is_absolute() or ".." in rel.parts
                or rel.parts[:1] != ("datasets",) or not source.is_file()
                or not source.resolve(strict=True).is_relative_to((root / "datasets").resolve(strict=True))
                or not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)
                or source.stat().st_size != receipt.get("parquet_size_bytes")):
            return {}
        return receipt
    except (OSError, ValueError):
        return {}


def schedule_due(root: Path, keys: list[str], *, now: datetime) -> list[str]:
    catalog = set(_json(root / "catalog/discovery.json")["keys"])
    if not set(keys).issubset(catalog):
        raise ValueError("selected model has FinLab keys absent from the existing catalog")
    active = pending_gap_requests(root, now=now, available=catalog)
    due = [key for key in keys if source_check_due(_local_receipt(root, key), now=now, days=1)]
    new = [key for key in due if key not in active]
    for offset in range(0, len(new), 64):
        plan = {"contract": "provider_priority_query_v1", "provider": "finlab",
                "reason": DAYTRADE_PRIORITY_REASON,
                "created_at_utc": now.isoformat(), "expires_at_utc": (now + timedelta(hours=23)).isoformat(),
                "fresh_after_utc": now.isoformat(), "items": new[offset:offset + 64]}
        register(root, plan, now=now)
    active = pending_gap_requests(root, now=now, available=catalog)
    return [key for key in keys if key in active]


def projection_needs_update(root: Path, public: Path, finlab: Path,
                            parent_sha: str, keys: list[str], *,
                            protect_ready_readers: bool = False) -> tuple[bool, str]:
    pointer = root / "current/factorized_manifest.json"
    if not pointer.is_file():
        return True, "projection_missing"
    path = pointer.resolve(strict=True)
    if not path.is_relative_to((root / "generations").resolve(strict=True)):
        raise ValueError("projection pointer escapes its canonical generations")
    manifest = _json(path)
    paper = manifest.get("paper_inference") or {}
    identity = paper.get("source_identity") or {}
    heads = identity.get("finlab") or {}
    if manifest.get("status") != "complete" or paper.get("status") != "verified":
        raise ValueError("projection is not an accepted generation")
    if identity.get("parent") != parent_sha or set(heads) != set(keys):
        return True, "selected_parent_or_source_scope_changed"
    summary = _json(public / "features/tw_public_stock_daily.summary.json")
    if paper.get("completed_session") != summary.get("requested_end_date"):
        # A missing completed session must not be hidden by a changed raw
        # source head; this is the urgent build needed for today's model.
        return True, "completed_session_changed"
    if protect_ready_readers and _daytrade_reader_hold(
            public, decision_session=str(paper.get("decision_session") or ""), observed=datetime.now(UTC)):
        # The timer will retry after the reader hold. Do not stat hundreds of
        # source objects every 15 seconds when only an optional head update
        # could differ; urgent missing-session/scope checks ran above.
        return False, "daytrade_reader_priority"
    changed = False
    for key in keys:
        receipt = _local_receipt(finlab, key)
        if not receipt:
            return False, "source_receipt_or_object_missing"
        if receipt.get("sha256") != heads[key]:
            changed = True
    if changed:
        return True, "finlab_source_head_changed"
    return False, "source_heads_current"


def _daytrade_reader_hold(public: Path, *, decision_session: str, observed: datetime) -> dict:
    """Keep optional head-only rebuilds away from today's live readers.

    The verified projection's decision session is the calendar evidence: it
    also handles exceptional trading days. Do not prevent a missing-session
    projection from being built. Raw source acquisition continues separately.
    """
    local = observed.astimezone(TAIPEI)
    if decision_session != local.date().isoformat():
        return {}
    freeze = active_opening_revision_freeze(public, observed=observed)
    if datetime_time(8, 19) <= local.time().replace(tzinfo=None) < datetime_time(13, 35):
        return {"session_date": decision_session,
                "resume_after": local.replace(hour=13, minute=35, second=0, microsecond=0).isoformat()}
    if freeze and freeze.get("session_date") == decision_session:
        return {"session_date": decision_session, "resume_after": freeze.get("defer_apply_until_taipei")}
    return {}


def _run_projection(command: list[str], *, reason: str, decision_session: str) -> int:
    if reason != "finlab_source_head_changed":
        return subprocess.run(command, cwd=ROOT, check=False).returncode
    # Only this coordinator's optional child is supervised. A rebuild started
    # before the reader hold must not cross it and alter shared inode ctimes.
    child = subprocess.Popen(command, cwd=ROOT, start_new_session=True)
    while True:
        try:
            return child.wait(timeout=2)
        except subprocess.TimeoutExpired:
            hold = _daytrade_reader_hold(PUBLIC, decision_session=decision_session, observed=datetime.now(UTC))
            if not hold:
                continue
            try:
                os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                child.wait()
            print(json.dumps({"state": "daytrade_reader_preempted_optional_refresh", **hold}), flush=True)
            return 75


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("sync", "reconcile"))
    args = parser.parse_args(argv)
    STATE.mkdir(parents=True, exist_ok=True, mode=0o700)
    # Registration uses the existing priority registry's own lock. It must
    # not wait for the account lock: a running history owner needs to see the
    # request before it can hand over at a safe dataset boundary.
    own_lock = STATE / ("registration.lock" if args.mode == "sync" else "reconcile.lock")
    with _lock(own_lock) as acquired:
        if not acquired:
            print(json.dumps({"state": "waiting_existing_owner", "mode": args.mode}))
            return 75
        if args.mode == "reconcile":
            # Avoid even reading the selection manifest while the existing
            # source owner is building a generation. Recheck after selection
            # because it may start in the meantime.
            with _lock(PROJECTION / "prepare.lock") as producer_idle:
                if not producer_idle:
                    print(json.dumps({"state": "waiting_existing_projection_owner"}))
                    return 75
        # Serialize the expensive one-time dictionary selection across modes.
        with _lock(STATE / "selection.lock") as selection_acquired:
            if not selection_acquired:
                print(json.dumps({"state": "waiting_selection", "mode": args.mode}))
                return 75
            parent_sha, keys = selected_datasets(PARENT, STATE / "selected.json")
        if args.mode == "sync":
            pending = schedule_due(FINLAB, keys, now=datetime.now(UTC))
            if not pending:
                print(json.dumps({"state": "no_due_priority", "datasets": len(keys)}))
                return 0
            with _lock(FINLAB / ".sync.lock") as account_acquired:
                if not account_acquired:
                    print(json.dumps({"state": "priority_registered_waiting_account_owner", "pending": len(pending)}))
                    return 75
                # A preceding owner may have serviced the registered requests
                # before releasing the account. Never dispatch stale intents.
                active = pending_gap_requests(FINLAB, now=datetime.now(UTC), available=keys)
                pending = [key for key in pending if key in active]
                if not pending:
                    print(json.dumps({"state": "priority_satisfied_by_previous_owner"}))
                    return 0
                command = [sys.executable, str(ROOT / "scripts/download_finlab_history.py"), "sync",
                           "--priority-only", "--limit", str(len(pending)), "--refresh-days", "1",
                           "--min-quota-remaining-mb", "0", "--key-timeout-seconds", "600"]
                for key in pending:
                    command.extend(["--dataset", key])
                return subprocess.run(command, cwd=ROOT, check=False).returncode
        # The producer holds this lock while it materializes an immutable
        # generation. Do not launch duplicate work or preempt another window.
        with _lock(PROJECTION / "prepare.lock") as producer_idle:
            if not producer_idle:
                print(json.dumps({"state": "waiting_existing_projection_owner"}))
                return 75
            needed, reason = projection_needs_update(
                PROJECTION, PUBLIC, FINLAB, parent_sha, keys, protect_ready_readers=True)
        if not needed:
            print(json.dumps({"state": reason, "datasets": len(keys)}))
            return 75 if reason in {"source_receipt_or_object_missing", "daytrade_reader_priority"} else 0
        decision_session = ""
        if reason == "finlab_source_head_changed":
            paper = _json((PROJECTION / "current/factorized_manifest.json").resolve(strict=True)).get("paper_inference") or {}
            decision_session = str(paper.get("decision_session") or "")
            hold = _daytrade_reader_hold(PUBLIC, decision_session=decision_session, observed=datetime.now(UTC))
            if hold:
                # Hardlink reuse changes inode ctime even when bytes are
                # immutable. Do not disturb an already-ready daytrade reader;
                # raw source checks continue and the timer retries afterward.
                print(json.dumps({"state": "daytrade_reader_priority", **hold}))
                return 75
        print(json.dumps({"state": "projection_reconciliation_started", "reason": reason}), flush=True)
        # Reuse the existing source owner, which takes its own leases and
        # validates the complete immutable generation before publication.
        return _run_projection([sys.executable, str(ROOT / "scripts/update_tw_day_trade_factorized_live.py"),
                                "--phase", "completed", "--source-wait-seconds", "1800"],
                               reason=reason, decision_session=decision_session)


if __name__ == "__main__":
    raise SystemExit(main())
