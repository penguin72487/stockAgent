"""Release observations from existing validated downloads; never a poller.

Network clocks, data dates and provider publication clocks are different. A
changed response only brackets availability between the previous request start
and this response completion. No historical receipt is backdated or promoted.
"""
from __future__ import annotations

from contextlib import closing
from datetime import UTC, date, datetime, timedelta
import hashlib
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from typing import Any
import uuid

CONTRACT_VERSION = 1
DB_NAME = "update_observations.sqlite3"
PUBLIC_FIELDS = frozenset({
    'checked_at_utc', 'last_data_observed_at_utc', 'last_changed_at_utc',
    'availability_after_utc', 'availability_by_utc', 'source_first_date', 'source_last_date',
    'rows', 'response_rows', 'network_seconds', 'event', 'checks', 'changes', 'unchanged',
    'failed_checks', 'last_failed_check_at_utc', 'last_error_code', 'next_check_at_utc',
    'publication_basis',
})


class ResponseRows(list):
    """Keep transport evidence across validation, merging and batch fan-out."""

    def __init__(self, rows, *, observation: dict):
        super().__init__(rows)
        self.observation = observation


def received_rows(rows: list, started: datetime) -> ResponseRows:
    completed = datetime.now(UTC)
    return ResponseRows(rows, observation={
        "request_id": uuid.uuid4().hex,
        "request_started_at_utc": started.astimezone(UTC).isoformat(),
        "response_received_at_utc": completed.isoformat(),
        "response_rows": len(rows),
        "network_seconds": max(0, (completed - started).total_seconds()),
    })


def retain_observation(rows: list, original: list) -> list:
    evidence = getattr(original, "observation", None)
    return ResponseRows(rows, observation=evidence) if evidence else rows


def row_fingerprint(rows: list[dict]) -> str:
    """Order-independent, duplicate-sensitive fingerprint, O(payload bytes).

    Do not sort rows, reread historical files or hash a second Parquet copy.
    Hash canonical fields in each row; accumulate 256-bit hashes as a multiset.
    Row count disambiguates additions of repeated rows. Values remain unchanged.
    """
    total = 0
    for row in rows:
        raw = json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
        total = (total + int.from_bytes(hashlib.sha256(raw).digest(), "big")) % (1 << 256)
    return hashlib.sha256(f"{len(rows)}:{total:064x}".encode()).hexdigest()


def monitored(task: Any, now: datetime) -> bool:
    if task.kind == "derived":
        return False
    if task.kind in {"snapshot", "id_history"} or task.priority == 0:
        return True
    from downloader.finmind_scheduling import TAIPEI
    local = now.astimezone(TAIPEI).date()
    try:
        if len(task.partition) == 4:
            return int(task.partition) == local.year
        day = date.fromisoformat(task.partition)
    except (ValueError, TypeError):
        return False
    return local - timedelta(days=7) <= day <= local


def _schema(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS streams (
            dataset TEXT, data_id TEXT, partition TEXT, fingerprint TEXT,
            rows INTEGER, source_first_date TEXT, source_last_date TEXT,
            checked_at_utc TEXT, request_started_at_utc TEXT,
            first_observed_at_utc TEXT, last_changed_at_utc TEXT,
            unchanged_checks INTEGER, next_check_at_utc TEXT,
            PRIMARY KEY(dataset,data_id,partition));
        CREATE TABLE IF NOT EXISTS checks (
            id INTEGER PRIMARY KEY, request_id TEXT, dataset TEXT, data_id TEXT,
            partition TEXT, checked_at_utc TEXT, kind TEXT, metadata_json TEXT,
            UNIQUE(request_id,dataset,data_id,partition));
        CREATE INDEX IF NOT EXISTS idx_updates_dataset_checked
            ON checks(dataset,checked_at_utc);
        CREATE TABLE IF NOT EXISTS dataset_heads (
            dataset TEXT PRIMARY KEY, metadata_json TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS idx_updates_stream_due
            ON streams(dataset,next_check_at_utc);
    """)


def record_success(root: Path, task: Any, rows: list, receipt: dict,
                   now: datetime, *, transport: dict | None = None,
                   fingerprint: str | None = None,
                   fingerprint_basis: str = 'canonical_row_multiset_v1') -> dict | None:
    if not monitored(task, now):
        return None
    transport = transport or getattr(rows, "observation", {})
    checked = transport.get("response_received_at_utc") or now.astimezone(UTC).isoformat()
    started = transport.get("request_started_at_utc")
    request_id = transport.get("request_id") or uuid.uuid4().hex
    fingerprint = fingerprint or row_fingerprint(rows)
    first = receipt.get("source_first_date") or receipt.get("date")
    last = receipt.get("source_last_date") or receipt.get("date")
    root.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(root / DB_NAME, timeout=5)) as conn, conn:
        _schema(conn)
        existing = conn.execute(
            "SELECT metadata_json FROM checks WHERE request_id=? AND dataset=? AND data_id=? AND partition=?",
            (request_id, task.dataset, task.data_id, task.partition)).fetchone()
        if existing:
            return json.loads(existing[0])
        conn.row_factory = sqlite3.Row
        previous = conn.execute("SELECT * FROM streams WHERE dataset=? AND data_id=? AND partition=?",
                                (task.dataset, task.data_id, task.partition)).fetchone()
        if previous and previous["checked_at_utc"] > checked:
            raise ValueError("update_observation_clock_regression")
        changed = previous is not None and previous["fingerprint"] != fingerprint
        kind = ("first_nonempty" if rows else "first_empty") if previous is None else (
            "unchanged" if not changed else "first_nonempty_after_empty" if not previous["rows"] and rows else
            "coverage_changed" if not rows or (last or "") < (previous["source_last_date"] or "") else
            "new_data" if (last or "") > (previous["source_last_date"] or "") else "revision")
        unchanged = (previous["unchanged_checks"] + 1) if previous and not changed else 0
        changed_at = checked if changed else (previous["last_changed_at_utc"] if previous else None)
        first_seen = previous["first_observed_at_utc"] if previous else checked
        evidence = {
            "contract_version": CONTRACT_VERSION, "event": kind,
            "checked_at_utc": checked, "request_started_at_utc": started,
            "first_observed_at_utc": first_seen, "last_changed_at_utc": changed_at,
            "availability_after_utc": previous["request_started_at_utc"] if changed else None,
            "availability_by_utc": checked if changed else None,
            "provider_published_at_utc": None,
            "publication_basis": "observed_interval_not_exact_publication",
            "source_first_date": first, "source_last_date": last,
            "rows": len(rows), "fingerprint": fingerprint,
            "fingerprint_basis": fingerprint_basis, "unchanged_checks": unchanged,
            "stored_content_sha256": receipt.get("sha256"),
            "response_rows": transport.get("response_rows", len(rows)),
            "network_seconds": transport.get("network_seconds"),
            "request_id": request_id,
        }
        # Commit comparisons and the check together; a retry of the same batch
        # cannot manufacture another update or advance the observation clock.
        conn.execute("INSERT INTO checks(request_id,dataset,data_id,partition,checked_at_utc,kind,metadata_json) "
                     "VALUES (?,?,?,?,?,?,?)", (request_id, task.dataset, task.data_id, task.partition,
                                               checked, kind, json.dumps(evidence, sort_keys=True)))
        conn.execute("INSERT INTO streams VALUES (?,?,?,?,?,?,?,?,?,?,?,?,NULL) "
                     "ON CONFLICT(dataset,data_id,partition) DO UPDATE SET "
                     "fingerprint=excluded.fingerprint,rows=excluded.rows,source_first_date=excluded.source_first_date,"
                     "source_last_date=excluded.source_last_date,checked_at_utc=excluded.checked_at_utc,"
                     "request_started_at_utc=excluded.request_started_at_utc,last_changed_at_utc=excluded.last_changed_at_utc,"
                     "unchanged_checks=excluded.unchanged_checks",
                     (task.dataset, task.data_id, task.partition, fingerprint, len(rows), first, last,
                      checked, started, first_seen, changed_at, unchanged))
        head_row = conn.execute("SELECT metadata_json FROM dataset_heads WHERE dataset=?", (task.dataset,)).fetchone()
        head = json.loads(head_row[0]) if head_row else {"checks": 0, "changes": 0, "unchanged": 0}
        head.update(checks=head["checks"] + 1, changes=head["changes"] + int(changed),
                    unchanged=head["unchanged"] + int(kind == "unchanged"))
        if checked >= head.get("checked_at_utc", ""):
            head.update({key: evidence[key] for key in (
                "checked_at_utc", "event", "source_first_date", "source_last_date", "rows", "response_rows",
                "network_seconds", "publication_basis")})
            if checked >= head.get("last_failed_check_at_utc", ""):
                head["last_error_code"] = None
        if rows and (previous is None or changed) and checked >= head.get("last_data_observed_at_utc", ""):
            head.update(last_data_observed_at_utc=checked,
                        availability_after_utc=evidence["availability_after_utc"],
                        availability_by_utc=evidence["availability_by_utc"])
        if changed:
            head["last_changed_at_utc"] = max(checked, head.get("last_changed_at_utc") or checked)
        conn.execute("INSERT INTO dataset_heads VALUES (?,?) ON CONFLICT(dataset) DO UPDATE SET metadata_json=excluded.metadata_json",
                     (task.dataset, json.dumps(head, sort_keys=True)))
    return evidence


def set_next_check(root: Path, task: Any, due: str | None) -> None:
    if not (root / DB_NAME).is_file():
        return
    with closing(sqlite3.connect(root / DB_NAME, timeout=5)) as conn, conn:
        conn.execute("UPDATE streams SET next_check_at_utc=? WHERE dataset=? AND data_id=? AND partition=?",
                     (due, task.dataset, task.data_id, task.partition))


def record_failure(root: Path, task: Any, code: str, now: datetime) -> None:
    """A failed check cannot erase the last validated fingerprint/update time."""
    if not monitored(task, now):
        return
    root.mkdir(parents=True, exist_ok=True)
    checked = now.astimezone(UTC).isoformat()
    with closing(sqlite3.connect(root / DB_NAME, timeout=5)) as conn, conn:
        _schema(conn)
        evidence = {'contract_version': CONTRACT_VERSION, 'event': 'failed',
                    'checked_at_utc': checked, 'error_code': code}
        conn.execute('INSERT INTO checks(request_id,dataset,data_id,partition,checked_at_utc,kind,metadata_json) '
                     'VALUES (?,?,?,?,?,?,?)', (uuid.uuid4().hex, task.dataset, task.data_id, task.partition,
                                               checked, 'failed', json.dumps(evidence)))
        row = conn.execute('SELECT metadata_json FROM dataset_heads WHERE dataset=?', (task.dataset,)).fetchone()
        head = json.loads(row[0]) if row else {'checks': 0, 'changes': 0, 'unchanged': 0}
        head['failed_checks'] = head.get('failed_checks', 0) + 1
        if checked >= head.get('last_failed_check_at_utc', ''):
            head['last_failed_check_at_utc'] = checked
            if checked >= head.get('checked_at_utc', ''):
                head['last_error_code'] = code
        conn.execute('INSERT INTO dataset_heads VALUES (?,?) ON CONFLICT(dataset) '
                     'DO UPDATE SET metadata_json=excluded.metadata_json', (task.dataset, json.dumps(head)))


def empty_retry(now: datetime, observation: dict | None, *, default_seconds: int) -> datetime:
    """Fast initial detection, then bounded backoff for proven unchanged empties.

    This never postpones the initial official release boundary, and only
    changes retries following an actual validated empty response.
    """
    if not observation:
        return now + timedelta(seconds=default_seconds)
    count = min(int(observation.get("unchanged_checks", 0)), 8)
    return now + timedelta(seconds=min(default_seconds, 60 * (2 ** count)))


def free_task(dataset: str, partition: str, *, kind: str = "day") -> Any:
    return SimpleNamespace(dataset=dataset, data_id="", partition=partition, kind=kind, priority=0)


def read_summary(root: Path) -> dict:
    """Bounded read-only provider metadata; no raw rows, paths or API calls."""
    if not (root / DB_NAME).is_file():
        return {"state": "not_observed", "datasets": {}}
    try:
        with closing(sqlite3.connect((root / DB_NAME).resolve().as_uri() + "?mode=ro", uri=True, timeout=.2)) as conn:
            heads = {dataset: {key: value for key, value in json.loads(raw).items() if key in PUBLIC_FIELDS}
                     for dataset, raw in conn.execute("SELECT dataset,metadata_json FROM dataset_heads")}
            next_due = dict(conn.execute("SELECT dataset,min(next_check_at_utc) FROM streams "
                                         "WHERE next_check_at_utc IS NOT NULL GROUP BY dataset"))
        for dataset, head in heads.items():
            head["next_check_at_utc"] = next_due.get(dataset)
        return {"state": "observed", "datasets": heads}
    except (sqlite3.Error, ValueError, TypeError, AttributeError):
        return {"state": "unreadable", "datasets": {}}


def queued_next_checks(root: Path, now: datetime) -> dict[str, str | None]:
    """Read actual queue deadlines, not obsolete deadlines from old receipts."""
    path = root / 'queue.sqlite3'
    if not path.is_file():
        return {}
    try:
        with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=.2)) as conn:
            return {dataset: (now.isoformat() if pending else due) for dataset, due, pending in conn.execute(
                "SELECT dataset,min(next_attempt_at_utc),"
                "sum(state='pending' AND next_attempt_at_utc IS NULL) FROM tasks "
                "WHERE kind!='derived' AND priority=0 AND state IN ('pending','complete','observed_empty','failed') "
                "GROUP BY dataset")}
    except sqlite3.Error:
        return {}
