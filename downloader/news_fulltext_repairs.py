"""Reviewed missing-body work, admitted by the existing single queue writer.

Submission is read-only SQLite plus a private, immutable local request: no
queue lease, HTTP client or worker lifecycle. The resident owner rechecks the
exact failed head and current source contract before changing only its state.
SQL admission and its once-per-parser marker commit together; a crash before
archiving the request cannot grant a second attempt. Admission is not capture.
"""
from __future__ import annotations

from collections import Counter
from contextlib import closing
from datetime import datetime
import hashlib
from itertools import islice
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import time
from urllib.parse import urlsplit

from downloader.artifact_io import atomic_write_bytes
from downloader.news_fulltext_policy import NewsError, normalize_url
from downloader.news_fulltext_queue import parser_repair_identity, utc_now

CONTRACT_VERSION = 1
MAX_TARGETS = 512
MAX_PENDING = 64
MAX_REQUEST_BYTES = 1 << 20
PUMP_INTERVAL_SECONDS = 5
REPAIRABLE = frozenset({"body_not_found", "article_identity_not_verified",
                       "article_attribution_not_verified", "article_title_not_verified"})
HEAD_FIELDS = ("id", "url", "provider", "state", "error_code", "attempts", "due", "last_checked_at_utc")
SOURCE_FIELDS = {"contract_version", "data_fingerprint", "parser_fingerprint", "authorization_fingerprint"}


def _bytes(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _clock(value):
    if not isinstance(value, str) or not 1 <= len(value) <= 64:
        raise ValueError("invalid_repair_clock")
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise ValueError("naive_repair_clock")
    return stamp.timestamp()


def _head(row):
    return {key: bytes(row[key]).hex() if key == "id" else row[key] for key in HEAD_FIELDS}


def _head_problem(row, now):
    if row is None:
        return "unknown_url"
    if row["body_sha256"] is not None or row["parquet_path"] is not None or row["body_bytes"] != 0 or row["captured_at_utc"] is not None:
        return "stored_head_protected"
    if row["lease_owner"] is not None or row["lease_until"] is not None:
        return "leased_head_protected"
    if row["state"] not in REPAIRABLE or row["error_code"] != row["state"]:
        return "not_semantic_parser_failure"
    if type(row["attempts"]) is not int or row["attempts"] < 0:
        return "invalid_attempts"
    if type(row["due"]) not in (int, float) or not math.isfinite(row["due"]) or row["due"] < 0 or row["due"] > now:
        return "future_or_invalid_deadline"
    try:
        if _clock(row["last_checked_at_utc"]) > now:
            return "future_check_protected"
    except (ValueError, TypeError, OverflowError):
        return "missing_or_invalid_check"
    return None


def _source(db, registry, url):
    source = registry.require(url)
    if source.article_path_pattern and not re.search(source.article_path_pattern, urlsplit(url).path):
        raise ValueError("unsupported_article_path")
    if source.acquisition_mode == "finite":
        original = db.execute("SELECT 1 FROM finite_cohort_members WHERE provider=? AND url=?",
                              (source.provider, url)).fetchone()
        supplement = source.allow_reviewed_supplements and db.execute(
            "SELECT 1 FROM finite_supplement_members WHERE provider=? AND url=?", (source.provider, url)).fetchone()
        if not original and not supplement:
            raise ValueError("outside_frozen_cohort")
    identity = parser_repair_identity(source)
    if identity is None:
        raise ValueError("parser_generation_unattested")
    return source, dict(identity, authorization_fingerprint=source.authorization_fingerprint)


def _directory(root, *parts):
    target = root
    for part in ("reviewed_repairs", *parts):
        target = target / part
        if target.is_symlink():
            raise ValueError("repair_directory_symlink")
        target.mkdir(mode=0o700, exist_ok=True)
        if not target.is_dir() or target.stat().st_mode & 0o077:
            raise ValueError("repair_directory_not_private")
    return target


def _write_once(path, payload):
    if path.is_symlink():
        raise ValueError("repair_receipt_symlink")
    if path.exists():
        if path.stat().st_size > MAX_REQUEST_BYTES or path.read_bytes() != payload:
            raise ValueError("repair_evidence_conflict")
        return
    atomic_write_bytes(path, payload, durable=True, mode=0o600)


def _pending(directory):
    # Bounded directory discovery, not a scan of the article or history trees.
    with os.scandir(directory) as entries:
        return sorted((Path(entry.path) for entry in islice(
            (entry for entry in entries if entry.name.endswith(".json")), MAX_PENDING + 1)), key=lambda p: p.name)


def submit_repairs(root, registry, urls, *, reason):
    """Seal explicit eligible failures while the canonical owner stays alive."""
    if not isinstance(reason, str) or not reason.strip() or len(reason) > 240:
        raise ValueError("repair_requires_bounded_review_reason")
    selected = list(islice(urls, MAX_TARGETS + 1))
    if not 1 <= len(selected) <= MAX_TARGETS:
        raise ValueError("repair_requires_one_to_512_urls")
    selected = list(dict.fromkeys(normalize_url(url) for url in selected))
    root, now = Path(root).resolve(), time.time()
    targets, sources, excluded = [], {}, []
    with closing(sqlite3.connect((root / "queue.sqlite3").as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute("BEGIN")
        has_markers = db.execute("SELECT 1 FROM sqlite_master WHERE name='reviewed_repair_items' AND type='table'").fetchone()
        for url in selected:
            row = db.execute("SELECT * FROM articles WHERE url=?", (url,)).fetchone()
            problem = _head_problem(row, now)
            if problem is None:
                try:
                    source, identity = _source(db, registry, url)
                    if source.provider != row["provider"]:
                        raise ValueError("provider_changed")
                    if has_markers and db.execute(
                            "SELECT 1 FROM reviewed_repair_items WHERE provider=? AND parser_fingerprint=? "
                            "AND article_id=? AND outcome='enqueued'", (source.provider, identity["parser_fingerprint"], row["id"])).fetchone():
                        raise ValueError("already_admitted_for_parser")
                except (ValueError, NewsError) as error:
                    problem = error.code if isinstance(error, NewsError) else str(error)
            if problem:
                excluded.append({"url": url, "reason": problem})
            else:
                targets.append(_head(row))
                sources[source.provider] = identity
    result = {"contract_version": CONTRACT_VERSION, "submitted_targets": len(targets), "excluded": excluded,
              "publisher_requests": 0, "queue_mutations": False, "accepted_as_bodies": False}
    if not targets:
        return dict(result, submitted=False, request_id=None)
    request = {"contract_version": CONTRACT_VERSION, "kind": "reviewed_missing_body_repair",
               "created_at_utc": utc_now(), "reason": reason.strip(), "sources": sources, "targets": targets}
    payload = _bytes(request)
    if len(payload) > MAX_REQUEST_BYTES:
        raise ValueError("repair_request_too_large")
    directory = _directory(root, "pending")
    if len(_pending(directory)) >= MAX_PENDING:
        raise ValueError("repair_inbox_full")
    digest = hashlib.sha256(payload).hexdigest()
    path = directory / (digest + ".json")
    _write_once(path, payload)
    return dict(result, submitted=True, request_id=digest, request_path=str(path))


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_repair_json_key")
        result[key] = value
    return result


def _load(path):
    with os.fdopen(os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)), "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_REQUEST_BYTES or info.st_mode & 0o077:
            raise ValueError("repair_input_not_bounded_private_file")
        payload = stream.read(MAX_REQUEST_BYTES + 1)
    if len(payload) > MAX_REQUEST_BYTES or hashlib.sha256(payload).hexdigest() != path.stem:
        raise ValueError("repair_input_digest_mismatch")
    request = json.loads(payload, object_pairs_hook=_unique)
    if (not isinstance(request, dict) or set(request) != {"contract_version", "kind", "created_at_utc", "reason", "sources", "targets"}
            or type(request["contract_version"]) is not int or request["contract_version"] != CONTRACT_VERSION
            or request["kind"] != "reviewed_missing_body_repair"
            or not isinstance(request["reason"], str) or not request["reason"].strip() or len(request["reason"]) > 240
            or _clock(request["created_at_utc"]) > time.time()
            or not isinstance(request["targets"], list) or not 1 <= len(request["targets"]) <= MAX_TARGETS
            or not isinstance(request["sources"], dict)):
        raise ValueError("invalid_repair_request")
    seen = set()
    for target in request["targets"]:
        if (not isinstance(target, dict) or set(target) != set(HEAD_FIELDS)
                or not isinstance(target["url"], str) or normalize_url(target["url"]) != target["url"]
                or target["id"] != hashlib.sha256(target["url"].encode()).hexdigest()
                or target["id"] in seen or not isinstance(target["provider"], str)
                or not isinstance(target["state"], str) or target["state"] not in REPAIRABLE
                or target["error_code"] != target["state"]
                or type(target["attempts"]) is not int or target["attempts"] < 0
                or type(target["due"]) not in (int, float) or not math.isfinite(target["due"]) or target["due"] < 0
                or target["due"] > _clock(request["created_at_utc"])
                or _clock(target["last_checked_at_utc"]) > _clock(request["created_at_utc"])):
            raise ValueError("invalid_repair_target")
        seen.add(target["id"])
    if set(request["sources"]) != {target["provider"] for target in request["targets"]}:
        raise ValueError("invalid_repair_sources")
    for identity in request["sources"].values():
        if (not isinstance(identity, dict) or set(identity) != SOURCE_FIELDS
                or type(identity["contract_version"]) is not int or identity["contract_version"] != 1
                or any(not isinstance(identity[key], str) or not re.fullmatch(r"[0-9a-f]{64}", identity[key])
                       for key in SOURCE_FIELDS - {"contract_version"})):
            raise ValueError("invalid_repair_generation")
    return request


def _schema(queue):
    if getattr(queue, "reviewed_repair_schema_ready", False):
        return
    with queue.db:
        queue.db.execute("CREATE TABLE IF NOT EXISTS reviewed_repair_jobs("
                         "request_id TEXT PRIMARY KEY,targets INTEGER NOT NULL,next_index INTEGER NOT NULL,"
                         "created_at_utc TEXT NOT NULL,completed_at_utc TEXT,owner_id TEXT NOT NULL)")
        queue.db.execute("CREATE TABLE IF NOT EXISTS reviewed_repair_items("
                         "request_id TEXT NOT NULL,target_index INTEGER NOT NULL,article_id BLOB NOT NULL,"
                         "provider TEXT NOT NULL,parser_fingerprint TEXT NOT NULL,outcome TEXT NOT NULL,"
                         "recorded_at_utc TEXT NOT NULL,owner_id TEXT NOT NULL,PRIMARY KEY(request_id,target_index))")
        queue.db.execute("CREATE UNIQUE INDEX IF NOT EXISTS reviewed_repair_once ON reviewed_repair_items("
                         "provider,parser_fingerprint,article_id) WHERE outcome='enqueued'")
    queue.reviewed_repair_schema_ready = True


def _admit(queue, request, target):
    row = queue.db.execute("SELECT * FROM articles WHERE id=?", (bytes.fromhex(target["id"]),)).fetchone()
    problem = _head_problem(row, time.time())
    if problem:
        return problem
    if _head(row) != target:
        return "head_changed_since_review"
    try:
        source, identity = _source(queue.db, queue.registry, target["url"])
    except (NewsError, ValueError) as error:
        return error.code if isinstance(error, NewsError) else str(error)
    if source.provider != target["provider"] or identity != request["sources"][source.provider]:
        return "source_identity_changed"
    if queue.db.execute("SELECT 1 FROM reviewed_repair_items WHERE provider=? AND parser_fingerprint=? "
                        "AND article_id=? AND outcome='enqueued'",
                        (source.provider, identity["parser_fingerprint"], row["id"])).fetchone():
        return "already_admitted_for_parser"
    # No attempts/deadline/error/check reset, no lease or saved-body mutation.
    queue.db.execute("UPDATE articles SET state='retry' WHERE id=?", (row["id"],))
    return "enqueued"


def _archive(queue, path):
    job = dict(queue.db.execute("SELECT * FROM reviewed_repair_jobs WHERE request_id=?", (path.stem,)).fetchone())
    outcomes = [dict(row) for row in queue.db.execute(
        "SELECT target_index,lower(hex(article_id)) AS article_id,provider,outcome,recorded_at_utc,owner_id "
        "FROM reviewed_repair_items WHERE request_id=? ORDER BY target_index", (path.stem,))]
    receipt = {"contract_version": CONTRACT_VERSION, **job, "outcomes": outcomes,
               "admitted_targets": sum(row["outcome"] == "enqueued" for row in outcomes),
               "accepted_as_bodies": False, "request_sha256": path.stem}
    directory = _directory(queue.root, "processed")
    _write_once(directory / (path.stem + ".outcome.json"), _bytes(receipt))
    archived = directory / path.name
    if archived.is_symlink() or archived.exists() and hashlib.sha256(archived.read_bytes()).hexdigest() != path.stem:
        raise ValueError("repair_archive_conflict")
    os.replace(path, archived)
    for parent in (directory, path.parent):
        descriptor = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _quarantine(queue, path, error):
    # Preserve the input inode, including an unsafe symlink, without following
    # it. Bad local control files must not starve later valid work or HTTP.
    directory = _directory(queue.root, "rejected")
    info = path.lstat()
    identity = {"name": path.name, "inode": info.st_ino, "mtime_ns": info.st_mtime_ns,
                "size": info.st_size, "mode": info.st_mode}
    key = hashlib.sha256(_bytes(identity)).hexdigest()
    archived = directory / (key + ".input")
    if archived.exists() or archived.is_symlink():
        raise ValueError("repair_rejection_conflict")
    _write_once(directory / (key + ".outcome.json"), _bytes({
        "contract_version": CONTRACT_VERSION, "input_file_identity": identity,
        "error": type(error).__name__, "accepted_as_bodies": False}))
    # Deterministic receipt bytes let a crash between receipt and rename resume;
    # a later bad file with the same name gets a distinct retained inode proof.
    os.replace(path, archived)
    for parent in (directory, path.parent):
        descriptor = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def consume_repairs(queue, *, limit=32, max_seconds=0.05, stop=None):
    """Bound local admission work; normal dispatch still owns every request."""
    if type(limit) is not int or not 1 <= limit <= MAX_TARGETS or not math.isfinite(max_seconds) or not 0 < max_seconds <= 0.1:
        raise ValueError("invalid_bounded_repair_slice")
    started, counts, processed = time.monotonic(), Counter(), 0
    base = queue.root / "reviewed_repairs"
    if not base.exists() and not base.is_symlink():
        return {"processed": 0, "outcomes": {}}
    try:
        directory = _directory(queue.root, "pending")
        paths = _pending(directory)
    except (OSError, ValueError):
        queue.metrics.count("local_repairs", "inbox_unavailable")
        return {"processed": 0, "outcomes": {}, "inbox_unavailable": True}
    for path in paths:
        if processed >= limit or time.monotonic() - started >= max_seconds or stop is not None and stop.is_set():
            break
        try:
            request = _load(path)
        except (OSError, ValueError, TypeError, KeyError, OverflowError, RecursionError, NewsError) as error:
            try:
                _quarantine(queue, path, error)
            except (OSError, ValueError):
                queue.metrics.count("local_repairs", "quarantine_deferred")
            counts["invalid_request"] += 1
            processed += 1
            continue
        _schema(queue)
        with queue.db:
            queue.db.execute("INSERT OR IGNORE INTO reviewed_repair_jobs VALUES (?,?,0,?,NULL,?)",
                             (path.stem, len(request["targets"]), request["created_at_utc"], queue.owner))
            job = queue.db.execute("SELECT * FROM reviewed_repair_jobs WHERE request_id=?", (path.stem,)).fetchone()
            cursor = job["next_index"]
            while cursor < len(request["targets"]) and processed < limit:
                if time.monotonic() - started >= max_seconds or stop is not None and stop.is_set():
                    break
                target = request["targets"][cursor]
                outcome = _admit(queue, request, target)
                queue.db.execute("INSERT INTO reviewed_repair_items VALUES (?,?,?,?,?,?,?,?)", (
                    path.stem, cursor, bytes.fromhex(target["id"]), target["provider"],
                    request["sources"][target["provider"]]["parser_fingerprint"], outcome, utc_now(), queue.owner))
                counts[outcome] += 1
                processed += 1
                cursor += 1
                queue.counter("reviewed_repair_targets_processed", 1)
                if outcome == "enqueued":
                    queue.counter("reviewed_repair_targets_admitted", 1)
            complete = cursor == len(request["targets"])
            queue.db.execute("UPDATE reviewed_repair_jobs SET next_index=?,completed_at_utc=coalesce(completed_at_utc,?) "
                             "WHERE request_id=?", (cursor, utc_now() if complete else None, path.stem))
        if complete:
            try:
                _archive(queue, path)
            except (OSError, ValueError):
                queue.metrics.count("local_repairs", "archive_deferred")
    for outcome, count in counts.items():
        queue.metrics.count("local_repairs", "reviewed_" + outcome, count)
    return {"processed": processed, "outcomes": dict(counts)}
