"""Finite, cache-first queries admitted by the existing provider owners.

Registration only changes local scheduling metadata. It never calls a vendor,
reopens a terminal task or takes over an account/desktop worker lock.
"""
from __future__ import annotations

from contextlib import closing
from datetime import UTC, date, datetime, timedelta
import fcntl
import hashlib
import json
from pathlib import Path
import re
import sqlite3

from downloader.artifact_io import atomic_write_json

CONTRACT = "provider_priority_query_v1"
PROVIDERS = ("finlab", "finmind", "tej", "tej_api")


def _stamp(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("An explicit timezone is required")
    return result.astimezone(UTC)


def ensure_schema(con: sqlite3.Connection) -> None:
    con.execute("CREATE TABLE IF NOT EXISTS provider_query_intents ("
                "request_id TEXT NOT NULL,task_key TEXT NOT NULL,created_at_utc TEXT NOT NULL,"
                "expires_at_utc TEXT NOT NULL,PRIMARY KEY(request_id,task_key))")
    con.execute("CREATE INDEX IF NOT EXISTS provider_query_intents_task "
                "ON provider_query_intents(task_key,expires_at_utc)")
    con.execute("CREATE INDEX IF NOT EXISTS provider_query_intents_expiry "
                "ON provider_query_intents(expires_at_utc)")


def has_intents(con: sqlite3.Connection) -> bool:
    return con.execute("SELECT 1 FROM sqlite_master WHERE name='provider_query_intents'").fetchone() is not None


def task_key(item: dict | str) -> str:
    if isinstance(item, dict):
        fields = ("dataset", "data_id", "partition") if "data_id" in item else ("dataset", "partition")
        return json.dumps([item[k] for k in fields], ensure_ascii=False, separators=(",", ":"))
    return item


def _finlab_futures_partition(item: dict) -> tuple[str, date]:
    """A finite provider key/day, not a claim that a partition exists."""
    if not isinstance(item, dict) or set(item) != {'dataset', 'partition'}:
        raise ValueError('FinLab futures items need exact dataset and trade-date partition')
    key, partition = item['dataset'], item['partition']
    if (not isinstance(key, str) or not re.fullmatch(
            r'tw_futures_minute:[A-Z0-9]{1,6}_[0-9]{6}(?:W[1-5])?', key)
            or not isinstance(partition, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', partition)):
        raise ValueError('Invalid FinLab futures contract-month partition')
    month = re.search(r'_([0-9]{6})', key).group(1)
    datetime.strptime(month, '%Y%m')
    return key, date.fromisoformat(partition)


def _finlab_stock_partition(item: dict) -> tuple[str, date]:
    """An exact stock/day request; eligibility is checked by the market owner."""
    if not isinstance(item, dict) or set(item) != {'dataset', 'partition'}:
        raise ValueError('FinLab stock items need exact dataset and trade-date partition')
    key, partition = item['dataset'], item['partition']
    if (not isinstance(key, str) or not re.fullmatch(r'tw_(?:tick|minute):[A-Z0-9]{1,12}', key)
            or not isinstance(partition, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', partition)):
        raise ValueError('Invalid FinLab stock intraday partition')
    return key, date.fromisoformat(partition)


def _finlab_partition(item: dict) -> tuple[str, date]:
    key = item.get('dataset') if isinstance(item, dict) else None
    return (_finlab_futures_partition(item) if isinstance(key, str) and key.startswith('tw_futures_minute:')
            else _finlab_stock_partition(item))


def finlab_futures_candidates(con: sqlite3.Connection, now: datetime) -> list[tuple[str, date]]:
    """Read existing finite intents; the native intraday owner still admits each call."""
    result = []
    for raw in active_keys(con, now):
        try:
            values = json.loads(raw)
            if isinstance(values, list) and len(values) == 2:
                result.append(_finlab_futures_partition(dict(dataset=values[0], partition=values[1])))
        except (ValueError, TypeError, KeyError):
            continue
    return result


def finlab_stock_candidates(con: sqlite3.Connection, now: datetime, kind: str) -> list[tuple[str, date]]:
    """Return only exact stock requests for the existing owner of this family."""
    if kind not in {'tw_tick', 'tw_minute'}:
        raise ValueError('Invalid stock intraday family')
    result = []
    for raw in active_keys(con, now):
        try:
            values = json.loads(raw)
            if isinstance(values, list) and len(values) == 2:
                key, day = _finlab_stock_partition(dict(dataset=values[0], partition=values[1]))
                if key.startswith(kind + ':'):
                    result.append((key, day))
        except (ValueError, TypeError, KeyError):
            continue
    return result


def active_keys(con: sqlite3.Connection, now: datetime) -> list[str]:
    if not has_intents(con):
        return []
    # Deduplicate the registered demand, without silently truncating its tail.
    return [r[0] for r in con.execute(
        "SELECT task_key FROM provider_query_intents WHERE created_at_utc<=? AND expires_at_utc>? "
        "GROUP BY task_key ORDER BY MIN(created_at_utc),task_key",
        (now.isoformat(), now.isoformat()))]


def finmind_candidate(con, now, *, delegated, retry_filter, retry_args, background_only=False,
                      preempt_foreign_history=False):
    if not has_intents(con):
        return None
    excluded = " AND dataset NOT IN (" + ",".join("?" for _ in delegated) + ")" if delegated else ""
    priority_excluded = excluded
    priority_args = tuple(sorted(delegated))
    if preempt_foreign_history:
        from downloader.finmind_product_query_verification import FOREIGN_HISTORY_BACKFILLS
        priority_excluded += " AND NOT (kind='id_history' AND dataset IN (" + ','.join('?' for _ in FOREIGN_HISTORY_BACKFILLS) + '))'
        priority_args += tuple(FOREIGN_HISTORY_BACKFILLS)
    if not background_only and con.execute(
            "SELECT 1 FROM tasks WHERE priority=0 AND ((state='pending' AND next_attempt_at_utc IS NULL) "
            "OR (state IN ('pending','complete','observed_empty','failed') AND next_attempt_at_utc<=?))"
            + priority_excluded + retry_filter + " LIMIT 1", (now.isoformat(), *priority_args, *retry_args)).fetchone():
        return None
    return con.execute(
        "SELECT t.dataset,t.data_id,t.partition,t.kind,t.priority,t.state "
        "FROM provider_query_intents p JOIN tasks t "
        "ON t.dataset=json_extract(p.task_key,'$[0]') "
        "AND t.data_id=json_extract(p.task_key,'$[1]') AND t.partition=json_extract(p.task_key,'$[2]') "
        "WHERE p.created_at_utc<=? AND p.expires_at_utc>? AND t.state IN ('pending','failed') "
        "AND (t.next_attempt_at_utc IS NULL OR t.next_attempt_at_utc<=?)"
        + excluded.replace("dataset", "t.dataset") + retry_filter.replace("tasks.", "t.")
        + " ORDER BY p.created_at_utc,t.dataset,t.data_id,t.partition LIMIT 1",
        (now.isoformat(), now.isoformat(), now.isoformat(), *sorted(delegated), *retry_args)).fetchone()


def finmind_reservation(con, plan, now, delegated):
    """Finite reads may precede foreign history renewals, never dated releases."""
    from downloader import finmind_retry_cohorts, finmind_product_query_verification
    retry_filter, retry_args = finmind_retry_cohorts.admission_clause(con, now)
    ready = finmind_candidate(con, now, delegated=delegated, retry_filter=retry_filter,
                             retry_args=retry_args, background_only=True) is not None
    return finmind_product_query_verification.prioritize_source_checks(
        con, plan, now, delegated, finite_query_ready=ready)


def finmind_native_candidate(con, now, *, where, args):
    """Sponsor keeps its native dependency, dataset and release predicate."""
    if not has_intents(con):
        return None
    return con.execute(
        "SELECT tasks.dataset,tasks.data_id,tasks.partition,tasks.kind,tasks.priority,tasks.state "
        "FROM provider_query_intents p JOIN tasks "
        "ON tasks.dataset=json_extract(p.task_key,'$[0]') AND tasks.data_id=json_extract(p.task_key,'$[1]') "
        "AND tasks.partition=json_extract(p.task_key,'$[2]') WHERE p.created_at_utc<=? AND p.expires_at_utc>? "
        "AND tasks.state IN ('pending','failed') AND (" + where + ") "
        "ORDER BY p.created_at_utc,tasks.dataset,tasks.partition LIMIT 1",
        (now.isoformat(), now.isoformat(), *args)).fetchone()


def queued_candidate(con, now: str, *, provider: str, where: str, args=()):
    """A native selector supplies its full admission predicate, including retry isolation."""
    if not has_intents(con):
        return None
    identity = "id" if provider == "tej_api" else "task_id"
    return con.execute(
        f"SELECT tasks.* FROM provider_query_intents p JOIN tasks ON tasks.{identity}=p.task_key "
        "WHERE p.created_at_utc<=? AND p.expires_at_utc>? AND " + where
        + f" ORDER BY p.created_at_utc,tasks.{identity} LIMIT 1", (now, now, *args)).fetchone()


def _items(plan: dict) -> list:
    allowed = {'contract', 'provider', 'reason', 'created_at_utc', 'expires_at_utc', 'items', 'fresh_after_utc'}
    if not isinstance(plan, dict) or set(plan) - allowed:
        raise ValueError('Unexpected priority plan fields')
    if plan.get('fresh_after_utc') is not None and plan.get('provider') != 'finlab':
        raise ValueError('fresh_after_utc is supported only for general FinLab keys')
    items = plan.get("items")
    if (plan.get("contract") != CONTRACT or plan.get("provider") not in PROVIDERS
            or not isinstance(plan.get("reason"), str) or not plan["reason"].strip()
            or len(plan["reason"]) > 1000 or not isinstance(items, list) or not items):
        raise ValueError("Expected a named provider, reason and at least one native item")
    for item in items:
        if plan["provider"] == "finmind":
            if (not isinstance(item, dict) or set(item) != {"dataset", "data_id", "partition"}
                    or any(not isinstance(v, str) or len(v) > 512 for v in item.values())
                    or not item["dataset"] or not item["partition"]):
                raise ValueError("FinMind items need exact dataset/data_id/partition strings")
        elif plan['provider'] == 'finlab' and isinstance(item, dict):
            _finlab_partition(item)
        elif not isinstance(item, str) or not item or len(item) > 512:
            raise ValueError("Items must be native catalog keys or task IDs")
    if plan['provider'] == 'finlab' and any(isinstance(item, dict) for item in items):
        if not all(isinstance(item, dict) for item in items) or plan.get('fresh_after_utc') is not None:
            raise ValueError('FinLab intraday partition plans cannot mix general keys or demand freshness')
    # Equivalent scopes share one intent even when clients repeat/reorder keys.
    unique = {task_key(item): item for item in items}
    return [unique[key] for key in sorted(unique)]


def _connect(root: Path):
    db = root / "queue.sqlite3"
    if not db.is_file() or db.is_symlink():
        raise ValueError("An existing canonical provider queue is required")
    con = sqlite3.connect(db, timeout=10)
    con.row_factory = sqlite3.Row
    return con


def _native_rows(con, provider, items, *, allow_superseded=False):
    fields = ("dataset", "data_id", "partition") if provider == "finmind" else (
        "id" if provider == "tej_api" else "task_id",)
    # This is a local SQL batch size, never a cap on demand or a vendor call.
    # Honour the runtime SQLite parameter limit, including older builds.
    batch_size = max(1, min(1000, con.getlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER) // len(fields)))
    rows = []
    for start in range(0, len(items), batch_size):
        batch = items[start:start + batch_size]
        identities = [tuple(item[field] for field in fields) if isinstance(item, dict) else (item,)
                      for item in batch]
        placeholders = ','.join('(' + ','.join('?' for _ in fields) + ')' for _ in batch)
        join = ' AND '.join(f't.{field}=r.{field}' for field in fields)
        found = {tuple(row[field] for field in fields): dict(row) for row in con.execute(
            f"WITH requested({','.join(fields)}) AS (VALUES {placeholders}) "
            f"SELECT t.* FROM requested r JOIN tasks t ON {join}",
            tuple(value for key in identities for value in key))}
        for item, key in zip(batch, identities):
            row = found.get(key)
            if row is None or (not allow_superseded and row['state'].startswith('superseded')):
                raise ValueError('Native item not registered or superseded: ' + task_key(item))
            rows.append(row)
    return rows


def inherit(con, parent_key: str, children: list[str]) -> None:
    """A native, already-verified disjoint split retains finite priority/expiry."""
    if not has_intents(con):
        return
    con.executemany("INSERT OR IGNORE INTO provider_query_intents "
                    "SELECT request_id,?,created_at_utc,expires_at_utc FROM provider_query_intents WHERE task_key=?",
                    [(child, parent_key) for child in children])


def register(root: Path, plan: dict, *, now: datetime | None = None) -> dict:
    now = (now or datetime.now(UTC)).astimezone(UTC)
    items = _items(plan)
    legacy_digest = hashlib.sha256(json.dumps(plan, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    plan = {**plan, 'items': items}
    created, expires = _stamp(plan["created_at_utc"]), _stamp(plan["expires_at_utc"])
    if created > now or not now < expires <= created + timedelta(days=7):
        raise ValueError("Priority intents must be due, unexpired and at most seven days long")
    if plan.get('fresh_after_utc') is not None and _stamp(plan['fresh_after_utc']) > created:
        raise ValueError('Required source check cannot be later than request creation')
    digest = hashlib.sha256(json.dumps(plan, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    root = root.resolve(strict=True)
    folder = root / "priority_queries"
    folder.mkdir(mode=0o700, exist_ok=True)
    receipt = folder / (digest + ".json")
    with (root / ".priority_queries.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if legacy_digest != digest and (folder / (legacy_digest + '.json')).exists():
            return inspect(root, legacy_digest, now=now)
        if receipt.exists():
            return inspect(root, digest, now=now)
        provider = plan["provider"]
        if provider == "finlab":
            _register_finlab(root, plan, digest, created, expires, now)
        else:
            with closing(_connect(root)) as con:
                con.execute("BEGIN IMMEDIATE")
                native = _native_rows(con, provider, items)  # Validate the entire request before mutation.
                ensure_schema(con)
                con.executemany("INSERT OR IGNORE INTO provider_query_intents VALUES (?,?,?,?)",
                                ((digest, task_key(item), created.isoformat(), expires.isoformat())
                                 for item, row in zip(items, native) if row['state'] in {'pending','failed','retry','running'}))
                con.commit()
        atomic_write_json(receipt, {"contract": CONTRACT, "request_id": digest, "plan": plan,
                                    "registered_at_utc": now.isoformat(), "provider_calls": 0})
    return inspect(root, digest, now=now)


def _register_finlab(root, plan, digest, created, expires, now):
    if isinstance(plan['items'][0], dict):
        _register_finlab_intraday(root, plan, digest, created, expires, now)
        return
    from stockagent.data.finlab_gap_priority import CONTRACT as GAP, pending_gap_requests
    from scripts.download_finlab_history import has_local_download
    discovery = json.loads((root / "catalog/discovery.json").read_text())
    allowed = set(discovery["keys"])
    if any(key not in allowed or key.startswith(("tw_minute:", "tw_tick:")) for key in plan["items"]):
        raise ValueError("Use registered general FinLab keys; intraday is owned by its partition scheduler")
    with (root / ".gap_priority.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        path = root / "gap_priority_requests.json"
        old = json.loads(path.read_text()) if path.exists() else {"contract": GAP, "requests": []}
        pending_gap_requests(root, now=now)  # Validate the old document before extending it.
        rows = [r for r in old["requests"] if _stamp(r["expires_at_utc"]) > now]
        ids = {r["request_id"] for r in rows}
        for key in plan["items"]:
            if plan.get('fresh_after_utc') is None and has_local_download(key, root):
                continue
            identifier = hashlib.sha256((digest + "\0" + key).encode()).hexdigest()
            if identifier not in ids:
                rows.append({"dataset": key, "reason": "interactive_query", "priority": 0,
                             "request_id": identifier, "evidence_sha256": digest,
                             "required_after_utc": plan.get('fresh_after_utc') or created.isoformat(),
                             "expires_at_utc": expires.isoformat()})
        if path.exists():
            history = root / "gap_priority_history"
            history.mkdir(exist_ok=True)
            raw = path.read_bytes()
            archived = history / (hashlib.sha256(raw).hexdigest() + ".json")
            if not archived.exists():
                archived.write_bytes(raw)
                archived.chmod(0o600)
        atomic_write_json(path, {"contract": GAP, "requests": rows, "updated_at_utc": now.isoformat()})


def _register_finlab_intraday(root, plan, digest, created, expires, now):
    from scripts.download_finlab_history import TAIPEI
    from scripts.download_finlab_intraday import _partition_paths, _stored_receipt
    parsed = [_finlab_partition(item) for item in plan['items']]
    if any(day > now.astimezone(TAIPEI).date() for _, day in parsed):
        raise ValueError('A FinLab intraday priority date cannot be in the future')
    db = root / 'intraday/market_index.sqlite3'
    if not db.is_file() or db.is_symlink():
        raise ValueError('The existing canonical FinLab intraday index is required')
    # Receipt verification can hash sizeable source objects.  Never hold the
    # owner's SQLite write lock while reading them; the owner rechecks receipts
    # again before dispatch, so a concurrent completion remains cache-first.
    rows = [(digest, task_key(item), created.isoformat(), expires.isoformat())
            for item, (key, day) in zip(plan['items'], parsed)
            if _stored_receipt(_partition_paths(root, key, day)[0], root, key, day, verify_hash=True) is None]
    with closing(sqlite3.connect(db, timeout=10)) as con:
        con.execute('BEGIN IMMEDIATE')
        ensure_schema(con)
        con.executemany('INSERT OR IGNORE INTO provider_query_intents VALUES (?,?,?,?)', rows)
        con.commit()


def inspect(root: Path, request_id: str, *, now: datetime | None = None) -> dict:
    now = now or datetime.now(UTC)
    if len(request_id) != 64 or any(c not in "0123456789abcdef" for c in request_id):
        raise ValueError("A request SHA256 is required")
    receipt = json.loads((root / "priority_queries" / (request_id + ".json")).read_text())
    plan = receipt["plan"]
    if hashlib.sha256(json.dumps(plan, sort_keys=True, ensure_ascii=False).encode()).hexdigest() != request_id:
        raise ValueError("Priority receipt identity mismatch")
    items = _items(plan)
    if plan["provider"] == "finlab":
        from stockagent.data.finlab_acquisition_contract import UPSTREAM_CHECK_MODES, safe_stem, utc_time
        from scripts.download_finlab_history import has_local_download
        from scripts.download_finlab_intraday import (
            _partition_paths, _priority_scope_path, _priority_scope_wait, _stored_receipt,
        )
        states = []
        for key in items:
            if isinstance(key, dict):
                dataset, day = _finlab_partition(key)
                path, attempt_path = _partition_paths(root, dataset, day)
                native = _stored_receipt(path, root, dataset, day, verify_hash=True)
                try:
                    attempt = json.loads(attempt_path.read_text())
                except (OSError, ValueError):
                    attempt = {}
                attempted = utc_time(attempt.get('attempted_at_utc'))
                local_scope = None
                if (dataset.startswith(('tw_tick:', 'tw_minute:'))
                        and _priority_scope_path(root, dataset, day).is_file()):
                    from scripts.check_finlab_refresh_due import intraday_source_identity
                    local_scope = _priority_scope_wait(root, dataset, day, now=now,
                        source_identity=intraday_source_identity(root.parent))
                state = (native['status'] if native else
                         'invalid_local_receipt' if path.exists() else
                         'expired' if _stamp(plan['expires_at_utc']) <= now else
                         'waiting_retry_existing_owner' if attempted and attempted >= _stamp(plan['created_at_utc'])
                         else 'waiting_local_scope' if local_scope
                         else 'pending_existing_owner')
                states.append({'key': task_key(key), 'state': state, 'receipt_path': str(path),
                               'last_attempt_state': attempt.get('status'),
                               'last_provider_code': attempt.get('provider_code'),
                               'last_attempted_at_utc': attempt.get('attempted_at_utc'),
                               'local_scope_reason': local_scope.get('reason') if local_scope else None})
                continue
            path = root / 'receipts' / (safe_stem(key) + '.json')
            native = json.loads(path.read_text()) if path.exists() else {}
            checked = utc_time(native.get('source_checked_at_utc'))
            verified = (native.get('dataset') == key and native.get('source_check_mode') in UPSTREAM_CHECK_MODES
                        and checked and _stamp(plan.get('fresh_after_utc') or plan['created_at_utc']) <= checked <= now)
            cached = plan.get('fresh_after_utc') is None and has_local_download(key, root)
            state = ('available_locally' if cached else 'upstream_checked' if verified else
                     'expired' if _stamp(plan['expires_at_utc']) <= now else 'pending_existing_owner')
            states.append({'key': key, 'state': state, 'receipt_path': str(path),
                           'source_checked_at_utc': native.get('source_checked_at_utc'),
                           'last_check_result': native.get('last_check_result')})
    else:
        with closing(_connect(root)) as con:
            states = [{"key": task_key(key), "state": row["state"],
                       "next_attempt_at_utc": row.get("next_attempt_at_utc", row.get("next_attempt_utc")),
                       "receipt_path": row.get("receipt_path"),
                       "rows": row.get("actual_rows", row.get("rows"))}
                      for key, row in zip(items, _native_rows(con, plan["provider"], items, allow_superseded=True))]
            if has_intents(con) and plan['provider'] in {'tej', 'tej_api'}:
                requested_keys = {task_key(item) for item in items}
                derived = [r[0] for r in con.execute('SELECT task_key FROM provider_query_intents WHERE request_id=?', (request_id,)) if r[0] not in requested_keys]
                states.extend({'key': key, 'state': row['state'], 'derived_from_requested_scope': True}
                              for key, row in zip(derived, _native_rows(con, plan['provider'], derived, allow_superseded=True)))
    return {"contract": CONTRACT, "request_id": request_id, "provider": plan["provider"],
            "expires_at_utc": plan["expires_at_utc"], "observed_at_utc": now.isoformat(), "items": states,
            "provider_calls": 0, "priority_active": _stamp(plan['created_at_utc']) <= now < _stamp(plan['expires_at_utc']),
            "data_completeness_claim": False,
            "note": "Native completion/empty is a source receipt state; inspect and validate the stored data before use."}
