"""Trial API adapter: account-scoped quota ledger, resumable native pages.

Smart Wizard is a separate source representation. API observations never become
fake Wizard receipts, and a public catalogue is not an entitlement certificate.
"""
from __future__ import annotations

from contextlib import closing
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
import hashlib
import gzip
import io
import json
import os
from pathlib import Path
import re
import sqlite3
from urllib.parse import quote, urlencode
import uuid
from zoneinfo import ZoneInfo

from downloader.artifact_io import atomic_write_bytes, atomic_write_json, atomic_write_parquet, sha256_file
from downloader.common import SharedRateLimiter, load_env_file, parse_retry_after_seconds
from downloader.http_transport import HttpRequestPolicy, ResilientHttpTransport
from downloader.tej_api_catalog import BASE, HEADERS, TABLE_ID, TRIAL_LIMITS

CONTRACT = "tej_trial_native_pages_v1"
ACCOUNT_KEYS = ("startDate", "endDate", "todayRows", "todayReqCount", "rowsDayLimit",
                "reqDayLimit", "lastStatTime", "multiConn")
ENTITLEMENT_KEYS = ("tableId", "startUsageDate", "endUsageDate", "dataStartYear",
                    "dataEndYear", "allowColumns", "conditions")


class TejAPIError(RuntimeError):
    """An allowlisted operational code, never a vendor body or credential URL."""


def connect(root: Path) -> sqlite3.Connection:
    root = root.resolve()
    if root in {root.parent, Path.home().resolve(), Path.cwd().resolve()}:
        raise ValueError("Dedicated TEJ API workspace required")
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    root.chmod(0o700)
    con = sqlite3.connect(root / "queue.sqlite3", timeout=10)
    con.row_factory = sqlite3.Row
    con.executescript("""
      CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS requests(
        id TEXT PRIMARY KEY,account TEXT NOT NULL,started REAL NOT NULL,
        reserved_rows INTEGER NOT NULL,actual_rows INTEGER,state TEXT NOT NULL);
      CREATE INDEX IF NOT EXISTS requests_window ON requests(account,started);
      CREATE TABLE IF NOT EXISTS tasks(
        id TEXT PRIMARY KEY,table_id TEXT NOT NULL,params_json TEXT NOT NULL,
        priority INTEGER NOT NULL,state TEXT NOT NULL,cursor TEXT,query_rows INTEGER NOT NULL DEFAULT 0,
        actual_rows INTEGER NOT NULL DEFAULT 0,next_attempt_utc TEXT,error_code TEXT);
      CREATE TABLE IF NOT EXISTS pages(
        request_id TEXT PRIMARY KEY,task_id TEXT,table_id TEXT NOT NULL,raw_path TEXT NOT NULL,
        parquet_path TEXT,receipt_path TEXT NOT NULL,rows INTEGER NOT NULL,sha256 TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS applied_pages(request_id TEXT PRIMARY KEY);
    """)
    task_columns = {r[1] for r in con.execute("PRAGMA table_info(tasks)")}
    for name, kind in (("active_request_id", "TEXT"), ("page_size", "INTEGER")):
        if name not in task_columns:
            con.execute(f"ALTER TABLE tasks ADD COLUMN {name} {kind}")
    return con


def _bounded_int(value, name: str, cap: int) -> int:
    if type(value) is not int or not 0 <= value <= 10**12:
        raise TejAPIError("invalid_account_" + name)
    return min(value, cap)


def sanitize_account(payload: dict, *, now: datetime) -> dict:
    if not isinstance(payload, dict):
        raise TejAPIError("invalid_account_payload")
    source_tables = (payload.get("user") or {}).get("tables") or payload.get("tables")
    if not isinstance(source_tables, dict) or not source_tables:
        raise TejAPIError("account_entitlements_missing")
    tables = {}
    for key, entry in source_tables.items():
        if TABLE_ID.fullmatch(key) and isinstance(entry, dict) and entry.get("tableId") == key:
            tables[key] = {name: entry.get(name) for name in ENTITLEMENT_KEYS}
    limits = {"calls_per_day": _bounded_int(payload.get("reqDayLimit"), "calls_limit", 500),
              "rows_per_day": _bounded_int(payload.get("rowsDayLimit"), "rows_limit", 50_000),
              "rows_per_page": 10_000, "rows_per_query": 50_000}
    for name in ("todayRows", "todayReqCount"):
        _bounded_int(payload.get(name), name, 10**12)
    return {"contract": CONTRACT, "observed_at_utc": now.isoformat(), "quota": limits,
            "account": {key: payload.get(key) for key in ACCOUNT_KEYS}, "tables": tables,
            "credential_included": False, "identity_included": False}


class TejTrialAPI:
    def __init__(self, root: Path, key: str, *, transport=None, now=None):
        if not isinstance(key, str) or not key or key != key.strip():
            raise TejAPIError("credential_missing_or_invalid_shape")
        self.root, self._key = root.resolve(), key
        self._account_id = hashlib.sha256(key.encode()).hexdigest()
        self.now = now or (lambda: datetime.now(UTC))
        with closing(connect(self.root)) as con, con:
            previous = con.execute("SELECT value FROM meta WHERE key='credential_fingerprint'").fetchone()
            if previous and previous[0] != self._account_id:
                # Rows from a different entitlement must not certify new key scope.
                raise TejAPIError("credential_changed_use_new_account_workspace")
            con.execute("INSERT OR IGNORE INTO meta VALUES('credential_fingerprint',?)", (self._account_id,))
        self.transport = transport or ResilientHttpTransport(
            HttpRequestPolicy("tej-api", max_retries=0, timeout_seconds=45),
            limiter=SharedRateLimiter(.2, name="tej-api"))

    @classmethod
    def from_env(cls, root: Path, env_file: Path):
        load_env_file(env_file, allowed_names={"TEJ_API_KEY"})
        return cls(root, os.environ.get("TEJ_API_KEY", ""))

    def _account(self, con) -> dict | None:
        value = con.execute("SELECT value FROM meta WHERE key='account'").fetchone()
        return json.loads(value[0]) if value else None

    def cooldown_until(self, con) -> datetime | None:
        value = con.execute("SELECT value FROM meta WHERE key='provider_cooldown_until'").fetchone()
        return datetime.fromisoformat(value[0]) if value else None

    def budget(self, con, now: datetime) -> dict:
        account = self._account(con)
        limits = (account or {}).get("quota", TRIAL_LIMITS)
        # Until the vendor reset timezone is independently confirmed, also
        # enforce a rolling 24h cap; a guessed midnight cannot release quota.
        start = min(now.timestamp() - 86400,
                    now.astimezone(ZoneInfo("Asia/Taipei")).replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        rows = con.execute("SELECT COUNT(*),COALESCE(SUM(COALESCE(actual_rows,reserved_rows)),0) "
                           "FROM requests WHERE account=? AND started>?", (self._account_id, start)).fetchone()
        calls_used, rows_used = int(rows[0]), int(rows[1])
        if account:
            observed = datetime.fromisoformat(account["observed_at_utc"])
            # Server counters cover other callers too. Add only calls after
            # the snapshot, never add the whole local history a second time.
            if (now - observed).total_seconds() < 86400:
                after = con.execute("SELECT COUNT(*),COALESCE(SUM(COALESCE(actual_rows,reserved_rows)),0) "
                                    "FROM requests WHERE account=? AND started>?",
                                    (self._account_id, observed.timestamp())).fetchone()
                calls_used = max(calls_used, int(account["account"]["todayReqCount"]) + int(after[0]))
                rows_used = max(rows_used, int(account["account"]["todayRows"]) + int(after[1]))
        return {"calls_used": calls_used, "rows_used": rows_used,
                "calls_remaining": max(0, limits["calls_per_day"] - calls_used),
                "rows_remaining": max(0, limits["rows_per_day"] - rows_used),
                "quota": limits, "reset_basis": "calendar_plus_rolling_24h_until_vendor_reset_verified",
                "counter_basis": "conservative_all_http_admission_plus_external_usage",
                "official_usage_snapshot": {name: account["account"][name] for name in
                    ("todayRows", "todayReqCount", "lastStatTime")} if account else None,
                "official_usage_observed_at_utc": account["observed_at_utc"] if account else None}

    def _claim(self, wanted_rows: int, task_id: str | None = None, *, exact_rows: bool = False) -> tuple[str, int]:
        checked = self.now()
        with closing(connect(self.root)) as con, con:
            con.execute("BEGIN IMMEDIATE")
            until = self.cooldown_until(con)
            if until and until > checked:
                raise TejAPIError("provider_cooldown")
            budget = self.budget(con, checked)
            take = min(wanted_rows, budget["rows_remaining"], 10_000)
            if exact_rows and take != wanted_rows:
                # A cursor may retain the initial page size on the server.
                # Never assume a smaller later per_page can protect quota.
                raise TejAPIError("waiting_page_budget")
            if not budget["calls_remaining"] or wanted_rows and take < 1:
                raise TejAPIError("waiting_quota")
            identifier = uuid.uuid4().hex
            con.execute("INSERT INTO requests VALUES(?,?,?,?,NULL,'reserved')",
                        (identifier, self._account_id, checked.timestamp(), take))
            if task_id:
                con.execute("UPDATE tasks SET active_request_id=? WHERE id=?", (identifier, task_id))
        return identifier, take

    def _request(self, path: str, params: dict, wanted_rows: int,
                 *, task_id: str | None = None) -> tuple[dict, bytes, str, int]:
        identifier, take = self._claim(wanted_rows, task_id, exact_rows=bool(params.get("opts.cursor_id")))
        # Record the public request scope before transport; recovery never
        # guesses filters or resends a response already saved on this host.
        from downloader.http_transport import sanitized_url
        intent = {"contract": CONTRACT, "request_id": identifier, "task_id": task_id,
                  "endpoint": sanitized_url(BASE + path), "params": params,
                  "page_bound": take, "started_at_utc": self.now().isoformat()}
        atomic_write_json(self.root / "requests" / (identifier + ".intent.json"), intent)
        params = {**params, "api_key": self._key}
        if wanted_rows:
            params["opts.per_page"] = take
        url = BASE + path + ("?" + urlencode(params) if params else "")
        try:
            response = self.transport.request_bytes(url, headers=HEADERS,
                accepted_statuses=frozenset({400, 401, 403, 404, 429, 500, 502, 503, 504}))
        except Exception:
            # A lost response may have consumed the entire reservation.
            with closing(connect(self.root)) as con, con:
                con.execute("UPDATE requests SET state='unknown_transport_outcome' WHERE id=?", (identifier,))
            raise TejAPIError("unknown_transport_outcome") from None
        if response.status != 200:
            with closing(connect(self.root)) as con, con:
                con.execute("UPDATE requests SET state=?,actual_rows=0 WHERE id=?", ("http_" + str(response.status), identifier))
                if response.status in {429, 503}:
                    retry_after = next((value for name, value in response.headers.items()
                                        if name.casefold() == "retry-after"), None)
                    delay = parse_retry_after_seconds(retry_after, now=self.now())
                    if delay is not None or response.status == 429:
                        until = self.now() + timedelta(seconds=max(1, delay if delay is not None else 3600))
                        previous = self.cooldown_until(con)
                        con.execute("INSERT OR REPLACE INTO meta VALUES('provider_cooldown_until',?)",
                                    (max(until, previous).isoformat() if previous else until.isoformat(),))
            code = {400: "query_rejected", 401: "authentication_rejected", 403: "access_denied",
                    404: "table_or_endpoint_missing", 429: "waiting_quota"}.get(response.status, "transient_provider_error")
            raise TejAPIError(code)
        try:
            encoding = next((value for name, value in response.headers.items()
                             if name.casefold() == "content-encoding"), "identity").casefold()
            if encoding == "gzip":
                with gzip.GzipFile(fileobj=io.BytesIO(response.body)) as stream:
                    body = stream.read(128 * 1024 * 1024 + 1)
            elif encoding in {"identity", ""}:
                body = response.body
            else:
                raise ValueError("unsupported_encoding")
            if len(body) > 128 * 1024 * 1024:
                raise ValueError("oversized_response")
            if path.startswith("/api/datatables/TRAIL/") and path.endswith(".json"):
                if self._key.encode() in body:
                    raise ValueError("credential_echo")
                code = path.removesuffix(".json").rsplit("/", 1)[-1]
                try:
                    atomic_write_bytes(self.root / "pages" / code / (identifier + ".json"), body, durable=True)
                except OSError:
                    raise TejAPIError("local_response_storage_failed") from None
            payload = json.loads(body, parse_float=Decimal)
            if not isinstance(payload, dict) or payload.get("tej_error") or payload.get("quandl_error"):
                raise ValueError("vendor_error")
            rows = (payload.get("datatable") or {}).get("data", [])
            if not isinstance(rows, list):
                raise ValueError("page_bound")
        except (ValueError, TypeError, OSError, EOFError):
            raise TejAPIError("invalid_source_payload") from None
        with closing(connect(self.root)) as con, con:
            con.execute("UPDATE requests SET state='received',actual_rows=? WHERE id=?", (len(rows), identifier))
        if len(rows) > take:
            raise TejAPIError("provider_page_bound_exceeded")
        return payload, body, identifier, take

    def authenticate(self) -> dict:
        payload, _, identifier, _ = self._request("/api/apiKeyInfo/" + quote(self._key, safe=""), {}, 0)
        account = sanitize_account(payload, now=self.now())
        local_day = self.now().astimezone(ZoneInfo("Asia/Taipei")).date().isoformat()
        if not account["account"].get("startDate") or not account["account"].get("endDate") \
                or not str(account["account"]["startDate"])[:10] <= local_day <= str(account["account"]["endDate"])[:10]:
            raise TejAPIError("account_not_in_usage_window")
        with closing(connect(self.root)) as con, con:
            con.execute("INSERT OR REPLACE INTO meta VALUES('account',?)", (json.dumps(account),))
        atomic_write_json(self.root / "account_status.json", account)
        return account

    def _entitlement(self, table_id: str) -> dict:
        if not TABLE_ID.fullmatch(table_id):
            raise TejAPIError("non_trial_table_not_allowed")
        with closing(connect(self.root)) as con:
            account = self._account(con)
        if not account or (self.now() - datetime.fromisoformat(account["observed_at_utc"])).total_seconds() > 21600:
            raise TejAPIError("fresh_account_permission_required")
        entry = account["tables"].get(table_id)
        local_day = self.now().astimezone(ZoneInfo("Asia/Taipei")).date().isoformat()
        if not entry or not str(entry.get("startUsageDate"))[:10] <= local_day <= str(entry.get("endUsageDate"))[:10]:
            raise TejAPIError("table_permission_unavailable")
        if not str(account["account"]["startDate"])[:10] <= local_day <= str(account["account"]["endDate"])[:10]:
            raise TejAPIError("account_usage_expired")
        return entry

    def metadata(self, table_id: str) -> dict:
        self._entitlement(table_id)
        payload, _, _, _ = self._request("/api/datatables/" + table_id + "/metadata", {}, 0)
        item = payload.get("datatable")
        if not isinstance(item, dict) or str(item.get("dbCode")) + "/" + str(item.get("tableCode")) != table_id:
            raise TejAPIError("metadata_identity_mismatch")
        atomic_write_json(self.root / "metadata" / (table_id.split("/")[1] + ".json"), item)
        return item

    def page(self, table_id: str, params: dict, *, wanted_rows: int = 10_000,
             task_id: str | None = None) -> dict:
        self._entitlement(table_id)
        if any(key.casefold() in {"api_key", "key", "token", "api_token"} for key in params) \
                or any(self._key in str(value) for value in params.values()):
            raise TejAPIError("credential_must_not_be_query_scope")
        if type(wanted_rows) is not int or not 1 <= wanted_rows <= 10_000:
            raise TejAPIError("invalid_page_size")
        payload, body, identifier, bound = self._request("/api/datatables/" + table_id + ".json", params, wanted_rows, task_id=task_id)
        return self.ingest_page(table_id, params, payload, body, identifier, bound, task_id)

    def ingest_page(self, table_id: str, params: dict, payload: dict, body: bytes,
                    identifier: str, bound: int, task_id: str | None) -> dict:
        """Local adoption of one saved response; never a provider request."""
        item = payload.get("datatable")
        columns, rows = (item or {}).get("columns"), (item or {}).get("data")
        if not isinstance(columns, list) or not isinstance(rows, list):
            raise TejAPIError("missing_native_table")
        names = [column.get("name") for column in columns]
        if not names or any(not isinstance(x, str) for x in names) or len(set(names)) != len(names) \
                or any(not isinstance(row, list) or len(row) != len(names) for row in rows):
            raise TejAPIError("invalid_native_schema_or_row")
        cursor = (payload.get("meta") or {}).get("next_cursor_id")
        if cursor is not None and (not isinstance(cursor, str) or not cursor):
            raise TejAPIError("invalid_pagination_cursor")
        data_digest = hashlib.sha256(json.dumps(item, ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()
        if rows and task_id:
            # A TEJ cursor identifies server-side pagination state and can
            # remain unchanged across distinct pages. Check native content,
            # not token rotation; retain and block genuine repeated pages.
            with closing(connect(self.root)) as con:
                previous = list(con.execute("SELECT receipt_path,raw_path FROM pages WHERE task_id=?", (task_id,)))
            for page in previous:
                saved = json.loads((self.root / page["receipt_path"]).read_text())
                signature = saved.get("native_data_sha256")
                if not signature:
                    older = json.loads((self.root / page["raw_path"]).read_bytes(), parse_float=Decimal)
                    signature = hashlib.sha256(json.dumps(older['datatable'], ensure_ascii=False,
                        sort_keys=True, default=str).encode()).hexdigest()
                if signature == data_digest:
                    raise TejAPIError("pagination_repeated_page_content")
        digest = hashlib.sha256(body).hexdigest()
        prefix = "pages/" + table_id.split("/")[1] + "/" + identifier
        raw_path, parquet_path, receipt_path = prefix + ".json", prefix + ".parquet", prefix + ".receipt.json"
        atomic_write_bytes(self.root / raw_path, body, durable=True)
        # Numeric JSON is parsed as Decimal. No binary float round-trip, log,
        # imputation, resampling or undocumented unit conversion is applied.
        if rows:
            import pyarrow as pa
            arrays = []
            physical_fallbacks = []
            for index, column in enumerate(columns):
                values = [row[index] for row in rows]
                match = re.fullmatch(r"decimal\((\d+),(\d+)\)", str(column.get("type", "")))
                dtype = pa.decimal128(*map(int, match.groups())) if match else None
                try:
                    numeric = [None if value is None else Decimal(str(value)) for value in values] if match else values
                    arrays.append(pa.array(numeric, type=dtype))
                except (InvalidOperation, pa.ArrowInvalid, pa.ArrowTypeError):
                    # Some declared numeric fields contain vendor sentinels or
                    # a wider scale. Preserve them, not zeros/rounded numbers.
                    physical_fallbacks.append(column["name"])
                    arrays.append(pa.array([None if value is None else str(value) for value in values], type=pa.string()))
            frame = pa.Table.from_arrays(arrays, names=names)
            frame = frame.replace_schema_metadata({b"source_contract": CONTRACT.encode(),
                b"source_table_id": table_id.encode(),
                b"source_columns": json.dumps(columns, ensure_ascii=False).encode(),
                b"physical_string_fallback_fields": json.dumps(physical_fallbacks).encode()})
            atomic_write_parquet(self.root / parquet_path, frame, durable=True)
        else:
            parquet_path = None
            physical_fallbacks = []
        dates = [row[names.index("mdate")] for row in rows if "mdate" in names and row[names.index("mdate")] is not None]
        receipt = {"contract": CONTRACT, "table_id": table_id, "request_id": identifier,
                   "task_id": task_id, "observed_at_utc": self.now().isoformat(),
                   "params": params, "rows": len(rows), "page_bound": bound,
                   "columns": columns, "first_date": min(dates, default=None),
                   "physical_string_fallback_fields": physical_fallbacks,
                   "last_date": max(dates, default=None), "next_cursor": cursor,
                   "raw_path": raw_path, "raw_sha256": digest, "parquet_path": parquet_path,
                   "native_data_sha256": data_digest, "pagination_contract": "native_page_content_progress_v1",
                   "parquet_sha256": sha256_file(self.root / parquet_path) if parquet_path else None,
                   "publication_time_verified": False, "publication_at_utc": None,
                   "complete_query": cursor is None, "complete_history": False,
                   "representation": "native_api_values_not_wizard_display_strings"}
        atomic_write_json(self.root / receipt_path, receipt)
        with closing(connect(self.root)) as con, con:
            con.execute("INSERT INTO pages VALUES(?,?,?,?,?,?,?,?)",
                        (identifier, task_id, table_id, raw_path, parquet_path, receipt_path, len(rows), digest))
        from downloader.tej_api_ownership import record_price_coverage
        record_price_coverage(self.root, receipt, rows, columns)
        return receipt

    def recover_saved_pages(self) -> dict:
        """Rebuild missing Parquet/receipts from exact received-response intents."""
        with closing(connect(self.root)) as con:
            rows = list(con.execute("SELECT id FROM requests WHERE state='received' AND actual_rows>0 "
                                    "AND NOT EXISTS(SELECT 1 FROM pages WHERE request_id=requests.id)"))
        recovered, failed = [], []
        for row in rows:
            intent_path = self.root / "requests" / (row[0] + ".intent.json")
            if not intent_path.is_file():
                continue  # Retain earlier unindexed probes; do not invent scope.
            intent = json.loads(intent_path.read_text())
            endpoint = intent.get("endpoint", "")
            table_id = endpoint.removeprefix(BASE + "/api/datatables/").removesuffix(".json")
            if intent.get("request_id") != row[0] or not TABLE_ID.fullmatch(table_id):
                raise TejAPIError("saved_request_identity_invalid")
            raw_path = self.root / "pages" / table_id.split("/")[1] / (row[0] + ".json")
            try:
                body = raw_path.read_bytes()
                receipt = self.ingest_page(table_id, intent["params"], json.loads(body, parse_float=Decimal),
                                           body, row[0], intent["page_bound"], intent.get("task_id"))
                recovered.append(receipt["request_id"])
            except (OSError, ValueError, TejAPIError, ArithmeticError):
                failed.append(row[0])
                with closing(connect(self.root)) as con, con:
                    con.execute("UPDATE requests SET state='local_recovery_failed' WHERE id=?", (row[0],))
        return {"provider_requests_sent": 0, "saved_pages_recovered": len(recovered),
                "saved_pages_needing_review": len(failed)}
