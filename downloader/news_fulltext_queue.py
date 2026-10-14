"""Incremental, single-owner news frontier and durable body acceptance.

Original provider Parquet retains every stock association. The frontier stores
one URL and references the accepted source partition, not another copy of the
whole FinMind/FinLab dataset. Body batches are private, compressed and immutable.
"""

from __future__ import annotations

from collections import Counter
import codecs
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import fcntl
import hashlib
import io
from itertools import islice
import json
import locale
import os
from pathlib import Path
import re
import sqlite3
import time
from urllib.parse import urlsplit
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file, table_from_records
from downloader.news_fulltext_metrics import NewsMetrics, CNYES_NATIVE_BODY_FAILURE_STAGES
from downloader.news_fulltext_policy import (CONTRACT_VERSION, NewsError, PolicyRegistry, normalize_url,
                                            REQUEST_DEFERRAL_CONTRACT_VERSION, REQUEST_DEFERRAL_CODES)
from stockagent.private_json import loads as decode_private_json
from downloader.news_fulltext_status import (
    STATUS_PROJECTION_CONTRACT_VERSION, STATUS_QUERY_TIMING_CONTRACT_VERSION,
    BULK_ERROR_COUNT_CONTRACT_VERSION, bulk_item_error_count, initialize_bulk_error_count,
    summarize_bulk_window_gaps, DISCOVERY_SUMMARY_CONTRACT_VERSION,
    discovery_summary, initialize_discovery_summary,
)
from downloader.news_fulltext_metadata_ingest import METADATA_INGEST_CONTRACT_VERSION


ARTICLE_DISPATCH_CONTRACT_VERSION = 6
ARTICLE_READY_PROBE_SQL = (
    "SELECT 1 FROM articles INDEXED BY dispatch WHERE provider=? "
    "AND state IN ('pending','retry','retry_cooldown') AND due<=? LIMIT 1"
)
CNYES_WINDOW_DISPATCH_CONTRACT_VERSION = 3
STORED_BODY_RETRY_FAIRNESS_CONTRACT_VERSION = 1
FAILURE_OBSERVATION_CONTRACT_VERSION = 2
DISCOVERY_FAILURE_OBSERVATION_CONTRACT_VERSION = 1
DISCOVERY_STATUS_PROJECTION_CONTRACT_VERSION = 1
DISCOVERY_READINESS_PROBE_CONTRACT_VERSION = 2
DISCOVERY_DUE_LOOKAHEAD_ROWS = 256
DISCOVERY_READINESS_PROBE_SQL = (
    "SELECT 1 FROM discovery INDEXED BY discovery_due_frontier "
    "WHERE provider=? AND due<=? AND superseded_by_windows_json IS NULL LIMIT "
    + str(DISCOVERY_DUE_LOOKAHEAD_ROWS + 1)
)
ARTICLE_RECHECK_DAY_CONTRACT_VERSION = 1
CNYES_UNKNOWN_BODY_RECHECK_CONTRACT_VERSION = 2
CNYES_UNKNOWN_IDENTITY_RECHECK_CONTRACT_VERSION = 1
CNYES_UNKNOWN_BODY_RECHECK_SECONDS = 300
ARTICLE_PATH_ADMISSION_CONTRACT_VERSION = 1
ACCEPTANCE_RECEIPT_DECODE_VERSION = 2
ACCEPTANCE_RECEIPT_INPUT_VERSION = 1
PARSER_REPAIR_IDENTITY_CONTRACT_VERSION = 1
ABANDONED_WORK_RECOVERY_SQL = (
    "UPDATE articles SET state='retry',due=0,lease_owner=NULL,lease_until=NULL,"
    "error_code='owner_restart' WHERE state='inflight'"
)


def parser_repair_identity(source):
    """Attest only the registry's complete current capture, not a legacy alias."""
    if (source.data_fingerprint != source.parser_repair_capture_fingerprint
            or not re.fullmatch(r'[0-9a-f]{64}', source.data_fingerprint)
            or not re.fullmatch(r'[0-9a-f]{64}', source.parser_repair_fingerprint)):
        return None
    return {'contract_version': PARSER_REPAIR_IDENTITY_CONTRACT_VERSION,
            'data_fingerprint': source.data_fingerprint,
            'parser_fingerprint': source.parser_repair_fingerprint}


def parser_repair_changed(source, previous_capture, previous_identity):
    """Suppress a replay only with matched, versioned old/new parser proofs.

    Pre-contract queues and replaced legacy capture identities retain the
    original repair behavior. Missing/malformed/mismatched evidence is never
    interpreted as proof that an actual parser change can be skipped.
    """
    if previous_capture == source.data_fingerprint:
        return False
    current = parser_repair_identity(source)
    if current and isinstance(previous_identity, str) and len(previous_identity) <= 512:
        try:
            old = json.loads(previous_identity)
        except (TypeError, ValueError):
            old = None
        if (isinstance(old, dict) and set(old) == set(current)
                and type(old['contract_version']) is int
                and old['contract_version'] == PARSER_REPAIR_IDENTITY_CONTRACT_VERSION
                and isinstance(previous_capture, str) and re.fullmatch(r'[0-9a-f]{64}', previous_capture)
                and old['data_fingerprint'] == previous_capture
                and old['parser_fingerprint'] == current['parser_fingerprint']):
            return False
    return True


def discovery_selection_sql(statement, readiness_rows):
    """Same full selection, with a bounded sparse/dense physical plan choice.

    The probe has no eligibility filters: at most 256 due rows proves the
    complete ordered selection needs to sort at most 256, even after applying
    every scope/circuit/origin guard. Dense frontiers retain priority-first
    early LIMIT. Neither index changes the actual result or polling deadline.
    """
    if 0 < readiness_rows <= DISCOVERY_DUE_LOOKAHEAD_ROWS:
        return statement.replace(' INDEXED BY discovery_ready_order ',
                                 ' INDEXED BY discovery_due_frontier ', 1)
    return statement


def sorted_acceptance_receipt_paths(root: Path):
    """Same direct-child *.json set/order, with bounded Path materialization.

    On this POSIX single-owner queue, sibling Path order equals filename
    order. Include hidden files, symlinks and even malformed .json directories
    as glob did: their original read/validation must still fail. Do not use
    entry type, size, mtime or filename as evidence that a receipt is valid.
    """
    directory = root / 'receipts'
    try:
        with os.scandir(directory) as children:
            names = sorted(child.name for child in children if child.name.endswith('.json'))
    except (FileNotFoundError, NotADirectoryError):
        return iter(())  # Same empty frontier as Path.glob, not a valid receipt.
    return (directory / name for name in names)


def read_acceptance_receipt_input(path: Path):
    """One full original read with exact default-text JSON input semantics.

    Normal UTF-8/ASCII receipts need no intermediate decoded string. Preserve
    universal newlines; NUL, non-ASCII and other locale encodings use the same
    strict default TextIO conversion on these captured bytes. In particular,
    do not newly admit BOM/UTF-16/invalid UTF-8 through JSON's byte sniffing.
    Every receipt, including committed ones, is still fully decoded below.
    """
    raw = path.read_bytes()
    encoding = codecs.lookup(locale.getpreferredencoding(False)).name
    if encoding in {'utf-8', 'ascii'} and raw.isascii() and b'\x00' not in raw:
        return raw.replace(b'\r\n', b'\n').replace(b'\r', b'\n') if b'\r' in raw else raw
    with io.TextIOWrapper(io.BytesIO(raw), encoding=None, newline=None) as reader:
        return reader.read()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def order_day(value: str) -> str:
    """Sorting only. Never relabel this day as the observed publication clock."""
    if len(value) >= 10 and value[4] == "-" and value[7] == "-" and value[:4].isdigit():
        return value[:10]
    try:
        return parsedate_to_datetime(value).date().isoformat()
    except (ValueError, TypeError, OverflowError):
        return ""


def accepted_publication_sort_day(value) -> str:
    """UTC scheduling hint from an accepted source clock, never a capture day.

    Leave unknown/naive clocks unknown. This does not modify raw metadata or
    publication evidence, and cannot make an unaccepted response a success.
    """
    if not isinstance(value, str) or not value:
        return ''
    try:
        published = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        try:
            published = parsedate_to_datetime(value)
        except (ValueError, TypeError, OverflowError):
            return ''
    try:
        return published.astimezone(timezone.utc).date().isoformat() if published.utcoffset() is not None else ''
    except (ValueError, OverflowError):
        return ''


def source_path(root: Path, relative: str) -> Path:
    candidate = (root / relative).resolve()
    if not candidate.is_relative_to(root.resolve()) or not candidate.is_file():
        raise ValueError("invalid or missing source artifact")
    return candidate


def summarize_discovery_status(db, now):
    """Exact transaction-derived counts; one shared clock, no source scan."""
    return discovery_summary(db, now)


def summarize_bulk_coverage(db, registry):
    """Read-only endpoint projection; SQLite retains the full scope graph."""
    endpoints = []
    for row in db.execute("""
        WITH coverage AS (
            SELECT c.*,
                   CASE WHEN instr(c.base_url,'?')=0 THEN c.base_url
                        ELSE substr(c.base_url,1,instr(c.base_url,'?')-1) END AS endpoint,
                   coalesce(json_array_length(s.child_urls_json),0)>0 AS superseded
            FROM bulk_api_coverage c LEFT JOIN bulk_window_splits s ON s.scope_url=c.base_url
        )
        SELECT provider,endpoint AS base_url,count(*) AS coverage_scopes,
               sum(superseded) AS superseded_scopes,sum(NOT superseded) AS leaf_scopes,
               sum(CASE WHEN NOT superseded THEN total_articles ELSE 0 END) AS total_articles,
               sum(CASE WHEN NOT superseded THEN total_pages ELSE 0 END) AS total_pages,
               max(observed_at_utc) AS observed_at_utc
        FROM coverage GROUP BY provider,endpoint ORDER BY provider,endpoint
    """):
        item = dict(row)
        source = registry.sources.get(item["provider"])
        item["enabled"] = bool(source and source.allowed and item["leaf_scopes"]
                               and item["base_url"] in (*source.wordpress_apis, *source.cnyes_apis))
        endpoints.append(item)
    return endpoints


class NewsQueue:
    def __init__(self, root: Path, registry: PolicyRegistry, *, metrics: NewsMetrics | None = None,
                 defer_initial_finite_freeze: bool = False):
        if type(defer_initial_finite_freeze) is not bool:
            raise ValueError('finite_cohort_freeze_switch_must_be_boolean')
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root, self.registry = root, registry
        self.metrics = metrics or NewsMetrics()
        self.owner_lock = (root / "owner.lock").open("a")
        try:
            fcntl.flock(self.owner_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.owner_lock.close()
            raise RuntimeError("news_fulltext_owner_already_running") from None
        self.owner = uuid.uuid4().hex
        self.metrics.owner_id = self.owner
        self.db = sqlite3.connect(root / "queue.sqlite3")
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS contract(version INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS articles(
                id BLOB PRIMARY KEY, url TEXT NOT NULL UNIQUE, host TEXT NOT NULL,
                provider TEXT NOT NULL, state TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
                due REAL NOT NULL DEFAULT 0, lease_until REAL, lease_owner TEXT,
                title TEXT, metadata_date_raw TEXT, metadata_order_day TEXT, first_seen_utc TEXT NOT NULL,
                last_seen_utc TEXT NOT NULL, error_code TEXT, captured_at_utc TEXT, last_checked_at_utc TEXT,
                body_bytes INTEGER DEFAULT 0, body_sha256 TEXT, parquet_path TEXT,
                etag TEXT, last_modified TEXT, authorization_fingerprint TEXT
            );
            CREATE INDEX IF NOT EXISTS dispatch ON articles(provider,state,due,metadata_date_raw DESC);
            CREATE INDEX IF NOT EXISTS host_state ON articles(host,state);
            CREATE TABLE IF NOT EXISTS hosts(host TEXT PRIMARY KEY,provider TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS state_metrics(
                provider TEXT NOT NULL,state TEXT NOT NULL,articles INTEGER NOT NULL,body_bytes INTEGER NOT NULL,
                PRIMARY KEY(provider,state)
            ) WITHOUT ROWID;
            CREATE TRIGGER IF NOT EXISTS metrics_insert AFTER INSERT ON articles BEGIN
                INSERT INTO state_metrics VALUES (NEW.provider,NEW.state,1,coalesce(NEW.body_bytes,0))
                ON CONFLICT(provider,state) DO UPDATE SET articles=articles+1,body_bytes=body_bytes+excluded.body_bytes;
            END;
            CREATE TRIGGER IF NOT EXISTS metrics_update AFTER UPDATE OF provider,state,body_bytes ON articles
            WHEN OLD.provider!=NEW.provider OR OLD.state!=NEW.state OR OLD.body_bytes!=NEW.body_bytes BEGIN
                UPDATE state_metrics SET articles=articles-1,body_bytes=body_bytes-coalesce(OLD.body_bytes,0)
                WHERE provider=OLD.provider AND state=OLD.state;
                INSERT INTO state_metrics VALUES (NEW.provider,NEW.state,1,coalesce(NEW.body_bytes,0))
                ON CONFLICT(provider,state) DO UPDATE SET articles=articles+1,body_bytes=body_bytes+excluded.body_bytes;
            END;
            CREATE TRIGGER IF NOT EXISTS metrics_delete AFTER DELETE ON articles BEGIN
                UPDATE state_metrics SET articles=articles-1,body_bytes=body_bytes-coalesce(OLD.body_bytes,0)
                WHERE provider=OLD.provider AND state=OLD.state;
            END;
            CREATE TABLE IF NOT EXISTS origins(
                id INTEGER PRIMARY KEY, origin_key TEXT NOT NULL UNIQUE, provider TEXT NOT NULL,
                receipt_path TEXT NOT NULL, sha256 TEXT NOT NULL, rows INTEGER NOT NULL,
                invalid_urls INTEGER NOT NULL DEFAULT 0, imported_at_utc TEXT NOT NULL,
                parquet_path TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS source_links(
                article_id BLOB NOT NULL, origin_id INTEGER NOT NULL, association_rows INTEGER NOT NULL,
                PRIMARY KEY(article_id,origin_id)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS origin_errors(
                receipt_path TEXT PRIMARY KEY, error_code TEXT NOT NULL, checked_at_utc TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS receipt_heads(
                path TEXT PRIMARY KEY, mtime_ns INTEGER NOT NULL, bytes INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS metadata_ingest_progress(
                origin_id INTEGER PRIMARY KEY, expected_rows INTEGER NOT NULL,
                processed_rows INTEGER NOT NULL, link_column TEXT NOT NULL,
                state TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS metadata_ingest_state ON metadata_ingest_progress(state,origin_id);
            CREATE TABLE IF NOT EXISTS discovery(
                url TEXT PRIMARY KEY, kind TEXT NOT NULL, provider TEXT NOT NULL, due REAL NOT NULL DEFAULT 0,
                etag TEXT, last_modified TEXT, attempts INTEGER NOT NULL DEFAULT 0, error_code TEXT,
                priority INTEGER NOT NULL DEFAULT 100, response_sha256 TEXT, parser_fingerprint TEXT,
                last_checked_at_utc TEXT, superseded_by_windows_json TEXT
            );
            CREATE TABLE IF NOT EXISTS metadata_feed_heads(
                provider TEXT NOT NULL,feed_url TEXT NOT NULL,parser_fingerprint TEXT NOT NULL,
                response_sha256 TEXT NOT NULL,receipt_path TEXT NOT NULL,rows INTEGER NOT NULL,
                response_bytes INTEGER NOT NULL,checked_at_utc TEXT NOT NULL,
                first_publication_aware TEXT,last_publication_aware TEXT,
                PRIMARY KEY(provider,feed_url)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS bulk_api_coverage(
                base_url TEXT PRIMARY KEY,provider TEXT NOT NULL,total_articles INTEGER NOT NULL,
                total_pages INTEGER NOT NULL,observed_at_utc TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS bulk_item_errors(
                request_url TEXT NOT NULL,item_number INTEGER NOT NULL,provider TEXT NOT NULL,
                article_url TEXT,error_code TEXT NOT NULL,observed_at_utc TEXT NOT NULL,
                PRIMARY KEY(request_url,item_number)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS bulk_adapter_cooldowns(
                provider TEXT PRIMARY KEY,data_fingerprint TEXT NOT NULL,not_before REAL NOT NULL,error_code TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS bulk_endpoint_cooldowns(
                provider TEXT NOT NULL,endpoint TEXT NOT NULL,data_fingerprint TEXT NOT NULL,
                not_before REAL NOT NULL,error_code TEXT NOT NULL,request_url TEXT NOT NULL,
                PRIMARY KEY(provider,endpoint)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS bulk_page_evidence(
                scope_url TEXT NOT NULL,page INTEGER NOT NULL,total INTEGER NOT NULL,ids_sha256 TEXT NOT NULL,
                observed_at_utc TEXT NOT NULL,PRIMARY KEY(scope_url,page)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS bulk_window_splits(
                scope_url TEXT PRIMARY KEY,provider TEXT NOT NULL,child_urls_json TEXT NOT NULL,
                observed_at_utc TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS bulk_window_gaps(
                scope_url TEXT PRIMARY KEY,provider TEXT NOT NULL,error_code TEXT NOT NULL,
                observed_at_utc TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS bulk_page_progress ON bulk_page_evidence(scope_url,total,ids_sha256,page);
            CREATE TABLE IF NOT EXISTS counters(key TEXT PRIMARY KEY,value INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS accepted_batches(path TEXT PRIMARY KEY,receipt_path TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS accepted_batches_receipt_path ON accepted_batches(receipt_path,path);
            CREATE TABLE IF NOT EXISTS queue_settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS finite_cohorts(
                provider TEXT PRIMARY KEY,urls INTEGER NOT NULL,urls_sha256 TEXT NOT NULL,
                created_at_utc TEXT NOT NULL,receipt_path TEXT NOT NULL,receipt_sha256 TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS finite_cohort_members(
                provider TEXT NOT NULL,article_id BLOB NOT NULL,url TEXT NOT NULL,
                PRIMARY KEY(provider,article_id),UNIQUE(provider,url)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS finite_capture_supplements(
                provider TEXT NOT NULL,scope_sha256 TEXT NOT NULL,urls INTEGER NOT NULL,
                receipt_path TEXT NOT NULL,receipt_sha256 TEXT NOT NULL,
                PRIMARY KEY(provider,scope_sha256)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS finite_supplement_members(
                provider TEXT NOT NULL,article_id BLOB NOT NULL,url TEXT NOT NULL,scope_sha256 TEXT NOT NULL,
                PRIMARY KEY(provider,article_id),UNIQUE(provider,url)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS finite_source_outcomes(
                article_id BLOB PRIMARY KEY,provider TEXT NOT NULL,outcome TEXT NOT NULL,
                observed_at_utc TEXT NOT NULL,recorded_at_utc TEXT NOT NULL,
                source_data_fingerprint TEXT NOT NULL,receipt_path TEXT NOT NULL,receipt_sha256 TEXT NOT NULL
            ) WITHOUT ROWID;
            CREATE INDEX IF NOT EXISTS finite_source_outcomes_provider ON finite_source_outcomes(provider,article_id);
        """)
        contract = self.db.execute("SELECT version FROM contract").fetchone()
        if contract and contract[0] != CONTRACT_VERSION:
            self.close()
            raise ValueError("incompatible news fulltext queue; use a versioned root")
        if not contract:
            self.db.execute("INSERT INTO contract VALUES (?)", (CONTRACT_VERSION,))
        if "metadata_order_day" not in {row[1] for row in self.db.execute("PRAGMA table_info(articles)")}:
            self.db.execute("ALTER TABLE articles ADD COLUMN metadata_order_day TEXT")
        # A capture-contract change repairs only missing valid metadata days.
        # host_state otherwise scans millions of already repaired/stored rows
        # on every parser update. Keep the tiny eligibility set indexed; row
        # values and the repair predicate remain exactly the same.
        with self.metrics.measure('metadata_order_repair_index_initialization'):
            self.db.execute("CREATE INDEX IF NOT EXISTS metadata_order_repair ON articles(host) "
                            "WHERE metadata_order_day IS NULL AND metadata_date_raw LIKE '____-__-__%'")
        if "last_checked_at_utc" not in {row[1] for row in self.db.execute("PRAGMA table_info(articles)")}:
            self.db.execute("ALTER TABLE articles ADD COLUMN last_checked_at_utc TEXT")
        if "priority" not in {row[1] for row in self.db.execute("PRAGMA table_info(discovery)")}:
            self.db.execute("ALTER TABLE discovery ADD COLUMN priority INTEGER NOT NULL DEFAULT 100")
        discovery_columns = {row[1] for row in self.db.execute("PRAGMA table_info(discovery)")}
        for column in ("response_sha256", "parser_fingerprint", "last_checked_at_utc", "superseded_by_windows_json"):
            if column not in discovery_columns:
                self.db.execute(f"ALTER TABLE discovery ADD COLUMN {column} TEXT")
        self.db.execute("CREATE INDEX IF NOT EXISTS dispatch_latest ON articles(provider,state,metadata_order_day DESC,due)")
        # Only runnable-state rows enter this partial index. Unknown sources
        # and already complete history do not enlarge every dispatch lookup.
        # Work kind precedes date: a newer stored recheck must not indefinitely
        # outrank an older missing body. Lease/due filters remain mandatory.
        self.db.execute("CREATE INDEX IF NOT EXISTS dispatch_work_kind ON articles(provider,state,"
                        "(body_sha256 IS NULL) DESC,metadata_order_day DESC,due) "
                        "WHERE state IN ('pending','retry','retry_cooldown')")
        # A small due-error lane, not another index of all complete history.
        # Old transient failures must not wait behind every newer pending URL.
        self.db.execute("CREATE INDEX IF NOT EXISTS dispatch_missing_retry ON articles(provider,due,metadata_order_day DESC) "
                        "WHERE body_sha256 IS NULL AND error_code IS NOT NULL AND state IN ('retry','retry_cooldown')")
        # Index only actual stored-body failures, not every complete body or
        # ordinary due=0 refresh. Recover oldest deadlines without a hot sort.
        with self.metrics.measure('stored_retry_index_initialization'):
            self.db.execute("CREATE INDEX IF NOT EXISTS dispatch_stored_retry ON articles(provider,due,metadata_order_day DESC) "
                            "WHERE body_sha256 IS NOT NULL AND error_code IS NOT NULL AND state IN ('retry','retry_cooldown')")
        # Preserve latest-first network recovery without scanning thousands of
        # newer parser failures; those use their bounded independent turn.
        self.db.execute("CREATE INDEX IF NOT EXISTS dispatch_nonparser_retry ON articles(provider,state,"
                        "(body_sha256 IS NULL) DESC,metadata_order_day DESC,due) "
                        "WHERE state IN ('retry','retry_cooldown') AND (error_code IS NULL OR "
                        "error_code NOT IN ('body_not_found','article_attribution_not_verified'))")
        self.db.execute("CREATE INDEX IF NOT EXISTS stored_bodies ON articles(provider) WHERE body_sha256 IS NOT NULL")
        # Ownership recovery is global, including unregistered providers.
        # Provider-leading dispatch indexes cannot satisfy state='inflight';
        # this small exact-state index avoids scanning every historical body
        # at each restart. First installation is a measured one-time scan.
        with self.metrics.measure('abandoned_work_index_initialization'):
            self.db.execute("CREATE INDEX IF NOT EXISTS abandoned_work ON articles(id) WHERE state='inflight'")
        self.db.execute("INSERT OR REPLACE INTO queue_settings VALUES ('article_dispatch_contract_version',?)",
                        (str(ARTICLE_DISPATCH_CONTRACT_VERSION),))
        self.db.execute("INSERT OR REPLACE INTO queue_settings VALUES ('article_request_deferral_contract_version',?)",
                        (str(REQUEST_DEFERRAL_CONTRACT_VERSION),))
        self.db.execute("INSERT OR REPLACE INTO queue_settings VALUES ('article_recheck_day_contract_version',?)",
                        (str(ARTICLE_RECHECK_DAY_CONTRACT_VERSION),))
        self.db.execute("INSERT OR REPLACE INTO queue_settings VALUES ('article_path_admission_contract_version',?)",
                        (str(ARTICLE_PATH_ADMISSION_CONTRACT_VERSION),))
        # Match dispatch order; avoid a full historical scan/sort per request.
        self.db.execute("CREATE INDEX IF NOT EXISTS discovery_ready_order ON discovery(provider,"
                        "(kind='feed') DESC,(kind IN ('wordpress_posts','cnyes_news','rss_fulltext')) DESC,priority,due,url) "
                        "WHERE superseded_by_windows_json IS NULL")
        # Ordering and readiness are different questions. The priority index
        # cannot range-seek due after its kind/priority prefixes; an idle
        # publisher would scan its entire historical frontier on every round.
        # This exact existence probe is not a metrics gauge or cached deadline.
        with self.metrics.measure('discovery_due_index_initialization'):
            self.db.execute("CREATE INDEX IF NOT EXISTS discovery_due_frontier ON discovery(provider,due) "
                            "WHERE superseded_by_windows_json IS NULL")
        if not self.db.execute("SELECT 1 FROM state_metrics LIMIT 1").fetchone():
            self.db.execute("INSERT INTO state_metrics SELECT provider,state,count(*),coalesce(sum(body_bytes),0) "
                            "FROM articles GROUP BY provider,state")
        if not self.db.execute("SELECT 1 FROM hosts LIMIT 1").fetchone():
            self.db.execute("INSERT INTO hosts SELECT host,provider FROM articles GROUP BY host")
        self.db.commit()
        from downloader.news_fulltext_cnyes_work import initialize as initialize_cnyes_work
        initialize_cnyes_work(self)
        self.initialize_stored_body_metrics()
        with self.metrics.measure('bulk_item_error_count_initialization'):
            initialize_bulk_error_count(self.db)
        with self.metrics.measure('discovery_summary_initialization'):
            initialize_discovery_summary(self.db)
        # Only after acquiring exclusive ownership can abandoned work be requeued.
        with self.metrics.measure('abandoned_work_recovery'):
            self.db.execute(ABANDONED_WORK_RECOVERY_SQL)
            self.db.commit()
        self.recover_acceptance()
        from downloader.news_fulltext_cnyes_history import initialize as initialize_cnyes_history
        initialize_cnyes_history(self)
        from downloader.news_fulltext_cnyes_index import initialize as initialize_cnyes_index
        initialize_cnyes_index(self)
        self.reconcile_policy()
        self.reconcile_successful_feed_rechecks()
        try:
            self.sync_finite_cohorts(freeze_new=not defer_initial_finite_freeze)
        except Exception:
            self.close()
            raise
        self.migrate_cnyes_endpoint_circuits()
        from downloader.news_fulltext_urls import initialize_original_urls
        initialize_original_urls(self)
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO queue_settings VALUES ('current_owner_id',?)", (self.owner,))

    def close(self):
        from downloader.news_fulltext_metadata_ingest import close_readers
        close_readers(self)
        entries = getattr(self, '_cached_cnyes_inventory_entries', None)
        if entries is not None:
            entries.close()
            self._cached_cnyes_inventory_entries = None
        self.db.close()
        self.owner_lock.close()

    def initialize_stored_body_metrics(self):
        """One transactional migration, then exact O(providers) head counts.

        Count stored URL heads, not versions or states: a failed recheck can
        still have a stored body, and replacing its SHA must not add a head.
        A crash rolls back both the seed and its version marker. Receipts and
        source rows are untouched; recovery subsequently updates the triggers.
        """
        existed = self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='stored_body_metrics'").fetchone()
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS stored_body_metrics(
                provider TEXT PRIMARY KEY,heads INTEGER NOT NULL CHECK(heads>=0)
            ) WITHOUT ROWID;
            CREATE TRIGGER IF NOT EXISTS stored_heads_insert AFTER INSERT ON articles
            WHEN NEW.body_sha256 IS NOT NULL BEGIN
                INSERT INTO stored_body_metrics VALUES (NEW.provider,1)
                ON CONFLICT(provider) DO UPDATE SET heads=heads+1;
            END;
            CREATE TRIGGER IF NOT EXISTS stored_heads_update AFTER UPDATE OF provider,body_sha256 ON articles
            WHEN OLD.provider!=NEW.provider OR (OLD.body_sha256 IS NULL)!=(NEW.body_sha256 IS NULL) BEGIN
                UPDATE stored_body_metrics SET heads=heads-1
                WHERE provider=OLD.provider AND OLD.body_sha256 IS NOT NULL;
                INSERT INTO stored_body_metrics SELECT NEW.provider,1 WHERE NEW.body_sha256 IS NOT NULL
                ON CONFLICT(provider) DO UPDATE SET heads=heads+1;
            END;
            CREATE TRIGGER IF NOT EXISTS stored_heads_delete AFTER DELETE ON articles
            WHEN OLD.body_sha256 IS NOT NULL BEGIN
                UPDATE stored_body_metrics SET heads=heads-1 WHERE provider=OLD.provider;
            END;
        """)
        with self.db:
            marker = self.db.execute("SELECT value FROM queue_settings WHERE key='stored_body_metrics_version'").fetchone()
            if not existed or not marker or marker[0] != '1':
                self.db.execute("DELETE FROM stored_body_metrics")
                self.db.execute("INSERT INTO stored_body_metrics SELECT provider,count(*) FROM articles "
                                "WHERE body_sha256 IS NOT NULL GROUP BY provider")
                self.db.execute("INSERT OR REPLACE INTO queue_settings VALUES ('stored_body_metrics_version','1')")

    def sync_finite_cohorts(self, *, freeze_new=True):
        from downloader.news_fulltext_cohorts import sync_cohorts
        sync_cohorts(self, freeze_new=freeze_new)

    def migrate_cnyes_endpoint_circuits(self):
        """Retain old parser evidence/deadlines without blocking healthy categories.

        These are representation failures, NOT HTTP/source refusals. The latter
        remain in NewsHTTP's independent provider-wide persisted cooldowns.
        Migration runs once, rather than scanning discovery on every dispatch.
        """
        source = self.registry.sources.get("cnyes")
        if not source or self.db.execute("SELECT 1 FROM queue_settings WHERE key='cnyes_endpoint_circuits_v1'").fetchone():
            return
        with self.db:
            rows = self.db.execute("SELECT url,due,error_code FROM discovery WHERE provider='cnyes' AND kind='cnyes_news' "
                                   "AND error_code IN ('bulk_all_items_unusable','bulk_pagination_no_progress','bulk_pagination_totals_invalid') "
                                   "AND due>? ORDER BY due", (time.time(),)).fetchall()
            for row in rows:
                endpoint = row["url"].split("?", 1)[0]
                if endpoint in source.cnyes_apis:
                    self.db.execute("INSERT OR REPLACE INTO bulk_endpoint_cooldowns VALUES (?,?,?,?,?,?)",
                                    (source.provider, endpoint, source.data_fingerprint, row["due"], row["error_code"], row["url"]))
            self.db.execute("INSERT INTO queue_settings VALUES ('cnyes_endpoint_circuits_v1','1')")

    def bulk_allowed(self, source, now=None):
        if not (source.wordpress_apis or source.cnyes_apis or source.fulltext_feeds):
            return False
        if source.cnyes_apis:
            return True  # Indexed per-endpoint circuits filter BEFORE LIMIT.
        cooldown = self.db.execute("SELECT data_fingerprint,not_before FROM bulk_adapter_cooldowns WHERE provider=?",
                                   (source.provider,)).fetchone()
        return not cooldown or cooldown[0] != source.data_fingerprint or cooldown[1] <= (time.time() if now is None else now)

    def counter(self, key: str, value: int):
        self.db.execute("INSERT INTO counters VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=value+excluded.value",
                        (key, value))

    def gauge(self, key: str, value: int):
        self.db.execute("INSERT OR REPLACE INTO counters VALUES (?,?)", (key, value))

    def add_articles(self, rows: list[dict], *, origin_id: int | None = None):
        aggregated: dict[bytes, dict] = {}
        invalid = 0
        now = utc_now()
        host_policy = {}
        frozen_sources = {r[0] for r in self.db.execute("SELECT provider FROM finite_cohorts")}
        for row in rows:
            try:
                url = normalize_url(row.get("url"))
            except NewsError:
                invalid += 1
                continue
            key = hashlib.sha256(url.encode()).digest()
            if key in aggregated:
                aggregated[key]["associations"] += 1
                continue
            host = urlsplit(url).hostname
            if host not in host_policy:
                host_policy[host] = self.registry.match(url)
            source = host_policy[host]
            # Match fetch_article's existing namespace gate before finite
            # membership can hide non-article metadata as unreviewed work.
            # Upserts below retain all existing states, deadlines and heads.
            path_excluded = bool(source and source.allowed and source.article_path_pattern
                                 and not re.search(source.article_path_pattern, urlsplit(url).path))
            aggregated[key] = {"key": key, "url": url, "host": host,
                               "provider": source.provider if source else "unregistered:" + host,
                               "state": ("unsupported_article_path" if path_excluded else
                                         "waiting_cohort" if source and source.allowed and source.acquisition_mode == "finite"
                                         and source.provider in frozen_sources else
                                         "pending" if source and source.allowed else "awaiting_authorization"),
                               "error": "unsupported_article_path" if path_excluded else None,
                               "title": str(row.get("title") or ""),
                               "date": str(row.get("date") or ""), "associations": 1}
        self.db.executemany("""
            INSERT INTO articles(id,url,host,provider,state,error_code,title,metadata_date_raw,metadata_order_day,first_seen_utc,last_seen_utc)
            VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET
                last_seen_utc=excluded.last_seen_utc,
                metadata_date_raw=max(coalesce(articles.metadata_date_raw,''),excluded.metadata_date_raw),
                metadata_order_day=max(coalesce(articles.metadata_order_day,''),excluded.metadata_order_day),
                title=CASE WHEN excluded.title!='' THEN excluded.title ELSE articles.title END
        """, ((r["key"], r["url"], r["host"], r["provider"], r["state"], r["error"], r["title"], r["date"], order_day(r["date"]), now, now)
              for r in aggregated.values()))
        self.db.executemany("INSERT OR IGNORE INTO hosts VALUES (?,?)",
                            ((host, source.provider if source else "unregistered:" + host) for host, source in host_policy.items()))
        if origin_id is not None:
            self.db.executemany("INSERT INTO source_links VALUES (?,?,?) ON CONFLICT(article_id,origin_id) "
                                "DO UPDATE SET association_rows=association_rows+excluded.association_rows",
                                ((r["key"], origin_id, r["associations"]) for r in aggregated.values()))
        if 'news.google.com' in host_policy:
            from downloader.news_fulltext_urls import refresh_known_original_metadata
            refresh_known_original_metadata(self, aggregated.values())
        if {'m.cnyes.com', 'amp-news.cnyes.com', 'news.cnyes.com'} & host_policy.keys():
            from downloader.news_fulltext_cnyes_work import admit_native_lookup, classify
            for row in aggregated.values():
                if (row['host'] in {'m.cnyes.com', 'amp-news.cnyes.com'}
                        or row['host'] == 'news.cnyes.com' and row['url'].endswith('?exp=a')):
                    candidate = {'id': row['key'], 'url': row['url'], 'provider': row['provider']}
                    classify(self, candidate)
                    if row['host'] == 'amp-news.cnyes.com':
                        admit_native_lookup(self, candidate)
        return len(aggregated), invalid

    def ingest_receipt(self, root: Path, receipt_path: Path, *, provider: str, link_column: str,
                       maximum_wall_seconds=None, stop=None):
        from downloader.news_fulltext_metadata_ingest import ingest_receipt
        return ingest_receipt(self, root, receipt_path, provider=provider, link_column=link_column,
                              maximum_wall_seconds=maximum_wall_seconds, stop=stop)

    def ingest_local(self, finlab_root: Path, finmind_root: Path):
        return self.ingest_local_slice(finlab_root, finmind_root)['imported']

    def ingest_local_slice(self, finlab_root: Path, finmind_root: Path, *, maximum_wall_seconds=None, stop=None):
        from downloader.news_fulltext_metadata_ingest import ingest_local_slice
        return ingest_local_slice(self, finlab_root, finmind_root,
                                  maximum_wall_seconds=maximum_wall_seconds, stop=stop)

    def recheck_cnyes_mobile_identity_views(self, previous_fingerprint):
        """One pinned HTML repair, preserving failures/attempts and receipts."""
        from downloader.news_fulltext_publishers import (
            CNYES_MOBILE_CANONICAL_V1_CAPTURE_FINGERPRINT,
            CNYES_MOBILE_CANONICAL_V2_CAPTURE_FINGERPRINT,
            CNYES_MOBILE_CANONICAL_V5_CAPTURE_FINGERPRINT,
        )

        known_contracts = {
            CNYES_MOBILE_CANONICAL_V1_CAPTURE_FINGERPRINT: ('cnyes_completed_mobile_stream_repair_v1:', '', 'pinned_mobile_stream_identity_rechecks'),
            CNYES_MOBILE_CANONICAL_V2_CAPTURE_FINGERPRINT: ('cnyes_completed_mobile_stream_repair_v1:', '', 'pinned_mobile_stream_identity_rechecks'),
            CNYES_MOBILE_CANONICAL_V5_CAPTURE_FINGERPRINT: ('cnyes_mobile_query_view_repair_v1:', 'exp=a', 'pinned_mobile_query_identity_rechecks'),
        }
        source = self.registry.sources.get('cnyes')
        if (not source or not source.allowed
                or previous_fingerprint not in known_contracts
                or previous_fingerprint == source.data_fingerprint):
            return
        prefix, supported_query, counter = known_contracts[previous_fingerprint]
        marker = prefix + source.data_fingerprint
        if self.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (marker,)).fetchone():
            return
        with self.db:
            rows = self.db.execute("SELECT id,url FROM articles INDEXED BY dispatch "
                                   "WHERE provider='cnyes' AND host='m.cnyes.com' "
                                   "AND state='article_identity_not_verified' "
                                   "AND error_code='article_identity_not_verified' "
                                   "AND body_sha256 IS NULL AND lease_owner IS NULL AND lease_until IS NULL ORDER BY url").fetchall()
            selected = [(key, url) for key, url in rows
                        if re.fullmatch(r'/news/id/[0-9]+', urlsplit(url).path)
                        and urlsplit(url).netloc == 'm.cnyes.com'
                        and urlsplit(url).scheme in {'http', 'https'}
                        and urlsplit(url).query == supported_query and not urlsplit(url).fragment]
            # No stored head, lease, source refusal or retry deadline matches
            # this terminal parser-only cohort. Keep original error/attempt/
            # check clock; the current parser still has to accept each body.
            self.db.executemany("UPDATE articles SET state='pending',due=0 WHERE id=?",
                                ((key,) for key, _ in selected))
            self.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, json.dumps({
                'old_source_fingerprint': previous_fingerprint,
                'new_source_fingerprint': source.data_fingerprint,
                'previous_outcome': 'article_identity_not_verified', 'url_count': len(selected),
                'urls_sha256': hashlib.sha256('\n'.join(url for _, url in selected).encode()).hexdigest(),
                'observed_at_utc': utc_now(), 'stored_heads_or_source_refusals_reset': False,
                'accepted_as_bodies': False}, sort_keys=True)))
            self.metrics.count('cnyes', counter, len(selected))

    def recheck_cnyes_mobile_body_declarations(self, previous_fingerprint):
        from downloader.news_fulltext_publishers import (
            CNYES_MOBILE_CANONICAL_V3_CAPTURE_FINGERPRINT,
            CNYES_MOBILE_CANONICAL_V4_CAPTURE_FINGERPRINT,
        )
        known_contracts = {
            CNYES_MOBILE_CANONICAL_V3_CAPTURE_FINGERPRINT: ('paragraph_marker', 'pinned_mobile_paragraph_marker_rechecks'),
            CNYES_MOBILE_CANONICAL_V4_CAPTURE_FINGERPRINT: ('schema_caption', 'pinned_mobile_schema_caption_rechecks'),
        }
        source = self.registry.sources.get('cnyes')
        if (not source or not source.allowed or previous_fingerprint not in known_contracts
                or source.data_fingerprint == previous_fingerprint):
            return
        kind, counter = known_contracts[previous_fingerprint]
        marker = 'cnyes_mobile_' + kind + '_repair_v1:' + source.data_fingerprint
        if self.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (marker,)).fetchone():
            return
        with self.db:
            rows = self.db.execute("SELECT id,url FROM articles INDEXED BY dispatch WHERE provider='cnyes' "
                                   "AND host='m.cnyes.com' AND state='body_not_found' AND error_code='body_not_found' "
                                   "AND body_sha256 IS NULL AND lease_owner IS NULL AND lease_until IS NULL ORDER BY url").fetchall()
            selected = [(key, url) for key, url in rows if re.fullmatch(r'/news/id/[0-9]+', urlsplit(url).path)
                        and urlsplit(url).scheme in {'http', 'https'} and urlsplit(url).netloc == 'm.cnyes.com'
                        and not urlsplit(url).query and not urlsplit(url).fragment]
            self.db.executemany("UPDATE articles SET state='pending',due=0 WHERE id=?", ((key,) for key, _ in selected))
            self.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, json.dumps({
                'old_source_fingerprint': previous_fingerprint, 'new_source_fingerprint': source.data_fingerprint,
                'url_count': len(selected), 'observed_at_utc': utc_now(), 'accepted_as_bodies': False,
                'urls_sha256': hashlib.sha256('\n'.join(url for _, url in selected).encode()).hexdigest(),
                'attempts_errors_and_last_http_checks_preserved': True,
                'source_refusals_and_stored_heads_reset': False}, sort_keys=True)))
            self.metrics.count('cnyes', counter, len(selected))

    def recheck_cnyes_native_v6_views(self, previous_fingerprint):
        """One indexed parser-only cohort; retain every old failure witness."""
        from downloader.news_fulltext_publishers import (
            CNYES_MOBILE_CANONICAL_V5_CAPTURE_FINGERPRINT,
            CNYES_MOBILE_CANONICAL_V6_CAPTURE_FINGERPRINT,
            CNYES_MOBILE_CANONICAL_V7_CAPTURE_FINGERPRINT,
            cnyes_supported_native_alias_view,
        )
        source = self.registry.sources.get('cnyes')
        if not source or not source.allowed:
            return
        marker = 'cnyes_native_v6_view_repair_v1:' + source.data_fingerprint
        if self.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (marker,)).fetchone():
            return
        cutoff = None
        if previous_fingerprint != CNYES_MOBILE_CANONICAL_V6_CAPTURE_FINGERPRINT:
            # Recover only the evidenced first v7 rollout that missed this
            # migration. Do not assume arbitrary old fingerprints equivalent.
            if not previous_fingerprint == source.data_fingerprint == CNYES_MOBILE_CANONICAL_V7_CAPTURE_FINGERPRINT:
                return
            legacy = self.db.execute('SELECT value FROM queue_settings WHERE key=?', (
                'cnyes_mobile_query_view_repair_v1:' + CNYES_MOBILE_CANONICAL_V6_CAPTURE_FINGERPRINT,)).fetchone()
            current = self.db.execute('SELECT value FROM queue_settings WHERE key=?', (
                'parser_repair_retry_lane_v1:cnyes:' + source.data_fingerprint + ':' + str(ARTICLE_DISPATCH_CONTRACT_VERSION),)).fetchone()
            try:
                legacy, current = json.loads(legacy[0]), json.loads(current[0])
                legacy_clock = datetime.fromisoformat(legacy['observed_at_utc'])
                clock = datetime.fromisoformat(current['observed_at_utc'])
                if (legacy['old_source_fingerprint'] != CNYES_MOBILE_CANONICAL_V5_CAPTURE_FINGERPRINT
                        or legacy['new_source_fingerprint'] != CNYES_MOBILE_CANONICAL_V6_CAPTURE_FINGERPRINT
                        or legacy['stored_heads_or_source_refusals_reset'] is not False
                        or legacy['accepted_as_bodies'] is not False
                        or current['source_fingerprint'] != source.data_fingerprint
                        or current['dispatch_contract_version'] != ARTICLE_DISPATCH_CONTRACT_VERSION
                        or current['original_error_clock_attempts_due_lease_and_body_preserved'] is not True
                        or current['http_refusals_or_unobserved_first_downloads_changed'] is not False
                        or legacy_clock.tzinfo is None or clock.tzinfo is None
                        or not legacy_clock < clock <= datetime.now(timezone.utc)):
                    return
                cutoff = current['observed_at_utc']
            except (TypeError, ValueError, KeyError, OverflowError):
                return
        elif source.data_fingerprint == previous_fingerprint:
            return
        with self.db:
            rows = self.db.execute("SELECT id,url FROM articles INDEXED BY dispatch "
                "WHERE provider='cnyes' AND host IN ('m.cnyes.com','news.cnyes.com') "
                "AND state IN ('body_not_found','article_identity_not_verified') AND error_code=state "
                "AND last_checked_at_utc IS NOT NULL AND body_sha256 IS NULL "
                "AND lease_owner IS NULL AND lease_until IS NULL "
                + ("AND julianday(last_checked_at_utc)<julianday(?) " if cutoff else '')
                + "ORDER BY url", (cutoff,) if cutoff else ()).fetchall()
            selected = [(key, url) for key, url in rows
                        if cnyes_supported_native_alias_view(urlsplit(url))]
            self.db.executemany("UPDATE articles SET state='retry',due=0 WHERE id=?",
                                ((key,) for key, _ in selected))
            self.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, json.dumps({
                'old_source_fingerprint': CNYES_MOBILE_CANONICAL_V6_CAPTURE_FINGERPRINT,
                'new_source_fingerprint': source.data_fingerprint,
                'previous_queue_fingerprint': previous_fingerprint,
                'recovered_missed_same_contract_rollout': cutoff is not None,
                'failures_before_utc': cutoff, 'target_state': 'retry',
                'url_count': len(selected), 'observed_at_utc': utc_now(),
                'urls_sha256': hashlib.sha256('\n'.join(url for _, url in selected).encode()).hexdigest(),
                'attempts_errors_and_last_http_checks_preserved': True,
                'source_refusals_and_stored_heads_reset': False,
                'accepted_as_bodies': False}, sort_keys=True)))
            self.metrics.count('cnyes', 'pinned_native_v6_parser_rechecks', len(selected))

    def recheck_cnyes_native_v7_titles(self, previous_fingerprint):
        """Recheck a pinned title-identity repair, never reset refusals/heads."""
        from downloader.news_fulltext_publishers import (
            CNYES_MOBILE_CANONICAL_V7_CAPTURE_FINGERPRINT, cnyes_supported_native_alias_view,
        )
        source = self.registry.sources.get('cnyes')
        if (not source or not source.allowed or previous_fingerprint != CNYES_MOBILE_CANONICAL_V7_CAPTURE_FINGERPRINT
                or source.data_fingerprint == previous_fingerprint):
            return
        marker = 'cnyes_native_v7_title_repair_v1:' + source.data_fingerprint
        if self.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (marker,)).fetchone():
            return
        with self.db:
            rows = self.db.execute("SELECT id,url FROM articles INDEXED BY dispatch "
                "WHERE provider='cnyes' AND host IN ('m.cnyes.com','news.cnyes.com') "
                "AND state='article_identity_not_verified' AND error_code=state "
                "AND last_checked_at_utc IS NOT NULL AND body_sha256 IS NULL "
                "AND lease_owner IS NULL AND lease_until IS NULL ORDER BY url").fetchall()
            selected = [(key, url) for key, url in rows if cnyes_supported_native_alias_view(urlsplit(url))]
            self.db.executemany("UPDATE articles SET state='retry',due=0 WHERE id=?", ((key,) for key, _ in selected))
            self.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, json.dumps({
                'old_source_fingerprint': previous_fingerprint, 'new_source_fingerprint': source.data_fingerprint,
                'previous_outcome': 'article_identity_not_verified', 'target_state': 'retry',
                'url_count': len(selected), 'observed_at_utc': utc_now(),
                'urls_sha256': hashlib.sha256('\n'.join(url for _, url in selected).encode()).hexdigest(),
                'attempts_errors_and_last_http_checks_preserved': True,
                'source_refusals_and_stored_heads_reset': False, 'accepted_as_bodies': False}, sort_keys=True)))
            self.metrics.count('cnyes', 'pinned_native_v7_title_rechecks', len(selected))

    def recheck_cnyes_native_empty_signature(self, previous_fingerprint):
        """One pinned author-witness repair; not a refusal/deadline reset."""
        from downloader.news_fulltext_publishers import (
            CNYES_MOBILE_CANONICAL_V15_CAPTURE_FINGERPRINT, cnyes_supported_native_alias_view,
        )
        source = self.registry.sources.get('cnyes')
        if (not source or not source.allowed or previous_fingerprint != CNYES_MOBILE_CANONICAL_V15_CAPTURE_FINGERPRINT
                or source.data_fingerprint == previous_fingerprint):
            return
        marker = 'cnyes_native_v15_empty_signature_repair_v1:' + source.data_fingerprint
        if self.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (marker,)).fetchone():
            return
        with self.db:
            rows = self.db.execute("SELECT id,url FROM articles INDEXED BY dispatch "
                "WHERE provider='cnyes' AND host IN ('m.cnyes.com','news.cnyes.com') "
                "AND state='article_attribution_not_verified' AND error_code=state "
                "AND last_checked_at_utc IS NOT NULL AND body_sha256 IS NULL AND due<=? "
                "AND lease_owner IS NULL AND lease_until IS NULL ORDER BY url", (time.time(),)).fetchall()
            selected = [(key, url) for key, url in rows if cnyes_supported_native_alias_view(urlsplit(url))]
            self.db.executemany("UPDATE articles SET state='retry',due=0 WHERE id=?", ((key,) for key, _ in selected))
            self.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, json.dumps({
                'old_source_fingerprint': previous_fingerprint, 'new_source_fingerprint': source.data_fingerprint,
                'previous_outcome': 'article_attribution_not_verified', 'target_state': 'retry',
                'url_count': len(selected), 'observed_at_utc': utc_now(),
                'urls_sha256': hashlib.sha256('\n'.join(url for _, url in selected).encode()).hexdigest(),
                'attempts_errors_and_last_http_checks_preserved': True,
                'source_refusals_stored_heads_and_future_deadlines_reset': False,
                'accepted_as_bodies': False}, sort_keys=True)))
            self.metrics.count('cnyes', 'pinned_native_v15_empty_signature_rechecks', len(selected))

    def recheck_cnyes_native_v16_declarations(self, previous_fingerprint):
        """One pinned native parser/source-update recheck, never permission.

        Current public 5418903 proves the new list-marker comparison; 5463919
        now passes native identity without relaxing that gate. Retry eligible
        old negative declarations once, retaining failures, attempts, stored
        bodies, leases and all future source/circuit deadlines.
        """
        from downloader.news_fulltext_publishers import (
            CNYES_MOBILE_CANONICAL_V16_CAPTURE_FINGERPRINT, cnyes_supported_native_alias_view,
        )
        source = self.registry.sources.get('cnyes')
        if (not source or not source.allowed or previous_fingerprint != CNYES_MOBILE_CANONICAL_V16_CAPTURE_FINGERPRINT
                or source.data_fingerprint == previous_fingerprint):
            return
        marker = 'cnyes_native_v16_declaration_repair_v1:' + source.data_fingerprint
        if self.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (marker,)).fetchone():
            return
        with self.db:
            rows = self.db.execute("SELECT id,url,state FROM articles INDEXED BY dispatch "
                "WHERE provider='cnyes' AND host IN ('m.cnyes.com','news.cnyes.com') "
                "AND state IN ('article_identity_not_verified','body_not_found') AND error_code=state "
                "AND last_checked_at_utc IS NOT NULL AND body_sha256 IS NULL AND due<=? "
                "AND lease_owner IS NULL AND lease_until IS NULL ORDER BY url", (time.time(),)).fetchall()
            selected = [row for row in rows if cnyes_supported_native_alias_view(urlsplit(row['url']))]
            self.db.executemany("UPDATE articles SET state='retry',due=0 WHERE id=?", ((row['id'],) for row in selected))
            self.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, json.dumps({
                'old_source_fingerprint': previous_fingerprint, 'new_source_fingerprint': source.data_fingerprint,
                'previous_outcomes': ['article_identity_not_verified', 'body_not_found'], 'target_state': 'retry',
                'url_count': len(selected), 'observed_at_utc': utc_now(),
                'urls_sha256': hashlib.sha256('\n'.join(row['url'] for row in selected).encode()).hexdigest(),
                'attempts_errors_and_last_http_checks_preserved': True,
                'source_refusals_stored_heads_and_future_deadlines_reset': False,
                'accepted_as_bodies': False}, sort_keys=True)))
            self.metrics.count('cnyes', 'pinned_native_v16_declaration_rechecks', len(selected))

    def reconcile_policy(self):
        self.migrate_cnyes_window_dispatch()
        # Policy/expiry is evaluated by the registry on every owner start.
        # Unchanged policy does not justify rescanning millions of URL rows.
        fingerprint = hashlib.sha256(json.dumps([
            (source.provider, source.allowed, source.domains, source.data_fingerprint)
            for source in self.registry.sources.values()
        ], sort_keys=True).encode()).hexdigest()
        previous = self.db.execute("SELECT value FROM queue_settings WHERE key='policy_fingerprint'").fetchone()
        prior_sources = dict(self.db.execute("SELECT key,value FROM queue_settings WHERE key LIKE 'source_capture_fingerprint:%'"))
        prior_allowed = dict(self.db.execute("SELECT key,value FROM queue_settings WHERE key LIKE 'source_capture_allowed:%'"))
        prior_parsers = dict(self.db.execute("SELECT key,value FROM queue_settings WHERE key LIKE 'source_parser_repair_fingerprint:%'"))
        parser_settings = []
        for provider, source in self.registry.sources.items():
            identity = parser_repair_identity(source)
            if identity is not None:
                key = 'source_parser_repair_fingerprint:' + provider
                value = json.dumps(identity, sort_keys=True)
                if prior_parsers.get(key) != value:
                    parser_settings.append((key, value))
        scheduling_only_changes = frozenset(
            provider for provider, source in self.registry.sources.items()
            if prior_sources.get('source_capture_fingerprint:' + provider) != source.data_fingerprint
            and not parser_repair_changed(source, prior_sources.get('source_capture_fingerprint:' + provider),
                                         prior_parsers.get('source_parser_repair_fingerprint:' + provider)))
        # Discovery migrations have separate versioned ABIs. Keep their
        # normal upgrade behavior, but do not treat scheduling/review-only
        # capture changes as a new XML/mixed-page parser to replay.
        self.recheck_rss_metadata_parser(skip_providers=scheduling_only_changes)
        self.recheck_cnyes_mobile_identity_views(prior_sources.get('source_capture_fingerprint:cnyes'))
        self.recheck_cnyes_mobile_body_declarations(prior_sources.get('source_capture_fingerprint:cnyes'))
        self.recheck_cnyes_native_v6_views(prior_sources.get('source_capture_fingerprint:cnyes'))
        self.recheck_cnyes_native_v7_titles(prior_sources.get('source_capture_fingerprint:cnyes'))
        self.recheck_cnyes_native_empty_signature(prior_sources.get('source_capture_fingerprint:cnyes'))
        self.recheck_cnyes_native_v16_declarations(prior_sources.get('source_capture_fingerprint:cnyes'))
        self.recheck_changed_cnyes_parser(prior_sources.get('source_capture_fingerprint:cnyes'),
                                         prior_parsers.get('source_parser_repair_fingerprint:cnyes'))
        self.recheck_changed_feed_parser(prior_sources, prior_parsers)
        self.recheck_taiwanhot_native_views(prior_sources.get('source_capture_fingerprint:taiwanhot'),
                                          prior_parsers.get('source_parser_repair_fingerprint:taiwanhot'))
        self.recheck_taiwanhot_route_metadata(prior_sources.get('source_capture_fingerprint:taiwanhot'),
                                            prior_parsers.get('source_parser_repair_fingerprint:taiwanhot'))
        self.recheck_restricted_bulk_pages(skip_providers=scheduling_only_changes)
        if previous and previous[0] == fingerprint:
            if parser_settings:
                with self.db:
                    self.db.executemany('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', parser_settings)
            if any(prior_sources.get("source_capture_fingerprint:" + p) != s.data_fingerprint
                   or prior_allowed.get("source_capture_allowed:" + p) != str(int(s.allowed))
                   for p, s in self.registry.sources.items()):
                with self.db:
                    self.db.executemany("INSERT OR REPLACE INTO queue_settings VALUES (?,?)",
                                        (("source_capture_fingerprint:" + p, s.data_fingerprint) for p, s in self.registry.sources.items()))
                    self.db.executemany("INSERT OR REPLACE INTO queue_settings VALUES (?,?)",
                                        (("source_capture_allowed:" + p, str(int(s.allowed))) for p, s in self.registry.sources.items()))
            self.migrate_parser_repair_lanes()
            self.migrate_unknown_cnyes_body_rechecks()
            self.migrate_unknown_cnyes_identity_rechecks()
            return
        hosts = self.db.execute("SELECT host,provider FROM hosts").fetchall()
        with self.metrics.measure("policy_reconciliation"), self.db:
            for host, provider in hosts:
                source = self.registry.match("https://" + host + "/")
                capture_changed = bool(source and prior_sources.get("source_capture_fingerprint:" + source.provider) != source.data_fingerprint)
                parser_changed = bool(source and parser_repair_changed(
                    source, prior_sources.get('source_capture_fingerprint:' + source.provider),
                    prior_parsers.get('source_parser_repair_fingerprint:' + source.provider)))
                target = source.provider if source else "unregistered:" + host
                if (provider == target and source and not capture_changed
                        and prior_allowed.get("source_capture_allowed:" + source.provider) == str(int(source.allowed))):
                    continue  # A different provider's configuration is not a frontier change here.
                if provider != target:
                    self.db.execute("UPDATE articles SET provider=? WHERE host=?", (target, host))
                    self.db.execute("UPDATE hosts SET provider=? WHERE host=?", (target, host))
                if source and source.allowed:
                    self.db.execute("UPDATE articles SET metadata_order_day=substr(metadata_date_raw,1,10) WHERE host=? "
                                    "AND metadata_order_day IS NULL AND metadata_date_raw LIKE '____-__-__%'", (host,))
                    self.db.execute("UPDATE articles SET state='pending',due=0,error_code=NULL "
                                    "WHERE host=? AND state='awaiting_authorization'", (host,))
                    if (parser_changed and source.provider != 'cnyes'
                            and not (source.provider == 'taiwanhot'
                                     and source.embedded_article_layout == 'taiwanhot_visible_article_v3')):
                        # Retry parser failures only after a parser repair,
                        # not changed scheduling/review prose. Access and the
                        # complete capture contract are evaluated separately.
                        # TWH v3's pinned migration above owns its observation
                        # cohort; the generic reset would erase leased/head
                        # failures and their attempts/deadlines afterwards.
                        self.db.execute("UPDATE articles SET state='pending',due=0,attempts=0 "
                                        "WHERE host=? AND state IN ('body_not_found','article_attribution_not_verified',"
                                        "'article_title_not_verified')", (host,))
                    if parser_changed and source.provider == 'nownews' and source.embedded_article_layout == 'nownews_visible_article_v1':
                        from downloader.news_fulltext_public_originals import NOWNEWS_V1_CAPTURE_FINGERPRINT
                        if prior_sources.get('source_capture_fingerprint:nownews') == NOWNEWS_V1_CAPTURE_FINGERPRINT:
                            # Only this pinned parser concatenated the current
                            # publication and revision labels. Preserve prior
                            # error codes as evidence; no source denial, clock
                            # from another provider or stored head is reset.
                            failures = self.db.execute("SELECT id,url FROM articles WHERE host=? AND state='invalid_bulk_publication_clock' "
                                                       "AND body_sha256 IS NULL ORDER BY url", (host,)).fetchall()
                            self.db.executemany("UPDATE articles SET state='pending',due=0,attempts=0 WHERE id=?",
                                                ((key,) for key, _ in failures))
                            self.db.execute("INSERT OR REPLACE INTO queue_settings VALUES (?,?)", (
                                'nownews_v1_published_updated_clock_repair', json.dumps({
                                    'old_source_fingerprint': NOWNEWS_V1_CAPTURE_FINGERPRINT,
                                    'new_source_fingerprint': source.data_fingerprint,
                                    'previous_outcome': 'invalid_bulk_publication_clock', 'url_count': len(failures),
                                    'urls_sha256': hashlib.sha256('\n'.join(url for _, url in failures).encode()).hexdigest(),
                                    'observed_at_utc': utc_now(), 'source_refusals_or_stored_heads_reset': False}, sort_keys=True)))
                            self.metrics.count(source.provider, 'pinned_v1_published_updated_clock_rechecked', len(failures))
                    if parser_changed and source.provider == "cnews" and source.embedded_article_layout == "cnews_visible_article_v1":
                        from downloader.news_fulltext_cnews_cmmedia import (
                            CNEWS_V1_SCOPE_CLASSIFICATION_FINGERPRINT,
                            CNEWS_V2_CREDIT_CLASSIFICATION_FINGERPRINT,
                            CNEWS_V2_REVIEWED_ORIGINAL_URL_SHA256,
                        )
                        if prior_sources.get("source_capture_fingerprint:cnews") == CNEWS_V1_SCOPE_CLASSIFICATION_FINGERPRINT:
                            # The pinned v1 parser emitted this outcome for a
                            # header/body attribution mismatch, including the
                            # verified editorial-account originals. Re-evaluate
                            # that exact version once, preserving its evidence;
                            # do not reset any refusal, paywall, absence, stored
                            # head or another version/provider's rights result.
                            failures = self.db.execute("SELECT id,url FROM articles WHERE host=? AND state='article_rights_or_scope_excluded'", (host,)).fetchall()
                            self.db.executemany("UPDATE articles SET state='pending',due=0,attempts=0 WHERE id=?", ((key,) for key, _ in failures))
                            self.db.execute("INSERT OR REPLACE INTO queue_settings VALUES (?,?)", (
                                "cnews_editorial_byline_v2_repair", json.dumps({
                                    "old_source_fingerprint": CNEWS_V1_SCOPE_CLASSIFICATION_FINGERPRINT,
                                    "new_source_fingerprint": source.data_fingerprint,
                                    "previous_outcome": "article_rights_or_scope_excluded",
                                    "url_count": len(failures),
                                    "urls_sha256": hashlib.sha256("\n".join(sorted(url for _, url in failures)).encode()).hexdigest(),
                                    "observed_at_utc": utc_now(),
                                    "source_refusals_or_stored_heads_reset": False}, sort_keys=True)))
                            self.metrics.count(source.provider, "pinned_v1_byline_outcomes_rechecked", len(failures))
                        elif prior_sources.get("source_capture_fingerprint:cnews") == CNEWS_V2_CREDIT_CLASSIFICATION_FINGERPRINT:
                            # Three live-reviewed originals only: legacy credit
                            # formats were misclassified by this pinned parser.
                            # Preserve every other scope failure and all paid,
                            # refused, absent or already stored article states.
                            failures = [(key, url) for key, url in self.db.execute(
                                "SELECT id,url FROM articles WHERE host=? AND state='article_rights_or_scope_excluded'", (host,))
                                if hashlib.sha256(url.encode()).hexdigest() in CNEWS_V2_REVIEWED_ORIGINAL_URL_SHA256]
                            self.db.executemany("UPDATE articles SET state='pending',due=0,attempts=0 WHERE id=?", ((key,) for key, _ in failures))
                            self.db.execute("INSERT OR REPLACE INTO queue_settings VALUES (?,?)", (
                                "cnews_original_credit_v3_repair", json.dumps({
                                    "old_source_fingerprint": CNEWS_V2_CREDIT_CLASSIFICATION_FINGERPRINT,
                                    "new_source_fingerprint": source.data_fingerprint,
                                    "previous_outcome": "article_rights_or_scope_excluded",
                                    "url_count": len(failures),
                                    "urls_sha256": hashlib.sha256("\n".join(sorted(url for _, url in failures)).encode()).hexdigest(),
                                    "observed_at_utc": utc_now(),
                                    "source_refusals_or_stored_heads_reset": False}, sort_keys=True)))
                            self.metrics.count(source.provider, "pinned_v2_original_credit_outcomes_rechecked", len(failures))
                    if parser_changed and source.embedded_article_layout == "einfo_next_post_v1":
                        # v2 verifies the old source's explicit reporter role
                        # and matching visible names. Do not retry exclusions,
                        # missing CC markers, paywalls or outside writers.
                        self.db.execute("UPDATE articles SET state='pending',due=0,attempts=0 "
                                        "WHERE host=? AND state='article_rights_not_verified'", (host,))
                    if parser_changed and source.provider == "fed_board" and source.pdf_path_pattern:
                        # Independently verified PDF Accept negotiation fixes
                        # only this representation-specific 406. Preserve the
                        # old outcome as error_code until acceptance; source
                        # access denials/paywalls are never reclassified.
                        failures = self.db.execute("SELECT id,url FROM articles WHERE host=? AND state IN ('http_406','response_too_large')", (host,)).fetchall()
                        for key, url in failures:
                            if re.fullmatch(source.pdf_path_pattern, urlsplit(url).path):
                                self.db.execute("UPDATE articles SET state='pending',due=0,attempts=0 WHERE id=?", (key,))
                    if (parser_changed and (source.provider == 'inside' or source.source_not_found_paths or source.allow_short_configured_body)
                            and not (source.provider == 'taiwanhot'
                                     and source.embedded_article_layout == 'taiwanhot_visible_article_v3')):
                        # Reviewed namespace/soft-404 and exact short-body
                        # parser repairs only, not a source-error reset.
                        # Preserve all source refusals, paywalls, missing URLs
                        # and their cooldowns; no wholesale failure replay.
                        repaired_states = ['unsupported_article_path', 'redirect_not_article']
                        if source.allow_short_configured_body:
                            repaired_states.append('body_not_found')
                        failures = self.db.execute("SELECT id,url FROM articles WHERE host=? AND state IN ("
                                                   + ','.join('?' for _ in repaired_states) + ')', (host, *repaired_states)).fetchall()
                        for key, url in failures:
                            if source.article_path_pattern and re.search(source.article_path_pattern, urlsplit(url).path):
                                self.db.execute("UPDATE articles SET state='pending',due=0,attempts=0 WHERE id=?", (key,))
                    if parser_changed and source.provider == "cnyes" and source.cnyes_apis:
                        # Policy changes repair only these verified parser/path
                        # outcomes, never source denials, rights or paywalls.
                        failures = self.db.execute("SELECT id,url,state,body_sha256 FROM articles WHERE host=? AND state IN "
                                                   "('unsupported_article_path','body_not_found','article_attribution_not_verified')", (host,)).fetchall()
                        for key, url, state, body_sha256 in failures:
                            # The bounded unknown-body migration owns ongoing
                            # reviewed native routes. A parser rollout must not
                            # reset their original due/attempt/error/lease before
                            # that migration can record and preserve evidence.
                            if state == 'body_not_found' and self.unknown_cnyes_body_recheck_allowed(
                                    source, {'provider': 'cnyes', 'url': url, 'body_sha256': body_sha256}):
                                continue
                            if state == "article_attribution_not_verified" and (host not in {"news.cnyes.com", "m.cnyes.com"}
                                    or not re.fullmatch(r"/news/print/[0-9]+", urlsplit(url).path)):
                                continue
                            if re.search(source.article_path_pattern, urlsplit(url).path):
                                self.db.execute("UPDATE articles SET state='pending',due=0,attempts=0 WHERE id=?", (key,))
                                self.metrics.count(source.provider, "policy_requeued_" + state)
                else:
                    self.db.execute("UPDATE articles SET state='awaiting_authorization' "
                                    "WHERE host=? AND state IN ('pending','retry','retry_cooldown','inflight')", (host,))
            self.db.execute("INSERT OR REPLACE INTO queue_settings VALUES ('policy_fingerprint',?)", (fingerprint,))
            self.db.executemany("INSERT OR REPLACE INTO queue_settings VALUES (?,?)",
                                (("source_capture_fingerprint:" + p, s.data_fingerprint) for p, s in self.registry.sources.items()))
            self.db.executemany("INSERT OR REPLACE INTO queue_settings VALUES (?,?)",
                                (("source_capture_allowed:" + p, str(int(s.allowed))) for p, s in self.registry.sources.items()))
            self.db.executemany('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', parser_settings)
        self.migrate_parser_repair_lanes()
        self.migrate_unknown_cnyes_body_rechecks()
        self.migrate_unknown_cnyes_identity_rechecks()

    def recheck_taiwanhot_native_views(self, previous_capture, previous_identity):
        """Pinned v1 URI admission defects, not body failures/source refusals.

        Keep attempts, due, error/observation clocks and raw/head evidence.
        Move only newly admitted missing/unleased literal views to the bounded
        retry lane. Unknown old parsers and unchanged owner starts do no scan.
        """
        from downloader.news_fulltext_public_originals import (
            TAIWANHOT_V1_NATIVE_VIEW_REPAIR_IDENTITY, taiwanhot_article_id)
        source = self.registry.sources.get('taiwanhot')
        old_capture, old_parser = TAIWANHOT_V1_NATIVE_VIEW_REPAIR_IDENTITY
        if (source is None or not source.allowed or source.embedded_article_layout != 'taiwanhot_visible_article_v2'
                or previous_capture != old_capture or not isinstance(previous_identity, str)):
            return
        try:
            old_identity = json.loads(previous_identity)
        except (ValueError, TypeError):
            return
        if old_identity != {'contract_version': 1, 'data_fingerprint': old_capture, 'parser_fingerprint': old_parser}:
            return
        marker = 'taiwanhot_native_view_identity_repair_v2:' + old_capture + ':' + source.data_fingerprint
        if self.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (marker,)).fetchone():
            return
        with self.metrics.measure('native_view_identity_repair', source.provider), self.db:
            candidates = self.db.execute('SELECT id,url,state,error_code,attempts,due,last_checked_at_utc '
                'FROM articles INDEXED BY dispatch WHERE provider=? AND state IN '
                "('article_identity_not_verified','unsupported_article_path') AND body_sha256 IS NULL "
                'AND lease_owner IS NULL AND lease_until IS NULL', (source.provider,)).fetchall()
            repaired = []
            for row in candidates:
                if row['error_code'] != row['state'] or taiwanhot_article_id(row['url'], requested=True, native_view=True) is None:
                    continue
                bare_identity = (row['state'] == 'article_identity_not_verified'
                    and urlsplit(row['url']).netloc == 'taiwanhot.net'
                    and taiwanhot_article_id(row['url'], requested=True) is not None)
                new_query_view = (row['state'] == 'unsupported_article_path'
                                  and taiwanhot_article_id(row['url'], requested=True) is None)
                if not (bare_identity or new_query_view):
                    continue
                self.db.execute("UPDATE articles SET state='retry' WHERE id=?", (row['id'],))
                repaired.append({k: row[k] for k in ('url', 'state', 'error_code', 'attempts', 'due', 'last_checked_at_utc')})
            self.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, json.dumps({
                'contract_version': 2, 'old_capture': old_capture, 'old_parser': old_parser,
                'new_capture': source.data_fingerprint, 'new_parser': source.parser_repair_fingerprint,
                'observed_at_utc': utc_now(), 'rows_moved': len(repaired), 'rows': repaired,
                'target_state': 'retry', 'attempts_due_errors_clocks_and_bodies_preserved': True,
                'source_refusals_leases_and_stored_heads_reset': False, 'accepted_as_bodies': False}, sort_keys=True)))
            self.metrics.count(source.provider, 'native_view_identity_failures_scheduled_for_recheck', len(repaired))

    def recheck_taiwanhot_route_metadata(self, previous_capture, previous_identity):
        """Pinned route observation or verified whole-short-prose repair.

        A short/old focus path can now be validated. Other still-unidentified
        public responses need the newly bounded failure metadata once, not an
        unproven body-complete verdict. Preserve leases/heads/source refusals
        and original attempts, due and clocks; normal retry fairness applies.
        A separate pinned v3 short-prose opt-in moves only missing bodies;
        other identity errors and diagnostic budgets are never reset.
        """
        from downloader.news_fulltext_public_originals import (
            TAIWANHOT_V2_ROUTE_REPAIR_IDENTITY, TAIWANHOT_V3_SHORT_PROSE_REPAIR_IDENTITY,
            taiwanhot_request_ids)
        source = self.registry.sources.get('taiwanhot')
        if (source is None or not source.allowed or source.embedded_article_layout != 'taiwanhot_visible_article_v3'
                or not isinstance(previous_identity, str)):
            return
        short_prose = (source.allow_short_configured_body
                       and previous_capture == TAIWANHOT_V3_SHORT_PROSE_REPAIR_IDENTITY[0])
        old_capture, old_parser = (TAIWANHOT_V3_SHORT_PROSE_REPAIR_IDENTITY if short_prose else
                                   TAIWANHOT_V2_ROUTE_REPAIR_IDENTITY)
        if previous_capture != old_capture:
            return
        try:
            old = json.loads(previous_identity)
        except (ValueError, TypeError):
            return
        if old != {'contract_version': 1, 'data_fingerprint': old_capture, 'parser_fingerprint': old_parser}:
            return
        marker = ('taiwanhot_complete_short_prose_repair_v3:' if short_prose else
                  'taiwanhot_route_metadata_observation_v3:') + old_capture + ':' + source.data_fingerprint
        if self.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (marker,)).fetchone():
            return
        with self.metrics.measure('route_metadata_recheck', source.provider), self.db:
            candidates = self.db.execute('SELECT id,url,state,error_code,attempts,due,last_checked_at_utc '
                'FROM articles INDEXED BY dispatch WHERE provider=? AND state IN '
                + ("('body_not_found')" if short_prose else
                   "('unsupported_article_path','article_identity_not_verified','body_not_found')")
                + ' AND body_sha256 IS NULL AND lease_owner IS NULL AND lease_until IS NULL', (source.provider,)).fetchall()
            rows = []
            for row in candidates:
                if row['error_code'] != row['state'] or not taiwanhot_request_ids(row['url']):
                    continue
                self.db.execute("UPDATE articles SET state='retry' WHERE id=?", (row['id'],))
                rows.append({k: row[k] for k in ('url','state','error_code','attempts','due','last_checked_at_utc')})
            self.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, json.dumps({
                'contract_version': 3, 'old_capture': old_capture, 'old_parser': old_parser,
                'new_capture': source.data_fingerprint, 'new_parser': source.parser_repair_fingerprint,
                'observed_at_utc': utc_now(), 'rows_moved': len(rows), 'rows': rows,
                'target_state': 'retry', 'attempts_due_errors_clocks_and_bodies_preserved': True,
                'source_refusals_leases_and_stored_heads_reset': False,
                'accepted_as_bodies': False, 'metadata_observation_is_not_a_body_repair': not short_prose,
                'whole_configured_short_prose_with_native_metadata_optin': short_prose}, sort_keys=True)))
            self.metrics.count(source.provider, 'native_route_or_unknown_metadata_rechecks_scheduled', len(rows))

    @staticmethod
    def unknown_cnyes_body_recheck_allowed(source, row):
        """An unidentified missing body is not proof of permanent absence.

        Only already registered public native article routes, missing heads
        and ongoing enabled scope qualify. Source absence, empty templates,
        access/rights failures and finite cohorts remain distinct outcomes.
        """
        if (source is None or not source.allowed or source.provider != 'cnyes'
                or source.acquisition_mode == 'finite' or row.get('provider') != 'cnyes'
                or row.get('body_sha256') is not None):
            return False
        try:
            from downloader.news_fulltext_publishers import cnyes_supported_native_alias_view

            parts = urlsplit(row['url'])
            return bool(parts.scheme == 'https' and parts.netloc in {'news.cnyes.com', 'm.cnyes.com'}
                        and any(parts.hostname == domain or parts.hostname.endswith('.' + domain) for domain in source.domains)
                        and (not parts.query or cnyes_supported_native_alias_view(parts)) and not parts.fragment
                        and re.fullmatch(r'/news/id/[1-9][0-9]*', parts.path)
                        and source.article_path_pattern and re.search(source.article_path_pattern, parts.path))
        except (KeyError, TypeError, ValueError):
            return False

    def migrate_unknown_cnyes_body_rechecks(self):
        self._migrate_unknown_cnyes_parser_rechecks(
            error_code='body_not_found', lane='body', version=CNYES_UNKNOWN_BODY_RECHECK_CONTRACT_VERSION)

    def migrate_unknown_cnyes_identity_rechecks(self):
        # A current native page may later recover its own declarations. Keep
        # a mismatch negative until a new complete response passes every gate;
        # four real terminal failures recovered unchanged in v75 probes.
        self._migrate_unknown_cnyes_parser_rechecks(
            error_code='article_identity_not_verified', lane='identity',
            version=CNYES_UNKNOWN_IDENTITY_RECHECK_CONTRACT_VERSION)

    def _migrate_unknown_cnyes_parser_rechecks(self, *, error_code, lane, version):
        """Once per source/repair ABI, preserve old observations in a retry lane.

        The failure-state index limits this to the old missing-body cohort,
        not the millions of pending or saved articles. No publisher request,
        source refusal, lease, body, original clock or attempt count is reset.
        """
        source = self.registry.sources.get('cnyes')
        if not source or not source.allowed or source.acquisition_mode == 'finite':
            return
        marker = f'unknown_cnyes_{lane}_rechecks_v{version}:' + source.data_fingerprint
        if self.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (marker,)).fetchone():
            return
        now = time.time()
        with self.metrics.measure(f'unknown_{lane}_recheck_migration', 'cnyes'), self.db:
            candidates = self.db.execute(
                "SELECT id,provider,url,body_sha256,due,attempts,last_checked_at_utc "
                "FROM articles INDEXED BY dispatch WHERE provider='cnyes' AND state=? "
                "AND error_code=? AND body_sha256 IS NULL AND last_checked_at_utc IS NOT NULL "
                "AND lease_owner IS NULL AND lease_until IS NULL", (error_code, error_code)).fetchall()
            repaired = []
            for candidate in candidates:
                row = dict(candidate)
                if not self.unknown_cnyes_body_recheck_allowed(source, row):
                    continue
                try:
                    clock = datetime.fromisoformat(row['last_checked_at_utc'].replace('Z', '+00:00'))
                    if clock.tzinfo is None:
                        continue
                    due = max(now, row['due'], clock.timestamp() + CNYES_UNKNOWN_BODY_RECHECK_SECONDS)
                except (TypeError, ValueError, OverflowError):
                    continue
                self.db.execute("UPDATE articles SET state='retry_cooldown',due=? WHERE id=?", (due, row['id']))
                repaired.append({'url': row['url'], 'previous_last_checked_at_utc': row['last_checked_at_utc'],
                                 'attempts_preserved': row['attempts'], 'previous_due': row['due'], 'due': due})
            self.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, json.dumps({
                'contract_version': version,
                'source_fingerprint': source.data_fingerprint, 'observed_at_utc': utc_now(),
                'target_state': 'retry_cooldown', 'original_error_code': error_code,
                'recheck_minimum_seconds': CNYES_UNKNOWN_BODY_RECHECK_SECONDS,
                'rows': repaired, 'accepted_as_bodies': False,
                'source_refusals_leases_stored_heads_and_finite_cohorts_changed': False}, sort_keys=True)))
            self.metrics.count('cnyes', f'unknown_{lane}_failures_scheduled_for_recheck', len(repaired))

    def migrate_parser_repair_lanes(self):
        """Keep previously observed parser repairs out of first-download work.

        Once per source capture/dispatch ABI, move only unleased pending rows
        with a real previous parser-failure clock to the bounded retry lane.
        No due time, attempt count, original error/clock, stored head or source
        refusal is reset. A later parser change gets its own one-time marker;
        unchanged owner starts never rescan pending history for this migration.
        """
        for source in self.registry.sources.values():
            if not source.allowed:
                continue
            marker = ('parser_repair_retry_lane_v1:' + source.provider + ':'
                      + source.data_fingerprint + ':' + str(ARTICLE_DISPATCH_CONTRACT_VERSION))
            if self.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (marker,)).fetchone():
                continue
            predicate = ("provider=? AND state='pending' AND error_code IN "
                         "('body_not_found','article_attribution_not_verified') "
                         "AND last_checked_at_utc IS NOT NULL AND lease_owner IS NULL AND lease_until IS NULL")
            with self.metrics.measure('parser_repair_lane_migration', source.provider), self.db:
                count, stored = self.db.execute('SELECT count(*),coalesce(sum(body_sha256 IS NOT NULL),0) '
                                                'FROM articles WHERE ' + predicate, (source.provider,)).fetchone()
                self.db.execute("UPDATE articles SET state='retry' WHERE " + predicate, (source.provider,))
                self.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, json.dumps({
                    'dispatch_contract_version': ARTICLE_DISPATCH_CONTRACT_VERSION,
                    'source_fingerprint': source.data_fingerprint, 'rows_moved': count,
                    'stored_body_rechecks_preserved': stored, 'observed_at_utc': utc_now(),
                    'previous_state': 'pending', 'target_state': 'retry',
                    'previous_parser_outcomes': ['body_not_found', 'article_attribution_not_verified'],
                    'original_error_clock_attempts_due_lease_and_body_preserved': True,
                    'http_refusals_or_unobserved_first_downloads_changed': False}, sort_keys=True)))
                self.metrics.count(source.provider, 'observed_parser_repairs_moved_to_retry_lane', count)

    def record_scheduling_only_discovery_guard(self, marker, source, parser, error):
        """Durably skip the unchanged parser across later owner restarts.

        The current discovery ABI remains part of the marker: a real later
        XML/envelope ABI upgrade still gets its original migration. No source
        request, successful parse or body acceptance is asserted here.
        """
        previous = self.db.execute('SELECT value FROM queue_settings WHERE key=?',
            ('source_capture_fingerprint:' + source.provider,)).fetchone()
        identity = self.db.execute('SELECT value FROM queue_settings WHERE key=?',
            ('source_parser_repair_fingerprint:' + source.provider,)).fetchone()
        if (not previous or not identity or previous[0] == source.data_fingerprint
                or parser_repair_changed(source, previous[0], identity[0])):
            raise ValueError('proven_scheduling_only_discovery_migration_required')
        with self.db:
            self.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, json.dumps({
                'new_discovery_fingerprint': parser, 'jobs': [], 'previous_error': error,
                'old_source_fingerprint': previous[0], 'new_source_fingerprint': source.data_fingerprint,
                'parser_repair_identity_before': json.loads(identity[0]),
                'parser_repair_identity_after': parser_repair_identity(source),
                'scheduling_only_capture_change': True, 'source_capture_contract_changed': True,
                'source_requests_or_acceptance_asserted': False, 'article_or_source_outcomes_reset': False,
                'http_refusals_requeued': False, 'observed_at_utc': utc_now()}, sort_keys=True)))

    def recheck_rss_metadata_parser(self, *, skip_providers=()):
        """One retry of registered invalid XML jobs under the new discovery ABI.

        The article capture contract did not change: do not re-fetch already
        saved bodies or reset a publisher's refusal/cooldown to fix discovery.
        Preserve the previous failure and deadline in the independent audit.
        """
        from downloader.news_fulltext import discovery_fingerprint
        from downloader.news_fulltext_feed import feed_page

        for source in self.registry.sources.values():
            if not source.allowed or not source.fulltext_feeds or source.acquisition_mode == 'finite':
                continue
            parser = discovery_fingerprint(source)
            marker = 'rss_xml_controls_parser_recheck:' + source.provider + ':' + parser
            if self.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (marker,)).fetchone():
                continue
            if source.provider in skip_providers:
                self.record_scheduling_only_discovery_guard(marker, source, parser, 'invalid_feed')
                continue
            repaired = []
            with self.db:
                for row in self.db.execute("SELECT url,due,attempts,parser_fingerprint FROM discovery WHERE provider=? "
                    "AND kind='rss_fulltext' AND error_code='invalid_feed'", (source.provider,)).fetchall():
                    if row['url'].split('?', 1)[0] not in source.fulltext_feeds or row['parser_fingerprint'] == parser:
                        continue
                    try:
                        feed_page(row['url'])
                    except NewsError:
                        continue
                    self.db.execute('UPDATE discovery SET due=0,etag=NULL,last_modified=NULL,parser_fingerprint=NULL WHERE url=?', (row['url'],))
                    repaired.append(dict(row))
                self.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (marker, json.dumps({
                    'new_discovery_fingerprint': parser, 'jobs': repaired, 'previous_error': 'invalid_feed',
                    'source_capture_contract_changed': False, 'http_refusals_requeued': False,
                    'observed_at_utc': utc_now()}, sort_keys=True)))
                self.counter('rss_xml_controls_parser_jobs_requeued', len(repaired))

    def recheck_restricted_bulk_pages(self, *, skip_providers=()):
        """Once per discovery ABI, resume a mixed page's permitted identities.

        Keep the old outcome/deadline/attempts as evidence. No body policy,
        article state, source cooldown, HTTP refusal or removed endpoint is
        reset. A current mixed-page metadata checkpoint stays on its own due
        time; this only repairs older jobs that could strand the page chain.
        """
        from downloader.news_fulltext import discovery_fingerprint
        from downloader.news_fulltext_bulk import wordpress_base, wordpress_page
        from downloader.news_fulltext_feed import feed_page

        for source in self.registry.sources.values():
            bases = {'rss_fulltext': source.fulltext_feeds, 'wordpress_posts': source.wordpress_apis,
                     'cnyes_news': source.cnyes_apis}
            if not source.allowed or source.acquisition_mode == 'finite' or not any(bases.values()):
                continue
            parser = discovery_fingerprint(source)
            marker = 'mixed_bulk_scope_continuation:' + source.provider + ':' + parser
            if self.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (marker,)).fetchone():
                continue
            if source.provider in skip_providers:
                self.record_scheduling_only_discovery_guard(marker, source, parser, 'bulk_response_contains_restricted_text')
                continue
            repaired = []
            with self.db:
                rows = self.db.execute("SELECT url,kind,due,attempts,parser_fingerprint FROM discovery "
                    "WHERE provider=? AND error_code='bulk_response_contains_restricted_text'",
                    (source.provider,)).fetchall()
                for row in rows:
                    if (row['due'] == 0 or row['parser_fingerprint'] == parser
                            or wordpress_base(row['url']) not in bases.get(row['kind'], ())):
                        continue
                    try:
                        page = feed_page(row['url']) if row['kind'] == 'rss_fulltext' else wordpress_page(row['url'])
                        if not 1 <= page <= (30 if row['kind'] == 'cnyes_news' else 100000):
                            continue
                    except (NewsError, ValueError, TypeError):
                        continue
                    self.db.execute('UPDATE discovery SET due=0,etag=NULL,last_modified=NULL,parser_fingerprint=NULL WHERE url=?',
                                    (row['url'],))
                    repaired.append(dict(row))
                self.db.execute('INSERT INTO queue_settings VALUES (?,?)', (marker, json.dumps({
                    'new_discovery_fingerprint': parser, 'jobs': repaired,
                    'previous_error': 'bulk_response_contains_restricted_text',
                    'source_capture_contract_changed': False, 'article_or_source_outcomes_reset': False,
                    'http_refusals_requeued': False, 'observed_at_utc': utc_now()}, sort_keys=True)))
                self.counter('mixed_bulk_scope_pages_requeued', len(repaired))

    def recheck_changed_feed_parser(self, prior_sources, prior_parsers=None):
        """Re-evaluate failed RSS rights parsing once per changed contract.

        A prose word such as 'paywall' used to reject a whole public feed page.
        Only its exact registered RSS parser job is requeued. Preserve prior
        item errors/deadlines in an audit record and never reset HTTP refusals,
        provider rate limits, healthy pages or article leases.
        """
        from downloader.news_fulltext_feed import feed_page

        for source in self.registry.sources.values():
            previous = prior_sources.get('source_capture_fingerprint:' + source.provider)
            if (not source.allowed or not source.fulltext_feeds or not previous
                    or not parser_repair_changed(source, previous,
                        (prior_parsers or {}).get('source_parser_repair_fingerprint:' + source.provider))):
                continue
            marker = 'rss_parser_recheck:' + source.provider + ':' + source.data_fingerprint
            if self.db.execute('SELECT 1 FROM queue_settings WHERE key=?', (marker,)).fetchone():
                continue
            repaired = []
            with self.db:
                rows = self.db.execute('SELECT url,due,attempts FROM discovery WHERE provider=? '
                    "AND kind='rss_fulltext' AND error_code='bulk_response_contains_restricted_text'",
                    (source.provider,)).fetchall()
                for row in rows:
                    url = row['url']
                    if url.split('?', 1)[0] not in source.fulltext_feeds:
                        continue
                    try:
                        feed_page(url)
                    except NewsError:
                        continue
                    self.db.execute('UPDATE discovery SET due=0,etag=NULL,last_modified=NULL,parser_fingerprint=NULL WHERE url=?', (url,))
                    repaired.append({'url': url, 'previous_error': 'bulk_response_contains_restricted_text',
                                     'previous_deadline': row['due'], 'previous_attempts': row['attempts']})
                self.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (marker, json.dumps({
                    'old_source_fingerprint': previous, 'new_source_fingerprint': source.data_fingerprint,
                    'jobs': repaired, 'http_refusals_requeued': False, 'observed_at_utc': utc_now()}, sort_keys=True)))
                self.counter('rss_changed_parser_jobs_requeued', len(repaired))

    def reconcile_successful_feed_rechecks(self):
        """Retire stale RSS error projections after proven parser recovery.

        Older successful RSS parses did not clear item errors. Only an exact
        job previously requeued by this source's changed-contract record, now
        successfully checked under the current parser, can retire old errors.
        Archive them in the queue audit; do not erase a subsequent item failure.
        """
        from downloader.news_fulltext import discovery_fingerprint

        for source in self.registry.sources.values():
            marker = 'rss_parser_recheck:' + source.provider + ':' + source.data_fingerprint
            saved = self.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
            if not source.allowed or not source.fulltext_feeds or not saved:
                continue
            recovery = json.loads(saved[0])
            for job in recovery['jobs']:
                url, recovery_time = job['url'], recovery['observed_at_utc']
                proof = self.db.execute('SELECT error_code,last_checked_at_utc,parser_fingerprint,response_sha256 '
                                       'FROM discovery WHERE url=? AND provider=? AND kind=\'rss_fulltext\'',
                                       (url, source.provider)).fetchone()
                if (not proof or proof['error_code'] or not proof['response_sha256']
                        or proof['parser_fingerprint'] != discovery_fingerprint(source)
                        or not proof['last_checked_at_utc'] or proof['last_checked_at_utc'] <= recovery_time):
                    continue
                old = [dict(row) for row in self.db.execute('SELECT * FROM bulk_item_errors WHERE request_url=? '
                    'AND provider=? AND observed_at_utc<?', (url, source.provider, recovery_time))]
                if not old:
                    continue
                with self.db:
                    self.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (
                        'rss_resolved_item_errors:' + hashlib.sha256((marker + url).encode()).hexdigest(),
                        json.dumps({'source_recovery_marker': marker, 'current_source_response_sha256': proof['response_sha256'],
                            'successful_check_at_utc': proof['last_checked_at_utc'], 'retired_projection': old,
                            'observed_at_utc': utc_now(), 'article_or_source_outcomes_reset': False}, sort_keys=True)))
                    self.db.execute('DELETE FROM bulk_item_errors WHERE request_url=? AND provider=? AND observed_at_utc<?',
                                    (url, source.provider, recovery_time))
                    self.counter('rss_resolved_stale_item_errors', len(old))

    def recheck_changed_cnyes_parser(self, previous_fingerprint, previous_parser_identity=None):
        """One retry of old parser failures; preserve source refusal evidence.

        Only an old circuit's exact failed job is due under a genuinely changed
        source contract. Keep its old errors/deadlines in the circuit table.
        Unchanged restarts cannot erase rate limits, leases or retry outcomes.
        """
        source = self.registry.sources.get('cnyes')
        if (not source or not source.allowed or not previous_fingerprint
                or not parser_repair_changed(source, previous_fingerprint, previous_parser_identity)):
            return
        repaired = []
        with self.db:
            for row in self.db.execute('SELECT request_url,error_code,not_before FROM bulk_endpoint_cooldowns '
                "WHERE provider='cnyes' AND data_fingerprint!=? "
                "AND error_code IN ('bulk_all_items_unusable','bulk_pagination_totals_invalid')", (source.data_fingerprint,)).fetchall():
                url = row['request_url']
                if url.split('?', 1)[0] not in source.cnyes_apis:
                    continue
                count = self.db.execute('UPDATE discovery SET due=0,etag=NULL,last_modified=NULL,parser_fingerprint=NULL '
                    "WHERE provider='cnyes' AND kind='cnyes_news' AND url=? AND error_code=? AND superseded_by_windows_json IS NULL",
                    (url, row['error_code'])).rowcount
                if count:
                    repaired.append({'url': url, 'previous_error': row['error_code'], 'previous_endpoint_deadline': row['not_before']})
            self.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (
                'cnyes_parser_recheck:' + source.data_fingerprint,
                json.dumps({'old_source_fingerprint': previous_fingerprint,
                            'new_source_fingerprint': source.data_fingerprint, 'jobs': repaired,
                            'http_refusals_requeued': False, 'observed_at_utc': utc_now()}, sort_keys=True)))
            self.counter('cnyes_changed_parser_jobs_requeued', len(repaired))

    @staticmethod
    def cnyes_partition_retry_deadline(jobs):
        from urllib.parse import parse_qs
        return max((row[1] for row in jobs if row[2] is not None and not (
            row[2] == 'discovery_http_422' and int(parse_qs(urlsplit(row[0]).query).get('page', ['1'])[0]) > 30)), default=0)

    def partition_cnyes_window(self, url, pages, source, *, existing_jobs=None):
        """Schedule exact source-date children, not a history-completeness proof.

        Keep every old page, outcome, cache validator, attempt and deadline.
        Failed parent pages defer new children until their existing deadline;
        only the obsolete page>30 input error is not an access prohibition.
        Old source totals/pages/gaps remain inspectable after retirement. The
        caller supplies actual validated pagination, live or retained.
        Migration passes its one-walk job index, avoiding one category scan per
        dense day. A single live split uses the existing exact endpoint seek.
        """
        from downloader.news_fulltext_bulk import cnyes_history_priority, cnyes_scope, cnyes_window_partitions, wordpress_base
        from urllib.parse import parse_qs

        children = cnyes_window_partitions(url, pages)
        if not children:
            return []
        endpoint, scope = wordpress_base(url), cnyes_scope(url)
        if not source.allowed or endpoint not in source.cnyes_apis:
            raise NewsError('cnyes_window_partition_unregistered_endpoint')
        previous = self.db.execute('SELECT child_urls_json FROM bulk_window_splits WHERE scope_url=?', (scope,)).fetchone()
        if previous:
            if json.loads(previous[0]) != children:
                raise NewsError('cnyes_window_partition_conflict')
            from downloader.news_fulltext_cnyes_history import index_children
            index_children(self, scope, source.provider, children)
            return children
        if existing_jobs is None:
            query = parse_qs(urlsplit(url).query)
            existing_jobs = self.db.execute(
                "SELECT url,due,error_code,superseded_by_windows_json FROM discovery "
                "WHERE url>=? AND url<? AND kind='cnyes_news' "
                "AND instr(replace(url,'?','&')||'&',?)>0 AND instr(replace(url,'?','&')||'&',?)>0",
                (endpoint + '?', endpoint + '@', '&startAt=' + query['startAt'][0] + '&',
                 '&endAt=' + query['endAt'][0] + '&')).fetchall()
        owned = [row for row in existing_jobs if cnyes_scope(row[0]) == scope]
        deferred_until = self.cnyes_partition_retry_deadline(owned)
        split_json = json.dumps(children)
        self.db.execute('INSERT INTO bulk_window_splits VALUES (?,?,?,?)', (scope, source.provider, split_json, utc_now()))
        from downloader.news_fulltext_cnyes_history import index_children
        index_children(self, scope, source.provider, children)
        self.db.executemany('UPDATE discovery SET superseded_by_windows_json=? WHERE url=?',
                            ((split_json, row[0]) for row in owned))
        self.db.executemany("INSERT INTO discovery(url,kind,provider,priority,due) VALUES (?,'cnyes_news',?,?,?) "
                            'ON CONFLICT(url) DO UPDATE SET due=max(discovery.due,excluded.due) '
                            'WHERE discovery.superseded_by_windows_json IS NULL AND discovery.due<excluded.due',
                            ((child, source.provider, cnyes_history_priority(child), deferred_until) for child in children))
        # Scheduling is not acquisition: never erase an old gap merely because
        # its replacement pages are queued. Child coverage is verified later.
        self.db.execute('INSERT OR IGNORE INTO bulk_window_gaps VALUES (?,?,?,?)',
                        (scope, source.provider, 'cnyes_publication_day_partition_pending', utc_now()))
        return children

    def migrate_cnyes_window_dispatch(self):
        """Once per scheduler/bulk ABI, repair legacy UTC-day cap scopes.

        Actual retained first-page evidence can directly seed date children;
        no redundant parent fetch, source-body ABI change or refusal reset.
        Without that evidence require one real parent response. Genuine
        single source dates retain the old cap and permitted-page recovery.
        """
        from downloader.news_fulltext_bulk import (CNYES_WINDOW_PARTITION_CONTRACT_VERSION,
            cnyes_history_priority, cnyes_scope, cnyes_url, cnyes_window_partitions, wordpress_base)
        from downloader.news_fulltext_policy import cnyes_bulk_fingerprint
        from urllib.parse import parse_qs

        for source in self.registry.sources.values():
            if not source.allowed or not source.cnyes_apis:
                continue
            marker = f'cnyes_window_dispatch_v{CNYES_WINDOW_DISPATCH_CONTRACT_VERSION}:' + source.provider
            fingerprint = cnyes_bulk_fingerprint(source) + ':calendar-v' + str(CNYES_WINDOW_PARTITION_CONTRACT_VERSION)
            previous = self.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()
            if previous and previous[0] == fingerprint:
                continue
            scopes = {r[0]: r[1] for r in self.db.execute(
                'SELECT b.base_url,b.total_pages FROM bulk_api_coverage b '
                'LEFT JOIN bulk_window_splits s ON b.base_url=s.scope_url '
                'WHERE b.provider=? AND b.total_pages>30 AND s.scope_url IS NULL', (source.provider,))}
            jobs = {}
            first_pages = set()
            if scopes:
                # One indexed provider walk, not one endpoint-wide scan per
                # dense day, and no scan of millions of article bodies.
                for row in self.db.execute('SELECT url,due,error_code,superseded_by_windows_json FROM discovery '
                                           'WHERE provider=? AND kind=\'cnyes_news\'', (source.provider,)):
                    scope = cnyes_scope(row[0])
                    if scope in scopes:
                        jobs.setdefault(scope, []).append(tuple(row))
                first_pages = {row[0] for row in self.db.execute(
                    'SELECT p.scope_url FROM bulk_page_evidence p JOIN bulk_api_coverage c ON c.base_url=p.scope_url '
                    'LEFT JOIN bulk_window_splits s ON c.base_url=s.scope_url '
                    'WHERE c.provider=? AND c.total_pages>30 AND s.scope_url IS NULL '
                    'AND p.page=1 AND p.total=c.total_articles', (source.provider,))}
            with self.db:
                for scope, pages in scopes.items():
                    endpoint = wordpress_base(scope)
                    query = parse_qs(urlsplit(scope).query)
                    if endpoint not in source.cnyes_apis or not {'startAt', 'endAt'} <= query.keys():
                        continue  # Never expand a retired/undeclared endpoint.
                    low, high = int(query['startAt'][0]), int(query['endAt'][0])
                    if high < low:
                        continue
                    parent = cnyes_url(endpoint, start=low, end=high)
                    owned_retirement = json.dumps({'pending_partition_scope': scope})
                    partitions = cnyes_window_partitions(parent, pages)
                    if partitions and scope in first_pages:
                        self.partition_cnyes_window(parent, pages, source, existing_jobs=jobs.get(scope, []))
                        self.counter('cnyes_window_v3_retained_scopes_partitioned', 1)
                        self.counter('cnyes_window_v3_new_publication_day_children', len(partitions))
                        continue
                    if not partitions:
                        for url, due, error, retirement in jobs.get(scope, []):
                            page = int(parse_qs(urlsplit(url).query).get('page', ['1'])[0])
                            if 2 <= page <= 30 and retirement == owned_retirement:
                                self.db.execute('UPDATE discovery SET superseded_by_windows_json=NULL WHERE url=?', (url,))
                                self.counter('cnyes_window_v3_permitted_jobs_restored', 1)
                        # Retain the actual source cap, not fake hourly slices
                        # or a completeness assertion from the first 900 rows.
                        self.db.execute('INSERT OR IGNORE INTO bulk_window_gaps VALUES (?,?,?,?)',
                                        (scope, source.provider, 'cnyes_single_day_page_cap', utc_now()))
                        observed = {r[0] for r in self.db.execute(
                            'SELECT page FROM bulk_page_evidence WHERE scope_url=?', (scope,))}
                        if 1 in observed:
                            first_missing = next((p for p in range(2, min(pages, 30) + 1) if p not in observed), None)
                            if first_missing is not None:
                                child = cnyes_url(endpoint, first_missing, start=low, end=high)
                                result = self.db.execute('INSERT OR IGNORE INTO discovery(url,kind,provider,priority) '
                                    'VALUES (?,\'cnyes_news\',?,?)', (child, source.provider, cnyes_history_priority(child)))
                                self.counter('cnyes_window_v3_missing_pages_seeded', result.rowcount)
                        continue  # Leave page-1 validators and due untouched.
                    retry_deadline = self.cnyes_partition_retry_deadline(jobs.get(scope, []))
                    for url, due, error, retirement in jobs.get(scope, []):
                        page = int(parse_qs(urlsplit(url).query).get('page', ['1'])[0])
                        if page == 1 and error is None:
                            self.db.execute('UPDATE discovery SET due=?,etag=NULL,last_modified=NULL WHERE url=?', (retry_deadline, url))
                            self.counter('cnyes_window_v3_missing_evidence_reobservations', 1)
                        elif page > 1 and retirement is None:
                            self.db.execute('UPDATE discovery SET superseded_by_windows_json=? WHERE url=?',
                                            (owned_retirement, url))
                    self.db.execute('INSERT OR IGNORE INTO discovery(url,kind,provider,priority,due) VALUES '
                                    '(?,\'cnyes_news\',?,?,?)', (parent, source.provider, cnyes_history_priority(parent), retry_deadline))
                self.db.execute('INSERT OR REPLACE INTO queue_settings VALUES (?,?)', (marker, fingerprint))

    @staticmethod
    def article_dispatch_statement(*, stored_only: bool = False, undated_only: bool = False,
                                   exclude_parser_repairs: bool = False):
        # Equality precedes the partial-index predicate; current SQLite would
        # otherwise sort the entire ready state. The stored lane has constant
        # work kind and only needs date ordering. Benchmarks reuse this SQL.
        if stored_only and undated_only:
            raise ValueError('stored recheck and undated missing-body lanes are distinct')
        if undated_only:
            # SQLite does not range-seek an OR over '' / NULL through this
            # expression index. Two fixed equality seeks merge the same order
            # without scanning every dated missing body or adding another index.
            # Numbered binds keep the public three-parameter query contract.
            base = ("SELECT * FROM articles INDEXED BY dispatch_work_kind "
                    "WHERE provider=?1 AND state=?2 AND due<=?3 "
                    "AND (body_sha256 IS NULL)=1 "
                    "AND state IN ('pending','retry','retry_cooldown') ")
            return (base + "AND metadata_order_day='' UNION ALL "
                    + base + "AND metadata_order_day IS NULL ORDER BY metadata_order_day DESC LIMIT 1")
        index = 'dispatch_nonparser_retry' if exclude_parser_repairs else 'dispatch_work_kind'
        parser_filter = ("AND state IN ('retry','retry_cooldown') AND (error_code IS NULL OR "
                         "error_code NOT IN ('body_not_found','article_attribution_not_verified')) "
                         if exclude_parser_repairs else '')
        return ("SELECT * FROM articles INDEXED BY " + index + " "
                "WHERE provider=? AND state=? AND due<=? "
                + parser_filter
                + ("AND (body_sha256 IS NULL)=0 " if stored_only else "")
                + "AND state IN ('pending','retry','retry_cooldown') "
                + ("ORDER BY metadata_order_day DESC LIMIT 1" if stored_only else
                   "ORDER BY (body_sha256 IS NULL) DESC,metadata_order_day DESC LIMIT 1"))

    @staticmethod
    def retry_dispatch_statement():
        return ("SELECT * FROM articles INDEXED BY dispatch_missing_retry WHERE provider=? AND due<=? "
                "AND body_sha256 IS NULL AND error_code IS NOT NULL AND state IN ('retry','retry_cooldown') "
                "ORDER BY due,metadata_order_day DESC LIMIT 1")

    @staticmethod
    def stored_retry_dispatch_statement():
        return ("SELECT * FROM articles INDEXED BY dispatch_stored_retry WHERE provider=? AND due<=? "
                "AND body_sha256 IS NOT NULL AND error_code IS NOT NULL AND state IN ('retry','retry_cooldown') "
                "ORDER BY due,metadata_order_day DESC LIMIT 1")

    def claim(self, provider: str, lease_seconds: int, *, missing_body_burst: int = 8,
              dated_body_burst: int = 8, pending_body_burst: int = 8, cnyes_primary_body_burst: int = 8,
              stored_body_retry_burst: int = 8,
              excluded_hosts=(), excluded_origins=()):
        """Prefer missing bodies, with bounded service for stored-body rechecks.

        An independent per-provider burst survives scheduling cycles of this
        owner. Dated missing bodies retain latest-first ordering; periodically
        serve an undated missing body without inventing a publication clock.
        Stored-body rechecks and due missing-body failures each keep separate
        bounded service. The retry lane is oldest-due, not newest-publication;
        a continuously growing pending frontier cannot starve recovery. Conversely,
        regular work prefers first downloads over newer known parser repairs;
        those repairs receive their bounded independent turn. Due network
        retries retain latest-first service instead of being demoted with
        parser failures. A due access refusal yields to ready first downloads
        in the regular lane, but keeps the oldest-due retry lane's bounded turn.
        This changes no source cooldown, per-row deadline or access policy.
        A regular-lane retry must not reset the oldest-due
        lane's independent service credit; only selecting that lane does.
        Within an already awarded stored-body slot, a separate burst serves
        the oldest due failed recheck. This does not borrow missing-body work
        or let ordinary latest-first rechecks starve a stored-body failure.
        Retry deadlines
        and leases remain mandatory in every lane. Bursts are dispatch fairness,
        not publisher quotas or a completeness proof. Cnyes additionally prefers
        missing bodies without a known canonical candidate, while periodically
        serving candidate mobile views. This is not alias or field acceptance.
        """
        if type(missing_body_burst) is not int or not 1 <= missing_body_burst <= 128:
            raise ValueError('invalid missing-body recheck burst')
        if type(dated_body_burst) is not int or not 1 <= dated_body_burst <= 128:
            raise ValueError('invalid dated-body undated burst')
        if type(pending_body_burst) is not int or not 1 <= pending_body_burst <= 128:
            raise ValueError('invalid pending-body retry burst')
        if type(cnyes_primary_body_burst) is not int or not 1 <= cnyes_primary_body_burst <= 128:
            raise ValueError('invalid cnyes primary-body candidate burst')
        if type(stored_body_retry_burst) is not int or not 1 <= stored_body_retry_burst <= 128:
            raise ValueError('invalid stored-body retry burst')
        source = self.registry.sources[provider]
        if source.acquisition_mode == "finite" and not self.db.execute("SELECT 1 FROM finite_cohorts WHERE provider=?", (provider,)).fetchone():
            return None
        if not source.allowed:
            return None  # A metadata-only capability never leases article bodies.
        bursts = getattr(self, '_missing_body_dispatch_bursts', {})
        dated_bursts = getattr(self, '_dated_body_dispatch_bursts', {})
        pending_bursts = getattr(self, '_pending_body_dispatch_bursts', {})
        stored_retry_bursts = getattr(self, '_stored_retry_dispatch_bursts', {})
        recheck_turn = bursts.get(provider, 0) >= missing_body_burst
        undated_turn = dated_bursts.get(provider, 0) >= dated_body_burst
        retry_turn = pending_bursts.get(provider, 0) >= pending_body_burst
        value_burst = getattr(self, '_cnyes_primary_body_burst', 0)
        candidate_turn = provider == 'cnyes' and value_burst >= cnyes_primary_body_burst
        now = time.time()
        from downloader.news_fulltext_host_circuit import host_filtered_query
        def lookup(statement, parameters):
            statement, parameters = host_filtered_query(statement, parameters, excluded_hosts, excluded_origins=excluded_origins)
            return self.db.execute(statement, parameters).fetchone()
        with self.metrics.measure("article_dispatch"), self.db:
            # A conservative existence check uses the already-maintained
            # covering index. Empty/future-only providers need none of the
            # fairness/order lanes below. No cache or new deadline: inserts,
            # rollback and a due-clock boundary are visible on the next call.
            # A positive result still runs every original scope/host filter.
            if not self.db.execute(ARTICLE_READY_PROBE_SQL, (provider, now)).fetchone():
                self.metrics.count(provider, 'frontier_no_due_work_selections')
                return None
            lanes = (['stored'] if recheck_turn else [])
            if undated_turn:
                lanes.append('undated')
            if retry_turn:
                lanes.append('retry')
            lanes.append('regular')
            candidates = []
            for lane in lanes:
                if lane == 'retry':
                    candidate = lookup(self.retry_dispatch_statement(), (provider, now))
                    if candidate:
                        candidates.append(candidate)
                else:
                    fresh_missing_ready = False
                    for state in ("pending", "retry", "retry_cooldown"):
                        if provider == 'cnyes' and lane == 'regular':
                            from downloader.news_fulltext_cnyes_work import candidate as cnyes_candidate
                            candidate = cnyes_candidate(self, state, now, candidate_turn,
                                exclude_parser_repairs=fresh_missing_ready and state != 'pending',
                                excluded_hosts=excluded_hosts, excluded_origins=excluded_origins)
                        else:
                            candidate = None
                        if candidate is None:
                            candidate = lookup(self.article_dispatch_statement(stored_only=lane == 'stored',
                                                    undated_only=lane == 'undated',
                                                    exclude_parser_repairs=lane == 'regular' and fresh_missing_ready and state != 'pending'),
                                                    (provider, state, now))
                        if candidate:
                            candidates.append(candidate)
                            if state == 'pending' and candidate['body_sha256'] is None:
                                fresh_missing_ready = True
                if candidates:
                    break  # A lane without due work never consumes an idle slot.
            # The newer refused URL otherwise wins after every source cooldown
            # and starves older first downloads. Use the already-found, bounded
            # candidates: no new query/index, due reset or publisher request.
            # An awarded retry/stored/undated turn retains its original share.
            if lane == 'regular' and any(r['state'] == 'pending' and r['body_sha256'] is None
                                         and r['error_code'] is None for r in candidates):
                ready = [r for r in candidates if not (r['body_sha256'] is None
                         and r['state'] in {'retry', 'retry_cooldown'}
                         and r['error_code'] == 'source_access_denied')]
                if len(ready) < len(candidates):
                    self.metrics.count(provider, 'frontier_access_refusal_fairness_deferrals')
                    candidates = ready
            row = max(candidates, key=lambda r: (r['body_sha256'] is None,
                (r['cnyes_known_body_candidate'] == candidate_turn) if provider == 'cnyes' and lane == 'regular' else True,
                r["metadata_order_day"] or "")) if candidates else None
            if row is None:
                return None
            # Replace only a slot already awarded to stored-body work. Missing
            # body, undated and missing retry quotas retain exactly their share.
            # This also applies when stored work falls through the regular lane
            # because no missing bodies are due. Empty/paused failure lanes
            # preserve credit and never idle an ordinary refresh.
            oldest_stored_retry = False
            if row['body_sha256'] is not None and stored_retry_bursts.get(provider, 0) >= stored_body_retry_burst:
                failed = lookup(self.stored_retry_dispatch_statement(), (provider, now))
                if failed is not None:
                    row, oldest_stored_retry = failed, True
            self.db.execute("UPDATE articles SET state='inflight',attempts=attempts+1,lease_owner=?,lease_until=? WHERE id=?",
                            (self.owner, now + lease_seconds, row["id"]))
        bursts[provider] = min(missing_body_burst, bursts.get(provider, 0) + 1) if row['body_sha256'] is None else 0
        self._missing_body_dispatch_bursts = bursts
        if row['body_sha256'] is None:
            dated_bursts[provider] = (min(dated_body_burst, dated_bursts.get(provider, 0) + 1)
                                      if row['metadata_order_day'] else 0)
            due_retry = row['state'] in {'retry', 'retry_cooldown'} and row['error_code'] is not None
            pending_bursts[provider] = (0 if lane == 'retry' else
                min(pending_body_burst, pending_bursts.get(provider, 0) + 1))
            if due_retry:
                self.metrics.count(provider, "frontier_due_missing_retry_dispatches")
                if lane == 'retry':
                    self.metrics.count(provider, 'frontier_oldest_due_retry_dispatches')
        self._dated_body_dispatch_bursts = dated_bursts
        self._pending_body_dispatch_bursts = pending_bursts
        if row['body_sha256'] is not None:
            stored_retry_bursts[provider] = (0 if oldest_stored_retry else
                min(stored_body_retry_burst, stored_retry_bursts.get(provider, 0) + 1))
            if row['state'] in {'retry', 'retry_cooldown'} and row['error_code'] is not None:
                self.metrics.count(provider, 'frontier_due_stored_retry_dispatches')
            if oldest_stored_retry:
                self.metrics.count(provider, 'frontier_oldest_due_stored_retry_dispatches')
        self._stored_retry_dispatch_bursts = stored_retry_bursts
        self.metrics.count(provider, "frontier_attempts_started")
        if provider == 'cnyes' and row['body_sha256'] is None:
            known = row['cnyes_known_body_candidate'] == 1
            self._cnyes_primary_body_burst = 0 if known else min(cnyes_primary_body_burst, value_burst + 1)
            self.metrics.count(provider, 'frontier_existing_body_candidate_dispatches' if known
                               else 'frontier_no_known_body_candidate_dispatches')
        self.metrics.count(provider, "frontier_missing_body_dispatches" if row['body_sha256'] is None else "frontier_stored_recheck_dispatches")
        if row['body_sha256'] is None and not row['metadata_order_day']:
            self.metrics.count(provider, "frontier_undated_missing_body_dispatches")
        return dict(row)

    @staticmethod
    def discovery_dispatch_filter(source, bulk_allowed=True, *, include_circuits=True):
        """Removed endpoints remain evidence, never an implicitly enabled job.

        Filter before LIMIT: skipping old jobs after selection could otherwise
        starve the registered RSS/API at the front of the same publisher queue.
        """
        if not source.allowed:
            feeds = source.metadata_feeds
            return ("kind='metadata_feed' AND url IN (" + ','.join('?' for _ in feeds) + ")", list(feeds)) if feeds else ("0", [])
        if source.acquisition_mode == "finite" or source.known_url_only:
            return "0", []  # Retain old discovery receipts, never poll them.
        clauses = ["kind NOT IN ('wordpress_posts','cnyes_news','rss_fulltext','metadata_feed')"]
        parameters = []
        if source.metadata_feeds:
            clauses.append("(kind='metadata_feed' AND url IN (" + ','.join('?' for _ in source.metadata_feeds) + "))")
            parameters.extend(source.metadata_feeds)
        if bulk_allowed:
            for kind, bases in (("wordpress_posts", source.wordpress_apis), ("cnyes_news", source.cnyes_apis),
                                ("rss_fulltext", source.fulltext_feeds)):
                if bases:
                    clauses.append("(kind=? AND substr(url,1,instr(url||'?','?')-1) IN (" + ','.join('?' for _ in bases) + "))")
                    parameters.extend((kind, *bases))
        result = '(' + ' OR '.join(clauses) + ')'
        if source.provider == 'cnyes':
            from downloader.news_fulltext_cnyes_index import ENDPOINT, request_failure_scope_guard
            if getattr(source,'cnyes_index_api',''):
                from downloader.news_fulltext_cnyes_index import compatible_fingerprints
                versions = compatible_fingerprints(source)
                scope_guard, scope_parameters = request_failure_scope_guard(source)
                result += (" AND (kind!='index' OR substr(url,1,instr(url||'?','?')-1)!=? OR NOT EXISTS "
                           "(SELECT 1 FROM bulk_endpoint_cooldowns AS circuit WHERE circuit.provider=? "
                           "AND circuit.endpoint=? AND circuit.data_fingerprint IN ("
                           + ','.join('?' for _ in versions) + ") AND circuit.not_before>?"
                           + scope_guard + "))")
                parameters.extend((ENDPOINT,source.provider,ENDPOINT,*versions,time.time()))
                parameters.extend(scope_parameters)
            else:
                result += " AND (kind!='index' OR substr(url,1,instr(url||'?','?')-1)!=?)"
                parameters.append(ENDPOINT)
        if source.cnyes_apis and bulk_allowed and include_circuits:
            result += (" AND (kind!='cnyes_news' OR NOT EXISTS (SELECT 1 FROM bulk_endpoint_cooldowns AS circuit "
                       "WHERE circuit.provider=? AND circuit.endpoint=substr(discovery.url,1,instr(discovery.url||'?','?')-1) "
                       "AND circuit.data_fingerprint=? AND circuit.not_before>?))")
            parameters.extend((source.provider, source.data_fingerprint, time.time()))
        return result, parameters

    @staticmethod
    def discovery_candidates_query(source, limit, *, bulk_allowed=True, excluded_origins=()):
        """Apply origin filters only to the outer row before its bounded LIMIT.

        Endpoint cooldowns contain correlated WHEREs and literal question
        marks. Never run the article-lane bind/filter rewrite over those guards.
        """
        from downloader.news_fulltext_host_circuit import host_filtered_query
        # SQLite cannot prove a partial-index predicate once the finite-scope
        # WHERE 0 is constant-folded. Keep that explicit no-discovery query
        # free of an index hint; it must remain a no-work result, not a crash.
        index_hint = '' if (source.acquisition_mode == 'finite' or source.known_url_only
                            or not source.request_enabled) else ' INDEXED BY discovery_ready_order'
        statement, binds = host_filtered_query(
            'SELECT * FROM discovery' + index_hint +
            ' WHERE provider=? AND due<=? AND superseded_by_windows_json IS NULL',
            (source.provider, time.time()), (), excluded_origins=excluded_origins)
        dispatch_filter, parameters = NewsQueue.discovery_dispatch_filter(source, bulk_allowed)
        statement += (" AND " + dispatch_filter + " ORDER BY kind='feed' DESC,"
                      "kind IN ('wordpress_posts','cnyes_news','rss_fulltext') DESC,priority,due,url LIMIT ?")
        return statement, (*binds, *parameters, limit)

    def discovery_candidates(self, source, limit, *, bulk_allowed=True, excluded_origins=()):
        statement, binds = self.discovery_candidates_query(source, limit, bulk_allowed=bulk_allowed,
                                                           excluded_origins=excluded_origins)
        with self.metrics.measure('discovery_dispatch', source.provider):
            readiness_rows = len(self.db.execute(DISCOVERY_READINESS_PROBE_SQL, binds[:2]).fetchall())
            if not readiness_rows:
                self.metrics.count(source.provider, 'discovery_idle_index_skips')
                return []
            # Any due row is only a reason to run the original selection. It
            # does not grant permission: endpoint circuits, origin exclusions,
            # finite scope, stable priority/ties and LIMIT still apply there.
            statement = discovery_selection_sql(statement, readiness_rows)
            self.metrics.count(source.provider, 'discovery_sparse_due_index_selections'
                if readiness_rows <= DISCOVERY_DUE_LOOKAHEAD_ROWS else 'discovery_dense_priority_index_selections')
            return self.db.execute(statement, binds).fetchall()

    def next_wake_delay(self, provider_cooldowns: dict, maximum: float = 60, *, request_scope_cooldowns=None) -> float:
        """Index lookups, not a full pending/frontier scan or busy polling."""
        now, deadlines = time.time(), []
        for provider, source in self.registry.sources.items():
            if not source.request_enabled:
                continue
            scopes = (request_scope_cooldowns or {}).get(provider, {})
            hosts = {host: until for host, until in scopes.get('hosts', {}).items() if until > now}
            origins = {origin: until for origin, until in scopes.get('origins', {}).items() if until > now}
            # Expiration permits an actual policy/request probe, not success.
            # Paused rows with due=0 must not create repeated empty cycles.
            earliest = list(hosts.values()) + list(origins.values())
            from downloader.news_fulltext_host_circuit import host_filtered_query
            for state in ("pending", "retry", "retry_cooldown") if source.allowed else ():
                statement, binds = host_filtered_query(
                    'SELECT due FROM articles INDEXED BY dispatch WHERE provider=? AND state=? ORDER BY due LIMIT 1',
                    (provider, state), tuple(hosts), excluded_origins=tuple(origins))
                row = self.db.execute(statement, binds).fetchone()
                if row:
                    earliest.append(row[0])
            bulk_allowed = self.bulk_allowed(source, now)
            # An unusable bulk adapter must not hold HTML/RSS behind its timer.
            dispatch_filter, parameters = self.discovery_dispatch_filter(source, bulk_allowed)
            statement, binds = host_filtered_query(
                'SELECT min(due) FROM discovery WHERE provider=? AND superseded_by_windows_json IS NULL',
                (provider,), (), excluded_origins=tuple(origins))
            row = self.db.execute(statement + ' AND ' + dispatch_filter, (*binds, *parameters)).fetchone()
            if row and row[0] is not None:
                earliest.append(row[0])
            if earliest:
                deadlines.append(max(min(earliest), provider_cooldowns.get(provider, 0)))
        return min(maximum, max(0.0, min(deadlines) - now)) if deadlines else maximum

    def claim_url(self, url: str, lease_seconds: int, *, refresh: bool = False):
        """Explicit article selection still respects persisted retry deadlines."""
        url = normalize_url(url)
        source = self.registry.require(url)
        if source.acquisition_mode == "finite":
            original = self.db.execute("SELECT 1 FROM finite_cohort_members WHERE provider=? AND url=?", (source.provider, url)).fetchone()
            supplement = source.allow_reviewed_supplements and self.db.execute(
                "SELECT 1 FROM finite_supplement_members WHERE provider=? AND url=?", (source.provider, url)).fetchone()
            if not original and not supplement:
                return None
        now = time.time()
        with self.db:
            row = self.db.execute("SELECT * FROM articles WHERE url=?", (url,)).fetchone()
            if row is None or row["due"] > now or row["state"] in {"inflight", "waiting_cohort"}:
                return None
            if row["body_sha256"] and row["state"] == "complete" and not refresh:
                return None
            self.db.execute("UPDATE articles SET state='inflight',attempts=attempts+1,lease_owner=?,lease_until=? WHERE id=?",
                            (self.owner, now + lease_seconds, row["id"]))
        self.metrics.count(row["provider"], "frontier_attempts_started")
        return dict(row)

    def schedule_recent_rechecks(self, seconds: int, days: int):
        now = datetime.now(timezone.utc)
        cutoff = (now - timedelta(seconds=seconds)).isoformat()
        start_day = (now - timedelta(days=days)).date().isoformat()
        with self.db:
            for source in self.registry.sources.values():
                if source.allowed and source.acquisition_mode != "finite":
                    self.db.execute("UPDATE articles SET state='retry',due=0,attempts=0,error_code=NULL "
                                    "WHERE provider=? AND state='complete' AND metadata_order_day>=? "
                                    "AND coalesce(last_checked_at_utc,captured_at_utc)<?", (source.provider, start_day, cutoff))

    def failed(self, row: dict, error: NewsError, max_attempts: int):
        attempts = row["attempts"] + 1
        deferred = (getattr(error, 'request_deferred', False) is True and error.retry is True
                    and error.code in REQUEST_DEFERRAL_CODES)
        unknown_body = (error.code == 'body_not_found' and self.unknown_cnyes_body_recheck_allowed(
            self.registry.sources.get(row['provider']), row))
        unknown_identity = (error.code == 'article_identity_not_verified' and self.unknown_cnyes_body_recheck_allowed(
            self.registry.sources.get(row['provider']), row))
        recoverable = error.retry or unknown_body or unknown_identity
        retry = recoverable and (deferred or attempts < max_attempts)
        # Bound immediate retries, not the lifetime of a recoverable network
        # failure. Exhausted bursts cool down for an hour, then start a new
        # bounded burst. Permanent source/rights outcomes are not reset.
        cooldown = recoverable and not retry
        state = "retry" if retry else "retry_cooldown" if cooldown else error.code
        delay = max(error.delay, CNYES_UNKNOWN_BODY_RECHECK_SECONDS if unknown_body or unknown_identity else 0,
                    0 if deferred else 3600 if cooldown else min(900, 15 * 2 ** min(attempts, 6)))
        # Preserve the pre-claim failure burst when a verified source gate or
        # graceful stop postpones this request. Never reset old errors/deadlines
        # at startup, refund real HTTP requests, or weaken provider-level policy.
        with self.db:
            self.db.execute("UPDATE articles SET state=?,error_code=?,due=?,last_checked_at_utc=?,"
                            "attempts=CASE WHEN ? THEN ? WHEN ? THEN 0 ELSE attempts END,"
                            "lease_owner=NULL,lease_until=NULL WHERE id=?",
                            (state, error.code, time.time() + delay if recoverable else 0, utc_now(),
                             deferred, row['attempts'], cooldown,
                             row["id"]))
            self.counter("article_attempts_finished", 1)
            self.counter("article_failures", 1)
        self.metrics.count(row["provider"], "frontier_attempts_failed")
        self.metrics.count(row["provider"], "frontier_error_" + error.code)
        if deferred:
            self.metrics.count(row['provider'], 'frontier_source_gate_deferrals')
            self.metrics.count(row['provider'], 'frontier_deferred_' + error.code)
        if (row['provider'] == 'ftvnews' and error.code == 'article_rights_or_scope_excluded'
                and getattr(error, 'ftv_native_scope_witness', None) == 'standalone_visible_CNA_credit_v1'):
            self.metrics.count('ftvnews', 'native_standalone_CNA_credit_exclusions_v1')
        stage = getattr(error, 'cnyes_native_body_failure_stage', None)
        if (row['provider'] == 'cnyes' and error.code == 'body_not_found'
                and type(stage) is str and stage in CNYES_NATIVE_BODY_FAILURE_STAGES):
            # Reuse the failed extractor's static branch witness. No body,
            # arbitrary metric names, extra parse, file or HTTP request. The
            # original error/deadline and diagnostic daily budget stay intact.
            self.metrics.count('cnyes', 'native_body_failure_stage_' + stage)
        if unknown_body:
            self.metrics.count('cnyes', 'unknown_body_failure_rechecks_deferred')
        if unknown_identity:
            self.metrics.count('cnyes', 'unknown_identity_failure_rechecks_deferred')
        if getattr(error, 'failure_response', None) is not None:
            from downloader.news_fulltext_diagnostics import retain_failure_response
            retain_failure_response(self, row, error)

    def unchanged(self, row: dict, max_attempts: int = 6):
        if not row.get("parquet_path") or not row.get("body_sha256"):
            self.failed(row, NewsError("unverifiable_not_modified"), 1)
            return
        try:
            target = source_path(self.root, row["parquet_path"])
            accepted = self.db.execute("SELECT receipt_path FROM accepted_batches WHERE path=?", (row["parquet_path"],)).fetchone()
            receipt = json.loads(source_path(self.root, accepted[0]).read_text()) if accepted else None
            if receipt is None or sha256_file(target) != receipt["sha256"]:
                raise ValueError("saved_body_digest_mismatch")
        except (OSError, ValueError, KeyError):
            with self.db:
                self.db.execute("UPDATE articles SET etag=NULL,last_modified=NULL WHERE id=?", (row["id"],))
            self.failed(row, NewsError("saved_body_unavailable", retry=True, delay=1), max_attempts)
            return
        with self.db:
            self.db.execute("UPDATE articles SET state='complete',last_checked_at_utc=?,lease_owner=NULL,lease_until=NULL,"
                            "error_code=NULL WHERE id=?", (utc_now(), row["id"]))
            self.counter("article_attempts_finished", 1)
            self.counter("not_modified", 1)
        self.metrics.count(row["provider"], "frontier_not_modified")

    def accept(self, records: list[dict]):
        if not records:
            return
        for record in records:
            if record.get('retained_metadata_head_json') is not None and not self._metadata_head_matches(record):
                raise ValueError('retained_metadata_head_changed_before_acceptance')
            if record.get('retained_supplement_head_json') is not None and not self._supplement_head_matches(record):
                raise ValueError('retained_supplement_head_changed_before_acceptance')
            if record.get('retained_failure_head_json') is not None and not self._failure_replay_head_matches(record):
                raise ValueError('retained_failure_head_changed_before_acceptance')
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "-" + uuid.uuid4().hex[:8]
        relative = f"bodies/{stamp[:8]}/{stamp}.parquet"
        target = self.root / relative
        with self.metrics.measure("batch_build"):
            table = table_from_records(records)
        with self.metrics.measure("parquet_compress_fsync"):
            atomic_write_parquet(target, table, compression="zstd", compression_level=9, durable=True)
        receipt_relative = "receipts/" + stamp + ".json"
        with self.metrics.measure("receipt_hash_fsync"):
            atomic_write_json(self.root / receipt_relative, {
                "contract_version": CONTRACT_VERSION, "parquet_path": relative, "sha256": sha256_file(target),
                "rows": len(records), "text_bytes": sum(r["body_bytes"] for r in records),
                "raw_response_bytes": sum(len(r.get("raw_response_body") or b"") + len(r.get("url_resolution_response_body") or b"") for r in records),
                "canonical_resolution_response_bytes": sum(len(r.get("url_resolution_response_body") or b"") for r in records),
                "accepted_at_utc": utc_now(), "publish": False, "historical_point_in_time": False,
            })
        with self.metrics.measure("acceptance_sqlite_commit"):
            # Count new URL heads separately from revisions/rechecks, using a
            # single indexed lookup per bounded chunk, not a full lake scan.
            ids = {bytes.fromhex(row["article_id"]): row["provider"] for row in records}
            from downloader.news_fulltext_metrics import record_workload
            workloads = {bytes.fromhex(row["article_id"]): record_workload(row) for row in records}
            new_heads = Counter()
            new_workload_heads = Counter()
            keys = list(ids)
            for offset in range(0, len(keys), 500):
                chunk = keys[offset:offset + 500]
                for row in self.db.execute("SELECT id FROM articles WHERE body_sha256 IS NULL AND id IN ("
                                           + ",".join("?" for _ in chunk) + ")", chunk):
                    new_heads[ids[row[0]]] += 1
                    new_workload_heads[(ids[row[0]], workloads[row[0]])] += 1
            metadata_heads = self._accept_records(records, relative, receipt_relative)
        for provider, count in Counter(row["provider"] for row in records).items():
            self.metrics.count(provider, "durably_accepted_versions", count)
        for provider, count in Counter(row['provider'] for row in records
                                       if row.get('acquisition_transport') == 'retained_raw_reparse').items():
            self.metrics.count(provider, 'durably_accepted_cached_versions', count)
        for provider, count in new_heads.items():
            self.metrics.count(provider, "durably_accepted_new_url_heads", count)
        for (provider, workload), count in new_workload_heads.items():
            self.metrics.count(provider, "durable_workload_" + workload + "_new_heads", count)
            if workload == 'cached_source_reparse':
                self.metrics.count(provider, 'durably_accepted_cached_new_url_heads', count)
        return metadata_heads

    def _metadata_head_matches(self, record):
        from downloader.news_fulltext_metadata_replay import HEAD_FIELDS, METADATA_REPLAY_VERSION
        if (record.get('metadata_replay_contract_version') not in {1, METADATA_REPLAY_VERSION}
                or record.get('acquisition_transport') != 'retained_raw_reparse'
                or record.get('replay_preserves_last_http_check') is not True):
            raise ValueError('invalid retained metadata acceptance contract')
        expected = json.loads(record['retained_metadata_head_json'])
        if (set(expected) != set(HEAD_FIELDS) or expected['state'] != 'complete'
                or expected['lease_owner'] is not None or expected['lease_until'] is not None
                or expected['captured_at_utc'] != record['captured_at_utc']):
            raise ValueError('invalid retained metadata head witness')
        head = self.db.execute('SELECT * FROM articles WHERE id=?',
                               (bytes.fromhex(record['article_id']),)).fetchone()
        return head is not None and all(head[key] == expected[key] for key in HEAD_FIELDS)

    def _supplement_head_matches(self, record):
        """A crash-replayed diagnostic cannot overwrite a later HTTP/refusal head."""
        from downloader.news_fulltext_cohorts import RETAINED_SUPPLEMENT_HEAD_FIELDS, RETAINED_SUPPLEMENT_REPLAY_VERSION
        if (record.get('reviewed_supplement_replay_contract_version') != RETAINED_SUPPLEMENT_REPLAY_VERSION
                or record.get('acquisition_transport') != 'retained_raw_reparse'):
            raise ValueError('invalid retained supplement acceptance contract')
        expected = json.loads(record['retained_supplement_head_json'])
        if (set(expected) != set(RETAINED_SUPPLEMENT_HEAD_FIELDS) or expected['state'] != 'inflight'
                or expected['body_sha256'] is not None or not expected['lease_owner']
                or expected['url'] != record['requested_url'] or expected['provider'] != record['provider']):
            raise ValueError('invalid retained supplement missing-head witness')
        try:
            source = self.registry.require(record['requested_url'])
        except NewsError:
            return False
        if (not source.allow_reviewed_supplements or source.data_fingerprint != record['data_fingerprint']
                or source.authorization_fingerprint != record['authorization_fingerprint']
                or not self.db.execute('SELECT 1 FROM finite_supplement_members WHERE provider=? AND url=? AND scope_sha256=?',
                    (source.provider, record['requested_url'], record['reviewed_supplement_scope_sha256'])).fetchone()):
            return False
        head = self.db.execute('SELECT * FROM articles WHERE id=?', (bytes.fromhex(record['article_id']),)).fetchone()
        if head is None:
            return False
        if all(head[key] == expected[key] for key in RETAINED_SUPPLEMENT_HEAD_FIELDS):
            return True
        # The exclusive owner's startup only releases its abandoned lease.
        # All source checks, HTTP clocks, attempts and metadata must still match.
        restart_fields = {'state', 'due', 'error_code', 'lease_owner', 'lease_until'}
        return (head['state'] == 'retry' and head['due'] == 0 and head['error_code'] == 'owner_restart'
                and head['lease_owner'] is None and head['lease_until'] is None
                and all(head[key] == expected[key] for key in RETAINED_SUPPLEMENT_HEAD_FIELDS if key not in restart_fields))

    def _failure_replay_head_matches(self, record):
        """A retained normal response cannot restore a superseded URL head."""
        from downloader.news_fulltext_cohorts import RETAINED_SUPPLEMENT_HEAD_FIELDS
        from downloader.news_fulltext_replay import RETAINED_FAILURE_REPLAY_VERSION
        if (record.get('retained_failure_replay_contract_version') != RETAINED_FAILURE_REPLAY_VERSION
                or record.get('acquisition_transport') != 'retained_raw_reparse'):
            raise ValueError('invalid retained failure acceptance contract')
        expected = json.loads(record['retained_failure_head_json'])
        if (set(expected) != set(RETAINED_SUPPLEMENT_HEAD_FIELDS) or expected['state'] != 'inflight'
                or expected['body_sha256'] is not None or not expected['lease_owner']
                or expected['url'] != record['requested_url'] or expected['provider'] != 'cnyes'):
            raise ValueError('invalid retained failure missing-head witness')
        source = self.registry.require(record['requested_url'])
        if (not source.allowed or source.provider != 'cnyes' or source.acquisition_mode == 'finite'
                or source.data_fingerprint != record['data_fingerprint']
                or source.authorization_fingerprint != record['authorization_fingerprint']):
            return False
        if not self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='failure_evidence_focus_attempts'").fetchone():
            return False
        head = self.db.execute('SELECT * FROM articles WHERE id=?', (bytes.fromhex(record['article_id']),)).fetchone()
        ledger = self.db.execute('''SELECT 1 FROM failure_evidence_focus_attempts WHERE provider='cnyes'
            AND url_sha256=? AND attempt_number=? AND cohort_id=? AND receipt_path=? AND response_sha256=? AND outcome='raw_saved' ''',
            (bytes.fromhex(record['article_id']), record['retained_failure_attempt_number'],
             record['retained_failure_scope_id'], record['replay_source_receipt'], record['response_sha256'])).fetchone()
        if head is None or ledger is None:
            return False
        if all(head[key] == expected[key] for key in RETAINED_SUPPLEMENT_HEAD_FIELDS):
            return True
        restart_fields = {'state', 'due', 'error_code', 'lease_owner', 'lease_until'}
        return (head['state'] == 'retry' and head['due'] == 0 and head['error_code'] == 'owner_restart'
                and head['lease_owner'] is None and head['lease_until'] is None
                and all(head[key] == expected[key] for key in RETAINED_SUPPLEMENT_HEAD_FIELDS if key not in restart_fields))

    def _accept_records(self, records: list[dict], relative: str, receipt_relative: str):
        metadata_heads, http_attempts = 0, 0
        with self.db:
            for record in records:
                if record.get('retained_failure_head_json') is not None and not self._failure_replay_head_matches(record):
                    self.counter('retained_failure_superseded_at_commit', 1)
                    continue
                if record.get('retained_supplement_head_json') is not None and not self._supplement_head_matches(record):
                    self.counter('retained_supplement_superseded_at_commit', 1)
                    continue
                if record.get('retained_metadata_head_json') is not None:
                    # A durable derived receipt can survive a crash before its
                    # SQLite commit. Never restore it over a newer HTTP head,
                    # active lease, refusal, or changed retry/check state.
                    if not self._metadata_head_matches(record):
                        self.counter('retained_metadata_superseded_at_commit', 1)
                        continue
                    self.db.execute('UPDATE articles SET body_bytes=?,body_sha256=?,parquet_path=? WHERE id=?',
                                    (record['body_bytes'], record['body_sha256'], relative,
                                     bytes.fromhex(record['article_id'])))
                    metadata_heads += 1
                    continue
                self.db.execute("""UPDATE articles SET state='complete',error_code=NULL,captured_at_utc=?,last_checked_at_utc=?,body_bytes=?,
                    body_sha256=?,parquet_path=?,etag=?,last_modified=?,authorization_fingerprint=?,
                    metadata_order_day=CASE WHEN coalesce(metadata_order_day,'')='' THEN ? ELSE metadata_order_day END,
                    lease_owner=NULL,lease_until=NULL WHERE id=?""",
                    (record["captured_at_utc"], (json.loads(record['retained_failure_head_json'])['last_checked_at_utc']
                        if record.get('retained_failure_head_json') is not None else record["captured_at_utc"]), record["body_bytes"], record["body_sha256"], relative,
                     record.get("etag"), record.get("last_modified"), record["authorization_fingerprint"],
                     accepted_publication_sort_day(record.get('published_at_raw')),
                     bytes.fromhex(record["article_id"])))
                http_attempts += 1
            self.db.execute("INSERT INTO accepted_batches VALUES (?,?)", (relative, receipt_relative))
            self.counter("article_attempts_finished", http_attempts)
            self.counter("accepted_versions", len(records))
            if metadata_heads:
                self.counter('retained_metadata_versions', metadata_heads)
        return metadata_heads

    def recover_acceptance(self, *, receipt_paths=None):
        # Only small commit receipts, not a recursive raw-data scan. Replay files
        # written before a crash between durable receipt and SQLite commit.
        # Read every JSON as before, including already committed receipts. A
        # covering indexed seek in bounded chunks replaces one SQL round trip
        # per historical receipt. Never skip orphan digest/row validation or
        # use filename/mtime as proof. Explicit paths serve read-only benchmarks.
        paths = (sorted_acceptance_receipt_paths(self.root) if receipt_paths is None
                 else iter(sorted(receipt_paths)))
        receipt_directory = self.root / 'receipts'
        with self.metrics.measure('acceptance_recovery'):
            while chunk := list(islice(paths, 128)):
                # Glob supplies direct children of this exact directory. Avoid
                # Python 3.12 relative_to walking ancestor paths for each one;
                # nonstandard explicit paths keep the original guarded route.
                relative_receipts = [('receipts/' + path.name if path.parent == receipt_directory
                                      else str(path.relative_to(self.root))) for path in chunk]
                committed = {tuple(row) for row in self.db.execute('SELECT receipt_path,path FROM accepted_batches '
                    'INDEXED BY accepted_batches_receipt_path WHERE receipt_path IN ('
                    + ','.join('?' for _ in chunk) + ')', relative_receipts)}
                for receipt_path, receipt_relative in zip(chunk, relative_receipts):
                    self._recover_acceptance_receipt(receipt_path, receipt_relative, committed)

    def _recover_acceptance_receipt(self, receipt_path, receipt_relative, committed, *, receipt_text=None):
        # The explicit text seam is for isolated read-only I/O benchmarks; the
        # production owner always reads each original file synchronously.
        # Decode the entire original input, including committed receipts. Reuse
        # the optional private-cache decoder; its stdlib fallback preserves
        # legacy JSON values/errors. Default-text input semantics, replay order and orphan
        # digest/row/contract verification are unchanged.
        receipt = decode_private_json(read_acceptance_receipt_input(receipt_path) if receipt_text is None else receipt_text)
        relative = receipt["parquet_path"]
        if ((receipt_relative, relative) in committed
                or self.db.execute("SELECT 1 FROM accepted_batches WHERE path=?", (relative,)).fetchone()):
            return
        target = source_path(self.root, relative)
        if receipt["contract_version"] != CONTRACT_VERSION or sha256_file(target) != receipt["sha256"]:
            raise ValueError("news_acceptance_recovery_digest_mismatch")
        table = pq.read_table(target)
        if table.num_rows != receipt["rows"]:
            raise ValueError("news_acceptance_recovery_rows_mismatch")
        self._accept_records(table.to_pylist(), relative, receipt_relative)

    def status(self, *, requests_by_provider: dict | None = None, response_bytes: int = 0,
               run_seconds: float = 0, accepted_this_run: int = 0, activity: str = "idle",
               provider_cooldowns: dict | None = None) -> dict:
        from downloader.news_fulltext_urls import original_source_inventory, original_url_progress
        from downloader.news_fulltext_cohorts import NONBODY_SCOPE_PROGRESS_QUERY_VERSION
        from downloader.news_fulltext_cnyes_work import CONTRACT_VERSION as work_version, LOOKUP_DISCOVERY_VERSION
        requests_by_provider = dict(requests_by_provider or {})
        if provider_cooldowns is None and hasattr(self, 'provider_cooldown_snapshot'):
            provider_cooldowns = self.provider_cooldown_snapshot()
        provider_cooldowns = dict(provider_cooldowns or {})
        providers = {}
        for provider, state, count, body_bytes in self.db.execute(
                "SELECT provider,state,articles,body_bytes FROM state_metrics WHERE articles>0"):
            item = providers.setdefault(provider, {"states": {}, "text_bytes": 0})
            item["states"][state] = count
            item["text_bytes"] += body_bytes or 0
        # Transactional head counters are O(providers), not an articles-table
        # scan per heartbeat. Rechecks/failures retain old bodies and must not
        # make their stored-body progress vanish with the complete state.
        stored_heads = dict(self.db.execute("SELECT provider,heads FROM stored_body_metrics"))
        with self.metrics.measure('status_original_source_inventory'):
            original_sources = original_source_inventory(self.db, self.registry)
        for provider, evidence in original_sources['sources'].items():
            item = providers.setdefault(provider, {"states": {}, "text_bytes": 0})
            item['original_metadata'] = {
                'contract_version': original_sources['contract_version'],
                'literal_metadata_links': evidence['literal_metadata_links'],
                'original_hosts': len(evidence['hosts']),
            }
        for provider, item in providers.items():
            source = self.registry.sources.get(provider)
            item["stored_body_urls"] = stored_heads.get(provider, 0)
            item["configured"] = source is not None
            item["acquisition_mode"] = source.acquisition_mode if source else "unreviewed"
            if source and source.acquisition_mode == "finite":
                from downloader.news_fulltext_cohorts import cohort_progress
                item["finite_cohort"] = cohort_progress(self.db, provider)
                from downloader.news_fulltext_cohorts import supplement_progress
                item["finite_supplements"] = supplement_progress(self.db, provider)
            item["automatic_fulltext_allowed"] = bool(source and source.allowed)
            item["authorization_kind"] = source.authorization.get("kind") if source else "unreviewed"
            item["automatic_collection_enabled"] = bool(source and source.request_enabled)
            if source and source.metadata_feeds:
                from downloader.news_fulltext_metadata_feeds import progress
                item['metadata_feed_progress'] = progress(self.db, source)
            item["rights_status"] = ("not_cleared_for_reuse" if source and source.authorization.get("kind") == "personal_research"
                                     else "source_license_scope_only" if source and source.allowed else "unreviewed_or_restricted")
        totals = Counter()
        for item in providers.values():
            totals.update(item["states"])
        eligible_remaining = sum(totals[s] for s in ("pending", "retry", "retry_cooldown", "inflight"))
        rate = accepted_this_run / run_seconds if run_seconds > 0 and accepted_this_run >= 3 else None
        origins = self.db.execute("SELECT count(*),coalesce(sum(rows),0),coalesce(sum(invalid_urls),0) FROM origins "
                                  "WHERE id IN (SELECT max(id) FROM origins WHERE id NOT IN "
                                  "(SELECT origin_id FROM metadata_ingest_progress) GROUP BY receipt_path)").fetchone()
        metadata_pending = self.db.execute("SELECT count(*),coalesce(sum(processed_rows),0),coalesce(sum(expected_rows),0) "
                                           "FROM metadata_ingest_progress WHERE state='pending'").fetchone()
        origin_versions = self.db.execute("SELECT count(*) FROM origins").fetchone()[0]
        counters = dict(self.db.execute("SELECT key,value FROM counters"))
        with self.metrics.measure('status_metrics_snapshot'):
            performance = self.metrics.snapshot()
        elapsed = performance["owner_elapsed_seconds"]
        if 'cnyes' in providers:
            from downloader.news_fulltext_cnyes_work import progress as cnyes_work_progress
            providers['cnyes']['body_work_progress'] = cnyes_work_progress(self.db)
            from downloader.news_fulltext_cnyes_history import progress as cnyes_history_progress
            providers['cnyes']['history_list_progress'] = cnyes_history_progress(self.db, self.registry.sources.get('cnyes'))
            from downloader.news_fulltext_cnyes_index import progress as cnyes_index_progress
            index_clock = datetime.fromisoformat(performance['snapshot_at_utc']).timestamp()
            with self.metrics.measure('status_cnyes_public_index_projection'):
                providers['cnyes']['public_index_progress'] = cnyes_index_progress(
                    self.db, self.registry.sources.get('cnyes'), owner_start_epoch=index_clock - elapsed,
                    now_epoch=index_clock, activity=activity)
        for provider, item in providers.items():
            observed = performance["providers"].get(provider, {}).get("counts", {}).get("durably_accepted_versions", 0)
            owner_rate = observed / elapsed if observed >= 10 and elapsed >= 60 else None
            remaining = sum(item["states"].get(state, 0) for state in ("pending", "retry", "retry_cooldown", "inflight"))
            item["observed_owner_articles_per_second"] = owner_rate
            item["remaining_eligible_estimate_seconds"] = remaining / owner_rate if owner_rate else None
            item["eta_basis"] = "conditional projection from this owner's full wall time; includes waits, failures, ingestion and idle"
        eligible_provider_etas = [item["remaining_eligible_estimate_seconds"] for item in providers.values()
                                 if any(item["states"].get(state, 0) for state in ("pending", "retry", "retry_cooldown", "inflight"))]
        eta = max(eligible_provider_etas) if eligible_provider_etas and all(value is not None for value in eligible_provider_etas) else None
        enabled_bulk_pages, registered_bulk_pages = 0, 0
        # Project all scopes once per real endpoint, not one heartbeat entry per
        # history window plus its full child list. The authoritative scopes,
        # split graph and receipts remain unchanged in SQLite. This bounds the
        # JSON projection by source endpoints rather than years of partitions.
        bulk_endpoints = summarize_bulk_coverage(self.db, self.registry)
        for source in self.registry.sources.values():
            if not source.allowed or not (source.wordpress_apis or source.cnyes_apis or source.fulltext_feeds):
                continue
            for active in (False, True):
                clause, params = self.discovery_dispatch_filter(source, self.bulk_allowed(source) if active else True,
                                                                include_circuits=active)
                count = self.db.execute("SELECT count(*) FROM discovery WHERE provider=? "
                                        "AND kind IN ('wordpress_posts','cnyes_news','rss_fulltext') "
                                        "AND superseded_by_windows_json IS NULL AND due<=? AND " + clause,
                                        (source.provider, time.time(), *params)).fetchone()[0]
                if active:
                    enabled_bulk_pages += count
                else:
                    registered_bulk_pages += count
        with self.metrics.measure('status_discovery_projection'):
            discovery_pending, discovery_errors, discovery_superseded, discovery_archived_errors, all_bulk_pages = summarize_discovery_status(self.db, time.time())
        from downloader.news_fulltext_diagnostics import FAILURE_DIAGNOSTIC_BUDGET_CONTRACT_VERSION
        from downloader.news_fulltext_bulk import CNYES_WINDOW_PARTITION_CONTRACT_VERSION, CNYES_QUERY_DAY_OFFSET_SECONDS
        from downloader.news_fulltext_cnyes_index import (
            FAILURE_SCOPE_CONTRACT_VERSION, FAILURE_DEADLINE_CONTRACT_VERSION,
            FAILURE_DIAGNOSTIC_CONTRACT_VERSION as INDEX_FAILURE_DIAGNOSTIC_VERSION,
        )
        gap_samples, gap_projection = summarize_bulk_window_gaps(self.db)
        with self.metrics.measure('status_bulk_item_error_count'):
            item_errors = bulk_item_error_count(self.db)
        with self.metrics.measure('status_http_capacity_snapshot'):
            capacity = self.http_capacity_snapshot() if hasattr(self, 'http_capacity_snapshot') else None
        result = {
            "contract_version": CONTRACT_VERSION, "updated_at_utc": utc_now(), "activity": activity,
            "status_projection_contract_version": STATUS_PROJECTION_CONTRACT_VERSION,
            "status_query_timing_contract_version": STATUS_QUERY_TIMING_CONTRACT_VERSION,
            "bulk_item_error_count_contract_version": BULK_ERROR_COUNT_CONTRACT_VERSION,
            "status_json_encoding": "compact_utf8",
            "provider_stored_body_counter_contract_version": 1,
            "original_source_inventory": {key: value for key, value in original_sources.items() if key != 'sources'},
            "article_failure_observation_contract_version": FAILURE_OBSERVATION_CONTRACT_VERSION,
            "failure_diagnostic_budget_contract_version": FAILURE_DIAGNOSTIC_BUDGET_CONTRACT_VERSION,
            **({'failure_diagnostic_focus': self.failure_diagnostic_focus.progress()}
               if getattr(self, 'failure_diagnostic_focus', None) is not None else {}),
            "acceptance_receipt_decode_version": ACCEPTANCE_RECEIPT_DECODE_VERSION,
            "acceptance_receipt_input_version": ACCEPTANCE_RECEIPT_INPUT_VERSION,
            "nonbody_scope_progress_query_version": NONBODY_SCOPE_PROGRESS_QUERY_VERSION,
            "article_dispatch_contract_version": ARTICLE_DISPATCH_CONTRACT_VERSION,
            "cnyes_window_dispatch_contract_version": CNYES_WINDOW_DISPATCH_CONTRACT_VERSION,
            "cnyes_window_partition_contract_version": CNYES_WINDOW_PARTITION_CONTRACT_VERSION,
            "cnyes_query_day_offset_seconds": CNYES_QUERY_DAY_OFFSET_SECONDS,
            "article_request_deferral_contract_version": REQUEST_DEFERRAL_CONTRACT_VERSION,
            "stored_body_retry_fairness_contract_version": STORED_BODY_RETRY_FAIRNESS_CONTRACT_VERSION,
            "discovery_failure_observation_contract_version": DISCOVERY_FAILURE_OBSERVATION_CONTRACT_VERSION,
            "discovery_status_projection_contract_version": DISCOVERY_STATUS_PROJECTION_CONTRACT_VERSION,
            "discovery_summary_contract_version": DISCOVERY_SUMMARY_CONTRACT_VERSION,
            "discovery_readiness_probe_contract_version": DISCOVERY_READINESS_PROBE_CONTRACT_VERSION,
            "article_recheck_day_contract_version": ARTICLE_RECHECK_DAY_CONTRACT_VERSION,
            "cnyes_unknown_body_recheck_contract_version": CNYES_UNKNOWN_BODY_RECHECK_CONTRACT_VERSION,
            "cnyes_unknown_identity_recheck_contract_version": CNYES_UNKNOWN_IDENTITY_RECHECK_CONTRACT_VERSION,
            "cnyes_work_dispatch_contract_version": work_version,
            "cnyes_index_failure_scope_contract_version": FAILURE_SCOPE_CONTRACT_VERSION,
            "cnyes_index_failure_deadline_contract_version": FAILURE_DEADLINE_CONTRACT_VERSION,
            "cnyes_index_failure_diagnostic_contract_version": INDEX_FAILURE_DIAGNOSTIC_VERSION,
            "cnyes_native_lookup_discovery_contract_version": LOOKUP_DISCOVERY_VERSION,
            "owner_id": self.owner,
            "unique_articles": sum(totals.values()), "states": dict(totals), "providers": providers,
            "accepted_text_bytes": sum(p["text_bytes"] for p in providers.values()),
            "articles_with_stored_body": sum(stored_heads.values()),
            "metadata_partitions": origins[0], "metadata_association_rows": origins[1], "invalid_metadata_urls": origins[2],
            "metadata_inventory": {"contract_version": METADATA_INGEST_CONTRACT_VERSION, "partial_partitions": metadata_pending[0],
                                   "partial_rows_imported": metadata_pending[1], "partial_rows_expected": metadata_pending[2],
                                   "complete_receipts_only_in_metadata_totals": True},
            "metadata_partition_versions_retained": origin_versions,
            "metadata_association_basis": "latest accepted version per source receipt; original stock associations retained",
            "original_url_resolution": original_url_progress(self),
            "metadata_partition_errors": self.db.execute("SELECT count(*) FROM origin_errors").fetchone()[0],
            "discovery_pending": discovery_pending,
            "discovery_errors": discovery_errors,
            "discovery_superseded": discovery_superseded,
            "discovery_archived_errors": discovery_archived_errors,
            "bulk_api_observed_articles": sum(item["total_articles"] for item in bulk_endpoints),
            "bulk_api_observed_articles_basis": "sum of source totals in leaf scopes; overlapping dates/latest feed may duplicate; not unique or a complete historical inventory",
            "bulk_historical_window_gaps": gap_samples,
            "bulk_gap_projection": gap_projection,
            "bulk_api_endpoints": bulk_endpoints,
            "bulk_coverage_projection": {
                "contract_version": 2, "grain": "provider and source endpoint; leaf scope totals may overlap",
                "coverage_scopes": sum(item["coverage_scopes"] for item in bulk_endpoints),
                "leaf_scopes": sum(item["leaf_scopes"] for item in bulk_endpoints),
                "superseded_scopes": sum(item["superseded_scopes"] for item in bulk_endpoints),
                "all_scope_details_retained": True, "detail_authority": "SQLite bulk_api_coverage and bulk_window_splits",
            },
            "bulk_api_pages_waiting": enabled_bulk_pages,
            "bulk_api_pages_disabled": all_bulk_pages - registered_bulk_pages,
            "bulk_api_pages_temporarily_blocked": registered_bulk_pages - enabled_bulk_pages,
            "bulk_item_errors": item_errors,
            "bulk_adapter_cooldowns": [dict(r) for r in self.db.execute("SELECT * FROM bulk_adapter_cooldowns WHERE provider!='cnyes' AND not_before>?", (time.time(),))],
            "bulk_endpoint_cooldowns": [dict(r) for r in self.db.execute("SELECT * FROM bulk_endpoint_cooldowns WHERE not_before>?", (time.time(),))
                                        if r["provider"] in self.registry.sources and r["data_fingerprint"] == self.registry.sources[r["provider"]].data_fingerprint],
            "counters": counters, "http_requests_this_run": requests_by_provider or {},
            "http_response_bytes_this_run": response_bytes,
            "eligible_remaining_articles": eligible_remaining, "observed_accepted_articles_per_second": rate,
            "eligible_eta_seconds": eta, "all_sources_eta_seconds": None,
            "eta_basis": "maximum conditional provider ETA using full owner wall time; unavailable when any eligible source lacks measured throughput",
            "historical_completeness_verified": False, "publish": False, "remote_training_export": False,
            "collection_purpose": "personal_research", "fair_use_legally_determined": False,
            "training_injection_enabled": False,
            "provider_next_attempt_utc": {p: datetime.fromtimestamp(t, timezone.utc).isoformat()
                                          for p, t in (provider_cooldowns or {}).items() if t > time.time()},
            "performance": performance,
            "http_capacity": capacity,
        }
        with self.metrics.measure('status_json_publication'):
            atomic_write_json(self.root / "status.json", result, durable=False, compact=True)
        return result


def verify_bodies(root: Path) -> dict:
    """Verify immutable receipts against one consistent SQLite snapshot.

    A live owner may accept more batches while verification reads large files.
    Pin the database generation before enumerating receipts, so later heads
    cannot be misreported as missing source evidence. No writer is blocked.
    """
    database = root / "queue.sqlite3"
    db = sqlite3.connect(f"file:{database.resolve()}?mode=ro", uri=True) if database.is_file() else None
    snapshot_at = utc_now()
    try:
        if db is not None:
            db.execute("BEGIN")
            # BEGIN alone is deferred; the first read actually pins WAL state.
            db.execute("SELECT count(*) FROM accepted_batches").fetchone()
        return _verify_bodies_snapshot(root, db, snapshot_at)
    finally:
        if db is not None:
            db.close()


def _verify_bodies_snapshot(root: Path, db, snapshot_at: str) -> dict:
    from downloader.news_fulltext_public_originals import verify_resolution_response
    source_bytes, text_bytes, versions, errors, referenced = 0, 0, 0, [], {}
    for receipt_path in sorted((root / "receipts").glob("*.json")):
        try:
            receipt = json.loads(receipt_path.read_text())
            target = source_path(root, receipt["parquet_path"])
            if receipt["contract_version"] != CONTRACT_VERSION or sha256_file(target) != receipt["sha256"]:
                raise ValueError("parquet_digest_or_contract_mismatch")
            table = pq.read_table(target)
            if table.num_rows != receipt["rows"]:
                raise ValueError("parquet_row_count_mismatch")
            records = table.to_pylist()
            raw_envelopes = {r["article_id"]: r["raw_response_body"] for r in records if r["raw_response_body"] is not None}
            batch_source_bytes, batch_text_bytes = sum(map(len, raw_envelopes.values())), 0
            for record in records:
                batch_source_bytes += verify_resolution_response(record)
                response_id = record.get("source_response_article_id") or record["article_id"]
                raw = raw_envelopes.get(response_id)
                if raw is None:
                    raise ValueError("missing_shared_raw_response")
                if hashlib.sha256(record["body"].encode()).hexdigest() != record["body_sha256"]:
                    raise ValueError("body_digest_mismatch")
                if hashlib.sha256(raw).hexdigest() != record["response_sha256"]:
                    raise ValueError("raw_response_digest_mismatch")
                if len(record["body"].encode()) != record["body_bytes"] or len(raw) != record["http_response_bytes"]:
                    raise ValueError("byte_count_mismatch")
                if record["historical_point_in_time"] or record["publish"] or record["remote_training_export"]:
                    raise ValueError("unapproved_publication_or_pit_claim")
                if record.get("capture_policy_version", 1) >= 2:
                    if record.get("collection_purpose") != "personal_research" or record.get("fair_use_legally_determined") is not False:
                        raise ValueError("unsupported_capture_purpose_or_rights_claim")
                    if record.get("commercial_use_enabled") is not False or record.get("training_injection_enabled") is not False:
                        raise ValueError("unapproved_downstream_use")
                    if record.get("collection_basis") == "personal_research" and record.get("rights_status") != "not_cleared_for_reuse":
                        raise ValueError("personal_research_is_not_a_license")
                referenced[(receipt["parquet_path"], record["article_id"])] = record["body_sha256"]
                versions += 1
                batch_text_bytes += record["body_bytes"]
                text_bytes += record["body_bytes"]
            if receipt["raw_response_bytes"] != batch_source_bytes or receipt["text_bytes"] != batch_text_bytes:
                raise ValueError("receipt_byte_count_mismatch")
            source_bytes += batch_source_bytes
        except (OSError, ValueError, KeyError, TypeError, pa.ArrowException) as error:
            errors.append({"receipt": str(receipt_path.relative_to(root)), "error": type(error).__name__ + ":" + str(error)[:160]})
    complete_heads = 0
    if db is not None:
        for key, path, digest in db.execute("SELECT id,parquet_path,body_sha256 FROM articles WHERE state='complete'"):
            complete_heads += 1
            if referenced.get((path, key.hex())) != digest:
                errors.append({"article_id": key.hex(), "error": "queue_head_missing_verified_body"})
        actual = dict(((p, s), (n, b)) for p, s, n, b in db.execute(
            "SELECT provider,state,count(*),coalesce(sum(body_bytes),0) FROM articles GROUP BY provider,state"))
        metrics = dict(((p, s), (n, b)) for p, s, n, b in db.execute(
            "SELECT provider,state,articles,body_bytes FROM state_metrics WHERE articles>0"))
        if actual != metrics:
            errors.append({"error": "incremental_progress_metrics_mismatch"})
    return {"contract_version": CONTRACT_VERSION, "verified_at_utc": utc_now(), "accepted": not errors,
            "body_versions_verified": versions, "complete_queue_heads_verified": complete_heads,
            "raw_response_bytes": source_bytes, "text_bytes": text_bytes, "errors": errors,
            "historical_completeness_verified": False, "publish": False,
            "database_snapshot_at_utc": snapshot_at, "verification_scope": "immutable receipts and consistent read-only queue snapshot; not future writes or all source history"}
