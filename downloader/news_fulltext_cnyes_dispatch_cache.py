"""Connection-local proof for an empty CNYES revision or host-scoped selection.

The durable queue remains authoritative. The cache is installed lazily only
after a revision blocks due work, and is never used inside a transaction.
It cannot grant permission, skip a deadline, or survive an owner restart.
"""
from __future__ import annotations

import math
import sqlite3
import time

from downloader.news_fulltext_host_circuit import host_filtered_query


_COUNTER = 'cnyes_revision_dispatch_cache_generation'
# Periodically re-prove even a completely blocked frontier. This is an
# engineering bound, not a publisher deadline or a request pacing setting.
_MAX_PROOF_SECONDS = 60
_WATCHED = {
    'discovery': ("NEW.provider='cnyes'", "OLD.provider='cnyes'"),
    'bulk_endpoint_cooldowns': ("NEW.provider='cnyes'", "OLD.provider='cnyes'"),
    'queue_settings': ("NEW.key GLOB 'cnyes_index_request_failure_scope_v1:*'",
                       "OLD.key GLOB 'cnyes_index_request_failure_scope_v1:*'"),
    'cnyes_index_revision_rounds': ('1', '1'),
    'cnyes_index_revision_retired': ('1', '1'),
    # Pending local recovery suppresses only its exact list URL. Releasing
    # that marker can make the previously empty selection immediately ready.
    'cnyes_interrupted_page_checkpoints': ("NEW.state='pending'", "OLD.state='pending'"),
}


def _key(source, bulk_allowed, excluded_origins, excluded_hosts):
    # SourcePolicy is frozen, but compare the exact fields used by its SQL as
    # well as identity. This also prevents reusing a proof after a policy swap.
    return (id(source), source.allowed, source.acquisition_mode, source.known_url_only,
            source.metadata_feeds, source.wordpress_apis, source.cnyes_apis,
            source.fulltext_feeds, source.cnyes_index_api, source.cnyes_index_data_fingerprint,
            source.authorization_fingerprint, source.cnyes_index_compatibility,
            source.data_fingerprint, bulk_allowed, tuple(excluded_origins), tuple(excluded_hosts))


def _eligible(source, limit, bulk_allowed, excluded_origins, excluded_hosts):
    return (source.provider == 'cnyes' and source.allowed and bool(source.cnyes_index_api)
            and type(limit) is int and limit > 0 and type(bulk_allowed) is bool
            and isinstance(excluded_origins, (tuple, list))
            and all(type(item) is str for item in excluded_origins)
            and isinstance(excluded_hosts, (tuple, list))
            and all(type(item) is str for item in excluded_hosts))


class EmptyRevisionSelection:
    def __init__(self, db):
        self.db = db
        self.proof = None
        self.source = None
        self.disabled = False
        if db.in_transaction:
            self.disabled = True
            return
        try:
            # TEMP triggers run only for this owner's writes; data_version
            # separately fences commits made by any other SQLite connection.
            db.execute(f'CREATE TEMP TABLE {_COUNTER} (generation INTEGER NOT NULL)')
            db.execute(f'INSERT INTO {_COUNTER} VALUES (0)')
            for table, (new, old) in _WATCHED.items():
                update = f'({new}) OR ({old})'
                if table == 'discovery':
                    # Attempts, validators, priority and diagnostic fields do
                    # not turn an empty result into a nonempty one. Avoid
                    # discarding its proof for these unrelated updates.
                    update = (f'(({new}) OR ({old})) AND ('
                              'OLD.url IS NOT NEW.url OR OLD.provider IS NOT NEW.provider OR '
                              'OLD.kind IS NOT NEW.kind OR OLD.due IS NOT NEW.due OR '
                              'OLD.superseded_by_windows_json IS NOT NEW.superseded_by_windows_json)')
                for action, predicate in (('INSERT', new), ('UPDATE', update), ('DELETE', old)):
                    trigger = f'{_COUNTER}_{table}_{action.lower()}'
                    db.execute(f'CREATE TEMP TRIGGER {trigger} AFTER {action} ON main.{table} '
                               f'WHEN {predicate} BEGIN UPDATE {_COUNTER} SET generation=generation+1; END')
            db.commit()
            self.temp_schema = db.execute('PRAGMA temp.schema_version').fetchone()[0]
            self.main_schema = db.execute('PRAGMA main.schema_version').fetchone()[0]
        except sqlite3.Error:
            db.rollback()
            self.disabled = True

    def _stamp(self):
        if self.disabled or self.db.in_transaction:
            return None
        try:
            if (self.db.execute('PRAGMA temp.schema_version').fetchone()[0] != self.temp_schema
                    or self.db.execute('PRAGMA main.schema_version').fetchone()[0] != self.main_schema):
                # Do not rebuild a proof if a trigger or its table was changed.
                self.disabled, self.proof = True, None
                return None
            row = self.db.execute(f'SELECT generation FROM {_COUNTER}').fetchone()
            if row is None or type(row[0]) is not int:
                self.disabled, self.proof = True, None
                return None
            return (row[0], self.db.execute('PRAGMA main.data_version').fetchone()[0])
        except sqlite3.Error:
            self.disabled, self.proof = True, None
            return None

    def hit(self, source, limit, bulk_allowed, excluded_origins, excluded_hosts=()):
        if not _eligible(source, limit, bulk_allowed, excluded_origins, excluded_hosts):
            return False
        proof = self.proof
        if (proof is None or self.source is not source
                or proof['key'] != _key(source, bulk_allowed, excluded_origins, excluded_hosts)
                or proof['limit'] != limit):
            return False
        now, monotonic_now = time.time(), time.monotonic()
        if (now < proof['started'] or now >= proof['expires']
                or monotonic_now >= proof['monotonic_expires']):
            return False
        stamp = self._stamp()
        return stamp is not None and stamp == proof['stamp']

    def wake_deadline(self, source, bulk_allowed, excluded_origins, excluded_hosts=()):
        """Reuse the same proof without changing source or retry deadlines."""
        proof = self.proof
        if (proof is None or self.source is not source
                or not _eligible(source, proof['limit'], bulk_allowed, excluded_origins, excluded_hosts)
                or proof['key'] != _key(source, bulk_allowed, excluded_origins, excluded_hosts)):
            return None
        now, monotonic_now = time.time(), time.monotonic()
        if (now < proof['started'] or now >= proof['expires']
                or monotonic_now >= proof['monotonic_expires']):
            return None
        stamp = self._stamp()
        if stamp is None or stamp != proof['stamp']:
            return None
        # A backwards or stalled wall clock must not extend the real-time
        # lifetime of the bounded proof or its scheduler wake.
        return min(proof['expires'], now + max(0.0, proof['monotonic_expires'] - monotonic_now))

    def record(self, queue, source, limit, bulk_allowed, excluded_origins, excluded_hosts=()):
        """Re-prove emptiness and its earliest possible clock-driven change."""
        self.proof = None
        if not _eligible(source, limit, bulk_allowed, excluded_origins, excluded_hosts):
            return
        stamp = self._stamp()
        if stamp is None:
            return
        start, monotonic_start = time.time(), time.monotonic()
        if not excluded_hosts and not self.db.execute('SELECT 1 FROM cnyes_index_revision_rounds '
                'WHERE index_fingerprint=? AND authorization_fingerprint=? LIMIT 1',
                (source.cnyes_index_data_fingerprint, source.authorization_fingerprint)).fetchone():
            return
        # The same outer scope and dispatch predicate as the selection, with
        # no due bound: a minimum in the future (or no row) proves an empty
        # positive-LIMIT selection and gives its next possible due deadline.
        sql, binds = host_filtered_query(
            'SELECT min(due) FROM discovery WHERE provider=? AND superseded_by_windows_json IS NULL',
            (source.provider,), excluded_hosts, excluded_origins=excluded_origins, discovery_url=True)
        # Use one clock for both eligibility and circuit-expiry proof. A wall
        # clock adjustment between the two must not hide newly due work.
        condition, params = queue.discovery_dispatch_filter(source, bulk_allowed, now=start)
        due = self.db.execute(sql + ' AND ' + condition, (*binds, *params)).fetchone()[0]
        circuit = self.db.execute('SELECT min(not_before) FROM bulk_endpoint_cooldowns '
                                  'WHERE provider=? AND not_before>?', (source.provider, start)).fetchone()[0]
        deadlines = [value for value in (due, circuit) if value is not None]
        if any(type(value) not in (int, float) or not math.isfinite(value) for value in deadlines):
            return
        expires = min([start + _MAX_PROOF_SECONDS, *deadlines])
        current, current_monotonic = time.time(), time.monotonic()
        if (current < start or expires <= current or current_monotonic >= monotonic_start + _MAX_PROOF_SECONDS
                or self._stamp() != stamp):
            return
        self.source = source  # Keep identity alive; id cannot be recycled.
        self.proof = {'key': _key(source, bulk_allowed, excluded_origins, excluded_hosts), 'limit': limit, 'started': start,
                      'expires': expires, 'monotonic_expires': monotonic_start + _MAX_PROOF_SECONDS, 'stamp': stamp}


def maybe_record(queue, source, limit, bulk_allowed, excluded_origins, excluded_hosts=()):
    if not _eligible(source, limit, bulk_allowed, excluded_origins, excluded_hosts) or queue.db.in_transaction:
        return
    cache = getattr(queue, '_cnyes_revision_empty_selection', None)
    if cache is None:
        if not excluded_hosts:
            active = queue.db.execute('SELECT 1 FROM cnyes_index_revision_rounds '
                'WHERE index_fingerprint=? AND authorization_fingerprint=? LIMIT 1',
                (source.cnyes_index_data_fingerprint, source.authorization_fingerprint)).fetchone()
            if not active:
                return
        cache = queue._cnyes_revision_empty_selection = EmptyRevisionSelection(queue.db)
    if not cache.disabled:
        try:
            cache.record(queue, source, limit, bulk_allowed, excluded_origins, excluded_hosts)
        except sqlite3.Error:
            # This is optional process-local bookkeeping: the already-run
            # authoritative selection remains valid, and later calls fall
            # back to it if proof construction cannot complete.
            cache.disabled, cache.proof = True, None
