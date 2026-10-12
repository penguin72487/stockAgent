"""Finite, explicit failed-response evidence in the existing news owner.

This is a storage budget, not requests, permission, retries or article bodies.
Each selected URL may produce one durable diagnostic; a proven storage error
has at most one later NORMAL-response retry. Reservations, bytes and unknown
writes survive owner/parser/config changes. No arbitrary per-URL reset budget.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import PurePosixPath
import re
from urllib.parse import urlsplit


CONTRACT_VERSION = 1
RECEIPT_CONTRACT_VERSION = 7
MAXIMUM_URLS = 512
MAXIMUM_RAW_BYTES = 256 << 20
MAXIMUM_WRITE_ATTEMPTS = 2


def validate_focus(spec, registry):
    if spec is None:
        return None
    fields = {'contract_version', 'cohort_id', 'provider', 'urls', 'maximum_raw_bytes'}
    if not isinstance(spec, dict) or set(spec) != fields:
        raise ValueError('complete finite failure evidence focus required')
    urls = spec['urls']
    if (type(spec['contract_version']) is not int or spec['contract_version'] != CONTRACT_VERSION
            or spec['provider'] != 'cnyes' or not isinstance(spec['cohort_id'], str)
            or not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,79}', spec['cohort_id'])
            or not isinstance(urls, list) or not 1 <= len(urls) <= MAXIMUM_URLS
            or any(not isinstance(url, str) or len(url) > 2048 for url in urls)
            or len(set(urls)) != len(urls)
            or type(spec['maximum_raw_bytes']) is not int
            or not 1 <= spec['maximum_raw_bytes'] <= MAXIMUM_RAW_BYTES):
        raise ValueError('invalid bounded failure evidence focus')
    from downloader.news_fulltext_policy import NewsError, normalize_url
    from downloader.news_fulltext_publishers import cnyes_supported_native_alias_view
    for url in urls:
        try:
            parts = urlsplit(url)
            normalized = normalize_url(url)
            source = registry.match(url)
        except (ValueError, NewsError) as exc:
            raise ValueError('exact reviewed native Cnyes focus URL required') from exc
        plain = not parts.query and parts.netloc in {'news.cnyes.com', 'm.cnyes.com'}
        if (parts.scheme != 'https' or parts.netloc not in {'news.cnyes.com', 'm.cnyes.com'}
                or parts.fragment or url != normalized
                or not re.fullmatch(r'/news/id/[1-9][0-9]*', parts.path)
                or not (plain or cnyes_supported_native_alias_view(parts))
                or source is None or source.provider != 'cnyes'):
            raise ValueError('exact reviewed native Cnyes focus URL required')
    return {**spec, 'urls': sorted(urls)}


def fingerprint(spec):
    return hashlib.sha256(json.dumps(spec, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


class FailureEvidenceFocus:
    def __init__(self, queue, spec):
        self.queue = queue
        self.spec = validate_focus(spec, queue.registry)
        self.id = self.spec['cohort_id']
        self.digest = fingerprint(self.spec)
        self.targets = frozenset(self.spec['urls'])
        db = queue.db
        db.execute('CREATE TABLE IF NOT EXISTS failure_evidence_focus_cohorts('
            'cohort_id TEXT PRIMARY KEY,manifest_sha256 TEXT NOT NULL,maximum_responses INTEGER NOT NULL,'
            'maximum_raw_bytes INTEGER NOT NULL,reserved_responses INTEGER NOT NULL DEFAULT 0,'
            'reserved_raw_bytes INTEGER NOT NULL DEFAULT 0)')
        db.execute('CREATE TABLE IF NOT EXISTS failure_evidence_focus_attempts('
            'provider TEXT NOT NULL,url_sha256 BLOB NOT NULL,attempt_number INTEGER NOT NULL,'
            'cohort_id TEXT NOT NULL,reserved_bytes INTEGER NOT NULL,response_sha256 TEXT NOT NULL,'
            'receipt_path TEXT NOT NULL,outcome TEXT NOT NULL '
            "CHECK(outcome IN ('reserved','raw_saved','metadata_saved','write_failed')),"
            'PRIMARY KEY(provider,url_sha256,attempt_number))')
        db.execute('CREATE INDEX IF NOT EXISTS failure_focus_progress ON failure_evidence_focus_attempts(cohort_id,outcome)')
        with db:
            db.execute('INSERT OR IGNORE INTO failure_evidence_focus_cohorts('
                'cohort_id,manifest_sha256,maximum_responses,maximum_raw_bytes) VALUES (?,?,?,?)',
                (self.id, self.digest, len(self.targets) * MAXIMUM_WRITE_ATTEMPTS, self.spec['maximum_raw_bytes']))
            row = db.execute('SELECT manifest_sha256 FROM failure_evidence_focus_cohorts WHERE cohort_id=?', (self.id,)).fetchone()
            if row[0] != self.digest:
                raise ValueError('existing failure focus generation cannot be overwritten')
        self._budget()
        totals = db.execute('SELECT count(*),coalesce(sum(reserved_bytes),0) '
            'FROM failure_evidence_focus_attempts WHERE cohort_id=?', (self.id,)).fetchone()
        if tuple(self._budget()[2:]) != tuple(totals):
            raise ValueError('failure focus reservations and counters disagree')
        self.reconcile_writes()

    def _budget(self):
        row = self.queue.db.execute('SELECT maximum_responses,maximum_raw_bytes,reserved_responses,reserved_raw_bytes '
            'FROM failure_evidence_focus_cohorts WHERE cohort_id=?', (self.id,)).fetchone()
        if (row is None or any(type(value) is not int for value in row)
                or row[0] != len(self.targets) * MAXIMUM_WRITE_ATTEMPTS or row[1] != self.spec['maximum_raw_bytes']
                or not 0 <= row[2] <= row[0] or not 0 <= row[3] <= row[1]):
            raise ValueError('invalid persistent failure focus budget')
        return row

    def needs_response(self, url):
        """Indexed duplicate check before UUID, response hashing or scope parse."""
        previous = self.queue.db.execute('SELECT attempt_number,outcome FROM failure_evidence_focus_attempts '
            'WHERE provider=\'cnyes\' AND url_sha256=? ORDER BY attempt_number DESC LIMIT 1',
            (hashlib.sha256(url.encode()).digest(),)).fetchone()
        needed = previous is None or previous[1] == 'write_failed' and previous[0] < MAXIMUM_WRITE_ATTEMPTS
        if not needed:
            self.queue.metrics.count('cnyes', 'failure_focus_duplicate_or_unknown_writes_suppressed')
        return needed

    def reserve(self, url, raw_bytes, response_sha256, receipt_path):
        """Return an immutable storage reservation, never source acceptance."""
        if (url not in self.targets or type(raw_bytes) is not int or not 0 <= raw_bytes <= 8 << 20
                or not re.fullmatch(r'[0-9a-f]{64}', response_sha256)):
            return None
        self.receipt_path(receipt_path)
        key = hashlib.sha256(url.encode()).digest()
        with self.queue.db:
            previous = self.queue.db.execute('SELECT attempt_number,outcome FROM failure_evidence_focus_attempts '
                'WHERE provider=\'cnyes\' AND url_sha256=? ORDER BY attempt_number DESC LIMIT 1', (key,)).fetchone()
            if previous and (previous[1] != 'write_failed' or previous[0] >= MAXIMUM_WRITE_ATTEMPTS):
                self.queue.metrics.count('cnyes', 'failure_focus_duplicate_or_unknown_writes_suppressed')
                return None
            maximum, maximum_bytes, attempts, size = self._budget()
            if attempts >= maximum or size + raw_bytes > maximum_bytes:
                self.queue.metrics.count('cnyes', 'failure_focus_storage_budget_exhausted')
                return None
            number = previous[0] + 1 if previous else 1
            self.queue.db.execute('INSERT INTO failure_evidence_focus_attempts VALUES (?,?,?,?,?,?,?,\'reserved\')',
                ('cnyes', key, number, self.id, raw_bytes, response_sha256, receipt_path))
            self.queue.db.execute('UPDATE failure_evidence_focus_cohorts '
                'SET reserved_responses=reserved_responses+1,reserved_raw_bytes=reserved_raw_bytes+? WHERE cohort_id=?',
                (raw_bytes, self.id))
        self.queue.metrics.count('cnyes', 'failure_focus_normal_response_reservations')
        return {'budget_bucket': 'finite_missing_response_cohort', 'budget_period_kind': 'finite_cohort',
            'budget_period': self.id, 'budget_attempt_limit': maximum, 'daily_bucket_attempt_limit': None,
            'failure_diagnostic_focus_contract_version': CONTRACT_VERSION,
            'failure_diagnostic_focus_manifest_sha256': self.digest,
            'failure_diagnostic_focus_attempt_number': number,
            'failure_diagnostic_focus_maximum_raw_bytes': maximum_bytes,
            'failure_diagnostic_focus_maximum_write_attempts_per_url': MAXIMUM_WRITE_ATTEMPTS,
            '_url_sha256': key}

    def finish(self, token, outcome):
        if outcome not in {'raw_saved', 'metadata_saved', 'write_failed'}:
            raise ValueError('explicit failure diagnostic write outcome required')
        with self.queue.db:
            changed = self.queue.db.execute('UPDATE failure_evidence_focus_attempts SET outcome=? '
                "WHERE provider='cnyes' AND url_sha256=? AND attempt_number=? AND cohort_id=? AND outcome='reserved'",
                (outcome, token['_url_sha256'], token['failure_diagnostic_focus_attempt_number'], self.id)).rowcount
            if changed != 1:
                raise ValueError('failure focus outcome is not an uncommitted reservation')
        self.queue.metrics.count('cnyes', 'failure_focus_' + outcome)

    def receipt_path(self, relative):
        if not isinstance(relative, str):
            raise ValueError('exact focus diagnostic receipt namespace required')
        parts = PurePosixPath(relative).parts
        if (PurePosixPath(relative).as_posix() != relative or len(parts) != 6
                or parts[0] != 'diagnostics' or not re.fullmatch(r'[0-9]{8}', parts[1])
                or parts[2:5] != ('cnyes', 'focus', self.id)
                or not re.fullmatch(r'[0-9]{8}T[0-9]{12}Z-[0-9a-f]{8}\.receipt\.json', parts[5])):
            raise ValueError('exact focus diagnostic receipt namespace required')
        # A reservation names a FUTURE write, so the existing source artifact
        # checker (which requires a file) cannot validate this destination.
        # Resolve any existing symlinks and enforce containment before writing.
        root = self.queue.root.resolve()
        target = (root / relative).resolve()
        if not target.is_relative_to(root):
            raise ValueError('focus diagnostic destination escapes owner root')
        return target

    def reconcile_writes(self):
        """Read back only this finite owner's unknown writes; never fetch again.

        Both files absent is a proven uncommitted write. A valid durable pair
        reconciles success. Partial/corrupt pairs stay unknown and untouched;
        no overwrite, deletion, body acceptance or unbounded retry occurs.
        """
        from downloader.artifact_io import sha256_file
        import pyarrow.parquet as pq
        rows = self.queue.db.execute('SELECT url_sha256,attempt_number,reserved_bytes,response_sha256,receipt_path '
            "FROM failure_evidence_focus_attempts WHERE cohort_id=? AND outcome='reserved'", (self.id,)).fetchall()
        for row in rows:
            token = {'_url_sha256': row[0], 'failure_diagnostic_focus_attempt_number': row[1]}
            try:
                receipt_path = self.receipt_path(row[4])
                data_path = receipt_path.with_suffix('').with_suffix('.parquet')
                if not receipt_path.exists() and not data_path.exists():
                    self.finish(token, 'write_failed')
                    self.queue.metrics.count('cnyes', 'failure_focus_absent_write_reconciled')
                    continue
                if (not receipt_path.is_file() or not data_path.is_file()
                        or receipt_path.stat().st_size > 65536 or data_path.stat().st_size > 12 << 20):
                    raise ValueError('partial_or_unbounded_focus_write')
                receipt = json.loads(receipt_path.read_bytes())
                if (not isinstance(receipt, dict) or receipt.get('path') != str(data_path.relative_to(self.queue.root.resolve()))
                        or receipt.get('sha256') != sha256_file(data_path) or receipt.get('provider') != 'cnyes'
                        or receipt.get('contract_version') != RECEIPT_CONTRACT_VERSION
                        or receipt.get('response_sha256') != row[3] or receipt.get('response_bytes') != row[2]
                        or receipt.get('failure_diagnostic_focus_manifest_sha256') != self.digest
                        or receipt.get('failure_diagnostic_focus_attempt_number') != row[1]
                        or receipt.get('budget_period') != self.id or receipt.get('budget_period_kind') != 'finite_cohort'
                        or receipt.get('failure_diagnostic_focus_contract_version') != CONTRACT_VERSION
                        or receipt.get('failure_diagnostic_focus_maximum_raw_bytes') != self.spec['maximum_raw_bytes']
                        or receipt.get('failure_diagnostic_focus_maximum_write_attempts_per_url') != MAXIMUM_WRITE_ATTEMPTS
                        or receipt.get('request_url_sha256') != row[0].hex()
                        or receipt.get('status') != 200 or receipt.get('error_code') != 'body_not_found'
                        or receipt.get('diagnostic_only') is not True or receipt.get('accepted_as_article') is not False
                        or receipt.get('publish') is not False or receipt.get('remote_training_export') is not False):
                    raise ValueError('invalid_focus_write_receipt')
                table = pq.read_table(data_path)
                if table.num_rows != 1:
                    raise ValueError('invalid_focus_diagnostic_grain')
                record = table.to_pylist()[0]
                identity = {key: value for key, value in receipt.items() if key not in {'path', 'sha256'}}
                if (record.get('request_url_sha256') != row[0].hex()
                        or record.get('response_sha256') != row[3]
                        or record.get('response_bytes') != row[2]
                        or record.get('failure_diagnostic_focus_manifest_sha256') != self.digest
                        or set(record) != set(identity) | {'raw_response_body'}
                        or any(record[key] != value for key, value in identity.items())):
                    raise ValueError('invalid_focus_record_identity')
                raw = record['raw_response_body']
                if receipt.get('raw_response_retained') is True:
                    if not isinstance(raw, bytes) or len(raw) != row[2] or hashlib.sha256(raw).hexdigest() != row[3]:
                        raise ValueError('invalid_focus_raw_response')
                    outcome = 'raw_saved'
                elif receipt.get('raw_response_retained') is False and raw is None:
                    outcome = 'metadata_saved'
                else:
                    raise ValueError('invalid_focus_retention_outcome')
                self.finish(token, outcome)
                self.queue.metrics.count('cnyes', 'failure_focus_durable_pair_reconciled')
            except (OSError, ValueError, KeyError, TypeError):
                self.queue.metrics.count('cnyes', 'failure_focus_unknown_partial_write_preserved')

    def progress(self):
        maximum, maximum_bytes, attempts, size = self._budget()
        counts = dict(self.queue.db.execute('SELECT outcome,count(*) FROM failure_evidence_focus_attempts '
            'WHERE cohort_id=? GROUP BY outcome', (self.id,)))
        return {'contract_version': CONTRACT_VERSION, 'cohort_id': self.id, 'manifest_sha256': self.digest,
            'selected_url_heads': len(self.targets), 'maximum_write_attempts': maximum,
            'maximum_raw_bytes': maximum_bytes, 'write_attempts_reserved': attempts,
            'raw_bytes_reserved': size, 'outcomes': counts, 'publisher_requests_added': 0,
            'raw_retention_is_not_article_body_acceptance': True, 'unknown_partial_writes_are_not_overwritten': True}


def configure_focus(queue, spec):
    queue.failure_diagnostic_focus = FailureEvidenceFocus(queue, spec) if spec is not None else None
