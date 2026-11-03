"""Audit a witnessed CMNews login-mask shape in retained originals, offline.

The prior provider storage audit owns batch verification. This scanner binds
its result to the same quiescent heads, batch identities, code and parser; it
does not fetch, submit repairs, change source data or prove other paywalls or
publisher history complete. Its private checkpoint is resumable by batch.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
import fcntl
import hashlib
from importlib.metadata import version
from itertools import groupby, zip_longest
import json
import os
from pathlib import Path
import sqlite3
import stat
import sys
from time import monotonic, process_time
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from lxml import etree, html
import pyarrow
import pyarrow.parquet as parquet

from downloader.artifact_io import sha256_file
from downloader.news_fulltext_policy import NewsError, normalize_url
from downloader.news_fulltext_publishers import cmnews_visible_article
from scripts.audit_news_provider_storage_readback import snapshot, source_file, stamp, state_snapshot

REPO = Path(__file__).resolve().parent.parent
CONTRACT = 'cmnews_retained_login_mask_audit_v3'
INPUTS = ('scripts/audit_news_cmnews_retained_access.py',
          'scripts/audit_news_provider_storage_readback.py',
          'downloader/news_fulltext_publishers.py', 'downloader/news_fulltext_policy.py',
          'downloader/news_fulltext_queue.py', 'downloader/artifact_io.py')
SCOPES = "//*[contains(concat(' ',normalize-space(@class),' '),' articleContent ')][@data-article-path]"
MASKS = (".//*[contains(concat(' ',normalize-space(@class),' '),' articleContent__textContent ')]"
         "//*[local-name()='vip-content' and contains(concat(' ',normalize-space(@class),' '),' vipWrapper ')]"
         "//*[contains(concat(' ',normalize-space(@class),' '),' vipContent ')]"
         "//*[contains(concat(' ',normalize-space(@class),' '),' utilsMask ')]")
REQUIRED = ('article_id', 'provider', 'requested_url', 'body_sha256', 'body_bytes',
            'captured_at_utc', 'response_sha256', 'raw_response_body')


def implementation():
    return {'files': {name: sha256_file(REPO / name) for name in INPUTS},
            'python': list(sys.version_info[:3]), 'sqlite': sqlite3.sqlite_version,
            'pyarrow': pyarrow.__version__, 'lxml': version('lxml'),
            'beautifulsoup4': version('beautifulsoup4')}


def inspect_original(raw, url):
    """Return a narrow candidate outcome; canonical parser confirms positives."""
    try:
        doc = html.fromstring(raw)
        scopes = []
        for node in doc.xpath(SCOPES):
            try:
                if normalize_url(urljoin(url, node.get('data-article-path'))) == url:
                    scopes.append(node)
            except (NewsError, ValueError):
                pass
        if len(scopes) != 1:
            return 'scope_not_unique', None
        texts = {' '.join(node.text_content().split()) for mask in scopes[0].xpath(MASKS)
                 for node in mask.xpath('.//p')}
    except (ValueError, TypeError, UnicodeError, RecursionError, etree.LxmlError):
        return 'html_parse_error', None
    if not texts.intersection({'請登入以查看完整文章', '閱讀VIP文章請先登入理財寶會員'}):
        return 'no_observed_login_mask', None
    try:
        cmnews_visible_article(BeautifulSoup(raw, 'lxml'), page_url=url)
        outcome = 'accepted'
    except NewsError as error:
        outcome = error.code
    except (ValueError, TypeError, RecursionError, etree.LxmlError) as error:
        outcome = 'parser_exception:' + type(error).__name__
    return ('confirmed_login_mask' if outcome == 'paywall_or_login' else 'native_mismatch'), outcome


def inspect_batch(root, source, job, rows):
    """Match every selected head to the previously verified immutable batch."""
    path, receipt, selected, selected_sha, body_stamp, receipt_stamp = job
    if (len(rows) != selected or hashlib.sha256(b''.join(row[0] for row in rows)).hexdigest() != selected_sha
            or source.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (path,)).fetchone() != (receipt,)):
        raise ValueError('head_or_receipt_mapping_changed')
    body, receipt_file = source_file(root, path), source_file(root, receipt)
    if stamp(body) != json.loads(body_stamp) or stamp(receipt_file) != json.loads(receipt_stamp):
        raise ValueError('verified_batch_artifact_changed')
    file = parquet.ParquetFile(body)
    available = set(file.schema_arrow.names)
    if not set(REQUIRED) <= available:
        raise ValueError('required_original_fields_missing')
    optional = ['source_response_article_id'] if 'source_response_article_id' in available else []
    records = file.read(columns=list(REQUIRED) + optional, use_threads=False).to_pylist()
    originals = {rec['article_id']: rec['raw_response_body'] for rec in records if rec['raw_response_body'] is not None}
    by_id, seen, counts, found, unresolved = {row[0].hex(): row for row in rows}, set(), Counter(), [], []
    for rec in records:
        key = rec['article_id']
        if key not in by_id:
            continue
        if key in seen:
            raise ValueError('duplicate_selected_head')
        seen.add(key)
        ident, url, digest, size, captured, state = by_id[key]
        if (rec['provider'] != 'cmnews' or rec['requested_url'] != url
                or hashlib.sha256(url.encode()).digest() != ident
                or (rec['body_sha256'], rec['body_bytes'], rec['captured_at_utc']) != (digest, size, captured)):
            raise ValueError('selected_head_metadata_changed')
        raw = originals.get(rec.get('source_response_article_id') or key)
        if not isinstance(raw, bytes) or hashlib.sha256(raw).hexdigest() != rec['response_sha256']:
            outcome = 'original_unavailable_or_digest_mismatch'
            counts[outcome] += 1
            unresolved.append((key, path, state, digest, captured, rec.get('response_sha256'), outcome))
            continue
        outcome, native = inspect_original(raw, url)
        counts[outcome] += 1
        if native is not None:
            found.append((key, path, state, digest, captured, rec['response_sha256'], native))
        if outcome in {'scope_not_unique', 'html_parse_error', 'native_mismatch'}:
            unresolved.append((key, path, state, digest, captured, rec['response_sha256'], outcome))
    if (seen != set(by_id) or stamp(body) != json.loads(body_stamp)
            or stamp(receipt_file) != json.loads(receipt_stamp)):
        raise ValueError('selected_head_missing_or_artifact_changed')
    if sum(counts.values()) != selected:
        raise ValueError('selected_head_outcome_count_mismatch')
    return counts, found, unresolved


def reference_identity(root, reference_state, readback):
    """Bind to an independently accepted provider storage readback."""
    reference_state, readback = reference_state.resolve(strict=True), readback.resolve(strict=True)
    before = state_snapshot(reference_state)
    readback_bytes = readback.read_bytes()
    summary = json.loads(readback_bytes)
    with closing(sqlite3.connect(reference_state.as_uri() + '?mode=ro&immutable=1', uri=True)) as db:
        meta = {key: json.loads(value) for key, value in db.execute('SELECT key,value FROM metadata')}
        jobs, heads, failed = db.execute('SELECT count(*),coalesce(sum(selected_heads),0),'
            'coalesce(sum(error IS NOT NULL),0) FROM jobs').fetchone()
    if (not summary.get('accepted') or not summary.get('provider_head_pointer_consistency', {}).get('accepted')
            or summary.get('provider') != 'cmnews' or meta.get('provider') != 'cmnews'
            or summary.get('snapshot') != snapshot(root) or meta.get('snapshot') != summary['snapshot']
            or jobs != meta.get('target_batches') or heads != meta.get('stored_heads') or failed
            or summary.get('counts', {}).get('processed_batches') != jobs
            or summary.get('counts', {}).get('verified_stored_heads') != heads):
        raise ValueError('reference_provider_audit_not_accepted')
    result = {'state': str(reference_state), 'state_stamp': before, 'state_sha256': sha256_file(reference_state),
              'readback': str(readback), 'readback_sha256': hashlib.sha256(readback_bytes).hexdigest(),
              'snapshot': summary['snapshot'], 'scope_sha256': summary['scope_sha256'],
              'batches': jobs, 'heads': heads}
    check_reference(result)
    return result


def check_reference(reference):
    if (state_snapshot(Path(reference['state'])) != reference['state_stamp']
            or sha256_file(Path(reference['state'])) != reference['state_sha256']
            or sha256_file(Path(reference['readback'])) != reference['readback_sha256']):
        raise ValueError('reference_audit_changed')


def audit(root, reference_state, readback, checkpoint, *, max_batches=None, progress_every=32):
    root, checkpoint = Path(root).resolve(strict=True), Path(checkpoint)
    if checkpoint.is_symlink():
        raise ValueError('checkpoint_must_not_be_symlink')
    checkpoint = checkpoint.resolve()
    if checkpoint.is_relative_to(root):
        raise ValueError('checkpoint_must_be_outside_source')
    if max_batches is not None and (type(max_batches) is not int or max_batches < 1):
        raise ValueError('invalid_max_batches')
    if type(progress_every) is not int or progress_every < 1:
        raise ValueError('invalid_progress_interval')
    ref = reference_identity(root, Path(reference_state), Path(readback))
    code = implementation()
    checkpoint.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    owner = os.open(root / 'owner.lock', os.O_RDONLY | os.O_NOFOLLOW)
    lock = None
    started, cpu = monotonic(), process_time()
    try:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock = os.open(str(checkpoint) + '.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if snapshot(root) != ref['snapshot']:
            raise ValueError('source_changed_before_scan')
        try:
            fd = os.open(checkpoint, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        except FileExistsError:
            info = checkpoint.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077:
                raise ValueError('checkpoint_must_be_private_regular_file') from None
        else:
            os.close(fd)
        with closing(sqlite3.connect((root / 'queue.sqlite3').as_uri() + '?mode=ro&immutable=1', uri=True)) as source, \
             closing(sqlite3.connect(Path(ref['state']).as_uri() + '?mode=ro&immutable=1', uri=True)) as witness, \
             closing(sqlite3.connect(checkpoint)) as state:
            source.execute('PRAGMA query_only=ON')
            state.execute('PRAGMA journal_mode=WAL')
            state.execute('PRAGMA synchronous=FULL')
            state.executescript('''CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS jobs(path TEXT PRIMARY KEY,selected_heads INTEGER NOT NULL,
                  selected_ids_sha256 TEXT NOT NULL,outcomes_json TEXT NOT NULL,error TEXT);
                CREATE TABLE IF NOT EXISTS candidates(article_id TEXT PRIMARY KEY,path TEXT NOT NULL,
                  state TEXT NOT NULL,body_sha256 TEXT NOT NULL,captured_at_utc TEXT NOT NULL,
                  response_sha256 TEXT NOT NULL,native_outcome TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS unresolved(article_id TEXT PRIMARY KEY,path TEXT NOT NULL,
                  state TEXT NOT NULL,body_sha256 TEXT NOT NULL,captured_at_utc TEXT NOT NULL,
                  response_sha256 TEXT,outcome TEXT NOT NULL);''')
            expected = {'contract': CONTRACT, 'reference': ref, 'implementation': code}
            stored = {key: json.loads(value) for key, value in state.execute('SELECT key,value FROM metadata')}
            if not stored:
                with state:
                    state.executemany('INSERT INTO metadata VALUES(?,?)',
                        [(key, json.dumps(value, sort_keys=True)) for key, value in expected.items()])
            elif stored != expected:
                raise ValueError('checkpoint_identity_changed')
            existing = {row[0]: row[1:] for row in state.execute(
                'SELECT path,selected_heads,selected_ids_sha256,error FROM jobs')}
            processed_now = 0
            jobs = witness.execute('SELECT path,receipt_path,selected_heads,selected_ids_sha256,body_stamp,receipt_stamp '
                                   'FROM jobs WHERE error IS NULL ORDER BY path')
            # The source index is keyed by provider, not path. Scan and sort
            # its heads once rather than repeating that scan for every batch.
            source_rows = source.execute("SELECT parquet_path,id,url,body_sha256,body_bytes,captured_at_utc,state "
                "FROM articles INDEXED BY stored_bodies WHERE provider='cmnews' AND body_sha256 IS NOT NULL "
                "AND parquet_path IS NOT NULL AND parquet_path<>'' ORDER BY parquet_path,id")
            groups = groupby(source_rows, key=lambda row: row[0])
            for job, group in zip_longest(jobs, groups):
                if job is None or group is None:
                    raise ValueError('source_scope_count_mismatch')
                path, receipt, heads, ids_sha, body_stamp, receipt_stamp = job
                group_path, grouped = group
                rows = [row[1:] for row in grouped]
                if (group_path != path or len(rows) != heads
                        or hashlib.sha256(b''.join(row[0] for row in rows)).hexdigest() != ids_sha):
                    raise ValueError('source_scope_identity_mismatch')
                if path in existing and existing[path][:2] != (heads, ids_sha):
                    raise ValueError('checkpoint_scope_changed')
                if path in existing and existing[path][2] is None:
                    if (stamp(source_file(root, path)) != json.loads(body_stamp)
                            or stamp(source_file(root, receipt)) != json.loads(receipt_stamp)):
                        raise ValueError('checkpoint_artifact_changed')
                    continue
                try:
                    outcomes, candidates, unknowns = inspect_batch(root, source, job, rows)
                    error = None
                except (OSError, ValueError, KeyError, TypeError, IndexError, pyarrow.ArrowException) as exc:
                    outcomes, candidates, unknowns = Counter({'batch_validation_error': heads}), [], []
                    error = type(exc).__name__ + ':' + str(exc)[:160]
                with state:
                    state.execute('DELETE FROM candidates WHERE path=?', (path,))
                    state.execute('DELETE FROM unresolved WHERE path=?', (path,))
                    state.executemany('INSERT INTO candidates VALUES(?,?,?,?,?,?,?)', candidates)
                    state.executemany('INSERT INTO unresolved VALUES(?,?,?,?,?,?,?)', unknowns)
                    state.execute('INSERT OR REPLACE INTO jobs VALUES(?,?,?,?,?)',
                                  (path, heads, ids_sha, json.dumps(dict(outcomes), sort_keys=True), error))
                processed_now += 1
                if processed_now % progress_every == 0:
                    count, selected = state.execute('SELECT count(*),coalesce(sum(selected_heads),0) FROM jobs').fetchone()
                    print(json.dumps({'processed_batches': count, 'target_batches': ref['batches'],
                                      'processed_heads': selected, 'target_heads': ref['heads']}, sort_keys=True), flush=True)
                if max_batches is not None and processed_now >= max_batches:
                    break
            count, selected, errors = state.execute('SELECT count(*),coalesce(sum(selected_heads),0),'
                'coalesce(sum(error IS NOT NULL),0) FROM jobs').fetchone()
            outcomes = Counter()
            for (raw,) in state.execute('SELECT outcomes_json FROM jobs'):
                outcomes.update(json.loads(raw))
            candidates = dict(state.execute('SELECT native_outcome,count(*) FROM candidates GROUP BY native_outcome'))
            unknowns = dict(state.execute('SELECT outcome,count(*) FROM unresolved GROUP BY outcome'))
            completed = count == ref['batches'] and selected == ref['heads']
            if completed:
                own_jobs = state.execute('SELECT path,selected_heads,selected_ids_sha256 FROM jobs ORDER BY path')
                verified_jobs = witness.execute('SELECT path,selected_heads,selected_ids_sha256 '
                                               'FROM jobs WHERE error IS NULL ORDER BY path')
                if any(own != verified for own, verified in zip_longest(own_jobs, verified_jobs)):
                    raise ValueError('completed_scope_membership_mismatch')
            if (sum(candidates.values()) != outcomes['confirmed_login_mask'] + outcomes['native_mismatch']
                    or candidates.get('paywall_or_login', 0) != outcomes['confirmed_login_mask']
                    or sum(outcomes.values()) != selected or snapshot(root) != ref['snapshot']
                    or implementation() != code):
                raise ValueError('audit_inputs_or_outcome_count_changed')
            check_reference(ref)
            unresolved = sum(outcomes[name] for name in ('scope_not_unique', 'html_parse_error',
                'original_unavailable_or_digest_mismatch', 'native_mismatch', 'batch_validation_error'))
            if sum(unknowns.values()) != unresolved - outcomes['batch_validation_error']:
                raise ValueError('unresolved_identity_count_mismatch')
            return {'contract': CONTRACT, 'observed_at_utc': datetime.now(timezone.utc).isoformat(),
                    'processed_batches': count, 'target_batches': ref['batches'],
                    'processed_heads': selected, 'target_heads': ref['heads'],
                    'failed_batches': errors, 'unresolved_heads': unresolved,
                    'outcomes': dict(outcomes), 'candidate_native_outcomes': candidates,
                    'completed_scope': completed, 'scope_membership_verified': completed,
                    'accepted': completed and errors == 0,
                    'elapsed_seconds_this_run': monotonic() - started, 'cpu_seconds_this_run': process_time() - cpu,
                    'publisher_requests': 0, 'production_queue_writes': 0,
                    'scope': 'only the observed scoped CMNews login-mask shape; negatives are not proof of a complete body',
                    'reference': ref}
    finally:
        if lock is not None:
            os.close(lock)
        os.close(owner)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--reference-state', type=Path, required=True)
    parser.add_argument('--readback', type=Path, required=True)
    parser.add_argument('--checkpoint', type=Path, required=True)
    parser.add_argument('--max-batches', type=int)
    parser.add_argument('--progress-every', type=int, default=32)
    args = parser.parse_args(argv)
    result = audit(args.root, args.reference_state, args.readback, args.checkpoint,
                   max_batches=args.max_batches, progress_every=args.progress_every)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
    return 0 if result['accepted'] or not result['completed_scope'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
