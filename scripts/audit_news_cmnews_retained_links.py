"""Inventory native article links in receipt-verified CMNews originals, offline.

The scan holds the existing owner lock and writes only a private, resumable
checkpoint outside the source root. It neither fetches nor enqueues targets.
An anchor is a discovery hint, not proof of the target's identity or body.
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
import re
import sqlite3
import stat
import sys
from time import monotonic, process_time
from urllib.parse import urljoin, urlsplit

from lxml import etree, html
import pyarrow
import pyarrow.parquet as parquet

from downloader.artifact_io import sha256_file
from downloader.news_fulltext_policy import NewsError, normalize_url
from scripts.audit_news_cmnews_retained_access import check_reference, reference_identity
from scripts.audit_news_provider_storage_readback import snapshot, source_file, stamp

REPO = Path(__file__).resolve().parent.parent
CONTRACT = 'cmnews_retained_article_link_inventory_v1'
ARTICLE = re.compile(r'/article/[A-Za-z0-9_-]+\Z')
TAG = re.compile(r'/tag/[^/?#]+\Z')
REQUIRED = ('article_id', 'provider', 'requested_url', 'body_sha256', 'body_bytes',
            'captured_at_utc', 'response_sha256', 'raw_response_body')
INPUTS = ('scripts/audit_news_cmnews_retained_links.py',
          'scripts/audit_news_cmnews_retained_access.py',
          'scripts/audit_news_provider_storage_readback.py',
          'downloader/news_fulltext_policy.py', 'downloader/artifact_io.py')


def implementation():
    return {'files': {name: sha256_file(REPO / name) for name in INPUTS},
            'python': list(sys.version_info[:3]), 'sqlite': sqlite3.sqlite_version,
            'pyarrow': pyarrow.__version__, 'lxml': version('lxml')}


def local_url(base, href):
    """Accept exact HTTPS same-host paths; do not equate query/URL variants."""
    if not isinstance(href, str) or not href or len(href) > 8192 or any(ord(c) < 32 for c in href):
        return None
    try:
        joined = urljoin(base, href)
        parts = urlsplit(joined)
        if (parts.scheme != 'https' or parts.netloc != 'cmnews.com.tw' or parts.query or parts.fragment
                or not (ARTICLE.fullmatch(parts.path) or TAG.fullmatch(parts.path))):
            return None
        return joined if normalize_url(joined) == joined else None
    except (ValueError, NewsError):
        return None


def links_from_original(raw, parent_url):
    if not isinstance(raw, bytes):
        raise ValueError('original_unavailable')
    doc = html.fromstring(raw)
    links = {}
    for href in doc.xpath('//a[@href]/@href'):
        url = local_url(parent_url, href)
        if url:
            links[url] = 'article' if ARTICLE.fullmatch(urlsplit(url).path) else 'tag'
    return links


def inspect_batch(root, source, job, rows):
    path, receipt, selected, selected_sha, body_stamp, receipt_stamp = job
    if (len(rows) != selected or hashlib.sha256(b''.join(row[0] for row in rows)).hexdigest() != selected_sha
            or source.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (path,)).fetchone() != (receipt,)):
        raise ValueError('head_or_receipt_mapping_changed')
    body, receipt_file = source_file(root, path), source_file(root, receipt)
    if stamp(body) != json.loads(body_stamp) or stamp(receipt_file) != json.loads(receipt_stamp):
        raise ValueError('verified_batch_artifact_changed')
    evidence = json.loads(receipt_file.read_bytes())
    if evidence.get('parquet_path') != path or evidence.get('sha256') != sha256_file(body):
        raise ValueError('batch_receipt_digest_mismatch')
    file = parquet.ParquetFile(body)
    if not set(REQUIRED) <= set(file.schema_arrow.names):
        raise ValueError('required_original_fields_missing')
    optional = ['source_response_article_id'] if 'source_response_article_id' in file.schema_arrow.names else []
    records = file.read(columns=list(REQUIRED) + optional, use_threads=False).to_pylist()
    originals = {rec['article_id']: rec['raw_response_body'] for rec in records if rec['raw_response_body'] is not None}
    expected = {row[0].hex(): row for row in rows}
    seen, links = set(), {}
    for rec in records:
        ident = rec['article_id']
        if ident not in expected:
            continue
        if ident in seen:
            raise ValueError('duplicate_selected_head')
        seen.add(ident)
        key, url, body_hash, body_bytes, captured = expected[ident]
        if (rec['provider'] != 'cmnews' or rec['requested_url'] != url
                or hashlib.sha256(url.encode()).digest() != key
                or (rec['body_sha256'], rec['body_bytes'], rec['captured_at_utc']) != (body_hash, body_bytes, captured)):
            raise ValueError('selected_head_metadata_changed')
        raw = originals.get(rec.get('source_response_article_id') or ident)
        if not isinstance(raw, bytes) or hashlib.sha256(raw).hexdigest() != rec['response_sha256']:
            raise ValueError('original_unavailable_or_digest_mismatch')
        for target, kind in links_from_original(raw, url).items():
            links.setdefault(target, (kind, ident, url, captured, rec['response_sha256']))
    if (seen != set(expected) or stamp(body) != json.loads(body_stamp) or stamp(receipt_file) != json.loads(receipt_stamp)):
        raise ValueError('selected_head_missing_or_artifact_changed')
    return links


def _classify(source, state):
    """Rebuild a bounded-chunk view of targets, using exact source URL keys."""
    state.execute('DELETE FROM targets')
    groups = state.execute('SELECT target_url,kind,count(*),min(batch_path) FROM links '
                           'GROUP BY target_url,kind ORDER BY target_url')
    while True:
        chunk = groups.fetchmany(500)
        if not chunk:
            break
        urls = [row[0] for row in chunk]
        placeholders = ','.join('?' for _ in urls)
        known = {row[0]: row[1:] for row in source.execute(
            'SELECT url,provider,state,body_sha256 FROM articles WHERE url IN (' + placeholders + ')', urls)}
        entries = []
        for url, kind, occurrences, first_path in chunk:
            witness = state.execute('SELECT parent_id,parent_url,captured_at_utc,response_sha256 FROM links '
                                    'WHERE batch_path=? AND target_url=?', (first_path, url)).fetchone()
            value = known.get(url)
            entries.append((url, kind, occurrences, first_path, *witness,
                            None if value is None else value[0], None if value is None else value[1],
                            None if value is None else int(value[2] is not None)))
        state.executemany('INSERT INTO targets VALUES (?,?,?,?,?,?,?,?,?,?,?)', entries)


def audit(root, reference_state, readback, checkpoint, *, max_batches=None, progress_every=64):
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
    ref, code = reference_identity(root, Path(reference_state), Path(readback)), implementation()
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
                  selected_ids_sha256 TEXT NOT NULL,error TEXT);
                CREATE TABLE IF NOT EXISTS links(batch_path TEXT NOT NULL,target_url TEXT NOT NULL,
                  kind TEXT NOT NULL,parent_id TEXT NOT NULL,parent_url TEXT NOT NULL,captured_at_utc TEXT NOT NULL,
                  response_sha256 TEXT NOT NULL,PRIMARY KEY(batch_path,target_url)) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS retained_target ON links(target_url,kind,batch_path);
                CREATE TABLE IF NOT EXISTS targets(url TEXT PRIMARY KEY,kind TEXT NOT NULL,batches INTEGER NOT NULL,
                  witness_batch TEXT NOT NULL,parent_id TEXT NOT NULL,parent_url TEXT NOT NULL,
                  captured_at_utc TEXT NOT NULL,response_sha256 TEXT NOT NULL,known_provider TEXT,
                  known_state TEXT,known_body_pointer INTEGER) WITHOUT ROWID;''')
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
            jobs = witness.execute('SELECT path,receipt_path,selected_heads,selected_ids_sha256,body_stamp,receipt_stamp '
                                   'FROM jobs WHERE error IS NULL ORDER BY path')
            source_rows = source.execute("SELECT parquet_path,id,url,body_sha256,body_bytes,captured_at_utc "
                "FROM articles INDEXED BY stored_bodies WHERE provider='cmnews' AND body_sha256 IS NOT NULL "
                "AND parquet_path IS NOT NULL AND parquet_path<>'' ORDER BY parquet_path,id")
            processed_now = 0
            for job, group in zip_longest(jobs, groupby(source_rows, key=lambda row: row[0])):
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
                    links = inspect_batch(root, source, job, rows)
                    error = None
                except (OSError, ValueError, KeyError, TypeError, IndexError, pyarrow.ArrowException,
                        etree.LxmlError) as exc:
                    links, error = {}, type(exc).__name__ + ':' + str(exc)[:160]
                with state:
                    state.execute('DELETE FROM links WHERE batch_path=?', (path,))
                    state.executemany('INSERT INTO links VALUES (?,?,?,?,?,?,?)',
                        [(path, target, *detail) for target, detail in links.items()])
                    state.execute('INSERT OR REPLACE INTO jobs VALUES (?,?,?,?)', (path, heads, ids_sha, error))
                processed_now += 1
                if processed_now % progress_every == 0:
                    count, selected = state.execute('SELECT count(*),coalesce(sum(selected_heads),0) FROM jobs').fetchone()
                    print(json.dumps({'processed_batches': count, 'target_batches': ref['batches'],
                                      'processed_heads': selected, 'target_heads': ref['heads']}, sort_keys=True), flush=True)
                if max_batches is not None and processed_now >= max_batches:
                    break
            count, selected, errors = state.execute('SELECT count(*),coalesce(sum(selected_heads),0),'
                'coalesce(sum(error IS NOT NULL),0) FROM jobs').fetchone()
            completed = count == ref['batches'] and selected == ref['heads']
            if completed:
                own = state.execute('SELECT path,selected_heads,selected_ids_sha256 FROM jobs ORDER BY path')
                reference = witness.execute('SELECT path,selected_heads,selected_ids_sha256 '
                                            'FROM jobs WHERE error IS NULL ORDER BY path')
                if any(left != right for left, right in zip_longest(own, reference)):
                    raise ValueError('completed_scope_membership_mismatch')
            with state:
                _classify(source, state)
            counts = Counter()
            for kind, provider, body, n in state.execute('SELECT kind,known_provider,known_body_pointer,count(*) '
                                                          'FROM targets GROUP BY kind,known_provider,known_body_pointer'):
                label = 'unregistered' if provider is None else 'known_with_body' if body else 'known_without_body'
                counts[kind + '_' + label] += n
            if snapshot(root) != ref['snapshot'] or implementation() != code or source.total_changes:
                raise ValueError('audit_inputs_changed')
            check_reference(ref)
            return {'contract': CONTRACT, 'observed_at_utc': datetime.now(timezone.utc).isoformat(),
                    'processed_batches': count, 'target_batches': ref['batches'],
                    'processed_heads': selected, 'target_heads': ref['heads'], 'failed_batches': errors,
                    'completed_scope': completed, 'accepted': completed and errors == 0, 'target_counts': dict(counts),
                    'scope': 'exact literal CMNews article and tag anchors in existing receipt-verified body-head originals',
                    'target_identity_or_body_verified': False, 'publisher_history_complete': False,
                    'elapsed_seconds_this_run': monotonic() - started, 'cpu_seconds_this_run': process_time() - cpu,
                    'publisher_requests': 0, 'production_queue_writes': 0, 'reference': ref}
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
    parser.add_argument('--progress-every', type=int, default=64)
    args = parser.parse_args(argv)
    result = audit(args.root, args.reference_state, args.readback, args.checkpoint,
                   max_batches=args.max_batches, progress_every=args.progress_every)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
    return 0 if result['accepted'] or not result['completed_scope'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
