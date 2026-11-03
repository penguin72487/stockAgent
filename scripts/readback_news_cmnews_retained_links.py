"""Independently verify an offline CMNews retained-link inventory.

Only newly unregistered article hints are replayed against the parent original;
source and checkpoint identity, all target classifications and inbox overlap
are also checked. This does not fetch, enqueue or establish target content.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime, timezone
import fcntl
import hashlib
from itertools import groupby, islice, zip_longest
import json
import os
from pathlib import Path
import sqlite3
import stat
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import pyarrow.parquet as parquet

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext_repairs import MAX_PENDING, _pending
from downloader.news_fulltext_url_metadata import _load
from scripts.audit_news_cmnews_retained_access import check_reference, reference_identity
from scripts.audit_news_cmnews_retained_links import CONTRACT, implementation
from scripts.audit_news_provider_storage_readback import snapshot, source_file, stamp, state_snapshot


def _finished_report(log):
    reports = [json.loads(line) for line in Path(log).read_text().splitlines()
               if line.startswith('{') and json.loads(line).get('contract') == CONTRACT]
    if len(reports) != 1 or not reports[0].get('accepted') or not reports[0].get('completed_scope'):
        raise ValueError('completed_accepted_inventory_required')
    return reports[0]


def _pending_urls(root):
    directory = root
    for part in ('reviewed_repairs', 'url_metadata', 'pending'):
        directory = directory / part
        try:
            info = directory.lstat()
        except FileNotFoundError:
            return set(), []
        if not stat.S_ISDIR(info.st_mode) or info.st_mode & 0o077:
            raise ValueError('unsafe_url_metadata_inbox')
    paths = _pending(directory)
    if len(paths) > MAX_PENDING:
        raise ValueError('url_metadata_inbox_exceeds_limit')
    targets, files = set(), []
    for path in paths:
        before = stamp(path)
        request = _load(path)
        if stamp(path) != before:
            raise ValueError('url_metadata_request_changed')
        files.append((path.name, before))
        targets.update(target['url'] for target in request['targets'])
    return targets, files


def readback(root, checkpoint, log):
    root, checkpoint, log = Path(root).resolve(strict=True), Path(checkpoint).resolve(strict=True), Path(log).resolve(strict=True)
    if checkpoint.is_relative_to(root):
        raise ValueError('checkpoint_must_be_outside_source')
    report = _finished_report(log)
    log_sha = sha256_file(log)
    checkpoint_before = state_snapshot(checkpoint)
    owner = os.open(root / 'owner.lock', os.O_RDONLY | os.O_NOFOLLOW)
    try:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with closing(sqlite3.connect((root / 'queue.sqlite3').as_uri() + '?mode=ro&immutable=1', uri=True)) as source, \
             closing(sqlite3.connect(checkpoint.as_uri() + '?mode=ro&immutable=1', uri=True)) as state:
            source.execute('PRAGMA query_only=ON')
            meta = {key: json.loads(value) for key, value in state.execute('SELECT key,value FROM metadata')}
            ref = meta['reference']
            checked_ref = reference_identity(root, Path(ref['state']), Path(ref['readback']))
            if (meta['contract'] != CONTRACT or meta['implementation'] != implementation() or checked_ref != ref
                    or report['reference'] != ref or snapshot(root) != ref['snapshot']):
                raise ValueError('inventory_input_identity_changed')
            with closing(sqlite3.connect(Path(ref['state']).as_uri() + '?mode=ro&immutable=1', uri=True)) as witness:
                reference_jobs = witness.execute('SELECT path,selected_heads,selected_ids_sha256 '
                    'FROM jobs WHERE error IS NULL ORDER BY path')
                actual_jobs = state.execute('SELECT path,selected_heads,selected_ids_sha256 FROM jobs ORDER BY path')
                if any(a != b for a, b in zip_longest(reference_jobs, actual_jobs)):
                    raise ValueError('inventory_batch_scope_mismatch')
                job_count, heads, failures = state.execute('SELECT count(*),coalesce(sum(selected_heads),0),'
                    'coalesce(sum(error IS NOT NULL),0) FROM jobs').fetchone()
                if (job_count != ref['batches'] or heads != ref['heads'] or failures
                        or (report['processed_batches'], report['processed_heads'], report['failed_batches'])
                           != (job_count, heads, failures)):
                    raise ValueError('inventory_incomplete_or_failed')
                aggregate = state.execute('SELECT target_url,kind,count(*),min(batch_path) FROM links '
                    'GROUP BY target_url,kind ORDER BY target_url')
                projected = state.execute('SELECT url,kind,batches,witness_batch,parent_id,parent_url,captured_at_utc,'
                    'response_sha256,known_provider,known_state,known_body_pointer FROM targets ORDER BY url')
                counts, target_hash = {}, hashlib.sha256()
                pairs = iter(zip_longest(aggregate, projected))
                while True:
                    chunk = list(islice(pairs, 500))
                    if not chunk:
                        break
                    if any(a is None or b is None or a != b[:4] for a, b in chunk):
                        raise ValueError('target_projection_mismatch')
                    urls = [b[0] for _, b in chunk]
                    marks = ','.join('?' for _ in urls)
                    known = {row[0]: row[1:] for row in source.execute(
                        'SELECT url,provider,state,body_sha256 FROM articles WHERE url IN (' + marks + ')', urls)}
                    for _, row in chunk:
                        url, kind, _, path, parent_id, parent, captured, raw_sha, provider, article_state, body = row
                        linked = state.execute('SELECT parent_id,parent_url,captured_at_utc,response_sha256 FROM links '
                                               'WHERE batch_path=? AND target_url=?', (path, url)).fetchone()
                        if linked != (parent_id, parent, captured, raw_sha):
                            raise ValueError('target_witness_projection_mismatch')
                        found = known.get(url)
                        expected = (None, None, None) if found is None else (found[0], found[1], int(found[2] is not None))
                        if (provider, article_state, body) != expected:
                            raise ValueError('target_source_head_mismatch')
                        key = kind + ('_unregistered' if provider is None else '_known_with_body' if body else '_known_without_body')
                        counts[key] = counts.get(key, 0) + 1
                        if kind == 'article' and provider is None:
                            target_hash.update(url.encode() + b'\n')
                if counts != report['target_counts']:
                    raise ValueError('target_summary_mismatch')
                pending, pending_before = _pending_urls(root)
                unknown = state.execute("SELECT witness_batch,parent_id,parent_url,captured_at_utc,response_sha256,url "
                                        "FROM targets WHERE kind='article' AND known_provider IS NULL "
                                        "ORDER BY witness_batch,parent_id,url")
                verified = 0
                pending_overlap = 0
                parent_batches = 0
                for path, rows_iter in groupby(unknown, key=lambda r: r[0]):
                    rows = list(rows_iter)
                    job = witness.execute('SELECT receipt_path,body_stamp,receipt_stamp FROM jobs WHERE path=? AND error IS NULL',
                                          (path,)).fetchone()
                    if not job or source.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (path,)).fetchone() != (job[0],):
                        raise ValueError('witness_receipt_registration_changed')
                    body_file, receipt_file = source_file(root, path), source_file(root, job[0])
                    if stamp(body_file) != json.loads(job[1]) or stamp(receipt_file) != json.loads(job[2]):
                        raise ValueError('witness_batch_artifact_changed')
                    receipt = json.loads(receipt_file.read_bytes())
                    if receipt.get('parquet_path') != path or receipt.get('sha256') != sha256_file(body_file):
                        raise ValueError('witness_batch_digest_mismatch')
                    pf = parquet.ParquetFile(body_file)
                    fields = ['article_id', 'provider', 'requested_url', 'body_sha256', 'body_bytes',
                              'captured_at_utc', 'response_sha256', 'raw_response_body']
                    if 'source_response_article_id' in pf.schema_arrow.names:
                        fields.append('source_response_article_id')
                    records = pf.read(columns=fields, use_threads=False).to_pylist()
                    by_id = {r['article_id']: r for r in records}
                    if len(by_id) != len(records):
                        raise ValueError('witness_batch_duplicate_id')
                    for parent_id, parent_rows in groupby(rows, key=lambda r: r[1]):
                        members = list(parent_rows)
                        record = by_id.get(parent_id)
                        if not record:
                            raise ValueError('witness_parent_missing')
                        raw_rec = by_id.get(record.get('source_response_article_id') or parent_id)
                        raw = raw_rec.get('raw_response_body') if raw_rec else None
                        if (not isinstance(raw, bytes) or hashlib.sha256(raw).hexdigest() != record['response_sha256']
                                or record['provider'] != 'cmnews'):
                            raise ValueError('witness_original_digest_mismatch')
                        head = source.execute('SELECT url,parquet_path,body_sha256,body_bytes,captured_at_utc '
                                              'FROM articles WHERE id=?', (bytes.fromhex(parent_id),)).fetchone()
                        if (not head or head != (record['requested_url'], path, record['body_sha256'],
                                                 record['body_bytes'], record['captured_at_utc'])
                                or hashlib.sha256(record['requested_url'].encode()).hexdigest() != parent_id):
                            raise ValueError('witness_parent_head_mismatch')
                        links = {urljoin(record['requested_url'], node.get('href'))
                                 for node in BeautifulSoup(raw, 'html.parser').find_all('a', href=True)}
                        for _, _, parent, captured, raw_sha, target in members:
                            if (parent != record['requested_url'] or captured != record['captured_at_utc']
                                    or raw_sha != record['response_sha256'] or target not in links):
                                raise ValueError('target_not_in_independently_parsed_original')
                            verified += 1
                            pending_overlap += target in pending
                    if stamp(body_file) != json.loads(job[1]) or stamp(receipt_file) != json.loads(job[2]):
                        raise ValueError('witness_batch_changed_during_readback')
                    parent_batches += 1
                if verified != counts.get('article_unregistered', 0):
                    raise ValueError('unregistered_witness_count_mismatch')
                pending_after, pending_after_files = _pending_urls(root)
                if pending_after != pending or pending_after_files != pending_before:
                    raise ValueError('url_metadata_inbox_changed')
            if snapshot(root) != ref['snapshot'] or state_snapshot(checkpoint) != checkpoint_before or sha256_file(log) != log_sha:
                raise ValueError('inventory_inputs_changed_during_readback')
            check_reference(ref)
            return {'contract': 'cmnews_retained_article_link_readback_v1',
                    'checked_at_utc': datetime.now(timezone.utc).isoformat(), 'accepted': True,
                    'checkpoint_sha256': sha256_file(checkpoint), 'log_sha256': log_sha,
                    'source_snapshot': ref['snapshot'], 'reference_scope_sha256': ref['scope_sha256'],
                    'target_counts': counts, 'unregistered_article_witnesses_verified': verified,
                    'unregistered_article_set_sha256': target_hash.hexdigest(),
                    'pending_inbox_overlap': pending_overlap, 'unregistered_not_in_pending': verified-pending_overlap,
                    'witness_batches_replayed': parent_batches, 'publisher_requests': 0, 'production_queue_writes': 0,
                    'target_identity_or_body_verified': False, 'publisher_history_complete': False,
                    'scope': 'independent source-head classification and parent-anchor replay for every unregistered article hint'}
    finally:
        os.close(owner)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--checkpoint', type=Path, required=True)
    parser.add_argument('--log', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.resolve().is_relative_to(args.root.resolve()):
        raise ValueError('output_must_be_outside_source')
    os.umask(0o077)
    result = readback(args.root, args.checkpoint, args.log)
    atomic_write_json(args.output, result)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
