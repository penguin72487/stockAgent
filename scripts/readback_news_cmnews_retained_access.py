"""Independently reconcile a finished offline CMNews access audit.

Checks every selected source head against the checkpoint and replays every
candidate's exact retained raw response through the canonical parser. This
does not reclassify heads, fetch pages or certify that other bodies are full.
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
import sys
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from lxml import etree
import pyarrow
import pyarrow.parquet as parquet

from downloader.artifact_io import atomic_write_bytes, sha256_file
from downloader.news_fulltext_policy import NewsError, normalize_url
from downloader.news_fulltext_publishers import cmnews_visible_article
from scripts.audit_news_provider_storage_readback import snapshot, source_file, stamp, state_snapshot

SUPPORTED_CONTRACTS = {'cmnews_retained_login_mask_audit_v2', 'cmnews_retained_login_mask_audit_v3'}
SCANNER_PATH = 'scripts/audit_news_cmnews_retained_access.py'
INPUTS = (SCANNER_PATH, 'scripts/audit_news_provider_storage_readback.py',
          'downloader/news_fulltext_publishers.py', 'downloader/news_fulltext_policy.py',
          'downloader/news_fulltext_queue.py', 'downloader/artifact_io.py')
REPO = Path(__file__).resolve().parent.parent


def implementation():
    """Measure the scanner's bound inputs independently of the scanner."""
    return {'files': {name: sha256_file(REPO / name) for name in INPUTS},
            'python': list(sys.version_info[:3]), 'sqlite': sqlite3.sqlite_version,
            'pyarrow': pyarrow.__version__, 'lxml': version('lxml'),
            'beautifulsoup4': version('beautifulsoup4')}


def check_reference(reference):
    if (state_snapshot(Path(reference['state'])) != reference['state_stamp']
            or sha256_file(Path(reference['state'])) != reference['state_sha256']
            or sha256_file(Path(reference['readback'])) != reference['readback_sha256']):
        raise ValueError('reference_audit_changed')


def check_implementation(expected, contract, implementation_root=None):
    """Keep old scan evidence verifiable while requiring the active parser ABI.

    Only the scanner's lookup strategy may differ for archived v2 evidence;
    its original bytes and all parser/runtime dependencies remain bound.
    """
    active = implementation()
    if implementation_root is None:
        if active != expected:
            raise ValueError('audit_implementation_changed')
        return active
    if contract != 'cmnews_retained_login_mask_audit_v2':
        raise ValueError('archive_only_supported_for_v2')
    archive = Path(implementation_root).resolve(strict=True)
    files = expected.get('files', {})
    if (not archive.is_dir() or set(files) != set(active.get('files', {}))
            or {key: value for key, value in expected.items() if key != 'files'}
            != {key: value for key, value in active.items() if key != 'files'}):
        raise ValueError('audit_implementation_changed')
    for name, digest in files.items():
        archived = (archive / name).resolve(strict=True)
        if (not archived.is_relative_to(archive) or not archived.is_file()
                or sha256_file(archived) != digest
                or (name != SCANNER_PATH and active['files'][name] != digest)):
            raise ValueError('audit_implementation_changed')
    return active


def independent_mask(soup, url):
    scopes = []
    for node in soup.select('.articleContent[data-article-path]'):
        try:
            if normalize_url(urljoin(url, node.get('data-article-path'))) == url:
                scopes.append(node)
        except (NewsError, ValueError):
            pass
    if len(scopes) != 1:
        return False
    for node in scopes[0].select('.articleContent__textContent vip-content.vipWrapper .vipContent .utilsMask p'):
        if node.get_text(' ', strip=True) in {'請登入以查看完整文章', '閱讀VIP文章請先登入理財寶會員'}:
            return True
    return False


def readback(root, checkpoint, run_log, *, implementation_root=None):
    root = Path(root).resolve(strict=True)
    checkpoint, run_log = Path(checkpoint).resolve(strict=True), Path(run_log).resolve(strict=True)
    verifier_sha = sha256_file(Path(__file__).resolve())
    log_bytes = run_log.read_bytes()
    reports = [json.loads(line) for line in log_bytes.decode().splitlines()
               if line.startswith('{') and json.loads(line).get('contract') in SUPPORTED_CONTRACTS]
    if len(reports) != 1 or not reports[0].get('completed_scope'):
        raise ValueError('finished_audit_report_required')
    report = reports[0]
    owner = os.open(root / 'owner.lock', os.O_RDONLY | os.O_NOFOLLOW)
    lock = None
    try:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock = os.open(str(checkpoint) + '.lock', os.O_RDWR | os.O_NOFOLLOW)
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        before, state_before, state_sha = snapshot(root), state_snapshot(checkpoint), sha256_file(checkpoint)
        with closing(sqlite3.connect(checkpoint.as_uri() + '?mode=ro&immutable=1', uri=True)) as state, \
             closing(sqlite3.connect((root / 'queue.sqlite3').as_uri() + '?mode=ro&immutable=1', uri=True)) as source:
            meta = {key: json.loads(value) for key, value in state.execute('SELECT key,value FROM metadata')}
            ref = meta['reference']
            contract = meta.get('contract')
            if (contract not in SUPPORTED_CONTRACTS or report.get('contract') != contract
                    or before != ref['snapshot'] or report.get('reference') != ref):
                raise ValueError('audit_identity_changed')
            code_before = check_implementation(meta['implementation'], contract, implementation_root)
            check_reference(ref)
            with closing(sqlite3.connect(Path(ref['state']).as_uri() + '?mode=ro&immutable=1', uri=True)) as witness:
                own = state.execute('SELECT path,selected_heads,selected_ids_sha256,outcomes_json,error FROM jobs ORDER BY path')
                expected = witness.execute('SELECT path,receipt_path,selected_heads,selected_ids_sha256,body_stamp,receipt_stamp '
                                           'FROM jobs WHERE error IS NULL ORDER BY path')
                # stored_bodies is keyed by provider, not by (provider,path).
                # Sort the provider's heads once, then stream the same path
                # order as the two audit ledgers instead of rescanning every
                # head for every batch.
                source_rows = source.execute("SELECT parquet_path,id,url,body_sha256,body_bytes,captured_at_utc,state "
                    "FROM articles INDEXED BY stored_bodies WHERE provider='cmnews' AND body_sha256 IS NOT NULL "
                    "AND parquet_path IS NOT NULL AND parquet_path<>'' ORDER BY parquet_path,id")
                groups = groupby(source_rows, key=lambda row: row[0])
                counts, totals, replayed = Counter(), Counter(), Counter()
                for job, expected_job, group in zip_longest(own, expected, groups):
                    if job is None or expected_job is None or group is None:
                        raise ValueError('checkpoint_scope_count_mismatch')
                    path, heads, ids_sha, raw_outcomes, error = job
                    w_path, receipt, w_heads, w_ids, body_stamp, receipt_stamp = expected_job
                    group_path, grouped = group
                    if (path, heads, ids_sha) != (w_path, w_heads, w_ids) or path != group_path:
                        raise ValueError('checkpoint_scope_identity_mismatch')
                    rows = [row[1:] for row in grouped]
                    if (len(rows) != heads or hashlib.sha256(b''.join(row[0] for row in rows)).hexdigest() != ids_sha
                            or source.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (path,)).fetchone() != (receipt,)):
                        raise ValueError('source_head_or_receipt_changed')
                    body, receipt_file = source_file(root, path), source_file(root, receipt)
                    if stamp(body) != json.loads(body_stamp) or stamp(receipt_file) != json.loads(receipt_stamp):
                        raise ValueError('verified_artifact_changed')
                    outcomes = json.loads(raw_outcomes)
                    if sum(outcomes.values()) != heads:
                        raise ValueError('batch_outcome_count_mismatch')
                    totals.update(outcomes)
                    counts['batches'] += 1
                    counts['heads'] += heads
                    counts['failed_batches'] += error is not None
                    candidates = state.execute('SELECT article_id,path,state,body_sha256,captured_at_utc,'
                        'response_sha256,native_outcome FROM candidates WHERE path=? ORDER BY article_id', (path,)).fetchall()
                    if len(candidates) != outcomes.get('confirmed_login_mask', 0) + outcomes.get('native_mismatch', 0):
                        raise ValueError('candidate_batch_count_mismatch')
                    unknowns = state.execute('SELECT article_id,path,state,body_sha256,captured_at_utc,'
                        'response_sha256,outcome FROM unresolved WHERE path=? ORDER BY article_id', (path,)).fetchall()
                    unknown_counts = Counter(row[-1] for row in unknowns)
                    unresolved_kinds = ('scope_not_unique', 'html_parse_error',
                                        'original_unavailable_or_digest_mismatch', 'native_mismatch')
                    if (any(unknown_counts[name] != outcomes.get(name, 0) for name in unresolved_kinds)
                            or sum(unknown_counts.values()) != sum(outcomes.get(name, 0) for name in unresolved_kinds)):
                        raise ValueError('unresolved_batch_count_mismatch')
                    by_id = {row[0].hex(): row for row in rows}
                    for key, stored_path, status, digest, captured, _response_sha, _outcome in unknowns:
                        head = by_id.get(key)
                        if head is None or (stored_path, status, digest, captured) != (path, head[5], head[2], head[4]):
                            raise ValueError('unresolved_head_binding_changed')
                    mismatch_ids = {row[0] for row in candidates if row[-1] != 'paywall_or_login'}
                    if mismatch_ids != {row[0] for row in unknowns if row[-1] == 'native_mismatch'}:
                        raise ValueError('native_mismatch_identity_changed')
                    if not candidates:
                        continue
                    f = parquet.ParquetFile(body)
                    fields = ['article_id', 'provider', 'requested_url', 'body_sha256', 'body_bytes',
                              'captured_at_utc', 'response_sha256', 'raw_response_body']
                    if 'source_response_article_id' in f.schema_arrow.names:
                        fields.append('source_response_article_id')
                    records = f.read(columns=fields, use_threads=False).to_pylist()
                    originals = {rec['article_id']: rec['raw_response_body'] for rec in records if rec['raw_response_body'] is not None}
                    for key, stored_path, status, digest, captured, response_sha, native in candidates:
                        selected = [rec for rec in records if rec['article_id'] == key]
                        if len(selected) != 1 or key not in by_id:
                            raise ValueError('candidate_not_unique_in_batch')
                        ident, url, body_sha, size, when, current_state = by_id[key]
                        rec = selected[0]
                        raw = originals.get(rec.get('source_response_article_id') or key)
                        if ((stored_path, status, digest, captured) != (path, current_state, body_sha, when)
                                or rec['provider'] != 'cmnews' or rec['requested_url'] != url
                                or hashlib.sha256(url.encode()).digest() != ident
                                or (rec['body_sha256'], rec['body_bytes'], rec['captured_at_utc']) != (digest, size, captured)
                                or not isinstance(raw, bytes) or hashlib.sha256(raw).hexdigest() != response_sha
                                or rec['response_sha256'] != response_sha):
                            raise ValueError('candidate_original_binding_changed')
                        soup = BeautifulSoup(raw, 'lxml')
                        if not independent_mask(soup, url):
                            raise ValueError('candidate_mask_not_independently_reproduced')
                        try:
                            cmnews_visible_article(soup, page_url=url)
                            actual = 'accepted'
                        except NewsError as exc:
                            actual = exc.code
                        except (ValueError, TypeError, RecursionError, etree.LxmlError) as exc:
                            actual = 'parser_exception:' + type(exc).__name__
                        if actual != native:
                            raise ValueError('candidate_native_outcome_changed')
                        replayed[actual] += 1
                    if stamp(body) != json.loads(body_stamp) or stamp(receipt_file) != json.loads(receipt_stamp):
                        raise ValueError('verified_artifact_changed_during_replay')
            unresolved = sum(totals[name] for name in ('scope_not_unique', 'html_parse_error',
                'original_unavailable_or_digest_mismatch', 'native_mismatch', 'batch_validation_error'))
            if (counts['batches'] != ref['batches'] or counts['heads'] != ref['heads']
                    or counts['batches'] != report['processed_batches'] or counts['heads'] != report['processed_heads']
                    or counts['failed_batches'] != report['failed_batches'] or dict(totals) != report['outcomes']
                    or dict(replayed) != report['candidate_native_outcomes']
                    or unresolved != report['unresolved_heads']
                    or bool(report['accepted']) != (counts['failed_batches'] == 0)
                    or not report['scope_membership_verified']):
                raise ValueError('audit_aggregate_mismatch')
            check_reference(ref)
            if (snapshot(root) != before or state_snapshot(checkpoint) != state_before
                    or sha256_file(checkpoint) != state_sha or sha256_file(run_log) != hashlib.sha256(log_bytes).hexdigest()
                    or sha256_file(Path(__file__).resolve()) != verifier_sha
                    or check_implementation(meta['implementation'], contract, implementation_root) != code_before):
                raise ValueError('audit_inputs_changed_during_readback')
            return {'contract': 'cmnews_retained_login_mask_readback_v1',
                    'checked_at_utc': datetime.now(timezone.utc).isoformat(), 'accepted': report['accepted'],
                    'processed_batches': counts['batches'], 'processed_heads': counts['heads'],
                    'failed_batches': counts['failed_batches'], 'outcomes': dict(totals),
                    'candidates_independently_replayed': dict(replayed), 'unresolved_heads': report['unresolved_heads'],
                    'checkpoint_sha256': state_sha, 'run_log_sha256': hashlib.sha256(log_bytes).hexdigest(),
                    'verifier_sha256': verifier_sha,
                    'reference_scope_sha256': ref['scope_sha256'], 'source_snapshot': before,
                    'publisher_requests': 0, 'production_queue_writes': 0,
                    'scope': 'current retained heads and exact observed mask only; not publisher history or full-body completeness'}
    finally:
        if lock is not None:
            os.close(lock)
        os.close(owner)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('root', type=Path)
    parser.add_argument('checkpoint', type=Path)
    parser.add_argument('run_log', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--implementation-root', type=Path,
                        help='verified archived scanner implementation for a v2 audit')
    args = parser.parse_args(argv)
    root = args.root.resolve(strict=True)
    if args.output.is_symlink():
        raise ValueError('new_output_must_be_outside_source')
    output = args.output.resolve()
    if output.is_relative_to(root) or output.exists():
        raise ValueError('new_output_must_be_outside_source')
    result = readback(root, args.checkpoint, args.run_log, implementation_root=args.implementation_root)
    atomic_write_bytes(output, (json.dumps(result, ensure_ascii=False, sort_keys=True) + '\n').encode(),
                       durable=True, mode=0o600)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
    return 0 if result['accepted'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
