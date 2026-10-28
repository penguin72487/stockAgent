#!/usr/bin/env python3
"""Recover retained Board PDF text locally; candidates never complete a news head.

Reads the existing closed diagnostic, private original and receipt. The shared
PDF review extractor keeps native text, rendered pages and OCR coordinates.
No HTTP client, writable queue, article admission or historical-clock inference.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import sys
import time

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_pdf import (
    BOARD_PDF_ORIGINAL_LAYOUT, BOARD_PDF_ORIGINAL_QUESTION,
    MAX_PDF_ORIGINAL_BYTES, board_pdf_original_source, private_board_pdf_permit,
)
from scripts.extract_taifex_rule_review_candidates import extract


_FLAGS = dict(diagnostic_only=True, accepted_as_article=False, publish=False,
    historical_point_in_time=False, per_article_license_verified=False,
    training_injection_enabled=False, remote_training_export=False)


def _private_file(root, relative, maximum):
    path = root / relative
    if (not path.resolve().is_relative_to(root.resolve()) or path.is_symlink()
            or not path.is_file() or path.stat().st_mode & 0o077
            or path.parent.stat().st_mode & 0o077 or path.stat().st_size > maximum):
        raise ValueError('private Board original path or resource bound invalid')
    return path


def read_original(root, request_id, source):
    """Verify the existing collector's container and positive source permit."""
    if (not isinstance(request_id, str) or not re.fullmatch('[0-9a-f]{64}', request_id)
            or not board_pdf_original_source(source)):
        raise ValueError('registered Board original required')
    with closing(sqlite3.connect((root / 'queue.sqlite3').resolve().as_uri() + '?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        job = db.execute('SELECT * FROM native_candidate_jobs WHERE request_id=?', (request_id,)).fetchone()
        if job is None or job['state'] != 'observed' or job['provider'] != source.provider:
            raise ValueError('closed Board original observation required')
        request, result = json.loads(job['request_json']), json.loads(job['result_json'])
        if (hashlib.sha256(job['request_json'].encode()).hexdigest() != request_id
                or request.get('evidence_contract_version') != 12
                or request.get('candidate_layout') != BOARD_PDF_ORIGINAL_LAYOUT
                or request.get('source', {}).get('data_fingerprint') != source.data_fingerprint
                or request.get('source', {}).get('authorization_fingerprint') != source.authorization_fingerprint
                or result.get('accepted_as_article') is not False):
            raise ValueError('Board original source identity changed')
        url = request['target']['url']
        head = db.execute('SELECT body_sha256 FROM articles WHERE url=? AND provider=?', (url, source.provider)).fetchone()
        if head is None:
            raise ValueError('Board original absent from frontier')
        if head['body_sha256'] is not None:
            return None  # Completed bodies are outside this remaining-only operation.
        reference = result.get('private_original', {})
        relative = 'reviewed_repairs/candidate_diagnostics/originals/' + request_id + '.parquet'
        receipt_relative = relative.removesuffix('.parquet') + '.receipt.json'
        if reference.get('path') != relative or reference.get('receipt_path') != receipt_relative:
            raise ValueError('Board original reference invalid')
        path = _private_file(root, relative, MAX_PDF_ORIGINAL_BYTES + (2 << 20))
        receipt_path = _private_file(root, receipt_relative, 65536)
        if sha256_file(path) != reference.get('sha256'):
            raise ValueError('Board original container hash mismatch')
        receipt = json.loads(receipt_path.read_bytes())
        if (receipt.get('original') != reference or receipt.get('request_id') != request_id
                or receipt.get('question') != BOARD_PDF_ORIGINAL_QUESTION
                or any(receipt.get(k) is not v for k, v in _FLAGS.items())):
            raise ValueError('Board original receipt invalid')
        parquet = pq.ParquetFile(path)
        if (parquet.metadata.num_rows != 1 or parquet.metadata.num_row_groups != 1
                or parquet.metadata.row_group(0).total_byte_size > 2 * MAX_PDF_ORIGINAL_BYTES):
            raise ValueError('Board original decoded resource bound invalid')
        raw, = parquet.read(use_threads=False).to_pylist()
        if (raw.get('request_id') != request_id or raw.get('provider') != source.provider
                or raw.get('request_url') != url or raw.get('source_document_url') != url
                or raw.get('final_url') != url or json.loads(raw['transport_redirects_json']) != []
                or any(raw.get(k) is not v for k, v in _FLAGS.items())
                or raw.get('data_fingerprint') != source.data_fingerprint
                or raw.get('authorization_fingerprint') != source.authorization_fingerprint):
            raise ValueError('Board original raw provenance invalid')
        response = Response(url, 200, json.loads(raw['headers_json']), raw['raw_response_body'])
        permit = private_board_pdf_permit(response, source, url)
        if (permit != receipt.get('permit') or permit['response_sha256'] != reference.get('response_sha256')
                or permit['response_sha256'] != raw.get('response_sha256')
                or len(response.body) != reference.get('response_bytes')):
            raise ValueError('Board original source permit mismatch')
        return response.body, dict(request_id=request_id, url=url, original=reference,
            receipt_sha256=sha256_file(receipt_path), observed_at_utc=raw['observed_at_utc'],
            candidate_job_sha256=hashlib.sha256(json.dumps(dict(job), sort_keys=True).encode()).hexdigest(),
            data_fingerprint=source.data_fingerprint, authorization_fingerprint=source.authorization_fingerprint)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--request-id', action='append', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--page', type=int, action='append',
        help='Only these physical one-based PDF pages; use a separate output profile')
    parser.add_argument('--rotate-page', action='append', default=[], metavar='PAGE:DEGREES',
        help='Explicit clockwise 90/180/270 degree review rotation for a selected page')
    args = parser.parse_args(argv)
    if len(set(args.request_id)) != len(args.request_id) or not 1 <= len(args.request_id) <= 16:
        parser.error('one to sixteen distinct retained originals required')
    rotations = {}
    try:
        for value in args.rotate_page:
            page,angle=map(int,value.split(':'))
            if page in rotations or angle not in (90,180,270):
                raise ValueError('invalid or duplicate rotation')
            rotations[page]=angle
    except ValueError:
        parser.error('rotations must be distinct PAGE:90, PAGE:180 or PAGE:270')
    if (args.page is not None and (len(args.request_id)!=1 or any(p<=0 for p in args.page)
            or len(set(args.page))!=len(args.page) or len(args.page)>64)
            or rotations and (args.page is None or set(rotations)-set(args.page))):
        parser.error('page repairs require one retained original, distinct physical pages and matching rotations')
    config, registry, repo = load_config(args.config)
    root = (repo / config['root']).resolve()
    if args.output.is_symlink():
        parser.error('output directory cannot be a symlink')
    output = args.output.resolve()
    if output == root or output.is_relative_to(root):
        parser.error('candidate output must be separate from the canonical news queue')
    if output.exists() and (output.is_symlink() or output.stat().st_mode & 0o077):
        parser.error('private output directory required')
    output.mkdir(parents=True, exist_ok=True, mode=0o700)
    from stockagent.data_sync.cache_capacity import acquisition_dispatch_status
    from stockagent.data_sync.completed_forward_capacity import CapacityDeferred
    try:
        capacity = acquisition_dispatch_status(output)
    except CapacityDeferred as error:
        capacity = dict(state='deferred', reason=error.details['reason'], retryable=True)
    if capacity['state'] == 'deferred':
        # The local filesystem count may be virtual WSL space, not the
        # physical backing count. Do not expose arbitrary diagnostic fields.
        details = dict(state='capacity_deferred', publisher_requests=0, queue_writes=0)
        for key in ('reason', 'retryable', 'required_bytes', 'minimum_free_bytes'):
            if key in capacity:
                details[key] = capacity[key]
        if 'available_bytes' in capacity:
            details['filesystem_available_bytes'] = capacity['available_bytes']
        print(json.dumps(details), flush=True)
        return 75
    started = time.monotonic()
    completed, skipped = [], []

    def report():
        progress = dict(contract_version=1, at_utc=datetime.now(timezone.utc).isoformat(),
            selected_originals=len(args.request_id), candidates_processed=len(completed), skipped_complete=skipped,
            documents=completed, elapsed_seconds=time.monotonic()-started, publisher_requests=0, queue_writes=0,
            candidate_only=True, historical_point_in_time=False, training_injection_enabled=False,
            source_code_sha256={str(Path(p).resolve().relative_to(repo)):sha256_file(p)
                for p in (__file__, extract.__code__.co_filename)}, goal_complete=False)
        atomic_write_json(output/'progress.json', progress, compact=True)
        (output/'progress.json').chmod(0o600)
        print(json.dumps({k:v for k,v in progress.items() if k not in {'documents','source_code_sha256'}}), flush=True)

    for identifier in args.request_id:
        original = read_original(root, identifier, registry.sources['fed_board'])
        if original is None:
            skipped.append(identifier)
            report()
            continue
        payload, provenance = original
        # This uses the established native/OCR, blank-page and resource logic.
        # The input is already a verified collector record, so no gzip copy.
        item = dict(document=dict(url=provenance['url'], content_sha256=provenance['original']['response_sha256'],
            raw_sha256=provenance['original']['sha256']))
        receipt = extract(root, output, item, max_pages=64, dpi=200, languages='eng', verified_payload=payload,
            **(dict(selected_pages=args.page,page_rotations=rotations) if args.page is not None else {}))
        # Closed evidence must be unchanged even if the independent owner kept running.
        reread = read_original(root, identifier, registry.sources['fed_board'])
        if reread is None or reread[1] != provenance:
            raise ValueError('Board original changed during candidate extraction')
        completed.append(dict(provenance, receipt=receipt,
            derived_text_accuracy_verified=False, accepted_as_article=False, published_at_raw=None))
        report()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
