"""Prepare exact CMNews retained-quality requests outside the news inbox.

Only independently read-back login-mask candidates are considered. Each
candidate is revalidated with the canonical request builder. This command
never submits requests, calls a publisher or mutates the collector's DB.
"""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import tempfile

import pyarrow as pa

from downloader.artifact_io import atomic_write_bytes, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_repairs import _bytes, _write_once
from downloader.news_fulltext_stored_quality import (
    MAX_TARGETS, prepare_stored_quality_reviews, validate_request,
)
from scripts.audit_news_provider_storage_readback import snapshot, state_snapshot


def prior_targets(directory):
    """Validate previously staged requests before considering deduplication."""
    if directory is None:
        return {}, None
    directory = Path(directory).resolve(strict=True)
    manifest_path = directory / 'manifest.json'
    if manifest_path.is_symlink():
        raise ValueError('prior_staged_manifest_unsafe')
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    previous = {}
    for item in manifest['staged']:
        digest = item['request_id']
        if not isinstance(digest, str) or not re.fullmatch('[0-9a-f]{64}', digest):
            raise ValueError('prior_staged_request_id_invalid')
        path = directory / (digest + '.json')
        if path.is_symlink() or not path.is_file():
            raise ValueError('prior_staged_request_unavailable')
        payload = path.read_bytes()
        if hashlib.sha256(payload).hexdigest() != digest:
            raise ValueError('prior_staged_request_digest_mismatch')
        request = validate_request(json.loads(payload))
        if len(request['targets']) != item['targets']:
            raise ValueError('prior_staged_target_count_mismatch')
        for target in request['targets']:
            key = target['id']
            if key in previous:
                raise ValueError('prior_staged_duplicate_target')
            previous[key] = (target, request['originals'][key], request['sources'][target['provider']],
                             request['expected_error'])
    return previous, hashlib.sha256(manifest_bytes).hexdigest()


def _same_previous(request, target, previous):
    prior = previous.get(target['id'])
    return prior == (target, request['originals'][target['id']], request['sources'][target['provider']],
                     request['expected_error'])


def _bound_candidate(target, request, candidate):
    key, path, state, body_sha, captured, response_sha = candidate
    return (target['id'] == key and target['provider'] == 'cmnews' and target['state'] == state == 'complete'
            and target['parquet_path'] == path and target['body_sha256'] == body_sha
            and target['captured_at_utc'] == captured
            and request['originals'][key]['response_sha256'] == response_sha)


def prepare(root, registry, checkpoint, readback, output, *, prior=None):
    root = Path(root).resolve(strict=True)
    checkpoint, readback = Path(checkpoint).resolve(strict=True), Path(readback).resolve(strict=True)
    output = Path(output)
    if output.is_symlink():
        raise ValueError('new_stage_must_be_outside_source')
    output = output.resolve()
    if output.is_relative_to(root) or output.exists():
        raise ValueError('new_stage_must_be_outside_source')
    previous, prior_sha = prior_targets(prior)
    readback_bytes = readback.read_bytes()
    proof = json.loads(readback_bytes)
    if (proof.get('contract') != 'cmnews_retained_login_mask_readback_v1' or not proof.get('accepted')
            or proof.get('checkpoint_sha256') != sha256_file(checkpoint)):
        raise ValueError('independent_readback_not_accepted')
    owner = os.open(root / 'owner.lock', os.O_RDONLY | os.O_NOFOLLOW)
    audit_lock = None
    try:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        audit_lock = os.open(str(checkpoint) + '.lock', os.O_RDONLY | os.O_NOFOLLOW)
        fcntl.flock(audit_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        source_before, checkpoint_before = snapshot(root), state_snapshot(checkpoint)
        checkpoint_sha = sha256_file(checkpoint)
        if source_before != proof['source_snapshot'] or checkpoint_sha != proof['checkpoint_sha256']:
            raise ValueError('source_or_checkpoint_changed')
        with closing(sqlite3.connect(checkpoint.as_uri() + '?mode=ro&immutable=1', uri=True)) as state, \
             closing(sqlite3.connect((root / 'queue.sqlite3').as_uri() + '?mode=ro&immutable=1', uri=True)) as source:
            candidates = state.execute('SELECT article_id,path,state,body_sha256,captured_at_utc,response_sha256 '
                "FROM candidates WHERE native_outcome='paywall_or_login' ORDER BY article_id").fetchall()
            if len(candidates) != proof['outcomes'].get('confirmed_login_mask', 0):
                raise ValueError('candidate_count_changed')
            counts, failures, payloads = Counter(), [], []
            selected = []
            for candidate in candidates:
                if candidate[2] != 'complete':
                    counts['already_noncomplete'] += 1
                    continue
                row = source.execute('SELECT url FROM articles WHERE id=? AND provider=?',
                                     (bytes.fromhex(candidate[0]), 'cmnews')).fetchone()
                if row is None:
                    failures.append({'article_id': candidate[0], 'error': 'head_missing'})
                else:
                    selected.append((candidate, row[0]))

            def stage_group(group):
                try:
                    built = prepare_stored_quality_reviews(root, registry, [item[1] for item in group],
                        reason='Retained original shows a scoped member login mask', expected_error='paywall_or_login')
                except (ValueError, OSError, KeyError, TypeError, sqlite3.Error, NewsError, pa.ArrowException) as exc:
                    if len(group) > 1:
                        for item in group:
                            stage_group([item])
                    else:
                        failures.append({'article_id': group[0][0][0],
                                         'error': type(exc).__name__ + ':' + str(exc)[:160]})
                    return
                request = built['request']
                requested = {item[0][0]: item[0] for item in group}
                retained = []
                for target in request['targets']:
                    candidate = requested.get(target['id'])
                    if candidate is None or not _bound_candidate(target, request, candidate):
                        failures.append({'article_id': target['id'], 'error': 'candidate_binding_changed'})
                    elif _same_previous(request, target, previous):
                        counts['already_staged'] += 1
                    else:
                        retained.append(target)
                if not retained:
                    return
                filtered = dict(request, targets=retained,
                    originals={target['id']: request['originals'][target['id']] for target in retained})
                validate_request(filtered)
                payload = _bytes(filtered)
                payloads.append((hashlib.sha256(payload).hexdigest(), payload, len(retained)))
                counts['new_staged'] += len(retained)

            for offset in range(0, len(selected), MAX_TARGETS):
                stage_group(selected[offset:offset + MAX_TARGETS])
        if (snapshot(root) != source_before or state_snapshot(checkpoint) != checkpoint_before
                or sha256_file(checkpoint) != checkpoint_sha
                or sha256_file(readback) != hashlib.sha256(readback_bytes).hexdigest()):
            raise ValueError('source_or_proof_changed_during_preparation')
        if prior is not None and prior_targets(prior) != (previous, prior_sha):
            raise ValueError('prior_staged_requests_changed')
        if len(candidates) != counts['already_noncomplete'] + counts['already_staged'] + counts['new_staged'] + len(failures):
            raise ValueError('candidate_preparation_count_mismatch')
        output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        temporary = Path(tempfile.mkdtemp(prefix=output.name + '.tmp-', dir=output.parent))
        for digest, payload, _ in payloads:
            _write_once(temporary / (digest + '.json'), payload)
        report = {'contract': 'cmnews_retained_quality_private_stage_v1',
            'observed_at_utc': datetime.now(timezone.utc).isoformat(), 'candidates': len(candidates),
            'already_noncomplete': counts['already_noncomplete'], 'already_staged': counts['already_staged'],
            'new_staged': counts['new_staged'], 'failed_preflight': len(failures), 'failures': failures,
            'staged': [{'request_id': digest, 'targets': size} for digest, _payload, size in payloads],
            'source_snapshot': source_before, 'checkpoint_sha256': checkpoint_sha,
            'readback_sha256': hashlib.sha256(readback_bytes).hexdigest(), 'prior_manifest_sha256': prior_sha,
            'publisher_requests': 0, 'production_queue_writes': 0, 'production_inbox_requests': 0,
            'submitted': False, 'scope': 'private requests; owner must revalidate before any future admission'}
        atomic_write_bytes(temporary / 'manifest.json', (json.dumps(report, ensure_ascii=False, sort_keys=True) + '\n').encode(),
                           durable=True, mode=0o600)
        os.rename(temporary, output)
        parent_fd = os.open(output.parent, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
        return report
    finally:
        if audit_lock is not None:
            os.close(audit_lock)
        os.close(owner)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--checkpoint', type=Path, required=True)
    parser.add_argument('--readback', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--prior', type=Path)
    args = parser.parse_args(argv)
    _config, registry, _repo = load_config(args.config)
    result = prepare(args.root, registry, args.checkpoint, args.readback, args.output, prior=args.prior)
    print(json.dumps({key: result[key] for key in ('candidates', 'already_noncomplete', 'already_staged',
        'new_staged', 'failed_preflight', 'publisher_requests', 'production_queue_writes',
        'production_inbox_requests', 'submitted')}, ensure_ascii=False), flush=True)
    return 0 if not result['failed_preflight'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
