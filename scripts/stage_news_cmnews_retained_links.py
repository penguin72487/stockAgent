"""Seal independently verified CMNews hints outside the resident owner's inbox.

This is an offline staging aid, not an owner or a downloader. The owner must
revalidate a staged request before later admission; a link is not a body.
"""
from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
from urllib.parse import urlsplit

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_repairs import MAX_PENDING, MAX_TARGETS
from downloader.news_fulltext_url_metadata import _load, _source_identity, submit_urls
from scripts.audit_news_cmnews_retained_access import check_reference, reference_identity
from scripts.audit_news_cmnews_retained_links import CONTRACT, implementation, local_url
from scripts.audit_news_provider_storage_readback import snapshot, state_snapshot
from scripts.readback_news_cmnews_retained_links import _finished_report, _pending_urls


def stage(root, checkpoint, log, readback_path, stage_root, allowed_base, registry):
    root = Path(root).resolve(strict=True)
    checkpoint = Path(checkpoint).resolve(strict=True)
    log = Path(log).resolve(strict=True)
    readback_path = Path(readback_path).resolve(strict=True)
    allowed_base = Path(allowed_base).resolve(strict=True)
    destination = Path(stage_root)
    parent = destination.parent.resolve(strict=True)
    destination = parent / destination.name
    if (not destination.is_relative_to(allowed_base) or destination.is_relative_to(root)
            or destination.is_relative_to((root.parent / 'data_news').resolve())
            or checkpoint.is_relative_to(root) or readback_path.is_relative_to(root)):
        raise ValueError('private_stage_outside_source_required')
    if destination.exists() or destination.is_symlink():
        raise ValueError('private_stage_must_be_new')
    readback_hash = sha256_file(readback_path)
    readback = json.loads(readback_path.read_bytes())
    if (readback.get('contract') != 'cmnews_retained_article_link_readback_v1'
            or readback.get('accepted') is not True or readback.get('publisher_requests') != 0
            or readback.get('production_queue_writes') != 0
            or readback.get('target_identity_or_body_verified') is not False):
        raise ValueError('accepted_independent_readback_required')
    report = _finished_report(log)
    checkpoint_before = state_snapshot(checkpoint)
    if sha256_file(checkpoint) != readback['checkpoint_sha256'] or sha256_file(log) != readback['log_sha256']:
        raise ValueError('readback_inputs_changed')
    owner = os.open(root / 'owner.lock', os.O_RDONLY | os.O_NOFOLLOW)
    try:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with closing(sqlite3.connect(checkpoint.as_uri() + '?mode=ro&immutable=1', uri=True)) as state:
            state.execute('PRAGMA query_only=ON')
            meta = {key: json.loads(value) for key, value in state.execute('SELECT key,value FROM metadata')}
            ref = meta['reference']
            if (meta['contract'] != CONTRACT or meta['implementation'] != implementation()
                    or reference_identity(root, Path(ref['state']), Path(ref['readback'])) != ref
                    or report['reference'] != ref or snapshot(root) != ref['snapshot']
                    or readback['source_snapshot'] != ref['snapshot']
                    or readback['reference_scope_sha256'] != ref['scope_sha256']):
                raise ValueError('source_reference_changed')
            pending, pending_before = _pending_urls(root)
            all_urls = [row[0] for row in state.execute("SELECT url FROM targets WHERE kind='article' "
                                                        "AND known_provider IS NULL ORDER BY url")]
            digest = hashlib.sha256()
            for url in all_urls:
                digest.update(url.encode() + b'\n')
            if (len(all_urls) != readback['unregistered_article_witnesses_verified']
                    or digest.hexdigest() != readback['unregistered_article_set_sha256']):
                raise ValueError('readback_target_set_changed')
            candidates = [url for url in all_urls if url not in pending]
            identities = set()
            for url in candidates:
                source = registry.match(url)
                if (local_url(url, url) != url or not source or source.provider != 'cmnews'
                        or not source.allowed or not re.fullmatch(source.article_path_pattern, urlsplit(url).path)):
                    raise ValueError('target_outside_current_source_scope')
                identity = _source_identity(registry, url)
                identities.add(json.dumps(identity, sort_keys=True))
            if len(identities) > 1:
                raise ValueError('multiple_source_contracts_in_one_stage')
            destination.mkdir(mode=0o700)
            requests = []
            for index in range(0, len(candidates), MAX_TARGETS):
                group = destination / f'group_{index // (MAX_TARGETS * MAX_PENDING) + 1:04d}'
                if not group.exists():
                    group.mkdir(mode=0o700)
                intended = candidates[index:index + MAX_TARGETS]
                receipt = submit_urls(group, registry, intended)
                path = Path(receipt['pending_path'])
                request = _load(path)
                if ([item['url'] for item in request['targets']] != intended
                        or any(item['source'] != _source_identity(registry, item['url']) for item in request['targets'])
                        or receipt['request_id'] != sha256_file(path)):
                    raise ValueError('staged_request_readback_mismatch')
                requests.append({'path': str(path.relative_to(destination)), 'sha256': receipt['request_id'],
                                 'targets': len(intended)})
            if (_pending_urls(root) != (pending, pending_before) or snapshot(root) != ref['snapshot']
                    or state_snapshot(checkpoint) != checkpoint_before or sha256_file(checkpoint) != readback['checkpoint_sha256']
                    or sha256_file(log) != readback['log_sha256'] or sha256_file(readback_path) != readback_hash):
                raise ValueError('inputs_changed_during_staging')
            check_reference(ref)
            manifest = {'contract': 'cmnews_retained_article_link_private_stage_v1', 'accepted': True,
                        'created_at_utc': datetime.now(timezone.utc).isoformat(),
                        'source_snapshot': ref['snapshot'], 'reference_scope_sha256': ref['scope_sha256'],
                        'checkpoint_sha256': readback['checkpoint_sha256'], 'readback_sha256': readback_hash,
                        'source_identity': json.loads(next(iter(identities))) if identities else None,
                        'unregistered_hints_verified': len(all_urls), 'already_in_owner_inbox': len(all_urls) - len(candidates),
                        'staged_candidates': len(candidates), 'requests': requests,
                        'publisher_requests': 0, 'production_queue_writes': 0, 'owner_admission': False,
                        'target_identity_or_body_verified': False,
                        'revalidation_required_before_owner_admission': True}
            atomic_write_json(destination / 'manifest.json', manifest)
            return manifest
    finally:
        os.close(owner)


def seal_handoff(root, checkpoint, log, readback_path, stage_root, allowed_base, registry, output):
    """Bind a complete staged cohort to the independently reviewed source heads.

    This only creates a private artifact.  A later owner must still compare
    these exact heads, batch identities and the current source policy before
    handing the requests to its ordinary URL metadata inbox.
    """
    root = Path(root).resolve(strict=True)
    checkpoint, log, readback_path = (Path(p).resolve(strict=True) for p in (checkpoint, log, readback_path))
    allowed_base = Path(allowed_base).resolve(strict=True)
    stage_root = Path(stage_root).resolve(strict=True)
    output = Path(output)
    output = output.parent.resolve(strict=True) / output.name
    if (not stage_root.is_relative_to(allowed_base) or not output.is_relative_to(allowed_base)
            or stage_root.is_relative_to(root) or output.is_relative_to(root)
            or output.exists() or output.is_symlink()):
        raise ValueError('handoff_artifact_must_be_new_and_private')
    manifest_file = stage_root / 'manifest.json'
    manifest_hash, readback_hash = sha256_file(manifest_file), sha256_file(readback_path)
    manifest, independent = json.loads(manifest_file.read_bytes()), json.loads(readback_path.read_bytes())
    if (manifest.get('contract') != 'cmnews_retained_article_link_private_stage_v1'
            or manifest.get('accepted') is not True or manifest.get('owner_admission') is not False
            or manifest.get('revalidation_required_before_owner_admission') is not True
            or manifest.get('already_in_owner_inbox') != 0
            or independent.get('contract') != 'cmnews_retained_article_link_readback_v1'
            or independent.get('accepted') is not True or independent.get('publisher_requests') != 0
            or independent.get('production_queue_writes') != 0
            or manifest.get('readback_sha256') != readback_hash
            or independent.get('checkpoint_sha256') != manifest.get('checkpoint_sha256')
            or sha256_file(checkpoint) != manifest.get('checkpoint_sha256')
            or sha256_file(log) != independent.get('log_sha256')):
        raise ValueError('handoff_evidence_changed')
    report = _finished_report(log)
    owner = os.open(root / 'owner.lock', os.O_RDONLY | os.O_NOFOLLOW)
    try:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with closing(sqlite3.connect(checkpoint.as_uri() + '?mode=ro&immutable=1', uri=True)) as state, \
             closing(sqlite3.connect((root / 'queue.sqlite3').as_uri() + '?mode=ro&immutable=1', uri=True)) as source:
            meta = {key: json.loads(value) for key, value in state.execute('SELECT key,value FROM metadata')}
            ref = meta['reference']
            if (meta['contract'] != CONTRACT or meta['implementation'] != implementation()
                    or reference_identity(root, Path(ref['state']), Path(ref['readback'])) != ref
                    or report['reference'] != ref or snapshot(root) != manifest['source_snapshot']
                    or independent['source_snapshot'] != ref['snapshot']
                    or independent['reference_scope_sha256'] != ref['scope_sha256']
                    or manifest['reference_scope_sha256'] != ref['scope_sha256']):
                raise ValueError('handoff_source_reference_changed')
            rows = state.execute("SELECT url,witness_batch,parent_id,parent_url,captured_at_utc,response_sha256 "
                                 "FROM targets WHERE kind='article' AND known_provider IS NULL ORDER BY url").fetchall()
            digest = hashlib.sha256()
            for row in rows:
                digest.update(row[0].encode() + b'\n')
            if (len(rows) != manifest['staged_candidates'] or len(rows) != independent['unregistered_article_witnesses_verified']
                    or digest.hexdigest() != independent['unregistered_article_set_sha256']):
                raise ValueError('handoff_target_set_changed')
            staged, requests = set(), []
            for item in manifest['requests']:
                path = (stage_root / item['path']).resolve(strict=True)
                if (not path.is_relative_to(stage_root) or path.name != item['sha256'] + '.json'
                        or sha256_file(path) != item['sha256']):
                    raise ValueError('handoff_request_changed')
                request = _load(path)
                if len(request['targets']) != item['targets']:
                    raise ValueError('handoff_request_count_changed')
                for target in request['targets']:
                    url = target['url']
                    matched = registry.match(url)
                    if (url in staged or local_url(url, url) != url or not matched or not matched.allowed
                            or matched.provider != 'cmnews' or not re.fullmatch(matched.article_path_pattern, urlsplit(url).path)
                            or target['source'] != manifest['source_identity']
                            or target['source'] != _source_identity(registry, url)):
                        raise ValueError('handoff_target_or_policy_changed')
                    staged.add(url)
                requests.append({'path': str(path), 'sha256': item['sha256'], 'targets': item['targets']})
            if staged != {row[0] for row in rows}:
                raise ValueError('handoff_staged_target_set_changed')
            parents, batches = {}, {}
            with closing(sqlite3.connect(Path(ref['state']).as_uri() + '?mode=ro&immutable=1', uri=True)) as witness:
                for _, batch, parent_id, parent_url, captured, _ in rows:
                    head = source.execute('SELECT url,provider,body_sha256,body_bytes,captured_at_utc,parquet_path '
                                          'FROM articles WHERE id=?', (bytes.fromhex(parent_id),)).fetchone()
                    if not head or head[0] != parent_url or head[1] != 'cmnews' or head[4] != captured or head[5] != batch:
                        raise ValueError('handoff_parent_head_changed')
                    previous = parents.setdefault(parent_id, list(head))
                    if previous != list(head):
                        raise ValueError('handoff_parent_witness_conflict')
                    if batch not in batches:
                        job = witness.execute('SELECT receipt_path,body_stamp,receipt_stamp FROM jobs '
                                              'WHERE path=? AND error IS NULL', (batch,)).fetchone()
                        if not job or source.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (batch,)).fetchone() != (job[0],):
                            raise ValueError('handoff_batch_registration_changed')
                        from scripts.audit_news_provider_storage_readback import source_file, stamp
                        if (stamp(source_file(root, batch)) != json.loads(job[1])
                                or stamp(source_file(root, job[0])) != json.loads(job[2])):
                            raise ValueError('handoff_batch_artifact_changed')
                        batches[batch] = {'receipt_path': job[0], 'body_stamp': json.loads(job[1]),
                                          'receipt_stamp': json.loads(job[2])}
            if (snapshot(root) != ref['snapshot'] or sha256_file(manifest_file) != manifest_hash
                    or sha256_file(readback_path) != readback_hash or sha256_file(checkpoint) != manifest['checkpoint_sha256']
                    or sha256_file(log) != independent['log_sha256']):
                raise ValueError('handoff_inputs_changed_during_sealing')
            check_reference(ref)
            certificate = {'contract': 'cmnews_retained_link_handoff_v1', 'source_root': str(root),
                           'source_snapshot': ref['snapshot'], 'reference_scope_sha256': ref['scope_sha256'],
                           'stage_manifest': {'path': str(manifest_file), 'sha256': manifest_hash},
                           'checkpoint': {'path': str(checkpoint), 'sha256': manifest['checkpoint_sha256']},
                           'independent_readback': {'path': str(readback_path), 'sha256': readback_hash},
                           'source_identity': manifest['source_identity'], 'candidate_count': len(rows),
                           'requests': requests, 'parents': parents, 'batches': batches,
                           'target_identity_or_body_verified': False}
            from downloader.artifact_io import atomic_write_bytes
            atomic_write_bytes(output, (json.dumps(certificate, ensure_ascii=False, sort_keys=True) + '\n').encode(),
                               durable=True, mode=0o600)
            return {'accepted': True, 'certificate_sha256': sha256_file(output), 'candidates': len(rows),
                    'parent_heads': len(parents), 'witness_batches': len(batches), 'requests': len(requests),
                    'publisher_requests': 0, 'production_queue_writes': 0, 'owner_admission': False}
    finally:
        os.close(owner)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--checkpoint', type=Path, required=True)
    parser.add_argument('--log', type=Path, required=True)
    parser.add_argument('--readback', type=Path, required=True)
    parser.add_argument('--stage-root', type=Path, required=True)
    parser.add_argument('--seal-existing', type=Path,
                        help='Seal an existing complete private stage, without submitting it')
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    args = parser.parse_args(argv)
    os.umask(0o077)
    _, registry, repo = load_config(args.config)
    if args.seal_existing:
        result = seal_handoff(args.root, args.checkpoint, args.log, args.readback, args.stage_root,
                              repo / 'artifacts/data_quality', registry, args.seal_existing)
    else:
        result = stage(args.root, args.checkpoint, args.log, args.readback, args.stage_root,
                       repo / 'artifacts/data_quality', registry)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
