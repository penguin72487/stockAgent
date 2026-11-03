"""Run and resume bounded CNYES original audits from durable local reports.

This is an offline audit driver. It never starts a collector or writes its queue.
Each chunk remains a separate proof and is not a full-history completion claim.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import sys

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from downloader.artifact_io import atomic_write_bytes, sha256_file
from downloader.news_fulltext_policy import cnyes_bulk_fingerprint
from downloader.news_fulltext_queue import source_path
from scripts import audit_news_cnyes_retained_missing as audit


_REPORT_NAME = re.compile(r'chunk-([0-9]{6})\.json')
# Only the exact previously reviewed reader may be inspected after a local
# scheduler-only update. This exception is readback-only, never a resume gate.
_REVIEWED_HISTORICAL_PARSER = {
    'scripts/audit_news_cnyes_retained_missing.py': '6826846fe35b96e3dc4de745b8e8d986e370ee2e613ea15ffb2b583f5708c850',
    'scripts/audit_news_cached_replay.py': '23792105265d5543a2a18dc4eafc142755cba2f29b110b450519274c57b37124',
    'downloader/news_fulltext_bulk.py': '62c0b83cf936819e71f5bae731fbde808f81c75bf1f5bf855fc82485dfae0da8',
    'downloader/news_fulltext_cnyes_history.py': 'c4746be93374616e0275da94fed1151488256797d7644ca807b84e988adb2ce9',
    'downloader/news_fulltext_policy.py': 'd4ee857f04aec54c3e17299a3ddd7cde0d492e11d8f65ccc6c4dbafcd62f26f1',
}


def _read_json(path):
    before = audit._stamp(path)
    if before is None or before[2] > 128 << 20:
        raise ValueError('scan_input_missing_or_oversized')
    data = path.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    if before != audit._stamp(path) or digest != sha256_file(path):
        raise ValueError('scan_input_changed_during_read')
    return json.loads(data), digest


def _snapshot(root):
    paths = (root / 'queue.sqlite3', root / 'queue.sqlite3-wal', root / 'queue.sqlite3-journal')
    stamps = [audit._stamp(path) for path in paths]
    if stamps[0] is None or any(stamp is not None and stamp[2] for stamp in stamps[1:]):
        raise ValueError('immutable_snapshot_requires_empty_wal_and_journal')
    return [list(stamp) if stamp is not None else None for stamp in stamps]


def _equivalent_empty_sidecars(previous, current):
    """Empty WAL/journal identities may change while the database stays identical."""
    def empty(stamp):
        return stamp is None or (type(stamp) is list and len(stamp) == 5
                                 and type(stamp[2]) is int and stamp[2] == 0)
    return (type(previous) is list and type(current) is list and len(previous) == len(current) == 3
            and previous[0] is not None and previous[0] == current[0]
            and all(empty(stamp) for stamp in previous[1:] + current[1:]))


def _new_state():
    return {'reports': 0, 'observation_keys_scanned': 0, 'source_envelope_failures': 0,
            'request_mismatches': 0, 'other_observation_anomalies': 0, 'last_start': None,
            'last_path': None, 'last_report_sha256': None, 'previous_report_sha256': None,
            'last_chunk_size': None, 'transition': None, 'finished': False}


def _accept_report(state, path, *, root, snapshot, fingerprint, parser_identity, prior_digest, prior_shas):
    match = _REPORT_NAME.fullmatch(path.name)
    if match is None or int(match.group(1)) != state['reports'] + 1:
        raise ValueError('scan_report_gap')
    report, digest = _read_json(path)
    if (report.get('publisher_http_requests') != 0 or report.get('production_queue_writes') != 0
            or report.get('article_bodies_copied') != 0 or report.get('all_history_complete') is not False):
        raise ValueError('scan_report_scope_invalid')
    if report.get('excluded_prior_audit_sha256') != prior_shas:
        raise ValueError('scan_input_prior_changed')
    if report.get('resumed_scan_audit_sha256') != state['last_report_sha256']:
        raise ValueError('scan_report_chain_changed')
    scan = report.get('observation_scan', {})
    if not state['reports']:
        if scan.get('start_after') is not None:
            raise ValueError('scan_report_chain_changed')
    else:
        transition = state['transition']
        if transition is None:
            raise ValueError('scan_report_after_finish')
        start, offset, candidate_digest, old_failures, old_mismatches = transition
        if (scan.get('start_after') != start or type(scan.get('candidate_offset')) is not int
                or scan['candidate_offset'] < offset
                or candidate_digest is not None and scan['candidate_offset'] <= offset
                or candidate_digest is not None and scan.get('candidate_digest') != candidate_digest
                or candidate_digest is not None and scan.get('chunk_size') != state['last_chunk_size']
                or scan.get('chunk_response_failures') != old_failures + len(report.get('response_failures', []))
                or scan.get('chunk_request_mismatches') != old_mismatches + len(report.get('request_mismatches', []))):
            raise ValueError('scan_report_chain_changed')
    try:
        transition = audit._resume_scan(report, root=root, snapshot=snapshot, fingerprint=fingerprint,
            parser_identity=parser_identity, prior_digest=prior_digest, chunk_size=scan.get('chunk_size'))
    except ValueError as error:
        if str(error) != 'scan_already_finished':
            raise
        transition = None
    if not state['reports'] or scan['start_after'] != state['last_start']:
        state['observation_keys_scanned'] += scan['observations_scanned']
        state['other_observation_anomalies'] += sum(report.get(key, 0) for key in ('invalid_matching_observations',
            'noncanonical_matching_observations', 'other_authorization_matching_observations'))
    state['source_envelope_failures'] += len(report['response_failures'])
    state['request_mismatches'] += len(report['request_mismatches'])
    state.update(reports=state['reports'] + 1, last_start=scan['start_after'], last_path=path,
                 previous_report_sha256=state['last_report_sha256'], last_report_sha256=digest,
                 last_chunk_size=scan['chunk_size'], transition=transition, finished=transition is None)


def _status(state):
    issues = bool(state['source_envelope_failures'] or state['request_mismatches'] or state['other_observation_anomalies'])
    result = ('scan_finished_with_issues' if issues else 'scan_finished_no_reported_issues') if state['finished'] else 'continuation_available'
    return {'contract': 'cnyes_retained_observation_scan_chain_status_v1', 'state': result,
        'reports': state['reports'], 'observation_keys_scanned': state['observation_keys_scanned'],
        'source_envelope_failures': state['source_envelope_failures'], 'request_mismatches': state['request_mismatches'],
        'other_observation_anomalies': state['other_observation_anomalies'],
        'last_report_sha256': state['last_report_sha256'], 'publisher_requests': 0,
        'production_queue_writes': 0, 'all_history_complete': False,
        'scope': 'offline observation chunks; source failures and each proof remain in separate reports'}


def _source_only_witness(root, artifact, items, target_count):
    """Reuse a same-report request mismatch without promoting its source-only bodies.

    Older source-only results include the verified Parquet digest but not the
    old file signatures. Check the present bytes against that digest and the
    present receipt against the same file and response. This proves the
    mismatch diagnosis only; the receipt is not claimed unchanged since audit.
    """
    if artifact[2] != 'canonical_quarantine' or not items:
        return None
    try:
        ids = [item['news_id'] for item in items]
        requests = {item['source_request_sha256'] for item in items}
        parquet_shas = {item['parquet_sha256'] for item in items}
        receipts = {item['source_receipt_path'] for item in items}
        if (any(type(item) is not int or item <= 0 for item in ids)
                or len(ids) != len(set(ids)) or len(ids) != target_count
                or len(requests) != 1 or len(parquet_shas) != 1
                or any(not isinstance(value, str) or not re.fullmatch(r'[0-9a-f]{64}', value)
                       for value in requests | parquet_shas)
                or receipts != {artifact[1] + '.receipt.json'}):
            return None
        original = source_path(root, artifact[1])
        receipt_path = source_path(root, artifact[1] + '.receipt.json')
        original_sig, receipt_sig = audit._stamp(original), audit._stamp(receipt_path)
        if original_sig[2] > 16 << 20 or receipt_sig[2] > 65536:
            return None
        receipt = json.loads(receipt_path.read_bytes())
        expected_sha = next(iter(parquet_shas))
        actual_sha = sha256_file(original)
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return None
    if (audit._stamp(original), audit._stamp(receipt_path)) != (original_sig, receipt_sig):
        raise ValueError('scan_original_changed_during_plan')
    if (not isinstance(receipt, dict) or actual_sha != expected_sha
            or receipt.get('sha256') != expected_sha or receipt.get('response_sha256') != artifact[0]
            or receipt.get('parquet_path') != artifact[1] or receipt.get('publish') is not False
            or type(receipt.get('article_count')) is not int or type(receipt.get('accepted_body_count')) is not int
            or not 0 <= receipt['accepted_body_count'] <= receipt['article_count']):
        return None
    return target_count, next(iter(requests)), original_sig, receipt_sig, frozenset(ids)


def _plan_unchanged_mismatches(root, registry, prior_paths, priors, prior_shas,
                               report_paths, state, snapshot, fingerprint, parser_identity, *,
                               report_snapshot=None, prior_parser_identity=None):
    """Find already-diagnosed candidates without replaying publisher originals.

    This is a snapshot-bound plan, not an adoption or a resolution. Reuse is
    limited to mismatches with a same-report witness. Exact-request witnesses
    need unchanged original/receipt signatures; older source-only witnesses
    need the original content hash and a currently valid receipt binding.
    """
    source = registry.sources['cnyes']
    if report_snapshot is None:
        report_snapshot = snapshot
    if prior_parser_identity is None:
        prior_parser_identity = parser_identity
    db_path = root / 'queue.sqlite3'
    report_inputs, witnesses, conflicts = [], {}, set()
    previous_digest = None
    with sqlite3.connect(db_path.as_uri() + '?mode=ro&immutable=1', uri=True) as db:
        db.row_factory = sqlite3.Row
        db.execute('PRAGMA query_only=ON')
        pairs = audit._prior_pairs(priors, root=root, fingerprint=fingerprint,
                                   parser_identity=prior_parser_identity, db=db)
        for path in report_paths:
            report, digest = _read_json(path)
            stamp = audit._stamp(path)
            if (report.get('resumed_scan_audit_sha256') != previous_digest
                    or report.get('parser_identity') != parser_identity
                    or report.get('source_fingerprint') != fingerprint
                    or report.get('observation_scan', {}).get('snapshot') != report_snapshot):
                raise ValueError('scan_report_chain_changed')
            previous_digest = digest
            report_inputs.append((path, stamp, digest))
            pairs.update(audit._prior_pairs(report, root=root, fingerprint=fingerprint,
                                            parser_identity=parser_identity, db=db))
            signatures = defaultdict(set)
            for item in report.get('target_outcomes', []):
                if item.get('evidence_state') != 'verified' or item.get('original_request_identity_checked') is not True:
                    continue
                try:
                    artifact = (item['source_response_sha256'], item['source_artifact_path'],
                                item['source_artifact_kind'])
                    actual_request = item['source_request_sha256']
                    file_signature = tuple(item['source_file_signature'])
                    receipt_signature = tuple(item['source_receipt_signature'])
                    receipt = item['source_receipt_path']
                    if (artifact[2] not in {'canonical_quarantine', 'cnyes_history_source_envelope'}
                            or not re.fullmatch(r'[0-9a-f]{64}', actual_request)
                            or len(file_signature) != 5 or len(receipt_signature) != 5
                            or receipt != artifact[1] + '.receipt.json'):
                        continue
                    signatures[artifact].add((receipt, file_signature, receipt_signature, actual_request))
                except (KeyError, TypeError, ValueError):
                    continue
            source_only = defaultdict(list)
            for item in report.get('source_only_target_outcomes', []):
                try:
                    artifact = (item['source_response_sha256'], item['source_artifact_path'],
                                item['source_artifact_kind'])
                    observations = item['mismatched_observation_sha256']
                    if (item['evidence_state'] != 'verified_raw_source_only'
                            or item['observation_reference_verified'] is not False
                            or type(observations) is not list or len(observations) != len(set(observations))
                            or any(not isinstance(value, str) or not re.fullmatch(r'[0-9a-f]{64}', value)
                                   for value in observations)):
                        continue
                    for observation in observations:
                        source_only[(artifact, observation)].append(item)
                except (KeyError, TypeError, ValueError):
                    continue
            for item in report.get('request_mismatches', []):
                try:
                    artifact = (item['source_response_sha256'], item['source_artifact_path'],
                                item['source_artifact_kind'])
                    choices = signatures[artifact]
                    if type(item['target_count']) is not int or item['target_count'] < 1:
                        continue
                    if choices:
                        if len(choices) != 1:
                            continue
                        receipt, original_sig, receipt_sig, actual_request = next(iter(choices))
                        if (audit._stamp(source_path(root, artifact[1])) != original_sig
                                or audit._stamp(source_path(root, receipt)) != receipt_sig):
                            continue
                        proof = (item['target_count'], actual_request, original_sig, receipt_sig, None)
                    else:
                        receipt = artifact[1] + '.receipt.json'
                        proof = _source_only_witness(root, artifact,
                            source_only[(artifact, item['observation_sha256'])], item['target_count'])
                        if proof is None:
                            continue
                    key = artifact + (receipt, item['observation_sha256'])
                    previous = witnesses.get(key)
                    if previous:
                        earlier = previous[0]
                        if (earlier[:4] != proof[:4] or earlier[4] is not None and proof[4] is not None
                                and earlier[4] != proof[4]):
                            conflicts.add(key)
                        elif earlier[4] is None and proof[4] is not None:
                            witnesses[key] = (proof, previous[1])
                    witnesses.setdefault(key, (proof, set()))[1].add((str(path), digest))
                except (OSError, KeyError, TypeError, ValueError):
                    continue
        if previous_digest != state['last_report_sha256']:
            raise ValueError('scan_report_chain_changed')
        for key in conflicts:
            witnesses.pop(key, None)

        groups, anomaly_counts = {}, defaultdict(int)
        start_after = None
        while True:
            _, candidates, counts, _, scan = audit._candidate_index(
                db, pairs, source, observation_chunk_size=100_000, start_after=start_after)
            for name in ('invalid_matching_observations', 'noncanonical_matching_observations',
                         'other_authorization_matching_observations'):
                anomaly_counts[name] += counts[name]
            for key, page in candidates.items():
                group = groups.setdefault(key, {'reusable': True, 'observations': set(), 'proofs': set(),
                                                'witness_types': set()})
                observed, request_hashes = defaultdict(set), {}
                for request_url, request in page['requests'].items():
                    request_hash = hashlib.sha256(request_url.encode()).hexdigest()
                    for news_id, observations in request['observations'].items():
                        for observation in observations:
                            observed[observation].add(news_id)
                            if observation in request_hashes and request_hashes[observation] != request_hash:
                                raise ValueError('scan_observation_request_conflict')
                            request_hashes[observation] = request_hash
                if not observed or key[2] not in {'canonical_quarantine', 'cnyes_history_source_envelope'}:
                    group['reusable'] = False
                for observation, ids in observed.items():
                    group['observations'].add(observation)
                    witness = witnesses.get(key[:4] + (observation,))
                    if (not witness or witness[0][0] != len(ids) or witness[0][1] == request_hashes[observation]
                            or witness[0][4] is not None and witness[0][4] != frozenset(ids)):
                        group['reusable'] = False
                    else:
                        group['proofs'].update(witness[1])
                        group['witness_types'].add('same_report_source_only' if witness[0][4] is not None
                                                   else 'same_report_exact_request')
            if not scan['has_more']:
                break
            start_after = scan['last_scanned_key']

        # A retained proof whose original changes while observations are being
        # scanned must not make its target disappear from the pending plan.
        rechecked_pairs = audit._prior_pairs(priors, root=root, fingerprint=fingerprint,
                                             parser_identity=prior_parser_identity, db=db)
        for path in report_paths:
            report, _ = _read_json(path)
            rechecked_pairs.update(audit._prior_pairs(report, root=root, fingerprint=fingerprint,
                                                      parser_identity=parser_identity, db=db))
        if rechecked_pairs != pairs:
            raise ValueError('scan_prior_proofs_changed_during_plan')

    if _snapshot(root) != snapshot:
        raise ValueError('scan_snapshot_changed')
    for path, stamp, digest in report_inputs:
        if audit._stamp(path) != stamp or sha256_file(path) != digest:
            raise ValueError('scan_report_changed_during_plan')
    for path, digest in zip(prior_paths, prior_shas):
        if _read_json(Path(path))[1] != digest:
            raise ValueError('scan_input_prior_changed')

    reusable, needs_evidence = [], []
    for key, group in sorted(groups.items()):
        item = {'response_sha256': key[0], 'source_artifact_path': key[1],
                'source_artifact_kind': key[2], 'observation_sha256': sorted(group['observations'])}
        if group['reusable']:
            for observation in group['observations']:
                proof = witnesses[key[:4] + (observation,)][0]
                if (audit._stamp(source_path(root, key[1])) != proof[2]
                        or audit._stamp(source_path(root, key[3])) != proof[3]):
                    raise ValueError('scan_original_changed_during_plan')
            item['proof_reports'] = [{'path': path, 'sha256': digest}
                                     for path, digest in sorted(group['proofs'])]
            item['witness_types'] = sorted(group['witness_types'])
            reusable.append(item)
        else:
            needs_evidence.append(item)
    pending_shas = sorted({item['response_sha256'] for item in needs_evidence})
    pending_set = set(pending_shas)
    return {'contract': 'cnyes_unchanged_mismatch_plan_v2',
            'state': 'unresolved_evidence_plan', 'all_history_complete': False,
            'source_snapshot': snapshot, 'source_fingerprint': fingerprint,
            'parser_identity': parser_identity, 'prior_exact_pairs': len(pairs),
            'candidate_envelopes': len(groups), 'unchanged_mismatch_envelopes': len(reusable),
            'needs_evidence_envelopes': len(needs_evidence), 'pending_response_sha256': pending_shas,
            'unchanged_sharing_pending_response_sha': sum(item['response_sha256'] in pending_set for item in reusable),
            'unclassifiable_observations': dict(anomaly_counts),
            'unchanged_mismatches': reusable, 'needs_evidence': needs_evidence,
            'publisher_requests': 0, 'production_queue_writes': 0, 'original_contents_redecoded': 0,
            'scope': 'unchanged mismatch evidence remains unresolved; source-only outcomes stay in referenced reports'}


def run_chain(config_path, prior_paths, output_dir, *, observation_chunk_size=500,
              max_responses=2000, max_seconds=300, max_reports=0, check_only=False, plan_mismatches=False):
    prior_paths = list(dict.fromkeys(Path(path) for path in prior_paths))
    if (not prior_paths or len(prior_paths) > 16 or type(observation_chunk_size) is not int
            or not 1 <= observation_chunk_size <= 100_000 or not 1 <= max_responses <= 10_000
            or not 1 <= max_seconds <= 900 or type(max_reports) is not int or max_reports < 0
            or plan_mismatches and not check_only):
        raise ValueError('invalid_scan_chain_bounds')
    config, registry, repo = audit.load_config(config_path)
    root = (repo / config['root']).resolve()
    output_dir = output_dir.resolve()
    if output_dir.is_relative_to(root):
        raise ValueError('scan_output_inside_source')
    if check_only and not output_dir.is_dir():
        raise ValueError('scan_output_not_found')
    if not check_only:
        output_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
    lock_path = output_dir / '.scan.lock'
    flags = (os.O_RDONLY if check_only else os.O_RDWR | os.O_CREAT) | getattr(os, 'O_NOFOLLOW', 0)
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except FileNotFoundError as error:
        raise ValueError('scan_lock_not_found') from error
    owner_descriptor = None
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError('scan_already_running') from error
        priors, prior_shas = [], []
        for path in prior_paths:
            value, digest = _read_json(Path(path))
            priors.append(value)
            prior_shas.append(digest)
        snapshot = _snapshot(root)
        fingerprint = cnyes_bulk_fingerprint(registry.sources['cnyes'])
        parser_identity = {path: sha256_file(repo / path) for path in audit._PARSER_PATHS}
        current_parser_identity = parser_identity
        report_paths = sorted(output_dir.glob('chunk-*.json'))
        historical_readback = False
        changed_empty_sidecars = False
        if check_only and report_paths and audit._reviewed_current_reader(parser_identity):
            first_report, _ = _read_json(report_paths[0])
            if first_report.get('parser_identity') == _REVIEWED_HISTORICAL_PARSER:
                # The old identity is used solely to verify the immutable
                # chain and snapshot. It is never used to generate a report.
                try:
                    owner_descriptor = os.open(root / 'owner.lock', os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
                    fcntl.flock(owner_descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError as error:
                    raise ValueError('scan_source_owner_busy') from error
                except FileNotFoundError as error:
                    raise ValueError('scan_source_owner_lock_missing') from error
                if _snapshot(root) != snapshot:
                    raise ValueError('scan_snapshot_changed')
                first_scan = first_report.get('observation_scan')
                if not isinstance(first_scan, dict):
                    raise ValueError('scan_checkpoint_invalid')
                old_snapshot = first_scan.get('snapshot')
                if old_snapshot != snapshot:
                    if not _equivalent_empty_sidecars(old_snapshot, snapshot):
                        raise ValueError('scan_snapshot_changed')
                    changed_empty_sidecars = True
                parser_identity = _REVIEWED_HISTORICAL_PARSER
                historical_readback = True
        kwargs = {'root': root, 'snapshot': snapshot, 'fingerprint': fingerprint,
                  'parser_identity': parser_identity, 'prior_digest': audit._json_digest(priors),
                  'prior_shas': prior_shas}
        if historical_readback and changed_empty_sidecars:
            kwargs['snapshot'] = old_snapshot
        state = _new_state()
        for path in report_paths:
            _accept_report(state, path, **kwargs)
        status_path = output_dir / 'status.json'
        if status_path.exists():
            old, _ = _read_json(status_path)
            if old.get('contract') != 'cnyes_retained_observation_scan_chain_status_v1':
                raise ValueError('scan_status_invalid')
            old_count = old.get('reports')
            if type(old_count) is not int or old_count > state['reports'] or old_count < max(0, state['reports'] - 1):
                raise ValueError('scan_report_gap')
            if old_count == state['reports'] and old.get('last_report_sha256') != state['last_report_sha256']:
                raise ValueError('scan_status_digest_mismatch')
            if old_count == state['reports'] - 1 and old.get('last_report_sha256') != state['previous_report_sha256']:
                raise ValueError('scan_status_digest_mismatch')
        if check_only:
            if plan_mismatches:
                if not state['finished']:
                    raise ValueError('scan_not_finished')
                result = _plan_unchanged_mismatches(root, registry, prior_paths, priors, prior_shas,
                    report_paths, state, snapshot, fingerprint, parser_identity,
                    report_snapshot=old_snapshot if historical_readback else None,
                    prior_parser_identity=current_parser_identity if historical_readback else None)
            else:
                result = _status(state)
            if historical_readback:
                if _snapshot(root) != snapshot:
                    raise ValueError('scan_snapshot_changed')
                if current_parser_identity != {path: sha256_file(repo / path) for path in audit._PARSER_PATHS}:
                    raise ValueError('scan_reader_changed')
                result.update(historical_reader_readback=True, resumable_with_current_reader=False,
                              empty_sidecar_identity_changed=changed_empty_sidecars)
                if not state['finished']:
                    result['state'] = 'historical_partial_read_only'
            return result
        made = 0
        if (not state['finished'] and state['transition'] is not None and state['transition'][2] is not None
                and state['last_chunk_size'] != observation_chunk_size):
            raise ValueError('scan_scope_changed')
        while not state['finished'] and (not max_reports or made < max_reports):
            if state['reports'] >= 999_999:
                raise ValueError('scan_report_name_exhausted')
            target = output_dir / f"chunk-{state['reports'] + 1:06d}.json"
            args = ['--config', str(config_path), '--output', str(target),
                    '--observation-chunk-size', str(observation_chunk_size),
                    '--max-responses', str(max_responses), '--max-seconds', str(max_seconds)]
            for path in prior_paths:
                args.extend(('--exclude-audit', str(path)))
            if state['last_path'] is not None:
                args.extend(('--resume-scan', str(state['last_path'])))
            audit.main(args)
            _accept_report(state, target, **kwargs)
            atomic_write_bytes(status_path, (json.dumps(_status(state), ensure_ascii=False, indent=2) + '\n').encode(),
                               durable=True, mode=0o600)
            made += 1
        status = _status(state)
        atomic_write_bytes(status_path, (json.dumps(status, ensure_ascii=False, indent=2) + '\n').encode(),
                           durable=True, mode=0o600)
        return status
    finally:
        if owner_descriptor is not None:
            os.close(owner_descriptor)
        os.close(descriptor)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--exclude-audit', type=Path, action='append', required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--observation-chunk-size', type=int, default=500)
    parser.add_argument('--max-responses', type=int, default=2000)
    parser.add_argument('--max-seconds', type=int, default=300)
    parser.add_argument('--max-reports', type=int, default=0, help='Maximum new reports in this run; 0 continues to the end')
    parser.add_argument('--check-only', action='store_true', help='Validate existing reports without scanning another chunk')
    parser.add_argument('--plan-mismatches', action='store_true',
                        help='With --check-only, plan reuse of unchanged mismatch evidence; does not resolve it')
    args = parser.parse_args(argv)
    status = run_chain(args.config, args.exclude_audit, args.output_dir,
        observation_chunk_size=args.observation_chunk_size, max_responses=args.max_responses,
        max_seconds=args.max_seconds, max_reports=args.max_reports, check_only=args.check_only,
        plan_mismatches=args.plan_mismatches)
    print(json.dumps(status, ensure_ascii=False))
    return 1 if status['state'] == 'scan_finished_with_issues' else 0


if __name__ == '__main__':
    raise SystemExit(main())
