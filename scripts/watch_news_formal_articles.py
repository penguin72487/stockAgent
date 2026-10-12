"""Bounded read-only acceptance of exact articles from a pinned news owner.

Observe indexed heads, then reuse the canonical immutable-batch/source replay
auditor. This is not a downloader, a retry trigger or a new queue owner. A
deadline means observation ended, not that the producer failed or history is
complete. No publisher transport is created.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import re
import sqlite3
import time

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import normalize_url
from downloader.news_fulltext_queue import utc_now
from scripts.audit_news_runtime import completion_owner_check, service_snapshot
from scripts.audit_news_selected_articles import audit


def expected_heads(payload: dict) -> dict[str, str]:
    if not isinstance(payload, dict) or not 1 <= len(payload) <= 16:
        raise ValueError('one_to_sixteen_exact_expected_heads_required')
    result = {}
    for url, digest in payload.items():
        if not isinstance(url, str) or url != normalize_url(url):
            raise ValueError('normalized_exact_article_url_required')
        if not isinstance(digest, str) or not re.fullmatch(r'[0-9a-f]{64}', digest):
            raise ValueError('exact_expected_body_sha256_required')
        result[url] = digest
    return result


def read_heads(root: Path, expected: dict, owner: str, fingerprints: dict) -> dict:
    with sqlite3.connect(f'file:{(root / "queue.sqlite3").resolve()}?mode=ro', uri=True, timeout=2) as db:
        db.row_factory = sqlite3.Row
        db.execute('BEGIN')
        current = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()
        if not current or current[0] != owner:
            raise ValueError('observed_queue_owner_changed')
        for provider, fingerprint in fingerprints.items():
            loaded = db.execute('SELECT value FROM queue_settings WHERE key=?',
                                (f'source_capture_fingerprint:{provider}',)).fetchone()
            if not loaded or loaded[0] != fingerprint:
                raise ValueError('observed_capture_contract_changed')
        rows = {}
        for url in expected:
            row = db.execute('SELECT url,state,error_code,attempts,due,body_sha256,parquet_path,'
                'captured_at_utc,last_checked_at_utc,lease_owner,lease_until FROM articles WHERE url=?', (url,)).fetchone()
            rows[url] = dict(row) if row else None
        return rows


def heads_ready(rows: dict, expected: dict, owner_started: float) -> bool:
    if set(rows) != set(expected):
        return False
    for url, digest in expected.items():
        row = rows[url]
        if not row or row['state'] != 'complete' or row['body_sha256'] != digest or row['lease_owner']:
            return False
        try:
            for key in ('captured_at_utc', 'last_checked_at_utc'):
                stamp = datetime.fromisoformat(row[key])
                if stamp.utcoffset() is None or stamp.timestamp() < owner_started:
                    return False
        except (TypeError, ValueError):
            return False
    return True


def pinned_files(repo: Path, baseline: dict) -> dict[Path, str]:
    required = ('status_database_owner_consistent', 'loaded_provider_policy_matches_config',
                'source_files_unchanged_since_owner_start', 'metrics_snapshot_clock_verified')
    if not all(baseline.get(key) is True for key in required):
        raise ValueError('accepted_coherent_runtime_baseline_required')
    if not baseline.get('owner_process_clock_check', {}).get('accepted'):
        raise ValueError('baseline_process_clock_not_verified')
    if not baseline.get('owner_id') or baseline['owner_id'] != baseline.get('status_owner_id'):
        raise ValueError('baseline_owner_not_coherent')
    files = {}
    for key in ('source_code_sha256', 'configuration_files_sha256'):
        if not baseline.get(key):
            raise ValueError('baseline_source_and_configuration_hashes_required')
        for name, digest in baseline[key].items():
            path = (repo / name).resolve()
            if not path.is_relative_to(repo.resolve()) or sha256_file(path) != digest:
                raise ValueError('baseline_pinned_file_changed')
            files[path] = digest
    # Acceptance code is not producer code; pin its current generation too.
    for path in (Path(__file__).resolve(), Path(audit.__code__.co_filename).resolve()):
        files[path] = sha256_file(path)
    return files


def file_stats(files: dict) -> dict:
    return {path: (path.stat().st_size, path.stat().st_mtime_ns) for path in files}


def verify_owner(root: Path, baseline: dict) -> dict:
    status = json.loads((root / 'status.json').read_bytes())
    proof = completion_owner_check(baseline['owner_id'], baseline['service'],
        {'owner_id': baseline['owner_id']}, service_snapshot(), status, utc_now())
    if not proof['accepted']:
        raise ValueError('pinned_owner_process_or_fresh_heartbeat_not_verified')
    return proof


def watch(root: Path, registry, repo: Path, baseline: dict, expected: dict, output: Path,
          *, maximum_seconds: float, poll_seconds: float) -> dict:
    expected = expected_heads(expected)
    if not 60 <= maximum_seconds <= 7200 or not 1 <= poll_seconds <= 60:
        raise ValueError('bounded_observation_and_poll_interval_required')
    if output.exists():
        raise ValueError('immutable_formal_observation_output_already_exists')
    files = pinned_files(repo, baseline)
    stats = file_stats(files)
    fingerprints = {registry.require(url).provider: registry.require(url).data_fingerprint for url in expected}
    owner_proof = verify_owner(root, baseline)
    owner_started = baseline['owner_process_clock_check']['owner_metric_started_at_epoch']
    started = time.monotonic()
    heartbeat, previous = started, None
    progress_path = output.with_suffix('.pending.json')
    basis = {'contract_version': 1, 'owner_id': baseline['owner_id'],
        'expected_heads': expected, 'expected_source_fingerprints': fingerprints,
        'pinned_files_sha256': {str(path.relative_to(repo)): digest for path, digest in files.items()},
        'publisher_requests': 0, 'queue_writes': False, 'all_history_complete': False,
        'scope': 'exact normal-forward heads under a single pinned owner, not source history'}
    while True:
        if file_stats(files) != stats:
            raise ValueError('observed_source_configuration_or_verifier_changed')
        now = time.monotonic()
        elapsed = now - started
        try:
            rows = read_heads(root, expected, baseline['owner_id'], fingerprints)
            observation = {'rows': rows, 'probe_error': None}
        except sqlite3.OperationalError as error:
            # Read-side contention never resets a producer or its retry clock.
            observation = {'rows': {}, 'probe_error': type(error).__name__ + ': ' + str(error)[:160]}
        ready = heads_ready(observation['rows'], expected, owner_started)
        heartbeat_due = now >= heartbeat
        if ready or heartbeat_due:
            owner_proof = verify_owner(root, baseline)
            heartbeat = now + 60
        if observation != previous or heartbeat_due or ready or elapsed >= maximum_seconds:
            atomic_write_json(progress_path, {**basis, **observation, 'owner_proof': owner_proof,
                'observed_at_utc': utc_now(), 'elapsed_seconds': elapsed, 'maximum_seconds': maximum_seconds,
                'state': 'waiting_for_normal_forward_heads', 'accepted': False}, durable=True)
            previous = observation
        if ready:
            if any(sha256_file(path) != digest for path, digest in files.items()):
                raise ValueError('observed_pinned_file_bytes_changed')
            proof = audit(root, list(expected), registry=registry)
            hashes_match = len(proof['articles']) == len(expected) and all(
                item['body_sha256'] == expected[item['url']]
                and item.get('capture_contract_matches_current_source') is True
                for item in proof['articles'])
            owner_proof = verify_owner(root, baseline)
            if any(sha256_file(path) != digest for path, digest in files.items()):
                raise ValueError('observed_pinned_file_bytes_changed_during_acceptance')
            accepted = bool(proof['accepted'] and hashes_match)
            final = {**basis, 'observed_at_utc': utc_now(), 'elapsed_seconds': elapsed,
                'maximum_seconds': maximum_seconds, 'owner_proof': owner_proof,
                'state': 'formal_article_verified' if accepted else 'formal_article_verification_rejected',
                'accepted': accepted, 'article_proof': proof}
            atomic_write_json(output, final, durable=True)
            atomic_write_json(progress_path, final, durable=True)
            return final
        if elapsed >= maximum_seconds:
            final = {**basis, **observation, 'observed_at_utc': utc_now(), 'elapsed_seconds': elapsed,
                'maximum_seconds': maximum_seconds, 'owner_proof': owner_proof,
                'state': 'observation_deadline_reached', 'accepted': False, 'producer_failed': False}
            atomic_write_json(output, final, durable=True)
            atomic_write_json(progress_path, final, durable=True)
            return final
        time.sleep(min(poll_seconds, maximum_seconds - elapsed))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--expected-heads', type=Path, required=True, help='JSON object: exact normalized URL to expected original body SHA256')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--maximum-seconds', type=float, default=5400)
    parser.add_argument('--poll-seconds', type=float, default=20)
    args = parser.parse_args(argv)
    if args.expected_heads.stat().st_size > 65536:
        parser.error('bounded expected-heads file required')
    config, registry, repo = load_config(args.config)
    result = watch(repo / config['root'], registry, repo, json.loads(args.baseline.read_bytes()),
        json.loads(args.expected_heads.read_bytes()), args.output,
        maximum_seconds=args.maximum_seconds, poll_seconds=args.poll_seconds)
    print(json.dumps({key: result[key] for key in ('state', 'accepted', 'elapsed_seconds', 'owner_id', 'publisher_requests', 'queue_writes', 'all_history_complete')}))
    return 0 if result['accepted'] else 2


if __name__ == '__main__':
    raise SystemExit(main())
