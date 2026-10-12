"""Read-only, bounded acceptance of exact nonbody outcomes by one news owner.

No downloader, HTTP client, queue mutation, deadline reset or article acceptance.
An observation timeout is not producer failure or historical completion.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import json
from pathlib import Path
import sqlite3
import time

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import normalize_url
from downloader.news_fulltext_queue import utc_now
from scripts.audit_news_runtime import completion_owner_check
from scripts.watch_news_formal_articles import file_stats, pinned_files, read_heads, verify_owner
from scripts.verify_news_source_diagnostics import verify, verify_queue_outcomes


NONBODY_OUTCOMES = frozenset({'source_body_image_only', 'source_body_embedded_only',
    'source_body_quote_only', 'source_body_removed', 'source_article_empty_template',
    'source_not_found', 'source_document_landing_page', 'paywall_or_login'})


def expected_outcomes(payload):
    if not isinstance(payload, dict) or not 1 <= len(payload) <= 16:
        raise ValueError('one_to_sixteen_exact_nonbody_heads_required')
    result = {}
    for url, outcome in payload.items():
        if not isinstance(url, str) or url != normalize_url(url):
            raise ValueError('normalized_exact_article_url_required')
        if not isinstance(outcome, str) or outcome not in NONBODY_OUTCOMES:
            raise ValueError('explicit_verified_nonbody_outcome_required')
        result[url] = outcome
    return result


def ready_item(row, outcome, owner_started):
    if (not isinstance(row, dict) or not {'state', 'error_code', 'body_sha256',
            'lease_owner', 'lease_until', 'due', 'last_checked_at_utc'} <= row.keys()
            or row['state'] != outcome or row['error_code'] != outcome
            or row['body_sha256'] is not None or row['lease_owner'] is not None
            or row['lease_until'] is not None or type(row['due']) not in (int, float) or row['due'] != 0):
        return False
    try:
        stamp = datetime.fromisoformat(row['last_checked_at_utc'])
        return stamp.utcoffset() is not None and stamp.timestamp() >= owner_started
    except (ValueError, TypeError):
        return False


def ready(rows, expected, owner_started):
    return set(rows) == set(expected) and all(isinstance(rows[url], dict) and rows[url].get('url') == url
        and ready_item(rows[url], outcome, owner_started)
        for url, outcome in expected.items())


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--input', type=Path, required=True, help='Exact expected/probes source-evidence manifest.')
    parser.add_argument('--runtime-baseline', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--maximum-seconds', type=float, default=4500)
    parser.add_argument('--poll-seconds', type=float, default=10)
    args = parser.parse_args(argv)
    if not 1 <= args.maximum_seconds <= 43200 or not 1 <= args.poll_seconds <= 60:
        parser.error('bounded positive observation and polling periods required')
    intent_path = args.output.with_suffix('.intent.json')
    pending_path = args.output.with_suffix('.pending.json')
    if args.output.exists() or intent_path.exists():
        raise ValueError('existing_watch_intent_read_original_run_not_duplicate')
    if args.input.stat().st_size > 65536:
        raise ValueError('bounded_exact_nonbody_manifest_required')
    payload = json.loads(args.input.read_bytes())
    expected = expected_outcomes(payload['expected'])
    probes = payload['probes']
    if (not isinstance(probes, list) or len(probes) != len(expected)
            or any(not isinstance(probe, dict) for probe in probes)
            or {probe.get('url') for probe in probes} != set(expected)):
        raise ValueError('one_original_source_probe_per_exact_head_required')
    cfg, registry, repo = load_config(args.config)
    root = repo / cfg['root']
    sources = {registry.require(url).provider: registry.require(url) for url in expected}
    if any(not source.allowed for source in sources.values()):
        raise ValueError('allowed_source_scope_required')
    baseline = json.loads(args.runtime_baseline.read_bytes())
    fingerprints = {name: source.data_fingerprint for name, source in sources.items()}
    files = pinned_files(repo, baseline)
    for path in (Path(__file__).resolve(), Path(verify.__code__.co_filename).resolve(),
                 Path(completion_owner_check.__code__.co_filename).resolve(),
                 args.input.resolve(), args.runtime_baseline.resolve()):
        files[path] = sha256_file(path)
    stats = file_stats(files)
    started_epoch = baseline['owner_process_clock_check']['owner_metric_started_at_epoch']
    originals = verify(probes, registry)
    if ({row['url'] for row in originals['results'] if row.get('diagnostic_integrity_verified')
            and row.get('source_outcome_verified') and row.get('source_outcome') == expected.get(row['url'])}
            != set(expected)):
        raise ValueError('current_parser_original_integrity_and_expected_outcome_required')
    owner_proof = verify_owner(root, baseline)
    intent = {'contract': 'exact_normal_owner_nonbody_watch_v1', 'observed_at_utc': utc_now(),
        'owner_id': baseline['owner_id'], 'expected': expected, 'source_fingerprints': fingerprints,
        'maximum_observation_seconds': args.maximum_seconds, 'poll_seconds': args.poll_seconds,
        'pinned_files_sha256': {str(path): digest for path, digest in files.items()},
        'source_original_proof': originals, 'publisher_requests': 0, 'queue_writes': False,
        'retries_reset': False, 'new_article_bodies_claimed': 0,
        'observation_deadline_is_not_completion_eta': True, 'all_history_complete': False}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_json(intent_path, intent, durable=True)
    started = time.monotonic()
    next_heartbeat, previous = started, None
    while True:
        if file_stats(files) != stats:
            raise ValueError('pinned_producer_configuration_or_verifier_changed')
        now = time.monotonic()
        elapsed = now - started
        try:
            rows = read_heads(root, expected, baseline['owner_id'], fingerprints)
            observation = {'rows': rows, 'probe_error': None}
        except sqlite3.OperationalError as error:
            observation = {'rows': {}, 'probe_error': type(error).__name__}
        complete = ready(observation['rows'], expected, started_epoch)
        heartbeat = now >= next_heartbeat
        if complete or heartbeat:
            owner_proof = verify_owner(root, baseline)
            next_heartbeat = now + 60
        if complete or heartbeat or observation != previous or elapsed >= args.maximum_seconds:
            atomic_write_json(pending_path, {**intent, **observation, 'observed_at_utc': utc_now(),
                'elapsed_seconds': elapsed, 'owner_proof': owner_proof,
                'source_classifications_observed': sum(ready_item(row, expected[url], started_epoch)
                    for url, row in observation['rows'].items()),
                'accepted': False, 'state': 'waiting_for_normal_source_classification'}, durable=True)
            previous = observation
        if complete or elapsed >= args.maximum_seconds:
            if any(sha256_file(path) != digest for path, digest in files.items()):
                raise ValueError('pinned_producer_or_verifier_digest_changed')
            diagnostics = verify(probes, registry)
            queue = verify_queue_outcomes(diagnostics['results'], root / 'queue.sqlite3', registry)
            after = read_heads(root, expected, baseline['owner_id'], fingerprints)
            accepted = complete and queue['accepted'] and ready(after, expected, started_epoch)
            result = {**intent, 'observed_at_utc': utc_now(), 'elapsed_seconds': elapsed,
                'accepted': accepted, 'rows': after, 'state': 'accepted_source_classifications'
                    if accepted else 'observation_incomplete',
                'source_original_proof': diagnostics, 'exact_current_queue_verification': queue,
                'owner_proof': verify_owner(root, baseline), 'new_article_bodies': 0,
                'incomplete_observation_is_not_producer_failure': not complete}
            atomic_write_json(args.output, result, durable=True)
            print(json.dumps({'accepted': accepted, 'state': result['state'], 'owner_id': baseline['owner_id'],
                'new_article_bodies': 0, 'publisher_requests': 0}))
            return 0 if accepted or not complete else 1
        time.sleep(min(args.poll_seconds, args.maximum_seconds - elapsed))


if __name__ == '__main__':
    raise SystemExit(main())
