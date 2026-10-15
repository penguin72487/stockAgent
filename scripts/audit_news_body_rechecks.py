"""Read-only proof of the bounded Cnyes unknown-body recovery contract.

Inspect the original repair observations and one current WAL snapshot. A
present body hash is a head observation, NOT immutable body acceptance; use
audit_news_selected_articles/verify_news_recent_batches for that separate proof.
No publisher requests, writer lock, repairs, author names or article text.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import json
import hashlib
from pathlib import Path
import sqlite3

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import load_config
from downloader.news_fulltext_queue import (NewsQueue, CNYES_UNKNOWN_BODY_RECHECK_CONTRACT_VERSION,
                                           CNYES_UNKNOWN_BODY_RECHECK_SECONDS, utc_now)
from stockagent.data.news_source_catalog import configuration_paths


def audit(root, registry):
    status = json.loads((root / 'status.json').read_text())
    source = registry.sources['cnyes']
    prefix = f'unknown_cnyes_body_rechecks_v{CNYES_UNKNOWN_BODY_RECHECK_CONTRACT_VERSION}:'
    marker = prefix + source.data_fingerprint
    db = sqlite3.connect(f'file:{(root / "queue.sqlite3").resolve()}?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN')
        owner = db.execute("SELECT value FROM queue_settings WHERE key='current_owner_id'").fetchone()
        loaded = db.execute("SELECT value FROM queue_settings WHERE key='source_capture_fingerprint:cnyes'").fetchone()
        # A new parser fingerprint usually creates an empty migration journal:
        # the old failures are already retry rows. Keep the original cohort
        # across retained journals of this repair ABI instead of reporting 0.
        # Index only the small settings namespace, never the article frontier.
        proofs = db.execute('SELECT key,value FROM queue_settings WHERE key>=? AND key<? ORDER BY key LIMIT 17',
                            (prefix, prefix + '\uffff')).fetchall()
        if len(proofs) > 16:
            raise ValueError('bounded_original_recheck_journal_set_required')
        journals, originals, errors = [], {}, []
        journal_match = any(proof['key'] == marker for proof in proofs)
        for proof in proofs:
            journal = json.loads(proof['value'])
            observations = journal.get('rows', [])
            if not isinstance(observations, list) or len(observations) > 512:
                raise ValueError('bounded_original_recheck_cohort_required')
            valid = (journal.get('contract_version') == CNYES_UNKNOWN_BODY_RECHECK_CONTRACT_VERSION
                     and proof['key'] == prefix + str(journal.get('source_fingerprint'))
                     and journal.get('accepted_as_bodies') is False
                     and journal.get('original_error_code') == 'body_not_found'
                     and journal.get('target_state') == 'retry_cooldown'
                     and journal.get('recheck_minimum_seconds') == CNYES_UNKNOWN_BODY_RECHECK_SECONDS
                     and journal.get('source_refusals_leases_stored_heads_and_finite_cohorts_changed') is False)
            journal_match = journal_match and valid
            journals.append({'marker': proof['key'], 'source_fingerprint': journal.get('source_fingerprint'),
                             'original_urls': len(observations),
                             'journal_sha256': hashlib.sha256(proof['value'].encode()).hexdigest(), 'accepted': valid})
            if not valid:
                errors.append({'journal': proof['key'], 'error': 'original_recheck_journal_invalid'})
                continue
            for observation in observations:
                clock = datetime.fromisoformat(observation['previous_last_checked_at_utc'].replace('Z', '+00:00'))
                if clock.tzinfo is None:
                    raise ValueError('original_recheck_observation_clock_invalid')
                previous = originals.get(observation['url'])
                if previous is None or clock < previous[0]:
                    originals[observation['url']] = (clock, observation)
                if len(originals) > 512:
                    raise ValueError('bounded_original_recheck_cohort_required')
        loaded_match = bool(loaded and loaded[0] == source.data_fingerprint
                            and status.get('cnyes_unknown_body_recheck_contract_version') == CNYES_UNKNOWN_BODY_RECHECK_CONTRACT_VERSION)
        owner_coherent = bool(owner and owner[0] == status.get('owner_id') and status.get('activity') not in {'stopped', 'error'})
        try:
            stamp = datetime.fromisoformat(status.get('updated_at_utc', '').replace('Z', '+00:00'))
            age = (datetime.now(timezone.utc) - stamp).total_seconds() if stamp.tzinfo else float('inf')
            owner_coherent = owner_coherent and 0 <= age <= 300
        except (ValueError, TypeError):
            owner_coherent = False
        rows = []
        for _, original in sorted(originals.values(), key=lambda value: (value[0], value[1]['url'])):
            current = db.execute('SELECT provider,url,state,error_code,due,attempts,last_checked_at_utc,body_sha256 '
                                 'FROM articles WHERE url=?', (original['url'],)).fetchone()
            accepted = bool(current and current['provider'] == 'cnyes')
            if accepted:
                candidate = {'provider': 'cnyes', 'url': original['url'], 'body_sha256': None}
                clock = datetime.fromisoformat(original['previous_last_checked_at_utc'].replace('Z', '+00:00'))
                accepted = NewsQueue.unknown_cnyes_body_recheck_allowed(source, candidate) and clock.tzinfo is not None
                unchanged_observation = current['last_checked_at_utc'] == original['previous_last_checked_at_utc']
                if unchanged_observation and current['state'] != 'inflight':
                    accepted = accepted and (current['state'] in {'retry', 'retry_cooldown'}
                        and current['error_code'] == 'body_not_found' and current['body_sha256'] is None
                        and current['attempts'] == original['attempts_preserved']
                        and current['due'] >= max(original['previous_due'], clock.timestamp() + CNYES_UNKNOWN_BODY_RECHECK_SECONDS))
                elif current['state'] == 'complete':
                    accepted = accepted and bool(current['body_sha256'])
                elif current['state'] != 'inflight':
                    latest = datetime.fromisoformat(current['last_checked_at_utc'].replace('Z', '+00:00'))
                    accepted = accepted and latest.tzinfo is not None and latest >= clock
            item = {'url': original['url'], 'original_observation': original,
                    'current_state': current['state'] if current else None,
                    'current_error_code': current['error_code'] if current else None,
                    'current_last_checked_at_utc': current['last_checked_at_utc'] if current else None,
                    'current_due_epoch': current['due'] if current else None,
                    'stored_head_observed': bool(current and current['body_sha256']), 'accepted': accepted}
            rows.append(item)
            if not accepted:
                errors.append({'url': original['url'], 'error': 'recheck_contract_or_current_state_mismatch'})
        return {'contract_version': 2, 'observed_at_utc': utc_now(),
                'queue_owner_id': owner[0] if owner else None, 'status_owner_id': status.get('owner_id'),
                'owner_status_coherent': owner_coherent, 'loaded_recheck_contract_matches': loaded_match,
                'original_recheck_journal_matches': journal_match,
                'retained_recheck_journals': journals,
                'cohort_basis': 'unique original URLs across retained journals of the loaded repair ABI; earliest observation preserved',
                'original_failure_cohort_urls': len(rows), 'current_cohort_states': dict(Counter(r['current_state'] for r in rows)),
                'stored_body_heads_observed': sum(r['stored_head_observed'] for r in rows),
                'rows': rows, 'errors': errors, 'publisher_requests': 0, 'source_states_mutated': False,
                'body_durability_verified': False, 'source_history_complete': False,
                'scope': 'original unknown-body recheck cohort, loaded contract and current head states; not body or history acceptance',
                'accepted': owner_coherent and loaded_match and journal_match and not errors}
    finally:
        db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, default=Path('configs/news_fulltext.json'))
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    config, registry, repo = load_config(args.config)
    result = audit(repo / config['root'], registry)
    result['configuration_files_sha256'] = {str(p): sha256_file(p) for p in configuration_paths(args.config)}
    result['implementation_sha256'] = {str(p): sha256_file(repo / p) for p in (
        Path('scripts/audit_news_body_rechecks.py'), Path('downloader/news_fulltext_queue.py'))}
    args.output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    atomic_write_json(args.output, result)
    print(json.dumps({k: result[k] for k in ('accepted', 'original_failure_cohort_urls', 'current_cohort_states', 'errors')}))
    return 0 if result['accepted'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
