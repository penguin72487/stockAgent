"""CNYES missing-original scans advance without skipping evidence or retry work."""
import copy
import hashlib
import itertools
import json
import os
from pathlib import Path
import sqlite3

import pytest

from downloader.news_fulltext_bulk import cnyes_url
from downloader.news_fulltext_cnyes_history import initialize
from downloader.news_fulltext_queue import NewsQueue
from downloader.news_fulltext_replay import ensure_replay_schema
from scripts import audit_news_cnyes_retained_missing as module
from test_cnyes_news_bulk import BASE, cfg, payload, registry_http, response  # noqa: F401
from test_news_cached_replay import _insert_crosscheck_observation, _request_scoped_history_fixture, retained


def test_large_unselected_scan_advances_by_primary_key_without_losing_observations(cfg, tmp_path, monkeypatch):
    registry, _, _, _, artifacts, _ = _request_scoped_history_fixture(cfg, tmp_path, monkeypatch)
    db = sqlite3.connect(tmp_path / 'queue.sqlite3')
    try:
        with db:
            db.executemany('INSERT INTO cnyes_history_page_observations VALUES (?,?,?,?,?,?,?,?)',
                ((f'noise-{number:06}', 1, '0' * 64, 1, 'a' * 64, 'b' * 64, f'{number:064x}',
                  '{"identities":[]}') for number in range(100_001)))
    finally:
        db.close()
    reports, previous = [], None
    for _ in range(3):
        result = module.audit(tmp_path, registry, {}, observation_chunk_size=50_000, resume_scan=previous)
        reports.append(result)
        previous = result
    assert [r['observation_scan']['resume_action'] for r in reports] == ['next_chunk', 'next_chunk', 'finished']
    assert [r['observation_scan']['observations_scanned'] for r in reports] == [50_000, 50_000, 3]
    assert all(r['contract'] == 'cnyes_retained_missing_crosscheck_chunk_v1' for r in reports)
    assert sum(r['source_envelopes_verified'] for r in reports) == 2
    assert {r['source_artifact_path'] for report in reports for r in report['target_outcomes']} == {
        artifact['parquet_path'] for artifact in artifacts}
    assert not any(r['response_failures'] or r['unresolved_matching_observations'] for r in reports)
    with pytest.raises(ValueError, match='scan_already_finished'):
        module.audit(tmp_path, registry, {}, observation_chunk_size=50_000, resume_scan=previous)


def test_interrupted_chunk_continues_candidate_groups_without_repeating_them(cfg, tmp_path, monkeypatch):
    registry, _, _, _, artifacts, _ = _request_scoped_history_fixture(cfg, tmp_path, monkeypatch)
    first = module.audit(tmp_path, registry, {}, observation_chunk_size=2, max_responses=1)
    assert first['unfinished_source_envelopes'] == 1
    assert first['observation_scan']['resume_action'] == 'retry_chunk'
    second = module.audit(tmp_path, registry, {}, observation_chunk_size=2, max_responses=1, resume_scan=first)
    assert second['unfinished_source_envelopes'] == 0
    assert second['observation_scan']['resume_action'] == 'finished'
    outcomes = first['target_outcomes'] + second['target_outcomes']
    assert len(outcomes) == 2
    assert {row['source_artifact_path'] for row in outcomes} == {a['parquet_path'] for a in artifacts}


def test_chunk_size_can_change_between_complete_chunks_but_not_during_retry(cfg, tmp_path, monkeypatch):
    registry, _, _, _, _, _ = _request_scoped_history_fixture(cfg, tmp_path, monkeypatch)
    first = module.audit(tmp_path, registry, {}, observation_chunk_size=1)
    assert first['observation_scan']['resume_action'] == 'next_chunk'
    second = module.audit(tmp_path, registry, {}, observation_chunk_size=2, resume_scan=first)
    assert second['observation_scan']['resume_action'] == 'finished'
    partial = module.audit(tmp_path, registry, {}, observation_chunk_size=2, max_responses=1)
    assert partial['observation_scan']['resume_action'] == 'retry_chunk'
    with pytest.raises(ValueError, match='scan_scope_changed'):
        module.audit(tmp_path, registry, {}, observation_chunk_size=1, resume_scan=partial)


def test_slow_scan_still_attempts_one_candidate_per_slice(cfg, tmp_path, monkeypatch):
    registry, _, _, _, _, _ = _request_scoped_history_fixture(cfg, tmp_path, monkeypatch)
    clock = itertools.count(0, 10)
    with monkeypatch.context() as patch:
        patch.setattr(module.time, 'monotonic', lambda: next(clock))
        first = module.audit(tmp_path, registry, {}, observation_chunk_size=2, max_seconds=1)
    assert first['observation_scan']['candidate_offset'] == 1
    assert first['observation_scan']['resume_action'] == 'retry_chunk'
    assert first['source_envelopes_verified'] == 1


def test_retry_rejects_changed_candidate_set_when_a_prior_receipt_changes(cfg, tmp_path, monkeypatch):
    from downloader import news_fulltext_cnyes_history as history

    registry, data, _, _, _, _ = _request_scoped_history_fixture(cfg, tmp_path, monkeypatch)
    queue = NewsQueue(tmp_path, registry)
    source = registry.sources['cnyes']
    url = cnyes_url(BASE, start=1388534600, end=1388534700)
    try:
        with queue.db:
            artifact = history.retain_response(queue, response(data, url), source, accepted=[], restricted=False)
            _insert_crosscheck_observation(queue, data, url, artifact)
    finally:
        queue.close()
    prior = module.audit(tmp_path, registry, {})
    prior['target_outcomes'] = prior['target_outcomes'][:1]
    first = module.audit(tmp_path, registry, prior, observation_chunk_size=3, max_responses=1)
    assert first['observation_scan']['resume_action'] == 'retry_chunk'
    receipt = tmp_path / prior['target_outcomes'][0]['source_receipt_path']
    receipt.write_bytes(receipt.read_bytes() + b' ')
    with pytest.raises(ValueError, match='scan_candidates_changed'):
        module.audit(tmp_path, registry, prior, observation_chunk_size=3, max_responses=1, resume_scan=first)


def test_failures_remain_counted_when_a_chunk_is_resumed(cfg, tmp_path, monkeypatch):
    registry, _, _, _, artifacts, _ = _request_scoped_history_fixture(cfg, tmp_path, monkeypatch)
    for artifact in artifacts:
        (tmp_path / artifact['receipt_path']).write_text('invalid receipt')
    first = module.audit(tmp_path, registry, {}, observation_chunk_size=2, max_responses=1)
    assert first['observation_scan']['chunk_response_failures'] == 1
    second = module.audit(tmp_path, registry, {}, observation_chunk_size=2, max_responses=1, resume_scan=first)
    assert second['observation_scan']['resume_action'] == 'finished'
    assert second['observation_scan']['chunk_response_failures'] == 2


def test_resume_rejects_changed_prior_snapshot_and_tampered_checkpoint(cfg, tmp_path, monkeypatch):
    registry, _, _, _, _, _ = _request_scoped_history_fixture(cfg, tmp_path, monkeypatch)
    alternate = tmp_path / 'alternate'
    alternate.mkdir()
    for name in ('queue.sqlite3', 'queue.sqlite3-wal', 'queue.sqlite3-journal'):
        if (tmp_path / name).exists():
            os.link(tmp_path / name, alternate / name)
    first = module.audit(tmp_path, registry, {}, observation_chunk_size=1)
    assert first['observation_scan']['resume_action'] == 'next_chunk'
    with pytest.raises(ValueError, match='scan_scope_changed'):
        module.audit(alternate, registry, {}, observation_chunk_size=1, resume_scan=first)
    with pytest.raises(ValueError, match='scan_prior_changed'):
        module.audit(tmp_path, registry, {'different': True}, observation_chunk_size=1, resume_scan=first)
    changed = copy.deepcopy(first)
    changed['observation_scan']['candidate_offset'] += 1
    with pytest.raises(ValueError, match='scan_checkpoint_invalid'):
        module.audit(tmp_path, registry, {}, observation_chunk_size=1, resume_scan=changed)
    changed = copy.deepcopy(first)
    changed['observation_scan']['observations_scanned'] = 0
    scan = changed['observation_scan']
    scan['checkpoint_sha256'] = module._json_digest({k: v for k, v in scan.items() if k != 'checkpoint_sha256'})
    with pytest.raises(ValueError, match='scan_checkpoint_invalid'):
        module.audit(tmp_path, registry, {}, observation_chunk_size=1, resume_scan=changed)
    db = sqlite3.connect(tmp_path / 'queue.sqlite3')
    try:
        with db:
            db.execute('INSERT INTO cnyes_history_page_observations VALUES (?,?,?,?,?,?,?,?)',
                ('later-scope', 1, '0' * 64, 1, 'a' * 64, 'b' * 64, 'f' * 64, '{"identities":[]}'))
    finally:
        db.close()
    with pytest.raises(ValueError, match='scan_snapshot_changed'):
        module.audit(tmp_path, registry, {}, observation_chunk_size=1, resume_scan=first)


def test_split_requests_keep_mismatch_and_verified_original_distinct(cfg, tmp_path):
    registry, _ = registry_http(cfg, tmp_path)
    queue = NewsQueue(tmp_path, registry)
    data = payload(ids=(1,))
    try:
        path = retained(queue, data)
        initialize(queue)
        ensure_replay_schema(queue)
        artifact = {'kind': 'canonical_quarantine', 'parquet_path': 'quarantine/' + path.name,
                    'receipt_path': 'quarantine/' + path.name + '.receipt.json'}
        with queue.db:
            queue.add_articles([{'url': 'https://news.cnyes.com/news/id/1'}])
            queue.db.execute("UPDATE articles SET state='source_article_empty_template'")
            exact = _insert_crosscheck_observation(queue, data, response(data).url, artifact)
            wrong = _insert_crosscheck_observation(queue, data,
                cnyes_url(BASE, start=1388534400, end=1388534500), artifact)
    finally:
        queue.close()
    first = module.audit(tmp_path, registry, {}, observation_chunk_size=1)
    second = module.audit(tmp_path, registry, {}, observation_chunk_size=1, resume_scan=first)
    outcomes = first['target_outcomes'] + second['target_outcomes']
    mismatches = first['request_mismatches'] + second['request_mismatches']
    assert len(outcomes) == len(mismatches) == 1
    assert outcomes[0]['observation_sha256'] == [exact]
    assert mismatches[0]['observation_sha256'] == wrong
    assert sum(len(r['source_only_target_outcomes']) for r in (first, second)) == 1


def test_reviewed_previous_v4_individual_proof_survives_chunk_reader_change(cfg, tmp_path, monkeypatch):
    registry, _, _, _, _, _ = _request_scoped_history_fixture(cfg, tmp_path, monkeypatch)
    prior = module.audit(tmp_path, registry, {})
    # Simulate the reviewed historical reader as a whole. Combining its old
    # audit SHA with the current history writer SHA is not that old reader.
    prior['parser_identity'] = dict(module._V4_REVIEWED_RECENT_PARSER)
    reused = module.audit(tmp_path, registry, prior)
    assert reused['candidate_source_envelopes'] == 0
    prior['parser_identity']['downloader/news_fulltext_policy.py'] = '0' * 64
    assert module.audit(tmp_path, registry, prior)['candidate_source_envelopes'] == 2


def test_cli_persists_and_resumes_chunk_report_without_writing_source(cfg, tmp_path, monkeypatch):
    source = tmp_path / 'source'
    source.mkdir()
    registry, _, _, _, artifacts, _ = _request_scoped_history_fixture(cfg, source, monkeypatch)
    monkeypatch.setattr(module, 'load_config', lambda _: ({'root': str(source)}, registry, Path.cwd()))
    prior = tmp_path / 'prior.json'
    prior.write_text('{}')
    first_path, second_path = tmp_path / 'first.json', tmp_path / 'second.json'
    stamp = module._stamp(source / 'queue.sqlite3')
    base = ['--exclude-audit', str(prior), '--observation-chunk-size', '1']
    assert module.main([*base, '--output', str(first_path)]) == 0
    assert module.main([*base, '--resume-scan', str(first_path), '--output', str(second_path)]) == 0
    first, second = json.loads(first_path.read_text()), json.loads(second_path.read_text())
    assert second['resumed_scan_audit_sha256'] == hashlib.sha256(first_path.read_bytes()).hexdigest()
    assert {r['source_artifact_path'] for report in (first, second) for r in report['target_outcomes']} == {
        artifact['parquet_path'] for artifact in artifacts}
    assert second['observation_scan']['resume_action'] == 'finished'
    assert module._stamp(source / 'queue.sqlite3') == stamp
    from scripts.audit_news_cnyes_issuer_overlap import audit as issuer_audit
    with pytest.raises(ValueError, match='invalid_cnyes_proof_scope'):
        issuer_audit(source, tmp_path, registry, second, {})


def test_incremental_checkpoint_digest_matches_canonical_json():
    value = {'中文': ['結果', 2, None], 'other': {'valid': True}}
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True,
                         separators=(',', ':'), allow_nan=False).encode()
    assert module._json_digest(value) == hashlib.sha256(encoded).hexdigest()
