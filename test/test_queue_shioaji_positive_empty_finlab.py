from datetime import datetime, timezone
from contextlib import contextmanager
import json
from types import SimpleNamespace

import pytest

from scripts import queue_shioaji_positive_empty_finlab as handoff
from scripts import download_finlab_intraday as owner


def test_handoff_requires_same_sources_and_exact_candidate_scope():
    candidate = {'contract': 'shioaji_positive_empty_finlab_candidates_v1',
                 'official_daily_path': '/official', 'official_daily_sha256': 'a',
                 'manifest_root': '/history', 'manifest_pins': [{'alias': 'A', 'sha256': 'b'}],
                 'source_gap_alias_dates': 1, 'candidates': [{'dataset': 'tw_futures_minute:TX_202501',
                                                              'partition': '2025-01-02'}],
                 'unresolved': []}
    handoff.validate_candidate(dict(candidate), candidate)
    with pytest.raises(ValueError, match='source generations'):
        handoff.validate_candidate({**candidate, 'official_daily_sha256': 'changed'}, candidate)


def test_handoff_defers_invalid_local_receipt_and_keeps_other_candidates(tmp_path, monkeypatch):
    invalid = tmp_path / 'invalid.json'
    invalid.write_text('{}')
    monkeypatch.setattr(owner, '_partition_paths',
        lambda root, dataset, day: ((invalid if dataset.endswith('TX_202501') else tmp_path / 'missing'),
                                    tmp_path / 'attempt'))
    monkeypatch.setattr(owner, '_stored_receipt', lambda *args, **kwargs: None)
    candidate = {'candidates': [
        {'dataset': 'tw_futures_minute:TX_202501', 'partition': '2025-01-02'},
        {'dataset': 'tw_futures_minute:TX_202502', 'partition': '2025-01-02'}]}
    admitted, deferred = handoff.items_without_invalid_receipts(candidate, tmp_path)
    assert [row['dataset'] for row in admitted] == ['tw_futures_minute:TX_202502']
    assert [row['dataset'] for row in deferred] == ['tw_futures_minute:TX_202501']
    request = handoff.make_request(admitted, datetime(2026, 11, 3, tzinfo=timezone.utc))
    assert request['expires_at_utc'] == '2026-11-10T00:00:00+00:00'
    assert request['items'] == admitted


def test_failed_provider_handoff_never_registers_or_calls_source(tmp_path, monkeypatch):
    output = tmp_path / 'candidate.json'
    output.write_text('{}')
    runs = tmp_path / 'runs'
    run = runs / handoff.HANDOFF_RUN
    run.mkdir(parents=True)
    (run / 'run.json').write_text(json.dumps({'state': 'failed'}))
    monkeypatch.setattr(handoff, 'OUTPUT', output)
    monkeypatch.setattr(handoff, 'REPORT', tmp_path / 'report.json')
    monkeypatch.setattr(handoff, 'RUNS', runs)
    monkeypatch.setattr(handoff, 'register', lambda *a, **k: pytest.fail('must not register'))
    assert handoff.main() == 75
    assert json.loads((tmp_path / 'report.json').read_text())['state'] == 'provider_handoff_failed'


def test_successful_handoff_is_idempotent_and_uses_original_finite_owner(tmp_path, monkeypatch):
    candidate = {'contract': 'shioaji_positive_empty_finlab_candidates_v1',
                 'official_daily_path': '/official', 'official_daily_sha256': 'a',
                 'manifest_root': '/history', 'manifest_pins': [{'alias': 'A', 'sha256': 'b'}],
                 'source_gap_alias_dates': 1, 'candidates': [{'dataset': 'tw_futures_minute:TX_202501',
                                                              'partition': '2025-01-02'}],
                 'unresolved': []}
    output = tmp_path / 'candidate.json'
    output.write_text(json.dumps(candidate))
    runs = tmp_path / 'runs'
    run = runs / handoff.HANDOFF_RUN
    run.mkdir(parents=True)
    (run / 'run.json').write_text(json.dumps({'state': 'succeeded'}))
    for name, path in [('OUTPUT', output), ('REPORT', tmp_path / 'report.json'),
                       ('REQUEST', tmp_path / 'request.json'), ('JOURNAL', tmp_path / 'journal.json'),
                       ('RUNS', runs), ('FINLAB', tmp_path / 'finlab')]:
        monkeypatch.setattr(handoff, name, path)
    monkeypatch.setattr(handoff.placement, 'MIGRATION_FENCE', tmp_path / 'absent-fence')
    monkeypatch.setattr(handoff.placement, 'load', lambda: {'providers': {name: {} for name in handoff.placement.PROVIDERS}})
    monkeypatch.setattr(handoff.placement, 'check_entry', lambda name, entry: None)
    monkeypatch.setattr(handoff, 'build', lambda: candidate)
    item = candidate['candidates'][0]
    monkeypatch.setattr(handoff, 'items_without_invalid_receipts', lambda *args: ([item], []))
    monkeypatch.setattr(handoff.volume, 'path', lambda *args: tmp_path)
    monkeypatch.setattr(handoff.volume, 'configuration', lambda: {'minimum_free_bytes': 1})
    admissions = []
    monkeypatch.setattr(handoff.volume, 'admit', lambda value: admissions.append(value))

    @contextmanager
    def reserve(*args, **kwargs):
        yield {'other_reserved_growth_bytes': 123}

    monkeypatch.setattr(handoff, 'reserve_capacity', reserve)
    monkeypatch.setattr(handoff, 'register', lambda root, request: {
        'request_id': 'a' * 64, 'items': [{'state': 'pending_existing_owner'}]})
    monkeypatch.setattr(handoff, 'process_start_ticks', lambda pid: 42)
    launched = []

    def fake_launch(command, **kwargs):
        argv = command[command.index('--') + 1:]
        record = {'task_id': handoff.TASK, 'argv': argv, 'run_id': handoff.FINITE_NAME + '-one'}
        path = runs / record['run_id']
        path.mkdir()
        (path / 'run.json').write_text(json.dumps(record))
        launched.append(record)
        return SimpleNamespace(stdout=json.dumps(record))

    monkeypatch.setattr(handoff.subprocess, 'run', fake_launch)
    assert handoff.main() == 0
    assert handoff.main() == 0
    assert len(launched) == 1
    assert admissions == [128 * 1024**2 + 123] * 2
    assert 'scripts/run_finlab_finite_priority_handoff.py' in launched[0]['argv']
    report = json.loads((tmp_path / 'report.json').read_text())
    assert report['state'] == 'registered_to_canonical_finlab_owner'
    assert report['provider_calls_by_handoff'] == 0 and not report['data_or_training_admission']
