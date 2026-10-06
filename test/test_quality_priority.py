from datetime import UTC, datetime, timedelta
import json
from types import SimpleNamespace

import pytest
from downloader import quality_priority as priority


def intent(root):
    row = {'code': 'BTCUSDT', 'reason': 'gap_candidate', 'request_id': 'a' * 64,
           'evidence_sha256': 'b' * 64, 'state': 'pending', 'attempts': 0,
           'expires_at_utc': (datetime.now(UTC) + timedelta(days=1)).isoformat()}
    path = root / 'quality_priority_requests.json'
    path.write_text(json.dumps({'contract': priority.CONTRACT, 'requests': [row]}))
    return path


def test_historical_repair_first_without_delaying_live_tail(tmp_path):
    intent(tmp_path)
    rows = [SimpleNamespace(code='ETHUSDT'), SimpleNamespace(code='BTCUSDT')]
    assert [r.code for r in priority.prioritize_records(rows, tmp_path, tail_only=False)] == ['BTCUSDT', 'ETHUSDT']
    assert priority.prioritize_records(rows, tmp_path, tail_only=True) is rows


def test_a_successful_source_recheck_is_not_claimed_as_a_gap_fill(tmp_path):
    path = intent(tmp_path)
    def collect(page):
        page('BTCUSDT')
        return 'upstream_empty'
    assert priority.collect_with_quality_priority(collect, root=tmp_path, code='BTCUSDT',
               tail_only=False, page_observer=lambda *_: None) == 'upstream_empty'
    row = json.loads(path.read_text())['requests'][0]
    assert row['state'] == 'source_checked_not_fill_proof'
    assert row['history'][0]['gap_filled'] is None
    assert priority.pending_requests(tmp_path) == []


def test_failed_rechecks_have_a_cap_and_preserve_attempt_evidence(tmp_path):
    path = intent(tmp_path)
    for _ in range(3):
        with pytest.raises(RuntimeError):
            with priority.quality_attempt(tmp_path, 'BTCUSDT', tail_only=False):
                raise RuntimeError('provider unavailable')
    row = json.loads(path.read_text())['requests'][0]
    assert row['state'] == 'retry_exhausted'
    assert row['attempts'] == len(row['history']) == 3
    assert priority.pending_requests(tmp_path) == []


def test_bad_optional_plan_does_not_block_unrelated_collection(tmp_path):
    (tmp_path / 'quality_priority_requests.json').write_text('{bad')
    rows = [SimpleNamespace(code='ETHUSDT')]
    assert priority.prioritize_records(rows, tmp_path, tail_only=False) == rows


def test_untrusted_symbol_path_is_rejected_not_dispatched(tmp_path):
    path = intent(tmp_path)
    payload = json.loads(path.read_text()); payload['requests'][0]['code'] = '../outside'
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError):
        priority.load_plan(tmp_path)


def test_optional_receipt_failure_does_not_change_source_success(tmp_path, monkeypatch):
    intent(tmp_path)
    def fail(*_):
        raise OSError('disk full')
    monkeypatch.setattr(priority, 'atomic_write_json', fail)
    with priority.quality_attempt(tmp_path, 'BTCUSDT', tail_only=False) as attempt:
        attempt.observe_page()
    assert priority.pending_requests(tmp_path)[0]['attempts'] == 0


def test_worker_returning_failure_is_not_acknowledged_as_source_checked(tmp_path):
    path = intent(tmp_path)
    def collect(page):
        page('BTCUSDT')
        return SimpleNamespace(status='failed', message='invalid provider data')
    priority.collect_with_quality_priority(collect, root=tmp_path, code='BTCUSDT',
        tail_only=False, page_observer=lambda *_: None)
    row = json.loads(path.read_text())['requests'][0]
    assert row['state'] == 'retryable'
    assert row['history'][0]['failed'] is True


def test_legacy_intent_adoption_never_duplicates_or_resets_retry_cap(tmp_path, monkeypatch):
    from scripts import queue_source_anomaly_priorities as queue
    owner = tmp_path / 'data_binance' / '1m'
    owner.mkdir(parents=True)
    path = intent(owner)
    plan = json.loads(path.read_text())
    plan['requests'][0].update(attempts=3, state='retry_exhausted')
    path.write_text(json.dumps(plan))
    item = {'code': 'BTCUSDT', 'reason': 'gap_candidate', 'repair_identity_sha256': 'c' * 64,
            'evidence_sha256': 'b' * 64}
    monkeypatch.setattr(queue, 'candidates', lambda *_args, **_kwargs: [(owner, item)])
    for _ in range(2):
        result = queue.enqueue(tmp_path, tmp_path / 'audit', live=True, now=datetime.now(UTC))
        assert result['owners'][0]['added'] == 0
    rows = json.loads(path.read_text())['requests']
    assert len(rows) == 1 and rows[0]['attempts'] == 3
    assert rows[0]['state'] == 'retry_exhausted'
    # A different future missing minute is a new observation, not an old retry.
    item['repair_identity_sha256'] = 'd' * 64
    queue.enqueue(tmp_path, tmp_path / 'audit', live=True, now=datetime.now(UTC))
    assert len(json.loads(path.read_text())['requests']) == 2


def test_legacy_crypto_evidence_is_kept_without_dispatching_to_minute_writer(tmp_path, monkeypatch):
    from scripts import queue_source_anomaly_priorities as queue
    owner = tmp_path / 'data_yahoo/crypto'
    owner.mkdir(parents=True)
    item = {'code': 'BTCUSD', 'reason': 'invalid_values', 'repair_identity_sha256': 'c' * 64,
        'evidence_sha256': 'b' * 64, 'initial_state': 'legacy_grain_review',
        'dispatch_scope': 'legacy_daily_not_current_1m_collector_no_overwrite', 'audit_path': str(tmp_path)}
    monkeypatch.setattr(queue, 'candidates', lambda *_args, **_kwargs: [])
    monkeypatch.setattr(queue, 'yahoo_candidates', lambda *_args, **_kwargs: [(owner, item)])
    result = queue.enqueue(tmp_path, tmp_path, live=False, now=datetime.now(UTC), yahoo_report=tmp_path)
    assert result['owners'][0]['added'] == 1 and result['owners'][0]['pending'] == 0
    row = priority.load_plan(owner)['requests'][0]
    assert row['state'] == 'legacy_grain_review' and row['attempts'] == 0
    assert not priority.pending_requests(owner)
