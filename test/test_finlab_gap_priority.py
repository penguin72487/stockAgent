from datetime import UTC, datetime, timedelta
import json

import pytest

from stockagent.data.finlab_acquisition_contract import safe_stem
from stockagent.data.finlab_gap_priority import CONTRACT, pending_gap_requests
from scripts import download_finlab_history as collector
from scripts.check_finlab_refresh_due import refresh_due

NOW = datetime(2026, 10, 4, 12, tzinfo=UTC)


def plan(root, key="monthly_revenue:當月營收"):
    root.mkdir(parents=True, exist_ok=True)
    row = {"dataset": key, "request_id": "audited-gap-1", "evidence_sha256": "a" * 64,
           "reason": "interior_observation_gap_recheck", "priority": 1,
           "required_after_utc": (NOW - timedelta(seconds=1)).isoformat(),
           "expires_at_utc": (NOW + timedelta(days=1)).isoformat()}
    (root / "gap_priority_requests.json").write_text(json.dumps({"contract": CONTRACT, "requests": [row]}))
    return row


def test_cache_read_cannot_discharge_owner_recheck(tmp_path):
    row = plan(tmp_path)
    receipt = tmp_path / "receipts" / (safe_stem(row["dataset"]) + ".json")
    receipt.parent.mkdir()
    receipt.write_text(json.dumps({"dataset": row["dataset"], "source_checked_at_utc": NOW.isoformat(),
                                   "source_check_mode": "sdk_cache_allowed"}))
    assert row["dataset"] in pending_gap_requests(tmp_path, now=NOW)
    receipt.write_text(json.dumps({"dataset": row["dataset"], "source_checked_at_utc": NOW.isoformat(),
                                   "source_check_mode": "upstream_incremental", "last_check_result": "unchanged"}))
    assert pending_gap_requests(tmp_path, now=NOW) == {}  # Check != gap filled.


def test_future_expired_unknown_keys_are_not_admitted(tmp_path):
    row = plan(tmp_path)
    assert not pending_gap_requests(tmp_path, now=NOW - timedelta(hours=1))
    assert not pending_gap_requests(tmp_path, now=NOW + timedelta(days=2))
    assert not pending_gap_requests(tmp_path, now=NOW, available=["price:close"])
    assert row["dataset"] in pending_gap_requests(tmp_path, now=NOW)


def test_existing_scheduler_prioritizes_but_keeps_cooldowns(tmp_path, monkeypatch):
    row = plan(tmp_path)
    gap, ordinary = row["dataset"], "financial_statement:cash"
    monkeypatch.setattr(collector, "has_local_download", lambda *a: True)
    monkeypatch.setattr(collector, "read_key_state", lambda *a: {})
    monkeypatch.setattr(collector, "source_check_due", lambda *a, **k: True)
    monkeypatch.setattr(collector, "recent_attempt", lambda *a, **k: False)
    monkeypatch.setattr(collector, "last_source_check", lambda *a: "2020-01-01")
    monkeypatch.setattr(collector, "verified_source_empty", lambda *a: False)
    args = ([ordinary, gap], {}, tmp_path)
    result = collector._build_sync_work_plan(*args, now=NOW, refresh_days=1, retry_unavailable=False)
    assert result["primary"] == [gap, ordinary]
    monkeypatch.setattr(collector, "recent_attempt", lambda key, *a, **k: key == gap)
    result = collector._build_sync_work_plan(*args, now=NOW, refresh_days=1, retry_unavailable=False)
    assert result["primary"] == [ordinary]
    assert gap in result["required_blocked"]


def test_priority_intent_does_not_starve_market_price_updates(tmp_path, monkeypatch):
    row = plan(tmp_path); gap, market = row['dataset'], 'price:close'
    monkeypatch.setattr(collector, 'has_local_download', lambda *a: True)
    monkeypatch.setattr(collector, 'read_key_state', lambda *a: {})
    monkeypatch.setattr(collector, 'source_check_due', lambda *a, **k: True)
    monkeypatch.setattr(collector, 'recent_attempt', lambda *a, **k: False)
    monkeypatch.setattr(collector, 'last_source_check', lambda *a: '2020-01-01')
    monkeypatch.setattr(collector, 'verified_source_empty', lambda *a: False)
    result = collector._build_sync_work_plan([gap, market], {}, tmp_path, now=NOW, refresh_days=1, retry_unavailable=False)
    assert result['primary'] == [market, gap]


def test_timer_notices_intent_without_provider_network(tmp_path):
    plan(tmp_path / "data_finlab")
    assert refresh_due(tmp_path, now=NOW) == (True, "owner_gap_recheck_due")


def test_invalid_plan_fails_closed(tmp_path):
    plan(tmp_path)
    p = tmp_path / "gap_priority_requests.json"
    p.write_text(json.dumps({"contract": "wrong", "requests": []}))
    with pytest.raises(ValueError):
        pending_gap_requests(tmp_path, now=NOW)


@pytest.mark.parametrize('document', [[], {'contract': CONTRACT, 'requests': [None]}])
def test_malformed_plan_shape_is_recoverable_error(tmp_path, document):
    plan(tmp_path)
    (tmp_path / 'gap_priority_requests.json').write_text(json.dumps(document))
    with pytest.raises(ValueError):
        pending_gap_requests(tmp_path, now=NOW)


def test_nonobject_receipt_does_not_discharge_request(tmp_path):
    row = plan(tmp_path)
    receipt = tmp_path / 'receipts' / (safe_stem(row['dataset']) + '.json')
    receipt.parent.mkdir()
    receipt.write_text('[]')
    assert row['dataset'] in pending_gap_requests(tmp_path, now=NOW)


def test_captured_source_uses_current_receipt_not_stale_catalog_and_survives_head_refresh(tmp_path):
    from stockagent.data.finlab_acquisition_contract import read_receipt_bound_source
    import hashlib
    root = tmp_path / 'finlab'
    (root / 'datasets').mkdir(parents=True)
    (root / 'receipts').mkdir()
    old, current = root / 'datasets/old.parquet', root / 'datasets/current.parquet'
    old.write_bytes(b'old version'); current.write_bytes(b'new actual observations')
    key = 'financial_statement:資產總額'
    receipt = root / 'receipts/key.json'
    receipt.write_text(json.dumps({'dataset': key, 'parquet_path': 'datasets/current.parquet',
                                  'sha256': hashlib.sha256(current.read_bytes()).hexdigest()}))
    captured, proof = read_receipt_bound_source(root, key, receipt)
    assert captured == current and captured != old
    receipt.write_text('{}')  # A later head is not the already captured object.
    assert hashlib.sha256(captured.read_bytes()).hexdigest() == proof['sha256']
    current.write_bytes(b'corruption')
    receipt.write_text(json.dumps(proof))
    with pytest.raises(ValueError, match='hash mismatch'):
        read_receipt_bound_source(root, key, receipt)


def test_queue_is_idempotent_and_binds_audit_hash(tmp_path):
    from scripts.queue_tw_feature_gap_priorities import enqueue
    from scripts.prepare_tw_day_trade_feature_catalog import write_csv, sha256
    root, audit = tmp_path / 'finlab', tmp_path / 'audit'
    (root / 'catalog').mkdir(parents=True); audit.mkdir()
    key = 'monthly_revenue:當月營收'
    (root / 'catalog/discovery.json').write_text(json.dumps({'keys': [key]}))
    work = audit / 'provider_priority_worklist.csv'
    write_csv(work, [{'source': 'FinLab', 'dataset': key, 'reason': 'interior_observation_gap_recheck',
                     'priority': 1, 'unresolved_recheck_keys': 3}])
    (audit / 'audit_manifest.json').write_text(json.dumps({'priority_dataset_requests': 1,
        'outputs': {'provider_priority_worklist.csv': {'sha256': sha256(work)}}}))
    assert enqueue(audit, root, now=NOW)['queued_dataset_requests'] == 1
    assert enqueue(audit, root, now=NOW)['queued_dataset_requests'] == 0
    work.write_text('corrupt')
    with pytest.raises(ValueError, match='fixed gap audit'):
        enqueue(audit, root, now=NOW)


def test_status_keeps_expired_intents_separate_from_true_checks(tmp_path):
    from scripts.queue_tw_feature_gap_priorities import report_intents
    row = plan(tmp_path)
    future = NOW + timedelta(days=2)
    receipt, rows = report_intents(tmp_path, now=future)
    assert receipt['states'] == {'expired_without_verified_check': 1}
    assert receipt['all_provider_gaps_filled'] is False
    path = tmp_path / 'receipts' / (safe_stem(row['dataset']) + '.json')
    path.parent.mkdir()
    path.write_text(json.dumps({'dataset': row['dataset'], 'source_checked_at_utc': NOW.isoformat(),
                               'source_check_mode': 'sdk_cache_allowed'}))
    assert report_intents(tmp_path, now=future)[1][0]['state'] == 'expired_without_verified_check'
    path.write_text(json.dumps({'dataset': row['dataset'], 'source_checked_at_utc': NOW.isoformat(),
                               'source_check_mode': 'upstream_incremental', 'last_check_result': 'unchanged'}))
    receipt, rows = report_intents(tmp_path, now=future)
    assert receipt['states'] == {'true_upstream_checked_not_gap_fill_proof': 1}
    assert rows[0]['last_check_result'] == 'unchanged'
    assert rows[0]['captured_receipt_sha256']
    assert receipt['filled_observations_claim'] is None


def test_status_requires_causal_check_for_same_dataset(tmp_path):
    from scripts.queue_tw_feature_gap_priorities import report_intents
    row = plan(tmp_path)
    receipt = tmp_path / 'receipts' / (safe_stem(row['dataset']) + '.json')
    receipt.parent.mkdir()
    for dataset, checked in [('other', NOW), (row['dataset'], NOW + timedelta(hours=1))]:
        receipt.write_text(json.dumps({'dataset': dataset, 'source_checked_at_utc': checked.isoformat(),
                                       'source_check_mode': 'upstream_incremental'}))
        result, rows = report_intents(tmp_path, now=NOW)
        assert result['states'] == {'pending_existing_owner': 1}
    assert report_intents(tmp_path, now=NOW - timedelta(hours=1))[1][0]['state'] == 'not_yet_due'
