from datetime import UTC, datetime, timedelta
import json

import pytest

from downloader.quality_priority import CONTRACT, pending_requests
from scripts.probe_source_anomaly_windows import classify_response
from scripts.verify_source_anomaly_rechecks import acknowledge


def write_intent(root):
    row = {'code': 'BTCUSDT', 'reason': 'gap_candidate', 'request_id': 'a' * 64,
           'evidence_sha256': 'b' * 64, 'state': 'source_checked_not_fill_proof',
           'attempts': 1, 'history': [{'request_pages': 1, 'gap_filled': None}],
           'expires_at_utc': (datetime.now(UTC) + timedelta(days=1)).isoformat()}
    path = root / 'quality_priority_requests.json'
    path.write_text(json.dumps({'contract': CONTRACT, 'requests': [row]}))
    return path


@pytest.mark.parametrize('status,expected', [
    ('values_checked', 'verified_logical_price_clear'),
    ('gap_candidate', 'source_rechecked_anomaly_remains'),
    ('invalid_values', 'source_rechecked_anomaly_remains'),
    ('changed_during_check', 'source_checked_not_fill_proof'),
])
def test_only_stable_full_value_proof_acknowledges_repair(tmp_path, status, expected):
    path = write_intent(tmp_path)
    acknowledge(tmp_path, 'BTCUSDT', {'status': status, 'file_binding': []}, tmp_path / 'audit')
    row = json.loads(path.read_text())['requests'][0]
    assert row['state'] == expected
    assert row['attempts'] == 1 and row['history'][0]['gap_filled'] is None
    if status != 'changed_during_check':
        assert row['verification']['gap_filled'] == (status == 'values_checked')
        assert not row['verification']['all_financial_features_checked']
    assert pending_requests(tmp_path) == []


def test_exact_bybit_response_preserves_real_upstream_geometry_error():
    timestamp = 1612419480000
    row = [str(timestamp), '24.633', '24.718', '24.687', '24.687', '128.7', '3177.2169']
    result = classify_response('data_bybit', {'result': {'list': [row]}}, timestamp, timestamp)
    assert result['not_returned_requested_keys'] == 0
    assert result['source_invalid_requested_rows'] == 1
    assert result['source_rows_in_requested_window'] == [row]  # Never widen high/low.


@pytest.mark.parametrize('payload', [{}, {'result': {}}, {'result': {'list': {}}},
                                      {'result': {'list': [['0']]}}])
def test_missing_or_malformed_response_is_not_an_empty_source(payload):
    with pytest.raises(ValueError):
        classify_response('data_bybit', payload, 0, 0)


def test_empty_authenticated_page_is_scoped_to_requested_instants():
    result = classify_response('data_bybit', {'result': {'list': []}}, 60000, 180000)
    assert result['page_covers_requested_lower_bound']
    assert result['not_returned_requested_keys'] == 3


def test_okx_unconfirmed_bar_is_not_historical_completion():
    row = ['60000', '100', '101', '99', '100', '1', '1', '100', '0']
    result = classify_response('data_okx', {'data': [row]}, 60000, 60000)
    assert result['not_returned_requested_keys'] == 1
    assert result['source_rows_in_requested_window'] == []
