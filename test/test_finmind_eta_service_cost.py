"""Own accepted JSON costs must not inherit a large-object lane's idle time."""
from datetime import timedelta
import json
import sqlite3

import pytest

from downloader.finmind_eta_telemetry import json_service_statistics
from stockagent.live.finmind_eta_projection import public_completion_estimate
from test_finmind_eta import NOW
from test_finmind_eta_stages import inputs, run


def observations(root):
    owner = root / 'sponsor'
    owner.mkdir()
    with sqlite3.connect(owner / 'update_observations.sqlite3') as conn:
        conn.execute('CREATE TABLE checks(id INTEGER PRIMARY KEY,request_id TEXT,dataset TEXT,data_id TEXT,partition TEXT,metadata_json TEXT)')
        conn.execute('CREATE TABLE streams(dataset TEXT,data_id TEXT,partition TEXT,checked_at_utc TEXT)')
        for index in range(6):
            finish = NOW - timedelta(minutes=index + 1)
            checked = finish - timedelta(seconds=.1)
            metadata = {'request_started_at_utc': (finish - timedelta(seconds=1)).isoformat(),
                        'checked_at_utc': checked.isoformat()}
            conn.execute('INSERT INTO checks VALUES (?,?,?,?,?,?)',
                         (index, f'req-{index}', 'TaiwanStockNews', str(index), 'day', json.dumps(metadata)))
            conn.execute('INSERT INTO streams VALUES (?,?,?,?)',
                         ('TaiwanStockNews', str(index), 'day', checked.isoformat()))
    with sqlite3.connect(owner / 'queue.sqlite3') as conn:
        conn.execute('CREATE TABLE tasks(dataset TEXT,data_id TEXT,partition TEXT,state TEXT,last_attempt_at_utc TEXT,priority INTEGER)')
        for index in range(6):
            conn.execute('INSERT INTO tasks VALUES (?,?,?,?,?,?)',
                         ('TaiwanStockNews', str(index), 'day', 'complete',
                          (NOW - timedelta(minutes=index + 1)).isoformat(), 0))
    return owner


def test_cost_uses_own_completion_and_is_read_only(tmp_path):
    owner = observations(tmp_path)
    before = {path.name: path.read_bytes() for path in owner.iterdir()}
    result = json_service_statistics(tmp_path, NOW)
    assert result['pooled']['samples'] == result['priority']['samples'] == 6
    assert result['pooled']['rates']['central'] == 3600
    assert result['provider_calls'] == 0
    assert {path.name: path.read_bytes() for path in owner.iterdir()} == before


def test_old_checks_failed_receipts_and_later_completion_cannot_be_samples(tmp_path):
    owner = observations(tmp_path)
    with sqlite3.connect(owner / 'update_observations.sqlite3') as conn:
        conn.execute("UPDATE streams SET checked_at_utc=? WHERE data_id='0'", (NOW.isoformat(),))
    with sqlite3.connect(owner / 'queue.sqlite3') as conn:
        conn.execute("UPDATE tasks SET state='failed' WHERE data_id='1'")
        conn.execute("UPDATE tasks SET last_attempt_at_utc=? WHERE data_id='2'", (NOW.isoformat(),))
        conn.execute("UPDATE tasks SET last_attempt_at_utc=? WHERE data_id='3'", ((NOW - timedelta(hours=1)).isoformat(),))
        conn.execute("UPDATE tasks SET last_attempt_at_utc=? WHERE data_id='4'", ((NOW + timedelta(seconds=1)).isoformat(),))
    # Index 2 still fits the deliberately bounded 300-second store cost. It
    # cannot inflate service time by hours, and old stream 0 is excluded.
    result = json_service_statistics(tmp_path, NOW)
    assert result['pooled']['samples'] == 2


def test_missing_costs_do_not_create_or_migrate_databases(tmp_path):
    result = json_service_statistics(tmp_path, NOW)
    assert result['pooled']['samples'] == 0
    assert result['pooled']['rates']['central'] is None
    assert list(tmp_path.iterdir()) == []


def test_json_forecast_preserves_observed_traffic_but_does_not_project_object_idle():
    work, telemetry = inputs()
    telemetry['traffic']['windows']['1h'].update(attempts=82, wall_requests_per_hour=82)
    telemetry['traffic']['rate_distributions']['24h'].update(p50=82, p10=70, wall_p10=70)
    cost = {'samples': 6, 'rates': {'fastest': 4000, 'central': 3600, 'slowest': 3200}}
    telemetry['json_service_statistics'] = {'pooled': cost, 'priority': cost,
        'datasets': {'TaiwanFuturesKBar': {'samples': 1, 'rates': {'central': 1}}}}
    result = run(work, telemetry)
    stage = next(item for item in result['stages'] if item['key'] == 'tw_futures_minute')
    assert result['rate_evidence']['rolling_requests_60m'] == 82
    assert stage['rate_evidence']['request_processing']['sample_scope'] == 'pooled_json_proxy'
    assert stage['scenarios']['central']['estimated_complete_at_utc'] is not None
    assert stage['scenarios']['central']['effective_requests_per_hour'] > 82
    assert any('條件模型' in note for note in stage['assumptions'])


def test_actual_parallel_json_batch_rate_beats_per_response_proxy():
    work, telemetry = inputs()
    cost = {'samples': 32, 'rates': {'fastest': 20000, 'central': 18000, 'slowest': 17000}}
    telemetry['json_service_statistics'] = {'pooled': cost, 'datasets': {'TaiwanFuturesKBar': cost}}
    telemetry['json_pipeline_statistics'] = {'TaiwanFuturesKBar': {
        'samples': 4, 'capacity_basis': 'accepted_parallel_pipeline_wall',
        'rates': {'fastest': 2500, 'central': 2000, 'slowest': 1800}, 'download_workers': 8}}
    result = run(work, telemetry)
    stage = next(item for item in result['stages'] if item['key'] == 'tw_futures_minute')
    assert stage['rate_evidence']['request_processing']['sample_scope'] == 'dataset_current_configuration_full_batches'
    assert stage['scenarios']['central']['effective_requests_per_hour'] <= 2000


def test_public_projection_keeps_verified_json_capacity_identity(tmp_path):
    work, telemetry = inputs()
    telemetry['json_pipeline_statistics'] = {'TaiwanFuturesKBar': {
        'samples': 4, 'capacity_basis': 'accepted_parallel_pipeline_wall',
        'processing_engine': 'bounded_json_keepalive_single_publication_owner',
        'rates': {'fastest': 2500, 'central': 2000, 'slowest': 1800},
        'download_workers': 8, 'derivation_workers': 0, 'arrow_threads': 1,
        'full_cycle_wall_verified': True, 'measured_preemption_time_excluded': True,
        'full_owner_cycles': 2, 'sampled_requests': 480, 'sampled_wall_seconds': 864,
        'owner_cycle_wall_seconds': 900, 'private_provider_secret': 'must-not-reach-public'}}
    payload = run(work, telemetry)
    (tmp_path / 'eta_status.json').write_text(json.dumps({'schema_version': 1, 'estimate': payload}))
    public = public_completion_estimate(tmp_path, NOW)
    stage = next(item for item in public['stages'] if item['key'] == 'tw_futures_minute')
    cost = stage['rate_evidence']['request_processing']
    assert cost['sample_scope'] == 'dataset_current_configuration_full_batches'
    assert cost['processing_engine'] == 'bounded_json_keepalive_single_publication_owner'
    assert cost['basis'] == 'accepted_parallel_json_fetch_validate_publish_wall'
    assert cost['full_cycle_wall_verified'] and cost['full_owner_cycles'] == 2
    assert cost['sampled_requests'] == 480 and cost['sampled_wall_seconds'] == 864
    assert 'private_provider_secret' not in json.dumps(public)


@pytest.mark.parametrize('capability', [1, 2, True])
def test_known_semantic_capability_survives_new_numeric_version_only(tmp_path, capability):
    payload = run()
    payload['schema_version'] = 999
    payload['scenario_projection_contract'] = capability
    for child in payload['stages']:
        child['schema_version'] = 999
    (tmp_path / 'eta_status.json').write_text(json.dumps({'schema_version': 1, 'estimate': payload}))
    result = public_completion_estimate(tmp_path, NOW)
    if type(capability) is int and capability == 1:
        assert result['schema_version'] == 999
        assert result['stages'][0]['schema_version'] == 999
        assert result['scenarios']['central']['estimated_complete_at_utc'] is not None
    else:
        assert result['state'] == 'unavailable'
        assert result['scenarios']['central']['estimated_complete_at_utc'] is None
