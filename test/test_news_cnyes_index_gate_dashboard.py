"""The index phase's forecast must reflect its actual request-scope gates."""
from datetime import timedelta
import time

import pytest

from test_news_cnyes_index_failure_scope import cfg, fail_page, queue, scope_key
from test_news_cnyes_public_index import accept, index_payload
from test_news_dashboard_cnyes_manifest_phase import NOW, cnyes, fixture, save


def control():
    return {'temporarily_paused_gateway_hosts': {}, 'unverified_gateway_hosts': {},
            'temporarily_paused_robots_origins': {}, 'unverified_robots_origins': {},
            'unverified_robots_origins_overflow': False}


def status_gate(queue, gates):
    queue.add_articles([{'url': 'https://news.cnyes.com/news/id/123'}])
    queue.http_capacity_snapshot = lambda: {'providers': {'cnyes': gates}}
    status = queue.status(activity='running', publish=False)
    return status['providers']['cnyes']['public_index_progress']['source_gate']


def test_index_endpoint_gate_persists_until_real_recovery(queue):
    fail_page(queue, 'http_503')
    before = [tuple(row) for row in queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns')]
    gate = status_gate(queue, control())
    assert gate['state'] == 'waiting' and gate['next_probe_at_utc']
    assert [tuple(row) for row in queue.db.execute('SELECT * FROM bulk_endpoint_cooldowns')] == before
    with queue.db:
        queue.db.execute('UPDATE bulk_endpoint_cooldowns SET not_before=?', (time.time() - 1,))
    assert status_gate(queue, control())['state'] == 'rechecking'
    with queue.db:
        queue.db.execute('DELETE FROM bulk_endpoint_cooldowns')  # Synthetic successful retirement.
    assert status_gate(queue, control())['state'] == 'clear'


def test_verified_single_page_error_does_not_freeze_other_pages(queue):
    fail_page(queue)
    gates = control()
    assert status_gate(queue, gates)['state'] == 'clear'
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?',
                         ('{}', scope_key(queue.registry.sources['cnyes'])))
    assert status_gate(queue, gates)['state'] == 'waiting'


@pytest.mark.parametrize('active,unverified,target', [
    ('temporarily_paused_gateway_hosts', 'unverified_gateway_hosts', 'api.cnyes.com'),
    ('temporarily_paused_robots_origins', 'unverified_robots_origins', 'https://api.cnyes.com'),
])
def test_exact_index_host_and_origin_gates_are_not_provider_wide(queue, active, unverified, target):
    gates = control()
    deadline = time.time() + 120
    gates[active]['unrelated.cnyes.com'] = deadline
    gates[unverified]['unrelated.cnyes.com'] = deadline
    assert status_gate(queue, gates)['state'] == 'clear'
    gates[active][target] = gates[unverified][target] = deadline
    assert status_gate(queue, gates)['state'] == 'waiting'
    del gates[active][target]
    gates[unverified][target] = time.time() - 1
    assert status_gate(queue, gates)['state'] == 'rechecking'
    del gates[unverified][target]
    assert status_gate(queue, gates)['state'] == 'clear'


def test_incomplete_http_scope_is_unknown_not_a_clear_recovery(queue):
    gates = control()
    gates['unverified_robots_origins_overflow'] = True
    assert status_gate(queue, gates)['state'] == 'unknown'
    deadline = time.time() + 120
    gates['temporarily_paused_gateway_hosts']['api.cnyes.com'] = deadline
    gates['unverified_gateway_hosts']['api.cnyes.com'] = deadline
    assert status_gate(queue, gates)['state'] == 'waiting'  # Known wait outranks unknown robots state.
    gates = control()
    del gates['unverified_gateway_hosts']
    assert status_gate(queue, gates)['state'] == 'unknown'


@pytest.mark.parametrize('state,expected', [
    ('waiting', 'provider_waiting'), ('rechecking', 'source_rechecking'),
    ('unknown', 'source_scope_unknown'), ('clear', 'recent_measured'),
])
def test_index_gate_reaches_public_phase_without_exposing_private_values(tmp_path, state, expected):
    status, _ = fixture(tmp_path)
    status['providers']['cnyes']['public_index_progress']['source_gate'] = {
        'contract_version': 1, 'state': state,
        'next_probe_at_utc': (NOW + timedelta(minutes=2)).isoformat() if state == 'waiting' else None,
        'source_recovery_requires_success': True, 'private_url': 'PRIVATE_TOKEN'}
    phase = cnyes(save(tmp_path, status))['manifest_phase_progress']
    assert phase['forecast']['state'] == expected
    if state != 'clear':
        assert phase['forecast']['middle_finish_at_utc'] is None
    assert 'PRIVATE_TOKEN' not in str(phase)


def test_index_wait_expiry_and_malformed_gate_do_not_prove_recovery(tmp_path):
    status, _ = fixture(tmp_path)
    index = status['providers']['cnyes']['public_index_progress']
    index['source_gate'] = {'contract_version': 1, 'state': 'waiting',
        'next_probe_at_utc': (NOW - timedelta(seconds=1)).isoformat(),
        'source_recovery_requires_success': True}
    phase = cnyes(save(tmp_path, status))['manifest_phase_progress']
    assert phase['source_gate']['state'] == 'rechecking'
    assert phase['forecast']['state'] == 'source_rechecking'
    index['source_gate']['contract_version'] = True
    phase = cnyes(save(tmp_path, status))['manifest_phase_progress']
    assert phase['source_gate']['state'] == 'unknown'
    assert phase['forecast']['state'] == 'source_scope_unknown'


def test_legacy_snapshot_without_index_gate_keeps_counts_but_not_forecast(tmp_path):
    status, _ = fixture(tmp_path)
    status['providers']['cnyes']['public_index_progress'].pop('source_gate', None)
    phase = cnyes(save(tmp_path, status))['manifest_phase_progress']
    assert phase['verified_pages'] == 340 and phase['progress_ratio'] == .34
    assert phase['forecast']['state'] == 'source_scope_unknown'
    assert phase['forecast']['middle_finish_at_utc'] is None


def test_unavailable_index_gate_does_not_erase_proven_phase_completion(tmp_path):
    status, phase = fixture(tmp_path)
    phase.update(source_reported_total_pages=340, source_reported_total_metadata_rows=10200,
                 remaining_pages=0, progress_ratio=1., phase_complete=True)
    status['providers']['cnyes']['public_index_progress']['source_gate'] = {
        'contract_version': 1, 'state': 'unknown', 'next_probe_at_utc': None,
        'source_recovery_requires_success': True}
    result = cnyes(save(tmp_path, status))['manifest_phase_progress']
    assert result['phase_complete'] and result['forecast']['state'] == 'phase_complete'
    assert not result['all_history_complete'] and not result['body_completion_claimed']


def test_real_queue_gate_drives_public_phase_wait_and_recovery(queue, tmp_path):
    fail_page(queue, 'http_503')
    status, _ = fixture(tmp_path)
    index = status['providers']['cnyes']['public_index_progress']
    index['source_gate'] = status_gate(queue, control())
    assert cnyes(save(tmp_path, status))['manifest_phase_progress']['forecast']['state'] == 'provider_waiting'
    with queue.db:
        queue.db.execute('UPDATE bulk_endpoint_cooldowns SET not_before=?', (time.time() - 1,))
    index['source_gate'] = status_gate(queue, control())
    assert cnyes(save(tmp_path, status))['manifest_phase_progress']['forecast']['state'] == 'source_rechecking'
    _, _, accepted = accept(queue, index_payload())  # Simulated response through canonical parser/receipt path.
    assert accepted == 0  # Metadata is not a stored article body.
    assert queue.db.execute('SELECT count(*) FROM bulk_endpoint_cooldowns').fetchone()[0] == 0
    index['source_gate'] = status_gate(queue, control())
    assert cnyes(save(tmp_path, status))['manifest_phase_progress']['forecast']['state'] == 'recent_measured'


@pytest.mark.parametrize('state,expected', [
    ('waiting', 'last_page_waiting'), ('rechecking', 'last_page_rechecking'),
    ('unknown', 'last_page_scope_unknown'), ('ready', 'recent_measured'),
])
def test_final_page_job_is_a_phase_gate_not_a_provider_gate(tmp_path, state, expected):
    status, phase = fixture(tmp_path)
    phase.update(source_reported_total_pages=341, source_reported_total_metadata_rows=10201,
                 remaining_pages=1, progress_ratio=340 / 341)
    phase['forecast'].update(fastest_seconds=60 / 11, middle_seconds=15,
                             middle_finish_at_utc=(NOW + timedelta(seconds=15)).isoformat())
    phase['last_page_gate'] = {'contract_version': 1, 'state': state,
        'next_probe_at_utc': (NOW + timedelta(minutes=2)).isoformat() if state == 'waiting' else None,
        'recovery_requires_success': True, 'private_url': 'PRIVATE_FINAL_PAGE'}
    result = cnyes(save(tmp_path, status))['manifest_phase_progress']
    assert result['source_gate']['state'] == 'clear'
    assert result['forecast']['state'] == expected
    assert result['verified_pages'] == 340 and result['remaining_pages'] == 1
    if state != 'ready':
        assert result['forecast']['middle_finish_at_utc'] is None
    assert 'PRIVATE_FINAL_PAGE' not in str(result)


def test_final_page_expiry_and_legacy_snapshots_do_not_invent_a_recovery(tmp_path):
    status, phase = fixture(tmp_path)
    phase.update(source_reported_total_pages=341, source_reported_total_metadata_rows=10201,
                 remaining_pages=1, progress_ratio=340 / 341)
    phase['last_page_gate'] = {'contract_version': 1, 'state': 'waiting',
        'next_probe_at_utc': (NOW - timedelta(seconds=1)).isoformat(), 'recovery_requires_success': True}
    result = cnyes(save(tmp_path, status))['manifest_phase_progress']
    assert result['last_page_gate']['state'] == 'rechecking'
    assert result['forecast']['state'] == 'last_page_rechecking'
    phase.pop('last_page_gate')
    result = cnyes(save(tmp_path, status))['manifest_phase_progress']
    assert result['last_page_gate']['state'] == 'unknown'
    assert result['forecast']['state'] == 'last_page_scope_unknown'


def test_actual_queue_final_page_failure_projects_until_success(queue, tmp_path):
    from datetime import datetime, timezone
    from downloader.artifact_io import atomic_write_json
    from downloader.news_fulltext import fail_discovery
    from downloader.news_fulltext_cnyes_index import progress, source_gate_progress
    from downloader.news_fulltext_policy import NewsError
    from stockagent.live.news_dashboard import build_news_public_status
    from test_news_cnyes_index_manifest_schedule import add_gap
    from test_news_cnyes_manifest_phase import page
    from test_news_cnyes_public_index import DAY, register

    with queue.db:
        add_gap(queue, DAY)
    for number in range(1, 12):
        page(queue, number=number, total=360)
    task = register(queue, page=12)
    fail_discovery(queue, task, NewsError('cnyes_index_unverified_metadata', retry=True, delay=3600))
    source = queue.registry.sources['cnyes']

    def render(epoch):
        now = datetime.fromtimestamp(epoch, timezone.utc)
        status, _ = fixture(tmp_path, now=now)
        index = progress(queue.db, source, owner_start_epoch=epoch - 600, now_epoch=epoch, activity='running')
        index['source_gate'] = source_gate_progress(queue.db, source, control(), now_epoch=epoch)
        status['providers']['cnyes']['public_index_progress'] = index
        atomic_write_json(tmp_path / 'data_news_fulltext/status.json', status)
        return cnyes(build_news_public_status(tmp_path, now=now))['manifest_phase_progress']

    observed = time.time()
    waiting = render(observed)
    assert waiting['source_gate']['state'] == 'clear'
    assert waiting['forecast']['state'] == 'last_page_waiting'
    deadline = datetime.fromisoformat(waiting['last_page_gate']['next_probe_at_utc']).timestamp()
    assert render(deadline + 1)['forecast']['state'] == 'last_page_rechecking'
    _, _, body_count = page(queue, number=12, total=360)  # Simulated source and real receipt path.
    complete = render(time.time())
    assert body_count == 0 and complete['phase_complete']
    assert complete['forecast']['state'] == 'phase_complete'
    assert not complete['body_completion_claimed'] and not complete['all_history_complete']


@pytest.mark.parametrize('state,failed,expected', [
    ('waiting', 2, 'partial_page_waiting'), ('rechecking', 1, 'partial_page_rechecking'),
    ('unknown', None, 'partial_page_scope_unknown'), ('none_observed', 0, 'recent_measured'),
])
def test_partial_page_failure_is_projected_without_exposing_a_url(tmp_path, state, failed, expected):
    status, phase = fixture(tmp_path)
    phase['partial_page_gate'] = {'contract_version': 1, 'state': state, 'failed_pages': failed,
        'next_probe_at_utc': (NOW + timedelta(minutes=3)).isoformat() if state == 'waiting' else None,
        'recovery_requires_success': True, 'scope': 'observed_request_local_failures_only',
        'private_url': 'PRIVATE_PARTIAL_PAGE'}
    result = cnyes(save(tmp_path, status))['manifest_phase_progress']
    assert result['forecast']['state'] == expected
    assert result['source_gate']['state'] == 'clear'
    if state != 'none_observed':
        assert result['forecast']['middle_finish_at_utc'] is None
    assert 'PRIVATE_PARTIAL_PAGE' not in str(result)


def test_partial_gate_expiry_and_old_snapshot_do_not_invent_recovery(tmp_path):
    status, phase = fixture(tmp_path)
    phase['partial_page_gate'].update(state='waiting', failed_pages=1,
        next_probe_at_utc=(NOW - timedelta(seconds=1)).isoformat())
    result = cnyes(save(tmp_path, status))['manifest_phase_progress']
    assert result['partial_page_gate']['state'] == 'rechecking'
    assert result['forecast']['state'] == 'partial_page_rechecking'
    phase.pop('partial_page_gate')
    result = cnyes(save(tmp_path, status))['manifest_phase_progress']
    assert result['partial_page_gate']['state'] == 'unknown'
    assert result['forecast']['state'] == 'partial_page_scope_unknown'
    assert result['verified_pages'] == 340


@pytest.mark.parametrize('change', [
    {'state': 'none_observed', 'failed_pages': 1}, {'state': 'rechecking', 'failed_pages': 0},
    {'state': 'waiting', 'failed_pages': True}, {'state': 'waiting', 'failed_pages': 661},
    {'state': 'waiting', 'failed_pages': 1, 'next_probe_at_utc': None},
    {'contract_version': True}, {'scope': 'all_pages_verified'}, {'recovery_requires_success': False},
])
def test_malformed_partial_gate_never_becomes_a_ready_forecast(tmp_path, change):
    status, phase = fixture(tmp_path)
    phase['partial_page_gate'].update(change)
    result = cnyes(save(tmp_path, status))['manifest_phase_progress']
    assert result['partial_page_gate']['state'] == 'unknown'
    assert result['forecast']['state'] == 'partial_page_scope_unknown'
    assert result['forecast']['middle_finish_at_utc'] is None


def test_real_multi_page_failure_drives_public_status_without_hiding_healthy_work(queue, tmp_path):
    from datetime import datetime, timezone
    from downloader.artifact_io import atomic_write_json
    from downloader.news_fulltext import fail_discovery
    from downloader.news_fulltext_cnyes_index import progress, source_gate_progress
    from downloader.news_fulltext_policy import NewsError
    from stockagent.live.news_dashboard import build_news_public_status
    from test_news_cnyes_index_manifest_schedule import add_gap
    from test_news_cnyes_manifest_phase import page
    from test_news_cnyes_public_index import DAY, register

    with queue.db:
        add_gap(queue, DAY)
    for number in range(1, 13):
        page(queue, number=number, total=450)
    failed, healthy = register(queue, page=13), register(queue, page=15)
    fail_discovery(queue, failed, NewsError('cnyes_index_unverified_metadata', retry=True, delay=3600))
    source = queue.registry.sources['cnyes']

    def render(epoch):
        now = datetime.fromtimestamp(epoch, timezone.utc)
        status, _ = fixture(tmp_path, now=now)
        index = progress(queue.db, source, owner_start_epoch=epoch - 600, now_epoch=epoch, activity='running')
        index['source_gate'] = source_gate_progress(queue.db, source, control(), now_epoch=epoch)
        status['providers']['cnyes']['public_index_progress'] = index
        atomic_write_json(tmp_path / 'data_news_fulltext/status.json', status)
        return cnyes(build_news_public_status(tmp_path, now=now))['manifest_phase_progress']

    observed = time.time()
    waiting = render(observed)
    assert waiting['source_gate']['state'] == 'clear'
    assert waiting['forecast']['state'] == 'partial_page_waiting'
    assert healthy['url'] in [job['url'] for job in queue.discovery_candidates(source, 20)]
    deadline = datetime.fromisoformat(waiting['partial_page_gate']['next_probe_at_utc']).timestamp()
    assert render(deadline + 1)['forecast']['state'] == 'partial_page_rechecking'
    page(queue, number=13, total=450)
    recovered = render(time.time())
    assert recovered['partial_page_gate']['state'] == 'none_observed'
    assert recovered['forecast']['state'] not in {'partial_page_waiting', 'partial_page_rechecking'}
    assert recovered['remaining_pages'] == 2 and not recovered['body_completion_claimed']
