from pathlib import Path
from types import SimpleNamespace
import json
import shutil
import subprocess
from datetime import UTC, datetime, timedelta
import os

import pytest

from scripts.serve_public_dashboards import (
    InvalidPublicRequest, PublicDashboardHandler, PublicDashboardServer, build_public_overview,
)


@pytest.fixture
def server(tmp_path):
    root = Path(__file__).resolve().parents[1]
    static = root / 'services'
    server = PublicDashboardServer(
        ('127.0.0.1', 0), repo_root=tmp_path,
        public_static_root=static/'public_dashboards', taifex_static_root=static/'taifex_dashboard',
        tw_static_root=static/'tw_day_trade_dashboard', shioaji_static_root=static/'shioaji_api_dashboard',
        openbb_static_root=static/'openbb_archive_dashboard',
        data_monitor_static_root=static/'data_monitor_dashboard', traffic_static_root=static/'traffic_dashboard',
        tej_static_root=static/'tej_dashboard', taifex_upstream='http://127.0.0.1:1', tw_upstream='http://127.0.0.1:1',
    )
    try:
        yield server
    finally:
        server.server_close()


def test_tej_assets_reuse_existing_gateway_and_read_only_projection(server):
    handler=SimpleNamespace(server=server)
    for route,needle in [('/tej/',b'TEJ Smart Wizard'),('/tej/app.js',b'api/features'),('/tej/styles.css',b'tej-toolbar')]:
        result=PublicDashboardHandler._static_response(handler,route)
        assert result is not None and needle in result.body
        assert result.cache_control==('public, max-age=31536000, immutable' if route.endswith('.css') else 'no-cache, must-revalidate')
    response=PublicDashboardHandler._api_response(handler,'/tej/api/status','')
    body=json.loads(response.body)
    assert body['state']=='not_registered' and body['raw_values_exposed'] is False
    assert response.cache_control=='no-store'
    assert PublicDashboardHandler._static_response(handler,'/data_tej/raw.json') is None


@pytest.mark.parametrize('query',['limit=100000','q=x&q=y','table=../../raw','phase=DROP','offset=-1','token=private','q=%00'])
def test_unbounded_or_private_queries_rejected(query):
    with pytest.raises(InvalidPublicRequest):PublicDashboardHandler._tej_feature_query(query)


def test_literal_query_and_pagination_contract():
    parsed=PublicDashboardHandler._tej_feature_query('q=ROI%25&offset=50&limit=50&phase=P1')
    assert parsed==dict(search='ROI%',offset=50,limit=50,phase='P1',table_id='')


def test_overview_only_includes_allowlisted_tej_metadata():
    result=build_public_overview({}, {}, {}, tej_status={
        'state':'queued','catalog':{'tables':255,'fields':45826,'catalog_scan_complete':True},
        'workload':{'exported_rows':123},'cells':[['private-value']], 'account_id':'private-account',
    })
    assert result['tej']['fields']==45826 and result['tej']['exported_rows']==123
    assert 'private' not in json.dumps(result)


def test_feature_request_coordinator_deduplicates_and_rejects_stale_pages():
    node = shutil.which('node')
    if node is None:
        pytest.skip('Node.js unavailable for frontend request semantics')
    script = Path(__file__).with_name('test_tej_dashboard_requests.mjs')
    result = subprocess.run([node,'--test',str(script)],capture_output=True,text=True,timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('mutation', [None, 'expired', 'pid_reused', 'stopped', 'invalid_contract', 'private_reason'])
def test_automatic_liveness_is_independent_bounded_and_public_allowlisted(tmp_path, mutation):
    from stockagent.live.tej_dashboard import _scheduler_public
    now = datetime.now(UTC)
    ticks = Path(f'/proc/{os.getpid()}/stat').read_text().rsplit(') ', 1)[1].split()[19]
    value = {'contract':'persistent_serial_evidence_preserving_supervision_v1', 'continuous':True,
             'state':'waiting_recovery', 'observed_at_utc':now.isoformat(),
             'deadline_at_utc':(now+timedelta(seconds=60)).isoformat(),
             'owner_pid':os.getpid(), 'owner_start_ticks':ticks,
             'paused_reason':'inflight_requires_recovery', 'cycles':1, 'completed_tasks':0,
             'private_path':'SECRET', 'account':'SECRET'}
    if mutation=='expired':value['deadline_at_utc']=(now-timedelta(seconds=1)).isoformat()
    if mutation=='pid_reused':value['owner_start_ticks']='different'
    if mutation=='stopped':value['state']='stopped'
    if mutation=='invalid_contract':value['contract']='other'
    if mutation=='private_reason':value['paused_reason']='SECRET'
    (tmp_path/'scheduler_status.json').write_text(json.dumps(value))
    public = _scheduler_public(tmp_path, now)
    assert public['alive'] == (mutation in {None, 'private_reason'})
    assert 'SECRET' not in json.dumps(public) and 'owner_pid' not in public
    assert public['unknown_outcome_auto_retry'] is False
    if mutation=='private_reason':assert public['paused_reason'] is None


@pytest.mark.parametrize('state', ['starting','executing','between_tasks','waiting_queue','waiting_storage',
    'waiting_owner','waiting_desktop','waiting_local_retry','waiting_recovery',None,'unreviewed'])
def test_estimate_execution_state_distinguishes_waiting_from_running(state):
    from stockagent.live.tej_dashboard import _automatic_execution_state
    expected=('automatic_running' if state in {'executing','between_tasks'} else
              'automatic_waiting_recovery' if state=='waiting_recovery' else 'automatic_waiting')
    assert _automatic_execution_state(state)==expected
