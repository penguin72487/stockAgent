from contextlib import closing
from datetime import UTC, datetime, timedelta
import json
from types import SimpleNamespace

from downloader.artifact_io import atomic_write_json
from downloader.tej_history import connect, configure_preview_planning, run_one, task_request
from downloader.tej_incremental import CONTRACT, refill_wizard, seed_api, seed_wizard
from downloader.tej_api import connect as api_connect
from downloader.download_tej_api import _partition_at_cap
import test_tej_planning as fixtures
from test_tej_planning import registry  # Reuse the canonical source/receipt fixture.

SETTINGS = {'contract': CONTRACT, 'enabled': True, 'check_hour_taipei': 19, 'api_snapshot_days': 7,
            'overlap_days': {'daily': 3, 'weekly': 8, 'monthly': 35, 'quarterly': 120, 'yearly': 370, 'default': 3}}
NOW = datetime(2014, 1, 9, 12, tzinfo=UTC)


def test_wizard_tail_finishes_without_replacing_unfinished_history_and_follows_native_receipts(registry, monkeypatch):
    root, original = registry
    config = {**original, 'incremental': SETTINGS}
    configure_preview_planning(root, original)
    bridge = fixtures.DynamicBridge()
    assert run_one(root, bridge) == 'completed_task'  # Original discovery.
    assert run_one(root, bridge) == 'completed_task'  # Only one of five history queries.
    with closing(connect(root)) as con, con:
        baseline = tuple(con.execute('SELECT plan_json,next_query,total_queries FROM download_plans').fetchone())
        old_axes = tuple(con.execute('SELECT discovery_path,grid_rows,grid_dates FROM tables').fetchone())
        assert seed_wizard(con, config, NOW) == 1
        assert seed_wizard(con, config, NOW) == 0
        request = task_request(root, dict(con.execute("SELECT * FROM tasks WHERE kind='discover' AND state='pending'").fetchone()), connection=con)
        assert request['end'] == '2014-01-09' and request['incremental_contract'] == CONTRACT
    companies = fixtures.axes()[0]
    monkeypatch.setattr(fixtures, 'axes', lambda: (companies, ['2014/01/09']))
    assert run_one(root, bridge) == 'completed_task'  # Incremental discovery.
    with closing(connect(root)) as con:
        queries = con.execute('SELECT total_queries FROM tej_incremental_cycles').fetchone()[0]
        assert queries > 0
    for _ in range(queries):
        assert run_one(root, bridge) == 'completed_task'
    with closing(connect(root)) as con, con:
        seed_wizard(con, config, NOW + timedelta(seconds=61))
        assert con.execute('SELECT state FROM tej_incremental_cycles').fetchone()[0] == 'checked'
        after = tuple(con.execute('SELECT plan_json,next_query,total_queries FROM download_plans').fetchone())
        assert after[0] == baseline[0] and after[2] == baseline[2]
        # The canonical lazy history owner may prepare its next pending
        # rectangle; no original plan is replaced, rewound or marked finished.
        assert baseline[1] <= after[1] <= baseline[1] + 1
        assert con.execute("SELECT COUNT(*) FROM tasks WHERE kind='download' AND state='complete' "
                           "AND task_id NOT IN (SELECT task_id FROM tej_incremental_tasks)").fetchone()[0] == 1
        assert tuple(con.execute('SELECT discovery_path,grid_rows,grid_dates FROM tables').fetchone()) == old_axes
        assert con.execute("SELECT value FROM meta WHERE key='cutoff'").fetchone()[0] == '2014-01-07'
        assert seed_wizard(con, config, NOW + timedelta(days=1)) == 1


def test_wizard_unknown_and_split_child_prevent_false_completion(registry):
    root, config = registry
    configure_preview_planning(root, config)
    bridge = fixtures.DynamicBridge()
    run_one(root, bridge)
    with closing(connect(root)) as con, con:
        seed_wizard(con, {**config, 'incremental': SETTINGS}, NOW)
        cycle = con.execute('SELECT cycle_id,discovery_id FROM tej_incremental_cycles').fetchone()
        con.execute("UPDATE tasks SET state='complete' WHERE task_id=?", (cycle['discovery_id'],))
        con.execute("UPDATE tej_incremental_cycles SET plan_json='{}',state='downloading',next_query=1,total_queries=1")
        request = json.dumps({'incremental_cycle': cycle['cycle_id']})
        for identity, state in [('parent', 'superseded_selector_geometry_v1'), ('child', 'blocked')]:
            con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,state,request_json,scope_contract) "
                        "VALUES(?,'t','download','P1',-1000,?,?,'editable_source_scope_v1')", (identity, state, request))
        seed_wizard(con, {**config, 'incremental': SETTINGS}, NOW + timedelta(seconds=61))
        assert con.execute('SELECT state FROM tej_incremental_cycles').fetchone()[0] == 'downloading'
        con.execute("UPDATE tasks SET state='complete' WHERE task_id='child'")
        seed_wizard(con, {**config, 'incremental': SETTINGS}, NOW + timedelta(seconds=122))
        assert con.execute('SELECT state FROM tej_incremental_cycles').fetchone()[0] == 'checked'


def test_check_clock_on_closed_days_and_disabled_mode(registry):
    root, config = registry
    configure_preview_planning(root, config)
    run_one(root, fixtures.DynamicBridge())
    with closing(connect(root)) as con, con:
        assert seed_wizard(con, config, NOW) == 0
        assert seed_wizard(con, {**config, 'incremental': SETTINGS}, datetime(2014, 1, 8, 10, tzinfo=UTC)) == 0
        assert seed_wizard(con, {**config, 'incremental': SETTINGS}, datetime(2014, 1, 11, 12, tzinfo=UTC)) == 1
        assert con.execute('SELECT end_day FROM tej_incremental_cycles').fetchone()[0] == '2014-01-11'


def test_api_tail_is_independent_of_history_quota_and_tracks_capped_children(tmp_path):
    root = tmp_path / 'api'
    now = datetime(2026, 10, 27, 12, tzinfo=UTC)
    api = SimpleNamespace(root=root, now=lambda: now)
    with closing(api_connect(root)) as con, con:
        con.execute("INSERT INTO tasks(id,table_id,params_json,priority,state) VALUES('history','TRAIL/TAPRCD','{}',10,'pending')")
    atomic_write_json(root / 'scope_inventory.json', {'tables': [
        {'table_id': 'TRAIL/TAPRCD', 'frequency': 'daily', 'access_state': 'verified_nonempty', 'last_observed': '2026-10-01'},
        {'table_id': 'TRAIL/AIND', 'frequency': 'current_snapshot', 'access_state': 'verified_nonempty', 'last_observed': '2026-10-01'}]})
    assert seed_api(api, SETTINGS) == 2
    assert seed_api(api, SETTINGS) == 0
    with closing(api_connect(root)) as con, con:
        parent = dict(con.execute("SELECT * FROM tasks WHERE table_id='TRAIL/TAPRCD' AND id<>'history'").fetchone())
        assert json.loads(parent['params_json'])['mdate.gte'] == '2026-10-02'
        outcome = _partition_at_cap(con, parent, {}, api)
        assert outcome == 'superseded_disjoint_partition'
        con.execute('UPDATE tasks SET state=? WHERE id=?', (outcome, parent['id']))
        assert con.execute('SELECT COUNT(*) FROM api_incremental_tasks WHERE cycle_id=(SELECT cycle_id FROM api_incremental_tasks WHERE task_id=?)', (parent['id'],)).fetchone()[0] == 3
    assert seed_api(api, SETTINGS) == 0
    with closing(api_connect(root)) as con, con:
        assert con.execute("SELECT state FROM api_incremental_cycles WHERE table_id='TRAIL/TAPRCD'").fetchone()[0] == 'pending'
        con.execute("UPDATE tasks SET state='complete' WHERE id IN (SELECT task_id FROM api_incremental_tasks) AND state='pending'")
    assert seed_api(api, SETTINGS) == 0
    with closing(api_connect(root)) as con:
        assert {row[0] for row in con.execute('SELECT state FROM api_incremental_cycles')} == {'checked'}
        assert con.execute("SELECT state FROM tasks WHERE id='history'").fetchone()[0] == 'pending'
