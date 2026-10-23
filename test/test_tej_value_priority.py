from contextlib import closing
import copy
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
import sqlite3

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.tej_history import connect, _ready_task
from downloader.tej_value_priority import (
    CONTRACT, PRIORITY_WRITE_CONTRACT, _priority_writes_only,
    coverage_hint, feature_value, installed_priority, install_priorities,
    rank_feature, refresh_if_due, summarize_table, validate_policy,
)


@pytest.fixture
def policy():
    return json.loads((Path(__file__).parents[1] / "configs/tej_value_priority.json").read_text())


def feature(**kwargs):
    return {"table": "Future DB", "category": "futures", "field": "DSP", "field_index": 0,
            "local_match_ids": [], "same_family_cadence_match_ids": [], **kwargs}


def local(first="2010-01-01", last="2026-10-02", count=100):
    return {"first": first, "last": last, "non_null_count": count}


def snapshot():
    now = datetime.now(UTC)
    return {"contract": CONTRACT, "fingerprint": "test", "generated_at_utc": now.isoformat(),
            "next_refresh_at_utc": (now + timedelta(minutes=15)).isoformat(),
            "tables": [{"channel": c, "table_id": tid, "priority": p, "rank": i,
                        "value_score": 100 - p // 100, "missing_or_gap_fields": 1,
                        "crosscheck_candidate_fields": 0, "value_basis": "test"}
                       for i, (c, tid, p) in enumerate([
                           ("smart_wizard", "risk", 0), ("smart_wizard", "fund", 5500),
                           ("api", "TRAIL/TAOPTION", 500), ("api", "TRAIL/TANAV", 5500)], 1)]}


def test_gap_precedes_even_high_value_complete_concept(policy):
    high = feature(local_match_ids=["x"], same_family_cadence_match_ids=["x"])
    seen = rank_feature(high, {"x": local()}, policy)
    missing = rank_feature(feature(table="Fund's NAV", category="funds_bonds", field="NAV"), {}, policy)
    assert missing["priority"] < seen["priority"]
    assert seen["coverage_is_complete"] is False and seen["exclusion_enabled_by_ranking"] is False


def test_isolated_installer_benchmark_verifies_all_source_columns_and_order(tmp_path):
    from scripts.benchmark_tej_value_priority import benchmark
    report = benchmark(tmp_path/'abba.json', tasks=20, payload_bytes=32)
    assert report['accepted'] and report['source_and_priority_parity']
    assert report['fixture_is_synthetic'] and report['production_database_read'] is False
    assert report['provider_queries_sent'] == 0 and len(report['samples']) == 8
    for channel in ('smart_wizard', 'api'):
        samples = [r for r in report['samples'] if r['channel'] == channel]
        assert [r['variant'] for r in samples] == ['baseline', 'candidate', 'candidate', 'baseline']
        assert len({r['source_columns_sha256'] for r in samples}) == 1
        assert len({r['priority_columns_sha256'] for r in samples}) == 1
    with pytest.raises(FileExistsError):
        benchmark(tmp_path/'abba.json')


def test_high_value_gap_before_cheap_auxiliary_gap(policy):
    risk = rank_feature(feature(table="Contract Adjustment of Stock Futures", field="Shares per Contract"), {}, policy)
    fund = rank_feature(feature(table="Fund's Dividends", category="corporate_actions", field="Dividend"), {}, policy)
    assert risk["priority"] == 0 < fund["priority"]
    assert fund["value_score"] <= 45


@pytest.mark.parametrize("count", [None, 0])
def test_schema_or_all_null_is_not_acquired(count):
    row = feature(local_match_ids=["x"], same_family_cadence_match_ids=["x"])
    assert coverage_hint(row, {"x": local(count=count)}, start="2014-01-01")[0] == 0


def test_cannot_combine_two_partial_sources_into_complete_interval():
    row = feature(local_match_ids=["x", "y"], same_family_cadence_match_ids=["x", "y"])
    assert coverage_hint(row, {"x": local(last="2015-01-01"), "y": local(first="2020-01-01")},
                         start="2014-01-01", end="2025-01-01")[0] == 0


def test_wrong_grain_or_market_is_unknown_not_duplicate():
    row = feature(local_match_ids=["x"], same_family_cadence_match_ids=[])
    assert coverage_hint(row, {"x": local()}, start="2014-01-01")[0] == 0


def test_trial_cannot_repair_out_of_entitlement_years(policy):
    row = feature(local_match_ids=["x"], same_family_cadence_match_ids=["x"], actual_first="2025-01-02T00:00:00Z")
    result = rank_feature(row, {"x": local(first="2020-01-01")}, policy, end="2025-12-31")
    assert result["acquisition_stage"] == "crosscheck_candidate"


def test_value_is_not_schema_key_and_old_history_is_retained(policy):
    assert feature_value(feature(field="證券代碼", source_field="coid"), policy)[0] <= 25
    assert feature_value(feature(table="Delisted Option(2010~2013)", category="options"), policy)[0] <= 50
    assert feature_value(feature(field="ROI%-Ln"), policy)[0] <= 20
    assert feature_value(feature(table="IFRS Qly", category="financial", field="Announcement date"), policy)[0] > 86


def test_best_missing_field_drives_native_batch_not_context_or_crosscheck(policy):
    observed = rank_feature(feature(local_match_ids=["x"], same_family_cadence_match_ids=["x"]), {"x": local()}, policy)
    missing = rank_feature(feature(field="other", field_index=1), {}, policy)
    table = summarize_table("smart_wizard", {"table_id": "t", "table": "Future DB", "category": "futures"}, [observed, missing])
    assert table["priority"] == missing["priority"]
    assert table["missing_or_gap_fields"] == 1 and table["crosscheck_candidate_fields"] == 1


@pytest.mark.parametrize("bad", [True, -1, 101, "80"])
def test_invalid_scores_rejected(policy, bad):
    policy["family_values"]["futures"] = bad
    with pytest.raises(ValueError):
        validate_policy(policy)


def test_wizard_install_preserves_scopes_states_and_explicit_finite_repairs(tmp_path):
    with closing(connect(tmp_path)) as con, con:
        for name, table, kind, state, priority in [
            ("r", "risk", "download", "running", 100),
            ("b", "risk", "download", "blocked", 100),
            ("c", "risk", "download", "complete", 100),
            ("d", "risk", "discover", "pending", 101),
            ("f", "fund", "download", "pending", 100),
            ("e", "fund", "download", "pending", -20)]:
            con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,state) VALUES(?,?,?,'P1',?,'{\"original\":true}',?)", (name, table, kind, priority, state))
        before = list(con.execute("SELECT task_id,state,request_json FROM tasks ORDER BY task_id"))
        install_priorities(con, snapshot(), "smart_wizard")
        assert [tuple(r) for r in before] == [tuple(r) for r in con.execute("SELECT task_id,state,request_json FROM tasks ORDER BY task_id")]
        assert dict(con.execute("SELECT task_id,priority FROM tasks")) == {"r": 100, "b": 100, "c": 100, "d": 1, "f": 5500, "e": -20}
        assert installed_priority(con, "fund", 100) == 5500
        assert installed_priority(con, "risk", 101, kind="discover") == 1
        assert installed_priority(con, "fund", -20) == -20


def test_value_dispatch_overrides_old_phase_alphabet_and_preserves_retry_gates(tmp_path):
    with closing(connect(tmp_path)) as con, con:
        for name, table, kind, phase, priority in [
            ("low", "fund", "download", "P1", 100),
            ("high", "risk", "discover", "P2", 201)]:
            con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json) VALUES(?,?,?,?,?,'{}')", (name, table, kind, phase, priority))
        install_priorities(con, snapshot(), "smart_wizard")
        assert _ready_task(con, "2099", None, {"download_burst": 4})["task_id"] == "high"
        con.execute("UPDATE tasks SET next_attempt_at_utc='9999' WHERE task_id='high'")
        assert _ready_task(con, "2099", None, {"download_burst": 4})["task_id"] == "low"


def test_api_partial_cursor_and_paid_pages_are_never_discarded(tmp_path):
    from downloader.tej_api import connect as api_connect
    with closing(api_connect(tmp_path)) as con, con:
        con.execute("INSERT INTO tasks(id,table_id,params_json,priority,state,cursor,query_rows,actual_rows,page_size) VALUES('paid','TRAIL/TANAV','{}',0,'pending','same-token',20000,20000,10000)")
        con.execute("INSERT INTO tasks(id,table_id,params_json,priority,state) VALUES('new','TRAIL/TAOPTION','{}',2,'pending')")
        install_priorities(con, snapshot(), "api")
        row = con.execute("SELECT cursor,query_rows,actual_rows,page_size,state,priority FROM tasks WHERE id='paid'").fetchone()
        assert tuple(row) == ('same-token', 20000, 20000, 10000, 'pending', 5500)
        assert con.execute("SELECT id FROM tasks WHERE state='pending' ORDER BY priority,id LIMIT 1").fetchone()[0] == "new"


def test_no_installed_policy_keeps_canonical_legacy_order(tmp_path):
    with closing(connect(tmp_path)) as con:
        assert installed_priority(con, "fund", 100) == 100
    assert refresh_if_due(tmp_path) is None


def test_new_api_seed_uses_installed_value_not_snapshot_or_size_default(tmp_path):
    from types import SimpleNamespace
    from downloader.tej_api import connect as api_connect
    from downloader.download_tej_api import seed_queue
    with closing(api_connect(tmp_path)) as con, con:
        install_priorities(con, snapshot(), 'api')
    catalog = {'tables': [dict(tableId=tid, access_state='verified_nonempty', frequency='daily',
                             actual_first='2025-01-02', actual_last='2025-12-31', rowCount=100)
                          for tid in ('TRAIL/TAOPTION', 'TRAIL/TANAV')]}
    assert seed_queue(SimpleNamespace(root=tmp_path), catalog)['tasks_inserted'] == 2
    with closing(api_connect(tmp_path)) as con:
        assert dict(con.execute('SELECT table_id,priority FROM tasks')) == {'TRAIL/TAOPTION': 500, 'TRAIL/TANAV': 5500}


def test_new_lazy_wizard_query_inherits_value_without_scope_fingerprint_change(tmp_path):
    from downloader.tej_planning import build_plan, CONTRACT as PLAN, refill_ready_plans, next_request
    from downloader.tej_history import stable_id
    req = dict(fields=['OI'], catalog_fields=['OI'], type='Future Index (ALL)', smart_id='TEJ Derivatives',
               table='Future DB', max_rows=100, max_cells=1000, start='2025-01-02', end='2025-01-02')
    plan = build_plan(req, ['TX==>Future'], ['20250102'], {'max_companies_per_export': 8}, [])
    with closing(connect(tmp_path)) as con, con:
        con.execute("INSERT INTO tables(table_id,smart_id,name,category,phase,frequency,query_type,schema_sha256,fields_json,state) VALUES('risk','TEJ Derivatives','Future DB','futures','P1','daily','Future Index (ALL)','schema','[\"OI\"]','backfilling')")
        con.execute("INSERT INTO download_plans VALUES('risk',?,?,0,1,'P1',100)", (PLAN, json.dumps(plan)))
        install_priorities(con, snapshot(), 'smart_wizard')
        assert refill_ready_plans(con) == 1
        row = con.execute('SELECT task_id,priority FROM tasks').fetchone()
        assert row['priority'] == 0 and row['task_id'] == stable_id(['risk', next_request(plan, 0)])
        assert json.loads(con.execute('SELECT plan_json FROM download_plans').fetchone()[0])['fingerprint'] == plan['fingerprint']


def test_refresh_is_local_periodic_and_failures_do_not_stop_download(tmp_path, monkeypatch, policy):
    now = datetime.now(UTC)
    doc = {**snapshot(), "policy": policy}
    atomic_write_json(tmp_path / "value_priority.json", doc)
    calls, display = [], []
    def fail(*args):
        calls.append(1)
        raise OSError("private-path-or-source-error-not-published")
    monkeypatch.setattr('downloader.tej_value_priority.build_ranking', fail)
    assert refresh_if_due(tmp_path, now=now, on_refresh=lambda: display.append(1)) is None and calls == []
    assert display == []
    due = datetime.fromisoformat(doc['next_refresh_at_utc'])
    result = refresh_if_due(tmp_path, now=due, on_refresh=lambda: display.append(1))
    assert result['existing_order_retained'] and result['provider_requests_sent'] == 0
    assert 'private-path' not in json.dumps(result)
    assert refresh_if_due(tmp_path, now=due + timedelta(seconds=1), on_refresh=lambda: display.append(1)) is None and calls == [1]
    assert display == [1]  # No transient "refreshing" on every healthy query.


def test_due_refresh_reports_real_inventory_and_install_time_without_source_calls(tmp_path, monkeypatch, policy):
    doc = {**snapshot(), 'policy': policy}
    atomic_write_json(tmp_path/'value_priority.json', doc)
    events = []
    def build(*args):
        events.append('inventory')
        return doc, [], []
    monkeypatch.setattr('downloader.tej_value_priority.build_ranking', build)
    result = refresh_if_due(tmp_path, now=datetime.fromisoformat(doc['next_refresh_at_utc']),
                            on_refresh=lambda: events.append('display'))
    assert events == ['display', 'inventory']
    assert result['state'] == 'local_value_priorities_refreshed'
    assert result['total_refresh_seconds'] >= result['local_inventory_seconds'] >= 0
    assert datetime.fromisoformat(result['observed_at_utc']).tzinfo is not None
    assert result['provider_requests_sent'] == 0


@pytest.mark.parametrize('payload', ['{broken', 'null', '{}', '{"contract":"wrong"}'])
def test_invalid_ranking_cache_cannot_stop_healthy_source_or_clear_existing_order(tmp_path, payload):
    (tmp_path / 'value_priority.json').write_text(payload)
    result = refresh_if_due(tmp_path)
    assert result['existing_order_retained'] and result['provider_requests_sent'] == 0
    assert refresh_if_due(tmp_path) is None


def test_priority_install_does_not_decode_history_and_noop_updates_no_tasks(tmp_path):
    with closing(connect(tmp_path)) as con, con:
        con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,state) "
                    "VALUES('history','risk','download','P1',100,?,'complete')", ('private history' * 10000,))
        con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json) "
                    "VALUES('new','fund','download','P1',100,'original')")
        sql = []
        con.set_trace_callback(sql.append)
        first = install_priorities(con, snapshot(), 'smart_wizard')
        second = install_priorities(con, snapshot(), 'smart_wizard')
        assert first['pending_priorities_updated'] == 1
        assert second['pending_priorities_updated'] == 0
        assert first['source_identity_verification'] == PRIORITY_WRITE_CONTRACT
        assert first['source_identity_verified_unchanged'] and first['source_payloads_decoded'] == 0
        assert first['preserved_source_identity_sha256'] is None and first['audited_source_tasks'] is None
        assert not any('request_json' in statement or 'ORDER BY task_id' in statement for statement in sql)
        assert con.execute("SELECT request_json FROM tasks WHERE task_id='history'").fetchone()[0] == 'private history' * 10000


@pytest.mark.parametrize('statement', [
    "UPDATE tasks SET request_json='changed'", "UPDATE tasks SET state='complete'",
    "UPDATE tasks SET active_attempt_id='changed'", "DELETE FROM tasks",
    "INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json) VALUES('x','x','download','P1',0,'{}')",
    "UPDATE download_plans SET next_query=999", "UPDATE download_plans SET plan_json='changed'",
    "DELETE FROM download_plans", "UPDATE tables SET state='complete'",
    "UPDATE meta SET value='changed'", "INSERT INTO meta VALUES('source_contract','changed')",
    "PRAGMA user_version=999", "CREATE TABLE bypass(value)",
])
def test_priority_capability_rejects_all_non_priority_writes(tmp_path, statement):
    with closing(connect(tmp_path)) as con:
        with _priority_writes_only(con), pytest.raises(sqlite3.DatabaseError):
            con.execute(statement)


def test_prepared_statement_is_reauthorized_and_policy_is_removed_after_error(tmp_path):
    with closing(connect(tmp_path)) as con, con:
        sql = "UPDATE tasks SET state='pending' WHERE task_id='missing'"
        con.execute(sql)  # Populate sqlite3's statement cache before the fence.
        with pytest.raises(sqlite3.DatabaseError), _priority_writes_only(con):
            con.execute(sql)
        # The installer owns this connection's policy; unrelated later owner
        # work must not retain the installer's priority-only restrictions.
        con.execute(sql)


@pytest.mark.parametrize('origin', ['tasks', 'download_plans', 'acquisition_value_priorities', 'meta'])
def test_trigger_cannot_change_source_and_failed_install_rolls_back_only_itself(tmp_path, origin):
    with closing(connect(tmp_path)) as con:
        install_priorities(con, snapshot(), 'smart_wizard')
        con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json) "
                    "VALUES('pending','fund','download','P1',100,'original')")
        con.execute("INSERT INTO download_plans VALUES('fund','original','original',0,1,'P1',100)")
        con.execute(f"CREATE TRIGGER source_mutation AFTER UPDATE ON {origin} "
                    "BEGIN UPDATE tasks SET request_json='changed'; END")
        con.commit()
        # Caller has earlier uncommitted work. Failed ranking must not commit
        # it or roll it back, nor leave a half-installed order behind.
        con.execute("INSERT INTO meta VALUES('caller','preserve')")
        before = [tuple(r) for r in con.execute('SELECT * FROM tasks ORDER BY task_id')]
        with pytest.raises(sqlite3.DatabaseError):
            install_priorities(con, snapshot(), 'smart_wizard')
        assert before == [tuple(r) for r in con.execute('SELECT * FROM tasks ORDER BY task_id')]
        assert con.execute("SELECT value FROM meta WHERE key='caller'").fetchone()[0] == 'preserve'
        assert con.in_transaction
        con.rollback()
        assert con.execute("SELECT 1 FROM meta WHERE key='caller'").fetchone() is None


def test_api_scope_cursor_and_receipt_writes_are_denied_even_from_a_trigger(tmp_path):
    from downloader.tej_api import connect as api_connect
    with closing(api_connect(tmp_path)) as con, con:
        con.execute("INSERT INTO tasks(id,table_id,params_json,priority,state,cursor,query_rows,actual_rows,page_size) "
                    "VALUES('paid','TRAIL/TANAV','{}',0,'pending','paid-token',20000,20000,10000)")
        con.execute("CREATE TRIGGER mutate_paid_cursor AFTER UPDATE OF priority ON tasks "
                    "BEGIN UPDATE tasks SET cursor=NULL,query_rows=0,actual_rows=0; END")
        before = tuple(con.execute("SELECT * FROM tasks WHERE id='paid'").fetchone())
        with pytest.raises(sqlite3.DatabaseError):
            install_priorities(con, snapshot(), 'api')
        assert before == tuple(con.execute("SELECT * FROM tasks WHERE id='paid'").fetchone())


def test_foreign_key_cascade_outside_priority_columns_is_denied_and_rolled_back():
    with closing(sqlite3.connect(':memory:')) as con:
        con.execute('PRAGMA foreign_keys=ON')
        con.executescript("CREATE TABLE tasks(id TEXT PRIMARY KEY,table_id TEXT,state TEXT,priority INTEGER UNIQUE);"
                         "CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT);"
                         "CREATE TABLE child(value INTEGER REFERENCES tasks(priority) ON UPDATE CASCADE);"
                         "INSERT INTO tasks VALUES('x','TRAIL/TANAV','pending',100);"
                         "INSERT INTO child VALUES(100);")
        with pytest.raises(sqlite3.DatabaseError):
            install_priorities(con, snapshot(), 'api')
        assert con.execute('SELECT priority FROM tasks').fetchone()[0] == 100
        assert con.execute('SELECT value FROM child').fetchone()[0] == 100


@pytest.mark.parametrize('mismatch', [False, True])
def test_inventory_abba_keeps_read_snapshot_and_retains_failed_oracle(tmp_path, monkeypatch, mismatch):
    from scripts import benchmark_tej_value_priority as tool
    from scripts import build_tej_smart_wizard_inventory as inventory
    baseline = b'''def local_features(root):
        coverage_sql = "SELECT dataset,MIN(first_data_date),MAX(last_data_date) FROM tasks WHERE state='complete' GROUP BY dataset"
        samples_sql = "SELECT dataset,receipt_path FROM (SELECT dataset,receipt_path,ROW_NUMBER() OVER(PARTITION BY dataset ORDER BY last_data_date DESC,last_attempt_at_utc DESC) AS rank FROM tasks WHERE state='complete' AND receipt_path IS NOT NULL) WHERE rank=1"
'''
    monkeypatch.setattr(tool.subprocess, 'check_output', lambda args, **kwargs:
                        'a'*40 if args[1]=='rev-parse' else baseline)
    root = tmp_path/'repo'
    queue = root/'data_finmind'/'complement'/'queue.sqlite3'
    queue.parent.mkdir(parents=True)
    with closing(sqlite3.connect(queue)) as con, con:
        con.execute('CREATE TABLE tasks(dataset,state,first_data_date,last_data_date,last_attempt_at_utc,receipt_path)')
        con.execute("INSERT INTO tasks VALUES('fixture','complete','2000','2025','now','private-receipt')")
    before = queue.read_bytes()
    if mismatch:
        original = inventory.finmind_local_metadata
        monkeypatch.setattr(inventory, 'finmind_local_metadata', lambda con:(original(con)[0], []))
    output = tmp_path/'oracle.json'
    if mismatch:
        with pytest.raises(ValueError, match='failed ABBA receipt retained'):
            tool.benchmark_inventory(root, output)
    else:
        tool.benchmark_inventory(root, output)
    report = json.loads(output.read_text())
    assert report['accepted'] is (not mismatch)
    assert len(report['samples']) == 4
    assert report['provider_queries_sent'] == 0 and report['queue_modified'] is False
    assert report['source_values_read'] is False
    assert 'private-receipt' not in output.read_text()
    assert queue.read_bytes() == before
    with pytest.raises(FileExistsError):
        tool.benchmark_inventory(root, output)
