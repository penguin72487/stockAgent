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
    CONTRACT, coverage_hint, feature_value, installed_priority, install_priorities,
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
    calls = []
    def fail(*args):
        calls.append(1)
        raise OSError("private-path-or-source-error-not-published")
    monkeypatch.setattr('downloader.tej_value_priority.build_ranking', fail)
    assert refresh_if_due(tmp_path, now=now) is None and calls == []
    due = datetime.fromisoformat(doc['next_refresh_at_utc'])
    result = refresh_if_due(tmp_path, now=due)
    assert result['existing_order_retained'] and result['provider_requests_sent'] == 0
    assert 'private-path' not in json.dumps(result)
    assert refresh_if_due(tmp_path, now=due + timedelta(seconds=1)) is None and calls == [1]


@pytest.mark.parametrize('payload', ['{broken', 'null', '{}', '{"contract":"wrong"}'])
def test_invalid_ranking_cache_cannot_stop_healthy_source_or_clear_existing_order(tmp_path, payload):
    (tmp_path / 'value_priority.json').write_text(payload)
    result = refresh_if_due(tmp_path)
    assert result['existing_order_retained'] and result['provider_requests_sent'] == 0
    assert refresh_if_due(tmp_path) is None
