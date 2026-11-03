from collections import Counter
from contextlib import closing
from copy import deepcopy
import json
from random import Random

import pytest

from downloader.tej_history import (SOURCE_SCOPE_CONTRACT, compact_request, company_code,
                                    configure_preview_planning, connect, normalize_period,
                                    run_one, stable_id)
from downloader.tej_key_layout import KEY1_CONTRACT
from downloader.tej_planning import build_plan, next_request, refill_ready_plans
from downloader.tej_priority_coverage import (ensure_priority_coverage_index,
    exclude_verified_scopes, index_new_priority_tasks, inherit_priority_coverage)
from test_tej_planning import DynamicBridge, axes, registry as registry
from test_tej_priority import batch as priority_batch, submit as submit_priority


def request():
    return {"contract_version": 4, "action": "download", "type": "Option (ALL)",
            "smart_id": "TEJ Derivatives", "table": "Fixture",
            "fields": ["Open", "Close"], "catalog_fields": ["Open", "Close"],
            "company_labels": ["AAA==>A", "BBB==>B", "CCC==>C"],
            "date_labels": ["20110215", "20110216", "20110217"],
            "frequency": "daily", "start": "2011-02-15", "end": "2011-02-17",
            "max_rows": 20, "max_cells": 300}


def cells(value):
    dates = value["date_labels"] or [None]
    return {(company_code(c), normalize_period(d) if d else None)
            for c in value["company_labels"] for d in dates}


def witness(identifier, base, companies, dates):
    return {"task_id": identifier, "request": {**base, "company_labels": companies,
                                                   "date_labels": dates}}


def test_completed_sparse_rectangles_are_removed_once_without_losing_other_work():
    base = request()
    prior = [witness("a", base, base["company_labels"][:2], base["date_labels"][1:]),
             witness("b", base, base["company_labels"][1:], base["date_labels"][2:])]
    children, audit = exclude_verified_scopes("table", 42, base, prior)
    original = cells(base)
    already = set().union(*(cells(w["request"]) for w in prior))
    observed = Counter(cell for child in children for cell in cells(child))
    assert set(observed) == original - already and set(observed.values()) == {1}
    assert audit["excluded_work_rows"] == len(already)
    assert audit["remaining_work_rows"] == len(original - already)
    assert audit["priority_task_ids"] == ["a", "b"]
    assert all(child["fields"] == base["fields"] and child["start"] == base["start"]
               and child["end"] == base["end"] for child in children)


def test_full_coverage_creates_no_duplicate_query_and_no_coverage_keeps_identity():
    base = request()
    children, audit = exclude_verified_scopes("table", 0, base, [witness("done", base,
                                              base["company_labels"], base["date_labels"])])
    assert children == [] and audit["excluded_work_rows"] == 9 and audit["remaining_work_rows"] == 0
    children, audit = exclude_verified_scopes("table", 0, base, [])
    assert children == [base] and children[0] is base and audit["excluded_work_rows"] == 0


def test_fewer_exact_rectangles_are_chosen_between_company_and_date_grouping():
    base = request()
    base["company_labels"].append("DDD==>D")
    companies, dates = base["company_labels"], base["date_labels"]
    priors = [witness("a", base, [companies[0]], [dates[0]]),
              witness("b", base, [companies[1]], [dates[1]]),
              witness("c", base, [companies[2]], dates[:2])]
    children, _ = exclude_verified_scopes("table", 3, base, priors)
    assert len(children) == 3  # Company grouping would require four rectangles.
    covered = set().union(*(cells(item["request"]) for item in priors))
    assert set().union(*(cells(child) for child in children)) == cells(base) - covered


@pytest.mark.parametrize("defect", ["partial_fields", "wrong_table", "wrong_key", "alias", "duplicate_period"])
def test_ambiguous_or_incompatible_coverage_fails_closed(defect):
    base = request()
    prior = witness("done", base, base["company_labels"][:1], base["date_labels"][:1])
    if defect == "partial_fields":
        prior["request"]["fields"] = ["Open"]
    elif defect == "wrong_table":
        prior["request"]["table"] = "Other"
    elif defect == "wrong_key":
        prior["request"].update(source_key_mode=1, date_labels=[])
    elif defect == "alias":
        base["company_labels"] = ["AAA==>A", "AAA==>other"]
    else:
        base["date_labels"] = ["20110215", "2011/02/15"]
    with pytest.raises(ValueError):
        exclude_verified_scopes("table", 0, base, [prior])


def test_snapshot_is_not_treated_as_an_equivalent_later_observation():
    base = request()
    base.update(source_key_mode=1, key_layout_contract=KEY1_CONTRACT,
                date_labels=[], frequency="snapshot")
    prior = witness("done", base, base["company_labels"][:1], [])
    with pytest.raises(ValueError, match="dated daily"):
        exclude_verified_scopes("table", 5, base, [prior])


def test_random_overlapping_rectangles_match_independent_cell_oracle():
    rng = Random(20261029)
    for _ in range(100):
        base = request()
        priors = []
        for index in range(rng.randrange(7)):
            companies = [c for c in base["company_labels"] if rng.randrange(2)]
            dates = [d for d in base["date_labels"] if rng.randrange(2)]
            if companies and dates:
                priors.append(witness(str(index), deepcopy(base), companies, dates))
        children, audit = exclude_verified_scopes("table", 7, base, priors)
        original = cells(base)
        covered = set().union(*(cells(item["request"]) for item in priors)) if priors else set()
        remaining = Counter(cell for child in children for cell in cells(child))
        assert set(remaining) == original - covered
        assert all(count == 1 for count in remaining.values())
        assert audit["original_work_rows"] == audit["excluded_work_rows"] + audit["remaining_work_rows"] == len(original)


def completed_priority_in_existing_plan(registry):
    """Use the real owner and receipts for a finite task added after planning."""
    root, config = registry
    configure_preview_planning(root, config)
    bridge = DynamicBridge()
    assert run_one(root, bridge) == "completed_task"  # Native source discovery.
    with closing(connect(root)) as con, con:
        definition = dict(con.execute("SELECT * FROM tables").fetchone())
        plan = json.loads(con.execute("SELECT plan_json FROM download_plans").fetchone()[0])
        lazy = next_request(plan, 0)
        finite = {**lazy, "company_labels": lazy["company_labels"][:1],
                  "date_labels": lazy["date_labels"][:1]}
        task_id = stable_id([definition["table_id"], finite])
        encoded = json.dumps(compact_request(finite, definition))
        con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,state,"
                    "scope_contract,expected_rows,work_expected_rows) VALUES(?,?,'download','P1',-20,?,"
                    "'pending',?,1,1)", (task_id, definition["table_id"], encoded, SOURCE_SCOPE_CONTRACT))
        receipt = {"tasks": [{"task_id": task_id, "table_id": definition["table_id"]}]}
        con.execute("INSERT INTO meta VALUES(?,?)", ("finite_priority_plan:" + "a" * 64, json.dumps(receipt)))
        index_new_priority_tasks(con, receipt)
    assert run_one(root, bridge) == "completed_task"
    return root, bridge, task_id


def test_canonical_owner_reads_priority_receipt_and_finishes_each_source_cell_once(registry):
    root, bridge, _ = completed_priority_in_existing_plan(registry)
    for _ in range(10):
        if run_one(root, bridge) == "idle":
            break
    else:
        pytest.fail("Lazy owner did not finish the finite fixture")
    seen = Counter()
    for req in bridge.requests:
        if req["action"] != "download":
            continue
        for field in req["fields"]:
            for company, period in cells(req):
                seen[(field, company, period)] += 1
    companies, dates = axes()
    assert len(seen) == 59 * len(companies) * len(dates)
    assert set(seen.values()) == {1}
    with closing(connect(root)) as con:
        rows = con.execute("SELECT sum(work_expected_rows) FROM tasks "
                           "WHERE kind='download' AND state='complete'").fetchone()[0]
        total = con.execute("SELECT work_grid_rows FROM tables").fetchone()[0]
        assert rows == total == 30
        assert con.execute("SELECT next_query=total_queries FROM download_plans").fetchone()[0] == 1


def test_full_discovery_preserves_pending_previously_verified_finite_work(registry):
    root, config = registry
    configure_preview_planning(root, config)
    bridge = DynamicBridge()
    with closing(connect(root)) as con, con:
        definition = dict(con.execute('SELECT * FROM tables').fetchone())
        discover = dict(con.execute("SELECT * FROM tasks WHERE kind='discover'").fetchone())
        base = json.loads(discover['request_json'])
        companies, dates = axes()
        plan = build_plan(base, companies, dates, config, [])
        query = next_request(plan, 0)
        query = {**query, 'company_labels': companies[:1], 'date_labels': dates[:1]}
        task_id = stable_id([definition['table_id'], query])
        con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,state,"
                    "scope_contract,expected_rows,work_expected_rows) VALUES(?,?,'download','P1',-20,?,"
                    "'pending',?,1,1)", (task_id, definition['table_id'],
                    json.dumps(compact_request(query, definition)), SOURCE_SCOPE_CONTRACT))
        receipt = {'tasks': [{'task_id': task_id, 'table_id': definition['table_id']}]}
        con.execute('INSERT INTO meta VALUES(?,?)', ('finite_priority_plan:' + 'b' * 64, json.dumps(receipt)))
    assert run_one(root, bridge, kind='discover') == 'completed_task'
    with closing(connect(root)) as con:
        assert con.execute('SELECT state FROM tasks WHERE task_id=?', (task_id,)).fetchone()[0] == 'pending'
    for _ in range(15):
        if run_one(root, bridge) == 'idle':
            break
    else:
        pytest.fail('Owner did not finish retained finite and lazy work')
    seen = Counter((field, company, period)
                   for request in bridge.requests if request['action'] == 'download'
                   for field in request['fields'] for company, period in cells(request))
    assert len(seen) == 59 * len(companies) * len(dates) and set(seen.values()) == {1}
    with closing(connect(root)) as con:
        assert con.execute("SELECT sum(work_expected_rows) FROM tasks WHERE kind='download' AND state='complete'").fetchone()[0] == 30
        assert con.execute('SELECT work_grid_rows FROM tables').fetchone()[0] == 30


def test_changed_retained_original_rolls_back_lazy_plan_and_never_authorizes_a_query(registry):
    root, _, task_id = completed_priority_in_existing_plan(registry)
    with closing(connect(root)) as con:
        task = con.execute("SELECT receipt_path FROM tasks WHERE task_id=?", (task_id,)).fetchone()
        receipt = json.loads((root / task["receipt_path"]).read_text())
        before = con.execute("SELECT next_query FROM download_plans").fetchone()[0]
    (root / receipt["raw_path"]).write_text("changed retained source")
    with closing(connect(root)) as con:
        with pytest.raises(ValueError, match="differs from its receipt"):
            with con:
                refill_ready_plans(con)
    with closing(connect(root)) as con:
        assert con.execute("SELECT next_query FROM download_plans").fetchone()[0] == before
        assert con.execute("SELECT count(*) FROM tasks WHERE kind='download'").fetchone()[0] == 1


def test_native_split_children_inherit_finite_coverage_index(registry):
    root, _, task_id = completed_priority_in_existing_plan(registry)
    with closing(connect(root)) as con, con:
        ensure_priority_coverage_index(con)
        parent = dict(con.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone())
        child = "f" * 24
        con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,state,scope_contract) "
                    "VALUES(?,?,'download','P1',-20,?,'pending',?)",
                    (child, parent["table_id"], parent["request_json"], SOURCE_SCOPE_CONTRACT))
        inherit_priority_coverage(con, task_id, [child])
        count = con.execute("SELECT count(*) FROM finite_priority_coverage_company_v1 WHERE task_id=?",
                            (child,)).fetchone()[0]
        assert count == 1


def test_new_priority_registration_updates_existing_index_in_same_transaction(priority_batch):
    root = priority_batch[0]
    with closing(connect(root)) as con, con:
        ensure_priority_coverage_index(con)
    result = submit_priority(priority_batch)
    with closing(connect(root)) as con:
        indexed = con.execute("SELECT table_id,company_code FROM finite_priority_coverage_company_v1 "
                              "WHERE task_id=?", (result['tasks'][0]['task_id'],)).fetchall()
        assert [tuple(row) for row in indexed] == [(result['tasks'][0]['table_id'], 'AA1201112')]
