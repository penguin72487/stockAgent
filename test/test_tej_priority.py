from contextlib import closing
import copy
import json

import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.tej_history import SOURCE_SCOPE_CONTRACT, compact_request, connect, stable_id
from downloader.tej_key_layout import KEY1_CONTRACT
from downloader.tej_priority import CONTRACT, register_priority_requests
from scripts.build_tej_smart_wizard_inventory import table_identity


@pytest.fixture
def batch(tmp_path):
    root = tmp_path / "data_tej"
    with closing(connect(root)) as con, con:
        config = {"max_rows_per_export": 100, "max_cells_per_export": 1000,
                  "max_companies_per_export": 32}
        con.execute("INSERT INTO meta VALUES('config',?)", (json.dumps(config),))
        names = ["OPEN", "SETTLE"]
        tid, digest = table_identity("TEJ Derivatives", "Future DB", names)
        definition = dict(table_id=tid, smart_id="TEJ Derivatives", name="Future DB",
                          query_type="Future Index (ALL)", schema_sha256=digest,
                          fields_json=json.dumps(names), phase="P1", frequency="daily",
                          category="derivatives", state="backfilling", discovery_path="raw/axes.json")
        con.execute("INSERT INTO tables(table_id,smart_id,name,query_type,schema_sha256,fields_json,"
                    "phase,frequency,category,state,discovery_path) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    tuple(definition.values()))
    axes = {"contract_version": 4, "action": "plan", "smart_id": definition["smart_id"],
            "table": definition["name"], "type": definition["query_type"], "fields": names,
            "company_labels": ["AA1201112==>AA December", "BB1201112==>BB December"],
            "date_labels": ["20111117", "20111118"]}
    atomic_write_json(root / "raw/axes.json", axes)
    source = tmp_path / "gap.csv"
    source.write_text("product,contract\nAA1,201112\n")
    request = {"contract_version": 4, "action": "download", "smart_id": definition["smart_id"],
               "table": definition["name"], "type": definition["query_type"], "frequency": "daily",
               "fields": names, "catalog_fields": names, "start": "2011-11-17", "end": "2011-11-18",
               "company_labels": axes["company_labels"][:1], "date_labels": axes["date_labels"],
               "max_rows": 100, "max_cells": 1000}
    item = {"table_id": tid, "discovery_sha256": sha256_file(root / "raw/axes.json"),
            "request": request, "priority": -20, "reason": "exact own-month valuation gaps"}
    plan = {"contract": CONTRACT, "sources": [{"path": str(source), "sha256": sha256_file(source)}],
            "requests": [item]}
    return root, definition, plan, tmp_path / "plan.json", tmp_path / "receipt.json"


def submit(batch):
    root, _, plan, path, receipt = batch
    atomic_write_json(path, plan)
    return register_priority_requests(root, path, receipt)


def add_prior(batch, state="complete", request=None, priority=100):
    root, definition, plan, _, _ = batch
    request = copy.deepcopy(request or plan["requests"][0]["request"])
    task = stable_id([definition["table_id"], request])
    with closing(connect(root)) as con, con:
        con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,state,scope_contract,"
                    "last_error_code) VALUES(?,?,'download','P1',?,?,?,?,?)",
                    (task, definition["table_id"], priority, json.dumps(compact_request(request, definition)),
                     state, SOURCE_SCOPE_CONTRACT,
                     "unknown_outcome_no_auto_retry" if state == "blocked" else None))
    return task


def test_live_unrelated_query_is_untouched_and_finite_tasks_precede_original_plan(batch):
    root, _, plan, _, receipt = batch
    unrelated = copy.deepcopy(plan["requests"][0]["request"])
    unrelated["company_labels"] = ["BB1201112==>BB December"]
    prior = add_prior(batch, "running", unrelated)
    result = submit(batch)
    with closing(connect(root)) as con:
        assert tuple(con.execute("SELECT state,priority FROM tasks WHERE task_id=?", (prior,)).fetchone()) == ("running", 100)
        row = con.execute("SELECT state,priority,scope_contract,expected_rows FROM tasks WHERE task_id=?",
                          (result["tasks"][0]["task_id"],)).fetchone()
        assert tuple(row) == ("pending", -20, SOURCE_SCOPE_CONTRACT, 2)
        assert con.execute("SELECT count(*) FROM meta WHERE key LIKE 'finite_priority_plan:%'").fetchone()[0] == 1
    assert receipt.exists() and result["desktop_actions"] == 0 and result["unknown_outcomes_retried"] == 0


@pytest.mark.parametrize("state", ["complete", "running", "blocked"])
def test_prior_positive_negative_or_unknown_scope_cannot_be_resubmitted(batch, state):
    task = add_prior(batch, state)
    with pytest.raises(ValueError, match="overlaps"):
        submit(batch)
    with closing(connect(batch[0])) as con:
        assert con.execute("SELECT state FROM tasks WHERE task_id=?", (task,)).fetchone()[0] == state
        assert con.execute("SELECT count(*) FROM tasks").fetchone()[0] == 1


def test_existing_pending_exact_scope_is_promoted_without_copying_source_task(batch):
    task = add_prior(batch, "pending")
    result = submit(batch)
    assert result["tasks"][0]["task_id"] == task
    assert result["tasks"][0]["previous_priority"] == 100
    with closing(connect(batch[0])) as con:
        assert con.execute("SELECT count(*) FROM tasks").fetchone()[0] == 1
        assert con.execute("SELECT priority FROM tasks").fetchone()[0] == -20


def test_partial_pending_overlap_does_not_duplicate_provider_work(batch):
    request = copy.deepcopy(batch[2]["requests"][0]["request"])
    request["date_labels"] = ["20111117"]
    add_prior(batch, "pending", request)
    with pytest.raises(ValueError, match="overlaps"):
        submit(batch)


@pytest.mark.parametrize("change", [
    lambda p: p["sources"][0].update(sha256="0" * 64),
    lambda p: p["requests"][0].update(discovery_sha256="0" * 64),
    lambda p: p["requests"][0].update(priority=True),
    lambda p: p["requests"][0].update(priority=-100),
    lambda p: p["requests"][0]["request"].update(company_labels=["ZZ1201112==>not captured"]),
    lambda p: p["requests"][0]["request"].update(date_labels=["20111119"]),
    lambda p: p["requests"][0]["request"].update(date_labels=["20111117", "20111117"]),
    lambda p: p["requests"][0]["request"].update(start="2011-11-18"),
    lambda p: p["requests"][0]["request"].update(fields=["OPEN"]),
    lambda p: p["requests"][0]["request"].update(max_rows=101),
    lambda p: p["requests"][0]["request"].update(max_rows=1),
    lambda p: p["requests"][0]["request"].update(max_cells=1001),
    lambda p: p["requests"][0]["request"].update(frequency="monthly"),
    lambda p: p["requests"].append(copy.deepcopy(p["requests"][0])),
])
def test_invalid_batch_is_rejected_atomically_before_queue_changes(batch, change):
    change(batch[2])
    with pytest.raises(ValueError):
        submit(batch)
    with closing(connect(batch[0])) as con:
        assert con.execute("SELECT count(*) FROM tasks").fetchone()[0] == 0
    assert not batch[4].exists()


def test_snapshot_keeps_source_observation_mode_not_fabricated_daily_axis(batch):
    root, definition, plan, _, _ = batch
    req = plan["requests"][0]["request"]
    req.update(source_key_mode=1, key_layout_contract=KEY1_CONTRACT, frequency="snapshot", date_labels=[])
    with closing(connect(root)) as con, con:
        con.execute("UPDATE tables SET source_key_mode=1 WHERE table_id=?", (definition["table_id"],))
    result = submit(batch)
    assert result["tasks"][0]["requested_grid_rows"] == 1


def test_snapshot_with_dates_is_not_admitted_as_history(batch):
    root, definition, plan, _, _ = batch
    plan["requests"][0]["request"].update(source_key_mode=1, key_layout_contract=KEY1_CONTRACT, frequency="snapshot")
    with closing(connect(root)) as con, con:
        con.execute("UPDATE tables SET source_key_mode=1 WHERE table_id=?", (definition["table_id"],))
    with pytest.raises(ValueError, match="Snapshot"):
        submit(batch)


def test_completed_priority_batch_leaves_original_pending_priority_and_rotation(batch):
    from downloader.tej_history import _ready_task
    root, _, plan, _, _ = batch
    unrelated = copy.deepcopy(plan["requests"][0]["request"])
    unrelated["company_labels"] = ["BB1201112==>BB December"]
    original = add_prior(batch, "pending", unrelated)
    result = submit(batch)
    with closing(connect(root)) as con, con:
        assert _ready_task(con, "2026-10-03T00:00:00+00:00", "download", {})["task_id"] == result["tasks"][0]["task_id"]
        con.execute("UPDATE tasks SET state='complete' WHERE task_id=?", (result["tasks"][0]["task_id"],))
        assert _ready_task(con, "2026-10-03T00:00:00+00:00", "download", {})["task_id"] == original


def test_priority_never_bypasses_existing_table_quarantine(batch):
    from downloader.tej_history import _ready_task
    root, _, plan, _, _ = batch
    unrelated = copy.deepcopy(plan["requests"][0]["request"])
    unrelated["company_labels"] = ["BB1201112==>BB December"]
    old = add_prior(batch, "blocked", unrelated)
    submit(batch)
    with closing(connect(root)) as con, con:
        con.execute("UPDATE tasks SET last_error_code='source_validation_failed_deferred' WHERE task_id=?", (old,))
        assert _ready_task(con, "2026-10-03T00:00:00+00:00", "download", {}) is None


@pytest.mark.parametrize("cap,rows", [(128, 10000), (2, 6), (1, 2)])
def test_sparse_rectangles_cover_all_needed_cells_once_with_native_bounds(cap, rows):
    from downloader.tej_priority import pack_gap_rectangles
    wanted = {"A": {"20111117", "20111118"}, "B": {"20111118", "20111119"},
              "C": {"20111118"}, "D": {"20111201"}}
    forbidden = {("A", "20111201")}
    result = pack_gap_rectangles(wanted, forbidden=forbidden, max_companies=cap, max_rows=rows)
    captured = []
    for rectangle in result:
        cells = [(c, d) for c in rectangle["companies"] for d in rectangle["dates"]]
        assert not set(cells) & forbidden
        assert len(cells) <= rows and len(rectangle["companies"]) <= cap
        assert len(cells) <= 2 * rectangle["needed_rows"]
        captured.extend(cells)
    assert len(captured) == len(set(captured))
    assert {(c, d) for c, days in wanted.items() for d in days} <= set(captured)


def test_legacy_scope_is_not_requeried_or_upgraded_to_verified_source(batch):
    root, _, _, _, _ = batch
    task = add_prior(batch)
    with closing(connect(root)) as con, con:
        con.execute("UPDATE tasks SET scope_contract='legacy_scope_unverified_v3' WHERE task_id=?", (task,))
    with pytest.raises(ValueError, match="overlaps"):
        submit(batch)
    with closing(connect(root)) as con:
        assert con.execute("SELECT scope_contract FROM tasks").fetchone()[0] == "legacy_scope_unverified_v3"
