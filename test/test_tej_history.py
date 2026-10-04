from contextlib import closing
from datetime import UTC, datetime
import json
from pathlib import Path

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.tej_history import (DATE_AXIS, DesktopBridge, connect, discovery_tasks, normalize_period,
                                   BeforeDataQueryError, MetadataPreparationError, _ready_task, configure_runtime_policy,
                                   isolate_failed_metadata, mark_batch_finished, recover_legacy_date_input, recover_verified_input,
                                   compact_queue, company_code, display_decimal, expand_request, mark_worker_wait, recover_desktop_response,
                                   recover_evidence, refresh_axis_profiles, register_inventory, revalidate_source_scopes, run_one, settle_metadata_failure,
                                   scale_decimal_power10, task_request, validate_download_evidence, validate_export)
from scripts.build_tej_smart_wizard_inventory import main as build_inventory
from stockagent.live.tej_dashboard import build_tej_feature_page, build_tej_public_status


@pytest.fixture
def registry(tmp_path):
    source = tmp_path / "catalog.jsonl"
    source.write_text("\n".join(json.dumps(r) for r in [
        {"contract_version":1,"record_kind":"header","provider":"tej_smart_wizard","types":["LISTED & DELISTED"]},
        {"record_kind":"binding","type":"LISTED & DELISTED","smart_id":"TEJEquity","table":"TSE/OTC Unadjusted_Price(Daily)",
         "fields":["Volume(1000S)","Ratio%"],"field_count":2},
        {"record_kind":"completion","complete":True},
    ]))
    inventory = tmp_path / "inventory"
    build_inventory(["--catalog",str(source),"--root",str(tmp_path),"--output-dir",str(inventory)])
    config = {"history_search_start":"1900-01-01","max_cells_per_export":100,"max_rows_per_export":10,
              "max_companies_per_export":2,"quota":{}}
    root = tmp_path / "data_tej"
    register_inventory(root,inventory,config,cutoff="2014-01-03")
    return tmp_path, root, inventory, config


def request():
    return {"type":"LISTED & DELISTED","smart_id":"TEJEquity","table":"TSE/OTC Unadjusted_Price(Daily)",
            "fields":["Volume(1000S)","Ratio%"],"company_labels":["2330=>TSMC","2317=>HonHai"],
            "date_labels":["2014/01/02","2014/01/03"],"max_rows":10,"max_cells":100}


def export():
    result = {"contract_version":2,"provider":"tej_smart_wizard","action":"download","date_axis":DATE_AXIS,
            **{k:request()[k] for k in ('type','smart_id','table','fields')},
            "universe_scope":"exact_type_smart_id_all_sectors","calendar_date_mode":False,
            "checkbox_verification_method":"msaa_role44_state_flags",
            "credentials_read":False,"query_comments_read":False,"date_system":"excel_1900",
            "observed_at_utc":"2026-10-01T16:00:00+00:00",
            "cells":[["CO_ID","Date","Volume(1000S)","Ratio%"],
                     ["2330 TSMC",41641,10,-1.5],["2330 TSMC",41642,None,0],
                     ["2317 HonHai",41641,20,None],["2317 HonHai",41642,0,2.5]]}
    preview = [result['cells'][0],
               ['2330 TSMC','2014/01/02','10','-1.5'],['2330 TSMC','2014/01/03','', '0'],
               ['2317 HonHai','2014/01/02','20',''],['2317 HonHai','2014/01/03','0','2.5']]
    result['preview'] = [len(preview), [[i,row] for i,row in enumerate(preview,1)]]
    return result


def test_register_is_idempotent_and_preserves_unknown_counts(registry):
    repo,root,inventory,config=registry
    register_inventory(root,inventory,config,cutoff="2014-01-03")
    with closing(connect(root)) as con:
        assert con.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]==1
        assert con.execute("SELECT exported_non_null_cells FROM features LIMIT 1").fetchone()[0] is None
    status=build_tej_public_status(repo)
    assert status["workload"]["total_rows"] is None
    assert status["workload"]["global_row_ratio"] is None
    assert status["quota"]["smart_wizard_requests_per_day"] is None


def test_raw_units_missing_values_zero_and_signed_ratio():
    headers,rows,profile=validate_export(request(),export())
    assert profile["non_null_counts"]==[3,3]
    assert rows[0][-1]==-1.5 and rows[-1][2]==0
    assert not profile["native_observation_completeness_verified"]


@pytest.mark.parametrize("mutation", ["missing","duplicate","schema","out_of_scope","nonfinite","oversized"])
def test_bad_export_is_rejected(mutation):
    doc=export()
    if mutation=="missing":doc["cells"].pop()
    if mutation=="duplicate":doc["cells"][-1]=doc["cells"][1]
    if mutation=="schema":doc["cells"][0][2]="Volume(shares)"
    if mutation=="out_of_scope":doc["cells"][1][0]="9999 Unknown"
    if mutation=="nonfinite":doc["cells"][1][2]=float("nan")
    req=request()
    if mutation=="oversized":req["max_cells"]=10
    with pytest.raises(ValueError):validate_export(req,doc)


@pytest.mark.parametrize("value", ["2025/02/30","n/a",float("nan"),60])
def test_invalid_or_excel_phantom_dates_not_invented(value):
    with pytest.raises(ValueError):normalize_period(value,system="excel_1900")


def test_source_plan_requires_exact_axes_no_duplicate_labels(registry):
    _,root,_,_=registry
    with closing(connect(root)) as con:
        task=dict(con.execute("SELECT * FROM tasks").fetchone())
    payload={"provider":"tej_smart_wizard","action":"plan","date_axis":DATE_AXIS,
             "fields":request()["fields"],"company_labels":["2330=>TSMC"]*2,"date_labels":["2014/01/02"]}
    with pytest.raises(ValueError):discovery_tasks(root,task,payload,root/"raw.json")


class FakeBridge:
    def execute(self,root,task):
        req=json.loads(task["request_json"])
        if task["kind"]=="discover":
            payload={"provider":"tej_smart_wizard","action":"plan","date_axis":DATE_AXIS,"fields":req["fields"],
                     **{k:req[k] for k in ('type','smart_id','table')},"contract_version":req['contract_version'],"credentials_read":False,
                     "source_scope_proof":scope_proof(),
                     "universe_scope":"exact_type_smart_id_all_sectors","calendar_date_mode":False,
                     "checkbox_verification_method":"msaa_role44_state_flags",
                     "company_labels":request()["company_labels"],"date_labels":request()["date_labels"]}
        else:payload=full_grid_export()
        payload['task_id']=task['task_id']
        output=root/"raw"/(task["task_id"]+"-fake.json")
        atomic_write_json(output,payload)
        return payload,output,1.0


def test_verified_exports_are_private_and_register_individual_fields(registry):
    repo,root,_,_=registry
    assert run_one(root,FakeBridge())=="completed_task"
    assert run_one(root,FakeBridge())=="completed_task"
    assert run_one(root,FakeBridge())=="idle"
    status=build_tej_public_status(repo)
    assert status["workload"]["exported_rows"]==4
    assert status["workload"]["exported_non_null_cells"]==6
    assert not status["native_observation_completeness_verified"]
    fields=build_tej_feature_page(repo)
    assert fields["page"]["matched_total"]==2
    assert all(r["exported_non_null_cells"]==3 for r in fields["features"])
    body=json.dumps(status)+json.dumps(fields)
    assert 'raw_path' not in body and 'parquet_path' not in body and '"cells"' not in body
    assert 'desktop_session' not in body and 'CO_ID' not in body


def test_page_search_literal_wildcards_and_invalid_parameters(registry):
    repo,_,_,_=registry
    assert build_tej_feature_page(repo,search="Ratio%")['page']['matched_total']==1
    assert build_tej_feature_page(repo,search="%")["page"]["matched_total"]==1
    with pytest.raises(ValueError):build_tej_feature_page(repo,limit=100000)
    with pytest.raises(ValueError):build_tej_feature_page(repo,table_id="../raw")
    with pytest.raises(ValueError):build_tej_feature_page(repo,phase="DROP TABLE")


def test_unknown_desktop_failure_never_retries_or_counts_complete(registry):
    repo,root,_,_=registry
    class Failed:
        def execute(self,*args):raise RuntimeError("account and key should not reach status")
    assert run_one(root,Failed())=="unknown_outcome_no_auto_retry"
    assert run_one(root,Failed())=="inflight_requires_recovery"
    status=build_tej_public_status(repo)
    assert status["state"]=="needs_review"
    assert status["workload"]["exported_rows"]==0
    assert 'account and key' not in json.dumps(status)


def test_proven_prequery_input_failure_only_retries_twice_and_never_counts_rows(registry):
    repo, root, _, _ = registry
    run_one(root, FakeBridge())

    class InputFailure:
        def execute(self, *args):
            raise BeforeDataQueryError('local_date_input_failed_before_preview')

    for _ in range(2):
        assert run_one(root, InputFailure()) == 'prequery_retry_scheduled'
        with closing(connect(root)) as con, con:
            assert con.execute("SELECT state FROM tasks WHERE kind='download'").fetchone()[0] == 'pending'
            con.execute("UPDATE tasks SET next_attempt_at_utc=NULL WHERE kind='download'")
    assert run_one(root, InputFailure()) == 'date_input_prequery_needs_review'
    status = build_tej_public_status(repo)
    assert status['state'] == 'needs_review'
    assert status['workload']['exported_rows'] == 0
    assert status['workload']['resolved_grid_rows'] == 0


@pytest.mark.parametrize('key,value', [('type','Bankstat'),('smart_id','wrong'),('table','other'),
                                    ('calendar_date_mode',True),('universe_scope','current_sector'),
                                    ('fields',['Ratio%'])])
def test_wrong_binding_or_partial_universe_rejected(key,value):
    doc=export();doc[key]=value
    with pytest.raises(ValueError):validate_export(request(),doc)


def test_preview_is_required_and_value_parity_checked():
    doc=export();doc.pop('preview')
    with pytest.raises(ValueError,match='Preview'):validate_export(request(),doc)
    doc=export();doc['preview'][1][1][1][2]='999'
    with pytest.raises(ValueError,match='value mismatch'):validate_export(request(),doc)


def test_native_sparse_source_rows_are_not_fabricated_or_called_a_lost_export():
    doc = export()
    doc["source_key_headers"] = ["Bank ID", "Data YYMM"]
    doc["cells"] = [["Bank ID", "Data YYMM", *request()["fields"]], doc["cells"][1]]
    doc["preview"] = [2, [[1, ["Top Left Header Cell", *doc["cells"][0]]],
                          [2, ["", "2330 TSMC", "2014/01/02", "10", "-1.5"]]]]
    headers, rows, profile = validate_export(request(), doc)
    assert headers[:2] == ["Bank ID", "Data YYMM"]
    assert len(rows) == 1
    assert profile["requested_query_rows"] == 4
    assert profile["omitted_query_grid_rows"] == 3
    assert not profile["native_observation_completeness_verified"]


def test_recover_committed_receipt_is_idempotent_and_does_not_query(registry):
    repo,root,_,_=registry
    run_one(root,FakeBridge());run_one(root,FakeBridge())
    with closing(connect(root)) as con:
        task=dict(con.execute("SELECT * FROM tasks WHERE kind='download'").fetchone())
    before=build_tej_public_status(repo)['workload']['exported_non_null_cells']
    recover_evidence(root,task['task_id'],root/task['output_path'])
    assert build_tej_public_status(repo)['workload']['exported_non_null_cells']==before==6


def test_running_task_blocks_other_work_until_evidence_recovery(registry):
    _,root,_,_=registry
    with closing(connect(root)) as con,con:
        con.execute("UPDATE tasks SET state='running'")
    assert run_one(root,FakeBridge())=='inflight_requires_recovery'
    assert run_one(root,FakeBridge(),kind='discover')=='inflight_requires_recovery'


def test_discovery_only_does_not_repeat_or_download_known_history(registry):
    _, root, _, _ = registry
    assert run_one(root, FakeBridge(), kind='discover') == 'completed_task'
    assert run_one(root, FakeBridge(), kind='discover') == 'idle'
    with closing(connect(root)) as con:
        assert con.execute("SELECT COUNT(*) FROM tasks WHERE kind='download' AND state='pending'").fetchone()[0] > 0
    with pytest.raises(ValueError, match='task kind'):
        run_one(root, FakeBridge(), kind='unreviewed')


def test_re_register_next_day_cannot_duplicate_history(registry):
    _,root,inventory,config=registry
    with pytest.raises(ValueError,match='cutoff is immutable'):
        register_inventory(root,inventory,config,cutoff='2014-01-04')


@pytest.mark.parametrize("label", ["2330=>台積電", "2330==>台積電"])
def test_current_and_legacy_source_company_separators(label):
    assert company_code(label) == "2330"
    req = request()
    req["company_labels"] = [label, "2317==>鴻海"]
    assert validate_export(req, export())[2]["non_null_counts"] == [3, 3]


@pytest.mark.parametrize("label", ["2330", "==>name", "two words==>name", "2330==>"])
def test_unverified_company_label_rejected(label):
    with pytest.raises(ValueError):
        company_code(label)


@pytest.mark.parametrize("value", ["197101", 197101, "1971-01", "1971/01"])
def test_native_month_labels_are_not_excel_serials(value):
    assert normalize_period(value, system="excel_1900") == "1971-01"


def scope_proof():
    return dict(contract='editable_source_scope_v1',company_group_enabled=True,date_group_enabled=True,
                vendor_notices_absent=True,binding_readback_verified=True)


def empty_export():
    doc = export()
    for key in ("cells", "preview", "date_system"):
        doc.pop(key)
    return {**doc, "contract_version":4,"source_scope_proof":scope_proof(),"source_outcome": "explicit_empty_scope",
            "capture_method": "owned_vendor_empty_dialog", "source_message": "ERROR1:No data !!(wbk22)"}


def test_explicit_empty_response_resolves_scope_not_data_and_recovers_once(registry):
    repo, root, _, _ = registry
    run_one(root, FakeBridge())

    class Empty(FakeBridge):
        def execute(self, root, task):
            payload = {**empty_export(), "task_id": task["task_id"]}
            output = root / "raw" / (task["task_id"] + "-empty.json")
            atomic_write_json(output, payload)
            return payload, output, 1.0

    assert run_one(root, Empty()) == "completed_task"
    status = build_tej_public_status(repo)
    assert status["state"] == "query_scope_checked"
    assert status["workload"]["exported_rows"] == 0
    assert status["workload"]["resolved_grid_rows"] == 4
    assert status["workload"]["source_empty_tasks"] == 1
    assert status["workload"]["global_row_ratio"] == 0
    assert status["workload"]["global_query_scope_ratio"] == 1
    with closing(connect(root)) as con:
        task = dict(con.execute("SELECT * FROM tasks WHERE kind='download'").fetchone())
    recover_evidence(root, task["task_id"], root / task["output_path"])
    assert build_tej_public_status(repo)["workload"]["source_empty_tasks"] == 1


@pytest.mark.parametrize("mutation", ["notice", "method", "credentials", "data", "outcome"])
def test_empty_response_is_not_a_generic_error_or_data_discard(mutation):
    doc = empty_export()
    if mutation == "notice": doc["source_message"] = "Access denied / quota reached"
    if mutation == "method": doc["capture_method"] = "assumed_empty"
    if mutation == "credentials": doc["credentials_read"] = True
    if mutation == "data": doc["cells"] = export()["cells"]
    if mutation == "outcome": doc["source_outcome"] = "unknown"
    with pytest.raises(ValueError):
        validate_download_evidence(request(), doc)


def test_zero_tasks_is_not_complete(registry):
    repo, root, _, _ = registry
    with closing(connect(root)) as con, con:
        con.execute("DELETE FROM tasks")
    assert build_tej_public_status(repo)["state"] == "queued"


def test_month_menu_search_expands_only_lower_boundary_not_data_scope(registry):
    _, root, _, _ = registry
    with closing(connect(root)) as con, con:
        task = dict(con.execute("SELECT * FROM tasks").fetchone())
        req = json.loads(task["request_json"])
        req["frequency"] = "monthly"
        task["request_json"] = json.dumps(req)
    payload = {"contract_version":4,"source_scope_proof":scope_proof(), "provider":"tej_smart_wizard", "action":"plan",
               "task_id":task["task_id"], "date_axis":DATE_AXIS, "credentials_read":False,
               "universe_scope":"exact_type_smart_id_all_sectors", "calendar_date_mode":False,
               "checkbox_verification_method":"msaa_role44_state_flags",
               **{k:req[k] for k in ("type", "smart_id", "table", "fields")},
               "company_labels":["2330==>TSMC"], "date_labels":["201401"]}
    discovery_tasks(root, task, payload, root / "planned.json")
    with closing(connect(root)) as con:
        query = json.loads(con.execute("SELECT request_json FROM tasks WHERE kind='download'").fetchone()[0])
    assert query["start"] == "2013-12-31"
    assert query["end"] == "2014-01-03"
    assert query["date_labels"] == ["201401"]


def full_grid_export():
    doc = export()
    doc.pop("date_system")
    cells = [doc["cells"][0], *[sample[1] for sample in doc["preview"][1][1:]]]
    return {**doc, "contract_version":4,"source_scope_proof":scope_proof(), "cells":cells,
            "capture_method":"native_msaa_preview_full",
            "source_value_representation":"vendor_display_strings_not_underlying_excel_values",
            "source_grid_rows":4, "source_grid_columns":4}


def test_v4_complete_source_display_grid_preserves_raw_and_exact_decimal_columns(registry):
    import pyarrow.parquet as pq
    _, root, _, _ = registry
    run_one(root, FakeBridge())

    class Full(FakeBridge):
        def execute(self, root, task):
            payload = {**full_grid_export(), "task_id":task["task_id"]}
            output = root / "raw" / (task["task_id"] + "-full.json")
            atomic_write_json(output, payload)
            return payload, output, 1.0

    assert run_one(root, Full()) == "completed_task"
    with closing(connect(root)) as con:
        task = dict(con.execute("SELECT * FROM tasks WHERE kind='download'").fetchone())
    receipt = json.loads((root / task["receipt_path"]).read_text())
    assert receipt["contract_version"] == 4
    assert receipt["source_value_representation"] == "vendor_display_strings_not_underlying_excel_values"
    assert "not_independent_excel_parity" in receipt["preview_parity"]
    assert "datasets/v4/" in receipt["parquet_path"]
    frame = pq.read_table(root / receipt["parquet_path"])
    assert frame.column("Ratio%").to_pylist() == ["-1.5", "0", "", "2.5"]
    normalized = [n for n in frame.column_names if n.startswith("_normalized_shares_")]
    assert [str(v) if v is not None else None for v in frame.column(normalized[0]).to_pylist()] == ["10000", None, "20000", "0"]
    assert receipt["training_auto_injected"] is False


def test_all_checked_sparse_nonempty_scopes_are_not_all_grid_rows_exported(registry):
    repo, root, _, _ = registry
    run_one(root, FakeBridge())

    class Sparse(FakeBridge):
        def execute(self, root, task):
            payload = {**full_grid_export(), "task_id":task["task_id"]}
            payload["cells"] = payload["cells"][:2]
            payload["source_grid_rows"] = 1
            payload["preview"] = [2, payload["preview"][1][:2]]
            output = root / "raw" / (task["task_id"] + "-sparse.json")
            atomic_write_json(output, payload)
            return payload, output, 1.0

    assert run_one(root, Sparse()) == "completed_task"
    status = build_tej_public_status(repo)
    assert status["state"] == "query_scope_checked"
    assert status["workload"]["exported_rows"] == 1
    assert status["workload"]["resolved_grid_rows"] == 4
    assert status["workload"]["source_empty_tasks"] == 0


@pytest.mark.parametrize("key,value", [("source_grid_rows",3),("source_grid_columns",99),
                                      ("source_value_representation","excel_value2"),
                                      ("capture_method","assumed_source")])
def test_v3_cannot_masquerade_as_excel_or_omit_source_shape(key, value):
    doc = full_grid_export()
    doc[key] = value
    with pytest.raises(ValueError):
        validate_export(request(), doc)


@pytest.mark.parametrize("value", ["1,000", "NaN", "N/A", True, "12%", "1.2.3"])
def test_numeric_display_locale_and_sentinels_not_guessed(value):
    with pytest.raises(ValueError):
        display_decimal(value)


def test_display_decimal_keeps_negative_zero_and_scientific_values():
    assert str(display_decimal("-0.00")) == "-0.00"
    assert str(display_decimal("-1.250")) == "-1.250"
    assert display_decimal("1e3") == 1000


def test_operator_recovery_adopts_existing_result_without_query(registry):
    repo, root, _, _ = registry
    run_one(root, FakeBridge())
    with closing(connect(root)) as con, con:
        task = dict(con.execute("SELECT * FROM tasks WHERE kind='download'").fetchone())
        con.execute("UPDATE tasks SET state='blocked',last_error_code='unknown_outcome_no_auto_retry' WHERE task_id=?", (task["task_id"],))
    req=task_request(root,task)
    atomic_write_json(root/'raw'/(task['task_id']+'-original.json.stage.json'),{
        'contract_version':4,'task_id':task['task_id'],'stage':'prepreview_verified',
        **{k:req[k] for k in ('type','smart_id','table','fields','company_labels','date_labels')},
        'source_scope_proof':scope_proof()})

    class Existing(FakeBridge):
        def execute(self, root, task):
            assert json.loads(task["request_json"])["action"] == "recover_preview"
            return super().execute(root, task)

    result = recover_desktop_response(root, task["task_id"], Existing(), response="preview")
    assert result["data_query_repeated"] is False
    assert build_tej_public_status(repo)["workload"]["exported_rows"] == 4
    with pytest.raises(ValueError, match="unresolved"):
        recover_desktop_response(root, task["task_id"], Existing(), response="preview")


def test_error_acknowledgement_requires_explicit_inspected_error_window(registry):
    _, root, _, _ = registry
    run_one(root, FakeBridge())
    with closing(connect(root)) as con, con:
        task = dict(con.execute("SELECT * FROM tasks WHERE kind='download'").fetchone())
        con.execute("UPDATE tasks SET state='blocked' WHERE task_id=?", (task["task_id"],))
    with pytest.raises(ValueError, match="exact inspected"):
        recover_desktop_response(root, task["task_id"], FakeBridge(), response="excel_error")


def test_sparse_eta_uses_query_scope_units_not_result_row_units(registry):
    repo, root, _, _ = registry
    with closing(connect(root)) as con, con:
        table_id = con.execute("SELECT table_id FROM tables").fetchone()[0]
        con.execute("DELETE FROM tasks")
        con.execute("UPDATE tables SET grid_rows=300,grid_dates=30,universe_count=10,state='backfilling'")
        for i, state, actual_rows in [(1,"complete",1),(2,"complete",0),(3,"pending",None)]:
            con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,state,expected_rows,actual_rows,actual_bytes,seconds,timing_basis,scope_contract) VALUES(?,?,'download','P1',100,'{}',?,100,?,1000,?,'fresh_end_to_end','editable_source_scope_v1')",
                        (str(i),table_id,state,actual_rows,10 if state == "complete" else None))
    status = build_tej_public_status(repo)
    table = status["tables"][0]
    assert table["remaining_seconds"] == 10
    assert table["exported_rows"] == 1
    assert table["resolved_grid_rows"] == 200
    assert table["source_empty_tasks"] == 1
    assert table["estimated_total_bytes"] >= table["recorded_bytes"]
    assert table["estimated_total_export_rows"] >= table["exported_rows"]
    assert status["workload"]["global_query_scope_ratio"] == pytest.approx(2 / 3)


def test_compact_queue_reconstructs_exact_scopes_and_source_evidence(registry):
    _, root, _, _ = registry
    run_one(root, FakeBridge())
    with closing(connect(root)) as con:
        tasks = [dict(r) for r in con.execute("SELECT * FROM tasks")]
        original = {t['task_id']:task_request(root,t) for t in tasks}
    proof = compact_queue(root)
    assert proof['exact_request_reconstruction_verified'] is True
    assert proof['source_data_or_receipts_deleted'] is False
    with closing(connect(root)) as con:
        tasks = [dict(r) for r in con.execute("SELECT * FROM tasks")]
    assert {t['task_id']:task_request(root,t) for t in tasks} == original
    assert run_one(root, FakeBridge()) == 'completed_task'
    assert run_one(root, FakeBridge()) == 'idle'
    assert compact_queue(root)['changed_tasks'] == 0


def test_changed_schema_reference_cannot_silently_resume(registry):
    _, root, _, _ = registry
    compact_queue(root)
    with closing(connect(root)) as con:
        task = dict(con.execute("SELECT * FROM tasks").fetchone())
    request = json.loads(task['request_json'])
    request['_fields_ref']['schema_sha256'] = 'wrong'
    task['request_json'] = json.dumps(request)
    with pytest.raises(ValueError,match='schema reference'):
        task_request(root,task)


def test_long_active_owner_is_not_called_stalled_and_reused_pid_is_rejected():
    import os
    from datetime import timedelta
    from stockagent.live.tej_dashboard import _worker_alive
    now = datetime.now(UTC)
    worker = {'state':'running','observed_at_utc':(now-timedelta(seconds=400)).isoformat(),
              'deadline_at_utc':(now+timedelta(seconds=500)).isoformat(),'owner_pid':os.getpid(),
              'owner_start_ticks':Path(f'/proc/{os.getpid()}/stat').read_text().rsplit(') ',1)[1].split()[19]}
    assert _worker_alive(worker,now)
    assert not _worker_alive({**worker,'owner_start_ticks':'different'},now)
    assert not _worker_alive(worker,now+timedelta(seconds=501))


def test_low_disk_stops_before_source_action_without_erasing_pending_work(registry,monkeypatch):
    from types import SimpleNamespace
    import downloader.tej_history as collector
    repo, root, _, _ = registry
    monkeypatch.setattr(collector.shutil,'disk_usage',lambda path:SimpleNamespace(free=0))
    class NoCalls:
        def execute(self,*args):raise AssertionError('No provider action with low disk')
    assert run_one(root,NoCalls()) == 'local_disk_headroom_low'
    with closing(connect(root)) as con:
        assert con.execute("SELECT COUNT(*) FROM tasks WHERE state='pending'").fetchone()[0] == 1
        assert con.execute("SELECT COUNT(*) FROM traffic").fetchone()[0] == 0
    assert build_tej_public_status(repo)['state'] == 'paused_for_storage'


@pytest.mark.parametrize('response,action', [('plan','recover_plan'),
    ('plan-empty-fields','resume_plan_empty_fields'), ('plan-preparation','plan')])
def test_scoped_plan_recovery_only_accepts_unresolved_discovery(registry,response,action):
    _, root, _, _ = registry
    with closing(connect(root)) as con, con:
        task = dict(con.execute("SELECT * FROM tasks WHERE kind='discover'").fetchone())
        con.execute("UPDATE tasks SET state='blocked',last_error_code='unknown_outcome_no_auto_retry'")

    class Preparation(FakeBridge):
        def execute(self, root, task):
            assert json.loads(task['request_json'])['action'] == action
            assert task['kind'] == 'discover'
            return super().execute(root,task)

    assert recover_desktop_response(root,task['task_id'],Preparation(),response=response)['data_query_repeated'] is False
    with closing(connect(root)) as con, con:
        download = dict(con.execute("SELECT * FROM tasks WHERE kind='download'").fetchone())
        con.execute("UPDATE tasks SET state='blocked' WHERE kind='download'")
    with pytest.raises(ValueError,match='matching kind'):
        recover_desktop_response(root,download['task_id'],Preparation(),response=response)


@pytest.mark.parametrize('mutation', [None,'task_id','table','provider','action','submission_possible','error_code','absent'])
def test_bridge_safe_retry_requires_exact_private_prequery_proof(registry,monkeypatch,mutation):
    from types import SimpleNamespace
    import downloader.tej_history as collector
    _, root, _, _ = registry
    run_one(root,FakeBridge())
    with closing(connect(root)) as con:
        task = dict(con.execute("SELECT * FROM tasks WHERE kind='download'").fetchone())
    req = task_request(root,task)
    session = dict(TejProcessId=1,ExpectedWindow=2,ExpectedTitle='reviewed test title',
                   ExpectedWorkbook='Book2',ExpectedExcelWindow=3)
    monkeypatch.setattr(DesktopBridge,'windows_path',staticmethod(lambda path:str(path)))
    def fail(*args,**kwargs):
        request_path = next((root/'requests').glob('*.json'))
        output = root/'raw'/request_path.name
        proof = {'contract_version':4,'provider':'tej_smart_wizard','task_id':task['task_id'],
                 **{k:req[k] for k in ('type','smart_id','table')},'action':'download',
                 'market_data_query_submission_possible':False,
                 'error_code':'local_date_input_failed_before_preview'}
        if mutation == 'submission_possible':proof['market_data_query_submission_possible'] = True
        elif mutation in {'task_id','table','provider','action','error_code'}:proof[mutation] = 'wrong'
        if mutation != 'absent':atomic_write_json(output.with_suffix('.json.outcome.json'),proof)
        return SimpleNamespace(returncode=1,stderr=b'SECRET private diagnostic',stdout=b'')
    monkeypatch.setattr(collector.subprocess,'run',fail)
    expected = BeforeDataQueryError if mutation is None else RuntimeError
    with pytest.raises(expected) as caught:
        DesktopBridge(root,session).execute(root,task)
    assert 'SECRET' not in str(caught.value)
    if mutation is not None:assert not isinstance(caught.value,BeforeDataQueryError)
    diagnostic = next((root/'diagnostics').glob('*.txt'))
    assert diagnostic.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize('mutation',[None,'request','diagnostic','script','callsite','stage','readback','retry_limit'])
def test_legacy_foreground_recovery_requires_original_proof_and_never_submits(registry,monkeypatch,mutation):
    import hashlib
    import downloader.tej_history as collector
    _,root,_,_=registry
    run_one(root,FakeBridge())
    with closing(connect(root)) as con,con:
        task=dict(con.execute("SELECT * FROM tasks WHERE kind='download'").fetchone())
        con.execute("UPDATE tasks SET state='blocked',last_error_code='unknown_outcome_no_auto_retry',safe_prequery_retries=? WHERE task_id=?",(2 if mutation=='retry_limit' else 0,task['task_id']))
    req=task_request(root,task)
    prepared=root/'requests'/(task['task_id']+'-'+'a'*32+'.json')
    original={**req,'contract_version':4,'task_id':task['task_id']}
    if mutation=='request':original['table']='wrong'
    atomic_write_json(prepared,original)
    source=root/'original-bridge.ps1'
    code=b'original reviewed script\n[TejBridgeNative]::DateText($ExpectedWindow,$startEdit)\n'
    source.write_bytes(code)
    monkeypatch.setattr(collector,'LEGACY_DATE_INPUT_BRIDGE_SHA256',hashlib.sha256(code).hexdigest())
    if mutation=='script':source.write_bytes(b'current substituted script')
    monkeypatch.setattr(collector.subprocess,'check_output',lambda *a,**k:str(source))
    message='Exception calling "DateText" with "4" argument(s): "Foreground unavailable; no input sent"'
    if mutation=='diagnostic':message='Exception calling "Preview": "Foreground unavailable; no input sent"'
    stack='at <ScriptBlock>, C:\\Users\\test\\AppData\\Local\\StockAgent\\TEJSmartWizard\\worker-'+'b'*32+'\\bridge.ps1: line '+('1' if mutation=='callsite' else '2')
    diagnostic=root/'diagnostics'/prepared.with_suffix('.txt').name
    diagnostic.parent.mkdir(exist_ok=True)
    diagnostic.write_text(message+'\n'+stack+'\nat <ScriptBlock>, <No file>: line 1\n')
    if mutation=='stage':atomic_write_json(root/'raw'/(prepared.name+'.stage.json'),{'stage':'prepreview_verified'})
    class Readback:
        def __init__(self):self.calls=[]
        def execute(self,root,task):
            check=json.loads(task['request_json']);self.calls.append(check['action'])
            assert check['action']=='confirm_metadata_error_cleared'
            payload={'contract_version':4,'provider':'tej_smart_wizard','task_id':task['task_id'],
                     'action':check['action'],**{k:req[k] for k in ('type','smart_id','table')},
                     'vendor_notices_absent':True,'source_selectors_enabled':True,
                     'source_binding_stable':mutation!='readback','market_data_query_submitted':False,'credentials_read':False}
            output=root/'raw'/'readback.json';atomic_write_json(output,payload);return payload,output,1
    bridge=Readback()
    if mutation not in {None,'retry_limit'}:
        with pytest.raises(ValueError):recover_legacy_date_input(root,task['task_id'],bridge,prepared)
        with closing(connect(root)) as con:
            assert con.execute("SELECT last_error_code FROM tasks WHERE task_id=?",(task['task_id'],)).fetchone()[0]=='unknown_outcome_no_auto_retry'
        assert bridge.calls==(['confirm_metadata_error_cleared'] if mutation=='readback' else [])
    else:
        result=recover_legacy_date_input(root,task['task_id'],bridge,prepared)
        assert result['state']==('date_input_prequery_needs_review' if mutation=='retry_limit' else 'prequery_retry_scheduled')
        assert result['market_data_query_repeated'] is False and result['source_rows_adopted'] is False
        with closing(connect(root)) as con:
            row=con.execute('SELECT * FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()
            assert row['actual_rows'] is None and row['safe_prequery_retries']==(2 if mutation=='retry_limit' else 1)


def test_bridge_timeout_never_exposes_command_or_retries(registry,monkeypatch):
    import subprocess
    import downloader.tej_history as collector
    _, root, _, _ = registry
    with closing(connect(root)) as con:
        task = dict(con.execute('SELECT * FROM tasks').fetchone())
    monkeypatch.setattr(DesktopBridge,'windows_path',staticmethod(lambda path:str(path)))
    def timeout(*args,**kwargs):
        raise subprocess.TimeoutExpired('SECRET command',900,stderr=b'private vendor path')
    monkeypatch.setattr(collector.subprocess,'run',timeout)
    session = dict(TejProcessId=1,ExpectedWindow=2,ExpectedTitle='test',ExpectedWorkbook='Book2',ExpectedExcelWindow=3)
    with pytest.raises(RuntimeError,match='requires_recovery') as caught:
        DesktopBridge(root,session).execute(root,task)
    assert 'SECRET' not in str(caught.value)
    assert caught.value.__suppress_context__


def test_proven_list_selection_retry_is_bounded_not_an_unknown_preview(registry):
    _,root,_,_=registry
    run_one(root,FakeBridge())
    class SelectionFailure:
        def execute(self,*args):raise BeforeDataQueryError('local_list_selection_failed_before_preview')
    for _ in range(2):
        assert run_one(root,SelectionFailure())=='prequery_retry_scheduled'
        with closing(connect(root)) as con,con:
            con.execute("UPDATE tasks SET next_attempt_at_utc=NULL WHERE kind='download'")
    assert run_one(root,SelectionFailure())=='list_selection_prequery_needs_review'
    with closing(connect(root)) as con:
        assert con.execute("SELECT actual_rows FROM tasks WHERE kind='download'").fetchone()[0] is None


@pytest.mark.parametrize('input_kind', ['date','list','query'])
@pytest.mark.parametrize('mutation', [None,'request','unknown','completed','task_id','table','provider',
    'action','submission_possible','error_code','stale','stage','source_rows','readback','readback_query','readback_credentials'])
def test_operator_local_input_recovery_requires_latest_unsent_proof_and_never_queries(registry,input_kind,mutation):
    _,root,_,_=registry
    run_one(root,FakeBridge())
    code={'date':'local_date_input_failed_before_preview','list':'local_list_selection_failed_before_preview',
          'query':'local_query_activation_failed_before_preview'}[input_kind]
    from downloader.tej_history import PREQUERY_FAILURES
    blocked=PREQUERY_FAILURES[code]
    with closing(connect(root)) as con,con:
        task=dict(con.execute("SELECT * FROM tasks WHERE kind='download'").fetchone())
        con.execute("UPDATE tasks SET state=?,last_error_code=?,safe_prequery_retries=2,attempted_at_utc=? WHERE task_id=?",
                    ('complete' if mutation=='completed' else 'blocked',
                     'unknown_outcome_no_auto_retry' if mutation=='unknown' else blocked,
                     '2026-10-02T03:00:00+00:00',task['task_id']))
    req=task_request(root,task)
    prepared=root/'requests'/(task['task_id']+'-'+'a'*32+'.json')
    original={**req,'contract_version':4,'task_id':task['task_id']}
    if mutation=='request':original['fields']=['another field']
    atomic_write_json(prepared,original)
    proof={'provider':'tej_smart_wizard','contract_version':4,'action':'download','task_id':task['task_id'],
           **{k:req[k] for k in ('type','smart_id','table')},'error_code':code,
           'market_data_query_submission_possible':False,'observed_at_utc':'2026-10-02T03:00:05+00:00'}
    if mutation in {'task_id','table','provider','action','error_code'}:proof[mutation]='different'
    if mutation=='submission_possible':proof['market_data_query_submission_possible']=True
    if mutation=='stale':proof['observed_at_utc']='2026-10-02T02:59:59+00:00'
    output=root/'raw'/prepared.name
    atomic_write_json(output.with_suffix('.json.outcome.json'),proof)
    if mutation=='stage':atomic_write_json(output.with_suffix('.json.stage.json'),{'stage':'prepreview_verified'})
    if mutation=='source_rows':atomic_write_json(output,{'cells':[['source data']]})
    class Readback:
        def __init__(self):self.calls=[]
        def execute(self,root,task):
            check=json.loads(task['request_json']);self.calls.append(check['action'])
            assert check['action']=='confirm_metadata_error_cleared'
            payload={'provider':'tej_smart_wizard','contract_version':4,'action':check['action'],
                     'task_id':task['task_id'],**{k:req[k] for k in ('type','smart_id','table')},
                     'source_binding_stable':mutation!='readback','source_selectors_enabled':True,
                     'vendor_notices_absent':True,'market_data_query_submitted':mutation=='readback_query',
                     'credentials_read':mutation=='readback_credentials'}
            readback=root/'raw'/'readonly.json';atomic_write_json(readback,payload)
            return payload,readback,0.1
    bridge=Readback()
    if mutation is None:
        result=recover_verified_input(root,task['task_id'],bridge,prepared)
        assert result=={'state':'prequery_retry_scheduled','data_query_repeated':False,'source_rows_adopted':False}
        audit=json.loads(next((root/'diagnostics').glob('*-verified_input_recovery-*.json')).read_text())
        assert audit['prior_safe_prequery_retries']==2 and audit['automatic_recovery'] is False
        with closing(connect(root)) as con:
            row=con.execute('SELECT * FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()
            assert row['state']=='pending' and row['safe_prequery_retries']==1
            assert row['actual_rows'] is None and row['last_error_code']==code
        assert bridge.calls==['confirm_metadata_error_cleared']
    else:
        with pytest.raises(ValueError):recover_verified_input(root,task['task_id'],bridge,prepared)
        with closing(connect(root)) as con:
            row=con.execute('SELECT * FROM tasks WHERE task_id=?',(task['task_id'],)).fetchone()
            assert row['safe_prequery_retries']==2 and row['actual_rows'] is None
        assert bridge.calls==(['confirm_metadata_error_cleared'] if mutation.startswith('readback') else [])


@pytest.mark.parametrize('selection', [None,'vendor_default_dash_not_all_units_verified',
    'vendor_scale_Thousand_units_require_source_review'])
def test_source_scale_metadata_is_preserved_and_explicit_conversion_not_multiplied_twice(registry,selection):
    import pyarrow.parquet as pq
    _, root, _, _ = registry
    run_one(root,FakeBridge())
    class Scaled(FakeBridge):
        def execute(self,root,task):
            payload = {**full_grid_export(),'task_id':task['task_id'],
                       'vendor_numeric_scale_selection':selection,'sector_filter_applicable':False}
            output = root/'raw'/(task['task_id']+'-scaled.json')
            atomic_write_json(output,payload)
            return payload,output,1.0
    assert run_one(root,Scaled()) == 'completed_task'
    with closing(connect(root)) as con:
        task = dict(con.execute("SELECT * FROM tasks WHERE kind='download'").fetchone())
    receipt = json.loads((root/task['receipt_path']).read_text())
    assert receipt['vendor_numeric_scale_selection'] == selection
    assert receipt['sector_filter_applicable'] is False
    assert receipt['all_source_units_verified'] is False
    frame = pq.read_table(root/receipt['parquet_path'])
    normalized = [n for n in frame.column_names if n.startswith('_normalized_shares_')]
    assert bool(normalized) is not (isinstance(selection,str) and selection.startswith('vendor_scale_'))
    assert frame.column('Volume(1000S)').to_pylist() == ['10','','20','0']


@pytest.mark.parametrize('mutation',['company_group_enabled','date_group_enabled','vendor_notices_absent',
                                     'binding_readback_verified','missing','old_contract','numeric_true'])
def test_v4_cannot_adopt_disabled_or_unproven_cached_scope(mutation):
    doc=full_grid_export()
    req={**request(),'contract_version':4}
    if mutation=='missing':doc.pop('source_scope_proof')
    elif mutation=='old_contract':doc['contract_version']=3
    elif mutation=='numeric_true':doc['source_scope_proof']['company_group_enabled']=1
    else:doc['source_scope_proof'][mutation]=False
    with pytest.raises(ValueError,match='source scope proof'):
        validate_export(req,doc)


def test_scope_revalidation_preserves_sources_receipts_and_completed_task_states(registry):
    import hashlib
    repo,root,_,_=registry
    run_one(root,FakeBridge());run_one(root,FakeBridge())
    with closing(connect(root)) as con,con:
        con.execute("DELETE FROM meta WHERE key='editable_scope_contract'")
        con.execute("UPDATE tasks SET task_id='legacy_'||task_id,scope_contract=NULL")
        completed_ids=[r[0] for r in con.execute("SELECT task_id FROM tasks WHERE state='complete'")]
    originals={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest()
               for folder in ('raw','datasets','receipts') for p in (root/folder).rglob('*') if p.is_file()}
    outcome=revalidate_source_scopes(root)
    assert outcome['tables_requeued']==1 and outcome['provider_data_queries_sent']==0
    assert outcome['legacy_exports_preserved']['exported_rows']==4
    assert revalidate_source_scopes(root)['state']=='already_revalidated_queue_registered'
    assert originals=={name:hashlib.sha256((root/name).read_bytes()).hexdigest() for name in originals}
    with closing(connect(root)) as con:
        assert all(con.execute("SELECT state FROM tasks WHERE task_id=?",(identity,)).fetchone()[0]=='complete' for identity in completed_ids)
        assert con.execute("SELECT COUNT(*) FROM tasks WHERE state='pending' AND scope_contract='editable_source_scope_v1'").fetchone()[0]==1
    status=build_tej_public_status(repo)
    assert status['workload']['exported_rows']==0 and status['workload']['total_rows'] is None
    assert status['legacy_exports']['exported_rows']==4
    assert status['tables'][0]['first_available_query_period'] is None
    assert build_tej_feature_page(repo)['features'][0]['exported_non_null_cells'] is None


@pytest.mark.parametrize('state,code',[('running',None),('blocked','unknown_outcome_no_auto_retry')])
def test_scope_revalidation_refuses_an_unresolved_running_query(registry,state,code):
    _,root,_,_=registry
    with closing(connect(root)) as con,con:
        con.execute("DELETE FROM meta WHERE key='editable_scope_contract'")
        con.execute("UPDATE tasks SET state=?,last_error_code=?",(state,code))
    with pytest.raises(ValueError,match='running query'):
        revalidate_source_scopes(root)


def test_v4_recovery_requires_original_query_scope_attestation(registry):
    _,root,_,_=registry
    run_one(root,FakeBridge())
    with closing(connect(root)) as con,con:
        task=dict(con.execute("SELECT * FROM tasks WHERE kind='download'").fetchone())
        con.execute("UPDATE tasks SET state='blocked' WHERE task_id=?",(task['task_id'],))
    class NoCall:
        def execute(self,*args):raise AssertionError('No source call without original query proof')
    with pytest.raises(ValueError,match='original prepreview scope proof'):
        recover_desktop_response(root,task['task_id'],NoCall(),response='preview')


@pytest.mark.parametrize('mutation',[None,'task_id','error_code','submitted','selector_disabled','credentials'])
def test_metadata_failure_settlement_is_exact_read_only_and_keeps_failed_table_visible(registry,mutation):
    repo,root,_,_=registry
    with closing(connect(root)) as con,con:
        task=dict(con.execute("SELECT * FROM tasks WHERE kind='discover'").fetchone())
        con.execute("UPDATE tasks SET state='blocked',last_error_code='unknown_outcome_no_auto_retry'")
    req=task_request(root,task)
    ack={'contract_version':4,'provider':'tej_smart_wizard','action':'ack_source_memory_error',
         'task_id':task['task_id'],**{k:req[k] for k in ('type','smart_id','table')},
         'error_code':'vendor_mysql_allocation_failed','data_query_repeated':False,'credentials_read':False}
    if mutation in {'task_id','error_code'}:ack[mutation]='wrong'
    evidence=root/'raw'/'exact-runtime-ack.json';atomic_write_json(evidence,ack)
    class Readback:
        def execute(self,root,task):
            request=json.loads(task['request_json'])
            assert request['action']=='confirm_metadata_error_cleared'
            payload={'contract_version':4,'provider':'tej_smart_wizard','action':request['action'],
                     'task_id':task['task_id'],**{k:req[k] for k in ('type','smart_id','table')},
                     'vendor_notices_absent':True,'source_selectors_enabled':True,
                     'company_group_enabled':False,'date_group_enabled':False,
                     'market_data_query_submitted':mutation=='submitted','credentials_read':mutation=='credentials'}
            if mutation=='selector_disabled':payload['source_selectors_enabled']=False
            output=root/'raw'/'exact-readback.json';atomic_write_json(output,payload)
            return payload,output,1
    if mutation is not None:
        with pytest.raises(ValueError):settle_metadata_failure(root,task['task_id'],Readback(),evidence)
        with closing(connect(root)) as con:
            assert con.execute('SELECT last_error_code FROM tasks').fetchone()[0]=='unknown_outcome_no_auto_retry'
        return
    result=settle_metadata_failure(root,task['task_id'],Readback(),evidence)
    assert result['failed_table_still_blocked'] and not result['data_query_repeated'] and not result['source_axes_adopted']
    status=build_tej_public_status(repo)
    assert status['state']=='needs_review' and status['workload']['total_rows'] is None
    assert status['tables'][0]['last_error_code']=='vendor_metadata_allocation_failed_deferred'


def test_metadata_settlement_never_clears_an_unknown_data_preview(registry):
    _,root,_,_=registry
    run_one(root,FakeBridge())
    with closing(connect(root)) as con,con:
        task=dict(con.execute("SELECT * FROM tasks WHERE kind='download'").fetchone())
        con.execute("UPDATE tasks SET state='blocked',last_error_code='unknown_outcome_no_auto_retry' WHERE task_id=?",(task['task_id'],))
    class NoCall:
        def execute(self,*args):raise AssertionError('No source call for an unknown data Preview')
    with pytest.raises(ValueError,match='never a data query'):
        settle_metadata_failure(root,task['task_id'],NoCall(),root/'raw'/'unused.json')


class MetadataRecoveryBridge:
    session = {'ExpectedWindow':1234}

    def __init__(self, *, notices=None, mutation=None):
        self.notices=list(notices or []);self.mutation=mutation;self.calls=[]

    def execute(self,root,task):
        req=json.loads(task['request_json']);action=req['action'];self.calls.append(action)
        base={'contract_version':4,'provider':'tej_smart_wizard','task_id':task['task_id'],
              **{k:req[k] for k in ('type','smart_id','table')},'action':action,'credentials_read':False}
        if action=='plan':
            original=root/'requests'/(task['task_id']+'-original.json')
            atomic_write_json(original,{**req,'contract_version':4,'task_id':task['task_id']})
            raise MetadataPreparationError(original)
        if action=='inspect_notices':
            result={**base,'notices':self.notices[:1],'notice_acknowledged':False,'data_query_repeated':False}
        elif action=='ack_source_memory_error':
            self.notices.pop(0)
            result={**base,'error_code':req['error_code'],'data_query_repeated':False}
        elif action=='confirm_metadata_error_cleared':
            result={**base,'vendor_notices_absent':True,'source_selectors_enabled':True,
                    'source_binding_stable':True,'market_data_query_submitted':False,
                    'binding_matches_failed_plan':False,'source_axes_adopted':False}
            if self.mutation=='selector':result['source_selectors_enabled']=False
            if self.mutation=='unstable':result['source_binding_stable']=False
            if self.mutation=='submitted':result['market_data_query_submitted']=True
            if self.mutation=='credentials':result['credentials_read']=True
        else:raise AssertionError('Never repeat a metadata plan or submit a data Preview')
        path=root/'raw'/(task['task_id']+'-'+str(len(self.calls))+'.json')
        atomic_write_json(path,result)
        return result,path,1


def runtime_notice(message='MySql:malloc',owner=1234):
    return {'handle':99,'owner':owner,'controls':[
        {'class':'Static','name':message}, {'class':'Button','name':'OK','enabled':True}]}


@pytest.mark.parametrize('notices',[[],[runtime_notice()],
                                  [runtime_notice(),runtime_notice('Cannot find table 0.')]])
def test_verified_metadata_only_failure_is_isolated_without_retry_or_fake_completion(registry,notices):
    repo,root,_,config=registry
    configure_runtime_policy(root,{**config,'metadata_failure_isolation':True,
                                   'acknowledge_known_metadata_runtime_notices':True})
    bridge=MetadataRecoveryBridge(notices=notices)
    expected='vendor_metadata_allocation_failed_deferred' if notices else 'metadata_preparation_failed_deferred'
    assert run_one(root,bridge)==expected
    assert bridge.calls.count('plan')==1
    with closing(connect(root)) as con:
        row=con.execute('SELECT state,last_error_code FROM tasks').fetchone()
        assert row['state']=='blocked' and row['last_error_code']==expected
        assert con.execute("SELECT state FROM traffic").fetchone()[0]=='failed'
    assert run_one(root,FakeBridge())=='idle'
    status=build_tej_public_status(repo)
    assert status['workload']['exported_rows']==0 and status['workload']['resolved_grid_rows']==0
    assert status['workload']['discovered_tables']==0


@pytest.mark.parametrize('mutation',['selector','unstable','submitted','credentials'])
def test_unverified_metadata_context_keeps_global_barrier(registry,mutation):
    _,root,_,config=registry
    configure_runtime_policy(root,{**config,'metadata_failure_isolation':True})
    bridge=MetadataRecoveryBridge(mutation=mutation)
    assert run_one(root,bridge)=='unknown_outcome_no_auto_retry'
    assert run_one(root,FakeBridge())=='inflight_requires_recovery'


@pytest.mark.parametrize('notices',[[runtime_notice('quota exceeded')],[runtime_notice('please login')],
                                  [runtime_notice(owner=999)],[runtime_notice('MySql:malloc extra')]])
def test_unknown_quota_login_foreign_and_changed_notices_are_never_acknowledged(registry,notices):
    _,root,_,config=registry
    configure_runtime_policy(root,{**config,'metadata_failure_isolation':True,
                                   'acknowledge_known_metadata_runtime_notices':True})
    bridge=MetadataRecoveryBridge(notices=notices)
    assert run_one(root,bridge)=='unknown_outcome_no_auto_retry'
    assert 'ack_source_memory_error' not in bridge.calls
    assert 'confirm_metadata_error_cleared' not in bridge.calls


def test_metadata_notice_acknowledgements_are_finite_and_opt_in(registry):
    _,root,_,config=registry
    configure_runtime_policy(root,{**config,'metadata_failure_isolation':True,
                                   'acknowledge_known_metadata_runtime_notices':True})
    bridge=MetadataRecoveryBridge(notices=[runtime_notice()]*3)
    assert run_one(root,bridge)=='unknown_outcome_no_auto_retry'
    assert bridge.calls.count('ack_source_memory_error')==2


def test_legacy_metadata_failure_can_be_isolated_only_from_exact_original_private_plan(registry):
    _,root,_,_=registry
    bridge=MetadataRecoveryBridge()
    assert run_one(root,bridge)=='unknown_outcome_no_auto_retry' # policy is off
    with closing(connect(root)) as con:task=dict(con.execute('SELECT * FROM tasks').fetchone())
    original=root/'requests'/(task['task_id']+'-original.json')
    result=isolate_failed_metadata(root,task['task_id'],bridge,original)
    assert result['state']=='metadata_preparation_failed_deferred'
    assert bridge.calls.count('plan')==1 and not result['source_axes_adopted']


@pytest.mark.parametrize('mutation',['action','table','fields','version'])
def test_metadata_isolation_refuses_different_original_request_before_any_ui_read(registry,mutation):
    _,root,_,_=registry;bridge=MetadataRecoveryBridge()
    assert run_one(root,bridge)=='unknown_outcome_no_auto_retry'
    with closing(connect(root)) as con:task=dict(con.execute('SELECT * FROM tasks').fetchone())
    original=root/'requests'/(task['task_id']+'-original.json');doc=json.loads(original.read_text())
    doc[{'action':'action','table':'table','fields':'fields','version':'contract_version'}[mutation]]='wrong'
    atomic_write_json(original,doc);calls=list(bridge.calls)
    with pytest.raises(ValueError,match='exact reviewed metadata-only plan'):
        isolate_failed_metadata(root,task['task_id'],bridge,original)
    assert bridge.calls==calls


def test_interleaving_preserves_phase_priority_and_rotates_download_tables(registry):
    _,root,_,config=registry
    configure_runtime_policy(root,config)
    with closing(connect(root)) as con,con:
        for name,table,kind,phase,priority in [('download-a','a','download','P1',100),
                                             ('download-b','b','download','P1',100),
                                             ('later-plan','c','discover','P2',201)]:
            con.execute('INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json) VALUES(?,?,?,?,?,?)',
                        (name,table,kind,phase,priority,'{}'))
        assert _ready_task(con,'2099',None,{'download_burst':4})['task_id']=='download-a'
        con.execute("INSERT INTO meta VALUES ('scheduler_last_download_table','a')")
        assert _ready_task(con,'2099',None,{'download_burst':4})['task_id']=='download-b'
        con.execute("INSERT INTO meta VALUES ('scheduler_consecutive_downloads','4')")
        assert _ready_task(con,'2099',None,{'download_burst':4})['phase']=='P1'
        assert _ready_task(con,'2099',None,{'download_burst':4})['kind']=='discover'
        con.execute("UPDATE tasks SET state='complete' WHERE kind='discover' AND phase='P1'")
        assert _ready_task(con,'2099',None,{'download_burst':4})['kind']=='download'
        assert _ready_task(con,'2099','discover',{'download_burst':4})['task_id']=='later-plan'


def test_finished_finite_batch_does_not_claim_a_live_worker_or_complete_history(registry):
    repo,root,_,_=registry
    mark_batch_finished(root,reason='finite_task_limit_reached',attempted=3,mode='interleaved')
    receipt=json.loads((root/'worker_status.json').read_text())
    assert receipt['history_complete'] is False and receipt['state']=='batch_finished'
    public=build_tej_public_status(repo)
    assert public['state']=='queued' and public['worker']['alive'] is False
    assert public['worker']['batch_mode']=='interleaved'
    assert public['worker']['batch_end_reason']=='finite_task_limit_reached'
    assert public['worker']['attempted_tasks']==3


def test_metadata_recovery_has_live_owner_but_never_claims_data_query_running(registry):
    repo,root,_,_=registry
    mark_worker_wait(root,kind='discover',seconds=900,metadata_recovery=True)
    public=build_tej_public_status(repo)
    assert public['state']=='running' and public['worker']['alive']
    assert public['worker']['state']=='recovering_metadata' and public['workload']['running_tasks']==0


def test_batch_public_projection_does_not_expose_arbitrary_private_context(registry):
    repo,root,_,_=registry
    atomic_write_json(root/'worker_status.json',{'state':'batch_finished','reason':'private path',
        'batch_mode':'private account','attempted_tasks':True})
    worker=build_tej_public_status(repo)['worker']
    assert worker['batch_end_reason'] is None and worker['batch_mode'] is None and worker['attempted_tasks'] is None


def test_fresh_task_timing_includes_commit_instead_of_trusting_bridge_duration(registry):
    _, root, _, _ = registry
    class WrongDuration(FakeBridge):
        def execute(self,root,task):
            payload,output,_ = super().execute(root,task)
            return payload,output,100000.0
    assert run_one(root,WrongDuration()) == 'completed_task'
    with closing(connect(root)) as con:
        task = dict(con.execute("SELECT * FROM tasks WHERE state='complete'").fetchone())
    assert task['timing_basis'] == 'fresh_end_to_end'
    assert 0 < task['seconds'] < 100000


@pytest.mark.parametrize('value', ['12345678901234567890123456789012345678',
    '-12345678901234567890123456789.0123456789', '-0.000'])
@pytest.mark.parametrize('exponent', [3,6])
def test_explicit_unit_scale_never_rounds_at_default_decimal_context(value,exponent):
    raw = display_decimal(value)
    converted = scale_decimal_power10(raw,exponent)
    assert converted.as_tuple().digits == raw.as_tuple().digits
    assert converted.as_tuple().sign == raw.as_tuple().sign
    assert converted.as_tuple().exponent == raw.as_tuple().exponent + exponent


def test_high_precision_unit_conversion_is_exact_in_parquet(registry):
    from decimal import Decimal
    import pyarrow.parquet as pq
    _, root, _, _ = registry
    run_one(root,FakeBridge())
    number = '12345678901234567890123456789012345678'
    class Precise(FakeBridge):
        def execute(self,root,task):
            payload = {**full_grid_export(),'task_id':task['task_id']}
            payload['cells'][1][2] = number
            payload['preview'][1][1][1][2] = number
            output = root/'raw'/(task['task_id']+'-precise.json')
            atomic_write_json(output,payload)
            return payload,output,1.0
    assert run_one(root,Precise()) == 'completed_task'
    with closing(connect(root)) as con:
        task = dict(con.execute("SELECT * FROM tasks WHERE kind='download'").fetchone())
    receipt = json.loads((root/task['receipt_path']).read_text())
    frame = pq.read_table(root/receipt['parquet_path'])
    column = next(n for n in frame.column_names if n.startswith('_normalized_shares_'))
    assert frame.column(column)[0].as_py() == Decimal(number+'000')
    assert receipt['normalization_contract'] == 'explicit_field_units_v2_exact_power10_with_scale_guard'


def test_menu_bounds_are_not_exported_observation_or_release_dates(registry):
    repo,root,_,_=registry
    assert run_one(root,FakeBridge()) == 'completed_task'
    table = build_tej_public_status(repo)['tables'][0]
    assert table['first_available_query_period'] == '2014-01-02'
    assert table['last_available_query_period'] == '2014-01-03'
    assert table['first_query_period'] is None
    assert table['axis_profile_basis'] == 'source_menu_bounds_not_native_history_or_release_times'
    assert not build_tej_public_status(repo)['publication_verified']


def test_old_menu_metadata_upgrade_is_local_bounded_idempotent(registry):
    repo,root,_,_=registry
    run_one(root,FakeBridge())
    with closing(connect(root)) as con, con:
        con.execute('UPDATE tables SET axis_profile_basis=NULL,first_available_query_period=NULL,last_available_query_period=NULL')
    result = refresh_axis_profiles(root)
    assert result == {'profiled_tables':1,'local_axis_evidence_requires_review':0,'provider_queries_sent':0}
    assert refresh_axis_profiles(root)['profiled_tables'] == 0
    assert build_tej_public_status(repo)['tables'][0]['first_available_query_period'] == '2014-01-02'


@pytest.mark.parametrize('mutation',['wrong_task','scope','size','date'])
def test_menu_metadata_upgrade_rejects_unverified_local_evidence(registry,mutation):
    repo,root,_,_=registry
    run_one(root,FakeBridge())
    with closing(connect(root)) as con, con:
        table = dict(con.execute('SELECT * FROM tables').fetchone())
        con.execute('UPDATE tables SET axis_profile_basis=NULL,first_available_query_period=NULL,last_available_query_period=NULL')
    path = root/table['discovery_path']
    body = json.loads(path.read_text())
    if mutation == 'wrong_task':body['task_id'] = 'wrong'
    elif mutation == 'scope':body['table'] = 'wrong'
    elif mutation == 'date':body['date_labels'] = ['not-a-date','2014/01/03']
    if mutation == 'size':path.write_bytes(b' '*(16*1024**2+1))
    else:atomic_write_json(path,body)
    assert refresh_axis_profiles(root)['local_axis_evidence_requires_review'] == 1
    table = build_tej_public_status(repo)['tables'][0]
    assert table['first_available_query_period'] is None
    assert table['axis_profile_basis'] == 'local_axis_evidence_requires_review'


def test_source_root_is_private_and_broad_roots_are_rejected(tmp_path):
    root = tmp_path/'data_tej'
    root.mkdir(mode=0o755)
    with closing(connect(root)):
        assert root.stat().st_mode & 0o777 == 0o700
    for broad in (Path('/'),Path.home(),Path.cwd()):
        with pytest.raises(ValueError,match='dedicated acquisition'):
            connect(broad)


def test_inventory_snapshot_contains_all_metadata_without_source_values(registry,tmp_path):
    import csv
    from scripts.snapshot_tej_acquisition_inventory import snapshot
    repo,root,_,_=registry
    run_one(root,FakeBridge())
    run_one(root,FakeBridge())
    output = tmp_path/'acquisition_snapshot'
    manifest = snapshot(repo,output)
    assert manifest['fields'] == 2 and manifest['tables'] == 1
    assert manifest['provider_queries_sent'] == 0 and not manifest['raw_values_exposed']
    with (output/'feature_download_status.csv').open(encoding='utf-8-sig') as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 2 and all(row['exported_non_null_cells']=='3' for row in rows)
    body = (output/'public_status.json').read_text()
    assert 'raw_path' not in body and 'parquet_path' not in body and '"cells"' not in body
    with pytest.raises(FileExistsError):snapshot(repo,output)


@pytest.mark.parametrize('local_retry',[False,True])
def test_actual_worker_short_wait_does_not_flap_queued_or_claim_active_query(registry,local_retry):
    repo,root,_,_=registry
    mark_worker_wait(root,kind='discover',seconds=2,local_retry=local_retry)
    status = build_tej_public_status(repo)
    assert status['state'] == 'running' and status['worker']['alive']
    assert status['worker']['state'] == ('waiting_local_retry' if local_retry else 'between_tasks')
    assert status['workload']['running_tasks'] == 0
    assert 'owner_pid' not in json.dumps(status)


def test_recovered_result_current_scale_is_not_original_query_unit_proof(registry):
    import pyarrow.parquet as pq
    _,root,_,_=registry
    run_one(root,FakeBridge())
    class CurrentOnly(FakeBridge):
        def execute(self,root,task):
            payload = {**full_grid_export(),'task_id':task['task_id'],
                'vendor_numeric_scale_selection':'vendor_default_dash_not_all_units_verified',
                'vendor_numeric_scale_readback_basis':'current_ui_only_original_query_setting_unverified'}
            output = root/'raw'/(task['task_id']+'-current-scale.json')
            atomic_write_json(output,payload)
            return payload,output,1.0
    assert run_one(root,CurrentOnly()) == 'completed_task'
    with closing(connect(root)) as con:
        task = dict(con.execute("SELECT * FROM tasks WHERE kind='download'").fetchone())
    receipt = json.loads((root/task['receipt_path']).read_text())
    frame = pq.read_table(root/receipt['parquet_path'])
    assert not any(n.startswith('_normalized_shares_') for n in frame.column_names)
    assert frame.column('Volume(1000S)')[0].as_py() == '10'
    assert receipt['all_source_units_verified'] is False
