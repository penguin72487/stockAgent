from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import gzip
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from downloader.http_transport import HttpResponse, HttpStatusError, sanitized_url
from downloader.tej_api import TejAPIError, TejTrialAPI, connect, sanitize_account
from downloader.tej_api_catalog import HEADERS, collect_catalog, publish_catalog
from downloader.download_tej_api import _advance, run_queue, seed_queue, status, publish_financial_reference_fields
from downloader.tej_api_ownership import FIELD_PAIRS, remaining_rectangles, record_price_coverage, EXPECTED_UNITS

NOW = datetime(2026, 10, 4, 2, tzinfo=UTC)
KEY = "test-only-not-a-real-key"


def account(**overrides):
    return {"key": KEY, "user": {"name": "PRIVATE NAME", "userId": "PRIVATE ACCOUNT",
        "tables": {"TRAIL/TAPRCD": {"tableId": "TRAIL/TAPRCD", "startUsageDate": "2018-01-01",
        "endUsageDate": "2099-12-31", "dataStartYear": 2018, "dataEndYear": 9999,
        "allowColumns": [], "conditions": ""}}}, "startDate": "2026-10-04", "endDate": "2027-01-04",
        "todayRows": 0, "todayReqCount": 0, "rowsDayLimit": 50_000, "reqDayLimit": 500,
        "lastStatTime": 1791064800000, "multiConn": False, **overrides}


class Transport:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def request_bytes(self, url, **kwargs):
        self.calls.append((url, kwargs))
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        if isinstance(result, HttpResponse):
            return result
        return HttpResponse(200, json.dumps(result).encode(), {}, 1)


def client(tmp_path, *responses):
    api = TejTrialAPI(tmp_path / "api", KEY, transport=Transport(account(), *responses), now=lambda: NOW)
    api.authenticate()
    return api


def native(rows, cursor=None, columns=None):
    return {"datatable": {"columns": columns or [
        {"name": "coid", "type": "char(7)"}, {"name": "mdate", "type": "datetime"},
        {"name": "price", "type": "decimal(20,8)"}], "data": rows}, "meta": {"next_cursor_id": cursor}}


def test_account_identity_and_key_never_persist(tmp_path):
    api = client(tmp_path)
    text = (api.root / "account_status.json").read_text()
    assert KEY not in text and "PRIVATE NAME" not in text and "PRIVATE ACCOUNT" not in text
    assert len(json.loads(text)["tables"]) == 1
    assert api.transport.calls[0][1]["headers"] == HEADERS


def test_trial_caps_cannot_be_expanded_by_paid_response():
    clean = sanitize_account(account(rowsDayLimit=3_000_000, reqDayLimit=2000), now=NOW)
    assert clean["quota"]["rows_per_day"] == 50_000
    assert clean["quota"]["calls_per_day"] == 500


@pytest.mark.parametrize("value", [True, -1, "500", None, 1.5])
def test_invalid_limits_fail_closed(value):
    with pytest.raises(TejAPIError):
        sanitize_account(account(reqDayLimit=value), now=NOW)


def test_concurrent_reservations_do_not_overdraw_rows(tmp_path):
    api = client(tmp_path)
    def claim(_):
        try:
            return api._claim(10_000)[1]
        except TejAPIError:
            return 0
    with ThreadPoolExecutor(max_workers=8) as pool:
        claims = list(pool.map(claim, range(8)))
    assert sum(claims) == 50_000
    assert status(api)["quota"]["rows_remaining"] == 0


def test_failed_or_lost_response_keeps_row_reservation(tmp_path):
    api = client(tmp_path, TimeoutError())
    with pytest.raises(TejAPIError, match="unknown_transport_outcome"):
        api.page("TRAIL/TAPRCD", {}, wanted_rows=10_000)
    assert status(api)["quota"]["rows_remaining"] == 40_000


def test_server_usage_plus_only_later_local_calls(tmp_path):
    api = client(tmp_path)
    clean = sanitize_account(account(todayRows=40_000, todayReqCount=200), now=NOW)
    with closing(connect(api.root)) as con, con:
        con.execute("INSERT OR REPLACE INTO meta VALUES('account',?)", (json.dumps(clean),))
    api.now = lambda: NOW + timedelta(seconds=1)
    api._claim(5000)
    assert status(api)["quota"]["rows_used"] == 45_000
    assert status(api)["quota"]["calls_used"] == 201


def test_unconfirmed_midnight_does_not_release_reserved_quota(tmp_path):
    api = client(tmp_path)
    for _ in range(5):
        api._claim(10_000)
    api.now = lambda: NOW + timedelta(hours=18)
    assert status(api)["quota"]["rows_remaining"] == 0
    api.now = lambda: NOW + timedelta(hours=25)
    assert status(api)["quota"]["rows_remaining"] == 50_000


def test_gzip_and_decimal_precision_are_preserved(tmp_path):
    body = b'{"datatable":{"columns":[{"name":"price","type":"decimal(20,8)"}],"data":[[123456789012.12345678]]},"meta":{"next_cursor_id":null}}'
    api = client(tmp_path, HttpResponse(200, gzip.compress(body), {"Content-Encoding": "gzip"}, 1))
    receipt = api.page("TRAIL/TAPRCD", {}, wanted_rows=1)
    assert (api.root / receipt["raw_path"]).read_bytes() == body
    assert pq.read_table(api.root / receipt["parquet_path"]).column(0)[0].as_py() == Decimal("123456789012.12345678")
    assert receipt["complete_query"] and not receipt["complete_history"]


@pytest.mark.parametrize("sentinel", ["", "-", "N/A", "not_a_number"])
def test_source_numeric_sentinels_are_not_zero_or_null(tmp_path, sentinel):
    api = client(tmp_path, native([["2330", "2025-01-02T00:00:00Z", sentinel]]))
    receipt = api.page("TRAIL/TAPRCD", {}, wanted_rows=1)
    frame = pq.read_table(api.root / receipt["parquet_path"])
    assert frame["price"][0].as_py() == sentinel
    assert receipt["physical_string_fallback_fields"] == ["price"]


def test_decimal_wider_than_declared_scale_is_not_rounded(tmp_path):
    api = client(tmp_path, native([["2330", "2025-01-02T00:00:00Z", "1.123456789"]]))
    receipt = api.page("TRAIL/TAPRCD", {}, wanted_rows=1)
    assert pq.read_table(api.root / receipt["parquet_path"])["price"][0].as_py() == "1.123456789"


def test_unlicensed_non_trial_table_is_rejected_without_call(tmp_path):
    api = client(tmp_path)
    with pytest.raises(TejAPIError, match="non_trial_table"):
        api.page("TWN/APRCD", {})
    assert len(api.transport.calls) == 1


def test_credentials_cannot_enter_receipt_params(tmp_path):
    api = client(tmp_path)
    with pytest.raises(TejAPIError, match="credential_must_not"):
        api.page("TRAIL/TAPRCD", {"api_key": "OTHER"})
    assert len(api.transport.calls) == 1


def test_same_cursor_can_advance_native_page_content(tmp_path):
    api = client(tmp_path, native([["2330", "2025-01-02T00:00:00Z", 1]], "same"),
                 native([["2330", "2025-01-03T00:00:00Z", 2]], "same"))
    with closing(connect(api.root)) as con, con:
        con.execute("INSERT INTO tasks(id,table_id,params_json,priority,state) VALUES('task','TRAIL/TAPRCD','{}',0,'running')")
    api.page("TRAIL/TAPRCD", {}, wanted_rows=1, task_id='task')
    receipt = api.page("TRAIL/TAPRCD", {"opts.cursor_id": "same"}, wanted_rows=1, task_id='task')
    assert receipt['rows'] == 1 and receipt['next_cursor'] == 'same'
    assert status(api)['stored_rows'] == 2


def test_repeated_native_page_is_blocked_even_if_cursor_rotates(tmp_path):
    rows = [["2330", "2025-01-02T00:00:00Z", 1]]
    api = client(tmp_path, native(rows, 'first'), native(rows, 'different'))
    with closing(connect(api.root)) as con, con:
        con.execute("INSERT INTO tasks(id,table_id,params_json,priority,state) VALUES('task','TRAIL/TAPRCD','{}',0,'running')")
    api.page('TRAIL/TAPRCD', {}, wanted_rows=1, task_id='task')
    with pytest.raises(TejAPIError, match='repeated_page_content'):
        api.page('TRAIL/TAPRCD', {'opts.cursor_id': 'first'}, wanted_rows=1, task_id='task')
    assert status(api)['stored_rows'] == 1


def test_cursor_request_cannot_guess_smaller_server_page_size(tmp_path):
    api = client(tmp_path)
    for _ in range(5):
        api._claim(9900)
    before = len(api.transport.calls)
    with pytest.raises(TejAPIError, match='waiting_page_budget'):
        api.page('TRAIL/TAPRCD', {'opts.cursor_id': 'opaque'}, wanted_rows=10000)
    assert len(api.transport.calls) == before


def test_page_bound_is_enforced(tmp_path):
    api = client(tmp_path, native([["2330", "2025-01-02T00:00:00Z", 1]] * 2))
    with pytest.raises(TejAPIError, match="provider_page_bound_exceeded"):
        api.page("TRAIL/TAPRCD", {}, wanted_rows=1)


def test_api_key_in_path_and_echo_are_redacted():
    url = "https://api.tej.com.tw/api/apiKeyInfo/" + KEY
    err = HttpStatusError(403, url, ("credential=" + KEY).encode())
    assert KEY not in str(err) and KEY.encode() not in err.body
    assert "[REDACTED]" in sanitized_url(url)


def test_empty_page_stays_observed_empty_not_full_history(tmp_path):
    api = client(tmp_path, native([]))
    receipt = api.page("TRAIL/TAPRCD", {}, wanted_rows=1)
    assert receipt["rows"] == 0 and receipt["parquet_path"] is None
    assert receipt["complete_query"] and not receipt["complete_history"]


def test_successful_page_is_adopted_once(tmp_path):
    api = client(tmp_path, native([["2330", "2025-01-02T00:00:00Z", 1]], "next"))
    with closing(connect(api.root)) as con, con:
        con.execute("INSERT INTO tasks(id,table_id,params_json,priority,state) VALUES('task','TRAIL/TAPRCD','{}',0,'running')")
    receipt = api.page("TRAIL/TAPRCD", {}, wanted_rows=1, task_id="task")
    with closing(connect(api.root)) as con, con:
        task = dict(con.execute("SELECT * FROM tasks WHERE id='task'").fetchone())
        _advance(con, task, receipt, api); _advance(con, task, receipt, api)
        assert con.execute("SELECT query_rows,actual_rows FROM tasks WHERE id='task'").fetchone()[:] == (1, 1)


def test_full_budget_has_no_hourly_api_calls(tmp_path):
    api = client(tmp_path)
    for _ in range(5):
        api._claim(10_000)
    before = len(api.transport.calls)
    assert run_queue(api)["reason"] == "waiting_quota"
    assert len(api.transport.calls) == before


def test_retry_after_is_durable_and_suppresses_other_requests(tmp_path):
    api = client(tmp_path, HttpResponse(429, b'{}', {'Retry-After': '60'}, 1))
    with pytest.raises(TejAPIError, match='waiting_quota'):
        api.page('TRAIL/TAPRCD', {}, wanted_rows=10000)
    before = len(api.transport.calls)
    with pytest.raises(TejAPIError, match='provider_cooldown'):
        api.metadata('TRAIL/TAPRCD')
    result = run_queue(api)
    assert result['reason'] == 'provider_cooldown'
    assert result['next_attempt_utc'] == (NOW + timedelta(seconds=60)).isoformat()
    assert len(api.transport.calls) == before


def test_authentication_failure_is_visible_without_private_exception(tmp_path):
    api = client(tmp_path, HttpResponse(403, b'PRIVATE BODY', {}, 1))
    result = run_queue(api)
    assert result['reason'] == 'access_denied'
    assert 'PRIVATE BODY' not in (api.root / 'worker_status.json').read_text()


def test_stored_page_unblocks_only_its_local_materialization_failure(tmp_path, monkeypatch):
    api = client(tmp_path, native([['2330', '2025-01-02T00:00:00Z', 1]], None))
    with closing(connect(api.root)) as con, con:
        con.execute("INSERT INTO tasks(id,table_id,params_json,priority,state) VALUES('task','TRAIL/TAPRCD','{}',0,'running')")
    api.page('TRAIL/TAPRCD', {}, wanted_rows=1, task_id='task')
    with closing(connect(api.root)) as con, con:
        con.execute("UPDATE tasks SET state='blocked',error_code='local_materialization_failed' WHERE id='task'")
    monkeypatch.setattr(api, 'authenticate', lambda: {})
    run_queue(api)
    with closing(connect(api.root)) as con:
        assert con.execute("SELECT state,actual_rows FROM tasks WHERE id='task'").fetchone()[:] == ('complete', 1)
    assert status(api)['stored_rows'] == 1


def test_financial_dictionary_inventory_is_not_a_history_claim(tmp_path):
    import pyarrow as pa
    api = client(tmp_path)
    output = tmp_path / 'report'
    assert publish_financial_reference_fields(api, output)['account_dictionary_state'] == 'waiting_complete_native_query'
    dictionary = [{'id': 'TAIM1A', 'code': '1000', 'cname': '現金', 'ename': 'Cash', 'unit': 'T'}]
    pq.write_table(pa.Table.from_pylist(dictionary), api.root / 'dictionary.parquet')
    with closing(connect(api.root)) as con, con:
        con.execute("INSERT INTO tasks(id,table_id,params_json,priority,state) VALUES('dict','TRAIL/TAIACC','{}',0,'complete')")
        con.execute("INSERT INTO pages VALUES('ref','dict','TRAIL/TAIACC','raw','dictionary.parquet','receipt',1,'sha')")
    result = publish_financial_reference_fields(api, output)
    assert result['accounting_concepts'] == 1 and result['financial_series_definitions'] == 2
    assert not result['all_concept_histories_complete']
    assert 'source_dictionary_definition_not_value_history' in (output / 'financial_account_fields.csv').read_text()


def test_conflicting_native_revisions_never_reenable_exact_delegation(tmp_path):
    from downloader.tej_api_ownership import API_TABLE
    api = client(tmp_path)
    units = EXPECTED_UNITS
    columns = [{'name': name, 'unit': units.get(name, '-')} for name in ['coid', 'mdate', *(p[0] for p in FIELD_PAIRS)]]
    row = ['2330', '2025-01-02T00:00:00Z', *([1] * len(FIELD_PAIRS))]
    receipt = {'table_id': API_TABLE, 'request_id': 'old', 'physical_string_fallback_fields': []}
    record_price_coverage(api.root, receipt, [row], columns)
    changed = list(row); changed[2] = 2
    record_price_coverage(api.root, {**receipt, 'request_id': 'new'}, [changed], columns)
    record_price_coverage(api.root, receipt, [row], columns)
    with closing(connect(api.root)) as con:
        assert con.execute('SELECT field_mask,row_sha256 FROM native_coverage').fetchone()[:] == (0, None)


def test_ambiguous_or_changed_native_units_are_never_delegated(tmp_path):
    from downloader.tej_api_ownership import API_TABLE
    api = client(tmp_path)
    columns = [{'name': name, 'unit': EXPECTED_UNITS.get(name, '-')} for name in ['coid', 'mdate', *(p[0] for p in FIELD_PAIRS)]]
    next(c for c in columns if c['name'] == 'volume')['unit'] = 'S'
    row = ['2330', '2025-01-02T00:00:00Z', *([1] * len(FIELD_PAIRS))]
    record_price_coverage(api.root, {'table_id': API_TABLE, 'request_id': 'row'}, [row], columns)
    with closing(connect(api.root)) as con:
        mask = con.execute('SELECT field_mask FROM native_coverage').fetchone()[0]
    bits = {code: 1 << index for index, (code, _) in enumerate(FIELD_PAIRS)}
    assert not mask & bits['volume'] and not mask & bits['mv']
    assert mask & bits['open_d']


def test_local_materialization_recovers_without_another_provider_call(tmp_path, monkeypatch):
    api = client(tmp_path, native([["2330", "2025-01-02T00:00:00Z", 1]], "next"))
    original = __import__('downloader.tej_api', fromlist=['atomic_write_parquet']).atomic_write_parquet
    monkeypatch.setattr('downloader.tej_api.atomic_write_parquet', lambda *a, **k: (_ for _ in ()).throw(OSError('disk unavailable')))
    with pytest.raises(OSError):
        api.page("TRAIL/TAPRCD", {}, wanted_rows=1)
    before = len(api.transport.calls)
    monkeypatch.setattr('downloader.tej_api.atomic_write_parquet', original)
    result = api.recover_saved_pages()
    assert result['saved_pages_recovered'] == 1 and result['provider_requests_sent'] == 0
    assert len(api.transport.calls) == before
    assert status(api)['stored_rows'] == 1
    assert api.recover_saved_pages()['saved_pages_recovered'] == 0


def test_exact_pagination_ceiling_without_cursor_is_not_sold_as_complete(tmp_path):
    api = client(tmp_path)
    with closing(connect(api.root)) as con, con:
        params = {'mdate.gte': '2025-01-01', 'mdate.lte': '2025-01-31'}
        con.execute("INSERT INTO tasks(id,table_id,params_json,priority,state,query_rows) VALUES('cap','TRAIL/TAPRCD',?,0,'running',40000)", (json.dumps(params),))
        task = dict(con.execute("SELECT * FROM tasks WHERE id='cap'").fetchone())
        _advance(con, task, {'request_id': 'page5', 'rows': 10000, 'next_cursor': None}, api)
        assert con.execute("SELECT state FROM tasks WHERE id='cap'").fetchone()[0] == 'superseded_disjoint_partition'
        assert con.execute("SELECT COUNT(*) FROM tasks WHERE state='pending'").fetchone()[0] == 2


def test_queue_recovery_adopts_the_active_response_exactly_once(tmp_path, monkeypatch):
    api = client(tmp_path, native([["2330", "2025-01-02T00:00:00Z", 1]], None))
    with closing(connect(api.root)) as con, con:
        con.execute("INSERT INTO tasks(id,table_id,params_json,priority,state) VALUES('task','TRAIL/TAPRCD','{}',0,'running')")
    api.page("TRAIL/TAPRCD", {}, wanted_rows=1, task_id='task')
    monkeypatch.setattr(api, 'authenticate', lambda: {})
    run_queue(api)
    with closing(connect(api.root)) as con:
        assert con.execute("SELECT state,actual_rows FROM tasks WHERE id='task'").fetchone()[:] == ('complete', 1)
    assert status(api)['stored_rows'] == 1


def test_quarterly_plan_does_not_make_daily_empty_queries(tmp_path):
    api = client(tmp_path)
    table = {"tableId": "TRAIL/TAIM1A", "access_state": "verified_nonempty",
             "actual_first": "2025-03-01T00:00:00Z", "actual_last": "2025-12-01T00:00:00Z",
             "rowCount": 2_000_000, "frequency": "quarterly_cumulative"}
    report = seed_queue(api, {"tables": [table]})
    assert report["tasks_inserted"] == 4 * 92
    with closing(connect(api.root)) as con:
        params = [json.loads(row[0]) for row in con.execute("SELECT params_json FROM tasks")]
    assert {p['mdate.gte'] for p in params} == {"2025-03-01", "2025-06-01", "2025-09-01", "2025-12-01"}
    assert seed_queue(api, {"tables": [table]})["tasks_inserted"] == 0


def test_public_catalog_adds_account_only_tables_without_sample_values(tmp_path):
    catalog = {"tables": [{"tableId": "TRAIL/TAPRCD", "groupName": "交易"}]}
    def detail(code):
        return {"tableId": code, "columns": [{"name": "coid", "type": "char(7)", "unit": "-"}],
                "samples": [{"secret_licensed_value": "MUST NOT PERSIST"}], "cName": code,
                "rowCount": 5, "minYear": 2018, "dataRange": "過去一年"}
    transport = Transport(catalog, detail("TRAIL/TAPRCD"), detail("TRAIL/TAAPRRENT"))
    result = collect_catalog(transport, now=NOW, entitled_table_ids=("TRAIL/TAPRCD", "TRAIL/TAAPRRENT"))
    publish_catalog(result, tmp_path / "inventory")
    assert result["table_count"] == 2 and result["public_catalog_table_count"] == 1
    assert "MUST NOT PERSIST" not in (tmp_path / "inventory/catalog.json").read_text()


def request():
    return {"smart_id": "TEJ Equity", "table": "TSE/OTC Unadjusted_Price(Daily)",
            "frequency": "daily", "source_key_mode": 2, "fields": [p[1] for p in FIELD_PAIRS[:3]],
            "catalog_fields": [p[1] for p in FIELD_PAIRS[:3]],
            "company_labels": ["2330=>TSMC", "1101=>Cement"],
            "date_labels": ["2024/12/31", "2025/01/02", "2026/01/02"]}


def cells(req):
    return {(field, company, period) for field in req['fields'] for company in req['company_labels'] for period in req['date_labels']}


def test_exact_scope_removes_only_observed_symbol_period_field():
    req = request()
    parts = remaining_rectangles(req, {("2330", "2025-01-02"): 1})
    actual = set()
    for part in parts:
        assert not actual & cells(part)
        actual |= cells(part)
    assert actual == cells(req) - {(FIELD_PAIRS[0][1], "2330=>TSMC", "2025/01/02")}
    assert any("2024/12/31" in p['date_labels'] for p in parts)
    assert any("2026/01/02" in p['date_labels'] for p in parts)


def test_no_proof_or_other_grid_never_delegates():
    req = request()
    assert remaining_rectangles(req, {}) is None
    for field, value in (("frequency", "monthly"), ("source_key_mode", 3), ("table", "Adjusted Price")):
        assert remaining_rectangles({**req, field: value}, {("2330", "2025-01-02"): 7}) is None


def test_durable_wizard_repartition_preserves_original_and_never_fakes_completion(tmp_path):
    from downloader.artifact_io import atomic_write_json
    from downloader.tej_history import connect as wizard_connect, compact_request, SOURCE_SCOPE_CONTRACT
    from downloader.tej_api_ownership import repartition_pending, record_price_coverage, CONTRACT
    from downloader.tej_planning import CONTRACT as PLAN
    from scripts.build_tej_smart_wizard_inventory import table_identity
    wizard = tmp_path / 'wizard'
    names = [pair[1] for pair in FIELD_PAIRS]
    smart, table = 'TEJ Equity', 'TSE/OTC Unadjusted_Price(Daily)'
    identifier, sha = table_identity(smart, table, names)
    req = {**request(), 'catalog_fields': names,
           'field_partition': {'contract': PLAN, 'field_range': [0, 3], 'catalog_fields': len(names)}}
    definition = {'table_id': identifier, 'smart_id': smart, 'name': table, 'schema_sha256': sha, 'fields_json': json.dumps(names)}
    with closing(wizard_connect(wizard)) as con, con:
        con.execute('INSERT INTO tables(table_id,smart_id,name,category,phase,frequency,query_type,schema_sha256,fields_json,state,work_grid_rows) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                    (identifier, smart, table, 'Equity', 'P1', 'daily', 'Equity', sha, json.dumps(names), 'backfilling', 6))
        original = json.dumps(compact_request(req, definition))
        con.execute("INSERT INTO tasks(task_id,table_id,kind,phase,priority,request_json,state,expected_rows,scope_contract,work_expected_rows) VALUES('original',?,'download','P1',100,?,'pending',6,?,6)",
                    (identifier, original, SOURCE_SCOPE_CONTRACT))
    columns = [{'name': 'coid'}, {'name': 'mdate'}, *({'name': code, 'unit': {'open_d': 'NTD', 'close_d': 'NTD', 'volume': 'T', 'amount': 'NTD,T', 'outstanding': 'T'}.get(code, '-')} for code, _ in FIELD_PAIRS)]
    values = ['2330', '2025-01-02T00:00:00Z', *([None] * len(FIELD_PAIRS))]
    values[2] = 1
    record_price_coverage(wizard / 'api_trial_v1', {'table_id': 'TRAIL/TAPRCD', 'request_id': 'verified'}, [values], columns)
    atomic_write_json(wizard / 'api_source_allocation.json', {'contract': CONTRACT, 'enabled': True})
    with closing(wizard_connect(wizard)) as con, con:
        row = con.execute("SELECT * FROM tasks WHERE task_id='original'").fetchone()
        assert repartition_pending(con, wizard, row)
        saved = con.execute("SELECT * FROM tasks WHERE task_id='original'").fetchone()
        assert saved['request_json'] == original and saved['state'] == 'superseded_exact_api_scope_v1'
        assert saved['receipt_path'] is None and saved['actual_rows'] is None
        assert con.execute("SELECT COUNT(*) FROM tasks WHERE state='complete'").fetchone()[0] == 0
        assert con.execute('SELECT work_grid_rows FROM tables').fetchone()[0] == 11
        assert not repartition_pending(con, wizard, saved)
    audit = json.loads((wizard / 'source_allocation/original.json').read_text())
    assert audit['excluded_native_cells'] == 1
    assert audit['original_work_rows'] == 6 and audit['remaining_work_rows'] == 11
    assert not audit['fake_wizard_receipts']


@pytest.mark.parametrize("mask", range(8))
def test_exact_subtraction_matches_independent_field_mask(mask):
    req = request()
    parts = remaining_rectangles(req, {("2330", "2025-01-02"): mask})
    expected = cells(req) - {(pair[1], "2330=>TSMC", "2025/01/02") for i, pair in enumerate(FIELD_PAIRS[:3]) if mask & (1 << i)}
    assert set.union(set(), *(cells(part) for part in (parts if parts is not None else [req]))) == expected
