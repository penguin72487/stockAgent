from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader import download_forex_frankfurter as fx
from downloader import download_dune_crypto_history as dune
from downloader.frankfurter_official_history import official_jobs
from scripts import queue_openbb_public_priority as queue
from downloader import openbb_eia_history as energy
from types import SimpleNamespace
from io import BytesIO


def test_frankfurter_singleflight_all_quotes(monkeypatch):
    calls = []
    fx._BASE_RESPONSES.clear()
    monkeypatch.setattr(fx, "_get_json", lambda url, timeout: calls.append(url) or {"rates": {}})
    with ThreadPoolExecutor(8) as pool:
        result = list(pool.map(lambda _: fx._base_rates("EUR", "1999-01-04", "2026-01-01", 30), range(20)))
    assert len(calls) == 1 and len(result) == 20
    fx._BASE_RESPONSES.clear()


def test_frankfurter_missing_head_is_repaired_once(monkeypatch, tmp_path):
    path = tmp_path / "EURUSD.parquet"
    frame = fx._normalize_rate_rows([{"date": "2000-01-03", "open": 1.1, "close": 1.1,
            "max": 1.1, "min": 1.1, "adjclose": 1.1, "Trading_Volume": None}], "1999-01-04", "2026-01-01")
    fx._write_parquet(frame, path)
    calls = []
    monkeypatch.setattr(fx, "_base_rates", lambda *args: calls.append(args) or {"rates": {"1999-01-04": {"USD": 1.1789}}})
    record = fx.SymbolRecord("EURUSD", "EURUSD", "forex", "EUR", "USD")
    fx._repair_history_head(record, "1999-01-04", "2026-01-01", path, 30)
    assert fx._read_parquet(path).height == 2
    fx._repair_history_head(record, "1999-01-04", "2026-01-01", path, 30)
    assert len(calls) == 1


def test_frankfurter_v2_uses_actual_provider_bounds_no_blending():
    jobs = official_jobs([{"key": "BBK", "pivot_currency": "DEM", "start_date": "1948-06-21", "end_date": "1998-12-30"}], selected=("BBK",))
    assert jobs[0].params["from"] == "1948-06-21"
    assert jobs[0].endpoint.endswith("/providers/bbk/rates")


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -1, 0])
def test_frankfurter_tail_rejects_invalid_rate(monkeypatch, tmp_path, value):
    monkeypatch.setattr(fx, "_base_rates", lambda *a: {"rates": {"2026-09-25": {"USD": value}}})
    result = fx._download_pair(fx.SymbolRecord("EURUSD", "EURUSD", "forex", "EUR", "USD"),
                              "2026-09-25", "2026-09-25", tmp_path, 30, False, True)
    assert result.status == "failed" and not (tmp_path / "EURUSD_features.parquet").exists()


def dune_client(monkeypatch, pages):
    client = dune.DuneClient("fake", max_retries=0, retry_base=0, poll_seconds=1, page_size=2)
    iterator = iter(pages)
    monkeypatch.setattr(client, "_request", lambda *args, **kwargs: next(iterator))
    return client


def test_dune_pages_complete(monkeypatch):
    client = dune_client(monkeypatch, [
        {"result": {"rows": [{"a": 1}, {"a": 2}], "metadata": {"total_row_count": 3}}, "next_offset": 2},
        {"result": {"rows": [{"a": 3}], "metadata": {"total_row_count": 3}}},
    ])
    assert [offset for offset, _ in client.results("id")] == [0, 2]


@pytest.mark.parametrize("page", [
    {"result": {"rows": [{"a": 1}], "metadata": {"total_row_count": 2}}},
    {"result": {"rows": [{"a": 1}]}, "next_offset": 0},
    {"result": {"rows": ["bad"]}},
])
def test_dune_cannot_silently_drop_or_repeat_pages(monkeypatch, page):
    with pytest.raises(RuntimeError):
        list(dune_client(monkeypatch, [page]).results("id"))


def test_openbb_tail_uses_latest_completed_boundary():
    original = {"start_date": "2000-01-01", "end_date": "2026-07-18", "country": "tw"}
    first = queue.tail_kwargs(original, "2026-09-27")
    second = queue.tail_kwargs(first, "2026-09-28")
    assert second["start_date"] == "2026-08-27"
    assert queue.tail_kwargs(second, "2026-09-28") is None


@pytest.mark.parametrize(("frequency", "start"), [("annual", "2024-01-01"), ("quarter", "2025-01-01"), ("month", "2025-01-01")])
def test_openbb_low_frequency_tail_retains_complete_observation_periods(frequency, start):
    kwargs = {"start_date": "2000-01-01", "end_date": "2026-07-18", "frequency": frequency}
    assert queue.tail_kwargs(kwargs, "2026-09-27")["start_date"] == start


def test_openbb_wrapped_empty_timeout_is_retryable():
    from downloader.download_openbb_archive import classify_error
    outer = RuntimeError("Error fetching data from the EIA API -> ")
    outer.__cause__ = TimeoutError()
    assert classify_error(outer) == "transient"


def test_eia_discontinued_table_empty_only_after_valid_full_workbook(monkeypatch):
    from downloader.download_openbb_archive import _fetch_eia_petroleum_status_workaround
    from openbb_us_eia.models.petroleum_status_report import EiaPetroleumStatusReportFetcher as fetcher
    from openbb_core.provider.utils.errors import EmptyDataError
    def query(kwargs):
        assert kwargs["start_date"] is None and kwargs["end_date"] is None
        return SimpleNamespace()
    async def extract(*args):
        return {"file": "verified-workbook"}
    monkeypatch.setattr(fetcher, "transform_query", query)
    monkeypatch.setattr(fetcher, "aextract_data", extract)
    monkeypatch.setattr(fetcher, "transform_data", lambda *a: [{"date": "2014-01-01", "value": 1}])
    with pytest.raises(EmptyDataError, match="Validated EIA workbook"):
        _fetch_eia_petroleum_status_workaround({"table": "ulta_low_sulfur_distillate_reclassification",
                                               "start_date": "2026-06-17", "end_date": "2026-09-27"})


def test_openbb_footer_not_false_success(tmp_path):
    path = tmp_path / "data.parquet"
    pq.write_table(pa.table({"date": ["2026-01-01"], "value": [1]}), path)
    assert queue.footer_evidence(path, 1)["valid"]
    assert not queue.footer_evidence(path, 2)["valid"]
    assert not queue.footer_evidence(tmp_path / "missing.parquet", 1)["valid"]


@pytest.mark.parametrize("tail_status", ["pending", "empty", "success"])
def test_openbb_followup_cursor_no_duplicate_and_authoritative_empty_advances(monkeypatch, tmp_path, tail_status):
    from pathlib import Path
    from downloader import download_openbb_archive as archive
    context = SimpleNamespace(commands={}, output_dir=tmp_path, end_date="2026-07-18")
    monkeypatch.setattr(queue, "ENERGY_ROUTES", {"gas_prices_daily": {}})
    manifest = archive.Manifest(tmp_path / "state.sqlite3")
    try:
        original = archive.make_task(context, energy.ENDPOINT, "gas_prices_daily",
                                     {"dataset": "gas_prices_daily", "start_date": "1900-01-01", "end_date": "2026-07-18"}, ("eia",))
        path = Path(original.output_path)
        path.parent.mkdir(parents=True)
        pq.write_table(pa.table({"date": ["2026-07-17"], "value": ["1"]}), path)
        manifest.upsert_tasks([original], plan_token="test")
        manifest.complete(archive.TaskResult(original, "success", "eia", 1, str(path), 1))
        cursors = {}
        _, tails, _ = queue.build_work(manifest, context, end="2026-09-27", cursors=cursors)
        assert len(tails) == 1
        tail = tails[0]
        manifest.upsert_tasks(tails, plan_token="test")
        if tail_status == "empty":
            manifest.complete(archive.TaskResult(tail, "empty", "eia", 0, None, 1))
        elif tail_status == "success":
            tail_path = Path(tail.output_path)
            tail_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.table({"date": ["2026-09-25"], "value": ["2"]}), tail_path)
            manifest.complete(archive.TaskResult(tail, "success", "eia", 1, str(tail_path), 1))
        assert queue.build_work(manifest, context, end="2026-09-27", cursors=cursors)[1] == []
        next_tails = queue.build_work(manifest, context, end="2026-09-28", cursors=cursors)[1]
        if tail_status == "pending":
            assert next_tails == []
        else:
            assert len(next_tails) == 1 and next_tails[0].kwargs["start_date"] == "2026-08-27"
    finally:
        manifest.close()


def test_dune_blocked_is_not_complete_or_zero_eta(tmp_path):
    p = dune.DuneAcquisitionProgress(tmp_path / "p.json", label="dune", total=2, unit="partitions", basis="test")
    p.update("a", "complete")
    p.update("b", "blocked_credits")
    p.finish(state="blocked")
    x = json.loads((tmp_path / "p.json").read_text())
    assert x["current"] == 1 and x["processed"] == 2 and x["ratio"] == 0.5
    assert x["remaining_seconds"] is None and x["estimated_complete_at_utc"] is None
    assert x["phase"] == "blocked"
    assert x["processed_per_second"] == 2 * x["items_per_second"]


def test_dune_old_attempt_rate_not_successful_throughput():
    fields = dune.acquisition_progress_fields({"status_counts": {"blocked_credits": 1, "not_started_credits": 63},
                                               "total": 64, "elapsed_seconds": 2, "items_per_second": 32,
                                               "phase": "complete", "state": "blocked"})
    assert fields["items_per_second"] == 0 and fields["processed_per_second"] == 32
    assert fields["phase"] == "blocked" and fields["ratio"] == 0


def test_eia_pagination_all_rows_precision_units_and_shared_transport():
    pages = iter([
        {"response": {"total": "2", "data": [{"period": "2025-01-01", "series": "x", "value": "1.1234567890123", "units": "USD"}]}},
        {"response": {"total": "2", "data": [{"period": "2025-01-02", "series": "x", "value": "2", "units": "USD"}]}},
    ])
    calls = []
    def opener(req, timeout):
        calls.append(req.full_url)
        return BytesIO(json.dumps(next(pages)).encode())
    obb = SimpleNamespace(user=SimpleNamespace(credentials=SimpleNamespace(eia_api_key="fake")))
    result = energy.fetch_history({"dataset": "gas_prices_daily", "start_date": "2025-01-01", "end_date": "2025-01-02"}, obb, opener=opener)
    assert result.num_rows == 2 and result["value"][0].as_py() == "1.1234567890123"
    assert "offset=1" in calls[1] and result["units"].to_pylist() == ["USD", "USD"]


def test_eia_truncation_is_failure():
    obb = SimpleNamespace(user=SimpleNamespace(credentials=SimpleNamespace(eia_api_key="fake")))
    with pytest.raises(RuntimeError, match="truncated"):
        energy.fetch_history({"dataset": "gas_prices_daily", "start_date": "2025-01-01", "end_date": "2025-01-02"}, obb,
                             opener=lambda *a, **kw: BytesIO(b'{"response":{"total":5,"data":[]}}'))


def test_eia_default_transport_resolved_after_canonical_limiter_install(monkeypatch):
    calls = []
    def wrapped(request, timeout):
        calls.append(1)
        return BytesIO(b'{"response":{"total":1,"data":[{"period":"2025-01-01","series":"X","value":"2"}]}}')
    monkeypatch.setattr(energy.urlrequest, "urlopen", wrapped)
    obb = SimpleNamespace(user=SimpleNamespace(credentials=SimpleNamespace(eia_api_key="fake")))
    assert energy.fetch_history({"dataset": "gas_prices_daily", "start_date": "2025-01-01", "end_date": "2025-01-01"}, obb).num_rows == 1
    assert calls == [1]


def test_oecd_latest_forecast_keeps_negative_growth_zero_precision_and_vintage():
    from downloader.openbb_oecd_history import fetch_gdp_forecast
    calls = []
    payload = b'STRUCTURE_ID,REF_AREA,TIME_PERIOD,OBS_VALUE,MEASURE,FREQ\nOECD.ECO.MAD:DSD_EO@DF_EO(1.5),USA,2025,-1.23456789012345,GDPV_ANNPCT,A\nOECD.ECO.MAD:DSD_EO@DF_EO(1.5),USA,2026,0,GDPV_ANNPCT,A\n'
    def opener(req, timeout):
        calls.append(req.full_url)
        return BytesIO(payload)
    table = fetch_gdp_forecast({"frequency": "annual", "units": "growth", "country": "united_states",
                                "start_date": "2024-01-01", "end_date": "2026-12-31"}, opener=opener)
    assert "DSD_EO@DF_EO,/" in calls[0] and "1.1" not in calls[0]
    assert table["source_value"].to_pylist() == ["-1.23456789012345", "0"]
    assert table["value"][0].as_py() < 0 and table["value"][1].as_py() == 0
    assert table["source_vintage_version"].to_pylist() == ["1.5", "1.5"]


@pytest.mark.parametrize("package", ["downloader", ""])
def test_eia_adapter_worker_supports_module_and_direct_script(monkeypatch, tmp_path, package):
    import sys
    from downloader import download_openbb_archive as archive

    monkeypatch.setattr(archive, "__package__", package)
    monkeypatch.setitem(sys.modules, "openbb_eia_history", energy)
    monkeypatch.setattr(energy, "fetch_history", lambda *args: pa.table({"period": ["2025-01-01"], "value": ["1.125"]}))
    task = archive.DownloadTask("energy-test", energy.ENDPOINT, "commodity", "gas",
                                {"dataset": "gas_prices_daily"}, ("eia",), str(tmp_path / "energy.parquet"))
    worker = archive.OpenBBWorker(SimpleNamespace(), archive.ProviderRuntime({"eia": 1000.0}, {"eia": 1}, 1.0),
                                  max_retries=1, base_backoff=0, max_backoff=1, metadata_only=True)
    result = worker(task)
    assert result.status == "success" and result.rows == 1


def test_monitor_receipts_not_catalog_bounds_or_secret_fields(tmp_path):
    from stockagent.live.data_monitor_dashboard import _public_economic_history_sources, _record_stats_for_row
    from datetime import UTC
    folder = tmp_path / "data_public_economic"
    (folder / "receipts/moi").mkdir(parents=True)
    receipt = {"provider": "moi", "dataset": "101S4", "status": "acquired", "rows": 100,
               "observed_at_utc": "2026-09-27T00:00:00+00:00", "first_observation": "2012-09-01", "last_observation": "2012-12-31",
               "rejected_rows": 2, "unapproved_key": "SECRET"}
    (folder / "receipts/moi/101S4.json").write_text(json.dumps(receipt))
    (folder / "download_summary.json").write_text(json.dumps({"providers": [{"datasets": [receipt]}]}))
    row = _public_economic_history_sources(tmp_path, now=datetime(2026,9,27,1,tzinfo=UTC))[0]
    assert row["status"] == "degraded" and row["record_stats"]["count"] == 100
    assert row["record_stats"]["first"] == "2012-09-01"
    assert "SECRET" not in json.dumps(row) and row["eta"]["remaining_seconds"] is None
    assert _record_stats_for_row(row, {})["count"] == 100


@pytest.mark.parametrize(("provider", "category"), [("moi", "taiwan_public"), ("bea", "macro"), ("frankfurter", "forex")])
def test_economic_monitor_category_uses_dataset_provider(provider, category):
    from stockagent.live.data_monitor_dashboard import _market_category
    assert _market_category({"id": f"economic:{provider}:test", "provider": provider,
                             "parent_id": "group:public-economic-history"}) == category
