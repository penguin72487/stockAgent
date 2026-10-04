import io
import json
import zipfile

import pyarrow as pa
import pytest

from downloader import download_public_economic_history as worker
from downloader import public_economic_sources as source
from downloader.download_keyed_public_catalogs import CatalogError, RequestBudget, SafeCatalogTransport


def census_body(value="123.4567890123456789"):
    return json.dumps([["time", "cell_value", "data_type_code", "category_code"],
                       ["2025-01", value, "SM", "44X72"]]).encode()


def test_census_preserves_precision_suppression_and_dimensions():
    table, meta = source.parse_economic_json(source.core_jobs()[0], census_body())
    assert table["cell_value"][0].as_py() == "123.4567890123456789"
    assert meta["date_column"] == "time"
    assert "assume" in meta["unit_contract"]
    assert source.parse_economic_json(source.core_jobs()[0], census_body("(S)"))[0]["cell_value"][0].as_py() == "(S)"


def test_modern_census_eits_geography_and_case_are_not_legacy_defaults():
    jobs = {job.dataset: job for job in source.core_jobs() if job.provider == "census"}
    for program in ("m3", "advm3", "qtax", "bfs", "mhs2"):
        assert jobs[program].params["for"] == "us:*"
    assert jobs["qtax_state"].params["for"] == "state:*"
    assert jobs["qtax"].params["get"] == source.CENSUS_FIELDS.upper()
    body = b'[["time","CELL_VALUE","DATA_TYPE_CODE","CATEGORY_CODE","state"],["2025-Q1","(S)","TAX","1","06"]]'
    table, meta = source.parse_economic_json(jobs["qtax_state"], body)
    assert table["CELL_VALUE"][0].as_py() == "(S)"
    assert table["state"][0].as_py() == "06"
    assert meta["value_column"] == "CELL_VALUE"


def test_bounded_numeric_quality_audit_does_not_count_suppression_as_zero(tmp_path):
    import pyarrow.parquet as pq
    from downloader.artifact_io import sha256_file
    from scripts.audit_public_provider_repairs import economic_value_audit
    path = tmp_path / "values.parquet"
    pq.write_table(pa.table({"cell_value": ["1,234.5", "-0.25", "(S)", None]}), path)
    receipt = {"rows": 4, "value_column": "cell_value", "files": [
        {"kind": "parquet", "path": "values.parquet", "sha256": sha256_file(path)},
    ]}
    result = economic_value_audit(tmp_path, receipt)
    assert result["numeric_value_rows"] == 2
    assert result["null_value_rows"] == 1
    assert result["suppressed_or_other_text_rows"] == 1
    assert result["decoded_rows"] == 4 and result["sha256_verified"]


@pytest.mark.parametrize("body", [b"{}", b"[]", b"<html>denied</html>", b'[["time","time"],["2024","2025"]]'])
def test_error_or_broken_schema_is_not_data(body):
    with pytest.raises(CatalogError):
        source.parse_economic_json(source.core_jobs()[0], body)


def test_bea_inactive_credential_never_becomes_empty_success():
    job = next(j for j in source.core_jobs() if j.provider == "bea")
    body = b'{"BEAAPI":{"Results":{"Error":{"APIErrorCode":"4"}}}}'
    with pytest.raises(CatalogError, match="credential_activation_required"):
        source.parse_economic_json(job, body)


@pytest.mark.parametrize("status,bypassed", [("credential_rejected", True),
    ("credential_activation_required", True), ("http_401", True), ("http_429", False),
    ("provider_throttled", False), ("response_byte_limit", False)])
def test_explicit_credential_recheck_never_bypasses_quota(monkeypatch, tmp_path, status, bypassed):
    from datetime import timedelta
    job = source.core_jobs()[0]
    attempt = tmp_path / "attempts" / job.provider / f"{job.dataset}.json"
    attempt.parent.mkdir(parents=True)
    attempt.write_text(json.dumps({"status": status, "retry_at_utc": (worker.now() + timedelta(hours=1)).isoformat()}))
    calls = []

    class Transport:
        def __init__(self, *args, **kwargs):
            pass

        def fetch(self, *args):
            calls.append(1)
            return census_body()

    kwargs = dict(max_bytes=1024**2, min_free_bytes=0, transport_factory=Transport)
    credentials = {"CENSUS_API_KEY": "replacement-secret"}
    assert worker.acquire(job, tmp_path, credentials, RequestBudget(1), **kwargs)["status"] == status
    assert not calls
    row = worker.acquire(job, tmp_path, credentials, RequestBudget(1), recheck_credentials=True, **kwargs)
    assert row["status"] == ("acquired" if bypassed else status)
    assert bool(calls) == bypassed


def test_incremental_replaces_complete_query_slice_and_preserves_old_history():
    old = pa.table({"time": ["2014-01", "2025-01", "2025-02"], "cell_value": ["1", "2", "3"]})
    fresh = pa.table({"time": ["2025-01"], "cell_value": ["9"]})
    result = worker.merge_revision(old, fresh, "time", "2025-01-01")
    assert result.to_pydict() == {"time": ["2014-01", "2025-01"], "cell_value": ["1", "9"]}


def test_incremental_refuses_out_of_query_rows():
    table = pa.table({"time": ["2014-01"]})
    with pytest.raises(CatalogError):
        worker.merge_revision(table, table, "time", "2025-01-01")


def test_moi_catalog_not_synthetic_quarters():
    jobs = source.moi_season_jobs(b'<option value="115S2">x</option><option value="101S3">x</option><option value="csv">x</option>')
    assert [j.dataset for j in jobs] == ["101S3", "115S2"]
    assert all(j.params["season"] == j.dataset for j in jobs)


def make_zip(name="a_lvr_land_a.csv", text=None):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(name, text or "鄉鎮市區,交易年月日,總價元\nThe villages and towns,transaction date,total price\n中正區,1010801,123456.789\n")
    return buffer.getvalue()


def test_moi_preserves_raw_roc_unit_precision_and_unknown_publication():
    tables, meta = source.parse_moi_zip(make_zip())
    table = tables["a_lvr_land_a"]
    assert table.num_rows == 1
    assert table["交易年月日"][0].as_py() == "1010801"
    assert table["總價元"][0].as_py() == "123456.789"
    assert meta["first_observation"] == "2012-08-01"
    assert "not publication" in meta["date_semantics"]


def test_moi_child_dictionary_and_bad_rows_not_transactions():
    text = "編號,面積\nThe serial number,area\nID,10.5\nBAD,11,broken\n"
    tables, meta = source.parse_moi_zip(make_zip("a_lvr_land_a_land.csv", text))
    assert tables["a_lvr_land_a_land"].num_rows == 1
    assert meta["observation_rows"] == 1
    assert meta["rejected_rows"] == 1 and not meta["normalization_complete"]
    assert tables["rejected_rows"]["raw_fields_json"][0].as_py() == '["BAD", "11", "broken"]'


def test_moi_official_empty_batch_is_not_request_failure():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        archive.writestr("manifest.csv", "name,rows\n")
        archive.writestr("build.ttt", "empty batch")
    tables, meta = source.parse_moi_zip(buf.getvalue())
    assert tables == {} and meta["source_empty"]
    assert meta["observation_rows"] == 0


def test_moi_swallowed_csv_rows_quarantined_without_losing_valid_rows():
    import csv
    original_limit = csv.field_size_limit()
    text = '編號,備註\nThe serial number,notes\nVALID,ok\nBAD,"' + ('x\n' * 70000) + '"\n'
    tables, meta = source.parse_moi_zip(make_zip("a_lvr_land_a_land.csv", text))
    assert tables["a_lvr_land_a_land"].num_rows == 1
    assert meta["rejected_rows"] == 1 and not meta["normalization_complete"]
    assert tables["rejected_rows"]["reason"][0].as_py() == "oversize_csv_field_requires_review"
    assert csv.field_size_limit() == original_limit


def test_moi_parse_failure_keeps_raw_and_retry_uses_no_network(monkeypatch, tmp_path):
    from datetime import timedelta
    calls = []

    class Transport:
        def __init__(self, spec, budget, **kwargs):
            self.budget = budget

        def fetch(self, *args):
            calls.append(1)
            self.budget.used += 1
            return make_zip()

    job = source.moi_season_jobs(b'<option value="101S3">x</option>')[0]
    original_parse = worker.parse_moi_zip
    monkeypatch.setattr(worker, "parse_moi_zip", lambda _: (_ for _ in ()).throw(ValueError("bad CSV")))
    failed = worker.acquire(job, tmp_path, {}, RequestBudget(1), max_bytes=1024**2,
                            min_free_bytes=0, transport_factory=Transport)
    assert failed["status"] == "local_processing_failed"
    assert not worker.receipt_path(tmp_path, job).exists()
    assert (tmp_path / "raw_receipts/moi/101S3.json").is_file()
    later = worker.now() + timedelta(minutes=6)
    monkeypatch.setattr(worker, "now", lambda: later)
    monkeypatch.setattr(worker, "parse_moi_zip", original_parse)
    repaired = worker.acquire(job, tmp_path, {}, RequestBudget(0), max_bytes=1024**2,
                              min_free_bytes=0, transport_factory=Transport)
    assert repaired["status"] == "acquired" and repaired["rows"] == 1
    assert repaired["requests_this_run"] == 0 and len(calls) == 1


@pytest.mark.parametrize("name", ["../a_lvr_land_a.csv", "/a_lvr_land_a.csv", "foo\\a_lvr_land_a.csv"])
def test_zip_slip_refused(name):
    with pytest.raises(CatalogError):
        source.parse_moi_zip(make_zip(name))


def test_zip_expansion_bound():
    with pytest.raises(CatalogError, match="normalized_byte_limit"):
        source.parse_moi_zip(make_zip(), max_uncompressed=10)


def test_invalid_roc_date_is_not_guessed():
    assert source.moi_trade_date("1150231") is None
    assert source.moi_trade_date("0000000") is None


def test_moi_future_source_date_kept_raw_but_not_normalized():
    from datetime import date
    tables, meta = source.parse_moi_zip(make_zip(text="交易年月日,總價元\n1200101,100\n"), observed_date=date(2026, 9, 27))
    table = tables["a_lvr_land_a"]
    assert table["交易年月日"][0].as_py() == "1200101"
    assert table["observation_date"][0].as_py() is None
    assert table["transaction_date_issue"][0].as_py() == "future_transaction_date"
    assert meta["last_observation"] is None and not meta["normalization_complete"]


def test_frankfurter_numeric_json_does_not_round_source_decimal():
    job = source.EconomicJob("frankfurter", "BBK", "https://api.frankfurter.dev/v2/providers/bbk/rates",
                             {"base": "DEM", "from": "1948-01-01", "to": "1998-12-31"})
    body = b'[{"date":"1948-06-21","base":"DEM","quote":"USD","rate":1.1234567890123456789}]'
    table, _ = source.parse_economic_json(job, body)
    assert table["rate"][0].as_py() == "1.1234567890123456789"


def fake_transport(body, calls):
    class Fake:
        retry_after_seconds = 0
        def __init__(self, spec, budget, **kwargs):
            self.budget = budget
        def fetch(self, url, headers):
            self.budget.claim("test")
            calls.append(url)
            return body
    return Fake


def test_resume_zero_calls_and_corruption_repaired(tmp_path):
    job = source.core_jobs()[0]
    calls = []
    kwargs = dict(max_bytes=1024**2, min_free_bytes=0, transport_factory=fake_transport(census_body(), calls))
    first = worker.acquire(job, tmp_path, {"CENSUS_API_KEY": "test-key"}, RequestBudget(1), **kwargs)
    assert first["status"] == "acquired"
    assert not first["historical_point_in_time"] and not first["history_complete"]
    second = worker.acquire(job, tmp_path, {"CENSUS_API_KEY": "test-key"}, RequestBudget(0), **kwargs)
    assert second["status"] == "current_cached" and len(calls) == 1
    path = tmp_path / first["files"][-1]["path"]
    path.write_bytes(b"broken")
    assert not worker.verified_receipt(tmp_path, job)
    assert worker.acquire(job, tmp_path, {"CENSUS_API_KEY": "test-key"}, RequestBudget(1), **kwargs)["status"] == "acquired"


def test_bea_raw_does_not_leak_key(tmp_path):
    job = next(j for j in source.core_jobs() if j.provider == "bea")
    body = json.dumps({"BEAAPI": {"Request": {"UserID": "secret-for-test"}, "Results": {"Data": [
        {"TimePeriod": "2025Q1", "DataValue": "100.0123", "UNIT_MULT": "6", "METRIC_NAME": "Current Dollars"}]}}}).encode()
    receipt = worker.acquire(job, tmp_path, {"BEA_API_KEY": "secret-for-test"}, RequestBudget(1),
                             max_bytes=1024**2, min_free_bytes=0, transport_factory=fake_transport(body, []))
    assert receipt["status"] == "acquired"
    for path in tmp_path.rglob("*.json"):
        assert "secret-for-test" not in path.read_text()


def test_safe_census_invalid_key_redirect_not_followed():
    from downloader.download_keyed_public_catalogs import SPEC_BY_PROVIDER
    client = SafeCatalogTransport(SPEC_BY_PROVIDER["census"], RequestBudget(1), maximum_bytes=1024, secrets=("secret",), limiter=object())
    with pytest.raises(CatalogError, match="credential_rejected"):
        client._accept_redirect("https://api.census.gov/data/timeseries/eits/mrts?key=secret",
                                "https://api.census.gov/data/invalid_key.html")
