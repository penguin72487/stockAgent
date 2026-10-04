from datetime import date
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.audit_taifex_public_inventory import build_inventory, render_markdown, write_reports


def put(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def seed(root: Path, endpoints=("/Margin",)) -> Path:
    public = root / "data_taifex_public_history"
    swagger = {"paths": {key: {"get": {"tags": ["官方分類"], "summary": "官方資料"}}
                         for key in endpoints}}
    put(public / "raw/swagger.json", swagger)
    digest = hashlib.sha256((public / "raw/swagger.json").read_bytes()).hexdigest()
    put(public / "openapi_latest.json", {"capture_date": "2026-09-26", "swagger_path": "raw/swagger.json",
        "swagger_sha256": digest, "datasets": [], "delegated_endpoints": {}})
    return public


def by_endpoint(inventory, endpoint):
    return next(row for row in inventory["datasets"] if row["endpoint"] == endpoint)


def test_discovers_unknown_official_endpoint_without_registry(tmp_path):
    seed(tmp_path, ("/NewOfficialEndpoint",))
    report = build_inventory(tmp_path)
    row = by_endpoint(report, "/NewOfficialEndpoint")
    assert row["name"] == "官方資料"
    assert row["official_category"] == ["官方分類"]
    assert row["status"] == "missing_or_invalid_receipt"
    assert row["rows"] is None
    assert not report["all_history_complete"]


def test_snapshot_never_becomes_historical_coverage(tmp_path):
    public = seed(tmp_path)
    raw, normalized = public / "raw/body.gz", public / "shards/body.parquet"
    raw.write_bytes(b"raw")
    normalized.parent.mkdir()
    normalized.write_bytes(b"test-footer-not-read")
    receipt = {"endpoint": "/Margin", "capture_date": "2026-09-26", "status": "complete", "rows": 2,
               "raw_path": "raw/body.gz", "raw_bytes": 3, "normalized_path": "shards/body.parquet",
               "normalized_bytes": normalized.stat().st_size, "captured_at_utc": "2026-09-26T00:00:00Z"}
    put(public / "receipts/openapi/2026-09-26/margin.json", receipt)
    put(public / "manifests/openapi/2026-09-03.json", {"capture_date": "2026-09-03",
        "datasets": [{"endpoint": "/Margin", "status": "complete"}]})
    row = by_endpoint(build_inventory(tmp_path), "/Margin")
    assert row["status"] == "snapshot_preserved"
    assert row["rows"] == 2
    assert row["first_capture_date"] == "2026-09-03"
    assert row["last_capture_date"] == "2026-09-26"
    assert row["first_date"] is None and row["last_date"] is None
    assert row["history_complete"] is None


def test_delegated_existing_footer_not_complete(tmp_path, monkeypatch):
    public = seed(tmp_path)
    latest = json.loads((public / "openapi_latest.json").read_text())
    latest["delegated_endpoints"] = {"/Margin": "data_tw_public/test.parquet"}
    put(public / "openapi_latest.json", latest)
    path = tmp_path / "data_tw_public/test.parquet"
    path.parent.mkdir()
    pq.write_table(pa.table({"date": [date(2024, 1, 2), date(2026, 9, 24)], "x": [1, 2]}), path)
    monkeypatch.setattr(pq, "read_table", lambda *a, **k: (_ for _ in ()).throw(AssertionError("data scan")))
    row = by_endpoint(build_inventory(tmp_path), "/Margin")
    assert row["status"] == "delegated_present_unverified"
    assert row["rows"] == 2
    assert row["first_date"] == "2024-01-02"
    assert row["last_date"] == "2026-09-24"
    assert row["history_complete"] is None


def test_missing_owner_not_declared_complete(tmp_path):
    public = seed(tmp_path)
    latest = json.loads((public / "openapi_latest.json").read_text())
    latest["delegated_endpoints"] = {"/Margin": "data_tw_public/missing.parquet"}
    put(public / "openapi_latest.json", latest)
    row = by_endpoint(build_inventory(tmp_path), "/Margin")
    assert row["status"] == "delegated_missing"
    assert row["owner_path_exists"] is False
    assert row["rows"] is None


def test_snapshot_date_is_not_mistaken_for_settlement_date(tmp_path):
    public = seed(tmp_path)
    latest = json.loads((public / "openapi_latest.json").read_text())
    latest["delegated_endpoints"] = {"/Margin": "data_tw_public/test.parquet"}
    put(public / "openapi_latest.json", latest)
    path = tmp_path / "data_tw_public/test.parquet"
    path.parent.mkdir()
    pq.write_table(pa.table({"date": ["2026-09-26"], "_as_of_date": ["2026-09-26"],
                             "TheFinalSettlementDay": ["20260916"]}), path)
    row = by_endpoint(build_inventory(tmp_path), "/Margin")
    assert row["first_date"] == "2026-09-16"
    assert "footer_explicit_source_dates" in row["date_basis"]


def test_owner_can_be_canonical_external_live_symlink(tmp_path):
    public = seed(tmp_path)
    latest = json.loads((public / "openapi_latest.json").read_text())
    latest["delegated_endpoints"] = {"/Margin": "data_tw_public/test.parquet"}
    put(public / "openapi_latest.json", latest)
    live = tmp_path / "live"
    live.mkdir()
    (tmp_path / "data_tw_public").symlink_to(live, target_is_directory=True)
    pq.write_table(pa.table({"date": [date(2026, 9, 24)]}), live / "test.parquet")
    assert by_endpoint(build_inventory(tmp_path), "/Margin")["owner_path_exists"] is True


def test_rules_always_listed_even_without_new_collector(tmp_path):
    seed(tmp_path)
    rows = [row for row in build_inventory(tmp_path)["datasets"] if row["source_kind"] == "web_rules"]
    assert len(rows) == 5
    assert all(row["status"] == "missing" for row in rows)
    assert {row["dataset"] for row in rows} >= {"his_news", "contract_adjustments", "position_limits_equity"}


def test_rules_never_reuse_overall_manifest_success_for_missing_source(tmp_path):
    public = seed(tmp_path)
    put(public / "rules/manifest.json", {"status": "complete", "sources": [
        {"source_id": "his_news", "status": "raw_preserved", "rows": 42}]})
    rows = {row["dataset"]: row for row in build_inventory(tmp_path)["datasets"] if row["source_kind"] == "web_rules"}
    assert rows["his_news"]["status"] == "raw_preserved"
    assert rows["contract_adjustments"]["status"] == "missing"
    assert rows["his_news"]["history_complete"] is None


def test_rule_manifest_new_sources_and_archive_evidence(tmp_path):
    public = seed(tmp_path)
    put(public / "rules/manifest.json", {"status": "partial", "sources": [
        {"source_id": "his_news", "status": "partial"},
        {"source_id": "margins", "name": "保證金", "kind": "margin", "url": "https://www.taifex.com.tw/cht/5/indexMarging", "status": "current"}],
        "counts": {"announcements": 600, "documents_pending": 30},
        "coverage": {"first_published_date": "2000-01-02", "last_published_date": "2026-09-24",
                     "historical_values_complete": False}})
    rows = {row["dataset"]: row for row in build_inventory(tmp_path)["datasets"] if row["source_kind"] == "web_rules"}
    assert len(rows) == 6
    assert rows["his_news"]["rows"] == 600
    assert rows["his_news"]["first_date"] == "2000-01-02"
    assert rows["his_news"]["archive_counts"]["documents_pending"] == 30
    assert rows["margins"]["history_complete"] is None


def test_rule_source_capture_clock_does_not_invent_data_or_first_capture_dates(tmp_path):
    public = seed(tmp_path)
    put(public / "rules/manifest.json", {
        "observed_at_utc": "2026-09-28T10:00:00Z",
        "sources": [{"source_id": "contract_specs", "status": "complete", "rows": 15,
                     "first_date": None, "last_date": None,
                     "observed_at_utc": "2026-09-27T07:34:54+08:00"}],
    })
    report = build_inventory(tmp_path)
    row = next(row for row in report["datasets"] if row["dataset"] == "contract_specs")
    assert row["last_capture_at_utc"] == "2026-09-26T23:34:54Z"
    assert row["last_capture_date"] == "2026-09-26"
    assert row["observed_at_utc"] == row["last_capture_at_utc"]
    assert row["first_capture_date"] is None
    assert row["first_date"] is None and row["last_date"] is None
    markdown_row = next(line for line in render_markdown(report).splitlines() if "2/sTF" in line)
    assert "未證實 → 2026-09-26" in markdown_row
    assert "未證實 → 未證實" in markdown_row
    missing = next(row for row in report["datasets"] if row["dataset"] == "position_limits_equity")
    assert missing["last_capture_date"] is None  # Global manifest time is not this source's capture.


def test_rule_failed_or_timezone_unknown_observation_is_not_a_capture(tmp_path):
    public = seed(tmp_path)
    for status, observed in (("failed", "2026-09-27T08:00:00Z"),
                             ("complete", "2026-09-27"), ("complete", "bad-clock")):
        put(public / "rules/manifest.json", {"observed_at_utc": "2026-09-28T10:00:00Z",
            "sources": [{"source_id": "contract_specs", "status": status, "observed_at_utc": observed}]})
        row = next(row for row in build_inventory(tmp_path)["datasets"] if row["dataset"] == "contract_specs")
        assert row["last_capture_date"] is None
        assert row["last_capture_at_utc"] is None
        assert row["first_date"] is None and row["last_date"] is None


def test_rule_quality_metadata_is_optional_separate_and_preserves_unknown(tmp_path):
    public = seed(tmp_path)
    put(public / "rules/manifest.json", {"quality": {
        "parsing_status_counts": {"parsed": 10, "no_rule_value": 4, "bad": -1},
        "parser_version_counts": {"1": 13, "2": 1, "unknown": None},
        "unrelated": "do-not-project",
    }})
    report = build_inventory(tmp_path)
    expected = {"parsing_status_counts": {"parsed": 10, "no_rule_value": 4, "bad": None},
                "parser_version_counts": {"1": 13, "2": 1, "unknown": None}}
    row = next(row for row in report["datasets"] if row["dataset"] == "his_news")
    assert row["archive_quality"] == expected
    assert report["summary"]["rule_archive_quality"] == expected
    assert row["rows"] is None and row["history_complete"] is None
    assert "公告解析品質" in render_markdown(report)
    assert "do-not-project" not in json.dumps(report)
    put(public / "rules/manifest.json", {})
    assert "rule_archive_quality" not in build_inventory(tmp_path)["summary"]


def test_receipt_path_escape_is_not_accepted(tmp_path):
    public = seed(tmp_path)
    outside = tmp_path / "outside.gz"
    outside.write_bytes(b"raw")
    put(public / "receipts/openapi/2026-09-26/margin.json", {
        "endpoint": "/Margin", "capture_date": "2026-09-26", "status": "source_empty", "rows": 0,
        "raw_path": "../outside.gz", "raw_bytes": 3})
    row = by_endpoint(build_inventory(tmp_path), "/Margin")
    assert row["status"] == "missing_or_invalid_receipt"
    assert row["rows"] is None


def test_corrupted_catalog_retains_recorded_endpoint_and_reports_invalid(tmp_path):
    public = seed(tmp_path)
    latest = json.loads((public / "openapi_latest.json").read_text())
    latest["swagger_sha256"] = "bad"
    latest["datasets"] = [{"endpoint": "/Recorded", "status": "complete", "rows": 8}]
    put(public / "openapi_latest.json", latest)
    report = build_inventory(tmp_path)
    assert not report["evidence"]["swagger_available_and_digest_valid"]
    assert by_endpoint(report, "/Recorded")["rows"] is None


def test_report_writers_and_no_total_rows_overlapping_sources(tmp_path):
    seed(tmp_path)
    report = build_inventory(tmp_path)
    paths = write_reports(report, tmp_path / "reports")
    assert set(paths) == {"json", "csv", "md"}
    assert all(Path(path).is_file() for path in paths.values())
    assert "rows_total" not in report["summary"]
    assert "不可直接加總" in Path(paths["md"]).read_text()


def test_all_product_csv_history_has_own_boundary_coverage_and_publication_clock(tmp_path):
    public = seed(tmp_path)
    path = public / "normalized/large_trader_futures_all.parquet"
    path.parent.mkdir()
    path.write_bytes(b"footer-not-scanned")
    put(public / "manifest.json", {"datasets": [{
        "dataset": "large_trader_futures_all", "status": "partial", "output_path": "normalized/large_trader_futures_all.parquet",
        "rows": 100, "first_date": "2004-07-01", "last_date": "2026-09-24",
        "coverage_status": "unverified_gaps", "expected_session_count": 5471,
        "observed_session_count": 5470, "missing_session_count": 1,
        "source_history_start_date": "2004-07-01", "effective_source_start_date": "2004-07-01",
        "requested_start_date": "1998-07-21", "first_publication_date": "2005-01-03",
        "first_publication_timing": "post_close_inferred_from_regular_publication_rule",
        "availability_pending_rows": 3, "availability_state": "waiting_verified_next_session",
        "source_not_supported": {"start_date": "1998-07-21", "end_date": "2004-06-30",
                                 "reason": "before_official_history_start", "verified_session_count": 1525},
        "product_universe_completeness": "not_independently_verified",
    }]})
    report = build_inventory(tmp_path)
    row = next(row for row in report["datasets"] if row["dataset"] == "large_trader_futures_all")
    assert row["status"] == "history_partial"
    assert row["acquisition_counts"]["missing_session_count"] == 1
    assert row["source_range"]["first_publication_date"] == "2005-01-03"
    assert row["source_range"]["source_not_supported"]["verified_session_count"] == 1525
    assert "official_web_query_rolling_three_year_window_not_all_history" not in row["gaps"]
    markdown = render_markdown(report)
    assert "2005-01-03" in markdown
    assert "不是只保留近三年歷史" in markdown
    assert "待下一個已驗證交易日對齊 3 列；不是下載失敗" in markdown


def test_complete_csv_pending_availability_is_not_acquisition_failure(tmp_path):
    public = seed(tmp_path)
    (public / "normalized").mkdir()
    (public / "normalized/large_trader_options_all.parquet").write_bytes(b"not-read")
    put(public / "manifest.json", {"large_trader_effective_source_start_date": "2004-07-01", "datasets": [{
        "dataset": "large_trader_options_all", "status": "complete",
        "output_path": "normalized/large_trader_options_all.parquet", "coverage_status": "verified_calendar_coverage",
        "expected_session_count": 5471, "observed_session_count": 5471, "missing_session_count": 0,
        "availability_pending_rows": 320, "availability_state": "waiting_verified_next_session",
    }, {"dataset": "institutional_options", "status": "complete"}]})
    report = build_inventory(tmp_path)
    row = next(row for row in report["datasets"] if row["dataset"] == "large_trader_options_all")
    assert row["status"] == "history_preserved_scope_limited"
    assert row["history_complete"] is None
    assert row["source_range"]["effective_source_start_date"] == "2004-07-01"
    assert row["source_range"]["first_publication_date"] is None
    assert row["acquisition_counts"]["availability_pending_rows"] == 320
    legacy = next(row for row in report["datasets"] if row["dataset"] == "institutional_options")
    assert "official_web_query_rolling_three_year_window_not_all_history" in legacy["gaps"]
