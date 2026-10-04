from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

import polars as pl
import pytest

from stockagent.live import data_monitor_dashboard as dashboard
from stockagent.live.data_monitor_inventory import PHYSICAL_FAMILIES, build_record_inventory
from test_public_dashboards import protocol_server  # noqa: F401 - shared localhost fixture


NOW = datetime(2026, 9, 27, 10, tzinfo=UTC)


def manifest(**overrides):
    result = {
        "schema_version": 1, "dataset": "taifex_public_rule_history", "status": "partial",
        "observed_at_utc": NOW.isoformat(),
        "counts": {"index_years_complete": 30, "index_years_total": 30, "announcements": 300,
                   "documents_complete": 80, "documents_pending": 20, "documents_failed": 0,
                   "attachments_complete": 7, "normalized_table_rows": 1000, "temporal_mentions": 120,
                   "external_links_not_crawled": 0, "parse_gaps": 0},
        "coverage": {"first_published_date": "1997-09-01", "last_published_date": "2026-09-24",
                     "all_documents_fetched": False, "point_in_time_verified": False,
                     "historical_values_complete": False},
    }
    for key, value in overrides.items():
        if isinstance(value, dict):
            result[key].update(value)
        else:
            result[key] = value
    return result


def write(tmp_path, payload, filename="manifest.json"):
    path = tmp_path / "data_taifex_public_history/rules" / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def project(root, **kwargs):
    return dashboard._taifex_rule_history_sources(root, now=NOW, **kwargs)[0]


def test_missing_archive_stays_visible_but_not_complete(tmp_path):
    row = project(tmp_path)
    assert row["id"] == "taifex:rule-history"
    assert row["title"] == "期交所公告與契約規則歷史"
    assert row["status"] == "waiting"
    assert row["coverage"] is None
    assert row["record_stats"]["count"] is None
    assert row["eta"]["remaining_seconds"] is None


def test_document_progress_does_not_add_failed_twice(tmp_path):
    write(tmp_path, manifest(counts={"documents_failed": 3}))
    row = project(tmp_path)
    assert row["status"] == "degraded"
    assert row["coverage"]["current"] == 80
    assert row["coverage"]["total"] == 100
    assert row["rows"] is None
    assert row["record_stats"]["count"] == 300
    assert "其中失敗 3" in row["detail"]


def test_incomplete_index_does_not_show_document_100_percent(tmp_path):
    write(tmp_path, manifest(counts={"index_years_complete": 3, "documents_pending": 0}))
    row = project(tmp_path)
    assert row["coverage"]["unit"] == "年份"
    assert row["coverage"]["ratio"] == 0.1
    assert row["rule_archive"]["index_complete"] is False
    assert row["status"] == "waiting"


def test_parse_gaps_are_not_hidden_by_raw_download_success(tmp_path):
    write(tmp_path, manifest(status="current", counts={"documents_pending": 0, "parse_gaps": 5},
                             coverage={"all_documents_fetched": True}))
    row = project(tmp_path)
    assert row["status"] == "degraded"
    assert "解析缺口 5" in row["status_label"]
    assert row["eta"]["state"] != "complete"
    enriched = dashboard._enrich_and_sort_rows([row], now=NOW,
        refresh_services={"taifex_rules": {"timer_active": True}})[0]
    assert enriched["operation_state"] == "unable"
    assert enriched["acquisition_progress"]["ratio"] is None


def test_live_service_and_fresh_running_receipt_required(tmp_path):
    write(tmp_path, manifest(status="running"))
    idle = project(tmp_path, service_state={"active": False})
    live = project(tmp_path, service_state={"active": True})
    assert idle["status"] == "waiting"
    assert live["status"] == "updating"
    assert live["eta"]["state"] == "running_unmeasured"
    assert live["eta"]["remaining_seconds"] is None
    write(tmp_path, manifest(status="running", observed_at_utc=(NOW - timedelta(hours=1)).isoformat()))
    assert project(tmp_path, service_state={"active": True})["status"] == "waiting"


def test_active_download_gaps_remain_visible_without_hiding_progress(tmp_path):
    write(tmp_path, manifest(status="running", counts={"parse_gaps": 4, "external_links_not_crawled": 733}))
    row = project(tmp_path, service_state={"active": True})
    assert row["status"] == "updating"
    assert row["rule_archive"]["health_degraded"] is True
    assert row["rule_archive"]["counts"]["parse_gaps"] == 4
    enriched = dashboard._enrich_and_sort_rows([row], now=NOW,
        refresh_services={"taifex_rules": {"active": True, "timer_active": True}})[0]
    assert enriched["operation_state"] == "catching_up"
    assert enriched["execution_state"] == "running"
    assert enriched["acquisition_progress"]["ratio"] == 0.8


@pytest.mark.parametrize("last_published_date", ["2026-09-23", "2026-09-26"])
def test_rule_backfill_does_not_infer_next_publication_day(tmp_path, last_published_date):
    write(tmp_path, manifest(status="running", coverage={"last_published_date": last_published_date}))
    row = project(tmp_path, service_state={"active": True})
    enriched = dashboard._enrich_and_sort_rows([row], now=NOW,
        refresh_services={"taifex_rules": {"active": True, "timer_active": True}})[0]
    progress = enriched["acquisition_progress"]
    assert enriched["operation_state"] == "catching_up"
    assert progress["data_through"] == last_published_date
    assert progress["preparing_for_date"] is None
    assert progress["label"] == "已發現文件回補中；不以公告日期推定下一發布日"
    assert progress["ratio"] == 0.8


def test_rule_clock_exception_does_not_change_market_data_progress():
    progress = dashboard._acquisition_progress(
        {"id": "tw-public:market-data", "data_through": "2026-09-23",
         "coverage": {"current": 80, "total": 100, "ratio": 0.8}},
        operation="catching_up", execution="running", publication={},
    )
    assert progress["preparing_for_date"] == "2026-09-24"
    assert progress["label"] == "首筆已到；取得中並準備下一資料日 2026-09-24"


def test_only_parse_work_left_is_degraded_even_while_service_active(tmp_path):
    write(tmp_path, manifest(status="running", counts={"documents_pending": 0, "parse_gaps": 4},
                             coverage={"all_documents_fetched": True}))
    row = project(tmp_path, service_state={"active": True})
    assert row["status"] == "degraded"
    assert row["rule_archive"]["acquisition_running"] is False
    enriched = dashboard._enrich_and_sort_rows([row], now=NOW,
        refresh_services={"taifex_rules": {"active": True, "timer_active": True}})[0]
    assert enriched["operation_state"] == "unable"


def test_archive_complete_is_not_numeric_or_pit_complete(tmp_path):
    write(tmp_path, manifest(status="current", counts={"documents_pending": 0},
                             coverage={"all_documents_fetched": True}))
    row = project(tmp_path)
    assert row["status"] == "current"
    assert row["rule_archive"]["history_values_verified"] is False
    assert row["rule_archive"]["point_in_time_verified"] is False
    assert "數值規則" in row["status_label"]
    assert row["publishable"] is False


def test_private_manifest_fields_are_not_exposed(tmp_path):
    payload = manifest()
    payload.update(errors=[{"error": "/private/path/token=secret"}],
                   outputs={"documents": {"path": "/private/path/secret"}})
    payload["counts"]["secret"] = "secret"
    payload["coverage"]["api_token"] = "secret"
    write(tmp_path, payload)
    encoded = json.dumps(project(tmp_path))
    assert "secret" not in encoded
    assert "/private" not in encoded


def test_corrupt_counts_fail_closed_without_zero_fill(tmp_path):
    payload = manifest()
    del payload["counts"]["documents_pending"]
    write(tmp_path, payload)
    row = project(tmp_path)
    assert row["status"] == "degraded"
    assert row["rule_archive"]["counts"]["documents_pending"] is None
    assert row["coverage"] is None


def test_latest_progress_metadata_wins(tmp_path):
    write(tmp_path, manifest(observed_at_utc=(NOW - timedelta(minutes=1)).isoformat()))
    write(tmp_path, manifest(status="running", counts={"documents_complete": 90, "documents_pending": 10}),
          "progress.json")
    assert project(tmp_path)["coverage"]["current"] == 90


def test_future_receipt_is_not_a_fresh_complete_claim(tmp_path):
    write(tmp_path, manifest(status="current", observed_at_utc=(NOW + timedelta(days=1)).isoformat(),
                             counts={"documents_pending": 0}, coverage={"all_documents_fetched": True}))
    assert project(tmp_path)["status"] == "degraded"


def test_canonical_service_profile_uses_dedicated_rules_timer():
    profile = dashboard._profile_for_row({"id": "taifex:rule-history", "parent_id": "group:taifex-public-history"})
    assert profile["service_keys"] == ("taifex_rules",)
    assert profile["requires_timer_active"] is True
    assert "休市日" in profile["schedule_label"]


def write_main_history(root, *, status="complete", missing=0, pending=0):
    storage = root / "data_taifex_public_history"
    (storage / "normalized").mkdir(parents=True, exist_ok=True)
    (storage / "normalized/large_trader_futures_all.parquet").write_bytes(b"not-scanned")
    payload = {"dataset": "taifex_public_history", "status": status, "completed_at_utc": NOW.isoformat(),
               "availability_pending_rows": pending, "datasets": [{
                   "dataset": "large_trader_futures_all", "status": "partial" if missing else "complete",
                   "output_path": "normalized/large_trader_futures_all.parquet",
                   "rows": 3952182, "first_date": "2004-07-01", "last_date": "2026-09-24",
                   "expected_session_count": 5471, "observed_session_count": 5471 - missing,
                   "missing_session_count": missing,
                   "coverage_status": "unverified_gaps" if missing else "verified_calendar_coverage",
               }]}
    (storage / "manifest.json").write_text(json.dumps(payload))
    return storage


def test_complete_rule_child_cannot_hide_partial_main_history(tmp_path):
    write_main_history(tmp_path, status="partial", missing=2)
    write(tmp_path, manifest(status="current", counts={"documents_pending": 0},
                             coverage={"all_documents_fetched": True}))
    rows = dashboard._taifex_public_history_sources(tmp_path, now=NOW) + [project(tmp_path)]
    enriched = dashboard._enrich_and_sort_rows(rows, now=NOW, refresh_services={
        "taifex_rules": {"timer_active": True}, "taifex_public_history": {"timer_active": True}})
    group = {"id": "group:taifex-public-history", "status": "current", "warnings": []}
    dashboard._rollup_storage_groups([group], enriched)
    assert group["status"] != "current"
    assert group["active_child_endpoint_count"] == 2
    main = next(row for row in enriched if row["id"] == "taifex:public-history")
    assert main["operation_state"] == "catching_up"
    assert main["history_acquisition"]["all_registered_datasets_acquired"] is False


def test_main_history_next_session_pending_is_not_download_failure(tmp_path):
    write_main_history(tmp_path, pending=1836)
    row = dashboard._taifex_public_history_sources(tmp_path, now=NOW)[0]
    enriched = dashboard._enrich_and_sort_rows([row], now=NOW, refresh_services={
        "taifex_public_history": {"timer_active": True}})[0]
    assert enriched["operation_state"] == "complete"
    assert "不是下載失敗" in " ".join(enriched["warnings"])
    assert enriched["history_acquisition"]["all_registered_datasets_acquired"] is True
    assert enriched["acquisition_progress"]["preparing_for_date"] is None
    assert row["rows"] is None
    assert row["publishable"] is False


def test_mixed_history_group_has_no_shared_release_date_but_keeps_timer(tmp_path):
    write_main_history(tmp_path, pending=1836)
    write(tmp_path, manifest(status="running", counts={"parse_gaps": 4}))
    next_run = "2026-09-28T09:30:00Z"
    services = {"taifex_rules": {"active": True, "timer_active": True},
                "taifex_public_history": {"active": False, "timer_active": True,
                                          "next_run_at_utc": next_run}}
    children = dashboard._taifex_public_history_sources(tmp_path, now=NOW) + [
        project(tmp_path, service_state=services["taifex_rules"])]
    children = dashboard._enrich_and_sort_rows(children, now=NOW, refresh_services=services)
    group = {"id": "group:taifex-public-history", "scope": "storage_group", "status": "current",
             "warnings": [], "data_through": "2026-09-24", "latest_at_utc": NOW.isoformat(),
             "_publication_hint": {"expected_release_at_utc": "2026-09-25T09:00:00Z"}}
    dashboard._rollup_storage_groups([group], children)
    result = dashboard._enrich_and_sort_rows([group, *children], now=NOW, refresh_services=services)
    by_id = {row["id"]: row for row in result}
    aggregate = by_id["group:taifex-public-history"]
    assert aggregate["operation_state"] == "catching_up"
    assert aggregate["acquisition_progress"]["preparing_for_date"] is None
    assert aggregate["acquisition_progress"]["label"] == "依個別來源發布時程增量；歷史缺口持續回補"
    assert aggregate["publication"]["schedule_kind"] == "aggregate_source_schedules"
    assert aggregate["publication"]["expected_release_at_utc"] is None
    assert aggregate["publication"]["exact_time_declared"] is False
    assert aggregate["automation"]["next_run_at_utc"] == next_run
    assert aggregate["publication"]["next_acquisition_at_utc"] == next_run
    assert by_id["taifex:public-history"]["operation_state"] == "complete"
    assert by_id["taifex:public-history"]["acquisition_progress"]["preparing_for_date"] is None
    assert by_id["taifex:rule-history"]["operation_state"] == "catching_up"
    assert by_id["taifex:rule-history"]["acquisition_progress"]["preparing_for_date"] is None


@pytest.mark.parametrize("group_id", ["group:tw-index-futures", "group:tw-index-options-daily",
                                     "group:tw-index-derivatives-ticks"])
def test_other_taifex_parent_groups_do_not_invent_next_data_dates(group_id):
    next_run = "2026-09-28T09:30:00Z"
    row = {"id": group_id, "scope": "storage_group", "status": "current", "data_through": "2026-09-24",
           "latest_at_utc": NOW.isoformat(), "freshness": {"state": "current"},
           "eta": dashboard._complete_eta(),
           "_publication_hint": {"expected_release_at_utc": "2026-09-25T09:00:00Z"}}
    keys = dashboard._profile_for_row(row)["service_keys"]
    services = {key: {"active": False, "timer_active": True, "next_run_at_utc": next_run} for key in keys}
    enriched = dashboard._enrich_and_sort_rows([row], now=NOW, refresh_services=services)[0]
    assert enriched["operation_state"] == "complete"
    assert enriched["acquisition_progress"]["preparing_for_date"] is None
    assert enriched["publication"]["expected_release_at_utc"] is None
    assert enriched["publication"]["schedule_kind"] == "aggregate_source_schedules"
    assert enriched["automation"]["next_run_at_utc"] == next_run


def test_all_futures_group_uses_actual_quality_dates_not_query_cutoff(tmp_path):
    storage = tmp_path / "data_tw_index_futures"
    storage.mkdir()
    payload = {"end_date": "2026-09-25", "all_futures_daily": {"quality": {
        "first_date": "1998-07-21", "last_date": "2026-09-24", "rows": 2005925}}}
    (storage / "manifest.json").write_text(json.dumps(payload))
    row = dashboard._generic_group(tmp_path, {"dataset": "tw-index-futures", "source": storage.name}, now=NOW)
    assert row["data_through"] == "2026-09-24"
    assert "1998-07-21 → 2026-09-24" in " ".join(row["warnings"])
    assert row["latest_at_utc"] == "2026-09-24T23:59:59Z"
    # The exception is source-specific, not a global rewrite of date extraction.
    assert dashboard._extract_data_through([payload]) == "2026-09-25"


@pytest.mark.parametrize("quality", [{}, {"first_date": "2026-09-25", "last_date": "2026-09-24"}])
def test_all_futures_missing_or_inverted_quality_dates_stay_unknown(tmp_path, quality):
    storage = tmp_path / "data_tw_index_futures"
    storage.mkdir()
    (storage / "manifest.json").write_text(json.dumps({
        "end_date": "2026-09-25", "all_futures_daily": {"quality": quality}}))
    row = dashboard._generic_group(tmp_path, {"dataset": "tw-index-futures", "source": storage.name}, now=NOW)
    assert row["data_through"] is None
    assert row["latest_at_utc"] is None
    assert row["status"] == "degraded"


def test_main_history_running_phase_is_not_global_coverage(tmp_path):
    storage = write_main_history(tmp_path, status="partial", missing=2)
    (storage / "progress.json").write_text(json.dumps({
        "state": "running", "phase": "large_trader_futures_all", "completed": 30, "total": 39,
        "updated_at_utc": NOW.isoformat(),
    }))
    row = dashboard._taifex_public_history_sources(tmp_path, now=NOW, service_state={"active": True})[0]
    assert row["status"] == "updating"
    assert row["coverage"]["ratio"] == 30 / 39
    assert row["coverage"]["unit"] == "本階段範圍"
    assert "非全域" in row["coverage"]["label"]
    assert row["eta"]["remaining_seconds"] is None
    assert dashboard._profile_for_row(row)["service_keys"] == ("taifex_public_history",)


def test_main_manifest_partial_remains_noncomplete_with_complete_children(tmp_path):
    write_main_history(tmp_path, status="partial")
    row = dashboard._taifex_public_history_sources(tmp_path, now=NOW)[0]
    assert row["status"] == "waiting"
    assert row["coverage"] is None
    assert row["history_acquisition"]["all_registered_datasets_acquired"] is False


def test_main_history_missing_output_is_not_complete(tmp_path):
    storage = write_main_history(tmp_path)
    (storage / "normalized/large_trader_futures_all.parquet").unlink()
    row = dashboard._taifex_public_history_sources(tmp_path, now=NOW)[0]
    assert row["status"] == "degraded"
    assert row["history_acquisition"]["datasets_complete"] == 0


def test_main_history_inconsistent_session_counts_cannot_claim_complete(tmp_path):
    storage = write_main_history(tmp_path)
    payload = json.loads((storage / "manifest.json").read_text())
    payload["datasets"][0]["observed_session_count"] = 4000
    (storage / "manifest.json").write_text(json.dumps(payload))
    row = dashboard._taifex_public_history_sources(tmp_path, now=NOW)[0]
    assert row["status"] == "waiting"
    assert row["history_acquisition"]["all_registered_datasets_acquired"] is False


def test_rules_inventory_tables_do_not_inflate_market_rows(tmp_path):
    for family, count in (("taifex-public:normalized", 2), ("taifex-rules:announcements", 3),
                          ("taifex-rules:documents", 4), ("taifex-rules:document-links", 8)):
        pattern = PHYSICAL_FAMILIES[family][2]
        path = tmp_path / pattern.replace("*", "test")
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"published_date": ["2026-09-24"] * count}).write_parquet(path)
    result = build_record_inventory(tmp_path, refresh=True)["datasets"]
    assert result["group:taifex-public-history"]["count"] == 2
    assert result["physical:taifex-rules:announcements"]["count"] == 3
    assert result["physical:taifex-rules:documents"]["count"] == 4
    assert all(not details[3] for family, details in PHYSICAL_FAMILIES.items() if family.startswith("taifex-rules:"))


def test_registry_alias_and_logical_work_are_not_two_active_downloads(tmp_path):
    source = json.loads((Path(__file__).resolve().parents[1] / "configs/free_public_data_sources.json").read_text())
    source["sources"] = [item for item in source["sources"] if item["id"] == "taifex_rule_history"]
    config = tmp_path / "configs/free_public_data_sources.json"
    config.parent.mkdir()
    config.write_text(json.dumps(source))
    write(tmp_path, manifest())
    rows = dashboard._free_public_registry_sources(tmp_path, now=NOW) + [project(tmp_path)]
    projected = dashboard._enrich_and_sort_rows(rows, now=NOW,
        refresh_services={"taifex_rules": {"timer_active": True}})
    assert sum(row["in_active_scope"] for row in projected) == 1
    assert sum(row["operation_state"] == "reference" for row in projected) == 1
    assert dashboard._market_category(project(tmp_path)) == "taiwan_derivatives"


def test_rule_archive_renders_on_existing_provider_desktop_and_mobile(tmp_path, protocol_server):
    playwright = pytest.importorskip("playwright.sync_api")
    from playwright.sync_api import Error as PlaywrightError
    from stockagent.live.data_monitor_providers import project_provider_detail
    from urllib.parse import parse_qs, urlparse

    write(tmp_path, manifest(status="running", counts={"parse_gaps": 4}))
    row = project(tmp_path, service_state={"active": True})
    row = dashboard._enrich_and_sort_rows([row], now=NOW, refresh_services={
        "taifex_rules": {"active": True, "timer_active": True}})[0]
    row.update(market_category="taiwan_derivatives", market_category_label="臺灣期貨／選擇權")
    snapshot = {"generated_at_utc": NOW.isoformat(), "sources": [row]}

    def provider_route(route):
        query = parse_qs(urlparse(route.request.url).query)
        result = project_provider_detail(snapshot, "TAIFEX", search=query.get("q", [""])[0])
        route.fulfill(status=200, content_type="application/json", body=json.dumps(result))

    try:
        with playwright.sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            try:
                for width in (1280, 390):
                    page = browser.new_page(viewport={"width": width, "height": 900})
                    errors = []
                    page.on("pageerror", lambda error: errors.append(str(error)))
                    page.route("**/data-monitor/api/provider?*", provider_route)
                    page.goto(f"http://127.0.0.1:{protocol_server.port}/data-monitor/providers/TAIFEX/",
                              wait_until="domcontentloaded")
                    table = page.locator("#provider-source-rows")
                    table.get_by_text("期交所公告與契約規則歷史", exact=True).wait_for(timeout=8000)
                    for text in ("公告 300 筆", "文件 80/100 份", "解析缺口 4", "PIT", "1997-09-01"):
                        playwright.expect(table).to_contain_text(text, timeout=8000)
                    assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
                    page.locator("#provider-source-search").fill("無符合來源")
                    page.get_by_text("0/0 項符合結果").wait_for(timeout=8000)
                    page.locator("#provider-source-search").fill("")
                    table.get_by_text("期交所公告與契約規則歷史", exact=True).wait_for(timeout=8000)
                    assert not errors
                    page.close()
            finally:
                browser.close()
    except PlaywrightError as exc:
        if "Executable doesn't exist" in str(exc):
            pytest.skip("Playwright Chromium is not installed")
        raise
