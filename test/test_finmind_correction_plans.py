"""No-network tests of notice identity, source ownership and bounded scopes."""
from copy import deepcopy
from datetime import UTC, datetime, timedelta
import hashlib

import pytest

from downloader.finmind_correction_plans import (
    LONG_INSTITUTIONAL, WIDE_INSTITUTIONAL, build_repair_plan,
    load_scope_registry, resolve_owner,
)


NOW = datetime(2026, 9, 27, 4, 0, tzinfo=UTC)
EMPTY = {"schema_version": 1, "scopes": {}}


def entry(text="TaiwanStockPrice 2026-09-02 資料已更正", *,
          datasets=("TaiwanStockPrice",), notice_date="2026-09-25", **extra):
    return {"entry_id": hashlib.sha256((notice_date + text).encode()).hexdigest(),
            "revision_family_id": "family", "notice_date": notice_date,
            "text": text, "leading_text": text, "datasets": list(datasets),
            "nested_items": [], "is_correction": True,
            "known_unavailable": False, "source_url": "https://finmind.github.io/WhatIsNew/",
            **extra}


def curated_entries(registry=None):
    registry = registry or load_scope_registry()
    return [entry(value["evidence_text"], datasets=value["datasets"],
                  notice_date=value["notice_date"], entry_id=key,
                  revision_family_id=value["revision_family_id"])
            for key, value in registry["scopes"].items()]


def plan(items, previous=None, **kwargs):
    return build_repair_plan(items, previous, NOW, scopes=EMPTY, **kwargs)


def test_baseline_next_taipei_midnight_and_stable_daily_identity():
    e = entry()
    first = plan([e])
    assert first["requests"][0]["required_after_utc"] == "2026-09-25T16:00:00+00:00"
    assert first["baseline_boundary_is_point_in_time_proof"] is False
    later = build_repair_plan([e], first["state"], NOW + timedelta(days=5), scopes=EMPTY)
    assert first["plan_id"] == later["plan_id"]
    assert first["requests"] == later["requests"]
    assert first["state"] == later["state"]


def test_baseline_today_does_not_wait_for_future_midnight():
    p = plan([entry(notice_date="2026-09-27")])
    assert p["requests"][0]["required_after_utc"] == NOW.isoformat()
    assert p["requests"][0]["watermark_basis"] == "baseline_same_day_detection_not_pit"


def test_new_and_edited_old_heading_require_post_detection_fetch():
    first = plan([entry()])
    e = entry("TaiwanStockPrice 2020-01-02 資料已更正", notice_date="2020-01-03")
    second = plan([entry(), e], first["state"])
    repair = next(x for x in second["requests"] if x["entry_id"] == e["entry_id"])
    assert repair["required_after_utc"] == NOW.isoformat()
    assert repair["watermark_basis"] == "first_detection_of_new_or_edited_notice"


def test_full_history_is_finite_and_frozen_at_notice_date():
    e = entry("TaiwanStockPriceAdj 全歷史更正", datasets=("TaiwanStockPriceAdj",))
    first = plan([e])
    q = first["requests"][0]
    assert (q["start_date"], q["end_date"]) == ("1994-10-01", "2026-09-25")
    later = build_repair_plan([e], first["state"], NOW + timedelta(days=40), scopes=EMPTY)
    assert later["requests"] == first["requests"]


@pytest.mark.parametrize("text", [
    "TaiwanStockPrice 2020-01-01 起資料已更正",
    "TaiwanStockPrice 2020-01-01 以前資料已更正",
    "TaiwanStockPrice 2020-01-01 已更正,部分日期亦有影響",
    "TaiwanStockPrice 2020-01-01 已更正,例如2020-01-02",
    "TaiwanStockPrice 2020-01-01 不受影響",
    "TaiwanStockPrice 2020-01-01、2020-01-03 已更正",
    "TaiwanStockPrice 2020-01-03 ~ 2020-01-01 已更正",
    "TaiwanStockPrice 2020-02-30 已更正",
    "TaiwanStockPrice 2020年資料已更正",
    "TaiwanStockPrice 2020-01-01 全歷史更正",
])
def test_ambiguous_dates_are_not_guessed(text):
    p = plan([entry(text)])
    assert p["requests"] == []
    assert p["notices"][0]["status"] == "needs_review"


def test_simple_exact_interval_is_automatically_scheduled():
    p = plan([entry("TaiwanStockPrice 2020-01-01 ~ 2020-01-03 已更正")])
    assert [(x["start_date"], x["end_date"]) for x in p["requests"]] == [("2020-01-01", "2020-01-03")]


def test_old_unreviewed_and_unsupported_not_marked_repaired():
    old = plan([entry(notice_date="2020-01-03")])
    assert old["notices"][0]["status"] == "not_scheduled"
    tick = plan([entry("TaiwanFuturesTick 2020-01-01 已更正", datasets=("TaiwanFuturesTick",))])
    assert not tick["requests"]
    assert tick["notices"][0]["issues"][0]["reason"] == "dataset_has_no_enabled_acquisition_owner"


def test_nested_or_unavailable_scope_requires_review():
    for change in ({"nested_items": [{"text": "unavailable details"}]}, {"known_unavailable": True}):
        p = plan([entry(**change)])
        assert not p["requests"]
        assert p["notices"][0]["status"] == "needs_review"


def test_owner_priority_avoids_complement_sponsor_duplicates():
    assert resolve_owner("TaiwanStockPrice") == "sponsor"
    assert resolve_owner("TaiwanStockBalanceSheet") == "sponsor"
    assert resolve_owner("TaiwanFuturesFinalSettlementPrice") == "complement"
    assert resolve_owner("TaiwanStockStatisticsOfOrderBookAndTrade") == "free"
    assert resolve_owner("TaiwanVariousIndicators5Seconds") == "free"
    assert resolve_owner("TaiwanStockTradingDate") == "free"
    assert resolve_owner(WIDE_INSTITUTIONAL) == "sponsor"
    assert resolve_owner("TaiwanFuturesTick") is None


def test_long_and_wide_share_one_raw_request():
    e = entry("三大法人 2020-01-01 已更正", datasets=(LONG_INSTITUTIONAL, WIDE_INSTITUTIONAL))
    p = plan([e])
    assert len(p["requests"]) == 1
    assert p["requests"][0]["dataset"] == LONG_INSTITUTIONAL
    assert p["requests"][0]["derived_datasets"] == [WIDE_INSTITUTIONAL]


def test_curated_full_hash_change_requires_new_review_even_if_family_changes():
    registry = load_scope_registry()
    e = curated_entries(registry)[1]
    first = build_repair_plan([e], now=NOW, scopes=registry)
    changed = {**e, "entry_id": "e" * 64, "revision_family_id": "changed-family",
               "text": "TaiwanStockStatisticsOfOrderBookAndTrade 2020-01-01 已更正",
               "leading_text": "TaiwanStockStatisticsOfOrderBookAndTrade 2020-01-01 已更正"}
    p = build_repair_plan([changed], first["state"], NOW, scopes=registry)
    assert not p["requests"]
    assert p["notices"][0]["reason"] == "edited_curated_notice_requires_new_scope_review"
    assert p["state"]["entries"][changed["entry_id"]]["required_after_utc"] == NOW.isoformat()


def test_same_curated_id_with_changed_text_is_rejected():
    e = curated_entries()[0]
    e["text"] += " modified"
    p = build_repair_plan([e], now=NOW)
    assert not p["requests"]
    assert p["notices"][0]["reason"] == "curated_identity_or_text_mismatch"


def test_curated_scopes_exact_and_consumer_identity_unique():
    p = build_repair_plan(curated_entries(), now=NOW)
    assert len(p["requests"]) == 54
    identities = {(x["owner"], x["correction_id"], x["dataset"]) for x in p["requests"]}
    assert len(identities) == len(p["requests"])
    assert all(len(x["correction_id"]) == 64 and len(x["reason"]) <= 4096 for x in p["requests"])
    book = [x for x in p["requests"] if x["dataset"] == "TaiwanStockStatisticsOfOrderBookAndTrade"]
    assert {x["start_date"] for x in book} == {"2011-01-21", "2023-08-04"}
    assert all(x["end_date"] == x["start_date"] for x in book)


def test_etf_excludes_unrecoverable_baseline_date():
    p = build_repair_plan(curated_entries(), now=NOW)
    etf = [x for x in p["requests"] if x["dataset"].startswith("TaiwanStockActiveETF")]
    assert len(etf) == 4
    assert {x["start_date"] for x in etf} == {"2026-02-11", "2026-02-23"}
    assert all(x["data_ids"] is None and not x["allow_empty"] for x in etf)
    issues = [issue for notice in p["notices"] for issue in notice["issues"]]
    unavailable = next(x for x in issues if x.get("start_date") == "2026-02-10")
    assert unavailable["status"] == "known_unavailable"
    assert len(unavailable["data_ids"]) == 4


def test_typhoon_empty_is_authoritative_daily_only_and_calendar_refreshes():
    p = build_repair_plan(curated_entries(), now=NOW)
    empty = [x for x in p["requests"] if x["allow_empty"]]
    assert len(empty) == 29
    assert {x["dataset"] for x in empty} == {
        "TaiwanFuturesDaily", "TaiwanOptionDaily", "TaiwanFuturesDealerTradingVolumeDaily",
        "TaiwanOptionDealerTradingVolumeDaily", "TaiwanFuturesInstitutionalInvestorsAfterHours"}
    daily = [x for x in empty if x["dataset"] == "TaiwanFuturesDaily"]
    assert {x["start_date"] for x in daily} == {
        "2023-08-03", "2024-07-24", "2024-07-25", "2024-10-02", "2024-10-03", "2024-10-31", "2026-07-10"}
    calendar = next(x for x in p["requests"] if x["dataset"] == "TaiwanStockTradingDate")
    assert calendar["owner"] == "free" and calendar["scope_kind"] == "snapshot"
    assert calendar["start_date"] == calendar["end_date"] == "2026-07-10"
    assert not calendar["allow_empty"]
    assert not any("Tick" in x["dataset"] or "KBar" in x["dataset"] for x in p["requests"])


def test_financial_and_daily_week_month_scopes_do_not_expand_examples():
    p = build_repair_plan(curated_entries(), now=NOW)
    assert not any(x["dataset"] in {"TaiwanStockFinancialStatements", "TaiwanStockCashFlowsStatement"} for x in p["requests"])
    balance = next(x for x in p["requests"] if x["dataset"] == "TaiwanStockBalanceSheet")
    assert balance["start_date"] == balance["end_date"] == "2024-06-30"
    for ds in ("TaiwanStockWeekPrice", "TaiwanStockMonthPrice"):
        q = next(x for x in p["requests"] if x["dataset"] == ds)
        assert (q["start_date"], q["end_date"]) == ("2020-03-01", "2024-12-31")
    price = [x for x in p["requests"] if x["dataset"] == "TaiwanStockPrice"]
    assert {x["start_date"] for x in price} == {"2015-11-23", "2020-03-09", "2020-03-10", "2020-04-24"}


def test_duplicate_or_invalid_entry_identity_fails_loudly():
    with pytest.raises(ValueError, match="duplicate"):
        plan([entry(), entry()])
    with pytest.raises(ValueError, match="SHA-256"):
        plan([entry(entry_id="short")])
    with pytest.raises(ValueError, match="timezone-aware"):
        build_repair_plan([], now=NOW.replace(tzinfo=None), scopes=EMPTY)


def test_future_notice_and_invalid_state_do_not_create_intents():
    p = plan([entry(notice_date="2026-09-28")])
    assert not p["requests"]
    assert p["notices"][0]["status"] == "needs_review"
    state = deepcopy(plan([entry()])["state"])
    state["entries"][entry()["entry_id"]]["required_after_utc"] = "2026-09-26T00:00:00"
    with pytest.raises(ValueError, match="timezone-aware"):
        plan([entry()], state)
