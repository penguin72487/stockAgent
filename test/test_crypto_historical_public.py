from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import parse_qs, urlsplit

import pytest

from downloader.download_crypto_historical_public import (
    _cftc_url,
    _wiki_url,
    normalize_cftc,
    normalize_wikimedia,
)


def test_cftc_normalization_keeps_assumed_and_strict_clocks_separate() -> None:
    observed = datetime(2026, 9, 4, 12, 0, tzinfo=timezone.utc)
    frame = normalize_cftc(
        [
            {
                "id": "200101133741F",
                "market_and_exchange_names": "BITCOIN - CME",
                "report_date_as_yyyy_mm_dd": "2020-01-01T00:00:00.000",
                "cftc_contract_market_code": "133741",
                "commodity_subgroup_name": "DIGITAL ASSET",
                "open_interest_all": "100",
                "dealer_positions_long_all": "20",
                "dealer_positions_short_all": "5",
                "asset_mgr_positions_long": "7",
                "asset_mgr_positions_short": "9",
            }
        ],
        observed,
    )
    row = frame.row(0, named=True)
    assert row["dealer_net"] == pytest.approx(15.0)
    assert row["dealer_net_fraction"] == pytest.approx(0.15)
    assert row["assumed_available_at_utc"].date().isoformat() == "2020-01-05"
    assert row["strict_available_at_utc"] == observed
    assert row["causal_use_status"] == "quarantined_until_release_calendar_audit"


def test_wikimedia_normalization_has_unique_article_day_key() -> None:
    observed = datetime(2026, 9, 4, 12, 0, tzinfo=timezone.utc)
    frame = normalize_wikimedia(
        [
            {
                "project": "en.wikipedia",
                "article": "Bitcoin",
                "granularity": "daily",
                "timestamp": "2015070100",
                "access": "all-access",
                "agent": "user",
                "views": 1234,
            }
        ],
        "Bitcoin",
        observed,
    )
    row = frame.row(0, named=True)
    assert row["views"] == 1234
    assert row["event_date_utc"].date().isoformat() == "2015-07-01"
    assert row["assumed_available_at_utc"].date().isoformat() == "2015-07-03"
    assert row["strict_available_at_utc"] == observed
    assert row["causal_use_status"] == "research_only_until_revision_and_redirect_audit"


def test_source_urls_are_encoded_and_bounded() -> None:
    cftc = urlsplit(_cftc_url(datetime(2019, 1, 1).date(), datetime(2020, 1, 1).date()))
    query = parse_qs(cftc.query)
    assert query["$limit"] == ["50000"]
    assert "2019-01-01" in query["$where"][0]
    assert "2020-01-01" in query["$where"][0]
    wiki = _wiki_url("Tether_(cryptocurrency)", datetime(2015, 7, 1).date(), datetime(2020, 1, 1).date())
    assert wiki.endswith("/Tether_%28cryptocurrency%29/daily/2015070100/2020010100")
