from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import parse_qs, urlsplit

import pytest
import polars as pl

from downloader.download_crypto_historical_public import (
    _cftc_url,
    _wiki_url,
    _upsert_history,
    _profile,
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


def test_history_refresh_preserves_unchanged_clock_and_old_keys(tmp_path) -> None:
    before = datetime(2026, 9, 4, tzinfo=timezone.utc)
    now = datetime(2026, 9, 27, tzinfo=timezone.utc)
    path = tmp_path / "history.parquet"
    old = pl.DataFrame({"id": ["old", "same", "revised"], "value": [1, 2, 3],
                        "strict_available_at_utc": [before] * 3})
    old.write_parquet(path)
    old_bytes = path.read_bytes()
    fresh = pl.DataFrame({"id": ["same", "revised", "new"], "value": [2, 30, 4],
                          "strict_available_at_utc": [now] * 3})
    result = _upsert_history(path, fresh, ["id"])
    actual = {r["id"]: r for r in result.to_dicts()}
    assert set(actual) == {"old", "same", "revised", "new"}
    assert actual["same"]["strict_available_at_utc"] == before
    assert actual["old"]["strict_available_at_utc"] == before
    assert actual["revised"]["strict_available_at_utc"] == now
    assert actual["new"]["strict_available_at_utc"] == now
    assert actual["revised"]["value"] == 30
    assert next((tmp_path / "versions").iterdir()).read_bytes() == old_bytes
    assert path.read_bytes() == old_bytes  # caller owns the atomic commit


def test_history_refresh_rejects_duplicate_keys(tmp_path) -> None:
    with pytest.raises(ValueError, match="duplicate incoming"):
        _upsert_history(tmp_path / "history.parquet", pl.DataFrame({"id": [1, 1]}), ["id"])


def test_profile_counts_duplicates_beyond_first() -> None:
    frame = pl.DataFrame({"id": [1, 1, 1, 2], "date": [datetime(2020, 1, 1)] * 4})
    assert _profile(frame, ["id"], "date")["duplicate_rows_beyond_first"] == 2


def test_stored_dune_inventory_is_independent_of_last_run(tmp_path) -> None:
    from scripts.audit_crypto_historical_coverage import _footer_tree_profile

    partition = tmp_path / "dex_asset_activity/year=2019"
    partition.mkdir(parents=True)
    pl.DataFrame({"event_date": ["2019-01-01", "2019-01-02"]}).write_parquet(
        partition / "2019-01-01.parquet"
    )
    # A failed later run contributes zero new rows, not zero stored rows.
    (tmp_path / "download_summary.json").write_text('{"state":"blocked","rows":0}')
    result = _footer_tree_profile(tmp_path, "*/year=*/*.parquet", "event_date")
    assert result["rows_from_base_parquet_footers"] == 2
    assert result["earliest_event_date"] == "2019-01-01"
    assert result["latest_event_date"] == "2019-01-02"


def test_okx_source_gaps_do_not_falsely_invalidate_successful_mapped_build() -> None:
    from scripts.audit_crypto_historical_coverage import _okx_auxiliary_finding

    finding = _okx_auxiliary_finding(
        {"files": 493, "schema_complete_files": 483},
        {"failed_symbols": 0, "input_receipts": {"okx_symbols": {"path": "data_okx/1m/symbols.csv"}}},
    )
    assert finding is not None
    assert "failed_symbols=0" in finding["evidence"]
    assert "data_okx/1m/symbols.csv" in finding["evidence"]
    assert "not proof" in finding["impact"]
    assert "Do not infer or force a 15m fallback" in finding["action"]


def test_complete_okx_schema_does_not_emit_incomplete_finding() -> None:
    from scripts.audit_crypto_historical_coverage import _okx_auxiliary_finding

    assert _okx_auxiliary_finding({"files": 1, "schema_complete_files": 1}, {}) is None
