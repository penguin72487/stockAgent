"""All ordinary TW strategies share this source producer, not just Factorized."""
from datetime import date
import hashlib
import json

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from polars.testing import assert_frame_equal
import pytest

from stockagent.data.tw_public_incremental import session_fingerprints
import stockagent.data.tw_public_incremental as incremental_module
import stockagent.data.tw_public_features as feature_module
from stockagent.data.tw_public_features import build_tw_public_training_features


def write_daily(root, days, closes):
    pl.DataFrame({"date": days, "證券代號": ["2330"] * len(days),
                  "收盤價": closes, "最高價": closes, "最低價": closes,
                  "成交股數": ["1000"] * len(days), "成交金額": ["100000"] * len(days),
                  "成交筆數": ["10"] * len(days)}).write_parquet(root / "twse_daily_ohlcv.parquet")


def _unchanged_file_evidence(path):
    info = path.stat()
    return ((info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns),
            hashlib.sha256(path.read_bytes()).hexdigest())


def _write_cbc_money(path, scenario="original"):
    rows = [
        {"期間": "2024M04", "貨幣總計數-Ｍ１Ｂ-原始值": "100",
         "貨幣總計數-Ｍ２-原始值": "200", "_as_of_date": "2024-06-10"},
        {"期間": "2024M05", "貨幣總計數-Ｍ１Ｂ-原始值": "120",
         "貨幣總計數-Ｍ２-原始值": "220", "_as_of_date": "2024-06-11"},
    ]
    if scenario == "old_revision":
        rows[0]["貨幣總計數-Ｍ１Ｂ-原始值"] = "105"
    elif scenario == "append":
        rows.append({"期間": "2024M06", "貨幣總計數-Ｍ１Ｂ-原始值": "130",
                     "貨幣總計數-Ｍ２-原始值": "230", "_as_of_date": "2024-06-12"})
    elif scenario == "remove_old":
        rows = rows[1:]
    elif scenario == "observation_clock":
        rows[0]["_as_of_date"] = "2024-06-12"
    elif scenario != "original":
        rows[0]["貨幣總計數-Ｍ１Ｂ-原始值"] = "106"
    pl.DataFrame(rows).write_parquet(path)


@pytest.mark.parametrize("scenario", [
    "old_revision", "append", "remove_old", "observation_clock",
    "mixed_stock", "changed_end", "invalid_output_receipt",
    "source_file_removed", "unrelated_dependency_added", "changed_feature_fingerprint",
])
def test_cbc_money_change_reuses_only_verified_stock_rows(tmp_path, scenario):
    source = tmp_path / "source"
    source.mkdir()
    days = ["2024-06-10", "2024-06-11", "2024-06-12"]
    write_daily(source, days, ["100", "101", "102"])
    money = source / "cbc_money_aggregates.parquet"
    _write_cbc_money(money)
    target = tmp_path / "candidate.parquet"
    common = dict(input_dir=source, symbols_root=None,
                  allow_daily_publication_lag=True, end_date=date(2024, 6, 12))
    first = build_tw_public_training_features(**common, output_path=target,
                                              incremental_auto=True)
    before = pl.read_parquet(target)
    _write_cbc_money(money, scenario)
    if scenario == "mixed_stock":
        write_daily(source, days, ["100", "101", "108"])
    if scenario == "invalid_output_receipt":
        before.with_columns(pl.lit(1.0).alias("twpub_cbc_m1b_raw")).write_parquet(target)
    if scenario == "source_file_removed":
        money.unlink()
    if scenario == "unrelated_dependency_added":
        pl.DataFrame({"date": days, "value": [1, 2, 3]}).write_parquet(
            source / "unrelated_but_registered_dependency.parquet"
        )
    if scenario == "changed_feature_fingerprint":
        summary_path = target.with_suffix(".summary.json")
        summary = json.loads(summary_path.read_text())
        summary["feature_implementation_sha256"] = "different-implementation"
        summary_path.write_text(json.dumps(summary))
    if scenario == "changed_end":
        common["end_date"] = date(2024, 6, 13)
    actual = build_tw_public_training_features(**common, output_path=target,
                                               incremental_auto=True)
    actual_frame = pl.read_parquet(target)
    if scenario in {"mixed_stock", "changed_end", "invalid_output_receipt",
                    "source_file_removed", "unrelated_dependency_added", "changed_feature_fingerprint"}:
        assert actual.build_mode == "full"
    else:
        assert actual.build_mode == "market_only_rebuild"
        assert actual.reused_rows == first.stock_rows
        assert "stock_official_ohlcv" not in actual.stage_elapsed_seconds
        assert_frame_equal(
            before.filter(pl.col("symbol") != "__MARKET__"),
            actual_frame.filter(pl.col("symbol") != "__MARKET__"),
            check_row_order=True, check_column_order=True,
        )
        if scenario == "old_revision":
            old_market = before.filter(pl.col("symbol") == "__MARKET__")
            new_market = actual_frame.filter(pl.col("symbol") == "__MARKET__")
            assert old_market["twpub_cbc_m1b_raw"].to_list() == [100.0, 120.0]
            assert new_market["twpub_cbc_m1b_raw"].to_list() == [105.0, 120.0]
    reference = tmp_path / "full.parquet"
    build_tw_public_training_features(**common, output_path=reference)
    assert_frame_equal(actual_frame, pl.read_parquet(reference),
                       check_row_order=True, check_column_order=True)


@pytest.mark.parametrize("change", [
    "append_market_day", "revise_old_market_day", "remove_old_market_day",
    "mixed_stock_change", "end_extended", "invalid_output_receipt",
    "changed_feature_fingerprint",
])
def test_dated_market_only_change_reuses_verified_stock_rows(tmp_path, change):
    source = tmp_path / "source"
    source.mkdir()
    days = ["2024-01-02", "2024-01-03", "2024-01-04"]
    write_daily(source, days, ["100", "101", "102"])
    futures = source / "taifex_daily_futures.parquet"

    def write_futures(future_days, settlements):
        pl.DataFrame({
            "Date": [day.replace("-", "") for day in future_days],
            "date": ["2024-02-01"] * len(future_days),
            "Contract": ["TX"] * len(future_days),
            "TradingSession": ["一般"] * len(future_days),
            "ContractMonth(Week)": ["202401"] * len(future_days),
            "Volume": ["100"] * len(future_days),
            "OpenInterest": ["500"] * len(future_days),
            "SettlementPrice": settlements,
        }).write_parquet(futures)

    write_futures(days[:2], ["18000", "18100"])
    target = tmp_path / "candidate.parquet"
    common = dict(input_dir=source, symbols_root=None,
                  allow_daily_publication_lag=True, end_date=date(2024, 1, 4))
    before = build_tw_public_training_features(**common, output_path=target, incremental_auto=True)
    prior = pl.read_parquet(target)
    if change == "revise_old_market_day":
        write_futures(days[:2], ["17000", "18100"])
    elif change == "remove_old_market_day":
        write_futures(days[1:2], ["18100"])
    else:
        write_futures(days, ["18000", "18100", "18200"])
    if change == "mixed_stock_change":
        write_daily(source, days, ["100", "101", "108"])
    elif change == "end_extended":
        common["end_date"] = date(2024, 1, 5)
    elif change == "invalid_output_receipt":
        prior.with_columns(pl.lit(1.0).alias("twpub_official_close_logret_1d")).write_parquet(target)
    elif change == "changed_feature_fingerprint":
        summary_path = target.with_suffix(".summary.json")
        summary = json.loads(summary_path.read_text())
        summary["feature_implementation_sha256"] = "different-implementation"
        summary_path.write_text(json.dumps(summary))

    actual = build_tw_public_training_features(**common, output_path=target, incremental_auto=True)
    candidate = pl.read_parquet(target)
    if change in {"append_market_day", "revise_old_market_day", "remove_old_market_day"}:
        assert actual.build_mode == "market_only_rebuild"
        assert actual.reused_rows == before.stock_rows
        assert "stock_official_ohlcv" not in actual.stage_elapsed_seconds
        assert_frame_equal(prior.filter(pl.col("symbol") != "__MARKET__"),
                           candidate.filter(pl.col("symbol") != "__MARKET__"))
    elif change in {"invalid_output_receipt", "changed_feature_fingerprint"}:
        assert actual.build_mode == "full"
    else:
        assert actual.build_mode == "incremental_tail"

    reference = tmp_path / "full.parquet"
    build_tw_public_training_features(**common, output_path=reference)
    assert_frame_equal(candidate, pl.read_parquet(reference),
                       check_row_order=True, check_column_order=True)


@pytest.mark.parametrize("changed_during_build", ["source", "base_output"])
def test_market_only_change_during_build_does_not_publish(tmp_path, monkeypatch, changed_during_build):
    source = tmp_path / "source"
    source.mkdir()
    days = ["2024-01-02", "2024-01-03"]
    write_daily(source, days, ["100", "101"])
    futures = source / "taifex_daily_futures.parquet"

    def write_futures(settlement):
        pl.DataFrame({"Date": ["20240102", "20240103"], "Contract": ["TX", "TX"],
                      "ContractMonth(Week)": ["202401", "202401"],
                      "Volume": ["100", "100"], "OpenInterest": ["500", "500"],
                      "SettlementPrice": ["18000", settlement]}).write_parquet(futures)

    write_futures("18100")
    target = tmp_path / "candidate.parquet"
    common = dict(input_dir=source, output_path=target, symbols_root=None,
                  allow_daily_publication_lag=True, end_date=date(2024, 1, 3), incremental_auto=True)
    build_tw_public_training_features(**common)
    old_output = target.read_bytes()
    summary = target.with_suffix(".summary.json")
    old_summary = summary.read_bytes()
    write_futures("18200")
    original_builder = feature_module._build_taifex_tx_features

    def change_after_build(*args, **kwargs):
        frame = original_builder(*args, **kwargs)
        if changed_during_build == "source":
            write_futures("18300")
        else:
            target.touch()
        return frame

    monkeypatch.setattr(feature_module, "_build_taifex_tx_features", change_after_build)
    expected = "source parquet changed" if changed_during_build == "source" else "feature base changed"
    with pytest.raises(RuntimeError, match=expected):
        build_tw_public_training_features(**common)
    assert target.read_bytes() == old_output
    assert summary.read_bytes() == old_summary


@pytest.mark.parametrize("change", [
    "append_visible", "revise_old", "delete_old", "duplicate_order", "reorder_collision_days", "clock_shift",
    "cosmetic_only", "after_end", "after_end_and_nonfeature", "mixed_stock_change", "end_extended",
])
def test_tpex_shorting_semantic_partitions_match_full_builder(tmp_path, change):
    source = tmp_path / "source"
    source.mkdir()
    days = ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08", "2024-01-09"]
    write_daily(source, days, ["100", "101", "102", "103", "104", "105"])
    shorting = source / "tpex_api_tpex_margin_sbl.parquet"
    rows = [
        {"Date": "1130102", "SecuritiesCompanyCode": "2330", "SaleAvailableVolumes": "10",
         "SecuritiesBorrowingBalanceOfTheMarketDay": "50", "AvailableVolumesForSBLShortSale": "20",
         "_as_of_date": "2024-01-02", "_downloaded_at_utc": "2024-01-02T00:30:00Z", "CompanyName": "old"},
        {"Date": "1130102", "SecuritiesCompanyCode": "2330", "SaleAvailableVolumes": "11",
         "SecuritiesBorrowingBalanceOfTheMarketDay": "51", "AvailableVolumesForSBLShortSale": "21",
         "_as_of_date": "2024-01-02", "_downloaded_at_utc": "2024-01-02T00:30:00Z", "CompanyName": "old"},
        {"Date": "1130103", "SecuritiesCompanyCode": "2330", "SaleAvailableVolumes": "12",
         "SecuritiesBorrowingBalanceOfTheMarketDay": "52", "AvailableVolumesForSBLShortSale": "22",
         "_as_of_date": "2024-01-03", "_downloaded_at_utc": "2024-01-03T01:30:00Z", "CompanyName": "old"},
        # Both weekend availability dates map to Monday's same exchange key.
        {"Date": "1130106", "SecuritiesCompanyCode": "2330", "SaleAvailableVolumes": "30",
         "SecuritiesBorrowingBalanceOfTheMarketDay": "60", "AvailableVolumesForSBLShortSale": "30",
         "_as_of_date": "2024-01-06", "_downloaded_at_utc": "2024-01-06T00:30:00Z", "CompanyName": "old"},
        {"Date": "1130107", "SecuritiesCompanyCode": "2330", "SaleAvailableVolumes": "40",
         "SecuritiesBorrowingBalanceOfTheMarketDay": "70", "AvailableVolumesForSBLShortSale": "40",
         "_as_of_date": "2024-01-07", "_downloaded_at_utc": "2024-01-07T00:30:00Z", "CompanyName": "old"},
    ]
    pl.DataFrame(rows).write_parquet(shorting)
    # Both sources can contribute the same feature for the same issuer/day.
    pl.DataFrame({"date": ["2024-01-03"], "TWSECode": ["2330"],
                  "TWSEAvailableVolume": ["24"], "GRETAICode": ["6488"],
                  "GRETAIAvailableVolume": ["25"]}).write_parquet(source / "twse_api_sbl_twt96u.parquet")
    extra_source = source / "tpex_api_tpex_short_sell.parquet"
    if change == "after_end_and_nonfeature":
        pl.DataFrame({"original": [1]}).write_parquet(extra_source)
    target = tmp_path / "candidate.parquet"
    common = dict(input_dir=source, output_path=target, symbols_root=None,
                  allow_daily_publication_lag=True, end_date=date(2024, 1, 8), incremental_auto=True)
    first = build_tw_public_training_features(**common)
    prior_bytes = target.read_bytes()
    assert first.input_partitions["sources"][shorting.name]["session_clock"]["column"] == (
        "canonical_tpex_shorting_availability"
    )
    rows = [dict(row) for row in rows]
    if change == "revise_old":
        rows[0]["SaleAvailableVolumes"] = "99"
    elif change == "delete_old":
        rows.pop(0)
    elif change == "duplicate_order":
        rows[0], rows[1] = rows[1], rows[0]
    elif change == "reorder_collision_days":
        rows[3], rows[4] = rows[4], rows[3]
    elif change == "clock_shift":
        rows[0]["_downloaded_at_utc"] = "2024-01-02T01:30:00Z"
    elif change == "cosmetic_only":
        rows[0]["CompanyName"] = "corrected name"
    elif change in {"after_end", "after_end_and_nonfeature"}:
        rows.append({**rows[0], "Date": "1130109", "_as_of_date": "2024-01-09",
                     "_downloaded_at_utc": "2024-01-09T01:30:00Z"})
    else:
        rows.append({**rows[0], "Date": "1130104", "_as_of_date": "2024-01-04",
                     "_downloaded_at_utc": "2024-01-04T01:30:00Z"})
    pl.DataFrame(rows).write_parquet(shorting)
    if change == "after_end_and_nonfeature":
        pl.DataFrame({"original": [2]}).write_parquet(extra_source)
    if change == "mixed_stock_change":
        write_daily(source, days, ["100", "101", "112", "103", "104", "105"])
    if change == "end_extended":
        common["end_date"] = date(2024, 1, 9)
    actual = build_tw_public_training_features(**common)
    candidate = pl.read_parquet(target)
    if change in {"cosmetic_only", "after_end", "after_end_and_nonfeature", "reorder_collision_days"}:
        assert actual.build_mode == "unchanged_verified"
        assert target.read_bytes() == prior_bytes
    else:
        assert actual.build_mode == "incremental_tail"
        assert actual.incremental_start_date == ("2024-01-02" if change in {
            "revise_old", "delete_old", "duplicate_order", "clock_shift"
        } else ("2024-01-04" if change == "mixed_stock_change" else "2024-01-05"))
    reference = tmp_path / "full.parquet"
    build_tw_public_training_features(source, reference, symbols_root=None,
        allow_daily_publication_lag=True, end_date=common["end_date"])
    assert_frame_equal(candidate, pl.read_parquet(reference), check_exact=True,
                       check_row_order=True, check_column_order=True)


def test_tpex_shorting_proof_rejects_receipt_mismatch(tmp_path):
    path = tmp_path / "tpex_api_tpex_margin_sbl.parquet"
    pl.DataFrame({"Date": ["1130102"]}).write_parquet(path)
    with pytest.raises(ValueError, match="does not match its captured receipt"):
        incremental_module._tpex_shorting_feature_session_fingerprints(path, source_sha256="0" * 64)


def test_tpex_semantic_proof_is_bound_to_feature_implementation(tmp_path):
    path = tmp_path / "tpex_api_tpex_margin_sbl.parquet"
    row = {"Date": "1130102", "SecuritiesCompanyCode": "2330",
           "SaleAvailableVolumes": "10", "SecuritiesBorrowingBalanceOfTheMarketDay": "50",
           "AvailableVolumesForSBLShortSale": "20"}
    pl.DataFrame([row]).write_parquet(path)
    receipts = feature_module._source_content_receipts(tmp_path)
    previous = incremental_module.capture_partitions(tmp_path, receipts, None)
    previous["sources"][path.name]["semantic_feature_implementation_sha256"] = "stale"
    refreshed = incremental_module.capture_partitions(tmp_path, receipts, previous)
    assert refreshed["work"] == {"scanned_sources": 1, "reused_sources": 0}
    assert refreshed["sources"][path.name]["semantic_feature_implementation_sha256"] == (
        feature_module._feature_implementation_sha256()
    )
    pl.DataFrame([{**row, "SaleAvailableVolumes": "11"}]).write_parquet(path)
    changed_receipts = feature_module._source_content_receipts(tmp_path)
    current = incremental_module.capture_partitions(tmp_path, changed_receipts, previous)
    start, plan = incremental_module.affected_start(
        {"source_receipts": receipts, "input_partitions": previous, "requested_end_date": "2024-01-02"},
        changed_receipts, current, date(2024, 1, 2),
    )
    assert start is None
    assert plan["reason"] == "source_partition_proof_missing_or_schema_changed"


def test_tpex_semantic_proof_rejects_missing_feature_columns(tmp_path):
    path = tmp_path / "tpex_api_tpex_margin_sbl.parquet"
    pl.DataFrame({"Date": ["1130102"]}).write_parquet(path)
    receipts = feature_module._source_content_receipts(tmp_path)
    proof = incremental_module.capture_partitions(tmp_path, receipts, None)
    assert proof["sources"][path.name]["unsupported"] == "ValueError"


def test_six_unused_tpex_sources_update_receipts_without_rebuilding(tmp_path):
    names = {
        "tpex_api_tpex_3insti_qfii.parquet", "tpex_api_tpex_3insti_qfii_industry.parquet",
        "tpex_api_tpex_margin_trading_margin_mark.parquet", "tpex_api_tpex_margin_trading_margin_used.parquet",
        "tpex_api_tpex_margin_trading_short_sell.parquet", "tpex_api_tpex_short_sell.parquet",
    }
    source = tmp_path / "source"
    source.mkdir()
    write_daily(source, ["2024-01-02", "2024-01-03"], ["100", "101"])
    for name in names:
        pl.DataFrame({"original": [1]}).write_parquet(source / name)
    target = tmp_path / "candidate.parquet"
    common = dict(input_dir=source, output_path=target, symbols_root=None,
                  allow_daily_publication_lag=True, end_date=date(2024, 1, 3), incremental_auto=True)
    build_tw_public_training_features(**common)
    original = target.read_bytes()
    for name in names:
        pl.DataFrame({"original": [2]}).write_parquet(source / name)
    actual = build_tw_public_training_features(**common)
    assert actual.build_mode == "unchanged_verified"
    assert set(actual.incremental_plan["non_feature_sources"]) == names
    assert target.read_bytes() == original
    receipts = {entry["name"]: entry["sha256"] for entry in actual.source_receipts}
    for name in names:
        assert receipts[name] == hashlib.sha256((source / name).read_bytes()).hexdigest()
    reference = tmp_path / "full.parquet"
    build_tw_public_training_features(source, reference, symbols_root=None,
        allow_daily_publication_lag=True, end_date=common["end_date"])
    assert_frame_equal(pl.read_parquet(target), pl.read_parquet(reference), check_exact=True)


@pytest.mark.parametrize("change", ["new_day", "old_revision", "remove_old", "long_halt"])
def test_actual_shared_producer_matches_full_rebuild_for_each_affected_tail(tmp_path, change):
    source = tmp_path / "source"
    source.mkdir()
    days = ["2024-01-02", "2024-01-03", "2024-01-04"]
    write_daily(source, days, ["100", "101", "102"])
    target = tmp_path / "incremental.parquet"
    common = dict(input_dir=source, symbols_root=None, allow_daily_publication_lag=True)
    first = build_tw_public_training_features(**common, output_path=target,
        end_date=date(2024, 1, 4), incremental_auto=True)
    assert first.input_partitions["sources"]["twse_daily_ohlcv.parquet"]["sessions"]
    if change == "new_day":
        days += ["2024-01-05"]
        values = ["100", "101", "102", "103"]
        end, expected_start = date(2024, 1, 5), "2024-01-05"
    elif change == "old_revision":
        values = ["110", "101", "102"]
        end, expected_start = date(2024, 1, 4), "2024-01-02"
    elif change == "remove_old":
        days = days[1:]
        values = ["101", "102"]
        end, expected_start = date(2024, 1, 4), "2024-01-02"
    else:
        days += ["2024-10-01"]
        values = ["100", "101", "102", "110"]
        end, expected_start = date(2024, 10, 1), "2024-01-05"
    write_daily(source, days, values)
    delta = build_tw_public_training_features(**common, output_path=target,
        end_date=end, incremental_auto=True)
    assert delta.build_mode == "incremental_tail"
    assert delta.incremental_start_date == expected_start
    reference = tmp_path / "reference.parquet"
    build_tw_public_training_features(**common, output_path=reference, end_date=end)
    assert_frame_equal(pl.read_parquet(target), pl.read_parquet(reference),
                       check_row_order=True, check_column_order=True)
    if change == "new_day":
        assert delta.reused_rows == first.rows
    if change == "long_halt":
        actual = pl.read_parquet(target).filter(pl.col("date") == end)["twpub_official_close_logret_1d"].item()
        assert actual == pytest.approx(np.log(110 / 102))


def test_unchanged_proofs_are_reused_and_unknown_dependency_forces_full_build(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    days = ["2024-01-02", "2024-01-03"]
    write_daily(source, days, ["100", "101"])
    unknown = source / "unrelated_but_registered_dependency.parquet"
    pl.DataFrame({"date": days, "value": [1, 2]}).write_parquet(unknown)
    target = tmp_path / "incremental.parquet"
    common = dict(input_dir=source, output_path=target, symbols_root=None,
                  allow_daily_publication_lag=True, end_date=date(2024, 1, 3), incremental_auto=True)
    build_tw_public_training_features(**common)
    original = _unchanged_file_evidence(target)
    summary = target.with_suffix(".summary.json")
    summary_stat = summary.stat()
    unchanged = build_tw_public_training_features(**common)
    assert unchanged.build_mode == "unchanged_verified"
    assert unchanged.input_partitions["work"] == {"reused_sources": 1, "scanned_sources": 0}
    assert _unchanged_file_evidence(target) == original
    after = summary.stat()
    assert (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns) == (
        summary_stat.st_ino, summary_stat.st_size, summary_stat.st_mtime_ns, summary_stat.st_ctime_ns)
    pl.DataFrame({"date": days, "value": [2, 3]}).write_parquet(unknown)
    rebuilt = build_tw_public_training_features(**common)
    assert rebuilt.build_mode == "full"
    assert rebuilt.incremental_plan["reason"] == "non_dated_or_global_dependency_changed"


@pytest.mark.parametrize("name", sorted(incremental_module.NON_FEATURE_SOURCES))
def test_audited_non_feature_source_updates_receipt_without_rewriting_output(tmp_path, name):
    source = tmp_path / "source"
    source.mkdir()
    write_daily(source, ["2024-01-02", "2024-01-03", "2024-01-04"], ["100", "101", "102"])
    unrelated = source / name
    pl.DataFrame({"original": [1, 2]}).write_parquet(unrelated)
    target = tmp_path / "candidate.parquet"
    common = dict(input_dir=source, symbols_root=None,
                  allow_daily_publication_lag=True, end_date=date(2024, 1, 4))
    build_tw_public_training_features(**common, output_path=target, incremental_auto=True)
    old_output = _unchanged_file_evidence(target)
    before = json.loads(target.with_suffix(".summary.json").read_text())
    pl.DataFrame({"original": [2, 3]}).write_parquet(unrelated)
    background = source / "gcis_open_data_catalog.parquet"
    pl.DataFrame({"catalog": ["new"]}).write_parquet(background)
    actual = build_tw_public_training_features(**common, output_path=target, incremental_auto=True)
    after = json.loads(target.with_suffix(".summary.json").read_text())
    assert actual.build_mode == "unchanged_verified"
    assert actual.incremental_plan["non_feature_sources"] == [name]
    assert _unchanged_file_evidence(target) == old_output
    assert str(unrelated) in after["source_files"]
    assert str(background) in after["source_files"]
    assert background.name not in {row["name"] for row in after["source_receipts"]}
    old_receipt = next(row for row in before["source_receipts"] if row["name"] == name)
    new_receipt = next(row for row in after["source_receipts"] if row["name"] == name)
    assert old_receipt != new_receipt
    assert new_receipt["sha256"] == hashlib.sha256(unrelated.read_bytes()).hexdigest()
    assert after["source_receipts"] == actual.source_receipts
    reference = tmp_path / "full.parquet"
    build_tw_public_training_features(**common, output_path=reference)
    assert_frame_equal(pl.read_parquet(target), pl.read_parquet(reference),
                       check_row_order=True, check_column_order=True)


@pytest.mark.parametrize("non_feature_source", [
    "tpex_api_tpex50_constituents.parquet", "tpex_api_tpex200_constituents.parquet",
])
@pytest.mark.parametrize("scenario", [
    "dated_change", "end_extended", "unknown_changed", "source_added",
    "source_removed", "unverified_output", "feature_changed", "proof_expired",
])
def test_non_feature_proof_preserves_dated_work_and_rejects_unknowns(tmp_path, scenario, monkeypatch, non_feature_source):
    source = tmp_path / "source"
    source.mkdir()
    days = ["2024-01-02", "2024-01-03", "2024-01-04"]
    write_daily(source, days, ["100", "101", "102"])
    unrelated = source / non_feature_source
    unknown = source / "unverified_dependency.parquet"
    pl.DataFrame({"original": [1]}).write_parquet(unrelated)
    pl.DataFrame({"original": [1]}).write_parquet(unknown)
    target = tmp_path / "candidate.parquet"
    common = dict(input_dir=source, symbols_root=None,
                  allow_daily_publication_lag=True, end_date=date(2024, 1, 4))
    build_tw_public_training_features(**common, output_path=target, incremental_auto=True)
    pl.DataFrame({"original": [2]}).write_parquet(unrelated)
    if scenario == "dated_change":
        write_daily(source, days, ["100", "101", "108"])
    elif scenario == "end_extended":
        common["end_date"] = date(2024, 1, 5)
    elif scenario == "unknown_changed":
        pl.DataFrame({"original": [2]}).write_parquet(unknown)
    elif scenario == "source_added":
        pl.DataFrame({"original": [1]}).write_parquet(source / "new_dependency.parquet")
    elif scenario == "source_removed":
        unrelated.unlink()
    elif scenario == "unverified_output":
        pl.read_parquet(target).with_columns(pl.lit(1.0).alias("twpub_cbc_m1b_raw")).write_parquet(target)
    elif scenario == "feature_changed":
        summary_path = target.with_suffix(".summary.json")
        summary = json.loads(summary_path.read_text())
        summary["feature_implementation_sha256"] = "different-implementation"
        summary_path.write_text(json.dumps(summary))
    elif scenario == "proof_expired":
        monkeypatch.setattr(incremental_module, "NON_FEATURE_CONSUMPTION_PROOF", "expired")
    actual = build_tw_public_training_features(**common, output_path=target, incremental_auto=True)
    if scenario in {"dated_change", "end_extended"}:
        assert actual.build_mode == "incremental_tail"
        assert actual.incremental_start_date == ("2024-01-04" if scenario == "dated_change" else "2024-01-05")
    else:
        assert actual.build_mode == "full"
    reference = tmp_path / "full.parquet"
    build_tw_public_training_features(**common, output_path=reference)
    assert_frame_equal(pl.read_parquet(target), pl.read_parquet(reference),
                       check_row_order=True, check_column_order=True)


def test_non_feature_and_institutional_revision_match_full_as_of_features(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    days = ["2024-01-02", "2024-01-03", "2024-01-04"]
    write_daily(source, days, ["100", "101", "102"])
    unrelated = source / "tpex_api_tpex50_constituents.parquet"
    pl.DataFrame({"original": [1]}).write_parquet(unrelated)
    institutional = source / "twse_institutional_trades.parquet"

    def write_institutional(value):
        pl.DataFrame({
            "date": days[:2], "證券代號": ["2330", "2330"],
            "外資買賣超股數": ["1", value], "投信買賣超股數": ["0", "0"],
            "自營商買賣超股數": ["0", "0"], "三大法人買賣超股數": ["1", value],
        }).write_parquet(institutional)

    write_institutional("2")
    target = tmp_path / "candidate.parquet"
    common = dict(input_dir=source, symbols_root=None,
                  allow_daily_publication_lag=True, end_date=date(2024, 1, 4))
    build_tw_public_training_features(**common, output_path=target, incremental_auto=True)
    pl.DataFrame({"original": [2]}).write_parquet(unrelated)
    write_institutional("20")
    actual = build_tw_public_training_features(**common, output_path=target, incremental_auto=True)
    assert actual.build_mode == "incremental_tail"
    assert actual.incremental_start_date == "2024-01-03"
    assert actual.incremental_plan["affected_source_sessions"][institutional.name] == ["2024-01-03"]
    frame = pl.read_parquet(target)
    observed = frame.filter((pl.col("symbol") == "2330") &
                            (pl.col("date") == date(2024, 1, 4)))
    assert observed["twpub_institutional_net_buy_shares_raw"].item() == 20.0
    reference = tmp_path / "full.parquet"
    build_tw_public_training_features(**common, output_path=reference)
    assert_frame_equal(frame, pl.read_parquet(reference),
                       check_row_order=True, check_column_order=True)


def test_non_feature_source_change_during_proof_cannot_publish(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    write_daily(source, ["2024-01-02", "2024-01-03"], ["100", "101"])
    unrelated = source / "tpex_api_tpex50_constituents.parquet"
    pl.DataFrame({"original": [1]}).write_parquet(unrelated)
    target = tmp_path / "candidate.parquet"
    common = dict(input_dir=source, output_path=target, symbols_root=None,
                  allow_daily_publication_lag=True, end_date=date(2024, 1, 3), incremental_auto=True)
    build_tw_public_training_features(**common)
    summary_path = target.with_suffix(".summary.json")
    prior_summary = summary_path.read_bytes()
    prior_output = _unchanged_file_evidence(target)
    pl.DataFrame({"original": [2]}).write_parquet(unrelated)
    original = feature_module._source_content_receipts
    calls = 0

    def changing_receipts(root):
        nonlocal calls
        calls += 1
        if calls == 2:
            pl.DataFrame({"original": [3]}).write_parquet(unrelated)
        return original(root)

    monkeypatch.setattr(feature_module, "_source_content_receipts", changing_receipts)
    with pytest.raises(RuntimeError, match="source parquet changed"):
        build_tw_public_training_features(**common)
    assert _unchanged_file_evidence(target) == prior_output
    assert summary_path.read_bytes() == prior_summary


def test_partition_sha_does_not_depend_on_parquet_chunking_but_retains_null_and_duplicate_order(tmp_path):
    path = tmp_path / "daily.parquet"
    table = pa.table({"date": ["2024-01-02"] * 20 + ["2024-01-03"] * 10,
                      "symbol": ["2330"] * 30, "value": [None, -0.0, 1.123456789] * 10})
    pq.write_table(table, path, row_group_size=7)
    first = session_fingerprints(path)
    pq.write_table(table, path, row_group_size=17)
    assert session_fingerprints(path) == first
    changed = table.set_column(2, "value", pa.array([0.0, None, 1.123456789] * 10))
    pq.write_table(changed, path)
    assert session_fingerprints(path) != first


@pytest.mark.parametrize("values", [["2024-01-03", "2024-01-02"], ["2024-01-02", None],
                                   ["2024/01/02", "2024/01/03"]])
def test_unsupported_source_dates_cannot_authorize_prefix_reuse(tmp_path, values):
    path = tmp_path / "source.parquet"
    pl.DataFrame({"date": values, "value": [1.0, 2.0]}).write_parquet(path)
    with pytest.raises(ValueError):
        session_fingerprints(path)


def test_native_date32_proofs_survive_append_and_row_group_boundaries(tmp_path):
    path = tmp_path / "dated.parquet"
    table = pa.table({"date": pa.array([date(2026, 10, 19)] * 4 + [date(2026, 10, 20)] * 3, type=pa.date32()),
                      "value": [None, -0.0, 1.123456789, float("nan"), 3.0, 4.0, None]})
    pq.write_table(table.slice(0, 4), path, row_group_size=2)
    old = session_fingerprints(path)
    pq.write_table(table, path, row_group_size=3)
    current = session_fingerprints(path)
    assert current["sessions"]["2026-10-19"] == old["sessions"]["2026-10-19"]


def _logical_sessions(proof):
    return {key: proof[key] for key in ("schema_sha256", "sessions", "session_clock")}


@pytest.mark.parametrize("change", ["append", "revise_middle", "remove_tail", "regroup", "delete_middle"])
def test_compressed_group_prefix_matches_independent_complete_logical_proof(tmp_path, change):
    path = tmp_path / "source.parquet"
    days = [f"2024-01-{index:02}" for index in range(1, 21) for _ in range(3)]
    values = [None, -0.0, 1.123456789, float("nan"), 0.0] * 12
    old = pa.table({"date": days[:-3], "symbol": ["2330"] * 57, "value": values[:-3]})
    pq.write_table(old, path, row_group_size=7)
    previous = session_fingerprints(path, capture_physical=True)
    current = pa.table({"date": days, "symbol": ["2330"] * 60, "value": values})
    if change == "revise_middle":
        values[30] = 900.0
        current = current.set_column(2, "value", pa.array(values))
    elif change == "remove_tail":
        current = old.slice(0, 47)
    elif change == "delete_middle":
        current = pa.concat_tables([current.slice(0, 18), current.slice(21)])
    pq.write_table(current, path, row_group_size=11 if change == "regroup" else 7)
    actual = session_fingerprints(path, previous=previous, capture_physical=True)
    assert _logical_sessions(actual) == session_fingerprints(path)
    work = actual["row_group_work"]
    assert work["decoded_groups"] + work["reused_groups"] == work["total_groups"]
    if change == "append":
        assert work["reused_groups"] > 0
        assert work["decoded_rows"] < current.num_rows // 2
    if change == "regroup":
        assert work["reused_groups"] == 0


@pytest.mark.parametrize("damage", ["logical", "group", "checksum", "schema"])
def test_bad_group_checkpoint_is_only_a_full_scan_miss(tmp_path, damage):
    path = tmp_path / "source.parquet"
    table = pa.table({"date": [f"2024-01-{day:02}" for day in range(1, 20)], "value": range(19)})
    pq.write_table(table, path, row_group_size=3)
    previous = session_fingerprints(path, capture_physical=True)
    if damage == "logical":
        previous["sessions"]["2024-01-01"]["sha256"] = "corrupt"
    elif damage == "schema":
        previous["schema_sha256"] = "corrupt"
    elif damage == "group":
        previous["physical_prefix"]["groups"][0]["rows"] = 99
    else:
        previous["physical_prefix"]["checkpoint_sha256"] = "corrupt"
    actual = session_fingerprints(path, previous=previous, capture_physical=True)
    assert actual["row_group_work"]["reused_groups"] == 0
    assert _logical_sessions(actual) == session_fingerprints(path)


def test_group_reuse_cannot_hide_a_backward_date_or_partial_write(tmp_path, monkeypatch):
    import stockagent.data.tw_public_incremental as module
    path = tmp_path / "source.parquet"
    table = pa.table({"date": [f"2024-01-{day:02}" for day in range(1, 20)], "value": range(19)})
    pq.write_table(table, path, row_group_size=3)
    previous = session_fingerprints(path, capture_physical=True)
    pq.write_table(pa.concat_tables([table, table.slice(0, 1)]), path, row_group_size=3)
    with pytest.raises(ValueError, match="chronologically"):
        session_fingerprints(path, previous=previous, capture_physical=True)
    pq.write_table(table, path, row_group_size=3)
    original = module._encoded_row_groups
    def mutate(*args):
        result = original(*args)
        with path.open("ab") as source:
            source.write(b"partial-write")
        return result
    monkeypatch.setattr(module, "_encoded_row_groups", mutate)
    with pytest.raises(OSError, match="changed during"):
        session_fingerprints(path, previous=previous, capture_physical=True)


def test_boundary_session_spanning_many_groups_is_never_split_by_reuse(tmp_path):
    path = tmp_path / "source.parquet"
    old = pa.table({"date": ["2024-01-01"] * 20 + ["2024-01-02"] * 30, "value": range(50)})
    pq.write_table(old, path, row_group_size=7)
    previous = session_fingerprints(path, capture_physical=True)
    current = pa.concat_tables([old, pa.table({"date": ["2024-01-02"] * 6, "value": range(50, 56)})])
    pq.write_table(current, path, row_group_size=7)
    actual = session_fingerprints(path, previous=previous, capture_physical=True)
    assert _logical_sessions(actual) == session_fingerprints(path)
    assert actual["row_group_work"]["reused_groups"] < 3


@pytest.mark.parametrize("invalid", [[], {"input_partitions": []}, {"source_receipts": [1]},
                                     {"input_partitions": {"sources": []}}])
def test_bad_summary_never_acknowledges_or_reuses_unverified_proofs(tmp_path, invalid):
    source = tmp_path / "source"
    source.mkdir()
    write_daily(source, ["2024-01-02", "2024-01-03"], ["100", "101"])
    target = tmp_path / "features.parquet"
    common = dict(input_dir=source, output_path=target, symbols_root=None,
                  allow_daily_publication_lag=True, end_date=date(2024, 1, 3), incremental_auto=True)
    build_tw_public_training_features(**common)
    target.with_suffix(".summary.json").write_text(json.dumps(invalid))
    result = build_tw_public_training_features(**common)
    assert result.build_mode == "full"
    assert result.reused_rows == 0


def test_changed_feature_implementation_cannot_relabel_old_unchanged_payload(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    write_daily(source, ["2024-01-02", "2024-01-03"], ["100", "101"])
    target = tmp_path / "features.parquet"
    common = dict(input_dir=source, output_path=target, symbols_root=None,
                  allow_daily_publication_lag=True, end_date=date(2024, 1, 3), incremental_auto=True)
    build_tw_public_training_features(**common)
    summary_path = target.with_suffix(".summary.json")
    summary = json.loads(summary_path.read_text())
    summary["feature_implementation_sha256"] = "not_the_loaded_builder"
    summary_path.write_text(json.dumps(summary))
    result = build_tw_public_training_features(**common)
    assert result.build_mode == "full" and result.reused_rows == 0


def test_old_price_predecessor_is_not_misclassified_as_missing_tail_eligibility(tmp_path):
    from stockagent.data import tw_public_features as builder
    old, current = date(2024, 1, 2), date(2024, 10, 1)
    for venue, symbol, code in (("twse", "2330", "證券代號"), ("tpex", "6488", "代號")):
        pl.DataFrame({"date": [old, current], code: [symbol] * 2}).write_parquet(tmp_path / f"{venue}_daily_ohlcv.parquet")
        pl.DataFrame({"date": [old, current], "證券代號": [symbol] * 2, "證券名稱": ["test"] * 2,
            builder.DAY_TRADE_SUSPENSION_COLUMN: [""] * 2}).write_parquet(tmp_path / f"{venue}_day_trade_eligibility.parquet")
    expected = builder._build_day_trade_rule_features(tmp_path, end_date=current).filter(pl.col("date") == current)
    token = builder._INCREMENTAL_READ_RANGE.set((date(2024, 9, 1), current))
    try:
        actual = builder._build_day_trade_rule_features(tmp_path, end_date=current)
    finally:
        builder._INCREMENTAL_READ_RANGE.reset(token)
    assert_frame_equal(actual.sort("date", "symbol"), expected.sort("date", "symbol"))


@pytest.mark.parametrize("change", ["new_day", "old_revision", "invalid_dated_row"])
def test_taifex_shared_producer_binds_trade_day_not_download_day(tmp_path, change):
    source = tmp_path / "source"
    source.mkdir()
    days = ["2024-01-02", "2024-01-03", "2024-01-04"]
    write_daily(source, days, ["100", "101", "102"])
    futures = source / "taifex_daily_futures.parquet"

    def write_futures(count, revise=False, invalid=False):
        declared = [day.replace("-", "") for day in days[:count]]
        values = ["18000", "18100", "18200"][:count]
        if revise:
            values[0] = "17000"
        if invalid:
            declared[0] = "not_a_date"
        pl.DataFrame({"Date": [*declared, None],
            "date": ["2024-02-01"] * (count + 1),  # capture date is NOT the feature date
            "Contract": ["TX"] * (count + 1), "TradingSession": ["一般"] * (count + 1),
            "ContractMonth(Week)": ["202401"] * (count + 1),
            "Volume": ["100"] * (count + 1), "OpenInterest": ["500"] * (count + 1),
            "SettlementPrice": [*values, "99999"]}).write_parquet(futures)

    write_futures(2)
    target = tmp_path / "incremental.parquet"
    common = dict(input_dir=source, output_path=target, symbols_root=None,
                  allow_daily_publication_lag=True, incremental_auto=True)
    first = build_tw_public_training_features(**common, end_date=date(2024, 1, 3))
    proof = first.input_partitions["sources"][futures.name]
    assert set(proof["sessions"]) == {"2024-01-02", "2024-01-03"}
    assert proof["session_clock"] == {"column": "Date", "format": "yyyymmdd", "ignore_null": True}
    write_futures(3, revise=change == "old_revision", invalid=change == "invalid_dated_row")
    result = build_tw_public_training_features(**common, end_date=date(2024, 1, 4))
    if change == "invalid_dated_row":
        assert result.build_mode == "full"
        assert result.incremental_plan["reason"] == "source_partition_proof_missing_or_schema_changed"
    else:
        assert result.build_mode == "incremental_tail"
        assert result.incremental_start_date == ("2024-01-02" if change == "old_revision" else "2024-01-04")
    full = tmp_path / "full.parquet"
    build_tw_public_training_features(source, full, symbols_root=None,
        allow_daily_publication_lag=True, end_date=date(2024, 1, 4))
    assert_frame_equal(pl.read_parquet(target), pl.read_parquet(full))


@pytest.mark.parametrize("when", ["before", "during"])
def test_loaded_dependency_drift_never_relabels_or_replaces_the_verified_output(tmp_path, monkeypatch, when):
    from stockagent.data import tw_public_features as builder
    source = tmp_path / "source"
    source.mkdir()
    write_daily(source, ["2024-01-02"], ["100"])
    target = tmp_path / "features.parquet"
    common = dict(input_dir=source, output_path=target, symbols_root=None,
                  allow_daily_publication_lag=True, incremental_auto=True)
    build_tw_public_training_features(**common, end_date=date(2024, 1, 2))
    original = target.read_bytes()
    summary = target.with_suffix(".summary.json").read_bytes()
    write_daily(source, ["2024-01-02", "2024-01-03"], ["100", "101"])
    if when == "before":
        monkeypatch.setattr(builder, "_feature_implementation_sha256", lambda: "changed_dependency")
    else:
        owner = builder._build_official_ohlcv_features

        def change(*args, **kwargs):
            result = owner(*args, **kwargs)
            monkeypatch.setattr(builder, "_feature_implementation_sha256", lambda: "changed_dependency")
            return result

        monkeypatch.setattr(builder, "_build_official_ohlcv_features", change)
    with pytest.raises(RuntimeError, match="implementation changed"):
        build_tw_public_training_features(**common, end_date=date(2024, 1, 3))
    assert target.read_bytes() == original
    assert target.with_suffix(".summary.json").read_bytes() == summary
