"""All ordinary TW strategies share this source producer, not just Factorized."""
from datetime import date
import json

import numpy as np
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from polars.testing import assert_frame_equal
import pytest

from stockagent.data.tw_public_incremental import session_fingerprints
from stockagent.data.tw_public_features import build_tw_public_training_features


def write_daily(root, days, closes):
    pl.DataFrame({"date": days, "證券代號": ["2330"] * len(days),
                  "收盤價": closes, "最高價": closes, "最低價": closes,
                  "成交股數": ["1000"] * len(days), "成交金額": ["100000"] * len(days),
                  "成交筆數": ["10"] * len(days)}).write_parquet(root / "twse_daily_ohlcv.parquet")


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
    original = target.stat()
    summary = target.with_suffix(".summary.json")
    summary_stat = summary.stat()
    unchanged = build_tw_public_training_features(**common)
    assert unchanged.build_mode == "unchanged_verified"
    assert unchanged.input_partitions["work"] == {"reused_sources": 1, "scanned_sources": 0}
    assert target.stat() == original
    after = summary.stat()
    assert (after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns) == (
        summary_stat.st_ino, summary_stat.st_size, summary_stat.st_mtime_ns, summary_stat.st_ctime_ns)
    pl.DataFrame({"date": days, "value": [2, 3]}).write_parquet(unknown)
    rebuilt = build_tw_public_training_features(**common)
    assert rebuilt.build_mode == "full"
    assert rebuilt.incremental_plan["reason"] == "non_dated_or_global_dependency_changed"


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
