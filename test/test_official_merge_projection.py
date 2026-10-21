"""Complete owner parity, not only newest OHLCV or an unchanged cache hit."""
from dataclasses import asdict
from datetime import date
import json

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from scripts.build_tw_official_symbol_parquets import _official_frame, _symbol_code_identity
from stockagent.data.parquet_group_projection import ParquetGroupProjection
from stockagent.data.partitioned_parquet import ParquetSource
from test_tw_official_symbol_builder import (
    _empty_twse_quotes, _write_core_official_sources, _write_empty_lifecycle_evidence,
)


@pytest.fixture(autouse=True)
def capacity_controls(tmp_path, monkeypatch):
    from stockagent.data_sync import cache_capacity
    controls = tmp_path / "controls"
    controls.mkdir()
    monkeypatch.setattr(cache_capacity, "CONTROL_ROOT", controls)
    monkeypatch.setattr(cache_capacity, "_admit_backing_volume", lambda *_: {})


def quotes(days):
    return pl.DataFrame({"date": days, "代號": ["6488"] * len(days),
        "名稱": ["環球晶"] * len(days), "開盤": [100.0] * len(days),
        "最高": [101.0] * len(days), "最低": [99.0] * len(days),
        "收盤": [100.0] * len(days), "成交股數": [1000.0] * len(days),
        "漲跌": ["--"] * len(days), "次日參考價": [100.0] * len(days)})


def build(root, cache, days, frame=None, **kwargs):
    _write_core_official_sources(root, twse=_empty_twse_quotes(),
        tpex=quotes(days) if frame is None else frame, sessions=days)
    projection = ParquetGroupProjection(cache, _symbol_code_identity())
    actual = _official_frame(root, source_cache=projection, incremental_merge=True, **kwargs)
    expected = _official_frame(root, **kwargs)
    assert_frame_equal(actual[0], expected[0], check_exact=True)
    assert actual[1:4] == expected[1:4]
    assert asdict(actual[-1]) == asdict(expected[-1])
    return projection.stats()["merge_projection"], actual


@pytest.mark.parametrize("change", ["append", "revision", "delete", "unchanged", "truncate",
                                   "regroup", "end_date", "lifecycle", "corrupt_member"])
def test_session_suffix_preserves_every_column_receipt_and_diagnostic(tmp_path, change):
    root, cache = tmp_path / "public", tmp_path / "cache"
    root.mkdir()
    _write_empty_lifecycle_evidence(root)
    days = [date(2024, 12, 30), date(2024, 12, 31), date(2025, 1, 2), date(2025, 1, 3)]
    first, _ = build(root, cache, days[:3])
    assert first["mode"] == "full"
    frame = quotes(days)
    options = {}
    if change == "revision":
        frame = frame.with_columns(pl.when(pl.col("date") == days[1]).then(90.0)
            .otherwise(pl.col("次日參考價")).alias("次日參考價"))
    elif change == "delete":
        frame = frame.filter(pl.col("date") != days[1])
    elif change == "truncate":
        days, frame = days[:1], frame.head(1)
    elif change == "unchanged":
        days, frame = days[:3], frame.head(3)
    elif change == "end_date":
        options["end_date"] = days[1]
    elif change == "lifecycle":
        pl.DataFrame({"公司代號": ["6488"], "公司簡稱": ["環球晶"],
                      "公司名稱": ["環球晶"], "上市日期": ["2024-12-31"]}).write_parquet(
                          root / "twse_listed_company_basic.parquet")
    elif change == "corrupt_member":
        pointer = json.loads((cache / "merged/current.json").read_bytes())
        with ParquetSource(cache / "merged" / pointer["manifest"]) as previous:
            path = previous.members[0]
        path.chmod(0o600)
        path.write_bytes(b"bad projection, not source")
    work, result = build(root, cache, days, frame, **options)
    if change in {"append", "regroup"}:
        assert work["mode"] == "suffix"
        assert work["reused_rows"] == 3 and work["rebuilt_rows"] == 1
        assert result[-1].previous_session_next_reference_candidate_rows == 3
    if change == "unchanged":
        assert work["mode"] == "unchanged" and work["rebuilt_rows"] == 0
        assert all(value["decoded_rows"] == 0 for value in work["source_proof_work"].values())
    if change in {"lifecycle", "corrupt_member"}:
        assert work["mode"] == "full"
    if change == "revision":
        assert work["replace_from"] == days[1].isoformat()
    assert work["old_generations_removed"] == 0


def test_merge_projection_is_explicit_opt_in_not_a_scheduled_writer_cutover(tmp_path, monkeypatch):
    from scripts.build_tw_official_symbol_parquets import parse_args
    root = tmp_path / "public"
    root.mkdir()
    _write_core_official_sources(root, twse=_empty_twse_quotes(),
        tpex=quotes([date(2025, 1, 2)]), sessions=[date(2025, 1, 2)])
    projection = ParquetGroupProjection(tmp_path / "cache", _symbol_code_identity())
    actual = _official_frame(root, source_cache=projection)
    expected = _official_frame(root)
    assert_frame_equal(actual[0], expected[0], check_exact=True)
    assert "merge_projection" not in projection.stats()
    assert not (tmp_path / "cache/merged/current.json").exists()
    monkeypatch.setattr("sys.argv", ["official-symbol-builder"])
    args = parse_args()
    assert args.output_layout == "file"
    assert not args.incremental_merge_cache


def test_suspended_symbol_retains_exact_old_selected_context(tmp_path):
    root, cache = tmp_path / "public", tmp_path / "cache"
    root.mkdir()
    _write_empty_lifecycle_evidence(root)
    days = [date(2024, 12, 30), date(2024, 12, 31), date(2025, 1, 2), date(2025, 1, 3)]
    prior = quotes(days[:3]).with_columns(pl.Series("代號", ["6488", "1101", "1101"]))
    build(root, cache, days[:3], prior)
    frame = pl.concat([prior, quotes(days[3:])])
    work, result = build(root, cache, days, frame)
    last = result[0].filter((pl.col("symbol") == "6488") & (pl.col("date") == days[3])).row(0, named=True)
    assert work["mode"] == "suffix"
    assert last["_selected_previous_date"] == days[0]
    assert not last["_previous_session_next_reference_eligible"]


def test_real_source_benchmark_runs_the_entire_original_merge_and_oracle(tmp_path):
    from scripts.benchmark_official_daily_merge import run
    root = tmp_path / "public"
    root.mkdir()
    _write_empty_lifecycle_evidence(root)
    days = [date(2024, 12, 30), date(2024, 12, 31), date(2025, 1, 2)]
    # The real accepted source shape includes both exchanges, even when one
    # venue has no rows on a particular fixture day.
    twse = _empty_twse_quotes().vstack(pl.DataFrame({
        "date": days, "證券代號": ["2330"] * 3, "證券名稱": ["台積電"] * 3,
        **{key: [100.0] * 3 for key in ("開盤價", "最高價", "最低價", "收盤價", "成交股數", "漲跌價差")},
        "漲跌(+/-)": ["+"] * 3}).select(_empty_twse_quotes().columns))
    _write_core_official_sources(root, twse=twse, tpex=quotes(days), sessions=days)
    receipt = run(root, tmp_path / "benchmark")
    assert receipt["status"].startswith("accepted_real_source_merge_daily")
    assert receipt["daily_shape"] == [6, 41]
    assert receipt["work"]["daily"]["merge_projection"]["mode"] == "suffix"
    assert receipt["ledger_writes"] == receipt["production_source_writes"] == 0
    reference = tmp_path / "benchmark/receipt.json"
    repeated = run(root, tmp_path / "reference_benchmark", reference_receipt=reference)
    assert repeated["oracle"] == receipt["oracle"]
    assert repeated["fixed_pre_change_reference"]["path"] == str(reference)
    quotes(days).with_columns(pl.lit(101.0).alias("收盤")).write_parquet(root / "tpex_daily_ohlcv.parquet")
    with pytest.raises(ValueError, match="reference original source bytes differ"):
        run(root, tmp_path / "changed_benchmark", reference_receipt=reference)


@pytest.mark.parametrize("empty", [False, True])
def test_lifecycle_scoped_lazy_quotes_preserve_eager_policy_and_empty_history(tmp_path, empty):
    from scripts.build_tw_official_symbol_parquets import _load_official_lifecycle_policy
    _write_empty_lifecycle_evidence(tmp_path)
    pl.DataFrame({"date": ["2025-01-02"], "market": ["tpex"], "symbol": ["6488"],
                  "company_name": ["舊名稱"], "delisting_reason": ["櫃轉市"]}).write_parquet(
                      tmp_path / "tpex_delisted_company.parquet")
    pl.DataFrame({"Code": ["6488"], "Company": ["環球晶"], "ListingDate": ["2025-01-03"],
                  "ApprovedListingDate": [""], "Note": ["櫃轉市"]}).write_parquet(
                      tmp_path / "twse_api_company_newlisting.parquet")
    frame = pl.DataFrame({"date": [date(2025, 1, 3)] * 4,
        "symbol": ["6488", "6488", "2330", "1101"],
        "market": ["twse", "twse", "twse", "twse"],
        "name": ["舊名稱", "環球晶", "台積電", "台泥"]})
    if empty:
        frame = frame.head(0)
    calendar = pl.DataFrame({"date": [date(2025, 1, 2), date(2025, 1, 3)]})
    eager = _load_official_lifecycle_policy(tmp_path, frame, calendar, required=True)
    lazy = _load_official_lifecycle_policy(tmp_path, frame.lazy(), calendar, required=True)
    assert_frame_equal(eager.terminal_intervals, lazy.terminal_intervals, check_exact=True)
    assert_frame_equal(eager.listing_boundaries, lazy.listing_boundaries, check_exact=True)
    assert eager.source_receipts == lazy.source_receipts
    assert eager.nonterminal_transfer_examples == lazy.nonterminal_transfer_examples
    if not empty:
        assert eager.terminal_intervals.is_empty()
        assert len(eager.nonterminal_transfer_examples) == 1


@pytest.mark.parametrize("archive_format", ["parquet", "csv"])
def test_suffix_keeps_legacy_yahoo_priority_and_all_dropped_row_statistics(tmp_path, archive_format):
    root, cache = tmp_path / "public", tmp_path / "cache"
    root.mkdir()
    _write_empty_lifecycle_evidence(root)
    days = [date(2024, 12, 30), date(2024, 12, 31), date(2025, 1, 2), date(2025, 1, 3)]
    legacy = root / f"legacy.{archive_format}"
    fallback = root / "yahoo.parquet"
    legacy_frame = pl.DataFrame({"date": days[:1], "symbol": ["1101"], "market": ["twse"],
        **{name: [50.0] for name in ("open", "max", "min", "close", "Trading_Volume")}})
    if archive_format == "csv":
        legacy_frame.write_csv(legacy)
    else:
        legacy_frame.write_parquet(legacy)
    def archives(selected):
        fallback_frame = pl.DataFrame({"date": [*selected, date(2024, 12, 29)],
            "symbol": ["6488"] * (len(selected) + 1), "market": ["tpex"] * (len(selected) + 1),
            **{name: [100.0] * (len(selected) + 1) for name in ("open", "max", "min", "close", "Trading_Volume")}}).sort("date")
        fallback_frame.write_parquet(fallback)
    options = dict(legacy_paths=[legacy], fallback_paths=[fallback])
    archives(days[:3])
    first = quotes(days[:3]).with_columns(pl.when(pl.col("date") == days[1]).then(0.0)
        .otherwise(pl.col("收盤")).alias("收盤"))
    build(root, cache, days[:3], first, **options)
    archives(days)
    latest = quotes(days).with_columns(pl.when(pl.col("date") == days[1]).then(0.0)
        .otherwise(pl.col("收盤")).alias("收盤"))
    work, actual = build(root, cache, days, latest, **options)
    assert work["mode"] == "suffix"
    assert actual[-1].fallback_replaced_unusable_official_rows == 1
    assert actual[-1].dropped_off_calendar_fallback_rows == 1
    assert actual[0].filter((pl.col("symbol") == "6488") & (pl.col("date") == days[1]))["quote_source"].item() == "yahoo_fallback"


def test_unrecognized_changed_clock_rebuilds_instead_of_assuming_append(tmp_path):
    root, cache = tmp_path / "public", tmp_path / "cache"
    root.mkdir()
    _write_empty_lifecycle_evidence(root)
    days = [date(2024, 12, 30), date(2024, 12, 31), date(2025, 1, 2)]
    archive = root / "legacy.csv"
    def legacy(value):
        pl.DataFrame({"date": [days[0]], "symbol": ["1101"], "market": ["twse"],
            **{name: [value] for name in ("open", "max", "min", "close", "Trading_Volume")}}).write_csv(archive)
    legacy(10.0)
    build(root, cache, days[:2], legacy_paths=[archive])
    legacy(11.0)
    work, _ = build(root, cache, days, legacy_paths=[archive])
    assert work["mode"] == "full"


@pytest.mark.parametrize("clock", ["date32", "iso", "unsupported_slash"])
def test_bounded_price_scan_requires_exact_decoded_clock_not_footer_statistics(tmp_path, clock):
    from datetime import timedelta
    import pyarrow as pa
    import pyarrow.parquet as pq
    from downloader.artifact_io import sha256_file
    from stockagent.data.official_merge_projection import OfficialMergeProjection
    days = [date(2025, 1, 1) + timedelta(days=offset) for offset in range(20)]
    values = days if clock == "date32" else [
        day.strftime("%Y/%m/%d") if clock == "unsupported_slash" else day.isoformat() for day in days]
    path = tmp_path / "original.parquet"
    # No footer statistics at all. Bounds must come from the proved actual
    # chronological values, not min/max hints or guessed append counts.
    pq.write_table(pa.table({"date": values, "value": [-0.0, None] * 10}),
                   path, row_group_size=1, write_statistics=False)
    with OfficialMergeProjection(tmp_path / "cache", {"test": "exact-clock"}) as projection:
        projection.prepare_sources([{"path": str(path), "sha256": sha256_file(path)}])
        scan, expression = projection.input_scan(path, lower=days[-2], upper=days[-1])
        actual = scan.collect()
        if clock == "unsupported_slash":
            assert expression is None and actual.height == 20
        else:
            assert_frame_equal(actual.with_columns(expression.alias("date")),
                pl.DataFrame({"date": days[-2:], "value": [-0.0, None]}), check_exact=True)
            work = projection.stats()["price_source_scans"][str(path)]
            assert work["decoded_groups"] == work["decoded_rows"] == 2
            assert work["total_groups"] == 20
