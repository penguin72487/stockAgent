from datetime import date
import hashlib
import json

import polars as pl
from polars.testing import assert_frame_equal
import pytest

import stockagent.data.tw_public_features as feature_module
import stockagent.data.tw_public_incremental as incremental_module
from stockagent.data.tw_public_features import build_tw_public_training_features


DAYS = ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"]


def _unchanged_file_evidence(path):
    info = path.stat()
    return ((info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns),
            hashlib.sha256(path.read_bytes()).hexdigest())


def write_daily(root, values=("100", "101", "102", "103")):
    pl.DataFrame({"date": DAYS, "證券代號": ["2330"] * 4,
        "收盤價": values, "最高價": values, "最低價": values,
        "成交股數": ["1000"] * 4, "成交金額": ["100000"] * 4,
        "成交筆數": ["10"] * 4}).write_parquet(root / "twse_daily_ohlcv.parquet")


def write_notice(path, rows):
    schema = {"Date": pl.String, "Code": pl.String, "ClosingPrice": pl.String,
              "PE": pl.String, "_downloaded_at_utc": pl.String, "_as_of_date": pl.String,
              "date": pl.String, "irrelevant_source_metadata": pl.String}
    pl.DataFrame(rows, schema=schema).write_parquet(path)


def row(day, observed, *, code="2330", close="100", marker="first"):
    return {"Date": day, "Code": code, "ClosingPrice": close, "PE": "15",
            "_downloaded_at_utc": observed, "_as_of_date": day, "date": day,
            "irrelevant_source_metadata": marker}


@pytest.mark.parametrize("name,symbol_col,close_col,pe_col,attention", [
    ("tpex_attention_stock", "SecuritiesCompanyCode", "ClosePrice", "PriceEarningRatio", True),
    ("twse_disposal_stock", "Code", "ClosingPrice", "PE", False),
    ("tpex_disposal_stock", "SecuritiesCompanyCode", "ClosePrice", "PriceEarningRatio", False),
])
@pytest.mark.parametrize("change", [
    "metadata_only", "nonconsumer_append", "future_after_close", "remove",
    "duplicate", "clock_revision", "price_revision",
])
def test_other_attention_disposal_projections_match_full_producer(
        tmp_path, name, symbol_col, close_col, pe_col, attention, change):
    source = tmp_path / "source"
    source.mkdir()
    write_daily(source)
    path = source / f"{name}.parquet"
    schema = {"Date": pl.String, symbol_col: pl.String, close_col: pl.String,
              pe_col: pl.String, "_downloaded_at_utc": pl.String,
              "_as_of_date": pl.String, "date": pl.String, "metadata": pl.String}
    original = {"Date": "2024-01-02", symbol_col: "2330", close_col: "100", pe_col: "15",
                "_downloaded_at_utc": "2024-01-02T00:30:00Z", "_as_of_date": "2024-01-02",
                "date": "2024-01-02", "metadata": "first"}

    def write(rows):
        pl.DataFrame(rows, schema=schema).write_parquet(path)

    write([original])
    output = tmp_path / "result.parquet"
    common = dict(input_dir=source, symbols_root=None, allow_daily_publication_lag=True,
                  end_date=date(2024, 1, 5))
    first = build_tw_public_training_features(**common, output_path=output, incremental_auto=True)
    before = _unchanged_file_evidence(output)
    if change == "metadata_only":
        rows = [{**original, "metadata": "changed"}]
    elif change == "nonconsumer_append":
        rows = [original, {**original, symbol_col: ""}]
    elif change == "future_after_close":
        rows = [original, {**original, "Date": "2024-01-05",
                            "_downloaded_at_utc": "2024-01-05T01:30:00Z"}]
    elif change == "remove":
        rows = []
    elif change == "duplicate":
        rows = [original, original]
    elif change == "clock_revision":
        rows = [{**original, "_downloaded_at_utc": "2024-01-02T01:30:00Z"}]
    else:
        rows = [{**original, close_col: "130"}]
    write(rows)
    actual = build_tw_public_training_features(**common, output_path=output, incremental_auto=True)
    unchanged = change in {"metadata_only", "nonconsumer_append", "future_after_close"} or (
        change == "price_revision" and not attention)
    if unchanged:
        assert actual.build_mode == "unchanged_verified"
        assert actual.reused_rows == first.rows
        assert _unchanged_file_evidence(output) == before
    else:
        assert actual.build_mode == "incremental_tail"
        assert actual.incremental_start_date == "2024-01-02"
    assert path.name in actual.incremental_plan["changed_sources"]
    reference = tmp_path / "reference.parquet"
    build_tw_public_training_features(**common, output_path=reference)
    assert_frame_equal(pl.read_parquet(output), pl.read_parquet(reference),
                       check_row_order=True, check_column_order=True)


@pytest.mark.parametrize("change, expected_mode, expected_start", [
    ("nonconsumer_append", "unchanged_verified", None),
    ("metadata_only", "unchanged_verified", None),
    ("future_after_close", "unchanged_verified", None),
    ("add_before_open", "incremental_tail", "2024-01-03"),
    ("add_after_open", "incremental_tail", "2024-01-04"),
    ("old_revision", "incremental_tail", "2024-01-02"),
    ("remove", "incremental_tail", "2024-01-02"),
    ("clock_revision", "incremental_tail", "2024-01-02"),
    ("duplicate", "incremental_tail", "2024-01-02"),
    ("mixed_daily", "incremental_tail", "2024-01-03"),
    ("proof_damaged", "full", None),
    ("output_damaged", "full", None),
    ("unknown_changed", "full", None),
])
def test_notice_projection_matches_full_producer(tmp_path, change, expected_mode, expected_start):
    source = tmp_path / "source"
    source.mkdir()
    write_daily(source)
    notice = source / "twse_notice_stock.parquet"
    original = row("2024-01-02", "2024-01-02T00:30:00Z")
    write_notice(notice, [original])
    unknown = source / "unverified_dependency.parquet"
    pl.DataFrame({"value": [1]}).write_parquet(unknown)
    output = tmp_path / "result.parquet"
    common = dict(input_dir=source, symbols_root=None, allow_daily_publication_lag=True,
                  end_date=date(2024, 1, 5))
    first = build_tw_public_training_features(**common, output_path=output, incremental_auto=True)
    before = _unchanged_file_evidence(output)
    rows = [original]
    if change == "nonconsumer_append":
        rows += [row("2024-01-04", "2024-01-04T00:30:00Z", code="")]
    elif change == "metadata_only":
        rows = [{**original, "irrelevant_source_metadata": "updated"}]
    elif change == "future_after_close":
        rows += [row("2024-01-05", "2024-01-05T01:30:00Z")]
    elif change in {"add_before_open", "mixed_daily"}:
        rows += [row("2024-01-03", "2024-01-03T00:30:00Z")]
    elif change == "add_after_open":
        rows += [row("2024-01-03", "2024-01-03T01:30:00Z")]
    elif change == "old_revision":
        rows = [{**original, "ClosingPrice": "130"}]
    elif change == "remove":
        rows = []
    elif change == "clock_revision":
        rows = [{**original, "_downloaded_at_utc": "2024-01-02T01:30:00Z"}]
    elif change == "duplicate":
        rows *= 2
    else:
        rows += [row("2024-01-03", "2024-01-03T00:30:00Z")]
    write_notice(notice, rows)
    if change == "mixed_daily":
        write_daily(source, ("100", "111", "102", "103"))
    if change == "unknown_changed":
        pl.DataFrame({"value": [2]}).write_parquet(unknown)
    if change == "proof_damaged":
        summary_path = output.with_suffix(".summary.json")
        summary = json.loads(summary_path.read_text())
        summary["input_partitions"]["sources"][notice.name]["schema_sha256"] = "invalid"
        summary_path.write_text(json.dumps(summary))
    if change == "output_damaged":
        pl.read_parquet(output).with_columns(pl.lit(123.0).alias("twpub_attention_count_log")).write_parquet(output)

    actual = build_tw_public_training_features(**common, output_path=output, incremental_auto=True)
    assert actual.build_mode == expected_mode
    assert actual.incremental_start_date == expected_start
    if expected_mode == "unchanged_verified":
        assert _unchanged_file_evidence(output) == before
        assert actual.reused_rows == first.rows
        assert notice.name in actual.incremental_plan["changed_sources"]
    reference = tmp_path / "reference.parquet"
    build_tw_public_training_features(**common, output_path=reference)
    assert_frame_equal(pl.read_parquet(output), pl.read_parquet(reference),
                       check_row_order=True, check_column_order=True)


def test_notice_changes_during_noop_cannot_publish(tmp_path, monkeypatch):
    source = tmp_path / "source"
    source.mkdir()
    write_daily(source)
    notice = source / "twse_notice_stock.parquet"
    original = row("2024-01-02", "2024-01-02T00:30:00Z")
    write_notice(notice, [original])
    output = tmp_path / "result.parquet"
    common = dict(input_dir=source, output_path=output, symbols_root=None,
                  allow_daily_publication_lag=True, end_date=date(2024, 1, 5), incremental_auto=True)
    build_tw_public_training_features(**common)
    summary = output.with_suffix(".summary.json")
    old_output, old_summary = _unchanged_file_evidence(output), summary.read_bytes()
    write_notice(notice, [{**original, "irrelevant_source_metadata": "new"}])
    original_receipts = feature_module._source_content_receipts
    calls = 0

    def changing_receipts(root):
        nonlocal calls
        calls += 1
        if calls == 2:
            write_notice(notice, [{**original, "ClosingPrice": "999"}])
        return original_receipts(root)

    monkeypatch.setattr(feature_module, "_source_content_receipts", changing_receipts)
    with pytest.raises(RuntimeError, match="source parquet changed"):
        build_tw_public_training_features(**common)
    assert _unchanged_file_evidence(output) == old_output
    assert summary.read_bytes() == old_summary


def test_notice_file_replacement_during_projection_is_rejected(tmp_path, monkeypatch):
    path = tmp_path / "twse_notice_stock.parquet"
    original = row("2024-01-02", "2024-01-02T00:30:00Z")
    write_notice(path, [original])
    read_parquet = incremental_module.pl.read_parquet

    def changing_read(path_arg, *args, **kwargs):
        result = read_parquet(path_arg, *args, **kwargs)
        write_notice(path, [{**original, "ClosingPrice": "999"}])
        return result

    monkeypatch.setattr(incremental_module.pl, "read_parquet", changing_read)
    with pytest.raises(OSError, match="changed during semantic partition proof"):
        incremental_module._notice_feature_session_fingerprints(path)


def test_notice_future_row_is_admitted_when_completed_calendar_advances(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    write_daily(source)
    notice = source / "twse_notice_stock.parquet"
    first = row("2024-01-02", "2024-01-02T00:30:00Z")
    future = row("2024-01-05", "2024-01-05T01:30:00Z")
    write_notice(notice, [first])
    target = tmp_path / "result.parquet"
    common = dict(input_dir=source, output_path=target, symbols_root=None,
                  allow_daily_publication_lag=True, incremental_auto=True)
    build_tw_public_training_features(**common, end_date=date(2024, 1, 5))
    write_notice(notice, [first, future])
    hidden = build_tw_public_training_features(**common, end_date=date(2024, 1, 5))
    assert hidden.build_mode == "unchanged_verified"

    days = [*DAYS, "2024-01-08"]
    prices = ["100", "101", "102", "103", "104"]
    pl.DataFrame({"date": days, "證券代號": ["2330"] * 5,
        "收盤價": prices, "最高價": prices, "最低價": prices,
        "成交股數": ["1000"] * 5, "成交金額": ["100000"] * 5,
        "成交筆數": ["10"] * 5}).write_parquet(source / "twse_daily_ohlcv.parquet")
    actual = build_tw_public_training_features(**common, end_date=date(2024, 1, 8))
    assert actual.build_mode == "incremental_tail"
    assert actual.incremental_start_date == "2024-01-06"
    result = pl.read_parquet(target)
    assert result.filter((pl.col("date") == date(2024, 1, 8))
                         & (pl.col("symbol") == "2330"))["twpub_attention_count_log"].item() == pytest.approx(0.6931471805599453)
    reference = tmp_path / "reference.parquet"
    build_tw_public_training_features(input_dir=source, output_path=reference,
        symbols_root=None, allow_daily_publication_lag=True, end_date=date(2024, 1, 8))
    assert_frame_equal(result, pl.read_parquet(reference), check_row_order=True, check_column_order=True)
