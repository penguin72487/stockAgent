import hashlib
import json
import os
from datetime import date

import polars as pl
import pytest

from stockagent.data.tw_public_features import (
    _build_day_trade_rule_features,
    _source_content_receipts,
    build_tw_public_training_features,
)


def _inputs(root, *, actual_extra_session=False):
    before, closed, after = date(2026, 10, 22), date(2026, 10, 23), date(2026, 10, 27)
    for venue, symbol, code in (("twse", "2330", "證券代號"), ("tpex", "6488", "代號")):
        pl.DataFrame({"date": [before, after], code: [symbol, symbol]}).write_parquet(
            root / f"{venue}_daily_ohlcv.parquet"
        )
        pl.DataFrame({
            "date": [before, closed, after], "證券代號": [symbol] * 3,
            "證券名稱": ["test"] * 3, "暫停現股賣出後現款買進當沖註記": [""] * 3,
        }).write_parquet(root / f"{venue}_day_trade_eligibility.parquet")
    sessions = [before, closed, after] if actual_extra_session else [before, after]
    path = root / "twse_taiex_ohlc.parquet"
    pl.DataFrame({
        "date": sessions, "opening_index": [100.0] * len(sessions),
        "highest_index": [102.0] * len(sessions), "lowest_index": [99.0] * len(sessions),
        "closing_index": [101.0] * len(sessions),
    }).write_parquet(path)
    summary = {
        "dataset": "twse_taiex_ohlc", "source": "TWSE",
        "source_product": "indicesReport/MI_5MINS_HIST", "coverage_complete": True,
        "replacement_promoted": True, "failed_count": 0, "unresolved_month_count": 0,
        "effective_start_date": before.isoformat(), "effective_end_date": after.isoformat(),
        "output_rows": len(sessions),
        "output_receipt": {"size": path.stat().st_size,
                           "sha256": hashlib.sha256(path.read_bytes()).hexdigest()},
    }
    path.with_suffix(".summary.json").write_text(json.dumps(summary), encoding="utf-8")
    return closed, path, summary


def test_verified_closure_does_not_require_a_fictitious_price_and_preserves_raw(tmp_path):
    closed, path, summary = _inputs(tmp_path)
    result = _build_day_trade_rule_features(tmp_path)
    assert closed not in result["date"].to_list()
    assert result.height == 4
    for venue in ("twse", "tpex"):
        assert closed in pl.read_parquet(tmp_path / f"{venue}_day_trade_eligibility.parquet")["date"].to_list()

    summary["output_receipt"]["sha256"] = "unverified"
    path.with_suffix(".summary.json").write_text(json.dumps(summary), encoding="utf-8")
    with pytest.raises(ValueError, match="coverage does not match"):
        _build_day_trade_rule_features(tmp_path)


def test_actual_trading_day_with_missing_prices_remains_an_error(tmp_path):
    _inputs(tmp_path, actual_extra_session=True)
    with pytest.raises(ValueError, match="coverage does not match"):
        _build_day_trade_rule_features(tmp_path)


@pytest.mark.parametrize("field", [
    "dataset", "source", "source_product", "coverage_complete", "replacement_promoted",
    "failed_count", "unresolved_month_count", "effective_start_date", "effective_end_date",
    "output_rows", "output_receipt.size", "output_receipt.sha256",
])
def test_calendar_source_identity_binds_every_verifier_input(tmp_path, field):
    _, path, summary = _inputs(tmp_path)
    before = _source_content_receipts(tmp_path)
    assert before[-1]["basis"] == "actual_session_receipt_semantics_v1"
    owner = summary
    if "." in field:
        parent, field = field.split(".")
        owner = summary[parent]
    value = owner[field]
    owner[field] = not value if isinstance(value, bool) else value+1 if isinstance(value, int) else value+"-changed"
    path.with_suffix(".summary.json").write_text(json.dumps(summary), encoding="utf-8")
    assert _source_content_receipts(tmp_path) != before


def test_calendar_cosmetic_metadata_does_not_invalidate_feature_sources(tmp_path):
    _, path, summary = _inputs(tmp_path)
    before = _source_content_receipts(tmp_path)
    summary.update(completed_at="new-capture-clock", elapsed_seconds=9.0, output_path="new-report-path")
    summary["output_receipt"]["path"] = "another-report-label"
    path.with_suffix(".summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    assert _source_content_receipts(tmp_path) == before


def test_calendar_receipt_rewrite_with_restored_mtime_invalidates_cached_closure(tmp_path):
    _, path, summary = _inputs(tmp_path)
    receipt = path.with_suffix(".summary.json")
    assert _build_day_trade_rule_features(tmp_path).height == 4
    before = receipt.stat()
    summary["failed_count"] = 1
    receipt.write_text(json.dumps(summary), encoding="utf-8")
    os.utime(receipt, ns=(before.st_atime_ns, before.st_mtime_ns))
    assert receipt.stat().st_size == before.st_size
    assert receipt.stat().st_mtime_ns == before.st_mtime_ns
    with pytest.raises(ValueError, match="coverage does not match"):
        _build_day_trade_rule_features(tmp_path)


@pytest.mark.parametrize("change", ["removed", "invalidated"])
def test_incremental_noop_cannot_reuse_a_lost_calendar_proof(tmp_path, change):
    _, path, summary = _inputs(tmp_path)
    for venue, columns in {
        "twse": {"收盤價":100.0,"最高價":101.0,"最低價":99.0,"成交股數":1000.0,"成交金額":100000.0,"成交筆數":10.0},
        "tpex": {"收盤":100.0,"最高":101.0,"最低":99.0,"成交股數":1000.0,"成交金額(元)":100000.0,"成交筆數":10.0,"發行股數":1000000.0,"次日漲停價":110.0,"次日跌停價":90.0},
    }.items():
        quotes = tmp_path/f"{venue}_daily_ohlcv.parquet"
        pl.read_parquet(quotes).with_columns([
            pl.lit(value).alias(name) for name,value in columns.items()
        ]).write_parquet(quotes)
    target = tmp_path/"features"/"daily.parquet"
    common = dict(input_dir=tmp_path, output_path=target, symbols_root=None,
                  end_date=date(2026,10,27), incremental_auto=True, allow_daily_publication_lag=True)
    build_tw_public_training_features(**common)
    assert build_tw_public_training_features(**common).build_mode == "unchanged_verified"
    before = hashlib.sha256(target.read_bytes()).hexdigest()
    if change == "removed":
        path.with_suffix(".summary.json").unlink()
    else:
        summary["coverage_complete"] = False
        path.with_suffix(".summary.json").write_text(json.dumps(summary), encoding="utf-8")
    with pytest.raises(ValueError, match="coverage does not match"):
        build_tw_public_training_features(**common)
    assert hashlib.sha256(target.read_bytes()).hexdigest() == before
