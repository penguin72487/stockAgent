import hashlib
import json
from datetime import date

import polars as pl
import pytest

from stockagent.data.tw_public_features import _build_day_trade_rule_features


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
