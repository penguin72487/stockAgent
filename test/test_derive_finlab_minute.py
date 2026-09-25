from datetime import UTC, date, datetime
import json

import pandas as pd
import pytest

from scripts.derive_finlab_minute import (
    derive_partition, derived_receipt_path, stored_derived_receipt, ticks_to_minutes,
)
from scripts.download_finlab_intraday import fetch_partition


DAY = date(2026, 6, 1)


def ticks():
    return pd.DataFrame({
        "stock_id": ["2330"] * 6,
        "trade_date": [DAY.isoformat()] * 6,
        "timestamp": pd.to_datetime([
            "2026-06-01 09:00:01+08:00", "2026-06-01 09:00:01+08:00",
            "2026-06-01 09:00:59+08:00", "2026-06-01 13:30:00+08:00",
            "2026-06-01 14:30:00+08:00", "2026-06-01 09:01:01+08:00",
        ]),
        "sequence": [0, 1, 2, 3, 4, 5],
        "close": [100., 102., 101., 99., 98., 105.],
        "volume": [1, 2, 3, 4, 5, 0],
        "session": ["regular"] * 4 + ["after_hours_fixed", "regular"],
    })


def test_derived_bars_keep_sequence_auction_and_only_traded_minutes():
    bars = ticks_to_minutes(ticks(), "2330", DAY)
    assert bars["timestamp"].dt.strftime("%H:%M").tolist() == ["09:00", "13:30"]
    first = bars.iloc[0]
    assert (first["open"], first["high"], first["low"], first["close"]) == (100, 102, 100, 101)
    assert (first["volume"], first["tick_count"]) == (6, 3)
    assert first["vwap"] == pytest.approx((100 + 204 + 303) / 6)


def test_derivation_is_local_idempotent_and_provenance_pinned(tmp_path, monkeypatch):
    from finlab import data
    monkeypatch.setattr(data, "get", lambda *args, **kwargs: ticks())
    source = fetch_partition(tmp_path, "tw_tick:2330", DAY,
                             now=datetime(2026, 9, 25, tzinfo=UTC))
    receipt = derive_partition(tmp_path, "2330", DAY)
    assert receipt["source_tick_sha256"] == source["sha256"]
    assert receipt["source_kind"] == "derived_from_tw_tick"
    assert receipt["status"] == "derived_unverified_for_pit"
    assert receipt["rows"] == 2
    assert stored_derived_receipt(tmp_path, "2330", DAY, source["sha256"]) == receipt
    assert derive_partition(tmp_path, "2330", DAY) == receipt
    assert json.loads(derived_receipt_path(tmp_path, "2330", DAY).read_text()) == receipt


def test_invalid_regular_timestamp_fails_closed():
    frame = ticks()
    frame.loc[0, "timestamp"] = pd.Timestamp("2026-06-01 08:59:00+08:00")
    with pytest.raises(ValueError, match="outside 09:00"):
        ticks_to_minutes(frame, "2330", DAY)
