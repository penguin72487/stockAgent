from datetime import date, datetime
import csv
import json
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from stockagent.live.tw_day_trade_completed_replay import verify_completed_open_sources


DAY = date(2026, 11, 9)
TAIPEI = ZoneInfo("Asia/Taipei")


def _sources(tmp_path):
    raw = tmp_path / "raw"
    for exchange, symbol, price in (("twse", "2330", "2,210.00"), ("tpex", "6488", "535.00")):
        folder = raw / f"{exchange}_daily_ohlcv"
        folder.mkdir(parents=True)
        (folder / f"{DAY}.json").write_text(json.dumps({"date": "20261109", "stat": "OK",
            "tables": [{"fields": ["證券代號", "開盤價", "成交股數"],
                        "data": [[symbol, price, "1,000"], [f"NONE-{exchange}", "----", "0"]]}]}))
    prices = tmp_path / "input.csv"
    with prices.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["symbol", "price", "open_price"])
        writer.writeheader()
        writer.writerows([{"symbol": "2330", "price": 2210, "open_price": 2210},
                          {"symbol": "6488", "price": 535, "open_price": 535}])
    return raw, prices


def _verify(raw, prices, *, hour=14, minute=31, **kwargs):
    return verify_completed_open_sources(session=DAY,
        observed_at=datetime(2026, 11, 9, hour, minute, tzinfo=TAIPEI),
        prices_csv=prices, raw_root=raw, **kwargs)


def test_completed_open_proof_checks_both_sources_and_only_observed_prices(tmp_path):
    raw, prices = _sources(tmp_path)
    proof = _verify(raw, prices)
    assert proof["traded_open_count"] == 2
    assert proof["simulation_only"] and not proof["broker_fill"] and not proof["live_publication"]
    assert _verify(raw, prices, expected=proof) == proof


def test_completed_open_proof_rejects_incomplete_or_changed_sources(tmp_path):
    raw, prices = _sources(tmp_path)
    proof = _verify(raw, prices)
    with pytest.raises(ValueError, match="maintenance window"):
        _verify(raw, prices, hour=14, minute=30)
    original = prices.read_text()
    prices.write_text(original.replace("2210", "2211"))
    with pytest.raises(ValueError, match="differs"):
        _verify(raw, prices)
    prices.write_text(original)
    with pytest.raises(ValueError, match="proof changed"):
        _verify(raw, prices, expected=proof | {"prices_sha256": "0" * 64})
    source = raw / "twse_daily_ohlcv" / f"{DAY}.json"
    payload = json.loads(source.read_text())
    payload["tables"][0]["data"][0][2] = "0"
    source.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="differs"):
        _verify(raw, prices)


def test_completed_open_proof_rejects_partial_or_duplicate_csv(tmp_path):
    raw, prices = _sources(tmp_path)
    original = prices.read_text()
    prices.write_text("symbol,price,open_price\n2330,2210,2210\n")
    with pytest.raises(ValueError, match="omits"):
        _verify(raw, prices)
    prices.write_text(original + "2330,2210,2210\n")
    with pytest.raises(ValueError, match="differs"):
        _verify(raw, prices)


def test_pinned_official_versions_survive_latest_pointer_update(tmp_path):
    raw, prices = _sources(tmp_path)
    pinned = {}
    for exchange in ("twse", "tpex"):
        original = raw / f"{exchange}_daily_ohlcv" / f"{DAY}.json"
        target = raw / ".close_evidence" / exchange / f"{exchange}_daily_ohlcv" / f"{DAY}.json"
        target.parent.mkdir(parents=True)
        target.write_bytes(original.read_bytes())
        pinned[exchange] = target
    proof = _verify(raw, prices, source_paths=pinned)
    pointer = raw / "twse_daily_ohlcv" / f"{DAY}.json"
    pointer.write_text(pointer.read_text() + " ")
    assert _verify(raw, prices, expected=proof) == proof
    pinned["twse"].write_text(pinned["twse"].read_text() + " ")
    with pytest.raises(ValueError, match="proof changed"):
        _verify(raw, prices, expected=proof)


def test_completed_open_requires_aware_same_session_clock(tmp_path):
    raw, prices = _sources(tmp_path)
    with pytest.raises(ValueError, match="timezone-aware"):
        verify_completed_open_sources(session=DAY, observed_at=datetime(2026, 11, 9, 15),
                                      prices_csv=prices, raw_root=raw)
    with pytest.raises(ValueError, match="maintenance window"):
        verify_completed_open_sources(session=DAY, observed_at=datetime(2026, 11, 10, 15, tzinfo=TAIPEI),
                                      prices_csv=prices, raw_root=raw)


def test_signal_generator_rechecks_same_day_proof_before_inference(tmp_path, monkeypatch):
    from stockagent.live import signal_engine as signals

    raw, prices = _sources(tmp_path)
    proof = _verify(raw, prices)

    class FixedClock(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 9, 15, tzinfo=TAIPEI).astimezone(tz) if tz else cls(2026, 11, 9, 15)

    monkeypatch.setattr(signals, "datetime", FixedClock)
    monkeypatch.setattr(signals, "loaded_implementation_identity", lambda: None)
    monkeypatch.setattr(signals, "require_live_feature_source", lambda *_: None)
    config = SimpleNamespace(data=SimpleNamespace(parquet_root=str(tmp_path / "stocks")),
        trading=SimpleNamespace(execution_mode="tw_day_trade"),
        training=SimpleNamespace(strict_no_fallback=True))
    monkeypatch.setattr(signals, "load_config", lambda *_args, **_kwargs: config)

    def reached_inference(_config):
        raise RuntimeError("source admission passed")

    monkeypatch.setattr(signals, "configure_inference_runtime", reached_inference)
    kwargs = {"market": "test", "price_source": "csv", "prices_csv": prices,
        "live_output_dir": tmp_path / "private", "publish_latest": False,
        "signal_id": "test-counterfactual-official-open-2026-11-09",
        "asof_date": "2026-11-09T09:00:00+08:00",
        "_counterfactual_official_open_session": str(DAY),
        "day_trade_opening_reference_policy": signals.OPENING_PROVISIONAL_REFERENCE_POLICY}
    with pytest.raises(ValueError, match="isolated past-session"):
        signals.generate_live_signal(**kwargs)
    with pytest.raises(RuntimeError, match="source admission passed"):
        signals.generate_live_signal(**kwargs, _counterfactual_completed_source_evidence=proof)
    with pytest.raises(ValueError, match="proof changed"):
        signals.generate_live_signal(**kwargs,
            _counterfactual_completed_source_evidence=proof | {"prices_sha256": "0" * 64})
