import hashlib
import json

import polars as pl
import pytest

from scripts import audit_tw_day_trade_prior_price_reference as reference


@pytest.fixture
def sources(tmp_path, monkeypatch):
    inventory = tmp_path / "inventory.json"
    inventory.write_text(json.dumps({"session_date": "2026-10-30", "ledger_modified": False,
                                     "official_open_unavailable_symbols": ["5906"],
                                     "official_no_trade_symbols": []}))
    twse, tpex = tmp_path / "twse.parquet", tmp_path / "tpex.parquet"
    twse.write_bytes(b"twse")
    tpex.write_bytes(b"tpex")
    raw = tmp_path / "official.json"
    raw.write_text("{}")
    record = {"day": "2026-10-29", "venue": "twse", "dataset": "twse_daily_ohlcv",
              "path": str(raw), "sha256": hashlib.sha256(raw.read_bytes()).hexdigest(),
              "size": raw.stat().st_size, "response_kind": "json"}
    prior = {"5906": {"price": 38.0, "session_date": "2026-10-29", "source": "twse_daily_ohlcv"}}
    monkeypatch.setattr(reference, "DEFAULT_TWSE_DAILY_OHLCV_PATH", twse)
    monkeypatch.setattr(reference, "DEFAULT_TPEX_DAILY_OHLCV_PATH", tpex)
    monkeypatch.setattr(reference, "_previous_official_last_trades", lambda **kwargs: prior)
    monkeypatch.setattr(reference, "pin_official_market_inputs", lambda *args: [record])
    monkeypatch.setattr(reference, "read_official_market_evidence", lambda *args, **kwargs: None)
    monkeypatch.setattr(reference, "_parse_historical_response_content", lambda *args: (
        pl.DataFrame({"證券代號": ["5906"], "收盤價": ["38.00"], "成交股數": ["1,000"]}), "json"))
    return inventory, prior


def test_prior_reference_is_never_a_fill_or_no_print_claim(sources, tmp_path):
    inventory, _ = sources
    result = reference.audit(inventory_path=inventory, public_root=tmp_path)
    row = result["references"][0]
    assert row["price"] == 38 and row["prior_volume_shares"] == 1000
    assert row["target_day_open_status"] == "opening_status_unresolved"
    assert row["reference_is_executable_quote"] is False and row["fill_authorized"] is False
    assert result["ledger_modified"] is False and result["publication_authorized"] is False


def test_prior_reference_rejects_target_day_or_later_trade(sources, tmp_path):
    inventory, prior = sources
    prior["5906"]["session_date"] = "2026-10-30"
    with pytest.raises(RuntimeError, match="prior dated official trade"):
        reference.audit(inventory_path=inventory, public_root=tmp_path)


def test_prior_reference_rejects_wrong_or_nonpositive_raw_price(sources, tmp_path, monkeypatch):
    inventory, _ = sources
    monkeypatch.setattr(reference, "_parse_historical_response_content", lambda *args: (
        pl.DataFrame({"證券代號": ["5906"], "收盤價": ["38.50"], "成交股數": ["1,000"]}), "json"))
    with pytest.raises(RuntimeError, match="disagree"):
        reference.audit(inventory_path=inventory, public_root=tmp_path)
    monkeypatch.setattr(reference, "_parse_historical_response_content", lambda *args: (
        pl.DataFrame({"證券代號": ["5906"], "收盤價": ["38.00"], "成交股數": ["0"]}), "json"))
    with pytest.raises(ValueError, match="positive official trade"):
        reference.audit(inventory_path=inventory, public_root=tmp_path)
