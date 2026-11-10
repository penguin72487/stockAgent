from datetime import date
from pathlib import Path

import pytest

from scripts import build_tw_day_trade_private_1030_nontrade_candidate as candidate


def _mode(*symbols):
    return {"market": candidate.MARKET, "positions": {
        str(i): {"symbol": symbol, "signed_shares": 1000} for i, symbol in enumerate(symbols)
    }}


def _item(day=candidate.DAY, *, bars=None, delayed=None):
    return {"day": day, "bars": bars or {}, "delayed_close_quotes": delayed or {}}


def _delegate(engine, mode, item, quotes):
    for position in mode["positions"].values():
        position["nontrade_settlement"] = {"reference": {
            "symbol": candidate.SYMBOL, "reference_observed_at": candidate.OBSERVED_AT,
            "reference_price": candidate.OBSERVED_PRICE, "source_sha256": candidate.TICK_SHA256},
            "simulated_order": False, "simulated_fill": False, "exchange_fill": False, "broker_fill": False}
    return {"symbols": [candidate.SYMBOL], "position_count": 1}


def test_exact_scoped_settlement_is_allowed():
    result = candidate._guarded_nontrade(_delegate, None, _mode("2235"), _item(), {})
    assert result["symbols"] == ["2235"]


@pytest.mark.parametrize("mode,item", [
    (_mode("1234"), _item()),
    (_mode("2235", "1234"), _item()),
    (_mode("2235"), _item(day=date(2026, 10, 29))),
    ({**_mode("2235"), "market": "tw_day_trade_multi_basis"}, _item()),
    (_mode("2235"), _item(bars={"2235": {"2026-10-30T13:30+08:00": {}}})),
    (_mode("2235"), _item(delayed={"2235": {}})),
])
def test_out_of_scope_residual_never_calls_owner(mode, item):
    def forbidden(*args):
        pytest.fail("out-of-scope settlement reached owner")

    with pytest.raises(RuntimeError, match="authorized only"):
        candidate._guarded_nontrade(forbidden, None, mode, item, {})


@pytest.mark.parametrize("key,value", [
    ("reference_observed_at", "2026-10-30T13:30:00+08:00"),
    ("reference_price", 31.0),
    ("source_sha256", "0" * 64),
])
def test_changed_observation_rejects_candidate(key, value):
    def changed(engine, mode, item, quotes):
        result = _delegate(engine, mode, item, quotes)
        mode["positions"]["0"]["nontrade_settlement"]["reference"][key] = value
        return result

    with pytest.raises(RuntimeError, match="exact approved source"):
        candidate._guarded_nontrade(changed, None, _mode("2235"), _item(), {})


def test_nontrade_cannot_be_reported_as_a_fill():
    def changed(engine, mode, item, quotes):
        result = _delegate(engine, mode, item, quotes)
        mode["positions"]["0"]["nontrade_settlement"]["simulated_fill"] = True
        return result

    with pytest.raises(RuntimeError, match="exact approved source"):
        candidate._guarded_nontrade(changed, None, _mode("2235"), _item(), {})


def test_torn_private_journal_fails_closed(tmp_path: Path):
    path = tmp_path / "fills.jsonl"
    path.write_bytes(b'{"symbol":"2235"}')
    with pytest.raises(RuntimeError, match="torn private journal"):
        candidate._rows(path)
