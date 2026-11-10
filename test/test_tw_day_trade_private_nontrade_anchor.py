from contextlib import contextmanager
from copy import deepcopy

import pytest

from scripts import tw_day_trade_private_nontrade_anchor as adapter


def _position():
    return {"market": adapter.authorized.MARKET, "session_date": str(adapter.authorized.DAY),
            "symbol": adapter.authorized.SYMBOL, "position_id": "p", "signed_shares": 0,
            "filled_shares": 2000, "realized_net_pnl_twd": 7.5,
            "nontrade_settlement": {"reference": {"reference_observed_at": adapter.authorized.OBSERVED_AT,
                "reference_price": adapter.authorized.OBSERVED_PRICE, "source_sha256": adapter.authorized.TICK_SHA256},
                "assumed_exit_fee_and_tax_twd": 3.0, "simulated_order": False, "simulated_fill": False,
                "exchange_fill": False, "broker_fill": False}}


def _check(position, fills, expected_position, expected_fills):
    return adapter._checked_position(position, fills, market=adapter.authorized.MARKET,
        day=str(adapter.authorized.DAY), expected_position=expected_position, expected_fills=expected_fills)


def test_nontrade_position_preserves_actual_fill_count_and_assumed_fee():
    position = _position()
    fills = [{"fee_and_tax_twd": 1.25}, {"fee_and_tax_twd": 2.5}]
    checked = _check(position, fills, deepcopy(position), deepcopy(fills))
    assert checked == {"shares": 2000, "fill_count": 2, "net_pnl_twd": 7.5, "fees_twd": 6.75}


@pytest.mark.parametrize("mutation", ["position", "fill", "source", "fake_fill", "symbol"])
def test_changed_or_misrepresented_nontrade_anchor_fails(mutation):
    expected_position = _position()
    expected_fills = [{"fee_and_tax_twd": 1.25}]
    position, fills = deepcopy(expected_position), deepcopy(expected_fills)
    if mutation == "position":
        position["realized_net_pnl_twd"] += 1
    elif mutation == "fill":
        fills[0]["fee_and_tax_twd"] += 1
    elif mutation == "source":
        position["nontrade_settlement"]["reference"]["reference_price"] = 31.0
        expected_position = deepcopy(position)
    elif mutation == "fake_fill":
        position["nontrade_settlement"]["simulated_fill"] = True
        expected_position = deepcopy(position)
    else:
        position["symbol"] = "1234"
        expected_position = deepcopy(position)
    with pytest.raises(RuntimeError, match="independently audited"):
        _check(position, fills, expected_position, expected_fills)


def test_anchor_receipt_is_not_published_if_final_recheck_fails(tmp_path, monkeypatch):
    candidate = tmp_path / "private_candidate_receipt.json"
    candidate.touch()
    output = tmp_path / "snapshot_anchor.json"

    @contextmanager
    def failed_recheck(_):
        yield {"unused": True}
        raise RuntimeError("source changed during operation")

    def fake_stage(**kwargs):
        adapter.chain._write_new_plan(output, {"source_pins": {}})
        return {"status": "private"}

    monkeypatch.setattr(adapter, "checked_nontrade_anchor", failed_recheck)
    monkeypatch.setattr(adapter.chain, "stage", fake_stage)
    with pytest.raises(RuntimeError, match="source changed"):
        adapter.stage(candidate_receipt=candidate, output_root=tmp_path)
    assert not output.exists()
