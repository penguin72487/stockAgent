import json
from types import SimpleNamespace

import pytest

from scripts.promote_tw_day_trade_replay import TERMINAL_CLOSE_UNLIMITED_CONTRACT, _require_added_runtime_policy
from stockagent.live.market_status import short_file_fingerprint


@pytest.mark.parametrize("change", [None, "enabled", "day_trade_simulation_enabled", "day_trade_strict_intraday",
                                    "day_trade_residual_margin_conversion", "day_trade_terminal_liquidation_unlimited_capacity",
                                    "checkpoint_path"])
def test_addition_cutover_requires_loaded_paper_policy_and_checkpoint(monkeypatch, tmp_path, change):
    checkpoint = tmp_path / "checkpoint.pt"
    checkpoint.write_bytes(b"exact-checkpoint")
    config = dict(market="new", enabled=True, day_trade_simulation_enabled=True, day_trade_strict_intraday=True,
                  day_trade_residual_margin_conversion=False, day_trade_terminal_liquidation_unlimited_capacity=True,
                  checkpoint_path=str(checkpoint))
    mode = {"terminal_close_contract": TERMINAL_CLOSE_UNLIMITED_CONTRACT, "margin_carry_contract": None,
            "checkpoint_fingerprint": short_file_fingerprint(checkpoint)}
    (tmp_path / "state.json").write_text(json.dumps({"modes": {"new": mode}}))
    if change == "checkpoint_path":
        config[change] = None
    elif change:
        config[change] = not config[change]
    monkeypatch.setattr("stockagent.live.market_config.load_market_config", lambda _: SimpleNamespace(**config))
    if change:
        with pytest.raises(RuntimeError, match="independent runtime"):
            _require_added_runtime_policy(tmp_path, {"new"})
    else:
        _require_added_runtime_policy(tmp_path, {"new"})
