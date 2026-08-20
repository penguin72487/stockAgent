from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import promote_tw_day_trade_replay as promotion


MARKETS = {"mode_a", "mode_b", "mode_c"}


def _candidate(tmp_path: Path, *, register_result: str = "registered") -> Path:
    root = tmp_path / "candidate"
    root.mkdir()
    modes = {
        market: {
            "positions": {"2330": {"signed_shares": 0}},
            "total_equity_twd": 10_000_000.0,
        }
        for market in MARKETS
    }
    (root / "state.json").write_text(
        json.dumps({"modes": modes}), encoding="utf-8"
    )
    (root / "rebuild_receipt.json").write_text(
        json.dumps(
            {
                "simulation_only": True,
                "production_order_possible": False,
                "sessions": [
                    {
                        "session_date": "2026-08-13",
                        "close": {"status": "settled_official_close"},
                        "modes": [
                            {
                                "market": market,
                                "register_result": register_result,
                                "after_close": {"open_position_rows": 0},
                            }
                            for market in sorted(MARKETS)
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return root


def test_validate_rebuild_accepts_exact_flat_mode_set(tmp_path: Path) -> None:
    result = promotion._validate_rebuild(
        _candidate(tmp_path), expected_markets=MARKETS
    )

    assert result["registrations"] == 3
    assert result["mode_set"] == sorted(MARKETS)
    assert result["final_open_positions"] == {
        market: 0 for market in sorted(MARKETS)
    }


def test_validate_rebuild_rejects_blocked_registration(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="register_result='blocked'"):
        promotion._validate_rebuild(
            _candidate(tmp_path, register_result="blocked"),
            expected_markets=MARKETS,
        )
