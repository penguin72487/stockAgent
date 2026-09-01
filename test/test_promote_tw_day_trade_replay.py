from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from scripts import promote_tw_day_trade_replay as promotion


MARKETS = {"mode_a", "mode_b", "mode_c"}


def _candidate(tmp_path: Path, *, register_result: str = "registered") -> Path:
    root = tmp_path / "candidate"
    root.mkdir()
    modes = {
        market: {
            "entry_fill_policy": "causal_best_quote",
            "entry_fill_is_synthetic": False,
            "positions": {"2330": {"signed_shares": 0}},
            "total_equity_twd": 10_000_000.0,
        }
        for market in MARKETS
    }
    (root / "state.json").write_text(json.dumps({"modes": modes}), encoding="utf-8")
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
    base = datetime.fromisoformat("2026-08-13T09:01:00+08:00")
    minute_rows = [
        {
            "session_date": "2026-08-13",
            "market": market,
            "minute": (base + timedelta(minutes=offset)).isoformat(
                timespec="minutes"
            ),
        }
        for market in sorted(MARKETS)
        for offset in range(promotion.MINUTE_CURVE_SESSION_POINTS)
    ]
    marks_path = root / "marks.jsonl"
    marks_path.write_text(
        "".join(json.dumps(row) + "\n" for row in minute_rows),
        encoding="utf-8",
    )
    benchmark_path = root / "benchmark_history.json"
    benchmark_path.write_text(json.dumps({"marks": []}), encoding="utf-8")
    (root / "minute_curve_receipt.json").write_text(
        json.dumps(
            {
                "simulation_only": True,
                "production_order_possible": False,
                "start_date": "2026-08-13",
                "end_date": "2026-08-13",
                "minute_contract": promotion.MINUTE_CURVE_CONTRACT,
                "linear_interpolation_used": False,
                "accepted_09_01_strategy_and_13_30_endpoints_preserved": True,
                "coverage_after_fetch": {"missing_pairs": 0},
                "strategy": {
                    "session_dates": ["2026-08-13"],
                    "markets": sorted(MARKETS),
                    "generated_rows": len(minute_rows),
                },
                "outputs": {
                    "marks": {"sha256": promotion._sha256(marks_path)},
                    "benchmark_history": {
                        "sha256": promotion._sha256(benchmark_path)
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    return root


def test_validate_rebuild_accepts_exact_flat_mode_set(tmp_path: Path) -> None:
    result = promotion._validate_rebuild(_candidate(tmp_path), expected_markets=MARKETS)

    assert result["registrations"] == 3
    assert result["mode_set"] == sorted(MARKETS)
    assert result["final_open_positions"] == {market: 0 for market in sorted(MARKETS)}
    assert result["minute_curve_validation"]["validated_rows"] == 810


def test_validate_rebuild_rejects_stale_daily_only_curve(tmp_path: Path) -> None:
    candidate = _candidate(tmp_path)
    receipt_path = candidate / "minute_curve_receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["strategy"]["generated_rows"] = 3
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

    with pytest.raises(RuntimeError, match="270 points per completed session"):
        promotion._validate_rebuild(candidate, expected_markets=MARKETS)


def test_validate_rebuild_accepts_0901_official_open_contract(tmp_path: Path) -> None:
    candidate = _candidate(tmp_path)
    state_path = candidate / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    for mode in state["modes"].values():
        mode["entry_fill_policy"] = promotion.OFFICIAL_OPEN_ENTRY_POLICY
        mode["entry_fill_contract"] = promotion.OFFICIAL_OPEN_REPLAY_CONTRACT
    state_path.write_text(json.dumps(state), encoding="utf-8")

    receipt_path = candidate / "rebuild_receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["replay_contract"] = {"entry": promotion.OFFICIAL_OPEN_REPLAY_CONTRACT}
    for row in receipt["sessions"][0]["modes"]:
        row["entry"] = {
            "entry_fill_policy": promotion.OFFICIAL_OPEN_ENTRY_POLICY,
            "entry_fill_count": 1,
            "entry_official_open_fill_count": 1,
            "entry_fill_is_synthetic": False,
        }
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    (candidate / "signals.jsonl").write_text(
        "".join(
            json.dumps(
                {
                    "market": market,
                    "recorded_at": "2026-08-13T09:01:00+08:00",
                    "entry_fill_policy": promotion.OFFICIAL_OPEN_ENTRY_POLICY,
                    "entry_price_offset_ticks": 0,
                    "filled_shares": 1_000,
                    "execution_price": 100.0,
                    "sizing_open_price": 100.0,
                    "counterfactual_open_price_fill": True,
                    "synthetic_fill": False,
                    "synthetic_fallback_fill": False,
                    "paper_market_fill": False,
                }
            )
            + "\n"
            for market in sorted(MARKETS)
        ),
        encoding="utf-8",
    )
    (candidate / "fills.jsonl").write_text(
        "".join(
            json.dumps(
                {
                    "market": market,
                    "purpose": "entry",
                    "fill_at": "2026-08-13T09:01:00+08:00",
                    "fill_contract": promotion.OFFICIAL_OPEN_REPLAY_CONTRACT,
                    "entry_fill_policy": promotion.OFFICIAL_OPEN_ENTRY_POLICY,
                    "entry_price_offset_ticks": 0,
                    "price": 100.0,
                    "counterfactual_open_price_fill": True,
                    "synthetic_fill": False,
                    "synthetic_fallback_fill": False,
                    "paper_market_fill": False,
                }
            )
            + "\n"
            for market in sorted(MARKETS)
        ),
        encoding="utf-8",
    )

    result = promotion._validate_rebuild(candidate, expected_markets=MARKETS)

    assert result["official_open_fills"] == 3
    assert result["signal_ledger_validation"]["fill_ledger_official_open_fills"] == 3
    assert result["synthetic_fallback_fills"] == 0


def test_validate_rebuild_accepts_0900_open_0901_vwap_contract(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)
    state_path = candidate / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    for mode in state["modes"].values():
        mode["entry_fill_policy"] = promotion.MINUTE_VWAP_0901_ENTRY_POLICY
        mode["entry_fill_contract"] = promotion.MINUTE_VWAP_0901_REPLAY_CONTRACT
    state_path.write_text(json.dumps(state), encoding="utf-8")

    receipt_path = candidate / "rebuild_receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["replay_contract"] = {"entry": promotion.MINUTE_VWAP_0901_REPLAY_CONTRACT}
    for row in receipt["sessions"][0]["modes"]:
        row["entry"] = {
            "entry_fill_policy": promotion.MINUTE_VWAP_0901_ENTRY_POLICY,
            "entry_fill_count": 1,
            "entry_0901_vwap_fill_count": 1,
            "entry_official_open_fill_count": 0,
            "entry_fill_is_synthetic": False,
        }
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    common = {
        "entry_fill_policy": promotion.MINUTE_VWAP_0901_ENTRY_POLICY,
        "entry_price_offset_ticks": 0,
        "counterfactual_0901_price_fill": True,
        "counterfactual_open_price_fill": False,
        "synthetic_fill": False,
        "synthetic_fallback_fill": False,
        "paper_market_fill": False,
    }
    (candidate / "signals.jsonl").write_text(
        "".join(
            json.dumps(
                {
                    **common,
                    "market": market,
                    "recorded_at": "2026-08-13T09:01:00+08:00",
                    "filled_shares": 1_000,
                    "execution_price": 101.0,
                    "sizing_open_price": 100.0,
                    "entry_price_source": "fixture_0901_minute_vwap",
                }
            )
            + "\n"
            for market in sorted(MARKETS)
        ),
        encoding="utf-8",
    )
    (candidate / "fills.jsonl").write_text(
        "".join(
            json.dumps(
                {
                    **common,
                    "market": market,
                    "purpose": "entry",
                    "fill_at": "2026-08-13T09:01:00+08:00",
                    "fill_contract": promotion.MINUTE_VWAP_0901_REPLAY_CONTRACT,
                    "price": 101.0,
                }
            )
            + "\n"
            for market in sorted(MARKETS)
        ),
        encoding="utf-8",
    )

    result = promotion._validate_rebuild(candidate, expected_markets=MARKETS)

    assert result["minute_vwap_0901_fills"] == 3
    assert result["signal_ledger_validation"][
        "fill_ledger_minute_vwap_0901_fills"
    ] == 3


def test_validate_rebuild_rejects_blocked_registration(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="register_result='blocked'"):
        promotion._validate_rebuild(
            _candidate(tmp_path, register_result="blocked"),
            expected_markets=MARKETS,
        )


def test_validate_rebuild_rejects_legacy_synthetic_open_tick(tmp_path: Path) -> None:
    candidate = _candidate(tmp_path)
    state_path = candidate / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["modes"]["mode_a"]["entry_fill_policy"] = "synthetic_open_tick"
    state["modes"]["mode_a"]["entry_fill_is_synthetic"] = True
    state_path.write_text(json.dumps(state), encoding="utf-8")

    with pytest.raises(RuntimeError, match="cannot be promoted"):
        promotion._validate_rebuild(candidate, expected_markets=MARKETS)


def test_validate_rebuild_accepts_explicit_hybrid_fallback_contract(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)
    state_path = candidate / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    for mode in state["modes"].values():
        mode["entry_fill_policy"] = promotion.HYBRID_ENTRY_POLICY
        mode["entry_fill_is_synthetic"] = True
    state_path.write_text(json.dumps(state), encoding="utf-8")

    receipt_path = candidate / "rebuild_receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["replay_contract"] = {"entry": promotion.HYBRID_REPLAY_CONTRACT}
    for row in receipt["sessions"][0]["modes"]:
        row["entry"] = {
            "entry_fill_policy": promotion.HYBRID_ENTRY_POLICY,
            "entry_fill_count": 2,
            "entry_best_quote_fill_count": 1,
            "entry_synthetic_fallback_fill_count": 1,
            "entry_fill_is_synthetic": True,
        }
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    signal_rows = []
    for market in sorted(MARKETS):
        signal_rows.extend(
            [
                {
                    "market": market,
                    "session_date": "2026-08-13",
                    "entry_fill_policy": promotion.HYBRID_ENTRY_POLICY,
                    "filled_shares": 1_000,
                    "side": "long",
                    "execution_price": 101.0,
                    "ask": 101.0,
                    "top_book_capacity_shares": 2_000,
                    "status": "ready",
                    "synthetic_fill": False,
                    "synthetic_fallback_fill": False,
                    "historical_source_quote_at": "2026-08-13T09:00:07+08:00",
                },
                {
                    "market": market,
                    "session_date": "2026-08-13",
                    "entry_fill_policy": promotion.HYBRID_ENTRY_POLICY,
                    "filled_shares": 1_000,
                    "side": "long",
                    "execution_price": 100.5,
                    "sizing_open_price": 100.0,
                    "upper_limit": 110.0,
                    "lower_limit": 90.0,
                    "status": "forced_synthetic_fill",
                    "entry_price_offset_ticks": 1,
                    "entry_price_source": (
                        "official_daily_session_open:adverse_one_legal_tick_fallback"
                    ),
                    "synthetic_fill": True,
                    "synthetic_fallback_fill": True,
                },
            ]
        )
    (candidate / "signals.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in signal_rows),
        encoding="utf-8",
    )

    result = promotion._validate_rebuild(candidate, expected_markets=MARKETS)

    assert result["best_quote_fills"] == 3
    assert result["synthetic_fallback_fills"] == 3


def test_validate_rebuild_accepts_explicit_current_open_counterfactual(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)
    current_date = datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
    state_path = candidate / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    for mode in state["modes"].values():
        mode.update(
            {
                "session_date": current_date,
                "engine_status": "active",
                "counterfactual_open_replay": True,
                "entry_fill_contract": (
                    "retrospective_observed_best_quote_counterfactual"
                ),
                "entry_fill_is_synthetic": False,
                "positions": {
                    "2330": {
                        "signed_shares": 1000,
                        "entry_price": 1005.0,
                        "sizing_open_price": 1000.0,
                        "counterfactual_open_replay": True,
                        "entry_fill_is_synthetic": False,
                    }
                },
            }
        )
    state_path.write_text(json.dumps(state), encoding="utf-8")
    receipt_path = candidate / "rebuild_receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["sessions"][0]["session_date"] = current_date
    receipt["sessions"][0]["close"] = {
        "status": "current_session_left_open_for_live_service"
    }
    for row in receipt["sessions"][0]["modes"]:
        row.pop("after_close")
        row["entry"] = {"engine_status": "active", "open_position_rows": 1}
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

    result = promotion._validate_rebuild(
        candidate,
        expected_markets=MARKETS,
        allow_current_open_session=True,
    )

    assert result["current_open_session"] == current_date
    assert result["final_open_positions"] == {market: 1 for market in sorted(MARKETS)}


def test_validate_rebuild_accepts_current_paper_market_tick_fallback(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)
    current_date = datetime.now(ZoneInfo("Asia/Taipei")).date().isoformat()
    state_path = candidate / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    for mode in state["modes"].values():
        mode.update(
            {
                "session_date": current_date,
                "engine_status": "active",
                "counterfactual_open_replay": True,
                "entry_fill_policy": promotion.PAPER_MARKET_ENTRY_POLICY,
                "entry_fill_contract": promotion.PAPER_MARKET_REPLAY_CONTRACT,
                "entry_fill_is_synthetic": True,
                "positions": {
                    "2330": {
                        "signed_shares": 1_000,
                        "entry_price": 100.5,
                        "sizing_open_price": 100.0,
                        "counterfactual_open_replay": True,
                        "entry_fill_is_synthetic": True,
                    }
                },
            }
        )
    state_path.write_text(json.dumps(state), encoding="utf-8")

    receipt_path = candidate / "rebuild_receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["replay_contract"] = {"entry": promotion.PAPER_MARKET_REPLAY_CONTRACT}
    receipt["sessions"][0]["session_date"] = current_date
    receipt["sessions"][0]["close"] = {
        "status": "current_session_left_open_for_live_service"
    }
    for row in receipt["sessions"][0]["modes"]:
        row.pop("after_close")
        row["entry"] = {
            "engine_status": "active",
            "open_position_rows": 1,
            "entry_fill_policy": promotion.PAPER_MARKET_ENTRY_POLICY,
            "entry_fill_count": 1,
            "entry_best_quote_fill_count": 0,
            "entry_synthetic_fallback_fill_count": 1,
            "entry_fill_is_synthetic": True,
        }
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

    signal_rows = [
        {
            "market": market,
            "session_date": current_date,
            "entry_fill_policy": promotion.PAPER_MARKET_ENTRY_POLICY,
            "requested_shares": 1_000,
            "filled_shares": 1_000,
            "side": "long",
            "execution_price": 100.5,
            "sizing_open_price": 100.0,
            "upper_limit": 110.0,
            "lower_limit": 90.0,
            "status": "forced_synthetic_fill",
            "entry_price_offset_ticks": 1,
            "entry_price_source": (
                "official_daily_session_open:adverse_one_legal_tick_fallback"
            ),
            "synthetic_fill": True,
            "synthetic_fallback_fill": True,
            "paper_market_fill": True,
        }
        for market in sorted(MARKETS)
    ]
    (candidate / "signals.jsonl").write_text(
        "".join(json.dumps(row) + "\n" for row in signal_rows),
        encoding="utf-8",
    )

    result = promotion._validate_rebuild(
        candidate,
        expected_markets=MARKETS,
        allow_current_open_session=True,
    )

    assert result["synthetic_fallback_fills"] == 3
    assert result["final_open_positions"] == {market: 1 for market in sorted(MARKETS)}


def test_validate_rebuild_rejects_current_open_without_explicit_flag(
    tmp_path: Path,
) -> None:
    candidate = _candidate(tmp_path)
    receipt_path = candidate / "rebuild_receipt.json"
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["sessions"][0]["close"] = {
        "status": "current_session_left_open_for_live_service"
    }
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

    with pytest.raises(RuntimeError, match="not settled at official close"):
        promotion._validate_rebuild(candidate, expected_markets=MARKETS)
