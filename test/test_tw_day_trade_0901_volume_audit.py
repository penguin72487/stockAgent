from __future__ import annotations

import json
from pathlib import Path

from scripts.audit_tw_day_trade_0901_volume_units import TICK_SOURCE, audit


def test_audit_uses_receipt_lots_and_distinguishes_real_zero_capacity(tmp_path: Path) -> None:
    receipt_dir = tmp_path / "receipts"
    receipt_dir.mkdir()
    day = "2026-09-10"
    (receipt_dir / f"{day}.json").write_text(
        json.dumps({
            "schema_version": 2,
            "session_date": day,
            "simulation_only": True,
            "production_order_possible": False,
            "execution_price_contract": (
                "source_backed_right_labelled_09_01_minute_price_vwap_else_kbar_close"
            ),
            "prices": {
                "2330": {"symbol": "2330", "source": TICK_SOURCE, "tick_volume_units_0901": 4.0},
                "1234": {"symbol": "1234", "source": TICK_SOURCE, "tick_volume_units_0901": 1.0},
            },
        }),
        encoding="utf-8",
    )
    ledger = tmp_path / "signals.jsonl"
    ledger.write_text(
        "\n".join(json.dumps({
            "session_date": day,
            "market": "tw_day_trade_100m",
            "symbol": symbol,
            "quote_source": TICK_SOURCE,
            "minute_kbar_volume_lots": raw_lots / 1_000,
            "minute_kbar_capacity_shares": 0,
            "minute_volume_participation": 0.5,
            "requested_shares": 1_000,
            "filled_shares": 0,
        }) for symbol, raw_lots in (("2330", 4.0), ("1234", 1.0))) + "\n",
        encoding="utf-8",
    )

    report = audit(ledger, receipt_dir)
    counts = report["by_date_mode"][f"{day}|tw_day_trade_100m"]

    assert counts["requested_rows"] == 2
    assert counts["false_zero_capacity_rows"] == 1
    assert counts["corrected_capacity_full_rows"] == 1
    assert counts["still_zero_capacity_rows"] == 1
    assert counts["legacy_underdivided_rows"] == 2
    assert report["source_errors"] == {}
    assert len(report["legacy_capacity_corrections"]) == 1
    assert report["legacy_capacity_corrections"][0]["source_backed_capacity_shares"] == 2_000
    assert report["first_session"] == day
    assert report["last_session"] == day
    assert report["source_coverage_by_date"][f"{day}|{TICK_SOURCE}"]["requested_rows"] == 2
    assert report["execution_policy_by_date"][f"{day}|unknown"]["requested_rows"] == 2


def test_audit_covers_non_tick_history_without_calling_it_tick_corruption(tmp_path: Path) -> None:
    ledger = tmp_path / "signals.jsonl"
    ledger.write_text(
        json.dumps({
            "session_date": "2026-02-25",
            "market": "tw_day_trade_100m",
            "symbol": "2330",
            "quote_source": "local_minute_parquet_0901_vwap",
            "requested_shares": 1_000,
            "filled_shares": 1_000,
        }) + "\n",
        encoding="utf-8",
    )
    report = audit(ledger, tmp_path)
    assert report["first_session"] == "2026-02-25"
    assert report["by_date_mode"] == {}
    assert report["unresolved_0901_price_requested_rows"] == 0
    assert report["source_coverage_by_date"]["2026-02-25|local_minute_parquet_0901_vwap"]["recorded_filled_shares"] == 1_000


def test_audit_accepts_already_correct_tick_lot_rows(tmp_path: Path) -> None:
    day = "2026-09-23"
    receipts = tmp_path / "receipts"
    receipts.mkdir()
    (receipts / f"{day}.json").write_text(json.dumps({
        "schema_version": 2,
        "session_date": day,
        "simulation_only": True,
        "production_order_possible": False,
        "execution_price_contract": "source_backed_right_labelled_09_01_minute_price_vwap_else_kbar_close",
        "prices": {"2330": {"symbol": "2330", "source": TICK_SOURCE, "tick_volume_units_0901": 4.0}},
    }), encoding="utf-8")
    ledger = tmp_path / "signals.jsonl"
    ledger.write_text(json.dumps({
        "session_date": day,
        "market": "tw_day_trade_100m",
        "symbol": "2330",
        "quote_source": TICK_SOURCE,
        "minute_kbar_volume_lots": 4.0,
        "minute_kbar_capacity_shares": 2_000,
        "minute_volume_participation": 0.5,
        "requested_shares": 1_000,
        "filled_shares": 1_000,
    }) + "\n", encoding="utf-8")
    report = audit(ledger, receipts)
    counts = report["by_date_mode"][f"{day}|tw_day_trade_100m"]
    assert counts["corrected_unit_rows"] == 1
    assert counts.get("understated_capacity_rows", 0) == 0
    assert report["source_errors"] == {}
    assert report["legacy_capacity_corrections"] == []


def test_audit_reports_missing_opening_price_separately(tmp_path: Path) -> None:
    ledger = tmp_path / "signals.jsonl"
    ledger.write_text(json.dumps({
        "session_date": "2026-02-25",
        "market": "tw_day_trade_100m",
        "symbol": "0056",
        "quote_source": "missing_observed_09_01_minute_price",
        "requested_shares": 1_000,
        "filled_shares": 0,
    }) + "\n", encoding="utf-8")
    report = audit(ledger, tmp_path)
    assert report["unresolved_0901_price_requested_rows"] == 1
    assert report["missing_0901_price_symbols_by_date"] == {"2026-02-25": ["0056"]}
    assert report["by_date_mode"] == {}
