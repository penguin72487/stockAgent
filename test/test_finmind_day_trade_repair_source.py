from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import json

import polars as pl
import pytest

from scripts.prepare_finmind_tw_day_trade_repair_source import normalize_source, verify
from scripts import repair_tw_day_trade_flat_opening as repair


def _inputs():
    rows = [
        {"stock_id": "2330", "date": "2026-11-04", "minute": "09:00:00",
         "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 2},
        {"stock_id": "2330", "date": "2026-11-04", "minute": "09:02:00",
         "open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0, "volume": 1},
        {"stock_id": "2330", "date": "2026-11-04", "minute": "13:30:00",
         "open": 102.0, "high": 102.0, "low": 102.0, "close": 102.0, "volume": 3},
        {"stock_id": "6516", "date": "2026-11-04", "minute": "09:02:00",
         "open": 50.0, "high": 51.0, "low": 49.0, "close": 50.0, "volume": 1},
        {"stock_id": "6516", "date": "2026-11-04", "minute": "13:33:00",
         "open": 50.4, "high": 50.4, "low": 50.4, "close": 50.4, "volume": 1},
        {"stock_id": "2330", "date": "2026-11-04", "minute": "14:30:00",
         "open": 102.0, "high": 102.0, "low": 102.0, "close": 102.0, "volume": 1},
    ]
    venues = pl.DataFrame([
        {"symbol": "2330", "date": "2026-11-04", "venue": "twse", "source_ref": "a"},
        {"symbol": "6516", "date": "2026-11-04", "venue": "tpex", "source_ref": "b"},
    ])
    official = {"2330": {"open": 100.0, "close": 102.0, "volume_shares": 6100},
                "6516": {"open": 50.0, "close": 50.4, "volume_shares": 2000}}
    return rows, venues, official


def test_finmind_source_preserves_sparse_minutes_and_delayed_auction():
    rows, venues, official = _inputs()
    result, summary = normalize_source(pl.DataFrame(rows), day="2026-11-04",
        symbols={"2330", "6516"}, venues=venues, official=official)
    assert result.height == 5
    assert result.filter((pl.col("symbol") == "2330")
                         & (pl.col("ts") == datetime(2026, 11, 4, 9, 1)))["volume_shares"].item() == 2000
    assert result.filter(pl.col("ts") == datetime(2026, 11, 4, 9, 2)).is_empty()
    assert result.filter((pl.col("symbol") == "6516")
                         & (pl.col("ts") == datetime(2026, 11, 4, 13, 33)))["Close"].item() == 50.4
    assert result["Amount"].null_count() == 5
    assert summary["first_minute_missing_symbols"] == ["6516"]
    assert summary["delayed_1333_symbols"] == ["6516"]
    assert summary["missing_minutes_are_unknown_not_zero"] is True


def _candidate_scope_fixture(tmp_path):
    import hashlib

    market, day = "tw_day_trade_unit_mgda_2m_20261103", "2026-11-09"
    summary = tmp_path / "summary.json"
    summary.write_text(json.dumps({"market": market, "signal_id": "replay-1",
        "counterfactual_signal_regeneration": True, "checkpoint_fingerprint": "abc123"}))
    weights = tmp_path / "weights.parquet"
    pl.DataFrame({"symbol": ["3059", "2330"], "date": [day + "T09:00:00+08:00"] * 2,
                  "target_weight": [-0.2, 0.0]}).write_parquet(weights)
    opening = tmp_path / "open.csv"
    opening.write_text("symbol,open\n3059,34.75\n")
    digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    artifact = {"market": market, "session_date": day, "signal_id": "replay-1"}
    for key, path in (("summary", summary), ("weights", weights), ("open_input", opening)):
        artifact[key + "_path"] = str(path)
        artifact[key + "_sha256"] = digest(path)
    preflight = tmp_path / "preflight.json"
    preflight.write_text(json.dumps({"contract": "exact_selected_signal_preflight_v1",
        "status": "accepted_sources_only", "simulation_only": True, "production_order_possible": False,
        "execution_or_minute_coverage_accepted": False, "expected_signals": 1,
        "accepted_signals": 1, "failures": [], "artifacts": [artifact]}))
    return preflight, weights, market, day


def test_finmind_candidate_scope_uses_pinned_nonzero_replay_targets(tmp_path):
    from scripts.prepare_finmind_tw_day_trade_repair_source import _candidate_signal_scope

    preflight, _, market, day = _candidate_scope_fixture(tmp_path)
    symbols, pins = _candidate_signal_scope(day, preflight, market)
    assert symbols == {"3059"}
    assert len(pins) == 4
    assert pins[0]["path"] == str(preflight)


@pytest.mark.parametrize("defect", ["tampered_weights", "wrong_market", "wrong_day", "duplicate_artifact"])
def test_finmind_candidate_scope_rejects_ambiguous_or_changed_signal(tmp_path, defect):
    from scripts.prepare_finmind_tw_day_trade_repair_source import _candidate_signal_scope

    preflight, weights, market, day = _candidate_scope_fixture(tmp_path)
    if defect == "tampered_weights":
        weights.write_bytes(b"changed")
    elif defect == "wrong_market":
        market = "another_market"
    elif defect == "wrong_day":
        day = "2026-11-06"
    else:
        receipt = json.loads(preflight.read_text())
        receipt["artifacts"].append(receipt["artifacts"][0])
        preflight.write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="candidate signal"):
        _candidate_signal_scope(day, preflight, market)


@pytest.mark.parametrize("failure", ["missing_symbol", "duplicate", "bad_price", "negative_volume",
    "missing_venue", "duplicate_venue", "unexplained_volume", "open_mismatch", "close_mismatch",
    "unexpected_auction_window"])
def test_finmind_source_rejects_ambiguous_or_unsupported_data(failure):
    rows, venues, official = _inputs()
    rows = deepcopy(rows)
    if failure == "missing_symbol": rows = [row for row in rows if row["stock_id"] != "6516"]
    elif failure == "duplicate": rows.append(dict(rows[0]))
    elif failure == "bad_price": rows[0]["high"] = 98.0
    elif failure == "negative_volume": rows[0]["volume"] = -1
    elif failure == "missing_venue": venues = venues.filter(pl.col("symbol") != "6516")
    elif failure == "duplicate_venue":
        venues = pl.concat([venues, pl.DataFrame([
            {"symbol": "2330", "date": "2026-11-04", "venue": "tpex", "source_ref": "c"}])])
    elif failure == "unexplained_volume": official["2330"]["volume_shares"] = 1000
    elif failure == "open_mismatch": official["2330"]["open"] = 98.0
    elif failure == "close_mismatch": official["6516"]["close"] = 50.3
    elif failure == "unexpected_auction_window": rows[1]["minute"] = "13:27:00"
    with pytest.raises((ValueError, pl.exceptions.PolarsError)):
        normalize_source(pl.DataFrame(rows), day="2026-11-04", symbols={"2330", "6516"},
                         venues=venues, official=official)


def test_finmind_source_verification_rejects_projection_outside_root(tmp_path):
    root = tmp_path / "source"
    partition = root / "trade_date=2026-11-04"
    partition.mkdir(parents=True)
    (partition / "data.parquet").write_bytes(b"placeholder")
    manifest = {"contract": "finmind_tw_day_trade_private_replay_source_v1",
        "session_date": "2026-11-04", "simulation_only": True,
        "production_order_possible": False, "public_export_authorized": False,
        "output": {"path": str(tmp_path / "other.parquet")}, "source_pins": []}
    (root / "finmind_replay_source.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="escaped its source root"):
        verify(root)


def test_retained_entry_audit_requires_verified_finmind_projection(tmp_path, monkeypatch):
    import hashlib
    from datetime import date
    from scripts import audit_tw_day_trade_margin_replay as audit

    root = tmp_path / "projection"
    minute = root / "trade_date=2026-11-04" / "data.parquet"
    minute.parent.mkdir(parents=True)
    minute.write_bytes(b"minute")
    manifest = root / "finmind_replay_source.json"
    manifest.write_text("{}")
    books = tmp_path / "state" / "replay_entry_books"
    books.mkdir(parents=True)
    book = books / "2026-11-04.parquet"
    pl.DataFrame({"symbol": ["2330"], "quote_at": ["2026-11-04T09:01:00+08:00"],
        "source": ["local_minute_parquet_0901_minute_close:" + str(minute)],
        "execution_price_0901": [100.0], "tick_volume_units_0901": [1000.0]}).write_parquet(book)
    session = {"session_date": "2026-11-04", "historical_entry_books": {
        "path": str(book), "sha256": hashlib.sha256(book.read_bytes()).hexdigest()}}
    monkeypatch.setattr(audit, "_verified_finmind_minute_projection", lambda path, **kwargs:
                        {str(minute): "a" * 64, str(manifest): "b" * 64})
    sources = audit._retained_entry_book_source_days(tmp_path / "state", session, {"2330"}, {})
    assert sources[str(minute)] == {date(2026, 11, 4)}
    monkeypatch.setattr(audit, "_verified_finmind_minute_projection", lambda *args, **kwargs:
                        (_ for _ in ()).throw(ValueError("source pin changed")))
    with pytest.raises(ValueError, match="source pin changed"):
        audit._retained_entry_book_source_days(tmp_path / "state", session, {"2330"}, {})


def test_repair_requires_exact_verified_finmind_session_and_pins(tmp_path, monkeypatch):
    from datetime import date
    from scripts import prepare_finmind_tw_day_trade_repair_source as source

    marker = tmp_path / source.MANIFEST
    marker.write_text("{}")
    projection = tmp_path / "data.parquet"
    projection.write_bytes(b"projection")
    native = tmp_path / "native.parquet"
    native.write_bytes(b"native")
    proof = {"contract": source.CONTRACT, "session_date": "2026-11-04", "summary": {},
        "output": {"path": str(projection), "sha256": repair._sha256(projection)},
        "source_pins": [{"path": str(native), "sha256": repair._sha256(native)}]}
    monkeypatch.setattr(source, "verify", lambda _root: proof)
    observed, pins = repair._verified_finmind_replay_sources([str(tmp_path)], date(2026, 11, 4))
    assert observed[0]["root"] == str(tmp_path)
    assert {item["path"] for item in pins} == {str(marker), str(projection), str(native)}
    repair._assert_source_pins(pins)
    native.write_bytes(b"changed")
    with pytest.raises(RuntimeError, match="repair source changed"):
        repair._assert_source_pins(pins)
    with pytest.raises(RuntimeError, match="another session"):
        repair._verified_finmind_replay_sources([str(tmp_path)], date(2026, 11, 5))


def test_finmind_source_rejects_noncanonical_provider_root(tmp_path, monkeypatch):
    from scripts import prepare_finmind_tw_day_trade_repair_source as source

    canonical = tmp_path / "data_finmind" / "complement"
    canonical.mkdir(parents=True)
    public = tmp_path / "data_tw_public"
    public.mkdir()
    other = tmp_path / "not-the-provider-owner"
    other.mkdir()
    monkeypatch.setattr(source, "ROOT", tmp_path)
    with pytest.raises(ValueError, match="canonical complement owner"):
        source._inputs("2026-11-04", other, public, tmp_path)
