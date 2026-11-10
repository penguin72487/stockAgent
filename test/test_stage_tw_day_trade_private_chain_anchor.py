import copy
from datetime import date, datetime
import json

import polars as pl
import pytest

from scripts import stage_tw_day_trade_private_chain_anchor as chain
from scripts.stage_tw_day_trade_private_chain_anchor import _check_private_receipt


def _receipt():
    return {"contract": "private_historical_progressive_candidate_v1",
            "status": "private_candidate_verified_not_applied", "simulation_only": True,
            "broker_orders_created": 0, "live_ledger_modified": False, "publication_authorized": False,
            "subsequent_sessions_replayed": False,
            "minute_source_coverage": {"minute_points": 270, "duplicate_minute_points": 0,
                "unsourced_held_price_minutes": 0, "unknown_carried_minute_symbols": {}}}


def test_private_chain_requires_full_observed_candidate_and_never_mutates_it():
    receipt = _receipt()
    before = copy.deepcopy(receipt)
    _check_private_receipt(receipt)
    assert receipt == before


@pytest.mark.parametrize("damage", ["gap", "duplicate", "short_curve", "broker", "published", "applied", "replayed"])
def test_private_chain_rejects_incomplete_or_nonprivate_candidate(damage):
    receipt = _receipt()
    if damage == "gap": receipt["minute_source_coverage"]["unknown_carried_minute_symbols"] = {"09:01": ["2330"]}
    if damage == "duplicate": receipt["minute_source_coverage"]["duplicate_minute_points"] = 1
    if damage == "short_curve": receipt["minute_source_coverage"]["minute_points"] = 269
    if damage == "broker": receipt["broker_orders_created"] = 1
    if damage == "published": receipt["publication_authorized"] = True
    if damage == "applied": receipt["live_ledger_modified"] = True
    if damage == "replayed": receipt["subsequent_sessions_replayed"] = True
    with pytest.raises(ValueError):
        _check_private_receipt(receipt)


@pytest.mark.parametrize("damage", [None, "missing_zero", "price_mismatch", "wrong_source"])
def test_zero_trade_supplement_requires_native_receipt_and_matching_positive_minutes(monkeypatch, tmp_path, damage):
    monkeypatch.setattr(chain, "ROOT", tmp_path)
    day = date(2026, 11, 2)
    raw = tmp_path / "data_tw_minute/shioaji_1m/minute_chunks/2330/2026-11-02_2026-11-02.parquet"
    raw.parent.mkdir(parents=True)
    rows = [{"symbol": "2330", "ts": datetime(2026, 11, 2, 9, minute),
             "Open": price, "High": price, "Low": price, "Close": price,
             "Volume": volume, "Amount": volume * price * 1000, "volume_shares": volume * 1000}
            for minute, price, volume in ((1, 10.0, 1.0), (2, 10.0, 0.0), (3, 11.0, 1.0))]
    if damage == "missing_zero": rows[1]["Volume"] = 1.0
    pl.DataFrame(rows).write_parquet(raw)
    native = {"underlying_data_method": "provider_kbars" if damage != "wrong_source" else "unknown",
              "status": "ok", "returned_dates": [str(day)], "output_receipt": {"path": str(raw)}}
    raw.with_suffix(".receipt.json").write_text(json.dumps(native))
    finmind = tmp_path / "finmind.parquet"
    positive = [row for row in rows if row["volume_shares"] > 0]
    if damage == "price_mismatch": positive[-1] = {**positive[-1], "Close": 12.0}
    pl.DataFrame(positive).write_parquet(finmind)
    manifest = tmp_path / "finmind_replay_source.json"
    manifest.write_text("{}")
    receipt = {"source_pins": [{"path": str(manifest)}],
               "minute_source_coverage": {"unknown_carried_minute_symbols": {"2026-11-02T09:02": ["2330"]}}}
    monkeypatch.setattr(chain, "minute_receipt_valid", lambda *args, **kwargs: True)
    monkeypatch.setattr(chain, "verify_finmind_source", lambda root: {"session_date": str(day),
                                                                     "output": {"path": str(finmind)}})
    if damage:
        with pytest.raises(RuntimeError): chain._verified_zero_trade_sources(receipt, day=day)
    else:
        files, pins = chain._verified_zero_trade_sources(receipt, day=day)
        assert files[0]["path"] == str(raw) and len(pins) == 2


@pytest.mark.parametrize("damage", [None, "missing_pin", "missing_symbol", "different_gaps"])
def test_provisional_private_gaps_require_same_pinned_observations(monkeypatch, tmp_path, damage):
    day = date(2026, 10, 30)
    manifest = tmp_path / "finmind_replay_source.json"
    manifest.write_text("{}")
    parquet = tmp_path / "data.parquet"
    parquet.write_bytes(b"pinned source")
    pins = [{"path": str(manifest), "sha256": "a" * 64}]
    if damage != "missing_pin":
        pins.append({"path": str(parquet), "sha256": "b" * 64})
    coverage = {"minute_points": 270, "unknown_carried_minute_symbols": {"2026-10-30T10:32": ["6213"]}}
    receipt = {"market": "m", "source_pins": pins, "minute_source_coverage": coverage}
    mode = {"positions": {"p": {"symbol": "6213", "session_date": str(day)}}}
    monkeypatch.setattr(chain, "verify_finmind_source", lambda root: {"session_date": str(day)})
    monkeypatch.setattr(chain.owner, "_minute_bar_rows", lambda *args, **kwargs: (
        {} if damage == "missing_symbol" else {"6213": {}},
        {"source_files": [{"path": str(parquet), "sha256": "b" * 64}]}))
    monkeypatch.setattr(chain, "verify_minute_source_coverage", lambda *args, **kwargs:
        {"minute_points": 269} if damage == "different_gaps" else coverage)
    monkeypatch.setattr(chain.provisional_audit, "audit_marks", lambda *args, **kwargs: {})
    if damage:
        with pytest.raises(RuntimeError):
            chain._provisional_coverage(receipt, day=day, mode=mode, marks=[])
    else:
        assert chain._provisional_coverage(receipt, day=day, mode=mode, marks=[]) == coverage


def test_unverified_upstream_cannot_silently_become_a_strict_private_chain():
    prior = {"inherited_unverified_minute_gaps": [{"session_date": "2026-10-30"}]}
    with pytest.raises(RuntimeError, match="explicit private diagnostic"):
        chain._inherited_gaps(prior, allowed=False)
    assert chain._inherited_gaps(prior, allowed=True) == prior["inherited_unverified_minute_gaps"]
    assert chain._inherited_gaps({}, allowed=False) == []
    with pytest.raises(ValueError, match="invalid inherited"):
        chain._inherited_gaps({"inherited_unverified_minute_gaps": "unknown"}, allowed=True)


@pytest.mark.parametrize("case", ["proved", "unproved_allowed", "unproved_forbidden", "conflict_allowed"])
def test_stage_prefers_proved_zero_and_never_downgrades_source_conflicts(monkeypatch, case):
    def proof(*args, **kwargs):
        if case.startswith("unproved"):
            raise chain.UnprovenZeroTradeSource("no explicit row")
        if case == "conflict_allowed":
            raise RuntimeError("provider positive-minute data differ")
        return ([{"path": "/source"}], [{"path": "/receipt", "sha256": "a" * 64}])
    monkeypatch.setattr(chain, "_verified_zero_trade_sources", proof)
    monkeypatch.setattr(chain, "_provisional_coverage", lambda *args, **kwargs: {"kind": "provisional"})
    monkeypatch.setattr(chain, "verify_minute_source_coverage", lambda *args, **kwargs: {"kind": "strict"})
    kwargs = {"receipt": {"market": "m"}, "day": date(2026, 11, 2), "mode": {}, "marks": [],
              "allow_unverified_private_gaps": case != "unproved_forbidden"}
    if case == "proved":
        coverage, pins, unverified = chain._stage_coverage(**kwargs)
        assert coverage == {"kind": "strict"} and len(pins) == 1 and unverified is False
    elif case == "unproved_allowed":
        assert chain._stage_coverage(**kwargs) == ({"kind": "provisional"}, [], True)
    else:
        with pytest.raises(RuntimeError):
            chain._stage_coverage(**kwargs)


@pytest.mark.parametrize("conflict", [False, True])
def test_zero_proof_checks_all_existing_minute_roots(monkeypatch, tmp_path, conflict):
    monkeypatch.setattr(chain, "ROOT", tmp_path)
    monkeypatch.setattr(chain.owner, "DEFAULT_MINUTE_DATA_ROOTS", ("sparse", "maintenance"))
    day = date(2026, 11, 3)
    rows = [{"symbol": "2330", "ts": datetime(2026, 11, 3, 9, minute),
             "Open": price, "High": price, "Low": price, "Close": price,
             "Volume": volume, "Amount": volume * price * 1000, "volume_shares": volume * 1000}
            for minute, price, volume in ((1, 10.0, 1.0), (2, 10.0, 0.0), (3, 11.0, 1.0))]
    for root, values in (("sparse", [rows[0], rows[2]]), ("maintenance", rows)):
        raw = tmp_path / root / "minute_chunks/2330/2026-11-03_2026-11-03.parquet"
        raw.parent.mkdir(parents=True)
        if conflict and root == "sparse":
            values = [values[0], {**values[1], "Close": 12.0}]
        pl.DataFrame(values).write_parquet(raw)
        raw.with_suffix(".receipt.json").write_text(json.dumps({"underlying_data_method": "provider_kbars",
            "status": "ok", "returned_dates": [str(day)], "output_receipt": {"path": str(raw)}}))
    finmind = tmp_path / "finmind.parquet"
    pl.DataFrame([rows[0], rows[2]]).write_parquet(finmind)
    manifest = tmp_path / "finmind_replay_source.json"
    manifest.write_text("{}")
    receipt = {"source_pins": [{"path": str(manifest)}],
        "minute_source_coverage": {"unknown_carried_minute_symbols": {"2026-11-03T09:02": ["2330"]}}}
    monkeypatch.setattr(chain, "minute_receipt_valid", lambda *args, **kwargs: True)
    monkeypatch.setattr(chain, "verify_finmind_source", lambda root: {"session_date": str(day),
                                                                     "output": {"path": str(finmind)}})
    if conflict:
        with pytest.raises(RuntimeError, match="positive-minute data differ"):
            chain._verified_zero_trade_sources(receipt, day=day)
    else:
        files, pins = chain._verified_zero_trade_sources(receipt, day=day)
        assert len(files) == 1 and "/maintenance/" in files[0]["path"] and len(pins) == 4
