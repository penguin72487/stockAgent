from datetime import date
import json

import polars as pl
import pytest

from scripts import stage_tw_day_trade_price_limit_bundle as bundle


def _inputs(tmp_path, monkeypatch):
    build = tmp_path / "build"
    build.mkdir()
    baseline = tmp_path / "baseline"
    baseline.mkdir()
    projection = build / "projections"
    projection.mkdir()
    days = [date(2026, 1, 2), date(2026, 1, 5)]
    for day in days:
        pl.DataFrame({"trading_date": [str(day)], "symbol": ["0050"], "reference_price": [100.],
                      "upper_limit_price": [110.], "lower_limit_price": [90.]}).write_parquet(baseline / f"{day}.parquet")
    extra = projection / str(days[1])
    extra.mkdir()
    source = extra / f"{days[1]}.parquet"
    pl.DataFrame({"trading_date": [str(days[1]), str(days[1])], "symbol": ["0050", "2330"],
                  "reference_price": [100., 1000.], "upper_limit_price": [110., None],
                  "lower_limit_price": [90., None]}).write_parquet(source)
    (extra / "finmind_price_limits_source.json").write_text("{}")
    audit = build / "audit.json"
    audit.write_text(json.dumps({"contract": "isolated_counterfactual_signals_source_audit_v1",
        "status": "source_gaps_remain", "ledger_modified": False, "publication_authorized": False,
        "sessions": 2, "start_date": str(days[0]), "end_date": str(days[1]), "market": "test",
        "targets_without_valid_limits": [{"date": str(days[1]), "symbol": "2330"}]}))
    monkeypatch.setattr(bundle.workspace_volume, "configuration", lambda: {"mount_point": str(tmp_path)})
    monkeypatch.setattr(bundle.workspace_volume, "admit", lambda **_kw: None)
    monkeypatch.setattr(bundle, "verify", lambda _root: {"session_date": str(days[1]),
        "output": {"path": str(source), "sha256": bundle.sha256_file(source)},
        "summary": {"unresolved_or_no_limit_symbols": ["2330"]}})
    return {"sessions": days, "baseline_root": baseline, "projection_root": projection,
            "source_audit_path": audit, "output_root": build / "bundle"}, source


def test_stages_exact_sessions_preserves_unresolved_and_originals(tmp_path, monkeypatch):
    args, _ = _inputs(tmp_path, monkeypatch)
    before = bundle.sha256_file(args["baseline_root"] / "2026-01-05.parquet")
    result = bundle.stage(**args)
    assert result["status"] == "staged_with_unresolved_limits"
    assert result["unresolved_or_no_limit_targets"] == [{"date": "2026-01-05", "symbol": "2330"}]
    assert result["ledger_modified"] is False and result["publication_authorized"] is False
    assert bundle.sha256_file(args["baseline_root"] / "2026-01-05.parquet") == before
    assert pl.read_parquet(args["output_root"] / "2026-01-05.parquet").height == 2
    with pytest.raises(ValueError, match="new isolated"):
        bundle.stage(**args)


def test_refuses_wrong_session_and_nonprivate_destination(tmp_path, monkeypatch):
    args, source = _inputs(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="new isolated"):
        bundle.stage(**{**args, "output_root": tmp_path / "outside"})
    pl.DataFrame({"trading_date": ["2026-01-06"], "symbol": ["2330"]}).write_parquet(source)
    with pytest.raises(ValueError, match="wrong dates"):
        bundle.stage(**args)
