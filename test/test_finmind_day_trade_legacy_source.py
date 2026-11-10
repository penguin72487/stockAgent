from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import hashlib
import json
from pathlib import Path

import polars as pl
import pytest

from scripts import prepare_finmind_tw_day_trade_legacy_source as legacy
from scripts import audit_tw_day_trade_margin_replay as audit


def legacy_units():
    return {"stock_share_unit_status": "mixed_market_lots_or_shares_preserved",
            "source_volume_units": {"volume": "TWSE_TPEX_lots_emerging_shares"},
            "unit_source": legacy.FINMIND_CASH_INTRADAY_UNIT_SOURCE}


@pytest.mark.parametrize("change", [
    {"stock_share_unit_status": "unknown"},
    {"source_volume_units": {"volume": "shares"}},
    {"unit_source": "other"},
    {"contract": "future_unrecognized_contract"},
    {"documented_share_multipliers_by_market": {"twse": 1}},
    {"requires_same_symbol_dated_market_evidence": False},
])
def test_legacy_units_fail_closed_on_changed_or_partial_declaration(change):
    value = legacy_units()
    legacy._require_legacy_units(value)
    value.update(change)
    with pytest.raises(ValueError, match="legacy native unit"):
        legacy._require_legacy_units(value)


def _fixture(tmp_path, monkeypatch):
    monkeypatch.setattr(legacy, "ROOT", tmp_path)
    provider = tmp_path / "data_finmind/complement"
    public = tmp_path / "data_tw_public"
    source = provider / "parquet/day.parquet"
    source.parent.mkdir(parents=True)
    public.mkdir()
    frame = pl.DataFrame([
        {"stock_id": "2330", "date": "2026-01-02", "minute": "09:00:00",
         "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 2},
        {"stock_id": "2330", "date": "2026-01-02", "minute": "13:30:00",
         "open": 102.0, "high": 102.0, "low": 102.0, "close": 102.0, "volume": 3},
    ])
    frame.write_parquet(source)
    receipt_path = provider / "receipts/TaiwanStockKBar/all/2026-01-02.json"
    receipt_path.parent.mkdir(parents=True)
    receipt = {"status": "complete", "dataset": "TaiwanStockKBar", "data_id": "",
        "partition": "2026-01-02", "parquet_path": "parquet/day.parquet",
        "parquet_size_bytes": source.stat().st_size, "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "rows": 2, "volume_units": legacy_units()}
    receipt_path.write_text(json.dumps(receipt))
    preflight = tmp_path / "preflight.json"
    preflight.write_text("pinned")
    monkeypatch.setattr(legacy, "_candidate_signal_scope", lambda *args: ({"2330"}, [legacy._pin(preflight)]))
    monkeypatch.setattr(legacy, "validate_parquet", lambda *_args: {"rows": 2})
    records = []
    for venue in ("twse", "tpex"):
        raw = public / (venue + ".json")
        raw.write_text(venue)
        record = legacy._pin(raw)
        records.append({"venue": venue, "path": str(raw), "sha256": record["sha256"], "size": record["size_bytes"]})
    monkeypatch.setattr(legacy, "pin_official_market_inputs", lambda *_args: records)
    monkeypatch.setattr(legacy, "_official_prices", lambda *_args: {
        "2330": {"open": 100.0, "close": 102.0, "volume_shares": 5000}})
    venue_frame = pl.DataFrame([{"symbol": "2330", "date": "2026-01-02", "venue": "twse", "source_ref": "a"}])
    monkeypatch.setattr(legacy, "read_official_market_evidence", lambda row, **_kw:
                        venue_frame if row["venue"] == "twse" else venue_frame.head(0))
    for name in ("scripts/prepare_finmind_tw_day_trade_legacy_source.py",
                 "scripts/prepare_finmind_tw_day_trade_repair_source.py", "stockagent/data/tw_minute_sources.py",
                 "stockagent/data/tw_minute_unit_evidence.py", "downloader/download_tw_public_data.py",
                 "downloader/finmind_storage_objects.py", "downloader/finmind_volume_units.py",
                 "downloader/stock_volume_units.py", "downloader/parquet_integrity.py"):
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(name)
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    monkeypatch.setattr(legacy.workspace_volume, "configuration", lambda: {"mount_point": str(workspace)})
    monkeypatch.setattr(legacy.workspace_volume, "admit", lambda **_kw: None)
    args = {"day": "2026-01-02", "finmind_root": provider, "public_root": public,
            "candidate_signal_preflight": preflight, "candidate_market": "test-market",
            "symbols": ("2330",), "output_root": workspace / "build/private-source"}
    return args, source, receipt_path


def test_legacy_projection_preserves_receipt_and_reconstructs_exact_source(tmp_path, monkeypatch):
    args, source, receipt = _fixture(tmp_path, monkeypatch)
    before = receipt.read_bytes()
    proof = legacy.build(**args)
    assert proof["status"] == "source_verified_not_ledger_applied"
    assert proof["summary"]["native_receipt_modified"] is False
    assert receipt.read_bytes() == before
    output = pl.read_parquet(proof["output"]["path"])
    first = output.filter(pl.col("ts") == datetime(2026, 1, 2, 9, 1))
    assert first["volume_shares"].item() == 2000
    assert output.filter(pl.col("ts") == datetime(2026, 1, 2, 9, 2)).is_empty()
    assert proof["summary"]["missing_minutes_are_unknown_not_zero"] is True
    audited = audit._verified_finmind_minute_projection(Path(proof["output"]["path"]), day="2026-01-02")
    assert audited[str(receipt)] == hashlib.sha256(before).hexdigest()
    assert audited[proof["output"]["path"]] == proof["output"]["sha256"]
    source.write_bytes(b"changed native source")
    with pytest.raises(RuntimeError, match="pinned legacy replay input changed"):
        legacy.verify(args["output_root"])
    with pytest.raises(RuntimeError, match="pinned legacy replay input changed"):
        audit._verified_finmind_minute_projection(Path(proof["output"]["path"]), day="2026-01-02")


def test_legacy_projection_rejects_unpinned_target_or_receipt_units(tmp_path, monkeypatch):
    args, _source, receipt = _fixture(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="nonzero targets"):
        legacy._inputs(**{key: value for key, value in args.items() if key != "output_root"} | {"symbols": ("9999",)})
    value = json.loads(receipt.read_text())
    value["volume_units"] = deepcopy(value["volume_units"]) | {"source_volume_units": {"volume": "shares"}}
    receipt.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="legacy native unit"):
        legacy.build(**args)
    assert not args["output_root"].exists()


def test_auditor_rejects_ambiguous_original_and_legacy_manifests(tmp_path):
    root = tmp_path / "projection"
    data = root / "trade_date=2026-01-02/data.parquet"
    data.parent.mkdir(parents=True)
    data.write_bytes(b"data")
    (root / "finmind_replay_source.json").write_text("{}")
    (root / legacy.MANIFEST).write_text("{}")
    with pytest.raises(ValueError, match="ambiguous"):
        audit._verified_finmind_minute_projection(data, day="2026-01-02")
