from dataclasses import asdict
from datetime import date, timedelta
import json
from pathlib import Path

import polars as pl
import pytest

from scripts.prepare_tw_day_trade_feature_catalog import sha256
from scripts.prepare_tw_day_trade_mixed_frequency import prepare, verify_sources, attach_formal_companions
from stockagent.config import load_config
from stockagent.data.panel import _load_external_feature_arrays, _resolve_corporate_action_reference_paths
from stockagent.data.tw_day_trade_mixed_frequency import CONTRACT, PRIVATE_USE, public_spec, storage_lookup
from stockagent.data.tw_public_release_schedule import RULES


def test_storage_dates_do_not_add_a_second_release_lag():
    sessions = [date(2026, 9, 1), date(2026, 9, 2), date(2026, 9, 3)]
    result = storage_lookup(sessions)
    assert result["date"].to_list() == sessions[:-1]
    assert result["decision_date"].to_list() == sessions[1:]
    with pytest.raises(ValueError, match="unique"):
        storage_lookup([sessions[0], sessions[0]])


def test_public_cadence_does_not_carry_daily_prices_or_snapshot_metadata():
    assert public_spec("twpub_pe_raw")["clock"] == "completed_session"
    assert public_spec("twpub_margin_balance_lots_raw")["clock"] == "available_session"
    assert public_spec("twpub_cbc_m1b_raw")["rule"]["carry_days"] == 62
    assert public_spec("twpub_dgbas_gdp_yoy_pct_raw")["rule"]["carry_days"] == 200
    assert public_spec("twpub_company_age_years") is None
    assert public_spec("twpub_mof_export_log") is None
    assert public_spec("twpub_cbc_m1b_log") is None
    assert public_spec("sha256") is None


def test_source_verification_checks_exact_content_and_delivery_authority(tmp_path):
    p = tmp_path/"fact.parquet"
    p.write_bytes(b"observed")
    manifest = {"contract": CONTRACT, "source_only": True, "private_delivery_authorized": True,
                "use_restriction": PRIVATE_USE, "files": {p.name: {"sha256": sha256(p)}}}
    (tmp_path/"source_manifest.json").write_text(json.dumps(manifest))
    assert verify_sources(tmp_path) == manifest
    p.write_bytes(b"changed")
    with pytest.raises(ValueError, match="hash mismatch"):
        verify_sources(tmp_path)


def test_formal_companions_bind_original_receipts_without_moving_model_inputs(tmp_path):
    source, out = tmp_path/"source", tmp_path/"prepared"
    source.mkdir()
    out.mkdir()
    # Formal root-bound raw receipts cannot be symlinked out of the view.
    raw_relative = "raw_receipts/exact.jsonl"
    (source/"raw_receipts").mkdir()
    (source/raw_relative).write_text('{}\n')
    for name in ("tw_corporate_action_reference.parquet", "tw_corporate_action_entitlements.parquet"):
        (source/name).write_bytes(b"original formal observation")
    (source/"tw_corporate_action_reference.summary.json").write_text('{}')
    (source/"tw_corporate_action_entitlements.summary.json").write_text(json.dumps({
        "raw_receipt_manifest": {"relative_path": raw_relative}}))
    files = {str(p.relative_to(source)): {"sha256": sha256(p)} for p in source.rglob('*') if p.is_file()}
    manifest = {"contract": CONTRACT, "source_only": True, "private_delivery_authorized": True,
                "use_restriction": PRIVATE_USE, "files": files}
    (source/"source_manifest.json").write_text(json.dumps(manifest))
    matrix = out/"model_inputs.parquet"
    matrix.write_bytes(b"unchanged model X")
    before = sha256(matrix)
    receipt = attach_formal_companions(source, out)
    assert sha256(matrix) == before
    assert receipt["model_matrix_changed"] is False
    assert len(receipt["formal_action_members"]) == 5
    assert not (out/"features/tw_public_stock_daily.parquet").exists()
    paths = _resolve_corporate_action_reference_paths(matrix, include_rules=True)
    assert paths is not None and paths.entitlements_parquet == out/"tw_corporate_action_entitlements.parquet"
    assert (out/raw_relative).resolve().is_relative_to(out.resolve())
    assert not (out/raw_relative).is_symlink()
    assert attach_formal_companions(source, out) == receipt
    (out/raw_relative).write_text('changed\n')
    with pytest.raises(ValueError, match="existing formal companion mismatch"):
        attach_formal_companions(source, out)


def test_remote_view_round_trip_has_causal_clocks_separate_rules_and_fresh_config(tmp_path):
    source = tmp_path/"source"
    (source/"stocks").mkdir(parents=True)
    (source/"features").mkdir()
    (source/"finlab/observations").mkdir(parents=True)
    days = [date(2014, 1, 3), date(2014, 1, 6), date(2014, 1, 7), date(2014, 1, 8)]
    pl.DataFrame({"date": days, "lifecycle_episode_id": [0]*4}).write_parquet(source/"stocks/2330_features.parquet")
    pl.DataFrame({"date": days}).write_parquet(source/"twse_taiex_ohlc.parquet")
    pl.DataFrame({"date": days, "symbol": ["2330"]*4, "twpub_pe_raw": [1., 2., 3., 999.],
                  "_twpub_day_trade_eligible": [0., 1., 1., 1.]}).write_parquet(source/"features/tw_public_stock_daily.parquet")
    key = "etl:inventory:大於四百張佔比"
    path = source/"finlab/observations/weekly.parquet"
    pl.DataFrame({"source_index": ["2014-01-03", "2014-01-06"], "2330": [10., 20.]}).write_parquet(path)
    from stockagent.data.tw_public_release_schedule import feature_name
    name = feature_name(key)
    specs = [public_spec("twpub_pe_raw"), {"feature": name, "source": "FinLab", "dataset": key,
        "path": str(path.relative_to(source)), "rule": asdict(RULES["weekly"]), "clock": "estimated_publication"}]
    files = {str(p.relative_to(source)): {"sha256": sha256(p)} for p in source.rglob("*.parquet")}
    (source/"source_manifest.json").write_text(json.dumps({"contract": CONTRACT, "source_only": True,
        "private_delivery_authorized": True, "use_restriction": PRIVATE_USE, "files": files,
        "feature_specs": specs, "end_date": "2014-01-08"}))
    base = Path(__file__).resolve().parents[1]/"configs/deployments/tw_day_trade_last_last_only_training_vastai1t_v10_research_143_no_bottleneck.yaml"
    out = tmp_path/"experiment/prepared"
    result = prepare(source=source, out=out, base_config=base, minute_root=tmp_path/"minutes", snapshot_id="test-source-id", chunk_sessions=1)
    assert result["feature_view_ready"] and not result["training_ready"]
    config = load_config(out/"training.yaml")
    assert config.runner.resume is False
    assert config.training.pretrained_initialization_root is None
    assert config.data.feature_shift_next_session == []
    assert config.data.feature_availability_indicators == []
    assert config.training.financial_transformer.feature_bottleneck_dim == 0
    frame = pl.read_parquet(out/"model_inputs.parquet").sort("date")
    # Jan 6's source close is first legal for the Jan 7 model, whose canonical
    # feature window ends on the stored Jan 6 row. Jan 8's close must not leak.
    assert frame.filter(pl.col("date")==date(2014,1,6))["twcad_pe_raw"].item() == 2.
    assert frame.filter(pl.col("date")==date(2014,1,6))[name].item() == 20.
    assert frame.filter(pl.col("date")==date(2014,1,7))[name+"__age_days"].item() == 1.
    assert frame.filter(pl.col("date")==date(2014,1,8))["twcad_pe_raw"].item() is None
    assert 999. not in frame["twcad_pe_raw"].drop_nulls().to_list()
    assert frame["_twpub_day_trade_eligible"].to_list() == [0.,1.,1.,1.]
    arrays = _load_external_feature_arrays(out/"model_inputs.parquet", selected_feature_names=(name,name+"__available"))
    assert arrays.feature_names == [name,name+"__available"]
    assert "_twpub_day_trade_eligible" in arrays.rule_names
    assert result["rows"] == frame.height
    # Chunking/predicate pruning must not alter values, NULL barriers, or the
    # date of any executor rule, including the final completed-session tail.
    out2 = tmp_path/"experiment2/prepared"
    result2 = prepare(source=source, out=out2, base_config=base,
                      minute_root=tmp_path/"minutes", snapshot_id="test-source-id", chunk_sessions=2)
    assert frame.equals(pl.read_parquet(out2/"model_inputs.parquet").sort("date"))
    proof1 = json.loads((out/"dataset_manifest.json").read_text())
    proof2 = json.loads((out2/"dataset_manifest.json").read_text())
    assert proof1["feature_available_cells"] == proof2["feature_available_cells"]
    with pytest.raises(FileExistsError):
        prepare(source=source, out=out, base_config=base, minute_root=tmp_path/"minutes", snapshot_id="test-source-id")
