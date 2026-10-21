"""Exact daily continuation of the original factorized causal sweep."""
from copy import deepcopy
from dataclasses import asdict
from datetime import date, timedelta

import numpy as np
import polars as pl
import pytest
import json
from pathlib import Path

from scripts.prepare_tw_day_trade_factorized_panel import AnnualObservationEvents, causal_state_blocks
from stockagent.data.causal_projection_state import load_state, save_state
from stockagent.data.tw_public_release_schedule import RULES


def materialize(blocks, rows, stocks, features):
    output = np.zeros((rows, stocks, features), dtype=np.float32)
    for start, block, _ in blocks:
        if isinstance(block, np.ndarray):
            output[start:start + len(block)] = block
        else:
            for symbol_start, columns, values in block.symbol_blocks:
                output[start:start + block.shape[0], symbol_start:symbol_start + values.shape[1], columns] = values
    return output


@pytest.mark.parametrize("compact", [False, True])
@pytest.mark.parametrize("policy", ["value_only", "value_available_age_updated"])
@pytest.mark.parametrize("annual", [False, True])
def test_ready_state_resume_equals_full_bits_counts_and_clocks(tmp_path, compact, policy, annual):
    days = [date(2015, 4, 28) + timedelta(days=offset) for offset in range(45)]
    definitions = [{"feature": name, "rule": asdict(RULES[rule]), "report_group": group}
                   for name, rule, group in [("a", "quarter", "report"), ("b", "quarter", "report"),
                                              ("weekly", "weekly", None), ("daily", "daily", None),
                                              ("free", "quarter", None)]]
    raw = pl.DataFrame({"date": [days[0], days[0], days[0], days[0], days[21], days[22], days[24], days[28]],
        "symbol": ["2330"] * 8, "feature": ["a", "b", "weekly", "free", "daily", "a", "free", "a"],
        "value": [-0.0, 100000001.123456, 4., 9., 8., None, 999., 999.],
        "period": ["2015Q2", "2015Q2", "2015-04-01", "2015Q2", "2015-05-19", "2015Q3", "2015Q1", "2015Q1"]})
    resets = raw.filter(pl.col("feature").is_in(["a", "b"])).select("date", "symbol", "period").unique().with_columns(
        pl.lit("report").alias("report_group"))
    lifecycle = np.full((len(days), 1), date(2014, 1, 1).toordinal(), dtype=np.int32)
    lifecycle[35:] = days[35].toordinal()
    if annual:
        path = tmp_path / "events.parquet"
        raw.with_columns(pl.lit(0).alias("_source_part")).write_parquet(path)
        events = lambda: AnnualObservationEvents([path])
    else:
        events = lambda: raw
    options = dict(resets=resets, model_channel_policy=policy, column_blocks=compact, max_block_bytes=400)
    width = len(definitions) * (1 if policy == "value_only" else 4)
    snapshots = []
    original = list(causal_state_blocks(days, ["2330"], definitions, events(), lifecycle,
        checkpoint_row=21, on_checkpoint=lambda state: snapshots.append(deepcopy(state)), **options))
    state = snapshots[0]
    assert state["values"].dtype == np.float64
    assert state["values"][0, 1] == 100000001.123456
    assert state["observed"].dtype == np.int32
    assert state["row"] == 21 and state["decision"] == days[21].isoformat()
    proof = save_state(tmp_path, "state", state)
    recovered = load_state(tmp_path, proof)
    resumed = list(causal_state_blocks(days, ["2330"], definitions, events(), lifecycle,
                                     resume_state=recovered, **options))
    full = materialize(original, len(days), 1, width)
    tail = materialize(resumed, len(days), 1, width)
    np.testing.assert_array_equal(tail[21:].view(np.uint32), full[21:].view(np.uint32))
    np.testing.assert_array_equal(resumed[-1][2], original[-1][2])
    assert resumed[0][0] == 21
    if policy == "value_only":
        assert tail[24, 0, 4] == 9.  # Late Q1 cannot overwrite the checkpoint's Q2.


@pytest.mark.parametrize("corruption", ["precision", "symbols", "definitions", "calendar", "clock", "payload"])
def test_checkpoint_cannot_resume_under_incompatible_inputs(tmp_path, corruption):
    days = [date(2026, 10, 19) + timedelta(days=offset) for offset in range(4)]
    definitions = [{"feature": "a", "rule": asdict(RULES["quarter"])}]
    raw = pl.DataFrame({"date": [days[0]], "symbol": ["2330"], "feature": ["a"], "value": [1.123456789]})
    states = []
    list(causal_state_blocks(days, ["2330"], definitions, raw, None,
                            checkpoint_row=2, on_checkpoint=lambda state: states.append(deepcopy(state))))
    state = states[0]
    if corruption == "payload":
        proof = save_state(tmp_path, "state", state)
        path = tmp_path / "state_values.npy.zst"
        path.chmod(0o644)
        path.write_bytes(b"not a verified checkpoint")
        with pytest.raises(ValueError, match="array changed"):
            load_state(tmp_path, proof)
        return
    if corruption == "precision":
        state["values"] = state["values"].astype(np.float32)
    elif corruption == "symbols":
        symbols = ["2317"]
    elif corruption == "definitions":
        definitions = [{**definitions[0], "report_group": "different"}]
    elif corruption == "calendar":
        days[2] += timedelta(days=1)
    else:
        state["decision"] = "2026-10-01"
    with pytest.raises(ValueError, match="causal"):
        list(causal_state_blocks(days, symbols if corruption == "symbols" else ["2330"],
                                 definitions, raw, None, resume_state=state))


@pytest.fixture
def native_preparer(tmp_path, monkeypatch):
    """Real source/event/writer/publication owners; synthetic BASE/calendar only."""
    from scripts import prepare_tw_day_trade_factorized_live as owner
    from stockagent.data.factorized_panel import VALUE_ONLY_CONTRACT, file_sha256
    from stockagent.data.panel import PanelData

    public, parent_root, native = [tmp_path / name for name in ("public", "parent", "native")]
    for path in (public / "stocks", public / "features", parent_root, native):
        path.mkdir(parents=True)
    symbols = ["2330", "2317"]
    definitions = [{"feature": name, "source": "tw-public", "source_column": name,
                    "clock": "available_session", "rule": {**asdict(RULES["quarter"]), "scope": scope}}
                   for name, scope in (("issuer", "stock"), ("market", "market"))]
    dictionary = parent_root / "feature_dictionary.json"
    dictionary.write_text(json.dumps({"features": definitions}))
    seeds = []
    for year, day in ((2025, date(2025, 12, 31)), (2026, date(2026, 10, 9))):
        path = parent_root / f"{year}.parquet"
        pl.DataFrame({"date": [day] * 3, "symbol": [*symbols, "*"],
            "feature": ["issuer", "issuer", "market"], "value": [100000001.12345, -0., 8.],
            "period": [day.isoformat()] * 3, "_source_part": [0] * 3}).write_parquet(path)
        seeds.append({"path": path.name, "sha256": file_sha256(path)})
    parent = {"contract": VALUE_ONLY_CONTRACT, "status": "complete", "feature_lag": 1,
        "research_only": True, "historical_point_in_time": False, "dates": ["2026-10-09"],
        "feature_dictionary_sha256": file_sha256(dictionary), "annual_events": seeds,
        "individual_channels": ["issuer"], "common_channels": ["market"], "symbols": symbols,
        "base_feature_names": ["close_raw"], "logical_model_channels": 3,
        "model_channel_policy": "value_only", "value_features": 2}
    parent_path = parent_root / "factorized_manifest.json"
    parent_path.write_text(json.dumps(parent))
    (native / "source_manifest.json").write_text(json.dumps({"feature_specs": definitions}))
    config_path = tmp_path / "config.yaml"
    config_path.write_text(json.dumps({"base_config": str(Path("configs/markets/tw.yaml").resolve())}))
    (public / owner.TW_HOLIDAY_SCHEDULE_NAME).write_text("{}")
    (public / "twse_taiex_ohlc.summary.json").write_text("{}")
    current = {}

    def update(days, *, revise=False, episode=False):
        raw_dates = [date(2026, 10, 9), *days]
        pl.DataFrame({"date": raw_dates}).write_parquet(public / "twse_taiex_ohlc.parquet")
        for symbol in symbols:
            pl.DataFrame({"date": raw_dates,
                "lifecycle_episode_id": ["original"] * (len(raw_dates) - 1) + ["new" if episode else "original"]
                }).write_parquet(public / "stocks" / f"{symbol}_features.parquet")
        pl.DataFrame([{"date": day, "symbol": symbol, "issuer": float(index + 10) + (9. if revise and index == 1 else 0.),
                       "market": float(index)} for index, day in enumerate(raw_dates) for symbol in symbols]
                     ).write_parquet(public / "features/tw_public_stock_daily.parquet")
        (public / "features/tw_public_stock_daily.summary.json").write_text(json.dumps({
            "requested_end_date": str(days[-1]), "output_receipt": {
                "sha256": file_sha256(public / "features/tw_public_stock_daily.parquet")}}))
        mask = np.ones((len(days), 2), dtype=bool)
        current["panel"] = PanelData(np.array(days, dtype="datetime64[D]"), symbols, ["close_raw"],
            np.ones((len(days), 2, 1), dtype=np.float32), np.zeros((len(days), 2), dtype=np.float32),
            mask, mask, np.zeros(len(days), dtype=np.float32), np.ones((len(days), 2), dtype=np.float32))

    monkeypatch.setattr(owner, "admit_output", lambda path, _: path)
    monkeypatch.setattr(owner, "build_panel", lambda *args, **kwargs: current["panel"])
    monkeypatch.setattr(owner, "_build_panel_kwargs", lambda config: {})
    monkeypatch.setattr(owner, "_next_projection_session", lambda public, completed: (completed + timedelta(days=1), {}))
    kwargs = dict(config_path=config_path, parent_path=parent_path, native_source=native,
        public=public, finlab=tmp_path / "finlab", start_date="2026-10-12")
    return owner, update, kwargs


@pytest.mark.parametrize("change", ["new_day", "new_lifecycle", "old_revision", "missing_state"])
def test_actual_preparer_daily_publication_equals_fresh_full_generation(native_preparer, tmp_path, change):
    from stockagent.data.factorized_panel import FactorizedPanelFeatures, verify_factorized_members

    owner, update, kwargs = native_preparer
    output = tmp_path / "incremental"
    days = [date(2026, 10, 12), date(2026, 10, 13)]
    update(days)
    first = owner.prepare(**kwargs, output=output)
    prior_path = Path(first["manifest"])
    prior = json.loads(prior_path.read_text())
    prior_member = prior_path.parent / prior["blocks"][0]["symbol_blocks"][0]["path"]
    original_inode = prior_member.stat().st_ino
    if change == "missing_state":
        state = prior["daily_incremental"]["states"]["individual"]
        (prior_path.parent / state["arrays"]["values"]["path"]).unlink()
    days.append(date(2026, 10, 14))
    update(days, revise=change == "old_revision", episode=change == "new_lifecycle")
    incremental = owner.prepare(**kwargs, output=output)
    fresh = owner.prepare(**kwargs, output=tmp_path / "fresh")
    manifests = []
    dense = []
    for result in (incremental, fresh):
        path = Path(result["manifest"])
        manifest = json.loads(path.read_text())
        verify_factorized_members(path.parent, manifest)
        arrays = FactorizedPanelFeatures(path.parent, manifest, np.ones((len(days), 2, 1), dtype=np.float32))
        dense.append(arrays[:])
        manifests.append(manifest)
    np.testing.assert_array_equal(dense[0].view(np.uint32), dense[1].view(np.uint32))
    daily = manifests[0]["daily_incremental"]
    timings = manifests[0]["stage_elapsed_seconds"]
    assert {"source_normalization_and_event_proofs", "prefix_integrity_and_state_restore",
            "individual_causal_sweep_and_block_io", "common_causal_sweep_and_block_io",
            "new_generation_member_integrity", "prepublication_source_fence"} <= timings.keys()
    assert all(value >= 0 for value in timings.values())
    assert daily["checkpoint_io_seconds_included_in_sweeps"] >= 0
    assert incremental["stage_elapsed_seconds"]["generation_publish"] >= 0
    if change in {"new_day", "new_lifecycle"}:
        assert daily["reused_rows"] == 1 and daily["rebuilt_rows"] == 2
        path = Path(incremental["manifest"]).parent / manifests[0]["blocks"][0]["symbol_blocks"][0]["path"]
        assert path.stat().st_ino == original_inode
        assert daily["resume_reason"] == "verified_exact_prefix"
    else:
        assert daily["reused_rows"] == 0 and daily["rebuilt_rows"] == len(days)
    assert owner.prepare(**kwargs, output=output)["status"] == "already_current"


def test_actual_preparer_reuses_unchanged_normalized_finlab_across_new_session(native_preparer, tmp_path, monkeypatch):
    from stockagent.data.factorized_panel import FactorizedPanelFeatures, file_sha256
    from stockagent.data.tw_public_release_schedule import feature_name
    owner, update, kwargs = native_preparer
    parent_path, native = kwargs["parent_path"], kwargs["native_source"]
    dictionary_path = parent_path.parent / "feature_dictionary.json"
    parent, dictionary = json.loads(parent_path.read_text()), json.loads(dictionary_path.read_text())
    key = "security_lending:借券成交"
    name = feature_name(key)
    definition = {"feature": name, "source": "FinLab", "dataset": key, "rule": asdict(RULES["daily"])}
    dictionary["features"].append(definition)
    dictionary_path.write_text(json.dumps(dictionary))
    parent["individual_channels"].append(name)
    parent["value_features"] += 1
    parent["logical_model_channels"] += 1
    parent["feature_dictionary_sha256"] = file_sha256(dictionary_path)
    parent_path.write_text(json.dumps(parent))
    (native / "source_manifest.json").write_text(json.dumps({"feature_specs": dictionary["features"]}))
    raw = tmp_path / "finlab-source.parquet"
    pl.DataFrame({"source_index": ["2026-10-09", "2026-10-12"],
        "2330": [-0.0, 100000001.123456], "2317": [9., 8.]}).write_parquet(raw)
    monkeypatch.setattr(owner, "read_receipt_bound_source", lambda *args: (raw, {"sha256": file_sha256(raw)}))
    days, output = [date(2026, 10, 12), date(2026, 10, 13)], tmp_path / "reuse"
    update(days)
    first = owner.prepare(**kwargs, output=output)
    prior = json.loads(Path(first["manifest"]).read_text())
    assert prior["daily_incremental"]["source_normalization_reuse"]["builds"] == 1
    days.append(date(2026, 10, 14))
    update(days)
    with monkeypatch.context() as patch:
        patch.setattr(owner, "normalize_source", lambda *a, **kw: pytest.fail("unchanged raw source normalized again"))
        result = owner.prepare(**kwargs, output=output)
    fresh = owner.prepare(**kwargs, output=tmp_path / "fresh-normalization")
    actual = json.loads(Path(result["manifest"]).read_text())
    assert actual["daily_incremental"]["source_normalization_reuse"]["hits"] == 1
    arrays = []
    for item in (result, fresh):
        path = Path(item["manifest"])
        manifest = json.loads(path.read_text())
        arrays.append(FactorizedPanelFeatures(path.parent, manifest, np.ones((len(days), 2, 1), dtype=np.float32))[:])
    np.testing.assert_array_equal(arrays[0].view(np.uint32), arrays[1].view(np.uint32))


def test_generation_budget_deferral_keeps_last_good_pointer_and_existing_hit(native_preparer, tmp_path, monkeypatch):
    from stockagent.data_sync.completed_forward_capacity import CapacityDeferred
    owner, update, kwargs = native_preparer
    days, output = [date(2026, 10, 12), date(2026, 10, 13)], tmp_path / "budget"
    update(days)
    first = owner.prepare(**kwargs, output=output)
    original = (output / "current").resolve()
    monkeypatch.setattr(owner, "load_prepared_capacity_policy", lambda: {"maximum_generation_bytes": 1})
    assert owner.prepare(**kwargs, output=output)["status"] == "already_current"
    days.append(date(2026, 10, 14))
    update(days)
    with pytest.raises(CapacityDeferred, match="generation_byte_budget_exhausted"):
        owner.prepare(**kwargs, output=output)
    assert (output / "current").resolve() == original
    assert Path(first["manifest"]).is_file()
