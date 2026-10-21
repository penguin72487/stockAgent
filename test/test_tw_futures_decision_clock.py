from dataclasses import replace
from datetime import date
import json

import numpy as np
import polars as pl
import pytest
import torch
import yaml

from downloader.artifact_io import sha256_file
from scripts.prepare_tw_futures_decision_clocks import paired_entries
from stockagent.config import load_config, _load_raw_config
from stockagent.data.panel import slice_panel_start
from stockagent.data.tw_futures_decision_clock import (
    CLOCK_DATASET, decision_clock_contract, reprice_margin_entries,
    validate_clock_source, attach_futures_decision_clock,
)
from test_tw_stock_context_futures_portfolio import _stock_panel
from test_tw_futures_margin import tape, run


def source(quote=110., status="observed_minute"):
    return pl.DataFrame({
        "date": [date(2026, 1, 2)], "status": [status],
        "entry_quote": [quote], "entry_volume": [20. if quote is not None else 0.],
        "open": [100.], "contract_multiplier": [10.],
        "opening_contract_value_twd": [1010.],  # 10 TWD of corporate cash
        "known_margin_contract_value_twd": [1000.],
        "opening_margin_value_twd": [1000.], "known_margin_value_twd": [990.],
        "initial": [.1], "maintenance": [.075], "margin_kind": ["notional_rate"],
        "opening_tax_twd": [0.], "known_tax_twd": [0.],
        "upper_limit": [120.], "lower_limit": [90.],
    }, schema_overrides={"entry_quote": pl.Float64})


def execution():
    ex = np.pad(tape().numpy()[0], ((0, 0), (0, 4)))
    ex[0, 3:5] = [1010., 1020.]
    ex[0, 29:33] = [1., 1., 1., 0.]
    return ex


def test_clock_windows_are_strictly_later_and_equal_latency():
    for clock, decision in [("08:45", 525), ("09:00", 540)]:
        contract = decision_clock_contract(clock)
        assert decision < contract["window_start_minute"] < contract["window_end_minute"]
        assert contract["latency_minutes"] == 1
    with pytest.raises(ValueError):
        decision_clock_contract("08:46")


def test_entry_reprice_keeps_corporate_cash_and_independent_terminal_capacity():
    old = execution()
    new = reprice_margin_entries(old, source(), participation=.5, broker_multiplier=1.)
    assert new[0, 3] == 1110.  # shares * new quote + original corporate cash
    assert new[0, 11:13].tolist() == [110., 83.]
    assert new[0, 8] == 10
    changed = {0, 1, 3, 6, 8, 11, 12, 18, 19}
    np.testing.assert_array_equal(new[:, [i for i in range(33) if i not in changed]],
                                  old[:, [i for i in range(33) if i not in changed]])
    assert old[0, 3] == 1010.  # shared source tape must not mutate


def test_no_window_quote_is_not_a_fill_or_daily_open_fallback():
    old = execution()
    new = reprice_margin_entries(old, source(None, "verified_no_window_trade"),
                                  participation=.5, broker_multiplier=1.)
    assert new[0, [1, 8, 18, 19]].tolist() == [0., 0., 0., 0.]
    assert new[0, 3] == 1000.  # previous official value, valuation only
    assert new[0, 25] == old[0, 25]  # expiry/terminal volume remains independent


@pytest.mark.parametrize("quantity", [2., -2.])
def test_no_entry_preserves_incoming_inventory_and_real_mark_to_market(quantity):
    new = reprice_margin_entries(execution(), source(None, "verified_no_window_trade"),
                                  participation=.5, broker_multiplier=1.)
    result = run([[0.]], torch.from_numpy(new[None]), initial_quantities=torch.tensor([quantity]))
    assert result.final_weights.item() == quantity
    assert result.final_equity_scale.item() == pytest.approx(1 + quantity * 20 / 1000)


@pytest.mark.parametrize("field,value", [("opening_margin_value_twd", 777.),
                                        ("opening_tax_twd", 20.)])
def test_unknown_margin_or_tax_value_base_is_rejected(field, value):
    with pytest.raises(ValueError, match="unsupported"):
        reprice_margin_entries(execution(), source().with_columns(pl.lit(value).alias(field)),
                               participation=.5, broker_multiplier=1.)


def test_source_gap_and_verified_empty_window_remain_distinct():
    d = date(2026, 1, 2)
    base = pl.DataFrame({"date": [d]*3, "physical_contract": ["A", "B", "C"],
                         "symbol": ["s1", "s2", "s3"], "executable": [True]*3})
    minutes = pl.DataFrame({"date": [d, d], "physical_contract": ["A", "A"],
        "minute": [527, 542], "vwap": [10., 11.], "volume": [2., 4.], "source_file_sha256": ["a"]*2})
    verified = base.head(2).select("date", "physical_contract")
    pair = paired_entries(base, minutes, verified)
    assert pair["08:45"]["status"].to_list() == ["observed_minute", "verified_no_window_trade", "missing_source"]
    assert pair["09:00"]["entry_quote"].to_list() == [11., None, None]
    perturbed = minutes.with_columns(pl.when(pl.col("minute") == 542).then(99.).otherwise(pl.col("vwap")).alias("vwap"))
    assert paired_entries(base, perturbed, verified)["08:45"].equals(pair["08:45"])


def test_inclusive_panel_slice_preserves_fingerprints():
    panel = _stock_panel()
    panel.content_fingerprints = {"features": {"source": "immutable-generation"}}
    sliced = slice_panel_start(panel, "2026-01-03")
    assert sliced.content_fingerprints == panel.content_fingerprints
    np.testing.assert_array_equal(sliced.features, panel.features[1:])


def test_pair_configs_only_change_clock_and_artifact_identity(tmp_path):
    a = load_config("configs/markets/tw_stock_futures_carry_0845_20261021_v1.yaml")
    b = load_config("configs/markets/tw_stock_futures_carry_0900_20261021_v1.yaml")
    assert a.training == b.training
    assert a.data == b.data and a.walk_forward == b.walk_forward
    assert a.training.pretrained_initialization_root is None
    assert a.training.warm_start_from_previous_fold is False
    assert a.training.epochs * a.training.early_stopping_no_improve_ratio == 100
    assert a.training.loss_type == "log_utility"
    assert replace(a.trading, tw_futures_portfolio_decision_time="09:00",
                   tw_futures_portfolio_entry_clock_path=b.trading.tw_futures_portfolio_entry_clock_path) == b.trading
    raw = _load_raw_config("configs/markets/tw_stock_futures_carry_0900_20261021_v1.yaml")
    raw["data"]["tw_futures_current_open_feature"] = True
    path = tmp_path / "invalid.yaml"
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match="prior-only"):
        load_config(path)


def test_wrong_clock_partial_or_modified_entry_source_cannot_be_admitted(tmp_path):
    daily, rules, path = [tmp_path / n for n in ("daily", "rules", "entries.parquet")]
    daily.write_bytes(b"daily"); rules.write_bytes(b"rules")
    entries = pl.DataFrame({"date": [date(2026,1,2)], "physical_contract": ["A"],
        "symbol": ["s1"], "entry_quote": [10.], "entry_volume": [1.], "status": ["observed_minute"]})
    entries.write_parquet(path)
    manifest = {"dataset": CLOCK_DATASET, "clock": decision_clock_contract("08:45"),
        "status": "complete", "entries_sha256": sha256_file(path), "rows": 1,
        "source_daily_sha256": sha256_file(daily), "source_rules_sha256": sha256_file(rules),
        "start_date": "2026-01-02", "end_date": "2026-01-02"}
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    validate_clock_source(path, daily_path=daily, rules_path=rules, decision_time="08:45")
    with pytest.raises(ValueError, match="incompatible"):
        validate_clock_source(path, daily_path=daily, rules_path=rules, decision_time="09:00")
    manifest_path.write_text(json.dumps({**manifest, "status": "partial"}))
    with pytest.raises(ValueError, match="incomplete"):
        validate_clock_source(path, daily_path=daily, rules_path=rules, decision_time="08:45")
    manifest_path.write_text(json.dumps(manifest))
    entries.with_columns(pl.lit(20.).alias("entry_quote")).write_parquet(path)
    with pytest.raises(ValueError, match="incompatible"):
        validate_clock_source(path, daily_path=daily, rules_path=rules, decision_time="08:45")


@pytest.mark.parametrize("with_reference", [False, True])
def test_attached_clock_changes_execution_only_and_crops_all_daily_axes(tmp_path, with_reference):
    from datetime import datetime
    from stockagent.data.tw_futures_decision_clock import (
        OPENING_REFERENCE_DATASET, OPENING_REFERENCE_FEATURES,
        opening_reference_contract, resolve_opening_references,
    )
    from test_tw_futures_margin import rule_panel, write_rules
    from test_tw_futures_margin_corporate import corporate_rules
    from stockagent.data.tw_futures_margin import attach_futures_margin_rules
    panel, rows = rule_panel(tmp_path)
    daily_path = panel.stock_context_futures_portfolio_daily.source_path
    pl.read_parquet(daily_path).with_columns(pl.lit("stock_future").alias("asset_class")).write_parquet(daily_path)
    for row in corporate_rules(rows):
        row.update(contract_multiplier=10., opening_tax_twd=0., known_tax_twd=0.,
            opening_margin_value_twd=1000., settlement_margin_value_twd=1000., known_margin_value_twd=1000.,
            second_position_group="month", second_position_unit=1., second_position_limit=100.)
    rules_path = write_rules(tmp_path, panel, rows)
    rule_meta = rules_path.with_name("manifest.json")
    meta = json.loads(rule_meta.read_text()); meta["schema_version"] = 4
    rule_meta.write_text(json.dumps(meta))
    panel = attach_futures_margin_rules(panel, rules_path)
    prior = panel.stock_context_futures_portfolio_daily
    directory = tmp_path / "0900"; directory.mkdir()
    path = directory / "entries.parquet"
    entries = pl.DataFrame({"date": [date(2026,1,3),date(2026,1,4)],
        "physical_contract": ["TX202601"]*2, "symbol": ["TAIFEX_SLOT_0001"]*2,
        "entry_quote": [105.,None], "entry_volume": [4.,0.],
        "status": ["observed_minute", "verified_no_window_trade"]})
    entries.write_parquet(path)
    path.with_name("manifest.json").write_text(json.dumps({"dataset": CLOCK_DATASET,
        "clock": decision_clock_contract("09:00"), "status": "complete", "rows": 2,
        "entries_sha256": sha256_file(path), "source_daily_sha256": sha256_file(daily_path),
        "source_rules_sha256": sha256_file(rules_path), "start_date": "2026-01-03", "end_date": "2026-01-04",
        "pair_identity": "synthetic-test-only"}))
    reference_path = None
    if with_reference:
        references = resolve_opening_references(entries.select("date", "physical_contract", "symbol"),
            pl.DataFrame({"physical_contract": ["TX202601"], "price": [102.],
                "trade_date": [date(2026,1,2)], "trade_ts": [None],
                "available_by": [datetime(2026,1,2,23,59,59)], "source_sha256": ["a"*64],
                "is_session_open": [False]}), decision_time="09:00")
        reference_path = directory / "opening_reference.parquet"
        references.write_parquet(reference_path)
        reference_path.with_name("opening_reference_manifest.json").write_text(json.dumps({
            "dataset": OPENING_REFERENCE_DATASET, "contract": opening_reference_contract("09:00"),
            "status": "complete", "rows": 2, "data_sha256": sha256_file(reference_path),
            "source_daily_sha256": sha256_file(daily_path), "pair_identity": "synthetic-test-only"}))
    attached = attach_futures_decision_clock(panel, path, decision_time="09:00", opening_reference_path=reference_path)
    new = attached.stock_context_futures_portfolio_daily
    assert len(attached.dates) == 2 and len(new.dates) == 2
    width = prior.candidate_features.shape[-1]
    np.testing.assert_array_equal(new.candidate_features[...,:width], prior.candidate_features[1:])
    assert new.candidate_features.shape[-1] == width + (len(OPENING_REFERENCE_FEATURES) if with_reference else 0)
    if with_reference:
        assert new.candidate_features[:,0,width+1].tolist() == [1.,1.]
        assert new.candidate_features[:,0,width+2].tolist() == [0.,0.]
    np.testing.assert_array_equal(new.candidate_mask, prior.candidate_mask[1:])
    assert new.integer_execution[:,0,3].tolist() == [1050.,1000.]
    assert new.integer_execution[:,0,8].tolist() == [2.,0.]
    assert new.decision_clock_contract["decision_time"] == "09:00"
    assert prior.integer_execution[1,0,3] == 1000.


def test_history_identity_uses_dated_cells_not_generation_string_parsing(tmp_path, monkeypatch):
    import scripts.prepare_tw_futures_decision_clocks as builder
    root = tmp_path / "history"; root.mkdir()
    d = date(2026,1,2)
    bars = pl.DataFrame({"date": [d,d], "physical_contract": ["CAF:202601"]*2,
        "minute": [527,542], "vwap": [100.,101.], "high": [101.,102.], "low": [99.,100.],
        "close": [100.,101.], "volume": [2.,3.], "source_file_sha256": ["raw-test-sha"]*2})
    coverage = pl.DataFrame({"date": [d], "physical_contract": ["CAF:202601"],
        "status": ["minute_verified"], "alias": ["CAFR1"]})
    bars.write_parquet(root / "all_minutes.parquet"); coverage.write_parquet(root / "coverage.parquet")
    (root / "manifest.json").write_text(json.dumps({"dataset": "taifex_stock_futures_minute_history_v2",
        "contract_version": 2, "source_daily_sha256": "original-test-source", "outputs": {
            n: {"file": n+".parquet", "sha256": sha256_file(root / (n+".parquet"))}
            for n in ("all_minutes", "coverage")}}))
    src = pl.DataFrame({"date": [d], "product": ["CAF"], "contract": ["202601"],
        "physical_contract": ["opaque-lifetime-id"], "high": [102.], "low": [99.], "close": [101.], "volume": [5.]})
    checked = []
    def raw_check(roots, row):
        checked.append(row["physical_contract"])
        return True
    monkeypatch.setattr(builder, "_reuse_verified_contract", raw_check)
    minutes, verified, _ = builder.load_verified_history(root, [], src)
    assert checked == ["CAF:202601"]
    assert verified["physical_contract"].to_list() == ["opaque-lifetime-id"]
    assert minutes["physical_contract"].to_list() == ["opaque-lifetime-id"]*2
    with pytest.raises(ValueError, match="ambiguous"):
        builder.load_verified_history(root, [], pl.concat([src, src.with_columns(pl.lit("other-id").alias("physical_contract"))]))


def test_portable_final_retains_raw_and_data_bytes_and_does_not_edit_old_receipt(tmp_path):
    from scripts.stage_tw_futures_margin_sources import materialize_portable_final_settlements
    source = tmp_path / "source"; (source / "final/raw").mkdir(parents=True)
    raw = source / "final/raw/a.html"; raw.write_bytes(b"official-source-test")
    data = source / "final/futures_final_settlement_history.parquet"; data.write_bytes(b"original-parquet-test")
    manifest = source / "final/manifest.json"
    manifest.write_text(json.dumps({"schema_version": 2, "status": "complete",
        "outputs": {"futures_final_settlement_history": {"sha256": sha256_file(data)}},
        "receipts": [{"path": "/retired/original/source.html", "sha256": sha256_file(raw)}]}))
    original = manifest.read_bytes()
    locations = source / "original_locations.json"
    locations.write_text(json.dumps({"final/raw/a.html": "/retired/original/source.html"}))
    (source / "source_manifest.json").write_text(json.dumps({"files": {
        n: {"sha256": sha256_file(source/n)} for n in ("original_locations.json", "final/manifest.json", "final/raw/a.html")}}))
    destination = tmp_path / "portable"
    proof = materialize_portable_final_settlements(source, destination)
    assert proof["data_sha256"] == sha256_file(data)
    assert manifest.read_bytes() == original
    assert (destination / "original_manifest.json").read_bytes() == original
    assert (destination / "raw/a.html").read_bytes() == raw.read_bytes()
    assert json.loads((destination / "manifest.json").read_text())["receipts"][0]["path"] == "raw/a.html"
    raw.write_bytes(b"changed")
    with pytest.raises(ValueError, match="SHA/path"):
        materialize_portable_final_settlements(source, tmp_path / "bad-portable")
