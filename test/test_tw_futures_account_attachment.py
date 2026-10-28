from dataclasses import replace
from datetime import date, timedelta
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import polars as pl
import pytest
import torch

from stockagent.data import tw_futures_margin as margin
from stockagent.data import tw_futures_account_attachment as attachment
from test_tw_futures_margin import rule_panel, tape
from test_tw_stock_context_futures_portfolio import _stock_panel


def _fixture(tmp_path, monkeypatch, *, event_capacity=0.):
    full_panel, _ = rule_panel(tmp_path)
    source = full_panel.stock_context_futures_portfolio_daily.source_path
    pl.read_parquet(source).with_columns(
        pl.Series("accounting_only_date", [False, True, False]),
        pl.lit("TX").alias("product"), pl.lit(False).alias("cash_settlement"),
    ).write_parquet(source)
    clock_path = tmp_path / "entries.parquet"
    (tmp_path / "manifest.json").write_text(json.dumps({
        "start_date": "2026-01-02", "end_date": "2026-01-04",
    }))
    trading = SimpleNamespace(execution_mode="tw_stock_context_futures_portfolio",
        tw_futures_portfolio_capital_basis="initial_margin", tw_futures_portfolio_holding_policy="carry",
        tw_futures_portfolio_decision_time="08:45", tw_futures_portfolio_entry_clock_path=str(clock_path),
        tw_futures_portfolio_data_path=source)
    config = SimpleNamespace(trading=trading)
    market_panel = replace(_stock_panel(rows=2), dates=full_panel.dates[[0, 2]])

    def attach(axes, supplied_config, *, futures_account_calendar):
        assert supplied_config is config
        assert not hasattr(axes, "features")
        assert futures_account_calendar.account_only_mask.tolist() == [False, True, False]
        execution = tape(rows=3, slots=1936).numpy()
        execution[1, :, 1] = 0
        execution[1, :, 8] = event_capacity
        execution[1, 0, 2] = 1
        execution[1, 0, margin.CASH_SETTLEMENT] = 1
        daily = replace(full_panel.stock_context_futures_portfolio_daily,
            integer_execution=execution, margin_contract_version=1)
        return replace(axes, stock_context_futures_portfolio_daily=daily)

    monkeypatch.setattr(attachment, "attach_configured_stock_context_futures_portfolio", attach)
    return market_panel, config


def test_attachment_keeps_real_decisions_and_separate_account_tape(tmp_path, monkeypatch):
    from stockagent.config import load_config
    from stockagent.training.checkpoint_contract import build_checkpoint_manifest
    from stockagent.training.dataset import CrossSectionalDataset

    panel, config = _fixture(tmp_path, monkeypatch)
    result = attachment.attach_configured_futures_account_calendar(panel, config)
    daily = result.stock_context_futures_portfolio_daily
    assert result.features is panel.features
    assert result.dates.tolist() == [date(2026, 1, 2), date(2026, 1, 4)]
    assert daily.dates.tolist() == result.dates.tolist()
    assert daily.integer_execution.shape[0] == 2
    assert daily.account_only_integer_execution.shape[0] == 1
    assert daily.futures_account_calendar.dates.tolist()[1] == date(2026, 1, 3)
    assert daily.account_only_integer_execution[0, 0, margin.CASH_SETTLEMENT] == 1
    with pytest.raises(ValueError, match="settlement tape cannot be discarded"):
        CrossSectionalDataset(result, np.arange(2), lookback=1,
                              execution_mode="tw_stock_context_futures_portfolio")
    real_config = load_config(
        "configs/markets/tw_stock_context_all_futures_portfolio_multi_basis_projection_l1.yaml"
    )
    with pytest.raises(ValueError, match="not yet part of the checkpoint"):
        build_checkpoint_manifest(result, real_config, include_data_content=False)
    orphan_events = replace(result, stock_context_futures_portfolio_daily=replace(
        daily, futures_account_calendar=None))
    with pytest.raises(ValueError, match="not yet part of the checkpoint"):
        build_checkpoint_manifest(orphan_events, real_config, include_data_content=False)


def test_dated_checkpoint_pins_event_tape_and_rejects_legacy_resume(tmp_path, monkeypatch):
    from downloader.artifact_io import sha256_file
    from stockagent.config import load_config
    from stockagent.data.tw_futures_decision_clock import CLOCK_DATASET, decision_clock_contract
    from stockagent.training.checkpoint_contract import (
        build_checkpoint_manifest, validate_checkpoint_manifest,
    )

    panel, config = _fixture(tmp_path, monkeypatch)
    panel = attachment.attach_configured_futures_account_calendar(panel, config)
    clock = Path(config.trading.tw_futures_portfolio_entry_clock_path)
    clock.write_bytes(b"checkpoint clock fixture")
    clock.with_name("manifest.json").write_text(json.dumps({
        "dataset": CLOCK_DATASET, "status": "complete",
        "clock": decision_clock_contract("08:45"), "entries_sha256": sha256_file(clock),
    }))
    real = load_config("configs/markets/tw_stock_context_all_futures_portfolio_multi_basis_projection_l1.yaml")
    trading = replace(real.trading, tw_futures_account_calendar_contract="dated_futures_account_calendar_v1",
        tw_futures_portfolio_capital_basis="initial_margin", tw_futures_portfolio_holding_policy="carry",
        tw_futures_portfolio_integer_contracts=True, tw_futures_portfolio_decision_time="08:45",
        tw_futures_portfolio_entry_clock_path=str(clock))
    active = replace(real, trading=trading)
    current = build_checkpoint_manifest(panel, active)
    contract = current["contracts"]["data"]
    identity = contract["panel_arrays"]["stock_context_futures_account_source"]
    assert contract["futures_account_calendar_contract"] == "dated_futures_account_calendar_v1"
    assert len(identity["calendar_fingerprint"]) == len(identity["source_fingerprint"]) == 64
    assert (current["contracts"]["trading"]["futures_margin_contract"]["risk_clock"]
            == "dated_futures_account_events_and_stock_decisions")
    validate_checkpoint_manifest({"experiment_manifest": current}, current,
                                 checkpoint_path=tmp_path / "current.pt", scope="resume")

    daily = panel.stock_context_futures_portfolio_daily
    altered = daily.account_only_integer_execution.copy()
    altered[0, 0, margin.TERMINAL_MARK] += 1
    changed = replace(panel, stock_context_futures_portfolio_daily=replace(
        daily, account_only_integer_execution=altered))
    changed_manifest = build_checkpoint_manifest(changed, active)
    assert (changed_manifest["contracts"]["data"]["panel_arrays"]["stock_context_futures_account_source"]
            ["source_fingerprint"] != identity["source_fingerprint"])
    with pytest.raises(RuntimeError, match="data"):
        validate_checkpoint_manifest({"experiment_manifest": current}, changed_manifest,
                                     checkpoint_path=tmp_path / "current.pt", scope="resume")

    legacy_panel = replace(panel, stock_context_futures_portfolio_daily=replace(
        daily, futures_account_calendar=None, account_only_integer_execution=None))
    legacy_config = replace(active, trading=replace(trading, tw_futures_account_calendar_contract=None))
    old = build_checkpoint_manifest(legacy_panel, legacy_config)
    for scope in ("resume", "artifact", "inference", "model"):
        with pytest.raises(RuntimeError, match="calendar contract differs"):
            validate_checkpoint_manifest({"experiment_manifest": old}, current,
                                         checkpoint_path=tmp_path / "old.pt", scope=scope)
        with pytest.raises(RuntimeError, match="calendar contract differs"):
            validate_checkpoint_manifest({"experiment_manifest": current}, old,
                                         checkpoint_path=tmp_path / "new.pt", scope=scope)
    with pytest.raises(RuntimeError, match="lacks the dated futures account"):
        validate_checkpoint_manifest({}, current, checkpoint_path=tmp_path / "legacy.pt", scope="resume")
    with pytest.raises(ValueError, match="requires its explicit calendar"):
        build_checkpoint_manifest(legacy_panel, active, include_data_content=False)


def test_dated_calendar_config_requires_complete_margin_carry_contract(tmp_path):
    import yaml
    from stockagent.config import _load_raw_config, load_config

    raw = _load_raw_config("configs/markets/tw_stock_futures_carry_0845_20261021_v1.yaml")
    raw["trading"]["tw_futures_account_calendar_contract"] = "dated_futures_account_calendar_v1"
    path = tmp_path / "experiment.yaml"

    def read():
        path.write_text(yaml.safe_dump(raw))
        return load_config(path)

    assert read().trading.tw_futures_account_calendar_contract == "dated_futures_account_calendar_v1"
    for group, key, bad in (
        ("trading", "tw_futures_account_calendar_contract", "unknown"),
        ("trading", "tw_futures_portfolio_holding_policy", "intraday"),
        ("trading", "tw_futures_portfolio_capital_basis", "notional"),
        ("trading", "tw_futures_portfolio_integer_contracts", False),
        ("trading", "tw_futures_portfolio_entry_clock_path", None),
        ("training", "loss_type", "sharpe"),
    ):
        original = raw[group][key]
        raw[group][key] = bad
        with pytest.raises(ValueError, match="futures account calendar|dated futures account calendar"):
            read()
        raw[group][key] = original


@pytest.mark.parametrize("event_capacity", [1., float("nan")])
def test_attachment_rejects_market_capacity_on_account_only_node(tmp_path, monkeypatch, event_capacity):
    panel, config = _fixture(tmp_path, monkeypatch, event_capacity=event_capacity)
    with pytest.raises(ValueError, match="no executable order capacity"):
        attachment.attach_configured_futures_account_calendar(panel, config)


@pytest.mark.parametrize("bad_executable", [False, True])
def test_real_attachment_chain_distinguishes_prior_volume_from_account_day_orders(tmp_path, bad_executable):
    from downloader.artifact_io import sha256_file
    from stockagent.config import load_config
    from stockagent.data.tw_futures_decision_clock import EQUITY_CLOCK_DATASET, decision_clock_contract
    from stockagent.data.tw_futures_portfolio_daily import FUTURES_MODEL_FEATURE_COLUMNS
    from stockagent.research.taifex_transaction_tax import futures_transaction_tax_rate
    from test_tw_futures_margin import write_rules
    from test_tw_futures_margin_corporate import mixed_corporate_source
    from train import _attach_configured_stock_context_futures_panel

    full, daily_path, old_rules = mixed_corporate_source(tmp_path, "DL2")
    source = pl.read_parquet(daily_path).with_columns(
        (pl.col("date") == date(2026, 1, 3)).alias("accounting_only_date"),
        pl.when(pl.col("date") == date(2026, 1, 3)).then(pl.lit(bad_executable))
        .otherwise(pl.col("executable")).alias("executable"),
        pl.lit(False).alias("cash_settlement"),
        pl.when(pl.col("date") == date(2026, 1, 2)).then(11.)
        .when(pl.col("date") == date(2026, 1, 3)).then(999.)
        .otherwise(22.).alias("taifex_product_id"),
    )
    source.write_parquet(daily_path)
    daily_manifest_path = daily_path.with_name("manifest.json")
    daily_manifest = json.loads(daily_manifest_path.read_text())
    daily_manifest["outputs"]["continuous_daily"]["sha256"] = sha256_file(daily_path)
    daily_manifest_path.write_text(json.dumps(daily_manifest))
    rules = pl.read_parquet(old_rules).to_dicts()
    for row in rules:
        value = row["opening_contract_value_twd"]
        tax_class = "stock_index" if row["position_group"] == "TX" else "stock"
        tax = float(np.floor(100. * row["contract_multiplier"]
                             * futures_transaction_tax_rate(row["date"], tax_class=tax_class) + .5))
        row.update(opening_tax_twd=tax, known_tax_twd=tax,
                   opening_margin_value_twd=value, settlement_margin_value_twd=value,
                   known_margin_value_twd=value, second_position_group=row["position_group"],
                   second_position_unit=1., second_position_limit=100.)
    rules_path = write_rules(tmp_path, full, rules)
    rules_manifest_path = rules_path.with_name("manifest.json")
    rules_manifest = json.loads(rules_manifest_path.read_text())
    rules_manifest["schema_version"] = 4
    rules_manifest_path.write_text(json.dumps(rules_manifest))
    clock_dir = tmp_path / "clock"
    clock_dir.mkdir()
    entries_path = clock_dir / "entries.parquet"
    entries = source.select("date", "physical_contract", "symbol", "accounting_only_date").with_columns(
        pl.when(pl.col("accounting_only_date")).then(None).otherwise(100.).alias("entry_quote"),
        pl.when(pl.col("accounting_only_date")).then(0.).otherwise(100.).alias("entry_volume"),
        pl.when(pl.col("accounting_only_date")).then(pl.lit("daily_not_executable"))
        .otherwise(pl.lit("observed_minute")).alias("status"),
    ).drop("accounting_only_date")
    entries.write_parquet(entries_path)
    (clock_dir / "manifest.json").write_text(json.dumps({
        "dataset": EQUITY_CLOCK_DATASET, "clock": decision_clock_contract("08:45", asset_scope="twd_equity"),
        "status": "complete", "rows": entries.height, "entries_sha256": sha256_file(entries_path),
        "source_daily_sha256": sha256_file(daily_path), "source_rules_sha256": sha256_file(rules_path),
        "start_date": "2026-01-02", "end_date": "2026-01-04", "pair_identity": "synthetic-only",
    }))
    real = load_config("configs/markets/tw_stock_futures_carry_0845_20261021_v1.yaml")
    model = replace(real.training.financial_transformer, futures_causal_account_policy=False,
                    futures_margin_amount_context=False, futures_beta_notional_cap=None,
                    futures_residual_signal_context=False)
    config = replace(real,
        data=replace(real.data, tw_futures_portfolio_slot_count=1936,
                     tw_futures_expiry_settlement_valuation=False),
        training=replace(real.training, financial_transformer=model),
        trading=replace(real.trading, tw_futures_account_calendar_contract="dated_futures_account_calendar_v1",
            tw_futures_portfolio_data_path=str(daily_path), tw_futures_portfolio_margin_rules_path=str(rules_path),
            tw_futures_portfolio_entry_clock_path=str(entries_path),
            tw_futures_portfolio_benchmark_mode="legacy_front_holding_return"))
    stock = replace(_stock_panel(rows=4), dates=np.array(
        ["2026-01-01", "2026-01-02", "2026-01-04", "2026-01-05"], dtype="datetime64[D]"))
    if bad_executable:
        with pytest.raises(ValueError, match="account-only source rows cannot be executable"):
            _attach_configured_stock_context_futures_panel(stock, config)
        return
    attached = _attach_configured_stock_context_futures_panel(stock, config)
    result = attached.stock_context_futures_portfolio_daily
    assert attached.dates.tolist() == [date(2026, 1, 2), date(2026, 1, 4)]
    assert source.filter(pl.col("accounting_only_date"))["previous_volume"].to_list() == [100., 100.]
    assert np.all(result.account_only_integer_execution[..., 1] == 0)
    assert np.all(result.account_only_integer_execution[..., 8] == 0)
    feature = FUTURES_MODEL_FEATURE_COLUMNS.index("taifex_product_id")
    assert result.candidate_features[1, 1, feature] == 11.
    assert result.futures_account_calendar.account_only_mask.tolist() == [False, True, False]
    from stockagent.training.checkpoint_contract import build_checkpoint_manifest
    from stockagent.training.trainer import _build_execution_runtime
    runtime = _build_execution_runtime(attached, config, torch.device("cpu"))
    manifest = build_checkpoint_manifest(attached, config)
    assert (manifest["contracts"]["data"]["panel_arrays"]["stock_context_futures_account_source"]
            ["source_fingerprint"] == runtime.futures_account_source.fingerprint)


def test_attachment_rejects_unreviewed_nonexempt_holiday_account_mark(tmp_path, monkeypatch):
    panel, config = _fixture(tmp_path, monkeypatch)
    path = config.trading.tw_futures_portfolio_data_path
    frame = pl.read_parquet(path).with_columns(
        pl.Series("date", [date(2024, 2, 5), date(2024, 2, 6), date(2024, 2, 15)]),
        pl.lit("TGF").alias("product"))
    frame.write_parquet(path)
    dates = np.array(["2024-02-05", "2024-02-15"], dtype="datetime64[D]")
    with pytest.raises(ValueError, match="holiday nonterminal valuations"):
        attachment._reject_unreviewed_holiday_valuations(path, dates)
    # No unverified historical event lies in this clipped decision horizon.
    attachment._reject_unreviewed_holiday_valuations(
        path, np.array(["2024-02-08", "2024-02-15"], dtype="datetime64[D]"))


@pytest.mark.parametrize("day,product", [
    (date(2014, 1, 28), "TX"),
    (date(2024, 2, 6), "TX"),
    (date(2024, 2, 6), "PK1"),
])
def test_attachment_requires_review_for_all_holiday_product_classes(tmp_path, day, product):
    path = tmp_path / "account.parquet"
    pl.DataFrame({"date": [day], "accounting_only_date": [True], "product": [product],
                  "cash_settlement": [False]}).write_parquet(path)
    dates = np.array([str(day - timedelta(days=1)), str(day + timedelta(days=1))], dtype="datetime64[D]")
    with pytest.raises(ValueError, match="holiday nonterminal valuations"):
        attachment._reject_unreviewed_holiday_valuations(path, dates)
    # Terminal cash is checked by the separate final-settlement contract.
    pl.read_parquet(path).with_columns(pl.lit(True).alias("cash_settlement")).write_parquet(path)
    attachment._reject_unreviewed_holiday_valuations(path, dates)


def test_attachment_requires_all_broker_holiday_days_and_rejects_market_conflict():
    dates = np.array(["2024-02-05", "2024-02-15"], dtype="datetime64[D]")
    days = pl.DataFrame({"date": [date(2024, 2, 5), date(2024, 2, 6), date(2024, 2, 15)],
                         "accounting_only_date": [False, True, False]})
    with pytest.raises(ValueError, match="2024-02-07"):
        attachment._reject_missing_holiday_account_dates(days, dates)
    complete = pl.concat([days, pl.DataFrame({"date": [date(2024, 2, 7)],
                                                "accounting_only_date": [True]})])
    attachment._reject_missing_holiday_account_dates(complete, dates)
    invalid = complete.with_columns(
        pl.when(pl.col("date") == date(2024, 2, 7)).then(False)
        .otherwise(pl.col("accounting_only_date")).alias("accounting_only_date"))
    with pytest.raises(ValueError, match="invalid=.*2024-02-07"):
        attachment._reject_missing_holiday_account_dates(invalid, dates)
    attachment._reject_missing_holiday_account_dates(
        days, np.array(["2024-02-08", "2024-02-15"], dtype="datetime64[D]"))


@pytest.mark.parametrize("before,after", [
    ("2011-01-28", "2011-02-08"), ("2012-01-18", "2012-01-30"),
])
def test_attachment_rejects_unreviewed_early_holiday_crossing(tmp_path, monkeypatch, before, after):
    panel, config = _fixture(tmp_path, monkeypatch)
    dates = np.asarray([before, after], dtype="datetime64[D]")
    panel = replace(panel, dates=dates)
    clock = Path(config.trading.tw_futures_portfolio_entry_clock_path).with_name("manifest.json")
    clock.write_text(json.dumps({"start_date": before, "end_date": after}))
    with pytest.raises(ValueError, match="requires source-bound 2011/2012"):
        attachment.attach_configured_futures_account_calendar(panel, config)
    # A clipped interval that does not cross the holiday needs no event from it.
    attachment._reject_unreviewed_early_holiday_calendars(dates[:1])
    attachment._reject_unreviewed_early_holiday_calendars(dates[1:])


def test_dated_dataset_binds_same_event_source_through_window_operations(tmp_path, monkeypatch):
    from stockagent.training.dataset import CrossSectionalDataset
    from stockagent.training.futures_account_bridge import (
        PreparedFuturesAccountSource, verify_futures_account_binding,
    )
    from stockagent.training.trainer import (
        _combine_datasets_to_windowed, _pad_windowed_training_split, _windowed_base_compatible,
    )
    from stockagent.training.windowed import dataset_to_windowed_tensors

    panel, config = _fixture(tmp_path, monkeypatch)
    panel = attachment.attach_configured_futures_account_calendar(panel, config)
    daily = panel.stock_context_futures_portfolio_daily
    source = PreparedFuturesAccountSource.from_daily(daily, panel.dates, prepare_execution=True)
    dataset = CrossSectionalDataset(
        panel, np.arange(2), lookback=1, execution_mode="tw_stock_context_futures_portfolio",
        futures_account_source=source,
    )
    split = dataset_to_windowed_tensors(dataset)
    assert split.futures_account_source_fingerprint == source.fingerprint
    assert split.futures_execution_release_fingerprint == source.execution_release_fingerprint
    assert split.overnight_log_returns.shape[1] == 1936
    for changed in (
        split.to_device_cache(torch.device("cpu")),
        _pad_windowed_training_split(split, batch_size=2),
        split.subset_symbols(torch.tensor([0])),
        split.pad_symbols(3),
    ):
        assert changed.futures_account_source_fingerprint == source.fingerprint
        assert changed.futures_execution_release_fingerprint == source.execution_release_fingerprint
        assert changed.overnight_log_returns.shape[1] == 1936
        torch.testing.assert_close(changed.overnight_log_returns, split.overnight_log_returns)
        verify_futures_account_binding(changed, source)
        with pytest.raises(ValueError, match="cannot run without"):
            verify_futures_account_binding(changed, None)

    other_events = daily.account_only_integer_execution.copy()
    other_events[0, 0, margin.TERMINAL_MARK] += 1
    other_daily = replace(daily, account_only_integer_execution=other_events)
    other = PreparedFuturesAccountSource.from_daily(other_daily, panel.dates, prepare_execution=True)
    with pytest.raises(ValueError, match="differs from the attached event tape"):
        CrossSectionalDataset(
            panel, np.arange(2), lookback=1, execution_mode="tw_stock_context_futures_portfolio",
            futures_account_source=other,
        )
    with pytest.raises(ValueError, match="event sources differ"):
        verify_futures_account_binding(split, other)

    changed_market = daily.integer_execution.copy()
    changed_market[0, 0, 3] += 1
    different_execution = PreparedFuturesAccountSource.from_daily(
        replace(daily, integer_execution=changed_market), panel.dates, prepare_execution=True,
    )
    assert different_execution.fingerprint == source.fingerprint
    assert different_execution.execution_release_fingerprint != source.execution_release_fingerprint
    with pytest.raises(ValueError, match="decision releases differ"):
        verify_futures_account_binding(split, different_execution)
    other_panel = replace(panel, stock_context_futures_portfolio_daily=replace(
        daily, integer_execution=changed_market))
    other_dataset = CrossSectionalDataset(
        other_panel, np.arange(2), lookback=1, execution_mode="tw_stock_context_futures_portfolio",
        futures_account_source=different_execution,
    )
    other_split = dataset_to_windowed_tensors(other_dataset)
    assert not _windowed_base_compatible(split, other_split)
    with pytest.raises(ValueError, match="same futures execution release"):
        _combine_datasets_to_windowed([dataset, other_dataset])

    altered_split = replace(split, overnight_log_returns=split.overnight_log_returns.clone())
    altered_split.overnight_log_returns[0, 0, 3] += 1
    with pytest.raises(ValueError, match="split decision execution was modified"):
        verify_futures_account_binding(altered_split, source)
    with pytest.raises(ValueError, match="split decision execution was modified"):
        altered_split.to_device_cache(torch.device("cpu"))


def test_runtime_owns_dated_source_and_binds_it_to_dataset(tmp_path, monkeypatch):
    from stockagent.config import load_config
    from stockagent.training.dataset import CrossSectionalDataset
    from stockagent.training.trainer import _build_execution_runtime
    from stockagent.training.windowed import dataset_to_windowed_tensors

    panel, attachment_config = _fixture(tmp_path, monkeypatch)
    panel = attachment.attach_configured_futures_account_calendar(panel, attachment_config)
    config = load_config(
        "configs/markets/tw_stock_context_all_futures_portfolio_multi_basis_projection_l1.yaml"
    )
    config = replace(config, trading=replace(config.trading, tw_futures_portfolio_integer_contracts=True))
    runtime = _build_execution_runtime(panel, config, torch.device("cpu"))
    assert runtime.futures_account_source is not None
    dataset = CrossSectionalDataset(
        panel, np.arange(2), lookback=1, execution_mode=config.trading.execution_mode,
        futures_account_source=runtime.futures_account_source,
    )
    split = dataset_to_windowed_tensors(dataset)
    source = runtime.futures_account_source
    from stockagent.training.futures_account_bridge import futures_account_execution_fingerprint
    assert runtime.futures_execution_release_fingerprint == futures_account_execution_fingerprint(
        panel.stock_context_futures_portfolio_daily, source)
    assert split.futures_account_source_fingerprint == source.fingerprint
    assert dataset.overnight_log_returns_t.data_ptr() == source.decision_execution.data_ptr()
    second_dataset = CrossSectionalDataset(
        panel, np.arange(2), lookback=1, execution_mode=config.trading.execution_mode,
        futures_account_source=source,
    )
    assert second_dataset.overnight_log_returns_t.data_ptr() == dataset.overnight_log_returns_t.data_ptr()
    # Editing the attached panel after preparation cannot silently change the
    # decision tape consumed by an already pinned runtime or a later split.
    original = source.decision_execution[0, 0, 3].item()
    panel.stock_context_futures_portfolio_daily.integer_execution[0, 0, 3] += 1
    assert dataset.overnight_log_returns_t[0, 0, 3].item() == original
    assert second_dataset.overnight_log_returns_t[0, 0, 3].item() == original
    third_dataset = CrossSectionalDataset(
        panel, np.arange(2), lookback=1, execution_mode=config.trading.execution_mode,
        futures_account_source=source,
    )
    assert third_dataset.overnight_log_returns_t.data_ptr() == source.decision_execution.data_ptr()
    assert third_dataset.overnight_log_returns_t[0, 0, 3].item() == original
    source.verify_identity()
    # With causal lookback one, the first panel row provides history and the
    # only decision retains the intervening settlement event.
    batch = source.prepare_batch(split, 0, 1)
    assert batch.calendar.account_only_mask.tolist() == [True, False]
    assert batch.account_execution[0, 0, margin.CASH_SETTLEMENT] == 1

    daily = panel.stock_context_futures_portfolio_daily
    malformed = replace(panel, stock_context_futures_portfolio_daily=replace(
        daily, account_only_integer_execution=None))
    with pytest.raises(ValueError, match="both calendar and event tape"):
        _build_execution_runtime(malformed, config, torch.device("cpu"))
    non_integer = replace(config, trading=replace(config.trading, tw_futures_portfolio_integer_contracts=False))
    with pytest.raises(ValueError, match="whole-contract futures portfolio executor"):
        _build_execution_runtime(panel, non_integer, torch.device("cpu"))

    legacy = replace(panel, stock_context_futures_portfolio_daily=replace(
        daily, futures_account_calendar=None, account_only_integer_execution=None))
    assert _build_execution_runtime(legacy, config, torch.device("cpu")).futures_account_source is None


def test_windowed_evaluation_uses_pinned_execution_without_staging_duplicate_tape(tmp_path, monkeypatch):
    from stockagent.config import load_config
    from stockagent.training import trainer
    from stockagent.training.dataset import CrossSectionalDataset
    from stockagent.training.windowed import dataset_to_windowed_tensors

    monkeypatch.setenv("STOCKAGENT_EVAL_BACKTEST_COMPILE", "0")
    panel, attachment_config = _fixture(tmp_path, monkeypatch)
    panel = attachment.attach_configured_futures_account_calendar(panel, attachment_config)
    config = load_config("configs/markets/tw_stock_context_all_futures_portfolio_multi_basis_projection_l1.yaml")
    config = replace(config, trading=replace(config.trading, tw_futures_portfolio_integer_contracts=True))
    runtime = trainer._build_execution_runtime(panel, config, torch.device("cpu"))
    dataset = CrossSectionalDataset(
        panel, np.arange(2), lookback=1, execution_mode=config.trading.execution_mode,
        futures_account_source=runtime.futures_account_source,
    )
    split = dataset_to_windowed_tensors(dataset)

    class FlatPolicy(torch.nn.Module):
        def forward(self, x, mask, portfolio_context=None):
            assert portfolio_context is not None
            return torch.zeros((x.shape[0], 1936), device=x.device)

    moved_fields = []
    move = trainer._move_windowed_batch_to_device

    def observe_move(batch, device, non_blocking):
        moved_fields.append(set(batch))
        return move(batch, device, non_blocking)

    monkeypatch.setattr(trainer, "_move_windowed_batch_to_device", observe_move)
    result, _, _ = trainer._evaluate_windowed_tensor_batch_decoupled(
        FlatPolicy(), None, split, torch.device("cpu"), None, False, False, 0., 0.,
        0., 1., 0., 1, 1, portfolio_activation="pre_normalized",
        compute_ic=False, compute_metrics_summary=True, return_weights_history=True,
        execution_runtime=runtime,
    )
    assert moved_fields and all("overnight_log_returns" not in fields for fields in moved_fields)
    assert (result.futures_account_calendar_result.execution_release_fingerprint
            == runtime.futures_account_source.execution_release_fingerprint)
    expected = runtime.futures_account_source.prepare_batch(split, 0, len(split)).calendar
    assert result.futures_account_calendar_result.calendar.dates.tolist() == expected.dates.tolist()
    assert result.futures_account_calendar_result.calendar.account_only_mask.tolist() == [True, False]


def test_dated_dataset_retains_liquidation_row_from_pinned_execution(tmp_path, monkeypatch):
    from stockagent.training.dataset import CrossSectionalDataset
    from stockagent.training.futures_account_bridge import PreparedFuturesAccountSource

    panel, config = _fixture(tmp_path, monkeypatch)
    panel = attachment.attach_configured_futures_account_calendar(panel, config)
    daily = panel.stock_context_futures_portfolio_daily
    daily.executable_mask[:] = False
    daily.must_liquidate_mask[1, 0] = True
    source = PreparedFuturesAccountSource.from_daily(daily, panel.dates, prepare_execution=True)
    daily.must_liquidate_mask[1, 0] = False
    dataset = CrossSectionalDataset(
        panel, np.arange(2), lookback=1, execution_mode="tw_stock_context_futures_portfolio",
        futures_account_source=source,
    )
    assert dataset.valid_indices.tolist() == [1]
    assert dataset.overnight_log_returns_t[1, 0, 2] == 1


def test_dated_dataset_pins_margin_session_admission_with_release(tmp_path, monkeypatch):
    from stockagent.training.dataset import CrossSectionalDataset
    from stockagent.training.futures_account_bridge import (
        PreparedFuturesAccountSource, futures_account_execution_fingerprint,
    )

    panel, config = _fixture(tmp_path, monkeypatch)
    panel = attachment.attach_configured_futures_account_calendar(panel, config)
    original_daily = panel.stock_context_futures_portfolio_daily
    market = original_daily.integer_execution.copy()
    market[..., 1] = 0  # A real account day can still settle without a fill.
    daily = replace(original_daily, integer_execution=market,
                    margin_session_mask=np.array([False, True], dtype=bool))
    panel = replace(panel, stock_context_futures_portfolio_daily=daily)
    source = PreparedFuturesAccountSource.from_daily(daily, panel.dates, prepare_execution=True)
    assert futures_account_execution_fingerprint(daily, source) == source.execution_release_fingerprint
    daily.margin_session_mask[1] = False
    assert futures_account_execution_fingerprint(daily, source) != source.execution_release_fingerprint
    dataset = CrossSectionalDataset(
        panel, np.arange(2), lookback=1, execution_mode="tw_stock_context_futures_portfolio",
        futures_account_source=source,
    )
    assert dataset.valid_indices.tolist() == [1]
    assert source.decision_session_mask.tolist() == [False, True]
    source.decision_session_mask.setflags(write=True)
    source.decision_session_mask[1] = False
    with pytest.raises(ValueError, match="decision session mask was modified"):
        source.verify_identity()
