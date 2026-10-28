from datetime import date

import polars as pl
import pytest
import numpy as np

from scripts.audit_tw_futures_universe_coverage import audit_actual_membership, audit_universe


def test_source_inventory_keeps_unknown_volume_and_all_requested_codes():
    from scripts.audit_tw_futures_universe_coverage import audit_source_inventory
    days = [date(2011, 8, 1)] * 3
    raw = pl.DataFrame(dict(date=days, product=['NULL', 'ZERO', 'PRINT'], contract=['201108'] * 3,
        session=['一般'] * 3, volume=[None, 0., 3.], open=[None, None, 10.], high=[None, None, 10.],
        low=[None, None, 10.], close=[None, None, 10.], settlement=[10.] * 3))
    requested = pl.DataFrame(dict(product=['NULL', 'ZERO', 'PRINT', 'ABSENT']))
    daily = pl.DataFrame(dict(date=[days[0]], product=['ZERO'], executable=[False]))
    flags = pl.DataFrame(dict(date=[days[0]], product=['PRINT'], physical_contract=['PRINT:201108'],
        is_warmup=[False], has_blocker=[True], missing_valuation=[True]))
    summary, rows = audit_source_inventory(requested, raw, daily, flags, start='2011-01-01', end='2011-12-31')
    assert rows.height == 4 and summary['codes_with_positive_trade_evidence'] == 1
    assert summary['external_limits_proven'] is False
    indexed = {r['product']: r for r in rows.to_dicts()}
    assert indexed['NULL']['raw_null_volume_rows'] == 1
    assert indexed['NULL']['raw_explicit_zero_volume_rows'] == 0
    assert indexed['ZERO']['raw_explicit_zero_volume_rows'] == 1
    assert indexed['ABSENT']['source_status'] == 'no_source_row_in_horizon'
    assert indexed['PRINT']['source_status'] == 'positive_source_not_admitted'
    assert indexed['PRINT']['blocked_missing_valuation'] == 1
    with pytest.raises(ValueError, match='unique requested'):
        audit_source_inventory(pl.concat([requested, requested.head(1)]), raw, daily, flags,
            start='2011-01-01', end='2011-12-31')


def test_configured_audit_uses_canonical_cropped_clock_dates(tmp_path, monkeypatch):
    import json
    from types import SimpleNamespace
    import stockagent.config as config_module
    import stockagent.data.tw_stock_context_futures_portfolio as owner
    from scripts.audit_tw_futures_universe_coverage import audit_configured_membership

    axes = tmp_path / "axes.json"
    axes.write_text(json.dumps(dict(dates=["2026-02-27", "2026-03-02", "2026-03-03"], symbols=["stock"])))
    cfg_path = tmp_path / "config.yaml"
    cfg_path.write_text("test_config: true\n")
    rules = tmp_path / "rules.parquet"
    rules.write_bytes(b"rules identity")
    daily = tmp_path / "daily.parquet"
    pl.DataFrame(dict(date=[date(2026, 3, 2), date(2026, 3, 3)],
        physical_contract=["old:1", "new:1"],
        symbol=["TAIFEX_SLOT_0001", "TAIFEX_SLOT_0001"], product=["OLD", "NEW"])).write_parquet(daily)
    # DataConfig has no panel_end_date. The canonical entry adapter crops the
    # earlier context only after preparing the full prior/corporate history.
    config = SimpleNamespace(data=SimpleNamespace(factorized_feature_manifest=axes, panel_start_date="2026-02-27"),
        trading=SimpleNamespace(tw_futures_portfolio_benchmark_mode="other", tw_futures_portfolio_margin_rules_path=rules))
    monkeypatch.setattr(config_module, "load_config", lambda _: config)

    def attach(panel, config, **kwargs):
        assert len(panel.dates) == 3
        execution = np.ones((2, 1, 11), dtype=np.float32)
        payload = SimpleNamespace(source_path=daily, candidate_mask=np.ones((2, 1), dtype=bool),
            candidate_features=None, integer_execution=execution, holding_log_returns=None,
            executable_mask=None, must_liquidate_mask=None, can_hold_overnight_mask=None,
            benchmark_log_returns=None)
        return SimpleNamespace(dates=panel.dates[1:], stock_context_futures_portfolio_daily=payload)

    monkeypatch.setattr(owner, "attach_configured_stock_context_futures_portfolio", attach)
    result = audit_configured_membership(cfg_path, tmp_path / "output", test_start="2026-03-02", test_end="2026-03-03")
    assert result["periods"]["history"]["sessions"] == 2
    assert result["periods"]["test"]["candidate_product_union"] == 2
    assert result["periods"]["test"]["candidate_product_intersection"] == 0


def test_actual_mask_counts_union_without_requiring_survival():
    dates = np.array(["2026-03-01", "2026-03-02", "2026-03-03"], dtype="datetime64[D]")
    mask = np.array([[True, False], [False, False], [False, True]])
    source = pl.DataFrame({"date": dates, "physical_contract": ["old:1", "old:1", "new:1"],
        "symbol": ["TAIFEX_SLOT_0001", "TAIFEX_SLOT_0001", "TAIFEX_SLOT_0002"],
        "product": ["OLD", "OLD", "NEW"]}).with_columns(pl.col("date").cast(pl.Date))
    summary, rows = audit_actual_membership(dates, mask, source,
        test_start="2026-03-01", test_end="2026-03-03")
    period = summary["periods"]["test"]
    assert period == dict(sessions=3, candidate_product_union=2, candidate_product_intersection=0,
        partial_period_candidates=2, daily_minimum=0, daily_maximum=1)
    assert summary["no_candidate_outside_dated_source"]
    mask[1, 1] = True
    with pytest.raises(ValueError, match="outside dated source"):
        audit_actual_membership(dates, mask, source, test_start="2026-03-01", test_end="2026-03-03")


def test_historical_adjusted_codes_are_not_contemporary_tradable_products():
    # OLD and NEW share an economic underlying but occupy different lifetimes.
    daily = pl.DataFrame(dict(date=[date(2011, 6, 1), date(2021, 6, 1), date(2026, 3, 1), date(2026, 3, 1)],
        product=['OLD', 'MID', 'NEW', 'MARK'], physical_contract=['old:1', 'mid:1', 'new:1', 'mark:1'],
        symbol=['s1', 's1', 's1', 's2'], underlying_symbol=['2330'] * 4,
        asset_class=['stock_future'] * 4, executable=[True, True, True, False],
        previous_volume=[10.] * 4, same_contract_as_previous_session=[True] * 4))
    entries = daily.filter(pl.col('product').is_in(['NEW', 'MARK'])).select('date', 'physical_contract', 'symbol').with_columns(
        pl.Series('entry_quote', [100., None]), pl.Series('entry_volume', [1., 0.]),
        pl.Series('status', ['observed_minute', 'daily_not_executable']))
    summary, products, per_day = audit_universe(daily, {'one_min': entries}, train_start='2020-01-01',
        test_start='2026-02-26', test_end='2026-09-24')
    assert summary['historical_inventory_products'] == 4
    assert summary['historical_daily_executable_products'] == 3
    assert summary['training_horizon_products'] == 3
    assert summary['test_daily_executable_products'] == 1
    assert summary['distinct_known_underlyings'] == 1
    assert summary['coverage_partition'] == dict(ended_before_training=1, ended_before_test=1,
        test_daily_executable=1, test_valuation_only=1)
    assert summary['clocks']['one_min']['test_observed_products'] == 1
    assert summary['clocks']['one_min']['test_one_lot_capacity_products'] == 0  # floor(1 * 50%)
    assert per_day['daily_executable_codes'].to_list() == [1, 1, 1]
    with pytest.raises(ValueError, match='identity absent'):
        audit_universe(daily, {'bad': entries.head(1).with_columns(pl.lit('different-contract').alias('physical_contract'))},
            train_start='2011-01-01', test_start='2026-02-26', test_end='2026-09-24')
