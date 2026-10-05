from datetime import date

import polars as pl
import pytest

from stockagent.data.tw_expected_observation_gaps import interior_period_gaps, annotate_gap_candidates, fiscal_period


def test_missing_whole_quarter_included_but_optional_not_declared_corrupt():
    wide = pl.DataFrame({"source_index": ["2024-Q1", "2024-Q3", "2024-Q4"],
                         "2330": [1., 3., None], "0050": [None, None, None]})
    gaps, raw = interior_period_gaps(wide, "quarter", start=date(2014, 1, 1), as_of=date(2025, 4, 1))
    assert gaps.select("period", "symbol").to_dicts() == [{"period": "2024Q2", "symbol": "2330"}]
    assert gaps["source_index"].to_list() == [None]
    assert annotate_gap_candidates(gaps, None, accepted=False)["state"].to_list() == ["interior_period_omission_requires_provider_recheck"]


def test_future_release_cannot_create_missing_prior_quarter():
    wide = pl.DataFrame({"source_index": ["2026-Q1", "2026-Q2", "2026-Q3"], "2330": [1., None, 3.]})
    gaps, _ = interior_period_gaps(wide, "quarter", start=date(2014, 1, 1), as_of=date(2026, 10, 4))
    assert gaps.is_empty()


def test_exact_month_axis_and_actual_zero_are_valid():
    wide = pl.DataFrame({"source_index": ["2024-02-10", "2024-03-10", "2024-04-10", "2024-05-10"],
                         "2330": [1., None, 0., 4.]})
    gaps, _ = interior_period_gaps(wide, "revenue", start=date(2014, 1, 1), as_of=date(2024, 6, 1))
    assert gaps["period"].to_list() == ["2024-02"]
    candidate = pl.DataFrame({"period": ["2024-02"], "symbol": ["2330"], "value": [2.]})
    assert annotate_gap_candidates(gaps, candidate, accepted=True)["state"][0] == "already_resolved_by_verified_source_union"
    assert annotate_gap_candidates(gaps, candidate, accepted=False)["state"][0] != "already_resolved_by_verified_source_union"


def test_revised_axis_and_ambiguous_periods_fail_closed():
    assert fiscal_period("2024-03-31", "quarter_date")[0] == "2023Q4"
    with pytest.raises(ValueError):
        fiscal_period("2024-04-30", "quarter_date")
    with pytest.raises(ValueError):
        interior_period_gaps(pl.DataFrame({"source_index": ["2024-02-10", "2024-02-11"], "2330": [1., 2.]}),
                             "revenue", start=date(2014, 1, 1), as_of=date(2025, 1, 1))


def test_source_comparison_detects_added_entities_and_keeps_real_zero():
    from scripts.compare_tw_day_trade_source_observations import compare_wide
    old=pl.DataFrame({'source_index':['2024-Q1','2024-Q2'], '2330':[0.,None]})
    new=pl.DataFrame({'source_index':['2024-Q2','2024-Q1'], '2330':[2.,0.], '2317':[3.,None]})
    cells,stats=compare_wide(old,new)
    assert stats['previous_null_now_real_value']==2 and stats['finite_source_revisions']==0
    assert cells.sort('symbol_or_dimension')['value'].to_list()==[3.,2.]


def test_source_comparison_retains_exact_removed_and_revised_observation_keys():
    from scripts.compare_tw_day_trade_source_observations import compare_wide
    old=pl.DataFrame({'source_index':['2024-Q1','2024-Q2'],'2330':[0.,2.]})
    new=pl.DataFrame({'source_index':['2024-Q1','2024-Q2'],'2330':[None,3.]})
    changes,stats=compare_wide(old,new,include_changes=True)
    assert stats['finite_no_longer_present_in_current_provider']==1
    assert changes['removed_finite']['source_index'].to_list()==['2024-Q1']
    assert changes['removed_finite']['previous_value'].to_list()==[0.]
    assert changes['revisions']['previous_value'].to_list()==[2.]
    assert changes['revisions']['current_value'].to_list()==[3.]


def test_gap_count_growth_is_not_the_same_as_source_value_loss():
    from scripts.compare_tw_day_trade_source_observations import compare_gap_keys
    old = pl.DataFrame({'period': ['2024Q2', '2024Q3'], 'symbol': ['2330', '2330']})
    new = pl.DataFrame({'period': ['2024Q3', '2025Q1', '2025Q2'], 'symbol': ['2330'] * 3})
    counts = compare_gap_keys(old, new)
    assert counts['previous_gaps_no_longer_missing_in_source'] == 1
    assert counts['newly_visible_bracketed_candidates'] == 2 and counts['still_missing_source_candidates'] == 1


def test_required_ohlc_uses_trading_evidence_not_halt_or_optional_periods():
    from stockagent.data.tw_expected_observation_gaps import mandatory_trading_price_gaps
    frame = pl.DataFrame({'date': [date(2025, 1, 2)] * 3, 'symbol': ['2330', '0050', '2317'],
        'Trading_Volume': [100., 0., 1.], 'open': [1., None, 0.], 'max': [2., None, 1.],
        'min': [1., None, 1.], 'close': [None, None, 1.]})
    result = mandatory_trading_price_gaps(frame.lazy(), start=date(2014, 1, 1), as_of=date(2025, 1, 3))
    assert set(result.select('symbol', 'feature').rows()) == {('2330', 'close'), ('2317', 'open')}


def test_new_source_audit_cannot_reuse_old_alias_acceptance(tmp_path):
    import json
    from dataclasses import asdict
    from scripts.audit_tw_expected_feature_gaps import audit
    from scripts.prepare_tw_day_trade_feature_catalog import sha256, write_csv
    from stockagent.data.tw_public_release_schedule import RULES, feature_name
    from stockagent.data.tw_day_trade_mixed_frequency import CONTRACT, PRIVATE_USE
    source, report = tmp_path / 'source', tmp_path / 'report'
    (source / 'stocks').mkdir(parents=True); report.mkdir()
    key, name = 'financial_statement:資產總額', feature_name('financial_statement:資產總額')
    symbols = [str(1000+i) for i in range(100)]
    pl.DataFrame({'source_index': ['2024-Q1', '2024-Q2', '2024-Q3'],
        **{s: [1., None, 3.] for s in symbols}}).write_parquet(source / 'assets.parquet')
    # Previously accepted alias now fails the numeric unit/vintage gate.
    pl.DataFrame([{'date': day, 'stock_id': s, 'type': 'TotalAssets', 'value': value}
        for day, value in [('2024-03-31', 2000.), ('2024-06-30', 2000.), ('2024-09-30', 4000.)]
        for s in symbols]).write_parquet(source / 'finmind.parquet')
    pl.DataFrame({'date': [date(2024, 1, 2)], 'open': [1.], 'max': [1.],
        'min': [1.], 'close': [1.], 'Trading_Volume': [1.]}).write_parquet(source / 'stocks/1000_features.parquet')
    definition = {'dataset': key, 'feature': name, 'source': 'FinLab', 'path': 'assets.parquet',
                  'scope': 'stock', 'rule': asdict(RULES['quarter'])}
    manifest = {'contract': CONTRACT, 'source_only': True, 'private_delivery_authorized': True,
        'use_restriction': PRIVATE_USE, 'feature_specs': [definition],
        'native_adapters': [{'kind': 'finmind_financial_facts', 'members': [
            {'dataset': 'TaiwanStockBalanceSheet', 'path': 'finmind.parquet'}]}],
        'files': {str(p.relative_to(source)): {'sha256': sha256(p)} for p in source.rglob('*.parquet')}}
    (source / 'source_manifest.json').write_text(json.dumps(manifest))
    (report / 'stock_0001.md').write_text('```json\n' + json.dumps(definition) + '\n```\n')
    (report / 'aliases.md').write_text('```json\n' + json.dumps({
        'source': 'TaiwanStockBalanceSheet:TotalAssets', 'comparison': {'accepted': True}}) + '\n```\n')
    (report / 'report_receipt.json').write_text(json.dumps({'selected_quantities': 1}))
    write_csv(report.parent / 'feature_coverage.csv', [{'feature': name, 'available_panel_cells': 1}])
    result = audit(source, report, tmp_path / 'audit', start=date(2014, 1, 1),
                   as_of=date(2025, 1, 1), fresh_source_unions=True)
    assert result['candidate_period_keys'] == 100 and result['unverified_period_keys_for_recheck'] == 100
    assert result['already_resolved_in_fixed_panel_keys'] == 0
    assert pl.read_csv(tmp_path / 'audit/fresh_source_union_checks.csv')['accepted'].to_list() == [False]
    status_csv=tmp_path/'owner_checks.csv'
    write_csv(status_csv,[{'dataset':key,'state':'true_upstream_checked_not_gap_fill_proof',
        'source_checked_at_utc':'2025-01-01T00:00:00+00:00','source_sha256':'a'*64}])
    status=tmp_path/'owner_status.json'
    status.write_text(json.dumps({'status_csv':str(status_csv),'status_csv_sha256':sha256(status_csv),'total_intents':1}))
    checked=audit(source,report,tmp_path/'checked-audit',start=date(2014,1,1),as_of=date(2025,1,1),
        fresh_source_unions=True,priority_status=status)
    assert checked['unverified_period_keys_for_recheck']==100 # A check is not a missing-value fill.
    row=pl.read_csv(tmp_path/'checked-audit/selected_feature_status.csv').row(0,named=True)
    assert row['next_action']=='owner_rechecked; inspect_native_context_and_source_response; no_infinite_requery'
    assert checked['priority_status_sha256']==sha256(status)
    status_csv.write_text('tampered')
    with pytest.raises(ValueError,match='status CSV changed'):
        audit(source,report,tmp_path/'bad-status',start=date(2014,1,1),as_of=date(2025,1,1),priority_status=status)
