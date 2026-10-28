from dataclasses import replace
from datetime import date, timedelta
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


@pytest.mark.parametrize("prior", [0., 1., 7.])
@pytest.mark.parametrize("volume", [0., 1., 2., 20.])
def test_single_contract_probe_uses_real_prints_without_rounding_up(prior, volume):
    old = execution()
    old[:, 8] = prior
    facts = source().with_columns(pl.lit(volume).alias("entry_volume"))
    result = reprice_margin_entries(old, facts, participation=.5, broker_multiplier=1.,
                                   capacity_policy="single_contract_probe")
    assert result[0, 8] == min(max(prior, 1.), np.floor(volume * .5))
    # Order admission must not change the independent end-of-day/terminal cap,
    # prior valuation, carry ownership, corporate cash or source market values.
    original = reprice_margin_entries(old, facts, participation=.5, broker_multiplier=1.)
    keep = np.arange(result.shape[1]) != 8
    np.testing.assert_array_equal(result[:, keep], original[:, keep])
    assert old[0, 8] == prior


@pytest.mark.parametrize("status", ["verified_no_window_trade", "unverified_execution_quarantined"])
def test_probe_does_not_turn_unobserved_or_unknown_execution_into_fills(status):
    old = execution()
    old[:, 8] = 0.
    result = reprice_margin_entries(old, source(None, status), participation=1., broker_multiplier=1.,
                                   capacity_policy="single_contract_probe")
    assert result[0, 8] == 0. and result[0, 1] == 0.


def test_probe_can_fill_one_actual_contract_at_full_participation():
    old = execution()
    old[:, 8] = 0.
    facts = source().with_columns(pl.lit(1.).alias("entry_volume"))
    result = reprice_margin_entries(old, facts, participation=1., broker_multiplier=1.,
                                   capacity_policy="single_contract_probe")
    assert result[0, 8] == 1.
    with pytest.raises(ValueError, match="capacity policy"):
        reprice_margin_entries(old, facts, participation=1., broker_multiplier=1., capacity_policy="invent")


def test_fixed_window_uses_only_post_latency_prints_and_volume_weighted_capacity():
    from stockagent.data.tw_futures_decision_clock import observed_window_entries
    minutes = pl.DataFrame(dict(date=[date(2026, 1, 2)] * 5,
        physical_contract=['TX:202601'] * 5, minute=[526, 527, 540, 556, 557],
        vwap=[999., 100., 110., 120., 999.], volume=[100., 2., 3., 5., 100.],
        source_file_sha256=['raw'] * 5))
    contract = decision_clock_contract('08:45', asset_scope='twd_equity', window_minutes=30)
    row = observed_window_entries(minutes, contract).row(0, named=True)
    assert row['entry_volume'] == 10 and row['entry_quote'] == 113.
    # A future quote cannot change the declared fixed-window execution proxy.
    changed = minutes.with_columns(pl.when(pl.col('minute') == 557).then(1e9)
        .otherwise(pl.col('vwap')).alias('vwap'))
    assert observed_window_entries(changed, contract).equals(observed_window_entries(minutes, contract))
    original = observed_window_entries(minutes, decision_clock_contract('08:45'))
    assert original['entry_quote'].to_list() == [100.]
    assert original['entry_volume'].to_list() == [2.]
    for bad in (0, 301, True, 1.5):
        with pytest.raises(ValueError, match='window'):
            decision_clock_contract('08:45', window_minutes=bad)
    mixed = minutes.with_columns(pl.when(pl.col('minute') == 540).then(pl.lit('other'))
        .otherwise(pl.col('source_file_sha256')).alias('source_file_sha256'))
    with pytest.raises(ValueError, match='certificates'):
        observed_window_entries(mixed, contract)


@pytest.mark.parametrize('quantity', [2., -2.])
def test_exact_unknown_execution_quarantine_preserves_inventory_and_official_pnl(quantity):
    from stockagent.data.tw_futures_decision_clock import validate_execution_quarantine, QUARANTINE_CONTRACT
    gaps = pl.DataFrame(dict(date=[date(2012, 6, 4)], physical_contract=['CTF:201206@generation=1']))
    document = dict(contract=QUARANTINE_CONTRACT, source_daily_sha256='daily', source_rules_sha256='rules',
        contract_days=[dict(date='2012-06-04', physical_contract='CTF:201206@generation=1', reason='107_vs_108')])
    validate_execution_quarantine(document, gaps, daily_sha256='daily', rules_sha256='rules')
    for invalid in (gaps.head(0), pl.concat([gaps, gaps]),
                    gaps.with_columns(pl.lit(date(2012, 6, 5)).alias('date'))):
        with pytest.raises(ValueError, match='allowlist'):
            validate_execution_quarantine(document, invalid, daily_sha256='daily', rules_sha256='rules')
    with pytest.raises(ValueError, match='financial source'):
        validate_execution_quarantine(document, gaps, daily_sha256='changed', rules_sha256='rules')
    daily = gaps.with_columns(pl.lit('slot1').alias('symbol'), pl.lit(True).alias('executable'))
    minutes = pl.DataFrame(schema=dict(date=pl.Date, physical_contract=pl.String, minute=pl.Int64,
        vwap=pl.Float64, volume=pl.Float64, source_file_sha256=pl.String))
    verified = gaps.head(0)
    legacy = paired_entries(daily, minutes, verified)['08:45']
    assert legacy['status'].to_list() == ['missing_source']
    admitted = paired_entries(daily, minutes, verified, quarantine_days=gaps)['08:45']
    assert admitted['status'].to_list() == ['unverified_execution_quarantined']
    assert admitted['entry_quote'].to_list() == [None] and admitted['entry_volume'].to_list() == [0.]
    new = reprice_margin_entries(execution(), source(None, admitted['status'][0]), participation=.5, broker_multiplier=1.)
    result = run([[100.]], torch.from_numpy(new[None]), initial_quantities=torch.tensor([quantity]))
    assert result.final_weights.item() == quantity
    assert result.final_equity_scale.item() == pytest.approx(1 + quantity * 20 / 1000)


def test_reporting_calendar_uses_verified_clock_horizon_and_retains_original_context(tmp_path):
    from types import SimpleNamespace
    from scripts.check_tw_scale_cash_ablation_context import calendar_for_config

    daily, rules = tmp_path / 'daily.parquet', tmp_path / 'rules.parquet'
    daily.write_bytes(b'bound daily'); rules.write_bytes(b'bound rules')
    path = tmp_path / 'entries.parquet'
    dates = [date(2026, 1, 2), date(2026, 1, 5)]
    pl.DataFrame(dict(date=dates, physical_contract=['TX:202601'] * 2, symbol=['slot1'] * 2,
        entry_quote=[100., 101.], entry_volume=[1., 2.], status=['observed_minute'] * 2)).write_parquet(path)
    manifest = dict(dataset=CLOCK_DATASET, clock=decision_clock_contract('08:45'),
        status='complete', entries_sha256=sha256_file(path), rows=2,
        source_daily_sha256=sha256_file(daily), source_rules_sha256=sha256_file(rules),
        start_date='2026-01-02', end_date='2026-01-05', pair_identity='same-source-pair')
    path.with_name('manifest.json').write_text(json.dumps(manifest))
    config = SimpleNamespace(trading=SimpleNamespace(tw_futures_portfolio_entry_clock_path=str(path),
        tw_futures_portfolio_data_path=str(daily), tw_futures_portfolio_margin_rules_path=str(rules),
        tw_futures_portfolio_decision_time='08:45'))
    context = ['2011-01-03', '2026-01-02', '2026-01-05', '2026-10-02']
    actual, proof = calendar_for_config(config, context)
    assert actual.astype(str).tolist() == ['2026-01-02', '2026-01-05']
    assert proof['source_context_sessions'] == 4 and proof['model_panel_sessions'] == 2
    assert context[0] == '2011-01-03'
    with pytest.raises(ValueError, match='does not cover'):
        calendar_for_config(config, context[:2])
    daily.write_bytes(b'changed canonical daily')
    with pytest.raises(ValueError, match='incompatible'):
        calendar_for_config(config, context)


def test_extended_precommitted_window_admits_late_prints_with_a_distinct_source_contract():
    from stockagent.data.tw_futures_decision_clock import (
        EXTENDED_WINDOW_CLOCK_DATASET, WINDOW_CLOCK_DATASET, clock_dataset, observed_window_entries,
    )
    minutes = pl.DataFrame(dict(date=[date(2011,8,16)]*3,
        physical_contract=['HF1:201109']*3, minute=[526,682,827],
        vwap=[1e9,14.55,1e9], volume=[100,3,100], source_file_sha256=['a'*64]*3))
    narrow = decision_clock_contract('08:45', window_minutes=30)
    wide = decision_clock_contract('08:45', window_minutes=300)
    assert narrow['version'] == 3 and wide['version'] == 4
    assert clock_dataset('stock_only', window_minutes=30) == WINDOW_CLOCK_DATASET
    assert clock_dataset('stock_only', window_minutes=300) == EXTENDED_WINDOW_CLOCK_DATASET
    assert wide['feature_clock'] == narrow['feature_clock']
    assert wide['order_lifetime'] == 'precommitted_postdecision_window_no_intraday_feature_refresh'
    assert observed_window_entries(minutes, narrow).height == 0
    rows = observed_window_entries(minutes, wide)
    assert rows.height == 1 and rows['entry_volume'][0] == 3
    assert rows['entry_quote'][0] == pytest.approx(14.55)


@pytest.mark.parametrize('prior_required,old_prefix', [(True,False),(False,True)])
def test_probe_rejects_an_inherited_prior_volume_reachability_proof(tmp_path, prior_required, old_prefix):
    from types import SimpleNamespace
    from stockagent.data.tw_futures_margin_release import WHOLE_CONTRACT_PREFIX_CONTRACT
    manifest = tmp_path/'manifest.json'
    manifest.write_text(json.dumps(dict(context_only_prefix_contract=
        WHOLE_CONTRACT_PREFIX_CONTRACT if old_prefix else None)))
    daily = SimpleNamespace(integer_execution=np.zeros((1,1,33)), intraday_execution=None,
        margin_rules_path='not_read', require_prior_capacity=prior_required, manifest_path=str(manifest))
    with pytest.raises(ValueError, match='prior-volume prefix omissions'):
        attach_futures_decision_clock(SimpleNamespace(stock_context_futures_portfolio_daily=daily),
            'not_read',decision_time='08:45',capacity_policy='single_contract_probe')


def test_streamed_long_window_matches_full_history_and_preserves_source_certificates(tmp_path, monkeypatch):
    import scripts.prepare_tw_futures_decision_clocks as builder
    import stockagent.data.tw_futures_finmind_ticks as reader
    import stockagent.data.tw_stock_futures_repair as repair
    from polars.testing import assert_frame_equal
    days = [date(2011,8,16),date(2011,8,17)]
    facts = pl.DataFrame(dict(date=days,product=['HF1']*2,contract=['201109']*2,
        asset_class=['stock_future']*2,
        physical_contract=['HF1:201109']*2,symbol=['slot1']*2,executable=[True]*2,
        tenor_rank=[1]*2,outright_volume=[7,7]))
    manifest = tmp_path/'blocks.json'; manifest.write_text('{}')
    monkeypatch.setattr(repair,'apply_block_evidence',lambda source,path:(source,{}))
    monkeypatch.setattr(reader,'pin_bulk_tick_inventory',lambda root,dates:[dict(date=str(d)) for d in dates])
    inspected = []
    def read(item, expected, **kwargs):
        d = date.fromisoformat(item['date']); inspected.append(d)
        bars = pl.DataFrame(dict(date=[d]*3,physical_contract=['HF1:201109']*3,
            minute=[527,543,682],vwap=[14.5,14.55,14.6],volume=[2.,2.,3.],
            source_file_sha256=[str(d)]*3))
        certified = expected.select('date','physical_contract').with_columns(pl.lit(True).alias('verified'))
        events = pl.DataFrame(dict(trade_date=[d],physical_contract=['HF1:201109'],price=[14.5]))
        return bars,certified,events
    monkeypatch.setattr(reader,'read_bulk_stock_tick_day',read)
    options = dict(progress=lambda **kw:None,workers=1,window_minutes=300)
    full,verified,proof,coverage,trades = builder.load_bulk_minutes(tmp_path,manifest,[],facts,**options)
    prepared = {}
    tiny,verified2,proof2,coverage2,trades2 = builder.load_bulk_minutes(tmp_path,manifest,[],facts,
        prepared_entries=prepared,entry_windows=[30,300],**options)
    assert inspected == days+days  # Both routes certify every complete source day.
    assert full.height == 6 and tiny.height == 0
    assert_frame_equal(verified,verified2)
    assert_frame_equal(coverage,coverage2)
    assert_frame_equal(trades,trades2)
    assert proof2['entry_materialization']['full_session_source_certification_preserved']
    for window in (30,300):
        expected = builder.paired_entries(facts,full,verified,window_minutes=window)
        actual = builder.paired_entries(facts,tiny,verified2,window_minutes=window,
            prepared_entries=prepared[window])
        for clock in ('08:45','09:00'):
            assert_frame_equal(expected[clock],actual[clock])


@pytest.mark.parametrize('days', [[date(2019,9,27),date(2019,9,30)],
                                  [date(2026,9,24),date(2026,9,28)]])
def test_bulk_source_review_boundaries_fail_before_tick_or_block_io(tmp_path, monkeypatch, days):
    import scripts.prepare_tw_futures_decision_clocks as builder
    import stockagent.data.tw_futures_finmind_ticks as reader
    import stockagent.data.tw_stock_futures_repair as repair
    def must_not_read(*args, **kwargs):
        pytest.fail('unreviewed product dates must be rejected before source I/O')
    monkeypatch.setattr(reader,'pin_bulk_tick_inventory',must_not_read)
    monkeypatch.setattr(repair,'apply_block_evidence',must_not_read)
    facts = pl.DataFrame(dict(date=days,product=['G2F']*2,
        asset_class=['index_future']*2,executable=[True]*2))
    with pytest.raises(ValueError,match='bulk source preflight G2F'):
        builder.load_bulk_minutes(tmp_path,tmp_path/'absent.json',[],facts,
            progress=lambda **kw:None,asset_scope='twd_equity')


@pytest.mark.parametrize('empty,gaps,expected', [
    (True, [], []), (True, [{'date':'2026-01-02','physical_contract':'CAF:202601'}], []),
    (False, [], ['CAF:202601','CBF:202601']),
    (False, [{'date':'2026-01-02','physical_contract':'CAF:202601'}], ['CBF:202601']),
])
def test_kbar_empty_or_incomplete_receipts_do_not_abort_or_become_verified_days(empty,gaps,expected):
    from scripts.prepare_tw_futures_decision_clocks import _complete_kbar_days
    days=pl.DataFrame({'date':[date(2026,1,2)]*2,
        'physical_contract':['CAF:202601','CBF:202601']})
    if empty:days=days.head(0)
    kept=_complete_kbar_days(days,gaps)
    assert kept.schema==days.schema
    assert kept['physical_contract'].to_list()==expected


@pytest.mark.parametrize('corrupt',['raw','reduction'])
def test_bulk_checkpoint_reuses_exact_outputs_and_rejects_changed_source_or_reduction(tmp_path,monkeypatch,corrupt):
    import scripts.prepare_tw_futures_decision_clocks as builder
    import stockagent.data.tw_futures_finmind_ticks as reader
    import stockagent.data.tw_stock_futures_repair as repair
    from polars.testing import assert_frame_equal
    day=date(2011,8,16)
    facts=pl.DataFrame(dict(date=[day],product=['HF1'],contract=['201109'],
        asset_class=['stock_future'],physical_contract=['HF1:201109'],symbol=['slot1'],
        executable=[True],tenor_rank=[1],outright_volume=[7]))
    blocks=tmp_path/'blocks.json';blocks.write_text('{}')
    raw=tmp_path/'raw.parquet';raw.write_bytes(b'exact-retained-raw-test-object')
    item=dict(date=str(day),path=str(raw),sha256=sha256_file(raw))
    monkeypatch.setattr(repair,'apply_block_evidence',lambda source,path:(source,{}))
    monkeypatch.setattr(reader,'pin_bulk_tick_inventory',lambda root,dates:[item])
    inspected=[]
    def read(item,expected,**kwargs):
        inspected.append(item['date'])
        bars=pl.DataFrame(dict(date=[day]*2,physical_contract=['HF1:201109']*2,
            minute=[527,682],vwap=[14.5,14.6],volume=[2.,5.],source_file_sha256=[item['sha256']]*2))
        certified=expected.select('date','physical_contract').with_columns(pl.lit(True).alias('verified'))
        events=pl.DataFrame(dict(trade_date=[day],physical_contract=['HF1:201109'],price=[14.5]))
        return bars,certified,events
    monkeypatch.setattr(reader,'read_bulk_stock_tick_day',read)
    options=dict(progress=lambda **kw:None,workers=1,window_minutes=300,
        entry_windows=[30,300],checkpoint_root=tmp_path/'checkpoints')
    first_entries={}
    first=builder.load_bulk_minutes(tmp_path,blocks,[],facts,prepared_entries=first_entries,**options)
    replay_entries={}
    second=builder.load_bulk_minutes(tmp_path,blocks,[],facts,prepared_entries=replay_entries,**options)
    assert inspected==[str(day)]
    for i in (0,1,3,4):assert_frame_equal(first[i],second[i])
    assert first[2]==second[2]
    for window in (30,300):
        for clock in ('08:45','09:00'):
            assert_frame_equal(first_entries[window][clock],replay_entries[window][clock])
    # Different execution windows cannot inherit old reductions.
    changed={}
    builder.load_bulk_minutes(tmp_path,blocks,[],facts,prepared_entries=changed,
        **dict(options,window_minutes=301))
    assert inspected==[str(day)]*2
    if corrupt=='raw':raw.write_bytes(b'changed-retained-raw-test-object')
    else:
        candidates=list((tmp_path/'checkpoints').glob('*/coverage.parquet'))
        for path in candidates:path.write_bytes(b'changed-reduction')
    with pytest.raises(ValueError,match='SHA'):
        builder.load_bulk_minutes(tmp_path,blocks,[],facts,prepared_entries={},**options)


def test_bulk_checkpoint_hashes_shared_inventory_once_and_rejects_conflicts(tmp_path, monkeypatch):
    from collections import Counter
    import scripts.prepare_tw_futures_decision_clocks as builder
    inventory = tmp_path/'inventory.json'; inventory.write_bytes(b'original shared inventory')
    sources = []
    for index in range(4):
        raw = tmp_path/f'raw-{index}.bin'; raw.write_bytes(bytes([index]))
        receipt = tmp_path/f'receipt-{index}.json'; receipt.write_bytes(bytes([index+10]))
        sources.append(dict(path=str(raw), sha256=sha256_file(raw),
            receipt_path=str(receipt), receipt_sha256=sha256_file(receipt),
            inventory_path=str(inventory), inventory_sha256=sha256_file(inventory)))
    path = tmp_path/'checkpoint'; identity = {'scope':'test'}
    builder._bulk_checkpoint_write(path, identity, {'coverage':pl.DataFrame({'value':[1]})})
    calls = Counter(); original = builder.sha256_file
    def counted(member, **kwargs):
        calls[str(member)] += 1
        return original(member, **kwargs)
    monkeypatch.setattr(builder, 'sha256_file', counted)
    assert builder._bulk_checkpoint_read(path, identity, sources, 2)['coverage']['value'].to_list() == [1]
    assert calls[str(inventory)] == 1
    bad = [dict(item) for item in sources]; bad[-1]['inventory_sha256'] = '0'*64
    with pytest.raises(ValueError, match='conflicting SHA'):
        builder._bulk_checkpoint_read(path, identity, bad, 2)
    inventory.write_bytes(b'corrupt shared inventory')
    with pytest.raises(ValueError, match='raw SHA'):
        builder._bulk_checkpoint_read(path, identity, sources, 2)


def test_bulk_date_delta_recomputes_complete_changed_day_and_matches_fresh_build(tmp_path,monkeypatch):
    import hashlib
    import scripts.prepare_tw_futures_decision_clocks as builder
    import stockagent.data.tw_futures_finmind_ticks as reader
    import stockagent.data.tw_stock_futures_repair as repair
    from polars.testing import assert_frame_equal
    days=[date(2011,8,16),date(2011,8,17)]
    previous=pl.DataFrame(dict(date=days,product=['HF1']*2,contract=['201109']*2,
        asset_class=['stock_future']*2,physical_contract=['HF1:201109']*2,symbol=['slot1']*2,
        executable=[True]*2,tenor_rank=[1]*2,outright_volume=[7,7]))
    current=previous.with_columns(pl.when(pl.col('date')==days[1]).then(8)
        .otherwise(pl.col('outright_volume')).alias('outright_volume'))
    blocks=tmp_path/'blocks.json';blocks.write_text('{}')
    items=[]
    for day in days:
        raw=tmp_path/(str(day)+'.parquet');raw.write_bytes(str(day).encode())
        items.append(dict(date=str(day),path=str(raw),sha256=sha256_file(raw)))
    monkeypatch.setattr(repair,'apply_block_evidence',lambda source,path:(source,{}))
    monkeypatch.setattr(reader,'pin_bulk_tick_inventory',lambda root,dates:items)
    inspected=[]
    def read(item,expected,**kwargs):
        inspected.append(item['date']); day=date.fromisoformat(item['date']); volume=expected['outright_volume'][0]
        bars=pl.DataFrame(dict(date=[day],physical_contract=['HF1:201109'],minute=[527],
            vwap=[14.5],volume=[float(volume)],source_file_sha256=[item['sha256']]))
        coverage=expected.select('date','physical_contract').with_columns(pl.lit(True).alias('verified'))
        events=pl.DataFrame(dict(trade_date=[day],physical_contract=['HF1:201109'],price=[float(volume)]))
        return bars,coverage,events
    monkeypatch.setattr(reader,'read_bulk_stock_tick_day',read)
    options=dict(progress=lambda **kw:None,workers=1,window_minutes=300,
        entry_windows=[1,300],checkpoint_root=tmp_path/'old')
    builder.load_bulk_minutes(tmp_path,blocks,[],previous,prepared_entries={},**options)
    parent=next((tmp_path/'old').iterdir())
    saved=json.loads((parent/'manifest.json').read_text())['identity']
    def verified_parent(path,receipt,comparison):
        assert comparison['source_frame_sha256']==hashlib.sha256(previous.serialize()).hexdigest()
        return saved,dict(frozen_source={'source_sha256':'a'*64})
    monkeypatch.setattr(builder,'_bulk_checkpoint_parent',verified_parent)
    parent_proof=dict(daily_path=str(blocks),daily_sha256=sha256_file(blocks),
                      rules_path=str(blocks),rules_sha256=sha256_file(blocks))
    inspected.clear()
    updated_entries={}
    updated=builder.load_bulk_minutes(tmp_path,blocks,[],current,prepared_entries=updated_entries,
        **dict(options,checkpoint_root=tmp_path/'delta',reuse_checkpoint=parent,
            reuse_source_receipt=tmp_path/'release.json',delta_parent_source=previous,
            delta_days=[days[1]],delta_parent_proof=parent_proof))
    assert inspected==[str(days[1])]
    assert updated[2]['reused_verified_core']['contract']=='verified_prior_core_date_delta_v1'
    inspected.clear()
    fresh_entries={}
    fresh=builder.load_bulk_minutes(tmp_path,blocks,[],current,prepared_entries=fresh_entries,
        **dict(options,checkpoint_root=tmp_path/'fresh'))
    assert inspected==list(map(str,days))
    for i in (0,1,3,4):
        key='trade_date' if i==4 else 'date'
        assert_frame_equal(updated[i].sort(key),fresh[i].sort(key))
    for window in (1,300):
        for clock in ('08:45','09:00'):
            assert_frame_equal(updated_entries[window][clock].sort('date'),fresh_entries[window][clock].sort('date'))
    with pytest.raises(ValueError,match='outside its explicit dates'):
        builder.load_bulk_minutes(tmp_path,blocks,[],current,prepared_entries={},
            **dict(options,checkpoint_root=tmp_path/'bad',reuse_checkpoint=parent,
                reuse_source_receipt=tmp_path/'release.json',delta_parent_source=previous,
                delta_days=[days[0]],delta_parent_proof=parent_proof))


def test_bulk_prepend_recomputes_only_earlier_complete_day_and_matches_full_build(tmp_path, monkeypatch):
    import hashlib
    import scripts.prepare_tw_futures_decision_clocks as builder
    import stockagent.data.tw_futures_finmind_ticks as reader
    import stockagent.data.tw_stock_futures_repair as repair
    from polars.testing import assert_frame_equal
    days = [date(2011, 1, 3), date(2011, 1, 4), date(2011, 1, 5)]
    current = pl.DataFrame(dict(date=days, product=['HF1']*3, contract=['201101']*3,
        asset_class=['stock_future']*3, physical_contract=['HF1:201101']*3, symbol=['slot1']*3,
        executable=[True]*3, tenor_rank=[1]*3, outright_volume=[5, 7, 9]))
    previous = current.filter(pl.col('date') > days[0])
    blocks = tmp_path/'blocks.json'; blocks.write_text('{}')
    items = []
    for day in days:
        raw = tmp_path/(str(day)+'.parquet'); raw.write_bytes(str(day).encode())
        items.append(dict(date=str(day), path=str(raw), sha256=sha256_file(raw)))
    monkeypatch.setattr(repair, 'apply_block_evidence', lambda source, path:(source, {}))
    monkeypatch.setattr(reader, 'pin_bulk_tick_inventory', lambda root, dates:
        [item for item in items if date.fromisoformat(item['date']) in dates])
    inspected = []
    def read(item, expected, **kwargs):
        inspected.append(item['date']); day = date.fromisoformat(item['date'])
        volume = float(expected['outright_volume'][0])
        bars = pl.DataFrame(dict(date=[day], physical_contract=['HF1:201101'], minute=[527],
            vwap=[14.5], volume=[volume], source_file_sha256=[item['sha256']]))
        coverage = expected.select('date','physical_contract').with_columns(pl.lit(True).alias('verified'))
        trades = pl.DataFrame(dict(trade_date=[day],physical_contract=['HF1:201101'],price=[volume]))
        return bars, coverage, trades
    monkeypatch.setattr(reader, 'read_bulk_stock_tick_day', read)
    options = dict(progress=lambda **kw:None, workers=1, window_minutes=300,
                   entry_windows=[1,300], checkpoint_root=tmp_path/'old')
    builder.load_bulk_minutes(tmp_path, blocks, [], previous, prepared_entries={}, **options)
    parent = next((tmp_path/'old').iterdir())
    saved = json.loads((parent/'manifest.json').read_text())['identity']
    def verified_parent(path, receipt, comparison):
        assert comparison['source_frame_sha256'] == hashlib.sha256(previous.serialize()).hexdigest()
        assert comparison['objects'] == items[1:]
        return saved, dict(frozen_source={'source_sha256':'a'*64})
    monkeypatch.setattr(builder, '_bulk_checkpoint_parent', verified_parent)
    parent_proof = dict(daily_path=str(blocks), daily_sha256=sha256_file(blocks),
                        rules_path=str(blocks), rules_sha256=sha256_file(blocks))
    inspected.clear(); extended_entries = {}
    kwargs = dict(options, checkpoint_root=tmp_path/'extended', reuse_checkpoint=parent,
        reuse_source_receipt=tmp_path/'release.json', delta_parent_source=previous,
        prepend_days=[days[0]], delta_parent_proof=parent_proof)
    extended = builder.load_bulk_minutes(tmp_path, blocks, [], current, prepared_entries=extended_entries, **kwargs)
    assert inspected == [str(days[0])]
    assert extended[2]['reused_verified_core']['contract'] == 'verified_prior_core_prepend_v1'
    inspected.clear(); fresh_entries = {}
    fresh = builder.load_bulk_minutes(tmp_path, blocks, [], current, prepared_entries=fresh_entries,
                                     **dict(options, checkpoint_root=tmp_path/'fresh'))
    assert inspected == list(map(str, days))
    for i in (0, 1, 3, 4):
        key = 'trade_date' if i == 4 else 'date'
        assert_frame_equal(extended[i].sort(key), fresh[i].sort(key))
    for window in (1,300):
        for clock in ('08:45','09:00'):
            assert_frame_equal(extended_entries[window][clock].sort('date'),
                               fresh_entries[window][clock].sort('date'))
    with pytest.raises(ValueError, match='declare every new source date'):
        builder._validate_bulk_prepend(previous, current, [days[0] - timedelta(days=1)])
    with pytest.raises(ValueError, match='must all precede'):
        builder._validate_bulk_prepend(previous, current, [days[1]])
    changed = current.with_columns(pl.when(pl.col('date') == days[1]).then(100)
                                   .otherwise(pl.col('outright_volume')).alias('outright_volume'))
    with pytest.raises(ValueError, match='changes the parent'):
        builder._validate_bulk_prepend(previous, changed, [days[0]])
    with pytest.raises(ValueError, match='non-null dated physical identities'):
        builder._validate_bulk_prepend(previous, current.with_columns(
            pl.when(pl.col('date') == days[0]).then(None).otherwise(pl.col('physical_contract'))
            .alias('physical_contract')), [days[0]])
    saved['loader_sha256'] = 'changed'
    with pytest.raises(ValueError, match='different per-day implementations'):
        builder.load_bulk_minutes(tmp_path, blocks, [], current, prepared_entries={},
                                  **dict(kwargs, checkpoint_root=tmp_path/'different-code'))


def test_bulk_parent_history_reproduces_saved_checkpoint_source_identity(tmp_path):
    import hashlib
    from scripts.prepare_tw_futures_decision_clocks import _load_clock_history
    days = [date(2010,12,31),date(2011,1,26),date(2011,1,27),date(2011,1,28)]
    rows = pl.DataFrame(dict(date=days,product=['MTX']*4,contract=['201102']*4,
        asset_class=['index_future']*4,physical_contract=['MTX:201102']*4,symbol=['slot1']*4,
        open=[100.]*4,high=[101.]*4,low=[99.]*4,close=[100.]*4,volume=[7.]*4,
        executable=[True]*4,outright_volume=[7]*4,tenor_rank=[1]*4,official_expiry=days))
    path = tmp_path/'daily.parquet'
    rows.write_parquet(path,row_group_size=1)
    start,end = days[1],days[2]
    current = _load_clock_history(pl.scan_parquet(path),end,[],[],
        ['outright_volume','tenor_rank','official_expiry'])
    assert current['date'].to_list() == days[:3]  # older history remains available to references
    saved = current.filter(pl.col('date') >= start)
    restored = _load_clock_history(pl.scan_parquet(path),end,[],[],
        ['outright_volume','tenor_rank','official_expiry']).filter(pl.col('date') >= start)
    assert restored['date'].to_list() == days[1:3]
    assert hashlib.sha256(restored.serialize()).digest() == hashlib.sha256(saved.serialize()).digest()


@pytest.mark.parametrize('corruption',[None,'scope','code','fingerprint'])
def test_prior_core_checkpoint_requires_same_inputs_and_original_frozen_code(tmp_path,monkeypatch,corruption):
    import hashlib
    from downloader.artifact_io import atomic_write_json
    import stockagent.runtime_identity as runtime
    import scripts.prepare_tw_futures_decision_clocks as builder
    source = tmp_path/'release'/'build-source'; source.mkdir(parents=True)
    names = ['stockagent/data/tw_futures_decision_clock.py',
             'stockagent/data/tw_futures_finmind_ticks.py',
             'stockagent/data/tw_stock_futures_repair.py',
             'stockagent/data/tw_price_rules.py']
    files = {name:str(i)*64 for i,name in enumerate(names,1)}
    files['scripts/prepare_tw_futures_decision_clocks.py'] = '5'*64
    receipt = source.parent/'release.json'
    atomic_write_json(receipt,dict(code=dict(files=files)))
    monkeypatch.setattr(runtime,'verify_source_release',lambda receipt,root:dict(source_sha256='a'*64))
    old = dict(source_frame_sha256='b'*64,block_sha256='c'*64,objects=[{'sha256':'d'*64}],
        asset_scope='all_twd',window_minutes=300,entry_windows=[1,300],streaming=True,polars_version='test',
        loader_sha256=files['scripts/prepare_tw_futures_decision_clocks.py'],
        implementation_sha256s={str(source/name):files[name] for name in names})
    fingerprint = hashlib.sha256(json.dumps(old,sort_keys=True).encode()).hexdigest()
    checkpoint = tmp_path/('wrong' if corruption=='fingerprint' else fingerprint)
    checkpoint.mkdir()
    atomic_write_json(checkpoint/'manifest.json',dict(contract='verified_bulk_window_checkpoint_v1',identity=old))
    current = dict(old)
    if corruption == 'scope':current['window_minutes'] = 1
    if corruption == 'code':
        files[names[0]] = 'f'*64
        atomic_write_json(receipt,dict(code=dict(files=files)))
    if corruption:
        with pytest.raises(ValueError,match='checkpoint|implementation'):
            builder._bulk_checkpoint_parent(checkpoint,receipt,current)
    else:
        identity,proof=builder._bulk_checkpoint_parent(checkpoint,receipt,current)
        assert identity==old and proof['frozen_source']['source_sha256']=='a'*64


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


@pytest.mark.parametrize('asset', ['stock_future', 'etf_future', 'index_future'])
def test_v2_entry_reprice_uses_dated_equity_tax_and_preserves_fixed_margin(asset):
    # 50,000 -> 55,000 notional: dated 2e-5 tax rounds 1.0 -> 1.1 to whole TWD.
    facts = source().with_columns(pl.lit(asset).alias('asset_class'),
        pl.lit(500.).alias('contract_multiplier'), pl.lit(50010.).alias('opening_contract_value_twd'),
        pl.lit(50000.).alias('opening_margin_value_twd'), pl.lit(1.).alias('opening_tax_twd'),
        pl.lit('fixed_twd').alias('margin_kind'), pl.lit(1234.).alias('initial'),
        pl.lit(987.).alias('maintenance'))
    new = reprice_margin_entries(execution(), facts, participation=.5,broker_multiplier=1.1,
                                 asset_scope='twd_equity')
    assert new[0,3] == 55010. and new[0,6] == 1.
    assert new[0,11:13].tolist() == [1357., 1086.]
    if asset != 'stock_future':
        with pytest.raises(ValueError,match='asset classes'):
            reprice_margin_entries(execution(),facts,participation=.5,broker_multiplier=1.)
    invalid = facts.with_columns(pl.lit('commodity_future').alias('asset_class'))
    with pytest.raises(ValueError,match='asset classes'):
        reprice_margin_entries(execution(),invalid,participation=.5,broker_multiplier=1.,asset_scope='twd_equity')


def test_expanded_clock_has_distinct_manifest_and_rejects_scope_drift(tmp_path):
    from stockagent.data.tw_futures_decision_clock import EQUITY_CLOCK_DATASET
    daily, rules = tmp_path/'daily.parquet', tmp_path/'rules.parquet'
    daily.write_bytes(b'daily'); rules.write_bytes(b'rules')
    path = tmp_path/'entries.parquet'
    pl.DataFrame(dict(date=[date(2026,1,2)],physical_contract=['TX:202601'],symbol=['slot1'],
        entry_quote=[30001.],entry_volume=[3.],status=['observed_minute'])).write_parquet(path)
    contract = decision_clock_contract('08:45',asset_scope='twd_equity')
    manifest = dict(dataset=EQUITY_CLOCK_DATASET,clock=contract,status='complete',rows=1,
        entries_sha256=sha256_file(path),source_daily_sha256=sha256_file(daily),
        source_rules_sha256=sha256_file(rules),start_date='2026-01-02',end_date='2026-01-02')
    path.with_name('manifest.json').write_text(json.dumps(manifest))
    validate_clock_source(path,daily_path=daily,rules_path=rules,decision_time='08:45')
    assert contract['version'] == 2 and decision_clock_contract('08:45')['version'] == 1
    manifest['dataset'] = CLOCK_DATASET
    path.with_name('manifest.json').write_text(json.dumps(manifest))
    with pytest.raises(ValueError,match='incompatible'):
        validate_clock_source(path,daily_path=daily,rules_path=rules,decision_time='08:45')


def test_quarantine_manifest_requires_exact_receipt_and_cannot_resume_legacy_clock(tmp_path):
    from stockagent.data.tw_futures_decision_clock import WINDOW_CLOCK_DATASET, QUARANTINE_CONTRACT
    daily, rules = tmp_path/'daily.parquet', tmp_path/'rules.parquet'
    daily.write_bytes(b'daily'); rules.write_bytes(b'rules')
    path = tmp_path/'entries.parquet'
    pl.DataFrame(dict(date=[date(2012, 6, 4)], physical_contract=['CTF:201206'], symbol=['slot1'],
        entry_quote=[None], entry_volume=[0.], status=['unverified_execution_quarantined']),
        schema_overrides={'entry_quote': pl.Float64}).write_parquet(path)
    receipt = tmp_path/'execution_quarantine.json'
    receipt.write_text(json.dumps(dict(contract=QUARANTINE_CONTRACT, source_daily_sha256=sha256_file(daily),
        source_rules_sha256=sha256_file(rules), contract_days=[dict(date='2012-06-04',
            physical_contract='CTF:201206', reason='independent_quantity_mismatch')])) )
    manifest = dict(dataset=WINDOW_CLOCK_DATASET, clock=decision_clock_contract('08:45',
        asset_scope='twd_equity', window_minutes=30, quarantine_unknown=True), status='complete', rows=1,
        entries_sha256=sha256_file(path), source_daily_sha256=sha256_file(daily),
        source_rules_sha256=sha256_file(rules), start_date='2012-06-04', end_date='2012-06-04',
        source_coverage_complete=False, coverage_gaps=1,
        execution_quarantine=dict(file=receipt.name, sha256=sha256_file(receipt), contract_days=1))
    path.with_name('manifest.json').write_text(json.dumps(manifest))
    validate_clock_source(path, daily_path=daily, rules_path=rules, decision_time='08:45')
    receipt.write_text(receipt.read_text() + ' ')
    with pytest.raises(ValueError, match='receipt'):
        validate_clock_source(path, daily_path=daily, rules_path=rules, decision_time='08:45')
    manifest['clock'] = decision_clock_contract('08:45', asset_scope='twd_equity')
    path.with_name('manifest.json').write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match='incompatible'):
        validate_clock_source(path, daily_path=daily, rules_path=rules, decision_time='08:45')


def test_universe_study_keeps_account_model_and_stopping_identical(tmp_path):
    from pathlib import Path
    from scripts.run_ablation_experiments import _experiment_rows, _deep_merge
    spec, rows = _experiment_rows(Path('configs/ablations/tw_futures_equity_universe_20261023_v2.yaml'))
    raw = _load_raw_config(spec['base_config'])
    configs = []
    for row in rows:
        case = _deep_merge(raw,row['overrides'])
        path = tmp_path/(row['name']+'.yaml')
        path.write_text(yaml.safe_dump(case))
        configs.append(load_config(path))
    assert [r['name'] for r in rows] == ['stock_only__0845','stock_only__0900','twd_equity__0845','twd_equity__0900']
    assert all(c.training == configs[0].training and c.walk_forward == configs[0].walk_forward for c in configs)
    assert {c.trading.tw_futures_portfolio_integer_initial_capital for c in configs} == {100_000_000.}
    assert configs[0].training.epochs * configs[0].training.early_stopping_no_improve_ratio == 100
    assert configs[0].training.pretrained_initialization_root is None
    assert configs[0].training.loss_type == 'log_utility' and configs[0].training.lookback == 32
    assert configs[2].trading.tw_futures_portfolio_data_path != configs[0].trading.tw_futures_portfolio_data_path
    assert configs[2].trading.tw_futures_portfolio_decision_time == '08:45'
    assert configs[3].trading.tw_futures_portfolio_decision_time == '09:00'


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


@pytest.mark.parametrize("bad_event", [None, "observed_trade", "execution_capacity", "nonfinite_capacity", "source_flag"])
def test_clock_preserves_account_only_settlement_rows(tmp_path, bad_event):
    from stockagent.data.tw_futures_account_calendar import build_futures_account_calendar
    from stockagent.data.tw_futures_margin import attach_futures_margin_rules
    from test_tw_futures_margin import rule_panel, write_rules
    from test_tw_futures_margin_corporate import corporate_rules

    panel, rows = rule_panel(tmp_path)
    daily_path = panel.stock_context_futures_portfolio_daily.source_path
    pl.read_parquet(daily_path).with_columns(
        pl.lit("stock_future").alias("asset_class"),
        pl.Series("accounting_only_date", [False, bad_event != "source_flag", False]),
    ).write_parquet(daily_path)
    calendar = build_futures_account_calendar(
        ["2026-01-02", "2026-01-04"], panel.dates, [False, True, False],
    )
    for row in corporate_rules(rows):
        row.update(contract_multiplier=10., opening_tax_twd=0., known_tax_twd=0.,
            opening_margin_value_twd=1000., settlement_margin_value_twd=1000.,
            known_margin_value_twd=1000., second_position_group="month",
            second_position_unit=1., second_position_limit=100.)
    rules_path = write_rules(tmp_path, panel, rows)
    rule_meta = rules_path.with_name("manifest.json")
    meta = json.loads(rule_meta.read_text()); meta["schema_version"] = 4
    rule_meta.write_text(json.dumps(meta))
    panel = attach_futures_margin_rules(panel, rules_path, futures_account_calendar=calendar)
    prior = panel.stock_context_futures_portfolio_daily
    execution = prior.integer_execution.copy()
    execution[1, :, 1] = 0.
    execution[1, :, 8] = 0.
    execution[1, 0, 3:5] = [1234., 1250.]
    if bad_event == "execution_capacity":
        execution[1, 0, 8] = 1.
    if bad_event == "nonfinite_capacity":
        execution[1, 0, 8] = float("nan")
    panel = replace(panel, stock_context_futures_portfolio_daily=replace(
        prior, integer_execution=execution,
    ))
    original_event = execution[1].copy()
    original_open = prior.open_prices[1].copy()

    directory = tmp_path / "clock"; directory.mkdir()
    path = directory / "entries.parquet"
    entries = pl.DataFrame({"date": [date(2026, 1, 2), date(2026, 1, 3), date(2026, 1, 4)],
        "physical_contract": ["TX202601"] * 3, "symbol": ["TAIFEX_SLOT_0001"] * 3,
        "entry_quote": [105., 104. if bad_event == "observed_trade" else None, 107.],
        "entry_volume": [4., 1. if bad_event == "observed_trade" else 0., 4.],
        "status": ["observed_minute", "observed_minute" if bad_event == "observed_trade"
                   else "daily_not_executable", "observed_minute"]})
    entries.write_parquet(path)
    path.with_name("manifest.json").write_text(json.dumps({"dataset": CLOCK_DATASET,
        "clock": decision_clock_contract("09:00"), "status": "complete", "rows": 3,
        "entries_sha256": sha256_file(path), "source_daily_sha256": sha256_file(daily_path),
        "source_rules_sha256": sha256_file(rules_path), "start_date": "2026-01-02",
        "end_date": "2026-01-04", "pair_identity": "synthetic-test-only"}))
    if bad_event is not None:
        expected_error = ("conflicts with source date flags" if bad_event == "source_flag"
                          else "account-only dates cannot contain")
        with pytest.raises(ValueError, match=expected_error):
            attach_futures_decision_clock(panel, path, decision_time="09:00",
                                          futures_account_calendar=calendar)
        return
    attached = attach_futures_decision_clock(panel, path, decision_time="09:00",
                                              futures_account_calendar=calendar)
    actual = attached.stock_context_futures_portfolio_daily
    np.testing.assert_array_equal(actual.integer_execution[1], original_event)
    np.testing.assert_array_equal(actual.open_prices[1], original_open)
    assert actual.integer_execution[[0, 2], 0, 3].tolist() == [1050., 1070.]
    assert actual.integer_execution[[0, 2], 0, 8].tolist() == [2., 2.]


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
