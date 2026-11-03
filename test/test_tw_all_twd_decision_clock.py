"""All-TWD entries agree with the canonical dated financial compiler."""
from datetime import date, datetime
import json

import numpy as np
import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from stockagent.data.tw_futures_decision_clock import (
    ALL_TWD_CLOCK_DATASET, bind_entry_specifications, clock_dataset,
    dated_entry_sessions, decision_clock_contract, reprice_margin_entries, validate_clock_source,
)
from stockagent.data.tw_futures_execution_terms import SPEC_FIELDS, compile_execution_terms
from stockagent.data.tw_futures_finmind_ticks import decode_bulk_stock_prices, normalize_bulk_stock_ticks
from stockagent.data.tw_stock_futures_history import normalize_continuous_ticks
from test_tw_futures_execution_terms import inputs, specification
from test_tw_futures_decision_clock import execution


@pytest.mark.parametrize('tax_class,asset,margin_basis,tax_basis,face',[
    ('gold','commodity_future','quoted_value','quoted_value',None),
    ('oil','commodity_future','full_value','full_value',None),
    ('commercial_paper','interest_rate_future','face_value','face_value',100_000_000.),
    ('government_bond','interest_rate_future','face_value','face_value',5_000_000.),
    ('stock','stock_future','full_value','quoted_value',None),
])
def test_repriced_entry_matches_recompiled_canonical_values(tax_class,asset,margin_basis,tax_basis,face):
    args=inputs()
    specs=pl.DataFrame([specification(tax_class=tax_class,margin_value_kind=margin_basis,
        tax_value_kind=tax_basis,tax_fixed_value_twd=face)],schema=SPEC_FIELDS)
    args['specifications']=specs
    old,_=compile_execution_terms(**args)
    new_frame=args['frame'].with_columns((pl.col('open')+7.).alias('open'))
    new,_=compile_execution_terms(**dict(args,frame=new_frame))
    facts=args['frame'].select('date','product','physical_contract','open').join(
        old.drop('product','contract'),on=['date','physical_contract'],maintain_order='left').with_columns(
            pl.lit(asset).alias('asset_class'),pl.lit('observed_minute').alias('status'),
            (pl.col('open')+7.).alias('entry_quote'),pl.lit(20.).alias('entry_volume'))
    facts=bind_entry_specifications(facts,specs)
    old_ex=np.repeat(execution(),facts.height,axis=0)
    old_ex[:,3]=old['opening_contract_value_twd'].to_numpy()
    old_ex[:,4]=old['terminal_contract_value_twd'].to_numpy()
    old_ex[:,6]=old['opening_tax_twd'].to_numpy()
    result=reprice_margin_entries(old_ex,facts,participation=.5,broker_multiplier=1.,asset_scope='all_twd')
    np.testing.assert_allclose(result[:,3],new['opening_contract_value_twd'].to_numpy(),rtol=1e-7)
    np.testing.assert_array_equal(result[:,6],new['opening_tax_twd'].to_numpy())
    np.testing.assert_array_equal(result[:,11],np.floor(new['initial'].to_numpy()*new['opening_margin_value_twd'].to_numpy()+.5))
    np.testing.assert_array_equal(result[:,12],np.floor(new['maintenance'].to_numpy()*new['opening_margin_value_twd'].to_numpy()+.5))
    # Repricing cannot change independent terminal capacity, carry quantities,
    # closing value, terminal tax or position constraints.
    untouched=[i for i in range(result.shape[1]) if i not in {0,1,3,6,8,11,12,18,19}]
    np.testing.assert_array_equal(result[:,untouched],old_ex[:,untouched])
    if face is not None:
        np.testing.assert_array_equal(result[:,6],old_ex[:,6])


def _bound_source(product='TGF',asset='commodity_future',tick=.5,close='16:15:00'):
    day=date(2026,1,5)
    specs=pl.DataFrame([specification(product=product,tax_class='gold',tick_size=tick,
        settlement_time=close,expiry_time='12:00:00')],schema=SPEC_FIELDS)
    raw=pl.DataFrame(dict(date=[day],product=[product],contract=['202602'],
        physical_contract=[product+':202602'],asset_class=[asset],official_expiry=[date(2026,2,18)]))
    return bind_entry_specifications(raw,specs),specs


def test_dated_binding_rejects_late_missing_and_ended_specs():
    source,specs=_bound_source()
    source=source.select('date','product','contract','physical_contract','asset_class','official_expiry')
    for update in [pl.lit('2026-01-05T09:00:00+08:00').alias('known_at'),
                   pl.lit(date(2026,1,5)).alias('valid_until_exclusive'),
                   pl.lit('OTHER').alias('product')]:
        with pytest.raises(ValueError,match='unannounced, ended or missing'):
            bind_entry_specifications(source,specs.with_columns(update))


def test_all_twd_manifest_requires_exact_specification_payload(tmp_path):
    source,specs=_bound_source()
    daily,rules=tmp_path/'daily.parquet',tmp_path/'rules.parquet'
    daily.write_bytes(b'daily');rules.write_bytes(b'rules')
    path=tmp_path/'entries.parquet'
    pl.DataFrame(dict(date=[date(2026,1,5)],physical_contract=['TGF:202602'],symbol=['slot1'],
        entry_quote=[100.],entry_volume=[2.],status=['observed_minute'])).write_parquet(path)
    folder=tmp_path/'entry_specifications';folder.mkdir()
    specs.write_parquet(folder/'specifications.parquet')
    proof=dict(dataset='taifex_dated_product_specifications',outputs={
        'specifications.parquet':dict(sha256=sha256_file(folder/'specifications.parquet'))})
    (folder/'manifest.json').write_text(json.dumps(proof))
    manifest=dict(dataset=ALL_TWD_CLOCK_DATASET,clock=decision_clock_contract('08:45',asset_scope='all_twd'),
        status='complete',rows=1,start_date='2026-01-05',end_date='2026-01-05',
        entries_sha256=sha256_file(path),source_daily_sha256=sha256_file(daily),source_rules_sha256=sha256_file(rules))
    path.with_name('manifest.json').write_text(json.dumps(manifest))
    with pytest.raises(ValueError,match='specifications'):
        validate_clock_source(path,daily_path=daily,rules_path=rules,decision_time='08:45')
    manifest['entry_specifications']=dict(directory=folder.name,
        data_sha256=sha256_file(folder/'specifications.parquet'),manifest_sha256=sha256_file(folder/'manifest.json'))
    path.with_name('manifest.json').write_text(json.dumps(manifest))
    validate_clock_source(path,daily_path=daily,rules_path=rules,decision_time='08:45')
    (folder/'manifest.json').write_text(json.dumps(proof)+' ')
    with pytest.raises(ValueError,match='specifications changed'):
        validate_clock_source(path,daily_path=daily,rules_path=rules,decision_time='08:45')
    assert clock_dataset('all_twd')==ALL_TWD_CLOCK_DATASET
    assert decision_clock_contract('09:00',asset_scope='all_twd',window_minutes=300)['version']==5
    assert decision_clock_contract('08:45')['version']==1
    assert decision_clock_contract('08:45',asset_scope='twd_equity')['version']==2


@pytest.mark.parametrize('tick,price,product,asset,close',[
    (.5,100.5,'TGF','commodity_future','16:15:00'),
    (.005,98.005,'CPF','interest_rate_future','12:00:00'),
])
def test_bulk_and_continuous_sources_share_dated_session_and_subcent_grid(tick,price,product,asset,close):
    bound,_=_bound_source(product,asset,tick,close)
    day=bound['date'][0]
    expected=bound.with_columns(pl.lit(price).alias('high'),pl.lit(price).alias('low'),
        pl.lit(price).alias('close'),pl.lit(2.).alias('outright_volume'),pl.lit(0.).alias('block_volume'))
    # Preserve Float32 native bits, exclude the print one second after the
    # product's close even if the stock session would classify it differently.
    end=datetime.fromisoformat(str(day)+'T'+close)
    from datetime import timedelta
    times=[end,end+timedelta(seconds=1)]
    raw=pl.DataFrame(dict(date=[str(t) for t in times],futures_id=[product]*2,
        contract_date=['202602']*2,price=[float(np.float32(price))]*2,volume=[4.,400.]))
    bars,coverage,_=normalize_bulk_stock_ticks(raw,day=day,expected=expected,source_sha256='raw',asset_scope='all_twd')
    assert coverage['verified'].to_list()==[True] and coverage['tick_volume'].to_list()==[2]
    assert bars['vwap'].to_list()==pytest.approx([price])
    continuous=pl.DataFrame(dict(event_ts=times,ts=[int(np.datetime64(t,'ns').astype(np.int64)) for t in times],
        close=[price]*2,volume=[2,200],trading_date=[day]*2,query_contract=[product+'R1']*2,source_row_index=[0,1])).with_columns(
            pl.col('event_ts').cast(pl.Datetime('ns')))
    b,stats=normalize_continuous_ticks(continuous,day=day,alias=product+'R1',physical=product+':202602',
        digest='raw',product=product,asset_class=asset,dated_specification=bound)
    assert stats['tick_volume']==2 and b['vwap'].to_list()==pytest.approx([price])
    expiry=bound.with_columns(pl.lit(day).alias('official_expiry'))
    assert dated_entry_sessions(expiry)['_end'][0]==12*3600


def test_explicit_ticks_reject_ambiguous_float32_or_unbound_source():
    with pytest.raises(ValueError,match='no unique'):
        decode_bulk_stock_prices(np.array([1e8],dtype=np.float32).astype(np.float64),day=date(2026,1,5),
            products=np.array(['CPF']),tick_sizes=np.array([.005]))
    bound,_=_bound_source()
    with pytest.raises(ValueError,match='bound physical'):
        dated_entry_sessions(bound.with_columns(pl.lit(False).alias('specification_bound')))


def test_unknown_final_session_only_admits_common_hours_after_full_book_reconciliation():
    bound, _ = _bound_source()
    day = bound['date'][0]
    unknown = bound.with_columns(pl.lit(None, dtype=pl.Date).alias('official_expiry'))
    assert dated_entry_sessions(unknown)['_end'].to_list() == [12 * 3600]
    assert dated_entry_sessions(unknown.head(0)).height == 0
    with pytest.raises(ValueError, match='after its recorded final date'):
        dated_entry_sessions(bound.with_columns(pl.lit(date(2026, 1, 4)).alias('official_expiry')))
    with pytest.raises(ValueError, match='unknown date or session boundary'):
        dated_entry_sessions(unknown.with_columns(pl.lit(None, dtype=pl.String).alias('spec_expiry_time')))
    with pytest.raises(ValueError, match='unknown final-day role'):
        dated_entry_sessions(unknown.with_columns(
            pl.lit(None, dtype=pl.String).alias('spec_final_cash_accounting_policy')))
    deferred = bound.with_columns(
        pl.lit('official_final_day_end_v1').alias('spec_final_cash_accounting_policy'))
    assert dated_entry_sessions(deferred)['_end'].to_list() == [12 * 3600]
    assert dated_entry_sessions(bound)['_end'].to_list() == [16 * 3600 + 15 * 60]

    expected = unknown.with_columns(pl.lit(100.5).alias('high'), pl.lit(100.5).alias('low'),
        pl.lit(100.5).alias('close'), pl.lit(1.).alias('outright_volume'), pl.lit(0.).alias('block_volume'))
    common_print = pl.DataFrame(dict(date=['2026-01-05 08:45:01'], futures_id=['TGF'],
        contract_date=['202602'], price=[100.5], volume=[2]))
    bars, coverage, _ = normalize_bulk_stock_ticks(common_print, day=day, expected=expected,
        source_sha256='a' * 64, asset_scope='all_twd')
    assert coverage['verified'].to_list() == [True]
    assert bars['volume'].to_list() == [1.]
    # If the official full book includes a later print, the conservative
    # intersection is incomplete. Never convert that uncertainty into a fill
    # or a verified empty session.
    late = pl.DataFrame(dict(date=['2026-01-05 12:30:00'], futures_id=['TGF'],
        contract_date=['202602'], price=[100.5], volume=[2]))
    bars, coverage, events = normalize_bulk_stock_ticks(pl.concat([common_print, late]), day=day,
        expected=expected.with_columns(pl.lit(2.).alias('outright_volume')),
        source_sha256='a' * 64, asset_scope='all_twd')
    assert coverage['verified'].to_list() == [False]
    assert bars.height == 0 and events.height == 0


def test_official_original_union_requires_complete_daily_book(tmp_path):
    import zipfile
    from stockagent.data.tw_futures_official_ticks import pin_official_tick_inventory, read_official_tick_day

    bound, _ = _bound_source()
    day = bound['date'][0]
    source = bound.with_columns(pl.lit(None, dtype=pl.Date).alias('official_expiry'),
        pl.lit(100.5).alias('high'), pl.lit(100.5).alias('low'),
        pl.lit(100.5).alias('close'), pl.lit(2).alias('outright_volume'),
        pl.lit(0).alias('block_volume'))
    assert dated_entry_sessions(source)['_end'].to_list() == [12 * 3600]
    assert dated_entry_sessions(source, unknown_final_mode='union_for_full_book_verification')['_end'].to_list() == [16 * 3600 + 15 * 60]
    with pytest.raises(ValueError, match='unsupported unknown-final'):
        dated_entry_sessions(source, unknown_final_mode='guess')
    archive = tmp_path / 'Daily_2026_01_05.zip'
    with zipfile.ZipFile(archive, 'w') as zipped:
        zipped.writestr('Daily_2026_01_05.csv',
            'date,product,month,time,price,quantity,near,far,opening\n'
            '20260105,TGF,202602,084501,100.5,2,-,-,*\n'
            '20260105,TGF,202602,123000,100.5,2,-,-,\n'
            '20260105,TGF,202602,161601,100.5,2,-,-,\n')
    receipt = tmp_path / 'manifest.json'
    record = dict(kind='futures', trading_date=str(day), path=str(archive), bytes=archive.stat().st_size,
        sha256=sha256_file(archive), url='https://www.taifex.com.tw/file/taifex/Dailydownload/'
        'DailydownloadCSV/Daily_2026_01_05.zip')
    receipt.write_text(json.dumps(dict(dataset='taifex_recent_tx_txo_trade_by_trade',
                                       status='complete', raw_downloads=[record])))
    item = pin_official_tick_inventory(receipt, [day])[0]
    bars, coverage, events = read_official_tick_day(item, source)
    assert coverage['verified'].to_list() == [True]
    assert coverage['tick_volume'].to_list() == [2]
    assert bars['volume'].sum() == 2
    assert not events.is_empty()
    # A common-hours book that already matches is retained even if the raw
    # archive has a later non-day print inside the wider candidate interval.
    bars, coverage, events = read_official_tick_day(item,
        source.with_columns(pl.lit(1).alias('outright_volume')))
    assert coverage['verified'].to_list() == [True]
    assert bars['volume'].sum() == 1
    # If neither candidate matches the official day book, neither may be used
    # to infer a fill or verified empty window.
    bars, coverage, events = read_official_tick_day(item,
        source.with_columns(pl.lit(3).alias('outright_volume')))
    assert coverage['verified'].to_list() == [False]
    assert bars.is_empty() and events.is_empty()
    # With a known same-day final session, the later print is correctly outside
    # that session and the one-contract official day book can be certified.
    bars, coverage, _ = read_official_tick_day(item, source.with_columns(
        pl.lit(day).alias('official_expiry'), pl.lit(1).alias('outright_volume')))
    assert coverage['verified'].to_list() == [True]
    assert bars['volume'].sum() == 1


def test_official_supplement_repairs_only_full_book_verified_gaps(tmp_path):
    import zipfile
    from scripts.prepare_tw_futures_decision_clocks import supplement_official_recent, paired_entries

    bound, _ = _bound_source()
    day = bound['date'][0]
    source = pl.concat([bound.with_columns(pl.lit(month).alias('contract'),
                        pl.lit('TGF:' + month).alias('physical_contract'),
                        pl.lit(None, dtype=pl.Date).alias('official_expiry'),
                        pl.lit(100.5).alias('high'), pl.lit(100.5).alias('low'),
                        pl.lit(100.5).alias('close'), pl.lit(100.5).alias('observed_open'),
                        pl.lit(volume).alias('outright_volume'), pl.lit(0).alias('block_volume'),
                        pl.lit(True).alias('executable'), pl.lit(month).alias('symbol'))
                        for month, volume in [('202602', 2), ('202603', 3), ('202604', 1)]])
    raw = pl.DataFrame(dict(date=[f'{day} 08:47:01'] * 3, futures_id=['TGF'] * 3,
        contract_date=['202602', '202603', '202604'], price=[100.5] * 3, volume=[2] * 3))
    minutes, coverage, trades = normalize_bulk_stock_ticks(raw, day=day, expected=source,
        source_sha256='b' * 64, asset_scope='all_twd')
    assert coverage.sort('contract')['verified'].to_list() == [False, False, True]
    archive = tmp_path / 'Daily_2026_01_05.zip'
    with zipfile.ZipFile(archive, 'w') as zipped:
        zipped.writestr('Daily_2026_01_05.csv',
            'date,product,month,time,price,quantity,near,far,opening\n'
            '20260105,TGF,202602,084701,100.5,2,-,-,*\n'
            '20260105,TGF,202602,123000,100.5,2,-,-,\n'
            '20260105,TGF,202603,084701,100.5,2,-,-,*\n'
            '20260105,TGF,202603,123000,100.5,2,-,-,\n'
            '20260105,TGF,202604,084701,100.5,2,-,-,*\n')
    receipt = tmp_path / 'manifest.json'
    record = dict(kind='futures', trading_date=str(day), path=str(archive), bytes=archive.stat().st_size,
        sha256=sha256_file(archive), url='https://www.taifex.com.tw/file/taifex/Dailydownload/'
        'DailydownloadCSV/Daily_2026_01_05.zip')
    receipt.write_text(json.dumps(dict(dataset='taifex_recent_tx_txo_trade_by_trade',
                                       status='complete', raw_downloads=[record])))
    # The block quantities come from the source coverage, as in the builder.
    rebuilt, verified, updated, events, proof = supplement_official_recent(
        receipt, source.drop('block_volume'), minutes, coverage, trades,
        progress=lambda **_: None, workers=1, window_minutes=300)
    assert updated.sort('contract')['verified'].to_list() == [True, False, True]
    assert updated.sort('contract')['source_file_sha256'].to_list() == [sha256_file(archive), 'b' * 64, 'b' * 64]
    assert proof['accepted_contract_days'] == 1 and proof['fill_verified'] is False
    assert set(verified['physical_contract']) == {'TGF:202602', 'TGF:202604'}
    assert set(events['source_sha256']) == {sha256_file(archive), 'b' * 64}
    entries = paired_entries(source, rebuilt, verified, asset_scope='all_twd', window_minutes=300)
    assert entries['08:45'].sort('physical_contract')['status'].to_list() == [
        'observed_minute', 'missing_source', 'observed_minute']
    archive.write_bytes(archive.read_bytes() + b'x')
    with pytest.raises(ValueError, match='SHA|size'):
        supplement_official_recent(receipt, source.drop('block_volume'), minutes, coverage, trades,
                                  progress=lambda **_: None, workers=1, window_minutes=300)


@pytest.mark.parametrize('price', [49.99, 50.05])
def test_bound_etf_specification_controls_grid_after_legacy_registry_window(price):
    day = date(2026, 10, 6)
    spec = pl.DataFrame([specification(product='NYF', tick_kind='etf_future',
        tick_size=None)], schema=SPEC_FIELDS)
    source = pl.DataFrame(dict(date=[day], product=['NYF'], contract=['202611'],
        physical_contract=['NYF:202611'], asset_class=['etf_future'],
        official_expiry=[date(2026, 11, 18)], high=[price], low=[price], close=[price],
        outright_volume=[1.], block_volume=[0.]))
    expected = bind_entry_specifications(source, spec)
    raw = pl.DataFrame(dict(date=['2026-10-06 08:45:01'], futures_id=['NYF'],
        contract_date=['202611'], price=pl.Series([price], dtype=pl.Float32), volume=[2]))
    bars, coverage, events = normalize_bulk_stock_ticks(raw, day=day, expected=expected,
        source_sha256='a' * 64, asset_scope='all_twd')
    assert coverage['verified'].to_list() == [True]
    assert bars['vwap'].to_list() == pytest.approx([price])
    assert bars['volume'].to_list() == [1.]
    assert events['source_sha256'].unique().to_list() == ['a' * 64]
    # Per-product FinMind queries retain Float64 decimal values rather than
    # the whole-market object's Float32 encoding. Both must validate against
    # the same dated grid without replacing the native source value.
    decimal_raw = raw.with_columns(pl.lit(price, dtype=pl.Float64).alias('price'))
    decimal_bars, decimal_coverage, _ = normalize_bulk_stock_ticks(decimal_raw, day=day,
        expected=expected, source_sha256='b' * 64, asset_scope='all_twd',
        price_representation='native_decimal')
    assert decimal_coverage['verified'].to_list() == [True]
    assert decimal_bars['vwap'].to_list() == pytest.approx([price])
    with pytest.raises(ValueError, match='bound physical'):
        normalize_bulk_stock_ticks(raw, day=day,
            expected=expected.with_columns(pl.lit(False).alias('specification_bound')),
            source_sha256='a' * 64, asset_scope='all_twd')
    with pytest.raises(ValueError, match='kind and asset class disagree'):
        normalize_bulk_stock_ticks(raw, day=day,
            expected=expected.with_columns(pl.lit('index_future').alias('asset_class')),
            source_sha256='a' * 64, asset_scope='all_twd')


def test_bound_etf_specification_rejects_off_grid_quote():
    day = date(2026, 10, 6)
    spec = pl.DataFrame([specification(product='NYF', tick_kind='etf_future',
        tick_size=None)], schema=SPEC_FIELDS)
    source = pl.DataFrame(dict(date=[day], product=['NYF'], contract=['202611'],
        physical_contract=['NYF:202611'], asset_class=['etf_future'],
        official_expiry=[date(2026, 11, 18)], high=[50.01], low=[50.01], close=[50.01],
        outright_volume=[1.], block_volume=[0.]))
    raw = pl.DataFrame(dict(date=['2026-10-06 08:45:01'], futures_id=['NYF'],
        contract_date=['202611'], price=pl.Series([50.01], dtype=pl.Float32), volume=[2]))
    with pytest.raises(ValueError, match='no unique'):
        normalize_bulk_stock_ticks(raw, day=day, expected=bind_entry_specifications(source, spec),
            source_sha256='a' * 64, asset_scope='all_twd')


def test_product_day_fallback_keeps_native_provenance_and_unknown_distinct_from_zero(tmp_path, monkeypatch):
    import scripts.prepare_tw_futures_decision_clocks as builder
    import stockagent.data.tw_stock_futures_repair as repair
    from polars.testing import assert_frame_equal
    from stockagent.data.tw_futures_finmind_ticks import (
        PRODUCT_FALLBACK_INVENTORY, pin_mixed_tick_inventory, read_pinned_tick_day,
    )

    day = date(2026, 10, 6)
    collector = tmp_path / 'collector'
    collector.mkdir()
    raw_path = collector / 'NYF.parquet'
    raw = pl.DataFrame(dict(date=['2026-10-06 08:45:01'], futures_id=['NYF'],
        contract_date=['202611'], price=pl.Series([49.99], dtype=pl.Float64), volume=[2]))
    raw.write_parquet(raw_path)
    receipt_path = collector / 'NYF.json'
    receipt = dict(dataset='TaiwanFuturesTick', data_id='NYF', partition=str(day), status='complete',
        parquet_path=raw_path.name, sha256=sha256_file(raw_path), rows=1,
        parquet_size_bytes=raw_path.stat().st_size, historical_point_in_time=False)
    receipt_path.write_text(json.dumps(receipt))
    manifest_path = tmp_path / 'fallback.json'
    manifest_path.write_text(json.dumps(dict(contract=PRODUCT_FALLBACK_INVENTORY,
        dataset='TaiwanFuturesTick', objects=[dict(product='NYF', date=str(day),
            receipt_path=receipt_path.name, receipt_sha256=sha256_file(receipt_path),
            sha256=receipt['sha256'], rows=1)])))
    products = ['NYF', 'NIF', 'OAF']
    specs = pl.DataFrame([specification(product=p, tick_kind='etf_future', tick_size=None)
                          for p in products], schema=SPEC_FIELDS)
    source = pl.DataFrame(dict(date=[day] * 3, product=products, contract=['202611'] * 3,
        physical_contract=[p + ':202611' for p in products], asset_class=['etf_future'] * 3,
        official_expiry=[date(2026, 11, 18)] * 3, high=[49.99, None, 20.],
        low=[49.99, None, 20.], close=[49.99, None, 20.],
        outright_volume=[1., 0., 1.], block_volume=[0., 0., 0.]))
    expected = bind_entry_specifications(source, specs)
    item = pin_mixed_tick_inventory(collector, [day], manifest_path)[0]
    bars, coverage, events = read_pinned_tick_day(item, expected, asset_scope='all_twd')
    by_product = {row['product']: row for row in coverage.to_dicts()}
    assert by_product['NYF']['verified'] is True
    assert by_product['NYF']['source_file_sha256'] == receipt['sha256']
    assert by_product['NIF']['verified'] is True  # independent official zero book
    assert by_product['NIF']['source_file_sha256'] is None
    assert by_product['OAF']['verified'] is False  # positive official book but no source
    assert bars['physical_contract'].to_list() == ['NYF:202611']
    assert events['source_sha256'].unique().to_list() == [receipt['sha256']]

    # Exercise the production loader and its checkpoint, not just its reader.
    # The synthetic block evidence is already present in this fixture; actual
    # daily receipts are read through the shared block implementation.
    block_manifest = tmp_path / 'blocks.json'
    block_manifest.write_text('{}')
    monkeypatch.setattr(repair, 'apply_block_evidence', lambda frame, path: (frame, {}))
    facts = expected.with_columns(pl.lit(True).alias('executable'),
        pl.lit(2).alias('tenor_rank'), pl.col('physical_contract').alias('symbol'))
    progress = []
    options = dict(progress=lambda **event: progress.append(event), workers=1,
        asset_scope='all_twd', window_minutes=300, entry_windows=[1, 300],
        checkpoint_root=tmp_path / 'checkpoint', product_fallback_manifest=manifest_path)
    first_entries, replay_entries = {}, {}
    first = builder.load_bulk_minutes(collector, block_manifest, [], facts,
        prepared_entries=first_entries, **options)
    second = builder.load_bulk_minutes(collector, block_manifest, [], facts,
        prepared_entries=replay_entries, **options)
    assert any(event['stage'] == 'bulk_checkpoint_reused' for event in progress)
    assert first[2]['source'] == 'finmind_whole_market_and_exact_product_dated_all_twd_prints_v2'
    assert_frame_equal(first[3], coverage)
    assert_frame_equal(first[4], events)
    for index in (0, 1, 3, 4):
        assert_frame_equal(first[index], second[index])
    for window in (1, 300):
        for clock in ('08:45', '09:00'):
            assert_frame_equal(first_entries[window][clock], replay_entries[window][clock])
    raw_path.write_bytes(raw_path.read_bytes() + b'changed')
    with pytest.raises(ValueError, match='original SHA mismatch'):
        read_pinned_tick_day(item, expected, asset_scope='all_twd')
    with pytest.raises(ValueError, match='original raw SHA mismatch'):
        builder.load_bulk_minutes(collector, block_manifest, [], facts, prepared_entries={}, **options)


def test_official_recent_archive_fallback_uses_original_and_rechecks_checkpoint(tmp_path, monkeypatch):
    import zipfile
    import scripts.prepare_tw_futures_decision_clocks as builder
    import stockagent.data.tw_stock_futures_repair as repair
    from stockagent.data.tw_futures_finmind_ticks import pin_mixed_tick_inventory, read_pinned_tick_day

    day = date(2026, 10, 6)
    archive = tmp_path / 'Daily_2026_10_06.zip'
    with zipfile.ZipFile(archive, 'w') as zipped:
        zipped.writestr('Daily_2026_10_06.csv',
            'date,product,month,time,price,quantity,near,far,opening\n'
            '20261006,NYF,202611,084501,49.99,2,-,-,*\n'
            '20261006,NYF,202611,090001,50.05,4,-,-,\n')
    receipt = tmp_path / 'official.json'
    record = dict(kind='futures', trading_date=str(day), path=str(archive), bytes=archive.stat().st_size,
        sha256=sha256_file(archive), url='https://www.taifex.com.tw/file/taifex/Dailydownload/'
        'DailydownloadCSV/Daily_2026_10_06.zip')
    receipt.write_text(json.dumps(dict(dataset='taifex_recent_tx_txo_trade_by_trade',
                                       status='complete', raw_downloads=[record])))
    spec = pl.DataFrame([specification(product='NYF', tick_kind='etf_future', tick_size=None)],
                        schema=SPEC_FIELDS)
    source = pl.DataFrame(dict(date=[day], product=['NYF'], contract=['202611'],
        physical_contract=['NYF:202611'], asset_class=['etf_future'],
        official_expiry=[date(2026, 11, 18)], high=[50.05], low=[49.99], close=[50.05],
        outright_volume=[3.], block_volume=[0.]))
    source = bind_entry_specifications(source, spec).with_columns(
        pl.lit(True).alias('executable'), pl.lit(2).alias('tenor_rank'),
        pl.col('physical_contract').alias('symbol'))
    items = pin_mixed_tick_inventory(tmp_path, [day], official_manifest=receipt)
    bars, coverage, events = read_pinned_tick_day(items[0], source, asset_scope='all_twd')
    assert coverage['verified'].to_list() == [True]
    assert bars['volume'].to_list() == [1., 2.]
    assert events['source_sha256'].unique().to_list() == [record['sha256']]
    missing = source.with_columns(pl.lit('OAF').alias('product'),
        pl.lit('OAF:202611').alias('physical_contract'))
    missing_bars, missing_coverage, missing_events = read_pinned_tick_day(items[0], missing, asset_scope='all_twd')
    assert missing_bars.is_empty() and missing_events.is_empty()
    assert missing_coverage['verified'].to_list() == [False]
    zero = missing.with_columns(pl.lit(0.).alias('outright_volume'))
    _, zero_coverage, _ = read_pinned_tick_day(items[0], zero, asset_scope='all_twd')
    assert zero_coverage['verified'].to_list() == [True]

    blocks = tmp_path / 'blocks.json'
    blocks.write_text('{}')
    monkeypatch.setattr(repair, 'apply_block_evidence', lambda frame, path: (frame, {}))
    progress = []
    options = dict(progress=lambda **event: progress.append(event), workers=1,
        asset_scope='all_twd', window_minutes=300, entry_windows=[1, 300],
        checkpoint_root=tmp_path / 'checkpoints', official_fallback_manifest=receipt)
    first = builder.load_bulk_minutes(tmp_path, blocks, [], source, prepared_entries={}, **options)
    second = builder.load_bulk_minutes(tmp_path, blocks, [], source, prepared_entries={}, **options)
    assert first[3]['verified'].to_list() == [True]
    assert first[2]['official_source'] == 'taifex_recent_original_all_product_futures_tick_v1'
    assert first[3].equals(second[3])
    assert any(event['stage'] == 'bulk_checkpoint_reused' for event in progress)
    archive.write_bytes(archive.read_bytes() + b'changed')
    with pytest.raises(ValueError, match='original raw SHA mismatch'):
        builder.load_bulk_minutes(tmp_path, blocks, [], source, prepared_entries={}, **options)

    record['url'] = 'https://example.invalid/fake.zip'
    receipt.write_text(json.dumps(dict(dataset='taifex_recent_tx_txo_trade_by_trade',
                                       status='complete', raw_downloads=[record])))
    with pytest.raises(ValueError, match='identity, URL, or receipt'):
        pin_mixed_tick_inventory(tmp_path, [day], official_manifest=receipt)
