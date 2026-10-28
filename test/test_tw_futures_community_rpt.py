from datetime import date
import json
from pathlib import Path
import zipfile

import polars as pl
import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_futures_community_rpt import (
    INVENTORY, SOURCE, SourceChangedError, pin_community_rpt_inventory, read_community_rpt_day,
)


DAY = date(2020, 3, 23)


def expected():
    return pl.DataFrame(dict(date=[DAY]*2, product=['TX']*2, contract=['202004','202005'],
        physical_contract=['TX:202004','TX:202005'], asset_class=['index_future']*2,
        high=[9002.,10000.], low=[9001.,10000.], close=[9002.,10000.],
        observed_open=[9001.,10000.], outright_volume=[3,3], block_volume=[0,0],
        official_expiry=[date(2020,4,15),date(2020,5,20)], specification_bound=[True]*2,
        spec_opening_time=['08:45:00']*2, spec_settlement_time=['13:45:00']*2,
        spec_expiry_time=['13:30:00']*2, spec_tick_kind=['fixed']*2, spec_tick_size=[1.]*2,
        spec_final_cash_accounting_policy=['expiry_event_time']*2,
        executable=[True]*2, symbol=['s1','s2']))


def archive(tmp_path, first='9001', last='9002', second_quantity=2, extra=()):
    raw = tmp_path/'raw'; raw.mkdir(exist_ok=True)
    path = raw/'Daily_2020_03_23.zip'
    rows = [f'20200323,TX,202004,084500,{first},2,,',
            f'20200323,TX,202004,084500,{last},2,,',
            f'20200323,TX,202004,090100,{last},2,,',
            f'20200323,TX,202005,084500,10000,{second_quantity},,',
            '20200320,TX,202004,150000,9005,200,,',
            '20200323,TX,202004/202005,090000,-1,100,,']
    rows.extend(extra)
    with zipfile.ZipFile(path, 'w', compression=zipfile.ZIP_STORED) as z:
        z.writestr('Daily_2020_03_23.rpt', '\n'.join(rows).encode('cp950'))
    item = dict(date=str(DAY), path='raw/'+path.name, sha256=sha256_file(path),
                bytes=path.stat().st_size, drive_id='public_file_id')
    inv = tmp_path/'inventory.json'
    atomic_write_json(inv, dict(contract=INVENTORY, source=SOURCE,
        historical_point_in_time_verified=False, catalog_sha256='a'*64, objects=[item]))
    return path, inv


def test_full_session_admission_keeps_real_open_and_rejects_volume_deficit(tmp_path):
    path, inv = archive(tmp_path)
    items, proof = pin_community_rpt_inventory(inv, [DAY])
    bars, coverage, events, detail = read_community_rpt_day(items[0], expected())
    assert proof['historical_point_in_time_verified'] is False
    assert coverage['physical_contract'].to_list() == ['TX:202004']
    assert detail['native_rows'] == 4 and detail['archive_crc'] == 'verified_by_full_member_read'
    assert bars.sort('minute')['volume'].to_list() == [2.,1.]
    opening = events.filter(pl.col('is_session_open')).row(0, named=True)
    assert opening['price'] == 9001. and opening['source_sha256'] == sha256_file(path)
    assert opening['opening_ambiguous'] is False


def test_first_second_without_official_open_match_stays_quarantined(tmp_path):
    _, inv = archive(tmp_path)
    items, _ = pin_community_rpt_inventory(inv, [DAY])
    facts = expected().with_columns(pl.when(pl.col('contract') == '202004')
        .then(9010.).otherwise(pl.col('observed_open')).alias('observed_open'))
    bars, coverage, events, _ = read_community_rpt_day(items[0], facts)
    assert coverage.is_empty() and bars.is_empty() and events.is_empty()


def test_native_off_grid_quote_is_rejected_without_rounding(tmp_path):
    _, inv = archive(tmp_path, first='9001.25')
    items, _ = pin_community_rpt_inventory(inv, [DAY])
    with pytest.raises(ValueError, match='legal|identical'):
        read_community_rpt_day(items[0], expected())


def test_official_block_detail_removes_only_uniquely_matched_prints(tmp_path):
    _, inv = archive(tmp_path, extra=['20200323,TX,202004,100000,9002,400,,'])
    items, _ = pin_community_rpt_inventory(inv, [DAY])
    facts = expected().with_columns(pl.when(pl.col('contract') == '202004').then(200)
        .otherwise(pl.col('block_volume')).alias('block_volume'))
    detail = [dict(product='TX', contract='202004', price='9002', quantity=200)]
    assert read_community_rpt_day(items[0], facts)[1].is_empty()
    bars, coverage, _, proof = read_community_rpt_day(items[0], facts, official_block_details=detail)
    assert coverage['physical_contract'].to_list() == ['TX:202004']
    assert bars['volume'].sum() == 3.
    assert proof['block_checks'][0]['status'] == 'official_detail_unique_exact_prints_excluded'


def test_ambiguous_same_price_quantity_never_guesses_block_identity(tmp_path):
    _, inv = archive(tmp_path, extra=['20200323,TX,202004,100000,9002,400,,',
                                     '20200323,TX,202004,110000,9002,400,,'])
    items, _ = pin_community_rpt_inventory(inv, [DAY])
    facts = expected().with_columns(pl.when(pl.col('contract') == '202004').then(200)
        .otherwise(pl.col('block_volume')).alias('block_volume'),
        pl.when(pl.col('contract') == '202004').then(203)
        .otherwise(pl.col('outright_volume')).alias('outright_volume'))
    detail = [dict(product='TX', contract='202004', price='9002', quantity=200)]
    _, coverage, _, proof = read_community_rpt_day(items[0], facts, official_block_details=detail)
    assert coverage.is_empty()
    assert proof['block_checks'][0]['status'] == 'ambiguous_or_missing_exact_block_print'


def test_crc_failure_and_pinned_source_mutation_are_distinct(tmp_path):
    path, inv = archive(tmp_path)
    damaged = path.read_bytes().replace(b'9001', b'9003', 1)
    path.write_bytes(damaged)
    item = json.loads(inv.read_text())['objects'][0]
    item['sha256'] = sha256_file(path)
    atomic_write_json(inv, dict(contract=INVENTORY, source=SOURCE,
        historical_point_in_time_verified=False, catalog_sha256='a'*64, objects=[item]))
    items, _ = pin_community_rpt_inventory(inv, [DAY])
    with pytest.raises(zipfile.BadZipFile, match='CRC'):
        read_community_rpt_day(items[0], expected())
    path.write_bytes(damaged + b'x')
    with pytest.raises(SourceChangedError, match='SHA|size'):
        read_community_rpt_day(items[0], expected())


def test_supplement_updates_only_exact_verified_days_and_long_windows(tmp_path):
    from scripts.prepare_tw_futures_decision_clocks import supplement_community_rpt, paired_entries
    _, inv = archive(tmp_path)
    items, _ = pin_community_rpt_inventory(inv, [DAY])
    valid_bars, valid_coverage, valid_events, _ = read_community_rpt_day(items[0], expected())
    # A known contract and an unresolved contract-day coexist in the old tape.
    missing = valid_coverage.with_columns(pl.lit('TX:202005').alias('physical_contract'),
        pl.lit('202005').alias('contract'), pl.lit(10000.).alias('high'),
        pl.lit(10000.).alias('low'), pl.lit(10000.).alias('close'),
        pl.lit(False).alias('verified'))
    coverage = pl.concat([valid_coverage.with_columns(pl.lit(False).alias('verified')), missing])
    empty_bars, empty_events = valid_bars.head(0), valid_events.head(0)
    minutes, verified, updated, events, proof = supplement_community_rpt(inv, expected(),
        empty_bars, coverage, empty_events, progress=lambda **kw: None, workers=1, window_minutes=300)
    assert verified['physical_contract'].to_list() == ['TX:202004']
    assert updated.filter(pl.col('physical_contract') == 'TX:202005')['verified'].item() is False
    assert proof['accepted_contract_days'] == 1 and proof['source_is_official_publication'] is False
    rows = paired_entries(expected(), minutes, verified, asset_scope='all_twd', window_minutes=300)
    assert rows['08:45'].filter(pl.col('physical_contract') == 'TX:202004')['status'].item() == 'observed_minute'
    assert rows['08:45'].filter(pl.col('physical_contract') == 'TX:202005')['status'].item() == 'missing_source'
    assert events['source_sha256'].unique().to_list() == [items[0]['sha256']]
