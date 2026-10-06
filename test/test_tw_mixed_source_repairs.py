"""Source repair evidence must survive the actual publication projection."""
import json
from pathlib import Path

import polars as pl
import pytest

from scripts import stage_tw_day_trade_mixed_sources as stager
from scripts.prepare_tw_day_trade_feature_catalog import sha256, write_csv
from scripts.prepare_tw_day_trade_mixed_frequency import verify_sources
from stockagent.data.tw_public_cross_source_fill import CONTRACT
from stockagent.data.tw_public_release_schedule import feature_name


def repaired_source(tmp_path, monkeypatch):
    public = tmp_path / 'public'
    (public / 'features').mkdir(parents=True)
    (public / 'stocks').mkdir()
    pl.DataFrame({'date': ['2024-03-11'], 'symbol': ['2330']}).write_parquet(public / 'features/tw_public_stock_daily.parquet')
    (public / 'features/tw_public_stock_daily.summary.json').write_text(json.dumps({
        'output_receipt': {'sha256': sha256(public / 'features/tw_public_stock_daily.parquet')},
        'requested_end_date': '2024-03-11'}))
    (public / 'tw_corporate_action_entitlements.summary.json').write_text('{}')
    (public / 'download_summary.json').write_text('{}')
    (public / 'stocks/official_symbol_build_report.csv').write_text('symbol\n2330\n')
    for path in ('twse_taiex_ohlc.parquet', 'stocks/2330_features.parquet'):
        pl.DataFrame({'date': ['2024-03-11']}).write_parquet(public / path)
    monkeypatch.setattr(stager, '_required_formal_members', lambda *a: ['skip-summary'])
    monkeypatch.setattr(stager, 'PHYSICAL_PUBLIC_RELATIVE_MEMBERS', [])
    monkeypatch.setattr(stager, 'classify_candidate', lambda row: {'decision': 'quarantined_pit'})
    key = 'monthly_revenue:當月營收'
    finlab = tmp_path / 'finlab'
    (finlab / 'datasets').mkdir(parents=True)
    (finlab / 'receipts').mkdir()
    source = finlab / 'datasets/monthly.parquet'
    original = pl.DataFrame({'source_index': ['2024-02-10', '2024-03-10'], '2330': [0., None]})
    original.write_parquet(source)
    receipt = finlab / 'receipts/monthly.json'
    receipt.write_text(json.dumps({'dataset': key, 'sha256': sha256(source), 'parquet_path': 'datasets/monthly.parquet'}))
    catalog = tmp_path / 'catalog.csv'
    write_csv(catalog, [{'dataset_id': key, 'source_path': str(source), 'receipt_path': str(receipt)}])
    grant = tmp_path / 'grant.json'
    grant.write_text(json.dumps({'private_vastai1T_delivery_authorized': True}))
    bundle = tmp_path / 'bundle'
    bundle.mkdir()
    patched = bundle / 'monthly.parquet'
    original.with_columns(pl.col('2330').fill_null(7.)).write_parquet(patched)
    fills = bundle / 'fills.parquet'
    pl.DataFrame({'source_index': ['2024-03-10'], 'symbol': ['2330'], 'value': [7.]}).write_parquet(fills)
    for name in ('source_receipts', 'mapping_audit'):
        (bundle / (name + '.json')).write_text('{}')
    (bundle / 'bundle_manifest.json').write_text(json.dumps({'contract': CONTRACT,
        'overrides': {key: {'primary_sha256': sha256(source), 'path': str(patched), 'sha256': sha256(patched),
            'fills_path': str(fills), 'fills_sha256': sha256(fills), 'filled_observations': 1}},
        'source_receipts_sha256': sha256(bundle / 'source_receipts.json'),
        'mapping_audit_sha256': sha256(bundle / 'mapping_audit.json'), 'filled_observations': 1}))
    out = tmp_path / 'staged'
    stager.stage(public=public, finlab_catalog=catalog, out=out, authorization=grant, cross_source_bundle=bundle)
    return out, key


def test_text_period_axis_is_not_mistaken_for_nonnumeric_model_quantity(tmp_path, monkeypatch):
    root, key = repaired_source(tmp_path, monkeypatch)
    manifest = verify_sources(root)
    spec = next(s for s in manifest['feature_specs'] if s.get('dataset') == key)
    assert pl.read_parquet(root / spec['path'])['2330'].to_list() == [0., 7.]
    assert manifest['source_repairs']['filled_observations'] == 1
    evidence = manifest['files'][spec['path']]['source_repair']['relative_fills_path']
    assert evidence in manifest['files']


def test_manifest_bundle_count_without_actual_staged_fills_is_rejected(tmp_path, monkeypatch):
    root, key = repaired_source(tmp_path, monkeypatch)
    path = root / 'source_manifest.json'
    manifest = json.loads(path.read_text())
    spec = next(s for s in manifest['feature_specs'] if s.get('dataset') == key)
    manifest['files'][spec['path']].pop('source_repair')
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match='not actually staged'):
        verify_sources(root)


def test_receipt_hash_alone_cannot_cover_missing_actual_fill_writeback(tmp_path, monkeypatch):
    root, key = repaired_source(tmp_path, monkeypatch)
    path = root / 'source_manifest.json'
    manifest = json.loads(path.read_text())
    spec = next(s for s in manifest['feature_specs'] if s.get('dataset') == key)
    table_path = root / spec['path']
    pl.read_parquet(table_path).with_columns(pl.lit(0.).alias('2330')).write_parquet(table_path)
    manifest['files'][spec['path']]['sha256'] = sha256(table_path)
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match='not written'):
        verify_sources(root)


def test_incremental_native_refresh_preserves_previous_release_and_verified_fills(tmp_path, monkeypatch):
    from scripts.stage_tw_day_trade_panel_sources import stage
    from stockagent.data.finlab_acquisition_contract import safe_stem
    root, monthly = repaired_source(tmp_path, monkeypatch)
    key='financial_statement:資產總額'
    raw=tmp_path/'finlab/datasets/assets.parquet'
    previous=pl.DataFrame({'source_index':['2024-Q1','2024-Q2'],'2330':[0.,None]})
    previous.write_parquet(raw)
    projected=root/'finlab/observations/assets.parquet'
    previous.write_parquet(projected)
    mpath=root/'source_manifest.json'
    m=json.loads(mpath.read_text())
    m['feature_specs'].append({'source':'FinLab','dataset':key,'feature':'assets','path':'finlab/observations/assets.parquet'})
    m['files']['finlab/observations/assets.parquet']={'sha256':sha256(projected),'bytes':projected.stat().st_size,
                                                   'original_source_sha256':sha256(raw)}
    m['native_adapters']=[{'kind':'pinned_native_test','path':'twse_taiex_ohlc.parquet'}]
    mpath.write_text(json.dumps(m))
    original_manifest_sha=sha256(mpath)
    current=previous.with_columns(pl.col('2330').fill_null(9.))
    current.write_parquet(raw)
    receipt=tmp_path/'finlab/receipts/assets.json'
    receipt.write_text(json.dumps({'dataset':key,'parquet_path':'datasets/assets.parquet','sha256':sha256(raw)}))
    catalog=tmp_path/'refresh-catalog';catalog.mkdir()
    write_csv(catalog/'finlab_keys.csv',[{'dataset_id':key,'receipt_path':str(receipt)},
        {'dataset_id':monthly,'receipt_path':str(tmp_path/'finlab/receipts/monthly.json')}])
    out=tmp_path/'refreshed'
    result=stage(root,out,catalog,tmp_path/'grant.json',refresh_finlab=True)
    after=verify_sources(out)
    assert sha256(mpath)==original_manifest_sha
    assert pl.read_parquet(projected)['2330'].to_list()==[0.,None]
    assert pl.read_parquet(out/'finlab/observations/assets.parquet')['2330'].to_list()==[0.,9.]
    assert after['native_adapters']==m['native_adapters']
    assert after['source_repairs']['filled_observations']==1
    assert len(after['feature_specs'])==len(m['feature_specs'])
    assert [r['dataset'] for r in result['incremental_refresh']['refreshed_datasets']]==[key]


def test_changed_repaired_primary_requires_new_overlap_bundle(tmp_path, monkeypatch):
    from scripts.stage_tw_day_trade_panel_sources import stage
    root,key=repaired_source(tmp_path,monkeypatch)
    mpath=root/'source_manifest.json';m=json.loads(mpath.read_text())
    m['native_adapters']=[{'kind':'pinned_native_test'}];mpath.write_text(json.dumps(m))
    raw=tmp_path/'finlab/datasets/monthly.parquet'
    pl.read_parquet(raw).with_columns(pl.lit(8.).alias('2330')).write_parquet(raw)
    receipt=tmp_path/'finlab/receipts/monthly.json'
    receipt.write_text(json.dumps({'dataset':key,'parquet_path':'datasets/monthly.parquet','sha256':sha256(raw)}))
    catalog=tmp_path/'refresh-catalog';catalog.mkdir()
    write_csv(catalog/'finlab_keys.csv',[{'dataset_id':key,'receipt_path':str(receipt)}])
    with pytest.raises(ValueError,match='newly validated missing-only bundle'):
        stage(root,tmp_path/'invalid-refresh',catalog,tmp_path/'grant.json',refresh_finlab=True)
    assert not (tmp_path/'invalid-refresh/source_manifest.json').exists()
    assert verify_sources(root)['source_repairs']['filled_observations']==1


def test_staged_snapshot_history_union_is_bound_to_both_originals_and_actual_cells(tmp_path,monkeypatch):
    import shutil
    from scripts.stage_tw_day_trade_panel_sources import stage
    from dataclasses import asdict
    from stockagent.data.tw_public_release_schedule import RULES
    prior,monthly=repaired_source(tmp_path,monkeypatch)
    key='financial_statement:資產總額';relative='finlab/observations/assets.parquet'
    stock={str(1000+n):[1.,1.] for n in range(100)}
    old=pl.DataFrame({'source_index':['2024-Q1','2024-Q2'],**stock,'2330':[0.,7.],'2317':[2.,2.],'9999':[3.,4.]})
    old.write_parquet(prior/relative)
    mpath=prior/'source_manifest.json';m=json.loads(mpath.read_text())
    m['feature_specs'].append({'source':'FinLab','dataset':key,'feature':'assets','path':relative,
        'rule':asdict(RULES['quarter']),'category':'financial'})
    m['files'][relative]={'sha256':sha256(prior/relative),'bytes':(prior/relative).stat().st_size}
    mpath.write_text(json.dumps(m))
    latest=tmp_path/'latest';shutil.copytree(prior,latest)
    current=old.drop('9999').with_columns(pl.Series('2330',[0.,None]),pl.Series('2317',[3.,2.]))
    current.write_parquet(latest/relative)
    latest_manifest=json.loads((latest/'source_manifest.json').read_text())
    latest_manifest['files'][relative]={'sha256':sha256(latest/relative),'bytes':(latest/relative).stat().st_size}
    (latest/'source_manifest.json').write_text(json.dumps(latest_manifest))
    out=tmp_path/'retained'
    result=stage(latest,out,tmp_path/'unused-catalog',tmp_path/'grant.json',retain_prior_source=prior)
    assert result['source_snapshot_retention']['filled_observations']==3
    actual=verify_sources(out)
    assert actual['source_repairs']['filled_observations']==1
    assert pl.read_parquet(out/relative)['2330'].to_list()==[0.,7.]
    assert pl.read_parquet(out/relative)['2317'].to_list()==[3.,2.]
    assert pl.read_parquet(latest/relative)['2330'].to_list()==[0.,None]
    pl.read_parquet(out/relative).with_columns(pl.lit(99.).alias('2330')).write_parquet(out/relative)
    actual['files'][relative]['sha256']=sha256(out/relative)
    (out/'source_manifest.json').write_text(json.dumps(actual))
    with pytest.raises(ValueError,match='observed primary'):
        verify_sources(out)
