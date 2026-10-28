from datetime import date
import csv
import io
import json
from pathlib import Path
import zipfile

import polars as pl
import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from stockagent.data.tw_stock_futures_repair import official_day_evidence


def fixture(tmp_path: Path, *, provider_price=20, extra_spread=False):
    day = date(2020, 1, 2)
    labels = ['202002W1', '202002', '202003W1']
    spread = ['202002W1/202002', '202002W1/202003W1']
    archive = tmp_path/'official.zip'
    text = io.StringIO(newline='')
    writer = csv.writer(text)
    writer.writerow(['交易日期','契約','到期月份(週別)','成交量','開盤價','最高價','最低價','收盤價','交易時段'])
    for label, volume, price in zip(labels, [15,22,33], [100,90,80]):
        writer.writerow(['2020/01/02','MTX',label,volume,price,price,price,price,'一般'])
    for volume, price in [(2,10),(3,20)]:
        writer.writerow(['2020/01/02','MTX',spread[0],volume,price,price,price,price,'一般'])
    if extra_spread:
        writer.writerow(['2020/01/02','MTX','202002/202004',1,30,30,30,30,'一般'])
    with zipfile.ZipFile(archive, 'w') as out:
        out.writestr('2020_fut.csv', text.getvalue().encode('cp950'))
    manifest = tmp_path/'official.json'
    atomic_write_json(manifest, dict(receipts=[dict(path=str(archive),sha256=sha256_file(archive))]))

    raw = tmp_path/'mirror'/'raw'
    raw.mkdir(parents=True)
    mirror = raw/'Daily_2020_01_02.zip'
    trades = list(zip(labels+spread, [100,90,80,10,20], [20,40,60,8,12]))
    text = io.StringIO(newline='')
    writer = csv.writer(text)
    for i,(label, price, volume) in enumerate(trades):
        writer.writerow(['20200102','MTX',label,f'0845{i:02}',price,volume,'-','-'])
    with zipfile.ZipFile(mirror, 'w') as out:
        out.writestr('Daily_2020_01_02.rpt', text.getvalue().encode('cp950'))
    inventory = tmp_path/'mirror'/'inventory.json'
    atomic_write_json(inventory,dict(contract='community_rpt_dated_prints_inventory_v1',
        source='community_rpt_mirror_unverified_origin_v1',historical_point_in_time_verified=False,
        catalog_sha256='a'*64,objects=[dict(date=str(day),path='raw/'+mirror.name,drive_id='fixture',
            sha256=sha256_file(mirror),bytes=mirror.stat().st_size)]))

    provider = tmp_path/'provider'
    provider.mkdir()
    object_path = provider/'object.parquet'
    pl.DataFrame([dict(date=f'2020-01-02 08:45:{i:02}',futures_id='MTX',contract_date=label,
        price=float(provider_price if i == 4 else price),volume=volume)
        for i,(label,price,volume) in enumerate(trades)]).write_parquet(object_path)
    receipt = tmp_path/'receipt.json'
    atomic_write_json(receipt,dict(dataset='TaiwanFuturesTick',partition=str(day),status='complete',
        historical_point_in_time=False,source_first_date=str(day),source_last_date=str(day),
        parquet_path='object.parquet',sha256=sha256_file(object_path)))
    policy = tmp_path/'policy.json'
    atomic_write_json(policy,dict(contract='taifex_duplicate_spread_label_research_v1',
        historical_point_in_time_verified=False,exchange_correction_verified=False,date=str(day),
        product='MTX',asset_class='index_future',official_duplicate_label=spread[0],
        native_spread_labels=spread,official_archive=dict(path=str(archive),sha256=sha256_file(archive)),
        community_inventory=dict(path=str(inventory),sha256=sha256_file(inventory)),
        finmind_receipt=dict(path=str(receipt),sha256=sha256_file(receipt)),finmind_root=str(provider)))
    keys = pl.DataFrame(dict(date=[day]*3,physical_contract=[f'MTX:{label}' for label in labels]))
    return manifest, policy, keys, archive


def test_reconciles_duplicate_legs_from_pinned_native_tapes_and_refuses_silent_reuse(tmp_path):
    manifest, policy, keys, _ = fixture(tmp_path)
    before = official_day_evidence(manifest,keys,tmp_path/'original')
    after = official_day_evidence(manifest,keys,tmp_path/'research',
        reuse_verified_cache=tmp_path/'original',spread_research_policy=policy)
    original = {r['physical_contract']:r for r in before.to_dicts()}
    revised = {r['physical_contract']:r for r in after.to_dicts()}
    assert [original[f'MTX:{m}']['outright_volume'] for m in ['202002W1','202002','202003W1']] == [10,17,33]
    assert [revised[f'MTX:{m}']['outright_volume'] for m in ['202002W1','202002','202003W1']] == [10,20,30]
    assert [revised[f'MTX:{m}']['spread_leg_volume'] for m in ['202002W1','202002','202003W1']] == [5,2,3]
    assert before.filter(pl.col('physical_contract') == 'MTX:202002W1').equals(
        after.filter(pl.col('physical_contract') == 'MTX:202002W1'))
    proof = json.loads((tmp_path/'research/official_evidence_manifest.json').read_text())
    assert proof['spread_research_reconciliation']['historical_point_in_time_verified'] is False
    assert proof['spread_research_reconciliation']['tick_rows_compared'] == 5
    with pytest.raises(ValueError,match='different source/volume contract'):
        official_day_evidence(manifest,keys,tmp_path/'silent-reuse',reuse_verified_cache=tmp_path/'research')
    with pytest.raises(ValueError,match='requires the verified original'):
        official_day_evidence(manifest,keys,tmp_path/'no-cache',spread_research_policy=policy)


@pytest.mark.parametrize('kwargs, message', [
    ({'provider_price':21}, 'trade multisets differ'),
    ({'extra_spread':True}, 'larger or different reconciliation scope'),
])
def test_unreconciled_tapes_or_extra_official_spread_stay_quarantined(tmp_path, kwargs, message):
    manifest, policy, keys, _ = fixture(tmp_path, **kwargs)
    official_day_evidence(manifest,keys,tmp_path/'original')
    with pytest.raises(ValueError,match=message):
        official_day_evidence(manifest,keys,tmp_path/'research',
            reuse_verified_cache=tmp_path/'original',spread_research_policy=policy)
    assert not (tmp_path/'research'/'official_evidence.parquet').exists()


def test_rejects_mutated_raw_or_another_official_source(tmp_path):
    manifest, policy, keys, archive = fixture(tmp_path)
    official_day_evidence(manifest,keys,tmp_path/'original')
    policy_document = json.loads(policy.read_text())
    policy_document['official_archive']['sha256'] = '0'*64
    atomic_write_json(policy,policy_document)
    with pytest.raises(ValueError,match='input SHA mismatch'):
        official_day_evidence(manifest,keys,tmp_path/'research',
            reuse_verified_cache=tmp_path/'original',spread_research_policy=policy)
    assert not (tmp_path/'research'/'official_evidence.parquet').exists()
    assert archive.exists()
