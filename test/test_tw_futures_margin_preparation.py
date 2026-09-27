from datetime import date
import json
import sqlite3
from types import SimpleNamespace

import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from stockagent.data.tw_futures_margin_preparation import (
    align_rules, check_publication, event_at, margin_effective, margin_pdf_values,
    position_change, prepare_daily, validate_event_chain, verified_margin_values,
    reviewed_legacy_events,
)


PDF = '''單位：新臺幣元
TX
調整後保證金金額 調整前保證金金額
原始保證金金額 維持保證金金額 結算保證金金額
原始保證金金額 維持保證金金額 結算保證金金額
保證金 184,000 141,000 136,000 167,000 128,000 123,000
'''


def test_margin_requires_product_local_currency_and_order():
    assert margin_pdf_values(PDF, 'TX') == (184000, 141000, 136000, 167000, 128000, 123000)
    for text in [PDF.replace('新臺幣', '美元'), PDF.replace('TX\n', 'MTX\n'),
                 PDF.replace('調整後保證金金額 調整前保證金金額', '調整前保證金金額 調整後保證金金額')]:
        with pytest.raises(ValueError):
            margin_pdf_values(text, 'TX')


def test_csv_pdf_crosscheck_does_not_trust_reused_url():
    values = (184000, 141000, 136000, 167000, 128000, 123000)
    headers = [phase + kind + '保證金' for phase in ['調整後', '調整前'] for kind in ['原始','維持','結算']]
    row = dict(zip(headers, map(str, values)), 契約代碼='TX', 契約中文簡稱='臺股期貨')
    assert verified_margin_values([row], PDF, 'TX') == values
    row['調整後原始保證金'] = '701000'
    with pytest.raises(ValueError, match='disagreement'):
        verified_margin_values([row], PDF, 'TX')
    with pytest.raises(ValueError, match='incomplete'):
        verified_margin_values([row, row], PDF, 'TX')


def test_holiday_effective_clause_and_attachment_date():
    assert margin_effective('自112年1月17日一般交易時段結束後起，預計於春節後恢復。') == '2023-01-17T13:45:00+08:00'
    check_publication('發文日期：中華民國112年1月16日', '2023-01-16')
    with pytest.raises(ValueError):
        check_publication('發文日期：中華民國115年1月16日', '2023-01-16')
    assert margin_effective('自100年9月8日交易時段結束後起實施') == '2011-09-08T13:45:00+08:00'
    with pytest.raises(ValueError, match='regular close'):
        margin_effective('自107年9月8日交易時段結束後起實施')


def test_historical_limit_boundary_and_expiry_phase():
    frame = pl.DataFrame(dict(date=[date(2015,5,29),date(2015,6,1)],
        physical_contract=['TX:201506']*2, product=['TX']*2, asset_class=['index_future']*2,
        previous_settlement=[9000.]*2, liquidation_reason=['carry_same_contract','last_trade_date']))
    event = dict(after=[83000,64000,61000], before=None, known_at='2014-01-01T23:59:59+08:00',
        effective_at='2014-01-02T13:45:00+08:00', source_url='verified')
    rules=align_rules(frame,dict(TX=[event]),[dict(event,after=6000)])
    assert rules['upper_limit'].to_list() == [9630.,9900.]
    assert rules['lower_limit'].to_list() == [8370.,8100.]
    assert rules['settlement_time'].to_list() == ['13:45:00','13:30:00']


def legacy_position_archive(after=6000, digest='a'*64):
    conn=sqlite3.connect(':memory:'); conn.row_factory=sqlite3.Row
    conn.execute('CREATE TABLE announcements(url TEXT,published_date TEXT)')
    notices=[]
    for i,pub,eff,amount,immediate in [(0,'2010-10-13','2010-11-18',5000,False),
                                     (1,'2011-04-08','2011-04-07',after,True)]:
        url=f'https://www.taifex.com.tw/{i}.pdf'
        conn.execute('INSERT INTO announcements VALUES (?,?)',(url,pub))
        notices.append(dict(url=url,announcement_url=url,published_date=pub,issued_date=pub if not immediate else eff,
            effective_date=eff,kind='position',after=amount,immediate=immediate,
            content_sha256=digest,pages_one_based=[1]))
    return SimpleNamespace(conn=conn,legacy=dict(verified_start='2011-01-03',notices=notices),
        children=lambda u:[],document=lambda u:dict(content_sha256='a'*64))


def test_late_known_relaxation_retains_stricter_limit_but_tightening_fails():
    _,events,_=reviewed_legacy_events(legacy_position_archive(),date(2011,1,3))
    assert event_at(events,'2011-04-08T08:45:00+08:00')['after'] == 5000
    assert event_at(events,'2011-04-11T08:45:00+08:00')['after'] == 6000
    with pytest.raises(ValueError,match='tightening'):
        reviewed_legacy_events(legacy_position_archive(after=4000),date(2011,1,3))
    with pytest.raises(ValueError,match='different source bytes'):
        reviewed_legacy_events(legacy_position_archive(digest='b'*64),date(2011,1,3))
    with pytest.raises(ValueError,match='predates'):
        reviewed_legacy_events(legacy_position_archive(),date(2010,1,3))


@pytest.mark.parametrize('clause,expected', [
    ('自然人與法人部位限制數分別由12,000口與24,000口調降至10,000口與20,000口，「臺指選擇權」自然人與法人部位限制數分別由35,000口與80,000口調降至30,000口與70,000口，自111年3月17日一般交易時段起生效。', (12000,10000,'2022-03-17')),
    ('自然人部位限制數由10,000個契約調降至9,000個契約、法人部位限制數由20,000個契約調降至18,000個契約，自114年12月18日一般交易時段起生效。「臺指選擇權」法人部位限制數由60,000個契約調升至70,000個契約，自114年10月16日一般交易時段起生效。', (10000,9000,'2025-12-18')),
    ('法人部位限制數由22,000口調降至20,000口，自114年9月18日一般交易時段起生效。', None),
])
def test_position_rule_selects_natural_person_and_tx_clock(clause, expected):
    result = position_change('二、「臺股期貨」' + clause + '三、其他股票期貨')
    assert (result['before'], result['after'], result['effective_at'][:10]) == expected if expected else result is None


def test_event_chain_and_two_session_phases_are_causal():
    first = dict(before=[167000,128000], after=[184000,141000], known_at='2021-05-18T23:59:59+08:00', effective_at='2021-05-19T13:45:00+08:00')
    second = dict(before=first['after'], after=[203000,156000], known_at='2022-01-25T23:59:59+08:00', effective_at='2022-01-26T13:45:00+08:00')
    events = validate_event_chain([second, first], ('before','after'))
    assert event_at(events, '2022-01-26T08:45:00+08:00') == first
    assert event_at(events, '2022-01-26T13:45:00+08:00') == second
    with pytest.raises(ValueError, match='transition'):
        validate_event_chain([first, dict(second, before=[1,1])], ('before','after'))
    with pytest.raises(ValueError, match='public'):
        validate_event_chain([dict(first, known_at='2021-05-20T23:59:59+08:00')], ('before','after'))


def daily_fixture(tmp_path):
    source = tmp_path/'source'; source.mkdir()
    proof = tmp_path/'proof'; proof.mkdir()
    final = tmp_path/'final'; final.mkdir()
    raw = source/'raw.csv'; raw.write_text('original raw evidence')
    rows=[]
    for physical, days, settlement in [('TX:202106',[18,19,20],100.),('TX:202107',[20,21],200.)]:
        for day in days:
            rows.append(dict(date=date(2021,5,day), product='TX', contract=physical.split(':')[1],
                physical_contract=physical, symbol='TAIFEX_SLOT_'+('0001' if days[0]==18 else '0002'),
                settlement=settlement, volume=10., open_interest=20., source_row_observed=True,
                open=settlement, close=settlement, can_hold_overnight=day != days[-1],
                liquidation_reason='carry_same_contract', executable=True,
                must_liquidate=False, resolved_last_trade_date=date(2021,6,16),
                previous_market_date=date(2021,5,day-1)))
    data=source/'continuous_daily.parquet'; pl.DataFrame(rows).write_parquet(data)
    (source/'manifest.json').write_text(json.dumps({'outputs':{'continuous_daily':{'sha256':sha256_file(data)}}}))
    p=proof/'official_evidence.parquet'
    pl.DataFrame([dict(date=r['date'],physical_contract=r['physical_contract'],
                      official_settlement=str(r['settlement']),official_source_sha256=sha256_file(raw)) for r in rows]).write_parquet(p)
    (proof/'official_evidence_manifest.json').write_text(json.dumps({'source':'taifex_complete_daily_and_spread_legs_v1',
        'sha256':sha256_file(p),'sources':[{'path':str(raw),'sha256':sha256_file(raw)}]}))
    f=final/'final.parquet'
    pl.DataFrame(schema={'settlement_date':pl.Date,'product':pl.String,'contract':pl.String,'final_settlement_price':pl.Float64}).write_parquet(f)
    (final/'manifest.json').write_text(json.dumps({'outputs':{'futures_final_settlement_history':{'sha256':sha256_file(f)}}}))
    return data,p,f,raw


def test_daily_scope_uses_physical_prior_and_excludes_only_first_observation(tmp_path):
    daily, proof, final, _ = daily_fixture(tmp_path)
    # Inject the exact source error: a recycled slot's previous price is another asset.
    f=pl.read_parquet(daily).with_columns(pl.lit(9000.).alias('previous_settlement'))
    f.write_parquet(daily)
    daily.with_name('manifest.json').write_text(json.dumps({'outputs':{'continuous_daily':{'sha256':sha256_file(daily)}}}))
    result, summary = prepare_daily(daily,proof,final,tmp_path/'out',date(2021,5,20),date(2021,5,21))
    new = result.filter(pl.col('physical_contract')=='TX:202107')
    assert new['date'].to_list() == [date(2021,5,21)]
    assert new['previous_settlement'].to_list() == [200.]
    assert summary['excluded_first_observations'] == 1


def test_daily_rejects_tampered_raw_source(tmp_path):
    daily, proof, final, raw = daily_fixture(tmp_path)
    raw.write_text('tampered')
    with pytest.raises(ValueError, match='raw archive SHA'):
        prepare_daily(daily,proof,final,tmp_path/'out',date(2021,5,20),date(2021,5,21))


@pytest.mark.parametrize('expiry', [False, True])
def test_snapshot_end_marks_open_account_without_fake_liquidation(tmp_path, expiry):
    daily, proof, final, _ = daily_fixture(tmp_path)
    f = pl.read_parquet(daily).with_columns(
        pl.when(pl.col('date') == date(2021,5,21)).then(pl.lit('dataset_terminal'))
        .otherwise(pl.col('liquidation_reason')).alias('liquidation_reason'),
        (pl.col('date') == date(2021,5,21)).alias('must_liquidate'),
        pl.lit(date(2021,5,21) if expiry else date(2021,6,16)).alias('resolved_last_trade_date'))
    if expiry:
        pl.DataFrame({'settlement_date':[date(2021,5,21)],'product':['TX'],'contract':['202107'],
                      'final_settlement_price':[205.]}).write_parquet(final)
        final.with_name('manifest.json').write_text(json.dumps({'outputs':{
            'futures_final_settlement_history':{'sha256':sha256_file(final)}}}))
    f.write_parquet(daily)
    daily.with_name('manifest.json').write_text(json.dumps({'outputs':{'continuous_daily':{'sha256':sha256_file(daily)}}}))
    result, summary = prepare_daily(daily,proof,final,tmp_path/'out',date(2021,5,20),date(2021,5,21))
    end = result.filter(pl.col('date') == date(2021,5,21))
    assert end['liquidation_reason'].to_list() == ['last_trade_date' if expiry else 'dataset_boundary_mark_only']
    assert end['must_liquidate'].any() == expiry
    assert end['settlement'].to_list() == [205. if expiry else 200.]
    assert summary['snapshot_boundary_mark_only_rows'] == (0 if expiry else 1)
