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
    reviewed_legacy_events, margin_legacy_word_levels,
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


def test_named_margin_grid_binds_own_source_and_keeps_reversed_columns():
    from stockagent.data.tw_futures_margin_preparation import margin_grid_candidates
    cells = [['契約名稱', '調整前保證金金額', None, None, '調整後保證金金額', None, None],
             [None, '結算', '維持', '原始', '結算', '維持', '原始'],
             ['臺股期貨', '68,000', '71,000', '92,000', '61,000', '64,000', '83,000'],
             ['小型臺指期貨', '17,000', '17,750', '23,000', '15,250', '16,000', '20,750'],
             ['電子期貨', '55,000', '57,000', '75,000', '50,000', '52,000', '68,000']]
    source = ('105年2月15日公告回調臺股期貨契約(TX)、小型臺指期貨契約(MTX)、'
              '電子期貨契約(TE)。')
    facts = margin_grid_candidates(cells, '單位：新臺幣元', source_text=source)
    assert [(f['product'], f['after'], f['before']) for f in facts] == [
        ('TX', [83000, 64000, 61000], [92000, 71000, 68000]),
        ('MTX', [20750, 16000, 15250], [23000, 17750, 17000]),
        ('TE', [68000, 52000, 50000], [75000, 57000, 55000])]
    assert all(f['product_code_policy'] == 'same_source_explicit_contract_name_code' for f in facts)
    assert not margin_grid_candidates(cells, '單位：新臺幣元')
    assert [f['product'] for f in margin_grid_candidates(cells, '單位：新臺幣元',
        source_text=source.replace('電子期貨契約(TE)', '微型電子期貨契約(MEF)'))] == ['TX', 'MTX']
    assert [f['product'] for f in margin_grid_candidates(cells, '單位：新臺幣元',
        source_text=source + '臺股期貨契約(ZZF)')] == ['MTX', 'TE']


def test_mixed_margin_caption_owns_each_gold_currency():
    from stockagent.data.tw_futures_margin_preparation import margin_grid_candidates
    cells = [['契約名稱', '調整前', None, None, '調整後', None, None],
             [None, '結算', '維持', '原始', '結算', '維持', '原始'],
             ['黃金期貨', '6,100', '6,320', '8,240', '5,500', '5,700', '7,430'],
             ['臺幣黃金期貨', '24,000', '25,000', '33,000', '21,000', '22,000', '29,000'],
             ['臺幣黃金選擇權', '13,000', '14,000', '18,000', '11,000', '12,000', '15,000']]
    source = '黃金期貨契約(GDF)、臺幣黃金期貨契約(TGF)、臺幣黃金選擇權契約(TGO)'
    facts = margin_grid_candidates(cells, '單位：黃金期貨為美元計價，其餘商品為新臺幣元', source_text=source)
    assert [(f['product'], f['margin_kind'], f['after']) for f in facts] == [
        ('GDF', 'fixed_usd', [7430, 5700, 5500]), ('TGF', 'fixed_twd', [29000, 22000, 21000])]
    assert not margin_grid_candidates(cells, '單位：美元、新臺幣元', source_text=source)
    cells[0][1] = '調整前保證金金額\n結算 維持 原始\n保證金 保證金 保證金'
    cells[1][1] = None
    assert margin_grid_candidates(cells, '單位：黃金期貨為美元計價，其餘商品為新臺幣元',
                                  source_text=source) == facts
    cells[1][2] = None
    assert not margin_grid_candidates(cells, '單位：黃金期貨為美元計價，其餘商品為新臺幣元',
                                      source_text=source)


def test_margin_vertical_header_labels_are_not_the_numeric_row():
    from stockagent.data.tw_futures_margin_preparation import margin_table_candidates
    text = ('單位：比例(%)\nCXF\n(大同期貨)\n調整後保證金適用比例\n(級距2)\n'
            '調整前保證金適用比例\n(級距1)\n' +
            '\n'.join(['原始\n保證金', '維持\n保證金', '結算\n保證金'] * 2) +
            '\n保證金 16.20% 12.42% 12.00% 13.50% 10.35% 10.00%\n')
    rows = margin_table_candidates(text)
    assert len(rows) == 1 and rows[0]['product'] == 'CXF'
    assert rows[0]['after'] == [0.162, 0.1242, 0.12]
    assert rows[0]['before'] == [0.135, 0.1035, 0.1]
    assert margin_table_candidates(text.replace('13.50%', '13.50')) == []
    assert margin_table_candidates(text.replace('CXF', 'CXO')) == []
    assert margin_table_candidates(text.replace('維持\n保證金', '原始\n保證金', 1)) == []


def test_native_non_disposed_margin_column_is_not_transition_before():
    from stockagent.data.tw_futures_margin_preparation import margin_table_candidates,margin_grid_candidates
    text=('單位：比例(%)\nDIF\n(旺宏期貨)\n'
          '調高2倍後保證金適用比例 標的證券未經處置前保證金適用比例\n'
          '原始保證金 維持保證金 結算保證金 原始保證金 維持保證金 結算保證金\n'
          '保證金 32.40% 24.84% 24.00% 16.20% 12.42% 12.00%\n')
    cells=[['DIF','調高2倍後保證金適用比例',None,None,
            '標的證券未經處置前保證金適用比例',None,None],
           [None,'原始','維持','結算','原始','維持','結算'],
           ['保證金','32.40%','24.84%','24.00%','16.20%','12.42%','12.00%']]
    for rows in (margin_table_candidates(text),margin_grid_candidates(cells,'單位：比例(%)')):
        assert len(rows)==1
        assert rows[0]['product']=='DIF' and rows[0]['after']==[.324,.2484,.24]
        assert rows[0]['before'] is None
        assert rows[0]['declared_non_disposed_margin']==[.162,.1242,.12]
    assert not margin_table_candidates(text.replace('24.84%','24.84'))
    assert not margin_grid_candidates(cells[:1]+cells[2:],'單位：比例(%)')


def test_numbered_native_extension_owns_prior_expiry_and_preserves_old_clocks():
    from copy import deepcopy
    from scripts.build_tw_futures_margin_event_candidates import (
        candidate_notice_clock,product_margin_restoration_clock,bind_native_margin_period_extension)
    text=('發文日期：中華民國115年3月5日。發文字號：台期結字第11503004431號。'
        '燿華期貨契約(VBF)，並自115年3月6日一般交易時段結束後起實施。'
        '本次延長保證金調整期間之商品為瑞軒期貨契約(HAF)，配合證券市場處置措施，'
        '本公司115年3月2日台期結字第11503003891號函，公告調整瑞軒期貨之保證金'
        '為標的證券未經處置前所屬級距2倍之期間，延長至115年3月18日一般交易時段結束後，'
        '恢復為115年3月3日調整前之保證金。')
    clock=candidate_notice_clock(text,'2026-03-05')
    scoped=product_margin_restoration_clock(text,'HAF',clock)
    assert scoped['effective_date'] is None
    assert scoped['extension_prior_notice_join_required']
    assert product_margin_restoration_clock(text,'VBF',clock)['effective_date']=='2026-03-06'
    prior=dict(product='HAF',published_date='2026-03-02',effective_date='2026-03-03',
        effective_phase='after_product_regular_close',known_at='2026-03-02T23:59:59+08:00',
        margin_kind='notional_rate',issue_date_bound=True,source_url='prior',
        source_content_sha256='a'*64,after=[.324,.2484,.24],before=[.162,.1242,.12],
        temporary_end_evidence=json.dumps([dict(date_iso='2026-03-13',boundary='after_regular_session')]))
    amendment=dict(scoped,product='HAF',published_date='2026-03-05',
        known_at='2026-03-05T23:59:59+08:00',source_content_sha256='b'*64,
        margin_kind='notional_rate',after=[.324,.2484,.24],
        declared_non_disposed_margin=[.162,.1242,.12],before_column_semantics='explicit_non_disposed_base')
    original=deepcopy(prior)
    archive=SimpleNamespace(document=lambda _:dict(content_sha256='a'*64,text=
        '發文日期：中華民國115年3月2日。發文字號：台期結字第11503003891號。'))
    bound=bind_native_margin_period_extension(archive,text,amendment,[prior])
    assert prior==original
    assert bound['effective_date']=='2026-03-13' and bound['known_at']==amendment['known_at']
    assert bound['before']==prior['after']
    assert json.loads(bound['temporary_end_evidence'])[0]['date_iso']=='2026-03-18'
    assert json.loads(bound['extension_join_evidence'])['prior_known_at']==prior['known_at']
    assert bind_native_margin_period_extension(archive,text,amendment,[]) is None
    assert bind_native_margin_period_extension(archive,text,amendment,[dict(prior,after=[.243,.1863,.18])]) is None
    assert bind_native_margin_period_extension(archive,text,amendment,[dict(prior,product='VBF')]) is None
    assert bind_native_margin_period_extension(archive,text.replace('11503003891','11503009999'),amendment,[prior]) is None


def _same_level_period_replacement():
    from scripts.build_tw_futures_margin_event_candidates import candidate_notice_clock,product_margin_restoration_clock
    text=('發文日期：中華民國115年6月2日。發文字號：台期結字第11503011611號。'
        '主旨：公告調整力積電期貨契約(QZF)及延長合晶期貨契約(PLF)保證金調整期間。'
        '本次調整自115年6月3日一般交易時段結束後起實施。'
        '115年6月15日一般交易時段結束後，合晶期貨恢復為115年5月29日調整前之保證金。'
        '本公司115年5月28日台期結字第11503011071號函有關合晶期貨保證金之規定，'
        '自115年6月3日一般交易時段結束後停止適用。PLF(合晶期貨)')
    prior=dict(product='PLF',published_date='2026-05-28',effective_date='2026-05-29',
        effective_phase='after_product_regular_close',known_at='2026-05-28T23:59:59+08:00',
        margin_kind='notional_rate',issue_date_bound=True,source_url='prior',
        source_content_sha256='a'*64,after=[.324,.2484,.24],before=[.162,.1242,.12],
        requires_reversion_review=True,
        temporary_end_evidence=json.dumps([dict(date_iso='2026-06-10',boundary='after_regular_session')]))
    clock=product_margin_restoration_clock(text,'PLF',candidate_notice_clock(text,'2026-06-02'))
    row=dict(clock,product='PLF',published_date='2026-06-02',
        known_at='2026-06-02T23:59:59+08:00',source_url='own',source_content_sha256='b'*64,
        margin_kind='notional_rate',after=[.324,.2484,.24],before=[.162,.1242,.12])
    docs={'prior':dict(content_sha256='a'*64,text=
        '發文日期：中華民國115年5月28日。發文字號：台期結字第11503011071號。'),
        'own':dict(content_sha256='b'*64,text=text)}
    return SimpleNamespace(document=docs.__getitem__),text,prior,row


def test_same_level_period_replacement_keeps_live_amount_and_named_restoration_base():
    from copy import deepcopy
    from scripts.build_tw_futures_margin_event_candidates import (
        bind_native_margin_period_extension,compose_bound_margin_extension_views,margin_candidate_intervals)
    archive,text,prior,row=_same_level_period_replacement()
    original=deepcopy([prior,row]);bound=bind_native_margin_period_extension(archive,text,row,[prior])
    assert [prior,row]==original
    assert bound['before']==prior['after'] and bound['after']==prior['after']
    assert bound['restoration_target']==prior['before']
    assert bound['effective_date']=='2026-06-03' and bound['known_at']==row['known_at']
    assert json.loads(bound['extension_join_evidence'])['printed_restoration_base']==row['before']
    peer=dict(row,extraction='another_same_original_table')
    assert len(compose_bound_margin_extension_views(archive,[prior,bound,peer]))==1
    assert peer['before']==prior['after'] and peer['restoration_target']==prior['before']
    intervals,issues=margin_candidate_intervals([prior,bound,peer])
    assert not issues
    assert [(r['effective_date'],r['valid_until_date_exclusive'],r['initial']) for r in intervals]==[
        ('2026-05-29','2026-06-03',.324),('2026-06-03','2026-06-15',.324)]
    wrong=dict(row,source_content_sha256='c'*64)
    assert not compose_bound_margin_extension_views(archive,[bound,wrong])
    assert wrong['before']==row['before']


@pytest.mark.parametrize('change',[
    {'product':'QZF'},{'before':[.135,.1035,.1]},{'after':[.243,.1863,.18]},
    {'source_content_sha256':'c'*64},{'known_at':'2026-06-01T23:59:59+08:00'},
    {'effective_date':'2026-06-04'},{'issue_date_bound':False},
    {'prior_notice_revocation_evidence':'[]'},
])
def test_period_replacement_rejects_unbound_product_amount_clock_or_revocation(change):
    from scripts.build_tw_futures_margin_event_candidates import bind_native_margin_period_extension
    archive,text,prior,row=_same_level_period_replacement()
    assert bind_native_margin_period_extension(archive,text,dict(row,**change),[prior]) is None
    if 'prior_notice_revocation_evidence' not in change:
        assert bind_native_margin_period_extension(archive,text,row,[dict(prior,**change)]) is None


def test_prior_notice_revocation_does_not_end_replacement_at_its_start():
    from scripts.build_tw_futures_margin_event_candidates import candidate_notice_clock
    text=('發文日期：中華民國115年3月24日。發文字號：台期結字第11503005871號。'
        '公告115年3月25日一般交易時段結束後，調高旺宏期貨保證金，並於115年4月8日'
        '一般交易時段結束後，恢復為標的證券未經處置前之保證金。'
        '原115年3月19日台期結字第11503005501號函公告旺宏期貨保證金調整之規定，'
        '自115年3月25日一般交易時段結束後停止適用。')
    clock=candidate_notice_clock(text,'2026-03-24')
    assert clock['effective_date']=='2026-03-25'
    assert {e['date_iso'] for e in json.loads(clock['temporary_end_evidence'])}=={'2026-04-08'}
    revoked=json.loads(clock['prior_notice_revocation_evidence'])
    assert len(revoked)==1 and revoked[0]['revoked_notice_number']=='11503005501'


@pytest.mark.parametrize('change', ['', 'ambiguous_period', 'no_period_close_rule'])
def test_nominal_restoration_uses_only_own_explicit_disposition_period(change):
    from scripts.build_tw_futures_margin_event_candidates import candidate_notice_clock
    text = ('臺灣期貨交易所 新聞稿\n中華民國105年11月25日\n'
        '玉晶光期貨契約(LEF)，處置期間為105年11月25日至12月8日。'
        '自105年11月28日該股票期貨契約交易時段結束後起實施，'
        '並於證券市場處置期間結束後，於該契約交易時段結束後恢復為調整前之保證金。')
    if change == 'ambiguous_period':text += '另一處置期間為105年11月25日至12月9日。'
    if change == 'no_period_close_rule':text = text.replace('證券市場處置期間結束後', '另行公告後')
    clock = candidate_notice_clock(text, '2016-11-25')
    assert clock['effective_date'] == '2016-11-28' and clock['requires_reversion_review']
    ends = json.loads(clock['temporary_end_evidence'])
    if change:
        assert ends == []
    else:
        assert len(ends) == 1 and ends[0]['date_iso'] == '2016-12-08'
        assert ends[0]['derivation'] == 'own_disposition_period_and_explicit_period_close_restoration'
        assert ends[0]['boundary'] == 'after_regular_session'


@pytest.mark.parametrize('ambiguous', ['', 'mixed_year', 'conflicting_close', 'unbound_issue'])
def test_native_margin_start_short_date_requires_own_unique_publication_year(ambiguous):
    from scripts.build_tw_futures_margin_event_candidates import candidate_notice_clock
    text = ('臺灣期貨交易所新聞稿部門：結算部中華民國115年8月27日。'
        '處置期間為115年8月27日至9月2日。期交所依規定自8月28日'
        '(證券市場處置生效日次一營業日)一般交易時段結束後起調高保證金，'
        '並於9月2日一般交易時段結束後恢復為調整前之保證金。')
    if ambiguous == 'mixed_year':text += '另參照114年1月1日公告。'
    if ambiguous == 'conflicting_close':text += '另自115年8月29日一般交易時段結束後起實施。'
    if ambiguous == 'unbound_issue':text = text.replace('中華民國115年8月27日', '未載發布日期')
    clock = candidate_notice_clock(text, '2026-08-27')
    if ambiguous:
        assert clock['effective_date'] != '2026-08-28'
    else:
        assert clock['effective_date'] == '2026-08-28'
        assert clock['effective_phase'] == 'after_product_regular_close'
        assert {e['date_iso'] for e in json.loads(clock['temporary_end_evidence'])} == {'2026-09-02'}


def test_margin_close_and_same_day_underlying_suspension_keep_distinct_clocks():
    from scripts.build_tw_futures_margin_event_candidates import candidate_notice_clock
    text = ('臺灣期貨交易所新聞稿中華民國105年5月16日。'
        '處置期間為105年5月16日至5月27日。'
        '期交所依規定自105年5月17日(證券市場處置生效日次一營業日)'
        '該股票期貨契約交易時段結束後起實施，並於證券市場處置期間結束後，'
        '於該契約交易時段結束後恢復為調整前之保證金。'
        '櫃買中心另公告該普通股自105年5月17日起停止買賣，恢復日將順延。')
    clock = candidate_notice_clock(text, '2016-05-16')
    assert clock['effective_date'] == '2016-05-17'
    assert clock['effective_phase'] == 'after_product_trading_session_unspecified'
    assert {e['date_iso'] for e in json.loads(clock['temporary_end_evidence'])} == {'2016-05-27'}
    assert clock['requires_reversion_review']


def test_comma_at_restoration_and_unpunctuated_prior_revocation_are_distinct():
    from scripts.build_tw_futures_margin_event_candidates import candidate_notice_clock
    text = ('發文日期：中華民國110年10月29日。發文字號：台期結字第11003015001號。'
        '本次調整自110年11月1日一般交易時段結束後起實施，'
        '並於110年11月11日一般交易時段結束後，恢復為10月27日調整前之保證金。'
        '原110年10月26日公告之台期結字第1100301469號函自110年11月1日'
        '旨揭契約交易時段結束後停止適用。')
    clock = candidate_notice_clock(text, '2021-10-29')
    assert clock['effective_date'] == '2021-11-01'
    assert {e['date_iso'] for e in json.loads(clock['temporary_end_evidence'])} == {'2021-11-11'}
    assert {e['revoked_notice_number'] for e in json.loads(clock['prior_notice_revocation_evidence'])} == {'1100301469'}


@pytest.mark.parametrize('view', ['formal', 'press'])
def test_period_only_numbered_extension_is_not_a_retroactive_new_raise(view):
    from copy import deepcopy
    from scripts.build_tw_futures_margin_event_candidates import (
        candidate_notice_clock, product_margin_restoration_clock, bind_native_margin_period_extension,
        compose_bound_margin_extension_views, margin_candidate_intervals)
    formal = ('發文日期：中華民國115年2月4日。'
        '本公司115年1月30日台期結字第11503002331號函，公告調整聯亞期貨之保證金為'
        '標的證券未經處置前所屬級距2倍之期間，延長至115年2月26日一般交易時段結束後，'
        '恢復為115年2月2日調整前之保證金。OTF(聯亞期貨)')
    press = ('臺灣期貨交易所 新聞稿\n中華民國115年2月4日\n'
        '原期交所115年1月30日台期結字第11503002331號函公告自115年2月2日至2月23日'
        '一般交易時段止，調整聯亞期貨保證金適用比例為標的證券未經處置前2倍之期間，'
        '延長至2月26日。115年2月26日一般交易時段結束後，聯亞期貨契約(OTF)'
        '恢復為115年2月2日調整前之保證金。OTF(聯亞期貨)')
    text = formal if view == 'formal' else press
    clock = product_margin_restoration_clock(text, 'OTF', candidate_notice_clock(text, '2026-02-04'))
    assert clock['extension_prior_notice_join_required'] and clock['effective_date'] is None
    prior = dict(product='OTF', published_date='2026-01-30', effective_date='2026-02-02',
        effective_phase='after_product_regular_close', known_at='2026-01-30T23:59:59+08:00',
        margin_kind='notional_rate', issue_date_bound=True, source_url='prior',
        source_content_sha256='a'*64, after=[.459,.3519,.34], before=[.2295,.176,.17],
        temporary_end_evidence=json.dumps([dict(date_iso='2026-02-23', boundary='after_regular_session')]))
    row = dict(clock, product='OTF', published_date='2026-02-04', known_at='2026-02-04T23:59:59+08:00',
        margin_kind='notional_rate', source_url='extension', source_content_sha256='b'*64, after=prior['after'],
        declared_non_disposed_margin=prior['before'], before_column_semantics='explicit_non_disposed_base')
    archive = SimpleNamespace(document=lambda url: dict(content_sha256='a'*64,
        text='發文日期：中華民國115年1月30日。發文字號：台期結字第11503002331號。')
        if url == 'prior' else dict(content_sha256='b'*64, text=text))
    bound = bind_native_margin_period_extension(archive, text, row, [prior])
    assert bound['effective_date'] == '2026-02-23'
    assert bound['before'] == bound['after'] == prior['after']
    assert bound['known_at'] == row['known_at'] and prior['effective_date'] == '2026-02-02'
    assert bind_native_margin_period_extension(archive, text, dict(row, after=[.432,.3312,.32]), [prior]) is None
    assert bind_native_margin_period_extension(archive, text.replace('11503002331', '11503009999'), row, [prior]) is None
    partial = dict(row, before=None, before_column_semantics=None, declared_non_disposed_margin=None,
        effective_date='2026-02-02', effective_phase='date_only_requires_phase_review')
    assert len(compose_bound_margin_extension_views(archive, [prior, bound, partial])) == 1
    assert partial['effective_date'] == '2026-02-23' and partial['known_at'] == row['known_at']
    assert partial['before'] == prior['after'] and partial['after'] == row['after']
    levels, _ = margin_candidate_intervals([prior, bound, partial])
    assert [(r['effective_date'], r['valid_until_date_exclusive']) for r in levels] == [
        ('2026-02-02', '2026-02-23'), ('2026-02-23', '2026-02-26')]
    for change in [dict(published_date='2026-02-05'), dict(after=[.432,.3312,.32]),
                   dict(before=[.216,.1656,.16]), dict(declared_non_disposed_margin=[.216,.1656,.16]),
                   dict(effective_date='2026-02-06'), dict(source_content_sha256='c'*64)]:
        wrong = dict(row, before=None, before_column_semantics=None, declared_non_disposed_margin=None)
        wrong.update(change)
        old = deepcopy(wrong)
        assert not compose_bound_margin_extension_views(archive, [prior, bound, wrong])
        assert wrong == old


@pytest.mark.parametrize('change', ['', 'missing_cash', 'zero_cash', 'wrong_prior_number',
                                  'wrong_base', 'wrong_annotation', 'future_law',
                                  'sibling_exact', 'sibling_before', 'sibling_clock', 'sibling_end'])
def test_shortened_period_requires_own_revision_and_complete_observed_cash_sessions(tmp_path, change):
    from copy import deepcopy
    from scripts.build_tw_futures_margin_event_candidates import (
        repair_native_restoration_period_revisions, margin_candidate_intervals)
    law = tmp_path/'law.html'
    law.write_text('發文日期：中華民國115年8月3日。並自115年8月10日起實施。'
                   '累積處置日數已達修正後規定之5(或7)個營業日者解除處置。')
    prior_text = '發文日期：中華民國115年8月3日。發文字號：台期結字第11503017001號。'
    amendment_text = ('發文日期：中華民國115年8月6日。公告調整保證金適用比例調整之期間。'
        '自修正條文施行日(115年8月10日)起實施。'
        '本公司115年8月3日台期結字第11503017001號函自即日起停止適用。'
        '調整期間如遇休市、有價證券停止買賣、全日暫停交易則恢復日順延執行。'
        '單位：比例(%)LYF(南電期貨)'
        '※115年8月11日一般交易時段結束後恢復為115年8月4日標的證券未經處置前保證金。')
    if change == 'wrong_prior_number':
        prior_text = prior_text.replace('11503017001', '11503009999')
    documents = {}
    for name, text in [('prior', prior_text), ('amendment', amendment_text)]:
        file = tmp_path/(name+'.txt');file.write_text(text)
        documents[name] = dict(text=text, content_sha256=sha256_file(file))
    base, raised = [.216,.1656,.16], [.324,.2484,.24]
    prior = dict(product='LYF', margin_kind='notional_rate', published_date='2026-08-03',
        effective_date='2026-08-04', effective_phase='after_product_regular_close',
        known_at='2026-08-03T23:59:59+08:00', issue_date_bound=True, before=base, after=raised,
        requires_reversion_review=True, source_url='prior', source_content_sha256=documents['prior']['content_sha256'],
        temporary_end_evidence=json.dumps([dict(date_iso='2026-08-18', boundary='after_regular_session')]))
    amendment = dict(prior, published_date='2026-08-06', effective_date=None, effective_phase='unresolved',
        known_at='2026-08-06T23:59:59+08:00', source_url='amendment',
        source_content_sha256=documents['amendment']['content_sha256'], before=None,
        before_column_semantics='explicit_non_disposed_base', declared_non_disposed_margin=base)
    old_restore = dict(prior, effective_date='2026-08-18', known_at='2026-08-18T13:35:00+08:00',
        before=raised, after=base, requires_reversion_review=False, temporary_end_evidence='[]',
        original_rule_known_at=prior['known_at'], extraction='observed_disposition_conditional_restoration')
    if change == 'wrong_base':
        amendment['declared_non_disposed_margin'] = [.243,.1863,.18]
    period = dict(date='2026-07-31', stock_id='8046', period_start='2026-08-03', period_end='2026-08-18',
        measure='期間自115年8月3日起至8月18日〔十二個營業日〕。'
                '註：配合115年8月10日處置新規定，期間調整為115/08/03~115/08/11。', source_sha256='d'*64)
    if change == 'wrong_annotation':
        period['measure'] = period['measure'].replace('08/11', '08/12')
    dates = ['2026-08-03','2026-08-04','2026-08-05','2026-08-06','2026-08-07','2026-08-10','2026-08-11']
    if change == 'missing_cash':
        dates.remove('2026-08-05')
    observations = [dict(date=d, symbol='8046', volume=0. if change == 'zero_cash' and d=='2026-08-05'
                         else 100., source_sha256='e'*64) for d in dates]
    inputs = {}
    for key, frame in [('universe', pl.DataFrame(dict(product=['LYF'], underlying_symbol=['8046']))),
                       ('dispositions', pl.from_dicts([period])), ('observations', pl.from_dicts(observations))]:
        file = tmp_path/(key+('.csv' if key=='universe' else '.parquet'))
        frame.write_csv(file) if key=='universe' else frame.write_parquet(file)
        inputs[key] = dict(path=str(file), sha256=sha256_file(file))
    law_spec = dict(path=str(law), sha256=sha256_file(law), url='law', published_date='2026-08-03',
        known_at='2026-08-03T23:59:59+08:00', effective_date='2026-08-10',
        required_clauses=['中華民國115年8月3日', '並自115年8月10日起實施', '5(或7)個營業日'])
    if change == 'future_law':
        law_spec.update(published_date='2026-08-07', known_at='2026-08-07T23:59:59+08:00')
    path = tmp_path/'context.json';path.write_text(json.dumps(dict(inputs, restoration_period_revision_laws=[law_spec])))
    bundle = tmp_path/'delta';bundle.mkdir()
    archive = SimpleNamespace(bundle=bundle, copy=lambda *a, **k: None, document=lambda url: documents[url])
    original = deepcopy([prior, amendment, old_restore])
    siblings = []
    if change.startswith('sibling_'):
        news_text = '臺灣期貨交易所 新聞稿\n中華民國115年8月3日\n南電期貨。'
        news_file = tmp_path/'press.txt';news_file.write_text(news_text)
        documents['press'] = dict(text=news_text, content_sha256=sha256_file(news_file))
        news = dict(prior, source_url='press', source_content_sha256=documents['press']['content_sha256'])
        if change == 'sibling_before':news['before'] = [.243,.1863,.18]
        if change == 'sibling_clock':news['known_at'] = '2026-08-03T22:00:00+08:00'
        if change == 'sibling_end':news['temporary_end_evidence'] = json.dumps([
            dict(date_iso='2026-08-19', boundary='after_regular_session')])
        siblings = [news, dict(old_restore, source_url='press',
            original_rule_known_at=news['known_at'], source_content_sha256=news['source_content_sha256'])]
        original.extend(deepcopy(siblings))
    untouched = deepcopy(original)
    if change == 'future_law':
        with pytest.raises(ValueError, match='law publication mismatch'):
            repair_native_restoration_period_revisions(archive, original, path)
        return
    result, repairs = repair_native_restoration_period_revisions(archive, original, path)
    assert original == untouched
    if change and (not change.startswith('sibling_') or change == 'sibling_before'):
        assert not repairs and result == original
    else:
        assert len(repairs) == 1
        assert result[0] == prior
        restored = result[-1]
        assert restored['effective_date'] == '2026-08-11'
        assert restored['known_at'] == '2026-08-11T13:35:00+08:00'
        assert restored['before'] == raised and restored['after'] == base
        assert json.loads(restored['restoration_evidence'])['completed_cash_dates'] == dates
        assert repairs[0]['revised_required_sessions'] == 7 and repairs[0]['financial_values_inferred'] is False
        if change == 'sibling_exact':
            assert len(result) == 3 and result[1] == siblings[0]
            assert all(r.get('effective_date') != '2026-08-18' for r in result)
            levels, _ = margin_candidate_intervals(result)
            assert any(r['effective_date'] == '2026-08-11' for r in levels)
        elif change.startswith('sibling_'):
            assert all(r in result for r in siblings)
        else:
            assert len(result) == 2


@pytest.mark.parametrize('retained_proof', [True, False])
def test_visual_generic_close_requires_retained_dated_single_session_proof(tmp_path, retained_proof):
    from scripts.build_tw_futures_margin_event_candidates import source_review_candidates
    conn=sqlite3.connect(':memory:');conn.row_factory=sqlite3.Row
    conn.executescript("CREATE TABLE announcements(url,published_date);"
                      "INSERT INTO announcements VALUES('source','2018-05-15');")
    image=tmp_path/'page.png';image.write_bytes(b'inspected page')
    universe=tmp_path/'products.csv'
    pl.DataFrame(dict(product=['LXF'],underlying_security_type=['stock'])).write_csv(universe)
    session=tmp_path/'session.json'
    session.write_text(json.dumps(dict(review_kind='source_bound_single_regular_session',
        effective_date='2010-01-25',valid_until_exclusive='2024-01-22',known_at='2010-01-08T23:59:59+08:00',
        sources=[dict(source_url='law',content_sha256='b'*64,required_clauses=['日盤時段'])],
        universe=dict(path=str(universe),sha256=sha256_file(universe)),products=['LXF'])))
    sources={'session':dict(path=session.name,sha256=sha256_file(session),kind='single_session_rule_review')}
    archive=SimpleNamespace(conn=conn,bundle=tmp_path,copy=lambda *a,**kw:None,
        sources=sources if retained_proof else {},
        document=lambda url:dict(content_sha256='b'*64,text='日盤時段') if url=='law'
        else dict(content_sha256='a'*64,text=''))
    review=dict(review_kind='source_bound_visual_transcription',source_url='source',content_sha256='a'*64,
        published_date='2018-05-15',known_at='2018-05-15T23:59:59+08:00',effective_date='2018-05-16',
        effective_phase='after_product_regular_close',margin_kind='notional_rate',
        transcribed_text='發文日期：中華民國107年5月15日。本次保證金調整自107年5月16日'
          '該股票期貨契約交易時段結束後起實施，並於5月28日該契約交易時段結束後'
          '恢復為5月16日調整前之保證金。',
        temporary_end_evidence=[dict(date_iso='2018-05-28',boundary='after_regular_session')],
        pages=[dict(page=1,path=image.name,sha256=sha256_file(image))],
        rows=[dict(product='LXF',page=1,after=dict(initial=.2025,maintenance=.1553,clearing=.15),
                   before=dict(initial=.135,maintenance=.1035,clearing=.1))])
    path=tmp_path/'review.json';path.write_text(json.dumps(review))
    if not retained_proof:
        with pytest.raises(ValueError,match='temporary clock differs'):
            source_review_candidates(archive,path)
    else:
        facts,_=source_review_candidates(archive,path)
        assert facts[0]['effective_phase']=='after_product_regular_close'
        assert json.loads(facts[0]['temporary_end_evidence'])[0]['boundary']=='after_regular_session'
        assert facts[0]['single_session_rule_review_sha256']==sha256_file(session)


@pytest.mark.parametrize('change', ['', 'wrong_before', 'wrong_publication', 'unbound_publication',
                                  'wrong_product', 'explicit_conflict', 'wrong_own_start', 'tampered_anchor',
                                  'shared_parent', 'unrelated_parent', 'unproved_parent'])
def test_same_notice_margin_clock_composition_keeps_event_identity(change):
    from scripts.build_tw_futures_margin_event_candidates import compose_same_notice_margin_clocks
    anchor=dict(product='PQF',margin_kind='notional_rate',before=[.162,.1242,.12],after=[.324,.2484,.24],
        published_date='2025-11-26',known_at='2025-11-26T23:59:59+08:00',issue_date_bound=True,
        effective_date='2025-11-27',effective_phase='after_product_regular_close',
        source_url='formal',source_content_sha256='a'*64,requires_reversion_review=True,
        temporary_end_evidence=json.dumps([dict(date_iso='2025-12-09',boundary='after_regular_session')]))
    incomplete=dict(anchor,source_url='press',source_content_sha256='b'*64,effective_date=None,
        effective_phase='unresolved',temporary_end_evidence='[]')
    if change=='wrong_before':incomplete['before']=[.216,.1656,.16]
    if change=='wrong_publication':incomplete.update(published_date='2025-11-25',known_at='2025-11-25T23:59:59+08:00')
    if change=='unbound_publication':incomplete['issue_date_bound']=False
    if change=='wrong_product':incomplete['product']='LYF'
    if change=='explicit_conflict':incomplete['effective_date']='2025-11-28'
    text='發文日期：中華民國114年11月26日。調整自114年11月27日一般交易時段結束後起實施，於114年12月9日一般交易時段結束後恢復為調整前保證金。'
    if change=='wrong_own_start':text=text.replace('11月27日','11月28日')
    documents={'formal':dict(text=text,content_sha256='c'*64 if change=='tampered_anchor' else 'a'*64),
               'press':dict(text=text,content_sha256='b'*64)}
    archive=SimpleNamespace(sources={},document=lambda url:documents[url]);original=dict(anchor)
    if change.endswith('parent'):
        import sqlite3
        anchor['announcement_url'] = 'notice'
        incomplete['announcement_url'] = 'other' if change == 'unrelated_parent' else 'notice'
        documents['press']['text'] = text.replace('於114年12月9日一般交易時段結束後恢復為',
            '並於證券市場處置期間結束後，於該契約一般交易時段結束後恢復為')
        documents['notice'] = dict(content_sha256='c'*64, text='官方同事件公告')
        archive.conn = sqlite3.connect(':memory:');archive.conn.row_factory = sqlite3.Row
        archive.conn.execute('CREATE TABLE announcements(url TEXT, published_date TEXT)')
        archive.conn.execute('INSERT INTO announcements VALUES (?,?)',('notice','2025-11-26'))
        archive.children = lambda _: ['formal'] if change == 'unproved_parent' else ['formal','press']
        original = dict(anchor)
    if change=='tampered_anchor':
        with pytest.raises(ValueError,match='anchor original SHA'):
            compose_same_notice_margin_clocks(archive,[anchor,incomplete])
        return
    repaired=compose_same_notice_margin_clocks(archive,[anchor,incomplete])
    assert anchor==original
    if change and change != 'shared_parent':
        assert not repaired and incomplete['effective_phase']=='unresolved'
    else:
        assert len(repaired)==1 and incomplete['known_at']==anchor['known_at']
        assert incomplete['effective_date']==anchor['effective_date']
        assert incomplete['effective_phase']==anchor['effective_phase']
        assert incomplete['before']==anchor['before'] and incomplete['after']==anchor['after']
        assert json.loads(incomplete['same_notice_clock_evidence'])['source_content_sha256s']==['a'*64,'b'*64]


def test_native_non_disposed_restoration_still_requires_own_dated_reference():
    from scripts.build_tw_futures_margin_event_candidates import referenced_before_restoration
    from stockagent.data.tw_futures_margin_preparation import compact
    text=('發文日期：中華民國115年3月24日。旺宏期貨契約(DIF)，'
          '115年4月8日一般交易時段結束後恢復為標的證券未經處置前之保證金。'
          'DIF(旺宏期貨)115年4月8日一般交易時段結束後恢復為'
          '115年3月20日標的證券未經處置前保證金。')
    fact=dict(product='DIF',effective_date='2026-03-25',margin_kind='notional_rate',before=None,
        declared_non_disposed_margin=[.162,.1242,.12],known_at='2026-03-24T23:59:59+08:00',
        issue_date_bound=True)
    prior=dict(product='DIF',effective_date='2026-03-20',margin_kind='notional_rate',
        before=[.162,.1242,.12],known_at='2026-03-19T23:59:59+08:00',issue_date_bound=True,
        source_content_sha256='a'*64)
    bound=referenced_before_restoration([compact(text)],fact,[prior,fact])
    assert bound['restoration_target']==[.162,.1242,.12]
    assert bound['restoration_rule']=='return_to_referenced_before'
    assert referenced_before_restoration([compact(text)],fact,[dict(prior,product='HAF'),fact]) is None
    assert referenced_before_restoration([compact(text)],dict(fact,declared_non_disposed_margin=[.216,.1656,.16]),[prior]) is None
    assert referenced_before_restoration([compact(text.split('。')[0]+'。'+text.split('。')[1])],fact,[prior]) is None


def test_listing_grid_metric_owns_the_percentage_before_fixed_clearing_label():
    from stockagent.data.tw_futures_margin_preparation import margin_grid_candidates
    cells=[['序號','標的證券','證券代碼','契約英文代碼','中文簡稱',
            '部位限制數',None,None,'標準型證券股數','保證金所屬級距',
            '結算保證金適用比例；結算保證金','維持保證金適用比例；結算保證金',
            '原始保證金適用比例；結算保證金'],
           [None,None,None,None,None,'自然人','法人','造市者',None,None,None,None,None],
           ['1','漢翔','2634','RC','漢翔期貨','2000','6000','15000','2000','級距1',
            '10.00%','10.35%','13.50%'],
           ['2','中天','4128','RD','中天期貨','2000','6000','15000','2000','級距2',
            '12.00%','12.42%','16.20%']]
    facts=margin_grid_candidates(cells,'112年2月10日新增7檔股票期貨契約')
    assert [(f['product'],f['after'],f['margin_kind']) for f in facts]==[
        ('RCF',[.135,.1035,.1],'notional_rate'),('RDF',[.162,.1242,.12],'notional_rate')]
    assert all(f['product_code_policy']=='dated_stock_futures_family_code_plus_F' for f in facts)
    cells[0][11]='原始保證金適用比例；結算保證金'
    assert not margin_grid_candidates(cells,'股票期貨')
    cells[0][11]='維持保證金適用比例；原始保證金'
    assert not margin_grid_candidates(cells,'股票期貨')


def test_holiday_effective_clause_and_attachment_date():
    assert margin_effective('自112年1月17日一般交易時段結束後起，預計於春節後恢復。') == '2023-01-17T13:45:00+08:00'
    check_publication('發文日期：中華民國112年1月16日', '2023-01-16')
    check_publication('發文日期：中華民國九十三年五月二十六日', '2004-05-27')
    with pytest.raises(ValueError):
        check_publication('發文日期：中華民國九十三年五月二十六日', '2005-05-27')
    with pytest.raises(ValueError):
        check_publication('發文日期：中華民國115年1月16日', '2023-01-16')
    assert margin_effective('自100年9月8日交易時段結束後起實施') == '2011-09-08T13:45:00+08:00'
    with pytest.raises(ValueError, match='regular close'):
        margin_effective('自107年9月8日交易時段結束後起實施')


def test_legacy_word_levels_keep_rows_and_futures_option_columns_distinct():
    text = ('結算保證金適用比例 |維持保證金適用比例 |原始保證金適用比例 | |'
            '1 |DSF |宏碁期貨 |宏碁股份有限公司 |級距1 |10.00% |10.35% |13.50% | |'
            '2 |DTF |勝華期貨 |勝華科技股份有限公司 |級距2 |12.00% |12.42% |16.20% | |'
            '3 |DSO |宏碁選擇權 |宏碁股份有限公司 |級距1 |10.00% |10.35% |13.50% | |註：其他')
    rows = margin_legacy_word_levels(text)
    assert [r['product'] for r in rows] == ['DSF','DTF']
    assert rows[0]['after'] == [0.135,0.1035,0.1]
    assert rows[1]['after'] == [0.162,0.1242,0.12]
    assert [r['product'] for r in margin_legacy_word_levels(text.replace('13.50%','13.50'))] == ['DTF']
    assert margin_legacy_word_levels(text.replace('結算保證金適用比例','原始保證金適用比例')) == []


def test_legacy_fixed_margin_keeps_chinese_amounts_and_unknown_currency():
    text='''|商品別|調整後保證金金額|調整前保證金金額|
| |原始|維持|結算|原始|維持|結算|
|TX|十萬五千元|八萬一千元|七萬元|十二萬元|九萬二千元|八萬元|
|MTX|2萬7千元|2萬1千元|1萬8千元|3萬元|2萬3千元|2萬元|
|TXO|三萬元|二萬元|一萬元|三萬元|二萬元|一萬元|
'''
    rows=margin_legacy_word_levels(text)
    assert [r['product'] for r in rows]==['TX','MTX']
    assert rows[0]['after']==[105000,81000,70000]
    assert rows[0]['before']==[120000,92000,80000]
    assert rows[1]['after']==[27000,21000,18000]
    assert all(r['margin_kind']=='fixed_unresolved_currency' for r in rows)
    assert all(r['candidate_only'] for r in rows)
    wrapped='''|商品別|原始|維持|結算|
| |保證金|保證金|保證金|
|T5F|四萬|三萬|三萬元|
| |五千元|五千元| |
'''
    assert margin_legacy_word_levels(wrapped)[0]['after']==[45000,35000,30000]
    assert margin_legacy_word_levels(wrapped.replace('五千元','每口五千元'))==[]


def test_legacy_positions_never_confuse_grade_or_corporate_limit():
    from stockagent.data.tw_futures_margin_preparation import position_legacy_word_candidates
    text='''股票期貨 部位限制契約數 自然人 法人
|CAF|南亞期貨|4|1,250|3,750|
|CBF|中鋼期貨|1|5,000|15,000|
|CAO|南亞選擇權|4|1,250|3,750|
1 |宏碁股份有限公司 |DSF |宏碁期貨 |DSO |宏碁選擇權 |2,500 |7,500 |18,750 | |2 |勝華科技股份有限公司 |DTF |勝華期貨 |DTO |勝華選擇權 |5,000 |15,000 |37,500 |
'''
    rows=position_legacy_word_candidates(text)
    assert [(r['product'],r['natural_person_limit']) for r in rows]==[
        ('CAF',1250),('CBF',5000),('DSF',2500),('DTF',5000)]
    assert all(r['candidate_only'] and r['effective_date'] is None for r in rows)
    assert position_legacy_word_candidates(text.replace('自然人','法人'))==[]


def test_native_launch_position_table_preserves_combined_mini_formula():
    from stockagent.data.tw_futures_margin_preparation import position_grid_candidates
    rows=[['標的證券代碼','契約英文代碼','中文簡稱','部位限制數(單位：契約數)','標準型證券股數/受益權單位'],
          [None,None,None,'自然人',None],
          ['1519','VM','華城期貨','2,000','2,000'],
          ['1519','VN','小型華城期貨','與華城期貨合併計算(依20口小型華城期貨等於1口華城期貨合併計算)','100']]
    facts=position_grid_candidates(rows,'新增股票期貨')
    assert len(facts)==2
    assert facts[0]['natural_person_limit']==2000
    assert facts[1]['natural_person_limit'] is None
    assert facts[1]['combined_position_ratio']=='1/20'
    assert facts[1]['combined_position_base_name']=='華城期貨'
    assert facts[1]['underlying_symbol']=='1519'
    assert facts[1]['contract_multiplier']==100
    assert facts[1]['candidate_only'] and facts[1]['effective_date'] is None


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


def test_candidate_press_body_clock_and_explicit_listing_date():
    from scripts.build_tw_futures_margin_event_candidates import candidate_notice_clock
    press='臺灣期貨交易所新聞稿 部門：結算部 中華民國110年10月13日 臺灣期貨交易所於110年10月13日公告調整，並自110年10月14日一般交易時段結束後起實施。'
    clock=candidate_notice_clock(press,'2021-10-13')
    assert clock['issue_date_bound'] and clock['chronological']
    assert clock['effective_date']=='2021-10-14'
    assert not candidate_notice_clock(press,'2020-10-13')['issue_date_bound']
    assert not candidate_notice_clock('發文日期：中華民國113年10月13日 '+press,'2021-10-13')['issue_date_bound']
    disposal=('臺灣期貨交易所新聞稿 部門：結算部 中華民國109年5月19日 '
              '臺灣期貨交易所於109年5月20日調高，期交所依規定調高保證金，'
              '自109年5月20日(證券市場處置生效日次一營業日)該契約交易時段結束後起實施。')
    result=candidate_notice_clock(disposal,'2020-05-19')
    assert result['issue_date_bound'] and result['effective_date']=='2020-05-20'
    assert result['effective_phase']=='after_product_trading_session_unspecified'
    listing='發文日期：中華民國113年10月28日 前揭契約訂於113年11月1日上市。'
    clock=candidate_notice_clock(listing,'2024-10-28')
    assert clock['effective_phase']=='new_contract_listing' and clock['effective_date']=='2024-11-01'
    assert candidate_notice_clock(listing+'生效日期：113年11月1日','2024-10-28')['effective_phase']=='new_contract_listing'
    # Multiple product dates cannot silently become a single launch clock.
    assert candidate_notice_clock(listing+'另一契約訂於113年11月4日上市。','2024-10-28')['effective_date'] is None
    listing='發文日期：中華民國111年1月24日 旨揭契約上市日期為111年2月7日，交割月份為111年2月。'
    clock=candidate_notice_clock(listing,'2022-01-24')
    assert clock['effective_phase']=='new_contract_listing' and clock['effective_date']=='2022-02-07'


def test_partial_native_tables_do_not_hide_reviewed_scanned_notice_clock(tmp_path):
    from scripts.build_tw_futures_margin_event_candidates import refresh_candidate_clocks
    text='發文日期：中華民國110年10月13日 並自110年10月14日一般交易時段結束後起實施。'
    (tmp_path/'review.txt').write_text(text)
    (tmp_path/'tables.json').write_text(json.dumps({'pages':[{'native_text':'原始 維持 結算 保證金金額'}]}))
    receipt={'status':'complete','content_sha256':'original'}
    (tmp_path/'native_receipt.json').write_text(json.dumps(receipt))
    (tmp_path/'review_receipt.json').write_text(json.dumps(dict(receipt,
        files=[dict(path='candidate.txt',sha256=sha256_file(tmp_path/'review.txt'))])))
    kinds={'review.txt':'review_pages_including_ocr','tables.json':'native_table_tables.json',
           'native_receipt.json':'native_table_receipt.json','review_receipt.json':'page_extraction_receipt'}
    archive=SimpleNamespace(bundle=tmp_path,document=lambda url:{'content_sha256':'original','text':''},
        sources={p:dict(path=p,url='official',kind=k,sha256=sha256_file(tmp_path/p)) for p,k in kinds.items()})
    row=dict(source_url='official',source_content_sha256='original',published_date='2021-10-13',
             extraction='canonical_native',issue_date_bound=False,point_in_time_verified=False)
    assert refresh_candidate_clocks(archive,[row],[])==1
    assert row['issue_date_bound'] and row['effective_date']=='2021-10-14'
    assert row['point_in_time_verified'] is False
    (tmp_path/'native_receipt.json').write_text(json.dumps(dict(receipt,content_sha256='different')))
    with pytest.raises(ValueError,match='different source'):
        refresh_candidate_clocks(archive,[row],[])


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


def test_visual_rule_review_binds_amendment_clock_and_per_row_limits(tmp_path):
    from scripts.build_tw_futures_margin_event_candidates import source_review_candidates
    c=sqlite3.connect(':memory:');c.row_factory=sqlite3.Row
    c.executescript("CREATE TABLE announcements(url,published_date);"
                   "INSERT INTO announcements VALUES('launch','2013-11-11'),('amendment','2013-11-19');")
    archive=SimpleNamespace(conn=c,document=lambda url: {'content_sha256':url},copy=lambda *a,**kw:None)
    review=dict(review_kind='source_bound_visual_transcription',source_url='launch',content_sha256='launch',
        published_date='2013-11-11',known_at='2013-11-19T23:59:59+08:00',effective_date='2013-11-21',
        effective_phase='product_regular_open',margin_kind='notional_rate',
        amendments=[dict(source_url='amendment',content_sha256='amendment',published_date='2013-11-19')],
        rows=[dict(product='MFF',after=dict(initial=.135,maintenance=.1035,clearing=.1),natural_person_limit=2000),
              dict(product='DDF',after=dict(initial=.162,maintenance=.1242,clearing=.12),natural_person_limit=8000)])
    path=tmp_path/'review.json'
    path.write_text(json.dumps({'reviews':[review]}))
    margins,positions=source_review_candidates(archive,path)
    assert [m['after'] for m in margins]==[[.135,.1035,.1],[.162,.1242,.12]]
    assert [p['natural_person_limit'] for p in positions]==[2000,8000]
    assert all(not f['point_in_time_verified'] for f in margins+positions)
    review['rows'][0]['before'] = dict(initial=.162, maintenance=.1242, clearing=.12)
    path.write_text(json.dumps(review))
    margins, _ = source_review_candidates(archive, path)
    assert margins[0]['before'] == [.162, .1242, .12]
    assert margins[0]['event_type'] == 'before_after'
    assert margins[1]['before'] is None
    review['rows'][0]['before']['maintenance'] = .2
    path.write_text(json.dumps(review))
    with pytest.raises(ValueError, match='prior margin hierarchy'):
        source_review_candidates(archive, path)
    review['rows'][0].pop('before')
    review['known_at']='2013-11-11T23:59:59+08:00'
    path.write_text(json.dumps({'reviews':[review]}))
    with pytest.raises(ValueError,match='clock'):
        source_review_candidates(archive,path)
    review['known_at']='2013-11-19T23:59:59+08:00'
    review['amendments'][0]['content_sha256']='wrong'
    path.write_text(json.dumps(review))
    with pytest.raises(ValueError,match='SHA'):
        source_review_candidates(archive,path)


def test_reviewed_temporary_margin_keeps_product_ends_and_no_invented_before(tmp_path):
    from scripts.build_tw_futures_margin_event_candidates import source_review_candidates,referenced_before_restoration
    from downloader.artifact_io import sha256_file
    from stockagent.data.tw_futures_margin_preparation import compact
    text=('發文日期：中華民國115年8月3日。商品為南電期貨(LYF)及合晶期貨(PLF)。'
          '本次調整自115年8月4日一般交易時段結束後起實施。'
          '南電期貨於115年8月18日一般交易時段結束後，恢復為115年8月4日調整前之保證金；'
          '合晶期貨於115年8月14日一般交易時段結束後，'
          '恢復為115年7月27日標的證券未經處置前之保證金。')
    c=sqlite3.connect(':memory:');c.row_factory=sqlite3.Row
    c.executescript("CREATE TABLE announcements(url,published_date);"
                   "INSERT INTO announcements VALUES('source','2026-08-03');")
    image=tmp_path/'page.png';image.write_bytes(b'inspected source page')
    archive=SimpleNamespace(conn=c,document=lambda url:{'content_sha256':'a'*64},copy=lambda *a,**kw:None)
    review=dict(review_kind='source_bound_visual_transcription',source_url='source',content_sha256='a'*64,
        published_date='2026-08-03',known_at='2026-08-03T23:59:59+08:00',effective_date='2026-08-04',
        effective_phase='after_product_regular_close',margin_kind='notional_rate',transcribed_text=text,
        pages=[dict(page=1,path=image.name,sha256=sha256_file(image))],
        temporary_end_evidence=[dict(date_iso=d,boundary='after_regular_session')
            for d in ['2026-08-14','2026-08-18']],
        rows=[dict(product='LYF',page=1,after=dict(initial=.324,maintenance=.2484,clearing=.24),
                   before=dict(initial=.216,maintenance=.1656,clearing=.16)),
              dict(product='PLF',page=1,after=dict(initial=.405,maintenance=.3105,clearing=.3),
                   non_disposed_reference=dict(initial=.2025,maintenance=.1553,clearing=.15))])
    path=tmp_path/'review.json';path.write_text(json.dumps(dict(reviews=[review])))
    facts,positions=source_review_candidates(archive,path)
    assert not positions
    for fact,day in zip(facts,['2026-08-18','2026-08-14'],strict=True):
        assert {e['date_iso'] for e in json.loads(fact['temporary_end_evidence'])}=={day}
        assert fact['visual_review_sha256']==sha256_file(path)
        assert fact['source_review_clock_text']==text
    assert facts[1]['before'] is None
    assert facts[1]['declared_non_disposed_margin']==[.2025,.1553,.15]
    prior=dict(facts[1],effective_date='2026-07-27',known_at='2026-07-24T23:59:59+08:00',
               source_content_sha256='b'*64,before=[.2025,.1553,.15])
    bound=referenced_before_restoration([compact(text)],facts[1],[prior,*facts])
    assert bound['restoration_target']==prior['before']
    assert json.loads(bound['restoration_reference_evidence'])['source_content_sha256s']==['b'*64]
    assert referenced_before_restoration([compact(text)],facts[1],facts) is None
    assert referenced_before_restoration([compact(text)],facts[1],[dict(prior,before=[.216,.1656,.16])]) is None


@pytest.mark.parametrize('problem', [None, 'missing_issuing_page', 'wrong_day', 'tampered_page',
                                    'unbound_attachment', 'amendment_without_issuing_date'])
def test_scanned_margin_issuing_page_owns_only_its_bound_attachment_clock(tmp_path, problem):
    from scripts.build_tw_futures_margin_event_candidates import source_review_candidates
    from downloader.artifact_io import sha256_file
    c = sqlite3.connect(':memory:'); c.row_factory = sqlite3.Row
    c.executescript("CREATE TABLE announcements(url,published_date);"
                   "INSERT INTO announcements VALUES('notice','2025-12-23'),('amended','2025-12-24');")
    image = tmp_path / 'issuing.png'; image.write_bytes(b'inspected issuing page')
    def document(url):
        return dict(content_sha256=('a' if url == 'attachment' else 'b') * 64,
                    text='native financial annex with no issuing date')
    def copy(path, digest, **kwargs):
        if sha256_file(path) != digest:
            raise ValueError('review page SHA mismatch')
    archive = SimpleNamespace(conn=c, document=document, copy=copy,
                              children=lambda url: [] if problem == 'unbound_attachment' else ['attachment'])
    review = dict(review_kind='source_bound_visual_transcription', source_url='attachment',
        announcement_url='notice', content_sha256='a' * 64, published_date='2025-12-23',
        known_at='2025-12-23T23:59:59+08:00', effective_date='2025-12-25',
        effective_phase='after_product_regular_close', margin_kind='notional_rate',
        transcribed_text='發文日期：中華民國114年12月23日。',
        pages=[dict(page=1, path=image.name, sha256=sha256_file(image))],
        rows=[dict(product='OTF', page=1, after=dict(initial=.432, maintenance=.3312, clearing=.32))])
    if problem == 'missing_issuing_page': review['pages'][0]['page'] = 2
    if problem == 'wrong_day': review['transcribed_text'] = '發文日期：中華民國114年12月29日。'
    if problem == 'tampered_page': image.write_bytes(b'different issuing page')
    if problem == 'amendment_without_issuing_date':
        review['known_at'] = '2025-12-24T23:59:59+08:00'
        review['amendments'] = [dict(source_url='amendment_attachment', announcement_url='amended',
            content_sha256='b' * 64, published_date='2025-12-24')]
        archive.children = lambda url: ['attachment', 'amendment_attachment']
    path = tmp_path / 'review.json'; path.write_text(json.dumps(review))
    if problem:
        with pytest.raises(ValueError): source_review_candidates(archive, path)
    else:
        margins, positions = source_review_candidates(archive, path)
        assert not positions and margins[0]['known_at'] == review['known_at']
        assert margins[0]['after'] == [.432, .3312, .32]


def test_bounded_margin_reextraction_never_opens_an_unselected_original():
    from scripts.build_tw_futures_margin_event_candidates import reextract_retained_margin_text
    c = sqlite3.connect(':memory:'); c.row_factory = sqlite3.Row
    c.executescript('CREATE TABLE announcements(url,published_date);'
                   'CREATE TABLE documents(url,state);CREATE TABLE links(parent,child);')
    own = "source-with-'quote"
    c.executemany('INSERT INTO announcements VALUES (?,?)', [(own, '2026-05-01'),
                                                           ('unselected', '2026-05-01')])
    c.executemany('INSERT INTO documents VALUES (?,?)', [(own, 'complete'), ('unselected', 'complete')])
    text = ('發文日期：中華民國115年5月1日。自115年5月4日一般交易時段結束後起實施。\n'
            '單位：比例(%)\nHBF\n調高2倍後保證金適用比例 標的證券未經處置前保證金適用比例\n'
            '原始保證金 維持保證金 結算保證金 原始保證金 維持保證金 結算保證金\n'
            '保證金 32.40% 24.84% 24.00% 16.20% 12.42% 12.00%\n')
    opened = []
    def document(url):
        assert url == own, 'an unrelated original must not be opened'
        opened.append(url)
        return dict(text=text, content_sha256='a' * 64)
    archive = SimpleNamespace(conn=c, sources={}, document=document)
    assert reextract_retained_margin_text(archive, source_urls=[]) == [] and not opened
    facts = reextract_retained_margin_text(archive, source_urls=[own, own])
    assert opened == [own] and len(facts) == 1
    assert facts[0]['product'] == 'HBF' and facts[0]['before'] is None
    assert facts[0]['declared_non_disposed_margin'] == [.162, .1242, .12]
    assert facts[0]['effective_date'] == '2026-05-04'
    assert facts[0]['issue_date_bound']


def test_candidate_reuse_preserves_provenance_and_rejects_tampering(tmp_path):
    from scripts.build_tw_futures_margin_event_candidates import reuse_candidate_bundle
    parent=tmp_path/'parent';parent.mkdir()
    for name in ('margin','position','corporate'):
        pl.DataFrame({'product':['TX'],'point_in_time_verified':[False]}).write_parquet(
            parent/(name+'_event_candidates.parquet'))
    (parent/'announcement_audit.json').write_text('[{"announcement_url":"official"}]')
    (parent/'product_event_coverage.csv').write_text('product\nTX\n')
    source=parent/'source.txt';source.write_text('retained original')
    outputs={p.name:{'sha256':sha256_file(p)} for p in parent.iterdir() if p!=source}
    receipt=dict(schema_version=1,point_in_time_verified=False,all_products_training_ready=False,
                 outputs=outputs,sources=[dict(path='source.txt',sha256=sha256_file(source),url='official',kind='raw')])
    (parent/'manifest.json').write_text(json.dumps(receipt))
    copied=[]
    def copy(path,digest,**proof):
        if sha256_file(path)!=digest:
            raise ValueError('source SHA mismatch')
        copied.append((path,proof))
    archive=SimpleNamespace(copy=copy)
    *groups,audit=reuse_candidate_bundle(archive,parent)
    assert all(g[0]['product']=='TX' for g in groups)
    assert audit[0]['announcement_url']=='official'
    assert copied[-1][1]['kind']=='parent_candidate_manifest'
    source.write_text('changed')
    with pytest.raises(ValueError,match='SHA'):
        reuse_candidate_bundle(archive,parent)
    source.write_text('retained original')
    (parent/'product_event_coverage.csv').write_text('changed')
    with pytest.raises(ValueError,match='output SHA'):
        reuse_candidate_bundle(archive,parent)


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
