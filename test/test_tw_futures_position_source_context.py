from copy import deepcopy
from datetime import date
import json
import sqlite3
from types import SimpleNamespace

import polars as pl
import pytest
import scripts.build_tw_futures_margin_event_candidates as builder

from scripts.build_tw_futures_margin_event_candidates import (
    next_nearby_position_date, repair_position_source_context, retained_document_text_views,
)
from downloader.artifact_io import sha256_file


def lives():
    return pl.DataFrame([
        dict(product='HSF', contract=month, first_observed_date=date(2019, 12, 1),
             last_observed_date=end, official_expiry=end, calendar_end=end)
        for month, end in [('202001', date(2020, 1, 15)), ('202002', date(2020, 2, 19)),
                           ('202003', date(2020, 3, 18)), ('202006', date(2020, 6, 17))]
    ])


def test_next_nearby_uses_post_publication_month_and_next_actual_session():
    result = next_nearby_position_date('HSF', '2020-01-15', lives(),
                                       [date(2020, 3, 18), date(2020, 3, 19)])
    assert result['next_nearby_contract'] == '202003'
    assert result['effective_date'] == '2020-03-19'
    # A null near expiry is not permission to skip to a more distant month.
    broken = lives().with_columns(pl.when(pl.col('contract') == '202002')
        .then(None).otherwise(pl.col('official_expiry')).alias('official_expiry'))
    assert next_nearby_position_date('HSF', '2020-01-15', broken, [date(2020, 3, 19)]) is None
    assert next_nearby_position_date('HSF', '2020-01-15', lives(), [date(2020, 3, 18)]) is None
    duplicates = pl.concat([lives(), lives().filter(pl.col('contract') == '202002')])
    assert next_nearby_position_date('HSF', '2020-01-15', duplicates, [date(2020, 3, 19)]) is None


def archive(tmp_path, docs, covers=()):
    conn = sqlite3.connect(':memory:')
    conn.executescript('CREATE TABLE announcements(url TEXT,published_date TEXT);'
                       'CREATE TABLE links(parent TEXT,child TEXT);'
                       'CREATE TABLE documents(url TEXT,state TEXT);')
    conn.execute('INSERT INTO announcements VALUES (?,?)', ('notice', '2021-01-20'))
    for url in ['annex', *covers]:
        conn.execute('INSERT INTO links VALUES (?,?)', ('notice', url))
    for url in docs:
        conn.execute('INSERT INTO documents VALUES (?,?)', (url, 'complete'))
    return SimpleNamespace(conn=conn, bundle=tmp_path, sources={},
        document=lambda url: dict(text=docs[url], content_sha256=url + '-sha'),
        children=lambda url: [r[0] for r in conn.execute('SELECT child FROM links WHERE parent=?', (url,))])


def fact():
    return dict(product='HSF', source_url='annex', source_content_sha256='annex-sha',
        published_date='2021-01-20', issue_date_bound=False, effective_date='2021-01-21',
        effective_phase='product_regular_open', direction='raise', after=8000)


COVER = ('發文日期：中華民國110年1月20日 股票期貨 部位限制 詳如附件 '
         '調高者，自110年1月21日起生效；調降者，自110年3月18日起生效。')


def test_multiple_retained_ocr_views_bind_each_own_receipt(tmp_path):
    sources = []
    for index, text in enumerate(['first extraction 16.20%', 'second extraction 13.50%']):
        page = tmp_path / f'view{index}.txt'
        page.write_text(text)
        receipt = tmp_path / f'proof{index}.json'
        receipt.write_text(json.dumps(dict(status='complete', content_sha256='source-sha',
            files=[dict(path='candidate.txt', sha256=sha256_file(page))])))
        for path, kind in [(page, 'review_pages_including_ocr'), (receipt, 'page_extraction_receipt')]:
            sources.append(dict(url='source', path=path.name, sha256=sha256_file(path), kind=kind))
    a = SimpleNamespace(bundle=tmp_path, sources={s['path']: s for s in sources})
    document = dict(text='native', content_sha256='source-sha')
    views = retained_document_text_views(a, 'source', document, sources=sources)
    assert {t for t, _ in views} == {'native', 'first extraction 16.20%', 'second extraction 13.50%'}
    assert retained_document_text_views(a, 'source', document) == views
    with pytest.raises(ValueError, match='matching extraction receipt'):
        retained_document_text_views(a, 'source', document, sources=sources[:-1])
    (tmp_path / 'view0.txt').write_text('tampered')
    with pytest.raises(ValueError, match='text SHA mismatch'):
        retained_document_text_views(a, 'source', document, sources=sources)


def test_reextraction_preserves_source_bound_visual_cells_only_in_reviewed_scope():
    row = dict(source_content_sha256='a' * 64, source_url='notice', product='CYF',
        margin_kind='notional_rate', before=None, after=[.135, .1035, .1])
    reviewed = dict(row, extraction='source_bound_visual_transcription')
    flawed = dict(row, after=[.162, .1242, .12])
    sibling = dict(flawed, product='CWF')
    other_notice = dict(flawed, source_content_sha256='b' * 64)
    result = builder.retain_margin_code_corrections([reviewed], [flawed, sibling, other_notice])
    assert result == [sibling, other_notice]


def test_margin_restoration_dates_follow_product_clauses_not_other_revocations():
    text = ('配合證券市場處置期間，115年1月22日一般交易時段結束後，南亞科期貨契約(CYF)、'
            '金居期貨契約(PQF)及力積電期貨契約(QZF)恢復為115年1月12日調整前保證金，'
            '晶豪科期貨契約(IIF)恢復為115年1月7日調整前保證金；'
            '華邦電期貨契約(FZF)於115年1月26日一般交易時段結束後恢復為115年1月12日調整前保證金。'
            '原115年1月6日公告自115年1月12日晶豪科期貨一般交易時段結束後停止適用。')
    base = dict(effective_date='2026-01-12', effective_phase='after_product_regular_close',
                requires_reversion_review=True, temporary_end_evidence='[]')
    for product, day in [('CYF', '2026-01-22'), ('PQF', '2026-01-22'),
                         ('IIF', '2026-01-22'), ('FZF', '2026-01-26')]:
        result = builder.product_margin_restoration_clock(text, product, base)
        assert result['effective_date'] == '2026-01-12'
        assert {e['date_iso'] for e in json.loads(result['temporary_end_evidence'])} == {day}
    assert builder.product_margin_restoration_clock(text, 'TX', base) == base
    ambiguous = text + '另115年1月23日一般交易時段結束後恢復原保證金。'
    assert builder.product_margin_restoration_clock(ambiguous, 'CYF', base) == base


def test_visual_corporate_identity_replaces_only_reviewed_origin_date_months():
    reviewed = dict(source_content_sha256='a'*64, from_product='IAF', product='IA1',
        effective_date='2020-07-22', contract_months=['202008', '202009'])
    wrong = dict(reviewed, product='TA1')
    others = [dict(wrong, contract_months=['202012']), dict(wrong, from_product='TQF'),
              dict(wrong, source_content_sha256='b'*64),
              dict(wrong, effective_date='2021-07-22'), dict(wrong, contract_months=[])]
    result = builder.replace_reviewed_corporate_facts([wrong, *others], [reviewed])
    assert result == [*others, reviewed]


def test_same_day_grade_change_keeps_combined_share_constraint_and_source_chain(tmp_path):
    old = dict(product='EYF', effective_date='2021-06-17', effective_phase='product_regular_open',
        event_type='absolute_level', unit='contracts', natural_person_limit=8000,
        known_at='2021-04-20T23:59:59+08:00', issue_date_bound=True,
        source_content_sha256='a'*64, source_url='old')
    new = dict(old, effective_date='2021-12-16', natural_person_limit=4000,
        known_at='2021-10-20T23:59:59+08:00', source_content_sha256='b'*64, source_url='new')
    corporate = dict(old, unit='shares', event_type='corporate_securities_unit_limit',
        effective_date='2021-12-16', natural_person_limit=16000000, position_unit=2000,
        combined_products=['EYF','EY1'], source_content_sha256='c'*64, source_url='corporate',
        known_at='2021-10-18T23:59:59+08:00')
    a = SimpleNamespace(bundle=tmp_path, sources={}, document=lambda url:dict(
        content_sha256='c'*64,text='部位限制數應依本契約最新適用部位限制級數計算'))
    simultaneous = [dict(old), dict(new, known_at=corporate['known_at']), dict(corporate),
                    dict(corporate, product='EY1', position_unit=2080)]
    assert len(builder.repair_same_day_position_grade(a, simultaneous)) == 1
    assert simultaneous[2]['natural_person_limit'] == 8000000
    rows=[old,new,corporate,dict(corporate,product='EY1',position_unit=2080)]
    result=builder.repair_same_day_position_grade(a,rows)
    assert len(result)==1 and result[0]['corrected_share_limit']==8000000
    assert all(r['natural_person_limit']==8000000 for r in rows[2:])
    assert all(r['position_grade_source_sha256s']==['a'*64,'b'*64,'c'*64] for r in rows[2:])
    intervals,issues=builder.position_candidate_intervals(rows)
    selected=next(r for r in intervals if r['product']=='EYF' and r['effective_date']=='2021-12-16')
    assert selected['unit']=='shares' and selected['combined_products']==['EY1','EYF']
    assert selected['position_limit']==8000000 and selected['known_at']==new['known_at']
    assert selected['source_content_sha256s']==['a'*64,'b'*64,'c'*64]
    assert not issues
    # Nullable evidence columns survive a Parquet round trip on unrelated facts.
    reloaded = [dict(old, position_grade_source_sha256s=None),
                dict(new, position_grade_source_sha256s=None), *rows[2:]]
    restored, errors = builder.position_candidate_intervals(reloaded)
    assert not errors
    assert restored == intervals
    # Neither a future-publication rule nor an earlier notice overrides this cell.
    for known in ['2021-12-16T12:00:00+08:00','2021-10-17T23:59:59+08:00']:
        unchanged=[old,dict(new,known_at=known),dict(corporate,natural_person_limit=16000000)]
        assert not builder.repair_same_day_position_grade(a,unchanged)
    # An enlarged temporary cap needs its own formula, not this restored-basis case.
    assert not builder.repair_same_day_position_grade(a,
        [old,new,dict(corporate,natural_person_limit=16640000)])
    no_clause=SimpleNamespace(bundle=tmp_path,sources={},document=lambda url:dict(content_sha256='c'*64,text='部位限制'))
    assert not builder.repair_same_day_position_grade(no_clause,
        [old,new,dict(corporate,natural_person_limit=16000000)])


def test_restoration_does_not_borrow_immediately_prior_amount_for_earlier_reference():
    assert builder.has_local_before_restoration(['恢復為調整前之保證金'], '2026-01-12')
    assert builder.has_local_before_restoration(['恢復為115年1月12日調整前保證金'], '2026-01-12')
    for clause in ['恢復為115年1月7日調整前保證金', '恢復為1月7日調整前之保證金']:
        assert not builder.has_local_before_restoration([clause], '2026-01-12')
        assert not builder.has_local_before_restoration(['恢復為調整前之保證金', clause], '2026-01-12')


def test_closure_postponement_requires_the_literal_original_clause():
    clause='調整期間如遇休市、有價證券停止買賣、全\n日暫停交易，則恢復日順延執行。'
    assert builder.has_closed_cash_postponement_clause([clause])
    assert builder.has_closed_cash_postponement_clause([builder.compact(clause)])
    assert not builder.has_closed_cash_postponement_clause(['如遇休市則恢復日不順延執行'])
    assert not builder.has_closed_cash_postponement_clause([clause.replace('則恢復日順延執行','另行公告')])


def test_restoration_short_date_needs_unique_explicit_year_and_forward_boundary():
    text=('臺灣期貨交易所新聞稿中華民國109年3月23日 '
          '自109年3月24日該契約交易時段結束後起實施，'
          '4月7日該契約交易時段結束後恢復為調整前之保證金。')
    row=builder.candidate_notice_clock(text,'2020-03-23')
    assert any(e['date_iso']=='2020-04-07' and e['boundary']=='after_trading_session'
               for e in json.loads(row['temporary_end_evidence']))
    for ambiguous in [text+'參照108年1月1日公告。',text.replace('4月7日','3月7日')]:
        row=builder.candidate_notice_clock(ambiguous,'2020-03-23')
        assert not any(e.get('year_binding') for e in json.loads(row['temporary_end_evidence']))


def test_referenced_restoration_binds_product_and_older_amount_not_current_before():
    text=('115年1月22日一般交易時段結束後，南亞科期貨契約(CYF)恢復為115年1月12日調整前保證金，'
          '晶豪科期貨契約(IIF)恢復為115年1月7日調整前保證金；'
          '華邦電期貨契約(FZF)於115年1月26日一般交易時段結束後恢復為115年1月12日調整前保證金。')
    row=dict(product='IIF',effective_date='2026-01-12',before=[.243,.1863,.18],
        after=[.324,.2484,.24],margin_kind='notional_rate',known_at='2026-01-09T23:59:59+08:00')
    prior=dict(row,effective_date='2026-01-07',before=[.162,.1242,.12],
        issue_date_bound=True,known_at='2026-01-06T23:59:59+08:00',source_content_sha256='a'*64)
    target=builder.referenced_before_restoration([text],row,[prior])
    assert target['restoration_target']==[.162,.1242,.12]
    assert target['restoration_rule']=='return_to_referenced_before'
    assert builder.referenced_before_restoration([text],row,[]) is None
    assert builder.referenced_before_restoration([text],dict(row,effective_date=None),[prior]) is None
    assert builder.referenced_before_restoration([text],dict(row,known_at=None),[prior]) is None
    assert builder.referenced_before_restoration([text],row,[dict(prior,known_at='2026-01-10T23:59:59+08:00')]) is None
    assert builder.referenced_before_restoration([text],row,[prior,dict(prior,before=[.135,.1035,.1])]) is None
    for product in ['CYF','FZF']:
        target=builder.referenced_before_restoration([text],dict(row,product=product),[prior])
        assert target['restoration_target']==row['before']


def test_incomplete_margin_table_binds_issuing_context_without_overriding_complete_clocks(tmp_path):
    text = ('發文日期：中華民國112年1月30日。'
            '本次保證金金額並自112年1月31日一般交易時段結束後實施。')
    archive = SimpleNamespace(bundle=tmp_path, sources={}, document=lambda url:dict(
        text=text, content_sha256='a'*64))
    incomplete = dict(product='TX', effective_date=None, effective_phase='unresolved',
        issue_date_bound=False, published_date='2023-01-30', known_at='2023-01-30T23:59:59+08:00',
        before=[203000.,156000.,150000.], after=[184000.,141000.,136000.],
        margin_kind='fixed_twd', source_url='source', source_content_sha256='a'*64)
    complete = dict(incomplete, product='TF', effective_date='2023-01-31',
        effective_phase='after_product_regular_close', issue_date_bound=True,
        temporary_end_evidence='product-local')
    restoration = dict(incomplete, extraction='observed_disposition_conditional_restoration')
    rows = [dict(incomplete), dict(complete), dict(restoration)]
    result = builder.repair_missing_margin_source_context(archive, rows)
    assert len(result) == 1 and rows[0]['effective_date'] == '2023-01-31'
    assert rows[0]['effective_phase'] == 'after_product_regular_close'
    assert rows[0]['before'] == incomplete['before'] and rows[0]['after'] == incomplete['after']
    assert rows[1:] == [complete, restoration]
    # Distinct starts in the original remain ambiguous, with no partial mutation.
    archive.document = lambda url:dict(text=text +
        '另自112年2月1日一般交易時段結束後實施。', content_sha256='a'*64)
    unresolved = [dict(incomplete)]
    assert not builder.repair_missing_margin_source_context(archive, unresolved)
    assert unresolved == [incomplete]


def test_restoration_uses_latest_named_subject_in_a_multi_product_notice():
    text=('中華民國109年3月24日公告宣德期貨契約(PPF)及為升期貨契約(ODF)。'
          '本次調高係宣德期貨契約(PPF)，4月8日恢復為調整前之保證金。'
          '本次為升期貨契約(ODF)調高，自109年3月25日起實施，4月8日恢復為3月20日調整前之保證金。')
    row=dict(product='ODF',effective_date='2020-03-25',before=[.243,.1863,.18],
        margin_kind='notional_rate',known_at='2020-03-24T23:59:59+08:00')
    prior=dict(row,effective_date='2020-03-20',before=[.162,.1242,.12],
        issue_date_bound=True,known_at='2020-03-19T23:59:59+08:00',source_content_sha256='a'*64)
    result=builder.referenced_before_restoration([text],row,[prior])
    assert result['restoration_target']==prior['before']
    result=builder.referenced_before_restoration([text],dict(row,product='PPF'),[prior])
    assert result['restoration_target']==row['before']
    # A connected list retains every explicitly named member across a sentence.
    text=('本次華新科期貨契約(HBF)、元太期貨契約(NVF)及金居期貨契約(PQF)，自115年5月18日起實施。'
          '115年5月28日恢復為115年5月18日調整前之保證金。'
          '三、本次延長之信昌電期貨契約(PKF)，恢復為115年5月13日調整前之保證金。')
    row=dict(row,effective_date='2026-05-18',known_at='2026-05-15T23:59:59+08:00')
    for product in ['HBF','NVF','PQF']:
        result=builder.referenced_before_restoration([text],dict(row,product=product),[])
        assert result['restoration_target']==row['before']


def test_partial_review_bundle_admits_only_independently_complete_documents(tmp_path):
    root=tmp_path/'ocr';folder=root/'documents'/('a'*64);folder.mkdir(parents=True)
    source=folder/'candidate.txt';source.write_text('完整原頁')
    proof=dict(url='source',content_sha256='a'*64,status='complete',pages=[dict(extraction='ocr_candidate')],
        files=[dict(path='candidate.txt',sha256=sha256_file(source))])
    (folder/'receipt.json').write_text(json.dumps(proof))
    manifest=dict(status='partial',point_in_time_verified=False,
        documents=[dict(content_sha256='a'*64,status='complete'),dict(content_sha256='b'*64,status='partial')],
        failures=[dict(content_sha256='c'*64,error='no text')],unprocessed_documents=2)
    (root/'manifest.json').write_text(json.dumps(manifest))
    archive=SimpleNamespace(copy=lambda *args,**kw:None,document=lambda url:dict(content_sha256='a'*64))
    result=builder.retain_review_override(archive,root)
    assert result['admitted_complete_documents']==1
    assert result['source_status']=='partial' and result['unprocessed_documents']==2
    assert result['skipped_partial_documents']==['b'*64]
    assert result['failed_documents']==['c'*64]
    source.write_text('changed')
    with pytest.raises(ValueError,match='SHA mismatch'):
        builder.retain_review_override(archive,root)


def test_single_session_clock_review_is_stock_only_dated_and_source_bound(tmp_path):
    universe = tmp_path/'products.csv'
    pl.DataFrame(dict(product=['ODF','TX','NYF'], underlying_security_type=['stock','index','etf'])).write_csv(universe)
    review = dict(review_kind='source_bound_single_regular_session', effective_date='2010-01-25',
        valid_until_exclusive='2024-01-22', known_at='2010-01-08T23:59:59+08:00',
        sources=[dict(source_url='law',content_sha256='a'*64,required_clauses=['日盤時段'])],
        universe=dict(path=str(universe),sha256=sha256_file(universe)),products=['ODF'])
    path=tmp_path/'review.json';path.write_text(json.dumps(review))
    a=SimpleNamespace(bundle=tmp_path, copy=lambda *args,**kw:None,
        document=lambda url:dict(content_sha256='a'*64,text='日盤時段'))
    row=dict(product='ODF',effective_date='2020-03-20',source_content_sha256='b'*64,
        effective_phase='after_product_trading_session_unspecified',
        temporary_end_evidence=json.dumps([dict(date_iso='2020-04-01',boundary='after_trading_session')]))
    rows=[dict(row),dict(row,product='TX'),dict(row,product='NYF'),dict(row,effective_date='2024-01-22')]
    assert len(builder.bind_single_session_margin_clocks(a,rows,path))==1
    assert rows[0]['effective_phase']=='after_product_regular_close'
    assert json.loads(rows[0]['temporary_end_evidence'])[0]['boundary']=='after_regular_session'
    assert all(r['effective_phase']=='after_product_trading_session_unspecified' for r in rows[1:])
    universe.write_text('modified')
    with pytest.raises(ValueError,match='scope SHA'):
        builder.bind_single_session_margin_clocks(a,[row],path)


def test_annex_requires_exact_dated_parent_and_matching_cover(tmp_path):
    a = archive(tmp_path, dict(annex='金額附件', notice='公告', cover=COVER), ['cover'])
    row = fact()
    changes = repair_position_source_context(a, [row], lives(), [])
    assert len(changes) == 1 and row['issue_date_bound']
    assert row['after'] == 8000 and row['effective_date'] == '2021-01-21'
    assert json.loads(row['publication_context_evidence'])['cover_content_sha256'] == 'cover-sha'
    # A second dated cover that contradicts the same annex leaves it blocked.
    a = archive(tmp_path, dict(annex='附件', notice='公告', cover=COVER,
        conflicting=COVER.replace('1月21日', '1月22日')), ['cover', 'conflicting'])
    row = fact()
    assert repair_position_source_context(a, [row], lives(), []) == []
    assert not row['issue_date_bound']
    a.conn.execute('DELETE FROM links WHERE child=?', ('conflicting',))
    a.conn.execute('UPDATE announcements SET published_date=?', ('2021-01-19',))
    assert repair_position_source_context(a, [fact()], lives(), []) == []


def test_relative_clause_requires_literal_unqualified_lowering_rule(tmp_path, monkeypatch):
    clause = ('調整本公司股票期貨部位限制，調降者，自公告日該期貨或該選擇權已上市之'
              '次近月份契約到期後次一營業日一般交易時段生效。正本：各期貨商')
    row = dict(fact(), published_date='2020-01-15', issue_date_bound=True,
               effective_date=None, direction='lower')
    a = archive(tmp_path, dict(annex=clause))
    before = deepcopy(row)
    assert repair_position_source_context(a, [row], lives(), [date(2020, 3, 19)])
    assert row['effective_date'] == '2020-03-19'
    assert row['after'] == before['after']
    monkeypatch.setattr(builder, 'retained_document_text_views', lambda *args:
        [(clause, 'verified_view'), (clause.replace('月份契約', '月份夫約'), 'incomplete_view')])
    repeated = deepcopy(before)
    assert repair_position_source_context(a, [repeated], lives(), [date(2020, 3, 19)])
    assert repeated['effective_date'] == row['effective_date']
    monkeypatch.undo()
    a = archive(tmp_path, dict(annex=clause.replace('調降者', '除HSF外調降者')))
    assert repair_position_source_context(a, [before], lives(), [date(2020, 3, 19)]) == []
    assert before['effective_date'] is None
    with pytest.raises(ValueError, match='candidate bytes'):
        repair_position_source_context(a, [dict(before, source_content_sha256='wrong')], lives(), [])
