from copy import deepcopy
from datetime import date

import polars as pl
import pytest

from stockagent.data.tw_futures_margin_preparation import (
    bind_dated_corporate_terms,
    corporate_deliverable_components,
    corporate_grid_candidates,
    corporate_identity_boundaries,
    corporate_retained_view_candidates,
    corporate_terms_intervals,
)


def _fact(**changes):
    return dict(product='AA1', from_product='AAF', effective_date='2015-08-06',
        contract_months=['201509'], issue_date_bound=True,
        known_at='2015-07-22T23:59:59+08:00', source_content_sha256='a' * 64,
        source_url='https://www.taifex.com.tw/original', has_equity_credit_fields=True,
        equity_credit_long_per_contract=5000, equity_debit_short_per_contract=5000,
        cash_equity_pair_agrees=True, contract_multiplier=2200.,
        extraction_method='source_ruled_ocr_cell_grid',
        **corporate_deliverable_components('調整為2,200股標的證券', 2200.)) | changes


def _numeric_peer(amount=2000.,**changes):
    import json
    return _fact(to_product='AA1',published_date='2015-07-22',contract_multiplier=amount,
        deliverable_text=f'調整為{amount:,.4f}股標的證券',
        **corporate_deliverable_components(f'調整為{amount:,.4f}股標的證券',amount),
        numeric_source_cells_evidence=json.dumps(dict(source_content_sha256='a'*64,
            path='sources/retained_table.json',sha256='b'*64))) | changes


@pytest.mark.parametrize('problem', [None, 'missing_basis', 'wrong_issue', 'wrong_notice',
    'missing_image', 'partial_pages', 'foreign_url', 'changed_capture', 'wrong_render',
    'current_capture_clock', 'wrong_record'])
def test_missing_index_review_requires_complete_official_original_and_keeps_clock_proxy(tmp_path, problem):
    import gzip
    import json
    import sqlite3
    from types import SimpleNamespace
    import fitz
    from downloader.artifact_io import sha256_file
    from scripts.build_tw_futures_margin_event_candidates import reviewed_corporate_publication
    pdf = fitz.open()
    pdf.new_page(); pdf.new_page()
    original = pdf.tobytes(); pdf.close()
    raw = tmp_path / 'raw.gz'; raw.write_bytes(gzip.compress(original))
    import hashlib
    content = hashlib.sha256(original).hexdigest()
    url = 'https://www.taifex.com.tw/file/taifex/CHINESE/11/attach/original.pdf'
    record = dict(url=url, state='complete', content_sha256=content, raw_path=raw.name,
                  raw_sha256=sha256_file(raw))
    conn = sqlite3.connect(':memory:'); conn.row_factory = sqlite3.Row
    conn.execute('CREATE TABLE documents(url,state,content_sha256,raw_path,raw_sha256)')
    conn.execute('INSERT INTO documents VALUES(?,?,?,?,?)', tuple(record.values()))
    pages = [dict(page=i, image_path=f'page{i}.png', image_sha256=str(i)*64) for i in (1, 2)]
    capture = dict(status='canonical_capture_matches_inspected_original', document=record,
                   publication_inferred_from_current_capture=False)
    render = dict(source_url=url, source_content_sha256=content, pages=deepcopy(pages))
    if problem == 'wrong_record': capture['document'] = record | {'content_sha256':'c'*64}
    if problem == 'current_capture_clock': capture['publication_inferred_from_current_capture'] = True
    if problem == 'wrong_render': render['source_content_sha256'] = 'c'*64
    cp = tmp_path/'capture.json'; cp.write_text(json.dumps(capture))
    rp = tmp_path/'render.json'; rp.write_text(json.dumps(render))
    review = dict(source_url=url, content_sha256=content, published_date='2012-09-04',
        issued_date='2012-09-04', issuing_notice_number='10102015280', pages=pages,
        publication_basis='inspected_original_issuing_date_missing_index',
        canonical_capture_evidence=dict(path=cp.name, sha256=sha256_file(cp)),
        render_evidence=dict(path=rp.name, sha256=sha256_file(rp)))
    if problem == 'missing_basis': review.pop('publication_basis')
    if problem == 'wrong_issue': review['issued_date'] = '2012-09-05'
    if problem == 'wrong_notice': review['issuing_notice_number'] = '10102015281'
    if problem == 'missing_image': review['pages'][1].pop('image_path')
    if problem == 'partial_pages': review['pages'] = review['pages'][:1]
    if problem == 'foreign_url': review['source_url'] = url.replace('www.taifex.com.tw', 'example.com')
    if problem == 'changed_capture': cp.write_text(json.dumps(capture)+' ')
    archive = SimpleNamespace(conn=conn, root=tmp_path, copy=lambda *args, **kwargs: None)
    doc = dict(format='pdf', source_url=url, content_sha256=content)
    text = '發文日期：中華民國101年9月4日。發文字號：台期交字第10102015280號。'
    if problem:
        with pytest.raises(ValueError, match='publication mismatch|missing-index'):
            reviewed_corporate_publication(archive, tmp_path/'review.json', review, doc, text, None)
    else:
        proof = reviewed_corporate_publication(archive, tmp_path/'review.json', review, doc, text, None)
        assert proof['index_publication_verified'] is False
        assert proof['publication_basis'] == review['publication_basis']
        assert 'known_at' not in proof
        with pytest.raises(ValueError, match='publication mismatch'):
            reviewed_corporate_publication(archive, tmp_path/'review.json', review, doc, text,
                {'published_date':'2012-09-05'})


@pytest.mark.parametrize('field',['contract_multiplier','deliverable_security_quantity'])
def test_numeric_repair_requires_same_event_complete_source_cells_and_preserves_original(field):
    from scripts.build_tw_futures_margin_event_candidates import reconcile_corporate_numeric_views
    peer=_numeric_peer(2080.)
    damaged=peer|{field:2.08,'numeric_source_cells_evidence':None}
    before=deepcopy([damaged,peer])
    rows,report=reconcile_corporate_numeric_views([damaged,peer])
    assert [damaged,peer]==before
    assert len(report)==1 and rows[0]['contract_multiplier']==2080.
    assert rows[0]['deliverable_security_quantity']==2080.
    assert rows[0]['equity_credit_long_per_contract']==5000
    assert rows[0]['original_'+field]==2.08
    assert reconcile_corporate_numeric_views(rows)[1]==[]


@pytest.mark.parametrize('problem',['source','product','from_product','date','month','publication',
    'known_at','unbound','no_cells','cells_wrong_original','coherent_conflict','cash_clause',
    'missing_quantity','small_coherent_peer'])
def test_numeric_repair_does_not_cross_identity_or_choose_between_real_conflicts(problem):
    import json
    from scripts.build_tw_futures_margin_event_candidates import reconcile_corporate_numeric_views
    peer=_numeric_peer();damaged=peer|{'contract_multiplier':2.,'numeric_source_cells_evidence':None}
    change=dict(source={'source_content_sha256':'c'*64},product={'product':'BB1'},
        from_product={'from_product':'BBF'},date={'effective_date':'2015-08-07'},
        month={'contract_months':['201512']},publication={'published_date':'2015-07-23'},
        known_at={'known_at':'2015-07-23T23:59:59+08:00'},unbound={'issue_date_bound':False},
        no_cells={'numeric_source_cells_evidence':None},cash_clause={'deliverable_cash_twd':100.})
    if problem in change:peer|=change[problem]
    if problem=='cells_wrong_original':
        peer['numeric_source_cells_evidence']=json.dumps(dict(source_content_sha256='c'*64,
            path='sources/retained_table.json',sha256='b'*64))
    if problem=='missing_quantity':damaged['deliverable_security_quantity']=None
    if problem=='small_coherent_peer':peer=_numeric_peer(2.)
    peers=[peer,_numeric_peer(2100.)] if problem=='coherent_conflict' else [peer]
    rows,report=reconcile_corporate_numeric_views([damaged,*peers])
    assert rows[0]==damaged and report==[]


def test_numeric_diagnostics_preserve_true_small_units_and_zero_cash():
    from scripts.build_tw_futures_margin_event_candidates import corporate_numeric_anomalies,reconcile_corporate_numeric_views
    true_small=_numeric_peer(79.5925,extraction_method='native_cell_grid')
    assert reconcile_corporate_numeric_views([true_small])[0]==[true_small]
    diagnostic=corporate_numeric_anomalies([true_small])[0]
    assert diagnostic['reasons']==['small_contract_multiplier_review','small_deliverable_quantity_review']
    assert 'deliverable_cash_twd_invalid' not in diagnostic['reasons']
    contradictory=_numeric_peer()|{'deliverable_security_quantity':2.}
    assert 'multiplier_deliverable_quantity_disagree' in corporate_numeric_anomalies([contradictory])[0]['reasons']


def test_position_decimal_ocr_and_grade_are_not_a_two_contract_limit():
    from stockagent.data.tw_futures_margin_preparation import position_grid_candidates
    grid=[['契約代碼','契約名稱','自然人','調整後部位限制級距'],['AAF','甲期貨','2.000','2']]
    assert position_grid_candidates(grid,'股票期貨 單位：契約數')==[]
    grid[1][2]='2,000'
    assert position_grid_candidates(grid,'股票期貨 單位：契約數')[0]['natural_person_limit']==2000


@pytest.mark.parametrize('review',['ocr','native','inspected','unbound_image'])
def test_small_ocr_unit_cannot_publish_an_interval_without_actual_source_review(review):
    from scripts.build_tw_futures_margin_event_candidates import require_reviewed_small_unit_intervals
    row=_numeric_peer(2.)
    if review=='native':row['extraction_method']='native_cell_grid'
    if review in ('inspected','unbound_image'):
        row.update(extraction_method='source_bound_visual_corporate_cells',
                   visual_review_sha256='c'*64 if review=='inspected' else None)
    interval=dict(product='AA1',contract='201509',effective_date='2015-08-06',
        contract_multiplier=2.,source_content_sha256s=['a'*64])
    if review in ('native','inspected'):require_reviewed_small_unit_intervals([row],[interval])
    else:
        with pytest.raises(ValueError,match='requires source review'):
            require_reviewed_small_unit_intervals([row],[interval])


@pytest.mark.parametrize(('literal', 'multiplier'), [('2.080', 2080.), ('2.059.8812', 2059.8812)])
def test_damaged_quantity_is_not_repaired_from_a_multiplier(literal, multiplier):
    result = corporate_deliverable_components(f'調整為{literal}股標的證券', multiplier)
    assert not result['deliverable_components_resolved']
    assert result['deliverable_security_quantity'] == (2.08 if literal == '2.080' else None)


def test_missing_caption_uses_only_single_same_source_adjustment_scope():
    cells = [['契約代號', 'AAF調整為AA1'], ['約定標的物', '調整為2,200股標的證券'],
        ['契約乘數', 'AA1契約乘數調整為2,200']]
    text = '調整生效日：104年8月6日。調整契約月份：104年9月、12月及105年3月到期契約。契約代號AAF調整為AA1'
    fact, = corporate_grid_candidates(cells, text)
    assert fact['contract_months'] == ['201509', '201512', '201603']
    assert fact['contract_months_origin'] == 'same_source_single_adjustment_scope'
    for suffix in ('。契約代號BBF調整為BB1', '。調整契約月份：104年10月到期契約'):
        assert not corporate_grid_candidates(cells, text + suffix)[0]['contract_months']
    assert not corporate_grid_candidates(cells, text.replace('AAF調整為AA1', 'BBF調整為BB1'))[0]['contract_months']


def test_retained_fields_share_only_one_explicit_adjustment_and_keep_conflicts():
    scope = '一、契約調整：調整生效日：104年8月6日調整契約月份：104年9月、12月及105年3月到期契約'
    first = scope + '契約代號AAF調整為AA1買方權益數加項新臺幣5,000元賣方權益數減項新臺幣5,000元二、部位限制'
    second = '一、契約調整：契約代號AAF調整為AA1約定標的物調整為2,200股標的證券契約乘數AA1契約乘數調整為2,200二、部位限制'
    assert not corporate_retained_view_candidates([(first, 'v1')])
    fact, = corporate_retained_view_candidates([(first, 'v1'), (second, 'v2')])
    assert fact['contract_months'] == ['201509', '201512', '201603']
    assert fact['cash_equity_pair_agrees']
    assert fact['equity_credit_long_per_contract'] == 5000
    assert fact['deliverable_components_resolved']
    assert fact['retained_field_views']['契約乘數'] == ['v2']
    assert not corporate_retained_view_candidates([(first, 'v1'), (second.replace('AAF調整為AA1', 'BBF調整為BB1'), 'v2')])
    alternatives = corporate_retained_view_candidates([(first, 'v1'), (second, 'v2'),
        (first.replace('5,000', '6,000'), 'v3')])
    assert {f['equity_credit_long_per_contract'] for f in alternatives if f['cash_equity_pair_agrees']} == {5000, 6000}
    # Preserve a financial conflict rather than selecting a last/better view.
    enriched = [_fact() | f for f in alternatives]
    assert not corporate_terms_intervals(enriched)[0]
    # Fields belonging to a newly listed standard contract are excluded.
    assert not corporate_retained_view_candidates([(first, 'v1'),
        ('一、契約調整：契約代號AAF調整為AA1加掛新契約' + second, 'v2')])


def test_same_bytes_cash_peer_fills_missing_cells_but_never_a_numeric_disagreement():
    complete = _fact(extraction_method='explicit_text_fields_requires_source_review')
    missing = _fact(equity_credit_long_per_contract=None, equity_debit_short_per_contract=None,
                    cash_equity_pair_agrees=False)
    original = deepcopy(missing)
    rows, _ = corporate_terms_intervals([missing, complete])
    assert rows[0]['equity_cash_credit_twd'] == 5000
    assert missing == original
    for change in ({'source_content_sha256': 'b' * 64}, {'from_product': 'BBF'}):
        rows, _ = corporate_terms_intervals([missing, complete | change])
        assert not rows
    conflict = complete | {'equity_credit_long_per_contract': 5001, 'equity_debit_short_per_contract': 5001}
    assert not corporate_terms_intervals([missing, complete, conflict])[0]
    wrong = _fact(equity_credit_long_per_contract=4000, equity_debit_short_per_contract=4000)
    assert not corporate_terms_intervals([wrong, complete])[0]


def test_flat_text_identity_cannot_override_unique_same_bytes_grid_identity():
    original = _fact()
    wrong = _fact(from_product='BBF', extraction_method='explicit_text_fields_requires_source_review')
    rows, issues = corporate_terms_intervals([wrong, original])
    assert rows[0]['from_product'] == 'AAF'
    assert any(i['reasons'] == 'text_identity_outside_same_source_structured_grid' for i in issues)
    _, edges = corporate_identity_boundaries(pl.from_dicts([wrong, original], infer_schema_length=None))
    assert edges['product'].to_list() == ['AAF']
    assert edges['corporate_transfer_target'].to_list() == ['AA1']
    # Another document or conflicting explicit grids still blocks admission.
    assert not corporate_terms_intervals([original, wrong | {'source_content_sha256': 'b' * 64}])[0]
    assert not corporate_terms_intervals([original, wrong | {'extraction_method': 'native_cell_grid'}])[0]


def test_inherited_inspected_rights_survive_reextraction_and_explicit_review_can_replace():
    from scripts.build_tw_futures_margin_event_candidates import replace_reviewed_corporate_facts
    inspected = _fact(extraction='source_bound_visual_corporate_cells',
        subscription_rights_at_final_settlement=True)
    noisy = inspected | {'contract_multiplier': 2., 'deliverable_security_quantity': 2.,
        'extraction': None, 'subscription_rights_at_final_settlement': False}
    other_source = noisy | {'source_content_sha256': 'b' * 64}
    inherited = replace_reviewed_corporate_facts([inspected, noisy, other_source], [inspected])
    assert inherited == [other_source, inspected]
    replacement = inspected | {'contract_multiplier': 2300., 'deliverable_security_quantity': 2300.}
    assert replace_reviewed_corporate_facts(inherited, [replacement]) == [other_source, replacement]
    assert corporate_terms_intervals([inspected])[0][0]['subscription_rights_at_final_settlement']


def _reviewed_code_correction():
    import json
    correction = dict(from_product='ITF', product='KI1', effective_date='2015-08-06',
        contract_months=['201509'], corrected_from_product='KTF', corrected_product='KT1')
    reviewed = _fact(product='KT1', from_product='KTF',
        extraction='source_bound_visual_corporate_cells', visual_review_sha256='c'*64,
        reviewed_code_pair_corrections=json.dumps([correction]))
    return reviewed, correction


def test_both_misdecoded_codes_are_replaced_only_within_the_inspected_source_date_and_months():
    from scripts.build_tw_futures_margin_event_candidates import replace_reviewed_corporate_facts
    reviewed, _ = _reviewed_code_correction()
    wrong = _fact(product='KI1', from_product='ITF', contract_multiplier=2.05)
    others = [wrong | {'source_content_sha256': 'b'*64},
              wrong | {'effective_date': '2016-07-23'},
              wrong | {'contract_months': ['201509', '201512']},
              wrong | {'from_product': 'CTF'}, wrong | {'product': 'OZ1'},
              wrong | {'contract_months': []}]
    fixed = replace_reviewed_corporate_facts([wrong, *others], [reviewed])
    assert fixed == [*others, reviewed]
    assert replace_reviewed_corporate_facts(fixed, [reviewed]) == fixed


@pytest.mark.parametrize('change', [
    {'extraction': 'source_ruled_ocr_cell_grid'}, {'visual_review_sha256': None},
    {'source_content_sha256': 'bad'}, {'issue_date_bound': False},
])
def test_code_pair_retirement_requires_inspected_original_provenance(change):
    from scripts.build_tw_futures_margin_event_candidates import replace_reviewed_corporate_facts
    reviewed, _ = _reviewed_code_correction()
    with pytest.raises(ValueError, match='source-bound inspected cells'):
        replace_reviewed_corporate_facts([], [reviewed | change])


@pytest.mark.parametrize('change', [
    {'corrected_from_product': 'ITF'}, {'corrected_product': 'KI1'},
    {'effective_date': '2016-07-23'}, {'contract_months': ['201508']},
    {'contract_months': []}, {'contract_months': ['201508', '201508']},
    {'contract_months': ['201513']}, {'product': 'KI'}, {'extra': 'unbound_scope'},
])
def test_corrected_code_pair_requires_one_actual_inspected_replacement(change):
    from scripts.build_tw_futures_margin_event_candidates import reviewed_corporate_code_pair_corrections
    reviewed, correction = _reviewed_code_correction()
    with pytest.raises(ValueError):
        reviewed_corporate_code_pair_corrections({'corrected_ocr_code_pairs': [correction | change]}, [reviewed])


def test_incremental_input_identity_ignores_duplicate_views_but_detects_financial_and_clock_changes(tmp_path):
    import json
    from downloader.artifact_io import sha256_file
    from scripts.prepare_tw_futures_margin_training import rule_product_inputs
    files = {'corporate_event_candidates': pl.from_dicts([_fact()], infer_schema_length=None)}
    files.update({name: pl.DataFrame({'product': ['AA1'], 'amount': [2000.],
        'known_at': ['2015-07-22T23:59:59+08:00']}) for name in
        ('corporate_terms_intervals', 'corporate_unit_intervals', 'margin_level_intervals', 'position_level_intervals')})

    def publish():
        for name, frame in files.items():
            frame.write_parquet(tmp_path / (name + '.parquet'))
        (tmp_path / 'manifest.json').write_text(json.dumps({'outputs': {
            name + '.parquet': {'sha256': sha256_file(tmp_path / (name + '.parquet'))} for name in files}}))

    publish()
    baseline = rule_product_inputs(tmp_path)
    files['corporate_event_candidates'] = pl.from_dicts([_fact(),
        _fact(extraction='new_text_view')], infer_schema_length=None)
    publish()
    assert rule_product_inputs(tmp_path) == baseline
    for column, value in [('amount', 2001.), ('known_at', '2015-07-23T23:59:59+08:00')]:
        prior = files['margin_level_intervals']
        files['margin_level_intervals'] = prior.with_columns(pl.lit(value).alias(column))
        publish()
        changed = rule_product_inputs(tmp_path)
        assert changed['AA1']['margin_level_intervals'] != baseline['AA1']['margin_level_intervals']
        assert changed['AA1']['corporate_boundaries'] == baseline['AA1']['corporate_boundaries']
        files['margin_level_intervals'] = prior


def test_late_amendment_never_retroactively_removes_known_history():
    original = _fact()
    late = _fact(source_content_sha256='b' * 64, known_at='2015-08-20T23:59:59+08:00',
                 contract_multiplier=2300., deliverable_security_quantity=2300.)
    before, _ = corporate_terms_intervals([original])
    after, issues = corporate_terms_intervals([late, original])
    assert after[0]['valid_until_exclusive'] == '2015-08-21'
    days = pl.DataFrame({'date': [date(2015, 8, 7), date(2015, 8, 20), date(2015, 8, 21)],
        'product': ['AA1'] * 3, 'contract': ['201509'] * 3})
    schema = {'valid_until_exclusive': pl.String}
    base = bind_dated_corporate_terms(days, pl.from_dicts(before, infer_schema_length=None, schema_overrides=schema))
    changed = bind_dated_corporate_terms(days, pl.from_dicts(after, infer_schema_length=None, schema_overrides=schema))
    assert base.head(2).equals(changed.head(2))
    assert changed['terms_binding_status'][2] == 'terms_ended'
    assert changed['contract_multiplier'][2] is None
    assert any('not_known_before_effective_day' in i['reasons'] for i in issues)
    # A newly disclosed amount is not applied to the earlier conversion.
    assert after[0]['contract_multiplier'] == 2200.


def test_referenced_terminal_formula_revision_keeps_earlier_units_and_cash_only():
    from scripts.build_tw_futures_margin_event_candidates import (
        classify_terminal_formula_revision, replace_reviewed_corporate_facts,
    )
    original = _fact(extraction='source_bound_visual_corporate_cells',
        subscription_rights_at_final_settlement=True)
    revised = original | {'source_content_sha256': 'b' * 64,
        'known_at': '2015-08-20T23:59:59+08:00',
        'supersedes_source_sha256s': ['a' * 64], 'supersession_review_sha256': 'c' * 64}
    classified, = classify_terminal_formula_revision([revised], [original])
    assert classified['event_role'] == 'terminal_formula_revision_only'
    noisy = revised | {'contract_multiplier': 2., 'deliverable_components_resolved': False}
    events = replace_reviewed_corporate_facts([original, noisy], [original, classified])
    assert events == [original]
    before, _ = corporate_terms_intervals([original])
    assert corporate_terms_intervals(events)[0] == before
    assert before[0]['known_at'] == original['known_at']
    assert before[0]['subscription_rights_at_final_settlement']
    for changed in ({'contract_multiplier': 2300.}, {'deliverable_cash_twd': 100.},
            {'equity_credit_long_per_contract': 6000.}, {'from_product': 'BBF'},
            {'contract_months': ['201512']}, {'subscription_rights_at_final_settlement': False},
            {'fixed_subscription_rights_twd': 0.}, {'supersedes_source_sha256s': []},
            {'supersession_review_sha256': None}, {'known_at': original['known_at']}):
        with pytest.raises(ValueError, match='terminal formula revision'):
            classify_terminal_formula_revision([revised | changed], [original])
    with pytest.raises(ValueError, match='terminal formula revision'):
        classify_terminal_formula_revision([revised], [original | {'issue_date_bound': False}])
