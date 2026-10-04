"""A notice's before column cannot authorize unknown earlier years."""
from copy import deepcopy
import json
from types import SimpleNamespace

import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from scripts.repair_tw_futures_margin_source_intervals import (
    before_margin_opening_boundaries, stage_opening_margin_boundary_repair,
)


def inputs(monkeypatch):
    fact = dict(product='CPF', margin_kind='fixed_twd', before=[12000., 10000., 8000.],
        after=[11000., 9000., 8000.], effective_date='2008-06-13',
        effective_phase='after_product_regular_close', issue_date_bound=True,
        published_date='2008-06-12', known_at='2008-06-12T23:59:59+08:00',
        requires_reversion_review=False, source_url='official', source_content_sha256='a'*64,
        extraction='canonical_retained_margin_grid:native_table_tables.json:'+'b'*64)
    native = [deepcopy(fact)]
    monkeypatch.setattr('scripts.build_tw_futures_margin_event_candidates.reextract_retained_margin_text',
                        lambda archive, source_urls: deepcopy(native))
    interval = dict(product='CPF', margin_kind='fixed_twd', initial=11000., maintenance=9000.,
        clearing=8000., effective_date='2008-06-13', effective_phase=1,
        known_at=fact['known_at'], requires_delayed_admission=False,
        source_content_sha256s=['a'*64], source_urls=['official'], point_in_time_verified=False,
        requires_dated_session_and_archive_admission=True,
        valid_until_date_exclusive='2009-03-20', valid_until_phase_exclusive=1,
        temporary_level=False, restoration_applied=False)
    other = dict(interval, product='KF1', effective_date='2017-01-02')
    review = dict(review_kind='source_bound_before_margin_opening_session_v1', cases=[dict(
        product='CPF', source_url='official', content_sha256='a'*64,
        effective_date='2008-06-13', published_date='2008-06-12')])
    archive = SimpleNamespace(document=lambda url: dict(content_sha256='a'*64))
    return pl.DataFrame([fact]), pl.DataFrame([interval, other]), review, archive, native


def test_only_own_opening_boundary_is_added_and_unrelated_levels_survive(monkeypatch):
    facts, levels, review, archive, _ = inputs(monkeypatch)
    after, proofs = before_margin_opening_boundaries(facts, levels, [review], archive=archive)
    extra = after.filter((pl.col('product')=='CPF') & (pl.col('effective_phase')==0)).row(0, named=True)
    assert [extra[f] for f in ('initial','maintenance','clearing')]==[12000.,10000.,8000.]
    assert extra['effective_date']==extra['valid_until_date_exclusive']=='2008-06-13'
    assert extra['valid_until_phase_exclusive']==1
    assert not extra['temporary_level'] and not extra['restoration_applied']
    assert after.filter(pl.col('effective_phase')==1).equals(levels.sort('product','effective_date','effective_phase'))
    assert not after.filter((pl.col('product')=='CPF') & (pl.col('effective_date')<'2008-06-13')).height
    assert not proofs[0]['older_dates_admitted']
    again, _ = before_margin_opening_boundaries(facts, after, [review], archive=archive)
    assert again.equals(after)


@pytest.mark.parametrize('problem', ['other_product', 'other_day', 'source_sha', 'publication',
    'same_day', 'timezone', 'native_before_disagrees', 'native_rate', 'native_bad_hierarchy',
    'native_ocr', 'named_restoration', 'closing_values', 'closing_source', 'known_level_conflict'])
def test_unknown_or_conflicting_inputs_cannot_admit_a_margin(monkeypatch, problem):
    facts, levels, review, archive, native = inputs(monkeypatch)
    case = review['cases'][0]
    if problem=='other_product': case['product']='CZF'
    elif problem=='other_day': case['effective_date']='2008-06-12'
    elif problem=='source_sha': case['content_sha256']='c'*64
    elif problem=='publication': case['published_date']='2008-06-11'
    elif problem=='same_day': native[0]['known_at']='2008-06-13T00:00:00+08:00'
    elif problem=='timezone': native[0]['known_at']='2008-06-12T00:00:00'
    elif problem=='native_before_disagrees': native[0]['before'][0]=13000.
    elif problem=='native_rate': native[0]['margin_kind']='notional_rate'
    elif problem=='native_bad_hierarchy': native[0]['before'][1]=14000.
    elif problem=='native_ocr': native[0]['extraction']='canonical_retained_margin_grid:ocr_table_tables.json:sha'
    elif problem=='named_restoration': native[0]['before_column_semantics']='explicit_non_disposed_base'
    elif problem=='closing_values': levels=levels.with_columns(pl.when(pl.col('product')=='CPF').then(15000.).otherwise(pl.col('initial')).alias('initial'))
    elif problem=='closing_source': levels=levels.with_columns(pl.lit(['c'*64]).alias('source_content_sha256s'))
    elif problem=='known_level_conflict':
        old=dict(levels.row(0,named=True),effective_date='2004-05-31',initial=17000.,maintenance=13000.,clearing=11000.)
        levels=pl.concat([levels,pl.DataFrame([old],schema=levels.schema)])
    with pytest.raises(ValueError):
        before_margin_opening_boundaries(facts, levels, [review], archive=archive)


def test_known_matching_interval_is_retained_without_truncation(monkeypatch):
    facts, levels, review, archive, _=inputs(monkeypatch)
    old=dict(levels.row(0,named=True),effective_date='2008-06-02',initial=12000.,maintenance=10000.,
        clearing=8000.,valid_until_date_exclusive='2008-06-13',valid_until_phase_exclusive=1)
    levels=pl.concat([levels,pl.DataFrame([old],schema=levels.schema)])
    result, proofs=before_margin_opening_boundaries(facts, levels, [review], archive=archive)
    assert result.equals(levels.sort('product','effective_date','effective_phase'))
    assert proofs[0]['already_covered']


def test_staging_keeps_financial_facts_and_existing_source_overlays(tmp_path,monkeypatch):
    facts, levels, review, archive, _=inputs(monkeypatch)
    raw=tmp_path/'raw';pending=tmp_path/'pending'
    (raw/'rules').mkdir(parents=True);pending.mkdir()
    (raw/'source_manifest.json').write_text('{}')
    (raw/'rules/manifest.json').write_text(json.dumps(dict(sources=[])))
    outputs={}
    for name,frame in [('margin_event_candidates.parquet',facts),('margin_level_intervals.parquet',levels),
        ('corporate_event_candidates.parquet',pl.DataFrame(dict(product=['ABC'])) )]:
        frame.write_parquet(pending/name);outputs[name]=dict(sha256=sha256_file(pending/name))
    terminal=dict(path='terminal',sha256='c'*64,products=['ES1'])
    information=dict(path='information_halt',sha256='d'*64,products=['DFF'])
    parent=dict(status='pending_source_bound_rule_delta',outputs=outputs,sources=[],changed_products=[],
        parent_source_manifest_sha256=sha256_file(raw/'source_manifest.json'),
        terminal_source_delta=terminal,information_halt_source_delta=information)
    parent_path=pending/'manifest.json';parent_path.write_text(json.dumps(parent))
    review.update(parent_pending_manifest_sha256=sha256_file(parent_path),
        parent_source_manifest_sha256=parent['parent_source_manifest_sha256'],
        margin_facts_sha256=outputs['margin_event_candidates.parquet']['sha256'],
        margin_levels_sha256=outputs['margin_level_intervals.parquet']['sha256'])
    review_path=tmp_path/'review.json';review_path.write_text(json.dumps(review))
    archive.sources={};archive.conn=SimpleNamespace(close=lambda:None)
    monkeypatch.setattr('stockagent.data.tw_futures_margin_preparation.RuleArchive',lambda *a:archive)
    receipt=tmp_path/'receipt.json'
    proof=stage_opening_margin_boundary_repair(raw,pending,review_path,receipt,archive_root=tmp_path)
    final=json.loads(parent_path.read_text())
    assert final['terminal_source_delta']==terminal and final['information_halt_source_delta']==information
    for name in ['margin_event_candidates.parquet','corporate_event_candidates.parquet']:
        assert sha256_file(pending/name)==outputs[name]['sha256']
    assert final['outputs']['margin_level_intervals.parquet']['sha256']==sha256_file(pending/'margin_level_intervals.parquet')
    assert proof['boundaries'][0]['older_dates_admitted'] is False
    assert final['changed_products']==['CPF']
    with pytest.raises((ValueError, FileExistsError)):
        stage_opening_margin_boundary_repair(raw,pending,review_path,receipt,archive_root=tmp_path)
