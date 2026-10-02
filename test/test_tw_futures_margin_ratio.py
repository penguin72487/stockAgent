from datetime import date

import polars as pl
import pytest

from stockagent.data.tw_futures_margin_preparation import (
    align_product_margin_intervals, compose_margin_ratio_intervals,
)


def interval(product, start, stop, values, *, known='2014-01-01T23:59:59+08:00'):
    return dict(product=product, margin_kind='fixed_twd', initial=values[0],
        maintenance=values[1], clearing=values[2], effective_date=start,
        effective_phase=1, valid_until_date_exclusive=stop,
        valid_until_phase_exclusive=1 if stop else None, known_at=known,
        requires_delayed_admission=False, source_content_sha256s=['a'*64],
        source_urls=['parent'], temporary_level=False, restoration_applied=False,
        point_in_time_verified=False, requires_dated_session_and_archive_admission=True)


def law(**changes):
    return dict(dict(product='MTX', base_product='TX', ratio='1/4', rounding='exact',
        fields=['initial','maintenance','clearing'], effective_date='2014-01-09',
        effective_phase=0, valid_until_date_exclusive=None,
        valid_until_phase_exclusive=None, known_at='2014-01-08T23:59:59+08:00',
        source_content_sha256s=['b'*64], source_urls=['law']), **changes)


def test_ratio_law_keeps_parent_gaps_target_expiry_and_phase_ownership():
    old=interval('MTX','2014-01-05','2014-01-08',(21000.,16000.,15000.))
    parent1=interval('TX','2014-01-10','2014-01-21',(92000.,71000.,68000.))
    parent2=interval('TX','2014-01-23',None,(83000.,64000.,61000.))
    rows,issues=compose_margin_ratio_intervals([old,parent1,parent2],[law()])
    assert not issues
    days=pl.DataFrame(dict(product=['MTX']*4,
        date=[date(2014,1,9),date(2014,1,10),date(2014,1,22),date(2014,1,24)]))
    bound=align_product_margin_intervals(days,pl.DataFrame(rows))
    assert bound['opening_initial'].to_list()==[None,None,None,20750.]
    assert bound['settlement_initial'].to_list()==[None,23000.,None,20750.]
    target=[r for r in rows if r['product']=='MTX' and r['effective_date']=='2014-01-10'][0]
    assert target['maintenance']==17750. and target['clearing']==17000.
    assert target['source_content_sha256s']==['a'*64,'b'*64]


def test_ratio_law_does_not_override_direct_conflicting_level():
    parent=interval('TX','2014-01-10',None,(92000.,71000.,68000.))
    direct=interval('MTX','2014-01-15','2014-01-20',(24000.,18500.,17750.))
    rows,issues=compose_margin_ratio_intervals([parent,direct],[law()])
    assert len(issues)==1 and issues[0]['reasons']=='direct_margin_and_dated_ratio_disagree'
    bound=align_product_margin_intervals(pl.DataFrame(dict(product=['MTX']*2,
        date=[date(2014,1,16),date(2014,1,21)])),pl.DataFrame(rows))
    assert bound['opening_initial'].to_list()==[None,23000.]


def test_newer_parent_supersedes_matching_carried_amount_without_reviving_after_gap():
    old_parent=interval('TX','2014-01-10','2014-02-10',(83000.,64000.,61000.))
    direct=interval('MTX','2014-01-10',None,(20750.,16000.,15250.))
    new_parent=interval('TX','2014-02-10','2014-02-20',(92000.,71000.,68000.),
                        known='2014-02-09T23:59:59+08:00')
    rows,issues=compose_margin_ratio_intervals([old_parent,direct,new_parent],[law()])
    assert not issues
    bound=align_product_margin_intervals(pl.DataFrame(dict(product=['MTX']*3,
        date=[date(2014,2,10),date(2014,2,11),date(2014,2,21)])),pl.DataFrame(rows))
    assert bound['opening_initial'].to_list()==[20750.,23000.,None]
    assert bound['settlement_initial'].to_list()==[23000.,23000.,None]
    old=next(r for r in rows if r['product']=='MTX' and r['effective_date']=='2014-01-10')
    assert old['valid_until_date_exclusive']=='2014-02-10'
    assert old['superseded_by_dated_ratio_source_sha256s']==['a'*64,'b'*64]
    # A different initial target or a parent not independently established
    # at the target boundary cannot be relabelled as an old carried amount.
    for changed in (dict(direct,initial=22000.), dict(direct,effective_date='2014-01-09')):
        _,issues=compose_margin_ratio_intervals([old_parent,changed,new_parent],[law()])
        assert any(r['reasons']=='direct_margin_and_dated_ratio_disagree' for r in issues)


def test_ratio_stops_at_law_expiry_and_respects_actual_knowledge_time():
    parent=interval('TX','2014-01-10',None,(92000.,71000.,68000.),
                    known='2014-01-12T01:00:00+00:00')
    rows,issues=compose_margin_ratio_intervals([parent],[law(
        valid_until_date_exclusive='2014-01-20',valid_until_phase_exclusive=0)])
    assert not issues
    target=next(r for r in rows if r['product']=='MTX')
    assert target['known_at']=='2014-01-12T01:00:00+00:00'
    bound=align_product_margin_intervals(pl.DataFrame(dict(product=['MTX']*3,
        date=[date(2014,1,11),date(2014,1,13),date(2014,1,20)])),pl.DataFrame(rows))
    assert bound['opening_initial'].to_list()==[None,23000.,None]


@pytest.mark.parametrize('change',[dict(ratio='1'),dict(rounding='ceil_1000'),
    dict(fields=['clearing']),dict(source_content_sha256s=[]),
    dict(known_at='2014-01-08T23:59:59'),dict(effective_phase=2)])
def test_incomplete_or_unsupported_ratio_proof_is_rejected(change):
    with pytest.raises(ValueError):
        compose_margin_ratio_intervals([interval('TX','2014-01-10',None,
            (92000.,71000.,68000.))],[law(**change)])
