"""Issuing a notice and publishing it must not share an invented clock."""
import json
import sqlite3
from types import SimpleNamespace

import pytest

from scripts.build_tw_futures_margin_event_candidates import corporate_source_review_candidates


@pytest.mark.parametrize('problem', [None,'after_publication','wrong_literal_date','wrong_index_date','unreviewed_difference'])
def test_own_corporate_issue_date_keeps_the_later_public_clock(tmp_path, problem):
    db=sqlite3.connect(':memory:')
    db.row_factory=sqlite3.Row
    db.execute('create table announcements(url text,published_date text)')
    db.execute('insert into announcements values (?,?)',('official','2016-08-12'))
    archive=SimpleNamespace(conn=db,document=lambda url:dict(content_sha256='a'*64),copy=lambda *a,**k:None)
    tables=[dict(caption='一、契約調整',cells=[['調整生效日','105年8月19日'],
        ['調整契約月份','105年9月、10月、12月、106年3月及6月到期契約'],
        ['契約代號','KIF調整為KI1'],['約定標的物','調整為2,040股標的證券'],
        ['契約乘數','KI1契約乘數調整為2,040'],
        ['買方權益數加項','每口買方未沖銷部位調整買方權益數加項新臺幣4,000元'],
        ['賣方權益數減項','每口賣方未沖銷部位調整賣方權益數減項新臺幣4,000元']])]
    review=dict(review_kind='source_bound_visual_corporate_cells',source_url='official',
        content_sha256='a'*64,published_date='2016-08-12',issued_date='2016-08-10',
        visually_reviewed_pages=[1,2],pages=[dict(page=1,native_text='發文日期：中華民國105年8月10日',tables=[]),
        dict(page=2,native_text='附件 契約調整',tables=tables)])
    if problem=='after_publication':review['issued_date']='2016-08-13'
    if problem=='wrong_literal_date':review['issued_date']='2016-08-11'
    if problem=='wrong_index_date':review['published_date']='2016-08-10'
    if problem=='unreviewed_difference':review.pop('issued_date')
    path=tmp_path/'review.json'
    path.write_text(json.dumps(review))
    if problem and problem!='unreviewed_difference':
        with pytest.raises(ValueError):corporate_source_review_candidates(archive,path)
    else:
        facts,_,_=corporate_source_review_candidates(archive,path)
        assert len(facts)==1
        row=facts[0]
        if problem=='unreviewed_difference':
            assert 'issued_date' not in row
        else:
            assert row['issued_date']=='2016-08-10'
        assert row['published_date']=='2016-08-12'
        assert row['known_at']=='2016-08-12T23:59:59+08:00'
        assert row['contract_multiplier']==2040
        assert row['equity_credit_long_per_contract']==4000
