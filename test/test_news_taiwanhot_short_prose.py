"""Verified complete native short prose uses shared extraction, not a body waiver."""
from contextlib import closing
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader.news_fulltext_policy import NewsError,PolicyRegistry,extract_article
from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_public_originals import TAIWANHOT_V3_SHORT_PROSE_REPAIR_IDENTITY
from downloader.news_fulltext_queue import NewsQueue
from test_news_taiwanhot_native import URL,config,page


TEXT='公司公布六月營收120億元，月增2.23%、年增12.75%；前六個月累計營收700億元，年增34.54%。'


def registry(enabled=True,parser='lxml'):
    cfg=config()
    source=cfg['providers'][0]
    source.update(embedded_article_layout='taiwanhot_visible_article_v3',html_parser=parser,
        article_path_pattern='^(?:/news/(?:focus/)?[1-9][0-9]*(?:/|$)|/$)')
    if enabled:
        source['allow_short_configured_body']=True
    return PolicyRegistry(cfg,base=Path.cwd())


@pytest.mark.parametrize('parser',['html.parser','lxml'])
def test_complete_native_short_post_preserves_exact_text_numbers_byline_and_clock(parser):
    assert 0<len(TEXT)<100
    raw=page(body='<p><img src="/public-chart.png" alt=""></p><p>'+TEXT+'</p><p><img src="/public-chart2.png" alt=""></p>')
    source=registry(parser=parser).sources['taiwanhot']
    result=extract_article(raw,source,page_url=URL)
    assert result['body']=='原始小標\n'+TEXT and result['source_native_article_id']=='907503'
    assert result['published_at_raw']=='2026-10-14 15:30'
    assert json.loads(result['authors_json'])==['理財周刊／新聞中心']
    assert result['body_completeness']=='verified_current_article_container'
    committed=fetch_article(SimpleNamespace(registry=registry(parser=parser),
        get=lambda *_a,**_k:Response(URL,200,{'content-type':'text/html'},raw)),{'url':URL,'id':b'x'*32})
    assert not committed['publish'] and not committed['remote_training_export'] and not result['historical_point_in_time']
    with pytest.raises(NewsError,match='body_not_found'):
        extract_article(raw,registry(False,parser).sources['taiwanhot'],page_url=URL)


@pytest.mark.parametrize('parser',['html.parser','lxml'])
@pytest.mark.parametrize('body',['','<img src="/chart.png" alt="這不是正文">','one short word',
    '<figure><img src="/chart.png"><figcaption><p>圖說不能被標成完整新聞正文。</p></figcaption></figure>',
    '<div class="photo_wrap"><p>排除的圖說不能補成缺少的正文。</p></div>'])
def test_empty_image_only_or_trivial_latin_text_never_becomes_a_short_body(parser,body):
    with pytest.raises(NewsError,match='body_not_found'):
        extract_article(page(body=body),registry(parser=parser).sources['taiwanhot'],page_url=URL)


@pytest.mark.parametrize('options,error',[
    ({'og_url':URL.replace('907503','42')},'article_identity_not_verified'),
    ({'clock':'2026-10-14'},'invalid_bulk_publication_clock'),
    ({'author':''},'article_attribution_not_verified'),
    ({'extra_body':'<div class="paywall">登入</div>'},'paywall_or_login'),
    ({'body':'本文禁止轉載。'},'article_rights_or_scope_excluded'),
])
def test_short_prose_does_not_waive_native_identity_actual_clock_author_paid_or_rights(options,error):
    raw=page(**{'body':TEXT,**options})
    with pytest.raises(NewsError,match=error):
        extract_article(raw,registry().sources['taiwanhot'],page_url=URL)


def pin(queue,capture=None,parser=None):
    original,original_parser=TAIWANHOT_V3_SHORT_PROSE_REPAIR_IDENTITY
    original,original_parser=capture or original,parser or original_parser
    with queue.db:
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?',(original,'source_capture_fingerprint:taiwanhot'))
        queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?',(json.dumps({
            'contract_version':1,'data_fingerprint':original,'parser_fingerprint':original_parser}),
            'source_parser_repair_fingerprint:taiwanhot'))


def test_pinned_short_body_only_migration_keeps_old_attempts_due_errors_heads_leases_and_budgets(tmp_path):
    with closing(NewsQueue(tmp_path,registry(False))) as queue:
        cases=[('body_not_found',None,None),('article_identity_not_verified',None,None),
            ('body_not_found','old-body',None),('body_not_found',None,'worker'),
            ('http_403',None,None),('http_429',None,None),('robots_disallowed',None,None)]
        for i,(state,head,lease) in enumerate(cases):
            url=URL+'?local_case='+str(i)
            queue.add_articles([{'url':url}])
            with queue.db:
                queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=5,due=2000000000,'
                    'last_checked_at_utc=?,body_sha256=?,lease_owner=?,lease_until=? WHERE url=?',
                    (state,state,'2026-10-14T16:01:44+00:00',head,lease,2000001000 if lease else None,url))
        with queue.db:
            queue.db.execute('INSERT INTO queue_settings VALUES (?,?)',('failure_identity_metadata_attempts:v1:taiwanhot:20261014','2'))
        before={r['url']:dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        pin(queue)
        queue.registry=registry()
        queue.reconcile_policy()
        after={r['url']:dict(r) for r in queue.db.execute('SELECT * FROM articles')}
        target=URL+'?local_case=0'
        for url,row in before.items():
            assert after[url]==({**row,'state':'retry'} if url==target else row)
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key='failure_identity_metadata_attempts:v1:taiwanhot:20261014'").fetchone()[0]=='2'
        records=queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'taiwanhot_complete_short_prose_repair_v3:%'").fetchall()
        assert len(records)==1 and json.loads(records[0][0])['rows_moved']==1
        queue.reconcile_policy()
        assert after=={r['url']:dict(r) for r in queue.db.execute('SELECT * FROM articles')}


@pytest.mark.parametrize('capture,parser,enabled',[('unknown',None,True),(None,'unknown',True),(None,None,False)])
def test_unknown_previous_capture_parser_or_missing_short_body_optin_never_requeues(tmp_path,capture,parser,enabled):
    with closing(NewsQueue(tmp_path,registry(False))) as queue:
        queue.add_articles([{'url':URL}])
        with queue.db:
            queue.db.execute("UPDATE articles SET state='body_not_found',error_code='body_not_found',attempts=5,due=2000000000")
        before=dict(queue.db.execute('SELECT * FROM articles').fetchone())
        pin(queue,capture,parser)
        queue.registry=registry(enabled)
        queue.reconcile_policy()
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone())==before
        assert not queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE 'taiwanhot_complete_short_prose_repair_v3:%'").fetchone()
