"""Synthetic different-story and inert-audio boundaries, never publisher prose."""
from contextlib import closing
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_bastillepost_native import settings, page, parse, versioned, URL

AUDIO = ('<iframe allowtransparency="true" attrmap="{opaque=true}" '
    'style="height:150px;border:none" '
    'src="https://www.bastillepost.com/hongkong/article-audio?opaque_key=1"></iframe>')
MARKER = ('<div style="padding:20px"><h3>往下看更多文章</h3>'
    '<a href="/hongkong/article/20202020-%e5%90%88%e6%88%90">'
    '<h3 class="bpcategory-special-color read-more-post">另一個文章標題</h3></a>'
    '<span class="news-large__data" style="display:block">&nbsp;</span></div>')
BANNERS = ('<div class="banner-image desktop-only"><div class="banner-wrapper">'
    '<div class="banner-frame" data-ads="synthetic"></div></div></div>'
    '<div class="banner-image mobile-only"><div class="banner-wrapper"></div></div>')
FOLLOWING_BODY = ('<div class="article-body"><p class="font-size-scaler-control">'
    '這是另一篇文章，不可加入原文，不可當成原文摘要或作者。</p></div>')


def original_body():
    return (AUDIO+'<p class="font-size-scaler-control">'+('原文原始數值 -12.50、0%、股數 0。'*8)+
        '</p><figure><img src="/unfetched.jpg"><figcaption>原文圖說與信用不能截斷。</figcaption></figure>'
        '<table><tr><td>-12.50</td><td>0%</td></tr></table><p>原文最後段仍完整。</p>')


def following_page():
    return page(body=original_body()).replace('<div class="fb-comments">',
        MARKER+BANNERS+FOLLOWING_BODY+'<div class="fb-comments">')


@pytest.mark.parametrize('engine',['lxml','html.parser'])
def test_v7_binds_only_original_across_explicit_different_id_marker(settings,engine,monkeypatch):
    import requests
    config=versioned(settings,7);config['providers'][0]['html_parser']=engine
    registry=PolicyRegistry(config,base=Path.cwd());raw=following_page().encode();calls=[]
    def get(url,**kwargs):
        calls.append(url)
        assert url==URL
        return Response(URL,200,{'content-type':'text/html'},raw)
    monkeypatch.setattr(requests.sessions.Session,'request',lambda *a,**k:pytest.fail('no audio or following-story HTTP'))
    result=fetch_article(SimpleNamespace(registry=registry,get=get),{'url':URL,'id':bytes.fromhex('12'*16)})
    assert calls==[URL]
    assert result['body'].count('原文原始數值 -12.50、0%、股數 0。')==8
    assert '原文圖說與信用不能截斷。' in result['body'] and '原文最後段仍完整。' in result['body']
    assert '-12.50\n0%' in result['body']
    assert not any(text in result['body'] for text in ('這是另一篇文章','另一個文章標題','往下看更多文章'))
    notices=json.loads(result['verified_article_notices_json'])
    boundary,=[n for n in notices if n['kind']=='source_declared_following_story_boundary']
    assert boundary['following_article_id']=='20202020' and boundary['different_literal_article_ID']
    assert boundary['following_body_excluded'] and boundary['following_body_or_author_not_admitted']
    assert not boundary['following_story_requested'] and boundary['not_first_body_or_title_similarity_selection']
    audio,=[n for n in notices if n['kind']=='unfetched_native_audio_player']
    assert audio['src_raw'].endswith('opaque_key=1') and audio['inline_characters']==0
    assert not audio['resource_requested'] and not audio['script_executed'] and not audio['audio_transcription_acquired']
    assert audio['static_native_fallback_text_retained'] and audio['native_audio_not_authorship_proof']
    assert 'following_story_not_joined_to_original' in result['body_completeness']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    assert result['attribution_status']=='source_declared_corporate_author_not_verified_person'


@pytest.mark.parametrize('version',[1,2,3,4,5,6])
def test_old_contracts_do_not_silently_select_from_duplicate_bodies(settings,version):
    with pytest.raises(NewsError,match='^body_not_found$'):
        parse(versioned(settings,version),following_page())
    with pytest.raises(NewsError,match='^article_identity_not_verified$'):
        parse(versioned(settings,version),page(body=original_body()))


@pytest.mark.parametrize('edit',[
    lambda s:s.replace('往下看更多文章','可能還有其他內容'),
    lambda s:s.replace('/article/20202020-','/article/10123210-'),
    lambda s:s.replace('href="/hongkong/article/20202020-','href="https://other.test/hongkong/article/20202020-'),
    lambda s:s.replace('read-more-post','unrecognized-title'),
    lambda s:s.replace('另一個文章標題',''),
    lambda s:s.replace('<div style="padding:20px">','<div hidden style="padding:20px">'),
    lambda s:s.replace('<h3>往下看更多文章','<h3 onclick="run()">往下看更多文章'),
    lambda s:s.replace('<div style="padding:20px">','<div style="padding:20px">不明的原文延續不得丟棄'),
    lambda s:s.replace('>&nbsp;</span>','><script></script></span>'),
    lambda s:s.replace(MARKER,MARKER*2),
    lambda s:s.replace(BANNERS,''),
    lambda s:s.replace('class="banner-frame"','class="banner-frame" onclick="run()"'),
    lambda s:s.replace('data-ads="synthetic"></div>','data-ads="synthetic">不可遺失的文字</div>'),
    lambda s:s.replace('data-ads="synthetic"></div>','data-ads="synthetic"><script></script></div>'),
    lambda s:s.replace(BANNERS,BANNERS+'<p>不明的跨容器文字不可遺失。</p>'),
    lambda s:s.replace(FOLLOWING_BODY,FOLLOWING_BODY*2),
    lambda s:s.replace(FOLLOWING_BODY,'<section>'+FOLLOWING_BODY+'</section>'),
    lambda s:s.replace(FOLLOWING_BODY,FOLLOWING_BODY.replace('class="article-body"','hidden class="article-body"')),
    lambda s:s.replace(MARKER+BANNERS+FOLLOWING_BODY,FOLLOWING_BODY+MARKER+BANNERS),
    lambda s:s.replace('</main>','<div class="article-body"><p>第三個不明正文。</p></div></main>'),
])
@pytest.mark.parametrize('engine',['lxml','html.parser'])
def test_ambiguous_or_hidden_following_boundary_never_becomes_first_body_fallback(settings,engine,edit):
    config=versioned(settings,7);config['providers'][0]['html_parser']=engine
    with pytest.raises(NewsError):
        parse(config,edit(following_page()))


@pytest.mark.parametrize('edit',[
    lambda s:s.replace('https://www.bastillepost.com','http://www.bastillepost.com'),
    lambda s:s.replace('www.bastillepost.com','www.bastillepost.com.evil.test'),
    lambda s:s.replace('www.bastillepost.com','user@www.bastillepost.com'),
    lambda s:s.replace('article-audio','article-video'),
    lambda s:s.replace('?opaque_key=1','#fragment'),
    lambda s:s.replace('?opaque_key=1','?bad=%ZZ'),
    lambda s:s.replace('</iframe>','unknown fallback cannot be dropped</iframe>'),
    lambda s:s.replace('</iframe>','<!-- cannot be dropped --></iframe>'),
    lambda s:s.replace('allowtransparency="true"','allowtransparency="false"'),
    lambda s:s.replace('allowtransparency','onload="run()" allowtransparency'),
    lambda s:s.replace('allowtransparency','srcdoc="unknown" allowtransparency'),
    lambda s:s.replace('allowtransparency','hidden allowtransparency'),
    lambda s:s.replace('height:150px','display:none'),
    lambda s:s.replace('{opaque=true}','x'*513),
    lambda s:s.replace('height:150px;border:none','x'*2049),
    lambda s:'<div>'+s+'</div>',
])
@pytest.mark.parametrize('engine',['lxml','html.parser'])
def test_unknown_nonempty_or_relocated_audio_is_not_stripped(settings,engine,edit):
    config=versioned(settings,7);config['providers'][0]['html_parser']=engine
    with pytest.raises(NewsError,match='^article_identity_not_verified$'):
        parse(config,page(body=original_body().replace(AUDIO,edit(AUDIO))))


def test_inert_audio_alone_needs_no_following_story_and_never_relabels_related_credit(settings):
    config=versioned(settings,7)
    result=parse(config,page(body=original_body()))
    assert not any(n['kind']=='source_declared_following_story_boundary'
        for n in json.loads(result['verified_article_notices_json']))
    assert 'audio_transcription_not_acquired' in result['body_completeness']
    unrelated=following_page().replace('這是另一篇文章','（星島日報報道）這是另一篇文章')
    assert parse(config,unrelated)['body']==result['body']
    with pytest.raises(NewsError,match='^article_rights_or_scope_excluded$'):
        parse(config,following_page().replace('原文原始數值','（星島日報報道）原文原始數值'))
    with pytest.raises(NewsError,match='^paywall_or_login$'):
        parse(config,following_page().replace('allowtransparency','data-paywall="1" allowtransparency'))


def test_v7_preserves_old_text_clocks_author_notices_and_http_authorization_identity(settings,tmp_path):
    for raw in (page(),page(body='<p class="font-size-scaler-control">原生完整短文 0%。</p>')):
        assert parse(versioned(settings,7),raw)==parse(versioned(settings,6),raw)
    old=PolicyRegistry(versioned(settings,6),base=tmp_path).sources['bastillepost']
    new=PolicyRegistry(versioned(settings,7),base=tmp_path).sources['bastillepost']
    assert new.parser_repair_prior_capture_fingerprint==old.data_fingerprint!=new.data_fingerprint
    assert (new.authorization_fingerprint,new.http_capacity_source_fingerprint,new.http_capacity_authorization_fingerprint)==(
        old.authorization_fingerprint,old.http_capacity_source_fingerprint,old.http_capacity_authorization_fingerprint)


@pytest.mark.parametrize('state',['body_not_found','article_identity_not_verified','article_rights_or_scope_excluded',
    'invalid_bulk_publication_clock','unsupported_article_path','robots_disallow','http_403','http_429'])
def test_v7_never_automatically_rearms_prior_heads(settings,tmp_path,state):
    from test_news_reviewed_repairs import failure,head
    with closing(NewsQueue(tmp_path,PolicyRegistry(versioned(settings,6),base=tmp_path))) as queue:
        before=failure(queue,URL,state=state,error_code=state,attempts=19)
    with closing(NewsQueue(tmp_path,PolicyRegistry(versioned(settings,7),base=tmp_path))) as queue:
        queue.reconcile_policy()
        assert head(queue,URL)==before
