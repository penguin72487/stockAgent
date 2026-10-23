"""Synthetic photo credits are nontext, never completed articles or grants."""
from copy import deepcopy
from contextlib import closing
from dataclasses import replace
from pathlib import Path
import json

import pytest

from downloader.news_fulltext import discovery_fingerprint,load_config
from downloader.news_fulltext_policy import NewsError,PolicyRegistry,extract_article
from downloader.news_fulltext_queue import NewsQueue,parser_repair_changed,parser_repair_identity
from test_news_globalvoices_small_body import page,URL,CLOCK,BODY


PHOTO='http://www.flickr.com/photos/synthetic-photographer/12345/'
PROFILE='http://www.flickr.com/photos/synthetic-photographer/'
IMAGE='<img src="https://media.test/synthetic.jpg" alt="Synthetic photo">'
LINK='<a href="'+PHOTO+'">'+IMAGE+'</a>'
CAPTION='“Synthetic photo” by <a href="'+PROFILE+'">Synthetic Photographer</a>'
FORMS=[
    '<p>'+IMAGE+'</p><p>Photo by <a href="https://author.test/">Synthetic Photographer</a></p>',
    '<p>'+IMAGE+'</p><p>Photo by<a href="http://author.test/?category=4"> Synthetic Photographer</a></p>',
    '<div align="center">'+LINK+'</div><p><i>Photograph by <a href="'+PROFILE+'">Synthetic Photographer</a></i></p>',
    '<div align="left">'+LINK+'</div><p><em>Photograph by <a href="'+PROFILE+'">Synthetic Photographer</a></em></p>',
    '<div align="center">'+LINK+'</div><p><i>'+CAPTION+'</i></p>',
    '<div align="center">'+LINK+'</div><p><em>'+CAPTION+'</em></p>',
    '<p>'+LINK+'</p><p><cite>“<a href="'+PHOTO+'">Synthetic photo</a>” by <a href="'+PROFILE+'">Synthetic Photographer</a></cite></p>',
    '<p>'+LINK+'<br><br><span style="font-size: 0.9em; margin-top: 0px;">'
        '<a <p="">“Synthetic photo” by </a><a href="'+PROFILE+'">Synthetic Photographer</a></span></p>',
]


def document(entry):
    return page().replace(('<p>'+BODY+'</p>').encode(),entry.encode())


def source():
    return load_config(Path('configs/news_fulltext.json'))[1].sources['globalvoices']


@pytest.mark.parametrize('parser',['lxml','html.parser'])
@pytest.mark.parametrize('entry',FORMS)
def test_entire_native_photo_credit_is_image_only_not_prose(entry,parser):
    with pytest.raises(NewsError,match='^source_body_image_only$'):
        extract_article(document(entry),replace(source(),html_parser=parser),page_url=URL)


@pytest.mark.parametrize('change',[
    lambda text:text+'<p>A complete source narrative follows.</p>',
    lambda text:'A complete source narrative precedes.'+text,
    lambda text:text.replace('“Synthetic photo” by','An important statement by'),
    lambda text:text.replace(PROFILE,'https://author.test/profile/'),
    lambda text:text.replace('href="'+PROFILE+'"','href="http://www.flickr.com/photos/different-photographer/"'),
    lambda text:text.replace('by <a','by <b>Additional narrative</b><a'),
    lambda text:text.replace('<p><i>','<p hidden><i>'),
    lambda text:text.replace('<img','<img hidden'),
    lambda text:text.replace('<img','<img onerror="active()"'),
    lambda text:text.replace('<p><i>','<p><i style="display:none">'),
    lambda text:text.replace('</i>','</i><!-- unknown native content -->'),
    lambda text:text.replace('</p>','<iframe src="https://embed.test"></iframe></p>'),
    lambda text:text.replace('</p>','<script>active()</script></p>'),
    lambda text:text.replace('<div align="center">','<section>'),
])
def test_no_arbitrary_quote_hidden_credit_narrative_active_resource_or_unknown_shape(change):
    with pytest.raises(NewsError,match='^(?:body_not_found|article_license_not_verified)$'):
        extract_article(document(change(FORMS[4])),source(),page_url=URL)


@pytest.mark.parametrize('old,new',[
    ('<a <p="">','<a unknown="">'),
    ('<br><br>','<br>'),
    ('margin-top: 0px;','margin-top: 1px;'),
    ('“Synthetic photo” by </a>','“Synthetic photo” by complete narrative </a>'),
])
def test_legacy_malformed_shape_is_literal_not_general_sanitization(old,new):
    with pytest.raises(NewsError,match='^body_not_found$'):
        extract_article(document(FORMS[-1].replace(old,new)),source(),page_url=URL)


@pytest.mark.parametrize('old,new',[
    (URL,'https://globalvoices.org/2007/02/13/another/'),
    (CLOCK,'2007-02-13T12:17:00'),
    ('/author/writer/','https://external.test/author/writer/'),
    ('https://creativecommons.org/licenses/by/3.0/','https://unknown.test/license'),
    ('class="entry"','class="entry paywall"'),
])
def test_photo_classification_does_not_exempt_identity_clock_license_or_access(old,new):
    with pytest.raises(NewsError,match='body_not_found|article_license_not_verified|paywall_or_login'):
        extract_article(document(FORMS[4]).replace(old.encode(),new.encode()),source(),page_url=URL)


def test_capture_outcome_is_versioned_without_prose_requeue_http_or_rss_reset(monkeypatch,tmp_path):
    import downloader.news_fulltext_policy as policy
    config,_,repo=load_config(Path('configs/news_fulltext.json'))
    settings=deepcopy(config);settings['providers']=[p for p in config['providers'] if p['id']=='globalvoices']
    settings.pop('provider_catalog',None)
    with monkeypatch.context() as old:
        old.setattr(policy,'GLOBALVOICES_PHOTO_CREDIT_OUTCOME_CONTRACT_VERSION',0)
        previous=PolicyRegistry(settings,base=repo)
    current=PolicyRegistry(settings,base=repo);a,b=previous.sources['globalvoices'],current.sources['globalvoices']
    assert a.data_fingerprint!=b.data_fingerprint and b.parser_repair_prior_capture_fingerprint==a.data_fingerprint
    assert a.parser_repair_fingerprint==b.parser_repair_fingerprint
    assert not parser_repair_changed(b,a.data_fingerprint,json.dumps(parser_repair_identity(a)))
    assert (a.http_capacity_source_fingerprint or a.data_fingerprint)==b.http_capacity_source_fingerprint
    assert a.authorization_fingerprint==b.authorization_fingerprint
    assert a.fulltext_feed_data_fingerprint==b.fulltext_feed_data_fingerprint
    assert discovery_fingerprint(a)==discovery_fingerprint(b)
    with closing(NewsQueue(tmp_path,previous)) as queue:
        queue.add_articles([{'url':URL}])
        with queue.db:
            queue.db.execute("UPDATE articles SET state='body_not_found',error_code='body_not_found',"
                "attempts=1,due=0,last_checked_at_utc='2026-01-01T00:00:00+00:00' WHERE url=?",(URL,))
        before=dict(queue.db.execute('SELECT * FROM articles').fetchone())
    with closing(NewsQueue(tmp_path,current)) as restarted:
        assert dict(restarted.db.execute('SELECT * FROM articles').fetchone())==before
