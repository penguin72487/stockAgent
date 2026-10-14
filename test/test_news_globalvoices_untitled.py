"""Untitled source-authored notes are not missing authors or invented titles."""
from dataclasses import replace
from contextlib import closing
from pathlib import Path

import pytest

from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import NewsError, extract_article


URL = 'https://globalvoices.org/2010/09/24/164959/'
CLOCK = '2010-09-24T21:02:07+0000'
BODY = 'This synthetic source-authored note preserves the full original prose, exact source clock, and visible attribution without fabricating a news headline.'


def source():
    return load_config(Path('configs/news_fulltext.json'))[1].sources['globalvoices']


def page(body=BODY):
    return ('<html><head><link rel="canonical" href="'+URL+'">'
        '<meta property="article:published_time" content="'+CLOCK+'"></head><body>'
        '<div class="post-container"><div class="post post-164959 type-post status-publish">'
        '<div class="post-header"><h2 class="post-title"></h2>'
        '<div id="post-header-credit"><a class="user-link" href="/author/writer/"></a>'
        '<a class="user-link" href="/author/writer/">Writer</a></div></div>'
        '<div class="entry"><p>'+body+'</p></div>'
        '<div class="postfooter-credits post-credit-container"><span class="contributor-name">'
        '<a class="user-link" href="/author/writer/">Writer</a></span>'
        '<a rel="license" href="https://creativecommons.org/licenses/by/3.0/">CC</a></div>'
        '</div></div></body></html>').encode()


def test_verified_untitled_note_keeps_empty_title_original_prose_author_and_clock():
    result=extract_article(page(),source(),page_url=URL)
    assert result['body']==BODY and result['title']==''
    assert result['title_basis']=='source_visible_title_empty_verified_numeric_article'
    assert result['authors_json']=='["Writer"]' and result['attribution_status']=='source_byline_present'
    assert result['published_at_raw']==CLOCK and result['per_article_license_verified']
    assert 'matching_header_footer_authors_clock_and_article_license' in result['article_identity_basis']
    assert 'source_visible_title_empty' in result['body_completeness']


@pytest.mark.parametrize('body',['A complete native source update.','Confirmed.'])
def test_verified_untitled_short_note_keeps_source_text_without_word_count_padding(body):
    result=extract_article(page(body),source(),page_url=URL)
    assert result['body']==body and result['title']=='' and result['authors_json']=='["Writer"]'
    assert result['body_completeness'].startswith('verified_publisher_short_body;')


@pytest.mark.parametrize('change',[
    'wrong_canonical','multiple_canonical','no_request_url','nonnumeric_url','wrong_post_id',
    'no_post_id','multiple_post_id','no_title_element','multiple_title_elements','nonempty_title',
    'hidden_title','missing_header_credit','different_footer_author','foreign_author_profile',
    'unaware_clock','bad_clock','multiple_clocks','missing_license','site_footer_license_only',
    'multiple_entry','hidden_prose','embedded_only','previous_report_only','other_provider',
])
def test_untitled_exception_requires_one_real_source_bound_article(change):
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_publishers import globalvoices_verified_untitled_body
    raw,policy,url=page(),source(),URL
    if change=='wrong_canonical': raw=raw.replace(URL.encode(),b'https://globalvoices.org/2010/09/24/164881/')
    elif change=='multiple_canonical': raw=raw.replace(b'</head>',('<link rel="canonical" href="'+URL+'"></head>').encode())
    elif change=='no_request_url': url=None
    elif change=='nonnumeric_url': url=URL.replace('164959','synthetic-article');raw=raw.replace(URL.encode(),url.encode())
    elif change=='wrong_post_id': raw=raw.replace(b'post-164959',b'post-164881')
    elif change=='no_post_id': raw=raw.replace(b'post-164959',b'post-no-id')
    elif change=='multiple_post_id': raw=raw.replace(b'<div class="entry">',b'<div class="post-164959"></div><div class="entry">')
    elif change=='no_title_element': raw=raw.replace(b'<h2 class="post-title"></h2>',b'')
    elif change=='multiple_title_elements': raw=raw.replace(b'<h2 class="post-title"></h2>',b'<h2 class="post-title"></h2>'*2)
    elif change=='nonempty_title': raw=raw.replace(b'<h2 class="post-title"></h2>',b'<h2 class="post-title">Existing real title</h2>')
    elif change=='hidden_title': raw=raw.replace(b'<h2 class="post-title">',b'<h2 hidden class="post-title">')
    elif change=='missing_header_credit': raw=raw.replace(b'id="post-header-credit"',b'id="unrelated-credit"')
    elif change=='different_footer_author': raw=raw.replace(b'Writer</a></span>',b'Different</a></span>')
    elif change=='foreign_author_profile': raw=raw.replace(b'/author/writer/',b'https://foreign.test/author/writer/')
    elif change=='unaware_clock': raw=raw.replace(CLOCK.encode(),b'2010-09-24T21:02:07')
    elif change=='bad_clock': raw=raw.replace(CLOCK.encode(),b'unknown')
    elif change=='multiple_clocks': raw=raw.replace(b'</head>',('<meta property="article:published_time" content="'+CLOCK+'"></head>').encode())
    elif change=='missing_license': raw=raw.replace(b'rel="license"',b'rel="external"')
    elif change=='site_footer_license_only': raw=raw.replace(b'post-credit-container',b'footer-site-notice')
    elif change=='multiple_entry': raw=raw.replace(b'<div class="entry">',b'<div class="entry">Other story</div><div class="entry">')
    elif change=='hidden_prose': raw=raw.replace(b'<p>',b'<p hidden>')
    elif change=='embedded_only': raw=raw.replace(('<p>'+BODY+'</p>').encode(),b'<iframe src="https://foreign.test/video"></iframe>')
    elif change=='previous_report_only': raw=raw.replace(BODY.encode(),b"More information see GV's previous report.")
    elif change=='other_provider': policy=replace(policy,provider='unrelated')
    soup=BeautifulSoup(raw,'lxml')
    assert globalvoices_verified_untitled_body(soup,policy,BODY,page_url=url) is None


def test_missing_title_and_missing_author_are_distinct_not_misleading_same_error():
    raw=page().replace(b'post-164959',b'post-unverified')
    with pytest.raises(NewsError,match='^article_title_not_verified$'):
        extract_article(raw,source(),page_url=URL)
    raw=page().replace(b'<h2 class="post-title"></h2>',b'<h2 class="post-title">Real headline</h2>')
    raw=raw.replace(b'id="post-header-credit"',b'id="other-credit"')
    with pytest.raises(NewsError,match='^article_attribution_not_verified$'):
        extract_article(raw,source(),page_url=URL)


def test_untitled_html_capture_version_never_invalidates_healthy_feed_contracts(monkeypatch):
    from downloader import news_fulltext_policy as policy
    new=load_config(Path('configs/news_fulltext.json'))[1].sources
    monkeypatch.setattr(policy,'_GLOBALVOICES_HTML_BODY_CONTRACT_VERSION',6)
    old=load_config(Path('configs/news_fulltext.json'))[1].sources
    assert {key for key in new if new[key].data_fingerprint!=old[key].data_fingerprint}=={'globalvoices'}
    assert all(new[key].authorization_fingerprint==old[key].authorization_fingerprint for key in new)
    assert new['globalvoices'].fulltext_feed_data_fingerprint==old['globalvoices'].fulltext_feed_data_fingerprint


def test_versioned_repair_requeues_only_missing_parser_failures_not_complete_or_refused_work(tmp_path,monkeypatch):
    from downloader import news_fulltext_policy as policy
    from downloader.news_fulltext_queue import NewsQueue
    monkeypatch.setattr(policy,'_GLOBALVOICES_HTML_BODY_CONTRACT_VERSION',6)
    old=load_config(Path('configs/news_fulltext.json'))[1]
    with closing(NewsQueue(tmp_path,old)) as queue:
        urls={state:URL+'?case='+state for state in ('article_attribution_not_verified','article_title_not_verified',
            'complete','http_403','http_429','paywall_or_login','source_body_removed','retry_cooldown')}
        with queue.db:
            queue.add_articles([{'url':url} for url in urls.values()])
            for state,url in urls.items():
                queue.db.execute('UPDATE articles SET state=?,error_code=?,due=12345678900,attempts=1 WHERE url=?',
                                 (state,None if state=='complete' else state,url))
        before={state:dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(url,)).fetchone())
                for state,url in urls.items()}
    monkeypatch.setattr(policy,'_GLOBALVOICES_HTML_BODY_CONTRACT_VERSION',7)
    new=load_config(Path('configs/news_fulltext.json'))[1]
    with closing(NewsQueue(tmp_path,new)) as queue:
        for state,url in urls.items():
            row=dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(url,)).fetchone())
            if state in {'article_attribution_not_verified','article_title_not_verified'}:
                assert row['state']=='pending' and row['due']==0 and row['attempts']==0
                assert row['error_code']==state
            else:
                assert row==before[state]
