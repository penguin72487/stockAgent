"""Synthetic CMS blocks, never a media request or a grant of invisible prose."""
from contextlib import closing
from pathlib import Path
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import PolicyRegistry, NewsError, extract_article
from downloader.news_fulltext_queue import NewsQueue, pocket_native_parser_repair, utc_now
from test_news_pocket_original import settings, page, versioned, URL, POCKET_ORIGIN


def parse(config, raw=None):
    source=PolicyRegistry(config,base=Path.cwd()).sources['pocket']
    return extract_article((page()if raw is None else raw).encode(),source,page_url=URL)


def flat(*,script=True,embed=True):
    soup=BeautifulSoup(page(nested=False),'lxml')
    root=soup.select_one('.cms-article-scope')
    style=soup.new_tag('style');style.string='table { color: black; }';root.insert(0,style)
    article=soup.new_tag('article',attrs={'dir':'auto','data-scroll-anchor':'false'})
    h=soup.new_tag('h2');h.string='保留原生尾端段落';article.append(h);root.append(article)
    caption=soup.new_tag('caption',attrs={'style':'display:none;'});caption.string='原生表格標題'
    root.select_one('table').insert(0,caption)
    cta=soup.new_tag('h2',attrs={'id':'section-cta'});cta.string='怎麼買 合成股票？'
    broker=BeautifulSoup('<div><p>口袋證券廣告不是研究正文</p><a href="https://pse.im/78wp56">開戶連結</a></div>','lxml').div
    if embed:
        iframe=soup.new_tag('iframe',attrs={'src':'https://www.youtube.com/embed/AbCdEfG1234?si=synthetic',
            'title':'YouTube video player','width':'560','height':'315','frameborder':'0',
            'allow':'accelerometer; autoplay; encrypted-media; picture-in-picture',
            'allowfullscreen':'','referrerpolicy':'strict-origin-when-cross-origin'})
        root.append(iframe)
    root.append(cta);root.append(broker)
    if script:
        script=soup.new_tag('script',attrs={'type':'application/ld+json'})
        script.string=json.dumps([{'@context':'https://schema.org','@type':'FAQPage','mainEntity':[
            {'@type':'Question','name':'合成 FAQ？','acceptedAnswer':{'@type':'Answer','text':'不可再放一份的 JSON 答案'}}]}],ensure_ascii=False)
        root.append(script)
    return str(soup)


@pytest.mark.parametrize('engine',['lxml','html.parser'])
def test_native_blocks_preserve_prose_table_and_embedded_article_not_just_the_inner_block(settings,engine):
    config=versioned(settings,4);config['providers'][0]['html_parser']=engine
    raw=flat();result=parse(config,raw);text=result['body']
    for i in range(4):assert text.count(f'第{i}段')==1
    for word in ('-12.50','原始欄','比率','合成 FAQ 正文','保留原生尾端段落','原生表格標題'):
        assert word in text
    for word in ('color: black','JSON 答案','口袋證券廣告','開戶連結','YouTube video player'):
        assert word not in text
    notices=json.loads(result['verified_article_notices_json'])
    assert sum(n['kind']=='native_nonvisible_plain_table_caption_retained_as_context'for n in notices)==1
    caption=next(n for n in notices if n['kind'].startswith('native_nonvisible'))
    assert not caption['visible_prose'] and caption['style_raw']=='display:none;'
    media=next(n for n in notices if n['kind'].startswith('unfetched_native_YouTube'))
    assert media['source_url_raw'].endswith('?si=synthetic') and not media['media_requested']
    assert not media['transcript_requested'] and not media['media_or_transcript_text_inferred']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    assert 'embedded_YouTube_reference_unfetched_not_transcribed'in result['body_completeness']
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        parse(versioned(settings,3),raw)


@pytest.mark.parametrize('nested',[True,False])
def test_v4_leaves_previously_accepted_output_exactly_equal(settings,nested):
    raw=page(nested=nested)
    assert parse(versioned(settings,4),raw)==parse(versioned(settings,3),raw)


@pytest.mark.parametrize('engine',['lxml','html.parser'])
def test_v5_distinct_native_body_heading_is_preserved_not_an_identity_replacement(settings,engine):
    soup=BeautifulSoup(page(),'lxml')
    native=soup.select_one('.cms-article-scope > article.pocket-article')
    heading=soup.new_tag('h1');heading.string='不同的正文標題：保留原始值 -12.50 與 0'
    native.insert(0,heading)
    raw=str(soup);config=versioned(settings,5);config['providers'][0]['html_parser']=engine
    result=parse(config,raw)
    assert result['body'].count(heading.get_text())==1
    assert result['body'].index(heading.get_text())<result['body'].index('第0段')
    for i in range(4):assert result['body'].count(f'第{i}段')==1
    assert result['title']==soup.select_one('head > meta[property="og:title"]')['content']
    notice=next(n for n in json.loads(result['verified_article_notices_json'])
                if n['kind']=='native_distinct_inner_heading_preserved_in_body')
    assert notice['text']==heading.get_text() and not notice['outer_headline_replaced']
    assert not notice['heading_used_as_alternate_article_identity']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        parse(versioned(settings,4),raw)


@pytest.mark.parametrize('nested',[True,False])
def test_v5_preserves_accepted_v4_outputs_exactly(settings,nested):
    for raw in (page(nested=nested),flat()):
        assert parse(versioned(settings,5),raw)==parse(versioned(settings,4),raw)


@pytest.mark.parametrize('mutation',[
    lambda h:h.__setitem__('hidden',''),
    lambda h:h.__setitem__('aria-hidden','true'),
    lambda h:h.__setitem__('style','display:none;'),
    lambda h:h.__setitem__('class',['sr-only']),
    lambda h:h.insert_after(BeautifulSoup('<h1>another competing body heading</h1>','lxml').h1),
    lambda h:h.wrap(BeautifulSoup('<div></div>','lxml').div),
    lambda h:h.clear(),
    lambda h:h.append(BeautifulSoup('<span hidden>hidden competing heading</span>','lxml').span),
])
def test_v5_body_heading_keeps_native_boundary_and_visibility_gates(settings,mutation):
    soup=BeautifulSoup(page(),'lxml');h=soup.new_tag('h1');h.string='different native heading'
    soup.select_one('article.pocket-article').insert(0,h);mutation(h)
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        parse(versioned(settings,5),str(soup))


def test_CSS_and_FAQ_hashes_have_named_HTML_newline_basis_not_raw_response_hash(settings):
    raw=flat().replace('table { color: black; }','table {\r\n color: black;\r\n}')
    results=[]
    for engine in ('lxml','html.parser'):
        config=versioned(settings,4);config['providers'][0]['html_parser']=engine
        results.append(parse(config,raw))
    assert results[0]==results[1]
    notices=json.loads(results[0]['verified_article_notices_json'])
    for n in notices:
        if n['kind']in {'native_CSS_not_written_prose_not_executed','native_FAQ_JSONLD_not_a_second_written_body'}:
            assert n['hash_basis']=='HTML_input_newlines_CRLF_or_CR_to_LF' and len(n['DOM_text_LF_sha256'])==64
            assert 'text_sha256'not in n and not n['executed']


@pytest.mark.parametrize('version',[4,5])
@pytest.mark.parametrize('old,new',[
    ('rel="canonical"','rel="alternate"'),
    ('<h1>','<h1 hidden>'),
    ('"isAccessibleForFree": true','"isAccessibleForFree": false'),
    ('口袋學堂編輯團隊</span>','外部供稿</span>'),
    ('"name": "口袋學堂編輯團隊"','"name": "外部供稿"'),
    ('2023-02-14T23:01:02.000Z','2023-02-14T23:01:02'),
])
def test_native_blocks_are_not_a_clock_identity_rights_or_paywall_exception(settings,old,new,version):
    # Change before DOM serialization so synthetic JSON uses its literal
    # ensure_ascii=False names and the existing raw declaration precision.
    raw=page(nested=False).replace(old,new)
    with pytest.raises(NewsError):
        parse(versioned(settings,version),raw)


@pytest.mark.parametrize('mutation',[
    lambda n:n.__setitem__('src','https://evil.example/embed/AbCdEfG1234'),
    lambda n:n.__setitem__('src','https://www.youtube.com.evil.example/embed/AbCdEfG1234'),
    lambda n:n.__setitem__('src','http://www.youtube.com/embed/AbCdEfG1234'),
    lambda n:n.__setitem__('src','https://user@www.youtube.com/embed/AbCdEfG1234'),
    lambda n:n.__setitem__('src','https://www.youtube.com/embed/AbCdEfG1234?autoplay=1'),
    lambda n:n.__setitem__('src','https://www.youtube.com/embed/AbCdEfG1234?si=one&si=two'),
    lambda n:n.__setitem__('src','https://www.youtube.com/embed/AbCdEfG1234#fragment'),
    lambda n:n.__setitem__('srcdoc','<p>never executed</p>'),
    lambda n:n.__setitem__('onload','never executed'),
    lambda n:n.__setitem__('allow','camera'),
    lambda n:n.__setitem__('width','javascript:bad'),
    lambda n:n.__setitem__('height','0'),
    lambda n:n.__setitem__('title','unknown embed'),
    lambda n:n.__setitem__('style','display:none;'),
    lambda n:n.append('unaccounted fallback prose'),
])
def test_iframe_exemption_is_not_an_external_content_or_fallback_prose_bypass(settings,mutation):
    soup=BeautifulSoup(flat(),'lxml');mutation(soup.select_one('iframe'))
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        parse(versioned(settings,4),str(soup))


@pytest.mark.parametrize('mutation',[
    lambda n:n.__setitem__('hidden',''),
    lambda n:n.__setitem__('aria-hidden','true'),
    lambda n:n.__setitem__('style','display:none; position:absolute;'),
    lambda n:n.__setitem__('class',['hidden']),
    lambda n:n.append(BeautifulSoup('<span>hidden markup</span>','lxml').span),
    lambda n:n.__setattr__('string','x'*257),
])
def test_nonvisible_caption_is_only_exact_plain_table_context(settings,mutation):
    soup=BeautifulSoup(flat(),'lxml');mutation(soup.select_one('caption'))
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        parse(versioned(settings,4),str(soup))


def test_caption_does_not_hide_the_table_or_other_prose(settings):
    for selector in ('table','.cms-article-scope p','article[data-scroll-anchor]'):
        soup=BeautifulSoup(flat(),'lxml');soup.select_one(selector)['hidden']=''
        with pytest.raises(NewsError,match='article_identity_not_verified'):
            parse(versioned(settings,4),str(soup))


@pytest.mark.parametrize('mutation',[
    lambda n:n.__setitem__('type','text/javascript'),
    lambda n:n.__setitem__('src','https://example.org/code.js'),
    lambda n:n.__setattr__('string','document.write("not rendered prose")'),
    lambda n:n.__setattr__('string','{"@type":"Article","articleBody":"competing prose"}'),
    lambda n:n.__setattr__('string','{"@type":"FAQPage","mainEntity":[]}'),
    lambda n:n.__setattr__('string','{"@type":"FAQPage","@type":"Other"}'),
])
def test_scripts_are_only_bounded_FAQ_metadata_not_arbitrary_code_or_article(settings,mutation):
    soup=BeautifulSoup(flat(),'lxml');mutation(soup.select_one('.cms-article-scope script'))
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        parse(versioned(settings,4),str(soup))


def test_flat_cta_does_not_truncate_a_later_prose_sibling(settings):
    soup=BeautifulSoup(flat(),'lxml');p=soup.new_tag('p');p.string='must not disappear'
    soup.select_one('.cms-article-scope').append(p)
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        parse(versioned(settings,4),str(soup))


@pytest.mark.parametrize('prior,current',[(3,4),(4,5),(5,6)])
def test_exact_predecessor_and_original_HTTP_identity(settings,prior,current):
    original=PolicyRegistry(settings,base=Path.cwd()).sources['pocket']
    old=PolicyRegistry(versioned(settings,prior),base=Path.cwd()).sources['pocket']
    new=PolicyRegistry(versioned(settings,current),base=Path.cwd()).sources['pocket']
    assert new.parser_repair_prior_capture_fingerprint==old.data_fingerprint!=new.data_fingerprint
    assert new.http_capacity_source_fingerprint==old.http_capacity_source_fingerprint==original.data_fingerprint
    assert new.authorization_fingerprint==old.authorization_fingerprint
    assert pocket_native_parser_repair(new,old.data_fingerprint)
    assert not pocket_native_parser_repair(new,original.data_fingerprint)


@pytest.mark.parametrize('prior,current',[(3,4),(4,5),(5,6)])
@pytest.mark.parametrize('obstruction',[None,'stored','lease','missing_observation','wrong_predecessor'])
def test_once_only_unstored_observed_identity_failures_not_clocks_paths_rights_or_refusals(settings,tmp_path,obstruction,prior,current):
    old=PolicyRegistry(versioned(settings,prior),base=tmp_path);new=PolicyRegistry(versioned(settings,current),base=tmp_path)
    states=['article_identity_not_verified','invalid_bulk_publication_clock','unsupported_article_path',
        'article_rights_or_scope_excluded','http_403','http_429','complete']
    before={}
    with closing(NewsQueue(tmp_path,old))as queue:
        for i,state in enumerate(states,1):
            url=POCKET_ORIGIN+f'/school/articles/{i}/';queue.add_articles([{'url':url}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=9000000000,last_checked_at_utc=? WHERE url=?',
                (state,state,utc_now(),url))
            if obstruction=='stored':queue.db.execute("UPDATE articles SET body_sha256='old-body'WHERE url=?",(url,))
            elif obstruction=='lease':queue.db.execute("UPDATE articles SET lease_owner='other-owner'WHERE url=?",(url,))
            elif obstruction=='missing_observation':queue.db.execute('UPDATE articles SET last_checked_at_utc=NULL WHERE url=?',(url,))
            before[url]=dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(url,)).fetchone())
        if obstruction=='wrong_predecessor':queue.db.execute("UPDATE queue_settings SET value=? WHERE key='source_capture_fingerprint:pocket'",('f'*64,))
        queue.db.commit()
    marker=f'pocket_native_parser_recheck_v{current}:'+new.sources['pocket'].data_fingerprint
    with closing(NewsQueue(tmp_path,new))as queue:
        for url,row in before.items():
            eligible=obstruction is None and row['state']=='article_identity_not_verified'
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?',(url,)).fetchone())==({**row,'state':'retry'}if eligible else row)
        evidence=queue.db.execute('SELECT value FROM queue_settings WHERE key=?',(marker,)).fetchone()
        assert (evidence is None)==(obstruction=='wrong_predecessor')
        if evidence:assert len(json.loads(evidence[0])['rows'])==(1 if obstruction is None else 0)
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified'WHERE state='retry'");queue.db.commit()
    with closing(NewsQueue(tmp_path,new))as queue:
        assert queue.db.execute("SELECT count(*)FROM articles WHERE state='retry'").fetchone()[0]==0
