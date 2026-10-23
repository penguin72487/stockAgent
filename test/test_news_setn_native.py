"""SETN structural/clock contracts, using synthetic prose rather than news text."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_setn import (
    SETN_AUTHOR_SELECTOR, SETN_BODY_SELECTOR, SETN_LAYOUT, SETN_PATH_PATTERN, SETN_REVIEWED_URLS,
)
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article


TEXT = '自行編寫的測試段落，原始股數1000、價格12.34及負數-5都不改動；這不是新聞內容。' * 4
SCHEMA_CLOCK = '2026-09-23 06:08 +00:00'


def entry():
    return {'id': 'setn', 'domains': ['www.setn.com'], 'enabled': True,
        'acquisition_mode': 'finite', 'finite_article_urls': list(SETN_REVIEWED_URLS),
        'html_parser': 'lxml', 'interval_seconds': 10, 'parallelism': 1,
        'embedded_article_layout': SETN_LAYOUT, 'article_selectors': [SETN_BODY_SELECTOR],
        'article_path_pattern': SETN_PATH_PATTERN, 'title_selector': 'h1',
        'require_configured_container': True, 'require_author_attribution': True,
        'author_selectors': [SETN_AUTHOR_SELECTOR],
        'authorization': {'kind': 'personal_research', 'evidence_url': 'https://www.setn.com/',
            'reviewed_on': '2026-10-22', 'scope': 'Only two reviewed private public originals, not a license.',
            'automated_access_review': 'no_explicit_prohibition_identified',
            'original_author_scope_evidence_urls': list(SETN_REVIEWED_URLS)}}


def policy(value=None):
    return PolicyRegistry({'contract_version': 1, 'publish': False, 'remote_training_export': False,
        'providers': [value or entry()]}, base=Path('.')).sources['setn']


def page(index=0):
    url = SETN_REVIEWED_URLS[index]
    name, profile = ('施郁韻', 'https://www.setn.com/author/shi-yuyun') if index == 0 else ('蔡佩蓉', '')
    declared = {'@type': 'NewsArticle', 'mainEntityOfPage': {'@type': 'WebPage', '@id': url},
        'url': url, 'headline': '合成測試標題', 'publisher': {'@type': 'Organization', 'name': '三立新聞網'},
        'author': [{'@type': 'Person', 'name': name, 'url': profile}] if index == 0 else
            {'@type': 'Person', 'name': name, 'url': ''},
        'datePublished': SCHEMA_CLOCK, 'dateModified': SCHEMA_CLOCK,
        'description': 'NOT_ARTICLE_SCHEMA_SUMMARY'}
    visible = f'編輯 <a href="/author/shi-yuyun">{name}</a> 綜合報導' if index == 0 else ''
    printed = '' if index == 0 else '<p>記者蔡佩蓉／台北報導</p>'
    return BeautifulSoup(f'''<html><head><link rel="canonical" href="{url}">
      <meta property="og:url" content="{url}"><meta property="og:type" content="article">
      <meta property="og:title" content="合成測試標題 | 財經 | 三立新聞網  SETN.COM">
      <meta name="author" content="三立新聞網">
      <script type="application/ld+json">{json.dumps(declared,ensure_ascii=False)}</script></head>
      <body><nav>NOT_ARTICLE_NAV</nav><article class="article_wrap"><section class="article_part01">
      <div class="article_breadcrumb_wrap">NOT_ARTICLE_BREADCRUMB</div><h1 class="article_title_wrap">合成測試標題</h1>
      <!-- Source HTML comments are not written prose. -->
      <div class="article_time_wrap"><div class="author_wrap">{visible}</div>
      <div class="article_time_box"><div class="time_item"><span>2026/09/23 06:08:00</span></div>
      <div class="time_item"><span>2026/09/23 06:08:48</span><span>更新</span></div></div></div>
      <div class="textAD_vote_wrap">NOT_ARTICLE_AD</div><div class="ai_article_wrap">NOT_ARTICLE_AI_SUMMARY</div>
      <div class="article_content_wrap"><!-- 文章工具 --><div class="article_tool_wrap">NOT_ARTICLE_TOOLS</div>
      <div class="article_content_area" id="article_content"><div id="newsContent">
      {printed}<p>{TEXT}</p><figure><picture><img src="https://example.org/img" alt="ALT_NOT_PROSE"></picture>
      <figcaption>合成圖說與攝影署名。</figcaption></figure><p>合成結尾。</p>
      <p><strong>來源風險提示，不應丟棄。</strong></p></div></div>
      <div class="more_news_list_wrap">NOT_ARTICLE_MORE</div></div></section>
      <section class="article_part02">NOT_ARTICLE_RELATED</section></article>
      <input id="hfNewsID" value="{url.rsplit('/',1)[1]}"></body></html>''','lxml')


def extract(soup,index=0):
    return extract_article(str(soup).encode(),policy(),page_url=SETN_REVIEWED_URLS[index])


def schema(soup,mutate):
    node=soup.select_one('script[type="application/ld+json"]')
    value=json.loads(node.get_text());mutate(value);node.string=json.dumps(value,ensure_ascii=False)


@pytest.mark.parametrize('index',[0,1])
def test_complete_native_body_caption_role_and_unresolved_clocks_are_lossless(index):
    result=extract(page(index),index)
    assert result['body'].count(TEXT)==1 and result['body'].count('合成圖說與攝影署名。')==1
    assert result['body'].index(TEXT)<result['body'].index('合成圖說')<result['body'].index('合成結尾')
    assert '來源風險提示，不應丟棄。' in result['body']
    assert 'NOT_ARTICLE_' not in result['body'] and 'ALT_NOT_PROSE' not in result['body']
    assert result['body_sha256']==hashlib.sha256(result['body'].encode()).hexdigest()
    assert result['published_at_raw']==result['modified_at_raw']==SCHEMA_CLOCK
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    name=('施郁韻','蔡佩蓉')[index]
    assert json.loads(result['authors_json'])==[name]
    role=json.loads(result['author_attribution_detail_json'])
    assert role['source_declared_person_name']==name and role['meta_author_is_publisher_not_person']
    assert not role['original_ownership_verified']
    if index==1: assert '記者蔡佩蓉／台北報導' in result['body']
    clocks=json.loads(result['verified_article_notices_json'])
    assert clocks['schema_timezone_offset_raw']=='+00:00' and clocks['schema_clock_precision']=='minute'
    assert clocks['visible_modified_clock_raw']=='2026/09/23 06:08:48'
    assert clocks['visible_clock_precision']=='second' and not clocks['visible_timezone_declared']
    assert clocks['publication_offset_resolution']=='unresolved' and not clocks['exact_PIT_eligible']
    assert not clocks['same_wall_minute_is_same_instant_proof']


@pytest.mark.parametrize('selector,attribute,value,error',[
    ('link[rel=canonical]','href',SETN_REVIEWED_URLS[1],'article_identity_not_verified'),
    ('meta[property="og:url"]','content',SETN_REVIEWED_URLS[1],'article_identity_not_verified'),
    ('input#hfNewsID','value','999','article_identity_not_verified'),
    ('#newsContent','hidden','','article_identity_not_verified'),
    ('figcaption','style','display: none','article_identity_not_verified'),
    ('.author_wrap > a','href','/author/other','article_attribution_not_verified'),
])
def test_identity_visibility_and_declared_byline_fail_closed(selector,attribute,value,error):
    soup=page();soup.select_one(selector)[attribute]=value
    with pytest.raises(NewsError,match=error): extract(soup)


@pytest.mark.parametrize('mutate,error',[
    (lambda a:a.update(mainEntityOfPage={'@id':SETN_REVIEWED_URLS[1]}),'article_identity_not_verified'),
    (lambda a:a.update(headline='不同標題'),'article_identity_not_verified'),
    (lambda a:a.update(publisher={'@type':'Organization','name':'其他來源'}),'article_identity_not_verified'),
    (lambda a:a.update(author={'@type':'Organization','name':'三立新聞網'}),'article_attribution_not_verified'),
    (lambda a:a.update(author=[{'@type':'Person','name':'中央社'}]),'article_attribution_not_verified'),
    (lambda a:a.update(datePublished='2026-09-23T06:08:00+08:00'),'invalid_bulk_publication_clock'),
    (lambda a:a.update(datePublished='2026-02-30 06:08 +00:00'),'invalid_bulk_publication_clock'),
    (lambda a:a.update(datePublished='2026-09-23 07:08 +00:00'),'invalid_bulk_publication_clock'),
    (lambda a:a.update(dateModified='2026-09-22 06:08 +00:00'),'invalid_bulk_publication_clock'),
    (lambda a:a.update(isAccessibleForFree=False),'paywall_or_login'),
    (lambda a:a.update(isAccessibleForFree='false'),'paywall_or_login'),
])
def test_schema_cannot_override_source_identity_author_access_or_clock_precision(mutate,error):
    soup=page();schema(soup,mutate)
    with pytest.raises(NewsError,match=error): extract(soup)


@pytest.mark.parametrize('selector,markup',[
    ('#newsContent','<div>不可遺失的新正文區。</div>'),
    ('#newsContent','<p hidden>不能刪掉以宣稱完整。</p>'),
    ('#newsContent','<script>window.unreviewed=1;</script>'),
    ('#article_content','<p>容器外的新段落。</p>'),
    ('.article_content_wrap','<p>另一個未審核正文區。</p>'),
    ('.article_part01','<p>不可因selector太窄而丟掉。</p>'),
    ('picture','<span>不可忽略的圖片旁文字。</span>'),
    ('figure','<p>不能丟掉的圖旁段落。</p>'),
])
def test_unknown_written_editor_content_is_not_discarded(selector,markup):
    soup=page();fragment=BeautifulSoup('<div>'+markup+'</div>','lxml')
    soup.select_one(selector).append(fragment.div.contents[0])
    with pytest.raises(NewsError,match='article_identity_not_verified'):extract(soup)


@pytest.mark.parametrize('url',[
    SETN_REVIEWED_URLS[0]+'?member=1',SETN_REVIEWED_URLS[0].replace('www.','inews.'),
    'https://www.setn.com/News.aspx?NewsID=1911453','https://www.setn.com/news/952211',
])
def test_literal_native_identity_does_not_claim_legacy_aliases_or_other_sources(url):
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        extract_article(str(page()).encode(),policy(),page_url=url)


@pytest.mark.parametrize('field,value',[
    ('domains',['setn.com']),('acquisition_mode','continuous'),
    ('finite_article_urls',[SETN_REVIEWED_URLS[0]]),('allow_reviewed_supplements',True),
    ('article_selectors',['article']),('require_author_attribution',False),
    ('exclude_selectors',['figcaption']),('permitted_author_names',['三立新聞網']),
    ('sitemaps',['https://www.setn.com/sitemapindex.xml']),
])
def test_finite_native_contract_does_not_grant_whole_history(field,value):
    candidate=entry();candidate[field]=value
    with pytest.raises(ValueError,match='unreviewed embedded article parser|finite article scope'):
        policy(candidate)


def test_only_exact_reviewed_pair_preserves_prior_HTTP_control_identity(monkeypatch):
    source=policy()
    monkeypatch.setattr('downloader.news_fulltext_setn.SETN_FINITE_HTTP_BINDING',
        (source.data_fingerprint,source.authorization_fingerprint,'1'*64,'2'*64))
    bound=policy()
    assert bound.http_capacity_source_fingerprint=='1'*64
    assert bound.http_capacity_authorization_fingerprint=='2'*64
    changed=entry();changed['interval_seconds']=11
    assert not policy(changed).http_capacity_source_fingerprint
    changed=entry();changed['authorization']['scope']+=' Changed scope.'
    assert not policy(changed).http_capacity_authorization_fingerprint


def test_retained_native_materialization_is_durable_once_and_never_creates_HTTP(tmp_path,monkeypatch):
    import pyarrow.parquet as pq
    from downloader.artifact_io import atomic_write_json
    from downloader.news_fulltext_retained_native import materialize_reviewed_native
    from test_news_retained_native_replay import approved,head
    config={'contract_version':1,'publish':False,'remote_training_export':False,
        'collection_purpose':'personal_research','providers':[entry()]}
    auth=deepcopy(entry()['authorization']);auth.pop('original_author_scope_evidence_urls')
    auth['scope']='Two public diagnostic original questions, not yet formal bodies.'
    queue,registry,path,manifest=approved(tmp_path,config,urls=SETN_REVIEWED_URLS,provider='setn',
        original_auth=auth,raw_factory=lambda url:str(page(SETN_REVIEWED_URLS.index(url))).encode())
    try:
        queue.sync_finite_cohorts()
        directory=path.parent/'private-source-diagnostics';directory.chmod(0o700)
        for member in directory.iterdir():member.chmod(0o600)
        manifest.update(contract_version=3,scope_kind='reviewed_finite_native_materialization_v3',
            original_author_scope_evidence_addition_reviewed=True,diagnostic_directory=directory.name)
        atomic_write_json(path,manifest,durable=True)
        monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
            lambda *a,**k:pytest.fail('retained bodies cannot create a second HTTP owner'))
        accepted=materialize_reviewed_native(queue,SETN_REVIEWED_URLS,path,lease_seconds=300)
        assert accepted['materialized_bodies']==2 and accepted['publisher_requests_this_command']==0
        heads=[head(queue,url) for url in SETN_REVIEWED_URLS]
        for selected in heads:
            row,=[r for r in pq.ParquetFile(queue.root/selected['parquet_path']).read().to_pylist()
                  if r['requested_url']==selected['url']]
            assert selected['state']=='complete' and row['acquisition_transport']=='retained_raw_reparse'
            assert row['published_at_raw']==SCHEMA_CLOCK
            assert not json.loads(row['verified_article_notices_json'])['exact_PIT_eligible']
            assert json.loads(row['original_diagnostic_authorization_json'])==auth
        assert materialize_reviewed_native(queue,SETN_REVIEWED_URLS,path,lease_seconds=300)['materialized_bodies']==0
        assert heads==[head(queue,url) for url in SETN_REVIEWED_URLS]
    finally:queue.close()
