"""Exact source test-account signatures never become accepted news bodies."""
from copy import deepcopy
import hashlib
import json

from bs4 import BeautifulSoup
import pytest

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext_policy import NewsError, extract_article
from downloader.news_fulltext_publishers import cnyes_verified_test_article
from stockagent.live.news_dashboard import build_news_public_status
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_html import CLOCK, TITLE, URL, short_page, source
from test_news_dashboard import _seed, NOW
from test_news_cnyes_nonbody_completion import AD


CASES = [('測試帳號：XX','XXXX'),('測試帳號：123','1234'),('測試帳號：1655566','165655+55')]


def test_page(headline, body, *, native=False, **options):
    payload=short_page(body=body, **options).replace(TITLE.encode(), headline.encode())
    if native:
        payload=payload.replace(b'<div id="article-container"><p>',
            b'<div id="article-container"><section style="margin-top:30px"><p>').replace(
            b'</p></div></article>',b'</p></section><p></p></div></article>')
    return payload


test_page.__test__ = False


@pytest.mark.parametrize('parser',['lxml','html.parser'])
@pytest.mark.parametrize('native',[False,True])
@pytest.mark.parametrize('headline,body',CASES)
def test_literal_test_accounts_are_source_outcomes_not_news(cnyes_config,tmp_path,parser,native,headline,body):
    cnyes_config['providers'][0]['html_parser']=parser
    with pytest.raises(NewsError,match='^source_article_test_only$') as caught:
        extract_article(test_page(headline,body,native=native),source(cnyes_config,tmp_path),page_url=URL)
    assert not caught.value.retry


@pytest.mark.parametrize('headline,body',CASES)
@pytest.mark.parametrize('options',[
    {'change':{'payment':1}}, {'change':{'payment':False}}, {'change':{'payment':'0'}},
    {'change':{'publishAt':1388534401}}, {'change':{'video':'source video'}},
    {'change':{'content':'<p>actual independent body</p>'}}, {'visible':'actual independent body'},
    {'schema_change':{'articleBody':'another declared body'}}, {'conflict':True},
    {'duplicate_key':True}, {'video_conflict':True}, {'canonical':URL.replace('/42','/43')},
])
def test_test_account_classification_preserves_identity_free_clock_and_whole_text(cnyes_config,tmp_path,headline,body,options):
    with pytest.raises(NewsError) as caught:
        extract_article(test_page(headline,body,**options),source(cnyes_config,tmp_path),page_url=URL)
    assert caught.value.code != 'source_article_test_only'


@pytest.mark.parametrize('body',['XX','123','16565','-', 'b','XXXX 1','XXXX正文','1234 news',
    '165655+55 extra','Useful real financial news report with a source test account.'])
def test_no_unobserved_placeholder_or_prose_inferred_from_a_test_headline(cnyes_config,tmp_path,body):
    soup=BeautifulSoup(test_page(CASES[0][0],body),'lxml')
    article={'@type':'NewsArticle','url':URL,'headline':CASES[0][0],'datePublished':CLOCK}
    assert not cnyes_verified_test_article(soup,source(cnyes_config,tmp_path),article,body,page_url=URL)


@pytest.mark.parametrize('headline',['公司：XXXX','測試帳號：YYY','測試帳號:XX','測試帳號：XX 的公告','一般測試與財報'])
def test_no_short_or_named_test_report_is_excluded_without_exact_signature(cnyes_config,tmp_path,headline):
    with pytest.raises(NewsError) as caught:
        extract_article(test_page(headline,'XXXX'),source(cnyes_config,tmp_path),page_url=URL)
    assert caught.value.code != 'source_article_test_only'


@pytest.mark.parametrize('markup',[
    '<p hidden>XXXX</p>','<p style="display:none">XXXX</p>','<p data-unknown="1">XXXX</p>',
    '<p>XXXX<!--unknown--></p>','<p>XX<b>XX</b></p>','<p>XXXX</p><script>unknown()</script>',
    '<p>XXXX</p><figure></figure>','<p>XXXX</p><img>','<p>XXXX</p><table></table>',
    '<p>XXXX</p><p></p><p></p>',
])
def test_source_test_signature_cannot_erase_other_unknown_body_structure(cnyes_config,tmp_path,markup):
    payload=test_page(CASES[0][0],CASES[0][1]).replace(b'<p>XXXX</p>',markup.encode(),1)
    with pytest.raises(NewsError) as caught:
        extract_article(payload,source(cnyes_config,tmp_path),page_url=URL)
    assert caught.value.code != 'source_article_test_only'


@pytest.mark.parametrize('url',[URL+'?exp=a', URL+'?token=secret',URL.replace('https','http'),
    URL.replace('news.','m.'),URL.replace('/42','/43')])
def test_no_generated_alias_scope_granted(cnyes_config,tmp_path,url):
    with pytest.raises(NewsError) as caught:
        extract_article(test_page(CASES[0][0],CASES[0][1]),source(cnyes_config,tmp_path),page_url=url)
    assert caught.value.code != 'source_article_test_only'


@pytest.mark.parametrize('mutation',['duplicate','conflict','malformed'])
def test_ambiguous_schema_never_proves_a_source_test_outcome(cnyes_config,tmp_path,mutation):
    soup=BeautifulSoup(test_page(CASES[0][0],CASES[0][1]),'lxml')
    node=soup.select_one('script[type="application/ld+json"]')
    extra=deepcopy(node)
    if mutation=='conflict':
        payload=json.loads(extra.string)
        payload['@graph'][0]['headline']='another article'
        extra.string=json.dumps(payload)
    elif mutation=='malformed':
        extra.string='{bad-json'
    soup.head.append(extra)
    with pytest.raises(NewsError) as caught:
        extract_article(str(soup).encode(),source(cnyes_config,tmp_path),page_url=URL)
    assert caught.value.code != 'source_article_test_only'


def test_only_new_html_source_outcome_contract_changes_not_http_api_or_authorization(cnyes_config,tmp_path,monkeypatch):
    new=source(cnyes_config,tmp_path)
    original=hashlib.sha256
    def old(value):
        try:
            payload=json.loads(value)
        except (ValueError,TypeError,UnicodeDecodeError):
            return original(value)
        if isinstance(payload,dict) and 'verified_source_test_article_outcome_contract_version' in payload:
            assert payload.pop('verified_source_test_article_outcome_contract_version') == 1
            return original(json.dumps(payload,sort_keys=True).encode())
        return original(value)
    monkeypatch.setattr('downloader.news_fulltext_policy.hashlib.sha256',old)
    prior=source(cnyes_config,tmp_path)
    assert new.data_fingerprint != prior.data_fingerprint
    assert new.authorization_fingerprint == prior.authorization_fingerprint
    assert new.cnyes_bulk_data_fingerprint == prior.cnyes_bulk_data_fingerprint
    assert new.http_capacity_source_fingerprint == prior.http_capacity_source_fingerprint


def test_dashboard_test_source_count_is_not_a_saved_or_ready_body(tmp_path):
    status=_seed(tmp_path)
    status['providers']['cnyes']['states']['source_article_test_only']=3
    status['states']['source_article_test_only']=3
    status['unique_articles']+=3
    atomic_write_json(tmp_path/'data_news_fulltext/status.json',status)
    result=build_news_public_status(tmp_path,now=NOW)
    cnyes=next(p for p in result['providers'] if p['id']=='cnyes')
    assert cnyes['states']['source_article_test_only']==3
    assert cnyes['complete_urls']==2 and cnyes['remaining_urls']==4
    assert cnyes['blocked_urls']==3
    assert cnyes['known_urls']==9
    assert 'PRIVATE_' not in json.dumps(result)


def multi_section_test_page(*, include_ad=False):
    headline,body=CASES[2]
    paragraphs=['16565','5','+5','5']
    current=''.join('<p>'+text+'</p>' for text in paragraphs)
    visible=''.join('<section style="margin-top:30px"><p>'+text+'</p></section><p></p>'
        for text in paragraphs)
    if include_ad:
        visible=visible.replace('<section style="margin-top:30px"><p>+5</p></section><p></p>',
            '<section style="margin-top:30px"><p>+5</p></section>'+AD)
    payload=test_page(headline,body,change={'content':current})
    return payload.replace(('<p>'+body+'</p>').encode(),visible.encode(),1)


@pytest.mark.parametrize('parser',['lxml','html.parser'])
@pytest.mark.parametrize('include_ad',[False,True])
def test_actual_section_delimiters_preserve_every_source_paragraph(cnyes_config,tmp_path,parser,include_ad):
    cnyes_config['providers'][0]['html_parser']=parser
    with pytest.raises(NewsError,match='^source_article_test_only$'):
        extract_article(multi_section_test_page(include_ad=include_ad),source(cnyes_config,tmp_path),page_url=URL)


@pytest.mark.parametrize('old,new',[
    ('</section><p></p>','</section><p></p><p></p>'),
    ('</section><p></p>','</section><p> </p>'),
    ('</section><p></p>','</section><p hidden></p>'),
    ('style="margin-top:30px"','style="margin-top:31px"'),
    ('<p>16565</p>','<p>165</p><p>65</p>'),
    ('<p>16565</p>','<p>16565<!--unknown--></p>'),
])
def test_renderer_grammar_does_not_erase_extra_blanks_or_repartition_values(cnyes_config,tmp_path,old,new):
    payload=multi_section_test_page().replace(old.encode(),new.encode(),1)
    with pytest.raises(NewsError) as caught:
        extract_article(payload,source(cnyes_config,tmp_path),page_url=URL)
    assert caught.value.code != 'source_article_test_only'


@pytest.mark.parametrize('old,new',[
    ('class="i1y9ycug"','class="unknown"'),
    ('aria-busy="true"','aria-busy="false"'),
    ('\u200c','actual body text'),
    ('\u200c','\u200c<!--unknown-->'),
    ('<div class="i1y9ycug"','<div hidden class="i1y9ycug"'),
    ('border-radius:6px','border-radius:5px'),
])
def test_exact_existing_ad_skeleton_cannot_strip_unknown_body_fields(cnyes_config,tmp_path,old,new):
    payload=multi_section_test_page(include_ad=True).replace(old.encode(),new.encode(),1)
    with pytest.raises(NewsError) as caught:
        extract_article(payload,source(cnyes_config,tmp_path),page_url=URL)
    assert caught.value.code != 'source_article_test_only'
