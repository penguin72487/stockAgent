"""House native repairs keep literal authorship, external quotes and clocks."""
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_ettoday import ETTODAY_V25_PROVED_HOUSE_NATIVE_FAILURES
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, utc_now
import test_news_ettoday as native
import test_news_ettoday_house_v24 as house
import test_news_ettoday_static_twitter_v19 as twitter


def registry(version=25):
    return PolicyRegistry(house.config(version), base=Path.cwd())


def extract(soup, parser='lxml', version=25):
    return extract_article(str(soup).encode(), replace(registry(version).sources['ettoday'], html_parser=parser),
        page_url=house.URL)


def bare_page(parser='lxml'):
    soup = house.page(parser)
    soup.select_one('#credit').string = '研究記者／台北報導'
    soup.select_one('.story > p:nth-of-type(2) strong').string = '▲原始圖片出處與123456股。（圖／記者研究記者攝）'
    native.update_schema(soup, creator=['1940-研究記者', '1940'])
    return BeautifulSoup(str(soup), parser)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_bare_native_signature_not_rewritten_and_role_requires_same_visible_name_and_creator(parser):
    soup = bare_page(parser)
    original = str(soup)
    expected = deepcopy(soup)
    expected.select_one('#credit').string = '記者研究記者／台北報導'
    actual, old = extract(soup, parser), extract(expected, parser, version=24)
    assert str(soup) == original
    for key in ('body','body_sha256','authors_json','published_at_raw','modified_at_raw','verified_image_captions_json'):
        assert actual[key] == old[key], key
    detail, = json.loads(actual['author_attribution_detail_json'])
    assert detail['native_credit'] == '研究記者／台北報導' and detail['role'] == 'reporter'
    note, = [n for n in json.loads(actual['verified_article_notices_json'])
        if n['kind'] == 'native_reporter_prefix_omitted_with_role_and_creator_witness']
    assert not note['prefix_added_or_source_values_rewritten']
    assert note['same_article_creator_name_and_identifier_required']
    with pytest.raises(NewsError): extract(soup, parser, version=24)


@pytest.mark.parametrize('parser', ['lxml','html.parser'])
@pytest.mark.parametrize('change', ['creator_missing','creator_name','creator_id','creator_type','duplicate_creator',
    'caption_name','caption_role','alt_only','hidden_caption','hidden_credit','credit_prose','center',
    'no_schema','duplicate_schema','paid','wrong_canonical','wrong_clock','unknown_script','external_credit'])
def test_bare_name_never_supplies_unknown_reporter_role_or_waives_existing_guards(parser, change):
    soup = bare_page(parser)
    caption = soup.select_one('.story > p:nth-of-type(2) strong')
    if change.startswith('creator') or change=='duplicate_creator':
        creator = {'creator_missing':None,'creator_name':['1940-其他記者','1940'],
            'creator_id':['1940-研究記者','1941'],'creator_type':['1940-研究記者',1940],
            'duplicate_creator':['1940-研究記者','1940','1940']}[change]
        native.update_schema(soup, creator=creator)
    elif change=='caption_name': caption.string='▲原始照片。（圖／記者其他記者攝）'
    elif change=='caption_role': caption.string='▲原始照片。（圖／研究記者攝）'
    elif change=='alt_only':
        soup.select_one('.story img')['alt']=caption.get_text()
        caption.string='▲原始照片。'
    elif change=='hidden_caption': caption['hidden']=''
    elif change=='hidden_credit': soup.select_one('#credit')['style']='display:none'
    elif change=='credit_prose': soup.select_one('#credit').string='研究記者／台北報導 今日內文'
    elif change=='center':
        soup.select_one('#credit').string='房產中心／台北報導'
        caption.string='▲原始照片。（圖／記者房產中心攝）'
        native.update_schema(soup,creator=['1940-房產中心','1940'])
    elif change=='no_schema': soup.select_one('#report').decompose()
    elif change=='duplicate_schema': soup.head.append(deepcopy(soup.select_one('#report')))
    elif change=='paid': native.update_schema(soup,isAccessibleForFree=False)
    elif change=='wrong_canonical': soup.select_one('link[rel=canonical]')['href']+='0'
    elif change=='wrong_clock': native.update_schema(soup,datePublished=native.MOD)
    elif change=='unknown_script': soup.select_one('.story').append(soup.new_tag('script',src='/unknown.js'))
    elif change=='external_credit': soup.select_one('#credit').string='中央社／台北報導'
    with pytest.raises(NewsError): extract(soup,parser)


@pytest.mark.parametrize('parser', ['lxml','html.parser'])
def test_house_static_twitter_uses_existing_exact_separator_without_executing_or_fetching(parser):
    soup=house.page(parser)
    baseline=extract(soup,parser,version=24)
    for node in list(BeautifulSoup(twitter.QUOTE+'\n'+twitter.SCRIPT,'html.parser').contents):
        soup.select_one('.story').append(node.extract())
    actual=extract(soup,parser)
    for key in ('body','body_sha256','authors_json','published_at_raw','modified_at_raw','verified_image_captions_json'):
        assert actual[key]==baseline[key]
    note,=[n for n in json.loads(actual['verified_article_notices_json']) if n['kind']=='native_static_twitter_quote_declaration']
    assert note['excluded_from_newsroom_body'] and note['quoted_post_preserved_in_shared_original']
    assert not any(note[k] for k in ('script_executed','external_request_sent','post_credit_is_reporter_attribution'))
    with pytest.raises(NewsError): extract(soup,parser,version=24)
    soup.select_one('.story script[src*="twitter"]')['src']='https://other.example/widget.js'
    with pytest.raises(NewsError): extract(soup,parser)


@pytest.mark.parametrize('parser', ['lxml','html.parser'])
def test_existing_house_full_extractor_output_unchanged(parser):
    assert extract(house.page(parser),parser)==extract(house.page(parser),parser,version=24)


@pytest.mark.parametrize('case', ['eligible','has_body','partition','future_due','unobserved','naive',
    'future_observation','lease_owner','lease_until','other_failure','wrong_error','no_attempt','other_url'])
@pytest.mark.parametrize('url,error', ETTODAY_V25_PROVED_HOUSE_NATIVE_FAILURES)
def test_only_source_proved_missing_due_unleased_exact_failures_retry_once(tmp_path,case,url,error):
    old,new=registry(24),registry()
    if case=='other_url': url=house.URL
    with closing(NewsQueue(tmp_path,old)) as queue:
        queue.add_articles([{'url':url}])
        values={'state':error,'error_code':error,'attempts':2,'due':99,'last_checked_at_utc':utc_now()}
        if case=='has_body': values['body_sha256']='a'*64
        elif case=='partition': values['parquet_path']='prior.parquet'
        elif case=='future_due': values['due']=(datetime.now(timezone.utc)+timedelta(days=1)).timestamp()
        elif case=='unobserved': values['last_checked_at_utc']=None
        elif case=='naive': values['last_checked_at_utc']='2026-10-18T00:00:00'
        elif case=='future_observation': values['last_checked_at_utc']=(datetime.now(timezone.utc)+timedelta(days=1)).isoformat()
        elif case=='lease_owner': values['lease_owner']='other'
        elif case=='lease_until': values['lease_until']=1
        elif case=='other_failure': values['state']=values['error_code']='http_403'
        elif case=='wrong_error': values['error_code']='http_403'
        elif case=='no_attempt': values['attempts']=0
        with queue.db:
            queue.db.execute('UPDATE articles SET '+','.join(k+'=?' for k in values)+' WHERE url=?',(*values.values(),url))
        before=dict(queue.db.execute('SELECT * FROM articles').fetchone())
    with closing(NewsQueue(tmp_path,new)) as queue:
        actual=dict(queue.db.execute('SELECT * FROM articles').fetchone())
        assert actual=={**before,'state':'retry' if case=='eligible' else before['state']}
        marker=json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
            ('ettoday_house_native_recheck_v25:'+new.sources['ettoday'].data_fingerprint,)).fetchone()[0])
        assert len(marker['rows'])==(case=='eligible') and not marker['accepted_as_bodies']
        with queue.db: queue.db.execute('UPDATE articles SET state=?',(before['state'],))
        queue.recheck_ettoday_house_native(old.sources['ettoday'].data_fingerprint)
        assert queue.db.execute('SELECT state FROM articles').fetchone()[0]==before['state']


def test_parser_only_identity_keeps_authorization_capacity_and_exact_prior_capture():
    old,new=registry(24).sources['ettoday'],registry().sources['ettoday']
    assert old.authorization_fingerprint==new.authorization_fingerprint
    assert old.http_capacity_source_fingerprint==new.http_capacity_source_fingerprint
    assert old.data_fingerprint==new.parser_repair_prior_capture_fingerprint
    assert old.data_fingerprint!=new.data_fingerprint
