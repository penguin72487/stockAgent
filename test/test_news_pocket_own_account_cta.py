"""Exact visible account action, never an invented brokerage credit or request."""
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError
from test_news_pocket_original import settings, page, versioned
from test_news_pocket_native_blocks import parse


ACCOUNT='https://www.pocket.tw/openaccountonline/oa/login/new?mkCode=synthetic'


def raw_action():
    raw=page().replace('口袋證券廣告不是研究正文','此為帳戶申請說明，非原始研究正文')
    return raw.replace('href="https://pse.im/78wp56">開戶連結不是研究正文',
                       f'href="{ACCOUNT}">線上申請證券戶')


@pytest.mark.parametrize('engine',['lxml','html.parser'])
def test_v6_recognizes_native_own_account_action_without_inventing_brand_or_body(engine,settings):
    raw=raw_action();config=versioned(settings,6);config['providers'][0]['html_parser']=engine
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        parse(versioned(settings,5),raw)
    result=parse(config,raw)
    assert result['body']==parse(versioned(settings,5),page())['body']
    assert '申請說明' not in result['body'] and '線上申請證券戶' not in result['body']
    notices=json.loads(result['verified_article_notices_json'])
    action=next(n for n in notices if n['kind']=='native_own_account_action_identifies_brokerage_cta')
    assert action['source_url_raw']==ACCOUNT and not action['broker_name_text_invented']
    assert not action['account_form_or_media_requested'] and not result['historical_point_in_time']


@pytest.mark.parametrize('nested',[False,True])
def test_previously_accepted_output_is_exactly_unchanged(settings,nested):
    assert parse(versioned(settings,6),page(nested=nested))==parse(versioned(settings,5),page(nested=nested))


@pytest.mark.parametrize('mutation',['host','path','label','duplicate','hidden_action','hidden_label','hidden_box'])
def test_action_evidence_must_be_exact_unique_and_visible(settings,mutation):
    soup=BeautifulSoup(raw_action(),'lxml')
    link=soup.select_one(f'a[href="{ACCOUNT}"]')
    if mutation=='host':link['href']=ACCOUNT.replace('www.pocket.tw','www.pocket.tw.example.org')
    elif mutation=='path':link['href']=ACCOUNT.replace('/oa/login/new','/oa/login/other')
    elif mutation=='label':link.string='未核對動作'
    elif mutation=='duplicate':link.insert_after(BeautifulSoup(str(link),'lxml').a)
    elif mutation=='hidden_action':link['hidden']=''
    elif mutation=='hidden_box':link.parent['aria-hidden']='true'
    else:
        link.clear();span=soup.new_tag('span',attrs={'style':'display:none'});span.string='線上申請證券戶';link.append(span)
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        parse(versioned(settings,6),str(soup))
