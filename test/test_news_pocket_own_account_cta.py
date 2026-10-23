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


def nonterminal_cta(*, nested=True, brandless=False):
    if nested:
        raw=raw_action() if brandless else page()
    else:
        from test_news_pocket_native_blocks import flat
        raw=flat()
    soup=BeautifulSoup(raw,'lxml');cta=soup.select_one('#section-cta')
    ad=cta.find_next_sibling()
    empty=soup.new_tag('p');empty.string='\u00a0';ad.insert_after(empty)
    warning=soup.new_tag('div',attrs={'class':'caution-box'})
    strong=soup.new_tag('strong');strong.string='投資風險聲明：';warning.append(strong)
    warning.append('後續原生文字保留負值 -12.50、0、比率 2.8，不是開戶廣告。')
    empty.insert_after(warning)
    return soup


@pytest.mark.parametrize('engine',['lxml','html.parser'])
@pytest.mark.parametrize('nested,brandless',[(True,False),(True,True),(False,False)])
def test_v7_only_separates_adjacent_ad_and_preserves_following_native_prose(settings,engine,nested,brandless):
    soup=nonterminal_cta(nested=nested,brandless=brandless);raw=str(soup)
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        parse(versioned(settings,6),raw)
    config=versioned(settings,7);config['providers'][0]['html_parser']=engine
    result=parse(config,raw)
    assert result['body'].count('投資風險聲明：')==1
    assert result['body'].count('後續原生文字保留負值 -12.50、0、比率 2.8')==1
    assert result['body'].index('第0段')<result['body'].index('投資風險聲明：')
    assert '線上申請證券戶' not in result['body'] and '開戶連結' not in result['body']
    assert '廣告不是研究正文' not in result['body']
    assert result['published_at_raw']==parse(versioned(settings,6),page())['published_at_raw']
    notice=next(n for n in json.loads(result['verified_article_notices_json'])
        if n['kind']=='native_brokerage_cta_has_following_prose_preserved')
    assert notice['following_native_sibling_count']==2 and notice['following_prose_not_removed']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']


@pytest.mark.parametrize('raw',[page(),page(nested=False),raw_action()])
def test_v7_keeps_previously_accepted_entire_v6_output(settings,raw):
    assert parse(versioned(settings,7),raw)==parse(versioned(settings,6),raw)


@pytest.mark.parametrize('mutation',[
    'hidden_cta','hidden_ad','hidden_ad_child','hidden_following_prose','hidden_warning_text',
    'unsupported_following_tag','competing_cta','nonadjacent_ad','unknown_link','no_broker_identity',
])
def test_v7_retains_visibility_boundary_and_following_prose_validation(settings,mutation):
    soup=nonterminal_cta();cta=soup.select_one('#section-cta');ad=cta.find_next_sibling()
    warning=soup.select_one('.caution-box')
    if mutation=='hidden_cta':cta['hidden']=''
    elif mutation=='hidden_ad':ad['hidden']=''
    elif mutation=='hidden_ad_child':ad.select_one('a')['aria-hidden']='true'
    elif mutation=='hidden_following_prose':warning['hidden']=''
    elif mutation=='hidden_warning_text':warning.strong['class']=['sr-only']
    elif mutation=='unsupported_following_tag':warning.name='form'
    elif mutation=='competing_cta':warning.append(BeautifulSoup(str(cta),'lxml').h2)
    elif mutation=='nonadjacent_ad':cta.insert_after(soup.new_tag('hr'))
    elif mutation=='unknown_link':ad.select_one('a')['href']='https://unknown.example.org/'
    else:ad.p.string='沒有經核對的廣告身分'
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        parse(versioned(settings,7),str(soup))


@pytest.mark.parametrize('engine',['lxml','html.parser'])
@pytest.mark.parametrize('count',[1,3])
@pytest.mark.parametrize('trailing',[False,True])
def test_v8_preserves_visible_blank_paragraph_gap_and_all_following_prose(settings,engine,count,trailing):
    soup=nonterminal_cta()if trailing else BeautifulSoup(page(),'lxml')
    cta=soup.select_one('#section-cta');ad=cta.find_next_sibling()
    for _ in range(count):
        gap=BeautifulSoup('<p>\u00a0<span><br/></span></p>','lxml').p
        ad.insert_before(gap)
    raw=str(soup)
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        parse(versioned(settings,7),raw)
    config=versioned(settings,8);config['providers'][0]['html_parser']=engine
    result=parse(config,raw)
    expected=parse(versioned(settings,7),str(nonterminal_cta())if trailing else page())
    assert result['body']==expected['body']
    notice=next(n for n in json.loads(result['verified_article_notices_json'])
        if n['kind']=='native_whitespace_paragraphs_between_cta_and_ad_preserved')
    assert notice['paragraph_count']==count and notice['whitespace_not_deleted_or_interpreted_as_prose']
    assert result['published_at_raw']==expected['published_at_raw']and result['modified_at_raw']==expected['modified_at_raw']


@pytest.mark.parametrize('label',['線上 5 分鐘快速申請','👉 線上 5 分鐘快速申請'])
def test_v8_own_account_action_accepts_only_witnessed_fast_apply_labels(settings,label):
    raw=raw_action().replace('線上申請證券戶',label)
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        parse(versioned(settings,7),raw)
    result=parse(versioned(settings,8),raw)
    assert result['body']==parse(versioned(settings,7),raw_action())['body']
    notice=next(n for n in json.loads(result['verified_article_notices_json'])
        if n['kind']=='native_own_account_action_identifies_brokerage_cta')
    assert notice['visible_action']==label and not notice['account_form_or_media_requested']


@pytest.mark.parametrize('gap',[
    '<p>不可消失的實際段落 -12.50</p>', '<p hidden>\u00a0</p>',
    '<p><img src="/unfetched.png"/></p>', '<p><a href="/unknown">\u00a0</a></p>',
    '<p><font>\u00a0</font></p>', '<div>\u00a0</div>',
])
def test_v8_does_not_skip_intervening_prose_assets_hidden_or_unreviewed_gap(settings,gap):
    soup=BeautifulSoup(page(),'lxml');cta=soup.select_one('#section-cta')
    cta.insert_after(BeautifulSoup(gap,'lxml').body.contents[0])
    with pytest.raises(NewsError,match='article_identity_not_verified'):
        parse(versioned(settings,8),str(soup))


@pytest.mark.parametrize('raw',[page(),page(nested=False),raw_action(),str(nonterminal_cta())])
def test_v8_previous_full_v7_outputs_are_unchanged(settings,raw):
    assert parse(versioned(settings,8),raw)==parse(versioned(settings,7),raw)
