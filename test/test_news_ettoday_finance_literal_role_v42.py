"""A printed role typo stays literal and needs an independent creator witness."""
from copy import deepcopy
import json

import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
import test_news_ettoday as native
from test_news_ettoday_finance_caption_v41 import source

NAME='研究記者'
SIGNATURE='記埋'+NAME+'／台北報導'


def page():
    soup=native.page('finance');soup.select_one('#credit').string=SIGNATURE
    native.update_schema(soup,creator=['123-'+NAME,'123'])
    # Reparse the synthetic document so a second mutation sees a Script node,
    # rather than BeautifulSoup's freshly assigned plain NavigableString.
    return native.BeautifulSoup(str(soup),'lxml')


@pytest.mark.parametrize('parser',['lxml','html.parser'])
def test_literal_prefix_is_retained_without_correcting_raw_values_or_inferring_a_role(parser):
    soup=page();raw=str(soup).encode();before=str(soup)
    article=extract_article(raw,source(parser,42),page_url=native.URLS[0])
    assert json.loads(article['authors_json'])==[NAME]
    assert article['body']==native.PROSE+'\n原始小標題\n'+native.NOTICE
    assert article['published_at_raw']==native.PUB and article['modified_at_raw']==native.MOD
    notice,=[n for n in json.loads(article['verified_article_notices_json'])
        if n['kind']=='native_finance_literal_role_with_creator_witness']
    assert notice['native_credit']==SIGNATURE and notice['native_role']=='記埋'
    assert notice['same_article_creator_name_and_identifier_required']
    assert not notice['reporter_title_or_employment_inferred']
    assert not notice['prefix_corrected_or_source_values_rewritten'] and str(soup)==before
    assert SIGNATURE not in article['body'] and SIGNATURE in raw.decode()
    with pytest.raises(NewsError):extract_article(raw,source(parser,41),page_url=native.URLS[0])


@pytest.mark.parametrize('parser',['lxml','html.parser'])
@pytest.mark.parametrize('mutation',['missing_creator','foreign_creator','invalid_id','person_mismatch',
    'missing_schema','duplicate_schema','foreign_title','hidden','nested','prose','other_typo','no_image',
    'no_caption','duplicate_credit','no_body','foreign_url','paid'])
def test_typo_is_not_fuzzy_name_repair_or_an_identity_body_permission_fallback(parser,mutation):
    soup=page();credit=soup.select_one('#credit')
    if mutation=='missing_creator':native.update_schema(soup,creator=None)
    elif mutation=='foreign_creator':native.update_schema(soup,creator=['123-其他姓名','123'])
    elif mutation=='invalid_id':native.update_schema(soup,creator=['0-'+NAME,'0'])
    elif mutation=='person_mismatch':native.update_schema(soup,author={'@type':'Person','name':'其他姓名'})
    elif mutation=='missing_schema':soup.select_one('#report').decompose()
    elif mutation=='duplicate_schema':soup.head.append(deepcopy(soup.select_one('#report')))
    elif mutation=='foreign_title':native.update_schema(soup,headline='其他文章')
    elif mutation=='hidden':credit['hidden']=''
    elif mutation=='nested':credit.string.wrap(soup.new_tag('strong'))
    elif mutation=='prose':credit.append('這是額外的正文。')
    elif mutation=='other_typo':credit.string=SIGNATURE.replace('記埋','記理')
    elif mutation=='no_image':soup.select_one('.story img').decompose()
    elif mutation=='no_caption':soup.select_one('.story > p:nth-of-type(2)').string='正文不能當圖說'
    elif mutation=='duplicate_credit':credit.insert_after(deepcopy(credit))
    elif mutation=='no_body':
        for p in list(credit.find_next_siblings()):p.decompose()
    elif mutation=='foreign_url':soup.select_one('link[rel=canonical]')['href']=native.URLS[0]+'0'
    else:native.update_schema(soup,isAccessibleForFree=False)
    with pytest.raises(NewsError):extract_article(str(soup).encode(),source(parser,42),page_url=native.URLS[0])


def test_current_finance_typo_does_not_change_other_sites_or_old_successful_fields():
    for host in ('finance','www'):
        raw=str(native.page(host)).encode();url=native.URLS[host=='www']
        assert extract_article(raw,source(version=41),page_url=url)==extract_article(raw,source(version=42),page_url=url)
    soup=page();url=native.URLS[1]
    native.update_schema(soup,url=url,mainEntityOfPage=url)
    for selector,attribute in [('link[rel=canonical]','href'),('meta[property="og:url"]','content')]:
        soup.select_one(selector)[attribute]=url
    soup.select_one('meta[property="og:title"]')['content']=native.TITLE+' | ETtoday社會新聞 | ETtoday新聞雲'
    with pytest.raises(NewsError):extract_article(str(soup).encode(),source(version=42),page_url=url)


def test_v42_transition_preserves_all_heads_and_http_identities_without_an_automatic_requeue(tmp_path):
    cfg=native.config();cfg['providers'][0]['embedded_article_layout']='ettoday_visible_article_v41'
    old_registry=PolicyRegistry(cfg,base='.');root=tmp_path/'news';old=NewsQueue(root,old_registry)
    old.add_articles([dict(url=native.URLS[0])])
    with old.db:
        old.db.execute("UPDATE articles SET state='article_attribution_not_verified',error_code='article_attribution_not_verified',attempts=9,due=9999999999")
    head=dict(old.db.execute('SELECT * FROM articles').fetchone());old.close()
    cfg['providers'][0]['embedded_article_layout']='ettoday_visible_article_v42'
    registry=PolicyRegistry(cfg,base='.');new=NewsQueue(root,registry)
    try:
        assert dict(new.db.execute('SELECT * FROM articles').fetchone())==head
        current,before=registry.sources['ettoday'],old_registry.sources['ettoday']
        assert current.parser_repair_prior_capture_fingerprint==before.data_fingerprint!=current.data_fingerprint
        assert current.authorization_fingerprint==before.authorization_fingerprint
        assert current.http_capacity_source_fingerprint==before.http_capacity_source_fingerprint
        assert current.http_capacity_authorization_fingerprint==before.http_capacity_authorization_fingerprint
    finally:new.close()
