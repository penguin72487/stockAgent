"""No-events observations are sparse data, not missing text or made-up titles."""
from dataclasses import replace
import html
import json

import pytest

from downloader.news_fulltext_policy import NewsError, extract_article
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_html import CLOCK, TITLE, URL, short_page, source
from test_news_cnyes_image_outcomes import page as image_page


HEADER = '日期         公司名稱    代碼  理由      承銷價   備註'
LINES = HEADER + '\n' + '-' * 70 + '\n今日無資料'
FLAT = ''.join(LINES.split())


def bulletin(**options):
    schema = {'headline': '', 'articleBody': FLAT, **options.pop('schema', {})}
    change = {'title': '', 'content': html.escape('<p>' + LINES.replace('\n', '<br>') + '</p>'),
              **options.pop('change', {})}
    payload = short_page(body=FLAT, schema_change=schema, change=change, **options)
    payload = payload.replace(('<h1>' + TITLE + '</h1>').encode(), b'<h1></h1>')
    return payload.replace(('<p>' + FLAT + '</p>').encode(),
        ('<p>' + LINES.replace('\n', '<br>') + '</p>').encode())


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
def test_whole_explicit_no_events_bulletin_keeps_empty_title_original_clock_and_spacing(cnyes_config, tmp_path, parser):
    policy = replace(source(cnyes_config, tmp_path), html_parser=parser)
    result = extract_article(bulletin(), policy, page_url=URL)
    assert result['title'] == '' and result['title_basis'] == 'source_visible_title_empty_verified_numeric_article'
    assert result['body'] == LINES and result['published_at_raw'] == CLOCK
    assert result['extraction_method'] == 'cnyes_visible_untitled_no_events_bulletin_v1'
    assert 'source_explicit_no_events_bulletin_not_missing_data' in result['body_completeness']
    assert not result['historical_point_in_time']


def test_generic_site_og_title_is_not_an_invented_bulletin_title(cnyes_config, tmp_path):
    payload = bulletin().replace(b'</head>', '<meta property="og:title" content="鉅亨網的通用網站標題"></head>'.encode())
    result = extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)
    assert result['title'] == ''
    assert result['title_basis'] == 'source_visible_title_empty_verified_numeric_article'


def test_unrelated_heading_is_not_an_invented_bulletin_title(cnyes_config, tmp_path):
    payload = bulletin().replace(b'<body>', '<body><aside><h1>這是其他文章的標題</h1></aside>'.encode())
    result = extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)
    assert result['title'] == '' and result['body'] == LINES


@pytest.mark.parametrize('options', [
    {'schema': {'headline': None}}, {'schema': {'headline': 'another title'}},
    {'schema': {'articleBody': FLAT + '真實新增資料'}},
    {'change': {'title': 'another title'}}, {'change': {'payment': 1}},
    {'change': {'payment': False}}, {'change': {'publishAt': 1388534401}},
    {'change': {'newsId': 43}}, {'change': {'url': URL.replace('/42', '/43')}},
    {'change': {'content': '<p>來源有其他資料123</p>'}},
    {'canonical': URL.replace('/42', '/43')}, {'conflict': True}, {'duplicate_key': True},
])
def test_sparse_observation_does_not_waive_identity_free_access_or_whole_content(cnyes_config, tmp_path, options):
    with pytest.raises(NewsError):
        extract_article(bulletin(**options), source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('change', ['hidden_body', 'hidden_header', 'extra_visible', 'image', 'video', 'comment',
    'changed_header', 'changed_separator', 'changed_absence', 'missing_header'])
def test_no_events_is_a_complete_visible_statement_not_a_heuristic(cnyes_config, tmp_path, change):
    payload = bulletin()
    if change == 'hidden_body':
        payload = payload.replace(b'<p>', b'<p hidden>', 1)
    elif change == 'hidden_header':
        payload = payload.replace(b'<h1>', b'<h1 hidden>')
    elif change == 'extra_visible':
        payload = payload.replace(b'</div></article>', '額外公告123</div></article>'.encode())
    elif change in {'image', 'video', 'comment'}:
        extra = {'image': '<img src="https://source.invalid/x">', 'video': '<video></video>',
                 'comment': '<script>unknown()</script>'}[change]
        payload = payload.replace(b'</div></article>', (extra + '</div></article>').encode())
    elif change == 'changed_header':
        payload = payload.replace('承銷價'.encode(), '成交價'.encode())
    elif change == 'changed_separator':
        payload = payload.replace(('-' * 70).encode(), ('-' * 69).encode())
    elif change == 'changed_absence':
        payload = payload.replace('今日無資料'.encode(), '今日有資料'.encode())
    else:
        payload = payload.replace(b'<h1></h1>', b'')
    with pytest.raises(NewsError):
        extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('body', ['b', 'XXXX', '今日無資料', '這是沒有標題的其他未知短文。'])
def test_other_empty_title_short_text_is_not_accepted(cnyes_config, tmp_path, body):
    payload = short_page(body=body, change={'title': ''}, schema_change={'headline': ''})
    payload = payload.replace(('<h1>' + TITLE + '</h1>').encode(), b'<h1></h1>')
    with pytest.raises(NewsError):
        extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
def test_source_declared_signature_preserves_image_only_outcome_without_name_replacement(cnyes_config, tmp_path, parser):
    signature = '鉅亨網記者來源原文署名 台北'
    payload = image_page(changes={'signature': signature})
    # The actual source's named schema/current author differs from its own
    # visible/current signature. Prove both fields independently; never fix
    # the spelling or treat caption/alt as an article body.
    from test_news_cnyes_canonical_alias import AUTHOR
    payload = payload.replace(('鉅亨網記者' + AUTHOR + ' 台北').encode(), signature.encode())
    with pytest.raises(NewsError, match='^source_body_image_only$'):
        extract_article(payload, replace(source(cnyes_config, tmp_path), html_parser=parser), page_url=URL)


@pytest.mark.parametrize('signature', [None, '', '其他非記者署名', '鉅亨網記者另一名字 台北'])
def test_signature_alias_is_not_guessed_from_nearby_authors(cnyes_config, tmp_path, signature):
    from test_news_cnyes_canonical_alias import AUTHOR
    payload = image_page(changes={'signature': signature})
    payload = payload.replace(('鉅亨網記者' + AUTHOR + ' 台北').encode(), '鉅亨網記者不同署名 台北'.encode())
    with pytest.raises(NewsError) as caught:
        extract_article(payload, source(cnyes_config, tmp_path), page_url=URL)
    assert caught.value.code != 'source_body_image_only'


def test_signature_must_be_unambiguous_across_repeated_current_declarations(cnyes_config, tmp_path):
    from bs4 import BeautifulSoup
    from downloader.news_fulltext_publishers import _cnyes_current_article_data
    payload = image_page(changes={'signature': '鉅亨網記者甲乙 台北'})
    soup = BeautifulSoup(payload, 'lxml')
    scripts = soup.find_all('script', src=False)
    for script in scripts:
        if 'self.__next_f.push' in script.get_text() and 'signature' in script.get_text():
            duplicate = script.get_text().replace('鉅亨網記者甲乙 台北', '鉅亨網記者丙丁 台北')
            assert duplicate != script.get_text()
            # Reparse real source bytes. Assigning .string on a new BS tag
            # produces NavigableString, not the parser's Script subclass,
            # and get_text() correctly excludes that synthetic text node.
            soup = BeautifulSoup(payload + ('<script>' + duplicate + '</script>').encode(), 'lxml')
            break
    assert _cnyes_current_article_data(soup, 42, verify_video=True, verify_author=True,
                                     verify_signature=True) is None


def test_new_html_proofs_do_not_change_bulk_or_http_capacity_contract(cnyes_config, tmp_path, monkeypatch):
    import hashlib
    current = source(cnyes_config, tmp_path)
    real_sha256 = hashlib.sha256
    def without_new_proofs(value=b'', *args, **kwargs):
        try:
            payload = json.loads(value)
        except (ValueError, TypeError, UnicodeDecodeError):
            return real_sha256(value, *args, **kwargs)
        if isinstance(payload, dict) and 'verified_untitled_no_events_bulletin_contract_version' in payload:
            payload.pop('verified_untitled_no_events_bulletin_contract_version')
            payload.pop('verified_source_signature_image_outcome_contract_version')
            value = json.dumps(payload, sort_keys=True).encode()
        return real_sha256(value, *args, **kwargs)
    monkeypatch.setattr('downloader.news_fulltext_policy.hashlib.sha256', without_new_proofs)
    previous = source(cnyes_config, tmp_path)
    assert current.data_fingerprint != previous.data_fingerprint
    assert current.parser_repair_fingerprint != previous.parser_repair_fingerprint
    assert current.http_capacity_source_fingerprint == previous.http_capacity_source_fingerprint
    assert current.cnyes_bulk_data_fingerprint == previous.cnyes_bulk_data_fingerprint
    assert current.cnyes_index_data_fingerprint == previous.cnyes_index_data_fingerprint
