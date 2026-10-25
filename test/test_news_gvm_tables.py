"""Visible written-table cells remain original prose, not excluded widgets."""
from copy import deepcopy
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_gvm import (
    GVM_TABLE_LAYOUT, GVM_V3_VERIFICATION, GVM_V4_VERIFICATION,
)
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import config
from test_news_gvm_known_urls import authored, extract
from test_news_gvm_media import media_entry, media_page
from test_news_gvm_native import MODIFIED, PUBLISHED, TEXT, URL, policy, schema


TABLE = ('<div style="width:100%"><div style="overflow-x:auto">'
    '<table style="border-collapse:collapse"><thead><tr><th colspan="2" style="text-align:center">合成表格標題</th></tr>'
    '<tr><th>合成欄位</th><th>原始值</th></tr></thead><tbody><tr>'
    '<td style="color:black"><span>合成項目</span></td><td style="color:black">-1.25</td></tr>'
    '<tr><td>合成比率</td><td>1.00%</td></tr></tbody></table></div>'
    '<div style="text-align:left">合成圖表註解與來源</div></div>')


def table_entry():
    return {**media_entry(), 'embedded_article_layout': GVM_TABLE_LAYOUT,
        'adapter_verification': GVM_V4_VERIFICATION}


def table_page(markup=TABLE):
    soup = media_page()
    fragment = BeautifulSoup('<section>' + markup + '</section>', 'lxml')
    for node in list(fragment.section.contents):
        soup.select_one('.article-content').append(node)
    return soup


def test_table_headers_cells_caption_order_credit_and_clocks_survive():
    result = extract(table_page(), policy(table_entry()))
    assert TEXT in result['body'] and result['published_at_raw'] == PUBLISHED
    assert result['modified_at_raw'] == MODIFIED
    terms = ['合成表格標題', '合成欄位', '原始值', '合成項目', '-1.25', '合成比率', '1.00%', '合成圖表註解與來源']
    assert [result['body'].index(text) for text in terms] == sorted(result['body'].index(text) for text in terms)
    assert all(result['body'].count(text) == 1 for text in terms)
    assert result['authors_json'] == extract(authored('合成作者'))['authors_json']
    notices = json.loads(result['verified_article_notices_json'])
    assert notices['native_editor_table_prose_grammar_version'] == 1
    assert not notices['media_or_scripts_requested_or_executed']
    assert json.loads(result['author_attribution_detail_json'])['contract_version'] == 4
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(table_page(), policy(media_entry()))


def test_media_only_and_plain_prose_are_byte_identical():
    old, new = extract(media_page(), policy(media_entry())), extract(media_page(), policy(table_entry()))
    for key in ('body', 'body_sha256', 'authors_json', 'title', 'published_at_raw', 'modified_at_raw'):
        assert old[key] == new[key]


@pytest.mark.parametrize('markup', [
    TABLE.replace('width:100%', 'display:none'),
    TABLE.replace('border-collapse:collapse', 'visibility:hidden'),
    TABLE.replace('<table ', '<table hidden '),
    TABLE.replace('<th>', '<th aria-hidden="true">', 1),
    TABLE.replace('<div style="width:100%">', '<div id="other">'),
    TABLE.replace('<table ', '<table onclick="anything()" '),
    TABLE.replace('<tbody>', '<tbody data-loader="remote">'),
    TABLE.replace('colspan="2"', 'colspan="0"'),
    TABLE.replace('colspan="2"', 'colspan="many"'),
    TABLE.replace('<td>合成比率</td>', '<td><table><tr><td>nested</td></tr></table></td>'),
    TABLE.replace('</table>', '<button>remote loader</button></table>'),
    TABLE.replace('</table>', '<script>anything()</script></table>'),
    TABLE.replace('</table>', '<iframe src="https://example.com"></iframe></table>'),
    '<div style="width:100%"><p>ordinary unreviewed wrapper</p></div>',
    '<div style="width:100%"><table></table></div>',
])
def test_unknown_hidden_dynamic_and_unanchored_structures_still_fail(markup):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(table_page(markup), policy(table_entry()))


@pytest.mark.parametrize('mutate,error', [
    (lambda s: s.select_one('.article-head').__setitem__('data-is-accessible', '0'), 'paywall_or_login'),
    (lambda s: s.select_one('link[rel=canonical]').__setitem__('href', URL + '9'), 'article_identity_not_verified'),
    (lambda s: schema(s, lambda a: a['author'][0].update(name='different')), 'article_rights_or_scope_excluded'),
    (lambda s: s.select_one('.article-content > p').append('本文轉載自別家媒體'), 'article_rights_or_scope_excluded'),
    (lambda s: s.select_one('meta[property="article:published_time"]').__setitem__('content', 'unknown'), 'invalid_bulk_publication_clock'),
])
def test_written_tables_cannot_bypass_earlier_access_credit_identity_or_time(mutate, error):
    soup = table_page()
    mutate(soup)
    with pytest.raises(NewsError, match=error):
        extract(soup, policy(table_entry()))


def test_only_exact_predecessor_inherits_http_capacity_and_repair_identity():
    old_entry = {**media_entry(), 'adapter_verification': GVM_V3_VERIFICATION}
    old, new = policy(old_entry), policy(table_entry())
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    for key, value in [('interval_seconds', 5), ('parallelism', 2)]:
        changed = policy({**table_entry(), key: value})
        assert changed.http_capacity_source_fingerprint != old.http_capacity_source_fingerprint
        assert changed.parser_repair_prior_capture_fingerprint != old.data_fingerprint


def test_table_reconciliation_does_not_bulk_reset_any_heads(config, tmp_path):
    from downloader.news_fulltext_policy import PolicyRegistry
    settings = deepcopy(config)
    settings['providers'] = [{**media_entry(), 'adapter_verification': GVM_V3_VERIFICATION}]
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    states = ['complete', 'article_identity_not_verified', 'article_rights_or_scope_excluded', 'paywall_or_login', 'retry']
    for index, state in enumerate(states):
        url = URL + str(index)
        queue.add_articles([{'url': url}])
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=? WHERE url=?',
                (state, 'http_429' if state == 'retry' else state, 10**10, url))
    before = [dict(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')]
    queue.close()
    settings['providers'] = [table_entry()]
    queue = NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))
    try:
        assert [dict(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')] == before
    finally:
        queue.close()
