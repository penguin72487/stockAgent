"""Nested native-table correctness: structure is not concatenated numbers."""
import html
import json

from bs4 import BeautifulSoup
import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_publishers import _cnyes_nested_table_witness, _cnyes_table_grids
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE, mobile_page, streamed_mobile_page
from test_news_cnyes_html import BODY, CLOCK, URL


INNER = '<table style="width:557px"><tbody><tr><th>項目</th><th>原值</th></tr><tr><td>營收22</td><td>184</td></tr><tr><td>增減</td><td>-12.50</td></tr></tbody></table>'
TABLE = '<table border="1" cellpadding="1" cellspacing="1" style="width:100%"><tbody><tr><td>' + INNER + '</td></tr></tbody></table>'


def page(*, table=TABLE, current=None, schema=None, streamed=False):
    current = table if current is None else current
    body = BODY * 8 + BeautifulSoup(table, 'html.parser').get_text('', strip=True)
    builder = streamed_mobile_page if streamed else mobile_page
    payload = builder(body=body, changes={'content': html.escape('<p>' + BODY * 8 + '</p>' + current)},
                      schema_changes={'articleBody': body if schema is None else schema})
    return payload.replace(('<p>' + html.escape(body) + '</p>').encode(), ('<p>' + BODY * 8 + '</p>' + table).encode())


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('url', [MOBILE, MOBILE + '?exp=a', URL + '?exp=a'])
@pytest.mark.parametrize('streamed', [False, True])
@pytest.mark.parametrize('mixed', [False, True])
def test_nested_forest_keeps_whole_body_values_authors_and_clocks(cnyes_config, tmp_path, parser, url, streamed, mixed):
    cnyes_config['providers'][0]['html_parser'] = parser
    source = PolicyRegistry(cnyes_config, base=tmp_path).sources['cnyes']
    table = TABLE.replace('<td>' + INNER, '<td>前段123' + INNER).replace('</table></td>', '</table>後段456</td>') if mixed else TABLE
    # The retained original differs only in the outer cell's alignment style.
    visible = table.replace('<td>', '<td style="text-align: left;">', 1)
    raw = page(table=visible, current=table, streamed=streamed)
    result = extract_article(raw, source, page_url=url)
    assert BODY * 8 in result['body'] and '營收22\n184' in result['body'] and '-12.50' in result['body']
    assert result['published_at_raw'] == CLOCK and json.loads(result['authors_json']) == [AUTHOR]
    assert 'source_current_nested_table_cell_forest_comparison_v1' in result['article_identity_basis']
    if mixed:
        assert result['body'].index('前段123') < result['body'].index('營收22') < result['body'].index('後段456')


@pytest.mark.parametrize('parser', ['html.parser', 'lxml'])
@pytest.mark.parametrize('mutation', ['cell_collision', 'row_move', 'flatten', 'duplicate', 'th_to_td',
    'rowspan', 'colspan', 'headers', 'scope', 'abbr', 'summary', 'hide_table', 'hide_row',
    'hide_cell', 'hide_inline', 'inert', 'comment', 'script', 'unknown', 'empty_table',
    'text_outside_cell', 'schema_number_conflict'])
def test_same_compact_words_do_not_waive_structure_visibility_or_source_numbers(cnyes_config, tmp_path, parser, mutation):
    cnyes_config['providers'][0]['html_parser'] = parser
    source = PolicyRegistry(cnyes_config, base=tmp_path).sources['cnyes']
    current, visible, schema = TABLE, TABLE, None
    if mutation == 'cell_collision':
        current = TABLE.replace('營收22</td><td>184', '營收2</td><td>2184')
    elif mutation == 'row_move':
        current = TABLE.replace('營收22</td><td>184', '營收22</td></tr><tr><td>184')
    elif mutation == 'flatten':
        current = INNER
    elif mutation == 'duplicate':
        current += TABLE
    elif mutation == 'th_to_td':
        current = TABLE.replace('<th>', '<td>').replace('</th>', '</td>')
    elif mutation in ('rowspan', 'colspan', 'headers', 'scope', 'abbr'):
        current = TABLE.replace('<td>184', '<td ' + mutation + '="2">184')
    elif mutation == 'summary':
        current = TABLE.replace('<table style=', '<table summary="different context" style=')
    elif mutation.startswith('hide_'):
        tag = {'hide_table': 'table', 'hide_row': 'tr', 'hide_cell': 'td', 'hide_inline': 'span'}[mutation]
        if tag == 'span':
            visible = TABLE.replace('184', '<span style="display:none">184</span>')
        else:
            visible = TABLE.replace('<' + tag, '<' + tag + ' hidden', 1)
        current = visible
    elif mutation == 'inert':
        current = visible = TABLE.replace('<td>184', '<td inert>184')
    elif mutation == 'comment':
        current = visible = TABLE.replace('<td>184', '<td><!--unknown-->184')
    elif mutation in ('script', 'unknown'):
        tag = 'script' if mutation == 'script' else 'custom-unreviewed'
        current = visible = TABLE.replace('<td>184', '<td><' + tag + '></' + tag + '>184')
    elif mutation == 'empty_table':
        current = visible = TABLE.replace(INNER, '<table></table>')
    elif mutation == 'text_outside_cell':
        current = visible = TABLE.replace('<tbody>', '<tbody>額外123', 1)
    elif mutation == 'schema_number_conflict':
        schema = BODY * 8 + BeautifulSoup(TABLE, 'html.parser').get_text('', strip=True).replace('184', '999')
    with pytest.raises(NewsError):
        extract_article(page(table=visible, current=current, schema=schema), source, page_url=MOBILE)


def test_witness_is_nonmutating_and_does_not_relax_old_simple_grid(cnyes_config, tmp_path):
    source = PolicyRegistry(cnyes_config, base=tmp_path).sources['cnyes']
    value = BeautifulSoup(TABLE, source.html_parser)
    snapshot = str(value)
    witness = _cnyes_nested_table_witness(value, source)
    assert witness and str(value) == snapshot
    assert _cnyes_table_grids(value, source) is None
    assert _cnyes_nested_table_witness(BeautifulSoup(INNER, source.html_parser), source) is None


def test_simple_grid_hot_path_never_builds_nested_forest(cnyes_config, tmp_path, monkeypatch):
    source = PolicyRegistry(cnyes_config, base=tmp_path).sources['cnyes']
    def forbidden(*args):
        pytest.fail('ordinary table performed a nested forest walk')
    monkeypatch.setattr('downloader.news_fulltext_publishers._cnyes_nested_table_witness', forbidden)
    assert '營收22' in extract_article(page(table=INNER), source, page_url=MOBILE)['body']
