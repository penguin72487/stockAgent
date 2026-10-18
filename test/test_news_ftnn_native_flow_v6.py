"""Native charset/editor-flow repairs retain the entire source report."""
from contextlib import closing
from dataclasses import replace
import json
from pathlib import Path

import pytest

from downloader.news_fulltext_ftnn import ftnn_visible_article
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import (
    FTNN_NATIVE_V6_REPAIR_COHORT, NewsQueue, ftnn_native_v6_parser_repair, verify_bodies,
)
from test_news_ftnn import NOTICE, PROSE, URL, page
from test_news_ftnn_empty_spans_v3 import seed_failure
from test_news_ftnn_short_prose_v5 import candidate_config as prior_config, short_page, SHORT


KINDS = ('charset', 'one_section_embed', 'two_sections_ads', 'split_report', 'one_section')
ATTRS = {'data-scroll-anchor': 'false', 'data-testid': 'conversation-turn-2', 'data-turn': 'assistant',
         'data-turn-id': 'source-editor', 'data-turn-id-container': 'source-editor', 'dir': 'auto'}
TAIL = ('外層原始數值-2.5%、456股與比率0.25，不可因版型截掉。',
        '第二個外層段落同樣屬於完整報導，不是相關新聞。',
        '第三個外層段落保留來源數字與文字。')


def candidate_config(parser='lxml'):
    config = prior_config(parser)
    config['providers'][0]['embedded_article_layout'] = 'ftnn_visible_article_v6'
    return config


def policy(parser='lxml', version=6):
    config = candidate_config(parser)
    config['providers'][0]['embedded_article_layout'] = f'ftnn_visible_article_v{version}'
    return PolicyRegistry(config, base=Path.cwd()).sources['ftnn']


def layout_page(kind, parser='lxml'):
    soup = page(parser)
    body = soup.select_one('.news-body')
    if kind == 'charset':
        body.insert(0, soup.new_tag('meta', charset='UTF-8'))
        return soup
    empty = soup.new_tag('section', attrs=ATTRS)
    flow_attrs = {key: value for key, value in ATTRS.items()
                  if kind != 'one_section' or key != 'data-scroll-anchor'}
    flow = soup.new_tag('section', attrs=flow_attrs)
    notice = body.select(':scope > p')[-1].extract()
    if kind == 'split_report':
        credit, opening = body.select(':scope > p')[:2]
        flow.append(credit.extract())
        flow.append(opening.extract())
        photo = body.figure.extract()
        body.clear()
        body.extend([empty, flow, photo])
        for index, text in enumerate(TAIL):
            paragraph = soup.new_tag('p')
            paragraph.string = text
            body.append(paragraph)
            if index in (0, 2):
                advertisement = soup.new_tag('p')
                advertisement.string = '廣告 更多內容請繼續往下閱讀'
                body.append(advertisement)
                for offset in (0, 1):
                    body.append(soup.new_tag('div', id=f'div-gpt-ad-{123 + index}-{offset}'))
        body.append(notice)
        return soup
    for child in list(body.children):
        flow.append(child.extract())
    if kind == 'two_sections_ads':
        body.extend([empty, flow])
        for offset in (0, 1):
            body.append(soup.new_tag('div', id=f'div-gpt-ad-456-{offset}'))
    else:
        body.append(flow)
        if kind == 'one_section_embed':
            body.append(soup.new_tag('iframe', src='https://embed.invalid/original'))
    body.append(notice)
    return soup


def extract(soup, parser='lxml', version=6):
    return extract_article(str(soup).encode(), policy(parser, version), page_url=URL)


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('kind', KINDS)
def test_native_shapes_keep_whole_original_values_outer_paragraphs_captions_and_clocks(parser, kind):
    soup = layout_page(kind, parser)
    original = str(soup)
    result = extract(soup, parser)
    assert PROSE in result['body'] and NOTICE in result['body']
    assert '廣告 更多內容請繼續往下閱讀' not in result['body']
    assert '記者黃詩雯' not in result['body']
    assert '獨立照片說明' not in result['body']
    assert json.loads(result['verified_image_captions_json']) == ['獨立照片說明／原始出處']
    assert result['authors_json'] == '["黃詩雯"]'
    assert result['published_at_raw'] == '2026-10-14T22:00:15+08:00'
    assert 'complete_editor_flow_v7' in result['article_identity_basis']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    if kind == 'split_report':
        assert all(text in result['body'] for text in TAIL)
    if kind == 'charset':
        _, cleaned = ftnn_visible_article(soup, page_url=URL, adapter_contract_version=6)
        assert cleaned.select_one('.news-body > meta')['charset'].lower() == 'utf-8'
    assert str(soup) == original
    for old in range(1, 6):
        with pytest.raises(NewsError):
            extract(soup, parser, old)


@pytest.mark.parametrize('kind', KINDS)
def test_both_parsers_agree_on_every_output_field(kind):
    raw = str(layout_page(kind)).encode()
    assert extract_article(raw, policy('lxml'), page_url=URL) == extract_article(
        raw, policy('html.parser'), page_url=URL)


@pytest.mark.parametrize('change', ['encoding', 'refresh', 'unknown_attribute', 'hidden', 'two_meta',
    'meta_photo', 'literal_prefix', 'unknown_prefix', 'later_credit', 'hidden_parent', 'third_party'])
def test_charset_is_not_permission_to_skip_unknown_prefixes_hidden_text_or_unproved_credits(change):
    soup = layout_page('charset')
    body, meta = soup.select_one('.news-body'), soup.select_one('.news-body > meta')
    if change == 'encoding':
        meta['charset'] = 'big5'
    elif change == 'refresh':
        meta['http-equiv'] = 'refresh'
        meta['content'] = '0;url=https://other.invalid/'
    elif change == 'unknown_attribute':
        meta['data-other-source'] = 'unknown'
    elif change == 'hidden':
        meta['hidden'] = ''
    elif change == 'two_meta':
        meta.insert_after(soup.new_tag('meta', charset='UTF-8'))
    elif change == 'meta_photo':
        meta.insert_after(body.figure.extract())
    elif change == 'literal_prefix':
        meta.insert_after('不能略過的前文')
    elif change == 'unknown_prefix':
        meta.insert_after(soup.new_tag('div'))
    elif change == 'later_credit':
        paragraph = soup.new_tag('p')
        paragraph.string = '不能刪掉前段來找後面的署名。'
        meta.insert_after(paragraph)
    elif change == 'hidden_parent':
        body['aria-hidden'] = 'true'
    else:
        body.select_one(':scope > p').string = '鏡週刊報導'
    with pytest.raises(NewsError):
        extract(soup)


@pytest.mark.parametrize('kind', KINDS[1:])
@pytest.mark.parametrize('change', ['unknown_attribute', 'unknown_dir', 'nested_section', 'hidden_section',
    'extra_section', 'literal_prefix', 'later_credit', 'third_party', 'hidden_outer_notice'])
def test_editor_layout_is_not_a_descendant_search_or_body_truncation_fallback(kind, change):
    soup = layout_page(kind)
    body = soup.select_one('.news-body')
    flow = next(node for node in body.select(':scope > section') if node.get_text(strip=True))
    if change == 'unknown_attribute':
        flow['data-other-source'] = 'unknown'
    elif change == 'unknown_dir':
        flow['dir'] = 'unknown'
    elif change == 'nested_section':
        flow.append(soup.new_tag('section'))
    elif change == 'hidden_section':
        flow['hidden'] = ''
    elif change == 'extra_section':
        body.append(soup.new_tag('section', attrs=ATTRS))
    elif change == 'literal_prefix':
        body.insert(0, '不可忽略的前文')
    elif change == 'later_credit':
        paragraph = soup.new_tag('p')
        paragraph.string = '不能刪除前段以搜尋後面署名。'
        flow.insert(0, paragraph)
    elif change == 'third_party':
        flow.select_one(':scope > p').string = '中央社記者黃詩雯／綜合報導'
    else:
        body.select_one(':scope > p:last-of-type')['hidden'] = ''
    with pytest.raises(NewsError):
        extract(soup)


@pytest.mark.parametrize('kind', ['two_sections_ads', 'split_report'])
@pytest.mark.parametrize('change', ['text', 'image', 'paragraph', 'hidden'])
def test_empty_editor_prefix_must_actually_be_empty(kind, change):
    soup = layout_page(kind)
    empty = soup.select_one('.news-body > section')
    if change == 'text':
        empty.string = '不可忽略的原始文字'
    elif change == 'hidden':
        empty['hidden'] = ''
    else:
        empty.append(soup.new_tag('img' if change == 'image' else 'p'))
    with pytest.raises(NewsError):
        extract(soup)


def test_prior_short_original_is_still_supported_not_reclassified_as_missing():
    result = extract(short_page())
    assert result['body'] == SHORT
    assert 'whole_native_short_plain_prose_verified' in result['body_completeness']


def test_exact_prior_capture_preserves_http_refusal_and_authorization_identity():
    old, new = policy(version=5), policy()
    assert new.data_fingerprint != old.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint
    assert ftnn_native_v6_parser_repair(new, old.data_fingerprint)
    for previous in (None, new.data_fingerprint, old.http_capacity_source_fingerprint, 'a' * 64):
        assert not ftnn_native_v6_parser_repair(new, previous)
    assert not ftnn_native_v6_parser_repair(replace(new, allowed=False), old.data_fingerprint)
    assert not ftnn_native_v6_parser_repair(old, old.data_fingerprint)


def test_only_eleven_inspected_missing_heads_reopen_once_without_replaying_old_repairs(tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(prior_config(), base=tmp_path))) as queue:
        for url, error in FTNN_NATIVE_V6_REPAIR_COHORT.items():
            seed_failure(queue, url, error)
        for suffix, state in [('254694', 'complete'), ('520952', 'complete'), ('561059', 'complete'),
            ('547631', 'complete'), ('512507', 'article_attribution_not_verified'),
            ('532187', 'article_attribution_not_verified'), ('513796', 'article_attribution_not_verified'),
            ('42', 'body_not_found'), ('43', 'http_403')]:
            seed_failure(queue, f'https://www.ftnn.com.tw/news/{suffix}', state)
        queue.db.commit()
        before = {row['url']: dict(row) for row in queue.db.execute('SELECT * FROM articles')}
    registry = PolicyRegistry(candidate_config(), base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        for row in queue.db.execute('SELECT * FROM articles'):
            expected = before[row['url']]
            assert dict(row) == ({**expected, 'state': 'retry'}
                                 if row['url'] in FTNN_NATIVE_V6_REPAIR_COHORT else expected)
        marker = 'ftnn_native_layout_recheck_v6:' + registry.sources['ftnn'].data_fingerprint
        receipt = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0]
        proof = json.loads(receipt)
        assert {row['url'] for row in proof['rows']} == set(FTNN_NATIVE_V6_REPAIR_COHORT)
        assert len(proof['rows']) == 11 and proof['attempts_due_error_and_observation_preserved']
        assert not proof['accepted_as_bodies']
        queue.db.execute('UPDATE articles SET state=error_code')
        queue.db.commit()
        after = [dict(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')]
    with closing(NewsQueue(tmp_path, registry)) as queue:
        assert [dict(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY id')] == after
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == receipt


@pytest.mark.parametrize('change', ['body', 'path', 'lease_owner', 'lease_until', 'missing_clock',
    'naive_clock', 'future_clock', 'wrong_error', 'unknown_capture', 'changed_http', 'query'])
def test_unproved_generations_existing_heads_and_leases_do_not_reopen(tmp_path, change):
    url = next(iter(FTNN_NATIVE_V6_REPAIR_COHORT)) + ('?unknown=1' if change == 'query' else '')
    with closing(NewsQueue(tmp_path, PolicyRegistry(prior_config(), base=tmp_path))) as queue:
        seed_failure(queue, url, 'article_attribution_not_verified')
        columns = {'body': ('body_sha256', 'a' * 64), 'path': ('parquet_path', 'old.parquet'),
            'lease_owner': ('lease_owner', 'active-owner'), 'lease_until': ('lease_until', 4102444800),
            'missing_clock': ('last_checked_at_utc', None), 'naive_clock': ('last_checked_at_utc', '2026-10-17T21:00:00'),
            'future_clock': ('last_checked_at_utc', '2099-01-01T00:00:00+00:00'), 'wrong_error': ('error_code', 'http_403')}
        if change in columns:
            column, value = columns[change]
            queue.db.execute(f'UPDATE articles SET {column}=? WHERE url=?', (value, url))
        elif change == 'unknown_capture':
            queue.db.execute("UPDATE queue_settings SET value=? WHERE key='source_capture_fingerprint:ftnn'", ('a' * 64,))
        queue.db.commit()
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
    config = candidate_config()
    if change == 'changed_http':
        config['providers'][0]['interval_seconds'] = 3
    with closing(NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == before


def test_verified_source_flows_through_shared_writer_and_survives_restart(tmp_path):
    from downloader.news_fulltext import fetch_article
    from test_news_fulltext import FakeResponse, http_fixture

    raw = str(layout_page('split_report')).encode()
    http, _ = http_fixture(candidate_config(), tmp_path, {
        'https://www.ftnn.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'), URL: FakeResponse(raw)})
    try:
        with closing(NewsQueue(tmp_path, http.registry)) as queue:
            queue.add_articles([{'url': URL}])
            record = fetch_article(http, queue.claim_url(URL, 300))
            assert all(text in record['body'] for text in TAIL) and record['raw_response_body'] == raw
            assert not record['publish'] and not record['remote_training_export']
            queue.accept([record])
            before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
        with closing(NewsQueue(tmp_path, http.registry)) as queue:
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before
            assert verify_bodies(tmp_path)['accepted']
    finally:
        http.close()


@pytest.mark.parametrize('kind', KINDS)
def test_v6_failure_observations_retain_only_metadata_not_raw_or_unverified_prose(tmp_path, kind):
    from downloader.news_fulltext_http import Response
    from test_news_failure_diagnostics import fail
    from test_news_failure_identity_metadata import evidence

    with closing(NewsQueue(tmp_path, PolicyRegistry(candidate_config(), base=tmp_path))) as queue:
        fail(queue, Response(URL, 200, {'content-type': 'text/html'}, str(layout_page(kind)).encode()),
             code='article_identity_not_verified', received_at_utc='2026-10-17T21:35:00+00:00')
        paths, records = evidence(tmp_path)
        assert len(records) == 1 and records[0]['facts']['ftnn_native_validation']['contract_version'] == 7
        assert not records[0]['raw_response_retained'] and not records[0]['accepted_as_article']
        assert PROSE not in paths[0].read_text() and all(text not in paths[0].read_text() for text in TAIL)
