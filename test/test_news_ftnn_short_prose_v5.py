"""Native whole short FTNN originals, not captions, teasers or blanket minima."""
from contextlib import closing
from dataclasses import replace
import hashlib
import json
from pathlib import Path

from bs4 import Comment
import pytest

from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import FTNN_NATIVE_V5_REPAIR_COHORT, NewsQueue, ftnn_native_v5_parser_repair, verify_bodies
from test_news_ftnn import NOTICE, TITLE, URL, page
from test_news_ftnn_empty_spans_v3 import seed_failure
from test_news_ftnn_hidden_asset_v4 import candidate_config as prior_config


SHORT = '公司公告原始數值為負值-12.5%、123456股及比率0.75，各項業務仍依既有規劃持續營運。'


def candidate_config(parser='lxml'):
    config = prior_config(parser)
    config['providers'][0]['embedded_article_layout'] = 'ftnn_visible_article_v5'
    return config


def policy(parser='lxml', version=5):
    config = candidate_config(parser)
    config['providers'][0]['embedded_article_layout'] = f'ftnn_visible_article_v{version}'
    return PolicyRegistry(config, base=Path.cwd()).sources['ftnn']


def short_page(parser='lxml', text=SHORT):
    soup = page(parser)
    body = soup.select_one('.news-body')
    body.select(':scope > p')[1].string = text
    padding = body.select(':scope > p')[-1]
    padding.clear()
    padding.append(soup.new_tag('br'))
    padding.append('\xa0')
    return soup


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_complete_native_short_report_retains_exact_original_values_and_caption_separately(parser):
    soup = short_page(parser)
    original = str(soup)
    result = extract_article(original.encode(), policy(parser), page_url=URL)
    assert result['body'] == SHORT and result['body_sha256'] == hashlib.sha256(SHORT.encode()).hexdigest()
    assert len(result['body']) < 100 and result['title'] == TITLE
    assert json.loads(result['verified_image_captions_json']) == ['獨立照片說明／原始出處']
    assert 'whole_native_short_plain_prose_verified_not_summary_or_caption' in result['body_completeness']
    assert result['published_at_raw'] == '2026-10-14T22:00:15+08:00'
    assert result['modified_at_raw'] == '2026-10-14T22:01:00+08:00'
    assert result['authors_json'] == '["黃詩雯"]'
    assert str(soup) == original and not result['historical_point_in_time']


def test_all_output_values_match_between_parsers_not_only_body_hash():
    raw = str(short_page()).encode()
    assert extract_article(raw, policy(), page_url=URL) == extract_article(raw, policy('html.parser'), page_url=URL)


@pytest.mark.parametrize('version', [1, 2, 3, 4])
def test_old_body_minimum_contracts_remain_reproducible(version):
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(str(short_page()).encode(), policy(version=version), page_url=URL)


@pytest.mark.parametrize('change', ['caption_only', 'reminder_only', 'headline_only', 'linked_teaser',
    'styled_teaser', 'unknown_attributes', 'inline_fragment', 'literal_sibling', 'nested_prose',
    'iframe', 'hidden_paragraph', 'third_party_credit', 'no_sentence', 'ascii_placeholder', 'empty'])
def test_short_length_is_not_a_bypass_for_nonreports_or_unknown_shapes(change):
    soup = short_page()
    body, prose = soup.select_one('.news-body'), soup.select('.news-body > p')[1]
    if change == 'caption_only':
        prose.decompose()
    elif change == 'reminder_only':
        prose.string = NOTICE
    elif change == 'headline_only':
        prose.string = TITLE + '。'
    elif change == 'linked_teaser':
        prose.clear()
        link = soup.new_tag('a', href='https://example.invalid/continue')
        link.string = SHORT
        prose.append(link)
    elif change in {'styled_teaser', 'unknown_attributes'}:
        prose['style' if change == 'styled_teaser' else 'data-summary'] = 'display:block'
    elif change == 'inline_fragment':
        prose.clear()
        inline = soup.new_tag('strong')
        inline.string = SHORT
        prose.append(inline)
    elif change == 'literal_sibling':
        body.append('未知相鄰片段')
    elif change == 'nested_prose':
        wrapper = soup.new_tag('div')
        prose.wrap(wrapper)
    elif change == 'iframe':
        body.append(soup.new_tag('iframe', src='https://example.invalid/video'))
    elif change == 'hidden_paragraph':
        prose['hidden'] = ''
    elif change == 'third_party_credit':
        body.select_one(':scope > p').string = '中央社記者黃詩雯／綜合報導'
    elif change == 'no_sentence':
        prose.string = SHORT.rstrip('。')
    elif change == 'ascii_placeholder':
        prose.string = 'only placeholder.'
    else:
        prose.string = ''
    with pytest.raises(NewsError):
        extract_article(str(soup).encode(), policy(), page_url=URL)


def test_native_scope_identity_and_clocks_are_not_waived_for_short_text():
    for source in (replace(policy(), allow_short_configured_body=False),):
        assert extract_article(str(short_page()).encode(), source, page_url=URL)['body'] == SHORT
    soup = short_page()
    soup.select_one('link[rel=canonical]')['href'] = URL + '0'
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(str(soup).encode(), policy(), page_url=URL)


def test_generic_short_body_flag_cannot_accept_an_unknown_native_teaser():
    soup = short_page()
    soup.select('.news-body > p')[1]['data-summary'] = 'unknown'
    with pytest.raises(NewsError, match='body_not_found'):
        extract_article(str(soup).encode(), replace(policy(), allow_short_configured_body=True), page_url=URL)


@pytest.mark.parametrize('change', ['text', 'attributes', 'hidden', 'nested', 'two_breaks', 'comment'])
def test_empty_editor_padding_is_not_an_arbitrary_empty_subtree_exception(change):
    soup = short_page()
    padding = soup.select('.news-body > p')[-1]
    br = padding.br
    if change == 'text':
        padding.append('不可刪除的原文。')
    elif change == 'attributes':
        br['data-content'] = 'unknown'
    elif change == 'hidden':
        br['hidden'] = ''
    elif change == 'nested':
        br.wrap(soup.new_tag('span'))
    elif change == 'two_breaks':
        padding.append(soup.new_tag('br'))
    else:
        padding.append(soup.new_string(' ', Comment))
    with pytest.raises(NewsError):
        extract_article(str(soup).encode(), policy(), page_url=URL)


def test_only_exact_prior_capture_can_repair_without_changing_http_or_authority():
    old = PolicyRegistry(prior_config(), base=Path.cwd()).sources['ftnn']
    new = policy()
    assert old.data_fingerprint != new.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint
    assert ftnn_native_v5_parser_repair(new, old.data_fingerprint)
    for previous in (None, new.data_fingerprint, old.http_capacity_source_fingerprint, 'a' * 64):
        assert not ftnn_native_v5_parser_repair(new, previous)
    assert not ftnn_native_v5_parser_repair(replace(new, allowed=False), old.data_fingerprint)
    assert not ftnn_native_v5_parser_repair(old, old.data_fingerprint)


def test_one_missing_short_body_reopens_once_and_never_replays_completed_repairs(tmp_path):
    target = next(iter(FTNN_NATIVE_V5_REPAIR_COHORT))
    with closing(NewsQueue(tmp_path, PolicyRegistry(prior_config(), base=tmp_path))) as queue:
        seed_failure(queue, target, 'body_not_found')
        for suffix, state in [('520952', 'complete'), ('561059', 'complete'), ('547631', 'complete'),
                              ('512507', 'article_rights_or_scope_excluded'), ('42', 'article_attribution_not_verified'),
                              ('43', 'body_not_found'), ('44', 'http_403')]:
            seed_failure(queue, f'https://www.ftnn.com.tw/news/{suffix}', state)
        queue.db.commit()
        before = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
    registry = PolicyRegistry(candidate_config(), base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        for row in queue.db.execute('SELECT * FROM articles'):
            expected = before[row['url']]
            assert dict(row) == ({**expected, 'state': 'retry'} if row['url'] == target else expected)
        marker = 'ftnn_native_layout_recheck_v5:' + registry.sources['ftnn'].data_fingerprint
        receipt = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0]
        proof = json.loads(receipt)
        assert [r['url'] for r in proof['rows']] == [target]
        assert proof['attempts_due_error_and_observation_preserved'] and not proof['accepted_as_bodies']
        queue.db.execute('UPDATE articles SET state=error_code WHERE url=?', (target,))
        queue.db.commit()
        after = [dict(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY id')]
    with closing(NewsQueue(tmp_path, registry)) as queue:
        assert [dict(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY id')] == after
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == receipt


@pytest.mark.parametrize('change', ['body', 'path', 'lease', 'missing_clock', 'naive_clock', 'future_clock',
    'wrong_error', 'unknown_capture', 'changed_http', 'query'])
def test_unknown_generations_existing_heads_and_leases_never_reopen(tmp_path, change):
    url = next(iter(FTNN_NATIVE_V5_REPAIR_COHORT)) + ('?unproved=1' if change == 'query' else '')
    with closing(NewsQueue(tmp_path, PolicyRegistry(prior_config(), base=tmp_path))) as queue:
        seed_failure(queue, url, 'body_not_found')
        columns = {'body': ('body_sha256', 'a' * 64), 'path': ('parquet_path', 'old.parquet'),
            'lease': ('lease_owner', 'old-owner'), 'missing_clock': ('last_checked_at_utc', None),
            'naive_clock': ('last_checked_at_utc', '2026-10-17T21:00:00'),
            'future_clock': ('last_checked_at_utc', '2099-01-01T00:00:00+00:00'),
            'wrong_error': ('error_code', 'http_403')}
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


def test_short_original_reaches_shared_writer_and_survives_restart(tmp_path):
    from downloader.news_fulltext import fetch_article
    from test_news_fulltext import FakeResponse, http_fixture

    raw = str(short_page()).encode()
    http, _ = http_fixture(candidate_config(), tmp_path, {
        'https://www.ftnn.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'), URL: FakeResponse(raw)})
    try:
        with closing(NewsQueue(tmp_path, http.registry)) as queue:
            queue.add_articles([{'url': URL}])
            record = fetch_article(http, queue.claim_url(URL, 300))
            assert record['body'] == SHORT and record['raw_response_body'] == raw
            assert not record['publish'] and not record['remote_training_export']
            queue.accept([record])
            head = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
            assert head['state'] == 'complete' and head['parquet_path'] and head['body_sha256']
        with closing(NewsQueue(tmp_path, http.registry)) as queue:
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == head
            assert verify_bodies(tmp_path)['accepted']
    finally:
        http.close()


def test_v5_failure_observations_retain_metadata_only_not_the_short_report(tmp_path):
    from downloader.news_fulltext_http import Response
    from test_news_failure_diagnostics import fail
    from test_news_failure_identity_metadata import evidence

    with closing(NewsQueue(tmp_path, PolicyRegistry(candidate_config(), base=tmp_path))) as queue:
        fail(queue, Response(URL, 200, {'content-type': 'text/html'}, str(short_page()).encode()),
             code='article_identity_not_verified', received_at_utc='2026-10-17T21:10:00+00:00')
        paths, records = evidence(tmp_path)
        assert len(records) == 1 and records[0]['facts']['ftnn_native_validation']['contract_version'] == 6
        assert not records[0]['raw_response_retained'] and not records[0]['accepted_as_article']
        assert SHORT not in paths[0].read_text()
