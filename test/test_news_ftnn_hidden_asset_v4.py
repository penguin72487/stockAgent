"""Reviewed hidden data-URI asset stays separate from complete visible prose."""
import hashlib
import json
from contextlib import closing
from dataclasses import replace
from pathlib import Path

from bs4 import Comment
import pytest

from downloader.news_fulltext_ftnn import ftnn_visible_article
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import FTNN_NATIVE_V4_REPAIR_COHORT, NewsQueue, ftnn_native_v4_parser_repair, verify_bodies
from test_news_ftnn import URL, PROSE, NOTICE, page
from test_news_ftnn_empty_spans_v3 import candidate_config as prior_config, seed_failure


URI = 'data:image/gif;base64,AA=='  # Synthetic, not a saved publisher asset.


@pytest.fixture
def reviewed_asset(monkeypatch):
    import downloader.news_fulltext_ftnn as native

    assert native._HIDDEN_INLINE_ASSET == (
        246, 'f9a61a7f9aacc1db170363075e92849812736e36f86ccda3ff16c166d6b90c04')
    monkeypatch.setattr(native, '_HIDDEN_INLINE_ASSET', (len(URI), hashlib.sha256(URI.encode()).hexdigest()))


def asset_page(parser='lxml'):
    soup = page(parser)
    wrapper = soup.new_tag('span', attrs={'lang': 'EN-US', 'style': 'display:none'})
    wrapper.append(soup.new_tag('img', attrs={'alt': '', 'src': URI, 'style': 'width:15px; height:15px'}))
    soup.select('.news-body > p')[1].append(wrapper)
    return soup


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_full_asset_attributes_and_uri_are_preserved_without_changing_prose_or_raw_input(parser, reviewed_asset):
    soup = asset_page(parser)
    original = str(soup)
    expected, expected_body = ftnn_visible_article(page(parser), page_url=URL, adapter_contract_version=4)
    result, body = ftnn_visible_article(soup, page_url=URL, adapter_contract_version=4)
    before_notices = json.loads(expected.pop('verified_article_notices_json'))
    notices = json.loads(result.pop('verified_article_notices_json'))
    assert notices[:-1] == before_notices
    assert notices[-1] == {'kind': 'native_hidden_inline_image_preserved_v1', 'text': '',
        'wrapper_attributes': {'lang': 'EN-US', 'style': 'display:none'},
        'image_attributes': {'alt': '', 'style': 'width:15px; height:15px'},
        'source_uri_basis': 'same_article_raw_response_native_hidden_image_src',
        'source_uri_characters': len(URI), 'source_uri_sha256': hashlib.sha256(URI.encode()).hexdigest(),
        'source_uri_media_type': 'data:image/gif;base64',
        'source_display_state': 'display:none', 'visible_body_text_invented': False}
    assert result.pop('body_completeness') == expected.pop('body_completeness') + '; native_hidden_inline_asset_preserved_separately'
    assert result == expected and str(soup) == original
    assert body.get_text() == expected_body.get_text()
    assert PROSE in body.get_text() and NOTICE in body.get_text()


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('version', [1, 2, 3])
def test_old_native_adapters_stay_reproducible_and_reject_this_nonempty_hidden_node(parser, version, reviewed_asset):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        ftnn_visible_article(asset_page(parser), page_url=URL, adapter_contract_version=version)


CHANGES = [
    lambda s, w, i: w.append('hidden source prose'),
    lambda s, w, i: w.append(s.new_tag('span')),
    lambda s, w, i: w.append(s.new_string(' ', Comment)),
    lambda s, w, i: w.attrs.update({'hidden': ''}),
    lambda s, w, i: w.attrs.update({'lang': 'unknown'}),
    lambda s, w, i: w.attrs.update({'style': 'display:none;color:red'}),
    lambda s, w, i: i.attrs.update({'alt': 'original text'}),
    lambda s, w, i: i.attrs.update({'src': 'https://invalid.test/image.png'}),
    lambda s, w, i: i.attrs.update({'src': URI.replace('AA==', 'BA==')}),
    lambda s, w, i: i.attrs.update({'style': 'width:16px; height:15px'}),
    lambda s, w, i: i.attrs.update({'onload': 'unknown()'}),
    lambda s, w, i: i.attrs.update({'srcset': URI}),
    lambda s, w, i: i.attrs.pop('alt'),
    lambda s, w, i: w.parent.attrs.update({'hidden': ''}),
    lambda s, w, i: s.select_one('.news-body').attrs.update({'style': 'display:none'}),
]

@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('change', CHANGES)
def test_unknown_image_payload_hidden_prose_attributes_and_ancestors_cannot_be_removed(parser, change, reviewed_asset):
    soup = asset_page(parser)
    wrapper = soup.select_one('.news-body span[lang]')
    change(soup, wrapper, wrapper.img)
    before = str(soup)
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        ftnn_visible_article(soup, page_url=URL, adapter_contract_version=4)
    assert str(soup) == before


def test_actual_source_pin_is_required_not_only_the_reviewed_shape():
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        ftnn_visible_article(asset_page(), page_url=URL, adapter_contract_version=4)


def test_non_asset_source_values_and_scope_are_not_relaxed(reviewed_asset):
    # An image cannot turn a different credit into source-original attribution.
    soup = asset_page()
    soup.select_one('.news-body > p').string = '第三方記者／報導'
    with pytest.raises(NewsError, match='article_rights_or_scope_excluded'):
        ftnn_visible_article(soup, page_url=URL, adapter_contract_version=4)


def candidate_config(parser='lxml'):
    cfg = prior_config(parser)
    cfg['providers'][0]['embedded_article_layout'] = 'ftnn_visible_article_v4'
    return cfg


def test_only_exact_v3_capture_is_repaired_without_new_authority_or_http_identity():
    old = PolicyRegistry(prior_config(), base=Path.cwd()).sources['ftnn']
    new = PolicyRegistry(candidate_config(), base=Path.cwd()).sources['ftnn']
    assert new.data_fingerprint != old.data_fingerprint
    assert new.parser_repair_prior_capture_fingerprint == old.data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    assert new.http_capacity_source_fingerprint == old.http_capacity_source_fingerprint
    assert ftnn_native_v4_parser_repair(new, old.data_fingerprint)
    for previous in (None, new.data_fingerprint, old.http_capacity_source_fingerprint, 'a' * 64):
        assert not ftnn_native_v4_parser_repair(new, previous)
    assert not ftnn_native_v4_parser_repair(replace(new, allowed=False), old.data_fingerprint)
    assert not ftnn_native_v4_parser_repair(old, old.data_fingerprint)


def test_one_missing_asset_failure_reopens_once_and_never_replays_completed_old_cohorts(tmp_path):
    target = next(iter(FTNN_NATIVE_V4_REPAIR_COHORT))
    with closing(NewsQueue(tmp_path, PolicyRegistry(prior_config(), base=tmp_path))) as queue:
        for url, error in FTNN_NATIVE_V4_REPAIR_COHORT.items():
            seed_failure(queue, url, error)
        for suffix, state in [('561059', 'complete'), ('547631', 'complete'), ('587768', 'complete'),
                              ('512507', 'article_rights_or_scope_excluded'), ('42', 'article_attribution_not_verified')]:
            seed_failure(queue, f'https://www.ftnn.com.tw/news/{suffix}', state)
        queue.db.commit()
        before = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
    registry = PolicyRegistry(candidate_config(), base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        for row in queue.db.execute('SELECT * FROM articles'):
            expected = before[row['url']]
            assert dict(row) == ({**expected, 'state': 'retry'} if row['url'] == target else expected)
        marker = 'ftnn_native_layout_recheck_v4:' + registry.sources['ftnn'].data_fingerprint
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


@pytest.mark.parametrize('change', ['body', 'path', 'lease', 'missing_clock', 'naive_clock',
    'future_clock', 'wrong_error', 'unknown_capture', 'changed_http', 'query'])
def test_unproved_failure_lease_existing_body_or_changed_source_is_not_requeued(tmp_path, change):
    url = next(iter(FTNN_NATIVE_V4_REPAIR_COHORT)) + ('?unproved=1' if change == 'query' else '')
    with closing(NewsQueue(tmp_path, PolicyRegistry(prior_config(), base=tmp_path))) as queue:
        seed_failure(queue, url, 'article_identity_not_verified')
        columns = {'body': ('body_sha256', 'a' * 64), 'path': ('parquet_path', 'body/previous.parquet'),
            'lease': ('lease_owner', 'existing-owner'), 'missing_clock': ('last_checked_at_utc', None),
            'naive_clock': ('last_checked_at_utc', '2026-10-17T20:00:00'),
            'future_clock': ('last_checked_at_utc', '2099-01-01T00:00:00+00:00'),
            'wrong_error': ('error_code', 'http_403')}
        if change in columns:
            column, value = columns[change]
            queue.db.execute(f'UPDATE articles SET {column}=? WHERE url=?', (value, url))
        elif change == 'unknown_capture':
            queue.db.execute("UPDATE queue_settings SET value=? WHERE key='source_capture_fingerprint:ftnn'", ('a' * 64,))
        queue.db.commit()
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
    cfg = candidate_config()
    if change == 'changed_http':
        cfg['providers'][0]['interval_seconds'] = 3
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == before


def test_complete_visible_body_and_entire_asset_reach_shared_writer_and_survive_restart(tmp_path, reviewed_asset):
    from downloader.news_fulltext import fetch_article
    from test_news_fulltext import FakeResponse, http_fixture
    soup = asset_page()
    http, _ = http_fixture(candidate_config(), tmp_path, {
        'https://www.ftnn.com.tw/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        URL: FakeResponse(str(soup).encode())})
    try:
        with closing(NewsQueue(tmp_path, http.registry)) as queue:
            queue.add_articles([{'url': URL}])
            record = fetch_article(http, queue.claim_url(URL, 300))
            assert record['body'] and PROSE in record['body'] and NOTICE in record['body']
            notice = json.loads(record['verified_article_notices_json'])[-1]
            assert notice['source_uri_sha256'] == hashlib.sha256(URI.encode()).hexdigest()
            assert URI.encode() in record['raw_response_body']
            assert not record['publish'] and not record['remote_training_export']
            queue.accept([record])
            head = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
            assert head['state'] == 'complete' and head['parquet_path'] and head['body_sha256']
        with closing(NewsQueue(tmp_path, http.registry)) as queue:
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == head
            assert verify_bodies(tmp_path)['accepted']
    finally:
        http.close()


def test_v4_failure_diagnostics_keep_only_native_metadata_not_asset_or_prose(tmp_path, reviewed_asset):
    from downloader.news_fulltext_http import Response
    from test_news_failure_diagnostics import fail
    from test_news_failure_identity_metadata import evidence
    with closing(NewsQueue(tmp_path, PolicyRegistry(candidate_config(), base=tmp_path))) as queue:
        fail(queue, Response(URL, 200, {'content-type': 'text/html'}, str(asset_page()).encode()),
             code='article_identity_not_verified', received_at_utc='2026-10-17T21:00:00+00:00')
        paths, records = evidence(tmp_path)
        assert len(records) == 1 and records[0]['facts']['ftnn_native_validation']['contract_version'] == 5
        assert not records[0]['raw_response_retained'] and not records[0]['accepted_as_article']
        assert URI not in paths[0].read_text() and PROSE not in paths[0].read_text()
