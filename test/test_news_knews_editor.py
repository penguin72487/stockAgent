"""Native caption/TOC grammar retains prose without relaxing source identity."""
from contextlib import closing
from copy import deepcopy
import json
from pathlib import Path
import time

import pytest

from downloader import news_fulltext_knews as native
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from test_news_knews_known_urls import URL, authored, known_entry
from test_news_knews_native import TEXT, policy


CAPTION = '合成原生圖說，保留數字1000與-5；不是圖片辨識結果。'
QUOTE = '合成引文，單位股與原始比率12.34%都完整保留。'


def editor_entry():
    value = known_entry()
    value.update(embedded_article_layout=native.KNEWS_EDITOR_LAYOUT,
        adapter_verification=native.KNEWS_V3_VERIFICATION)
    return value


def editor_page(*, headings=True):
    soup = authored()
    body = soup.select_one('.article-content-inner')
    if not headings:
        soup.select_one('#article-left-toc').decompose()
        body.select_one('h3').decompose()
    figure = soup.new_tag('figure')
    link, image, caption = soup.new_tag('a', href='https://example.org/not-fetched'), soup.new_tag('img'), soup.new_tag('figcaption')
    image['alt'] = 'ALT_PRIVATE_NOT_A_CAPTION'
    image['data-src'] = 'https://example.org/not-fetched-image'
    link.append(image)
    figure.append(link)
    caption.string = CAPTION
    figure.append(caption)
    body.select_one('p').insert_after(figure)
    quote = soup.new_tag('blockquote')
    paragraph = soup.new_tag('p')
    paragraph.string = QUOTE
    quote.append(paragraph)
    figure.insert_after(quote)
    return soup


def extract(soup):
    return extract_article(str(soup).encode(), policy(editor_entry()), page_url=URL)


@pytest.mark.parametrize('headings', [True, False])
def test_caption_quote_intro_and_full_prose_retained_once_in_native_order(headings):
    result = extract(editor_page(headings=headings))
    body = result['body']
    assert body.count(CAPTION) == body.count(QUOTE) == 1
    assert TEXT in body and body.index(TEXT) < body.index(CAPTION) < body.index(QUOTE)
    assert 'ALT_PRIVATE' not in body and 'NOT_ARTICLE' not in body
    assert 'inline_captions' in result['body_completeness']
    assert json.loads(result['author_attribution_detail_json'])['contract_version'] == 3


def test_previous_body_values_credits_and_clocks_have_exact_parity():
    before = extract_article(str(authored()).encode(), policy(known_entry()), page_url=URL)
    after = extract(authored())
    for key in ('body', 'body_sha256', 'authors_json', 'published_at_raw', 'modified_at_raw'):
        assert after.get(key) == before.get(key), key
    assert after['body_sha256']


@pytest.mark.parametrize('case', ['headings_without_toc', 'toc_without_headings', 'mismatch', 'duplicate_toc'])
def test_optional_toc_is_not_permission_for_partial_or_conflicting_heading_sets(case):
    soup = editor_page()
    if case == 'headings_without_toc':
        soup.select_one('#article-left-toc').decompose()
    elif case == 'toc_without_headings':
        soup.select_one('.article-content-inner > h3').decompose()
    elif case == 'mismatch':
        soup.select_one('.toc-link')['href'] = '#different'
    else:
        soup.select_one('.article-body-wrapper').append(deepcopy(soup.select_one('#article-left-toc')))
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract(soup)


@pytest.mark.parametrize('case', ['hidden', 'aria', 'display', 'event', 'script', 'extra_text',
    'duplicate_caption', 'nested_caption_figure', 'missing_caption', 'media_link_text'])
def test_unknown_hidden_or_partial_caption_structure_is_rejected(case):
    soup = editor_page()
    figure = soup.select_one('.article-content-inner > figure')
    if case in {'hidden', 'aria', 'display', 'event'}:
        key, value = {'hidden': ('hidden', ''), 'aria': ('aria-hidden', 'true'),
            'display': ('style', 'display: none'), 'event': ('onclick', 'private()')}[case]
        figure.select_one('figcaption')[key] = value
    elif case == 'script':
        figure.append(soup.new_tag('script'))
    elif case == 'extra_text':
        figure.append('UNRECOGNIZED_SOURCE_TEXT')
    elif case == 'duplicate_caption':
        figure.append(deepcopy(figure.select_one('figcaption')))
    elif case == 'nested_caption_figure':
        wrapper = soup.new_tag('p')
        figure.wrap(wrapper)
    elif case == 'missing_caption':
        figure.select_one('figcaption').decompose()
    else:
        figure.select_one('a').append('NOT_CAPTION')
    with pytest.raises(NewsError):
        extract(soup)


@pytest.mark.parametrize('headings', [True, False])
def test_old_v2_still_rejects_unreviewed_editors(headings):
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        extract_article(str(editor_page(headings=headings)).encode(), policy(known_entry()), page_url=URL)


def test_exact_predecessor_preserves_http_owner_but_endpoint_pacing_changes_do_not(monkeypatch):
    previous = policy(known_entry())
    binding = (previous.data_fingerprint, previous.authorization_fingerprint, '1' * 64, '2' * 64)
    monkeypatch.setattr(native, 'KNEWS_KNOWN_HTTP_BINDING', binding)
    current = policy(editor_entry())
    assert current.data_fingerprint != previous.data_fingerprint
    assert current.parser_repair_prior_capture_fingerprint == previous.data_fingerprint
    assert current.http_capacity_source_fingerprint == '1' * 64
    assert current.http_capacity_authorization_fingerprint == '2' * 64
    changed = editor_entry()
    changed['interval_seconds'] = 11
    assert not policy(changed).http_capacity_source_fingerprint
    changed = editor_entry()
    changed['authorization']['scope'] += ' new scope'
    assert not policy(changed).http_capacity_source_fingerprint


def test_full_installed_editor_candidate_keeps_all_provider_and_http_contracts(monkeypatch):
    import test_news_fulltext as shared

    config, registry, repo = shared.load_config(Path('configs/news_fulltext.json'))
    candidate = deepcopy(config)
    candidate.pop('provider_catalog', None)
    entry = next(p for p in candidate['providers'] if p['id'] == 'knews')
    entry.update(embedded_article_layout=native.KNEWS_EDITOR_LAYOUT,
        adapter_verification=native.KNEWS_V3_VERIFICATION)
    approved = PolicyRegistry(candidate, base=repo)
    for provider, source in registry.sources.items():
        current = approved.sources[provider]
        assert current.authorization_fingerprint == source.authorization_fingerprint
        assert current.http_capacity_source_fingerprint == source.http_capacity_source_fingerprint
        assert current.http_capacity_authorization_fingerprint == source.http_capacity_authorization_fingerprint
        if provider != 'knews':
            assert current.data_fingerprint == source.data_fingerprint
    monkeypatch.setattr(shared, 'load_config', lambda path: (candidate, approved, repo))
    shared.test_default_configuration_separates_personal_capture_from_licenses()


@pytest.mark.parametrize('state', ['article_identity_not_verified', 'article_attribution_not_verified', 'paywall_or_login'])
def test_parser_change_preserves_prior_failures_bodies_and_attempts(tmp_path, state):
    settings = {'contract_version': 1, 'publish': False, 'remote_training_export': False, 'providers': [known_entry()]}
    with closing(NewsQueue(tmp_path, PolicyRegistry(settings, base=tmp_path))) as queue:
        saved_url = URL.replace('A' * 32, 'B' * 32)
        queue.add_articles([{'url': URL}, {'url': saved_url}])
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=?,last_checked_at_utc=? WHERE url=?',
                (state, state, time.time() + 3600, '2026-10-24T00:00:00+00:00', URL))
            queue.db.execute("UPDATE articles SET state='complete',body_sha256=?,body_bytes=100,parquet_path=?,"
                "captured_at_utc=?,attempts=1 WHERE url=?", ('f' * 64, 'bodies/original.parquet',
                    '2026-10-23T18:00:00+00:00', saved_url))
        before = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')}
    with closing(NewsQueue(tmp_path, PolicyRegistry({**settings, 'providers': [editor_entry()]}, base=tmp_path))) as queue:
        queue.reconcile_policy()
        assert {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM articles')} == before
