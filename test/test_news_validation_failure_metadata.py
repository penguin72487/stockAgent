"""Native validation failures retain bounded facts, not more publisher calls."""
from contextlib import closing
from dataclasses import replace
import hashlib
import json
from types import SimpleNamespace

from bs4 import BeautifulSoup
import pytest

from downloader import news_fulltext_diagnostics as diagnostics
from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_failure_diagnostics import fail, reports
from test_news_failure_identity_metadata import evidence
from test_news_ftnn import URL, page, policy_config, update_schema
from test_news_fulltext import config as cfg


PEOPO_LOGO = ('#block-peopo-content article.node--view-mode-full > '
              '.node-pre-content .view-node-field .license-logo')


def peopo_config(config):
    source = config['providers'][0]
    source.update(id='peopo', domains=['peopo.org'],
                  required_license_url_prefix='http://creativecommons.org/licenses/',
                  license_selectors=[PEOPO_LOGO + ' a[href]'])
    return config


def peopo_page(logo):
    return BeautifulSoup('<div id="block-peopo-content"><article class="node--view-mode-full">'
        '<div class="node-pre-content"><div class="view-node-field">' + logo + '</div></div>'
        '<div class="node__content">private-body</div></article></div>'
        '<footer><a href="http://creativecommons.org/licenses/by/3.0/tw/">footer-license</a></footer>', 'lxml')


@pytest.mark.parametrize('kind', ['by', 'by-nc', 'by-nc-sa', 'by-sa', 'by-nd', 'by-nc-nd'])
def test_license_metadata_describes_each_native_license_without_granting_a_body(cfg, tmp_path, kind):
    source = PolicyRegistry(peopo_config(cfg), base=tmp_path).require('https://www.peopo.org/news/42')
    href = f'http://creativecommons.org/licenses/{kind}/3.0/tw/'
    soup = peopo_page(f'<div class="license-logo"><a href="{href}"><img alt="創用CC"></a></div>')
    facts = diagnostics.article_license_validation_metadata(soup, source)
    chosen = facts['configured_article_license_selections'][0]
    assert chosen['node_count'] == 1 and chosen['values'] == [{
        'display_url': href, 'original_value_sha256': hashlib.sha256(href.encode()).hexdigest(),
        'matches_current_required_prefix': True}]
    assert facts['peopo_current_article_scope_count'] == facts['peopo_current_article_logo_count'] == 1
    assert facts['peopo_current_article_logos'][0]['image_labels'][0]['recognized_public_label'] == '創用CC'
    assert facts['additional_requests'] == 0 and not facts['license_or_article_acceptance_granted']
    assert not facts['body_text_retained'] and not facts['raw_response_retained']
    assert 'private-body' not in json.dumps(facts) and 'footer-license' not in json.dumps(facts)


@pytest.mark.parametrize('logo,count,label', [
    ('', 0, None),
    ('<div class="license-logo">版權所有</div>', 1, '版權所有'),
    ('<div class="license-logo">private-declaration<img alt="private-alt"></div>', 1, None),
])
def test_absent_reserved_and_unknown_native_license_markers_are_distinct_without_footer_substitution(
        cfg, tmp_path, logo, count, label):
    source = PolicyRegistry(peopo_config(cfg), base=tmp_path).require('https://www.peopo.org/news/42')
    facts = diagnostics.article_license_validation_metadata(peopo_page(logo), source)
    assert facts['configured_article_license_selections'][0]['node_count'] == 0
    assert facts['peopo_current_article_logo_count'] == count
    if count:
        item = facts['peopo_current_article_logos'][0]
        assert item['recognized_public_label'] == label and item['anchor_count'] == 0
        assert item['characters'] > 0 and len(item['sha256']) == 64
    assert 'private-' not in json.dumps(facts) and not facts['license_or_article_acceptance_granted']


def test_native_license_diagnostics_redact_credentials_and_do_not_silently_rewrite_policy(cfg, tmp_path):
    source = PolicyRegistry(peopo_config(cfg), base=tmp_path).require('https://www.peopo.org/news/42')
    hrefs = ['http://creativecommons.org/licenses/by/3.0/tw/?private-token#private-fragment',
             'https://creativecommons.org/licenses/by/3.0/tw/',
             'https://user:private-password@creativecommons.org/licenses/by/3.0/tw/',
             'javascript:private-code']
    soup = peopo_page('<div class="license-logo">' + ''.join(f'<a href="{v}"></a>' for v in hrefs) + '</div>')
    facts = diagnostics.article_license_validation_metadata(soup, source)
    values = facts['configured_article_license_selections'][0]['values']
    assert [v['display_url'] for v in values] == [hrefs[0].split('?')[0], hrefs[1], None, None]
    assert [v['matches_current_required_prefix'] for v in values] == [True, False, False, False]
    assert [v['original_value_sha256'] for v in values] == [hashlib.sha256(v.encode()).hexdigest() for v in hrefs]
    assert 'private-' not in json.dumps(facts) and not facts['license_or_article_acceptance_granted']


def test_native_license_metadata_bounds_selectors_links_logos_and_images(cfg, tmp_path):
    source = PolicyRegistry(peopo_config(cfg), base=tmp_path).require('https://www.peopo.org/news/42')
    source = replace(source, license_selectors=tuple([PEOPO_LOGO + ' a[href]'] * 20))
    logo = '<div class="license-logo">' + ('<a href="https://example.test/license"></a>' * 20)
    soup = peopo_page((logo + '<img alt="private-alt">' * 10 + '</div>') * 10)
    facts = diagnostics.article_license_validation_metadata(soup, source)
    selections = facts['configured_article_license_selections']
    assert facts['selector_list_truncated'] and len(selections) == 16
    assert all(v['truncated'] and v['node_count'] == 200 and len(v['values']) == 16 for v in selections)
    assert facts['peopo_current_article_logos_truncated'] and len(facts['peopo_current_article_logos']) == 8
    assert all(v['image_count'] == 10 and len(v['image_labels']) == 8 for v in facts['peopo_current_article_logos'])
    assert 'private-' not in json.dumps(facts)


def test_canonical_license_failure_keeps_private_native_facts_but_no_raw_body_or_success(cfg, tmp_path):
    registry = PolicyRegistry(peopo_config(cfg), base=tmp_path)
    soup = peopo_page('<div class="license-logo">版權所有</div>')
    raw = Response('https://www.peopo.org/news/42', 200, {'content-type': 'text/html'}, str(soup).encode())
    with closing(NewsQueue(tmp_path, registry)) as queue:
        fail(queue, raw, code='article_license_not_verified', received_at_utc='2026-10-18T04:00:00+00:00')
        paths, rows = evidence(tmp_path)
        assert len(rows) == 1 and rows[0]['facts']['native_article_license_validation']['peopo_current_article_logos'][0][
            'recognized_public_label'] == '版權所有'
        assert not rows[0]['accepted_as_article'] and not rows[0]['raw_response_retained']
        assert 'private-body' not in paths[0].read_text() and not reports(tmp_path)[0]
        assert queue.db.execute('SELECT state,error_code,body_sha256 FROM articles').fetchone()[:] == (
            'article_license_not_verified', 'article_license_not_verified', None)
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0


@pytest.mark.parametrize('code', sorted(diagnostics.VALIDATION_METADATA_CODES))
def test_static_validation_budgets_are_metadata_only_and_never_refill_identity_raw_or_retry(cfg, tmp_path, code):
    policy = PolicyRegistry(cfg, base=tmp_path)
    root = tmp_path / 'store'
    raw = Response('https://publisher.test/article/42?token=private-query', 200,
        {'content-type': 'text/html', 'set-cookie': 'private-cookie'},
        b'<h1>private-headline</h1><article>private-body</article>')
    with closing(NewsQueue(root, policy)) as queue:
        for _ in range(3):
            fail(queue, raw, code='article_identity_not_verified', received_at_utc='2026-10-14T15:00:00+00:00')
        for _ in range(4):
            fail(queue, raw, code=code, received_at_utc='2026-10-14T15:00:00+00:00')
        paths, rows = evidence(root)
        assert len(paths) == 4
        validated = [row for row in rows if row['error_code'] == code]
        assert len(validated) == 2 and all(row['budget_key'].startswith('failure_validation_metadata_attempts:v1:') for row in validated)
        assert not any('private-' in p.read_text() for p in paths)
        assert all(not row['raw_response_retained'] and not row['accepted_as_article'] for row in rows)
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0
        assert not reports(root)[0]
        assert queue.db.execute('SELECT state,error_code,body_sha256 FROM articles').fetchone()[:] == (code, code, None)
    provider = policy.match(raw.url).provider
    policy.sources[provider] = replace(policy.sources[provider], data_fingerprint='upgraded-parser')
    with closing(NewsQueue(root, policy)) as queue:
        fail(queue, raw, code=code, received_at_utc='2026-10-14T15:00:00+00:00')
        assert len(evidence(root)[0]) == 4


@pytest.mark.parametrize('code', ['http_403', 'http_429', 'access_challenge', 'paywall_or_login', 'source_not_found'])
def test_refusals_do_not_gain_a_metadata_budget_or_recovery_request(cfg, tmp_path, code):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        fail(queue, Response('https://publisher.test/article/42', 200, {'content-type': 'text/html'}, b'<h1>x</h1>'),
             code=code, received_at_utc='2026-10-14T15:00:00+00:00')
        assert not evidence(tmp_path)[0]
        assert not queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE 'failure_validation_metadata_attempts:%'").fetchone()


def test_failed_validation_storage_consumes_reservation_and_does_not_refetch(cfg, tmp_path, monkeypatch):
    calls = []
    def broken(*args, **kwargs):
        calls.append(args)
        raise OSError('owned storage failure')
    monkeypatch.setattr(diagnostics, 'atomic_write_json', broken)
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        for _ in range(4):
            fail(queue, Response('https://publisher.test/article/42', 200, {'content-type': 'text/html'}, b'<h1>x</h1>'),
                code='invalid_bulk_publication_clock', received_at_utc='2026-10-14T15:00:00+00:00')
        assert len(calls) == 2 and not evidence(tmp_path)[0]
        assert queue.db.execute("SELECT value FROM queue_settings WHERE key LIKE 'failure_validation_metadata_attempts:%'").fetchone()[0] == '2'


@pytest.mark.parametrize('code', sorted(diagnostics.VALIDATION_METADATA_CODES))
def test_worker_passes_already_received_validation_response_to_owner_without_io(tmp_path, monkeypatch, code):
    import downloader.news_fulltext as fulltext
    registry = PolicyRegistry(policy_config(), base=tmp_path)
    raw = Response(URL, 200, {'content-type': 'text/html'}, str(page()).encode())
    calls = []
    def get(url, **kwargs):
        calls.append(url)
        return raw
    def rejected(*args):
        raise NewsError(code)
    monkeypatch.setattr(fulltext, 'extract_article_response', rejected)
    monkeypatch.setattr(diagnostics, 'atomic_write_json', lambda *a, **k: pytest.fail('worker wrote'))
    with pytest.raises(NewsError, match='^' + code + '$') as caught:
        fetch_article(SimpleNamespace(registry=registry, get=get), {'url': URL, 'id': b'z' * 32})
    assert calls == [URL] and caught.value.failure_response.response is raw
    assert not (tmp_path / 'identity_metadata').exists()


def test_ftnn_native_clock_and_identity_facts_preserve_source_defect_without_text_or_acceptance(tmp_path):
    soup = page()
    update_schema(soup, 'dateModified', '2026-10-14T21:00:00+08:00')
    raw = Response(URL, 200, {'content-type': 'text/html'}, str(soup).encode())
    with closing(NewsQueue(tmp_path, PolicyRegistry(policy_config(), base=tmp_path))) as queue:
        fail(queue, raw, code='invalid_bulk_publication_clock', received_at_utc='2026-10-14T15:00:00+00:00')
        paths, rows = evidence(tmp_path)
        assert len(paths) == 1
        facts = rows[0]['facts']['ftnn_native_validation']
        assert facts['declared_publication'] == '2026-10-14T22:00:15+08:00'
        assert facts['declared_modification'] == '2026-10-14T21:00:00+08:00'
        assert facts['visible_release'] == '發布時間：2026/10/14 22:00'
        assert facts['declared_author_in_visible_byline'] and facts['native_reporter_byline_shape']
        assert facts['heading_compact_sha256'] == [facts['schema_headline_compact_sha256']]
        assert facts['schema_headline_compact_sha256'] == facts['breadcrumb_headline_compact_sha256']
        assert facts['og_title_compact_sha256'] == [facts['schema_expected_og_title_compact_sha256']]
        assert not facts['body_text_retained'] and not rows[0]['raw_response_retained']
        assert '黃詩雯' not in paths[0].read_text() and '原始報導' not in paths[0].read_text()
        assert not rows[0]['accepted_as_article'] and not rows[0]['publish'] and not rows[0]['remote_training_export']


@pytest.mark.parametrize('bad_schema', ['broken', 'oversized', 'too_many', 'nested_type'])
def test_invalid_native_metadata_is_bounded_and_cannot_accept_or_emit_private_text(tmp_path, bad_schema):
    soup = page()
    scripts = soup.select('script[type="application/ld+json"]')
    if bad_schema == 'broken':
        scripts[0].string = 'private-broken-json'
    elif bad_schema == 'oversized':
        scripts[0].string = 'private-' * 10000
    elif bad_schema == 'too_many':
        for _ in range(17):
            extra = soup.new_tag('script', type='application/ld+json')
            extra.string = '{}'
            soup.head.append(extra)
    else:
        data = json.loads(scripts[0].string)
        data['author'] = ['private-author']
        scripts[0].string = json.dumps(data)
    with closing(NewsQueue(tmp_path, PolicyRegistry(policy_config(), base=tmp_path))) as queue:
        fail(queue, Response(URL, 200, {'content-type': 'text/html'}, str(soup).encode()),
             code='article_attribution_not_verified', received_at_utc='2026-10-14T15:00:00+00:00')
        paths, rows = evidence(tmp_path)
        assert len(paths) == 1 and paths[0].stat().st_size < 65536
        assert 'private-' not in paths[0].read_text() and not rows[0]['accepted_as_article']
