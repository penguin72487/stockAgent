"""Synthetic RSS contracts: native prose, literal credits, and immutable ABI replay."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from xml.sax.saxutils import escape

import pytest

from downloader.news_fulltext import (accept_discovery, discovery_fingerprint, fetch_discovery,
    fulltext_feed_discovery_fingerprints, load_config, seed_discovery)
from downloader.news_fulltext_feed import FEED_BODY_PROVENANCE_FIELDS, feed_records, replay_feed_body_version
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError
from downloader.news_fulltext_queue import NewsQueue
from scripts.audit_news_selected_articles import audit
from test_news_fulltext import FakeResponse, config, http_fixture
from test_news_provider_efficiency import rss


URL = 'https://globalvoices.org/2005/07/12/synthetic-story/'
ROOT = 'https://globalvoices.org/feed/'
CLOCK = 'Tue, 12 Jul 2005 12:00:00 -0400'
BODY = 'The complete synthetic note links a public announcement.'


@pytest.fixture(scope='module')
def registry():
    return load_config(Path('configs/news_fulltext.json'))[1]


def footer(*, translated=False):
    credits = ('<div class="text-credits-section"><span class="credit-label">'
        + ('作者 (English)' if translated else 'Written by')
        + '</span><a class="user-link" href="https://globalvoices.org/author/writer/">Writer</a></div>')
    if translated:
        credits += ('<div class="text-credits-section"><span class="credit-label">譯者 (繁體中文)</span>'
            '<a class="user-link" href="https://zht.globalvoices.org/author/translator/">譯者</a></div>')
    return ('<div class="gv-rss-footer"><strong><div class="text-credits-container">' + credits + '</div>'
        + ('<span class="source-link"><a href="https://globalvoicesonline.org/2005/07/12/original/">'
           '閱讀原文 (English)</a></span>' if translated else '') + '</strong></div>')


def fragment(body=None, *, url=URL, translated=False):
    notice = ('' if translated else '<p class="originally-published"><small>Originally published on '
        '<a href="' + url + '">Global Voices</a></small></p>')
    return notice + (f'<p>{BODY}</p>' if body is None else body) + footer(translated=translated)


def item(markup=None, *, url=URL, creator='Writer', clock=CLOCK, rights=''):
    return (f'<item><title>Synthetic complete story</title><link>{escape(url)}</link>'
        f'<pubDate>{escape(clock)}</pubDate><dc:creator>{escape(creator)}</dc:creator>'
        '<description>This summary must never be used as the body.</description>'
        '<content:encoded><![CDATA[' + (fragment(url=url) if markup is None else markup)
        + ']]></content:encoded>' + (f'<dc:rights>{escape(rights)}</dc:rights>' if rights else '') + '</item>')


def parse(registry, markup=None, **kwargs):
    payload = rss([item(markup, **kwargs)])
    response = Response(ROOT, 200, {}, payload)
    return feed_records(response, SimpleNamespace(registry=registry), registry.sources['globalvoices'])


def test_native_notice_and_footer_are_literal_metadata_not_article_prose(registry):
    records, errors = parse(registry)
    assert not errors and len(records) == 1
    row = records[0]
    assert row['body'] == BODY and row['body_bytes'] == len(BODY.encode())
    assert row['body_sha256'] == hashlib.sha256(BODY.encode()).hexdigest()
    assert row['published_at_raw'] == CLOCK and json.loads(row['authors_json']) == ['Writer']
    assert row['verified_short_feed_body'] and row['extraction_method'] == 'rss_content_encoded_v3'
    assert row['fulltext_feed_body_contract_version'] == 3
    assert row['fulltext_feed_data_fingerprint'] == registry.sources['globalvoices'].fulltext_feed_data_fingerprint
    metadata = json.loads(row['feed_envelope_metadata_json'])
    assert metadata['credits'] == [{'name': 'Writer', 'role_label': 'Written by',
                                   'profile_url': 'https://globalvoices.org/author/writer/'}]
    assert metadata['originally_published_notice']['url'] == URL
    assert not row['historical_point_in_time'] and not row['publish'] and not row['remote_training_export']
    assert metadata['contract_version'] == 3


@pytest.mark.parametrize('engine', ['html.parser', 'lxml'])
def test_native_tagline_before_notice_is_retained_once_not_discarded_or_counted_as_credit(registry, engine):
    from dataclasses import replace
    source = replace(registry.sources['globalvoices'], html_parser=engine)
    markup = '<p><big class="tagline"><em>A native subtitle.</em></big></p>' + fragment()
    response = Response(ROOT, 200, {}, rss([item(markup)]))
    old, errors = feed_records(response, SimpleNamespace(registry=registry), source, body_contract_version=2)
    assert not old and errors[0]['error'] == 'invalid_bulk_post_fields'
    records, errors = feed_records(response, SimpleNamespace(registry=registry), source)
    assert not errors and records[0]['body'] == 'A native subtitle.\n' + BODY
    assert json.loads(records[0]['feed_envelope_metadata_json'])['originally_published_notice']['url'] == URL


@pytest.mark.parametrize('language', ['Français', 'Português', 'English'])
def test_explicit_english_language_roles_stay_literal_not_inferred_author_roles(registry, language):
    markup = fragment(translated=True).replace('作者 (English)', f'Written ({language}) by')
    markup = markup.replace('譯者 (繁體中文)', 'Translated (English) by')
    response = Response(ROOT, 200, {}, rss([item(markup, creator='譯者')]))
    old, errors = feed_records(response, SimpleNamespace(registry=registry), registry.sources['globalvoices'], body_contract_version=2)
    assert not old and errors[0]['error'] == 'article_attribution_not_verified'
    records, errors = feed_records(response, SimpleNamespace(registry=registry), registry.sources['globalvoices'])
    assert not errors and records[0]['body'] == BODY
    metadata = json.loads(records[0]['feed_envelope_metadata_json'])
    assert [c['role_label'] for c in metadata['credits']] == [f'Written ({language}) by', 'Translated (English) by']
    assert json.loads(records[0]['authors_json']) == ['譯者']


@pytest.mark.parametrize('prefix', [
    '<p>A generic prelude.</p>', '<p><big class="unknown"><em>A subtitle.</em></big></p>',
    '<p><big class="tagline"><em>A subtitle.</em></big><span>Unknown credit.</span></p>',
    '<p><big class="tagline"><em><a href="https://other.test/">Unknown subtitle.</a></em></big></p>',
    '<p hidden><big class="tagline"><em>Hidden subtitle.</em></big></p>',
    '<p><big class="tagline"><em></em></big></p>',
    '<p><big class="tagline"><em>A subtitle.</em></big></p><p>Other prelude.</p>',
])
def test_new_notice_position_requires_the_exact_visible_native_tagline(registry, prefix):
    records, errors = parse(registry, prefix + fragment())
    assert not records and errors[0]['error'] == 'invalid_bulk_post_fields'


@pytest.mark.parametrize('label', ['Sponsored (English) by', 'Written () by', 'Written (English) for',
    'Written (English) by an unknown desk', 'Translated ((English)) by'])
def test_language_annotation_is_not_a_wildcard_for_unknown_credits(registry, label):
    records, errors = parse(registry, fragment().replace('Written by', label))
    assert not records and errors[0]['error'] == 'article_attribution_not_verified'


def test_native_wordpress_original_post_permalink_is_metadata_only_and_v2_unchanged(registry):
    original = 'https://pt.globalvoices.org/?p=123804'
    markup = fragment(translated=True).replace('https://globalvoicesonline.org/2005/07/12/original/', original)
    response = Response(ROOT, 200, {}, rss([item(markup, creator='譯者')]))
    old, errors = feed_records(response, SimpleNamespace(registry=registry), registry.sources['globalvoices'], body_contract_version=2)
    assert not old and errors[0]['error'] == 'invalid_bulk_post_fields'
    records, errors = feed_records(response, SimpleNamespace(registry=registry), registry.sources['globalvoices'])
    assert not errors and records[0]['body'] == BODY
    assert json.loads(records[0]['feed_envelope_metadata_json'])['original_source_link']['url'] == original
    assert records[0]['requested_url'] == records[0]['final_url'] == URL


@pytest.mark.parametrize('original', ['https://pt.globalvoices.org/?p=0', 'https://pt.globalvoices.org/?p=01',
    'https://pt.globalvoices.org/?p=123&next=https://other.test/', 'https://pt.globalvoices.org/author/writer/?p=123',
    'https://pt.globalvoices.org/?p=123#fragment', 'https://other.test/?p=123',
    'https://pt.globalvoices.org:443/?p=123', 'http://pt.globalvoices.org/?p=123'])
def test_original_credit_post_id_does_not_allow_arbitrary_queries_origins_or_fragments(registry, original):
    markup = fragment(translated=True).replace('https://globalvoicesonline.org/2005/07/12/original/', original)
    records, errors = parse(registry, markup, creator='譯者')
    assert not records and errors[0]['error'] == 'invalid_bulk_post_fields'


def test_translation_preserves_roles_original_source_and_creator_without_following_links(registry):
    url = 'https://zht.globalvoices.org/2014/03/08/16515/'
    records, errors = parse(registry, fragment(translated=True), url=url, creator='譯者')
    assert not errors and records[0]['body'] == BODY
    metadata = json.loads(records[0]['feed_envelope_metadata_json'])
    assert metadata['originally_published_notice'] is None
    assert [c['role_label'] for c in metadata['credits']] == ['作者 (English)', '譯者 (繁體中文)']
    assert json.loads(records[0]['authors_json']) == ['譯者']
    assert metadata['original_source_link']['url'].startswith('https://globalvoicesonline.org/')


@pytest.mark.parametrize('profile', ['https://zh.globalvoicesonline.org/hant/author/translator/',
    'https://rising.globalvoices.org/blog/author/translator/'])
def test_historical_contributor_urls_are_preserved_as_metadata_not_requested_aliases(registry, profile):
    markup = fragment(translated=True).replace('https://zht.globalvoices.org/author/translator/', profile)
    records, errors = parse(registry, markup, creator='譯者')
    assert not errors and records[0]['body'] == BODY
    assert json.loads(records[0]['feed_envelope_metadata_json'])['credits'][1]['profile_url'] == profile


@pytest.mark.parametrize('body', ['', '<p> </p>', '<p>...</p>',
    '<p><img src="https://images.test/photo.jpg"></p><p>Photo by a photographer.</p>',
    '<p><iframe src="https://media.test/player"></iframe></p><p>Video credit.</p>',
    '<p hidden>' + BODY + '</p>', '<p style="display:none">' + BODY + '</p>',
    '<p>' + BODY + '</p><script src="https://source.test/load.js"></script>',
    '<p>' + BODY + '</p><template>More text loading</template>'])
def test_empty_credit_media_hidden_or_loading_content_is_never_a_short_body(registry, body):
    records, errors = parse(registry, fragment(body))
    assert not records and len(errors) == 1
    assert errors[0]['error'] in {'body_not_found', 'invalid_bulk_post_fields'}
    assert errors[0]['url'] == URL


@pytest.mark.parametrize('mutate', [
    lambda s: s.replace('/author/writer/', '/author/'),
    lambda s: s.replace('https://globalvoices.org/author/', 'https://evilglobalvoices.org/author/'),
    lambda s: s.replace('https://globalvoices.org/author/', 'http://globalvoices.org/author/'),
    lambda s: s.replace('https://globalvoices.org/author/', 'https://globalvoices.org:443/author/'),
    lambda s: s.replace('Written by', 'Sponsored by'),
    lambda s: s.replace('Writer</a>', 'Different byline</a>'),
    lambda s: s.replace('class="gv-rss-footer"', 'class="gv-rss-footer" hidden'),
    lambda s: s.replace('</strong>', '<span>unexplained footer prose</span></strong>'),
    lambda s: s + '<p>article content after footer</p>',
    lambda s: s + footer(),
    lambda s: s.replace(URL, URL + 'neighbour/'),
    lambda s: s.replace('Originally published on ', 'Unknown source notice '),
])
def test_unverified_envelopes_use_fallback_not_invented_byline_or_body(registry, mutate):
    records, errors = parse(registry, mutate(fragment()))
    assert not records and errors[0]['url'] == URL


@pytest.mark.parametrize('markup,rights', [
    (fragment(''), 'All rights reserved'),
    (fragment('<p class="paywall">Pay to read</p>'), ''),
    (fragment('<script type="application/ld+json">{"isAccessibleForFree": false}</script>'), ''),
    (fragment('').replace('Writer</a>', 'Different</a>'), 'All rights reserved'),
])
def test_restricted_short_empty_or_bad_byline_refuses_shared_raw_before_body_checks(registry, markup, rights):
    records, errors = parse(registry, markup, rights=rights)
    assert not records and errors[0]['error'] == 'article_rights_override'


def test_missing_native_envelope_does_not_grant_a_short_body_exception(registry):
    records, errors = parse(registry, '<p>' + BODY + '</p>')
    assert not records and errors[0]['error'] == 'body_not_found'
    records, errors = parse(registry, '<p>' + BODY * 4 + '</p>')
    assert not errors and records[0]['body'] == BODY * 4
    assert records[0]['feed_envelope_metadata_json'] is None and not records[0]['verified_short_feed_body']


def test_very_short_explicit_complete_prose_needs_no_padding_or_html_request(registry):
    payload = rss([item(fragment('<p>A complete note.</p>'))])
    response = Response(ROOT, 200, {}, payload)
    source = registry.sources['globalvoices']
    old, errors = feed_records(response, SimpleNamespace(registry=registry), source, body_contract_version=1)
    assert not old and errors[0]['error'] == 'body_not_found'
    rows, errors = feed_records(response, SimpleNamespace(registry=registry), source)
    assert not errors and rows[0]['body'] == 'A complete note.' and rows[0]['verified_short_feed_body']


def gv_fixture(config, tmp_path):
    cfg, _, _ = load_config(Path('configs/news_fulltext.json'))
    entry = deepcopy(next(p for p in cfg['providers'] if p['id'] == 'globalvoices'))
    entry['historical_fulltext_feeds'] = []
    config['providers'] = [entry]
    http, session = http_fixture(config, tmp_path, {})
    return http, NewsQueue(tmp_path, http.registry), session


def test_v1_v2_and_v3_in_one_shared_durable_envelope_replay_without_network(config, tmp_path, monkeypatch):
    http, queue, session = gv_fixture(config, tmp_path)
    try:
        seed_discovery(queue)
        second = URL.replace('synthetic-story/', 'second-story/')
        third = URL.replace('synthetic-story/', 'third-story/')
        payload = rss([item(fragment('<p>' + BODY * 4 + '</p>')), item(url=second), item(url=third)])
        response = Response(ROOT, 200, {}, payload)
        old, _ = feed_records(response, http, http.registry.sources['globalvoices'], body_contract_version=1)
        v2, _ = feed_records(response, http, http.registry.sources['globalvoices'], body_contract_version=2)
        new, errors = feed_records(response, http, http.registry.sources['globalvoices'])
        rows = [old[0], v2[1], new[2]]
        assert 'Originally published on' in old[0]['body'] and 'Written by' in old[0]['body']
        task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (ROOT,)).fetchone())
        assert accept_discovery(queue, task, response,
            [(r['requested_url'], r['title'], r['published_at_raw']) for r in rows], rows, errors) == 3
        changes = queue.db.total_changes
        monkeypatch.setattr(http, 'get', lambda *a, **kw: pytest.fail('readback may not call publisher'))
        proof = audit(tmp_path, [URL, second, third], registry=http.registry)
        assert proof['accepted'], proof['errors']
        assert [a['extraction_method'] for a in proof['articles']] == [f'rss_content_encoded_v{v}' for v in (1, 2, 3)]
        assert all(a['current_source_replay_verified'] for a in proof['articles'])
        assert queue.db.total_changes == changes and not session.calls
    finally:
        queue.close()
        http.close()


def test_mixed_v2_short_restricted_item_never_retains_shared_raw(config, tmp_path):
    http, queue, _ = gv_fixture(config, tmp_path)
    try:
        seed_discovery(queue)
        other = URL.replace('synthetic-story/', 'protected-story/')
        payload = rss([item(), item(fragment('', url=other), url=other, rights='All rights reserved')])
        response = Response(ROOT, 200, {}, payload)
        rows, errors = feed_records(response, http, http.registry.sources['globalvoices'])
        assert len(rows) == 1 and errors[0]['error'] == 'article_rights_override'
        task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (ROOT,)).fetchone())
        assert accept_discovery(queue, task, response,
            [(r['requested_url'], r['title'], r['published_at_raw']) for r in rows], rows, errors) == 0
        assert not list(tmp_path.rglob('*.parquet'))
        assert not queue.db.execute('SELECT 1 FROM articles WHERE body_sha256 IS NOT NULL').fetchone()
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('field,value', [
    ('fulltext_feed_body_contract_version', True), ('fulltext_feed_body_contract_version', 1),
    ('fulltext_feed_data_fingerprint', 'invented'), ('extraction_method', 'rss_content_encoded_v99'),
])
def test_durable_body_version_selection_rejects_forged_contract(registry, field, value):
    row = parse(registry)[0][0]
    with pytest.raises(NewsError, match='invalid_feed_body_contract'):
        replay_feed_body_version({**row, field: value}, registry.sources['globalvoices'])


@pytest.mark.parametrize('field,value', [
    ('feed_envelope_metadata_json', '{}'), ('verified_short_feed_body', False),
    ('fulltext_feed_data_fingerprint', 'invented'), ('fulltext_feed_body_contract_version', 1),
])
def test_self_consistent_parquet_cannot_forge_v2_provenance(config, tmp_path, field, value):
    http, queue, _ = gv_fixture(config, tmp_path)
    try:
        response = Response(ROOT, 200, {}, rss([item()]))
        rows, errors = feed_records(response, http, http.registry.sources['globalvoices'])
        assert not errors
        rows[0].update({field: value, 'raw_response_body': response.body,
                        'source_response_article_id': rows[0]['article_id']})
        queue.add_articles([{'url': URL}])
        queue.accept(rows)
        before = queue.db.total_changes
        proof = audit(tmp_path, [URL], registry=http.registry)
        assert not proof['accepted'] and proof['errors']
        assert not proof['articles'][0]['current_source_replay_verified']
        assert queue.db.total_changes == before
    finally:
        queue.close()
        http.close()


def test_feed_only_version_preserves_all_html_permission_http_and_other_provider_identities(monkeypatch):
    from downloader import news_fulltext_policy as policy
    cfg, current, repo = load_config(Path('configs/news_fulltext.json'))
    monkeypatch.setattr(policy, 'GLOBALVOICES_FEED_BODY_CONTRACT_VERSION', 1)
    old = policy.PolicyRegistry(cfg, base=repo)
    for provider, source in current.sources.items():
        for field in ('data_fingerprint', 'authorization_fingerprint', 'http_capacity_source_fingerprint',
                      'parser_repair_fingerprint', 'parser_repair_capture_fingerprint'):
            assert getattr(source, field) == getattr(old.sources[provider], field), (provider, field)
        if provider != 'globalvoices':
            assert source.fulltext_feed_data_fingerprint == old.sources[provider].fulltext_feed_data_fingerprint
    source = current.sources['globalvoices']
    assert source.fulltext_feed_data_fingerprint != old.sources['globalvoices'].fulltext_feed_data_fingerprint
    assert source.prior_fulltext_feed_data_fingerprint == old.sources['globalvoices'].fulltext_feed_data_fingerprint
    assert discovery_fingerprint(old.sources['globalvoices']) in fulltext_feed_discovery_fingerprints(source)


@pytest.mark.parametrize('old_version', [1, 2])
def test_upgrade_keeps_healthy_and_refused_feed_deadlines_and_conditional_validators(config, tmp_path, old_version):
    http, queue, session = gv_fixture(config, tmp_path)
    try:
        seed_discovery(queue)
        source = http.registry.sources['globalvoices']
        from downloader.news_fulltext import _discovery_fingerprint
        old = _discovery_fingerprint(source, dict(source.fulltext_feed_body_fingerprints)[old_version])
        with queue.db:
            queue.db.execute('UPDATE discovery SET due=12345678900,parser_fingerprint=?,etag=?', (old, '"old"'))
            queue.db.execute('UPDATE discovery SET error_code=?,attempts=3 WHERE url!=?', ('discovery_http_429', ROOT))
        before = [tuple(r) for r in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
        seed_discovery(queue)
        assert [tuple(r) for r in queue.db.execute('SELECT * FROM discovery ORDER BY url')] == before
        task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (ROOT,)).fetchone())
        session.routes['https://globalvoices.org/robots.txt'] = FakeResponse(b'User-agent: *\nAllow: /\n')
        session.routes[ROOT] = FakeResponse(b'', status=304)
        response, links, records, errors = fetch_discovery(http, task)
        assert response.status == 304 and not links and not records and not errors
        assert session.calls[-1][1]['headers']['If-None-Match'] == '"old"'
        accept_discovery(queue, task, response, links, records, errors)
        row = queue.db.execute('SELECT parser_fingerprint FROM discovery WHERE url=?', (ROOT,)).fetchone()
        assert row[0] == old  # A 304 is not evidence of a new body ABI parse.
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('xml_contract_changed', [False, True])
@pytest.mark.parametrize('old_version', [1, 2])
def test_real_restart_does_not_replay_old_xml_or_rights_errors_for_body_only_change(tmp_path, monkeypatch, xml_contract_changed, old_version):
    from downloader import news_fulltext as core, news_fulltext_policy as policy
    monkeypatch.setattr(policy, 'GLOBALVOICES_FEED_BODY_CONTRACT_VERSION', old_version)
    cfg, old, repo = load_config(Path('configs/news_fulltext.json'))
    legacy = discovery_fingerprint(old.sources['globalvoices'])
    urls = [ROOT + '?paged=1129', 'https://zht.globalvoices.org/feed/?paged=46']
    queue = NewsQueue(tmp_path, old)
    try:
        with queue.db:
            for url, error in zip(urls, ['invalid_feed', 'bulk_response_contains_restricted_text']):
                queue.db.execute('INSERT INTO discovery(url,provider,kind,due,attempts,error_code,parser_fingerprint,etag) '
                    "VALUES (?,'globalvoices','rss_fulltext',12345678900,7,?,?,?)",
                    (url, error, legacy if error != 'invalid_feed' else None, '"old"'))
        expected = [tuple(r) for r in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
    finally:
        queue.close()
    monkeypatch.setattr(policy, 'GLOBALVOICES_FEED_BODY_CONTRACT_VERSION', 3)
    current = policy.PolicyRegistry(cfg, base=repo)
    if xml_contract_changed:
        original = core._discovery_fingerprint
        monkeypatch.setattr(core, '_discovery_fingerprint',
            lambda s, fingerprint: hashlib.sha256((original(s, fingerprint) + ':genuine-XML-change').encode()).hexdigest())
    queue = NewsQueue(tmp_path, current)
    try:
        actual = [tuple(r) for r in queue.db.execute('SELECT * FROM discovery ORDER BY url')]
        if xml_contract_changed:
            assert actual != expected
            assert all(r[0] == 0 for r in queue.db.execute('SELECT due FROM discovery'))
        else:
            assert actual == expected
            parser = discovery_fingerprint(current.sources['globalvoices'])
            for prefix in ('rss_xml_controls_parser_recheck:', 'mixed_bulk_scope_continuation:'):
                guard = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
                    (prefix + 'globalvoices:' + parser,)).fetchone()[0])
                assert not guard['jobs'] and guard['feed_body_only_capture_change']
                assert guard['previous_migration_marker'] == prefix + 'globalvoices:' + legacy
    finally:
        queue.close()
