"""Scheduling/review changes must not replay unchanged parser failures."""
from copy import deepcopy
from dataclasses import replace
import json
from pathlib import Path
import time

import pytest

from downloader.news_fulltext import load_config
from downloader.news_fulltext_policy import PolicyRegistry, parser_repair_fingerprint
from downloader.news_fulltext_queue import NewsQueue, parser_repair_changed, parser_repair_identity
from test_news_fulltext import config


@pytest.fixture
def namespace_config(config):
    result = deepcopy(config)
    result['providers'][0].update(article_path_pattern=r'^/story/[0-9]+$', source_not_found_paths=['/not-found'])
    return result


def outcome_queue(cfg, root, state, error=None):
    queue = NewsQueue(root, PolicyRegistry(cfg, base=root))
    with queue.db:
        queue.add_articles([{'url': 'https://publisher.test/story/1'}])
        queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=7,due=?,last_checked_at_utc=?',
                         (state, error or state, time.time() + 7200, '2026-10-14T04:00:00+00:00'))
    head = dict(queue.db.execute('SELECT * FROM articles').fetchone())
    source = queue.registry.sources['publisher']
    marker = queue.db.execute("SELECT value FROM queue_settings WHERE key='source_parser_repair_fingerprint:publisher'").fetchone()[0]
    assert json.loads(marker) == parser_repair_identity(source)
    return queue, source, head


PROSE_OR_SCHEDULING = [
    ('acquisition_mode', 'finite'), ('private_known_url_scope', 'personal_noncommercial_originals_v1'),
    ('note', 'Changed known-URL scheduling review only'), ('authorization.scope', 'Reviewed same original-text scope'),
    ('authorization.reviewed_on', '2026-10-14'), ('interval_seconds', 3), ('parallelism', 2),
    ('discovery_interval_seconds', 7200), ('official_min_interval_seconds', 1),
    ('official_rate_limit_evidence_url', 'https://publisher.test/rates'),
]


def amend(cfg, field, value):
    changed = deepcopy(cfg)
    entry = changed['providers'][0]
    if field.startswith('authorization.'):
        entry['authorization'][field.split('.', 1)[1]] = value
    else:
        entry[field] = value
    if field == 'official_min_interval_seconds':
        entry['official_rate_limit_evidence_url'] = 'https://publisher.test/rates'
    return changed


@pytest.mark.parametrize('field,value', PROSE_OR_SCHEDULING)
@pytest.mark.parametrize('state', ['body_not_found', 'article_attribution_not_verified',
                                  'article_title_not_verified', 'unsupported_article_path', 'redirect_not_article'])
def test_proven_same_parser_does_not_replay_failures(namespace_config, tmp_path, field, value, state):
    old, old_source, protected = outcome_queue(namespace_config, tmp_path, state)
    old.close()
    changed = amend(namespace_config, field, value)
    new = NewsQueue(tmp_path, PolicyRegistry(changed, base=tmp_path))
    try:
        source = new.registry.sources['publisher']
        assert source.data_fingerprint != old_source.data_fingerprint
        assert source.parser_repair_fingerprint == old_source.parser_repair_fingerprint
        assert dict(new.db.execute('SELECT * FROM articles').fetchone()) == protected
        marker = new.db.execute("SELECT value FROM queue_settings WHERE key='source_parser_repair_fingerprint:publisher'").fetchone()[0]
        assert json.loads(marker) == parser_repair_identity(source)
    finally:
        new.close()


@pytest.mark.parametrize('field,value', [
    ('article_selectors', ['.new-story']), ('article_path_pattern', r'^/story/\d+$'),
    ('author_selectors', ['.byline']), ('publication_selectors', ['time[datetime]']),
    ('require_author_attribution', True), ('exclude_selectors', ['.advertising']),
    ('authorization.license', 'Actual-source-license-change'),
])
@pytest.mark.parametrize('state', ['body_not_found', 'redirect_not_article'])
def test_actual_parser_or_enforced_rights_change_keeps_legacy_repair(namespace_config, tmp_path, field, value, state):
    old, old_source, _ = outcome_queue(namespace_config, tmp_path, state)
    old.close()
    new = NewsQueue(tmp_path, PolicyRegistry(amend(namespace_config, field, value), base=tmp_path))
    try:
        source = new.registry.sources['publisher']
        assert source.parser_repair_fingerprint != old_source.parser_repair_fingerprint
        row = new.db.execute('SELECT state,attempts,due,error_code FROM articles').fetchone()
        # The existing shared parser-repair lane moves body failures to
        # retry, without erasing their original error. Other path repairs
        # remain pending; this change must preserve both behaviors.
        assert tuple(row) == ('retry' if state == 'body_not_found' else 'pending', 0, 0, state)
    finally:
        new.close()


@pytest.mark.parametrize('state,error', [
    ('robots_disallowed', 'robots_disallowed'), ('paywall_or_login', 'paywall_or_login'),
    ('article_rights_or_scope_excluded', 'article_rights_or_scope_excluded'),
    ('source_not_found', 'source_not_found'), ('source_article_empty_template', 'source_article_empty_template'),
    ('retry', 'http_429'), ('retry_cooldown', 'http_403'),
])
@pytest.mark.parametrize('field,value', [('note', 'Review only'), ('article_selectors', ['.new-story'])])
def test_real_refusals_and_future_deadlines_stay_exact(namespace_config, tmp_path, state, error, field, value):
    old, _, protected = outcome_queue(namespace_config, tmp_path, state, error)
    old.close()
    new = NewsQueue(tmp_path, PolicyRegistry(amend(namespace_config, field, value), base=tmp_path))
    try:
        assert dict(new.db.execute('SELECT * FROM articles').fetchone()) == protected
    finally:
        new.close()


@pytest.mark.parametrize('bad', [None, '', 'invalid', '{}', '[]', 'null', 'x' * 513,
    json.dumps({'contract_version': True, 'data_fingerprint': 'a' * 64, 'parser_fingerprint': 'b' * 64}),
    json.dumps({'contract_version': 99, 'data_fingerprint': 'a' * 64, 'parser_fingerprint': 'b' * 64}),
])
def test_absent_or_malformed_marker_never_suppresses_legacy_repair(namespace_config, tmp_path, bad):
    old, _, _ = outcome_queue(namespace_config, tmp_path, 'redirect_not_article')
    with old.db:
        if bad is None:
            old.db.execute("DELETE FROM queue_settings WHERE key='source_parser_repair_fingerprint:publisher'")
        else:
            old.db.execute("UPDATE queue_settings SET value=? WHERE key='source_parser_repair_fingerprint:publisher'", (bad,))
    old.close()
    new = NewsQueue(tmp_path, PolicyRegistry(amend(namespace_config, 'note', 'Review only'), base=tmp_path))
    try:
        assert new.db.execute('SELECT state FROM articles').fetchone()[0] == 'pending'
    finally:
        new.close()


@pytest.mark.parametrize('mutation', ['capture', 'parser', 'version', 'extra'])
def test_marker_must_bind_the_exact_previous_capture(namespace_config, tmp_path, mutation):
    source = PolicyRegistry(namespace_config, base=tmp_path).sources['publisher']
    old = parser_repair_identity(source)
    current = PolicyRegistry(amend(namespace_config, 'note', 'Review only'), base=tmp_path).sources['publisher']
    if mutation == 'capture':
        old['data_fingerprint'] = '0' * 64
    elif mutation == 'parser':
        old['parser_fingerprint'] = '0' * 64
    elif mutation == 'version':
        old['contract_version'] = True
    else:
        old['extra'] = 'not-the-versioned-contract'
    assert parser_repair_changed(current, source.data_fingerprint, json.dumps(old))


def test_unmodified_owner_backfills_marker_without_replaying_any_row(namespace_config, tmp_path):
    old, source, protected = outcome_queue(namespace_config, tmp_path, 'redirect_not_article')
    with old.db:
        old.db.execute("DELETE FROM queue_settings WHERE key='source_parser_repair_fingerprint:publisher'")
    old.close()
    new = NewsQueue(tmp_path, PolicyRegistry(namespace_config, base=tmp_path))
    try:
        assert dict(new.db.execute('SELECT * FROM articles').fetchone()) == protected
        assert json.loads(new.db.execute("SELECT value FROM queue_settings WHERE key='source_parser_repair_fingerprint:publisher'").fetchone()[0]) == parser_repair_identity(source)
    finally:
        new.close()


def test_legacy_replaced_capture_is_not_attested_as_current_parser(namespace_config, tmp_path):
    registry = PolicyRegistry(namespace_config, base=tmp_path)
    current = registry.sources['publisher']
    legacy = replace(current, data_fingerprint='0' * 64)
    assert parser_repair_identity(legacy) is None
    registry.sources['publisher'] = legacy
    old = NewsQueue(tmp_path, registry)
    with old.db:
        old.add_articles([{'url': 'https://publisher.test/story/1'}])
        old.db.execute("UPDATE articles SET state='body_not_found',error_code='body_not_found',attempts=2")
    assert old.db.execute("SELECT 1 FROM queue_settings WHERE key='source_parser_repair_fingerprint:publisher'").fetchone() is None
    old.close()
    registry.sources['publisher'] = current
    new = NewsQueue(tmp_path, registry)
    try:
        assert new.db.execute('SELECT state FROM articles').fetchone()[0] == 'pending'
    finally:
        new.close()


def test_allowed_classification_is_not_replaced_by_parser_marker(namespace_config, tmp_path):
    denied = amend(namespace_config, 'enabled', False)
    old = NewsQueue(tmp_path, PolicyRegistry(denied, base=tmp_path))
    with old.db:
        old.add_articles([{'url': 'https://publisher.test/story/1'}])
    assert old.db.execute('SELECT state FROM articles').fetchone()[0] == 'awaiting_authorization'
    old.close()
    new = NewsQueue(tmp_path, PolicyRegistry(namespace_config, base=tmp_path))
    try:
        assert new.db.execute('SELECT state FROM articles').fetchone()[0] == 'pending'
    finally:
        new.close()


def test_parser_version_and_actual_enforced_scope_are_not_normalized_away():
    payload = {'version': 1, 'authorization': {'kind': 'open_license', 'scope': 'originals', 'reviewed_on': 'old'},
               'entry': {'acquisition_mode': 'finite', 'authorization': {'kind': 'open_license', 'scope': 'originals'},
                         'permitted_author_names': ['Original Author']}, 'parser_version': 1}
    original = deepcopy(payload)
    fingerprint = parser_repair_fingerprint(payload)
    assert payload == original
    changed = deepcopy(payload)
    changed['parser_version'] = 2
    assert parser_repair_fingerprint(changed) != fingerprint
    changed = deepcopy(payload)
    changed['entry']['permitted_author_names'] = ['Another Author']
    assert parser_repair_fingerprint(changed) != fingerprint
    changed = deepcopy(payload)
    changed['authorization']['valid_until'] = '2027-01-01T00:00:00Z'
    assert parser_repair_fingerprint(changed) != fingerprint


def test_all_real_sources_attest_current_complete_capture_without_changing_usage_abi():
    _, registry, _ = load_config(Path('configs/news_fulltext.json'))
    for source in registry.sources.values():
        assert parser_repair_identity(source) == {
            'contract_version': 1, 'data_fingerprint': source.data_fingerprint,
            'parser_fingerprint': source.parser_repair_fingerprint}
        assert source.authorization_fingerprint != source.parser_repair_fingerprint


@pytest.mark.parametrize('error', ['invalid_feed', 'bulk_response_contains_restricted_text'])
@pytest.mark.parametrize('field,value', [('note', 'Review only'), ('article_selectors', ['.new-story'])])
def test_discovery_migrations_share_the_proven_scheduling_only_boundary(config, tmp_path, error, field, value):
    cfg = deepcopy(config)
    cfg['providers'][0]['fulltext_feeds'] = ['https://publisher.test/feed/']
    old = NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))
    with old.db:
        old.db.execute("INSERT INTO discovery(url,kind,provider,due,error_code,attempts,etag,parser_fingerprint) "
                       "VALUES ('https://publisher.test/feed/?paged=2','rss_fulltext','publisher',?,?,7,'old','old')",
                       (time.time() + 3600, error))
    protected = dict(old.db.execute('SELECT * FROM discovery').fetchone())
    old.close()
    registry = PolicyRegistry(amend(cfg, field, value), base=tmp_path)
    new = NewsQueue(tmp_path, registry)
    try:
        row = dict(new.db.execute('SELECT * FROM discovery').fetchone())
        if field == 'note':
            assert row == protected
        else:
            assert row == {**protected, 'due': 0, 'etag': None, 'last_modified': None, 'parser_fingerprint': None}
    finally:
        new.close()
    again = NewsQueue(tmp_path, registry)
    try:
        assert dict(again.db.execute('SELECT * FROM discovery').fetchone()) == row
    finally:
        again.close()
