"""Native publication/revision proof and exact, once-only failed-clock repair."""
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
import hashlib
import json
from pathlib import Path

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext_capacity import PublisherCapacity
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, ftnn_clock_only_parser_repair
from test_news_failure_diagnostics import fail
from test_news_failure_identity_metadata import evidence
from test_news_ftnn import URL, PROSE, NOTICE, page, extract, policy_config, update_schema
from test_news_fulltext import http_fixture


def revised_page(parser='lxml', *, padded=False, reverse=False):
    soup = page(parser)
    update_schema(soup, 'datePublished', '2024-07-02T08:51:00+08:00')
    update_schema(soup, 'dateModified', '2024-07-02T09:16:06+08:00')
    clock = soup.select_one('.ndt-left p')
    date = '2024/07/02' if padded else '2024/7/2'
    clock.string = '發布時間：' + date + ' 08:51'
    changed = soup.new_tag('p')
    changed.string = '異動時間：' + date + ' 09:16'
    clock.insert_before(changed) if reverse else clock.insert_after(changed)
    return soup


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('padded,reverse', [(False, False), (True, False), (False, True), (True, True)])
def test_independent_publication_and_revision_labels_keep_exact_clocks_and_whole_body(parser, padded, reverse):
    soup = revised_page(parser, padded=padded, reverse=reverse)
    before = str(soup)
    result = extract(soup, parser)
    assert result['published_at_raw'] == '2024-07-02T08:51:00+08:00'
    assert result['modified_at_raw'] == '2024-07-02T09:16:06+08:00'
    assert PROSE in result['body'] and NOTICE in result['body']
    assert result['body_sha256'] == hashlib.sha256(result['body'].encode()).hexdigest()
    labels = json.loads(result['verified_article_notices_json'])
    assert {row['kind'] for row in labels} == {'native_visible_release_label', 'native_visible_modification_label'}
    assert [row['text'] for row in labels] == [node.get_text(' ', strip=True) for node in soup.select('.ndt-left p')]
    assert 'release_and_modification_minutes' in result['published_time_basis']
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    assert str(soup) == before


@pytest.mark.parametrize('change', ['publication_mismatch', 'revision_mismatch', 'revision_precedes',
    'only_revision', 'duplicate_publication', 'duplicate_revision', 'unknown_label', 'blank_extra',
    'missing_clock', 'date_invalid', 'naive', 'other_timezone', 'hidden_revision', 'hidden_publication',
    'related_revision_only', 'hidden_revision_descendant'])
def test_ambiguous_missing_hidden_or_inconsistent_source_clocks_still_fail(change):
    soup = revised_page()
    pub, rev = soup.select('.ndt-left p')
    if change == 'publication_mismatch':
        pub.string = '發布時間：2024/7/2 08:52'
    elif change == 'revision_mismatch':
        rev.string = '異動時間：2024/7/2 09:17'
    elif change == 'revision_precedes':
        update_schema(soup, 'dateModified', '2024-07-02T08:50:06+08:00')
        rev.string = '異動時間：2024/7/2 08:50'
    elif change == 'only_revision':
        pub.decompose()
    elif change == 'duplicate_publication':
        rev.string = pub.get_text()
    elif change == 'duplicate_revision':
        rev.insert_after(deepcopy(rev))
    elif change == 'unknown_label':
        rev.string = '更新時間：2024/7/2 09:16'
    elif change == 'blank_extra':
        rev.string = ''
    elif change == 'missing_clock':
        pub.decompose()
        rev.decompose()
    elif change == 'date_invalid':
        rev.string = '異動時間：2024/2/30 09:16'
    elif change == 'naive':
        update_schema(soup, 'dateModified', '2024-07-02T09:16:06')
    elif change == 'other_timezone':
        update_schema(soup, 'dateModified', '2024-07-02T09:16:06+00:00')
    elif change == 'hidden_revision':
        rev['hidden'] = ''
    elif change == 'hidden_publication':
        pub['aria-hidden'] = 'true'
    elif change == 'hidden_revision_descendant':
        text = rev.get_text()
        rev.clear()
        span = soup.new_tag('span', hidden='')
        span.string = text
        rev.append(span)
    else:
        soup.select_one('.news-page-right').append(rev.extract())
        update_schema(soup, 'datePublished', '2024-07-02T09:16:06+08:00')
    with pytest.raises(NewsError):
        extract(soup)


@pytest.mark.parametrize('change', ['canonical', 'title', 'paid', 'external_byline', 'body_hidden', 'script'])
def test_two_valid_clocks_do_not_waive_identity_rights_or_body_checks(change):
    soup = revised_page()
    if change == 'canonical':
        soup.select_one('link[rel=canonical]')['href'] = URL + '0'
    elif change == 'title':
        soup.select_one('h1').string = '別篇'
    elif change == 'paid':
        update_schema(soup, 'isAccessibleForFree', False)
    elif change == 'external_byline':
        soup.select_one('.news-body p').string = '中央社記者黃詩雯／綜合報導'
    elif change == 'body_hidden':
        soup.select_one('.news-body')['hidden'] = ''
    else:
        soup.select_one('.news-body').append(soup.new_tag('script'))
    with pytest.raises(NewsError):
        extract(soup)


def test_single_release_preserves_previous_labels_basis_and_current_observation_semantics():
    result = extract(page())
    assert result['article_identity_basis'].endswith('visible_release_minute_v1')
    assert result['published_time_basis'] == 'source_declared_aware_datetime_corresponds_to_visible_release_minute; not_observed_historical_PIT'
    assert json.loads(result['verified_article_notices_json']) == [
        {'kind': 'native_visible_release_label', 'text': '發布時間：2026/10/14 22:00'}]


def legacy_registry(cfg, tmp_path, monkeypatch):
    sha = hashlib.sha256
    def old_payload(value=b''):
        try:
            payload = json.loads(value)
        except (ValueError, TypeError, UnicodeDecodeError):
            return sha(value)
        if (isinstance(payload, dict) and payload.get('entry', {}).get('id') == 'ftnn'
                and payload.get('embedded_article_adapter_contract_version') == 2):
            payload['embedded_article_adapter_contract_version'] = 1
            return sha(json.dumps(payload, sort_keys=True).encode())
        return sha(value)
    with monkeypatch.context() as patch:
        patch.setattr('downloader.news_fulltext_policy.hashlib.sha256', old_payload)
        registry = PolicyRegistry(cfg, base=tmp_path)
    registry.sources['ftnn'] = replace(registry.sources['ftnn'], http_capacity_source_fingerprint='')
    return registry


def test_exact_old_capture_is_the_normalized_http_identity_not_a_permission_alias(tmp_path, monkeypatch):
    cfg = policy_config()
    old = legacy_registry(cfg, tmp_path, monkeypatch).sources['ftnn']
    new = PolicyRegistry(cfg, base=tmp_path).sources['ftnn']
    assert old.data_fingerprint != new.data_fingerprint
    assert new.http_capacity_source_fingerprint == old.data_fingerprint
    assert new.authorization_fingerprint == old.authorization_fingerprint
    assert old.parser_repair_fingerprint != new.parser_repair_fingerprint
    assert ftnn_clock_only_parser_repair(new, old.data_fingerprint)
    assert not ftnn_clock_only_parser_repair(new, new.data_fingerprint)
    assert not ftnn_clock_only_parser_repair(new, 'unknown-prior-version')
    changed = deepcopy(cfg)
    changed['providers'][0]['interval_seconds'] = 3
    assert not ftnn_clock_only_parser_repair(PolicyRegistry(changed, base=tmp_path).sources['ftnn'], old.data_fingerprint)


def test_effective_rate_learning_and_real_429_deadline_survive_clock_parser_repair(tmp_path):
    cfg = policy_config()
    cfg['adaptive_rate'] = {'enabled': False}
    old_http, session = http_fixture(cfg, tmp_path, {})
    new_source = old_http.registry.sources['ftnn']
    old_http.registry.sources['ftnn'] = replace(new_source,
        data_fingerprint=new_source.http_capacity_source_fingerprint, http_capacity_source_fingerprint='')
    try:
        old = PublisherCapacity(old_http, cfg)
        old_http.set_client_capacity('ftnn', .25, 2)
        with pytest.raises(NewsError, match='http_429'):
            old_http._response_status('ftnn', 429, {'retry-after': '120'})
        old.states['ftnn'].update(interval_seconds=.5, parallelism=2, state='stable_pipeline_capacity')
        atomic_write_json(old.path, old.snapshot(), durable=True)
        refusal = json.loads((tmp_path / 'cooldowns/ftnn.json').read_bytes())
        assert not session.calls
    finally:
        old_http.close()
    new_http, session = http_fixture(cfg, tmp_path, {})
    try:
        new = PublisherCapacity(new_http, cfg)
        assert new.fingerprint == old.fingerprint and new.provider_fingerprints == old.provider_fingerprints
        assert new.states['ftnn']['state'] == 'stable_pipeline_capacity'
        assert new.states['ftnn']['interval_seconds'] == .5 and new.states['ftnn']['parallelism'] == 2
        assert new_http.rate_refusal_intervals['ftnn'] == .5
        assert json.loads((tmp_path / 'cooldowns/ftnn.json').read_bytes()) == refusal
        assert not session.calls
    finally:
        new_http.close()


def test_exact_old_missing_clock_failures_retry_once_without_resetting_any_other_evidence(tmp_path, monkeypatch):
    cfg = policy_config()
    old = legacy_registry(cfg, tmp_path, monkeypatch)
    original = {}
    with closing(NewsQueue(tmp_path, old)) as queue:
        for number, state in enumerate(['invalid_bulk_publication_clock', 'body_not_found',
                'article_attribution_not_verified', 'article_identity_not_verified', 'article_rights_or_scope_excluded',
                'http_403', 'http_429', 'paywall_or_login', 'source_not_found', 'complete'], 1):
            url = 'https://www.ftnn.com.tw/news/' + str(number)
            queue.add_articles([{'url': url}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=3,due=9000000000,'
                'last_checked_at_utc=? WHERE url=?', (state, state, '2026-10-14T22:27:32+00:00', url))
            original[url] = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        queue.db.commit()
    registry = PolicyRegistry(cfg, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        for url, prior in original.items():
            current = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
            expected = {**prior, 'state': 'retry'} if url.endswith('/1') else prior
            assert current == expected
        marker = 'ftnn_native_release_revision_recheck_v1:' + registry.sources['ftnn'].data_fingerprint
        receipt = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0]
        proof = json.loads(receipt)
        assert len(proof['rows']) == 1 and proof['rows'][0]['attempts'] == 3
        assert proof['rows'][0]['due'] == 9000000000
        queue.db.execute("UPDATE articles SET state='invalid_bulk_publication_clock' WHERE url=?", (next(iter(original)),))
        queue.db.commit()
    with closing(NewsQueue(tmp_path, registry)) as queue:
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (next(iter(original)),)).fetchone()[0] == 'invalid_bulk_publication_clock'
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == receipt


@pytest.mark.parametrize('change', ['stored_hash', 'stored_path', 'lease_owner', 'lease_until',
    'missing_observation', 'different_error', 'unknown_prior_capture'])
def test_clock_repair_does_not_touch_unproved_leased_stored_or_different_failures(tmp_path, monkeypatch, change):
    cfg = policy_config()
    registry = legacy_registry(cfg, tmp_path, monkeypatch)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        queue.add_articles([{'url': URL}])
        queue.db.execute("UPDATE articles SET state='invalid_bulk_publication_clock',"
            "error_code='invalid_bulk_publication_clock',attempts=4,due=9000000000,last_checked_at_utc=? WHERE url=?",
            ('2026-10-14T22:27:32+00:00', URL))
        columns = {'stored_hash': ('body_sha256', 'a' * 64), 'stored_path': ('parquet_path', 'bodies/old.parquet'),
            'lease_owner': ('lease_owner', 'another-live-owner'), 'lease_until': ('lease_until', 9000000000),
            'missing_observation': ('last_checked_at_utc', None), 'different_error': ('error_code', 'http_403')}
        if change in columns:
            column, value = columns[change]
            queue.db.execute('UPDATE articles SET ' + column + '=? WHERE url=?', (value, URL))
        else:
            queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?',
                ('b' * 64, 'source_capture_fingerprint:ftnn'))
        queue.db.commit()
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone()) == before


def test_dual_clock_failure_metadata_preserves_each_label_without_raw_or_text_grant(tmp_path):
    from downloader.news_fulltext_http import Response
    soup = revised_page()
    update_schema(soup, 'dateModified', '2024-07-02T09:17:06+08:00')
    with closing(NewsQueue(tmp_path, PolicyRegistry(policy_config(), base=tmp_path))) as queue:
        fail(queue, Response(URL, 200, {'content-type': 'text/html'}, str(soup).encode()),
             code='invalid_bulk_publication_clock', received_at_utc='2026-10-14T22:27:32+00:00')
        paths, rows = evidence(tmp_path)
        assert len(paths) == 1
        record = rows[0]
        facts = record['facts']['ftnn_native_validation']
        assert facts['contract_version'] == 2 and facts['release_node_count'] == 2
        assert [row['kind'] for row in facts['native_clock_labels']] == ['publication', 'modification']
        assert [row['text'] for row in facts['native_clock_labels']] == ['發布時間：2024/7/2 08:51', '異動時間：2024/7/2 09:16']
        assert not facts['body_text_retained'] and not record['raw_response_retained']
        assert not record['accepted_as_article'] and not record['publish'] and not record['remote_training_export']
        assert '黃詩雯' not in paths[0].read_text() and PROSE not in paths[0].read_text()
