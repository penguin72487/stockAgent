"""Lossless ETtoday JSON string syntax repair, not identity/access relaxation."""
from copy import deepcopy
from contextlib import closing
import json
from pathlib import Path

import pytest

from downloader.news_fulltext_ettoday import _native_json, ettoday_validation_metadata
from downloader.news_fulltext_ettoday import ETTODAY_V5_PROVED_LITERAL_CONTROL_URLS
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, ettoday_clock_only_parser_repair, ettoday_literal_value_parser_repair, utc_now
import test_news_ettoday as native
import test_news_ettoday_clock_v2 as clocks


@pytest.mark.parametrize('codepoint', range(32))
@pytest.mark.parametrize('field', ['headline', 'description', 'keywords'])
def test_literal_value_controls_are_escaped_losslessly_and_old_abi_stays_closed(codepoint, field):
    control = chr(codepoint)
    value = {'headline': native.TITLE, 'description': native.PROSE, 'keywords': ['原始值', '股']}
    value[field] = ['原始' + control + '值'] if field == 'keywords' else '原始' + control + '值'
    encoded = json.dumps(value, ensure_ascii=False, indent=2)
    literal = encoded.replace(json.dumps(control)[1:-1], control)
    before = literal
    parsed, anomalies = _native_json(literal, allow_literal_value_controls=True)
    assert parsed == value and literal == before and len(anomalies) == 1
    assert _native_json(encoded) == (value, [])
    if field == 'description' and control in '\r\n':
        assert _native_json(literal) == (parsed, anomalies)
    else:
        with pytest.raises(ValueError):
            _native_json(literal)


@pytest.mark.parametrize('text', ['{"bad\x08key":"value"}', '\x02{"description":"text"}',
    '{"description":"text"}\x08', '{"description":"\\\x02"}',
    '{"description":"text\x08",}', '{"description":"unclosed\x02}',
    '{"name":"記者"\n//另一註解\n,"description":"text\x08"}',
    '{"description":"text\x08"}eval(1)', '{"description":"text\x08" /*unknown*/}'])
def test_v5_never_changes_keys_grammar_unknown_comments_or_broken_escapes(text):
    with pytest.raises(ValueError):
        _native_json(text, allow_literal_value_controls=True)


def test_controls_do_not_confuse_escaped_quotes_slashes_nested_objects_or_observed_comment():
    value = {'author': {'name': '研究記者', 'url': 'https://example.invalid/'},
        'description': 'quote:" slash:\\ literal:\\u0008 control:\x08',
        'keywords': ['\x02', {'description': 'newline\n\r'}]}
    text = json.dumps(value, ensure_ascii=False, indent=2)
    text = text.replace('"name": "研究記者",', '"name": "研究記者",\n//如果有筆名，才會有url')
    # Only a real encoded control becomes literal: leave the escaped textual
    # sequence in description intact, not a broad replace of backslashes.
    text = text.replace('control:\\b', 'control:\x08').replace('"\\u0002"', '"\x02"')
    parsed, anomalies = _native_json(text, allow_literal_value_controls=True)
    assert parsed == value
    assert set(anomalies) == {'source_non_json_author_comment', 'source_unescaped_json_value_control'}


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('host', ['finance', 'www'])
@pytest.mark.parametrize('field,codepoint', [('description', 2), ('keywords', 8)])
def test_v5_preserves_whole_body_clocks_and_metadata_and_does_not_mutate_response(parser, host, field, codepoint):
    soup = native.page(host, parser)
    native.update_schema(soup, **{field: 'source' + chr(codepoint) + 'value'})
    raw = str(soup).encode().replace(json.dumps(chr(codepoint))[1:-1].encode(), bytes([codepoint]))
    cfg = native.config(parser)
    cfg['providers'][0]['embedded_article_layout'] = 'ettoday_visible_article_v5'
    source = PolicyRegistry(cfg, base=Path.cwd()).sources['ettoday']
    result = extract_article(raw, source, page_url=native.URLS[host == 'www'])
    assert native.PROSE in result['body'] and native.NOTICE in result['body']
    assert result['published_at_raw'] == native.PUB and result['modified_at_raw'] == native.MOD
    assert not result['historical_point_in_time'] and not result['per_article_license_verified']
    assert any(n['kind'] == 'source_unescaped_json_value_control'
        for n in json.loads(result['verified_article_notices_json']))
    for version in range(1, 5):
        old = deepcopy(cfg)
        old['providers'][0]['embedded_article_layout'] = f'ettoday_visible_article_v{version}'
        with pytest.raises(NewsError, match='article_identity_not_verified'):
            extract_article(raw, PolicyRegistry(old, base=Path.cwd()).sources['ettoday'],
                page_url=native.URLS[host == 'www'])


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
@pytest.mark.parametrize('host', ['finance', 'www'])
def test_v5_unchanged_positive_outputs_and_old_capture_http_and_authorization_identities(parser, host):
    raw = str(native.page(host, parser)).encode()
    old = [clocks.registry(v).sources['ettoday'] for v in range(1, 5)]
    new = clocks.registry(5).sources['ettoday']
    result = extract_article(raw, new, page_url=native.URLS[host == 'www'])
    assert all(extract_article(raw, source, page_url=native.URLS[host == 'www']) == result for source in old)
    assert new.data_fingerprint not in {source.data_fingerprint for source in old}
    assert new.http_capacity_source_fingerprint == old[0].data_fingerprint
    assert new.parser_repair_capture_history == tuple(source.data_fingerprint for source in old[1:])
    assert new.parser_repair_prior_capture_fingerprint == old[-1].data_fingerprint
    assert all(source.authorization_fingerprint == new.authorization_fingerprint for source in old)


@pytest.mark.parametrize('change', ['url', 'headline', 'author', 'publisher', 'publication', 'paid', 'body'])
def test_v5_lossless_syntax_never_waives_article_semantic_checks(change):
    soup = native.page()
    values = {'url': {'url': native.URLS[1]}, 'headline': {'headline': native.TITLE + '\x08'},
        'author': {'author': {'@type': 'Person', 'name': '另一記者'}},
        'publisher': {'publisher': {'@type': 'Organization', 'name': '其他媒體'}},
        'publication': {'datePublished': native.PUB + '\x02'},
        'paid': {'isAccessibleForFree': False}, 'body': {'articleBody': native.PROSE}}
    native.update_schema(soup, description='source\x08value', **values[change])
    raw = str(soup).encode().replace(b'\\b', b'\x08').replace(b'\\u0002', b'\x02')
    with pytest.raises(NewsError):
        extract_article(raw, clocks.registry(5).sources['ettoday'], page_url=native.URLS[0])


def test_v5_diagnostic_reports_syntax_shape_only_using_the_selected_parser_abi():
    from bs4 import BeautifulSoup
    soup = native.page()
    native.update_schema(soup, description=native.PROSE + '\x02')
    soup = BeautifulSoup(str(soup).replace('\\u0002', '\x02'), 'lxml')
    old = ettoday_validation_metadata(soup, page_url=native.URLS[0])
    new = ettoday_validation_metadata(soup, page_url=native.URLS[0], adapter_contract_version=5)
    assert old['schema_error'] and not new['schema_error']
    assert new['contract_version'] == 3 and new['article_schema_count'] == 1
    assert new['observed_schema_syntax_anomalies'] == ['source_unescaped_json_value_control']
    assert not new['body_text_retained'] and not new['raw_scope_or_article_acceptance_granted']
    assert native.PROSE not in json.dumps(new, ensure_ascii=False)


@pytest.mark.parametrize('old_version', range(1, 5))
def test_v5_only_proved_missing_identity_failures_retry_once_without_deadline_error_or_clock_replay(tmp_path, old_version):
    old, new = clocks.registry(old_version, tmp_path), clocks.registry(5, tmp_path)
    assert ettoday_literal_value_parser_repair(new.sources['ettoday'], old.sources['ettoday'].data_fingerprint)
    assert not ettoday_clock_only_parser_repair(new.sources['ettoday'], old.sources['ettoday'].data_fingerprint)
    selected = ETTODAY_V5_PROVED_LITERAL_CONTROL_URLS
    protected = ['body_not_found', 'article_attribution_not_verified', 'invalid_bulk_publication_clock',
        'http_403', 'http_429', 'paywall_or_login', 'source_not_found', 'complete', 'article_identity_not_verified']
    before = {}
    with closing(NewsQueue(tmp_path, old)) as queue:
        for number, (url, state) in enumerate([(url, 'article_identity_not_verified') for url in selected] +
                [(f'https://finance.ettoday.net/news/{100+number}', state) for number, state in enumerate(protected)]):
            queue.add_articles([{'url': url}])
            queue.db.execute('UPDATE articles SET state=?,error_code=?,attempts=4,due=?,last_checked_at_utc=? WHERE url=?',
                (state, state, 9000000000 if number == 0 else 0, utc_now(), url))
            before[url] = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
        queue.db.commit()
    marker = 'ettoday_literal_json_value_recheck_v5:' + new.sources['ettoday'].data_fingerprint
    with closing(NewsQueue(tmp_path, new)) as queue:
        for url, row in before.items():
            expected = {**row, 'state': 'retry'} if url in selected else row
            assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == expected
        receipt = queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0]
        proof = json.loads(receipt)
        assert [row['url'] for row in proof['rows']] == list(selected)
        assert proof['rows'][0]['due'] == 9000000000 and proof['rows'][0]['attempts'] == 4
        assert not proof['accepted_as_bodies']
        assert not queue.db.execute("SELECT 1 FROM queue_settings WHERE key LIKE 'ettoday_independent_release_revision_recheck_v5:%'").fetchone()
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified' WHERE url=?", (selected[1],))
        queue.db.commit()
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert queue.db.execute('SELECT state FROM articles WHERE url=?', (selected[1],)).fetchone()[0] == 'article_identity_not_verified'
        assert queue.db.execute('SELECT value FROM queue_settings WHERE key=?', (marker,)).fetchone()[0] == receipt


@pytest.mark.parametrize('change', ['stored_hash', 'stored_path', 'lease_owner', 'lease_until', 'missing_observation',
    'different_error', 'different_state', 'unknown_prior_capture', 'unknown_host', 'query', 'other_url'])
def test_v5_retry_never_reopens_unproved_heads(tmp_path, change):
    old, new = clocks.registry(4, tmp_path), clocks.registry(5, tmp_path)
    url = ETTODAY_V5_PROVED_LITERAL_CONTROL_URLS[0]
    if change == 'unknown_host': url = url.replace('finance.', 'house.')
    elif change == 'query': url += '?foo=1'
    elif change == 'other_url': url += '0'
    columns = {'stored_hash': ('body_sha256', 'a' * 64), 'stored_path': ('parquet_path', 'bodies/old.parquet'),
        'lease_owner': ('lease_owner', 'other-owner'), 'lease_until': ('lease_until', 9000000000),
        'missing_observation': ('last_checked_at_utc', None), 'different_error': ('error_code', 'http_403'),
        'different_state': ('state', 'http_403')}
    with closing(NewsQueue(tmp_path, old)) as queue:
        queue.add_articles([{'url': url}])
        queue.db.execute("UPDATE articles SET state='article_identity_not_verified',error_code='article_identity_not_verified',"
            'attempts=4,due=9000000000,last_checked_at_utc=? WHERE url=?', (utc_now(), url))
        if change in columns:
            column, value = columns[change]
            queue.db.execute('UPDATE articles SET ' + column + '=? WHERE url=?', (value, url))
        elif change == 'unknown_prior_capture':
            queue.db.execute('UPDATE queue_settings SET value=? WHERE key=?', ('b' * 64, 'source_capture_fingerprint:ettoday'))
        queue.db.commit()
        before = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone())
    with closing(NewsQueue(tmp_path, new)) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (url,)).fetchone()) == before


@pytest.mark.parametrize('change', ['scope', 'evidence', 'disabled', 'layout', 'unknown'])
def test_v5_retry_compatibility_is_not_a_changed_permission_or_parser_grant(change):
    old = clocks.registry(4).sources['ettoday']
    cfg = clocks.config(5)
    entry = cfg['providers'][0]
    if change == 'scope': entry['authorization']['scope'] += '; changed source scope'
    elif change == 'evidence': entry['authorization']['reviewed_on'] = '2026-10-16'
    elif change == 'disabled': entry['enabled'] = False
    elif change == 'layout': entry['embedded_article_layout'] = 'ettoday_visible_article_v4'
    new = PolicyRegistry(cfg, base=Path.cwd()).sources['ettoday']
    previous = 'b' * 64 if change == 'unknown' else old.data_fingerprint
    assert not ettoday_literal_value_parser_repair(new, previous)
