"""Private typed scope reasons do not grant access, alter budgets or accept text."""
from contextlib import closing
from dataclasses import replace
from datetime import datetime, timezone
import json

import pytest

from downloader import news_fulltext_diagnostics as diagnostics
from downloader import news_fulltext_publishers as publishers
from downloader.artifact_io import sha256_file
from downloader.news_fulltext_failure_focus import configure_focus
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_cnyes_news_bulk import cfg as cnyes_config
from test_news_cnyes_html import CLOCK, TITLE, URL, short_page
from test_news_cnyes_canonical_alias import MOBILE, mobile_page, streamed_mobile_page
from test_news_failure_diagnostics import fail, reports, globalvoices_registry
from test_news_failure_focus import spec
from test_news_globalvoices_small_body import URL as GV_URL, page as gv_page


def context(raw, source, requested=URL):
    return diagnostics.FailureResponse(raw, source.provider, source.data_fingerprint,
                                      requested, datetime.now(timezone.utc).isoformat())


@pytest.mark.parametrize('case,reason', [
    ('status', 'not_cnyes_or_not_success'),
    ('host', 'request_identity_not_verified'),
    ('id', 'request_identity_not_verified'),
    ('fragment', 'request_identity_not_verified'),
    ('final', 'response_identity_or_mime_not_verified'),
    ('mime', 'response_identity_or_mime_not_verified'),
    ('query', 'unreviewed_query'),
    ('canonical', 'canonical_identity_not_verified'),
    ('layout', 'visible_article_layout_not_verified'),
    ('data', 'current_article_data_not_verified'),
    ('payment', 'current_free_payment_not_verified'),
    ('boolean_payment', 'current_free_payment_not_verified'),
    ('epoch', 'current_publication_epoch_type_not_verified'),
    ('title', 'current_title_type_not_verified'),
    ('data_url', 'current_url_identity_not_verified'),
    ('visible_title', 'visible_current_title_not_verified'),
    ('excluded_title', 'excluded_article_title'),
    ('json', 'schema_json_not_verified'),
    ('schema_missing', 'matching_schema_not_found'),
    ('schema_clock', 'schema_clock_or_title_or_access_not_verified'),
    ('schema_naive_clock', 'schema_clock_or_title_or_access_not_verified'),
    ('schema_title', 'schema_clock_or_title_or_access_not_verified'),
    ('schema_paid', 'schema_clock_or_title_or_access_not_verified'),
])
def test_first_failed_invariant_has_bounded_text_free_reason(cnyes_config, tmp_path, case, reason):
    source = PolicyRegistry(cnyes_config, base=tmp_path).sources['cnyes']
    changes = {'payment': {'payment': 1}, 'boolean_payment': {'payment': False},
        'epoch': {'publishAt': 'private-value'}, 'title': {'title': None},
        'data_url': {'url': URL + '0'}, 'data': {'newsId': 43}}
    schema = {'schema_clock': {'datePublished': '2014-01-02T00:00:00.000Z'},
        'schema_naive_clock': {'datePublished': CLOCK.removesuffix('Z')},
        'schema_title': {'headline': 'private-schema-title'},
        'schema_paid': {'isAccessibleForFree': False},
        'schema_missing': {'@type': 'ItemList'}}
    raw = short_page(change=changes.get(case), schema_change=schema.get(case),
                     canonical=URL + '0' if case == 'canonical' else URL)
    replacements = {'layout': (b'id="article-container"', b'id="other"'),
        'visible_title': (TITLE.encode(), b'private-visible-title'),
        'json': (b'{"@graph":', b'{"@graph":private-bad-json,')}
    if case in replacements:
        old, new = replacements[case]
        raw = raw.replace(old, new, 1 if case == 'json' else -1)
    # Change only the actual visible title, not its current declaration.
    if case == 'visible_title':
        raw = short_page().replace(('<h1>' + TITLE + '</h1>').encode(), b'<h1>private-visible-title</h1>')
    if case == 'excluded_title':
        source = replace(source, excluded_article_title_pattern='測試')
    requested = {'host': URL.replace('news.cnyes.com', 'publisher.test'),
        'id': URL.replace('/42', '/not-an-id'), 'fragment': URL + '#private-fragment',
        'query': URL + '?token=private-token'}.get(case, URL)
    response = Response(URL + '0' if case == 'final' else requested,
        403 if case == 'status' else 200,
        {'content-type': 'application/json' if case == 'mime' else 'text/html'}, raw)
    observation = {}
    assert diagnostics._public_cnyes_scope(context(response, source, requested), source, observation) is False
    assert observation == {'contract_version': 1, 'raw_scope_proved': False,
                           'first_failed_check': reason, 'proof_basis': None}
    serialized = json.dumps(observation)
    assert len(serialized) < 300 and 'private-' not in serialized and TITLE not in serialized


@pytest.mark.parametrize('requested,raw,basis', [
    (URL, short_page(), 'canonical_current_public_scope_proved'),
    (MOBILE, mobile_page(), 'native_current_public_scope_proved'),
    (URL + '?exp=a', streamed_mobile_page(), 'native_current_public_scope_proved'),
])
def test_successful_scope_only_proves_private_raw_retention(cnyes_config, tmp_path, requested, raw, basis):
    source = PolicyRegistry(cnyes_config, base=tmp_path).sources['cnyes']
    observation = {}
    response = Response(requested, 200, {'content-type': 'text/html'}, raw)
    assert diagnostics._public_cnyes_scope(context(response, source, requested), source) is True
    assert diagnostics._public_cnyes_scope(context(response, source, requested), source, observation) is True
    assert observation == {'contract_version': 1, 'raw_scope_proved': True,
                           'first_failed_check': None, 'proof_basis': basis}


def test_native_invalid_identity_preserves_original_exception(cnyes_config, tmp_path):
    source = PolicyRegistry(cnyes_config, base=tmp_path).sources['cnyes']
    requested = URL + '?exp=a'
    response = Response(requested, 200, {'content-type': 'text/html'}, mobile_page(changes={'newsId': 43}))
    observation = {}
    with pytest.raises(NewsError, match='article_identity_not_verified'):
        diagnostics._public_cnyes_scope(context(response, source, requested), source, observation)
    assert observation == {}


def test_native_absent_identity_keeps_false_and_private_reason(cnyes_config, tmp_path, monkeypatch):
    monkeypatch.setattr(publishers, '_cnyes_native_article_identity', lambda *_a, **_k: None)
    source = PolicyRegistry(cnyes_config, base=tmp_path).sources['cnyes']
    requested = URL + '?exp=a'
    response = Response(requested, 200, {'content-type': 'text/html'}, mobile_page())
    observation = {}
    assert diagnostics._public_cnyes_scope(context(response, source, requested), source, observation) is False
    assert observation['first_failed_check'] == 'native_identity_not_verified'


@pytest.mark.parametrize('paid', [False, True])
def test_owner_receipt_and_arrow_pair_record_reason_without_changed_failure_or_body(cnyes_config, tmp_path, paid):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    raw = Response(URL, 200, {'content-type': 'text/html', 'set-cookie': 'private-cookie'},
                   short_page(change={'payment': 1} if paid else None))
    with closing(NewsQueue(tmp_path / 'store', registry)) as queue:
        fail(queue, raw)
        paths, rows = reports(queue.root)
        assert len(rows) == 1
        receipt = json.loads(paths[0].with_suffix('.receipt.json').read_bytes())
        assert receipt['contract_version'] == 6 and receipt['budget_contract_version'] == 3
        assert receipt['sha256'] == sha256_file(paths[0])
        assert rows[0]['raw_scope_observation'] == receipt['raw_scope_observation']
        assert receipt['raw_scope_observation']['raw_scope_proved'] is not paid
        assert (rows[0]['raw_response_body'] is None) is paid
        assert not receipt['accepted_as_article'] and not receipt['publish'] and not receipt['remote_training_export']
        assert 'private-cookie' not in json.dumps(receipt)
        row = queue.db.execute('SELECT state,error_code,body_sha256 FROM articles WHERE url=?', (URL,)).fetchone()
        assert tuple(row) == ('retry', 'body_not_found', None)
        assert queue.db.execute('SELECT count(*) FROM accepted_batches').fetchone()[0] == 0


def test_budget_exhaustion_returns_before_new_observation_or_scope_parse(cnyes_config, tmp_path, monkeypatch):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    response = Response(URL, 200, {'content-type': 'text/html'}, short_page())
    with closing(NewsQueue(tmp_path, registry)) as queue:
        for _ in range(3):
            fail(queue, response)
        old = [(path, sha256_file(path)) for path in queue.root.rglob('*.receipt.json')]
        monkeypatch.setattr(diagnostics, '_observe_raw_scope', lambda *_a: pytest.fail('spent budget observed scope'))
        fail(queue, response)
        assert len(reports(tmp_path)[0]) == 3
        assert all(sha256_file(path) == digest for path, digest in old)


def test_focus_duplicate_does_not_observe_again_or_change_finite_budget(cnyes_config, tmp_path, monkeypatch):
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    response = Response(URL, 200, {'content-type': 'text/html'}, short_page())
    with closing(NewsQueue(tmp_path, registry)) as queue:
        configure_focus(queue, spec())
        fail(queue, response)
        before = queue.failure_diagnostic_focus.progress()
        monkeypatch.setattr(diagnostics, '_observe_raw_scope', lambda *_a: pytest.fail('duplicate observed scope'))
        fail(queue, response)
        assert queue.failure_diagnostic_focus.progress() == before
        assert len(reports(tmp_path)[0]) == 1


def test_caught_scope_exception_records_only_enum_and_remains_metadata(cnyes_config, tmp_path, monkeypatch):
    def broken(*_a, **_k):
        raise ValueError('private-provider-text-and-key')
    monkeypatch.setattr(publishers, '_cnyes_current_article_data', broken)
    registry = PolicyRegistry(cnyes_config, base=tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        fail(queue, Response(URL, 200, {'content-type': 'text/html'}, short_page()))
        row = reports(tmp_path)[1][0]
        assert row['raw_scope_observation']['first_failed_check'] == 'scope_check_exception'
        assert row['raw_response_body'] is None and not row['raw_response_retained']
        assert 'private-' not in json.dumps(row['raw_scope_observation'])


def test_other_provider_has_no_cnyes_observation_and_retains_same_licensed_scope(tmp_path):
    registry = globalvoices_registry(tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        response = Response(GV_URL, 200, {'content-type': 'text/html'}, gv_page(''))
        fail(queue, response)
        paths, rows = reports(tmp_path)
        assert len(rows) == 1 and rows[0]['raw_response_body'] == response.body
        assert 'raw_scope_observation' not in rows[0]
        assert 'raw_scope_observation' not in json.loads(paths[0].with_suffix('.receipt.json').read_bytes())


def test_unknown_reason_cannot_copy_arbitrary_text_into_observation():
    with pytest.raises(ValueError, match='unknown_cnyes_raw_scope_reason'):
        diagnostics._observe_raw_scope({}, False, 'private-arbitrary-source-text')
