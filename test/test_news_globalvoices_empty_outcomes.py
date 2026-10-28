"""A verified empty native response is absent prose, never a completed body."""
from contextlib import closing
from dataclasses import replace
import json
from pathlib import Path

from bs4 import BeautifulSoup
import pytest

from downloader import news_fulltext_policy as policy
from downloader.artifact_io import sha256_file
from downloader.news_fulltext import discovery_fingerprint, load_config
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue, parser_repair_changed, parser_repair_identity
from test_news_globalvoices_photo_outcomes import document, source, URL
from test_news_globalvoices_private_originals import RAW, settings
from test_news_retained_nontext_outcomes import retained, submit, consume, head, receipt


@pytest.mark.parametrize('parser', ['lxml', 'html.parser'])
def test_empty_bound_native_entry_has_an_explicit_nonbody_outcome(parser):
    with pytest.raises(NewsError, match='^source_article_empty_template$'):
        extract_article(document(''), replace(source(), html_parser=parser), page_url=URL)


@pytest.mark.parametrize('entry', [
    ' ', '<!-- unknown content -->', '<div></div>', '<div class="loading"></div>',
    '<script src="https://widget.test/loader.js"></script>',
    '<iframe src="https://unknown.test/embed"></iframe>', '<p hidden>Hidden source prose.</p>',
])
def test_unresolved_loading_hidden_and_unknown_shapes_are_not_called_empty(entry):
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(document(entry), source(), page_url=URL)


@pytest.mark.parametrize('change', [
    lambda b: b.replace(b'rel="canonical"', b'rel="other"'),
    lambda b: b.replace(URL.encode(), URL.replace('synthetic-roundup', 'different-post').encode()),
    lambda b: b.replace(b'id="post-header-credit"', b'id="other-credit"'),
    lambda b: b.replace(b'property="article:published_time"', b'property="article:modified_time"'),
    lambda b: b.replace(b'rel="license"', b'rel="other"'),
    lambda b: b.replace(b'<div class="entry">', b'<div class="entry" hidden>'),
    lambda b: b.replace(b'<div class="entry"></div>', b'<div class="entry"></div><div class="entry"></div>'),
])
def test_absence_never_relaxes_identity_credit_clock_license_or_visibility(change):
    with pytest.raises(NewsError) as caught:
        extract_article(change(document('')), source(), page_url=URL)
    assert caught.value.code != 'source_article_empty_template'


def test_previous_contract_keeps_its_original_failure(monkeypatch):
    monkeypatch.setattr(policy, 'GLOBALVOICES_EMPTY_OUTCOME_CONTRACT_VERSION', 0)
    with pytest.raises(NewsError, match='^body_not_found$'):
        extract_article(document(''), source(), page_url=URL)


def test_only_capture_identity_changes_without_replaying_good_data_or_resetting_pacing(monkeypatch, tmp_path):
    config, _, repo = load_config(Path('configs/news_fulltext.json'))
    settings = {**config, 'providers': [p for p in config['providers'] if p['id'] == 'globalvoices']}
    settings.pop('provider_catalog', None)
    with monkeypatch.context() as old:
        old.setattr(policy, 'GLOBALVOICES_EMPTY_OUTCOME_CONTRACT_VERSION', 0)
        previous = PolicyRegistry(settings, base=repo)
    current = PolicyRegistry(settings, base=repo)
    a, b = previous.sources['globalvoices'], current.sources['globalvoices']
    assert a.data_fingerprint != b.data_fingerprint
    assert b.parser_repair_prior_capture_fingerprint == a.data_fingerprint
    assert a.data_fingerprint in b.parser_repair_capture_history
    assert a.parser_repair_fingerprint == b.parser_repair_fingerprint
    assert not parser_repair_changed(b, a.data_fingerprint, json.dumps(parser_repair_identity(a)))
    assert a.authorization_fingerprint == b.authorization_fingerprint
    assert a.http_capacity_source_fingerprint == b.http_capacity_source_fingerprint
    assert a.fulltext_feed_data_fingerprint == b.fulltext_feed_data_fingerprint
    assert discovery_fingerprint(a) == discovery_fingerprint(b)
    with closing(NewsQueue(tmp_path, previous)) as queue:
        queue.add_articles([{'url': URL}])
        with queue.db:
            queue.db.execute("UPDATE articles SET state='body_not_found',error_code='body_not_found',"
                "attempts=1,due=0,last_checked_at_utc='2026-01-01T00:00:00+00:00' WHERE url=?", (URL,))
        before = dict(queue.db.execute('SELECT * FROM articles').fetchone())
    with closing(NewsQueue(tmp_path, current)) as queue:
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == before


@pytest.mark.parametrize('kind', ['native_candidate_original', 'failure_diagnostic_original'])
@pytest.mark.parametrize('previous_photo', [0, 1])
def test_owner_classifies_retained_empty_response_without_http_or_clock_rewrite(settings, tmp_path, monkeypatch,
                                                                              kind, previous_photo):
    soup = BeautifulSoup(RAW, 'lxml')
    soup.select_one('.entry').clear()
    with retained(settings, tmp_path, monkeypatch, kind, raw=str(soup).encode(),
                  previous_photo=previous_photo) as (queue, ref, before):
        hashes = {k: sha256_file(queue.root / ref[k]) for k in ('path', 'receipt_path')}
        result = submit(queue, ref)
        assert consume(queue)['outcomes'] == {'classified_source_article_empty_template': 1}
        assert head(queue) == dict(before, state='source_article_empty_template', error_code='source_article_empty_template')
        proof = receipt(queue, result)
        assert proof['reclassified_targets'] == 1 and proof['admitted_targets'] == 0
        assert proof['publisher_requests'] == proof['body_acceptances'] == 0
        assert not proof['check_and_publication_clocks_changed'] and not proof['accepted_as_bodies']
        assert hashes == {k: sha256_file(queue.root / ref[k]) for k in hashes}
        assert not (queue.root / 'bodies').exists()
        assert consume(queue)['processed'] == 0
