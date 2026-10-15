"""Anue metadata seed invalidation uses its own ABI; no source HTTP."""
from dataclasses import replace
import time

import pytest

from downloader.news_fulltext import discovery_fingerprint, seed_discovery
from downloader.news_fulltext_cnyes_index import ENDPOINT
from test_news_cnyes_public_index import DAY, accept, cfg, index_payload, queue, request_url


def ready(queue):
    accept(queue, index_payload(ids=(1,), total=1))
    source = queue.registry.sources['cnyes']
    # Avoid unrelated historic category seeding in this isolated lifecycle
    # fixture. Keep the real independent index/body fingerprints and policy.
    source = replace(source, cnyes_apis=(), feeds=(), sitemaps=(), index_pages=())
    queue.registry.sources['cnyes'] = source
    url = request_url(source, DAY)
    return source, url, dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())


def test_same_metadata_fingerprint_does_not_reset_verified_pages(queue):
    source, url, before = ready(queue)
    assert source.cnyes_index_data_fingerprint != discovery_fingerprint(source)
    assert before['due'] > time.time() and before['parser_fingerprint'] == source.cnyes_index_data_fingerprint
    for _ in range(3):
        seed_discovery(queue)
    after = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    assert after == before  # Due, original hashes and source clocks remain intact.
    assert queue.db.execute('SELECT count(*) FROM cnyes_public_index_observations').fetchone()[0] == 1


def test_changed_metadata_fingerprint_reparses_once_without_relabelling_original(queue):
    source, url, before = ready(queue)
    queue.registry.sources['cnyes'] = replace(source, cnyes_index_data_fingerprint='f' * 64)
    seed_discovery(queue)
    after = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    assert after == {**before, 'due': 0}
    assert queue.db.execute('SELECT index_fingerprint FROM cnyes_public_index_observations').fetchone()[0] == before['parser_fingerprint']


@pytest.mark.parametrize('error', ['robots_disallowed', 'discovery_http_429', 'discovery_http_403', 'transport_error'])
def test_changed_index_contract_preserves_source_refusal_and_retry_deadlines(queue, error):
    source, url, before = ready(queue)
    queue.db.execute('UPDATE discovery SET error_code=?,attempts=4 WHERE url=?', (error, url))
    queue.registry.sources['cnyes'] = replace(source, cnyes_index_data_fingerprint='f' * 64)
    seed_discovery(queue)
    after = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    assert after == {**before, 'error_code': error, 'attempts': 4}


def test_disabled_index_preserves_prior_deadline_instead_of_resetting_to_generic_body_ABI(queue):
    source, url, before = ready(queue)
    queue.registry.sources['cnyes'] = replace(source, cnyes_index_api='', cnyes_index_data_fingerprint='')
    seed_discovery(queue)
    after = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
    assert after == before


@pytest.mark.parametrize('stale', [False, True])
def test_normal_html_indexes_keep_their_generic_contract_invalidation(queue, stale):
    source, url, before = ready(queue)
    html_url = 'https://news.cnyes.com/news/cat/headline'
    fingerprint = '0' * 64 if stale else discovery_fingerprint(source)
    queue.db.execute("INSERT INTO discovery(url,provider,kind,due,parser_fingerprint) VALUES (?,'cnyes','index',?,?)",
                     (html_url, before['due'], fingerprint))
    seed_discovery(queue)
    html = queue.db.execute('SELECT due,parser_fingerprint FROM discovery WHERE url=?', (html_url,)).fetchone()
    assert html['due'] == (0 if stale else before['due']) and html['parser_fingerprint'] == fingerprint
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before


def test_similar_endpoint_does_not_inherit_the_independent_metadata_ABI(queue):
    source, url, before = ready(queue)
    other_url = ENDPOINT + '-lookalike?limit=30'
    queue.db.execute("INSERT INTO discovery(url,provider,kind,due,parser_fingerprint) VALUES (?,'cnyes','index',?,?)",
                     (other_url, before['due'], source.cnyes_index_data_fingerprint))
    seed_discovery(queue)
    assert queue.db.execute('SELECT due FROM discovery WHERE url=?', (other_url,)).fetchone()[0] == 0
    assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone()) == before
