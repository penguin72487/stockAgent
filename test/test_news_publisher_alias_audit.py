"""Alias references reuse originals without changing capture/body semantics."""
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import hashlib
import json

import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.news_fulltext import article_response_record, fetch_article
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import NewsError, PolicyRegistry, extract_article
from downloader.news_fulltext_queue import NewsQueue
from scripts.audit_news_publisher_aliases import audit_pocket_aliases, main
from test_news_pocket_original import settings, page, URL, OLD  # noqa: F401 -- shared synthetic fixture
from test_news_pocket_linked_figures import current


ALIASES = (OLD.rstrip('/'), 'https://www.pocket.tw/invest_news/article/12345')


def fixture(tmp_path, settings, aliases=ALIASES):
    config = current(settings)
    config['failure_diagnostic_metadata_identity_upgrade'] = False
    config['retained_cnyes_failure_replay_enabled'] = False
    config.pop('failure_diagnostic_focus', None)
    queue = NewsQueue(tmp_path / 'news', PolicyRegistry(config, base=tmp_path))
    source = queue.registry.sources['pocket']
    now = datetime.now(UTC)
    clock = lambda seconds: (now - timedelta(seconds=seconds)).isoformat()
    queue.add_articles([{'url': u} for u in (*aliases, URL)])
    original = page().encode()
    article = extract_article(original, source, page_url=URL)
    target = dict(queue.db.execute('SELECT * FROM articles WHERE url=?', (URL,)).fetchone())
    record = article_response_record(source, target, Response(URL, 200, {}, original), article,
        captured_at_utc=clock(30), http_duration_seconds=0.1, parse_duration_seconds=0.1, duration_seconds=0.2)
    queue.accept([record])
    with queue.db:
        queue.db.executemany("UPDATE articles SET state='unsupported_article_path',error_code='unsupported_article_path',"
            'attempts=2,due=0,last_checked_at_utc=? WHERE url=?', [(clock(40), u) for u in aliases])
    observations = []
    for alias in aliases:
        location = '/school/articles/12345/'
        hop = {'from_url': alias, 'to_url': URL, 'location': location, 'status': 308,
               'observed_at_utc': clock(20), 'response_bytes': len(location),
               'response_sha256': hashlib.sha256(location.encode()).hexdigest()}
        for key in ('from_url', 'to_url', 'location'):
            hop[key + '_raw_sha256'] = hashlib.sha256(hop[key].encode()).hexdigest()
        # A different raw document can have the same native body: observation
        # metadata is not another retained raw response or another body capture.
        response_sha = hashlib.sha256(original + b'<!-- another rendering -->').hexdigest()
        observations.append({'url': alias, 'question_completed_at_utc': clock(18),
            'result': {'url': alias, 'provider': 'pocket', 'status': 200, 'final_url': URL,
                'article_durably_accepted': False, 'source_probe_contract_version': 2,
                'actual_redirects': [hop], 'bytes': len(original), 'response_sha256': response_sha,
                'extracted': {k: article[k] for k in ('title', 'authors_json', 'published_at_raw', 'body_sha256')}},
            'trace': [{'requested_url': alias, 'status': 308, 'body_read': True, 'observed_at_utc': clock(20),
                'response_bytes': len(location), 'response_sha256': hop['response_sha256'], 'headers': {'location': location}},
                {'requested_url': URL, 'status': 200, 'body_read': True, 'observed_at_utc': clock(19),
                 'response_bytes': len(original), 'response_sha256': response_sha}]})
    report = {'accepted': True, 'accepted_as_article': False, 'raw_body_retention': False, 'article_head_writes': 0,
        'source_identities': {'pocket': [source.data_fingerprint, source.authorization_fingerprint,
            source.http_capacity_source_fingerprint, source.http_capacity_authorization_fingerprint]},
        'observations': observations}
    path = tmp_path / 'completed-observations.json'
    atomic_write_json(path, report, durable=True)
    return queue, path, report


def audit(queue, path, aliases=ALIASES):
    return audit_pocket_aliases(queue.root, queue.registry, path, sha256_file(path), list(aliases))


def heads(queue):
    return [dict(r) for r in queue.db.execute('SELECT * FROM articles ORDER BY id')]


def test_completed_observations_and_body_batch_reused_without_an_http_or_queue_owner(tmp_path, settings, monkeypatch):
    queue, path, report = fixture(tmp_path, settings)
    try:
        before = heads(queue)
        monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__',
            lambda *a, **kw: pytest.fail('reference audit must not construct an HTTP client'))
        monkeypatch.setattr('downloader.news_fulltext_queue.NewsQueue.__init__',
            lambda *a, **kw: pytest.fail('reference audit must not construct a writer'))
        result = audit(queue, path)
        assert result['accepted'] and result['source_requests'] == result['queue_writes'] == result['new_article_bodies'] == 0
        assert not result['formal_queue_alias_admission'] and not result['raw_alias_response_retained']
        assert result['known_urls'] == 3 and result['stored_body_urls'] == 1
        assert len(result['verified_alias_references']) == 2 and len(result['physical_parts']) == 1
        assert result['physical_parts'][0]['physical_parquet_decodes'] == 1
        for reference in result['verified_alias_references']:
            assert reference['existing_final_url'] == URL and not reference['raw_responses_identical']
            assert reference['native_body_replayed_both_engines'] and not reference['alias_body_admitted']
            assert reference['existing_body_captured_at_utc'] != reference['redirect_observed_at_utc']
        assert heads(queue) == before
    finally:
        queue.close()


def test_actual_opaque_redirect_reference_does_not_relax_opaque_body_transport(tmp_path, settings):
    opaque = 'https://www.pocket.tw/school/articles/article-abc12345/'
    queue, path, _ = fixture(tmp_path, settings, (opaque,))
    try:
        assert len(audit(queue, path, (opaque,))['verified_alias_references']) == 1
        from types import SimpleNamespace
        http = SimpleNamespace(registry=queue.registry, metrics=None,
            get=lambda *a, **kw: Response(URL, 200, {'content-type': 'text/html'}, page().encode()))
        with pytest.raises(NewsError, match='article_identity_not_verified'):
            fetch_article(http, {'url': opaque, 'id': b'a' * 32})
    finally:
        queue.close()


@pytest.mark.parametrize('change', ['report_sha', 'accepted_body', 'retained_raw_claim', 'source_auth',
    'http_identity', 'duplicate_observation', 'missing_observation', 'request_raw_hash', 'location_raw_hash',
    'to_raw_hash', 'broken_chain', 'location', 'cross_origin', 'final_status', 'hop_status', 'hop_hash',
    'trace_hash', 'trace_location', 'redacted_query', 'naive_clock', 'future_clock', 'backward_clock',
    'old_http_clock', 'final_bytes', 'absent_body_identity', 'different_body', 'same_body_other_canonical',
    'title', 'author', 'release', 'parser_backend_parity', 'expired_scope', 'target_auth', 'target_raw',
    'parquet_corruption', 'receipt_rows', 'receipt_path', 'unaccepted_batch'])
def test_missing_or_conflicting_evidence_never_resolves_a_head(tmp_path, settings, monkeypatch, change):
    queue, path, report = fixture(tmp_path, settings)
    try:
        item = report['observations'][0]
        hop, trace, result = item['result']['actual_redirects'][0], item['trace'][0], item['result']
        supplied = None
        if change == 'report_sha': supplied = '0' * 64
        elif change == 'accepted_body': report['accepted_as_article'] = True
        elif change == 'retained_raw_claim': report['raw_body_retention'] = True
        elif change == 'source_auth': report['source_identities']['pocket'][1] = '0' * 64
        elif change == 'http_identity': report['source_identities']['pocket'][2] = '0' * 64
        elif change == 'duplicate_observation': report['observations'].append(deepcopy(item))
        elif change == 'missing_observation': report['observations'].pop(0)
        elif change.endswith('_raw_hash'):
            hop[{'request_raw_hash': 'from_url', 'location_raw_hash': 'location', 'to_raw_hash': 'to_url'}[change] + '_raw_sha256'] = '0' * 64
        elif change == 'broken_chain': hop['from_url'] = URL
        elif change == 'location': hop['location'] = '/school/articles/99999/'
        elif change == 'cross_origin': hop['to_url'] = 'https://other.example.org/school/articles/12345/'
        elif change == 'final_status': result['status'] = 403
        elif change == 'hop_status': hop['status'] = 429
        elif change == 'hop_hash': hop['response_sha256'] = ''
        elif change == 'trace_hash': trace['response_sha256'] = '0' * 64
        elif change == 'trace_location': trace['headers']['location'] = '/other/'
        elif change == 'redacted_query': hop['location_raw_sha256'] = hashlib.sha256((hop['location'] + '?hidden=1').encode()).hexdigest()
        elif change == 'naive_clock': hop['observed_at_utc'] = '2020-01-01T00:00:00'
        elif change == 'future_clock': hop['observed_at_utc'] = '2999-01-01T00:00:00+00:00'
        elif change == 'backward_clock': item['trace'][-1]['observed_at_utc'] = '2020-01-01T00:00:00+00:00'
        elif change == 'old_http_clock': trace['observed_at_utc'] = '2020-01-01T00:00:00+00:00'
        elif change == 'final_bytes': result['bytes'] += 1
        elif change == 'absent_body_identity': result['extracted'].pop('body_sha256')
        elif change == 'different_body': result['extracted']['body_sha256'] = '0' * 64
        elif change == 'same_body_other_canonical':
            result['final_url'] = URL.replace('12345', '54321')
            item['trace'][-1]['requested_url'] = result['final_url']
            hop['to_url'] = result['final_url']; hop['location'] = '/school/articles/54321/'
            for key in ('to_url', 'location'): hop[key + '_raw_sha256'] = hashlib.sha256(hop[key].encode()).hexdigest()
            trace['headers']['location'] = hop['location']
        elif change in {'title', 'author', 'release'}:
            result['extracted'][{'title': 'title', 'author': 'authors_json', 'release': 'published_at_raw'}[change]] = 'other'
        elif change == 'parser_backend_parity':
            import scripts.audit_news_publisher_aliases as module
            original_parse = module.extract_article
            def parse(raw, source, **kw):
                value = original_parse(raw, source, **kw)
                return {**value, 'title': 'other'} if source.html_parser == 'lxml' else value
            monkeypatch.setattr(module, 'extract_article', parse)
        elif change == 'expired_scope':
            queue.registry.sources['pocket'] = replace(queue.registry.sources['pocket'], allowed=False)
        else:
            row = queue.db.execute('SELECT parquet_path FROM articles WHERE url=?', (URL,)).fetchone()
            accepted = queue.db.execute('SELECT receipt_path FROM accepted_batches WHERE path=?', (row[0],)).fetchone()
            receipt_path = queue.root / accepted[0]
            receipt = json.loads(receipt_path.read_bytes())
            if change == 'parquet_corruption':
                monkeypatch.setattr('scripts.audit_news_publisher_aliases.sha256_file', lambda p: '0' * 64)
            elif change in {'receipt_rows', 'receipt_path'}:
                receipt['rows' if change == 'receipt_rows' else 'parquet_path'] = 999 if change == 'receipt_rows' else '../outside.parquet'
                atomic_write_json(receipt_path, receipt, durable=True)
            elif change == 'unaccepted_batch':
                with queue.db: queue.db.execute('DELETE FROM accepted_batches')
            else:
                # Corrupt the decoded row only: the physical receipt remains a
                # valid witness, so the independent row check must reject it.
                import scripts.audit_news_publisher_aliases as module
                original_file = module.pq.ParquetFile
                class Reader:
                    def __init__(self, p):
                        self.reader = original_file(p); self.metadata = self.reader.metadata
                    def iter_batches(self, **kw):
                        import pyarrow as pa
                        for batch in self.reader.iter_batches(**kw):
                            rows = batch.to_pylist()
                            rows[0]['authorization_fingerprint' if change == 'target_auth' else 'raw_response_body'] = 'bad' if change == 'target_auth' else b'bad'
                            yield pa.RecordBatch.from_pylist(rows)
                monkeypatch.setattr(module.pq, 'ParquetFile', Reader)
        atomic_write_json(path, report, durable=True)
        before = heads(queue)
        with pytest.raises(ValueError):
            audit_pocket_aliases(queue.root, queue.registry, path, supplied or sha256_file(path), list(ALIASES))
        assert heads(queue) == before
    finally:
        queue.close()


@pytest.mark.parametrize('change', ['refusal', 'saved', 'lease_owner', 'lease_until', 'future_due', 'newer_observation'])
def test_changed_missing_frontier_is_not_hidden_by_a_known_old_body(tmp_path, settings, change):
    queue, path, _ = fixture(tmp_path, settings)
    try:
        with queue.db:
            if change == 'refusal': queue.db.execute("UPDATE articles SET state='http_403' WHERE url=?", (ALIASES[0],))
            elif change == 'saved': queue.db.execute('UPDATE articles SET body_sha256=? WHERE url=?', ('a'*64, ALIASES[0]))
            elif change in {'lease_owner', 'lease_until'}:
                queue.db.execute('UPDATE articles SET ' + change + '=? WHERE url=?', ('other' if change == 'lease_owner' else 1e12, ALIASES[0]))
            elif change == 'future_due': queue.db.execute('UPDATE articles SET due=1e12 WHERE url=?', (ALIASES[0],))
            else: queue.db.execute('UPDATE articles SET last_checked_at_utc=? WHERE url=?', (datetime.now(UTC).isoformat(), ALIASES[0]))
        before = heads(queue)
        with pytest.raises(ValueError): audit(queue, path)
        assert heads(queue) == before
    finally:
        queue.close()


def test_cli_will_not_overwrite_completed_proof_or_even_load_a_configuration(tmp_path, monkeypatch):
    output = tmp_path / 'completed.json'
    output.write_text('original proof')
    monkeypatch.setattr('scripts.audit_news_publisher_aliases.load_config', lambda *a: pytest.fail('completed audit must be reused'))
    with pytest.raises(ValueError, match='reuse completed audit'):
        main(['--observations', str(tmp_path/'unused.json'), '--observations-sha256', '0'*64,
              '--url', ALIASES[0], '--output', str(output)])
    assert output.read_text() == 'original proof'
