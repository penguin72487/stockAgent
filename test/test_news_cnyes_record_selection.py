"""CNYES list selection preserves individual article heads and aliases."""
from contextlib import closing
import time

import pytest

from downloader.news_fulltext import _select_discovery_records, accept_discovery
from downloader.news_fulltext_bulk import cnyes_records, cnyes_url
from downloader.news_fulltext_queue import NewsQueue, utc_now, verify_bodies
from test_cnyes_news_bulk import BASE, cfg, payload, registry_http, response  # noqa: F401


def test_cnyes_page_selects_each_eligible_alias_and_head_without_repeated_lookups(cfg, tmp_path):
    registry, http = registry_http(cfg, tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        task = {'kind': 'cnyes_news', '_discovery_request_started_at_utc': utc_now()}
        records, errors = cnyes_records(response(payload(ids=(1, 2, 3, 4, 5, 6))), http, registry.sources['cnyes'])
        assert not errors
        aliases = [
            'https://news.cnyes.com/news/print/1', 'https://m.cnyes.com/news/print/1',
            'https://news.cnyes.com/news/print/2', 'https://m.cnyes.com/news/print/2',
            'https://news.cnyes.com/news/print/3', 'https://m.cnyes.com/news/print/3',
            'https://m.cnyes.com/news/print/4',
        ]
        with queue.db:
            queue.add_articles([{'url': r['requested_url']} for r in records[:5]] + [{'url': url} for url in aliases])
            queue.db.execute("UPDATE articles SET state='retry',due=0 WHERE url=?", (aliases[1],))
            queue.db.execute("UPDATE articles SET state='unsupported_article_path',due=0 WHERE url=?", (aliases[2],))
            queue.db.execute("UPDATE articles SET state='inflight',lease_owner='other',lease_until=? WHERE url=?",
                             (time.time() + 60, aliases[3]))
            queue.db.execute("UPDATE articles SET body_sha256=? WHERE url=?", ('a' * 64, aliases[4]))
            queue.db.execute("UPDATE articles SET state='retry',due=? WHERE url=?", (time.time() + 3600, aliases[5]))
            queue.db.execute("UPDATE articles SET state='article_attribution_not_verified',due=0 WHERE url=?", (aliases[6],))
            queue.db.execute("UPDATE articles SET state='inflight',lease_owner='other',lease_until=? WHERE url=?",
                             (time.time() + 60, records[1]['requested_url']))
            queue.db.execute("UPDATE articles SET state='retry',due=? WHERE url=?",
                             (time.time() + 3600, records[2]['requested_url']))
            queue.db.execute("UPDATE articles SET state='complete',body_sha256=? WHERE url=?",
                             ('b' * 64, records[3]['requested_url']))
            queue.db.execute("UPDATE articles SET state='retry',due=0,body_sha256=? WHERE url=?",
                             (records[4]['body_sha256'], records[4]['requested_url']))
        before = queue.db.total_changes
        statements = []
        queue.db.set_trace_callback(statements.append)
        accepted, unchanged = _select_discovery_records(queue, task, records, defer_unchanged=True)
        queue.db.set_trace_callback(None)
        assert [r['requested_url'] for r in accepted] == [aliases[0], aliases[1], records[0]['requested_url'], aliases[2], aliases[6]]
        assert [row['url'] for row, _ in unchanged] == [records[4]['requested_url']]
        assert all(r['unleased_article_head_witness_json'] for r in accepted)
        assert queue.db.total_changes == before
        lookups = [sql for sql in statements if sql.startswith('SELECT * FROM articles WHERE')]
        assert len(lookups) <= len(records)


@pytest.mark.parametrize('host', ['news.cnyes.com', 'm.cnyes.com'])
@pytest.mark.parametrize('newer_state', ['cooldown', 'lease'])
def test_cnyes_combined_lookup_fences_each_alias_if_another_worker_advances_it(
        cfg, tmp_path, monkeypatch, host, newer_state):
    registry, http = registry_http(cfg, tmp_path)
    with closing(NewsQueue(tmp_path, registry)) as queue:
        url = cnyes_url(BASE, start=1388505600, end=1391212799)
        alias = f'https://{host}/news/print/1'
        deadline = time.time() + 3600
        with queue.db:
            queue.add_articles([{'url': alias}])
            queue.db.execute("INSERT INTO discovery(url,kind,provider) VALUES (?,'cnyes_news','cnyes')", (url,))
        task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
        raw = response(payload(ids=(1,)), url)
        records, errors = cnyes_records(raw, http, registry.sources['cnyes'])
        task['_discovery_request_started_at_utc'] = records[0]['captured_at_utc']
        links = [(item['requested_url'], item['title'], item['metadata_date_raw']) for item in records]
        original = queue._accept_records

        def advance_alias(rows, relative, receipt, **kwargs):
            with queue.db:
                if newer_state == 'cooldown':
                    queue.db.execute("UPDATE articles SET state='retry_cooldown',due=?,error_code='http_429' WHERE url=?",
                                     (deadline, alias))
                else:
                    queue.db.execute("UPDATE articles SET state='inflight',lease_owner='other',lease_until=? WHERE url=?",
                                     (deadline, alias))
            return original(rows, relative, receipt, **kwargs)

        monkeypatch.setattr(queue, '_accept_records', advance_alias)
        assert accept_discovery(queue, task, raw, links, records, errors) == 2
        head = queue.db.execute('SELECT state,due,error_code,body_sha256,lease_owner,lease_until FROM articles WHERE url=?',
                                (alias,)).fetchone()
        if newer_state == 'cooldown':
            assert (head['state'], head['due'], head['error_code'], head['body_sha256']) == (
                'retry_cooldown', deadline, 'http_429', None)
        else:
            assert (head['state'], head['lease_owner'], head['lease_until'], head['body_sha256']) == (
                'inflight', 'other', deadline, None)
        assert queue.db.execute("SELECT state FROM articles WHERE url=?", (records[0]['requested_url'],)).fetchone()[0] == 'complete'
        assert verify_bodies(tmp_path)['accepted']
