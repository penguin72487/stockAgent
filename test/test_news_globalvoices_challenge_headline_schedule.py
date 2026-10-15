"""A parser rollout never clears a source refusal or advances its clock."""
from contextlib import closing
from pathlib import Path

from downloader.news_fulltext import load_config
from downloader.news_fulltext_queue import NewsQueue


def test_version9_preserves_exact_access_retry_refusals_saved_heads_and_deadlines(tmp_path, monkeypatch):
    import downloader.news_fulltext_policy as policy

    url = 'https://globalvoices.org/2010/08/23/st-vincent-the-grenadines-haiti-access-denied/'
    cases = {'access': ('retry', 'access_challenge'), 'forbidden': ('http_403', 'http_403'),
        'limited': ('http_429', 'http_429'), 'member': ('paywall_or_login', 'paywall_or_login'),
        'saved': ('complete', None), 'leased': ('inflight', None)}
    monkeypatch.setattr(policy, '_GLOBALVOICES_HTML_BODY_CONTRACT_VERSION', 8)
    previous = load_config(Path('configs/news_fulltext.json'))[1]
    with closing(NewsQueue(tmp_path, previous)) as queue:
        with queue.db:
            queue.add_articles([{'url': url + '?case=' + key} for key in cases])
            for key, (state, error) in cases.items():
                queue.db.execute('UPDATE articles SET state=?,error_code=?,due=?,attempts=2,'
                    'last_checked_at_utc=?,body_sha256=?,lease_owner=?,lease_until=? WHERE url=?',
                    (state, error, 12345678900, '2026-10-13T11:14:19+00:00',
                     'a' * 64 if key == 'saved' else None, 'original-owner' if key == 'leased' else None,
                     12345678900 if key == 'leased' else None, url + '?case=' + key))
        before = {key: dict(queue.db.execute('SELECT * FROM articles WHERE url=?',
            (url + '?case=' + key,)).fetchone()) for key in cases}
    monkeypatch.setattr(policy, '_GLOBALVOICES_HTML_BODY_CONTRACT_VERSION', 9)
    current = load_config(Path('configs/news_fulltext.json'))[1]
    with closing(NewsQueue(tmp_path, current)) as queue:
        for key in cases:
            actual = dict(queue.db.execute('SELECT * FROM articles WHERE url=?',
                (url + '?case=' + key,)).fetchone())
            expected = dict(before[key])
            if key == 'leased':
                # A fresh exclusive owner must recover abandoned work. That
                # independent startup contract is not a parser repair or a
                # license to advance a real source-refusal deadline.
                expected.update(state='retry', error_code='owner_restart', due=0.0,
                                lease_owner=None, lease_until=None)
            assert actual == expected
