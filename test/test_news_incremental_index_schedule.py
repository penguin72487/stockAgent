"""Declared latest indexes refresh promptly without re-polling old archives or refusals."""
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
import json
import time

import pytest

from downloader.news_fulltext import accept_discovery, discovery_fingerprint, seed_discovery
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from test_news_fulltext import config


LATEST = 'https://publisher.test/'
ARCHIVE = 'https://publisher.test/archive'


def declare(config):
    entry = config['providers'][0]
    entry.update(index_pages=[LATEST, ARCHIVE], incremental_index_pages=[LATEST],
                 discovery_interval_seconds=3600)
    return config


@pytest.mark.parametrize('status', [200, 304])
def test_declared_latest_index_success_honours_hourly_period_but_archive_stays_daily(config, tmp_path, status):
    registry = PolicyRegistry(declare(config), base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        seed_discovery(queue)
        for url, delay in ((LATEST, 3600), (ARCHIVE, 86400)):
            task = dict(queue.db.execute('SELECT * FROM discovery WHERE url=?', (url,)).fetchone())
            started = time.time()
            accept_discovery(queue, task, Response(url, status, {'etag': 'original-validator'},
                b'<html>Public index</html>' if status == 200 else b''), [], [], [])
            row = queue.db.execute('SELECT due,etag,error_code FROM discovery WHERE url=?', (url,)).fetchone()
            assert started+delay <= row['due'] <= time.time()+delay
            assert row['etag'] == 'original-validator' and row['error_code'] is None
    finally:
        queue.close()


def test_explicit_latest_index_scope_is_not_a_new_body_or_discovery_parser_contract(config, tmp_path):
    original = deepcopy(config)
    original['providers'][0].update(index_pages=[LATEST, ARCHIVE], discovery_interval_seconds=3600)
    old = PolicyRegistry(original, base=tmp_path).sources['publisher']
    new = PolicyRegistry(declare(config), base=tmp_path).sources['publisher']
    from downloader.news_fulltext_schedule import schedule_fingerprint
    assert new.incremental_index_pages == (LATEST,)
    assert old.data_fingerprint == new.data_fingerprint
    assert old.authorization_fingerprint == new.authorization_fingerprint
    assert discovery_fingerprint(old) == discovery_fingerprint(new)
    assert schedule_fingerprint(old) != schedule_fingerprint(new)
    assert schedule_fingerprint(new) != schedule_fingerprint(replace(new, discovery_interval_seconds=1800))


@pytest.mark.parametrize('scope', [None, LATEST, [LATEST, LATEST], ['https://foreign.test/'],
    ['https://publisher.test/unregistered'], [LATEST+'#fragment'], [LATEST+'?secret=not-a-key'], [None]])
def test_latest_index_scope_must_be_bounded_exact_registered_public_urls(config, tmp_path, scope):
    declare(config)['providers'][0]['incremental_index_pages'] = scope
    with pytest.raises(ValueError, match='incremental index'):
        PolicyRegistry(config, base=tmp_path)


@pytest.mark.parametrize('setting,value', [('acquisition_mode','finite'),
    ('discovery_interval_seconds',True), ('discovery_interval_seconds',30),
    ('discovery_interval_seconds','3600')])
def test_incremental_scope_requires_continuous_mode_and_exact_valid_interval(config, tmp_path, setting, value):
    declare(config)['providers'][0][setting] = value
    with pytest.raises(ValueError, match='incremental index'):
        PolicyRegistry(config, base=tmp_path)


def test_known_legacy_success_due_is_corrected_from_original_clock_once_with_receipt(config, tmp_path):
    registry = PolicyRegistry(declare(config), base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    source = registry.sources['publisher']
    try:
        # Simulate the old successful homepage plus unrelated protected/history work.
        checked = datetime.fromtimestamp(time.time()-7200, timezone.utc).isoformat()
        old_due = datetime.fromisoformat(checked).timestamp()+86400
        with queue.db:
            queue.db.executemany('INSERT INTO discovery(url,kind,provider,due,last_checked_at_utc,'
                'parser_fingerprint,etag,last_modified,response_sha256,attempts,error_code) '
                "VALUES (?,'index','publisher',?,? ,?,'old-etag','old-modified',?,0,?)",
                [(u, old_due, checked, discovery_fingerprint(source), 'a'*64, error)
                 for u,error in ((LATEST,None),(ARCHIVE,None),('https://publisher.test/denied','source_access_denied'))])
        before = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM discovery')}
        seed_discovery(queue)
        after = {r['url']: dict(r) for r in queue.db.execute('SELECT * FROM discovery')}
        expected_due = datetime.fromisoformat(checked).timestamp()+3600
        assert after[LATEST] == {**before[LATEST], 'due': expected_due}
        assert after[ARCHIVE] == before[ARCHIVE]
        assert after['https://publisher.test/denied'] == before['https://publisher.test/denied']
        from downloader.news_fulltext_schedule import receipt_key
        receipt = json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
                                              (receipt_key(source),)).fetchone()[0])
        assert receipt['corrected'] == [{'url':LATEST,'original_due':old_due,'corrected_due':expected_due,
                                         'original_last_checked_at_utc':checked}]
        assert receipt['publisher_requests'] == 0
        assert not receipt['source_observation_or_validators_changed']
        total = queue.db.total_changes
        seed_discovery(queue)
        # Other seed statements can run; the migration receipt/source fields cannot change.
        assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?',(LATEST,)).fetchone()) == after[LATEST]
        assert json.loads(queue.db.execute('SELECT value FROM queue_settings WHERE key=?',
                                          (receipt_key(source),)).fetchone()[0]) == receipt
        assert queue.db.total_changes >= total
    finally:
        queue.close()


@pytest.mark.parametrize('mode', ['refused','rate_limited','retry','attempt_unknown','unseen',
    'wrong_parser','naive_clock','broken_clock','future_clock','already_due','not_legacy_daily','disabled','finite'])
def test_schedule_correction_never_resets_refusals_unknown_work_or_undeclared_due(config, tmp_path, mode):
    registry = PolicyRegistry(declare(config), base=tmp_path)
    source = registry.sources['publisher']
    if mode in {'disabled','finite'}:
        source = replace(source, allowed=False) if mode == 'disabled' else replace(source, acquisition_mode='finite')
    queue = NewsQueue(tmp_path, registry)
    try:
        checked = datetime.fromtimestamp(time.time()-7200,timezone.utc).isoformat()
        due = datetime.fromisoformat(checked).timestamp()+86400
        error, attempts, parser = None,0,discovery_fingerprint(source)
        if mode in {'refused','rate_limited','retry'}:
            error = {'refused':'source_access_denied','rate_limited':'source_rate_limited','retry':'transport_error'}[mode]
        elif mode == 'attempt_unknown': attempts=2
        elif mode == 'unseen': checked=None
        elif mode == 'wrong_parser': parser='incompatible-old-parser'
        elif mode == 'naive_clock': checked='2026-01-01T00:00:00'
        elif mode == 'broken_clock': checked='not-a-source-clock'
        elif mode == 'future_clock':
            checked=datetime.fromtimestamp(time.time()+600,timezone.utc).isoformat()
            due=datetime.fromisoformat(checked).timestamp()+86400
        elif mode == 'already_due': due=0
        elif mode == 'not_legacy_daily': due+=3600
        with queue.db:
            queue.db.execute('INSERT INTO discovery(url,kind,provider,due,last_checked_at_utc,parser_fingerprint,'
                'attempts,error_code,etag) VALUES (?,\'index\',\'publisher\',?,?,?,?,?,\'original\')',
                (LATEST,due,checked,parser,attempts,error))
        before=dict(queue.db.execute('SELECT * FROM discovery WHERE url=?',(LATEST,)).fetchone())
        from downloader.news_fulltext_schedule import reconcile_incremental_indexes
        with queue.db:
            reconcile_incremental_indexes(queue,source,discovery_fingerprint(source))
        assert dict(queue.db.execute('SELECT * FROM discovery WHERE url=?',(LATEST,)).fetchone()) == before
    finally:
        queue.close()


def test_index_schedule_marker_and_due_update_roll_back_together(config, tmp_path):
    registry=PolicyRegistry(declare(config),base=tmp_path)
    source=registry.sources['publisher']
    queue=NewsQueue(tmp_path,registry)
    try:
        checked=datetime.fromtimestamp(time.time()-7200,timezone.utc).isoformat()
        due=datetime.fromisoformat(checked).timestamp()+86400
        with queue.db:
            queue.db.execute('INSERT INTO discovery(url,kind,provider,due,last_checked_at_utc,parser_fingerprint) '
                "VALUES (?,'index','publisher',?,?,?)",(LATEST,due,checked,discovery_fingerprint(source)))
        from downloader.news_fulltext_schedule import receipt_key,reconcile_incremental_indexes
        with pytest.raises(RuntimeError,match='simulated interruption'):
            with queue.db:
                reconcile_incremental_indexes(queue,source,discovery_fingerprint(source))
                raise RuntimeError('simulated interruption')
        assert queue.db.execute('SELECT due FROM discovery WHERE url=?',(LATEST,)).fetchone()[0]==due
        assert not queue.db.execute('SELECT 1 FROM queue_settings WHERE key=?',(receipt_key(source),)).fetchone()
    finally:
        queue.close()
