"""Offline correction preserves source evidence and HTTP scheduling semantics."""
from contextlib import closing
import hashlib
import json
import time
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file, table_from_records
from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_http import Response
from downloader.news_fulltext_metadata_replay import replay_retained_cnyes_metadata
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from test_cnyes_news_bulk import cfg
from test_news_cnyes_canonical_alias import AUTHOR, MOBILE, mobile_page
from test_news_cnyes_html import CLOCK


CAPTURED = '2026-10-01T00:00:00+00:00'


def old_head(queue, *, url=MOBILE, payload=None, patch=None):
    queue.add_articles([{'url': url}])
    head = dict(queue.db.execute('SELECT * FROM articles').fetchone())
    raw = payload if payload is not None else mobile_page()
    http = SimpleNamespace(registry=queue.registry,
                           get=lambda *_a, **_k: Response(url, 200, {'content-type': 'text/html'}, raw))
    record = fetch_article(http, head)
    record.update(captured_at_utc=CAPTURED, data_fingerprint='old-capture', published_at_raw=None,
                  authors_json='[]', body_completeness='article_container_unverified',
                  extraction_method='css:#article-container')
    record['body'] += '\u200c\u200c'  # The old extraction accidentally included a loader.
    record['body_sha256'] = hashlib.sha256(record['body'].encode()).hexdigest()
    record['body_bytes'] = len(record['body'].encode())
    record.update(patch or {})
    queue.accept([record])
    return dict(queue.db.execute('SELECT * FROM articles').fetchone())


def read_head(queue):
    head = queue.db.execute('SELECT * FROM articles').fetchone()
    return head, pq.read_table(queue.root / head['parquet_path']).to_pylist()[0]


def test_repair_uses_no_http_and_preserves_old_receipts_clocks_and_attempts(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old = old_head(queue)
        with queue.db:
            queue.db.execute('UPDATE articles SET attempts=3,due=?,last_checked_at_utc=?', (12345, '2026-10-02T00:00:00+00:00'))
        old = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        original_digest = sha256_file(tmp_path / old['parquet_path'])
        attempts = queue.db.execute("SELECT value FROM counters WHERE key='article_attempts_finished'").fetchone()[0]
        result = replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)
        assert result['repaired_versions'] == 1 and result['http_requests'] == 0
        head, record = read_head(queue)
        assert record['published_at_raw'] == CLOCK and json.loads(record['authors_json']) == [AUTHOR]
        assert head['body_sha256'] != old['body_sha256']
        assert record['acquisition_transport'] == 'retained_raw_reparse'
        assert record['source_observed_at_utc'] == record['captured_at_utc'] == CAPTURED
        assert record['original_capture_data_fingerprint'] == 'old-capture'
        assert record['reparsed_at_utc'] > CAPTURED
        for key in ('last_checked_at_utc', 'captured_at_utc', 'attempts', 'due', 'etag', 'last_modified', 'error_code'):
            assert head[key] == old[key]
        assert sha256_file(tmp_path / old['parquet_path']) == original_digest
        assert queue.db.execute("SELECT value FROM counters WHERE key='article_attempts_finished'").fetchone()[0] == attempts
        assert verify_bodies(tmp_path)['accepted']
        assert replay_retained_cnyes_metadata(queue)['heads_checked'] == 0
        metrics = queue.metrics.snapshot()['providers']['cnyes']['counts']
        assert metrics['retained_metadata_repaired_versions'] == 1
        assert metrics['durably_accepted_cached_versions'] == 1
        assert metrics.get('durably_accepted_cached_new_url_heads', 0) == 0
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        assert replay_retained_cnyes_metadata(queue)['repaired_versions'] == 0
        assert verify_bodies(tmp_path)['body_versions_verified'] == 2


@pytest.mark.parametrize('state', ['inflight', 'retry', 'retry_cooldown', 'paywall_or_login', 'source_not_found', 'awaiting_authorization'])
def test_no_repair_over_active_lease_refusal_or_retry(cfg, tmp_path, state):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old = old_head(queue)
        with queue.db:
            queue.db.execute('UPDATE articles SET state=?,due=?,error_code=?', (state, time.time() + 10000, 'original_outcome'))
        assert replay_retained_cnyes_metadata(queue)['repaired_versions'] == 0
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone())['state'] == state
        assert queue.db.execute('SELECT parquet_path FROM articles').fetchone()[0] == old['parquet_path']
        if state in {'retry', 'inflight', 'retry_cooldown'}:
            with queue.db:
                queue.db.execute("UPDATE articles SET state='complete',lease_owner=NULL,lease_until=NULL")
            assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 1


@pytest.mark.parametrize('patch,code', [
    ({'authorization_fingerprint': 'changed'}, 'retained_metadata_authorization_changed'),
    ({'authorization_json': '{}'}, 'retained_metadata_authorization_changed'),
    ({'published_at_raw': '2010-01-01T00:00:00Z'}, 'retained_metadata_publication_conflict'),
    ({'authors_json': '["conflicting source byline"]'}, 'retained_metadata_author_conflict'),
    ({'response_sha256': 'incorrect'}, 'retained_metadata_source_digest_mismatch'),
    ({'captured_at_utc': '2099-01-01T00:00:00+00:00'}, 'retained_metadata_observation_clock_invalid'),
    ({'captured_at_utc': '2026-01-01'}, 'retained_metadata_observation_clock_invalid'),
    ({'publish': True}, 'retained_metadata_unapproved_source_contract'),
    ({'remote_training_export': True}, 'retained_metadata_unapproved_source_contract'),
    ({'training_injection_enabled': True}, 'retained_metadata_unapproved_source_contract'),
    ({'commercial_use_enabled': True}, 'retained_metadata_unapproved_source_contract'),
    ({'fair_use_legally_determined': True}, 'retained_metadata_unapproved_source_contract'),
])
def test_unverified_or_conflicting_evidence_is_visible_never_silently_replaced(cfg, tmp_path, patch, code):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old = old_head(queue, patch=patch)
        result = replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)
        assert result['repaired_versions'] == 0
        state = queue.db.execute('SELECT state,error_code FROM retained_metadata_replays').fetchone()
        assert tuple(state) == ('failed_validation', code)
        assert queue.db.execute('SELECT parquet_path FROM articles').fetchone()[0] == old['parquet_path']


def test_receipt_corruption_does_not_change_source_head_or_abort_other_cohort(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old = old_head(queue)
        receipt = queue.db.execute('SELECT receipt_path FROM accepted_batches').fetchone()[0]
        data = json.loads((tmp_path / receipt).read_text())
        data['sha256'] = 'wrong'
        atomic_write_json(tmp_path / receipt, data)
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 0
        assert queue.db.execute('SELECT error_code FROM retained_metadata_replays').fetchone()[0] == 'retained_metadata_batch_receipt_invalid'
        assert queue.db.execute('SELECT parquet_path FROM articles').fetchone()[0] == old['parquet_path']


def test_acceptance_failure_is_not_checkpointed_and_restart_recovers_once(cfg, tmp_path, monkeypatch):
    registry = PolicyRegistry(cfg, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    old_head(queue)
    monkeypatch.setattr(queue, '_accept_records', lambda *_a: (_ for _ in ()).throw(OSError('commit interrupted')))
    try:
        with pytest.raises(OSError, match='commit interrupted'):
            replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)
        assert queue.db.execute('SELECT state FROM retained_metadata_replays').fetchone()[0] == 'pending'
    finally:
        queue.close()
    with closing(NewsQueue(tmp_path, registry)) as restarted:
        assert read_head(restarted)[1]['published_at_raw'] == CLOCK
        assert replay_retained_cnyes_metadata(restarted)['repaired_versions'] == 0
        assert verify_bodies(tmp_path)['body_versions_verified'] == 2


def test_durable_replay_receipt_cannot_overwrite_a_new_refusal_after_crash(cfg, tmp_path, monkeypatch):
    registry = PolicyRegistry(cfg, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    old = old_head(queue)
    monkeypatch.setattr(queue, '_accept_records', lambda *_a: (_ for _ in ()).throw(OSError('commit interrupted')))
    try:
        with pytest.raises(OSError):
            replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)
        with queue.db:
            queue.db.execute("UPDATE articles SET state='paywall_or_login',error_code='original_refusal'")
    finally:
        queue.close()
    with closing(NewsQueue(tmp_path, registry)) as restarted:
        head = restarted.db.execute('SELECT state,error_code,parquet_path FROM articles').fetchone()
        assert tuple(head) == ('paywall_or_login', 'original_refusal', old['parquet_path'])
        assert replay_retained_cnyes_metadata(restarted)['repaired_versions'] == 0


def test_changed_head_before_acceptance_is_rejected(cfg, tmp_path, monkeypatch):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old = old_head(queue)
        accept = queue.accept
        def changed(records):
            with queue.db:
                queue.db.execute("UPDATE articles SET state='inflight',lease_owner='actual_http_owner',lease_until=?", (time.time() + 300,))
            return accept(records)
        monkeypatch.setattr(queue, 'accept', changed)
        with pytest.raises(ValueError, match='head_changed_before_acceptance'):
            replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)
        assert queue.db.execute('SELECT parquet_path FROM articles').fetchone()[0] == old['parquet_path']
        assert len(list((tmp_path / 'receipts').glob('*.json'))) == 1


def test_stop_and_small_materialization_bound_do_not_relabel_missing_metadata(cfg, tmp_path):
    import threading
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old_head(queue)
        stop = threading.Event()
        stop.set()
        assert replay_retained_cnyes_metadata(queue, stop=stop)['repaired_versions'] == 0
        assert queue.db.execute('SELECT state FROM retained_metadata_replays').fetchone()[0] == 'pending'
        assert replay_retained_cnyes_metadata(queue, maximum_bytes=1, maximum_wall_seconds=1)['repaired_versions'] == 0
        assert queue.db.execute('SELECT error_code FROM retained_metadata_replays').fetchone()[0] == 'retained_metadata_batch_exceeds_resource_bound'


@pytest.mark.parametrize('bound', [0, 65, True])
def test_unbounded_work_is_rejected(cfg, tmp_path, bound):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        with pytest.raises(ValueError, match='unbounded'):
            replay_retained_cnyes_metadata(queue, maximum_heads=bound)


@pytest.mark.parametrize('title', ['...', '…'])
def test_observed_loader_headline_is_repaired_without_discarding_original(cfg, tmp_path, title):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old_head(queue, patch={'title': title})
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 1
        record = read_head(queue)[1]
        assert record['title'] != title
        assert record['original_capture_title'] == title
        assert 'old_loader_title_preserved' in record['replay_title_basis']


@pytest.mark.parametrize('method', ['css_article', 'css:.other-container', 'api_fulltext'])
def test_loader_exception_does_not_apply_to_unobserved_extractors(cfg, tmp_path, method):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old_head(queue, patch={'title': '...', 'extraction_method': method})
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 0
        assert queue.db.execute('SELECT error_code FROM retained_metadata_replays').fetchone()[0] == 'retained_metadata_title_conflict'


def test_selector_migration_is_once_only_and_preserves_head_and_original_error(cfg, tmp_path):
    from downloader.news_fulltext_metadata_replay import _ensure_cohort
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old_head(queue, patch={'title': '...'})
        source = queue.registry.sources['cnyes']
        _ensure_cohort(queue, source)
        original = dict(queue.db.execute('SELECT * FROM articles').fetchone())
        marker = 'retained_mobile_loader_selector_v1:' + source.data_fingerprint
        with queue.db:
            queue.db.execute('DELETE FROM queue_settings WHERE key=?', (marker,))
            queue.db.execute("UPDATE retained_metadata_replays SET state='failed_validation',error_code='retained_metadata_title_conflict'")
        _ensure_cohort(queue, source)
        assert tuple(queue.db.execute('SELECT state,error_code FROM retained_metadata_replays').fetchone()) == ('pending', 'retained_metadata_title_conflict')
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == original
        with queue.db:
            queue.db.execute("UPDATE retained_metadata_replays SET state='failed_validation'")
        _ensure_cohort(queue, source)
        assert queue.db.execute('SELECT state FROM retained_metadata_replays').fetchone()[0] == 'failed_validation'


def test_actual_conflicting_headline_is_not_waived(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old_head(queue, patch={'title': 'another real source headline'})
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 0
        assert queue.db.execute('SELECT error_code FROM retained_metadata_replays').fetchone()[0] == 'retained_metadata_title_conflict'


def test_pinned_older_newsroom_review_keeps_historical_restrictions_and_same_scope(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        current = queue.registry.sources['cnyes'].authorization
        prior = {k: current[k] for k in ('kind', 'scope', 'evidence_url', 'reviewed_on', 'automated_access_review', 'rights_restrictions')}
        prior['reviewed_on'] = '2026-10-08'
        prior['rights_restrictions'] = prior['rights_restrictions'].split(' Hao is')[0]
        digest = hashlib.sha256(json.dumps(prior, sort_keys=True).encode()).hexdigest()
        old_head(queue, patch={'authorization_json': json.dumps(prior, sort_keys=True), 'authorization_fingerprint': digest})
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 1
        head, record = read_head(queue)
        assert head['authorization_fingerprint'] == record['authorization_fingerprint'] == digest
        assert json.loads(record['authorization_json']) == prior
        assert record['replay_policy_authorization_fingerprint'] == queue.registry.sources['cnyes'].authorization_fingerprint
        assert record['training_injection_enabled'] is False and record['remote_training_export'] is False


def test_api_representation_with_legitimately_missing_byline_is_not_an_html_error(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        old_head(queue, patch={'final_url': 'https://api.cnyes.com/media/api/v1/newslist/category/headline'})
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 0
        assert queue.db.execute('SELECT state,error_code FROM retained_metadata_replays').fetchone()[0] == 'not_mobile_html_candidate'


def test_caption_repair_preserves_source_response_capture_and_separate_role(cfg, tmp_path):
    from test_news_cnyes_captions import CAPTION, IMAGE, caption_page
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        original = old_head(queue, payload=caption_page(), patch={'verified_image_captions_json': None})
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1,
                                             target_article_ids=[original['id']])['repaired_versions'] == 1
        head, record = read_head(queue)
        assert json.loads(record['verified_image_captions_json']) == [{'text': CAPTION, 'image_src': IMAGE}]
        assert CAPTION not in record['body']
        assert head['captured_at_utc'] == record['captured_at_utc'] == original['captured_at_utc']
        assert record['acquisition_transport'] == 'retained_raw_reparse'
        assert verify_bodies(tmp_path)['accepted']


def test_focused_source_repair_leaves_unrequested_old_heads_pending(cfg, tmp_path):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        original = old_head(queue)
        result = replay_retained_cnyes_metadata(queue, target_article_ids=[b'0' * 32], maximum_wall_seconds=1)
        assert result['heads_checked'] == result['repaired_versions'] == result['http_requests'] == 0
        assert dict(queue.db.execute('SELECT * FROM articles').fetchone()) == original
        assert queue.db.execute('SELECT state FROM retained_metadata_replays').fetchone()[0] == 'pending'
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 1


def test_retained_observed_query_view_preserves_actual_requested_and_final_uri(cfg, tmp_path):
    url = MOBILE + '?exp=a'
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        original = old_head(queue, url=url)
        assert replay_retained_cnyes_metadata(queue, maximum_wall_seconds=1)['repaired_versions'] == 1
        head, record = read_head(queue)
        assert head['url'] == record['requested_url'] == record['final_url'] == url
        assert head['captured_at_utc'] == original['captured_at_utc']
        assert 'observed_exp_a_request_variant' in record['article_identity_basis']
        assert verify_bodies(tmp_path)['accepted']


@pytest.mark.parametrize('targets', [[], [b'wrong'], ['0' * 64], [b'0' * 32, b'0' * 32], [bytes([i]) * 32 for i in range(33)]])
def test_focused_repair_cannot_hide_an_unbounded_or_ambiguous_cohort(cfg, tmp_path, targets):
    with closing(NewsQueue(tmp_path, PolicyRegistry(cfg, base=tmp_path))) as queue:
        with pytest.raises(ValueError, match='invalid bounded'):
            replay_retained_cnyes_metadata(queue, target_article_ids=targets)
