from __future__ import annotations

import pytest

from downloader.news_fulltext import fetch_article
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue
from scripts.verify_news_recent_batches import verify_recent_batches
from scripts.audit_news_runtime import (baseline_progress_requirements, has_dispatchable_missing_body_work,
    active_request_scope_pauses, source_readiness_snapshot, heartbeat_clock, completion_owner_check, frontier_snapshot_counts)
from scripts.audit_news_selected_articles import audit as audit_selected
from scripts.benchmark_cnyes_acquisition import verify_batches
from types import SimpleNamespace
from test_news_fulltext import FakeResponse, article_html, config as config, http_fixture
from test_cnyes_news_bulk import cfg as cnyes_config


def test_recent_verification_is_read_only_and_can_run_with_active_writer(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
        "https://publisher.test/a": FakeResponse(article_html()),
    })
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.add_articles([{"url": "https://publisher.test/a"}])
        queue.accept([fetch_article(http, queue.claim("publisher", 300))])
        before = tuple(queue.db.execute("SELECT key,value FROM queue_settings ORDER BY key"))
        proof = verify_recent_batches(tmp_path, 1)
        assert proof["accepted"] and proof["body_versions_verified"] == 1
        assert proof["batches_verified"] == 1
        assert proof["owner_id"] == queue.owner
        assert "not the whole lake" in proof["verification_scope"]
        assert proof["historical_completeness_verified"] is False
        assert tuple(queue.db.execute("SELECT key,value FROM queue_settings ORDER BY key")) == before
        (tmp_path / proof["batch_paths"][0]).write_bytes(b"corrupt fixture")
        rejected = verify_recent_batches(tmp_path, 1)
        assert not rejected["accepted"]
        assert "receipt_mismatch" in rejected["errors"][0]["error"]
    finally:
        queue.close()
        http.close()


def test_empty_frontier_is_not_verified(config, tmp_path):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        proof = verify_recent_batches(tmp_path)
        assert not proof["accepted"]
        assert proof["errors"] == [{"error": "no_committed_batches_to_verify"}]
    finally:
        queue.close()


@pytest.mark.parametrize('fixed_old_cohort', [False, True])
def test_recent_write_versions_are_not_all_current_heads_but_every_hash_is_verified(config, tmp_path, fixed_old_cohort):
    url = 'https://publisher.test/a'
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        url: FakeResponse(article_html())})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        queue.add_articles([{'url': url}])
        original = fetch_article(http, queue.claim('publisher', 300))
        queue.accept([original])
        first_path = queue.db.execute('SELECT path FROM accepted_batches').fetchone()[0]
        queue.accept([dict(original)])  # A legitimate later version with the same body SHA.
        settings = tuple(queue.db.execute('SELECT key,value FROM queue_settings ORDER BY key'))
        proof = verify_recent_batches(tmp_path, 2, batch_paths=[first_path] if fixed_old_cohort else None)
        assert proof['verification_contract_version'] == 2
        assert proof['accepted'] and proof['body_versions_verified'] == (1 if fixed_old_cohort else 2)
        assert proof['superseded_body_versions_verified'] == 1
        assert proof['current_heads_verified'] == (0 if fixed_old_cohort else 1)
        assert proof['current_heads_outside_cohort'] == (1 if fixed_old_cohort else 0)
        assert proof['distinct_article_ids_checked'] == 1
        assert tuple(queue.db.execute('SELECT key,value FROM queue_settings ORDER BY key')) == settings
        # Benchmarks keep their stricter all-rows-current contract. The repair
        # is an explicit bounded recent-version audit, not a blanket bypass.
        with pytest.raises(ValueError, match='benchmark_queue_head_mismatch'):
            verify_batches(SimpleNamespace(root=tmp_path, db=queue.db), [first_path])
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('damage', ['body', 'raw', 'current_sha', 'missing_head', 'uncommitted_path', 'unsafe_path'])
def test_recent_version_mode_never_hides_real_damage_or_dangling_heads(config, tmp_path, damage):
    url = 'https://publisher.test/a'
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        url: FakeResponse(article_html())})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        queue.add_articles([{'url': url}])
        original = fetch_article(http, queue.claim('publisher', 300))
        damaged = dict(original)
        if damage == 'body':
            damaged['body'] = 'incorrect historical body; old SHA deliberately retained'
        elif damage == 'raw':
            damaged['raw_response_body'] = b'incorrect historical raw response; old SHA retained'
        queue.accept([damaged])
        first_path = queue.db.execute('SELECT path FROM accepted_batches').fetchone()[0]
        queue.accept([original])
        with queue.db:
            if damage == 'current_sha':
                queue.db.execute("UPDATE articles SET body_sha256=?", ('0' * 64,))
            elif damage == 'missing_head':
                queue.db.execute('DELETE FROM articles')  # This isolated fixture only.
            elif damage == 'uncommitted_path':
                queue.db.execute("UPDATE articles SET parquet_path='bodies/never-committed.parquet'")
            elif damage == 'unsafe_path':
                queue.db.execute("UPDATE articles SET parquet_path='../outside.parquet'")
        proof = verify_recent_batches(tmp_path, 2, batch_paths=[first_path] if damage in {'uncommitted_path', 'unsafe_path'} else None)
        assert not proof['accepted'] and proof['errors']
        if damage in {'body', 'raw'}:
            assert 'raw_or_body_hash_mismatch' in proof['errors'][0]['error']
        if damage == 'current_sha':
            assert 'recent_queue_head_mismatch' in proof['errors'][0]['error']
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('paths', [[], ['a', 'a'], [None], 'not-a-list', ['a', 'b', 'c']])
def test_explicit_recent_snapshot_requires_bounded_distinct_paths(config, tmp_path, paths):
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        with pytest.raises(ValueError, match='bounded_distinct_snapshot_batch_paths_required'):
            verify_recent_batches(tmp_path, 2, batch_paths=paths)
    finally:
        queue.close()


@pytest.mark.parametrize("limit", [0, -1, 1025])
def test_recent_verification_requires_bounded_work(tmp_path, limit):
    with pytest.raises(ValueError, match="maximum_batches"):
        verify_recent_batches(tmp_path, limit)


def test_progress_requires_ready_sources_not_idle_or_preexisting_source_refusals():
    previous = {"observed_at_utc": "2026-10-09T01:00:00+00:00", "providers": {
        "ready": {"enabled": True, "missing_body_work": 100, "has_dispatchable_missing_body_work": True},
        "expired_hold": {"enabled": True, "missing_body_work": 1, "has_dispatchable_missing_body_work": True,
                         "next_attempt_at_utc": "2026-10-09T00:59:00+00:00"},
        "later_article_retry": {"enabled": True, "missing_body_work": 1, "has_dispatchable_missing_body_work": False,
                                "next_attempt_at_utc": "2026-10-09T00:59:00+00:00"},
        "legacy_readiness_unknown": {"enabled": True, "missing_body_work": 1},
        "idle_with_terminal_errors": {"enabled": True, "missing_body_work": 0, "states": {"body_not_found": 225}},
        "refused": {"enabled": True, "missing_body_work": 100, "next_attempt_at_utc": "2026-10-09T01:01:00+00:00"},
        "unreviewed": {"enabled": False, "missing_body_work": 0}}}
    result = baseline_progress_requirements(previous)
    assert result["required_provider_ids"] == ["ready", "expired_hold"]
    assert "not_complete" in result["excluded_provider_reasons"]["refused"]
    assert "not_complete" in result["excluded_provider_reasons"]["idle_with_terminal_errors"]
    assert "not_complete" in result["excluded_provider_reasons"]["later_article_retry"]
    assert "readiness_unknown" in result["excluded_provider_reasons"]["legacy_readiness_unknown"]
    assert "not all-provider acceptance" in result["basis"]


@pytest.mark.parametrize('state,due,body,lease_owner,lease_until,expected', [
    ('pending', 0, None, None, None, True), ('pending', 200, None, None, None, False),
    ('retry', 100, None, None, None, True), ('retry_cooldown', 101, None, None, None, False),
    ('pending', 0, 'already-stored', None, None, False), ('body_not_found', 0, None, None, None, False),
    ('waiting_cohort', 0, None, None, None, False), ('inflight', 0, None, 'current', 200, True),
    ('inflight', 0, None, 'current', 100, False), ('inflight', 0, None, 'previous', 200, False),
])
def test_runtime_readiness_matches_article_deadline_and_actual_owner_without_writes(
        config, tmp_path, state, due, body, lease_owner, lease_until, expected):
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/a'}])
            queue.db.execute('UPDATE articles SET state=?,due=?,body_sha256=?,lease_owner=?,lease_until=?',
                             (state, due, body, lease_owner, lease_until))
        before = queue.db.total_changes
        assert has_dispatchable_missing_body_work(queue.db, registry.sources['publisher'], 'current', 100) is expected
        assert queue.db.total_changes == before
    finally:
        queue.close()


def test_runtime_readiness_excludes_uninitialized_finite_cohort(config, tmp_path):
    from dataclasses import replace
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        with queue.db:
            queue.add_articles([{'url': 'https://publisher.test/a'}])
        source = replace(registry.sources['publisher'], acquisition_mode='finite')
        assert not has_dispatchable_missing_body_work(queue.db, source, queue.owner, 100)
    finally:
        queue.close()


@pytest.mark.parametrize('host_pause,origin_pause,second_url,expected', [
    ({'publisher.test': 101}, {}, None, False),
    ({'publisher.test': 100}, {}, None, True),
    ({}, {'https://publisher.test': 101}, None, False),
    ({}, {'https://publisher.test': 100}, None, True),
    ({}, {'http://publisher.test': 101}, None, True),
    ({}, {'https://publisher.test': 101}, 'http://publisher.test/b', True),
    ({'publisher.test': 101}, {}, 'https://other.test/b', True),
])
def test_runtime_readiness_uses_actual_expiring_scope_filters_without_provider_wide_suppression(
        config, tmp_path, host_pause, origin_pause, second_url, expected):
    config['providers'][0]['domains'].append('other.test')
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        queue.add_articles([{'url': 'https://publisher.test/a'}] +
                           ([{'url': second_url}] if second_url else []))
        with queue.db:
            queue.db.execute("UPDATE articles SET due=0")
        before = tuple(tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url'))
        scopes = active_request_scope_pauses({'temporarily_paused_gateway_hosts': host_pause,
            'temporarily_paused_robots_origins': origin_pause}, 100)
        ready = has_dispatchable_missing_body_work(queue.db, registry.sources['publisher'], queue.owner, 100,
            excluded_hosts=tuple(scopes['active_gateway_hosts']), excluded_origins=tuple(scopes['active_robots_origins']))
        assert ready is expected
        assert tuple(tuple(row) for row in queue.db.execute('SELECT * FROM articles ORDER BY url')) == before
        baseline = {'observed_at_utc': '2026-10-15T00:00:00+00:00', 'providers': {'publisher': {
            'enabled': True, 'missing_body_work': len(before), 'has_dispatchable_missing_body_work': ready,
            'request_scope_readiness': {**scopes, 'has_unfiltered_due_work': True}}}}
        proof = baseline_progress_requirements(baseline)
        assert proof['required_provider_ids'] == (['publisher'] if expected else [])
        if not expected:
            assert proof['excluded_provider_reasons']['publisher'] == (
                'due_missing_bodies_wait_for_request_scope_policy; backlog_not_complete')
    finally:
        queue.close()


def test_active_current_owner_lease_remains_required_when_its_origin_is_later_paused(config, tmp_path):
    from dataclasses import replace
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        queue.add_articles([{'url': 'https://publisher.test/a'}])
        with queue.db:
            queue.db.execute("UPDATE articles SET state='inflight',lease_owner=?,lease_until=101", (queue.owner,))
        source = registry.sources['publisher']
        assert has_dispatchable_missing_body_work(queue.db, source, queue.owner, 100,
            excluded_origins=('https://publisher.test',))
        assert not has_dispatchable_missing_body_work(queue.db, source, 'old-owner', 100,
            excluded_origins=('https://publisher.test',))
        assert not has_dispatchable_missing_body_work(queue.db, replace(source, allowed=False), queue.owner, 100)
    finally:
        queue.close()


@pytest.mark.parametrize('pauses', [None, [], {'publisher.test': True}, {'publisher.test': float('nan')},
                                  {'publisher.test': float('inf')}, {'publisher.test': '101'}])
def test_runtime_scope_pause_rejects_malformed_telemetry_not_silently_green(pauses):
    with pytest.raises(ValueError, match='invalid_request_scope_pause_snapshot'):
        active_request_scope_pauses({'temporarily_paused_gateway_hosts': pauses}, 100)


def test_runtime_scope_pause_reuses_canonical_exact_origin_validation():
    with pytest.raises(ValueError, match='bounded_exact_excluded_origins_required'):
        active_request_scope_pauses({'temporarily_paused_robots_origins': {'https://publisher.test/path': 101}}, 100)


@pytest.mark.parametrize('kind', ['gateway', 'origin'])
@pytest.mark.parametrize('second_url,lease,expected', [
    (None, False, False), ('https://other.test/b', False, True),
    ('http://publisher.test/b', False, True), (None, True, True)])
def test_pause_expiry_between_metric_and_database_clocks_is_not_a_policy_success(
        config, tmp_path, kind, second_url, lease, expected):
    from datetime import datetime, timedelta, timezone
    config['providers'][0]['domains'].append('other.test')
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    metric = datetime(2026, 10, 18, 8, 30, 2, tzinfo=timezone.utc)
    observed, expired = metric + timedelta(seconds=10), (metric + timedelta(seconds=5)).timestamp()
    control = {'temporarily_paused_gateway_hosts': {'publisher.test': expired}} if kind == 'gateway' else {
        'temporarily_paused_robots_origins': {'https://publisher.test': expired}}
    try:
        queue.add_articles([{'url': 'https://publisher.test/a'}] + ([{'url': second_url}] if second_url else []))
        with queue.db:
            queue.db.execute('UPDATE articles SET due=0')
            if lease:
                queue.db.execute("UPDATE articles SET state='inflight',lease_owner=?,lease_until=?",
                    (queue.owner, (observed + timedelta(seconds=10)).timestamp()))
        before = queue.db.total_changes
        ready, details = source_readiness_snapshot(queue.db, registry.sources['publisher'], queue.owner, control,
            queue_observed_at_utc=observed.isoformat(), metric_snapshot_at_utc=metric.isoformat())
        # Gateway isolation excludes both schemes; an exact HTTPS origin does not.
        if kind == 'gateway' and second_url == 'http://publisher.test/b': expected = False
        assert ready is expected and details['has_unfiltered_due_work']
        assert details['pause_evaluated_at_utc'] == metric.isoformat()
        assert any(details['expired_after_metric_snapshot'].values())
        assert not details['permission_after_snapshot_inferred'] and queue.db.total_changes == before
        baseline = {'observed_at_utc': observed.isoformat(), 'providers': {'publisher': {
            'enabled': True, 'missing_body_work': 1, 'has_dispatchable_missing_body_work': ready,
            'request_scope_readiness': details}}}
        assert baseline_progress_requirements(baseline)['required_provider_ids'] == (['publisher'] if expected else [])
    finally:
        queue.close()


def test_a_later_snapshot_must_witness_gate_expiry_before_ready_work_is_required(config, tmp_path):
    from datetime import datetime, timedelta, timezone
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        queue.add_articles([{'url': 'https://publisher.test/a'}])
        clock = datetime(2026, 10, 18, 8, 30, 12, tzinfo=timezone.utc)
        control = {'temporarily_paused_robots_origins': {'https://publisher.test': (clock - timedelta(seconds=5)).timestamp()}}
        ready, details = source_readiness_snapshot(queue.db, registry.sources['publisher'], queue.owner, control,
            queue_observed_at_utc=clock.isoformat(), metric_snapshot_at_utc=clock.isoformat())
        assert ready and not any(details['active_robots_origins'])
        # A subsequent failure in this genuinely ready window stays required;
        # no end-of-window circuit or error is used to erase the requirement.
        baseline = {'observed_at_utc': clock.isoformat(), 'providers': {'publisher': {
            'enabled': True, 'missing_body_work': 1, 'has_dispatchable_missing_body_work': ready,
            'request_scope_readiness': details}}}
        assert baseline_progress_requirements(baseline)['required_provider_ids'] == ['publisher']
    finally:
        queue.close()


@pytest.mark.parametrize('queue_clock,metric_clock', [
    ('2026-10-18T08:30:12', '2026-10-18T08:30:02+00:00'),
    ('2026-10-18T08:30:12+00:00', '2026-10-18T08:30:02')])
def test_readiness_does_not_silently_mix_naive_clocks(queue_clock, metric_clock):
    with pytest.raises(ValueError, match='timezone_aware_readiness_clocks_required'):
        source_readiness_snapshot(None, None, None, {}, queue_observed_at_utc=queue_clock, metric_snapshot_at_utc=metric_clock)


@pytest.mark.parametrize('age,fresh', [(-6, False), (-5, True), (0, True), (45, True), (46, False), (120, False)])
def test_runtime_heartbeat_freshness_uses_its_read_clock_not_reconciliation_finish(age, fresh):
    from datetime import datetime, timedelta, timezone
    read_at = datetime(2026, 10, 11, 0, 0, tzinfo=timezone.utc)
    status = {'updated_at_utc': (read_at - timedelta(seconds=age)).isoformat()}
    proof = heartbeat_clock(status, read_at.isoformat())
    assert proof['age_seconds'] == age and proof['fresh'] is fresh


@pytest.mark.parametrize('stamp,read_at', [
    ('2026-10-11T00:00:00', '2026-10-11T00:00:00+00:00'),
    ('2026-10-11T00:00:00+00:00', '2026-10-11T00:00:00'),
])
def test_runtime_heartbeat_rejects_ambiguous_clocks(stamp, read_at):
    with pytest.raises(ValueError, match='timezone_aware_heartbeat_clock_required'):
        heartbeat_clock({'updated_at_utc': stamp}, read_at)


@pytest.mark.parametrize('failure', [None, 'stale', 'future', 'stopped', 'new_pid', 'no_pid',
                                    'new_owner', 'missing_owner', 'restarted', 'old_process_clock'])
def test_runtime_completion_read_rejects_real_stall_and_restart_after_slow_audit(monkeypatch, failure):
    service = {'MainPID': '123', 'ActiveState': 'active', 'NRestarts': '0'}
    status = {'owner_id': 'owner', 'updated_at_utc': '2026-10-11T00:00:00+00:00'}
    # The initial snapshot is intentionally 120 seconds older than completion;
    # it was fresh when read. Do not erase this real elapsed audit time.
    assert heartbeat_clock(status, '2026-10-11T00:00:00+00:00')['fresh']
    completed = {**status, 'updated_at_utc': '2026-10-11T00:02:00+00:00'}
    completed_service = dict(service)
    if failure == 'stale':
        completed['updated_at_utc'] = status['updated_at_utc']
    elif failure == 'future':
        completed['updated_at_utc'] = '2026-10-11T00:02:06+00:00'
    elif failure == 'stopped':
        completed_service['ActiveState'] = 'inactive'
    elif failure == 'new_pid':
        completed_service['MainPID'] = '456'
    elif failure == 'no_pid':
        completed_service['MainPID'] = '0'
    elif failure == 'new_owner':
        completed['owner_id'] = 'another-owner'
    elif failure == 'missing_owner':
        completed.pop('owner_id')
    elif failure == 'restarted':
        completed_service['NRestarts'] = '1'
    monkeypatch.setattr('scripts.audit_news_runtime.owner_process_clock_check',
                        lambda *_: {'accepted': failure != 'old_process_clock'})
    proof = completion_owner_check('owner', service, status, completed_service, completed,
                                   '2026-10-11T00:02:00+00:00')
    assert proof['accepted'] is (failure is None)


@pytest.mark.parametrize('cached', [True, False])
def test_runtime_frontier_counts_match_full_oracle_without_writes_and_preserve_rechecks(config, tmp_path, cached):
    registry = PolicyRegistry(config, base=tmp_path)
    queue = NewsQueue(tmp_path, registry)
    try:
        queue.add_articles([{'url': 'https://publisher.test/' + name} for name in ('a', 'b', 'c', 'd')])
        with queue.db:
            queue.db.execute("UPDATE articles SET state='complete',body_sha256=? WHERE url LIKE '%/a'", ('1' * 64,))
            queue.db.execute("UPDATE articles SET state='retry',body_sha256=? WHERE url LIKE '%/b'", ('2' * 64,))
            queue.db.execute("UPDATE articles SET state='paywall_or_login',body_sha256=? WHERE url LIKE '%/d'", ('3' * 64,))
            if not cached:
                queue.db.execute("DELETE FROM queue_settings WHERE key='stored_body_metrics_version'")

        def check():
            settings = dict(queue.db.execute('SELECT key,value FROM queue_settings'))
            before = queue.db.total_changes
            heads, states, rechecks, basis = frontier_snapshot_counts(queue.db, settings)
            oracle_heads = dict(queue.db.execute('SELECT provider,count(*) FROM articles WHERE body_sha256 IS NOT NULL GROUP BY provider'))
            oracle_checks = [dict(r) for r in queue.db.execute('SELECT provider,state,count(*) AS articles '
                "FROM articles WHERE body_sha256 IS NOT NULL AND state!='complete' GROUP BY provider,state")]
            assert heads == oracle_heads
            assert sorted(rechecks, key=lambda r: (r['provider'], r['state'])) == oracle_checks
            assert sum(r['articles'] for r in states) == 4
            assert ('transactional_' if cached else 'legacy_full_') in basis
            assert queue.db.total_changes == before
            return heads, rechecks

        heads, rechecks = check()
        assert heads['publisher'] == 3
        assert sum(r['articles'] for r in rechecks) == 2
        with queue.db:
            # A revision is not a new head. Provider and missing-body changes
            # still follow the canonical transactional metric triggers.
            queue.db.execute("UPDATE articles SET body_sha256=? WHERE url LIKE '%/b'", ('4' * 64,))
            queue.db.execute("UPDATE articles SET provider='other' WHERE url LIKE '%/d'")
            queue.db.execute("UPDATE articles SET body_sha256=NULL WHERE url LIKE '%/a'")
        heads, rechecks = check()
        assert heads == {'publisher': 1, 'other': 1}
        assert {r['provider'] for r in rechecks} == {'publisher', 'other'}
    finally:
        queue.close()


def test_selected_article_audit_never_fetches_or_takes_writer_lock(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
        "https://publisher.test/a": FakeResponse(article_html())})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.add_articles([{"url": "https://publisher.test/a"}])
        queue.accept([fetch_article(http, queue.claim("publisher", 300))])
        before = tuple(queue.db.execute("SELECT key,value FROM queue_settings ORDER BY key"))
        result = audit_selected(tmp_path, ["https://publisher.test/a"])
        assert result['contract_version'] == 8
        assert result["accepted"] and result["batch_proof"]["body_versions"] == 1
        assert result['articles'][0]['requested_url'] == result['articles'][0]['final_url'] == 'https://publisher.test/a'
        assert result["articles"][0]["body_characters"] > 100
        assert '"body":' not in str(result)
        assert not result["all_history_complete"]
        assert tuple(queue.db.execute("SELECT key,value FROM queue_settings ORDER BY key")) == before
        missing = audit_selected(tmp_path, ["https://publisher.test/missing"])
        assert not missing["accepted"] and missing["errors"][0]["state"] == "not_registered"
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('wrong_revision', [False, True])
def test_selected_source_replay_verifies_revision_clock_not_just_body_and_publication(config, tmp_path, wrong_revision):
    url = 'https://publisher.test/a'
    http, _ = http_fixture(config, tmp_path, {
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        url: FakeResponse(article_html())})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        queue.add_articles([{'url': url}])
        record = fetch_article(http, queue.claim('publisher', 300))
        if wrong_revision:
            # A self-consistent file/SHA receipt is not proof its metadata
            # agrees with the original; alter only this one metadata field.
            record['modified_at_raw'] = '2099-01-01T09:00:00+08:00'
        queue.accept([record])
        result = audit_selected(tmp_path, [url], registry=http.registry)
        assert result['accepted'] is (not wrong_revision)
        assert result['articles'][0]['modified_at_raw'] == record['modified_at_raw']
        assert result['articles'][0]['current_source_replay_verified'] is (not wrong_revision)
        if wrong_revision:
            assert result['errors'][0]['error'] == 'current_source_replay_changed_body_title_author_or_clock'
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('wrong_notice', [False, True])
def test_selected_source_replay_and_parquet_preserve_verified_notice_metadata(cnyes_config, tmp_path, wrong_notice):
    from test_news_cnyes_captions import NOTICE_FIGURE, NOTICE_TEXT, caption_page
    from test_news_cnyes_canonical_alias import MOBILE
    import json
    import pyarrow.parquet as pq

    payload = caption_page(current_figure=NOTICE_FIGURE, visible_figure=NOTICE_FIGURE, schema_caption=NOTICE_TEXT)
    http, _ = http_fixture(cnyes_config, tmp_path, {
        'https://m.cnyes.com/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        MOBILE: FakeResponse(payload),
    })
    queue = NewsQueue(tmp_path, http.registry)
    try:
        queue.add_articles([{'url': MOBILE}])
        record = fetch_article(http, queue.claim('cnyes', 300))
        assert json.loads(record['verified_article_notices_json'])[0]['text'] == NOTICE_TEXT
        if wrong_notice:
            record['verified_article_notices_json'] = None
        queue.accept([record])
        path = queue.db.execute('SELECT path FROM accepted_batches').fetchone()[0]
        saved = pq.read_table(tmp_path / path).to_pylist()[0]
        assert saved['verified_article_notices_json'] == record['verified_article_notices_json']
        changes = queue.db.total_changes
        result = audit_selected(tmp_path, [MOBILE], registry=http.registry)
        assert result['accepted'] is (not wrong_notice)
        assert queue.db.total_changes == changes
        assert result['articles'][0]['current_source_replay_verified'] is (not wrong_notice)
        if wrong_notice:
            assert result['errors'][0]['error'] == 'current_source_replay_changed_verified_article_notices'
        else:
            assert result['articles'][0]['verified_article_notice_count'] == 1
        assert '本公司所推薦' not in str(result)
    finally:
        queue.close()
        http.close()


@pytest.mark.parametrize('mutation', ['intact', 'object_key_order', 'missing', 'pair_order', 'image', 'caption', 'basis'])
def test_selected_source_replay_preserves_ordered_image_caption_pairs(cnyes_config, tmp_path, mutation):
    import json
    from test_news_cnyes_caption_narrative import FOREST, article_markup, schema
    from test_news_cnyes_canonical_alias import MOBILE, factset_page

    before = '<figure><img src="https://cimg.cnyes.cool/before.jpg"><figcaption>前方原始圖說</figcaption></figure>'
    after = '<figure><img src="https://cimg.cnyes.cool/after.jpg"><figcaption>後方原始圖說</figcaption></figure>'
    markup = article_markup(forest=before + FOREST + after)
    http, _ = http_fixture(cnyes_config, tmp_path, {
        'https://m.cnyes.com/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        MOBILE: FakeResponse(factset_page(markup, schema=schema(markup))),
    })
    queue = NewsQueue(tmp_path, http.registry)
    try:
        queue.add_articles([{'url': MOBILE}])
        record = fetch_article(http, queue.claim('cnyes', 300))
        original_sha = record['body_sha256']
        captions = json.loads(record['verified_image_captions_json'])
        assert len(captions) == 4
        if mutation == 'object_key_order':
            captions = [dict(reversed(list(pair.items()))) for pair in captions]
        elif mutation == 'pair_order':
            captions = captions[1:] + captions[:1]
        elif mutation == 'image':
            captions[0]['image_src'] = 'https://cimg.cnyes.cool/other.jpg'
        elif mutation == 'caption':
            captions[0]['text'] += '來源未提供的 123'
        elif mutation == 'basis':
            captions[0]['structure_basis'] = 'unverified'
        record['verified_image_captions_json'] = None if mutation == 'missing' else json.dumps(captions, ensure_ascii=False)
        queue.accept([record])
        changes = queue.db.total_changes
        proof = audit_selected(tmp_path, [MOBILE], registry=http.registry)
        expected = mutation in {'intact', 'object_key_order'}
        assert proof['accepted'] is expected
        assert queue.db.total_changes == changes
        assert proof['articles'][0]['body_sha256'] == original_sha
        assert proof['articles'][0]['current_source_replay_verified'] is expected
        if expected:
            assert proof['articles'][0]['verified_image_caption_count'] == 4
        else:
            assert proof['errors'][0]['error'] == 'current_source_replay_changed_verified_image_captions'
        assert '來源未提供' not in str(proof)
    finally:
        queue.close()
        http.close()


def test_selected_old_batch_neighbour_can_have_a_newer_head_and_shared_batch_is_read_once(config, tmp_path, monkeypatch):
    import scripts.audit_news_selected_articles as selected_module

    urls = ["https://publisher.test/a", "https://publisher.test/b", "https://publisher.test/c"]
    routes = {"https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /")}
    routes.update({url: FakeResponse(article_html()) for url in urls})
    http, _ = http_fixture(config, tmp_path, routes)
    queue = NewsQueue(tmp_path, http.registry)
    try:
        queue.add_articles([{"url": url} for url in urls])
        records = [fetch_article(http, queue.claim("publisher", 300)) for _ in urls]
        queue.accept(records)
        newer = dict(records[0])
        replacement = article_html(body="這是來源更新後的真實測試正文。" * 30)
        from downloader.news_fulltext_policy import extract_article
        import hashlib
        newer.update(extract_article(replacement, http.registry.sources["publisher"], page_url=records[0]["final_url"]))
        newer.update(raw_response_body=replacement, response_sha256=hashlib.sha256(replacement).hexdigest(),
                     http_response_bytes=len(replacement))
        queue.accept([newer])
        calls = []
        original = selected_module.pq.read_table
        monkeypatch.setattr(selected_module.pq, "read_table", lambda *a, **k: (calls.append(a[0]), original(*a, **k))[1])
        before = tuple(queue.db.execute("SELECT key,value FROM queue_settings ORDER BY key"))
        selected_urls = [record["requested_url"] for record in records[1:]]
        result = audit_selected(tmp_path, selected_urls, registry=http.registry)
        assert result["accepted"] and result["batch_proof"]["current_heads_verified"] == 2
        assert result["batch_proof"]["body_versions"] == 3  # Neighbour's original version is not discarded.
        assert len(calls) == 2  # One projection read and one canonical immutable-batch verification.
        assert all(article["current_source_replay_verified"] for article in result["articles"])
        assert tuple(queue.db.execute("SELECT key,value FROM queue_settings ORDER BY key")) == before
    finally:
        queue.close()
        http.close()


def test_selected_replay_rejects_changed_author_or_clock_without_rewriting_capture(config, tmp_path, monkeypatch):
    import scripts.audit_news_selected_articles as selected_module
    http, _ = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
        "https://publisher.test/a": FakeResponse(article_html())})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        queue.add_articles([{"url": "https://publisher.test/a"}])
        queue.accept([fetch_article(http, queue.claim("publisher", 300))])
        original = selected_module.extract_article
        monkeypatch.setattr(selected_module, "extract_article", lambda *a, **k: {
            **original(*a, **k), "authors_json": '["錯誤作者"]'})
        before = queue.db.total_changes
        result = audit_selected(tmp_path, ["https://publisher.test/a"], registry=http.registry)
        assert not result["accepted"] and result["articles"][0]["current_source_replay_verified"] is False
        assert result["errors"][0]["error"] == "current_source_replay_changed_body_title_author_or_clock"
        assert queue.db.total_changes == before
        assert '"body":' not in str(result)
    finally:
        queue.close()
        http.close()


def test_selected_audit_preserves_original_http_identity_and_observed_https_redirect(config, tmp_path):
    original, final = 'http://publisher.test/a', 'https://publisher.test/a'
    http, _ = http_fixture(config, tmp_path, {
        'http://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        'https://publisher.test/robots.txt': FakeResponse(b'User-agent: *\nAllow: /'),
        original: FakeResponse(status=301, headers={'Location': final}),
        final: FakeResponse(article_html()),
    })
    queue = NewsQueue(tmp_path, http.registry)
    try:
        with queue.db:
            queue.add_articles([{'url': original}])
        queue.accept([fetch_article(http, queue.claim('publisher', 300))])
        result = audit_selected(tmp_path, [original])
        assert result['accepted']
        assert result['articles'][0]['url'] == result['articles'][0]['requested_url'] == original
        assert result['articles'][0]['final_url'] == final
        assert queue.db.execute('SELECT url FROM articles').fetchone()[0] == original
    finally:
        queue.close()
        http.close()
