"""Missing-only repairs must not become another downloader or retry reset."""
from contextlib import closing
from collections import Counter
from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
import threading
import time

import pytest

from downloader.artifact_io import atomic_write_bytes
from downloader.news_fulltext import run_service
from downloader.news_fulltext_policy import PolicyRegistry
from downloader.news_fulltext_queue import NewsQueue, verify_bodies
from downloader import news_fulltext_repairs as repair
from test_news_fulltext import FakeResponse, article_html, config, http_fixture


URL = "https://publisher.test/story/1"


@pytest.fixture
def queue(config, tmp_path):
    owner = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    yield owner
    owner.close()


def failure(queue, url=URL, **changes):
    values = {"state": "article_identity_not_verified", "error_code": "article_identity_not_verified",
              "attempts": 7, "due": time.time() - 60, "last_checked_at_utc": "2026-01-01T00:00:00+00:00"}
    values.update(changes)
    with queue.db:
        queue.add_articles([{"url": url}])
        queue.db.execute("UPDATE articles SET " + ",".join(key + "=?" for key in values) + " WHERE url=?",
                         (*values.values(), url))
    return head(queue, url)


def head(queue, url=URL):
    return dict(queue.db.execute("SELECT * FROM articles WHERE url=?", (url,)).fetchone())


def submit(queue, urls=(URL,), reason="Reviewed native parser handles the still-missing original"):
    return repair.submit_repairs(queue.root, queue.registry, iter(urls), reason=reason)


def rewrite(path, request=None, *, raw=None):
    value = raw if raw is not None else repair._bytes(request)
    destination = path.with_name(hashlib.sha256(value).hexdigest() + ".json")
    path.unlink()
    atomic_write_bytes(destination, value, durable=True, mode=0o600)
    return destination


def test_submission_keeps_live_owner_and_all_heads_unchanged(queue, monkeypatch):
    before = failure(queue)
    owner = queue.owner
    total = queue.db.total_changes
    monkeypatch.setattr("downloader.news_fulltext_http.NewsHTTP.__init__", lambda *a, **k: pytest.fail("no HTTP"))
    result = submit(queue)
    assert result["submitted"] and result["submitted_targets"] == 1
    assert result["publisher_requests"] == 0 and not result["queue_mutations"]
    assert not result["accepted_as_bodies"]
    assert queue.owner == owner and queue.db.total_changes == total and head(queue) == before
    assert queue.db.execute("SELECT name FROM sqlite_master WHERE name LIKE 'reviewed_repair_%'").fetchall() == []
    path = Path(result["request_path"])
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700
    request = repair._load(path)
    assert request["targets"][0]["id"] == before["id"].hex()
    assert request["sources"]["publisher"]["parser_fingerprint"] == queue.registry.sources["publisher"].parser_repair_fingerprint


def test_admission_changes_only_state_and_retains_durable_input(queue):
    before = failure(queue)
    result = submit(queue)
    assert repair.consume_repairs(queue)["outcomes"] == {"enqueued": 1}
    assert head(queue) == dict(before, state="retry")
    assert not Path(result["request_path"]).exists()
    processed = queue.root / "reviewed_repairs/processed"
    request_path = processed / (result["request_id"] + ".json")
    receipt = json.loads((processed / (result["request_id"] + ".outcome.json")).read_bytes())
    assert hashlib.sha256(request_path.read_bytes()).hexdigest() == result["request_id"]
    assert receipt["admitted_targets"] == 1 and not receipt["accepted_as_bodies"]
    assert receipt["outcomes"][0]["outcome"] == "enqueued"
    assert dict(queue.db.execute("SELECT key,value FROM counters"))["reviewed_repair_targets_admitted"] == 1
    assert repair.consume_repairs(queue)["processed"] == 0


def address_failure(queue, **changes):
    return failure(queue, state='non_public_address', error_code='non_public_address', **changes)


def clock_failure(queue, **changes):
    values = dict(state='invalid_bulk_publication_clock', error_code='invalid_bulk_publication_clock')
    values.update(changes)
    return failure(queue, **values)


def clock_submit(queue):
    return repair.submit_repairs(queue.root, queue.registry, [URL], reason='Reviewed original publication declarations',
                                 publication_clock_recheck=True)


def test_publication_clock_recheck_is_explicit_readonly_and_preserves_raw_head(queue, monkeypatch):
    before = clock_failure(queue)
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__', lambda *a, **k: pytest.fail('no HTTP'))
    assert not submit(queue)['submitted']
    result = clock_submit(queue)
    assert result['contract_version'] == 7 and result['publisher_requests'] == 0
    assert not result['queue_mutations'] and head(queue) == before
    assert repair._load(Path(result['request_path']))['kind'] == repair.PUBLICATION_CLOCK_RECHECK_KIND
    assert repair.consume_repairs(queue)['outcomes'] == {repair.PUBLICATION_CLOCK_RECHECK_OUTCOME: 1}
    assert head(queue) == dict(before, state='retry')
    receipt = json.loads((queue.root / 'reviewed_repairs/processed' / (result['request_id'] + '.outcome.json')).read_bytes())
    assert receipt['contract_version'] == 7 and receipt['admitted_targets'] == 1
    assert not receipt['publication_clocks_rewritten'] and not receipt['accepted_as_bodies']
    assert receipt['full_body_still_requires_normal_capture']
    assert dict(queue.db.execute('SELECT key,value FROM counters'))['reviewed_publication_clock_rechecks_admitted'] == 1


@pytest.mark.parametrize('changes', [
    {'body_sha256': 'a' * 64}, {'body_bytes': 1}, {'parquet_path': 'retained.parquet'},
    {'captured_at_utc': '2026-01-01T00:00:00+00:00'}, {'lease_owner': 'live'}, {'lease_until': 1},
    {'due': lambda: time.time() + 86400}, {'last_checked_at_utc': '2099-01-01T00:00:00+00:00'},
    {'attempts': -1}, {'error_code': 'http_403'},
])
def test_publication_clock_recheck_protects_bodies_leases_and_failure_clocks(queue, changes):
    before = clock_failure(queue, **{k: v() if callable(v) else v for k, v in changes.items()})
    assert not clock_submit(queue)['submitted'] and head(queue) == before


@pytest.mark.parametrize('state', [*sorted(repair.REPAIRABLE), 'article_rights_or_scope_excluded',
    'non_public_address', 'robots_disallowed', 'http_403', 'http_429', 'paywall_or_login', 'retry'])
def test_publication_clock_recheck_cannot_admit_other_failure_or_active_state(queue, state):
    before = failure(queue, state=state, error_code=state)
    assert not clock_submit(queue)['submitted'] and head(queue) == before


@pytest.mark.parametrize('changes', [{'attempts': 8}, {'lease_owner': 'live'}, {'body_sha256': 'b' * 64}])
def test_publication_clock_recheck_owner_revalidates_exact_current_head(queue, changes):
    clock_failure(queue)
    clock_submit(queue)
    with queue.db:
        queue.db.execute('UPDATE articles SET ' + ','.join(k + '=?' for k in changes) + ' WHERE url=?',
                         (*changes.values(), URL))
    protected = head(queue)
    assert repair.PUBLICATION_CLOCK_RECHECK_OUTCOME not in repair.consume_repairs(queue)['outcomes']
    assert head(queue) == protected


@pytest.mark.parametrize('version', [1, 2])
def test_legacy_missing_body_request_cannot_gain_publication_clock_scope(queue, version):
    clock_failure(queue)
    result = clock_submit(queue)
    path = Path(result['request_path'])
    request = json.loads(path.read_bytes())
    request.update(contract_version=version, kind='reviewed_missing_body_repair')
    rewrite(path, request)
    protected = head(queue)
    assert repair.consume_repairs(queue)['outcomes'] == {'invalid_request': 1}
    assert head(queue) == protected


@pytest.mark.parametrize('mode', ['address_recheck', 'retained_originals', 'empty_response_witnesses',
                                  'native_success_witnesses'])
def test_publication_clock_recheck_cannot_combine_other_modes(queue, mode):
    clock_failure(queue)
    with pytest.raises(ValueError, match='invalid_.*recheck_mode'):
        repair.submit_repairs(queue.root, queue.registry, [URL], reason='One explicit mode only',
                              publication_clock_recheck=True, **{mode: True if mode == 'address_recheck' else {}})


def test_publication_clock_recheck_crash_archive_resumes_without_minting_attempt(queue, monkeypatch):
    clock_failure(queue)
    result = clock_submit(queue)
    archive = repair._archive
    def unavailable(*args, **kwargs):
        raise OSError('simulated receipt-volume failure')
    monkeypatch.setattr(repair, '_archive', unavailable)
    assert repair.consume_repairs(queue)['outcomes'] == {repair.PUBLICATION_CLOCK_RECHECK_OUTCOME: 1}
    assert Path(result['request_path']).exists()
    before = clock_failure(queue, attempts=8)
    assert not clock_submit(queue)['submitted']  # Same parser and URL cannot mint another attempt.
    monkeypatch.setattr(repair, '_archive', archive)
    assert repair.consume_repairs(queue)['processed'] == 0
    assert head(queue) == before and not Path(result['request_path']).exists()
    assert queue.db.execute('SELECT count(*) FROM reviewed_repair_items').fetchone()[0] == 1


def address_submit(queue, reason='Recheck the exact legacy address rejection with every normal safety check'):
    return repair.submit_repairs(queue.root, queue.registry, [URL], reason=reason, address_recheck=True)


def test_address_recheck_is_readonly_until_owner_admission_and_changes_only_state(queue, monkeypatch):
    before = address_failure(queue)
    monkeypatch.setattr('downloader.news_fulltext_http.NewsHTTP.__init__', lambda *a, **k: pytest.fail('no HTTP'))
    total = queue.db.total_changes
    assert not submit(queue)['submitted']  # A parser review cannot re-arm a security failure.
    result = address_submit(queue)
    assert result['contract_version'] == 6 and result['publisher_requests'] == 0
    assert queue.db.total_changes == total and head(queue) == before
    assert repair._load(Path(result['request_path']))['kind'] == repair.ADDRESS_RECHECK_KIND
    assert repair.consume_repairs(queue)['outcomes'] == {repair.ADDRESS_RECHECK_OUTCOME: 1}
    assert head(queue) == dict(before, state='retry')
    receipt = json.loads((queue.root / 'reviewed_repairs/processed' / (result['request_id'] + '.outcome.json')).read_bytes())
    assert receipt['contract_version'] == 6 and receipt['admitted_targets'] == 1
    assert receipt['address_and_robots_checks_still_required'] and not receipt['permission_granted']
    assert receipt['full_body_still_requires_normal_capture'] and not receipt['accepted_as_bodies']
    assert dict(queue.db.execute('SELECT key,value FROM counters'))['reviewed_address_validation_rechecks_admitted'] == 1


@pytest.mark.parametrize('changes', [
    {'body_sha256': 'a' * 64}, {'parquet_path': 'retained.parquet'}, {'body_bytes': 1},
    {'captured_at_utc': '2026-01-01T00:00:00+00:00'}, {'lease_owner': 'live'}, {'lease_until': 1},
    {'due': lambda: time.time() + 86400}, {'attempts': -1},
    {'last_checked_at_utc': '2099-01-01T00:00:00+00:00'},
])
def test_address_recheck_preserves_saved_heads_leases_deadlines_and_clocks(queue, changes):
    before = address_failure(queue, **{k: v() if callable(v) else v for k, v in changes.items()})
    assert not address_submit(queue)['submitted'] and head(queue) == before


@pytest.mark.parametrize('state', ['http_403', 'http_429', 'robots_disallowed', 'paywall_or_login',
    'article_rights_or_scope_excluded', 'article_identity_not_verified', 'retry', 'robots_non_public_address'])
def test_address_recheck_cannot_retry_any_other_refusal_or_active_state(queue, state):
    before = failure(queue, state=state, error_code=state)
    assert not address_submit(queue)['submitted'] and head(queue) == before


@pytest.mark.parametrize('changes', [{'attempts': 8}, {'lease_owner': 'live'}, {'body_sha256': 'b' * 64}])
def test_address_recheck_owner_rechecks_exact_head_after_submission(queue, changes):
    address_failure(queue)
    address_submit(queue)
    with queue.db:
        queue.db.execute('UPDATE articles SET ' + ','.join(k + '=?' for k in changes) + ' WHERE url=?',
                         (*changes.values(), URL))
    protected = head(queue)
    result = repair.consume_repairs(queue)
    assert result['processed'] == 1 and repair.ADDRESS_RECHECK_OUTCOME not in result['outcomes']
    assert head(queue) == protected


@pytest.mark.parametrize('field,value', [('article_selectors', ['.changed']), ('enabled', False),
                                      ('article_path_pattern', '^/different$')])
def test_address_recheck_never_overrides_current_source_contract(config, queue, field, value):
    protected = address_failure(queue)
    address_submit(queue)
    changed = deepcopy(config)
    changed['providers'][0][field] = value
    queue.registry = PolicyRegistry(changed, base=queue.root)
    result = repair.consume_repairs(queue)
    assert repair.ADDRESS_RECHECK_OUTCOME not in result['outcomes'] and head(queue) == protected


def test_address_recheck_once_per_url_even_after_new_parser_reason_and_restart(config, queue):
    address_failure(queue)
    address_submit(queue)
    assert repair.consume_repairs(queue)['outcomes'] == {repair.ADDRESS_RECHECK_OUTCOME: 1}
    address_failure(queue, attempts=8)
    changed = deepcopy(config)
    changed['providers'][0]['article_selectors'] = ['.changed-parser']
    prior = queue.registry.sources['publisher'].parser_repair_fingerprint
    queue.close()
    with closing(NewsQueue(queue.root, PolicyRegistry(changed, base=queue.root))) as owner:
        assert owner.registry.sources['publisher'].parser_repair_fingerprint != prior
        protected = head(owner)
        result = address_submit(owner, reason='New reason does not grant another security recheck')
        assert not result['submitted'] and result['excluded'][0]['reason'] == 'already_admitted_for_address_validation'
        assert head(owner) == protected


def test_address_recheck_archive_restart_preserves_committed_once_marker(queue, monkeypatch):
    address_failure(queue)
    result = address_submit(queue)
    archive = repair._archive
    def unavailable(*args, **kwargs):
        raise OSError('simulated receipt-volume failure')
    monkeypatch.setattr(repair, '_archive', unavailable)
    assert repair.consume_repairs(queue)['outcomes'] == {repair.ADDRESS_RECHECK_OUTCOME: 1}
    assert Path(result['request_path']).exists()
    protected = address_failure(queue, attempts=8)
    monkeypatch.setattr(repair, '_archive', archive)
    assert repair.consume_repairs(queue)['processed'] == 0
    assert head(queue) == protected and not Path(result['request_path']).exists()
    assert queue.db.execute('SELECT count(*) FROM reviewed_repair_items').fetchone()[0] == 1


@pytest.mark.parametrize('mode', ['retained_originals', 'empty_response_witnesses', 'native_success_witnesses'])
def test_address_recheck_cannot_combine_other_repair_modes(queue, mode):
    address_failure(queue)
    with pytest.raises(ValueError, match='invalid_address_recheck_mode'):
        repair.submit_repairs(queue.root, queue.registry, [URL], reason='Explicit address review',
                              address_recheck=True, **{mode: {}})


def test_address_recheck_abi_cannot_be_rewritten_to_admit_parser_failure(queue):
    address_failure(queue)
    result = address_submit(queue)
    path = Path(result['request_path'])
    request = json.loads(path.read_bytes())
    request['targets'][0].update(state='article_identity_not_verified', error_code='article_identity_not_verified')
    rewrite(path, request)
    protected = head(queue)
    assert repair.consume_repairs(queue)['outcomes'] == {'invalid_request': 1}
    assert head(queue) == protected


@pytest.mark.parametrize("changes,expected", [
    ({"state": "complete", "error_code": None}, "not_semantic_parser_failure"),
    ({"body_sha256": "a" * 64}, "stored_head_protected"),
    ({"parquet_path": "sealed.parquet"}, "stored_head_protected"),
    ({"body_bytes": 1}, "stored_head_protected"),
    ({"captured_at_utc": "2026-01-01T00:00:00+00:00"}, "stored_head_protected"),
    ({"lease_owner": "other"}, "leased_head_protected"),
    ({"lease_until": 1}, "leased_head_protected"),
    ({"due": lambda: time.time() + 86400}, "future_or_invalid_deadline"),
    ({"due": float("inf")}, "future_or_invalid_deadline"),
    ({"due": -1}, "future_or_invalid_deadline"),
    ({"attempts": -1}, "invalid_attempts"),
    ({"last_checked_at_utc": "2099-01-01T00:00:00+00:00"}, "future_check_protected"),
    ({"last_checked_at_utc": "2026-01-01"}, "missing_or_invalid_check"),
    ({"last_checked_at_utc": None}, "missing_or_invalid_check"),
    ({"error_code": "http_403"}, "not_semantic_parser_failure"),
])
def test_protected_heads_never_mutated_or_submitted(queue, changes, expected):
    changes = {key: value() if callable(value) else value for key, value in changes.items()}
    before = failure(queue, **changes)
    result = submit(queue)
    assert not result["submitted"] and result["excluded"] == [{"url": URL, "reason": expected}]
    assert head(queue) == before
    assert not (queue.root / "reviewed_repairs").exists()


@pytest.mark.parametrize("state", ["http_403", "http_429", "robots_disallowed", "paywall_or_login",
                                    "article_rights_or_scope_excluded", "source_not_found", "source_article_empty_template",
                                    "awaiting_authorization", "waiting_cohort", "unsupported_article_path", "redirect_not_article",
                                    "retry", "retry_cooldown", "pending", "inflight", "unknown"])
def test_no_review_can_admit_refusal_or_normal_retry(queue, state):
    before = failure(queue, state=state, error_code=state)
    assert not submit(queue)["submitted"]
    assert head(queue) == before


@pytest.mark.parametrize("state", sorted(repair.REPAIRABLE))
def test_only_explicit_parser_outcomes_are_admitted(queue, state):
    before = failure(queue, state=state, error_code=state)
    assert submit(queue)["submitted"]
    assert repair.consume_repairs(queue)["outcomes"] == {"enqueued": 1}
    assert head(queue) == dict(before, state="retry")


def test_original_abi1_semantic_request_still_resumes(queue):
    before = failure(queue)
    result = submit(queue)
    path = Path(result["request_path"])
    request = json.loads(path.read_bytes())
    request["contract_version"] = 1
    rewrite(path, request)
    assert repair.consume_repairs(queue)["outcomes"] == {"enqueued": 1}
    assert head(queue) == dict(before, state="retry")


def test_abi2_scope_state_cannot_override_another_publishers_rights(queue):
    failure(queue)
    result = submit(queue)
    path = Path(result["request_path"])
    request = json.loads(path.read_bytes())
    before = failure(queue, state="article_rights_or_scope_excluded", error_code="article_rights_or_scope_excluded")
    request["targets"] = [repair._head(before)]
    rewrite(path, request)
    assert repair.consume_repairs(queue)["outcomes"] == {"not_semantic_parser_failure": 1}
    assert head(queue) == before


@pytest.fixture
def mitrade_queue(tmp_path):
    from downloader.news_fulltext import load_config
    settings, _, repo = load_config(Path("configs/news_fulltext.json"))
    settings = deepcopy(settings)
    settings["providers"] = [entry for entry in settings["providers"] if entry["id"] == "mitrade"]
    owner = NewsQueue(tmp_path, PolicyRegistry(settings, base=repo))
    yield owner, settings, repo
    owner.close()


MITRADE_URL = "https://www.mitrade.com/zh/insights/shares-analysis/more/20250919S05"


def legacy_scope_failure(queue, url=MITRADE_URL, **changes):
    values = {"state": "article_rights_or_scope_excluded", "error_code": "article_rights_or_scope_excluded",
              "last_checked_at_utc": "2026-10-20T23:07:06.765225+00:00"}
    values.update(changes)
    return failure(queue, url, **values)


def test_reviewed_v7_legacy_scope_retry_reuses_live_owner_without_scope_or_budget_reset(mitrade_queue, monkeypatch):
    queue, _, _ = mitrade_queue
    before = legacy_scope_failure(queue)
    with queue.db:
        queue.db.execute("INSERT INTO queue_settings VALUES ('failure_validation_metadata_attempts:v1:mitrade:article_rights_or_scope_excluded:20261020','2')")
    settings_before = dict(queue.db.execute("SELECT key,value FROM queue_settings"))
    owner, changes = queue.owner, queue.db.total_changes
    monkeypatch.setattr("downloader.news_fulltext_http.NewsHTTP.__init__", lambda *a, **k: pytest.fail("no HTTP during repair"))
    result = submit(queue, (MITRADE_URL,))
    assert result["submitted_targets"] == 1 and result["publisher_requests"] == 0
    assert not result["queue_mutations"] and head(queue, MITRADE_URL) == before
    assert queue.owner == owner and queue.db.total_changes == changes
    request = repair._load(Path(result["request_path"]))
    assert request["contract_version"] == 2
    # This checks authorization/admission, not how much real I/O fits in one
    # production slice. Deadline/stop behavior has independent scheduler tests.
    with monkeypatch.context() as clock:
        clock.setattr(repair.time, 'monotonic', lambda: 0.0)
        assert repair.consume_repairs(queue)["outcomes"] == {"enqueued": 1}
    assert head(queue, MITRADE_URL) == dict(before, state="retry")
    settings_after = dict(queue.db.execute("SELECT key,value FROM queue_settings"))
    assert settings_after.pop(repair.ROUND_ROBIN_CURSOR_KEY) == result['request_id']+'.json'
    assert settings_after == settings_before  # Only scheduling position is new; every source/budget value is exact.
    assert queue.owner == owner and repair.consume_repairs(queue)["processed"] == 0


@pytest.mark.parametrize("changes", [
    {"body_sha256": "a" * 64}, {"parquet_path": "saved.parquet"}, {"body_bytes": 1},
    {"captured_at_utc": "2026-01-01T00:00:00+00:00"}, {"lease_owner": "live"}, {"lease_until": 1},
    {"due": time.time() + 86400}, {"attempts": -1}, {"error_code": "http_403"},
    {"last_checked_at_utc": None}, {"last_checked_at_utc": "2026-01-01"},
    {"last_checked_at_utc": repair._MITRADE_PRIOR_SCOPE_CUTOFF},
    {"last_checked_at_utc": "2026-10-20T23:46:59+00:00"},
    {"last_checked_at_utc": "2099-01-01T00:00:00+00:00"},
    {"state": "http_429", "error_code": "http_429"},
    {"state": "robots_disallowed", "error_code": "robots_disallowed"},
])
def test_legacy_scope_retry_never_touches_current_failures_deadlines_leases_or_refusals(mitrade_queue, changes):
    queue, _, _ = mitrade_queue
    before = legacy_scope_failure(queue, **changes)
    result = submit(queue, (MITRADE_URL,))
    assert not result["submitted"] and head(queue, MITRADE_URL) == before


@pytest.mark.parametrize("field,value", [("embedded_article_layout", "mitrade_visible_article_v6"),
    ("note", "Changed capture contract"), ("enabled", False)])
def test_legacy_scope_retry_requires_the_entire_pinned_reviewed_capture(mitrade_queue, field, value):
    queue, settings, repo = mitrade_queue
    before = legacy_scope_failure(queue)
    settings["providers"][0][field] = value
    queue.registry = PolicyRegistry(settings, base=repo)
    assert not submit(queue, (MITRADE_URL,))["submitted"]
    assert head(queue, MITRADE_URL) == before


def test_legacy_scope_repair_never_admits_regional_path_or_refreshes_completed_body(mitrade_queue):
    queue, _, _ = mitrade_queue
    regional = MITRADE_URL.replace("/zh/", "/zh-au/")
    native = legacy_scope_failure(queue)
    region = legacy_scope_failure(queue, regional)
    saved = legacy_scope_failure(queue, MITRADE_URL+"-saved", body_sha256="b" * 64)
    result = submit(queue, (MITRADE_URL, regional, MITRADE_URL+"-saved"))
    assert result["submitted_targets"] == 1 and len(result["excluded"]) == 2
    assert repair.consume_repairs(queue)["outcomes"] == {"enqueued": 1}
    assert head(queue, MITRADE_URL) == dict(native, state="retry")
    assert head(queue, regional) == region and head(queue, MITRADE_URL+"-saved") == saved


def test_legacy_scope_request_rechecks_head_and_source_on_admission(mitrade_queue):
    queue, settings, repo = mitrade_queue
    legacy_scope_failure(queue)
    assert submit(queue, (MITRADE_URL,))["submitted"]
    settings["providers"][0]["authorization"]["scope"] += " Unreviewed later change."
    queue.registry = PolicyRegistry(settings, base=repo)
    before = head(queue, MITRADE_URL)
    assert repair.consume_repairs(queue)["outcomes"] == {"not_semantic_parser_failure": 1}
    assert head(queue, MITRADE_URL) == before


def test_legacy_scope_retry_does_not_rearm_after_unknown_byline_or_duplicate_review(mitrade_queue):
    queue, _, _ = mitrade_queue
    legacy_scope_failure(queue)
    assert submit(queue, (MITRADE_URL,))["submitted"]
    assert repair.consume_repairs(queue)["outcomes"] == {"enqueued": 1}
    # Even a replay of the old observation cannot bypass the once-per-parser marker.
    before = legacy_scope_failure(queue, attempts=8)
    assert submit(queue, (MITRADE_URL,))["excluded"][0]["reason"] == "already_admitted_for_parser"
    assert head(queue, MITRADE_URL) == before
    # A real v7 response with an unreviewed byline remains excluded, not retried.
    current = legacy_scope_failure(queue, attempts=8, last_checked_at_utc="2026-10-20T23:47:01+00:00")
    assert not submit(queue, (MITRADE_URL,))["submitted"]
    assert head(queue, MITRADE_URL) == current


def test_original_abi1_cannot_be_used_to_submit_new_scope_failure(mitrade_queue):
    queue, _, _ = mitrade_queue
    before = legacy_scope_failure(queue)
    result = submit(queue, (MITRADE_URL,))
    path = Path(result["request_path"])
    request = json.loads(path.read_bytes())
    request["contract_version"] = 1
    rewrite(path, request)
    assert repair.consume_repairs(queue)["outcomes"] == {"invalid_request": 1}
    assert head(queue, MITRADE_URL) == before


@pytest.mark.parametrize("changes,expected", [
    ({"attempts": 8}, "head_changed_since_review"),
    ({"due": 1}, "head_changed_since_review"),
    ({"last_checked_at_utc": "2026-02-01T00:00:00+00:00"}, "head_changed_since_review"),
    ({"body_sha256": "b" * 64}, "stored_head_protected"),
    ({"due": lambda: time.time() + 900}, "future_or_invalid_deadline"),
    ({"lease_owner": "live"}, "leased_head_protected"),
])
def test_head_race_is_rechecked_by_owner(queue, changes, expected):
    # Collection can precede this test by more than 15 minutes in the full suite.
    # Relative deadlines must start at execution, not at module import.
    changes = {key: value() if callable(value) else value for key, value in changes.items()}
    failure(queue)
    submit(queue)
    with queue.db:
        queue.db.execute("UPDATE articles SET " + ",".join(key + "=?" for key in changes), tuple(changes.values()))
    before = head(queue)
    assert repair.consume_repairs(queue)["outcomes"] == {expected: 1}
    assert head(queue) == before


@pytest.mark.parametrize("field,value", [("article_selectors", [".new"]), ("note", "Same parser, new review"),
                                          ("enabled", False), ("article_path_pattern", r"^/different$")])
def test_source_contract_change_never_reinterprets_old_request(config, queue, field, value):
    before = failure(queue)
    submit(queue)
    changed = deepcopy(config)
    changed["providers"][0][field] = value
    queue.registry = PolicyRegistry(changed, base=queue.root)
    outcomes = repair.consume_repairs(queue)["outcomes"]
    assert outcomes in ({"source_identity_changed": 1}, {"awaiting_authorization": 1}, {"unsupported_article_path": 1})
    assert head(queue) == before


def test_same_parser_once_even_after_new_failure_prose_change_and_restart(config, queue):
    failure(queue)
    assert submit(queue)["submitted"]
    assert repair.consume_repairs(queue)["outcomes"] == {"enqueued": 1}
    failure(queue, attempts=8, last_checked_at_utc="2026-02-01T00:00:00+00:00")
    changed = deepcopy(config)
    changed["providers"][0]["note"] = "A changed note must not grant another request"
    root = queue.root
    queue.close()
    owner = NewsQueue(root, PolicyRegistry(changed, base=root))
    try:
        protected = head(owner)
        result = submit(owner, reason="Different reason must not re-arm this parser")
        assert not result["submitted"] and result["excluded"][0]["reason"] == "already_admitted_for_parser"
        assert head(owner) == protected
    finally:
        owner.close()


def test_duplicate_pending_reviews_allow_only_one_admission(queue):
    before = failure(queue)
    first, second = submit(queue), submit(queue, reason="Second independent review, same parser")
    assert first["request_id"] != second["request_id"]
    outcomes = repair.consume_repairs(queue)["outcomes"]
    assert outcomes["enqueued"] == 1 and sum(outcomes.values()) == 2
    assert head(queue) == dict(before, state="retry")
    assert queue.db.execute("SELECT count(*) FROM reviewed_repair_items WHERE outcome='enqueued'").fetchone()[0] == 1


def test_archive_failure_after_commit_resumes_without_rearming(queue, monkeypatch):
    failure(queue)
    result = submit(queue)
    real_archive = repair._archive
    monkeypatch.setattr(repair, "_archive", lambda *a: (_ for _ in ()).throw(OSError("simulated power loss")))
    assert repair.consume_repairs(queue)["outcomes"] == {"enqueued": 1}
    assert Path(result["request_path"]).exists()
    # The actual attempt could already have finished before a later pump.
    protected = failure(queue, attempts=8, last_checked_at_utc="2026-02-01T00:00:00+00:00")
    monkeypatch.setattr(repair, "_archive", real_archive)
    assert repair.consume_repairs(queue)["processed"] == 0
    assert not Path(result["request_path"]).exists()
    assert head(queue) == protected
    assert queue.db.execute("SELECT count(*) FROM reviewed_repair_items").fetchone()[0] == 1


@pytest.mark.parametrize('first_pump_deferred', [False, True])
def test_replayed_processed_input_is_idempotent_after_actual_attempt(queue, monkeypatch, first_pump_deferred):
    failure(queue)
    result = submit(queue)
    request = Path(result['request_path']).read_bytes()
    actual_consume = repair.consume_repairs
    calls = 0
    def consume(*args, **kwargs):
        nonlocal calls
        calls += 1
        return {'processed': 0, 'outcomes': {}} if first_pump_deferred and calls == 1 else actual_consume(*args, **kwargs)
    monkeypatch.setattr(repair, 'consume_repairs', consume)
    deadline = time.monotonic() + 5
    while head(queue)['state'] != 'retry' and time.monotonic() < deadline:
        repair.consume_repairs(queue, max_seconds=.1)
    assert head(queue)['state'] == 'retry'
    protected = failure(queue, attempts=8, last_checked_at_utc="2026-02-01T00:00:00+00:00")
    atomic_write_bytes(result['request_path'], request, durable=True, mode=0o600)
    total = queue.db.execute("SELECT count(*) FROM reviewed_repair_items").fetchone()[0]
    deadline = time.monotonic() + 5
    while Path(result['request_path']).exists() and time.monotonic() < deadline:
        assert repair.consume_repairs(queue, max_seconds=.1)['processed'] == 0
    assert not Path(result['request_path']).exists()
    assert head(queue) == protected
    assert queue.db.execute("SELECT count(*) FROM reviewed_repair_items").fetchone()[0] == total


def test_new_request_with_current_failed_head_still_cannot_bypass_once_marker(queue):
    failure(queue)
    result = submit(queue)
    request = json.loads(Path(result['request_path']).read_bytes())
    repair.consume_repairs(queue)
    protected = failure(queue, attempts=8, last_checked_at_utc="2026-02-01T00:00:00+00:00")
    request['reason'] = 'Changed current review does not change the parser'
    request['targets'] = [repair._head(protected)]
    payload = repair._bytes(request)
    pending = queue.root / 'reviewed_repairs/pending' / (hashlib.sha256(payload).hexdigest() + '.json')
    atomic_write_bytes(pending, payload, durable=True, mode=0o600)
    assert repair.consume_repairs(queue)['outcomes'] == {'already_admitted_for_parser': 1}
    assert head(queue) == protected


def test_admission_and_once_marker_rollback_together(queue, monkeypatch):
    original = failure(queue)
    result = submit(queue)
    real_admit = repair._admit

    def crash(*args):
        assert real_admit(*args) == 'enqueued'
        raise RuntimeError('crash before admission marker commit')

    monkeypatch.setattr(repair, '_admit', crash)
    with pytest.raises(RuntimeError, match='crash before'):
        repair.consume_repairs(queue)
    assert head(queue) == original and Path(result['request_path']).exists()
    assert queue.db.execute('SELECT count(*) FROM reviewed_repair_jobs').fetchone()[0] == 0
    assert queue.db.execute('SELECT count(*) FROM reviewed_repair_items').fetchone()[0] == 0
    monkeypatch.setattr(repair, '_admit', real_admit)
    assert repair.consume_repairs(queue)['outcomes'] == {'enqueued': 1}


def test_finite_cohort_is_not_expanded(config, tmp_path):
    config['providers'][0]['acquisition_mode'] = 'finite'
    queue = NewsQueue(tmp_path, PolicyRegistry(config, base=tmp_path))
    try:
        original = failure(queue)
        result = submit(queue)
        assert not result['submitted'] and result['excluded'][0]['reason'] == 'outside_frozen_cohort'
        assert head(queue) == original
        with queue.db:
            queue.db.execute('INSERT INTO finite_cohort_members VALUES (?,?,?)', ('publisher', original['id'], URL))
        assert submit(queue)['submitted']
        assert repair.consume_repairs(queue)['outcomes'] == {'enqueued': 1}
        assert head(queue) == dict(original, state='retry')
    finally:
        queue.close()


def test_bounded_slices_resume_and_stop_without_new_scans(queue, monkeypatch):
    # Exercise the item budget/cursor without host scheduling consuming the
    # independent wall-time budget. Stop/deadline tests cover preemption.
    monkeypatch.setattr(repair, 'time', SimpleNamespace(monotonic=lambda: 0.0, time=time.time))
    urls = ["https://publisher.test/story/" + str(i) for i in range(70)]
    for url in urls:
        failure(queue, url)
    result = submit(queue, urls)
    assert repair.consume_repairs(queue, limit=17)["outcomes"] == {"enqueued": 17}
    assert Path(result["request_path"]).exists()
    stopped = threading.Event()
    stopped.set()
    assert repair.consume_repairs(queue, stop=stopped)["processed"] == 0
    assert repair.consume_repairs(queue, limit=32)["outcomes"] == {"enqueued": 32}
    assert repair.consume_repairs(queue, limit=32)["outcomes"] == {"enqueued": 21}
    assert not Path(result["request_path"]).exists()
    assert queue.db.execute("SELECT count(*) FROM articles WHERE state='retry'").fetchone()[0] == 70


@pytest.mark.parametrize("malform", ["digest", "duplicate_key", "duplicate_target", "naive_clock", "future_clock",
                                     "body_state", "wrong_id", "bool_attempt", "infinite_due", "wrong_generation",
                                     "unknown_key", "oversize", "deep_json", "symlink", "directory", "fifo"])
def test_bad_control_file_quarantined_without_blocking_later_work(queue, malform):
    failure(queue)
    result = submit(queue)
    path = Path(result["request_path"])
    request = json.loads(path.read_bytes())
    if malform == "digest":
        atomic_write_bytes(path, b"{}", mode=0o600)
    elif malform == "duplicate_key":
        path = rewrite(path, raw=b'{"contract_version":1,"contract_version":1}')
    elif malform == "duplicate_target":
        request["targets"].append(request["targets"][0])
        path = rewrite(path, request)
    elif malform == "naive_clock":
        request["targets"][0]["last_checked_at_utc"] = "2026-01-01"
        path = rewrite(path, request)
    elif malform == "future_clock":
        request["created_at_utc"] = "2099-01-01T00:00:00+00:00"
        path = rewrite(path, request)
    elif malform == "body_state":
        request["targets"][0]["state"] = "complete"
        path = rewrite(path, request)
    elif malform == "wrong_id":
        request["targets"][0]["id"] = "a" * 64
        path = rewrite(path, request)
    elif malform == "bool_attempt":
        request["targets"][0]["attempts"] = True
        path = rewrite(path, request)
    elif malform == "infinite_due":
        request["targets"][0]["due"] = float("inf")
        path = rewrite(path, raw=json.dumps(request).encode())
    elif malform == "wrong_generation":
        request["sources"]["publisher"]["parser_fingerprint"] = "x" * 64
        path = rewrite(path, request)
    elif malform == "unknown_key":
        request["reset_due"] = True
        path = rewrite(path, request)
    elif malform == "oversize":
        atomic_write_bytes(path, b" " * (repair.MAX_REQUEST_BYTES + 1), mode=0o600)
    elif malform == "deep_json":
        path = rewrite(path, raw=b'[' * 2000 + b'0' + b']' * 2000)
    else:
        path.unlink()
        if malform == "symlink":
            outside = queue.root / "outside.json"
            outside.write_text("DO NOT READ OR CHANGE")
            path.symlink_to(outside)
        elif malform == "directory":
            path.mkdir()
        else:
            os.mkfifo(path, 0o600)
    good_url = "https://publisher.test/story/2"
    failure(queue, good_url)
    submit(queue, [good_url])
    original = head(queue)
    # Quarantine durably writes evidence. A slow fsync may exhaust this
    # bounded slice; the next ordinary owner iteration must still admit the
    # good request, without relaxing the production wall-time budget.
    outcomes = Counter()
    for _ in range(8):
        outcomes.update(repair.consume_repairs(queue, max_seconds=0.1)["outcomes"])
        if outcomes.get('enqueued'):
            break
    assert outcomes == {"invalid_request": 1, "enqueued": 1}
    assert not path.exists() and not path.is_symlink()
    assert head(queue) == original and head(queue, good_url)["state"] == "retry"
    assert list((queue.root / "reviewed_repairs/rejected").glob("*.input"))
    if malform == "symlink":
        assert outside.read_text() == "DO NOT READ OR CHANGE"


def test_quarantine_resumes_after_receipt_written_before_move(queue, monkeypatch):
    original = failure(queue)
    result = submit(queue)
    path = Path(result['request_path'])
    atomic_write_bytes(path, b'{}', mode=0o600)
    real_replace = os.replace

    def crash(source, target):
        if Path(source) == path:
            raise OSError('crash after rejection receipt before input move')
        real_replace(source, target)

    monkeypatch.setattr(repair.os, 'replace', crash)
    assert repair.consume_repairs(queue)['outcomes'] == {'invalid_request': 1}
    assert path.exists() and not list((queue.root / 'reviewed_repairs/rejected').glob('*.input'))
    monkeypatch.setattr(repair.os, 'replace', real_replace)
    assert repair.consume_repairs(queue)['outcomes'] == {'invalid_request': 1}
    assert not path.exists() and len(list((queue.root / 'reviewed_repairs/rejected').glob('*.input'))) == 1
    assert head(queue) == original
    # Reintroducing a bad file under the same name does not overwrite evidence
    # or leave the inbox permanently stuck on a rejected input.
    atomic_write_bytes(path, b'{"different":"invalid"}', mode=0o600)
    assert repair.consume_repairs(queue)['outcomes'] == {'invalid_request': 1}
    assert not path.exists() and len(list((queue.root / 'reviewed_repairs/rejected').glob('*.input'))) == 2


def test_no_inbox_means_no_schema_or_disk_writes(queue):
    before = queue.db.total_changes
    assert repair.consume_repairs(queue) == {"processed": 0, "outcomes": {}}
    assert queue.db.total_changes == before
    assert not (queue.root / "reviewed_repairs").exists()


def test_slice_wall_budget_defers_rest_instead_of_readmitting(queue, monkeypatch):
    urls = [URL, 'https://publisher.test/story/2']
    for url in urls:
        failure(queue, url)
    submit(queue, urls)
    clock = [0.0]
    real_admit = repair._admit

    def slow(*args):
        outcome = real_admit(*args)
        clock[0] += 0.06
        return outcome

    monkeypatch.setattr(repair.time, 'monotonic', lambda: clock[0])
    monkeypatch.setattr(repair, '_admit', slow)
    assert repair.consume_repairs(queue)['outcomes'] == {'enqueued': 1}
    assert repair.consume_repairs(queue)['outcomes'] == {'enqueued': 1}
    assert repair.consume_repairs(queue)['processed'] == 0


def test_symlink_inbox_is_refused_without_following_it(queue, tmp_path):
    failure(queue)
    outside = tmp_path / "outside"
    outside.mkdir()
    (queue.root / "reviewed_repairs").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="repair_directory_symlink"):
        submit(queue)
    assert repair.consume_repairs(queue)["inbox_unavailable"]
    assert list(outside.iterdir()) == []


@pytest.mark.parametrize("urls,reason", [([], "review"), ([URL] * 513, "review"), ([URL], ""), ([URL], "x" * 241)])
def test_explicit_submission_is_bounded(queue, urls, reason):
    failure(queue)
    with pytest.raises(ValueError):
        submit(queue, urls, reason=reason)


@pytest.mark.parametrize('clock', [False, True])
def test_actual_service_uses_original_http_and_durable_acceptance(config, tmp_path, clock, monkeypatch):
    healthy = "https://publisher.test/story/2"
    http, session = http_fixture(config, tmp_path, {
        "https://publisher.test/robots.txt": FakeResponse(b"User-agent: *\nAllow: /"),
        URL: FakeResponse(article_html()), healthy: FakeResponse(article_html(body="另一篇獨立的原始新聞正文。" * 32)),
    })
    queue = NewsQueue(tmp_path, http.registry)
    try:
        before = clock_failure(queue) if clock else failure(queue)
        with queue.db:
            queue.add_articles([{"url": healthy}])
        clock_submit(queue) if clock else submit(queue)
        owner = queue.owner
        accept = queue.accept
        def stop_after_both_durable_acceptances(records):
            accepted = accept(records)
            if queue.db.execute("SELECT count(*) FROM articles WHERE state='complete'").fetchone()[0] == 2:
                http.stop.set()
            return accepted
        monkeypatch.setattr(queue, 'accept', stop_after_both_durable_acceptances)
        result = run_service(queue, http, config, max_articles=1, max_discovery_requests=0, duration_seconds=5)
        assert queue.owner == owner and result["states"] == {"complete": 2}
        assert [url for url, kwargs in session.calls].count(URL) == 1
        assert [url for url, kwargs in session.calls].count(healthy) == 1
        assert len(session.calls) == 3  # one REP request; no probe then refetch
        assert head(queue)["attempts"] == before["attempts"] + 1
        assert verify_bodies(tmp_path)["accepted"]
        assert result["counters"]["reviewed_repair_targets_admitted"] == 1
        if clock:
            assert result['counters']['reviewed_publication_clock_rechecks_admitted'] == 1
        assert result["performance"]["stages"]["reviewed_repair_admission"]["count"] >= 1
    finally:
        http.close()
        queue.close()


def test_service_admission_does_not_clear_existing_provider_deadline(config, tmp_path, monkeypatch):
    http, session = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        before = failure(queue)
        submit(queue)
        deadline = time.time() + 600
        http.provider_not_before['publisher'] = deadline
        consume = repair.consume_repairs
        def stop_after_admission(*args, **kwargs):
            result = consume(*args, **kwargs)
            if head(queue)['state'] == 'retry':
                http.stop.set()
            return result
        monkeypatch.setattr(repair, 'consume_repairs', stop_after_admission)
        run_service(queue, http, config, max_articles=1, max_discovery_requests=0, duration_seconds=5)
        assert head(queue) == dict(before, state='retry')
        assert session.calls == [] and http.provider_not_before['publisher'] == deadline
    finally:
        http.close()
        queue.close()


@pytest.mark.parametrize('condition', ['stopped', 'low_disk'])
def test_service_does_not_admit_while_draining_or_low_disk(config, tmp_path, monkeypatch, condition):
    from types import SimpleNamespace

    http, session = http_fixture(config, tmp_path, {})
    queue = NewsQueue(tmp_path, http.registry)
    try:
        original = failure(queue)
        result = submit(queue)
        if condition == 'stopped':
            http.stop.set()
        else:
            monkeypatch.setattr('downloader.news_fulltext.shutil.disk_usage', lambda path: SimpleNamespace(free=0))
        run_service(queue, http, config, max_articles=1, max_discovery_requests=0, duration_seconds=0.03)
        assert head(queue) == original and Path(result['request_path']).exists()
        assert session.calls == []
    finally:
        http.close()
        queue.close()


@pytest.mark.parametrize('mode', ['missing', 'address', 'clock'])
def test_cli_submission_while_owner_held_does_not_construct_lifecycle_or_http(config, queue, tmp_path, monkeypatch, capsys, mode):
    from scripts import download_news_fulltext as cli

    before = address_failure(queue) if mode == 'address' else clock_failure(queue) if mode == 'clock' else failure(queue)
    cfg = tmp_path / "config.json"
    cfg.write_text(json.dumps(config))
    monkeypatch.setattr(cli, "NewsQueue", lambda *a, **k: pytest.fail("no second owner"))
    monkeypatch.setattr(cli, "NewsHTTP", lambda *a, **k: pytest.fail("no HTTP"))
    monkeypatch.setattr("stockagent.data.news_worker_lifecycle.NewsWorkerLifecycle", lambda *a: pytest.fail("no lifecycle"))
    output = tmp_path / "private-submission.json"
    assert cli.main(["repair", "--config", str(cfg), "--root", str(queue.root), "--url", URL,
                     "--reason", "Reviewed explicit missing original", "--output", str(output)]
                    + (['--address-recheck'] if mode == 'address'
                       else ['--publication-clock-recheck'] if mode == 'clock' else [])) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["submitted_targets"] == 1 and result["publisher_requests"] == 0
    assert head(queue) == before and output.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("args", [
    ["repair", "--url", URL], ["repair", "--reason", "review"],
    ["repair", "--reason", "review", "--url", URL, "--refresh"],
    ["repair", "--reason", "review", "--url", URL, "--skip-local-ingest"],
    ["repair", "--reason", "review", "--url", URL, "--continuous"],
    ["fetch", "--url", URL, "--reason", "review"],
    ["fetch", "--url", URL, "--address-recheck"],
    ["repair", "--url", URL, "--reason", "review", "--address-recheck", "--reviewed-empty-responses", "private.json"],
    ['fetch', '--url', URL, '--publication-clock-recheck'],
    ['repair', '--url', URL, '--reason', 'review', '--publication-clock-recheck', '--address-recheck'],
    ['repair', '--url', URL, '--reason', 'review', '--publication-clock-recheck', '--reviewed-empty-responses', 'private.json'],
])
def test_cli_rejects_scope_changing_combinations(args):
    from scripts.download_news_fulltext import main

    with pytest.raises(SystemExit) as error:
        main(args)
    assert error.value.code == 2
