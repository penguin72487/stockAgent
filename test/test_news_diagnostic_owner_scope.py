"""Canonical HTTP ownership cannot be bypassed by changing a probe's directory."""
from copy import deepcopy
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader.artifact_io import atomic_write_json
from downloader.news_fulltext import load_config
from scripts.probe_news_sources import isolated_diagnostic_config, require_diagnostic_owner_scope


def registry(*domains, allowed=True, provider='publisher'):
    return SimpleNamespace(sources={provider: SimpleNamespace(provider=provider, domains=domains, allowed=allowed)})


@pytest.mark.parametrize('probe_domain,live_domain', [
    ('news.publisher.test', 'publisher.test'), ('publisher.test', 'news.publisher.test'),
    ('publisher.test', 'publisher.test'), ('www.nccu.edu.tw', 'www.nccu.edu.tw'),
])
def test_overlapping_enabled_host_cannot_have_independent_root(tmp_path, probe_domain, live_domain):
    with pytest.raises(ValueError, match='isolated_diagnostic_overlaps_enabled_production_host'):
        require_diagnostic_owner_scope({'root': 'probe'}, registry(probe_domain),
                                       {'root': 'production'}, registry(live_domain), tmp_path)


@pytest.mark.parametrize('root', ['production', './production', 'probe/../production'])
def test_same_resolved_root_keeps_canonical_http_lock(root, tmp_path):
    require_diagnostic_owner_scope({'root': root}, registry('publisher.test'),
                                   {'root': 'production'}, registry('publisher.test'), tmp_path)


@pytest.mark.parametrize('probe_domain', ['other.test', 'notpublisher.test', 'publisher.test.evil.test'])
def test_disjoint_new_host_does_not_stop_other_providers(probe_domain, tmp_path):
    require_diagnostic_owner_scope({'root': 'probe'}, registry(probe_domain),
                                   {'root': 'production'}, registry('publisher.test'), tmp_path)


def test_disabled_candidate_does_not_claim_live_http_domain(tmp_path):
    require_diagnostic_owner_scope({'root': 'probe'}, registry('publisher.test'),
                                   {'root': 'production'}, registry('publisher.test', allowed=False), tmp_path)


def test_scope_check_does_not_mutate_source_or_configuration(tmp_path):
    probe, canonical = {'root': 'probe'}, {'root': 'production'}
    snapshot = deepcopy((probe, canonical))
    require_diagnostic_owner_scope(probe, registry('new.test'), canonical, registry('old.test'), tmp_path)
    assert (probe, canonical) == snapshot


def _isolated_source(domain='new.test', provider='new_source', parallelism=1):
    return {'id': provider, 'domains': [domain], 'enabled': True,
            'interval_seconds': 10, 'parallelism': parallelism,
            'authorization': {'kind': 'personal_research', 'scope': 'Private bounded public-reading diagnosis only',
                'reviewed_on': '2026-10-21', 'evidence_url': 'https://'+domain+'/',
                'automated_access_review': 'no_explicit_prohibition_identified'}}


def test_isolated_preparation_does_not_inherit_production_cnyes_focus_or_change_input(tmp_path):
    base = json.loads(Path('configs/news_fulltext.json').read_text())
    base.update(failure_diagnostic_focus={'private': 'exact old-owner selection'},
                failure_diagnostic_metadata_identity_upgrade=True,
                retained_cnyes_failure_replay_enabled=True)
    sources = [_isolated_source()]
    original = deepcopy((base, sources))
    candidate = isolated_diagnostic_config(base, sources, root=tmp_path/'probe',
        repo=tmp_path, canonical_registry=registry('live.test'))
    assert (base, sources) == original
    assert not list(tmp_path.iterdir())  # Preparation owns no files, HTTP or queue.
    assert 'provider_catalog' not in candidate
    assert candidate['failure_diagnostic_focus'] is None
    for flag in ('failure_diagnostic_metadata_identity_upgrade', 'retained_cnyes_failure_replay_enabled',
                 'cached_cnyes_replay_enabled', 'cached_cnyes_metadata_replay_enabled'):
        assert candidate[flag] is False
    assert candidate['exclusive_http_owner'] is True and candidate['adaptive_rate'] == {'enabled': False}
    assert candidate['publish'] is candidate['remote_training_export'] is False
    assert candidate['providers'] == sources and candidate['providers'] is not sources
    for key in ('user_agent', 'unknown_source_policy', 'max_response_bytes', 'request_timeout_seconds',
                'response_deadline_seconds', 'robots_ttl_seconds', 'min_free_disk_bytes', 'max_attempts'):
        assert candidate[key] == base[key]
    path = tmp_path/'candidate.json'
    atomic_write_json(path, candidate)
    loaded, policies, _ = load_config(path, repo=tmp_path)
    assert loaded['failure_diagnostic_focus'] is None
    assert set(policies.sources) == {'new_source'} and policies.sources['new_source'].allowed


@pytest.mark.parametrize('domain', ['live.test', 'news.live.test', 'test'])
def test_isolated_preparation_rejects_live_host_or_invalid_domain(tmp_path, domain):
    base = json.loads(Path('configs/news_fulltext.json').read_text())
    with pytest.raises(ValueError):
        isolated_diagnostic_config(base, [_isolated_source(domain)], root='probe',
            repo=tmp_path, canonical_registry=registry('live.test'))
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize('sources', [None, [], 'provider', [{}]*9])
def test_isolated_preparation_requires_bounded_explicit_sources(tmp_path, sources):
    with pytest.raises(ValueError, match='bounded explicit diagnostic source list'):
        isolated_diagnostic_config({'root': 'live'}, sources, root='probe',
            repo=tmp_path, canonical_registry=registry('live.test'))


def test_isolated_preparation_never_resets_canonical_root_or_promotes_permission(tmp_path):
    base = json.loads(Path('configs/news_fulltext.json').read_text())
    with pytest.raises(ValueError, match='disjoint root'):
        isolated_diagnostic_config(base, [_isolated_source()], root=base['root'],
            repo=tmp_path, canonical_registry=registry('live.test'))
    source = _isolated_source()
    source['authorization']['kind'] = 'awaiting_authorization'
    with pytest.raises(ValueError, match='still requires reviewed permission'):
        isolated_diagnostic_config(base, [source], root='probe',
            repo=tmp_path, canonical_registry=registry('live.test'))


@pytest.mark.parametrize('slots,accepted', [(1, True), (8, True), (16, True), (24, False)])
def test_isolated_preparation_preserves_explicit_independent_worker_reservations(tmp_path, slots, accepted):
    base = json.loads(Path('configs/news_fulltext.json').read_text())
    sources = [_isolated_source(f'new{i}.test', f'new{i}', min(8, slots))
               for i in range((slots+7)//8)]
    if not accepted:
        with pytest.raises(ValueError, match='worker reservations'):
            isolated_diagnostic_config(base, sources, root='probe', repo=tmp_path,
                canonical_registry=registry('live.test'))
    else:
        candidate = isolated_diagnostic_config(base, sources, root='probe', repo=tmp_path,
            canonical_registry=registry('live.test'))
        assert candidate['workers'] == slots
        assert [s['parallelism'] for s in candidate['providers']] == [s['parallelism'] for s in sources]
