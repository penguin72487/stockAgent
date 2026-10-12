"""Canonical HTTP ownership cannot be bypassed by changing a probe's directory."""
from copy import deepcopy
from types import SimpleNamespace

import pytest

from scripts.probe_news_sources import require_diagnostic_owner_scope


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
