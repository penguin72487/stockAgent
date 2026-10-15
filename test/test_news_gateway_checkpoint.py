"""Body parser upgrades cannot erase or relabel exact-host negative proof."""
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
import json

import pytest

from downloader import news_fulltext_host_circuit as circuit_module
from downloader.news_fulltext_host_circuit import (
    GatewayHostCircuit, NEGATIVE_EVIDENCE_SCOPE, pause_seconds,
)
from downloader.news_fulltext_metrics import GATEWAY_HOST_CHECKPOINT_CONTRACT_VERSION
from downloader.news_fulltext_policy import NewsError, PolicyRegistry
from test_news_fulltext import FakeResponse, config, http_fixture
from test_news_gateway_host_circuit import gateway_clock


BAD, GOOD = 'bad.publisher.test', 'good.publisher.test'


def saved_pause(root, registry, *, version=4, generation=1, deadline=None):
    row = {'not_before_epoch': circuit_module.time.time() + pause_seconds(generation) if deadline is None else deadline,
           'source_data_fingerprint': registry.sources['publisher'].data_fingerprint,
           'pause_generation': generation, 'failure_count': 7,
           'evidence_scope': NEGATIVE_EVIDENCE_SCOPE}
    saved = {'contract_version': version, 'providers': {'publisher': {BAD: row}}}
    (root / 'gateway_host_cooldowns.json').write_text(json.dumps(saved))
    return saved, row


def changed_parser(registry):
    previous = registry.sources['publisher']
    registry.sources['publisher'] = replace(previous, data_fingerprint='a' * 64)
    assert previous.data_fingerprint != registry.sources['publisher'].data_fingerprint
    return previous.data_fingerprint


@pytest.mark.parametrize('version', [1, 2, 3, 4])
def test_parser_change_retains_exact_old_deadline_and_originating_evidence(config, tmp_path, monkeypatch, version):
    http, _ = http_fixture(config, tmp_path, {})
    with closing(http):
        clock = gateway_clock(monkeypatch)
        _, row = saved_pause(tmp_path, http.registry, version=version)
        path = tmp_path / 'gateway_host_cooldowns.json'
        original_bytes = path.read_bytes()
        old_fp = changed_parser(http.registry)
        restored = GatewayHostCircuit(tmp_path, http.registry)
        assert restored.paused['publisher'][BAD] == row['not_before_epoch']
        assert restored.pause_evidence_fingerprints['publisher'][BAD] == old_fp
        assert restored.pause_generations['publisher'][BAD] == 1
        assert restored.failures['publisher'][BAD] == (3 if version in {1, 2} else 7)
        assert not restored.successes and path.read_bytes() == original_bytes
        # Migration persists the actual original negative fingerprint, not the
        # currently loaded parser. Repeated restarts cannot extend the deadline.
        restored._checkpoint()
        migrated = json.loads(path.read_bytes())
        assert migrated['contract_version'] == GATEWAY_HOST_CHECKPOINT_CONTRACT_VERSION == 4
        migrated_row = migrated['providers']['publisher'][BAD]
        assert migrated_row['source_data_fingerprint'] == old_fp
        assert migrated_row['not_before_epoch'] == row['not_before_epoch']
        assert migrated_row['evidence_scope'] == NEGATIVE_EVIDENCE_SCOPE
        clock.value += 5
        restarted = GatewayHostCircuit(tmp_path, http.registry)
        assert restarted.remaining('publisher', BAD) == 25
        assert restarted.pause_evidence_fingerprints['publisher'][BAD] == old_fp


@pytest.mark.parametrize('version', [3, 4])
@pytest.mark.parametrize('generation', [1, 3, 5])
def test_expired_progressive_negative_survives_parser_change_but_not_as_health(
        config, tmp_path, monkeypatch, version, generation):
    http, _ = http_fixture(config, tmp_path, {})
    with closing(http):
        clock = gateway_clock(monkeypatch)
        _, row = saved_pause(tmp_path, http.registry, version=version, generation=generation)
        old_fp = changed_parser(http.registry)
        clock.value = row['not_before_epoch'] + 1
        restored = GatewayHostCircuit(tmp_path, http.registry)
        assert restored.blocked('publisher') == {} and restored.remaining('publisher', BAD) == 0
        assert restored.pause_generations['publisher'][BAD] == generation
        assert restored.failures['publisher'][BAD] == 7
        assert restored.pause_evidence_fingerprints['publisher'][BAD] == old_fp
        observation = restored.gateway_failure('publisher', BAD, 502, None)
        assert observation['scope'] == 'publisher' and not observation['pause_open']
        assert not restored.successes  # Expiry/restart never invents a success.
        restored.success('publisher', GOOD)
        observation = restored.gateway_failure('publisher', BAD, 504, None)
        expected_generation = min(5, generation + 1)
        assert observation['host_pause_generation'] == expected_generation
        assert observation['host_pause_seconds'] == pause_seconds(expected_generation)
        assert restored.paused['publisher'][BAD] == clock.value + pause_seconds(expected_generation)
        # A NEW real failed probe, unlike migration, owns the new fingerprint.
        assert restored.pause_evidence_fingerprints['publisher'][BAD] == 'a' * 64


@pytest.mark.parametrize('version', [1, 2])
def test_expired_legacy_pause_does_not_invent_a_failure_generation(config, tmp_path, monkeypatch, version):
    http, _ = http_fixture(config, tmp_path, {})
    with closing(http):
        clock = gateway_clock(monkeypatch)
        _, row = saved_pause(tmp_path, http.registry, version=version)
        clock.value = row['not_before_epoch'] + 1
        changed_parser(http.registry)
        restored = GatewayHostCircuit(tmp_path, http.registry)
        assert not restored.paused and not restored.failures and not restored.pause_generations
        assert not restored.pause_evidence_fingerprints and not restored.successes


def test_unrelated_host_write_does_not_relabel_or_extend_restored_negative(config, tmp_path, monkeypatch):
    http, _ = http_fixture(config, tmp_path, {})
    with closing(http):
        clock = gateway_clock(monkeypatch)
        _, row = saved_pause(tmp_path, http.registry, generation=5)
        old_fp = changed_parser(http.registry)
        restored = GatewayHostCircuit(tmp_path, http.registry)
        clock.value += 10
        restored.success('publisher', GOOD)
        for _ in range(3):
            restored.gateway_failure('publisher', 'another.publisher.test', 502, None)
        saved = json.loads(restored.path.read_bytes())['providers']['publisher']
        assert saved[BAD]['source_data_fingerprint'] == old_fp
        assert saved[BAD]['not_before_epoch'] == row['not_before_epoch']
        assert saved[BAD]['pause_generation'] == 5 and saved[BAD]['failure_count'] == 7
        assert saved['another.publisher.test']['source_data_fingerprint'] == 'a' * 64
        # Actual recovery clears only its own negative evidence.
        assert restored.success('publisher', 'another.publisher.test')
        assert restored.pause_evidence_fingerprints['publisher'] == {BAD: old_fp}
        assert restored.success('publisher', BAD)
        assert not restored.pause_evidence_fingerprints['publisher']
        assert json.loads(restored.path.read_bytes())['providers'] == {}


@pytest.mark.parametrize('bad_host', [
    'BAD.publisher.test', 'bad.publisher.test.', 'bad.publisher.test:443',
    'bad.publisher.test/path', 'bad.publisher.test?query=1', 'bad.publisher.test#fragment',
    'bad.publisher.test@other.test', 'bad.publisher.test\\other.test', '', 'a' * 254,
])
def test_malformed_hostname_cannot_transfer_a_negative_pause(config, tmp_path, bad_host):
    http, _ = http_fixture(config, tmp_path, {})
    with closing(http):
        saved, row = saved_pause(tmp_path, http.registry)
        saved['providers']['publisher'] = {bad_host: row}
        http.host_circuits.path.write_text(json.dumps(saved))
        restored = GatewayHostCircuit(tmp_path, http.registry)
        assert not restored.paused and not restored.pause_evidence_fingerprints


@pytest.mark.parametrize('corruption', [None, {}, {'not_before_epoch': True},
    {'source_data_fingerprint': 'wrong'}, {'pause_generation': None}, {'failure_count': True},
    {'evidence_scope': 'body_parser_success'}, {'evidence_scope': None}])
def test_one_malformed_row_cannot_erase_another_valid_host(config, tmp_path, corruption):
    http, _ = http_fixture(config, tmp_path, {})
    with closing(http):
        saved, valid = saved_pause(tmp_path, http.registry)
        invalid = None if corruption is None else (dict(valid, **corruption) if corruption else {})
        saved['providers']['publisher'] = {'invalid.publisher.test': invalid, BAD: valid}
        http.host_circuits.path.write_text(json.dumps(saved))
        restored = GatewayHostCircuit(tmp_path, http.registry)
        assert list(restored.paused['publisher']) == [BAD]
        assert restored.paused['publisher'][BAD] == valid['not_before_epoch']


@pytest.mark.parametrize('change', ['disabled', 'host_removed', 'host_reassigned', 'provider_removed'])
def test_pause_does_not_transfer_to_a_disabled_or_reassigned_provider(config, tmp_path, change):
    http, _ = http_fixture(config, tmp_path, {})
    with closing(http):
        saved_pause(tmp_path, http.registry)
        current = deepcopy(config)
        if change == 'disabled':
            current['providers'][0]['enabled'] = False
        elif change == 'host_removed':
            current['providers'][0]['domains'] = ['other.test']
        elif change == 'host_reassigned':
            other = dict(current['providers'][0], id='other', domains=[BAD])
            current['providers'].append(other)
        else:
            current['providers'][0]['id'] = 'other'
        restored = GatewayHostCircuit(tmp_path, PolicyRegistry(current, base=tmp_path))
        assert not restored.paused and not restored.pause_evidence_fingerprints


def test_parser_update_retains_host_pause_before_send_and_allows_healthy_host(config, tmp_path, monkeypatch):
    healthy, broken = 'https://' + GOOD + '/a', 'https://' + BAD + '/a'
    http, session = http_fixture(config, tmp_path, {healthy: FakeResponse(), broken: FakeResponse()})
    with closing(http):
        gateway_clock(monkeypatch)
        _, row = saved_pause(tmp_path, http.registry, generation=3)
        changed_parser(http.registry)
        http.host_circuits = GatewayHostCircuit(tmp_path, http.registry)
        with pytest.raises(NewsError, match='host_gateway_cooldown') as refusal:
            http._raw(broken)
        assert refusal.value.delay == 120
        assert not session.calls and http.requests_by_provider == {}
        assert http._raw(healthy).status == 200 and len(session.calls) == 1
        assert http.host_circuits.paused['publisher'][BAD] == row['not_before_epoch']
        assert http.host_circuits.pause_generations['publisher'][BAD] == 3
        assert not http.provider_not_before


def test_new_authorization_is_checked_even_with_restored_technical_negative(config, tmp_path, monkeypatch):
    healthy = 'https://' + GOOD + '/a'
    http, session = http_fixture(config, tmp_path, {healthy: FakeResponse()})
    with closing(http):
        gateway_clock(monkeypatch)
        saved_pause(tmp_path, http.registry)
        source = http.registry.sources['publisher']
        http.registry.sources['publisher'] = replace(source, data_fingerprint='a' * 64,
            authorization_fingerprint='b' * 64,
            authorization=dict(source.authorization, valid_until='2000-01-01T00:00:00Z'))
        http.host_circuits = GatewayHostCircuit(tmp_path, http.registry)
        assert http.host_circuits.remaining('publisher', BAD) == 30
        with pytest.raises(NewsError, match='authorization_expired'):
            http._raw(healthy)
        assert not session.calls and not http.requests_by_provider


def test_checkpoint_marker_does_not_relabel_existing_gateway_decision_contract(config, tmp_path):
    http, _ = http_fixture(config, tmp_path, {})
    with closing(http):
        observation = http.host_circuits.gateway_failure('publisher', BAD, 502, None)
        assert observation['contract_version'] == 3
        assert http.metrics.snapshot()['gateway_host_checkpoint_contract_version'] == 4
