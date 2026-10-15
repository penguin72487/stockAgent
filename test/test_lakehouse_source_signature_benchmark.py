import ast
from datetime import UTC, datetime
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from downloader.artifact_io import atomic_write_json
from scripts import benchmark_lakehouse_source_signature as bench
from stockagent.runtime_identity import identity_sha256


def test_single_stat_preserves_exact_order_fields_and_policy_bit():
    calls = []

    class ObservedPath:
        def __init__(self, name, inode):
            self.name, self.inode = name, inode

        def __str__(self):
            return self.name

        def stat(self):
            calls.append(self.name)
            return SimpleNamespace(st_ino=self.inode, st_size=13, st_mtime_ns=19, st_ctime_ns=23)

    paths = [ObservedPath('second', 2), ObservedPath('first', 1)]
    expected = [('second', 2, 13, 19, 23), ('first', 1, 13, 19, 23)]
    assert bench.one_stat_signature(paths, False) == identity_sha256([False, expected])
    assert calls == ['second', 'first']
    assert bench.one_stat_signature(paths, True) == identity_sha256([True, expected])
    assert calls == ['second', 'first'] * 2  # Never memoize across signature passes.


def test_single_stat_preserves_source_failure():
    class VanishedPath:
        def stat(self):
            raise FileNotFoundError('source disappeared')

    with pytest.raises(FileNotFoundError, match='disappeared'):
        bench.one_stat_signature([VanishedPath()], False)


def test_complete_source_observation_reuses_the_same_stat_pass(tmp_path):
    path = tmp_path / 'metadata.json'
    path.write_text('{}')
    observations = []
    token = bench.one_stat_signature([path], False, observations)
    assert len(observations) == 1 and identity_sha256(observations[0]) == token
    assert len(observations[0][1]) == 1 and observations[0][1][0][0] == str(path)


def test_candidate_refuses_an_unrecognized_original_boundary():
    with pytest.raises(ValueError, match='boundary'):
        bench.candidate_source('def register(c):\n    source_signature = None\n')
    with pytest.raises(ValueError, match='four stat'):
        bench.candidate_source('def register(c):\n    source_signature = lambda: p.stat()\n')


@pytest.fixture
def idle_fixture(tmp_path, monkeypatch):
    # A synthetic unit-test reference, not the measured native old-code
    # artifact. Keep the original four-stat expression explicit even after
    # production adopts the candidate; do not redefine the benchmark baseline.
    tree = ast.parse(Path(bench.lakehouse.__file__).read_text())
    tree.body = [node for node in tree.body
                 if not (isinstance(node, ast.FunctionDef) and node.name == '_source_metadata_signature')]
    register = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == 'register')
    assignment = next(node for node in ast.walk(register) if isinstance(node, ast.Assign)
                      and any(isinstance(target, ast.Name) and target.id == 'source_signature' for target in node.targets))
    assignment.value = ast.parse('lambda: identity_sha256([include_orphans, '
        '[(str(p), p.stat().st_ino, p.stat().st_size, p.stat().st_mtime_ns, p.stat().st_ctime_ns) '
        'for p in paths]])', mode='eval').body
    original = ast.unparse(ast.fix_missing_locations(tree))
    variants = {key: bench.module(source, key) for key, source in (
        ('baseline', original), ('candidate', bench.candidate_source(original)))}
    monkeypatch.chdir(tmp_path)
    config = tmp_path / 'configs/data_sync'
    config.mkdir(parents=True)
    for name in ('packed_datasets.json', 'backup_history_disposition_20261004.json'):
        (config / name).write_text('{}')
    cold = tmp_path / 'cold'
    manifest = cold / 'manifests/example/version.json'
    manifest.parent.mkdir(parents=True)
    manifest.write_text('{"fixed":true}')
    c = {'cold_root': str(cold), 'include_unreferenced_cold_objects': False}
    for active in variants.values():
        monkeypatch.setattr(active, 'guard', lambda config: None)
    monkeypatch.setattr(bench, 'limits', lambda: {'constant': True})

    class Connection:
        def execute(self, statement):
            return SimpleNamespace(fetchone=lambda: (7,) if 'snapshot' in statement else (11,))

        def close(self):
            pass

    monkeypatch.setattr(bench.lakehouse, 'connect', lambda config, **options: Connection())
    return variants, c, {'snapshot_id': 7, 'release_count': 11, 'unavailable_release_count': 0}


def test_complete_idle_replay_readback_matches_preserved_function(idle_fixture, tmp_path):
    variants, c, status = idle_fixture
    paths = bench.metadata_paths(c)
    token = bench.one_stat_signature(paths, False)
    clock = datetime(2026, 10, 15, 2, tzinfo=UTC)
    results = [bench.run_trial(active, c, tmp_path / label, status, token, False, clock)
               for label, active in variants.items()]
    assert results[0]['semantic_output_sha256'] == results[1]['semantic_output_sha256']
    assert all(row['native_complete_source_signature_sha256'] == token and row['native_signature_passes'] == 1
               for row in results)
    assert all(row['readonly_connections'] == 1 and row['durable_complete_readback']
               and row['full_capture_calls'] == row['production_catalog_writes'] == row['source_writes'] == 0
               for row in results)


def test_mutated_input_refuses_reuse_instead_of_suppressing_recapture(idle_fixture, tmp_path):
    variants, c, status = idle_fixture
    token = bench.one_stat_signature(bench.metadata_paths(c), False)
    (tmp_path / 'configs/data_sync/packed_datasets.json').write_text('{"changed":true}')
    for label, active in variants.items():
        with pytest.raises(ValueError, match='full source capture'):
            bench.run_trial(active, c, tmp_path / label, status, token, False, datetime.now(UTC))
        rejected = json.loads((tmp_path / label / 'rejection.json').read_bytes())
        assert rejected['criterion'] == 'native_metadata_token_changed'
        assert rejected['native_readonly_connection_calls'] == rejected['production_catalog_writes'] == 0
        observed = json.loads((tmp_path / label / 'native-source-signature-input.json').read_bytes())
        assert identity_sha256(observed[0]) == rejected['native_observed_source_tokens'][0]


def test_readonly_catalog_advance_refuses_reuse(idle_fixture, tmp_path):
    variants, c, status = idle_fixture
    status['snapshot_id'] = 6
    token = bench.one_stat_signature(bench.metadata_paths(c), False)
    for label, active in variants.items():
        with pytest.raises(ValueError, match='full source capture'):
            bench.run_trial(active, c, tmp_path / label, status, token, False, datetime.now(UTC))
        rejected = json.loads((tmp_path / label / 'rejection.json').read_bytes())
        assert rejected['criterion'] == 'native_catalog_reuse_not_proven'
        assert rejected['native_readonly_connection_calls'] == 1


@pytest.mark.parametrize('guard', ['expired', 'unavailable', 'row_count'])
def test_existing_hourly_recovery_and_count_guards_are_preserved(idle_fixture, tmp_path, monkeypatch, guard):
    variants, c, status = idle_fixture
    if guard == 'unavailable':
        status['unavailable_release_count'] = 1
    elif guard == 'row_count':
        status['release_count'] = 10
    else:
        write = bench.atomic_write_json

        def expired(path, value, **options):
            if path.name == 'catalog-input-signature.json':
                value = {**value, 'full_capture_epoch': value['full_capture_epoch'] - 3601}
            return write(path, value, **options)

        monkeypatch.setattr(bench, 'atomic_write_json', expired)
    token = bench.one_stat_signature(bench.metadata_paths(c), False)
    for label, active in variants.items():
        with pytest.raises(ValueError, match='full source capture'):
            bench.run_trial(active, c, tmp_path / label, status, token, False, datetime.now(UTC))


def test_memo_scope_does_not_hide_metadata_changes_or_source_set(idle_fixture, tmp_path):
    _, c, _ = idle_fixture
    old_paths = bench.metadata_paths(c)
    old = bench.one_stat_signature(old_paths, False)
    path = tmp_path / 'cold/manifests/example/new.json'
    atomic_write_json(path, {'new': True})
    assert bench.metadata_paths(c) != old_paths
    assert bench.one_stat_signature(bench.metadata_paths(c), False) != old
    path.unlink()
    assert bench.one_stat_signature(bench.metadata_paths(c), False) == old
