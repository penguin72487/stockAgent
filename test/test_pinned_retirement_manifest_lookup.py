from pathlib import Path
import json

import pytest

from stockagent.data_sync import artifact_discard as module
from stockagent.data_sync.desync_snapshots import SnapshotError


def _manifest(root, dataset, snapshot, relative='operations/example'):
    path = root / 'manifests' / dataset / (snapshot + '.json')
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({'metadata': {'artifact_relative_root': relative}}))
    return path


def test_pin_lookup_matches_every_dataset_and_escapes_literal_glob_characters(tmp_path):
    names = {'snapshot-1', 'literal*[?]'}
    expected = {_manifest(tmp_path, dataset, name)
                for dataset in ('first', 'second') for name in names}
    _manifest(tmp_path, 'first', 'literal-foreign')
    _manifest(tmp_path, 'second', 'snapshot-2')
    assert set(module._pinned_manifest_paths(tmp_path, names)) == expected
    assert not list(module._pinned_manifest_paths(tmp_path, {'missing'}))
    assert not list(module._pinned_manifest_paths(tmp_path, set()))


def test_large_history_uses_exact_names_without_losing_broken_symlink_or_duplicate_pin(tmp_path, monkeypatch):
    pin = 'literal*[?]'
    expected = {_manifest(tmp_path, dataset, pin) for dataset in ('first', 'second')}
    for index in range(200):
        _manifest(tmp_path, 'first', 'history-' + str(index))
    broken = tmp_path / 'manifests/first/broken.json'
    broken.symlink_to(tmp_path / 'absent.json')
    # A regression to a global manifest glob would enumerate the whole history.
    monkeypatch.setattr(Path, 'glob', lambda *args: pytest.fail('unbounded manifest glob'))
    assert set(module._pinned_manifest_paths(tmp_path, {pin, 'broken'})) == expected | {broken}


@pytest.mark.parametrize('snapshot', ['', '.', '..', '../foreign', 'dataset/snapshot'])
def test_pin_lookup_fails_closed_on_non_filename_snapshot_ids(tmp_path, snapshot):
    with pytest.raises(SnapshotError, match='exact manifest filename'):
        list(module._pinned_manifest_paths(tmp_path, {snapshot}))


def _isolate_other_gates(monkeypatch):
    monkeypatch.setattr(module, 'artifact_process_references', lambda *args: [])
    monkeypatch.setattr(module, 'active_configuration_references', lambda *args: [])
    monkeypatch.setattr(module, 'shared_inode_references', lambda *args: [])
    monkeypatch.setattr(module, 'artifact_service_references', lambda *args: {})


def test_fresh_pin_gates_follow_selected_source_dependencies_and_changes(tmp_path, monkeypatch):
    _isolate_other_gates(monkeypatch)
    repo, cold = tmp_path / 'repo', tmp_path / 'cold'
    source = repo / 'artifacts/operations/example/child'
    _manifest(cold, 'a', 'pinned')
    _manifest(cold, 'b', 'unrelated', 'operations/other')
    # Unpinned malformed JSON cannot require reading an unrelated history.
    _manifest(cold, 'a', 'ignored').write_text('invalid JSON')
    pins = {'pinned'}
    monkeypatch.setattr(module, '_pinned_snapshot_ids', lambda *args: set(pins))
    assert module._gates(source, {'rows': []}, repo, tmp_path / 'hot', cold) == ['pinned-release']
    pins.clear()
    pins.add('unrelated')
    assert not module._gates(source, {'rows': []}, repo, tmp_path / 'hot', cold)
    pins.add('ignored')
    with pytest.raises(json.JSONDecodeError):
        module._gates(source, {'rows': []}, repo, tmp_path / 'hot', cold)


def test_pin_gate_preserves_redirected_manifest_rejection(tmp_path, monkeypatch):
    _isolate_other_gates(monkeypatch)
    cold = tmp_path / 'cold'
    pinned = _manifest(cold, 'a', 'pinned')
    other = tmp_path / 'redirect.json'
    pinned.rename(other)
    pinned.symlink_to(other)
    monkeypatch.setattr(module, '_pinned_snapshot_ids', lambda *args: {'pinned'})
    with pytest.raises(SnapshotError):
        module._gates(tmp_path / 'repo/artifacts/operations/example', {'rows': []},
                      tmp_path / 'repo', tmp_path / 'hot', cold)
