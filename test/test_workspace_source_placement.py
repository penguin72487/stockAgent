from pathlib import Path

import pytest

from scripts import audit_workspace_source_placement as audit


def test_inventory_preserves_mixed_mounts_and_does_not_sum_old_sizes(tmp_path, monkeypatch):
    repo, workspace = tmp_path / 'repo', tmp_path / 'e'
    repo.mkdir()
    workspace.mkdir()
    (repo / 'data_alpha').mkdir()
    (repo / 'data_beta').mkdir()
    (repo / 'artifacts').mkdir()
    (repo / 'service_data').mkdir()
    def device(path):
        return 2 if path == workspace or path == repo / 'data_beta' else 1
    monkeypatch.setattr(audit, '_device', device)
    catalog = {'datasets': [{'dataset': 'alpha', 'source': 'data_alpha/parts',
                             'role': 'history', 'publish': True},
                            {'dataset': 'beta', 'source': 'data_beta', 'publish': False}]}
    nested = repo / 'data_beta' / 'old'
    previous = {str(repo / 'data_beta'): {'allocated_bytes': 123, 'state': 'partial'}}
    value = audit.inventory(repo, workspace, catalog, [(nested, '8:64')], previous, '2026-11-03T00:00:00Z')
    rows = {Path(row['path']).name: row for row in value['roots']}
    assert rows['data_alpha']['root_placement'] == 'C'
    assert rows['data_beta']['root_placement'] == 'E'
    assert rows['data_beta']['nested_mounts'] == [{'path': str(nested), 'device': '8:64'}]
    assert rows['data_beta']['catalog_datasets'][0]['publish'] is False
    assert rows['data_beta']['old_allocated_observation']['allocated_bytes'] == 123
    assert rows['data_beta']['current_recursive_size_bytes'] is None
    assert not rows['data_beta']['source_retirement_authorized']
    assert value['old_sizes_are_nonatomic_and_not_additive']
    assert '2026-11-03T00:00:00Z' in audit.render(value)


def test_missing_catalog_source_is_unknown_not_zero(tmp_path, monkeypatch):
    repo, workspace = tmp_path / 'repo', tmp_path / 'e'
    repo.mkdir()
    workspace.mkdir()
    monkeypatch.setattr(audit, '_device', lambda path: 2 if path == workspace else 1)
    value = audit.inventory(repo, workspace,
        {'datasets': [{'dataset': 'not-yet', 'source': 'data_missing/history', 'publish': True}]}, [], {})
    row = next(row for row in value['roots'] if row['path'] == str(repo / 'data_missing'))
    assert row['state'] == 'missing' and row['root_placement'] == 'unknown'
    assert row['current_recursive_size_bytes'] is None


def test_same_device_is_not_misrepresented_as_e_workspace(tmp_path):
    repo, workspace = tmp_path / 'repo', tmp_path / 'e'
    repo.mkdir()
    workspace.mkdir()
    with pytest.raises(ValueError, match='distinct'):
        audit.inventory(repo, workspace, {'datasets': []}, [], {})


def test_mountinfo_unescapes_paths_and_rejects_incomplete_input():
    row = '10 1 8:80 / /srv/a\\040b rw,relatime - ext4 /dev/sdf rw'
    assert audit.mount_rows(row) == [(Path('/srv/a b'), '8:80')]
    with pytest.raises(ValueError):
        audit.mount_rows('bad row')
