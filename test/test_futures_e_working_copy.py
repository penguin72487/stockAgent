from contextlib import contextmanager
import json
from pathlib import Path

import pytest

from scripts import stage_futures_e_working_copy as stage


def _setup(tmp_path, monkeypatch):
    source = tmp_path/'source'
    source.mkdir()
    (source/'source_manifest.json').write_text('{"source":"original"}')
    (source/'contract.json').write_bytes(b'original margin evidence')
    for directory in ('downloads', 'migration'):
        (tmp_path/directory).mkdir()
    monkeypatch.setattr(stage, 'SOURCE', source)
    monkeypatch.setattr(stage, 'REPORT', tmp_path/'report.json')
    monkeypatch.setattr(stage.volume, 'ENROLLMENT', tmp_path/'enrollment.json')
    monkeypatch.setattr(stage.volume, 'check', lambda: {'state': 'mounted'})
    monkeypatch.setattr(stage.volume, 'admit', lambda required: {'admitted_bytes': required})
    monkeypatch.setattr(stage.volume, 'path', lambda name: tmp_path/name)
    monkeypatch.setattr(stage.volume, 'configuration', lambda: {'minimum_free_bytes': 1})
    @contextmanager
    def reserve(*args, **kwargs):
        yield {'other_reserved_growth_bytes': 0}
    monkeypatch.setattr(stage, 'reserve_capacity', reserve)
    monkeypatch.setattr(stage, 'process_references_many', lambda *a, **k: [])
    return source


def test_e_margin_copy_verifies_original_without_changing_owners(tmp_path, monkeypatch):
    source = _setup(tmp_path, monkeypatch)
    stage.main()
    result = json.loads((tmp_path/'report.json').read_text())
    destination = Path(result['e_destination'])
    assert (destination/'contract.json').read_bytes() == (source/'contract.json').read_bytes()
    assert result['source_files_deleted'] == 0
    assert result['source_alias_changed'] is False
    assert result['trainer_or_publisher_repointed'] is False
    assert result['financial_or_training_admission'] is False
    assert Path(result['relocation_evidence']).is_file()


def test_e_margin_copy_defers_when_original_has_live_owner(tmp_path, monkeypatch):
    source = _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(stage, 'process_references_many', lambda *a, **k: ['original-owner'])
    with pytest.raises(RuntimeError, match='live owner'):
        stage.main()
    assert (source/'contract.json').read_bytes() == b'original margin evidence'
    assert not (tmp_path/'report.json').exists()
    assert not (tmp_path/'downloads'/stage.NAME).exists()


def test_e_source_copy_rejects_marker_outside_selected_source(tmp_path, monkeypatch):
    _setup(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match='relative marker'):
        stage.main(marker='../outside')


def test_e_source_seed_preserves_hardlinks_and_readback(tmp_path, monkeypatch):
    import os
    source = _setup(tmp_path, monkeypatch)
    os.link(source/'contract.json', source/'other-name.json')
    stage.main(seed_hardlinks=True)
    result = json.loads((tmp_path/'report.json').read_text())
    destination = Path(result['e_destination'])
    assert (destination/'contract.json').samefile(destination/'other-name.json')
    assert result['hardlinked_seed_used'] is True
    assert result['source_files_deleted'] == 0


def test_e_source_preserves_symlink_names_without_reading_targets(tmp_path, monkeypatch):
    import os
    source = _setup(tmp_path, monkeypatch)
    (source/'internal-link').symlink_to('contract.json')
    (source/'external-link').symlink_to('../outside/not-present')
    stage.main(preserve_symlinks=True)
    result = json.loads((tmp_path/'report.json').read_text())
    destination = Path(result['e_destination'])
    assert os.readlink(destination/'internal-link') == 'contract.json'
    assert os.readlink(destination/'external-link') == '../outside/not-present'
    assert result['symlink_names_preserved'] == 2
    assert result['symlink_targets_independently_verified'] is False
    assert result['source_alias_changed'] is False and result['source_files_deleted'] == 0


def test_e_source_rejects_symlinks_without_opt_in(tmp_path, monkeypatch):
    source = _setup(tmp_path, monkeypatch)
    (source/'external-link').symlink_to('/not-present')
    with pytest.raises(ValueError, match='redirected/special'):
        stage.main()
    assert (source/'external-link').is_symlink()
    assert not (tmp_path/'report.json').exists()


def test_e_source_without_internal_hardlinks_skips_redundant_seed(tmp_path, monkeypatch):
    import os
    source = _setup(tmp_path, monkeypatch)
    # An alias outside the selected scope is not an E hardlink to recreate.
    os.link(source/'contract.json', tmp_path/'outside-scope.json')
    monkeypatch.setattr(stage.subprocess, 'run',
                        lambda *a, **k: pytest.fail('unneeded complete seed was started'))
    stage.main(seed_hardlinks=True)
    result = json.loads((tmp_path/'report.json').read_text())
    destination = Path(result['e_destination'])
    assert (destination/'contract.json').read_bytes() == (source/'contract.json').read_bytes()
    assert result['hardlinked_seed_requested'] is True
    assert result['hardlinked_seed_used'] is False
    assert result['source_files_deleted'] == 0 and not result['source_alias_changed']


def test_e_copy_preserves_checkout_relative_source_path(tmp_path, monkeypatch):
    source = _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(stage, 'ROOT', tmp_path)
    stage.main()
    result = json.loads((tmp_path/'report.json').read_text())
    assert Path(result['e_destination']) == tmp_path/'downloads'/stage.NAME/source.name


def test_e_copy_does_not_stage_when_physical_admission_fails(tmp_path, monkeypatch):
    source = _setup(tmp_path, monkeypatch)
    def denied(required):
        raise RuntimeError('physical backing unavailable')
    monkeypatch.setattr(stage.volume, 'admit', denied)
    with pytest.raises(RuntimeError, match='physical backing unavailable'):
        stage.main()
    assert (source/'contract.json').read_bytes() == b'original margin evidence'
    assert not (tmp_path/'downloads'/stage.NAME).exists()
