"""Regression for the R123 source-list omission, across both evidence layouts."""
import hashlib
import json
import subprocess

import pytest


@pytest.mark.parametrize('layout', ['individual_gzip', 'packed_gzip_v1'])
def test_complete_collection_retains_atomic_writer_source(tmp_path, monkeypatch, layout):
    from scripts import profile_workflow_speeds as profile

    root = tmp_path / 'repository'
    root.mkdir()
    subprocess.run(['git', 'init', '--quiet', str(root)], check=True)
    atomic_source = b'"""Observed atomic publisher; this fixture must never execute."""\nraise AssertionError("executed")\n'
    collector_source = b'"""Observed collector source fixture."""\nraise AssertionError("executed")\n'
    for name, raw in [('downloader/artifact_io.py', atomic_source),
                      ('scripts/profile_workflow_speeds.py', collector_source)]:
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
    monkeypatch.setattr(profile, 'ROOT', root)
    # Restore any backend previously selected by another maintained test.
    monkeypatch.setattr(profile, '_METRIC_TREE', profile._METRIC_TREE)
    monkeypatch.setattr(profile, '_METRIC_DESCRIPTOR', profile._METRIC_DESCRIPTOR)
    monitor = root / 'monitor.json'
    monitor.write_text('{"sources": []}')
    output = tmp_path / 'observation'
    assert profile.main(['--output-dir', str(output), '--metric-backend', 'python',
        '--monitor-snapshot', str(monitor), '--training-root', str(root / 'empty_training'),
        '--evidence-storage-layout', layout, '--max-file-mib', '1', '--max-total-mib', '4']) == 0
    sources = json.loads((output / 'measurement_sources.json').read_bytes())['files']
    expected_sha = hashlib.sha256(atomic_source).hexdigest()
    assert {'path': 'downloader/artifact_io.py', 'sha256': expected_sha} in sources, (
        'atomic_writer_measurement_source_missing')
    manifest = json.loads((output / 'evidence_manifest.json').read_bytes())
    original, = [r for r in manifest['files'] if r['path'] == str(root / 'downloader/artifact_io.py')]
    assert original['state'] == 'captured' and original['sha256'] == expected_sha
    assert original['bytes'] == len(atomic_source)
    # Future worktree edits must not replace the source of this frozen observation.
    (root / 'downloader/artifact_io.py').write_bytes(b'raise RuntimeError("new generation")\n')
    readback = profile.verify_collection(output)
    assert readback['state'] == 'verified'
    assert readback['collector_phases_verified'] == 13
    assert readback['unique_originals_verified'] >= 3
