from datetime import date
import json

import pytest

from downloader.artifact_io import atomic_write_json, sha256_file
from scripts import run_tej_option_gap_after_large_axis as relay


def test_prepare_binds_profile_and_candidates_to_exact_native_source(tmp_path, monkeypatch):
    root = tmp_path / 'source'
    root.mkdir()
    snapshot = root / 'snapshot.json'
    snapshot.write_text('{}')
    official = tmp_path / 'official.json'
    official.write_text('{}')
    proof = {'contract': 'native', 'task_id': 'a', 'query_attempt_id': 'b', 'type': 'Option (ALL)',
             'smart_id': 'TEJ Derivatives', 'table': 'Delisted Option(2014~2017)',
             'snapshot_path': 'binary', 'snapshot_manifest_path': str(snapshot),
             'snapshot_manifest_sha256': sha256_file(snapshot), 'source_proof': {'sha256': 'c'},
             'market_observations_obtained': False, 'full_axis_admitted_to_plan': False,
             'training_input_admitted': False}
    receipt = tmp_path / 'readback.json'
    atomic_write_json(receipt, proof)
    profile = tmp_path / 'profile.json'
    candidate_dir = tmp_path / 'candidates'
    report = tmp_path / 'report.json'
    monkeypatch.setattr(relay, 'audit_large', lambda *a: proof)

    def profile_menu(readback, output):
        atomic_write_json(output, {'contract': 'tej_option_source_menu_label_profile_v1',
            'source_receipt_sha256': sha256_file(readback), 'source_snapshot_sha256': 'c',
            'source_table': proof['table'], 'implementation_sha256': sha256_file(
                relay.REPO / 'scripts/audit_tej_option_axis_labels.py'),
            'market_observations_obtained': False, 'training_input_admitted': False})

    def build_candidates(source, menu, output, result):
        output.mkdir()
        body = {'source_pins': {str(source.resolve()): sha256_file(source),
                                str(menu.resolve()): sha256_file(menu)}}
        atomic_write_json(output / 'manifest.json', body)
        atomic_write_json(result, body)

    monkeypatch.setattr(relay, 'profile_menu', profile_menu)
    monkeypatch.setattr(relay, 'build_candidates', build_candidates)
    monkeypatch.setattr(relay, 'candidate_labels', lambda *a: (['candidate'], 'digest'))
    assert relay.prepare(root, receipt, official, date(2014, 12, 11), profile, candidate_dir, report) == (
        snapshot, candidate_dir / 'manifest.json')
    assert relay.prepare(root, receipt, official, date(2014, 12, 11), profile, candidate_dir, report)[0] == snapshot
    official.write_text('{"changed":true}')
    with pytest.raises(ValueError, match='exact inputs'):
        relay.prepare(root, receipt, official, date(2014, 12, 11), profile, candidate_dir, report)


def test_downstream_plan_only_follows_successful_capture(tmp_path, monkeypatch):
    root = tmp_path / 'root'
    root.mkdir()
    output = tmp_path / 'output'
    output.mkdir()
    receipt = tmp_path / 'large-receipt.json'
    receipt.write_text('{}')
    official = tmp_path / 'official.json'
    official.write_text('{}')
    day = date(2014, 12, 11)
    calls = []
    monkeypatch.setattr(relay, 'prepare', lambda *a: (tmp_path / 'snapshot', tmp_path / 'manifest'))

    def capture(*args):
        calls.append('capture')
        return 2

    monkeypatch.setattr(relay, 'capture_wait', capture)
    monkeypatch.setattr(relay, 'priority_wait', lambda *a: calls.append('priority') or 0)
    assert relay.run(root, receipt, tmp_path / 'run.json', 'a' * 64, official, day,
                     output, tmp_path / 'candidate', 0.01) == 2
    assert calls == ['capture']

    def successful_capture(*args):
        calls.append('capture')
        atomic_write_json(args[5], {'fixture': True})
        return 0

    monkeypatch.setattr(relay, 'capture_wait', successful_capture)
    assert relay.run(root, receipt, tmp_path / 'run.json', 'a' * 64, official, day,
                     output, tmp_path / 'candidate', 0.01) == 0
    assert calls == ['capture', 'capture', 'priority']


def test_ended_upstream_without_receipt_does_not_plan(tmp_path, monkeypatch):
    root = tmp_path / 'root'
    root.mkdir()
    output = tmp_path / 'output'
    output.mkdir()
    upstream = tmp_path / 'run.json'
    atomic_write_json(upstream, {'state': 'succeeded'})
    monkeypatch.setattr(relay, 'inspect', lambda *a: {'priority_active': True})
    monkeypatch.setattr(relay, 'prepare', lambda *a: pytest.fail('missing source cannot be planned'))
    assert relay.run(root, tmp_path / 'absent.json', upstream, 'a' * 64,
                     tmp_path / 'official', date(2014, 12, 11), output,
                     tmp_path / 'candidates', 0.01) == 1
