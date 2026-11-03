import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from downloader.artifact_io import sha256_file
from scripts import run_finmind_option_reference_boundaries as runner


def fake_plan(tmp_path):
    root = tmp_path / 'data_finmind' / 'complement'
    root.mkdir(parents=True)
    markets = tmp_path / 'artifacts' / 'markets'
    markets.mkdir(parents=True)
    history = tmp_path / 'data_finmind' / 'research' / 'history'
    history.mkdir(parents=True)
    policy = markets / 'policy.json'
    policy.write_text('{}')
    windows = [{'start': day, 'end': day, 'source_receipt_pins': [{'date': day}]}
               for day in ('2011-01-03', '2011-01-04', '2011-01-05')]
    plan = markets / 'plan.json'
    plan.write_text(json.dumps({'contract': 'finmind_option_reference_history_plan_v1',
                                'training_ready': False, 'policy_path': str(policy),
                                'policy_sha256': sha256_file(policy), 'windows': windows}))
    return root, markets, history, plan, windows


def make_parent(history, window):
    parent = history / f"{window['start']}_{window['end']}"
    parent.mkdir(parents=True)
    (parent / 'manifest.json').write_text('{}')


def test_waits_for_missing_parent_and_resumes_completed_boundaries(tmp_path, monkeypatch):
    root, markets, history, plan, windows = fake_plan(tmp_path)
    make_parent(history, windows[0])
    make_parent(history, windows[1])
    output = tmp_path / 'data_finmind' / 'research' / 'boundaries'
    monkeypatch.setattr(runner, 'capacity_check', lambda _: {})
    def fake_build(_root, _plan, _history, _day, directory):
        directory.mkdir(parents=True)
        (directory / 'manifest.json').write_text('{}')
    monkeypatch.setattr(runner, 'build', fake_build)
    monkeypatch.setattr(runner, 'verify', lambda directory, *_: {'manifest': str(directory / 'manifest.json')})
    first = runner.run(root, plan, history, output, markets / 'first.json', max_wait_seconds=1, poll_seconds=1)
    assert first['attempted_boundaries'] == 1
    assert first['results'][0]['state'] == 'built_verified'
    assert first['pending_parent_starts'] == ['2011-01-05']
    make_parent(history, windows[2])
    second = runner.run(root, plan, history, output, markets / 'second.json', max_wait_seconds=1, poll_seconds=1)
    assert [item['state'] for item in second['results']] == ['reused_verified', 'built_verified']
    assert not second['pending_parent_starts'] and second['training_ready'] is False


def test_reused_boundary_rechecks_parent_source_and_payload(tmp_path, monkeypatch):
    root, markets, history, plan_path, windows = fake_plan(tmp_path)
    windows = windows[:2]
    plan = json.loads(plan_path.read_bytes())
    plan['windows'] = windows
    source_pins, parents = [], []
    for window in windows:
        source = markets / f"source-{window['start']}"
        receipt = markets / f"receipt-{window['start']}"
        source.write_text(window['start'])
        receipt.write_text(window['end'])
        window['source_receipt_pins'][0].update(parquet_sha256=sha256_file(source),
                                                receipt_sha256=sha256_file(receipt))
        source_pins.append({'day': window['start'], 'path': str(source), 'sha256': sha256_file(source),
                            'receipt_path': str(receipt), 'receipt_sha256': sha256_file(receipt)})
        make_parent(history, window)
        manifest = history / f"{window['start']}_{window['end']}" / 'manifest.json'
        parents.append({'window': [window['start'], window['end']], 'manifest': str(manifest),
                        'manifest_sha256': sha256_file(manifest)})
    plan_path.write_text(json.dumps(plan))
    output = tmp_path / 'data_finmind' / 'research' / 'boundary'
    output.mkdir(parents=True)
    payload = output / 'observed_references.parquet'
    pq.write_table(pa.table({'value': [1]}), payload)
    implementation = Path(runner.__file__).resolve()
    doc = {'contract': runner.CONTRACT, 'training_ready': False, 'plan_sha256': sha256_file(plan_path),
           'parents': parents, 'data_path': str(payload), 'data_sha256': sha256_file(payload), 'rows': 1,
           'implementation_sha256': {str(implementation): sha256_file(implementation)},
           'policy_path': plan['policy_path'], 'policy_sha256': plan['policy_sha256'],
           'policy_linked_pins': {}, 'source_pins': source_pins, 'comparison': {}}
    (output / 'manifest.json').write_text(json.dumps(doc))
    monkeypatch.setattr(runner, 'check_planned_receipts', lambda *_: None)
    monkeypatch.setattr(runner, 'verify_window',
                        lambda directory, *_: {'manifest_sha256': sha256_file(directory / 'manifest.json')})
    assert runner.verify(output, plan_path, windows[1], root)['rows'] == 1
    Path(source_pins[0]['path']).write_text('changed')
    with pytest.raises(ValueError, match='source changed'):
        runner.verify(output, plan_path, windows[1], root)
