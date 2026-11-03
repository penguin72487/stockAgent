import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from downloader.artifact_io import sha256_file
from scripts import run_finmind_option_reference_history as history


def setup_plan(tmp_path):
    root = tmp_path / 'data_finmind' / 'complement'
    receipts = root / 'receipts' / 'TaiwanOptionTick' / 'all'
    receipts.mkdir(parents=True)
    markets = tmp_path / 'artifacts' / 'markets'
    markets.mkdir(parents=True)
    policy = markets / 'policy.json'
    policy.write_text('{}')
    summary = markets / 'inventory-summary.json'
    summary.write_text('{}')
    inventory = markets / 'inventory.csv'
    inventory.write_text('date\n')
    windows = []
    for day in ('2011-01-03', '2011-01-04'):
        source_sha = hashlib.sha256(day.encode()).hexdigest()
        receipt = receipts / f'{day}.json'
        receipt.write_text(json.dumps({'sha256': source_sha}))
        group = [{'date': day, 'prior_readback_status': 'complete_payload_sha_verified',
                  'receipt_sha256': sha256_file(receipt), 'parquet_sha256': source_sha,
                  'planned': True}]
        digest = hashlib.sha256(json.dumps(group, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        windows.append({'start': day, 'end': day, 'source_days': 1,
                        'source_receipt_pins': group, 'source_group_fingerprint': digest})
    plan = markets / 'plan.json'
    plan.write_text(json.dumps({'contract': history.PLAN_CONTRACT, 'training_ready': False,
        'policy_path': str(policy), 'policy_sha256': sha256_file(policy),
        'source_inventory_pins': [{'summary_path': str(summary), 'summary_sha256': sha256_file(summary),
                                   'inventory_path': str(inventory), 'inventory_sha256': sha256_file(inventory)}],
        'windows': windows}))
    return root, markets, plan, tmp_path / 'data_finmind' / 'research' / 'history', windows


def fake_builder(root, policy, dates, output):
    output.mkdir(parents=True)
    data = output / 'observed_references.parquet'
    pq.write_table(pa.table({'value': [1]}), data)
    source_pins = []
    for day in dates:
        receipt = root / 'receipts' / 'TaiwanOptionTick' / 'all' / f'{day.isoformat()}.json'
        source_pins.append({'day': day.isoformat(), 'receipt_sha256': sha256_file(receipt),
                            'sha256': json.loads(receipt.read_text())['sha256']})
    implementation = Path(history.__file__).resolve()
    (output / 'manifest.json').write_text(json.dumps({
        'contract': history.WINDOW_CONTRACT, 'training_ready': False,
        'policy_sha256': sha256_file(policy), 'source_dates': [x.isoformat() for x in dates],
        'source_pins': source_pins, 'implementation_sha256': {str(implementation): sha256_file(implementation)},
        'data_path': str(data.resolve()), 'data_sha256': sha256_file(data), 'rows': 1}))


def test_history_resume_verifies_outputs_and_rejects_corruption(tmp_path, monkeypatch):
    root, markets, plan, output, _ = setup_plan(tmp_path)
    monkeypatch.setattr(history, 'capacity_check', lambda _: {})
    monkeypatch.setattr(history, 'build', fake_builder)
    first = history.run(root, plan, output, markets / 'first.json')
    assert [r['state'] for r in first['results']] == ['built_verified', 'built_verified']
    second = history.run(root, plan, output, markets / 'second.json')
    assert [r['state'] for r in second['results']] == ['reused_verified', 'reused_verified']
    corrupt = output / '2011-01-03_2011-01-03' / 'observed_references.parquet'
    corrupt.write_bytes(b'corrupted')
    third = history.run(root, plan, output, markets / 'third.json')
    assert [r['state'] for r in third['results']] == ['failed', 'reused_verified']
    assert corrupt.read_bytes() == b'corrupted'
    assert third['training_ready'] is False


def test_changed_planned_receipt_is_not_processed(tmp_path, monkeypatch):
    root, markets, plan, output, _ = setup_plan(tmp_path)
    monkeypatch.setattr(history, 'capacity_check', lambda _: {})
    monkeypatch.setattr(history, 'build', fake_builder)
    (root / 'receipts' / 'TaiwanOptionTick' / 'all' / '2011-01-03.json').write_text('{}')
    result = history.run(root, plan, output, markets / 'result.json')
    assert [r['state'] for r in result['results']] == ['failed', 'built_verified']
    assert not (output / '2011-01-03_2011-01-03').exists()


def test_capacity_defers_remaining_windows(tmp_path, monkeypatch):
    root, markets, plan, output, _ = setup_plan(tmp_path)
    monkeypatch.setattr(history, 'capacity_check', lambda _: {})
    def deferred(*_):
        raise history.CapacityNotAdmitted({'state': 'deferred', 'reason': 'reserve'})
    monkeypatch.setattr(history, 'build', deferred)
    result = history.run(root, plan, output, markets / 'deferred.json')
    assert result['capacity_deferred'] is True
    assert result['attempted_windows'] == 1
    assert result['results'][0]['state'] == 'capacity_deferred'
