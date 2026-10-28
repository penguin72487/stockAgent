import json
import os
from pathlib import Path
import subprocess

import pytest

from scripts.audit_wsl_storage_usage import make_plan, parse_records, progress, scan, summarize


def test_one_du_census_counts_cross_root_hardlinks_once(tmp_path):
    root = tmp_path / 'wsl'
    (root / 'a' / 'sub').mkdir(parents=True)
    (root / 'b').mkdir()
    (root / 'a' / 'sub' / 'payload').write_bytes(b'x' * 8193)
    os.link(root / 'a' / 'sub' / 'payload', root / 'b' / 'linked')
    os.link(root / 'a' / 'sub' / 'payload', root / 'direct-link')
    (root / 'b' / 'unique').write_bytes(b'y' * 1700)
    (root / 'b' / 'symbolic').symlink_to(root / 'a', target_is_directory=True)
    output = tmp_path / 'receipt'

    assert scan(root, output, splits=('.', 'a'), mounts=[]) == 0
    result = json.loads((output / 'result.json').read_text())
    baseline = subprocess.run(['du', '-x', '-B1', '-s', '--', str(root)],
                              check=True, capture_output=True).stdout
    assert result['included_allocated_bytes'] == int(baseline.split(b'\t', 1)[0])
    assert result['state'] == 'complete'
    assert result['authorizes_retirement'] is False
    assert result['reclaimable_bytes'] is None
    assert result['completed_records'] + result['omitted_hardlink_operands'] == result['planned_roots']
    view = progress(output, 100, under=root, max_depth=1)
    assert view['selected_subtree_observation']['all_planned_operands_accounted'] is True
    if result['omitted_hardlink_operands']:
        assert view['selected_subtree_observation']['all_planned_operands_recorded'] is False
    assert view['incomplete_direct_children_count'] == 0


def test_changed_expanded_namespace_and_missing_operand_are_partial(tmp_path):
    root = tmp_path / 'wsl'
    root.mkdir()
    old = root / 'old'
    old.write_bytes(b'data')
    plan = make_plan(root, splits=('.',), mounts=[])
    (root / 'new').write_bytes(b'new')
    # A clean-looking subprocess cannot hide either new names or a missing operand.
    report = summarize(plan, b'', returncode=0)
    assert report['state'] == 'partial'
    assert str(root) in report['changed_expanded_directories']
    assert str(old) in report['unresolved_roots']


def test_progress_only_counts_complete_records_and_rejects_unknown_paths(tmp_path):
    root = tmp_path / 'wsl'
    root.mkdir()
    child = root / 'child'
    child.write_bytes(b'a')
    plan = make_plan(root, splits=('.',), mounts=[])
    full = b'4096\t' + os.fsencode(child) + b'\0'
    records, complete = parse_records(full + b'123\tunfinished', {str(child)})
    assert records == {str(child): 4096}
    assert not complete
    assert summarize(plan, full + b'123\tunfinished')['state'] == 'running'
    with pytest.raises(ValueError, match='unexpected'):
        parse_records(b'1\t/unknown\0', {str(child)})


def test_progress_marks_partially_scanned_directories(tmp_path):
    root = tmp_path / 'wsl'
    (root / 'group').mkdir(parents=True)
    one, two = root / 'group' / 'one', root / 'group' / 'two'
    one.write_bytes(b'a')
    two.write_bytes(b'b')
    plan = make_plan(root, splits=('.', 'group'), mounts=[])
    out = tmp_path / 'receipt'
    out.mkdir()
    (out / 'plan.json').write_text(json.dumps(plan))
    (out / 'records.nul').write_bytes(b'4096\t' + os.fsencode(one) + b'\0')
    view = progress(out, 10, under=root, max_depth=1)
    group = next(row for row in view['largest_observed_paths'] if row['path'] == str(root / 'group'))
    assert (group['recorded_roots'], group['planned_roots']) == (1, 2)
    assert group['all_planned_operands_recorded'] is False
    assert group['allocated_bytes'] >= 4096
    assert view['incomplete_direct_children'] == [{
        'path': str(root / 'group'), 'planned_roots': 2, 'recorded_roots': 1}]
    assert view['selected_subtree_observation']['all_planned_operands_recorded'] is False
    assert view['authorizes_retirement'] is False


def test_explicit_mount_exclusion_is_not_traversed(tmp_path):
    root = tmp_path / 'wsl'
    (root / 'keep').mkdir(parents=True)
    (root / 'mount' / 'nested').mkdir(parents=True)
    (root / 'keep' / 'x').write_bytes(b'keep')
    (root / 'mount' / 'nested' / 'large').write_bytes(b'x' * 20000)
    out = tmp_path / 'receipt'
    assert scan(root, out, splits=('.',), mounts=[root / 'mount']) == 0
    plan = json.loads((out / 'plan.json').read_text())
    result = json.loads((out / 'result.json').read_text())
    assert str(root / 'mount') in plan['excluded_mounts_or_devices']
    assert not any(Path(row['path']) == root / 'mount' for row in plan['roots'])
    baseline = subprocess.run(['du', '-x', '-B1', '-s', '--exclude=' + str(root / 'mount'),
                               '--', str(root)], check=True, capture_output=True).stdout
    assert result['included_allocated_bytes'] == int(baseline.split(b'\t', 1)[0])
    assert progress(out, 10)['state'] == 'complete'


def test_finmind_symbol_roots_do_not_starve_other_roots_or_double_count_links(tmp_path):
    root = tmp_path / 'wsl'
    symbols = root / 'root/stockAgent/data_finmind/complement/parquet/USStockPriceMinute'
    (symbols / 'symbol-a').mkdir(parents=True)
    (symbols / 'symbol-b').mkdir()
    payload = symbols / 'symbol-a' / 'minute.parquet'
    payload.write_bytes(b'x' * 8193)
    os.link(payload, symbols / 'symbol-b' / 'minute.parquet')
    service = root / 'srv' / 'service'
    service.mkdir(parents=True)
    os.link(payload, service / 'same-inode')
    (service / 'other').write_bytes(b'y' * 1200)

    out = tmp_path / 'receipt'
    assert scan(root, out, mounts=[]) == 0
    plan = json.loads((out / 'plan.json').read_text())
    paths = [row['path'] for row in plan['roots']]
    assert str(symbols / 'symbol-a') in paths and str(symbols / 'symbol-b') in paths
    assert paths.index(str(service)) < paths.index(str(symbols / 'symbol-a'))
    report = json.loads((out / 'result.json').read_text())
    baseline = subprocess.run(['du', '-x', '-B1', '-s', '--', str(root)],
                              check=True, capture_output=True).stdout
    assert report['included_allocated_bytes'] == int(baseline.split(b'\t', 1)[0])
    assert report['state'] == 'complete' and report['authorizes_retirement'] is False
