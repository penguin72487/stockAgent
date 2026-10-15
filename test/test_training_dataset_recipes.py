from datetime import date, datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

import polars as pl
import pytest

from stockagent.data_sync.desync_snapshots import SnapshotError, sha256_file
from stockagent.data_sync.source_validation import VerificationCache, audit_stock_minute_source
from stockagent.data_sync.minute_catalog_validation import validate_stock_minute_catalog
from stockagent.data_sync import training_recipes as recipes
from stockagent.data_sync.packed_snapshots import initialize_packed_layout, publish_packed_snapshot


def write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


def minute_source(root):
    """Small real parquet, real receipts and real bar validator, no API mock."""
    start = end = '2026-10-06'
    path = root/'minute_chunks/2330'/f'{start}_{end}.parquet'
    path.parent.mkdir(parents=True)
    pl.DataFrame({'date': [date(2026, 10, 6)], 'ts': [datetime(2026, 10, 6, 9, 1)],
                  'symbol': ['2330'], 'Open': [100.], 'High': [101.],
                  'Low': [99.], 'Close': [100.], 'Volume': [2.],
                  'Amount': [200000.], 'contract_unit': [1000.]}).write_parquet(path)
    receipt = {'schema_version': 1, 'source': 'shioaji_kbars_1m', 'storage_frequency': 'minute',
               'symbol': '2330', 'simulation': True, 'start_date': start, 'end_date': end,
               'status': 'ok', 'rows': 1, 'returned_dates': [start], 'source_gap_dates': [],
               'output_receipt': {'path': '/obsolete/producer.parquet', 'size': path.stat().st_size,
                                  'sha256': sha256_file(path)}}
    write_json(path.with_suffix('.receipt.json'), receipt)
    entry = {'start_date': start, 'end_date': end, 'status': 'ok', 'rows': 1,
             'data_sha256': sha256_file(path), 'data_path': '/obsolete/producer.parquet',
             'source_gap_dates': []}
    manifest = {'schema_version': 1, 'source': receipt['source'], 'storage_frequency': 'minute',
                'symbol': '2330', 'simulation': True, 'requested_start': start, 'requested_end': end,
                'minute_rows': 1, 'source_gap_dates': [], 'terminal_coverage_dates': [start],
                'chunks': [entry]}
    write_json(root/'symbols/2330.manifest.json', manifest)
    (root/'download_report.csv').write_text('symbol,status,source_minute_rows\n2330,complete,1\n')
    summary = {'source': receipt['source'], 'simulation': True, 'chunk_days': 29,
               'start_date': start, 'end_date': end, 'selected_symbols': 1, 'reported_symbols': 1,
               'resumable_collection_complete': True, 'failed_symbols': 0, 'partial_symbols': 0,
               'complete_symbols': 1, 'complete_with_source_gap_symbols': 0,
               'contract_unavailable_symbols': 0, 'selected_coverage_complete': True}
    write_json(root/'download_summary.json', summary)
    return path, receipt, manifest, summary


def test_native_cache_reuses_only_unchanged_bytes_and_validator(tmp_path):
    member = tmp_path/'member'
    member.write_bytes(b'1234')
    expected = sha256_file(member)
    cache = VerificationCache(tmp_path/'cache.sqlite3', 'contract-v1')
    assert cache.check(member, expected, lambda: {'value': 4}) == {'value': 4}
    assert cache.check(member, expected, lambda: pytest.fail('unchanged should reuse')) == {'value': 4}
    saved = member.stat()
    member.write_bytes(b'4321')
    os.utime(member, ns=(saved.st_atime_ns, saved.st_mtime_ns))
    with pytest.raises(SnapshotError, match='fingerprint'):
        cache.check(member, expected, lambda: {})
    assert (cache.checked, cache.reused) == (1, 1)
    cache.close()
    member.write_bytes(b'1234')
    cache = VerificationCache(tmp_path/'cache.sqlite3', 'contract-v2')
    assert cache.check(member, expected, lambda: {'value': 9}) == {'value': 9}
    assert cache.checked == 1 and cache.reused == 0
    cache.close()


def test_failed_semantics_never_become_a_cache_hit(tmp_path):
    member = tmp_path/'member'
    member.write_bytes(b'x')
    cache = VerificationCache(tmp_path/'cache.sqlite3', 'v1')
    with pytest.raises(ValueError):
        cache.check(member, sha256_file(member), lambda: (_ for _ in ()).throw(ValueError('bad')))
    assert cache.check(member, sha256_file(member), lambda: {'fixed': True}) == {'fixed': True}
    assert cache.checked == 1 and cache.reused == 0
    cache.close()


def test_real_bar_and_terminal_catalog_gate_is_incremental(tmp_path):
    source = tmp_path/'source'
    minute_source(source)
    first = audit_stock_minute_source(source, tmp_path/'bar.sqlite3')
    second = audit_stock_minute_source(source, tmp_path/'bar.sqlite3')
    assert first['rows'] == 1 and first['checked_files'] == 1
    assert second['reused_files'] == 1
    assert validate_stock_minute_catalog(source, tmp_path/'catalog.sqlite3')['receipts'] == 1


@pytest.mark.parametrize('defect', ['nonterminal', 'sha', 'receipt_count', 'range', 'terminal_dates'])
def test_stock_source_gates_reject_incomplete_or_disagreeing_evidence(tmp_path, defect):
    source = tmp_path/'source'
    path, receipt, manifest, summary = minute_source(source)
    if defect == 'nonterminal':
        summary['partial_symbols'] = 1
        write_json(source/'download_summary.json', summary)
    elif defect == 'sha':
        path.write_bytes(b'corrupt')
    elif defect == 'receipt_count':
        receipt['rows'] = 2
        write_json(path.with_suffix('.receipt.json'), receipt)
    elif defect == 'range':
        manifest['chunks'] = []
        write_json(source/'symbols/2330.manifest.json', manifest)
    else:
        manifest['terminal_coverage_dates'] = []
        write_json(source/'symbols/2330.manifest.json', manifest)
    with pytest.raises((SnapshotError, OSError, ValueError)):
        audit_stock_minute_source(source, tmp_path/'bar.sqlite3')
        validate_stock_minute_catalog(source, tmp_path/'catalog.sqlite3')


def test_tick_fallback_lineage_is_retained_and_verified_on_selected_release(tmp_path):
    source = tmp_path/'source'
    path, receipt, _, _ = minute_source(source)
    raw = b'observed-trade-evidence'
    expected = hashlib.sha256(raw).hexdigest()
    member = source/'raw_tick_recovery/2330'/f'2026-10-06.{expected}.parquet'
    member.parent.mkdir(parents=True)
    member.write_bytes(raw)
    receipt.update(underlying_data_method='observed_ticks_aggregated_to_right_labelled_1m',
                   raw_tick_sources=[{'path': '/wrong/producer.parquet', 'symbol': '2330',
                                     'session_date': '2026-10-06', 'sha256': expected, 'size': len(raw)}])
    write_json(path.with_suffix('.receipt.json'), receipt)
    assert validate_stock_minute_catalog(source, tmp_path/'ticks.sqlite3')['retained_tick_sources'] == 1
    member.write_bytes(b'corrupted-trade-evidence')
    with pytest.raises(SnapshotError):
        validate_stock_minute_catalog(source, tmp_path/'ticks.sqlite3')
    receipt['raw_tick_sources'] = []
    write_json(path.with_suffix('.receipt.json'), receipt)
    with pytest.raises(SnapshotError, match='no retained'):
        validate_stock_minute_catalog(source, tmp_path/'ticks.sqlite3')


def test_minute_builder_rebases_instead_of_reading_an_existing_producer(tmp_path):
    from scripts.build_shioaji_tw_minute_dataset import discover_chunk_groups
    source = tmp_path/'source'
    path, _, manifest, _ = minute_source(source)
    obsolete = tmp_path/'producer.parquet'
    obsolete.write_bytes(b'wrong mutable data')
    manifest['chunks'][0]['data_path'] = str(obsolete)
    write_json(source/'symbols/2330.manifest.json', manifest)
    groups, symbols = discover_chunk_groups(source)
    assert list(groups.values()) == [[path]] and symbols == ['2330']
    path.unlink()
    path.symlink_to(obsolete)
    with pytest.raises(SnapshotError, match='escapes'):
        discover_chunk_groups(source)


def test_native_tree_guard_rejects_mutation_with_unchanged_declared_manifest(tmp_path):
    from scripts.validate_training_sources import _identity, verify_unchanged
    source = tmp_path/'source'
    path, _, _, _ = minute_source(source)
    entry = {'source': str(source), 'validation_gate': {'kind': 'stock_minute_source'}}
    proof = {'identity': _identity(entry, tmp_path)}
    saved = path.stat()
    path.write_bytes(b'modified')
    os.utime(path, ns=(saved.st_atime_ns, saved.st_mtime_ns))
    with pytest.raises(SnapshotError, match='changed before publication'):
        verify_unchanged(entry, proof, repo_root=tmp_path)


def test_calendar_changes_invalidate_a_whole_derived_validation_proof(tmp_path):
    from scripts.validate_training_sources import _identity, verify_unchanged
    source, calendar = tmp_path/'derived', tmp_path/'calendar'
    source.mkdir()
    calendar.mkdir()
    write_json(source/'manifest.json', {'partitions': []})
    for name in ('twse_taiex_ohlc.parquet', 'twse_taiex_ohlc.summary.json'):
        (calendar/name).write_bytes(b'calendar-version-one')
    entry = {'source': str(source), 'validation_gate': {
        'kind': 'stock_minute_dataset', 'calendar_root': str(calendar)}}
    proof = {'identity': _identity(entry, tmp_path)}
    (calendar/'twse_taiex_ohlc.summary.json').write_bytes(b'calendar-version-two')
    with pytest.raises(SnapshotError, match='changed before publication'):
        verify_unchanged(entry, proof, repo_root=tmp_path)


def test_whole_raw_proof_reuse_requires_unchanged_source_and_gate(tmp_path, monkeypatch):
    from scripts import validate_training_sources as gates
    source = tmp_path/'source'
    minute_source(source)
    entry = {'dataset': 'minute-fixture', 'source': str(source),
             'validation_gate': {'kind': 'stock_minute_source'}}
    original, calls = gates.audit_stock_minute_source, []
    def observe(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)
    observe.__module__ = original.__module__  # Keep the actual implementation's code identity.
    monkeypatch.setattr(gates, 'audit_stock_minute_source', observe)
    first = gates.validate_catalog_source(entry, repo_root=tmp_path)
    assert gates.validate_catalog_source(entry, repo_root=tmp_path) == first
    assert len(calls) == 1
    write_json(source/'extra-retained-receipt.json', {'source': 'new-evidence'})
    second = gates.validate_catalog_source(entry, repo_root=tmp_path)
    assert first['identity'] != second['identity'] and len(calls) == 2


def recipe_fixture(tmp_path):
    repo = tmp_path/'repo'
    repo.mkdir()
    (repo/'train.py').write_text('# canonical training placeholder for this fixture\n')
    builder = repo/'scripts/build_trial.py'
    builder.parent.mkdir()
    builder.write_text('''import argparse, json
from pathlib import Path
p=argparse.ArgumentParser()
p.add_argument('--input')
p.add_argument('--output')
p.add_argument('--fail', action='store_true')
a=p.parse_args()
out=Path(a.output)
out.mkdir(parents=True)
(out/'manifest.json').write_text(json.dumps({'input':Path(a.input,'source.txt').read_text()}))
if a.fail: raise RuntimeError('trial deliberately failed')
''')
    source = tmp_path/'source'
    source.mkdir()
    (source/'source.txt').write_text('source-v1')
    cold = tmp_path/'cold'
    initialize_packed_layout(cold, node_id='test-node')
    release = publish_packed_snapshot(cold, 'source-package', source)
    templates = repo/'configs/data_sync/training_dataset_recipes.json'
    write_json(templates, {'schema_version': 1, 'contract': recipes.CONTRACT, 'recipes': [
        {'recipe': 'trial-method', 'inputs': {'source': 'source-package'}, 'parameters': [],
         'steps': [['scripts/build_trial.py', '--input', '{source}', '--output', '{output}']]}]})
    catalog = repo/'configs/data_sync/packed_datasets.json'
    write_json(catalog, {'datasets': [{'dataset': 'source-package', 'publish': True}]})
    output = tmp_path/'methods'
    return repo, source, cold, release, templates, output


def test_recipe_noop_and_revision_have_exact_version_boundaries(tmp_path):
    repo, source, cold, release, _, output = recipe_fixture(tmp_path)
    first = recipes.refresh_recipes(repo=repo, sync_root=cold, output_root=output)
    head = output/'heads/trial-method.json'
    first_mtime = head.stat().st_mtime_ns
    again = recipes.refresh_recipes(repo=repo, sync_root=cold, output_root=output)
    assert again['recipes'] == first['recipes'] and head.stat().st_mtime_ns == first_mtime
    (source/'source.txt').write_text('source-v2')
    publish_packed_snapshot(cold, 'source-package', source)
    new = recipes.refresh_recipes(repo=repo, sync_root=cold, output_root=output)
    assert new['recipes'][0]['recipe_id'] != first['recipes'][0]['recipe_id']
    old = json.loads((output/first['recipes'][0]['path']).read_text())
    assert old['inputs']['source']['snapshot_id'] == release.manifest['snapshot_id']
    assert len(list((output/'versions/trial-method').glob('*.json'))) == 2
    assert not new['stores_assembled_data'] and not new['all_history_complete_claim']


def test_recipe_waits_for_a_compatible_published_calendar(tmp_path):
    repo, source, cold, _, templates, output = recipe_fixture(tmp_path)
    value = json.loads(templates.read_text())
    value['recipes'][0]['inputs']['calendar'] = 'calendar-package'
    value['recipes'][0]['source_range_requirements'] = [{'input': 'calendar', 'at_least': 'source'}]
    write_json(templates, value)
    catalog = repo/'configs/data_sync/packed_datasets.json'
    write_json(catalog, {'datasets': [{'dataset': v, 'publish': True}
                                    for v in ('source-package', 'calendar-package')]})
    (source/'source.txt').write_text('10/6 new source')
    publish_packed_snapshot(cold, 'source-package', source, metadata={'freshness_value': '2026-10-06'})
    calendar = tmp_path/'calendar'
    calendar.mkdir()
    (calendar/'calendar.txt').write_text('10/5')
    publish_packed_snapshot(cold, 'calendar-package', calendar, metadata={'freshness_value': '2026-10-05'})
    result = recipes.refresh_recipes(repo=repo, sync_root=cold, output_root=output)
    assert result['recipes'][0]['state'] == 'waiting_compatible_source'
    assert not (output/'heads/trial-method.json').exists()
    (calendar/'calendar.txt').write_text('10/6')
    publish_packed_snapshot(cold, 'calendar-package', calendar, metadata={'freshness_value': '2026-10-06'})
    result = recipes.refresh_recipes(repo=repo, sync_root=cold, output_root=output)
    assert result['recipes'][0]['state'] == 'pinned_method_ready_not_training_acceptance'


@pytest.mark.parametrize('fail', [False, True])
def test_actual_trial_builder_cleanup_success_and_failure(tmp_path, fail):
    repo, source, cold, _, templates, output = recipe_fixture(tmp_path)
    if fail:
        value = json.loads(templates.read_text())
        value['recipes'][0]['steps'][0].append('--fail')
        write_json(templates, value)
    status = recipes.refresh_recipes(repo=repo, sync_root=cold, output_root=output)
    recipe = output/status['recipes'][0]['path']
    receipt = tmp_path/'trial-receipt.json'
    if fail:
        with pytest.raises(Exception, match='non-zero exit status'):
            recipes.build_recipe(recipe, repo=repo, sync_root=cold,
                input_paths={'source': str(source)}, parameters={}, receipt_path=receipt)
    else:
        result = recipes.build_recipe(recipe, repo=repo, sync_root=cold,
            input_paths={'source': str(source)}, parameters={}, receipt_path=receipt)
        assert result['state'] == 'verified_builder_completed'
    proof = json.loads(receipt.read_text())
    assert proof['trial_output_removed'] and not proof['assembled_data_retained']
    if not fail:
        out = Path(proof['steps'][0]['argv'][-1])
        assert not out.parent.exists()
    assert (source/'source.txt').read_text() == 'source-v1'


def test_changed_code_or_source_does_not_silently_rebuild_pinned_recipe(tmp_path):
    repo, source, cold, _, _, output = recipe_fixture(tmp_path)
    status = recipes.refresh_recipes(repo=repo, sync_root=cold, output_root=output)
    recipe = output/status['recipes'][0]['path']
    kwargs = dict(repo=repo, sync_root=cold, input_paths={'source': str(source)},
                  parameters={}, receipt_path=tmp_path/'receipt.json')
    (source/'source.txt').write_text('not-the-pinned-source')
    with pytest.raises(SnapshotError):
        recipes.build_recipe(recipe, **kwargs)
    (repo/'train.py').write_text('# changed shared code\n')
    with pytest.raises(SnapshotError, match='code changed'):
        recipes.build_recipe(recipe, **kwargs)


def test_explicit_hydration_uses_exact_version_not_newer_head(tmp_path):
    repo, source, cold, release, _, output = recipe_fixture(tmp_path)
    status = recipes.refresh_recipes(repo=repo, sync_root=cold, output_root=output)
    recipe = output/status['recipes'][0]['path']
    (source/'source.txt').write_text('newer-source-v2')
    publish_packed_snapshot(cold, 'source-package', source)
    inputs = recipes.materialize_recipe_inputs(recipe, repo=repo, sync_root=cold,
        materialized_root=tmp_path/'managed-inputs', edge_state=tmp_path/'absent-edge-state.json')
    assert Path(inputs['source']).name == release.manifest['snapshot_id']
    assert (Path(inputs['source'])/'source.txt').read_text() == 'source-v1'
    receipt = tmp_path/'trial-proof.json'
    result = recipes.build_recipe(recipe, repo=repo, sync_root=cold,
        input_paths=inputs, parameters={}, receipt_path=receipt)
    assert result['trial_output_removed'] and not result['assembled_data_retained']
    assert (source/'source.txt').read_text() == 'newer-source-v2'


def test_method_inventory_is_metadata_only_and_does_not_churn_unchanged_files(tmp_path):
    from scripts.inventory_training_dataset_recipes import inventory, declared_release_bindings
    repo, output = tmp_path/'repo', tmp_path/'methods'
    repo.mkdir()
    raw = repo/'data_okx/1m'
    raw.mkdir(parents=True)
    (raw/'broken_features.parquet').write_bytes(b'not even a parquet footer')
    first = inventory(repo, output, include_minutes=False)
    times = {p.name: p.stat().st_mtime_ns for p in output.iterdir()}
    assert inventory(repo, output, include_minutes=False) == first
    assert times == {p.name: p.stat().st_mtime_ns for p in output.iterdir()}
    assert first['minute_inventory_requested'] is False and first['minute_datasets'] == 0
    config = {'data': {'parquet_root': '/srv/stockagent-packed-materialized/tw-public/'
                     'tw-public-20261007T050725143636373Z-l0-penguin-example/stocks'}}
    rows = declared_release_bindings(config)
    assert len(rows) == 1 and rows[0]['dataset'] == 'tw-public'
    assert rows[0]['manifest_sha256_verified'] is False


def test_exported_cookbook_links_to_the_catalog_that_is_actually_shipped():
    from scripts.maintain_training_dataset_recipes import packaged_cookbook
    text = ('[組合](../artifacts/data_quality/audit-date/inventory/training_combinations.md)\n'
            '[逐項報告](minute_validation_and_recipes_2026-10-07.md#尚未放行的項目)\n'
            '[程式](../configs/data_sync/training_dataset_recipes.json)\n')
    value = packaged_cookbook(text)
    assert '(catalog/training_combinations.md)' in value
    assert 'artifacts/data_quality' not in value
    assert '(minute_validation_and_recipes_' not in value
    assert 'docs/minute_validation_and_recipes_2026-10-07.md' in value
    assert '[程式](../configs/data_sync/training_dataset_recipes.json)' in value


def test_tampered_recipe_is_rejected_before_any_hydration_or_builder(tmp_path):
    repo, source, cold, _, _, output = recipe_fixture(tmp_path)
    status = recipes.refresh_recipes(repo=repo, sync_root=cold, output_root=output)
    path = output/status['recipes'][0]['path']
    value = json.loads(path.read_text())
    value['template']['steps'][0].append('--unexpected-mutation')
    write_json(path, value)
    with pytest.raises(SnapshotError, match='immutable recipe id'):
        recipes.build_recipe(path, repo=repo, sync_root=cold, input_paths={'source':str(source)},
                             parameters={}, receipt_path=tmp_path/'must-not-run.json')
    assert not (tmp_path/'must-not-run.json').exists()


@pytest.mark.parametrize('wrapper', ['run_data_release.sh', 'run_training_dataset_recipes.sh'])
def test_publication_shell_keeps_parsed_body_across_in_place_rollout(tmp_path, wrapper):
    """A long child must not resume at offsets in a newly edited shell file."""
    repo = tmp_path/'repo'
    scripts = repo/'scripts'
    scripts.mkdir(parents=True)
    path = scripts/wrapper
    original = Path(__file__).resolve().parents[1]/'scripts'/wrapper
    path.write_bytes(original.read_bytes())
    (scripts/'runtime_env.sh').write_text(
        'run_fintech_python() { "$TEST_PYTHON" "$TEST_CHILD" "$@"; }\n')
    if wrapper == 'run_data_release.sh':
        (scripts/'run_training_dataset_recipes.sh').write_text(
            '#!/usr/bin/env bash\nprintf ok > "$TEST_POSTHOOK"\n')
    child = tmp_path/'child.py'
    child.write_text('''import os, time
from pathlib import Path
started, proceed, calls = [Path(os.environ[k]) for k in ('TEST_STARTED', 'TEST_PROCEED', 'TEST_CALLS')]
previous = int(calls.read_text()) if calls.exists() else 0
calls.write_text(str(previous+1))
if previous == 0:
    started.write_text('started')
    deadline = time.monotonic()+8
    while not proceed.exists():
        if time.monotonic() > deadline: raise RuntimeError('fixture release timed out')
        time.sleep(.01)
''')
    env = {**os.environ, 'TEST_PYTHON': sys.executable, 'TEST_CHILD': str(child),
           **{f'TEST_{key.upper()}': str(tmp_path/key)
              for key in ('started', 'proceed', 'calls', 'posthook')}}
    argv = ['bash', str(path)]
    if wrapper == 'run_data_release.sh':
        argv.extend(['publish', 'fixture-source'])
    argv.extend(['--sync-root', str(tmp_path/'cold')])
    process = subprocess.Popen(argv, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        deadline = time.monotonic()+5
        while not (tmp_path/'started').exists():
            if process.poll() is not None or time.monotonic() > deadline:
                pytest.fail('fixture child never started')
            time.sleep(.01)
        # Deliberately overwrite the exact script inode while the child runs.
        path.write_text('#!/usr/bin/env bash\nexit 91\n')
        (tmp_path/'proceed').touch()
        stdout, stderr = process.communicate(timeout=10)
        assert process.returncode == 0, (stdout, stderr)
        if wrapper == 'run_data_release.sh':
            assert (tmp_path/'posthook').read_text() == 'ok'
        else:
            assert (tmp_path/'calls').read_text() == '2'
    finally:
        if process.poll() is None:
            process.terminate()
            process.communicate(timeout=10)
