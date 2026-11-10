import fcntl
import hashlib
import json
import os
import subprocess
import threading
from pathlib import Path

import pytest

from scripts import run_workspace_source_staging as queue
from stockagent.data_sync import storage_progress as progress


@pytest.fixture
def setup_queue(tmp_path, monkeypatch):
    root = tmp_path / 'repo'
    source = root / 'data_archive'
    source.mkdir(parents=True)
    (source / 'marker.json').write_text('{}')
    workspace = tmp_path / 'workspace'
    (workspace / 'downloads').mkdir(parents=True)
    (workspace / 'migration').mkdir()
    (workspace / 'verify').mkdir()
    locks = tmp_path / 'locks'
    locks.mkdir()
    monkeypatch.setattr(queue, 'ROOT', root)
    item = {'name': 'archive-e-v1', 'source': 'data_archive', 'marker': 'marker.json',
            'budget_bytes': 1024**3, 'seed_hardlinks': True}
    return root, source, workspace, locks, item


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))


def test_config_rejects_nested_sources_and_unsafe_paths(tmp_path):
    path = tmp_path / 'queue.json'
    entry = {'name': 'first', 'source': 'data_a', 'marker': 'm.json',
             'budget_bytes': 1, 'seed_hardlinks': False}
    config = {'schema_version': 1, 'contract': queue.CONTRACT, 'sources': [entry]}
    write_json(path, config)
    assert queue.load_sources(path) == [entry]
    config['sources'].append({**entry, 'name': 'second', 'source': 'data_a/sub'})
    write_json(path, config)
    with pytest.raises(ValueError, match='scope'):
        queue.load_sources(path)
    config['sources'] = [{**entry, 'source': '../data_a'}]
    write_json(path, config)
    with pytest.raises(ValueError, match='normalized'):
        queue.load_sources(path)


def test_single_pass_mode_is_enrolled_only_for_the_named_minute_generation(tmp_path):
    path = tmp_path/'queue.json'
    item = {'name': 'tw-minute-train-stream-e-20261108-v1',
            'source': 'data_tw_minute/research_dataset', 'marker': 'manifest.json',
            'budget_bytes': 4*1024**3, 'seed_hardlinks': False,
            'copy_mode': 'source_stream_no_readback'}
    config = {'schema_version': 1, 'contract': queue.CONTRACT, 'sources': [item]}
    write_json(path, config)
    assert queue.load_sources(path) == [item]
    for update in ({'name': 'other'}, {'source': 'data_other'}, {'seed_hardlinks': True}):
        config['sources'] = [{**item, **update}]
        write_json(path, config)
        with pytest.raises(ValueError, match='scope'):
            queue.load_sources(path)


def test_bybit_stream_accepts_only_the_fixed_physical_root_and_catalog_alias(tmp_path):
    path = tmp_path/'queue.json'
    name, source, marker = queue.BYBIT_STREAM_SCOPE
    item = {'name':name, 'source':source, 'catalog_source':'data_bybit', 'marker':marker,
            'budget_bytes':64*1024**3, 'seed_hardlinks':False, 'copy_mode':'source_stream_no_readback'}
    config = {'schema_version':1, 'contract':queue.CONTRACT, 'sources':[item]}
    write_json(path, config)
    assert queue.load_sources(path) == [item]
    for change in ({'name':'other'}, {'source':'service_data/stockagent-live/other'},
                   {'marker':'different.json'}, {'catalog_source':'data_other'},
                   {'seed_hardlinks':True}, {'copy_mode':'other'}):
        config['sources'] = [{**item, **change}]; write_json(path, config)
        with pytest.raises(ValueError, match='scope'):
            queue.load_sources(path)


def test_bybit_staging_binds_alias_and_fences_original_owners(setup_queue, monkeypatch):
    root, original, workspace, locks, _ = setup_queue
    name, relative, marker = queue.BYBIT_STREAM_SCOPE
    physical = root/relative
    physical.parent.mkdir(parents=True)
    original.rename(physical)
    (physical/marker).write_text('{}')
    alias = root/'data_bybit'; alias.symlink_to(physical, target_is_directory=True)
    native = [physical/grain/'.download.lock' for grain in ('1m', 'daily')]
    for path in native:
        path.parent.mkdir(); path.touch()
    global_lock = root/'artifacts/daily_downloader/bybit_source_publish.lock'
    global_lock.parent.mkdir(parents=True); global_lock.touch()
    item = {'name':name, 'source':relative, 'catalog_source':'data_bybit', 'marker':marker,
            'budget_bytes':64*1024**3, 'seed_hardlinks':False, 'copy_mode':'source_stream_no_readback'}
    entry = {'dataset':'bybit', 'source':'data_bybit', 'active_process_substrings':[],
             'source_coordination_lock':'artifacts/daily_downloader/bybit_source_publish.lock'}
    write_json(root/'configs/data_sync/packed_datasets.json', {'datasets':[entry]})
    monkeypatch.setattr(queue.publisher, 'REPO_ROOT', root)
    monkeypatch.setattr(queue.publisher, '_running_commands', lambda:[])
    assert queue.catalog_entries(item) == [entry]
    def check_held(**_kwargs):
        for path in (*native, global_lock):
            with path.open('r+b') as other, pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        raise ValueError('Bybit original owners held')
    monkeypatch.setattr(queue.stage, 'main', check_held)
    with pytest.raises(ValueError, match='Bybit original owners held'):
        queue.run_source(item, workspace, locks)
    alias.unlink(); alias.symlink_to(root/'data_archive', target_is_directory=True)
    with pytest.raises((OSError, ValueError), match='Bybit|No such file'):
        queue.catalog_entries(item)


def test_bybit_staging_defers_for_independent_repair_writer(setup_queue, monkeypatch):
    name, relative, marker = queue.BYBIT_STREAM_SCOPE
    item = {'name':name, 'source':relative, 'catalog_source':'data_bybit', 'marker':marker}
    monkeypatch.setattr(queue.publisher, '_running_commands', lambda:[
        (123, '/usr/bin/python downloader/repair_bybit_1m_gaps.py')])
    with pytest.raises(RuntimeError, match='live owner'):
        queue.check_known_running_owners(item, entries=[])


def test_new_minute_source_generation_retains_only_the_fixed_prior_evidence(tmp_path):
    path = tmp_path/'queue.json'
    old = {'name': 'tw-minute-source-e-primary-20261104-v1',
           'source': 'data_tw_minute/shioaji_1m', 'marker': 'download_summary.json',
           'budget_bytes': 4*1024**3, 'seed_hardlinks': True}
    new = {**old, 'name': 'tw-minute-source-stream-e-20261108-v1',
           'seed_hardlinks': False, 'copy_mode': 'source_stream_no_readback'}
    config = {'schema_version': 1, 'contract': queue.CONTRACT, 'sources': [old, new]}
    write_json(path, config)
    assert queue.load_sources(path) == [old, new]
    config['sources'].append({**old, 'name': 'unrelated-generation'})
    write_json(path, config)
    with pytest.raises(ValueError, match='scope'):
        queue.load_sources(path)
    config['sources'] = [old, {**new, 'marker': 'another.json'}]
    write_json(path, config)
    with pytest.raises(ValueError, match='scope'):
        queue.load_sources(path)


def test_options_new_stream_generation_is_limited_to_its_exact_existing_source(tmp_path):
    path = tmp_path / 'queue.json'
    name, source, marker = queue.OPTIONS_STREAM_SCOPE
    old = {'name': 'tw-index-options-daily-e-primary-20261104-v1', 'source': source,
           'marker': marker, 'budget_bytes': 4096, 'seed_hardlinks': True}
    new = {**old, 'name': name, 'seed_hardlinks': False, 'copy_mode': 'source_stream_no_readback'}
    config = {'schema_version': 1, 'contract': queue.CONTRACT, 'sources': [old, new]}
    write_json(path, config)
    assert queue.load_sources(path) == [old, new]
    config['sources'] = [old, {**new, 'marker': 'another.json'}]
    write_json(path, config)
    with pytest.raises(ValueError, match='scope'):
        queue.load_sources(path)


def test_options_stream_waits_for_independent_settlement_writer(setup_queue, monkeypatch):
    name, source, marker = queue.OPTIONS_STREAM_SCOPE
    item = {'name': name, 'source': source, 'marker': marker}
    monkeypatch.setattr(queue.publisher, '_running_commands', lambda: [
        (123, '/usr/bin/python scripts/download_taifex_final_settlement_history.py')])
    with pytest.raises(RuntimeError, match='live owner'):
        queue.check_known_running_owners(item, entries=[])


def test_completed_raw_stream_is_automatically_enrolled_only_when_safe(monkeypatch):
    from stockagent.data_sync import workspace_source_handoff as handoff
    name = handoff.MINUTE_RAW_NAME
    calls = []
    def enroll(selected, *, apply):
        calls.append((selected, apply))
        return {'state': 'enrolled_for_existing_owner'}
    monkeypatch.setattr(handoff, 'registered_names', lambda: ())
    assert queue.auto_enroll_minute_raw([], start_allowed=lambda: True, enroll=enroll)['state'] == 'waiting'
    staged = [{'name': name, 'state': 'source_stream_staged'}]
    assert queue.auto_enroll_minute_raw(staged, start_allowed=lambda: False, enroll=enroll) == {
        'state': 'waiting', 'reason': 'tw_day_trade_priority_window'}
    assert not calls
    assert queue.auto_enroll_minute_raw(staged, start_allowed=lambda: True, enroll=enroll)['state'] == 'enrolled'
    assert calls == [(name, True)]
    monkeypatch.setattr(handoff, 'registered_names', lambda: (name,))
    assert queue.auto_enroll_minute_raw(staged, start_allowed=lambda: False, enroll=enroll)['state'] == 'enrolled'
    assert len(calls) == 1


def test_minute_auto_enrollment_records_busy_and_invalid_evidence(monkeypatch):
    from stockagent.data_sync import workspace_source_handoff as handoff
    monkeypatch.setattr(handoff, 'registered_names', lambda: ())
    rows = [{'name': handoff.MINUTE_RAW_NAME, 'state': 'source_stream_staged'}]
    def busy(*_args, **_kwargs):
        raise BlockingIOError('owner busy')
    assert queue.auto_enroll_minute_raw(rows, start_allowed=lambda: True, enroll=busy) == {
        'state': 'waiting', 'reason': 'original_owner_or_reader'}
    def invalid(*_args, **_kwargs):
        raise queue.SnapshotError('generation changed')
    result = queue.auto_enroll_minute_raw(rows, start_allowed=lambda: True, enroll=invalid)
    assert result['state'] == 'blocked' and result['reason'] == 'source_or_copy_recheck_required'


def test_bybit_auto_enrollment_uses_only_its_completed_stage_and_existing_owner(monkeypatch):
    from stockagent.data_sync import workspace_source_handoff as handoff

    calls = []
    def enroll(name, *, apply):
        calls.append((name, apply))
        return {'state': 'enrolled_for_existing_owner'}
    monkeypatch.setattr(handoff, 'registered_names', lambda: ())
    other = [{'name': handoff.MINUTE_RAW_NAME, 'state': 'source_stream_staged'}]
    assert queue.auto_enroll_bybit(other, start_allowed=lambda: True, enroll=enroll)['state'] == 'waiting'
    staged = [{'name': handoff.BYBIT_NAME, 'state': 'source_stream_staged'}]
    assert queue.auto_enroll_bybit(staged, start_allowed=lambda: False, enroll=enroll)['reason'] == 'tw_day_trade_priority_window'
    assert calls == []
    assert queue.auto_enroll_bybit(staged, start_allowed=lambda: True, enroll=enroll)['state'] == 'enrolled'
    assert calls == [(handoff.BYBIT_NAME, True)]


def test_offline_repair_is_a_single_fixed_scope_and_binds_its_receipt(tmp_path, monkeypatch):
    name, source, marker = queue.OFFLINE_REPAIR_SCOPE
    item = {'name': name, 'source': source, 'marker': marker,
            'budget_bytes': 96 * 1024**3, 'seed_hardlinks': False,
            'copy_mode': 'source_stream_no_readback'}
    path = tmp_path/'queue.json'
    config = {'schema_version': 1, 'contract': queue.CONTRACT, 'sources': [item]}
    write_json(path, config)
    assert queue.load_sources(path) == [item]
    for update in ({'name': 'other'}, {'source': 'service_data/live/tw_day_trade_simulation'},
                   {'marker': 'other.json'}, {'seed_hardlinks': True}, {'copy_mode': 'other'}):
        config['sources'] = [{**item, **update}]
        write_json(path, config)
        with pytest.raises(ValueError, match='scope'):
            queue.load_sources(path)
    receipt = tmp_path/'receipt.json'
    write_json(receipt, {'candidate_directory': str(tmp_path/source),
                         'status': 'candidate_built_not_promoted', 'publication': 'not_attempted',
                         'simulation_only': True})
    monkeypatch.setattr(queue, 'ROOT', tmp_path)
    monkeypatch.setattr(queue, 'OFFLINE_REPAIR_RECEIPT', receipt)
    monkeypatch.setattr(queue, 'OFFLINE_REPAIR_RECEIPT_SHA256', hashlib.sha256(receipt.read_bytes()).hexdigest())
    queue.check_offline_repair_provenance(item)
    receipt.write_text('{}')
    with pytest.raises(ValueError, match='receipt identity'):
        queue.check_offline_repair_provenance(item)


def test_close_rollback_requires_both_fixed_receipts_and_exact_scope(tmp_path, monkeypatch):
    name, source, marker = queue.OFFLINE_ROLLBACK_SCOPE
    item = {'name': name, 'source': source, 'marker': marker,
            'budget_bytes': 64*1024**3, 'seed_hardlinks': False,
            'copy_mode': 'source_stream_no_readback'}
    config_path = tmp_path/'queue.json'
    write_json(config_path, {'schema_version': 1, 'contract': queue.CONTRACT, 'sources': [item]})
    assert queue.load_sources(config_path) == [item]
    monkeypatch.setattr(queue, 'ROOT', tmp_path)
    value = {'contract': 'user_authorized_official_close_paper_settlement_v1', 'status': 'applied',
             'simulation_only': True, 'production_order_possible': False,
             'operation_id': '2026-10-29:1b2d5096974bb6ab',
             'rollback_directory': str(tmp_path/'artifacts/live/official-close-settlement-kb2xdiof/ledger')}
    historic = tmp_path/'service_data/live/tw_day_trade_simulation/settlement_receipts/2026-10-29-1b2d5096974bb6ab.json'
    own = tmp_path/source/marker
    write_json(historic, value); write_json(own, value)
    monkeypatch.setattr(queue, 'OFFLINE_ROLLBACK_RECEIPT_SHA256', hashlib.sha256(historic.read_bytes()).hexdigest())
    queue.check_offline_repair_provenance(item)
    own.write_text('{}')
    with pytest.raises(ValueError, match='receipt identity'):
        queue.check_offline_repair_provenance(item)
    write_json(own, value)
    historic.write_text('{}')
    with pytest.raises(ValueError, match='receipt identity'):
        queue.check_offline_repair_provenance(item)
    for changed in ({'source': 'service_data/live/tw_day_trade_simulation'}, {'marker': 'other.json'}, {'name': 'other'}):
        write_json(config_path, {'schema_version': 1, 'contract': queue.CONTRACT, 'sources': [{**item, **changed}]})
        with pytest.raises(ValueError, match='scope'):
            queue.load_sources(config_path)


def test_single_pass_stage_reports_source_hash_without_destination_readback(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from scripts import stage_futures_e_working_copy as stage
    from stockagent.data_sync import workspace_relocation as relocation

    repo = tmp_path/'repo'
    source = repo/'data_tw_minute/research_dataset'
    source.mkdir(parents=True)
    (source/'manifest.json').write_text('{"generation":1}')
    (source/'data.parquet').write_bytes(b'minute-source')
    workspace = tmp_path/'workspace'
    for name in ('downloads', 'migration', 'verify'):
        (workspace/name).mkdir(parents=True)
    control = tmp_path/'control'
    control.mkdir()
    item = {'name': 'tw-minute-train-stream-e-20261108-v1',
            'source': 'data_tw_minute/research_dataset', 'marker': 'manifest.json',
            'budget_bytes': 4*1024**3, 'seed_hardlinks': False,
            'copy_mode': 'source_stream_no_readback'}
    monkeypatch.setattr(queue, 'ROOT', repo)
    monkeypatch.setattr(stage, 'ROOT', repo)
    monkeypatch.setattr(stage.volume, 'ENROLLMENT', control/'enrollment.json')
    monkeypatch.setattr(stage.volume, 'configuration', lambda: {'mount_point': str(workspace), 'minimum_free_bytes': 0})
    monkeypatch.setattr(stage.volume, 'path', lambda name: workspace/name)
    monkeypatch.setattr(stage.volume, 'check', lambda: {})
    monkeypatch.setattr(stage.volume, 'admit', lambda _bytes: {})
    monkeypatch.setattr(stage, 'process_references_many', lambda *_a, **_k: [])
    from stockagent.data_sync import workspace_source_handoff
    monkeypatch.setattr(workspace_source_handoff, 'binance_handoff_window_allowed', lambda **_kwargs: True)
    @contextmanager
    def lease(*_a, **_k):
        yield {'other_reserved_growth_bytes': 0}
    monkeypatch.setattr(stage, 'reserve_capacity', lease)
    original_digest = relocation.digest
    def source_only(path, **kwargs):
        path = Path(path)
        assert path.is_relative_to(source) or path == evidence/'copy-result.json', 'destination payload must not be reread'
        return original_digest(path, **kwargs)
    monkeypatch.setattr(relocation, 'digest', source_only)
    _source, destination, evidence, report = queue.paths(item, workspace)
    stage.main(source=source, name=item['name'], marker=Path(item['marker']), report=report,
               budget=item['budget_bytes'], source_stream_no_readback=True)
    result = queue.verified_report(item, workspace)
    assert result['state'] == 'source_stream_staged_without_destination_readback'
    assert result['destination_independent_readback'] is False
    assert json.loads((evidence/'copy-result.json').read_text())['source_sha256_streamed'] is True
    assert (destination/'data.parquet').read_bytes() == b'minute-source'
    assert (source/'data.parquet').read_bytes() == b'minute-source'
    assert queue.run_source(item, workspace, control)['proof_scope'] == 'source_stream_sha256_no_destination_readback'
    config = repo/'queue.json'
    write_json(config, {'schema_version': 1, 'contract': queue.CONTRACT, 'sources': [item]})
    monkeypatch.setattr(progress, '_staging_e_io', lambda *_a: None)
    row, = progress.workspace_source_staging_tasks(repo, config, workspace)
    assert row['phase'] == 'workspace_source_stream_staged' and row['complete_evidence'] is True
    assert row['completed_items'] == 2 and row['total_items'] == 2


def test_catalogued_subtree_requires_covering_parent_and_no_exclusions(setup_queue):
    root, _, _, _, item = setup_queue
    catalog = root / 'configs/data_sync/packed_datasets.json'
    entry = {'dataset': 'archive', 'source': 'data_archive', 'excluded_subtrees': ['other']}
    write_json(catalog, {'datasets': [entry]})
    item.update(source='data_archive/prepared', catalog_source='data_archive', preserve_symlinks=True)
    assert queue.catalog_entries(item) == [entry]
    item['source'] = 'data_archive/other/child'
    with pytest.raises(ValueError, match='exclusion'):
        queue.catalog_entries(item)
    item['source'] = 'data_unrelated/prepared'
    with pytest.raises(ValueError, match='outside'):
        queue.catalog_entries(item)
    item['source'] = 'data_archive/prepared'
    write_json(catalog, {'datasets': [{**entry, 'included_root_files': ['only.json']}]})
    with pytest.raises(ValueError, match='does not cover'):
        queue.catalog_entries(item)


def test_staging_whole_root_fences_separately_catalogued_child_owner(setup_queue):
    root, _, _, _, item = setup_queue
    parent = {'dataset': 'parent', 'source': 'data_archive'}
    child = {'dataset': 'child', 'source': 'data_archive/private',
             'source_coordination_lock': 'data_archive/private/writer.lock'}
    other = {'dataset': 'other', 'source': 'data_elsewhere'}
    write_json(root / 'configs/data_sync/packed_datasets.json', {'datasets': [parent, child, other]})
    assert queue.catalog_entries(item) == [parent, child]


def test_interrupted_symlink_inventory_is_preserved_before_retry(setup_queue):
    _, source, workspace, locks, item = setup_queue
    item['preserve_symlinks'] = True
    _, destination, evidence, _ = queue.paths(item, workspace)
    write_json(evidence / 'intent.json', {'contract': 'workspace_scoped_relocation_v1', 'state': 'indexing',
        'preserve_symlinks': True, 'excluded_subtrees': [],
        'source': str(source), 'destination': str(destination)})
    assert queue.preserve_interrupted_index(item, workspace, locks)
    assert len(list((workspace/'migration').glob('archive-e-v1.interrupted-index-*'))) == 1
    assert source.is_dir() and not destination.exists()


def test_process_argument_paths_detect_active_writers_without_open_files():
    source = Path('/root/stockAgent/data_tw_index_derivatives_ticks')
    cwd = Path('/root/stockAgent')
    assert queue._argv_mentions_source(source, cwd,
        'python -m downloader.capture --output-dir data_tw_index_derivatives_ticks/shioaji_fop_captures')
    assert queue._argv_mentions_source(source, cwd,
        'python worker.py --output-dir=/root/stockAgent/data_tw_index_derivatives_ticks/raw')
    assert not queue._argv_mentions_source(source, cwd,
        'python worker.py --output-dir data_tw_index_derivatives_ticks_old/raw')
    assert not queue._argv_mentions_source(source, Path('/tmp'),
        'python worker.py --output-dir data_tw_index_derivatives_ticks/raw')
    with pytest.raises(ValueError, match='parse'):
        queue._argv_mentions_source(source, cwd, 'python "unterminated')


def test_binance_public_archive_staging_uses_the_collector_root_lock():
    entry = {'dataset': 'binance-public-archive', 'source': 'data_binance_archive'}
    owners = queue.original_source_owners(entry)
    assert any(owner.get('source_coordination_lock') == '.download.lock'
               and owner.get('source_coordination_lock_scope') == 'source_parent'
               and owner.get('source_coordination_lock_mode') == 'exclusive'
               for owner in owners)


def test_only_registered_fop_settlement_reader_can_share_source(setup_queue):
    root, _, _, _, _ = setup_queue
    source = root / 'data_tw_index_options_daily'
    cgroup = '0::/system.slice/stockagent-shioaji-taifex-bidask.service\n'
    command = ('/usr/bin/python -m downloader.stream_shioaji_taifex_bidask '
               '--final-settlement-path data_tw_index_options_daily/txo_final_settlement_history.parquet '
               '--output-dir data_tw_index_derivatives_ticks/captures')
    assert queue._argv_mentions_source(source, root, command)
    assert queue._known_option_settlement_reader(source, root, command, cgroup)
    assert not queue._known_option_settlement_reader(source, root, command, '0::/untrusted.service')
    assert not queue._known_option_settlement_reader(source, root, command.replace('python', 'sh'), cgroup)
    assert not queue._known_option_settlement_reader(source, root, command.replace(
        'txo_final_settlement_history.parquet', 'other.parquet'), cgroup)
    assert not queue._known_option_settlement_reader(source, root, command.replace(
        'data_tw_index_derivatives_ticks/captures', 'data_tw_index_options_daily/raw'), cgroup)
    assert not queue._known_option_settlement_reader(source, root, command + ' --unknown data_tw_index_options_daily', cgroup)


def test_new_copy_waits_for_catalogued_live_owner(setup_queue, monkeypatch):
    root, source, workspace, locks, item = setup_queue
    catalog = root / 'configs/data_sync/packed_datasets.json'
    write_json(catalog, {'datasets': [{'source': 'data_archive',
                                       'active_process_substrings': ['download_archive.py']}]})
    monkeypatch.setattr(queue.publisher, '_running_commands',
                        lambda: [(12345, 'python scripts/download_archive.py')])
    monkeypatch.setattr(queue.stage, 'main', lambda **kwargs: pytest.fail('live writer must defer staging'))
    with pytest.raises(RuntimeError, match='live owner'):
        queue.run_source(item, workspace, locks)
    assert not (workspace / 'migration' / item['name']).exists()


def test_new_copy_waits_for_process_with_source_as_argument(setup_queue, monkeypatch):
    root, source, workspace, locks, item = setup_queue
    write_json(root / 'configs/data_sync/packed_datasets.json', {
        'datasets': [{'source': 'data_archive', 'active_process_substrings': []}]})
    monkeypatch.setattr(queue.publisher, '_running_commands',
                        lambda: [(os.getpid(), f'python -m streamer --output-dir={source}/capture')])
    monkeypatch.setattr(queue.stage, 'main', lambda **kwargs: pytest.fail('live source must defer staging'))
    with pytest.raises(RuntimeError, match='live owner'):
        queue.run_source(item, workspace, locks)


def test_catalog_source_lock_is_held_through_staging_and_released(setup_queue, monkeypatch):
    root, source, workspace, locks, item = setup_queue
    lock = source / 'state' / 'collector.lock'
    lock.parent.mkdir()
    lock.touch()
    entry = {'dataset': 'archive', 'source': 'data_archive',
             'source_coordination_lock': 'data_archive/state/collector.lock',
             'active_process_substrings': []}
    write_json(root / 'configs/data_sync/packed_datasets.json', {'datasets': [entry]})
    monkeypatch.setattr(queue.publisher, 'REPO_ROOT', root)
    monkeypatch.setattr(queue.publisher, '_running_commands', lambda: [])

    def stage_under_original_lock(**kwargs):
        with lock.open('r+b') as other:
            with pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        raise ValueError('test completed while locked')

    monkeypatch.setattr(queue.stage, 'main', stage_under_original_lock)
    with pytest.raises(ValueError, match='completed while locked'):
        queue.run_source(item, workspace, locks)
    with lock.open('r+b') as other:
        fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)


def test_child_catalog_lock_is_held_when_parent_is_staged(setup_queue, monkeypatch):
    root, source, workspace, locks, item = setup_queue
    lock = source / 'nested' / 'writer.lock'
    lock.parent.mkdir(); lock.touch()
    write_json(root / 'configs/data_sync/packed_datasets.json', {'datasets': [
        {'dataset': 'parent', 'source': 'data_archive', 'active_process_substrings': []},
        {'dataset': 'nested', 'source': 'data_archive/nested',
         'source_coordination_lock': 'data_archive/nested/writer.lock', 'active_process_substrings': []}]})
    monkeypatch.setattr(queue.publisher, 'REPO_ROOT', root)
    monkeypatch.setattr(queue.publisher, '_running_commands', lambda: [])
    def verify_original_child_lock(**kwargs):
        with lock.open('r+b') as other, pytest.raises(BlockingIOError):
            fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        raise ValueError('verified child owner fence')
    monkeypatch.setattr(queue.stage, 'main', verify_original_child_lock)
    with pytest.raises(ValueError, match='verified child owner fence'):
        queue.run_source(item, workspace, locks)


def test_catalog_source_lock_busy_defers_staging(setup_queue, monkeypatch):
    root, source, workspace, locks, item = setup_queue
    lock = source / 'state' / 'collector.lock'
    lock.parent.mkdir()
    lock.touch()
    write_json(root / 'configs/data_sync/packed_datasets.json', {'datasets': [{
        'dataset': 'archive', 'source': 'data_archive',
        'source_coordination_lock': 'data_archive/state/collector.lock',
        'active_process_substrings': []}]})
    monkeypatch.setattr(queue.publisher, 'REPO_ROOT', root)
    monkeypatch.setattr(queue.stage, 'main', lambda **kwargs: pytest.fail('must not copy while locked'))
    with lock.open('r+b') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(queue.publisher.SourceCoordinationLockTimeout) as error:
            queue.run_source(item, workspace, locks)
    assert queue.classify_error(error.value) == ('waiting', 'original_owner_or_reader')


@pytest.mark.parametrize('busy', [False, True])
def test_native_cex_grain_locks_protect_the_entire_stage(setup_queue, monkeypatch, busy):
    root, source, workspace, locks, item = setup_queue
    cex = root / 'data_binance'
    source.rename(cex)
    item = {**item, 'source': 'data_binance'}
    native = [cex / grain / '.download.lock' for grain in ('1m', 'daily')]
    for path in native:
        path.parent.mkdir()
        path.touch()
    entry = {'dataset': 'binance', 'source': 'data_binance', 'active_process_substrings': []}
    # Two catalog rows must not make a process contend with its own flock.
    write_json(root / 'configs/data_sync/packed_datasets.json', {'datasets': [entry, dict(entry)]})
    monkeypatch.setattr(queue.publisher, 'REPO_ROOT', root)
    monkeypatch.setattr(queue.publisher, '_running_commands', lambda: [])

    def check_original_locks(**kwargs):
        for path in native:
            with path.open('r+b') as other:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        raise ValueError('both native grain owners held')

    monkeypatch.setattr(queue.stage, 'main', check_original_locks)
    if busy:
        with native[-1].open('r+b') as owner:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with pytest.raises(queue.publisher.SourceCoordinationLockTimeout):
                queue.run_source(item, workspace, locks)
        # The partially acquired first lock was released on the failed try.
        with native[0].open('r+b') as other:
            fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
    else:
        with pytest.raises(ValueError, match='both native grain owners held'):
            queue.run_source(item, workspace, locks)


def test_reviewed_collector_root_lock_protects_stage(setup_queue, monkeypatch):
    root, source, workspace, locks, item = setup_queue
    actual = root / 'data_dune_crypto'
    source.rename(actual)
    item = {**item, 'source': actual.name}
    lock = actual / '.download.lock'
    lock.touch()
    entry = {'dataset': 'dune-crypto', 'source': actual.name, 'active_process_substrings': []}
    write_json(root / 'configs/data_sync/packed_datasets.json', {'datasets': [entry]})
    monkeypatch.setattr(queue.publisher, 'REPO_ROOT', root)
    monkeypatch.setattr(queue.publisher, '_running_commands', lambda: [])

    def check_collector_lock(**kwargs):
        with lock.open('r+b') as other:
            with pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        raise ValueError('collector lock held')

    monkeypatch.setattr(queue.stage, 'main', check_collector_lock)
    with pytest.raises(ValueError, match='collector lock held'):
        queue.run_source(item, workspace, locks)
    # The same dataset label cannot extend the reviewed lock to another root.
    assert len(queue.original_source_owners({**entry, 'source': 'data_unrelated'})) == 1


@pytest.mark.parametrize('busy', [False, True])
def test_economic_stage_holds_all_original_provider_locks(setup_queue, monkeypatch, busy):
    root, source, workspace, locks, item = setup_queue
    actual = root / 'data_public_economic'
    source.rename(actual)
    item['source'] = 'data_public_economic'
    native = [actual / f'.{provider}.lock' for provider in ('bea', 'census', 'moi')]
    for path in native:
        path.touch()
    entry = {'dataset': 'public-economic-history', 'source': item['source']}
    monkeypatch.setattr(queue, 'catalog_entries', lambda _: [entry])
    monkeypatch.setattr(queue, 'check_known_running_owners', lambda *_: None)
    monkeypatch.setattr(queue.publisher, 'REPO_ROOT', root)

    def check_original_locks(**_kwargs):
        for path in native:
            with path.open('r+b') as other, pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        raise ValueError('all economic provider locks held')

    monkeypatch.setattr(queue.stage, 'main', check_original_locks)
    if busy:
        with native[-1].open('r+b') as owner:
            fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with pytest.raises(queue.publisher.SourceCoordinationLockTimeout):
                queue.run_source(item, workspace, locks)
        with native[0].open('r+b') as other:
            fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
    else:
        with pytest.raises(ValueError, match='all economic provider locks held'):
            queue.run_source(item, workspace, locks)
    assert len(queue.original_source_owners({**entry, 'source': 'data_unrelated'})) == 1


def test_option_stage_holds_existing_auxiliary_writer_lock(setup_queue, monkeypatch):
    root, source, workspace, locks, item = setup_queue
    actual = root / 'data_tw_index_options_daily'
    source.rename(actual)
    item['source'] = 'data_tw_index_options_daily'
    state = actual / 'state'
    state.mkdir()
    native = state / 'registered_auxiliary_daily.lock'
    native.touch()
    entry = {'dataset': 'tw-index-options-daily', 'source': item['source']}
    monkeypatch.setattr(queue, 'catalog_entries', lambda _: [entry])
    monkeypatch.setattr(queue, 'check_known_running_owners', lambda *_: None)
    monkeypatch.setattr(queue.publisher, 'REPO_ROOT', root)

    def check_native_lock(**_kwargs):
        with native.open('r+b') as other, pytest.raises(BlockingIOError):
            fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        raise ValueError('option source writer lock held')

    monkeypatch.setattr(queue.stage, 'main', check_native_lock)
    with pytest.raises(ValueError, match='option source writer lock held'):
        queue.run_source(item, workspace, locks)
    with native.open('r+b') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(queue.publisher.SourceCoordinationLockTimeout):
            queue.run_source(item, workspace, locks)


def test_verified_copy_is_not_repeated_or_mistaken_for_current_integrity(setup_queue, monkeypatch):
    _, source, workspace, locks, item = setup_queue
    _, destination, evidence, report = queue.paths(item, workspace)
    destination.mkdir(parents=True)
    copy = {'state': 'exact_destination_readback_verified',
            'all_source_sha256_and_destination_readback_verified': True,
            'source_files_deleted': 0, 'files': 2, 'logical_bytes': 17}
    copy_path = evidence / 'copy-result.json'
    write_json(copy_path, copy)
    report_value = {'state': 'verified_temporary_e_source_copy', 'source': str(source),
                    'e_destination': str(destination), 'relocation_evidence': str(copy_path),
                    'relocation_evidence_sha256': hashlib.sha256(copy_path.read_bytes()).hexdigest(),
                    'source_files_deleted': 0, 'source_alias_changed': False,
                    'files': 2, 'logical_bytes': 17, 'observed_at_epoch': 100.0}
    write_json(report, report_value)
    monkeypatch.setattr(queue.stage, 'main', lambda **kwargs: pytest.fail('must reuse the original copy proof'))
    result = queue.run_source(item, workspace, locks)
    assert result['state'] == 'copy_verified'
    assert result['proof_observed_at_epoch'] == 100.0
    assert result['proof_scope'] == 'historical_complete_readback_not_current_integrity'
    copy_path.write_text('{}')
    with pytest.raises(ValueError, match='copy receipt'):
        queue.run_source(item, workspace, locks)


def test_interrupted_inventory_is_preserved_and_replanned_only_before_copy(setup_queue):
    _, source, workspace, locks, item = setup_queue
    _, destination, evidence, _ = queue.paths(item, workspace)
    write_json(evidence / 'intent.json', {'contract': 'workspace_exact_relocation_v1', 'state': 'indexing',
                                         'source': str(source), 'destination': str(destination)})
    (evidence / 'files.sqlite').write_bytes(b'partial')
    assert queue.preserve_interrupted_index(item, workspace, locks)
    previous, = (workspace / 'migration').glob('archive-e-v1.interrupted-index-*')
    assert (previous / 'files.sqlite').read_bytes() == b'partial'
    assert source.is_dir() and not destination.exists() and not evidence.exists()


def test_never_replans_a_live_or_payload_bearing_inventory(setup_queue):
    _, source, workspace, locks, item = setup_queue
    _, destination, evidence, _ = queue.paths(item, workspace)
    write_json(evidence / 'intent.json', {'contract': 'workspace_exact_relocation_v1', 'state': 'indexing',
                                         'source': str(source), 'destination': str(destination)})
    lock = locks / (item['name'] + '.lock')
    with lock.open('a') as active:
        fcntl.flock(active, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(BlockingIOError):
            queue.preserve_interrupted_index(item, workspace, locks)
    assert evidence.exists()
    destination.mkdir(parents=True)
    with pytest.raises(ValueError, match='safely replanned'):
        queue.preserve_interrupted_index(item, workspace, locks)
    assert evidence.exists() and destination.exists()


def test_indexed_copy_reuses_existing_journal(setup_queue):
    _, source, workspace, locks, item = setup_queue
    _, destination, evidence, _ = queue.paths(item, workspace)
    write_json(evidence / 'intent.json', {'contract': 'workspace_exact_relocation_v1', 'state': 'indexed',
                                         'source': str(source), 'destination': str(destination)})
    destination.mkdir(parents=True)
    assert not queue.preserve_interrupted_index(item, workspace, locks)
    assert evidence.is_dir() and destination.is_dir()


@pytest.mark.parametrize(('error', 'expected'), [
    (BlockingIOError(), ('waiting', 'original_owner_or_reader')),
    (BlockingIOError('tw_day_trade_priority_window'), ('waiting', 'tw_day_trade_priority_window')),
    (queue.CapacityDeferred('filesystem_headroom_exhausted'), ('waiting', 'workspace_capacity_or_reservation')),
    (queue.CapacityDeferred('untrusted_reservation'), ('blocked', 'workspace_reservation_evidence_invalid')),
    (queue.SnapshotError('E workspace lacks physical/guest verification headroom'), ('waiting', 'workspace_capacity')),
    (queue.SnapshotError('native E workspace operation failed: WSL ERROR: UtilAcceptVsock: accept4 failed 110'),
     ('waiting', 'workspace_native_bridge_unavailable')),
    (subprocess.TimeoutExpired([queue.volume.POWERSHELL, '-NoProfile'], 60),
     ('waiting', 'workspace_native_bridge_unavailable')),
    (subprocess.TimeoutExpired(['rsync', '--archive'], 21600), ('blocked', 'source_or_copy_recheck_required')),
    (queue.SnapshotError('E volume does not match its pinned NVMe identity'),
     ('blocked', 'workspace_identity_or_source_invalid')),
    (queue.SnapshotError('E workspace filesystem UUID differs'), ('blocked', 'workspace_identity_or_source_invalid')),
    (RuntimeError('the source has a live owner; wait without changing it'), ('waiting', 'original_owner_or_reader')),
    (ValueError('source generation changed'), ('blocked', 'source_or_copy_recheck_required')),
])
def test_classifies_retryable_waits_separately_from_invalid_evidence(error, expected):
    assert queue.classify_error(error) == expected


def test_monitor_seed_has_total_but_does_not_invent_progress_or_eta(setup_queue):
    root, source, workspace, _, item = setup_queue
    item['name'] = 'binance-archive-e-primary-20261104-v1'
    config = root / 'queue.json'
    write_json(config, {'schema_version': 1, 'contract': queue.CONTRACT, 'sources': [item]})
    _, destination, evidence, _ = queue.paths(item, workspace)
    write_json(evidence / 'intent.json', {'contract': 'workspace_exact_relocation_v1', 'state': 'indexed',
        'source': str(source), 'destination': str(destination), 'files': 5, 'logical_bytes': 1000,
        'started_at_epoch': 1000})
    write_json(evidence / 'progress.json', {'state': 'seeding_hardlinked_copy',
        'files_total': 5, 'observed_at_epoch': 1001, 'source_files_deleted': 0})
    write_json(workspace / 'migration/source-staging-queue-v1.json', {'contract': queue.CONTRACT,
        'observed_at_epoch': 1002, 'sources': [{'name': item['name'], 'state': 'waiting',
        'reason': 'original_owner_or_reader', 'error_type': 'BlockingIOError'}]})
    row, = progress.workspace_source_staging_tasks(root, config, workspace, now=1003)
    assert row['state'] == 'running' and row['phase'] == 'workspace_seed_copy'
    assert row['total_bytes'] == 1000 and row['total_items'] == 5
    assert row['completed_bytes'] is None and row['completed_items'] is None
    assert row['_original_staging_owner_locked'] and not row['complete_evidence']
    row['eta'], _ = progress.estimate(row, {}, now=1003)
    assert row['eta']['completion_at_utc'] is None


def test_monitor_old_stream_owner_does_not_call_write_phase_final_completion(setup_queue, monkeypatch):
    root, source, workspace, _, item = setup_queue
    item.update(name='tw-minute-source-stream-e-20261108-v1',
                source='data_tw_minute/shioaji_1m', marker='download_summary.json',
                seed_hardlinks=False, copy_mode='source_stream_no_readback')
    source = root/item['source']
    source.mkdir(parents=True)
    config = root/'queue.json'
    write_json(config, {'schema_version': 1, 'contract': queue.CONTRACT, 'sources': [item]})
    _, destination, evidence, _ = queue.paths(item, workspace)
    write_json(evidence/'intent.json', {'contract': 'workspace_exact_relocation_v1', 'state': 'indexed',
        'source': str(source), 'destination': str(destination), 'files': 5, 'logical_bytes': 1000,
        'started_at_epoch': 1000})
    write_json(evidence/'progress.json', {'state': 'source_stream_copy', 'files_completed': 5,
        'bytes_completed': 1000, 'observed_at_epoch': 1001})
    monkeypatch.setattr(progress, '_staging_e_io', lambda *_a: 1)
    row, = progress.workspace_source_staging_tasks(root, config, workspace, now=1003)
    assert row['state'] == 'running' and row['phase'] == 'workspace_source_finalizing'
    assert row['completed_bytes'] is None and row['total_bytes'] is None
    assert not row['complete_evidence']
    row['eta'], _ = progress.estimate(row, {}, now=1003)
    assert row['eta']['completion_at_utc'] is None


def test_monitor_rejects_wrong_scope_and_mismatched_copy_proof(setup_queue):
    root, source, workspace, _, item = setup_queue
    item['name'] = 'binance-archive-e-primary-20261104-v1'
    config = root / 'queue.json'
    write_json(config, {'schema_version': 1, 'contract': queue.CONTRACT, 'sources': [item]})
    _, destination, evidence, report = queue.paths(item, workspace)
    destination.mkdir(parents=True)
    write_json(evidence / 'intent.json', {'contract': 'workspace_exact_relocation_v1', 'state': 'indexed',
        'source': str(source), 'destination': str(destination), 'files': 1, 'logical_bytes': 3})
    copy_path = evidence / 'copy-result.json'
    write_json(copy_path, {'state': 'exact_destination_readback_verified',
        'all_source_sha256_and_destination_readback_verified': True, 'source_files_deleted': 0,
        'files': 1, 'logical_bytes': 3})
    write_json(report, {'state': 'verified_temporary_e_source_copy', 'source': str(source),
        'e_destination': str(destination), 'relocation_evidence': str(copy_path),
        'relocation_evidence_sha256': hashlib.sha256(copy_path.read_bytes()).hexdigest(),
        'source_files_deleted': 0, 'source_alias_changed': False, 'files': 1, 'logical_bytes': 3,
        'observed_at_epoch': 1000})
    row, = progress.workspace_source_staging_tasks(root, config, workspace, now=1001)
    assert row['complete_evidence'] and row['state'] == 'completed'
    copy_path.write_text('{}')
    row, = progress.workspace_source_staging_tasks(root, config, workspace, now=1001)
    assert not row['complete_evidence'] and row['state'] == 'blocked'


@pytest.mark.parametrize(('name', 'subject', 'label'), [
    ('tw-minute-source-e-primary-20261104-v1', 'tw-minute-source', '台股分鐘原始資料'),
    ('tw-minute-train-e-primary-20261104-v1', 'tw-minute-train', '台股分鐘研究資料'),
    ('tw-microstructure-train-e-primary-20261104-v1', 'tw-microstructure-train', '台股微結構研究資料'),
    ('tw-futures-prepared-e-primary-20261104-v1', 'tw-futures-prepared', '台灣期貨訓練準備資料'),
    ('legacy-parquet-e-primary-20261104-v1', 'legacy-parquet', '台股舊版 Parquet 資料'),
    ('public-economic-e-primary-20261104-v1', 'public-economic-history', '公開經濟歷史資料（本機）'),
])
def test_monitor_includes_new_staging_tasks(setup_queue, name, subject, label):
    root, _, workspace, _, item = setup_queue
    item['name'] = name
    config = root / 'queue.json'
    write_json(config, {'schema_version': 1, 'contract': queue.CONTRACT, 'sources': [item]})
    row, = progress.workspace_source_staging_tasks(root, config, workspace, now=1001)
    assert row['subject'] == subject
    assert progress.SUBJECTS[subject] == label
    assert row['state'] == 'waiting' and not row['complete_evidence']


def test_monitor_accepts_only_exact_symlink_preserving_staging_scope(setup_queue):
    root, source, workspace, _, item = setup_queue
    item.update(name='tw-futures-prepared-e-primary-20261104-v1', preserve_symlinks=True)
    config = root/'queue.json'
    write_json(config, {'schema_version': 1, 'contract': queue.CONTRACT, 'sources': [item]})
    _, destination, evidence, _ = queue.paths(item, workspace)
    intent = {'contract': 'workspace_scoped_relocation_v1', 'state': 'indexed',
              'source': str(source), 'destination': str(destination), 'files': 5, 'logical_bytes': 1000,
              'preserve_symlinks': True, 'excluded_subtrees': [], 'started_at_epoch': 1000}
    write_json(evidence/'intent.json', intent)
    write_json(evidence/'progress.json', {'state': 'copying_and_independent_sha_readback',
                                          'files_completed': 2, 'bytes_completed': 400,
                                          'observed_at_epoch': 1001})
    row, = progress.workspace_source_staging_tasks(root, config, workspace, now=1003)
    assert row['phase'] == 'workspace_copy' and row['completed_bytes'] == 400
    intent['excluded_subtrees'] = ['hidden']
    write_json(evidence/'intent.json', intent)
    row, = progress.workspace_source_staging_tasks(root, config, workspace, now=1003)
    assert row['phase'] == 'workspace_inventory' and row['completed_bytes'] is None


def test_lock_owner_requires_exact_device_inode_and_single_exclusive_lock():
    rows = '43: FLOCK  ADVISORY  WRITE 171417 08:40:68856315 0 EOF\n'
    assert progress._staging_owner_pid(rows, (8, 64), 68856315) == 171417
    assert progress._staging_owner_pid(rows, (8, 80), 68856315) is None
    assert progress._staging_owner_pid(rows.replace('0 EOF', '0 100'), (8, 64), 68856315) is None
    assert progress._staging_owner_pid(rows + rows, (8, 64), 68856315) is None


def test_cgroup_observation_accepts_real_agent_id_but_rejects_other_paths():
    group = '0::/system.slice/stockagent-agent-binance-20261104T100606-f188c191.service'
    assert progress._staging_cgroup_path(group) == group[3:]
    assert progress._staging_cgroup_path('0::/system.slice/stockagent-workspace-source-staging.service')
    assert progress._staging_cgroup_path('0::/system.slice/unrelated.service') is None
    assert progress._staging_cgroup_path('0::/system.slice/../unrelated.service') is None


def test_live_io_is_not_a_verified_progress_counter_or_a_finish_estimate():
    row = progress.task('workspace_source_staging', 'fixed', state='running',
        phase='workspace_seed_copy', observed=1000, activity=2000, total_bytes=1000,
        total_items=5, metrics={'staging_e_write_bytes': 800})
    row['checked_at_utc'] = progress.iso(2000)
    row['subject'] = 'binance-public-archive'
    row['owner_state'] = 'active'
    row['eta'], _ = progress.estimate(row, {}, now=2000)
    value = progress.public_status({'contract': progress.CONTRACT,
        'observed_at_utc': progress.iso(2000), 'tasks': [row], 'schedules': [
            {'id': 'workspace_source_staging', 'mode': 'timer', 'registered': True,
             'timer_active': True, 'owner_running': False, 'owner_state': 'inactive'}]}, now=2000)
    public, = value['tasks']
    assert public['progress']['completed_bytes'] is None
    assert public['progress']['percent'] is None
    assert public['progress_stale']
    assert public['metrics']['staging_e_write_bytes'] == 800
    assert public['eta']['state'] == 'unknown' and public['eta']['completion_at_utc'] is None
    assert public['continuation']['automatic'] and public['continuation']['owner_running']


def test_bounded_seed_sample_exposes_only_list_progress_and_scoped_estimate(setup_queue, monkeypatch):
    from stockagent.data_sync import workspace_seed_observation as seed
    root, source, workspace, _, item = setup_queue
    item['name'] = 'binance-archive-e-primary-20261104-v1'
    config = root/'queue.json'
    write_json(config, {'schema_version': 1, 'contract': queue.CONTRACT, 'sources': [item]})
    _, destination, evidence, _ = queue.paths(item, workspace)
    write_json(evidence/'intent.json', {'contract': 'workspace_exact_relocation_v1', 'state': 'indexed',
        'source': str(source), 'destination': str(destination), 'files': 10, 'entries': 10,
        'logical_bytes': 1000, 'started_at_epoch': 1000})
    write_json(evidence/'progress.json', {'state': 'seeding_hardlinked_copy', 'observed_at_epoch': 1001})
    monkeypatch.setattr(progress, '_staging_e_io', lambda *a: 900)
    sample = {'observed_at_epoch': 1002, 'entries_checked': 2, 'entries_total': 11,
              'transferred_bytes': 200, 'client_start_ticks': 'fixed'}
    monkeypatch.setattr(seed, 'bound_sample', lambda *a, **k: sample)
    first, = progress.workspace_source_staging_tasks(root, config, workspace, now=1002)
    _, history = progress.estimate(first, {}, now=1002)
    sample.update(observed_at_epoch=1062, entries_checked=5, transferred_bytes=600)
    row, = progress.workspace_source_staging_tasks(root, config, workspace, now=1062)
    row['eta'], _ = progress.estimate(row, history, now=1062)
    assert row['completed_bytes'] is None and row['total_bytes'] is None
    assert row['completed_items'] == 5 and row['total_items'] == 11
    assert row['eta']['state'] == 'estimated' and row['eta']['scope'] == 'rsync_filelist_estimate'
    assert not row['complete_evidence'] and row['metrics']['seed_transferred_bytes'] == 600
    row.update(checked_at_utc=progress.iso(1062), owner_state='active')
    public = progress.public_status({'contract': progress.CONTRACT, 'observed_at_utc': progress.iso(1062),
                                     'tasks': [row]}, now=1062)['tasks'][0]
    assert public['eta']['scope'] == 'rsync_filelist_estimate'
    assert public['progress']['percent'] == 45.45 and not public['complete_evidence']


def test_seed_checkpoint_is_visible_but_never_a_current_progress_or_eta(setup_queue, monkeypatch):
    from stockagent.data_sync import workspace_seed_observation as seed
    root, source, workspace, _, item = setup_queue
    item['name'] = 'binance-archive-e-primary-20261104-v1'
    config = root/'queue.json'
    write_json(config, {'schema_version': 1, 'contract': queue.CONTRACT, 'sources': [item]})
    _, destination, evidence, _ = queue.paths(item, workspace)
    write_json(evidence/'intent.json', {'contract': 'workspace_exact_relocation_v1', 'state': 'indexed',
        'source': str(source), 'destination': str(destination), 'files': 10, 'entries': 10,
        'logical_bytes': 1000, 'started_at_epoch': 1000})
    write_json(evidence/'progress.json', {'state': 'seeding_hardlinked_copy', 'observed_at_epoch': 1001})
    monkeypatch.setattr(progress, '_staging_e_io', lambda *a: 900)
    monkeypatch.setattr(seed, 'bound_sample', lambda *a, **k: None)
    monkeypatch.setattr(seed, 'bound_checkpoint', lambda *a, **k: {
        'verified_files': 4, 'verified_bytes': 300, 'observed_at_epoch': 1002})
    row, = progress.workspace_source_staging_tasks(root, config, workspace, now=1003)
    row['eta'], _ = progress.estimate(row, {}, now=1003)
    row.update(checked_at_utc=progress.iso(1003), owner_state='active')
    public = progress.public_status({'contract': progress.CONTRACT, 'observed_at_utc': progress.iso(1003),
                                     'tasks': [row]}, now=1003)['tasks'][0]
    assert public['metrics']['seed_checkpoint_files'] == 4
    assert public['progress']['completed_items'] is None and public['progress']['completed_bytes'] is None
    assert public['eta']['completion_at_utc'] is None and not public['complete_evidence']


@pytest.mark.parametrize(('state', 'field'), [
    ('verifying_previously_copied_files', 'files_completed'),
    ('verifying_source_namespace', 'entries_completed'),
    ('applying_destination_directory_metadata', 'entries_completed'),
    ('verifying_destination_namespace', 'entries_completed'),
])
def test_metadata_only_phase_does_not_claim_full_source_bytes_as_its_work(setup_queue, state, field):
    root, source, workspace, _, item = setup_queue
    item['name'] = 'binance-archive-e-primary-20261104-v1'
    config = root/'queue.json'
    write_json(config, {'schema_version': 1, 'contract': queue.CONTRACT, 'sources': [item]})
    _, destination, evidence, _ = queue.paths(item, workspace)
    write_json(evidence/'intent.json', {'contract': 'workspace_exact_relocation_v1', 'state': 'indexed',
        'source': str(source), 'destination': str(destination), 'files': 10, 'entries': 12,
        'logical_bytes': 1000, 'started_at_epoch': 1000})
    write_json(evidence/'progress.json', {'state': state, field: 3, 'files_total': 10,
        'entries_total': 12, 'observed_at_epoch': 1001})
    row, = progress.workspace_source_staging_tasks(root, config, workspace, now=1003)
    assert row['completed_items'] == 3 and row['total_items'] in (10, 12)
    assert row['completed_bytes'] is None and row['total_bytes'] is None


def test_shared_queue_running_does_not_mark_unstarted_source_running():
    waiting = progress.task('workspace_source_staging', 'waiting', state='waiting',
                            phase='workspace_inventory', reason='staging_queued')
    waiting.update(checked_at_utc=progress.iso(2000), owner_state='unknown')
    current = progress.task('workspace_source_staging', 'current', state='running',
                            phase='workspace_seed_copy', activity=2000)
    current.update(checked_at_utc=progress.iso(2000), owner_state='active')
    value = progress.public_status({'contract': progress.CONTRACT,
        'observed_at_utc': progress.iso(2000), 'tasks': [waiting, current], 'schedules': [
            {'id': 'workspace_source_staging', 'mode': 'timer', 'registered': True,
             'timer_active': True, 'owner_running': True, 'owner_state': 'active'}]}, now=2000)
    queued, running = value['tasks']
    assert queued['continuation']['mode'] == 'scheduled' and not queued['continuation']['owner_running']
    assert running['continuation']['mode'] == 'running' and running['continuation']['owner_running']


def test_cftc_restore_monitor_requires_matching_fixed_receipts_and_ready(tmp_path):
    snapshot, digest, inventory = 'fixed-v1', 'a' * 64, 'b' * 64
    target = tmp_path / 'restore/cftc-closed-release-e-20261104-v1/cftc-legacy-pre2000' / snapshot
    target.mkdir(parents=True)
    receipt = {'contract': 'stockagent.cftc_closed_E_restore.v1',
        'state': 'fixed_release_materialized_and_readback_verified', 'dataset': 'cftc-legacy-pre2000',
        'snapshot_id': snapshot, 'manifest_sha256': digest, 'inventory_sha256': inventory,
        'raw_originals_restored': 2, 'source_files_deleted': 0, 'source_alias_changed': False,
        'D_snapshot_changed': False, 'NAS_acceptance_claimed': False, 'observed_at_epoch': 1000.5,
        'E_materialized_path': str(target)}
    reread = {'contract': 'stockagent.cftc_e_raw_second_read.v1', 'state': 'verified',
        'fixed_manifest_sha256': digest, 'raw_files': 2, 'raw_bytes': 30,
        'items': [{'bytes': 10, 'sha256': 'c' * 64}, {'bytes': 20, 'sha256': 'd' * 64}]}
    write_json(tmp_path / 'verify/cftc-closed-release-e-20261104-v1.json', receipt)
    second = tmp_path / 'verify/cftc-E-second-readback-20261104-v1.json'
    write_json(second, reread)
    ready = target.parent / ('.' + snapshot + '.READY.json')
    write_json(ready, {'snapshot_id': snapshot, 'manifest_sha256': digest, 'inventory_sha256': inventory})
    assert progress._cftc_e_restore_metrics(tmp_path) == {'cftc_raw_restored_files': 2,
        'cftc_raw_restored_bytes': 30, 'cftc_raw_restored_epoch': 1000}
    write_json(second, {**reread, 'fixed_manifest_sha256': 'e' * 64})
    assert progress._cftc_e_restore_metrics(tmp_path) == {}
    write_json(second, reread)
    ready.unlink()
    assert progress._cftc_e_restore_metrics(tmp_path) == {}


def test_one_busy_source_does_not_prevent_a_disjoint_source_from_running(setup_queue, monkeypatch):
    _, _, workspace, locks, item = setup_queue
    items = [item, {**item, 'name': 'other-e-v1', 'source': 'data_other'}]
    observed = []
    monkeypatch.setattr(queue, 'load_sources', lambda: items)
    monkeypatch.setattr(queue.volume, 'check', lambda: None)
    monkeypatch.setattr(queue.volume, 'configuration', lambda: {'mount_point': str(workspace)})
    monkeypatch.setattr(queue.volume, 'ENROLLMENT', locks / 'enrollment.json')
    def run(value, _, __):
        observed.append(value['name'])
        if value['name'] == item['name']:
            raise BlockingIOError('existing owner')
        return {'name': value['name'], 'state': 'copy_verified'}
    monkeypatch.setattr(queue, 'run_source', run)
    assert queue.main() == 75
    value = json.loads((workspace / 'migration/source-staging-queue-v1.json').read_text())
    assert set(observed) == {'archive-e-v1', 'other-e-v1'}
    assert [x['state'] for x in value['sources']] == ['waiting', 'copy_verified']
    assert value['source_files_deleted'] == 0 and not value['source_aliases_changed']


def test_disjoint_completion_is_published_while_the_first_lane_is_still_running(setup_queue, monkeypatch):
    _, _, workspace, locks, item = setup_queue
    items = [item, {**item, 'name': 'other-e-v1', 'source': 'data_other'}]
    monkeypatch.setattr(queue, 'load_sources', lambda: items)
    monkeypatch.setattr(queue.volume, 'check', lambda: None)
    monkeypatch.setattr(queue.volume, 'configuration', lambda: {'mount_point': str(workspace)})
    monkeypatch.setattr(queue.volume, 'ENROLLMENT', locks / 'enrollment.json')
    first_started, release, published = threading.Event(), threading.Event(), threading.Event()

    def run(value, _, __):
        if value['name'] == item['name']:
            first_started.set()
            assert release.wait(5)
        return {'name': value['name'], 'state': 'copy_verified'}

    original_write = queue.atomic_write_json

    def capture(path, value, **kwargs):
        original_write(path, value, **kwargs)
        if [x['state'] for x in value['sources']] == ['queued', 'copy_verified']:
            published.set()

    monkeypatch.setattr(queue, 'run_source', run)
    monkeypatch.setattr(queue, 'atomic_write_json', capture)
    thread = threading.Thread(target=queue.main)
    thread.start()
    try:
        assert first_started.wait(5) and published.wait(5)
        value = json.loads((workspace / 'migration/source-staging-queue-v1.json').read_text())
        assert [x['state'] for x in value['sources']] == ['queued', 'copy_verified']
        assert not value['NAS_backup_claimed'] and not value['source_aliases_changed']
    finally:
        release.set()
        thread.join(5)
    assert not thread.is_alive()


def test_monitor_distinguishes_a_new_queued_source_from_a_lock_blocker(setup_queue):
    root, _, workspace, _, item = setup_queue
    item['name'] = 'coinmetrics-community-e-primary-20261104-v1'
    config = root / 'queue.json'
    write_json(config, {'schema_version': 1, 'contract': queue.CONTRACT, 'sources': [item]})
    write_json(workspace / 'migration/source-staging-queue-v1.json', {
        'contract': queue.CONTRACT, 'observed_at_epoch': 1000, 'sources': []})
    row, = progress.workspace_source_staging_tasks(root, config, workspace, now=1001)
    assert row['state'] == 'waiting' and row['reason'] == 'staging_queued'
    assert not row['complete_evidence']
