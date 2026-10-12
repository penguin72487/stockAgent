from pathlib import Path

import pytest

from stockagent.storage_layout import admit_output, relocate_configuration, runtime_cache_path
import stockagent.data_sync.training_layout as layout
from stockagent.data_sync.desync_snapshots import SnapshotError


@pytest.mark.parametrize("role", ["prepared", "code", "cache", "benchmark", "operations"])
def test_non_training_writers_reject_markets_including_aliases(tmp_path, role):
    markets = tmp_path / "artifacts/markets"
    markets.mkdir(parents=True)
    with pytest.raises(ValueError, match="reserved"):
        admit_output(markets / "new", role)
    alias = tmp_path / "alias"
    alias.symlink_to(markets, target_is_directory=True)
    with pytest.raises(ValueError, match="reserved"):
        admit_output(alias / "new", role)
    assert admit_output(tmp_path / "artifacts/data_preparation/new", role)
    assert admit_output(markets / "result", "training")


def test_new_cache_launch_copy_localizes_legacy_path_and_keeps_custom_path(tmp_path):
    root = tmp_path / 'owner'
    (root / '.git').mkdir(parents=True)
    (root / 'scripts').mkdir(); (root / 'scripts/runtime_env.sh').write_text('')
    frozen = root / 'artifacts/code_releases/run/build-source'; frozen.mkdir(parents=True)
    legacy = root / 'artifacts/markets/run/runtime-cache/physical-source'
    alias = tmp_path / 'old-cache'; alias.symlink_to(legacy)
    expected = root / 'artifacts/cache/run/runtime-cache/physical-source'
    assert runtime_cache_path(legacy, frozen) == expected
    assert runtime_cache_path(alias, frozen) == expected
    custom = tmp_path / 'custom-cache'
    assert runtime_cache_path(custom, frozen) == custom
    assert not expected.exists() and not custom.exists()


@pytest.mark.parametrize('alias', [False, True])
def test_canonical_executor_cache_writers_reject_markets_before_source_reads(tmp_path, alias):
    import numpy as np
    from stockagent.data.tw_day_trade_execution import load_tw_day_trade_execution_tape
    from stockagent.data.tw_day_trade_carry_source import build_prepared_day_trade_carry_source
    cache = tmp_path / 'artifacts/markets/run/runtime-cache'
    selected = tmp_path / 'alias' if alias else cache
    if alias:
        selected.symlink_to(cache)
    with pytest.raises(ValueError, match='reserved for training results'):
        load_tw_day_trade_execution_tape(tmp_path / 'missing-source', panel_dates=np.array([]),
            panel_symbols=[], official_open_prices=np.empty((0, 0)), cache_dir=selected)
    with pytest.raises(ValueError, match='reserved for training results'):
        build_prepared_day_trade_carry_source(panel=None, minute_root=tmp_path / 'missing-source',
            public_feature_path=tmp_path / 'missing-features', cache_dir=selected,
            allow_daily_proxy=True, daily_proxy_price_policy='official_open_close', corporate_action_mode='avoid')
    assert not cache.exists()


@pytest.fixture
def example(tmp_path, monkeypatch):
    root = tmp_path / "repo"
    a = root / "artifacts/markets/experiment/prepared"
    b = root / "artifacts/markets/experiment/code-release"
    for p in (a, b):
        p.mkdir(parents=True)
        (p / "payload").write_bytes(b"immutable-input-or-code")
    moves = [{"source": str(p.relative_to(root)),
              "destination": f"artifacts/{role_dir}/experiment/{p.name}", "role": role}
             for p, role, role_dir in [(a, "prepared", "data_preparation"), (b, "code", "code_releases")]]
    monkeypatch.setattr(layout, "artifact_process_references", lambda *args: [])
    monkeypatch.setattr(layout, "active_configuration_references_many", lambda sources, root: {})
    monkeypatch.setattr(layout, "artifact_service_references", lambda *args: {})
    return root, a, b, moves, tmp_path / "state"


def test_same_files_move_and_idempotent_receipts_preserve_inodes(example):
    root, a, _, moves, state = example
    info = (a / "payload").stat()
    fixed = layout.plan(root, moves)
    result = layout.apply_plan(fixed, state)
    new = root / moves[0]["destination"] / "payload"
    assert result["state"] == "complete" and result["completed"] == 2
    assert not a.exists() and new.read_bytes() == b"immutable-input-or-code"
    assert (new.stat().st_dev, new.stat().st_ino, new.stat().st_mtime_ns) == (info.st_dev, info.st_ino, info.st_mtime_ns)
    assert layout.apply_plan(fixed, state)["state"] == "complete"


def test_busy_source_does_not_block_independent_move_and_retries(example, monkeypatch):
    root, a, b, moves, state = example
    fixed = layout.plan(root, moves)
    monkeypatch.setattr(layout, "artifact_process_references", lambda p, _: ["pid=1:maps"] if p == a else [])
    first = layout.apply_plan(fixed, state)
    assert a.exists() and not b.exists() and first["pending"] == 1
    monkeypatch.setattr(layout, "artifact_process_references", lambda *args: [])
    assert layout.apply_plan(fixed, state)["state"] == "complete"


def test_changed_source_or_occupied_destination_never_overwritten(example):
    root, a, _, moves, state = example
    fixed = layout.plan(root, moves)
    (a / "payload").write_bytes(b"new generation")
    result = layout.apply_plan(fixed, state)
    assert result["rows"][0]["state"] == "source-changed" and a.exists()
    (root / moves[1]["source"]).mkdir(parents=True)
    with pytest.raises(SnapshotError, match="occupied"):
        layout.plan(root, moves[1:])


def test_interrupted_rename_only_admits_same_fixed_destination(example):
    root, a, _, moves, state = example
    fixed = layout.plan(root, moves)
    dst = root / moves[0]["destination"]
    dst.parent.mkdir(parents=True)
    a.rename(dst)
    result = layout.apply_plan(fixed, state)
    assert result["rows"][0]["recovered_interrupted_intent"]


def test_layout_rejects_training_result_moves_and_overlapping_scopes(example):
    root, _, _, moves, _ = example
    bad = {**moves[0], "role": "training", "destination": "artifacts/markets/new"}
    with pytest.raises(SnapshotError):
        layout.plan(root, [bad])
    with pytest.raises(SnapshotError, match="overlap"):
        layout.plan(root, [moves[0], moves[0]])


def test_enrolled_live_benchmark_freezes_new_generation_only_after_idle(example, monkeypatch):
    root, a, _, _, state = example
    moves = [{"source": str(a.relative_to(root)), "destination": "artifacts/benchmarks/experiment",
              "role": "benchmark", "refresh_when_idle": True}]
    fixed = layout.plan(root, moves)
    (a / "next-result").write_bytes(b"new result while owner running")
    monkeypatch.setattr(layout, "artifact_process_references", lambda *args: ["active-owner"])
    assert layout.apply_plan(fixed, state)["pending"] == 1 and a.exists()
    monkeypatch.setattr(layout, "artifact_process_references", lambda *args: [])
    result = layout.apply_plan(fixed, state)
    assert result["state"] == "complete" and result["rows"][0]["enrollment_fingerprint"] == fixed["moves"][0]["fingerprint"]
    assert (root / moves[0]["destination"] / "next-result").read_bytes() == b"new result while owner running"


def test_mutable_cache_keeps_files_created_after_enrollment(example):
    root, a, _, _, state = example
    moves = [{"source": str(a.relative_to(root)), "destination": "artifacts/cache/experiment",
              "role": "cache"}]
    fixed = layout.plan(root, moves)
    (a / 'new-array').write_bytes(b'newly built cache')
    result = layout.apply_plan(fixed, state)
    assert result['state']=='complete' and result['rows'][0]['enrollment_fingerprint']==fixed['moves'][0]['fingerprint']
    assert (root/'artifacts/cache/experiment/new-array').read_bytes()==b'newly built cache'


def test_recreated_cache_reopens_queue_and_preserves_each_generation(example, monkeypatch):
    root, a, _, _, state = example
    moves = [{'source': str(a.relative_to(root)), 'destination': 'artifacts/cache/experiment/runtime-cache', 'role': 'cache'}]
    fixed = layout.plan(root, moves)
    assert layout.apply_plan(fixed, state)['state'] == 'complete'
    original = root / moves[0]['destination']
    a.mkdir(); (a / 'payload').write_bytes(b'cache rebuilt by a later frozen run')
    monkeypatch.setattr(layout, 'artifact_process_references', lambda p, _: ['active-owner'] if p == a else [])
    busy = layout.apply_plan(fixed, state)
    assert busy['state'] == 'pending' and busy['pending'] == 1 and busy['completed'] == 1
    assert original.joinpath('payload').read_bytes() == b'immutable-input-or-code'
    monkeypatch.setattr(layout, 'artifact_process_references', lambda *args: [])
    result = layout.apply_plan(fixed, state)
    assert result['state'] == 'complete' and not a.exists()
    first = root / result['recreated_cache_generations'][0]['destination']
    assert first.joinpath('payload').read_bytes() == b'cache rebuilt by a later frozen run'
    a.mkdir(); (a / 'next').write_bytes(b'third complete cache')
    result = layout.apply_plan(fixed, state)
    second = root / result['recreated_cache_generations'][0]['destination']
    assert second != first and second.joinpath('next').read_bytes() == b'third complete cache'
    assert original.joinpath('payload').read_bytes() == b'immutable-input-or-code'
    assert layout.apply_plan(fixed, state)['pending'] == 0


def test_recreated_cache_resumes_post_rename_crash_from_its_child_journal(example, monkeypatch):
    root, a, _, _, state = example
    fixed = layout.plan(root, [{'source': str(a.relative_to(root)), 'destination': 'artifacts/cache/experiment/runtime-cache', 'role': 'cache'}])
    layout.apply_plan(fixed, state)
    a.mkdir(); (a / 'new').write_bytes(b'complete rebuilt cache')
    real_rename = layout.rename_no_replace

    def interrupted(source, destination):
        real_rename(source, destination)
        raise RuntimeError('interrupted before completion receipt')

    monkeypatch.setattr(layout, 'rename_no_replace', interrupted)
    with pytest.raises(RuntimeError, match='interrupted'):
        layout.apply_plan(fixed, state)
    monkeypatch.setattr(layout, 'rename_no_replace', real_rename)
    result = layout.apply_plan(fixed, state)
    generation = result['recreated_cache_generations'][0]
    assert result['state'] == 'complete' and generation['rows'][0]['recovered_interrupted_intent']
    assert (root / generation['destination'] / 'new').read_bytes() == b'complete rebuilt cache'


def test_localization_changes_exact_path_prefix_only_and_keeps_output(example):
    root, a, _, moves, _ = example
    original = {"data": {"manifest": str(a / "manifest.json")},
                "runner": {"output_dir": str(a.parent / "training")},
                "note": "not-a-path " + str(a), "suffix": str(a) + "-different"}
    result = relocate_configuration(original, moves, root)
    assert result["data"]["manifest"] == str(root / moves[0]["destination"] / "manifest.json")
    assert result["runner"] == original["runner"] and result["note"] == original["note"]
    assert result["suffix"] == original["suffix"] and original["data"]["manifest"] == str(a / "manifest.json")


def test_overlay_directory_entry_size_is_bookkeeping_but_file_size_is_identity(example):
    _, a, _, _, _ = example
    from copy import deepcopy
    rows = layout.inventory(a)["rows"]
    changed = deepcopy(rows)
    changed[0]["size"] += 16
    assert changed[0]["kind"] == "directory" and layout.same_relocated_tree(rows, changed)
    changed[1]["size"] += 1
    assert not layout.same_relocated_tree(rows, changed)


def test_live_frozen_configuration_uses_its_selected_schema_without_ignoring_fields(tmp_path):
    from stockagent.data_sync.remote_legacy_return import _active_configuration
    root = tmp_path / 'frozen'; package = root / 'stockagent'; package.mkdir(parents=True)
    (package / '__init__.py').write_text('')
    (package / 'config.py').write_text('''
from dataclasses import dataclass
from pathlib import Path
import json
@dataclass
class Config:
    data: dict
def _config_source_identity(p):
    s=p.stat();return (s.st_ino,s.st_size,s.st_mtime_ns)
def load_config(path,source_signatures=None):
    p=Path(path);v=json.loads(p.read_text())
    source_signatures[p]=_config_source_identity(p)
    return Config(v['data'])
''')
    config = root / 'config.yaml'
    config.write_text('{"new_frozen_schema_field":true,"data":{"input":"/exact/source"}}')
    value, sources = _active_configuration(config, root, [str(root / 'train.py'), '--config',str(config)])
    assert value == {'data':{'input':'/exact/source'}} and sources == [config]


@pytest.mark.parametrize('fault', [None, 'source-pin', 'source-ready', 'renewed-lease'])
def test_relocated_training_preserves_exact_original_source_leases_and_pins(tmp_path, monkeypatch, fault):
    import json
    import time
    from types import SimpleNamespace
    from scripts import run_relocated_training as entry
    from stockagent.data_sync import packed_snapshots, materialized_cache

    materialized, sync = tmp_path / 'materialized', tmp_path / 'packed'
    previous = materialized / '.training-pins/tw-daytrade-no-basis-flat-bf16-tf32-b128-v1'
    previous.mkdir(parents=True)
    fields = ('parquet_root', 'day_trade_minute_execution_root', 'day_trade_physical_public_feature_path')
    configuration = {}
    for i, field in enumerate(fields):
        dataset, snapshot = f'dataset-{i}', f'fixed-source-{i}'
        source = materialized / dataset / snapshot
        source.mkdir(parents=True)
        configuration[field] = str(source)
        (previous / (dataset + '.pin.json')).write_text(json.dumps({
            'manifest': {'snapshot_id': snapshot}, 'manifest_sha256': '0' * 64 if fault != 'source-pin' else '1' * 64}))
    original = tmp_path / 'original-run.json'
    original.write_text(json.dumps({'configuration': {'data': configuration}}))
    calls, pins = [], []
    monkeypatch.setattr(packed_snapshots, 'resolve_packed_snapshot_id',
                        lambda root, dataset, snapshot: SimpleNamespace(manifest_sha256='0' * 64))
    monkeypatch.setattr(materialized_cache, '_ready_matches', lambda *args: fault != 'source-ready')
    monkeypatch.setattr(packed_snapshots, 'write_packed_pin', lambda path, resolved: pins.append(path))

    def renew(command, **kwargs):
        calls.append(command)
        dataset = command[3]
        snapshot = command[command.index('--snapshot-id') + 1]
        target = materialized / dataset / snapshot
        lease = materialized / '.cache-state/leases' / dataset / (snapshot + '.json')
        lease.parent.mkdir(parents=True)
        lease.write_text(json.dumps({'state': 'hot', 'verification': 'full' if fault != 'renewed-lease' else 'ready-marker',
                                    'expires_ns': time.time_ns() + 10**12, 'manifest_sha256': '0' * 64, 'target': str(target)}))

    monkeypatch.setattr(entry.subprocess, 'run', renew)
    launch = {'source_manifest': str(original), 'output_root': '/repo/artifacts/markets/formal-run/training-bf16'}
    if fault:
        with pytest.raises(AssertionError):
            entry.renew_original_source_leases(launch, materialized=materialized, sync=sync)
        assert not pins
        assert len(calls) == (1 if fault == 'renewed-lease' else 0)
    else:
        entry.renew_original_source_leases(launch, materialized=materialized, sync=sync)
        assert len(calls) == len(pins) == 3
        for i, command in enumerate(calls):
            assert command[command.index('--snapshot-id') + 1] == f'fixed-source-{i}'
            assert command[command.index('--ttl-days') + 1] == '7' and '--retain-payload' in command
            assert pins[i].parent == materialized / '.training-pins/formal-run'
