import fcntl
import errno
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3

import pytest

from stockagent.data_sync import benchmark_fixtures as fixtures
from stockagent.data_sync.benchmark_fixtures import ImmutableFixtureInputs, RECEIPT


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _taifex_proof_memo_case(parent):
    source = parent / 'artifacts/benchmarks/running_services_20261013/taifex_proof_memo_20261015'
    source.mkdir(parents=True)
    outputs = dict.fromkeys(('1h', '1d', '1w', '1mo', '1q', '1y', 'all'), 'a' * 64)
    order = ['baseline', 'candidate', 'candidate', 'baseline'] * 2
    def trial(phase, index, variant):
        return {'phase': phase, 'trial': index, 'variant': variant,
            'output_sha256': {'1d': 'b' * 64} if phase == 'setup' else outputs.copy(),
            'durable_complete_readback': True, 'warm_exact_output_parity': True,
            'provider_calls': 0, 'production_writes': False, 'model_forward_calls': 0,
            'wall_seconds': 1.0, 'cpu_seconds': 0.5, 'warm_wall_seconds': 0.1, 'warm_cpu_seconds': 0.1}
    report = {'contract': fixtures.TAIFEX_PROOF_MEMO_CONTRACT, 'state': 'measurement_complete',
        'scope': 'complete_seven_range_real_append_curve_durable_readback_and_warm_not_broker_or_whole_host',
        'fixture': str(source / 'fixture'), 'trial_order': order,
        'balanced_abba': True, 'code_stable': True, 'source_unchanged': True, 'exact_output_parity': True,
        'provider_calls': 0, 'model_forward_calls': 0, 'production_writes': False,
        'completed_at_utc': '2026-10-15T02:00:00+00:00',
        'source_sha256': {str(source / 'fixture/state' / name): 'c' * 64 for name in (
            'state.json', 'status.json', 'marks.jsonl',
            'backfills/all_strategies_bidask_recapitalizing_v3/marks.jsonl',
            'backfills/all_strategies_bidask_recapitalizing_v3/receipt.json')},
        'code_sha256': {str(source / 'original_column_cache.py'): 'd' * 64,
            str(parent / 'scripts/benchmark_taifex_source_proof_memo.py'): 'e' * 64},
        'independent_raw_1d': {'path': str(source / 'comparison-v2/independent-raw-1d.json'),
            'sha256': 'f' * 64, 'output_sha256': outputs['1d']},
        'base_setup': [trial('setup', i, v) for i, v in enumerate(order)],
        'trials': [trial('update', i, v) for i, v in enumerate(order)]}
    objects = {'comparison-v2.json': report, 'original-code-proof.json': {
        'source': 'stockagent/live/taifex_history_column_cache.py', 'prefix_bytes': 123,
        'sha256': 'd' * 64}, 'benchmark-started.json': {'at_utc': '2026-10-15T01:00:00+00:00'}}
    for name, value in objects.items():
        (source / name).write_text(json.dumps(value))
    return source, objects


def test_taifex_proof_memo_retains_original_evidence_after_source_retirement(tmp_path):
    source, objects = _taifex_proof_memo_case(tmp_path)
    saved = tmp_path / 'retained-evidence'
    assert fixtures.external_benchmark_contract(source) == fixtures.TAIFEX_PROOF_MEMO_CONTRACT
    result = fixtures.completed_benchmark_evidence(source, destination=saved)
    assert result['producer_contract'] == fixtures.TAIFEX_PROOF_MEMO_CONTRACT
    assert {row['path'] for row in result['files']} == set(objects)
    assert all((saved / name).read_bytes() == (source / name).read_bytes() for name in objects)
    source.rename(source.with_name(source.name + '-preserved'))
    assert fixtures.completed_benchmark_evidence(saved, original_source=source,
        required_contract=fixtures.TAIFEX_PROOF_MEMO_CONTRACT) == result


@pytest.mark.parametrize('problem', ['failed', 'range_missing', 'different_values', 'wrong_order',
    'missing_setup', 'missing_trial', 'duplicate_index', 'unknown_phase', 'writes', 'calls',
    'forward', 'bool_calls', 'nondurable', 'warm_mismatch', 'source_missing', 'source_escape',
    'original_code', 'raw_mismatch', 'raw_escape', 'nan_time', 'negative_time',
    'naive_clock', 'reverse_clock', 'unknown_metadata', 'other_case'])
def test_taifex_proof_memo_refuses_incomplete_or_unbound_completions(tmp_path, problem):
    source, objects = _taifex_proof_memo_case(tmp_path)
    report = objects['comparison-v2.json']
    trial = report['trials'][1]
    if problem == 'failed': report['state'] = 'failed'
    elif problem == 'range_missing': del trial['output_sha256']['all']
    elif problem == 'different_values': trial['output_sha256']['1d'] = '0' * 64
    elif problem == 'wrong_order': report['trial_order'] = report['trial_order'][1:] + report['trial_order'][:1]
    elif problem == 'missing_setup': report['base_setup'].pop()
    elif problem == 'missing_trial': report['trials'].pop()
    elif problem == 'duplicate_index': trial['trial'] = 0
    elif problem == 'unknown_phase': trial['phase'] = 'setup'
    elif problem == 'writes': trial['production_writes'] = True
    elif problem == 'calls': report['provider_calls'] = 1
    elif problem == 'forward': trial['model_forward_calls'] = 1
    elif problem == 'bool_calls': trial['provider_calls'] = False
    elif problem == 'nondurable': trial['durable_complete_readback'] = False
    elif problem == 'warm_mismatch': trial['warm_exact_output_parity'] = False
    elif problem == 'source_missing': report['source_sha256'].popitem()
    elif problem == 'source_escape': report['source_sha256'][str(source / '../other')] = 'c' * 64
    elif problem == 'original_code': objects['original-code-proof.json']['sha256'] = '0' * 64
    elif problem == 'raw_mismatch': report['independent_raw_1d']['output_sha256'] = '0' * 64
    elif problem == 'raw_escape': report['independent_raw_1d']['path'] = '/another/independent-raw-1d.json'
    elif problem == 'nan_time': trial['wall_seconds'] = float('nan')
    elif problem == 'negative_time': trial['cpu_seconds'] = -1
    elif problem == 'naive_clock': report['completed_at_utc'] = '2026-10-15T02:00:00'
    elif problem == 'reverse_clock': report['completed_at_utc'] = '2026-10-14T02:00:00+00:00'
    elif problem == 'unknown_metadata': objects['unknown.json'] = {}
    elif problem == 'other_case':
        target = source.with_name('not-taifex')
        source.rename(target)
        source = target
    for name, value in objects.items():
        (source / name).write_text(json.dumps(value))
    with pytest.raises(fixtures.SnapshotError, match='supported completed producer'):
        fixtures.completed_benchmark_evidence(source)


def _public_builder_case(parent):
    source = parent / 'artifacts/benchmarks/running_services_20261013/public-feature-assembly-20261015c'
    (source / 'source').mkdir(parents=True)
    original = source / 'source/data.parquet'
    original.write_bytes(b'complete literal source')
    fixture = {'contract': 'tw_public_private_full_builder_fixture_v1',
        'original_code_sha256': 'c' * 64, 'symbol_universe': {'names': ['0050', '2330']},
        'source_files': [{'name': original.name, 'size': original.stat().st_size, 'sha256': _sha(original)}]}
    fixture_raw = json.dumps(fixture).encode()
    (source / 'fixture.json').write_bytes(fixture_raw)
    rows, measurements = [], []
    for name, variant in [('a0', 'original'), ('a1', 'original'), ('b1', 'streaming'),
                          ('b2', 'streaming'), ('a2', 'original')]:
        trial = source / name
        trial.mkdir()
        output = trial / 'features.parquet'
        output.write_bytes(b'complete byte-identical values\0')
        descriptor = {'name': output.name, 'size': output.stat().st_size, 'sha256': _sha(output)}
        receipt = {'state': 'completed_trial', 'variant': variant, 'build_mode': 'full',
            'original_code_sha256': fixture['original_code_sha256'],
            'symbol_universe_receipt': fixture['symbol_universe'],
            'source_receipts': fixture['source_files'], 'rows': 3, 'stock_rows': 3, 'market_rows': 0,
            'features': 1, 'pool_profile': {'threads': 2}, 'polars_threads': 2, 'arrow_threads': 2,
            'harness_code_sha256': ('a' if name == 'a0' else 'b') * 64,
            'provider_calls': 0, 'production_writes': False, 'model_runtime_imported': False,
            'output_receipt': descriptor}
        raw = json.dumps(receipt)
        (trial / 'receipt.json').write_text(raw)
        sha = hashlib.sha256(raw.encode()).hexdigest()
        rows.append({'trial': name, 'receipt_raw': raw, 'receipt_sha256': sha, 'verified_output': descriptor})
        if name != 'a0':
            measurements.append({'trial': name, 'variant': variant, 'receipt_sha256': sha, 'output': descriptor})
    paths = fixtures._public_builder_receipts(source)
    proof = {'contract': fixtures.PUBLIC_BUILDER_CONTRACT, 'state': 'accepted_exact_output',
        'fixture': str(source), 'fixture_sha256': _sha(source / 'fixture.json'),
        'source_files': 1, 'rows': 3, 'feature_count': 1, 'measurements': measurements,
        'proof_method': 'complete_parquet_bytes', 'exact_output_parity': True,
        'repeated_per_variant': True, 'provider_calls': 0, 'production_writes': False}
    paths[0].write_text(json.dumps(proof))
    evidence = {'contract': 'tw_public_builder_frozen_retirement_evidence_v1',
        'source': str(source), 'fixture_sha256': proof['fixture_sha256'],
        'verification_sha256': _sha(paths[0]), 'source_files': fixture['source_files'],
        'trials': rows, 'provider_calls': 0, 'production_writes': False}
    paths[1].write_text(json.dumps(evidence))
    return source, paths, fixture, proof, evidence


def test_public_builder_preserves_all_original_receipts_and_separate_harness_scope(tmp_path):
    source, paths, fixture, proof, evidence = _public_builder_case(tmp_path)
    saved = tmp_path / 'retained-evidence'
    result = fixtures.completed_benchmark_evidence(source, destination=saved)
    assert result['producer_contract'] == fixtures.PUBLIC_BUILDER_CONTRACT
    assert {row['path'] for row in result['files']} == {'fixture.json', *(p.name for p in paths)}
    assert json.loads((saved / paths[1].name).read_bytes())['trials'] == evidence['trials']
    # The archived original can be gone; retained exact bytes bind the same
    # source path without borrowing another source's current filesystem state.
    source.rename(source.with_name(source.name + '-kept-for-test'))
    assert fixtures.completed_benchmark_evidence(saved, original_source=source,
        required_contract=fixtures.PUBLIC_BUILDER_CONTRACT) == result


@pytest.mark.parametrize('problem', ['proof_bytes', 'fixture_bytes', 'receipt_bytes', 'output',
    'unknown_state', 'missing_trial', 'duplicate_trial', 'mixed_measured_harness', 'single_repeat',
    'source_scope', 'writes', 'calls', 'wrong_source', 'unknown_metadata'])
def test_public_builder_rejects_changed_incomplete_or_mixed_evidence(tmp_path, problem):
    source, paths, fixture, proof, evidence = _public_builder_case(tmp_path)
    if problem == 'proof_bytes':
        proof['added'] = True
        paths[0].write_text(json.dumps(proof))
    elif problem == 'fixture_bytes':
        (source / 'fixture.json').write_text(json.dumps(fixture) + '\n')
    elif problem == 'missing_trial':
        evidence['trials'].pop()
    elif problem == 'duplicate_trial':
        evidence['trials'].append(evidence['trials'][0])
    elif problem == 'single_repeat':
        proof['measurements'].pop()
        paths[0].write_text(json.dumps(proof))
        evidence['verification_sha256'] = _sha(paths[0])
    elif problem == 'wrong_source':
        evidence['source'] = str(source.parent / 'another')
    elif problem == 'unknown_metadata':
        (source / 'unknown.json').write_text('{}')
    elif problem in {'calls', 'writes'}:
        evidence['provider_calls' if problem == 'calls' else 'production_writes'] = 1
    else:
        row = evidence['trials'][1]
        receipt = json.loads(row['receipt_raw'])
        if problem == 'receipt_bytes':
            row['receipt_raw'] += '\n'  # Keep the original digest to expose drift.
        elif problem == 'output':
            row['verified_output']['sha256'] = 'f' * 64
        else:
            if problem == 'unknown_state':
                receipt['state'] = 'failed'
            elif problem == 'mixed_measured_harness':
                receipt['harness_code_sha256'] = 'a' * 64
            else:
                receipt['source_receipts'] = []
            row['receipt_raw'] = json.dumps(receipt)
            row['receipt_sha256'] = hashlib.sha256(row['receipt_raw'].encode()).hexdigest()
            proof['measurements'][0]['receipt_sha256'] = row['receipt_sha256']
            paths[0].write_text(json.dumps(proof))
            evidence['verification_sha256'] = _sha(paths[0])
    paths[1].write_text(json.dumps(evidence))
    with pytest.raises(fixtures.SnapshotError, match='supported completed producer'):
        fixtures.completed_benchmark_evidence(source)


def _capture(parent, name, sources):
    root = parent / name
    root.mkdir()
    with ImmutableFixtureInputs(root) as inputs:
        for label, source in sources.items():
            inputs.copy(source, label)
        receipt = inputs.commit()
    return root, receipt


def test_complete_sources_shared_but_mutable_original_never_hardlinked(tmp_path):
    source = tmp_path / "live.parquet"
    source.write_bytes(b"all rows, raw nulls and float bits\0" * 500)
    first, cold = _capture(tmp_path, "a", {"source/data.parquet": source})
    second, warm = _capture(tmp_path, "b", {"source/data.parquet": source})
    a, b = first / "source/data.parquet", second / "source/data.parquet"
    assert cold["copied_files"] == 1 and cold["reused_files"] == 0
    assert warm["copied_bytes"] == 0 and warm["reused_bytes"] == source.stat().st_size
    assert a.stat().st_ino == b.stat().st_ino != source.stat().st_ino
    assert a.stat().st_mode & 0o222 == 0
    original = b.read_bytes()
    source.write_bytes(b"new current source")
    assert a.read_bytes() == b.read_bytes() == original
    a.unlink()  # A donor name retiring does not strand the surviving experiment.
    assert b.read_bytes() == original


def test_update_copies_only_changed_source_and_preserves_full_manifest(tmp_path):
    source = tmp_path / "current"
    source.mkdir()
    a, b = source / "a", source / "b"
    a.write_bytes(b"unchanged history")
    b.write_bytes(b"old tail")
    old, _ = _capture(tmp_path, "old", {"a": a, "b": b})
    b.write_bytes(b"corrected tail of different length")
    new, result = _capture(tmp_path, "new", {"a": a, "b": b})
    assert result["reused_files"] == result["copied_files"] == 1
    assert result["copied_bytes"] == b.stat().st_size
    assert (old / "a").stat().st_ino == (new / "a").stat().st_ino
    assert (old / "b").read_bytes() == b"old tail"
    assert (new / "b").read_bytes() == b.read_bytes()
    assert {item["name"] for item in result["files"]} == {"a", "b"}


@pytest.mark.parametrize("defect", ["writable", "wrong_hash", "symlink", "missing", "busy", "corrupt_receipt"])
def test_unsafe_or_busy_donor_falls_back_to_independent_verified_copy(tmp_path, defect):
    source = tmp_path / "live"
    source.write_bytes(b"exact source")
    old, _ = _capture(tmp_path, "old", {"data": source})
    donor = old / "data"
    lock = None
    if defect == "writable":
        donor.chmod(0o600)
    elif defect == "wrong_hash":
        donor.chmod(0o600)
        donor.write_bytes(b"wrong source")
        donor.chmod(0o400)
    elif defect == "symlink":
        donor.unlink()
        donor.symlink_to(source)
    elif defect == "missing":
        donor.unlink()
    elif defect == "busy":
        lock = (old / ".immutable-inputs.lock").open("rb")
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    else:
        (old / RECEIPT).write_text("[]")
    try:
        new, result = _capture(tmp_path, "new", {"data": source})
    finally:
        if lock:
            lock.close()
    assert result["reused_files"] == 0 and result["copied_files"] == 1
    assert (new / "data").read_bytes() == source.read_bytes()
    assert (new / "data").stat().st_ino != source.stat().st_ino


def test_existing_sealed_public_fixture_reused_without_editing_its_receipt(tmp_path):
    source = tmp_path / "live"
    source.write_bytes(b"full retained history")
    old = tmp_path / "old"
    (old / "source").mkdir(parents=True)
    frozen = old / "source/input.parquet"
    frozen.write_bytes(source.read_bytes())
    frozen.chmod(0o400)
    receipt = {"contract": "tw_public_private_full_builder_fixture_v1", "source_files": [
        {"name": "input.parquet", "size": source.stat().st_size, "sha256": _sha(source)}]}
    (old / "fixture.json").write_text(json.dumps(receipt))
    before = (old / "fixture.json").read_bytes()
    new, result = _capture(tmp_path, "new", {"source/input.parquet": source})
    assert result["reused_files"] == 1
    assert frozen.stat().st_ino == (new / "source/input.parquet").stat().st_ino
    assert (old / "fixture.json").read_bytes() == before
    assert not (old / RECEIPT).exists()


def test_incomplete_or_failed_capture_cannot_seed_reuse(tmp_path):
    source = tmp_path / "live"
    source.write_bytes(b"source")
    partial = tmp_path / "partial"
    partial.mkdir()
    with pytest.raises(RuntimeError), ImmutableFixtureInputs(partial) as inputs:
        inputs.copy(source, "data")
        raise RuntimeError("interrupted before coherent source fence")
    assert not (partial / RECEIPT).exists()
    new, receipt = _capture(tmp_path, "new", {"data": source})
    assert receipt["reused_files"] == 0
    assert (new / "data").stat().st_ino != (partial / "data").stat().st_ino


@pytest.mark.parametrize("name", ["../outside", "/outside", "a/../outside", "./data", "a//b", ""])
def test_member_cannot_escape_or_alias_experiment(tmp_path, name):
    source = tmp_path / "live"
    source.write_bytes(b"source")
    root = tmp_path / "new"
    root.mkdir()
    with ImmutableFixtureInputs(root) as inputs, pytest.raises(ValueError):
        inputs.copy(source, name)


def test_rejects_wrong_source_hash_and_existing_destination(tmp_path):
    source = tmp_path / "live"
    source.write_bytes(b"source")
    root = tmp_path / "new"
    root.mkdir()
    with ImmutableFixtureInputs(root) as inputs:
        with pytest.raises(ValueError, match="source differs"):
            inputs.copy(source, "data", expected_sha256="0" * 64)
        assert not (root / "data").exists()
        (root / "occupied").write_bytes(b"keep")
        with pytest.raises(ValueError, match="already exists"):
            inputs.copy(source, "occupied")
        assert (root / "occupied").read_bytes() == b"keep"


def test_mutation_during_freeze_prevents_commit(tmp_path, monkeypatch):
    source = tmp_path / "live"
    source.write_bytes(b"first")
    root = tmp_path / "new"
    root.mkdir()
    copyfileobj = fixtures.shutil.copyfileobj
    def changing_copy(reader, writer, length):
        copyfileobj(reader, writer, length)
        source.write_bytes(b"other")
    monkeypatch.setattr(fixtures.shutil, "copyfileobj", changing_copy)
    with ImmutableFixtureInputs(root) as inputs, pytest.raises(ValueError):
        inputs.copy(source, "data")
    assert not (root / RECEIPT).exists()
    assert not list(root.glob("*.partial"))


def test_fifo_metadata_is_not_read_or_blocked(tmp_path):
    source = tmp_path / "live"
    source.write_bytes(b"source")
    donor = tmp_path / "untrusted"
    donor.mkdir()
    os.mkfifo(donor / RECEIPT)
    _, receipt = _capture(tmp_path, "new", {"data": source})
    assert receipt["reused_files"] == 0


def test_commit_rechecks_full_payload_and_readonly_contract(tmp_path):
    source = tmp_path / "live"
    source.write_bytes(b"first")
    root = tmp_path / "new"
    root.mkdir()
    with ImmutableFixtureInputs(root) as inputs:
        inputs.copy(source, "data")
        (root / "data").chmod(0o600)
        (root / "data").write_bytes(b"other")
        (root / "data").chmod(0o400)
        with pytest.raises(ValueError, match="changed before commit"):
            inputs.commit()
    assert not (root / RECEIPT).exists()


def test_frozen_donor_can_be_the_selected_source_without_false_mutation(tmp_path):
    source = tmp_path / "live"
    source.write_bytes(b"history")
    old, _ = _capture(tmp_path, "old", {"data": source})
    frozen = old / "data"
    new, receipt = _capture(tmp_path, "new", {"data": frozen})
    assert receipt["reused_files"] == 1
    assert frozen.stat().st_ino == (new / "data").stat().st_ino


def test_commit_seals_member_set_and_cannot_be_overwritten(tmp_path):
    source = tmp_path / "live"
    source.write_bytes(b"history")
    root = tmp_path / "new"
    root.mkdir()
    with ImmutableFixtureInputs(root) as inputs:
        inputs.copy(source, "data")
        inputs.commit()
        with pytest.raises(RuntimeError, match="already committed"):
            inputs.copy(source, "more")
        with pytest.raises(ValueError, match="already committed"):
            inputs.commit()
    with pytest.raises(ValueError, match="fresh experiment"):
        ImmutableFixtureInputs(root)


def test_public_capture_uses_shared_inputs_and_keeps_full_source_and_code_proof(tmp_path, monkeypatch):
    from scripts import benchmark_tw_public_feature_assembly as benchmark
    repo = tmp_path / "repo"
    (repo / "stockagent/data").mkdir(parents=True)
    (repo / "stockagent/data/tw_public_features.py").write_bytes(b"# fixed builder\n")
    benchmarks = repo / "artifacts/benchmarks"
    benchmarks.mkdir(parents=True)
    source = tmp_path / "source"
    (source / "stocks").mkdir(parents=True)
    (source / "stocks/2330_features.parquet").write_bytes(b"symbol names only")
    (source / "prices.parquet").write_bytes(b"full price history")
    (source / "features.parquet").write_bytes(b"full features")
    monkeypatch.setattr(benchmark, "ROOT", repo)
    monkeypatch.setattr(benchmark.shutil, "disk_usage", lambda _: shutil._ntuple_diskusage(100 << 30, 0, 100 << 30))
    first, second = benchmarks / "a", benchmarks / "b"
    benchmark.capture(first, source, source / "stocks")
    benchmark.capture(second, source, source / "stocks")
    receipt = benchmark._verified_fixture(second)
    assert receipt["storage_build"]["reused_files"] == 2
    assert receipt["storage_build"]["copied_bytes"] == 0
    assert len(receipt["source_files"]) == 2
    assert receipt["production_writes"] is False and receipt["providers_called"] == 0
    assert (first / "source/prices.parquet").stat().st_ino == (second / "source/prices.parquet").stat().st_ino


def test_openbb_capture_shares_readonly_data_but_never_mutable_sqlite(tmp_path, monkeypatch):
    from scripts import benchmark_openbb_metadata_audit as benchmark
    source = tmp_path / "source"
    (source / "_state").mkdir(parents=True)
    payload = source / "data.parquet"
    payload.write_bytes(b"full source table")
    with sqlite3.connect(source / "_state/openbb_archive.sqlite3") as conn:
        conn.execute("CREATE TABLE tasks(task_id TEXT,active INTEGER,plan_token TEXT,status TEXT,output_path TEXT,rows INTEGER,endpoint TEXT)")
        conn.execute("CREATE TABLE archive_meta(key TEXT,value TEXT)")
        conn.execute("INSERT INTO archive_meta VALUES('active_plan_token','test')")
        conn.execute("INSERT INTO tasks VALUES('one',1,'test','success',?,2,'macro')", (str(payload),))
    monkeypatch.setattr(benchmark.subprocess, "run", lambda *a, **kw: type("Git", (), {"stdout": b"# baseline\n"})())
    monkeypatch.setattr(benchmark.shutil, "disk_usage", lambda _: shutil._ntuple_diskusage(100 << 30, 0, 100 << 30))
    a, b = tmp_path / "a", tmp_path / "b"
    benchmark.prepare(source, a, "test")
    receipt = benchmark.prepare(source, b, "test")
    assert receipt["storage_build"]["reused_files"] == 1
    assert (a / "data/one.parquet").stat().st_ino == (b / "data/one.parquet").stat().st_ino
    db_a, db_b = a / "_state/openbb_archive.sqlite3", b / "_state/openbb_archive.sqlite3"
    assert db_a.stat().st_ino != db_b.stat().st_ino
    with sqlite3.connect(db_b) as conn:
        conn.execute("UPDATE tasks SET rows=999")
    with sqlite3.connect(db_a) as conn:
        assert conn.execute("SELECT rows FROM tasks").fetchone()[0] == 2


def test_target_publication_io_error_propagates_without_copied_fallback(tmp_path, monkeypatch):
    source = tmp_path / "live"
    source.write_bytes(b"source")
    _capture(tmp_path, "old", {"data": source})
    def disk_full(*args, **kwargs):
        raise OSError(errno.ENOSPC, "publication disk full")
    monkeypatch.setattr(fixtures, "link_immutable_payload", disk_full)
    root = tmp_path / "new"
    root.mkdir()
    with ImmutableFixtureInputs(root) as inputs, pytest.raises(OSError) as error:
        inputs.copy(source, "data")
    assert error.value.errno == errno.ENOSPC
    assert not (root / "data").exists() and not (root / RECEIPT).exists()


def test_busy_public_source_rejects_before_creating_empty_generation(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from scripts import benchmark_tw_public_feature_assembly as benchmark
    from stockagent.live import tw_public_source_guard as source_guard
    repo = tmp_path / "repo"
    parent = repo / "artifacts/benchmarks"
    parent.mkdir(parents=True)
    source = tmp_path / "source"
    source.mkdir()
    (source / "prices.parquet").write_bytes(b"source")
    monkeypatch.setattr(benchmark, "ROOT", repo)
    monkeypatch.setattr(benchmark.shutil, "disk_usage", lambda _: shutil._ntuple_diskusage(100 << 30, 0, 100 << 30))
    @contextmanager
    def busy(_root):
        raise source_guard.TWPublicSourceUpdatePending(source, source / "writer.lock")
        yield
    monkeypatch.setattr(source_guard, "live_tw_public_source_read_guard", busy)
    root = parent / "new"
    with pytest.raises(source_guard.TWPublicSourceUpdatePending):
        benchmark.capture(root, source, source)
    assert not root.exists()


def _finmind_group_case(tmp_path):
    import copy

    source = tmp_path / 'artifacts/benchmarks/suite/finmind-group-status-abba-v2-20261015-fixture'
    source.mkdir(parents=True)
    paths = fixtures._finmind_group_receipts(source)
    code = ['benchmark_finmind_group_status.py', 'download_finmind_complement.py',
            'finmind_updates.py', 'artifact_io.py', 'finmind_status_aggregates.py']
    base = {'schema_version': 1, 'state': 'measurement_complete',
        'fixture': str(source), 'source_queue': '/repo/data_finmind/complement/queue.sqlite3',
        'task_count': 100, 'tasks_per_status': 2, 'rows_published_per_trial': 5,
        'dataset': 'UKStockPrice', 'snapshot_sha256': 'a' * 64,
        'preparation_wall_seconds': 1.0, 'exact_output_parity': True,
        'all_task_values_equal': True, 'source_files_stable': True, 'code_stable': True,
        'provider_calls': 0, 'production_queue_writes': 0, 'production_deployed': False,
        'code_sha256': {'/repo/' + name: 'b' * 64 for name in code},
        'initial_full_status': {name: {'status_sha256': 'c' * 64, 'wall_seconds': 1.,
            'cpu_seconds': 1.} for name in ('baseline', 'candidate')},
        'source_receipts': [{'rows': n, 'sha256': 'd' * 64, 'parquet_sha256': 'e' * 64}
                            for n in (2, 3)],
        'trials': [{'label': name, 'ordinal': ordinal, 'wall_seconds': 1., 'cpu_seconds': 1.,
            'whole_status_sha256': str(ordinal) * 64, 'all_receipt_sha256': 'f' * 64,
            'parquet_bytes_sha256': ['a' * 64, 'b' * 64]}
            for name, ordinal in [('baseline', 0), ('candidate', 0), ('candidate', 1), ('baseline', 1)]]}
    objects = {p.name: copy.deepcopy(base) for p in paths}
    objects[paths[1].name].update(prepared_from=str(paths[0]), resource_limits_stable=True)
    for p in paths:
        p.write_text(json.dumps(objects[p.name]))
    return source, paths, objects


def test_original_adjacent_finmind_group_receipts_are_retained_unchanged(tmp_path):
    source, paths, objects = _finmind_group_case(tmp_path)
    originals = {p.name: p.read_bytes() for p in paths}
    result = fixtures.completed_benchmark_evidence(source, destination=tmp_path / 'retained')
    assert result['producer_contract'] == fixtures.FINMIND_GROUP_STATUS_CONTRACT
    assert result['producer_state'] == 'measurement_complete'
    assert {r['path'] for r in result['files']} == set(objects)
    assert list(source.iterdir()) == []
    for p in paths:
        assert p.read_bytes() == originals[p.name]
        assert (tmp_path / 'retained' / p.name).read_bytes() == originals[p.name]
    # The final retained-evidence reader requires the original frozen scope;
    # never infer a different fixture from the producer's own claim.
    retained = tmp_path / 'retained-evidence'
    (tmp_path / 'retained').rename(retained)
    source.rmdir()
    assert fixtures.completed_benchmark_evidence(retained,
        required_contract=fixtures.FINMIND_GROUP_STATUS_CONTRACT, original_source=source) == result
    with pytest.raises(fixtures.SnapshotError, match='supported completed'):
        fixtures.completed_benchmark_evidence(retained, required_contract=fixtures.FINMIND_GROUP_STATUS_CONTRACT)
    with pytest.raises(fixtures.SnapshotError):
        fixtures.completed_benchmark_evidence(retained, original_source=source)


@pytest.mark.parametrize('defect', [None, 'unowned-path', 'nonce', 'schema', 'state',
    'dataset', 'quarantine', 'relative', 'snapshot', 'manifest', 'writable', 'symlink', 'reappeared'])
def test_adjacent_receipts_require_exact_owned_quarantine_identity(tmp_path, defect):
    import uuid

    source, _, _ = _finmind_group_case(tmp_path)
    expected = fixtures.completed_benchmark_evidence(source)
    dataset = 'legacy-wsl-group'
    state_root = tmp_path / 'retirements'
    nonce = '12-' + uuid.uuid4().hex if defect != 'nonce' else 'arbitrary'
    quarantine = state_root / 'quarantine' / dataset / nonce
    if defect == 'unowned-path':
        quarantine = tmp_path / 'arbitrary'
    quarantine.mkdir(parents=True)
    source.rename(quarantine / 'source')
    relative = 'benchmarks/suite/' + source.name
    state = dict(schema_version=1, dataset=dataset, relative_root=relative, state='retiring',
        quarantine=str(quarantine), snapshot_id='fixed-release', manifest_sha256='a' * 64)
    if defect in {'schema', 'state', 'dataset', 'quarantine', 'snapshot', 'manifest'}:
        key = 'schema_version' if defect == 'schema' else ('snapshot_id' if defect == 'snapshot'
              else 'manifest_sha256' if defect == 'manifest' else defect)
        state[key] = False if defect == 'schema' else ''
    elif defect == 'relative':
        state['relative_root'] = relative.replace('20261015', '20261016')
    if defect == 'reappeared':
        source.mkdir()
    state_root.mkdir(exist_ok=True)
    path = state_root / (dataset + '.json')
    path.write_text(json.dumps(state))
    if defect == 'writable':
        path.chmod(0o666)
    elif defect == 'symlink':
        path.rename(path.with_suffix('.saved'))
        path.symlink_to(path.with_suffix('.saved'))
    read = lambda: fixtures.completed_benchmark_evidence(quarantine / 'source',
        original_source=source, required_contract=fixtures.FINMIND_GROUP_STATUS_CONTRACT)
    if defect:
        with pytest.raises((fixtures.SnapshotError, OSError)):
            read()
    else:
        assert read() == expected
    assert (quarantine / 'source').is_dir()


@pytest.mark.parametrize('change', [
    'missing_trial', 'duplicate_ordinal', 'status_hash', 'receipt_hash', 'parquet_hash',
    'false_parity', 'task_values', 'source_stability', 'code_stability', 'provider_call',
    'queue_write', 'boolean_calls', 'deployed', 'fixture', 'prepared_from', 'resources',
    'snapshot', 'initial', 'source_rows', 'source_sha', 'code_sha', 'nan_timing'])
def test_adjacent_finmind_completion_rejects_incomplete_or_changed_comparison(tmp_path, change):
    from stockagent.data_sync.desync_snapshots import SnapshotError

    source, paths, objects = _finmind_group_case(tmp_path)
    row = objects[paths[1].name]
    if change == 'missing_trial': row['trials'].pop()
    elif change == 'duplicate_ordinal': row['trials'][2]['ordinal'] = 0
    elif change == 'status_hash': row['trials'][1]['whole_status_sha256'] = 'e' * 64
    elif change == 'receipt_hash': row['trials'][1]['all_receipt_sha256'] = 'e' * 64
    elif change == 'parquet_hash': row['trials'][1]['parquet_bytes_sha256'][0] = 'e' * 64
    elif change == 'false_parity': row['exact_output_parity'] = False
    elif change == 'task_values': row['all_task_values_equal'] = False
    elif change == 'source_stability': row['source_files_stable'] = False
    elif change == 'code_stability': row['code_stable'] = False
    elif change == 'provider_call': row['provider_calls'] = 1
    elif change == 'queue_write': row['production_queue_writes'] = 1
    elif change == 'boolean_calls': row['provider_calls'] = False
    elif change == 'deployed': row['production_deployed'] = True
    elif change == 'fixture': row['fixture'] += '-other'
    elif change == 'prepared_from': row['prepared_from'] += '.other'
    elif change == 'resources': row['resource_limits_stable'] = False
    elif change == 'snapshot': row['snapshot_sha256'] = 'unknown'
    elif change == 'initial': row['initial_full_status']['candidate']['status_sha256'] = 'e' * 64
    elif change == 'source_rows': row['source_receipts'][0]['rows'] = 999
    elif change == 'source_sha': row['source_receipts'][0]['sha256'] = 'unknown'
    elif change == 'code_sha': row['code_sha256']['/repo/artifact_io.py'] = 'unknown'
    elif change == 'nan_timing': row['trials'][0]['wall_seconds'] = float('nan')
    paths[1].write_text(json.dumps(row))
    with pytest.raises(SnapshotError, match='supported completed'):
        fixtures.completed_benchmark_evidence(source)


def test_adjacent_comparison_never_falls_back_to_missing_v3_or_embedded_alias(tmp_path):
    source, paths, objects = _finmind_group_case(tmp_path)
    paths[1].unlink()
    with pytest.raises(FileNotFoundError):
        fixtures.completed_benchmark_evidence(source)
    paths[1].write_text(json.dumps(objects[paths[1].name]))
    (source / paths[0].name).write_bytes(paths[0].read_bytes())
    with pytest.raises(fixtures.SnapshotError, match='collides'):
        fixtures.completed_benchmark_evidence(source)
