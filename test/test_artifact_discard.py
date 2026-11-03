import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from stockagent.data_sync import artifact_discard as d
from stockagent.data_sync.desync_snapshots import SnapshotError


def policy(tmp_path, targets=None):
    body = {"contract": d.CONTRACT, "authority_node_id": "penguin", "origin_node_id": "vastai1T",
            "authorization": "user explicitly discards testing outputs and cold copies",
            "decision": "discard_without_preservation", "targets": targets or [
                {"base": "artifact", "relative": "smoke/trial", "kind": "directory", "reason": "smoke-test"}]}
    value = {**body, "identity_sha256": d.identity(body)}
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(value))
    return path, d.load_policy(path)


@pytest.fixture
def setup(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    root = repo / "artifacts/smoke/trial"
    root.mkdir(parents=True)
    (root / "report.txt").write_text("unused engineering result")
    (root / "empty").mkdir()
    _, p = policy(tmp_path)
    monkeypatch.setattr(d, "artifact_process_references", lambda *a: [])
    monkeypatch.setattr(d, "active_configuration_references", lambda *a: [])
    monkeypatch.setattr(d, "artifact_service_references", lambda sources, *a: {str(s): [] for s in sources})
    monkeypatch.setattr(d, "shared_inode_references", lambda *a: [])
    monkeypatch.setattr(d, "process_references", lambda *a: [])
    args = dict(repo_root=repo, engineering_root=tmp_path / "engineering", state_root=tmp_path / "state",
                materialized_root=tmp_path / "materialized", cold_root=tmp_path / "cold")
    return p, root, args


def test_exact_unused_test_discard_needs_no_cold_recovery(setup):
    p, root, args = setup
    plan = d.discard_target(p, p["targets"][0], **args)
    assert root.exists() and not plan["blockers"]
    result = d.discard_target(p, p["targets"][0], **args, apply=True,
                              expected_plan_identity=plan["identity_sha256"])
    assert result["deleted"] and not result["recovery_claimed"] and not root.exists()
    assert result["files"] == 1 and result["reclaimed_allocated_bytes"] > 0
    again = d.discard_target(p, p["targets"][0], **args, apply=True)
    assert again["state"] == "already-absent" and again["reclaimed_allocated_bytes"] == 0


def test_dry_run_generation_change_is_rejected(setup):
    p, root, args = setup
    plan = d.discard_target(p, p["targets"][0], **args)
    (root / "new.txt").write_text("new result")
    with pytest.raises(SnapshotError, match="dry run changed"):
        d.discard_target(p, p["targets"][0], **args, apply=True, expected_plan_identity=plan["identity_sha256"])
    assert (root / "new.txt").exists()


@pytest.mark.parametrize("gate", ["artifact_process_references", "active_configuration_references", "shared_inode_references"])
def test_active_training_and_config_or_inode_use_overrides_discard(setup, monkeypatch, gate):
    p, root, args = setup
    plan = d.discard_target(p, p["targets"][0], **args)
    monkeypatch.setattr(d, gate, lambda *a: ["live training"])
    with pytest.raises(SnapshotError, match="protection applies"):
        d.discard_target(p, p["targets"][0], **args, apply=True, expected_plan_identity=plan["identity_sha256"])
    assert root.exists()


def test_shared_external_names_survive_and_are_not_counted_as_reclaimed(setup, tmp_path):
    p, root, args = setup
    external = tmp_path / "formal-result.txt"
    os.link(root / "report.txt", external)
    plan = d.discard_target(p, p["targets"][0], **args)
    result = d.discard_target(p, p["targets"][0], **args, apply=True, expected_plan_identity=plan["identity_sha256"])
    assert not root.exists() and external.read_text() == "unused engineering result"
    assert result["reclaimed_allocated_bytes"] == 0


def test_symlink_escape_is_rejected(setup, tmp_path):
    p, root, args = setup
    (root / "escape").symlink_to(tmp_path)
    plan = d.discard_target(p, p["targets"][0], **args)
    assert "unsupported-or-cross-filesystem-entry" in plan["blockers"]
    with pytest.raises(SnapshotError):
        d.discard_target(p, p["targets"][0], **args, apply=True, expected_plan_identity=plan["identity_sha256"])


def local_testing_policy(tmp_path, relative='benchmarks/closed-case', **changes):
    body = dict(contract=d.LOCAL_TESTING_CONTRACT, authority_node_id='penguin', origin_node_id='penguin',
                authorization='2026-11-02 user explicitly allows discarding performance work copies',
                decision='discard_without_preservation', targets=[dict(base='artifact', relative=relative,
                    kind='directory', reason='benchmark')])
    body.update(changes)
    path = tmp_path / 'local-testing-policy.json'
    path.write_text(json.dumps({**body, 'identity_sha256': d.identity(body)}))
    return d.load_policy(path)


def test_local_performance_discard_unlinks_only_aliases_and_preserves_external_targets(setup, tmp_path):
    _, _, args = setup
    p = local_testing_policy(tmp_path)
    target = p['targets'][0]
    root = args['repo_root'] / 'artifacts' / target['relative']
    root.mkdir(parents=True)
    external = tmp_path / 'original-source'
    external.mkdir()
    (external / 'keep').write_bytes(b'canonical observation')
    (root / 'copy').write_bytes(b'disposable benchmark')
    (root / 'external-directory').symlink_to(external, target_is_directory=True)
    (root / 'external-file').symlink_to(external / 'keep')
    (root / 'dangling').symlink_to(tmp_path / 'absent')
    (root / 'internal').symlink_to('copy')
    dry = d.discard_target(p, target, **args)
    assert not dry['blockers'] and dry['observed']['files'] == 1
    result = d.discard_target(p, target, **args, apply=True, expected_plan_identity=dry['identity_sha256'])
    assert result['deleted'] and not result['recovery_claimed'] and not root.exists()
    assert (external / 'keep').read_bytes() == b'canonical observation'
    assert not d.manifest_discarded({'metadata': {'source_relative_root': target['relative']}}, p)
    with pytest.raises(SnapshotError, match='cannot discard cold'):
        d.cold_discard_plan(args['cold_root'], p, materialized_root=args['materialized_root'])


@pytest.mark.parametrize('gate', ['artifact_process_references', 'active_configuration_references',
                                 'shared_inode_references'])
def test_local_testing_discard_still_protects_active_work(setup, tmp_path, monkeypatch, gate):
    _, _, args = setup
    p = local_testing_policy(tmp_path)
    target = p['targets'][0]
    root = args['repo_root'] / 'artifacts' / target['relative']
    root.mkdir(parents=True)
    (root / 'copy').write_bytes(b'working measurement')
    dry = d.discard_target(p, target, **args)
    monkeypatch.setattr(d, gate, lambda *a: ['active'])
    with pytest.raises(SnapshotError, match='protection applies'):
        d.discard_target(p, target, **args, apply=True, expected_plan_identity=dry['identity_sha256'])
    assert root.exists()


def test_local_testing_discard_rejects_changed_alias_and_allows_unused_fifo(setup, tmp_path):
    _, _, args = setup
    p = local_testing_policy(tmp_path)
    target = p['targets'][0]
    root = args['repo_root'] / 'artifacts' / target['relative']
    root.mkdir(parents=True)
    (root / 'link').symlink_to(tmp_path / 'before')
    dry = d.discard_target(p, target, **args)
    (root / 'link').unlink()
    (root / 'link').symlink_to(tmp_path / 'after')
    with pytest.raises(SnapshotError, match='dry run changed'):
        d.discard_target(p, target, **args, apply=True, expected_plan_identity=dry['identity_sha256'])
    os.mkfifo(root / 'fifo')
    assert 'unsupported-or-cross-filesystem-entry' not in d.discard_target(p, target, **args)['blockers']


def test_only_explicit_local_testing_discard_may_remove_unused_fifos(setup, tmp_path, monkeypatch):
    from scripts import discard_local_testing_artifacts as cleanup
    _, _, args = setup
    p = local_testing_policy(tmp_path)
    target = p['targets'][0]
    root = args['repo_root'] / 'artifacts' / target['relative']
    root.mkdir(parents=True)
    os.mkfifo(root / 'summary.json')
    os.mkfifo(root / 'unused-pipe')
    (root / 'copy').write_bytes(b'closed benchmark')
    original = os.open
    opaque_opens = []
    def inspect_open(path, flags, *a, **kw):
        if str(path).endswith(('summary.json', 'unused-pipe')):
            assert flags & os.O_PATH, 'a disposable FIFO must never be opened as a byte stream'
            opaque_opens.append(str(path))
        return original(path, flags, *a, **kw)
    monkeypatch.setattr(os, 'open', inspect_open)
    cleanup.retain_small_receipts(root, tmp_path / 'retained')
    dry = d.discard_target(p, target, **args)
    assert not dry['blockers']
    result = d.discard_target(p, target, **args, apply=True, expected_plan_identity=dry['identity_sha256'])
    assert result['deleted'] and not root.exists() and len(opaque_opens) == 2


def test_local_testing_discard_retains_bounded_fixture_and_control_receipts(tmp_path):
    from scripts import discard_local_testing_artifacts as cleanup
    root = tmp_path / 'benchmark'
    root.mkdir()
    names = ('fixture.json', 'immutable-inputs.json', 'current-all-file-actual-open-count-control.json')
    for name in names:
        (root / name).write_bytes(b'{"result":true}')
    (root / 'large-payload').write_bytes(b'not a receipt')
    retained = tmp_path / 'retained'
    cleanup.retain_small_receipts(root, retained)
    assert all((retained / name).read_bytes() == b'{"result":true}' for name in names)
    assert not (retained / 'large-payload').exists()


def test_general_discard_still_rejects_fifo_and_local_discard_rejects_devices(setup, tmp_path):
    import stat
    p, root, args = setup
    os.mkfifo(root / 'fifo')
    assert 'unsupported-or-cross-filesystem-entry' in d.discard_target(p, p['targets'][0], **args)['blockers']
    observed = d._observed(root, 'directory')
    observed['rows'][0]['kind'] = 'unsupported'
    observed['rows'][0]['signature'][5] = stat.S_IFCHR | 0o600
    assert 'unsupported-or-cross-filesystem-entry' in d._gates(root, observed, args['repo_root'],
        args['materialized_root'], args['cold_root'], allow_opaque_symlinks=True)


def test_disposable_posix_names_roundtrip_in_durable_discard_evidence(setup, tmp_path):
    _, _, args = setup
    p = local_testing_policy(tmp_path)
    target = p['targets'][0]
    root = args['repo_root'] / 'artifacts' / target['relative']
    root.mkdir(parents=True)
    raw_name = b'undecodable-\xff'
    name = os.fsdecode(raw_name)
    (root / name).write_bytes(b'disposable fixture')
    os.mkfifo(root / (name + '-fifo'))
    (root / (name + '-link')).symlink_to('absent')
    dry = d.discard_target(p, target, **args)
    receipt = tmp_path / 'dry.json'
    d.write_discard_receipt(receipt, dry)
    saved = json.loads(receipt.read_bytes())
    assert saved == dry
    assert os.fsencode(saved['observed']['rows'][0]['path']).startswith(raw_name)
    result = d.discard_target(p, target, **args, apply=True, expected_plan_identity=dry['identity_sha256'])
    assert result['deleted'] and not root.exists()
    journal = args['state_root'] / (d.identity(target) + '.json')
    assert json.loads(journal.read_bytes())['plan']['observed'] == saved['observed']


@pytest.mark.parametrize('relative', ['benchmarks', 'operations', 'markets/formal', 'live/ledger',
                                    'cache/service', '../benchmarks/closed', '/benchmarks/closed'])
def test_local_testing_policy_rejects_sources_and_namespaces(tmp_path, relative):
    with pytest.raises(SnapshotError):
        local_testing_policy(tmp_path, relative)


def test_local_testing_policy_does_not_claim_vast_origin(tmp_path):
    with pytest.raises(SnapshotError):
        local_testing_policy(tmp_path, origin_node_id='vastai1T')


def test_local_cleanup_resumes_completed_counters_and_protects_recreated_output(setup, tmp_path, monkeypatch):
    from scripts import discard_local_testing_artifacts as cleanup
    _, _, args = setup
    monkeypatch.setattr(cleanup, 'COMMON_OWNER', tmp_path / 'common.lock')
    monkeypatch.setattr(d, '_pinned_snapshot_ids', lambda *a: set())
    p = local_testing_policy(tmp_path)
    policy_path = tmp_path / 'local-testing-policy.json'
    root = args['repo_root'] / 'artifacts' / p['targets'][0]['relative']
    root.mkdir(parents=True)
    (root / 'data').write_bytes(b'closed performance copy')
    state = tmp_path / 'cleanup-state'
    first = cleanup.cycle(policy_path, state, apply=True, repo=args['repo_root'])
    assert first['state'] == 'completed' and first['reclaimed_allocated_bytes'] > 0
    # A restart must keep prior receipts even in its first progress write,
    # before the loop has revisited previously completed targets.
    publish = cleanup.atomic_write_json
    observations = []
    def record(path, value):
        if path == state / 'status.json':
            observations.append(value)
        return publish(path, value)
    monkeypatch.setattr(cleanup, 'atomic_write_json', record)
    second = cleanup.cycle(policy_path, state, apply=True, repo=args['repo_root'])
    assert second['reclaimed_allocated_bytes'] == first['reclaimed_allocated_bytes']
    assert observations[0]['reclaimed_allocated_bytes'] == first['reclaimed_allocated_bytes']
    assert observations[0]['targets_completed'] == 1
    assert second['targets_completed'] == 1 and not root.exists()
    root.mkdir()
    (root / 'data').write_bytes(b'new generation')
    recreated = cleanup.cycle(policy_path, state, apply=True, repo=args['repo_root'])
    assert recreated['targets_blocked'] == 1
    assert (root / 'data').read_bytes() == b'new generation'


def test_local_cleanup_busy_owner_retains_target_without_consuming_error_retry(setup, tmp_path, monkeypatch):
    import fcntl
    from scripts import discard_local_testing_artifacts as cleanup
    _, _, args = setup
    lock = tmp_path / 'common.lock'
    monkeypatch.setattr(cleanup, 'COMMON_OWNER', lock)
    monkeypatch.setattr(d, '_pinned_snapshot_ids', lambda *a: set())
    p = local_testing_policy(tmp_path)
    root = args['repo_root'] / 'artifacts' / p['targets'][0]['relative']
    root.mkdir(parents=True)
    (root / 'data').write_bytes(b'closed copy')
    with lock.open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX)
        result = cleanup.cycle(tmp_path / 'local-testing-policy.json', tmp_path / 'cleanup-state',
                               apply=True, repo=args['repo_root'])
    assert result['targets_blocked'] == 1 and root.exists()
    assert result['results'][0]['reason'] == 'canonical_owner_busy'
    assert 'error_attempts' not in result['results'][0]


def test_exhausted_cleanup_retries_only_after_code_repair_and_retains_failure(setup, tmp_path, monkeypatch):
    from scripts import discard_local_testing_artifacts as cleanup
    _, _, args = setup
    monkeypatch.setattr(cleanup, 'COMMON_OWNER', tmp_path / 'common.lock')
    monkeypatch.setattr(d, '_pinned_snapshot_ids', lambda *a: set())
    p = local_testing_policy(tmp_path)
    root = args['repo_root'] / 'artifacts' / p['targets'][0]['relative']
    root.mkdir(parents=True)
    (root / 'data').write_bytes(b'closed copy')
    state = tmp_path / 'cleanup-state'
    policy_path = tmp_path / 'local-testing-policy.json'
    original = d.discard_target
    failures = []
    def fail(*a, **kw):
        failures.append(True)
        raise OSError('fixture encoding failed')
    monkeypatch.setattr(d, 'discard_target', fail)
    for _ in range(4):
        failed = cleanup.cycle(policy_path, state, apply=True, repo=args['repo_root'])
    assert len(failures) == 3 and failed['results'][0]['state'] == 'retry_exhausted'
    monkeypatch.setattr(d, 'discard_target', original)
    monkeypatch.setattr(cleanup, 'EXECUTION_SHA256', 'repaired-implementation')
    fixed = cleanup.cycle(policy_path, state, apply=True, repo=args['repo_root'])
    assert fixed['state'] == 'completed' and not root.exists()
    receipt = json.loads((state / fixed['results'][0]['resumed_error_receipt']).read_bytes())
    assert receipt['previous_error']['error_attempts'] == 3
    assert receipt['previous_error']['detail'] == 'fixture encoding failed'
    assert not fixed['recovery_claimed'] and not fixed['nas_objects_deleted']


@pytest.mark.parametrize('changed', [False, True])
def test_prepared_cross_mount_failure_rolls_back_only_unchanged_source(setup, tmp_path, monkeypatch, changed):
    import errno
    _, _, args = setup
    p = local_testing_policy(tmp_path)
    target = p['targets'][0]
    root = args['repo_root'] / 'artifacts' / target['relative']
    root.mkdir(parents=True)
    (root / 'copy').write_bytes(b'closed comparison')
    dry = d.discard_target(p, target, **args)
    original = Path.rename
    def cross_mount(self, destination):
        if self == root:
            # Local quarantine must be a sibling within this exact mount.
            assert destination.parent.parent == root.parent
            raise OSError(errno.EXDEV, 'controlled cross-mount failure')
        return original(self, destination)
    monkeypatch.setattr(Path, 'rename', cross_mount)
    with pytest.raises(OSError, match='cross-mount'):
        d.discard_target(p, target, **args, apply=True, expected_plan_identity=dry['identity_sha256'])
    if changed:
        (root / 'new').write_bytes(b'new producer generation')
        with pytest.raises(SnapshotError, match='prepared testing quarantine changed'):
            d.restore_unchanged_discard_quarantine(p, target, **args)
    else:
        assert d.restore_unchanged_discard_quarantine(p, target, **args)
        monkeypatch.setattr(Path, 'rename', original)
        new = d.discard_target(p, target, **args)
        assert d.discard_target(p, target, **args, apply=True,
                               expected_plan_identity=new['identity_sha256'])['deleted']


@pytest.mark.parametrize("relative", ["smoke", "../source", "/source", "data_tw_futures/original"])
def test_namespace_and_source_roots_cannot_be_discarded(tmp_path, relative):
    with pytest.raises(SnapshotError):
        policy(tmp_path, [{"base": "artifact", "relative": relative, "kind": "directory", "reason": "smoke-test"}])


def test_explicit_completed_benchmark_discard_uses_the_same_guards(setup, tmp_path):
    _, _, args = setup
    target = dict(base='artifact', relative='benchmarks/futures-abba', kind='directory', reason='benchmark')
    _, p = policy(tmp_path, [target])
    source = args['repo_root'] / 'artifacts' / target['relative']
    source.mkdir(parents=True)
    (source / 'measured-output').write_bytes(b'disposable measurement')
    dry = d.discard_target(p, target, **args)
    result = d.discard_target(p, target, **args, apply=True, expected_plan_identity=dry['identity_sha256'])
    assert result['deleted'] and not result['recovery_claimed'] and not source.exists()


def test_overlapping_and_duplicate_targets_rejected(tmp_path):
    target = {"base": "artifact", "relative": "smoke/trial", "kind": "directory", "reason": "smoke-test"}
    for other in [target, {**target, "relative": "smoke/trial/output"}]:
        with pytest.raises(SnapshotError, match="overlapping"):
            policy(tmp_path, [target, other])


def local_preparation_policy(tmp_path, relative, **changes):
    from stockagent.data_sync.testing_disposition import PREPARATION_CONTRACT
    body = dict(contract=PREPARATION_CONTRACT, authority_node_id="penguin", origin_node_id="penguin",
                authorization="user discards measured temporary preparation outputs; keeps numerical receipts",
                decision="discard_without_preservation", targets=[dict(base="preparation-benchmark",
                    relative=relative, kind="directory", reason="benchmark")])
    body.update(changes)
    path = tmp_path / "local-preparation-policy.json"
    path.write_text(json.dumps({**body, "identity_sha256": d.identity(body)}))
    return d.load_policy(path)


def test_local_preparation_benchmark_reuses_guarded_discard_and_keeps_source_links(setup, tmp_path):
    _, _, args = setup
    relative = "data_futures/preparation_sources/experiment/temporary-measurement"
    p = local_preparation_policy(tmp_path, relative)
    source = args["repo_root"] / relative
    source.mkdir(parents=True)
    (source / "original-alias").write_bytes(b"preserve the canonical original")
    original = args["repo_root"] / "canonical-original"
    os.link(source / "original-alias", original)
    dry = d.discard_target(p, p["targets"][0], **args)
    assert dry["contract"] == p["contract"] and not dry["blockers"]
    result = d.discard_target(p, p["targets"][0], **args, apply=True,
                              expected_plan_identity=dry["identity_sha256"])
    assert result["deleted"] and not source.exists()
    assert original.read_bytes() == b"preserve the canonical original"
    assert result["reclaimed_allocated_bytes"] == 0
    assert not d.artifact_roots(p)
    assert not d.manifest_discarded({"metadata": {"source_relative_root": relative}}, p)


@pytest.mark.parametrize("relative", ["data_futures", "data_futures/preparation_sources",
    "data_futures/preparation_sources/experiment", "data_futures/raw/experiment/result",
    "data_/preparation_sources/experiment/result", "../data_futures/preparation_sources/result",
    "/data_futures/preparation_sources/experiment/result", "artifacts/benchmarks/experiment/result"])
def test_local_preparation_discard_rejects_source_roots_and_other_namespaces(tmp_path, relative):
    with pytest.raises(SnapshotError):
        local_preparation_policy(tmp_path, relative)


def test_local_preparation_disposition_cannot_claim_remote_origin(tmp_path):
    with pytest.raises(SnapshotError):
        local_preparation_policy(tmp_path, "data_futures/preparation_sources/experiment/result",
                                 origin_node_id="vastai1T")


@pytest.mark.parametrize("gate", ["artifact_process_references", "active_configuration_references",
                                 "shared_inode_references"])
def test_local_preparation_live_gates_override_exact_disposition(setup, tmp_path, monkeypatch, gate):
    _, _, args = setup
    relative = "data_futures/preparation_sources/experiment/result"
    p = local_preparation_policy(tmp_path, relative)
    source = args["repo_root"] / relative
    source.mkdir(parents=True)
    (source / "result").write_bytes(b"keep while referenced")
    dry = d.discard_target(p, p["targets"][0], **args)
    monkeypatch.setattr(d, gate, lambda *a: ["active consumer"])
    with pytest.raises(SnapshotError, match="protection applies"):
        d.discard_target(p, p["targets"][0], **args, apply=True,
                         expected_plan_identity=dry["identity_sha256"])
    assert source.exists()


def test_pinned_preparation_source_is_protected_by_common_gate(setup, tmp_path, monkeypatch):
    _, _, args = setup
    relative = "data_futures/preparation_sources/experiment/result"
    p = local_preparation_policy(tmp_path, relative)
    source = args["repo_root"] / relative
    source.mkdir(parents=True)
    (source / "result").write_bytes(b"pinned prepared input")
    manifest = tmp_path / "pinned-manifest.json"
    manifest.write_text(json.dumps({"metadata": {"source_relative_root": relative}}))
    monkeypatch.setattr(d, "_pinned_snapshot_ids", lambda *a: {"pinned"})
    monkeypatch.setattr(d, "_pinned_manifest_paths", lambda *a: [manifest])
    dry = d.discard_target(p, p["targets"][0], **args)
    assert dry["blockers"] == ["pinned-release"]
    with pytest.raises(SnapshotError, match="protection applies"):
        d.discard_target(p, p["targets"][0], **args, apply=True,
                         expected_plan_identity=dry["identity_sha256"])
    assert source.exists()


def test_only_matching_test_cold_manifest_is_disposed(tmp_path):
    _, p = policy(tmp_path)
    assert d.manifest_discarded({"metadata": {"source_relative_root": "smoke/trial"}}, p)
    assert d.manifest_discarded({"metadata": {"source_relative_root": "smoke/trial/cache"}}, p)
    assert not d.manifest_discarded({"metadata": {"source_relative_root": "smoke/other"}}, p)
    assert not d.manifest_discarded({"metadata": {"source_relative_root": "markets/formal"}}, p)


def test_individual_engineering_file_with_other_links(tmp_path, monkeypatch):
    name = "stockagent-remote-build-measurement-fixture/measurements/trial/first.parquet"
    _, p = policy(tmp_path, [{"base": "engineering", "relative": name, "kind": "file", "reason": "benchmark"}])
    source = tmp_path / "engineering" / name
    source.parent.mkdir(parents=True)
    source.write_bytes(b"test parquet fixture")
    peer = source.with_name("keep.parquet")
    os.link(source, peer)
    monkeypatch.setattr(d, "_gates", lambda *a: [])
    monkeypatch.setattr(d, "process_references", lambda *a: [])
    args = dict(repo_root=tmp_path / "repo", engineering_root=tmp_path / "engineering", state_root=tmp_path / "state",
                materialized_root=tmp_path / "materialized", cold_root=tmp_path / "cold")
    plan = d.discard_target(p, p["targets"][0], **args)
    result = d.discard_target(p, p["targets"][0], **args, apply=True, expected_plan_identity=plan["identity_sha256"])
    assert not source.exists() and peer.exists() and result["reclaimed_allocated_bytes"] == 0


def test_directory_rename_size_drift_is_not_content_change():
    before = [64, 123, 24, 99, 98, 16893, 3]
    after = [64, 123, 32, 99, 101, 16893, 3]
    assert d._same_root_before_after_rename(before, after, "directory")
    assert not d._same_root_before_after_rename(before, after, "file")
    assert not d._same_root_before_after_rename(before, [64, 124, *after[2:]], "directory")


def test_only_unchanged_owned_quarantine_can_resume(setup, monkeypatch):
    p, root, args = setup
    plan = d.discard_target(p, p["targets"][0], **args)
    monkeypatch.setattr(d, "process_references", lambda *a: ["temporary reader"])
    with pytest.raises(SnapshotError, match="quarantined"):
        d.discard_target(p, p["targets"][0], **args, apply=True, expected_plan_identity=plan["identity_sha256"])
    assert not root.exists()
    monkeypatch.setattr(d, "process_references", lambda *a: [])
    assert d.restore_unchanged_discard_quarantine(p, p["targets"][0], **args)
    assert (root / "report.txt").exists()
    plan = d.discard_target(p, p["targets"][0], **args)
    result = d.discard_target(p, p["targets"][0], **args, apply=True, expected_plan_identity=plan["identity_sha256"])
    assert result["deleted"] and not root.exists()


def cold_fixture(tmp_path):
    from stockagent.data_sync.packed_snapshots import initialize_packed_layout, publish_packed_snapshot
    cold = tmp_path / "cold"
    initialize_packed_layout(cold, node_id="penguin")
    source = tmp_path / "source"
    source.mkdir()
    (source / "shared.txt").write_text("shared original observation")
    (source / "formal.txt").write_text("retained formal result")
    formal = publish_packed_snapshot(cold, "formal", source, loose_file_threshold_bytes=1)
    (source / "formal.txt").unlink()
    (source / "test.txt").write_text("discard this test result")
    test = publish_packed_snapshot(cold, "testing", source, loose_file_threshold_bytes=1,
                                  metadata={"source_relative_root": "smoke/trial"})
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"datasets": []}))
    path, p = policy(tmp_path)
    return cold, formal, test, catalog, path, p


def test_cold_discard_keeps_shared_formal_objects_and_manifest_receipts(tmp_path):
    from stockagent.data_sync.packed_snapshots import fetch_packed_snapshot
    from stockagent.data_sync.backup_stream import capture_catalog
    cold, formal, test, catalog, path, p = cold_fixture(tmp_path)
    before = capture_catalog(cold, catalog, testing_disposition=path, include_unreferenced_objects=True)
    assert len(before["releases"]) == 1 and len(before["discarded_testing_releases"]) == 1
    assert not before["missing_objects"] and not before["metadata_errors"]
    plan = d.cold_discard_plan(cold, p, materialized_root=tmp_path / "materialized")
    assert plan["objects"] and plan["retained_shared_objects"] and not plan["blockers"]
    result = d.apply_cold_discard(cold, p, plan, materialized_root=tmp_path / "materialized", receipt=tmp_path / "receipt.json")
    assert result["reclaimed_allocated_bytes"] > 0 and not result["nas_deletion_claimed"]
    assert (cold / plan["releases"][0]["manifest_relative"]).exists()
    recovered = fetch_packed_snapshot(cold, tmp_path / "restore", formal)
    assert (recovered / "formal.txt").read_text() == "retained formal result"
    after = capture_catalog(cold, catalog, testing_disposition=path, include_unreferenced_objects=True)
    assert not after["missing_objects"] and not after["metadata_errors"]
    assert len(after["releases"]) == 1
    assert not any(r["relative"] in after["discarded_testing_objects"] for r in after["files"])


def test_new_formal_reference_prevents_stale_cold_discard(tmp_path):
    from stockagent.data_sync.packed_snapshots import publish_packed_snapshot
    cold, formal, test, catalog, path, p = cold_fixture(tmp_path)
    plan = d.cold_discard_plan(cold, p, materialized_root=tmp_path / "materialized")
    publish_packed_snapshot(cold, "another-formal", tmp_path / "source", loose_file_threshold_bytes=1)
    with pytest.raises(SnapshotError, match="graph changed"):
        d.apply_cold_discard(cold, p, plan, materialized_root=tmp_path / "materialized", receipt=tmp_path / "receipt.json")
    assert all((cold / r["relative"]).exists() for r in plan["objects"])


def test_pinned_cold_testing_version_is_protected(tmp_path, monkeypatch):
    cold, formal, test, catalog, path, p = cold_fixture(tmp_path)
    monkeypatch.setattr(d, "_pinned_snapshot_ids", lambda *a: {test.manifest["snapshot_id"]})
    plan = d.cold_discard_plan(cold, p, materialized_root=tmp_path / "materialized")
    assert any(b.startswith("pinned-testing-release:") for b in plan["blockers"])
    with pytest.raises(SnapshotError, match="protection applies"):
        d.apply_cold_discard(cold, p, plan, materialized_root=tmp_path / "materialized", receipt=tmp_path / "receipt.json")


def test_disposition_runs_without_training_or_site_packages():
    root = Path(__file__).resolve().parents[1]
    code = "from stockagent.data_sync.testing_disposition import DEFAULT_POLICY,load_policy; import sys; load_policy(DEFAULT_POLICY); assert 'numpy' not in sys.modules and 'torch' not in sys.modules"
    result = subprocess.run([sys.executable, "-S", "-c", code], cwd=root, text=True, capture_output=True)
    assert result.returncode == 0, result.stderr


def test_same_process_cold_audit_rechecks_new_references_and_age(tmp_path):
    import time
    from stockagent.data_sync.packed_snapshots import publish_packed_snapshot
    cold, formal, test, catalog, path, p = cold_fixture(tmp_path)
    plan = d.cold_discard_plan(cold, p, materialized_root=tmp_path / "materialized")
    publish_packed_snapshot(cold, "formal-new", tmp_path / "source", loose_file_threshold_bytes=1)
    with pytest.raises(SnapshotError, match="generation changed"):
        d.apply_cold_discard(cold, p, plan, materialized_root=tmp_path / "materialized",
                            receipt=tmp_path / "receipt.json", _audit_started_monotonic=time.monotonic())
    with pytest.raises(SnapshotError, match="expired"):
        d.apply_cold_discard(cold, p, plan, materialized_root=tmp_path / "materialized",
                            receipt=tmp_path / "receipt.json", _audit_started_monotonic=time.monotonic()-1501)
    assert all((cold/r["relative"]).exists() for r in plan["objects"])


def test_shared_blob_descriptor_statistics_do_not_change_cas_identity(tmp_path):
    from stockagent.data_sync.packed_snapshots import initialize_packed_layout, publish_packed_snapshot
    cold=tmp_path/'cold';initialize_packed_layout(cold,node_id='penguin')
    source=tmp_path/'source';source.mkdir();(source/'one.bin').write_bytes(b'unused same testing blob')
    publish_packed_snapshot(cold,'testing-one',source,loose_file_threshold_bytes=1,
                           metadata={'source_relative_root':'smoke/trial'})
    (source/'two.bin').write_bytes((source/'one.bin').read_bytes())
    publish_packed_snapshot(cold,'testing-two',source,loose_file_threshold_bytes=1,
                           metadata={'source_relative_root':'smoke/trial'})
    _,p=policy(tmp_path)
    plan=d.cold_discard_plan(cold,p,materialized_root=tmp_path/'materialized')
    assert len(plan['releases'])==2 and len({r['relative'] for r in plan['objects']})==len(plan['objects'])


def test_corrupt_discarded_testing_head_is_still_a_catalog_error(tmp_path):
    from stockagent.data_sync.backup_stream import capture_catalog
    cold,formal,test,catalog,path,p=cold_fixture(tmp_path)
    head=cold/'heads/testing/penguin.json';value=json.loads(head.read_bytes());value['snapshot_id']='invalid-snapshot'
    head.write_text(json.dumps(value))
    captured=capture_catalog(cold,catalog,testing_disposition=path)
    assert any(r['relative']=='heads/testing/penguin.json' for r in captured['metadata_errors'])
