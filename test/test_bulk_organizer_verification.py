import json
import time
from contextlib import contextmanager
import fcntl
from types import SimpleNamespace

import pytest

from scripts import organize_vast_bulk_archives as organizer
from scripts.status_vast_bulk_return import transport_phase
from scripts.handoff_remote_legacy_archive_worker import BULK_STATE, validate_worker_argv, safe_boundary
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync import remote_legacy_return as legacy_return


def test_read_only_decode_does_not_overwrite_or_skip_retirement_journal(tmp_path, monkeypatch):
    journal = tmp_path / "markets.organization.json"
    original = {"state": "cold_preserved_retirement_audited", "roots": {"markets/run": {"deleted": True}}}
    journal.write_text(json.dumps(original))
    receipt = {"scope": "markets", "payload": str(tmp_path / "markets.tar.zst"),
               "compressed_sha256": "a" * 64}
    def decode(*args, **kwargs):
        kwargs["progress"]({"state": "decoding_all_originals", "compressed_read_bytes": 42})
        return {"compressed_sha256": "a" * 64, "files": 1, "logical_bytes": 100, "rows": []}
    monkeypatch.setattr(organizer, "index_zstd", decode)
    assert organizer.organize(SimpleNamespace(apply=False), tmp_path, receipt)
    assert json.loads(journal.read_text()) == original
    assert (tmp_path / "markets.original-index.json").exists()
    assert (tmp_path / "markets.verification.json").exists()
    assert json.loads((tmp_path / "markets.verification.json").read_text())["state"] == "received_originals_verified_read_only"


def test_read_only_retained_index_still_requires_exact_received_sha(tmp_path):
    (tmp_path / "cache.original-index.json").write_text(json.dumps({"compressed_sha256": "b" * 64}))
    receipt = {"scope": "cache", "compressed_sha256": "a" * 64}
    with pytest.raises(SnapshotError, match="differs"):
        organizer.organize(SimpleNamespace(apply=False), tmp_path, receipt)


@pytest.mark.parametrize("state", ["transport_failed_partial_retained",
                                 "compressed_transport_received", "transport_incomplete_source_preserved"])
def test_retained_or_finished_transport_is_not_live_verified_append(state):
    assert transport_phase({"state": state, "resumed_prefix_bytes": 10}, 20) == state


def test_live_prefix_verification_and_append_are_distinct():
    receipt = {"state": "receiving", "resumed_prefix_bytes": 10}
    assert transport_phase(receipt, 10) == "exact_prefix_verification"
    assert transport_phase(receipt, 20) == "verified_tail_append"
    assert transport_phase({"state": "receiving"}, 20) == "receiving"


def test_bulk_handoff_recognizes_only_the_exact_default_retirement_program():
    argv = ["python", "scripts/organize_vast_bulk_archives.py", "--watch", "--apply", "--retire"]
    validate_worker_argv(argv, BULK_STATE)
    for invalid in (argv[:-1], argv + ["--batch", "/other"], ["python", "other.py"]):
        with pytest.raises(SnapshotError):
            validate_worker_argv(invalid, BULK_STATE)


def test_bulk_handoff_accepts_only_the_exact_retained_batch_options():
    argv = ["python", "scripts/organize_vast_bulk_archives.py", "--watch", "--apply", "--retire"]
    validate_worker_argv(argv + ["--batch", str(BULK_STATE / "20261004T094634-a473273e07ba")], BULK_STATE)
    for options in (["--ssh-target", "root@other"], ["--batch", str(BULK_STATE / "other")], ["--apply"]):
        with pytest.raises(SnapshotError):
            validate_worker_argv(argv + options, BULK_STATE)


def test_batch_discovery_accepts_later_dates_but_never_redirects(tmp_path, monkeypatch):
    monkeypatch.setattr(organizer, "INCOMING", tmp_path)
    current = tmp_path / "20261005T031234-abcdef012345"
    current.mkdir()
    (tmp_path / "20261004T031234-abcdef012345").symlink_to(current, target_is_directory=True)
    (tmp_path / "20261005T031234-bad").mkdir()
    assert organizer.received_batches() == [current]


def test_partial_retirement_is_retried_with_same_cold_proof(tmp_path, monkeypatch):
    state = {"state": "cold_preserved_retirement_audited", "all_sources_retired": False,
             "roots": {"cache/one": {"deleted": False, "state": "source-protected"}}}
    (tmp_path / "cache.organization.json").write_text(json.dumps(state))
    (tmp_path / "cache.original-index.json").write_text(json.dumps({"compressed_sha256": "a" * 64}))
    proof = {"dataset": "exact", "snapshot_id": "fixed", "manifest_sha256": "b" * 64,
             "cold_verified": True, "decoded_originals_verified": True}
    (tmp_path / "cache.cold-proof.json").write_text(json.dumps(proof))
    calls = []
    @contextmanager
    def owner(*, wait):
        assert wait is False
        calls.append("owner")
        yield
    monkeypatch.setattr(organizer, "ingress_owner", owner)
    monkeypatch.setattr(organizer, "verify_retained_proof", lambda received: calls.append(received))
    def retire(*args):
        assert args[4] == proof
        calls.append("retire")
        return False
    monkeypatch.setattr(organizer, "retire_roots", retire)
    monkeypatch.setattr(organizer, "publish_preservation", lambda *_: pytest.fail("republished retained proof"))
    receipt = {"scope": "cache", "compressed_sha256": "a" * 64}
    assert organizer.organize(SimpleNamespace(apply=True, retire=True, watch=True), tmp_path, receipt) is False
    assert calls == [proof, "retire"]


def test_unusable_candidates_do_not_refresh_expired_recovery_or_send_retirement(tmp_path, monkeypatch):
    records = [{'relative_root': 'cache/' + name, 'directory_root': True,
                'unsupported': False, 'logical_bytes': 10} for name in ('gone', 'active', 'changed')]
    states = ['source-absent-without-new-retirement', 'source-protected', 'source-changed-from-preservation']
    observed = {r['relative_root']: {'state': state, 'deleted': False, 'candidate_only': True}
                for r, state in zip(records, states)}
    monkeypatch.setattr(organizer, 'root_records', lambda _: records)
    monkeypatch.setattr(organizer, 'private_observe_roots', lambda *_: observed)
    monkeypatch.setattr(organizer, 'verify_retained_proof', lambda *_: pytest.fail('reread unusable cold cohort'))
    monkeypatch.setattr(organizer, 'index_zstd', lambda *_: pytest.fail('refreshed recovery from unusable source'))
    monkeypatch.setattr(organizer, 'private_remote', lambda *_args, **_kwargs: pytest.fail('sent retirement'))
    proof = {'verified_at_epoch': time.time() - 10000}
    before = dict(proof)
    state = {}
    receipt = {'scope': 'cache', 'approved_cache_roots': list(observed)}
    assert not organizer.retire_roots(SimpleNamespace(retire=True), tmp_path, receipt,
                                       {'rows': []}, proof, state)
    assert proof == before and state['all_sources_retired'] is False
    assert all(r['deleted'] is False for r in state['roots'].values())


def test_incomplete_candidate_observation_is_rejected(tmp_path, monkeypatch):
    records = [{'relative_root': 'cache/one', 'directory_root': True,
                'unsupported': False, 'logical_bytes': 10}]
    monkeypatch.setattr(organizer, 'root_records', lambda _: records)
    monkeypatch.setattr(organizer, 'private_observe_roots', lambda *_: {})
    with pytest.raises(SnapshotError, match='omitted or added'):
        organizer.retire_roots(SimpleNamespace(retire=True), tmp_path,
                               {'scope': 'cache', 'approved_cache_roots': ['cache/one']},
                               {'rows': []}, {}, {})


def test_candidate_never_replaces_exact_plan_apply_and_largest_is_processed_first(tmp_path, monkeypatch):
    records = [{'relative_root': 'cache/' + name, 'directory_root': True,
                'unsupported': False, 'logical_bytes': size} for name, size in [('small', 1), ('large', 100)]]
    monkeypatch.setattr(organizer, 'root_records', lambda _: records)
    monkeypatch.setattr(organizer, 'private_observe_roots', lambda *_: {
        r['relative_root']: {'state': 'metadata-candidate-requires-exact-proof',
                             'candidate_only': True, 'deleted': False} for r in records})
    monkeypatch.setattr(organizer, 'verify_retained_proof', lambda *_: {})
    monkeypatch.setattr(organizer, 'credentials', lambda: ('private-local', 'hidden'))
    monkeypatch.setattr(organizer, '_convergence', lambda *_: {'ok': True})
    monkeypatch.setattr(organizer, 'load_policy', lambda: {'shared_file_policy': 'unlink_preserved_names_only'})
    @contextmanager
    def owner(*, wait):
        assert wait is False
        yield
    monkeypatch.setattr(organizer, 'ingress_owner', owner)
    calls = []
    def remote(args, ack, *, apply):
        calls.append((ack['root']['relative_root'], apply))
        return {'state': 'retired' if apply else 'would-retire', 'deleted': apply,
                'reclaimed_allocated_bytes': 10 if apply else 0}
    monkeypatch.setattr(organizer, 'private_remote', remote)
    proof = dict.fromkeys(('dataset', 'snapshot_id', 'manifest_sha256', 'preservation_contract',
                          'compressed_sha256', 'member_inventory_sha256', 'source_scope'), 'fixed')
    proof.update(cold_verified=True, decoded_originals_verified=True, verified_at_epoch=time.time())
    assert organizer.retire_roots(SimpleNamespace(retire=True), tmp_path,
                                  {'scope': 'cache', 'approved_cache_roots': [r['relative_root'] for r in records]},
                                  {'rows': []}, proof, {})
    assert calls == [('cache/large', False), ('cache/large', True),
                     ('cache/small', False), ('cache/small', True)]


def test_publication_is_owned_but_independent_recovery_does_not_block_other_cohorts(tmp_path, monkeypatch):
    index = {"compressed_sha256": "a" * 64}
    (tmp_path / "cache.original-index.json").write_text(json.dumps(index))
    publication = {"dataset": "exact", "snapshot_id": "fixed", "manifest_sha256": "b" * 64,
                   "cold_verified": False, "decoded_originals_verified": False}
    proof = {**publication, "cold_verified": True, "decoded_originals_verified": True}
    held, calls = [], []
    @contextmanager
    def owner(*, wait):
        assert wait is False and not held
        held.append(True)
        try:
            yield
        finally:
            held.clear()
    def publish(*args, **kwargs):
        assert held and kwargs["defer_recovery"] is True
        calls.append("publish")
        return publication
    def recover(published, originals, **kwargs):
        assert not held and published == publication and originals == index
        calls.append("recover")
        return proof
    def retire(*args):
        assert not held and args[4] == proof
        calls.append("retire")
        return False
    monkeypatch.setattr(organizer, "ingress_owner", owner)
    monkeypatch.setattr(organizer, "publish_preservation", publish)
    monkeypatch.setattr(organizer, "verify_preservation", recover)
    monkeypatch.setattr(organizer, "retire_roots", retire)
    assert not organizer.organize(SimpleNamespace(apply=True, retire=True), tmp_path,
                                  {"scope": "cache", "compressed_sha256": "a" * 64})
    assert calls == ["publish", "recover", "retire"]
    assert json.loads((tmp_path / "cache.cold-publication.json").read_text())["cold_verified"] is False
    assert json.loads((tmp_path / "cache.cold-proof.json").read_text())["cold_verified"] is True


def test_false_recovery_receipt_never_enters_source_retirement(tmp_path, monkeypatch):
    (tmp_path / "cache.original-index.json").write_text(json.dumps({"compressed_sha256": "a" * 64}))
    (tmp_path / "cache.cold-proof.json").write_text(json.dumps({"cold_verified": False}))
    monkeypatch.setattr(organizer, "retire_roots", lambda *_: pytest.fail("unverified source retirement"))
    with pytest.raises(SnapshotError, match="publication receipt"):
        organizer.organize(SimpleNamespace(apply=True, retire=True), tmp_path,
                           {"scope": "cache", "compressed_sha256": "a" * 64})


@pytest.mark.parametrize("recovery_fails", [False, True])
def test_stale_inode_proof_requires_complete_recovery_and_preserves_old_evidence(
        tmp_path, monkeypatch, recovery_fails):
    index = {"compressed_sha256": "a" * 64}
    (tmp_path / "cache.original-index.json").write_text(json.dumps(index))
    proof = {"dataset": "exact", "snapshot_id": "fixed", "manifest_sha256": "b" * 64,
             "cold_verified": True, "decoded_originals_verified": True, "verified_at_epoch": 1}
    proof_path = tmp_path / "cache.cold-proof.json"
    proof_path.write_text(json.dumps(proof))
    original_bytes = proof_path.read_bytes()
    refreshed = {**proof, "verified_at_epoch": 2}
    calls = []

    def retained(received):
        calls.append(("inode", received["verified_at_epoch"]))
        if received == proof:
            raise organizer.PreservationProofExpired("changed after independent decode")

    def recover(publication, originals, **kwargs):
        assert publication == proof and originals == index
        calls.append(("complete_recovery", publication["snapshot_id"]))
        if recovery_fails:
            raise SnapshotError("canonical original decode differs")
        return refreshed

    def retire(*args):
        assert args[4] == refreshed
        assert json.loads(proof_path.read_bytes()) == refreshed
        calls.append(("retire", refreshed["snapshot_id"]))
        return False

    monkeypatch.setattr(organizer, "verify_retained_proof", retained)
    monkeypatch.setattr(organizer, "verify_preservation", recover)
    monkeypatch.setattr(organizer, "retire_roots", retire)
    monkeypatch.setattr(organizer, "publish_preservation", lambda *_: pytest.fail("republished preserved data"))
    args, receipt = SimpleNamespace(apply=True, retire=True), {"scope": "cache", "compressed_sha256": "a" * 64}
    if recovery_fails:
        with pytest.raises(SnapshotError, match="original decode"):
            organizer.organize(args, tmp_path, receipt)
        assert proof_path.read_bytes() == original_bytes
        assert calls == [("inode", 1), ("complete_recovery", "fixed")]
    else:
        assert organizer.organize(args, tmp_path, receipt) is False
        assert calls == [("inode", 1), ("complete_recovery", "fixed"), ("inode", 2), ("retire", "fixed")]
    prior = tmp_path / ("cache.cold-proof-" + organizer.digest(proof) + ".json")
    assert prior.read_bytes() == original_bytes


def test_canonical_identity_failure_cannot_refresh_or_retire(tmp_path, monkeypatch):
    (tmp_path / "cache.original-index.json").write_text(json.dumps({"compressed_sha256": "a" * 64}))
    proof = {"cold_verified": True, "decoded_originals_verified": True}
    (tmp_path / "cache.cold-proof.json").write_text(json.dumps(proof))

    def reject(_):
        raise SnapshotError("canonical identity differs")

    monkeypatch.setattr(organizer, "verify_retained_proof", reject)
    monkeypatch.setattr(organizer, "verify_preservation", lambda *_: pytest.fail("refreshed invalid identity"))
    monkeypatch.setattr(organizer, "retire_roots", lambda *_: pytest.fail("retired invalid identity"))
    with pytest.raises(SnapshotError, match="identity differs"):
        organizer.organize(SimpleNamespace(apply=True, retire=True), tmp_path,
                           {"scope": "cache", "compressed_sha256": "a" * 64})
    assert not list(tmp_path.glob("cache.cold-proof-*.json"))


@pytest.mark.parametrize("cohort_code,expected", [(0, 75), (75, 75), (2, 2)])
def test_remote_inventory_unavailable_defers_without_hiding_cohort_failure(tmp_path, monkeypatch, cohort_code, expected):
    import sys
    from scripts import capture_inactive_training_cache as capture
    from scripts.inventory_remote_training_cache import RemoteCacheInventoryUnavailable

    monkeypatch.setattr(organizer, "COORDINATOR_LOCK", tmp_path / "owner.lock")
    monkeypatch.setattr(organizer, "COORDINATOR_STATUS", tmp_path / "private/status.json")
    monkeypatch.setattr(organizer, "native_guard", lambda: None)
    monkeypatch.setattr(organizer.shutil, "disk_usage", lambda *_: SimpleNamespace(free=1024**3))
    monkeypatch.setattr(organizer.socket, "gethostname", lambda: "penguin")
    monkeypatch.setattr(organizer.os, "umask", lambda *_: 0o022)
    monkeypatch.setattr(organizer, "received_batches", lambda: [])
    monkeypatch.setattr(organizer, "run_batches", lambda *_: cohort_code)
    monkeypatch.setattr(sys, "argv", ["organizer", "--apply", "--retire", "--capture-inactive-caches"])

    def disconnected(*_):
        raise RemoteCacheInventoryUnavailable("private SSH failure")

    monkeypatch.setattr(capture, "capture", disconnected)
    assert organizer.main() == expected
    status = json.loads(organizer.COORDINATOR_STATUS.read_bytes())
    assert status["exit_code"] == expected and status["full_history_verified"] is False
    deferred = json.loads((organizer.COORDINATOR_STATUS.parent / "remote-inventory-deferred.json").read_bytes())
    assert deferred["cohort_exit_code"] == cohort_code and deferred["source_deleted"] is False
    assert "private SSH failure" not in json.dumps(deferred)


@pytest.mark.parametrize("exit_code", [255, 1])
def test_inventory_transport_failure_is_typed_and_private(tmp_path, monkeypatch, exit_code):
    from scripts import inventory_remote_training_cache as inventory

    monkeypatch.setattr(inventory.subprocess, "run", lambda *_, **__: SimpleNamespace(
        returncode=exit_code, stdout="", stderr="private transport diagnostic"))
    args = SimpleNamespace(output=tmp_path / "inventory.json", identity_file=tmp_path / "identity",
                           ssh_target="root@host", ssh_port=22)
    args.identity_file.write_text("engineering identity fixture; no real SSH call")
    args.identity_file.chmod(0o600)
    expected = inventory.RemoteCacheInventoryUnavailable if exit_code == 255 else RuntimeError
    with pytest.raises(expected) as caught:
        inventory.observe(args)
    assert isinstance(caught.value, inventory.RemoteCacheInventoryUnavailable) == (exit_code == 255)
    assert "private transport diagnostic" not in str(caught.value)
    assert not args.output.exists()
    assert json.loads(args.output.with_suffix(".error.json").read_bytes())["stderr"] == "private transport diagnostic"


@pytest.mark.parametrize("message,is_unavailable", [
    ("ssh: connect to host example.invalid port 22: No route to host\n", True),
    ("ssh: connect to host example.invalid port 22: Network is unreachable\n", True),
    ("ssh: connect to host example.invalid port 22: Connection timed out\n", True),
    ("ssh: connect to host example.invalid port 22: Connection refused\n", True),
    ("Permission denied (publickey).\n", False),
    ("ssh: connect to host example.invalid port 22: Connection reset by peer\n", False),
    ("remote program failed\nssh: connect to host example.invalid port 22: No route to host\n", False),
])
def test_retirement_defers_only_proven_preconnection_failures(tmp_path, monkeypatch, message, is_unavailable):
    monkeypatch.setattr(organizer, "ROOT", tmp_path)
    monkeypatch.setattr(organizer, "_ssh_base", lambda *_: ["ssh"])
    monkeypatch.setattr(organizer, "_validate_ssh_target", lambda target: target)
    monkeypatch.setattr(organizer.subprocess, "run", lambda *_, **__: SimpleNamespace(
        returncode=255, stdout="", stderr=message))
    args = SimpleNamespace(identity_file=tmp_path / "key", ssh_port=22, ssh_target="root@example.invalid")
    with pytest.raises(SnapshotError) as caught:
        organizer._private_request(args, "unused", "a" * 64)
    assert isinstance(caught.value, organizer.RemotePreservationUnavailable) == is_unavailable
    assert "example.invalid" not in str(caught.value)
    receipts = list((tmp_path / "artifacts/operations/vast_bulk_return_20261004/retirement-errors").glob("*.json"))
    assert len(receipts) == 1
    assert json.loads(receipts[0].read_bytes())["stderr"] == message


def test_preconnection_text_with_other_exit_code_is_still_a_hard_error(tmp_path, monkeypatch):
    monkeypatch.setattr(organizer, "ROOT", tmp_path)
    monkeypatch.setattr(organizer, "_ssh_base", lambda *_: ["ssh"])
    monkeypatch.setattr(organizer, "_validate_ssh_target", lambda target: target)
    monkeypatch.setattr(organizer.subprocess, "run", lambda *_, **__: SimpleNamespace(
        returncode=1, stdout="", stderr="ssh: connect to host host port 22: No route to host\n"))
    args = SimpleNamespace(identity_file=tmp_path / "key", ssh_port=22, ssh_target="root@host")
    with pytest.raises(SnapshotError) as caught:
        organizer._private_request(args, "unused", "a" * 64)
    assert not isinstance(caught.value, organizer.RemotePreservationUnavailable)


def test_remote_outage_defers_remaining_retirements_and_retries_next_cycle(tmp_path, monkeypatch):
    monkeypatch.setattr(organizer, "INCOMING", tmp_path)
    batches = []
    for number in range(2):
        directory = tmp_path / f"20261006T00000{number}-abcdef012345"
        directory.mkdir()
        (directory / "intent.json").write_text(json.dumps({
            "authority_node_id": "penguin", "origin_node_id": "vastai1T", "scopes": ["cache"]}))
        (directory / "cache.receipt.json").write_text(json.dumps({
            "scope": "cache", "state": "compressed_transport_received",
            "completed_at_epoch": 1, "compressed_sha256": "a" * 64}))
        batches.append(directory)
    calls = []
    reachable = False

    def process(args, directory, receipt):
        phase = "preserve" if getattr(args, "_preserve_only", False) else "retire"
        calls.append((phase, directory))
        if phase == "retire" and not reachable:
            raise organizer.RemotePreservationUnavailable("preconnection; private receipt")
        return True

    monkeypatch.setattr(organizer, "organize", process)
    args = SimpleNamespace(batch=batches, watch=False, apply=True, retire=True)
    assert organizer.run_batches(args) == 75
    assert calls == [("preserve", p) for p in batches] + [("retire", batches[0])]
    deferred = json.loads((batches[0] / "cache.organization-deferred.json").read_bytes())
    assert deferred["source_deleted_by_failed_request"] is False
    assert not (batches[0] / "cache.organization-error.json").exists()

    calls.clear()
    reachable = True
    assert organizer.run_batches(args) == 0
    assert calls == [("preserve", p) for p in batches] + [("retire", p) for p in batches]


def test_remote_outage_does_not_hide_a_prior_preservation_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(organizer, "INCOMING", tmp_path)
    batches = []
    for number in range(2):
        directory = tmp_path / f"20261006T00000{number}-abcdef012345"
        directory.mkdir()
        (directory / "intent.json").write_text(json.dumps({
            "authority_node_id": "penguin", "origin_node_id": "vastai1T", "scopes": ["cache"]}))
        (directory / "cache.receipt.json").write_text(json.dumps({
            "scope": "cache", "state": "compressed_transport_received",
            "completed_at_epoch": 1, "compressed_sha256": "a" * 64}))
        batches.append(directory)

    def process(args, directory, receipt):
        if getattr(args, "_preserve_only", False):
            if directory == batches[0]:
                raise SnapshotError("cold identity mismatch")
            return True
        raise organizer.RemotePreservationUnavailable("preconnection; private receipt")

    monkeypatch.setattr(organizer, "organize", process)
    args = SimpleNamespace(batch=batches, watch=False, apply=True, retire=True)
    assert organizer.run_batches(args) == 2
    assert "cold identity mismatch" in (batches[0] / "cache.organization-error.json").read_text()
    assert json.loads((batches[1] / "cache.organization-deferred.json").read_bytes())["source_deleted_by_failed_request"] is False


def test_missing_manifest_does_not_abort_other_bulk_cohorts(tmp_path, monkeypatch):
    monkeypatch.setattr(organizer, 'INCOMING', tmp_path)
    batches = []
    for number in range(2):
        directory = tmp_path / f'20261006T00000{number}-abcdef012345'
        directory.mkdir()
        (directory / 'intent.json').write_text(json.dumps({
            'authority_node_id': 'penguin', 'origin_node_id': 'vastai1T', 'scopes': ['cache']}))
        (directory / 'cache.receipt.json').write_text(json.dumps({
            'scope': 'cache', 'state': 'compressed_transport_received',
            'completed_at_epoch': 1, 'compressed_sha256': 'a' * 64}))
        batches.append(directory)
    visited = []

    def process(_args, directory, _receipt):
        visited.append(directory)
        if directory == batches[0]:
            raise FileNotFoundError('superseded manifest missing')
        return True

    monkeypatch.setattr(organizer, 'organize', process)
    assert organizer.run_batches(SimpleNamespace(batch=batches, watch=False)) == 2
    assert visited == batches
    status = json.loads((batches[0] / 'cache.organization-error.json').read_bytes())
    assert status['state'] == 'source_preserved_organization_failed'
    assert 'superseded manifest missing' in status['error']


def test_bulk_organizer_waits_for_exact_local_migration_without_claiming_remote_retirement(tmp_path, monkeypatch):
    monkeypatch.setattr(organizer.bulk_landing_alias_retirement, 'organizer_deferred', lambda *_: True)
    monkeypatch.setattr(organizer, 'index_zstd', lambda *_a, **_kw: pytest.fail('unexpected old decode'))
    receipt = {'scope': 'markets', 'compressed_sha256': 'a'*64}
    assert not organizer.organize(SimpleNamespace(apply=True, retire=True), tmp_path, receipt)
    status = json.loads((tmp_path/'markets.organization.json').read_bytes())
    assert status['state'] == 'local_source_migration_reconciliation_pending'
    assert status['independent_recovery_claimed'] is False and status['remote_delete_performed'] is False


def test_bulk_organizer_keeps_completed_remote_status_when_removing_old_local_carrier(tmp_path, monkeypatch):
    (tmp_path/'markets.organization.json').write_text(json.dumps({
        'state': 'cold_preserved_retirement_audited', 'all_sources_retired': True}))
    monkeypatch.setattr(organizer.bulk_landing_alias_retirement, 'organizer_deferred', lambda *_: True)
    monkeypatch.setattr(organizer, 'retire_repacked_original_carrier', lambda *_: {
        'state': 'completed', 'estimated_allocated_bytes_reclaimed': 123})
    assert organizer.organize(SimpleNamespace(apply=True, retire=True), tmp_path,
                              {'scope': 'markets', 'compressed_sha256': 'a'*64})
    value = json.loads((tmp_path/'markets.organization.json').read_bytes())
    assert value['state'] == 'cold_preserved_retirement_audited' and value['all_sources_retired'] is True
    assert value['original_carrier_reclaimed_allocated_bytes'] == 123


def test_busy_retry_does_not_erase_prior_completed_carrier_evidence(tmp_path, monkeypatch):
    (tmp_path/'markets.organization.json').write_text(json.dumps({
        'state': 'local_source_migration_reconciliation_pending', 'original_carrier_state': 'completed',
        'original_carrier_reclaimed_allocated_bytes': 123}))
    monkeypatch.setattr(organizer.bulk_landing_alias_retirement, 'organizer_deferred', lambda *_: True)
    monkeypatch.setattr(organizer, 'retire_repacked_original_carrier', lambda *_: {'state': 'waiting_original_owner'})
    assert not organizer.organize(SimpleNamespace(apply=True, retire=True), tmp_path,
                                  {'scope': 'markets', 'compressed_sha256': 'a'*64})
    value = json.loads((tmp_path/'markets.organization.json').read_bytes())
    assert value['original_carrier_state'] == 'completed'
    assert value['original_carrier_reclaimed_allocated_bytes'] == 123
    assert value['original_carrier_last_attempt'] == 'waiting_original_owner'


def test_original_carrier_scheduler_dry_runs_before_applying_and_yields_to_busy_owner(tmp_path, monkeypatch):
    from scripts import run_bounded_storage_maintenance as maintenance
    monkeypatch.setattr(maintenance, 'allowed_seconds', lambda *_: 1000)
    monkeypatch.setattr(organizer.workspace_volume, 'configuration', lambda: {'mount_point': str(tmp_path)})
    monkeypatch.setattr(organizer, 'metadata_path', lambda value: value)
    calls = []
    def retire(_directory, _receipt, **kwargs):
        calls.append(kwargs['apply'])
        return {'state': 'completed' if kwargs['apply'] else 'planned'}
    monkeypatch.setattr(organizer.bulk_original_carrier_retirement, 'retire', retire)
    args = SimpleNamespace(_coordinator_fd=7)
    assert organizer.retire_repacked_original_carrier(args, tmp_path, {})['state'] == 'completed'
    assert calls == [False, True]
    def busy(*_a, **_kw):
        raise SnapshotError('retirement owner busy: cycle.lock')
    monkeypatch.setattr(organizer.bulk_original_carrier_retirement, 'retire', busy)
    assert organizer.retire_repacked_original_carrier(args, tmp_path, {})['state'] == 'waiting_original_owner'


def test_carrier_reclaim_summary_only_counts_completed_and_exposes_partial_scope(tmp_path, monkeypatch):
    monkeypatch.setattr(organizer, 'received_batches', lambda: [tmp_path])
    monkeypatch.setattr(organizer.workspace_volume, 'configuration', lambda: {'mount_point': str(tmp_path)})
    def complete(_workspace, _batch, scope):
        if scope == 'cache':
            raise SnapshotError('invalid journal')
        return 100 if scope == 'markets' else None
    monkeypatch.setattr(organizer.bulk_original_carrier_retirement, 'completed_reclamation', complete)
    monkeypatch.setattr(organizer.bulk_committed_landing_retirement, 'completed_reclamation',
                        lambda _workspace, _batch, scope: (2, 200) if scope == 'cache' else None)
    value = organizer.original_carrier_reclaim_summary()
    assert value == {'original_carriers_reclaimed': 1, 'original_carrier_reclaimed_allocated_bytes': 100,
                     'landing_parts_reclaimed': 2, 'landing_parts_reclaimed_allocated_bytes': 200,
                     'original_carrier_summary_incomplete': True}


def test_bulk_coordinator_keeps_completed_reclaim_total_while_running(tmp_path, monkeypatch):
    import sys
    monkeypatch.setattr(organizer, 'COORDINATOR_LOCK', tmp_path/'owner.lock')
    monkeypatch.setattr(organizer, 'COORDINATOR_STATUS', tmp_path/'private/status.json')
    monkeypatch.setattr(organizer, 'native_guard', lambda: None)
    monkeypatch.setattr(organizer.socket, 'gethostname', lambda: 'penguin')
    monkeypatch.setattr(organizer.shutil, 'disk_usage', lambda *_: SimpleNamespace(free=1024**3))
    totals = {'original_carriers_reclaimed': 1, 'original_carrier_reclaimed_allocated_bytes': 100,
              'original_carrier_summary_incomplete': False}
    monkeypatch.setattr(organizer, 'original_carrier_reclaim_summary', lambda: totals)
    def run(_args):
        status = json.loads(organizer.COORDINATOR_STATUS.read_bytes())
        assert status['state'] == 'checking_received_cohorts'
        assert status['original_carrier_reclaimed_allocated_bytes'] == 100
        return 75
    monkeypatch.setattr(organizer, 'run_batches', run)
    monkeypatch.setattr(sys, 'argv', ['organizer', '--apply', '--retire'])
    assert organizer.main() == 75


def test_busy_publication_owner_defers_without_ignoring_next_batch(tmp_path, monkeypatch):
    (tmp_path / "cache.original-index.json").write_text(json.dumps({"compressed_sha256": "a" * 64}))
    @contextmanager
    def busy(*, wait):
        assert wait is False
        raise BlockingIOError()
        yield
    monkeypatch.setattr(organizer, "ingress_owner", busy)
    receipt = {"scope": "cache", "compressed_sha256": "a" * 64}
    assert not organizer.organize(SimpleNamespace(apply=True, retire=True, watch=True), tmp_path, receipt)
    assert json.loads((tmp_path / "cache.organization.json").read_text())["state"] == "originals_verified_waiting_existing_ingress_owner"


def test_finished_cohort_skips_and_pending_cohort_returns_deferred(tmp_path, monkeypatch):
    monkeypatch.setattr(organizer, "INCOMING", tmp_path)
    batches = []
    for name in ["20261005T031234-abcdef012345", "20261005T031235-abcdef012345"]:
        directory = tmp_path / name
        directory.mkdir()
        (directory / "intent.json").write_text(json.dumps({"authority_node_id": "penguin", "origin_node_id": "vastai1T", "scopes": ["cache"]}))
        (directory / "cache.receipt.json").write_text(json.dumps({"scope": "cache", "state": "compressed_transport_received", "completed_at_epoch": 1, "compressed_sha256": "a" * 64}))
        batches.append(directory)
    calls = []
    def process(args, directory, receipt):
        calls.append(directory)
        return directory == batches[1]
    monkeypatch.setattr(organizer, "organize", process)
    assert organizer.run_batches(SimpleNamespace(batch=batches, watch=False)) == 75
    assert calls == batches


def test_existing_coordinator_prevents_competing_retirement_journal(tmp_path, monkeypatch):
    import sys
    lock = tmp_path / "owner.lock"
    monkeypatch.setattr(organizer, "COORDINATOR_LOCK", lock)
    monkeypatch.setattr(organizer, "native_guard", lambda: None)
    monkeypatch.setattr(organizer.socket, "gethostname", lambda: "penguin")
    monkeypatch.setattr(organizer.os, "umask", lambda *_: 0o022)
    monkeypatch.setattr(organizer, "run_batches", lambda *_: pytest.fail("second coordinator entered"))
    monkeypatch.setattr(sys, "argv", ["organizer", "--apply", "--retire"])
    with lock.open("a+b") as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        assert organizer.main() == 75


def test_full_cold_volume_defers_before_decoding_or_source_retirement(tmp_path, monkeypatch):
    import sys
    monkeypatch.setattr(organizer, 'COORDINATOR_LOCK', tmp_path / 'owner.lock')
    monkeypatch.setattr(organizer, 'COORDINATOR_STATUS', tmp_path / 'private/status.json')
    monkeypatch.setattr(organizer, 'native_guard', lambda: None)
    monkeypatch.setattr(organizer.socket, 'gethostname', lambda: 'penguin')
    monkeypatch.setattr(organizer.os, 'umask', lambda *_: 0o022)
    monkeypatch.setattr(organizer.shutil, 'disk_usage', lambda *_: SimpleNamespace(free=0))
    monkeypatch.setattr(organizer, 'run_batches', lambda *_: pytest.fail('decoded despite full D'))
    monkeypatch.setattr(sys, 'argv', ['organizer', '--apply', '--retire'])
    assert organizer.main() == 75
    status = json.loads(organizer.COORDINATOR_STATUS.read_bytes())
    assert status['state'] == 'waiting_cold_physical_capacity'
    assert status['source_deleted'] is False and status['full_history_verified'] is False


def test_every_received_cohort_is_preserved_before_remote_retirement(tmp_path, monkeypatch):
    monkeypatch.setattr(organizer, 'INCOMING', tmp_path)
    batches = []
    for number in range(2):
        directory = tmp_path / f'20261006T00000{number}-abcdef012345'
        directory.mkdir()
        (directory / 'intent.json').write_text(json.dumps({
            'authority_node_id': 'penguin', 'origin_node_id': 'vastai1T', 'scopes': ['cache']}))
        (directory / 'cache.receipt.json').write_text(json.dumps({
            'scope': 'cache', 'state': 'compressed_transport_received',
            'completed_at_epoch': 1, 'compressed_sha256': 'a' * 64}))
        batches.append(directory)
    calls = []
    def process(args, directory, receipt):
        calls.append(('preserve' if getattr(args, '_preserve_only', False) else 'retire', directory))
        return getattr(args, '_preserve_only', False)
    monkeypatch.setattr(organizer, 'organize', process)
    assert organizer.run_batches(SimpleNamespace(batch=batches, watch=False, apply=True, retire=True)) == 75
    assert calls == [('preserve', p) for p in batches] + [('retire', p) for p in batches]


def test_bulk_handoff_never_accepts_a_live_transaction_or_child():
    sample = {"observation_complete": True, "common_fd_present": True,
              "holds_common_lock": False, "children": [], "state": "S",
              "wchan": "locks_lock_inode_wait"}
    assert safe_boundary(sample)
    assert not safe_boundary({**sample, "holds_common_lock": True})
    assert not safe_boundary({**sample, "children": [123]})
    assert not safe_boundary({**sample, "wchan": "p9_client_rpc"})


class AcknowledgementClock:
    def __init__(self, now, *, stalled=False):
        self.now, self.elapsed, self.stalled = now, 0, stalled

    def time(self):
        return self.now

    def monotonic(self):
        return self.elapsed

    def sleep(self, seconds):
        self.elapsed += seconds
        if not self.stalled:
            self.now += seconds


def test_future_ack_waits_for_origin_clock_without_refreshing_proof(monkeypatch):
    clock = AcknowledgementClock(1000)
    monkeypatch.setattr(legacy_return, "time", clock)
    timestamp = 1000.6
    age = legacy_return.acknowledgement_age(timestamp, 300)
    assert age >= 0 and clock.elapsed >= 0.6 and timestamp == 1000.6


@pytest.mark.parametrize("timestamp", [1301, 994, float("nan"), float("inf")])
def test_clock_wait_does_not_accept_old_or_far_future_proof(monkeypatch, timestamp):
    clock = AcknowledgementClock(1000)
    monkeypatch.setattr(legacy_return, "time", clock)
    with pytest.raises(SnapshotError, match="stale"):
        legacy_return.acknowledgement_age(timestamp, 5)
    assert clock.elapsed == 0


def test_clock_wait_fails_closed_when_origin_clock_stalls(monkeypatch):
    clock = AcknowledgementClock(1000, stalled=True)
    monkeypatch.setattr(legacy_return, "time", clock)
    with pytest.raises(SnapshotError, match="stale"):
        legacy_return.acknowledgement_age(1000.5, 300)
    assert 5 <= clock.elapsed < 5.1


def test_explicit_recovery_hold_protects_root_children_and_ancestors(tmp_path, monkeypatch):
    monkeypatch.setattr(legacy_return, "RECOVERY_HOLDS", tmp_path / "private/holds.json")
    root = "markets/tw_futures_v8_margin_preparation"
    legacy_return.install_recovery_hold(root)
    for suffix in ("artifacts/markets", "artifacts/" + root, "artifacts/" + root + "/rules"):
        assert legacy_return.recovery_hold_references(tmp_path / suffix, tmp_path)
    assert not legacy_return.recovery_hold_references(tmp_path / "artifacts/markets/unrelated", tmp_path)
    assert legacy_return.install_recovery_hold(root)["protected_roots"] == [root]


def test_recovery_hold_cannot_protect_a_broad_or_escaped_scope(tmp_path, monkeypatch):
    monkeypatch.setattr(legacy_return, "RECOVERY_HOLDS", tmp_path / "private/holds.json")
    for root in ("markets", "markets/../other", "markets/futures/child", "/etc"):
        with pytest.raises(SnapshotError):
            legacy_return.install_recovery_hold(root)


def test_malformed_or_redirected_recovery_hold_fails_closed(tmp_path, monkeypatch):
    path = tmp_path / "holds.json"
    monkeypatch.setattr(legacy_return, "RECOVERY_HOLDS", path)
    path.write_text("{}"); path.chmod(0o600)
    with pytest.raises(SnapshotError):
        legacy_return.recovery_hold_references(tmp_path / "artifacts/markets/a", tmp_path)
    path.unlink()
    path.symlink_to(tmp_path / "absent")
    with pytest.raises(SnapshotError):
        legacy_return.recovery_hold_references(tmp_path / "artifacts/markets/a", tmp_path)
