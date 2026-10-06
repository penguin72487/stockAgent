import json
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
