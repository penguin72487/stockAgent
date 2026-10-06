from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import scripts.manage_packed_transport as transport
from stockagent.data_sync.desync_snapshots import SnapshotError
from stockagent.data_sync.packed_edge_cache import release_payload_relpaths, render_edge_ignore
from stockagent.data_sync.packed_snapshots import initialize_packed_layout, publish_packed_snapshot


BASE = ["(?d).local-state/**", "#include .stignore-edge"]


def release(tmp_path, name="prices"):
    source = tmp_path / name
    source.mkdir(exist_ok=True)
    (source / "values.json").write_text('{"value": 42}\n')
    cold = tmp_path / "cold"
    if not cold.exists():
        initialize_packed_layout(cold, node_id="penguin")
    resolved = publish_packed_snapshot(cold, name, source)
    return cold, resolved


def demand_for(resolved):
    return {"state": {"schema_version": 1, "mode": "index-only",
        "hydrating": {resolved.manifest["dataset"]: resolved.manifest["snapshot_id"]}},
        "ignore": render_edge_ignore(release_payload_relpaths(resolved)).decode().splitlines()}


class FakeAPI:
    def __init__(self, root, ignores, *, extra_peer=False, connected=True):
        self.root = root
        self.ignores = list(ignores)
        self.extra_peer = extra_peer
        self.connected = connected
        self.posts = []
        self.scans = []

    def __call__(self, path, query=None, **kwargs):
        if path == "/rest/config/folders/stockagent-packed":
            devices = [{"deviceID": "source"}, {"deviceID": "edge"}]
            if self.extra_peer:
                devices.append({"deviceID": "full-replica"})
            return {"path": str(self.root), "paused": False, "devices": devices}
        if path == "/rest/system/status":
            return {"myID": "source"}
        if path == "/rest/config/devices":
            return [{"name": "penguin", "deviceID": "source"},
                    {"name": "vastai1T", "deviceID": "edge"}]
        if path == "/rest/system/connections":
            return {"connections": {"edge": {"connected": self.connected, "crypto": "TLS1.3"}}}
        if path == "/rest/db/ignores":
            if kwargs.get("method") == "POST":
                self.posts.append(kwargs["payload"])
                self.ignores = kwargs["payload"]["ignore"]
            return {"ignore": list(self.ignores)}
        if path.startswith("/rest/db/scan"):
            self.scans.append((path, query))
            return None
        raise AssertionError(path)


def args(root):
    state = root.parent / (root.name + "-transport-state")
    state.mkdir(exist_ok=True)
    return SimpleNamespace(sync_root=root, state_root=state, apply=True)


@pytest.fixture(autouse=True)
def mounted(monkeypatch):
    monkeypatch.setattr(transport.subprocess, "run", lambda *a, **k: SimpleNamespace(returncode=0))


def test_real_release_demand_opens_only_its_exact_objects_and_preserves_authority(tmp_path):
    cold, resolved = release(tmp_path)
    wanted = release_payload_relpaths(resolved)
    payloads = {p: (cold / p).read_bytes() for p in wanted}
    api = FakeAPI(cold, [*BASE, transport.LEGACY, *transport.EXCLUSIONS])
    result = transport.reconcile(args(cold), api, lambda a: demand_for(resolved))
    assert result["requested_payload_count"] == len(wanted)
    assert [line for line in api.ignores if line in BASE] == BASE
    for p in wanted:
        assert api.ignores.index("!/" + p) < api.ignores.index(transport.EXCLUSIONS[0])
        assert api.ignores.index("!/" + p) < api.ignores.index("#include .stignore-edge")
        assert (cold / p).read_bytes() == payloads[p]
    assert result["authoritative_payload_deleted"] is False
    assert result["remote_state_modified"] is False
    assert result["peer_delivery_verified"] is False
    assert result["scan_requests_acknowledged"] > 0


def test_exact_exceptions_precede_an_included_legacy_edge_exclusion():
    path="objects/blobs/aa/"+"a"*64+".blob"
    original=["(?d).local-state/**","#include .stignore-edge","unrelated-user-rule"]
    wanted=transport.source_ignores(original,{path})
    assert wanted.index("!/"+path)<wanted.index("#include .stignore-edge")
    assert [line for line in wanted if line in original]==original
    assert transport.source_ignores(wanted,{path})==wanted
    assert transport.source_ignores(wanted,set(),full_replica=True)==original


def test_hydration_completion_closes_exceptions_without_deleting_objects(tmp_path):
    cold, resolved = release(tmp_path)
    wanted = release_payload_relpaths(resolved)
    api = FakeAPI(cold, transport.source_ignores(BASE, wanted))
    empty = {"state": {"schema_version": 1, "mode": "index-only", "hydrating": {}},
             "ignore": render_edge_ignore().decode().splitlines()}
    result = transport.reconcile(args(cold), api, lambda a: empty)
    assert result["changed"] and result["requested_payload_count"] == 0
    assert not any(line.startswith("!/") for line in api.ignores)
    assert all((cold / p).is_file() for p in wanted)


def test_unchanged_request_is_idempotent(tmp_path):
    cold, resolved = release(tmp_path)
    api = FakeAPI(cold, transport.source_ignores(BASE, release_payload_relpaths(resolved)))
    result = transport.reconcile(args(cold), api, lambda a: demand_for(resolved))
    assert not result["changed"]
    assert api.posts == []


def test_new_full_replica_peer_restores_complete_payload_transport(tmp_path):
    api = FakeAPI(tmp_path, transport.source_ignores(BASE, set()), extra_peer=True)
    def forbidden(a):
        raise AssertionError("must not require the edge for full-replica delivery")
    result = transport.reconcile(args(tmp_path), api, forbidden)
    assert result["state"] == "full_replica_transport"
    assert api.ignores == BASE


def test_rclone_has_sole_payload_writer_and_keeps_exact_release_scope(tmp_path):
    cold, resolved = release(tmp_path)
    allowed = release_payload_relpaths(resolved)
    api = FakeAPI(cold, transport.source_ignores(BASE, allowed))
    def deliver(opts, policy, demand, selected):
        assert selected == allowed
        assert not any(line.startswith('!/objects/') for line in api.ignores)
        return {'all_requested_objects_sha256_verified': True}
    result = transport.reconcile(args(cold), api, lambda _: demand_for(resolved),
        rclone_policy={'producer_device_id': 'source', 'receiver_device_id': 'edge'}, payload_delivery=deliver)
    assert result['state'] == 'immutable_rclone_edge_transport' and result['peer_delivery_verified']
    assert cold.exists()


def test_rclone_refuses_new_full_replica_and_foreign_peer_before_policy_change(tmp_path):
    cold, resolved = release(tmp_path)
    api = FakeAPI(cold, transport.source_ignores(BASE, set()), extra_peer=True)
    with pytest.raises(SnapshotError, match='sole index-only'):
        transport.reconcile(args(cold), api, lambda _: demand_for(resolved),
            rclone_policy={'producer_device_id': 'source', 'receiver_device_id': 'edge'})
    assert not api.posts


def test_offline_peer_preserves_an_existing_active_request(tmp_path):
    original = transport.source_ignores(BASE, {"objects/blobs/aa/" + "a" * 64 + ".blob"})
    api = FakeAPI(tmp_path, original, connected=False)
    with pytest.raises(SnapshotError, match="not connected"):
        transport.reconcile(args(tmp_path), api, lambda a: {})
    assert api.posts == [] and api.ignores == original


def test_state_ignore_transition_does_not_clear_or_open_anything(tmp_path):
    cold, resolved = release(tmp_path)
    api = FakeAPI(cold, transport.source_ignores(BASE, set()))
    demand = demand_for(resolved)
    demand["ignore"] = render_edge_ignore().decode().splitlines()
    with pytest.raises(SnapshotError, match="not yet consistent"):
        transport.reconcile(args(cold), api, lambda a: demand)
    assert api.posts == []


def test_remote_arbitrary_object_exception_is_rejected(tmp_path):
    cold, resolved = release(tmp_path)
    demand = demand_for(resolved)
    demand["ignore"].insert(0, "!/objects/blobs/aa/" + "a" * 64 + ".blob")
    with pytest.raises(SnapshotError, match="not yet consistent"):
        transport.requested_payloads(cold, demand)


@pytest.mark.parametrize("field,value", [("schema_version", 2), ("mode", "full-replica"), ("hydrating", [])])
def test_malformed_or_unenrolled_consumer_is_rejected(tmp_path, field, value):
    cold, resolved = release(tmp_path)
    demand = demand_for(resolved)
    demand["state"][field] = value
    with pytest.raises(SnapshotError):
        transport.requested_payloads(cold, demand)


def test_nonexistent_exact_release_is_never_substituted_with_latest(tmp_path):
    cold, resolved = release(tmp_path)
    demand = demand_for(resolved)
    demand["state"]["hydrating"]["prices"] = "missing-release"
    with pytest.raises(FileNotFoundError):
        transport.requested_payloads(cold, demand)


def test_concurrent_source_ignore_edit_is_preserved(tmp_path):
    cold, resolved = release(tmp_path)
    api = FakeAPI(cold, transport.source_ignores(BASE, set()))
    calls = 0
    def concurrent(path, query=None, **kwargs):
        nonlocal calls
        if path == "/rest/db/ignores" and kwargs.get("method", "GET") == "GET":
            calls += 1
            if calls == 2:
                api.ignores.append("new-user-rule")
        return api(path, query, **kwargs)
    with pytest.raises(SnapshotError, match="changed during"):
        transport.reconcile(args(cold), concurrent, lambda a: demand_for(resolved))
    assert api.posts == [] and api.ignores[-1] == "new-user-rule"


def test_scan_timeout_retries_even_when_the_allowlist_is_already_updated(tmp_path):
    cold, resolved = release(tmp_path)
    api = FakeAPI(cold, transport.source_ignores(BASE, set()))
    opts = args(cold)
    def timeout(path, query=None, **kw):
        if path.startswith("/rest/db/scan"):
            raise TimeoutError("simulated scan timeout")
        return api(path, query, **kw)
    with pytest.raises(TimeoutError):
        transport.reconcile(opts, timeout, lambda a: demand_for(resolved))
    pending = opts.state_root / "pending-scan.json"
    assert pending.is_file()
    result = transport.reconcile(opts, api, lambda a: demand_for(resolved))
    assert result["changed"] is False
    assert result["scan_requests_acknowledged"] > 0
    assert not pending.exists()
    assert len(api.posts) == 1


def test_post_timeout_retains_intent_and_reconfirms_before_retry(tmp_path):
    cold, resolved = release(tmp_path)
    api = FakeAPI(cold, transport.source_ignores(BASE, set()))
    opts = args(cold)
    def timeout(path, query=None, **kw):
        result = api(path, query, **kw)
        if path == "/rest/db/ignores" and kw.get("method") == "POST":
            raise TimeoutError("response lost after accepted POST")
        return result
    with pytest.raises(TimeoutError):
        transport.reconcile(opts, timeout, lambda a: demand_for(resolved))
    assert (opts.state_root / "pending-scan.json").is_file()
    result = transport.reconcile(opts, api, lambda a: demand_for(resolved))
    assert not result["changed"] and result["scan_requests_acknowledged"] > 0


@pytest.mark.parametrize("lines", [
    [transport.BEGIN], [transport.END, transport.BEGIN],
    [transport.BEGIN, transport.END, transport.BEGIN],
    [*BASE, *transport.EXCLUSIONS],
    [*BASE, transport.LEGACY, *transport.EXCLUSIONS, "edited-rule"],
])
def test_ambiguous_or_unowned_filter_is_not_replaced(lines):
    with pytest.raises(SnapshotError):
        transport.source_ignores(lines, set())
