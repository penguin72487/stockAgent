from __future__ import annotations

import hashlib
import fcntl
from pathlib import Path

import pytest

from scripts.manage_packed_edge import (
    _acquire_operation_lock,
    _allowed_relpaths,
    _can_resume_hydration,
    _retained_snapshots,
    _write_edge_ignore_if_changed,
)
from stockagent.data_sync.desync_snapshots import ResolvedSnapshot, SnapshotError
from stockagent.data_sync.packed_edge_cache import (
    ensure_edge_include,
    local_payload_inventory,
    prune_local_payloads,
    release_payload_relpaths,
    render_edge_ignore,
    verify_payload_relpaths,
    write_edge_ignore,
)


def test_edge_operation_lock_defers_concurrent_gc(tmp_path: Path) -> None:
    first = _acquire_operation_lock(tmp_path, nonblocking=False)
    try:
        with pytest.raises(BlockingIOError):
            _acquire_operation_lock(tmp_path, nonblocking=True)
    finally:
        fcntl.flock(first, fcntl.LOCK_UN)
        first.close()

    second = _acquire_operation_lock(tmp_path, nonblocking=True)
    fcntl.flock(second, fcntl.LOCK_UN)
    second.close()


def test_retained_edge_payload_expires_and_rejects_malformed_state() -> None:
    state = {
        "retained_payloads": {
            "tw-public": {"snapshot_id": "old", "expires_ns": 200},
            "tw-minute": {"snapshot_id": "minute", "expires_ns": 100},
        }
    }
    assert _retained_snapshots(state, now_ns=150) == {"tw-public": "old"}
    assert _retained_snapshots(state, now_ns=200) == {}
    with pytest.raises(SnapshotError, match="invalid retained"):
        _retained_snapshots({"retained_payloads": {"tw-public": {}}})


def test_exact_edge_hydration_can_resume_only_with_healthy_source() -> None:
    state = {"hydrating": {"tw-public": "release-2"}}
    peer = {
        "checks": {
            "folder_idle": False,
            "need_bytes_zero": False,
            "need_items_zero": False,
            "peer_connected": True,
            "peer_remote_state_valid": True,
            "errors_zero": True,
            "pull_errors_zero": True,
        }
    }
    assert _can_resume_hydration(peer, state, "tw-public", "release-2")
    assert not _can_resume_hydration(peer, state, "tw-public", "release-3")
    peer["checks"]["pull_errors_zero"] = False
    assert not _can_resume_hydration(peer, state, "tw-public", "release-2")


def test_edge_hydration_keeps_old_retained_release_until_new_is_ready(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import scripts.manage_packed_edge as edge

    monkeypatch.setattr(edge, "_retained_snapshots", lambda state: {"tw-public": "old"})
    monkeypatch.setattr(
        edge,
        "resolve_packed_snapshot_id",
        lambda root, dataset, snapshot_id, require_objects: snapshot_id,
    )
    monkeypatch.setattr(edge, "release_payload_relpaths", lambda snapshot: {snapshot})
    assert _allowed_relpaths(
        tmp_path, {"hydrating": {"tw-public": "new"}}
    ) == {"old", "new"}


def test_unchanged_edge_allowlist_does_not_trigger_rescan(tmp_path: Path) -> None:
    (tmp_path / ".stignore").write_text("", encoding="utf-8")
    allowed = {"objects/blobs/aa/" + "a" * 64 + ".blob"}
    assert _write_edge_ignore_if_changed(tmp_path, allowed)
    first = (tmp_path / ".stignore-edge").stat().st_mtime_ns
    assert not _write_edge_ignore_if_changed(tmp_path, allowed)
    assert (tmp_path / ".stignore-edge").stat().st_mtime_ns == first


def _object(root: Path, kind: str, payload: bytes) -> tuple[Path, str]:
    digest = hashlib.sha256(payload).hexdigest()
    suffix = ".blob" if kind == "blobs" else ".zip"
    path = root / "objects" / kind / digest[:2] / f"{digest}{suffix}"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return path, path.relative_to(root).as_posix()


def test_edge_ignore_places_exact_exceptions_before_general_rules() -> None:
    payload = render_edge_ignore(
        ["objects/blobs/aa/" + "a" * 64 + ".blob"]
    ).decode()
    lines = payload.splitlines()
    assert lines.index("!/objects/blobs/aa/" + "a" * 64 + ".blob") < lines.index(
        "/objects/blobs/*/*"
    )
    with pytest.raises(SnapshotError, match="not a payload object"):
        render_edge_ignore(["objects/inventories/aa/inventory.json"])


def test_edge_include_preserves_existing_rules(tmp_path: Path) -> None:
    (tmp_path / ".stignore").write_text("(?d).local-state/**\n", encoding="utf-8")
    ensure_edge_include(tmp_path)
    ensure_edge_include(tmp_path)
    text = (tmp_path / ".stignore").read_text(encoding="utf-8")
    assert "(?d).local-state/**" in text
    assert text.count("#include .stignore-edge") == 1
    write_edge_ignore(tmp_path)
    assert "/objects/packs/*/*" in (tmp_path / ".stignore-edge").read_text()


def test_edge_ignore_excludes_payload_children_not_empty_shard_directories():
    from pathlib import PurePosixPath
    rules = render_edge_ignore().decode().splitlines()
    for kind, suffix in [('blobs','blob'),('packs','zip')]:
        rule = '/objects/'+kind+'/*/*'
        assert rule in rules
        assert not PurePosixPath('/objects/'+kind+'/aa').match(rule)
        assert PurePosixPath('/objects/'+kind+'/aa/'+'a'*64+'.'+suffix).match(rule)


def test_prune_keeps_allowed_payload_and_all_inventories(tmp_path: Path) -> None:
    kept, kept_relative = _object(tmp_path, "blobs", b"keep")
    removed, _ = _object(tmp_path, "packs", b"remove")
    inventory = tmp_path / "objects" / "inventories" / "aa" / "inventory.json.gz"
    inventory.parent.mkdir(parents=True)
    inventory.write_bytes(b"inventory")

    before = local_payload_inventory(tmp_path)
    preview = prune_local_payloads(
        tmp_path, allowed_relpaths=[kept_relative], apply=False
    )
    applied = prune_local_payloads(
        tmp_path, allowed_relpaths=[kept_relative], apply=True
    )

    assert before["files"] == 2
    assert preview["would_delete_files"] == 1
    assert applied["deleted_files"] == 1
    assert kept.is_file()
    assert not removed.exists()
    assert inventory.is_file()
    assert verify_payload_relpaths(tmp_path, [kept_relative])["files"] == 1


def test_release_payload_paths_reject_inventory_as_payload(tmp_path: Path) -> None:
    resolved = ResolvedSnapshot(
        manifest={
            "archive": {
                "objects": [
                    {"relpath": "objects/blobs/aa/" + "a" * 64 + ".blob"}
                ]
            }
        },
        manifest_path=tmp_path / "manifest.json",
        manifest_sha256="b" * 64,
        head_path=tmp_path / "head.json",
    )
    assert len(release_payload_relpaths(resolved)) == 1


def test_expired_payload_ignore_is_scanned_before_prune(tmp_path, monkeypatch):
    import scripts.manage_packed_edge as edge
    obj, relative = _object(tmp_path, 'blobs', b'expired payload')
    write_edge_ignore(tmp_path, {relative})
    events = []
    monkeypatch.setattr(edge, 'process_references', lambda root: [])
    def scan(*args):
        assert ('!/'+relative) not in (tmp_path/'.stignore-edge').read_text()
        assert obj.is_file()
        events.append('scan')
    monkeypatch.setattr(edge, '_scan', scan)
    monkeypatch.setattr(edge, '_convergence', lambda *args: events.append('probe') or {'ok':True})
    proof = edge._prepare_ignored_payload_prune(
        tmp_path, set(), base_url='local', api_key='test', folder='packed', peer_name='durable')
    assert proof['ok'] and events == ['scan','probe']
    assert obj.is_file()  # Preparation itself never deletes any bytes.


def test_ignore_scan_failure_preserves_payload(tmp_path, monkeypatch):
    import scripts.manage_packed_edge as edge
    obj, relative = _object(tmp_path, 'blobs', b'preserve')
    write_edge_ignore(tmp_path, {relative})
    monkeypatch.setattr(edge, 'process_references', lambda root: [])
    monkeypatch.setattr(edge, '_scan', lambda *args: (_ for _ in ()).throw(SnapshotError('scan failed')))
    with pytest.raises(SnapshotError, match='scan failed'):
        edge._prepare_ignored_payload_prune(tmp_path,set(),base_url='local',api_key='test',folder='packed',peer_name='durable')
    assert obj.is_file()


def test_unchanged_ignore_still_requires_current_peer_proof(tmp_path, monkeypatch):
    import scripts.manage_packed_edge as edge
    write_edge_ignore(tmp_path)
    monkeypatch.setattr(edge, 'process_references', lambda root: [])
    monkeypatch.setattr(edge, '_scan', lambda *args: pytest.fail('unchanged ignore must not trigger rescan'))
    monkeypatch.setattr(edge, '_convergence', lambda *args: {'ok':False})
    with pytest.raises(SnapshotError, match='not fully converged'):
        edge._prepare_ignored_payload_prune(tmp_path,set(),base_url='local',api_key='test',folder='packed',peer_name='durable')


def test_new_process_reference_blocks_prune_after_scan(tmp_path, monkeypatch):
    import scripts.manage_packed_edge as edge
    obj, _ = _object(tmp_path,'blobs',b'busy')
    refs = iter([[],['pid=123:fd']])
    monkeypatch.setattr(edge, 'process_references', lambda root: next(refs))
    monkeypatch.setattr(edge, '_scan', lambda *args: None)
    monkeypatch.setattr(edge, '_convergence', lambda *args: {'ok':True})
    with pytest.raises(SnapshotError, match='became referenced'):
        edge._prepare_ignored_payload_prune(tmp_path,set(),base_url='local',api_key='test',folder='packed',peer_name='durable')
    assert obj.is_file()


@pytest.mark.parametrize('command',[['gc'],['evict','example'],['prune','--apply']])
def test_mutating_edge_entrypoints_prepare_ignore_before_any_delete(tmp_path,monkeypatch,command):
    import scripts.manage_packed_edge as edge
    import sys
    events = []
    state = tmp_path/'state'
    state.mkdir()
    (state/'state.json').write_text('{"schema_version":1,"mode":"index-only"}')
    monkeypatch.setenv('STGUIAPIKEY','test')
    monkeypatch.setattr(sys,'argv',['edge','--sync-root',str(tmp_path/'packed'),
                                  '--state-root',str(state),*command])
    peer = {'ok':True,'peer_name':'durable','completion':100,'remote_state':'valid',
            'global_bytes':0,'transport':'quic','crypto':'tls'}
    monkeypatch.setattr(edge,'_convergence',lambda *args:peer)
    monkeypatch.setattr(edge,'process_references',lambda root:[])
    monkeypatch.setattr(edge,'local_payload_inventory',lambda root:{})
    monkeypatch.setattr(edge,'_prepare_ignored_payload_prune',lambda *args,**kwargs:events.append('prepare') or peer)
    monkeypatch.setattr(edge,'evict_materialized_snapshots',lambda *args,**kwargs:events.append('evict') or {})
    monkeypatch.setattr(edge,'prune_local_payloads',lambda *args,**kwargs:events.append('prune') or {})
    monkeypatch.setattr(edge,'_scan',lambda *args:None)
    assert edge.main()==0
    assert events[0]=='prepare' and events[-1]=='prune'
    # A completed command must release the owner even in a long-lived caller.
    handle = edge._acquire_operation_lock(state, nonblocking=True)
    handle.close()


def test_edge_use_prepares_ignore_before_pruning_hydrated_objects(tmp_path, monkeypatch):
    import scripts.manage_packed_edge as edge
    import sys
    from types import SimpleNamespace
    state = tmp_path/'state'
    state.mkdir()
    (state/'state.json').write_text('{"schema_version":1,"mode":"index-only"}')
    monkeypatch.setenv('STGUIAPIKEY', 'test')
    monkeypatch.setattr(sys, 'argv', ['edge', '--sync-root',str(tmp_path/'packed'),
                                    '--state-root',str(state), 'use','example'])
    events = []
    peer = {'ok':True}
    monkeypatch.setattr(edge, '_convergence', lambda *args:peer)
    monkeypatch.setattr(edge, 'process_references', lambda root:[])
    monkeypatch.setattr(edge, 'local_payload_inventory', lambda root:{})
    monkeypatch.setattr(edge, 'resolve_latest_packed', lambda *args,**kwargs:SimpleNamespace(manifest={'snapshot_id':'exact-release'}))
    monkeypatch.setattr(edge, '_allowed_relpaths', lambda *args:set())
    monkeypatch.setattr(edge, '_write_edge_ignore_if_changed', lambda *args:False)
    monkeypatch.setattr(edge, 'release_payload_relpaths', lambda *args:set())
    monkeypatch.setattr(edge, 'verify_payload_relpaths', lambda *args:{})
    monkeypatch.setattr(edge, 'use_materialized_snapshot', lambda *args,**kwargs:{'expires_ns':1})
    monkeypatch.setattr(edge, '_prepare_ignored_payload_prune', lambda *args,**kwargs:events.append('prepare') or peer)
    monkeypatch.setattr(edge, 'prune_local_payloads', lambda *args,**kwargs:events.append('prune') or {})
    assert edge.main()==0
    assert events == ['prepare','prune']
    handle = edge._acquire_operation_lock(state, nonblocking=True)
    handle.close()


def test_edge_gc_dry_run_does_not_mutate_ignore(tmp_path,monkeypatch):
    import scripts.manage_packed_edge as edge
    import sys
    state = tmp_path/'state'
    state.mkdir()
    (state/'state.json').write_text('{"schema_version":1,"mode":"index-only"}')
    monkeypatch.setenv('STGUIAPIKEY','test')
    monkeypatch.setattr(sys,'argv',['edge','--sync-root',str(tmp_path/'packed'),
                                  '--state-root',str(state),'gc','--dry-run'])
    peer = {'ok':True,'peer_name':'durable','completion':100,'remote_state':'valid',
            'global_bytes':0,'transport':'quic','crypto':'tls'}
    monkeypatch.setattr(edge,'_convergence',lambda *args:peer)
    monkeypatch.setattr(edge,'process_references',lambda root:[])
    monkeypatch.setattr(edge,'local_payload_inventory',lambda root:{})
    monkeypatch.setattr(edge,'_prepare_ignored_payload_prune',lambda *args,**kwargs:pytest.fail('dry run wrote ignores'))
    monkeypatch.setattr(edge,'evict_materialized_snapshots',lambda *args,**kwargs:{})
    monkeypatch.setattr(edge,'prune_local_payloads',lambda *args,**kwargs:{})
    assert edge.main()==0
