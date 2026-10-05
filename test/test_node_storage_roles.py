import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from stockagent.data_sync.node_roles import EXPECTED, training_only_node
from stockagent.data_sync.desync_snapshots import SnapshotError
from scripts.audit_node_storage_roles import inventory


@pytest.fixture
def enrollment(tmp_path):
    policy = tmp_path / "private.json"
    policy.write_text(json.dumps(EXPECTED))
    policy.chmod(0o600)
    cold = tmp_path / "cold"
    (cold / ".local-state").mkdir(parents=True)
    (cold / ".local-state/node-id").write_text("vastai1T\n")
    edge = tmp_path / "edge.json"
    edge.write_text('{"mode":"index-only"}')
    proc = tmp_path / "proc"
    proc.mkdir()
    return {"role_file": policy, "cold_root": cold, "edge_state": edge, "proc_root": proc,
            "run": lambda *a, **kw: SimpleNamespace(returncode=3,
                stdout="caddy RUNNING pid 1\nstockagent_tw_test_training STOPPED Not started\n")}


def test_missing_enrollment_preserves_conservative_service_gates(tmp_path):
    assert not training_only_node(role_file=tmp_path / "absent")


def test_training_edge_does_not_treat_foreign_service_templates_as_local(enrollment):
    assert training_only_node(**enrollment)


@pytest.mark.parametrize("change", ["mode", "identity", "edge", "redirected", "unknown_field"])
def test_invalid_enrollment_cannot_waive_service_consumers(enrollment, change):
    policy = enrollment["role_file"]
    if change == "mode":
        policy.chmod(0o644)
    elif change == "identity":
        (enrollment["cold_root"] / ".local-state/node-id").write_text("penguin")
    elif change == "edge":
        enrollment["edge_state"].write_text('{"mode":"replica"}')
    elif change == "redirected":
        policy.rename(policy.with_suffix(".real"))
        policy.symlink_to(policy.with_suffix(".real"))
    else:
        policy.write_text(json.dumps({**EXPECTED, "allow_delete": True}))
    with pytest.raises(SnapshotError):
        training_only_node(**enrollment)


def test_active_supervised_service_blocks_training_only_cleanup(enrollment):
    enrollment["run"] = lambda *a, **kw: SimpleNamespace(returncode=0, stdout="stockagent_discord RUNNING pid 1")
    with pytest.raises(SnapshotError, match="still active"):
        training_only_node(**enrollment)


def test_independent_live_service_blocks_role_waiver(enrollment):
    process = enrollment["proc_root"] / "100"
    process.mkdir()
    (process / "cmdline").write_bytes(b"python\0-m\0stockagent.live.tw_day_trade_service\0")
    with pytest.raises(SnapshotError, match="independent"):
        training_only_node(**enrollment)


def test_inventory_does_not_follow_symlinks_or_double_count_hardlinks(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    payload = root / "values.npy"
    payload.write_bytes(b"original")
    (root / "alias.npy").hardlink_to(payload)
    (root / "outside").symlink_to(tmp_path, target_is_directory=True)
    value = inventory(root)
    assert value["files"] == 2 and value["symlinks"] == 1 and value["shared_inodes"] == 1
    assert value["allocated_unique_file_bytes"] == payload.stat().st_blocks * 512
    assert value["logical_bytes"] == 2 * payload.stat().st_size and value["complete"]


def test_partial_inventory_is_explicitly_incomplete(tmp_path):
    (tmp_path / "one").write_bytes(b"one")
    (tmp_path / "two").write_bytes(b"two")
    assert not inventory(tmp_path, max_files=1)["complete"]
