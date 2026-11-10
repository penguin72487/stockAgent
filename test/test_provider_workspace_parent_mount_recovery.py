import pytest

from scripts.relocate_provider_workspace import (
    _provider_child_mount_unit, _provider_child_upgrade, _provider_parent_mount_unit,
)


def test_parent_mount_wants_all_retained_children_after_delayed_recovery():
    entry = {"alias": "/root/stockAgent/data_finmind", "destination": "/srv/workspace/finmind"}
    children = ["root-stockAgent-data_finmind-parquet.mount", "root-stockAgent-data_finmind-receipts.mount"]
    rendered = _provider_parent_mount_unit(entry, children)
    assert "Wants=" + " ".join(children) + "\n" in rendered
    assert "Requires=stockagent-e-workspace-mount.service\n" in rendered
    assert "After=stockagent-e-workspace-mount.service\n" in rendered
    assert "DefaultDependencies=no\n" in rendered
    assert "Before=umount.target\nConflicts=umount.target\n" in rendered
    assert "What=/srv/workspace/finmind\nWhere=/root/stockAgent/data_finmind\n" in rendered


def test_parent_mount_without_retained_children_has_no_empty_wants():
    entry = {"alias": "/root/stockAgent/data_tej", "destination": "/srv/workspace/tej"}
    assert "Wants=" not in _provider_parent_mount_unit(entry, [])


def test_only_the_exact_legacy_child_unit_is_upgraded():
    child = {"source": "/retained/source", "readonly": True}
    target = "/provider/retained"
    parent = "provider.mount"
    legacy = ("[Unit]\nDescription=Provider enrolled retained subtree\nRequires=provider.mount\n"
              "After=provider.mount\n[Mount]\nWhat=/retained/source\nWhere=/provider/retained\n"
              "Type=none\nOptions=bind,ro\n[Install]\nWantedBy=multi-user.target\n")
    upgraded = _provider_child_upgrade(child, target, parent, legacy)
    assert upgraded == _provider_child_mount_unit(child, target, parent)
    assert "DefaultDependencies=no\n" in upgraded
    assert "After=provider.mount\n" in upgraded and "Options=bind,ro\n" in upgraded
    assert _provider_child_upgrade(child, target, parent, legacy + "# another owner\n") is None
    with pytest.raises(RuntimeError):
        _provider_child_upgrade(child, target, parent, legacy.replace("/retained/source", "/wrong"))
