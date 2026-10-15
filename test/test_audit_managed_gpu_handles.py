import os

import pytest

from scripts import audit_managed_gpu_handles as audit


def proc(tmp_path, pid="123", *, unit="stockagent-test.service", ticks=77):
    path = tmp_path / pid
    path.mkdir()
    (path / "fd").mkdir()
    # fields after the command are stat fields 3..52; starttime is field 22.
    fields = ["0"] * 50
    fields[19] = str(ticks)
    (path / "stat").write_text(f"{pid} (name with ) parentheses) " + " ".join(fields))
    (path / "cgroup").write_text(f"0::/system.slice/{unit}\n")
    return path


def test_gpu_rows_are_whitelisted_and_never_export_other_fd_targets(tmp_path):
    path = proc(tmp_path)
    for name, target in (("7", "/dev/dxg"), ("8", "/dev/nvidia0"),
                         ("9", "/private/credentials.env"), ("10", "socket:[1]")):
        (path / "fd" / name).symlink_to(target)
    row = audit.inspect_process(path)
    assert row["start_time_ticks"] == 77 and row["state"] == "observed"
    assert row["gpu_device_handles"] == [{"fd": 7, "device": "/dev/dxg"}, {"fd": 8, "device": "/dev/nvidia0"}]
    assert "private" not in repr(row) and "credentials" not in repr(row)


def test_nonproject_and_already_exited_processes_are_not_false_zero_rows(tmp_path):
    assert audit.inspect_process(proc(tmp_path, unit="other.service")) is None
    assert audit.inspect_process(tmp_path / "exited") is None


def test_multiple_cgroup_controllers_and_render_capability_devices(tmp_path):
    path = proc(tmp_path)
    (path / "cgroup").write_text("3:cpu:/system.slice/stockagent-test.service\n4:other:/unrelated\n")
    for name, target in (("7", "/dev/dri/renderD128"), ("8", "/dev/nvidia-caps/nvidia-cap1")):
        (path / "fd" / name).symlink_to(target)
    row = audit.inspect_process(path)
    assert row["unit"] == "stockagent-test.service" and len(row["gpu_device_handles"]) == 2


@pytest.mark.parametrize("boundary", ["ticks", "unit"])
def test_identity_change_is_unknown_not_absent(tmp_path, monkeypatch, boundary):
    path = proc(tmp_path)
    before = audit.process_identity(path)
    after = {**before, **({"start_time_ticks": 999} if boundary == "ticks" else {"cgroup": "other"})}
    responses = iter((before, after))
    monkeypatch.setattr(audit, "process_identity", lambda _path: next(responses))
    row = audit.inspect_process(path)
    assert row["state"] == "identity_changed" and row["gpu_device_handles"] is None


def test_unreadable_fd_is_unknown_not_zero(tmp_path, monkeypatch):
    path = proc(tmp_path)
    (path / "fd" / "7").symlink_to("/dev/dxg")
    monkeypatch.setattr(audit.os, "readlink", lambda _path: (_ for _ in ()).throw(PermissionError()))
    row = audit.inspect_process(path)
    assert row["state"] == "unreadable" and row["gpu_device_handles"] is None


def test_closed_fd_is_tolerated_and_inventory_has_explicit_scope(tmp_path, monkeypatch):
    path = proc(tmp_path)
    (path / "fd" / "7").symlink_to("/dev/dxg")
    (path / "fd" / "8").symlink_to("/dev/nvidiactl")
    original = os.readlink

    def closing(entry):
        if entry.name == "7":
            raise FileNotFoundError()
        return original(entry)

    monkeypatch.setattr(audit.os, "readlink", closing)
    result = audit.inventory(tmp_path)
    assert result["managed_processes"] == result["observed_processes"] == 1
    assert result["gpu_owner_count"] == result["gpu_device_handles"] == 1
    assert result["all_windows_allocations_attributed"] is False
    assert result["atomic_whole_host_snapshot"] is False
    assert result["cuda_initialized_by_probe"] is False
