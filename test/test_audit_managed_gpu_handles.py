import os
import json
import sys

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


def test_explicit_visible_scope_finds_unmanaged_owner_without_exporting_private_paths(tmp_path):
    managed = proc(tmp_path)
    other = proc(tmp_path, pid="456", unit="desktop.service")
    (managed / "fd" / "3").symlink_to("/private/credentials.env")
    (other / "fd" / "7").symlink_to("/dev/dxg")
    (other / "fd" / "8").symlink_to("/private/other.env")
    default = audit.inventory(tmp_path)
    assert default["contract"] == "stockagent_linux_managed_gpu_fd_sample_v1"
    assert default["managed_processes"] == default["observed_processes"] == 1
    assert default["gpu_owner_count"] == 0
    assert "selected_processes" not in default and "managed" not in default["rows"][0]
    result = audit.inventory(tmp_path, include_unmanaged=True)
    assert result["contract"] == "stockagent_linux_visible_gpu_fd_sample_v1"
    assert result["proc_candidates"] == result["selected_processes"] == result["observed_processes"] == 2
    assert result["managed_processes"] == result["unmanaged_processes"] == 1
    assert result["ownership_unknown_processes"] == 0
    assert result["gpu_owner_count"] == result["gpu_device_handles"] == 1
    owner, = [row for row in result["rows"] if row["gpu_device_handles"]]
    assert owner["pid"] == 456 and owner["managed"] is False and owner["unit"] is None
    assert "private" not in repr(result) and "credentials" not in repr(result)
    assert result["all_windows_allocations_attributed"] is False
    assert result["atomic_whole_host_snapshot"] is False


@pytest.mark.parametrize("error,state", [(PermissionError, "unreadable"), (FileNotFoundError, "exited"),
                                        (ValueError, "identity_invalid"), (IndexError, "identity_invalid")])
def test_visible_scope_retains_initial_identity_failures_as_unknown(tmp_path, monkeypatch, error, state):
    proc(tmp_path)
    monkeypatch.setattr(audit, "process_identity", lambda _path: (_ for _ in ()).throw(error()))
    result = audit.inventory(tmp_path, include_unmanaged=True)
    assert result["state"] == "partial_visibility"
    assert result["proc_candidates"] == result["selected_processes"] == result["ownership_unknown_processes"] == 1
    assert result["observed_processes"] == result["gpu_owner_count"] == 0
    row, = result["rows"]
    assert row["state"] == state and row["start_time_ticks"] is None and row["managed"] is None
    assert row["gpu_device_handles"] is None and row["error_type"] == error.__name__


def test_visible_scope_identity_change_preserves_unknown_handles(tmp_path, monkeypatch):
    path = proc(tmp_path, unit="desktop.service")
    before = audit.process_identity(path)
    answers = iter([before, {**before, "start_time_ticks": before["start_time_ticks"] + 1}])
    monkeypatch.setattr(audit, "process_identity", lambda _path: next(answers))
    row = audit.inspect_process(path, include_unmanaged=True)
    assert row["state"] == "identity_changed" and row["gpu_device_handles"] is None
    assert row["managed"] is False and row["unit"] is None


def test_visible_scope_unreadable_fd_keeps_known_unmanaged_identity(tmp_path, monkeypatch):
    path = proc(tmp_path, unit="desktop.service")
    (path / "fd" / "7").symlink_to("/dev/dxg")
    monkeypatch.setattr(audit.os, "readlink", lambda _path: (_ for _ in ()).throw(PermissionError()))
    result = audit.inventory(tmp_path, include_unmanaged=True)
    row, = result["rows"]
    assert result["state"] == "partial_visibility" and result["unmanaged_processes"] == 1
    assert result["ownership_unknown_processes"] == result["observed_processes"] == 0
    assert row["state"] == "unreadable" and row["gpu_device_handles"] is None


@pytest.mark.parametrize("flags", [[], ["--include-unmanaged"]])
def test_cli_selects_explicit_scope_and_writes_new_receipt(tmp_path, monkeypatch, flags):
    fake_proc = tmp_path / "proc"
    fake_proc.mkdir()
    proc(fake_proc, unit="desktop.service")
    original = audit.inventory
    monkeypatch.setattr(audit, "inventory", lambda *, include_unmanaged: original(
        fake_proc, include_unmanaged=include_unmanaged))
    monkeypatch.setattr(audit, "ROOT", tmp_path)
    output = tmp_path / "artifacts/operations/owners.json"
    monkeypatch.setattr(sys, "argv", ["audit", "--output", str(output), *flags])
    audit.main()
    result = json.loads(output.read_bytes())
    assert result["contract"] == ("stockagent_linux_visible_gpu_fd_sample_v1" if flags else
                                  "stockagent_linux_managed_gpu_fd_sample_v1")
    assert result["observed_processes"] == (1 if flags else 0)
