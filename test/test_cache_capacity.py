"""Admission protects bytes without deleting cache, source or cold payloads."""
from pathlib import Path
from types import SimpleNamespace

import pytest

from stockagent.data_sync import cache_capacity as capacity
from stockagent.data_sync.completed_forward_capacity import CapacityDeferred, capacity_status

MIB = 1024**2


@pytest.fixture
def policy(tmp_path, monkeypatch):
    controls = tmp_path / "controls"
    controls.mkdir()
    monkeypatch.setattr(capacity, "CONTROL_ROOT", controls)
    monkeypatch.setattr(capacity, "_admit_backing_volume", lambda *args: {})
    monkeypatch.setattr("stockagent.data_sync.completed_forward_capacity.os.statvfs",
                        lambda _path: SimpleNamespace(f_bavail=100 * MIB, f_frsize=1))
    return {**capacity.load_capacity_policy(), "minimum_free_bytes": MIB,
            "materialized_capacity_bytes": 50 * MIB,
            "panel_family_capacity_bytes": 50 * MIB}


def test_census_counts_unique_inodes_and_does_not_follow_symlinks(tmp_path):
    root = tmp_path / "cache"
    root.mkdir()
    payload = root / "one"
    payload.write_bytes(b"source bytes" * 1000)
    (root / "two").hardlink_to(payload)
    (root / "outside").symlink_to(tmp_path)
    expected = sum(path.lstat().st_blocks * 512 for path in (root, payload, root / "outside"))
    assert capacity.allocated_tree_bytes(root, maximum_entries=10, maximum_seconds=5) == expected


def test_census_bound_defers_instead_of_ignoring_unknown_allocations(tmp_path):
    root = tmp_path / "cache"
    root.mkdir()
    for number in range(4):
        (root / str(number)).write_bytes(b"keep")
    with pytest.raises(CapacityDeferred, match="cache_inventory_bound"):
        capacity.allocated_tree_bytes(root, maximum_entries=2, maximum_seconds=5)
    assert len(list(root.iterdir())) == 4


def test_census_rejects_same_device_nested_bind_mount(tmp_path, monkeypatch):
    root = tmp_path / "cache"
    root.mkdir()
    monkeypatch.setattr(capacity, "_mountinfo",
                        lambda: f"31 30 8:1 / {root}/nested rw - ext4 /dev/sda rw\n")
    with pytest.raises(CapacityDeferred, match="nested_cache_mount"):
        capacity.allocated_tree_bytes(root, maximum_entries=10, maximum_seconds=5)


def test_capacity_rejection_keeps_existing_payload(tmp_path, policy):
    root = tmp_path / "cache"
    root.mkdir()
    payload = root / "existing"
    payload.write_bytes(b"x" * MIB)
    policy["materialized_capacity_bytes"] = 2 * MIB
    with pytest.raises(CapacityDeferred, match="capacity_exhausted"):
        with capacity.reserve_cache_write(root, key="source", maximum_growth_bytes=2 * MIB,
                family="materialized", policy=policy):
            pytest.fail("rejected writer ran")
    assert payload.stat().st_size == MIB


def test_separate_families_reserve_shared_filesystem_headroom(tmp_path, monkeypatch, policy):
    monkeypatch.setattr("stockagent.data_sync.completed_forward_capacity.os.statvfs",
                        lambda _path: SimpleNamespace(f_bavail=8 * MIB, f_frsize=1))
    with capacity.reserve_cache_write(tmp_path / "one", key="one", maximum_growth_bytes=4 * MIB,
            family="materialized", policy=policy):
        with pytest.raises(CapacityDeferred, match="filesystem_headroom_exhausted"):
            with capacity.reserve_cache_write(tmp_path / "two", key="two", maximum_growth_bytes=4 * MIB,
                    family="panel", policy=policy):
                pytest.fail("concurrent over-allocation ran")
    controls = next((capacity.CONTROL_ROOT / f"stockagent-capacity-{__import__('os').geteuid()}").glob("filesystem-*"))
    assert not capacity_status(controls)["active_reservations"]


def test_exception_releases_both_kernel_leases(tmp_path, policy):
    with pytest.raises(RuntimeError, match="producer failed"):
        with capacity.reserve_cache_write(tmp_path / "cache", key="source", maximum_growth_bytes=MIB,
                family="panel", policy=policy):
            raise RuntimeError("producer failed")
    for control in capacity.CONTROL_ROOT.glob("stockagent-capacity-*/*"):
        assert not capacity_status(control)["active_reservations"]


def test_backing_volume_failure_does_not_trust_virtual_free_space(tmp_path, monkeypatch, policy):
    def unavailable(*args):
        raise CapacityDeferred("physical_backing_headroom_unconfirmed")
    monkeypatch.setattr(capacity, "_admit_backing_volume", unavailable)
    with pytest.raises(CapacityDeferred, match="physical_backing_headroom_unconfirmed"):
        with capacity.reserve_cache_write(tmp_path / "cache", key="source", maximum_growth_bytes=MIB,
                family="panel", policy=policy):
            pytest.fail("admitted without actual physical backing capacity")
    for control in capacity.CONTROL_ROOT.glob("stockagent-capacity-*/*"):
        assert not capacity_status(control)["active_reservations"]


def test_unattended_backing_query_uses_explicit_authority_identity_without_mutating_environment(tmp_path, monkeypatch):
    import json
    import os
    from stockagent.data_sync import windows_cold_io
    read_text = Path.read_text
    monkeypatch.setattr(Path, "read_text", lambda path, *a, **kw:
                        "microsoft" if str(path) == "/proc/sys/kernel/osrelease" else read_text(path, *a, **kw))
    monkeypatch.delenv("WSL_DISTRO_NAME", raising=False)
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs/training_rescue.json").write_text(json.dumps({
        "schema_version": 1, "contract": "stockagent.training_rescue.v1",
        "cold_guard": "penguin-d-primary", "windows_distribution": "explicit-test-node"}))
    monkeypatch.setattr(capacity, "REPO_ROOT", tmp_path)
    observed = []
    def admit(path, required, **options):
        observed.append((path, required, options))
        return {"physical_free_bytes": 10**12}
    monkeypatch.setattr(windows_cold_io, "admit_workspace", admit)
    assert capacity._admit_backing_volume(tmp_path, MIB)["physical_free_bytes"] == 10**12
    assert observed[0][2]["windows_distribution"] == "explicit-test-node"
    assert "WSL_DISTRO_NAME" not in os.environ


@pytest.mark.parametrize("identity", [{}, {"schema_version": True}, {
    "schema_version": 1, "contract": "stockagent.training_rescue.v1",
    "cold_guard": "penguin-d-primary", "windows_distribution": "wrong;command"}])
def test_unattended_backing_identity_is_required_and_validated(tmp_path, monkeypatch, identity):
    import json
    read_text = Path.read_text
    monkeypatch.setattr(Path, "read_text", lambda path, *a, **kw:
                        "microsoft" if str(path) == "/proc/sys/kernel/osrelease" else read_text(path, *a, **kw))
    monkeypatch.delenv("WSL_DISTRO_NAME", raising=False)
    (tmp_path / "configs").mkdir()
    (tmp_path / "configs/training_rescue.json").write_text(json.dumps(identity))
    monkeypatch.setattr(capacity, "REPO_ROOT", tmp_path)
    with pytest.raises(CapacityDeferred, match="physical_backing_headroom_unconfirmed"):
        capacity._admit_backing_volume(tmp_path, MIB)


def test_existing_hit_does_not_require_space_or_scan_payloads(tmp_path, monkeypatch):
    monkeypatch.setattr(capacity, "load_capacity_policy", lambda: pytest.fail("hit read policy"))
    with capacity.reserve_cache_write(tmp_path, key="accepted", maximum_growth_bytes=MIB,
            family="materialized", enabled=False) as observed:
        assert observed["state"] == "existing-hit"


def test_unenrolled_custom_namespace_stays_opt_in(tmp_path):
    with capacity.reserve_cache_write(tmp_path, key="custom", maximum_growth_bytes=MIB,
            family="panel") as observed:
        assert observed["state"] == "not-enrolled"


def test_public_panel_compatibility_writer_is_also_enrolled(tmp_path, monkeypatch):
    source = tmp_path / "public"
    monkeypatch.setattr(capacity.socket, "gethostname", lambda: "penguin")
    monkeypatch.setattr(capacity, "PANEL_COMPATIBILITY_ROOT", source)
    configured = capacity.load_capacity_policy()
    assert capacity._enrolled_capacity(source / "stocks/panel_cache_v2", "panel", configured) == configured["panel_family_capacity_bytes"]
    assert capacity._enrolled_capacity(source / "stocks/other", "panel", configured) is None


def test_authority_budget_does_not_silently_limit_remote_training_nodes(tmp_path, monkeypatch):
    monkeypatch.setattr(capacity.socket, "gethostname", lambda: "unrelated-training-node")
    assert capacity._enrolled_capacity(capacity.MATERIALIZED_ROOT, "materialized", capacity.load_capacity_policy()) is None
    with capacity.reserve_cache_write(capacity.ARTIFACT_CACHE_ROOT / "large/panel_cache_v2",
            key="large-experiment", maximum_growth_bytes=200 * 1024**3, family="panel") as observed:
        assert observed["state"] == "not-enrolled"


def test_cli_reports_typed_retryable_capacity_without_a_path_on_stdout(monkeypatch, capsys):
    from scripts import data_cache
    def deferred(*args, **kwargs):
        raise CapacityDeferred("filesystem_headroom_exhausted", minimum_free_bytes=25 * 1024**3)
    monkeypatch.setattr(data_cache, "use_materialized_snapshot", deferred)
    assert data_cache.main(["use", "prices", "--path-only"]) == 75
    result = capsys.readouterr()
    assert result.out == ""
    import json
    assert json.loads(result.err)["state"] == "deferred"


def test_redirected_control_parent_cannot_create_a_child_elsewhere(tmp_path, policy):
    import os
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (capacity.CONTROL_ROOT / f"stockagent-capacity-{os.geteuid()}").symlink_to(elsewhere)
    with pytest.raises(CapacityDeferred, match="untrusted_cache_capacity_controls"):
        with capacity.reserve_cache_write(tmp_path / "cache", key="source", maximum_growth_bytes=MIB,
                family="panel", policy=policy):
            pytest.fail("redirected control admitted")
    assert not list(elsewhere.iterdir())


@pytest.mark.parametrize("text", ['{"schema_version":1,"schema_version":1}',
                                  '{"schema_version":true}', '{}'])
def test_policy_rejects_duplicate_and_unsupported_fields(tmp_path, text):
    path = tmp_path / "policy.json"
    path.write_text(text)
    with pytest.raises(ValueError):
        capacity.load_capacity_policy(path)


def test_prepared_generation_namespace_joins_shared_capacity_policy(tmp_path, monkeypatch):
    monkeypatch.setattr(capacity.socket, "gethostname", lambda: "penguin")
    monkeypatch.setattr(capacity, "PREPARED_ROOT", tmp_path)
    configured = capacity.load_capacity_policy()
    assert capacity._enrolled_capacity(tmp_path / "factorized", "prepared", configured) == capacity.load_prepared_capacity_policy()["family_capacity_bytes"]
    assert capacity._enrolled_capacity(tmp_path, "prepared", configured) is None


def test_generation_budget_rejects_before_excess_bytes_and_keeps_old_data(tmp_path):
    budget = capacity.GenerationWriteBudget(4 * 65536)
    old = tmp_path / "last-good"
    old.write_bytes(b"accepted")
    with budget.open(tmp_path / "partial") as stream:
        assert stream.write(b"first") == 5
        with pytest.raises(CapacityDeferred, match="generation_byte_budget_exhausted"):
            stream.write(b"x" * (4 * 65536))
    assert (tmp_path / "partial").read_bytes() == b"first"
    assert old.read_bytes() == b"accepted"
    assert budget.stats()["charged_bytes"] <= budget.maximum_bytes


@pytest.mark.parametrize("text", ['{"schema_version":1,"schema_version":1}', '[]', '{}',
    '{"schema_version":1,"contract":"prepared_generation_admission_v1","family_capacity_bytes":1,"maximum_generation_bytes":2}'])
def test_prepared_policy_is_independently_strict_and_bounded(tmp_path, text):
    path = tmp_path / "prepared.json"
    path.write_text(text)
    with pytest.raises(ValueError):
        capacity.load_prepared_capacity_policy(path)


def test_generation_budget_applies_to_native_numpy_and_parquet_writers(tmp_path):
    import numpy as np
    import polars as pl
    import pyarrow.parquet as pq
    budget = capacity.GenerationWriteBudget(32 * MIB)
    values = np.arange(64, dtype=np.float64).reshape(8, 8)
    with budget.open(tmp_path / "array.npy") as stream:
        np.save(stream, values, allow_pickle=False)
    np.testing.assert_array_equal(np.load(tmp_path / "array.npy"), values)
    table = pl.DataFrame({"value": [-0., None, 1.123456789]})
    with budget.open(tmp_path / "table.parquet") as stream:
        table.write_parquet(stream)
    assert table.equals(pl.read_parquet(tmp_path / "table.parquet"))
    with budget.open(tmp_path / "stream.parquet") as stream:
        with pq.ParquetWriter(stream, table.to_arrow().schema) as writer:
            writer.write_table(table.to_arrow())
    assert table.equals(pl.read_parquet(tmp_path / "stream.parquet"))
    assert budget.stats()["write_calls"] > 3


def test_prepared_allowance_is_coordinated_with_panel_writes(tmp_path, monkeypatch, policy):
    monkeypatch.setattr("stockagent.data_sync.completed_forward_capacity.os.statvfs",
                        lambda _path: SimpleNamespace(f_bavail=8 * MIB, f_frsize=1))
    with capacity.reserve_cache_write(tmp_path / "prepared", key="generation", maximum_growth_bytes=4 * MIB,
            family="prepared", policy=policy):
        with pytest.raises(CapacityDeferred, match="filesystem_headroom_exhausted"):
            with capacity.reserve_cache_write(tmp_path / "panel", key="panel", maximum_growth_bytes=4 * MIB,
                    family="panel", policy=policy):
                pytest.fail("shared disk over-allocation")


def test_native_io_exception_wrapping_cannot_turn_capacity_into_blind_retry():
    with pytest.raises(CapacityDeferred, match="generation_byte_budget_exhausted"):
        with capacity.GenerationWriteBudget(1) as budget:
            try:
                budget.charge(100)
            except CapacityDeferred as error:
                raise OSError("native serializer wrapped Python callback") from error
