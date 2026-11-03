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
    monkeypatch.setattr(capacity, "load_acquisition_verification_reserve", lambda: MIB)
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


def test_acquisition_dispatch_low_capacity_defers_without_scan_or_backing_call(tmp_path, monkeypatch, policy):
    configured = {**policy, "nodes": ["authority"], "minimum_free_bytes": 101 * MIB}
    monkeypatch.setattr(capacity.socket, "gethostname", lambda: "authority")
    monkeypatch.setattr(capacity, "load_capacity_policy", lambda: configured)
    monkeypatch.setattr(capacity, "_admit_backing_volume", lambda *a: pytest.fail("queried backing before local rejection"))
    result = capacity.acquisition_dispatch_status(tmp_path)
    assert result["state"] == "deferred" and result["reason"] == "filesystem_reserve"
    assert result["minimum_free_bytes"] == 101 * MIB and result["data_paths_removed"] == 0
    assert not list(tmp_path.glob("**/*.capacity-*"))


def test_acquisition_dispatch_rechecks_physical_backing_and_resumes_when_available(tmp_path, monkeypatch, policy):
    configured = {**policy, "nodes": ["authority"]}
    monkeypatch.setattr(capacity.socket, "gethostname", lambda: "authority")
    monkeypatch.setattr(capacity, "load_capacity_policy", lambda: configured)
    def unavailable(*args):
        raise CapacityDeferred("physical_backing_headroom_unconfirmed")
    monkeypatch.setattr(capacity, "_admit_backing_volume", unavailable)
    assert capacity.acquisition_dispatch_status(tmp_path)["reason"] == "physical_backing_headroom_unconfirmed"
    calls = []
    monkeypatch.setattr(capacity, "_admit_backing_volume", lambda root, required: calls.append(required) or {"state": "verified"})
    result = capacity.acquisition_dispatch_status(tmp_path, maximum_growth_bytes=2 * MIB)
    assert result["state"] == "admitted" and calls == [3 * MIB]
    assert result["guard_kind"] == "dispatch_preflight_not_write_reservation"


def test_acquisition_preserves_larger_verification_headroom_on_both_volumes(tmp_path, monkeypatch, policy):
    configured = {**policy, "nodes": ["authority"]}
    monkeypatch.setattr(capacity.socket, "gethostname", lambda: "authority")
    monkeypatch.setattr(capacity, "load_capacity_policy", lambda: configured)
    monkeypatch.setattr(capacity, "load_acquisition_verification_reserve", lambda: 40 * MIB)
    calls = []
    monkeypatch.setattr(capacity, "_admit_backing_volume",
                        lambda root, required: calls.append(required) or {"state": "verified"})
    result = capacity.acquisition_dispatch_status(tmp_path, maximum_growth_bytes=2 * MIB)
    assert result["state"] == "admitted" and result["minimum_free_bytes"] == 40 * MIB
    assert calls == [42 * MIB]
    # The cache policy stays separate; this floor does not change its writer ABI.
    assert configured["minimum_free_bytes"] == MIB


def test_invalid_acquisition_reserve_rejects_dispatch_before_backing_or_mutation(tmp_path, monkeypatch):
    path = tmp_path / "acquisition.json"
    path.write_text('{"schema_version":1,"contract":"acquisition_verification_headroom_v1",'
                    '"minimum_free_bytes":1,"minimum_free_bytes":2}')
    loader = capacity.load_acquisition_verification_reserve
    with pytest.raises(ValueError, match="duplicate"):
        loader(path)
    monkeypatch.setattr(capacity.socket, "gethostname", lambda: "authority")
    policy = {**capacity.load_capacity_policy(), "nodes": ["authority"]}
    monkeypatch.setattr(capacity, "load_capacity_policy", lambda: policy)
    monkeypatch.setattr(capacity, "load_acquisition_verification_reserve",
                        lambda: loader(path))
    monkeypatch.setattr(capacity, "_admit_backing_volume", lambda *_: pytest.fail("unsafe backing check"))
    with pytest.raises(ValueError, match="duplicate"):
        capacity.acquisition_dispatch_status(tmp_path)


def test_acquisition_dispatch_does_not_enroll_other_hosts_or_redirected_workspaces(tmp_path, monkeypatch, policy):
    monkeypatch.setattr(capacity, "load_capacity_policy", lambda: {**policy, "nodes": ["authority"]})
    monkeypatch.setattr(capacity.socket, "gethostname", lambda: "other")
    assert capacity.acquisition_dispatch_status(tmp_path)["state"] == "not_enrolled"
    alias = tmp_path / "alias"
    alias.symlink_to(tmp_path, target_is_directory=True)
    monkeypatch.setattr(capacity.socket, "gethostname", lambda: "authority")
    with pytest.raises(CapacityDeferred, match="redirected_acquisition_workspace"):
        capacity.acquisition_dispatch_status(alias)


def test_verified_backing_shortage_is_reported_as_capacity_not_unknown(tmp_path, monkeypatch):
    from stockagent.data_sync import windows_cold_io
    original_read_text = Path.read_text
    monkeypatch.setattr(Path, "read_text", lambda path, *a, **kw:
                        "microsoft" if str(path) == "/proc/sys/kernel/osrelease"
                        else original_read_text(path, *a, **kw))
    monkeypatch.setenv("WSL_DISTRO_NAME", "fixture")
    monkeypatch.setattr(capacity.socket, "gethostname", lambda: "authority")
    policy = {**capacity.load_capacity_policy(), "minimum_free_bytes": MIB, "nodes": ["authority"]}
    monkeypatch.setattr(capacity, "load_capacity_policy", lambda: policy)
    monkeypatch.setattr(capacity, "load_acquisition_verification_reserve", lambda: MIB)
    monkeypatch.setattr(capacity.os, "statvfs", lambda _: SimpleNamespace(f_bavail=100 * MIB, f_frsize=1))
    def shortage(*a, **kw):
        raise windows_cold_io.WorkspaceCapacityDeferred("real physical free space",
                                                       physical_free_bytes=MIB // 2, required_bytes=MIB)
    monkeypatch.setattr(windows_cold_io, "admit_workspace", shortage)
    result = capacity.acquisition_dispatch_status(tmp_path)
    assert result["state"] == "deferred" and result["reason"] == "physical_backing_reserve"
    assert result["contract"] == "acquisition_dispatch_capacity_v1"
    assert result["physical_free_bytes"] == MIB // 2
    assert result["required_bytes_including_reserve"] == MIB
    assert result["data_paths_removed"] == 0


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


def test_protected_trading_source_uses_only_its_own_physical_reserve(tmp_path, monkeypatch, policy):
    from stockagent.data_sync.completed_forward_capacity import CONTROL_HEADROOM_BYTES
    gib = 1024**3
    trade = tmp_path / "trade"
    trade.mkdir()
    panel = tmp_path / "trade-panel"
    panel.mkdir()
    monkeypatch.setattr(capacity, "TRADING_PREPARED_ROOT", trade)
    monkeypatch.setattr(capacity, "TRADING_PANEL_ROOT", panel)
    monkeypatch.setenv(capacity.TRADING_PRIORITY_ENV, "1")
    monkeypatch.setattr("stockagent.data_sync.completed_forward_capacity.os.statvfs",
                        lambda _path: SimpleNamespace(f_bavail=100 * gib, f_frsize=1))
    policy["minimum_free_bytes"] = 25 * gib
    policy["panel_family_capacity_bytes"] = 2 * gib
    observed = []

    def admit(path, required):
        observed.append(required)
        if required > 38 * gib:
            raise CapacityDeferred("physical_backing_reserve")
        return {"physical_free_bytes": 38 * gib}

    monkeypatch.setattr(capacity, "_admit_backing_volume", admit)
    with capacity.reserve_cache_write(trade, key="trade", maximum_growth_bytes=16 * gib,
                                     family="prepared", policy=policy) as accepted:
        assert accepted["physical_reserve_bytes"] == 20 * gib
        assert accepted["filesystem"]["minimum_free_bytes"] == 25 * gib
        assert observed[-1] == 36 * gib + CONTROL_HEADROOM_BYTES
        with capacity.reserve_cache_write(panel, key="trade-panel", maximum_growth_bytes=gib,
                                         family="panel", policy=policy) as nested:
            assert nested["physical_reserve_bytes"] == 20 * gib
            assert observed[-1] == 37 * gib + 2 * CONTROL_HEADROOM_BYTES
        # Another family still reserves the full 25 GiB and the trading lease;
        # it cannot displace the in-progress protected source.
        with pytest.raises(CapacityDeferred, match="physical_backing_reserve"):
            with capacity.reserve_cache_write(tmp_path / "other", key="other", maximum_growth_bytes=gib,
                                             family="panel", policy=policy):
                pytest.fail("nonpriority writer displaced protected source")

    # A different prepared path and the exact path without the unit opt-in
    # cannot spend the trading reserve.
    for root in (tmp_path / "other-prepared", trade):
        if root == trade:
            monkeypatch.delenv(capacity.TRADING_PRIORITY_ENV)
        with pytest.raises(CapacityDeferred, match="physical_backing_reserve"):
            with capacity.reserve_cache_write(root, key="normal", maximum_growth_bytes=16 * gib,
                                             family="prepared", policy=policy):
                pytest.fail("ordinary source used priority backing reserve")


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


def test_registered_cache_alias_keeps_admission_and_one_lease_namespace(tmp_path, monkeypatch, policy):
    from stockagent import service_layout

    legacy, target = tmp_path / "artifacts/cache", tmp_path / "service_data/cache"
    legacy.parent.mkdir(parents=True)
    target.mkdir(parents=True)
    legacy.symlink_to(target)
    monkeypatch.setattr(service_layout, "service_moves", lambda _root: {legacy: target})
    monkeypatch.setattr(capacity, "ARTIFACT_CACHE_ROOT", legacy)
    monkeypatch.setattr(capacity.socket, "gethostname", lambda: "authority")
    monkeypatch.setattr(capacity, "load_capacity_policy", lambda: {**policy, "nodes": ["authority"]})
    admitted = []
    monkeypatch.setattr(capacity, "_admit_backing_volume", lambda root, required:
                        admitted.append((root, required)) or {"state": "verified"})

    for root in (legacy / "groups", target / "groups"):
        with capacity.reserve_cache_write(root, key="same-source", maximum_growth_bytes=MIB,
                family="panel") as observed:
            assert observed["contract"] == capacity.CONTRACT
        assert capacity.cache_capacity_status(root, family="panel")["root"] == str(target / "groups")
    assert len(admitted) == 2  # Neither spelling bypasses the physical check.
    assert all(root == target for root, _ in admitted)
    controls = capacity.CONTROL_ROOT / f"stockagent-capacity-{__import__('os').geteuid()}"
    assert len(list(controls.glob("cache-*"))) == 1

    def unavailable(*_):
        raise CapacityDeferred("physical_backing_headroom_unconfirmed")
    monkeypatch.setattr(capacity, "_admit_backing_volume", unavailable)
    with pytest.raises(CapacityDeferred, match="physical_backing_headroom_unconfirmed"):
        with capacity.reserve_cache_write(target / "groups", key="same-source",
                maximum_growth_bytes=MIB, family="panel"):
            pytest.fail("canonical spelling bypassed admission")

    (target / "redirected").symlink_to(tmp_path)
    with pytest.raises(CapacityDeferred, match="redirected_cache_namespace"):
        with capacity.reserve_cache_write(legacy / "redirected/groups", key="bad",
                maximum_growth_bytes=MIB, family="panel"):
            pytest.fail("descendant redirect admitted")
    legacy.unlink()
    legacy.symlink_to(tmp_path)
    with pytest.raises(CapacityDeferred, match="redirected_cache_namespace"):
        with capacity.reserve_cache_write(legacy / "groups", key="bad",
                maximum_growth_bytes=MIB, family="panel"):
            pytest.fail("unregistered compatibility target admitted")


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
