"""Immutable representation never changes financial state or hides corruption."""

from copy import deepcopy
from datetime import datetime, timedelta
import json
import os
import stat

import pytest

from stockagent.live import paper_mode_snapshot as store
from stockagent.live import tw_day_trade_simulation as simulation
from stockagent.live.tw_day_trade_service_sync import (
    PaperReadChanged,
    committed_paper_read_guard,
    load_service_sync,
)


CLOCK = datetime.fromisoformat("2026-10-06T09:00:05+08:00")


def state():
    return {
        "schema_version": simulation.SIMULATION_SCHEMA_VERSION,
        "created_at": CLOCK.isoformat(),
        "simulation_only": True,
        "production_order_possible": False,
        "modes": {
            f"fixture_{i}": {
                "market": f"fixture_{i}",
                "signed_zero": -0.0,
                "large_integer": 2**100,
                "positions": {},
                "margin_exit_capacity_used": {
                    f"2026-10-05:13:20:{j}": j * 1000 for j in range(2000)
                },
                "target_risk": {"gross": 0.1},
                "名字": "台積電",
            }
            for i in range(6)
        },
        "benchmarks": {},
        "minute_liquidity": {},
    }


def publish(root, value):
    writer = store.ModeSnapshotWriter(root)
    manifest = writer.prepare(value)
    simulation._atomic_json(root / "state.json", manifest)
    return writer, manifest


def test_six_mode_roundtrip_bit_identity_and_legacy_compatibility(tmp_path):
    value = state()
    writer, manifest = publish(tmp_path, value)
    result = store.read_paper_state(tmp_path / "state.json")
    assert simulation.ledger_bytes(result) == simulation.ledger_bytes(value)
    assert store.expand_paper_state(value, tmp_path) is value
    assert "modes" not in manifest
    assert writer.last_metrics["mode_count"] == 6
    assert all(
        reference["sections"]
        for reference in manifest[store.MANIFEST_KEY]["objects"].values()
    )
    assert list(tmp_path.rglob(".pending-*")) == []


@pytest.mark.parametrize(
    "payload",
    [
        None,
        [],
        {},
        {"schema_version": 999},
        {"schema_version": True},
        {"schema_version": simulation.SIMULATION_SCHEMA_VERSION, "modes": []},
    ],
)
def test_corrupt_or_unknown_canonical_checkpoint_never_resets_to_empty(
    tmp_path, payload
):
    path = tmp_path / "state.json"
    path.write_text(json.dumps(payload))
    before = path.read_bytes()
    with pytest.raises(ValueError):
        simulation.TwDayTradeSimulationEngine(tmp_path)
    assert path.read_bytes() == before
    assert not (tmp_path / "service_sync.json").exists()


@pytest.mark.parametrize("name", list(simulation.PAPER_JOURNAL_FILENAMES))
def test_lost_checkpoint_with_recorded_execution_requires_recovery_not_empty_account(
    tmp_path, name
):
    path = tmp_path / name
    path.write_bytes(b'{"retained_record":true}\n')
    before = path.read_bytes()
    with pytest.raises(ValueError, match="explicit recovery"):
        simulation.TwDayTradeSimulationEngine(tmp_path)
    assert path.read_bytes() == before
    assert not (tmp_path / "state.json").exists()


def test_fifo_checkpoint_is_rejected_without_blocking(tmp_path):
    os.mkfifo(tmp_path / "state.json")
    with pytest.raises(ValueError, match="regular file"):
        simulation.TwDayTradeSimulationEngine(tmp_path)


def test_commit_child_fence_rejects_fifo_checkpoint_without_blocking(tmp_path):
    os.mkfifo(tmp_path / "state.json")
    with pytest.raises(ValueError, match="regular file"):
        store.mode_object_generations(tmp_path)


def test_root_checkpoint_read_budget_applies_to_commit_child_fence(
    tmp_path, monkeypatch
):
    publish(tmp_path, state())
    monkeypatch.setattr(store, "MAX_SNAPSHOT_BYTES", 16)
    with pytest.raises(ValueError, match="bounded regular"):
        store.mode_object_generations(tmp_path)


def test_one_mode_header_change_reuses_all_unchanged_large_sections(tmp_path):
    value = state()
    writer, before = publish(tmp_path, value)
    generations = store.mode_object_generations(tmp_path)
    value["modes"]["fixture_0"]["target_risk"]["gross"] = 0.2
    after = writer.prepare(value)
    simulation._atomic_json(tmp_path / "state.json", after)
    old = before[store.MANIFEST_KEY]["objects"]
    new = after[store.MANIFEST_KEY]["objects"]
    assert old["fixture_0"]["sha256"] != new["fixture_0"]["sha256"]
    assert old["fixture_0"]["sections"] == new["fixture_0"]["sections"]
    assert all(old[f"fixture_{i}"] == new[f"fixture_{i}"] for i in range(1, 6))
    assert writer.last_metrics["created_objects"] == 1
    assert writer.last_metrics["new_object_bytes"] < 1024
    current = store.mode_object_generations(tmp_path)
    assert all(
        current[name] == generation
        for name, generation in generations.items()
        if name in current
    )
    assert simulation.ledger_bytes(
        store.read_paper_state(tmp_path / "state.json")
    ) == simulation.ledger_bytes(value)


def test_header_uses_each_already_encoded_value_once_without_losing_legacy_values(
    tmp_path, monkeypatch
):
    from decimal import Decimal

    value = state()
    small = {
        "float": -0.0,
        "integer": 2**100,
        "unicode": "價格",
        "legacy": Decimal("1.234567890123"),
    }
    value["modes"]["fixture_0"]["small"] = small
    calls, original = [], store.ledger_bytes

    def encode(item):
        if item is small:
            calls.append(item)
        elif isinstance(item, dict) and item.get("small") is small:
            pytest.fail("header must not encode an already encoded subtree again")
        return original(item)

    monkeypatch.setattr(store, "ledger_bytes", encode)
    publish(tmp_path, value)
    assert len(calls) == 1
    result = store.read_paper_state(tmp_path / "state.json")
    assert simulation.ledger_bytes(result) == simulation.ledger_bytes(value)


@pytest.mark.parametrize(
    "damage", ["delete", "rewrite", "replace", "symlink", "fifo", "truncate"]
)
def test_child_damage_never_becomes_flat_or_a_cached_good_result(tmp_path, damage):
    _, manifest = publish(tmp_path, state())
    expected = store.read_paper_state(tmp_path / "state.json")
    reference = manifest[store.MANIFEST_KEY]["objects"]["fixture_0"]
    path = tmp_path / store.OBJECT_DIRECTORY / (reference["sha256"] + ".json")
    body = path.read_bytes()
    if damage == "rewrite":
        path.write_bytes(body.replace(b"-0.0", b" 0.0"))
    elif damage == "replace":
        replacement = path.with_suffix(".replacement")
        replacement.write_bytes(body.replace(b"-0.0", b" 0.0"))
        os.replace(replacement, path)
    else:
        path.unlink()
        if damage == "symlink":
            target = tmp_path / "external"
            target.write_bytes(body)
            path.symlink_to(target)
        elif damage == "fifo":
            os.mkfifo(path)
        elif damage == "truncate":
            path.write_bytes(body[:-1])
    with pytest.raises((ValueError, OSError)):
        store.read_paper_state(tmp_path / "state.json")
    assert len(expected["modes"]) == 6
    if damage in {"rewrite", "replace", "symlink", "fifo", "truncate"}:
        with pytest.raises((ValueError, OSError)):
            store.ModeSnapshotWriter(tmp_path).prepare(state())


@pytest.mark.parametrize(
    "damage", ["contract", "hash", "size", "sections", "inline", "traversal"]
)
def test_manifest_validation_fails_closed(tmp_path, damage):
    _, manifest = publish(tmp_path, state())
    reference = manifest[store.MANIFEST_KEY]["objects"]["fixture_0"]
    if damage == "contract":
        manifest[store.MANIFEST_KEY]["contract"] = "future_unknown"
    elif damage == "hash":
        reference["sha256"] = "0" * 64
    elif damage == "traversal":
        reference["sha256"] = "../../outside"
    elif damage == "size":
        reference["size_bytes"] = True
    elif damage == "sections":
        reference["sections"] = []
    else:
        manifest["modes"] = {}
    simulation._atomic_json(tmp_path / "state.json", manifest)
    with pytest.raises((ValueError, OSError)):
        store.read_paper_state(tmp_path / "state.json")


def test_reader_returns_owned_values_not_mutable_cached_dictionaries(tmp_path):
    publish(tmp_path, state())
    first = store.read_paper_state(tmp_path / "state.json")
    first["modes"]["fixture_0"]["target_risk"]["gross"] = 999
    first["modes"]["fixture_0"]["margin_exit_capacity_used"].clear()
    assert (
        store.read_paper_state(tmp_path / "state.json")["modes"]["fixture_0"]
        == state()["modes"]["fixture_0"]
    )


def test_insufficient_capacity_defers_before_creating_objects_or_state(
    tmp_path, monkeypatch
):
    from types import SimpleNamespace

    monkeypatch.setattr(
        store.os, "fstatvfs", lambda _fd: SimpleNamespace(f_bavail=1, f_frsize=4096)
    )
    with pytest.raises(OSError, match="capacity admission"):
        publish(tmp_path, state())
    assert not (tmp_path / "state.json").exists()
    assert list((tmp_path / store.OBJECT_DIRECTORY).iterdir()) == []


def test_monitor_manifest_cache_does_not_hide_mutated_children(tmp_path):
    from stockagent.live import shioaji_api_dashboard as monitor

    _, manifest = publish(tmp_path, state())
    assert monitor._read_json(tmp_path / "state.json")["modes"] == state()["modes"]
    reference = manifest[store.MANIFEST_KEY]["objects"]["fixture_0"]
    path = tmp_path / store.OBJECT_DIRECTORY / (reference["sha256"] + ".json")
    path.write_bytes(path.read_bytes().replace(b"-0.0", b" 0.0"))
    assert monitor._read_json(tmp_path / "state.json") is None


@pytest.mark.parametrize(
    "barrier",
    ["object_file", "object_directory", "state", "group_directory", "marker", "ack"],
)
def test_failed_commit_never_acknowledges_new_state_and_retry_recovers(
    tmp_path, monkeypatch, barrier
):
    engine = simulation.TwDayTradeSimulationEngine(
        tmp_path, immutable_mode_snapshots=True
    )
    engine.state = state()
    engine._persist(CLOCK)
    old_revision = load_service_sync(tmp_path)["state_revision"]
    old_objects = set((tmp_path / store.OBJECT_DIRECTORY).glob("*.json"))
    engine.state["modes"]["fixture_0"]["target_risk"]["gross"] = 0.2
    original_sync, original_atomic = os.fsync, simulation._atomic_json
    hit = []

    def sync(fd):
        path = os.readlink(f"/proc/self/fd/{fd}")
        directory = stat.S_ISDIR(os.fstat(fd).st_mode)
        matches = (
            barrier == "object_file"
            and not directory
            and ".pending-" in path
            or barrier == "object_directory"
            and directory
            and path.endswith(store.OBJECT_DIRECTORY)
            or barrier == "group_directory"
            and directory
            and path == str(tmp_path)
        )
        if matches and not hit:
            hit.append(barrier)
            raise OSError("injected durability failure")
        original_sync(fd)

    def atomic(path, payload, **kwargs):
        expected = {
            "state": "state.json",
            "marker": "service_sync.json",
            "ack": "service_sync_durable_ack.json",
        }.get(barrier)
        if path.name == expected and not hit:
            hit.append(barrier)
            raise OSError("injected publication failure")
        return original_atomic(path, payload, **kwargs)

    monkeypatch.setattr(os, "fsync", sync)
    monkeypatch.setattr(simulation, "_atomic_json", atomic)
    with pytest.raises(OSError, match="injected"):
        engine._persist(CLOCK + timedelta(seconds=1))
    assert hit == [barrier]
    receipt = load_service_sync(tmp_path)
    assert receipt is None or receipt["state_revision"] == old_revision
    if barrier in {"group_directory", "marker", "ack"}:
        with pytest.raises(PaperReadChanged):
            with committed_paper_read_guard(tmp_path):
                pass
    assert old_objects.issubset(set((tmp_path / store.OBJECT_DIRECTORY).glob("*.json")))
    assert list(tmp_path.rglob(".pending-*")) == []
    engine._persist(CLOCK + timedelta(seconds=2))
    with committed_paper_read_guard(tmp_path):
        assert store.read_paper_state(engine.state_path) == engine.state
    restored = simulation.TwDayTradeSimulationEngine(tmp_path)
    assert restored._mode_snapshot_writer is not None
    expected = deepcopy(engine.state["modes"])
    for mode in expected.values():
        mode["executed_positions_path"] = str(engine.positions_path)
    assert restored.state["modes"] == expected


def test_object_rewrite_during_projection_publication_invalidates_commit(
    tmp_path, monkeypatch
):
    engine = simulation.TwDayTradeSimulationEngine(
        tmp_path, immutable_mode_snapshots=True
    )
    engine.state = state()
    original = simulation._atomic_json

    def atomic(path, payload, **kwargs):
        original(path, payload, **kwargs)
        if path.name == "positions.json":
            reference = json.loads(engine.state_path.read_bytes())[store.MANIFEST_KEY][
                "objects"
            ]["fixture_0"]
            child = tmp_path / store.OBJECT_DIRECTORY / (reference["sha256"] + ".json")
            child.write_bytes(child.read_bytes().replace(b"-0.0", b" 0.0"))

    monkeypatch.setattr(simulation, "_atomic_json", atomic)
    with pytest.raises(OSError, match="mode objects changed"):
        engine._persist(CLOCK)
    assert load_service_sync(tmp_path) is None


def test_child_rewrite_invalidates_guard_even_when_parent_has_not_changed(tmp_path):
    engine = simulation.TwDayTradeSimulationEngine(
        tmp_path, immutable_mode_snapshots=True
    )
    engine.state = state()
    engine._persist(CLOCK)
    with committed_paper_read_guard(tmp_path):
        pass
    parent = engine.state_path.read_bytes()
    ref = json.loads(parent)[store.MANIFEST_KEY]["objects"]["fixture_0"]
    child = tmp_path / store.OBJECT_DIRECTORY / (ref["sha256"] + ".json")
    child.write_bytes(child.read_bytes().replace(b"-0.0", b" 0.0"))
    assert engine.state_path.read_bytes() == parent
    with pytest.raises(PaperReadChanged):
        with committed_paper_read_guard(tmp_path):
            pass


def test_six_modes_three_revisions_have_complete_identical_state_and_journals(tmp_path):
    from dataclasses import replace
    from scripts.benchmark_opening_signal_pipeline import (
        _new_isolated_entry_engine,
        _paper_ledger_financial_oracle,
    )
    from test_opening_signal_revision import register, revision_spec
    from test_tw_day_trade_simulation import _now

    results = []
    for partitioned in (False, True):
        root = tmp_path / str(partitioned)
        root.mkdir()
        engine = _new_isolated_entry_engine(
            root / "ledger",
            created_at=_now(8, 50),
            immutable_mode_snapshots=partitioned,
        )
        base = revision_spec(root)
        previous, proofs = {}, []
        for index, weight in enumerate((0.4, -0.2, 0.0)):
            for mode in range(6):
                spec = replace(base, market=f"fixture_{mode}")
                previous[mode] = register(
                    engine, spec, index, weight, previous.get(mode)
                )
                proofs.append(_paper_ledger_financial_oracle(engine))
        assert proofs[-1]["journals"]["signals.jsonl"]["records"] == 18
        assert proofs[-1]["journals"]["fills.jsonl"]["records"] > 0
        results.append(proofs)
    assert results[0] == results[1]


def test_dashboard_reader_does_not_serve_good_parent_cache_after_child_corruption(
    tmp_path,
):
    from stockagent.live.tw_day_trade_dashboard import _object

    _, manifest = publish(tmp_path, state())
    assert len(_object(tmp_path / "state.json")["modes"]) == 6
    reference = manifest[store.MANIFEST_KEY]["objects"]["fixture_0"]["sections"][
        "margin_exit_capacity_used"
    ]
    child = tmp_path / store.OBJECT_DIRECTORY / (reference["sha256"] + ".json")
    child.write_bytes(child.read_bytes().replace(b"1000", b"2000", 1))
    with pytest.raises((ValueError, OSError)):
        _object(tmp_path / "state.json")


@pytest.mark.parametrize(
    "module,function",
    [
        ("stockagent.live.day_trade_addition", "_object"),
        ("scripts.reload_tw_day_trade_services_if_flat", "read_object"),
        ("scripts.import_tw_day_trade_market_history", "_object"),
        ("scripts.promote_tw_day_trade_replay", "_load_object"),
        ("scripts.deploy_tw_day_trade_multi_basis_22_history", "_object"),
        ("scripts.repair_tw_day_trade_flat_opening", "_object"),
        ("scripts.rebuild_tw_day_trade_minute_curves", "_read_json"),
        ("scripts.maintain_tw_day_trade_minute_curves", "_object"),
        ("scripts.audit_tw_day_trade_live_readiness", "_read_json"),
    ],
)
def test_repair_and_operational_readers_accept_the_same_complete_snapshot(
    tmp_path, module, function
):
    import importlib

    value = state()
    publish(tmp_path, value)
    result = getattr(importlib.import_module(module), function)(tmp_path / "state.json")
    assert simulation.ledger_bytes(result) == simulation.ledger_bytes(value)


def test_layout_benchmark_keeps_complete_values_and_separates_first_write(tmp_path):
    from scripts.benchmark_day_trade_persistence import benchmark_mode_snapshots

    source = tmp_path / "source.json"
    value = {**state(), "updated_at": CLOCK.isoformat()}
    simulation._atomic_json(source, value)
    before = source.read_bytes()
    report = benchmark_mode_snapshots(
        source, repeats=3, output=tmp_path / "output/result.json"
    )
    assert report["full_publication_values_identical"] is True
    assert report["post_fsync_ack_verified_each_sample"] is True
    assert report["mode_count"] == 6
    assert source.read_bytes() == before
    samples = report["samples"]
    assert [row["variant"] for row in samples] == [
        "inline_state",
        "immutable_mode_objects",
        "immutable_mode_objects",
        "inline_state",
        "inline_state",
        "immutable_mode_objects",
    ]
    assert sum(row["first_write"] for row in samples) == 2
    partitioned = [row for row in samples if row["variant"] == "immutable_mode_objects"]
    assert partitioned[0]["state_publication"]["mode_snapshot"]["created_objects"] > 1
    assert all(
        row["state_publication"]["mode_snapshot"]["created_objects"] == 1
        for row in partitioned[1:]
    )
