from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.maintain_tw_overnight_history import _deployment_current
from scripts.rebuild_tw_overnight_history import (
    _check_source_revision,
    _history_lineage,
    _model_source_paths,
    _require_compatible_history_cache,
    _require_current_daily_receipt,
    _source_generation,
)


def test_disabled_overnight_history_is_idle_without_calendar_or_rebuild(
    tmp_path: Path, monkeypatch,
) -> None:
    from scripts import maintain_tw_overnight_history as maintenance

    monkeypatch.setattr(maintenance, "load_market_configs", lambda *_args: {})
    monkeypatch.setattr(
        maintenance,
        "configured_history_lineage",
        lambda *_args: pytest.fail("disabled mode must not load history code"),
    )
    monkeypatch.setattr(
        maintenance,
        "_latest_completed_session",
        lambda **_kwargs: pytest.fail("disabled mode must not scan the calendar"),
    )
    monkeypatch.setattr(
        maintenance.subprocess,
        "run",
        lambda *_args, **_kwargs: pytest.fail("disabled mode must not rebuild"),
    )
    receipts = []
    monkeypatch.setattr(
        maintenance,
        "atomic_write_json",
        lambda path, payload: receipts.append((path, payload)),
    )
    args = SimpleNamespace(
        state_dir=tmp_path / "state",
        work_dir=tmp_path / "work",
        markets_dir=tmp_path / "markets",
        public_root=tmp_path,
        start_date="2026-02-25",
    )
    result = maintenance.maintain(args)
    assert result["status"] == "idle_no_enabled_modes"
    assert result["enabled_market_count"] == 0
    assert len(receipts) == 1
    assert receipts[0][0].name == "latest_attempt.json"
    assert not (tmp_path / "state/overnight_history.json").exists()


def _market(config: str, checkpoint: str = "c" * 64) -> dict[str, str]:
    return {
        "market": "tw_overnight_fixture",
        "checkpoint_sha256": checkpoint,
        "market_config_sha256": config,
    }


def test_history_lineage_is_stable_for_new_sessions_but_changes_with_model_contract() -> None:
    first = _history_lineage("2026-02-25", [_market("a" * 64)])
    same = _history_lineage("2026-02-25", [_market("a" * 64)])
    changed = _history_lineage("2026-02-25", [_market("b" * 64)])

    assert first == same
    assert first["fingerprint_sha256"] != changed["fingerprint_sha256"]
    assert "end_date" not in first


def test_history_source_generation_is_stable_and_changes_with_any_input() -> None:
    plan = {
        "history_lineage": {"fingerprint_sha256": "lineage"},
        "source_files": {"official": {"path": "/official", "sha256": "a"}},
        "model_source_files": {"model": {"path": "/model", "sha256": "b"}},
        "price_limit_files": {"2026-09-24": {
            "path": "/limits/2026-09-24.parquet", "sha256": "c",
        }},
    }
    first = _source_generation(plan)
    assert first == _source_generation(json.loads(json.dumps(plan)))
    plan["model_source_files"]["model"]["sha256"] = "c"
    assert _source_generation(plan) != first
    plan["model_source_files"]["model"]["sha256"] = "b"
    plan["source_files"]["official"]["sha256"] = "d"
    assert _source_generation(plan) != first
    plan["source_files"]["official"]["sha256"] = "a"
    plan["price_limit_files"]["2026-09-24"]["sha256"] = "d"
    assert _source_generation(plan) != first


def test_maintenance_uses_source_generation_and_rechecks_before_deploy(
    tmp_path, monkeypatch,
) -> None:
    from scripts import maintain_tw_overnight_history as maintenance

    work = tmp_path / "work"
    state = tmp_path / "state"
    source = tmp_path / "official"
    source.write_bytes(b"source revision")
    lineage = "a" * 64
    checked = []
    deployed = []
    receipts = []
    monkeypatch.setattr(
        maintenance,
        "load_market_configs",
        lambda *_args: {
            "overnight": SimpleNamespace(enabled=True, overnight_simulation_enabled=True)
        },
    )
    monkeypatch.setattr(
        maintenance, "_latest_completed_session",
        lambda **_kwargs: (date(2026, 9, 24), [date(2026, 9, 24)], "calendar"),
    )
    monkeypatch.setattr(
        maintenance, "configured_history_lineage",
        lambda *_args: {"fingerprint_sha256": lineage},
    )
    monkeypatch.setattr(maintenance, "_deployment_current", lambda *_args: False)
    monkeypatch.setattr(
        maintenance, "atomic_write_json",
        lambda path, payload: receipts.append((path, payload)),
    )
    monkeypatch.setattr(
        maintenance, "_check_source_revision", lambda plan: checked.append(plan),
    )
    monkeypatch.setattr(
        maintenance, "deploy",
        lambda output, target: deployed.append((output, target)) or {"status": "ready"},
    )

    def fake_run(command, **_kwargs):
        assert "--versioned-output" in command
        root = tmp_path / "work/lineages" / lineage
        plan = {
            "history_lineage": {"fingerprint_sha256": lineage},
            "source_files": {"official": {
                "path": str(source), "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
            }},
            "model_source_files": {},
            "sessions": ["2026-09-24"],
            "price_limit_files": {"2026-09-24": {
                "path": str(source), "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
            }},
            "end_date": "2026-09-24",
        }
        generation = _source_generation(plan)
        plan["source_generation_sha256"] = generation
        output = root / "source_generations" / generation
        output.mkdir(parents=True)
        (output / "plan.json").write_text(json.dumps(plan))
        receipt_path = command[command.index("--resolved-output-receipt") + 1]
        with open(receipt_path, "w", encoding="utf-8") as handle:
            json.dump({
                "output": str(output),
                "source_generation_sha256": generation,
                "history_lineage_fingerprint": lineage,
                "end_date": "2026-09-24",
            }, handle)

    monkeypatch.setattr(maintenance.subprocess, "run", fake_run)
    args = SimpleNamespace(
        state_dir=state, work_dir=work, markets_dir=tmp_path / "markets",
        public_root=tmp_path, minute_root=tmp_path, price_limit_dir=tmp_path,
        start_date="2026-02-25", force=False,
    )
    result = maintenance.maintain(args)
    assert len(checked) == 1
    assert len(deployed) == 1
    assert deployed[0][0] == Path(result["lineage_work_dir"])
    assert result["lineage_work_dir"].endswith(
        checked[0]["source_generation_sha256"]
    )
    assert len(receipts) == 1


@pytest.mark.parametrize(
    "tamper", ["outside", "generation", "end_date", "missing_limits"]
)
def test_maintenance_rejects_wrong_generation_receipt(
    tmp_path, monkeypatch, tamper: str,
) -> None:
    from scripts import maintain_tw_overnight_history as maintenance

    lineage = "a" * 64
    root = tmp_path / "lineages" / lineage
    root.mkdir(parents=True)
    plan = {
        "history_lineage": {"fingerprint_sha256": lineage},
        "source_files": {}, "model_source_files": {},
        "sessions": ["2026-09-24"],
        "price_limit_files": {"2026-09-24": {
            "path": "/limits/2026-09-24.parquet", "sha256": "a",
        }},
        "end_date": "2026-09-24",
    }
    if tamper == "missing_limits":
        plan["price_limit_files"] = {}
    generation = _source_generation(plan)
    plan["source_generation_sha256"] = generation
    output = (
        tmp_path / "outside" if tamper == "outside"
        else root / "source_generations" / generation
    )
    output.mkdir(parents=True)
    (output / "plan.json").write_text(json.dumps(plan))
    receipt = {
        "output": str(output),
        "source_generation_sha256": (
            "0" * 64 if tamper == "generation" else generation
        ),
        "history_lineage_fingerprint": lineage,
        "end_date": "2026-09-23" if tamper == "end_date" else "2026-09-24",
    }
    (root / "resolved_output.json").write_text(json.dumps(receipt))
    monkeypatch.setattr(
        maintenance, "_check_source_revision",
        lambda _plan: pytest.fail("tampered output must not reach source validation"),
    )
    with pytest.raises(
        RuntimeError, match="source generations|does not match|lacks complete"
    ):
        maintenance._verified_rebuild_output(root, "2026-09-24", lineage)


def test_history_source_revision_detects_content_change_during_replay(tmp_path) -> None:
    source = tmp_path / "official.parquet"
    source.write_bytes(b"original")
    plan = {"source_files": {"twse_daily": {
        "path": str(source), "sha256": hashlib.sha256(b"original").hexdigest(),
    }}}
    signatures = _check_source_revision(plan)
    assert _check_source_revision(plan, signatures) == signatures

    source.write_bytes(b"original")
    signatures = _check_source_revision(plan, signatures)

    source.write_bytes(b"replacement")
    with pytest.raises(RuntimeError, match="Source changed while computing history"):
        _check_source_revision(plan, signatures)


def test_history_source_revision_includes_model_inputs(tmp_path) -> None:
    official = tmp_path / "official.parquet"
    model = tmp_path / "model.parquet"
    official.write_bytes(b"official")
    model.write_bytes(b"model")
    plan = {
        "source_files": {"official": {
            "path": str(official), "sha256": hashlib.sha256(b"official").hexdigest(),
        }},
        "model_source_files": {str(model): {
            "path": str(model), "sha256": hashlib.sha256(b"model").hexdigest(),
        }},
    }
    signatures = _check_source_revision(plan)
    model.write_bytes(b"changed")
    with pytest.raises(RuntimeError, match="Source changed while computing history"):
        _check_source_revision(plan, signatures)


def test_history_source_revision_includes_daily_price_limits(tmp_path) -> None:
    limits = tmp_path / "limits.parquet"
    limits.write_bytes(b"original")
    plan = {
        "source_files": {},
        "price_limit_files": {"2026-09-24": {
            "path": str(limits),
            "sha256": hashlib.sha256(limits.read_bytes()).hexdigest(),
        }},
    }
    signatures = _check_source_revision(plan)
    limits.write_bytes(b"changed")
    with pytest.raises(RuntimeError, match="Source changed while computing history"):
        _check_source_revision(plan, signatures)


def test_overnight_model_source_inventory_covers_panel_inputs(tmp_path, monkeypatch) -> None:
    from scripts import rebuild_tw_overnight_history as history

    root = tmp_path / "stocks"
    root.mkdir()
    tail = root / "_hot_tail"
    tail.mkdir()
    features = tmp_path / "features"
    features.mkdir()
    names = (
        root / "2330_features.parquet",
        tail / "2330_features.parquet",
        root / "symbols.csv",
        features / "tw_public_stock_daily.parquet",
        features / "tw_public_stock_daily.summary.json",
        tmp_path / "tw_corporate_action_reference.parquet",
        tmp_path / "tw_corporate_action_reference.summary.json",
        tmp_path / "tw_corporate_action_entitlements.parquet",
        tmp_path / "tw_corporate_action_entitlements.summary.json",
    )
    for path in names:
        path.write_bytes(b"source")
    config_path = tmp_path / "fixture.yaml"
    config_path.write_text("fixture")
    data = SimpleNamespace(
        parquet_root=str(root), use_external_features=False,
        use_tw_public_features=True, use_tw_public_rules=True,
        tw_public_feature_path=str(names[3]), tw_public_market_symbol="__MARKET__",
        overnight_1325_root=None,
    )
    monkeypatch.setattr(history, "load_config", lambda _path: SimpleNamespace(data=data))
    discovered = _model_source_paths([str(config_path)])
    assert discovered == sorted(
        [config_path.resolve(), *(path.resolve() for path in names)]
    )
    plan = {
        "source_files": {}, "model_config_paths": [str(config_path)],
        "model_source_files": {
            str(path): {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            for path in discovered
        },
    }
    _check_source_revision(plan)
    (root / "2317_features.parquet").write_bytes(b"new listing")
    with pytest.raises(RuntimeError, match="Model source inventory changed"):
        _check_source_revision(plan)


def test_history_cache_rejects_changed_official_source_before_replay(tmp_path) -> None:
    previous = {"source_files": {"twse_daily": {"sha256": "old"}}}
    current = {"source_files": {"twse_daily": {"sha256": "new"}}}
    _require_compatible_history_cache(previous, current, tmp_path)
    receipt = tmp_path / "inputs" / "2026-09-24" / "source.json"
    receipt.parent.mkdir(parents=True)
    receipt.write_text("{}")
    with pytest.raises(RuntimeError, match="cannot reuse this cache namespace"):
        _require_compatible_history_cache(previous, current, tmp_path)
    _require_compatible_history_cache(current, current, tmp_path)


def test_history_cache_rejects_changed_model_input_before_replay(tmp_path) -> None:
    previous = {
        "source_files": {"twse_daily": {"sha256": "same"}},
        "model_source_files": {"stock": {"sha256": "old"}},
    }
    current = {
        "source_files": {"twse_daily": {"sha256": "same"}},
        "model_source_files": {"stock": {"sha256": "new"}},
    }
    receipt = tmp_path / "signals" / "mode" / "2026-09-24" / "replay_signal.json"
    receipt.parent.mkdir(parents=True)
    receipt.write_text("{}")
    with pytest.raises(RuntimeError, match=r"model_inputs_changed\(1\)"):
        _require_compatible_history_cache(previous, current, tmp_path)
    unversioned = {"source_files": previous["source_files"]}
    with pytest.raises(RuntimeError, match=r"model_inputs_unversioned\(1\)"):
        _require_compatible_history_cache(unversioned, current, tmp_path)


def test_cached_overnight_day_requires_same_limits_and_minute_partition(tmp_path) -> None:
    limits = tmp_path / "limits.parquet"
    limits.write_bytes(b"official limits")
    prior = {
        "source_hashes": {"price_limits": hashlib.sha256(limits.read_bytes()).hexdigest()},
        "minute_partition_sha256": "minute-a",
    }
    _require_current_daily_receipt(
        prior, day="2026-09-24", limits_path=limits,
        minute_partition_sha256="minute-a",
    )
    with pytest.raises(RuntimeError, match="minute partition changed"):
        _require_current_daily_receipt(
            prior, day="2026-09-24", limits_path=limits,
            minute_partition_sha256="minute-b",
        )
    limits.write_bytes(b"revised limits")
    with pytest.raises(RuntimeError, match="price limits changed"):
        _require_current_daily_receipt(
            prior, day="2026-09-24", limits_path=limits,
            minute_partition_sha256="minute-a",
        )


def test_deployed_overnight_history_is_stale_when_official_source_changes(tmp_path) -> None:
    state = tmp_path / "state"
    source = tmp_path / "source"
    state.mkdir()
    source.mkdir()
    official = tmp_path / "official.parquet"
    model_source = tmp_path / "model.parquet"
    limits_dir = tmp_path / "limits"
    limits_dir.mkdir()
    limits = limits_dir / "2026-09-24.parquet"
    official.write_bytes(b"first revision")
    model_source.write_bytes(b"first model")
    limits.write_bytes(b"first limits")

    def digest(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()

    plan_path = source / "plan.json"
    plan_path.write_text(json.dumps({
        "sessions": ["2026-09-24"],
        "source_files": {"twse_daily": {
            "path": str(official), "sha256": digest(official),
        }},
        "model_source_files": {str(model_source): {
            "path": str(model_source), "sha256": digest(model_source),
        }},
    }))
    inputs = source / "inputs/2026-09-24"
    inputs.mkdir(parents=True)
    (inputs / "source.json").write_text(json.dumps({
        "source_hashes": {"price_limits": digest(limits)},
    }))
    history = state / "overnight_history.json"
    signals = state / "overnight_signal_history.parquet"
    events = state / "overnight_event_history.parquet"
    history.write_text(json.dumps({"status": "ready", "end_date": "2026-09-24"}))
    signals.write_bytes(b"signals")
    events.write_bytes(b"events")
    deployment = state / "overnight_history_deployment.json"
    deployment.write_text(json.dumps({
        "source": str(source), "source_plan_sha256": digest(plan_path),
        "history_lineage_fingerprint": "lineage", "end_date": "2026-09-24",
        "history_sha256": digest(history),
        "signal_history_sha256": digest(signals),
        "event_history_sha256": digest(events),
    }))

    assert _deployment_current(state, "2026-09-24", "lineage")
    assert _deployment_current(state, "2026-09-24", "lineage", limits_dir)
    limits.write_bytes(b"corrected limits")
    assert not _deployment_current(state, "2026-09-24", "lineage", limits_dir)
    limits.write_bytes(b"first limits")
    assert _deployment_current(state, "2026-09-24", "lineage", limits_dir)
    official.write_bytes(b"corrected revision")
    assert not _deployment_current(state, "2026-09-24", "lineage")
    official.write_bytes(b"first revision")
    assert _deployment_current(state, "2026-09-24", "lineage")
    model_source.write_bytes(b"corrected model source")
    assert not _deployment_current(state, "2026-09-24", "lineage")
    model_source.write_bytes(b"first model")
    assert _deployment_current(state, "2026-09-24", "lineage")
    plan_path.write_text("{}")
    assert not _deployment_current(state, "2026-09-24", "lineage")
