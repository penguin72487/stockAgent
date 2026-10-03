from __future__ import annotations

import ast
import os
import subprocess
from types import SimpleNamespace

import train
from stockagent.training import trainer as trainer_module


class _Fold:
    def __init__(self, fold_id: int) -> None:
        self.fold_id = fold_id


def test_materialized_snapshot_discovery_requires_exact_release_path(
    tmp_path, monkeypatch
) -> None:
    materialized_root = tmp_path / "materialized"
    monkeypatch.setenv("STOCKAGENT_MATERIALIZED_ROOT", str(materialized_root))
    exact = materialized_root / "tw-public" / "tw-public-release-123" / "stocks"
    config = SimpleNamespace(
        __dataclass_fields__={"data": object()},
        data=SimpleNamespace(
            __dataclass_fields__={"exact": object(), "current": object()},
            exact=str(exact),
            current=str(materialized_root / "current" / "tw-public" / "stocks"),
        ),
    )

    assert train._configured_materialized_snapshots(config) == (
        (
            materialized_root,
            "tw-public",
            "tw-public-release-123",
            exact.parent,
        ),
    )


def test_training_holds_existing_materialized_release_for_proc_monitor(
    tmp_path, monkeypatch
) -> None:
    train._release_materialized_dataset_holds()
    materialized_root = tmp_path / "materialized"
    monkeypatch.setenv("STOCKAGENT_MATERIALIZED_ROOT", str(materialized_root))
    target = materialized_root / "tw-public" / "tw-public-release-123"
    target.mkdir(parents=True)
    (target.parent / ".tw-public-release-123.READY.json").write_text("{}")
    config = SimpleNamespace(
        __dataclass_fields__={"path": object()},
        path=str(target / "features.parquet"),
    )

    def unexpected_restore(**kwargs):
        raise AssertionError(f"unexpected restore: {kwargs}")

    monkeypatch.setattr(train, "_restore_materialized_snapshot", unexpected_restore)
    train._hold_configured_materialized_snapshots(config)

    descriptor = train._MATERIALIZED_DATASET_HOLDS[target]
    assert os.readlink(f"/proc/self/fd/{descriptor}") == str(target)
    train._release_materialized_dataset_holds()


def test_training_restores_missing_exact_release_before_holding(
    tmp_path, monkeypatch
) -> None:
    train._release_materialized_dataset_holds()
    materialized_root = tmp_path / "materialized"
    monkeypatch.setenv("STOCKAGENT_MATERIALIZED_ROOT", str(materialized_root))
    target = materialized_root / "tw-public" / "tw-public-release-123"
    config = SimpleNamespace(
        __dataclass_fields__={"path": object()},
        path=str(target / "features.parquet"),
    )
    restored = []

    def fake_restore(*, dataset, snapshot_id):
        restored.append((dataset, snapshot_id))
        target.mkdir(parents=True)
        (target.parent / f".{snapshot_id}.READY.json").write_text("{}")

    monkeypatch.setattr(train, "_restore_materialized_snapshot", fake_restore)
    train._hold_configured_materialized_snapshots(config)

    assert restored == [("tw-public", "tw-public-release-123")]
    assert target in train._MATERIALIZED_DATASET_HOLDS
    train._release_materialized_dataset_holds()


def test_isolated_fold_command_appends_authoritative_single_fold_overrides() -> None:
    command = train._isolated_fold_command(
        [
            "--config",
            "experiment.yaml",
            "--start-fold",
            "2",
            "--max-folds",
            "8",
            "--post-train-infer",
        ],
        fold_id=7,
    )

    assert command[:2] == [
        train.sys.executable,
        str(train.Path(train.__file__).resolve()),
    ]
    assert command[-6:] == [
        "--start-fold",
        "7",
        "--max-folds",
        "1",
        "--no-post-train-infer",
        "--no-isolate-train-folds",
    ]


def test_isolated_inference_command_overrides_train_mode_in_fresh_process() -> None:
    command = train._isolated_inference_command(
        ["--config", "experiment.yaml", "--mode", "train"]
    )

    assert command[-6:] == [
        "--mode",
        "infer",
        "--multi-gpu-strategy",
        "none",
        "--no-post-train-infer",
        "--no-isolate-train-folds",
    ]


def test_isolated_fold_runner_uses_sequential_children_and_stops_on_failure(
    monkeypatch,
) -> None:
    calls: list[tuple[list[str], dict[str, str]]] = []

    def fake_run(command, *, env):
        calls.append((command, env))
        return subprocess.CompletedProcess(command, 0 if len(calls) == 1 else 9)

    monkeypatch.setattr(train, "_run_managed_subprocess", fake_run)

    try:
        train._run_isolated_train_fold_processes(
            [_Fold(3), _Fold(4), _Fold(5)],
            argv=["--config", "experiment.yaml"],
        )
    except RuntimeError as exc:
        assert "fold=4 returncode=9" in str(exc)
    else:
        raise AssertionError("expected the second isolated child failure")

    assert len(calls) == 2
    assert calls[0][0][-6:] == [
        "--start-fold",
        "3",
        "--max-folds",
        "1",
        "--no-post-train-infer",
        "--no-isolate-train-folds",
    ]
    assert calls[1][0][-6:] == [
        "--start-fold",
        "4",
        "--max-folds",
        "1",
        "--no-post-train-infer",
        "--no-isolate-train-folds",
    ]
    assert all(
        env[train._FOLD_ISOLATION_CHILD_ENV] == "1" for _, env in calls
    )


def test_isolated_fold_failure_replaces_stale_running_progress(
    tmp_path, monkeypatch
) -> None:
    progress = tmp_path / "progress.json"
    progress.write_text('{"state":"running","phase":"reporting"}\n')
    monkeypatch.setattr(
        train,
        "_run_managed_subprocess",
        lambda *args, **kwargs: subprocess.CompletedProcess(args[0], 17),
    )

    try:
        train._run_isolated_train_fold_processes(
            [_Fold(8)],
            argv=["--config", "experiment.yaml"],
            output_dir=tmp_path,
        )
    except RuntimeError:
        pass
    else:
        raise AssertionError("expected isolated child failure")

    payload = train.json.loads(progress.read_text())
    assert payload["state"] == "failed"
    assert payload["phase"] == "failed"
    assert payload["fold_id"] == 8
    assert payload["failure"]["returncode"] == 17


def test_ddp_relaunch_is_deferred_to_isolated_fold_child() -> None:
    config = SimpleNamespace(
        training=SimpleNamespace(multi_gpu_strategy="distributed_data_parallel"),
        runner=SimpleNamespace(isolate_train_folds=True),
    )
    args = SimpleNamespace(
        multi_gpu_strategy=None,
        isolate_train_folds=None,
    )

    # Returning without os.exec proves the outer orchestrator remains a
    # single process; the authoritative child command disables isolation and
    # therefore performs the normal torchrun relaunch itself.
    train._maybe_relaunch_for_ddp(config, args)


def test_single_selected_fold_still_uses_fresh_process_boundary(
    monkeypatch,
) -> None:
    monkeypatch.delenv(train._FOLD_ISOLATION_CHILD_ENV, raising=False)

    assert train._should_isolate_selected_folds(
        mode="train",
        isolate_train_folds=True,
        folds=[_Fold(12)],
    )

    monkeypatch.setenv(train._FOLD_ISOLATION_CHILD_ENV, "1")
    assert not train._should_isolate_selected_folds(
        mode="train",
        isolate_train_folds=True,
        folds=[_Fold(12)],
    )


def test_isolated_fold_child_defers_global_walkforward_refresh(monkeypatch) -> None:
    monkeypatch.delenv(train._FOLD_ISOLATION_CHILD_ENV, raising=False)
    assert not trainer_module._isolated_fold_child_enabled()

    monkeypatch.setenv(train._FOLD_ISOLATION_CHILD_ENV, "1")
    assert trainer_module._isolated_fold_child_enabled()


def test_isolated_lifecycle_finalizer_rejects_partial_fold_coverage() -> None:
    folds = [_Fold(1), _Fold(2)]
    results = [SimpleNamespace(fold_id=1)]

    try:
        trainer_module._finalize_isolated_training_lifecycle(
            SimpleNamespace(),
            folds,
            SimpleNamespace(),
            "unused",
            results,
        )
    except RuntimeError as exc:
        assert "expected=[1, 2] completed=[1]" in str(exc)
    else:
        raise AssertionError("expected partial isolated lifecycle to fail closed")


def test_isolated_parent_rebuilds_complete_walkforward_with_panel_and_config() -> None:
    tree = ast.parse(train.Path(train.__file__).read_text(encoding="utf-8"))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_refresh_walkforward_artifacts"
    ]

    # Refresh immediately after each child AND once before final completion.
    assert len(calls) == 2
    calls.sort(key=lambda call: call.lineno)
    for call, result_name in zip(calls, ("partial_results", "results"), strict=True):
        assert isinstance(call.args[0], ast.Call)
        assert isinstance(call.args[1], ast.Name) and call.args[1].id == result_name
        assert {keyword.arg for keyword in call.keywords} == {"panel", "config"}
    child_loops = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.For)
        and isinstance(node.target, ast.Name)
        and node.target.id == "pending_fold"
    ]
    assert len(child_loops) == 1
    assert calls[0] in list(ast.walk(child_loops[0]))
    assert calls[1] not in list(ast.walk(child_loops[0]))

    finalizers = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_finalize_isolated_training_lifecycle"
    ]
    assert len(finalizers) == 1
    assert finalizers[0].lineno > calls[-1].lineno


def test_inference_does_not_relaunch_training_ddp(monkeypatch) -> None:
    config = SimpleNamespace(
        training=SimpleNamespace(multi_gpu_strategy="distributed_data_parallel"),
        runner=SimpleNamespace(mode="infer", isolate_train_folds=False),
    )
    args = SimpleNamespace(mode=None, multi_gpu_strategy="distributed_data_parallel")
    def forbidden(*args, **kwargs):
        raise AssertionError("inference must not enter training DDP setup")
    monkeypatch.setattr(train, "_resolve_multi_gpu_strategy", forbidden)
    train._maybe_relaunch_for_ddp(config, args)
