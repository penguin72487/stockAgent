from __future__ import annotations

import ast
import subprocess
from types import SimpleNamespace

import train


class _Fold:
    def __init__(self, fold_id: int) -> None:
        self.fold_id = fold_id


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

    assert command[-4:] == [
        "--mode",
        "infer",
        "--no-post-train-infer",
        "--no-isolate-train-folds",
    ]


def test_isolated_fold_runner_uses_sequential_children_and_stops_on_failure(
    monkeypatch,
) -> None:
    calls: list[tuple[list[str], dict[str, str]]] = []

    def fake_run(command, *, env, check):
        calls.append((command, env))
        return subprocess.CompletedProcess(command, 0 if len(calls) == 1 else 9)

    monkeypatch.setattr(train.subprocess, "run", fake_run)

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


def test_isolated_parent_rebuilds_complete_walkforward_with_panel_and_config() -> None:
    tree = ast.parse(train.Path(train.__file__).read_text(encoding="utf-8"))
    calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "_refresh_walkforward_artifacts"
    ]

    assert len(calls) == 1
    call = calls[0]
    assert isinstance(call.args[0], ast.Call)
    assert isinstance(call.args[1], ast.Name) and call.args[1].id == "results"
    keyword_names = {keyword.arg for keyword in call.keywords}
    assert keyword_names == {"panel", "config"}
