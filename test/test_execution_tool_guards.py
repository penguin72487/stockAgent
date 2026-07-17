from __future__ import annotations

import ast
from pathlib import Path

import pytest

from stockagent.backtest.tw_execution import require_naive_execution_for_tool


REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_DRIVEN_NAIVE_TOOLS = (
    "scripts/benchmark_inference_chunks.py",
    "scripts/benchmark_windowed_pipeline.py",
    "scripts/benchmark_postprocess.py",
    "scripts/find_tw_boost_seed_with_gradients.py",
    "scripts/diagnose_convergence.py",
    "scripts/benchmark_transformer_hotpath.py",
)


def _statement_calls(statement: ast.stmt, function_name: str) -> bool:
    return any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == function_name
        for node in ast.walk(statement)
    )


def test_naive_execution_tool_guard_accepts_naive() -> None:
    assert (
        require_naive_execution_for_tool("naive", tool_name="example.py")
        == "naive"
    )


@pytest.mark.parametrize("execution_mode", ("tw_cash", "tw_day_trade"))
def test_naive_execution_tool_guard_rejects_taiwan_modes(
    execution_mode: str,
) -> None:
    with pytest.raises(
        RuntimeError,
        match=rf"example\.py supports execution_mode='naive' only.*{execution_mode}",
    ):
        require_naive_execution_for_tool(
            execution_mode,
            tool_name="example.py",
        )


@pytest.mark.parametrize("relative_path", CONFIG_DRIVEN_NAIVE_TOOLS)
def test_config_driven_naive_tool_guards_immediately_after_config_load(
    relative_path: str,
) -> None:
    tree = ast.parse((REPO_ROOT / relative_path).read_text(encoding="utf-8"))
    main = next(
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name == "main"
    )
    load_index = next(
        index
        for index, statement in enumerate(main.body)
        if _statement_calls(statement, "load_config")
    )

    assert _statement_calls(
        main.body[load_index + 1],
        "require_naive_execution_for_tool",
    )
