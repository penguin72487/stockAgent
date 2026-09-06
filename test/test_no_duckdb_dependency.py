from __future__ import annotations

import ast
from pathlib import Path


def test_duckdb_dependency_is_not_in_runtime_imports() -> None:
    root = Path(__file__).resolve().parents[1]
    runtime_dirs = [root / "stockagent", root / "train.py", root / "explain_model.py", root / "explain_model"]
    source_paths: list[Path] = []
    for item in runtime_dirs:
        if item.is_file() and item.suffix == ".py":
            source_paths.append(item)
        elif item.is_dir():
            source_paths.extend(item.rglob("*.py"))

    offenders = []
    for path in source_paths:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        rel = str(path.relative_to(root))
        imports = [
            node for node in ast.walk(tree)
            if (isinstance(node, ast.Import) and any(
                alias.name.split(".")[0] == "duckdb" for alias in node.names
            )) or (isinstance(node, ast.ImportFrom) and (node.module or "").split(".")[0] == "duckdb")
        ]
        if rel == "stockagent/data/columnar_lake.py":
            # Explicit offline-only role in configs/columnar_storage.json.
            # Even here, importing receipt helpers must not import DuckDB.
            compactor = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                             and node.name == "compact_parquet_files")
            assert imports and all(node in list(ast.walk(compactor)) for node in imports)
        elif imports:
            offenders.append(rel)

    assert offenders == []
