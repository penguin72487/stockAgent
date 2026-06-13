from __future__ import annotations

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
        text = path.read_text(encoding="utf-8", errors="ignore").lower()
        rel = str(path.relative_to(root))
        if "import duckdb" in text or "from duckdb" in text:
            offenders.append(rel)

    assert offenders == []
