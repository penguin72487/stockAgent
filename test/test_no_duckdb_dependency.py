from __future__ import annotations

from pathlib import Path


def test_runtime_source_does_not_import_duckdb() -> None:
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
        if "import duckdb" in text or "from duckdb" in text:
            offenders.append(str(path.relative_to(root)))

    assert offenders == []
