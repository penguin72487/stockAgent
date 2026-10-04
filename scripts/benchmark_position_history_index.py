#!/usr/bin/env python3
"""Read-only source-copy benchmark for the TW position locator delta index.

Only symlinks under a temporary directory point at the canonical snapshots.
The simulated one-file change and cache writes remain inside that directory.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.live import tw_day_trade_dashboard as dashboard


def _timed_index(root: Path) -> tuple[float, dashboard._PositionHistoryIndex]:
    started = time.perf_counter()
    index = dashboard._position_history_index(root)
    return time.perf_counter() - started, index


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--state-dir", type=Path,
        default=Path("artifacts/live/tw_day_trade_simulation"),
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    source = args.state_dir.resolve()
    if not source.is_dir():
        parser.error(f"state directory does not exist: {source}")

    with tempfile.TemporaryDirectory(prefix="stockagent-position-index-") as name:
        scratch = Path(name)
        root = scratch / "state"
        cache = scratch / "cache"
        cache.mkdir()
        targets = []
        for directory in ("position_history", dashboard.OVERNIGHT_POSITION_HISTORY_DIRNAME):
            for original in sorted((source / directory).glob("*/*.json")):
                target = root / original.relative_to(source)
                target.parent.mkdir(parents=True, exist_ok=True)
                target.symlink_to(original)
                targets.append(target)
        if not targets:
            parser.error("no position snapshots to benchmark")
        old_env = os.environ.get("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR")
        os.environ["STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR"] = str(cache)
        try:
            dashboard._POSITION_HISTORY_INDEX_CACHE.clear()
            cold_seconds, cold = _timed_index(root)
            dashboard._POSITION_HISTORY_INDEX_CACHE.clear()
            persisted_seconds, persisted = _timed_index(root)
            if cold.entries != persisted.entries:
                raise AssertionError("persistent index changed locator entries")

            # Replace only this scratch symlink with semantically identical
            # bytes plus whitespace. The real source is never opened for write.
            changed = targets[-1]
            replacement = changed.with_suffix(".json.tmp")
            shutil.copyfile(changed, replacement)
            with replacement.open("ab") as handle:
                handle.write(b"\n")
            replacement.replace(changed)
            dashboard._POSITION_HISTORY_INDEX_CACHE.clear()
            incremental_seconds, incremental = _timed_index(root)

            cache_file = dashboard._persistent_position_history_index_path(root)
            if cache_file is None:
                raise AssertionError("missing scratch index path")
            dashboard._POSITION_HISTORY_INDEX_CACHE.clear()
            cache_file.unlink()
            full_rebuild_seconds, rebuilt = _timed_index(root)
            if incremental.entries != rebuilt.entries:
                raise AssertionError("incremental index differs from full rebuild")
            result = {
                "source_directory": str(source),
                "source_files": len(targets),
                "locator_entries": len(rebuilt.entries),
                "change_model": "one scratch source received trailing whitespace",
                "cold_build_seconds": round(cold_seconds, 6),
                "persistent_exact_load_seconds": round(persisted_seconds, 6),
                "one_changed_source_seconds": round(incremental_seconds, 6),
                "same_source_full_rebuild_seconds": round(full_rebuild_seconds, 6),
                "exact_locator_parity": True,
                "production_source_modified": False,
            }
        finally:
            dashboard._POSITION_HISTORY_INDEX_CACHE.clear()
            if old_env is None:
                os.environ.pop("STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR", None)
            else:
                os.environ["STOCKAGENT_DASHBOARD_INDEX_CACHE_DIR"] = old_env
    if args.output is not None:
        output = args.output.resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_suffix(output.suffix + ".tmp")
        temporary.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        temporary.replace(output)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
