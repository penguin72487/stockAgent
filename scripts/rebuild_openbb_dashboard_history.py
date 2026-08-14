#!/usr/bin/env python3
"""Rebuild the compact public OpenBB trend ledger from full monitor history."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
from typing import Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.live.openbb_archive_dashboard import project_openbb_history_row  # noqa: E402


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("data_openBB"))
    return parser.parse_args(argv)


def run(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    state_dir = args.output_dir / "_state"
    source = state_dir / "monitor_history.jsonl"
    target = state_dir / "monitor_dashboard_history.jsonl"
    if not source.is_file():
        raise FileNotFoundError(f"OpenBB monitor history does not exist: {source}")
    target.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    skipped = 0
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=target.parent,
        prefix=f".{target.name}.",
        suffix=".tmp",
        delete=False,
    ) as output:
        temporary = Path(output.name)
        try:
            with source.open("r", encoding="utf-8") as handle:
                for line in handle:
                    try:
                        raw = json.loads(line)
                    except json.JSONDecodeError:
                        skipped += 1
                        continue
                    if not isinstance(raw, dict):
                        skipped += 1
                        continue
                    row = project_openbb_history_row(raw)
                    if not row:
                        skipped += 1
                        continue
                    output.write(
                        json.dumps(row, ensure_ascii=False, separators=(",", ":"))
                        + "\n"
                    )
                    written += 1
            output.flush()
            os.fsync(output.fileno())
            os.replace(temporary, target)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    print(
        f"[openbb-dashboard-history] rows={written} skipped={skipped} target={target}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
