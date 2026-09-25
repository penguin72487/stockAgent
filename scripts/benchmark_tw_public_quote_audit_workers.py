#!/usr/bin/env python3
"""Read-only worker-count A/B for the exact TW public per-symbol quote audit."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import sys
import time

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.audit_tw_public_data_layer import (  # noqa: E402
    _benchmark_sessions,
    audit_quote_source_files,
)
from scripts.build_tw_official_symbol_parquets import (  # noqa: E402
    _load_verified_taiex_session_calendar,
)
from stockagent.config import load_config  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--workers", type=int, nargs="+", default=[64, 16, 16, 64])
    args = parser.parse_args()
    if any(workers <= 0 for workers in args.workers):
        parser.error("all worker counts must be positive")
    config = load_config(args.config)
    parquet_root = Path(config.data.parquet_root)
    feature_path = Path(config.data.tw_public_feature_path)
    public_dir = feature_path.parent.parent
    sessions = _benchmark_sessions(
        parquet_root,
        config.data.benchmark_name,
        feature_path,
        public_dir,
        config.walk_forward.expected_first_year,
        config.data.panel_start_date,
    )
    calendar, _, _ = _load_verified_taiex_session_calendar(public_dir)
    source_sessions = np.asarray(
        calendar["date"].to_numpy(), dtype="datetime64[D]"
    )
    results: list[dict[str, object]] = []
    for workers in args.workers:
        started = time.perf_counter()
        profiles, summary, findings = audit_quote_source_files(
            parquet_root,
            sessions,
            workers=workers,
            require_official=True,
            source_sessions=source_sessions,
        )
        encoded = json.dumps(
            {
                "profiles": profiles,
                "summary": summary,
                "findings": [asdict(item) for item in findings],
            },
            sort_keys=True,
            ensure_ascii=False,
            default=str,
        ).encode()
        results.append(
            {
                "workers": workers,
                "elapsed_seconds": round(time.perf_counter() - started, 3),
                "profiles": len(profiles),
                "result_sha256": hashlib.sha256(encoded).hexdigest(),
            }
        )
    if len({row["result_sha256"] for row in results}) != 1:
        raise RuntimeError("quote audit results changed across worker counts")
    print(
        json.dumps(
            {
                "config": str(args.config),
                "claim_boundary": (
                    "Same-source per-symbol quote audit worker-count A/B only; "
                    "not full model-safety audit wall time or p95."
                ),
                "results": results,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
