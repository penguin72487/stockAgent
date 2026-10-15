#!/usr/bin/env python3
"""Run the canonical TW data owner, then publish a verified paper projection.

This updater is scheduled by the normal per-market preopen/completed-session
hooks. A failed build retains the last accepted immutable generation and fails
visibly; it cannot advertise a stale projection as ready for another session.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
if __name__ == "__main__":
    from stockagent.data.source_processing_runtime import configure_cpu_source_runtime
    source_profile = configure_cpu_source_runtime()
    # Arrow has a separate pool: POLARS_MAX_THREADS does not configure it.
    # Set the observed benchmark's scoped profile before the source owner runs.
    import pyarrow as pa
    pa.set_cpu_count(source_profile["effective_threads"])
from scripts.prepare_tw_day_trade_factorized_live import prepare
from downloader.artifact_io import atomic_write_json
from stockagent.live.tw_public_source_guard import TWPublicSourceUpdatePending
from stockagent.data.annual_cache_admission import AnnualCacheBuildDeferred


def _prepare_with_retry(operation, *, receipt_path: Path, phase: str,
                        attempts: int, source_wait_seconds: float):
    """A busy canonical writer consumes wait time, not a build attempt.

    Finalization and preparation are separate owners. Another publication can
    legitimately acquire the writer lease between them; no lock is bypassed.
    Other failures keep the bounded build retry policy and remain visible.
    """
    deadline = time.monotonic() + source_wait_seconds
    attempt = 1
    while True:
        try:
            result = operation()
            atomic_write_json(receipt_path, {**result, "phase": phase, "attempt": attempt})
            return result
        except TWPublicSourceUpdatePending as exc:
            waiting = {"status": "waiting_source", "phase": phase, "attempt": attempt,
                       "error_type": type(exc).__name__, "error": str(exc),
                       "model_executed": False, "last_good_generation_retained": True}
            atomic_write_json(receipt_path, waiting)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                atomic_write_json(receipt_path, {**waiting, "status": "failed_retryable",
                                                "source_wait_expired": True})
                raise
            print(json.dumps(waiting, ensure_ascii=False), flush=True)
            time.sleep(min(2.0, remaining))
        except AnnualCacheBuildDeferred as exc:
            # Repeating the same cold build in this cgroup cannot create RAM.
            # Keep the exact request for the existing isolated cache worker.
            atomic_write_json(receipt_path, {"status": "cache_warming_required", "phase": phase,
                "attempt": attempt, "error_type": type(exc).__name__, "error": str(exc),
                "admission": exc.details, "model_executed": False,
                "last_good_generation_retained": True,
                "retry_owner": "stockagent-discord-artifact-maintenance.service"})
            raise
        except Exception as exc:
            atomic_write_json(receipt_path, {"status": "failed_retryable", "phase": phase,
                "attempt": attempt, "error_type": type(exc).__name__, "error": str(exc),
                "last_good_generation_retained": True})
            if attempt == attempts:
                raise
            time.sleep(min(2**(attempt-1), 4))
            attempt += 1


def _processing_pools() -> dict:
    """Record actual pools, not a presumed CPU quota or GPU inference policy."""
    import polars as pl
    import pyarrow as pa

    return {"polars_observed_threads": pl.thread_pool_size(),
            "arrow_observed_threads": pa.cpu_count(), "process_cpu_quota": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("preopen", "completed"), required=True)
    parser.add_argument("--attempts", type=int, default=2)
    parser.add_argument("--source-wait-seconds", type=float, default=600.0)
    args = parser.parse_args()
    if not 1 <= args.attempts <= 4:
        raise ValueError("attempts must be between one and four")
    if not 0 <= args.source_wait_seconds <= 1800:
        raise ValueError("source wait must be between zero and 1800 seconds")
    command = ([sys.executable, str(ROOT / "scripts/activate_tw_public_opening_data.py"),
                "--link", "data_tw_public"] if args.phase == "preopen" else
               [sys.executable, str(ROOT / "scripts/finalize_tw_public_completed_session.py")])
    subprocess.run(command, cwd=ROOT, check=True)
    receipt_path = ROOT / "artifacts/data_preparation/tw_factorized_scale_cash_live_v1/updater_status.json"

    def build():
        prepared = prepare(
            config_path=ROOT / "configs/deployments/tw_day_trade_factorized_scale_separated_cash_fold11_candidate_v1.yaml",
            parent_path=ROOT / "artifacts/data_preparation/tw_factorized_fold11_live_parent_20261013/factorized_manifest.json",
            native_source=Path("/srv/stockagent-live/data_tw_daytrade_panel_sources_20261004_gaprepair_v4"),
            public=Path("/srv/stockagent-live/data_tw_public"), finlab=ROOT / "data_finlab",
            output=ROOT / "artifacts/data_preparation/tw_factorized_scale_cash_live_v1",
            start_date="2025-12-01",
            annual_cache_root=ROOT / "artifacts/cache/tw_factorized_live/annual")
        return {**prepared, "processing_pools": _processing_pools()}

    result = _prepare_with_retry(build,
        receipt_path=receipt_path, phase=args.phase, attempts=args.attempts,
        source_wait_seconds=args.source_wait_seconds)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
