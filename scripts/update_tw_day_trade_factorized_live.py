#!/usr/bin/env python3
"""Prepare completed-session projections; consume published inputs at preopen.

This updater is scheduled by the normal per-market preopen/completed-session
hooks. A failed build retains the last accepted immutable generation and fails
visibly; it cannot advertise a stale projection as ready for another session.
"""
from __future__ import annotations

import argparse
from datetime import date, datetime
import json
from pathlib import Path
import subprocess
import sys
import time
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
if __name__ == "__main__":
    from stockagent.data.source_processing_runtime import configure_cpu_source_runtime
    source_profile = configure_cpu_source_runtime()
    # Arrow has a separate pool: POLARS_MAX_THREADS does not configure it.
    # Set the observed benchmark's scoped profile before the source owner runs.
    import pyarrow as pa
    pa.set_cpu_count(source_profile["effective_threads"])
from scripts.prepare_tw_day_trade_factorized_live import prepare, _next_projection_session
from downloader.artifact_io import atomic_write_json
from stockagent.live.tw_public_source_guard import TWPublicSourceUpdatePending
from stockagent.data.annual_cache_admission import AnnualCacheBuildDeferred
from stockagent.data_sync.completed_forward_capacity import CapacityDeferred
from stockagent.config import load_config
from stockagent.data.factorized_panel import (
    VALUE_ONLY_CONTRACT, factorized_member_proofs, file_sha256, verify_factorized_members,
    verify_factorized_layout,
)
from stockagent.data.tw_public_features import _exchange_session_dates


class FactorizedPreopenProjectionPending(RuntimeError):
    """The background producer has not published this session's complete input."""


def verify_preopen_projection(*, config_path: Path, parent_path: Path,
                              public: Path, output: Path, start_date: str,
                              session_date: date | None = None) -> dict:
    """Consume one fully verified published snapshot, never rebuild under 09:00.

    Raw acquisition heads may advance after publication. They are NOT the
    model input: the immutable, receipt-bound published projection is. Opening
    eligibility/price freshness remain independently validated by activation
    and inference. Completed-session maintenance alone publishes new features.
    No source receipt is relabelled as current and no historical rows are cut.
    """
    from stockagent.live.tw_public_source_guard import live_tw_public_source_read_guard

    started = time.perf_counter()
    session_date = session_date or datetime.now(ZoneInfo("Asia/Taipei")).date()
    root = output.resolve()
    pointer = root / "current/factorized_manifest.json"
    if not pointer.is_file():
        raise FactorizedPreopenProjectionPending("no published Factorized projection; canonical background preparation required")
    path = pointer.resolve(strict=True)
    if not path.is_relative_to(root / "generations") or path.is_symlink():
        raise ValueError("published Factorized projection escapes its canonical generations")
    manifest_sha = file_sha256(path)
    manifest = json.loads(path.read_bytes())
    parent_sha = file_sha256(parent_path)
    parent = json.loads(parent_path.read_bytes())
    config_sha = file_sha256(config_path)
    paper = manifest.get("paper_inference") or {}
    identity = paper.get("source_identity") or {}
    if (manifest.get("contract") != VALUE_ONLY_CONTRACT or manifest.get("status") != "complete"
            or manifest.get("research_only") is not True or manifest.get("historical_point_in_time") is not False
            or manifest.get("feature_lag") != 1 or manifest.get("model_channel_policy") != "value_only"
            or paper.get("contract") != "factorized_paper_projection_lag1_v1"
            or paper.get("status") != "verified" or paper.get("raw_source_publication") is not False):
        raise ValueError("unverified Factorized paper projection ABI")
    if (paper.get("parent_manifest_sha256") != parent_sha or identity.get("parent") != parent_sha
            or identity.get("config_sha") != config_sha
            or any(manifest.get(key) != parent.get(key) for key in (
                "feature_dictionary_sha256", "individual_channels", "common_channels", "symbols",
                "base_feature_names", "logical_model_channels", "source_snapshot_id"))):
        raise ValueError("published Factorized projection does not match the selected parent/configuration")
    dates = manifest.get("dates")
    if (not isinstance(dates, list) or not dates
            or any(not isinstance(day, str) or date.fromisoformat(day).isoformat() != day for day in dates)
            or dates != sorted(set(dates))):
        raise ValueError("published Factorized calendar must be complete ordered canonical dates")
    if identity.get("start_date") != start_date:
        raise FactorizedPreopenProjectionPending("published Factorized projection lacks the selected full history horizon")
    with live_tw_public_source_read_guard(public / "stocks"):
        summary_path = public / "features/tw_public_stock_daily.summary.json"
        summary = json.loads(summary_path.read_bytes())
        completed = date.fromisoformat(summary["requested_end_date"])
        decision, _ = _next_projection_session(public, completed)
        if (decision != session_date or paper.get("decision_session") != session_date.isoformat()
                or paper.get("completed_session") != completed.isoformat()
                or identity.get("completed") != completed.isoformat()
                or identity.get("decision") != session_date.isoformat() or dates[-1] != completed.isoformat()):
            raise FactorizedPreopenProjectionPending("published Factorized projection does not cover this session's completed inputs")
        expected = [day.isoformat() for day in _exchange_session_dates(public)["_session_date"].to_list()
                    if date.fromisoformat(start_date) <= day <= completed]
        if dates != expected:
            raise ValueError("published Factorized projection is missing official history sessions")
        verify_factorized_layout(manifest, symbols=parent["symbols"], dates=expected,
                                 base_feature_names=parent["base_feature_names"])
        metadata_ms = (time.perf_counter() - started) * 1000
        verified = time.perf_counter()
        verify_factorized_members(path.parent, manifest)
        verification_ms = (time.perf_counter() - verified) * 1000
        if (pointer.resolve(strict=True) != path or file_sha256(path) != manifest_sha
                or file_sha256(parent_path) != parent_sha or file_sha256(config_path) != config_sha
                or json.loads(summary_path.read_bytes())["requested_end_date"] != completed.isoformat()):
            raise ValueError("published Factorized input generation changed during preopen verification")
    return {"status": "already_current", "manifest": str(path), "manifest_sha256": manifest_sha,
            "decision_session": session_date.isoformat(), "completed_session": completed.isoformat(),
            "dates": len(dates), "features": manifest["logical_model_channels"],
            "wall_seconds": time.perf_counter() - started,
            "stages_ms": {"published_metadata_and_calendar": round(metadata_ms, 3),
                          "full_member_and_dictionary_sha": round(verification_ms, 3)},
            "verified_members": len(factorized_member_proofs(manifest)),
            "input_policy": "published_complete_session_projection_v1",
            "source_built_at_utc": paper.get("built_at_utc"),
            "source_identity": identity, "full_history_rebuilt": False,
            "latest_raw_heads_claimed": False, "provider_requests": 0, "model_executed": False}


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
        except CapacityDeferred as exc:
            # A retry in the same loop cannot create disk headroom or change
            # the generation allowance. Preserve evidence for the next owner
            # run instead of repeating expensive source preparation four times.
            atomic_write_json(receipt_path, {"status": "capacity_deferred", "phase": phase,
                "attempt": attempt, "error_type": type(exc).__name__, "error": str(exc),
                "admission": exc.details, "model_executed": False,
                "last_good_generation_retained": True,
                "retry_owner": "stockagent-discord-artifact-maintenance.service"})
            raise
        except FactorizedPreopenProjectionPending as exc:
            atomic_write_json(receipt_path, {"status": "projection_refresh_required", "phase": phase,
                "attempt": attempt, "error_type": type(exc).__name__, "error": str(exc),
                "model_executed": False, "last_good_generation_retained": True,
                "retry_owner": "stockagent-discord-artifact-maintenance.service"})
            raise  # Never repeat a full-history build inside the opening owner.
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


def _projection_start_date(config_path: Path, parent_path: Path) -> str:
    """One canonical projection must cover every consumer, not just live tails.

    Live inference already selects its bounded working set at read time. A
    producer-only 2025 cutoff made that tail look valid but permanently blocked
    the same deployment's full-history maintenance. Use the selected model's
    declared horizon; when omitted, retain the fixed trained parent's horizon.
    """
    configured = load_config(config_path).data.panel_start_date
    parent = json.loads(parent_path.read_text(encoding="utf-8"))
    dates = parent.get("dates")
    if (not isinstance(dates, list) or not dates
            or not all(isinstance(day, str) and date.fromisoformat(day).isoformat() == day for day in dates)
            or dates != sorted(set(dates))):
        raise ValueError("fixed parent requires a complete ordered ISO-date calendar")
    start = date.fromisoformat(configured or dates[0])
    if start < date.fromisoformat(dates[0]):
        raise ValueError("selected projection horizon precedes the fixed trained source calendar")
    return start.isoformat()


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
    config_path = ROOT / "configs/deployments/tw_day_trade_factorized_scale_separated_cash_fold11_candidate_v1.yaml"
    parent_path = ROOT / "artifacts/data_preparation/tw_factorized_fold11_live_parent_20261013/factorized_manifest.json"
    start_date = _projection_start_date(config_path, parent_path)
    command = ([sys.executable, str(ROOT / "scripts/activate_tw_public_opening_data.py"),
                "--link", "data_tw_public"] if args.phase == "preopen" else
               [sys.executable, str(ROOT / "scripts/finalize_tw_public_completed_session.py")])
    activation_started = time.perf_counter()
    subprocess.run(command, cwd=ROOT, check=True)
    activation_ms = (time.perf_counter() - activation_started) * 1000
    receipt_path = ROOT / "artifacts/data_preparation/tw_factorized_scale_cash_live_v1/updater_status.json"

    def build():
        common = dict(
            config_path=config_path,
            parent_path=parent_path,
            public=Path("/srv/stockagent-live/data_tw_public"),
            output=ROOT / "artifacts/data_preparation/tw_factorized_scale_cash_live_v1",
            start_date=start_date)
        if args.phase == "preopen":
            prepared = verify_preopen_projection(**common)
        else:
            prepared = prepare(**common,
                native_source=Path("/srv/stockagent-live/data_tw_daytrade_panel_sources_20261004_gaprepair_v4"),
                finlab=ROOT / "data_finlab",
                annual_cache_root=ROOT / "artifacts/cache/tw_factorized_live/annual")
        return {**prepared, "requested_start_date": start_date,
                "stages_ms": {**prepared.get("stages_ms", {}),
                              "canonical_activation_or_finalization": round(activation_ms, 3)},
                "processing_pools": _processing_pools()}

    result = _prepare_with_retry(build,
        receipt_path=receipt_path, phase=args.phase, attempts=args.attempts,
        source_wait_seconds=args.source_wait_seconds)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
