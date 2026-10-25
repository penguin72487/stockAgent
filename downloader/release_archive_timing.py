"""Observe original-release collector calls without changing their artifacts."""
from __future__ import annotations

from datetime import datetime, timezone
from functools import wraps
import hashlib
import json
import os
from pathlib import Path
import sys
import threading

try:
    from downloader.operation_timing import OperationTiming
except ImportError:  # direct invocation from downloader/
    from operation_timing import OperationTiming


CONTRACT = "tw_public_release_archive_operation_timing_v1"
PREFIX = "[tw-public-release:operation-timing] "
SCOPE = "collect_call_includes_final_state_write_excludes_cli_admission_locks_logging_and_post_call_handlers"
SOURCES = {
    "cbc_overnight_official_pages": "download_tw_cbc_overnight_pages.py",
    "cbc_usdtwd_annual_pages": "download_tw_cbc_fx_annual_pages.py",
    "mof_macro_release_dates": "download_tw_mof_macro_release_dates.py",
    "mof_trade_release_values": "download_tw_mof_trade_release_values.py",
    "mof_original_release_archive": "download_tw_mof_release_archive.py",
    "dgbas_release_vintages": "download_tw_dgbas_release_archive.py",
}
_LOG_LOCK = threading.Lock()


def _emit(dataset, function, result, returned, timing):
    definition = {"dataset": dataset, "producer_file": SOURCES[dataset],
                  "producer_function": "collect", "timing_scope": SCOPE}
    definition_sha = hashlib.sha256(json.dumps(definition, sort_keys=True).encode()).hexdigest()
    matches = isinstance(result, dict) and result.get("dataset") == dataset
    status = result.get("status") if isinstance(result, dict) else None
    owner_matches = (Path(function.__code__.co_filename).name == SOURCES[dataset]
                     and function.__name__ == "collect")
    record = {
        "contract": CONTRACT, "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "dataset": dataset, "source_endpoint_id": "tw-public:" + dataset,
        "producer_file": Path(function.__code__.co_filename).name,
        "producer_function": function.__name__, "definition_sha256": definition_sha,
        "producer_pid": os.getpid(), "producer_thread_id": threading.get_ident(),
        "returned_dataset": result.get("dataset") if isinstance(result, dict) else None,
        "result_dataset_matches_request": matches,
        "status": status if isinstance(status, str) else None,
        "outcome": "returned" if returned else "raised",
        "source_binding_eligible": (returned and matches and owner_matches
            and isinstance(status, str) and status != "unsupported"
            and timing.get("invalid_clock") is False),
        "operation_timing": timing, "work_units_denominator": None,
        "configuration_is_resolved_request_range": False,
        "clock_is_source_publication_time": False,
        "whole_loaded_source_identity_verified": False,
        "acquisition_completeness_verified": False, "live_readiness_verified": False,
    }
    line = PREFIX + json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n"
    with _LOG_LOCK:
        sys.stderr.write(line)
        sys.stderr.flush()


def timed_release_archive(dataset):
    """Time returned, failed and interrupted calls; telemetry never retries them.

    Raw/Parquet/state bytes and the original return value are untouched. CLI
    admission and writer locks remain with their original owners. A failed
    clock/log leaves an observation gap while preserving the native outcome.
    """
    if dataset not in SOURCES:
        raise ValueError("unknown release archive timing owner")

    def decorate(function):
        @wraps(function)
        def observed(*args, **kwargs):
            try:
                clock = OperationTiming()
            except Exception:
                clock = None
            result, returned = None, False
            try:
                result = function(*args, **kwargs)
                returned = True
                return result
            finally:
                if clock is not None:
                    try:
                        timing = clock.snapshot(SCOPE)
                        timing["started_monotonic_ns"] = clock.started_ns
                        _emit(dataset, function, result, returned, timing)
                    except Exception:
                        pass
        return observed
    return decorate
