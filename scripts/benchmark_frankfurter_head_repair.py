"""Bounded native-v1 parity/timing and four isolated FX tail-repair checks.

No source files, live summaries or service states are modified. Shadow copies
are temporary and never promoted. This is not an all-pair completeness audit,
nor an estimate of full daily-job or process-cold-start latency.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import math
from pathlib import Path
import shutil
import statistics
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader import download_forex_frankfurter as fx  # noqa: E402
from downloader.common import SharedRateLimiter, resolve_request_interval  # noqa: E402
from downloader.artifact_io import atomic_write_json, sha256_bytes, sha256_file  # noqa: E402

PAIRS = ("BRLUSD", "CNYUSD", "ILSUSD", "INRUSD")
MAX_REQUESTS = 24


def run_probe(source_root: Path, *, timeout: int = 10, budget_seconds: float = 60) -> dict:
    if not isinstance(timeout, int) or isinstance(timeout, bool) or not 1 <= timeout <= 15:
        raise ValueError("request timeout must be 1..15 seconds")
    if not math.isfinite(budget_seconds) or not 1 <= budget_seconds <= 60:
        raise ValueError("request-admission budget must be 1..60 seconds")
    started = time.monotonic()
    original_fetch = fx._get_json
    original_limiter, original_retries = fx._RATE_LIMITER, fx._MAX_RETRIES
    requests = []
    stage = "latest_completed_boundary"
    result = {"schema_version": 1, "state": "not_accepted", "source_root": str(source_root),
              "generated_at_utc": datetime.now(timezone.utc).isoformat(),
              "acquisition_contract": fx.acquisition_contract(),
              "scope": "four temporary pair copies; first evidence build plus no-change recheck; imports excluded",
              "source_files_written": 0, "source_summaries_written": 0, "promoted": False,
              "provider_and_os_cache_controlled": False, "process_cold_start_measured": False,
              "request_admission_budget_seconds": budget_seconds, "request_timeout_seconds": timeout,
              "benchmark_max_retries": 0, "service_retry_policy_changed": False,
              "logical_get_call_budget": MAX_REQUESTS, "redirect_http_legs_counted": False,
              "requests": requests, "shadow_pairs": [], "endpoint_trials": []}

    def fetch(url, request_timeout):
        if len(requests) >= MAX_REQUESTS or time.monotonic() - started >= budget_seconds:
            raise TimeoutError("read-only benchmark request-admission budget exceeded")
        observed = {"stage": stage, "url": url}
        requests.append(observed)
        begin = time.perf_counter()
        try:
            payload = original_fetch(url, request_timeout)
            observed["state"] = "returned_json"
            return payload
        except Exception as exc:
            observed["state"] = "failed"
            observed["error_type"] = type(exc).__name__
            observed["http_status"] = getattr(getattr(exc, "response", None), "status_code", None)
            raise
        finally:
            observed["elapsed_seconds"] = time.perf_counter() - begin

    fx._get_json = fetch
    fx._RATE_LIMITER = SharedRateLimiter(resolve_request_interval("frankfurter_public", None), name="frankfurter_public")
    fx._MAX_RETRIES = 0
    fx._BASE_RESPONSES.clear()
    before = {}
    try:
        latest = datetime.strptime(fx._resolve_api_end_date(timeout), "%Y-%m-%d").date()
        result["provider_latest_date"] = latest.isoformat()
        end = min(latest, datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
        result["fixed_applied_end"] = end
        window_start = (datetime.strptime(end, "%Y-%m-%d") - timedelta(days=2)).date().isoformat()
        reference = None
        stage = "endpoint_ABBA"
        for variant in ("legacy_alias", "native_v1", "native_v1", "legacy_alias"):
            origin = "https://api.frankfurter.app" if variant == "legacy_alias" else fx.API_BASE
            begin = time.perf_counter()
            payload = fx._get_json(f"{origin}/{window_start}..{end}?from=EUR", timeout)
            digest = sha256_bytes(json.dumps(payload, sort_keys=True, allow_nan=False).encode())
            reference = reference or digest
            if "fixed_endpoint_reference_response" not in result:
                result["fixed_endpoint_reference_response"] = payload
            result["endpoint_trials"].append({"variant": variant, "elapsed_seconds": time.perf_counter() - begin,
                                               "normalized_payload_sha256": digest, "exact_payload_parity": digest == reference})
        medians = {name: statistics.median(t["elapsed_seconds"] for t in result["endpoint_trials"] if t["variant"] == name)
                   for name in ("legacy_alias", "native_v1")}
        result["endpoint_medians_seconds"] = medians
        result["endpoint_latency_reduction_fraction"] = 1 - medians["native_v1"] / medians["legacy_alias"]
        fx._BASE_RESPONSES.clear()
        stage = "shadow_first_evidence_and_tail"
        with tempfile.TemporaryDirectory(prefix="stockagent-frankfurter-head-probe-") as temporary:
            shadow = Path(temporary)
            for code in PAIRS:
                path = source_root / f"{code}_features.parquet"
                if path.stat().st_size > 2 * 1024**2 or fx._read_parquet_row_count(path) > 20000:
                    raise ValueError("source exceeds bounded shadow-verification size")
                before[code] = sha256_file(path)
                old = fx._read_parquet(path)
                first, last = old["date"].min(), old["date"].max()
                if (first.date().isoformat() != "2000-01-13"
                        or not 0 <= (datetime.strptime(end, "%Y-%m-%d").date() - last.date()).days <= 14):
                    raise ValueError("source is outside the narrowly bounded first-observation/tail scope")
                copied = shadow / path.name
                shutil.copyfile(path, copied)
                if sha256_file(copied) != before[code]:
                    raise RuntimeError("source changed while creating the shadow copy")
                record = fx.SymbolRecord(code, code, "forex", code[:3], code[3:])
                begin = time.perf_counter()
                outcome = fx._download_pair(record, "1999-01-04", end, shadow, timeout, False, True)
                seconds = time.perf_counter() - begin
                new = fx._read_parquet(copied)
                proof_path = copied.with_suffix(".head.json")
                proof = json.loads(proof_path.read_text()) if proof_path.exists() else {}
                retained = new.filter(fx.pl.col("date") <= last).equals(old)
                observed_end = new["date"].max().date().isoformat()
                result["shadow_pairs"].append({"code": code, "status": outcome.status, "rows_before": old.height,
                                               "rows_after": new.height, "first_pass_seconds": seconds,
                                               "all_original_rows_and_dtypes_preserved": retained,
                                               "observed_end": observed_end, "head_status": proof.get("head_status"),
                                               "head_proof": proof,
                                               "source_sha256_before": before[code]})
            stage = "shadow_no_change_recheck"
            fx._BASE_RESPONSES.clear()
            request_count = len(requests)
            for item in result["shadow_pairs"]:
                code = item["code"]
                record = fx.SymbolRecord(code, code, "forex", code[:3], code[3:])
                begin = time.perf_counter()
                outcome = fx._download_pair(record, "1999-01-04", end, shadow, timeout, False, True)
                item["recheck_status"] = outcome.status
                item["recheck_seconds"] = time.perf_counter() - begin
            result["recheck_network_requests"] = len(requests) - request_count
            result["shared_pivot_source_files"] = len(list((shadow / ".head_sources").glob("*.json")))
            # Preserve the parsed provider response and its exact serialized
            # evidence digest in the durable benchmark receipt before the
            # temporary copies are cleaned up. No live source is promoted.
            result["pivot_sources"] = [
                {"relative_path": str(path.relative_to(shadow)), "file_sha256": sha256_file(path),
                 "source_evidence": json.loads(path.read_text(encoding="utf-8"))}
                for path in sorted((shadow / ".head_sources").glob("*.json"))
            ]
        valid = all(item["status"] in {"updated_incremental", "up_to_date"}
                    and item["all_original_rows_and_dtypes_preserved"] and item["observed_end"] == end
                    and item["head_status"] == "verified_base_unpublished" and item["recheck_status"] == "up_to_date"
                    for item in result["shadow_pairs"])
        if (len(result["shadow_pairs"]) == len(PAIRS) and valid and result["recheck_network_requests"] == 0
                and result["shared_pivot_source_files"] == 1
                and all(item["exact_payload_parity"] for item in result["endpoint_trials"])):
            result["state"] = "accepted_isolated_candidate"
    except Exception as exc:
        result["failure"] = {"type": type(exc).__name__, "message": str(exc), "stage": stage}
    finally:
        unchanged = {}
        for code, digest in before.items():
            try:
                unchanged[code] = sha256_file(source_root / f"{code}_features.parquet") == digest
            except OSError:
                unchanged[code] = False
        result["source_hashes_unchanged"] = unchanged
        if not all(result["source_hashes_unchanged"].values()):
            result["state"] = "not_accepted"
        result["elapsed_seconds"] = time.monotonic() - started
        result["code_sha256"] = {"collector": sha256_file(Path(fx.__file__)), "benchmark": sha256_file(Path(__file__))}
        fx._get_json = original_fetch
        fx._RATE_LIMITER, fx._MAX_RETRIES = original_limiter, original_retries
        fx._BASE_RESPONSES.clear()
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=ROOT / "data_forex_frankfurter")
    parser.add_argument("--timeout", type=int, default=10)
    parser.add_argument("--budget-seconds", type=float, default=60)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("refusing to overwrite an existing benchmark receipt")
    receipt = run_probe(args.source_root, timeout=args.timeout, budget_seconds=args.budget_seconds)
    atomic_write_json(args.output, receipt)
    print(json.dumps({"state": receipt["state"], "request_count": len(receipt["requests"]),
                      "source_files_written": 0, "output": str(args.output)}, ensure_ascii=False))
    if receipt["state"] != "accepted_isolated_candidate":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
