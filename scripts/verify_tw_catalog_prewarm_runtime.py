"""Prove the date-only prewarm on the actual read-only gateway owner.

This verifier never restarts a service or invokes a model/broker/provider. It
binds the complete balanced measurement to current code and sources, captures
the previous actual HTTP curves, then requires a new reader with identical
curve bytes and untouched protected owners. Failure receipts remain visible.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import statistics
import sys
from urllib.parse import urlencode
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from scripts import benchmark_tw_catalog_prewarm as workflow
from scripts.verify_public_feature_index_runtime import (
    UNIT, PROTECTED, check_reader, identity,
)


def accepted_measurement(path):
    value = json.loads(Path(path).read_bytes())
    trials, code = value.get("trials", []), value.get("code_sha256", {})
    source = ROOT / "artifacts/live/tw_day_trade_simulation"
    required = {str(Path(workflow.__file__)), str(Path(workflow.dashboard.__file__)),
                str(Path(workflow.projection.__file__)),
                str(ROOT / "scripts/profile_public_gateway_residency.py")}
    order = ["baseline", "catalog", "catalog", "baseline"]*2
    metrics = ("wall_seconds", "cpu_seconds", "idle_rss_bytes", "rss_peak_bytes", "warm_wall_seconds")
    medians = {variant: {key: statistics.median(row[key] for row in trials if row["variant"] == variant)
                        for key in metrics} for variant in ("baseline", "catalog")}
    if (value.get("contract") != "tw_complete_default_history_catalog_prewarm_idle_v1"
            or Path(value.get("source_root", "")).resolve() != source.resolve()
            or len(trials) != 8 or value.get("trial_order") != order
            or any(value.get(key) is not True for key in (
                "balanced_abba", "exact_output_parity", "consumed_inputs_unchanged", "code_stable"))
            or value.get("source_writes") is not False or value.get("live_cache_writes") is not False
            or value.get("provider_calls") != 0 or value.get("gpu_calls") != 0
            or set(code) != required or any(workflow.sha(name) != digest for name, digest in code.items())
            or value.get("source_signatures") != workflow.source_inventory(source)
            or any(workflow.sha(name) != digest for name, digest in value["source_sha256"].items())
            or any(row["variant"] != variant or row.get("warm_exact_output_parity") is not True
                   or row["output_sha256"] != trials[0]["output_sha256"]
                   or row["index_counts"] != trials[0]["index_counts"]
                   or row["outputs"] != trials[0]["outputs"]
                   for row, variant in zip(trials, order, strict=True))
            or medians != value.get("medians")
            or medians["catalog"]["idle_rss_bytes"] >= .8*medians["baseline"]["idle_rss_bytes"]
            or medians["catalog"]["wall_seconds"] > 1.05*medians["baseline"]["wall_seconds"]
            or medians["catalog"]["cpu_seconds"] > 1.05*medians["baseline"]["cpu_seconds"]):
        raise ValueError("current_code_source_or_complete_workflow_not_bound_to_accepted_measurement")
    return {"path": str(path), "sha256": workflow.sha(path), "code_sha256": code,
            "source_sha256": value["source_sha256"], "medians": medians}


def http_curves():
    source = ROOT / "artifacts/live/tw_day_trade_simulation"
    head = workflow.projection.load_benchmark_projection(state_dir=source,
        source_path=source / "benchmark_history.json", selected_sessions=())
    if head is None or not head.session_entries:
        raise ValueError("current_benchmark_date_catalog_unavailable")
    latest = max(head.session_entries)
    # Match the browser's public query contract, not the internal Python API.
    common = {"range": "all", "resolution": "1m", "encoding": "v2"}
    queries = (common, {**common, "start_date": latest, "end_date": latest})
    result = []
    for query in queries:
        with urlopen("http://127.0.0.1:8770/tw-day-trade/api/history?" + urlencode(query), timeout=30) as response:
            body = response.read(64*1024**2+1)
        if len(body) > 64*1024**2:
            raise ValueError("actual_curve_exceeded_bounded_readback")
        payload = json.loads(body)
        if payload.get("simulation_only") is not True or payload.get("production_order_possible") is not False:
            raise ValueError("actual_history_not_the_read_only_simulation_projection")
        result.append({"query": query, "bytes": len(body), "sha256": hashlib.sha256(body).hexdigest(),
                       "returned_points": payload.get("returned_points"),
                       "series_count": len(payload.get("minute_series", []))})
    return result


def verify(acceptance, output, baseline=None):
    if output.exists():
        raise FileExistsError("never overwrite prior acceptance evidence")
    owner = identity(UNIT)
    protected = {unit: identity(unit) for unit in PROTECTED}
    result = {"contract": "tw_date_catalog_actual_gateway_readback_v1",
              "observed_at_utc": datetime.now(timezone.utc).isoformat(),
              "owner": owner, "protected": protected, "service_restarts_requested": 0,
              "provider_calls": 0, "gpu_calls": 0, "source_writes": False}
    try:
        check_reader(owner)
        measurement = accepted_measurement(acceptance)
        result["accepted_measurement"] = measurement
        curves = http_curves()
        if baseline is None:
            result.update(state="actual_previous_reader_curves_captured", curves=curves)
        else:
            previous = json.loads(Path(baseline).read_bytes())
            if (previous.get("state") != "actual_previous_reader_curves_captured"
                    or previous["protected"] != protected
                    or previous["accepted_measurement"] != measurement
                    or previous["owner"]["InvocationID"] == owner["InvocationID"]
                    or previous["curves"] != curves):
                raise ValueError("new_reader_protected_owner_or_exact_actual_curve_mismatch")
            result.update(state="actual_new_reader_complete_curves_verified", curves=curves,
                          exact_http_byte_parity=True)
        if identity(UNIT) != owner or {unit: identity(unit) for unit in PROTECTED} != protected:
            raise ValueError("reader_or_protected_owner_changed_during_probe")
        accepted_measurement(acceptance)
    except Exception as error:
        result.update(state="failed_readback", error_type=type(error).__name__, error=str(error))
        raise
    finally:
        atomic_write_json(output, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--acceptance", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline", type=Path)
    args = parser.parse_args()
    result = verify(args.acceptance, args.output, args.baseline)
    print(json.dumps({"state": result["state"], "owner": result["owner"], "output": str(args.output)}))


if __name__ == "__main__":
    main()
