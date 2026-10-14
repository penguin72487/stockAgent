"""Bind the accepted feature-index measurement to the actual read-only owner.

No service restart, source/provider mutation, credentials, model or CUDA work.
Before adoption write an isolated prewarm receipt; afterwards require a new
owner, untouched protected services and exact HTTP pages from one live source
generation. Producer changes cause bounded retries, never fabricated parity.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
from urllib.parse import urlencode
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from stockagent.live.dashboard_updates import metadata_signature
from stockagent.live import data_monitor_feature_pages as features
from stockagent.live.data_monitor_feature_pages import FeaturePageIndex

UNIT = "stockagent-public-dashboards.service"
PROTECTED = ("stockagent-shioaji-taifex-bidask.service", "stockagent-tw-day-trade-simulation.service",
             "stockagent-finmind-complement.service", "stockagent-discord-bot.service",
             "stockagent-shioaji-taifex-dashboard.service")
SOURCE = ROOT / "artifacts/live/data_monitor/feature_inventory.json"
BASE = "http://127.0.0.1:8770"


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def identity(unit):
    result = subprocess.run(["systemctl", "show", unit, "-p", "ActiveState", "-p", "MainPID", "-p", "InvocationID"],
                            check=True, capture_output=True, text=True, timeout=5)
    return dict(row.split("=", 1) for row in result.stdout.splitlines() if "=" in row)


def check_reader(owner):
    pid = int(owner.get("MainPID", "0"))
    if owner.get("ActiveState") != "active" or not owner.get("InvocationID") or pid <= 0:
        raise ValueError("public_reader_not_running")
    args = Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
    if (b"scripts/serve_public_dashboards.py" not in args
            or Path(f"/proc/{pid}/cwd").resolve() != ROOT.resolve()
            or Path(f"/proc/{pid}/exe").resolve() != Path(sys.executable).resolve()):
        raise ValueError("unexpected_public_reader_or_python_runtime")


def accepted_measurement(path):
    value = json.loads(Path(path).read_bytes())
    hashes = value["input_and_code_sha256"]
    control = value["independent_control"]
    independent = json.loads(Path(control["path"]).read_bytes())
    medians = value["medians"]
    fixture = Path(value["fixture"])
    source_path = str(fixture / "features.json")
    if features._native is None:
        raise ValueError("current_feature_code_not_bound_to_accepted_measurement")
    required = {str(ROOT / "scripts/benchmark_public_feature_residency.py"),
                str(ROOT / "scripts/serve_public_dashboards.py"),
                str(ROOT / "stockagent/live/data_monitor_feature_pages.py"),
                str(fixture / "features.json"), str(fixture / "fixture.json"),
                str(Path(features._native.__file__)), str(Path(features._native._core.__file__))}
    trials = value.get("trials", [])
    if len(trials) != 8 or any(sum(row.get("variant") == variant for row in trials) != 4
                              for variant in ("baseline", "shared")):
        raise ValueError("current_feature_code_not_bound_to_accepted_measurement")
    metrics = ("cpu_seconds", "wall_seconds", "rss_peak_bytes", "retained_index_rss_bytes")
    measured = {variant: {metric: statistics.median(row[metric] for row in trials if row["variant"] == variant)
                         for metric in metrics} for variant in ("baseline", "shared")}
    population = json.loads((fixture / "fixture.json").read_bytes())["rows"]
    expected = independent["trials"][0]["output_sha256"]
    if (value.get("contract") != "public_feature_complete_paging_residency_v1"
            or any(value.get(key) is not True for key in ("exact_output_parity", "code_stable", "source_unchanged", "balanced_abba"))
            or value.get("production_writes") is not False or value.get("provider_calls") != 0 or value.get("gpu_calls") != 0
            or value["trial_order"] != ["baseline", "shared", "shared", "baseline"]*2
            or sha(control["path"]) != control["sha256"] or independent.get("exact_output_parity") is not True
            or independent["input_and_code_sha256"][source_path] != hashes[source_path]
            or any(row["output_sha256"] != expected or row["variant"] != variant
                   or row["rows"] != population or row["requests"] != independent["trials"][0]["requests"]
                   or variant == "shared" and row["metadata_decoder_backend"] != "msgspec_shared_labels"
                   for row, variant in zip(trials, value["trial_order"], strict=True))
            or any(sha(name) != digest for name, digest in hashes.items())
            or set(hashes) != required
            or any(measured[variant][metric] != medians[variant][metric]
                   for variant in measured for metric in metrics)
            or medians["shared"]["cpu_seconds"] > 1.05*medians["baseline"]["cpu_seconds"]
            or medians["shared"]["wall_seconds"] > 1.05*medians["baseline"]["wall_seconds"]
            or medians["shared"]["rss_peak_bytes"] >= .85*medians["baseline"]["rss_peak_bytes"]
            or medians["shared"]["retained_index_rss_bytes"] >= .9*medians["baseline"]["retained_index_rss_bytes"]):
        raise ValueError("current_feature_code_not_bound_to_accepted_measurement")
    return hashes


def snapshot():
    before = metadata_signature(SOURCE.stat())
    with SOURCE.open("rb") as stream:
        opened = metadata_signature(os.fstat(stream.fileno()))
        body = stream.read()
        finished = metadata_signature(os.fstat(stream.fileno()))
    if not before == opened == finished == metadata_signature(SOURCE.stat()):
        raise ValueError("source_changed_during_capture")
    # The unchanged, original stdlib + canonical index is an independent live
    # oracle. This verifier does not select the optimized decoder as its oracle.
    index = FeaturePageIndex.from_payload(json.loads(body), source_signature=before)
    return before, hashlib.sha256(body).hexdigest(), index


def http_page(query):
    with urlopen(BASE + "/data-monitor/api/features/page?" + urlencode(query), timeout=10) as response:
        body = response.read(4*1024**2+1)
    if len(body) > 4*1024**2:
        raise ValueError("oversized_live_page")
    return body.rstrip(b"\n")


def observe_live_pages():
    queries = ({"offset": 0, "limit": 7, "q": "TWSE"},
               {"offset": 3, "limit": 37, "q": "台"},
               {"offset": 79, "limit": 11},
               {"offset": 100, "limit": 7, "revision": "0"*32})
    for _ in range(3):
        signature, digest, index = snapshot()
        rows = []
        for template in queries:
            query = {**template}
            if query["offset"] > 0 and "revision" not in query:
                query["revision"] = index.revision
            expected_query = {**query}
            if "q" in expected_query:
                expected_query["search"] = expected_query.pop("q")
            if "revision" in expected_query:
                expected_query["requested_revision"] = expected_query.pop("revision")
            expected = json.dumps(index.page(**expected_query), ensure_ascii=False,
                                  allow_nan=False, separators=(",", ":")).encode()
            actual = http_page(query)
            rows.append({"query": query, "exact_bytes": actual == expected,
                         "output_sha256": hashlib.sha256(actual).hexdigest(), "output_bytes": len(actual)})
        if metadata_signature(SOURCE.stat()) != signature:
            continue
        if not all(row["exact_bytes"] for row in rows):
            raise ValueError("actual_gateway_pages_do_not_match_original_live_oracle")
        return {"source_signature": signature, "source_sha256": digest, "rows": len(index.rows),
                "revision": index.revision, "pages": rows}
    raise ValueError("no_stable_live_generation_within_bounded_readback")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--acceptance", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline", type=Path)
    args = parser.parse_args(argv)
    if args.output.exists():
        parser.error("never overwrite prior evidence")
    hashes = accepted_measurement(args.acceptance)
    owner = identity(UNIT)
    protected = {unit: identity(unit) for unit in PROTECTED}
    check_reader(owner)
    result = {"contract": "public_feature_runtime_readback_v1", "owner": owner, "protected": protected,
              "code_and_input_sha256": hashes, "provider_calls": 0, "gpu_calls": 0,
              "source_mutations": False, "service_restart_requested": False,
              "observed_at_utc": datetime.now(timezone.utc).isoformat()}
    try:
        if args.baseline is None:
            signature, digest, index = snapshot()
            result.update(state="isolated_live_oracle_verified", source_signature=signature,
                          source_sha256=digest, rows=len(index.rows))
        else:
            baseline = json.loads(args.baseline.read_bytes())
            if (baseline.get("state") != "isolated_live_oracle_verified" or baseline["protected"] != protected
                    or baseline["code_and_input_sha256"] != hashes
                    or baseline["owner"]["InvocationID"] == owner["InvocationID"]):
                raise ValueError("reader_not_bound_to_baseline_and_protected_owners")
            result["live"] = observe_live_pages()
            result["state"] = "actual_gateway_feature_pages_verified"
        if identity(UNIT) != owner or {unit: identity(unit) for unit in PROTECTED} != protected:
            raise ValueError("reader_or_protected_owner_changed_during_readback")
        accepted_measurement(args.acceptance)
    except Exception as error:
        result.update(state="failed_readback", error_type=type(error).__name__, error=str(error))
        raise
    finally:
        atomic_write_json(args.output, result)
    print(json.dumps({"state": result["state"], "output": str(args.output)}))


if __name__ == "__main__":
    main()
