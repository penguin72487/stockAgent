"""Measure every registered economic receipt through its canonical verifier.

No provider request, source write or service operation is performed. This clocks
receipt verification, not a historical download. A separate byte readback checks
the exact native receipt members; its cost is not allocated to source stages.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import UTC, datetime
import gzip
import hashlib
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json, sha256_file
from downloader.download_public_economic_history import file_signature, job_fingerprint, receipt_path, verified_receipt
from downloader.frankfurter_official_history import official_jobs
from downloader.operation_timing import OperationTiming
from downloader.public_economic_sources import EconomicJob, core_jobs, current_moi_job

def benchmark(*, inventory: Path, economic_root: Path, frankfurter_root: Path, output: Path,
              max_readback_bytes: int = 8 << 30, verifier=verified_receipt) -> dict:
    started = time.perf_counter()
    output.mkdir(parents=True, exist_ok=False)
    evidence = output / "evidence"
    evidence.mkdir()
    frozen = []

    def capture(path):
        before = file_signature(path)
        body = path.read_bytes()
        if file_signature(path) != before:
            raise ValueError("source changed while capturing: " + str(path))
        sha = hashlib.sha256(body).hexdigest()
        archive = evidence / (sha + ".json.gz")
        if not archive.exists():
            archive.write_bytes(gzip.compress(body, compresslevel=3, mtime=0))
        frozen.append({"path": str(path.resolve()), "sha256": sha, "bytes": len(body), "signature": before,
                       "archive": str(archive.relative_to(output))})
        return json.loads(body), sha

    registry, registry_sha = capture(inventory)
    rows = registry["rows"] if "rows" in registry else registry["sources"]
    selected = sorted({row["id"] for row in rows if row.get("id", "").startswith("economic:")
                       and row.get("kind", "source_endpoint") == "source_endpoint"})
    if not selected:
        raise ValueError("economic source inventory is empty")
    jobs = {f"economic:{job.provider}:{job.dataset}": job for job in core_jobs()}
    moi, _ = capture(economic_root / "catalog/moi_seasons.json")
    for item in moi["jobs"]:
        job = EconomicJob(**item)
        jobs[f"economic:{job.provider}:{job.dataset}"] = job
    job = current_moi_job()
    jobs[f"economic:{job.provider}:{job.dataset}"] = job
    frankfurter_catalog = frankfurter_root / "catalog.json"
    if frankfurter_catalog.is_file():
        catalog, _ = capture(frankfurter_catalog)
        for job in official_jobs(catalog["providers"]):
            jobs[f"economic:{job.provider}:{job.dataset}"] = job
    payloads = {}
    results = []
    for source_id in selected:
        _, provider, dataset = source_id.split(":", 2)
        root = frankfurter_root if provider == "frankfurter" else economic_root
        job = jobs.get(source_id)
        timer = OperationTiming()
        receipt = timer.call("existing_receipt_verification", verifier, root, job) if job else {}
        timing = timer.snapshot("verified_receipt_readonly")
        result = {"source_id": source_id, "provider": provider, "dataset": dataset,
                  "status": "receipt_verified" if receipt else "no_verified_receipt" if job else "job_definition_unavailable",
                  "query_fingerprint": job_fingerprint(job) if job else None,
                  "operation_timing": timing, "root": str(root.resolve()),
                  "source_binding_eligible": job is not None,
                  "rows": receipt.get("rows"), "files": len(receipt.get("files", [])),
                  "observed_at_utc": datetime.now(UTC).isoformat()}
        if job and receipt_path(root, job).is_file():
            native, sha = capture(receipt_path(root, job))
            result["native_receipt_sha256"] = sha
            if receipt and native != receipt:
                raise ValueError("receipt changed across measured verification: " + source_id)
        if receipt:
            for item in receipt["files"]:
                path = (root / item["path"]).resolve()
                previous = payloads.get(str(path))
                if previous and previous["expected_sha256"] != item["sha256"]:
                    raise ValueError("conflicting payload identities: " + str(path))
                payloads[str(path)] = {"path": str(path), "expected_sha256": item["sha256"],
                                       "bytes": path.stat().st_size, "signature": file_signature(path)}
        results.append(result)
    declared_bytes = sum(row["bytes"] for row in payloads.values())
    if declared_bytes > max_readback_bytes:
        raise ValueError("readback byte admission exceeded; source timings are not accepted")
    readback_started = time.perf_counter()
    for row in payloads.values():
        path = Path(row["path"])
        row["readback_sha256"] = sha256_file(path)
        if row["readback_sha256"] != row["expected_sha256"] or file_signature(path) != row["signature"]:
            raise ValueError("native payload readback failed: " + str(path))
    readback_seconds = time.perf_counter() - readback_started
    for row in frozen:
        path = Path(row["path"])
        if file_signature(path) != row["signature"] or sha256_file(path) != row["sha256"]:
            raise ValueError("metadata changed before acceptance: " + str(path))
    result = {"contract": "economic_receipt_validation_v1", "state": "verified_readonly_scope",
              "observed_at_utc": datetime.now(UTC).isoformat(), "inventory_sha256": registry_sha,
              "sources": results, "source_count": len(results), "status_counts": dict(Counter(row["status"] for row in results)),
              "native_metadata": frozen, "payloads": list(payloads.values()), "payload_bytes_readback": declared_bytes,
              "independent_byte_readback_seconds": readback_seconds, "wall_seconds": time.perf_counter() - started,
              "cpu_affinity": sorted(os.sched_getaffinity(0)), "loadavg_at_end": os.getloadavg(),
              "provider_calls": 0, "source_writes": 0,
              "scope": "all registered economic source IDs; canonical receipt verification clocks plus separate exact native member hash readback. Not fetch/parse/publication timings, complete source history, cold I/O or deployed producer adoption.",
              "all_workflows_measured": False, "hardware_limit_proven": False}
    atomic_write_json(output / "economic_receipt_validation.json", result, durable=True)
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-inventory", type=Path, required=True)
    parser.add_argument("--economic-root", type=Path, default=ROOT / "data_public_economic")
    parser.add_argument("--frankfurter-root", type=Path, default=ROOT / "data_forex_frankfurter/official_v2")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    result = benchmark(inventory=args.source_inventory, economic_root=args.economic_root,
                       frankfurter_root=args.frankfurter_root, output=args.output_dir)
    print(json.dumps({key: result[key] for key in ("state", "source_count", "status_counts", "payload_bytes_readback", "wall_seconds")}))


if __name__ == "__main__":
    main()
