"""Frozen real feature-index construction, full pagination and idle residency.

An engineering experiment only. It does not change the gateway decoder, read
credentials, call a provider/API or modify the producer's public observations.
Both paths use the canonical index and preserve all numeric/Unicode values.
The candidate decodes metadata natively when available and shares equal labels
within one generation, with a strict stdlib fallback for unsupported JSON.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import gc
import hashlib
import json
import os
from pathlib import Path
import resource
import shutil
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_bytes, atomic_write_json
from stockagent.live import data_monitor_feature_pages as pages
from stockagent.live.dashboard_updates import metadata_signature

def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def rss():
    return int(Path("/proc/self/statm").read_text().split()[1]) * os.sysconf("SC_PAGE_SIZE")


def reject_nonfinite(value):
    raise ValueError(f"nonfinite feature JSON: {value}")


def decode(body, variant, observation=None):
    if variant == "baseline":
        if observation is not None:
            observation.update(backend="stdlib")
        return json.loads(body, parse_constant=reject_nonfinite)
    return pages.decode_feature_index_payload(body, observation=observation)


def prepare(source, fixture):
    if fixture.exists() or shutil.disk_usage(fixture.parent).free < 30 * 1024**3:
        raise ValueError("fresh fixture and at least 30 GiB safe free space required")
    before = metadata_signature(source.stat())
    with source.open("rb") as stream:
        opened = metadata_signature(os.fstat(stream.fileno()))
        body = stream.read()
        finished = metadata_signature(os.fstat(stream.fileno()))
    if not before == opened == finished == metadata_signature(source.stat()):
        raise ValueError("producer changed during bounded capture")
    payload = decode(body, "baseline")
    index = pages.FeaturePageIndex.from_payload(payload, source_signature=before)
    fixture.mkdir(mode=0o700)
    atomic_write_bytes(fixture / "features.json", body)
    atomic_write_json(fixture / "fixture.json", {"source": str(source.resolve()),
        "source_signature": before, "source_sha256": hashlib.sha256(body).hexdigest(),
        "source_bytes": len(body), "rows": len(index.rows), "revision": index.revision,
        "captured_at_utc": datetime.now(timezone.utc).isoformat(), "source_writes": False})


def worker(fixture, variant):
    metadata = json.loads((fixture / "fixture.json").read_bytes())
    body = (fixture / "features.json").read_bytes()
    if hashlib.sha256(body).hexdigest() != metadata["source_sha256"]:
        raise ValueError("frozen feature source changed")
    began, cpu = time.perf_counter(), time.process_time()
    observation = {}
    payload = decode(body, variant, observation)
    after_decode_rss = rss()
    index = pages.FeaturePageIndex.from_payload(payload, source_signature=tuple(metadata["source_signature"]))
    build_wall, build_cpu = time.perf_counter()-began, time.process_time()-cpu
    after_index_rss = rss()
    del payload, body
    gc.collect()
    retained_index_rss = rss()
    digest = hashlib.sha256()
    rows = requests = output_bytes = 0
    paging, paging_cpu = time.perf_counter(), time.process_time()

    def observe(page):
        nonlocal requests, output_bytes
        encoded = json.dumps(page, ensure_ascii=False, allow_nan=False,
                             separators=(",", ":")).encode()
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        requests += 1
        output_bytes += len(encoded)

    for offset in range(0, len(index.rows), 80):
        result = index.page(offset=offset, limit=80)
        rows += len(result["rows"])
        observe(result)
    for search in ("TWSE", "close", "volume", "台", "not-a-feature-xyz"):
        observe(index.page(offset=3, limit=37, search=search))
    for source in tuple(index.source_positions)[:5]:
        observe(index.page(offset=1, limit=17, source=source, search="a"))
    for category in index.category_positions:
        observe(index.page(offset=2, limit=19, category=category))
    observe(index.page(offset=100, limit=80, requested_revision="previous-generation"))
    paging_wall, paging_cpu_seconds = time.perf_counter()-paging, time.process_time()-paging_cpu
    if rows != metadata["rows"] or index.revision != metadata["revision"]:
        raise ValueError("source population or revision changed")
    total_wall, total_cpu = time.perf_counter()-began, time.process_time()-cpu
    idle, idle_cpu = time.perf_counter(), time.process_time()
    gc.collect()
    time.sleep(max(0., 2.-(time.perf_counter()-idle)))
    return {"variant": variant, "metadata_decoder_backend": observation["backend"],
        "rows": rows, "requests": requests, "output_bytes": output_bytes,
        "output_sha256": digest.hexdigest(), "wall_seconds": total_wall,
        "cpu_seconds": total_cpu, "build_wall_seconds": build_wall,
        "build_cpu_seconds": build_cpu, "paging_wall_seconds": paging_wall,
        "paging_cpu_seconds": paging_cpu_seconds, "rss_after_decode_bytes": after_decode_rss,
        "rss_after_index_bytes": after_index_rss, "retained_index_rss_bytes": retained_index_rss,
        "idle_rss_bytes": rss(), "idle_cpu_seconds": time.process_time()-idle_cpu,
        "rss_peak_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024}


def benchmark(fixture, output, control=None):
    if output.exists():
        raise FileExistsError("never overwrite evidence")
    paths = [Path(__file__).resolve(), Path(pages.__file__), ROOT / "scripts/serve_public_dashboards.py",
             fixture / "features.json", fixture / "fixture.json"]
    if pages._native is not None:
        paths += [Path(pages._native.__file__), Path(pages._native._core.__file__)]
    before = {str(path): sha(path) for path in paths}
    prior = json.loads(control.read_bytes()) if control is not None else None
    if prior is not None and (
        prior.get("exact_output_parity") is not True
        or Path(prior["fixture"]).resolve() != fixture.resolve()
        or prior["input_and_code_sha256"][str(fixture / "features.json")] != before[str(fixture / "features.json")]
    ):
        raise ValueError("independent control must bind this exact complete source")
    trials = []
    for variant in ("baseline", "shared", "shared", "baseline")*2:
        run = subprocess.run([sys.executable, str(Path(__file__).resolve()), "--fixture", str(fixture),
            "--worker", variant], text=True, capture_output=True, check=True)
        row = json.loads(run.stdout.splitlines()[-1])
        trials.append(row)
        print(json.dumps(row), flush=True)
    if (before != {str(path): sha(path) for path in paths}
            or len({row["output_sha256"] for row in trials}) != 1
            or prior is not None and trials[0]["output_sha256"] != prior["trials"][0]["output_sha256"]):
        raise ValueError("source/code generation or full paginated outputs differ")
    metrics = ("wall_seconds", "cpu_seconds", "rss_peak_bytes", "retained_index_rss_bytes", "idle_rss_bytes",
               "build_wall_seconds", "build_cpu_seconds", "paging_wall_seconds", "paging_cpu_seconds")
    medians = {variant: {metric: statistics.median(row[metric] for row in trials if row["variant"] == variant)
        for metric in metrics} for variant in ("baseline", "shared")}
    result = {"contract": "public_feature_complete_paging_residency_v1", "state": "measurement_complete",
        "scope": "decode_validate_index_full_pagination_search_generation_reset_and_idle_not_http_gzip_or_host",
        "fixture": str(fixture), "input_and_code_sha256": before, "medians": medians, "trials": trials,
        "trial_order": [row["variant"] for row in trials], "balanced_abba": True,
        "exact_output_parity": True, "source_unchanged": True, "code_stable": True,
        "provider_calls": 0, "gpu_calls": 0, "source_writes": False, "production_writes": False}
    result["native_metadata_decoder"] = {"available": pages._native is not None,
                                         "version": getattr(pages._native, "__version__", None)}
    result["python_version"] = sys.version
    if control is not None:
        result["independent_control"] = {"path": str(control), "sha256": sha(control)}
    atomic_write_json(output, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--prepare", action="store_true")
    parser.add_argument("--source", type=Path, default=ROOT / "artifacts/live/data_monitor/feature_inventory.json")
    parser.add_argument("--worker", choices=("baseline", "shared"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--control", type=Path)
    args = parser.parse_args()
    if args.prepare:
        prepare(args.source, args.fixture)
    elif args.worker:
        print(json.dumps(worker(args.fixture, args.worker)))
    elif args.output:
        print(json.dumps(benchmark(args.fixture, args.output, args.control)["medians"]))
    else:
        parser.error("--prepare, --worker or --output required")


if __name__ == "__main__":
    main()
