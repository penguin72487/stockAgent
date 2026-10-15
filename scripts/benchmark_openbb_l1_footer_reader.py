#!/usr/bin/env python3
"""Run the canonical complete L1 derivative audit with alternate local readers.

Capture only the existing segment manifest into private scratch, never L0 or
payload copies. Every trial still opens every selected real footer and runs the
canonical row/schema/physical-view checks. File signatures fence before/after;
they are race detection, not a payload-content or full-archive integrity proof.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import textwrap
from threading import Lock
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
CONTRACT = "openbb_canonical_derivative_footer_reader_v4"
CODE_PATHS = (
    "scripts/benchmark_openbb_l1_footer_reader.py",
    "scripts/compact_openbb_l1.py",
    "scripts/openbb_l1_view_schema.py",
    "scripts/openbb_l1_source_journal.py",
    "scripts/openbb_l1_task_contract_index.py",
    "scripts/openbb_l1_status_counts.py",
    "scripts/openbb_l1_footer_io.py",
    "scripts/benchmark_openbb_l1_source_contract.py",
    "stockagent/data/columnar_lake.py",
    "stockagent/data/source_processing_runtime.py",
    "downloader/artifact_io.py",
)


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def implementation_identity() -> dict:
    import pyarrow as pa
    return {
        "code": {name: digest(ROOT / name) for name in CODE_PATHS},
        "python": sys.version,
        "executable_sha256": digest(Path(sys.executable).resolve()),
        "arrow": pa.__version__,
        "sqlite": sqlite3.sqlite_version,
    }


def file_signatures(connection: sqlite3.Connection) -> str:
    from scripts.compact_openbb_l1 import _view_file_signature
    result = hashlib.sha256()
    for row in connection.execute(
        "SELECT segment_id, output_path FROM l1_compaction_segments ORDER BY segment_id"
    ):
        signature = _view_file_signature(Path(row[1]))
        result.update(json.dumps([row[0], row[1], signature], separators=(",", ":")).encode())
        result.update(b"\n")
    return result.hexdigest()


def fresh_output(path: Path) -> None:
    if path.exists() or path.is_symlink():
        raise ValueError("preserve previous result; choose a new output path")
    if not path.resolve().is_relative_to(ROOT / "artifacts/benchmarks"):
        raise ValueError("private benchmark artifact path required")


def capture(root: Path, manifest: Path, limit: int) -> None:
    from downloader.artifact_io import atomic_write_json
    fresh_output(root)
    if limit < 0 or shutil.disk_usage(root.parent).free < 30 * 1024**3:
        raise ValueError("invalid limit or insufficient private scratch headroom")
    root.mkdir(mode=0o700)
    path = root / "segments.sqlite3"
    identity = implementation_identity()
    for name, sha in identity.get("code", {}).items():
        preserved = root / "code-closure" / name
        preserved.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / name, preserved)
        preserved.chmod(0o400)
        if digest(preserved) != sha:
            raise RuntimeError("code capture changed")
    with sqlite3.connect(f"file:{manifest.resolve(strict=True)}?mode=ro", uri=True) as source:
        source.execute("PRAGMA query_only=ON")
        source.execute("BEGIN")
        schema = source.execute(
            "SELECT sql FROM sqlite_master WHERE name='l1_compaction_segments' AND type='table'"
        ).fetchone()[0]
        total = int(source.execute(
            "SELECT COUNT(*) FROM l1_compaction_segments WHERE status='success'"
        ).fetchone()[0])
        query = "SELECT * FROM l1_compaction_segments WHERE status='success' ORDER BY segment_id"
        if limit:
            query += " LIMIT ?"
        cursor = source.execute(query, (limit,) if limit else ())
        columns = len(cursor.description)
        with sqlite3.connect(path) as target:
            target.execute(schema)
            while batch := cursor.fetchmany(2048):
                target.executemany(
                    f"INSERT INTO l1_compaction_segments VALUES ({','.join('?' for _ in range(columns))})",
                    batch,
                )
            selected = int(target.execute("SELECT COUNT(*) FROM l1_compaction_segments").fetchone()[0])
            signature = file_signatures(target)
            target.commit()
        source.rollback()
    if selected == 0 or implementation_identity() != identity:
        raise RuntimeError("empty capture or changed implementation")
    path.chmod(0o400)
    receipt = {
        "contract": CONTRACT, "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "manifest_sha256": digest(path), "implementation": identity,
        "selected_segments": selected, "available_success_segments": total,
        "all_success_segments": selected == total,
        "source_signatures_sha256": signature,
        "source_manifest": str(manifest.resolve()),
        "scope": "canonical derivative audit only; source-member/journal/publication phases excluded",
        "provider_calls": 0, "production_writes": False,
    }
    atomic_write_json(root / "fixture.json", receipt)
    print(json.dumps({key: receipt[key] for key in (
        "selected_segments", "available_success_segments", "all_success_segments"
    )}), flush=True)


@contextmanager
def reader_variant(module, variant: str, *, reader_counts: dict[str, int] | None = None):
    original = module.pq.ParquetFile
    original_identity = module.schema_identities
    native_opener = getattr(module, "FooterOpener", None)
    cache = None
    opener = None
    if variant not in {"original", "mmap", "schema_cache", "adopted", "local", "metadata_only", "prefetch"}:
        raise ValueError("unknown reader variant")
    if variant in {"local", "metadata_only", "prefetch"}:
        from scripts.openbb_l1_footer_io import FooterOpener
        opener = FooterOpener(reader=original, metadata_only=variant != "local")
    elif variant == "adopted" and native_opener is not None:
        opener = native_opener(reader=original)
    if variant == "schema_cache":
        from scripts.openbb_l1_view_schema import PhysicalSchemaIdentityCache
        cache = PhysicalSchemaIdentityCache()
    counter_lock = Lock()
    def counted_reader(*args, **kwargs):
        if reader_counts is not None:
            with counter_lock:
                reader_counts["footer_open_attempts"] += 1
        if opener is not None:
            return opener(*args, **kwargs)
        if variant == "mmap":
            kwargs = {**kwargs, "memory_map": True}
        return original(*args, **kwargs)

    # The original trial must explicitly disable the adopted default cache;
    # otherwise an old/new comparison would silently time the same candidate.
    if variant in {"original", "mmap", "schema_cache"}:
        module.schema_identities = lambda parquet_file, **kwargs: original_identity(
            parquet_file, **{**kwargs, "physical_cache": cache},
        )
    module.pq.ParquetFile = counted_reader
    if native_opener is not None:
        class InstrumentedOpener:
            __call__ = staticmethod(counted_reader)

            def snapshot(self):
                return opener.snapshot() if opener is not None else {"benchmark_legacy_reader": True}

        # Instrument the factory too after native adoption. Otherwise wrapping
        # the public class would disable its capability guard and manufacture
        # extra failed calls rather than count actual footer reads.
        module.FooterOpener = InstrumentedOpener
    audit_function = module._mark_stale_segments
    if variant == "prefetch":
        import inspect
        from scripts.openbb_l1_footer_io import prefetched_footer_rows
        namespace = module.__dict__.copy()
        namespace["prefetched_footer_rows"] = prefetched_footer_rows
        exec(compile(prefetch_source(inspect.getsource(audit_function)), "<full_audit_prefetch>", "exec"), namespace)
        module._mark_stale_segments = namespace["_mark_stale_segments"]
    try:
        yield opener if opener is not None else cache
    finally:
        module.pq.ParquetFile = original
        module.schema_identities = original_identity
        module._mark_stale_segments = audit_function
        if native_opener is not None:
            module.FooterOpener = native_opener


def prefetch_source(source: str) -> str:
    """Preserve the complete canonical audit; replace only footer preparation."""
    start = source.index("    for row in connection.execute(\n", source.index("    footer_open_attempts = 0\n"))
    stop = source.index("    if metadata_audit is not None:\n", start)
    loop = source[start:stop]
    split = loop.index("    ):\n") + len("    ):\n")
    query = loop[len("    for row in "):split - len("    ):\n")] + "    )"
    body = loop[split:]
    skip = '        if str(row["segment_id"]) in known:\n            continue\n'
    if body.count(skip) != 1:
        raise ValueError("canonical footer loop shape changed")
    body = body.replace(skip, "", 1)
    body = body.replace('        path = Path(str(row["output_path"]))\n',
                        "        row, path = opened.row, opened.path\n", 1)
    read_start = body.index("            # Opening the Parquet metadata")
    read_stop = body.index("            if before is not None:\n", read_start)
    body = body[:read_start] + (
        "            footer_open_attempts += int(opened.attempted)\n"
        "            if opened.error is not None:\n"
        "                raise opened.error\n"
        "            before, parquet_file = opened.before, opened.parquet_file\n"
    ) + body[read_stop:]
    replacement = (
        "    segment_rows = (row for row in " + query.lstrip()
        + ' if str(row["segment_id"]) not in known)\n'
        "    with prefetched_footer_rows(\n"
        "        segment_rows, opener=pq.ParquetFile, signature=_view_file_signature,\n"
        "        grouped_endpoint=SCHEMA_GROUPED_ENDPOINT, collect_proofs=view_proofs is not None,\n"
        "        workers=2, max_pending=4,\n"
        "    ) as prepared:\n"
        "        for opened in prepared:\n"
        + textwrap.indent(body, "    ")
    )
    return source[:start] + replacement + source[stop:]


def worker(root: Path, output: Path, variant: str) -> None:
    from stockagent.data.source_processing_runtime import configure_cpu_source_runtime
    configure_cpu_source_runtime(2)
    import pyarrow as pa
    import requests
    from downloader.artifact_io import atomic_write_json
    from scripts import compact_openbb_l1 as module
    pa.set_cpu_count(2)
    fresh_output(output)
    fixture = json.loads((root / "fixture.json").read_bytes())
    path = root / "segments.sqlite3"
    if (fixture.get("contract") != CONTRACT or implementation_identity() != fixture["implementation"]
            or digest(path) != fixture["manifest_sha256"]):
        raise RuntimeError("fixture or exact code/runtime generation changed")

    def forbidden(*_args, **_kwargs):
        raise AssertionError("footer reader trial must not call a provider")
    requests.Session.request = forbidden
    with sqlite3.connect(f"file:{path.resolve()}?mode=ro&immutable=1", uri=True) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        if file_signatures(connection) != fixture["source_signatures_sha256"]:
            raise RuntimeError("source signature changed before canonical audit")
        timings, proofs, metadata_audit = {}, {}, {}
        reader_counts = {"footer_open_attempts": 0}
        before = module._stage_resources()
        started, cpu_started = time.perf_counter(), time.process_time()
        with reader_variant(module, variant, reader_counts=reader_counts) as cache:
            stale_endpoints, stale_count = module._mark_stale_segments(
                connection, [], changed_task_ids=[], timing=timings, view_proofs=proofs,
                metadata_audit=metadata_audit,
            )
        wall, cpu = time.perf_counter() - started, time.process_time() - cpu_started
        after = module._stage_resources()
        if (stale_endpoints or stale_count
                or reader_counts["footer_open_attempts"] != fixture["selected_segments"]):
            raise RuntimeError("canonical audit found invalid derivatives")
        if file_signatures(connection) != fixture["source_signatures_sha256"]:
            raise RuntimeError("source signature changed during canonical audit")
    result_digest = hashlib.sha256()
    result_digest.update(json.dumps([sorted(stale_endpoints), stale_count]).encode())
    for key, proof in sorted(proofs.items()):
        result_digest.update(json.dumps([key, asdict(proof)], sort_keys=True, separators=(",", ":")).encode())
        result_digest.update(b"\n")
    if (implementation_identity() != fixture["implementation"] or digest(path) != fixture["manifest_sha256"]
            or "torch" in sys.modules):
        raise RuntimeError("code/runtime/fixture changed or model runtime imported")
    receipt = {
        "contract": CONTRACT, "state": "completed_trial", "variant": variant,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "wall_seconds": wall, "cpu_seconds": cpu,
        "selected_segments": fixture["selected_segments"], "all_success_segments": fixture["all_success_segments"],
        "stale_count": stale_count, "view_proofs": len(proofs),
        "complete_audit_output_sha256": result_digest.hexdigest(),
        "fixture_sha256": digest(root / "fixture.json"),
        "stage_seconds": timings, "resources_before": before, "resources_after": after,
        "rss_peak_bytes": next(int(line.split()[1]) * 1024
            for line in Path("/proc/self/status").read_text().splitlines() if line.startswith("VmHWM:")),
        "provider_calls": 0, "production_writes": False, "model_runtime_imported": False,
        "physical_cache": (cache.snapshot() if variant == "schema_cache" else metadata_audit.get("physical_cache")),
        "footer_io": cache.snapshot() if variant in {"local", "metadata_only", "prefetch"} else None,
        "footer_open_attempts": reader_counts["footer_open_attempts"],
    }
    atomic_write_json(output, receipt)
    print(json.dumps({key: receipt[key] for key in (
        "variant", "wall_seconds", "cpu_seconds", "selected_segments", "view_proofs", "complete_audit_output_sha256"
    )}), flush=True)


def run_comparison(root: Path, manifest: Path) -> None:
    from downloader.artifact_io import atomic_write_json
    from scripts.benchmark_openbb_l1_source_contract import native_budget
    from stockagent.data.source_processing_runtime import configure_cpu_source_runtime
    configure_cpu_source_runtime(2)
    budget = native_budget()
    capture(root, manifest, 0)
    sequence = ("adopted", "local", "metadata_only", "prefetch", "prefetch", "metadata_only", "local", "adopted")
    trials = []
    for index, variant in enumerate(("adopted", *sequence)):
        output = root / ("warmup.json" if index == 0 else f"trial-{index}-{variant}.json")
        subprocess.run([sys.executable, str(Path(__file__).resolve()), "--fixture", str(root),
                        "--worker", variant, "--output", str(output)], check=True)
        if index:
            trials.append(json.loads(output.read_bytes()))
    fixture = json.loads((root / "fixture.json").read_bytes())
    if (len({row["complete_audit_output_sha256"] for row in trials}) != 1
            or any(row["selected_segments"] != fixture["selected_segments"]
                   or row["all_success_segments"] is not True
                   or row["footer_open_attempts"] != fixture["selected_segments"]
                   or row["state"] != "completed_trial" for row in trials)):
        raise RuntimeError("full derivative/proof parity or coverage differs")
    means = {variant: {key: sum(row[key] for row in trials if row["variant"] == variant) / 2
                       for key in ("wall_seconds", "cpu_seconds", "rss_peak_bytes")}
             for variant in set(sequence)}
    reductions = {variant: {key: 100 * (1 - value / means["adopted"][key])
                           for key, value in means[variant].items()}
                  for variant in means if variant != "adopted"}
    result = {"contract": CONTRACT, "state": "accepted_complete_audit_output_parity",
              "observed_at_utc": datetime.now(timezone.utc).isoformat(), "trials": trials,
              "warmup": json.loads((root / "warmup.json").read_bytes()), "means": means,
              "reductions_percent": reductions, "budget": budget,
              "fixture_sha256": digest(root / "fixture.json"),
              "all_success_segments": True, "exact_complete_output_parity": True,
              "scope": "all derivative footers and complete view proofs; not whole compaction or payload audit",
              "os_page_cache_controlled": False, "exclusive_host": False,
              "provider_calls": 0, "production_writes": False}
    atomic_write_json(root / "comparison.json", result)
    print(json.dumps({"state": result["state"], "reductions_percent": reductions}), flush=True)


def aggregate(root: Path, output: Path, candidate: str = "mmap") -> None:
    from downloader.artifact_io import atomic_write_json
    fresh_output(output)
    fixture_sha = digest(root / "fixture.json")
    fixture = json.loads((root / "fixture.json").read_bytes())
    variants = ("original", candidate, candidate, "original")
    trials = [json.loads((root / f"{index:02d}-{variant}.json").read_bytes())
              for index, variant in enumerate(variants, 1)]
    if (candidate not in {"mmap", "schema_cache"} or fixture["contract"] != CONTRACT
            or [row["variant"] for row in trials] != list(variants)
            or any(row["contract"] != CONTRACT or row["state"] != "completed_trial"
                   or row["fixture_sha256"] != fixture_sha or row["stale_count"] != 0
                   or row["selected_segments"] != fixture["selected_segments"]
                   or row["footer_open_attempts"] != fixture["selected_segments"]
                   or row["all_success_segments"] is not fixture["all_success_segments"]
                   or row["provider_calls"] != 0 or row["production_writes"] is not False
                   or row["model_runtime_imported"] is not False for row in trials)
            or len({(row["selected_segments"], row["view_proofs"], row["complete_audit_output_sha256"])
                    for row in trials}) != 1):
        raise ValueError("complete canonical ABBA evidence does not match")
    means = {variant: {key: sum(row[key] for row in trials if row["variant"] == variant) / 2
                       for key in ("wall_seconds", "cpu_seconds", "rss_peak_bytes")}
             for variant in ("original", candidate)}
    reductions = {key: 100 * (1 - means[candidate][key] / means["original"][key]) for key in means["original"]}
    receipt = {
        "contract": CONTRACT, "state": "accepted_phase_comparison",
        "observed_at_utc": datetime.now(timezone.utc).isoformat(),
        "trials": trials, "means": means, "reductions_percent": reductions,
        "exact_canonical_complete_output_parity": True,
        "scope": "all captured derivative footers and physical view proofs; not whole compaction or payload audit",
        "all_success_segments": all(row["all_success_segments"] for row in trials),
        "os_page_cache_controlled": False, "exclusive_host": False,
        "production_writes": False, "provider_calls": 0,
        "candidate": candidate,
    }
    atomic_write_json(output, receipt)
    print(json.dumps({"state": receipt["state"], "reductions_percent": reductions}), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", required=True, type=Path)
    parser.add_argument("--capture", action="store_true")
    parser.add_argument("--run-comparison", action="store_true", help="full adopted/local/metadata/prefetch mirror-order trials")
    parser.add_argument("--manifest", type=Path, default=ROOT / "data_openBB/_state/openbb_archive.sqlite3")
    parser.add_argument("--limit", type=int, default=0, help="0 captures all successful segments; samples are explicitly labeled")
    parser.add_argument("--worker", choices=("original", "mmap", "schema_cache", "adopted", "local", "metadata_only", "prefetch"))
    parser.add_argument("--candidate", choices=("mmap", "schema_cache"), default="mmap")
    parser.add_argument("--aggregate", action="store_true")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.run_comparison:
        run_comparison(args.fixture, args.manifest)
    elif args.capture:
        capture(args.fixture, args.manifest, args.limit)
    elif args.worker and args.output:
        worker(args.fixture, args.output, args.worker)
    elif args.aggregate and args.output:
        aggregate(args.fixture, args.output, args.candidate)
    else:
        parser.error("use --capture, --worker/--output or --aggregate/--output")


if __name__ == "__main__":
    main()
