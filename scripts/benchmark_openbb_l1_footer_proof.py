"""Bounded read-only attribution of the OpenBB L1 footer-proof overhead.

Production code is only imported, never edited. Every trial opens every selected
footer again; the parent fences exact manifest rows and file stat signatures.
Experimental caches are per-worker/per-file and must produce identical proofs.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from functools import lru_cache
import hashlib
import json
import os
from pathlib import Path
import resource
import sqlite3
import subprocess
import sys
import time


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ENDPOINT = "regulators.sec.filing_headers"
LEGACY_SEQUENCE = ("ordinary", "safe", "safe", "ordinary", "cached_arrow", "cached_arrow_json", "profile_safe")
SINGLE_PASS_SEQUENCE = ("safe", "single_pass_proof", "single_pass_proof", "safe", "profile_safe", "profile_single_pass")
VARIANTS = frozenset((*LEGACY_SEQUENCE, *SINGLE_PASS_SEQUENCE))


class Timings:
    def __init__(self) -> None:
        self.rows = defaultdict(lambda: {"calls": 0, "wall_seconds": 0.0, "cpu_seconds": 0.0})

    def call(self, label, function, *args, **kwargs):
        wall, cpu = time.perf_counter(), time.process_time()
        try:
            return function(*args, **kwargs)
        finally:
            row = self.rows[label]
            row["calls"] += 1
            row["wall_seconds"] += time.perf_counter() - wall
            row["cpu_seconds"] += time.process_time() - cpu


class _Profiled:
    """Transparent probe for the existing Arrow calls, diagnostic trial only."""

    def __init__(self, value, timings: Timings, kind: str):
        self.value, self.timings, self.kind = value, timings, kind

    def __getattr__(self, name):
        if name in {"schema_arrow", "schema", "logical_type"}:
            value = self.timings.call(name, getattr, self.value, name)
            return _Profiled(value, self.timings, name)
        if name in {"remove_metadata", "serialize", "to_pybytes", "column", "to_json"}:
            def wrapped(*args, **kwargs):
                value = self.timings.call(name, getattr(self.value, name), *args, **kwargs)
                if name in {"remove_metadata", "serialize", "column"}:
                    return _Profiled(value, self.timings, name)
                return value
            return wrapped
        return getattr(self.value, name)

    def __len__(self):
        return len(self.value)

    def __str__(self):
        return str(self.value)


class _CachedArrow:
    """Per-file immutable Arrow bytes only; no cross-file schema inference."""

    def __init__(self, parquet):
        self.parquet = parquet
        self.cleaned = None
        self.buffer = None
        self.content = None

    def __getattr__(self, name):
        return getattr(self.parquet, name)

    @property
    def schema_arrow(self):
        return self

    def remove_metadata(self):
        if self.cleaned is None:
            self.cleaned = self.parquet.schema_arrow.remove_metadata()
        return self

    def serialize(self):
        if self.buffer is None:
            self.buffer = self.cleaned.serialize()
        return self

    def to_pybytes(self):
        if self.content is None:
            self.content = self.buffer.to_pybytes()
        return self.content


def _signature(path: Path) -> tuple[int, int, int, int, int]:
    info = path.stat()
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def _arrow_fingerprint(parquet) -> str:
    return hashlib.sha256(parquet.schema_arrow.remove_metadata().serialize().to_pybytes()).hexdigest()


def _resources() -> dict[str, int]:
    result = {"peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024}
    for line in Path("/proc/self/io").read_text().splitlines():
        name, _, value = line.partition(":")
        if name in {"rchar", "read_bytes", "write_bytes"}:
            result[name] = int(value)
    return result


@contextmanager
def _instrument(compactor, identity, timings: Timings, variant: str):
    original_identity = identity._physical_schema_identity
    original_joint = compactor.schema_identities
    original_stat = compactor._view_file_signature
    original_tree, original_loads, original_dumps = identity._schema_tree, json.loads, json.dumps
    cached_loads = lru_cache(maxsize=128)(original_loads)
    try:
        identity._physical_schema_identity = lambda pf, digest: timings.call("physical_identity_core", original_identity, pf, digest)
        compactor.schema_identities = lambda pf: timings.call("joint_schema_identities", original_joint, pf)
        compactor._view_file_signature = lambda path: timings.call("proof_stat_after", original_stat, path)
        if variant == "cached_arrow_json":
            # Exact logical JSON bytes are still read from every physical leaf.
            # Only deterministic parsing is memoized; nothing mutates the dict.
            json.loads = cached_loads
        if variant in {"profile_safe", "profile_single_pass"}:
            identity._schema_tree = lambda schema: timings.call("tree_validation", original_tree, schema)
            json.loads = lambda *a, **k: timings.call("logical_json_parse", original_loads, *a, **k)
            json.dumps = lambda *a, **k: timings.call("canonical_json_encode", original_dumps, *a, **k)
        yield cached_loads
    finally:
        identity._physical_schema_identity = original_identity
        compactor.schema_identities = original_joint
        compactor._view_file_signature = original_stat
        identity._schema_tree, json.loads, json.dumps = original_tree, original_loads, original_dumps


def _legacy_view_source_proof(compactor, identity, path, segment_id, parquet, before,
                              *, output_rows, output_bytes, arrow_fingerprint):
    """Frozen pre-single-pass caller: retain its second and third Arrow reads.

    The public physical helper still observes its own Arrow digest. A pinned
    historical proof vector must match, so both new paths cannot silently agree
    on a changed physical identity and be called a successful optimization.
    """
    observed_arrow = _arrow_fingerprint(parquet)
    if (parquet.metadata.num_rows != output_rows or before[2] != output_bytes
            or observed_arrow != arrow_fingerprint):
        raise RuntimeError("legacy source receipt mismatch")
    physical = identity.physical_schema_identity(parquet)
    if compactor._view_file_signature(path) != before:
        raise RuntimeError("source changed during legacy proof")
    return compactor.ViewSourceProof(
        segment_id, compactor._lexical_absolute_path(path), before,
        output_rows, observed_arrow, physical,
    )


def _worker(payload: dict[str, object]) -> dict[str, object]:
    import pyarrow.parquet as pq
    from scripts import compact_openbb_l1 as compactor
    from scripts import openbb_l1_view_schema as identity

    signatures = {path: tuple(value) for path, value in payload["signatures"].items()}
    for path, signature in signatures.items():
        if _signature(Path(path)) != signature:
            raise RuntimeError("source changed before trial")
    variant = str(payload["variant"])
    timings = Timings()
    integrity_digest, identity_digest = hashlib.sha256(), hashlib.sha256()
    identities = set()
    safe = variant != "ordinary"
    single_pass = variant in {"single_pass_proof", "profile_single_pass"}
    before_resources = _resources()
    wall, cpu = time.perf_counter(), time.process_time()
    with _instrument(compactor, identity, timings, variant) as cache:
        for row in payload["segments"]:
            path = Path(row["output_path"])
            before = timings.call("proof_stat_before", _signature, path) if safe else None
            parquet = timings.call("footer_open", pq.ParquetFile, str(path))
            if variant in {"profile_safe", "profile_single_pass"}:
                parquet = _Profiled(parquet, timings, "parquet")
            elif variant.startswith("cached_arrow"):
                parquet = _CachedArrow(parquet)
            if not single_pass:
                if parquet.metadata.num_rows != row["output_rows"]:
                    raise RuntimeError("source row receipt mismatch")
                fingerprint = timings.call("ordinary_arrow_integrity", _arrow_fingerprint, parquet)
                if fingerprint != row["schema_fingerprint"]:
                    raise RuntimeError("source schema receipt mismatch")
            if safe:
                proof_function = compactor._view_source_proof if single_pass else (
                    lambda *args, **kwargs: _legacy_view_source_proof(compactor, identity, *args, **kwargs)
                )
                proof = timings.call(
                    "complete_safe_proof", proof_function,
                    path, row["segment_id"], parquet, before,
                    output_rows=row["output_rows"], output_bytes=row["output_bytes"],
                    arrow_fingerprint=row["schema_fingerprint"],
                )
                if proof.physical_identity is None:
                    raise RuntimeError("sample has unsupported physical proof")
                identities.add(proof.physical_identity)
                identity_digest.update(bytes.fromhex(proof.physical_identity))
                fingerprint = proof.arrow_fingerprint
            integrity_digest.update(bytes.fromhex(fingerprint))
            integrity_digest.update(int(row["output_rows"]).to_bytes(8, "little"))
        elapsed_wall, elapsed_cpu = time.perf_counter() - wall, time.process_time() - cpu
        cache_info = cache.cache_info()._asdict()
    for path, signature in signatures.items():
        if _signature(Path(path)) != signature:
            raise RuntimeError("source changed during trial")
    return {
        "variant": variant, "wall_seconds": elapsed_wall, "cpu_seconds": elapsed_cpu,
        "timings": dict(timings.rows), "integrity_sha256": integrity_digest.hexdigest(),
        "physical_identity_sha256": identity_digest.hexdigest() if safe else None,
        "physical_groups": len(identities) if safe else None,
        "logical_json_cache": cache_info if variant == "cached_arrow_json" else None,
        "resources_before": before_resources, "resources_after": _resources(),
        "phases_are_nested_not_additive": True,
        "diagnostic_instrumentation": variant in {"profile_safe", "profile_single_pass"},
    }


def benchmark(manifest: Path, *, count: int = 1024, created_at_before: str | None = None,
              sequence: tuple[str, ...] = SINGLE_PASS_SEQUENCE,
              baseline_receipt: Path | None = None,
              baseline_receipt_sha256: str | None = None) -> dict[str, object]:
    if not 1 <= count <= 4096:
        raise ValueError("sample must contain at most 4096 segments")
    if not sequence or not set(sequence) <= set(VARIANTS):
        raise ValueError("invalid benchmark sequence")
    if (baseline_receipt is None) != (baseline_receipt_sha256 is None):
        raise ValueError("historical proof requires both receipt and SHA256")
    historical = None
    historical_identity = None
    if baseline_receipt is not None:
        content = baseline_receipt.read_bytes()
        if hashlib.sha256(content).hexdigest() != baseline_receipt_sha256:
            raise RuntimeError("historical footer receipt SHA256 mismatch")
        historical = json.loads(content)
        historical_identities = {trial["physical_identity_sha256"] for trial in historical["trials"] if trial["variant"] == "safe"}
        if (historical.get("state") != "verified" or historical.get("physical_identity_byte_equal") is not True
                or historical.get("source_signatures_unchanged") is not True
                or historical.get("integrity_equal") is not True
                or len(historical_identities) != 1 or None in historical_identities):
            raise RuntimeError("historical footer receipt lacks verified legacy proofs")
        historical_identity = next(iter(historical_identities))
    with sqlite3.connect(f"{manifest.resolve().as_uri()}?mode=ro", uri=True) as connection:
        connection.row_factory = sqlite3.Row
        clause = " AND created_at<=?" if created_at_before else ""
        parameters = (ENDPOINT, created_at_before, count) if clause else (ENDPOINT, count)
        segments = [dict(row) for row in connection.execute(
            "SELECT segment_id,output_path,output_rows,output_bytes,schema_fingerprint "
            "FROM l1_compaction_segments WHERE status='success' AND endpoint=?"
            + clause + " ORDER BY segment_id LIMIT ?", parameters,
        )]
    if not segments:
        raise ValueError("no matching SEC segments")
    contract_sha256 = hashlib.sha256(json.dumps(segments, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if historical is not None and any(historical.get(key) != value for key, value in (
        ("endpoint", ENDPOINT), ("segments", len(segments)), ("created_at_before", created_at_before),
        ("ordered_source_contract_sha256", contract_sha256),
    )):
        raise RuntimeError("historical footer source contract mismatch")
    signatures = {row["output_path"]: _signature(Path(row["output_path"])) for row in segments}
    if any(signatures[row["output_path"]][2] != row["output_bytes"] for row in segments):
        raise RuntimeError("source byte receipt mismatch")
    code_paths = (Path(__file__).resolve(), ROOT / "scripts/compact_openbb_l1.py", ROOT / "scripts/openbb_l1_view_schema.py")
    code_hashes = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in code_paths}
    trials = []
    for variant in sequence:
        result = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--worker"],
            input=json.dumps({"segments": segments, "signatures": signatures, "variant": variant}),
            text=True, capture_output=True, check=True, timeout=120,
        )
        trials.append(json.loads(result.stdout))
        if any(_signature(Path(path)) != old for path, old in signatures.items()):
            raise RuntimeError("source changed between trials")
        if any(hashlib.sha256(Path(path).read_bytes()).hexdigest() != old for path, old in code_hashes.items()):
            raise RuntimeError("implementation changed during benchmark")
    integrity_equal = len({row["integrity_sha256"] for row in trials}) == 1
    identity_equal = len({row["physical_identity_sha256"] for row in trials if row["variant"] != "ordinary"}) == 1
    pinned_identity_equal = historical_identity is None or all(
        row["physical_identity_sha256"] == historical_identity for row in trials if row["variant"] != "ordinary"
    )
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(), "segments": len(segments),
        "endpoint": ENDPOINT, "created_at_before": created_at_before,
        "ordered_source_contract_sha256": contract_sha256,
        "production_writes": False, "source_open_mode": "read_only", "source_signatures_unchanged": True,
        "integrity_equal": integrity_equal, "physical_identity_byte_equal": identity_equal,
        "state": "verified" if integrity_equal and identity_equal and pinned_identity_equal else "mismatch",
        "historical_physical_identity_equal": pinned_identity_equal if historical else None,
        "historical_proof": None if historical is None else {
            "receipt_path": str(baseline_receipt.resolve()), "receipt_sha256": baseline_receipt_sha256,
            "physical_identity_sha256": historical_identity, "paired_timing_comparison": False,
        },
        "implementation_sha256": code_hashes, "nice": os.getpriority(os.PRIO_PROCESS, 0),
        "trials": trials,
        "limits": ["At most 4096 unique files; every trial reopens every footer.",
                   "Profile trial has instrumentation overhead and is not a speed comparison.",
                   "Cached variants are benchmark-only, preserve current checks, and are not deployed.",
                   "Nested phase timings cannot be summed; page cache and host load can affect wall time."],
    }


def main() -> int:
    if sys.argv[1:] == ["--worker"]:
        print(json.dumps(_worker(json.load(sys.stdin))))
        return 0
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=Path("data_openBB/_state/openbb_archive.sqlite3"))
    parser.add_argument("--count", type=int, default=1024)
    parser.add_argument("--created-at-before")
    parser.add_argument("--sequence", choices=("old-single", "ordinary-safe"), default="old-single")
    parser.add_argument("--baseline-receipt", type=Path)
    parser.add_argument("--baseline-receipt-sha256")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.count <= 4096:
        parser.error("--count must be between 1 and 4096")
    if not args.output.resolve().is_relative_to((ROOT / "artifacts/benchmarks").resolve()):
        parser.error("--output must be under artifacts/benchmarks")
    if args.baseline_receipt is not None and args.output.resolve() == args.baseline_receipt.resolve():
        parser.error("--output cannot overwrite the historical baseline")
    os.nice(15)
    subprocess.run(["ionice", "-c", "3", "-p", str(os.getpid())], check=True)
    result = benchmark(
        args.manifest, count=args.count, created_at_before=args.created_at_before,
        sequence=SINGLE_PASS_SEQUENCE if args.sequence == "old-single" else LEGACY_SEQUENCE,
        baseline_receipt=args.baseline_receipt, baseline_receipt_sha256=args.baseline_receipt_sha256,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({key: result[key] for key in ("state", "segments", "physical_identity_byte_equal")}))
    return 0 if result["state"] == "verified" else 2


if __name__ == "__main__":
    raise SystemExit(main())
