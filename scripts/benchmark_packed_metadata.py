"""ABBA complete packed publish/recover/materialize benchmark in a private fixture.

Compare retained metadata/serial durability/two-pass new files with bounded
metadata/parallel durability/one-pass new files, under the SAME canonical
SHA/CRC/reader implementation. The reference emulates the previous work, not
an old production binary. --metadata-only isolates row-index RAM and CPU cost.
No provider/source/current cold head is touched.

--bootstrap-only measures the real public-to-private donor reader/index in an
isolated synthetic inventory, not source coverage or a full payload restore.
"""
from __future__ import annotations

import argparse
import ast
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import resource
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from downloader.artifact_io import atomic_write_json
from stockagent.data_sync import packed_snapshots as packed


class _SequentialPackWriter:
    """Engineering-only forward ZIP sink; not the production default.

    Full D publish/recovery/readback ABBA rejected this encoding as slower in
    the measured workflow. Retain the experiment here, not in the publisher.
    """
    def __init__(self, stream):
        self.stream, self.position = stream, 0

    def write(self, block):
        count = self.stream.write(block)
        if count != len(block):
            raise OSError("short sequential ZIP write; retain uncommitted staging")
        self.position += count
        return count

    def tell(self):
        return self.position

    def seek(self, *_args):
        raise io.UnsupportedOperation("sequential ZIP sink")

    def flush(self):
        self.stream.flush()


@contextmanager
def _sequential_zip(destination, **options):
    with destination.open("xb", buffering=8 * 1024 * 1024) as stream, \
            packed.zipfile.ZipFile(_SequentialPackWriter(stream), **options) as archive:
        yield archive


def tree_digest(root):
    digest = hashlib.sha256()
    for current, directories, files in os.walk(root):
        directories.sort()
        for name in sorted(files):
            path = Path(current) / name
            digest.update(path.relative_to(root).as_posix().encode() + b"\0")
            digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def pack_writer_reference(path, expected_sha, *, sequential=False):
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_sha:
        raise ValueError("preserved pack-writer source SHA differs")
    tree = ast.parse(raw)
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "_write_pack"]
    if len(nodes) != 1:
        raise ValueError("one preserved canonical pack writer is required")
    namespace = dict(packed.__dict__)
    if sequential:
        # Change only the output adapter in this private execution namespace,
        # never monkeypatch the shared stdlib module or production publisher.
        namespace['zipfile'] = SimpleNamespace(**vars(packed.zipfile))
        namespace['zipfile'].ZipFile = _sequential_zip
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), namespace)
    return namespace["_write_pack"]


def trial(files, variant, memory_rows, *, pack_write_only=False, pack_buffer_only=False,
          reference=None, reference_sha=None,
          cold_parent=None, stored_member_bytes=0):
    pack_scope = pack_write_only or pack_buffer_only
    packed.METADATA_MEMORY_ROWS = (10**9 if variant == "retained" and not pack_scope else memory_rows)
    packed.MATERIALIZE_FSYNC_WORKERS = 0 if variant == "retained" and not pack_scope else 4
    write_pack = packed._write_pack
    if pack_write_only:
        packed._write_pack = pack_writer_reference(reference, reference_sha, sequential=variant != "retained")
    elif pack_buffer_only:
        packed._write_pack = (pack_writer_reference(reference, reference_sha)
                              if variant == "retained" else write_pack)
    elif variant == "retained":
        def two_pass(source, entries, temporary, **options):
            entries = list(entries)  # Already bounded by MAX_PACK_MEMBERS.
            for entry in entries:
                if entry.sha256 is None:
                    entry.sha256 = packed._hash_source_entry(source, entry)
            return write_pack(source, entries, temporary, **options)
        packed._write_pack = two_pass
    with tempfile.TemporaryDirectory(prefix="stockagent-packed-benchmark-", dir="/var/tmp") as temporary, \
            (tempfile.TemporaryDirectory(prefix="stockagent-pack-write-benchmark-", dir=cold_parent)
             if cold_parent else nullcontext(None)) as cold_temporary:
        base = Path(temporary)
        source, cold = base / "source", Path(cold_temporary) if cold_temporary else base / "cold"
        source.mkdir()
        for number in range(files):
            directory = source / f"{number // 500:06}"
            directory.mkdir(exist_ok=True)
            if number >= 3 and stored_member_bytes:
                # Opaque byte-storage fixture, not a claimed Parquet financial
                # table. Exercise the same already-compressed suffix policy.
                block = hashlib.sha256(str(number).encode()).digest()
                (directory / f"{number:09}.parquet").write_bytes(
                    (block * ((stored_member_bytes + 31) // 32))[:stored_member_bytes])
            else:
                (directory / f"{number:09}.json").write_text(json.dumps(
                    {"code": "0050", "value": -number, "missing": None, "volume_unit": "shares"}))
        (source / "large.bin").write_bytes(b"a" * 524288 + b"b" * 524288)
        original = tree_digest(source)
        phases, versions = [], []
        started, cpu = time.perf_counter(), time.process_time()
        for version in range(2):
            phase = {"stage": "baseline" if version == 0 else "incremental"}
            stamp = time.perf_counter()
            release = packed.publish_packed_snapshot(cold, "benchmark", source, node_id="fixture",
                loose_file_threshold_bytes=524288, file_chunk_bytes=524288,
                pack_buckets=8, batch_directory_fsync=True)
            phase["publish_seconds"] = time.perf_counter() - stamp
            stamp = time.perf_counter()
            proof = packed.verify_packed_snapshot(cold, release, reconstruct_all=True)
            if proof["independently_reconstructed_files"] != files + 1:
                raise ValueError("all-file reconstruction count differs")
            phase["full_recovery_seconds"] = time.perf_counter() - stamp
            stamp = time.perf_counter()
            restored = packed.fetch_packed_snapshot(cold, base / "restored", release)
            if tree_digest(restored) != tree_digest(source):
                raise ValueError("restored original bytes differ")
            if json.loads((restored / "000000/000000000.json").read_text())["missing"] is not None:
                raise ValueError("NULL changed during recovery")
            phase["materialize_and_readback_seconds"] = time.perf_counter() - stamp
            phase["inventory_sha256"] = release.manifest["archive"]["inventory"]["sha256"]
            phase["original_tree_sha256"] = tree_digest(source)
            phase["new_stored_bytes"] = release.manifest["archive"]["new_stored_bytes"]
            phases.append(phase)
            versions.append(release)
            if version == 0:
                stamp = time.perf_counter()
                noop = packed.publish_packed_snapshot(cold, "benchmark", source, node_id="fixture",
                    loose_file_threshold_bytes=524288, file_chunk_bytes=524288,
                    pack_buckets=8, batch_directory_fsync=True)
                if noop.manifest_sha256 != release.manifest_sha256:
                    raise ValueError("no-op advanced the fixed semantic head")
                phases.append({"stage": "semantic_noop", "seconds": time.perf_counter() - stamp})
                (source / "000000/000000001.json").write_bytes(b"historical revision")
                (source / "000000/000000002.json").unlink()
                (source / "000000/new.json").write_bytes(b"new observation")
                with (source / "large.bin").open("ab") as stream:
                    stream.write(b"incremental tail")
        stamp = time.perf_counter()
        old = packed.fetch_packed_snapshot(cold, base / "old-restored", versions[0])
        if tree_digest(old) != original:
            raise ValueError("incremental change damaged the old version")
        phases.append({"stage": "old_version_full_restore_and_readback", "seconds": time.perf_counter() - stamp})
        return {"variant": variant, "source_files_per_version": files + 1,
                "metadata_memory_rows": packed.METADATA_MEMORY_ROWS,
                "fsync_workers": packed.MATERIALIZE_FSYNC_WORKERS,
                "complete_wall_seconds": time.perf_counter() - started,
                "complete_cpu_seconds": time.process_time() - cpu,
                "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
                "phases": phases, "all_originals_and_old_version_verified": True}


def metadata_trial(files, variant, memory_rows):
    """Controlled census/routing/inventory, without creating payload files."""
    started, cpu = time.perf_counter(), time.process_time()
    with packed.MetadataRows(packed._entry_record, packed._entry_from_record,
                             memory_rows=10**9 if variant == "retained" else memory_rows) as rows:
        for number in range(files):
            rows.append(packed._SourceEntry(path=f"observations/{number:09}.json", kind="file",
                mode=0o644, size=256, source_stat=(256, 1, 1, 1, number)))
        for number, row in enumerate(rows):
            row.sha256 = hashlib.sha256(row.path.encode()).hexdigest()
            row.storage = {"kind": "pack", "member": row.path,
                           "object_sha256": f"{number % 16:064x}"}
            rows.update(row, changed_bucket=number % 16)
        groups = rows.changed_groups()
        digest = hashlib.sha256()
        count = 0
        for row in rows:
            digest.update(json.dumps(packed._entry_record(row), sort_keys=True).encode() + b"\n")
            count += 1
        if count != files or sum(group[1] for group in groups) != files:
            raise ValueError("metadata census/routing lost original rows")
        return {"variant": variant, "metadata_rows": count, "spilled": rows.spilled,
                "complete_wall_seconds": time.perf_counter() - started,
                "complete_cpu_seconds": time.process_time() - cpu,
                "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
                "inventory_sha256": digest.hexdigest()}


def bootstrap_trial(files, variant, memory_rows):
    from stockagent.data_sync import dataset_migration as migration
    with tempfile.TemporaryDirectory(prefix="stockagent-bootstrap-benchmark-", dir="/var/tmp") as temporary:
        base = Path(temporary)
        source, parent, cold = base / "source", base / "public", base / "private"
        source.mkdir()
        (parent / "heads/donor").mkdir(parents=True)
        cold.mkdir()
        body = b"metadata-only fixture object"
        digest = hashlib.sha256(body).hexdigest()
        relative = "objects/blobs/" + digest[:2] + "/" + digest + ".blob"
        payload = parent / relative
        payload.parent.mkdir(parents=True)
        payload.write_bytes(body)
        inventory = parent / "inventory.jsonl.gz"
        with gzip.open(inventory, "wt", encoding="utf-8") as stream:
            for number in range(files):
                row = {"path": f"observations/{number:09}.json", "kind": "file", "mode": 0o644,
                       "size": len(body), "sha256": digest,
                       "storage": {"kind": "blob", "object_sha256": digest}}
                stream.write(json.dumps(row, separators=(",", ":")) + "\n")
        manifest = {"snapshot_id": "fixture", "archive": {
            "inventory": {"relpath": inventory.name, "bytes": inventory.stat().st_size,
                          "sha256": hashlib.sha256(inventory.read_bytes()).hexdigest()},
            "objects": [{"sha256": digest, "relpath": relative, "bytes": len(body)}]}}
        donor = {"dataset": "donor", "source": str(source), "publish": True}
        selected = SimpleNamespace(manifest=manifest, manifest_sha256="f" * 64)
        native_iterator = migration._iter_inventory
        reader = (lambda *args: iter(list(native_iterator(*args)))) if variant == "retained" else native_iterator
        context = nullcontext(None) if variant == "retained" else packed.MetadataRows(
            dict, dict, memory_rows=memory_rows)
        started, cpu = time.perf_counter(), time.process_time()
        with patch.object(migration, "resolve_latest_packed", return_value=selected), \
                patch.object(migration, "_iter_inventory", reader), context as sink:
            hints, objects, proofs = migration._bootstrap_public_storage(base,
                {"dataset": "private", "source": str(source)}, parent, cold, [donor], file_sink=sink)
            bootstrap_seconds = time.perf_counter() - started
            result_hash = hashlib.sha256()
            rows = hints.values() if isinstance(hints, dict) else hints
            for row in rows:
                result_hash.update(json.dumps(row, sort_keys=True).encode() + b"\n")
            if len(hints) != files or len(objects) != 1 or len(proofs) != 1:
                raise ValueError("bootstrap lost donor rows or exact object bindings")
            if payload.stat().st_ino != (cold / relative).stat().st_ino:
                raise ValueError("bootstrap duplicated the admitted immutable object")
            return {"variant": variant, "metadata_rows": len(hints),
                    "spilled": isinstance(hints, packed.MetadataRows) and hints.spilled,
                    "bootstrap_seconds": bootstrap_seconds,
                    "complete_wall_seconds": time.perf_counter() - started,
                    "complete_cpu_seconds": time.process_time() - cpu,
                    "peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
                    "inventory_sha256": result_hash.hexdigest(), "public_heads_modified": 0}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--files", type=int, default=4096)
    parser.add_argument("--memory-rows", type=int, default=1024)
    parser.add_argument("--output", type=Path)
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument("--metadata-only", action="store_true")
    scope.add_argument("--bootstrap-only", action="store_true")
    scope.add_argument("--pack-write-only", action="store_true",
                       help="compare only the preserved seekable and current sequential pack writer")
    scope.add_argument("--pack-buffer-only", action="store_true",
                       help="compare seekable buffering with the preserved writer; identical ZIP bytes")
    parser.add_argument("--reference", type=Path)
    parser.add_argument("--reference-sha256")
    parser.add_argument("--d-cold-fixture", action="store_true",
                       help="use only owned private D scratch, under canonical mount guards")
    parser.add_argument("--native-limits", action="store_true")
    parser.add_argument("--stored-member-bytes", type=int, default=0,
                       help="opaque stored-format bytes per member in the pack-write fixture")
    parser.add_argument("--trial", choices=("retained", "bounded"), help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    metadata_scope = args.metadata_only or args.bootstrap_only
    pack_scope = args.pack_write_only or args.pack_buffer_only
    if not 10 <= args.files <= (1000000 if metadata_scope else 200000):
        parser.error("choose a bounded 10..200000 file or 10..1000000 metadata fixture")
    if not 1 <= args.memory_rows <= packed.DEFAULT_METADATA_MEMORY_ROWS:
        parser.error("choose a bounded metadata row limit")
    if bool(args.reference) != bool(args.reference_sha256) or pack_scope != bool(args.reference):
        parser.error("pack-write comparison requires the exact preserved source and SHA")
    if args.reference and (args.reference.is_symlink() or not args.reference.resolve().is_relative_to(
            (ROOT / "artifacts/benchmarks").resolve())):
        parser.error("the preserved local reference must be under private benchmark outputs")
    if args.d_cold_fixture and not pack_scope:
        parser.error("D scratch is restricted to the complete pack-write comparison")
    if not 0 <= args.stored_member_bytes <= 4 * 1024**2 or (args.stored_member_bytes and not pack_scope):
        parser.error("stored-format fixture requires pack-write scope and 0..4MiB per member")
    if args.files * args.stored_member_bytes > 512 * 1024**2:
        parser.error("the opaque pack-write fixture is bounded to 512MiB")
    cold_parent = None
    if args.d_cold_fixture:
        from stockagent.data_sync.cold_primary import _check_d_primary_mount
        _check_d_primary_mount(Path("/srv/stockagent-packed"))
        cold_parent = Path("/srv/stockagent-packed/.local-state")
        if cold_parent.resolve() != cold_parent or shutil.disk_usage(cold_parent).free < 32 * 1024**3:
            raise ValueError("private D fixture requires its real guarded path and free-space floor")
    if args.trial:
        measure = bootstrap_trial if args.bootstrap_only else metadata_trial if args.metadata_only else trial
        options = ({"pack_write_only": args.pack_write_only, "pack_buffer_only": args.pack_buffer_only,
                    "reference": args.reference,
                    "reference_sha": args.reference_sha256, "cold_parent": cold_parent,
                    "stored_member_bytes": args.stored_member_bytes}
                   if pack_scope else {})
        print(json.dumps(measure(args.files, args.trial, args.memory_rows, **options)))
        return 0
    if args.output is None or args.output.exists() or args.output.is_symlink():
        parser.error("a new output receipt is required")
    paths = ("scripts/benchmark_packed_metadata.py", "stockagent/data_sync/packed_snapshots.py",
             "stockagent/data_sync/packed_metadata.py", "stockagent/data_sync/dataset_migration.py")
    if args.reference:
        paths += (str(args.reference),)
    def identity():
        return {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in paths}
    code = identity()
    resource_limits = None
    native_owner = None
    if args.native_limits:
        from scripts.benchmark_lakehouse_source_wave import match_native_limits
        from scripts.verify_lakehouse_workflow_history import owner_identity
        resource_limits = match_native_limits()
        native_owner = owner_identity()
    results = []
    for variant in ("retained", "bounded", "bounded", "retained"):
        completed = subprocess.run([sys.executable, str(Path(__file__).resolve()), "--files", str(args.files),
                                    "--memory-rows", str(args.memory_rows),
                                    "--trial", variant,
                                    *(["--bootstrap-only"] if args.bootstrap_only else
                                      ["--metadata-only"] if args.metadata_only else
                                      ["--pack-write-only" if args.pack_write_only else "--pack-buffer-only",
                                       "--reference", str(args.reference),
                                       "--reference-sha256", args.reference_sha256,
                                       "--stored-member-bytes", str(args.stored_member_bytes),
                                       *(["--d-cold-fixture"] if args.d_cold_fixture else [])]
                                      if pack_scope else [])],
                                    capture_output=True, text=True, check=True)
        results.append(json.loads(completed.stdout))
        print(json.dumps({"trial": len(results), "variant": variant,
                          "complete_wall_seconds": results[-1]["complete_wall_seconds"],
                          "peak_rss_bytes": results[-1]["peak_rss_bytes"]}), flush=True)
    means = {variant: {metric: statistics.mean(row[metric] for row in results if row["variant"] == variant)
                      for metric in ("complete_wall_seconds", "complete_cpu_seconds", "peak_rss_bytes")}
             for variant in ("retained", "bounded")}
    parity_field = "original_tree_sha256" if args.pack_write_only else "inventory_sha256"
    inventories = ([row["inventory_sha256"] for row in results] if metadata_scope else
                   [[phase[parity_field] for phase in row["phases"] if parity_field in phase]
                    for row in results])
    if any(value != inventories[0] for value in inventories) or identity() != code:
        raise ValueError("cross-trial inventory or implementation parity failed")
    if args.native_limits:
        from scripts.benchmark_lakehouse_source_signature import limits
        if resource_limits != limits() or native_owner != owner_identity():
            raise ValueError("native owner or benchmark resource limits changed")
    result = {"contract": ("packed_seekable_buffer_complete_pipeline_abba_v1" if args.pack_buffer_only else
                           "packed_sequential_write_complete_pipeline_abba_v1" if args.pack_write_only else
                           "packed_bootstrap_metadata_abba_v1" if args.bootstrap_only else
                           "packed_metadata_index_abba_v1" if args.metadata_only else
                           "packed_metadata_complete_pipeline_abba_v1"), "state": "verified",
              "observed_at_utc": datetime.now(UTC).isoformat(), "fixture_items": args.files,
              "scope": ("bootstrap_metadata_only" if args.bootstrap_only else
                        "metadata_only" if args.metadata_only else "complete_publish_recover_materialize"),
              "reference": ("preserved canonical seekable ZIP writer; identical remaining workflow" if pack_scope else
                            "retained donor list plus path dict (not old binary replay)" if args.bootstrap_only else
                            "retained metadata; serial fsync; new-small-file prehash (not old binary replay)"),
              "trials": results, "means": means, "code_sha256": code,
              "inventory_parity": not args.pack_write_only, "inventory_and_original_byte_parity": not metadata_scope and not args.pack_write_only,
              "all_original_bytes_parity": not metadata_scope,
              "native_resource_limits": resource_limits,
              "reference_sha256": args.reference_sha256,
              "private_D_scratch_used": args.d_cold_fixture,
              "pack_encoding_changed": args.pack_write_only,
              "stored_format_fixture_bytes_per_member": args.stored_member_bytes,
              "production_migration_claim": False,
              "provider_calls": 0, "production_source_writes": 0, "production_cold_writes": 0}
    atomic_write_json(args.output, result)
    print(json.dumps({"state": result["state"], "means": means}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
