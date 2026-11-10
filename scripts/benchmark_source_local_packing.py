#!/usr/bin/env python3
"""Compare source-local and legacy packs on a bounded, private E-workspace sample.

No source files, releases, heads, pins or production backups are changed. The
temporary copies and restores are removed only from the directory created by
this process. The receipt describes a sample, never a dataset acceptance.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import stat
import sys
import tempfile
import time
import zipfile

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from stockagent.data_sync import packed_snapshots as packed, workspace_volume  # noqa: E402


def _stable_copy(source: Path, destination: Path) -> tuple[str, int]:
    descriptor = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or not os.path.samestat(before, source.lstat()):
            raise ValueError("sample source is not the selected regular inode")
        digest = hashlib.sha256()
        destination.parent.mkdir(parents=True, exist_ok=True)
        with os.fdopen(descriptor, "rb", closefd=False) as reader, destination.open("xb") as writer:
            while block := reader.read(1024 * 1024):
                digest.update(block)
                writer.write(block)
            writer.flush()
            os.fsync(writer.fileno())
        after = os.fstat(descriptor)
        current = source.lstat()
        def identity(value):
            return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns
        if identity(before) != identity(after) or identity(after) != identity(current):
            raise ValueError("sample source changed while being copied")
        return digest.hexdigest(), before.st_size
    finally:
        os.close(descriptor)


def _sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _sample(source: Path, target: Path, directories: list[str], files_per_dir: int,
            bytes_per_dir: int, extensions: tuple[str, ...]) -> dict[str, dict[str, object]]:
    rows = {}
    for name in directories:
        if not name or Path(name).name != name or name in {".", ".."}:
            raise ValueError("sample directory must be one direct name")
        directory = source / name
        if directory.is_symlink() or not directory.is_dir() or directory.resolve() != directory:
            raise ValueError("sample directory is missing or redirected")
        count = total = 0
        with os.scandir(directory) as scanner:
            candidates = sorted((e.name for e in scanner if e.name.lower().endswith(extensions)))
        for filename in candidates:
            path = directory / filename
            info = path.lstat()
            if not stat.S_ISREG(info.st_mode) or not 0 < info.st_size <= bytes_per_dir:
                continue
            if total + info.st_size > bytes_per_dir:
                continue
            relative = f"{name}/{filename}"
            digest, size = _stable_copy(path, target / relative)
            rows[relative] = {"sha256": digest, "bytes": size}
            count += 1
            total += size
            if count >= files_per_dir:
                break
        if not count:
            raise ValueError(f"no bounded sample files in {name}")
    return rows


def benchmark(source: Path, work_root: Path, directories: list[str], files_per_dir: int,
              bytes_per_dir: int, extensions: tuple[str, ...], compare_large_files: bool) -> dict:
    source = source.absolute()
    work_root = work_root.absolute()
    if (source.is_symlink() or source.resolve(strict=True) != source or not source.is_dir()
            or work_root.is_symlink() or work_root.resolve(strict=True) != work_root or not work_root.is_dir()
            or source.is_relative_to(work_root) or work_root.is_relative_to(source)):
        raise ValueError("source and scratch must be distinct real directories")
    config = workspace_volume.configuration()
    capacity = workspace_volume.admit(8 * bytes_per_dir * len(directories), c=config)
    if not work_root.is_relative_to(Path(config["mount_point"])):
        raise ValueError("the pilot must remain inside the enrolled E workspace")
    with tempfile.TemporaryDirectory(prefix="source-local-pilot-", dir=work_root) as scratch:
        scratch = Path(scratch)
        staged = scratch / "sample"
        rows = _sample(source, staged, directories, files_per_dir, bytes_per_dir, extensions)
        results = []
        for index, layout in enumerate(("path-hash-v1", "source-directory-v1",
                                        "source-directory-v1", "path-hash-v1")):
            cold = scratch / f"cold-{index}"
            usage = resource.getrusage(resource.RUSAGE_SELF)
            started = time.perf_counter()
            options = {"pack_layout": layout}
            if layout == "source-directory-v1":
                options["pack_target_bytes"] = 512 * 1024**2
            if compare_large_files:
                options["file_chunk_bytes"] = 8 * 1024**2
                options["large_file_layout"] = ("whole-file-v1" if layout == "source-directory-v1"
                                                else "chunked-v1")
            result = packed.publish_packed_snapshot(cold, "sample", staged, node_id="pilot",
                pack_buckets=64, loose_file_threshold_bytes=8 * 1024**2, **options)
            build_seconds = time.perf_counter() - started
            started = time.perf_counter()
            proof = packed.verify_packed_snapshot(cold, result, reconstruct_all=True)
            verify_seconds = time.perf_counter() - started
            started = time.perf_counter()
            restored = packed.fetch_packed_snapshot(cold, scratch / f"restore-{index}", result)
            for name, row in rows.items():
                path = restored / name
                if path.stat().st_size != row["bytes"] or _sha(path) != row["sha256"]:
                    raise ValueError("restored sample differs from the sealed original")
            restore_seconds = time.perf_counter() - started
            subtree_seconds = {}
            for directory in directories:
                started = time.perf_counter()
                subtree = packed.fetch_packed_subtree(cold, scratch / f"subtree-{index}-{directory}",
                                                      result, directory)
                for name, row in rows.items():
                    if name.startswith(directory + "/"):
                        path = subtree / name[len(directory) + 1:]
                        if path.stat().st_size != row["bytes"] or _sha(path) != row["sha256"]:
                            raise ValueError("restored subtree differs from the sealed original")
                subtree_seconds[directory] = time.perf_counter() - started
            incremental = None
            if compare_large_files:
                candidates = [name for name, row in rows.items() if row["bytes"] > 16 * 1024**2]
                if not candidates:
                    raise ValueError("large-file comparison requires a sample above 16 MiB")
                changed = staged / sorted(candidates)[0]
                offset = changed.stat().st_size // 2
                with changed.open("r+b") as stream:
                    stream.seek(offset)
                    original = stream.read(1)
                    stream.seek(offset)
                    stream.write(bytes([original[0] ^ 1]))
                    stream.flush()
                    os.fsync(stream.fileno())
                started = time.perf_counter()
                updated = packed.publish_packed_snapshot(cold, "sample", staged, node_id="pilot",
                    pack_buckets=64, loose_file_threshold_bytes=8 * 1024**2)
                update_seconds = time.perf_counter() - started
                update_proof = packed.verify_packed_snapshot(cold, updated, reconstruct_all=True)
                incremental = {"changed_file_bytes": changed.stat().st_size,
                    "new_stored_bytes": updated.manifest["archive"]["new_stored_bytes"],
                    "changed_files": updated.manifest["archive"]["changed_files"],
                    "build_seconds": update_seconds,
                    "reconstructed_files": update_proof["independently_reconstructed_files"]}
                with changed.open("r+b") as stream:
                    stream.seek(offset)
                    stream.write(original)
                    stream.flush()
                    os.fsync(stream.fileno())
                if _sha(changed) != rows[str(changed.relative_to(staged))]["sha256"]:
                    raise ValueError("pilot did not restore its private sample to the sealed bytes")
            groups = {}
            mixed = pack_count = 0
            for obj in result.manifest["archive"]["objects"]:
                if obj["kind"] != "pack":
                    continue
                pack_count += 1
                with zipfile.ZipFile(cold / obj["relpath"]) as archive:
                    origins = {packed._source_pack_directory(name) for name in archive.namelist()}
                mixed += len(origins) > 1
                for name in origins:
                    groups[name] = groups.get(name, 0) + 1
            current_usage = resource.getrusage(resource.RUSAGE_SELF)
            results.append({"layout": layout, "build_seconds": build_seconds,
                "verify_seconds": verify_seconds, "restore_and_hash_seconds": restore_seconds,
                "subtree_restore_and_hash_seconds": subtree_seconds,
                "cpu_seconds": current_usage.ru_utime + current_usage.ru_stime - usage.ru_utime - usage.ru_stime,
                "pack_count": pack_count, "mixed_directory_packs": mixed,
                "large_file_layout": options.get("large_file_layout"),
                "one_byte_incremental": incremental,
                "object_count": result.manifest["archive"]["object_count"],
                "packs_touched_by_directory": groups, "stored_bytes": result.manifest["archive"]["stored_bytes"],
                "reconstructed_files": proof["independently_reconstructed_files"]})
        return {"contract": "stockagent.source_local_pack_pilot.v1", "state": "sample_verified",
            "sample_files": len(rows), "sample_bytes": sum(row["bytes"] for row in rows.values()),
            "sample_directories": directories, "runs": results, "ab_order": "ABBA",
            "original_sources_modified": False, "production_heads_modified": False,
            "D_or_NAS_acceptance": False, "capacity_admission": capacity,
            "observed_at_epoch": time.time()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--work-root", required=True, type=Path)
    parser.add_argument("--directory", action="append", required=True)
    parser.add_argument("--files-per-directory", type=int, default=200)
    parser.add_argument("--mib-per-directory", type=int, default=32)
    parser.add_argument("--extension", action="append", help="sample file suffix; defaults to .parquet")
    parser.add_argument("--compare-large-files", action="store_true",
                        help="also compare new 8 MiB chunks with full-file blobs")
    parser.add_argument("--receipt", required=True, type=Path)
    args = parser.parse_args()
    if not 1 <= args.files_per_directory <= 10000 or not 1 <= args.mib_per_directory <= 256:
        parser.error("sample bounds must be positive and finite")
    if args.receipt.exists() or args.receipt.is_symlink():
        parser.error("preserve existing pilot receipts")
    extensions = tuple(args.extension or [".parquet"])
    if any(not e.startswith(".") or "/" in e or len(e) > 16 for e in extensions):
        parser.error("invalid sample extension")
    result = benchmark(args.source, args.work_root, args.directory, args.files_per_directory,
                       args.mib_per_directory * 1024**2, extensions, args.compare_large_files)
    with args.receipt.open("x", encoding="utf-8") as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    print(json.dumps(result, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    os.umask(0o077)
    raise SystemExit(main())
