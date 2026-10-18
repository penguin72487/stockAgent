"""Bounded base/delta layout for the existing verified history projection.

This changes disposable column storage, not the canonical JSONL ledger. Every
changed source still proves its complete retained prefix. At most two immutable
Parquet objects are referenced: an unchanged base and a small replaceable delta.
The original per-source lease remains held through validation and query.
"""
from __future__ import annotations

import io
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any

import polars as pl

from downloader.artifact_io import durable_replace
from stockagent.live import taifex_history_column_cache as cache

CONTRACT = "taifex_history_base_delta_columns_v2"
LAYOUT_ENV = cache.LAYOUT_ENV
MAX_TAIL_SOURCE_BYTES = 32 * 1024**2
_PART_NAME = re.compile(r"part-[0-9a-f]{64}\.parquet\Z")


def _columns(schema: dict[str, Any]) -> list[list[str]]:
    return [[name, str(dtype)] for name, dtype in schema.items()]


def _load_manifest(path: Path, source: Path, schema: dict[str, Any]) -> dict:
    try:
        fd = cache._open_regular(path)
        try:
            if os.fstat(fd).st_size > 64 * 1024:
                return {}
            value = json.loads(os.read(fd, 64 * 1024 + 1))
        finally:
            os.close(fd)
        if (not isinstance(value, dict) or value.get("contract") != CONTRACT
                or value.get("source") != str(source)
                or value.get("polars") != pl.__version__
                or value.get("columns") != _columns(schema)):
            return {}
        generation, parts = value.get("source_generation"), value.get("parts")
        if (not isinstance(generation, list) or len(generation) != 5
                or any(type(n) is not int or n < 0 for n in generation)
                or not isinstance(parts, list) or not 1 <= len(parts) <= 2
                or type(value.get("rows")) is not int or value["rows"] < 0
                or not re.fullmatch(r"[0-9a-f]{64}", str(value.get("source_sha256")))):
            return {}
        offset = rows = 0
        for part in parts:
            if (not isinstance(part, dict)
                    or not _PART_NAME.fullmatch(str(part.get("name")))
                    or part["name"] != f"part-{part.get('sha256')}.parquet"
                    or any(type(part.get(key)) is not int for key in ("start", "end", "rows"))
                    or part["start"] != offset or part["end"] <= offset or part["rows"] < 0):
                return {}
            offset, rows = part["end"], rows + part["rows"]
        return value if offset == generation[2] and rows == value["rows"] else {}
    except (OSError, ValueError, TypeError):
        return {}


def _pin_parts(root: Path, parts: list[dict]) -> list[int]:
    descriptors = []
    try:
        for part in parts:
            path = root / part["name"]
            fd = cache._open_regular(path)
            descriptors.append(fd)
            generation = cache._generation(os.fstat(fd))
            if (not cache._verified_payload(str(path), generation, part["sha256"])
                    or cache._generation(os.fstat(fd)) != generation
                    or cache._generation(path.stat()) != generation):
                raise ValueError("history segment checksum or generation mismatch")
        return descriptors
    except BaseException:
        for fd in descriptors:
            os.close(fd)
        raise


def _scan(descriptors: list[int]) -> pl.LazyFrame:
    # Ordered vertical concatenation preserves the canonical parser's row order,
    # including equal timestamps. Parallel global ordering is not inferred.
    return pl.concat([pl.scan_parquet(f"/proc/self/fd/{fd}") for fd in descriptors],
                     how="vertical", parallel=False)


def _legacy_base(source: Path, payload: Path, receipt: Path,
                 root: Path, schema: dict[str, Any]) -> dict:
    """Adopt an independently verified v1 inode without copying its payload."""
    old = cache._receipt(receipt, schema)
    if not old or old.get("source") != str(source):
        return {}
    fd = cache._open_regular(payload)
    try:
        generation = cache._generation(os.fstat(fd))
        if not cache._verified_payload(str(payload), generation, old["payload_sha256"]):
            return {}
        part = {"name": f"part-{old['payload_sha256']}.parquet",
                "sha256": old["payload_sha256"], "start": 0,
                "end": old["source_generation"][2], "rows": old["rows"]}
        target = root / part["name"]
        try:
            os.link(payload, target, follow_symlinks=False)
        except FileExistsError:
            pass  # The regular-file/checksum proof below still must pass.
        pinned = _pin_parts(root, [part])
        try:
            if (os.fstat(pinned[0]).st_dev, os.fstat(pinned[0]).st_ino) != generation[:2]:
                # An existing equal, separately verified content object is fine.
                if cache._prefix_digest(pinned[0], os.fstat(pinned[0]).st_size) != part["sha256"]:
                    return {}
        finally:
            for item in pinned:
                os.close(item)
        return {**old, "contract": CONTRACT, "parts": [part]}
    finally:
        os.close(fd)


def _write_part(frame: pl.LazyFrame, root: Path, schema: dict[str, Any],
                start: int, end: int, created: list[Path]) -> dict:
    fd, name = tempfile.mkstemp(prefix=".history-segment-", suffix=".parquet", dir=root)
    os.close(fd)
    temporary = Path(name)
    try:
        frame.sink_parquet(temporary, compression="zstd", compression_level=3,
                          row_group_size=8192, maintain_order=True, engine="streaming")
        check = pl.scan_parquet(temporary)
        if dict(check.collect_schema()) != schema:
            raise ValueError("history segment schema differs from canonical parser")
        rows = check.select(pl.len()).collect().item()
        fd = cache._open_regular(temporary)
        try:
            digest = cache._prefix_digest(fd, os.fstat(fd).st_size)
        finally:
            os.close(fd)
        target = root / f"part-{digest}.parquet"
        if not target.exists() and not target.is_symlink():
            created.append(target)
        durable_replace(temporary, target)
        return {"name": target.name, "sha256": digest, "start": start, "end": end, "rows": rows}
    finally:
        temporary.unlink(missing_ok=True)  # Only this call's unpublished cache file.


def _fence(source: Path, descriptor: int, generation: tuple[int, ...]) -> None:
    if (cache._generation(os.fstat(descriptor)) != generation
            or cache._generation(source.stat()) != generation):
        raise cache._SourceGenerationChanged("history source changed during segment proof/build")


def prepare(source: Path, payload: Path, receipt: Path,
            schema: dict[str, Any]) -> list[int] | None:
    """Return pinned verified base/delta FDs; caller owns the existing v1 lease.

    Generation changes still cost a complete source SHA256. This layout removes
    historical Parquet rewrite amplification; it does not waive prefix integrity.
    """
    source_fd = cache._open_regular(source)
    descriptors: list[int] = []
    created: list[Path] = []
    publication_attempted = False
    root = payload.parent / f"{payload.stem}.segments-v2"
    try:
        generation = cache._generation(os.fstat(source_fd))
        size = generation[2]
        if size < cache.MIN_SOURCE_BYTES or os.pread(source_fd, 1, size - 1) != b"\n":
            return None
        root.mkdir(mode=0o700, exist_ok=True)
        if root.is_symlink():
            raise ValueError("history segment directory cannot be a symlink")
        os.chmod(root, 0o700)
        manifest = root / "receipt.json"
        old = _load_manifest(manifest, source, schema)
        adopted = not old
        if not old:
            try:
                old = _legacy_base(source, payload, receipt, root, schema)
            except (OSError, ValueError):
                old = {}
        if old:
            try:
                descriptors = _pin_parts(root, old["parts"])
            except (OSError, ValueError):
                old = {}
        if old and old["source_generation"] == list(generation):
            if cache._verified_payload(str(source), generation, old["source_sha256"]):
                _fence(source, source_fd, generation)
                if adopted:
                    cache._publish_receipt(manifest, old)
                result, descriptors = descriptors, []
                return result

        previous_size = old["source_generation"][2] if old else 0
        incremental = bool(old and 0 < previous_size <= size
                           and size - previous_size <= cache.MAX_DELTA_BYTES)
        prefix_sha, source_sha = cache._source_digests(
            source_fd, size, previous_size if incremental else 0)
        incremental = incremental and prefix_sha == old["source_sha256"]
        _fence(source, source_fd, generation)
        if incremental and previous_size == size:
            parts = old["parts"]
        elif incremental:
            delta = os.pread(source_fd, size - previous_size, previous_size)
            if len(delta) != size - previous_size:
                raise cache._SourceGenerationChanged("history delta truncated during proof")
            tail = (pl.scan_ndjson(io.BytesIO(delta), schema=schema, low_memory=True)
                    if delta.strip() else pl.DataFrame(schema=schema).lazy())
            base_end = old["parts"][0]["end"]
            if size - base_end <= MAX_TAIL_SOURCE_BYTES:
                frames = [_scan(descriptors[1:]), tail] if len(descriptors) == 2 else [tail]
                part = _write_part(pl.concat(frames, parallel=False), root, schema, base_end, size, created)
                parts = [old["parts"][0], part]
            else:
                parts = [_write_part(pl.concat([_scan(descriptors), tail], parallel=False),
                                     root, schema, 0, size, created)]
        else:
            frame = pl.scan_ndjson(f"/proc/self/fd/{source_fd}", schema=schema, low_memory=True)
            parts = [_write_part(frame, root, schema, 0, size, created)]
        _fence(source, source_fd, generation)
        value = {"contract": CONTRACT, "source": str(source), "polars": pl.__version__,
                 "columns": _columns(schema), "source_generation": list(generation),
                 "source_sha256": source_sha, "parts": parts,
                 "rows": sum(part["rows"] for part in parts)}
        # Independent payload validation follows publication, just as in v1.
        # The original source proof is reusable only after both fences passed.
        result = _pin_parts(root, parts)
        try:
            publication_attempted = True
            cache._publish_receipt(manifest, value)
        except BaseException:
            for fd in result:
                os.close(fd)
            raise
        cache._remember_verified_payload(str(source), generation, source_sha)
        # Only objects named by the last verified receipt can be retired here.
        # The lease is still held; queries pin their inode, v1/source are untouched.
        current_names = {part["name"] for part in parts}
        for part in old.get("parts", []):
            if part["name"] not in current_names:
                try:
                    (root / part["name"]).unlink(missing_ok=True)
                except OSError:
                    pass  # Cleanup failure must not invalidate the committed projection.
        return result
    finally:
        os.close(source_fd)
        for fd in descriptors:
            os.close(fd)
        if not publication_attempted:
            for path in created:
                path.unlink(missing_ok=True)  # This attempt's new, uncommitted cache objects only.
