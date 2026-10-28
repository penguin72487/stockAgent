"""Reader-first Parquet ABI: an explicit manifest, not a directory/glob alias.

Legacy files retain their byte identity. A manifest pins an ordered set of
immutable, byte-verified period/row members and the logical Arrow schema.
No directory discovery, deduplication, filling, sorting or clock conversion is
performed while reading. Old generations and failed writes remain recoverable.
"""
from __future__ import annotations

import base64
from datetime import date
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import re
import time
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

from stockagent.data_sync.immutable_cache_files import payload_info, stable_payload_hash

CONTRACT = "ordered_partitioned_parquet_v1"
ROW_PARTS_CONTRACT = "ordered_row_parts_parquet_v1"
MANIFEST_SUFFIX = ".manifest.json"
MAX_MANIFEST_BYTES = 16 * 1024**2
MAX_MEMBERS = 4096


def _manifest_location(path: Path):
    if path.name.endswith(MANIFEST_SUFFIX):
        return path.parent, path.name + ".parts", None
    match = re.fullmatch(r"generation_([0-9a-f]{64})\.json", path.name)
    if match and path.parent.name.endswith(MANIFEST_SUFFIX + ".parts"):
        return path.parent.parent, path.parent.name, match[1]
    return None


def is_partition_manifest(path: str | Path) -> bool:
    """Recognize only the explicit live pointer or its sealed generation ABI."""
    return _manifest_location(Path(path)) is not None


def implementation_identity():
    return {Path(__file__).name: hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "arrow": pa.__version__}


def _digest(payload):
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def manifest_path(path: str | Path) -> Path:
    path = Path(path)
    return path if path.name.endswith(MANIFEST_SUFFIX) else path.with_name(path.name + MANIFEST_SUFFIX)


def resolve_source(path: str | Path) -> Path:
    """Select exactly one ABI. Never prefer stale legacy bytes over a manifest."""
    path = Path(path)
    if path.name.endswith(MANIFEST_SUFFIX):
        legacy = path.with_name(path.name.removesuffix(MANIFEST_SUFFIX))
        if legacy.exists():
            raise ValueError(f"ambiguous legacy/partitioned source: {legacy}")
        return path
    alternative = manifest_path(path)
    if alternative.exists():
        if path.exists():
            raise ValueError(f"ambiguous legacy/partitioned source: {path}")
        return alternative
    return path


def source_exists(path: str | Path) -> bool:
    return resolve_source(path).is_file()


def discover_sources(root: str | Path, pattern: str = "*_features.parquet") -> list[Path]:
    root = Path(root)
    return sorted(resolve_source(path) for path in (
        *root.glob(pattern),
        *(path.with_name(path.name.removesuffix(MANIFEST_SUFFIX))
          for path in root.glob(pattern + MANIFEST_SUFFIX)),
    ))


def logical_name(path: str | Path) -> str:
    return Path(path).name.removesuffix(MANIFEST_SUFFIX)


class ParquetSource:
    """One same-run snapshot; verify again after lazy consumption/publication.

    All members are checked, including those outside a requested date range.
    Filtering can prune decoding, never the source-completeness proof. The
    mutable manifest pointer is fenced as well, so no mixed generation passes.
    """

    def __init__(self, path: str | Path):
        self.path = resolve_source(path).resolve(strict=True)
        self.before = payload_info(self.path)
        self.manifest = None
        self.byte_sha256 = None
        self.members = []
        self.signatures = []
        self._metadata = []
        location = _manifest_location(self.path)
        if location is None:
            with pq.ParquetFile(self.path) as parquet:
                self.schema = parquet.schema_arrow
                self.rows = parquet.metadata.num_rows
                self._metadata.append(parquet.metadata)
            self.members = [self.path]
            self.signatures = [self.before]
            return
        with self.path.open("rb") as stream:
            raw = stream.read(MAX_MANIFEST_BYTES + 1)
        if len(raw) > MAX_MANIFEST_BYTES:
            raise ValueError("partition manifest byte bound")
        manifest = json.loads(raw)
        self.byte_sha256 = hashlib.sha256(raw).hexdigest()
        if (not isinstance(manifest, dict) or manifest.get("contract") not in {CONTRACT, ROW_PARTS_CONTRACT}
                or manifest.get("manifest_sha256") != _digest(
                    {k: v for k, v in manifest.items() if k != "manifest_sha256"})):
            raise ValueError("partition manifest contract/checksum mismatch")
        proofs = manifest.get("members")
        if (not isinstance(proofs, list) or len(proofs) > MAX_MEMBERS
                or type(manifest.get("rows")) is not int or manifest["rows"] < 0
                or (manifest.get("contract") == CONTRACT and (manifest.get("period") not in {"year", "month"}
                    or not isinstance(manifest.get("date_column"), str)))
                or (manifest.get("contract") == ROW_PARTS_CONTRACT and (manifest.get("period") != "rows"
                    or manifest.get("date_column") is not None
                    or type(manifest.get("partition_rows")) is not int
                    or not 1 <= manifest["partition_rows"] <= 1024**2))):
            raise ValueError("partition manifest inventory/clock mismatch")
        base, namespace, archived_digest = location
        if archived_digest is not None and (manifest["manifest_sha256"] != archived_digest
                                            or self.before["mode"] & 0o222):
            raise ValueError("immutable generation path/content mismatch")
        self.schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(manifest["schema"], validate=True)))
        row_parts = manifest["contract"] == ROW_PARTS_CONTRACT
        if not row_parts and not pa.types.is_date32(self.schema.field(manifest["date_column"]).type):
            raise ValueError("partition ABI requires an explicit Date32 observation clock")
        previous_key = ""
        total = 0
        for proof in proofs:
            key = proof["key"]
            pattern = (r"[0-9]{20}" if row_parts else r"[0-9]{4}" if manifest["period"] == "year"
                       else r"[0-9]{4}-(?:0[1-9]|1[0-2])")
            if (not isinstance(key, str) or not re.fullmatch(pattern, key) or key <= previous_key
                    or type(proof.get("rows")) is not int or proof["rows"] <= 0
                    or not re.fullmatch(r"[a-f0-9]{64}", str(proof.get("values_sha256", "")))
                    or (not row_parts and (not isinstance(proof.get("min_date"), str)
                        or not isinstance(proof.get("max_date"), str)
                        or not proof["min_date"].startswith(key) or not proof["max_date"].startswith(key)
                        or proof["min_date"] > proof["max_date"]))
                    or (row_parts and (key != f"{total:020d}" or proof.get("row_start") != total
                        or type(proof.get("row_start")) is not int
                        or proof["rows"] != min(manifest["partition_rows"], manifest["rows"]-total)))):
                raise ValueError("partition order, rows or date bounds mismatch")
            name = proof["path"]
            if (not isinstance(name, str) or not re.fullmatch(
                    re.escape(namespace) + r"/p_[a-f0-9]{64}_[a-f0-9]{32}\.parquet", name)):
                raise ValueError("partition member escaped its explicit namespace")
            member = base / name
            if member.resolve(strict=True) != member:
                raise ValueError("redirected partition member")
            before = payload_info(member)
            if (before["mode"] & 0o222 or before["uid"] != os.geteuid()
                    or before["size"] != proof["bytes"]
                    or stable_payload_hash(member, before) != proof["sha256"]):
                raise ValueError("partition member content proof mismatch")
            with pq.ParquetFile(member) as parquet:
                if (parquet.metadata.num_rows != proof["rows"]
                        or not parquet.schema_arrow.remove_metadata().equals(self.schema.remove_metadata())):
                    raise ValueError("partition member schema/row mismatch")
                self._metadata.append(parquet.metadata)
            self.members.append(member)
            self.signatures.append(before)
            total += proof["rows"]
            previous_key = key
        if total != manifest["rows"]:
            raise ValueError("partition row accounting mismatch")
        self.rows = total
        self.manifest = manifest
        self.verify()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        if exc_type is None:
            self.verify()

    def verify(self):
        if payload_info(self.path) != self.before:
            raise ValueError(f"Parquet source pointer changed during read: {self.path}")
        for path, before in zip(self.members, self.signatures, strict=True):
            if payload_info(path) != before:
                raise ValueError(f"Parquet source member changed during read: {path}")

    @property
    def member_footers(self):
        """Parsed metadata bound to this snapshot, in explicit member order.

        Consumers stay inside this source's context and retain its complete
        before/after member checks. Pass these footers to ParquetFile instead
        of parsing the same metadata a second time for a column projection.
        """
        return tuple(self._metadata)

    def table(self, *, columns=None, filters=None, tail_rows=None):
        self.verify()
        selected_schema = self.schema if columns is None else pa.schema(
            [self.schema.field(name) for name in columns], metadata=self.schema.metadata)
        members = list(zip(self.members, self._metadata, strict=True))
        if tail_rows is not None:
            if type(tail_rows) is not int or tail_rows <= 0 or filters:
                raise ValueError("bounded partition tail requires positive rows and no filters")
            if self.manifest is not None:
                count, begin = 0, len(members)
                for proof in reversed(self.manifest["members"]):
                    begin -= 1
                    count += proof["rows"]
                    if count >= tail_rows:
                        break
                members = members[begin:]
        # The Dataset factory is useful for predicate pushdown, but adds a
        # scanner/thread-pool per tiny BASE member when no filter is needed.
        tables = []
        for path, metadata in members:
            if filters or self.manifest is None:
                # Preserve Dataset predicate/legacy partition discovery semantics.
                tables.append(pq.read_table(path, columns=columns, filters=filters, memory_map=True))
            else:
                # This snapshot already verified the complete member SHA and
                # footer. Reuse only its parsed metadata, never an open handle
                # or a proof from another snapshot. Both stat fences remain.
                with pq.ParquetFile(path, memory_map=True, metadata=metadata) as parquet:
                    tables.append(parquet.read(columns=columns, use_threads=False))
        result = (pa.concat_tables(tables).replace_schema_metadata(selected_schema.metadata)
                  if tables else pa.Table.from_batches([], schema=selected_schema))
        if tail_rows is not None and result.num_rows > tail_rows:
            result = result.slice(result.num_rows - tail_rows)
        self.verify()
        return result

    def scan(self):
        """A lazy query must be collected within this snapshot's context."""
        import polars as pl
        self.verify()
        if not self.members:
            return pl.from_arrow(pa.Table.from_batches([], schema=self.schema)).lazy()
        return pl.concat([pl.scan_parquet(path, glob=False) for path in self.members], how="vertical")

    def source_paths(self):
        return [self.path, *self.members] if self.manifest is not None else [self.path]

    def fingerprints(self):
        """Reuse full byte proofs only within this verified same-run snapshot."""
        if self.manifest is None:
            raise ValueError("legacy byte proof remains owned by the original caller")
        result = [(str(self.path), self.before["size"], self.byte_sha256)]
        result.extend((str(path), proof["bytes"], proof["sha256"])
            for path, proof in zip(self.members, self.manifest["members"], strict=True))
        self.verify()
        return result


def read_table(path, *, columns=None, filters=None, tail_rows=None, memory_map=True):
    with ParquetSource(path) as source:
        return source.table(columns=columns, filters=filters, tail_rows=tail_rows)


def read_schema(path):
    with ParquetSource(path) as source:
        return source.schema


def source_paths(paths):
    """Expand explicit members for existing full-byte publication/cache owners."""
    result = []
    for path in paths:
        path = resolve_source(path)
        if path.name.endswith(MANIFEST_SUFFIX):
            with ParquetSource(path) as source:
                result.extend(source.source_paths())
        else:
            result.append(path)
    return sorted(result)


def write_partitioned_table(table: pa.Table, path: str | Path, *, period="year",
                            date_column="date", source_identity: dict,
                            replace_from: date | None = None, write_budget=None,
                            previous_source: ParquetSource | None = None,
                            sync_directory: bool = True, partition_rows: int | None = None,
                            parquet_options: dict | None = None):
    """Publish only changed partitions under a single writer lease.

    ``replace_from`` is a producer-certified boundary, not a heuristic: the
    caller has proved all prior input/session semantics unchanged. Only the
    overlapping month/year is decoded and joined to the replacement suffix.
    Corrections without such a proof supply the complete canonical table.
    Each new member is independently typed-roundtrip checked before commit.
    ``period="rows"`` partitions the supplied order without interpreting an
    observation clock. It requires a fixed ``partition_rows``, no date column
    and a complete canonical table; insertions/corrections are never inferred.
    """
    from contextlib import ExitStack
    import pyarrow.compute as pc
    from downloader.artifact_io import atomic_write_json, sha256_file
    from stockagent.data.tw_public_incremental import table_content_sha256
    from stockagent.data_sync.cache_capacity import (
        GenerationWriteBudget, load_capacity_policy, reserve_cache_write,
    )
    from stockagent.data_sync.factorized_projection_reuse import seal_new_file
    from stockagent.data_sync.desync_snapshots import _fsync_directory

    started = time.perf_counter()
    phases = {}
    path = manifest_path(path).absolute()
    legacy = path.with_name(path.name.removesuffix(MANIFEST_SUFFIX))
    if legacy.exists() or path.parent.resolve() != path.parent:
        raise ValueError("partition writer requires a separate, unambiguous non-redirected output root")
    row_parts = period == "rows"
    if (period not in {"year", "month", "rows"}
            or (row_parts and (date_column is not None or type(partition_rows) is not int
                or not 1 <= partition_rows <= 1024**2 or replace_from is not None))
            or (not row_parts and (partition_rows is not None
                or not pa.types.is_date32(table.schema.field(date_column).type)))):
        raise ValueError("partition writer requires a Date32 year/month clock or explicit ordered rows")
    if not isinstance(parquet_options, (dict, type(None))):
        raise ValueError("unsupported explicit partition Parquet encoding")
    options = {"compression": "snappy", "row_group_size": 64_000, **(parquet_options or {})}
    if (set(options) - {"compression", "row_group_size", "use_dictionary", "use_byte_stream_split"}
            or options["compression"] not in ("snappy", "zstd", None)
            or type(options["row_group_size"]) is not int or not 1 <= options["row_group_size"] <= 1024**2
            or any(type(options[name]) is not bool for name in ("use_dictionary", "use_byte_stream_split") if name in options)):
        raise ValueError("unsupported explicit partition Parquet encoding")
    if options.get("use_byte_stream_split"):
        options["use_byte_stream_split"] = [f.name for f in table.schema if pa.types.is_floating(f.type)]
    if not isinstance(source_identity, dict) or not source_identity:
        raise ValueError("partition writer requires the canonical producer's source identity")
    if replace_from is not None and type(replace_from) is not date:
        raise ValueError("replacement boundary must be an explicit observation date")
    dates = None if row_parts else table[date_column].combine_chunks()
    if dates is not None and (dates.null_count or (len(dates) > 1
            and pc.any(pc.less(dates.slice(1), dates.slice(0, len(dates)-1))).as_py())):
        raise ValueError("partition writer cannot sort, fill or reinterpret source dates")
    if replace_from is not None and len(dates) and pc.min(dates).as_py() < replace_from:
        raise ValueError("replacement suffix precedes its certified boundary")
    implementation = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    namespace = path.parent / (path.name + ".parts")
    path.parent.mkdir(parents=True, exist_ok=True)
    namespace.mkdir(exist_ok=True)
    if namespace.resolve() != namespace:
        raise ValueError("redirected partition namespace")
    # Only a control inode exists before capacity admission; old/failed members
    # are never removed to manufacture headroom.
    with (namespace / "writer.lock").open("a+b") as lock, ExitStack() as leases:
        fcntl.flock(lock, fcntl.LOCK_EX)
        previous = previous_source
        if previous is not None:
            if previous.path != path or previous.manifest is None:
                raise ValueError("partition proof belongs to another logical source")
            previous.verify()
        else:
            previous = ParquetSource(path) if path.is_file() else None
        if previous is not None and (previous.manifest["period"] != period
                or previous.manifest["date_column"] != date_column
                or (row_parts and previous.manifest.get("partition_rows") != partition_rows)
                or not previous.schema.remove_metadata().equals(table.schema.remove_metadata())):
            raise ValueError("partition layout/schema ABI changed; a fresh output root is required")
        old = {proof["key"]: proof for proof in previous.manifest["members"]} if previous else {}
        kept = []
        if replace_from is not None:
            if previous is None:
                raise ValueError("a replacement suffix requires an accepted prior manifest")
            key = replace_from.isoformat()[:4 if period == "year" else 7]
            kept = [proof for proof in old.values() if proof["key"] < key]
            # Decode the boundary partition only. Preserve every existing row
            # before the replacement clock, including NULLs and raw bit patterns.
            if key in old:
                member = path.parent / old[key]["path"]
                prefix = pq.ParquetFile(member, memory_map=True).read(use_threads=False)
                prefix = prefix.filter(pc.less(prefix[date_column], pa.scalar(replace_from, type=pa.date32())))
                table = pa.concat_tables([prefix.replace_schema_metadata(table.schema.metadata), table])
                dates = table[date_column].combine_chunks()
        if row_parts:
            run_keys = list(range(0, table.num_rows, partition_rows))
            run_ends = [min(begin + partition_rows, table.num_rows) for begin in run_keys]
        else:
            keys = pc.year(dates)
            if period == "month":
                keys = pc.add(pc.multiply(keys, 100), pc.month(dates))
            runs = pc.run_end_encode(keys)
            run_keys, run_ends = runs.values.to_pylist(), runs.run_ends.to_pylist()
        if len(kept) + len(run_keys) > MAX_MEMBERS:
            raise ValueError("partition inventory bound; no automatic retention")
        begin = 0
        phases["prior_proof_and_boundary_read"] = time.perf_counter() - started
        proofs = list(kept)
        contract = ROW_PARTS_CONTRACT if row_parts else CONTRACT
        stats = {"contract": contract, "written_partitions": 0, "reused_partitions": len(kept),
                 "written_rows": 0, "reused_rows": sum(p["rows"] for p in kept),
                 "written_payload_bytes": 0, "old_generations_removed": 0,
                 "manifest_changed": True}
        planned, changed_bytes, changed_parts = [], 0, 0
        for raw_key, end in zip(run_keys, run_ends, strict=True):
            phase_started = time.perf_counter()
            part = table.slice(begin, end - begin).replace_schema_metadata(None)
            key = (f"{raw_key:020d}" if row_parts else str(raw_key) if period == "year"
                   else f"{raw_key//100:04d}-{raw_key%100:02d}")
            values = table_content_sha256(part)
            phases["typed_digest"] = phases.get("typed_digest", 0.0) + time.perf_counter() - phase_started
            prior = old.get(key)
            if prior is None or prior["values_sha256"] != values:
                changed_bytes += part.nbytes
                changed_parts += 1
            # Retain offsets, not an additional Arrow table per partition.
            planned.append((raw_key, begin, end, key, values, prior))
            begin = end

        def make_manifest(members):
            value = {"contract": contract, "period": period, "date_column": date_column,
                "schema": base64.b64encode(table.schema.serialize().to_pybytes()).decode(),
                "rows": sum(proof["rows"] for proof in members), "members": members,
                "source_identity": source_identity, "writer_implementation_sha256": implementation,
                **({"partition_rows": partition_rows} if row_parts else {})}
            value["manifest_sha256"] = _digest(value)
            if len(json.dumps(value).encode()) > MAX_MANIFEST_BYTES:
                raise ValueError("partition manifest byte bound; last-good retained")
            return value

        def finish_stats(manifest, archived):
            stats["manifest"] = str(path)
            stats["immutable_manifest"] = str(archived)
            stats["current_payload_bytes"] = sum(proof["bytes"] for proof in manifest["members"])
            stats["phase_seconds"] = phases
            return stats

        # A no-op is a fully verified source read, not a new allocation. Even a
        # temporary hardlink to the same inode changes ctime/nlink and breaks an
        # existing reader's source fence. Keep both inode and generation intact.
        if previous is not None and not changed_parts:
            candidate = make_manifest([*kept, *(item[5] for item in planned)])
            if candidate == previous.manifest:
                phase_started = time.perf_counter()
                archived = namespace / f"generation_{candidate['manifest_sha256']}.json"
                if archived.exists() or archived.is_symlink():
                    before = payload_info(archived)
                    if (before["mode"] & 0o222 or before["uid"] != os.geteuid()
                            or before["size"] > MAX_MANIFEST_BYTES
                            or stable_payload_hash(archived, before) != previous.byte_sha256):
                        raise ValueError("immutable manifest archive changed; last-good pointer retained")
                    previous.verify()
                    if hashlib.sha256(Path(__file__).read_bytes()).hexdigest() != implementation:
                        raise ValueError("partition implementation changed during write")
                    stats.update(manifest_changed=False,
                        reused_partitions=len(candidate["members"]), reused_rows=candidate["rows"])
                    phases["manifest_reuse"] = time.perf_counter() - phase_started
                    return finish_stats(candidate, archived)

        # Admit only writes that are actually needed, including a metadata-only
        # generation. New members still have the original bounded serializer,
        # typed readback and durability gates before the pointer can advance.
        growth = max(1024**2, changed_bytes * 3 + 2 * MAX_MANIFEST_BYTES
                     + changed_parts * 512 * 1024)
        if write_budget is None:
            leases.enter_context(reserve_cache_write(namespace,
                key=implementation, maximum_growth_bytes=growth, family="panel", policy=load_capacity_policy()))
            write_budget = leases.enter_context(GenerationWriteBudget(growth))
        for raw_key, begin, end, key, values, prior in planned:
            part = table.slice(begin, end - begin).replace_schema_metadata(None)
            if prior is not None and prior["values_sha256"] == values:
                proof = prior
                stats["reused_partitions"] += 1
                stats["reused_rows"] += part.num_rows
            else:
                phase_started = time.perf_counter()
                target = namespace / f"p_{values}_{uuid.uuid4().hex}.parquet"
                # Parquet emits many tiny column/header writes. Buffer them
                # inside the reserved allowance rather than charging a full
                # filesystem allocation for every serializer callback.
                with io.BufferedWriter(write_budget.open(target), buffer_size=1024**2) as stream:
                    pq.write_table(part, stream, **options)
                phases["serialize"] = phases.get("serialize", 0.0) + time.perf_counter() - phase_started
                phase_started = time.perf_counter()
                roundtrip = pq.ParquetFile(target, memory_map=True).read(use_threads=False)
                if (not roundtrip.schema.equals(part.schema)
                        or table_content_sha256(roundtrip) != values):
                    raise ValueError("partition typed roundtrip failed; last-good manifest retained")
                phases["roundtrip_proof"] = phases.get("roundtrip_proof", 0.0) + time.perf_counter() - phase_started
                phase_started = time.perf_counter()
                seal_new_file(target)
                proof = {"key": key, "path": str(target.relative_to(path.parent)),
                    "sha256": sha256_file(target), "bytes": target.stat().st_size,
                    "rows": part.num_rows, "values_sha256": values,
                    **({"row_start": raw_key} if row_parts else {
                        "min_date": pc.min(part[date_column]).as_py().isoformat(),
                        "max_date": pc.max(part[date_column]).as_py().isoformat()})}
                stats["written_partitions"] += 1
                stats["written_rows"] += part.num_rows
                stats["written_payload_bytes"] += proof["bytes"]
                phases["seal_fsync_byte_proof"] = phases.get("seal_fsync_byte_proof", 0.0) + time.perf_counter() - phase_started
            proofs.append(proof)
        if len(proofs) > MAX_MEMBERS:
            raise ValueError("partition inventory bound; no automatic retention")
        phase_started = time.perf_counter()
        manifest = make_manifest(proofs)
        if previous is not None:
            previous.verify()
        if hashlib.sha256(Path(__file__).read_bytes()).hexdigest() != implementation:
            raise ValueError("partition implementation changed during write")
        # Retain the immutable manifest of every committed generation as well
        # as its payloads. Pointer publication follows durable member names.
        write_budget.charge_json(manifest)
        archived = namespace / f"generation_{manifest['manifest_sha256']}.json"
        if not archived.exists():
            atomic_write_json(archived, manifest, durable=False)
            seal_new_file(archived)
        elif (archived.is_symlink() or json.loads(archived.read_bytes()) != manifest
                or payload_info(archived)["mode"] & 0o222):
            raise ValueError("immutable manifest archive changed; last-good pointer retained")
        _fsync_directory(namespace)
        # The sealed archive inode is already durable. Publish those exact
        # bytes once, without a second serialization/data fsync. Batch callers
        # join all writers before their one final parent-directory barrier.
        write_budget.charge(0)
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        try:
            os.link(archived, temporary, follow_symlinks=False)
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
        if sync_directory:
            _fsync_directory(path.parent)
        phases["manifest_commit"] = time.perf_counter() - phase_started
        return finish_stats(manifest, archived)
