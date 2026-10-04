"""Bounded, replaceable website projection shards; never source publications.

The canonical identity scan and row math remain in data_monitor_inventory.
Only owners proven unchanged against the exact preceding cache generation may
reuse encoded rows. The complete legacy JSON stays the public authority.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time
from typing import Any, Mapping
from uuid import uuid4

from downloader.artifact_io import atomic_write_bytes, durable_replace
from stockagent.live import data_monitor_dashboard as dashboard
from stockagent.live import data_monitor_inventory as inventory
from stockagent.live.dashboard_updates import metadata_signature
from stockagent.live.data_monitor_feature_pages import (
    FEATURE_PREVIEW_LIMIT, FEATURE_SOURCE_PAGE_MAX_ROWS,
    feature_page_projections, valid_feature_page_preview, valid_feature_source_pages,
)


SHARD_VERSION = 1
MAX_CACHE_BYTES = 256 * 1024 * 1024
MAX_MANIFEST_BYTES = 2 * 1024 * 1024
OBJECT_NAME = re.compile(r"^[0-9a-f]{64}\.(?:rows|preview)$")
MARKER = b'{"format":"data-monitor-projection-shards-v1","source_data":false}\n'


class ProjectionObjectError(ValueError):
    """A known cache object needs reconstruction, not source repair."""


def _encode(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"),
                      sort_keys=False, allow_nan=False).encode("utf-8")


def projection_abi() -> str:
    digest = hashlib.sha256()
    for module in (inventory, dashboard):
        digest.update(Path(module.__file__).read_bytes())
    digest.update(Path(__file__).read_bytes())
    from stockagent.live import data_monitor_feature_pages
    digest.update(Path(data_monitor_feature_pages.__file__).read_bytes())
    return digest.hexdigest()


def _private_stat(path: Path) -> os.stat_result:
    value = path.lstat()
    if not stat.S_ISREG(value.st_mode) or value.st_uid != os.geteuid() or value.st_mode & 0o022:
        raise ValueError("untrusted projection cache file")
    return value


def _read_private(path: Path, *, max_bytes: int) -> bytes:
    before = _private_stat(path)
    if before.st_size > max_bytes:
        raise ValueError("projection cache file exceeds bound")
    with path.open("rb") as stream:
        if metadata_signature(os.fstat(stream.fileno())) != metadata_signature(before):
            raise ValueError("projection cache file moved")
        body = stream.read(max_bytes + 1)
        if metadata_signature(os.fstat(stream.fileno())) != metadata_signature(before):
            raise ValueError("projection cache file changed")
    if metadata_signature(_private_stat(path)) != metadata_signature(before) or len(body) > max_bytes:
        raise ValueError("projection cache file moved")
    return body


def _valid_signature(value: Any) -> bool:
    return isinstance(value, list) and len(value) == 5 and all(type(v) is int for v in value)


@dataclass(frozen=True)
class ShardedFeaturePublication:
    fields: int
    source_observation_root: str
    preview: Mapping[str, Any]
    source_pages: Mapping[str, Any]
    rebuilt_datasets: int
    reused_datasets: int
    timing_ms: Mapping[str, float]


class ProjectionStore:
    """Two manifests and bounded content objects, scoped to one output file."""

    def __init__(self, output: Path, *, max_bytes: int = MAX_CACHE_BYTES) -> None:
        self.output = Path(output)
        self.root = self.output.with_name(f".{self.output.name}.projection-cache")
        self.objects = self.root / "objects"
        self.max_bytes = max_bytes
        self._used_bytes: int | None = None

    @contextmanager
    def locked(self):
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        for path in (self.root, self.objects):
            path.mkdir(exist_ok=True, mode=0o700)
            value = path.lstat()
            if not stat.S_ISDIR(value.st_mode) or value.st_uid != os.geteuid() or value.st_mode & 0o022:
                raise ValueError("untrusted projection cache directory")
        marker = self.root / "FORMAT"
        if marker.exists():
            if _read_private(marker, max_bytes=1024) != MARKER:
                raise ValueError("unknown projection cache")
        else:
            if any(self.objects.iterdir()):
                raise ValueError("unmarked projection objects")
            atomic_write_bytes(marker, MARKER, durable=True)
        lock = self.root / "writer.lock"
        if lock.exists():
            _private_stat(lock)
        with lock.open("a+b") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def manifest(self, name: str = "current.json") -> Mapping[str, Any] | None:
        try:
            payload = json.loads(_read_private(self.root / name, max_bytes=MAX_MANIFEST_BYTES))
            checksum = payload.pop("sha256")
            if (
                payload.get("schema_version") != SHARD_VERSION
                or payload.get("source_data") is not False
                or payload.get("output") != str(self.output.resolve())
                or not isinstance(payload.get("datasets"), dict)
                or len(payload["datasets"]) > 10_000
                or checksum != hashlib.sha256(_encode(payload)).hexdigest()
            ):
                return None
            return payload
        except (OSError, ValueError, TypeError, KeyError, AttributeError):
            return None

    def prune(self) -> None:
        """Evict only this marked cache's orphaned, regular digest objects.

        No recursion, source paths, receipts, unknown names or shared inodes.
        Readers/writers of these private objects all hold the same writer lock.
        """
        retained: set[str] = set()
        for name in ("current.json", "previous.json"):
            manifest = self.manifest(name)
            if manifest is None:
                if (self.root / name).exists():
                    return  # damaged metadata is not a deletion inventory
                continue
            for item in manifest["datasets"].values():
                if not isinstance(item, dict):
                    return
                for key, suffix in (("sha256", "rows"), ("preview_sha256", "preview")):
                    value = item.get(key)
                    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
                        return
                    retained.add(f"{value}.{suffix}")
        for path in self.objects.iterdir():
            if not OBJECT_NAME.fullmatch(path.name) or path.name in retained:
                continue
            try:
                before = _private_stat(path)
                if before.st_nlink != 1:
                    continue
                body = _read_private(path, max_bytes=self.max_bytes)
                if hashlib.sha256(body).hexdigest() != path.name.split(".", 1)[0]:
                    continue
                if metadata_signature(_private_stat(path)) == metadata_signature(before):
                    path.unlink()
            except (OSError, ValueError):
                continue

    def put(self, body: bytes, *, suffix: str) -> tuple[str, int]:
        if suffix not in {"rows", "preview"}:
            raise ValueError("unknown projection object type")
        digest = hashlib.sha256(body).hexdigest()
        path = self.objects / f"{digest}.{suffix}"
        old_size = 0
        if path.exists():
            old_size = _private_stat(path).st_size
            if _read_private(path, max_bytes=self.max_bytes) == body:
                return digest, len(body)
        if self._used_bytes is None:
            self._used_bytes = sum(path.lstat().st_size for path in self.objects.iterdir())
        if self._used_bytes - old_size + len(body) > self.max_bytes:
            raise ValueError("projection cache capacity exhausted")
        atomic_write_bytes(path, body, durable=True)
        self._used_bytes += len(body) - old_size
        return digest, len(body)

    def read_object(self, item: Mapping[str, Any], *, preview: bool = False) -> bytes:
        key = "preview_sha256" if preview else "sha256"
        size_key = "preview_bytes" if preview else "bytes"
        digest = item.get(key)
        size = item.get(size_key)
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("invalid projection object digest")
        if type(size) is not int or not 0 <= size <= self.max_bytes:
            raise ValueError("invalid projection object size")
        try:
            body = _read_private(self.objects / f"{digest}.{'preview' if preview else 'rows'}", max_bytes=size)
        except (OSError, ValueError) as exc:
            raise ProjectionObjectError("projection object unavailable") from exc
        if len(body) != size or hashlib.sha256(body).hexdigest() != digest:
            raise ProjectionObjectError("corrupt projection object")
        return body

    def publish(self, header: Mapping[str, Any], ordered: list[Mapping[str, Any]]) -> None:
        temporary = self.output.with_name(f".{self.output.name}.shards.{uuid4().hex}.tmp")
        self.output.parent.mkdir(parents=True, exist_ok=True)
        try:
            with temporary.open("xb") as stream:
                stream.write(_encode(header)[:-1] + b',"rows":[')
                first = True
                for item in ordered:
                    if not item["fields"]:
                        continue
                    body = self.read_object(item)
                    if not first:
                        stream.write(b",")
                    stream.write(body)
                    first = False
                stream.write(b"]}\n")
            durable_replace(temporary, self.output)
        finally:
            temporary.unlink(missing_ok=True)

    def commit(self, payload: dict[str, Any]) -> None:
        previous = self.manifest()
        if previous is not None:
            previous["sha256"] = hashlib.sha256(_encode(previous)).hexdigest()
            atomic_write_bytes(self.root / "previous.json", _encode(previous), durable=True)
        payload["sha256"] = hashlib.sha256(_encode(payload)).hexdigest()
        body = _encode(payload)
        if len(body) > MAX_MANIFEST_BYTES:
            raise ValueError("projection manifest exceeds bound")
        atomic_write_bytes(self.root / "current.json", body, durable=True)


def _publish_feature_shards(
    repo_root: Path, output: Path, *, snapshot: inventory.InventorySnapshot,
    monitor_status: Mapping[str, Any], max_cache_bytes: int = MAX_CACHE_BYTES,
    allow_reuse: bool = True,
    expected_source_observation_root: str | None = None,
) -> ShardedFeaturePublication:
    """Scan every source, rebuild changed owners, atomically publish all rows.

    Caller retains the canonical full-build fallback if this optional cache is
    unavailable. A failed/torn object never replaces the authoritative JSON.
    """
    started = time.perf_counter()
    store = ProjectionStore(output, max_bytes=max_cache_bytes)
    timing_ms: dict[str, float] = {}
    with store.locked():
        abi = projection_abi()
        previous = store.manifest()
        scan = inventory.FeatureInventoryScan(repo_root, snapshot=snapshot, timing_ms=timing_ms)
        reusable = bool(
            allow_reuse and previous and previous.get("abi") == abi
            and _valid_signature(snapshot.previous_cache_signature)
            and previous.get("cache_signature") == snapshot.previous_cache_signature
            and snapshot.changed_dataset_ids is not None
        )
        store.prune()
        source_rows = dashboard._feature_source_rows(monitor_status)
        category_order = {name: index for index, name in enumerate(dashboard._MARKET_CATEGORY_LABELS)}
        items: dict[str, dict[str, Any]] = {}
        rebuilt = reused = 0
        for dataset in scan:
            owner = dataset.dataset_id
            metadata = list(dashboard._feature_source_metadata(source_rows, owner))
            old = previous["datasets"].get(owner) if reusable else None
            if (
                isinstance(old, dict) and old.get("dataset_id") == owner
                and owner not in snapshot.changed_dataset_ids
                and old.get("source_observation_root") == dataset.source_observation_root
                and old.get("metadata") == metadata
                and old.get("valid_files") == dataset.valid_files
                and old.get("files_total") == dataset.files_total
                and type(old.get("fields")) is int and 0 <= old["fields"] <= 10_000_000
            ):
                items[owner] = old
                reused += 1
                continue
            raw_rows = dataset.rows(timing_ms=timing_ms)
            raw = {"rows": raw_rows, "datasets_with_schema": bool(raw_rows),
                   "datasets_total": 1, "files_with_schema": dataset.valid_files,
                   "files_total": dataset.files_total, "state": "partial", "basis": ""}
            public = dashboard.build_data_monitor_feature_inventory(
                repo_root, monitor_status=monitor_status, inventory=raw,
            )
            # Use the canonical public validator even on first cache creation.
            feature_page_projections(public)
            body = b",".join(_encode(row) for row in public["rows"])
            digest, size = store.put(body, suffix="rows")
            preview_digest, preview_size = store.put(_encode(public["rows"][:FEATURE_PREVIEW_LIMIT]), suffix="preview")
            items[owner] = {
                "dataset_id": owner, "metadata": metadata, "fields": len(raw_rows),
                "valid_files": dataset.valid_files, "files_total": dataset.files_total,
                "source_observation_root": dataset.source_observation_root,
                "sha256": digest, "bytes": size,
                "preview_sha256": preview_digest, "preview_bytes": preview_size,
            }
            rebuilt += 1
        ordered = sorted(items.values(), key=lambda item: (
            category_order.get(item["metadata"][2], 99),
            str(item["metadata"][1]), str(item["metadata"][0]), item["dataset_id"],
        ))
        fields = sum(item["fields"] for item in ordered)
        raw_summary = scan.summary(datasets_with_schema=sum(bool(item["fields"]) for item in ordered))
        inventory.require_coherent_feature_source(raw_summary, expected_source_observation_root)
        public = dashboard.build_data_monitor_feature_inventory(
            repo_root, monitor_status=monitor_status, inventory={"rows": [], **raw_summary},
        )
        del public["rows"]
        public["summary"]["fields"] = fields
        preview_rows = []
        categories: dict[str, str] = {}
        sources: dict[str, str] = {}
        pages: dict[str, Any] = {}
        for item in ordered:
            if not item["fields"]:
                continue
            title, provider, category, label = item["metadata"]
            categories.setdefault(category, label)
            sources[item["dataset_id"]] = f"{title} · {provider}"
            if len(preview_rows) < FEATURE_PREVIEW_LIMIT:
                preview_rows.extend(json.loads(store.read_object(item, preview=True))[:FEATURE_PREVIEW_LIMIT - len(preview_rows)])
            if item["fields"] <= FEATURE_SOURCE_PAGE_MAX_ROWS:
                pages[item["dataset_id"]] = json.loads(b"[" + store.read_object(item) + b"]")
        preview = {
            "schema_version": 1, "read_only": True, "production_control_possible": False,
            "generated_at_utc": public["generated_at_utc"], "summary": public["summary"],
            "filters": {"categories": [{"id": k, "label": v} for k, v in categories.items()],
                        "sources": [{"id": k, "label": v} for k, v in sources.items()]},
            "rows": preview_rows,
        }
        if not valid_feature_page_preview(preview, fields=fields) or not valid_feature_source_pages(pages, fields=fields):
            raise ValueError("invalid sharded page projections")
        published = time.perf_counter()
        store.publish(public, ordered)
        timing_ms["atomic_publication"] = (time.perf_counter() - published) * 1_000
        store.commit({
            "schema_version": SHARD_VERSION, "source_data": False,
            "output": str(output.resolve()), "abi": abi,
            "cache_signature": snapshot.cache_signature, "datasets": items,
        })
        timing_ms["total"] = (time.perf_counter() - started) * 1_000
        return ShardedFeaturePublication(fields, raw_summary["source_observation_root"], preview, pages,
                                         rebuilt, reused, {k: round(v, 3) for k, v in timing_ms.items()})


def publish_feature_shards(
    repo_root: Path, output: Path, *, snapshot: inventory.InventorySnapshot,
    monitor_status: Mapping[str, Any], max_cache_bytes: int = MAX_CACHE_BYTES,
    expected_source_observation_root: str | None = None,
) -> ShardedFeaturePublication:
    """Reconstruct a lost/corrupt private object once; never weaken source checks."""
    started = time.perf_counter()
    try:
        return _publish_feature_shards(repo_root, output, snapshot=snapshot,
                                       monitor_status=monitor_status, max_cache_bytes=max_cache_bytes,
                                       expected_source_observation_root=expected_source_observation_root)
    except ProjectionObjectError:
        result = _publish_feature_shards(repo_root, output, snapshot=snapshot,
                                        monitor_status=monitor_status, max_cache_bytes=max_cache_bytes,
                                        allow_reuse=False,
                                        expected_source_observation_root=expected_source_observation_root)
        result.timing_ms["object_recovery_total"] = round((time.perf_counter() - started) * 1_000, 3)
        return result
