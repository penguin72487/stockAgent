"""Incremental producer-side reuse for the canonical v12 minute cache.

Only committed manifests seed lookup. A session input key covers its complete
ordered axes, source partition, canonical prices/limits/masks and policies.
Old manifests still participate by exact output SHA after one computation.
There is no permanent CAS or synchronized mutable index: generation names own
payloads, and this in-memory index lives only for one canonical build.
"""

from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager
import fcntl
import hashlib
import io
import json
import os
from pathlib import Path
import re
import stat
from typing import Any

import numpy as np

from downloader.artifact_io import atomic_write_bytes
from stockagent.data_sync.immutable_cache_files import (
    link_immutable_payload, payload_info as _info,
)

CONTRACT = "physical_minute_incremental_build_v1"
SOURCE_ABI = "tw_day_trade_physical_source_cache_v12_subscription_right_reference_value"
SESSION_ENCODING = "sorted_flat_sparse_exit_mark_and_exact_inventory_uncompressed_npz"
_GENERATION = re.compile(r"physical-([0-9a-f]{64})\Z")
_SESSION = re.compile(r"session-\d{4}-\d{2}-\d{2}\.npz\Z")
COUNT_NAMES = (
    "minute_symbol_days", "daily_proxy_post_minute_symbol_days",
    "post_minute_missing_or_empty_rows", "post_minute_volume_exceeds_official_day",
    "post_minute_missing_historical_limits",
)


def _real_directory(path: Path) -> Path:
    path = path.absolute()
    if path.resolve(strict=True) != path or not path.is_dir():
        raise ValueError(f"cache directory is redirected/not a directory: {path}")
    return path


def _metadata(path: Path) -> tuple[dict, str]:
    before = _info(path)
    if before["size"] > 16 * 1024**2:
        raise ValueError("oversized cache metadata")
    raw = path.read_bytes()
    if _info(path) != before:
        raise ValueError("cache metadata changed during read")
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate cache metadata key")
            result[key] = value
        return result
    value = json.loads(raw, object_pairs_hook=unique)
    if not isinstance(value, dict):
        raise ValueError("cache metadata must be an object")
    return value, hashlib.sha256(raw).hexdigest()


@contextmanager
def _writer_lock(path: Path):
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ValueError("unsafe physical cache lock")
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        os.close(descriptor)


def _generation_inventory(root: Path, generation: Path) -> tuple[list[dict], list[dict], list[dict]]:
    """Shared manifest/READY contract for the producer and old-cache compactor."""
    with _writer_lock(root / f".{generation.name}.lock"):
        _real_directory(generation)
        manifest_path, ready_path = generation / "manifest.json", generation / "READY.json"
        if not manifest_path.exists() or not ready_path.exists():
            return [], [], [{"generation": generation.name, "reason": "incomplete"}]
        manifest, manifest_hash = _metadata(manifest_path)
        if manifest.get("abi") != SOURCE_ABI:
            return [], [], [{"generation": generation.name, "reason": "unsupported_abi"}]
        ready, ready_hash = _metadata(ready_path)
        digest = _GENERATION.fullmatch(generation.name).group(1)
        members = manifest.get("files")
        if (manifest.get("source_digest") != digest or ready.get("source_digest") != digest
                or ready.get("abi") != SOURCE_ABI or ready.get("manifest_sha256") != manifest_hash
                or manifest.get("session_encoding") != SESSION_ENCODING
                or not isinstance(members, dict) or not members
                or type(ready.get("verified_files")) is not int
                or ready["verified_files"] != len(members)):
            raise ValueError(f"invalid physical cache commit: {generation}")
        expected_sessions = {name for name in members if _SESSION.fullmatch(name)}
        actual_sessions = {path.name for path in generation.glob("session-*.npz")}
        if not expected_sessions or actual_sessions != expected_sessions:
            raise ValueError(f"physical session membership differs: {generation}")
        files = []
        for name, member in sorted(members.items()):
            if (not isinstance(name, str) or Path(name).name != name or name in {".", ".."}
                    or not isinstance(member, dict) or type(member.get("bytes")) is not int
                    or member["bytes"] < 0 or not isinstance(member.get("sha256"), str)
                    or not re.fullmatch(r"[0-9a-f]{64}", member["sha256"])):
                raise ValueError("invalid physical cache member")
            path = generation / name
            info = _info(path)
            if info["device"] != root.stat().st_dev or info["size"] != member["bytes"]:
                raise ValueError(f"physical member type/device/size differs: {path}")
            if name in expected_sessions:
                if info["mode"] & ~0o666 or not info["mode"] & 0o400:
                    raise ValueError("unsupported physical payload permissions")
                if "system.posix_acl_access" in info["xattrs"]:
                    raise ValueError("physical cache ACL requires separate migration")
                files.append({"path": path.relative_to(root).as_posix(),
                              "sha256": member["sha256"], **info})
        proof = {"generation": generation.name, "manifest_sha256": manifest_hash,
                 "ready_sha256": ready_hash, "manifest_identity": _info(manifest_path),
                 "ready_identity": _info(ready_path)}
        return files, [proof], []


def session_input_key(context: dict[str, Any], **arrays: np.ndarray) -> str:
    from stockagent.data.panel_cache import array_content_fingerprint
    value = {"contract": CONTRACT, "context": context,
             "arrays": {name: array_content_fingerprint(array)
                        for name, array in sorted(arrays.items())}}
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def session_diagnostics(gaps: np.ndarray, valuation: np.ndarray, counts: dict) -> dict:
    return {"gaps": np.packbits(gaps, bitorder="little").tobytes().hex(),
            "valuation_known": np.packbits(valuation, bitorder="little").tobytes().hex(),
            "mode_counts": {name: int(counts[name]) for name in COUNT_NAMES}}


def restore_diagnostics(value: Any, symbols: int) -> tuple[np.ndarray, np.ndarray, dict]:
    if not isinstance(value, dict) or set(value) != {"gaps", "valuation_known", "mode_counts"}:
        raise ValueError("unknown incremental session diagnostics")
    counts = value["mode_counts"]
    if (not isinstance(counts, dict) or set(counts) != set(COUNT_NAMES)
            or any(type(number) is not int or not 0 <= number <= symbols for number in counts.values())):
        raise ValueError("invalid incremental session counts")
    masks = []
    for name in ("gaps", "valuation_known"):
        encoded = value[name]
        if not isinstance(encoded, str) or len(encoded) != 2 * ((symbols + 7) // 8):
            raise ValueError("invalid incremental session mask")
        raw = bytes.fromhex(encoded)
        bits = np.unpackbits(np.frombuffer(raw, dtype=np.uint8), bitorder="little")
        if bits[symbols:].any():
            raise ValueError("noncanonical incremental mask padding")
        masks.append(bits[:symbols].astype(bool))
    return masks[0], masks[1], dict(counts)


class PhysicalSessionReuse:
    """One build's bounded index; destination writer lock is already held."""

    def __init__(self, root: Path, current: Path, symbols: int):
        self.root, self.current, self.symbols = root.resolve(), current.resolve(), symbols
        self.by_content: dict[tuple[str, int], list[dict]] = defaultdict(list)
        self.by_input: dict[str, list[dict]] = defaultdict(list)
        self.reused_inputs = self.reused_content = self.written_sessions = 0
        self.avoided_payload_bytes = self.written_payload_bytes = 0
        for generation in sorted(self.root.iterdir()):
            if not _GENERATION.fullmatch(generation.name) or generation == self.current:
                continue
            try:
                files, proofs, _ = _generation_inventory(self.root, generation)
                if not proofs:
                    continue
                manifest, _ = _metadata(generation / "manifest.json")
                proof = proofs[0]
                if _info(generation / "manifest.json") != proof["manifest_identity"]:
                    continue
                builds = manifest.get("session_builds", {}) if manifest.get("storage_contract") == CONTRACT else {}
                for item in files:
                    candidate = {"path": self.root / item["path"], "proof": proof,
                                 "sha256": item["sha256"], "bytes": item["size"]}
                    self.by_content[(item["sha256"], item["size"])].append(candidate)
                    build = builds.get(candidate["path"].name) if isinstance(builds, dict) else None
                    if (isinstance(build, dict) and isinstance(build.get("input_key"), str)
                            and len(build["input_key"]) == 64):
                        restore_diagnostics(build.get("diagnostics"), symbols)
                        candidate["build"] = build
                        self.by_input[build["input_key"]].append(candidate)
            except (BlockingIOError, FileNotFoundError, ValueError, TypeError):
                continue  # Unknown/busy/broken caches remain, never become authority.

    def _link(self, candidate: dict, destination: Path) -> bool:
        def publish():
            proof = candidate["proof"]
            if proof is not None and any(_info(candidate["path"].parent / name) != proof[field]
                    for name, field in (("manifest.json", "manifest_identity"), ("READY.json", "ready_identity"))):
                return False
            return link_immutable_payload(candidate["path"], destination,
                                          sha256=candidate["sha256"], size=candidate["bytes"])
        try:
            if candidate["path"].parent == self.current:
                return publish()
            with _writer_lock(self.root / f".{candidate['path'].parent.name}.lock"):
                return publish()
        except (BlockingIOError, FileNotFoundError, ValueError):
            return False

    def reuse_input(self, key: str, destination: Path) -> dict | None:
        for candidate in self.by_input.get(key, ()):
            if self._link(candidate, destination):
                self.reused_inputs += 1
                self.avoided_payload_bytes += candidate["bytes"]
                return {"file": {"bytes": candidate["bytes"], "sha256": candidate["sha256"]},
                        "build": candidate["build"]}
        return None

    def write(self, destination: Path, payload: dict[str, np.ndarray]) -> dict:
        # Same seekable NPZ serializer as the original writer, one session at a
        # time in RAM. A hit creates no duplicate temporary payload on disk.
        buffer = io.BytesIO()
        np.savez(buffer, **payload)
        data = buffer.getbuffer()
        sha256, size = hashlib.sha256(data).hexdigest(), len(data)
        for candidate in self.by_content.get((sha256, size), ()):
            if self._link(candidate, destination):
                self.reused_content += 1
                self.avoided_payload_bytes += size
                break
        else:
            atomic_write_bytes(destination, data, durable=True, mode=0o400)
            self.written_sessions += 1
            self.written_payload_bytes += size
        self.by_content[(sha256, size)].append({"path": destination, "proof": None,
                                               "sha256": sha256, "bytes": size})
        return {"bytes": size, "sha256": sha256}

    def statistics(self) -> dict:
        return {"contract": CONTRACT, "reused_input_sessions": self.reused_inputs,
                "reused_content_sessions": self.reused_content,
                "written_sessions": self.written_sessions,
                "avoided_payload_bytes": self.avoided_payload_bytes,
                "written_payload_bytes": self.written_payload_bytes}
