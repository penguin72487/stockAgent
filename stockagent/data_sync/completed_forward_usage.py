"""Sample successful forward-cache use without mutating its immutable commit.

The caller holds the canonical per-key writer lock and has already validated
the complete payload and input identity. These small records are advisory LRU
evidence, not acceptance, current-consumer pins, cold recovery, or permission to
delete. Missing/damaged usage must retain a key, never cause GPU recomputation.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time

from downloader.artifact_io import atomic_write_bytes

CONTRACT = "completed_forward_successful_use_sample_v1"
SAMPLE_INTERVAL_NS = 60 * 10**9
MAX_USAGE_BYTES = 4096
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate usage metadata key")
        result[key] = value
    return result


def _nonfinite(value):
    raise ValueError("nonfinite usage metadata")


def read_usage(path: Path) -> tuple[dict, str]:
    """Bounded no-follow regular-file read; reject redirected or shared state."""
    descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_CLOEXEC | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        if (not stat.S_ISREG(before.st_mode) or before.st_size > MAX_USAGE_BYTES
                or before.st_uid != os.geteuid() or before.st_gid != os.getegid()
                or stat.S_IMODE(before.st_mode) != 0o600 or before.st_nlink != 1):
            raise ValueError("untrusted forward usage metadata")
        raw = stream.read(MAX_USAGE_BYTES + 1)
        after = os.fstat(stream.fileno())
        generation = lambda info: (info.st_dev, info.st_ino, info.st_size,
                                   info.st_mtime_ns, info.st_ctime_ns)
        if (len(raw) != before.st_size or generation(before) != generation(after)
                or generation(path.lstat()) != generation(before)):
            raise ValueError("usage metadata changed during read")
    try:
        record = json.loads(raw, object_pairs_hook=_unique, parse_constant=_nonfinite)
    except RecursionError as error:
        raise ValueError("usage metadata nesting exceeds parser bound") from error
    fields = {"contract", "key", "receipt_sha256", "payload_sha256", "payload_bytes",
              "first_success_at_ns", "last_success_at_ns", "recorded_samples"}
    if (not isinstance(record, dict) or set(record) != fields or record["contract"] != CONTRACT
            or any(not isinstance(record[name], str) or not _SHA256.fullmatch(record[name])
                   for name in ("key", "receipt_sha256", "payload_sha256"))
            or any(type(record[name]) is not int or record[name] <= 0 for name in (
                "payload_bytes", "first_success_at_ns", "last_success_at_ns", "recorded_samples"))
            or record["first_success_at_ns"] > record["last_success_at_ns"]):
        raise ValueError("invalid forward usage metadata")
    return record, hashlib.sha256(raw).hexdigest()


def commit_binding(*, key: str, receipt_sha256: str, payload_sha256: str,
                   payload_bytes: int) -> dict:
    if (any(not isinstance(value, str) or not _SHA256.fullmatch(value)
            for value in (key, receipt_sha256, payload_sha256))
            or type(payload_bytes) is not int or payload_bytes <= 0):
        raise ValueError("invalid forward usage commit binding")
    return {"key": key, "receipt_sha256": receipt_sha256,
            "payload_sha256": payload_sha256, "payload_bytes": payload_bytes}


def note_successful_use(root: Path, *, key: str, receipt_sha256: str,
                        payload_sha256: str, payload_bytes: int,
                        now_ns: int | None = None) -> dict:
    """At most one metadata replacement per minute/key under the owner's lock.

    Sample counts are NOT hit counts. A changed valid commit resets the sample
    series; malformed metadata is preserved for diagnosis. Non-durable usage
    loss after power failure is conservative: unknown use blocks LRU retirement.
    Any telemetry I/O failure is reported, not propagated into valid inference.
    """
    status = {"contract": CONTRACT, "sample_interval_seconds": SAMPLE_INTERVAL_NS // 10**9}
    try:
        binding = commit_binding(key=key, receipt_sha256=receipt_sha256,
                                 payload_sha256=payload_sha256, payload_bytes=payload_bytes)
        now = time.time_ns() if now_ns is None else now_ns
        if type(now) is not int or now <= 0:
            raise ValueError("invalid successful-use clock")
        path = root / f"{key}.usage.json"
        try:
            previous, _ = read_usage(path)
        except FileNotFoundError:
            previous = None
        same_commit = previous is not None and all(previous[name] == value
                                                   for name, value in binding.items())
        if same_commit and previous["last_success_at_ns"] > now:
            return {**status, "state": "deferred", "reason": "wall_clock_reversed"}
        if same_commit and now - previous["last_success_at_ns"] < SAMPLE_INTERVAL_NS:
            return {**status, "state": "sampled", "metadata_written": False}
        record = {"contract": CONTRACT, **binding,
                  "first_success_at_ns": previous["first_success_at_ns"] if same_commit else now,
                  "last_success_at_ns": now,
                  "recorded_samples": previous["recorded_samples"] + 1 if same_commit else 1}
        raw = json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        if len(raw) > MAX_USAGE_BYTES:
            raise ValueError("forward usage metadata exceeds bound")
        atomic_write_bytes(path, raw, mode=0o600)
        return {**status, "state": "recorded", "metadata_written": True}
    except (OSError, ValueError, OverflowError) as error:
        return {**status, "state": "deferred", "reason": type(error).__name__}
