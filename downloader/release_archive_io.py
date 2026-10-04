"""Keep original-release Parquet byte-stable when only the polling clock moves."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
import re
import tempfile
from typing import Any

try:
    from downloader.artifact_io import atomic_write_bytes
except ImportError:  # direct invocation from downloader/
    from artifact_io import atomic_write_bytes


MAX_RELEASE_STATE_BYTES = 4 * 1024 * 1024
_RESUME_DATASETS = frozenset({"cbc_money_release_vintages", "cbc_fx_reserve_release_vintages"})


class ReleaseStateTooLarge(ValueError):
    """A claimed success was persisted as degraded, never silently accepted."""


def _state_json_bytes(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _bounded_state_bytes(path: Path) -> bytes | None:
    with path.open("rb") as handle:
        raw = handle.read(MAX_RELEASE_STATE_BYTES + 1)
    return raw if len(raw) <= MAX_RELEASE_STATE_BYTES else None


def _companion_path(root: Path, dataset: str, digest: str | None = None) -> Path:
    """Derive local paths only; no receipt-controlled path or recursive pointer."""
    state_root = root.resolve() / "state"
    directory = state_root / ("release_resume_evidence" if digest else "release_state_diagnostics")
    target = directory / dataset / f"{digest}.json" if digest else directory / f"{dataset}.oversized.json"
    for path in (state_root, directory, target.parent, target):
        if path.is_symlink():
            raise ValueError("release state companion path is a symlink")
    return target


def _online_completed_state(state: object, dataset: str) -> dict[str, Any] | None:
    """Identify a resume *candidate*, never certify current source completeness.

    File bytes must be verified by the collector under its existing writer lock
    before use. A cached reparse cannot create a new online-success checkpoint.
    """
    if not isinstance(state, dict) or state.get("dataset") != dataset:
        return None
    if state.get("complete") is not True or state.get("status") != "complete":
        return None
    if state.get("offline_cache_only") or state.get("cached_list_pages"):
        return None
    if not isinstance(state.get("parquet_sha256"), str) or not re.fullmatch(
        r"[0-9a-f]{64}", state["parquet_sha256"]
    ):
        return None
    receipts = state.get("listing_receipts")
    if not isinstance(receipts, list) or not receipts or len(receipts) > 1000:
        return None
    if state.get("failures") or type(state.get("saved_releases")) is not int:
        return None
    if (type(state.get("registered_releases")) is not int
            or not 0 < state["saved_releases"] == state["registered_releases"]):
        return None
    try:
        generated = datetime.fromisoformat(state.get("generated_at_utc"))
        if generated.utcoffset() is None or generated > datetime.now(timezone.utc):
            return None
    except (TypeError, ValueError):
        return None
    if dataset == "cbc_money_release_vintages":
        if state.get("scan_scope") not in ("full_index", "recent_pages"):
            return None
        full_clock = state.get("last_full_index_scan_at_utc")
        if not full_clock and state.get("scan_scope") == "full_index":
            full_clock = state.get("generated_at_utc")
        try:
            observed = datetime.fromisoformat(full_clock)
            if observed.utcoffset() is None or observed > generated:
                return None
        except (TypeError, ValueError):
            return None
    return {key: value for key, value in state.items() if key != "resume_checkpoint"}


def _inline_resume_candidate(state: object, dataset: str) -> dict[str, Any] | None:
    candidate = _online_completed_state(state, dataset)
    if candidate is not None:
        return candidate
    if not isinstance(state, dict) or state.get("dataset") != dataset:
        return None
    checkpoint = state.get("resume_checkpoint")
    if (not isinstance(checkpoint, dict) or type(checkpoint.get("schema_version")) is not int
            or checkpoint["schema_version"] != 1):
        return None
    return _online_completed_state(checkpoint.get("completed_state"), dataset)


def _read_resume_evidence(
    root: Path, dataset: str,
) -> tuple[dict[str, Any] | None, bytes | None, dict[str, Any] | None]:
    if dataset not in _RESUME_DATASETS:
        raise ValueError("unsupported release resume dataset")
    try:
        raw = _bounded_state_bytes(root / "state" / f"{dataset}.json")
        if raw is None:
            return None, None, None
        state = json.loads(raw)
        candidate = _inline_resume_candidate(state, dataset)
        if candidate is not None:
            return candidate, raw, None
        if not isinstance(state, dict) or state.get("dataset") != dataset:
            return None, None, None
        checkpoint = state.get("resume_checkpoint")
        if (not isinstance(checkpoint, dict) or type(checkpoint.get("schema_version")) is not int
                or checkpoint["schema_version"] != 2 or "completed_state" in checkpoint):
            return None, None, None
        reference = checkpoint.get("completed_state_ref")
        if not isinstance(reference, dict) or set(reference) != {"sha256", "bytes"}:
            return None, None, None
        digest, size = reference["sha256"], reference["bytes"]
        if (not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None
                or type(size) is not int or not 0 < size <= MAX_RELEASE_STATE_BYTES):
            return None, None, None
        raw = _bounded_state_bytes(_companion_path(root, dataset, digest))
        if raw is None or len(raw) != size or hashlib.sha256(raw).hexdigest() != digest:
            return None, None, None
        # The object contains the original bounded inline receipt bytes, not
        # another pointer. No recursion, mutable alias or reconstructed proof.
        candidate = _inline_resume_candidate(json.loads(raw), dataset)
        return (candidate, raw, checkpoint) if candidate is not None else (None, None, None)
    except (OSError, ValueError, RecursionError):
        return None, None, None


def read_release_resume_state(root: Path, dataset: str) -> dict[str, Any] | None:
    """Read at most two bounded files; a candidate is not current health proof."""
    return _read_resume_evidence(root, dataset)[0]


def write_release_state(root: Path, dataset: str, state: dict[str, Any]) -> None:
    """Keep current attempt health separate from the last online resume proof.

    Callers hold their canonical writer lock. Only this bounded previous receipt
    is consulted; do not synthesize a success receipt from loose raw files. The
    old checkpoint survives repeated failures without recursive growth. Existing
    readers continue using top-level current status, complete and timestamps.
    """
    previous, previous_raw, previous_ref = _read_resume_evidence(root, dataset)
    payload = {key: value for key, value in state.items() if key != "resume_checkpoint"}
    if payload.get("dataset") != dataset:
        raise ValueError("release state dataset mismatch")
    if payload.get("status") != "complete":
        payload["complete"] = False
    completed = _online_completed_state(payload, dataset)
    if payload.get("status") == "complete" and completed is None:
        # Do not silently downgrade only the file while the caller returns a
        # green summary. CLI exception handling persists the current failure.
        raise ValueError("complete release state lacks an online resume proof")
    if completed is None and previous is not None:
        payload["resume_checkpoint"] = previous_ref or {"schema_version": 1, "completed_state": previous}
    raw = _state_json_bytes(payload)
    target = root / "state" / f"{dataset}.json"
    if len(raw) <= MAX_RELEASE_STATE_BYTES:
        atomic_write_bytes(target, raw, durable=True)
        return

    # Store the exact earlier, bounded inline bytes before replacing latest.
    # Re-encoding/nesting a near-limit checkpoint can itself exceed the limit.
    checkpoint_ref = None
    if previous is not None and previous_raw is not None:
        digest = hashlib.sha256(previous_raw).hexdigest()
        try:
            path = _companion_path(root, dataset, digest)
            try:
                existing = _bounded_state_bytes(path)
            except FileNotFoundError:
                existing = None
            if existing != previous_raw:
                atomic_write_bytes(path, previous_raw, durable=True)
        except (OSError, ValueError) as error:
            # Never destroy the only resume proof just to persist a red status.
            # With evidence storage unavailable, this attempt must fail; latest
            # remains the earlier receipt, NOT a persisted current success.
            error.add_note(
                f"{dataset}: new_state_not_persisted; previous receipt unchanged; "
                f"resume_sha256={digest}; resume_bytes={len(previous_raw)}"
            )
            raise
        checkpoint_ref = {"schema_version": 2, "completed_state_ref": {
            "sha256": digest, "bytes": len(previous_raw),
        }}
    message = f"release state is {len(raw)} bytes; maximum is {MAX_RELEASE_STATE_BYTES}"
    fallback: dict[str, Any] = {
        "dataset": dataset, "status": "degraded", "complete": False,
        "error": message, "reason": "release_state_too_large",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "attempt_status": payload.get("status") if payload.get("status") in (
            "complete", "running", "degraded"
        ) else "unknown",
    }
    if checkpoint_ref is not None:
        fallback["resume_checkpoint"] = checkpoint_ref
    diagnostic: dict[str, Any] = {
        "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest(), "saved": False,
    }
    try:
        path = _companion_path(root, dataset)
        atomic_write_bytes(path, raw, durable=True)
        diagnostic.update(saved=True, path=str(path.relative_to(root.resolve())))
    except (OSError, ValueError) as error:
        diagnostic["error_type"] = type(error).__name__
    fallback["oversized_diagnostic"] = diagnostic
    encoded = _state_json_bytes(fallback)
    if len(encoded) > MAX_RELEASE_STATE_BYTES:
        raise ValueError("release failure envelope exceeds reader limit")
    atomic_write_bytes(target, encoded, durable=True)
    if completed is not None:
        # Collectors otherwise return their original complete=True summary.
        raise ReleaseStateTooLarge(message)


def write_release_rows_if_changed(
    path: Path,
    rows: list[dict[str, Any]],
    *,
    identity_columns: tuple[str, ...],
    allow_placeholder_upgrade: bool = False,
) -> tuple[str, bool]:
    """Preserve the first observation of unchanged bytes; write only real changes.

    ``observed_at_utc`` is a collector clock, not a new source vintage.  A
    repeated read of identical original HTML/attachment hashes must not make
    the feature builder treat the entire historical source as revised.
    """

    import polars as pl

    def keyed(items: list[dict[str, Any]]) -> dict[tuple[Any, ...], dict[str, Any]]:
        result: dict[tuple[Any, ...], dict[str, Any]] = {}
        for item in items:
            key = tuple(item[column] for column in identity_columns)
            if key in result:
                raise ValueError(f"duplicate release identity: {key}")
            result[key] = item
        return result

    def without_poll_clock(item: dict[str, Any]) -> dict[str, Any]:
        return {key: value for key, value in item.items() if key != "observed_at_utc"}

    new_by_key = keyed(rows)
    prior_rows = pl.read_parquet(path).to_dicts() if path.is_file() else []
    prior_by_key = keyed(prior_rows)
    missing_prior = prior_by_key.keys() - new_by_key.keys()
    upgraded_placeholders: dict[str, dict[str, Any]] = {}
    if allow_placeholder_upgrade:
        if identity_columns != ("release_url", "metric"):
            raise ValueError("placeholder upgrade requires release URL and metric identity")
        for key in list(missing_prior):
            if key[1] is not None:
                continue
            url = str(key[0])
            upgrades = [row for row in rows if row["release_url"] == url]
            if ({row.get("metric") for row in upgrades} == {"m1b_yoy_pct", "m2_yoy_pct"}
                    and all(row.get("html_sha256") == prior_by_key[key].get("html_sha256")
                            for row in upgrades)):
                upgraded_placeholders[url] = prior_by_key[key]
                missing_prior.remove(key)
    if missing_prior:
        examples = sorted(map(str, missing_prior))[:5]
        raise ValueError(
            f"release identity regression: {len(missing_prior)} previously archived releases "
            f"are absent from this scan; examples={examples}"
        )
    stable_rows: list[dict[str, Any]] = []
    for row in rows:
        current = dict(row)
        old = prior_by_key.get(tuple(row[column] for column in identity_columns))
        if old is not None and without_poll_clock(old) == without_poll_clock(current):
            current["observed_at_utc"] = old.get("observed_at_utc")
        elif old is None and str(row.get("release_url")) in upgraded_placeholders:
            current["observed_at_utc"] = upgraded_placeholders[str(row["release_url"])].get("observed_at_utc")
        stable_rows.append(current)
    if path.is_file() and stable_rows == prior_rows:
        with path.open("rb") as handle:
            return hashlib.file_digest(handle, "sha256").hexdigest(), False
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
    try:
        pl.DataFrame(stable_rows, infer_schema_length=None).write_parquet(
            temporary, compression="zstd", statistics=True
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest(), True
