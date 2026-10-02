"""Adapt the installed SDK's validated .recent merge to a mandatory source check.

This does not implement another fetcher or merge algorithm. A plain cache hit
is not enough: lack of a fresh SDK publication marker falls back to a full SDK
fetch. No signed URL, token or account-scoped metadata is retained.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from importlib.metadata import version
import importlib
import math
from pathlib import Path


def checked_get(key: str, *, sdk=None) -> tuple[object, dict]:
    if sdk is None:
        from finlab import data as sdk

    ctx = getattr(sdk, "_default_context", None)
    storage = getattr(ctx, "_storage", None)
    flags = ("prefer_local_if_exists", "use_local_data_only", "force_cloud_download")
    compatible = (storage is not None and hasattr(storage, "set_time_expired")
                  and hasattr(storage, "get_time_created")
                  and all(hasattr(ctx, name) for name in flags)
                  and isinstance(getattr(ctx, "_loaded_datasets", None), set))
    if compatible and (getattr(ctx, "truncate_start", None) is not None
                       or getattr(ctx, "truncate_end", None) is not None
                       or getattr(ctx, "_projected_datasets", set())):
        raise ValueError("FinLab history refresh requires an unprojected full-history SDK context")
    if getattr(sdk, "__name__", None) == "finlab.data":
        universe = importlib.import_module("finlab.data.universe")
        if getattr(universe, "universe_stocks", set()):
            raise ValueError("FinLab history refresh cannot use a stock-universe projection")
    payload_bytes = 0
    segment_bytes = 0
    saw_payload_event = False

    def progress(event):
        nonlocal payload_bytes, segment_bytes, saw_payload_event
        # SDK byte counters are payload estimates, not account billing or an
        # HTTP-level ledger. Reset between delta/full segments; never double
        # count the cumulative complete event for the same segment.
        if not isinstance(event, dict) or event.get("dataset") != key:
            return
        phase, size = event.get("phase"), event.get("download_bytes")
        if (phase in {"download", "complete"} and isinstance(size, (int, float))
                and not isinstance(size, bool) and math.isfinite(size) and size >= 0):
            if size < segment_bytes:
                payload_bytes += segment_bytes
                segment_bytes = 0
            segment_bytes = max(segment_bytes, int(size))
            saw_payload_event = True
            if phase == "complete":
                payload_bytes += segment_bytes
                segment_bytes = 0

    saved_flags = {name: getattr(ctx, name) for name in flags} if compatible else {}
    old_expiry = storage.get_time_expired(key) if compatible else None
    marker = None
    forced = not compatible
    try:
        if compatible:
            for name in flags:
                setattr(ctx, name, False)
            ctx._loaded_datasets.discard(key)
            publications = getattr(storage, "_publications", None)
            if isinstance(publications, dict):
                publications.pop(key, None)
            storage.set_time_expired(key, datetime.now(UTC) - timedelta(seconds=1), save=False)
        frame = sdk.get(key, force_download=forced, progress="event", progress_callback=progress)
        if compatible:
            marker = getattr(storage, "_publications", {}).get(key)
            if not (isinstance(marker, tuple) and len(marker) == 2
                    and marker[0] == storage.get_time_created(key)):
                # Concurrent expiry refresh or a free/local-only cache policy
                # can bypass metadata. It must never count as a source check.
                forced = True
                frame = sdk.get(key, force_download=True, progress="event", progress_callback=progress)
                marker = getattr(storage, "_publications", {}).get(key)
        checked = datetime.now(UTC)
        evidence = {"source_checked_at_utc": checked.isoformat(),
                    "source_check_mode": "upstream_forced" if forced else "upstream_incremental",
                    "last_payload_bytes": payload_bytes + segment_bytes if saw_payload_event else None,
                    "payload_measurement_basis": "sdk_progress_payload_estimate_not_billing",
                    "incremental_adapter_version": 1, "normalization_contract_version": 1,
                    "provider_content_hash": None, "provider_hash_basis": None,
                    "finlab_sdk_version": version("finlab")}
        if compatible:
            expiry = storage.get_time_expired(key)
            if (isinstance(expiry, datetime) and expiry.tzinfo
                    and checked < expiry.astimezone(UTC) <= checked + timedelta(days=90)):
                evidence["next_source_check_at_utc"] = expiry.astimezone(UTC).isoformat()
            else:
                evidence["next_source_check_at_utc"] = None
        if (isinstance(marker, tuple) and len(marker) == 2
                and marker[0] == storage.get_time_created(key) and isinstance(marker[1], dict)):
            content_hash = marker[1].get("content_hash")
            if (isinstance(content_hash, str) and 8 <= len(content_hash) <= 64
                    and all(c in "0123456789abcdef" for c in content_hash)):
                evidence["provider_content_hash"] = content_hash
                evidence["provider_hash_basis"] = "sdk_validated_publication_v1"
        if sdk.__class__.__name__ == "module" and "/" not in key and "\\" not in key:
            from finlab import utils
            raw = Path(utils.get_tmp_dir()) / (key.replace(":", "#") + ".feather")
            if raw.is_file():
                evidence["last_full_source_bytes"] = raw.stat().st_size
        return frame, evidence
    except BaseException:
        if compatible and old_expiry is not None:
            storage.set_time_expired(key, old_expiry, save=False)
        raise
    finally:
        for name, value in saved_flags.items():
            setattr(ctx, name, value)
