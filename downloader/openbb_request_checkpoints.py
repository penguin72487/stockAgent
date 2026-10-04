"""Request-level resume owned by the archive, independent of provider calls."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import time
from typing import Any, Mapping

try:
    from downloader.openbb_archive_serialization import _write_json_atomic
except ModuleNotFoundError:  # Direct execution from downloader/.
    from openbb_archive_serialization import _write_json_atomic


def _request_checkpoint_path(checkpoint_dir: Path, url: str) -> tuple[Path, str]:
    """Return a credential-free content-addressed path for one GET request."""
    from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

    split = urlsplit(url)
    safe_query = [
        (key, value)
        for key, value in parse_qsl(split.query, keep_blank_values=True)
        if key.lower() not in {"api_key", "apikey", "token"}
    ]
    safe_url = urlunsplit(
        (
            split.scheme.lower(),
            split.netloc.lower(),
            split.path,
            urlencode(sorted(safe_query)),
            "",
        )
    )
    fingerprint = hashlib.sha256(safe_url.encode("utf-8")).hexdigest()
    return checkpoint_dir / f"{fingerprint}.json", fingerprint



def _load_request_checkpoint(
    checkpoint_dir: Path | None,
    url: str,
) -> dict[str, Any] | None:
    """Load a proven complete subrequest or quarantine a damaged checkpoint."""
    if checkpoint_dir is None:
        return None
    path, fingerprint = _request_checkpoint_path(checkpoint_dir, url)
    if not path.is_file():
        return None
    try:
        envelope = json.loads(path.read_text(encoding="utf-8"))
        if (
            not isinstance(envelope, Mapping)
            or int(envelope.get("schema_version") or 0) != 1
            or str(envelope.get("request_fingerprint") or "") != fingerprint
            or not isinstance(envelope.get("payload"), Mapping)
        ):
            raise ValueError("invalid request checkpoint envelope")
        return dict(envelope["payload"])
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        quarantine = path.with_name(f"{path.name}.corrupt.{time.time_ns()}")
        try:
            path.replace(quarantine)
        except OSError:
            pass
        return None



def _save_request_checkpoint(
    checkpoint_dir: Path | None,
    url: str,
    payload: Mapping[str, Any],
) -> None:
    """Atomically persist one successful provider subrequest for task resume."""
    if checkpoint_dir is None:
        return
    path, fingerprint = _request_checkpoint_path(checkpoint_dir, url)
    _write_json_atomic(
        path,
        {
            "schema_version": 1,
            "request_fingerprint": fingerprint,
            "payload": dict(payload),
        },
    )



def _clear_request_checkpoints(checkpoint_dir: Path | None) -> None:
    """Remove one exact task's generated request checkpoints after publication."""
    if checkpoint_dir is None or not checkpoint_dir.is_dir():
        return
    for path in checkpoint_dir.iterdir():
        if path.is_file() and (
            path.name.endswith(".json") or ".json.corrupt." in path.name
        ):
            path.unlink(missing_ok=True)
    try:
        checkpoint_dir.rmdir()
    except OSError:
        # Unknown files are preserved; never recursively delete a broad or
        # user-controlled directory merely because cleanup was incomplete.
        pass
