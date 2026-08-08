"""Shared receipt primitives for official TAIFEX daily-history downloaders."""

from __future__ import annotations

from datetime import date, timedelta
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
from typing import Iterator
from urllib import parse, request
import zipfile


def parse_iso_date(value: str) -> date:
    return date.fromisoformat(value)


def sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def month_ranges(start: date, end: date) -> Iterator[tuple[date, date]]:
    current = start.replace(day=1)
    while current <= end:
        next_month = (
            date(current.year + 1, 1, 1)
            if current.month == 12
            else date(current.year, current.month + 1, 1)
        )
        yield max(start, current), min(end, next_month - timedelta(days=1))
        current = next_month


def download_taifex_attachment(
    url: str,
    payload: dict[str, str],
    target: Path,
    *,
    attempts: int,
    request_interval: float,
    user_agent: str,
) -> Path:
    """Download one immutable TAIFEX attachment with atomic promotion."""

    if target.is_file() and target.stat().st_size > 0:
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    encoded = parse.urlencode(payload).encode("ascii")
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            http_request = request.Request(
                url,
                data=encoded,
                headers={
                    "User-Agent": user_agent,
                    "Content-Type": "application/x-www-form-urlencoded",
                },
                method="POST",
            )
            with request.urlopen(http_request, timeout=120) as response:
                body = response.read()
                disposition = str(response.headers.get("Content-Disposition") or "")
            if len(body) < 100 or "attachment" not in disposition.casefold():
                raise RuntimeError("TAIFEX response was not a downloadable attachment")
            with tempfile.NamedTemporaryFile(
                dir=target.parent,
                prefix=target.name + ".",
                suffix=".tmp",
                delete=False,
            ) as handle:
                temporary = Path(handle.name)
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
            temporary.replace(target)
            return target
        except Exception as exc:
            last_error = exc
            if attempt >= attempts:
                break
            time.sleep(max(request_interval, float(attempt)))
    raise RuntimeError(
        f"failed to download {target.name} after {attempts} attempts"
    ) from last_error


def validate_taifex_receipt(path: Path) -> None:
    if path.suffix.lower() == ".zip":
        if not zipfile.is_zipfile(path):
            raise ValueError(f"downloaded annual file is not a ZIP: {path}")
        with zipfile.ZipFile(path) as archive:
            members = [
                name for name in archive.namelist() if name.lower().endswith(".csv")
            ]
            if not members:
                raise ValueError(f"annual ZIP contains no CSV: {path}")
        return
    if path.suffix.lower() == ".csv":
        with path.open("rb") as handle:
            header = handle.readline()
        if b"," not in header or len(header) < 40:
            raise ValueError(f"downloaded range file has no CSV header: {path}")
        return
    raise ValueError(f"unsupported receipt: {path}")


def atomic_write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


__all__ = [
    "atomic_write_json",
    "download_taifex_attachment",
    "month_ranges",
    "parse_iso_date",
    "sha256_path",
    "validate_taifex_receipt",
]
