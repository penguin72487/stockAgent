"""Receipt-pinned access to TAIFEX customized-futures daily source rows.

These are daily observations, not prices executable at a decision clock or
proof of historical contract admission.  In particular, settlement prices and
zero-volume rows must never be promoted to trade prints.
"""
from __future__ import annotations

import csv
from datetime import date, datetime
import re
from pathlib import Path
from typing import Iterator

from downloader.artifact_io import sha256_file


CUSTOMIZED_DAILY_SOURCE_CONTRACT = "taifex_customized_futures_daily_source_v1"
_REQUIRED = frozenset({"交易日期", "契約", "到期日", "開盤價", "最高價", "最低價", "收盤價", "成交量", "結算價"})


def iter_customized_daily_rows(
    source: str | Path, *, expected_sha256: str, start: date, end: date,
) -> Iterator[dict[str, object]]:
    """Read all source rows and retain their original text and row identity.

    Exhaustion validates the content hash a second time.  An empty iterator
    means the response had only a valid header, not that no contracts existed.
    """
    path = Path(source)
    if path.is_symlink() or not path.is_file():
        raise ValueError("customized daily source must be a regular file")
    if not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
        raise ValueError("an explicit customized daily source digest is required")
    if type(start) is not date or type(end) is not date or start > end:
        raise ValueError("invalid customized daily source window")
    if sha256_file(path) != expected_sha256:
        raise ValueError("customized daily source digest mismatch")
    with path.open("r", encoding="cp950", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or not _REQUIRED.issubset(reader.fieldnames):
            raise ValueError("unsupported customized daily source header")
        if len(reader.fieldnames) != len(set(reader.fieldnames)):
            raise ValueError("duplicate customized daily source header")
        for line, row in enumerate(reader, start=2):
            extra = row.pop(None, None)
            if extra and any(value.strip() for value in extra):
                raise ValueError(f"unexpected customized daily source columns at line {line}")
            raw = {key: value if value is not None else "" for key, value in row.items()}
            try:
                trading_date = datetime.strptime(raw["交易日期"].strip(), "%Y/%m/%d").date()
                expiry = datetime.strptime(raw["到期日"].strip(), "%Y%m%d").date()
            except ValueError as exc:
                raise ValueError(f"invalid customized daily source date at line {line}") from exc
            product = raw["契約"].strip()
            if not re.fullmatch(r"[A-Z0-9]{2,12}", product) or not start <= trading_date <= end:
                raise ValueError(f"invalid customized daily source identity at line {line}")
            yield {
                "source_path": str(path.resolve()), "source_line": line,
                "trading_date": trading_date, "product": product,
                "expiry_date": expiry, "raw": raw,
            }
    if sha256_file(path) != expected_sha256:
        raise ValueError("customized daily source changed while reading")
