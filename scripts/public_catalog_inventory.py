"""Receipt-backed dataset discovery, not an inventory of downloaded history.

``catalog_inventory`` takes the repository root. It reads only the four
supported metadata catalogs under ``data_keyed_public_catalogs`` and never
contacts a provider. Advertised date ranges and Census vintages remain distinct
from local observation coverage. Current station/symbol rows are not datasets.
"""
from __future__ import annotations

from collections import Counter
from datetime import UTC, date, datetime, timedelta
import json
import os
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlsplit

from downloader.artifact_io import sha256_bytes
from downloader.download_keyed_public_catalogs import SPEC_BY_PROVIDER


CATALOG_PROVIDERS = ("noaa_cdo", "census", "nasa_firms", "bea")
MAX_OBJECT_BYTES = 64 * 1024 * 1024
MAX_RECEIPT_BYTES = 1024 * 1024
_IDENTIFIER = re.compile(r"[A-Za-z0-9_.-]{1,256}\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_EVIDENCE = "sha256_verified_sanitized_catalog; advertised_metadata_only; no_observation_history"


class _InvalidEvidence(ValueError):
    """Only internal fixed codes may be copied to a public issue row."""


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def _read_bounded(path: Path, storage: Path, maximum: int) -> bytes:
    if not path.resolve().is_relative_to(storage.resolve()):
        raise _InvalidEvidence("path_outside_storage")
    before = path.stat()
    if not path.is_file() or not 0 < before.st_size <= maximum:
        raise _InvalidEvidence("invalid_file_size_or_type")
    with path.open("rb") as stream:
        if _identity(os.fstat(stream.fileno())) != _identity(before):
            raise _InvalidEvidence("file_changed")
        payload = stream.read(maximum + 1)
        if _identity(os.fstat(stream.fileno())) != _identity(before):
            raise _InvalidEvidence("file_changed")
    if (len(payload) != before.st_size or _identity(path.stat()) != _identity(before)
            or not path.resolve().is_relative_to(storage.resolve())):
        raise _InvalidEvidence("file_changed")
    return payload


def _json_object(payload: bytes) -> dict[str, Any]:
    try:
        result = json.loads(payload)
    except (ValueError, UnicodeError, RecursionError):
        raise _InvalidEvidence("invalid_json") from None
    if not isinstance(result, dict):
        raise _InvalidEvidence("invalid_object_shape")
    return result


def _capture_time(value: Any) -> str:
    if not isinstance(value, str):
        raise _InvalidEvidence("invalid_capture_time")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed > datetime.now(UTC) + timedelta(seconds=60):
            raise ValueError
    except (ValueError, OverflowError):
        raise _InvalidEvidence("invalid_capture_time") from None
    return parsed.astimezone(UTC).isoformat()


def _verified_payload(storage: Path, provider: str) -> tuple[dict[str, Any], dict[str, Any], str]:
    spec = SPEC_BY_PROVIDER[provider]
    receipt_path = storage / "receipts" / f"{provider}.json"
    try:
        receipt_bytes = _read_bounded(receipt_path, storage, MAX_RECEIPT_BYTES)
    except FileNotFoundError:
        raise _InvalidEvidence("receipt_missing") from None
    receipt = _json_object(receipt_bytes)
    if (receipt.get("provider") != provider or receipt.get("dataset") != spec.dataset
            or receipt.get("status") != "acquired" or receipt.get("kind") != "metadata_catalog"
            or receipt.get("history_complete") is not False
            or receipt.get("retained_payload_format") != "sanitized_json"):
        raise _InvalidEvidence("receipt_contract_mismatch")
    digest, size = receipt.get("object_sha256"), receipt.get("object_bytes")
    if not isinstance(digest, str) or not _DIGEST.fullmatch(digest):
        raise _InvalidEvidence("invalid_object_digest")
    if type(size) is not int or not 0 < size <= MAX_OBJECT_BYTES:
        raise _InvalidEvidence("invalid_object_size")
    # Exact producer namespace, not just a filename or a receipt-supplied path.
    relative = f"objects/{provider}/{digest}.json"
    if receipt.get("object_path") != relative:
        raise _InvalidEvidence("noncanonical_object_path")
    capture = _capture_time(receipt.get("observed_at_utc"))
    try:
        content = _read_bounded(storage / relative, storage, MAX_OBJECT_BYTES)
    except FileNotFoundError:
        raise _InvalidEvidence("object_missing") from None
    if len(content) != size:
        raise _InvalidEvidence("object_size_mismatch")
    if sha256_bytes(content) != digest:
        raise _InvalidEvidence("object_digest_mismatch")
    # A concurrent receipt change must not pair new metadata with old bytes.
    if _read_bounded(receipt_path, storage, MAX_RECEIPT_BYTES) != receipt_bytes:
        raise _InvalidEvidence("receipt_changed")
    return _json_object(content), receipt, capture


def _records(provider: str, payload: dict[str, Any]) -> list[Any]:
    if provider == "noaa_cdo":
        records = payload.get("results")
    elif provider == "census":
        records = payload.get("dataset")
    elif provider == "nasa_firms":
        records = payload.get("records")
    else:
        api = payload.get("BEAAPI")
        results = api.get("Results") if isinstance(api, dict) else None
        records = results.get("Dataset") if isinstance(results, dict) and "Error" not in results else None
    if not isinstance(records, list) or not records:
        raise _InvalidEvidence("missing_or_invalid_dataset_list")
    return records


def _text(value: Any, *, maximum: int = 4096) -> str:
    if (not isinstance(value, str) or not value.strip() or len(value) > maximum
            or any(ord(character) < 32 for character in value)):
        raise _InvalidEvidence("invalid_dataset_text")
    return value.strip()


def _dataset_id(value: Any) -> str:
    if not isinstance(value, str) or not _IDENTIFIER.fullmatch(value):
        raise _InvalidEvidence("missing_or_invalid_dataset_id")
    return value


def _advertised_dates(record: dict[str, Any], first_key: str, last_key: str) -> tuple[str, str]:
    first, last = record.get(first_key), record.get(last_key)
    try:
        if not isinstance(first, str) or not isinstance(last, str):
            raise ValueError
        start, end = date.fromisoformat(first), date.fromisoformat(last)
        if start.isoformat() != first or end.isoformat() != last or start > end:
            raise ValueError
    except ValueError:
        raise _InvalidEvidence("invalid_advertised_date_range") from None
    return first, last


def _census_documentation(value: Any) -> str:
    text = _text(value, maximum=2048)
    try:
        parsed = urlsplit(text)
        hostname = parsed.hostname or ""
        if (parsed.scheme not in {"https", "http"}
                or not (hostname == "census.gov" or hostname.endswith(".census.gov"))
                or parsed.username is not None or parsed.password is not None
                or parsed.port not in {None, 80, 443} or parsed.query or parsed.fragment):
            raise ValueError
    except ValueError:
        raise _InvalidEvidence("invalid_documentation_url") from None
    return text


def _catalog_row(provider: str, record: Any, capture: str) -> dict[str, Any]:
    if not isinstance(record, dict):
        raise _InvalidEvidence("invalid_dataset_record")
    spec = SPEC_BY_PROVIDER[provider]
    first = last = vintage = None
    documentation = spec.documentation
    if provider == "noaa_cdo":
        identifier = _dataset_id(record.get("id"))
        title = _text(record.get("name"))
        first, last = _advertised_dates(record, "mindate", "maxdate")
    elif provider == "nasa_firms":
        identifier = _dataset_id(record.get("data_id"))
        title = identifier  # Availability CSV provides no separate product title.
        first, last = _advertised_dates(record, "min_date", "max_date")
    elif provider == "bea":
        identifier = _dataset_id(record.get("DatasetName"))
        title = _text(record.get("DatasetDescription"))
    else:
        parts = record.get("c_dataset")
        if not isinstance(parts, list) or not parts or len(parts) > 32:
            raise _InvalidEvidence("missing_or_invalid_dataset_id")
        identifier = "/".join(_dataset_id(part) for part in parts)
        title = _text(record.get("title"))
        year = record.get("c_vintage")
        if year is not None:
            if type(year) not in {int, str} or not re.fullmatch(r"[1-9][0-9]{3}", str(year)):
                raise _InvalidEvidence("invalid_vintage")
            vintage = str(year)  # Not YYYY-01-01; no date precision was supplied.
        documentation = _census_documentation(record.get("c_documentationLink"))
    return {
        "provider": provider, "dataset_id": identifier, "title": title,
        "advertised_first": first, "advertised_last": last, "year_or_vintage": vintage,
        "upstream_cadence": spec.upstream_cadence,
        "documentation": documentation, "capture_at": capture,
        "history_downloaded": False, "evidence": _EVIDENCE,
    }


def catalog_inventory(root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return dataset rows plus safe issues; no API, writes, or success guessing.

    Rows describe metadata within a checksum-verified captured catalog. A
    missing BEA receipt, incomplete catalog, bad record or duplicate identity
    remains an issue even when other providers or records were usable.
    """
    storage = Path(root) / "data_keyed_public_catalogs"
    rows: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []
    for provider in CATALOG_PROVIDERS:
        try:
            payload, receipt, capture = _verified_payload(storage, provider)
            records = _records(provider, payload)
            if type(receipt.get("rows")) is not int or receipt["rows"] != len(records):
                raise _InvalidEvidence("receipt_row_count_mismatch")
        except _InvalidEvidence as exc:
            issues.append({"provider": provider, "code": str(exc)})
            continue
        except (OSError, ValueError, TypeError, OverflowError, RuntimeError):
            issues.append({"provider": provider, "code": "local_evidence_unreadable"})
            continue
        if receipt.get("catalog_complete") is not True:
            issues.append({"provider": provider, "code": "catalog_listing_incomplete"})
        parsed: list[tuple[int, dict[str, Any]]] = []
        for index, record in enumerate(records):
            try:
                parsed.append((index, _catalog_row(provider, record, capture)))
            except _InvalidEvidence as exc:
                issues.append({"provider": provider, "code": str(exc), "row_index": index})
        keys = Counter((row["dataset_id"], row["year_or_vintage"]) for _, row in parsed)
        for index, row in parsed:
            if keys[row["dataset_id"], row["year_or_vintage"]] != 1:
                issues.append({"provider": provider, "code": "duplicate_dataset_identity", "row_index": index})
            else:
                rows.append(row)
    rows.sort(key=lambda row: (row["provider"], row["dataset_id"], row["year_or_vintage"] or ""))
    return rows, issues
