"""Read-only exact metadata overlap, not donated newspaper bodies.

Some retained ETtoday attribution failures have issuer-coded titles. Compare
those *candidates* with already acquired FinLab material disclosures before
planning another downloader. Symbol/day/title equality does not prove equal
bodies, publisher rights, a verified release time or training eligibility.
No SDK, publisher requests, queue writes or source-body exports are used.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from contextlib import closing
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import sys
import time

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from downloader.artifact_io import atomic_write_bytes, sha256_file
from downloader.news_fulltext_ettoday import ettoday_request_identity
from downloader.parquet_integrity import parquet_receipt_error, signature
from stockagent.data.finlab_acquisition_contract import safe_stem


CONTRACT_VERSION = 2
DATASET = "important_info_announcement"
_TITLE = re.compile(r"^([^()（）\r\n]{1,80})(?:\(([0-9]{4,6})\)|（([0-9]{4,6})）)[ \t]+(\S[^\r\n]*)$")
_DAY = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_METADATA_BRAND_SUFFIXES = (" - ETtoday財經雲", " - ETtoday新聞雲", " - ETtoday", " - ettoday.net")
_FIELDS = ("url", "title", "metadata_order_day", "state", "error_code", "last_checked_at_utc")


def issuer_metadata_key(row: dict) -> tuple[str, str, str] | None:
    """Literal native URL/title and actual metadata day, never a URL/date fix.

    The four exact publisher suffixes occur in the retained metadata, not in
    the issuer subject. Remove at most one for comparison, retaining the whole
    original title and the removed suffix in the audit. No punctuation,
    whitespace, entities or issuer names are normalized into an equality.
    """
    identity = ettoday_request_identity(row.get("url"), requested=True)
    title, day = row.get("title"), row.get("metadata_order_day")
    if (identity is None or not isinstance(title, str) or not isinstance(day, str) or not _DAY.fullmatch(day)):
        return None
    try:
        date.fromisoformat(day)
    except ValueError:
        return None
    suffix = metadata_brand_suffix(title)
    match = _TITLE.fullmatch(title[:-len(suffix)] if suffix else title)
    if match is None:
        return None
    return match[2] or match[3], day, match[4]


def metadata_brand_suffix(title: str) -> str | None:
    return next((suffix for suffix in _METADATA_BRAND_SUFFIXES if title.endswith(suffix)), None)


def missing_heads(root: Path) -> tuple[list[dict], str]:
    """One short read-only WAL transaction, only truly body-less failures."""
    path = root.resolve() / "queue.sqlite3"
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        rows = [dict(row) for row in db.execute(
            "SELECT " + ",".join(_FIELDS) + " FROM articles WHERE provider='ettoday' "
            "AND state='article_attribution_not_verified' "
            "AND body_sha256 IS NULL AND parquet_path IS NULL ORDER BY url")]
        observed = datetime.now(timezone.utc).isoformat()
    return rows, observed


def verified_finlab(root: Path) -> tuple[Path, Path, dict, tuple, tuple, str]:
    """Reuse the collector's receipt/file contract; footer alone is not proof."""
    root = root.resolve()
    receipt_path = root / "receipts" / (safe_stem(DATASET) + ".json")
    if receipt_path.is_symlink() or not receipt_path.resolve().is_relative_to(root):
        raise ValueError("unsafe_finlab_receipt")
    receipt_signature = signature(receipt_path)
    receipt_bytes = receipt_path.read_bytes()
    receipt = json.loads(receipt_bytes)
    relative = Path(str(receipt.get("parquet_path") or ""))
    if (receipt.get("dataset") != DATASET
            or receipt.get("status") != "downloaded_unverified_for_pit"
            or type(receipt.get("rows_with_values")) is not int or receipt["rows_with_values"] <= 0
            or relative.parts[:1] != ("datasets",)):
        raise ValueError("invalid_finlab_disclosure_receipt")
    error = parquet_receipt_error(root, receipt)
    if error:
        raise ValueError("finlab_" + error)
    if signature(receipt_path) != receipt_signature:
        raise ValueError("finlab_receipt_changed_during_verification")
    path = root / relative
    return path, receipt_path, receipt, signature(path), receipt_signature, hashlib.sha256(receipt_bytes).hexdigest()


def match_local_records(path: Path, keys: set[tuple[str, str, str]], *, batch_rows: int = 8192) -> dict:
    """O(local rows + missing heads), bounded batches, hash only matched bodies."""
    if type(batch_rows) is not int or not 1 <= batch_rows <= 65536:
        raise ValueError("batch_rows_out_of_bounds")
    parquet = pq.ParquetFile(path)
    columns = ["source_index", "symbol", "date", "title", "info"]
    if not set(columns).issubset(parquet.schema_arrow.names):
        raise ValueError("missing_finlab_disclosure_columns")
    if not pa.types.is_timestamp(parquet.schema_arrow.field("date").type):
        raise ValueError("finlab_event_time_is_not_timestamp")
    if not keys:
        return {}
    wanted_titles = pa.array(sorted({key[2] for key in keys}), type=pa.string())
    matches = defaultdict(list)
    for batch in parquet.iter_batches(batch_size=batch_rows, columns=columns, use_threads=False):
        # Native membership removes irrelevant timestamps/strings before Python
        # decoding. It is the same exact key comparison, not a reduced scope.
        batch = batch.filter(pc.is_in(batch.column(3), value_set=wanted_titles))
        values = [batch.column(i) for i in range(len(columns))]
        # Do not decode/export the whole text column. Read matching scalars only.
        for i, (symbol, event, title) in enumerate(zip(
                values[1].to_pylist(), values[2].to_pylist(), values[3].to_pylist())):
            if not isinstance(event, datetime) or not isinstance(symbol, str) or not isinstance(title, str):
                continue
            key = symbol, event.date().isoformat(), title
            if key not in keys:
                continue
            info = values[4][i].as_py()
            if info is not None and not isinstance(info, str):
                raise ValueError("finlab_disclosure_info_is_not_text")
            matches[key].append({
                "source_index": values[0][i].as_py(), "symbol": symbol,
                "provider_event_time_literal": event.isoformat(),
                "provider_event_timezone": parquet.schema_arrow.field("date").type.tz,
                "title_sha256": hashlib.sha256(title.encode()).hexdigest(),
                "info_chars": len(info) if info is not None else None,
                "info_sha256": hashlib.sha256(info.encode()).hexdigest() if info is not None else None,
                "nonempty_info": bool(info and info.strip()),
            })
    return dict(matches)


def audit(news_root: Path, finlab_root: Path, *, batch_rows: int = 8192) -> dict:
    started = time.monotonic()
    heads, observed = missing_heads(news_root)
    candidates = [(row, key) for row in heads if (key := issuer_metadata_key(row)) is not None]
    path, receipt_path, receipt, file_before, receipt_before, receipt_sha = verified_finlab(finlab_root)
    local = match_local_records(path, {key for _, key in candidates}, batch_rows=batch_rows)
    if signature(path) != file_before or signature(receipt_path) != receipt_before:
        raise ValueError("finlab_source_changed_during_audit")
    rows, states, years = [], Counter(), defaultdict(Counter)
    for head, key in candidates:
        records = local.get(key, [])
        state = ("no_local_match" if not records else "ambiguous_local_records" if len(records) > 1 else
                 "single_local_record_nonempty_info" if records[0]["nonempty_info"] else "single_local_record_empty_info")
        states[state] += 1
        years[key[1][:4]][state] += 1
        rows.append({**head, "symbol_candidate": key[0], "literal_title_suffix": key[2],
                     "removed_metadata_publisher_suffix": metadata_brand_suffix(head["title"]),
                     "match_state": state, "local_record_candidates": records})
    source = {key: receipt.get(key) for key in (
        "dataset", "status", "rows", "rows_with_values", "sha256", "parquet_path",
        "fetched_at_utc", "source_checked_at_utc", "first_event_at", "last_event_at",
        "publication_time_status", "index_semantics", "redistribution", "account_entitlement")}
    # Preserve absence/unknown policy values. Matching cannot grant any use.
    return {
        "contract_version": CONTRACT_VERSION, "accepted": True,
        "observed_at_utc": observed, "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": time.monotonic() - started,
        "code_sha256": sha256_file(Path(__file__)),
        "news_root": str(news_root.resolve()), "finlab_root": str(finlab_root.resolve()),
        "finlab_receipt_path": str(receipt_path), "finlab_receipt_sha256": receipt_sha,
        "finlab_source_receipt": source,
        "queue_snapshot_sha256": hashlib.sha256(json.dumps(heads, ensure_ascii=False,
            sort_keys=True, separators=(",", ":")).encode()).hexdigest(),
        "counts": {"missing_attribution_heads": len(heads), "issuer_title_candidates": len(candidates),
                   "other_missing_attribution_heads": len(heads) - len(candidates),
                   "distinct_candidate_keys": len({key for _, key in candidates}),
                   "match_states": dict(states),
                   "candidate_match_states_by_metadata_year": {key: dict(value) for key, value in sorted(years.items())}},
        "comparison_basis": "literal native title issuer-code prefix and at most one recorded publisher suffix removed; exact symbol/metadata day/subject; no fuzzy matching",
        "candidate_classification_only": True, "body_equivalence_verified": False,
        "publisher_identity_or_rights_granted": False, "publication_clock_verified_by_match": False,
        "all_history_complete": False, "news_training_injection": False,
        "publisher_requests": 0, "queue_writes": 0, "source_body_text_exported": False,
        "rows": rows,
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--news-root", type=Path, default=Path("data_news_fulltext"))
    parser.add_argument("--finlab-root", type=Path, default=Path("data_finlab"))
    parser.add_argument("--output", type=Path, required=True, help="new private evidence JSON, never a source file")
    parser.add_argument("--batch-rows", type=int, default=8192)
    args = parser.parse_args(argv)
    output = args.output.resolve()
    if (args.output.is_symlink() or output.exists()
            or any(output.is_relative_to(root.resolve()) for root in (args.news_root, args.finlab_root))):
        parser.error("output must be new and outside both source roots")
    result = audit(args.news_root, args.finlab_root, batch_rows=args.batch_rows)
    atomic_write_bytes(output, (json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode(),
                       durable=True, mode=0o600)
    print(json.dumps({key: result[key] for key in ("accepted", "observed_at_utc", "counts", "elapsed_seconds")}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
