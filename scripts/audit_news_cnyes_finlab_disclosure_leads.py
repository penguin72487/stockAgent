"""Find *unverified* disclosure leads in an already acquired FinLab version.

This read-only research audit never treats a lead as an article, filing identity,
or publication time. It does not access a publisher or the production queue.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import sys
import time
import unicodedata
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from downloader.artifact_io import atomic_write_bytes
from downloader.parquet_integrity import signature
from scripts.audit_news_issuer_overlap import verified_finlab
from scripts.plan_news_cnyes_disclosure_candidates import is_43_1_candidate, valid_request_scoped_provenance


TAIPEI = ZoneInfo("Asia/Taipei")
SYMBOL = re.compile(r"[0-9]{4,6}\Z")
SELF_ARTICLE = re.compile(r"(?:證券交易法|證交法)自第(?:43|四十三)(?:條)?(?:之|-)(?:1(?!\d)|一(?![〇零一二三四五六七八九十百千]))")
MAX_PAIRS = 200_000  # Fail rather than silently emit a truncated audit.


def _read_input(path: Path) -> tuple[dict, str, tuple]:
    if path.is_symlink():
        raise ValueError("symlink_input")
    before = signature(path)
    raw = path.read_bytes()
    if signature(path) != before:
        raise ValueError("input_changed_during_read")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("input_is_not_an_object")
    return payload, hashlib.sha256(raw).hexdigest(), before


def _explicit_43_1(title: object) -> bool:
    if is_43_1_candidate(title):
        return True
    # A previously verified literal CNYES title contains "交易法自第43條之1".
    # This recognizes the observed spelling only; never rewrite source text.
    return (isinstance(title, str) and len(title) <= 4096
            and bool(SELF_ARTICLE.search(_normalized_title(title))))


def candidates(old: dict, new: dict, old_sha: str) -> list[dict]:
    """Validate both earlier worklists and retain their literal identity/day."""
    if (old.get("contract_version") != 1
            or new.get("contract") not in {"cnyes_43_1_incremental_candidates_v1",
                                           "cnyes_43_1_incremental_candidates_v2",
                                           "cnyes_43_1_incremental_candidates_v3"}
            or not isinstance(new.get("input_sha256"), list)
            or len(new["input_sha256"]) != 3 or new["input_sha256"][2] != old_sha):
        raise ValueError("worklist_contract_or_chain_mismatch")
    rows, seen = [], set()
    for source, report in (("prior", old), ("incremental", new)):
        groups = report.get("groups")
        if (not isinstance(groups, list) or len(groups) != report.get("candidate_days")
                or report.get("source_requests") != 0 or report.get("production_queue_writes") != 0
                or report.get("historical_gaps_retired") not in (False, 0)):
            raise ValueError("invalid_worklist_scope")
        count = 0
        for group in groups:
            if not isinstance(group, dict) or not isinstance(group.get("candidates"), list):
                raise ValueError("invalid_worklist_group")
            day_text = group.get("cnyes_publication_day")
            try:
                day = date.fromisoformat(day_text)
            except (TypeError, ValueError) as exc:
                raise ValueError("invalid_publisher_day") from exc
            if day.isoformat() != day_text:
                raise ValueError("invalid_publisher_day")
            for row in group["candidates"]:
                if not isinstance(row, dict):
                    raise ValueError("invalid_worklist_row")
                number, symbol, title = (row.get("cnyes_news_id"), row.get("issuer_symbol_candidate"),
                                         row.get("cnyes_subject_literal"))
                if (type(number) is not int or number <= 0 or number in seen
                        or row.get("cnyes_url") != f"https://news.cnyes.com/news/id/{number}"
                        or not isinstance(symbol, str) or not SYMBOL.fullmatch(symbol)
                        or not _explicit_43_1(title)
                        or row.get("official_record_identity_verified") is not False
                        or row.get("official_release_clock_verified") is not False):
                    raise ValueError("invalid_or_duplicate_candidate")
                if source == "incremental":
                    if (new.get("contract") in {"cnyes_43_1_incremental_candidates_v2",
                                                "cnyes_43_1_incremental_candidates_v3"}
                            and not valid_request_scoped_provenance(row.get("source_observations"),
                                row.get("selected_response_sha256"), row.get("selected_source_artifact_path"),
                                **({"selected_receipt": row.get("selected_source_receipt_path"),
                                    "selected_request": row.get("selected_source_request_sha256"),
                                    "require_details": True}
                                   if new.get("contract") == "cnyes_43_1_incremental_candidates_v3" else {}))):
                        raise ValueError("invalid_candidate_provenance")
                    epoch = row.get("cnyes_published_epoch")
                    if type(epoch) is not int:
                        raise ValueError("invalid_candidate_epoch")
                    try:
                        epoch_day = datetime.fromtimestamp(epoch, TAIPEI).date()
                    except (ValueError, OverflowError, OSError) as exc:
                        raise ValueError("invalid_candidate_epoch") from exc
                    if epoch_day != day:
                        raise ValueError("candidate_epoch_day_mismatch")
                seen.add(number)
                count += 1
                rows.append({"news_id": number, "symbol": symbol, "day": day,
                             "subject": title, "normalized_subject": _normalized_title(title),
                             "worklist": source})
        if count != report.get("candidate_heads"):
            raise ValueError("worklist_head_count_mismatch")
    return rows


def _normalized_title(value: str) -> str:
    # Similarity is a lead only; it is never used to identify a filing or body.
    return re.sub(r"\s+", "", unicodedata.normalize("NFKC", value))


def scan(path: Path, rows: list[dict], *, batch_rows: int = 65_536) -> dict:
    if type(batch_rows) is not int or not 1 <= batch_rows <= 65_536:
        raise ValueError("batch_rows_out_of_bounds")
    parquet = pq.ParquetFile(path)
    columns = ("source_index", "symbol", "date", "title")
    if not set(columns).issubset(parquet.schema_arrow.names):
        raise ValueError("missing_finlab_disclosure_columns")
    event_type = parquet.schema_arrow.field("date").type
    if not pa.types.is_timestamp(event_type) or event_type.tz is not None:
        raise ValueError("unexpected_finlab_event_time_type")
    by_symbol = defaultdict(list)
    by_symbol_day = defaultdict(list)
    for row in rows:
        by_symbol[row["symbol"]].append(row)
        by_symbol_day[(row["symbol"], row["day"])].append(row)
    wanted = pa.array(sorted(by_symbol), type=pa.string())
    same_day_heads, matched_heads, pairs = set(), set(), []
    selected_rows = matching_legal_rows = 0
    offset = 0
    for batch in parquet.iter_batches(batch_size=batch_rows, columns=list(columns), use_threads=False):
        indices = pc.indices_nonzero(pc.fill_null(pc.is_in(pc.cast(batch.column(1), pa.string()), value_set=wanted), False))
        selected = batch.take(indices)
        positions = indices.to_pylist()
        selected_rows += selected.num_rows
        for position, source_index, symbol, event, title in zip(
                positions, *(selected.column(i).to_pylist() for i in range(len(columns)))):
            if not isinstance(event, datetime):
                continue
            event_day = event.date()
            same_day_heads.update(row["news_id"] for row in by_symbol_day.get((symbol, event_day), []))
            if not _explicit_43_1(title):
                continue
            matching_legal_rows += 1
            title_sha = hashlib.sha256(title.encode("utf-8")).hexdigest()
            normalized = _normalized_title(title)
            for row in by_symbol[symbol]:
                if len(pairs) >= MAX_PAIRS:
                    raise ValueError("lead_pair_limit_exceeded")
                matched_heads.add(row["news_id"])
                pairs.append({"cnyes_news_id": row["news_id"], "cnyes_worklist": row["worklist"],
                    "cnyes_publication_day": row["day"].isoformat(), "symbol_candidate": symbol,
                    "finlab_row_ordinal": offset + position, "finlab_source_index": source_index,
                    "finlab_event_time_literal": event.isoformat(), "finlab_event_timezone": None,
                    "finlab_title_sha256": title_sha,
                    "finlab_day_minus_cnyes_day": (event_day - row["day"]).days,
                    "literal_subject_equal": title == row["subject"],
                    "nfkc_whitespace_subject_equal": normalized == row["normalized_subject"]})
        offset += batch.num_rows
    pairs.sort(key=lambda pair: (pair["cnyes_news_id"], abs(pair["finlab_day_minus_cnyes_day"]),
                                 pair["finlab_day_minus_cnyes_day"], pair["finlab_row_ordinal"]))
    same_day_legal = {p["cnyes_news_id"] for p in pairs if p["finlab_day_minus_cnyes_day"] == 0}
    within_7_legal = {p["cnyes_news_id"] for p in pairs if abs(p["finlab_day_minus_cnyes_day"]) <= 7}
    return {"counts": {"candidate_heads": len(rows), "candidate_symbols": len(by_symbol),
        "finlab_rows_scanned": offset, "finlab_same_symbol_rows": selected_rows,
        "finlab_same_symbol_explicit_43_1_title_rows": matching_legal_rows,
        "candidate_heads_with_any_same_symbol_same_day_row": len(same_day_heads),
        "candidate_heads_with_explicit_43_1_lead_any_day": len(matched_heads),
        "candidate_heads_with_explicit_43_1_lead_same_day": len(same_day_legal),
        "candidate_heads_with_explicit_43_1_lead_within_7_days": len(within_7_legal),
        "lead_pairs": len(pairs), "literal_subject_equal_pairs": sum(p["literal_subject_equal"] for p in pairs),
        "normalized_subject_equal_pairs": sum(p["nfkc_whitespace_subject_equal"] for p in pairs)},
        "lead_pairs": pairs}


def audit(old_path: Path, new_path: Path, finlab_root: Path, *, batch_rows: int = 65_536) -> dict:
    started = time.monotonic()
    old, old_sha, old_signature = _read_input(old_path)
    new, new_sha, new_signature = _read_input(new_path)
    rows = candidates(old, new, old_sha)
    path, receipt_path, receipt, source_signature, receipt_signature, receipt_sha = verified_finlab(finlab_root)
    result = scan(path, rows, batch_rows=batch_rows)
    if (signature(path) != source_signature or signature(receipt_path) != receipt_signature
            or signature(old_path) != old_signature or signature(new_path) != new_signature):
        raise ValueError("source_changed_during_audit")
    return {"contract": "cnyes_finlab_disclosure_leads_v1", "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": time.monotonic() - started,
        "old_worklist_sha256": old_sha, "new_worklist_sha256": new_sha,
        "finlab_receipt_sha256": receipt_sha, "finlab_parquet_sha256": receipt["sha256"],
        "finlab_dataset": receipt["dataset"], "finlab_receipt_status": receipt["status"],
        "finlab_first_event_at": receipt.get("first_event_at"), "finlab_last_event_at": receipt.get("last_event_at"),
        "finlab_row_count_from_receipt": receipt["rows"],
        "scope": "candidate symbol and explicit 43-1 subject in both sources; all FinLab event dates",
        "comparison_is_only_a_lead": True, "filing_identity_verified": False,
        "article_body_equivalence_verified": False, "publication_clock_verified": False,
        "current_queue_state_rechecked": False, "publisher_requests": 0,
        "production_queue_writes": 0, "body_text_read": False, "historical_gaps_retired": False,
        **result}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prior", required=True, type=Path)
    parser.add_argument("--incremental", required=True, type=Path)
    parser.add_argument("--finlab-root", default=Path("data_finlab"), type=Path)
    parser.add_argument("--news-root", default=Path("data_news_fulltext"), type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--batch-rows", default=65_536, type=int)
    args = parser.parse_args(argv)
    output = args.output.resolve()
    if (args.output.is_symlink() or output in (args.prior.resolve(), args.incremental.resolve())
            or any(output.is_relative_to(root.resolve()) for root in (args.finlab_root, args.news_root))):
        raise ValueError("unsafe_output")
    report = audit(args.prior, args.incremental, args.finlab_root, batch_rows=args.batch_rows)
    report["implementation_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    atomic_write_bytes(output, (json.dumps(report, ensure_ascii=False, indent=2) + "\n").encode(), mode=0o600)
    print(json.dumps({"output": str(output), "counts": report["counts"],
                      "elapsed_seconds": report["elapsed_seconds"]}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
