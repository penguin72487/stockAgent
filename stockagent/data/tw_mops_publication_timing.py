"""Research-only publication-date candidates for locally imported MOPS XBRL.

The source ZIP contains financial facts and often a board approval date, but
neither proves when that exact XBRL document first appeared on MOPS. Keep the
candidate date separate from the fact table's ``published_at_utc`` field.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import hashlib
import json
from pathlib import Path
import re
import unicodedata

import polars as pl


BOARD_CONCEPT = "DateAndProceduresOfAuthorisationForIssueOfFinancialStatements"
LEGACY_DEADLINE_SOURCE = (
    "https://twse-regulation.twse.com.tw/TW/law/DAT06_print.aspx?"
    "FLCODE=FL007250&FLDATE=20100520&LSER=001"
)
MODERN_DEADLINE_SOURCE = "https://www.twse.com.tw/docs1/data01/set/public_html/1020500225.htm"
_DATE_PATTERN = re.compile(
    r"([0-9零〇○Ｏ一二三四五六七八九十百千]{2,8})\s*年\s*"
    r"([0-9零〇○Ｏ一二三四五六七八九十]{1,4})\s*月\s*"
    r"([0-9零〇○Ｏ一二三四五六七八九十]{1,4})\s*日"
)
_DIGITS = {char: index for index, char in enumerate("零一二三四五六七八九")}
_DIGITS.update({"〇": 0, "○": 0, "Ｏ": 0})


def _number(text: str) -> int | None:
    value = unicodedata.normalize("NFKC", text).replace("○", "〇")
    if value.isdecimal():
        return int(value)
    if not any(unit in value for unit in "十百千"):
        digits = "".join(str(_DIGITS[char]) for char in value if char in _DIGITS)
        return int(digits) if len(digits) == len(value) and digits else None
    total = current = 0
    for char in value:
        if char in _DIGITS:
            current = _DIGITS[char]
        elif char in "十百千":
            total += (current or 1) * {"十": 10, "百": 100, "千": 1000}[char]
            current = 0
        else:
            return None
    return total + current


def board_authorization_date(text: str, period_end: date) -> date | None:
    """Accept one plausible post-period board date; ambiguous notes stay unknown."""
    candidates: set[date] = set()
    for match in _DATE_PATTERN.finditer(unicodedata.normalize("NFKC", text)):
        numbers = [_number(part) for part in match.groups()]
        if any(part is None for part in numbers):
            continue
        year, month, day = numbers
        year = year + 1911 if year < 1912 else year
        try:
            candidate = date(year, month, day)
        except ValueError:
            continue
        if period_end < candidate <= period_end + timedelta(days=240):
            candidates.add(candidate)
    return next(iter(candidates)) if len(candidates) == 1 else None


def _period_end(period: str) -> date:
    match = re.fullmatch(r"(20\d{2})Q([1-4])", period)
    if not match:
        raise ValueError(f"invalid MOPS period: {period}")
    year, quarter = int(match.group(1)), int(match.group(2))
    return date(year, (3, 6, 9, 12)[quarter - 1], (31, 30, 30, 31)[quarter - 1])


def _deadline_proxy(period: str, standard: str) -> tuple[date, str]:
    end = _period_end(period)
    quarter = int(period[-1])
    if standard == "tw_gaap":
        if quarter == 4:
            return date(end.year + 1, 4, 30), LEGACY_DEADLINE_SOURCE
        return end + timedelta(days=75 if quarter == 2 else 45), LEGACY_DEADLINE_SOURCE
    if quarter == 4:
        return date(end.year + 1, 3, 31), MODERN_DEADLINE_SOURCE
    if quarter == 2:
        return date(end.year, 8, 31), MODERN_DEADLINE_SOURCE
    return end + timedelta(days=45), MODERN_DEADLINE_SOURCE


def _empirical_day(days: list[date]) -> date | None:
    if len(days) < 30:
        return None
    ordered = sorted(days)
    return ordered[(3 * (len(ordered) - 1)) // 4]


def _source_receipts(root: Path) -> list[dict[str, object]]:
    receipts = []
    for path in sorted((root / "raw").glob("*/*/*.json")):
        receipt = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(receipt, dict):
            raise ValueError(f"invalid MOPS receipt: {path}")
        period = str(receipt["period"])
        standard = str(receipt["standard"])
        digest = str(receipt["archive_sha256"])
        if (standard not in {"ifrs", "tw_gaap"}
                or not re.fullmatch(r"[0-9a-f]{64}", digest)
                or path != root / "raw" / standard / period / f"{digest}.json"
                or not Path(str(receipt["raw_path"])).is_file()
                or not Path(str(receipt["facts_path"])).is_file()):
            raise ValueError(f"MOPS receipt paths or digest do not match: {path}")
        receipts.append(receipt)
    if not receipts:
        raise ValueError("no imported MOPS receipts")
    return receipts


def source_fingerprint(receipts: list[dict[str, object]]) -> str:
    entries = sorted((str(row["archive_sha256"]), str(row["facts_sha256"]),
                      int(row["fact_count"])) for row in receipts)
    return hashlib.sha256(json.dumps(entries, separators=(",", ":")).encode()).hexdigest()


def build_publication_candidates(root: Path) -> tuple[pl.DataFrame, dict[str, object]]:
    """Create a document-grain lookup covering every normalized fact row."""
    receipts = _source_receipts(root)
    records: list[dict[str, object]] = []
    expected_facts = 0
    for receipt in receipts:
        path = Path(str(receipt["facts_path"]))
        period = str(receipt["period"])
        standard = str(receipt["standard"])
        digest = str(receipt["archive_sha256"])
        end = _period_end(period)
        scan = pl.scan_parquet(path)
        docs = scan.group_by("source_member", "document_sha256").agg(
            pl.col("entity_identifier").drop_nulls().first().alias("company_id"),
            pl.len().alias("fact_rows"),
        ).collect(engine="streaming")
        notes = scan.filter(pl.col("concept").str.ends_with(BOARD_CONCEPT)).select(
            "source_member", "document_sha256", "raw_value",
        ).unique().collect(engine="streaming")
        note_map: dict[tuple[str, str], set[str]] = {}
        for note in notes.iter_rows(named=True):
            key = (str(note["source_member"]), str(note["document_sha256"]))
            if note["raw_value"]:
                note_map.setdefault(key, set()).add(str(note["raw_value"]))
        period_rows: list[dict[str, object]] = []
        board_days: list[date] = []
        for doc in docs.iter_rows(named=True):
            member = str(doc["source_member"])
            doc_sha = str(doc["document_sha256"])
            note_values = note_map.get((member, doc_sha), set())
            candidates = {day for text in note_values
                          if (day := board_authorization_date(text, end)) is not None}
            board_day = next(iter(candidates)) if len(candidates) == 1 else None
            if board_day:
                board_days.append(board_day)
            company_id = doc["company_id"]
            if not company_id:
                match = re.search(r"-([0-9A-Za-z]{4,8})-" + re.escape(period), member)
                company_id = match.group(1) if match else None
            note_sha = (hashlib.sha256(next(iter(note_values)).encode()).hexdigest()
                        if board_day and len(note_values) == 1 else None)
            period_rows.append({
                "standard": standard, "period": period,
                "archive_sha256": digest, "source_member": member,
                "document_sha256": doc_sha, "company_id": company_id,
                "fact_rows": int(doc["fact_rows"]),
                "report_period_end": end.isoformat(),
                "board_authorization_on": board_day.isoformat() if board_day else None,
                "board_note_sha256": note_sha,
            })
        habit = _empirical_day(board_days)
        deadline, deadline_url = _deadline_proxy(period, standard)
        for record in period_rows:
            board_on = record["board_authorization_on"]
            if board_on:
                candidate_on = str(board_on)
                basis = "estimated_from_issuer_board_authorization"
                evidence = f"{path}#source_member={record['source_member']}"
            elif habit:
                candidate_on = habit.isoformat()
                basis = "estimated_same_quarter_board_date_p75"
                evidence = f"{path}#board_date_sample_count={len(board_days)}"
            else:
                candidate_on = deadline.isoformat()
                basis = "estimated_historical_filing_deadline_proxy"
                evidence = deadline_url
            record.update({
                "publication_date_taipei": candidate_on,
                "publication_clock_taipei": None,
                "publication_time_basis": basis,
                "publication_evidence_ref": evidence,
                "historical_point_in_time": False,
                "training_eligible": False,
                "exact_filing_time_lookup_status": "pending_official_filing_evidence",
            })
        records.extend(period_rows)
        expected_facts += int(receipt["fact_count"])
    frame = pl.DataFrame(records).sort("standard", "period", "source_member")
    if frame.get_column("fact_rows").sum() != expected_facts:
        raise ValueError("publication candidate document counts do not cover all XBRL facts")
    by_basis = {str(row["publication_time_basis"]): int(row["len"])
                for row in frame.group_by("publication_time_basis").len().iter_rows(named=True)}
    summary = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "status": "research_candidates_only",
        "archive_versions": len(receipts),
        "document_count": frame.height,
        "fact_rows_covered": expected_facts,
        "source_fingerprint_sha256": source_fingerprint(receipts),
        "publication_time_basis_counts": by_basis,
        "exact_filing_time_documents": 0,
        "historical_point_in_time": False,
        "training_eligible": False,
    }
    return frame, summary
