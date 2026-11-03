"""Source-bound, opt-in holiday account-calendar research preparation.

This supplements calendar dates only. Product rules, risk marks, corporate
actions and all ledger values must be separately validated before admission.
"""
from __future__ import annotations

import ast
from dataclasses import dataclass
from datetime import date
import gzip
import hashlib
import json
from pathlib import Path
import re
import unicodedata

import polars as pl

from downloader.artifact_io import sha256_file

HOLIDAY_ACCOUNT_DATES_ABI = "taifex_holiday_broker_account_dates_research_v1"
_STAGE_SCRIPT = Path("artifacts/operations/futures-universe765-repair-20261027-v1/stage-holiday-account-dates-v1.py")


def _workspace_path(workspace: Path, relative: str | Path) -> Path:
    path = (workspace / relative).resolve()
    if not path.is_relative_to(workspace):
        raise ValueError("holiday account source escapes its workspace")
    return path


@dataclass(frozen=True, slots=True)
class HolidayAccountDatesSource:
    """Immutable reviewed date identities and independently pinned inputs."""

    events: tuple[tuple[date, str], ...]
    # Each tuple is (last market session, reopening session, account dates).
    # It is derived from the reviewed original, not an inferred holiday rule.
    periods: tuple[tuple[date, date, tuple[date, ...]], ...]
    fingerprint: str
    manifest_path: Path
    pinned_files: tuple[tuple[Path, str], ...]

    def verify(self) -> None:
        if sha256_file(self.manifest_path) != self.fingerprint:
            raise ValueError("holiday account manifest changed")
        for path, digest in self.pinned_files:
            if sha256_file(path) != digest:
                raise ValueError("holiday account source input changed")
        manifest = json.loads(self.manifest_path.read_text())
        periods = tuple(sorted((date.fromisoformat(item["last_session"]),
                                date.fromisoformat(item["reopening_session"]),
                                tuple(date.fromisoformat(day) for day in item["account_dates"]))
                               for item in manifest["periods"]))
        events = tuple(sorted((date.fromisoformat(day), item["content_sha256"])
                              for item in manifest["periods"] for day in item["account_dates"]))
        if periods != self.periods or events != self.events:
            raise ValueError("holiday account source identities changed")


def load_holiday_account_dates(source_dir: Path, *, workspace: Path) -> HolidayAccountDatesSource:
    """Verify the staged notice, reviewed implementation and original files."""
    workspace = workspace.resolve()
    source_dir = source_dir.resolve()
    if not source_dir.is_relative_to(workspace):
        raise ValueError("holiday account evidence must reside in its workspace")
    path = source_dir / "manifest.json"
    manifest = json.loads(path.read_text())
    if (manifest.get("contract") != HOLIDAY_ACCOUNT_DATES_ABI
            or manifest.get("auto_adopted_for_training") is not False):
        raise ValueError("invalid holiday account source contract")
    stage = _workspace_path(workspace, _STAGE_SCRIPT)
    if sha256_file(stage) != manifest.get("staging_script_sha256"):
        raise ValueError("holiday account staging implementation changed")
    # Compare every manifest period with the reviewed, pinned literal source
    # specification. A new manifest checksum alone must not allow an edited
    # date to masquerade as a valid source observation.
    tree = ast.parse(stage.read_text())
    assignments = [node for node in tree.body if isinstance(node, ast.Assign)
                   and any(isinstance(target, ast.Name) and target.id == "SPECS" for target in node.targets)]
    if len(assignments) != 1:
        raise ValueError("holiday account reviewed specification is missing")
    specs = ast.literal_eval(assignments[0].value)
    reviewed = {item[0]: item for item in specs}
    if len(reviewed) != len(specs) or len(manifest.get("periods", [])) != len(reviewed):
        raise ValueError("holiday account reviewed periods differ")
    pins = {stage: manifest["staging_script_sha256"]}
    events = []
    periods = []
    seen_periods = set()
    for period in manifest.get("periods", []):
        item = reviewed.get(period["indexed_date"])
        if (item is None or period["indexed_date"] in seen_periods
                or (period["last_session"], tuple(period["account_dates"]), period["reopening_session"])
                != (item[1], item[2], item[3])):
            raise ValueError("holiday account dates differ from the reviewed original")
        seen_periods.add(period["indexed_date"])
        raw = _workspace_path(workspace, period["raw_path"])
        parsed = _workspace_path(workspace, period["parsed_path"])
        if (sha256_file(raw) != period["raw_sha256"]
                or hashlib.sha256(gzip.decompress(raw.read_bytes())).hexdigest() != period["content_sha256"]
                or sha256_file(parsed) != period["parsed_sha256"]):
            raise ValueError("holiday account notice source changed")
        parsed_document = json.loads(parsed.read_text())
        if (parsed_document.get("source_url") != period["source_url"]
                or parsed_document.get("content_sha256") != period["content_sha256"]):
            raise ValueError("holiday account parse does not bind original")
        if item[4] is not None:
            if (period["content_sha256"] != item[4]
                    or period.get("extraction") != "first_page_visual_review_of_exact_pdf"):
                raise ValueError("holiday account scan differs from visual review")
        else:
            text = re.sub(r"\s+", "", unicodedata.normalize("NFKC", parsed_document.get("text") or ""))
            days_in_text = [date.fromisoformat(value) for value in period["account_dates"]]
            formatted = [f"{day.month}月{day.day}日" for day in days_in_text]
            if (period.get("extraction") != "parsed_pdf_sentence_and_session_bounds"
                    or f"{formatted[0]}及{formatted[1]}無交易" not in text
                    or "部位結帳作業" not in text):
                raise ValueError("holiday account dates differ from the parsed official notice")
        published = date.fromisoformat(period["indexed_date"])
        last = date.fromisoformat(period["last_session"])
        days = [date.fromisoformat(value) for value in period["account_dates"]]
        reopening = date.fromisoformat(period["reopening_session"])
        if (len(days) != 2 or not (published < last < days[0] < days[1] < reopening)
                or not all(day.year == last.year for day in days)
                or period.get("extraction") not in {
                    "first_page_visual_review_of_exact_pdf", "parsed_pdf_sentence_and_session_bounds"}):
            raise ValueError("invalid holiday account date provenance")
        pins[raw], pins[parsed] = period["raw_sha256"], period["parsed_sha256"]
        events.extend((day, period["content_sha256"]) for day in days)
        periods.append((last, reopening, tuple(days)))
    if (not events or len(events) != manifest.get("account_date_count")
            or len({day for day, _ in events}) != len(events)):
        raise ValueError("holiday account dates are empty or duplicated")
    source = HolidayAccountDatesSource(
        tuple(sorted(events)), tuple(sorted(periods)), sha256_file(path), path, tuple(sorted(pins.items())))
    source.verify()
    return source


def supplement_holiday_account_calendar(calendar: pl.DataFrame, source: HolidayAccountDatesSource) -> pl.DataFrame:
    """Add documented no-market account nodes without adding prices or fills.

    The input is the canonical market/cash-event calendar. Existing cash event
    source identities are kept intact; holiday-notice sources are separate.
    Known no-market dates conflicting with a market node fail closed.
    """
    source.verify()
    required = {"date", "accounting_only_date", "accounting_event_source_sha256s"}
    if not required <= set(calendar.columns) or calendar.is_empty():
        raise ValueError("a nonempty canonical account calendar is required")
    if (calendar["date"].null_count() or calendar["accounting_only_date"].null_count()
            or calendar["date"].is_duplicated().any()):
        raise ValueError("canonical account calendar must have unique dated classifications")
    if any(column in calendar.columns for column in (
            "holiday_account_notice_sha256", "holiday_account_calendar_contract", "holiday_account_source_fingerprint")):
        raise ValueError("holiday account dates were already composed")
    base = calendar.with_columns(pl.col("date").cast(pl.Date)).sort("date")
    first, last = base["date"].min(), base["date"].max()
    selected = [(day, digest) for day, digest in source.events if first <= day <= last]
    holiday = pl.DataFrame({
        "date": pl.Series([day for day, _ in selected], dtype=pl.Date),
        "holiday_account_notice_sha256": pl.Series([digest for _, digest in selected], dtype=pl.String),
    })
    conflicts = base.filter(~pl.col("accounting_only_date")).join(holiday.select("date"), on="date", how="semi")
    if conflicts.height:
        raise ValueError("holiday account source conflicts with an observed market session")
    extra = (holiday.join(base.select("date"), on="date", how="anti").select("date")
             .with_columns(pl.lit(True).alias("accounting_only_date"),
                           pl.lit([], dtype=pl.List(pl.String)).alias("accounting_event_source_sha256s")))
    result = (pl.concat([base, extra], how="diagonal_relaxed").join(holiday, on="date", how="left", validate="1:1")
              .with_columns(
                  pl.when(pl.col("holiday_account_notice_sha256").is_not_null())
                  .then(pl.lit(HOLIDAY_ACCOUNT_DATES_ABI)).otherwise(None).alias("holiday_account_calendar_contract"),
                  pl.when(pl.col("holiday_account_notice_sha256").is_not_null())
                  .then(pl.lit(source.fingerprint)).otherwise(None).alias("holiday_account_source_fingerprint"))
              .sort("date"))
    if result["date"].is_duplicated().any():
        raise ValueError("supplemented holiday calendar is ambiguous")
    return result
