"""Explicit, source-bound research marks for exceptional holiday account dates.

The input is an audited retrospective reconstruction, not a C11 broker file,
execution price or a verified historical provider snapshot. It is deliberately
separate from ordinary accounting and has not been enabled in a training run.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
import hashlib
import json
import math
from pathlib import Path
import re

import polars as pl

from downloader.artifact_io import sha256_file


HOLIDAY_NIGHT_RESEARCH_ABI = "holiday_night_exchange_tick_corroborated_research_v1"
_DATE_PRODUCTS = {
    "2024-02-06": frozenset({"BRF", "F1F", "SPF", "SXF", "TGF", "UDF", "UNF"}),
    "2026-02-12": frozenset({"BRF", "F1F", "SPF", "SXF", "TGF", "UDF", "UNF"}),
}
_COLUMNS = ("date", "product", "contract", "physical_instance", "previous_mark", "night_close",
            "tick_timestamp", "attributed_trading_date", "holiday_notice_sha256", "category_notice_sha256")


def _digest(frame: pl.DataFrame, manifest_hash: str) -> str:
    rows = frame.select(_COLUMNS).sort("date", "physical_instance").to_dicts()
    payload = json.dumps(rows, default=str, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256((HOLIDAY_NIGHT_RESEARCH_ABI + "\0" + manifest_hash + "\0" + payload).encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class HolidayNightMarkSource:
    """Immutable identity for staged values; the consumer must verify it again."""

    rows: pl.DataFrame
    parent_sha256: str
    manifest_sha256: str
    fingerprint: str

    def verify_identity(self) -> None:
        if _digest(self.rows, self.manifest_sha256) != self.fingerprint:
            raise ValueError("holiday mark source rows changed")


def _bound_path(root: Path, relative: str) -> Path:
    path = (root / relative).resolve()
    if not path.is_relative_to(root):
        raise ValueError("holiday source path escapes its workspace")
    return path


def load_holiday_night_marks(
    source_dir: Path, *, workspace: Path, source_audit: Path,
    financial_snapshot: Path, expected_financial_parent_sha256: str,
) -> HolidayNightMarkSource:
    """Verify candidate rows against independent receipt-bound source records.

    The returned ABI openly retains retrospective source capture and cannot be
    relabeled as a fully point-in-time source. Unknown/no-print rows are not
    imputed. The exact financial parent prevents reuse with a different release.
    """
    workspace = workspace.resolve()
    source_dir = source_dir.resolve()
    if not source_dir.is_relative_to(workspace):
        raise ValueError("holiday source must reside in the selected workspace")
    manifest_path = source_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if (manifest.get("contract") != HOLIDAY_NIGHT_RESEARCH_ABI
            or manifest.get("point_in_time_source_capture_verified") is not False
            or manifest.get("exact_c11_field_independently_observed") is not False
            or manifest.get("auto_adopted_for_training") is not False
            or manifest.get("financial_parent_sha256") != expected_financial_parent_sha256
            or re.fullmatch(r"[0-9a-f]{64}", expected_financial_parent_sha256 or "") is None):
        raise ValueError("holiday evidence ABI or financial parent differs")
    marks_path = source_dir / "candidate_holiday_night_marks.parquet"
    if (sha256_file(marks_path) != manifest.get("marks_sha256")
            or sha256_file(source_audit) != manifest.get("source_audit_sha256")
            or sha256_file(financial_snapshot) != manifest.get("financial_snapshot_sha256")):
        raise ValueError("holiday evidence artifact hash differs")
    stage_script = workspace / "artifacts/operations/futures-universe765-repair-20261027-v1/stage-holiday-night-marks-v1.py"
    if sha256_file(stage_script) != manifest.get("staging_script_sha256"):
        raise ValueError("holiday staging implementation changed")
    audit = json.loads(source_audit.read_text())
    financial = json.loads(financial_snapshot.read_text())
    if (audit.get("contract") != "holiday_night_approximation_source_audit_v1"
            or financial.get("contract") != "remote_v18_holiday_candidate_rows_v1"
            or financial.get("source_sha256") != expected_financial_parent_sha256):
        raise ValueError("holiday audit or financial snapshot differs")
    for name, value in audit["input_pins"].items():
        if sha256_file(_bound_path(workspace, name)) != value:
            raise ValueError("holiday original market source hash differs")
    for rule_set in manifest["rules"].values():
        for source in rule_set.values():
            for path_key, hash_key in (("raw_path", "raw_sha256"), ("parsed_path", "parsed_sha256")):
                if sha256_file(_bound_path(workspace, source[path_key])) != source[hash_key]:
                    raise ValueError("holiday official rule source hash differs")
    rows = pl.read_parquet(marks_path)
    if (set(rows.columns) != set(_COLUMNS) or rows.height != manifest["mark_count"] or not rows.height
            or rows.select("date", "physical_instance").is_duplicated().any()):
        raise ValueError("holiday source row schema, count or identity differs")
    observations = {(p["account_event_date"], r["product"], r["contract"]): r
                    for p in audit["periods"] for r in p["rows"]}
    snapshots = {(r["date"], r["product"], r["contract"], r["physical_contract"]): r
                 for r in financial["rows"]}
    if len(snapshots) != len(financial["rows"]):
        raise ValueError("holiday financial snapshot contains duplicate identities")
    for row in rows.to_dicts():
        day = str(row["date"])
        product, contract = row["product"], row["contract"]
        tick = datetime.fromisoformat(row["tick_timestamp"])
        if (product not in _DATE_PRODUCTS.get(day, ()) or not re.fullmatch(r"\d{6}", contract)
                or row["attributed_trading_date"] <= row["date"]
                or tick > datetime.combine(row["date"], time(5, 0))
                or not math.isfinite(row["night_close"]) or row["night_close"] <= 0
                or not math.isfinite(row["previous_mark"]) or row["previous_mark"] <= 0):
            raise ValueError("invalid holiday mark identity, clock or value")
        original = snapshots.get((day, product, contract, row["physical_instance"]))
        observation = observations.get((day, product, contract))
        if (not original or original["cash_settlement"] or original["valuation_price"] != row["previous_mark"]
                or not observation or not observation["exact_close_match"]
                or observation["official"]["close"] != row["night_close"]
                or observation["finmind_last_tick"]["timestamp"] != row["tick_timestamp"]
                or observation["finmind_last_tick"]["prices"] != [row["night_close"]]
                or row["holiday_notice_sha256"] != manifest["rules"][day]["holiday"]["content_sha256"]
                or row["category_notice_sha256"] != manifest["rules"][day]["category"]["content_sha256"]):
            raise ValueError("holiday mark is not supported by its bound observations")
    fingerprint = _digest(rows, sha256_file(manifest_path))
    return HolidayNightMarkSource(rows, expected_financial_parent_sha256, sha256_file(manifest_path), fingerprint)


def apply_holiday_night_marks(physical: pl.DataFrame, source: HolidayNightMarkSource) -> pl.DataFrame:
    """Change only matched account-day valuations, before financial compilation.

    Physical contract generations and old values must match the verified
    parent exactly. This does not create order capacity, copy a future decision,
    or certify any other account-day price. Its new identity requires a new
    financial release and checkpoint ABI in the caller.
    """
    source.verify_identity()
    required = {"date", "product", "contract", "physical_instance", "valuation_price",
                "accounting_only_date", "cash_settlement"}
    if not required <= set(physical.columns):
        raise ValueError("holiday valuation requires a physical accounting calendar")
    keys = ["date", "product", "contract", "physical_instance"]
    if physical.select(keys).is_duplicated().any() or source.rows.select(keys).is_duplicated().any():
        raise ValueError("ambiguous physical contract on holiday account date")
    if source.rows.select(keys).join(physical.select(keys), on=keys, how="anti").height:
        raise ValueError("holiday source contains a contract absent from the selected parent")
    added = source.rows.select(*keys, "previous_mark", "night_close").rename(
        {"previous_mark": "_holiday_previous_mark", "night_close": "_holiday_night_close"})
    result = physical.join(added, on=keys, how="left", validate="1:1")
    selected = pl.col("_holiday_night_close").is_not_null()
    if result.filter(selected & (~pl.col("accounting_only_date") | pl.col("cash_settlement")
            | pl.col("valuation_price").is_null()
            | (pl.col("valuation_price") != pl.col("_holiday_previous_mark")))).height:
        raise ValueError("holiday mark does not match the original nonterminal account row")
    return result.with_columns(
        pl.when(selected).then(pl.col("_holiday_night_close"))
        .otherwise(pl.col("valuation_price")).alias("valuation_price"),
        pl.when(selected).then(pl.lit(HOLIDAY_NIGHT_RESEARCH_ABI))
        .otherwise(None).alias("holiday_valuation_contract"),
        pl.when(selected).then(pl.lit(source.fingerprint))
        .otherwise(None).alias("holiday_valuation_source_fingerprint"),
    ).drop("_holiday_previous_mark", "_holiday_night_close")
