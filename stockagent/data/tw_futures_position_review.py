"""Apply an explicit, source-bound transcription review, never infer a cap."""
from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
import re
from zoneinfo import ZoneInfo

import polars as pl


def reviewed_cap(review: dict) -> tuple[date, date, float, float]:
    if review.get("schema_version") != 1 or review.get("kind") != "visual_position_cap_transcription":
        raise ValueError("unsupported position transcription review")
    start, end = (date.fromisoformat(review[k]) for k in ("start", "end"))
    known = datetime.fromisoformat(review["known_at"])
    if start > end or known.tzinfo is None or known > datetime.combine(
        start, time(8, 45), ZoneInfo("Asia/Taipei")
    ):
        raise ValueError("review must preserve the original pre-decision publication clock")
    literal = review["official_natural_person_cap"]
    # Fractional shares are valid. Neither stripping decimal points nor a
    # blanket x1000 is a legal correction (e.g. HOF 2011 has 720,958.42 shares).
    if not re.fullmatch(r"(?:\d+|\d{1,3}(?:,\d{3})+)(?:\.\d+)?", literal):
        raise ValueError("ambiguous official cap transcription")
    old = Decimal(str(review["incorrect_cap"]))
    new = Decimal(literal.replace(",", ""))
    if not old.is_finite() or not new.is_finite() or min(old, new) <= 0 or old == new:
        raise ValueError("review needs distinct finite positive old/new caps")
    if review.get("unit") != "shares" or not review.get("products"):
        raise ValueError("review must name the exact shared securities-unit group")
    if set(review["products"]) != set(review["position_units"]):
        raise ValueError("review lacks per-product conversion units")
    for key in ("pdf_sha256", "page_image_sha256", "parent_rules_sha256"):
        if not re.fullmatch("[a-f0-9]{64}", review.get(key, "")):
            raise ValueError(f"review lacks {key}")
    return start, end, float(old), float(new)


def apply_position_cap_review(rules: pl.DataFrame, review: dict) -> tuple[pl.DataFrame, dict]:
    """Change both cap axes on the reviewed group/dates; preserve every other cell."""
    start, end, old, new = reviewed_cap(review)
    dated = pl.col("date").is_between(start, end)
    group = review["position_group"]
    affected = dated & ((pl.col("position_group") == group)
                        | (pl.col("second_position_group") == group))
    rows = rules.filter(affected)
    if rows.height != review["expected_rows"] or set(rows["product"]) != set(review["products"]):
        raise ValueError("review scope/row count differs from the pinned parent")
    for product, unit in review["position_units"].items():
        selected = rows.filter(pl.col("product") == product)
        if selected.filter((pl.col("position_unit") != unit)
                           | (pl.col("second_position_unit") != unit)).height:
            raise ValueError("reviewed conversion unit disagrees with accounting source")
    changed_columns = ["position_limit", "second_position_limit"]
    expressions = []
    counts = {}
    for prefix in ("", "second_"):
        key = prefix + "position_limit"
        mask = dated & (pl.col(prefix + "position_group") == group)
        selected = rules.filter(mask)
        if selected.filter(pl.col(key) != old).height:
            raise ValueError("review old cap differs; refusing a partial or repeated correction")
        counts[key] = selected.height
        expressions.append(pl.when(mask).then(pl.lit(new)).otherwise(pl.col(key)).alias(key))
    corrected = rules.with_columns(expressions)
    if not corrected.drop(changed_columns).equals(rules.drop(changed_columns)):
        raise AssertionError("position review changed another accounting field")
    if not corrected.filter(~affected).equals(rules.filter(~affected)):
        raise AssertionError("position review escaped its dated group")
    return corrected, dict(rows=rows.height, changed_cells=counts, old_cap=old, new_cap=new,
                          other_accounting_fields_equal=True, unaffected_rows_equal=True)
