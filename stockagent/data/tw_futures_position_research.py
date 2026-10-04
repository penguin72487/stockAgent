"""Opt-in research risk capacities for the existing physical futures compiler.

Original position facts remain intact. This policy supplies an operator's risk
constraint, never an assertion about an unknown exchange rule or publication.
Only position inputs change; quotes, financial units and margin clocks do not.
"""
from __future__ import annotations

from datetime import date, datetime
import json
import math
from pathlib import Path
import re

import polars as pl


POSITION_RESEARCH_CONTRACT = "stock_futures_previous_capacity_research_v1"
POSITION_RESEARCH_COLUMNS = [
    "position_research_applied", "position_research_method",
    "position_research_seed_date", "position_research_source_known_at",
]


def validate_position_research_policy(policy: dict) -> dict:
    if (policy.get("contract") != POSITION_RESEARCH_CONTRACT
            or policy.get("research_only") is not True):
        raise ValueError("unsupported research position policy")
    for key in ("fallback_standard_contracts", "standard_shares_per_contract"):
        value = policy.get(key)
        if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value) or value <= 0:
            raise ValueError("research fallback capacity must be positive and finite")
    if policy["standard_shares_per_contract"] != 2000:
        raise ValueError("this research policy supports 2000-share stock futures only")
    try:
        known = datetime.fromisoformat(policy["fallback_known_at"])
        effective = date.fromisoformat(policy["fallback_effective_date"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("research fallback requires dated provenance") from exc
    if known.tzinfo is None or not re.fullmatch(r"[a-f0-9]{64}", policy.get("fallback_source_content_sha256", "")):
        raise ValueError("research fallback requires a timezone and source SHA")
    if not str(policy.get("fallback_source_url", "")).startswith("https://www.taifex.com.tw/"):
        raise ValueError("research fallback requires its official reference")
    if effective < known.date():
        raise ValueError("research fallback cannot predate its reference")
    return policy


def validate_position_research_manifest(manifest: dict, root: Path) -> dict:
    """Accept the separate research ABI without calling it official PIT."""
    if (manifest.get("research_only") is not True
            or manifest.get("point_in_time_verified") is not False
            or manifest.get("financial_point_in_time_verified") is not True):
        raise ValueError("research position release requires explicit financial/research boundaries")
    proof = manifest.get("position_research_policy", {})
    relative = Path(proof.get("path", ""))
    if relative.is_absolute() or ".." in relative.parts or not relative.parts or relative.parts[0] != "sources":
        raise ValueError("unsafe research position policy receipt")
    from downloader.artifact_io import sha256_file
    if sha256_file(root / relative) != proof.get("sha256"):
        raise ValueError("research position policy SHA mismatch")
    policy = validate_position_research_policy(json.loads((root / relative).read_text()))
    if (manifest.get("position_research_contract") != policy["contract"]
            or manifest.get("position_research_policy_sha256") != proof["sha256"]):
        raise ValueError("research position manifest and policy differ")
    return policy


def apply_research_position_capacity(frame: pl.DataFrame, policy: dict,
                                     failed_position: pl.Expr) -> pl.DataFrame:
    """Repair entire affected dated groups with causally available share caps.

Known capacities are converted using each row's actual financial multiplier.
Group minima retain known tightening, even when another member is unresolved.
Backward asof supplies past capacities only. The first missing capacity uses
the explicit initial research grade. All months share one absolute gross pool;
the existing independent or monthly second constraint stays in the account.
"""
    validate_position_research_policy(policy)
    keys = ["date", "physical_contract"]
    opening = (pl.col("date").cast(pl.String) + "T" + pl.col("opening_time") + "+08:00").str.to_datetime(time_zone="UTC")
    positive = lambda c: (pl.col(c).is_finite() & (pl.col(c) > 0)).fill_null(False)
    eligible = (pl.col("specification_bound") & pl.col("spec_tick_kind").eq("stock_future")
                & pl.col("product").str.contains(r"^[A-Z0-9]{2}[F1-9]$")
                & positive("contract_multiplier")).fill_null(False)
    effective = (pl.col("pos_effective_date") + "T" + pl.when(pl.col("pos_effective_phase") == 0)
        .then(pl.col("opening_time")).otherwise(pl.col("settlement_time")) + "+08:00").str.to_datetime(time_zone="UTC", strict=False)
    known = pl.col("pos_known_at").str.to_datetime(time_zone="UTC", strict=False)
    source_valid = (eligible & pl.col("pos_position_numeric_inputs_resolved")
        & positive("pos_position_unit") & positive("pos_position_limit")
        & (known <= opening) & (effective <= opening)).fill_null(False)
    # Use the dated binder's group when present; an unknown initial group is
    # an explicitly inferred code family, not a fabricated legal membership.
    f = frame.with_columns(
        pl.coalesce("pos_position_root_product", pl.col("product").str.slice(0, 2) + "F").alias("_risk_root"),
        source_valid.alias("_risk_source_valid"), eligible.alias("_risk_eligible"),
        (failed_position & eligible).fill_null(False).alias("_risk_failed"),
        (pl.col("contract_multiplier") / pl.col("pos_position_unit")).alias("_risk_factor"),
    ).with_columns(pl.col("_risk_failed").any().over(["date", "_risk_root"]).alias("_risk_group_failed"))
    sources = f.filter(pl.col("_risk_source_valid")).group_by("date", "_risk_root").agg(
        (pl.col("pos_position_limit") * pl.col("_risk_factor")).min().floor().alias("_risk_cap"),
        (pl.coalesce("pos_monthly_position_limit", "pos_position_limit") * pl.col("_risk_factor"))
            .min().floor().alias("_risk_month_cap"),
        pl.col("pos_known_at").str.to_datetime(time_zone="UTC", strict=False).max()
            .dt.to_string("%Y-%m-%dT%H:%M:%S%:z").alias("_risk_seed_known"),
        (pl.col("pos_independent_contract_limit").is_not_null().any()
         & pl.col("pos_monthly_position_limit").is_not_null().any()).alias("_risk_three_axes"),
    ).rename({"date": "_risk_seed_date"})
    if sources.is_empty():
        f = f.with_columns(pl.lit(None, dtype=pl.Date).alias("_risk_seed_date"),
            pl.lit(None, dtype=pl.Float64).alias("_risk_cap"),
            pl.lit(None, dtype=pl.Float64).alias("_risk_month_cap"),
            pl.lit(None, dtype=pl.String).alias("_risk_seed_known"),
            pl.lit(False).alias("_risk_three_axes"))
    else:
        f = f.sort("date").join_asof(sources.sort("_risk_seed_date"),
            left_on="date", right_on="_risk_seed_date", by="_risk_root",
            strategy="backward", check_sortedness=False)
    fallback_date = date.fromisoformat(policy["fallback_effective_date"])
    fallback_shares = math.floor(policy["fallback_standard_contracts"] * policy["standard_shares_per_contract"])
    fallback = pl.col("_risk_seed_date").is_null() & (pl.col("date") >= fallback_date)
    f = f.with_columns(
        pl.when(fallback).then(float(fallback_shares)).otherwise(pl.col("_risk_cap")).alias("_risk_cap"),
        pl.when(fallback).then(float(fallback_shares)).otherwise(pl.col("_risk_month_cap")).alias("_risk_month_cap"),
        pl.when(fallback).then(pl.lit(fallback_date)).otherwise(pl.col("_risk_seed_date")).alias("_risk_seed_date"),
        pl.when(fallback).then(pl.lit(policy["fallback_known_at"])).otherwise(pl.col("_risk_seed_known")).alias("_risk_seed_known"),
        pl.when(fallback).then(pl.lit("initial_ordinary_grade_assumption"))
          .when(pl.col("_risk_seed_date") == pl.col("date")).then(pl.lit("known_group_minimum_shares"))
          .otherwise(pl.lit("previous_known_group_capacity")).alias("_risk_method"),
    ).with_columns(
        # With only two runtime axes, a known independent-product limit plus
        # a month limit needs a tighter total pool, not a discarded constraint.
        pl.when(pl.col("_risk_three_axes").fill_null(False)).then(pl.min_horizontal("_risk_cap", "_risk_month_cap"))
          .otherwise(pl.col("_risk_cap")).alias("_risk_cap"),
    )
    apply = (pl.col("_risk_eligible") & pl.col("_risk_group_failed")
        & positive("_risk_cap") & positive("_risk_month_cap")
        & (pl.col("_risk_seed_known").str.to_datetime(time_zone="UTC", strict=False) <= opening)).fill_null(False)
    # Independent per-product constraints remain separate from the common pool.
    # Unknown/invalid independent values never become silently valid facts.
    independent = (pl.when(pl.col("_risk_source_valid") & positive("pos_independent_contract_limit"))
        .then(pl.col("pos_independent_contract_limit")))
    replacements = {
        "position_unit": pl.col("contract_multiplier"),
        "position_limit": pl.col("_risk_cap"),
        "monthly_position_limit": pl.when(independent.is_null()).then(pl.col("_risk_month_cap")),
        "position_root_product": pl.lit("RESEARCH:") + pl.col("_risk_root"),
        "position_numeric_inputs_resolved": pl.lit(True),
        "known_at": pl.col("_risk_seed_known"),
        "effective_date": pl.col("_risk_seed_date").cast(pl.String),
        "effective_phase": pl.lit(0), "unit": pl.lit("shares"),
        "independent_contract_limit": independent,
    }
    f = f.with_columns(*[pl.when(apply).then(value).otherwise(pl.col("pos_" + c)).alias("pos_" + c)
                          for c, value in replacements.items()],
        apply.alias("position_research_applied"),
        pl.when(apply).then(pl.col("_risk_method")).alias("position_research_method"),
        pl.when(apply).then(pl.col("_risk_seed_date")).alias("position_research_seed_date"),
        pl.when(apply).then(pl.col("_risk_seed_known")).alias("position_research_source_known_at"))
    return f.drop([c for c in f.columns if c.startswith("_risk_")]).sort(keys)
