"""Provider-independent row selection with separate, inspectable provenance.

Callers must first normalize economic keys, units, clocks and revisions. This
module does not guess aliases, average conflicting observations or impute data.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl


CONTRACT = "same_semantics_source_rows_v1"
INTERNAL = ("_provider", "_source_ref", "_rank", "_invalid_reason")


@dataclass(frozen=True)
class SourceRows:
    provider: str
    source_ref: str
    semantics: dict[str, Any]
    frame: pl.DataFrame


@dataclass(frozen=True)
class MergedRows:
    values: pl.DataFrame
    lineage: pl.DataFrame
    conflicts: pl.DataFrame
    rejected: pl.DataFrame
    audit: dict[str, Any]


def merge_source_rows(sources: list[SourceRows], *, keys: list[str],
                      fields: list[str], semantics: dict[str, Any],
                      absolute_tolerance: float = 1e-8,
                      relative_tolerance: float = 1e-10) -> MergedRows:
    """Keep a valid primary row; use backups only for missing/rejected rows.

    OHLC groups remain atomic. Conflicting valid values are recorded, never
    called errors solely because another provider disagrees. An ambiguous
    duplicate within a provider is rejected, rather than won by file order.
    NULL quantities remain unknown and never turn into executable capacity.
    """
    if not sources or not keys or not fields or set(keys) & set(fields):
        raise ValueError("sources and disjoint nonempty keys/fields are required")
    if absolute_tolerance < 0 or relative_tolerance < 0:
        raise ValueError("negative comparison tolerance")
    rows, rejected, duplicate_rows = [], [], 0
    for rank, source in enumerate(sources):
        if source.semantics != semantics:
            raise ValueError(f"unverified semantic equivalence: {source.provider}")
        required = set(keys + fields + ["_invalid_reason"])
        if not required <= set(source.frame.columns):
            raise ValueError(f"missing normalized columns: {source.provider}")
        table = source.frame.select(keys + fields + ["_invalid_reason"]).with_columns(
            pl.lit(source.provider).alias("_provider"),
            pl.lit(source.source_ref).alias("_source_ref"), pl.lit(rank).alias("_rank"))
        table = table.with_columns(pl.when(pl.any_horizontal(*[pl.col(k).is_null() for k in keys]))
            .then(pl.lit("invalid_economic_key"))
            .otherwise(pl.col("_invalid_reason").fill_null("missing_validation_result")).alias("_invalid_reason"))
        counts = table.group_by(keys).agg(
            pl.len().alias("_rows"),
            pl.struct(fields + ["_invalid_reason"]).n_unique().alias("_variants"))
        duplicate_rows += int(counts.select((pl.col("_rows") - 1).sum()).item() or 0)
        table = table.join(counts.select(keys + ["_variants"]), on=keys, validate="m:1")
        table = table.with_columns(pl.when(pl.col("_variants") > 1)
            .then(pl.lit("conflicting_provider_duplicates"))
            .otherwise(pl.col("_invalid_reason")).alias("_invalid_reason")).drop("_variants")
        rejected.append(table.filter(pl.col("_invalid_reason") != ""))
        rows.append(table.unique(keys, keep="first", maintain_order=True))
    all_rows = pl.concat(rows, how="vertical")
    valid = all_rows.filter(pl.col("_invalid_reason") == "")
    chosen = valid.sort(keys + ["_rank"]).unique(keys, keep="first", maintain_order=True)
    primary_presence = all_rows.filter(pl.col("_rank") == 0).select(keys).with_columns(
        pl.lit(True).alias("_primary_present"))
    chosen = chosen.join(primary_presence, on=keys, how="left", validate="1:1").with_columns(
        pl.when(pl.col("_rank") == 0).then(pl.lit("primary_valid"))
        .when(pl.col("_primary_present").fill_null(False)).then(pl.lit("repaired_rejected_primary"))
        .otherwise(pl.lit("missing_primary_filled")).alias("selection_reason")).drop("_primary_present")
    peer = chosen.select(keys + [pl.col(c).alias("selected_" + c) for c in fields]
        + [pl.col("_provider").alias("selected_provider"), pl.col("_source_ref").alias("selected_source_ref")])
    compared = valid.join(peer, on=keys, validate="m:1")
    different = []
    for field in fields:
        a, b = pl.col(field), pl.col("selected_" + field)
        # Canonical measurements are numerical; NULL means no observation.
        different.append(a.is_not_null() & b.is_not_null() &
            ((a - b).abs() > absolute_tolerance + relative_tolerance * pl.max_horizontal(a.abs(), b.abs())))
    conflicts = compared.filter(pl.any_horizontal(*different)).with_columns(
        pl.concat_list([pl.when(d).then(pl.lit(c)).otherwise(None)
            for c, d in zip(fields, different)]).list.drop_nulls().alias("conflicting_fields"))
    values = chosen.select(keys + fields).sort(keys)
    lineage = chosen.select(keys + ["_provider", "_source_ref", "selection_reason"]).sort(keys)
    bad = pl.concat(rejected, how="vertical")
    return MergedRows(values, lineage, conflicts, bad, {
        "contract": CONTRACT, "source_rows": sum(s.frame.height for s in sources),
        "canonical_rows": values.height, "duplicate_rows": duplicate_rows,
        "rejected_rows": bad.height, "conflicting_valid_rows": conflicts.height,
        "selection": {r["selection_reason"]: r["len"] for r in
            lineage.group_by("selection_reason").len().to_dicts()},
        "provider_rows": {r["_provider"]: r["len"] for r in lineage.group_by("_provider").len().to_dicts()},
        "model_has_provider_channels": False, "invented_observations": False,
    })


def lineage_segments(lineage: pl.DataFrame, *, groups: list[str], time: str = "ts",
                     cadence_ns: int = 60_000_000_000) -> pl.DataFrame:
    """Bounded source intervals; gaps always split, never imply filled bars."""
    table = lineage.sort(groups + [time])
    if table.is_empty():
        return table.rename({time: "first_ts"}).with_columns(
            pl.lit(None, dtype=pl.Datetime("ns")).alias("last_ts"), pl.lit(0).alias("rows"))
    breaks = (pl.col(time).cast(pl.Int64).diff().over(groups) != cadence_ns)
    for column in ("_provider", "_source_ref", "selection_reason"):
        breaks |= pl.col(column) != pl.col(column).shift(1).over(groups)
    table = table.with_columns(breaks.fill_null(True).cast(pl.Int64).alias("_break"))
    table = table.with_columns(pl.col("_break").cum_sum().over(groups).alias("_segment"))
    return table.group_by(groups + ["_segment"], maintain_order=True).agg(
        pl.col(time).min().alias("first_ts"), pl.col(time).max().alias("last_ts"),
        pl.len().alias("rows"), *[pl.col(c).first() for c in
            ("_provider", "_source_ref", "selection_reason")]).drop("_segment")
