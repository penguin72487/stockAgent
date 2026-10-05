"""Native fiscal/context grains and conservative clocks for research panels.

Taxonomy namespaces, units, consolidated/individual reports, dimensions and
quarter-vs-YTD are semantic keys, not numeric model channels. This module never
turns comparative facts into new releases or estimates an execution price.
"""
from __future__ import annotations

from dataclasses import asdict
from datetime import date
import hashlib
import json
from pathlib import Path
import re

import polars as pl

from stockagent.data.tw_public_release_schedule import RULES, next_session, schedule_lookup
from stockagent.data.tw_feature_semantic_report import semantic_role

NATIVE_CONTRACT = "tw_native_economic_grain_clock_research_v1"


def native_feature_name(definition: dict) -> str:
    payload = json.dumps(definition,sort_keys=True,ensure_ascii=False,separators=(",",":"))
    return "twnative_" + hashlib.sha256(payload.encode()).hexdigest()[:24]


def taxonomy_key(concept: str, dictionary: dict | None = None) -> str:
    """Retain full namespace unless the existing canonical owner has an alias.

    Do not merge arbitrary extension/local names or all accounting eras. Only
    canonical research concepts already supported by the shared XBRL owner are
    mapped from versioned namespaces to their declared aliases.
    """
    if dictionary is not None:
        alias=dictionary.get("aliases",{}).get(concept)
        if alias and not alias.get("ambiguous"):
            return alias["semantic_key"]
    from stockagent.data.tw_public_research_features import _XBRL_CONCEPTS
    if concept.startswith("{http://xbrl.iasb.org/taxonomy/"):
        candidate="ifrs-full:"+concept.rsplit("}",1)[-1]
        return candidate if candidate in _XBRL_CONCEPTS else concept
    if concept.startswith("{http://www.xbrl.org/tifrs/bsci/ci/"):
        candidate="tifrs-ci:"+concept.rsplit("}",1)[-1]
        return candidate if candidate in _XBRL_CONCEPTS else concept
    return concept


def mops_native(path: Path, candidates: pl.DataFrame, symbols: list[str]) -> tuple[pl.DataFrame, dict]:
    """One archive -> nullable issuer facts at their actual financial grain."""
    import calendar
    period=path.parents[1].name
    m=re.fullmatch(r"(20\d\d)Q([1-4])",period)
    if not m:raise ValueError("archive fiscal period required")
    year,quarter=map(int,m.groups()); month=quarter*3
    end=date(year,month,calendar.monthrange(year,month)[1])
    start=date(year,month-2,1);year_start=date(year,1,1)
    frame=pl.scan_parquet(path).filter(pl.col("entity_identifier").is_in(symbols)).select(
        "archive_sha256","document_sha256","source_member","entity_identifier",
        "concept","unit_ref","dimensions_json","period_start","period_end","period_instant",
        "decimal_value","value_parse_status")
    # Join the current report's candidate, NOT the comparative fact period.
    c=candidates.filter(pl.col("period")==period).select(
        "archive_sha256","document_sha256","source_member","company_id",
        "publication_date_taipei","publication_time_basis","board_authorization_on")
    if c.select(pl.struct("archive_sha256","document_sha256","source_member").is_duplicated().any()).item():
        raise ValueError("MOPS publication-candidate identity is ambiguous")
    frame=frame.join(c.lazy(),on=["archive_sha256","document_sha256","source_member"],validate="m:1")
    frame=frame.filter((pl.col("entity_identifier")==pl.col("company_id")) &
        ((pl.col("period_instant")==str(end)) | (pl.col("period_end")==str(end))))
    frame=frame.with_columns(pl.col("decimal_value").cast(pl.Float64,strict=False).alias("value"),
        pl.col("source_member").str.extract(r"-(cr|ir)-",1).alias("report_basis"))
    frame=frame.with_columns(pl.when(pl.col("period_instant")==str(end)).then(pl.lit("instant"))
        .when(pl.col("period_start")==str(start)).then(pl.lit("quarter"))
        .when(pl.col("period_start")==str(year_start)).then(pl.lit("ytd"))
        .otherwise(pl.lit("other_duration")).alias("fiscal_basis"))
    table=frame.collect(engine="streaming")
    # The raw projection retains ambiguous dimensions, unsupported units and
    # parse failures. The admission mask is separate from the source values.
    table=table.with_columns(pl.lit(period).alias("period"),
        (pl.col("dimensions_json")=="[]").fill_null(False).alias("dimensionless"),
        pl.col("entity_identifier").alias("symbol"))
    if quarter==1:
        table=pl.concat([table,table.filter(pl.col("fiscal_basis")=="quarter")
                        .with_columns(pl.lit("ytd").alias("fiscal_basis"))])
    return table, {"archive_period":period,"current_period_rows":table.height,
                   "comparative_values_not_backdated":True}


def mops_admission(raw: pl.DataFrame, sessions: list[date], dictionary: dict | None = None) -> tuple[pl.DataFrame,list[dict],list[dict]]:
    """Unknown grain/unit/context stays in the worklist, not guessed or zeroed."""
    definitions=[];worklist=[];maps={}
    columns=["concept","unit_ref","report_basis","fiscal_basis","dimensions_json"]
    for r in raw.select(columns).unique().iter_rows(named=True):
        quantity=taxonomy_key(r["concept"],dictionary)
        identity={"provider":"MOPS","quantity":quantity,"unit":r["unit_ref"],
            "report_basis":r["report_basis"],"fiscal_basis":r["fiscal_basis"],
            "dimensions":r["dimensions_json"]}
        name=native_feature_name(identity)
        role,_=semantic_role({"field":r["concept"].rsplit("}",1)[-1].rsplit(":",1)[-1],
            "expansion_kind":"mops_xbrl_concept","numeric_observations":1})
        reason=None
        if r["dimensions_json"]!="[]":reason="dimensioned_notes_need_explicit_member_encoding"
        elif r["report_basis"] not in {"cr","ir"}:reason="unknown_consolidated_individual_report_basis"
        elif r["fiscal_basis"]=="other_duration":reason="nonstandard_fiscal_duration_not_quarter_or_ytd"
        elif r["unit_ref"] not in {"TWD","USD","JPY","CNY","HKD","EUR","EarningsPerShare","Shares","shares","Pure","pure","Percent","percent"}:
            reason="unit_ref_has_no_verified_currency_or_measure_identity"
        elif role!="economic_measure":reason="context_text_or_provenance_not_numeric_measure"
        if reason:
            worklist.append({"feature":name,**identity,"reason":reason});continue
        key=tuple(r[c] for c in columns);maps[key]=name
        evidence=(dictionary or {}).get("aliases",{}).get(r["concept"])
        definitions.append({"feature":name,"identity":identity,"source":"MOPS","category":"fundamentals",
            "rule":asdict(RULES["quarter"]),"clock":"known_upload_or_quarter_proxy",
            "publication_time_estimated":True,"value_vintage":"stored_current_report_not_verified_first_vintage",
            "report_group":"MOPS:"+str(r["report_basis"]),
            "original_concepts":[r["concept"]],"taxonomy_evidence":evidence})
    if not maps:return pl.DataFrame(),definitions,worklist
    mapping=pl.DataFrame([{**dict(zip(columns,key)),"feature":value} for key,value in maps.items()])
    raw=raw.join(mapping,on=columns,how="inner",validate="m:1")
    # Filing evidence (when present) replaces a guessed deadline; board dates
    # and cross-issuer percentiles are NOT proven filing dates. General deadline
    # is an explicit research estimate, with the same +1d safety as FinLab.
    lookup=schedule_lookup(raw["period"].unique().to_list(),RULES["quarter"],sessions).select(
        "source_index",pl.col("date").alias("proxy_ready"))
    raw=raw.join(lookup,left_on="period",right_on="source_index",how="left",validate="m:1")
    return raw,definitions,worklist


def collapse_conflicting_observations(frame: pl.DataFrame,keys:list[str]) -> tuple[pl.DataFrame,int]:
    """Exact duplicates coalesce; different values at one key become NULL barriers."""
    grouped=frame.group_by(keys).agg(pl.col("value").n_unique().alias("variants"),
        pl.col("value").first().alias("value"))
    conflicts=grouped.filter(pl.col("variants")>1).height
    grouped=grouped.with_columns(pl.when(pl.col("variants")==1).then(pl.col("value"))
        .otherwise(None).alias("value")).drop("variants")
    return grouped,conflicts
