"""Explicit cadence/clock ABI for the canonical lag-one cash day-trade loader.

Provider observations remain source evidence. This module does not infer an
update from a changed value, resample an execution price, or reconstruct vintages.
"""
from __future__ import annotations

from dataclasses import asdict, replace
from datetime import date

import polars as pl

from stockagent.data.tw_public_features import POST_CLOSE_CHIP_FEATURE_COLUMNS
from stockagent.data.tw_public_release_schedule import ReleaseRule, RULES

CONTRACT = "tw_day_trade_mixed_frequency_preopen_lag1_v1"
PRIVATE_USE = "owner_private_noncommercial_vastai1T_training_no_public_redistribution"
PUBLIC_COORDINATE_NULL_POLICY = "public_market_unobserved_coordinate_v2"


def public_coordinate_null_spec(spec: dict) -> dict:
    """Qualify calendar-padding NULLs, not native released NULL observations.

    The legacy shared daily table is an outer-joined coordinate view. A NULL
    cell alone is not proof that a new monthly/quarterly release occurred.
    Only registered low-frequency public market columns may use this policy;
    native report/release rows retain their ordinary NULL state barriers.
    """
    rule = rule_from_spec(spec)
    if (spec.get("source") != "tw-public" or rule.scope != "market"
            or rule.carry_days == 0):
        if spec.get("null_event_policy")==PUBLIC_COORDINATE_NULL_POLICY:
            raise ValueError("coordinate NULL policy escaped its public market scope")
        return dict(spec)
    registered = public_spec(spec.get("source_column", ""))
    if (registered is None or registered["clock"] != spec.get("clock")
            or rule_from_spec(registered) != rule):
        raise ValueError("coordinate NULL policy requires an exact registered public clock")
    return {**spec, "null_event_policy": PUBLIC_COORDINATE_NULL_POLICY,
        "null_event_policy_evidence": "outer-joined shared public calendar; NULL is no release evidence; native NULL release barriers unchanged"}


def public_spec(name: str) -> dict | None:
    """Only registered clocks; snapshots and lossy redundant transforms stay out.

    Canonical post-close chip and release-vintage builders already map their
    facts to available sessions. Completed-session prices still need one shift.
    """
    if name.endswith("_log") or name.endswith("_yoy") or name.startswith("twpub_mof_"):
        # Do not duplicate raw observations or revive positive-only log1p
        # transforms which discarded valid zero/negative observations.
        return None
    scope = "market" if name.startswith(("twpub_twse_", "twpub_usdtwd_", "twpub_cbc_", "twpub_dgbas_", "twpub_mof_", "twpub_taifex_")) else "stock"
    clock, rule = "completed_session", replace(RULES["daily"], scope=scope)
    if name in POST_CLOSE_CHIP_FEATURE_COLUMNS and (name.endswith("_raw") or name.endswith("_chg")):
        clock = "available_session"
    elif name.startswith(("twpub_cbc_m1", "twpub_cbc_m2", "twpub_cbc_fx_reserves", "twpub_dgbas_cpi", "twpub_dgbas_unemployment")):
        clock, rule = "available_session", replace(RULES["business"], scope=scope)
    elif name.startswith("twpub_dgbas_gdp"):
        clock, rule = "available_session", replace(RULES["quarter"], scope=scope)
    elif name.startswith("twpub_mof_"):
        clock, rule = "available_session", replace(RULES["business"], scope=scope)
    elif name.startswith(("twpub_tdcc_", "twpub_company_", "twpub_insider_", "twpub_exdiv_", "twpub_dividend_", "twpub_material_", "twpub_attention_", "twpub_disposal_", "twpub_monthly_revenue_", "twpub_financial_")):
        # Prefer raw scheduled/reconciled observations for these families. The
        # legacy table can contain snapshot-only facts or event-log transforms.
        return None
    elif not (name.endswith("_raw") or name in {
        "twpub_official_close_logret_1d", "twpub_official_turnover_ratio", "twpub_official_intraday_range",
        "twpub_official_close_to_high", "twpub_official_close_to_low", "twpub_usdtwd_logret_1d",
        "twpub_cbc_overnight_rate", "twpub_cbc_overnight_rate_chg", "twpub_taifex_tx_settlement_logret_1d",
        "twpub_taifex_txo_put_call_volume_ratio", "twpub_taifex_txo_put_call_oi_ratio",
        "twpub_taifex_dealer_net_oi_asinh", "twpub_taifex_foreign_net_oi_asinh", "twpub_taifex_trust_net_oi_asinh",
        "twpub_taifex_tx_top5_long_ratio", "twpub_taifex_tx_top10_long_ratio",
    }):
        return None
    return {"source_column": name, "feature": "twcad_" + name.removeprefix("twpub_"),
            "clock": clock, "rule": asdict(rule), "source": "tw-public",
            "value_vintage": "canonical_source_revision", "publication_time_estimated": False}


def storage_lookup(sessions: list[date]) -> pl.DataFrame:
    """Ready session t is stored at t-1, not exposed early at execution t-1.

    The existing exchange-aware dataset reads through t-1. This is the same
    contract as next_session_open_gap_logret, not a second safety/publication lag.
    """
    if not sessions or sessions != sorted(set(sessions)):
        raise ValueError("calendar must be nonempty, sorted and unique")
    return pl.DataFrame({"decision_date": sessions[1:], "date": sessions[:-1]},
                        schema={"decision_date": pl.Date, "date": pl.Date})


def rule_from_spec(spec: dict) -> ReleaseRule:
    rule = dict(spec["rule"])
    rule["urls"] = tuple(rule["urls"])
    return ReleaseRule(**rule)


def source_clock_lookup(sessions: list[date], clock: str) -> pl.DataFrame:
    if clock == "completed_session":
        return pl.DataFrame({"source_date": sessions[:-1], "date": sessions[1:]},
                            schema={"source_date": pl.Date, "date": pl.Date})
    if clock == "available_session":
        return pl.DataFrame({"source_date": sessions, "date": sessions},
                            schema={"source_date": pl.Date, "date": pl.Date})
    raise ValueError("unknown source clock; do not guess publication timing")
