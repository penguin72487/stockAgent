#!/usr/bin/env python3
"""Reconcile native TEJ event operands without admitting or inventing prices."""
from __future__ import annotations

import argparse
from collections import defaultdict
from decimal import Decimal
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import polars as pl
from downloader.artifact_io import atomic_write_bytes, atomic_write_json, atomic_write_text, sha256_file
from downloader.tej_history import display_decimal, normalize_period


def halt_events_from_rows(rows):
    """Retain actual TEJ event date cells; never infer a clearing value.

    The generic query axis is not a historical publication clock. These
    separately named native date fields are company-event evidence only.
    """
    events = []
    for row in rows:
        required = {"Co_id", "Suspended Date_Begin", "Suspended Re-trading", "_query_symbol"}
        if not required <= set(row):
            raise ValueError("Halt evidence lacks the exact native company/event headers")
        co = re.match(r"^([0-9A-Z]+)(?:\s|$)", row["Co_id"] or "")
        if co is None or co.group(1) != row["_query_symbol"]:
            raise ValueError("Halt native company differs from source query owner")
        def event_date(value):
            if not isinstance(value, str) or not re.fullmatch(r"\d{4}[-/]\d{2}[-/]\d{2}", value):
                raise ValueError("Halt event date requires an explicit complete native date")
            from datetime import date
            return date.fromisoformat(value.replace("/", "-")).isoformat()
        begin = event_date(row["Suspended Date_Begin"])
        resume_cell = row["Suspended Re-trading"]
        resume = event_date(resume_cell) if resume_cell not in (None, "", "(null)") else None
        if resume is not None and resume <= begin:
            raise ValueError("Halt resumption must follow its native starting date")
        events.append(dict(underlying_symbol=co.group(1), halt_start=begin, resume_date=resume,
            native_company=row["Co_id"], native_start=row["Suspended Date_Begin"],
            native_resume=resume_cell, publication_verified=False, source_values_admitted=False))
    return events


def lifecycle_from_rows(rows):
    """Read dated native fields without backdating the snapshot or clearing inventory."""
    from datetime import date
    result = []
    for row in rows:
        required = {'Future Code', 'Listed Date', 'Trade Date_End', 'Settle Date_End',
                    'Settle Price_End', '_query_symbol'}
        if not required <= set(row):
            raise ValueError('Lifecycle evidence lacks exact native contract/date headers')
        key = re.match(r'^([A-Z]{2}[F0-9])(\d{4}(?:0[1-9]|1[0-2]))(?:\s|$)', row['Future Code'] or '')
        if key is None or key.group(1) + key.group(2) != row['_query_symbol']:
            raise ValueError('Lifecycle native monthly key differs from its query owner')
        def native_date(value):
            if value in (None, '', '(null)'):
                return None
            if not isinstance(value, str) or not re.fullmatch(r'\d{4}[-/]\d{2}[-/]\d{2}', value):
                raise ValueError('Lifecycle date requires a complete literal native date')
            return date.fromisoformat(value.replace('/', '-')).isoformat()
        listed, trade, settle = (native_date(row[c]) for c in
                                ('Listed Date', 'Trade Date_End', 'Settle Date_End'))
        if (listed and trade and trade < listed) or (trade and settle and settle < trade):
            raise ValueError('Lifecycle native dates have inconsistent chronology')
        final = number(row['Settle Price_End'])
        result.append(dict(product=key.group(1), contract=key.group(2), listed_date=listed,
            native_last_trade_date=trade, native_settlement_date=settle,
            native_final_price_cell=row['Settle Price_End'], native_final_price_present=final is not None,
            native_last_trade_before_settlement=bool(trade and settle and trade < settle),
            generation_specific=False, publication_verified=False, source_values_admitted=False))
    return result


def audit_lifecycle(root, inventory, gaps_path, raw_source, receipt_audit, output):
    """Use existing verified TEJ exports and exact own observations, with no queries or fills."""
    from datetime import date
    if output.exists():
        raise ValueError('Preserve the source-bound lifecycle audit')
    accepted = json.loads(receipt_audit.read_text())
    if not accepted.get('accepted') or not accepted.get('all_selected_tasks_completed'):
        raise ValueError('Completed native receipt acceptance must precede lifecycle interpretation')
    ids = accepted.get('selected_task_ids') or []
    if not ids:
        raise ValueError('Lifecycle interpretation requires a finite audited task set')
    inv_path = inventory / 'manifest.json'
    inv = json.loads(inv_path.read_text())
    links_path = inventory / 'gap_operand_links.parquet'
    if sha256_file(links_path) != inv['outputs'][links_path.name]['sha256']:
        raise ValueError('Lifecycle operand inventory changed')
    gaps = pl.read_csv(gaps_path, schema_overrides={'contract': pl.String}, try_parse_dates=True)
    gaps = gaps.filter(~((pl.col('product') == 'CPF') & (pl.col('date') < date(2010, 1, 1))))
    if gaps.select('date', 'physical_contract').is_duplicated().any():
        raise ValueError('Lifecycle worklist repeats a physical coordinate')
    sources = [dict(path=str(p), sha256=sha256_file(p)) for p in
               (gaps_path, inv_path, links_path, receipt_audit)]
    native = []
    for task_id in ids:
        receipt_path = root / 'receipts' / (task_id + '.json')
        receipt = json.loads(receipt_path.read_text())
        if receipt['table_id'] != '1bea200ec21f8f1a39056861':
            raise ValueError('Lifecycle interpretation cannot consume an unrelated table')
        p = root / receipt['parquet_path']
        if sha256_file(p) != receipt['parquet_sha256']:
            raise ValueError('Lifecycle native parquet changed')
        native.extend(r | dict(source_task_id=task_id, source_parquet_sha256=receipt['parquet_sha256'])
                      for r in lifecycle_from_rows(pl.read_parquet(p).to_dicts()))
        sources.extend(dict(path=str(path), sha256=sha256_file(path)) for path in (receipt_path, p))
    attributes = pl.DataFrame(native)
    semantic_fields = [c for c in attributes.columns if c not in ('source_task_id', 'source_parquet_sha256')]
    conflicts = attributes.unique(semantic_fields).group_by('product', 'contract').len().filter(pl.col('len') > 1)
    if conflicts.height:
        raise ValueError('Conflicting native monthly lifecycle snapshots require source review')
    # Identical repeated captures remain cited in sources; no latest-snapshot selection.
    attributes = attributes.unique(semantic_fields).unique(['product', 'contract'])
    coverage = gaps.select('product', 'contract').unique().join(
        attributes, on=['product', 'contract'], how='left', validate='1:1')
    links = pl.read_parquet(links_path).filter(pl.col('operand') == 'physical_successor_and_termination')
    active = gaps.filter(pl.col('lost_inventory_continuation')).select(
        pl.col('date').cast(pl.String).alias('gap_date'), 'physical_contract')
    links = links.join(active, on=['gap_date', 'physical_contract'], how='semi')
    if links.height != active.height:
        raise ValueError('Every remaining continuation coordinate requires one exact retained operand')
    source_manifest = raw_source / 'source_manifest.json'
    raw_manifest = json.loads(source_manifest.read_text())
    raw_path = raw_source / 'observations/all_futures_daily_sessions.parquet'
    if sha256_file(raw_path) != raw_manifest['files']['observations/all_futures_daily_sessions.parquet']['sha256']:
        raise ValueError('Lifecycle own observation source changed')
    raw = pl.scan_parquet(raw_path).filter((pl.col('session') == '一般')
        & pl.col('product').is_in(links['product'].unique().to_list())).select(
            'date', 'product', 'contract', 'volume', 'open_interest', 'settlement', 'source_sha256').collect()
    if raw.select('date', 'product', 'contract').is_duplicated().any():
        raise ValueError('Lifecycle own daily observation key is ambiguous')
    own = raw.rename({'date': 'gap_date'}).with_columns(pl.col('gap_date').cast(pl.String))
    compared = links.join(own, on=['gap_date', 'product', 'contract'], how='left', validate='m:1').join(
        attributes, on=['product', 'contract'], how='left', validate='m:1')
    early = (pl.col('native_last_trade_date').eq(pl.col('gap_date'))
             & pl.col('native_last_trade_before_settlement') & pl.col('open_interest').eq(0)
             & ~pl.col('native_final_price_present')).fill_null(False)
    compared = compared.with_columns(pl.when(early).then(pl.lit('early_zero_oi_retirement_candidate'))
        .when(pl.col('open_interest') > 0).then(pl.lit('positive_inventory_owner_mapping_review'))
        .otherwise(pl.lit('own_generation_continuation_evidence_required')).alias('continuation_status'),
        pl.lit(False).alias('forced_fill_inferred'), pl.lit(False).alias('inventory_cleared'),
        pl.lit(False).alias('historical_rule_admitted'))
    sources.extend(dict(path=str(p), sha256=sha256_file(p)) for p in (source_manifest, raw_path))
    output.mkdir(parents=True)
    atomic_write_text(output / 'monthly_lifecycle_coverage.csv', coverage.write_csv())
    atomic_write_text(output / 'continuation_requirements.csv', compared.write_csv())
    result = dict(contract='futures_tej_own_month_lifecycle_diagnostics_v1',
        remaining_coordinates=gaps.height, product_months=coverage.height,
        native_months_matched=coverage['source_task_id'].count(), completed_attribute_queries=len(ids),
        continuation_coordinates=compared.height,
        continuation_status_counts=compared.group_by('continuation_status').len().sort('continuation_status').to_dicts(),
        provider_queries_sent=0, full_accounting_rebuilds=0, source_values_admitted=False,
        financial_values_inferred=False, inventory_cleared=False, all_resolved=False,
        generation_specific=False, sources=sources,
        outputs={p.name: dict(sha256=sha256_file(p)) for p in output.glob('*.csv')},
        implementation_sha256=sha256_file(Path(__file__)))
    atomic_write_json(output / 'manifest.json', result)
    return result


def audit_halt_events(root, inventory, registration, receipt_audit, output):
    """Reconcile the finite company-event query against the retained operands."""
    if output.exists():
        raise ValueError("Preserve the bound halt interpretation")
    reg = json.loads(registration.read_text())
    accepted = json.loads(receipt_audit.read_text())
    if (not accepted.get("accepted") or not accepted.get("all_selected_tasks_completed")
            or accepted.get("task_registration_sha256") != sha256_file(registration)):
        raise ValueError("Native receipt acceptance must precede halt interpretation")
    inv_path = inventory / "manifest.json"
    inv = json.loads(inv_path.read_text())
    links_path = inventory / "gap_operand_links.parquet"
    if sha256_file(links_path) != inv["outputs"][links_path.name]["sha256"]:
        raise ValueError("Halt operand census changed")
    links = pl.read_parquet(links_path)
    events, sources, exported_rows, omitted_rows = [], [], 0, 0
    for task in reg["tasks"]:
        if task["table_id"] != "0be66e50ef36b4318ce84d78":
            raise ValueError("Halt interpretation cannot consume an unrelated TEJ table")
        receipt_path = root / "receipts" / (task["task_id"] + ".json")
        receipt = json.loads(receipt_path.read_text())
        parquet = root / receipt["parquet_path"]
        if sha256_file(parquet) != receipt["parquet_sha256"]:
            raise ValueError("Halt native parquet changed")
        rows = pl.read_parquet(parquet).to_dicts()
        exported_rows += len(rows)
        omitted_rows += receipt.get("omitted_query_grid_rows", 0)
        for event in halt_events_from_rows(rows):
            events.append(event | dict(source_task_id=task["task_id"],
                source_parquet_sha256=receipt["parquet_sha256"]))
        sources.extend(dict(path=str(p), sha256=sha256_file(p)) for p in (receipt_path, parquet))
    frame = pl.DataFrame(events, schema_overrides={"resume_date": pl.String}) if events else pl.DataFrame(
        schema={"underlying_symbol": pl.String, "halt_start": pl.String, "resume_date": pl.String,
                "source_task_id": pl.String, "source_parquet_sha256": pl.String})
    index = defaultdict(list)
    for event in frame.to_dicts():
        index[event["underlying_symbol"]].append(event)
    matches = []
    for row in links.to_dicts():
        # Prior valuation follows its source-day dependency, not the resumed
        # contract's action date. Month ownership is retained in every link.
        own = [e for e in index[row["underlying_symbol"]]
            if row["required_date"] and e["halt_start"] <= row["required_date"]
            and (e["resume_date"] is None or row["required_date"] < e["resume_date"])]
        identities = {(e["halt_start"], e["resume_date"]) for e in own}
        matches.append(dict(gap_date=row["gap_date"], product=row["product"], contract=row["contract"],
            physical_contract=row["physical_contract"], operand=row["operand"],
            underlying_symbol=row["underlying_symbol"], required_date=row["required_date"],
            candidate_halt_events=len(identities),
            halt_starts=";".join(sorted({e["halt_start"] for e in own})),
            resume_dates=";".join(sorted({e["resume_date"] for e in own if e["resume_date"]})),
            source_task_ids=";".join(sorted({e["source_task_id"] for e in own})),
            classification="multiple_native_events_require_review" if len(identities) > 1 else
                "native_underlying_halt_requires_dated_futures_rule" if own else
                "no_matching_native_event_not_proof_of_no_halt",
            source_values_admitted=False))
    output.mkdir(parents=True)
    frame.write_csv(output / "native_halt_events.csv")
    mapped = pl.DataFrame(matches)
    mapped.write_csv(output / "halt_operand_event_links.csv")
    result = dict(contract="futures_tej_native_halt_event_evidence_v1", completed_queries=len(reg["tasks"]),
        exported_event_rows=exported_rows, omitted_query_grid_rows=omitted_rows,
        unique_company_events=frame.select("underlying_symbol", "halt_start", "resume_date").unique().height,
        operand_links=mapped.height,
        matched_blocked_coordinates=mapped.filter(pl.col("candidate_halt_events") > 0).select(
            "gap_date", "physical_contract").unique().height,
        missing_event_is_no_halt_proof=False, native_history_completeness_verified=False,
        financial_inputs_admitted=0, blocker_coordinates_removed=0, source_values_admitted=False,
        publication_verified=False, all_gaps_resolved=False,
        sources=sources + [dict(path=str(p), sha256=sha256_file(p))
            for p in (registration, receipt_audit, inv_path, links_path)],
        outputs={p.name: dict(sha256=sha256_file(p)) for p in output.iterdir() if p.is_file()},
        audit_code_sha256=sha256_file(Path(__file__)))
    atomic_write_json(output / "manifest.json", result)
    return result


def number(value):
    if value is None or value in {"", "(null)"}:
        return None
    return display_decimal(value)


def adjustment_role(native):
    reason = native.get("Reasons for Contract Adjustment")
    if reason == "契約調整重新推出標準契約":
        return "standard_contract_relisting"
    if reason in {"減資", "現增", "減資除息"}:
        return "adjustment_event"
    return "unverified_adjustment_role"


def compare_adjustment(native, terms, prior=None):
    shares = number(native.get("Shares per Contract"))
    reference = number(native.get("Reference Price"))
    expected_shares = Decimal(str(terms["contract_multiplier"]))
    role = adjustment_role(native)
    applicable = role == "adjustment_event"
    # Respect both captured display precisions. Integers remain exact; a 2 vs
    # 2000 error cannot hide behind a relative or half-share tolerance. This
    # classification never overwrites the official quantity.
    precisions = [d.normalize().as_tuple().exponent for d in (shares, expected_shares) if d is not None]
    fractional = [e for e in precisions if e < 0]
    tolerance = Decimal(5).scaleb(max(fractional) - 1) if fractional else Decimal(0)
    agrees = shares is not None and abs(shares - expected_shares) <= tolerance
    comparison = ("not_comparable_source_role" if not applicable else
                  "missing_native_quantity" if shares is None else
                  "exact" if shares == expected_shares else
                  "display_precision_difference" if agrees else "quantity_conflict")
    cash = number(native.get("Cash Dividends per Unit"))
    result = dict(native_reason=native.get("Reasons for Contract Adjustment"), native_role=role,
        native_adjustment_flag=native.get("Failure to Contract Adjustment"),
        native_adjustment_flag_semantics_verified=False,
        adjusted_quantity_comparison_applicable=applicable, quantity_comparison=comparison,
        display_precision_tolerance=str(tolerance),
        native_shares=str(shares) if shares is not None else None,
        expected_shares=str(expected_shares), shares_match_at_display_precision=agrees if applicable else None,
        native_reference=str(reference) if reference is not None else None,
        native_reference_positive=reference is not None and reference > 0,
        native_cash_deliverable=str(cash) if cash is not None else None,
        reference_is_daily_settlement=False, source_values_admitted=False)
    if applicable and prior is not None and reference is not None and reference > 0:
        credit = Decimal(str(terms.get("equity_cash_credit_twd") or 0))
        numerator = Decimal(terms.get("carry_quantity_numerator") or 1)
        denominator = Decimal(terms.get("carry_quantity_denominator") or 1)
        value = (Decimal(str(prior["settlement"])) * Decimal(str(prior["units"]))
                 + Decimal(str(prior.get("cash", 0))) - credit) * denominator / numerator
        reference_value = reference * expected_shares + Decimal(str(terms.get("deliverable_cash_twd") or 0))
        result.update(prior_economic_value_after_adjustment=str(value), reference_price_value=str(reference_value),
            reference_rounding_difference_twd=str(value - reference_value))
    return result


def audit(root, inventory, registration, terms_path, raw_source, receipt_audit, output, *, refresh=False):
    previous = None
    if output.exists():
        if not refresh:
            raise ValueError("Preserve the existing operand semantic audit; use an explicit interpretation refresh")
        previous = json.loads((output / "manifest.json").read_text())
        if previous.get("source_values_admitted") is not False:
            raise ValueError("Cannot refresh an admitted financial artifact as a diagnostic report")
        bound_paths = {Path(s["path"]).resolve() for s in previous["sources"]}
        requested_paths = {p.resolve() for p in (registration, receipt_audit, inventory / "manifest.json",
                                                terms_path, raw_source / "source_manifest.json")}
        if not requested_paths.issubset(bound_paths):
            raise ValueError("Interpretation refresh must retain the exact source inventory and registration")
        for s in previous["sources"]:
            if sha256_file(Path(s["path"])) != s["sha256"]:
                raise ValueError("Previous interpretation sources changed")
        if sha256_file(output / "adjustment_operand_comparison.csv") != previous["comparison_sha256"]:
            raise ValueError("Previous interpretation comparison changed")
    reg = json.loads(registration.read_text())
    accepted = json.loads(receipt_audit.read_text())
    if (not accepted.get("accepted") or not accepted.get("all_selected_tasks_completed")
            or accepted.get("task_registration_sha256") != sha256_file(registration)):
        raise ValueError("Native source receipt acceptance must precede financial interpretation")
    inv = json.loads((inventory / "manifest.json").read_text())
    expected_terms_sha = next(s["sha256"] for s in inv["sources"] if Path(s["path"]).resolve() == terms_path.resolve())
    if sha256_file(terms_path) != expected_terms_sha:
        raise ValueError("Source-bound pending terms changed")
    raw_manifest = json.loads((raw_source / "source_manifest.json").read_text())
    for name in ("specifications/specifications.parquet", "observations/all_futures_daily_sessions.parquet"):
        if sha256_file(raw_source / name) != raw_manifest["files"][name]["sha256"]:
            raise ValueError("Native prior/specification source changed")
    links_path = inventory / "gap_operand_links.parquet"
    if sha256_file(links_path) != inv["outputs"][links_path.name]["sha256"]:
        raise ValueError("Operand inventory changed")
    links = pl.read_parquet(links_path)
    terms = pl.read_parquet(terms_path).to_dicts()
    term_index = defaultdict(list)
    for t in terms:
        term_index[(t["product"], t["contract"], t["effective_date"])].append(t)
    specs = pl.read_parquet(raw_source / "specifications/specifications.parquet").to_dicts()
    raw = pl.scan_parquet(raw_source / "observations/all_futures_daily_sessions.parquet").filter(
        (pl.col("session") == "一般") & pl.col("product").is_in(links["required_product"].unique().to_list())).select(
            "date", "product", "contract", "settlement", "source_sha256").collect()
    raw_index = {(r["product"], r["contract"], r["date"].isoformat()): r for r in raw.to_dicts()}
    link_index = defaultdict(list)
    for r in links.to_dicts():
        link_index[(r["candidate_adjustment_product"], r["contract"], r["candidate_adjustment_date"])].append(r)
    comparisons, sources, totals = [], [], defaultdict(int)
    for task in reg["tasks"]:
        receipt_path = root / "receipts" / (task["task_id"] + ".json")
        receipt = json.loads(receipt_path.read_text())
        parquet = root / receipt["parquet_path"]
        if sha256_file(parquet) != receipt["parquet_sha256"]:
            raise ValueError("Native parquet changed")
        sources.append(dict(path=str(receipt_path), sha256=sha256_file(receipt_path)))
        rows = pl.read_parquet(parquet).to_dicts()
        totals["exported_rows"] += len(rows)
        for native in rows:
            if "Adjusted Date" not in native:
                totals["equity_event_rows"] += 1
                continue
            totals["adjustment_rows"] += 1
            match = re.fullmatch(r"([A-Z]{2}[F0-9])(\d{6})", native["_query_symbol"])
            if match is None:
                raise ValueError("Native futures company identity is not an own monthly contract")
            product, month = match.groups()
            day = normalize_period(native["Adjusted Date"])
            own = term_index[(product, month, day)]
            if len(own) != 1:
                comparisons.append(dict(product=product, contract=month, date=day,
                    status="not_one_exact_pending_event", native_reason=native.get("Reasons for Contract Adjustment"),
                    native_role=adjustment_role(native), native_adjustment_flag=native.get("Failure to Contract Adjustment"),
                    source_task_id=task["task_id"], source_parquet_sha256=receipt["parquet_sha256"],
                    source_values_admitted=False))
                continue
            t = own[0]
            prior = None
            candidates = {(r["required_product"], r["prior_positive_observation_date"])
                          for r in link_index[(product, month, day)] if r["prior_positive_observation_date"]}
            if candidates:
                origin, prior_day = max(candidates, key=lambda pair: pair[1])
                old = raw_index.get((origin, month, prior_day))
                if old and prior_day < day and old["settlement"] is not None:
                    old_terms = [r for r in terms if r["product"] == origin and r["contract"] == month
                                 and r["effective_date"] <= prior_day
                                 and (not r.get("valid_until_exclusive") or prior_day < r["valid_until_exclusive"])]
                    old_specs = [r for r in specs if r["product"] == origin and r["effective_date"].isoformat() <= prior_day
                                 and (r["valid_until_exclusive"] is None or prior_day < r["valid_until_exclusive"].isoformat())]
                    units, cash = None, 0
                    if origin.endswith("F") and len(old_specs) == 1:
                        units = old_specs[0]["contract_multiplier"]
                    elif old_terms:
                        last = max(old_terms, key=lambda r: r["effective_date"])
                        units, cash = last["contract_multiplier"], last["deliverable_cash_twd"]
                    if units is not None:
                        prior = dict(old, units=units, cash=cash or 0)
            compared = compare_adjustment(native, t, prior)
            status = ("exact_event_compared" if compared["adjusted_quantity_comparison_applicable"] else
                      "standard_contract_relisting_control" if compared["native_role"] == "standard_contract_relisting" else
                      "unverified_adjustment_role")
            comparisons.append(dict(product=product, contract=month, date=day, status=status, **compared,
                source_task_id=task["task_id"], source_parquet_sha256=receipt["parquet_sha256"]))
    output.mkdir(parents=True, exist_ok=refresh)
    if previous is not None and not (output / "interpretation_correction.json").exists():
        atomic_write_bytes(output / "prior_comparison.csv", (output / "adjustment_operand_comparison.csv").read_bytes())
        atomic_write_json(output / "interpretation_correction.json", dict(
            previous_manifest=previous, previous_comparison="prior_comparison.csv",
            correction="Separate standard-contract relisting rows and both source display precisions",
            financial_inputs_modified=False))
    frame = pl.DataFrame(comparisons)
    atomic_write_text(output / "adjustment_operand_comparison.csv", frame.write_csv())
    exact = frame.filter(pl.col("status") == "exact_event_compared")
    result = dict(contract="tej_gap_event_operand_semantics_v2", **totals,
        exact_pending_events_compared=exact.height,
        shares_conflicts=exact.filter(pl.col("quantity_comparison") == "quantity_conflict").height,
        display_precision_differences=exact.filter(pl.col("quantity_comparison") == "display_precision_difference").height,
        missing_native_quantity_rows=exact.filter(pl.col("quantity_comparison") == "missing_native_quantity").height,
        standard_contract_relisting_rows=frame.filter(pl.col("native_role") == "standard_contract_relisting").height,
        standard_relisting_rows_with_pending_event=frame.filter(pl.col("status") == "standard_contract_relisting_control").height,
        unverified_adjustment_role_rows=frame.filter(pl.col("native_role") == "unverified_adjustment_role").height,
        positive_reference_rows=exact.filter(pl.col("native_reference_positive")).height,
        financial_inputs_admitted=0, blocker_coordinates_removed=0, all_gaps_resolved=False,
        source_values_admitted=False, reference_is_daily_settlement=False,
        sources=sources + [dict(path=str(p), sha256=sha256_file(p)) for p in (
            registration, receipt_audit, inventory / "manifest.json", terms_path, raw_source / "source_manifest.json")],
        comparison_sha256=sha256_file(output / "adjustment_operand_comparison.csv"),
        audit_code_sha256=sha256_file(Path(__file__)))
    atomic_write_json(output / "manifest.json", result)
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path, default=Path("data_tej"))
    for name in ("inventory", "receipt-audit", "output"):
        p.add_argument("--" + name, type=Path, required=True)
    p.add_argument("--registration", type=Path)
    p.add_argument("--terms", type=Path)
    p.add_argument("--raw-source", type=Path)
    p.add_argument("--halt-events", action="store_true")
    p.add_argument("--lifecycle", action="store_true")
    p.add_argument("--gap-worklist", type=Path)
    p.add_argument("--refresh", action="store_true", help="Refresh only a source-identical diagnostic interpretation, preserving its prior evidence")
    a = p.parse_args()
    if a.lifecycle:
        if not a.gap_worklist or not a.raw_source or any((a.registration, a.terms, a.halt_events, a.refresh)):
            p.error("Lifecycle interpretation requires exact gaps/raw source, without financial terms or a new query plan")
        result = audit_lifecycle(a.root, a.inventory, a.gap_worklist, a.raw_source, a.receipt_audit, a.output)
    elif a.halt_events:
        if not a.registration or a.gap_worklist:
            p.error("Halt interpretation requires its registered query plan")
        if a.terms or a.raw_source or a.refresh:
            p.error("Halt event interpretation does not rewrite financial terms or an earlier audit")
        result = audit_halt_events(a.root, a.inventory, a.registration, a.receipt_audit, a.output)
    else:
        if not all((a.registration, a.terms, a.raw_source)) or a.gap_worklist:
            p.error("Adjustment interpretation requires its bound terms and raw source")
        result = audit(a.root, a.inventory, a.registration, a.terms, a.raw_source, a.receipt_audit, a.output, refresh=a.refresh)
    print(json.dumps({k: v for k, v in result.items() if not isinstance(v, (dict, list))}, ensure_ascii=False))


if __name__ == "__main__":
    main()
