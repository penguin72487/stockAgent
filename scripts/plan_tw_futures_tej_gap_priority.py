#!/usr/bin/env python3
"""Plan exact futures needs on captured TEJ axes, without touching the desktop."""
from __future__ import annotations

import argparse
from collections import defaultdict
from bisect import bisect_right
from contextlib import closing
from datetime import date, timedelta
import json
from pathlib import Path
import re
import sqlite3
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import polars as pl
from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from downloader.tej_history import SOURCE_SCOPE_CONTRACT, company_code, normalize_period
from downloader.tej_key_layout import KEY1_CONTRACT
from downloader.tej_priority import CONTRACT, pack_gap_rectangles, query_keys, retained_request

ATTRIBUTE = "1bea200ec21f8f1a39056861"
ADJUSTMENT = "117392c87504f94e71a04d01"
DAILY = "21383689fde1be3aeaa90211"
EX_RIGHT = "5908ef1a2f21c7f01139d1bb"
SUSPENDED = "0be66e50ef36b4318ce84d78"
OPERAND_CONTRACT = "futures_gap_actual_operand_inventory_v1"


def _positive(value):
    import math
    return isinstance(value, (int, float)) and math.isfinite(value) and value > 0


def _iso(value):
    return value.isoformat() if isinstance(value, date) else value or ""


def inventory_operands(gaps, frame, rules, raw, terms, universe):
    """Trace missing inputs without calculating, imputing or admitting values.

    A blocked day's prior value belongs to the previous market day and, at a
    corporate boundary, the source product. Last positive observations identify
    source investigation windows only; they are never used as missing prices.
    """
    keys = ["date", "physical_contract"]
    if gaps.select(keys).is_duplicated().any() or frame.select(keys).is_duplicated().any():
        raise ValueError("Ambiguous gap/accounting context")
    context = gaps.select(keys).join(frame, on=keys, how="left", validate="1:1")
    if context["product"].null_count():
        raise ValueError("Every blocked coordinate requires its latest accounting context")
    rule_context = {tuple(r[k] for k in keys): r for r in rules.to_dicts()}
    needed_pairs = gaps.select("product", "contract").unique()
    origins = terms.filter(pl.col("product").is_in(needed_pairs["product"].unique().to_list())).select(
        pl.col("from_product").alias("product"), "contract")
    needed_pairs = pl.concat([needed_pairs, origins]).unique()
    raw = raw.join(needed_pairs, on=["product", "contract"], how="semi")
    own_raw, histories = {}, defaultdict(list)
    for r in raw.to_dicts():
        key = (r["date"], r["product"], r["contract"])
        if key in own_raw:
            raise ValueError("Raw daily source key is ambiguous")
        own_raw[key] = r
        if _positive(r["settlement"]):
            histories[(r["product"], r["contract"])].append(r)
    history_days = {}
    for key, rows in histories.items():
        rows.sort(key=lambda r: r["date"])
        history_days[key] = [r["date"] for r in rows]
    term_index, outgoing = defaultdict(list), defaultdict(list)
    for r in terms.to_dicts():
        r = dict(r, effective_date=date.fromisoformat(r["effective_date"]))
        term_index[(r["product"], r["contract"])].append(r)
        outgoing[(r["from_product"], r["contract"])].append(r)
    for rows in [*term_index.values(), *outgoing.values()]:
        rows.sort(key=lambda r: r["effective_date"])
    names = {r["product"]: r.get("underlying_symbol", "") for r in universe.to_dicts()}
    lookup_days = set(context["date"]) | set(context["previous_market_date"].drop_nulls()) | set(context["next_market_date"].drop_nulls())
    lookup_frame = frame.filter(pl.col("date").is_in(sorted(lookup_days))).join(needed_pairs,
        on=["product", "contract"], how="semi")
    frame_rows = {(r["date"], r["product"], r["contract"]): r for r in lookup_frame.to_dicts()}
    contexts = {tuple(r[k] for k in keys): r for r in context.to_dicts()}
    operands = []
    groups = {
        "dated_margin_schedule": {"missing_opening_margin", "missing_settlement_margin", "settlement_rule_clock"},
        "dated_position_limit_schedule": {"missing_position_limit", "missing_position_clock"},
        "daily_clearing_or_legal_valuation": {"missing_valuation", "missing_settlement_value", "missing_terminal_value"},
        "prior_contract_value_before_open": {"missing_prior_valuation", "missing_price_limits", "missing_opening_value"},
        "physical_successor_and_termination": {"lost_inventory_continuation", "unresolved_lifetime"},
    }
    field_sets = {
        "dated_margin_schedule": "initial;maintenance;clearing_margin;published_at;effective_at;currency",
        "dated_position_limit_schedule": "natural_person_limit;unit;published_at;effective_at;research_policy",
        "daily_clearing_or_legal_valuation": "own_daily_settlement;legal_halt_formula;event_scope;published_at;effective_at",
        "prior_contract_value_before_open": "own_previous_settlement;own_previous_units;cash_credit;conversion;reference_price;known_at",
        "physical_successor_and_termination": "own_month_generation;next_market_day_owner;official_last_day;termination_semantics",
    }
    for gap in gaps.to_dicts():
        row = contexts[tuple(gap[k] for k in keys)]
        previous = row["previous_market_date"]
        events = [t for t in term_index[(row["product"], row["contract"])]
                  if previous is not None and previous < t["effective_date"] <= row["date"]]
        if len(events) > 1:
            raise ValueError("Corporate boundary has multiple source events")
        event = events[0] if events else None
        flags = {k for k, v in gap.items() if v is True and k not in {"has_blocker", "is_warmup"}}
        mapped = set().union(*groups.values())
        if flags - mapped:
            raise ValueError(f"Uninventoried financial flags: {sorted(flags - mapped)}")
        for operand, members in groups.items():
            reasons = flags & members
            if not reasons:
                continue
            required_product, required_date = row["product"], row["date"]
            if operand == "prior_contract_value_before_open":
                if row["executable"] and reasons == {"missing_opening_value"}:
                    operand = "own_open_execution_price"
                else:
                    required_date = previous
                    if event:
                        required_product = event["from_product"]
            elif operand == "physical_successor_and_termination":
                required_date = row["next_market_date"] or row["date"]
                required_product = row.get("corporate_transfer_target") or required_product
            raw_row = own_raw.get((required_date, required_product, row["contract"]), {})
            source_row = frame_rows.get((required_date, required_product, row["contract"]), {})
            observations = histories[(required_product, row["contract"])]
            pos = bisect_right(history_days.get((required_product, row["contract"]), []), required_date) if required_date else 0
            baseline = observations[pos - 1] if pos else {}
            next_positive = observations[pos] if pos < len(observations) else {}
            corporate = event
            if not corporate and baseline:
                candidates = [t for t in outgoing[(required_product, row["contract"])]
                              if baseline["date"] < t["effective_date"] <= row["date"] + timedelta(days=366)]
                corporate = candidates[0] if candidates else None
            if operand in {"dated_margin_schedule", "dated_position_limit_schedule"}:
                local_status = "dated_rule_chain_required"
            elif operand == "physical_successor_and_termination":
                local_status = "source_identity_or_termination_proof_required"
            elif _positive(source_row.get("settlement")):
                local_status = "local_accounting_value_present_binding_review_required"
            elif _positive(raw_row.get("settlement")):
                local_status = "local_observation_present_binding_review_required"
            else:
                local_status = "missing_observation_or_legal_halt_input"
            rule = rule_context.get(tuple(gap[k] for k in keys), {})
            operands.append(dict(gap_date=_iso(row["date"]), product=row["product"], contract=row["contract"],
                physical_contract=row["physical_contract"], operand=operand,
                blocked_flags=";".join(sorted(reasons)), required_product=required_product,
                required_contract=row["contract"], required_date=_iso(required_date),
                required_fields=field_sets.get(operand, "own_observed_open;executable_clock"),
                required_physical_contract=source_row.get("physical_contract", ""),
                dependency_raw_settlement=raw_row.get("settlement"),
                dependency_raw_source_sha256=raw_row.get("source_sha256", ""),
                prior_positive_observation_date=_iso(baseline.get("date")),
                prior_positive_observation_source_sha256=baseline.get("source_sha256", ""),
                next_positive_observation_date=_iso(next_positive.get("date")),
                candidate_adjustment_product=corporate["product"] if corporate else "",
                candidate_adjustment_date=_iso(corporate["effective_date"]) if corporate else "",
                candidate_adjustment_source_sha256s=";".join(corporate.get("source_content_sha256s", [])) if corporate else "",
                underlying_symbol=names.get(row["product"], ""),
                executable=bool(row["executable"]),
                inventory_entry_reachable=rule.get("inventory_entry_reachable"),
                local_status=local_status, source_values_admitted=False))
    return pl.DataFrame(operands)


def _read_context_chain(replay, diagnosis, gaps, *, frame_columns=None, rule_columns=None,
        scope_products=None, full_rule_scope=False, restore_context_prefix=False):
    columns = frame_columns or ["date", "product", "contract", "physical_contract", "previous_market_date", "next_market_date",
               "settlement", "executable", "cash_settlement", "corporate_transfer_target"]
    rule_columns = rule_columns or ["date", "physical_contract", "inventory_entry_reachable"]
    frames, rules, sources, seen, claimed = [], [], [], set(), set()
    wanted = set(gaps["product"]) if scope_products is None else set(scope_products)
    if not set(gaps["product"]) <= wanted:
        raise ValueError("Requested context scope omits a blocked product")
    while replay is not None:
        if replay.resolve() in seen:
            raise ValueError("Replay parent cycle")
        seen.add(replay.resolve())
        manifest_path = replay / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        sources.append(dict(path=str(manifest_path), sha256=sha256_file(manifest_path)))
        own = (set(manifest.get("affected_products", [])) & wanted) - claimed
        if own:
            for name, selected, target in [
                ("frame.parquet", columns, frames),
                ("compiled_rules.parquet", rule_columns, rules),
            ]:
                file = replay / name
                if sha256_file(file) != manifest["outputs"][name]["sha256"]:
                    raise ValueError("Replay context source SHA mismatch")
                data = pl.scan_parquet(file).filter(pl.col("product").is_in(sorted(own)))
                if name == "compiled_rules.parquet" and not full_rule_scope:
                    data = data.join(gaps.select("date", "physical_contract").lazy(), on=["date", "physical_contract"], how="semi")
                part = data.select([k for k in selected if k in data.collect_schema()]).collect()
                if name == "compiled_rules.parquet" and restore_context_prefix and manifest.get("context_only_prefix_policy"):
                    if not full_rule_scope:
                        raise ValueError("Restoring prefix originals requires the complete affected rule scope")
                    originals = []
                    for original_name in ("context_only_original_rules.parquet", "context_only_suppressed_carries.parquet"):
                        original = replay / original_name
                        digest = manifest["outputs"][original_name]["sha256"]
                        if sha256_file(original) != digest:
                            raise ValueError("Replay prefix original SHA mismatch")
                        originals.append(pl.scan_parquet(original).filter(pl.col("product").is_in(sorted(own)))
                            .select(selected).collect())
                        sources.append(dict(path=str(original), sha256=digest))
                    excluded, suppressed = originals
                    part = pl.concat([part.join(suppressed.select("date", "physical_contract"),
                        on=["date", "physical_contract"], how="anti"), suppressed, excluded])
                target.append(part)
                sources.append(dict(path=str(file), sha256=manifest["outputs"][name]["sha256"]))
            claimed.update(own)
        parent = manifest.get("previous_replay")
        if parent:
            replay = Path(parent)
            expected = manifest.get("previous_replay_manifest_sha256")
            if expected and sha256_file(replay / "manifest.json") != expected:
                raise ValueError("Replay parent identity changed")
        else:
            replay = None
    base_path = diagnosis / "manifest.json"
    base = json.loads(base_path.read_text())
    sources.append(dict(path=str(base_path), sha256=sha256_file(base_path)))
    for name, selected, target in [("frame", columns, frames),
            ("compiled_rules", rule_columns, rules)]:
        file = diagnosis / (name + ".parquet")
        if sha256_file(file) != base["outputs"][name]["sha256"]:
            raise ValueError("Diagnosis source SHA mismatch")
        data = pl.scan_parquet(file).filter(pl.col("product").is_in(sorted(wanted - claimed)))
        if name == "compiled_rules" and not full_rule_scope:
            data = data.join(gaps.select("date", "physical_contract").lazy(), on=["date", "physical_contract"], how="semi")
        target.append(data.select([k for k in selected if k in data.collect_schema()]).collect())
        sources.append(dict(path=str(file), sha256=base["outputs"][name]["sha256"]))
    return pl.concat(frames, how="diagonal_relaxed"), pl.concat(rules, how="diagonal_relaxed"), sources, len(seen)


def operand_query_requests(operands):
    requests = []
    event_dates = defaultdict(set)
    for row in operands.to_dicts():
        if row["operand"] == "physical_successor_and_termination":
            requests.append(dict(requested_fields="own_physical_successor_and_last_day", product=row["product"],
                contract=row["contract"], exact_dates=row["gap_date"], operand=row["operand"]))
        elif row["operand"] in {"daily_clearing_or_legal_valuation", "prior_contract_value_before_open", "own_open_execution_price"}:
            if row["local_status"] == "missing_observation_or_legal_halt_input" and row["required_date"]:
                requests.append(dict(requested_fields="own_month_open_and_clearing_value", product=row["required_product"],
                    contract=row["required_contract"], exact_dates=row["required_date"], operand=row["operand"]))
            if row["candidate_adjustment_product"]:
                key = (row["candidate_adjustment_product"], row["candidate_adjustment_date"], row["contract"], row["underlying_symbol"])
                if row["required_date"]:
                    event_dates[key].add(row["required_date"])
    # An event table contains adjustment dates, not one observation per halted
    # session. Probe the first missing operand day and the bound corporate date;
    # retain the full missing-day inventory, without repeatedly fetching it.
    for (product, effective, month, underlying), missing_days in sorted(event_dates.items()):
        dates = ";".join(sorted({effective, min(missing_days)}))
        requests.append(dict(requested_fields="corporate_adjustment_reference", product=product, contract=month,
            exact_dates=dates, operand="corporate_event_operands"))
        if underlying:
            requests.append(dict(requested_fields="equity_capital_reduction_event", product=underlying, contract="",
                exact_dates=dates, operand="corporate_event_operands"))
    return pl.DataFrame(requests).group_by("requested_fields", "product", "contract").agg(
        pl.col("exact_dates").str.split(";").explode().unique().sort().str.join(";").alias("exact_dates"),
        pl.col("operand").unique().sort().str.join(";").alias("operands"))


def write_inventory_tables(operands, catalog, output):
    all_required = operands.group_by("operand", "required_product", "required_contract", "required_fields", "local_status").agg(
        pl.col("gap_date").min().alias("first_blocked_day"), pl.col("gap_date").max().alias("last_blocked_day"),
        pl.col("required_date").unique().sort().str.join(";").alias("exact_operand_dates"),
        pl.len().alias("affected_coordinate_links"), pl.col("blocked_flags").unique().sort().str.join(";").alias("blocked_flags"))
    all_required.sort("operand", "required_product", "required_contract").write_csv(output / "all_required_data.csv")
    operands.filter(pl.col("operand").is_in(["daily_clearing_or_legal_valuation", "prior_contract_value_before_open"])).group_by(
        "required_product", "required_contract", "physical_contract", "candidate_adjustment_product", "candidate_adjustment_date",
        "prior_positive_observation_date", "prior_positive_observation_source_sha256", "candidate_adjustment_source_sha256s",
        "underlying_symbol").agg(pl.col("gap_date").min().alias("first_blocked_day"),
            pl.col("gap_date").max().alias("last_blocked_day"),
            pl.col("required_date").unique().sort().str.join(";").alias("exact_operand_dates"),
            pl.len().alias("operand_links")).sort("required_product", "required_contract", "first_blocked_day").write_csv(
                output / "valuation_and_reference_episodes.csv")
    matches = []
    for table in catalog:
        for field in json.loads(table["fields_json"]):
            if re.search(r"initial margin|maintenance margin|settlement margin|position limit|部位上限|原始保證金|維持保證金", field, re.I):
                matches.append(dict(table=table["name"], field=field,
                    futures_table_candidate="future" in table["name"].lower()))
    capabilities = [
        ("dated_margin_schedule", "no explicit futures schedule field in captured catalog", "initial;maintenance;clearing_margin;published_at;effective_at",
         "exact CPF dated schedule required; stock margin statistics are ineligible"),
        ("dated_position_limit_schedule", "no explicit futures limit field in captured catalog", "natural_person_limit;unit;published_at;effective_at",
         "approved research policy separately; observed OI is not a limit"),
        ("prior_contract_value_before_open", "Contract Adjustment of Stock Futures", "Reference Price;Shares per Contract;Cash Dividends per Unit",
         "own month/date, original event clock and financial identity still required"),
        ("corporate_event_quantities", "Ex_right Event", "Capital Decrease %;Cash Back PS;Share List Date;Stock Div. List Date",
         "equity event cross-check only; not futures daily settlement"),
        ("daily_clearing_or_legal_valuation", "Future DB", "SETTLE;OPEN;Volume;Open Interest",
         "do not repeat queried empty scopes; legal halt input remains required"),
        ("physical_successor_and_termination", "Future Attribute", "Listed Date;Trade Date_End;Settle Date_End;Settle Price_End",
         "snapshot cannot backdate last print as legal termination"),
        ("underlying_halt_event", "Company Suspended Records", "Suspended Re-trading",
         "event classification only; not a futures value, conversion or legal carry formula"),
    ]
    pl.DataFrame(capabilities, schema=["operand", "candidate_table", "fields", "admission"], orient="row").write_csv(
        output / "source_capabilities.csv")
    atomic_write_json(output / "margin_and_limit_field_search.json", dict(tables_searched=len(catalog),
        matching_fields=matches, futures_schedule_fields_found=sum(r["futures_table_candidate"] for r in matches),
        conclusion_scope="currently captured account catalog only"))
    return all_required.height


def halt_event_requests(operands, native_dates):
    """Query the company event once, independent of the futures month count.

    Investigate actual input dates and the adjacent observed quote controls.
    Never expand two separate events into the intervening years. A future
    quote is an event control, not an accounting input. A missing event row
    never authorizes price carry.
    """
    windows = defaultdict(set)
    for row in operands.to_dicts():
        symbol, day = row["underlying_symbol"], row["required_date"]
        if not symbol or not day:
            continue
        windows[symbol].add(day)
        for field in ("prior_positive_observation_date", "next_positive_observation_date"):
            if row[field]:
                windows[symbol].add(row[field])
    needed = defaultdict(set)
    for symbol, missing in windows.items():
        needed[symbol].update(d for d in native_dates if d in missing)
        # Exact boundaries remain visible to plan_gaps if absent from the
        # native axis; silently dropping them would claim incomplete coverage.
        needed[symbol].update(missing)
    return pl.DataFrame([dict(requested_fields="equity_halt_event", product=symbol, contract="",
        exact_dates=";".join(sorted(days)), operands="underlying_halt_event")
        for symbol, days in sorted(needed.items())], schema={"requested_fields": pl.String,
            "product": pl.String, "contract": pl.String, "exact_dates": pl.String, "operands": pl.String})


def build_halt_inventory(root, gap_path, retained_inventory, terms_path, output):
    """Reuse the exact operand census; no full ledger read or rebuild.

    This is a bounded subset of a hash-bound inventory, not a new observation
    or a way to reuse stale keys after financial terms change.
    """
    if output.exists():
        raise ValueError("Preserve the bound halt event inventory")
    manifest_path = retained_inventory / "manifest.json"
    retained = json.loads(manifest_path.read_text())
    gap_manifest = gap_path.parent / "manifest.json"
    latest = json.loads(gap_manifest.read_text())
    if (retained.get("contract") != OPERAND_CONTRACT
            or sha256_file(gap_path) != latest["outputs"][gap_path.name]["sha256"]):
        raise ValueError("Halt inventory requires the bound latest gaps and actual operand census")
    term_sources = [s for s in retained["sources"] if Path(s["path"]) == terms_path]
    if len(term_sources) != 1 or sha256_file(terms_path) != term_sources[0]["sha256"]:
        raise ValueError("Retained operands cannot be reused after corporate terms change")
    links_path = retained_inventory / "gap_operand_links.parquet"
    if sha256_file(links_path) != retained["outputs"][links_path.name]["sha256"]:
        raise ValueError("Retained operand coordinates changed")
    gaps = pl.read_csv(gap_path, schema_overrides={"contract": pl.String}, try_parse_dates=False)
    gaps = gaps.filter(~((pl.col("product") == "CPF") & (pl.col("date") < "2010-01-01")))
    keys = ["gap_date", "physical_contract"]
    gaps = gaps.rename({"date": "gap_date"})
    if gaps.select(keys).is_duplicated().any():
        raise ValueError("Ambiguous current halt coordinates")
    current = {tuple(r[k] for k in keys): r for r in gaps.to_dicts()}
    selected = pl.read_parquet(links_path).join(gaps.select(keys), on=keys, how="semi")
    rows, accounted = [], defaultdict(set)
    for row in selected.to_dicts():
        key = tuple(row[k] for k in keys)
        gap = current[key]
        if (row["product"], row["contract"]) != (gap["product"], gap["contract"]):
            raise ValueError("Retained operand owner differs from current coordinate")
        reasons = {f for f in row["blocked_flags"].split(";") if gap.get(f) is True}
        if reasons:
            row["blocked_flags"] = ";".join(sorted(reasons))
            rows.append(row)
            accounted[key].update(reasons)
    for key, gap in current.items():
        reasons = {f for f, value in gap.items() if value is True and f not in {"has_blocker", "is_warmup"}}
        if reasons != accounted[key]:
            raise ValueError("Current halt flags are not covered by retained causal operands")
    operands = pl.DataFrame(rows, schema=selected.schema)
    with closing(sqlite3.connect(f"file:{root / 'queue.sqlite3'}?mode=ro", uri=True)) as con:
        con.row_factory = sqlite3.Row
        catalog = [dict(r) for r in con.execute("SELECT table_id,name,category,fields_json,state,discovery_path,source_key_mode FROM tables ORDER BY table_id")]
    halt = next(t for t in catalog if t["table_id"] == SUSPENDED)
    axes_path = root / halt["discovery_path"]
    axes = json.loads(axes_path.read_text())
    requests = pl.concat([operand_query_requests(operands),
        halt_event_requests(operands, {normalize_period(d) for d in axes["date_labels"]})])
    output.mkdir(parents=True)
    atomic_write_parquet(output / "gap_operand_links.parquet", operands)
    gaps.write_csv(output / "current_halt_coordinates.csv")
    requests.sort("requested_fields", "product", "contract").write_csv(output / "operand_requests.csv")
    groups = write_inventory_tables(operands, catalog, output)
    summary = dict(contract="futures_halt_event_operand_subset_v1", remaining_coordinates=gaps.height,
        covered_coordinates=len(accounted), products=gaps["product"].n_unique(),
        product_months=gaps.select("product", "contract").unique().height,
        operand_links=operands.height, all_required_groups=groups, request_groups=requests.height,
        company_halt_request_groups=requests.filter(pl.col("requested_fields") == "equity_halt_event").height,
        source_values_admitted=False, all_gaps_resolved=False, full_accounting_rebuilds=0,
        source_semantics="retained causal operands; candidate events are investigation keys, not valuation facts",
        planning_code_sha256=sha256_file(Path(__file__)),
        sources=[dict(path=str(p), sha256=sha256_file(p)) for p in
            (manifest_path, links_path, gap_manifest, gap_path, terms_path, axes_path)],
        outputs={p.name: dict(sha256=sha256_file(p)) for p in output.iterdir() if p.is_file()})
    atomic_write_json(output / "manifest.json", summary)
    return summary


def build_operand_inventory(root, replay, diagnosis, raw_source, terms_path, output):
    if output.exists():
        raise ValueError("Preserve the existing bound operand inventory")
    gap_path = replay / "remaining_source_gaps.csv"
    replay_manifest = json.loads((replay / "manifest.json").read_text())
    if sha256_file(gap_path) != replay_manifest["outputs"][gap_path.name]["sha256"]:
        raise ValueError("Latest gap worklist changed")
    gaps = pl.read_csv(gap_path, try_parse_dates=True, schema_overrides={"contract": pl.String})
    frame, rules, sources, chain_length = _read_context_chain(replay, diagnosis, gaps)
    raw_manifest = json.loads((raw_source / "source_manifest.json").read_text())
    raw_path = raw_source / "observations/all_futures_daily_sessions.parquet"
    universe_path = raw_source / "universe/products.csv"
    for file in (raw_path, universe_path):
        if sha256_file(file) != raw_manifest["files"][str(file.relative_to(raw_source))]["sha256"]:
            raise ValueError("Retained raw source changed")
    pending_manifest_path = terms_path.parent / "manifest.json"
    pending = json.loads(pending_manifest_path.read_text())
    if (sha256_file(terms_path) != pending["outputs"][terms_path.name]["sha256"]
            or pending["parent_source_manifest_sha256"] != sha256_file(raw_source / "source_manifest.json")):
        raise ValueError("Pending corporate terms are not bound to the retained raw release")
    terms = pl.read_parquet(terms_path)
    products = sorted(set(frame["product"]) | set(terms.filter(pl.col("product").is_in(gaps["product"].to_list()))["from_product"]))
    raw = pl.scan_parquet(raw_path).filter((pl.col("session") == "一般") & pl.col("product").is_in(products)).select(
        "date", "product", "contract", "settlement", "source_sha256").collect()
    universe = pl.read_csv(universe_path, infer_schema=False)
    operands = inventory_operands(gaps, frame, rules, raw, terms, universe)
    links = operands.select("gap_date", "product", "contract", "physical_contract").unique()
    if links.height != gaps.height:
        raise ValueError("Inventory does not cover every gap exactly")
    requests = operand_query_requests(operands)
    with closing(sqlite3.connect(f"file:{root / 'queue.sqlite3'}?mode=ro", uri=True)) as con:
        con.row_factory = sqlite3.Row
        catalog = [dict(r) for r in con.execute("SELECT table_id,name,category,fields_json,state,discovery_path,source_key_mode FROM tables ORDER BY table_id")]
        feature_count = con.execute("SELECT count(*) FROM features").fetchone()[0]
    output.mkdir(parents=True)
    atomic_write_parquet(output / "gap_operand_links.parquet", operands)
    requests.sort("requested_fields", "product", "contract").write_csv(output / "operand_requests.csv")
    all_required_groups = write_inventory_tables(operands, catalog, output)
    # Full catalog preserves positive and negative field-search evidence.
    atomic_write_json(output / "tej_catalog.json", dict(tables=catalog, tables_count=len(catalog), feature_count=feature_count))
    summary = dict(contract=OPERAND_CONTRACT, remaining_coordinates=gaps.height, covered_coordinates=links.height,
        operand_links=operands.height, request_groups=requests.height, all_required_groups=all_required_groups, replay_chain_length=chain_length,
        operands=operands.group_by("operand", "local_status").len().sort("operand", "local_status").to_dicts(),
        tej_tables_searched=len(catalog), tej_features_searched=feature_count,
        source_values_admitted=False, all_gaps_resolved=False, full_accounting_rebuilds=0,
        event_query_dates_contract="first_missing_operand_day_and_bound_corporate_day_v1",
        planning_code_sha256=sha256_file(Path(__file__)),
        sources=sources + [dict(path=str(p), sha256=sha256_file(p)) for p in
            (gap_path, pending_manifest_path, terms_path, raw_source / "source_manifest.json", raw_path, universe_path)],
        outputs={p.name: dict(sha256=sha256_file(p)) for p in output.iterdir() if p.is_file()})
    atomic_write_json(output / "manifest.json", summary)
    return summary


def plan_gaps(root: Path, worklist: Path, terms_path: Path, output: Path, *, query_expansion: float = 2.0) -> dict:
    if output.exists():
        raise ValueError("Preserve the existing source-bound plan")
    if not 1 <= query_expansion <= 4:
        raise ValueError("Sparse query expansion must be bounded by 1..4")
    work = pl.read_csv(worklist, schema_overrides={"contract": pl.String}).to_dicts()
    terms = pl.read_parquet(terms_path)
    requests, diagnoses = [], {}
    with closing(sqlite3.connect(f"file:{root / 'queue.sqlite3'}?mode=ro", uri=True)) as con:
        con.row_factory = sqlite3.Row
        con.execute("BEGIN")
        config = json.loads(con.execute("SELECT value FROM meta WHERE key='config'").fetchone()[0])
        operand_mode = any("operands" in r for r in work)
        table_ids = (ATTRIBUTE, ADJUSTMENT, DAILY, EX_RIGHT) if operand_mode else (ATTRIBUTE, ADJUSTMENT, DAILY)
        if any(r["requested_fields"] == "equity_halt_event" for r in work):
            table_ids += (SUSPENDED,)
        for tid in table_ids:
            definition = dict(con.execute("SELECT * FROM tables WHERE table_id=?", (tid,)).fetchone())
            axes_path = root / definition["discovery_path"]
            axes = json.loads(axes_path.read_text())
            labels = {company_code(label): label for label in axes["company_labels"]}
            if len(labels) != len(axes["company_labels"]):
                raise ValueError("Captured company aliases are ambiguous")
            dates = {normalize_period(label): label for label in axes["date_labels"]}
            names = json.loads(definition["fields_json"])
            prior, covered = [], set()
            for row in con.execute("SELECT * FROM tasks WHERE table_id=? AND kind='download' AND "
                                   "state IN ('complete','pending','running','blocked')", (tid,)):
                q = retained_request(dict(row), definition)
                if q is None:
                    prior.append({"task_id": row["task_id"], "state": row["state"],
                                  "scope_contract": row["scope_contract"], "exclusion": "legacy_menu_only_plan"})
                    continue
                if set(q["fields"]) != set(names):
                    raise ValueError("Partial previous field scope requires explicit partitioning")
                covered.update(query_keys(q))
                prior.append({"task_id": row["task_id"], "state": row["state"], "scope_contract": row["scope_contract"],
                              "receipt_path": row["receipt_path"], "source_observation_admitted": False})
            needed = defaultdict(set)
            unknown = []
            for row in work:
                symbol = row["product"] + row["contract"]
                kind = row["requested_fields"]
                if tid == ATTRIBUTE:
                    if kind not in {"own_physical_successor_and_last_day", "own_month_open_and_clearing_value",
                                    "contract_deliverable_and_conversion"}:
                        continue
                    periods = {None}
                elif tid == DAILY:
                    if kind != "own_month_open_and_clearing_value":
                        continue
                    periods = set(row["exact_dates"].split(";"))
                elif tid == EX_RIGHT:
                    if kind != "equity_capital_reduction_event":
                        continue
                    periods = set(row["exact_dates"].split(";"))
                elif tid == SUSPENDED:
                    if kind != "equity_halt_event":
                        continue
                    periods = set(row["exact_dates"].split(";"))
                else:
                    if kind == "corporate_adjustment_reference":
                        periods = set(row["exact_dates"].split(";"))
                    elif kind == "contract_deliverable_and_conversion":
                        own = terms.filter((pl.col("product") == row["product"]) & (pl.col("contract") == row["contract"]))
                        if own.height != 1:
                            raise ValueError("Own rights event is ambiguous")
                        first = own["effective_date"][0]
                        first = first.isoformat() if isinstance(first, date) else str(first)
                        periods = {d for d in dates if first <= d <= row["last_date"]}
                    else:
                        continue
                if symbol not in labels:
                    unknown.append({"symbol": symbol, "reason": "not_in_captured_native_company_axis"})
                    continue
                for period in periods:
                    if period is not None and period not in dates:
                        unknown.append({"symbol": symbol, "period": period, "reason": "not_in_captured_native_date_axis"})
                    elif (symbol, period) not in covered:
                        needed[symbol].add(period)
            base = {"contract_version": 4, "action": "download", "type": definition["query_type"],
                    "smart_id": definition["smart_id"], "table": definition["name"],
                    "frequency": "snapshot" if tid == ATTRIBUTE else "daily", "fields": names, "catalog_fields": names,
                    "max_rows": config["max_rows_per_export"], "max_cells": config["max_cells_per_export"]}
            if tid == ATTRIBUTE:
                if definition["source_key_mode"] != 1:
                    raise ValueError("Attribute requires previously verified source Key=1")
                cap = min(config["max_companies_per_export"], config["max_rows_per_export"],
                          config["max_cells_per_export"] // (len(names) + 1) - 1)
                symbols = sorted(needed)
                geometries = [{"companies": symbols[start:start + cap], "dates": [],
                               "needed_rows": len(symbols[start:start + cap])}
                              for start in range(0, len(symbols), cap)]
                base.update(source_key_mode=1, key_layout_contract=KEY1_CONTRACT,
                            start=config["history_search_start"], end="2026-10-01")
                priority, reason = -30, "own-month lifecycle and terminal attributes; current snapshot only"
            else:
                capacity = min(config["max_rows_per_export"], config["max_cells_per_export"] // (len(names) + 2) - 1)
                geometries = pack_gap_rectangles(needed, forbidden=covered,
                    max_companies=config["max_companies_per_export"], max_rows=capacity, expansion=query_expansion)
                priority, reason = {
                    ADJUSTMENT: (-40, "own-month corporate reference, shares and cash operands; event dates retained"),
                    EX_RIGHT: (-35, "underlying capital-reduction ratio, cash and share dates; not futures clearing values"),
                    SUSPENDED: (-45, "deduplicated underlying halt/re-trading events; no futures valuation admitted"),
                    DAILY: (-20, "actual own-month and operand date clearing/open values; native control cells retained"),
                }[tid]
            for group in geometries:
                q = {**base, "company_labels": [labels[s] for s in group["companies"]],
                     "date_labels": [dates[d] for d in group["dates"]]}
                if group["dates"]:
                    q.update(start=min(group["dates"]), end=max(group["dates"]))
                requests.append({"table_id": tid, "discovery_sha256": sha256_file(axes_path),
                                 "request": q, "priority": priority, "reason": reason})
            diagnoses[tid] = {"table": definition["name"], "prior_tasks": prior,
                              "missing_native_axis": unknown, "unqueried_needed_keys": sum(map(len, needed.values())),
                              "planned_queries": len(geometries),
                              "requested_grid_rows": sum(len(g["companies"]) * (len(g["dates"]) or 1) for g in geometries)}
        con.rollback()
    source_paths = [worklist, terms_path]
    if operand_mode:
        source_paths.append(worklist.parent / "manifest.json")
    plan = {"contract": CONTRACT,
            "sources": [{"path": str(p), "sha256": sha256_file(p)} for p in source_paths],
            "requests": requests, "diagnosis": diagnoses,
            "margin_table_queries": 0, "desktop_actions_during_planning": 0,
            "sparse_query_expansion_bound": query_expansion, "global_optimality_claimed": False}
    atomic_write_json(output, plan)
    return plan


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path, default=Path("data_tej"))
    p.add_argument("--worklist", type=Path)
    p.add_argument("--terms", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--replay", type=Path)
    p.add_argument("--diagnosis", type=Path)
    p.add_argument("--raw-source", type=Path)
    p.add_argument("--inventory-output", type=Path)
    p.add_argument("--halt-gap-worklist", type=Path)
    p.add_argument("--retained-operand-inventory", type=Path)
    p.add_argument("--query-expansion", type=float, default=2.0)
    args = p.parse_args()
    inventory_args = (args.replay, args.diagnosis, args.raw_source, args.inventory_output)
    if args.halt_gap_worklist or args.retained_operand_inventory:
        if (not all((args.halt_gap_worklist, args.retained_operand_inventory, args.inventory_output))
                or any((args.replay, args.diagnosis, args.raw_source, args.worklist))):
            p.error("Halt subset requires its latest gaps, retained operand inventory and inventory output only")
        inventory = build_halt_inventory(args.root, args.halt_gap_worklist,
            args.retained_operand_inventory, args.terms, args.inventory_output)
        print(json.dumps({k: inventory[k] for k in ("remaining_coordinates", "covered_coordinates",
            "products", "product_months", "operand_links", "all_required_groups",
            "company_halt_request_groups", "full_accounting_rebuilds")}, ensure_ascii=False), flush=True)
        worklist = args.inventory_output / "operand_requests.csv"
    elif all(inventory_args) and args.worklist is None:
        inventory = build_operand_inventory(args.root, args.replay, args.diagnosis, args.raw_source, args.terms, args.inventory_output)
        print(json.dumps({k: inventory[k] for k in ("remaining_coordinates", "covered_coordinates", "operand_links",
            "request_groups", "tej_tables_searched", "tej_features_searched")}, ensure_ascii=False), flush=True)
        worklist = args.inventory_output / "operand_requests.csv"
    elif not any(inventory_args) and args.worklist is not None:
        worklist = args.worklist
    else:
        p.error("Use a worklist or all four source-bound inventory arguments")
    plan = plan_gaps(args.root, worklist, args.terms, args.output, query_expansion=args.query_expansion)
    print(json.dumps({"queries": len(plan["requests"]), "tables": {k: {
        **{f: v[f] for f in ("table", "unqueried_needed_keys", "planned_queries", "requested_grid_rows")},
        "missing_native_axis_keys": len(v["missing_native_axis"])}
        for k, v in plan["diagnosis"].items()}}, ensure_ascii=False))


if __name__ == "__main__":
    main()
