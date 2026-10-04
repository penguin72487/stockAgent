#!/usr/bin/env python3
"""Rebuild a disclosed margin/carry research release from exact source evidence.

Run on the training node. All calendar, numeric binding, accounting, validation
and publication semantics belong to the existing shared implementations.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import inspect
from datetime import UTC, date, datetime
import json
from pathlib import Path
import shutil
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import polars as pl
from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from scripts.prepare_tw_futures_physical_history import raw_physical_observations
from stockagent.data.tw_stock_futures_repair import official_day_evidence
from stockagent.data.tw_futures_margin_preparation import (
    physical_lifetime_calendar, load_preparation_final_settlements,
    derive_adjusted_final_fixings, load_same_security_final_fixing_rule,
    load_adjusted_zero_oi_rule, load_loss_reduction_halt_review,
    apply_loss_reduction_halt_values, bind_dated_corporate_terms,
    bind_equity_margin_families, bind_equity_position_families,
    bind_physical_position_inputs, bind_adjusted_terminal_values, equity_contract_families,
)
from stockagent.data.tw_futures_margin_release import (
    read_bound_output, materialize_margin_market_rows, margin_execution_dependency_rows,
    select_complete_margin_components, publish_all_twd_margin_release,
)
from stockagent.data.tw_futures_execution_terms import compile_execution_terms, EXECUTION_TERMS_COMPILER_VERSION
from stockagent.data.tw_futures_margin import (
    validate_margin_carry_rules, validate_margin_value_bases,
    validate_margin_second_position_limit, validate_margin_grandfather_rules,
)
from stockagent.data.tw_futures_portfolio_daily import (
    futures_slot_layout_version, TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION,
)


def repair_release_metadata(source: Path, output: Path) -> dict:
    """Correct a missing loader envelope while preserving every financial byte."""
    accepted_path = output / "build_acceptance.json"
    accepted = json.loads(accepted_path.read_text())
    checkpoint = json.loads((output / "accounting_checkpoint.json").read_text())
    if checkpoint["inputs"]["calculation"] != _calculation_identity():
        raise ValueError("metadata repair cannot change financial calculations")
    if sha256_file(source / "source_manifest.json") != accepted["source_manifest_sha256"]:
        raise ValueError("metadata repair source identity differs")
    daily = output / "release/daily/continuous_daily.parquet"
    rules = output / "release/rules/rules.parquet"
    if sha256_file(daily) != accepted["daily_sha256"] or sha256_file(rules) != accepted["rules_sha256"]:
        raise ValueError("metadata repair cannot change financial observations/rules")
    from stockagent.data.tw_futures_margin import validate_margin_rule_source
    validate_margin_rule_source(rules, daily)
    slots = int(checkpoint["inputs"]["slots"])
    path = daily.with_name("manifest.json")
    before = json.loads(path.read_text())
    corrected = dict(contract_version=futures_slot_layout_version(slots),
        feature_contract_version=TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION,
        fixed_model_output_slots=slots)
    if any(k in before and before[k] != v for k, v in corrected.items()):
        raise ValueError("metadata repair may only fill omitted, source-bound contract fields")
    if all(before.get(k) == v for k, v in corrected.items()):
        return dict(status="unchanged")
    coords = pl.scan_parquet(daily).select(pl.col("symbol").str.extract(r"TAIFEX_SLOT_(\d+)$",1)
        .cast(pl.Int64).alias("slot")).select(pl.col("slot").min(),pl.col("slot").max().alias("maximum"),
        pl.col("slot").null_count().alias("nulls")).collect().row(0)
    if coords[2] or not 1 <= coords[0] <= coords[1] <= slots:
        raise ValueError("actual source coordinates violate the retained slot count")
    atomic_write_json(output / "daily_manifest_before_contract_repair.json", before)
    atomic_write_json(output / "build_acceptance_before_contract_repair.json", accepted)
    old_sha = sha256_file(path)
    atomic_write_json(path, dict(before, **corrected))
    proof = dict(status="omitted_training_contract_metadata_repaired", previous_manifest_sha256=old_sha,
        manifest_sha256=sha256_file(path), original_builder_sha256=accepted["builder_sha256"],
        effective_builder_sha256=sha256_file(Path(__file__)), fields=corrected,
        financial_rows_recompiled=0, financial_bytes_changed=False,
        daily_sha256=sha256_file(daily), rules_sha256=sha256_file(rules))
    atomic_write_json(output / "contract_metadata_repair.json", proof)
    accepted.update(original_builder_sha256=accepted["builder_sha256"], builder_sha256=proof["effective_builder_sha256"],
        contract_metadata_repair_sha256=sha256_file(output / "contract_metadata_repair.json"))
    atomic_write_json(accepted_path, accepted)
    print(json.dumps(proof), flush=True)
    return proof


def _compile_accounting(source, universe, *, output, end, slots, save, log,
                        rule_source=None, scope_products=None, portfolio_lifetimes=None,
                        information_halt_reviews=(), valuation_research_policy=None):
    """Canonical financial calculation, separately fingerprinted for reuse."""
    full_universe = universe
    products = universe["product"].to_list()
    if scope_products is not None:
        if not scope_products or not set(scope_products) <= set(products) or portfolio_lifetimes is None:
            raise ValueError("affected accounting requires complete parent slots and a nonempty product subset")
    elif portfolio_lifetimes is not None:
        raise ValueError("parent slot metadata is only valid for an explicit affected scope")
    rule_source = source / "rules" if rule_source is None else rule_source
    if rule_source != source / "rules":
        proof = json.loads((rule_source / "manifest.json").read_text())
        if proof.get("parent_source_manifest_sha256") != sha256_file(source / "source_manifest.json"):
            raise ValueError("rule delta belongs to a different raw source release")
    raw, _ = read_bound_output(source / "observations/all_futures_daily_sessions.parquet")
    raw = raw.filter(pl.col("date") <= end)
    observed = raw_physical_observations(raw, universe)
    calendar = observed.select("date").unique().sort("date")
    if scope_products is not None:
        products = sorted(set(scope_products))
        universe = universe.filter(pl.col("product").is_in(products))
        observed = observed.filter(pl.col("product").is_in(products))
        raw = raw.filter(pl.col("product").is_in(products))
    cf, _ = read_bound_output(rule_source / "corporate_event_candidates.parquet")
    terms, _ = read_bound_output(rule_source / "corporate_terms_intervals.parquet")
    units_intervals, _ = read_bound_output(rule_source / "corporate_unit_intervals.parquet")
    margin_levels, _ = read_bound_output(rule_source / "margin_level_intervals.parquet")
    position_levels, _ = read_bound_output(rule_source / "position_level_intervals.parquet")
    specifications, _ = read_bound_output(source / "specifications/specifications.parquet")
    policy = json.loads((source / "position_research_policy.json").read_text())
    final_path = source / "final/futures_final_settlement_history.parquet"
    final = load_preparation_final_settlements(final_path).filter(pl.col("product").is_in(products))
    underlying = load_same_security_final_fixing_rule(source / "underlying_final_rule/review.json")
    derived = derive_adjusted_final_fixings(observed, final, terms, universe, **underlying)
    final = pl.concat([final.with_columns(pl.lit("official_product_final").alias("final_fixing_origin")),
                       derived], how="diagonal_relaxed")
    physical, lives = physical_lifetime_calendar(observed, final, calendar, corporate=cf,
        delisting_rule=load_adjusted_zero_oi_rule(source / "adjusted_lifecycle_rule/review.json"))
    save("physical_lifetimes", lives)
    log("physical_calendar", rows=physical.height, lives=lives.height, products=universe.height)
    receipt = json.loads((source / "official_daily/manifest.json").read_text())
    portable = dict(receipt, receipts=[dict(r, path=str(source / r["path"])) for r in receipt["receipts"]])
    atomic_write_json(output / "official_daily_manifest.json", portable)
    evidence = official_day_evidence(output / "official_daily_manifest.json",
        physical.select("date", "physical_contract"), output / "official_daily", allow_unreported_volume=True)
    physical = physical.join(evidence, on=["date", "physical_contract"], how="left", validate="1:1").with_columns(
        pl.col("official_settlement").str.replace_all(",", "").cast(pl.Float64, strict=False).alias("daily_mark"))
    cash = ((pl.col("date") == pl.col("official_expiry"))
        & (pl.col("settlement_method") == "cash_settlement")).fill_null(False)
    physical = physical.with_columns(cash.alias("cash_settlement"),
        pl.when(cash).then(pl.col("final_settlement_price")).otherwise(pl.col("daily_mark")).alias("valuation_price"))
    legal_values = []
    for review in sorted((source / "halt").glob("*/review.json")):
        physical, values = apply_loss_reduction_halt_values(physical, calendar, load_loss_reduction_halt_review(review))
        legal_values.append(values)
    save("legal_halt_values", pl.concat(legal_values, how="vertical"))
    if information_halt_reviews:
        from stockagent.data.tw_futures_information_halt import apply_information_halt_values
        information_values = []
        for episode in information_halt_reviews:
            physical, values = apply_information_halt_values(physical, calendar, episode,
                corporate=cf, corporate_input_sha256=sha256_file(rule_source / "corporate_event_candidates.parquet"))
            information_values.append(values)
        save("information_halt_values", pl.concat(information_values, how="vertical"))
    if valuation_research_policy is not None:
        from stockagent.data.tw_futures_valuation_research import prepare_frozen_value_physical_history
        physical, valuation_values = prepare_frozen_value_physical_history(physical, calendar,
            terms=terms, specifications=specifications, policy=valuation_research_policy,
            corporate_candidates=cf, final_settlements=final)
        save("research_valuation_rows", valuation_values)
        log("research_valuations_applied", rows=valuation_values.height)
    save("physical_daily_marks", physical)
    parent_lives = None
    if scope_products is not None:
        own_lives = physical.group_by("product", "contract", "physical_instance").agg(
            pl.col("date").min().alias("first_observed_date"),
            pl.col("date").max().alias("last_observed_date"))
        parent_lives = pl.concat([portfolio_lifetimes.filter(~pl.col("product").is_in(products)).select(own_lives.columns),
                                 own_lives], how="vertical")
        save("portfolio_slot_lifetimes", parent_lives)
    frame, _ = materialize_margin_market_rows(physical, raw, universe, slot_count=slots, market_dates=calendar,
        portfolio_lifetimes=parent_lives, portfolio_universe=full_universe if parent_lives is not None else None)
    days = frame.select("date", "product", "contract").unique()
    units = bind_dated_corporate_terms(days.filter(pl.col("product").str.contains(r"\d$")),
                                     units_intervals, unit_only=True)
    margin_law = json.loads((source / "margin_family.json").read_text())
    margins = bind_equity_margin_families(days.select("date", "product").unique(), margin_levels, universe,
        rule_effective_date=date.fromisoformat(margin_law["effective_date"]),
        rule_known_at=margin_law["known_at"], rule_source_sha256=margin_law["source_content_sha256"])
    laws = json.loads((source / "position_family/review.json").read_text())["rules"]
    product_positions = bind_equity_position_families(days, position_levels, universe, units, laws)
    positions = bind_physical_position_inputs(days, product_positions, units, universe, laws,
                                              corporate_unit_intervals=units_intervals)
    rights, _ = read_bound_output(source / "terminal/terminal_subscription_values.parquet")
    terminal = bind_adjusted_terminal_values(physical.filter(cash & pl.col("product").str.contains(r"\d$")).select(
        "date", "product", "contract", "final_settlement_price", "final_settlement_value").unique(), terms, rights)
    rules, flags = compile_execution_terms(frame, margins, positions, terms, margin_levels, specifications, terminal,
                                           position_research_policy=policy)
    if valuation_research_policy is not None:
        from stockagent.data.tw_futures_valuation_research import (
            attach_research_valuation_provenance, apply_research_terminal_cash_conversions)
        rules = attach_research_valuation_provenance(frame, rules)
        frame, rules, flags, terminal_research = apply_research_terminal_cash_conversions(
            frame, rules, flags, valuation_research_policy)
        save('research_terminal_conversions', terminal_research)
    dependencies = margin_execution_dependency_rows(frame, margins, positions,
        bind_dated_corporate_terms(days, terms), terminal)
    keys = ["date", "physical_contract"]
    flags = flags.join(dependencies.select(*keys, "missing_valuation", "intermediate_calendar_gap", "unresolved_lifetime"),
        on=keys, how="left", validate="1:1")
    reasons = [c for c in flags.columns if flags.schema[c] == pl.Boolean and c not in ["is_warmup", "has_blocker"]]
    flags = flags.with_columns(pl.any_horizontal(pl.col(c).fill_null(True) for c in reasons).alias("has_blocker"))
    save("contract_day_blockers", flags)
    save("compiled_rules", rules)
    return frame, rules, flags, dependencies


def _calculation_identity():
    files = ["stockagent/data/tw_futures_execution_terms.py",
             "stockagent/data/tw_futures_margin_preparation.py",
             "stockagent/data/tw_futures_position_research.py",
             "stockagent/data/tw_stock_futures_repair.py",
             "stockagent/data/tw_futures_portfolio_daily.py",
             "scripts/prepare_tw_futures_physical_history.py"]
    return dict(files={f:sha256_file(ROOT / f) for f in files},
        functions={f.__name__:hashlib.sha256(inspect.getsource(f).encode()).hexdigest()
            for f in [_compile_accounting, materialize_margin_market_rows, margin_execution_dependency_rows]})


def _context_prefix_tools(contract):
    from stockagent.data.tw_futures_margin_release import (
        CONTEXT_ONLY_PREFIX_CONTRACT, WHOLE_CONTRACT_PREFIX_CONTRACT,
        omit_unreachable_account_prefix, validate_context_only_prefix_sources,
        omit_whole_contract_empty_prefix, validate_whole_contract_prefix_sources,
        whole_contract_entry_frame,
    )
    if contract == CONTEXT_ONLY_PREFIX_CONTRACT:
        omit, validate = omit_unreachable_account_prefix, validate_context_only_prefix_sources
        functions = (omit, validate)
    elif contract == WHOLE_CONTRACT_PREFIX_CONTRACT:
        from stockagent.data.tw_futures_entry_capacity import whole_contract_trade_capacity
        omit, validate = omit_whole_contract_empty_prefix, validate_whole_contract_prefix_sources
        functions = (omit, validate, whole_contract_entry_frame, whole_contract_trade_capacity,
            omit_unreachable_account_prefix, validate_context_only_prefix_sources)
    else:
        raise ValueError('unsupported context-only account contract')
    implementation = {f.__name__: hashlib.sha256(inspect.getsource(f).encode()).hexdigest() for f in functions}
    return omit, validate, implementation


def _verify_valuation_research_extension(previous: dict, current: dict, policy: dict) -> dict:
    """Prove the old financial path is identical after removing two opt-in hooks."""
    baseline = policy['baseline_accounting_function_source']
    if (previous != policy['baseline_calculation'] or previous['files'] != current['files']
            or hashlib.sha256(baseline.encode()).hexdigest() != previous['functions']['_compile_accounting']
            or any(previous['functions'][name] != current['functions'][name]
                   for name in ('materialize_margin_market_rows', 'margin_execution_dependency_rows'))):
        raise ValueError('valuation research migration lacks its unchanged canonical financial baseline')
    candidate = ast.parse(inspect.getsource(_compile_accounting))
    function = candidate.body[0]
    if function.args.kwonlyargs[-1].arg != 'valuation_research_policy':
        raise ValueError('valuation research hook has an unexpected signature')
    function.args.kwonlyargs.pop(); function.args.kw_defaults.pop()
    kept, removed = [], []
    for node in function.body:
        if isinstance(node, ast.If) and ast.dump(node.test) == ast.dump(
                ast.parse('valuation_research_policy is not None', mode='eval').body):
            removed.append(node)
        else:
            kept.append(node)
    function.body = kept
    if len(removed) != 2 or ast.dump(candidate) != ast.dump(ast.parse(baseline)):
        raise ValueError('valuation research changes the retained financial calculation')
    return dict(contract='frozen_contract_value_research_v1', baseline=previous,
        canonical_nonresearch_path_unchanged=True, financial_values_inferred=True,
        financial_point_in_time_verified=False)


class _TerminalInputSource:
    """Explicit bound source composition; the financial kernel is unchanged."""
    def __init__(self, parent: Path, terminal: Path):
        self.parent, self.terminal = parent, terminal

    def __truediv__(self, relative):
        path = Path(relative)
        if path.is_absolute() or '..' in path.parts:
            raise ValueError('terminal source composition requires a relative canonical path')
        if str(path) == 'terminal/terminal_subscription_values.parquet':
            return self.terminal / 'terminal_subscription_values.parquet'
        return self.parent / path


def _accounting_terminal_source(source: Path, delta: Path):
    proof = json.loads((delta / 'manifest.json').read_text()).get('terminal_source_delta')
    if proof is None:
        return source, None
    if proof.get('contract') != 'bound_terminal_operand_source_delta_v1':
        raise ValueError('unknown terminal source delta contract')
    root = (delta / proof['path']).resolve()
    if not root.is_relative_to(delta.resolve()) or sha256_file(root / 'manifest.json') != proof['sha256']:
        raise ValueError('terminal source delta path/SHA mismatch')
    manifest = json.loads((root / 'manifest.json').read_text())
    if (manifest.get('status') != 'source_bound_terminal_components'
            or manifest.get('parent_source_manifest_sha256') != sha256_file(source / 'source_manifest.json')
            or manifest.get('parent_terminal_manifest_sha256') != sha256_file(source / 'terminal/manifest.json')):
        raise ValueError('terminal source delta changes its verified parent')
    for item in manifest['sources']:
        path = (root / item['path']).resolve()
        if not path.is_relative_to(root) or sha256_file(path) != item['sha256']:
            raise ValueError('terminal source original SHA mismatch')
    merged, _ = read_bound_output(root / 'terminal_subscription_values.parquet')
    added, _ = read_bound_output(root / 'terminal_overlay_rows.parquet')
    old, _ = read_bound_output(source / 'terminal/terminal_subscription_values.parquet')
    keys = ['date', 'product', 'contract']
    if (added.select(keys).is_duplicated().any() or old.join(added.select(keys), on=keys, how='semi').height
            or merged.select(keys).is_duplicated().any()
            or not merged.join(added.select(keys), on=keys, how='anti').equals(old)
            or not merged.join(added.select(keys), on=keys, how='semi').equals(added)):
        raise ValueError('terminal delta must retain every original row and add unique own events')
    identity = dict(contract=proof['contract'], manifest_sha256=proof['sha256'],
        implementation_sha256=hashlib.sha256((inspect.getsource(_TerminalInputSource)
            + inspect.getsource(_accounting_terminal_source)).encode()).hexdigest())
    return _TerminalInputSource(source, root), identity


def rule_product_inputs(delta: Path) -> dict:
    """Hash the actual dated financial inputs, not newly read OCR views."""
    from stockagent.data.tw_futures_margin_preparation import corporate_identity_boundaries
    frames = {name: read_bound_output(delta / (name + '.parquet'))[0] for name in
        ('corporate_terms_intervals', 'corporate_unit_intervals',
         'margin_level_intervals', 'position_level_intervals')}
    corporate, _ = read_bound_output(delta / 'corporate_event_candidates.parquet')
    boundaries, transfers = corporate_identity_boundaries(corporate)
    frames.update(corporate_boundaries=boundaries, corporate_transfers=transfers)
    terminal = json.loads((delta / 'manifest.json').read_text()).get('terminal_source_delta')
    if terminal is not None:
        frames['terminal_operands'] = read_bound_output(delta / terminal['path'] / 'terminal_overlay_rows.parquet')[0]
    signatures = {}
    for name, frame in frames.items():
        for product, rows in frame.partition_by('product', as_dict=True).items():
            states = sorted({json.dumps(r, sort_keys=True, ensure_ascii=False, default=str)
                             for r in rows.to_dicts()})
            signatures.setdefault(product[0], {})[name] = hashlib.sha256('\n'.join(states).encode()).hexdigest()
    manifest = json.loads((delta / 'manifest.json').read_text())
    for key, label in [('information_halt_source_delta', 'information_halt_reviews'),
                       ('loss_reduction_halt_source_delta', 'loss_reduction_halt_reviews')]:
        overlay = manifest.get(key)
        if overlay is None:
            continue
        root = delta / overlay['path']
        reviews = json.loads((root / 'manifest.json').read_text())['reviews']
        by_product = {}
        for review in reviews:
            path = root / review['path']
            product = json.loads(path.read_text())['episode']['product']
            by_product.setdefault(product, []).append(sha256_file(path))
        for product, hashes in by_product.items():
            signatures.setdefault(product, {})[label] = hashlib.sha256(
                '\n'.join(sorted(hashes)).encode()).hexdigest()
    return signatures


def _loss_reduction_halt_delta(source, delta, accounting_source):
    from stockagent.data.tw_futures_margin_halt_sources import (
        CONTRACT, LossReductionInputSource, load_loss_reduction_bundle,
    )
    proof = json.loads((delta / 'manifest.json').read_text()).get('loss_reduction_halt_source_delta')
    if proof is None:
        return accounting_source, None, None
    root = (delta / proof['path']).resolve()
    if (proof.get('contract') != CONTRACT or not root.is_relative_to(delta.resolve())
            or sha256_file(root / 'manifest.json') != proof['sha256']):
        raise ValueError('loss-reduction source delta path/SHA/contract mismatch')
    paths, episodes, identity, baseline = load_loss_reduction_bundle(
        source, delta / 'corporate_event_candidates.parquet', root)
    if sorted({e['product'] for e in episodes}) != proof['products']:
        raise ValueError('loss-reduction source delta changes its finite product scope')
    return LossReductionInputSource(accounting_source, paths), identity, baseline


def _verify_loss_reduction_loader_extension(previous, current, baseline):
    """Prove that only the exact equivalent loss-reduction wording was added."""
    path, digest = baseline
    module = 'stockagent/data/tw_futures_margin_preparation.py'
    if (sha256_file(path) != digest or previous['files'].get(module) != digest
            or sha256_file(ROOT / module) != current['files'].get(module)
            or previous['functions'] != current['functions']
            or set(previous['files']) != set(current['files'])
            or any(v != current['files'][k] for k, v in previous['files'].items() if k != module)):
        raise ValueError('loss-reduction extension changes financial calculation')
    original = ast.parse(path.read_text())
    effective = ast.parse((ROOT / module).read_text())
    old_guard = ast.parse("if '減資以彌補虧損' not in text:\n    raise ValueError('halt episode is not pure loss reduction')").body[0]
    new_guard = ast.parse("if not any(phrase in text for phrase in ('減資以彌補虧損', '減資彌補虧損')):\n    raise ValueError('halt episode is not pure loss reduction')").body[0]
    functions = [n for n in effective.body if isinstance(n, ast.FunctionDef)
                 and n.name == 'load_loss_reduction_halt_review']
    if len(functions) != 1:
        raise ValueError('loss-reduction loader is not unique')
    positions = [i for i, n in enumerate(functions[0].body) if ast.dump(n) == ast.dump(new_guard)]
    if len(positions) != 1:
        raise ValueError('loss-reduction extension has an unexpected semantic guard')
    functions[0].body[positions[0]] = old_guard
    if ast.dump(effective) != ast.dump(original):
        raise ValueError('loss-reduction extension changes other financial code')
    return dict(contract='unchanged_accounting_loss_reduction_wording_extension_v1',
                original_module_sha256=digest, effective_module_sha256=current['files'][module],
                accounting_functions_unchanged=True)


def _information_halt_delta(source: Path, delta: Path):
    """Validate the optional legal-value input independently of rule amounts."""
    proof = json.loads((delta / 'manifest.json').read_text()).get('information_halt_source_delta')
    if proof is None:
        return [], None, None
    if proof.get('contract') != 'bound_information_halt_source_delta_v1':
        raise ValueError('unknown information-halt source delta contract')
    root = (delta / proof['path']).resolve()
    if not root.is_relative_to(delta.resolve()) or sha256_file(root / 'manifest.json') != proof['sha256']:
        raise ValueError('information-halt source delta SHA/path mismatch')
    manifest = json.loads((root / 'manifest.json').read_text())
    implementation = ROOT / 'stockagent/data/tw_futures_information_halt.py'
    if (manifest.get('status') != 'source_bound_information_halt_extension'
            or manifest['parent_source_manifest_sha256'] != sha256_file(source / 'source_manifest.json')
            or manifest['implementation_sha256'] != sha256_file(implementation)):
        raise ValueError('information-halt extension changes its verified inputs/code')
    for item in manifest['sources']:
        file = (root / item['path']).resolve()
        if not file.is_relative_to(root) or sha256_file(file) != item['sha256']:
            raise ValueError('information-halt original source changed')
    from stockagent.data.tw_futures_information_halt import load_information_halt_review
    episodes = []
    for review in manifest['reviews']:
        file = (root / review['path']).resolve()
        if not file.is_relative_to(root) or sha256_file(file) != review['sha256']:
            raise ValueError('information-halt review changed')
        episode = load_information_halt_review(file)
        if episode['corporate_input_sha256'] != manifest['corporate_input_sha256']:
            raise ValueError('information-halt review changes its corporate parent')
        episodes.append(episode)
    if not episodes or sorted({e['product'] for e in episodes}) != sorted(proof['products']):
        raise ValueError('information-halt extension has a different product scope')
    baseline = manifest['accounting_builder_baseline']
    baseline_path = (root / baseline['path']).resolve()
    if not baseline_path.is_relative_to(root) or sha256_file(baseline_path) != baseline['sha256']:
        raise ValueError('information-halt accounting baseline SHA/path mismatch')
    identity = dict(contract=proof['contract'], manifest_sha256=proof['sha256'],
        implementation_sha256=manifest['implementation_sha256'], products=proof['products'],
        reviewed_corporate_input_sha256=manifest['corporate_input_sha256'],
        effective_corporate_input_sha256=sha256_file(delta / 'corporate_event_candidates.parquet'),
        opening_reference_historical_file_verified=False)
    return episodes, identity, (baseline_path, baseline['sha256'])


def _verify_information_halt_kernel_extension(previous: dict, current: dict,
                                              baseline: tuple[Path, str]) -> dict:
    """Reuse unaffected gaps only when the old accounting kernel is unchanged.

The only permitted function edit is an optional, explicit information-halt
input hook. Both the original code SHA and its previously receipted function
SHA are checked; AST comparison rejects any other accounting edit.
"""
    path, digest = baseline
    if (sha256_file(path) != digest or previous['files'] != current['files']
            or set(previous['functions']) != set(current['functions'])):
        raise ValueError('information-halt extension changes a financial kernel file')
    for name, value in previous['functions'].items():
        if name != '_compile_accounting' and current['functions'].get(name) != value:
            raise ValueError('information-halt extension changes another accounting function')
    text = path.read_text()
    original = next(n for n in ast.parse(text).body
                    if isinstance(n, ast.FunctionDef) and n.name == '_compile_accounting')
    source = ''.join(text.splitlines(keepends=True)[original.lineno - 1:original.end_lineno])
    if hashlib.sha256(source.encode()).hexdigest() != previous['functions']['_compile_accounting']:
        raise ValueError('information-halt baseline is not the accepted accounting kernel')
    effective = ast.parse(inspect.getsource(_compile_accounting)).body[0]
    if (effective.args.kwonlyargs[-1].arg != 'information_halt_reviews'
            or ast.dump(effective.args.kw_defaults[-1]) != ast.dump(ast.parse('()').body[0].value)):
        raise ValueError('information-halt extension has an unexpected input parameter')
    effective.args.kwonlyargs.pop()
    effective.args.kw_defaults.pop()
    hooks = [n for n in effective.body if isinstance(n, ast.If)
             and isinstance(n.test, ast.Name) and n.test.id == 'information_halt_reviews']
    expected = ast.parse('''if information_halt_reviews:
    from stockagent.data.tw_futures_information_halt import apply_information_halt_values
    information_values = []
    for episode in information_halt_reviews:
        physical, values = apply_information_halt_values(physical, calendar, episode,
            corporate=cf, corporate_input_sha256=sha256_file(rule_source / "corporate_event_candidates.parquet"))
        information_values.append(values)
    save("information_halt_values", pl.concat(information_values, how="vertical"))
''').body[0]
    if len(hooks) != 1 or ast.dump(hooks[0]) != ast.dump(expected):
        raise ValueError('information-halt extension has an unexpected accounting hook')
    effective.body.remove(hooks[0])
    if ast.dump(effective) != ast.dump(original):
        raise ValueError('information-halt extension changes the retained accounting calculation')
    return dict(contract='unchanged_accounting_kernel_information_halt_extension_v1',
        baseline_builder_sha256=digest, baseline_compile_sha256=previous['functions']['_compile_accounting'],
        effective_compile_sha256=current['functions']['_compile_accounting'],
        unchanged_kernel_ast_verified=True, unchanged_financial_files_verified=True)


def replay_rule_delta(source: Path, delta: Path, output: Path, *, worklist: Path,
                      parent_slots: Path, parent_slots_receipt: Path, end: date, slots: int = 2816,
                      previous_replay: Path | None = None, previous_input_receipt_sha256: str | None = None,
                      context_only_prefix_policy: Path | None = None,
                      valuation_research_policy: Path | None = None) -> dict:
    """Recompute connected products only, with complete parent coordinates.

    This prepares accounting evidence; it cannot publish a training release,
    replace an active experiment, or declare unaffected source gaps resolved.
    """
    begun = time.monotonic()
    calculation = _calculation_identity()
    source = source.resolve()
    if output.exists() and any(output.iterdir()):
        raise FileExistsError('affected replay exists; preserve its accounting evidence')
    source_sha = sha256_file(source / 'source_manifest.json')
    proof = json.loads((delta / 'manifest.json').read_text())
    slot_proof = json.loads(parent_slots_receipt.read_text())
    if (proof.get('status') != 'pending_source_bound_rule_delta'
            or proof.get('parent_source_manifest_sha256') != source_sha
            or slot_proof.get('source_manifest_sha256') != source_sha
            or slot_proof.get('metadata_sha256') != sha256_file(parent_slots)):
        raise ValueError('affected accounting inputs do not share the verified parent release')
    if proof.get('worklist_sha256') != sha256_file(worklist):
        raise ValueError('affected replay has a different blocker worklist')
    for item in proof.get('sources', []):
        path = delta / item['path']
        if not path.resolve().is_relative_to(delta.resolve()) or sha256_file(path) != item['sha256']:
            raise ValueError('pending source delta changed')
    source_manifest = json.loads((source / 'source_manifest.json').read_text())
    for relative in ('universe/products.csv', 'deferred_54_products.csv', 'position_research_policy.json',
                     'margin_family.json', 'position_family/review.json', 'rules/manifest.json'):
        if sha256_file(source / relative) != source_manifest['files'][relative]['sha256']:
            raise ValueError('retained accounting policy/universe changed')
    universe = pl.read_csv(source / 'universe/products.csv', infer_schema=False)
    deferred = pl.read_csv(source / 'deferred_54_products.csv', infer_schema=False)
    universe = universe.filter(~pl.col('product').is_in(deferred['product'].to_list()))
    if universe.height != 711 or set(universe['settlement_currency']) != {'TWD'}:
        raise ValueError('affected replay must retain the authorized 711-product parent universe')
    if sha256_file(source / 'rules/manifest.json') != proof['parent_manifest_sha256']:
        raise ValueError('retained rule parent changed')
    signatures = rule_product_inputs(delta)
    valuation_policy = None
    valuation_proof = None
    if valuation_research_policy is not None:
        from stockagent.data.tw_futures_valuation_research import validate_valuation_research_policy
        valuation_policy = validate_valuation_research_policy(json.loads(valuation_research_policy.read_text()))
        valuation_proof = dict(path=str(valuation_research_policy), sha256=sha256_file(valuation_research_policy),
            contract=valuation_policy['contract'], implementation_sha256=sha256_file(
                ROOT / 'stockagent/data/tw_futures_valuation_research.py'))
        if (previous_replay is None or valuation_policy['source_manifest_sha256'] != source_sha
                or valuation_policy['rule_delta_manifest_sha256'] != sha256_file(delta / 'manifest.json')
                or valuation_policy['prior_replay_manifest_sha256'] != sha256_file(previous_replay / 'manifest.json')
                or valuation_policy['prior_gap_worklist_sha256'] != sha256_file(previous_replay / 'remaining_source_gaps.csv')
                or not set(valuation_policy['products']) <= set(universe['product'])):
            raise ValueError('research valuation policy differs from its exact verified accounting parent')
    accounting_source, terminal_overlay = _accounting_terminal_source(source, delta)
    accounting_source, loss_overlay, loss_baseline = _loss_reduction_halt_delta(source, delta, accounting_source)
    information_reviews, information_overlay, information_baseline = _information_halt_delta(source, delta)
    kernel_extension = None
    previous = None
    previous_gaps = worklist
    if previous_replay is not None:
        previous = json.loads((previous_replay / 'manifest.json').read_text())
        receipt_path = previous_replay / 'input_rule_manifest.json'
        expected = previous.get('outputs', {}).get(receipt_path.name, {}).get('sha256')
        if expected is None:
            expected = previous_input_receipt_sha256
        if not expected or sha256_file(receipt_path) != expected:
            raise ValueError('incremental replay lacks its bound previous rule input receipt')
        prior_inputs = json.loads(receipt_path.read_text())
        if previous['calculation'] != calculation:
            if valuation_policy is not None:
                kernel_extension = _verify_valuation_research_extension(previous['calculation'], calculation, valuation_policy)
            elif loss_baseline is not None and previous['calculation']['functions'] == calculation['functions']:
                kernel_extension = _verify_loss_reduction_loader_extension(previous['calculation'], calculation, loss_baseline)
            elif information_baseline is not None:
                kernel_extension = _verify_information_halt_kernel_extension(previous['calculation'], calculation, information_baseline)
        if (previous['source_manifest_sha256'] != source_sha
                or previous['worklist_sha256'] != proof['worklist_sha256']
                or (previous['calculation'] != calculation and kernel_extension is None)
                or ('end' in previous and previous['end'] != str(end))
                or ('slots' in previous and previous['slots'] != slots)
                or prior_inputs['delta_manifest_sha256'] != previous['delta_manifest_sha256']):
            raise ValueError('incremental replay changes its verified financial calculation or parent')
        previous_gaps = previous_replay / 'remaining_source_gaps.csv'
        old_overlay = previous.get('terminal_source_overlay')
        if old_overlay is not None and (terminal_overlay is None
                or old_overlay['contract'] != terminal_overlay['contract']
                or old_overlay['implementation_sha256'] != terminal_overlay['implementation_sha256']):
            raise ValueError('incremental replay changes or removes terminal input composition')
        old_information = previous.get('information_halt_source_overlay')
        if old_information is not None and (information_overlay is None
                or old_information['contract'] != information_overlay['contract']
                or old_information['implementation_sha256'] != information_overlay['implementation_sha256']):
            raise ValueError('incremental replay changes or removes information-halt input composition')
        old_loss = previous.get('loss_reduction_halt_source_overlay')
        if old_loss is not None and (loss_overlay is None
                or old_loss['contract'] != loss_overlay['contract']
                or old_loss['implementation_sha256'] != loss_overlay['implementation_sha256']):
            raise ValueError('incremental replay changes or removes loss-reduction input composition')
        if sha256_file(previous_gaps) != previous['outputs'][previous_gaps.name]['sha256']:
            raise ValueError('previous affected blocker worklist changed')
    from stockagent.data.tw_futures_margin_preparation import corporate_identity_boundaries
    cf, _ = read_bound_output(delta / 'corporate_event_candidates.parquet')
    old_cf, _ = read_bound_output(source / 'rules/corporate_event_candidates.parquet')
    _, old_edges = corporate_identity_boundaries(old_cf)
    _, new_edges = corporate_identity_boundaries(cf)
    pairs = list(pl.concat([old_edges, new_edges], how='vertical').select(
        'product', 'corporate_transfer_target').unique().iter_rows())
    affected = set(proof['changed_products']) & set(universe['product'])
    if previous is not None:
        old_signatures = prior_inputs['product_sha256s']
        affected = {p for p in set(signatures) | set(old_signatures)
                    if signatures.get(p) != old_signatures.get(p)} & set(universe['product'])
        old_pairs = prior_inputs.get('corporate_transfers', [])
        pairs.extend(tuple(pair) for pair in old_pairs)
    products = set(universe['product'])
    if valuation_policy is not None:
        affected.update(valuation_policy['products'])
    prefix_policy = None
    prefix_implementation = None
    prefix_options = {}
    prefix_contract = None
    inherited_prefix = previous.get('context_only_prefix_policy') if previous is not None else None
    if inherited_prefix is not None or context_only_prefix_policy is not None:
        from stockagent.data.tw_futures_margin_release import (
            CONTEXT_ONLY_PREFIX_CONTRACT, WHOLE_CONTRACT_PREFIX_CONTRACT,
        )
        policy_path = context_only_prefix_policy if context_only_prefix_policy is not None else Path(inherited_prefix['path'])
        prefix_policy = dict(path=str(policy_path), sha256=sha256_file(policy_path))
        document = json.loads(policy_path.read_text())
        prefix_contract = document.get('contract')
        omit_prefix, _, prefix_implementation = _context_prefix_tools(prefix_contract)
        required = {'contract', 'source_manifest_sha256', 'prior_replay_manifest_sha256',
                    'prior_gap_worklist_sha256', 'rule_delta_manifest_sha256', 'products'}
        if prefix_contract == WHOLE_CONTRACT_PREFIX_CONTRACT:
            required.add('maximum_volume_participation')
            maximum = document.get('maximum_volume_participation')
            if (isinstance(maximum, bool) or not isinstance(maximum, (int, float))
                    or not 0 < maximum <= 1):
                raise ValueError('whole-contract context requires a finite participation bound in (0,1]')
            prefix_options = dict(maximum_volume_participation=maximum)
        if (set(document) != required
                or document['source_manifest_sha256'] != source_sha
                or not isinstance(document['products'], list) or not document['products']
                or any(not isinstance(p, str) for p in document['products'])
                or len(set(document['products'])) != len(document['products'])
                or not set(document['products']) <= products):
            raise ValueError('context-only policy lacks its exact source-bound retained scope')
        if inherited_prefix is not None and prefix_policy == inherited_prefix:
            if (prefix_policy != inherited_prefix
                    or previous.get('context_only_prefix_implementation') != prefix_implementation):
                raise ValueError('incremental replay changes its accepted empty-prefix contract')
        else:
            if inherited_prefix is not None:
                old_document = json.loads(Path(inherited_prefix['path']).read_text())
                if (context_only_prefix_policy is None or prefix_contract != WHOLE_CONTRACT_PREFIX_CONTRACT
                        or old_document.get('contract') != CONTEXT_ONLY_PREFIX_CONTRACT
                        or sha256_file(Path(inherited_prefix['path'])) != inherited_prefix['sha256']
                        or previous.get('context_only_prefix_implementation')
                           != _context_prefix_tools(CONTEXT_ONLY_PREFIX_CONTRACT)[2]):
                    raise ValueError('context-only migration lacks its unchanged, explicitly bound prior contract')
            if (previous is None
                    or document['prior_replay_manifest_sha256'] != sha256_file(previous_replay / 'manifest.json')
                    or document['prior_gap_worklist_sha256'] != sha256_file(previous_gaps)
                    or document['rule_delta_manifest_sha256'] != sha256_file(delta / 'manifest.json')):
                raise ValueError('initial context-only policy differs from the verified accounting parent')
            affected.update(document['products'])
    families = equity_contract_families(universe).to_dicts()
    while True:
        standards = {r['standard_product'] for r in families if r['product'] in affected}
        extra = {r['product'] for r in families if r['standard_product'] in standards}
        extra.update(p for a, b in pairs if a in affected or b in affected for p in (a, b) if p in products)
        if extra <= affected:
            break
        affected.update(extra)
    if not affected:
        raise ValueError('pending delta has no affected retained product')
    output.mkdir(parents=True, exist_ok=True)
    def save(name, frame):
        atomic_write_parquet(output / (name + '.parquet'), frame)
    def log(stage, **values):
        print(json.dumps(dict(stage=stage, elapsed_s=time.monotonic()-begun, **values), default=str), flush=True)
    log('affected_rule_replay', products=len(affected), full_parent_products=universe.height)
    frame, rules, flags, dependencies = _compile_accounting(accounting_source, universe, output=output,
        end=end, slots=slots, save=save, log=log, rule_source=delta, scope_products=sorted(affected),
        portfolio_lifetimes=pl.read_parquet(parent_slots), information_halt_reviews=information_reviews,
        valuation_research_policy=valuation_policy)
    compiled_account_rows = rules.height
    context_rows = 0
    suppressed_carry_rows = 0
    if prefix_policy is not None:
        rules, flags, empty, suppressed = omit_prefix(frame, rules, flags, **prefix_options)
        context_rows, suppressed_carry_rows = empty.height, suppressed.height
        save('context_only_original_rules', empty)
        save('context_only_suppressed_carries', suppressed)
        save('context_only_source_rows', frame.join(empty.select('date', 'physical_contract'),
            on=['date', 'physical_contract'], how='semi'))
        save('compiled_rules', rules)
        save('contract_day_blockers', flags)
        log('empty_account_prefix_verified', context_only_rows=context_rows,
            suppressed_empty_carry_rows=suppressed_carry_rows, retained_account_rows=rules.height)
    save('frame', frame)
    save('execution_dependencies', dependencies)
    old_gaps = pl.read_csv(previous_gaps, try_parse_dates=True, schema_overrides={'contract': pl.String})
    old = old_gaps.filter(pl.col('product').is_in(sorted(affected)))
    new = flags.filter(~pl.col('is_warmup') & pl.col('has_blocker')).select(old_gaps.columns)
    combined = pl.concat([old_gaps.filter(~pl.col('product').is_in(sorted(affected))), new], how='vertical')
    combined.sort('product', 'contract', 'date').write_csv(output / 'remaining_source_gaps.csv')
    keys = ['date', 'product', 'contract']
    removed = old.join(new.select(keys), on=keys, how='anti')
    retired = removed.join(frame.select(keys).unique(), on=keys, how='anti')
    resolved = removed.join(frame.select(keys).unique(), on=keys, how='semi')
    added = new.join(old.select(keys), on=keys, how='anti')
    save('resolved_blocker_rows', resolved)
    save('replaced_calendar_rows', retired)
    save('newly_blocked_rows', added)
    original_gaps = pl.read_csv(worklist, try_parse_dates=True, schema_overrides={'contract': pl.String})
    cumulatively_removed = original_gaps.join(combined.select(keys), on=keys, how='anti')
    cumulative_new = combined.join(original_gaps.select(keys), on=keys, how='anti')
    save('removed_original_blocker_coordinates', cumulatively_removed)
    save('new_blocker_coordinates_vs_original', cumulative_new)
    atomic_write_json(output / 'input_rule_manifest.json', dict(
        delta_manifest_sha256=sha256_file(delta / 'manifest.json'), product_sha256s=signatures,
        corporate_transfers=[list(pair) for pair in pairs]))
    if calculation != _calculation_identity():
        raise ValueError('accounting implementation changed during replay; no acceptance receipt')
    if valuation_proof is not None and (sha256_file(valuation_research_policy) != valuation_proof['sha256']
            or sha256_file(ROOT / 'stockagent/data/tw_futures_valuation_research.py') != valuation_proof['implementation_sha256']):
        raise ValueError('research valuation policy or implementation changed during replay')
    result = dict(status='affected_accounting_replayed_remaining_gaps_preserved',
        source_manifest_sha256=source_sha, delta_manifest_sha256=sha256_file(delta / 'manifest.json'),
        parent_slots_receipt_sha256=sha256_file(parent_slots_receipt),
        worklist_sha256=sha256_file(worklist), calculation=calculation,
        terminal_source_overlay=terminal_overlay,
        information_halt_source_overlay=information_overlay, loss_reduction_halt_source_overlay=loss_overlay,
        accounting_kernel_extension=kernel_extension,
        valuation_research_policy=valuation_proof,
        previous_replay_manifest_sha256=sha256_file(previous_replay / 'manifest.json') if previous else None,
        previous_input_receipt_sha256=expected if previous else None,
        previous_replay=str(previous_replay) if previous else None,
        context_only_prefix_policy=prefix_policy, context_only_prefix_implementation=prefix_implementation,
        context_only_prefix_contract=prefix_contract, context_only_prefix_options=prefix_options,
        context_only_prefix_rows=context_rows, suppressed_empty_carry_rows=suppressed_carry_rows,
        affected_products=sorted(affected), recompiled_rows=compiled_account_rows,
        retained_account_rows=rules.height, recompiled_lifetimes=frame['physical_contract'].n_unique(),
        end=str(end), slots=slots, original_gap_rows=original_gaps.height,
        input_gap_rows=old_gaps.height, old_affected_gap_rows=old.height,
        new_affected_gap_rows=new.height, resolved_original_coordinate_rows=resolved.height,
        removed_original_blocker_coordinates=cumulatively_removed.height,
        new_blocker_coordinates_vs_original=cumulative_new.height,
        stale_calendar_rows_replaced=retired.height, newly_blocked_coordinate_rows=added.height,
        remaining_source_gap_rows=combined.height, all_gap_rows_resolved=combined.is_empty(),
        full_accounting_rebuilds=0, financial_values_inferred=valuation_proof is not None,
        current_training_source_overwritten=False, training_release_published=False,
        elapsed_s=time.monotonic()-begun,
        outputs={p.name: dict(sha256=sha256_file(p)) for p in output.iterdir()
                 if p.is_file() and (p.suffix in ('.parquet', '.csv') or p.name == 'input_rule_manifest.json')})
    atomic_write_json(output / 'manifest.json', result)
    log('affected_rule_replay_verified', **{k: result[k] for k in
        ('recompiled_rows', 'resolved_original_coordinate_rows', 'stale_calendar_rows_replaced',
         'newly_blocked_coordinate_rows', 'remaining_source_gap_rows')})
    return result


def _repair_margin_accounting(source, previous_source, output, universe, cached, retained, inputs, log):
    previous=json.loads((previous_source/"source_manifest.json").read_text())
    current=json.loads((source/"source_manifest.json").read_text())
    if sha256_file(previous_source/"source_manifest.json")!=retained["inputs"]["source_manifest_sha256"]:
        raise ValueError("previous source does not own the retained accounting checkpoint")
    old_inputs=dict(retained["inputs"]);old_inputs["source_manifest_sha256"]=inputs["source_manifest_sha256"]
    if old_inputs!=inputs:raise ValueError("margin repair cannot change calculation, universe or date bounds")
    allowed={"rules/manifest.json","rules/margin_event_candidates.parquet","rules/margin_level_intervals.parquet",
             "rules/margin_interval_issues.json","rules/margin_interval_repair.json","original_locations.json"}
    changes=[n for n in set(previous["files"])|set(current["files"])
             if previous["files"].get(n)!=current["files"].get(n)]
    if any(n not in allowed and not n.startswith("rules/sources/margin-repair/") for n in changes):
        raise ValueError("margin-only repair contains another changed source input")
    review=json.loads((source/"rules/margin_interval_repair.json").read_text())
    affected=set(review["changed_products"])&set(universe["product"])
    # Close over every adjusted family and cross-product inventory conversion.
    families=universe.select("product").with_columns(pl.col("product").str.slice(0,2).alias("family"))
    family_ids=families.filter(pl.col("product").is_in(sorted(affected)))["family"].to_list()
    affected.update(families.filter(pl.col("family").is_in(family_ids))["product"])
    edges=cached["rules"].filter((pl.col("carry_from_physical_contract")!="")
        &(pl.col("carry_from_physical_contract")!=pl.col("physical_contract"))).select(
            "physical_contract","carry_from_physical_contract").unique()
    pairs=[(a.split(":",1)[0],b.split(":",1)[0]) for a,b in edges.iter_rows()]
    while True:
        extra={p for a,b in pairs if a in affected or b in affected for p in (a,b)}
        if extra<=affected:break
        affected.update(extra)
    affected&=set(universe["product"])
    frame=cached["frame"].filter(pl.col("product").is_in(sorted(affected)))
    days=frame.select("date","product","contract").unique()
    def read(name):return read_bound_output(source/"rules"/(name+".parquet"))[0]
    terms=read("corporate_terms_intervals");unit_intervals=read("corporate_unit_intervals")
    levels=read("margin_level_intervals");positions_levels=read("position_level_intervals")
    units=bind_dated_corporate_terms(days.filter(pl.col("product").str.contains(r"\d$")),unit_intervals,unit_only=True)
    law=json.loads((source/"margin_family.json").read_text())
    margins=bind_equity_margin_families(days.select("date","product").unique(),levels,universe,
        rule_effective_date=date.fromisoformat(law["effective_date"]),rule_known_at=law["known_at"],
        rule_source_sha256=law["source_content_sha256"])
    laws=json.loads((source/"position_family/review.json").read_text())["rules"]
    product_positions=bind_equity_position_families(days,positions_levels,universe,units,laws)
    positions=bind_physical_position_inputs(days,product_positions,units,universe,laws,corporate_unit_intervals=unit_intervals)
    physical=frame  # The retained frame is SHA-bound by the accounting checkpoint.
    rights,_=read_bound_output(source/"terminal/terminal_subscription_values.parquet")
    terminal=bind_adjusted_terminal_values(physical.filter(pl.col("cash_settlement")&pl.col("product").str.contains(r"\d$")).select(
        "date","product","contract","final_settlement_price","final_settlement_value").unique(),terms,rights)
    specifications,_=read_bound_output(source/"specifications/specifications.parquet")
    policy=json.loads((source/"position_research_policy.json").read_text())
    rules,flags=compile_execution_terms(frame,margins,positions,terms,levels,specifications,terminal,position_research_policy=policy)
    dependencies=margin_execution_dependency_rows(frame,margins,positions,bind_dated_corporate_terms(days,terms),terminal)
    keys=["date","physical_contract"]
    flags=flags.join(dependencies.select(*keys,"missing_valuation","intermediate_calendar_gap","unresolved_lifetime"),
        on=keys,how="left",validate="1:1")
    reasons=[c for c in flags.columns if flags.schema[c]==pl.Boolean and c not in ["is_warmup","has_blocker"]]
    flags=flags.with_columns(pl.any_horizontal(pl.col(c).fill_null(True) for c in reasons).alias("has_blocker"))
    for label,part in [("rules",rules),("flags",flags),("dependencies",dependencies)]:
        prior=cached[label].filter(pl.col("product").is_in(sorted(affected)))
        if not part.select(keys).sort(keys).equals(prior.select(keys).sort(keys)):
            raise ValueError("margin repair changed source accounting coordinates")
        cached[label]=pl.concat([cached[label].filter(~pl.col("product").is_in(sorted(affected))),part],how="diagonal_relaxed")
    log("affected_margin_products_recompiled",products=len(affected),rows=rules.height)
    return cached,dict(changed_source_files=changes,recompiled_products=sorted(affected),recompiled_rows=rules.height,
        full_accounting_rebuilds=0,financial_values_inferred=False)


def _read_research_replay_components(replay, source, delta, *, end, slots):
    """Compose disjoint accepted repairs without recompiling financial rows."""
    from stockagent.data.tw_futures_valuation_research import validate_valuation_research_policy
    parts = {name: [] for name in ('frame','rules','flags','dependencies')}
    filenames = dict(frame='frame.parquet',rules='compiled_rules.parquet',
        flags='contract_day_blockers.parquet',dependencies='execution_dependencies.parquet')
    seen, claimed, receipts, policies = set(), set(), [], []
    identity = _calculation_identity()
    current = replay
    while current is not None:
        manifest_path = current / 'manifest.json'
        if current.resolve() in seen:
            raise ValueError('research replay parent cycle')
        seen.add(current.resolve())
        m = json.loads(manifest_path.read_text())
        valuation = m.get('valuation_research_policy')
        if valuation is None:
            break
        if (m['source_manifest_sha256'] != sha256_file(source / 'source_manifest.json')
                or m['delta_manifest_sha256'] != sha256_file(delta / 'manifest.json')
                or m['calculation'] != identity or m['end'] != str(end) or m['slots'] != slots):
            raise ValueError('research composition changes source, financial kernel or coordinates')
        policy_path = Path(valuation['path'])
        if sha256_file(policy_path) != valuation['sha256']:
            raise ValueError('accepted research policy changed')
        policy = validate_valuation_research_policy(json.loads(policy_path.read_text()))
        if (policy['source_manifest_sha256'] != m['source_manifest_sha256']
                or policy['rule_delta_manifest_sha256'] != m['delta_manifest_sha256']):
            raise ValueError('accepted research policy has a different source')
        own = set(m['affected_products']) - claimed
        for name, filename in filenames.items():
            path = current / filename
            digest = m['outputs'][filename]['sha256']
            if sha256_file(path) != digest:
                raise ValueError('accepted research accounting changed')
            data = pl.scan_parquet(path).filter(pl.col('product').is_in(sorted(own))).collect()
            if name == 'rules':
                originals = []
                for original_name in ('context_only_original_rules.parquet','context_only_suppressed_carries.parquet'):
                    original_path = current / original_name
                    if sha256_file(original_path) != m['outputs'][original_name]['sha256']:
                        raise ValueError('accepted research prefix proof changed')
                    originals.append(pl.scan_parquet(original_path).filter(
                        pl.col('product').is_in(sorted(own))).collect())
                excluded, suppressed = originals
                data = pl.concat([data.join(suppressed.select('date','physical_contract'),
                    on=['date','physical_contract'],how='anti'),suppressed,excluded])
            parts[name].append(data)
        claimed.update(own)
        receipts.append(dict(path=str(manifest_path),sha256=sha256_file(manifest_path),
            owned_products=sorted(own),valuation_implementation_sha256=valuation['implementation_sha256']))
        policies.append(dict(path=str(policy_path),sha256=valuation['sha256'],document=policy))
        parent = m.get('previous_replay')
        current = Path(parent) if parent else None
        if current is not None and sha256_file(current / 'manifest.json') != m['previous_replay_manifest_sha256']:
            raise ValueError('accepted research replay parent changed')
    if not receipts:
        raise ValueError('no accepted research accounting to publish')
    cached = {name: pl.concat(values,how='diagonal_relaxed') for name,values in parts.items()}
    for data in cached.values():
        if data.select('date','physical_contract').is_duplicated().any():
            raise ValueError('research composition duplicates a financial coordinate')
    return cached, receipts, policies


def build(source: Path, output: Path, *, start: date, end: date, slots: int = 2816,
          diagnostic_products: list[str] | None = None, reuse_accounting: bool = False,
          previous_sources: Path | None = None, omit_empty_account_prefixes: bool = False,
          empty_prefix_max_volume_participation: float | None = None,
          replay_accounting: Path | None = None, rule_delta: Path | None = None,
          valuation_research_policy: Path | None = None) -> dict:
    begun = time.monotonic()
    if output.exists() and not reuse_accounting and not (
            replay_accounting is not None and (output/'research_accounting_composition.json').is_file()):
        raise FileExistsError("build output exists; use --reuse-accounting for its exact compiled checkpoint")
    if (output / "build_acceptance.json").exists():
        raise FileExistsError("completed releases are immutable; use the accepted build")
    source = source.resolve()
    manifest = json.loads((source / "source_manifest.json").read_text())
    if manifest.get("status") != "source_evidence_complete_training_scope_pending":
        raise ValueError("incomplete source evidence")
    for i, (relative, item) in enumerate(manifest["files"].items(), 1):
        path = source / relative
        if not path.resolve().is_relative_to(source) or sha256_file(path) != item["sha256"]:
            raise ValueError(f"source release SHA/path mismatch: {relative}")
        if i % 10000 == 0:
            print(json.dumps(dict(stage="source_verification", verified=i, total=len(manifest["files"]))), flush=True)
    output.mkdir(parents=True, exist_ok=reuse_accounting or replay_accounting is not None)

    def save(name, frame):
        atomic_write_parquet(output / (name + ".parquet"), frame)

    def log(stage, **values):
        print(json.dumps(dict(stage=stage, elapsed_s=time.monotonic()-begun, **values), default=str), flush=True)

    universe = pl.read_csv(source / "universe/products.csv", infer_schema=False)
    deferred = pl.read_csv(source / "deferred_54_products.csv", infer_schema=False)
    universe = universe.filter(~pl.col("product").is_in(deferred["product"].to_list()))
    if universe.height != 711 or not universe["settlement_currency"].eq("TWD").all():
        raise ValueError("the explicitly retained 711-product TWD universe changed")
    parent_products = universe['product'].to_list()
    if diagnostic_products:
        if not set(diagnostic_products) <= set(universe["product"]):
            raise ValueError("diagnostic products exceed the retained universe")
        universe = universe.filter(pl.col("product").is_in(diagnostic_products))
    replay_proof = None
    if replay_accounting is not None:
        replay_proof = json.loads((replay_accounting / 'manifest.json').read_text())
        valuation_proof = replay_proof.get('valuation_research_policy')
        if (rule_delta is None or valuation_research_policy is None or valuation_proof is None
                or replay_proof['source_manifest_sha256'] != sha256_file(source / 'source_manifest.json')
                or replay_proof['delta_manifest_sha256'] != sha256_file(rule_delta / 'manifest.json')
                or replay_proof['calculation'] != _calculation_identity()
                or valuation_proof['sha256'] != sha256_file(valuation_research_policy)
                or valuation_proof['implementation_sha256'] != sha256_file(ROOT / 'stockagent/data/tw_futures_valuation_research.py')
                or replay_proof['end'] != str(end) or replay_proof['slots'] != slots
                or not omit_empty_account_prefixes):
            raise ValueError('research publication requires its exact accepted replay, rules, policy and prefix contract')
        replay_cache, replay_receipts, replay_policies = _read_research_replay_components(
            replay_accounting,source,rule_delta,end=end,slots=slots)
        universe = universe.filter(pl.col('product').is_in(sorted(set(replay_cache['frame']['product']))))
    products = universe["product"].to_list()
    inputs = dict(source_manifest_sha256=sha256_file(source / "source_manifest.json"),
        end=str(end), slots=slots, products=products, calculation=_calculation_identity())
    if replay_proof is not None:
        inputs.update(replayed_accounting_manifest_sha256=sha256_file(replay_accounting / 'manifest.json'),
            rule_delta_manifest_sha256=sha256_file(rule_delta / 'manifest.json'),
            valuation_research_policy_sha256=sha256_file(valuation_research_policy),
            research_accounting_composition=replay_receipts)
    if omit_empty_account_prefixes:
        from stockagent.data.tw_futures_margin_release import (
            CONTEXT_ONLY_PREFIX_CONTRACT, WHOLE_CONTRACT_PREFIX_CONTRACT,
        )
        contract = (CONTEXT_ONLY_PREFIX_CONTRACT if empty_prefix_max_volume_participation is None
            else WHOLE_CONTRACT_PREFIX_CONTRACT)
        omit_prefix, validate_prefix, implementation = _context_prefix_tools(contract)
        prefix_options = ({} if empty_prefix_max_volume_participation is None else
            dict(maximum_volume_participation=empty_prefix_max_volume_participation))
        inputs['context_only_prefix'] = dict(contract=contract, implementation=implementation, **prefix_options)
    checkpoint = output / "accounting_checkpoint.json"
    cache_files = {"frame":"frame.parquet", "rules":"compiled_rules.parquet",
                   "flags":"contract_day_blockers.parquet", "dependencies":"execution_dependencies.parquet"}
    if replay_proof is not None:
        staging = output/'research_accounting_composition.json'
        if staging.exists() and json.loads(staging.read_text()) != inputs:
            raise ValueError('research publication staging differs from its exact accepted inputs')
        frame, rules, flags, dependencies = (replay_cache[k] for k in ['frame','rules','flags','dependencies'])
        # A new valuation-only owner may change later global slots. Reassign
        # model coordinates once while preserving all financial operands.
        from stockagent.data.tw_futures_portfolio_daily import _fixed_portfolio_slot_map
        slot_path = replay_accounting / 'portfolio_slot_lifetimes.parquet'
        if sha256_file(slot_path) != replay_proof['outputs'][slot_path.name]['sha256']:
            raise ValueError('accepted research slot metadata changed')
        slot_lives = pl.read_parquet(slot_path)
        own_lives = frame.group_by('product','contract','physical_instance').agg(
            pl.col('date').min().alias('first_observed_date'),pl.col('date').max().alias('last_observed_date'))
        slot_lives = pl.concat([slot_lives.filter(~pl.col('product').is_in(products)).select(own_lives.columns),own_lives])
        calendar = pl.scan_parquet(source/'observations/all_futures_daily_sessions.parquet').filter(
            pl.col('product').is_in(parent_products) & (pl.col('date') <= end)
            & (pl.col('session') == '一般') & pl.col('contract').str.contains(r'^\d{6}(?:W[1-5])?$'))
        calendar = calendar.select('date').unique().collect()['date'].to_list()
        slot_map = _fixed_portfolio_slot_map(slot_lives,calendar,fixed_slot_count=slots)
        frame = frame.drop('portfolio_slot','symbol').join(slot_map,
            on=['product','contract','physical_instance'],validate='m:1')
        dependencies = dependencies.drop('symbol').join(frame.select('date','physical_contract','symbol'),
            on=['date','physical_contract'],validate='1:1')
        if frame.select('date','symbol').is_duplicated().any():
            raise ValueError('composed research history collides in a model slot')
        atomic_write_json(output/'research_accounting_composition.json',inputs)
        log('accepted_affected_replay_reused',products=universe.height,rows=rules.height)
    elif reuse_accounting:
        retained = json.loads(checkpoint.read_text())
        if retained["inputs"] != inputs and previous_sources is None:
            raise ValueError("retained accounting inputs/code differ; repair affected source groups explicitly")
        cached = {}
        for key, filename in cache_files.items():
            path = output / filename
            if sha256_file(path) != retained["outputs"][filename]:
                raise ValueError("retained accounting checkpoint changed")
            cached[key] = pl.read_parquet(path)
        if previous_sources is not None:
            cached, repair = _repair_margin_accounting(source, previous_sources, output, universe, cached, retained, inputs, log)
            for key,filename in cache_files.items():
                if key!="frame":
                    preserved=output/(filename+".before_margin_repair")
                    if not preserved.exists():shutil.copyfile(output/filename,preserved)
                    atomic_write_parquet(output/filename,cached[key])
            atomic_write_json(output/"margin_accounting_repair.json",repair)
            atomic_write_json(checkpoint,dict(inputs=inputs,outputs={name:sha256_file(output/name) for name in cache_files.values()}))
        frame, rules, flags, dependencies = (cached[k] for k in ["frame", "rules", "flags", "dependencies"])
        log("verified_accounting_reused", rows=rules.height)
    else:
        frame, rules, flags, dependencies = _compile_accounting(source, universe, output=output,
            end=end, slots=slots, save=save, log=log)
        save("frame", frame); save("execution_dependencies", dependencies)
        atomic_write_json(checkpoint, dict(inputs=inputs,
            outputs={name:sha256_file(output/name) for name in cache_files.values()}))
    context_proof = None
    if omit_empty_account_prefixes:
        rules, flags, empty, suppressed = omit_prefix(frame, rules, flags, **prefix_options)
        # The cache remains the complete original calculation. Reuse verifies
        # that identity, then deterministically reproduces the account view.
        save('context_only_original_rules', empty)
        save('context_only_suppressed_carries', suppressed)
        log('empty_account_prefix_verified', context_only_rows=empty.height,
            suppressed_empty_carry_rows=suppressed.height, retained_account_rows=rules.height)
    rule_root = rule_delta if rule_delta is not None else source / 'rules'
    terms, _ = read_bound_output(rule_root / "corporate_terms_intervals.parquet")
    financial_source = (_accounting_terminal_source(source,rule_delta)[0]
        if rule_delta is not None else source)
    rights, _ = read_bound_output(financial_source / "terminal/terminal_subscription_values.parquet")
    policy = json.loads((source / "position_research_policy.json").read_text())
    final_path = source / "final/futures_final_settlement_history.parquet"
    active = flags.filter(~pl.col("is_warmup"))
    keys = ["date", "physical_contract"]
    log("complete_universe_accounting", rows=rules.height, blocked=int(active["has_blocker"].sum()))
    selected, chosen, coverage = select_complete_margin_components(frame, rules, flags, start=start, end=end,
        context_only_keys=empty.select(keys) if omit_empty_account_prefixes else None)
    save("lifetime_scope", coverage)
    save("blocked_financial_days", active.filter(pl.col("has_blocker")))
    counts = coverage.group_by("product").agg(pl.len().alias("lifetimes"),
        pl.col("selected").sum().alias("selected_lifetimes"),
        pl.col("start").filter(pl.col("selected")).min().alias("selected_start"),
        pl.col("end").filter(pl.col("selected")).max().alias("selected_end"))
    counts = counts.join(active.group_by("product").agg(pl.len().alias("account_days"),
        pl.col("has_blocker").sum().alias("blocked_account_days")), on="product", how="left")
    counts.sort("product").write_csv(output / "product_scope.csv")
    chosen_products = sorted(selected["product"].unique())
    for validator in [validate_margin_carry_rules, validate_margin_value_bases,
                      validate_margin_second_position_limit, validate_margin_grandfather_rules]:
        validator(chosen)
    log("complete_components_selected", products=len(chosen_products), rows=chosen.height)
    material, admitted = output / "materialization", output / "terms"
    material.mkdir(exist_ok=reuse_accounting or replay_accounting is not None)
    admitted.mkdir(exist_ok=reuse_accounting or replay_accounting is not None)
    atomic_write_parquet(material / "continuous_daily.parquet", selected)
    atomic_write_parquet(material / "execution_dependencies.parquet",
        dependencies.join(selected.select(keys), on=keys, how="semi"))
    atomic_write_json(material / "manifest.json", dict(dataset="taifex_all_twd_margin_materialization",
        contract_version=futures_slot_layout_version(slots),
        feature_contract_version=TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION,
        fixed_model_output_slots=slots,
        requested_products=chosen_products, products=len(chosen_products), rows=selected.height,
        scope_kind="user_authorized_retrospective_complete_inventory_components",
        full_twd_universe_training_ready=False,
        source_evidence_sha256=sha256_file(source / "source_manifest.json"),
        zero_print_products=selected.group_by("product").agg(pl.col("executable").any().alias("prints"))
            .filter(~pl.col("prints"))["product"].to_list(),
        outputs={p.name:dict(sha256=sha256_file(p)) for p in material.iterdir() if p.is_file()}))
    sources = []
    source_groups = [(source/'rules','rules'),(source/'specifications','specifications'),
        (financial_source/'terminal','terminal')]
    if rule_delta is not None:
        source_groups.append((rule_delta,'rule_delta'))
        terminal_delta = json.loads((rule_delta/'manifest.json').read_text()).get('terminal_source_delta')
        if terminal_delta is not None:
            source_groups.append((rule_delta/terminal_delta['path'],'terminal_delta'))
    for directory_root, directory in source_groups:
        proof = json.loads((directory_root / "manifest.json").read_text())
        for item in proof.get("sources", []):
            relative = Path("sources") / directory / item["path"]
            target = admitted / relative; target.parent.mkdir(parents=True, exist_ok=True)
            if not target.is_file() or sha256_file(target) != item["sha256"]:
                shutil.copyfile(directory_root / item["path"], target)
            sources.append(dict(item, path=str(relative)))
    used = bind_dated_corporate_terms(selected.select("date", "product", "contract"), terms)
    used_hashes = set(used["source_content_sha256s"].explode().drop_nulls())
    accepted_components = terms.filter(pl.col("source_content_sha256s").list.eval(
        pl.element().is_in(sorted(used_hashes))).list.any()).with_columns(pl.lit(True).alias("point_in_time_verified"))
    # Admission covers only the explicitly selected physical days, whose
    # financial units, value components, clocks and carry edges passed above.
    component_path = admitted / "sources/admitted_terminal_components.parquet"
    atomic_write_parquet(component_path, accepted_components)
    rights_path = admitted / "sources/terminal_subscription_values.parquet"
    atomic_write_parquet(rights_path, rights)
    policy_path = admitted / "sources/position_research_policy.json"
    shutil.copyfile(source / "position_research_policy.json", policy_path)
    for path in [component_path, rights_path, policy_path]:
        sources.append(dict(path=str(path.relative_to(admitted)), sha256=sha256_file(path), kind="scoped_source_evidence"))
    valuation_proof = None
    if valuation_research_policy is not None:
        valuation_path = admitted / 'sources/valuation_research_policy.json'
        combined_policy = dict(replay_policies[0]['document'])
        combined_policy.update(products=sorted(set(p for item in replay_policies for p in item['document']['products'])),
            physical_instances=sorted(set(p for item in replay_policies for p in item['document']['physical_instances'])),
            continuation_transfers=list({(r['date'],r['physical_contract']):r
                for item in replay_policies for r in item['document']['continuation_transfers']}.values()),
            accepted_replay_manifest_sha256=sha256_file(replay_accounting/'manifest.json'))
        combined_policy['accepted_policy_inputs'] = []
        for item in replay_policies:
            original_path = admitted/'sources/valuation_policy_inputs'/(item['sha256']+'.json')
            original_path.parent.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(item['path'],original_path)
            receipt = dict(path=str(original_path.relative_to(admitted)),sha256=item['sha256'])
            combined_policy['accepted_policy_inputs'].append(receipt)
            sources.append(dict(receipt,kind='operator_research_valuation_policy_input'))
        atomic_write_json(valuation_path,combined_policy)
        valuation_proof = dict(path=str(valuation_path.relative_to(admitted)), sha256=sha256_file(valuation_path))
        sources.append(dict(valuation_proof, kind='operator_research_valuation_policy'))
    if omit_empty_account_prefixes:
        context_proof = dict(contract=inputs['context_only_prefix']['contract'], **prefix_options)
        selected_keys = selected.select(keys)
        for key, original in [('excluded_rules', empty), ('suppressed_carries', suppressed)]:
            scoped = original.join(selected_keys, on=keys, how='semi')
            path = admitted / 'sources' / ('context_only_' + key + '.parquet')
            atomic_write_parquet(path, scoped)
            receipt = dict(path=str(path.relative_to(admitted)), sha256=sha256_file(path))
            sources.append(dict(receipt, kind='reversible_empty_inventory_proof'))
            context_proof[key] = receipt
            if key == 'excluded_rules':
                context_proof['rows'] = scoped.height
        context_proof['implementation'] = inputs['context_only_prefix']['implementation']
        validate_prefix(selected, chosen,
            dict(context_only_prefix=context_proof, sources=sources), admitted)
    review = dict(status="complete_inventory_components_financially_validated", research_only=True,
        requested_products=products, selected_products=chosen_products,
        lifetime_scope_sha256=sha256_file(output / "lifetime_scope.parquet"),
        source_evidence_sha256=sha256_file(source / "source_manifest.json"),
        full_history_training_ready=False, financial_values_inferred=valuation_proof is not None,
        selection_is_retrospective=True, remaining_blocked_account_rows=int(active["has_blocker"].sum()),
        excluded_products=sorted(set(products)-set(chosen_products)),
        not_verified=["minute/bidask execution", "broker margin surcharges", "unselected source periods", "investment profitability"])
    atomic_write_json(admitted / "sources/admission_review.json", review)
    sources.append(dict(path="sources/admission_review.json", sha256=sha256_file(admitted / "sources/admission_review.json"), kind="scope_admission"))
    atomic_write_parquet(admitted / "rules.parquet", chosen)
    atomic_write_json(admitted / "manifest.json", dict(dataset="taifex_futures_margin_execution_terms", schema_version=8 if valuation_proof else 7,
        compiler_version=EXECUTION_TERMS_COMPILER_VERSION, status="complete", point_in_time_verified=False,
        research_only=True, financial_point_in_time_verified=valuation_proof is None, official_position_history_complete=False,
        position_research_contract=policy["contract"], position_research_policy_sha256=sha256_file(policy_path),
        position_research_policy=dict(path="sources/position_research_policy.json", sha256=sha256_file(policy_path)),
        source_materialization_sha256=sha256_file(material / "manifest.json"),
        outputs={"rules":dict(sha256=sha256_file(admitted / "rules.parquet"))}, sources=sources,
        adjusted_terminal_components=dict(status="admitted",
            corporate_terms=dict(path=str(component_path.relative_to(admitted)), sha256=sha256_file(component_path)),
            subscription_values=dict(path=str(rights_path.relative_to(admitted)), sha256=sha256_file(rights_path))),
        full_twd_universe_training_ready=False,
        **(dict(valuation_research_contract='frozen_contract_value_research_v1',
            valuation_research_policy=valuation_proof,valuation_research_policy_sha256=valuation_proof['sha256'])
            if valuation_proof else {}),
        **({'context_only_prefix':context_proof} if context_proof is not None else {})))
    release = publish_all_twd_margin_release(materialization=material, execution_terms=admitted / "rules.parquet",
        final_settlement=final_path, output=output / "release")
    summary = dict(release, **review, created_at_utc=datetime.now(UTC).isoformat(),
        elapsed_s=time.monotonic()-begun, compiler_sha256=sha256_file(ROOT / "stockagent/data/tw_futures_execution_terms.py"),
        builder_sha256=sha256_file(Path(__file__)), source_manifest_sha256=sha256_file(source / "source_manifest.json"),
        initial_capital_twd=100000000, runtime_training_verified=False)
    atomic_write_json(output / "build_acceptance.json", summary)
    log("release_validated", products=len(chosen_products), rows=chosen.height, excluded_products=review["excluded_products"])
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sources", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2010, 11, 1))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2026, 9, 4))
    parser.add_argument("--slots", type=int, default=2816)
    parser.add_argument("--previous-sources", type=Path, help="Verified previous source for a margin-only incremental repair")
    parser.add_argument("--reuse-accounting", action="store_true")
    parser.add_argument("--repair-release-metadata", action="store_true")
    parser.add_argument("--diagnostic-product", action="append", default=[])
    parser.add_argument('--rule-delta', type=Path, help='Pending source-bound rules for connected-product replay only')
    parser.add_argument('--gap-worklist', type=Path)
    parser.add_argument('--parent-slot-lifetimes', type=Path)
    parser.add_argument('--parent-slot-receipt', type=Path)
    parser.add_argument('--prior-affected-replay', type=Path,
                        help='Reuse its verified unaffected gaps; recompile only newly changed financial inputs')
    parser.add_argument('--prior-rule-input-receipt-sha256',
                        help='Explicit receipt identity for a replay created before input receipts were bound')
    parser.add_argument('--context-only-prefix-policy', type=Path,
                        help='Source-bound opt-in to omit proved empty, nonexecuting account prefixes')
    parser.add_argument('--valuation-research-policy', type=Path,
                        help='Explicit source-bound frozen-contract-value research ABI for affected replay')
    parser.add_argument('--publish-replay', type=Path,
                        help='Reuse this accepted affected replay for canonical component publication; no financial rebuild')
    parser.add_argument('--omit-empty-account-prefixes', action='store_true',
                        help='Build with reversible, causally proved empty-prefix account admission')
    parser.add_argument('--empty-prefix-max-volume-participation', type=float,
                        help='Whole-contract prefix ABI; reject runtime participation above this explicit bound')
    args = parser.parse_args()
    if args.empty_prefix_max_volume_participation is not None:
        if (not args.omit_empty_account_prefixes or (args.rule_delta and not args.publish_replay) or args.repair_release_metadata
                or not 0 < args.empty_prefix_max_volume_participation <= 1):
            parser.error('whole-contract prefix bound requires --omit-empty-account-prefixes in the build mode')
    if not args.rule_delta and (args.prior_affected_replay or args.prior_rule_input_receipt_sha256
                               or args.context_only_prefix_policy or args.valuation_research_policy):
        parser.error('incremental replay arguments require --rule-delta')
    if args.publish_replay:
        if (not args.rule_delta or not args.valuation_research_policy or not args.omit_empty_account_prefixes
                or args.reuse_accounting or args.previous_sources or args.repair_release_metadata
                or args.prior_affected_replay or args.diagnostic_product):
            parser.error('replay publication requires explicit rules, valuation policy and empty-prefix admission')
        build(args.sources,args.output,start=args.start,end=args.end,slots=args.slots,
            omit_empty_account_prefixes=True,empty_prefix_max_volume_participation=args.empty_prefix_max_volume_participation,
            replay_accounting=args.publish_replay,rule_delta=args.rule_delta,
            valuation_research_policy=args.valuation_research_policy)
    elif args.rule_delta:
        if not all((args.gap_worklist, args.parent_slot_lifetimes, args.parent_slot_receipt)):
            parser.error('affected replay requires the blocker worklist and bound complete parent slot metadata')
        if (args.repair_release_metadata or args.previous_sources or args.reuse_accounting
                or args.diagnostic_product or args.omit_empty_account_prefixes):
            parser.error('affected rule replay cannot be combined with release mutation/reuse modes')
        replay_rule_delta(args.sources, args.rule_delta, args.output, worklist=args.gap_worklist,
            parent_slots=args.parent_slot_lifetimes, parent_slots_receipt=args.parent_slot_receipt,
            end=args.end, slots=args.slots, previous_replay=args.prior_affected_replay,
            previous_input_receipt_sha256=args.prior_rule_input_receipt_sha256,
            context_only_prefix_policy=args.context_only_prefix_policy,
            valuation_research_policy=args.valuation_research_policy)
    elif args.repair_release_metadata:
        if args.omit_empty_account_prefixes:
            parser.error('empty-prefix admission changes accounting membership, not only metadata')
        repair_release_metadata(args.sources, args.output)
    else:
        build(args.sources, args.output, start=args.start, end=args.end, slots=args.slots,
              diagnostic_products=args.diagnostic_product, reuse_accounting=args.reuse_accounting,
              previous_sources=args.previous_sources, omit_empty_account_prefixes=args.omit_empty_account_prefixes,
              empty_prefix_max_volume_participation=args.empty_prefix_max_volume_participation)
