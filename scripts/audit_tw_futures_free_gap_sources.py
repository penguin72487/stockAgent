#!/usr/bin/env python3
"""Inspect exact free-provider keys and causal entry bounds, without a replay."""
from __future__ import annotations

import argparse
from datetime import datetime, UTC
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import polars as pl
from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from scripts.plan_tw_futures_tej_gap_priority import _read_context_chain
from stockagent.config import load_config
from stockagent.data.tw_futures_execution_terms import inventory_entry_reachability
from stockagent.data.tw_futures_margin_release import whole_contract_entry_frame

CONTRACT = 'exact_free_futures_halt_and_causal_capacity_audit_v1'
KEYS = ['date', 'physical_contract']


def exact_finmind_settlements(frame: pl.DataFrame, requests: pl.DataFrame) -> pl.DataFrame:
    required = {'date', 'futures_id', 'contract_date', 'trading_session', 'settlement_price'}
    if required - set(frame.columns):
        raise ValueError('FinMind audit requires explicit own-month, date, session and settlement fields')
    keys = ['date', 'futures_id', 'contract_date']
    own = frame.filter(pl.col('trading_session') == 'position').with_columns(
        *[pl.col(c).cast(pl.String) for c in keys])
    return own.join(requests, on=keys, how='semi')


def audit(*, replay: Path, diagnosis: Path, terms: Path, config: Path,
          finmind: Path, finlab: Path, output: Path) -> dict:
    if (output / 'manifest.json').exists():
        raise FileExistsError('preserve the accepted source audit; refresh the maintained gap inventory separately')
    begun = datetime.now(UTC)
    cfg = load_config(config)
    maximum = cfg.trading.max_volume_participation
    if not cfg.trading.tw_futures_portfolio_integer_contracts:
        raise ValueError('selected experiment does not use whole-contract execution')
    worklist = replay / 'remaining_source_gaps.csv'
    gaps = pl.read_csv(worklist, try_parse_dates=True, schema_overrides={'contract': pl.String}).filter(
        ~((pl.col('product') == 'CPF') & (pl.col('date') < pl.date(2010, 1, 1))))
    operand_path = diagnosis / 'remaining_halt_operand_links.parquet'
    operands = pl.read_parquet(operand_path)
    if not operands.select(pl.col('gap_date').str.to_date().alias('date'), 'physical_contract').unique().sort(KEYS).equals(
            gaps.select(KEYS).sort(KEYS)):
        raise ValueError('free-source operand inventory differs from the selected replay gaps')
    requests = operands.filter(pl.col('operand').is_in([
        'daily_clearing_or_legal_valuation', 'prior_contract_value_before_open'])).select(
            pl.col('required_date').alias('date'), pl.col('required_product').alias('futures_id'),
            pl.col('required_contract').alias('contract_date')).unique().sort('date', 'futures_id', 'contract_date')
    sources = []
    matched, missing_days = [], []
    for day in requests['date'].unique().sort():
        receipt = finmind / 'sponsor/receipts/TaiwanFuturesDaily/all' / (day + '.json')
        if not receipt.is_file():
            missing_days.append(day); continue
        info = json.loads(receipt.read_text())
        relative = Path(info['parquet_path'])
        data = finmind / 'sponsor' / relative
        if (relative.is_absolute() or '..' in relative.parts or sha256_file(data) != info['sha256']):
            raise ValueError('FinMind exact-day source receipt SHA/path mismatch')
        frame = pl.read_parquet(data)
        if frame.filter(pl.col('date') != day).height:
            raise ValueError('FinMind day receipt contains an outside date')
        selected = exact_finmind_settlements(frame, requests.filter(pl.col('date') == day))
        if selected.height:
            matched.append(selected.with_columns(pl.lit(info['sha256']).alias('source_sha256')))
        sources.extend([dict(path=str(receipt), sha256=sha256_file(receipt)),
            dict(path=str(data), sha256=info['sha256'])])
    catalog = finlab / 'catalog/discovery.json'
    catalog_data = json.loads(catalog.read_text())
    finlab_keys = [k for k in catalog_data['keys'] if k.startswith('futures_price:')]
    sources.append(dict(path=str(catalog), sha256=sha256_file(catalog)))
    term_frame = pl.read_parquet(terms)
    scope = set(gaps['product'])
    edges = term_frame.select('from_product', 'product').unique().rows()
    while True:
        incoming = {source for source, destination in edges if destination in scope}
        if incoming <= scope:
            break
        scope.update(incoming)
    frame_columns = ['date', 'product', 'contract', 'physical_contract', 'executable',
        'volume', 'previous_volume', 'same_contract_as_previous_session']
    rule_columns = ['date', 'product', 'contract', 'physical_contract', 'carry_from_date',
        'carry_from_physical_contract', 'inventory_origin_unresolved', 'inventory_entry_reachable']
    frame, rules, context_sources, chains = _read_context_chain(replay, diagnosis, gaps,
        frame_columns=frame_columns, rule_columns=rule_columns, scope_products=scope,
        full_rule_scope=True, restore_context_prefix=True)
    sources.extend(context_sources)
    # This is a necessary dependency proof, not a repair acceptance or a new
    # training dataset. The canonical replay must verify the complete families.
    proof = inventory_entry_reachability(whole_contract_entry_frame(frame, maximum), rules)
    candidates = proof.filter(~pl.col('inventory_entry_reachable')).join(gaps,
        on=KEYS, how='inner', validate='1:1').sort('product', 'contract', 'date')
    output.mkdir(parents=True, exist_ok=True)
    atomic_write_parquet(output / 'empty_capacity_candidates.parquet', candidates)
    candidates.write_csv(output / 'empty_capacity_candidates.csv')
    matches = (pl.concat(matched, how='diagonal_relaxed') if matched else pl.DataFrame(schema={
        'date': pl.String, 'futures_id': pl.String, 'contract_date': pl.String,
        'settlement_price': pl.Float64, 'source_sha256': pl.String}))
    atomic_write_parquet(output / 'finmind_exact_key_matches.parquet', matches)
    for path in (worklist, operand_path, terms, config):
        sources.append(dict(path=str(path), sha256=sha256_file(path)))
    result = dict(contract=CONTRACT, status='source_inventory_and_empty_capacity_candidates_not_admitted',
        original_gap_rows=gaps.height, clearing_keys=requests.height, clearing_dates=requests['date'].n_unique(),
        finmind_verified_day_receipts=requests['date'].n_unique()-len(missing_days),
        finmind_missing_receipt_dates=missing_days, finmind_exact_matching_rows=matches.height,
        finmind_positive_settlement_rows=matches.filter(pl.col('settlement_price').is_finite()
            & (pl.col('settlement_price') > 0)).height,
        finlab_catalog_keys=len(catalog_data['keys']), finlab_futures_keys=finlab_keys,
        finlab_explicit_daily_settlement_fields=[k for k in finlab_keys if '結算' in k or 'settle' in k.lower()],
        candidate_empty_coordinates=candidates.height, candidate_products=sorted(set(candidates['product'])),
        candidate_continuation_coordinates=int(candidates['lost_inventory_continuation'].sum()),
        maximum_volume_participation=maximum, context_products=len(scope), context_account_rows=rules.height,
        replay_chain_manifests=chains, full_accounting_rebuilds=0, financial_values_admitted=False,
        provider_queries=0, sources=sources, elapsed_s=(datetime.now(UTC)-begun).total_seconds(),
        outputs={p.name: dict(sha256=sha256_file(p)) for p in output.iterdir() if p.is_file()})
    atomic_write_json(output / 'manifest.json', result)
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('replay', 'diagnosis', 'terms', 'config', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--finmind-root', type=Path, default=Path('data_finmind'))
    parser.add_argument('--finlab-root', type=Path, default=Path('data_finlab'))
    args = parser.parse_args()
    result = audit(replay=args.replay, diagnosis=args.diagnosis, terms=args.terms, config=args.config,
        finmind=args.finmind_root, finlab=args.finlab_root, output=args.output)
    print(json.dumps({k:v for k,v in result.items() if k not in ('sources', 'outputs', 'candidate_products')}, ensure_ascii=False))
