#!/usr/bin/env python3
"""Read-only, scoped verification of sourced paper close reconciliation.

Checks the appended fills against the retained pre-close inventory, original
journal prefixes, official raw sources, fees and NAV. Minute coverage is a
separate result: a proven close does not repair missing interior price marks.
Never sends orders, changes signals, reconstructs prices or edits a ledger.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timedelta
import json
import math
from pathlib import Path
import sys
from time import perf_counter

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.audit_tw_day_trade_restart import verify_prefix
from scripts.audit_tw_day_trade_margin_replay import _verify_terminal_close_fills
from scripts.settle_tw_day_trade_official_close import official_closes
from stockagent.live.tw_day_trade_simulation import (
    TAIPEI, TERMINAL_CLOSE_UNLIMITED_CONTRACT, _atomic_json,
)


def _equal(actual, expected, label):
    if not math.isclose(float(actual), float(expected), abs_tol=1e-6, rel_tol=1e-12):
        raise ValueError(f"close accounting mismatch: {label}")


def audit(state_dir: Path, before_path: Path, raw_root: Path, day: date):
    started = perf_counter()
    before = json.loads(before_path.read_text())
    old = json.loads(before_path.with_suffix('.state.json').read_text())
    current = json.loads((state_dir / 'state.json').read_text())
    if current.get('simulation_only') is not True or current.get('production_order_possible') is not False:
        raise ValueError('not a paper-only ledger')
    if set(old['modes']) != set(current['modes']):
        raise ValueError('mode identity changed')
    prefixes = verify_prefix(state_dir, before)
    prefix_ms = (perf_counter() - started) * 1000
    residuals = {(market, pid): p for market, mode in old['modes'].items()
                 for pid, p in mode.get('positions', {}).items() if p.get('signed_shares')}
    if any(old['modes'][market].get('session_date') != str(day) for market, _ in residuals):
        raise ValueError('pre-close inventory is outside the requested session')
    with (state_dir / 'fills.jsonl').open('rb') as stream:
        stream.seek(before['inputs']['fills.jsonl']['size_bytes'])
        fills = [json.loads(line) for line in stream if line.strip()]
    if len(fills) != len(residuals):
        raise ValueError('appended close count differs from pre-close inventory')
    keys = [(f['market'], f['position_id']) for f in fills]
    if len(set(keys)) != len(keys) or set(keys) != set(residuals):
        raise ValueError('duplicate, extra or missing paper close identity')
    _, sources = official_closes(raw_root, day)
    counts = Counter(f['market'] for f in fills)
    rebuild = {'replay_contract': {'terminal_close_contract': TERMINAL_CLOSE_UNLIMITED_CONTRACT},
               'sessions': [{'session_date': str(day), 'close': {'terminal_close_sources': sources},
                             'modes': [{'market': k, 'after_close': {
                                 'terminal_close_contract': m.get('terminal_close_contract'),
                                 'terminal_flatten_count': m.get('terminal_flatten_count'),
                                 'open_position_rows': sum(bool(p.get('signed_shares'))
                                     for p in m.get('positions', {}).values()),
                             }} for k, m in current['modes'].items()]}]}
    source_hashes = _verify_terminal_close_fills(fills, rebuild, current['modes'])
    net_by_market = {k: [] for k in current['modes']}
    for fill in fills:
        market, pid = fill['market'], fill['position_id']
        position = residuals[(market, pid)]
        after_position = current['modes'][market]['positions'][pid]
        signed = int(position['signed_shares'])
        quantity, price = abs(signed), float(fill['price'])
        if fill['quantity'] != quantity or after_position['signed_shares'] != 0:
            raise ValueError('residual shares were not fully closed')
        if fill['symbol'] != position['symbol'] or after_position['exit_price'] != price:
            raise ValueError('close symbol/price differs from inventory')
        prefix = 'cash_' if position.get('margin_carry_contract') else ''
        rate = float(position[prefix + ('sell_fee_rate' if signed > 0 else 'buy_fee_rate')])
        gross_fee = quantity * price * rate
        rebate = quantity * price * float(position.get('commission_rebate_rate') or 0)
        fee = gross_fee - rebate
        basis = float(position.get('inventory_basis_price', position['entry_price']))
        gross_pnl = (1 if signed > 0 else -1) * quantity * (price - basis)
        entry_fee = float(position.get('remaining_entry_fee_twd', position.get('entry_fee_twd', 0)) or 0)
        net = gross_pnl - entry_fee - fee
        for field, value in (('fee_and_tax_twd', fee), ('gross_fee_and_tax_twd', gross_fee),
                             ('commission_rebate_accrued_twd', rebate), ('gross_pnl_twd', gross_pnl),
                             ('entry_fee_allocated_twd', entry_fee), ('net_pnl_twd', net)):
            _equal(fill[field], value, f'{market}/{pid}/{field}')
        _equal(after_position['realized_net_pnl_twd'],
               float(position.get('realized_net_pnl_twd') or 0) + net, f'{market}/{pid}/realized')
        if after_position['remaining_entry_fee_twd'] != 0:
            raise ValueError('closed position retains unallocated entry fee')
        net_by_market[market].append(net)
    result = {}
    for market, mode in current['modes'].items():
        original = old['modes'][market]
        for field in ('signal_id', 'processed_signal_ids', 'session_sizing_nav_twd',
                      'checkpoint_fingerprint', 'config_fingerprint', 'initial_capital_twd'):
            if mode.get(field) != original.get(field):
                raise ValueError(f'original signal or sizing changed: {market}/{field}')
        _equal(mode['cumulative_realized_net_pnl_twd'],
               float(original.get('cumulative_realized_net_pnl_twd') or 0) + math.fsum(net_by_market[market]),
               f'{market}/cumulative_realized')
        _equal(mode['open_net_liquidation_pnl_twd'], 0, f'{market}/unrealized')
        expected_nav = (float(mode['initial_capital_twd']) + float(mode['cumulative_realized_net_pnl_twd'])
                        + float(mode.get('cumulative_corporate_action_net_twd') or 0)
                        - float(mode.get('cumulative_carry_cost_twd') or 0))
        _equal(mode['total_equity_twd'], expected_nav, f'{market}/NAV')
        result[market] = {'settled_count': counts[market], 'open_position_count': 0,
                          'ending_equity_twd': mode['total_equity_twd'],
                          'closing_net_pnl_twd': math.fsum(net_by_market[market]),
                          'signal_id': mode.get('signal_id')}
    # Latest canonical revision per minute wins; do not interpolate gaps.
    rows = {}
    needle = str(day).encode()
    with (state_dir / 'marks.jsonl').open('rb') as stream:
        for line in stream:
            if needle not in line:
                continue
            row = json.loads(line)
            if row.get('session_date') == str(day) and row.get('market') in current['modes']:
                rows[(row['market'], row['minute'])] = row
    first = datetime.fromisoformat(f'{day}T09:01:00+08:00')
    expected = {(first + timedelta(minutes=i)).isoformat(timespec='minutes') for i in range(270)}
    for market, mode in current['modes'].items():
        actual = {minute for m, minute in rows if m == market}
        endpoint = rows.get((market, f'{day}T13:30+08:00'))
        if endpoint is None or endpoint.get('open_position_count') != 0:
            raise ValueError(f'missing flat close endpoint: {market}')
        _equal(endpoint['total_equity_twd'], mode['total_equity_twd'], f'{market}/close_endpoint')
        result[market]['minute_curve'] = {'expected_points': 270, 'observed_points': len(actual),
                                         'missing_points': len(expected - actual),
                                         'unexpected_points': len(actual - expected),
                                         'complete_time_grid': actual == expected,
                                         'close_endpoint_verified': True}
    return {'schema_version': 1, 'status': 'accepted_close_reconciliation', 'session_date': str(day),
            'observed_at': datetime.now(TAIPEI).isoformat(), 'simulation_only': True,
            'production_order_possible': False, 'production_state_modified': False,
            'settled_count': len(fills), 'remaining_count': 0, 'ledger_prefixes': prefixes,
            'source_hashes': source_hashes, 'source_receipts': sources, 'markets': result,
            'minute_grid_complete': all(m['minute_curve']['complete_time_grid'] for m in result.values()),
            'minute_grid_is_not_source_completeness_claim': True,
            'timings_ms': {'original_prefix_verification': prefix_ms,
                           'total_audit': (perf_counter() - started) * 1000}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', type=Path, default=ROOT / 'artifacts/live/tw_day_trade_simulation')
    parser.add_argument('--raw-root', type=Path, default=ROOT / 'data_tw_public/raw')
    parser.add_argument('--date', type=date.fromisoformat, required=True)
    parser.add_argument('--before', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    report = audit(args.state_dir, args.before, args.raw_root, args.date)
    _atomic_json(args.output, report)
    print(json.dumps({k: report[k] for k in ('status', 'settled_count', 'remaining_count',
                                           'minute_grid_complete', 'timings_ms')}, ensure_ascii=False))


if __name__ == '__main__':
    main()
