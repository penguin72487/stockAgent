import json

import pytest

from scripts.audit_tw_day_trade_close_reconciliation import audit
from scripts.audit_tw_day_trade_restart import audit as restart_audit
from test_day_trade_official_close_settlement import reports
from test_day_trade_terminal_close import setup
from test_tw_day_trade_simulation import _now


def closed_fixture(tmp_path):
    from types import SimpleNamespace
    from scripts.run_tw_day_trade_simulation import _attach_terminal_official_close_context

    engine, spec = setup(tmp_path)
    before = tmp_path / 'before.json'
    restart_audit(engine.state_dir, before)
    root = reports(tmp_path, close='995', volume='10')
    quotes = {}
    _attach_terminal_official_close_context(
        quotes, engine=engine, specs=[spec],
        configs={spec.market: SimpleNamespace(day_trade_rule_data_dir=str(tmp_path))},
        observed=_now(13, 35),
    )
    engine.process_quotes(quotes=quotes, now=_now(13, 35))
    return engine, spec, before, root


def test_close_audit_is_read_only_and_does_not_claim_interior_curve_repair(tmp_path):
    engine, spec, before, root = closed_fixture(tmp_path)
    original = {name: (engine.state_dir / name).read_bytes()
                for name in ('state.json', 'fills.jsonl', 'orders.jsonl', 'marks.jsonl', 'events.jsonl')}
    report = audit(engine.state_dir, before, root, _now(13, 35).date())
    assert report['status'] == 'accepted_close_reconciliation'
    assert report['settled_count'] == 1 and report['remaining_count'] == 0
    assert report['minute_grid_complete'] is False
    assert report['markets'][spec.market]['minute_curve']['missing_points'] > 0
    assert report['markets'][spec.market]['minute_curve']['close_endpoint_verified'] is True
    assert len(report['source_hashes']) == 2
    assert original == {name: (engine.state_dir / name).read_bytes() for name in original}


@pytest.mark.parametrize('change', ['quantity', 'net_pnl', 'NAV', 'signal', 'raw_source', 'original_prefix'])
def test_close_audit_rejects_source_quantity_accounting_and_identity_tampering(tmp_path, change):
    engine, spec, before, root = closed_fixture(tmp_path)
    if change in ('quantity', 'net_pnl'):
        lines = engine.fills_path.read_text().splitlines()
        row = json.loads(lines[-1])
        row['quantity' if change == 'quantity' else 'net_pnl_twd'] += 1
        lines[-1] = json.dumps(row)
        engine.fills_path.write_text('\n'.join(lines) + '\n')
    elif change in ('NAV', 'signal'):
        state = json.loads(engine.state_path.read_text())
        mode = state['modes'][spec.market]
        if change == 'NAV':
            mode['total_equity_twd'] += 1
        else:
            mode['signal_id'] = 'different signal'
        engine.state_path.write_text(json.dumps(state))
    elif change == 'raw_source':
        reports(tmp_path, close='990', volume='10')
    else:
        raw = engine.fills_path.read_bytes()
        engine.fills_path.write_bytes(b'!' + raw[1:])
    with pytest.raises((ValueError, RuntimeError)):
        audit(engine.state_dir, before, root, _now(13, 35).date())
