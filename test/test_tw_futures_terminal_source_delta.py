from datetime import date
import json

import polars as pl
import pytest

from downloader.artifact_io import atomic_write_json, atomic_write_parquet, sha256_file
from scripts.prepare_tw_futures_margin_training import _accounting_terminal_source, _TerminalInputSource
from stockagent.data.tw_futures_margin_preparation import subscription_right_value_twd, terminal_contract_value_twd


@pytest.fixture
def terminal_delta(tmp_path):
    source, delta = tmp_path / 'source', tmp_path / 'pending'
    atomic_write_json(source / 'source_manifest.json', {'contract': 'retained-source'})
    old = pl.DataFrame([dict(date=date(2011, 12, 21), product='AB1', contract='201112', rights_twd=1.)])
    added = pl.DataFrame([dict(date=date(2011, 12, 21), product='ES1', contract='201112', rights_twd=64.)])
    atomic_write_parquet(source / 'terminal/terminal_subscription_values.parquet', old)
    atomic_write_json(source / 'terminal/manifest.json', {'outputs': {
        'terminal_subscription_values.parquet': {'sha256': sha256_file(source / 'terminal/terminal_subscription_values.parquet')}}})
    root = delta / 'terminal'
    atomic_write_parquet(root / 'terminal_subscription_values.parquet', pl.concat([old, added]))
    atomic_write_parquet(root / 'terminal_overlay_rows.parquet', added)
    atomic_write_json(root / 'sources/operands.json', {'close': '40.50', 'strike': '40', 'qty': '129.1638'})
    proof = dict(status='source_bound_terminal_components',
        parent_source_manifest_sha256=sha256_file(source / 'source_manifest.json'),
        parent_terminal_manifest_sha256=sha256_file(source / 'terminal/manifest.json'),
        sources=[dict(path='sources/operands.json', sha256=sha256_file(root / 'sources/operands.json'))],
        outputs={p.name: dict(sha256=sha256_file(p)) for p in root.glob('*.parquet')})
    atomic_write_json(root / 'manifest.json', proof)
    atomic_write_json(delta / 'manifest.json', {'terminal_source_delta': dict(
        contract='bound_terminal_operand_source_delta_v1', path='terminal', sha256=sha256_file(root / 'manifest.json'))})
    return source, delta, root


def _rebind_pointer(delta, root):
    top = json.loads((delta / 'manifest.json').read_text())
    top['terminal_source_delta']['sha256'] = sha256_file(root / 'manifest.json')
    atomic_write_json(delta / 'manifest.json', top)


def test_deadline_cash_fixing_cannot_be_replaced_by_adjustment_day_zero():
    rights = subscription_right_value_twd('40.50', '40', '129.1638')
    assert rights == 64.0
    assert terminal_contract_value_twd('40.94', '2000', 0, rights) == 81944.0
    assert terminal_contract_value_twd('40.94', '2000', 0, 0) != 81944.0


def test_terminal_view_changes_only_bound_terminal_input_and_retains_all_originals(terminal_delta):
    source, delta, root = terminal_delta
    view, identity = _accounting_terminal_source(source, delta)
    assert view / 'terminal/terminal_subscription_values.parquet' == root / 'terminal_subscription_values.parquet'
    for relative in ('observations/all_futures_daily_sessions.parquet', 'rules/manifest.json',
                     'source_manifest.json', 'halt', 'final/futures_final_settlement_history.parquet'):
        assert view / relative == source / relative
    assert identity['contract'] == 'bound_terminal_operand_source_delta_v1'
    assert len(identity['implementation_sha256']) == 64


@pytest.mark.parametrize('fault', ['parent', 'terminal_parent', 'source', 'escape', 'output', 'drop_old', 'change_old', 'duplicate_new'])
def test_terminal_source_overlay_rejects_ambiguous_or_changed_financial_evidence(terminal_delta, fault):
    source, delta, root = terminal_delta
    manifest = json.loads((root / 'manifest.json').read_text())
    if fault in {'parent', 'terminal_parent'}:
        manifest['parent_source_manifest_sha256' if fault == 'parent' else 'parent_terminal_manifest_sha256'] = '0' * 64
    elif fault == 'source':
        (root / 'sources/operands.json').write_text('{}')
    elif fault == 'escape':
        top = json.loads((delta / 'manifest.json').read_text()); top['terminal_source_delta']['path'] = '../source'
        atomic_write_json(delta / 'manifest.json', top)
        with pytest.raises((ValueError, FileNotFoundError)):
            _accounting_terminal_source(source, delta)
        return
    else:
        p = root / 'terminal_subscription_values.parquet'
        frame = pl.read_parquet(p)
        if fault == 'drop_old': frame = frame.filter(pl.col('product') == 'ES1')
        elif fault == 'change_old': frame = frame.with_columns(pl.when(pl.col('product') == 'AB1').then(2.).otherwise(pl.col('rights_twd')).alias('rights_twd'))
        elif fault == 'duplicate_new': frame = pl.concat([frame, frame.filter(pl.col('product') == 'ES1')])
        else: frame = frame.with_columns(pl.lit(0.).alias('rights_twd'))
        atomic_write_parquet(p, frame)
        if fault != 'output': manifest['outputs'][p.name]['sha256'] = sha256_file(p)
    atomic_write_json(root / 'manifest.json', manifest)
    _rebind_pointer(delta, root)
    with pytest.raises(ValueError):
        _accounting_terminal_source(source, delta)


def test_default_source_is_retained_and_paths_cannot_escape(terminal_delta):
    source, delta, root = terminal_delta
    atomic_write_json(delta / 'manifest.json', {})
    assert _accounting_terminal_source(source, delta) == (source, None)
    view = _TerminalInputSource(source, root)
    for name in ('/etc/passwd', '../source/terminal/terminal_subscription_values.parquet'):
        with pytest.raises(ValueError):
            view / name
