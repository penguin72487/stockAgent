from datetime import date
import json

import numpy as np
import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from stockagent.data.tw_futures_decision_clock import (
    PARTIAL_TWD_CLOCK_DATASET, QUARANTINE_CONTRACT, bind_entry_specifications,
    clock_dataset, decision_clock_contract, reprice_margin_entries, validate_clock_source,
)
from stockagent.data.tw_futures_execution_terms import SPEC_FIELDS, compile_execution_terms
from test_tw_all_twd_decision_clock import _bound_source
from test_tw_futures_decision_clock import execution
from test_tw_futures_execution_terms import inputs, specification


def _write_json(path, value):
    path.write_text(json.dumps(value))
    return sha256_file(path)


def _clock(tmp_path):
    daily, rules = tmp_path / 'daily', tmp_path / 'rules'
    daily.write_bytes(b'daily'); rules.write_bytes(b'rules')
    keys = [('TGF:202602', 'partial_observed_minute'),
            ('TGF:202603', 'unverified_execution_quarantined')]
    entry = tmp_path / 'entries.parquet'
    pl.DataFrame(dict(date=[date(2026, 1, 5)] * 2, physical_contract=[k[0] for k in keys],
        symbol=['slot1', 'slot2'], entry_quote=[100., None], entry_volume=[2., 0.],
        source_file_sha256=['a' * 64, None], status=[k[1] for k in keys]),
        schema_overrides={'entry_quote': pl.Float64}).write_parquet(entry)
    quarantine = tmp_path / 'execution_quarantine.json'
    quarantine_sha = _write_json(quarantine, dict(contract=QUARANTINE_CONTRACT,
        source_daily_sha256=sha256_file(daily), source_rules_sha256=sha256_file(rules),
        contract_days=[dict(date='2026-01-05', physical_contract=k[0], reason='incomplete') for k in keys]))
    partial = tmp_path / 'partial_observations.parquet'
    pl.DataFrame(dict(date=[date(2026, 1, 5)], physical_contract=[keys[0][0]],
        window_minutes=[1], decision_time=['08:45'], entry_quote=[100.], entry_volume=[2.],
        source_file_sha256=['a' * 64])).write_parquet(partial)
    diag = tmp_path / 'partial_observations_manifest.json'
    diag_sha = _write_json(diag, dict(contract='tw_futures_partial_window_observations_diagnostic_v1',
        research_only=True, training_ready=False, source_coverage_complete=False, candidate_rows=1,
        candidate_file=dict(sha256=sha256_file(partial)), sources=dict(daily_sha256=sha256_file(daily),
            rules_sha256=sha256_file(rules), quarantine_sha256=quarantine_sha)))
    readback = tmp_path / 'partial_observations_readback.json'
    readback_sha = _write_json(readback, dict(contract='tw_futures_partial_window_independent_arithmetic_v1',
        independent_implementation=True, candidate_sha256=sha256_file(partial),
        diagnostic_manifest_sha256=diag_sha, rows_checked=1))
    _, specs = _bound_source()
    folder = tmp_path / 'entry_specifications'; folder.mkdir()
    specs.write_parquet(folder / 'specifications.parquet')
    _write_json(folder / 'manifest.json', dict(dataset='taifex_dated_product_specifications',
        outputs={'specifications.parquet': dict(sha256=sha256_file(folder / 'specifications.parquet'))}))
    manifest = dict(dataset=PARTIAL_TWD_CLOCK_DATASET, clock=decision_clock_contract('08:45',
        asset_scope='all_twd', window_minutes=1, quarantine_unknown=True, allow_partial_observations=True),
        status='complete', rows=2, start_date='2026-01-05', end_date='2026-01-05',
        entries_sha256=sha256_file(entry), source_daily_sha256=sha256_file(daily),
        source_rules_sha256=sha256_file(rules), source_coverage_complete=False, coverage_gaps=2,
        execution_quarantine=dict(file=quarantine.name, sha256=quarantine_sha, contract_days=2),
        entry_specifications=dict(directory=folder.name, data_sha256=sha256_file(folder / 'specifications.parquet'),
            manifest_sha256=sha256_file(folder / 'manifest.json')),
        partial_observations=dict(data_file=partial.name, data_sha256=sha256_file(partial),
            manifest_file=diag.name, manifest_sha256=diag_sha, readback_file=readback.name,
            readback_sha256=readback_sha, contract_days=1))
    _write_json(tmp_path / 'manifest.json', manifest)
    return entry, daily, rules, manifest


def test_partial_clock_needs_explicit_version_source_and_preserves_unknown(tmp_path):
    entry, daily, rules, manifest = _clock(tmp_path)
    frame, readback = validate_clock_source(entry, daily_path=daily, rules_path=rules, decision_time='08:45')
    assert frame['status'].to_list() == ['partial_observed_minute', 'unverified_execution_quarantined']
    assert readback['source_coverage_complete'] is False
    assert clock_dataset('all_twd', quarantine_unknown=True, allow_partial_observations=True) == PARTIAL_TWD_CLOCK_DATASET
    assert decision_clock_contract('08:45', asset_scope='all_twd')['version'] == 5
    with pytest.raises(ValueError, match='partial observations require'):
        decision_clock_contract('08:45', asset_scope='all_twd', allow_partial_observations=True)
    # Candidate bytes cannot drift, and a v5 manifest cannot inherit v6 rows.
    (tmp_path / 'partial_observations.parquet').write_bytes(b'changed')
    with pytest.raises(ValueError, match='evidence'):
        validate_clock_source(entry, daily_path=daily, rules_path=rules, decision_time='08:45')
    manifest['clock'] = decision_clock_contract('08:45', asset_scope='all_twd', quarantine_unknown=True)
    _write_json(tmp_path / 'manifest.json', manifest)
    with pytest.raises(ValueError, match='incompatible'):
        validate_clock_source(entry, daily_path=daily, rules_path=rules, decision_time='08:45')


def test_partial_repricing_uses_only_observed_volume_and_requires_opt_in():
    args = inputs()
    specs = pl.DataFrame([specification(tax_class='gold')], schema=SPEC_FIELDS)
    args['specifications'] = specs
    old, _ = compile_execution_terms(**args)
    facts = args['frame'].select('date', 'product', 'physical_contract', 'open').join(
        old.drop('product', 'contract'), on=['date', 'physical_contract'], maintain_order='left').with_columns(
        pl.lit('commodity_future').alias('asset_class'), pl.lit('partial_observed_minute').alias('status'),
        (pl.col('open') + 7.).alias('entry_quote'), pl.lit(2.).alias('entry_volume'))
    facts = bind_entry_specifications(facts, specs)
    ex = np.repeat(execution(), facts.height, axis=0)
    ex[:, 3] = old['opening_contract_value_twd'].to_numpy()
    ex[:, 4] = old['terminal_contract_value_twd'].to_numpy()
    ex[:, 6] = old['opening_tax_twd'].to_numpy()
    with pytest.raises(ValueError, match='explicit research clock'):
        reprice_margin_entries(ex, facts, participation=.5, broker_multiplier=1., asset_scope='all_twd')
    result = reprice_margin_entries(ex, facts, participation=.5, broker_multiplier=1.,
        asset_scope='all_twd', capacity_policy='single_contract_probe', allow_partial_observations=True)
    verified = reprice_margin_entries(ex, facts.with_columns(pl.lit('observed_minute').alias('status')),
        participation=.5, broker_multiplier=1., asset_scope='all_twd', capacity_policy='single_contract_probe')
    np.testing.assert_array_equal(result, verified)
    assert np.all(result[:, 8] <= 1.)
