from datetime import date

import polars as pl
import pytest

from scripts.derive_tw_futures_partial_observation_clocks import merge_partial_observations
from scripts.verify_tw_futures_partial_clock_derivation import compare_tables


def inputs():
    entries = pl.DataFrame(dict(date=[date(2026, 1, 5)] * 3,
        physical_contract=['I5F:01', 'I5F:02', 'TX:01'], symbol=['a', 'b', 'c'],
        status=['unverified_execution_quarantined', 'unverified_execution_quarantined', 'observed_minute'],
        entry_quote=[None, None, 200.], entry_volume=[0., 0., 4.],
        source_file_sha256=[None, None, 'b' * 64]), schema_overrides={'entry_quote': pl.Float64})
    candidates = pl.DataFrame(dict(date=[date(2026, 1, 5)], physical_contract=['I5F:01'],
        decision_time=['08:45'], window_minutes=[1], entry_quote=[100.], entry_volume=[2.],
        source_file_sha256=['a' * 64]))
    return entries, candidates


def test_only_quarantined_observation_is_changed_with_original_order():
    entries, candidates = inputs()
    result = merge_partial_observations(entries, candidates, decision_time='08:45', window_minutes=1)
    assert result.columns == entries.columns
    assert result['physical_contract'].to_list() == entries['physical_contract'].to_list()
    assert result.row(0, named=True)['status'] == 'partial_observed_minute'
    assert result.row(0, named=True)['entry_volume'] == 2.
    assert result.row(0, named=True)['entry_quote'] == 100.
    assert result.slice(1).equals(entries.slice(1))
    assert merge_partial_observations(entries, candidates, decision_time='09:00', window_minutes=1).equals(entries)


def test_candidate_must_be_unique_quarantined_and_real():
    entries, candidate = inputs()
    with pytest.raises(ValueError, match='duplicated'):
        merge_partial_observations(entries, pl.concat([candidate, candidate]), decision_time='08:45', window_minutes=1)
    with pytest.raises(ValueError, match='original quarantine'):
        merge_partial_observations(entries, candidate.with_columns(pl.lit('TX:01').alias('physical_contract')),
                                   decision_time='08:45', window_minutes=1)
    with pytest.raises(ValueError, match='positive real print'):
        merge_partial_observations(entries, candidate.with_columns(pl.lit(0.).alias('entry_volume')),
                                   decision_time='08:45', window_minutes=1)
    with pytest.raises(ValueError, match='positive real print'):
        merge_partial_observations(entries, candidate.with_columns(pl.lit(2.5).alias('entry_volume')),
                                   decision_time='08:45', window_minutes=1)


def test_independent_arrow_readback_rejects_unrelated_or_wrong_candidate_change():
    entries, candidates = inputs()
    result = merge_partial_observations(entries, candidates, decision_time='08:45', window_minutes=1)
    old, new, partial = entries.to_arrow(), result.to_arrow(), candidates.to_arrow()
    assert compare_tables(old, new, partial, decision_time='08:45', window_minutes=1) == 1
    damaged = result.with_columns(pl.when(pl.col('symbol') == 'c').then(pl.lit('wrong'))
                                  .otherwise(pl.col('symbol')).alias('symbol')).to_arrow()
    with pytest.raises(ValueError, match='unchanged clock field'):
        compare_tables(old, damaged, partial, decision_time='08:45', window_minutes=1)
    damaged = result.with_columns(pl.when(pl.col('symbol') == 'a').then(101.)
                                  .otherwise(pl.col('entry_quote')).alias('entry_quote')).to_arrow()
    with pytest.raises(ValueError, match='differ from candidate'):
        compare_tables(old, damaged, partial, decision_time='08:45', window_minutes=1)
