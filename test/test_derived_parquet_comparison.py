from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.compare_derived_parquet import compare, logical_equal, main


def test_null_nan_infinity_and_integer_fidelity():
    a = pa.array([None, float('nan'), 1., float('inf'), -float('inf')])
    b = pa.array([float('nan'), None, 1., float('inf'), float('inf')])
    assert logical_equal(a, b).to_pylist() == [False, False, True, True, False]
    assert logical_equal(pa.array([2**63+9], type=pa.uint64()),
                         pa.array([2**63+10], type=pa.uint64())).to_pylist() == [False]


def table(values, dates=None):
    return pa.table({'date': dates or [date(2020, 1, 1), date(2020, 1, 2)],
                     'symbol': ['engineering-fixture']*2, 'value': values})


def test_complete_scope_comparison_and_different_row_groups(tmp_path):
    a, b = tmp_path/'a.parquet', tmp_path/'b.parquet'
    pq.write_table(table([1., float('nan')]), a, row_group_size=1)
    pq.write_table(table([1., float('nan')]).append_column('raw', pa.array([5., 6.])), b)
    r = compare(a, b)
    assert r['logical_common_columns_equal'] and r['compared_rows'] == 2
    assert r['added_right_columns'] == ['raw']
    assert not r['checkpoint_resume_compatibility_verified']


def test_explicit_cutoff_ignores_only_later_observations(tmp_path):
    a, b = tmp_path/'a.parquet', tmp_path/'b.parquet'
    pq.write_table(table([1., 2.]), a)
    pq.write_table(table([1., 200.]), b)
    assert compare(a, b)['different_cells_by_column'] == {'value': 1}
    assert compare(a, b, end_date=date(2020, 1, 1))['logical_common_columns_equal']


def test_row_key_mismatch_rejected(tmp_path):
    a, b = tmp_path/'a.parquet', tmp_path/'b.parquet'
    pq.write_table(table([1., 2.]), a)
    pq.write_table(table([1., 2.], [date(2020, 1, 2), date(2020, 1, 1)]), b)
    with pytest.raises(ValueError, match='row keys'):
        compare(a, b)


def test_symbol_scope_is_explicit(tmp_path):
    a, b = tmp_path/'a.parquet', tmp_path/'b.parquet'
    full = pa.table({'date': [date(2020, 1, 1)]*2, 'symbol': ['selected', 'other'], 'value': [1., 99.]})
    pq.write_table(full, a)
    pq.write_table(full.slice(0, 1), b)
    assert not compare(a, b)['logical_common_columns_equal']
    result = compare(a, b, symbols={'selected'})
    assert result['selected_symbol_count'] == 1 and result['logical_common_columns_equal']


@pytest.mark.parametrize('target', ['left', 'right', 'existing-evidence'])
def test_cli_output_cannot_overwrite_inputs_or_existing_evidence(tmp_path, monkeypatch, target):
    left, right, evidence = tmp_path/'a.parquet', tmp_path/'b.parquet', tmp_path/'existing.json'
    pq.write_table(table([1., 2.]), left)
    pq.write_table(table([1., 2.]), right)
    evidence.write_text('prior evidence remains intact\n')
    output = {'left':left, 'right':right, 'existing-evidence':evidence}[target]
    before = {p:p.read_bytes() for p in (left, right, evidence)}
    monkeypatch.setattr('sys.argv', ['compare', str(left), str(right), '--output', str(output)])
    with pytest.raises(ValueError, match='new path'):
        main()
    assert {p:p.read_bytes() for p in before} == before
