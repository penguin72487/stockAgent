from datetime import date
import json

import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from scripts.rebind_tw_futures_clock_slice import (
    MARKET_COLUMNS, merge_disjoint_observations, project_observations,
    split_disjoint_axis, validate_disjoint_delta, validate_market_axis,
)


def market():
    row = {name: 1 for name in MARKET_COLUMNS}
    row.update(date=date(2025, 3, 3), product='TX', contract='202503', asset_class='index_future',
               physical_contract='TX:202503', symbol='slot-old', official_source_sha256='a' * 64)
    return pl.DataFrame([row])


def test_verified_physical_axis_allows_only_slot_changes():
    original = market()
    revised = original.with_columns(pl.lit('slot-new').alias('symbol'))
    axis = validate_market_axis(original, revised)
    assert axis['symbol'].to_list() == ['slot-new']


@pytest.mark.parametrize('field,value', [('close', 2), ('volume', 9), ('product', 'MTX'),
                                          ('official_source_sha256', 'b' * 64)])
def test_changed_market_input_is_rejected(field, value):
    original = market()
    with pytest.raises(ValueError, match='differ'):
        validate_market_axis(original, original.with_columns(pl.lit(value).alias(field)))


def test_unobserved_contract_or_duplicate_is_rejected():
    original = market()
    with pytest.raises(ValueError, match='differ'):
        validate_market_axis(original, original.with_columns(pl.lit('MTX:202503').alias('physical_contract')))
    with pytest.raises(ValueError, match='invalid'):
        validate_market_axis(original, pl.concat([original, original]))


def test_projection_preserves_observation_and_reference_values():
    original = market().select('date', 'physical_contract', 'symbol')
    target = original.with_columns(pl.lit('slot-new').alias('symbol'))
    observations = original.with_columns(pl.lit(123.5).alias('entry_quote'),
                                          pl.lit('actual-source').alias('reference_source'))
    result = project_observations(observations, original, target)
    assert result['symbol'].to_list() == ['slot-new']
    assert result['entry_quote'].to_list() == [123.5]
    assert result['reference_source'].to_list() == ['actual-source']


def test_projection_refuses_missing_or_wrong_parent_slot():
    axis = market().select('date', 'physical_contract', 'symbol')
    with pytest.raises(ValueError, match='do not match'):
        project_observations(axis.head(0), axis, axis)
    with pytest.raises(ValueError, match='do not match'):
        project_observations(axis.with_columns(pl.lit('wrong').alias('symbol')), axis, axis)


def test_disjoint_delta_rebind_keeps_parent_values_and_adds_verified_physical_contract():
    original = market()
    target = original.with_columns(pl.lit('new-slot').alias('symbol'))
    addition = original.with_columns(pl.lit('MTX').alias('product'),
        pl.lit('MTX:202503').alias('physical_contract'), pl.lit('added-slot').alias('symbol'))
    common_axis, delta_axis = split_disjoint_axis(original, pl.concat([target, addition]))
    parent_axis = original.select('date', 'physical_contract', 'symbol')
    parent = parent_axis.with_columns(pl.lit(123.5).alias('entry_quote'))
    delta = delta_axis.with_columns(pl.lit(42.0).alias('entry_quote'))
    combined = merge_disjoint_observations(parent, parent_axis, common_axis, delta, delta_axis)
    assert combined.sort('physical_contract')['entry_quote'].to_list() == [42.0, 123.5]
    assert combined.sort('physical_contract')['symbol'].to_list() == ['added-slot', 'new-slot']


def test_disjoint_delta_rejects_changed_parent_missing_parent_and_extended_lifetime():
    old = market()
    addition = old.with_columns(pl.lit('MTX:202503').alias('physical_contract'))
    with pytest.raises(ValueError, match='differ'):
        split_disjoint_axis(old, pl.concat([old.with_columns(pl.lit(2, dtype=pl.Int64).alias('close')), addition]))
    with pytest.raises(ValueError, match='entire parent'):
        split_disjoint_axis(old, addition)
    extra_day = old.with_columns(pl.lit(date(2025, 3, 4)).alias('date'))
    with pytest.raises(ValueError, match='existing physical lifetime'):
        split_disjoint_axis(old, pl.concat([old, extra_day]))


def test_disjoint_delta_rejects_incorrect_slot_or_missing_observation():
    old = market()
    addition = old.with_columns(pl.lit('MTX:202503').alias('physical_contract'))
    common_axis, delta_axis = split_disjoint_axis(old, pl.concat([old, addition]))
    parent_axis = old.select('date', 'physical_contract', 'symbol')
    parent = parent_axis.with_columns(pl.lit(1.0).alias('entry_quote'))
    delta = delta_axis.with_columns(pl.lit(2.0).alias('entry_quote'))
    for invalid in (delta.head(0), delta.with_columns(pl.lit('wrong').alias('symbol'))):
        with pytest.raises(ValueError, match='exact physical axis'):
            merge_disjoint_observations(parent, parent_axis, common_axis, invalid, delta_axis)


def test_delta_receipt_rejects_changed_original_and_partial_promotion(tmp_path):
    files = {}
    for name in ('old_daily', 'old_rules', 'daily', 'rules', 'specifications.parquet', 'manifest.json', 'raw'):
        files[name] = tmp_path / name
        files[name].write_bytes(name.encode())
    release = tmp_path / 'release.json'
    release.write_text(json.dumps({'code': {'source_sha256': 'f' * 64}}))
    outputs = {}
    for index in range(8):
        path = tmp_path / f'output{index}'
        path.write_text(str(index))
        outputs[str(path)] = sha256_file(path)
    inputs = {str(path): sha256_file(path) for path in [*files.values(), release]}
    day = date(2025, 3, 3)
    axis = market().select('date', 'physical_contract', 'symbol')
    proof = {'contract': 'independent_disjoint_physical_clock_delta_v1',
        'start': str(day), 'end': str(day), 'full_lifetimes_rebuilt': True,
        'partial_clock_promoted': False, 'all_history_or_products_training_ready': False,
        'added_rows': 1, 'added_physical_contracts': ['TX:202503'],
        'source_release': {'source_files_verified': True, 'source_sha256': 'f' * 64,
                           'receipt_sha256': sha256_file(release)},
        'inputs': inputs, 'outputs': outputs,
        'raw_reconstruction': {'object': {'path': str(files['raw']), 'sha256': sha256_file(files['raw'])}}}
    report = tmp_path / 'readback.json'
    kwargs = dict(old_daily=files['old_daily'], old_rules=files['old_rules'], daily=files['daily'],
        rules=files['rules'], specs=files['specifications.parquet'], start=day, end=day, axis=axis)
    report.write_text(json.dumps(proof))
    assert validate_disjoint_delta(report, **kwargs)['added_rows'] == 1
    files['raw'].write_text('changed')
    with pytest.raises(ValueError, match='changed'):
        validate_disjoint_delta(report, **kwargs)
    files['raw'].write_bytes(b'raw')
    proof['partial_clock_promoted'] = True
    report.write_text(json.dumps(proof))
    with pytest.raises(ValueError, match='incompatible scope'):
        validate_disjoint_delta(report, **kwargs)
