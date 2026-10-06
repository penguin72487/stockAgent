from dataclasses import replace
import json

import numpy as np
import pytest

from stockagent.data.factorized_panel import (
    VALUE_ONLY_CONTRACT, _BlockCache, _ColumnBlock,
    attach_factorized_features, file_sha256, write_array_block,
)
from stockagent.data.factorized_panel_projection import _value_positions, project_factorized_values
from test_factorized_panel import example


def _complete_parent(tmp_path, *, compact):
    root = tmp_path / 'original'
    root.mkdir()
    panel, path, dense = example(root)
    manifest = json.loads(path.read_text())
    manifest.update(source_snapshot_id='fixed-fixture', value_features=2,
        individual_quantities=1, shared_quantities=1, logical_model_channels=10)
    if compact:
        for i, block in enumerate(manifest['blocks']):
            values = _BlockCache(root, [block], 0).get(0)
            proof = write_array_block(root/f'compact-{i}.npy.zst', values, omit_zero_columns=True)
            manifest['blocks'][i] = {**proof, 'start': block['start']}
    from dataclasses import asdict
    from stockagent.data.tw_public_release_schedule import RULES
    definitions = [{'feature': name, 'source': 'fixture', 'rule': {**asdict(RULES['daily']), 'scope': scope}}
        for name, scope in (('asset', 'stock'), ('M2', 'market'))]
    dictionary = root/'feature_dictionary.json'
    dictionary.write_text(json.dumps({'features': definitions, 'aliases': [], 'excluded': []}))
    manifest['feature_dictionary_sha256'] = file_sha256(dictionary)
    for name in ('quality_masks.json', 'execution_rules.parquet'):
        (root/name).write_bytes(b'immutable-fixture')
    (root/'feature_coverage.csv').write_text('feature,scope,source,available_panel_cells\n')
    raw = root/'observations/raw.json'
    raw.parent.mkdir()
    raw.write_text('{"value":null,"published_at":"2025-01-02"}')
    manifest['observations'] = [{'path': 'observations/raw.json', 'sha256': file_sha256(raw)}]
    path.write_text(json.dumps(manifest))
    return panel, path, dense


@pytest.mark.parametrize('compact', [False, True])
def test_projection_retains_only_value_bits_masks_and_raw_null(tmp_path, compact):
    panel, path, dense = _complete_parent(tmp_path, compact=compact)
    before = file_sha256(path)
    out = tmp_path/'values'
    receipt = project_factorized_values(path, out)
    assert receipt['retained_value_bits_exact'] and receipt['removed_metadata_channels'] == 6
    assert receipt['training_ready'] is False and file_sha256(path) == before
    manifest = json.loads((out/'factorized_manifest.json').read_text())
    assert manifest['contract'] == VALUE_ONLY_CONTRACT and manifest['logical_model_channels'] == 4
    assert manifest['individual_channels'] == ['asset'] and manifest['common_channels'] == ['M2']
    attached = attach_factorized_features(replace(panel), out/'factorized_manifest.json')
    np.testing.assert_array_equal(attached.features[:].view(np.uint32), dense[..., [0, 1, 2, 6]].view(np.uint32))
    assert attached.can_buy_mask is panel.can_buy_mask and attached.can_sell_mask is panel.can_sell_mask
    assert json.loads((out/'observations/raw.json').read_text())['value'] is None
    assert (out/'execution_rules.parquet').stat().st_ino == (path.parent/'execution_rules.parquet').stat().st_ino
    assert attached.features.content_fingerprint['sha256'] != attach_factorized_features(replace(panel), path).features.content_fingerprint['sha256']
    assert '不輸入可用性' in (out/'feature_report/stock_0001.md').read_text()
    with pytest.raises(FileExistsError):
        project_factorized_values(path, out)


def test_projection_rejects_unproved_order_and_mutated_parent(tmp_path):
    panel, path, _ = _complete_parent(tmp_path, compact=True)
    manifest = json.loads(path.read_text())
    manifest['individual_channels'][1] = 'wrong'
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match='ordering'):
        project_factorized_values(path, tmp_path/'values')
    with pytest.raises(ValueError, match='complete'):
        _value_positions(['value'])


def test_value_only_attachment_rejects_false_policy_or_count(tmp_path):
    panel, path, _ = _complete_parent(tmp_path, compact=True)
    out = tmp_path/'values'
    project_factorized_values(path, out)
    target = out/'factorized_manifest.json'
    manifest = json.loads(target.read_text())
    manifest['model_channel_policy'] = 'value_available_age_updated'
    target.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match='explicit channel policy'):
        attach_factorized_features(replace(panel), target)
    manifest['model_channel_policy'] = 'value_only'
    manifest['logical_model_channels'] = 10
    target.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match='channel count'):
        attach_factorized_features(replace(panel), target)


def test_projection_preserves_negative_zero_value_bits(tmp_path):
    panel, path, dense = _complete_parent(tmp_path, compact=False)
    manifest = json.loads(path.read_text())
    first = manifest['blocks'][0]
    values = _BlockCache(path.parent, [first], 0).get(0).copy()
    values[0, 0, 0] = np.float32(-0.0)
    proof = write_array_block(path.parent/'signed-zero.zst', values, omit_zero_columns=True)
    manifest['blocks'][0] = {**proof, 'start': first['start']}
    path.write_text(json.dumps(manifest))
    dense[0, 0, 2] = np.float32(-0.0)
    out = tmp_path/'values'
    project_factorized_values(path, out)
    projected = attach_factorized_features(replace(panel), out/'factorized_manifest.json')
    np.testing.assert_array_equal(projected.features[:].view(np.uint32), dense[..., [0, 1, 2, 6]].view(np.uint32))
    assert np.signbit(projected.features[0, 0, 2])


def test_projection_rejects_tampered_raw_member_before_writing(tmp_path):
    _, path, _ = _complete_parent(tmp_path, compact=True)
    (path.parent/'observations/raw.json').write_text('{"value":123}')
    out = tmp_path/'values'
    with pytest.raises(ValueError, match='identity mismatch'):
        project_factorized_values(path, out)
    assert not out.exists()
