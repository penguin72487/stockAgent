import json

import polars as pl
import pytest

from downloader.artifact_io import sha256_file
from scripts.run_finmind_option_reference_event_history import verify
from stockagent.data.tw_option_reference_event_index import CONTRACT


def test_reused_partition_rejects_a_changed_native_source_and_index(tmp_path):
    root = tmp_path / 'complement'
    receipt = root / 'receipts/TaiwanOptionTick/all/2011-01-03.json'
    receipt.parent.mkdir(parents=True)
    native = root / 'native.parquet'
    pl.DataFrame({'value': [1]}).write_parquet(native)
    native_sha = sha256_file(native)
    receipt.write_text(json.dumps({'sha256': native_sha}))
    plan = tmp_path / 'plan.json'
    plan.write_text('{}')
    implementation = tmp_path / 'implementation.py'
    implementation.write_text('# original source')
    output = tmp_path / 'window'
    output.mkdir()
    payload = output / 'observed_events.parquet'
    pl.DataFrame({'value': [1]}).write_parquet(payload)
    source = {'day': '2011-01-03', 'path': str(native), 'sha256': native_sha,
              'receipt_path': str(receipt), 'receipt_sha256': sha256_file(receipt)}
    doc = {'contract': CONTRACT, 'training_ready': False, 'plan_sha256': sha256_file(plan),
           'policy_sha256': 'policy-sha', 'source_pins': [source],
           'implementation_sha256': {str(implementation): sha256_file(implementation)},
           'data_path': str(payload), 'data_sha256': sha256_file(payload), 'rows': 1}
    (output / 'manifest.json').write_text(json.dumps(doc))
    window = {'source_receipt_pins': [{'date': '2011-01-03', 'parquet_sha256': native_sha,
                                      'receipt_sha256': sha256_file(receipt)}]}
    settings = {'policy_sha256': 'policy-sha'}
    assert verify(output, plan, settings, window, root)['rows'] == 1
    pl.DataFrame({'value': [2]}).write_parquet(native)
    with pytest.raises(ValueError, match='native source changed'):
        verify(output, plan, settings, window, root)
    pl.DataFrame({'value': [1]}).write_parquet(native)
    assert sha256_file(native) == native_sha
    pl.DataFrame({'value': [3]}).write_parquet(payload)
    with pytest.raises(ValueError, match='payload differs'):
        verify(output, plan, settings, window, root)
