"""The bounded catalog hash must retain the exact existing byte contract."""
import math
import json
from types import SimpleNamespace

import pytest

pytest.importorskip('temporalio', reason='benchmark admission uses the lakehouse control role')
from scripts.benchmark_backup_catalog_identity import (
    catalog_identity, content_identity_input, signature, validate_copy_receipt,
)
from stockagent.runtime_identity import identity_sha256


@pytest.mark.parametrize('content', [
    {}, {'files': [], 'releases': []},
    {'z': None, 'a': [True, False, -0.0, 1e-20, 1000000000000000000000]},
    {'quoted"key': [{'中': '中文\n"\\', 'b': {'y': [1, 2], 'a': None}}], 'scope': 'raw'},
    {'records': ({'raw': 'x'*1000000},), 'flag': True},
    {'nested': {'raw': [3, 2, 1]}, 'empty': ()},
])
def test_exact_original_catalog_identity(content):
    assert catalog_identity(content) == identity_sha256(content)


@pytest.mark.parametrize('content', [{'bad': [math.nan]}, {'bad': {'number': math.inf}}])
def test_nonfinite_values_still_rejected(content):
    with pytest.raises(ValueError):
        catalog_identity(content)


def test_original_scope_drops_only_observation_clock_and_filesystem_signatures():
    original = {'files': [{'relative': 'a', 'sha256': 'sha', 'signature': [1, 2],
                           'captured_bytes_utf8': '原始值'}],
                'releases': [{'required_file_keys': ['a@sha']}],
                'identity_sha256': 'existing', 'observed_at_utc': 'actual source clock',
                'other': 'preserved'}
    content = content_identity_input(original)
    assert content == {'files': [{'relative': 'a', 'sha256': 'sha', 'captured_bytes_utf8': '原始值'}],
                       'releases': original['releases'], 'other': 'preserved'}
    assert original['files'][0]['signature'] == [1, 2]


def test_catalog_schema_not_silently_coerced():
    with pytest.raises(ValueError, match='string field'):
        catalog_identity({1: 'not the catalog schema'})


def test_read_access_time_does_not_invalidate_source_signature():
    class Source:
        access = 1
        def stat(self):
            self.access += 1
            return SimpleNamespace(st_dev=1, st_ino=2, st_size=3, st_mtime_ns=4,
                                   st_ctime_ns=5, st_atime_ns=self.access)
    source = Source()
    assert signature(source) == signature(source) == (1, 2, 3, 4, 5)


@pytest.mark.parametrize('change', ['state', 'source_unchanged', 'source_sha256', 'copied_sha256', 'bytes'])
def test_catalog_probe_refuses_unverified_copy(tmp_path, change):
    catalog = tmp_path / 'catalog.json'
    catalog.write_bytes(b'original')
    proof = {'state': 'accepted_private_fixed_catalog_copy', 'source_unchanged': True,
             'source_sha256': 'original_sha', 'copied_sha256': 'original_sha', 'bytes': 8}
    path = tmp_path / 'copy-receipt.json'
    path.write_text(json.dumps(proof))
    validate_copy_receipt(catalog, 'original_sha')
    path.write_text(json.dumps({**proof, change: 'unverified'}))
    with pytest.raises(ValueError, match='verified genuine'):
        validate_copy_receipt(catalog, 'original_sha')
