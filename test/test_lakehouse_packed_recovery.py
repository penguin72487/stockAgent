import pytest

from stockagent.control.lakehouse_recovery import reconstruct_registered_release
from stockagent.data_sync.packed_snapshots import publish_packed_snapshot


class Registry:
    def __init__(self, rows):
        self.rows = rows
    def execute(self, sql, parameters):
        return self
    def fetchall(self):
        return self.rows


@pytest.fixture
def recovery(tmp_path):
    source = tmp_path / 'source'; source.mkdir()
    (source / '觀測.csv').write_bytes(b'price,volume\n123,45\n')
    (source / 'large.bin').write_bytes(bytes(range(256)) * 16)
    cold = tmp_path / 'original-packed'
    release = publish_packed_snapshot(cold, 'observations', source, loose_file_threshold_bytes=1024)
    raw = release.manifest_path.read_bytes()
    rows = [[release.manifest_path.relative_to(cold).as_posix(), release.manifest_sha256,
             release.manifest['source']['portable_fingerprint_sha256'], len(raw), raw.decode()]]
    options = {'dataset': 'observations', 'snapshot_id': release.manifest['snapshot_id'],
               'object_source': lambda item: cold / item['relpath']}
    return Registry(rows), tmp_path / 'fresh-recovery', options, cold, release


def test_restored_registry_reconstructs_original_values_with_canonical_verifier(recovery):
    registry, scratch, options, cold, release = recovery
    result = reconstruct_registered_release(registry, scratch, **options)
    assert result['canonical_verification']['materialized_verified'] is True
    assert result['source_fingerprint_sha256'] == release.manifest['source']['portable_fingerprint_sha256']
    assert result['source_files_deleted'] == 0 and release.manifest_path.exists()
    assert not list((scratch / 'packed/heads').rglob('*.json'))  # Exact release, no invented current head.


@pytest.mark.parametrize('damage', ['manifest', 'duplicate', 'object', 'source_fingerprint'])
def test_recovery_refuses_ambiguous_or_corrupted_evidence(recovery, damage):
    registry, scratch, options, cold, release = recovery
    if damage == 'manifest':
        registry.rows[0][4] += ' '
    elif damage == 'duplicate':
        registry.rows *= 2
    elif damage == 'object':
        item = release.manifest['archive']['objects'][0]
        (cold / item['relpath']).write_bytes(b'x' * item['bytes'])
    else:
        registry.rows[0][2] = '0' * 64
    with pytest.raises((ValueError, RuntimeError)):
        reconstruct_registered_release(registry, scratch, **options)
    assert release.manifest_path.exists()
