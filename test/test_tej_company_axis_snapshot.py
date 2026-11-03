import struct

import pytest

from downloader.tej_company_axis_snapshot import MAGIC, iter_snapshot, verify_snapshot


def dotnet_string(value: str) -> bytes:
    data = value.encode('utf-8')
    n = len(data)
    prefix = bytearray()
    while n >= 128:
        prefix.append((n & 0x7f) | 0x80)
        n >>= 7
    return bytes(prefix) + bytes([n]) + data


def snapshot(labels):
    return dotnet_string(MAGIC) + struct.pack('<q', len(labels)) + b''.join(map(dotnet_string, labels))


def test_roundtrip_unicode_order_and_duplicates(tmp_path):
    labels = ['AAA200307C30==>南亞買權', '期📈_000001', '期📈_000001', 'x' * 200]
    path = tmp_path / 'snapshot.bin'
    path.write_bytes(snapshot(labels))
    assert list(iter_snapshot(path)) == labels
    proof = verify_snapshot(path)
    assert (proof['count'], proof['unique_exact_labels'], proof['duplicate_exact_labels']) == (4, 3, 1)
    assert proof['full_axis_admitted'] is False


@pytest.mark.parametrize('mutate', [
    lambda data: data[:-1],
    lambda data: data + b'x',
    lambda data: b'\x03bad' + data[1 + len(MAGIC):],
    lambda data: data[:1 + len(MAGIC)] + struct.pack('<q', 2_000_001) + data[1 + len(MAGIC) + 8:],
    lambda data: data[:1 + len(MAGIC) + 8] + b'\xff\xff\xff\xff\x7f',
    lambda data: data[:1 + len(MAGIC) + 8] + b'\x01\xff',
])
def test_rejects_changed_framing_and_invalid_bytes(tmp_path, mutate):
    path = tmp_path / 'snapshot.bin'
    path.write_bytes(mutate(snapshot(['label'])))
    with pytest.raises(ValueError):
        list(iter_snapshot(path))


def test_rejects_empty_and_overlong_labels(tmp_path):
    path = tmp_path / 'snapshot.bin'
    for label in ('', '📈' * 2049):
        path.write_bytes(snapshot([label]))
        with pytest.raises(ValueError):
            list(iter_snapshot(path))
