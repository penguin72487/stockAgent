import hashlib
import json
import struct

import pytest

from downloader.tej_company_axis_snapshot import MAGIC
from scripts import audit_tej_large_axis_capture as mod


def encode(value):
    raw = value.encode('utf-8')
    size = len(raw)
    prefix = bytearray()
    while size >= 128:
        prefix.append((size & 127) | 128)
        size >>= 7
    return bytes(prefix) + bytes([size]) + raw


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def capture(tmp_path, monkeypatch):
    root = tmp_path / 'owner'
    for directory in ('raw', 'requests', 'large_axis_capture_intents/claimed'):
        (root / directory).mkdir(parents=True)
    task = 'a' * 24
    attempt = task + '-' + 'b' * 32
    scope = {'type': 'Option (ALL)', 'smart_id': 'TEJ Derivatives', 'table': 'Example'}
    request = root / 'requests' / (attempt + '.json')
    request.write_text(json.dumps({'action': 'plan', 'task_id': task, **scope}))
    sample = root / 'raw' / (task + '-' + 'c' * 32 + '.json.company-axis-sample.json')
    sample.write_text(json.dumps({'task_id': task, 'full_axis_admitted': False,
                                  'prepared_request_sha256': digest(request), 'available_count': 150001}))
    claim = root / 'large_axis_capture_intents/claimed' / (attempt + '.json')
    claim.write_text(json.dumps({'contract': 'tej_large_company_axis_capture_once_v1',
                                 'task_id': task, 'market_data_query_authorized': False,
                                 'prepared_request_sha256': digest(request), 'bridge_script_sha256': 'd' * 64,
                                 'priority_request_id': 'e' * 64, 'prior_sample_basename': sample.name,
                                 'prior_sample_sha256': digest(sample), 'expected_count': 150001, **scope}))
    binary = root / 'raw' / (attempt + '.json.company-axis.snapshot.bin')
    binary.write_bytes(encode(MAGIC) + struct.pack('<q', 150001)
                       + b''.join(encode(f'期📈_{i:06d}') for i in range(150001)))
    scope_proof = {'binding_readback_verified': True, 'vendor_notices_absent': True}
    manifest = root / 'raw' / (attempt + '.json.company-axis.snapshot.json')
    manifest.write_text(json.dumps({'contract': 'tej_large_company_axis_two_pass_observation_v1',
        'task_id': task, 'query_attempt_id': attempt, 'binary_name': binary.name,
        'market_data_query_submitted': False, 'full_axis_admitted': False, 'complete_ordered_live_reads': 2,
        'before_scope': scope_proof, 'after_scope': scope_proof, 'prepared_request_sha256': digest(request),
        'bridge_script_sha256': 'd' * 64, 'intent_sha256': digest(claim), 'priority_request_id': 'e' * 64,
        'prior_sample_sha256': digest(sample), 'binary_sha256': digest(binary),
        'binary_bytes': binary.stat().st_size, 'labels': 150001, **scope}))
    monkeypatch.setattr(mod.DesktopBridge, 'bound_script_release', lambda *args: (None, 'd' * 64))
    return root, manifest, binary


def test_independently_reads_every_label_but_never_admits_plan(capture):
    root, manifest, _ = capture
    result = mod.audit(root, manifest)
    assert result['source_proof']['count'] == 150001
    assert result['source_proof']['duplicate_exact_labels'] == 0
    assert result['full_axis_admitted_to_plan'] is False
    assert result['market_observations_obtained'] is False


def test_rejects_tampering_and_changed_source(capture):
    root, manifest, binary = capture
    data = bytearray(binary.read_bytes())
    data[-1] ^= 1
    binary.write_bytes(data)
    with pytest.raises(ValueError, match='hash'):
        mod.audit(root, manifest)
    body = json.loads(manifest.read_text())
    body['full_axis_admitted'] = True
    manifest.write_text(json.dumps(body))
    with pytest.raises(ValueError, match='binding'):
        mod.audit(root, manifest)
