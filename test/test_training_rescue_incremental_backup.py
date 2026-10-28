"""Fixed-base incremental transport keeps full independent restore verification."""
import copy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil

import pytest

from scripts.backup_delivery_receipt import ACK, DISPATCH, signed
from scripts.inventory_training_rescue import freeze_inventory
from scripts.verify_training_rescue import verify_restored_plan
from stockagent.training_rescue_backup import (
    deliveries, descriptor, incremental_bytes, incremental_plan, json_bytes, validate_incremental,
)
from stockagent.training_rescue_worker import digest
from test_training_rescue import formal, rescue, enroll_fixture

LIMITS = {"maximum_batch_bytes": 8 * 1024**3, "maximum_batch_files": 10000}
PAIRED = {"producer_device_id": "source-test", "receiver_device_id": "receiver-test",
          "repository_id": "a" * 64}


def test_envelope_encoding_matches_original_private_backup_writer_exactly(tmp_path):
    from stockagent.data_sync.offhost_backup import private_json
    value = {'contract': 'original', '檔案': [{'relative': 'objects/a', 'bytes': 7, 'sha256': 'a' * 64}]}
    target = tmp_path / 'original.json'
    private_json(target, value)
    assert json_bytes(value) == target.read_bytes()


def acknowledgements(parts, plan):
    result = {}
    for part in parts:
        raw = json_bytes(part['envelope'])
        dispatch = signed({"contract": DISPATCH, **PAIRED,
            "envelope_identity_sha256": part['envelope_identity_sha256'],
            "envelope_file_sha256": hashlib.sha256(raw).hexdigest(),
            "batch_relative": part['batch_relative'],
            "complete_files": len(part['files']) + 2,
            "complete_bytes": sum(row['bytes'] for row in part['files']) + len(raw) + 65,
            "catalog_identity_sha256": plan['identity_sha256'],
            "delivery_kind": "private_vast_recovery_backup", "complete_release_in_this_delivery": False})
        ack = signed({"contract": ACK, **{key: dispatch[key] for key in (*PAIRED,
            'envelope_identity_sha256', 'envelope_file_sha256', 'complete_files', 'complete_bytes')},
            "dispatch_identity_sha256": dispatch['identity_sha256'], "snapshot_id": "b" * 64,
            "repository_check_mode": "structural_plus_fixed_snapshot_full_restore",
            "repository_check_verified": True, "independent_restore_verified": True,
            "source_verifier_verified": True, "all_files_sha256_verified": True,
            "command_exit_codes": [0, 0, 0], "complete_workflow_seconds": 1.0,
            "accepted_at_utc": datetime.now(timezone.utc).isoformat(),
            "private_evidence_sha256": "c" * 64,
            "scope": "this_closed_delivery_off_host_file_backup_only",
            "canonical_reconstruction_verified": False, "control_database_restore_verified": False,
            "independent_key_custody_verified": False})
        result[part['envelope_identity_sha256']] = {"dispatch": dispatch, "acceptance": ack}
    return result


@pytest.fixture
def incremental_case(rescue, formal, monkeypatch, tmp_path):
    base_spec = formal[0]
    enroll_fixture(rescue, base_spec, monkeypatch)
    rescue.capture(base_spec['run_id'])
    base = freeze_inventory(rescue)
    base_raw = json_bytes(base)
    before = rescue.journal(base_spec['run_id'])
    changed = copy.deepcopy(before)
    changed['newer_observation_retained'] = True
    rescue.record(base_spec['run_id'], changed)
    current = freeze_inventory(rescue)
    raw = json_bytes(current)
    parts = deliveries(base, base_raw, **LIMITS)
    proofs = acknowledgements(parts, base)
    delta = incremental_plan(current, raw, base, base_raw, proofs, paired_identity=PAIRED, **LIMITS)
    return rescue, base, base_raw, current, raw, delta, proofs


def restore_parts(case, tmp_path):
    owner, base, base_raw, current, raw, delta, _ = case
    old = deliveries(base, base_raw, **LIMITS)
    new = deliveries(current, raw, incremental=delta, base_raw=base_raw, **LIMITS)
    roots = []
    for label, parts in [('delta', new), ('base', old)]:
        group = []
        for part in parts:
            root = tmp_path / f"{label}-{part['ordinal']}"
            root.mkdir()
            for row in part['members']:
                target = root / row['relative']
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(owner.cold / row['relative'], target)
            prefix = 'private-rescue/' + (current if label == 'delta' else base)['identity_sha256'] + '/'
            for name, content in part['control_bytes'].items():
                (root / name.removeprefix(prefix)).write_bytes(content)
            group.append(root)
        roots.append(group)
    return roots


def verify_parts(case, roots):
    _, _, _, current, _, delta, _ = case
    new, old = roots
    return verify_restored_plan(new[0], new[0] / 'recovery-plan.private.json', current['identity_sha256'],
        additional_roots=new[1:], base_roots=old,
        base_plan_path=new[0] / 'base-recovery-plan.private.json',
        incremental_path=new[0] / 'incremental-backup.private.json', incremental_id=delta['identity_sha256'])


def test_incremental_transport_sends_only_new_bytes_but_reads_every_base_original(incremental_case, tmp_path, monkeypatch):
    from scripts import verify_training_rescue as verifier
    owner, base, base_raw, current, raw, delta, _ = incremental_case
    before = {p: p.read_bytes() for p in (owner.state / 'runs').glob('*/state.json')}
    assert 0 < delta['new_files'] < current['files']
    assert delta['reused_files'] == base['files']
    assert delta['new_bytes'] + delta['reused_bytes'] == current['source_bytes']
    parts = deliveries(current, raw, incremental=delta, base_raw=base_raw, **LIMITS)
    assert sum(row['bytes'] for part in parts for row in part['members']) == delta['new_bytes']
    roots = restore_parts(incremental_case, tmp_path)
    reads = []
    actual = verifier._restored_member_sha
    def read(path):
        reads.append(path)
        return actual(path)
    monkeypatch.setattr(verifier, '_restored_member_sha', read)
    proof = verify_parts(incremental_case, roots)
    assert proof['contract'].endswith('.v3')
    assert proof['exact_restored_set_and_full_SHA_verified']
    assert len(reads) == len(set(reads)) == current['files']
    assert proof['bytes_verified'] == current['source_bytes']
    assert proof['incremental_backup']['reused_bytes_fully_reread'] == base['source_bytes']
    assert not proof['NAS_backup_verified'] and not proof['real_new_vast_resume_verified']
    assert all(path.read_bytes() == content for path, content in before.items())


def test_persisted_proof_key_order_never_changes_incremental_bytes_or_envelope(incremental_case, tmp_path):
    owner, base, base_raw, current, raw, delta, proofs = incremental_case
    path = tmp_path / 'persisted-control.json'
    # Real atomic journals sort nested dictionaries; receipt order may differ.
    from stockagent.training_rescue_worker import atomic_json
    atomic_json(path, delta)
    restored = json.loads(path.read_bytes())
    assert restored == delta and json_bytes(restored) != json_bytes(delta)
    assert incremental_bytes(restored) == incremental_bytes(delta)
    original = deliveries(current, raw, incremental=delta, base_raw=base_raw, **LIMITS)
    repeated = deliveries(current, raw, incremental=restored, base_raw=base_raw, **LIMITS)
    assert original == repeated
    case = (owner, base, base_raw, current, raw, restored, proofs)
    result = verify_parts(case, restore_parts(case, tmp_path))
    assert result['exact_restored_set_and_full_SHA_verified']
    assert result['files_verified'] == current['files']


@pytest.mark.parametrize('damage', ['missing-ack', 'wrong-pair', 'wrong-envelope', 'wrong-complete-count', 'missing-part'])
def test_bad_baseline_ack_cannot_suppress_any_source_payload(incremental_case, damage):
    _, base, base_raw, current, raw, _, proofs = incremental_case
    values = copy.deepcopy(proofs)
    key = next(iter(values))
    if damage == 'missing-ack':
        values[key]['acceptance'] = None
    elif damage == 'missing-part':
        values.pop(key)
    else:
        dispatch = values[key]['dispatch']
        if damage == 'wrong-pair':
            dispatch['receiver_device_id'] = 'other-receiver'
        elif damage == 'wrong-envelope':
            dispatch['envelope_file_sha256'] = 'f' * 64
        else:
            dispatch['complete_files'] += 1
        dispatch = signed({k: v for k, v in dispatch.items() if k != 'identity_sha256'})
        values[key]['dispatch'] = dispatch
        ack = values[key]['acceptance']
        ack.update({k: dispatch[k] for k in ('receiver_device_id', 'envelope_file_sha256', 'complete_files')})
        ack['dispatch_identity_sha256'] = dispatch['identity_sha256']
        values[key]['acceptance'] = signed({k: v for k, v in ack.items() if k != 'identity_sha256'})
    with pytest.raises(ValueError):
        incremental_plan(current, raw, base, base_raw, values, paired_identity=PAIRED, **LIMITS)


@pytest.mark.parametrize('damage', ['removed', 'changed-bytes', 'changed-sha'])
def test_private_base_is_immutable_and_append_only(incremental_case, damage):
    _, base, base_raw, current, _, _, proofs = incremental_case
    candidate = copy.deepcopy(current)
    row = candidate['members'][0]
    if damage == 'removed':
        candidate['members'].remove(row)
    elif damage == 'changed-bytes':
        row['bytes'] += 1
    else:
        row['sha256'] = 'f' * 64
    candidate.update(files=len(candidate['members']), source_bytes=sum(r['bytes'] for r in candidate['members']))
    candidate['identity_sha256'] = digest({k: v for k, v in candidate.items() if k != 'identity_sha256'})
    with pytest.raises(ValueError):
        incremental_plan(candidate, json_bytes(candidate), base, base_raw, proofs,
                         paired_identity=PAIRED, **LIMITS)


@pytest.mark.parametrize('damage', ['corrupt-base', 'corrupt-new', 'extra-member', 'missing-base',
                                   'missing-control', 'duplicate', 'wrong-root', 'symlink'])
def test_restore_never_uses_historical_ack_as_a_replacement_for_full_union(incremental_case, tmp_path, damage):
    roots = restore_parts(incremental_case, tmp_path)
    new, old = roots
    base_member = next(p for p in old[0].rglob('*') if p.is_file() and p.name != 'recovery-plan.private.json')
    delta_names = {r['relative'] for r in incremental_case[5]['new_members']}
    delta_member = next(new[0] / name for name in delta_names)
    if damage == 'corrupt-base':
        content = base_member.read_bytes()
        base_member.write_bytes(bytes([content[0] ^ 1]) + content[1:])
    elif damage == 'corrupt-new':
        delta_member.write_bytes(delta_member.read_bytes() + b'corrupt')
    elif damage == 'extra-member':
        (old[0] / 'unknown').write_bytes(b'preserve')
    elif damage == 'missing-base':
        roots[1].clear()
    elif damage == 'missing-control':
        (new[0] / 'incremental-backup.private.json').unlink()
    elif damage in {'duplicate', 'wrong-root'}:
        target = new[0] / base_member.relative_to(old[0])
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(base_member, target)
        if damage == 'wrong-root':
            base_member.unlink()
    else:
        base_member.unlink()
        base_member.symlink_to(delta_member)
    with pytest.raises((ValueError, FileNotFoundError)):
        verify_parts(incremental_case, roots)


@pytest.mark.parametrize('field', ['reused_files', 'reused_bytes', 'identity_sha256', 'new_GPU_resume_verified'])
def test_incremental_claims_and_pinned_identity_are_exact(incremental_case, field):
    _, base, base_raw, current, raw, delta, _ = incremental_case
    changed = copy.deepcopy(delta)
    changed[field] = True if field == 'new_GPU_resume_verified' else 'changed'
    with pytest.raises(ValueError, match='differs'):
        validate_incremental(current, raw, base, base_raw, changed)
