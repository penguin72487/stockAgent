"""Freeze a portable recovery cohort without holding up checkpoint capture."""
import copy
import json
from pathlib import Path
import shutil

import pytest

from scripts.inventory_training_rescue import freeze_inventory
from scripts.verify_training_rescue import verify_restored_plan
from stockagent.training_rescue_worker import atomic_json, digest
from test_training_rescue import formal, rescue, enroll_fixture


@pytest.fixture
def case(rescue, formal, monkeypatch):
    base = formal[0]
    enroll_fixture(rescue, base, monkeypatch)
    rescue.capture(base['run_id'])
    return rescue, base


def restored_copy(owner, plan, tmp_path):
    destination = tmp_path / 'isolated-restore'
    for row in plan['members']:
        target = destination / row['relative']
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(owner.cold / row['relative'], target)
    path = destination / 'recovery-plan.private.json'
    path.write_text(json.dumps(plan))
    return verify_restored_plan(destination, path, plan['identity_sha256'])


def test_frozen_cohort_passes_independent_offline_verifier_without_source_contact(case, monkeypatch, tmp_path):
    owner, base = case
    before = owner.journal(base['run_id'])
    monkeypatch.setattr(owner, 'source', lambda *_: pytest.fail('inventory must not contact the trainer'))
    plan = freeze_inventory(owner)
    assert plan['contract'].endswith('_v2')
    assert owner.journal(base['run_id']) == before
    proof = restored_copy(owner, plan, tmp_path)
    assert proof['exact_restored_set_and_full_SHA_verified']
    assert proof['runs'][0]['commit_id'] == before['latest']['commit_id']
    assert not plan['full_payload_SHA_verified_by_census']
    assert not plan['NAS_backup_verified'] and plan['source_files_deleted'] == 0


def test_recipe_and_preserved_resource_must_both_be_present(case, tmp_path):
    owner, base = case
    from stockagent.training_rescue_worker import PREPARED_BUILD_POLICY
    candidate = copy.deepcopy(base)
    candidate.update(prepared_build_policy=PREPARED_BUILD_POLICY,
        data_sources=[{'dataset': 'fixture-originals', 'snapshot_id': 'fixed-originals',
                       'manifest_sha256': 'a' * 64, 'materialized_root': str(tmp_path / 'originals')}])
    revision = owner.register_input_recipe(candidate)
    plan = freeze_inventory(owner)
    controls = plan['runs'][0]['controls']
    assert any(row['relative'].endswith('/input-recipe.json') for row in controls)
    assert any(row['relative'] == f"input-recipes/{base['run_id']}/{revision['input_recipe_revision_sha256']}.json"
               for row in plan['members'])
    proof = restored_copy(owner, plan, tmp_path)
    assert proof['runs'][0]['recovery_spec_sha256'] == digest(owner.recovery_spec(base['run_id']))
    assert not proof['runs'][0]['recorded_source_proof_matches_recovery_spec']


def test_checkpoint_owner_can_advance_after_controls_freeze(case, monkeypatch, tmp_path):
    owner, base = case
    before = owner.journal(base['run_id'])
    actual = owner.put_stream
    advanced = []

    def preserve(stream, row, **options):
        if not advanced:
            advanced.append(True)
            # An actual owner acquisition while preserving a frozen control
            # proves that slow D writes no longer hold the global capture lock.
            with owner.lock():
                current = owner.journal(base['run_id'])
                current['newer_capture_observation'] = 'kept'
                owner.record(base['run_id'], current)
        return actual(stream, row, **options)

    monkeypatch.setattr(owner, 'put_stream', preserve)
    plan = freeze_inventory(owner)
    assert advanced and owner.journal(base['run_id'])['newer_capture_observation'] == 'kept'
    assert plan['runs'][0]['latest'] == before['latest']
    assert restored_copy(owner, plan, tmp_path)['exact_restored_set_and_full_SHA_verified']


@pytest.mark.parametrize('damage', ['missing-object', 'wrong-recipe', 'symlink', 'unknown-object'])
def test_incomplete_or_redirected_cohort_is_rejected_and_originals_preserved(case, damage):
    owner, base = case
    if damage == 'missing-object':
        state = owner.journal(base['run_id'])
        commit = json.loads((owner.cold / 'commits' / base['run_id'] / (state['latest']['commit_id'] + '.json')).read_text())
        (owner.object_path(commit['inventory']['files'][0]['sha256'])).unlink()
    elif damage == 'wrong-recipe':
        atomic_json(owner.path(base['run_id']) / 'input-recipe.json', {
            'contract': 'stockagent.training_input_recipe_pointer.v1',
            'base_spec_sha256': 'f' * 64, 'revision_sha256': 'a' * 64})
    elif damage == 'symlink':
        (owner.cold / 'objects' / 'ff').symlink_to(owner.cold / 'commits', target_is_directory=True)
    else:
        (owner.cold / 'objects' / 'ff').mkdir()
        (owner.cold / 'objects' / 'ff' / 'unknown').write_bytes(b'preserve diagnostic')
    enrolled = (owner.path(base['run_id']) / 'spec.json').read_bytes()
    with pytest.raises((ValueError, OSError)):
        freeze_inventory(owner)
    assert (owner.path(base['run_id']) / 'spec.json').read_bytes() == enrolled


def test_uncommitted_partial_is_named_protected_and_never_advertised(case):
    owner, _ = case
    folder = owner.cold / 'objects' / 'ee'
    folder.mkdir(exist_ok=True)
    partial = folder / ('e' * 64 + '.partial.' + 'a' * 32)
    partial.write_bytes(b'failed original transfer')
    plan = freeze_inventory(owner)
    assert partial.read_bytes() == b'failed original transfer'
    name = partial.relative_to(owner.cold).as_posix()
    assert name in {row['relative'] for row in plan['excluded_original_partials']}
    assert name not in {row['relative'] for row in plan['members']}


def test_sha_only_original_reference_uses_its_bounded_real_CAS_length(case, monkeypatch):
    from scripts import inventory_training_rescue as inventory
    owner, _ = case
    row = owner.put_file(owner.path('formal') / 'spec.json')
    monkeypatch.setattr(inventory, 'preparation_resource_bindings', lambda _: [
        {'path': '/fixed/original/reference.json', 'sha256': row['sha256']}])
    plan = freeze_inventory(owner)
    assert any(member['sha256'] == row['sha256'] and member['bytes'] == row['bytes']
               for member in plan['members'])


def test_capacity_rejection_leaves_existing_history_and_journal(case, monkeypatch):
    from scripts import inventory_training_rescue as inventory
    owner, base = case
    before = owner.journal(base['run_id'])
    monkeypatch.setattr(inventory, 'MAX_BYTES', 1)
    with pytest.raises(ValueError, match='capacity'):
        freeze_inventory(owner)
    assert owner.journal(base['run_id']) == before
    assert (owner.cold / 'commits' / base['run_id'] / (before['latest']['commit_id'] + '.json')).is_file()


def test_canonical_cli_writes_a_private_plan_without_an_outer_owner_lock(case, monkeypatch, tmp_path):
    from scripts import training_rescue as cli
    from scripts import inventory_training_rescue as inventory
    import sys
    owner, _ = case
    output = tmp_path / 'current-plan.private.json'
    monkeypatch.setattr(cli, 'Rescue', lambda _: owner)
    original = inventory.write_inventory
    acquired = []

    def write(current, target, **options):
        with current.lock():
            acquired.append(True)
        return original(current, target, **options)

    monkeypatch.setattr(inventory, 'write_inventory', write)
    monkeypatch.setattr(sys, 'argv', ['training_rescue', 'inventory-backup', '--output', str(output)])
    assert cli.main() == 0 and acquired == [True]
    plan = json.loads(output.read_bytes())
    assert output.stat().st_mode & 0o777 == 0o600
    assert plan['contract'].endswith('_v2') and plan['identity_sha256'] == digest(
        {k: v for k, v in plan.items() if k != 'identity_sha256'})


@pytest.mark.parametrize('where', ['existing', 'source-state', 'cold'])
def test_inventory_output_cannot_overwrite_evidence_or_owner_state(case, monkeypatch, tmp_path, where):
    from scripts import inventory_training_rescue as inventory
    owner, _ = case
    output = (tmp_path / 'existing.json' if where == 'existing' else
              owner.state / 'new-plan.json' if where == 'source-state' else owner.cold / 'new-plan.json')
    if where == 'existing':
        output.write_bytes(b'original evidence')
    monkeypatch.setattr(inventory, 'freeze_inventory', lambda *_a, **_k:
                        pytest.fail('invalid output must fail before touching D'))
    with pytest.raises(ValueError):
        inventory.write_inventory(owner, output)
    if where == 'existing':
        assert output.read_bytes() == b'original evidence'
