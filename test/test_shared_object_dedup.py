import copy
import json
import os
from pathlib import Path

import pytest

from stockagent.data_sync import packed_snapshots as packed, semantic_link_promotion as promotion
from stockagent.data_sync import shared_object_dedup as dedup, stream_repack
from stockagent.data_sync.desync_snapshots import SnapshotError, atomic_write_json
from test_semantic_link_promotion import latest_with_shared_object  # noqa: F401


def setup_shared(candidate):
    cold, private, old, new, options = candidate
    result = promotion.promote(cold, private, **options)
    result['old_sources_deleted'] = True
    atomic_write_json(options['journal_path'], result)
    install_path = options['journal_path'].parent/'install.json'
    atomic_write_json(install_path, options['install'])
    journal = options['journal_path'].parent/'share.json'
    kwargs = {'promotion_path': options['journal_path'], 'install_path': install_path,
              'journal_path': journal, 'plan': options['plan'], 'policy': options['latest_policy'],
              'process_check': lambda _: []}
    return cold, private, new, result, kwargs


def test_shared_objects_are_one_inode_and_both_original_receipts_remain_in_journal(latest_with_shared_object):
    cold, private, new, result, kwargs = setup_shared(latest_with_shared_object)
    duplicate, = [row for row in result['linked'] if row.get('mode') == 'existing_public_same_sha']
    descriptor, = [row for row in new.manifest['archive']['objects'] if row['sha256'] == duplicate['sha256']]
    target = cold/descriptor['relpath']
    assert not os.path.samefile(target, private/descriptor['relpath'])
    preview = dedup.consolidate(cold, private, **kwargs)
    assert preview['state'] == 'planned' and not kwargs['journal_path'].exists()
    value = dedup.consolidate(cold, private, **kwargs, apply=True)
    assert value['state'] == 'completed' and os.path.samefile(target, private/descriptor['relpath'])
    assert value['plan']['promotion_before'] == result
    assert value['plan']['install_before'] == latest_with_shared_object[-1]['install']
    assert value['independent_D_readback'] is False and value['NAS_accepted'] is False
    updated_promotion = json.loads(kwargs['promotion_path'].read_text())
    updated_install = json.loads(kwargs['install_path'].read_text())
    assert updated_promotion['duplicate_encoded_bytes_pending'] == 0
    for row in updated_install['installed']:
        ref, = [ref for ref in [new.manifest['archive']['inventory'], *new.manifest['archive']['objects']]
                if ref['sha256'] == row['sha256']]
        assert stream_repack._signature_matches(private/ref['relpath'], row['D_signature'])
    assert dedup.consolidate(cold, private, **kwargs, apply=True)['state'] == 'completed'
    assert packed.resolve_latest_packed(cold, 'okx').manifest_sha256 == result['new_manifest_sha256']


@pytest.mark.parametrize('failure', ['after_link', 'after_replace', 'after_install', 'after_promotion'])
def test_shared_object_transaction_resumes_at_each_write_boundary(latest_with_shared_object, monkeypatch, failure):
    cold, private, _new, _result, kwargs = setup_shared(latest_with_shared_object)
    if failure in {'after_link', 'after_replace'}:
        original = getattr(dedup.os, 'link' if failure == 'after_link' else 'replace')
        def crash(*args, **options):
            result = original(*args, **options)
            if failure == 'after_link' or str(args[0]).find('.share-') >= 0:
                raise OSError('simulated interruption')
            return result
        monkeypatch.setattr(dedup.os, 'link' if failure == 'after_link' else 'replace', crash)
    else:
        original = dedup.atomic_write_json
        def crash(path, value, **options):
            result = original(path, value, **options)
            if path == kwargs['install_path' if failure == 'after_install' else 'promotion_path']:
                raise OSError('simulated interruption')
            return result
        monkeypatch.setattr(dedup, 'atomic_write_json', crash)
    with pytest.raises(OSError, match='simulated interruption'):
        dedup.consolidate(cold, private, **kwargs, apply=True)
    if failure in {'after_link', 'after_replace'}:
        monkeypatch.setattr(dedup.os, 'link' if failure == 'after_link' else 'replace', original)
    else:
        monkeypatch.setattr(dedup, 'atomic_write_json', original)
    assert dedup.consolidate(cold, private, **kwargs, apply=True)['state'] == 'completed'


def test_shared_object_transaction_never_accepts_a_changed_install_receipt(latest_with_shared_object):
    cold, private, _new, result, kwargs = setup_shared(latest_with_shared_object)
    changed = copy.deepcopy(json.loads(kwargs['install_path'].read_text()))
    changed['new_manifest_sha256'] = '0'*64
    atomic_write_json(kwargs['install_path'], changed)
    with pytest.raises(SnapshotError, match='installation differs'):
        dedup.consolidate(cold, private, **kwargs, apply=True)
    assert json.loads(kwargs['promotion_path'].read_text()) == result


def test_shared_object_retry_checks_receipts_before_replacing_the_public_name(latest_with_shared_object, monkeypatch):
    cold, private, new, result, kwargs = setup_shared(latest_with_shared_object)
    duplicate, = [row for row in result['linked'] if row.get('mode') == 'existing_public_same_sha']
    descriptor, = [row for row in new.manifest['archive']['objects'] if row['sha256'] == duplicate['sha256']]
    original = dedup.os.link
    def crash(*args, **options):
        original(*args, **options)
        raise OSError('simulated interruption')
    monkeypatch.setattr(dedup.os, 'link', crash)
    with pytest.raises(OSError, match='simulated interruption'):
        dedup.consolidate(cold, private, **kwargs, apply=True)
    monkeypatch.setattr(dedup.os, 'link', original)
    changed = json.loads(kwargs['install_path'].read_text())
    changed['unexpected_other_owner_change'] = True
    atomic_write_json(kwargs['install_path'], changed)
    with pytest.raises(SnapshotError, match='receipts changed'):
        dedup.consolidate(cold, private, **kwargs, apply=True)
    assert not os.path.samefile(cold/descriptor['relpath'], private/descriptor['relpath'])
