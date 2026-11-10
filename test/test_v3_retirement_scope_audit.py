import hashlib
import json

from scripts.audit_v3_retirement_scope import audit


def test_correction_preserves_the_original_receipt_and_is_idempotent(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack/fixture'
    root.mkdir(parents=True)
    receipt = {'contract': 'stockagent.source_local_exact_repack.v1', 'dataset': 'fixture',
        'old_manifest_sha256': 'a'*64, 'new_manifest_sha256': 'b'*64,
        'packed_format': 'content-alias-v3', 'unique_content_sha_streamed': True,
        'all_original_occurrences_reread': False, 'old_release_removed': True,
        'retirement': {'state': 'completed', 'old_source_member_hashes_checked_while_packing': True,
                       'observed_at_epoch': 123}}
    path = root/('b'*64+'.json')
    raw = json.dumps(receipt).encode()
    path.write_bytes(raw)
    preview = audit(tmp_path)
    assert len(preview) == 1 and not (root.parent/'v3-retirement-scope-corrections').exists()
    assert preview[0]['original_receipt_sha256'] == hashlib.sha256(raw).hexdigest()
    assert preview[0]['corrected_old_source_member_hashes_checked_while_packing'] is False
    assert audit(tmp_path, apply=True) == preview
    assert audit(tmp_path, apply=True) == preview
    assert path.read_bytes() == raw
    assert len(list((root.parent/'v3-retirement-scope-corrections').glob('*.json'))) == 1


def test_correct_v3_receipt_needs_no_correction(tmp_path):
    root = tmp_path/'migration/source-local-exact-repack/fixture'
    root.mkdir(parents=True)
    (root/('b'*64+'.json')).write_text(json.dumps({
        'contract': 'stockagent.source_local_exact_repack.v1', 'dataset': 'fixture',
        'old_manifest_sha256': 'a'*64, 'new_manifest_sha256': 'b'*64,
        'packed_format': 'content-alias-v3', 'unique_content_sha_streamed': True,
        'all_original_occurrences_reread': False, 'old_release_removed': True,
        'retirement': {'state': 'completed', 'old_source_member_hashes_checked_while_packing': False}}))
    assert audit(tmp_path, apply=True) == []
