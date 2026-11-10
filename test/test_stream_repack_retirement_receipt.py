import pytest

from scripts.retire_stream_repacked_release import packing_evidence
from stockagent.data_sync.desync_snapshots import SnapshotError


def test_retirement_receipt_distinguishes_unique_content_from_all_original_occurrences():
    standard = packing_evidence({})
    assert standard['old_source_member_hashes_checked_while_packing'] is True
    alias = packing_evidence({'packed_format': 'content-alias-v3',
                              'unique_content_sha_streamed': True, 'all_original_occurrences_reread': False})
    assert alias == {'old_source_member_hashes_checked_while_packing': False,
                     'unique_content_sha_streamed': True, 'all_original_occurrences_reread': False}


@pytest.mark.parametrize('receipt', [
    {'packed_format': 'content-alias-v3', 'all_original_occurrences_reread': False},
    {'packed_format': 'content-alias-v3', 'unique_content_sha_streamed': True,
     'all_original_occurrences_reread': True},
    {'packed_format': 'unknown'},
])
def test_retirement_rejects_an_unverified_or_unknown_hash_scope(receipt):
    with pytest.raises(SnapshotError):
        packing_evidence(receipt)
