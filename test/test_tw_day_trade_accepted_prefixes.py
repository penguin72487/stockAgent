import hashlib

import pytest

from scripts.audit_tw_day_trade_accepted_prefixes import multiple_prefix_hashes
from stockagent.live.paper_journal import publish_snapshot


@pytest.mark.parametrize("managed", [False, True])
def test_multiple_accepted_prefixes_share_one_exact_scan(tmp_path, managed):
    path = tmp_path / "signals.jsonl"
    body = b"alpha\nbeta\ngamma\n"
    path.write_bytes(body)
    if managed:
        publish_snapshot(path, operation="seed")
    result = multiple_prefix_hashes(path, {0, 6, 11, len(body)})
    assert result == {size: hashlib.sha256(body[:size]).hexdigest() for size in (0, 6, 11, len(body))}


@pytest.mark.parametrize("lengths", [set(), {-1}, {999}, {True}])
def test_prefix_scan_refuses_invalid_or_truncated_input(tmp_path, lengths):
    path = tmp_path / "signals.jsonl"
    path.write_bytes(b"source\n")
    with pytest.raises((ValueError, RuntimeError)):
        multiple_prefix_hashes(path, lengths)
