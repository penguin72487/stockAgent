from __future__ import annotations

import json

from scripts.stage_tw_public_all_observed_release import (
    MODEL_CHANNELS,
    PUBLIC_VALUE_CHANNELS,
    SOURCE,
    SOURCE_RECEIPT,
    STOCK_VALUE_CHANNELS,
    _validated_contract,
)
from stockagent.data_sync.desync_snapshots import sha256_file


def test_all_observed_release_contract_matches_current_feature_table() -> None:
    receipt, metadata, source_hash, wide_hash = _validated_contract()
    assert source_hash == sha256_file(SOURCE)
    assert source_hash == receipt["output_sha256"]
    assert wide_hash == receipt["inputs"]["wide"]["sha256"]
    assert int(metadata.num_rows) == int(receipt["rows"])
    assert MODEL_CHANNELS == STOCK_VALUE_CHANNELS + 2 * PUBLIC_VALUE_CHANNELS == 428
    assert json.loads(SOURCE_RECEIPT.read_text())["research_only"] is True
