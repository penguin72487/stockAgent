from __future__ import annotations

from copy import deepcopy

import pytest

from stockagent.live.data_monitor_api_contract import verify_feature_page
from stockagent.live.data_monitor_feature_pages import FeaturePageIndex


def _page():
    row = {"field": "Close", "dataset_id": "tw:stock", "source_title": "台股",
           "provider": "TWSE", "market_category": "taiwan_equity",
           "market_category_label": "台股", "unbounded_count": 2**128}
    index = FeaturePageIndex.from_payload({
        "schema_version": 1, "read_only": True, "production_control_possible": False,
        "generated_at_utc": None, "rows": [row], "summary": {"fields": 1},
    }, source_signature=(1, 2, 3, 4, 5))
    return index.page(offset=0, limit=80)


def test_canonical_page_keeps_generation_and_exact_metadata():
    page = _page()
    assert verify_feature_page(page) == page
    assert verify_feature_page(page)["rows"][0]["unbounded_count"] == 2**128


@pytest.mark.parametrize("changes", [
    {"read_only": False}, {"production_control_possible": True},
    {"offset": -1}, {"limit": 5001}, {"matching_total": -1},
    {"revision": "unbound-generation"}, {"reset_required": "false"},
    {"has_more": True}, {"rows": []}, {"matching_total": 2**53},
    {"offset": 1, "rows": [], "reset_required": True},
])
def test_page_contract_rejects_control_flags_and_unsafe_shapes(changes):
    page = deepcopy(_page())
    page.update(changes)
    with pytest.raises((ValueError, TypeError)):
        verify_feature_page(page)
