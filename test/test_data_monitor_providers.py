from __future__ import annotations

from stockagent.live.data_monitor_providers import project_provider_detail


def test_provider_detail_keeps_aliases_out_of_active_ratio() -> None:
    snapshot = {
        "generated_at_utc": "2026-09-26T01:00:00Z",
        "provider_summaries": [{"provider": "FinLab", "status": "partial"}],
        "finlab_acquisition": {"downloaded": 7, "catalog_total": 10, "quota_limit_mb": 5000,
                               "secret": "do-not-expose"},
        "sources": [
            {"id": "finlab:one", "provider": "FinLab", "title": "一", "registry_alias": True,
             "in_active_scope": False, "operation_state": "reference",
             "record_stats": {"state": "unverified", "count": None, "private_path": "/secret"}},
            {"id": "finlab:two", "provider": "FinLab", "title": "二", "registry_alias": True,
             "in_active_scope": False, "operation_state": "reference"},
            {"id": "official:one", "provider": "TWSE", "title": "官方",
             "in_active_scope": True, "operation_state": "complete"},
        ],
    }
    detail = project_provider_detail(snapshot, "FinLab")
    assert detail is not None
    assert detail["summary"]["registered"] == 2
    assert detail["summary"]["registry_aliases"] == 2
    assert detail["summary"]["active_endpoints"] == 0
    assert detail["summary"]["active_current_ratio"] is None
    assert detail["finlab_acquisition"]["downloaded"] == 7
    assert "secret" not in str(detail) and "private_path" not in str(detail)
    assert project_provider_detail(snapshot, "不存在") is None


def test_provider_detail_ratio_uses_only_active_endpoints() -> None:
    payload = {"sources": [
        {"id": "a", "provider": "TWSE", "in_active_scope": True,
         "operation_state": "complete", "record_stats": {"state": "verified"}},
        {"id": "b", "provider": "TWSE", "in_active_scope": True,
         "operation_state": "catching_up", "record_stats": {"state": "empty"}},
        {"id": "c", "provider": "TWSE", "in_active_scope": False,
         "operation_state": "reference", "record_stats": {"state": "unverified"}},
    ]}
    detail = project_provider_detail(payload, "TWSE")
    assert detail is not None
    assert detail["summary"]["active_endpoints"] == 2
    assert detail["summary"]["active_current_ratio"] == 0.5
    assert detail["summary"]["inventory_state_counts"] == {
        "verified": 1, "empty": 1, "unverified": 1,
    }
    second = project_provider_detail(payload, "TWSE", offset=1, limit=1)
    assert second is not None
    assert second["page"] == {"offset": 1, "limit": 1, "matched_total": 3, "has_more": True}
    assert len(second["sources"]) == 1
    searched = project_provider_detail(payload, "TWSE", search="b")
    assert searched is not None
    assert [row["id"] for row in searched["sources"]] == ["b"]
