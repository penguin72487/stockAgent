from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import snapshot_data_refresh_services as snapshot_service
from stockagent.live.data_monitor_feature_pages import (
    FEATURE_SOURCE_PAGE_MAX_ROWS,
    FeaturePageIndex,
    feature_page_preview,
    feature_page_projections,
    page_from_source_rows,
    valid_feature_page_preview,
    valid_feature_source_pages,
)
from stockagent.live.data_monitor_feature_receipt import (
    feature_reuse_checksum,
    feature_reuse_receipt_path,
    feature_source_pages_path,
    feature_source_signature,
    trusted_feature_preview,
    trusted_feature_source_pages,
)


def _payload() -> dict:
    return {
        "schema_version": 1,
        "read_only": True,
        "production_control_possible": False,
        "generated_at_utc": "2026-09-26T00:00:00Z",
        "summary": {"fields": 3, "state": "complete"},
        "rows": [
            {"field": "Close", "dataset_id": "tw:stock", "source_title": "台股",
             "provider": "TWSE", "market_category": "taiwan_equity",
             "market_category_label": "台股"},
            {"field": "Volume", "dataset_id": "tw:stock", "source_title": "台股",
             "provider": "TWSE", "market_category": "taiwan_equity",
             "market_category_label": "台股"},
            {"field": "Close", "dataset_id": "crypto:minute", "source_title": "現貨",
             "provider": "OKX", "market_category": "crypto",
             "market_category_label": "加密貨幣"},
        ],
    }


def test_feature_page_preserves_population_filters_and_revision() -> None:
    index = FeaturePageIndex.from_payload(_payload(), source_signature=(1, 2, 3, 4, 5))
    first = index.page(offset=0, limit=1)
    assert first["matching_total"] == 3
    assert first["has_more"] is True
    assert first["summary"]["fields"] == 3
    assert len(first["rows"]) == 1
    assert [item["id"] for item in first["filters"]["sources"]] == [
        "tw:stock", "crypto:minute",
    ]
    second = index.page(offset=1, limit=2, requested_revision=index.revision)
    assert first["rows"] + second["rows"] == _payload()["rows"]
    assert second["has_more"] is False
    assert index.page(offset=0, limit=5, search="close")["matching_total"] == 2
    assert index.page(offset=0, limit=5, category="crypto")["rows"] == [_payload()["rows"][2]]
    assert index.page(offset=0, limit=5, source="missing")["matching_total"] == 0
    changed = index.page(offset=2, limit=1, requested_revision="0" * 32)
    assert changed["reset_required"] is True
    assert changed["offset"] == 0
    assert changed["rows"] == first["rows"]


def test_first_page_preview_matches_complete_index() -> None:
    payload = _payload()
    index = FeaturePageIndex.from_payload(payload, source_signature=(1, 2, 3, 4, 5))
    preview = feature_page_preview(payload)
    assert valid_feature_page_preview(preview, fields=3)
    assert preview["rows"] == index.page(offset=0, limit=80)["rows"]
    assert preview["filters"] == {
        "categories": list(index.categories), "sources": list(index.sources),
    }
    assert preview["summary"] == payload["summary"]
    assert not valid_feature_page_preview({**preview, "rows": preview["rows"][:1]}, fields=3)


def test_feature_page_index_keeps_full_order_for_combined_filters() -> None:
    payload = _payload()
    payload["rows"] = [
        {**row, "field": f"{row['field']}{position}"}
        for position in range(90)
        for row in payload["rows"]
    ]
    payload["summary"]["fields"] = len(payload["rows"])
    index = FeaturePageIndex.from_payload(payload, source_signature=(1, 2, 3, 4, 5))
    for source in ("all", "tw:stock", "crypto:minute", "missing"):
        for category in ("all", "taiwan_equity", "crypto", "missing"):
            for search in ("", "close", "volume7", "missing"):
                expected = [
                    row for row in payload["rows"]
                    if (source == "all" or row["dataset_id"] == source)
                    and (category == "all" or row["market_category"] == category)
                    and (not search or search.lower() in "\0".join(
                        str(row[key]).lower() for key in (
                            "field", "dataset_id", "source_title", "provider",
                            "market_category_label",
                        )
                    ))
                ]
                for offset in (0, 1, 50, len(expected)):
                    page = index.page(
                        offset=offset, limit=7, source=source,
                        category=category, search=search,
                    )
                    assert page["matching_total"] == len(expected)
                    assert page["rows"] == expected[offset:offset + 7]
                    assert page["has_more"] == (offset + len(page["rows"]) < len(expected))


def test_small_source_projection_matches_full_page_contract() -> None:
    payload = _payload()
    preview, pages = feature_page_projections(payload)
    assert valid_feature_source_pages(pages, fields=3)
    index = FeaturePageIndex.from_payload(payload, source_signature=(1, 2, 3, 4, 5))
    for source in pages:
        for search in ("", "close", "TWSE", "missing"):
            for category in ("all", "crypto", "taiwan_equity", "missing"):
                for offset in (0, 1, 3):
                    for requested_revision in (None, index.revision, "0" * 32):
                        projected = page_from_source_rows(
                            rows=pages[source], preview=preview,
                            revision=index.revision,
                            requested_revision=requested_revision,
                            offset=offset, limit=1, search=search, category=category,
                        )
                        complete = index.page(
                            offset=offset, limit=1, search=search,
                            category=category, source=source,
                            requested_revision=requested_revision,
                        )
                        assert json.loads(json.dumps(projected)) == json.loads(json.dumps(complete))


def test_small_source_projection_excludes_large_source_without_losing_filters() -> None:
    payload = _payload()
    row = payload["rows"][0]
    payload["rows"] = [
        {**row, "field": f"field_{i}"}
        for i in range(FEATURE_SOURCE_PAGE_MAX_ROWS + 1)
    ] + payload["rows"][2:]
    payload["summary"]["fields"] = len(payload["rows"])
    preview, pages = feature_page_projections(payload)
    assert "tw:stock" not in pages
    assert "crypto:minute" in pages
    assert valid_feature_source_pages(pages, fields=len(payload["rows"]))
    assert {item["id"] for item in preview["filters"]["sources"]} == {
        "tw:stock", "crypto:minute",
    }


def test_oversized_optional_source_pages_keep_full_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = tmp_path / "feature_inventory.json"
    payload = _payload()
    snapshot.write_text(json.dumps(payload), encoding="utf-8")
    preview, pages = feature_page_projections(payload)
    monkeypatch.setattr(snapshot_service, "FEATURE_SOURCE_PAGES_MAX_BYTES", 100)

    snapshot_service._write_feature_reuse_receipt(
        snapshot, 3, validated_contract=True, preview=preview, source_pages=pages,
    )

    receipt = json.loads(feature_reuse_receipt_path(snapshot).read_bytes())
    assert "source_pages_sha256" not in receipt
    assert not feature_source_pages_path(snapshot).exists()
    assert trusted_feature_preview(snapshot, source_stat=snapshot.stat()) == preview
    assert trusted_feature_source_pages(snapshot, source_stat=snapshot.stat()) is None


def test_first_page_receipt_rejects_tamper_and_changed_source(tmp_path: Path) -> None:
    snapshot = tmp_path / "feature_inventory.json"
    payload = _payload()
    preview = feature_page_preview(payload)
    snapshot.write_text(json.dumps(payload), encoding="utf-8")
    snapshot_service._write_feature_reuse_receipt(
        snapshot, 3, validated_contract=True, preview=preview,
    )
    assert trusted_feature_preview(snapshot, source_stat=snapshot.stat()) == preview

    receipt_path = feature_reuse_receipt_path(snapshot)
    original_receipt = receipt_path.read_bytes()
    tampered = json.loads(original_receipt)
    tampered["first_page_preview"]["rows"][0]["field"] = "fabricated"
    receipt_path.write_text(json.dumps(tampered), encoding="utf-8")
    assert trusted_feature_preview(snapshot, source_stat=snapshot.stat()) is None

    receipt_path.write_bytes(original_receipt)
    payload["rows"][0]["field"] = "changed"
    snapshot.write_text(json.dumps(payload), encoding="utf-8")
    tampered = json.loads(original_receipt)
    signature = feature_source_signature(snapshot.stat())
    tampered["source_signature"] = signature
    tampered["receipt_sha256"] = feature_reuse_checksum(
        signature, tampered["source_sha256"], 3,
    )
    receipt_path.write_text(json.dumps(tampered), encoding="utf-8")
    assert trusted_feature_preview(snapshot, source_stat=snapshot.stat()) is None


@pytest.mark.parametrize("change", [
    {"read_only": False},
    {"production_control_possible": True},
    {"summary": {"fields": 2}},
    {"rows": [{"field": "incomplete"}]},
])
def test_feature_page_rejects_invalid_public_snapshot(change: dict) -> None:
    with pytest.raises(ValueError):
        FeaturePageIndex.from_payload({**_payload(), **change}, source_signature=(1, 2, 3, 4, 5))
