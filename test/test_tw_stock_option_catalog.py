import hashlib
import json
from pathlib import Path

import pytest

from scripts.audit_tw_stock_option_current_catalog import audit
from stockagent.data.tw_stock_futures_catalog import (
    SOURCE_URL, parse_stock_futures_catalog, parse_stock_option_catalog, rows_digest,
)


def table():
    return """<table><tr>
    <th>股票期貨、選擇權商品代碼</th><th>證券代號</th><th>標的證券簡稱</th>
    <th>是否為股票期貨標的</th><th>是否為股票選擇權標的</th>
    <th>是否為股票選擇權週契約標的</th><th>上市普通股標的證券</th>
    <th>上櫃普通股標的證券</th><th>上市ETF標的證券</th><th>上櫃ETF標的證券</th>
    </tr><tr><td>CD</td><td>2330</td><td>台積電</td><td>● 是股票期貨標的</td>
    <td>● 是股票選擇權標的</td><td>● 是股票選擇權週契約標的</td>
    <td>◎ 是上市普通股標的證券</td><td></td><td></td><td></td></tr>
    <tr><td>NY</td><td>0050</td><td>台灣50</td><td>●</td><td>●</td><td></td>
    <td></td><td></td><td>◎</td><td></td></tr>
    <tr><td>AB</td><td>9999</td><td>上櫃股</td><td></td><td>●</td><td></td>
    <td></td><td>◎</td><td></td><td></td></tr>
    <tr><td>ZZ</td><td>8888</td><td>上櫃ETF</td><td>●</td><td></td><td></td>
    <td></td><td></td><td></td><td>◎</td></tr>
    <tr><td>標的合計數：</td><td></td><td></td><td>3</td><td>3</td><td>1</td>
    <td>1</td><td>1</td><td>1</td><td>1</td></tr></table>"""


def test_option_catalog_uses_family_flags_and_weekly_subtotal():
    rows = parse_stock_option_catalog(table())
    assert [(r["product"], r["underlying_kind"], r["has_weekly"]) for r in rows] == [
        ("NY", "listed_etf", False), ("CD", "listed_stock", True), ("AB", "otc_stock", False),
    ]
    assert parse_stock_futures_catalog(table())[0]["product"] == "NY"


@pytest.mark.parametrize("body", [
    table().replace("<td>3</td><td>3</td><td>1</td>", "<td>3</td><td>4</td><td>1</td>"),
    table().replace("● 是股票選擇權標的", "不是股票選擇權標的"),
    table().replace("<td>● 是股票選擇權標的</td><td>● 是股票選擇權週契約標的</td>",
                    "<td></td><td>● 是股票選擇權週契約標的</td>"),
    table().replace("是否為股票選擇權週契約標的", "unknown weekly field"),
    table().replace("<td>AB</td>", "<td>CD</td>"),
])
def test_option_catalog_rejects_ambiguous_membership(body):
    with pytest.raises(ValueError):
        parse_stock_option_catalog(body)


def _snapshot(tmp_path: Path):
    body = table().encode()
    digest = hashlib.sha256(body).hexdigest()
    raw = tmp_path / "stock_futures_catalog_sources" / f"{digest}.html"
    raw.parent.mkdir()
    raw.write_bytes(body)
    rows = parse_stock_futures_catalog(body.decode())
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"schema_version": 1, "source_url": SOURCE_URL,
        "scope": "latest_catalog_not_signal_date", "complete": True,
        "retrieved_at_utc": "2026-10-29T04:51:00+00:00", "source_sha256": digest,
        "row_count": len(rows), "rows_sha256": rows_digest(rows), "rows": rows}))
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"contract": "taifex_option_session_policy_candidate_v1",
        "research_candidate_only": True, "products": {"CDO": {}, "NYO": {}}}))
    return raw, catalog, policy


def test_audit_pins_source_and_separates_policy_from_catalog(tmp_path):
    raw, catalog, policy = _snapshot(tmp_path)
    output = tmp_path / "readback.json"
    result = audit(catalog, policy, output)
    assert result["option_family_count"] == 3
    assert result["weekly_option_family_count"] == 1
    assert result["candidate_in_policy_count"] == 2
    assert result["candidates_absent_from_policy"] == ["ABO"]
    assert result["financial_or_training_adopted"] is False
    with pytest.raises(FileExistsError):
        audit(catalog, policy, output)
    raw.write_bytes(b"changed")
    with pytest.raises(ValueError, match="source digest"):
        audit(catalog, policy, tmp_path / "other.json")
