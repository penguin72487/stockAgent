from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

from stockagent.data.tw_stock_futures_catalog import (
    SOURCE_URL, load_stock_futures_catalog, parse_stock_futures_catalog, rows_digest,
)
from stockagent.live.public_dashboards import sanitize_tw_signals


def table(total="2"):
    return f"""<table><tr><th>股票期貨、<br>選擇權<br>商品代碼</th><th>證券代號</th>
    <th>標的證券簡稱</th><th>是否為<br>股票期貨<br>標的</th></tr>
    <tr><td>CD</td><td>2330</td><td>台積電</td><td>● 是股票期貨標的</td></tr>
    <tr><td>NY</td><td>0050</td><td>元大台灣50</td><td>◎</td></tr>
    <tr><td>ZZ</td><td>9999</td><td>僅選擇權</td><td></td></tr>
    <tr><td>標的合計數：</td><td></td><td></td><td>{total}</td></tr></table>"""


def write_catalog(path: Path, now: datetime, rows=None):
    rows = rows if rows is not None else parse_stock_futures_catalog(table())
    payload = {"schema_version": 1, "source_url": SOURCE_URL, "complete": True,
               "retrieved_at_utc": now.isoformat(), "rows": rows,
               "rows_sha256": rows_digest(rows), "row_count": len(rows)}
    path.write_text(json.dumps(payload))
    return payload


def test_parser_counts_exact_membership_not_options_or_fuzzy_names():
    rows = parse_stock_futures_catalog(table())
    assert rows[0]["symbol"] == "0050"
    assert next(row for row in rows if row["symbol"] == "9999")["has_futures"] is False
    with pytest.raises(ValueError, match="total"):
        parse_stock_futures_catalog(table("3"))
    with pytest.raises(ValueError):
        parse_stock_futures_catalog(table().replace("◎", "?"))
    with pytest.raises(ValueError):
        parse_stock_futures_catalog(table().replace("◎", "不是股票期貨標的"))
    with pytest.raises(ValueError):
        parse_stock_futures_catalog(table().replace("<td>NY</td>", "<td>CD</td>"))
    with pytest.raises(ValueError):
        parse_stock_futures_catalog(table().replace("證券代號", "wrong schema"))


def test_reader_receipt_freshness_tristate_and_symbol_identity(tmp_path):
    path = tmp_path / "catalog.json"
    now = datetime(2026, 9, 6, 13, tzinfo=timezone.utc)
    write_catalog(path, now)
    catalog = load_stock_futures_catalog(path, now=now)
    assert catalog.membership("2330.TW")["products"] == ["CD"]
    assert catalog.membership("0050")["status"] == "listed"
    assert catalog.membership("9999")["status"] == "not_listed"
    assert catalog.membership("1101")["status"] == "not_listed"
    assert catalog.membership("台積電")["status"] == "unknown"
    assert catalog.membership("50")["status"] == "unknown"
    assert catalog.membership("2330")["scope"] == "latest_catalog_not_signal_date"
    stale = load_stock_futures_catalog(path, now=now + timedelta(days=4))
    assert stale.membership("2330")["status"] == "unknown"
    assert stale.revision != catalog.revision
    future = load_stock_futures_catalog(path, now=now - timedelta(hours=1))
    assert future.reason == "future_catalog"


def test_catalog_change_invalidates_cache_and_corruption_fails_closed(tmp_path):
    path = tmp_path / "catalog.json"
    now = datetime.now(timezone.utc)
    write_catalog(path, now)
    first = load_stock_futures_catalog(path, now=now)
    payload = write_catalog(path, now, [{"product": "CD", "symbol": "2330", "name": "台積電", "has_futures": False}])
    second = load_stock_futures_catalog(path, now=now)
    assert second.membership("2330")["status"] == "not_listed"
    assert first.revision != second.revision
    payload["rows"][0]["has_futures"] = True
    path.write_text(json.dumps(payload))
    assert load_stock_futures_catalog(path).membership("2330")["status"] == "unknown"
    path.unlink()
    assert load_stock_futures_catalog(path).membership("2330")["status"] == "unknown"


def test_public_membership_dto_drops_unregistered_fields():
    payload = {"simulation_only": True, "production_order_possible": False,
               "rows": [{"symbol": "2330", "stock_futures": {
                   "status": "listed", "products": ["CD"], "source_url": SOURCE_URL,
                   "as_of": "2026-09-06", "scope": "latest_catalog_not_signal_date",
                   "private_extra": "not public", "account_id": "private"}}]}
    public = sanitize_tw_signals(payload)
    membership = public["rows"][0]["stock_futures"]
    assert membership["status"] == "listed"
    assert membership["products"] == ["CD"]
    assert "private_extra" not in membership
    assert "account_id" not in membership


def test_signal_page_joins_only_requested_page_and_refreshes_on_catalog_change(tmp_path, monkeypatch):
    from stockagent.live import tw_day_trade_dashboard as dashboard
    from stockagent.data import tw_stock_futures_catalog as catalogs

    path = tmp_path / "catalog.json"
    now = datetime.now(timezone.utc)
    write_catalog(path, now)
    monkeypatch.setattr(catalogs, "DEFAULT_CATALOG_PATH", path)
    (tmp_path / "state.json").write_text(json.dumps({"modes": {"stable": {"session_date": "2026-09-04"}}}))
    signals = [{"market": "stable", "symbol": symbol, "session_date": "2026-09-04",
                "target_weight": weight, "status": "hold"}
               for symbol, weight in [("2330", 0.2), ("1101", 0.1), ("0050", 0)]]
    (tmp_path / "signals.jsonl").write_text("".join(json.dumps(row) + "\n" for row in signals))
    page = dashboard.build_dashboard_signal_page(state_dir=tmp_path, start_date="2026-09-04", end_date="2026-09-04", limit=1)
    assert page["total"] == 3
    assert page["returned"] == 1
    assert page["rows"][0]["stock_futures"]["status"] == "listed"
    write_catalog(path, now, [{"product": "CD", "symbol": "2330", "name": "台積電", "has_futures": False}])
    page = dashboard.build_dashboard_signal_page(state_dir=tmp_path, start_date="2026-09-04", end_date="2026-09-04", limit=1)
    assert page["rows"][0]["stock_futures"]["status"] == "not_listed"
