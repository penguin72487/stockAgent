"""Official current stock-futures membership, not historical trade eligibility.

The producer validates the complete TAIFEX stockLists table (including its
published totals). Readers only join a small, checksummed local snapshot; they
never call a broker or fetch the network on a dashboard request.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
import hashlib
import json
from pathlib import Path
import re
from typing import Any

SOURCE_URL = "https://www.taifex.com.tw/cht/2/stockLists"
DEFAULT_CATALOG_PATH = Path(__file__).resolve().parents[2] / "data_tw_futures/taifex_stock_futures_catalog.json"
MAX_CATALOG_BYTES = 2 * 1024 * 1024
MAX_AGE = timedelta(days=3)


def rows_digest(rows: list[dict[str, Any]]) -> str:
    return hashlib.sha256(json.dumps(rows, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def parse_stock_futures_catalog(html: str) -> list[dict[str, Any]]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    tables = [table for table in soup.find_all("table") if "證券代號" in table.get_text()]
    if len(tables) != 1:
        raise ValueError("expected exactly one complete TAIFEX stockLists table")
    records = []
    expected_total = None
    indexes = None
    for tr in tables[0].find_all("tr"):
        headers = [re.sub(r"\s+", "", cell.get_text()) for cell in tr.find_all("th")]
        if headers:
            required = ("股票期貨、選擇權商品代碼", "證券代號", "標的證券簡稱", "是否為股票期貨標的")
            if not all(key in headers for key in required):
                raise ValueError("TAIFEX stockLists schema changed")
            indexes = [headers.index(key) for key in required]
            continue
        cells = [cell.get_text(" ", strip=True) for cell in tr.find_all("td")]
        if not cells:
            continue
        if indexes is None or len(cells) <= max(indexes):
            raise ValueError("incomplete TAIFEX stockLists row")
        product, symbol, name, membership = (cells[index] for index in indexes)
        if "標的合計數" in " ".join(cells):
            expected_total = int(membership.replace(",", ""))
            continue
        if not re.fullmatch(r"[A-Z0-9]{2,5}", product) or not re.fullmatch(r"\d{4,6}[A-Z]?", symbol) or not name:
            raise ValueError("invalid TAIFEX stock/product identity")
        flag_text = re.sub(r"\s+", "", membership)
        for positive in ("●", "◎", "是股票期貨標的"):
            flag_text = flag_text.replace(positive, "")
        if flag_text:
            raise ValueError("unrecognized TAIFEX futures membership flag")
        records.append({"product": product, "symbol": symbol, "name": name, "has_futures": bool(membership)})
    if not records or len({row["product"] for row in records}) != len(records):
        raise ValueError("empty or duplicate TAIFEX product table")
    if expected_total is None or expected_total != sum(row["has_futures"] for row in records):
        raise ValueError("TAIFEX futures total does not match the complete table")
    return sorted(records, key=lambda row: (row["symbol"], row["product"]))


@lru_cache(maxsize=4)
def _read_verified(path: Path, signature: tuple[int, ...]) -> dict[str, Any]:
    if signature[2] > MAX_CATALOG_BYTES:
        raise ValueError("catalog exceeds bounded read limit")
    with path.open("rb") as stream:
        raw = stream.read(MAX_CATALOG_BYTES + 1)
    if len(raw) > MAX_CATALOG_BYTES:
        raise ValueError("catalog changed beyond bounded read limit")
    data = json.loads(raw)
    rows = data["rows"]
    if (data.get("schema_version") != 1 or data.get("source_url") != SOURCE_URL
            or data.get("complete") is not True or not rows
            or data.get("rows_sha256") != rows_digest(rows)
            or data.get("row_count") != len(rows)):
        raise ValueError("invalid stock futures catalog proof")
    by_symbol: dict[str, list[str]] = {}
    for row in rows:
        if (not re.fullmatch(r"\d{4,6}[A-Z]?", row["symbol"])
                or not re.fullmatch(r"[A-Z0-9]{2,5}", row["product"])
                or type(row["has_futures"]) is not bool):
            raise ValueError("invalid stock futures catalog record")
        if row["has_futures"]:
            by_symbol.setdefault(row["symbol"], []).append(row["product"])
    return {**data, "by_symbol": by_symbol}


@dataclass(frozen=True)
class StockFuturesCatalog:
    revision: tuple[Any, ...]
    observed_at: str | None
    reason: str
    products: dict[str, list[str]]

    def membership(self, symbol: str) -> dict[str, Any]:
        code = str(symbol).strip().upper()
        # Only normalize explicit exchange suffixes; never coerce to an int and
        # lose ETF leading zeroes, or join by a fuzzy company name.
        code = re.sub(r"\.(TW|TWO)$", "", code)
        valid = re.fullmatch(r"\d{4,6}[A-Z]?", code) is not None
        products = self.products.get(code, []) if not self.reason and valid else []
        return {
            "status": "unknown" if self.reason or not valid else "listed" if products else "not_listed",
            "products": list(products),
            "as_of": self.observed_at,
            "scope": "latest_catalog_not_signal_date",
            "reason": self.reason or ("" if valid else "invalid_symbol"),
            "source_url": SOURCE_URL,
        }


def load_stock_futures_catalog(path: Path | None = None, *, now: datetime | None = None) -> StockFuturesCatalog:
    target = Path(path) if path is not None else DEFAULT_CATALOG_PATH
    signature: tuple[int, ...] = ()
    try:
        stat = target.stat()
        signature = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
        data = _read_verified(target, signature)
        observed = datetime.fromisoformat(data["retrieved_at_utc"])
        if observed.tzinfo is None:
            raise ValueError("catalog timestamp has no timezone")
        age = (now or datetime.now(timezone.utc)) - observed
        reason = "stale_catalog" if age > MAX_AGE else "future_catalog" if age < timedelta(minutes=-5) else ""
        return StockFuturesCatalog((str(target), *signature, reason), observed.isoformat(), reason, data["by_symbol"])
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return StockFuturesCatalog((str(target), *signature, "invalid_catalog"), None, "missing_or_invalid_catalog", {})
