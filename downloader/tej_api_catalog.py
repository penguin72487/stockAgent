"""Public TEJ trial metadata, distinct from licensed observations/entitlements.

The vendor spells this database TRAIL. The website's minYear, description and
dataRange can disagree; none of them certify this account's usable history.
"""
from __future__ import annotations

import csv
from datetime import UTC, datetime
import hashlib
import io
import json
from pathlib import Path
import re

from downloader.artifact_io import atomic_write_json, atomic_write_text
from downloader.common import SharedRateLimiter
from downloader.http_transport import HttpRequestPolicy, ResilientHttpTransport

BASE = "https://api.tej.com.tw"
CATALOG_CONTRACT = "tej_trial_public_metadata_v1"
TRIAL_LIMITS = {"calls_per_day": 500, "rows_per_day": 50_000,
                "rows_per_page": 10_000, "rows_per_query": 50_000}
TABLE_ID = re.compile(r"TRAIL/[A-Z0-9]+\Z")
HEADERS = {"User-Agent": "StockAgent/1.0 TEJ-trial-collector", "Accept": "application/json"}
FREQUENCIES = {
    "AIND": "current_snapshot", "TAATT": "current_snapshot",
    "TAOFATT": "current_snapshot", "TAOPBAS": "current_snapshot",
    "TAIACC": "reference", "TAMT": "event",
    "TAOFCAN": "event", "TAOFSUSP": "event",
    "TASALE": "monthly", "TAOFIVA": "monthly", "TAOFIVP": "monthly",
    "TAOFMNV": "monthly", "TAOFUNDS": "monthly",
    "TAIM1A": "quarterly_cumulative", "TAIM1AA": "quarterly_cumulative",
    "TAIM1AQ": "quarterly_single", "TAIM1AQA": "quarterly_single",
    "TAAPRRENT": "event", "TAAPRTRAN": "event", "TALANDTR": "event",
}


def metadata_transport() -> ResilientHttpTransport:
    # Local courtesy pacing, not a claimed official per-second limit.
    return ResilientHttpTransport(HttpRequestPolicy("tej-public-metadata", max_retries=2),
                                 limiter=SharedRateLimiter(.2, name="tej-public-metadata"))


def _object(transport, url: str) -> dict:
    response = transport.request_bytes(url, headers=HEADERS)
    if len(response.body) > 16 * 1024 * 1024:
        raise ValueError("tej_metadata_oversized")
    value = json.loads(response.body)
    if not isinstance(value, dict):
        raise ValueError("tej_metadata_not_object")
    return value


def collect_catalog(transport=None, *, now: datetime | None = None,
                    entitled_table_ids: tuple[str, ...] = ()) -> dict:
    transport = transport or metadata_transport()
    catalog = _object(transport, BASE + "/web/api/TRAIL")
    entries = catalog.get("tables")
    if not isinstance(entries, list) or not entries:
        raise ValueError("tej_trial_catalog_empty")
    listed = {entry.get("tableId") or "TRAIL/" + str(entry.get("tableName", "")) for entry in entries}
    public_count = len(listed)
    entries = entries + [{"tableId": table_id, "groupName": "帳號另列授權／不動產"}
                         for table_id in sorted(set(entitled_table_ids) - listed)]
    tables = []
    seen = set()
    for entry in entries:
        table_id = entry.get("tableId") or "TRAIL/" + str(entry.get("tableName", ""))
        if not TABLE_ID.fullmatch(table_id) or table_id in seen:
            raise ValueError("tej_trial_catalog_identity_invalid")
        seen.add(table_id)
        source = BASE + "/web/api/" + table_id
        detail = _object(transport, source)
        if detail.get("tableId") != table_id or not isinstance(detail.get("columns"), list):
            raise ValueError("tej_trial_column_identity_invalid")
        columns = [{key: column.get(key) for key in ("name", "type", "cname", "description", "unit")}
                   for column in detail["columns"]]
        names = [column["name"] for column in columns]
        if not names or len(set(names)) != len(names) or any(not isinstance(x, str) or not x for x in names):
            raise ValueError("tej_trial_columns_invalid")
        # Explicit allowlist: never persist the website's sample observations.
        item = {key: detail.get(key) for key in
                ("tableId", "tableName", "cName", "description", "rowCount",
                 "refreshDate", "minYear", "dataRange", "pivot")}
        item.update({"groupName": entry.get("groupName"), "columns": columns,
                     "source_url": source, "frequency": FREQUENCIES.get(table_id.split("/")[1], "daily"),
                     "frequency_basis": "description_or_table_semantics_not_verified_release_schedule",
                     "declared_range_basis": "public_catalog_not_account_permission",
                     "actual_first": None, "actual_last": None, "access_state": "not_verified"})
        item["schema_sha256"] = hashlib.sha256(json.dumps(columns, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
        tables.append(item)
    return {"contract": CATALOG_CONTRACT, "provider": "tej_api_trial", "database": "TRAIL",
            "observed_at_utc": (now or datetime.now(UTC)).isoformat(),
            "catalog_url": BASE + "/web/api/TRAIL", "tables": tables,
            "public_catalog_table_count": public_count,
            "table_count": len(tables), "field_count": sum(len(t["columns"]) for t in tables),
            "quota": TRIAL_LIMITS,
            "quota_source": "https://tejtw.github.io/EN-TEJAPI/",
            "licensed_data_requested": False, "account_entitlements_verified": False}


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        raise ValueError("Cannot publish an empty inventory")
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    atomic_write_text(path, stream.getvalue(), durable=True)


def publish_catalog(catalog: dict, destination: Path) -> dict:
    destination.mkdir(parents=True, exist_ok=True)
    atomic_write_json(destination / "catalog.json", catalog)
    rows, fields = [], []
    for table in catalog["tables"]:
        rows.append({"table_id": table["tableId"], "category": table["groupName"],
                     "name": str(table["cName"]).strip(), "fields": len(table["columns"]),
                     "frequency": table["frequency"], "declared_range": table["dataRange"],
                     "catalog_min_year": table["minYear"], "catalog_rows_estimate": table["rowCount"],
                     "catalog_refresh_date": table["refreshDate"],
                     "actual_first": table["actual_first"], "actual_last": table["actual_last"],
                     "permission_state": table["access_state"], "source_url": table["source_url"],
                     "schema_sha256": table["schema_sha256"], "description": table["description"]})
        for column in table["columns"]:
            fields.append({"table_id": table["tableId"], "table_name": str(table["cName"]).strip(),
                           "field": column["name"], "label": column["cname"],
                           "source_type": column["type"], "source_unit": column["unit"],
                           "description": column["description"], "source_url": table["source_url"],
                           "table_access_state": table["access_state"],
                           "table_first_observed": table["actual_first"], "table_last_observed": table["actual_last"],
                           "unit_quality_state": "conflicting_million_label_and_thousand_unit_requires_verification"
                             if table['tableId'] == 'TRAIL/TAPRCD' and column['name'] == 'mv'
                             else "source_unit_retained_not_canonical_normalized",
                           "field_history_complete": False})
    write_csv(destination / "tables.csv", rows)
    write_csv(destination / "fields.csv", fields)
    summary = {key: catalog[key] for key in ("contract", "observed_at_utc", "table_count", "field_count", "quota")}
    # This is a conditional capacity floor, never an observed completion ETA.
    estimates = [t["rowCount"] for t in catalog["tables"]]
    if all(type(x) is int and x >= 0 for x in estimates):
        total = sum(estimates)
        summary.update({"catalog_rows_estimate": total,
                        "row_quota_days_floor_if_catalog_counts_current": (total + 49_999) // 50_000,
                        "eta_state": "unavailable_until_account_scope_and_actual_counts_verified"})
    atomic_write_json(destination / "summary.json", summary)
    return summary
