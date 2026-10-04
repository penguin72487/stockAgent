"""Official economic history adapters, separate from metadata discovery.

Adapters return source strings and dimensions unchanged. Observation dates are
not publication dates, and a current revision is not a historical vintage.
Transport, atomic writes, pacing and receipts belong to the existing downloader
primitives; no SDK login or additional broker session is opened here.
"""
from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date
import io
import json
from pathlib import PurePosixPath
import re
import threading
from urllib.parse import urlencode
import zipfile

from bs4 import BeautifulSoup
import pyarrow as pa

from downloader.download_keyed_public_catalogs import CatalogError, CatalogSpec


CENSUS_PROGRAMS = tuple("mrts marts mwts mtis m3 advm3 resconst ressales vip hv qss qfr qtax qpr ftd bfs mhs mhs2".split())
CENSUS_FIELDS = "cell_value,data_type_code,time_slot_id,error_data,category_code,seasonally_adj,geo_level_code,time_slot_date,time_slot_name"
BEA_TABLES = {
    "T10101": "real_gdp_growth", "T10105": "nominal_gdp", "T10106": "real_gdp",
    "T10109": "gdp_deflator", "T20305": "personal_consumption",
    "T20304": "pce_price_index", "T20600": "income_outlays_saving",
    "T30100": "government_receipts_expenditures", "T11200": "national_income_by_type",
}
_CSV_LIMIT_LOCK = threading.Lock()


@dataclass(frozen=True)
class EconomicJob:
    provider: str
    dataset: str
    endpoint: str
    params: dict[str, str]
    credential: str | None = None
    ttl_seconds: int = 86400
    interval: float = 1.0
    kind: str = "current_revision_history"

    @property
    def key(self) -> str:
        return f"{self.provider}/{self.dataset}"

    def transport_spec(self) -> CatalogSpec:
        return CatalogSpec(self.provider, self.dataset, self.credential, self.endpoint,
                           self.endpoint, kind=self.kind, interval_seconds=self.interval)


def core_jobs() -> list[EconomicJob]:
    jobs = []
    for program in CENSUS_PROGRAMS:
        params = {"get": CENSUS_FIELDS, "time": "from 1900"}
        # Official geography/variables metadata differs between EITS API
        # generations. These five require an explicit nation predicate; QTAX
        # also uses uppercase fields and has a distinct state-level history.
        if program in {"m3", "advm3", "qtax", "bfs", "mhs2"}:
            params["for"] = "us:*"
        if program == "qtax":
            params["get"] = CENSUS_FIELDS.upper()
        endpoint = f"https://api.census.gov/data/timeseries/eits/{program}"
        jobs.append(EconomicJob("census", program, endpoint, params, "CENSUS_API_KEY"))
        if program == "qtax":
            jobs.append(EconomicJob("census", "qtax_state", endpoint,
                                    {**params, "for": "state:*"}, "CENSUS_API_KEY"))
    # One table/all native frequencies/all available years per request, not
    # one call per line, country, quarter or year. No hard-coded start in 2000.
    jobs += [EconomicJob("bea", f"NIPA_{table}", "https://apps.bea.gov/api/data",
                         {"method": "GetData", "DataSetName": "NIPA", "TableName": table,
                          "Frequency": "A,Q,M", "Year": "X", "ResultFormat": "JSON"},
                         "BEA_API_KEY", interval=10.0)
             for table in BEA_TABLES]
    return jobs


def request_url(job: EconomicJob, key: str, *, since: str | None = None) -> str:
    params = dict(job.params)
    if job.provider == "census":
        params["key"] = key
        if since:
            params["time"] = f"from {since[:4]}"
    elif job.provider == "bea":
        params["UserID"] = key
        if since:
            params["Year"] = ",".join(str(y) for y in range(int(since[:4]), date.today().year + 1))
    elif job.provider == "frankfurter" and since:
        params["from"] = max(params["from"], since)
    return job.endpoint + "?" + urlencode(params)


def observation_year(value: str) -> int | None:
    match = re.match(r"^(\d{4})(?:$|[-QM])", str(value))
    return int(match[1]) if match else None


def parse_economic_json(job: EconomicJob, body: bytes) -> tuple[pa.Table, dict]:
    try:
        payload = json.loads(body, parse_float=str)
        if job.provider == "frankfurter":
            if not isinstance(payload, list) or not payload:
                raise CatalogError("empty_or_invalid_records")
            import math
            for row in payload:
                if (not isinstance(row, dict) or not {"date", "base", "quote", "rate"} <= row.keys()
                        or row["base"].upper() != job.params["base"].upper()
                        or not job.params["from"] <= row["date"] <= job.params["to"]
                        or not math.isfinite(float(row["rate"])) or float(row["rate"]) <= 0):
                    raise CatalogError("invalid_payload_schema")
            records = payload
            date_field, value_field = "date", "rate"
            unit = "quote currency per 1 base currency; provider native reference rate, not traded OHLC"
        elif job.provider == "census":
            if not isinstance(payload, list) or len(payload) < 2:
                raise CatalogError("empty_or_invalid_records")
            header, raw = payload[0], payload[1:]
            if (not isinstance(header, list) or len(header) != len(set(header))
                    or not all(isinstance(field, str) for field in header)
                    or not {"cell_value", "time", "data_type_code", "category_code"} <= {field.lower() for field in header}
                    or any(not isinstance(r, list) or len(r) != len(header) for r in raw)):
                raise CatalogError("invalid_payload_schema")
            records = [dict(zip(header, row, strict=True)) for row in raw]
            actual = {field.lower(): field for field in header}
            date_field, value_field = actual["time"], actual["cell_value"]
            unit = "source program_code + data_type_code; never assume dollars or percent"
        else:
            results = payload.get("BEAAPI", {}).get("Results", {})
            if isinstance(results, dict) and "Error" in results:
                code = str(results["Error"].get("APIErrorCode"))
                raise CatalogError({"1": "credential_rejected", "4": "credential_activation_required",
                                    "7": "provider_throttled"}.get(code, "provider_application_error"))
            records = results.get("Data") if isinstance(results, dict) else None
            if not isinstance(records, list) or not records:
                raise CatalogError("empty_or_invalid_records")
            if any(not isinstance(row, dict) or not {"TimePeriod", "DataValue"} <= row.keys() for row in records):
                raise CatalogError("invalid_payload_schema")
            date_field, value_field = "TimePeriod", "DataValue"
            unit = "METRIC_NAME + UNIT_MULT retained per row; DataValue is not rescaled"
        # Retain decimal strings, suppression symbols and all unit/quality
        # dimensions. Floating point coercion would destroy source precision.
        columns = sorted(set().union(*(row.keys() for row in records)))
        table = pa.table({col: pa.array([None if row.get(col) is None else str(row[col]) for row in records],
                                       type=pa.string()) for col in columns})
        periods = table[date_field].to_pylist()
        if any(observation_year(value) is None for value in periods):
            raise CatalogError("invalid_payload_schema")
        return table, {"date_column": date_field, "value_column": value_field, "unit_contract": unit,
                       "first_observation": min(periods), "last_observation": max(periods)}
    except CatalogError:
        raise
    except (ValueError, TypeError, KeyError, AttributeError):
        raise CatalogError("invalid_payload_schema") from None


def moi_season_jobs(body: bytes) -> list[EconomicJob]:
    """Discover actual published identifiers; a season is NOT a trade quarter."""
    seasons = {str(node.get("value", "")) for node in BeautifulSoup(body, "html.parser").find_all("option")}
    seasons = sorted((x for x in seasons if re.fullmatch(r"\d{3}S[1-4]", x)),
                     key=lambda s: (int(s[:3]), int(s[-1])))
    if not seasons:
        raise CatalogError("invalid_payload_schema")
    return [EconomicJob("moi", season, "https://plvr.land.moi.gov.tw/DownloadSeason",
                        {"season": season, "type": "zip", "fileName": "lvr_landcsv.zip"},
                        ttl_seconds=30 * 86400, kind="published_archive") for season in seasons]


def moi_trade_date(raw: str | None) -> str | None:
    value = str(raw or "").strip()
    if not re.fullmatch(r"\d{6,7}", value):
        return None
    try:
        return date(int(value[:-4]) + 1911, int(value[-4:-2]), int(value[-2:])).isoformat()
    except ValueError:
        return None


def parse_moi_zip(body: bytes, *, max_uncompressed: int = 1024**3, observed_date: date | None = None) -> tuple[dict[str, pa.Table], dict]:
    """Never extract ZIP paths; bound expansion and preserve every CSV field."""
    tables: dict[str, pa.Table] = {}
    dates: list[str] = []
    invalid_dates = 0
    suspicious_dates = 0
    observed_date = observed_date or date.today()
    rejected = []
    with zipfile.ZipFile(io.BytesIO(body)) as archive:
        members = archive.infolist()
        if len(members) > 5000 or sum(i.file_size for i in members) > max_uncompressed:
            raise CatalogError("normalized_byte_limit")
        names = [i.filename for i in members]
        if len(names) != len(set(names)):
            raise CatalogError("invalid_payload_schema")
        for item in members:
            name = PurePosixPath(item.filename)
            if name.is_absolute() or ".." in name.parts or "\\" in item.filename:
                raise CatalogError("invalid_payload_schema")
            if not re.fullmatch(r"[a-z]_lvr_land_[abc](?:_(?:land|build|park))?\.csv", item.filename.lower()):
                continue  # README/schema/manifest remain preserved inside raw ZIP.
            if item.file_size > 128 * 1024**2:
                raise CatalogError("normalized_byte_limit")
            decoded = archive.read(item).decode("utf-8-sig")  # includes ZIP CRC validation
            # A malformed source quote can swallow thousands of physical rows.
            # Permit bounded inspection, then quarantine oversized fields rather
            # than losing the whole archive at csv's default 128 KiB limit.
            with _CSV_LIMIT_LOCK:
                previous_limit = csv.field_size_limit()
                try:
                    csv.field_size_limit(max(previous_limit, 128 * 1024**2))
                    rows = list(csv.reader(io.StringIO(decoded)))
                finally:
                    csv.field_size_limit(previous_limit)
            if not rows:
                raise CatalogError("invalid_payload_schema")
            header = rows[0]
            if len(header) != len(set(header)):
                raise CatalogError("invalid_payload_schema")
            raw = rows[1:]
            # The second row is an English field dictionary, not a transaction.
            if raw and raw[0] and raw[0][0].lower().startswith(("the villages", "the land", "the serial", "transaction", "building", "serial", "district", "number")):
                raw = raw[1:]
            valid = []
            for index, row in enumerate(raw):
                oversized = any(len(value) > 131072 for value in row)
                if len(row) != len(header) or oversized:
                    # Do not guess the location of an unescaped comma/newline.
                    # Retain exact fields for repair without dropping the batch.
                    rejected.append({"member": item.filename, "data_row_index": index,
                                     "expected_fields": len(header), "actual_fields": len(row),
                                     "reason": "oversize_csv_field_requires_review" if oversized else "field_count_mismatch",
                                     "raw_fields_json": json.dumps(row, ensure_ascii=False)})
                else:
                    valid.append(row)
            raw = valid
            table = pa.table({col: pa.array([row[idx] for row in raw], type=pa.string())
                              for idx, col in enumerate(header)})
            if "交易年月日" in header:
                parsed = [moi_trade_date(value) for value in table["交易年月日"].to_pylist()]
                issues = ["invalid_calendar_date" if d is None else "future_transaction_date" if d > observed_date.isoformat()
                          else "before_1990_requires_review" if d < "1990-01-01" else None for d in parsed]
                suspicious_dates += sum(issue is not None for issue in issues)
                # Future transaction dates are source errors, not forecasts.
                # Retain the source ROC string and reason; never invent a fix.
                parsed = [None if issue == "future_transaction_date" else d for d, issue in zip(parsed, issues)]
                dates.extend(d for d in parsed if d)
                invalid_dates += sum(d is None for d in parsed)
                table = table.append_column("observation_date", pa.array(parsed, type=pa.string()))
                table = table.append_column("transaction_date_issue", pa.array(issues, type=pa.string()))
            tables[item.filename.lower().removesuffix(".csv")] = table
        if not tables and set(names) != {"manifest.csv", "build.ttt"}:
            raise CatalogError("empty_or_invalid_records")
    observations = sum(t.num_rows for t in tables.values())
    if rejected:
        tables["rejected_rows"] = pa.Table.from_pylist(rejected)
    return tables, {"first_observation": min(dates, default=None), "last_observation": max(dates, default=None),
                    "observation_rows": observations, "rejected_rows": len(rejected),
                    "source_empty": not tables, "normalization_complete": not rejected and not invalid_dates,
                    "suspicious_transaction_dates": suspicious_dates,
                    "date_quality_policy": "Future dates excluded from normalized date bounds; pre-1990 retained with review flag, not continuous historical coverage",
                    "invalid_transaction_dates": invalid_dates, "unit_contract": "TWD; area=m2; preserve source column labels",
                    "date_semantics": "transaction date, not publication; season labels are source batch identifiers"}
