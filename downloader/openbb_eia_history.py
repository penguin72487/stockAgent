"""Bounded EIA v2 adapter executed only by the canonical OpenBB TaskWorker.

Its HTTP-boundary limiter owns every page. No independent EIA service, quota
bucket, credentials file, or storage namespace is introduced.
"""
from __future__ import annotations

import json
from urllib import request as urlrequest
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request

import pyarrow as pa

ENDPOINT = "commodity.energy_history"
ROUTES = {
    "gas_storage_weekly": ("natural-gas/stor/wkly", "weekly", ("value",), ("period", "series")),
    "gas_prices_daily": ("natural-gas/pri/fut", "daily", ("value",), ("period", "series")),
    "electricity_retail_monthly": ("electricity/retail-sales", "monthly", ("price", "sales", "revenue", "customers"),
                                    ("period", "stateid", "sectorid")),
}


def fetch_history(kwargs: dict, obb, *, opener=None, max_pages: int = 100) -> pa.Table:
    # Resolve after the canonical worker installs its HTTP-boundary limiter;
    # an early module import must not capture an unwrapped transport forever.
    open_request = opener or urlrequest.urlopen
    name = kwargs["dataset"]
    route, frequency, values, keys = ROUTES[name]
    credential = getattr(obb.user.credentials, "eia_api_key", None)
    if not credential:
        raise RuntimeError("EIA credential unavailable")
    key = credential.get_secret_value() if hasattr(credential, "get_secret_value") else str(credential)
    params = {"api_key": key, "frequency": frequency, "start": kwargs["start_date"],
              "end": kwargs["end_date"], "length": 5000}
    for index, column in enumerate(values):
        params[f"data[{index}]"] = column
    for index, column in enumerate(keys):
        params[f"sort[{index}][column]"] = column
        params[f"sort[{index}][direction]"] = "asc"
    if frequency == "monthly":
        params["start"], params["end"] = params["start"][:7], params["end"][:7]
    records, seen, expected = [], set(), None
    for _ in range(max_pages):
        params["offset"] = len(records)
        request = Request(f"https://api.eia.gov/v2/{route}/data/?{urlencode(params)}",
                          headers={"User-Agent": "stockAgent-economic-research/1.0", "Accept": "application/json"})
        try:
            with open_request(request, timeout=60) as response:
                body = response.read(16 * 1024**2 + 1)
            if len(body) > 16 * 1024**2:
                raise RuntimeError("EIA response byte limit")
            result = json.loads(body, parse_float=str).get("response", {})
        except HTTPError as exc:
            # Never include the request URL: EIA echoes the API key in it.
            raise RuntimeError(f"EIA HTTP {exc.code}") from None
        except (URLError, ValueError):
            raise RuntimeError("EIA transport or JSON failure") from None
        rows = result.get("data")
        total = int(result.get("total", -1))
        if total < 0 or (expected is not None and total != expected) or not isinstance(rows, list):
            raise RuntimeError("EIA invalid or changing pagination total")
        expected = total
        if not rows and total > len(records):
            raise RuntimeError("EIA truncated pagination")
        for row in rows:
            if not isinstance(row, dict) or not all(k in row for k in (*keys, *values)):
                raise RuntimeError("EIA response schema mismatch")
            identity = tuple(str(row[k]) for k in keys)
            if identity in seen:
                raise RuntimeError("EIA duplicate or repeated page")
            if not params["start"] <= str(row["period"]) <= params["end"]:
                raise RuntimeError("EIA observation outside requested window")
            seen.add(identity)
            records.append(row)
        if len(records) == total:
            if not records:
                from openbb_core.provider.utils.errors import EmptyDataError
                raise EmptyDataError("EIA authoritative empty query")
            columns = sorted(set().union(*(r.keys() for r in records)))
            return pa.table({c: pa.array([None if r.get(c) is None else str(r[c]) for r in records], type=pa.string())
                             for c in columns}).replace_schema_metadata({b"historical_point_in_time": b"false",
                                b"source": route.encode(), b"unit_contract": b"retain provider value/unit columns unchanged"})
        if len(records) > total:
            raise RuntimeError("EIA rows exceed declared total")
    raise RuntimeError("EIA pagination bound reached; archive remains incomplete")
