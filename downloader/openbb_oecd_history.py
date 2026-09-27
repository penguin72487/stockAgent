"""OECD forecast adapter inside the existing OpenBB worker and quota boundary.

The installed SDK pins Economic Outlook 1.1, truncates levels to integers and
removes negative growth. Keep raw SDMX columns and the standard value contract,
including negative/zero observations, without mutating old archive shards.
"""
from __future__ import annotations

import csv
from datetime import date
from decimal import Decimal, InvalidOperation
import io
import re
from urllib import request as urlrequest
from urllib.error import HTTPError
from urllib.parse import urlencode

import pyarrow as pa


def fetch_gdp_forecast(kwargs: dict, *, opener=None) -> pa.Table:
    from openbb_oecd.models.gdp_forecast import OECDGdpForecastFetcher, COUNTRY_TO_CODE_GDP_FORECAST, CODE_TO_COUNTRY_GDP_FORECAST
    from openbb_core.provider.utils.errors import EmptyDataError

    query = OECDGdpForecastFetcher.transform_query(kwargs)
    measures = {"current_prices": "GDP_USD", "volume": "GDPV_USD", "capita": "GDPVD_CAP",
                "growth": "GDPV_ANNPCT", "deflator": "PGDP"}
    frequency = "Q" if query.frequency == "quarter" and query.units != "capita" else "A"
    country = "" if query.country == "all" else "+".join(COUNTRY_TO_CODE_GDP_FORECAST[c.lower()] for c in query.country.split(","))
    # The official API documents an empty version as latest. STRUCTURE_ID in
    # the returned CSV records the actual version; it is not historical PIT.
    url = f"https://sdmx.oecd.org/public/rest/data/OECD.ECO.MAD,DSD_EO@DF_EO,/{country}.{measures[query.units]}.{frequency}?" + urlencode({
        "startPeriod": str(query.start_date), "endPeriod": str(query.end_date),
        "dimensionAtObservation": "AllDimensions", "detail": "full", "format": "csvfile"})
    request = urlrequest.Request(url, headers={"User-Agent": "stockAgent-economic-research/1.0",
                                             "Accept": "application/vnd.sdmx.data+csv;version=2.0"})
    try:
        with (opener or urlrequest.urlopen)(request, timeout=60) as response:
            body = response.read(64 * 1024**2 + 1)
    except HTTPError as exc:
        message = exc.read(4096).decode("utf-8", errors="replace")
        if exc.code == 404 and "NoRecordsFound" in message:
            raise EmptyDataError("OECD authoritative NoRecordsFound response") from None
        raise RuntimeError(f"OECD HTTP {exc.code}") from None
    if len(body) > 64 * 1024**2:
        raise RuntimeError("OECD response byte limit; no truncated success")
    reader = csv.DictReader(io.StringIO(body.decode("utf-8-sig")))
    required = {"STRUCTURE_ID", "REF_AREA", "TIME_PERIOD", "OBS_VALUE", "MEASURE", "FREQ"}
    if not required <= set(reader.fieldnames or []):
        raise RuntimeError("OECD forecast schema mismatch")
    records, identities = [], set()
    for row in reader:
        if None in row or any(value is None for value in row.values()):
            raise RuntimeError("OECD malformed CSV row")
        if row["MEASURE"] != measures[query.units] or row["FREQ"] != frequency:
            raise RuntimeError("OECD forecast outside requested dimensions")
        version = re.fullmatch(r"OECD\.ECO\.MAD:DSD_EO@DF_EO\(([^)]+)\)", row["STRUCTURE_ID"])
        if not version:
            raise RuntimeError("OECD forecast source version missing")
        period = row["TIME_PERIOD"]
        if re.fullmatch(r"\d{4}", period):
            observed = date(int(period), 1, 1)
        elif re.fullmatch(r"\d{4}-Q[1-4]", period):
            observed = date(int(period[:4]), (int(period[-1]) - 1) * 3 + 1, 1)
        else:
            raise RuntimeError("OECD forecast period format changed")
        if not query.start_date <= observed <= query.end_date:
            continue  # SDK/SDMX date-grain boundaries can include a partial period.
        identity = (row["STRUCTURE_ID"], row["REF_AREA"], period, row["MEASURE"], frequency)
        if identity in identities:
            raise RuntimeError("OECD duplicate observation; unhandled dimension")
        identities.add(identity)
        try:
            raw = Decimal(row["OBS_VALUE"]) if row["OBS_VALUE"] else None
            if raw is not None and not raw.is_finite():
                raise InvalidOperation
        except InvalidOperation:
            raise RuntimeError("OECD invalid numeric observation") from None
        # Retain source percent, precision and suppression/missingness. The
        # compatibility value is fractional growth, matching the old ABI.
        normalized = raw / 100 if raw is not None and query.units == "growth" else raw
        records.append({**row, "date": observed, "country": CODE_TO_COUNTRY_GDP_FORECAST.get(row["REF_AREA"], row["REF_AREA"]).replace("_", " ").title(),
                        "value": float(normalized) if normalized is not None else None,
                        "source_value": row["OBS_VALUE"], "source_vintage_version": version[1]})
    if not records:
        raise EmptyDataError("OECD validated forecast response contains no observations in the query window")
    return pa.Table.from_pylist(records).replace_schema_metadata({b"historical_point_in_time": b"false",
                b"source": b"OECD Economic Outlook latest edition; version retained per row",
                b"value_contract": b"value: growth/100 or native level; source_value/OBS_VALUE: original decimal text"})
