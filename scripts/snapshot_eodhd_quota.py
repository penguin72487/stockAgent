"""Read EODHD's zero-cost usage endpoint; never fetch prices or spend extras."""
from __future__ import annotations

import argparse
from datetime import UTC, datetime
import json
import os
from pathlib import Path
from urllib.parse import urlencode

from downloader.artifact_io import atomic_write_json
from downloader.common import load_env_file
from downloader.download_keyed_public_catalogs import CatalogSpec, RequestBudget, SafeCatalogTransport

ROOT = Path(__file__).resolve().parents[1]
ENDPOINT = "https://eodhd.com/api/user"


def public_quota(payload: dict) -> dict:
    """Allowlist counters, never return the name/email/token/user payload."""
    if not isinstance(payload, dict):
        raise ValueError("invalid EODHD account response")
    result = {}
    for field in ("dailyRateLimit", "apiRequests", "extraLimit"):
        value = payload.get(field)
        if isinstance(value, bool) or not str(value).isdigit():
            raise ValueError("invalid EODHD quota counter")
        result[field] = int(value)
    day = datetime.strptime(str(payload.get("apiRequestsDate")), "%Y-%m-%d").date()
    # Use only a known free plan marker; arbitrary paid labels are unnecessary
    # for this capability check and may change without a contract update.
    result["subscription_type"] = "free" if payload.get("subscriptionType") == "free" else "other_or_unknown"
    result.update(apiRequestsDate=day.isoformat(), observed_at_utc=datetime.now(UTC).isoformat(),
                  quota_day_timezone="UTC", taipei_reset_clock="08:00",
                  counter_is_for_today=day == datetime.now(UTC).date(),
                  usage_endpoint_call_cost=0, extra_calls_auto_spend=False,
                  data_endpoints_verified=False, secret_values_included=False)
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=ROOT / ".env")
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts/credentials/eodhd_quota.json")
    args = parser.parse_args(argv)
    load_env_file(args.env_file, allowed_names=["EODHD_API_KEY"])
    key = os.environ.get("EODHD_API_KEY", "").strip()
    if not key:
        print("EODHD_API_KEY is not configured")
        return 2
    spec = CatalogSpec("eodhd", "account_usage", "EODHD_API_KEY", ENDPOINT,
                       "https://eodhd.com/financial-apis/api-limits", interval_seconds=1)
    try:
        transport = SafeCatalogTransport(spec, RequestBudget(1), maximum_bytes=64 * 1024, secrets=(key,))
        body = transport.fetch(ENDPOINT + "?" + urlencode({"api_token": key, "fmt": "json"}), {})
        receipt = public_quota(json.loads(body))
        atomic_write_json(args.output, receipt)
        print(json.dumps(receipt, ensure_ascii=False))
        return 0
    except Exception as exc:
        print(f"EODHD account check failed: {type(exc).__name__}; private details withheld")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
