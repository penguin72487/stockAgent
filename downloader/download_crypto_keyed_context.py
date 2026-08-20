from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import fcntl
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import sys
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import polars as pl
import pyarrow.parquet as pq


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from common import (  # noqa: E402
    SharedRateLimiter,
    load_env_file,
    provider_rate_limit,
    retry_delay_seconds,
)


SCHEMA_VERSION = 1
UTC = timezone.utc


@dataclass(slots=True)
class DatasetResult:
    dataset: str
    provider: str
    status: str
    rows: int
    requests: int
    observed_at_utc: str
    output_path: str | None = None
    raw_path: str | None = None
    message: str | None = None


@dataclass(slots=True)
class ProviderState:
    provider_id: str
    provider: str
    credential_id: str | None
    credential_state: str
    operational_state: str
    entitlement_state: str
    rate_profile: str
    requests: int
    checked_at_utc: str
    message: str
    quota: dict[str, Any]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Capture non-duplicated crypto reference facts from configured APIs. "
            "CoinGecko is canonical; CoinMarketCap remains fallback/QA only."
        )
    )
    parser.add_argument("--output-dir", default="data_crypto_reference")
    parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="Credential file; defaults to the repository .env and never overrides exported variables.",
    )
    parser.add_argument("--max-retries", type=int, default=4)
    parser.add_argument("--retry-base", type=float, default=0.8)
    parser.add_argument(
        "--coingecko-max-market-pages",
        type=int,
        default=0,
        help="0 fetches every market page; a positive value is intended only for tests/smoke runs.",
    )
    parser.add_argument(
        "--allow-coinmarketcap-fallback",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Use CMC asset mapping only when the canonical CoinGecko catalog fails.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore cadence receipts. Provider quotas and rate limits still apply.",
    )
    return parser.parse_args()


def _atomic_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(payload)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_json(path: Path, payload: Any) -> None:
    _atomic_bytes(
        path,
        (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8"),
    )


def _atomic_parquet(path: Path, frame: pl.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        pq.write_table(
            frame.to_arrow(), temporary, compression="zstd", write_statistics=True
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _iso_now() -> str:
    return datetime.now(UTC).isoformat()


def _parse_time(value: Any) -> datetime | None:
    if value in {None, ""}:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _dataset_state_path(output_dir: Path, dataset: str) -> Path:
    return output_dir / "state" / "datasets" / f"{dataset}.json"


def _provider_state_path(output_dir: Path, provider_id: str) -> Path:
    return output_dir / "state" / "providers" / f"{provider_id}.json"


def _read_recent_provider_state(
    output_dir: Path, provider_id: str, cadence_seconds: int
) -> ProviderState | None:
    path = _provider_state_path(output_dir, provider_id)
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        checked = _parse_time(payload.get("checked_at_utc"))
        if checked is None or (datetime.now(UTC) - checked).total_seconds() >= cadence_seconds:
            return None
        payload["requests"] = 0
        payload["message"] = "Provider capability receipt is current; no duplicate probe was sent."
        return ProviderState(**payload)
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError):
        return None


def _write_provider_state(output_dir: Path, state: ProviderState) -> None:
    _atomic_json(_provider_state_path(output_dir, state.provider_id), asdict(state))


def _dataset_due(
    output_dir: Path, dataset: str, cadence_seconds: int, *, force: bool
) -> bool:
    if force:
        return True
    path = _dataset_state_path(output_dir, dataset)
    if not path.is_file():
        return True
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return True
    latest = _parse_time(payload.get("last_success_at_utc"))
    return latest is None or (datetime.now(UTC) - latest).total_seconds() >= cadence_seconds


def _record_dataset_success(
    output_dir: Path, result: DatasetResult, *, canonical_fact_key: list[str]
) -> None:
    _atomic_json(
        _dataset_state_path(output_dir, result.dataset),
        {
            "schema_version": SCHEMA_VERSION,
            "dataset": result.dataset,
            "provider": result.provider,
            "last_success_at_utc": result.observed_at_utc,
            "rows": result.rows,
            "output_path": result.output_path,
            "raw_path": result.raw_path,
            "canonical_fact_key": canonical_fact_key,
        },
    )


def _cached_result(output_dir: Path, dataset: str, provider: str) -> DatasetResult:
    state_path = _dataset_state_path(output_dir, dataset)
    payload: dict[str, Any] = {}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        pass
    return DatasetResult(
        dataset=dataset,
        provider=provider,
        status="current_cached",
        rows=int(payload.get("rows") or 0),
        requests=0,
        observed_at_utc=str(payload.get("last_success_at_utc") or _iso_now()),
        output_path=payload.get("output_path"),
        raw_path=payload.get("raw_path"),
        message="Cadence receipt is current; no duplicate request was sent.",
    )


def _snapshot_paths(
    output_dir: Path, dataset: str, observed: datetime
) -> tuple[Path, Path, Path]:
    stem = observed.strftime("%Y%m%dT%H%M%S.%fZ")
    root = output_dir / "snapshots" / dataset / observed.strftime("%Y/%m/%d")
    return root / f"{stem}.parquet", root / f"{stem}.json.gz", output_dir / "latest" / f"{dataset}.json"


def _persist_snapshot(
    output_dir: Path,
    *,
    dataset: str,
    provider: str,
    observed: datetime,
    payload: Any,
    frame: pl.DataFrame,
    requests: int,
    canonical_fact_key: list[str],
) -> DatasetResult:
    parquet_path, raw_path, latest_path = _snapshot_paths(output_dir, dataset, observed)
    serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
        "utf-8"
    )
    digest = hashlib.sha256(serialized).hexdigest()
    _atomic_bytes(raw_path, gzip.compress(serialized, compresslevel=6, mtime=0))
    enriched = frame.with_columns(
        pl.lit(observed.isoformat()).alias("observed_at_utc"),
        pl.lit(observed.isoformat()).alias("available_at_utc"),
        pl.lit(digest).alias("raw_sha256"),
    )
    _atomic_parquet(parquet_path, enriched)
    result = DatasetResult(
        dataset=dataset,
        provider=provider,
        status="updated",
        rows=enriched.height,
        requests=requests,
        observed_at_utc=observed.isoformat(),
        output_path=str(parquet_path),
        raw_path=str(raw_path),
    )
    _atomic_json(
        latest_path,
        {
            "schema_version": SCHEMA_VERSION,
            "dataset": dataset,
            "provider": provider,
            "observed_at_utc": result.observed_at_utc,
            "rows": result.rows,
            "parquet_path": result.output_path,
            "raw_path": result.raw_path,
            "raw_sha256": digest,
            "canonical_fact_key": canonical_fact_key,
        },
    )
    _record_dataset_success(
        output_dir, result, canonical_fact_key=canonical_fact_key
    )
    return result


class HttpClient:
    def __init__(self, *, max_retries: int, retry_base: float) -> None:
        self.max_retries = max(0, int(max_retries))
        self.retry_base = max(0.1, float(retry_base))
        self.limiters: dict[str, SharedRateLimiter] = {}

    def _limiter(self, profile_name: str) -> SharedRateLimiter:
        limiter = self.limiters.get(profile_name)
        if limiter is None:
            profile = provider_rate_limit(profile_name)
            limiter = SharedRateLimiter(profile.interval_seconds, name=profile_name)
            self.limiters[profile_name] = limiter
        return limiter

    def get_json(
        self,
        url: str,
        *,
        profile_name: str,
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
    ) -> tuple[Any, dict[str, str]]:
        if params:
            url = f"{url}?{urlencode(params)}"
        limiter = self._limiter(profile_name)
        request_headers = {
            "Accept": "application/json",
            "Accept-Encoding": "gzip",
            "User-Agent": "stockAgent/crypto-reference-v1",
            **(headers or {}),
        }
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            limiter.wait()
            request = Request(url, headers=request_headers)
            try:
                with urlopen(request, timeout=45) as response:
                    raw = response.read()
                    if str(response.headers.get("Content-Encoding") or "").lower() == "gzip":
                        raw = gzip.decompress(raw)
                    decoded = json.loads(raw)
                    return decoded, {str(k): str(v) for k, v in response.headers.items()}
            except HTTPError as exc:
                last_error = exc
                if exc.code in {408, 418, 429, 500, 502, 503, 504} and attempt < self.max_retries:
                    limiter.defer(
                        retry_delay_seconds(
                            attempt,
                            base=self.retry_base,
                            retry_after=exc.headers.get("Retry-After"),
                        )
                    )
                    continue
                raise
            except (URLError, TimeoutError, json.JSONDecodeError, gzip.BadGzipFile) as exc:
                last_error = exc
                if attempt < self.max_retries:
                    limiter.defer(retry_delay_seconds(attempt, base=self.retry_base))
                    continue
                raise
        if last_error is not None:
            raise last_error
        raise RuntimeError("request failed without an explicit error")


def _coingecko_catalog_frame(payload: Any) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for item in payload if isinstance(payload, list) else []:
        if not isinstance(item, dict) or not item.get("id"):
            continue
        platforms = item.get("platforms") if isinstance(item.get("platforms"), dict) else {}
        rows.append(
            {
                "asset_id": str(item["id"]),
                "symbol": str(item.get("symbol") or ""),
                "name": str(item.get("name") or ""),
                "platforms_json": json.dumps(platforms, ensure_ascii=False, sort_keys=True),
            }
        )
    if not rows:
        return pl.DataFrame(
            schema={
                "asset_id": pl.String,
                "symbol": pl.String,
                "name": pl.String,
                "platforms_json": pl.String,
            }
        )
    return pl.DataFrame(rows).unique("asset_id", keep="last").sort("asset_id")


def _coingecko_market_frame(payload: Any) -> pl.DataFrame:
    fields = (
        "current_price",
        "market_cap",
        "market_cap_rank",
        "fully_diluted_valuation",
        "total_volume",
        "high_24h",
        "low_24h",
        "price_change_24h",
        "price_change_percentage_24h",
        "market_cap_change_24h",
        "market_cap_change_percentage_24h",
        "circulating_supply",
        "total_supply",
        "max_supply",
        "ath",
        "ath_change_percentage",
        "atl",
        "atl_change_percentage",
    )
    rows: list[dict[str, Any]] = []
    for item in payload if isinstance(payload, list) else []:
        if not isinstance(item, dict) or not item.get("id"):
            continue
        row: dict[str, Any] = {
            "asset_id": str(item["id"]),
            "symbol": str(item.get("symbol") or ""),
            "name": str(item.get("name") or ""),
            "quote_currency": "USD",
            "source_updated_at_utc": item.get("last_updated"),
            "ath_date_utc": item.get("ath_date"),
            "atl_date_utc": item.get("atl_date"),
        }
        for field in fields:
            value = item.get(field)
            if value is None:
                row[field] = None
                continue
            try:
                number = float(value)
            except (TypeError, ValueError, OverflowError):
                number = None
            row[field] = number if number is not None and math.isfinite(number) else None
        rows.append(row)
    if not rows:
        return pl.DataFrame({"asset_id": []}, schema={"asset_id": pl.String})
    return pl.DataFrame(rows, infer_schema_length=None).unique(
        "asset_id", keep="last"
    ).sort(["market_cap_rank", "asset_id"], nulls_last=True)


def _coingecko_global_frame(payload: Any) -> pl.DataFrame:
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return pl.DataFrame({"entity": []}, schema={"entity": pl.String})
    market_cap = data.get("total_market_cap") or {}
    volume = data.get("total_volume") or {}
    percentages = data.get("market_cap_percentage") or {}
    return pl.DataFrame(
        [
            {
                "entity": "crypto_global",
                "active_cryptocurrencies": data.get("active_cryptocurrencies"),
                "markets": data.get("markets"),
                "total_market_cap_usd": market_cap.get("usd"),
                "total_volume_usd": volume.get("usd"),
                "btc_dominance_pct": percentages.get("btc"),
                "eth_dominance_pct": percentages.get("eth"),
                "market_cap_change_percentage_24h_usd": data.get(
                    "market_cap_change_percentage_24h_usd"
                ),
                "source_updated_at_utc": (
                    datetime.fromtimestamp(int(data["updated_at"]), tz=UTC).isoformat()
                    if data.get("updated_at")
                    else None
                ),
            }
        ]
    )


def _fetch_coingecko(
    client: HttpClient,
    output_dir: Path,
    *,
    key: str,
    force: bool,
    max_market_pages: int,
) -> tuple[ProviderState, list[DatasetResult]]:
    checked_at = _iso_now()
    if not key:
        return (
            ProviderState(
                "coingecko",
                "CoinGecko Demo",
                "coingecko",
                "missing",
                "unavailable",
                "not_checked",
                "coingecko_demo",
                0,
                checked_at,
                "COINGECKO_DEMO_API_KEY is missing; anonymous fallback is reserved for recovery, not routine bulk polling.",
                {},
            ),
            [],
        )
    headers = {"x-cg-demo-api-key": key}
    results: list[DatasetResult] = []
    requests = 0
    try:
        if _dataset_due(output_dir, "coingecko_asset_catalog", 24 * 3600, force=force):
            catalog, _ = client.get_json(
                "https://api.coingecko.com/api/v3/coins/list",
                profile_name="coingecko_demo",
                headers=headers,
                params={"include_platform": "true"},
            )
            requests += 1
            result = _persist_snapshot(
                output_dir,
                dataset="coingecko_asset_catalog",
                provider="CoinGecko Demo",
                observed=datetime.now(UTC),
                payload=catalog,
                frame=_coingecko_catalog_frame(catalog),
                requests=1,
                canonical_fact_key=["asset_id", "observed_at_utc"],
            )
            results.append(result)
        else:
            results.append(
                _cached_result(output_dir, "coingecko_asset_catalog", "CoinGecko Demo")
            )

        if _dataset_due(output_dir, "coingecko_market_snapshot", 24 * 3600, force=force):
            markets: list[Any] = []
            page = 1
            while True:
                payload, _ = client.get_json(
                    "https://api.coingecko.com/api/v3/coins/markets",
                    profile_name="coingecko_demo",
                    headers=headers,
                    params={
                        "vs_currency": "usd",
                        "order": "market_cap_desc",
                        "per_page": "250",
                        "page": str(page),
                        "sparkline": "false",
                    },
                )
                requests += 1
                if not isinstance(payload, list):
                    raise RuntimeError("CoinGecko markets response is not a list")
                markets.extend(payload)
                if len(payload) < 250:
                    break
                if max_market_pages > 0 and page >= max_market_pages:
                    break
                page += 1
            result = _persist_snapshot(
                output_dir,
                dataset="coingecko_market_snapshot",
                provider="CoinGecko Demo",
                observed=datetime.now(UTC),
                payload=markets,
                frame=_coingecko_market_frame(markets),
                requests=page,
                canonical_fact_key=["asset_id", "source_updated_at_utc"],
            )
            results.append(result)
        else:
            results.append(
                _cached_result(output_dir, "coingecko_market_snapshot", "CoinGecko Demo")
            )

        if _dataset_due(output_dir, "coingecko_global_snapshot", 15 * 60, force=force):
            global_payload, _ = client.get_json(
                "https://api.coingecko.com/api/v3/global",
                profile_name="coingecko_demo",
                headers=headers,
            )
            requests += 1
            result = _persist_snapshot(
                output_dir,
                dataset="coingecko_global_snapshot",
                provider="CoinGecko Demo",
                observed=datetime.now(UTC),
                payload=global_payload,
                frame=_coingecko_global_frame(global_payload),
                requests=1,
                canonical_fact_key=["entity", "source_updated_at_utc"],
            )
            results.append(result)
        else:
            results.append(
                _cached_result(output_dir, "coingecko_global_snapshot", "CoinGecko Demo")
            )
    except Exception as exc:
        return (
            ProviderState(
                "coingecko",
                "CoinGecko Demo",
                "coingecko",
                "configured",
                "failed",
                "unknown",
                "coingecko_demo",
                requests,
                _iso_now(),
                f"{type(exc).__name__}: {exc}",
                {"monthly_call_cap": 10000, "minute_request_cap": 100},
            ),
            results,
        )
    return (
        ProviderState(
            "coingecko",
            "CoinGecko Demo",
            "coingecko",
            "configured",
            "operational",
            "entitled",
            "coingecko_demo",
            requests,
            _iso_now(),
            "Canonical asset identity and market snapshots are operational.",
            {"monthly_call_cap": 10000, "minute_request_cap": 100},
        ),
        results,
    )


def _fetch_coinmarketcap_key_info(
    client: HttpClient, output_dir: Path, *, key: str, force: bool
) -> ProviderState:
    checked_at = _iso_now()
    if not key:
        return ProviderState(
            "coinmarketcap",
            "CoinMarketCap Basic",
            "coinmarketcap",
            "missing",
            "unavailable",
            "not_checked",
            "coinmarketcap_basic",
            0,
            checked_at,
            "COINMARKETCAP_API_KEY is missing.",
            {},
        )
    if not force and (cached := _read_recent_provider_state(output_dir, "coinmarketcap", 3600)):
        return cached
    try:
        payload, _ = client.get_json(
            "https://pro-api.coinmarketcap.com/v1/key/info",
            profile_name="coinmarketcap_basic",
            headers={"X-CMC_PRO_API_KEY": key},
        )
        status = payload.get("status") if isinstance(payload, dict) else None
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(status, dict) or status.get("error_code") != 0 or not isinstance(data, dict):
            raise RuntimeError(str((status or {}).get("error_message") or "invalid key-info response"))
        plan = data.get("plan") if isinstance(data.get("plan"), dict) else {}
        usage = data.get("usage") if isinstance(data.get("usage"), dict) else {}
        current_month = usage.get("current_month") if isinstance(usage.get("current_month"), dict) else {}
        quota = {
            "rate_limit_minute": plan.get("rate_limit_minute"),
            "credit_limit_monthly": plan.get("credit_limit_monthly"),
            "credits_left_month": current_month.get("credits_left"),
        }
        credits_left = current_month.get("credits_left")
        entitled = credits_left is None or int(credits_left) > 0
        state = ProviderState(
            "coinmarketcap",
            "CoinMarketCap Basic",
            "coinmarketcap",
            "configured",
            "operational" if entitled else "quota_exhausted",
            "fallback_only" if entitled else "blocked",
            "coinmarketcap_basic",
            1,
            _iso_now(),
            "Operational identifier fallback; full market archive remains disabled to avoid duplication.",
            quota,
        )
    except Exception as exc:
        state = ProviderState(
            "coinmarketcap",
            "CoinMarketCap Basic",
            "coinmarketcap",
            "configured",
            "failed",
            "unknown",
            "coinmarketcap_basic",
            1,
            _iso_now(),
            f"{type(exc).__name__}: {exc}",
            {},
        )
    _write_provider_state(output_dir, state)
    return state


def _cmc_asset_map_frame(payload: Any) -> pl.DataFrame:
    data = payload.get("data") if isinstance(payload, dict) else None
    rows: list[dict[str, Any]] = []
    for item in data if isinstance(data, list) else []:
        if not isinstance(item, dict) or item.get("id") is None:
            continue
        platform = item.get("platform") if isinstance(item.get("platform"), dict) else {}
        rows.append(
            {
                "cmc_id": int(item["id"]),
                "name": str(item.get("name") or ""),
                "symbol": str(item.get("symbol") or ""),
                "slug": str(item.get("slug") or ""),
                "is_active": item.get("is_active"),
                "first_historical_data_utc": item.get("first_historical_data"),
                "last_historical_data_utc": item.get("last_historical_data"),
                "platform_name": platform.get("name"),
                "platform_token_address": platform.get("token_address"),
            }
        )
    if not rows:
        return pl.DataFrame({"cmc_id": []}, schema={"cmc_id": pl.Int64})
    return pl.DataFrame(rows, infer_schema_length=None).unique("cmc_id").sort("cmc_id")


def _fetch_cmc_asset_fallback(
    client: HttpClient, output_dir: Path, *, key: str
) -> DatasetResult:
    if not key:
        return DatasetResult(
            "coinmarketcap_asset_map_fallback",
            "CoinMarketCap Basic",
            "credential_missing",
            0,
            0,
            _iso_now(),
            message="Fallback was needed but COINMARKETCAP_API_KEY is missing.",
        )
    payload, _ = client.get_json(
        "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map",
        profile_name="coinmarketcap_basic",
        headers={"X-CMC_PRO_API_KEY": key},
        params={
            "listing_status": "active,inactive,untracked",
            "limit": "5000",
            "sort": "id",
            "aux": "platform,first_historical_data,last_historical_data,is_active,status",
        },
    )
    return _persist_snapshot(
        output_dir,
        dataset="coinmarketcap_asset_map_fallback",
        provider="CoinMarketCap Basic",
        observed=datetime.now(UTC),
        payload=payload,
        frame=_cmc_asset_map_frame(payload),
        requests=1,
        canonical_fact_key=["cmc_id", "observed_at_utc"],
    )


def _etherscan_frame(gas_payload: Any, supply_payload: Any | None) -> pl.DataFrame:
    gas = gas_payload.get("result") if isinstance(gas_payload, dict) else None
    row: dict[str, Any] = {"chain_id": 1}
    if isinstance(gas, dict):
        row.update(
            {
                "last_block": gas.get("LastBlock"),
                "safe_gas_price_gwei": gas.get("SafeGasPrice"),
                "proposed_gas_price_gwei": gas.get("ProposeGasPrice"),
                "fast_gas_price_gwei": gas.get("FastGasPrice"),
                "suggested_base_fee_gwei": gas.get("suggestBaseFee"),
                "gas_used_ratio_csv": gas.get("gasUsedRatio"),
            }
        )
    if isinstance(supply_payload, dict) and str(supply_payload.get("status")) == "1":
        try:
            # Wei totals exceed signed 64-bit and may be inferred by Arrow as
            # an int128 extension that Polars cannot ingest. Preserve the exact
            # integer as a canonical decimal string; consumers can choose their
            # own arbitrary-precision representation without float loss.
            row["ether_supply_wei"] = str(int(supply_payload.get("result")))
        except (TypeError, ValueError, OverflowError):
            row["ether_supply_wei"] = None
    return pl.DataFrame([row], infer_schema_length=None)


def _fetch_etherscan(
    client: HttpClient, output_dir: Path, *, key: str, force: bool
) -> tuple[ProviderState, list[DatasetResult]]:
    checked_at = _iso_now()
    if not key:
        return (
            ProviderState(
                "etherscan",
                "Etherscan V2",
                "etherscan",
                "missing",
                "unavailable",
                "not_checked",
                "etherscan_free",
                0,
                checked_at,
                "ETHERSCAN_API_KEY is missing; Coin Metrics daily metrics remain the fallback.",
                {},
            ),
            [],
        )
    if not force:
        blocked = _read_recent_provider_state(output_dir, "etherscan", 24 * 3600)
        if blocked is not None and blocked.operational_state in {
            "invalid_credential",
            "not_entitled",
        }:
            return blocked, []
    if not _dataset_due(output_dir, "etherscan_ethereum_state", 60, force=force):
        return (
            ProviderState(
                "etherscan",
                "Etherscan V2",
                "etherscan",
                "configured",
                "operational_cached",
                "entitled",
                "etherscan_free",
                0,
                checked_at,
                "Recent Ethereum state receipt avoids a duplicate request.",
                {"requests_per_second": 3, "calls_per_day": 100000},
            ),
            [_cached_result(output_dir, "etherscan_ethereum_state", "Etherscan V2")],
        )
    base = "https://api.etherscan.io/v2/api"
    try:
        gas_payload, _ = client.get_json(
            base,
            profile_name="etherscan_free",
            params={
                "chainid": "1",
                "module": "gastracker",
                "action": "gasoracle",
                "apikey": key,
            },
        )
        if str(gas_payload.get("status")) != "1":
            result_text = str(gas_payload.get("result") or gas_payload.get("message") or "NOTOK")
            state = "invalid_credential" if "invalid api key" in result_text.lower() else "not_entitled"
            state = ProviderState(
                    "etherscan",
                    "Etherscan V2",
                    "etherscan",
                    "configured",
                    state,
                    "blocked",
                    "etherscan_free",
                    1,
                    _iso_now(),
                    result_text,
                    {"requests_per_second": 3, "calls_per_day": 100000},
            )
            _write_provider_state(output_dir, state)
            return state, []
        supply_payload: Any | None = None
        requests = 1
        if _dataset_due(output_dir, "etherscan_ethereum_supply", 24 * 3600, force=force):
            supply_payload, _ = client.get_json(
                base,
                profile_name="etherscan_free",
                params={
                    "chainid": "1",
                    "module": "stats",
                    "action": "ethsupply",
                    "apikey": key,
                },
            )
            requests += 1
        result = _persist_snapshot(
            output_dir,
            dataset="etherscan_ethereum_state",
            provider="Etherscan V2",
            observed=datetime.now(UTC),
            payload={"gas_oracle": gas_payload, "supply": supply_payload},
            frame=_etherscan_frame(gas_payload, supply_payload),
            requests=requests,
            canonical_fact_key=["chain_id", "last_block", "observed_at_utc"],
        )
        if supply_payload is not None and str(supply_payload.get("status")) == "1":
            _atomic_json(
                _dataset_state_path(output_dir, "etherscan_ethereum_supply"),
                {
                    "schema_version": SCHEMA_VERSION,
                    "last_success_at_utc": result.observed_at_utc,
                    "rows": 1,
                    "output_path": result.output_path,
                },
            )
        state = ProviderState(
                "etherscan",
                "Etherscan V2",
                "etherscan",
                "configured",
                "operational",
                "entitled",
                "etherscan_free",
                requests,
                _iso_now(),
                "Ethereum gas state is operational; supply is refreshed daily.",
                {"requests_per_second": 3, "calls_per_day": 100000},
        )
        _write_provider_state(output_dir, state)
        return state, [result]
    except Exception as exc:
        state = ProviderState(
                "etherscan",
                "Etherscan V2",
                "etherscan",
                "configured",
                "failed",
                "unknown",
                "etherscan_free",
                1,
                _iso_now(),
                f"{type(exc).__name__}: {exc}",
                {},
        )
        _write_provider_state(output_dir, state)
        return state, []


def _fetch_coinglass_probe(
    client: HttpClient, output_dir: Path, *, key: str, force: bool
) -> ProviderState:
    checked_at = _iso_now()
    if not key:
        return ProviderState(
            "coinglass",
            "CoinGlass",
            "coinglass",
            "missing",
            "unavailable",
            "not_checked",
            "coinglass_keyed",
            0,
            checked_at,
            "COINGLASS_API_KEY is missing; exchange-native liquidation streams are the free fallback.",
            {},
        )
    if not force and (cached := _read_recent_provider_state(output_dir, "coinglass", 24 * 3600)):
        return cached
    try:
        payload, headers = client.get_json(
            "https://open-api-v4.coinglass.com/api/futures/supported-coins",
            profile_name="coinglass_keyed",
            headers={"CG-API-KEY": key},
        )
        entitled = str(payload.get("code")) == "0"
        message = str(payload.get("msg") or ("success" if entitled else "not entitled"))
        quota = {
            "api_key_max_limit": headers.get("API-KEY-MAX-LIMIT")
            or headers.get("Api-Key-Max-Limit"),
            "api_key_use_limit": headers.get("API-KEY-USE-LIMIT")
            or headers.get("Api-Key-Use-Limit"),
        }
        state = ProviderState(
            "coinglass",
            "CoinGlass",
            "coinglass",
            "configured",
            "operational" if entitled else "not_entitled",
            "unique_facts_only" if entitled else "blocked",
            "coinglass_keyed",
            1,
            _iso_now(),
            message,
            quota,
        )
    except Exception as exc:
        state = ProviderState(
            "coinglass",
            "CoinGlass",
            "coinglass",
            "configured",
            "failed",
            "unknown",
            "coinglass_keyed",
            1,
            _iso_now(),
            f"{type(exc).__name__}: {exc}",
            {},
        )
    _write_provider_state(output_dir, state)
    return state


def _dune_state(repo_root: Path, *, key: str) -> ProviderState:
    path = repo_root / "configs/dune_crypto_queries.json"
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        payload = {}
    queries = payload.get("queries") if isinstance(payload, dict) else None
    query_count = len(queries) if isinstance(queries, list) else 0
    if not key:
        credential_state = "missing"
        operational = "unavailable"
        entitlement = "not_checked"
        message = "DUNE_API_KEY is missing."
    elif query_count == 0:
        credential_state = "configured"
        operational = "waiting_query_contract"
        entitlement = "not_checked"
        message = "Key is configured, but no query ID/schema contract is registered; no credits were spent."
    else:
        credential_state = "configured"
        operational = "ready_for_query_runner"
        entitlement = "query_specific"
        message = f"{query_count} query contracts are registered."
    return ProviderState(
        "dune",
        "Dune",
        "dune",
        credential_state,
        operational,
        entitlement,
        "dune_free_low+dune_free_high",
        0,
        _iso_now(),
        message,
        {"registered_query_contracts": query_count},
    )


def _write_progress(
    path: Path,
    *,
    started: datetime,
    state: str,
    current: int,
    total: int,
    status_counts: dict[str, int],
) -> None:
    now = datetime.now(UTC)
    elapsed = max(0.001, (now - started).total_seconds())
    rate = current / elapsed if current else 0.0
    remaining = max(0, total - current)
    eta_seconds = int(math.ceil(remaining / rate)) if rate > 0 else None
    _atomic_json(
        path,
        {
            "schema_version": SCHEMA_VERSION,
            "state": state,
            "label": "加密資料唯一主來源與設定 API",
            "current": current,
            "total": total,
            "unit": "provider",
            "ratio": current / total if total else 1.0,
            "started_at_utc": started.isoformat(),
            "updated_at_utc": now.isoformat(),
            "elapsed_seconds": elapsed,
            "items_per_second": rate,
            "remaining_seconds": eta_seconds,
            "estimated_complete_at_utc": (
                (now + timedelta(seconds=eta_seconds)).isoformat()
                if eta_seconds is not None
                else None
            ),
            "status_counts": status_counts,
            "basis": "completed independent provider groups divided by elapsed wall time",
        },
    )


def main() -> None:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    load_env_file(
        args.env_file or repo_root / ".env",
        allowed_names={
            "COINGECKO_DEMO_API_KEY",
            "COINMARKETCAP_API_KEY",
            "ETHERSCAN_API_KEY",
            "COINGLASS_API_KEY",
            "DUNE_API_KEY",
        },
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    lock_handle = (output_dir / ".download.lock").open("a+")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print(
            f"[crypto-reference] another updater owns {output_dir / '.download.lock'}; "
            "skip duplicate run"
        )
        return
    client = HttpClient(max_retries=args.max_retries, retry_base=args.retry_base)
    started = datetime.now(UTC)
    progress_path = output_dir / "progress.json"
    providers: list[ProviderState] = []
    datasets: list[DatasetResult] = []
    tasks: dict[str, Callable[[], tuple[ProviderState, list[DatasetResult]]]] = {
        "coingecko": lambda: _fetch_coingecko(
            client,
            output_dir,
            key=os.getenv("COINGECKO_DEMO_API_KEY", "").strip(),
            force=args.force,
            max_market_pages=max(0, int(args.coingecko_max_market_pages)),
        ),
        "coinmarketcap": lambda: (
            _fetch_coinmarketcap_key_info(
                client,
                output_dir,
                key=os.getenv("COINMARKETCAP_API_KEY", "").strip(),
                force=args.force,
            ),
            [],
        ),
        "etherscan": lambda: _fetch_etherscan(
            client,
            output_dir,
            key=os.getenv("ETHERSCAN_API_KEY", "").strip(),
            force=args.force,
        ),
        "coinglass": lambda: (
            _fetch_coinglass_probe(
                client,
                output_dir,
                key=os.getenv("COINGLASS_API_KEY", "").strip(),
                force=args.force,
            ),
            [],
        ),
        "dune": lambda: (
            _dune_state(repo_root, key=os.getenv("DUNE_API_KEY", "").strip()),
            [],
        ),
    }
    _write_progress(
        progress_path,
        started=started,
        state="running",
        current=0,
        total=len(tasks),
        status_counts={},
    )
    with ThreadPoolExecutor(max_workers=len(tasks)) as executor:
        futures = {executor.submit(task): name for name, task in tasks.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                provider, provider_datasets = future.result()
            except Exception as exc:
                provider = ProviderState(
                    name,
                    name,
                    name,
                    "unknown",
                    "failed",
                    "unknown",
                    name,
                    0,
                    _iso_now(),
                    f"{type(exc).__name__}: {exc}",
                    {},
                )
                provider_datasets = []
            providers.append(provider)
            datasets.extend(provider_datasets)
            counts: dict[str, int] = {}
            for item in providers:
                counts[item.operational_state] = counts.get(item.operational_state, 0) + 1
            _write_progress(
                progress_path,
                started=started,
                state="running",
                current=len(providers),
                total=len(tasks),
                status_counts=counts,
            )

    by_id = {provider.provider_id: provider for provider in providers}
    coin_gecko = by_id.get("coingecko")
    if (
        args.allow_coinmarketcap_fallback
        and coin_gecko is not None
        and coin_gecko.operational_state != "operational"
    ):
        try:
            fallback = _fetch_cmc_asset_fallback(
                client,
                output_dir,
                key=os.getenv("COINMARKETCAP_API_KEY", "").strip(),
            )
        except Exception as exc:
            fallback = DatasetResult(
                "coinmarketcap_asset_map_fallback",
                "CoinMarketCap Basic",
                "failed",
                0,
                1,
                _iso_now(),
                message=f"{type(exc).__name__}: {exc}",
            )
        datasets.append(fallback)

    providers.sort(key=lambda item: item.provider_id)
    datasets.sort(key=lambda item: item.dataset)
    ended = datetime.now(UTC)
    provider_counts: dict[str, int] = {}
    for item in providers:
        provider_counts[item.operational_state] = provider_counts.get(item.operational_state, 0) + 1
    dataset_counts: dict[str, int] = {}
    for item in datasets:
        dataset_counts[item.status] = dataset_counts.get(item.status, 0) + 1
    source_status = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": ended.isoformat(),
        "secret_values_included": False,
        "canonical_registry": "configs/crypto_data_acquisition.json",
        "allocation_registry": "configs/crypto_source_allocation.json",
        "provider_state_counts": dict(sorted(provider_counts.items())),
        "dataset_state_counts": dict(sorted(dataset_counts.items())),
        "providers": [asdict(item) for item in providers],
        "datasets": [asdict(item) for item in datasets],
        "dedup_contract": (
            "Fallbacks fill missing canonical keys only. CoinGecko owns aggregate "
            "asset facts; CMC is not archived as a parallel full market source."
        ),
    }
    _atomic_json(output_dir / "source_status.json", source_status)
    summary = {
        "schema_version": SCHEMA_VERSION,
        "asset_class": "crypto_reference_nonduplicated",
        "provider_count": len(providers),
        "dataset_count": len(datasets),
        "provider_state_counts": dict(sorted(provider_counts.items())),
        "status_counts": dict(sorted(dataset_counts.items())),
        "row_count": sum(item.rows for item in datasets),
        "requests": sum(item.requests for item in providers),
        "started_at_utc": started.isoformat(),
        "ended_at_utc": ended.isoformat(),
        "elapsed_seconds": (ended - started).total_seconds(),
        "end_date": ended.date().isoformat(),
        "source_status_path": str(output_dir / "source_status.json"),
        "rate_limits": {
            name: {
                "requests_per_second": provider_rate_limit(name).requests_per_second,
                "basis": provider_rate_limit(name).basis,
            }
            for name in (
                "coingecko_demo",
                "coinmarketcap_basic",
                "coinglass_keyed",
                "etherscan_free",
                "dune_free_low",
                "dune_free_high",
            )
        },
    }
    _atomic_json(output_dir / "download_summary.json", summary)
    failed = any(item.operational_state == "failed" for item in providers)
    _write_progress(
        progress_path,
        started=started,
        state="failed" if failed else "complete",
        current=len(tasks),
        total=len(tasks),
        status_counts=provider_counts,
    )
    print(f"[crypto-reference] source status -> {output_dir / 'source_status.json'}")
    print(f"[crypto-reference] summary -> {output_dir / 'download_summary.json'}")
    print(
        "[crypto-reference] "
        f"providers={len(providers)} datasets={len(datasets)} "
        f"rows={summary['row_count']} requests={summary['requests']}"
    )


if __name__ == "__main__":
    main()
