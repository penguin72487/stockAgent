from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import math
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import polars as pl
import pyarrow.parquet as pq
from tqdm import tqdm

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from common import (  # noqa: E402
    SharedRateLimiter,
    provider_rate_limit,
    retry_delay_seconds,
)
from artifact_io import (  # noqa: E402
    atomic_write_bytes as _atomic_bytes,
    atomic_write_json as _atomic_json,
    atomic_write_parquet as _atomic_parquet,
    sha256_bytes as _sha256_bytes,
    sha256_file as _sha256_path,
)


SCHEMA_VERSION = 1
OBSERVATION_COLUMNS = (
    "source",
    "dataset",
    "entity",
    "metric",
    "event_ts_utc",
    "published_at_utc",
    "observed_at_utc",
    "available_at_utc",
    "value_float",
    "value_text",
    "unit",
    "point_in_time_state",
    "raw_sha256",
)


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    dataset: str
    source: str
    provider_profile: str
    method: str
    url: str
    body: dict[str, Any] | None
    adapter: str
    point_in_time_state: str = "prospective_snapshot"


@dataclass(slots=True)
class DatasetResult:
    dataset: str
    source: str
    status: str
    observations_added: int
    observations_total: int
    entities: int
    raw_path: str | None
    raw_bytes: int
    raw_sha256: str | None
    observed_at_utc: str
    message: str | None = None


DATASETS: tuple[DatasetSpec, ...] = (
    DatasetSpec(
        "defillama_chains",
        "DefiLlama",
        "defillama_public",
        "GET",
        "https://api.llama.fi/v2/chains",
        None,
        "defillama_chains",
    ),
    DatasetSpec(
        "defillama_stablecoins",
        "DefiLlama",
        "defillama_public",
        "GET",
        "https://stablecoins.llama.fi/stablecoins?includePrices=true",
        None,
        "defillama_stablecoins",
    ),
    DatasetSpec(
        "defillama_yields",
        "DefiLlama",
        "defillama_public",
        "GET",
        "https://yields.llama.fi/pools",
        None,
        "defillama_yields",
    ),
    DatasetSpec(
        "defillama_dex_volume",
        "DefiLlama",
        "defillama_public",
        "GET",
        "https://api.llama.fi/overview/dexs?excludeTotalDataChart=false&excludeTotalDataChartBreakdown=true&dataType=dailyVolume",
        None,
        "defillama_dex_volume",
        "historical_archive_first_observed_now",
    ),
    DatasetSpec(
        "defillama_options_notional_volume",
        "DefiLlama",
        "defillama_public",
        "GET",
        "https://api.llama.fi/overview/options?excludeTotalDataChart=false&excludeTotalDataChartBreakdown=true&dataType=dailyNotionalVolume",
        None,
        "defillama_options_notional_volume",
        "historical_archive_first_observed_now",
    ),
    DatasetSpec(
        "defillama_open_interest",
        "DefiLlama",
        "defillama_public",
        "GET",
        "https://api.llama.fi/overview/open-interest?excludeTotalDataChart=false&excludeTotalDataChartBreakdown=true&dataType=openInterestAtEnd",
        None,
        "defillama_open_interest",
        "historical_archive_first_observed_now",
    ),
    DatasetSpec(
        "defillama_protocol_fees",
        "DefiLlama",
        "defillama_public",
        "GET",
        "https://api.llama.fi/overview/fees?excludeTotalDataChart=false&excludeTotalDataChartBreakdown=true&dataType=dailyFees",
        None,
        "defillama_protocol_fees",
        "historical_archive_first_observed_now",
    ),
    DatasetSpec(
        "defillama_protocol_revenue",
        "DefiLlama",
        "defillama_public",
        "GET",
        "https://api.llama.fi/overview/fees?excludeTotalDataChart=false&excludeTotalDataChartBreakdown=true&dataType=dailyRevenue",
        None,
        "defillama_protocol_revenue",
        "historical_archive_first_observed_now",
    ),
    DatasetSpec(
        "hyperliquid_perp_context",
        "Hyperliquid",
        "hyperliquid_info",
        "POST",
        "https://api.hyperliquid.xyz/info",
        {"type": "metaAndAssetCtxs"},
        "hyperliquid_perp",
    ),
    DatasetSpec(
        "hyperliquid_spot_context",
        "Hyperliquid",
        "hyperliquid_info",
        "POST",
        "https://api.hyperliquid.xyz/info",
        {"type": "spotMetaAndAssetCtxs"},
        "hyperliquid_spot",
    ),
    DatasetSpec(
        "hyperliquid_predicted_fundings",
        "Hyperliquid",
        "hyperliquid_info",
        "POST",
        "https://api.hyperliquid.xyz/info",
        {"type": "predictedFundings"},
        "hyperliquid_predicted_fundings",
    ),
    DatasetSpec(
        "deribit_btc_options",
        "Deribit",
        "deribit_public",
        "GET",
        "https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency=BTC&kind=option",
        None,
        "deribit_book_summary",
    ),
    DatasetSpec(
        "deribit_eth_options",
        "Deribit",
        "deribit_public",
        "GET",
        "https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency=ETH&kind=option",
        None,
        "deribit_book_summary",
    ),
    DatasetSpec(
        "deribit_btc_futures",
        "Deribit",
        "deribit_public",
        "GET",
        "https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency=BTC&kind=future",
        None,
        "deribit_book_summary",
    ),
    DatasetSpec(
        "deribit_eth_futures",
        "Deribit",
        "deribit_public",
        "GET",
        "https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency=ETH&kind=future",
        None,
        "deribit_book_summary",
    ),
    DatasetSpec(
        "coinmetrics_community_catalog",
        "Coin Metrics Community",
        "coinmetrics_community",
        "GET",
        "https://community-api.coinmetrics.io/v4/catalog-v2/asset-metrics",
        None,
        "coinmetrics_catalog",
        "revisioned_catalog_snapshot",
    ),
    DatasetSpec(
        "coinbase_exchange_products",
        "Coinbase Exchange",
        "coinbase_exchange_public",
        "GET",
        "https://api.exchange.coinbase.com/products",
        None,
        "coinbase_products",
    ),
    DatasetSpec(
        "kraken_asset_pairs",
        "Kraken",
        "kraken_public",
        "GET",
        "https://api.kraken.com/0/public/AssetPairs",
        None,
        "kraken_asset_pairs",
    ),
    DatasetSpec(
        "bitfinex_all_tickers",
        "Bitfinex",
        "bitfinex_public",
        "GET",
        "https://api-pub.bitfinex.com/v2/tickers?symbols=ALL",
        None,
        "bitfinex_tickers",
    ),
    DatasetSpec(
        "alternative_me_fear_greed",
        "Alternative.me",
        "alternative_me_public",
        "GET",
        "https://api.alternative.me/fng/?limit=0&format=json",
        None,
        "alternative_me_fear_greed",
        "historical_archive_first_observed_now",
    ),
    DatasetSpec(
        "bitcoin_mempool_fees",
        "mempool.space",
        "mempool_space_public",
        "GET",
        "https://mempool.space/api/v1/fees/recommended",
        None,
        "mempool_fees",
    ),
    DatasetSpec(
        "bitcoin_mempool_state",
        "mempool.space",
        "mempool_space_public",
        "GET",
        "https://mempool.space/api/mempool",
        None,
        "mempool_state",
    ),
    DatasetSpec(
        "bitcoin_difficulty_adjustment",
        "mempool.space",
        "mempool_space_public",
        "GET",
        "https://mempool.space/api/v1/difficulty-adjustment",
        None,
        "mempool_difficulty",
    ),
    DatasetSpec(
        "bitcoin_hashrate_history",
        "mempool.space",
        "mempool_space_public",
        "GET",
        "https://mempool.space/api/v1/mining/hashrate/3y",
        None,
        "mempool_hashrate",
        "historical_archive_first_observed_now",
    ),
    DatasetSpec(
        "blockscout_ethereum_gas",
        "Blockscout Ethereum",
        "blockscout_ethereum_public",
        "GET",
        "https://eth.blockscout.com/api/v2/stats",
        None,
        "blockscout_ethereum_gas",
    ),
    DatasetSpec(
        "blockscout_ethereum_latest_block",
        "Blockscout Ethereum",
        "blockscout_ethereum_public",
        "GET",
        "https://eth.blockscout.com/api/v2/blocks?type=block",
        None,
        "blockscout_ethereum_latest_block",
    ),
)

# Historical adapters remain readable, but the active exchange acquisition
# boundary is Binance, OKX and Bybit through their dedicated one-minute jobs.
DEFERRED_EXCHANGE_DATASETS = frozenset(
    {
        "hyperliquid_perp_context",
        "hyperliquid_spot_context",
        "hyperliquid_predicted_fundings",
        "deribit_btc_options",
        "deribit_eth_options",
        "deribit_btc_futures",
        "deribit_eth_futures",
        "coinbase_exchange_products",
        "kraken_asset_pairs",
        "bitfinex_all_tickers",
    }
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Capture compact anonymous market context as append-only raw snapshots "
            "and a causal long-form observation table."
        )
    )
    parser.add_argument("--output-dir", default="data_free_public")
    parser.add_argument(
        "--datasets",
        nargs="*",
        default=None,
        help="Optional dataset IDs; the default captures every registered compact source.",
    )
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--retry-base", type=float, default=0.8)
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help=(
            "Concurrent dataset workers across independent providers. Requests "
            "for the same provider still share its rate limiter."
        ),
    )
    return parser.parse_args()


def _iso_from_ms(value: Any) -> str | None:
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    try:
        return datetime.fromtimestamp(number / 1000, tz=timezone.utc).isoformat()
    except (OSError, OverflowError, ValueError):
        return None


def _iso_from_seconds(value: Any) -> str | None:
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    try:
        return datetime.fromtimestamp(number, tz=timezone.utc).isoformat()
    except (OSError, OverflowError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value in {None, ""} or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _merge_manifest_results(
    previous: Any,
    current: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Preserve unselected endpoint receipts during a targeted refresh."""
    registered_order = {spec.dataset: index for index, spec in enumerate(DATASETS)}
    merged: dict[str, dict[str, Any]] = {}
    if isinstance(previous, dict):
        for item in previous.get("results", []):
            if not isinstance(item, dict):
                continue
            dataset = str(item.get("dataset") or "")
            if dataset in registered_order:
                merged[dataset] = dict(item)
    for item in current:
        dataset = str(item.get("dataset") or "")
        if dataset in registered_order:
            merged[dataset] = dict(item)
    return sorted(
        merged.values(), key=lambda item: registered_order[str(item["dataset"])]
    )


class PublicClient:
    def __init__(self, *, max_retries: int, retry_base: float) -> None:
        self.max_retries = max(0, int(max_retries))
        self.retry_base = max(0.1, float(retry_base))
        self.limiters = {
            name: SharedRateLimiter(profile.interval_seconds, name=name)
            for name in {spec.provider_profile for spec in DATASETS}
            for profile in [provider_rate_limit(name)]
        }

    def request(
        self, spec: DatasetSpec, *, follow_pagination: bool = True
    ) -> tuple[Any, bytes]:
        limiter = self.limiters[spec.provider_profile]
        body = (
            json.dumps(spec.body, separators=(",", ":")).encode("utf-8")
            if spec.body is not None
            else None
        )
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            limiter.wait()
            request = Request(
                spec.url,
                data=body,
                method=spec.method,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "User-Agent": "stockAgent/1",
                },
            )
            try:
                with urlopen(request, timeout=60) as response:
                    payload = response.read()
                decoded = json.loads(payload)
                if (
                    follow_pagination
                    and spec.adapter == "coinmetrics_catalog"
                    and isinstance(decoded, dict)
                ):
                    combined = list(decoded.get("data") or [])
                    next_url = decoded.get("next_page_url")
                    seen_urls: set[str] = set()
                    while next_url:
                        next_text = str(next_url)
                        if next_text in seen_urls:
                            raise RuntimeError(
                                "Coin Metrics catalog pagination repeated next_page_url"
                            )
                        seen_urls.add(next_text)
                        page_spec = DatasetSpec(
                            dataset=spec.dataset,
                            source=spec.source,
                            provider_profile=spec.provider_profile,
                            method="GET",
                            url=next_text,
                            body=None,
                            adapter=spec.adapter,
                            point_in_time_state=spec.point_in_time_state,
                        )
                        page, _ = self.request(page_spec, follow_pagination=False)
                        if not isinstance(page, dict):
                            raise RuntimeError(
                                "Coin Metrics catalog page is not an object"
                            )
                        combined.extend(page.get("data") or [])
                        next_url = page.get("next_page_url")
                    decoded = {
                        "data": combined,
                        "page_count": len(seen_urls) + 1,
                        "pagination_complete": True,
                    }
                    payload = json.dumps(
                        decoded, ensure_ascii=False, separators=(",", ":")
                    ).encode("utf-8")
                return decoded, payload
            except HTTPError as exc:
                last_error = exc
                if (
                    exc.code in {408, 418, 429, 500, 502, 503, 504}
                    and attempt < self.max_retries
                ):
                    limiter.defer(
                        retry_delay_seconds(
                            attempt,
                            base=self.retry_base,
                            retry_after=exc.headers.get("Retry-After"),
                        )
                    )
                    continue
                raise
            except (URLError, TimeoutError, json.JSONDecodeError) as exc:
                last_error = exc
                if attempt < self.max_retries:
                    limiter.defer(retry_delay_seconds(attempt, base=self.retry_base))
                    continue
                raise
        if last_error is not None:
            raise last_error
        raise RuntimeError("public request failed without an explicit error")


def _observation(
    spec: DatasetSpec,
    observed_at: str,
    raw_sha256: str,
    *,
    entity: str,
    metric: str,
    value: Any,
    unit: str | None = None,
    event_ts: str | None = None,
    published_at: str | None = None,
    value_text: str | None = None,
) -> dict[str, Any] | None:
    numeric = _float_or_none(value)
    text = value_text
    if numeric is None and text is None and value not in {None, ""}:
        text = str(value)
    if numeric is None and text is None:
        return None
    published = published_at or event_ts
    available = observed_at
    if published:
        try:
            published_dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
            observed_dt = datetime.fromisoformat(observed_at.replace("Z", "+00:00"))
            available = max(published_dt, observed_dt).isoformat()
        except ValueError:
            available = observed_at
    return {
        "source": spec.source,
        "dataset": spec.dataset,
        "entity": str(entity),
        "metric": str(metric),
        "event_ts_utc": event_ts,
        "published_at_utc": published,
        "observed_at_utc": observed_at,
        "available_at_utc": available,
        "value_float": numeric,
        "value_text": text,
        "unit": unit,
        "point_in_time_state": spec.point_in_time_state,
        "raw_sha256": raw_sha256,
    }


def _metrics(
    spec: DatasetSpec,
    observed_at: str,
    raw_sha256: str,
    *,
    entity: str,
    values: dict[str, tuple[Any, str | None]],
    event_ts: str | None = None,
) -> list[dict[str, Any]]:
    return [
        row
        for metric, (value, unit) in values.items()
        if (
            row := _observation(
                spec,
                observed_at,
                raw_sha256,
                entity=entity,
                metric=metric,
                value=value,
                unit=unit,
                event_ts=event_ts,
            )
        )
        is not None
    ]


def _adapt_defillama_chains(
    spec: DatasetSpec, payload: Any, observed_at: str, digest: str
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for item in payload if isinstance(payload, list) else []:
        if not isinstance(item, dict):
            continue
        entity = str(item.get("name") or item.get("gecko_id") or "").strip()
        if not entity:
            continue
        output.extend(
            _metrics(
                spec,
                observed_at,
                digest,
                entity=entity,
                values={
                    "tvl_usd": (item.get("tvl"), "USD"),
                    "chain_id": (item.get("chainId"), "id"),
                    "token_symbol": (item.get("tokenSymbol"), None),
                    "gecko_id": (item.get("gecko_id"), None),
                },
            )
        )
    return output


def _adapt_defillama_stablecoins(
    spec: DatasetSpec, payload: Any, observed_at: str, digest: str
) -> list[dict[str, Any]]:
    assets = payload.get("peggedAssets", []) if isinstance(payload, dict) else []
    output: list[dict[str, Any]] = []
    for item in assets:
        if not isinstance(item, dict):
            continue
        entity = str(item.get("symbol") or item.get("name") or item.get("id") or "")
        peg_type = str(item.get("pegType") or "")

        def nested(name: str) -> Any:
            value = item.get(name)
            return value.get(peg_type) if isinstance(value, dict) else None

        output.extend(
            _metrics(
                spec,
                observed_at,
                digest,
                entity=entity,
                values={
                    "circulating": (nested("circulating"), peg_type or None),
                    "circulating_prev_day": (
                        nested("circulatingPrevDay"),
                        peg_type or None,
                    ),
                    "circulating_prev_week": (
                        nested("circulatingPrevWeek"),
                        peg_type or None,
                    ),
                    "circulating_prev_month": (
                        nested("circulatingPrevMonth"),
                        peg_type or None,
                    ),
                    "price": (item.get("price"), "USD"),
                    "peg_type": (peg_type, None),
                    "peg_mechanism": (item.get("pegMechanism"), None),
                },
            )
        )
    return output


def _adapt_defillama_yields(
    spec: DatasetSpec, payload: Any, observed_at: str, digest: str
) -> list[dict[str, Any]]:
    pools = payload.get("data", []) if isinstance(payload, dict) else []
    output: list[dict[str, Any]] = []
    for item in pools:
        if not isinstance(item, dict):
            continue
        entity = str(item.get("pool") or "").strip()
        if not entity:
            continue
        output.extend(
            _metrics(
                spec,
                observed_at,
                digest,
                entity=entity,
                values={
                    "tvl_usd": (item.get("tvlUsd"), "USD"),
                    "apy_pct": (item.get("apy"), "percent"),
                    "apy_base_pct": (item.get("apyBase"), "percent"),
                    "apy_reward_pct": (item.get("apyReward"), "percent"),
                    "apy_change_1d_pct": (item.get("apyPct1D"), "percentage_point"),
                    "apy_change_7d_pct": (item.get("apyPct7D"), "percentage_point"),
                    "apy_change_30d_pct": (item.get("apyPct30D"), "percentage_point"),
                    "chain": (item.get("chain"), None),
                    "project": (item.get("project"), None),
                    "symbol": (item.get("symbol"), None),
                    "stablecoin": (str(item.get("stablecoin")), None),
                    "exposure": (item.get("exposure"), None),
                    "il_risk": (item.get("ilRisk"), None),
                },
            )
        )
    return output


def _adapt_defillama_overview(
    spec: DatasetSpec,
    payload: Any,
    observed_at: str,
    digest: str,
    *,
    history_metric: str,
) -> list[dict[str, Any]]:
    """Normalize one overview without treating today's snapshot as old history.

    DefiLlama returns a global daily history and a current per-protocol ranking in
    the same response.  Every row deliberately retains the retrieval vintage;
    callers may only use it after ``observed_at`` until publication vintages are
    independently proven.
    """

    if not isinstance(payload, dict):
        return []
    output: list[dict[str, Any]] = []
    chart = payload.get("totalDataChart")
    for point in chart if isinstance(chart, list) else []:
        if not isinstance(point, list | tuple) or len(point) < 2:
            continue
        event_ts = _iso_from_seconds(point[0])
        if event_ts is None:
            continue
        row = _observation(
            spec,
            observed_at,
            digest,
            entity="all",
            metric=history_metric,
            value=point[1],
            unit="USD",
            event_ts=event_ts,
        )
        if row is not None:
            output.append(row)

    protocols = payload.get("protocols")
    for item in protocols if isinstance(protocols, list) else []:
        if not isinstance(item, dict):
            continue
        entity = str(
            item.get("slug")
            or item.get("id")
            or item.get("defillamaId")
            or item.get("name")
            or ""
        ).strip()
        if not entity:
            continue
        chains = item.get("chains")
        output.extend(
            _metrics(
                spec,
                observed_at,
                digest,
                entity=entity,
                values={
                    "name": (item.get("displayName") or item.get("name"), None),
                    "category": (item.get("category"), None),
                    "chains_json": (
                        json.dumps(chains, ensure_ascii=False, separators=(",", ":"))
                        if isinstance(chains, list)
                        else None,
                        None,
                    ),
                    "total_24h_usd": (item.get("total24h"), "USD"),
                    "total_7d_usd": (item.get("total7d"), "USD"),
                    "total_30d_usd": (item.get("total30d"), "USD"),
                    "total_all_time_usd": (item.get("totalAllTime"), "USD"),
                    "change_1d_pct": (item.get("change_1d"), "percent"),
                    "change_7d_pct": (item.get("change_7d"), "percent"),
                    "change_1m_pct": (item.get("change_1m"), "percent"),
                },
            )
        )
    return output


def _adapt_defillama_dex_volume(
    spec: DatasetSpec, payload: Any, observed_at: str, digest: str
) -> list[dict[str, Any]]:
    return _adapt_defillama_overview(
        spec,
        payload,
        observed_at,
        digest,
        history_metric="daily_dex_volume_usd",
    )


def _adapt_defillama_options_notional_volume(
    spec: DatasetSpec, payload: Any, observed_at: str, digest: str
) -> list[dict[str, Any]]:
    return _adapt_defillama_overview(
        spec,
        payload,
        observed_at,
        digest,
        history_metric="daily_options_notional_volume_usd",
    )


def _adapt_defillama_open_interest(
    spec: DatasetSpec, payload: Any, observed_at: str, digest: str
) -> list[dict[str, Any]]:
    return _adapt_defillama_overview(
        spec,
        payload,
        observed_at,
        digest,
        history_metric="open_interest_at_end_usd",
    )


def _adapt_defillama_protocol_fees(
    spec: DatasetSpec, payload: Any, observed_at: str, digest: str
) -> list[dict[str, Any]]:
    return _adapt_defillama_overview(
        spec,
        payload,
        observed_at,
        digest,
        history_metric="daily_protocol_fees_usd",
    )


def _adapt_defillama_protocol_revenue(
    spec: DatasetSpec, payload: Any, observed_at: str, digest: str
) -> list[dict[str, Any]]:
    return _adapt_defillama_overview(
        spec,
        payload,
        observed_at,
        digest,
        history_metric="daily_protocol_revenue_usd",
    )


def _adapt_hyperliquid_perp(
    spec: DatasetSpec, payload: Any, observed_at: str, digest: str
) -> list[dict[str, Any]]:
    if not isinstance(payload, list) or len(payload) < 2:
        return []
    meta, contexts = payload[0], payload[1]
    universe = meta.get("universe", []) if isinstance(meta, dict) else []
    output: list[dict[str, Any]] = []
    for instrument, context in zip(universe, contexts, strict=False):
        if not isinstance(instrument, dict) or not isinstance(context, dict):
            continue
        entity = str(instrument.get("name") or "").strip()
        if not entity:
            continue
        output.extend(
            _metrics(
                spec,
                observed_at,
                digest,
                entity=entity,
                values={
                    "mark_price": (context.get("markPx"), "USD"),
                    "oracle_price": (context.get("oraclePx"), "USD"),
                    "mid_price": (context.get("midPx"), "USD"),
                    "funding_rate": (context.get("funding"), "fraction"),
                    "open_interest": (context.get("openInterest"), "base_asset"),
                    "day_notional_volume": (context.get("dayNtlVlm"), "USD"),
                    "premium": (context.get("premium"), "fraction"),
                    "prev_day_price": (context.get("prevDayPx"), "USD"),
                    "max_leverage": (instrument.get("maxLeverage"), "multiple"),
                    "size_decimals": (instrument.get("szDecimals"), "digits"),
                    "is_delisted": (str(bool(instrument.get("isDelisted"))), None),
                },
            )
        )
    return output


def _adapt_hyperliquid_spot(
    spec: DatasetSpec, payload: Any, observed_at: str, digest: str
) -> list[dict[str, Any]]:
    if not isinstance(payload, list) or len(payload) < 2:
        return []
    meta, contexts = payload[0], payload[1]
    universe = meta.get("universe", []) if isinstance(meta, dict) else []
    output: list[dict[str, Any]] = []
    for instrument, context in zip(universe, contexts, strict=False):
        if not isinstance(instrument, dict) or not isinstance(context, dict):
            continue
        entity = str(instrument.get("name") or f"@{instrument.get('index')}")
        output.extend(
            _metrics(
                spec,
                observed_at,
                digest,
                entity=entity,
                values={
                    "mark_price": (context.get("markPx"), "USD"),
                    "mid_price": (context.get("midPx"), "USD"),
                    "day_base_volume": (context.get("dayBaseVlm"), "base_asset"),
                    "day_notional_volume": (context.get("dayNtlVlm"), "USD"),
                    "prev_day_price": (context.get("prevDayPx"), "USD"),
                    "circulating_supply": (
                        context.get("circulatingSupply"),
                        "base_asset",
                    ),
                },
            )
        )
    return output


def _adapt_hyperliquid_predicted_fundings(
    spec: DatasetSpec, payload: Any, observed_at: str, digest: str
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for row in payload if isinstance(payload, list) else []:
        if not isinstance(row, list) or len(row) < 2:
            continue
        coin, venues = str(row[0]), row[1]
        for venue_row in venues if isinstance(venues, list) else []:
            if not isinstance(venue_row, list) or len(venue_row) < 2:
                continue
            venue, values = str(venue_row[0]), venue_row[1]
            if not isinstance(values, dict):
                continue
            output.extend(
                _metrics(
                    spec,
                    observed_at,
                    digest,
                    entity=f"{coin}:{venue}",
                    values={
                        "predicted_funding_rate": (
                            values.get("fundingRate"),
                            "fraction",
                        ),
                        "funding_interval_hours": (
                            values.get("fundingIntervalHours"),
                            "hours",
                        ),
                        "next_funding_time_utc": (
                            _iso_from_ms(values.get("nextFundingTime")),
                            None,
                        ),
                    },
                )
            )
    return output


def _adapt_deribit_book_summary(
    spec: DatasetSpec, payload: Any, observed_at: str, digest: str
) -> list[dict[str, Any]]:
    results = payload.get("result", []) if isinstance(payload, dict) else []
    output: list[dict[str, Any]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        entity = str(item.get("instrument_name") or "").strip()
        if not entity:
            continue
        event_ts = _iso_from_ms(item.get("creation_timestamp"))
        output.extend(
            _metrics(
                spec,
                observed_at,
                digest,
                entity=entity,
                event_ts=event_ts,
                values={
                    "bid_price": (item.get("bid_price"), "base_currency"),
                    "ask_price": (item.get("ask_price"), "base_currency"),
                    "mid_price": (item.get("mid_price"), "base_currency"),
                    "mark_price": (item.get("mark_price"), "base_currency"),
                    "last_price": (item.get("last"), "base_currency"),
                    "open_interest": (item.get("open_interest"), "contract"),
                    "volume": (item.get("volume"), "contract"),
                    "volume_usd": (item.get("volume_usd"), "USD"),
                    "mark_iv": (item.get("mark_iv"), "percent"),
                    "underlying_price": (item.get("underlying_price"), "USD"),
                    "estimated_delivery_price": (
                        item.get("estimated_delivery_price"),
                        "USD",
                    ),
                    "price_change": (item.get("price_change"), "fraction"),
                },
            )
        )
    return output


def _adapt_coinmetrics_catalog(
    spec: DatasetSpec, payload: Any, observed_at: str, digest: str
) -> list[dict[str, Any]]:
    assets = payload.get("data", []) if isinstance(payload, dict) else []
    output: list[dict[str, Any]] = []
    for asset in assets:
        if not isinstance(asset, dict):
            continue
        asset_id = str(asset.get("asset") or "").strip()
        if not asset_id:
            continue
        for metric in asset.get("metrics", []):
            if not isinstance(metric, dict):
                continue
            metric_id = str(metric.get("metric") or "").strip()
            for frequency in metric.get("frequencies", []):
                if (
                    not isinstance(frequency, dict)
                    or frequency.get("community") is not True
                ):
                    continue
                frequency_id = str(frequency.get("frequency") or "")
                entity = f"{asset_id}:{metric_id}:{frequency_id}"
                output.extend(
                    _metrics(
                        spec,
                        observed_at,
                        digest,
                        entity=entity,
                        values={
                            "community_available": ("true", None),
                            "min_time": (frequency.get("min_time"), None),
                            "max_time": (frequency.get("max_time"), None),
                        },
                    )
                )
    return output


def _adapt_coinbase_products(
    spec: DatasetSpec, payload: Any, observed_at: str, digest: str
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for item in payload if isinstance(payload, list) else []:
        if not isinstance(item, dict):
            continue
        entity = str(item.get("id") or "").strip()
        if not entity:
            continue
        output.extend(
            _metrics(
                spec,
                observed_at,
                digest,
                entity=entity,
                values={
                    "base_currency": (item.get("base_currency"), None),
                    "quote_currency": (item.get("quote_currency"), None),
                    "base_increment": (item.get("base_increment"), "base_asset"),
                    "quote_increment": (item.get("quote_increment"), "quote_asset"),
                    "base_min_size": (item.get("base_min_size"), "base_asset"),
                    "base_max_size": (item.get("base_max_size"), "base_asset"),
                    "min_market_funds": (item.get("min_market_funds"), "quote_asset"),
                    "max_market_funds": (item.get("max_market_funds"), "quote_asset"),
                    "status": (item.get("status"), None),
                    "product_type": (item.get("product_type"), None),
                    "trading_disabled": (str(bool(item.get("trading_disabled"))), None),
                    "cancel_only": (str(bool(item.get("cancel_only"))), None),
                    "limit_only": (str(bool(item.get("limit_only"))), None),
                    "post_only": (str(bool(item.get("post_only"))), None),
                    "auction_mode": (str(bool(item.get("auction_mode"))), None),
                },
            )
        )
    return output


def _adapt_kraken_asset_pairs(
    spec: DatasetSpec, payload: Any, observed_at: str, digest: str
) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    errors = payload.get("error") or []
    if errors:
        raise RuntimeError(f"Kraken AssetPairs error: {errors}")
    result = payload.get("result") or {}
    output: list[dict[str, Any]] = []
    for pair_id, item in result.items() if isinstance(result, dict) else []:
        if not isinstance(item, dict):
            continue
        entity = str(pair_id).strip()
        if not entity:
            continue
        output.extend(
            _metrics(
                spec,
                observed_at,
                digest,
                entity=entity,
                values={
                    "alt_name": (item.get("altname"), None),
                    "websocket_name": (item.get("wsname"), None),
                    "base_asset": (item.get("base"), None),
                    "quote_asset": (item.get("quote"), None),
                    "pair_decimals": (item.get("pair_decimals"), "digits"),
                    "cost_decimals": (item.get("cost_decimals"), "digits"),
                    "lot_decimals": (item.get("lot_decimals"), "digits"),
                    "lot_multiplier": (item.get("lot_multiplier"), "multiple"),
                    "order_min": (item.get("ordermin"), "base_asset"),
                    "cost_min": (item.get("costmin"), "quote_asset"),
                    "tick_size": (item.get("tick_size"), "quote_asset"),
                    "margin_call_pct": (item.get("margin_call"), "percent"),
                    "margin_stop_pct": (item.get("margin_stop"), "percent"),
                    "status": (item.get("status"), None),
                },
            )
        )
    return output


def _adapt_bitfinex_tickers(
    spec: DatasetSpec, payload: Any, observed_at: str, digest: str
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for item in payload if isinstance(payload, list) else []:
        if not isinstance(item, list) or len(item) < 11:
            continue
        entity = str(item[0]).strip()
        if not entity:
            continue
        if entity.startswith("f"):
            if len(item) < 14:
                continue
            event_ts = _iso_from_ms(item[17]) if len(item) > 17 else None
            values = {
                "flash_return_rate": (item[1], "fraction_per_day"),
                "bid_rate": (item[2], "fraction_per_day"),
                "bid_period_days": (item[3], "day"),
                "bid_size": (item[4], "currency"),
                "ask_rate": (item[5], "fraction_per_day"),
                "ask_period_days": (item[6], "day"),
                "ask_size": (item[7], "currency"),
                "daily_change": (item[8], "fraction_per_day"),
                "daily_change_fraction": (item[9], "fraction"),
                "last_rate": (item[10], "fraction_per_day"),
                "daily_volume": (item[11], "currency"),
                "daily_high": (item[12], "fraction_per_day"),
                "daily_low": (item[13], "fraction_per_day"),
                "available_amount": (
                    item[16] if len(item) > 16 else None,
                    "currency",
                ),
            }
        else:
            event_ts = _iso_from_ms(item[11]) if len(item) > 11 else None
            values = {
                "bid": (item[1], "quote_asset"),
                "bid_size": (item[2], "base_asset"),
                "ask": (item[3], "quote_asset"),
                "ask_size": (item[4], "base_asset"),
                "daily_change": (item[5], "quote_asset"),
                "daily_change_fraction": (item[6], "fraction"),
                "last_price": (item[7], "quote_asset"),
                "daily_volume": (item[8], "base_asset"),
                "daily_high": (item[9], "quote_asset"),
                "daily_low": (item[10], "quote_asset"),
            }
        output.extend(
            _metrics(
                spec,
                observed_at,
                digest,
                entity=entity,
                event_ts=event_ts,
                values=values,
            )
        )
    return output


def _adapt_alternative_me_fear_greed(
    spec: DatasetSpec, payload: Any, observed_at: str, digest: str
) -> list[dict[str, Any]]:
    rows = payload.get("data", []) if isinstance(payload, dict) else []
    output: list[dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        event_ts = _iso_from_seconds(item.get("timestamp"))
        if not event_ts:
            continue
        output.extend(
            _metrics(
                spec,
                observed_at,
                digest,
                entity="BTC",
                event_ts=event_ts,
                values={
                    "fear_greed_index": (item.get("value"), "index_0_100"),
                    "classification": (item.get("value_classification"), None),
                },
            )
        )
    return output


def _adapt_mempool_fees(
    spec: DatasetSpec, payload: Any, observed_at: str, digest: str
) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    return _metrics(
        spec,
        observed_at,
        digest,
        entity="bitcoin",
        values={
            "fastest_fee": (payload.get("fastestFee"), "sat/vB"),
            "half_hour_fee": (payload.get("halfHourFee"), "sat/vB"),
            "hour_fee": (payload.get("hourFee"), "sat/vB"),
            "economy_fee": (payload.get("economyFee"), "sat/vB"),
            "minimum_fee": (payload.get("minimumFee"), "sat/vB"),
        },
    )


def _adapt_mempool_state(
    spec: DatasetSpec, payload: Any, observed_at: str, digest: str
) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    histogram = payload.get("fee_histogram") or []
    histogram_text = json.dumps(histogram, separators=(",", ":")) if histogram else None
    return _metrics(
        spec,
        observed_at,
        digest,
        entity="bitcoin",
        values={
            "unconfirmed_tx_count": (payload.get("count"), "transaction"),
            "virtual_size": (payload.get("vsize"), "vbyte"),
            "total_fee": (payload.get("total_fee"), "satoshi"),
            "fee_histogram": (histogram_text, "json"),
        },
    )


def _adapt_mempool_difficulty(
    spec: DatasetSpec, payload: Any, observed_at: str, digest: str
) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    return _metrics(
        spec,
        observed_at,
        digest,
        entity="bitcoin",
        values={
            "progress_pct": (payload.get("progressPercent"), "percent"),
            "estimated_change_pct": (payload.get("difficultyChange"), "percent"),
            "estimated_retarget_time": (
                _iso_from_ms(payload.get("estimatedRetargetDate")),
                None,
            ),
            "remaining_blocks": (payload.get("remainingBlocks"), "block"),
            "remaining_milliseconds": (payload.get("remainingTime"), "millisecond"),
            "previous_retarget_factor": (payload.get("previousRetarget"), "multiple"),
            "next_retarget_height": (payload.get("nextRetargetHeight"), "block_height"),
            "average_block_milliseconds": (payload.get("timeAvg"), "millisecond"),
            "adjusted_block_milliseconds": (
                payload.get("adjustedTimeAvg"),
                "millisecond",
            ),
        },
    )


def _adapt_mempool_hashrate(
    spec: DatasetSpec, payload: Any, observed_at: str, digest: str
) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    output = _metrics(
        spec,
        observed_at,
        digest,
        entity="bitcoin:current",
        values={
            "hashrate": (payload.get("currentHashrate"), "hash/second"),
            "difficulty": (payload.get("currentDifficulty"), "difficulty"),
        },
    )
    for item in payload.get("hashrates", []):
        if not isinstance(item, dict):
            continue
        event_ts = _iso_from_seconds(item.get("timestamp"))
        if event_ts:
            output.extend(
                _metrics(
                    spec,
                    observed_at,
                    digest,
                    entity="bitcoin:network",
                    event_ts=event_ts,
                    values={"hashrate": (item.get("avgHashrate"), "hash/second")},
                )
            )
    for item in payload.get("difficulty", []):
        if not isinstance(item, dict):
            continue
        event_ts = _iso_from_seconds(item.get("time"))
        if event_ts:
            output.extend(
                _metrics(
                    spec,
                    observed_at,
                    digest,
                    entity="bitcoin:network",
                    event_ts=event_ts,
                    values={
                        "difficulty": (item.get("difficulty"), "difficulty"),
                        "height": (item.get("height"), "block_height"),
                    },
                )
            )
    return output


def _adapt_blockscout_ethereum_gas(
    spec: DatasetSpec, payload: Any, observed_at: str, digest: str
) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    gas_prices = payload.get("gas_prices")
    gas_prices = gas_prices if isinstance(gas_prices, dict) else {}
    event_ts = str(payload.get("gas_price_updated_at") or "").strip() or None
    return _metrics(
        spec,
        observed_at,
        digest,
        entity="ethereum:1",
        event_ts=event_ts,
        values={
            "gas_price_slow_gwei": (gas_prices.get("slow"), "gwei"),
            "gas_price_average_gwei": (gas_prices.get("average"), "gwei"),
            "gas_price_fast_gwei": (gas_prices.get("fast"), "gwei"),
            "static_gas_price_gwei": (payload.get("static_gas_price"), "gwei"),
            "gas_prices_update_in_ms": (payload.get("gas_prices_update_in"), "ms"),
            "network_utilization_pct": (
                payload.get("network_utilization_percentage"),
                "percent",
            ),
            "transactions_today": (payload.get("transactions_today"), "transaction"),
            "gas_used_today": (payload.get("gas_used_today"), "gas"),
        },
    )


def _adapt_blockscout_ethereum_latest_block(
    spec: DatasetSpec, payload: Any, observed_at: str, digest: str
) -> list[dict[str, Any]]:
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list) or not items or not isinstance(items[0], dict):
        return []
    block = items[0]
    event_ts = str(block.get("timestamp") or "").strip() or None
    base_fee_wei = _float_or_none(block.get("base_fee_per_gas"))
    return _metrics(
        spec,
        observed_at,
        digest,
        entity="ethereum:1",
        event_ts=event_ts,
        values={
            "block_height": (block.get("height"), "block"),
            "base_fee_per_gas_wei": (base_fee_wei, "wei"),
            "base_fee_per_gas_gwei": (
                base_fee_wei / 1_000_000_000 if base_fee_wei is not None else None,
                "gwei",
            ),
            "gas_used": (block.get("gas_used"), "gas"),
            "gas_limit": (block.get("gas_limit"), "gas"),
            "gas_used_pct": (block.get("gas_used_percentage"), "percent"),
            "burnt_fees_wei": (block.get("burnt_fees"), "wei"),
            "priority_fee_wei": (block.get("priority_fee"), "wei"),
        },
    )


ADAPTERS: dict[str, Callable[[DatasetSpec, Any, str, str], list[dict[str, Any]]]] = {
    "defillama_chains": _adapt_defillama_chains,
    "defillama_stablecoins": _adapt_defillama_stablecoins,
    "defillama_yields": _adapt_defillama_yields,
    "defillama_dex_volume": _adapt_defillama_dex_volume,
    "defillama_options_notional_volume": _adapt_defillama_options_notional_volume,
    "defillama_open_interest": _adapt_defillama_open_interest,
    "defillama_protocol_fees": _adapt_defillama_protocol_fees,
    "defillama_protocol_revenue": _adapt_defillama_protocol_revenue,
    "hyperliquid_perp": _adapt_hyperliquid_perp,
    "hyperliquid_spot": _adapt_hyperliquid_spot,
    "hyperliquid_predicted_fundings": _adapt_hyperliquid_predicted_fundings,
    "deribit_book_summary": _adapt_deribit_book_summary,
    "coinmetrics_catalog": _adapt_coinmetrics_catalog,
    "coinbase_products": _adapt_coinbase_products,
    "kraken_asset_pairs": _adapt_kraken_asset_pairs,
    "bitfinex_tickers": _adapt_bitfinex_tickers,
    "alternative_me_fear_greed": _adapt_alternative_me_fear_greed,
    "mempool_fees": _adapt_mempool_fees,
    "mempool_state": _adapt_mempool_state,
    "mempool_difficulty": _adapt_mempool_difficulty,
    "mempool_hashrate": _adapt_mempool_hashrate,
    "blockscout_ethereum_gas": _adapt_blockscout_ethereum_gas,
    "blockscout_ethereum_latest_block": _adapt_blockscout_ethereum_latest_block,
}


def _empty_observations() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "source": pl.String,
            "dataset": pl.String,
            "entity": pl.String,
            "metric": pl.String,
            "event_ts_utc": pl.String,
            "published_at_utc": pl.String,
            "observed_at_utc": pl.String,
            "available_at_utc": pl.String,
            "value_float": pl.Float64,
            "value_text": pl.String,
            "unit": pl.String,
            "point_in_time_state": pl.String,
            "raw_sha256": pl.String,
        }
    )


def _append_observations(path: Path, rows: list[dict[str, Any]]) -> tuple[int, int]:
    fresh = (
        pl.DataFrame(rows, infer_schema_length=None).select(OBSERVATION_COLUMNS)
        if rows
        else _empty_observations()
    )
    existing = pl.read_parquet(path) if path.is_file() else _empty_observations()
    if fresh.is_empty():
        return 0, existing.height
    combined = (
        pl.concat([existing, fresh], how="diagonal_relaxed")
        .unique(
            subset=[
                "dataset",
                "entity",
                "metric",
                "event_ts_utc",
                "observed_at_utc",
            ],
            keep="last",
            maintain_order=True,
        )
        .sort(["observed_at_utc", "dataset", "entity", "metric"])
    )
    added = max(0, combined.height - existing.height)
    _atomic_parquet(path, combined)
    return added, combined.height


def _capture_dataset(
    client: PublicClient,
    spec: DatasetSpec,
    output_dir: Path,
) -> tuple[DatasetResult, list[dict[str, Any]]]:
    observed = datetime.now(timezone.utc)
    observed_at = observed.isoformat()
    payload, raw = client.request(spec)
    digest = _sha256_bytes(raw)
    snapshot_name = observed.strftime("%Y%m%dT%H%M%S.%fZ.json")
    raw_path = (
        output_dir
        / "raw"
        / spec.dataset
        / observed.strftime("%Y")
        / observed.strftime("%m")
        / snapshot_name
    )
    _atomic_bytes(raw_path, raw)
    rows = ADAPTERS[spec.adapter](spec, payload, observed_at, digest)
    unique_rows = {
        (
            str(row["dataset"]),
            str(row["entity"]),
            str(row["metric"]),
            str(row["event_ts_utc"]),
            str(row["observed_at_utc"]),
        ): row
        for row in rows
    }
    rows = list(unique_rows.values())
    entities = len({str(row["entity"]) for row in rows})
    status = "updated" if rows else "unavailable_empty"
    return (
        DatasetResult(
            dataset=spec.dataset,
            source=spec.source,
            status=status,
            observations_added=len(rows),
            observations_total=0,
            entities=entities,
            raw_path=str(raw_path),
            raw_bytes=len(raw),
            raw_sha256=digest,
            observed_at_utc=observed_at,
            message=None if rows else "Source returned no normalizable observations.",
        ),
        rows,
    )


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    selected_ids = set(args.datasets or [])
    unknown = selected_ids - {spec.dataset for spec in DATASETS}
    if unknown:
        raise ValueError(f"unknown free-public datasets: {sorted(unknown)}")
    deferred_requested = selected_ids & DEFERRED_EXCHANGE_DATASETS
    if deferred_requested:
        raise ValueError(
            "exchange scope is limited to Binance, OKX and Bybit; disabled datasets: "
            f"{sorted(deferred_requested)}"
        )
    selected = [
        spec
        for spec in DATASETS
        if spec.dataset not in DEFERRED_EXCHANGE_DATASETS
        and (not selected_ids or spec.dataset in selected_ids)
    ]
    observations_path = output_dir / "observations.parquet"
    progress_path = output_dir / "progress.json"
    client = PublicClient(max_retries=args.max_retries, retry_base=args.retry_base)
    started = datetime.now(timezone.utc)
    results: list[DatasetResult] = []
    pending_rows: list[dict[str, Any]] = []

    def write_progress(state: str) -> None:
        observed = datetime.now(timezone.utc)
        completed = len(results)
        elapsed = max(0.001, (observed - started).total_seconds())
        rate = completed / elapsed if completed else 0.0
        remaining = len(selected) - completed
        eta_seconds = int(math.ceil(remaining / rate)) if rate > 0 else None
        _atomic_json(
            progress_path,
            {
                "schema_version": SCHEMA_VERSION,
                "state": state,
                "label": "免費公開市場脈絡",
                "current": completed,
                "total": len(selected),
                "unit": "dataset",
                "ratio": completed / len(selected) if selected else 1.0,
                "started_at_utc": started.isoformat(),
                "updated_at_utc": observed.isoformat(),
                "elapsed_seconds": elapsed,
                "items_per_second": rate,
                "remaining_seconds": eta_seconds,
                "estimated_complete_at_utc": (
                    (observed + timedelta(seconds=eta_seconds)).isoformat()
                    if eta_seconds is not None
                    else None
                ),
                "status_counts": {
                    status: sum(item.status == status for item in results)
                    for status in sorted({item.status for item in results})
                },
                "basis": "completed datasets divided by full elapsed time; large catalog pages can change the estimate",
            },
        )

    write_progress("running")
    with tqdm(
        total=len(selected), desc="download:free-public", unit="dataset"
    ) as progress:
        with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as executor:
            futures = {
                executor.submit(_capture_dataset, client, spec, output_dir): spec
                for spec in selected
            }
            for future in as_completed(futures):
                spec = futures[future]
                try:
                    result, rows = future.result()
                    pending_rows.extend(rows)
                except Exception as exc:
                    result = DatasetResult(
                        dataset=spec.dataset,
                        source=spec.source,
                        status="failed",
                        observations_added=0,
                        observations_total=(
                            int(pq.ParquetFile(observations_path).metadata.num_rows)
                            if observations_path.is_file()
                            else 0
                        ),
                        entities=0,
                        raw_path=None,
                        raw_bytes=0,
                        raw_sha256=None,
                        observed_at_utc=datetime.now(timezone.utc).isoformat(),
                        message=f"{type(exc).__name__}: {exc}",
                    )
                results.append(result)
                progress.update(1)
                progress.set_postfix(
                    updated=sum(item.status == "updated" for item in results),
                    failed=sum(item.status == "failed" for item in results),
                    refresh=False,
                )
                write_progress("running")

    result_order = {spec.dataset: index for index, spec in enumerate(selected)}
    results.sort(key=lambda item: result_order[item.dataset])

    observations_added, observations_total = _append_observations(
        observations_path, pending_rows
    )
    expected_added = sum(result.observations_added for result in results)
    if observations_added != expected_added:
        raise RuntimeError(
            "normalized observation key collision: "
            f"expected {expected_added} additions but persisted {observations_added}"
        )
    for result in results:
        result.observations_total = observations_total

    ended = datetime.now(timezone.utc)
    status_counts: dict[str, int] = {}
    for result in results:
        status_counts[result.status] = status_counts.get(result.status, 0) + 1
    manifest_path = output_dir / "download_manifest.json"
    summary_path = output_dir / "download_summary.json"
    receipt_path = output_dir / "download_receipt.json"
    previous_manifest: Any = {}
    if manifest_path.is_file():
        try:
            previous_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            previous_manifest = {}
    current_result_rows = [asdict(result) for result in results]
    manifest_results = _merge_manifest_results(previous_manifest, current_result_rows)
    manifest_status_counts = {
        status: sum(str(item.get("status")) == status for item in manifest_results)
        for status in sorted({str(item.get("status")) for item in manifest_results})
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": ended.isoformat(),
        "causal_clock": {
            "event_ts": "source event timestamp when supplied",
            "published_at": "source publication timestamp when supplied",
            "observed_at": "first successful local retrieval timestamp",
            "available_at": "max(published_at, observed_at)",
            "snapshot_rule": "prospective only; never project a present snapshot backward",
            "revision_rule": "retain every raw snapshot and observed_at vintage",
        },
        "run_scope": {
            "selected_dataset_ids": [spec.dataset for spec in selected],
            "selected_count": len(selected),
            "registered_count": len(DATASETS),
        },
        "status_counts": manifest_status_counts,
        "results": manifest_results,
    }
    _atomic_json(manifest_path, manifest)
    summary = {
        "asset_class": "free_public_market_context",
        "dataset_count": len(manifest_results),
        "selected_dataset_count": len(selected),
        "registered_dataset_count": len(DATASETS),
        "status_counts": manifest_status_counts,
        "current_run_status_counts": status_counts,
        "row_count": (
            int(pq.ParquetFile(observations_path).metadata.num_rows)
            if observations_path.is_file()
            else 0
        ),
        "rows_added": observations_added,
        "entities_observed": sum(result.entities for result in results),
        "started_at_utc": started.isoformat(),
        "ended_at_utc": ended.isoformat(),
        "elapsed_seconds": (ended - started).total_seconds(),
        "end_date": ended.date().isoformat(),
        "observations_path": str(observations_path),
        "manifest_path": str(manifest_path),
        "rate_limits": {
            name: {
                "requests_per_second": provider_rate_limit(name).requests_per_second,
                "basis": provider_rate_limit(name).basis,
            }
            for name in sorted(client.limiters)
        },
    }
    _atomic_json(summary_path, summary)
    artifacts = [manifest_path, summary_path]
    if observations_path.is_file():
        artifacts.append(observations_path)
    receipt = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": ended.isoformat(),
        "artifacts": {
            path.name: {
                "bytes": path.stat().st_size,
                "sha256": _sha256_path(path),
            }
            for path in artifacts
        },
        "status_counts": status_counts,
    }
    _atomic_json(receipt_path, receipt)
    write_progress("failed" if status_counts.get("failed") else "complete")
    print(f"[free-public] observations -> {observations_path}")
    print(f"[free-public] manifest -> {manifest_path}")
    print(f"[free-public] summary -> {summary_path}")
    print(f"[free-public] receipt -> {receipt_path}")
    print(f"[free-public] done: {json.dumps(summary, ensure_ascii=False)}")
    if status_counts.get("failed"):
        raise RuntimeError(
            f"free public context incomplete: {status_counts['failed']} datasets failed"
        )


if __name__ == "__main__":
    main()
