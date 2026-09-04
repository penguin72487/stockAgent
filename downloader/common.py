from __future__ import annotations

from collections import deque
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import hashlib
import http.client
import ipaddress
import json
import math
import os
from pathlib import Path
from queue import Queue
import re
import socket
import threading
import tempfile
import time
from dataclasses import dataclass
from typing import TypeVar

try:
    from .artifact_io import atomic_write_text
except ImportError:  # direct ``python downloader/<script>.py`` execution
    from artifact_io import atomic_write_text

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback
    fcntl = None

from tqdm import tqdm

TItem = TypeVar("TItem")
TResult = TypeVar("TResult")


_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def load_env_file(
    path: str | Path,
    *,
    allowed_names: Iterable[str],
    override: bool = False,
) -> set[str]:
    """Load an allowlisted subset of a dotenv file without logging values.

    Existing process variables win by default.  The intentionally small parser
    accepts the ordinary ``NAME=value``, ``export NAME=value``, and matching
    single/double quoted forms used by this repository; it does not execute
    shell expansion.
    """

    target = Path(path)
    allowed = {str(name) for name in allowed_names if _ENV_NAME.fullmatch(str(name))}
    loaded: set[str] = set()
    if not target.is_file() or not allowed:
        return loaded
    try:
        lines = target.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        return loaded
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            continue
        name, raw_value = line.split("=", 1)
        name = name.strip()
        if name not in allowed or not _ENV_NAME.fullmatch(name):
            continue
        value = raw_value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        if not value or (not override and os.environ.get(name, "").strip()):
            continue
        os.environ[name] = value
        loaded.add(name)
    return loaded


class PersistentProgress:
    """Thread-safe, atomically published progress/ETA receipt for downloaders."""

    def __init__(
        self,
        path: str | Path,
        *,
        label: str,
        total: int,
        unit: str,
        basis: str,
        started_at: datetime | None = None,
    ) -> None:
        self.path = Path(path)
        self.label = str(label)
        self.total = max(0, int(total))
        self.unit = str(unit)
        self.basis = str(basis)
        self.started_at = started_at or datetime.now(timezone.utc)
        if self.started_at.tzinfo is None:
            self.started_at = self.started_at.replace(tzinfo=timezone.utc)
        self.current = 0
        self.status_counts: dict[str, int] = {}
        self.telemetry_counts: dict[str, int] = {}
        self.previous_run = self._load_previous_run()
        self._lock = threading.Lock()
        self._last_telemetry_publish = 0.0
        self._write("running", "initializing")

    def _load_previous_run(self) -> dict[str, object] | None:
        """Retain bounded evidence from an interrupted or completed prior run."""

        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            return None
        if not isinstance(payload, dict):
            return None
        keys = (
            "state",
            "phase",
            "current",
            "total",
            "unit",
            "ratio",
            "started_at_utc",
            "updated_at_utc",
            "status_counts",
            "telemetry_counts",
        )
        return {key: payload.get(key) for key in keys if key in payload}

    def _write(self, state: str, phase: str) -> None:
        observed = datetime.now(timezone.utc)
        elapsed = max(0.001, (observed - self.started_at).total_seconds())
        rate = self.current / elapsed if self.current else 0.0
        remaining = max(0, self.total - self.current)
        eta_seconds = int(math.ceil(remaining / rate)) if rate > 0 else None
        payload = {
            "schema_version": 1,
            "state": str(state),
            "label": self.label,
            "phase": str(phase),
            "current": self.current,
            "total": self.total,
            "unit": self.unit,
            "ratio": self.current / self.total if self.total else 1.0,
            "started_at_utc": self.started_at.isoformat(),
            "updated_at_utc": observed.isoformat(),
            "elapsed_seconds": elapsed,
            "items_per_second": rate,
            "remaining_seconds": eta_seconds,
            "estimated_complete_at_utc": (
                (observed + timedelta(seconds=eta_seconds)).isoformat()
                if eta_seconds is not None
                else None
            ),
            "status_counts": dict(sorted(self.status_counts.items())),
            "telemetry_counts": dict(sorted(self.telemetry_counts.items())),
            "previous_run": self.previous_run,
            "basis": self.basis,
        }
        atomic_write_text(
            self.path,
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        )

    def update(self, phase: str, status: str, *, count: int = 1) -> None:
        increment = max(0, int(count))
        with self._lock:
            self.current = min(self.total, self.current + increment)
            key = str(status)
            self.status_counts[key] = self.status_counts.get(key, 0) + increment
            self._write("running", phase)

    def heartbeat(self, phase: str) -> None:
        """Refresh a long-running phase without falsely incrementing progress."""

        with self._lock:
            self._write("running", phase)

    def observe(
        self,
        phase: str,
        event: str,
        *,
        count: int = 1,
        publish_interval_seconds: float = 1.0,
    ) -> None:
        """Record high-rate telemetry without advancing logical progress.

        Request pages are observations, not completion units: one symbol may
        need one page on an incremental tail and thousands during a historical
        rebuild.  Counting pages against a symbol/stage denominator can saturate
        progress at 100% while work is still running.  Publishing is throttled
        so a high-throughput downloader does not turn each HTTP response into an
        atomic JSON filesystem write.
        """

        increment = max(0, int(count))
        with self._lock:
            key = str(event)
            self.telemetry_counts[key] = self.telemetry_counts.get(key, 0) + increment
            now = time.monotonic()
            if now - self._last_telemetry_publish >= max(
                0.0, float(publish_interval_seconds)
            ):
                self._last_telemetry_publish = now
                self._write("running", phase)

    def finish(
        self,
        *,
        failed: bool = False,
        state: str | None = None,
        require_exact: bool = False,
    ) -> None:
        with self._lock:
            final_state = str(state or ("failed" if failed else "complete"))
            if (
                require_exact
                and final_state == "complete"
                and self.current != self.total
            ):
                final_state = "partial"
            if final_state == "complete":
                self.current = self.total
            self._write(final_state, "complete")


_SYSTEM_GETADDRINFO = socket.getaddrinfo
_DNS_FALLBACK_LOCK = threading.Lock()
_DNS_FALLBACK_CACHE: dict[str, tuple[float, tuple[str, ...]]] = {}
_DNS_FALLBACK_INSTALLED = False


def _dns_over_https_addresses(host: str) -> tuple[str, ...]:
    now = time.monotonic()
    with _DNS_FALLBACK_LOCK:
        cached = _DNS_FALLBACK_CACHE.get(host)
        if cached is not None and cached[0] > now:
            return cached[1]

    addresses: list[str] = []
    ttl = 60
    for record_type, answer_type in (("A", 1), ("AAAA", 28)):
        connection = http.client.HTTPSConnection("1.1.1.1", timeout=5)
        try:
            connection.request(
                "GET",
                f"/dns-query?name={host}&type={record_type}",
                headers={"Accept": "application/dns-json"},
            )
            response = connection.getresponse()
            payload = json.loads(response.read().decode("utf-8"))
            if response.status != 200 or int(payload.get("Status", -1)) != 0:
                continue
            for answer in payload.get("Answer") or []:
                if int(answer.get("type", -1)) != answer_type:
                    continue
                value = str(answer.get("data") or "").strip()
                try:
                    ipaddress.ip_address(value)
                except ValueError:
                    continue
                addresses.append(value)
                ttl = min(ttl, max(10, int(answer.get("TTL") or 60)))
        finally:
            connection.close()
    unique = tuple(dict.fromkeys(addresses))
    if not unique:
        raise socket.gaierror(f"DNS-over-HTTPS returned no address for {host}")
    with _DNS_FALLBACK_LOCK:
        _DNS_FALLBACK_CACHE[host] = (now + min(ttl, 300), unique)
    return unique


def install_dns_over_https_fallback() -> None:
    """Use DoH only after the host resolver fails; TLS still verifies the original host."""

    global _DNS_FALLBACK_INSTALLED
    if _DNS_FALLBACK_INSTALLED:
        return
    if os.getenv("STOCKAGENT_DNS_OVER_HTTPS_FALLBACK", "1").strip().lower() in {
        "0",
        "false",
        "no",
        "off",
    }:
        return

    def resilient_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        try:
            return _SYSTEM_GETADDRINFO(host, port, family, type, proto, flags)
        except socket.gaierror as original_error:
            text = str(host or "").strip()
            try:
                ipaddress.ip_address(text)
            except ValueError:
                pass
            else:
                raise original_error
            try:
                addresses = _dns_over_https_addresses(text)
            except Exception:
                raise original_error
            resolved: list[tuple] = []
            for address in addresses:
                address_family = socket.AF_INET6 if ":" in address else socket.AF_INET
                if family not in {0, socket.AF_UNSPEC, address_family}:
                    continue
                resolved.extend(
                    _SYSTEM_GETADDRINFO(
                        address,
                        port,
                        address_family,
                        type,
                        proto,
                        flags | socket.AI_NUMERICHOST,
                    )
                )
            if not resolved:
                raise original_error
            return resolved

    socket.getaddrinfo = resilient_getaddrinfo
    _DNS_FALLBACK_INSTALLED = True


install_dns_over_https_fallback()


@dataclass(frozen=True, slots=True)
class ProviderRateLimit:
    provider: str
    requests: float
    seconds: float
    basis: str
    source_url: str
    note: str = ""

    @property
    def requests_per_second(self) -> float:
        return max(0.001, float(self.requests) / float(self.seconds))

    @property
    def interval_seconds(self) -> float:
        return 1.0 / self.requests_per_second


DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND = 10.0


PROVIDER_RATE_LIMITS: dict[str, ProviderRateLimit] = {
    "fred_api": ProviderRateLimit(
        provider="fred_api",
        requests=2,
        seconds=1,
        basis=(
            "OpenBB FRED provider safety ceiling; FRED documents HTTP 429 "
            "throttling but no public numeric limit"
        ),
        source_url="https://fred.stlouisfed.org/docs/api/fred/errors.html",
        note="Direct FRED API v2 requests share this process-wide limiter.",
    ),
    "okx_history_candles": ProviderRateLimit(
        provider="okx_history_candles",
        requests=20,
        seconds=2,
        basis="official endpoint limit; IP",
        source_url="https://www.okx.com/docs-v5/en/",
        note="GET /api/v5/market/history-candles",
    ),
    "okx_history_mark_price_candles": ProviderRateLimit(
        provider="okx_history_mark_price_candles",
        requests=20,
        seconds=2,
        basis="official endpoint limit; IP",
        source_url="https://www.okx.com/docs-v5/en/",
        note="GET /api/v5/market/history-mark-price-candles",
    ),
    "okx_history_index_candles": ProviderRateLimit(
        provider="okx_history_index_candles",
        requests=10,
        seconds=2,
        basis="official endpoint limit; IP",
        source_url="https://www.okx.com/docs-v5/en/",
        note="GET /api/v5/market/history-index-candles",
    ),
    "okx_funding_rate_history": ProviderRateLimit(
        provider="okx_funding_rate_history",
        requests=10,
        seconds=2,
        basis="official endpoint limit; IP + Instrument ID",
        source_url="https://www.okx.com/docs-v5/en/",
        note="GET /api/v5/public/funding-rate-history; limiter is partitioned by instId.",
    ),
    "bybit_public_rest": ProviderRateLimit(
        provider="bybit_public_rest",
        requests=600,
        seconds=5,
        basis="official HTTP IP limit",
        source_url="https://bybit-exchange.github.io/docs/v5/rate-limit",
        note=(
            "Public market data shares the api.bybit.com IP bucket and also "
            "returns endpoint-specific X-Bapi-Limit headers."
        ),
    ),
    "binance_usdm_request_weight": ProviderRateLimit(
        provider="binance_usdm_request_weight",
        requests=2400,
        seconds=60,
        basis="official USD-M exchangeInfo REQUEST_WEIGHT fallback; IP",
        source_url=(
            "https://developers.binance.com/en/docs/catalog/"
            "core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data"
        ),
        note=(
            "Runtime exchangeInfo limits are authoritative. One limiter cost unit "
            "equals one request-weight unit."
        ),
    ),
    "binance_usdm_funding_history": ProviderRateLimit(
        provider="binance_usdm_funding_history",
        requests=500,
        seconds=300,
        basis="official shared fundingRate/fundingInfo IP limit",
        source_url=(
            "https://developers.binance.com/en/docs/catalog/"
            "core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data"
        ),
        note="GET /fapi/v1/fundingRate shares this bucket with fundingInfo.",
    ),
    "binance_usdm_statistics_history": ProviderRateLimit(
        provider="binance_usdm_statistics_history",
        requests=1000,
        seconds=300,
        basis="official futures-data IP limit",
        source_url=(
            "https://developers.binance.com/en/docs/catalog/"
            "core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data"
        ),
        note=(
            "Rolling futures/data statistics endpoints retain only the latest "
            "30 days; append-only recurring capture is required."
        ),
    ),
    "shioaji_quote_query": ProviderRateLimit(
        provider="shioaji_quote_query",
        requests=50,
        seconds=5,
        basis="user-selected account ceiling; matches legacy PDF/C# 50/5s",
        source_url="https://sinotrade.github.io/tutor/limit/",
        note=(
            "Ticks, snapshots, Kbars, credit and short-source queries share this "
            "ceiling. Current Python docs also contain a conflicting 50/10s value."
        ),
    ),
    "frankfurter_public": ProviderRateLimit(
        provider="frankfurter_public",
        requests=DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND,
        seconds=1,
        basis="client-side safety policy; no upstream numeric limit documented",
        source_url="https://frankfurter.dev/",
        note="No daily/monthly caps are published; keep a configurable client-side cap.",
    ),
    "tw_public": ProviderRateLimit(
        provider="tw_public",
        requests=DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND,
        seconds=1,
        basis="client-side safety policy; no upstream numeric limit documented",
        source_url="https://openapi.twse.com.tw/",
        note=(
            "Default 10 req/s is a stockAgent policy, not an official "
            "TWSE/TPEx/data.gov.tw limit."
        ),
    ),
    "taifex_public": ProviderRateLimit(
        provider="taifex_public",
        requests=DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND,
        seconds=1,
        basis="client-side safety policy; no upstream numeric limit documented",
        source_url="https://www.taifex.com.tw/cht/9/optQryRes",
        note=(
            "Official TAIFEX HTML/ZIP archives share one host-global bucket. "
            "This 10 req/s ceiling is a stockAgent policy, not an exchange claim."
        ),
    ),
    "cftc_public_archive": ProviderRateLimit(
        provider="cftc_public_archive",
        requests=DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND,
        seconds=1,
        basis="client-side safety policy; no upstream numeric limit documented",
        source_url="https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalCompressed/index.htm",
        note="Official historical compressed archives; requests share one bucket.",
    ),
    "yahoo_finance": ProviderRateLimit(
        provider="yahoo_finance",
        requests=DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND,
        seconds=1,
        basis="client-side safety policy; no upstream numeric limit documented",
        source_url="https://finance.yahoo.com/",
        note=(
            "CLI default is 10 req/s when no interval is supplied. Large TW "
            "fallback bootstraps explicitly use a slower 1.5-second interval."
        ),
    ),
    "alpaca_market_data_basic": ProviderRateLimit(
        provider="alpaca_market_data_basic",
        requests=200,
        seconds=60,
        basis="official Basic historical market-data limit; account",
        source_url="https://docs.alpaca.markets/docs/about-market-data-api",
        note="Algo Trader Plus supports 10,000 requests per minute via downloader configuration.",
    ),
    "defillama_public": ProviderRateLimit(
        provider="defillama_public",
        requests=DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND,
        seconds=1,
        basis="client-side safety policy; free endpoint has no stable numeric limit",
        source_url="https://defillama.com/docs/api",
        note="Compact TVL, stablecoin and yield snapshots; no Pro endpoint is used.",
    ),
    "hyperliquid_info": ProviderRateLimit(
        provider="hyperliquid_info",
        requests=60,
        seconds=60,
        basis="official 1200 weight/minute IP limit with conservative weight 20 per info request",
        source_url=(
            "https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/"
            "api/rate-limits-and-user-limits"
        ),
        note="Most info calls cost 20 weight; physical request rate remains below 10 req/s.",
    ),
    "deribit_public": ProviderRateLimit(
        provider="deribit_public",
        requests=DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND,
        seconds=1,
        basis="client-side safety policy below Deribit credit-based limits",
        source_url="https://docs.deribit.com/",
        note="Anonymous public book-summary snapshots only.",
    ),
    "coinmetrics_community": ProviderRateLimit(
        provider="coinmetrics_community",
        requests=10,
        seconds=6,
        basis="official Community API sliding-window IP limit",
        source_url="https://docs.coinmetrics.io/api/v4/",
        note="No API key; community data is licensed separately and is not assumed commercial-use free.",
    ),
    "coinbase_exchange_public": ProviderRateLimit(
        provider="coinbase_exchange_public",
        requests=10,
        seconds=1,
        basis="official Exchange REST public-endpoint IP limit",
        source_url="https://docs.cdp.coinbase.com/exchange/rest-api/rate-limits",
        note="Use the sustained 10 req/s rate and do not consume the documented burst allowance.",
    ),
    "kraken_public": ProviderRateLimit(
        provider="kraken_public",
        requests=DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND,
        seconds=1,
        basis="client-side safety policy; selected public catalog has no numeric cap documented",
        source_url="https://docs.kraken.com/api/",
        note="The selected compact catalog call needs one request per run.",
    ),
    "bitfinex_public": ProviderRateLimit(
        provider="bitfinex_public",
        requests=DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND,
        seconds=1,
        basis="client-side safety policy; endpoint-specific public limits vary",
        source_url="https://docs.bitfinex.com/reference/rest-public-tickers",
        note="One all-symbol ticker snapshot is captured per run.",
    ),
    "alternative_me_public": ProviderRateLimit(
        provider="alternative_me_public",
        requests=DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND,
        seconds=1,
        basis="client-side safety policy; no numeric public limit documented",
        source_url="https://alternative.me/crypto/api/",
        note="The complete Fear and Greed history is returned in one request.",
    ),
    "mempool_space_public": ProviderRateLimit(
        provider="mempool_space_public",
        requests=DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND,
        seconds=1,
        basis="client-side safety policy; public service documents 429 and ban enforcement",
        source_url="https://mempool.space/docs/api/rest",
        note="Compact Bitcoin network, fee and mining state only; never request the full mempool txid list.",
    ),
    "blockscout_ethereum_public": ProviderRateLimit(
        provider="blockscout_ethereum_public",
        requests=10,
        seconds=60,
        basis="runtime x-ratelimit-limit header on the public Ethereum instance",
        source_url="https://docs.blockscout.com/api-reference/get-stats-counters",
        note=(
            "Anonymous per-instance API; selected stats and latest-block calls are "
            "compact. Blockscout has announced future migration to its PRO API."
        ),
    ),
    "binance_public_archive": ProviderRateLimit(
        provider="binance_public_archive",
        requests=DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND,
        seconds=1,
        basis="client-side ceiling; public S3 archive has no documented numeric request cap",
        source_url="https://github.com/binance/binance-public-data",
        note=(
            "Checksum and ZIP downloads share one archive-host limiter; "
            "retries honor HTTP throttling."
        ),
    ),
    "binance_public_listing": ProviderRateLimit(
        provider="binance_public_listing",
        requests=DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND,
        seconds=1,
        basis="client-side ceiling; public S3 listing has no documented numeric request cap",
        source_url="https://github.com/binance/binance-public-data",
        note="Independent host-global bucket for the official S3 ListObjects endpoint.",
    ),
    "coingecko_demo": ProviderRateLimit(
        provider="coingecko_demo",
        requests=100,
        seconds=60,
        basis="official free Demo API key limit",
        source_url="https://www.coingecko.com/en/api/pricing",
        note=(
            "Demo also has a 10,000 call monthly cap. Endpoint cache freshness and "
            "remaining monthly credits, not the minute limit, determine polling cadence."
        ),
    ),
    "coinmarketcap_basic": ProviderRateLimit(
        provider="coinmarketcap_basic",
        requests=50,
        seconds=60,
        basis="official free Basic plan minute limit; API key",
        source_url="https://coinmarketcap.com/api/",
        note=(
            "Basic has 15,000 monthly call credits. Runtime /v1/key/info is "
            "authoritative for plan limits and remaining credits."
        ),
    ),
    "coinglass_keyed": ProviderRateLimit(
        provider="coinglass_keyed",
        requests=30,
        seconds=60,
        basis="lowest currently published API plan limit; API key",
        source_url="https://www.coinglass.com/pricing",
        note=(
            "Do not send data requests until an entitlement probe succeeds. Runtime "
            "API-KEY-MAX-LIMIT headers override this floor when present."
        ),
    ),
    "etherscan_free": ProviderRateLimit(
        provider="etherscan_free",
        requests=3,
        seconds=1,
        basis="official Etherscan free-tier key limit",
        source_url="https://docs.etherscan.io/resources/rate-limits",
        note="Free tier also has a 100,000 calls/day cap and selected-chain restrictions.",
    ),
    "dune_free_low": ProviderRateLimit(
        provider="dune_free_low",
        requests=15,
        seconds=60,
        basis="official Dune Free low-limit endpoint bucket; API key",
        source_url="https://docs.dune.com/api-reference/overview/rate-limits",
        note="Query execution and other write-heavy endpoints use this bucket and consume credits.",
    ),
    "dune_free_high": ProviderRateLimit(
        provider="dune_free_high",
        requests=40,
        seconds=60,
        basis="official Dune Free high-limit endpoint bucket; API key",
        source_url="https://docs.dune.com/api-reference/overview/rate-limits",
        note="Read-heavy result and status endpoints use this independent bucket.",
    ),
    "sec_edgar": ProviderRateLimit(
        provider="sec_edgar",
        requests=10,
        seconds=1,
        basis="official SEC fair-access maximum request rate; user agent required",
        source_url="https://www.sec.gov/filergroup/announcements-old/new-rate-control-limits",
        note=(
            "All SEC submissions, companyfacts and primary-document requests share "
            "this process- and host-global limiter."
        ),
    ),
    "ishares_public": ProviderRateLimit(
        provider="ishares_public",
        requests=DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND,
        seconds=1,
        basis="client-side safety policy; no upstream numeric limit documented",
        source_url="https://www.ishares.com/us/library/2025-tax-kit",
        note="Official tax-history workbooks and current holdings files.",
    ),
    "bitwise_public": ProviderRateLimit(
        provider="bitwise_public",
        requests=DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND,
        seconds=1,
        basis="client-side safety policy; no upstream numeric limit documented",
        source_url="https://bitbetf.com/",
        note="Official BITB page data; full page is versioned before normalization.",
    ),
}


class SharedRateLimiter:
    def __init__(
        self,
        interval_seconds: float,
        *,
        name: str = "rate-limit",
        state_dir: str | Path | None = None,
        on_claim: Callable[[], None] | None = None,
        on_caller_claim: Callable[[], None] | None = None,
    ) -> None:
        self.interval_seconds = max(0.0, float(interval_seconds))
        self.name = str(name)
        self._on_claim = on_claim
        # The stable dispatcher owns slot timing, while some observers need
        # the identity/context of the data worker that received that slot.
        # Keep these callbacks separate so pacing remains single-leader and
        # request attribution can use worker-local context safely.
        self._on_caller_claim = on_caller_claim
        self._lock = threading.Lock()
        self._next_time = 0.0
        # A stable FIFO dispatcher owns the local schedule.  Handing leadership
        # from one data worker to another after every ticket lets the newly
        # released worker enter provider parsing/network setup before the next
        # waiter gets CPU.  Under OpenBB's many blocking portals that GIL/thread
        # handoff cut independent providers to roughly half their configured
        # request-start rate.  The dispatcher never performs provider work: it
        # only claims the host-global slot, records it, and releases one waiter.
        self._dispatch_condition = threading.Condition()
        self._dispatch_queue: deque[tuple[threading.Event, float]] = deque()
        self._dispatcher_thread: threading.Thread | None = None
        # Granted-slot telemetry can persist JSON and contend with hundreds of
        # provider workers. It must never run in the cadence-owning dispatcher
        # thread, otherwise an observer lock turns an 8-10 req/s limiter into
        # an accidental ~4 req/s limiter. A single ordered observer preserves
        # every claim without delaying tickets or creating one thread per slot.
        self._claim_observer_queue: Queue[None] = Queue()
        self._claim_observer_thread: threading.Thread | None = None
        # Request cadence telemetry must be recorded at the grant boundary,
        # not when the asynchronous diagnostic observer eventually persists
        # it.  Under many provider workers that observer may lag while the
        # dispatcher continues issuing perfectly paced slots; timestamping the
        # delayed callbacks would falsely report low API utilization.
        self._grant_session_started_at = time.time()
        self._grant_times: deque[float] = deque()
        self._grant_total = 0
        self._grant_cost_times: deque[tuple[float, float]] = deque()
        self._grant_cost_total = 0.0

        root_value = state_dir or os.environ.get("STOCKAGENT_RATE_LIMIT_DIR")
        if root_value is None:
            uid = getattr(os, "getuid", lambda: "user")()
            root_value = Path(tempfile.gettempdir()) / f"stockagent-rate-limits-{uid}"
        root = Path(root_value)
        digest = hashlib.sha256(self.name.encode("utf-8")).hexdigest()[:20]
        self._state_path = root / f"{digest}.state"

    @staticmethod
    def _validated_cost(cost: float) -> float:
        value = float(cost)
        if not math.isfinite(value) or value <= 0.0:
            raise ValueError(
                f"rate-limit cost must be finite and positive, got {cost!r}"
            )
        return value

    def _claim_process_shared(self, cost: float = 1.0) -> tuple[bool, float]:
        """Claim the current slot only when it is ready.

        A caller that is too early gets a delay but does not reserve a future
        slot.  It must sleep and retry, which lets a concurrent ``defer()``
        extend the provider-wide cooldown before the request is sent.
        """
        claim_cost = self._validated_cost(cost)
        if fcntl is None:
            with self._lock:
                now = time.monotonic()
                wait_s = max(0.0, self._next_time - now)
                if wait_s > 0.0:
                    return False, wait_s
                self._next_time = self._next_deadline(
                    self._next_time, now, cost=claim_cost
                )
                return True, 0.0

        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock, self._state_path.open("a+", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                handle.seek(0)
                raw = handle.read().strip()
                try:
                    next_time = float(raw) if raw else 0.0
                except ValueError:
                    next_time = 0.0
                now = time.monotonic()
                # /tmp may survive a container or host restart while CLOCK_MONOTONIC
                # restarts from zero. Ignore an implausibly distant stale reservation.
                if next_time - now > max(300.0, self.interval_seconds * 100_000.0):
                    next_time = 0.0
                wait_s = max(0.0, next_time - now)
                claimed = wait_s <= 0.0
                if claimed:
                    next_time = self._next_deadline(next_time, now, cost=claim_cost)
                    handle.seek(0)
                    handle.truncate()
                    handle.write(f"{next_time:.9f}\n")
                    handle.flush()
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        return claimed, wait_s

    def _next_deadline(
        self, previous_deadline: float, now: float, *, cost: float = 1.0
    ) -> float:
        """Advance an absolute schedule without accumulating wake-up jitter.

        Reset after a full missed interval so a suspended process never emits
        a catch-up burst.  For normal sub-interval scheduler/GIL latency, keep
        the original cadence: otherwise adding the latency to every ticket
        permanently lowers a 10 req/s policy to roughly 8 req/s under load.
        """
        interval = self.interval_seconds
        claim_cost = self._validated_cost(cost)
        if interval <= 0.0:
            return now
        if previous_deadline <= 0.0 or now - previous_deadline >= interval:
            return now + interval * claim_cost
        return previous_deadline + interval * claim_cost

    def _defer_process_shared(self, seconds: float) -> None:
        if fcntl is None:
            with self._lock:
                now = time.monotonic()
                self._next_time = max(self._next_time, now + seconds)
            return

        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock, self._state_path.open("a+", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                handle.seek(0)
                raw = handle.read().strip()
                try:
                    next_time = float(raw) if raw else 0.0
                except ValueError:
                    next_time = 0.0
                now = time.monotonic()
                if next_time - now > max(300.0, self.interval_seconds * 100_000.0):
                    next_time = 0.0
                next_time = max(next_time, now + seconds)
                handle.seek(0)
                handle.truncate()
                handle.write(f"{next_time:.9f}\n")
                handle.flush()
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _ensure_dispatcher_locked(self) -> None:
        if self._dispatcher_thread is not None and self._dispatcher_thread.is_alive():
            return
        self._dispatcher_thread = threading.Thread(
            target=self._dispatch_waiters,
            name=f"{self.name}-dispatcher",
            daemon=True,
        )
        self._dispatcher_thread.start()

    def _ensure_claim_observer_locked(self) -> None:
        if self._on_claim is None:
            return
        if (
            self._claim_observer_thread is not None
            and self._claim_observer_thread.is_alive()
        ):
            return
        self._claim_observer_thread = threading.Thread(
            target=self._observe_claims,
            name=f"{self.name}-claim-observer",
            daemon=True,
        )
        self._claim_observer_thread.start()

    def _observe_claims(self) -> None:
        """Record granted slots independently from request-slot cadence."""
        while True:
            self._claim_observer_queue.get()
            try:
                self._notify_claim()
            finally:
                self._claim_observer_queue.task_done()

    def _dispatch_waiters(self) -> None:
        """Grant local waiters in FIFO order from one scheduling-only thread."""
        while True:
            with self._dispatch_condition:
                while not self._dispatch_queue:
                    self._dispatch_condition.wait()
                ticket, cost = self._dispatch_queue[0]

            claimed, wait_s = self._claim_process_shared(cost=cost)
            if not claimed:
                if wait_s > 0.0:
                    time.sleep(wait_s)
                continue

            with self._dispatch_condition:
                # Only this dispatcher removes local tickets.  Keeping the
                # identity check makes a future cancellation extension safe.
                if not self._dispatch_queue or self._dispatch_queue[0][0] is not ticket:
                    continue
                self._dispatch_queue.popleft()
                self._record_grant_locked(cost=cost)
            if self._on_claim is not None:
                with self._dispatch_condition:
                    self._ensure_claim_observer_locked()
                self._claim_observer_queue.put(None)
            ticket.set()

    def wait(self, cost: float = 1.0) -> None:
        claim_cost = self._validated_cost(cost)
        if self.interval_seconds <= 0.0:
            with self._dispatch_condition:
                self._record_grant_locked(cost=claim_cost)
            self._notify_claim()
            self._notify_caller_claim()
            return
        ticket = threading.Event()
        with self._dispatch_condition:
            self._dispatch_queue.append((ticket, claim_cost))
            self._ensure_dispatcher_locked()
            self._dispatch_condition.notify()
        ticket.wait()
        self._notify_caller_claim()

    def _notify_claim(self) -> None:
        """Publish one granted request slot without risking the data request."""
        if self._on_claim is None:
            return
        try:
            self._on_claim()
        except Exception:
            # Rate telemetry is diagnostic. A broken observer must never turn
            # a successfully paced provider call into a failed data request.
            return

    def _notify_caller_claim(self) -> None:
        """Publish a granted slot from the waiting data-worker context."""
        if self._on_caller_claim is None:
            return
        try:
            self._on_caller_claim()
        except Exception:
            # Like dispatcher telemetry, attribution must never fail a data
            # request that already received its process-shared slot.
            return

    def pending_waiters(self) -> int:
        """Return local callers queued for a request-start ticket."""
        with self._dispatch_condition:
            return len(self._dispatch_queue)

    def _record_grant_locked(self, *, cost: float = 1.0) -> None:
        now = time.time()
        self._grant_times.append(now)
        self._grant_total += 1
        self._grant_cost_times.append((now, float(cost)))
        self._grant_cost_total += float(cost)
        cutoff = now - 60.0
        while self._grant_times and self._grant_times[0] < cutoff:
            self._grant_times.popleft()
        while self._grant_cost_times and self._grant_cost_times[0][0] < cutoff:
            self._grant_cost_times.popleft()

    def grant_activity(self, now: float | None = None) -> dict[str, float | int]:
        """Return dispatcher-boundary activity without observer timestamp lag."""
        current = time.time() if now is None else float(now)
        with self._dispatch_condition:
            cutoff = current - 60.0
            while self._grant_times and self._grant_times[0] < cutoff:
                self._grant_times.popleft()
            while self._grant_cost_times and self._grant_cost_times[0][0] < cutoff:
                self._grant_cost_times.popleft()
            window_seconds = min(
                60.0,
                max(0.001, current - self._grant_session_started_at),
            )
            return {
                "grants_total": int(self._grant_total),
                "grants_last_60s": len(self._grant_times),
                "grant_cost_total": float(self._grant_cost_total),
                "grant_cost_last_60s": float(
                    sum(cost for _, cost in self._grant_cost_times)
                ),
                "window_seconds": window_seconds,
                "pending_claim_observations": self._claim_observer_queue.qsize(),
            }

    def flush_claim_observations(self) -> None:
        """Wait until all already-granted slots have reached telemetry.

        Normal requests never call this method. A provider quota response uses
        it once so the durable claims-at-limit evidence includes the request
        that produced that response, while ordinary slot cadence remains fully
        decoupled from diagnostic persistence.
        """
        if threading.current_thread() is self._claim_observer_thread:
            return
        self._claim_observer_queue.join()

    def defer(self, seconds: float) -> None:
        delay = max(0.0, float(seconds))
        if delay > 0.0:
            self._defer_process_shared(delay)


def parse_retry_after_seconds(
    value: str | None,
    *,
    now: datetime | None = None,
) -> float | None:
    """Parse an HTTP Retry-After delta or date into non-negative seconds."""
    text = str(value or "").strip()
    if not text:
        return None
    try:
        seconds = float(text)
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(text)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        seconds = (retry_at - current.astimezone(timezone.utc)).total_seconds()
    if not math.isfinite(seconds):
        return None
    return max(0.0, seconds)


def retry_delay_seconds(
    attempt: int,
    *,
    base: float,
    cap: float = 30.0,
    retry_after: str | None = None,
) -> float:
    """Return the larger of bounded exponential backoff and Retry-After."""
    exponential = min(
        max(0.0, float(cap)), max(0.0, float(base)) * (2 ** max(0, int(attempt)))
    )
    provider_delay = parse_retry_after_seconds(retry_after)
    return max(exponential, provider_delay or 0.0)


def resolve_incremental_reconcile_start_ms(
    *,
    expected_first_ms: int,
    earliest_existing_ms: int | None,
    latest_existing_ms: int | None,
    overlap_ms: int,
    repair_missing_head: bool = True,
) -> tuple[int, bool]:
    """Resolve a tail update start without preserving a truncated history head."""
    expected = int(expected_first_ms)
    overlap = max(0, int(overlap_ms))
    missing_head = (
        earliest_existing_ms is None
        or latest_existing_ms is None
        or int(earliest_existing_ms) > expected + overlap
    )
    if missing_head and repair_missing_head:
        return expected, True
    if latest_existing_ms is None:
        return expected, missing_head
    return max(expected, int(latest_existing_ms) - overlap), False


def parquet_temporal_metadata(
    parquet_file: object,
    *,
    column: str = "date",
    expected_interval_ms: int,
) -> tuple[int, int | None, int | None, bool | None]:
    """Read row count and timestamp bounds from Parquet footer statistics.

    The return value is ``(rows, earliest_ms, latest_ms, interval_likely_ok)``.
    ``interval_likely_ok`` is ``None`` when footer statistics are insufficient,
    allowing callers to fall back to an exact column scan.  A positive result
    avoids reading millions of timestamp values merely to decide where an
    incremental request should start.
    """

    metadata = getattr(parquet_file, "metadata", None)
    schema = getattr(parquet_file, "schema_arrow", None)
    if metadata is None or schema is None:
        raise TypeError("parquet_file must expose metadata and schema_arrow")
    names = list(getattr(schema, "names", ()))
    rows = int(metadata.num_rows)
    if column not in names:
        return rows, None, None, False
    column_index = names.index(column)

    def to_ms(value: object) -> int | None:
        if value is None:
            return None
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="strict")
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, date):
            parsed = datetime.combine(value, datetime.min.time())
        elif isinstance(value, (int, float)):
            numeric = float(value)
            magnitude = abs(numeric)
            if magnitude >= 1e17:
                return int(numeric / 1e6)  # nanoseconds
            if magnitude >= 1e14:
                return int(numeric / 1e3)  # microseconds
            if magnitude >= 1e11:
                return int(numeric)  # milliseconds
            if magnitude >= 1e8:
                return int(numeric * 1e3)  # seconds
            return None
        else:
            text_value = str(value).strip().replace("Z", "+00:00")
            try:
                parsed = datetime.fromisoformat(text_value)
            except ValueError:
                return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        else:
            parsed = parsed.astimezone(timezone.utc)
        return int(parsed.timestamp() * 1000)

    bounds: list[tuple[int, int, int]] = []
    for row_group_index in range(int(metadata.num_row_groups)):
        row_group = metadata.row_group(row_group_index)
        chunk = row_group.column(column_index)
        statistics = chunk.statistics
        if statistics is None or not statistics.has_min_max:
            continue
        low = to_ms(statistics.min)
        high = to_ms(statistics.max)
        if low is None or high is None:
            continue
        bounds.append((low, high, int(chunk.num_values)))

    if not bounds:
        return rows, None, None, None
    earliest = min(item[0] for item in bounds)
    latest = max(item[1] for item in bounds)
    if rows < 3:
        interval_likely_ok: bool | None = True
    else:
        sampled_steps = [
            (high - low) / max(1, count - 1)
            for low, high, count in bounds
            if count >= 3 and high >= low
        ]
        interval_likely_ok = (
            sorted(sampled_steps)[len(sampled_steps) // 2]
            <= max(1, int(expected_interval_ms)) * 4
            if sampled_steps
            else None
        )
    return rows, earliest, latest, interval_likely_ok


def provider_rate_limit(provider: str) -> ProviderRateLimit:
    key = str(provider).strip().lower()
    if key in PROVIDER_RATE_LIMITS:
        return PROVIDER_RATE_LIMITS[key]
    return ProviderRateLimit(
        provider=key or "unspecified",
        requests=DEFAULT_UNSPECIFIED_REQUESTS_PER_SECOND,
        seconds=1,
        basis="client-side safety policy; no upstream numeric limit documented",
        source_url="n/a",
        note="Unregistered providers default to 10 requests per second.",
    )


def resolve_request_interval(
    provider: str,
    requested_interval: float | None = None,
    *,
    env_var: str | None = None,
    allow_zero: bool = False,
) -> float:
    profile = provider_rate_limit(provider)
    raw: float | None = requested_interval
    if env_var:
        text = os.environ.get(env_var)
        if text not in {None, ""}:
            raw = float(text)
    floor = profile.interval_seconds
    if raw is None:
        return floor
    value = max(0.0, float(raw))
    if allow_zero and value == 0.0:
        return 0.0
    if value < floor:
        print(
            f"[rate-limit] provider={profile.provider} requested_interval={value:.6f}s "
            f"below_policy_interval={floor:.6f}s; clamped basis={profile.basis}",
            flush=True,
        )
        return floor
    return value


def describe_rate_limit(provider: str, interval_seconds: float) -> str:
    profile = provider_rate_limit(provider)
    rps = float("inf") if interval_seconds <= 0.0 else 1.0 / float(interval_seconds)
    return (
        f"provider={profile.provider} interval={interval_seconds:.6f}s "
        f"rps={rps:.2f} source={profile.source_url} basis={profile.basis}"
    )


def resolve_end_date(value: str) -> str:
    text = value.strip().lower()
    if text in {"today", "now"}:
        return date.today().isoformat()
    return value.strip()


def run_parallel_tasks(
    items: Iterable[TItem],
    worker: Callable[[TItem], TResult],
    *,
    max_workers: int,
    desc: str,
    unit: str = "item",
    on_error: Callable[[TItem, Exception], TResult] | None = None,
) -> list[TResult]:
    item_list = list(items)
    if not item_list:
        return []

    results: list[TResult] = []
    with ThreadPoolExecutor(max_workers=max(1, int(max_workers))) as executor:
        futures = {executor.submit(worker, item): item for item in item_list}
        progress = tqdm(total=len(futures), desc=desc, unit=unit)
        try:
            for future in as_completed(futures):
                item = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    if on_error is None:
                        raise
                    result = on_error(item, exc)
                results.append(result)
                progress.update(1)
        finally:
            progress.close()

    return results
