from __future__ import annotations

import argparse
from collections.abc import Callable
import contextlib
import io
import json
import multiprocessing as mp
import os
import re
import socket
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from html.parser import HTMLParser
import math
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import yfinance as yf
from tqdm import tqdm

try:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover - optional runtime dependency guard
    pa = None
    pc = None
    pq = None

try:
    import polars as pl
except Exception:  # pragma: no cover - optional runtime dependency guard
    pl = None


ASSET_CLASSES = ("tw_stocks", "us_stocks", "crypto", "forex")
WEEKDAY_DAILY_ASSET_CLASSES = {"tw_stocks", "us_stocks", "forex"}
TWSE_SOURCES = {
    "listed": ("https://isin.twse.com.tw/isin/C_public.jsp?strMode=2", ".TW"),
    "otc": ("https://isin.twse.com.tw/isin/C_public.jsp?strMode=4", ".TWO"),
}
US_SYMBOL_SOURCES = (
    "https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt",
    "https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt",
)
TWSE_DELISTED_SOURCES = (
    "https://www.twse.com.tw/en/listed/suspend-listing.html",
    "https://www.twse.com.tw/en/listed/delisted-company.html",
)
TPEX_DELISTED_SOURCES = (
    "https://www.tpex.org.tw/en-us/mainboard/termination.html",
    "https://www.tpex.org.tw/en-us/mainboard/terminated.html",
)
COINGECKO_COINS_LIST_URL = "https://api.coingecko.com/api/v3/coins/list"
YAHOO_CURRENCIES_URL = "https://finance.yahoo.com/markets/currencies/"
ALPHA_VANTAGE_LISTING_STATUS_URL = "https://www.alphavantage.co/query"
YAHOO_SYMBOL_SPLIT_PATTERN = re.compile(r"\s*,\s*")
TWSE_CODE_NAME_PATTERN = re.compile(r"^(?P<code>\d{4,6}[A-Z]{0,2})[\s\u3000]+(?P<name>.+)$")
TW_GENERIC_CODE_PATTERN = re.compile(r"\b(\d{4,6}[A-Z]{0,2})\b")
US_VALID_SYMBOL_PATTERN = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")
FOREX_YAHOO_SYMBOL_PATTERN = re.compile(r"\b([A-Z]{6}=X)\b")
FOREX_TICKER_PATTERN = re.compile(r"^[A-Z]{6}=X$")
CRYPTO_SYMBOL_PATTERN = re.compile(r"^[a-z]{2,8}$")
OUTPUT_COLUMNS = ["date", "open", "max", "min", "close", "Trading_Volume"]
BASE_OUTPUT_COLUMNS = ["date", "open", "max", "min", "close", "adjclose", "Trading_Volume"]
REPAIR_REQUIRED_COLUMNS = {"date", "open", "max", "min", "close", "adjclose", "Trading_Volume"}
PARQUET_META_SOURCE_KEY = b"stockagent.source"
PARQUET_META_ASSET_CLASS_KEY = b"stockagent.asset_class"
PARQUET_META_CHECKED_THROUGH_KEY = b"stockagent.yahoo_checked_through"
PARQUET_META_REQUESTED_END_KEY = b"stockagent.yahoo_requested_end"
PARQUET_META_FIRST_DATE_KEY = b"stockagent.first_date"
PARQUET_META_LAST_DATE_KEY = b"stockagent.last_date"
PARQUET_META_WRITE_TS_UTC_KEY = b"stockagent.write_ts_utc"
BLACKLIST_TRIGGER_TEXT = "possibly delisted; no timezone found"
UNAVAILABLE_TRIGGER_TEXTS = (
    BLACKLIST_TRIGGER_TEXT,
    "possibly delisted",
    "no price data found",
    "no data found, symbol may be delisted",
    "quote not found",
    "unavailable or delisted",
)
YF_DOWNLOAD_HARD_TIMEOUT_SECONDS = int(os.environ.get("YF_DOWNLOAD_HARD_TIMEOUT_SECONDS", "60"))
YF_CRYPTO_INTRADAY_INTERVAL = "15m"
YF_CRYPTO_INTRADAY_SECONDS = 15 * 60
YF_CRYPTO_MAX_LOOKBACK_DAYS = 59
DEFAULT_SYMBOLS: dict[str, list[tuple[str, str, str]]] = {
    "us_stocks": [
        ("AAPL", "Apple", "AAPL"),
        ("MSFT", "Microsoft", "MSFT"),
        ("NVDA", "NVIDIA", "NVDA"),
        ("AMZN", "Amazon", "AMZN"),
        ("GOOGL", "Alphabet", "GOOGL"),
        ("META", "Meta", "META"),
        ("TSLA", "Tesla", "TSLA"),
        ("NFLX", "Netflix", "NFLX"),
        ("AMD", "AMD", "AMD"),
        ("AVGO", "Broadcom", "AVGO"),
        ("COST", "Costco", "COST"),
        ("WMT", "Walmart", "WMT"),
        ("LLY", "Eli Lilly", "LLY"),
        ("UNH", "UnitedHealth", "UNH"),
        ("XOM", "Exxon Mobil", "XOM"),
        ("JNJ", "Johnson & Johnson", "JNJ"),
        ("PG", "Procter & Gamble", "PG"),
        ("KO", "Coca-Cola", "KO"),
        ("PEP", "PepsiCo", "PEP"),
        ("ORCL", "Oracle", "ORCL"),
        ("CRM", "Salesforce", "CRM"),
        ("ADBE", "Adobe", "ADBE"),
        ("CSCO", "Cisco", "CSCO"),
        ("QCOM", "Qualcomm", "QCOM"),
        ("INTC", "Intel", "INTC"),
        ("IBM", "IBM", "IBM"),
        ("UBER", "Uber", "UBER"),
        ("PLTR", "Palantir", "PLTR"),
        ("SHOP", "Shopify", "SHOP"),
        ("CRWD", "CrowdStrike", "CRWD"),
        ("JPM", "JPMorgan Chase", "JPM"),
        ("BAC", "Bank of America", "BAC"),
        ("GS", "Goldman Sachs", "GS"),
        ("V", "Visa", "V"),
        ("MA", "Mastercard", "MA"),
        ("SPY", "SPDR S&P 500 ETF", "SPY"),
        ("QQQ", "Invesco QQQ", "QQQ"),
        ("DIA", "SPDR Dow Jones ETF", "DIA"),
        ("IWM", "iShares Russell 2000 ETF", "IWM"),
        ("VTI", "Vanguard Total Stock Market ETF", "VTI"),
        ("VOO", "Vanguard S&P 500 ETF", "VOO"),
        ("XLK", "Technology Select Sector SPDR Fund", "XLK"),
        ("XLF", "Financial Select Sector SPDR Fund", "XLF"),
        ("XLE", "Energy Select Sector SPDR Fund", "XLE"),
        ("XLV", "Health Care Select Sector SPDR Fund", "XLV"),
        ("ARKK", "ARK Innovation ETF", "ARKK"),
        ("SMH", "VanEck Semiconductor ETF", "SMH"),
        ("SOXX", "iShares Semiconductor ETF", "SOXX"),
        ("TLT", "iShares 20+ Year Treasury Bond ETF", "TLT"),
        ("GLD", "SPDR Gold Shares", "GLD"),
        ("SLV", "iShares Silver Trust", "SLV"),
    ],
    "crypto": [
        ("BTCUSD", "Bitcoin", "BTC-USD"),
        ("ETHUSD", "Ethereum", "ETH-USD"),
        ("USDTUSD", "Tether", "USDT-USD"),
        ("USDCUSD", "USD Coin", "USDC-USD"),
        ("SOLUSD", "Solana", "SOL-USD"),
        ("BNBUSD", "BNB", "BNB-USD"),
        ("XRPUSD", "XRP", "XRP-USD"),
        ("DOGEUSD", "Dogecoin", "DOGE-USD"),
        ("ADAUSD", "Cardano", "ADA-USD"),
        ("AVAXUSD", "Avalanche", "AVAX-USD"),
        ("TRXUSD", "TRON", "TRX-USD"),
        ("TONUSD", "Toncoin", "TON11419-USD"),
        ("LINKUSD", "Chainlink", "LINK-USD"),
        ("DOTUSD", "Polkadot", "DOT-USD"),
        ("MATICUSD", "Polygon", "MATIC-USD"),
        ("LTCUSD", "Litecoin", "LTC-USD"),
        ("BCHUSD", "Bitcoin Cash", "BCH-USD"),
        ("ATOMUSD", "Cosmos", "ATOM-USD"),
        ("UNIUSD", "Uniswap", "UNI7083-USD"),
        ("XLMUSD", "Stellar", "XLM-USD"),
        ("ETCUSD", "Ethereum Classic", "ETC-USD"),
        ("FILUSD", "Filecoin", "FIL-USD"),
        ("NEARUSD", "NEAR Protocol", "NEAR-USD"),
        ("APTUSD", "Aptos", "APT21794-USD"),
        ("ALGOUSD", "Algorand", "ALGO-USD"),
        ("VETUSD", "VeChain", "VET-USD"),
        ("ICPUSD", "Internet Computer", "ICP-USD"),
        ("HBARUSD", "Hedera", "HBAR-USD"),
        ("SUIUSD", "Sui", "SUI20947-USD"),
        ("SEIUSD", "Sei", "SEI23149-USD"),
        ("AAVEUSD", "Aave", "AAVE-USD"),
        ("MKRUSD", "Maker", "MKR-USD"),
        ("ARBUSD", "Arbitrum", "ARB11841-USD"),
        ("OPUSD", "Optimism", "OP-USD"),
        ("PEPEUSD", "Pepe", "PEPE24478-USD"),
        ("SHIBUSD", "Shiba Inu", "SHIB-USD"),
        ("INJUSD", "Injective", "INJ-USD"),
        ("RENDERUSD", "Render", "RNDR-USD"),
        ("KASUSD", "Kaspa", "KAS-USD"),
        ("TIAUSD", "Celestia", "TIA22861-USD"),
    ],
    "forex": [
        ("EURUSD", "Euro / US Dollar", "EURUSD=X"),
        ("GBPUSD", "British Pound / US Dollar", "GBPUSD=X"),
        ("USDJPY", "US Dollar / Japanese Yen", "USDJPY=X"),
        ("AUDUSD", "Australian Dollar / US Dollar", "AUDUSD=X"),
        ("USDCAD", "US Dollar / Canadian Dollar", "USDCAD=X"),
        ("USDCHF", "US Dollar / Swiss Franc", "USDCHF=X"),
        ("NZDUSD", "New Zealand Dollar / US Dollar", "NZDUSD=X"),
        ("EURJPY", "Euro / Japanese Yen", "EURJPY=X"),
        ("EURGBP", "Euro / British Pound", "EURGBP=X"),
        ("EURCHF", "Euro / Swiss Franc", "EURCHF=X"),
        ("EURAUD", "Euro / Australian Dollar", "EURAUD=X"),
        ("EURNZD", "Euro / New Zealand Dollar", "EURNZD=X"),
        ("EURCAD", "Euro / Canadian Dollar", "EURCAD=X"),
        ("GBPJPY", "British Pound / Japanese Yen", "GBPJPY=X"),
        ("GBPCHF", "British Pound / Swiss Franc", "GBPCHF=X"),
        ("GBPAUD", "British Pound / Australian Dollar", "GBPAUD=X"),
        ("GBPCAD", "British Pound / Canadian Dollar", "GBPCAD=X"),
        ("AUDJPY", "Australian Dollar / Japanese Yen", "AUDJPY=X"),
        ("AUDNZD", "Australian Dollar / New Zealand Dollar", "AUDNZD=X"),
        ("AUDCAD", "Australian Dollar / Canadian Dollar", "AUDCAD=X"),
        ("AUDCHF", "Australian Dollar / Swiss Franc", "AUDCHF=X"),
        ("CADJPY", "Canadian Dollar / Japanese Yen", "CADJPY=X"),
        ("CHFJPY", "Swiss Franc / Japanese Yen", "CHFJPY=X"),
        ("NZDJPY", "New Zealand Dollar / Japanese Yen", "NZDJPY=X"),
        ("NZDCAD", "New Zealand Dollar / Canadian Dollar", "NZDCAD=X"),
        ("NZDCHF", "New Zealand Dollar / Swiss Franc", "NZDCHF=X"),
        ("CADCHF", "Canadian Dollar / Swiss Franc", "CADCHF=X"),
        ("USDSEK", "US Dollar / Swedish Krona", "USDSEK=X"),
        ("USDNOK", "US Dollar / Norwegian Krone", "USDNOK=X"),
        ("USDDKK", "US Dollar / Danish Krone", "USDDKK=X"),
        ("USDHKD", "US Dollar / Hong Kong Dollar", "USDHKD=X"),
        ("USDSGD", "US Dollar / Singapore Dollar", "USDSGD=X"),
        ("USDZAR", "US Dollar / South African Rand", "USDZAR=X"),
        ("USDMXN", "US Dollar / Mexican Peso", "USDMXN=X"),
        ("USDTRY", "US Dollar / Turkish Lira", "USDTRY=X"),
        ("USDINR", "US Dollar / Indian Rupee", "USDINR=X"),
        ("USDBRL", "US Dollar / Brazilian Real", "USDBRL=X"),
        ("USDKRW", "US Dollar / South Korean Won", "USDKRW=X"),
    ],
}

# Expanded offline fallback used when web discovery is rate-limited.
FOREX_EXPANDED_FALLBACK_YAHOO_SYMBOLS: tuple[str, ...] = (
    "EURUSD=X", "GBPUSD=X", "AUDUSD=X", "NZDUSD=X", "USDJPY=X", "USDCHF=X", "USDCAD=X",
    "EURJPY=X", "EURGBP=X", "EURCHF=X", "EURAUD=X", "EURNZD=X", "EURCAD=X", "EURSEK=X", "EURNOK=X",
    "GBPJPY=X", "GBPCHF=X", "GBPAUD=X", "GBPCAD=X", "GBPNZD=X", "GBPSEK=X", "GBPNOK=X",
    "AUDJPY=X", "AUDNZD=X", "AUDCAD=X", "AUDCHF=X", "AUDSGD=X",
    "NZDJPY=X", "NZDCAD=X", "NZDCHF=X", "NZDSGD=X",
    "CADJPY=X", "CADCHF=X", "CHFJPY=X",
    "USDSEK=X", "USDNOK=X", "USDDKK=X", "USDPLN=X", "USDHUF=X", "USDCZK=X",
    "USDHKD=X", "USDSGD=X", "USDTHB=X", "USDMYR=X", "USDIDR=X", "USDPHP=X", "USDTWD=X",
    "USDZAR=X", "USDMXN=X", "USDBRL=X", "USDCLP=X", "USDCOP=X", "USDPEN=X",
    "USDTRY=X", "USDINR=X", "USDKRW=X", "USDCNH=X", "USDCNY=X",
)

FALLBACK_SYMBOL_MANIFESTS: dict[str, Path] = {
    "tw_stocks": Path("configs") / "fallback_tw_stocks_symbols.csv",
    "us_stocks": Path("configs") / "fallback_us_stocks_symbols.csv",
    "crypto": Path("configs") / "fallback_crypto_symbols.csv",
    "forex": Path("configs") / "fallback_forex_symbols.csv",
}

TW_INCLUDED_SECTION_LABELS: dict[str, set[str]] = {
    "listed": {"股票", "特別股", "ETF"},
    "otc": {"股票", "特別股", "ETF"},
}


@dataclass(slots=True)
class SymbolRecord:
    code: str
    name: str
    market: str
    yahoo_symbol: str


@dataclass(slots=True)
class DownloadResult:
    asset_class: str
    code: str
    yahoo_symbol: str
    market: str
    status: str
    rows: int
    output_path: str | None
    message: str | None = None
    first_date: str | None = None
    last_date: str | None = None
    checked_through_date: str | None = None


@dataclass(slots=True)
class ExistingFileInfo:
    first_date: str | None
    last_date: str | None
    error: str | None
    columns: set[str]
    checked_through_date: str | None = None


@dataclass(slots=True)
class RepairCheck:
    record: SymbolRecord
    status: str
    output_path: Path
    first_date: str | None
    last_date: str | None
    repair_start_date: str | None
    merge_existing: bool = True
    message: str | None = None
    checked_through_date: str | None = None


@dataclass(slots=True)
class SymbolResolution:
    scheduled_records: list[SymbolRecord]
    manifest_records: list[SymbolRecord]
    new_codes: set[str]
    active_discovery_symbols: set[str] = field(default_factory=set)

    @property
    def active_records(self) -> list[SymbolRecord]:
        return self.scheduled_records


def _blacklist_file_path(output_dir: Path) -> Path:
    return output_dir / "yahoo_blacklist.txt"


def _whitelist_file_path(output_dir: Path) -> Path:
    return output_dir / "yahoo_whitelist.txt"


def _load_blacklist(path: Path) -> set[str]:
    if not path.exists():
        return set()
    symbols: set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            value = line.strip().upper()
            if not value or value.startswith("#"):
                continue
            symbols.add(value)
    return symbols


def _load_whitelist(path: Path) -> set[str]:
    if not path.exists():
        return set()
    symbols: set[str] = set()
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            value = line.strip().upper()
            if not value or value.startswith("#"):
                continue
            symbols.add(value)
    return symbols


def _append_blacklist_symbol(path: Path, symbol: str) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"{symbol}\n")


def _append_whitelist_symbol(path: Path, symbol: str) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"{symbol}\n")


def _rewrite_symbol_file(path: Path, symbols: set[str]) -> None:
    """Rewrite symbol list file with a deduplicated sorted snapshot."""
    ordered = sorted(sym for sym in symbols if sym)
    with path.open("w", encoding="utf-8") as handle:
        for sym in ordered:
            handle.write(f"{sym}\n")


def _clear_blacklist_candidates(
    *,
    blacklist_symbols: set[str],
    blacklist_path: Path,
    candidates: set[str],
    reason: str,
) -> int:
    """Remove candidate symbols from blacklist set+file and return cleared count."""
    if not candidates:
        return 0
    cleared = blacklist_symbols & {c.upper() for c in candidates if c}
    if not cleared:
        return 0
    blacklist_symbols -= cleared
    _rewrite_symbol_file(blacklist_path, blacklist_symbols)
    print(
        f"[repair] cleared {len(cleared)} blacklist entries for {reason} "
        "to allow fresh retry (will re-blacklist if still unavailable)"
    )
    return len(cleared)


def _blacklist_symbol(
    symbol: str,
    blacklist_symbols: set[str] | None,
    blacklist_path: Path | None,
    blacklist_lock: threading.Lock | None,
) -> None:
    if blacklist_symbols is None or blacklist_path is None:
        return

    normalized = symbol.strip().upper()
    if not normalized:
        return

    if blacklist_lock is None:
        if normalized in blacklist_symbols:
            return
        blacklist_symbols.add(normalized)
        _append_blacklist_symbol(blacklist_path, normalized)
        return

    with blacklist_lock:
        if normalized in blacklist_symbols:
            return
        blacklist_symbols.add(normalized)
        _append_blacklist_symbol(blacklist_path, normalized)


def _blacklist_record_symbols(
    raw_symbols: str,
    blacklist_symbols: set[str] | None,
    blacklist_path: Path | None,
    blacklist_lock: threading.Lock | None,
) -> None:
    candidates = [symbol for symbol in YAHOO_SYMBOL_SPLIT_PATTERN.split(raw_symbols.strip()) if symbol]
    if not candidates:
        candidates = [raw_symbols.strip()]
    for symbol in candidates:
        _blacklist_symbol(symbol, blacklist_symbols, blacklist_path, blacklist_lock)


def _whitelist_symbol(
    symbol: str,
    whitelist_symbols: set[str] | None,
    whitelist_path: Path | None,
    whitelist_lock: threading.Lock | None,
) -> None:
    if whitelist_symbols is None or whitelist_path is None:
        return

    normalized = symbol.strip().upper()
    if not normalized:
        return

    if whitelist_lock is None:
        if normalized in whitelist_symbols:
            return
        whitelist_symbols.add(normalized)
        _append_whitelist_symbol(whitelist_path, normalized)
        return

    with whitelist_lock:
        if normalized in whitelist_symbols:
            return
        whitelist_symbols.add(normalized)
        _append_whitelist_symbol(whitelist_path, normalized)
        return


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Yahoo Finance OHLCV data into asset-specific parquet directories.")
    parser.add_argument(
        "--mode",
        choices=["download", "repair", "daily-update"],
        default="download",
        help=(
            "download: grab the configured universe; "
            "repair: check existing parquet files and refill missing/stale data; "
            "daily-update: incremental refresh (same behavior as repair; crypto uses 15m bars)."
        ),
    )
    parser.add_argument(
        "--asset",
        choices=[*ASSET_CLASSES, "all"],
        default="all",
        help="Asset class to download. 'all' downloads tw_stocks, us_stocks, crypto, and forex.",
    )
    parser.add_argument("--output-root", default="data_yahoo", help="Root directory containing one subfolder per asset class.")
    parser.add_argument("--output-dir", default=None, help="Optional explicit output directory. Only valid when --asset is not 'all'.")
    parser.add_argument("--start-date", default="2000-01-01", help="Inclusive start date in YYYY-MM-DD.")
    parser.add_argument("--end-date", default=date.today().isoformat(), help="Inclusive end date in YYYY-MM-DD.")
    parser.add_argument(
        "--workers",
        type=int,
        default=max(1, os.cpu_count() or 1),
        help="Maximum parallel Yahoo requests.",
    )
    parser.add_argument(
        "--asset-workers",
        type=int,
        default=1,
        help="When --asset all, run up to this many asset classes in parallel.",
    )
    parser.add_argument("--retries", type=int, default=2, help="Retries per symbol when Yahoo temporarily fails.")
    parser.add_argument("--refresh", action="store_true", help="Re-download full history even if parquet exists.")
    parser.add_argument("--limit", type=int, default=None, help="Only process the first N symbols after filtering.")
    parser.add_argument("--symbols", nargs="+", default=None, help="Override symbols for the selected asset. Values can be codes or Yahoo symbols.")
    parser.add_argument("--symbols-file", default=None, help="Plain-text file with one symbol per line. Overrides default asset presets.")
    parser.add_argument(
        "--include-tw-delisted",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include TWSE/TPEx delisted symbol candidates in tw_stocks universe.",
    )
    parser.add_argument(
        "--include-us-delisted",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include US delisted symbols via Alpha Vantage LISTING_STATUS when API key is available.",
    )
    parser.add_argument(
        "--alpha-vantage-api-key",
        default=os.environ.get("ALPHAVANTAGE_API_KEY", "9IXWNBG0V9S16NPI"),
        help="Alpha Vantage API key for US delisted listing status (or set ALPHAVANTAGE_API_KEY env).",
    )
    parser.add_argument(
        "--repair-overlap-days",
        type=int,
        default=7,
        help="In repair mode, re-download this many overlap days before the local last date before merging.",
    )
    parser.add_argument(
        "--precheck-file-timeout-seconds",
        type=int,
        default=20,
        help="Max seconds to inspect one parquet during precheck; 0 disables timeout.",
    )
    parser.add_argument(
        "--repair-symbol-timeout-seconds",
        type=int,
        default=90,
        help="Max seconds to wait for one symbol's repair download; 0 disables per-symbol timeout.",
    )
    parser.add_argument(
        "--daily-stale-max-lag-days",
        type=int,
        default=14,
        help=(
            "Only for --mode daily-update: skip symbols whose local last date lags target end date "
            "by more than this many days. Set 0 to disable."
        ),
    )
    parser.add_argument(
        "--daily-discover-symbols",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Only for --mode daily-update: merge fresh stock listings and delisted candidates "
            "from upstream symbol sources into the local tracked universe."
        ),
    )
    parser.add_argument(
        "--daily-retry-known-missing-symbols",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Only for --mode daily-update: retry symbols that are already present in symbols.csv "
            "but still have no local parquet/whitelist entry. Default false prevents repeated "
            "downloads of known Yahoo-unavailable candidates."
        ),
    )
    parser.add_argument(
        "--retry-blacklisted-repair-symbols",
        action=argparse.BooleanOptionalAction,
        default=False,
        help=(
            "Retry symbols in yahoo_blacklist.txt during repair/daily-update by clearing them "
            "from the blacklist before download. Default false keeps unavailable symbols stable."
        ),
    )
    return parser.parse_args()


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def _today_str() -> str:
    return date.today().isoformat()


def _coerce_to_date(value: object) -> date | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    if isinstance(parsed, pd.Timestamp):
        return parsed.date()
    return parsed.to_pydatetime().date()


def _decode_metadata_date(value: bytes | str | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    value = str(value).strip()
    if not value:
        return None
    parsed = _coerce_to_date(value)
    return parsed.isoformat() if parsed is not None else None


def _frame_date_bounds(frame: pd.DataFrame) -> tuple[str | None, str | None]:
    if frame.empty or "date" not in frame.columns:
        return None, None
    dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
    if dates.empty:
        return None, None
    return dates.min().date().isoformat(), dates.max().date().isoformat()


def _previous_weekday(value: date) -> date:
    while value.weekday() >= 5:
        value -= timedelta(days=1)
    return value


def _effective_repair_target_end_date(asset_class: str, args: argparse.Namespace) -> date:
    requested = _parse_date(args.end_date or _today_str()).date()
    if getattr(args, "mode", "") != "daily-update":
        return requested
    if asset_class not in WEEKDAY_DAILY_ASSET_CLASSES:
        return requested
    return _previous_weekday(requested)


def _daily_update_target_adjustment_reason(asset_class: str, requested: date, effective: date) -> str | None:
    if requested == effective:
        return None
    if asset_class in WEEKDAY_DAILY_ASSET_CLASSES and requested.weekday() >= 5:
        return "weekend_non_trading_day"
    return "calendar_adjustment"


def _read_parquet_row_count(output_path: Path) -> int:
    if pq is not None:
        return int(pq.ParquetFile(output_path, memory_map=True).metadata.num_rows)
    if pl is not None:
        return int(pl.scan_parquet(str(output_path)).select(pl.len()).collect().item())
    return int(len(pd.read_parquet(output_path)))


def _read_parquet_frame(output_path: Path) -> pd.DataFrame:
    if pl is not None:
        return pl.read_parquet(str(output_path)).to_pandas()
    return pd.read_parquet(output_path)


def _date_bounds_from_arrow_table(table: object) -> tuple[str | None, str | None]:
    if pc is None or table.num_rows == 0:
        return None, None
    bounds = pc.min_max(table.column("date")).as_py()
    first = _coerce_to_date(bounds.get("min"))
    last = _coerce_to_date(bounds.get("max"))
    if first is None or last is None:
        return None, None
    return first.isoformat(), last.isoformat()


def _load_existing_file_info_pyarrow(output_path: Path) -> ExistingFileInfo | None:
    if pq is None:
        return None
    try:
        parquet_metadata = pq.read_metadata(output_path)
        arrow_schema = parquet_metadata.schema.to_arrow_schema()
        columns = set(arrow_schema.names)
        metadata = dict(parquet_metadata.metadata or {})
        metadata.update(arrow_schema.metadata or {})
        checked_through = _decode_metadata_date(metadata.get(PARQUET_META_CHECKED_THROUGH_KEY))
    except Exception as exc:
        return ExistingFileInfo(None, None, f"schema_error: {exc}", set())

    if "date" not in columns:
        return ExistingFileInfo(None, None, "empty", columns, checked_through)

    try:
        date_table = pq.read_table(output_path, columns=["date"], memory_map=True)
        first_date, last_date = _date_bounds_from_arrow_table(date_table)
    except Exception as exc:
        return ExistingFileInfo(None, None, f"read_error: {exc}", columns, checked_through)
    if first_date is None or last_date is None:
        return ExistingFileInfo(None, None, "no_valid_date", columns, checked_through)
    return ExistingFileInfo(first_date, last_date, None, columns, checked_through)


def _load_existing_file_info_polars(output_path: Path) -> ExistingFileInfo | None:
    if pl is None:
        return None
    try:
        lazy = pl.scan_parquet(str(output_path))
        schema = lazy.collect_schema()
        columns = set(schema.names() if hasattr(schema, "names") else schema.keys())
    except Exception as exc:
        return ExistingFileInfo(None, None, f"schema_error: {exc}", set())

    if "date" not in columns:
        return ExistingFileInfo(None, None, "empty", columns)

    try:
        bounds = lazy.select(
            pl.col("date").min().alias("first_date"),
            pl.col("date").max().alias("last_date"),
        ).collect()
    except Exception as exc:
        return ExistingFileInfo(None, None, f"read_error: {exc}", columns)
    if bounds.is_empty():
        return ExistingFileInfo(None, None, "empty", columns)
    row = bounds.to_dicts()[0]
    first = _coerce_to_date(row.get("first_date"))
    last = _coerce_to_date(row.get("last_date"))
    if first is None or last is None:
        return ExistingFileInfo(None, None, "no_valid_date", columns)
    return ExistingFileInfo(first.isoformat(), last.isoformat(), None, columns)


def _prepare_arrow_table_for_write(frame: pd.DataFrame) -> object:
    ordered_columns = [column for column in BASE_OUTPUT_COLUMNS if column in frame.columns] + [
        column for column in frame.columns if column not in set(BASE_OUTPUT_COLUMNS)
    ]
    if pl is not None:
        polars_frame = pl.from_pandas(frame[ordered_columns])
        if "date" in polars_frame.columns:
            polars_frame = (
                polars_frame.with_columns(pl.col("date").cast(pl.Datetime("us"), strict=False))
                .drop_nulls("date")
                .sort("date")
                .unique(subset=["date"], keep="last", maintain_order=True)
            )
        return polars_frame.select([column for column in ordered_columns if column in polars_frame.columns]).to_arrow()
    assert pa is not None
    return pa.Table.from_pandas(frame[ordered_columns], preserve_index=False)


def _fsync_file(path: Path) -> None:
    try:
        with path.open("rb") as handle:
            os.fsync(handle.fileno())
    except OSError:
        pass


def _fsync_directory(path: Path) -> None:
    if os.name != "posix":
        return
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_feature_parquet_atomic(
    frame: pd.DataFrame,
    output_path: Path,
    *,
    asset_class: str,
    requested_end_date: str,
) -> tuple[str | None, str | None]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    first_date, last_date = _frame_date_bounds(frame)
    handle = tempfile.NamedTemporaryFile(
        prefix=f".{output_path.name}.",
        suffix=".tmp",
        dir=output_path.parent,
        delete=False,
    )
    tmp_path = Path(handle.name)
    handle.close()
    try:
        if pa is not None and pq is not None:
            table = _prepare_arrow_table_for_write(frame)
            metadata = dict(table.schema.metadata or {})
            metadata.update(
                {
                    PARQUET_META_SOURCE_KEY: b"yahoo",
                    PARQUET_META_ASSET_CLASS_KEY: asset_class.encode("utf-8"),
                    PARQUET_META_REQUESTED_END_KEY: requested_end_date.encode("utf-8"),
                    PARQUET_META_CHECKED_THROUGH_KEY: requested_end_date.encode("utf-8"),
                    PARQUET_META_WRITE_TS_UTC_KEY: (
                        datetime.now(timezone.utc)
                        .replace(microsecond=0)
                        .isoformat()
                        .encode("utf-8")
                    ),
                }
            )
            if first_date is not None:
                metadata[PARQUET_META_FIRST_DATE_KEY] = first_date.encode("utf-8")
            if last_date is not None:
                metadata[PARQUET_META_LAST_DATE_KEY] = last_date.encode("utf-8")
            table = table.replace_schema_metadata(metadata)
            pq.write_table(
                table,
                tmp_path,
                compression="snappy",
                use_dictionary=True,
                write_statistics=True,
                row_group_size=128_000,
                coerce_timestamps="us",
                allow_truncated_timestamps=True,
            )
        elif pl is not None:
            pl.from_pandas(frame).write_parquet(str(tmp_path), compression="snappy", statistics=True)
        else:
            frame.to_parquet(tmp_path, index=False)
        _fsync_file(tmp_path)
        os.replace(tmp_path, output_path)
        _fsync_directory(output_path.parent)
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise
    return first_date, last_date


class PrecheckTimeoutError(TimeoutError):
    pass


def _precheck_worker_main(conn) -> None:
    try:
        while True:
            payload = conn.recv()
            if payload is None:
                break

            output_path = Path(payload)
            try:
                result = _load_existing_file_info(output_path)
                conn.send(("ok", result))
            except Exception as exc:
                conn.send(("err", str(exc)))
    finally:
        conn.close()


class PrecheckLoader:
    def __init__(self) -> None:
        # Use spawn unconditionally to avoid fork-from-thread deadlocks when asset_workers > 1.
        self._ctx = mp.get_context("spawn")
        self._parent_conn = None
        self._process = None
        self._start()

    def _start(self) -> None:
        parent_conn, child_conn = self._ctx.Pipe(duplex=True)
        process = self._ctx.Process(target=_precheck_worker_main, args=(child_conn,), daemon=True)
        process.start()
        child_conn.close()
        self._parent_conn = parent_conn
        self._process = process

    def _restart(self) -> None:
        self.close()
        self._start()

    def load_with_timeout(self, output_path: Path, timeout_seconds: int) -> ExistingFileInfo:
        if timeout_seconds <= 0:
            return _load_existing_file_info(output_path)

        if self._process is None or not self._process.is_alive():
            self._restart()

        assert self._parent_conn is not None
        try:
            self._parent_conn.send(str(output_path))
        except Exception:
            self._restart()
            assert self._parent_conn is not None
            self._parent_conn.send(str(output_path))
        if not self._parent_conn.poll(timeout_seconds):
            self._restart()
            raise PrecheckTimeoutError(f"precheck timed out after {timeout_seconds}s")

        try:
            status, payload = self._parent_conn.recv()
        except Exception:
            self._restart()
            raise RuntimeError("precheck worker pipe broken; worker restarted")
        if status == "ok":
            return payload

        raise RuntimeError(str(payload))

    def close(self) -> None:
        if self._parent_conn is not None:
            try:
                self._parent_conn.send(None)
            except Exception:
                pass
            try:
                self._parent_conn.close()
            except Exception:
                pass
            self._parent_conn = None

        if self._process is not None:
            if self._process.is_alive():
                self._process.terminate()
            self._process.join(timeout=1)
            self._process = None


def _normalize_download_frame(frame: pd.DataFrame, *, keep_zero_volume: bool = True) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=BASE_OUTPUT_COLUMNS)

    frame = frame.reset_index()
    if isinstance(frame.columns, pd.MultiIndex):
        flattened_columns: list[str] = []
        for column in frame.columns:
            if not isinstance(column, tuple):
                flattened_columns.append(str(column))
                continue

            primary = str(column[0]).strip()
            secondary = str(column[1]).strip() if len(column) > 1 else ""
            flattened_columns.append(primary or secondary)
        frame.columns = flattened_columns

    renamed = frame.rename(
        columns={
            "Date": "date",
            "Datetime": "date",
            "index": "date",
            "Open": "open",
            "High": "max",
            "Low": "min",
            "Close": "close",
            "Adj Close": "adjclose",
            "AdjClose": "adjclose",
            "Volume": "Trading_Volume",
        }
    )

    if "date" not in renamed.columns:
        return pd.DataFrame(columns=BASE_OUTPUT_COLUMNS)

    # Preserve canonical OHLCV columns and keep any extra Yahoo columns when available.
    extra_columns = [column for column in renamed.columns if column not in set(BASE_OUTPUT_COLUMNS)]
    ordered_columns = [column for column in BASE_OUTPUT_COLUMNS if column in renamed.columns] + extra_columns
    normalized = renamed[ordered_columns].copy()
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce").dt.tz_localize(None)
    numeric_columns = [column for column in normalized.columns if column != "date"]
    for column in numeric_columns:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    normalized = normalized.dropna(subset=["date", "close"]).sort_values("date").drop_duplicates(subset=["date"], keep="last")
    empty_extra_columns = [
        column
        for column in normalized.columns
        if column not in set(BASE_OUTPUT_COLUMNS) and normalized[column].isna().all()
    ]
    if empty_extra_columns:
        normalized = normalized.drop(columns=empty_extra_columns)
    if not keep_zero_volume and "Trading_Volume" in normalized.columns:
        volume = pd.to_numeric(normalized["Trading_Volume"], errors="coerce")
        if volume.isna().all() or volume.fillna(0).eq(0).all():
            normalized = normalized.drop(columns=["Trading_Volume"])

    return normalized.reset_index(drop=True)


def _frame_matches_expected_interval(frame: pd.DataFrame, expected_seconds: int) -> bool:
    if frame.empty or "date" not in frame.columns:
        return True

    parsed = pd.to_datetime(frame["date"], errors="coerce").dropna().sort_values()
    if len(parsed) < 3:
        return True

    deltas = parsed.diff().dropna().dt.total_seconds()
    deltas = deltas[deltas > 0]
    if deltas.empty:
        return True

    median_delta = float(deltas.median())
    large_gap_share = float((deltas >= 12 * 60 * 60).mean())
    if large_gap_share > 0.05:
        return False
    midnight_share = float(
        ((parsed.dt.hour == 0) & (parsed.dt.minute == 0) & (parsed.dt.second == 0)).mean()
    )
    if midnight_share > 0.95 and median_delta >= 12 * 60 * 60:
        return False
    return median_delta <= expected_seconds * 4


def _normalize_us_yahoo_symbol(symbol: str) -> str:
    return symbol.strip().upper().replace(".", "-")


def _http_get_text(url: str, timeout: int = 30) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/137.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        raw = response.read()
        charset = response.headers.get_content_charset() or "utf-8"
    try:
        return raw.decode(charset, errors="ignore")
    except LookupError:
        return raw.decode("utf-8", errors="ignore")


def _read_html_tables(url: str, timeout: int = 30) -> list[pd.DataFrame]:
    html = _http_get_text(url, timeout=timeout)
    return pd.read_html(io.StringIO(html))


def _http_get_json(url: str, timeout: int = 30) -> object:
    text = _http_get_text(url, timeout=timeout)
    return json.loads(text)


def _fetch_with_hard_timeout(fn, *args, timeout: int = 60, **kwargs):
    """Run fn in a daemon thread; raise concurrent.futures.TimeoutError if it takes longer than timeout seconds.

    Unlike urlopen(timeout=N), this is a true wall-clock timeout that covers DNS resolution,
    TCP handshake, and any other blocking operation inside fn.
    The worker thread is abandoned (not waited on) so we never block on shutdown.
    """
    ex = ThreadPoolExecutor(max_workers=1)
    try:
        return ex.submit(fn, *args, **kwargs).result(timeout=timeout)
    finally:
        ex.shutdown(wait=False)


def _extract_tw_codes_from_tables(url: str) -> set[str]:
    codes: set[str] = set()
    try:
        tables = _read_html_tables(url)
    except Exception:
        return codes

    for table in tables:
        for column in table.columns:
            values = table[column].astype(str)
            for value in values:
                for match in TW_GENERIC_CODE_PATTERN.finditer(value):
                    codes.add(match.group(1).upper())
    return codes


def _load_tw_delisted_symbols() -> list[SymbolRecord]:
    codes: set[str] = set()
    for url in (*TWSE_DELISTED_SOURCES, *TPEX_DELISTED_SOURCES):
        codes.update(_extract_tw_codes_from_tables(url))

    records: list[SymbolRecord] = []
    for code in sorted(codes):
        records.append(SymbolRecord(code=f"{code}_TW", name=f"{code} (delisted)", market="tw_delisted", yahoo_symbol=f"{code}.TW"))
        records.append(SymbolRecord(code=f"{code}_TWO", name=f"{code} (delisted)", market="tw_delisted", yahoo_symbol=f"{code}.TWO"))
    return records

def _record_from_input(asset_class: str, raw_symbol: str) -> SymbolRecord:
    value = raw_symbol.strip()
    if not value:
        raise ValueError("Symbol cannot be empty")

    upper_value = value.upper()
    if asset_class == "crypto":
        if upper_value.endswith("-USD"):
            yahoo_symbol = upper_value
        elif upper_value.endswith("USD") and len(upper_value) > 3:
            yahoo_symbol = f"{upper_value[:-3]}-USD"
        else:
            yahoo_symbol = f"{upper_value}-USD"
        code = yahoo_symbol.replace("-", "")
        return SymbolRecord(code=code, name=code, market=asset_class, yahoo_symbol=yahoo_symbol)
    if asset_class == "forex":
        yahoo_symbol = upper_value if upper_value.endswith("=X") else f"{upper_value}=X"
        code = yahoo_symbol.replace("=X", "")
        return SymbolRecord(code=code, name=code, market=asset_class, yahoo_symbol=yahoo_symbol)
    if asset_class == "tw_stocks":
        if upper_value.endswith(".TW") or upper_value.endswith(".TWO"):
            code = upper_value.split(".", maxsplit=1)[0]
            return SymbolRecord(code=code, name=code, market=asset_class, yahoo_symbol=upper_value)
        return SymbolRecord(code=upper_value, name=upper_value, market=asset_class, yahoo_symbol=f"{upper_value}.TW")

    return SymbolRecord(code=upper_value, name=upper_value, market=asset_class, yahoo_symbol=upper_value)


def _record_from_local_code(asset_class: str, code: str) -> SymbolRecord:
    normalized = code.strip().upper()
    if asset_class == "tw_stocks" and TW_GENERIC_CODE_PATTERN.fullmatch(normalized):
        return SymbolRecord(code=normalized, name=normalized, market=asset_class, yahoo_symbol=f"{normalized}.TW,{normalized}.TWO")
    return _record_from_input(asset_class, normalized)


def _candidate_symbols_for_record(asset_class: str, record: SymbolRecord) -> list[str]:
    candidates = [symbol.strip() for symbol in YAHOO_SYMBOL_SPLIT_PATTERN.split(record.yahoo_symbol.strip()) if symbol.strip()]
    if not candidates:
        candidates = [record.yahoo_symbol.strip()]

    if asset_class == "forex":
        forex_candidates = [symbol.upper() for symbol in candidates if FOREX_TICKER_PATTERN.match(symbol.upper())]
        for symbol in forex_candidates:
            pair = symbol.replace("=X", "")
            if len(pair) != 6:
                continue
            inverse = f"{pair[3:]}{pair[:3]}=X"
            if inverse not in candidates:
                candidates.append(inverse)

    return candidates


def _available_candidate_symbols(
    asset_class: str,
    record: SymbolRecord,
    blacklist_symbols: set[str] | None,
) -> list[str]:
    candidates = _candidate_symbols_for_record(asset_class, record)
    if blacklist_symbols is None:
        return candidates
    return [symbol for symbol in candidates if symbol.upper() not in blacklist_symbols]


def _all_candidate_symbols_blacklisted(
    asset_class: str,
    record: SymbolRecord,
    blacklist_symbols: set[str],
) -> bool:
    candidates = _candidate_symbols_for_record(asset_class, record)
    return bool(candidates) and not _available_candidate_symbols(asset_class, record, blacklist_symbols)


def _is_delisted_record(record: SymbolRecord) -> bool:
    return "delisted" in record.market.lower()


def _skip_status_for_unavailable_record(record: SymbolRecord) -> str:
    return "delisted_skip" if _is_delisted_record(record) else "not_found_skip"


def _delisted_market_for_asset(asset_class: str) -> str:
    if asset_class == "tw_stocks":
        return "tw_delisted"
    if asset_class == "us_stocks":
        return "us_delisted"
    return f"{asset_class}_delisted"


def _candidate_symbol_keys(asset_class: str, record: SymbolRecord) -> set[str]:
    return {symbol.upper() for symbol in _candidate_symbols_for_record(asset_class, record) if symbol}


def _record_overlaps_symbols(asset_class: str, record: SymbolRecord, symbols: set[str]) -> bool:
    return bool(_candidate_symbol_keys(asset_class, record) & symbols)


def _is_delisted_archive_record(record: SymbolRecord) -> bool:
    return record.code.upper().endswith(("_DL", "_TW", "_TWO"))


def _with_market(record: SymbolRecord, market: str) -> SymbolRecord:
    if record.market == market:
        return record
    return SymbolRecord(code=record.code, name=record.name, market=market, yahoo_symbol=record.yahoo_symbol)


def _apply_daily_listing_state(
    asset_class: str,
    records: list[SymbolRecord],
    active_symbols: set[str],
    delisted_symbols: set[str],
) -> list[SymbolRecord]:
    if asset_class not in {"tw_stocks", "us_stocks"} or (not active_symbols and not delisted_symbols):
        return records

    delisted_market = _delisted_market_for_asset(asset_class)
    updated: list[SymbolRecord] = []
    for record in records:
        overlaps_active = _record_overlaps_symbols(asset_class, record, active_symbols)
        overlaps_delisted = _record_overlaps_symbols(asset_class, record, delisted_symbols)
        if _is_delisted_record(record):
            if overlaps_active and not _is_delisted_archive_record(record):
                updated.append(_with_market(record, asset_class))
            else:
                updated.append(record)
        elif overlaps_delisted and not overlaps_active:
            updated.append(_with_market(record, delisted_market))
        else:
            updated.append(record)
    return updated


def _captured_indicates_unavailable(captured: str) -> bool:
    return any(trigger in captured for trigger in UNAVAILABLE_TRIGGER_TEXTS)


def _load_tw_symbols_from_local_manifest() -> list[SymbolRecord]:
    manifest_path = Path("data_parquet") / "symbols.csv"
    if not manifest_path.exists():
        return []

    frame = pd.read_csv(manifest_path, dtype=str).fillna("")
    required_columns = {"code", "name", "market", "yahoo_symbol"}
    if not required_columns.issubset(frame.columns):
        return []

    records: list[SymbolRecord] = []
    for row in frame.itertuples(index=False):
        code = str(getattr(row, "code", "")).strip().upper()
        yahoo_symbol = str(getattr(row, "yahoo_symbol", "")).strip().upper()
        if not code or not yahoo_symbol:
            continue
        records.append(
            SymbolRecord(
                code=code,
                name=str(getattr(row, "name", code)).strip() or code,
                market=str(getattr(row, "market", "tw_stocks")).strip() or "tw_stocks",
                yahoo_symbol=yahoo_symbol,
            )
        )
    return records


def _load_tw_symbols_from_local_parquet() -> list[SymbolRecord]:
    data_dir = Path("data_parquet")
    if not data_dir.exists():
        return []

    records: list[SymbolRecord] = []
    seen_codes: set[str] = set()
    suffix = "_features.parquet"
    for output_path in sorted(data_dir.glob(f"*{suffix}")):
        code = output_path.name[: -len(suffix)].strip().upper()
        if not code or code in seen_codes or not TW_GENERIC_CODE_PATTERN.fullmatch(code):
            continue
        seen_codes.add(code)
        records.append(
            SymbolRecord(
                code=code,
                name=code,
                market="tw_stocks",
                yahoo_symbol=f"{code}.TW,{code}.TWO",
            )
        )
    return records


def _load_tw_symbols_from_exchange() -> list[SymbolRecord]:
    records: list[SymbolRecord] = []
    seen_codes: set[str] = set()
    for market, (url, suffix) in TWSE_SOURCES.items():
        tables = _read_html_tables(url)
        for table in tables:
            current_section: str | None = None
            for row in table.fillna("").itertuples(index=False):
                values = [str(value).strip() for value in row]
                nonempty = [value for value in values if value]
                if nonempty and len(set(nonempty)) == 1 and len(nonempty) >= 3:
                    current_section = nonempty[0]
                    continue
                if current_section not in TW_INCLUDED_SECTION_LABELS.get(market, set()):
                    continue
                if len(values) < 4:
                    continue
                raw_value = values[0]
                market_value = values[3]
                if market == "listed" and market_value != "上市":
                    continue
                if market == "otc" and market_value != "上櫃":
                    continue
                match = TWSE_CODE_NAME_PATTERN.match(raw_value)
                if not match:
                    continue
                code = match.group("code").upper()
                if code in seen_codes:
                    continue
                seen_codes.add(code)
                records.append(
                    SymbolRecord(
                        code=code,
                        name=match.group("name").strip(),
                        market=market,
                        yahoo_symbol=f"{code}{suffix}",
                    )
                )
    return records


def _load_us_symbols_from_web(timeout: int = 60) -> list[SymbolRecord]:
    records: list[SymbolRecord] = []
    seen: set[str] = set()

    nasdaq_url, other_url = US_SYMBOL_SOURCES
    nasdaq = pd.read_csv(io.StringIO(_http_get_text(nasdaq_url, timeout=timeout)), sep="|", dtype=str, engine="python")
    other = pd.read_csv(io.StringIO(_http_get_text(other_url, timeout=timeout)), sep="|", dtype=str, engine="python")

    for frame, symbol_col, name_col, test_issue_col in (
        (nasdaq, "Symbol", "Security Name", "Test Issue"),
        (other, "ACT Symbol", "Security Name", "Test Issue"),
    ):
        if symbol_col not in frame.columns:
            continue

        for _, row in frame.iterrows():
            raw_symbol = str(row.get(symbol_col, "")).strip().upper()
            if not raw_symbol:
                continue
            if raw_symbol in {"FILE CREATION TIME", "SYMBOL"}:
                continue
            if test_issue_col in frame.columns:
                test_issue = str(row.get(test_issue_col, "")).strip().upper()
                if test_issue == "Y":
                    continue
            if not US_VALID_SYMBOL_PATTERN.match(raw_symbol):
                continue

            yahoo_symbol = _normalize_us_yahoo_symbol(raw_symbol)
            if yahoo_symbol in seen:
                continue
            seen.add(yahoo_symbol)
            name = str(row.get(name_col, "")).strip() or yahoo_symbol
            records.append(SymbolRecord(code=yahoo_symbol, name=name, market="us_stocks", yahoo_symbol=yahoo_symbol))
    return records


def _load_us_delisted_from_alpha_vantage(api_key: str, timeout: int = 60) -> list[SymbolRecord]:
    if not api_key:
        return []

    query = urlencode({"function": "LISTING_STATUS", "state": "delisted", "apikey": api_key})
    url = f"{ALPHA_VANTAGE_LISTING_STATUS_URL}?{query}"
    frame = pd.read_csv(io.StringIO(_http_get_text(url, timeout=timeout)), dtype=str)
    if "symbol" not in frame.columns:
        return []

    records: list[SymbolRecord] = []
    for row in frame.itertuples(index=False):
        symbol = str(getattr(row, "symbol", "")).strip().upper()
        if not symbol or not US_VALID_SYMBOL_PATTERN.match(symbol):
            continue
        yahoo_symbol = _normalize_us_yahoo_symbol(symbol)
        name = str(getattr(row, "name", yahoo_symbol)).strip() or yahoo_symbol
        records.append(SymbolRecord(code=f"{yahoo_symbol}_DL", name=name, market="us_delisted", yahoo_symbol=yahoo_symbol))
    return records


def _load_crypto_symbols_from_coingecko() -> list[SymbolRecord]:
    payload = _http_get_json(COINGECKO_COINS_LIST_URL)
    if not isinstance(payload, list):
        return []

    records: list[SymbolRecord] = []
    seen_codes: set[str] = set()
    for item in payload:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol", "")).strip().lower()
        if not CRYPTO_SYMBOL_PATTERN.match(symbol):
            continue
        upper_symbol = symbol.upper()
        code = f"{upper_symbol}USD"
        if code in seen_codes:
            continue
        seen_codes.add(code)
        name = str(item.get("name", upper_symbol)).strip() or upper_symbol
        records.append(SymbolRecord(code=code, name=name, market="crypto", yahoo_symbol=f"{upper_symbol}-USD"))
    return records


def _load_forex_symbols_from_yahoo_page(max_retries: int = 3) -> list[SymbolRecord]:
    last_error: Exception | None = None
    for attempt in range(max(1, max_retries)):
        try:
            text = _http_get_text(YAHOO_CURRENCIES_URL)
            matches = sorted(set(FOREX_YAHOO_SYMBOL_PATTERN.findall(text)))
            records: list[SymbolRecord] = []
            for yahoo_symbol in matches:
                code = yahoo_symbol.replace("=X", "")
                records.append(SymbolRecord(code=code, name=code, market="forex", yahoo_symbol=yahoo_symbol))
            if records:
                return records
            last_error = RuntimeError("Yahoo currencies page returned no forex symbols.")
        except (HTTPError, URLError, TimeoutError, ValueError) as exc:
            last_error = exc
            if attempt < max_retries - 1:
                time.sleep(1.2 * (attempt + 1))
                continue
            break

    if last_error is None:
        raise RuntimeError("Failed to load forex symbols from Yahoo currencies page.")
    raise RuntimeError(str(last_error))


def _load_symbols_from_file(asset_class: str, file_path: str) -> list[SymbolRecord]:
    records: list[SymbolRecord] = []
    with Path(file_path).open("r", encoding="utf-8") as handle:
        for line in handle:
            value = line.strip()
            if not value or value.startswith("#"):
                continue
            records.append(_record_from_input(asset_class, value))
    return records


def _load_cached_symbols_from_manifest(manifest_path: Path, asset_class: str) -> list[SymbolRecord]:
    if not manifest_path.exists():
        return []

    try:
        frame = pd.read_csv(manifest_path, dtype=str).fillna("")
    except Exception:
        return []

    if "yahoo_symbol" not in frame.columns:
        return []

    records: list[SymbolRecord] = []
    for row in frame.itertuples(index=False):
        yahoo_symbol = str(getattr(row, "yahoo_symbol", "")).strip().upper()
        if not FOREX_TICKER_PATTERN.match(yahoo_symbol):
            continue
        code = str(getattr(row, "code", "")).strip().upper() or yahoo_symbol.replace("=X", "")
        name = str(getattr(row, "name", code)).strip() or code
        market = str(getattr(row, "market", asset_class)).strip() or asset_class
        records.append(SymbolRecord(code=code, name=name, market=market, yahoo_symbol=yahoo_symbol))
    return records


def _load_cached_symbols_from_whitelist(whitelist_path: Path, asset_class: str) -> list[SymbolRecord]:
    if not whitelist_path.exists():
        return []

    records: list[SymbolRecord] = []
    with whitelist_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            yahoo_symbol = line.strip().upper()
            if not FOREX_TICKER_PATTERN.match(yahoo_symbol):
                continue
            code = yahoo_symbol.replace("=X", "")
            records.append(SymbolRecord(code=code, name=code, market=asset_class, yahoo_symbol=yahoo_symbol))
    return records


def _load_forex_expanded_fallback() -> list[SymbolRecord]:
    records: list[SymbolRecord] = []
    for yahoo_symbol in FOREX_EXPANDED_FALLBACK_YAHOO_SYMBOLS:
        code = yahoo_symbol.replace("=X", "")
        records.append(SymbolRecord(code=code, name=code, market="forex", yahoo_symbol=yahoo_symbol))
    return records


def _load_local_tracked_records(asset_class: str, output_dir: Path, cached: list[SymbolRecord]) -> list[SymbolRecord]:
    """For daily-update, prefer symbols that are already tracked locally.

    This avoids reprocessing huge historical universes from stale manifests.
    """
    by_code = {record.code: record for record in cached}
    records: list[SymbolRecord] = []
    seen_codes: set[str] = set()

    for output_path in output_dir.glob("*_features.parquet"):
        suffix = "_features.parquet"
        if not output_path.name.endswith(suffix):
            continue
        code = output_path.name[: -len(suffix)].strip().upper()
        if not code or code in seen_codes:
            continue
        cached_record = by_code.get(code)
        if cached_record is not None:
            records.append(cached_record)
        else:
            if asset_class == "forex":
                # data_yahoo/forex may contain Frankfurter cross-rate files
                # from older daily scripts; do not treat them as Yahoo tickers.
                continue
            try:
                records.append(_record_from_local_code(asset_class, code))
            except Exception:
                continue
        seen_codes.add(code)

    whitelist_symbols = _load_whitelist(_whitelist_file_path(output_dir))
    for yahoo_symbol in sorted(whitelist_symbols):
        try:
            record = _record_from_input(asset_class, yahoo_symbol)
        except Exception:
            continue
        if record.code in seen_codes:
            continue
        cached_record = by_code.get(record.code)
        records.append(cached_record if cached_record is not None else record)
        seen_codes.add(record.code)

    return records


def _records_from_defaults(asset_class: str) -> list[SymbolRecord]:
    default_items = DEFAULT_SYMBOLS.get(asset_class)
    if not default_items:
        return []
    return [
        SymbolRecord(code=code, name=name, market=asset_class, yahoo_symbol=yahoo_symbol)
        for code, name, yahoo_symbol in default_items
    ]


def _load_repo_symbol_fallback(asset_class: str) -> list[SymbolRecord]:
    manifest_path = FALLBACK_SYMBOL_MANIFESTS.get(asset_class)
    if manifest_path is None:
        return []
    return _load_symbols_from_manifest_csv(manifest_path, asset_class)


def _resolve_cached_manifest(output_dir: Path, asset_class: str) -> list[SymbolRecord]:
    return _load_symbols_from_manifest_csv(output_dir / "symbols.csv", asset_class)


def _asset_output_is_bootstrap_empty(output_dir: Path) -> bool:
    manifest_path = output_dir / "symbols.csv"
    if manifest_path.exists():
        return False
    if any(output_dir.glob("*_features.parquet")):
        return False
    if _blacklist_file_path(output_dir).exists():
        return False
    if _whitelist_file_path(output_dir).exists():
        return False
    return True


def _use_daily_update_cache_if_available(
    asset_class: str,
    args: argparse.Namespace,
    cached: list[SymbolRecord],
) -> list[SymbolRecord] | None:
    is_daily_update = getattr(args, "mode", "") == "daily-update"
    if cached and is_daily_update and not getattr(args, "daily_discover_symbols", True):
        print(f"[symbols] daily-update: cached manifest {asset_class} ({len(cached)} symbols, skipping HTTP)")
        return cached
    return None


def _resolve_tw_symbols(args: argparse.Namespace, cached: list[SymbolRecord]) -> list[SymbolRecord]:
    cached_daily = _use_daily_update_cache_if_available("tw_stocks", args, cached)
    if cached_daily is not None:
        return cached_daily

    records: list[SymbolRecord] = []
    local_manifest_records = _load_tw_symbols_from_local_manifest()
    local_parquet_records = _load_tw_symbols_from_local_parquet()
    repo_fallback_records = _load_repo_symbol_fallback("tw_stocks")
    try:
        print(f"[symbols] fetching tw_stocks from exchange (timeout=60s)…")
        fetched = _fetch_with_hard_timeout(_load_tw_symbols_from_exchange, timeout=60)
        records.extend(fetched)
        print(f"[symbols] loaded {len(fetched)} tw_stocks symbols from exchange")
    except Exception as exc:
        print(f"[symbols] failed to load tw symbols from exchange: {exc}")

    if records:
        # Keep historical/local names and stale but previously-tracked symbols in
        # the manifest while still allowing new exchange listings to be added.
        records.extend(local_manifest_records)
        records.extend(local_parquet_records)
    elif repo_fallback_records:
        print(f"[symbols] using repo fallback manifest for tw_stocks ({len(repo_fallback_records)} symbols)")
        records = repo_fallback_records
    elif local_manifest_records:
        print(f"[symbols] using local data_parquet manifest for tw_stocks ({len(local_manifest_records)} symbols)")
        records = local_manifest_records
    elif local_parquet_records:
        print(f"[symbols] fallback to local data_parquet codes for tw_stocks ({len(local_parquet_records)} symbols)")
        records = local_parquet_records
    else:
        records = cached or []

    if args.include_tw_delisted:
        try:
            records.extend(_fetch_with_hard_timeout(_load_tw_delisted_symbols, timeout=60))
        except Exception as exc:
            print(f"[symbols] failed to load tw delisted list: {exc}")
    return records


def _discover_daily_stock_records(
    asset_class: str,
    args: argparse.Namespace,
    cached: list[SymbolRecord],
) -> list[SymbolRecord]:
    discovery_args = argparse.Namespace(**vars(args))
    discovery_args.mode = "download"
    discovery_args.limit = None
    discovery_args.symbols = None
    discovery_args.symbols_file = None

    if asset_class == "tw_stocks":
        return _resolve_tw_symbols(discovery_args, cached)
    if asset_class == "us_stocks":
        return _resolve_us_symbols(discovery_args, cached)
    return []


def _resolve_us_symbols(args: argparse.Namespace, cached: list[SymbolRecord]) -> list[SymbolRecord]:
    cached_daily = _use_daily_update_cache_if_available("us_stocks", args, cached)
    if cached_daily is not None:
        return cached_daily

    repo_fallback_records = _load_repo_symbol_fallback("us_stocks")
    records = _records_from_defaults("us_stocks")
    try:
        print("[symbols] fetching us_stocks from Nasdaq (timeout=60s)…")
        fetched = _fetch_with_hard_timeout(_load_us_symbols_from_web, timeout=60)
        records.extend(fetched)
        print(f"[symbols] loaded {len(fetched)} us_stocks symbols from web")
    except Exception as exc:
        print(f"[symbols] fallback to static us_stocks list: {exc}")
        if repo_fallback_records:
            print(f"[symbols] using repo fallback manifest for us_stocks ({len(repo_fallback_records)} symbols)")
            records = repo_fallback_records
        elif cached:
            print(f"[symbols] using cached manifest as fallback ({len(cached)} symbols)")
            records = cached

    if args.include_us_delisted:
        try:
            records.extend(
                _fetch_with_hard_timeout(
                    _load_us_delisted_from_alpha_vantage,
                    args.alpha_vantage_api_key,
                    timeout=60,
                )
            )
        except Exception as exc:
            print(f"[symbols] failed to load us delisted list: {exc}")
    return records


def _resolve_crypto_symbols(args: argparse.Namespace, cached: list[SymbolRecord]) -> list[SymbolRecord]:
    cached_daily = _use_daily_update_cache_if_available("crypto", args, cached)
    if cached_daily is not None:
        return cached_daily

    repo_fallback_records = _load_repo_symbol_fallback("crypto")
    records = _records_from_defaults("crypto")
    try:
        print("[symbols] fetching crypto list from CoinGecko (timeout=60s)…")
        fetched = _fetch_with_hard_timeout(_load_crypto_symbols_from_coingecko, timeout=60)
        records.extend(fetched)
        print(f"[symbols] loaded {len(fetched)} crypto symbols from CoinGecko")
    except Exception as exc:
        print(f"[symbols] fallback to static crypto list: {exc}")
        if repo_fallback_records:
            print(f"[symbols] using repo fallback manifest for crypto ({len(repo_fallback_records)} symbols)")
            records = repo_fallback_records
        elif cached:
            print(f"[symbols] using cached manifest as fallback ({len(cached)} symbols)")
            records = cached
    return records


def _resolve_forex_symbols(args: argparse.Namespace, output_dir: Path, cached: list[SymbolRecord]) -> list[SymbolRecord]:
    cached_daily = _use_daily_update_cache_if_available("forex", args, cached)
    if cached_daily is not None:
        return cached_daily

    repo_fallback_records = _load_repo_symbol_fallback("forex")
    records = _records_from_defaults("forex")
    records.extend(_load_cached_symbols_from_manifest(output_dir / "symbols.csv", "forex"))
    records.extend(_load_cached_symbols_from_whitelist(_whitelist_file_path(output_dir), "forex"))

    used_web_symbols = False
    try:
        print("[symbols] fetching forex symbols from Yahoo (timeout=60s)…")
        fetched = _fetch_with_hard_timeout(_load_forex_symbols_from_yahoo_page, timeout=60)
        records.extend(fetched)
        used_web_symbols = True
        print(f"[symbols] loaded {len(fetched)} forex symbols from Yahoo")
    except Exception as exc:
        print(f"[symbols] fallback to static forex list: {exc}")
    if not used_web_symbols:
        if repo_fallback_records:
            print(f"[symbols] using repo fallback manifest for forex ({len(repo_fallback_records)} symbols)")
            records.extend(repo_fallback_records)
        else:
            records.extend(_load_forex_expanded_fallback())
    return records


def _dedupe_records_by_code(records: list[SymbolRecord]) -> list[SymbolRecord]:
    seen_codes: set[str] = set()
    deduped: list[SymbolRecord] = []
    for record in records:
        if record.code in seen_codes:
            continue
        seen_codes.add(record.code)
        deduped.append(record)
    return deduped


def _resolve_symbol_resolution(asset_class: str, args: argparse.Namespace) -> SymbolResolution:
    if args.symbols_file:
        records = _load_symbols_from_file(asset_class, args.symbols_file)
        deduped = _dedupe_records_by_code(records)
        if args.limit is not None:
            deduped = deduped[: args.limit]
        return SymbolResolution(scheduled_records=deduped, manifest_records=deduped, new_codes=set())
    elif args.symbols:
        records = [_record_from_input(asset_class, symbol) for symbol in args.symbols]
        deduped = _dedupe_records_by_code(records)
        if args.limit is not None:
            deduped = deduped[: args.limit]
        return SymbolResolution(scheduled_records=deduped, manifest_records=deduped, new_codes=set())
    else:
        output_dir = _resolve_asset_output_dir(args, asset_class)
        cached = _resolve_cached_manifest(output_dir, asset_class)
        blacklist_symbols = _load_blacklist(_blacklist_file_path(output_dir))
        is_daily_update = getattr(args, "mode", "") == "daily-update"

        if is_daily_update:
            active_records = _load_local_tracked_records(asset_class, output_dir, cached)
            active_records.extend(_records_from_defaults(asset_class))
            if not active_records and cached:
                # Manifest-only output dirs are partial bootstraps. Process them
                # once instead of leaving the asset permanently inert.
                active_records.extend(cached)

            manifest_records = list(cached)
            manifest_records.extend(active_records)
            known_before = {record.code for record in manifest_records}
            known_symbol_keys: set[str] = set()
            for record in manifest_records:
                known_symbol_keys.update(_candidate_symbol_keys(asset_class, record))
            new_codes: set[str] = set()
            active_discovery_symbols: set[str] = set()

            if getattr(args, "daily_retry_known_missing_symbols", False):
                active_codes = {record.code for record in active_records}
                known_missing = [record for record in cached if record.code not in active_codes]
                if known_missing:
                    print(
                        f"[symbols] daily-update: retrying {len(known_missing)} known missing "
                        f"{asset_class} symbols from cached manifest"
                    )
                    active_records.extend(known_missing)

            repo_candidates = _load_repo_symbol_fallback(asset_class)
            new_from_repo = [record for record in repo_candidates if record.code not in known_before]
            if new_from_repo:
                print(
                    f"[symbols] daily-update: adding {len(new_from_repo)} new symbols "
                    f"from repo fallback manifest for {asset_class}"
                )
                active_records.extend(new_from_repo)
                manifest_records.extend(new_from_repo)
                new_codes.update(record.code for record in new_from_repo)
                known_before.update(record.code for record in new_from_repo)

            if getattr(args, "daily_discover_symbols", True) and asset_class in {"tw_stocks", "us_stocks"}:
                try:
                    discovered = _discover_daily_stock_records(asset_class, args, cached)
                except Exception as exc:
                    print(f"[symbols] daily-update: discovery failed for {asset_class}: {exc}")
                else:
                    discovered_active_symbols: set[str] = set()
                    discovered_delisted_symbols: set[str] = set()
                    for record in discovered:
                        target = discovered_delisted_symbols if _is_delisted_record(record) else discovered_active_symbols
                        target.update(_candidate_symbol_keys(asset_class, record))
                    for record in [*active_records, *manifest_records]:
                        if (
                            _is_delisted_record(record)
                            and not _is_delisted_archive_record(record)
                            and _record_overlaps_symbols(asset_class, record, discovered_active_symbols)
                        ):
                            active_discovery_symbols.update(_candidate_symbol_keys(asset_class, record) & discovered_active_symbols)
                    active_records = _apply_daily_listing_state(
                        asset_class,
                        active_records,
                        discovered_active_symbols,
                        discovered_delisted_symbols,
                    )
                    manifest_records = _apply_daily_listing_state(
                        asset_class,
                        manifest_records,
                        discovered_active_symbols,
                        discovered_delisted_symbols,
                    )
                    known_symbol_keys = set()
                    for record in manifest_records:
                        known_symbol_keys.update(_candidate_symbol_keys(asset_class, record))

                    new_from_discovery = [
                        record
                        for record in discovered
                        if record.code not in known_before
                        and not (
                            _is_delisted_record(record)
                            and (
                                _candidate_symbol_keys(asset_class, record) & known_symbol_keys
                                or _all_candidate_symbols_blacklisted(asset_class, record, blacklist_symbols)
                            )
                        )
                    ]
                    if new_from_discovery:
                        print(
                            f"[symbols] daily-update: adding {len(new_from_discovery)} new symbols "
                            f"from upstream discovery for {asset_class}"
                        )
                        active_records.extend(new_from_discovery)
                        manifest_records.extend(new_from_discovery)
                        new_codes.update(record.code for record in new_from_discovery)
                        for record in new_from_discovery:
                            known_symbol_keys.update(_candidate_symbol_keys(asset_class, record))
                            if not _is_delisted_record(record):
                                active_discovery_symbols.update(_candidate_symbol_keys(asset_class, record) & discovered_active_symbols)

            active_deduped = _dedupe_records_by_code(active_records)
            manifest_deduped = _dedupe_records_by_code(manifest_records)
            if args.limit is not None:
                active_deduped = active_deduped[: args.limit]
                manifest_deduped = manifest_deduped[: args.limit]
            print(
                f"[symbols] daily-update: scheduled {asset_class} "
                f"({len(active_deduped)} scheduled, {len(manifest_deduped)} known; "
                "parquet+whitelist+defaults+new_discovery)"
            )
            return SymbolResolution(
                scheduled_records=active_deduped,
                manifest_records=manifest_deduped,
                new_codes=new_codes,
                active_discovery_symbols=active_discovery_symbols,
            )

        if asset_class == "tw_stocks":
            records = _resolve_tw_symbols(args, cached)
        elif asset_class == "us_stocks":
            records = _resolve_us_symbols(args, cached)
        elif asset_class == "crypto":
            records = _resolve_crypto_symbols(args, cached)
        elif asset_class == "forex":
            records = _resolve_forex_symbols(args, output_dir, cached)
        else:
            records = _records_from_defaults(asset_class)

    deduped = _dedupe_records_by_code(records)

    if args.limit is not None:
        deduped = deduped[: args.limit]
    return SymbolResolution(scheduled_records=deduped, manifest_records=deduped, new_codes=set())


def _resolve_symbols(asset_class: str, args: argparse.Namespace) -> list[SymbolRecord]:
    return _resolve_symbol_resolution(asset_class, args).scheduled_records


def _download_symbol(
    asset_class: str,
    record: SymbolRecord,
    output_dir: Path,
    start_date: str,
    end_date: str,
    retries: int,
    refresh: bool,
    merge_existing: bool = False,
    blacklist_symbols: set[str] | None = None,
    blacklist_path: Path | None = None,
    blacklist_lock: threading.Lock | None = None,
    whitelist_symbols: set[str] | None = None,
    whitelist_path: Path | None = None,
    whitelist_lock: threading.Lock | None = None,
) -> DownloadResult:
    output_path = output_dir / f"{record.code}_features.parquet"
    candidates_to_try = _available_candidate_symbols(asset_class, record, blacklist_symbols)
    if not candidates_to_try:
        return DownloadResult(
            asset_class=asset_class,
            code=record.code,
            yahoo_symbol=record.yahoo_symbol,
            market=record.market,
            status=_skip_status_for_unavailable_record(record),
            rows=0,
            output_path=str(output_path) if output_path.exists() else None,
            message="All candidate Yahoo symbols are in blacklist.",
        )
    if output_path.exists() and not refresh:
        if merge_existing:
            pass
        else:
            try:
                existing_rows = _read_parquet_row_count(output_path)
                existing_info = _load_existing_file_info(output_path)
                if existing_info.error is not None:
                    raise RuntimeError(existing_info.error)
                if asset_class == "crypto" and not _frame_matches_expected_interval(
                    _read_parquet_frame(output_path),
                    YF_CRYPTO_INTRADAY_SECONDS,
                ):
                    print(
                        f"[download] {record.code}: existing parquet does not look like "
                        f"{YF_CRYPTO_INTRADAY_INTERVAL}; rebuilding from crypto intraday source"
                    )
                else:
                    return DownloadResult(
                        asset_class=asset_class,
                        code=record.code,
                        yahoo_symbol=record.yahoo_symbol,
                        market=record.market,
                        status="skipped_existing",
                        rows=existing_rows,
                        output_path=str(output_path),
                        first_date=existing_info.first_date,
                        last_date=existing_info.last_date,
                        checked_through_date=existing_info.checked_through_date,
                    )
            except Exception as exc:
                return DownloadResult(
                    asset_class=asset_class,
                    code=record.code,
                    yahoo_symbol=record.yahoo_symbol,
                    market=record.market,
                    status="failed_existing_read",
                    rows=0,
                    output_path=str(output_path),
                    message=str(exc),
                )

    existing_frame: pd.DataFrame | None = None
    if output_path.exists() and merge_existing:
        try:
            existing_frame = _read_parquet_frame(output_path)
            if asset_class == "crypto" and not _frame_matches_expected_interval(
                existing_frame,
                YF_CRYPTO_INTRADAY_SECONDS,
            ):
                print(
                    f"[download] {record.code}: existing parquet does not look like "
                    f"{YF_CRYPTO_INTRADAY_INTERVAL}; rebuilding from crypto intraday source"
                )
                existing_frame = None
            elif "date" in existing_frame.columns:
                existing_frame["date"] = pd.to_datetime(existing_frame["date"], errors="coerce").dt.tz_localize(None)
        except Exception as exc:
            existing_frame = None
            print(f"[download] merge-existing read failed for {output_path.name}: {exc}")

    yf_interval = YF_CRYPTO_INTRADAY_INTERVAL if asset_class == "crypto" else "1d"
    effective_start_date = start_date
    if asset_class == "crypto":
        max_lookback_start = (_parse_date(end_date) - timedelta(days=YF_CRYPTO_MAX_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        if _parse_date(start_date) < _parse_date(max_lookback_start):
            effective_start_date = max_lookback_start
            print(
                f"[download] {record.code}: clamp start_date to {effective_start_date} "
                f"for Yahoo intraday {yf_interval} limit"
            )

    period_end_exclusive = (_parse_date(end_date) + timedelta(days=1)).strftime("%Y-%m-%d")
    last_error: str | None = None
    for candidate_symbol in candidates_to_try:
        for attempt in range(retries + 1):
            try:
                std_capture = io.StringIO()
                err_capture = io.StringIO()

                def _download_frame() -> pd.DataFrame:
                    return yf.download(
                        tickers=candidate_symbol,
                        start=effective_start_date,
                        end=period_end_exclusive,
                        interval=yf_interval,
                        auto_adjust=False,
                        actions=True,
                        progress=False,
                        threads=False,
                        timeout=20,
                    )

                with contextlib.redirect_stdout(std_capture), contextlib.redirect_stderr(err_capture):
                    # Guard yfinance against indefinite socket/DNS stalls.
                    frame = _fetch_with_hard_timeout(
                        _download_frame,
                        timeout=YF_DOWNLOAD_HARD_TIMEOUT_SECONDS,
                    )
                normalized = _normalize_download_frame(frame, keep_zero_volume=asset_class != "forex")
                captured = f"{std_capture.getvalue()}\n{err_capture.getvalue()}".lower()
                if _captured_indicates_unavailable(captured):
                    _blacklist_symbol(candidate_symbol, blacklist_symbols, blacklist_path, blacklist_lock)
                    last_error = f"{candidate_symbol}: unavailable or delisted"
                    break
                if normalized.empty:
                    last_error = f"{candidate_symbol}: Yahoo returned no rows."
                    if attempt < retries:
                        time.sleep(0.8 * (attempt + 1))
                        continue
                    break

                if existing_frame is not None and not existing_frame.empty:
                    normalized = pd.concat([existing_frame, normalized], ignore_index=True)
                    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce").dt.tz_localize(None)
                    normalized = normalized.dropna(subset=["date", "close"]).sort_values("date").drop_duplicates(subset=["date"], keep="last")
                    if asset_class == "forex" and "Trading_Volume" in normalized.columns:
                        volume = pd.to_numeric(normalized["Trading_Volume"], errors="coerce")
                        if volume.isna().all() or volume.fillna(0).eq(0).all():
                            normalized = normalized.drop(columns=["Trading_Volume"])
                    normalized = normalized.reset_index(drop=True)

                first_date, last_date = _write_feature_parquet_atomic(
                    normalized,
                    output_path,
                    asset_class=asset_class,
                    requested_end_date=end_date,
                )
                _whitelist_symbol(candidate_symbol, whitelist_symbols, whitelist_path, whitelist_lock)
                return DownloadResult(
                    asset_class=asset_class,
                    code=record.code,
                    yahoo_symbol=candidate_symbol,
                    market=record.market,
                    status="updated",
                    rows=int(len(normalized)),
                    output_path=str(output_path),
                    first_date=first_date,
                    last_date=last_date,
                    checked_through_date=end_date,
                )
            except Exception as exc:
                last_error = f"{candidate_symbol}: {exc}"
                if _captured_indicates_unavailable(str(exc).lower()):
                    _blacklist_symbol(candidate_symbol, blacklist_symbols, blacklist_path, blacklist_lock)
                    break
                if attempt < retries:
                    time.sleep(0.8 * (attempt + 1))

    return DownloadResult(
        asset_class=asset_class,
        code=record.code,
        yahoo_symbol=record.yahoo_symbol,
        market=record.market,
        status="failed",
        rows=0,
        output_path=None,
        message=last_error,
    )


def _write_symbol_manifest(output_dir: Path, records: list[SymbolRecord]) -> None:
    manifest_path = output_dir / "symbols.csv"
    frame = pd.DataFrame([asdict(record) for record in _dedupe_records_by_code(records)])
    frame.to_csv(manifest_path, index=False)


def _load_symbols_from_manifest_csv(manifest_path: Path, asset_class: str) -> list[SymbolRecord]:
    """Read a symbols.csv written by _write_symbol_manifest(). No pattern filter; works for all asset classes."""
    if not manifest_path.exists():
        return []
    try:
        frame = pd.read_csv(manifest_path, dtype=str).fillna("")
    except Exception:
        return []
    required = {"code", "name", "market", "yahoo_symbol"}
    if not required.issubset(frame.columns):
        return []
    records: list[SymbolRecord] = []
    for row in frame.itertuples(index=False):
        code = str(getattr(row, "code", "")).strip().upper()
        yahoo_symbol = str(getattr(row, "yahoo_symbol", "")).strip().upper()
        if not code or not yahoo_symbol:
            continue
        name = str(getattr(row, "name", code)).strip() or code
        market = str(getattr(row, "market", asset_class)).strip() or asset_class
        records.append(SymbolRecord(code=code, name=name, market=market, yahoo_symbol=yahoo_symbol))
    return records


def _write_download_artifacts(output_dir: Path, asset_class: str, results: list[DownloadResult]) -> None:
    report_path = output_dir / "download_report.csv"
    with report_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "asset_class",
                "code",
                "yahoo_symbol",
                "market",
                "status",
                "rows",
                "output_path",
                "message",
                "first_date",
                "last_date",
                "checked_through_date",
            ],
        )
        writer.writeheader()
        for result in results:
            writer.writerow(asdict(result))

    counts: dict[str, int] = {}
    total_rows = 0
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
        total_rows += result.rows

    summary = {
        "asset_class": asset_class,
        "symbol_count": len(results),
        "row_count": total_rows,
        "status_counts": counts,
    }
    if asset_class == "crypto":
        summary["interval"] = YF_CRYPTO_INTRADAY_INTERVAL
        summary["max_lookback_days"] = YF_CRYPTO_MAX_LOOKBACK_DAYS
    (output_dir / "download_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")


def _resolve_asset_output_dir(args: argparse.Namespace, asset_class: str) -> Path:
    if args.output_dir:
        if args.asset == "all":
            raise ValueError("--output-dir cannot be combined with --asset all. Use --output-root instead.")
        return Path(args.output_dir)
    return Path(args.output_root) / asset_class


def _load_existing_file_info(output_path: Path) -> ExistingFileInfo:
    if not output_path.exists():
        return ExistingFileInfo(None, None, "missing", set())

    pyarrow_info = _load_existing_file_info_pyarrow(output_path)
    if pyarrow_info is not None:
        return pyarrow_info

    polars_info = _load_existing_file_info_polars(output_path)
    if polars_info is not None:
        return polars_info

    try:
        frame = pd.read_parquet(output_path)
    except Exception as exc:
        return ExistingFileInfo(None, None, f"read_error: {exc}", set())
    columns = set(frame.columns)
    if frame.empty or "date" not in columns:
        return ExistingFileInfo(None, None, "empty", columns)

    dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
    if dates.empty:
        return ExistingFileInfo(None, None, "no_valid_date", columns)
    return ExistingFileInfo(dates.min().date().isoformat(), dates.max().date().isoformat(), None, columns)


def _summarize_repair_coverage(checks: list[RepairCheck], target_end: str) -> tuple[str | None, str | None, int | None, int]:
    first_dates = [_parse_date(check.first_date).date() for check in checks if check.first_date]
    last_dates = [_parse_date(check.last_date).date() for check in checks if check.last_date]
    if not first_dates or not last_dates:
        return None, None, None, 0

    oldest = min(first_dates)
    newest = max(last_dates)
    lag_days = (_parse_date(target_end).date() - newest).days
    return oldest.isoformat(), newest.isoformat(), lag_days, len(last_dates)


def _summarize_post_repair_coverage(
    checks: list[RepairCheck],
    results: list[DownloadResult],
    target_end: str,
) -> tuple[str | None, str | None, int | None, int]:
    """Compute post-repair coverage from in-memory data without re-reading disk."""
    repaired_last_dates = {
        r.code: r.last_date
        for r in results
        if r.status in {"repaired", "schema_repaired", "new_symbol_repaired"} and r.last_date
    }

    first_dates: list[date] = []
    last_dates: list[date] = []
    for check in checks:
        if check.status == "broken" or check.first_date is None:
            continue
        first_dates.append(_parse_date(check.first_date).date())
        repaired_last_date = repaired_last_dates.get(check.record.code)
        if repaired_last_date:
            last_dates.append(_parse_date(repaired_last_date).date())
        elif check.last_date:
            last_dates.append(_parse_date(check.last_date).date())

    if not first_dates or not last_dates:
        return None, None, None, 0

    oldest = min(first_dates)
    newest = max(last_dates)
    lag_days = (_parse_date(target_end).date() - newest).days
    return oldest.isoformat(), newest.isoformat(), lag_days, len(last_dates)


def _resolve_repair_plan(
    asset_class: str,
    args: argparse.Namespace,
    records: list[SymbolRecord],
    output_dir: Path,
    new_codes: set[str] | None = None,
) -> list[RepairCheck]:
    checks: list[RepairCheck] = []
    new_codes = new_codes or set()
    target_end = args.end_date or _today_str()
    requested_target_end_dt = _parse_date(target_end).date()
    target_end_dt = _effective_repair_target_end_date(asset_class, args)
    overlap = max(1, args.repair_overlap_days)
    precheck_loader = PrecheckLoader() if args.precheck_file_timeout_seconds > 0 else None
    blacklist_path = _blacklist_file_path(output_dir)
    blacklist_symbols = _load_blacklist(blacklist_path)
    blacklist_lock = threading.Lock()
    retry_blacklisted = bool(getattr(args, "retry_blacklisted_repair_symbols", False))

    def _unavailable_skip(
        record: SymbolRecord,
        output_path: Path,
        first_date: str | None,
        last_date: str | None,
    ) -> RepairCheck:
        return RepairCheck(
            record=record,
            status=_skip_status_for_unavailable_record(record),
            output_path=output_path,
            first_date=first_date,
            last_date=last_date,
            repair_start_date=None,
            merge_existing=False,
            message="All candidate Yahoo symbols are in blacklist.",
        )

    def _delisted_no_history(
        record: SymbolRecord,
        output_path: Path,
        reason: str,
    ) -> RepairCheck:
        removed = False
        if output_path.exists():
            try:
                output_path.unlink()
                removed = True
            except Exception as exc:
                return RepairCheck(
                    record=record,
                    status="broken",
                    output_path=output_path,
                    first_date=None,
                    last_date=None,
                    repair_start_date=None,
                    merge_existing=False,
                    message=f"failed to remove delisted no-history file: {exc}",
                )
        _blacklist_record_symbols(record.yahoo_symbol, blacklist_symbols, blacklist_path, blacklist_lock)
        return RepairCheck(
            record=record,
            status="delisted_removed" if removed else "delisted_no_history",
            output_path=output_path,
            first_date=None,
            last_date=None,
            repair_start_date=None,
            merge_existing=False,
            message=reason,
        )

    progress = tqdm(records, desc=f"precheck:{asset_class}", unit="symbol")
    try:
        for record in progress:
            progress.set_postfix_str(record.code, refresh=False)
            output_path = output_dir / f"{record.code}_features.parquet"
            try:
                if precheck_loader is not None:
                    info = precheck_loader.load_with_timeout(output_path, args.precheck_file_timeout_seconds)
                else:
                    info = _load_existing_file_info(output_path)
            except PrecheckTimeoutError as exc:
                _blacklist_record_symbols(record.yahoo_symbol, blacklist_symbols, blacklist_path, blacklist_lock)
                checks.append(
                    RepairCheck(
                        record=record,
                        status="broken",
                        output_path=output_path,
                        first_date=None,
                        last_date=None,
                        repair_start_date=args.start_date,
                        merge_existing=False,
                        message=str(exc),
                    )
                )
                continue
            except Exception as exc:
                checks.append(
                    RepairCheck(
                        record=record,
                        status="broken",
                        output_path=output_path,
                        first_date=None,
                        last_date=None,
                        repair_start_date=args.start_date,
                        merge_existing=False,
                        message=str(exc),
                    )
                )
                continue
            first_date = info.first_date
            last_date = info.last_date
            error = info.error
            columns = info.columns
            checked_through_date = info.checked_through_date
            if error == "missing":
                if _is_delisted_record(record):
                    checks.append(_delisted_no_history(record, output_path, "confirmed delisted with no local history"))
                    continue
                if not retry_blacklisted and _all_candidate_symbols_blacklisted(asset_class, record, blacklist_symbols):
                    checks.append(_unavailable_skip(record, output_path, None, None))
                    continue
                checks.append(
                    RepairCheck(
                        record=record,
                        status="new_symbol" if record.code in new_codes else "missing",
                        output_path=output_path,
                        first_date=None,
                        last_date=None,
                        repair_start_date=args.start_date,
                        merge_existing=False,
                    )
                )
                continue
            if error is not None:
                if _is_delisted_record(record):
                    if error in {"empty", "no_valid_date"}:
                        checks.append(_delisted_no_history(record, output_path, f"confirmed delisted with no usable history: {error}"))
                    else:
                        checks.append(_unavailable_skip(record, output_path, None, None))
                    continue
                checks.append(
                    RepairCheck(
                        record=record,
                        status="broken",
                        output_path=output_path,
                        first_date=None,
                        last_date=None,
                        repair_start_date=args.start_date,
                        merge_existing=False,
                        message=error,
                    )
                )
                continue
            if last_date is None:
                if _is_delisted_record(record):
                    checks.append(_delisted_no_history(record, output_path, "confirmed delisted with no valid dates"))
                    continue
                checks.append(
                    RepairCheck(
                        record=record,
                        status="empty",
                        output_path=output_path,
                        first_date=None,
                        last_date=None,
                        repair_start_date=args.start_date,
                        merge_existing=False,
                    )
                )
                continue

            missing_required = sorted(REPAIR_REQUIRED_COLUMNS - columns)
            if missing_required:
                if _is_delisted_record(record):
                    checks.append(_unavailable_skip(record, output_path, first_date, last_date))
                    continue
                if not retry_blacklisted and _all_candidate_symbols_blacklisted(asset_class, record, blacklist_symbols):
                    checks.append(_unavailable_skip(record, output_path, first_date, last_date))
                    continue
                checks.append(
                    RepairCheck(
                        record=record,
                        status="schema_mismatch",
                        output_path=output_path,
                        first_date=first_date,
                        last_date=last_date,
                        repair_start_date=args.start_date,
                        merge_existing=False,
                        message=f"missing_required_columns={','.join(missing_required)}",
                    )
                )
                continue

            local_last_dt = _parse_date(last_date).date()
            if _is_delisted_record(record):
                checks.append(_unavailable_skip(record, output_path, first_date, last_date))
                continue

            if not retry_blacklisted and _all_candidate_symbols_blacklisted(asset_class, record, blacklist_symbols):
                checks.append(_unavailable_skip(record, output_path, first_date, last_date))
                continue

            if args.mode == "daily-update" and args.daily_stale_max_lag_days > 0:
                lag_days = (target_end_dt - local_last_dt).days
                if lag_days > args.daily_stale_max_lag_days:
                    checks.append(
                        RepairCheck(
                            record=record,
                            status="lagging_skip",
                            output_path=output_path,
                            first_date=first_date,
                            last_date=last_date,
                            repair_start_date=None,
                            checked_through_date=checked_through_date,
                            merge_existing=False,
                            message=(
                                f"lag_days={lag_days} exceeds daily_stale_max_lag_days="
                                f"{args.daily_stale_max_lag_days}"
                            ),
                        )
                    )
                    continue

            checked_through_dt = _parse_date(checked_through_date).date() if checked_through_date else None
            if local_last_dt >= target_end_dt:
                checks.append(
                    RepairCheck(
                        record=record,
                        status="current",
                        output_path=output_path,
                        first_date=first_date,
                        last_date=last_date,
                        repair_start_date=None,
                        checked_through_date=checked_through_date,
                    )
                )
                continue
            if (
                args.mode == "daily-update"
                and checked_through_dt is not None
                and checked_through_dt >= requested_target_end_dt
            ):
                checks.append(
                    RepairCheck(
                        record=record,
                        status="current",
                        output_path=output_path,
                        first_date=first_date,
                        last_date=last_date,
                        repair_start_date=None,
                        checked_through_date=checked_through_date,
                        message=f"checked_through={checked_through_date}",
                    )
                )
                continue

            repair_start_dt = max(_parse_date(args.start_date).date(), local_last_dt - timedelta(days=overlap))
            checks.append(
                RepairCheck(
                    record=record,
                    status="stale",
                    output_path=output_path,
                    first_date=first_date,
                    last_date=last_date,
                    repair_start_date=repair_start_dt.isoformat(),
                    merge_existing=True,
                )
            )
    finally:
        progress.close()
        if precheck_loader is not None:
            precheck_loader.close()
    return checks


def _run_parallel_symbol_downloads(
    asset_class: str,
    args: argparse.Namespace,
    output_dir: Path,
    tasks: list[tuple[SymbolRecord, str, bool, bool, object]],
    progress_desc: str,
    blacklist_symbols: set[str],
    blacklist_path: Path,
    blacklist_lock: threading.Lock,
    whitelist_symbols: set[str],
    whitelist_path: Path,
    whitelist_lock: threading.Lock,
    symbol_timeout_seconds: int | None = None,
    timeout_handler: Callable[[SymbolRecord, object, int | None], DownloadResult] | None = None,
    result_transformer: Callable[[DownloadResult, object], DownloadResult] | None = None,
) -> list[DownloadResult]:
    if not tasks:
        return []

    results: list[DownloadResult] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(
                _download_symbol,
                asset_class,
                record,
                output_dir,
                start_date,
                args.end_date,
                args.retries,
                refresh,
                merge_existing,
                blacklist_symbols,
                blacklist_path,
                blacklist_lock,
                whitelist_symbols,
                whitelist_path,
                whitelist_lock,
            ): (record, meta)
            for record, start_date, refresh, merge_existing, meta in tasks
        }

        progress = tqdm(total=len(futures), desc=progress_desc, unit="symbol")
        try:
            for future in as_completed(futures, timeout=None):
                record, meta = futures[future]
                try:
                    result = future.result(timeout=symbol_timeout_seconds)
                except TimeoutError:
                    if timeout_handler is None:
                        raise
                    result = timeout_handler(record, meta, symbol_timeout_seconds)

                if result_transformer is not None:
                    result = result_transformer(result, meta)
                results.append(result)
                progress.update(1)
        finally:
            progress.close()

    return results


def _repair_asset_class(asset_class: str, args: argparse.Namespace) -> dict[str, int]:
    output_dir = _resolve_asset_output_dir(args, asset_class)
    output_dir.mkdir(parents=True, exist_ok=True)
    blacklist_path = _blacklist_file_path(output_dir)
    whitelist_path = _whitelist_file_path(output_dir)
    blacklist_symbols = _load_blacklist(blacklist_path)
    whitelist_symbols = _load_whitelist(whitelist_path)
    blacklist_lock = threading.Lock()
    whitelist_lock = threading.Lock()

    resolution = _resolve_symbol_resolution(asset_class, args)
    records = resolution.scheduled_records
    if not records:
        raise RuntimeError(f"No symbols resolved for asset class: {asset_class}")

    requested_target_end_dt = _parse_date(args.end_date or _today_str()).date()
    effective_target_end_dt = _effective_repair_target_end_date(asset_class, args)
    adjustment_reason = _daily_update_target_adjustment_reason(
        asset_class,
        requested_target_end_dt,
        effective_target_end_dt,
    )
    if adjustment_reason is not None:
        print(
            f"[daily-update] asset={asset_class} effective_target_end={effective_target_end_dt.isoformat()} "
            f"requested_end={requested_target_end_dt.isoformat()} reason={adjustment_reason}"
        )

    _write_symbol_manifest(output_dir, resolution.manifest_records)
    if resolution.active_discovery_symbols:
        active_listing_candidates: set[str] = set()
        for record in records:
            if _is_delisted_record(record):
                continue
            candidates = _candidate_symbol_keys(asset_class, record)
            if candidates & resolution.active_discovery_symbols:
                active_listing_candidates.update(candidates)
        _clear_blacklist_candidates(
            blacklist_symbols=blacklist_symbols,
            blacklist_path=blacklist_path,
            candidates=active_listing_candidates,
            reason="active listing discovery",
        )

    checks = _resolve_repair_plan(asset_class, args, records, output_dir, new_codes=resolution.new_codes)
    removed_delisted_codes = {
        check.record.code
        for check in checks
        if check.status in {"delisted_no_history", "delisted_removed"}
    }
    if removed_delisted_codes:
        _write_symbol_manifest(
            output_dir,
            [record for record in resolution.manifest_records if record.code not in removed_delisted_codes],
        )

    status_counts: dict[str, int] = {}
    pending = [check for check in checks if check.repair_start_date is not None]
    for check in checks:
        status_counts[check.status] = status_counts.get(check.status, 0) + 1

    if getattr(args, "retry_blacklisted_repair_symbols", False):
        retry_candidates: set[str] = set()
        for check in checks:
            if check.repair_start_date is not None:
                for cand in _candidate_symbols_for_record(asset_class, check.record):
                    if cand:
                        retry_candidates.add(cand.upper())
        _clear_blacklist_candidates(
            blacklist_symbols=blacklist_symbols,
            blacklist_path=blacklist_path,
            candidates=retry_candidates,
            reason="pending repair symbols",
        )

    needs_update = sum(
        status_counts.get(status, 0)
        for status in ("missing", "new_symbol", "stale", "empty", "broken", "schema_mismatch")
    )
    print(
        f"[repair] asset={asset_class} up_to_date={status_counts.get('current', 0)} "
        f"needs_update={needs_update} stale={status_counts.get('stale', 0)} "
        f"missing={status_counts.get('missing', 0)} new_symbol={status_counts.get('new_symbol', 0)} "
        f"broken={status_counts.get('broken', 0)} schema_mismatch={status_counts.get('schema_mismatch', 0)} "
        f"delisted_skip={status_counts.get('delisted_skip', 0)} "
        f"delisted_no_history={status_counts.get('delisted_no_history', 0)} "
        f"delisted_removed={status_counts.get('delisted_removed', 0)} "
        f"not_found_skip={status_counts.get('not_found_skip', 0)} "
        f"lagging_skip={status_counts.get('lagging_skip', 0)}"
    )
    target_end = effective_target_end_dt.isoformat()
    oldest_date, newest_date, lag_days, tracked = _summarize_repair_coverage(checks, target_end)
    if oldest_date and newest_date and lag_days is not None:
        print(
            f"[repair] asset={asset_class} local_range={oldest_date}..{newest_date} "
            f"latest_lag_days={lag_days} tracked={tracked}"
        )
    else:
        print(f"[repair] asset={asset_class} local_range=n/a tracked=0")

    def _repair_timeout_result(record: SymbolRecord, meta: object, timeout_seconds: int | None) -> DownloadResult:
        check = meta
        assert isinstance(check, RepairCheck)
        return DownloadResult(
            asset_class=asset_class,
            code=record.code,
            yahoo_symbol=record.yahoo_symbol,
            market=record.market,
            status="failed",
            rows=0,
            output_path=None,
            message=f"repair timed out after {timeout_seconds}s",
        )

    def _repair_result_transform(result: DownloadResult, meta: object) -> DownloadResult:
        check = meta
        assert isinstance(check, RepairCheck)
        if result.status == "updated":
            if check.status == "new_symbol":
                result.status = "new_symbol_repaired"
            elif check.status == "schema_mismatch":
                result.status = "schema_repaired"
            else:
                result.status = "repaired"
        elif result.status == "failed" and result.message and _captured_indicates_unavailable(result.message.lower()):
            result.status = "not_found"
        elif result.status == "empty":
            result.status = "still_stale"
            if check.last_date:
                result.message = f"No newer rows returned; local last date remains {check.last_date}"
        return result

    repair_tasks = [
        (check.record, check.repair_start_date, True, check.merge_existing, check)
        for check in pending
        if check.repair_start_date is not None
    ]
    results = _run_parallel_symbol_downloads(
        asset_class=asset_class,
        args=args,
        output_dir=output_dir,
        tasks=repair_tasks,
        progress_desc=f"repair:{asset_class}",
        blacklist_symbols=blacklist_symbols,
        blacklist_path=blacklist_path,
        blacklist_lock=blacklist_lock,
        whitelist_symbols=whitelist_symbols,
        whitelist_path=whitelist_path,
        whitelist_lock=whitelist_lock,
        symbol_timeout_seconds=args.repair_symbol_timeout_seconds or None,
        timeout_handler=_repair_timeout_result,
        result_transformer=_repair_result_transform,
    )

    if results:
        _write_download_artifacts(output_dir, asset_class, results)
    repair_report_path = output_dir / "repair_report.csv"
    with repair_report_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "code",
                "yahoo_symbol",
                "precheck_status",
                "first_date",
                "last_date",
                "checked_through_date",
                "repair_start_date",
                "output_path",
                "message",
            ],
        )
        writer.writeheader()
        for check in checks:
            writer.writerow(
                {
                    "code": check.record.code,
                    "yahoo_symbol": check.record.yahoo_symbol,
                    "precheck_status": check.status,
                    "first_date": check.first_date,
                    "last_date": check.last_date,
                    "checked_through_date": check.checked_through_date,
                    "repair_start_date": check.repair_start_date,
                    "output_path": str(check.output_path),
                    "message": check.message,
                }
            )

    # Post-repair coverage: compute entirely from in-memory data; no second disk scan.
    post_oldest, post_newest, post_lag_days, post_tracked = _summarize_post_repair_coverage(
        checks, results, target_end
    )
    if post_oldest and post_newest and post_lag_days is not None:
        print(
            f"[repair] asset={asset_class} post_repair_range={post_oldest}..{post_newest} "
            f"latest_lag_days={post_lag_days} tracked={post_tracked}"
        )

    final_counts = dict(status_counts)
    for result in results:
        final_counts[result.status] = final_counts.get(result.status, 0) + 1
    return final_counts


def _download_asset_class(asset_class: str, args: argparse.Namespace) -> dict[str, int]:
    output_dir = _resolve_asset_output_dir(args, asset_class)
    output_dir.mkdir(parents=True, exist_ok=True)
    blacklist_path = _blacklist_file_path(output_dir)
    whitelist_path = _whitelist_file_path(output_dir)
    blacklist_symbols = _load_blacklist(blacklist_path)
    whitelist_symbols = _load_whitelist(whitelist_path)
    blacklist_lock = threading.Lock()
    whitelist_lock = threading.Lock()

    resolution = _resolve_symbol_resolution(asset_class, args)
    records = resolution.scheduled_records
    if not records:
        raise RuntimeError(f"No symbols resolved for asset class: {asset_class}")

    _write_symbol_manifest(output_dir, resolution.manifest_records)
    download_tasks = [(record, args.start_date, args.refresh, False, None) for record in records]
    results = _run_parallel_symbol_downloads(
        asset_class=asset_class,
        args=args,
        output_dir=output_dir,
        tasks=download_tasks,
        progress_desc=f"download:{asset_class}",
        blacklist_symbols=blacklist_symbols,
        blacklist_path=blacklist_path,
        blacklist_lock=blacklist_lock,
        whitelist_symbols=whitelist_symbols,
        whitelist_path=whitelist_path,
        whitelist_lock=whitelist_lock,
    )

    results.sort(key=lambda item: item.code)
    _write_download_artifacts(output_dir, asset_class, results)

    counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
    return counts


def _run_one_asset(asset_class: str, args: argparse.Namespace) -> tuple[str, dict[str, int]]:
    print(f"[{args.mode}] asset={asset_class} start={args.start_date} end={args.end_date}")
    if args.mode == "daily-update":
        output_dir = _resolve_asset_output_dir(args, asset_class)
        if _asset_output_is_bootstrap_empty(output_dir):
            print(f"[daily-update] asset={asset_class} bootstrap empty output; switching to download mode")
            download_args = argparse.Namespace(**vars(args))
            download_args.mode = "download"
            counts = _download_asset_class(asset_class, download_args)
        else:
            counts = _repair_asset_class(asset_class, args)
    elif args.mode == "repair":
        counts = _repair_asset_class(asset_class, args)
    else:
        counts = _download_asset_class(asset_class, args)
    print(f"[{args.mode}] completed asset={asset_class} status_counts={counts}")
    return asset_class, counts


def main() -> None:
    # Cap all socket operations (including DNS via getaddrinfo on supported platforms)
    # so any single blocking call can't hang the process indefinitely.
    socket.setdefaulttimeout(30)
    args = parse_args()
    asset_classes = list(ASSET_CLASSES) if args.asset == "all" else [args.asset]
    summaries: dict[str, dict[str, int]] = {}

    asset_workers = max(1, int(args.asset_workers))
    asset_progress = tqdm(total=len(asset_classes), desc=f"{args.mode}:assets", unit="asset")
    try:
        if len(asset_classes) == 1 or asset_workers == 1:
            for asset_class in asset_classes:
                key, counts = _run_one_asset(asset_class, args)
                summaries[key] = counts
                asset_progress.update(1)
        else:
            with ThreadPoolExecutor(max_workers=min(asset_workers, len(asset_classes))) as executor:
                futures = {executor.submit(_run_one_asset, asset_class, args): asset_class for asset_class in asset_classes}
                for future in as_completed(futures):
                    key, counts = future.result()
                    summaries[key] = counts
                    asset_progress.update(1)
    finally:
        asset_progress.close()

    if args.mode == "download":
        summary_name = "download_summary.json"
    elif args.mode == "repair":
        summary_name = "repair_summary.json"
    else:
        summary_name = "daily_update_summary.json"
    summary_path = Path(args.output_root) / summary_name
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summaries, indent=2, ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    main()
