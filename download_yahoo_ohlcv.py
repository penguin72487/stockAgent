from __future__ import annotations

import argparse
import contextlib
import csv
import io
import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import pandas as pd
import yfinance as yf
from tqdm import tqdm


ASSET_CLASSES = ("tw_stocks", "us_stocks", "crypto", "forex")
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
CRYPTO_SYMBOL_PATTERN = re.compile(r"^[a-z]{2,8}$")
OUTPUT_COLUMNS = ["date", "open", "max", "min", "close", "Trading_Volume"]
BASE_OUTPUT_COLUMNS = ["date", "open", "max", "min", "close", "adjclose", "Trading_Volume"]
REPAIR_REQUIRED_COLUMNS = {"date", "open", "max", "min", "close", "adjclose"}
BLACKLIST_TRIGGER_TEXT = "possibly delisted; no timezone found"
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


@dataclass(slots=True)
class RepairCheck:
    record: SymbolRecord
    status: str
    output_path: Path
    last_date: str | None
    repair_start_date: str | None
    merge_existing: bool = True
    message: str | None = None


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

    with blacklist_lock:
        if normalized in blacklist_symbols:
            return
        blacklist_symbols.add(normalized)
        _append_blacklist_symbol(blacklist_path, normalized)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download Yahoo Finance OHLCV data into asset-specific parquet directories.")
    parser.add_argument(
        "--mode",
        choices=["download", "repair"],
        default="download",
        help="download: grab the configured universe; repair: check existing parquet files and refill missing/stale data.",
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
    return parser.parse_args()


def _parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d")


def _today_str() -> str:
    return date.today().isoformat()


def _normalize_download_frame(frame: pd.DataFrame) -> pd.DataFrame:
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
    normalized = normalized.dropna(axis=1, how="all")
    if "Trading_Volume" in normalized.columns:
        volume = pd.to_numeric(normalized["Trading_Volume"], errors="coerce")
        if volume.isna().all() or volume.fillna(0).eq(0).all():
            normalized = normalized.drop(columns=["Trading_Volume"])

    return normalized.reset_index(drop=True)


def _normalize_us_yahoo_symbol(symbol: str) -> str:
    return symbol.strip().upper().replace(".", "-")


def _http_get_text(url: str, timeout: int = 30) -> str:
    with urlopen(url, timeout=timeout) as response:
        raw = response.read()
    return raw.decode("utf-8", errors="ignore")


def _http_get_json(url: str, timeout: int = 30) -> object:
    text = _http_get_text(url, timeout=timeout)
    return json.loads(text)


def _extract_tw_codes_from_tables(url: str) -> set[str]:
    codes: set[str] = set()
    try:
        tables = pd.read_html(url)
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


def _load_tw_symbols_from_exchange() -> list[SymbolRecord]:
    records: list[SymbolRecord] = []
    seen_codes: set[str] = set()
    for market, (url, suffix) in TWSE_SOURCES.items():
        tables = pd.read_html(url)
        for table in tables:
            if "有價證券代號及名稱" not in table.columns:
                continue
            values = table["有價證券代號及名稱"].astype(str)
            for raw_value in values:
                match = TWSE_CODE_NAME_PATTERN.match(raw_value.strip())
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


def _load_us_symbols_from_web() -> list[SymbolRecord]:
    records: list[SymbolRecord] = []
    seen: set[str] = set()

    nasdaq_url, other_url = US_SYMBOL_SOURCES
    nasdaq = pd.read_csv(nasdaq_url, sep="|", dtype=str, engine="python")
    other = pd.read_csv(other_url, sep="|", dtype=str, engine="python")

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


def _load_us_delisted_from_alpha_vantage(api_key: str) -> list[SymbolRecord]:
    if not api_key:
        return []

    query = urlencode({"function": "LISTING_STATUS", "state": "delisted", "apikey": api_key})
    url = f"{ALPHA_VANTAGE_LISTING_STATUS_URL}?{query}"
    frame = pd.read_csv(url, dtype=str)
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


def _load_forex_symbols_from_yahoo_page() -> list[SymbolRecord]:
    text = _http_get_text(YAHOO_CURRENCIES_URL)
    matches = sorted(set(FOREX_YAHOO_SYMBOL_PATTERN.findall(text)))
    records: list[SymbolRecord] = []
    for yahoo_symbol in matches:
        code = yahoo_symbol.replace("=X", "")
        records.append(SymbolRecord(code=code, name=code, market="forex", yahoo_symbol=yahoo_symbol))
    return records


def _load_symbols_from_file(asset_class: str, file_path: str) -> list[SymbolRecord]:
    records: list[SymbolRecord] = []
    with Path(file_path).open("r", encoding="utf-8") as handle:
        for line in handle:
            value = line.strip()
            if not value or value.startswith("#"):
                continue
            records.append(_record_from_input(asset_class, value))
    return records


def _resolve_symbols(asset_class: str, args: argparse.Namespace) -> list[SymbolRecord]:
    if args.symbols_file:
        records = _load_symbols_from_file(asset_class, args.symbols_file)
    elif args.symbols:
        records = [_record_from_input(asset_class, symbol) for symbol in args.symbols]
    elif asset_class == "tw_stocks":
        records = _load_tw_symbols_from_local_manifest()
        if not records:
            records = _load_tw_symbols_from_exchange()
        if args.include_tw_delisted:
            try:
                records.extend(_load_tw_delisted_symbols())
            except Exception as exc:
                print(f"[symbols] failed to load tw delisted list: {exc}")
    elif asset_class == "us_stocks":
        records = [
            SymbolRecord(code=code, name=name, market=asset_class, yahoo_symbol=yahoo_symbol)
            for code, name, yahoo_symbol in DEFAULT_SYMBOLS[asset_class]
        ]
        try:
            records.extend(_load_us_symbols_from_web())
        except Exception as exc:
            print(f"[symbols] fallback to static us_stocks list: {exc}")
        if args.include_us_delisted:
            try:
                records.extend(_load_us_delisted_from_alpha_vantage(args.alpha_vantage_api_key))
            except Exception as exc:
                print(f"[symbols] failed to load us delisted list: {exc}")
    elif asset_class == "crypto":
        records = [
            SymbolRecord(code=code, name=name, market=asset_class, yahoo_symbol=yahoo_symbol)
            for code, name, yahoo_symbol in DEFAULT_SYMBOLS[asset_class]
        ]
        try:
            records.extend(_load_crypto_symbols_from_coingecko())
        except Exception as exc:
            print(f"[symbols] fallback to static crypto list: {exc}")
    elif asset_class == "forex":
        records = [
            SymbolRecord(code=code, name=name, market=asset_class, yahoo_symbol=yahoo_symbol)
            for code, name, yahoo_symbol in DEFAULT_SYMBOLS[asset_class]
        ]
        try:
            records.extend(_load_forex_symbols_from_yahoo_page())
        except Exception as exc:
            print(f"[symbols] fallback to static forex list: {exc}")
    else:
        records = [
            SymbolRecord(code=code, name=name, market=asset_class, yahoo_symbol=yahoo_symbol)
            for code, name, yahoo_symbol in DEFAULT_SYMBOLS[asset_class]
        ]

    seen_codes: set[str] = set()
    deduped: list[SymbolRecord] = []
    for record in records:
        if record.code in seen_codes:
            continue
        seen_codes.add(record.code)
        deduped.append(record)

    if args.limit is not None:
        deduped = deduped[: args.limit]
    return deduped


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
    candidate_symbols = [symbol for symbol in YAHOO_SYMBOL_SPLIT_PATTERN.split(record.yahoo_symbol.strip()) if symbol]
    if not candidate_symbols:
        candidate_symbols = [record.yahoo_symbol.strip()]
    candidates_to_try = [symbol for symbol in candidate_symbols if (blacklist_symbols is None or symbol.upper() not in blacklist_symbols)]
    if not candidates_to_try:
        return DownloadResult(
            asset_class=asset_class,
            code=record.code,
            yahoo_symbol=record.yahoo_symbol,
            market=record.market,
            status="blacklisted_skip",
            rows=0,
            output_path=str(output_path) if output_path.exists() else None,
            message="All candidate Yahoo symbols are in blacklist.",
        )
    if output_path.exists() and not refresh:
        if merge_existing:
            pass
        else:
            try:
                existing = pd.read_parquet(output_path)
                return DownloadResult(
                    asset_class=asset_class,
                    code=record.code,
                    yahoo_symbol=record.yahoo_symbol,
                    market=record.market,
                    status="skipped_existing",
                    rows=int(len(existing)),
                    output_path=str(output_path),
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
            existing_frame = pd.read_parquet(output_path)
            if "date" in existing_frame.columns:
                existing_frame["date"] = pd.to_datetime(existing_frame["date"], errors="coerce").dt.tz_localize(None)
        except Exception as exc:
            existing_frame = None
            print(f"[download] merge-existing read failed for {output_path.name}: {exc}")

    period_end_exclusive = (_parse_date(end_date) + timedelta(days=1)).strftime("%Y-%m-%d")
    last_error: str | None = None
    for candidate_symbol in candidates_to_try:
        for attempt in range(retries + 1):
            try:
                std_capture = io.StringIO()
                err_capture = io.StringIO()
                with contextlib.redirect_stdout(std_capture), contextlib.redirect_stderr(err_capture):
                    frame = yf.download(
                        tickers=candidate_symbol,
                        start=start_date,
                        end=period_end_exclusive,
                        interval="1d",
                        auto_adjust=False,
                        actions=True,
                        progress=False,
                        threads=False,
                        timeout=20,
                    )
                normalized = _normalize_download_frame(frame)
                captured = f"{std_capture.getvalue()}\n{err_capture.getvalue()}".lower()
                if BLACKLIST_TRIGGER_TEXT in captured:
                    _blacklist_symbol(candidate_symbol, blacklist_symbols, blacklist_path, blacklist_lock)
                    last_error = f"{candidate_symbol}: {BLACKLIST_TRIGGER_TEXT}"
                    break
                if normalized.empty:
                    last_error = f"{candidate_symbol}: Yahoo returned no rows."
                    break

                if existing_frame is not None and not existing_frame.empty:
                    normalized = pd.concat([existing_frame, normalized], ignore_index=True)
                    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce").dt.tz_localize(None)
                    normalized = normalized.dropna(subset=["date", "close"]).sort_values("date").drop_duplicates(subset=["date"], keep="last")
                    if "Trading_Volume" in normalized.columns:
                        volume = pd.to_numeric(normalized["Trading_Volume"], errors="coerce")
                        if volume.isna().all() or volume.fillna(0).eq(0).all():
                            normalized = normalized.drop(columns=["Trading_Volume"])
                    normalized = normalized.reset_index(drop=True)

                normalized.to_parquet(output_path, index=False)
                _whitelist_symbol(candidate_symbol, whitelist_symbols, whitelist_path, whitelist_lock)
                return DownloadResult(
                    asset_class=asset_class,
                    code=record.code,
                    yahoo_symbol=candidate_symbol,
                    market=record.market,
                    status="updated",
                    rows=int(len(normalized)),
                    output_path=str(output_path),
                )
            except Exception as exc:
                last_error = f"{candidate_symbol}: {exc}"
                if BLACKLIST_TRIGGER_TEXT in str(exc).lower():
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
    frame = pd.DataFrame([asdict(record) for record in records])
    frame.to_csv(manifest_path, index=False)


def _write_download_artifacts(output_dir: Path, asset_class: str, results: list[DownloadResult]) -> None:
    report_path = output_dir / "download_report.csv"
    with report_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["asset_class", "code", "yahoo_symbol", "market", "status", "rows", "output_path", "message"],
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
    (output_dir / "download_summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")


def _resolve_asset_output_dir(args: argparse.Namespace, asset_class: str) -> Path:
    if args.output_dir:
        if args.asset == "all":
            raise ValueError("--output-dir cannot be combined with --asset all. Use --output-root instead.")
        return Path(args.output_dir)
    return Path(args.output_root) / asset_class


def _load_existing_file_info(output_path: Path) -> tuple[str | None, str | None, set[str]]:
    if not output_path.exists():
        return None, "missing", set()
    try:
        frame = pd.read_parquet(output_path)
    except Exception as exc:
        return None, f"read_error: {exc}", set()
    if frame.empty or "date" not in frame.columns:
        return None, "empty", set(frame.columns)
    dates = pd.to_datetime(frame["date"], errors="coerce").dropna()
    if dates.empty:
        return None, "no_valid_date", set(frame.columns)
    return dates.max().date().isoformat(), None, set(frame.columns)


def _resolve_repair_plan(asset_class: str, args: argparse.Namespace, records: list[SymbolRecord], output_dir: Path) -> list[RepairCheck]:
    checks: list[RepairCheck] = []
    target_end = args.end_date or _today_str()
    target_end_dt = _parse_date(target_end).date()
    overlap = max(1, args.repair_overlap_days)

    for record in records:
        output_path = output_dir / f"{record.code}_features.parquet"
        last_date, error, columns = _load_existing_file_info(output_path)
        if error == "missing":
            checks.append(
                RepairCheck(
                    record=record,
                    status="missing",
                    output_path=output_path,
                    last_date=None,
                    repair_start_date=args.start_date,
                    merge_existing=False,
                )
            )
            continue
        if error is not None:
            checks.append(
                RepairCheck(
                    record=record,
                    status="broken",
                    output_path=output_path,
                    last_date=None,
                    repair_start_date=args.start_date,
                    merge_existing=False,
                    message=error,
                )
            )
            continue
        if last_date is None:
            checks.append(
                RepairCheck(
                    record=record,
                    status="empty",
                    output_path=output_path,
                    last_date=None,
                    repair_start_date=args.start_date,
                    merge_existing=False,
                )
            )
            continue

        missing_required = sorted(REPAIR_REQUIRED_COLUMNS - columns)
        if missing_required:
            checks.append(
                RepairCheck(
                    record=record,
                    status="schema_mismatch",
                    output_path=output_path,
                    last_date=last_date,
                    repair_start_date=args.start_date,
                    merge_existing=False,
                    message=f"missing_required_columns={','.join(missing_required)}",
                )
            )
            continue

        local_last_dt = _parse_date(last_date).date()
        if local_last_dt >= target_end_dt:
            checks.append(
                RepairCheck(
                    record=record,
                    status="current",
                    output_path=output_path,
                    last_date=last_date,
                    repair_start_date=None,
                )
            )
            continue

        repair_start_dt = max(_parse_date(args.start_date).date(), local_last_dt - timedelta(days=overlap))
        checks.append(
            RepairCheck(
                record=record,
                status="stale",
                output_path=output_path,
                last_date=last_date,
                repair_start_date=repair_start_dt.isoformat(),
                merge_existing=True,
            )
        )
    return checks


def _repair_asset_class(asset_class: str, args: argparse.Namespace) -> dict[str, int]:
    output_dir = _resolve_asset_output_dir(args, asset_class)
    output_dir.mkdir(parents=True, exist_ok=True)
    blacklist_path = _blacklist_file_path(output_dir)
    whitelist_path = _whitelist_file_path(output_dir)
    blacklist_symbols = _load_blacklist(blacklist_path)
    whitelist_symbols = _load_whitelist(whitelist_path)
    blacklist_lock = threading.Lock()
    whitelist_lock = threading.Lock()

    records = _resolve_symbols(asset_class, args)
    if not records:
        raise RuntimeError(f"No symbols resolved for asset class: {asset_class}")

    _write_symbol_manifest(output_dir, records)
    checks = _resolve_repair_plan(asset_class, args, records, output_dir)
    status_counts: dict[str, int] = {}
    pending = [check for check in checks if check.repair_start_date is not None]
    for check in checks:
        status_counts[check.status] = status_counts.get(check.status, 0) + 1

    print(
        f"[repair] asset={asset_class} current={status_counts.get('current', 0)} "
        f"missing={status_counts.get('missing', 0)} stale={status_counts.get('stale', 0)} "
        f"broken={status_counts.get('broken', 0)} schema_mismatch={status_counts.get('schema_mismatch', 0)}"
    )

    results: list[DownloadResult] = []
    if pending:
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
            futures = {
                executor.submit(
                    _download_symbol,
                    asset_class,
                    check.record,
                    output_dir,
                    check.repair_start_date,
                    args.end_date,
                    args.retries,
                    True,
                    check.merge_existing,
                    blacklist_symbols,
                    blacklist_path,
                    blacklist_lock,
                    whitelist_symbols,
                    whitelist_path,
                    whitelist_lock,
                ): check
                for check in pending
            }
            progress = tqdm(total=len(futures), desc=f"repair:{asset_class}", unit="symbol")
            try:
                for future in as_completed(futures):
                    result = future.result()
                    check = futures[future]
                    if result.status == "updated":
                        if check.status == "schema_mismatch":
                            result.status = "schema_repaired"
                        else:
                            result.status = "repaired"
                    elif result.status == "empty":
                        result.status = "still_stale"
                        if check.last_date:
                            result.message = f"No newer rows returned; local last date remains {check.last_date}"
                    results.append(result)
                    progress.update(1)
            finally:
                progress.close()

    if results:
        _write_download_artifacts(output_dir, asset_class, results)
    repair_report_path = output_dir / "repair_report.csv"
    with repair_report_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["code", "yahoo_symbol", "precheck_status", "last_date", "repair_start_date", "output_path", "message"],
        )
        writer.writeheader()
        for check in checks:
            writer.writerow(
                {
                    "code": check.record.code,
                    "yahoo_symbol": check.record.yahoo_symbol,
                    "precheck_status": check.status,
                    "last_date": check.last_date,
                    "repair_start_date": check.repair_start_date,
                    "output_path": str(check.output_path),
                    "message": check.message,
                }
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

    records = _resolve_symbols(asset_class, args)
    if not records:
        raise RuntimeError(f"No symbols resolved for asset class: {asset_class}")

    _write_symbol_manifest(output_dir, records)
    results: list[DownloadResult] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(
                _download_symbol,
                asset_class,
                record,
                output_dir,
                args.start_date,
                args.end_date,
                args.retries,
                args.refresh,
                False,
                blacklist_symbols,
                blacklist_path,
                blacklist_lock,
                whitelist_symbols,
                whitelist_path,
                whitelist_lock,
            ): record
            for record in records
        }
        progress = tqdm(total=len(futures), desc=f"download:{asset_class}", unit="symbol")
        try:
            for future in as_completed(futures):
                results.append(future.result())
                progress.update(1)
        finally:
            progress.close()

    results.sort(key=lambda item: item.code)
    _write_download_artifacts(output_dir, asset_class, results)

    counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
    return counts


def _run_one_asset(asset_class: str, args: argparse.Namespace) -> tuple[str, dict[str, int]]:
    print(f"[{args.mode}] asset={asset_class} start={args.start_date} end={args.end_date}")
    if args.mode == "repair":
        counts = _repair_asset_class(asset_class, args)
    else:
        counts = _download_asset_class(asset_class, args)
    print(f"[{args.mode}] completed asset={asset_class} status_counts={counts}")
    return asset_class, counts


def main() -> None:
    args = parse_args()
    asset_classes = list(ASSET_CLASSES) if args.asset == "all" else [args.asset]
    summaries: dict[str, dict[str, int]] = {}

    asset_workers = max(1, int(args.asset_workers))
    if len(asset_classes) == 1 or asset_workers == 1:
        for asset_class in asset_classes:
            key, counts = _run_one_asset(asset_class, args)
            summaries[key] = counts
    else:
        with ThreadPoolExecutor(max_workers=min(asset_workers, len(asset_classes))) as executor:
            futures = {executor.submit(_run_one_asset, asset_class, args): asset_class for asset_class in asset_classes}
            for future in as_completed(futures):
                key, counts = future.result()
                summaries[key] = counts

    summary_name = "repair_summary.json" if args.mode == "repair" else "download_summary.json"
    summary_path = Path(args.output_root) / summary_name
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summaries, indent=2, ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    main()