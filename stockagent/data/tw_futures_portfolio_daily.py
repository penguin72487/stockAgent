"""TAIFEX listed equity/ETF/index futures daily portfolio data contract.

The official archive is stored by physical contract.  Delivery month/week
metadata remains explicit, while physical-contract intervals are colored into
a fixed 1,936-column tensor universe.  A contract stays in one column for its
entire observed life; after expiry, the column remains empty for 31 sessions
before reuse so a 32-session model window never crosses physical identities.
An arbitrary far-month position can therefore remain open until its own expiry.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import hashlib
import heapq
import json
from pathlib import Path
import re
import unicodedata
from typing import Any, Final

import numpy as np

from stockagent.data.panel import PanelData

try:
    import polars as pl
except Exception:  # pragma: no cover - validated at the public entry points
    pl = None

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception:  # pragma: no cover - validated at the public entry points
    pa = None
    pq = None


TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION: Final[int] = 4
TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION: Final[int] = 3
TAIFEX_FUTURES_PORTFOLIO_BACKTEST_CONTRACT_VERSION: Final[int] = 3

# The receipt-backed 2005-01-03..2026-08-11 archive reaches 1,280 physical
# contracts active on the same market session.  Reusing exactly 1,280 tensor
# columns would nevertheless contaminate a 32-session model window with the
# expired occupant of a newly reused column.  Interval coloring after extending
# every physical contract by LOOKBACK-1 quarantine sessions has the exact
# historical clique number 1,936.  Fixing that capacity makes every physical
# contract lifetime-stable, leaves 31 blank rows before a slot can be reused,
# and deliberately fails closed if a future research snapshot exceeds it.
TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT: Final[int] = 1_936
TAIFEX_FUTURES_PORTFOLIO_SLOT_REUSE_COOLDOWN_SESSIONS: Final[int] = 31
TAIFEX_FUTURES_PORTFOLIO_MAX_SAFE_LOOKBACK: Final[int] = (
    TAIFEX_FUTURES_PORTFOLIO_SLOT_REUSE_COOLDOWN_SESSIONS + 1
)

DEFAULT_SOURCE_PATH: Final[str] = (
    "data_tw_index_futures/all_futures_daily_sessions.parquet"
)
DEFAULT_PRODUCT_MASTER_PATH: Final[str] = (
    "data_tw_futures/shioaji_contracts/futures_products.csv"
)
DEFAULT_OFFICIAL_PRODUCT_CODE_PATH: Final[str] = (
    "data_tw_futures/taifex_contract_codes.csv"
)
DEFAULT_STOCK_MASTER_PATH: Final[str] = "data_tw_public/stocks/symbols.csv"
DEFAULT_PUBLIC_FEATURE_PATH: Final[str] = (
    "data_tw_public/features/tw_public_stock_daily.parquet"
)
DEFAULT_OUTPUT_ROOT: Final[str] = "data_tw_futures/taifex_portfolio_daily_v4"

# These products are listed by TAIFEX but are outside the requested
# equity/ETF/index scope.  Keep the exclusion explicit and audited.
_EXCLUDED_NON_EQUITY_ROOTS: Final[frozenset[str]] = frozenset(
    {
        "BRF",
        "CPF",
        "GBF",
        "GDF",
        "TGF",
        "RHF",
        "RTF",
        "XAF",
        "XBF",
        "XEF",
        "XJF",
    }
)

_FOREIGN_INDEX_ROOTS: Final[frozenset[str]] = frozenset(
    {"TJF", "SPF", "UNF", "UDF", "SXF", "F1F", "I5F"}
)

_DOMESTIC_INDEX_ROOTS: Final[frozenset[str]] = frozenset(
    {
        "TXF",
        "MXF",
        "MX2",
        "TMF",
        "EXF",
        "FXF",
        "XIF",
        "ZEF",
        "ZFF",
        "GTF",
        "BTF",
        "E4F",
        "G2F",
        "M1F",
        "SHF",
        "SOF",
        "MSF",
        "T5F",
        "TX",
        "MTX",
        "TE",
        "TF",
    }
)

# Point-value multipliers from the TAIFEX product specifications.  MSF is
# deliberately absent: the retired MSCI Taiwan future was USD-denominated and
# needs a point-in-time FX series before a TWD fixed commission can be divided
# by contract notional.  Adjusted single-stock/ETF roots (suffix ``1``) are
# also resolved as unsupported below because their contract units vary by
# corporate action and must come from the historical adjustment notices.
TAIFEX_INDEX_FUTURES_MULTIPLIERS_TWD: Final[dict[str, float]] = {
    "TX": 200.0,
    "MTX": 50.0,
    "TMF": 10.0,
    "TE": 4_000.0,
    "ZEF": 500.0,
    "TF": 1_000.0,
    "ZFF": 250.0,
    "T5F": 100.0,
    "XIF": 10.0,
    "GTF": 4_000.0,
    "G2F": 50.0,
    "E4F": 100.0,
    "BTF": 50.0,
    "SHF": 1_000.0,
    "SOF": 50.0,
    "M1F": 10.0,
    "TJF": 200.0,
    "I5F": 50.0,
    "UDF": 20.0,
    "SPF": 200.0,
    "UNF": 50.0,
    "SXF": 80.0,
    "F1F": 50.0,
}

_SINOPAC_LARGE_FEE_INDEX_ROOTS: Final[frozenset[str]] = frozenset(
    {"TX", "TE", "TF", "T5F", "GTF", "XIF"}
)
_SINOPAC_MICRO_FEE_INDEX_ROOTS: Final[frozenset[str]] = frozenset(
    {"TMF", "M1F"}
)

# Shioaji roots and the official TAIFEX daily-report product identifiers are
# not identical for the original four index futures.  MX2 is the current
# weekly-small-TAIEX Shioaji root and belongs to the same official MTX family.
_OFFICIAL_PRODUCT_OVERRIDES: Final[dict[str, str]] = {
    "TXF": "TX",
    "MXF": "MTX",
    "MX2": "MTX",
    "EXF": "TE",
    "FXF": "TF",
}

# The historical issuer name "神達" is ambiguous in the cash master.  The
# currently listed PSF contract is MiTAC Holdings (3706), not the retired 2315.
_UNDERLYING_OVERRIDES: Final[dict[str, str]] = {
    "CM2": "2887",  # 台新金 -> 台新新光金
    "DT1": "2384",  # 勝華（歷史下市標的）
    "DTF": "2384",  # 勝華（歷史下市標的）
    "ES1": "1704",  # 榮化（歷史下市標的）
    "ESF": "1704",  # 榮化（歷史下市標的）
    "FJF": "2104",  # 中橡 -> 國際中橡
    "FP1": "2206",
    "FPF": "2206",  # 三陽 -> 三陽工業
    "FUF": "2315",  # 2011-2013 舊神達；price-ratio audited
    "FY1": "2340",  # 光磊 -> 台亞
    "JN1": "3673",  # F-TPK -> TPK-KY
    "JYF": "6120",  # 輔祥 -> 達運（代碼延續）
    "KZF": "2608",  # 大榮 -> 嘉里大榮
    "LA1": "2905",
    "LAF": "2905",  # 三商行 -> 三商
    "LGF": "3705",  # 永信；price-ratio audited
    "PS1": "3706",
    "PSF": "3706",  # 2020+ 新神達
}

FUTURES_MODEL_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    "taifex_product_id",
    "taifex_settlement_logret_1d",
    "taifex_volume_log1p",
    "taifex_volume_logret_1d",
    "taifex_open_interest_log1p",
    "taifex_open_interest_change_asinh",
    "taifex_spread_order_volume_log1p",
    "taifex_bid_ask_spread_ratio",
    "taifex_days_to_tenor_key_log1p",
    "taifex_tenor_rank_scaled",
    "taifex_delivery_month_sin",
    "taifex_delivery_month_cos",
    "taifex_expiry_slot_lane_scaled",
    "taifex_is_weekly",
    "taifex_is_stock_future",
    "taifex_is_etf_future",
    "taifex_is_domestic_index_future",
    "taifex_is_foreign_index_future",
)


@dataclass(frozen=True, slots=True)
class TaiwanFuturesPortfolioDaily:
    """Executor-only aligned arrays; none of these identities enter features."""

    dates: np.ndarray
    symbols: tuple[str, ...]
    contracts: np.ndarray
    holding_log_returns: np.ndarray
    executable_mask: np.ndarray
    must_liquidate_mask: np.ndarray
    can_hold_overnight_mask: np.ndarray
    contract_multipliers: np.ndarray
    fee_per_contract_per_side_twd: np.ndarray
    fee_rate_per_open_notional: np.ndarray
    source_path: str
    manifest_path: str
    contract_version: int = TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION


def _fixed_fee_contract_metadata(
    official_product: str,
    product_name: str,
    asset_class: str,
) -> tuple[float | None, str | None, str | None]:
    """Return multiplier, account fee group, and unsupported reason.

    Fee groups are the user's SinoPac network-order account schedule.  The
    amounts remain configuration values; this data contract owns only the
    unambiguous product-to-group mapping.
    """

    product = str(official_product).strip().upper()
    name = unicodedata.normalize("NFKC", str(product_name or "")).replace(
        "臺", "台"
    )
    if product == "MSF":
        return None, None, "legacy_usd_notional_requires_historical_fx"
    if asset_class in {"stock_future", "etf_future"} and product.endswith("1"):
        return None, None, "adjusted_contract_unit_requires_historical_notice"
    if asset_class == "index_future":
        multiplier = TAIFEX_INDEX_FUTURES_MULTIPLIERS_TWD.get(product)
        if multiplier is None:
            return None, None, "missing_official_index_multiplier"
        if product in _SINOPAC_LARGE_FEE_INDEX_ROOTS:
            group = "large"
        elif product in _SINOPAC_MICRO_FEE_INDEX_ROOTS:
            group = "micro"
        else:
            group = "standard"
        return multiplier, group, None
    is_small = name.startswith("小型")
    if asset_class == "stock_future":
        return (100.0 if is_small else 2_000.0), "stock", None
    if asset_class == "etf_future":
        return (1_000.0 if is_small else 10_000.0), (
            "micro" if is_small else "standard"
        ), None
    return None, None, f"unsupported_asset_class:{asset_class}"


def _require_dependencies() -> None:
    if pl is None or pa is None or pq is None:
        raise RuntimeError(
            "TAIFEX futures portfolio data requires polars and pyarrow"
        )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _normalized_name(value: object) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).replace("臺", "台")
    text = re.sub(r"^小型", "", text)
    text = re.sub(r"期貨$", "", text)
    text = text.replace("ETF", "")
    return re.sub(r"[^0-9A-Za-z\u4e00-\u9fff]", "", text).casefold()


def _nth_weekday(year: int, month: int, weekday: int, ordinal: int) -> date:
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (ordinal - 1))


def calendar_expiry_date(contract: str) -> date:
    """Resolve YYYYMM / YYYYMMWn to a deterministic tenor-ordering key.

    Weekly MTX contracts use their numbered Wednesday.  The third Wednesday
    for monthly contracts is only a sorting/proximity key; it is not presented
    as the product-specific legal last-trading date.  Mandatory liquidation is
    derived from physical-contract continuity in the official archive.
    """

    matched = re.fullmatch(r"(\d{4})(\d{2})(?:W([1-5]))?", str(contract).strip())
    if matched is None:
        raise ValueError(f"unsupported TAIFEX contract code {contract!r}")
    year = int(matched.group(1))
    month = int(matched.group(2))
    if not 1 <= month <= 12:
        raise ValueError(f"invalid TAIFEX contract month {contract!r}")
    week = int(matched.group(3) or 3)
    return _nth_weekday(year, month, weekday=2, ordinal=week)


def build_product_master(
    product_master_path: str | Path = DEFAULT_PRODUCT_MASTER_PATH,
    stock_master_path: str | Path = DEFAULT_STOCK_MASTER_PATH,
    official_product_code_path: str | Path = DEFAULT_OFFICIAL_PRODUCT_CODE_PATH,
    *,
    source_products: list[str] | tuple[str, ...] | None = None,
) -> Any:
    """Classify every requested historical TAIFEX product code."""

    _require_dependencies()
    products_path = Path(product_master_path)
    stocks_path = Path(stock_master_path)
    official_path = Path(official_product_code_path)
    if not official_path.exists():
        raise FileNotFoundError(
            f"official TAIFEX product-code master is missing: {official_path}; "
            "run scripts/download_taifex_contract_codes.py"
        )
    # The broker catalogue contributes current live aliases only.  Historical
    # research ownership comes from the official archive plus the official
    # TAIFEX code table, so an offline rebuild must not require broker
    # credentials or a point-in-time-incorrect current catalogue.
    products = (
        pl.read_csv(products_path, infer_schema_length=10_000).select(
            pl.col("root").cast(pl.String).str.strip_chars().str.to_uppercase(),
            pl.col("product_name").cast(pl.String).str.strip_chars(),
        )
        if products_path.exists()
        else pl.DataFrame(
            schema={"root": pl.String, "product_name": pl.String}
        )
    )
    official_codes = pl.read_csv(official_path, infer_schema_length=10_000).with_columns(
        pl.col("code").cast(pl.String).str.strip_chars().str.to_uppercase(),
        pl.col("product_name").cast(pl.String).str.strip_chars(),
    )
    stocks = pl.read_csv(stocks_path, infer_schema_length=10_000).with_columns(
        pl.col("code").cast(pl.String).str.strip_chars().str.to_uppercase(),
        pl.col("name").cast(pl.String).str.strip_chars(),
        pl.col("security_type").cast(pl.String).str.to_lowercase(),
    )

    name_to_rows: dict[str, list[dict[str, str]]] = {}
    stock_by_code: dict[str, dict[str, str]] = {}
    for row in stocks.select("code", "name", "security_type").iter_rows(named=True):
        name_to_rows.setdefault(_normalized_name(row["name"]), []).append(row)
        stock_by_code[str(row["code"])] = row

    official_names = {
        str(row["code"]): str(row["product_name"])
        for row in official_codes.iter_rows(named=True)
    }
    current_aliases: dict[str, list[str]] = {}
    current_names: dict[str, str] = {}
    for row in products.iter_rows(named=True):
        root = str(row["root"])
        official_product = _OFFICIAL_PRODUCT_OVERRIDES.get(root, root)
        current_aliases.setdefault(official_product, []).append(root)
        current_names.setdefault(official_product, str(row["product_name"]))
    requested = sorted(
        {
            str(value).strip().upper()
            for value in (
                source_products
                if source_products is not None
                else list(current_aliases)
            )
            if str(value).strip()
        }
    )

    records: list[dict[str, Any]] = []
    for official_product in requested:
        if official_product in _EXCLUDED_NON_EQUITY_ROOTS:
            continue
        product_name = official_names.get(official_product) or current_names.get(
            official_product
        )
        if not product_name:
            raise ValueError(
                f"TAIFEX product code {official_product!r} is absent from the official master"
            )
        underlying_symbol: str | None = None
        security_type: str | None = None
        asset_class: str
        region: str
        if official_product in _FOREIGN_INDEX_ROOTS:
            asset_class = "index_future"
            region = "foreign"
        elif official_product in _DOMESTIC_INDEX_ROOTS:
            asset_class = "index_future"
            region = "domestic"
        else:
            matches = name_to_rows.get(_normalized_name(product_name), [])
            override = _UNDERLYING_OVERRIDES.get(official_product)
            if override is not None:
                candidate = stock_by_code.get(override)
                matches = [] if candidate is None else [candidate]
            if len(matches) != 1:
                raise ValueError(
                    f"cannot uniquely map TAIFEX product={official_product!r} "
                    f"name={product_name!r} to TWSE/TPEx: {matches!r}"
                )
            underlying_symbol = str(matches[0]["code"])
            security_type = str(matches[0]["security_type"])
            if security_type not in {"stock", "etf"}:
                raise ValueError(
                    f"unsupported futures underlying security_type={security_type!r} "
                    f"for product={official_product!r}"
                )
            asset_class = f"{security_type}_future"
            region = "domestic"
        multiplier, fee_group, unsupported_reason = _fixed_fee_contract_metadata(
            official_product,
            product_name,
            asset_class,
        )
        records.append(
            {
                "shioaji_roots": ",".join(
                    sorted(current_aliases.get(official_product, []))
                ),
                "official_product": official_product,
                "product_name": product_name,
                "underlying_symbol": underlying_symbol,
                "underlying_security_type": security_type,
                "asset_class": asset_class,
                "region": region,
                "listing_scope": (
                    "current" if official_product in current_aliases else "historical"
                ),
                "contract_multiplier": multiplier,
                "sinopac_network_fee_group": fee_group,
                "fixed_fee_research_supported": unsupported_reason is None,
                "fixed_fee_research_exclusion_reason": unsupported_reason,
            }
        )

    return pl.DataFrame(records).sort("official_product")


def _contract_metadata(source: Any) -> Any:
    global_last = source.select(pl.col("date").max()).collect().item()
    contracts = (
        source.group_by("product", "contract")
        .agg(
            pl.col("date").min().alias("first_observed_date"),
            pl.col("date").max().alias("last_observed_date"),
            pl.col("series_type").last().alias("contract_series_type"),
        )
        .collect()
    )
    expiry_lookup = {
        contract: calendar_expiry_date(contract)
        for contract in contracts["contract"].unique().to_list()
    }
    contracts = contracts.with_columns(
        pl.col("contract")
        .replace_strict(expiry_lookup, return_dtype=pl.Date)
        .alias("tenor_sort_date")
    )
    return contracts.with_columns(
        pl.when(pl.col("last_observed_date") < pl.lit(global_last))
        .then(pl.min_horizontal("last_observed_date", "tenor_sort_date"))
        .otherwise(pl.col("tenor_sort_date"))
        .alias("resolved_last_trade_date")
    )


def _stable_rank_map(frame: Any, contract_meta: Any) -> Any:
    """Rank listed intervals for a causal tenor feature, never symbol identity."""

    pieces: list[Any] = []
    # Use the exchange-wide general-session calendar.  A far contract can have
    # zero transactions on an otherwise open market day; omitting that date
    # would incorrectly force an early liquidation.
    date_values = frame["date"].unique().sort().to_numpy()
    for key, group in contract_meta.partition_by("product", as_dict=True).items():
        product = str(key[0] if isinstance(key, tuple) else key)
        interval_frames: list[Any] = []
        for row in group.iter_rows(named=True):
            active_dates = date_values[
                (date_values >= np.datetime64(row["first_observed_date"], "D"))
                & (date_values <= np.datetime64(row["last_observed_date"], "D"))
            ]
            if active_dates.size == 0:
                continue
            interval_frames.append(
                pl.DataFrame(
                    {
                        "date": active_dates.astype("datetime64[D]"),
                        "product": [product] * int(active_dates.size),
                        "contract": [str(row["contract"])] * int(active_dates.size),
                        "resolved_last_trade_date": [row["resolved_last_trade_date"]]
                        * int(active_dates.size),
                    }
                )
            )
        if interval_frames:
            pieces.append(pl.concat(interval_frames, how="vertical"))
    if not pieces:
        raise ValueError("cannot construct TAIFEX active-contract rank intervals")
    active = pl.concat(pieces, how="vertical").sort(
        "date", "product", "resolved_last_trade_date", "contract"
    )
    return active.with_columns(
        pl.int_range(1, pl.len() + 1).over("date", "product").alias("tenor_rank")
    ).select("date", "product", "contract", "tenor_rank")


def _stable_expiry_slot_map(contract_meta: Any) -> Any:
    """Assign a lifetime-stable delivery-month/week slot to each contract.

    Most product/month pairs need one lane.  A few foreign index products list
    the same calendar delivery month in adjacent years at the same time.  A
    deterministic interval coloring adds L2 (or higher) only when necessary;
    contracts never migrate between lanes merely because a nearer expiry ends.
    """

    records: list[dict[str, Any]] = []
    for row in contract_meta.iter_rows(named=True):
        matched = re.fullmatch(
            r"(\d{4})(\d{2})(?:W([1-5]))?", str(row["contract"]).strip()
        )
        if matched is None:
            raise ValueError(f"unsupported TAIFEX contract code {row['contract']!r}")
        delivery_year = int(matched.group(1))
        delivery_month = int(matched.group(2))
        delivery_week = int(matched.group(3) or 0)
        base = f"M{delivery_month:02d}"
        if delivery_week:
            base += f"W{delivery_week}"
        records.append(
            {
                **row,
                "delivery_year": delivery_year,
                "delivery_month": delivery_month,
                "delivery_week": delivery_week,
                "expiry_slot_base": base,
            }
        )

    assigned: list[dict[str, Any]] = []
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in records:
        groups.setdefault(
            (str(row["product"]), str(row["expiry_slot_base"])), []
        ).append(row)
    for (product, base), group in sorted(groups.items()):
        lane_ends: list[date] = []
        ordered = sorted(
            group,
            key=lambda row: (
                row["first_observed_date"],
                row["resolved_last_trade_date"],
                str(row["contract"]),
            ),
        )
        for row in ordered:
            first = row["first_observed_date"]
            lane_index = next(
                (
                    index
                    for index, last in enumerate(lane_ends)
                    if last < first
                ),
                len(lane_ends),
            )
            if lane_index == len(lane_ends):
                lane_ends.append(row["last_observed_date"])
            else:
                lane_ends[lane_index] = row["last_observed_date"]
            lane = lane_index + 1
            assigned.append(
                {
                    "product": product,
                    "contract": str(row["contract"]),
                    "delivery_year": int(row["delivery_year"]),
                    "delivery_month": int(row["delivery_month"]),
                    "delivery_week": int(row["delivery_week"]),
                    "expiry_slot_base": base,
                    "expiry_slot_lane": lane,
                    "symbol": f"{product}_{base}_L{lane}",
                }
            )
    return pl.DataFrame(assigned).sort("product", "contract")


def _fixed_portfolio_slot_map(
    contract_meta: Any,
    market_dates: Any,
    *,
    fixed_slot_count: int = TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT,
    cooldown_sessions: int = (
        TAIFEX_FUTURES_PORTFOLIO_SLOT_REUSE_COOLDOWN_SESSIONS
    ),
) -> Any:
    """Color physical-contract intervals into fixed lifetime-stable slots.

    Intervals are inclusive.  A slot becomes reusable only after the contract's
    last observed market session plus ``cooldown_sessions``.  For lookback 32,
    the 31-session quarantine guarantees that a new occupant's model window
    cannot contain any feature row from the prior physical contract.
    """

    capacity = int(fixed_slot_count)
    cooldown = int(cooldown_sessions)
    if capacity <= 0:
        raise ValueError("TAIFEX fixed portfolio slot count must be positive")
    if cooldown < 0:
        raise ValueError("TAIFEX portfolio slot cooldown must be non-negative")
    calendar = sorted(market_dates)
    if not calendar:
        raise ValueError("TAIFEX fixed portfolio slots require a market calendar")
    calendar_index = {value: index for index, value in enumerate(calendar)}

    intervals: list[tuple[int, int, str, str]] = []
    for row in contract_meta.iter_rows(named=True):
        first = row["first_observed_date"]
        last = row["last_observed_date"]
        if first not in calendar_index or last not in calendar_index:
            raise RuntimeError("TAIFEX physical-contract interval is off calendar")
        intervals.append(
            (
                int(calendar_index[first]),
                min(int(calendar_index[last]) + cooldown, len(calendar) - 1),
                str(row["product"]),
                str(row["contract"]),
            )
        )
    intervals.sort(key=lambda value: (value[0], value[1], value[2], value[3]))

    # Active heap is ordered by inclusive release session.  Free slots use a
    # second min-heap so a rebuild is deterministic regardless of input order.
    active: list[tuple[int, int]] = []
    free_slots: list[int] = []
    next_slot = 1
    assigned: list[dict[str, Any]] = []
    for first_index, quarantine_end, product, contract in intervals:
        while active and active[0][0] < first_index:
            _, released_slot = heapq.heappop(active)
            heapq.heappush(free_slots, released_slot)
        if free_slots:
            slot = heapq.heappop(free_slots)
        else:
            slot = next_slot
            next_slot += 1
        if slot > capacity:
            raise RuntimeError(
                "TAIFEX fixed portfolio slot capacity exceeded: "
                f"required_at_least={slot} configured={capacity}; rebuild into "
                "a new data/checkpoint contract with a larger audited capacity"
            )
        heapq.heappush(active, (quarantine_end, slot))
        assigned.append(
            {
                "product": product,
                "contract": contract,
                "portfolio_slot": slot,
                "symbol": f"TAIFEX_SLOT_{slot:04d}",
            }
        )
    return pl.DataFrame(assigned).sort("product", "contract")


def build_continuous_daily(
    source_path: str | Path = DEFAULT_SOURCE_PATH,
    product_master_path: str | Path = DEFAULT_PRODUCT_MASTER_PATH,
    stock_master_path: str | Path = DEFAULT_STOCK_MASTER_PATH,
    official_product_code_path: str | Path = DEFAULT_OFFICIAL_PRODUCT_CODE_PATH,
) -> tuple[Any, Any]:
    """Build the physical-contract-safe delivery-month-slot daily table."""

    _require_dependencies()
    source_path = Path(source_path)
    source = pl.scan_parquet(source_path)
    source_products = (
        source.select(pl.col("product").cast(pl.String).str.strip_chars().str.to_uppercase())
        .unique()
        .collect()["product"]
        .to_list()
    )
    product_master = build_product_master(
        product_master_path,
        stock_master_path,
        official_product_code_path,
        source_products=source_products,
    )
    # Fixed-per-contract commissions cannot be represented truthfully without
    # a TWD contract notional.  Exclude only explicitly audited unsupported
    # families; do not fall back to a guessed multiplier.
    selected = product_master.filter(
        pl.col("fixed_fee_research_supported")
    )["official_product"].to_list()
    product_id_lookup = {
        product: index
        for index, product in enumerate(sorted(str(value) for value in selected), start=1)
    }
    product_master = product_master.with_columns(
        pl.col("official_product")
        .replace_strict(product_id_lookup, default=None, return_dtype=pl.UInt16)
        .alias("taifex_product_id")
    )
    raw = (
        source
        .filter(
            (pl.col("session") == "一般")
            & pl.col("product").is_in(selected)
        )
        .sort("date", "product", "contract")
    )
    duplicates = (
        raw.group_by("date", "product", "contract")
        .len()
        .filter(pl.col("len") != 1)
        .limit(1)
        .collect()
    )
    if duplicates.height:
        raise ValueError("TAIFEX source contains duplicate date/product/contract rows")
    observed = raw.collect()
    contract_meta = _contract_metadata(raw)
    rank_map = _stable_rank_map(observed, contract_meta)
    frame = rank_map.join(
        contract_meta,
        on=["product", "contract"],
        how="left",
        validate="m:1",
    )
    frame = frame.join(
        observed.with_columns(pl.lit(True).alias("source_row_observed")),
        on=["date", "product", "contract"],
        how="left",
        validate="1:1",
    )
    if frame["tenor_rank"].null_count():
        raise RuntimeError("TAIFEX contract interval ranking left unmapped rows")
    # Delivery metadata remains explicit model context (for example every
    # August contract has delivery_month=8), while the tensor symbol itself is
    # a globally fixed, lifetime-stable capacity slot.
    delivery_map = _stable_expiry_slot_map(contract_meta).rename(
        {"symbol": "delivery_slot_symbol"}
    )
    frame = frame.join(
        delivery_map,
        on=["product", "contract"],
        how="left",
        validate="m:1",
    )
    portfolio_slot_map = _fixed_portfolio_slot_map(
        contract_meta,
        observed["date"].unique().sort().to_list(),
    )
    frame = frame.join(
        portfolio_slot_map,
        on=["product", "contract"],
        how="left",
        validate="m:1",
    )
    if frame["symbol"].null_count():
        raise RuntimeError("TAIFEX fixed portfolio slot assignment left unmapped rows")
    slot_duplicates = (
        frame.group_by("date", "symbol")
        .len()
        .filter(pl.col("len") != 1)
        .limit(1)
    )
    if slot_duplicates.height:
        raise RuntimeError(
            "fixed portfolio slots collide for simultaneous physical contracts"
        )
    frame = frame.sort("symbol", "date").with_columns(
        pl.col("source_row_observed").fill_null(False),
        pl.coalesce("series_type", "contract_series_type").alias("series_type"),
        pl.when(pl.col("settlement").is_finite() & (pl.col("settlement") > 0.0))
        .then(pl.col("settlement"))
        .when(pl.col("close").is_finite() & (pl.col("close") > 0.0))
        .then(pl.col("close"))
        .otherwise(None)
        .alias("_valuation_mark_source"),
        pl.col("open_interest")
        .forward_fill()
        .over("symbol", "contract")
        .fill_null(0),
        pl.col("volume").fill_null(0),
        pl.col("spread_order_volume").fill_null(0),
    ).with_columns(
        pl.when(
            pl.col("source_row_observed")
            & pl.col("open").is_finite()
            & (pl.col("open") > 0.0)
        )
        .then(pl.col("open"))
        .otherwise(
            pl.col("_valuation_mark_source")
            .forward_fill()
            .over("symbol", "contract")
        )
        .alias("valuation_open"),
        pl.col("_valuation_mark_source")
        .forward_fill()
        .over("symbol", "contract")
        .alias("valuation_settlement"),
    ).with_columns(
        pl.col("valuation_open").alias("open"),
        pl.when(pl.col("source_row_observed"))
        .then(pl.col("high"))
        .otherwise(pl.col("valuation_open"))
        .alias("high"),
        pl.when(pl.col("source_row_observed"))
        .then(pl.col("low"))
        .otherwise(pl.col("valuation_open"))
        .alias("low"),
        pl.when(pl.col("source_row_observed"))
        .then(pl.col("close"))
        .otherwise(pl.col("valuation_open"))
        .alias("close"),
        pl.col("valuation_settlement").alias("settlement"),
    )
    invalid_valuation = frame.filter(
        ~pl.col("valuation_open").is_finite() | (pl.col("valuation_open") <= 0.0)
    ).height
    if invalid_valuation:
        raise RuntimeError("active TAIFEX contract interval has no causal valuation")

    calendar = (
        frame.select("date")
        .unique()
        .sort("date")
        .with_columns(pl.col("date").shift(-1).alias("next_market_date"))
    )
    frame = frame.join(calendar, on="date", how="left", validate="m:1")
    frame = frame.sort("symbol", "date").with_columns(
        pl.col("date").shift(-1).over("symbol").alias("next_symbol_date"),
        pl.col("product").shift(-1).over("symbol").alias("next_product"),
        pl.col("contract").shift(-1).over("symbol").alias("next_contract"),
        pl.col("open").shift(-1).over("symbol").alias("next_open"),
        pl.col("date").shift(1).over("symbol").alias("previous_symbol_date"),
        pl.col("product").shift(1).over("symbol").alias("previous_product"),
        pl.col("contract").shift(1).over("symbol").alias("previous_contract"),
        pl.col("settlement").shift(1).over("symbol").alias("previous_settlement"),
        pl.col("volume").shift(1).over("symbol").alias("previous_volume"),
        pl.col("open_interest").shift(1).over("symbol").alias("previous_open_interest"),
    )
    previous_calendar = calendar.select(
        pl.col("date").alias("previous_market_date"),
        pl.col("next_market_date").alias("date"),
    )
    frame = frame.join(previous_calendar, on="date", how="left", validate="m:1")
    same_next = (
        (pl.col("next_symbol_date") == pl.col("next_market_date"))
        & (pl.col("next_product") == pl.col("product"))
        & (pl.col("next_contract") == pl.col("contract"))
        & pl.col("next_open").is_finite()
        & (pl.col("next_open") > 0.0)
    ).fill_null(False)
    same_previous = (
        (pl.col("previous_symbol_date") == pl.col("previous_market_date"))
        & (pl.col("previous_product") == pl.col("product"))
        & (pl.col("previous_contract") == pl.col("contract"))
    ).fill_null(False)
    executable = (
        pl.col("source_row_observed")
        & pl.col("open").is_finite()
        & (pl.col("open") > 0.0)
        & pl.col("close").is_finite()
        & (pl.col("close") > 0.0)
    )
    frame = frame.with_columns(
        executable.alias("executable"),
        same_next.alias("can_hold_overnight"),
        (~same_next).alias("must_liquidate"),
        pl.when(same_next)
        .then((pl.col("next_open") / pl.col("open")).log())
        .when(pl.col("open").is_finite() & pl.col("close").is_finite())
        .then((pl.col("close") / pl.col("open")).log())
        .otherwise(None)
        .alias("holding_log_return"),
        pl.when(same_next)
        .then(pl.lit("carry_same_contract"))
        .when(pl.col("next_market_date").is_null())
        .then(pl.lit("dataset_terminal"))
        .when(pl.col("date") >= pl.col("resolved_last_trade_date"))
        .then(pl.lit("last_trade_date"))
        .when(pl.col("next_symbol_date") != pl.col("next_market_date"))
        .then(pl.lit("next_session_missing"))
        .otherwise(pl.lit("expiry_slot_contract_change"))
        .alias("liquidation_reason"),
        (pl.col("tenor_sort_date") - pl.col("date"))
        .dt.total_days()
        .clip(lower_bound=0)
        .alias("calendar_days_to_tenor_key"),
        same_previous.alias("same_contract_as_previous_session"),
        (~same_previous).alias("lifecycle_reset"),
        pl.concat_str("product", "contract", separator=":").alias(
            "physical_contract"
        ),
    )

    metadata = product_master.rename({"official_product": "product"})
    frame = frame.join(metadata, on="product", how="left", validate="m:1")
    if frame["asset_class"].null_count():
        raise RuntimeError("selected TAIFEX rows lost product classification")
    if (
        frame["contract_multiplier"].null_count()
        or frame["sinopac_network_fee_group"].null_count()
    ):
        raise RuntimeError("fixed-fee TAIFEX rows lost multiplier/fee classification")

    previous_valid = same_previous
    frame = frame.with_columns(
        pl.when(previous_valid & pl.col("settlement").is_finite() & (pl.col("settlement") > 0.0)
                & pl.col("previous_settlement").is_finite() & (pl.col("previous_settlement") > 0.0))
        .then((pl.col("settlement") / pl.col("previous_settlement")).log())
        .otherwise(None)
        .alias("taifex_settlement_logret_1d"),
        pl.col("volume").clip(lower_bound=0).log1p().alias("taifex_volume_log1p"),
        pl.when(previous_valid & (pl.col("volume") > 0) & (pl.col("previous_volume") > 0))
        .then((pl.col("volume") / pl.col("previous_volume")).log())
        .otherwise(None)
        .alias("taifex_volume_logret_1d"),
        pl.col("open_interest").fill_null(0).clip(lower_bound=0).log1p().alias(
            "taifex_open_interest_log1p"
        ),
        pl.when(previous_valid)
        .then((pl.col("open_interest").fill_null(0) - pl.col("previous_open_interest").fill_null(0)).arcsinh())
        .otherwise(None)
        .alias("taifex_open_interest_change_asinh"),
        pl.col("spread_order_volume").fill_null(0).clip(lower_bound=0).log1p().alias(
            "taifex_spread_order_volume_log1p"
        ),
        pl.when(
            pl.col("last_bid").is_finite()
            & pl.col("last_ask").is_finite()
            & (pl.col("last_bid") > 0.0)
            & (pl.col("last_ask") >= pl.col("last_bid"))
        )
        .then(
            (pl.col("last_ask") - pl.col("last_bid"))
            / ((pl.col("last_ask") + pl.col("last_bid")) * 0.5)
        )
        .otherwise(None)
        .alias("taifex_bid_ask_spread_ratio"),
        pl.col("calendar_days_to_tenor_key").log1p().alias(
            "taifex_days_to_tenor_key_log1p"
        ),
        (pl.col("tenor_rank").cast(pl.Float64) / 9.0).alias(
            "taifex_tenor_rank_scaled"
        ),
        (pl.col("delivery_month").cast(pl.Float64) * (2.0 * np.pi / 12.0))
        .sin()
        .alias("taifex_delivery_month_sin"),
        (pl.col("delivery_month").cast(pl.Float64) * (2.0 * np.pi / 12.0))
        .cos()
        .alias("taifex_delivery_month_cos"),
        (pl.col("expiry_slot_lane").cast(pl.Float64) / 2.0).alias(
            "taifex_expiry_slot_lane_scaled"
        ),
        (pl.col("series_type") == "weekly").cast(pl.Float64).alias("taifex_is_weekly"),
        (pl.col("asset_class") == "stock_future").cast(pl.Float64).alias("taifex_is_stock_future"),
        (pl.col("asset_class") == "etf_future").cast(pl.Float64).alias("taifex_is_etf_future"),
        ((pl.col("asset_class") == "index_future") & (pl.col("region") == "domestic"))
        .cast(pl.Float64).alias("taifex_is_domestic_index_future"),
        ((pl.col("asset_class") == "index_future") & (pl.col("region") == "foreign"))
        .cast(pl.Float64).alias("taifex_is_foreign_index_future"),
    )
    return frame.sort("date", "product", "tenor_rank", "symbol"), product_master


def _write_symbol_parquets(frame: Any, output_dir: Path) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    columns = {
        "high": "max",
        "low": "min",
        "volume": "Trading_Volume",
    }
    count = 0
    written_names: set[str] = set()
    for key, group in frame.partition_by("symbol", as_dict=True, maintain_order=True).items():
        symbol = str(key[0] if isinstance(key, tuple) else key)
        output_name = f"{symbol}_features.parquet"
        model_frame = (
            group.select(
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "lifecycle_reset",
                pl.col("must_liquidate").alias("return_quarantined"),
            )
            .rename(columns)
            .with_columns(pl.col("close").alias("adjclose"))
            .sort("date")
        )
        model_frame.write_parquet(
            output_dir / output_name,
            compression="zstd",
            statistics=True,
        )
        written_names.add(output_name)
        count += 1
    # A contract-version rebuild may change the standardized universe.  Remove
    # only stale generated symbol parquets after every replacement has been
    # written successfully, so an old Rn universe cannot contaminate v2.
    for path in output_dir.glob("*_features.parquet"):
        if path.name not in written_names:
            path.unlink()
    return count


def _build_model_features(
    continuous: Any,
    public_feature_path: Path,
) -> Any:
    public_columns = pq.read_schema(public_feature_path).names
    public_model_columns = [
        name
        for name in public_columns
        if name not in {"date", "symbol"} and not name.startswith("_")
    ]
    dates = continuous.select(
        pl.col("date").min().alias("date_start"),
        pl.col("date").max().alias("date_end"),
    ).row(0)
    needed_underlyings = (
        continuous["underlying_symbol"].drop_nulls().unique().to_list()
    )
    public = (
        pl.scan_parquet(public_feature_path)
        .filter(
            (pl.col("date") >= pl.lit(dates[0]))
            & (pl.col("date") <= pl.lit(dates[1]))
            & pl.col("symbol").is_in(["__MARKET__", *needed_underlyings])
        )
        .select("date", "symbol", *public_model_columns)
    )
    market = public.filter(pl.col("symbol") == "__MARKET__").drop("symbol")
    market = market.rename({name: f"__market_{name}" for name in public_model_columns})
    underlying = public.filter(pl.col("symbol") != "__MARKET__").rename(
        {"symbol": "underlying_symbol"}
    )
    features = continuous.select(
        "date",
        "symbol",
        "underlying_symbol",
        *FUTURES_MODEL_FEATURE_COLUMNS,
    ).lazy()
    features = features.join(market, on="date", how="left").join(
        underlying,
        on=["date", "underlying_symbol"],
        how="left",
    )
    expressions = [
        pl.coalesce(pl.col(name), pl.col(f"__market_{name}")).alias(name)
        for name in public_model_columns
    ]
    return features.select(
        "date",
        "symbol",
        *FUTURES_MODEL_FEATURE_COLUMNS,
        *expressions,
    ).sort("date", "symbol").collect(engine="streaming")


def _write_parquet_with_metadata(frame: Any, path: Path, *, dataset: str) -> None:
    table = frame.to_arrow()
    metadata = dict(table.schema.metadata or {})
    metadata.update(
        {
            b"stockagent.dataset": dataset.encode("utf-8"),
            b"stockagent.contract_version": str(
                TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION
            ).encode("ascii"),
        }
    )
    pq.write_table(
        table.replace_schema_metadata(metadata),
        path,
        compression="zstd",
        use_dictionary=True,
        write_statistics=True,
    )


def build_dataset(
    *,
    source_path: str | Path = DEFAULT_SOURCE_PATH,
    product_master_path: str | Path = DEFAULT_PRODUCT_MASTER_PATH,
    stock_master_path: str | Path = DEFAULT_STOCK_MASTER_PATH,
    official_product_code_path: str | Path = DEFAULT_OFFICIAL_PRODUCT_CODE_PATH,
    public_feature_path: str | Path = DEFAULT_PUBLIC_FEATURE_PATH,
    output_root: str | Path = DEFAULT_OUTPUT_ROOT,
) -> dict[str, Any]:
    """Materialize the audited long table and generic panel inputs."""

    _require_dependencies()
    source_path = Path(source_path)
    product_master_path = Path(product_master_path)
    stock_master_path = Path(stock_master_path)
    official_product_code_path = Path(official_product_code_path)
    public_feature_path = Path(public_feature_path)
    output_root = Path(output_root)
    for required in (
        source_path,
        stock_master_path,
        official_product_code_path,
        public_feature_path,
    ):
        if not required.exists():
            raise FileNotFoundError(required)
    output_root.mkdir(parents=True, exist_ok=True)
    symbols_dir = output_root / "symbols"
    symbols_dir.mkdir(parents=True, exist_ok=True)

    continuous, product_master = build_continuous_daily(
        source_path,
        product_master_path,
        stock_master_path,
        official_product_code_path,
    )
    continuous_path = output_root / "continuous_daily.parquet"
    product_output_path = output_root / "product_master.parquet"
    feature_path = output_root / "model_features.parquet"
    _write_parquet_with_metadata(
        continuous,
        continuous_path,
        dataset="taifex_futures_portfolio_continuous_daily",
    )
    _write_parquet_with_metadata(
        product_master,
        product_output_path,
        dataset="taifex_futures_portfolio_product_master",
    )
    symbol_count = _write_symbol_parquets(continuous, symbols_dir)
    if int(symbol_count) != TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT:
        raise RuntimeError(
            "TAIFEX production dataset no longer fills its audited fixed-slot "
            f"universe: generated={symbol_count} expected="
            f"{TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT}; create a new data and "
            "checkpoint contract instead of silently changing model width"
        )
    model_features = _build_model_features(continuous, public_feature_path)
    _write_parquet_with_metadata(
        model_features,
        feature_path,
        dataset="taifex_futures_portfolio_public_model_features",
    )

    counts = {
        row[0]: int(row[1])
        for row in product_master.group_by("asset_class").len().iter_rows()
    }
    reason_counts = {
        row[0]: int(row[1])
        for row in continuous.group_by("liquidation_reason").len().iter_rows()
    }
    daily_capacity = (
        continuous.group_by("date")
        .agg(
            pl.len().alias("active_contracts"),
            pl.col("executable").sum().alias("executable_contracts"),
            pl.col("product").n_unique().alias("active_products"),
        )
        .with_columns(pl.col("date").dt.year().alias("year"))
    )
    annual_capacity = (
        daily_capacity.group_by("year")
        .agg(
            pl.col("active_contracts").max().alias("maximum_active_contracts"),
            pl.col("executable_contracts")
            .max()
            .alias("maximum_executable_contracts"),
            pl.col("active_products").max().alias("maximum_active_products"),
        )
        .sort("year")
    )
    annual_capacity_records = [
        {
            "year": int(row[0]),
            "maximum_active_contracts": int(row[1]),
            "maximum_executable_contracts": int(row[2]),
            "maximum_active_products": int(row[3]),
        }
        for row in annual_capacity.iter_rows()
    ]
    manifest: dict[str, Any] = {
        "dataset": "taifex_futures_portfolio_daily",
        "contract_version": TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION,
        "feature_contract_version": TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION,
        "execution_contract": (
            "features_through_t_minus_1_execute_only_observed_open_t_hold_"
            "same_physical_contract_across_zero_trade_days_with_last_known_"
            "positive_settlement_then_next_open_else_liquidate_own_close"
        ),
        "symbol_contract": (
            "fixed_1936_global_slots_lifetime_stable_physical_contract_"
            "31_session_reuse_quarantine"
        ),
        "session": "一般",
        "scope": (
            "historical_and_current_taifex_stock_etf_domestic_and_foreign_"
            "index_futures_with_verified_twd_contract_notional"
        ),
        "excluded_roots": sorted(_EXCLUDED_NON_EQUITY_ROOTS),
        "rows": int(continuous.height),
        "observed_transaction_rows": int(
            continuous["source_row_observed"].sum()
        ),
        "carry_forward_valuation_rows": int(
            (~continuous["source_row_observed"]).sum()
        ),
        "weekly_observed_rows": int(
            continuous.filter(
                pl.col("source_row_observed") & (pl.col("series_type") == "weekly")
            ).height
        ),
        "products": int(product_master.height),
        "fixed_fee_research_products": int(
            product_master["fixed_fee_research_supported"].sum()
        ),
        "fixed_fee_research_exclusions": [
            {
                "product": str(row[0]),
                "reason": str(row[1]),
            }
            for row in product_master.filter(
                ~pl.col("fixed_fee_research_supported")
            )
            .select(
                "official_product",
                "fixed_fee_research_exclusion_reason",
            )
            .iter_rows()
        ],
        "fixed_fee_contract": (
            "continuous_fractional_contracts; per_side_fee_twd divided_by "
            "valuation_open_times_contract_multiplier; no_tax_no_slippage"
        ),
        "logical_symbols": int(symbol_count),
        "fixed_model_output_slots": int(
            TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT
        ),
        "slot_reuse_cooldown_sessions": int(
            TAIFEX_FUTURES_PORTFOLIO_SLOT_REUSE_COOLDOWN_SESSIONS
        ),
        "maximum_safe_lookback_sessions": int(
            TAIFEX_FUTURES_PORTFOLIO_MAX_SAFE_LOOKBACK
        ),
        "maximum_simultaneous_active_contracts": int(
            daily_capacity["active_contracts"].max()
        ),
        "maximum_simultaneous_executable_contracts": int(
            daily_capacity["executable_contracts"].max()
        ),
        "annual_capacity": annual_capacity_records,
        "maximum_tenor_rank": int(continuous["tenor_rank"].max()),
        "maximum_expiry_slot_lane": int(continuous["expiry_slot_lane"].max()),
        "mandatory_liquidation_boundary": (
            "last_observed_physical_contract_session_or_dataset_terminal;_"
            "calendar_tenor_key_is_not_claimed_as_legal_expiry"
        ),
        "date_start": str(continuous["date"].min()),
        "date_end": str(continuous["date"].max()),
        "product_class_counts": counts,
        "liquidation_reason_counts": reason_counts,
        "model_feature_columns": list(FUTURES_MODEL_FEATURE_COLUMNS),
        "snapshot_only_public_features": (
            "retained_in_source_archive_but_excluded_by_training_config"
        ),
        "outputs": {},
        "sources": {},
    }
    for label, path in {
        "official_all_futures_daily": source_path,
        "official_taifex_product_codes": official_product_code_path,
        "twse_tpex_stock_master": stock_master_path,
        "tw_public_daily_features": public_feature_path,
    }.items():
        manifest["sources"][label] = {
            "path": str(path),
            "size": int(path.stat().st_size),
            "sha256": _sha256_file(path),
        }
    manifest["sources"]["shioaji_current_product_master"] = (
        {
            "path": str(product_master_path),
            "size": int(product_master_path.stat().st_size),
            "sha256": _sha256_file(product_master_path),
            "required": False,
        }
        if product_master_path.exists()
        else {"path": str(product_master_path), "present": False, "required": False}
    )
    for label, path in {
        "continuous_daily": continuous_path,
        "product_master": product_output_path,
        "model_features": feature_path,
    }.items():
        manifest["outputs"][label] = {
            "path": str(path),
            "size": int(path.stat().st_size),
            "sha256": _sha256_file(path),
        }
    manifest["outputs"]["symbols"] = {
        "path": str(symbols_dir),
        "files": int(symbol_count),
    }
    manifest_path = output_root / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest


def attach_futures_portfolio_daily(
    panel: PanelData,
    data_path: str | Path,
    *,
    fee_per_side_twd_by_group: dict[str, float],
) -> PanelData:
    """Align executor labels to a generic panel without changing features."""

    _require_dependencies()
    data_path = Path(data_path)
    manifest_path = data_path.parent / "manifest.json"
    if not data_path.exists() or not manifest_path.exists():
        raise FileNotFoundError(
            f"TAIFEX futures portfolio dataset/manifest missing: "
            f"{data_path}, {manifest_path}"
        )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if int(manifest.get("contract_version", -1)) != TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION:
        raise ValueError("TAIFEX futures portfolio contract version mismatch")
    if int(manifest.get("fixed_model_output_slots", -1)) != int(
        TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT
    ):
        raise ValueError("TAIFEX futures portfolio fixed-slot count mismatch")
    if len(panel.symbols) != TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT:
        raise ValueError(
            "TAIFEX futures panel symbol count differs from the fixed contract: "
            f"panel={len(panel.symbols)} expected="
            f"{TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT}"
        )
    expected = manifest.get("outputs", {}).get("continuous_daily", {}).get("sha256")
    if expected != _sha256_file(data_path):
        raise ValueError("TAIFEX futures portfolio data SHA-256 differs from manifest")

    dates = np.asarray(panel.dates, dtype="datetime64[D]")
    if dates.size == 0:
        raise ValueError("cannot attach TAIFEX futures data to an empty panel")
    date_start = date.fromisoformat(str(dates.min()))
    date_end = date.fromisoformat(str(dates.max()))
    table = pq.read_table(
        data_path,
        columns=[
            "date",
            "product",
            "symbol",
            "contract",
            "physical_contract",
            "tenor_rank",
            "open",
            "close",
            "volume",
            "holding_log_return",
            "executable",
            "must_liquidate",
            "can_hold_overnight",
            "contract_multiplier",
            "sinopac_network_fee_group",
        ],
        filters=[("date", ">=", date_start), ("date", "<=", date_end)],
        memory_map=True,
    )
    frame = pl.from_arrow(table).with_columns(
        pl.col("date").cast(pl.Date),
        pl.col("symbol").cast(pl.String),
    )
    symbols = tuple(str(symbol) for symbol in panel.symbols)
    date_index = {
        int(value.astype(np.int64)): idx for idx, value in enumerate(dates)
    }
    symbol_index = {value: idx for idx, value in enumerate(symbols)}
    shape = (len(dates), len(symbols))
    returns = np.full(shape, np.nan, dtype=np.float32)
    open_prices = np.full(shape, np.nan, dtype=np.float32)
    close_prices = np.full(shape, np.nan, dtype=np.float32)
    volumes = np.full(shape, np.nan, dtype=np.float32)
    executable = np.zeros(shape, dtype=bool)
    must_liquidate = np.zeros(shape, dtype=bool)
    can_hold = np.zeros(shape, dtype=bool)
    multipliers = np.full(shape, np.nan, dtype=np.float32)
    fee_per_contract = np.full(shape, np.nan, dtype=np.float32)
    fee_rate_per_open_notional = np.full(shape, np.nan, dtype=np.float32)
    contracts = np.full(shape, "", dtype="U16")
    benchmark_returns = np.zeros(len(dates), dtype=np.float32)
    benchmark_assigned = np.zeros(len(dates), dtype=bool)
    assigned = 0
    for row in frame.iter_rows(named=True):
        di = date_index.get(int(np.datetime64(row["date"], "D").astype(np.int64)))
        si = symbol_index.get(str(row["symbol"]))
        if di is None or si is None:
            continue
        returns[di, si] = row["holding_log_return"]
        open_prices[di, si] = row["open"]
        close_prices[di, si] = row["close"]
        volumes[di, si] = row["volume"]
        executable[di, si] = bool(row["executable"])
        must_liquidate[di, si] = bool(row["must_liquidate"])
        can_hold[di, si] = bool(row["can_hold_overnight"])
        multiplier = float(row["contract_multiplier"])
        fee_group = str(row["sinopac_network_fee_group"])
        if fee_group not in fee_per_side_twd_by_group:
            raise ValueError(
                f"missing SinoPac network fee for group={fee_group!r}"
            )
        fee = float(fee_per_side_twd_by_group[fee_group])
        opening_price = float(row["open"])
        if (
            not np.isfinite(multiplier)
            or multiplier <= 0.0
            or not np.isfinite(fee)
            or fee < 0.0
            or not np.isfinite(opening_price)
            or opening_price <= 0.0
        ):
            raise ValueError(
                "invalid fixed-fee contract metadata for "
                f"product={row['product']!r} symbol={row['symbol']!r}"
            )
        multipliers[di, si] = multiplier
        fee_per_contract[di, si] = fee
        fee_rate_per_open_notional[di, si] = fee / (
            opening_price * multiplier
        )
        contracts[di, si] = str(row["physical_contract"])
        if str(row["product"]) == "TX" and int(row["tenor_rank"]) == 1:
            if benchmark_assigned[di]:
                raise ValueError("TAIFEX TX front-month benchmark is not unique")
            value = row["holding_log_return"]
            benchmark_returns[di] = (
                float(value) if value is not None and np.isfinite(value) else 0.0
            )
            benchmark_assigned[di] = True
        assigned += 1
    if assigned == 0:
        raise ValueError("TAIFEX futures portfolio data has no rows aligned to panel")
    invalid = executable & ~np.isfinite(returns)
    if bool(invalid.any()):
        raise ValueError("executable TAIFEX futures rows have non-finite holding returns")
    if bool((can_hold & must_liquidate).any()):
        raise ValueError("can_hold_overnight and must_liquidate overlap")
    invalid_fee = (executable | must_liquidate | can_hold) & (
        ~np.isfinite(fee_rate_per_open_notional)
        | (fee_rate_per_open_notional < 0.0)
    )
    if bool(invalid_fee.any()):
        raise ValueError("active TAIFEX futures rows have invalid fixed fee rates")

    panel.returns_1d = returns
    panel.open_prices = open_prices
    panel.close_prices = close_prices
    panel.daily_volumes = volumes
    panel.tradable_mask = executable
    # Selection at t may only use whether an executable quote existed by t-1.
    # Synthetic carry-forward valuation rows keep an existing position alive in
    # the ledger but must not masquerade as a new-entry opportunity.
    panel.alive_mask = executable.copy()
    panel.can_buy_mask = executable.copy()
    panel.can_sell_mask = executable.copy()
    panel.can_short_open_mask = executable.copy()
    panel.force_short_cover_mask = np.zeros(shape, dtype=bool)
    panel.force_exit_mask = must_liquidate
    if not bool(benchmark_assigned.any()):
        raise ValueError("TAIFEX TX front-month benchmark rows are missing")
    panel.benchmark_returns = benchmark_returns
    panel.content_fingerprints = None
    panel.futures_portfolio_daily = TaiwanFuturesPortfolioDaily(
        dates=dates,
        symbols=symbols,
        contracts=contracts,
        holding_log_returns=returns,
        executable_mask=executable,
        must_liquidate_mask=must_liquidate,
        can_hold_overnight_mask=can_hold,
        contract_multipliers=multipliers,
        fee_per_contract_per_side_twd=fee_per_contract,
        fee_rate_per_open_notional=fee_rate_per_open_notional,
        source_path=str(data_path),
        manifest_path=str(manifest_path),
    )
    return panel


__all__ = [
    "DEFAULT_OFFICIAL_PRODUCT_CODE_PATH",
    "DEFAULT_OUTPUT_ROOT",
    "DEFAULT_PRODUCT_MASTER_PATH",
    "DEFAULT_PUBLIC_FEATURE_PATH",
    "DEFAULT_SOURCE_PATH",
    "DEFAULT_STOCK_MASTER_PATH",
    "FUTURES_MODEL_FEATURE_COLUMNS",
    "TAIFEX_FUTURES_PORTFOLIO_BACKTEST_CONTRACT_VERSION",
    "TAIFEX_FUTURES_PORTFOLIO_DATA_CONTRACT_VERSION",
    "TAIFEX_FUTURES_PORTFOLIO_FEATURE_CONTRACT_VERSION",
    "TAIFEX_FUTURES_PORTFOLIO_FIXED_SLOT_COUNT",
    "TAIFEX_FUTURES_PORTFOLIO_MAX_SAFE_LOOKBACK",
    "TAIFEX_FUTURES_PORTFOLIO_SLOT_REUSE_COOLDOWN_SESSIONS",
    "TaiwanFuturesPortfolioDaily",
    "attach_futures_portfolio_daily",
    "build_continuous_daily",
    "build_dataset",
    "build_product_master",
    "calendar_expiry_date",
]
