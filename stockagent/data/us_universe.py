from __future__ import annotations

import re


BROKER_TRADABLE_SECURITY_FILTER = "broker_tradable"

_AMERICAN_DEPOSITARY_PATTERN = re.compile(r"\bAmerican Depositar(?:y|ies)|\bADR\b|\bADRs\b|\bADS\b", re.IGNORECASE)
_FUND_LIKE_PATTERN = re.compile(
    r"\b(ETF|ETN|Exchange Traded|Fund|Closed-End Fund|CEF|Index|Portfolio|iShares|"
    r"ProShares|Direxion|Vanguard|SPDR|Invesco|Global X|WisdomTree|VanEck|First Trust|"
    r"Franklin|YieldMax|Roundhill)\b",
    re.IGNORECASE,
)

_EXCLUDED_NAME_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("warrant", re.compile(r"\bWarrants?\b|\bWTS\b|\bWT\b", re.IGNORECASE)),
    ("right", re.compile(r"\bRights?\b", re.IGNORECASE)),
    ("unit", re.compile(r"\bUnits?\b", re.IGNORECASE)),
)

_PREFERRED_PATTERN = re.compile(
    r"\b(Preferred|Preference|Non-Cumulative|Cumulative Redeemable)\b|"
    r"\bDepositary Shares\b.*\b(Interest|Preferred|Series)\b",
    re.IGNORECASE,
)
_DEBT_PATTERN = re.compile(
    r"\b(Notes? due|Senior Notes?|Subordinated Notes?|Junior Subordinated|Debentures?)\b",
    re.IGNORECASE,
)
_EXPLICIT_SUFFIX_PATTERN = re.compile(r"(?:-|\.)(?:W|WS|WT|R|U)$", re.IGNORECASE)


def normalize_us_symbol_key(symbol: str) -> str:
    """Normalize local/Yahoo US symbols to the project's manifest key form."""
    normalized = str(symbol or "").strip().upper().replace(".", "-")
    if normalized.endswith("_DL"):
        normalized = normalized[: -len("_DL")]
    return normalized


def us_broker_untradable_reason(symbol: str, name: str, market: str = "") -> str | None:
    """Return an exclusion reason for listed US tools outside the normal broker-tradable pool.

    Delisted/archive status is deliberately ignored: historical common stocks,
    ADRs, ETFs, and REITs are kept to avoid survivorship bias. This filter only
    removes security *types* that are not part of the normal stock/ETF universe
    for this project.
    """
    del market  # Market tells us active vs archive, not security type.
    normalized_symbol = normalize_us_symbol_key(symbol)
    normalized_name = " ".join(str(name or "").strip().split())
    text = normalized_name or normalized_symbol

    for reason, pattern in _EXCLUDED_NAME_PATTERNS:
        if pattern.search(text):
            return reason

    if _EXPLICIT_SUFFIX_PATTERN.search(normalized_symbol):
        return "symbol_suffix"

    # ADRs/ADSs are ordinary broker-tradable US listings even when the
    # underlying foreign share class name contains "preferred".
    is_adr = bool(_AMERICAN_DEPOSITARY_PATTERN.search(text))
    is_fund_like = bool(_FUND_LIKE_PATTERN.search(text))

    if not is_adr and _PREFERRED_PATTERN.search(text):
        return "preferred"
    if not is_fund_like and _DEBT_PATTERN.search(text):
        return "debt"
    return None


def is_us_broker_tradable_security(symbol: str, name: str, market: str = "") -> bool:
    return us_broker_untradable_reason(symbol, name, market) is None
