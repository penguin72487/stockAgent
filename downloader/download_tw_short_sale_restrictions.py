from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
import json
import re
from pathlib import Path
import sys
import threading
import time
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import polars as pl
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stockagent.data.tw_delisting_rules import (
    _company_name_keys,
    classify_delisting_notice,
    extract_announcement_symbols,
    extract_stock_symbols,
    is_relevant_announcement_subject,
    split_announcement_symbols,
)
from stockagent.data.tw_security import classify_tw_stock_or_etf
from downloader.common import SharedRateLimiter, describe_rate_limit, resolve_request_interval


TWSE_SEARCH = "https://dsp.twse.com.tw/official/search"
TWSE_RESULT = "https://dsp.twse.com.tw/official/result"
TWSE_LEGACY_SEARCH = "https://dsp.twse.com.tw/announcement/official"
TWSE_LEGACY_RESULT = "https://dsp.twse.com.tw/announcement/officialResult"
TPEX_SEARCH = "https://dsp.tpex.org.tw/web/announcement/announcement.php"
TPEX_SHORT_TERM = "https://www.tpex.org.tw/openapi/v1/tpex_margin_trading_term"
USER_AGENT = "Mozilla/5.0 stockAgent/1.0"
_DATE_TOKEN = (
    r"(?<!\d)(?:"
    r"(?:民國\s*)?(?:本\s*)?[（(]?\d{2,4}[）)]?\s*[年/\-.]\s*"
    r"\d{1,2}\s*[月/\-.]\s*\d{1,2}\s*日?"
    r"|本\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日?"
    r")(?!\d)"
)
_TPEX_ARCHIVE_FIRST_MONTH = (2002, 11)
_TWSE_ARCHIVE_FIRST_MONTH = (2018, 1)
_TWSE_CURRENT_FIRST_MONTH = (2020, 10)
_LABELED_SECURITY_CODE_RE = re.compile(
    r"(?:上市|上櫃|興櫃)?(?:普通股|股票|證券|公司|有價證券)?\s*"
    r"(?:股票|證券)?\s*(?:代\s*(?:號|碼)|編\s*號)\s*[：:=#、，\s（(]*"
    r"(?P<symbol>[0-9]{4,6}[A-Z]?)",
    re.I,
)


@dataclass(slots=True)
class _AnnouncementDownloadBatch:
    records: list[dict] = field(default_factory=list)
    filtered_irrelevant: int = 0
    unparseable: int = 0
    unresolved_records: list[dict] = field(default_factory=list)
    detail_failure_urls: list[str] = field(default_factory=list)
    detail_subject_backfills: int = 0
    detail_content_unavailable: int = 0
    retry_count: int = 0


@dataclass(frozen=True, slots=True)
class _AnnouncementDetail:
    subject: str
    body_text: str


class _RetryableArchiveError(RuntimeError):
    """A transient archive response whose complete month should be retried."""


_MARKET_REQUEST_LOCKS = {
    "tpex": threading.Lock(),
    "twse": threading.Lock(),
}
_RATE_LIMITER: SharedRateLimiter | None = None
_RATE_LIMITER_LOCK = threading.Lock()


def _global_tw_public_rate_limiter() -> SharedRateLimiter:
    global _RATE_LIMITER
    if _RATE_LIMITER is None:
        with _RATE_LIMITER_LOCK:
            if _RATE_LIMITER is None:
                interval = resolve_request_interval("tw_public", None)
                _RATE_LIMITER = SharedRateLimiter(interval, name="tw_public")
    return _RATE_LIMITER


def _rate_limited_call(call, *args, **kwargs) -> requests.Response:
    limiter = _global_tw_public_rate_limiter()
    limiter.wait()
    response = call(*args, **kwargs)
    if int(getattr(response, "status_code", 200)) in {403, 408, 429, 500, 502, 503, 504}:
        retry_after = str(getattr(response, "headers", {}).get("Retry-After", "")).strip()
        try:
            delay = min(60.0, max(0.0, float(retry_after))) if retry_after else 1.0
        except ValueError:
            delay = 1.0
        limiter.defer(delay)
    return response


def _coerce_announcement_batch(
    value: _AnnouncementDownloadBatch | list[dict],
) -> _AnnouncementDownloadBatch:
    """Accept legacy list-returning test/adaptor implementations safely."""

    if isinstance(value, _AnnouncementDownloadBatch):
        return value
    records: list[dict] = []
    unparseable = 0
    detail_failure_urls: list[str] = []
    for record in value:
        if "DETAIL_FETCH_ERROR:" in str(record.get("body_text", "") or ""):
            detail_failure_urls.append(str(record.get("source_url", "unknown URL")))
        if split_announcement_symbols(str(record.get("symbols", "") or "")):
            records.append(record)
        else:
            unparseable += 1
    return _AnnouncementDownloadBatch(
        records=records,
        unparseable=unparseable,
        unresolved_records=[
            record
            for record in value
            if not split_announcement_symbols(str(record.get("symbols", "") or ""))
        ],
        detail_failure_urls=detail_failure_urls,
    )


CompanySymbolLookup = dict[tuple[str, str], set[str]]


def _has_only_explicit_unsupported_security_codes(text: str) -> bool:
    """Return whether a notice explicitly identifies only excluded products.

    The official archives also contain six-digit historical TDRs, 01-series
    REIT beneficiary securities, warrants, and other products that are outside
    the canonical stock/ETF execution universe.  An issuer-name lookup must not
    silently remap one of those labeled codes to a different four-digit stock.
    """

    labeled_codes = {
        match.group("symbol").upper()
        for match in _LABELED_SECURITY_CODE_RE.finditer(str(text or "").upper())
    }
    return bool(
        labeled_codes
        and all(classify_tw_stock_or_etf(symbol) is None for symbol in labeled_codes)
    )


def _announcement_report_stub(record: dict) -> dict[str, str]:
    """Keep unresolved/excluded evidence auditable without copying full bodies."""

    return {
        name: str(record.get(name, "") or "")
        for name in (
            "announcement_date",
            "market",
            "document_number",
            "subject",
            "source_url",
        )
    }


def _canonical_company_alias(value: str) -> str:
    alias = str(value or "")
    if "公告有關" in alias:
        alias = alias.rsplit("公告有關", 1)[-1]
    alias = re.sub(r"^主旨", "", alias)
    alias = re.sub(r"^(?:以下簡稱|下稱)", "", alias)
    return alias


def _has_likely_mojibake(value: str) -> bool:
    """Reject aliases whose decoded text is not trustworthy identity evidence."""

    normalized = str(value or "")
    return "\ufffd" in normalized or "嚙" in normalized


def _add_company_symbol_aliases(
    lookup: CompanySymbolLookup,
    *,
    market: str,
    text: str,
    symbols: list[str],
) -> None:
    normalized_market = str(market).strip().lower()
    normalized_symbols = {
        symbol.upper()
        for symbol in symbols
        if classify_tw_stock_or_etf(str(symbol).upper()) is not None
    }
    if not normalized_market or not normalized_symbols:
        return
    raw_text = str(text or "").strip()
    if _has_likely_mojibake(raw_text):
        return
    raw_aliases = _company_name_keys(raw_text)
    if not raw_aliases:
        fallback_alias = re.sub(
            r"(?:股份有限公司|有限公司|公司)$",
            "",
            raw_text,
        )
        fallback_alias = re.sub(
            r"[^A-Z0-9\u3400-\u9fff]",
            "",
            fallback_alias.upper(),
        )
        if fallback_alias:
            raw_aliases = [fallback_alias]
    for raw_alias in raw_aliases:
        alias = _canonical_company_alias(raw_alias)
        if len(alias) >= 2:
            lookup.setdefault((normalized_market, alias), set()).update(normalized_symbols)


def _load_company_symbol_lookup(output_dir: Path) -> CompanySymbolLookup:
    lookup: CompanySymbolLookup = {}
    for market in ("twse", "tpex"):
        path = output_dir / f"{market}_delisted_company.parquet"
        if not path.exists():
            continue
        frame = pl.read_parquet(path)
        if not {"symbol", "company_name"} <= set(frame.columns):
            continue
        for row in frame.select(["symbol", "company_name"]).iter_rows(named=True):
            _add_company_symbol_aliases(
                lookup,
                market=market,
                text=str(row.get("company_name", "") or ""),
                symbols=[str(row.get("symbol", "") or "")],
            )

    # Current company tables and historical official quote names close the
    # identity gap for venue transfers and older issuers that are absent from
    # the current delisted-company endpoint.  Every mapping remains tied to an
    # official parquet row and the canonical stock/ETF classifier.
    source_specs = (
        (
            "twse_listed_company_basic.parquet",
            "twse",
            "公司代號",
            ("公司名稱", "公司簡稱"),
        ),
        (
            "tpex_basic_company.parquet",
            "tpex",
            "SecuritiesCompanyCode",
            ("CompanyName", "CompanyAbbreviation"),
        ),
        (
            "twse_daily_ohlcv.parquet",
            "twse",
            "證券代號",
            ("證券名稱",),
        ),
        (
            "tpex_daily_ohlcv.parquet",
            "tpex",
            "代號",
            ("名稱",),
        ),
    )
    for filename, market, symbol_column, name_columns in source_specs:
        source_path = output_dir / filename
        if not source_path.is_file():
            continue
        schema = set(pl.read_parquet_schema(source_path).names())
        available_names = [column for column in name_columns if column in schema]
        if symbol_column not in schema or not available_names:
            continue
        name_status_column = (
            "_name_decode_status"
            if filename == "tpex_daily_ohlcv.parquet"
            and "_name_decode_status" in schema
            else None
        )
        scan = pl.scan_parquet(source_path)
        if name_status_column is not None:
            scan = scan.filter(
                pl.col(name_status_column)
                .cast(pl.String, strict=False)
                .fill_null("")
                .str.strip_chars()
                .eq("")
            )
        frame = (
            scan.select(
                pl.col(symbol_column).cast(pl.String, strict=False).alias("symbol"),
                *[
                    pl.col(column).cast(pl.String, strict=False).alias(column)
                    for column in available_names
                ],
            )
            .drop_nulls("symbol")
            .unique()
            .collect()
        )
        for row in frame.iter_rows(named=True):
            symbol = str(row.get("symbol", "") or "").strip().upper()
            for name_column in available_names:
                _add_company_symbol_aliases(
                    lookup,
                    market=market,
                    text=str(row.get(name_column, "") or ""),
                    symbols=[symbol],
                )
    existing_path = output_dir / "tw_delisting_short_sale_announcements.parquet"
    if existing_path.exists():
        existing = pl.read_parquet(existing_path)
        required = {"market", "symbols", "subject"}
        if required <= set(existing.columns):
            selected = sorted(required | ({"body_text"} & set(existing.columns)))
            for row in existing.select(selected).iter_rows(named=True):
                text = " ".join(
                    (
                        f"{row.get('subject', '') or ''} "
                        f"{row.get('body_text', '') or ''}"
                    ).split()
                )
                if _has_only_explicit_unsupported_security_codes(text):
                    continue
                _add_company_symbol_aliases(
                    lookup,
                    market=str(row.get("market", "") or ""),
                    text=str(row.get("subject", "") or ""),
                    symbols=split_announcement_symbols(str(row.get("symbols", "") or "")),
                )
    return lookup


def _resolve_unparsed_announcement(
    record: dict,
    lookup: CompanySymbolLookup,
) -> str:
    market = str(record.get("market", "") or "").strip().lower()
    subject = str(record.get("subject", "") or "")
    body = str(record.get("body_text", "") or "")
    if _has_only_explicit_unsupported_security_codes(f"{subject} {body}"):
        return "out_of_universe"
    if "到期日" in subject and "最後交易日" in subject:
        return "non_equity"
    aliases = [
        _canonical_company_alias(alias)
        for alias in _company_name_keys(subject)
    ]
    scores: dict[str, int] = {}
    for alias in aliases:
        for (candidate_market, known_alias), symbols in lookup.items():
            if alias == known_alias:
                score = 4 if candidate_market == market else 3
            elif len(alias) >= 2 and len(known_alias) >= 2 and (
                alias in known_alias or known_alias in alias
            ):
                score = 2 if candidate_market == market else 1
            else:
                continue
            for symbol in symbols:
                scores[symbol] = max(score, scores.get(symbol, 0))
    best_symbols: set[str] = set()
    if scores:
        best_score = max(scores.values())
        best_symbols = {symbol for symbol, score in scores.items() if score == best_score}
    if len(best_symbols) == 1:
        symbol = next(iter(best_symbols))
        record["symbols"] = symbol
        return "resolved"
    if "興櫃" in subject:
        # The canonical execution universe starts at TWSE/TPEx listing.  An
        # unresolved notice that explicitly concerns only emerging-market
        # trading cannot affect its buy/sell/short masks.  Keep it accounted as
        # out-of-universe rather than pretending the issuer code was parsed.
        return "out_of_universe"
    return "unparseable"


def _roc_date(text: str, *, default_year: int | None = None) -> str | None:
    normalized = re.sub(r"[（(]\s*(\d{2,4})\s*[）)]", r"\1", str(text or ""))
    match = re.search(
        r"(?<!\d)(?:民國\s*)?(?:本\s*)?(\d{2,4})\s*[年/\-.]\s*"
        r"(\d{1,2})\s*[月/\-.]\s*(\d{1,2})日?",
        normalized,
    )
    if match:
        try:
            year = int(match.group(1))
            if year < 1911:
                year += 1911
            return date(year, int(match.group(2)), int(match.group(3))).isoformat()
        except ValueError:
            pass
    if default_year is not None:
        current_year = re.search(
            r"本\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日?",
            normalized,
        )
        if current_year:
            try:
                return date(
                    int(default_year),
                    int(current_year.group(1)),
                    int(current_year.group(2)),
                ).isoformat()
            except ValueError:
                pass
    digits = re.sub(r"\D", "", normalized)
    if len(digits) in {6, 7, 8}:
        year_digits = {6: 2, 7: 3, 8: 4}[len(digits)]
        year = int(digits[:year_digits])
        if year_digits < 4:
            year += 1911
        try:
            return date(year, int(digits[year_digits : year_digits + 2]), int(digits[year_digits + 2 :])).isoformat()
        except ValueError:
            pass
    return None


def _event_date(
    text: str,
    phrases: tuple[str, ...],
    *,
    before_distance: int = 24,
    after_distance: int = 20,
    default_year: int | None = None,
) -> str | None:
    date_matches = tuple(re.finditer(_DATE_TOKEN, text))
    resolved: list[tuple[int, int, str]] = []
    for phrase in phrases:
        for action in re.finditer(re.escape(phrase), text):
            # Prefer the closest preceding date. Searching matches separately
            # avoids incorrectly binding an earlier date in the same clause to
            # a later action when a closer effective date is present.
            before = [
                match
                for match in date_matches
                if match.end() <= action.start()
                and action.start() - match.end() <= before_distance
                and not re.search(r"[。；;\n]", text[match.end() : action.start()])
            ]
            candidates: list[tuple[re.Match[str], bool]] = [
                (match, True) for match in before[-1:]
            ]
            if not candidates:
                after = [
                    match
                    for match in date_matches
                    if match.start() >= action.end()
                    and match.start() - action.end() <= after_distance
                    and not re.search(r"[。；;\n]", text[action.end() : match.start()])
                ]
                candidates = [(match, False) for match in after[:1]]
            for match, is_before in candidates:
                parsed = _roc_date(match.group(0), default_year=default_year)
                if parsed:
                    gap = action.start() - match.end() if is_before else match.start() - action.end()
                    resolved.append((gap, action.start(), parsed))
            # Some roundup notices state one effective date and then identify a
            # specific stock whose margin trading is suspended 「自同日起」.
            # This is an explicit backward reference, so it is safe to cross the
            # preceding sentence boundary and bind to the nearest earlier date.
            # Do not apply this relaxation without the literal 同日 cue.
            cue = text[max(0, action.start() - 16) : action.start()]
            if re.search(r"(?:自)?同日起?", cue):
                referenced = [
                    match
                    for match in date_matches
                    if match.end() <= action.start()
                    and action.start() - match.end() <= 320
                ]
                if referenced:
                    match = referenced[-1]
                    parsed = _roc_date(match.group(0), default_year=default_year)
                    if parsed:
                        resolved.append(
                            (action.start() - match.end(), action.start(), parsed)
                        )
    return min(resolved)[2] if resolved else None


def _date_immediately_after_label(
    text: str,
    labels: tuple[str, ...],
    *,
    default_year: int | None = None,
) -> str | None:
    """Parse the first date following a field label, never a prior event date."""

    candidates: list[tuple[int, str]] = []
    for label in labels:
        for match in re.finditer(re.escape(label), text):
            tail = text[match.end() : match.end() + 64]
            date_match = re.search(_DATE_TOKEN, tail)
            if date_match is None:
                continue
            prefix = tail[: date_match.start()]
            if re.search(r"[。；;\n]", prefix):
                continue
            parsed = _roc_date(date_match.group(0), default_year=default_year)
            if parsed:
                candidates.append((match.start(), parsed))
    return min(candidates)[1] if candidates else None


def _symbols(text: str) -> list[str]:
    return extract_stock_symbols(text)


def _announcement_fields(
    text: str,
    *,
    subject: str = "",
    body: str = "",
    announcement_date: str | None = None,
) -> dict:
    rules = classify_delisting_notice(text)
    symbols = extract_announcement_symbols(subject, body or text)
    try:
        default_year = (
            date.fromisoformat(str(announcement_date)).year
            if announcement_date
            else None
        )
    except ValueError:
        default_year = None
    parsed_cover_deadline = _event_date(
        text,
        ("償還或還券了結", "還券了結", "償還或還券", "最後回補"),
        default_year=default_year,
    )
    stop_transfer_cover_required = bool(
        re.search(
            r"融券餘額[^。；;\n]{0,80}停止過戶(?:日前?)?"
            r"第?\s*(?:6|六)\s*個?\s*營業日(?:[（(]含[）)])?前"
            r"[^。；;\n]{0,40}還券了結",
            text,
        )
    )
    short_cover_anchor_date = (
        _date_immediately_after_label(
            text,
            (
                "停止股東名簿記載變更起迄日期",
                "停止過戶開始日期",
                "停止過戶日期",
            ),
            default_year=default_year,
        )
        if stop_transfer_cover_required
        else None
    )
    short_open_ban_date = (
        None
        if rules.restriction_cancelled
        else _event_date(
            text,
            (
                "暫停融資融券",
                "暫停普通股融資融券",
                "暫停其普通股融資融券",
                "暫停融券",
                "停止融券",
            ),
            default_year=default_year,
        )
    )
    # 「繼續暫停」describes state as of publication even when the notice has
    # no new effective date.  Treat publication as the earliest knowable date.
    if (
        short_open_ban_date is None
        and rules.continues_short_open_ban
        and announcement_date
    ):
        short_open_ban_date = str(announcement_date)
    return {
        "symbols": ",".join(symbols),
        "short_open_ban_date": short_open_ban_date,
        "short_open_resume_date": _event_date(
            text,
            ("恢復融資融券", "恢復普通方法交割與融資融券", "恢復融券"),
            before_distance=48,
            default_year=default_year,
        ),
        # A negated Article 78 clause may contain the same date/cover words as a
        # positive deadline. Do not turn that sentence into an execution rule.
        "short_cover_deadline": None if rules.article_78_exempt else parsed_cover_deadline,
        "short_cover_lead_trading_days": 10 if rules.requires_relative_cover else None,
        "short_cover_anchor_date": short_cover_anchor_date,
        "short_cover_anchor_lead_trading_days": (
            6 if stop_transfer_cover_required and short_cover_anchor_date else None
        ),
        "trading_suspension_date": _event_date(
            text,
            ("停止買賣", "停止櫃檯買賣", "暫停交易"),
            default_year=default_year,
        ),
        "delisting_date": _event_date(
            text,
            ("終止上市", "終止上櫃", "終止櫃檯買賣", "終止在證券商營業處所買賣"),
            default_year=default_year,
        ),
        "closing_only": bool(re.search(r"了結(?:交易)?(?:不受限|不在此限)", text)),
        "merger_or_share_exchange": bool(re.search(r"合併|股份轉換|換股", text)),
        "article_78_exempt": rules.article_78_exempt,
        "technical_share_replacement": rules.technical_share_replacement,
        "delisting_cancelled": rules.delisting_cancelled,
        "restriction_cancelled": rules.restriction_cancelled,
        "continues_short_open_ban": rules.continues_short_open_ban,
        "emerging_to_listed_transition": rules.emerging_to_listed_transition,
    }


def _announcement_record(*, market: str, issued_date: str, number: str, subject: str, body: str, url: str) -> dict:
    text = " ".join(f"{subject} {body}".split())
    return {
        "announcement_date": issued_date,
        "market": market,
        "document_number": number,
        "subject": subject,
        **_announcement_fields(
            text,
            subject=subject,
            body=body,
            announcement_date=issued_date,
        ),
        "source_url": url,
        "body_text": text,
        "_downloaded_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }


def _repair_announcement_record(record: dict) -> dict:
    """Reparse cached notices whenever parser rules improve.

    Downloading is append/update based, so without this pass old rows would keep
    stale empty symbols and dates forever.
    """
    repaired = dict(record)
    subject = str(record.get("subject", "") or "")
    body = str(record.get("body_text", "") or "")
    text = " ".join(f"{subject} {body}".split())
    parsed = _announcement_fields(
        text,
        subject=subject,
        body=body,
        announcement_date=str(record.get("announcement_date", "") or "") or None,
    )
    # Always replace symbols when the subject is parseable.  A symbol-less
    # correction may retain a previously name-resolved code, but only when all
    # retained values still belong to the canonical universe.  Explicitly
    # labeled excluded products must never survive as stale six-digit symbols
    # or as an issuer-name remap to an unrelated four-digit stock.
    previous_symbols = split_announcement_symbols(
        str(repaired.get("symbols", "") or "")
    )
    previous_symbols_supported = bool(
        previous_symbols
        and all(
            classify_tw_stock_or_etf(symbol) is not None
            for symbol in previous_symbols
        )
    )
    if parsed["symbols"]:
        repaired["symbols"] = parsed["symbols"]
    elif _has_only_explicit_unsupported_security_codes(text):
        repaired["symbols"] = ""
    elif previous_symbols_supported:
        repaired["symbols"] = ",".join(previous_symbols)
    else:
        repaired["symbols"] = ""
    for name in (
        "short_open_ban_date",
        "short_open_resume_date",
        "short_cover_deadline",
        "short_cover_anchor_date",
        "trading_suspension_date",
        "delisting_date",
    ):
        if parsed[name] is not None:
            repaired[name] = parsed[name]
        elif name == "short_cover_deadline" and parsed["article_78_exempt"]:
            # Remove an old false deadline parsed out of a negated exemption.
            repaired[name] = None
        elif name == "short_open_ban_date" and parsed["restriction_cancelled"]:
            repaired[name] = None
    repaired["short_cover_lead_trading_days"] = parsed["short_cover_lead_trading_days"]
    repaired["short_cover_anchor_lead_trading_days"] = parsed[
        "short_cover_anchor_lead_trading_days"
    ]
    repaired["closing_only"] = parsed["closing_only"]
    repaired["merger_or_share_exchange"] = parsed["merger_or_share_exchange"]
    repaired["article_78_exempt"] = parsed["article_78_exempt"]
    repaired["technical_share_replacement"] = parsed["technical_share_replacement"]
    repaired["delisting_cancelled"] = parsed["delisting_cancelled"]
    repaired["restriction_cancelled"] = parsed["restriction_cancelled"]
    repaired["continues_short_open_ban"] = parsed["continues_short_open_ban"]
    repaired["emerging_to_listed_transition"] = parsed[
        "emerging_to_listed_transition"
    ]
    return repaired


def _detail(session: requests.Session, url: str, timeout: int) -> str:
    response = _rate_limited_call(session.get, url, timeout=timeout)
    response.raise_for_status()
    return BeautifulSoup(response.content, "html.parser").get_text(" ", strip=True)


def _tpex_announcement_detail(content: bytes) -> _AnnouncementDetail:
    """Extract only the TPEx announcement document, excluding site chrome.

    Older TPEx monthly result tables leave the subject cell empty even though
    the linked detail page still contains the authoritative subject and rule
    text.  Parsing the whole HTML page is unsafe because navigation copy also
    contains lifecycle phrases and can change rule classification.
    """

    soup = BeautifulSoup(content, "html.parser")
    wrapper = soup.select_one(".page-wrapper")
    if wrapper is None:
        # Test adapters and old provider responses may return the document
        # fragment without the current page wrapper.  The fragment itself is
        # still the narrowest available content container.
        wrapper = soup

    document_tables = wrapper.find_all("table")
    document_parts: list[str] = []
    subject_parts: list[str] = []
    for table in document_tables:
        table_text = " ".join(table.get_text(" ", strip=True).split())
        if table_text:
            document_parts.append(table_text)
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"], recursive=False)
            if len(cells) < 2:
                continue
            label = re.sub(r"\s+", "", cells[0].get_text(" ", strip=True))
            if label.rstrip("：:") != "主旨":
                continue
            subject_parts.extend(
                value
                for value in (
                    " ".join(cell.get_text(" ", strip=True).split())
                    for cell in cells[1:]
                )
                if value
            )

    subject = " ".join(subject_parts).strip()
    body_text = " ".join(document_parts).strip()
    if not document_tables:
        body_text = " ".join(wrapper.get_text(" ", strip=True).split())
    return _AnnouncementDetail(subject=subject, body_text=body_text)


def _download_tpex_detail(
    session: requests.Session,
    url: str,
    timeout: int,
) -> _AnnouncementDetail:
    response = _rate_limited_call(session.get, url, timeout=timeout)
    response.raise_for_status()
    return _tpex_announcement_detail(response.content)


def _download_tpex_month(
    year: int,
    month: int,
    timeout: int,
) -> _AnnouncementDownloadBatch:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    response = _rate_limited_call(
        session.post,
        TPEX_SEARCH,
        data={"inputY": str(year), "inputM": str(month), "inputD": "00", "inputType": "4", "inputKeyword": ""},
        timeout=timeout,
    )
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    batch = _AnnouncementDownloadBatch()
    for row in soup.select("table tbody tr"):
        cells = row.find_all("td")
        if len(cells) < 5:
            continue
        issued = _roc_date(cells[1].get_text(" ", strip=True))
        if issued is not None:
            issued_date = date.fromisoformat(issued)
            if (issued_date.year, issued_date.month) != (int(year), int(month)):
                raise RuntimeError(
                    "TPEx archive normalized or returned a different month: "
                    f"requested={year:04d}-{month:02d} returned={issued_date:%Y-%m}"
                )
        link = cells[4].find("a", href=True)
        if not issued or link is None:
            continue
        url = urljoin(TPEX_SEARCH, link["href"])
        listing_subject = cells[3].get_text(" ", strip=True)
        if listing_subject and not is_relevant_announcement_subject(listing_subject):
            batch.filtered_irrelevant += 1
            continue
        try:
            detail = _download_tpex_detail(session, url, timeout)
        except Exception as exc:
            detail = _AnnouncementDetail(
                subject="",
                body_text=f"DETAIL_FETCH_ERROR: {exc}",
            )
            batch.detail_failure_urls.append(url)
        subject = listing_subject or detail.subject
        if not listing_subject:
            if not detail.subject:
                # Do not label an empty historical detail shell as irrelevant:
                # it is an unresolved source-coverage fact that strict reports
                # and audits must retain.
                batch.unparseable += 1
                batch.detail_content_unavailable += 1
                continue
            batch.detail_subject_backfills += 1
            if not is_relevant_announcement_subject(subject):
                batch.filtered_irrelevant += 1
                continue
        record = _announcement_record(
            market="tpex",
            issued_date=issued,
            number=cells[2].get_text(" ", strip=True),
            subject=subject,
            body=detail.body_text,
            url=url,
        )
        if not split_announcement_symbols(record["symbols"]):
            batch.unparseable += 1
            batch.unresolved_records.append(record)
            continue
        batch.records.append(record)
    return batch


def _twse_current_detail(
    session: requests.Session,
    row: BeautifulSoup,
    *,
    result_url: str,
    timeout: int,
) -> tuple[str, str]:
    form = row.select_one("form[action]")
    if form is not None:
        url = urljoin(result_url, str(form.get("action", "")))
        data = {
            str(item.get("name")): str(item.get("value", ""))
            for item in form.select("input[name]")
        }
        response = _rate_limited_call(session.post, url, data=data, timeout=timeout)
        response.raise_for_status()
        return BeautifulSoup(response.content, "html.parser").get_text(" ", strip=True), url

    link = next(
        (
            item
            for item in row.find_all("a", href=True)
            if not str(item.get("href", "")).startswith("javascript:")
            and "/public/static/downloads/" not in str(item.get("href", ""))
        ),
        None,
    )
    if link is None:
        raise RuntimeError("TWSE result row did not provide a detail form")
    url = urljoin(result_url, str(link["href"]))
    return _detail(session, url, timeout), url


def _download_twse_current_month(
    year: int,
    month: int,
    timeout: int,
) -> _AnnouncementDownloadBatch:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    search = _rate_limited_call(session.get, TWSE_SEARCH, timeout=timeout)
    search.raise_for_status()
    soup = BeautifulSoup(search.content, "html.parser")
    form = soup.select_one("#form_category")
    token = None if form is None else form.select_one('input[name="SYNCHRONIZER_TOKEN"]')
    if token is None:
        raise RuntimeError("TWSE category search did not provide SYNCHRONIZER_TOKEN")
    response = _rate_limited_call(
        session.post,
        TWSE_RESULT,
        data={
            "SYNCHRONIZER_TOKEN": token.get("value", ""),
            "SYNCHRONIZER_URI": "/official/search",
            "queryby": "category",
            "startDate": f"{year:04d}{month:02d}01",
            "kind": "s",
        },
        timeout=timeout,
    )
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    batch = _AnnouncementDownloadBatch()
    for row in soup.select("tbody tr"):
        cells = row.find_all("td")
        if len(cells) < 3:
            continue
        row_text = row.get_text(" ", strip=True)
        issued = _roc_date(row_text)
        if issued is not None:
            issued_date = date.fromisoformat(issued)
            if (issued_date.year, issued_date.month) != (int(year), int(month)):
                raise RuntimeError(
                    "TWSE current archive returned a different month: "
                    f"requested={year:04d}-{month:02d} returned={issued_date:%Y-%m}"
                )
        if not issued:
            continue
        subject = cells[-2].get_text(" ", strip=True) if len(cells) >= 4 else row_text
        if not is_relevant_announcement_subject(subject):
            batch.filtered_irrelevant += 1
            continue
        try:
            body, url = _twse_current_detail(
                session,
                row,
                result_url=TWSE_RESULT,
                timeout=timeout,
            )
        except Exception as exc:
            form = row.select_one("form[action]")
            url = (
                urljoin(TWSE_RESULT, str(form.get("action", "")))
                if form is not None
                else TWSE_RESULT
            )
            body = f"DETAIL_FETCH_ERROR: {exc}"
            batch.detail_failure_urls.append(url)
        record = _announcement_record(
            market="twse",
            issued_date=issued,
            number=(
                cells[3].get_text(" ", strip=True)
                if len(cells) >= 5
                else cells[1].get_text(" ", strip=True)
            ),
            subject=subject,
            body=body,
            url=url,
        )
        if not split_announcement_symbols(record["symbols"]):
            batch.unparseable += 1
            batch.unresolved_records.append(record)
            continue
        batch.records.append(record)
    return batch


def _download_twse_legacy_month(
    year: int,
    month: int,
    timeout: int,
) -> _AnnouncementDownloadBatch:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    search = _rate_limited_call(session.get, TWSE_LEGACY_SEARCH, timeout=timeout)
    search.raise_for_status()
    soup = BeautifulSoup(search.content, "html.parser")
    form = soup.select_one("#form_category")
    token = None if form is None else form.select_one('input[name="SYNCHRONIZER_TOKEN"]')
    if token is None:
        raise RuntimeError("TWSE legacy category search did not provide SYNCHRONIZER_TOKEN")
    response = _rate_limited_call(
        session.post,
        TWSE_LEGACY_RESULT,
        data={
            "SYNCHRONIZER_TOKEN": token.get("value", ""),
            "SYNCHRONIZER_URI": "/announcement/official",
            "queryby": "category",
            "startDate": f"{year:04d}{month:02d}01",
            "kind": "s",
        },
        timeout=timeout,
    )
    response.raise_for_status()
    content_type = str(getattr(response, "headers", {}).get("Content-Type", ""))
    text = response.content.decode("utf-8", errors="replace")
    if "text/html" in content_type.lower() or re.search(r"<html\b", text, re.I):
        raise RuntimeError("TWSE legacy archive returned HTML instead of announcement text")

    batch = _AnnouncementDownloadBatch()
    blocks = [
        block.strip()
        for block in re.split(r"(?:\r?\n)?={20,}(?:\r?\n)?", text)
        if block.strip()
    ]
    for block in blocks:
        subject_match = re.search(
            r"主旨[：:]\s*(.*?)(?=\r?\n\s*(?:說明|正本|副本)[：:]|\Z)",
            block,
            re.S,
        )
        subject = " ".join(subject_match.group(1).split()) if subject_match else ""
        issued_match = re.search(r"發文日期[：:]\s*([^\r\n]+)", block)
        issued = _roc_date(issued_match.group(1)) if issued_match else None
        if issued is not None:
            issued_date = date.fromisoformat(issued)
            if (issued_date.year, issued_date.month) != (int(year), int(month)):
                raise RuntimeError(
                    "TWSE legacy archive returned a different month: "
                    f"requested={year:04d}-{month:02d} returned={issued_date:%Y-%m}"
                )
        if not is_relevant_announcement_subject(subject):
            batch.filtered_irrelevant += 1
            continue
        number_match = re.search(r"發文字號[：:]\s*([^\r\n]+)", block)
        number = " ".join(number_match.group(1).split()) if number_match else ""
        if not issued or not number:
            batch.unparseable += 1
            continue
        url = f"{TWSE_LEGACY_RESULT}#{number}"
        record = _announcement_record(
            market="twse",
            issued_date=issued,
            number=number,
            subject=subject,
            body=block,
            url=url,
        )
        if not split_announcement_symbols(record["symbols"]):
            batch.unparseable += 1
            batch.unresolved_records.append(record)
            continue
        batch.records.append(record)
    return batch


def _download_twse_month(
    year: int,
    month: int,
    timeout: int,
) -> _AnnouncementDownloadBatch:
    requested = (int(year), int(month))
    if requested < _TWSE_ARCHIVE_FIRST_MONTH:
        raise ValueError(
            "TWSE official archive is unavailable before 2018-01: "
            f"requested={year:04d}-{month:02d}"
        )
    if requested < _TWSE_CURRENT_FIRST_MONTH:
        return _download_twse_legacy_month(year, month, timeout)
    return _download_twse_current_month(year, month, timeout)


def _download_twse_keyword(timeout: int) -> _AnnouncementDownloadBatch:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    search = _rate_limited_call(session.get, TWSE_SEARCH, timeout=timeout)
    search.raise_for_status()
    soup = BeautifulSoup(search.content, "html.parser")
    form = soup.select_one("#form_keyword")
    token = None if form is None else form.select_one('input[name="SYNCHRONIZER_TOKEN"]')
    if token is None:
        raise RuntimeError("TWSE keyword search did not provide SYNCHRONIZER_TOKEN")
    response = _rate_limited_call(
        session.post,
        TWSE_RESULT,
        data={
            "SYNCHRONIZER_TOKEN": token.get("value", ""),
            "SYNCHRONIZER_URI": "/official/search",
            "queryby": "keyword",
            "keyword": "終止上市",
        },
        timeout=max(timeout, 60),
    )
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    batch = _AnnouncementDownloadBatch()
    for row in soup.select("tbody tr"):
        cells = row.find_all("td")
        if len(cells) < 5:
            continue
        issued = _roc_date(cells[1].get_text(" ", strip=True))
        subject = cells[4].get_text(" ", strip=True)
        if not issued:
            continue
        if not is_relevant_announcement_subject(subject):
            batch.filtered_irrelevant += 1
            continue
        try:
            body, url = _twse_current_detail(
                session,
                row,
                result_url=TWSE_RESULT,
                timeout=timeout,
            )
        except Exception as exc:
            form = row.select_one("form[action]")
            url = (
                urljoin(TWSE_RESULT, str(form.get("action", "")))
                if form is not None
                else TWSE_RESULT
            )
            body = f"{subject} DETAIL_FETCH_ERROR: {exc}"
            batch.detail_failure_urls.append(url)
        record = _announcement_record(
            market="twse",
            issued_date=issued,
            number=cells[3].get_text(" ", strip=True),
            subject=subject,
            body=body,
            url=url,
        )
        if not split_announcement_symbols(record["symbols"]):
            batch.unparseable += 1
            batch.unresolved_records.append(record)
            continue
        batch.records.append(record)
    return batch


def _download_tpex_current_terms(timeout: int) -> pl.DataFrame:
    response = _rate_limited_call(
        requests.get,
        TPEX_SHORT_TERM,
        headers={"User-Agent": USER_AGENT},
        timeout=timeout,
    )
    response.raise_for_status()
    rows = response.json()
    if not isinstance(rows, list):
        raise RuntimeError("TPEx current terms response is not a JSON list")
    records = []
    for row in rows:
        records.append({
            "announcement_date": _roc_date(str(row.get("Date", ""))),
            "market": "tpex",
            "symbol": str(row.get("SecuritiesCompanyCode", "")).strip(),
            "company_name": str(row.get("CompanyName", "")).strip(),
            "short_open_ban_date": _roc_date(str(row.get("ShortSaleSuspensionStartDate", ""))),
            "short_open_resume_date": _roc_date(str(row.get("ShortSaleSuspensionEndDate", ""))),
            "reason": str(row.get("Reason", "")).strip(),
            "source_url": TPEX_SHORT_TERM,
        })
    return pl.DataFrame(records, infer_schema_length=None) if records else pl.DataFrame()


def _download_archive_task(
    market: str,
    label: str,
    download,
    *args,
    retries: int,
    retry_backoff: float,
) -> object:
    """Serialize each official host and retry only transient/incomplete responses.

    Both official archive sites reject bursts long before local CPU/network is
    saturated.  The thread pool may still overlap TWSE with TPEx, but requests
    to the same market are deliberately serialized.  A month with failed
    detail pages is retried as a unit so strict mode never mistakes partial HTML
    for a complete archive response.
    """

    market_key = str(market).strip().lower()
    lock = _MARKET_REQUEST_LOCKS[market_key]
    max_attempts = max(1, int(retries) + 1)
    delay_base = max(0.0, float(retry_backoff))
    with lock:
        for attempt in range(max_attempts):
            try:
                result = download(*args)
                if (
                    isinstance(result, _AnnouncementDownloadBatch)
                    and result.detail_failure_urls
                ):
                    raise _RetryableArchiveError(
                        f"{len(result.detail_failure_urls)} detail page(s) failed"
                    )
                if isinstance(result, _AnnouncementDownloadBatch):
                    result.retry_count = attempt
                return result
            except (requests.RequestException, _RetryableArchiveError) as exc:
                if attempt + 1 >= max_attempts:
                    raise
                delay = min(8.0, delay_base * (2**attempt))
                print(
                    f"[tw-short-restrictions] retry {label} "
                    f"attempt={attempt + 2}/{max_attempts} delay={delay:.2f}s "
                    f"error={type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )
                if delay > 0.0:
                    _global_tw_public_rate_limiter().defer(delay)
                    time.sleep(delay)
    raise AssertionError("archive retry loop exited unexpectedly")


def _download_announcement_archive(
    *,
    start_year: int,
    end_year: int,
    workers: int,
    timeout: int,
    retries: int = 4,
    retry_backoff: float = 0.75,
    company_symbol_lookup: CompanySymbolLookup | None = None,
) -> tuple[list[dict], list[str], list[dict[str, object]]]:
    today = date.today()
    months = [
        (year, month)
        for year in range(int(start_year), int(end_year) + 1)
        for month in range(1, 13)
        if date(year, month, 1) <= today
    ]
    records: list[dict] = []
    failures: list[str] = []
    completeness: list[dict[str, object]] = []
    unresolved: list[tuple[dict, dict[str, object]]] = []
    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as executor:
        tasks = {}
        for year, month in months:
            period = f"{year:04d}-{month:02d}"
            if (year, month) < _TPEX_ARCHIVE_FIRST_MONTH:
                completeness.append(
                    {
                        "market": "tpex",
                        "period": period,
                        "source": "monthly",
                        "status": "source_unavailable",
                        "records": 0,
                        "filtered_irrelevant": 0,
                        "unparseable": 0,
                        "detail_failures": 0,
                        "reason": "TPEx official monthly archive begins at 2002-11",
                    }
                )
            else:
                label = f"tpex:{period}:monthly"
                tasks[
                    executor.submit(
                        _download_archive_task,
                        "tpex",
                        label,
                        _download_tpex_month,
                        year,
                        month,
                        timeout,
                        retries=retries,
                        retry_backoff=retry_backoff,
                    )
                ] = {
                    "market": "tpex",
                    "period": period,
                    "source": "monthly",
                }
            if (year, month) < _TWSE_ARCHIVE_FIRST_MONTH:
                completeness.append(
                    {
                        "market": "twse",
                        "period": period,
                        "source": "monthly",
                        "status": "source_unavailable",
                        "records": 0,
                        "filtered_irrelevant": 0,
                        "unparseable": 0,
                        "detail_failures": 0,
                        "reason": "TWSE official archive begins at 2018-01",
                    }
                )
            else:
                twse_source = (
                    "monthly:legacy"
                    if (year, month) < _TWSE_CURRENT_FIRST_MONTH
                    else "monthly:current"
                )
                label = f"twse:{period}:{twse_source}"
                tasks[
                    executor.submit(
                        _download_archive_task,
                        "twse",
                        label,
                        _download_twse_month,
                        year,
                        month,
                        timeout,
                        retries=retries,
                        retry_backoff=retry_backoff,
                    )
                ] = {
                    "market": "twse",
                    "period": period,
                    "source": twse_source,
                }
        tasks[
            executor.submit(
                _download_archive_task,
                "twse",
                "twse:all:keyword",
                _download_twse_keyword,
                timeout,
                retries=retries,
                retry_backoff=retry_backoff,
            )
        ] = {
            "market": "twse",
            "period": "all",
            "source": "keyword:終止上市",
        }

        for future in as_completed(tasks):
            task_info = tasks[future]
            label = f"{task_info['market']}:{task_info['period']}:{task_info['source']}"
            try:
                batch = _coerce_announcement_batch(future.result())
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                failures.append(f"{label}: {error}")
                completeness.append(
                    {
                        **task_info,
                        "status": "error",
                        "records": 0,
                        "filtered_irrelevant": 0,
                        "unparseable": 0,
                        "detail_failures": 0,
                        "error": error,
                    }
                )
                continue
            records.extend(batch.records)
            detail_failure_urls = batch.detail_failure_urls
            for url in detail_failure_urls:
                failures.append(f"{label}: detail fetch failed: {url}")
            entry: dict[str, object] = {
                **task_info,
                "status": "partial" if detail_failure_urls else "ok",
                "records": len(batch.records),
                "raw_rows": (
                    len(batch.records)
                    + batch.filtered_irrelevant
                    + batch.unparseable
                ),
                "filtered_irrelevant": batch.filtered_irrelevant,
                "unparseable": batch.unparseable,
                "detail_failures": len(detail_failure_urls),
                "detail_failure_urls": detail_failure_urls,
                "detail_subject_backfills": int(batch.detail_subject_backfills),
                "detail_content_unavailable": int(batch.detail_content_unavailable),
                "retries": int(batch.retry_count),
            }
            completeness.append(entry)
            unresolved.extend((record, entry) for record in batch.unresolved_records)

    lookup = company_symbol_lookup if company_symbol_lookup is not None else {}
    for record in records:
        _add_company_symbol_aliases(
            lookup,
            market=str(record.get("market", "") or ""),
            text=str(record.get("subject", "") or ""),
            symbols=split_announcement_symbols(str(record.get("symbols", "") or "")),
        )
    for record, entry in unresolved:
        resolution = _resolve_unparsed_announcement(record, lookup)
        if resolution == "resolved":
            records.append(record)
            entry["records"] = int(entry.get("records", 0) or 0) + 1
            entry["unparseable"] = int(entry.get("unparseable", 0) or 0) - 1
            _add_company_symbol_aliases(
                lookup,
                market=str(record.get("market", "") or ""),
                text=str(record.get("subject", "") or ""),
                symbols=split_announcement_symbols(str(record.get("symbols", "") or "")),
            )
        elif resolution == "non_equity":
            entry["filtered_irrelevant"] = int(entry.get("filtered_irrelevant", 0) or 0) + 1
            entry["unparseable"] = int(entry.get("unparseable", 0) or 0) - 1
        elif resolution == "out_of_universe":
            entry["filtered_irrelevant"] = int(
                entry.get("filtered_irrelevant", 0) or 0
            ) + 1
            entry["out_of_universe"] = int(
                entry.get("out_of_universe", 0) or 0
            ) + 1
            entry.setdefault("out_of_universe_records", []).append(
                _announcement_report_stub(record)
            )
            entry["unparseable"] = int(entry.get("unparseable", 0) or 0) - 1
        else:
            entry.setdefault("unresolved_records", []).append(
                _announcement_report_stub(record)
            )
    completeness.sort(
        key=lambda item: (
            str(item.get("market", "")),
            str(item.get("period", "")),
            str(item.get("source", "")),
        )
    )
    return records, failures, completeness


def _write_completeness_report(
    output_dir: Path,
    *,
    start_year: int,
    end_year: int,
    allow_partial: bool,
    entries: list[dict[str, object]],
    failures: list[str],
    data_quality: dict[str, object],
    data_output_written: bool,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "tw_short_sale_download_report.json"
    temporary = path.with_suffix(path.suffix + ".tmp")
    filtered_irrelevant = sum(
        int(entry.get("filtered_irrelevant", 0) or 0) for entry in entries
    )
    unparseable = sum(int(entry.get("unparseable", 0) or 0) for entry in entries)
    detail_subject_backfills = sum(
        int(entry.get("detail_subject_backfills", 0) or 0) for entry in entries
    )
    detail_content_unavailable = sum(
        int(entry.get("detail_content_unavailable", 0) or 0) for entry in entries
    )
    out_of_universe = sum(
        int(entry.get("out_of_universe", 0) or 0) for entry in entries
    )
    source_unavailable_requests = sum(
        entry.get("status") == "source_unavailable" for entry in entries
    )
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "start_year": int(start_year),
        "end_year": int(end_year),
        "allow_partial": bool(allow_partial),
        "requests_complete": not failures,
        "requested_range_available": source_unavailable_requests == 0,
        "coverage_complete": bool(data_quality.get("coverage_complete", False)),
        "archive_cohort_coverage_complete": bool(
            data_quality.get("archive_cohort_coverage_complete", False)
        ),
        "complete": bool(
            not failures
            and source_unavailable_requests == 0
            and data_quality.get("quality_complete", False)
        ),
        "data_output_written": bool(data_output_written),
        "failure_count": len(failures),
        "failures": failures,
        "filtered_irrelevant": filtered_irrelevant,
        "unparseable": unparseable,
        "detail_subject_backfills": detail_subject_backfills,
        "detail_content_unavailable": detail_content_unavailable,
        "out_of_universe": out_of_universe,
        "source_unavailable_requests": source_unavailable_requests,
        "requests": entries,
        "data_quality": data_quality,
    }
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)
    return path


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y"}


def _announcement_data_quality(
    announcements: pl.DataFrame,
    *,
    output_dir: Path,
) -> dict[str, object]:
    rows = int(announcements.height)
    columns = list(announcements.columns)
    schema = {name: str(dtype) for name, dtype in announcements.schema.items()}

    date_min: str | None = None
    date_max: str | None = None
    if rows and "announcement_date" in columns:
        date_values = [
            str(value)
            for value in announcements["announcement_date"].drop_nulls().to_list()
            if str(value).strip()
        ]
        if date_values:
            date_min = min(date_values)
            date_max = max(date_values)

    composite_key = ["market", "document_number", "source_url"]
    if rows and all(name in columns for name in composite_key):
        unique_rows = int(announcements.unique(composite_key, keep="first").height)
        duplicate_groups = int(
            announcements.group_by(composite_key)
            .len()
            .filter(pl.col("len") > 1)
            .height
        )
    else:
        unique_rows = rows
        duplicate_groups = 0
    duplicate_rows = rows - unique_rows

    symbol_rows = 0
    announcement_symbols: set[str] = set()
    delisting_announcement_symbols: set[str] = set()
    delisting_announcement_symbols_by_market: dict[str, set[str]] = {
        "twse": set(),
        "tpex": set(),
    }
    affirmative_cover_count = 0
    article_78_exempt_count = 0
    technical_share_replacement_count = 0
    explicit_deadline_count = 0
    for row in announcements.to_dicts():
        symbols = split_announcement_symbols(str(row.get("symbols", "") or ""))
        if symbols:
            symbol_rows += 1
            announcement_symbols.update(symbols)
        text = f"{row.get('subject', '') or ''} {row.get('body_text', '') or ''}"
        rules = classify_delisting_notice(text)
        article_78_exempt = _truthy(row.get("article_78_exempt")) or rules.article_78_exempt
        technical_replacement = (
            _truthy(row.get("technical_share_replacement"))
            or rules.technical_share_replacement
        )
        if rules.is_delisting_notice and symbols:
            delisting_announcement_symbols.update(symbols)
            market = str(row.get("market", "") or "").strip().lower()
            if market in delisting_announcement_symbols_by_market:
                delisting_announcement_symbols_by_market[market].update(symbols)
        if article_78_exempt:
            article_78_exempt_count += 1
        if technical_replacement:
            technical_share_replacement_count += 1
        lead = row.get("short_cover_lead_trading_days")
        try:
            has_positive_lead = lead is not None and int(lead) > 0
        except (TypeError, ValueError):
            has_positive_lead = False
        if (
            not article_78_exempt
            and not technical_replacement
            and (rules.requires_relative_cover or has_positive_lead)
        ):
            affirmative_cover_count += 1
        if str(row.get("short_cover_deadline", "") or "").strip():
            explicit_deadline_count += 1

    coverage: dict[str, object] = {}
    coverage_complete = True
    archive_cohort_coverage_complete = True
    coverage_specs = {
        "twse_delisted_company": ("twse", date(2018, 1, 1)),
        "tpex_delisted_company": ("tpex", date(2002, 11, 1)),
    }
    for dataset, (market, archive_start) in coverage_specs.items():
        path = output_dir / f"{dataset}.parquet"
        if not path.exists():
            coverage[dataset] = {
                "available": False,
                "market": market,
                "official_symbols": 0,
                "covered_symbols": 0,
                "coverage_rate": 0.0,
                "missing_symbols": [],
                "announcement_archive_available_from": archive_start.isoformat(),
                "archive_limitation": (
                    "full-history coverage includes delistings before the official "
                    "announcement archive is available"
                ),
                "archive_available_cohort": {
                    "official_symbols": 0,
                    "covered_symbols": 0,
                    "coverage_rate": 0.0,
                    "complete": False,
                    "missing_symbol_count": 0,
                    "missing_symbols": [],
                },
            }
            coverage_complete = False
            archive_cohort_coverage_complete = False
            continue
        official = pl.read_parquet(path)
        if "symbol" not in official.columns:
            coverage[dataset] = {
                "available": False,
                "market": market,
                "official_symbols": 0,
                "covered_symbols": 0,
                "coverage_rate": 0.0,
                "missing_symbols": [],
                "error": "missing symbol column",
                "announcement_archive_available_from": archive_start.isoformat(),
                "archive_available_cohort": {
                    "official_symbols": 0,
                    "covered_symbols": 0,
                    "coverage_rate": 0.0,
                    "complete": False,
                    "missing_symbol_count": 0,
                    "missing_symbols": [],
                },
            }
            coverage_complete = False
            archive_cohort_coverage_complete = False
            continue
        official_rows = official.to_dicts()
        official_symbols = {
            str(row.get("symbol", "") or "").strip().upper()
            for row in official_rows
            if str(row.get("symbol", "") or "").strip()
        }
        market_announcement_symbols = delisting_announcement_symbols_by_market[market]
        covered = official_symbols & market_announcement_symbols
        missing = sorted(official_symbols - market_announcement_symbols)
        total = len(official_symbols)
        rate = float(len(covered) / total) if total else 0.0
        dataset_complete = bool(total and len(covered) == total)
        coverage_complete = coverage_complete and dataset_complete

        cohort_symbols: set[str] = set()
        if "date" in official.columns:
            for row in official_rows:
                symbol = str(row.get("symbol", "") or "").strip().upper()
                raw_date = str(row.get("date", "") or "").strip()[:10]
                if not symbol or not raw_date:
                    continue
                try:
                    official_date = date.fromisoformat(raw_date)
                except ValueError:
                    continue
                if official_date >= archive_start:
                    cohort_symbols.add(symbol)
        cohort_covered = cohort_symbols & market_announcement_symbols
        cohort_missing = sorted(cohort_symbols - market_announcement_symbols)
        cohort_total = len(cohort_symbols)
        cohort_rate = (
            float(len(cohort_covered) / cohort_total) if cohort_total else 0.0
        )
        cohort_complete = bool(
            cohort_total and len(cohort_covered) == cohort_total
        )
        archive_cohort_coverage_complete = (
            archive_cohort_coverage_complete and cohort_complete
        )
        coverage[dataset] = {
            "available": True,
            "market": market,
            "official_symbols": total,
            "covered_symbols": len(covered),
            "coverage_rate": rate,
            "complete": dataset_complete,
            "missing_symbol_count": len(missing),
            "missing_symbols": missing,
            "announcement_archive_available_from": archive_start.isoformat(),
            "archive_limitation": (
                "full-history coverage includes delistings before the official "
                "announcement archive is available"
            ),
            "archive_available_cohort": {
                "official_symbols": cohort_total,
                "covered_symbols": len(cohort_covered),
                "coverage_rate": cohort_rate,
                "complete": cohort_complete,
                "missing_symbol_count": len(cohort_missing),
                "missing_symbols": cohort_missing,
            },
        }

    symbols_nonempty_rate = float(symbol_rows / rows) if rows else 0.0
    quality_complete = bool(
        rows > 0
        and duplicate_rows == 0
        and symbol_rows == rows
        and coverage_complete
    )
    return {
        "rows": rows,
        "schema": schema,
        "announcement_date_min": date_min,
        "announcement_date_max": date_max,
        "composite_key": composite_key,
        "composite_key_duplicate_rows": duplicate_rows,
        "composite_key_duplicate_groups": duplicate_groups,
        "symbols_nonempty_rows": symbol_rows,
        "symbols_nonempty_rate": symbols_nonempty_rate,
        "unique_announcement_symbols": len(announcement_symbols),
        "delisting_announcement_symbols": len(delisting_announcement_symbols),
        "delisting_announcement_symbols_by_market": {
            market: len(symbols)
            for market, symbols in delisting_announcement_symbols_by_market.items()
        },
        "affirmative_cover_count": affirmative_cover_count,
        "article_78_exempt_count": article_78_exempt_count,
        "technical_share_replacement_count": technical_share_replacement_count,
        "explicit_cover_deadline_count": explicit_deadline_count,
        "official_delisting_coverage": coverage,
        "coverage_complete": coverage_complete,
        "archive_cohort_coverage_complete": archive_cohort_coverage_complete,
        "quality_complete": quality_complete,
    }


def main(argv: list[str] | None = None) -> int:
    global _RATE_LIMITER
    parser = argparse.ArgumentParser(description="Download official TW delisting/short-sale restriction announcements.")
    parser.add_argument("--output-dir", default="data_tw_public")
    parser.add_argument("--start-year", type=int, default=1995)
    parser.add_argument("--end-year", type=int, default=date.today().year)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument(
        "--request-interval",
        type=float,
        default=None,
        help=(
            "Host-global minimum seconds between TW public HTTP requests. "
            "Unspecified endpoints default to the stockAgent 8 req/s policy."
        ),
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=4,
        help="retry transient official-site failures this many times per archive request",
    )
    parser.add_argument(
        "--retry-backoff",
        type=float,
        default=0.75,
        help="initial exponential retry delay in seconds (capped at 8 seconds)",
    )
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="write successful responses and exit zero even when some archive requests fail",
    )
    args = parser.parse_args(argv)
    if args.end_year < args.start_year:
        parser.error("--end-year must be greater than or equal to --start-year")
    request_interval = resolve_request_interval("tw_public", args.request_interval)
    _RATE_LIMITER = SharedRateLimiter(request_interval, name="tw_public")
    print(
        f"[tw-short-restrictions] {describe_rate_limit('tw_public', request_interval)}",
        flush=True,
    )

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    records, failures, completeness = _download_announcement_archive(
        start_year=args.start_year,
        end_year=args.end_year,
        workers=args.workers,
        timeout=args.timeout,
        retries=max(0, int(args.retries)),
        retry_backoff=max(0.0, float(args.retry_backoff)),
        company_symbol_lookup=_load_company_symbol_lookup(output_dir),
    )
    try:
        terms = _download_archive_task(
            "tpex",
            "tpex:current:margin-trading-terms",
            _download_tpex_current_terms,
            args.timeout,
            retries=max(0, int(args.retries)),
            retry_backoff=max(0.0, float(args.retry_backoff)),
        )
        if not isinstance(terms, pl.DataFrame):
            raise RuntimeError(
                "TPEx current terms downloader returned an unexpected payload"
            )
        completeness.append(
            {
                "market": "tpex",
                "period": "current",
                "source": "margin-trading-terms",
                "status": "ok",
                "records": int(terms.height),
                "filtered_irrelevant": 0,
                "unparseable": 0,
                "detail_failures": 0,
            }
        )
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        failures.append(f"tpex:current:margin-trading-terms: {error}")
        completeness.append(
            {
                "market": "tpex",
                "period": "current",
                "source": "margin-trading-terms",
                "status": "error",
                "records": 0,
                "filtered_irrelevant": 0,
                "unparseable": 0,
                "detail_failures": 0,
                "error": error,
            }
        )
        terms = pl.DataFrame()

    announcement_path = output_dir / "tw_delisting_short_sale_announcements.parquet"

    if failures:
        print(
            f"[tw-short-restrictions] incomplete download failures={len(failures)}",
            file=sys.stderr,
        )
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        if not args.allow_partial:
            existing = (
                pl.read_parquet(announcement_path)
                if announcement_path.exists()
                else pl.DataFrame()
            )
            data_quality = _announcement_data_quality(existing, output_dir=output_dir)
            report_path = _write_completeness_report(
                output_dir,
                start_year=args.start_year,
                end_year=args.end_year,
                allow_partial=args.allow_partial,
                entries=completeness,
                failures=failures,
                data_quality=data_quality,
                data_output_written=False,
            )
            print(
                "[tw-short-restrictions] refusing to write partial output; "
                "rerun successfully or pass --allow-partial explicitly "
                f"(report={report_path})",
                file=sys.stderr,
            )
            return 1

    announcements = pl.DataFrame(records, infer_schema_length=None) if records else pl.DataFrame()
    if announcement_path.exists():
        existing = pl.read_parquet(announcement_path)
        announcements = existing if announcements.is_empty() else pl.concat([existing, announcements], how="diagonal_relaxed")
    merge_filtered_irrelevant = 0
    merge_unparseable = 0
    if not announcements.is_empty():
        repaired_records: list[dict] = []
        for record in announcements.to_dicts():
            subject = str(record.get("subject", "") or "")
            body = str(record.get("body_text", "") or "")
            repaired = _repair_announcement_record(record)
            if not is_relevant_announcement_subject(subject or body):
                merge_filtered_irrelevant += 1
                continue
            if not split_announcement_symbols(str(repaired.get("symbols", "") or "")):
                merge_unparseable += 1
                continue
            repaired_records.append(repaired)
        announcements = (
            pl.DataFrame(repaired_records, infer_schema_length=None)
            if repaired_records
            else pl.DataFrame()
        )
        if not announcements.is_empty():
            announcements = announcements.unique(
                ["market", "document_number", "source_url"], keep="last"
            ).sort(["announcement_date", "market"])
            announcements.write_parquet(announcement_path)
        elif announcement_path.exists():
            announcement_path.unlink()
    if not terms.is_empty():
        terms.write_parquet(output_dir / "tpex_short_sale_suspension_terms.parquet")
    data_quality = _announcement_data_quality(announcements, output_dir=output_dir)
    data_quality["merge_filtered_irrelevant_dropped"] = merge_filtered_irrelevant
    data_quality["merge_unparseable_dropped"] = merge_unparseable
    data_output_written = bool(
        announcement_path.exists()
        or (output_dir / "tpex_short_sale_suspension_terms.parquet").exists()
    )
    report_path = _write_completeness_report(
        output_dir,
        start_year=args.start_year,
        end_year=args.end_year,
        allow_partial=args.allow_partial,
        entries=completeness,
        failures=failures,
        data_quality=data_quality,
        data_output_written=data_output_written,
    )
    print(
        f"[tw-short-restrictions] announcements={announcements.height} "
        f"current_tpex_terms={terms.height} failures={len(failures)} "
        f"report={report_path} output={output_dir}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
