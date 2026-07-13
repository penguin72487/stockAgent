from __future__ import annotations

from dataclasses import dataclass
import re

from stockagent.data.tw_security import classify_tw_stock_or_etf


_DELISTING_NOTICE_RE = re.compile(
    r"終止(?:上市|上櫃|櫃檯買賣|(?:在|於)證券商營業處所買賣)"
)
_DELISTING_CANCEL_RE = re.compile(
    r"(?:免除|取消|撤銷)[^。；;\n]{0,160}"
    r"終止(?:上市|上櫃|櫃檯買賣|(?:在|於)證券商營業處所買賣)"
)
_CONTINUING_SHORT_RESTRICTION_RE = re.compile(
    r"(?:繼續|仍|仍然|維持)[^。；;\n]{0,16}"
    r"(?:暫停融資融券|暫停融券|停止融券)"
)
_EMERGING_TO_LISTED_RE = re.compile(
    r"(?:初次申請|申請初次)(?:上市|上櫃)[^。；;\n]{0,800}"
    r"終止[^。；;\n]{0,80}興櫃"
)
_RELATIVE_COVER_RE = re.compile(
    r"(?:"
    r"前\s*第?\s*(?:10|十)\s*個?\s*營業日(?:以)?前"
    r"[^。；;，,\n]{0,80}(?:償還或還券|還券了結|償還融資|回補了結)"
    r"|"
    r"(?:償還或還券|還券了結|償還融資|回補了結)"
    r"[^。；;，,\n]{0,80}前\s*第?\s*(?:10|十)\s*個?\s*營業日(?:以)?前"
    r")"
)
_NOTICE_EXEMPT_RE = re.compile(
    r"(?:無須|無需|毋須|不須|不需|免予|免)\s*(?:另行)?\s*通知"
)
_ARTICLE_78_EXEMPT_RE = re.compile(
    r"(?:"
    r"第\s*78\s*條[^。；;\n]{0,80}(?:但書|第\s*1\s*項\s*第\s*3\s*款)"
    r"|"
    r"(?:但書|第\s*1\s*項\s*第\s*3\s*款)[^。；;\n]{0,80}第\s*78\s*條"
    r")"
)
_SHORT_SIDE_EARLY_CLOSE_EXEMPT_RE = re.compile(
    r"融資融券(?:交易|餘額)?[^。；;\n]{0,24}"
    r"(?:無須|無需|毋須|不須|不需|免予)\s*"
    r"(?:提前)?\s*(?:償還或還券)?\s*(?:了結|清償|還券)"
)
_CANCELLED_RESTRICTION_RE = re.compile(
    r"(?:原(?:公告|訂|定)[^。；;\n]{0,160})?"
    r"(?:暫停融資融券|暫停融券|停止融券)[^。；;\n]{0,80}"
    r"(?:免予|不予|取消|撤銷)\s*(?:執行|實施)?"
)
_TECHNICAL_REPLACEMENT_PATTERNS = (
    re.compile(
        r"(?:變更(?:股票)?面額|減資|股票分割)[^。；;\n]{0,200}"
        r"(?:舊(?:股|股票)|換發新(?:股|股票))"
    ),
    re.compile(
        r"舊(?:股|股票)[^。；;\n]{0,120}"
        r"(?:終止|停止)[^。；;\n]{0,120}"
        r"(?:換發新(?:股|股票)|新(?:股|股票)[^。；;\n]{0,80}(?:繼續|同日|開始|換發|上市|上櫃|買賣))"
    ),
)
_SYMBOL_LABEL_RE = re.compile(
    r"(?P<label>"
    r"(?:上市|上櫃|興櫃)?(?:普通股|股票|公司|證券|有價證券)?\s*"
    r"(?:股票|證券)?\s*(?:代\s*(?:號|碼)|編\s*號)"
    r")\s*[：:=#、，\s（(]*"
    r"(?P<symbol>[0-9]{4,6}[A-Z]?)"
)
_CHINESE_SYMBOL_LABEL_RE = re.compile(
    r"(?P<label>"
    r"(?:上市|上櫃|興櫃)?(?:普通股|股票|公司|證券|有價證券)?\s*"
    r"(?:股票|證券)?\s*(?:代\s*(?:號|碼)|編\s*號)"
    r")\s*[：:=#、，\s（(]*"
    r"(?P<symbol>[〇○零一二三四五六七八九]{4,6})"
)
_CHINESE_DIGITS = str.maketrans(
    {
        "〇": "0",
        "○": "0",
        "零": "0",
        "一": "1",
        "二": "2",
        "三": "3",
        "四": "4",
        "五": "5",
        "六": "6",
        "七": "7",
        "八": "8",
        "九": "9",
    }
)
_FUND_SYMBOL_RE = re.compile(r"[（(](?P<symbol>0[0-9]{3,5}[A-Z]?)[）)]")
_FUND_SECURITY_CUE_RE = re.compile(r"(?:ETF|受益憑證|證券投資信託基金)", re.I)
_NON_EQUITY_CONTEXT_RE = re.compile(
    r"(?:債券|公司債|金融債|公債|權證|指數投資證券|(?i:ETN)|特別股)"
)
_EXPLICIT_EQUITY_CUE_RE = re.compile(
    r"(?:普通股|股票代[號碼]|上市股票|上櫃股票|受益憑證|存託憑證)"
)
_SHORT_RESTRICTION_EVENT_RE = re.compile(
    r"(?:暫停|停止|恢復)[^。；;\n]{0,24}(?:融資融券|融券)"
)
_CLAUSE_BOUNDARY_RE = re.compile(r"(?:暨|；|;|。|\n)")
_COMPANY_NAME_RE = re.compile(
    r"(?:其中)?(?P<name>[^，,；;。\n]{2,70}?(?:股份有限公司|有限公司|公司))"
)
_NAMED_SYMBOL_PAIR_RE = re.compile(
    r"(?P<name>[A-Z\u3400-\u9fff][A-Z0-9\u3400-\u9fff-]{1,30})"
    r"\s*[（(](?P<symbol>[0-9]{4,6}[A-Z]?)[）)]"
)
_RELEVANT_ANNOUNCEMENT_SUBJECT_RE = re.compile(
    r"(?:"
    r"終止[^。；;\n]{0,40}(?:上市|上櫃|櫃檯買賣|有價證券買賣)"
    r"|"
    r"(?:暫停|停止|恢復)[^。；;\n]{0,24}(?:融資融券|融券|買賣|交易)"
    r")"
)
_GENERIC_TRADING_METHOD_NOTICE_RE = re.compile(
    r"(?:變更交易方法|分盤方式交易|普通交割方式交易|普通方法交割)"
)
_TECHNICAL_TRADING_HALT_SUBJECT_RE = re.compile(
    r"(?:減資|換股|換發)[^。；;\n]{0,80}(?:停止|暫停)[^。；;\n]{0,24}(?:買賣|交易)"
)
_NON_EQUITY_SECURITY_SUBJECT_RE = re.compile(
    r"(?:"
    r"(?:認購|認售|牛熊)?[（(]?(?:售)?[）)]?權證|權證"
    r"|(?:中央政府|地方政府|政府|市府|臺北市|台北市|新北市|桃園市|臺中市|台中市|高雄市)?公債"
    r"|市府債"
    r"|(?:可轉換|交換|普通|無擔保|有擔保)?公司債"
    r"(?=券|到期|終止|停止|上市|上櫃|簡稱|代號|發行|屆期|之|[\s，,。；;（(]|$)"
    r"|金融債(?:券)?|一般債券|債券"
    r"|指數投資證券|(?i:ETN)|特別股"
    r")"
)


@dataclass(frozen=True, slots=True)
class DelistingNoticeRules:
    is_delisting_notice: bool
    delisting_cancelled: bool
    requires_relative_cover: bool
    article_78_exempt: bool
    technical_share_replacement: bool
    restriction_cancelled: bool
    continues_short_open_ban: bool
    emerging_to_listed_transition: bool


def classify_delisting_notice(text: str) -> DelistingNoticeRules:
    """Classify an official notice without relying on downloaded lifecycle data."""

    normalized = " ".join(str(text or "").split())
    article_78_exempt = bool(
        _NOTICE_EXEMPT_RE.search(normalized)
        or _ARTICLE_78_EXEMPT_RE.search(normalized)
        or _SHORT_SIDE_EARLY_CLOSE_EXEMPT_RE.search(normalized)
    )
    restriction_cancelled = bool(_CANCELLED_RESTRICTION_RE.search(normalized))
    delisting_cancelled = bool(_DELISTING_CANCEL_RE.search(normalized))
    continues_short_open_ban = bool(
        _CONTINUING_SHORT_RESTRICTION_RE.search(normalized)
    )
    emerging_to_listed_transition = bool(_EMERGING_TO_LISTED_RE.search(normalized))
    technical_share_replacement = any(
        pattern.search(normalized) is not None
        for pattern in _TECHNICAL_REPLACEMENT_PATTERNS
    )
    is_delisting_notice = bool(
        _DELISTING_NOTICE_RE.search(normalized)
        and not delisting_cancelled
        and not emerging_to_listed_transition
    )
    requires_relative_cover = bool(
        is_delisting_notice
        and not article_78_exempt
        and not technical_share_replacement
        and _RELATIVE_COVER_RE.search(normalized)
    )
    return DelistingNoticeRules(
        is_delisting_notice=is_delisting_notice,
        delisting_cancelled=delisting_cancelled,
        requires_relative_cover=requires_relative_cover,
        article_78_exempt=article_78_exempt,
        technical_share_replacement=technical_share_replacement,
        restriction_cancelled=restriction_cancelled,
        continues_short_open_ban=continues_short_open_ban,
        emerging_to_listed_transition=emerging_to_listed_transition,
    )


def extract_stock_symbols(text: str) -> list[str]:
    normalized = str(text or "").upper()
    values: set[str] = set()
    for match in _SYMBOL_LABEL_RE.finditer(normalized):
        label = "".join(match.group("label").split())
        context_start = max(0, match.start() - 60)
        context = normalized[context_start : match.start()]
        context = re.split(r"[；;。\n]", context)[-1]
        non_equity = list(_NON_EQUITY_CONTEXT_RE.finditer(context))
        equity = list(_EXPLICIT_EQUITY_CUE_RE.finditer(context))
        explicitly_equity_labeled = any(
            token in label for token in ("股票", "普通股", "公司")
        )
        if non_equity and not explicitly_equity_labeled:
            last_non_equity = non_equity[-1].start()
            last_equity = equity[-1].start() if equity else -1
            if last_non_equity >= last_equity:
                continue
        values.add(match.group("symbol"))
    # Some early TPEx archive notices spell a stock code digit-by-digit with
    # Chinese numerals (for example 「股票代號：三一三三」).  Limit conversion to
    # an explicit symbol label so dates, share counts, and legal citations can
    # never become securities.
    for match in _CHINESE_SYMBOL_LABEL_RE.finditer(normalized):
        values.add(match.group("symbol").translate(_CHINESE_DIGITS))
    # Fund notices commonly put the ETF code in parentheses after the full
    # fund name without a literal 「證券代號」 label (for example 00925).
    # Leading zero plus an explicit fund/beneficiary-certificate cue keeps this
    # path from admitting company bonds, warrants and unrelated document IDs.
    if _FUND_SECURITY_CUE_RE.search(normalized):
        for match in _FUND_SYMBOL_RE.finditer(normalized):
            values.add(match.group("symbol"))
    return sorted(
        symbol
        for symbol in values
        if classify_tw_stock_or_etf(symbol) is not None
    )


def _event_clauses(text: str, event_pattern: re.Pattern[str]) -> list[str]:
    normalized = str(text or "")
    boundaries = list(_CLAUSE_BOUNDARY_RE.finditer(normalized))
    clauses: list[str] = []
    for event in event_pattern.finditer(normalized):
        start = max(
            (boundary.end() for boundary in boundaries if boundary.end() <= event.start()),
            default=0,
        )
        end = min(
            (boundary.start() for boundary in boundaries if boundary.start() >= event.end()),
            default=len(normalized),
        )
        clause = normalized[start:end].strip(" ，,：:")
        if clause:
            clauses.append(clause)
    return clauses


def _company_name_keys(text: str) -> list[str]:
    keys: list[str] = []
    for match in _COMPANY_NAME_RE.finditer(text):
        value = match.group("name")
        value = re.sub(r"^(?:公告|其中|暨)", "", value)
        value = re.sub(r"^(?:英屬開曼群島商|英屬維京群島商)", "", value)
        value = re.sub(r"(?:股份有限公司|有限公司|公司)$", "", value)
        value = re.sub(r"[^A-Z0-9\u3400-\u9fff]", "", value.upper())
        if len(value) >= 2:
            keys.append(value)
    return keys


def _named_symbol_pairs(text: str) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for match in _NAMED_SYMBOL_PAIR_RE.finditer(str(text or "").upper()):
        if classify_tw_stock_or_etf(match.group("symbol")) is None:
            continue
        name = re.sub(r"(?:股份有限公司|有限公司)$", "", match.group("name"))
        name = re.sub(r"(?:-?KY)$", "", name)
        name = re.sub(r"[^A-Z0-9\u3400-\u9fff]", "", name)
        if len(name) >= 2:
            pairs.append((name, match.group("symbol")))
    return pairs


def _resolve_clause_symbols(clause: str, full_text: str) -> list[str]:
    direct = extract_stock_symbols(clause)
    if direct:
        return direct
    names = _company_name_keys(clause)
    if not names:
        return []
    resolved_by_score: dict[str, int] = {}
    for alias, symbol in _named_symbol_pairs(full_text):
        scores = [
            2 if alias == name else 1
            for name in names
            if len(alias) >= 2
            and len(name) >= 2
            and (alias in name or name in alias)
        ]
        if scores:
            resolved_by_score[symbol] = max(scores)
    if not resolved_by_score:
        return []
    # Prefer an exact shorthand match when the same issuer also has a numbered
    # convertible-bond alias in the roundup (for example 普格/3073 versus
    # 普格三/30733).  Falling back to containment remains necessary when the
    # subject uses a legal name and the table uses a shorter market alias.
    best_score = max(resolved_by_score.values())
    return sorted(
        symbol for symbol, score in resolved_by_score.items() if score == best_score
    )


def _extract_event_symbols(
    primary_text: str,
    full_text: str,
    event_pattern: re.Pattern[str],
) -> list[str]:
    values: set[str] = set()
    for clause in _event_clauses(primary_text, event_pattern):
        values.update(_resolve_clause_symbols(clause, full_text))
    return sorted(values)


def extract_announcement_symbols(subject: str, body: str) -> list[str]:
    """Extract the equity targets of the notice, not every mentioned security."""

    full_text = f"{subject or ''} {body or ''}"
    for primary in (subject, body):
        delisting_symbols = _extract_event_symbols(
            primary,
            full_text,
            _DELISTING_NOTICE_RE,
        )
        if delisting_symbols:
            return delisting_symbols
        restriction_symbols = _extract_event_symbols(
            primary,
            full_text,
            _SHORT_RESTRICTION_EVENT_RE,
        )
        if restriction_symbols:
            return restriction_symbols
    subject_symbols = extract_stock_symbols(subject)
    if subject_symbols:
        return subject_symbols
    return extract_stock_symbols(body)


def is_relevant_announcement_subject(subject: str) -> bool:
    """Return whether a monthly archive row is relevant before fetching detail."""

    normalized = str(subject or "")
    if _EMERGING_TO_LISTED_RE.search(normalized):
        return False
    # Periodic TPEx disposition roundups use phrases such as 「恢復普通交割
    # 方式交易」.  They are not market resumptions, delistings, or margin-short
    # state transitions.  Keep a roundup only when its subject also contains an
    # explicit lifecycle or short-side event; otherwise the broad 「恢復…交易」
    # wording would admit unrelated lists and make every listed symbol look like
    # an execution-rule target.
    if (
        _GENERIC_TRADING_METHOD_NOTICE_RE.search(normalized)
        and not _DELISTING_NOTICE_RE.search(normalized)
        and not _SHORT_RESTRICTION_EVENT_RE.search(normalized)
    ):
        return False
    if (
        _TECHNICAL_TRADING_HALT_SUBJECT_RE.search(normalized)
        and not _DELISTING_NOTICE_RE.search(normalized)
        and not _SHORT_RESTRICTION_EVENT_RE.search(normalized)
    ):
        return False
    relevant = bool(
        _DELISTING_NOTICE_RE.search(normalized)
        or _RELEVANT_ANNOUNCEMENT_SUBJECT_RE.search(normalized)
    )
    if not relevant:
        return False
    if _NON_EQUITY_SECURITY_SUBJECT_RE.search(normalized):
        return bool(
            _EXPLICIT_EQUITY_CUE_RE.search(normalized)
            and extract_announcement_symbols(normalized, "")
        )
    return True


def split_announcement_symbols(value: str) -> list[str]:
    return sorted(
        {
            token.upper()
            for token in re.split(r"[,;；、\s]+", str(value or ""))
            if re.fullmatch(r"[0-9]{4,6}[A-Z]?", token.upper())
        }
    )
