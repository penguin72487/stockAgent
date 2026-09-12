"""Receipt-backed share-replacement evidence, separate from short-sale rules.

The exchange's ordinary ex-dividend table explicitly excludes capital
reductions. A missing stock quote during a proven halt is not a price to
download or interpolate. This collector preserves that separate lifecycle.
It does NOT infer issuer payment dates, fractional-share proceeds or fills.
"""
from __future__ import annotations

import argparse
from decimal import Decimal
from datetime import date, datetime, timedelta, timezone
import fcntl
import json
from pathlib import Path
import re
import sys
import unicodedata

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from bs4 import BeautifulSoup
import polars as pl

from downloader.download_tw_corporate_action_entitlements import (
    _cached_or_post, _configure_tw_public_rate_limiter, _file_receipt,
    _reset_raw_receipt_requests, _write_content_addressed_receipt_manifest,
    _write_json_atomic, _write_parquet_atomic, _public_access_denial_response,
    _mops_throttle_response,
)

LIST_URL = "https://www.twse.com.tw/exchangeReport/TWTAUU"
DETAIL_URL = "https://www.twse.com.tw/rwd/zh/reducation/TWTAVUDetail"
MOPS_ANNOUNCEMENT_URL = "https://mopsov.twse.com.tw/mops/web/ajax_t05st01"
TPEX_URL = "https://www.tpex.org.tw/www/zh-tw/bulletin/revivt"
_ISSUER_DATE = r"(?:民國)?([0-9]{3,4})[年/]([0-9]{1,2})[月/]([0-9]{1,2})日?"
_RESUMPTION_LABELS = (r"新股預計上[市櫃]日|換發新股暨上[市櫃]買賣日(?:期)?(?:\(即舊股票終止上[市櫃]日\))?"
    r"|新股票上[市櫃]開始買賣日期暨舊股票終止上[市櫃]日期|新股票上[市櫃]買賣日期(?:預訂為)?"
    r"|開始換發新股票及新股票上[市櫃]買賣日期預訂為")


def _issuer_text(content: bytes, symbol: str) -> str:
    text = re.sub(r"\s+", "", unicodedata.normalize("NFKC",
        BeautifulSoup(content, "html.parser", from_encoding="utf-8").get_text(" ", strip=True)))
    if not re.search(r"本資料由.{0,30}" + re.escape(symbol) + r"[^0-9]", text):
        raise ValueError("issuer announcement company identity mismatch")
    return text


def _issuer_dates(text: str, labels: str) -> set[date]:
    found = set()
    for match in re.finditer(r"(?:" + labels + r")(?:[:：]|為|自){0,2}" + _ISSUER_DATE, text):
        year, month, day = map(int, match.groups())
        found.add(date(year + 1911 if year < 1911 else year, month, day))
    return found


def parse_tpex(payload: dict, *, start: date, end: date) -> list[dict]:
    if payload.get("stat") != "ok" or payload.get("date") != f"{start:%Y%m%d}~{end:%Y%m%d}":
        raise ValueError("TPEx share-replacement response date/status mismatch")
    tables = payload.get("tables") or []
    if len(tables) != 1:
        raise ValueError("TPEx share-replacement table count changed")
    fields = tables[0].get("fields") or []
    if not {"恢復買賣日期", "股票代號", "名稱", "減資原因", "詳細資料"} <= set(fields):
        raise ValueError("TPEx share-replacement schema changed")
    result, seen = [], set()
    for values in tables[0].get("data", []):
        row = dict(zip(fields, values, strict=True))
        text = str(row["恢復買賣日期"])
        if not re.fullmatch(r"\d{7}", text):
            raise ValueError("TPEx resumption date is not compact ROC date")
        resumed = _day(f"{text[:3]}/{text[3:5]}/{text[5:]}")
        symbol = str(row["股票代號"]).strip()
        detail = {}
        for tr in BeautifulSoup(row["詳細資料"], "html.parser").find_all("tr"):
            cells = tr.find_all(["th", "td"])
            if len(cells) != 2:
                raise ValueError("TPEx share-replacement detail row shape changed")
            key = cells[0].get_text(strip=True).rstrip(":：")
            if key in detail:
                raise ValueError("duplicate TPEx share-replacement detail field")
            detail[key] = cells[1].get_text(" ", strip=True).replace("\xa0", " ")
        if (not re.fullmatch(r"[0-9A-Z]{4,6}", symbol) or not detail["股票代號/股票名稱"].startswith(symbol + " ")
                or _day(detail["恢復買賣日期"]) != resumed or (symbol, resumed) in seen
                or not start <= resumed <= end):
            raise ValueError("TPEx share-replacement identity/range mismatch")
        seen.add((symbol, resumed))
        halted = _day(detail["停止買賣日期"])
        shares, cash = _number(detail["每壹仟股換發新股票"], "股"), _number(detail["每股退還股款"], "元/股")
        if halted >= resumed or shares <= 0:
            raise ValueError("TPEx invalid share ratio or suspension interval")
        subscription = any(detail[key] != "NA" for key in ("現金增資總股數", "現金增資認購價", "現金增資配股率"))
        result.append({"symbol": symbol, "name": row["名稱"], "market": "tpex", "resume_date": resumed,
            "suspension_date": halted, "reason": row["減資原因"], "new_shares_per_1000_old": shares,
            "cash_return_per_old_share": cash, "cash_payment_date": None,
            "subscription_terms_present": subscription, "subscription_shares_per_1000": None,
            "cash_dividend_per_old_share": None, "source_url": TPEX_URL, "detail_source_url": TPEX_URL,
            "accounting_terms_complete": False, "unresolved_terms": "issuer_payment_or_fractional_share_terms_and_paper_policy",
            "historical_halt_evidence": True, "executable_price": False,
            "contract": "exchange_share_replacement_reference_v1"})
    return result


def parse_issuer_payment(content: bytes, event: dict) -> date | None:
    """Join an issuer plan to the exchange event, never guess its payment day."""
    text = _issuer_text(content, event["symbol"])
    resumed = _issuer_dates(text, _RESUMPTION_LABELS)
    if event["resume_date"] not in resumed:
        return None
    if len(resumed) != 1:
        raise ValueError("issuer plan contains conflicting resumption dates")
    payments = _issuer_dates(text, r"(?:現金減資)?退還股款發放日(?:期)?|現金減資款發放日期|退還現金股款發放日")
    if len(payments) > 1:
        raise ValueError("issuer plan contains conflicting cash payment dates")
    if payments and min(payments) < event["resume_date"]:
        raise ValueError("issuer cash return precedes share replacement; separate timing contract required")
    return min(payments) if payments else None


def _issuer_announcements(raw: Path, event: dict, args: argparse.Namespace):
    """One canonical bounded MOPS search for both payment and detail recovery."""
    anchor = event.get("suspension_date")
    if anchor is None:
        anchor = datetime.strptime(event["detail_file_date"], "%Y%m%d").date()
    month_index = anchor.year * 12 + anchor.month - 1
    candidates = []
    for offset in range(3):
        year, month = divmod(month_index - offset, 12)
        month += 1
        data = {"step": "1", "firstin": "1", "off": "1", "co_id": event["symbol"],
                "year": str(year - 1911), "month": f"{month:02d}", "TYPEK": "all", "encodeURIComponent": "1"}
        path = raw / f"mops-{event['symbol']}-{year}{month:02d}-asof-{args.end_date}-v1.html"
        content = _cached_or_post(path, url=MOPS_ANNOUNCEMENT_URL, data=data,
            timeout=args.timeout, retries=args.retries)
        soup = BeautifulSoup(content, "html.parser", from_encoding="utf-8")
        form = soup.find("form", attrs={"name": "t05st01_fm"})
        if form is None:
            if "查無" in soup.get_text() or "無資料" in soup.get_text():
                continue
            raise ValueError("MOPS announcement list has no detail form or no-data marker")
        template = {item["name"]: item.get("value", "") for item in form.select("input[name]")}
        for button in soup.select("input[onclick]"):
            row = button.find_parent("tr")
            # MOPS subjects contain embedded CR/LF; do not let a display wrap
            # hide a valid plan. Some subjects say only 換股作業計劃, so identity
            # and the exact resumption date are verified from the detail too.
            subject = re.sub(r"\s+", "", row.get_text(" ", strip=True)) if row else ""
            if not re.search(r"減資.*(?:換股|換發)|換股作業計[劃畫]", subject):
                continue
            fields = dict(re.findall(r"document\.t05st01_fm\.([a-zA-Z_]+)\.value='([^']*)'", button["onclick"]))
            if fields.get("co_id") != event["symbol"] or not {"spoke_date", "spoke_time", "seq_no", "TYPEK"} <= fields.keys():
                raise ValueError("MOPS announcement request identity changed")
            issued = datetime.strptime(fields["spoke_date"], "%Y%m%d").date()
            if issued >= event.get("suspension_date", event["resume_date"]):
                continue
            candidates.append((issued, fields["spoke_time"], fields["seq_no"], template | fields))
    for issued, clock, seq, data in sorted(candidates, reverse=True):
        path = raw / f"mops-{event['symbol']}-{issued}-{clock}-{seq}-detail-v1.html"
        content = _cached_or_post(path, url=MOPS_ANNOUNCEMENT_URL, data=data,
            timeout=args.timeout, retries=args.retries)
        yield issued, path, content


def fetch_issuer_payment(raw: Path, event: dict, args: argparse.Namespace) -> dict:
    # Explicit bounded archive scope. Unresolved cases are visible, not zeros.
    for issued, path, content in _issuer_announcements(raw, event, args):
        payment = parse_issuer_payment(content, event)
        if payment:
            return {"cash_payment_date": payment, "issuer_announcement_date": issued,
                "issuer_payment_source_url": MOPS_ANNOUNCEMENT_URL,
                "issuer_payment_source_sha256": _file_receipt(path)["sha256"],
                "unresolved_terms": "paper_share_basis_conversion_and_odd_lot_execution_policy"}
    return {}


def parse_issuer_replacement(content: bytes, event: dict) -> dict | None:
    """Recover physical terms from the issuer's original MOPS disclosure.

    Reference only: it does not certify cash-in-lieu/subscription accounting.
    Ratios must be stated explicitly; never invert prices, percentages or a
    dividend adjustment. An unrelated revision/resumption must not be used.
    """
    text = _issuer_text(content, event["symbol"])
    resumed = _issuer_dates(text, _RESUMPTION_LABELS)
    if event["resume_date"] not in resumed:
        return None
    if len(resumed) != 1:
        raise ValueError("issuer replacement has conflicting resumption dates")
    halted = _issuer_dates(text, r"(?:舊股票|減資股票(?:\(舊股票\))?)(?:停止在(?:交易)?市場買賣(?:交易)?|停止市場買賣|(?:於交易市場|市場)?停止(?:買賣|交易))(?:之)?(?:日期|期間|日)?")
    # Some issuers put the explicit interval BEFORE the trading-halt verb.
    # Never confuse that with the separate shareholder-register closing period.
    interval = (r"舊股票自" + _ISSUER_DATE + r"起?至" + _ISSUER_DATE
                + r"止(?:期間內)?[,，]?停止(?:在)?市[場埸]買賣(?:交易)?")
    for match in re.finditer(interval, text):
        values = list(map(int, match.groups()))
        days = [date(y + 1911 if y < 1911 else y, m, d)
                for y, m, d in (values[:3], values[3:])]
        if not days[0] <= days[1] < event["resume_date"]:
            raise ValueError("issuer suspension interval contradicts resumption")
        halted.add(days[0])
    if len(halted) != 1 or min(halted) >= event["resume_date"]:
        raise ValueError("issuer replacement lacks an exact suspension date")
    ratios = {float(value.replace(",", "")) for value in re.findall(
        r"每(?:壹|一)?[仟千]股換發(?:新(?:普通股|股票|股)?)?(?:為|計)?[:：]?([0-9][0-9,.]*)股", text)}
    if len(ratios) != 1 or min(ratios) <= 0:
        raise ValueError("issuer replacement lacks an explicit unambiguous shares-per-1000 ratio")
    cash_values = {float(value.replace(",", "")) for value in re.findall(
        r"每股(?:可)?(?:減資)?退(?:還|換)(?:現金股款|股款現金|現金|股款|股金)?(?:新[臺台]幣)?[:：]?([0-9][0-9,.]*)元", text)}
    # Explicit amount-per-1000 has an exact unit conversion. It is not a cash
    # estimate from capital, a rounded percentage or a price adjustment.
    cash_values.update(float(value.replace(",", "")) / 1000 for value in re.findall(
        r"每(?:壹|一)?[仟千]股退還(?:現金股款|現金|股款|股金)?(?:新[臺台]幣)?[:：]?([0-9][0-9,.]*)元", text))
    # The same explicit per-1000 subject can govern both shares and cash. Keep
    # the entire clause grammar bounded; never attach a later corporate total
    # or an unrelated fractional-share payment to that unit.
    number = r"[0-9][0-9,.]*"
    cash_values.update(float(value.replace(",", "")) / 1000 for value in re.findall(
        r"每(?:壹|一)?[仟千]股換發(?:新股)?" + number + r"股"
        r"(?:\(即每(?:壹|一)?[仟千]股減少" + number + r"股\))?"
        r"(?:,減資比率為" + number + r"%)?[,，]?並退還(?:現金股款|股款現金|現金|股款)?"
        r"(?:新[臺台]幣)?(" + number + r")元", text))
    # Some plans state the refund as an exact formula on CANCELLED shares.
    # This is not the distinct cash-in-lieu clause for fractional NEW shares.
    # Only an already explicit, non-approximate exchange ratio can supply the
    # cancelled quantity; never derive it from a rounded percent or a price.
    refund_faces = {Decimal(value.replace(",", "")) for value in re.findall(
        r"股東減少之股份按每股面額新[臺台]幣(" + number + r")元計算退還現金", text)}
    if refund_faces:
        if len(refund_faces) != 1 or min(refund_faces) <= 0 or min(ratios) > 1000:
            raise ValueError("issuer cancelled-share cash formula is ambiguous or invalid")
        cash_values.add(float((Decimal(1000) - Decimal(str(next(iter(ratios)))))
                              / Decimal(1000) * next(iter(refund_faces))))
    if "退還股款" in str(event["reason"]):
        if len(cash_values) != 1:
            raise ValueError("issuer cash reduction lacks an explicit cash-per-share amount")
        cash = next(iter(cash_values))
    elif event["reason"] == "彌補虧損" and not any(cash_values):
        cash = 0.0  # bound by the exchange's explicit event classification
    else:
        raise ValueError("issuer replacement cash terms conflict with exchange event")
    return event | {"suspension_date": next(iter(halted)),
        "new_shares_per_1000_old": next(iter(ratios)), "cash_return_per_old_share": cash,
        "cash_dividend_per_old_share": None, "subscription_shares_per_1000": None,
        "cash_payment_date": parse_issuer_payment(content, event) if cash else None,
        "accounting_terms_complete": False,
        "unresolved_terms": "issuer_fractional_share_subscription_and_other_distribution_terms_not_accounted",
        "detail_source_url": MOPS_ANNOUNCEMENT_URL,
        "detail_source_kind": "official_issuer_replacement_announcement",
        "reference_only": True,
        "cash_return_calculation": ("explicit_cancelled_shares_times_face_value"
                                    if refund_faces else "explicit_cash_per_share_or_event_classification"),
        "historical_halt_evidence": True, "executable_price": False,
        "contract": "exchange_share_replacement_reference_v1"}


def fetch_issuer_replacement(raw: Path, event: dict, args: argparse.Namespace) -> dict:
    for issued, path, content in _issuer_announcements(raw, event, args):
        row = parse_issuer_replacement(content, event)
        if row is not None:
            return row | {"issuer_announcement_date": issued,
                "detail_source_sha256": _file_receipt(path)["sha256"]}
    raise ValueError("no matching official issuer replacement announcement in bounded archive")


def _day(value: str) -> date:
    match = re.fullmatch(r"(\d{3,4})/(\d{2})/(\d{2})", str(value).strip())
    if not match:
        raise ValueError(f"invalid exchange date: {value!r}")
    year, month, day = map(int, match.groups())
    return date(year + 1911 if year < 1911 else year, month, day)


def _number(value: str, unit: str) -> float:
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)\s*" + re.escape(unit), str(value).strip())
    if not match:
        raise ValueError(f"unknown exchange amount/unit: {value!r}")
    return float(match.group(1))


def parse_list(payload: dict, *, start: date, end: date) -> list[dict]:
    if payload.get("stat") != "OK":
        raise ValueError("TWSE capital-reduction query was not successful")
    fields = payload.get("fields", [])
    required = {"恢復買賣日期", "股票代號", "名稱", "減資原因", "詳細資料"}
    if not required <= set(fields):
        raise ValueError("TWSE capital-reduction list schema changed")
    rows, seen = [], set()
    for values in payload.get("data", []):
        if len(values) != len(fields):
            raise ValueError("TWSE capital-reduction row length mismatch")
        row = dict(zip(fields, values, strict=True))
        symbol = str(row["股票代號"]).strip()
        resumed = _day(row["恢復買賣日期"])
        detail = re.fullmatch(r"([0-9A-Z]{4,6})\s*,(\d{8})", row["詳細資料"])
        if not re.fullmatch(r"[0-9A-Z]{4,6}", symbol) or not detail or detail[1] != symbol:
            raise ValueError("TWSE share-replacement identity mismatch")
        if not start <= resumed <= end or (symbol, resumed) in seen:
            raise ValueError("duplicate or out-of-range share-replacement event")
        seen.add((symbol, resumed))
        rows.append({"symbol": symbol, "name": row["名稱"], "market": "twse",
            "resume_date": resumed, "reason": row["減資原因"],
            "detail_file_date": detail[2], "source_url": LIST_URL})
    return rows


def parse_detail(payload: dict, event: dict) -> dict:
    fields, values = payload.get("fields", []), payload.get("data", [])
    if payload.get("stat") != "OK" or len(values) != 1 or len(values[0]) != len(fields):
        raise ValueError("TWSE share-replacement detail is incomplete")
    row = dict(zip(fields, values[0], strict=True))
    if str(row.get("股票代號：", "")).strip() != event["symbol"]:
        raise ValueError("TWSE share-replacement detail symbol mismatch")
    halted = _day(row["停止買賣日期："])
    shares = _number(row["每壹仟股換發新股票："], "股")
    cash = _number(row["每股退還股款："], "元/股")
    dividend = _number(row["原股每股配發現金股利："], "元/股")
    subscribed = _number(row["按股東持股比例每千股認購："], "股")
    if not halted < event["resume_date"] or shares <= 0:
        raise ValueError("invalid halt interval or share conversion ratio")
    return event | {"suspension_date": halted, "new_shares_per_1000_old": shares,
        "cash_return_per_old_share": cash, "cash_dividend_per_old_share": dividend,
        "subscription_shares_per_1000": subscribed,
        "cash_payment_date": None,
        "accounting_terms_complete": False,
        "unresolved_terms": ("issuer_cash_payment_date_and_fractional_share_disposition"
                             if cash or dividend else "paper_share_basis_conversion_and_fractional_share_disposition"),
        "detail_source_url": DETAIL_URL,
        "historical_halt_evidence": True, "executable_price": False,
        "contract": "exchange_share_replacement_reference_v1"}


def run(args: argparse.Namespace) -> dict:
    start, end = date.fromisoformat(args.start_date), date.fromisoformat(args.end_date)
    if start > end:
        raise ValueError("start date exceeds end date")
    lookahead = int(getattr(args, "announced_lookahead_days", 90))
    if not 1 <= lookahead <= 366:
        raise ValueError("announced lookahead must be between 1 and 366 days")
    # The endpoints filter on RESUMPTION, not suspension. A current halt
    # therefore needs already-published future-dated resumption announcements.
    # This does not claim observation or completeness of future market data.
    query_end = end + timedelta(days=lookahead)
    _configure_tw_public_rate_limiter(args.request_interval)
    _reset_raw_receipt_requests()
    raw = args.output_dir / "raw" / "tw_share_replacement_reference"
    output = args.output_dir / "tw_share_replacement_reference.parquet"
    summary = {"schema_version": 2, "issuer_parser_contract_version": 2,
        "coverage_start": str(start), "coverage_end": str(end),
        "announced_resumption_query_end": str(query_end),
        "future_market_observations_claimed": False,
        "covered_markets": [], "uncovered_markets": ["twse", "tpex"],
        "complete_all_markets": False, "accounting_ready": False,
        "generated_at_utc": datetime.now(timezone.utc).isoformat()}
    rows, failures, recovered = [], [], []

    def failed(stage: str, exc: Exception, event: dict | None = None) -> None:
        identity = {} if event is None else {
            key: str(event[key]) for key in ("market", "symbol", "resume_date")
        }
        failures.append({"stage": stage, **identity,
                         "error": f"{type(exc).__name__}: {exc}"})

    def covered(market: str) -> None:
        summary["covered_markets"].append(market)
        summary["uncovered_markets"].remove(market)

    try:
        listing_path = raw / f"twse-{start}-{query_end}-asof-{end}-v2.json"
        try:
            content = _cached_or_post(listing_path, url=LIST_URL,
                data={"startDate": start.strftime("%Y%m%d"), "endDate": query_end.strftime("%Y%m%d"), "response": "json"},
                method="GET", timeout=args.timeout, retries=args.retries)
            events = parse_list(json.loads(content), start=start, end=query_end)
        except Exception as exc:
            failed("twse_list", exc)
            events = []
        twse_detail_backoff = False
        for event in events:
            primary_attempted = False
            try:
                path = raw / f"twse-{event['symbol']}-{event['detail_file_date']}-asof-{end}-v1.json"
                cached = path.read_bytes() if path.is_file() else None
                if twse_detail_backoff and (cached is None or _public_access_denial_response(cached)
                                           or _mops_throttle_response(cached)):
                    raise RuntimeError("TWSE detail deferred for this attempt after source access denial")
                primary_attempted = True
                content = _cached_or_post(path, url=DETAIL_URL,
                    data={"STK_NO": event["symbol"], "FILE_DATE": event["detail_file_date"], "response": "json"},
                    method="GET", timeout=args.timeout, retries=args.retries)
                rows.append(parse_detail(json.loads(content), event) | {
                    "list_source_sha256": _file_receipt(listing_path)["sha256"],
                    "detail_source_sha256": _file_receipt(path)["sha256"]})
            except Exception as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status in {403, 428, 429} or "HTTP-200 throttle/access-denial" in str(exc):
                    twse_detail_backoff = True
                try:
                    row = fetch_issuer_replacement(raw, event, args)
                    rows.append(row | {"list_source_sha256": _file_receipt(listing_path)["sha256"]})
                    recovered.append({"market": event["market"], "symbol": event["symbol"],
                        "resume_date": str(event["resume_date"]),
                        "original_error": f"{type(exc).__name__}: {exc}",
                        "primary_attempted": primary_attempted,
                        "replacement_source": MOPS_ANNOUNCEMENT_URL,
                        "replacement_sha256": row["detail_source_sha256"]})
                except Exception as fallback_exc:
                    failed("twse_detail", RuntimeError(f"{exc}; issuer fallback: {fallback_exc}"), event)
            if len(rows) % 25 == 0 and rows:
                print(f"[share-replacement] reference_rows={len(rows)} recovered={len(recovered)} failures={len(failures)}", flush=True)
        summary["recovered_exchange_detail_failures"] = recovered
        summary["official_issuer_reference_recovery_count"] = len(recovered)
        if not failures:
            covered("twse")
        tpex_path = raw / f"tpex-{start}-{query_end}-asof-{end}-v2.json"
        try:
            content = _cached_or_post(tpex_path, url=TPEX_URL,
                data={"startDate": start.strftime("%Y/%m/%d"), "endDate": query_end.strftime("%Y/%m/%d"), "response": "json"},
                method="GET", timeout=args.timeout, retries=args.retries)
            rows.extend(row | {"list_source_sha256": _file_receipt(tpex_path)["sha256"],
                                "detail_source_sha256": _file_receipt(tpex_path)["sha256"]}
                        for row in parse_tpex(json.loads(content), start=start, end=query_end))
            covered("tpex")
        except Exception as exc:
            failed("tpex_reference", exc)
        for row in rows:
            if row["cash_return_per_old_share"] > 0:
                try:
                    row.update(fetch_issuer_payment(raw, row, args))
                except Exception as exc:
                    failed("issuer_payment", exc, row)
        if failures:
            # Successful raw requests remain content-addressed and resumable,
            # but a partial attempt may NEVER replace the last accepted table.
            partial_path = output.with_suffix(".attempt.parquet")
            partial_receipt = None
            if rows:
                # Retain successful structured rows as first-class evidence.
                # This is deliberately a quarantined attempt artifact: the
                # canonical accepted parquet above is unchanged, and callers
                # must join the explicit failure identities before using it.
                partial_frame = pl.from_dicts(rows, infer_schema_length=None).sort(
                    ["resume_date", "symbol"]
                )
                _write_parquet_atomic(partial_frame, partial_path)
                partial_receipt = _file_receipt(partial_path)
            summary.update(completed_reference_rows=len(rows), failures=failures,
                partial_reference_contract=(
                    "successful_rows_only_not_complete_or_accepted"
                    if partial_receipt is not None else None
                ),
                partial_reference_receipt=partial_receipt,
                raw_receipt_manifest=_write_content_addressed_receipt_manifest(
                    output_dir=args.output_dir, raw_root=raw))
            raise RuntimeError(f"share-replacement collection incomplete: {len(failures)} source failures")
        if not rows:
            raise ValueError("empty share-replacement range requires an explicit no-data receipt")
        frame = pl.from_dicts(rows, infer_schema_length=None).sort(["resume_date", "symbol"])
        manifest = _write_content_addressed_receipt_manifest(output_dir=args.output_dir, raw_root=raw)
        _write_parquet_atomic(frame, output)
        summary.update(rows=frame.height, source_download_complete=True, failure_count=0,
            complete_all_markets=True,
            exact_cash_return_payment_events=sum(row.get("cash_payment_date") is not None for row in rows),
            raw_receipt_manifest=manifest, output_receipt=_file_receipt(output))
        _write_json_atomic(output.with_suffix(".summary.json"), summary)
    except Exception as exc:
        summary.update(source_download_complete=False, failure_count=max(len(failures), 1),
                       error=f"{type(exc).__name__}: {exc}")
        _write_json_atomic(output.with_suffix(".attempt.summary.json"), summary)
        raise
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--announced-lookahead-days", type=int, default=90,
                        help="Query already-published later resumption dates to cover current halts; not future prices.")
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--request-interval", type=float, default=1.)
    args = parser.parse_args()
    lock = args.output_dir / "state/locks/tw_share_replacement_reference.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        print(json.dumps(run(args), ensure_ascii=False, default=str, indent=2))


if __name__ == "__main__":
    main()
