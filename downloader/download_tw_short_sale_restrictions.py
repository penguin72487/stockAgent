from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
import re
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import polars as pl
import requests


TWSE_SEARCH = "https://dsp.twse.com.tw/official/search"
TWSE_RESULT = "https://dsp.twse.com.tw/official/result"
TPEX_SEARCH = "https://dsp.tpex.org.tw/web/announcement/announcement.php"
TPEX_SHORT_TERM = "https://www.tpex.org.tw/openapi/v1/tpex_margin_trading_term"
USER_AGENT = "Mozilla/5.0 stockAgent/1.0"


def _roc_date(text: str) -> str | None:
    digits = re.sub(r"\D", "", text)
    if len(digits) in {7, 8}:
        year_digits = 4 if len(digits) == 8 else 3
        year = int(digits[:year_digits])
        if year_digits == 3:
            year += 1911
        try:
            return date(year, int(digits[year_digits : year_digits + 2]), int(digits[year_digits + 2 :])).isoformat()
        except ValueError:
            pass
    match = re.search(r"(?<!\d)(\d{2,3})[年/\-.](\d{1,2})[月/\-.](\d{1,2})日?", text)
    if not match:
        return None
    try:
        return date(int(match.group(1)) + 1911, int(match.group(2)), int(match.group(3))).isoformat()
    except ValueError:
        return None


def _event_date(text: str, phrases: tuple[str, ...]) -> str | None:
    for phrase in phrases:
        date_pattern = r"([民國\s]*\d{2,3}[年/\-.]\d{1,2}[月/\-.]\d{1,2}日?)"
        patterns = (
            rf"(?:自|訂於)\s*{date_pattern}\s*起.{{0,8}}?{phrase}",
            rf"{phrase}(?:開始|生效|終止)?日期\s*[：:]?\s*{date_pattern}",
        )
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                parsed = _roc_date(match.group(1))
                if parsed:
                    return parsed
    return None


def _symbols(text: str) -> list[str]:
    values = re.findall(r"(?:股票|證券|有價證券)(?:代號|編號)?[：:\s（(]*([0-9A-Z]{4,10})", text)
    return sorted(set(values))


def _announcement_record(*, market: str, issued_date: str, number: str, subject: str, body: str, url: str) -> dict:
    text = " ".join(f"{subject} {body}".split())
    return {
        "announcement_date": issued_date,
        "market": market,
        "symbols": ",".join(_symbols(text)),
        "document_number": number,
        "subject": subject,
        "short_open_ban_date": _event_date(text, ("暫停融資融券", "暫停融券", "停止融券")),
        "short_cover_deadline": _event_date(text, ("還券了結", "償還或還券", "最後回補")),
        "trading_suspension_date": _event_date(text, ("停止買賣", "停止櫃檯買賣", "暫停交易")),
        "delisting_date": _event_date(text, ("終止上市", "終止上櫃", "終止櫃檯買賣")),
        "closing_only": bool(re.search(r"了結(?:交易)?(?:不受限|不在此限)", text)),
        "merger_or_share_exchange": bool(re.search(r"合併|股份轉換|換股", text)),
        "source_url": url,
        "body_text": text,
        "_downloaded_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    }


def _detail(session: requests.Session, url: str, timeout: int) -> str:
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    return BeautifulSoup(response.content, "html.parser").get_text(" ", strip=True)


def _download_tpex_month(year: int, month: int, timeout: int) -> list[dict]:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    response = session.post(
        TPEX_SEARCH,
        data={"inputY": str(year), "inputM": str(month), "inputD": "00", "inputType": "4", "inputKeyword": ""},
        timeout=timeout,
    )
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    records: list[dict] = []
    for row in soup.select("table tbody tr"):
        cells = row.find_all("td")
        if len(cells) < 5:
            continue
        issued = _roc_date(cells[1].get_text(" ", strip=True))
        link = cells[4].find("a", href=True)
        if not issued or link is None:
            continue
        url = urljoin(TPEX_SEARCH, link["href"])
        subject = cells[3].get_text(" ", strip=True)
        try:
            body = _detail(session, url, timeout)
        except Exception as exc:
            body = f"DETAIL_FETCH_ERROR: {exc}"
        records.append(_announcement_record(market="tpex", issued_date=issued, number=cells[2].get_text(" ", strip=True), subject=subject, body=body, url=url))
    return records


def _download_twse_month(year: int, month: int, timeout: int) -> list[dict]:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    search = session.get(TWSE_SEARCH, timeout=timeout)
    search.raise_for_status()
    soup = BeautifulSoup(search.content, "html.parser")
    token = soup.select_one('input[name="SYNCHRONIZER_TOKEN"]')
    if token is None:
        return []
    response = session.post(
        TWSE_RESULT,
        data={
            "SYNCHRONIZER_TOKEN": token.get("value", ""),
            "SYNCHRONIZER_URI": "/official/search",
            "queryby": "category",
            "startDate": f"{year:04d}/{month:02d}",
            "kind": "s",
        },
        timeout=timeout,
    )
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    records: list[dict] = []
    for row in soup.select("tbody tr"):
        cells = row.find_all("td")
        if len(cells) < 3:
            continue
        row_text = row.get_text(" ", strip=True)
        issued = _roc_date(row_text)
        link = row.find("a", href=True)
        if not issued or link is None:
            continue
        url = urljoin(TWSE_RESULT, link["href"])
        subject = cells[-2].get_text(" ", strip=True) if len(cells) >= 4 else row_text
        try:
            body = _detail(session, url, timeout)
        except Exception as exc:
            body = f"DETAIL_FETCH_ERROR: {exc}"
        records.append(_announcement_record(market="twse", issued_date=issued, number=cells[1].get_text(" ", strip=True), subject=subject, body=body, url=url))
    return records


def _download_twse_keyword(timeout: int) -> list[dict]:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    search = session.get(TWSE_SEARCH, timeout=timeout)
    search.raise_for_status()
    soup = BeautifulSoup(search.content, "html.parser")
    form = soup.select_one("#form_keyword")
    token = None if form is None else form.select_one('input[name="SYNCHRONIZER_TOKEN"]')
    if token is None:
        return []
    response = session.post(
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
    records: list[dict] = []
    for row in soup.select("tbody tr"):
        cells = row.find_all("td")
        if len(cells) < 5:
            continue
        issued = _roc_date(cells[1].get_text(" ", strip=True))
        subject = cells[4].get_text(" ", strip=True)
        if not issued or "權證" in subject or "終止上市" not in subject:
            continue
        if not re.search(r"(?:公司|股票|存託憑證).{0,80}終止上市|終止上市.{0,80}(?:公司|股票|存託憑證)", subject):
            continue
        link = row.find("a", href=True)
        url = TWSE_RESULT if link is None else urljoin(TWSE_RESULT, link["href"])
        body = subject
        if link is not None:
            try:
                body = _detail(session, url, timeout)
            except Exception as exc:
                body = f"{subject} DETAIL_FETCH_ERROR: {exc}"
        records.append(_announcement_record(market="twse", issued_date=issued, number=cells[3].get_text(" ", strip=True), subject=subject, body=body, url=url))
    return records


def _download_tpex_current_terms(timeout: int) -> pl.DataFrame:
    rows = requests.get(TPEX_SHORT_TERM, headers={"User-Agent": USER_AGENT}, timeout=timeout).json()
    records = []
    for row in rows if isinstance(rows, list) else []:
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Download official TW delisting/short-sale restriction announcements.")
    parser.add_argument("--output-dir", default="data_tw_public")
    parser.add_argument("--start-year", type=int, default=2002)
    parser.add_argument("--end-year", type=int, default=date.today().year)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    months = [(year, month) for year in range(args.start_year, args.end_year + 1) for month in range(1, 13) if date(year, month, 1) <= date.today()]
    tasks = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        for year, month in months:
            tasks.append(executor.submit(_download_tpex_month, year, month, args.timeout))
        tasks.append(executor.submit(_download_twse_keyword, args.timeout))
        records: list[dict] = []
        for future in as_completed(tasks):
            try:
                records.extend(future.result())
            except Exception as exc:
                print(f"[tw-short-restrictions] request failed: {exc}")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    announcements = pl.DataFrame(records, infer_schema_length=None) if records else pl.DataFrame()
    if not announcements.is_empty():
        announcement_path = output_dir / "tw_delisting_short_sale_announcements.parquet"
        if announcement_path.exists():
            announcements = pl.concat([pl.read_parquet(announcement_path), announcements], how="diagonal_relaxed")
        announcements.unique(["market", "document_number", "source_url"], keep="last").sort(["announcement_date", "market"]).write_parquet(announcement_path)
    terms = _download_tpex_current_terms(args.timeout)
    if not terms.is_empty():
        terms.write_parquet(output_dir / "tpex_short_sale_suspension_terms.parquet")
    print(f"[tw-short-restrictions] announcements={announcements.height} current_tpex_terms={terms.height} output={output_dir}")


if __name__ == "__main__":
    main()
