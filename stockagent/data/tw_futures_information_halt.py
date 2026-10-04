"""Source-bound daily accounting values for unopened information halts.

This is an input extension to margin preparation. It does not supply trades,
final fixings, corporate adjustments, or generic forward-filled quotations.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import gzip
import hashlib
import json
from pathlib import Path
import re

from bs4 import BeautifulSoup
import polars as pl

from downloader.artifact_io import sha256_file
from stockagent.data.tw_futures_margin_preparation import TAIPEI, compact, timestamp


CONTRACT = "source_bound_information_halt_value_v1"
HALT_URL = "https://www.taifex.com.tw/cht/3/tradeHalt"


def load_information_halt_review(path: Path) -> dict:
    """Bind native rules, dated announcement and the official halt register."""
    review = json.loads(path.read_text())
    if review.get("review_kind") != CONTRACT:
        raise ValueError("unsupported information-halt review")
    originals, native = {}, {}
    for item in review["sources"]:
        file = (path.parent / item["path"]).resolve()
        if not file.is_relative_to(path.parent.resolve()) or sha256_file(file) != item["sha256"]:
            raise ValueError("information-halt source SHA/path mismatch")
        url = item["url"]
        if item["kind"] == "raw_gzip":
            originals[url] = gzip.decompress(file.read_bytes())
        elif item["kind"] == "parsed_native":
            native[url] = json.loads(file.read_text())
    for url, parsed in native.items():
        if (url not in originals or parsed.get("source_url") != url
                or hashlib.sha256(originals[url]).hexdigest() != parsed["content_sha256"]
                or parsed.get("format") not in {"pdf", "html"}):
            raise ValueError("information-halt native text lacks original bytes")
    law = compact(native[review["law_source_url"]]["text"])
    for clause in ("2015年12月印製",
                   "股票期貨暨選擇權契約之標的證券因訊息面暫停交易",
                   "倘當日該股票期貨暨選擇權契約未開盤交易",
                   "該契約之每日結算價以當日開盤參考價訂定之"):
        if clause not in law:
            raise ValueError("information-halt daily value rule clause missing")
    commencement = compact(native[review["commencement_source_url"]]["text"])
    if ("2016/01/14" not in commencement or "105年1月15日" not in commencement
            or "訊息面" not in commencement):
        raise ValueError("information-halt commencement is not source-owned")
    # This current rule page is corroboration of the unchanged ordinary
    # opening-reference calculation, not a backdated historical price file.
    opening = compact(native[review["opening_rule_source_url"]]["text"])
    if ("開盤參考價依下列原則決定" not in opening
            or "前一一般交易時段結算價格" not in opening
            or "契約調整時" not in opening):
        raise ValueError("information-halt ordinary opening-reference rule missing")
    episode = review["episode"]
    product = episode["product"]
    if not re.fullmatch(r"[A-Z]{2}F", product):
        raise ValueError("information-halt extension requires a standard stock future")
    start = date.fromisoformat(episode["halt_start_date"])
    end = date.fromisoformat(episode["halt_end_date"])
    resume = date.fromisoformat(episode["resumption_date"])
    known = datetime.fromisoformat(episode["known_at"])
    # A continuation notice may be published after the first halt day.
    # The extension only admits dates strictly after that notice.
    if (not date(2016, 1, 15) <= start <= end < resume or known.tzinfo is None
            or known.astimezone(TAIPEI).date() >= end
            or not episode["contract_months"]
            or not all(re.fullmatch(r"\d{4}(?:0[1-9]|1[0-2])", m) for m in episode["contract_months"])):
        raise ValueError("information-halt episode has an unavailable clock/regime")
    notice = compact(native[episode["source_url"]]["text"])
    roc = lambda d: f"{d.year - 1911}年{d.month}月{d.day}日"
    month_literals = [f"{end.year - 1911}年{int(m[4:])}月份到期契約" for m in episode["contract_months"]]
    if (episode["known_at"][:10].replace("-", "/") not in notice
            or f"契約代號:{product[:2]}" not in notice
            or f"證券代號:{episode['underlying']}" not in notice
            or f"{roc(end)}繼續暫停交易" not in notice
            or f"最後交易(結算)日順延至{roc(resume)}" not in notice
            or not all(m in notice for m in month_literals)):
        raise ValueError("information-halt continuation/months are not source-owned")
    query_item = review["halt_register_query"]
    query_path = (path.parent / query_item["path"]).resolve()
    if not query_path.is_relative_to(path.parent.resolve()) or sha256_file(query_path) != query_item["sha256"]:
        raise ValueError("information-halt query receipt SHA/path mismatch")
    query = json.loads(query_path.read_text())
    if (query["url"] != HALT_URL or query["content_sha256"] != hashlib.sha256(originals[HALT_URL]).hexdigest()
            or datetime.strptime(query["payload"]["sDate"], "%Y/%m/%d").date() > start
            or datetime.strptime(query["payload"]["eDate"], "%Y/%m/%d").date() < end
            or query["payload"]["commodity_stock_id"] not in {"", episode["underlying"]}):
        raise ValueError("information-halt register query lacks the episode scope")
    soup = BeautifulSoup(originals[HALT_URL], "html.parser")
    rows = [[cell.get_text(" ", strip=True) for cell in tr.find_all(["td", "th"])]
            for table in soup.find_all("table") for tr in table.find_all("tr")]
    own = [r for r in rows if len(r) == 7 and r[1:3] == [product[:2], episode["underlying"]]
           and r[5] == f"{start.year}/{start.month}/{start.day}"
           and r[6] == f"{resume.year}/{resume.month}/{resume.day}"]
    if len(own) != 1:
        raise ValueError("information-halt reason/dates lack one official register row")
    return dict(episode, legal_halt_value_contract=CONTRACT,
        law_effective_date=date(2016, 1, 15), rule_known_at=episode["known_at"],
        corporate_input_sha256=review["corporate_input_sha256"],
        source_sha256s=sorted({hashlib.sha256(body).hexdigest() for body in originals.values()}),
        review_sha256=sha256_file(path), opening_reference_historical_file_verified=False,
        value_origin="statutory_information_halt_previous_session_reference")


def validate_information_halt_corporate_scope(corporate: pl.DataFrame, episode: dict) -> None:
    """Revalidate only the episode's corporate scope after unrelated repairs."""
    start, end = (date.fromisoformat(episode[k]) for k in ("halt_start_date", "halt_end_date"))
    action = corporate.filter(pl.any_horizontal(pl.col(c).str.starts_with(episode["product"][:2])
        for c in ("product", "from_product", "to_product"))
        & pl.col("effective_date").cast(pl.Date).is_between(start, end))
    if action.height:
        raise ValueError("information-halt ordinary reference cannot cross corporate actions")


def apply_information_halt_values(physical: pl.DataFrame, market_dates: pl.DataFrame,
                                 episode: dict, *, corporate: pl.DataFrame,
                                 corporate_input_sha256: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Fill only missing, nonfinal legal marks using the own preceding session.

No search for an older price, different delivery month or reused product code
is allowed. Corporate action days and observed executions reject this narrow
ordinary-reference extension. Existing official marks keep priority.
"""
    keys = ["date", "product", "contract", "physical_instance"]
    required = {*keys, "valuation_price", "daily_mark", "cash_settlement",
                "official_source_sha256", "official_settlement", "outright_volume",
                "official_open", "official_close"}
    if required - set(physical.columns) or physical.select("date", "product", "contract").is_duplicated().any():
        raise ValueError("information-halt values require unique source-bound physical marks")
    start, end = (date.fromisoformat(episode[k]) for k in ("halt_start_date", "halt_end_date"))
    known = datetime.fromisoformat(episode["rule_known_at"])
    if (episode.get("legal_halt_value_contract") != CONTRACT or known.tzinfo is None
            or start < episode["law_effective_date"] or start > end
            or end >= date.fromisoformat(episode["resumption_date"])
            or known.astimezone(TAIPEI).date() >= end
            or not re.fullmatch(r"[A-Z]{2}F", episode["product"])
            or not re.fullmatch(r"[a-f0-9]{64}", corporate_input_sha256)
            or not re.fullmatch(r"[a-f0-9]{64}", episode["review_sha256"])
            or not episode["source_sha256s"]
            or not all(re.fullmatch(r"[a-f0-9]{64}", s) for s in episode["source_sha256s"])):
        raise ValueError("invalid/unbound information-halt value contract")
    validate_information_halt_corporate_scope(corporate, episode)
    dates = market_dates["date"].cast(pl.Date).sort()
    if dates.null_count() or dates.is_duplicated().any():
        raise ValueError("information-halt market calendar is not unique")
    previous_dates = dict(zip(dates.to_list()[1:], dates.to_list()[:-1]))
    target = physical.filter((pl.col("product") == episode["product"])
        & pl.col("contract").is_in(episode["contract_months"]) & pl.col("date").is_between(start, end))
    if target.select(pl.any_horizontal(pl.col("outright_volume").fill_null(0) > 0,
        *[pl.col(c).cast(pl.Float64, strict=False).fill_null(0) > 0
          for c in ("official_open", "official_close")]).any()).item():
        raise ValueError("observed execution contradicts an unopened information halt")
    own_physical = physical.filter((pl.col("product") == episode["product"])
        & pl.col("contract").is_in(episode["contract_months"]))
    prior = {(row["date"], row["physical_instance"]): row for row in own_physical.select(
        "date", "physical_instance", "daily_mark", "official_settlement", "official_source_sha256").iter_rows(named=True)}
    rows = []
    for row in target.iter_rows(named=True):
        if row["valuation_price"] is not None or row["cash_settlement"] or known.astimezone(TAIPEI).date() >= row["date"]:
            continue
        previous = previous_dates.get(row["date"])
        operand = prior.get((previous, row["physical_instance"]))
        if not operand or not re.fullmatch(r"[a-f0-9]{64}", operand["official_source_sha256"] or ""):
            continue
        try:
            price = Decimal(operand["official_settlement"].replace(",", ""))
        except (TypeError, AttributeError, InvalidOperation):
            continue
        if not price.is_finite() or price <= 0 or float(price) != operand["daily_mark"]:
            continue
        rows.append({**{k: row[k] for k in keys}, "information_halt_valuation_price": float(price),
            "information_halt_prior_date": previous, "information_halt_prior_source_sha256": operand["official_source_sha256"],
            "information_halt_known_at": max(episode["rule_known_at"], timestamp(previous, "23:59:59")),
            "information_halt_source_sha256s": episode["source_sha256s"],
            "information_halt_corporate_input_sha256": corporate_input_sha256,
            "information_halt_review_sha256": episode["review_sha256"]})
    schema = {**{k: physical.schema[k] for k in keys}, "information_halt_valuation_price": pl.Float64,
        "information_halt_prior_date": pl.Date, "information_halt_prior_source_sha256": pl.String,
        "information_halt_known_at": pl.String, "information_halt_source_sha256s": pl.List(pl.String),
        "information_halt_corporate_input_sha256": pl.String,
        "information_halt_review_sha256": pl.String}
    values = pl.DataFrame(rows, schema=schema)
    metadata = [c for c in schema if c not in keys]
    present = set(metadata) & set(physical.columns)
    if present and present != set(metadata):
        raise ValueError("incomplete information-halt provenance")
    if present:
        if target.filter(pl.col("information_halt_review_sha256").is_not_null()
                & (pl.col("information_halt_review_sha256") != episode["review_sha256"])).height:
            raise ValueError("overlapping distinct information-halt reviews")
        result = physical.join(values.rename({c: "_info_" + c for c in metadata}), on=keys,
            how="left", validate="1:1").with_columns(
                *[pl.coalesce(c, "_info_" + c).alias(c) for c in metadata]).drop(
                    *["_info_" + c for c in metadata])
    else:
        result = physical.join(values, on=keys, how="left", validate="1:1")
    return result.with_columns(pl.coalesce("valuation_price", "information_halt_valuation_price")
        .alias("valuation_price")), values
