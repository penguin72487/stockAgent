from datetime import date
import gzip
import hashlib
import json

import polars as pl
import pytest

from scripts.audit_tw_futures_2012_holiday_margin import DATES, EXPECTED, check_rules, verify_notice


def _rules():
    rows = []
    for day in DATES:
        for product, values in EXPECTED.items():
            holiday, prior = values[:2], values[3:5]
            opening = prior if day in (DATES[0], DATES[1], DATES[4]) else holiday
            closing = prior if day in (DATES[0], DATES[3], DATES[4]) else holiday
            rows.append({"date": date.fromisoformat(day), "product": product, "contract": "201202",
                         "physical_contract": product + ":201202", "terminal_event": "mark_only",
                         "initial": opening[0], "maintenance": opening[1],
                         "settlement_initial": closing[0], "settlement_maintenance": closing[1]})
    # The January contract settles on 18 January and cannot carry into the
    # increased post-close margin period.
    rows.append({"date": date.fromisoformat(DATES[1]), "product": "TX", "contract": "201201",
                 "physical_contract": "TX:201201", "terminal_event": "cash_settlement",
                 "initial": 83000, "maintenance": 64000,
                 "settlement_initial": 83000, "settlement_maintenance": 64000})
    return pl.DataFrame(rows)


def test_notice_crosscheck_separates_open_close_and_expiry():
    result = check_rules(_rules())
    assert result["physical_contract_rows_checked"] == 41
    assert result["terminal_events_by_date"][DATES[1]]["cash_settlement"] == 1
    assert result["product_date_counts"][DATES[1] + ":TX"] == 2


def test_notice_crosscheck_rejects_wrong_intraday_or_missing_product_date():
    bad = _rules().with_columns(
        pl.when((pl.col("date") == date.fromisoformat(DATES[1])) & (pl.col("contract") == "201202")
                & (pl.col("product") == "TX")).then(83000)
        .otherwise(pl.col("settlement_initial")).alias("settlement_initial"))
    with pytest.raises(ValueError, match="unexpected margin"):
        check_rules(bad)
    missing = _rules().filter(~((pl.col("date") == date.fromisoformat(DATES[3]))
                                 & (pl.col("product") == "TGF")))
    with pytest.raises(ValueError, match="missing product dates"):
        check_rules(missing)


def test_notice_crosscheck_rejects_duplicate_physical_rows():
    rules = _rules()
    with pytest.raises(ValueError, match="duplicate physical contract"):
        check_rules(pl.concat([rules, rules.head(1)]))


@pytest.mark.parametrize("rollback", [False, True])
def test_notice_crosscheck_checks_bound_tables_and_effective_time(tmp_path, rollback):
    body = b"synthetic-notice"
    digest = hashlib.sha256(body).hexdigest()
    raw = tmp_path / "original.gz"
    raw.write_bytes(gzip.compress(body))
    content = ["1月31日交易時段結束後" if rollback else "1月18日 收盤後生效 1月31日"]
    for product, values in EXPECTED.items():
        expected = values[3:] + values[:3] if rollback else values
        content.append(product + " 原始保證金 金額 保證金 " + " ".join(f"{n:,}" for n in expected))
    parsed = tmp_path / "parsed.json"
    record = {"source_url": "https://www.taifex.com.tw/example.pdf", "content_sha256": digest,
              "text": "\n".join(content)}
    parsed.write_text(json.dumps(record))
    verify_notice(raw, parsed, digest, rollback=rollback)
    record["text"] = record["text"].replace("92,000", "92,001", 1)
    parsed.write_text(json.dumps(record))
    with pytest.raises(ValueError, match="values differ"):
        verify_notice(raw, parsed, digest, rollback=rollback)
