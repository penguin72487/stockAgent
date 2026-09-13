from datetime import date
import json
from types import SimpleNamespace

import pytest
import polars as pl

from downloader.download_tw_share_replacement_reference import parse_detail, parse_list, parse_issuer_payment, parse_tpex


@pytest.fixture(autouse=True)
def _isolate_tpex_ca_bootstrap(monkeypatch, tmp_path):
    from downloader import download_tw_share_replacement_reference as collector

    monkeypatch.setattr(
        collector, "_tpex_verify_bundle",
        lambda *_args, **_kwargs: tmp_path / "test-ca-bundle.pem",
    )


def issuer_fixture():
    return ("本資料由 (上市公司) 1459 測試公司提供 新股預計上市日:115/08/03 "
            "舊股票停止在市場買賣自115年07月23日起至115年08月02日止 "
            "每仟股換發750.0000股 每股退還股款新臺幣2.5000元 "
            "退還股款發放日期:115/08/13").encode()


def test_official_issuer_fallback_is_reference_not_silent_full_accounting():
    from downloader.download_tw_share_replacement_reference import parse_issuer_replacement
    event = parse_list(listing(), start=date(2026, 1, 1), end=date(2026, 9, 9))[0]
    row = parse_issuer_replacement(issuer_fixture(), event)
    assert row["new_shares_per_1000_old"] == 750
    assert row["suspension_date"] == date(2026, 7, 23)
    assert row["cash_return_per_old_share"] == 2.5
    assert row["cash_payment_date"] == date(2026, 8, 13)
    assert row["reference_only"] and not row["accounting_terms_complete"]
    assert row["cash_dividend_per_old_share"] is None
    assert row["subscription_shares_per_1000"] is None


@pytest.mark.parametrize("before,after", [
    ("1459", "9999"), ("每仟股換發750.0000股", "減資比率25%"),
    ("750.0000股", "750.0000股 每仟股換發700股"),
    ("新臺幣2.5000元", "新臺幣待公告元"),
    ("115年07月23日", "115年08月04日"),
])
def test_issuer_fallback_rejects_ambiguous_or_incomplete_core_terms(before, after):
    from downloader.download_tw_share_replacement_reference import parse_issuer_replacement
    event = parse_list(listing(), start=date(2026, 1, 1), end=date(2026, 9, 9))[0]
    with pytest.raises(ValueError):
        parse_issuer_replacement(issuer_fixture().decode().replace(before, after).encode(), event)


def test_issuer_fallback_does_not_reuse_another_resumption():
    from downloader.download_tw_share_replacement_reference import parse_issuer_replacement
    event = parse_list(listing(), start=date(2026, 1, 1), end=date(2026, 9, 9))[0]
    assert parse_issuer_replacement(issuer_fixture().replace(b"115/08/03", b"115/08/04"), event) is None


@pytest.mark.parametrize("halt_label", ["減資股票市場停止買賣期間：", "減資股票停止買賣期間：自", "舊股票停止在交易市場買賣期間：", "舊股票停止交易日期："])
def test_issuer_explicit_halt_labels_and_per_1000_cash_units(halt_label):
    from downloader.download_tw_share_replacement_reference import parse_issuer_replacement
    event = parse_list(listing(), start=date(2026, 1, 1), end=date(2026, 9, 9))[0]
    content = issuer_fixture().decode().replace("舊股票停止在市場買賣自", halt_label)
    content = content.replace("每股退還股款新臺幣2.5000元", "每仟股退還現金新台幣2,500元")
    assert parse_issuer_replacement(content.encode(), event)["cash_return_per_old_share"] == 2.5


def test_issuer_approximate_terms_are_not_accepted_as_exact():
    from downloader.download_tw_share_replacement_reference import parse_issuer_replacement
    event = parse_list(listing(), start=date(2026, 1, 1), end=date(2026, 9, 9))[0]
    for before, after in [("換發750", "換發約750"), ("新臺幣2.5", "新臺幣約2.5")]:
        with pytest.raises(ValueError):
            parse_issuer_replacement(issuer_fixture().decode().replace(before, after).encode(), event)


@pytest.mark.parametrize("ratio,face,expected", [(900, 10, 1), (600, 10, 4), (750, 10, 2.5)])
def test_explicit_cancelled_share_refund_formula(ratio, face, expected):
    from downloader.download_tw_share_replacement_reference import parse_issuer_replacement
    event = parse_list(listing(), start=date(2026, 1, 1), end=date(2026, 9, 9))[0]
    content = issuer_fixture().decode().replace("750.0000", str(ratio)).replace(
        "每股退還股款新臺幣2.5000元", f"股東減少之股份按每股面額新台幣{face}元計算退還現金")
    row = parse_issuer_replacement(content.encode(), event)
    assert row["cash_return_per_old_share"] == expected
    assert row["cash_return_calculation"] == "explicit_cancelled_shares_times_face_value"
    assert row["reference_only"] and not row["accounting_terms_complete"]


@pytest.mark.parametrize("clause", [
    "畸零股份按每股面額新台幣10元計算退還現金",
    "股東減少之股份按每股面額新台幣約10元計算退還現金",
    "股東減少之股份按每股面額新台幣10元計算退還現金。每股退還股款新臺幣3元",
    "股東減少之股份按每股面額新台幣10元計算退還現金。股東減少之股份按每股面額新台幣5元計算退還現金",
])
def test_cancelled_share_formula_rejects_fractional_approximate_or_conflicting_terms(clause):
    from downloader.download_tw_share_replacement_reference import parse_issuer_replacement
    event = parse_list(listing(), start=date(2026, 1, 1), end=date(2026, 9, 9))[0]
    content = issuer_fixture().decode().replace("每股退還股款新臺幣2.5000元", clause)
    with pytest.raises(ValueError):
        parse_issuer_replacement(content.encode(), event)


@pytest.mark.parametrize("cash_clause", [
    "每股退還股款現金新台幣2.5元",
    "每股可退換新臺幣2.5元",
    "每仟股換發750股(即每仟股減少250股)並退還股款新台幣2,500元",
    "每仟股換發750股(即每仟股減少250股),減資比率為25%,並退還股款新台幣2,500元",
])
def test_issuer_explicit_shared_per_1000_cash_subject(cash_clause):
    from downloader.download_tw_share_replacement_reference import parse_issuer_replacement
    event = parse_list(listing(), start=date(2026, 1, 1), end=date(2026, 9, 9))[0]
    content = issuer_fixture().decode().replace("每股退還股款新臺幣2.5000元", cash_clause)
    assert parse_issuer_replacement(content.encode(), event)["cash_return_per_old_share"] == 2.5


@pytest.mark.parametrize("unbound_clause", [
    "每仟股換發750股。本公司退還股款新台幣2,500元",
    "每仟股換發750股(即每仟股減少250股),畸零股並退還股款新台幣2,500元",
    "每仟股換發750股(即每仟股減少250股)並退還股款新台幣約2,500元",
    "每股退還股款現金約新台幣2.5元",
])
def test_issuer_cash_unit_never_crosses_clauses_or_accepts_approximation(unbound_clause):
    from downloader.download_tw_share_replacement_reference import parse_issuer_replacement
    event = parse_list(listing(), start=date(2026, 1, 1), end=date(2026, 9, 9))[0]
    content = issuer_fixture().decode().replace("每股退還股款新臺幣2.5000元", unbound_clause)
    with pytest.raises(ValueError, match="cash-per-share"):
        parse_issuer_replacement(content.encode(), event)


def test_issuer_date_grammar_is_shared_by_replacement_and_payment():
    from downloader.download_tw_share_replacement_reference import parse_issuer_replacement
    event = parse_list(listing(), start=date(2026, 1, 1), end=date(2026, 9, 9))[0]
    content = issuer_fixture().decode().replace("新股預計上市日:", "換發新股暨上市買賣日期:")
    content = content.replace("舊股票停止在市場買賣自", "減資股票於交易市場停止買賣期間:")
    row = parse_issuer_replacement(content.encode(), event)
    assert row["cash_payment_date"] == date(2026, 8, 13)
    assert row["suspension_date"] == date(2026, 7, 23)
    with pytest.raises(ValueError, match="conflicting resumption"):
        parse_issuer_payment((content + " 新股預計上市日:115/08/04").encode(), event)


@pytest.mark.parametrize("subject", [
    "公告本公司現金減資基準日\r\n暨換發股票作業計畫等相关事宜",
    "公告本公司換股作業計劃等相關事宜",
])
def test_issuer_list_subject_wrapping_does_not_hide_plan(tmp_path, monkeypatch, subject):
    from downloader import download_tw_share_replacement_reference as collector
    calls = []

    def fetch(path, *, data, **kwargs):
        calls.append(data)
        if "spoke_date" in data:
            return issuer_fixture()
        return ('<form name="t05st01_fm"><input name="step" value="2"></form>'
                '<table><tr><td>' + subject + '</td><td><input onclick="'
                "document.t05st01_fm.co_id.value='1459';"
                "document.t05st01_fm.spoke_date.value='20260710';"
                "document.t05st01_fm.spoke_time.value='123456';"
                "document.t05st01_fm.seq_no.value='1';"
                "document.t05st01_fm.TYPEK.value='sii';"
                '"></td></tr></table>').encode()

    monkeypatch.setattr(collector, "_cached_or_post", fetch)
    event = parse_list(listing(), start=date(2026, 1, 1), end=date(2026, 9, 9))[0]
    args = SimpleNamespace(end_date="2026-09-09", timeout=1, retries=0)
    plans = list(collector._issuer_announcements(tmp_path, event, args))
    assert plans and plans[0][0] == date(2026, 7, 10)
    assert plans[0][2] == issuer_fixture()


@pytest.mark.parametrize("suffix", ["止，停止在市場買賣", "止期間內停止在市場買賣", "止停止市場買賣", "止停止在市埸買賣交易"])
def test_issuer_halt_interval_before_verb_is_not_register_closing(suffix):
    from downloader.download_tw_share_replacement_reference import parse_issuer_replacement
    event = parse_list(listing(), start=date(2026, 1, 1), end=date(2026, 9, 9))[0]
    content = issuer_fixture().decode().replace(
        "舊股票停止在市場買賣自115年07月23日起至115年08月02日止",
        "舊股票自民國115年07月23日起至115年08月02日" + suffix
        + "。舊股票自民國115年07月25日起至115年08月02日止停止辦理過戶。")
    assert parse_issuer_replacement(content.encode(), event)["suspension_date"] == date(2026, 7, 23)
    with pytest.raises(ValueError, match="contradicts resumption"):
        parse_issuer_replacement(content.replace("115年08月02日", "115年08月04日").encode(), event)


def listing():
    return {"stat": "OK", "fields": ["恢復買賣日期", "股票代號", "名稱", "減資原因", "詳細資料"],
        "data": [["115/08/03", "1459", "聯發", "退還股款", "1459  ,20260722"]]}


def detail():
    return {"stat": "OK", "fields": ["股票代號：", "停止買賣日期：", "每壹仟股換發新股票：",
        "每股退還股款：", "原股每股配發現金股利：", "按股東持股比例每千股認購："],
        "data": [["1459  ", "115/07/23", "750.00000000 股", "2.500000 元/股", "0 元/股", "0 股"]]}


def test_capital_reduction_is_halt_share_ratio_cash_not_price_or_fill():
    event = parse_list(listing(), start=date(2026, 1, 1), end=date(2026, 9, 9))[0]
    row = parse_detail(detail(), event)
    assert row["suspension_date"] == date(2026, 7, 23)
    assert row["resume_date"] == date(2026, 8, 3)
    assert row["new_shares_per_1000_old"] == 750
    assert row["cash_return_per_old_share"] == 2.5
    assert row["cash_payment_date"] is None
    assert not row["accounting_terms_complete"] and not row["executable_price"]


def test_current_halt_includes_already_announced_future_resumption(tmp_path, monkeypatch):
    from downloader import download_tw_share_replacement_reference as collector
    from downloader.download_tw_corporate_action_entitlements import _record_raw_receipt_request
    requests = []

    def fetch(path, *, url, data, **kwargs):
        requests.append((url, data))
        payload = listing() if url == collector.LIST_URL else detail()
        if url == collector.TPEX_URL:
            payload = {"stat": "ok", "date": "20260101~20261022", "tables": [
                {"fields": listing()["fields"], "data": []}]}
        content = json.dumps(payload).encode()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        _record_raw_receipt_request(path, url=url, data=data, content=content, method="GET")
        return content

    monkeypatch.setattr(collector, "_cached_or_post", fetch)
    monkeypatch.setattr(collector, "_configure_tw_public_rate_limiter", lambda *_: None)
    monkeypatch.setattr(collector, "fetch_issuer_payment", lambda *_: {})
    proof = collector.run(SimpleNamespace(start_date="2026-01-01", end_date="2026-07-24",
        output_dir=tmp_path, timeout=1, retries=1, request_interval=0))
    assert proof["rows"] == 1  # halted 07/23; resumes 08/03, after the requested as-of
    assert proof["coverage_end"] == "2026-07-24"
    assert proof["announced_resumption_query_end"] == "2026-10-22"
    assert proof["future_market_observations_claimed"] is False
    assert requests[0][1]["endDate"] == "20261022"
    assert requests[-1][1]["endDate"] == "2026/10/22"


@pytest.mark.parametrize("failure", ["symbol", "interval", "amount", "duplicate", "range"])
def test_ambiguous_terms_fail_closed(failure):
    source = listing()
    if failure == "duplicate":
        source["data"] *= 2
    end = date(2026, 7, 1) if failure == "range" else date(2026, 9, 9)
    data = detail()
    if failure == "symbol":
        data["data"][0][0] = "9999"
    if failure == "interval":
        data["data"][0][1] = "115/08/04"
    if failure == "amount":
        data["data"][0][3] = "--"
    with pytest.raises(ValueError):
        parse_detail(data, parse_list(source, start=date(2026, 1, 1), end=end)[0])


def test_issuer_payment_requires_company_and_matching_resumption():
    event = {"symbol": "1459", "resume_date": date(2026, 8, 3)}
    text = "本資料由 (上市公司) 1459 聯發 公司提供 新股預計上市日:115/08/03 現金減資退還股款發放日：民國115年08月13日。"
    assert parse_issuer_payment(text.encode(), event) == date(2026, 8, 13)
    assert parse_issuer_payment(text.replace("115/08/03", "115/08/04").encode(), event) is None
    with pytest.raises(ValueError, match="identity"):
        parse_issuer_payment(text.replace("1459", "9999").encode(), event)
    with pytest.raises(ValueError, match="conflicting"):
        parse_issuer_payment((text + "退還股款發放日:115/08/14").encode(), event)
    assert parse_issuer_payment(text.replace("上市", "上櫃").encode(), event) == date(2026, 8, 13)
    amended = text.replace(
        "發放日：民國115年08月13日",
        "發放日訂於民國115年08月13日",
    )
    assert parse_issuer_payment(amended.encode(), event) == date(2026, 8, 13)


def test_tpex_detail_is_separate_physical_share_evidence():
    fields = ["恢復買賣日期", "股票代號", "名稱", "減資原因", "詳細資料"]
    terms = {"股票代號/股票名稱": "3152 公司", "恢復買賣日期": "115/06/30",
             "停止買賣日期": "115/06/23", "每壹仟股換發新股票": "565.31945000 股",
             "每股退還股款": "4.34680553 元/股", "現金增資總股數": "NA",
             "現金增資認購價": "NA", "現金增資配股率": "NA"}
    html = "<table>" + "".join(f"<tr><th>{k}：</th><td>{v}</td></tr>" for k, v in terms.items()) + "</table>"
    data = {"stat": "ok", "date": "20260101~20260909", "tables": [
        {"fields": fields, "data": [["1150630", "3152", "公司", "退還股款", html]]}]}
    row = parse_tpex(data, start=date(2026, 1, 1), end=date(2026, 9, 9))[0]
    assert row["new_shares_per_1000_old"] == 565.31945
    assert row["cash_payment_date"] is None
    assert not row["subscription_terms_present"] and not row["executable_price"]
    with pytest.raises(ValueError, match="date/status"):
        parse_tpex(data, start=date(2026, 1, 1), end=date(2026, 9, 10))


@pytest.mark.parametrize("broken_stage", ["twse_list", "tpex_reference"])
def test_source_failure_is_isolated_and_never_replaces_accepted_output(tmp_path, monkeypatch, broken_stage):
    from downloader import download_tw_share_replacement_reference as collector
    from downloader.download_tw_corporate_action_entitlements import _record_raw_receipt_request

    accepted = tmp_path / "tw_share_replacement_reference.parquet"
    receipt = accepted.with_suffix(".summary.json")
    accepted.write_bytes(b"prior verified release")
    receipt.write_bytes(b"prior verified receipt")
    calls, payments = [], []
    broken = [True]

    def fetch(path, *, url, data, **kwargs):
        calls.append((url, data))
        stage = {collector.LIST_URL: "twse_list", collector.DETAIL_URL: "twse_detail",
                 collector.TPEX_URL: "tpex_reference"}[url]
        if broken[0] and stage == broken_stage and data.get("STK_NO", "1459") == "1459":
            raise RuntimeError("HTTP 428 fixture")
        payload = listing() if url == collector.LIST_URL else detail()
        if url == collector.LIST_URL:
            payload["data"].append(["115/08/04", "2330", "測試", "退還股款", "2330,20260723"])
        elif url == collector.DETAIL_URL:
            payload["data"][0][0] = data["STK_NO"]
        else:
            payload = {"stat": "ok", "date": "20260101~20261022", "tables": [
                {"fields": listing()["fields"], "data": []}]}
        content = json.dumps(payload).encode()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        _record_raw_receipt_request(path, url=url, data=data, content=content, method="GET")
        return content

    def payment(raw, row, args):
        payments.append(row["symbol"])
        if broken[0] and broken_stage == "issuer_payment" and row["symbol"] == "1459":
            raise RuntimeError("issuer unavailable fixture")
        return {}

    monkeypatch.setattr(collector, "_cached_or_post", fetch)
    monkeypatch.setattr(collector, "fetch_issuer_payment", payment)
    monkeypatch.setattr(collector, "_configure_tw_public_rate_limiter", lambda *_: None)
    args = SimpleNamespace(start_date="2026-01-01", end_date="2026-07-24",
        output_dir=tmp_path, timeout=1, retries=0, request_interval=0)
    with pytest.raises(RuntimeError, match="collection incomplete"):
        collector.run(args)
    assert accepted.read_bytes() == b"prior verified release"
    assert receipt.read_bytes() == b"prior verified receipt"
    failed = json.loads(accepted.with_suffix(".attempt.summary.json").read_text())
    assert not failed["source_download_complete"] and not failed["complete_all_markets"]
    assert failed["failure_count"] == 1
    assert failed["failures"][0]["stage"] == broken_stage
    attempt = accepted.with_suffix(".attempt.parquet")
    if failed.get("completed_reference_rows", 0):
        assert attempt.is_file()
        assert failed["partial_reference_contract"] == (
            "successful_rows_only_not_complete_or_accepted"
        )
        assert failed["partial_reference_receipt"]["sha256"]
    assert any(url == collector.TPEX_URL for url, _ in calls)
    if broken_stage != "twse_list":
        assert "2330" in payments  # later healthy event was not starved
    broken[0] = False
    ready = collector.run(args)
    assert ready["source_download_complete"] and ready["complete_all_markets"]
    assert ready["failure_count"] == 0 and ready["rows"] == 2


@pytest.mark.parametrize("broken_stage", ["twse_detail", "issuer_payment"])
def test_accounting_failure_preserves_complete_lifecycle_catalogue(
    tmp_path, monkeypatch, broken_stage,
):
    from downloader import download_tw_share_replacement_reference as collector
    from downloader.download_tw_corporate_action_entitlements import (
        _record_raw_receipt_request,
    )

    def fetch(path, *, url, data, **kwargs):
        if url == collector.DETAIL_URL and broken_stage == "twse_detail":
            raise RuntimeError("HTTP 428 fixture")
        payload = listing() if url == collector.LIST_URL else detail()
        if url == collector.TPEX_URL:
            payload = {
                "stat": "ok", "date": "20260101~20261022",
                "tables": [{"fields": listing()["fields"], "data": []}],
            }
        content = json.dumps(payload).encode()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        _record_raw_receipt_request(
            path, url=url, data=data, content=content, method="GET"
        )
        return content

    def payment(raw, row, args):
        if broken_stage == "issuer_payment":
            raise RuntimeError("issuer unavailable fixture")
        return {}

    monkeypatch.setattr(collector, "_cached_or_post", fetch)
    monkeypatch.setattr(collector, "fetch_issuer_payment", payment)
    monkeypatch.setattr(
        collector, "fetch_issuer_replacement",
        lambda *_: (_ for _ in ()).throw(RuntimeError("issuer unavailable fixture")),
    )
    monkeypatch.setattr(
        collector, "_configure_tw_public_rate_limiter", lambda *_: None
    )
    ready = collector.run(SimpleNamespace(
        start_date="2026-01-01", end_date="2026-07-24",
        output_dir=tmp_path, timeout=1, retries=0, request_interval=0,
    ))
    assert ready["source_download_complete"]
    assert ready["complete_all_markets"]
    assert ready["lifecycle_catalog_complete"]
    assert ready["failure_count"] == 0
    assert ready["accounting_failure_count"] == 1
    row = pl.read_parquet(
        tmp_path / "tw_share_replacement_reference.parquet"
    ).to_dicts()[0]
    if broken_stage == "twse_detail":
        assert row["suspension_date"] is None
        assert row["new_shares_per_1000_old"] is None
        assert row["detail_error"]
    else:
        assert row["suspension_date"] == date(2026, 7, 23)
        assert row["cash_payment_date"] is None


def test_official_issuer_recovery_preserves_provenance_and_bounds_primary_retries(tmp_path, monkeypatch):
    import requests
    import polars as pl
    from downloader import download_tw_share_replacement_reference as collector
    from downloader.download_tw_corporate_action_entitlements import _record_raw_receipt_request

    primary_calls = []

    def fetch(path, *, url, data, **kwargs):
        if url == collector.DETAIL_URL:
            primary_calls.append(data["STK_NO"])
            response = requests.Response()
            response.status_code = 428
            raise requests.HTTPError("official detail blocked", response=response)
        payload = listing()
        payload["data"].append(["115/08/04", "2330", "測試", "退還股款", "2330,20260723"])
        if url == collector.TPEX_URL:
            payload = {"stat": "ok", "date": "20260101~20261022", "tables": [
                {"fields": listing()["fields"], "data": []}]}
        content = json.dumps(payload).encode()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        _record_raw_receipt_request(path, url=url, data=data, content=content, method="GET")
        return content

    def fallback(raw, event, args):
        content = issuer_fixture().replace(b"1459", event["symbol"].encode())
        content = content.replace(b"115/08/03", f"115/08/{event['resume_date'].day:02d}".encode())
        row = collector.parse_issuer_replacement(content, event)
        path = raw / f"recovered-{event['symbol']}.html"
        path.write_bytes(content)
        _record_raw_receipt_request(path, url=collector.MOPS_ANNOUNCEMENT_URL,
            data={"co_id": event["symbol"]}, content=content)
        return row | {"detail_source_sha256": collector._file_receipt(path)["sha256"]}

    monkeypatch.setattr(collector, "_cached_or_post", fetch)
    monkeypatch.setattr(collector, "fetch_issuer_replacement", fallback)
    monkeypatch.setattr(collector, "fetch_issuer_payment", lambda *_: {})
    monkeypatch.setattr(collector, "_configure_tw_public_rate_limiter", lambda *_: None)
    ready = collector.run(SimpleNamespace(start_date="2026-01-01", end_date="2026-07-24",
        output_dir=tmp_path, timeout=1, retries=0, request_interval=0))
    assert primary_calls == ["1459"]  # no per-symbol hammering of denied endpoint
    assert ready["source_download_complete"] and ready["failure_count"] == 0
    assert ready["official_issuer_reference_recovery_count"] == 2
    assert not ready["accounting_ready"]
    assert [row["primary_attempted"] for row in ready["recovered_exchange_detail_failures"]] == [True, False]
    frame = pl.read_parquet(tmp_path / "tw_share_replacement_reference.parquet")
    assert frame["reference_only"].to_list() == [True, True]
    assert frame["cash_dividend_per_old_share"].null_count() == 2
